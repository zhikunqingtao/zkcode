//! Durable adapter for physical resources owned by `zk-tools` supervisors.

use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zk_db::{CasOutcome, Db, ExecutionResourceStatus, NewExecutionResource};
use zk_tools::{
    CallEnv, ExecutionResourceAllocation, ExecutionResourceLease, ExecutionResourceObserver,
    ExecutionResourceOwner, ExecutionResourceTerminal, Tool, ToolEvent, ToolExecutor,
    ToolExecutorShutdownReport,
};

/// Process-wide execution entry point shared by every production surface that
/// can launch a tool. It combines the global leaf-tool scheduler with the
/// durable resource observer, so a caller cannot accidentally attach a Run ID
/// while omitting physical-resource ownership.
#[derive(Clone)]
pub struct ExecutionSupervisor {
    executor: ToolExecutor,
    resource_observer: Arc<dyn ExecutionResourceObserver>,
}

impl std::fmt::Debug for ExecutionSupervisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExecutionSupervisor")
            .finish_non_exhaustive()
    }
}

impl ExecutionSupervisor {
    /// Build the production supervisor backed by the `TaskRuntime` database.
    #[must_use]
    pub fn new(db: Db) -> Self {
        Self {
            executor: ToolExecutor::new(),
            resource_observer: DbExecutionResourceObserver::shared(db),
        }
    }

    /// Whether the supervisor is wired to the process-wide workspace lease
    /// registry used by every production leaf-tool executor.
    #[must_use]
    pub fn workspace_leases_ready(&self) -> bool {
        self.executor.workspace_leases_ready()
    }

    /// Close the single production leaf-execution intake before `TaskRuntime`
    /// cancellation begins. Existing tool/process owners remain supervised.
    pub fn close_intake(&self) {
        self.executor.close_intake();
    }

    /// Whether the production leaf supervisor may accept another physical call.
    #[must_use]
    pub fn accepts_new_execution(&self) -> bool {
        self.executor.accepts_new_execution()
    }

    /// Number of retained tool/process cleanup owners.
    #[must_use]
    pub fn active_owner_count(&self) -> usize {
        self.executor.active_owner_count()
    }

    /// Cancel and drain all physical leaf owners inside the supplied window.
    pub async fn shutdown(&self, grace: std::time::Duration) -> ToolExecutorShutdownReport {
        self.executor.shutdown(grace).await
    }

    /// Register a protocol-level terminalizer before it creates any durable
    /// invocation rows.  Dropping the protocol response Future can only signal
    /// `cancel`; the terminalizer itself stays owned until cleanup and the
    /// terminal database transaction have both completed.
    ///
    /// # Errors
    ///
    /// Returns `EXECUTION_SUPERVISOR_SHUTTING_DOWN` after leaf/finalizer
    /// intake has been closed.
    pub fn spawn_owned_finalizer(
        &self,
        cancel: CancellationToken,
        future: BoxFuture<'static, ()>,
    ) -> Result<(), &'static str> {
        self.executor.spawn_owned_finalizer(cancel, future)
    }

    /// Dispatch a tool after its invocation row has been committed. The stable
    /// owner is bound to the context here rather than at each API surface.
    #[must_use]
    pub fn spawn_call_in(
        &self,
        tool: Arc<dyn Tool>,
        tool_use_id: String,
        input: serde_json::Value,
        parent_cancel: &CancellationToken,
        env: CallEnv,
        owner: ExecutionResourceOwner,
    ) -> mpsc::Receiver<ToolEvent> {
        self.executor.spawn_call_in(
            tool,
            tool_use_id,
            input,
            parent_cancel,
            env.with_execution_resources(owner, Arc::clone(&self.resource_observer)),
        )
    }

    pub(crate) fn executor(&self) -> ToolExecutor {
        self.executor.clone()
    }

    pub(crate) fn resource_observer(&self) -> Arc<dyn ExecutionResourceObserver> {
        Arc::clone(&self.resource_observer)
    }
}

/// Connects process/MCP lifetime ownership to the unified `SQLite` runtime.
#[derive(Clone)]
pub(crate) struct DbExecutionResourceObserver {
    db: Db,
}

impl DbExecutionResourceObserver {
    pub(crate) fn shared(db: Db) -> Arc<dyn ExecutionResourceObserver> {
        Arc::new(Self { db })
    }
}

impl ExecutionResourceObserver for DbExecutionResourceObserver {
    fn register(
        &self,
        owner: ExecutionResourceOwner,
        allocation: ExecutionResourceAllocation,
    ) -> BoxFuture<'static, Result<ExecutionResourceLease, String>> {
        let db = self.db.clone();
        Box::pin(async move {
            let lease = ExecutionResourceLease {
                resource_id: allocation.resource_id.clone(),
            };
            db.register_execution_resource(&NewExecutionResource {
                resource_id: allocation.resource_id,
                task_id: owner.task_id,
                run_id: owner.run_id,
                invocation_id: Some(owner.invocation_id),
                resource_kind: allocation.resource_kind,
                external_id: allocation.external_id,
                metadata_json: serde_json::to_string(&allocation.metadata)
                    .map_err(|error| format!("RESOURCE_METADATA_INVALID: {error}"))?,
            })
            .await
            .map_err(|error| format!("RESOURCE_REGISTER_FAILED: {error}"))?;
            Ok(lease)
        })
    }

    fn bind_external(
        &self,
        lease: ExecutionResourceLease,
        external_id: String,
    ) -> BoxFuture<'static, Result<(), String>> {
        let db = self.db.clone();
        Box::pin(async move {
            match db
                .bind_execution_resource_external(&lease.resource_id, &external_id)
                .await
                .map_err(|error| format!("RESOURCE_BIND_FAILED: {error}"))?
            {
                CasOutcome::Applied => Ok(()),
                outcome => Err(format!("RESOURCE_BIND_{outcome:?}")),
            }
        })
    }

    fn finish(
        &self,
        lease: ExecutionResourceLease,
        terminal: ExecutionResourceTerminal,
    ) -> BoxFuture<'static, Result<(), String>> {
        let db = self.db.clone();
        Box::pin(async move {
            let target = match terminal {
                ExecutionResourceTerminal::Released => ExecutionResourceStatus::Released,
                ExecutionResourceTerminal::Unconfirmed => ExecutionResourceStatus::Unconfirmed,
            };
            match db
                .finalize_execution_resource(&lease.resource_id, target)
                .await
                .map_err(|error| format!("RESOURCE_TERMINAL_WRITE_FAILED: {error}"))?
            {
                CasOutcome::Applied => Ok(()),
                outcome => Err(format!("RESOURCE_TERMINAL_{outcome:?}")),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rusqlite::params;
    use tokio_util::sync::CancellationToken;
    use zk_db::{CleanupStatus, CreateTaskWithRun, NewToolInvocation, ToolInvocationStatus};
    use zk_tools::{BashTool, ExecutionResourceOwner, ToolCleanupStatus};

    use super::*;

    fn id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    #[test]
    fn production_supervisor_has_process_wide_workspace_leases() {
        let supervisor = ExecutionSupervisor::new(Db::open_in_memory().expect("db"));
        assert!(supervisor.workspace_leases_ready());
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the lifecycle test verifies allocation, process cleanup and durable release"
    )]
    async fn real_process_is_owned_and_released_in_the_runtime_database() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/resource-adapter")
            .await
            .expect("session");
        let task_id = id();
        let run_id = id();
        db.create_task_with_run(&CreateTaskWithRun {
            task_id: task_id.clone(),
            run_id: run_id.clone(),
            root_session_id: session.id.clone(),
            transcript_session_id: session.id,
            parent_task_id: None,
            parent_run_id: None,
            creator_tool_use_id: None,
            ordinal: 0,
            description: "resource adapter".to_owned(),
            prompt: None,
            task_type: "agent".to_owned(),
            model: "test-model".to_owned(),
            working_dir: "/tmp/resource-adapter".to_owned(),
            execution_config_json: "{}".to_owned(),
            startup_epoch: 1,
        })
        .await
        .expect("task/run");

        let invocation_id = id();
        let invocation = db
            .create_tool_invocation(&NewToolInvocation {
                invocation_id: invocation_id.clone(),
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                tool_use_id: "tool-use-1".to_owned(),
                tool_name: "Bash".to_owned(),
                input_json: Some(r#"{"command":"printf wired"}"#.to_owned()),
                side_effect_class: "write".to_owned(),
                directory_generation: Some(1),
                connection_generation: None,
            })
            .await
            .expect("invocation");
        db.transition_tool_invocation_cas(
            &invocation_id,
            invocation.version,
            ToolInvocationStatus::Running,
            Some(r#"{"command":"printf wired"}"#),
            None,
            None,
            CleanupStatus::Pending,
        )
        .await
        .expect("running invocation");

        let supervisor = ExecutionSupervisor::new(db.clone());
        let mut events = supervisor.spawn_call_in(
            Arc::new(BashTool),
            "tool-use-1".to_owned(),
            serde_json::json!({"command": "printf wired"}),
            &CancellationToken::new(),
            CallEnv::new()
                .with_working_dir("/tmp")
                .with_session_id("resource-test")
                .with_run_id(run_id.clone()),
            ExecutionResourceOwner {
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                invocation_id: invocation_id.clone(),
            },
        );
        let terminal = loop {
            match events.recv().await.expect("terminal tool event") {
                ToolEvent::Progress { .. } => {}
                event @ ToolEvent::Finished { .. } => break event,
            }
        };
        assert!(matches!(
            terminal,
            ToolEvent::Finished {
                output,
                cleanup_status: ToolCleanupStatus::Confirmed,
                ..
            } if !output.is_error && output.content.contains("wired")
        ));

        let expected_task_id = task_id.clone();
        let expected_run_id = run_id.clone();
        let expected_invocation_id = invocation_id.clone();
        let row: (String, String, String, String, String) = db
            .with_conn_blocking(move |conn| {
                conn.query_row(
                    "SELECT er.task_id,er.run_id,er.invocation_id,er.status,ti.cleanup_status
                       FROM execution_resources er
                       JOIN tool_invocations ti ON ti.invocation_id=er.invocation_id
                      WHERE er.task_id=?1 AND er.run_id=?2 AND er.invocation_id=?3",
                    params![expected_task_id, expected_run_id, expected_invocation_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .map_err(Into::into)
            })
            .expect("resource projection");
        assert_eq!(row.0, task_id);
        assert_eq!(row.1, run_id);
        assert_eq!(row.2, invocation_id);
        assert_eq!(row.3, "released");
        assert_eq!(row.4, "confirmed");
        let report = supervisor.shutdown(Duration::from_secs(1)).await;
        assert!(report.drained);
        assert_eq!(report.owners_remaining, 0);
    }

    #[tokio::test]
    async fn finalizer_owner_survives_physical_finish_until_durable_commit() {
        let supervisor = ExecutionSupervisor::new(Db::open_in_memory().expect("db"));
        let cancel = CancellationToken::new();
        let (physical_finished_tx, physical_finished_rx) = tokio::sync::oneshot::channel();
        let (commit_release_tx, commit_release_rx) = tokio::sync::oneshot::channel();
        let (commit_finished_tx, commit_finished_rx) = tokio::sync::oneshot::channel();

        supervisor
            .spawn_owned_finalizer(
                cancel,
                Box::pin(async move {
                    let _ = physical_finished_rx.await;
                    let _ = commit_release_rx.await;
                    let _ = commit_finished_tx.send(());
                }),
            )
            .expect("register finalizer before durable work");
        physical_finished_tx
            .send(())
            .expect("mark physical execution finished");
        tokio::task::yield_now().await;
        assert_eq!(
            supervisor.active_owner_count(),
            1,
            "physical completion must not release the durable finalizer owner"
        );

        let shutdown_supervisor = supervisor.clone();
        let mut shutdown =
            tokio::spawn(async move { shutdown_supervisor.shutdown(Duration::from_secs(1)).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
                .await
                .is_err(),
            "shutdown must wait for the durable terminal commit"
        );
        commit_release_tx.send(()).expect("allow durable commit");
        commit_finished_rx.await.expect("durable commit completed");
        let report = shutdown.await.expect("shutdown joins");
        assert!(report.drained);
        assert_eq!(report.owners_requested, 1);
        assert_eq!(report.owners_remaining, 0);
    }
}
