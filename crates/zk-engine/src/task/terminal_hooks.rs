//! Child completion notifications borrow a live parent's existing execution owner.
use super::{RuntimeTaskRecord, TaskRuntimeInner};
use crate::hook::{HookContext, HookEvent, HookService};
use std::sync::Arc;
use tokio::sync::OwnedMutexGuard;

pub(super) struct PendingNotification {
    context: HookContext,
    hooks: Arc<HookService>,
    _admission: OwnedMutexGuard<bool>,
}

pub(super) async fn prepare(
    inner: &Arc<TaskRuntimeInner>,
    task_id: &str,
) -> Option<PendingNotification> {
    let hooks = inner.hooks.get()?.clone();
    let prepared = async {
        let supervisor = inner
            .hook_supervisor
            .get()
            .ok_or("HOOK_SUPERVISOR_UNAVAILABLE")?;
        let child = inner
            .db
            .find_runtime_task_by_id(task_id)
            .await
            .map_err(|_| "HOOK_OWNER_STORE_FAILED")?
            .ok_or("HOOK_CHILD_OWNER_MISSING")?;
        let Some(parent_id) = child.parent_task_id.as_deref() else {
            return Ok(None);
        };
        let active = inner
            .active
            .get(parent_id)
            .map(|active| active.clone())
            .ok_or("HOOK_PARENT_NOT_ACTIVE")?;
        if child.creator_run_id.as_deref() != Some(active.run_id.as_str()) {
            return Err("HOOK_PARENT_ATTEMPT_CHANGED");
        }
        let guard = active.hook_notifications.clone().lock_owned().await;
        if !*guard || active.cancel.is_cancelled() {
            return Err("HOOK_PARENT_ADMISSION_CLOSED");
        }
        let parent = inner
            .db
            .find_runtime_task_by_id(parent_id)
            .await
            .map_err(|_| "HOOK_OWNER_STORE_FAILED")?
            .ok_or("HOOK_PARENT_NOT_ACTIVE")?;
        let run = inner
            .db
            .find_run_by_id(&active.run_id)
            .await
            .map_err(|_| "HOOK_OWNER_STORE_FAILED")?
            .ok_or("HOOK_PARENT_NOT_ACTIVE")?;
        if parent.session_id != child.session_id
            || parent.current_run_id.as_deref() != Some(run.id.as_str())
            || run.task_id != parent.id
            || parent.status.is_terminal()
            || run.requested_exit_reason.is_some()
            || run.finished_at.is_some()
            || parent
                .deadline_at_ms
                .is_some_and(|deadline| deadline <= zk_db::time::now_millis())
        {
            return Err("HOOK_PARENT_NOT_ACTIVE");
        }
        if inner
            .db
            .session_retention(&child.session_id)
            .await
            .map_err(|_| "HOOK_CONTENT_POLICY_UNAVAILABLE")?
            != zk_db::content::ContentRetention::Persistent
        {
            return Err("HOOK_EPHEMERAL_EXTERNAL_UNSUPPORTED");
        }
        let session = inner
            .db
            .get_session(&run.session_id)
            .await
            .map_err(|_| "HOOK_OWNER_STORE_FAILED")?
            .ok_or("HOOK_PARENT_NOT_ACTIVE")?;
        let context = supervisor.process_context(
            &parent.id,
            &run.id,
            &run.session_id,
            std::path::Path::new(&session.working_dir),
            active.cancel.clone(),
        );
        let owner = context
            .execution_resource_owner()
            .cloned()
            .ok_or("HOOK_PARENT_NOT_ACTIVE")?;
        let context = context.with_execution_resources(
            owner,
            crate::execution_resources::DbExecutionResourceObserver::hook_shared(inner.db.clone()),
        );
        let hook_context = HookContext::new()
            .with_session(&run.session_id)
            .with_working_dir(session.working_dir)
            .with_cancellation(&active.cancel)
            .require_execution_owner()
            .with_execution(context, supervisor.executor());
        Ok(Some(PendingNotification {
            context: hook_context,
            hooks,
            _admission: guard,
        }))
    }
    .await;
    match prepared {
        Ok(notification) => notification,
        Err(code) => {
            tracing::debug!(
                code,
                task_id,
                "task completion hook skipped without a current parent owner"
            );
            let mut event = crate::ObservabilityEvent::new("task", "completionHook", "skipped");
            event
                .attributes
                .insert("taskId".into(), serde_json::json!(task_id));
            event
                .attributes
                .insert("code".into(), serde_json::json!(code));
            inner.observability.record(event);
            None
        }
    }
}

pub(super) async fn publish(
    notification: Option<PendingNotification>,
    task: &RuntimeTaskRecord,
    result_preview: &str,
) {
    let Some(notification) = notification else {
        return;
    };
    let PendingNotification {
        context,
        hooks,
        _admission: admission,
    } = notification;
    let context = context.with_result_preview(result_preview);
    // For async Hooks, fire installs the parent's pending receiver before this
    // admission guard drops. Parent sealing therefore precedes its final drain.
    hooks.fire(HookEvent::TaskCompleted, &context).await;
    if serde_json::from_str::<serde_json::Value>(&task.execution_config_json)
        .ok()
        .is_some_and(|value| value["teamId"].is_string())
    {
        hooks.fire(HookEvent::TeammateIdle, &context).await;
    }
    drop(admission);
}

#[cfg(test)]
mod tests {
    use super::{prepare, publish};
    use crate::task::runtime::{
        TaskExecutionLease, TaskExecutionResult, TaskRuntime, TerminalCommitResult, commit_outcome,
    };
    use crate::{execution_resources::ExecutionSupervisor, hook::HookService, sink::MessageSink};
    use futures::future::BoxFuture;
    use std::{path::PathBuf, sync::Arc, time::Duration};
    use tokio_util::sync::CancellationToken;
    use zk_db::{CasOutcome, CleanupStatus, CreateTaskWithRun, RuntimeTaskRecord};
    use zk_protocol::ServerMessage;

    struct Sink;
    impl MessageSink for Sink {
        fn push<'a>(&'a self, _: &'a str, _: ServerMessage) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
    }
    struct Fixture {
        runtime: TaskRuntime,
        hooks: Arc<HookService>,
        root_run: String,
        child: RuntimeTaskRecord,
        child_run: String,
        workspace: PathBuf,
        _lease: TaskExecutionLease,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.workspace);
        }
    }
    async fn fixture() -> Fixture {
        let workspace =
            std::env::temp_dir().join(format!("zk-parent-hook-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(workspace.join(".zk")).unwrap();
        std::fs::write(
            workspace.join(".zk/hooks.toml"),
            r#"
[[hook]]
name="completed"
event="TASK_COMPLETED"
command="printf done >> completed.txt"
async=true
timeout_secs=5
[[hook]]
name="idle"
event="TEAMMATE_IDLE"
command="printf idle >> idle.txt"
async=true
timeout_secs=5
"#,
        )
        .unwrap();
        let workspace = workspace.canonicalize().unwrap();
        let db = zk_db::Db::open_in_memory().unwrap();
        let session = db
            .create_session("fixture", workspace.to_str().unwrap())
            .await
            .unwrap();
        let root_run = uuid::Uuid::new_v4().to_string();
        db.start_root_run_with_budget_at_epoch(
            &root_run,
            &session.id,
            Some("query"),
            "fixture",
            &zk_db::TaskBudgetLimits {
                deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
                ..zk_db::TaskBudgetLimits::default()
            },
            1,
        )
        .await
        .unwrap();
        let runtime = TaskRuntime::new(db.clone(), Arc::new(Sink));
        let hooks = Arc::new(
            HookService::load_from_dir(&workspace)
                .with_admission(Arc::new(crate::hook::admission::TestHookAdmission)),
        );
        assert!(runtime.configure_hooks(hooks.clone()));
        assert!(runtime.configure_hook_supervisor(&ExecutionSupervisor::new(db.clone())));
        let lease = runtime
            .attach_existing_execution(&session.id, &root_run, &root_run, CancellationToken::new())
            .await
            .unwrap();
        let child_run = uuid::Uuid::new_v4().to_string();
        let child = db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: uuid::Uuid::new_v4().to_string(),
                run_id: child_run.clone(),
                root_session_id: session.id,
                transcript_session_id: uuid::Uuid::new_v4().to_string(),
                parent_task_id: Some(root_run.clone()),
                parent_run_id: Some(root_run.clone()),
                creator_tool_use_id: Some("create-child".into()),
                ordinal: 0,
                description: "child".into(),
                prompt: Some("work".into()),
                task_type: "agent".into(),
                model: "fixture".into(),
                working_dir: workspace.to_string_lossy().into_owned(),
                execution_config_json: serde_json::json!({"teamId":"team","lifecycle":"attached"})
                    .to_string(),
                startup_epoch: 1,
            })
            .await
            .unwrap();
        assert_eq!(
            db.claim_task_run_cas(&child.task.id, &child_run, child.task.version)
                .await
                .unwrap(),
            CasOutcome::Applied
        );
        let child = db
            .find_runtime_task_by_id(&child.task.id)
            .await
            .unwrap()
            .unwrap();
        Fixture {
            runtime,
            hooks,
            root_run,
            child,
            child_run,
            workspace,
            _lease: lease,
        }
    }
    async fn commit_child(fixture: &Fixture) {
        assert!(matches!(
            commit_outcome(
                &fixture.runtime.inner,
                &fixture.child.id,
                &fixture.child_run,
                &TaskExecutionResult::Complete("child result".into()),
                CleanupStatus::NotRequired
            )
            .await,
            TerminalCommitResult::Committed { .. }
        ));
    }
    async fn resources(fixture: &Fixture) -> Vec<(String, String, String)> {
        fixture
            .runtime
            .db()
            .with_reader(|conn| {
                let mut statement = conn.prepare(
                    "SELECT task_id,run_id,status FROM execution_resources ORDER BY resource_id",
                )?;
                Ok(statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                    .collect::<Result<Vec<_>, _>>()?)
            })
            .await
            .unwrap()
    }
    #[tokio::test]
    async fn admitted_completion_queues_before_parent_seal_and_drains_real_notifications() {
        let fixture = fixture().await;
        let notification = prepare(&fixture.runtime.inner, &fixture.child.id)
            .await
            .unwrap();
        let runtime = fixture.runtime.clone();
        let run = fixture.root_run.clone();
        let sealing = tokio::spawn(async move {
            runtime.seal_run_hook_notifications(&run).await;
        });
        tokio::task::yield_now().await;
        assert!(
            !sealing.is_finished(),
            "sealing waits for the in-progress child commit"
        );
        commit_child(&fixture).await;
        publish(Some(notification), &fixture.child, "child result").await;
        tokio::time::timeout(Duration::from_secs(2), sealing)
            .await
            .unwrap()
            .unwrap();
        fixture.hooks.drain_run(&fixture.root_run).await;
        assert_eq!(
            std::fs::read_to_string(fixture.workspace.join("completed.txt")).unwrap(),
            "done"
        );
        assert_eq!(
            std::fs::read_to_string(fixture.workspace.join("idle.txt")).unwrap(),
            "idle"
        );
        let actual = resources(&fixture).await;
        assert_eq!(actual.len(), 2);
        assert!(
            actual
                .iter()
                .all(|(task, run, status)| task == &fixture.root_run
                    && run == &fixture.root_run
                    && status == "released")
        );
        assert!(
            prepare(&fixture.runtime.inner, &fixture.child.id)
                .await
                .is_none()
        );
    }
    #[tokio::test]
    async fn parent_cancel_after_child_admission_prevents_actual_hook_side_effects() {
        let fixture = fixture().await;
        let notification = prepare(&fixture.runtime.inner, &fixture.child.id)
            .await
            .unwrap();
        commit_child(&fixture).await;
        let parent = fixture
            .runtime
            .db()
            .find_runtime_task_by_id(&fixture.root_run)
            .await
            .unwrap()
            .unwrap();
        assert!(
            super::super::persist_cancelling(
                &fixture.runtime.inner,
                &parent,
                zk_db::run::EXIT_USER_CANCELLED,
                "user stop"
            )
            .await
            .unwrap()
        );
        // Deliberately do not signal the in-memory token: the guarded SQLite
        // dispatch boundary must independently prevent the physical command.
        publish(Some(notification), &fixture.child, "child result").await;
        fixture
            .runtime
            .seal_run_hook_notifications(&fixture.root_run)
            .await;
        fixture.hooks.drain_run(&fixture.root_run).await;
        assert!(!fixture.workspace.join("completed.txt").exists());
        assert!(!fixture.workspace.join("idle.txt").exists());
        assert!(resources(&fixture).await.is_empty());
    }
    #[tokio::test]
    async fn dropped_failed_commit_admission_releases_parent_without_dispatch() {
        let fixture = fixture().await;
        let notification = prepare(&fixture.runtime.inner, &fixture.child.id)
            .await
            .unwrap();
        drop(notification);
        tokio::time::timeout(
            Duration::from_secs(2),
            fixture
                .runtime
                .seal_run_hook_notifications(&fixture.root_run),
        )
        .await
        .unwrap();
        assert!(
            prepare(&fixture.runtime.inner, &fixture.child.id)
                .await
                .is_none()
        );
        fixture.hooks.drain_run(&fixture.root_run).await;
        assert!(!fixture.workspace.join("completed.txt").exists());
        assert!(resources(&fixture).await.is_empty());
    }
    #[tokio::test]
    async fn timeout_durable_cause_wins_executor_cancel_race_without_usage_forgery() {
        let fixture = fixture().await;
        fixture
            .runtime
            .db()
            .start_llm_call_with_budget(
                &zk_db::NewLlmCall {
                    call_id: "cancelled-provider".into(),
                    task_id: fixture.child.id.clone(),
                    run_id: fixture.child_run.clone(),
                    provider: "fixture".into(),
                    model: "fixture".into(),
                    route: None,
                    provider_request_id: None,
                },
                &zk_db::LlmCallBudgetReservation {
                    input_tokens: 1,
                    output_tokens: 1,
                    cost_nanos_usd: 0,
                },
            )
            .await
            .unwrap();
        fixture
            .runtime
            .db()
            .finish_llm_call(
                "cancelled-provider",
                "cancelled",
                &zk_db::LlmUsageCompletion {
                    error_code: Some("STREAM_DROPPED".into()),
                    ..zk_db::LlmUsageCompletion::default()
                },
            )
            .await
            .unwrap();
        let run = fixture.child_run.clone();
        fixture
            .runtime
            .db()
            .with_writer(move |conn| {
                zk_db::run::request_cancel_in_current_write(conn, &run, zk_db::run::EXIT_TIMEOUT)?;
                Ok(())
            })
            .await
            .unwrap();
        let outcome = commit_outcome(
            &fixture.runtime.inner,
            &fixture.child.id,
            &fixture.child_run,
            &TaskExecutionResult::Cancelled {
                message: "provider cancelled".into(),
            },
            CleanupStatus::NotRequired,
        )
        .await;
        let TerminalCommitResult::Committed { result, .. } = outcome else {
            panic!("timeout outcome must commit");
        };
        assert_eq!(
            result.error_code.as_deref(),
            Some("SUBAGENT_DEADLINE_EXCEEDED")
        );
        assert_eq!(result.status, zk_db::ResultStatus::Error);
        let run = fixture
            .runtime
            .db()
            .find_run_by_id(&fixture.child_run)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.exit_reason.as_deref(), Some(zk_db::run::EXIT_TIMEOUT));
        assert!(
            !run.usage_complete,
            "cancelled provider usage remains unknown"
        );
        let physical: (bool, Option<i64>, Option<i64>, Option<i64>) = fixture.runtime.db().with_reader(|conn| {
            Ok(conn.query_row("SELECT usage_complete,input_tokens,output_tokens,cost_nanos_usd FROM llm_calls WHERE call_id='cancelled-provider'", [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)))?)
        }).await.unwrap();
        assert_eq!(physical, (false, None, None, None));
    }
}
