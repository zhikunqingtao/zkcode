//! Session-owned interpreter services reuse `TaskRuntime` and its setup/resource ledger.
use futures::future::BoxFuture;
use rusqlite::OptionalExtension;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fmt,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tokio_util::sync::CancellationToken;
use zk_db::{Db, content::ContentRetention};
use zk_engine::{
    ExecutionSupervisor, ExternalRootSubmission, TaskExecutionResult, TaskRuntime,
    run_tool_scopes::RunToolScopes,
};
use zk_tools::{RunToolScope, RunToolScopeFactory, Tool, ToolContext, ToolOutput, ToolRegistry};

const IDLE: Duration = Duration::from_mins(10);
const LIFETIME: Duration = Duration::from_hours(1);
struct Entry {
    session: String,
    task: String,
    run: String,
    handle: zk_tools::repl::ReplServiceHandle,
    cancel: CancellationToken,
    stopping: AtomicBool,
    closed: AtomicBool,
    cleanup_failed: AtomicBool,
    active: RwLock<()>,
    seen: Mutex<(Instant, i64)>,
    scopes: Arc<RunToolScopes>,
}
impl Entry {
    fn touch(&self) {
        *self
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            (Instant::now(), crate::iso::now_millis());
    }
}
type Slot = Arc<AsyncMutex<Option<Arc<Entry>>>>;
/// The host owns services; a query scope only borrows an authorized Session handle.
pub(crate) struct ReplServices {
    db: Db,
    runtime: Arc<TaskRuntime>,
    supervisor: Arc<ExecutionSupervisor>,
    epoch: Arc<AtomicI64>,
    slots: Mutex<HashMap<String, Slot>>,
}
impl fmt::Debug for ReplServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplServices").finish_non_exhaustive()
    }
}
impl ReplServices {
    pub(crate) fn new(
        db: Db,
        runtime: Arc<TaskRuntime>,
        supervisor: Arc<ExecutionSupervisor>,
        epoch: Arc<AtomicI64>,
    ) -> Self {
        Self {
            db,
            runtime,
            supervisor,
            epoch,
            slots: Mutex::new(HashMap::new()),
        }
    }
    fn slot(&self, session: &str) -> Slot {
        self.slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(session.into())
            .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
            .clone()
    }
    async fn get_or_start(self: &Arc<Self>, session: &str) -> Result<Arc<Entry>, String> {
        if self
            .db
            .session_retention(session)
            .await
            .map_err(|_| "REPL_SESSION_UNAVAILABLE")?
            != ContentRetention::Persistent
        {
            return Err("REPL_PERSISTENT_SERVICE_REQUIRED".into());
        }
        let slot = self.slot(session);
        let mut current = slot.lock().await;
        if let Some(entry) = current.as_ref() {
            if !entry.stopping.load(Ordering::Acquire)
                && !entry.closed.load(Ordering::Acquire)
                && !entry.cancel.is_cancelled()
            {
                return Ok(entry.clone());
            }
            let task = self
                .db
                .find_runtime_task_by_id(&entry.task)
                .await
                .map_err(|_| "REPL_SERVICE_STATUS_UNAVAILABLE")?
                .ok_or("REPL_SERVICE_TASK_MISSING")?;
            if !entry.closed.load(Ordering::Acquire)
                || !task.status.is_terminal()
                || !matches!(
                    task.cleanup_status,
                    zk_db::CleanupStatus::Confirmed | zk_db::CleanupStatus::NotRequired
                )
            {
                return Err("REPL_SERVICE_STOPPING".into());
            }
            current.take();
        }
        let stored = self
            .db
            .get_session(session)
            .await
            .map_err(|_| "REPL_SESSION_UNAVAILABLE")?
            .ok_or("REPL_SESSION_UNAVAILABLE")?;
        let workspace = PathBuf::from(stored.working_dir)
            .canonicalize()
            .map_err(|_| "REPL_WORKSPACE_UNAVAILABLE")?;
        let factory = Arc::new(zk_tools::repl::ReplServiceScopeFactory::new(session.into()));
        let scopes = Arc::new(RunToolScopes::new(vec![factory.clone()]));
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let host = self.clone();
        let startup = CancellationToken::new();
        let mut startup_guard = StartupGuard {
            token: Some(startup.clone()),
            run: None,
        };
        let receipt = self
            .runtime
            .submit_repl_service(
                ExternalRootSubmission {
                    session_id: session.into(),
                    startup_epoch: self.epoch.load(Ordering::Acquire),
                    timeout: LIFETIME,
                    // Native-only service has no model route or billable helper.
                    budget: zk_db::TaskBudgetLimits {
                        token_limit: Some(1),
                        cost_limit_nanos_usd: Some(1),
                        deadline_at_ms: None,
                    },
                },
                move |execution| async move {
                    host.drive(execution, workspace, scopes, factory, ready_tx, startup)
                        .await
                },
            )
            .await
            .map_err(|_| "REPL_SERVICE_ADMISSION_FAILED")?;
        startup_guard.run = Some((self.runtime.clone(), receipt.run_id));
        let entry = tokio::time::timeout(Duration::from_secs(30), ready_rx)
            .await
            .map_err(|_| "REPL_SERVICE_START_TIMEOUT")?
            .map_err(|_| "REPL_SERVICE_START_INTERRUPTED")?
            .map_err(str::to_owned)?;
        startup_guard.token.take();
        startup_guard.run.take();
        *current = Some(entry.clone());
        Ok(entry)
    }
    async fn drive(
        self: Arc<Self>,
        execution: zk_engine::TaskExecutionContext,
        workspace: PathBuf,
        scopes: Arc<RunToolScopes>,
        factory: Arc<zk_tools::repl::ReplServiceScopeFactory>,
        ready: tokio::sync::oneshot::Sender<Result<Arc<Entry>, &'static str>>,
        startup: CancellationToken,
    ) -> TaskExecutionResult {
        let base = Arc::new(ToolRegistry::new());
        base.register_dynamic(Arc::new(zk_tools::REPLTool::new(Arc::new(
            zk_tools::ReplManager::new(),
        ))));
        let prepared = scopes
            .prepare_for_execution(&self.db, &self.supervisor, &execution, &workspace, base)
            .await;
        let handle = prepared.and_then(|_| factory.prepared_handle());
        let Ok(handle) = handle else {
            let _ = ready.send(Err("REPL_SERVICE_SETUP_FAILED"));
            let _ = scopes.cleanup(&self.db, &execution.run_id).await;
            return TaskExecutionResult::failed("REPL_SERVICE_SETUP_FAILED");
        };
        let entry = Arc::new(Entry {
            session: execution.root_session_id.clone(),
            task: execution.task_id.clone(),
            run: execution.run_id.clone(),
            handle,
            cancel: execution.cancel.child_token(),
            stopping: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
            active: RwLock::new(()),
            seen: Mutex::new((Instant::now(), crate::iso::now_millis())),
            scopes,
        });
        if ready.send(Ok(entry.clone())).is_ok() {
            loop {
                tokio::select! {
                    ()=entry.cancel.cancelled()=>break,
                    ()=startup.cancelled()=>break,
                    ()=tokio::time::sleep(Duration::from_secs(5))=>{},
                }
                if let Ok(_idle) = entry.active.try_write()
                    && entry
                        .seen
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .0
                        .elapsed()
                        >= IDLE
                {
                    // Hold the exclusive activity lease while publishing stop.
                    // A just-arrived caller cannot race the idle decision.
                    entry.stopping.store(true, Ordering::Release);
                    break;
                }
            }
        }
        entry.stopping.store(true, Ordering::Release);
        entry.cancel.cancel();
        let _drained = entry.active.write().await;
        let mut clean = false;
        for attempt in 0..3 {
            if entry.scopes.cleanup(&self.db, &entry.run).await.is_ok() {
                clean = true;
                break;
            }
            if attempt < 2 {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        entry.cleanup_failed.store(!clean, Ordering::Release);
        entry.closed.store(clean, Ordering::Release);
        if clean {
            TaskExecutionResult::complete("REPL service ended; interpreter resources released")
        } else {
            TaskExecutionResult::failed("REPL_SERVICE_CLEANUP_UNCONFIRMED")
        }
    }

    async fn execute(
        self: &Arc<Self>,
        session: &str,
        input: Value,
        context: ToolContext,
    ) -> ToolOutput {
        if context.session_id() != Some(session) || context.is_ephemeral() {
            return ToolOutput::error("REPL_SERVICE_SCOPE_MISMATCH");
        }
        let entry = match self.get_or_start(session).await {
            Ok(entry) => entry,
            Err(error) => return ToolOutput::error(error),
        };
        let _active = entry.active.read().await;
        if entry.stopping.load(Ordering::Acquire) || entry.cancel.is_cancelled() {
            return ToolOutput::error("REPL_SERVICE_STOPPING");
        }
        entry.touch();
        let mut context = context;
        let request_cancel = context.cancel.child_token();
        context.cancel = request_cancel.clone();
        let service_cancel = entry.cancel.clone();
        let watcher = tokio::spawn(async move {
            service_cancel.cancelled().await;
            request_cancel.cancel();
        });
        let _watcher = AbortTask(watcher);
        let output = entry.handle.execute_authorized(input, context).await;
        entry.touch();
        output
    }
    pub(crate) async fn status(&self, session: &str) -> Result<Value, String> {
        let slot = self
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session)
            .cloned();
        let Some(slot) = slot else {
            return self.persisted_status(session).await;
        };
        let current = slot.lock().await;
        let Some(entry) = current.as_ref() else {
            return self.persisted_status(session).await;
        };
        let run = self
            .db
            .find_run_by_id(&entry.run)
            .await
            .map_err(|_| "REPL_SERVICE_STATUS_UNAVAILABLE")?
            .ok_or("REPL_SERVICE_RUN_MISSING")?;
        let closed = entry.closed.load(Ordering::Acquire);
        let failed =
            entry.cleanup_failed.load(Ordering::Acquire) || run.cleanup_status == "unconfirmed";
        let state = if failed {
            "cleanupUnconfirmed"
        } else if closed
            && run.finished_at.is_some()
            && matches!(run.cleanup_status.as_str(), "confirmed" | "notRequired")
        {
            "stopped"
        } else if entry.stopping.load(Ordering::Acquire) || entry.cancel.is_cancelled() {
            "stopping"
        } else {
            "running"
        };
        let last = entry
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .1;
        Ok(
            json!({"sessionId":entry.session,"taskId":entry.task,"runId":entry.run,"state":state,"cleanupStatus":if failed{"unconfirmed"}else if state=="stopped"{"confirmed"}else{"pending"},"idleTimeoutSeconds":IDLE.as_secs(),"maxLifetimeSeconds":LIFETIME.as_secs(),"lastActivityAt":crate::iso::format_rfc3339_micros(last)}),
        )
    }
    async fn persisted_status(&self, session: &str) -> Result<Value, String> {
        let session_id = session.to_owned();
        let stored=self.db.with_reader(move |connection| {
            connection.query_row("SELECT t.id,r.id,r.finished_at,r.cleanup_status FROM tasks t JOIN run_envelopes r ON r.id=t.current_run_id WHERE t.session_id=?1 AND t.task_type='repl' ORDER BY t.created_at DESC,t.rowid DESC LIMIT 1",[session_id], |row|Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,Option<String>>(2)?,row.get::<_,String>(3)?))).optional().map_err(Into::into)
        }).await.map_err(|_| "REPL_SERVICE_STATUS_UNAVAILABLE")?;
        let Some((task, run, finished, cleanup)) = stored else {
            return Ok(
                json!({"sessionId":session,"state":"absent","cleanupStatus":"notRequired","idleTimeoutSeconds":IDLE.as_secs(),"maxLifetimeSeconds":LIFETIME.as_secs()}),
            );
        };
        let state = if finished.is_some() && matches!(cleanup.as_str(), "confirmed" | "notRequired")
        {
            "stopped"
        } else if cleanup == "unconfirmed" {
            "cleanupUnconfirmed"
        } else {
            "stopping"
        };
        Ok(
            json!({"sessionId":session,"taskId":task,"runId":run,"state":state,"cleanupStatus":cleanup,"idleTimeoutSeconds":IDLE.as_secs(),"maxLifetimeSeconds":LIFETIME.as_secs()}),
        )
    }
    pub(crate) async fn stop(&self, session: &str) -> Result<Value, String> {
        let slot = self
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session)
            .cloned();
        let entry = match slot {
            Some(slot) => slot.lock().await.clone(),
            None => None,
        };
        let run_id = if let Some(entry) = entry.as_ref() {
            // Stop local work before attempting any persistence write.
            entry.stopping.store(true, Ordering::Release);
            entry.cancel.cancel();
            Some(entry.run.clone())
        } else {
            self.persisted_status(session).await?["runId"]
                .as_str()
                .map(str::to_owned)
        };
        if let Some(run_id) = run_id {
            let run = self
                .db
                .find_run_by_id(&run_id)
                .await
                .map_err(|_| "REPL_SERVICE_STATUS_UNAVAILABLE")?
                .ok_or("REPL_SERVICE_RUN_MISSING")?;
            if run.finished_at.is_none() {
                self.runtime
                    .cancel_run_with_cause(
                        &run_id,
                        zk_db::run::EXIT_USER_CANCELLED,
                        "REPL service stopped by user",
                    )
                    .await
                    .map_err(|_| "REPL_SERVICE_CANCEL_STORAGE_FAILED")?;
            } else if !matches!(run.cleanup_status.as_str(), "confirmed" | "notRequired") {
                if let Some(entry) = entry.as_ref() {
                    let _drained = entry.active.write().await;
                    let clean = entry.scopes.cleanup(&self.db, &entry.run).await.is_ok();
                    entry.cleanup_failed.store(!clean, Ordering::Release);
                    entry.closed.store(clean, Ordering::Release);
                }
                // This port checks all retained resource and invocation facts;
                // absence of an in-memory service owner never invents release.
                self.runtime
                    .retry_confirmed_cleanup(&run_id)
                    .await
                    .map_err(|_| "REPL_SERVICE_CLEANUP_RETRY_FAILED")?;
            }
        }
        self.status(session).await
    }
}
struct StartupGuard {
    token: Option<CancellationToken>,
    run: Option<(Arc<TaskRuntime>, String)>,
}
impl Drop for StartupGuard {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            token.cancel();
        }
        if let Some((runtime, run)) = self.run.take()
            && let Ok(executor) = tokio::runtime::Handle::try_current()
        {
            executor.spawn(async move {
                let _ = runtime
                    .cancel_run_with_cause(
                        &run,
                        zk_db::run::EXIT_INTERNAL_ERROR,
                        "REPL service startup abandoned",
                    )
                    .await;
            });
        }
    }
}
struct AbortTask(tokio::task::JoinHandle<()>);
impl Drop for AbortTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Run-local authorization binding that borrows, but never terminates, the Session service.
#[derive(Debug)]
pub(crate) struct ReplServiceBridgeFactory(pub(crate) Arc<ReplServices>);
struct BorrowedScope(Arc<ToolRegistry>);
impl RunToolScope for BorrowedScope {
    fn registry(&self) -> Arc<ToolRegistry> {
        self.0.clone()
    }
    fn cleanup(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}
impl RunToolScopeFactory for ReplServiceBridgeFactory {
    fn prepare(
        &self,
        context: ToolContext,
        base: Arc<ToolRegistry>,
    ) -> BoxFuture<'_, Result<Arc<dyn RunToolScope>, String>> {
        Box::pin(async move {
            if context.is_ephemeral() {
                return Ok(Arc::new(BorrowedScope(base)) as Arc<dyn RunToolScope>);
            }
            let Some(binding) = base.resolve("REPL") else {
                return Ok(Arc::new(BorrowedScope(base)) as Arc<dyn RunToolScope>);
            };
            let session = context
                .session_id()
                .ok_or("REPL_SESSION_REQUIRED")?
                .to_owned();
            let tool = Arc::new(BridgeTool {
                services: self.0.clone(),
                source: binding.tool(),
                session,
            });
            Ok(Arc::new(BorrowedScope(Arc::new(ToolRegistry::adapt_bound(
                base, binding, tool,
            )?))) as Arc<dyn RunToolScope>)
        })
    }
}
struct BridgeTool {
    services: Arc<ReplServices>,
    source: Arc<dyn Tool>,
    session: String,
}
impl Tool for BridgeTool {
    fn name(&self) -> &'static str {
        "REPL"
    }
    fn description(&self) -> &str {
        self.source.description()
    }
    fn parameters(&self) -> Value {
        self.source.parameters()
    }
    fn timeout(&self) -> Duration {
        self.source.timeout()
    }
    fn child_access(&self) -> zk_tools::ChildToolAccess {
        self.source.child_access()
    }
    fn is_destructive(&self, input: &Value) -> bool {
        self.source.is_destructive(input)
    }
    fn execute(&self, input: Value, context: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { self.services.execute(&self.session, input, context).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AppState;

    fn context(session: &str, run: &str, workspace: &std::path::Path) -> ToolContext {
        let (sender, _) = tokio::sync::mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), sender)
            .with_session_id(session)
            .with_run_id(run)
            .with_working_dir(workspace)
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "One real interpreter lifecycle verifies cross-Run state, exact resource ownership and final cleanup together"
    )]
    async fn persistent_service_keeps_state_between_query_runs_then_stops_with_real_ownership() {
        let root = std::env::temp_dir().join(format!("zk-repl-service-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let state = AppState::for_tests();
        let session = state
            .db
            .create_session("fixture", root.to_str().unwrap())
            .await
            .unwrap();
        let first = context(&session.id, "first-query", &root);
        let result = state
            .repl_services
            .execute(
                &session.id,
                json!({"language":"python","code":"answer=40\nanswer+2","sessionId":"console"}),
                first.clone(),
            )
            .await;
        assert!(!result.is_error, "{}", result.content);
        assert_eq!(result.content.trim(), "42");
        first.cancel.cancel();
        let second = context(&session.id, "second-query", &root);
        let result = state
            .repl_services
            .execute(
                &session.id,
                json!({"language":"python","code":"answer+3","session_id":"console"}),
                second,
            )
            .await;
        assert!(!result.is_error, "{}", result.content);
        assert_eq!(result.content.trim(), "43");
        let current = state.repl_services.status(&session.id).await.unwrap();
        assert_eq!(current["state"], "running");
        let run = state
            .db
            .find_run_by_id(current["runId"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            run.session_id, session.id,
            "service transcript must not be the user conversation"
        );
        let task = state
            .db
            .find_runtime_task_by_id(current["taskId"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.session_id, session.id);
        assert_eq!(task.task_type, "repl");
        let run_id = run.id.clone();
        let records = state
            .db
            .with_reader(move |conn| {
                let mut statement = conn.prepare(
                    "SELECT resource_kind,external_id FROM execution_resources WHERE run_id=?1",
                )?;
                Ok(statement
                    .query_map([run_id], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?)
            })
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, "processGroup");
        let pid: i32 = records[0].1.as_ref().unwrap().parse().unwrap();
        let wrong = context("other-session", "third-query", &root);
        assert!(
            state
                .repl_services
                .execute(
                    &session.id,
                    json!({"code":"answer","sessionId":"console"}),
                    wrong
                )
                .await
                .is_error
        );
        let requested = state.repl_services.stop(&session.id).await.unwrap();
        assert!(matches!(
            requested["state"].as_str(),
            Some("stopping" | "stopped")
        ));
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let status = state.repl_services.status(&session.id).await.unwrap();
                if status["state"] == "stopped" {
                    assert_eq!(status["cleanupStatus"], "confirmed");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            nix::sys::signal::killpg(nix::unistd::Pid::from_raw(pid), None),
            Err(nix::errno::Errno::ESRCH)
        );
        assert_eq!(
            std::fs::read_dir(&root).unwrap().count(),
            0,
            "automatic REPL source/history/log file appeared"
        );
        state
            .execution_supervisor
            .shutdown(Duration::from_secs(5))
            .await;
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn retained_service_reconciles_a_failed_release_without_changing_its_result() {
        let root = std::env::temp_dir().join(format!("zk-repl-retry-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let state = AppState::for_tests();
        let session = state
            .db
            .create_session("fixture", root.to_str().unwrap())
            .await
            .unwrap();
        let output = state
            .repl_services
            .execute(
                &session.id,
                json!({"code":"42","sessionId":"console"}),
                context(&session.id, "first-query", &root),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        let run = state.repl_services.status(&session.id).await.unwrap()["runId"]
            .as_str()
            .unwrap()
            .to_owned();
        state.db.with_writer(|connection|{connection.execute_batch("CREATE TRIGGER fail_repl_release BEFORE UPDATE OF status ON execution_resources WHEN NEW.status='released' BEGIN SELECT RAISE(ABORT,'injected release failure'); END;")?;Ok(())}).await.unwrap();
        state.repl_services.stop(&session.id).await.unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if state
                    .db
                    .find_run_by_id(&run)
                    .await
                    .unwrap()
                    .unwrap()
                    .finished_at
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            state.repl_services.status(&session.id).await.unwrap()["state"],
            "cleanupUnconfirmed"
        );
        let read_result = |run: String| {
            move |connection:&mut rusqlite::Connection|->Result<Vec<(String,String,Option<String>)>,zk_db::DbError>{
            let mut query=connection.prepare("SELECT result_id,status,content_sha256 FROM task_results WHERE run_id=?1 ORDER BY result_version")?;
            Ok(query.query_map([run],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?.collect::<Result<Vec<_>,_>>()?)
        }
        };
        let before = state
            .db
            .with_reader(read_result(run.clone()))
            .await
            .unwrap();
        assert!(
            !before.is_empty(),
            "cleanup failure must retain an immutable terminal result"
        );
        state
            .db
            .with_writer(|connection| {
                connection.execute_batch("DROP TRIGGER fail_repl_release;")?;
                Ok(())
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let status = state.repl_services.stop(&session.id).await.unwrap();
                if status["state"] == "stopped" {
                    assert_eq!(status["cleanupStatus"], "confirmed");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            before,
            state.db.with_reader(read_result(run)).await.unwrap()
        );
        let reloaded = ReplServices::new(
            state.db.clone(),
            state.task_runtime.clone(),
            state.execution_supervisor.clone(),
            Arc::new(AtomicI64::new(state.startup_epoch())),
        );
        assert_eq!(
            reloaded.status(&session.id).await.unwrap()["state"],
            "stopped"
        );
        state
            .execution_supervisor
            .shutdown(Duration::from_secs(5))
            .await;
        std::fs::remove_dir_all(root).unwrap();
    }
}
