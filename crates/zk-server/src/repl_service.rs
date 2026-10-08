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
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, RwLock};
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
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Starting,
    Running,
    Stopping,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum StopCause {
    User,
    StartupTimeout,
    StartupFailed,
}
impl StopCause {
    fn reason(self) -> &'static str {
        match self {
            Self::User => zk_db::run::EXIT_USER_CANCELLED,
            Self::StartupTimeout => zk_db::run::EXIT_TIMEOUT,
            Self::StartupFailed => zk_db::run::EXIT_INTERNAL_ERROR,
        }
    }
    fn message(self) -> &'static str {
        match self {
            Self::User => "REPL service stopped by user",
            Self::StartupTimeout => "REPL service startup timed out",
            Self::StartupFailed => "REPL service startup failed",
        }
    }
    fn code(self) -> &'static str {
        match self {
            Self::User => "REPL_SERVICE_STOPPING",
            Self::StartupTimeout => "REPL_SERVICE_START_TIMEOUT",
            Self::StartupFailed => "REPL_SERVICE_START_INTERRUPTED",
        }
    }
}
struct GenerationState {
    phase: Phase,
    entry: Option<Arc<Entry>>,
    identity: Option<(String, String)>,
    result: Option<Result<Arc<Entry>, String>>,
    cause: Option<StopCause>,
    startup_finished: bool,
}
struct Generation {
    id: u64,
    state: Mutex<GenerationState>,
    cancel: CancellationToken,
    ready: Notify,
}
impl Generation {
    fn new(id: u64) -> Self {
        Self {
            id,
            state: Mutex::new(GenerationState {
                phase: Phase::Starting,
                entry: None,
                identity: None,
                result: None,
                cause: None,
                startup_finished: false,
            }),
            cancel: CancellationToken::new(),
            ready: Notify::new(),
        }
    }
    fn request_stop(&self, cause: StopCause) -> Option<String> {
        let run = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if cause == StopCause::StartupTimeout && state.phase != Phase::Starting {
                return None;
            }
            if state.cause.is_none() {
                state.cause = Some(cause);
            }
            state.phase = Phase::Stopping;
            if state.result.is_none() {
                state.result = Some(Err(state.cause.unwrap_or(cause).code().into()));
            }
            if let Some(entry) = &state.entry {
                entry.stopping.store(true, Ordering::Release);
                entry.cancel.cancel();
            }
            state.identity.as_ref().map(|(_, run)| run.clone())
        };
        self.cancel.cancel();
        self.ready.notify_waiters();
        run
    }
    async fn wait(&self) -> Result<Arc<Entry>, String> {
        loop {
            let notified = self.ready.notified();
            if let Some(result) = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .result
                .clone()
            {
                return result;
            }
            notified.await;
        }
    }
}
type Slot = Arc<Mutex<Option<Arc<Generation>>>>;
/// The host owns services; a query scope only borrows an authorized Session handle.
pub(crate) struct ReplServices {
    db: Db,
    runtime: Arc<TaskRuntime>,
    supervisor: Arc<ExecutionSupervisor>,
    epoch: Arc<AtomicI64>,
    slots: Mutex<HashMap<String, Slot>>,
    next_generation: AtomicU64,
    #[cfg(test)]
    startup_gate: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    #[cfg(test)]
    admitted_gate: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
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
            next_generation: AtomicU64::new(1),
            #[cfg(test)]
            startup_gate: Mutex::new(None),
            #[cfg(test)]
            admitted_gate: Mutex::new(None),
        }
    }
    fn slot(&self, session: &str) -> Slot {
        self.slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(session.into())
            .or_insert_with(|| Arc::new(Mutex::new(None)))
            .clone()
    }
    async fn get_or_start(self: &Arc<Self>, session: &str) -> Result<Arc<Entry>, String> {
        let slot = self.slot(session);
        loop {
            let (generation, created) = {
                let mut current = slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(generation) = current.as_ref() {
                    (generation.clone(), false)
                } else {
                    let generation = Arc::new(Generation::new(
                        self.next_generation.fetch_add(1, Ordering::Relaxed),
                    ));
                    *current = Some(generation.clone());
                    (generation, true)
                }
            };
            if created {
                let host = self.clone();
                let session = session.to_owned();
                let worker_generation = generation.clone();
                tokio::spawn(async move {
                    host.start_generation(session, worker_generation).await;
                });
                return generation.wait().await;
            }
            let (phase, entry, identity, finished) = {
                let state = generation
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (
                    state.phase,
                    state.entry.clone(),
                    state.identity.clone(),
                    state.startup_finished,
                )
            };
            match phase {
                Phase::Starting => return generation.wait().await,
                Phase::Running => {
                    if let Some(entry) = entry
                        && !entry.stopping.load(Ordering::Acquire)
                        && !entry.cancel.is_cancelled()
                    {
                        return Ok(entry);
                    }
                    return Err("REPL_SERVICE_STOPPING".into());
                }
                Phase::Stopping => {
                    if !finished {
                        return Err("REPL_SERVICE_STOPPING".into());
                    }
                    if let Some((task, _)) = identity {
                        let stored = self
                            .db
                            .find_runtime_task_by_id(&task)
                            .await
                            .map_err(|_| "REPL_SERVICE_STATUS_UNAVAILABLE")?
                            .ok_or("REPL_SERVICE_TASK_MISSING")?;
                        if !stored.status.is_terminal()
                            || !matches!(
                                stored.cleanup_status,
                                zk_db::CleanupStatus::Confirmed | zk_db::CleanupStatus::NotRequired
                            )
                            || entry.is_some_and(|entry| !entry.closed.load(Ordering::Acquire))
                        {
                            return Err("REPL_SERVICE_STOPPING".into());
                        }
                    }
                    let mut current = slot
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if current
                        .as_ref()
                        .is_some_and(|present| present.id == generation.id)
                    {
                        current.take();
                    }
                }
            }
        }
    }
    async fn start_generation(self: Arc<Self>, session: String, generation: Arc<Generation>) {
        let timer_generation = generation.clone();
        let runtime = self.runtime.clone();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let starting = timer_generation
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .phase
                == Phase::Starting;
            if starting && let Some(run) = timer_generation.request_stop(StopCause::StartupTimeout)
            {
                let _ = runtime
                    .cancel_run_with_cause(
                        &run,
                        zk_db::run::EXIT_TIMEOUT,
                        "REPL service startup timed out",
                    )
                    .await;
            }
        });
        let result = self.prepare_generation(&session, generation.clone()).await;
        timer.abort();
        let late_run = {
            let mut state = generation
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.startup_finished = true;
            match result {
                Ok(entry)
                    if state.phase == Phase::Starting && !generation.cancel.is_cancelled() =>
                {
                    state.phase = Phase::Running;
                    state.entry = Some(entry.clone());
                    state.result = Some(Ok(entry));
                    None
                }
                Ok(entry) => {
                    entry.stopping.store(true, Ordering::Release);
                    entry.cancel.cancel();
                    state.entry = Some(entry);
                    state.phase = Phase::Stopping;
                    state.identity.as_ref().map(|(_, run)| run.clone())
                }
                Err(error) => {
                    state.phase = Phase::Stopping;
                    if state.result.is_none() {
                        state.result = Some(Err(error));
                    }
                    if state.cause.is_none() {
                        state.cause = Some(StopCause::StartupFailed);
                    }
                    state.identity.as_ref().map(|(_, run)| run.clone())
                }
            }
        };
        generation.ready.notify_waiters();
        if let Some(run) = late_run {
            generation.cancel.cancel();
            let cause = generation
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .cause
                .unwrap_or(StopCause::StartupFailed);
            if let Err(error) = self
                .runtime
                .cancel_run_with_cause(&run, cause.reason(), cause.message())
                .await
            {
                tracing::error!(%run,%error,"REPL startup cancellation requires reconciliation");
            }
        }
    }
    async fn prepare_generation(
        self: &Arc<Self>,
        session: &str,
        generation: Arc<Generation>,
    ) -> Result<Arc<Entry>, String> {
        #[cfg(test)]
        {
            let gate = self.startup_gate.lock().unwrap().clone();
            if let Some((entered, release)) = gate {
                entered.notify_one();
                release.notified().await;
            }
        }
        if generation.cancel.is_cancelled() {
            return Err("REPL_SERVICE_STOPPING".into());
        }
        if self
            .db
            .session_retention(session)
            .await
            .map_err(|_| "REPL_SESSION_UNAVAILABLE")?
            != ContentRetention::Persistent
        {
            return Err("REPL_PERSISTENT_SERVICE_REQUIRED".into());
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
        if generation.cancel.is_cancelled() {
            return Err("REPL_SERVICE_STOPPING".into());
        }
        let factory = Arc::new(zk_tools::repl::ReplServiceScopeFactory::new(session.into()));
        let scopes = Arc::new(RunToolScopes::new(vec![factory.clone()]));
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let host = self.clone();
        let driver_generation = generation.clone();
        let receipt = self
            .runtime
            .submit_repl_service(
                ExternalRootSubmission {
                    session_id: session.into(),
                    startup_epoch: self.epoch.load(Ordering::Acquire),
                    timeout: LIFETIME,
                    budget: zk_db::TaskBudgetLimits {
                        token_limit: Some(1),
                        cost_limit_nanos_usd: Some(1),
                        deadline_at_ms: None,
                    },
                },
                move |execution| async move {
                    host.drive(
                        execution,
                        workspace,
                        scopes,
                        factory,
                        ready_tx,
                        driver_generation,
                    )
                    .await
                },
            )
            .await
            .map_err(|_| "REPL_SERVICE_ADMISSION_FAILED")?;
        #[cfg(test)]
        {
            let gate = self.admitted_gate.lock().unwrap().clone();
            if let Some((entered, release)) = gate {
                entered.notify_one();
                release.notified().await;
            }
        }
        {
            let mut state = generation
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.identity = Some((receipt.task.id, receipt.run_id.clone()));
        }
        if generation.cancel.is_cancelled() {
            return Err("REPL_SERVICE_STOPPING".into());
        }
        tokio::select! {
            result=ready_rx=>result.map_err(|_|"REPL_SERVICE_START_INTERRUPTED")?.map_err(str::to_owned),
            ()=generation.cancel.cancelled()=>Err("REPL_SERVICE_STOPPING".into()),
        }
    }
    #[allow(
        clippy::too_many_lines,
        reason = "The service owner must keep startup publication, first stop cause and interpreter cleanup together through its terminal result"
    )]
    async fn drive(
        self: Arc<Self>,
        execution: zk_engine::TaskExecutionContext,
        workspace: PathBuf,
        scopes: Arc<RunToolScopes>,
        factory: Arc<zk_tools::repl::ReplServiceScopeFactory>,
        ready: tokio::sync::oneshot::Sender<Result<Arc<Entry>, &'static str>>,
        generation: Arc<Generation>,
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
        // Keep the cleanup handle even when startup is stopped before its ready reply is consumed.
        generation
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry = Some(entry.clone());
        if ready.send(Ok(entry.clone())).is_ok() {
            loop {
                tokio::select! {
                    ()=entry.cancel.cancelled()=>break,
                    ()=generation.cancel.cancelled()=>break,
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
        let was_cancelled = execution.cancel.is_cancelled()
            || generation.cancel.is_cancelled()
            || entry.cancel.is_cancelled();
        let cause = generation
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cause;
        // The driver knows its Run before the admission receipt reaches the
        // starter. Persist that first cause before publishing any terminal result.
        let cancel_storage_failed = if let Some(cause) = cause {
            self.runtime
                .cancel_run_with_cause(&execution.run_id, cause.reason(), cause.message())
                .await
                .is_err()
        } else {
            false
        };
        generation
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase = Phase::Stopping;
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
        if clean && cancel_storage_failed {
            TaskExecutionResult::failed("REPL_SERVICE_CANCEL_STORAGE_FAILED")
        } else if clean && was_cancelled {
            TaskExecutionResult::Cancelled {
                message: "REPL service stopped; interpreter resources released".into(),
            }
        } else if clean {
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
        let generation = slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(generation) = generation else {
            return self.persisted_status(session).await;
        };
        let (phase, entry, identity, finished) = {
            let state = generation
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                state.phase,
                state.entry.clone(),
                state.identity.clone(),
                state.startup_finished,
            )
        };
        if phase == Phase::Starting {
            return Ok(
                json!({"sessionId":session,"generation":generation.id,"taskId":identity.as_ref().map(|(task,_)|task),"runId":identity.as_ref().map(|(_,run)|run),"state":"starting","cleanupStatus":"pending","idleTimeoutSeconds":IDLE.as_secs(),"maxLifetimeSeconds":LIFETIME.as_secs()}),
            );
        }
        let Some(entry) = entry else {
            if phase == Phase::Starting || !finished {
                return Ok(
                    json!({"sessionId":session,"generation":generation.id,"taskId":identity.as_ref().map(|(task,_)|task),"runId":identity.as_ref().map(|(_,run)|run),"state":if phase==Phase::Starting{"starting"}else{"stopping"},"cleanupStatus":"pending","idleTimeoutSeconds":IDLE.as_secs(),"maxLifetimeSeconds":LIFETIME.as_secs()}),
                );
            }
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
        } else if phase == Phase::Stopping
            || entry.stopping.load(Ordering::Acquire)
            || entry.cancel.is_cancelled()
        {
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
        let generation = slot.and_then(|slot| {
            slot.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        });
        let (entry, run_id, cause) = if let Some(generation) = generation {
            let (starting, entry) = {
                let state = generation
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (
                    state.phase == Phase::Starting || !state.startup_finished,
                    state.entry.clone(),
                )
            };
            let run = generation.request_stop(StopCause::User);
            let cause = generation
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .cause
                .unwrap_or(StopCause::User);
            if starting {
                if let Some(run) = run.clone() {
                    let runtime = self.runtime.clone();
                    tokio::spawn(async move {
                        if let Err(error) = runtime
                            .cancel_run_with_cause(&run, cause.reason(), cause.message())
                            .await
                        {
                            tracing::error!(%run,%error,"REPL startup cancellation requires reconciliation");
                        }
                    });
                }
                return Ok(
                    json!({"sessionId":session,"generation":generation.id,"runId":run,"state":"stopping","cleanupStatus":"pending","idleTimeoutSeconds":IDLE.as_secs(),"maxLifetimeSeconds":LIFETIME.as_secs()}),
                );
            }
            (entry, run, cause)
        } else {
            (
                None,
                self.persisted_status(session).await?["runId"]
                    .as_str()
                    .map(str::to_owned),
                StopCause::User,
            )
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
                    .cancel_run_with_cause(&run_id, cause.reason(), cause.message())
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
    async fn repl_starting_is_observable_and_stoppable_before_admission_finishes() {
        let state = AppState::for_tests();
        let session = state.db.create_session("fixture", "/tmp").await.unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *state.repl_services.startup_gate.lock().unwrap() =
            Some((entered.clone(), release.clone()));
        let host = state.repl_services.clone();
        let session_id = session.id.clone();
        let start = tokio::spawn(async move { host.get_or_start(&session_id).await });
        entered.notified().await;
        let status = tokio::time::timeout(
            Duration::from_millis(200),
            state.repl_services.status(&session.id),
        )
        .await;
        if status.is_err() {
            release.notify_one();
            let _ = start.await;
            let _ = state.repl_services.stop(&session.id).await;
            panic!("status waited for the startup operation's lock");
        }
        assert_eq!(status.unwrap().unwrap()["state"], "starting");
        let stop = tokio::time::timeout(
            Duration::from_millis(200),
            state.repl_services.stop(&session.id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(stop["state"], "stopping");
        release.notify_one();
        assert!(
            start.await.unwrap().is_err(),
            "a stopped startup must never publish a running service"
        );
        state
            .execution_supervisor
            .shutdown(Duration::from_secs(5))
            .await;
    }

    #[tokio::test]
    async fn concurrent_start_and_abandoned_caller_keep_one_owned_generation() {
        let state = AppState::for_tests();
        let session = state.db.create_session("fixture", "/tmp").await.unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *state.repl_services.startup_gate.lock().unwrap() =
            Some((entered.clone(), release.clone()));
        let host = state.repl_services.clone();
        let id = session.id.clone();
        let abandoned = tokio::spawn(async move { host.get_or_start(&id).await });
        entered.notified().await;
        abandoned.abort();
        let host = state.repl_services.clone();
        let id = session.id.clone();
        let second = tokio::spawn(async move { host.get_or_start(&id).await });
        let host = state.repl_services.clone();
        let id = session.id.clone();
        let third = tokio::spawn(async move { host.get_or_start(&id).await });
        release.notify_one();
        let second = second.await.unwrap().unwrap();
        let third = third.await.unwrap().unwrap();
        assert!(Arc::ptr_eq(&second, &third));
        let count: i64 = state
            .db
            .with_reader(|conn| {
                Ok(conn.query_row(
                    "SELECT count(*) FROM tasks WHERE task_type='repl'",
                    [],
                    |row| row.get(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(
            state.repl_services.status(&session.id).await.unwrap()["state"],
            "running"
        );
        state.repl_services.stop(&session.id).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if state.repl_services.status(&session.id).await.unwrap()["state"] == "stopped" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        state
            .execution_supervisor
            .shutdown(Duration::from_secs(5))
            .await;
    }

    #[tokio::test]
    async fn startup_timeout_cause_precedes_late_admission_receipt() {
        let state = AppState::for_tests();
        let session = state.db.create_session("fixture", "/tmp").await.unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *state.repl_services.admitted_gate.lock().unwrap() =
            Some((entered.clone(), release.clone()));
        let host = state.repl_services.clone();
        let id = session.id.clone();
        let start = tokio::spawn(async move { host.get_or_start(&id).await });
        entered.notified().await;
        let slot = state.repl_services.slot(&session.id);
        let generation = slot.lock().unwrap().clone().unwrap();
        assert!(generation.request_stop(StopCause::StartupTimeout).is_none());
        // A later user click must retain the timeout already chosen by startup.
        assert_eq!(
            state.repl_services.stop(&session.id).await.unwrap()["state"],
            "stopping"
        );
        let session_id = session.id.clone();
        let run_id: String = state
            .db
            .with_reader(move |conn| {
                Ok(conn.query_row(
                    "SELECT current_run_id FROM tasks WHERE session_id=?1 AND task_type='repl'",
                    [session_id],
                    |row| row.get(0),
                )?)
            })
            .await
            .unwrap();
        let finished = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let run = state.db.find_run_by_id(&run_id).await.unwrap().unwrap();
                if run.finished_at.is_some() {
                    break run;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        release.notify_one();
        assert!(start.await.unwrap().is_err());
        state
            .execution_supervisor
            .shutdown(Duration::from_secs(5))
            .await;
        assert_eq!(
            finished.unwrap().requested_exit_reason.as_deref(),
            Some(zk_db::run::EXIT_TIMEOUT)
        );
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
