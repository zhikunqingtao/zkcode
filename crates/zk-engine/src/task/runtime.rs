//! Database-authoritative task runtime.
//!
//! A task is durable before an executor can observe it.  Process-local state owns only
//! cancellation, join handles, bounded admission permits, and result notifications; it is
//! never consulted to answer lifecycle queries.

#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    missing_docs
)]

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use dashmap::DashMap;
use serde::Serialize;
use tokio::sync::{Notify, RwLock, RwLockReadGuard, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout_at};
use tokio_util::sync::CancellationToken;
use tracing::error;
use uuid::Uuid;
use zk_db::{
    CasOutcome, CleanupStatus, CommitTaskResult, CommitTaskResultOutcome, CreateTaskWithRun,
    InboxStatus, ResultStatus, RuntimeTaskRecord, TaskBudgetLimits, TaskBudgetSnapshot,
    TaskInboxMessage, TaskResultChunk, TaskStatus as DurableTaskStatus, VerificationStatus,
};
use zk_protocol::ServerMessage;
use zk_tools::ToolExecutorShutdownReport;

use crate::execution_resources::ExecutionSupervisor;
use crate::sink::MessageSink;
use crate::{NoopObservabilityRecorder, ObservabilityEvent, ObservabilityRecorder};

/// Maximum number of executing task futures in the process. Queued tasks hold no permit.
pub const GLOBAL_AGENT_LIMIT: usize = 8;
/// Maximum number of executing direct children owned by one root task.
pub const ROOT_AGENT_LIMIT: usize = 4;
/// Default child deadline after the unified runtime release gate. Callers may
/// supply a stricter value, but validation keeps the hard ceiling at 30 minutes.
pub const DEFAULT_TASK_TIMEOUT: Duration = Duration::from_mins(30);
/// Time given to a cancellation-aware executor to confirm its own cleanup.
pub const CLEANUP_GRACE: Duration = Duration::from_secs(8);
/// Timeout-specific grace, including provider drain and checkpoint persistence.
pub const TIMEOUT_CLEANUP_GRACE: Duration = Duration::from_secs(30);

/// Stable error returned by the runtime boundary.
#[derive(Clone, Debug, thiserror::Error, Serialize)]
#[serde(rename_all = "camelCase")]
#[error("{code}: {message}")]
pub struct TaskRuntimeError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl TaskRuntimeError {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }

    fn storage(error: impl std::fmt::Display) -> Self {
        let message = error.to_string();
        let code = message
            .split_once(':')
            .map_or("TASK_STORAGE_ERROR", |(_, detail)| detail.trim())
            .split(':')
            .next()
            .unwrap_or("TASK_STORAGE_ERROR")
            .to_owned();
        Self::new(code, message, true)
    }

    fn submission_storage(error: zk_db::DbError) -> Self {
        if let zk_db::DbError::Invalid(code) = &error {
            return Self::new(code, error.to_string(), false);
        }
        Self::storage(error)
    }
}

/// Attached child submission. V1 intentionally has no detached or recursive mode.
#[derive(Clone, Debug)]
pub struct ChildTaskSubmission {
    pub root_session_id: String,
    pub parent_task_id: String,
    pub parent_run_id: String,
    pub creator_tool_use_id: String,
    pub ordinal: i64,
    pub description: String,
    pub prompt: String,
    /// `agent` for Agent/TaskCreate and `swarm` only for the gated Swarm adapter.
    pub task_type: String,
    pub model: String,
    pub working_dir: String,
    /// JSON execution policy. An optional lower-camel `budget` object may request a
    /// smaller child allocation (`tokenLimit`, `costLimitNanosUsd`, `deadlineAtMs`).
    /// Omitted token/cost dimensions inherit no ceiling from an unlimited root;
    /// finite root dimensions retain the default 20% child allocation.
    pub execution_config_json: String,
    pub startup_epoch: i64,
    pub timeout: Duration,
}

impl ChildTaskSubmission {
    /// Fill the fields whose values are fixed for the first attached runtime.
    #[must_use]
    pub fn attached(
        root_session_id: impl Into<String>,
        parent_task_id: impl Into<String>,
        parent_run_id: impl Into<String>,
        creator_tool_use_id: impl Into<String>,
        description: impl Into<String>,
        prompt: impl Into<String>,
        model: impl Into<String>,
        working_dir: impl Into<String>,
    ) -> Self {
        Self {
            root_session_id: root_session_id.into(),
            parent_task_id: parent_task_id.into(),
            parent_run_id: parent_run_id.into(),
            creator_tool_use_id: creator_tool_use_id.into(),
            ordinal: 0,
            description: description.into(),
            prompt: prompt.into(),
            task_type: "agent".to_owned(),
            model: model.into(),
            working_dir: working_dir.into(),
            execution_config_json: r#"{"isolation":"readOnly","lifecycle":"attached"}"#.to_owned(),
            startup_epoch: 0,
            timeout: DEFAULT_TASK_TIMEOUT,
        }
    }
}

/// IDs and cancellation lineage passed to the executor after durable submission and claim.
#[derive(Clone, Debug)]
pub struct TaskExecutionContext {
    pub task_id: String,
    pub run_id: String,
    pub transcript_session_id: String,
    pub root_session_id: String,
    /// Immutable child execution limits; token/cost ceilings may be absent.
    pub budget: TaskBudgetLimits,
    pub cancel: CancellationToken,
}

/// Executor outcome. Runtime code, not the model, owns the durable terminal transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskExecutionResult {
    Complete(String),
    Partial { content: String, code: String },
    Failed { message: String, code: String },
    Cancelled { message: String },
}

impl TaskExecutionResult {
    #[must_use]
    pub fn complete(content: impl Into<String>) -> Self {
        Self::Complete(content.into())
    }

    #[must_use]
    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
            code: "TASK_EXECUTION_FAILED".to_owned(),
        }
    }
}

/// Submission response. `created=false` is a successful idempotent replay. If the
/// original process crashed after committing a queued attempt but before attaching
/// its driver, exactly one replay in the current process may attach that same Run.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSubmissionReceipt {
    pub task: RuntimeTaskRecord,
    pub run_id: String,
    pub transcript_session_id: String,
    pub created: bool,
}

/// Durable identity returned when the gated Swarm adapter claims one worker.
///
/// Swarm remains disabled by default, but its development-only path must use the
/// same pre-created Task, Run, internal Session and budget contract as Agent.
#[derive(Clone, Debug)]
pub struct ExternalTaskExecution {
    pub task_id: String,
    pub run_id: String,
    pub transcript_session_id: String,
    pub budget: TaskBudgetLimits,
}

/// Idempotent stop response.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelReceipt {
    pub cancel_requested: bool,
    pub task: RuntimeTaskRecord,
}

/// Bounded result query.
#[derive(Clone, Debug)]
pub struct TaskOutputRequest {
    pub root_session_id: String,
    pub task_id: String,
    pub wait_ms: u64,
    pub result_version: Option<i64>,
    pub cursor: usize,
    pub max_bytes: usize,
}

/// A result query never turns an ordinary wait expiry into a tool error.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskOutputResponse {
    pub task: RuntimeTaskRecord,
    pub result: Option<TaskResultChunk>,
    pub wait_expired: bool,
}

/// Outcome of one explicit process shutdown. Counts describe durable facts,
/// while `drained` reports whether every process-local execution owner reached
/// a safe boundary before the supplied deadline.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRuntimeShutdownReport {
    pub intake_closed: bool,
    pub tasks_requested: usize,
    pub runs_requested: usize,
    pub runs_interrupted: usize,
    pub cleanup_unconfirmed: usize,
    pub local_owners_timed_out: usize,
    pub leaf_owners_requested: usize,
    pub leaf_owners_remaining: usize,
    pub drained: bool,
}

/// Consumed exactly once after the durable shutdown-intent/cancellation
/// boundary. Keeping this phase opaque prevents reconciliation from racing
/// either Task drivers or physical leaf owners.
#[must_use = "the shutdown phase must be drained to reconcile durable state"]
pub struct TaskRuntimeShutdownPhase {
    deadline: Instant,
    intent: Result<(usize, usize), TaskRuntimeError>,
}

struct ActiveExecution {
    run_id: String,
    cancel: CancellationToken,
    driver: Mutex<Option<JoinHandle<()>>>,
}

#[cfg(test)]
#[derive(Default)]
struct TerminalCommitFailpoint {
    /// 0 = disabled, 1 = retryable storage failure, 2 = permanent invariant failure.
    mode: std::sync::atomic::AtomicU8,
    attempts: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
#[derive(Default)]
struct CancellationPersistFailpoint {
    /// Number of upcoming persistence attempts which fail before touching `SQLite`.
    remaining_failures: std::sync::atomic::AtomicUsize,
    attempts: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
#[derive(Default)]
struct ShutdownIntentFailpoint {
    /// Number of upcoming shutdown-intent writes which fail before touching `SQLite`.
    remaining_failures: std::sync::atomic::AtomicUsize,
}

/// RAII registration for an execution loop which is owned outside
/// [`TaskRuntime`] but must participate in the same cancellation tree.
///
/// Root conversation Runs use this lease: the conversation engine owns the
/// future, while `TaskRuntime` remains the sole durable cancellation authority.
/// Dropping a stale lease cannot remove a newer attempt because the Run identity
/// is checked before removing the process-local accelerator entry.
pub struct TaskExecutionLease {
    inner: Weak<TaskRuntimeInner>,
    task_id: String,
    run_id: String,
}

impl Drop for TaskExecutionLease {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let matches_attempt = inner
            .active
            .get(&self.task_id)
            .is_some_and(|active| active.run_id == self.run_id);
        if matches_attempt {
            inner.active.remove(&self.task_id);
        }
    }
}

#[derive(Clone)]
struct ExternalTaskRegistration {
    task_id: String,
    run_id: String,
    transcript_session_id: String,
    root_session_id: String,
    cancel: CancellationToken,
}

struct TaskRuntimeInner {
    db: zk_db::Db,
    sink: Arc<dyn MessageSink>,
    observability: Arc<dyn ObservabilityRecorder>,
    active: DashMap<String, Arc<ActiveExecution>>,
    /// Reapers retain ownership after a result explicitly records unconfirmed cleanup.
    cleanup_reapers: DashMap<String, JoinHandle<()>>,
    /// One durable-result resolver per child result version. A marker is inserted before
    /// spawning, and the spawned future owns an `Arc<TaskRuntimeInner>` until it either
    /// commits the receipt at a parent safe boundary or observes that the parent may no
    /// longer consume it.
    parent_resolvers: DashMap<String, ()>,
    result_notify: DashMap<String, Arc<Notify>>,
    global_slots: Arc<Semaphore>,
    root_slots: DashMap<String, Arc<Semaphore>>,
    /// Gated Swarm worker IDs map to durable `TaskRuntime` identities. This is
    /// process-local scheduling state only; Task/Run/Result rows remain authoritative.
    external_tasks: DashMap<String, ExternalTaskRegistration>,
    /// An atomic fast-path plus an async gate close the submit/register race:
    /// shutdown flips the flag before taking the write side of the gate, then
    /// snapshots owners only after every in-flight intake operation has left.
    accepting_execution: AtomicBool,
    intake_gate: RwLock<()>,
    #[cfg(test)]
    terminal_commit_failpoint: TerminalCommitFailpoint,
    #[cfg(test)]
    cancellation_persist_failpoint: CancellationPersistFailpoint,
    #[cfg(test)]
    shutdown_intent_failpoint: ShutdownIntentFailpoint,
}

/// Unified durable runtime used by Agent, `TaskCreate` and (behind its gate) Swarm.
#[derive(Clone)]
pub struct TaskRuntime {
    inner: Arc<TaskRuntimeInner>,
}

impl TaskRuntime {
    #[must_use]
    pub fn new(db: zk_db::Db, sink: Arc<dyn MessageSink>) -> Self {
        Self {
            inner: Arc::new(TaskRuntimeInner {
                db,
                sink,
                observability: Arc::new(NoopObservabilityRecorder),
                active: DashMap::new(),
                cleanup_reapers: DashMap::new(),
                parent_resolvers: DashMap::new(),
                result_notify: DashMap::new(),
                global_slots: Arc::new(Semaphore::new(GLOBAL_AGENT_LIMIT)),
                root_slots: DashMap::new(),
                external_tasks: DashMap::new(),
                accepting_execution: AtomicBool::new(true),
                intake_gate: RwLock::new(()),
                #[cfg(test)]
                terminal_commit_failpoint: TerminalCommitFailpoint::default(),
                #[cfg(test)]
                cancellation_persist_failpoint: CancellationPersistFailpoint::default(),
                #[cfg(test)]
                shutdown_intent_failpoint: ShutdownIntentFailpoint::default(),
            }),
        }
    }

    /// Attach the process-wide best-effort recorder. Durable state remains in `SQLite`.
    #[must_use]
    pub fn with_observability(mut self, recorder: Arc<dyn ObservabilityRecorder>) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("observability must be configured before sharing TaskRuntime")
            .observability = recorder;
        self
    }

    #[must_use]
    pub fn db(&self) -> &zk_db::Db {
        &self.inner.db
    }

    /// Whether this process may still attach a new execution owner. Durable
    /// query, result, and terminalization APIs remain available while draining.
    #[must_use]
    pub fn accepts_new_execution(&self) -> bool {
        self.inner.accepting_execution.load(Ordering::Acquire)
    }

    async fn execution_intake(&self) -> Result<RwLockReadGuard<'_, ()>, TaskRuntimeError> {
        if !self.accepts_new_execution() {
            return Err(runtime_shutting_down());
        }
        let guard = self.inner.intake_gate.read().await;
        if !self.accepts_new_execution() {
            return Err(runtime_shutting_down());
        }
        Ok(guard)
    }

    /// Configure or tighten the hard budget owned by a root Task. The independent
    /// `budgetVersion` CAS avoids racing lifecycle-only Task updates.
    pub async fn configure_root_budget(
        &self,
        root_session_id: &str,
        root_task_id: &str,
        expected_budget_version: i64,
        limits: &TaskBudgetLimits,
    ) -> Result<CasOutcome, TaskRuntimeError> {
        let task = self
            .get_owned(root_session_id, root_task_id)
            .await?
            .ok_or_else(|| {
                TaskRuntimeError::new(
                    "TASK_NOT_FOUND",
                    "root Task does not exist in the current root session",
                    false,
                )
            })?;
        if task.parent_task_id.is_some() || task.root_task_id != task.id {
            return Err(TaskRuntimeError::new(
                "ROOT_TASK_REQUIRED",
                "budgets can only be configured on the root Task",
                false,
            ));
        }
        self.inner
            .db
            .configure_root_task_budget_cas(root_task_id, expected_budget_version, limits)
            .await
            .map_err(TaskRuntimeError::storage)
    }

    /// Return the durable account and this Task's child reservation, if any.
    pub async fn read_budget(
        &self,
        root_session_id: &str,
        task_id: &str,
    ) -> Result<TaskBudgetSnapshot, TaskRuntimeError> {
        if self.get_owned(root_session_id, task_id).await?.is_none() {
            return Err(TaskRuntimeError::new(
                "TASK_NOT_FOUND",
                "Task does not exist in the current root session",
                false,
            ));
        }
        self.inner
            .db
            .read_task_budget(task_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| {
                TaskRuntimeError::new("TASK_NOT_FOUND", "Task budget disappeared", false)
            })
    }

    /// Resolve the durable parent Task from a tool invocation's parent Run. This keeps
    /// `parentTaskId` out of model-controlled arguments and closes cross-session access.
    pub async fn resolve_parent_task(
        &self,
        root_session_id: &str,
        parent_run_id: &str,
    ) -> Result<RuntimeTaskRecord, TaskRuntimeError> {
        let run = self
            .inner
            .db
            .find_run_by_id(parent_run_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| {
                TaskRuntimeError::new("PARENT_RUN_NOT_FOUND", "parent Run does not exist", false)
            })?;
        if run.is_terminal() {
            return Err(TaskRuntimeError::new(
                "PARENT_RUN_TERMINAL",
                "parent Run is already terminal",
                false,
            ));
        }
        if run.task_id.is_empty() {
            return Err(TaskRuntimeError::new(
                "PARENT_TASK_NOT_FOUND",
                "parent Run is not attached to a durable Task",
                false,
            ));
        }
        let task = self
            .get_owned(root_session_id, &run.task_id)
            .await?
            .ok_or_else(|| {
                TaskRuntimeError::new(
                    "PARENT_TASK_NOT_FOUND",
                    "parent Run does not belong to the current root session",
                    false,
                )
            })?;
        if task.current_run_id.as_deref() != Some(parent_run_id) {
            return Err(TaskRuntimeError::new(
                "PARENT_RUN_STALE",
                "parent Run is not the Task's current attempt",
                false,
            ));
        }
        if task.parent_task_id.is_some() {
            return Err(TaskRuntimeError::new(
                "RECURSIVE_DELEGATION_DISABLED",
                "v1 child Agents cannot delegate another Agent",
                false,
            ));
        }
        Ok(task)
    }

    /// Update advisory fields only. Lifecycle and terminal state remain runtime-owned.
    pub async fn update_advisory(
        &self,
        root_session_id: &str,
        task_id: &str,
        description: Option<&str>,
        plan: Option<&str>,
        reported_progress: Option<f64>,
    ) -> Result<RuntimeTaskRecord, TaskRuntimeError> {
        if description.is_some_and(|value| value.trim().is_empty()) {
            return Err(TaskRuntimeError::new(
                "TASK_DESCRIPTION_INVALID",
                "description must be non-empty when supplied",
                false,
            ));
        }
        if let Some(progress) = reported_progress
            && (!progress.is_finite() || !(0.0..=1.0).contains(&progress))
        {
            return Err(TaskRuntimeError::new(
                "TASK_PROGRESS_INVALID",
                "reportedProgress must be between 0 and 1",
                false,
            ));
        }
        if let Some(plan) = plan {
            serde_json::from_str::<serde_json::Value>(plan).map_err(|error| {
                TaskRuntimeError::new(
                    "TASK_PLAN_INVALID",
                    format!("plan must be valid JSON: {error}"),
                    false,
                )
            })?;
        }
        if description.is_none() && plan.is_none() && reported_progress.is_none() {
            return self
                .get_owned(root_session_id, task_id)
                .await?
                .ok_or_else(|| {
                    TaskRuntimeError::new(
                        "TASK_NOT_FOUND",
                        "task does not exist in the current root session",
                        false,
                    )
                });
        }

        let description = description.map(str::to_owned);
        let plan = plan.map(str::to_owned);
        for _ in 0..6 {
            let task = self
                .get_owned(root_session_id, task_id)
                .await?
                .ok_or_else(|| {
                    TaskRuntimeError::new(
                        "TASK_NOT_FOUND",
                        "task does not exist in the current root session",
                        false,
                    )
                })?;
            let task_id_owned = task_id.to_owned();
            let session_owned = root_session_id.to_owned();
            let description_owned = description.clone();
            let plan_owned = plan.clone();
            let expected_version = task.version;
            let changed = self
                .inner
                .db
                .with_writer(move |connection| {
                    let now = zk_db::time::format_rfc3339_micros(zk_db::time::now_millis());
                    let count = connection.execute(
                        "UPDATE tasks SET
                            description=COALESCE(?1,description),
                            plan_json=COALESCE(?2,plan_json),
                            reported_progress=COALESCE(?3,reported_progress),
                            updated_at=?4,version=version+1
                         WHERE id=?5 AND session_id=?6 AND version=?7",
                        (
                            description_owned,
                            plan_owned,
                            reported_progress,
                            now,
                            task_id_owned,
                            session_owned,
                            expected_version,
                        ),
                    )?;
                    Ok(count == 1)
                })
                .await
                .map_err(TaskRuntimeError::storage)?;
            if changed {
                return self
                    .get_owned(root_session_id, task_id)
                    .await?
                    .ok_or_else(|| {
                        TaskRuntimeError::new(
                            "TASK_NOT_FOUND",
                            "task disappeared after advisory update",
                            false,
                        )
                    });
            }
        }
        Err(TaskRuntimeError::new(
            "TASK_VERSION_CONFLICT",
            "task changed while advisory fields were updated",
            true,
        ))
    }

    /// Atomically persist an attached child before scheduling its execution future.
    pub async fn submit_child<F, Fut>(
        &self,
        request: ChildTaskSubmission,
        build: F,
    ) -> Result<TaskSubmissionReceipt, TaskRuntimeError>
    where
        F: FnOnce(TaskExecutionContext) -> Fut + Send + 'static,
        Fut: Future<Output = TaskExecutionResult> + Send + 'static,
    {
        let _intake = self.execution_intake().await?;
        validate_submission(&request)?;
        let execution_config_json = durable_submission_config(&request)?;
        let task_id = Uuid::new_v4().to_string();
        let run_id = Uuid::new_v4().to_string();
        let transcript_session_id = Uuid::new_v4().to_string();
        let durable = self
            .inner
            .db
            .create_task_with_run(&CreateTaskWithRun {
                task_id,
                run_id,
                root_session_id: request.root_session_id.clone(),
                transcript_session_id,
                parent_task_id: Some(request.parent_task_id.clone()),
                parent_run_id: Some(request.parent_run_id.clone()),
                creator_tool_use_id: Some(request.creator_tool_use_id.clone()),
                ordinal: request.ordinal,
                description: request.description.clone(),
                prompt: Some(request.prompt.clone()),
                task_type: request.task_type.clone(),
                model: request.model.clone(),
                working_dir: request.working_dir.clone(),
                execution_config_json,
                startup_epoch: request.startup_epoch,
            })
            .await
            .map_err(TaskRuntimeError::submission_storage)?;

        let receipt = TaskSubmissionReceipt {
            task: durable.task.clone(),
            run_id: durable.run_id.clone(),
            transcript_session_id: durable.transcript_session_id.clone(),
            created: durable.created,
        };
        if durable.task.status != DurableTaskStatus::Queued {
            return Ok(receipt);
        }

        // `created=false` can mean the previous process committed the Task/Run but
        // crashed before installing a process-local owner. Validate the durable
        // attempt before competing for the accelerator entry. A replay of a Run
        // which has already started or terminated is observation-only and must not
        // execute the supplied closure again.
        if durable.task.current_run_id.as_deref() != Some(durable.run_id.as_str()) {
            return Err(TaskRuntimeError::new(
                "TASK_RUN_IDENTITY_INVALID",
                "queued Task does not reference the idempotently returned Run",
                false,
            ));
        }
        let durable_run = self
            .inner
            .db
            .find_run_by_id(&durable.run_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| {
                TaskRuntimeError::new(
                    "TASK_RUN_NOT_FOUND",
                    "queued Task's current Run does not exist",
                    false,
                )
            })?;
        if durable_run.task_id != durable.task.id
            || durable_run.session_id != durable.transcript_session_id
        {
            return Err(TaskRuntimeError::new(
                "TASK_RUN_IDENTITY_INVALID",
                "queued Task and current Run do not share one durable identity",
                false,
            ));
        }
        // The Task snapshot and Run lookup are intentionally separate reads. An
        // already-attached owner may claim or finish the Run between them; that
        // is an ordinary idempotent observation, not a corrupt submission.
        if durable_run.status != "queued" {
            return Ok(receipt);
        }

        let cancel = CancellationToken::new();
        let active = Arc::new(ActiveExecution {
            run_id: durable.run_id.clone(),
            cancel: cancel.clone(),
            driver: Mutex::new(None),
        });
        match self.inner.active.entry(durable.task.id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                if entry.get().run_id != durable.run_id {
                    return Err(TaskRuntimeError::new(
                        "TASK_EXECUTION_ATTEMPT_CONFLICT",
                        "another Run is already attached to this Task",
                        false,
                    ));
                }
                return Ok(receipt);
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&active));
            }
        }

        record_task_event(
            &self.inner.observability,
            &request.root_session_id,
            &durable.task.id,
            if durable.created {
                "submit"
            } else {
                "reattach"
            },
            "queued",
        );

        let inner = Arc::clone(&self.inner);
        let task = durable.task;
        let run_id = durable.run_id;
        let transcript_session_id = durable.transcript_session_id;
        let root_session_id = request.root_session_id;
        let mut task_timeout = request.timeout;
        if let Some(deadline_at_ms) = task.deadline_at_ms {
            let remaining_ms = deadline_at_ms.saturating_sub(zk_db::time::now_millis());
            task_timeout = task_timeout.min(Duration::from_millis(
                u64::try_from(remaining_ms.max(0)).unwrap_or(0),
            ));
        }
        let driver = tokio::spawn(async move {
            drive_task(
                Arc::clone(&inner),
                task,
                run_id,
                transcript_session_id,
                root_session_id,
                task_timeout,
                cancel,
                build,
            )
            .await;
        });
        *active.driver.lock().expect("task driver lock poisoned") = Some(driver);
        Ok(receipt)
    }

    /// Dispatch a root Cron Task whose Session, Task, queued Run and occurrence
    /// were already committed by one `SQLite` scheduling transaction.
    ///
    /// This entrypoint does not create or mutate a competing execution record. It
    /// only attaches the existing durable identity to the same bounded driver,
    /// cancellation tree and immutable-result commit path used by Agent tasks.
    /// `false` means another in-process dispatcher already owns the Task.
    pub async fn dispatch_precreated_cron<F, Fut>(
        &self,
        task_id: &str,
        task_timeout: Duration,
        build: F,
    ) -> Result<bool, TaskRuntimeError>
    where
        F: FnOnce(TaskExecutionContext) -> Fut + Send + 'static,
        Fut: Future<Output = TaskExecutionResult> + Send + 'static,
    {
        let _intake = self.execution_intake().await?;
        let task = self
            .inner
            .db
            .find_runtime_task_by_id(task_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| {
                TaskRuntimeError::new("TASK_NOT_FOUND", "pre-created Cron Task not found", false)
            })?;
        if task.task_type != "cron"
            || task.parent_task_id.is_some()
            || task.root_task_id != task.id
            || task.session_id.is_empty()
        {
            return Err(TaskRuntimeError::new(
                "CRON_TASK_IDENTITY_INVALID",
                "Cron dispatch requires a root cron Task",
                false,
            ));
        }
        if task.status != DurableTaskStatus::Queued {
            return Err(TaskRuntimeError::new(
                "CRON_TASK_NOT_QUEUED",
                format!("Cron Task is {}", task.status.as_db()),
                false,
            ));
        }
        let run_id = task.current_run_id.clone().ok_or_else(|| {
            TaskRuntimeError::new("TASK_RUN_NOT_FOUND", "Cron Task has no current Run", false)
        })?;
        let run = self
            .inner
            .db
            .find_run_by_id(&run_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| {
                TaskRuntimeError::new("TASK_RUN_NOT_FOUND", "Cron Run not found", false)
            })?;
        if run.task_id != task.id || run.session_id != task.session_id || run.status != "queued" {
            return Err(TaskRuntimeError::new(
                "CRON_RUN_IDENTITY_INVALID",
                "Cron Run does not match the queued root Task",
                false,
            ));
        }

        let cancel = CancellationToken::new();
        let active = Arc::new(ActiveExecution {
            run_id: run_id.clone(),
            cancel: cancel.clone(),
            driver: Mutex::new(None),
        });
        match self.inner.active.entry(task.id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => return Ok(false),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&active));
            }
        }
        record_task_event(
            &self.inner.observability,
            &task.session_id,
            &task.id,
            "dispatch",
            "queued",
        );

        let inner = Arc::clone(&self.inner);
        let root_session_id = task.session_id.clone();
        let transcript_session_id = task.session_id.clone();
        let task_for_driver = task;
        let run_for_driver = run_id.clone();
        let driver = tokio::spawn(async move {
            drive_task(
                inner,
                task_for_driver,
                run_for_driver,
                transcript_session_id,
                root_session_id,
                task_timeout,
                cancel,
                build,
            )
            .await;
        });
        *active.driver.lock().expect("task driver lock poisoned") = Some(driver);
        tracing::debug!(task_id, run_id = %run_id, "queued Cron Task attached to TaskRuntime driver");
        Ok(true)
    }

    /// Attach a fail-closed restart attempt that was atomically created by the
    /// database recovery gate. This method never creates an attempt and refuses
    /// first attempts, roots, non-Agent tasks, or Runs without a copied
    /// checkpoint. `false` means another in-process driver already owns it.
    pub async fn dispatch_precreated_recovery<F, Fut>(
        &self,
        task_id: &str,
        task_timeout: Duration,
        build: F,
    ) -> Result<bool, TaskRuntimeError>
    where
        F: FnOnce(TaskExecutionContext) -> Fut + Send + 'static,
        Fut: Future<Output = TaskExecutionResult> + Send + 'static,
    {
        let _intake = self.execution_intake().await?;
        let task = self
            .inner
            .db
            .find_runtime_task_by_id(task_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| {
                TaskRuntimeError::new("TASK_NOT_FOUND", "recovery Task not found", false)
            })?;
        if task.task_type != "agent"
            || task.parent_task_id.is_none()
            || task.status != DurableTaskStatus::Queued
        {
            return Err(TaskRuntimeError::new(
                "RECOVERY_TASK_IDENTITY_INVALID",
                "recovery dispatch requires a queued attached child Agent",
                false,
            ));
        }
        let run_id = task.current_run_id.clone().ok_or_else(|| {
            TaskRuntimeError::new(
                "TASK_RUN_NOT_FOUND",
                "recovery Task has no current Run",
                false,
            )
        })?;
        let run = self
            .inner
            .db
            .find_run_by_id(&run_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| {
                TaskRuntimeError::new("TASK_RUN_NOT_FOUND", "recovery Run not found", false)
            })?;
        if run.task_id != task.id
            || run.status != "queued"
            || run.attempt <= 1
            || run.startup_epoch <= 0
            || run.checkpoint_id.is_none()
        {
            return Err(TaskRuntimeError::new(
                "RECOVERY_RUN_IDENTITY_INVALID",
                "Run is not a checkpoint-backed queued recovery attempt",
                false,
            ));
        }

        let cancel = CancellationToken::new();
        let active = Arc::new(ActiveExecution {
            run_id: run_id.clone(),
            cancel: cancel.clone(),
            driver: Mutex::new(None),
        });
        match self.inner.active.entry(task.id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => return Ok(false),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&active));
            }
        }
        record_task_event(
            &self.inner.observability,
            &task.session_id,
            &task.id,
            "recover",
            "queued",
        );

        let inner = Arc::clone(&self.inner);
        let root_session_id = task.session_id.clone();
        let transcript_session_id = run.session_id;
        let task_for_driver = task;
        let run_for_driver = run_id.clone();
        let driver = tokio::spawn(async move {
            drive_task(
                inner,
                task_for_driver,
                run_for_driver,
                transcript_session_id,
                root_session_id,
                task_timeout,
                cancel,
                build,
            )
            .await;
        });
        *active.driver.lock().expect("task driver lock poisoned") = Some(driver);
        tracing::info!(task_id, run_id = %run_id, "safe recovery attempt attached to TaskRuntime");
        Ok(true)
    }

    /// Query a task only when it belongs to the caller's root session.
    ///
    /// Absence and denied ancestry are deliberately distinct at this boundary:
    /// callers receive `Ok(None)` only when the id does not exist, while an id
    /// owned by another root session returns `TASK_ACCESS_DENIED`. Storage
    /// failures retain the separate `TASK_STORAGE_ERROR` family.
    pub async fn get_owned(
        &self,
        root_session_id: &str,
        task_id: &str,
    ) -> Result<Option<RuntimeTaskRecord>, TaskRuntimeError> {
        let task = self
            .inner
            .db
            .find_runtime_task_by_id(task_id)
            .await
            .map_err(TaskRuntimeError::storage)?;
        match task {
            Some(task) if task.session_id != root_session_id => Err(TaskRuntimeError::new(
                "TASK_ACCESS_DENIED",
                "task belongs to a different root session",
                false,
            )),
            task => Ok(task),
        }
    }

    /// Stable root task tree projection, optionally filtered by canonical status.
    pub async fn list_owned(
        &self,
        root_session_id: &str,
        status: Option<DurableTaskStatus>,
    ) -> Result<Vec<RuntimeTaskRecord>, TaskRuntimeError> {
        let mut tasks = self
            .inner
            .db
            .find_task_tree_owned(root_session_id)
            .await
            .map_err(TaskRuntimeError::storage)?;
        if let Some(status) = status {
            tasks.retain(|task| task.status == status);
        }
        Ok(tasks)
    }

    /// Attach an already-created Run whose future is driven by another runtime
    /// surface to this runtime's cancellation tree.
    ///
    /// The durable Task/Run identity is validated before registration. A second
    /// owner for the same Task is rejected. After insertion the Task is read
    /// again so a cancellation which won the race immediately before
    /// registration still reaches the supplied token.
    pub async fn attach_existing_execution(
        &self,
        root_session_id: &str,
        task_id: &str,
        run_id: &str,
        cancel: CancellationToken,
    ) -> Result<TaskExecutionLease, TaskRuntimeError> {
        let _intake = self.execution_intake().await?;
        let task = self
            .get_owned(root_session_id, task_id)
            .await?
            .ok_or_else(|| {
                TaskRuntimeError::new(
                    "TASK_NOT_FOUND",
                    "Task does not exist in the current root session",
                    false,
                )
            })?;
        if task.current_run_id.as_deref() != Some(run_id) {
            return Err(TaskRuntimeError::new(
                "TASK_RUN_STALE",
                "Run is not the Task's current attempt",
                false,
            ));
        }
        let run = self
            .inner
            .db
            .find_run_by_id(run_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| TaskRuntimeError::new("TASK_RUN_NOT_FOUND", "Run not found", false))?;
        if run.task_id != task.id {
            return Err(TaskRuntimeError::new(
                "TASK_RUN_IDENTITY_INVALID",
                "Run does not belong to the Task",
                false,
            ));
        }

        let active = Arc::new(ActiveExecution {
            run_id: run_id.to_owned(),
            cancel: cancel.clone(),
            driver: Mutex::new(None),
        });
        match self.inner.active.entry(task_id.to_owned()) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                return Err(TaskRuntimeError::new(
                    "TASK_EXECUTION_ALREADY_ATTACHED",
                    "Task already has an attached execution owner",
                    false,
                ));
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(active);
            }
        }

        // Construct the lease before the post-registration read. Any storage
        // or identity failure below must drop it and remove the accelerator
        // entry; otherwise a failed attachment permanently blocks a retry.
        let lease = TaskExecutionLease {
            inner: Arc::downgrade(&self.inner),
            task_id: task_id.to_owned(),
            run_id: run_id.to_owned(),
        };

        let current = self
            .get_owned(root_session_id, task_id)
            .await?
            .ok_or_else(|| TaskRuntimeError::new("TASK_NOT_FOUND", "Task disappeared", false))?;
        if current.status == DurableTaskStatus::Cancelling || current.status.is_terminal() {
            cancel.cancel();
        }
        Ok(lease)
    }

    /// Resolve a Run to its durable Task and request cancellation through the
    /// same state machine used by `TaskStop`.
    ///
    /// This is an internal-authority entry point for transport and interaction
    /// coordinators which already authorized the Run. Model-facing tools must
    /// continue using [`Self::cancel_owned`] with an asserted root Session.
    pub async fn cancel_run_with_cause(
        &self,
        run_id: &str,
        exit_reason: &str,
        reason: &str,
    ) -> Result<CancelReceipt, TaskRuntimeError> {
        let run = self
            .inner
            .db
            .find_run_by_id(run_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| TaskRuntimeError::new("TASK_RUN_NOT_FOUND", "Run not found", false))?;
        if run.task_id.is_empty() {
            return Err(TaskRuntimeError::new(
                "TASK_RUN_UNOWNED",
                "Run is not owned by a durable Task",
                false,
            ));
        }
        let task = self
            .inner
            .db
            .find_runtime_task_by_id(&run.task_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| {
                TaskRuntimeError::new("TASK_NOT_FOUND", "Run's durable Task does not exist", false)
            })?;
        if task.current_run_id.as_deref() != Some(run_id) {
            return Err(TaskRuntimeError::new(
                "TASK_RUN_STALE",
                "Run is not the Task's current attempt",
                false,
            ));
        }
        self.cancel_owned_with_cause(&task.session_id, &task.id, exit_reason, reason, true)
            .await
    }

    /// Request cancellation and propagate the token. The driver, not this method, owns
    /// cleanup and the immutable terminal result.
    pub async fn cancel_owned(
        &self,
        root_session_id: &str,
        task_id: &str,
        reason: &str,
    ) -> Result<CancelReceipt, TaskRuntimeError> {
        self.cancel_owned_with_cause(
            root_session_id,
            task_id,
            zk_db::run::EXIT_USER_CANCELLED,
            reason,
            true,
        )
        .await
    }

    /// Cancel one attached child because its parent execution was cancelled.
    /// This entrypoint is intentionally separate from [`Self::cancel_owned`] so
    /// a transport/tool cancellation cannot be misreported as a user action.
    pub async fn cancel_attached_from_parent(
        &self,
        root_session_id: &str,
        task_id: &str,
        reason: &str,
    ) -> Result<CancelReceipt, TaskRuntimeError> {
        self.cancel_owned_with_cause(
            root_session_id,
            task_id,
            zk_db::run::EXIT_PARENT_CANCELLED,
            reason,
            false,
        )
        .await
    }

    async fn cancel_owned_with_cause(
        &self,
        root_session_id: &str,
        task_id: &str,
        exit_reason: &str,
        reason: &str,
        cascade_root: bool,
    ) -> Result<CancelReceipt, TaskRuntimeError> {
        let mut requested = false;
        let task = loop {
            let Some(task) = self.get_owned(root_session_id, task_id).await? else {
                return Err(TaskRuntimeError::new(
                    "TASK_NOT_FOUND",
                    "task does not exist in the current root session",
                    false,
                ));
            };
            if task.status.is_terminal()
                || matches!(
                    task.status,
                    DurableTaskStatus::Cancelling | DurableTaskStatus::NeedsAttention
                )
            {
                break task;
            }
            if persist_cancelling(&self.inner, &task, exit_reason, reason).await? {
                requested = true;
                let updated = self
                    .get_owned(root_session_id, task_id)
                    .await?
                    .ok_or_else(|| {
                        TaskRuntimeError::new(
                            "TASK_NOT_FOUND",
                            "task disappeared immediately after cancellation CAS",
                            false,
                        )
                    })?;
                break updated;
            }
        };

        // Attached parent cancellation cascades to every non-terminal descendant.
        if cascade_root && task.parent_task_id.is_none() {
            let descendants = self.list_owned(root_session_id, None).await?;
            let mut first_error = None;
            for child in descendants
                .into_iter()
                .filter(|candidate| candidate.root_task_id == task.id && candidate.id != task.id)
            {
                if let Err(error) = self
                    .request_child_cancel(&child, "parent Task was cancelled")
                    .await
                {
                    tracing::error!(
                        task_id,
                        child_task_id = %child.id,
                        code = %error.code,
                        message = %error.message,
                        "attached child cancellation did not become durable"
                    );
                    first_error.get_or_insert(error);
                }
            }
            if let Some(error) = first_error {
                // Root execution remains owned and unsignalled. A retry can
                // finish the cascade; child drivers also observe the durable
                // parent state and retain their own cleanup responsibility.
                return Err(error);
            }
        }

        // `needsAttention` is a quarantine state paired with an interrupted Run,
        // not a durable cancellation boundary. A stale in-memory registration
        // must not be signalled unless persistence proves cancelling (or the
        // logical Task is already terminal).
        if (task.status == DurableTaskStatus::Cancelling || task.status.is_terminal())
            && let Some(active) = self.inner.active.get(task_id)
        {
            active.cancel.cancel();
        }

        let current = self
            .get_owned(root_session_id, task_id)
            .await?
            .ok_or_else(|| TaskRuntimeError::new("TASK_NOT_FOUND", "task disappeared", false))?;
        Ok(CancelReceipt {
            cancel_requested: requested,
            task: current,
        })
    }

    async fn request_child_cancel(
        &self,
        child: &RuntimeTaskRecord,
        reason: &str,
    ) -> Result<(), TaskRuntimeError> {
        if child.status == DurableTaskStatus::NeedsAttention {
            return Ok(());
        }
        if !child.status.is_terminal() && child.status != DurableTaskStatus::Cancelling {
            request_cancelling(
                &self.inner,
                &child.id,
                zk_db::run::EXIT_PARENT_CANCELLED,
                reason,
            )
            .await?;
        }
        if let Some(active) = self.inner.active.get(&child.id) {
            active.cancel.cancel();
        }
        Ok(())
    }

    /// Read an immutable result page, optionally waiting up to 30 seconds.
    pub async fn read_output(
        &self,
        request: TaskOutputRequest,
    ) -> Result<TaskOutputResponse, TaskRuntimeError> {
        if request.wait_ms > 30_000 {
            return Err(TaskRuntimeError::new(
                "TASK_WAIT_INVALID",
                "waitMs must be between 0 and 30000",
                false,
            ));
        }
        if request.max_bytes == 0 || request.max_bytes > zk_db::INLINE_RESULT_LIMIT {
            return Err(TaskRuntimeError::new(
                "RESULT_PAGE_SIZE_INVALID",
                "maxBytes must be between 1 and 65536",
                false,
            ));
        }
        let notifier = self.notifier(&request.task_id);
        let deadline = Instant::now() + Duration::from_millis(request.wait_ms);
        loop {
            // Register before reading so a commit between the read and wait is not lost.
            let notification = notifier.notified();
            tokio::pin!(notification);
            let task = self
                .get_owned(&request.root_session_id, &request.task_id)
                .await?
                .ok_or_else(|| {
                    TaskRuntimeError::new(
                        "TASK_NOT_FOUND",
                        "task does not exist in the current root session",
                        false,
                    )
                })?;
            let result = self
                .inner
                .db
                .read_task_result(
                    &request.task_id,
                    request.result_version,
                    request.cursor,
                    request.max_bytes,
                )
                .await
                .map_err(TaskRuntimeError::storage)?;
            if result.is_some()
                || task.status.is_terminal()
                || task.status == DurableTaskStatus::NeedsAttention
            {
                // Task and result are separate repository reads. Re-read the projection after
                // observing a result so callers never receive a terminal result paired with a
                // stale pre-commit `cancelling`/`running` status.
                let task = if result.is_some() {
                    self.get_owned(&request.root_session_id, &request.task_id)
                        .await?
                        .ok_or_else(|| {
                            TaskRuntimeError::new(
                                "TASK_NOT_FOUND",
                                "task disappeared after result became available",
                                false,
                            )
                        })?
                } else {
                    task
                };
                return Ok(TaskOutputResponse {
                    task,
                    result,
                    wait_expired: false,
                });
            }
            if request.wait_ms == 0 || Instant::now() >= deadline {
                return Ok(TaskOutputResponse {
                    task,
                    result: None,
                    wait_expired: true,
                });
            }
            // Periodic DB reads keep SQLite authoritative if another process commits.
            let poll = sleep(Duration::from_millis(250));
            tokio::pin!(poll);
            tokio::select! {
                () = &mut notification => {}
                () = &mut poll => {}
                () = sleep_until(deadline) => {
                    let task = self.get_owned(&request.root_session_id, &request.task_id).await?
                        .ok_or_else(|| TaskRuntimeError::new("TASK_NOT_FOUND", "task disappeared", false))?;
                    let result = self.inner.db.read_task_result(
                        &request.task_id, request.result_version, request.cursor, request.max_bytes,
                    ).await.map_err(TaskRuntimeError::storage)?;
                    return Ok(TaskOutputResponse { task, result, wait_expired: true });
                }
            }
        }
    }

    /// Durable message delivery; a terminal target returns `TASK_TERMINAL:<status>`.
    pub async fn send_message(
        &self,
        root_session_id: &str,
        target_task_id: &str,
        sender_task_id: Option<&str>,
        message: &str,
    ) -> Result<TaskInboxMessage, TaskRuntimeError> {
        if self
            .get_owned(root_session_id, target_task_id)
            .await?
            .is_none()
        {
            return Err(TaskRuntimeError::new(
                "TASK_NOT_FOUND",
                "target task does not exist",
                false,
            ));
        }
        self.inner
            .db
            .enqueue_task_message(root_session_id, target_task_id, sender_task_id, message)
            .await
            .map_err(TaskRuntimeError::storage)
    }

    /// Read a target inbox after enforcing root-session ownership.
    pub async fn read_inbox(
        &self,
        root_session_id: &str,
        task_id: &str,
        statuses: &[InboxStatus],
        limit: usize,
    ) -> Result<Vec<TaskInboxMessage>, TaskRuntimeError> {
        if self.get_owned(root_session_id, task_id).await?.is_none() {
            return Err(TaskRuntimeError::new(
                "TASK_NOT_FOUND",
                "task does not exist in the current root session",
                false,
            ));
        }
        self.inner
            .db
            .read_task_inbox(task_id, statuses, limit)
            .await
            .map_err(TaskRuntimeError::storage)
    }

    /// Advance one durable inbox message at an executor safe boundary.
    pub async fn mark_inbox(
        &self,
        root_session_id: &str,
        task_id: &str,
        message_id: &str,
        expected: InboxStatus,
        target: InboxStatus,
        rejection_reason: Option<&str>,
    ) -> Result<CasOutcome, TaskRuntimeError> {
        if self.get_owned(root_session_id, task_id).await?.is_none() {
            return Err(TaskRuntimeError::new(
                "TASK_NOT_FOUND",
                "task does not exist in the current root session",
                false,
            ));
        }
        let outcome = self
            .inner
            .db
            .mark_task_inbox_message_for_task(
                task_id,
                message_id,
                expected,
                target,
                rejection_reason,
            )
            .await
            .map_err(TaskRuntimeError::storage)?;
        if outcome == CasOutcome::NotFound {
            return Err(TaskRuntimeError::new(
                "TASK_MESSAGE_NOT_FOUND",
                "inbox message is not owned by the target task",
                false,
            ));
        }
        Ok(outcome)
    }

    fn notifier(&self, task_id: &str) -> Arc<Notify> {
        self.inner
            .result_notify
            .entry(task_id.to_owned())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Number of executing or waiting in-process drivers (DB queries remain authoritative).
    #[must_use]
    pub fn local_active_count(&self) -> usize {
        self.inner.active.len()
    }

    /// Close Task execution intake, durably request a process restart stop,
    /// signal every local cancellation tree, drain Task drivers, and reconcile
    /// once. Production composition must use [`Self::shutdown_with_supervisor`]
    /// so physical leaf owners participate in the same ordered boundary.
    pub async fn shutdown(
        &self,
        grace: Duration,
    ) -> Result<TaskRuntimeShutdownReport, TaskRuntimeError> {
        let phase = self.begin_shutdown_inner(None, grace).await;
        self.finish_shutdown_inner(None, phase).await
    }

    /// Execute the complete production shutdown boundary: both execution
    /// intakes close before the durable intent write; cancellation follows that
    /// write; Task drivers and physical leaf owners drain before the one and
    /// only reconciliation pass.
    pub async fn shutdown_with_supervisor(
        &self,
        supervisor: &ExecutionSupervisor,
        grace: Duration,
    ) -> Result<TaskRuntimeShutdownReport, TaskRuntimeError> {
        let phase = self.begin_shutdown_with_supervisor(supervisor, grace).await;
        self.finish_shutdown_with_supervisor(supervisor, phase)
            .await
    }

    /// Enter the durable shutdown boundary without starting reconciliation.
    /// The server uses this phase before it starts draining HTTP so an in-flight
    /// request cannot create new Task or leaf work during graceful shutdown.
    pub async fn begin_shutdown_with_supervisor(
        &self,
        supervisor: &ExecutionSupervisor,
        grace: Duration,
    ) -> TaskRuntimeShutdownPhase {
        self.begin_shutdown_inner(Some(supervisor), grace).await
    }

    /// Drain both owner domains and then perform exactly one durable
    /// reconciliation. Consuming the phase prevents accidental reuse.
    pub async fn finish_shutdown_with_supervisor(
        &self,
        supervisor: &ExecutionSupervisor,
        phase: TaskRuntimeShutdownPhase,
    ) -> Result<TaskRuntimeShutdownReport, TaskRuntimeError> {
        self.finish_shutdown_inner(Some(supervisor), phase).await
    }

    async fn begin_shutdown_inner(
        &self,
        supervisor: Option<&ExecutionSupervisor>,
        grace: Duration,
    ) -> TaskRuntimeShutdownPhase {
        self.inner
            .accepting_execution
            .store(false, Ordering::Release);
        if let Some(supervisor) = supervisor {
            supervisor.close_intake();
        }

        // Wait for submissions which observed the old flag to finish durable
        // creation + local registration. No new reader can pass the second
        // flag check once this write guard is pending/acquired.
        let _intake_closed = self.inner.intake_gate.write().await;
        // A failed intent write must not abandon the process-local cleanup
        // responsibility. Keep the error, cancel and drain every owner, then
        // return the persistence failure only after best-effort reconciliation.
        // This preserves the durable-before-signal boundary on the success path
        // without turning an unavailable database into an orphan-process leak.
        #[cfg(test)]
        let injected_intent_failure = self
            .inner
            .shutdown_intent_failpoint
            .remaining_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();
        #[cfg(not(test))]
        let injected_intent_failure = false;

        let intent = if injected_intent_failure {
            Err(TaskRuntimeError::new(
                "SHUTDOWN_INTENT_PERSISTENCE_INJECTED",
                "scripted shutdown-intent persistence failure",
                true,
            ))
        } else {
            self.inner
                .db
                .request_runtime_shutdown()
                .await
                .map_err(TaskRuntimeError::storage)
        }
        .map(|report| (report.tasks_requested, report.runs_requested));

        // A successful transaction above is the ordered cancellation boundary.
        // On storage failure the tokens are still signalled so local/process
        // cleanup ownership is never abandoned; reconciliation below records
        // whatever durable state the database can still accept.
        for active in &self.inner.active {
            active.cancel.cancel();
        }
        for external in &self.inner.external_tasks {
            external.cancel.cancel();
        }

        TaskRuntimeShutdownPhase {
            deadline: Instant::now() + grace,
            intent,
        }
    }

    async fn finish_shutdown_inner(
        &self,
        supervisor: Option<&ExecutionSupervisor>,
        phase: TaskRuntimeShutdownPhase,
    ) -> Result<TaskRuntimeShutdownReport, TaskRuntimeError> {
        let task_drain = drain_task_runtime_owners(&self.inner, phase.deadline);
        let leaf_drain = drain_execution_supervisor(supervisor, phase.deadline);
        let ((handles, stuck, task_drained), leaf_report) = tokio::join!(task_drain, leaf_drain);

        // Even cooperative executors leave their Runs in `cancelling` during a
        // service shutdown. This is deliberately the sole reconciliation call,
        // and it occurs only after both Task and leaf ownership domains reached
        // their bounded drain boundary.
        let reconciliation = self.inner.db.reconcile_runtime_after_restart().await;
        let stuck_task_runs = stuck
            .iter()
            .map(|(task_id, run_id)| (task_id.clone(), run_id.clone()))
            .collect::<Vec<_>>();
        let cleanup_unconfirmed = if reconciliation.is_ok() {
            Some(
                self.inner
                    .db
                    .mark_shutdown_cleanup_unconfirmed(&stuck_task_runs)
                    .await,
            )
        } else {
            None
        };

        // At this point every unfinished owner has a durable interrupted /
        // unconfirmed fact. Aborting wrapper handles breaks their Arc cycle;
        // the process-wide Tokio teardown remains the last-resort owner of any
        // cancellation-insensitive nested future.
        for pending in handles {
            pending.handle.abort();
        }
        self.inner.active.clear();
        self.inner.cleanup_reapers.clear();
        self.inner.external_tasks.clear();
        self.inner.parent_resolvers.clear();

        // Prefer the original shutdown-intent error after all cleanup work has
        // run. Reconciliation may have repaired the durable projection, but the
        // caller still needs to know that the ordered cancellation boundary was
        // unavailable when shutdown began.
        let (tasks_requested, runs_requested) = phase.intent?;
        let reconciliation = reconciliation.map_err(TaskRuntimeError::storage)?;
        let cleanup_unconfirmed = cleanup_unconfirmed
            .expect("cleanup projection follows successful reconciliation")
            .map_err(TaskRuntimeError::storage)?;

        Ok(TaskRuntimeShutdownReport {
            intake_closed: true,
            tasks_requested,
            runs_requested,
            runs_interrupted: reconciliation.runs_interrupted,
            cleanup_unconfirmed,
            local_owners_timed_out: stuck.len(),
            leaf_owners_requested: leaf_report.owners_requested,
            leaf_owners_remaining: leaf_report.owners_remaining,
            drained: task_drained && leaf_report.drained,
        })
    }

    /// Register a worker for the separately gated Swarm HTTP surface.
    ///
    /// The adapter intentionally reuses the final `TaskRuntime` persistence contract:
    /// the caller-visible worker id is a `UUIDv4` Task id, and the queued Run plus
    /// internal transcript Session exist before the coordinator may execute it.
    /// New Agent and `TaskCreate` paths must continue to use [`Self::submit_child`].
    pub async fn register_external_task(
        &self,
        worker_id: &str,
        request: ChildTaskSubmission,
        cancel: CancellationToken,
    ) -> Result<TaskSubmissionReceipt, String> {
        let _intake = self
            .execution_intake()
            .await
            .map_err(|error| error.to_string())?;
        validate_submission(&request).map_err(|error| error.to_string())?;
        let execution_config_json =
            durable_submission_config(&request).map_err(|error| error.to_string())?;
        let parsed =
            Uuid::parse_str(worker_id).map_err(|_| "SWARM_WORKER_ID_MUST_BE_UUID_V4".to_owned())?;
        if parsed.get_version() != Some(uuid::Version::Random)
            || worker_id != parsed.hyphenated().to_string()
        {
            return Err("SWARM_WORKER_ID_MUST_BE_UUID_V4".to_owned());
        }
        if request.task_type != "agent" {
            return Err("UNSUPPORTED_CAPABILITY: Swarm workers are Agent tasks".to_owned());
        }

        let run_id = Uuid::new_v4().to_string();
        let transcript_session_id = Uuid::new_v4().to_string();
        let durable = self
            .inner
            .db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: worker_id.to_owned(),
                run_id,
                root_session_id: request.root_session_id.clone(),
                transcript_session_id,
                parent_task_id: Some(request.parent_task_id.clone()),
                parent_run_id: Some(request.parent_run_id.clone()),
                creator_tool_use_id: Some(request.creator_tool_use_id.clone()),
                ordinal: request.ordinal,
                description: request.description.clone(),
                prompt: Some(request.prompt.clone()),
                task_type: "agent".to_owned(),
                model: request.model,
                working_dir: request.working_dir,
                execution_config_json,
                startup_epoch: request.startup_epoch,
            })
            .await
            .map_err(|error| format!("TASK_STORAGE_ERROR: {error}"))?;
        if durable.created {
            record_task_event(
                &self.inner.observability,
                &request.root_session_id,
                &durable.task.id,
                "submit",
                "queued",
            );
        }
        self.inner.external_tasks.insert(
            worker_id.to_owned(),
            ExternalTaskRegistration {
                task_id: durable.task.id.clone(),
                run_id: durable.run_id.clone(),
                transcript_session_id: durable.transcript_session_id.clone(),
                root_session_id: request.root_session_id,
                cancel,
            },
        );
        Ok(TaskSubmissionReceipt {
            task: durable.task,
            run_id: durable.run_id,
            transcript_session_id: durable.transcript_session_id,
            created: durable.created,
        })
    }

    /// Atomically claim the pre-created Swarm worker Task and Run.
    pub async fn mark_external_task_running(
        &self,
        worker_id: &str,
        session_id: &str,
    ) -> Result<ExternalTaskExecution, String> {
        let _intake = self
            .execution_intake()
            .await
            .map_err(|error| error.to_string())?;
        let registration = self
            .inner
            .external_tasks
            .get(worker_id)
            .map(|entry| entry.clone())
            .ok_or_else(|| format!("TASK_NOT_FOUND: {worker_id}"))?;
        if registration.root_session_id != session_id {
            return Err("TASK_ACCESS_DENIED".to_owned());
        }
        if !claim_task(&self.inner, &registration.task_id).await {
            return Err("TASK_CLAIM_FAILED".to_owned());
        }
        let task = self
            .inner
            .db
            .find_runtime_task_by_id(&registration.task_id)
            .await
            .map_err(|error| format!("TASK_STORAGE_ERROR: {error}"))?
            .ok_or_else(|| format!("TASK_NOT_FOUND: {}", registration.task_id))?;
        Ok(ExternalTaskExecution {
            task_id: registration.task_id,
            run_id: registration.run_id,
            transcript_session_id: registration.transcript_session_id,
            budget: TaskBudgetLimits {
                token_limit: task.token_budget_limit,
                cost_limit_nanos_usd: task.cost_budget_nanos_usd,
                deadline_at_ms: task.deadline_at_ms,
            },
        })
    }

    /// Commit the Swarm worker outcome through the immutable `TaskResult` transaction.
    pub async fn finish_external_task(
        &self,
        worker_id: &str,
        session_id: &str,
        result: &Result<String, String>,
    ) -> Result<(), String> {
        let registration = self
            .inner
            .external_tasks
            .get(worker_id)
            .map(|entry| entry.clone())
            .ok_or_else(|| format!("TASK_NOT_FOUND: {worker_id}"))?;
        if registration.root_session_id != session_id {
            return Err("TASK_ACCESS_DENIED".to_owned());
        }
        let outcome = match result {
            Ok(output) => TaskExecutionResult::Complete(output.clone()),
            Err(message) => TaskExecutionResult::Failed {
                message: message.clone(),
                code: "SWARM_WORKER_FAILED".to_owned(),
            },
        };
        reap_outcome_until_durable(
            &self.inner,
            &registration.task_id,
            &registration.run_id,
            &registration.root_session_id,
            outcome,
            CleanupStatus::Confirmed,
        )
        .await;
        self.inner.external_tasks.remove(worker_id);
        let task = self
            .inner
            .db
            .find_runtime_task_by_id(&registration.task_id)
            .await
            .map_err(|error| format!("TASK_STORAGE_ERROR: {error}"))?
            .ok_or_else(|| format!("TASK_NOT_FOUND: {}", registration.task_id))?;
        if !task.status.is_terminal() {
            return Err("TASK_RESULT_COMMIT_FAILED".to_owned());
        }
        Ok(())
    }

    /// Stop a gated Swarm worker without pretending that process cleanup was confirmed.
    /// The immutable partial result remains queryable even if the coordinator drops its
    /// worker future before the executor reaches a safe boundary.
    pub async fn cancel_external_task(
        &self,
        worker_id: &str,
        session_id: &str,
        reason: &str,
    ) -> Result<(), String> {
        let registration = self
            .inner
            .external_tasks
            .get(worker_id)
            .map(|entry| entry.clone())
            .ok_or_else(|| format!("TASK_NOT_FOUND: {worker_id}"))?;
        if registration.root_session_id != session_id {
            return Err("TASK_ACCESS_DENIED".to_owned());
        }
        request_cancelling(
            &self.inner,
            &registration.task_id,
            zk_db::run::EXIT_USER_CANCELLED,
            reason,
        )
        .await
        .map_err(|error| error.to_string())?;
        registration.cancel.cancel();
        reap_outcome_until_durable(
            &self.inner,
            &registration.task_id,
            &registration.run_id,
            &registration.root_session_id,
            TaskExecutionResult::Partial {
                content: reason.to_owned(),
                code: "CLEANUP_UNCONFIRMED".to_owned(),
            },
            CleanupStatus::Unconfirmed,
        )
        .await;
        self.inner.external_tasks.remove(worker_id);
        Ok(())
    }
}

struct ShutdownHandle {
    task_id: String,
    run_id: Option<String>,
    handle: JoinHandle<()>,
}

async fn drain_execution_supervisor(
    supervisor: Option<&ExecutionSupervisor>,
    deadline: Instant,
) -> ToolExecutorShutdownReport {
    let Some(supervisor) = supervisor else {
        return ToolExecutorShutdownReport {
            owners_requested: 0,
            owners_remaining: 0,
            drained: true,
        };
    };
    supervisor
        .shutdown(deadline.saturating_duration_since(Instant::now()))
        .await
}

async fn drain_task_runtime_owners(
    inner: &TaskRuntimeInner,
    deadline: Instant,
) -> (Vec<ShutdownHandle>, BTreeMap<String, String>, bool) {
    let mut handles = take_shutdown_handles(inner);
    loop {
        collect_cleanup_reapers(inner, &mut handles);
        reap_finished_shutdown_handles(&mut handles).await;
        if handles.is_empty()
            && inner.active.is_empty()
            && inner.cleanup_reapers.is_empty()
            && inner.external_tasks.is_empty()
            && inner.parent_resolvers.is_empty()
        {
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }

    collect_cleanup_reapers(inner, &mut handles);
    reap_finished_shutdown_handles(&mut handles).await;
    let stuck = shutdown_stuck_task_runs(inner, &handles).await;
    let drained =
        stuck.is_empty() && inner.external_tasks.is_empty() && inner.parent_resolvers.is_empty();
    (handles, stuck, drained)
}

fn runtime_shutting_down() -> TaskRuntimeError {
    TaskRuntimeError::new(
        "RUNTIME_SHUTTING_DOWN",
        "TaskRuntime execution intake is closed",
        true,
    )
}

fn take_shutdown_handles(inner: &TaskRuntimeInner) -> Vec<ShutdownHandle> {
    let mut handles = Vec::new();
    for active in &inner.active {
        if let Some(handle) = active
            .driver
            .lock()
            .expect("task driver lock poisoned")
            .take()
        {
            handles.push(ShutdownHandle {
                task_id: active.key().clone(),
                run_id: Some(active.run_id.clone()),
                handle,
            });
        }
    }
    collect_cleanup_reapers(inner, &mut handles);
    handles
}

fn collect_cleanup_reapers(inner: &TaskRuntimeInner, handles: &mut Vec<ShutdownHandle>) {
    let keys = inner
        .cleanup_reapers
        .iter()
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    for task_id in keys {
        let run_id = inner
            .active
            .get(&task_id)
            .map(|active| active.run_id.clone());
        if let Some((_key, handle)) = inner.cleanup_reapers.remove(&task_id) {
            handles.push(ShutdownHandle {
                task_id,
                run_id,
                handle,
            });
        }
    }
}

async fn reap_finished_shutdown_handles(handles: &mut Vec<ShutdownHandle>) {
    let mut index = 0;
    while index < handles.len() {
        if handles[index].handle.is_finished() {
            let finished = handles.swap_remove(index);
            let _ = finished.handle.await;
        } else {
            index += 1;
        }
    }
}

async fn shutdown_stuck_task_runs(
    inner: &TaskRuntimeInner,
    handles: &[ShutdownHandle],
) -> BTreeMap<String, String> {
    let mut stuck = BTreeMap::new();
    for pending in handles {
        if let Some(run_id) = pending.run_id.as_ref() {
            stuck.insert(pending.task_id.clone(), run_id.clone());
        }
    }
    for active in &inner.active {
        stuck.insert(active.key().clone(), active.run_id.clone());
    }
    for external in &inner.external_tasks {
        stuck.insert(external.task_id.clone(), external.run_id.clone());
    }

    // A cleanup reaper can outlive the active accelerator entry. Resolve its
    // current durable Run rather than dropping the conservative signal.
    for task_id in handles
        .iter()
        .filter(|pending| pending.run_id.is_none())
        .map(|pending| pending.task_id.as_str())
    {
        if let Ok(Some(task)) = inner.db.find_runtime_task_by_id(task_id).await
            && let Some(run_id) = task.current_run_id
        {
            stuck.insert(task_id.to_owned(), run_id);
        }
    }
    stuck
}

fn validate_submission(request: &ChildTaskSubmission) -> Result<(), TaskRuntimeError> {
    if request.parent_task_id.is_empty()
        || request.parent_run_id.is_empty()
        || request.creator_tool_use_id.is_empty()
    {
        return Err(TaskRuntimeError::new(
            "TASK_PARENT_IDENTITY_INCOMPLETE",
            "attached child requires parentTaskId, parentRunId and creatorToolUseId",
            false,
        ));
    }
    if request.description.trim().is_empty() || request.prompt.trim().is_empty() {
        return Err(TaskRuntimeError::new(
            "TASK_INPUT_INVALID",
            "description and prompt must be non-empty",
            false,
        ));
    }
    if request.task_type != "agent" {
        return Err(TaskRuntimeError::new(
            "UNSUPPORTED_CAPABILITY",
            "v1 TaskRuntime supports only agent tasks; Swarm workers are adapters over Agent",
            false,
        ));
    }
    serde_json::from_str::<serde_json::Value>(&request.execution_config_json).map_err(|error| {
        TaskRuntimeError::new(
            "TASK_EXECUTION_CONFIG_INVALID",
            format!("executionConfig must be JSON: {error}"),
            false,
        )
    })?;
    if request.timeout.is_zero() || request.timeout > Duration::from_mins(30) {
        return Err(TaskRuntimeError::new(
            "TASK_TIMEOUT_INVALID",
            "task timeout must be between 1 and 1800 seconds",
            false,
        ));
    }
    Ok(())
}

fn durable_submission_config(request: &ChildTaskSubmission) -> Result<String, TaskRuntimeError> {
    let mut config = serde_json::from_str::<serde_json::Value>(&request.execution_config_json)
        .map_err(|error| {
            TaskRuntimeError::new(
                "TASK_EXECUTION_CONFIG_INVALID",
                format!("executionConfig must be JSON: {error}"),
                false,
            )
        })?;
    let Some(config) = config.as_object_mut() else {
        return Err(TaskRuntimeError::new(
            "TASK_EXECUTION_CONFIG_INVALID",
            "executionConfig must be a JSON object",
            false,
        ));
    };
    let timeout_ms = u64::try_from(request.timeout.as_millis()).map_err(|_| {
        TaskRuntimeError::new(
            "TASK_TIMEOUT_INVALID",
            "task timeout cannot be represented in milliseconds",
            false,
        )
    })?;
    config.insert(
        "taskTimeoutMs".to_owned(),
        serde_json::Value::Number(timeout_ms.into()),
    );
    serde_json::to_string(config).map_err(|error| {
        TaskRuntimeError::new(
            "TASK_EXECUTION_CONFIG_INVALID",
            format!("executionConfig cannot be persisted: {error}"),
            false,
        )
    })
}

fn sleep_until(deadline: Instant) -> impl Future<Output = ()> {
    tokio::time::sleep_until(deadline)
}

/// Observe the durable parent rather than relying on the lifetime of the Agent
/// tool future. `waitMode=background` intentionally lets that future return,
/// but attached cancellation responsibility must survive it.
async fn wait_for_parent_stop(db: zk_db::Db, parent_task_id: Option<String>) {
    let Some(parent_task_id) = parent_task_id else {
        std::future::pending::<()>().await;
        return;
    };
    loop {
        match db.find_runtime_task_by_id(&parent_task_id).await {
            Ok(Some(parent))
                if parent.status == DurableTaskStatus::Cancelling
                    || parent.status == DurableTaskStatus::NeedsAttention
                    || parent.status.is_terminal() =>
            {
                return;
            }
            Ok(Some(_)) => {}
            // A deleted parent cannot continue to own an attached execution.
            Ok(None) => return,
            Err(error) => {
                error!(parent_task_id, %error, "retrying attached parent state observation");
            }
        }
        sleep(Duration::from_millis(25)).await;
    }
}

async fn drive_task<F, Fut>(
    inner: Arc<TaskRuntimeInner>,
    created_task: RuntimeTaskRecord,
    run_id: String,
    transcript_session_id: String,
    root_session_id: String,
    task_timeout: Duration,
    cancel: CancellationToken,
    build: F,
) where
    F: FnOnce(TaskExecutionContext) -> Fut + Send + 'static,
    Fut: Future<Output = TaskExecutionResult> + Send + 'static,
{
    let task_id = created_task.id.clone();
    let root_id = created_task.root_task_id.clone();
    let root_slots = inner
        .root_slots
        .entry(root_id)
        .or_insert_with(|| Arc::new(Semaphore::new(ROOT_AGENT_LIMIT)))
        .clone();
    // One absolute deadline covers durable queue wait and execution. A queued task
    // cannot gain a fresh timeout window merely because capacity was unavailable.
    let deadline = Instant::now() + task_timeout;
    let parent_stopped =
        wait_for_parent_stop(inner.db.clone(), created_task.parent_task_id.clone());
    tokio::pin!(parent_stopped);

    let permits = tokio::select! {
        biased;
        () = sleep_until(deadline) => (None, true, false),
        () = &mut parent_stopped => (None, false, true),
        () = cancel.cancelled() => (None, false, false),
        acquired = async {
            let root = root_slots.acquire_owned().await.ok()?;
            let global = Arc::clone(&inner.global_slots).acquire_owned().await.ok()?;
            Some((root, global))
        } => (acquired, false, false),
    };
    let (permits, deadline_expired, parent_stopped_before_claim) = permits;
    let Some((_root_permit, _global_permit)) = permits else {
        if !inner.accepting_execution.load(Ordering::Acquire) {
            // Process shutdown already persisted `serviceRestart` for every
            // active durable Run before signalling this token. Keep the Run
            // non-terminal for the shutdown reconciler instead of manufacturing
            // a user-cancelled result.
            inner.active.remove(&task_id);
            return;
        }
        let outcome = if parent_stopped_before_claim {
            request_cancelling_until_durable(
                &inner,
                &task_id,
                zk_db::run::EXIT_PARENT_CANCELLED,
                "attached parent stopped before execution claim",
            )
            .await;
            TaskExecutionResult::Cancelled {
                message: "attached parent stopped before execution claim".to_owned(),
            }
        } else if deadline_expired {
            request_cancelling_until_durable(
                &inner,
                &task_id,
                zk_db::run::EXIT_TIMEOUT,
                "task deadline expired before execution claim",
            )
            .await;
            TaskExecutionResult::Failed {
                message: "task deadline expired before execution claim".to_owned(),
                code: "SUBAGENT_DEADLINE_EXCEEDED".to_owned(),
            }
        } else {
            TaskExecutionResult::Cancelled {
                message: "cancelled before execution claim".to_owned(),
            }
        };
        reap_outcome_until_durable(
            &inner,
            &task_id,
            &run_id,
            &root_session_id,
            outcome,
            CleanupStatus::NotRequired,
        )
        .await;
        inner.active.remove(&task_id);
        return;
    };

    let claimed = claim_task(&inner, &task_id).await;
    if !claimed {
        if !inner.accepting_execution.load(Ordering::Acquire) {
            inner.active.remove(&task_id);
            return;
        }
        let outcome = TaskExecutionResult::Cancelled {
            message: "cancelled before execution started".to_owned(),
        };
        reap_outcome_until_durable(
            &inner,
            &task_id,
            &run_id,
            &root_session_id,
            outcome,
            CleanupStatus::NotRequired,
        )
        .await;
        inner.active.remove(&task_id);
        return;
    }

    let context = TaskExecutionContext {
        task_id: task_id.clone(),
        run_id: run_id.clone(),
        transcript_session_id,
        root_session_id: root_session_id.clone(),
        budget: TaskBudgetLimits {
            token_limit: created_task.token_budget_limit,
            cost_limit_nanos_usd: created_task.cost_budget_nanos_usd,
            deadline_at_ms: created_task.deadline_at_ms,
        },
        cancel: cancel.clone(),
    };
    let mut execution = tokio::spawn(build(context));
    let terminal = tokio::select! {
        biased;
        () = sleep_until(deadline) => {
            let boundary = request_cancelling_before_signal(
                &inner,
                &task_id,
                zk_db::run::EXIT_TIMEOUT,
                "task execution deadline expired",
                &mut execution,
            )
            .await;
            match boundary {
                CancellationBoundary::Persisted => {
                    cancel.cancel();
                    await_cleanup_or_reap(
                        Arc::clone(&inner), &task_id, execution,
                        TaskExecutionResult::Failed {
                            message: format!("task timed out after {} seconds", task_timeout.as_secs()),
                            code: "TIMEOUT".to_owned(),
                        },
                    ).await
                }
                CancellationBoundary::ExecutionFinished(outcome) => {
                    Some((outcome, CleanupStatus::Confirmed))
                }
            }
        }
        () = &mut parent_stopped => {
            let boundary = request_cancelling_before_signal(
                &inner,
                &task_id,
                zk_db::run::EXIT_PARENT_CANCELLED,
                "attached parent stopped during child execution",
                &mut execution,
            )
            .await;
            match boundary {
                CancellationBoundary::Persisted => {
                    cancel.cancel();
                    await_cleanup_or_reap(
                        Arc::clone(&inner), &task_id, execution,
                        TaskExecutionResult::Cancelled {
                            message: "attached parent stopped during child execution".to_owned(),
                        },
                    ).await
                }
                CancellationBoundary::ExecutionFinished(outcome) => {
                    Some((outcome, CleanupStatus::Confirmed))
                }
            }
        }
        () = cancel.cancelled() => {
            let requested_outcome = if inner.db.find_run_by_id(&run_id).await.ok().flatten()
                .is_some_and(|run| run.requested_exit_reason.as_deref() == Some(zk_db::run::EXIT_TIMEOUT)) {
                TaskExecutionResult::Failed { message: "task execution deadline expired".to_owned(), code: "TIMEOUT".to_owned() }
            } else {
                TaskExecutionResult::Cancelled { message: "task cancelled".to_owned() }
            };
            let cancelled = await_cleanup_or_reap(
                Arc::clone(&inner), &task_id, execution,
                requested_outcome,
            ).await;
            if inner.accepting_execution.load(Ordering::Acquire) {
                cancelled
            } else {
                // Intent is already durable as `serviceRestart`. The global
                // shutdown reconciler owns the interrupted/cleanup projection.
                None
            }
        }
        joined = &mut execution => Some((map_join(joined), CleanupStatus::Confirmed)),
    };
    if let Some((outcome, cleanup)) = terminal {
        reap_outcome_until_durable(
            &inner,
            &task_id,
            &run_id,
            &root_session_id,
            outcome,
            cleanup,
        )
        .await;
    }
    inner.active.remove(&task_id);
}

async fn claim_task(inner: &TaskRuntimeInner, task_id: &str) -> bool {
    for _ in 0..4 {
        let Ok(Some(task)) = inner.db.find_runtime_task_by_id(task_id).await else {
            return false;
        };
        if task.status == DurableTaskStatus::Cancelling || task.status.is_terminal() {
            return false;
        }
        if task.status == DurableTaskStatus::Running {
            return true;
        }
        let Some(run_id) = task.current_run_id.clone() else {
            return false;
        };
        let task_id_owned = task_id.to_owned();
        let expected_version = task.version;
        let run_id_for_claim = run_id.clone();
        let claimed = inner
            .db
            .with_writer(move |connection| {
                let tx = connection.transaction()?;
                let now = zk_db::time::format_rfc3339_micros(zk_db::time::now_millis());
                let task_updated = tx.execute(
                    "UPDATE tasks SET status='running',reason=NULL,updated_at=?1,version=version+1
                     WHERE id=?2 AND version=?3 AND status='queued'",
                    (now.clone(), task_id_owned.clone(), expected_version),
                )?;
                if task_updated != 1 {
                    return Ok(false);
                }
                let run_updated = tx.execute(
                    "UPDATE run_envelopes SET status='running',updated_at=?1,version=version+1
                     WHERE id=?2 AND task_id=?3 AND status='queued'",
                    (now, run_id_for_claim.clone(), task_id_owned.clone()),
                )?;
                if run_updated != 1 {
                    return Err(zk_db::DbError::Invalid(
                        "TASK_RUN_CLAIM_MISMATCH".to_owned(),
                    ));
                }
                zk_db::run::append_event_in_current_write(
                    &tx,
                    &run_id_for_claim,
                    "task_claimed",
                    None,
                    &serde_json::json!({"taskId": task_id_owned, "status": "running"}),
                )?;
                tx.commit()?;
                Ok(true)
            })
            .await;
        match claimed {
            Ok(true) => {
                record_task_event(
                    &inner.observability,
                    &task.session_id,
                    task_id,
                    "start",
                    "running",
                );
                return true;
            }
            Ok(false) => {}
            Err(error) => {
                error!(task_id, run_id, %error, "failed to atomically claim task/run");
                return false;
            }
        }
    }
    false
}

/// Move the logical Task and its current Run into cancelling in one `SQLite` transaction.
async fn persist_cancelling(
    inner: &TaskRuntimeInner,
    task: &RuntimeTaskRecord,
    exit_reason: &str,
    detail: &str,
) -> Result<bool, TaskRuntimeError> {
    #[cfg(test)]
    {
        use std::sync::atomic::Ordering;

        inner
            .cancellation_persist_failpoint
            .attempts
            .fetch_add(1, Ordering::SeqCst);
        let injected = inner
            .cancellation_persist_failpoint
            .remaining_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();
        if injected {
            return Err(TaskRuntimeError::new(
                "TASK_CANCELLATION_PERSISTENCE_INJECTED",
                "scripted cancellation persistence failure",
                true,
            ));
        }
    }
    let Some(run_id) = task.current_run_id.clone() else {
        return Err(TaskRuntimeError::new(
            "TASK_RUN_NOT_FOUND",
            "task has no current Run to cancel",
            false,
        ));
    };
    let task_id = task.id.clone();
    let expected_version = task.version;
    let exit_reason = exit_reason.to_owned();
    let detail = detail.to_owned();
    inner
        .db
        .with_writer(move |connection| {
            let tx = connection.transaction()?;
            let now = zk_db::time::format_rfc3339_micros(zk_db::time::now_millis());
            let task_updated = tx.execute(
                "UPDATE tasks SET status='cancelling',reason=?1,cleanup_status='pending',
                    updated_at=?2,version=version+1
                 WHERE id=?3 AND version=?4 AND status IN
                    ('queued','running','waitingDependencies','waitingInteraction')",
                (
                    detail.clone(),
                    now.clone(),
                    task_id.clone(),
                    expected_version,
                ),
            )?;
            if task_updated != 1 {
                return Ok(false);
            }
            let run_updated = tx.execute(
                "UPDATE run_envelopes SET status='cancelling',requested_exit_reason=?1,
                    cleanup_status='pending',updated_at=?2,version=version+1
                 WHERE id=?3 AND task_id=?4 AND status IN
                    ('queued','running','waitingDependencies','waitingInteraction')",
                (exit_reason.clone(), now, run_id.clone(), task_id.clone()),
            )?;
            if run_updated != 1 {
                return Err(zk_db::DbError::Invalid(
                    "TASK_RUN_CANCEL_MISMATCH".to_owned(),
                ));
            }
            zk_db::run::append_event_in_current_write(
                &tx,
                &run_id,
                "task_cancelling",
                None,
                &serde_json::json!({
                    "taskId": task_id,
                    "exitReason": exit_reason,
                    "reason": detail,
                }),
            )?;
            tx.commit()?;
            Ok(true)
        })
        .await
        .map_err(TaskRuntimeError::storage)
}

async fn request_cancelling(
    inner: &TaskRuntimeInner,
    task_id: &str,
    exit_reason: &str,
    detail: &str,
) -> Result<(), TaskRuntimeError> {
    for _ in 0..4 {
        let Some(task) = inner
            .db
            .find_runtime_task_by_id(task_id)
            .await
            .map_err(TaskRuntimeError::storage)?
        else {
            return Err(TaskRuntimeError::new(
                "TASK_NOT_FOUND",
                "task not found",
                false,
            ));
        };
        if task.status == DurableTaskStatus::Cancelling || task.status.is_terminal() {
            return Ok(());
        }
        if persist_cancelling(inner, &task, exit_reason, detail).await? {
            return Ok(());
        }
    }
    Err(TaskRuntimeError::new(
        "TASK_VERSION_CONFLICT",
        "task changed while cancellation was requested",
        true,
    ))
}

async fn request_cancelling_until_durable(
    inner: &TaskRuntimeInner,
    task_id: &str,
    exit_reason: &str,
    detail: &str,
) {
    let mut failure_count = 0_u32;
    loop {
        match request_cancelling(inner, task_id, exit_reason, detail).await {
            Ok(()) => return,
            Err(error) => {
                if failure_count == 0 || failure_count.is_power_of_two() {
                    tracing::error!(
                        task_id,
                        exit_reason,
                        code = error.code,
                        message = %error.message,
                        failure_count,
                        "retaining TaskRuntime owner until cancellation intent is durable"
                    );
                }
                failure_count = failure_count.saturating_add(1);
                sleep(terminal_commit_retry_delay(failure_count)).await;
            }
        }
    }
}

enum CancellationBoundary {
    Persisted,
    ExecutionFinished(TaskExecutionResult),
}

/// Persist a timeout/parent-stop transition before signalling its execution token.
///
/// A transient storage failure cannot make the driver drop its `JoinHandle`: the
/// driver retries while also observing natural executor completion. Whichever
/// boundary becomes durable/observable first wins the race.
async fn request_cancelling_before_signal(
    inner: &TaskRuntimeInner,
    task_id: &str,
    exit_reason: &str,
    detail: &str,
    execution: &mut JoinHandle<TaskExecutionResult>,
) -> CancellationBoundary {
    let mut failure_count = 0_u32;
    loop {
        match request_cancelling(inner, task_id, exit_reason, detail).await {
            Ok(()) => return CancellationBoundary::Persisted,
            Err(error) => {
                if failure_count == 0 || failure_count.is_power_of_two() {
                    tracing::error!(
                        task_id,
                        exit_reason,
                        code = error.code,
                        message = %error.message,
                        failure_count,
                        "cancellation persistence failed; execution ownership retained"
                    );
                }
                failure_count = failure_count.saturating_add(1);
                let delay = sleep(terminal_commit_retry_delay(failure_count));
                tokio::pin!(delay);
                tokio::select! {
                    biased;
                    joined = &mut *execution => {
                        return CancellationBoundary::ExecutionFinished(map_join(joined));
                    }
                    () = &mut delay => {}
                }
            }
        }
    }
}

async fn await_cleanup_or_reap(
    inner: Arc<TaskRuntimeInner>,
    task_id: &str,
    mut execution: JoinHandle<TaskExecutionResult>,
    requested_outcome: TaskExecutionResult,
) -> Option<(TaskExecutionResult, CleanupStatus)> {
    let timed_out =
        matches!(&requested_outcome, TaskExecutionResult::Failed { code, .. } if code == "TIMEOUT");
    let grace = if timed_out {
        TIMEOUT_CLEANUP_GRACE
    } else {
        CLEANUP_GRACE
    };
    if let Ok(joined) = timeout_at(Instant::now() + grace, &mut execution).await {
        let outcome = if timed_out {
            recover_timeout_result(&inner, task_id, map_join(joined)).await
        } else {
            requested_outcome
        };
        Some((outcome, CleanupStatus::Confirmed))
    } else {
        // Do not drop/abort the future: the reaper retains cleanup ownership until the
        // executor reaches a safe boundary. The durable result explicitly says cleanup
        // is unconfirmed, and cancellation is represented as partial rather than cancelled.
        let task_id_owned = task_id.to_owned();
        let inner_for_reaper = Arc::clone(&inner);
        let reaper = tokio::spawn(async move {
            let _ = execution.await;
            inner_for_reaper.cleanup_reapers.remove(&task_id_owned);
        });
        inner.cleanup_reapers.insert(task_id.to_owned(), reaper);
        let (message, code) = match requested_outcome {
            TaskExecutionResult::Failed { message, code } => (message, code),
            TaskExecutionResult::Cancelled { message } => {
                (message, "CLEANUP_UNCONFIRMED".to_owned())
            }
            other => (format!("{other:?}"), "CLEANUP_UNCONFIRMED".to_owned()),
        };
        Some((
            TaskExecutionResult::Partial {
                content: message,
                code,
            },
            CleanupStatus::Unconfirmed,
        ))
    }
}

async fn recover_timeout_result(
    inner: &TaskRuntimeInner,
    task_id: &str,
    outcome: TaskExecutionResult,
) -> TaskExecutionResult {
    let mut content = match outcome {
        failure @ TaskExecutionResult::Failed { .. } => return failure,
        TaskExecutionResult::Complete(text)
        | TaskExecutionResult::Partial { content: text, .. } => text,
        _ => String::new(),
    };
    if let Ok(Some(task)) = inner.db.find_runtime_task_by_id(task_id).await
        && let Some(run_id) = task.current_run_id
    {
        if content.trim().is_empty()
            && let Ok(Some(checkpoint)) = inner.db.latest_agent_checkpoint(&run_id).await
        {
            content = timeout_checkpoint_text(&checkpoint.messages);
        }
        if let Ok(Some(manifest)) = inner.db.find_artifact_manifest_by_run(&run_id).await {
            for entry in manifest.entries {
                use std::fmt::Write as _;
                let _ = write!(
                    content,
                    "\n产物（状态 {}，未作为完成证明）：{}",
                    entry.state, entry.canonical_path
                );
            }
        }
        let source_run = run_id.clone();
        if let Ok(sources) = inner
            .db
            .with_reader(move |conn| {
                let mut statement = conn.prepare(
                "SELECT DISTINCT url FROM research_sources WHERE run_id=?1 ORDER BY url LIMIT 100"
            )?;
                let sources = statement
                    .query_map([source_run], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(sources)
            })
            .await
        {
            for url in sources {
                use std::fmt::Write as _;
                let _ = write!(content, "\n已采集来源（待核验）：{url}");
            }
        }
    }
    let code = "SUBAGENT_DEADLINE_EXCEEDED".to_owned();
    if content.trim().is_empty() {
        TaskExecutionResult::Failed {
            message: "子 Agent 达到执行期限；没有可恢复的正文。".to_owned(),
            code,
        }
    } else {
        TaskExecutionResult::Partial {
            content: format!(
                "[PARTIAL: 子 Agent 达到执行期限，以下是已产出的未完整核验内容]\n\n{content}"
            ),
            code,
        }
    }
}

fn timeout_checkpoint_text(messages: &serde_json::Value) -> String {
    messages
        .as_array()
        .into_iter()
        .flatten()
        .rev()
        .find_map(|message| {
            if message.get("role").and_then(serde_json::Value::as_str) != Some("assistant") {
                return None;
            }
            let content = message.get("content")?;
            let text = if let Some(text) = content.as_str() {
                text.to_owned()
            } else {
                content
                    .as_array()?
                    .iter()
                    .filter_map(|block| {
                        (block.get("type")?.as_str()? == "text")
                            .then(|| block.get("text")?.as_str())
                            .flatten()
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            (!text.trim().is_empty()).then_some(text)
        })
        .unwrap_or_default()
}

fn map_join(joined: Result<TaskExecutionResult, tokio::task::JoinError>) -> TaskExecutionResult {
    match joined {
        Ok(outcome) => outcome,
        Err(error) => TaskExecutionResult::Failed {
            message: format!("task executor terminated unexpectedly: {error}"),
            code: "INTERNAL_ERROR".to_owned(),
        },
    }
}

#[derive(Clone, Debug)]
struct TerminalCommitFailure {
    stage: &'static str,
    detail: String,
}

#[derive(Debug)]
enum TerminalCommitResult {
    Committed {
        task: Box<RuntimeTaskRecord>,
        result: Box<zk_db::TaskResultRecord>,
        content: String,
        cleanup_status: CleanupStatus,
    },
    AlreadyDurable,
    AlreadyNeedsAttention,
    Retryable(TerminalCommitFailure),
    Permanent(TerminalCommitFailure),
}

#[derive(Debug)]
enum NeedsAttentionCommitResult {
    Marked(Box<RuntimeTaskRecord>),
    AlreadyMarked,
    AlreadyDurable,
    Retryable(TerminalCommitFailure),
    Unrecoverable(TerminalCommitFailure),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DurableTerminalization {
    ResultCommitted,
    ResultAlreadyDurable,
    NeedsAttention,
}

fn db_failure(stage: &'static str, error: &zk_db::DbError) -> TerminalCommitResult {
    let failure = TerminalCommitFailure {
        stage,
        detail: error.to_string(),
    };
    if matches!(
        error,
        zk_db::DbError::Sqlite(_) | zk_db::DbError::Io(_) | zk_db::DbError::Join(_)
    ) {
        TerminalCommitResult::Retryable(failure)
    } else {
        TerminalCommitResult::Permanent(failure)
    }
}

const fn effective_cleanup_status(
    requested: CleanupStatus,
    ledger: CleanupStatus,
) -> CleanupStatus {
    match (requested, ledger) {
        (CleanupStatus::Unconfirmed | CleanupStatus::Pending, _)
        | (_, CleanupStatus::Unconfirmed | CleanupStatus::Pending) => CleanupStatus::Unconfirmed,
        (CleanupStatus::Confirmed, _) | (_, CleanupStatus::Confirmed) => CleanupStatus::Confirmed,
        _ => CleanupStatus::NotRequired,
    }
}

async fn existing_terminal_result(
    inner: &TaskRuntimeInner,
    task_id: &str,
    run_id: &str,
) -> TerminalCommitResult {
    match inner
        .db
        .read_task_result(task_id, None, 0, zk_db::INLINE_RESULT_LIMIT)
        .await
    {
        Ok(Some(chunk)) if chunk.result.run_id == run_id => TerminalCommitResult::AlreadyDurable,
        Ok(Some(chunk)) => TerminalCommitResult::Permanent(TerminalCommitFailure {
            stage: "terminal_result_identity",
            detail: format!(
                "latest immutable result belongs to Run {}, expected {run_id}",
                chunk.result.run_id
            ),
        }),
        Ok(None) => TerminalCommitResult::Permanent(TerminalCommitFailure {
            stage: "terminal_result_missing",
            detail: "Task/Run is terminal without an immutable TaskResult".to_owned(),
        }),
        Err(error) => db_failure("read_existing_terminal_result", &error),
    }
}

/// Make one strongly-typed attempt to persist an executor outcome.
///
/// This function never converts a storage failure into a model/tool failure and never
/// publishes. Its caller retains execution ownership until either the immutable result
/// exists or a durable `needsAttention` diagnostic has been committed.
async fn commit_outcome(
    inner: &Arc<TaskRuntimeInner>,
    task_id: &str,
    run_id: &str,
    outcome: &TaskExecutionResult,
    requested_cleanup: CleanupStatus,
) -> TerminalCommitResult {
    #[cfg(test)]
    {
        use std::sync::atomic::Ordering;

        inner
            .terminal_commit_failpoint
            .attempts
            .fetch_add(1, Ordering::SeqCst);
        match inner.terminal_commit_failpoint.mode.load(Ordering::SeqCst) {
            1 => {
                return TerminalCommitResult::Retryable(TerminalCommitFailure {
                    stage: "injected_terminal_commit",
                    detail: "scripted retryable persistence failure".to_owned(),
                });
            }
            2 => {
                return TerminalCommitResult::Permanent(TerminalCommitFailure {
                    stage: "injected_terminal_commit",
                    detail: "scripted permanent persistence failure".to_owned(),
                });
            }
            _ => {}
        }
    }

    let task = match inner.db.find_runtime_task_by_id(task_id).await {
        Ok(Some(task)) => task,
        Ok(None) => {
            return TerminalCommitResult::Permanent(TerminalCommitFailure {
                stage: "load_task",
                detail: "durable Task identity is missing".to_owned(),
            });
        }
        Err(error) => return db_failure("load_task", &error),
    };
    if task.status == DurableTaskStatus::NeedsAttention {
        return TerminalCommitResult::AlreadyNeedsAttention;
    }
    if task.status.is_terminal() {
        return existing_terminal_result(inner, task_id, run_id).await;
    }
    if task.current_run_id.as_deref() != Some(run_id) {
        return TerminalCommitResult::Permanent(TerminalCommitFailure {
            stage: "validate_task_run",
            detail: format!(
                "Task current Run is {:?}, expected {run_id}",
                task.current_run_id
            ),
        });
    }

    let ledger_cleanup = match inner.db.run_cleanup_status(run_id).await {
        Ok(status) => status,
        Err(error) => return db_failure("read_cleanup_ledger", &error),
    };
    let cleanup_status = effective_cleanup_status(requested_cleanup, ledger_cleanup);
    let effective_outcome = match (outcome.clone(), cleanup_status) {
        (TaskExecutionResult::Cancelled { message }, CleanupStatus::Unconfirmed) => {
            TaskExecutionResult::Partial {
                content: message,
                code: "CLEANUP_UNCONFIRMED".to_owned(),
            }
        }
        (TaskExecutionResult::Complete(content), CleanupStatus::Unconfirmed) => {
            TaskExecutionResult::Partial {
                content,
                code: "CLEANUP_UNCONFIRMED".to_owned(),
            }
        }
        (outcome, _) => outcome,
    };
    if let TaskExecutionResult::Complete(content) = &effective_outcome
        && content.len() <= zk_db::RESULT_HARD_LIMIT
        && let Err(error) = inner
            .db
            .ensure_task_final_assistant(task_id, run_id, content)
            .await
    {
        return db_failure("persist_final_assistant", &error);
    }
    let (status, content, error_code) = match effective_outcome {
        TaskExecutionResult::Complete(content) => (ResultStatus::Complete, content, None),
        TaskExecutionResult::Partial { content, code } => {
            (ResultStatus::Partial, content, Some(code))
        }
        TaskExecutionResult::Failed { message, code } => (ResultStatus::Error, message, Some(code)),
        TaskExecutionResult::Cancelled { message } => (
            ResultStatus::Cancelled,
            message,
            Some("USER_CANCELLED".to_owned()),
        ),
    };
    let request = CommitTaskResult {
        task_id: task_id.to_owned(),
        run_id: run_id.to_owned(),
        expected_task_version: task.version,
        status,
        content: content.clone(),
        media_type: "text/markdown".to_owned(),
        error_code,
        cleanup_status,
        verification_status: VerificationStatus::NotRequested,
    };
    match inner.db.commit_task_result(&request).await {
        Ok(CommitTaskResultOutcome::Committed { result, .. }) => TerminalCommitResult::Committed {
            task: Box::new(task),
            result: Box::new(result),
            content,
            cleanup_status,
        },
        Ok(CommitTaskResultOutcome::AlreadyTerminal) => {
            existing_terminal_result(inner, task_id, run_id).await
        }
        Ok(CommitTaskResultOutcome::VersionConflict) => {
            TerminalCommitResult::Retryable(TerminalCommitFailure {
                stage: "commit_result_cas",
                detail: "Task version changed while committing the result".to_owned(),
            })
        }
        Ok(CommitTaskResultOutcome::InvalidRun) => {
            TerminalCommitResult::Permanent(TerminalCommitFailure {
                stage: "commit_result_identity",
                detail: "Task no longer points to the executor Run".to_owned(),
            })
        }
        Ok(CommitTaskResultOutcome::NotFound) => {
            TerminalCommitResult::Permanent(TerminalCommitFailure {
                stage: "commit_result_identity",
                detail: "Task disappeared while committing the result".to_owned(),
            })
        }
        Err(error) => db_failure("commit_immutable_result", &error),
    }
}

async fn persist_commit_failure_as_needs_attention(
    inner: &Arc<TaskRuntimeInner>,
    task_id: &str,
    run_id: &str,
    requested_cleanup: CleanupStatus,
    failure: &TerminalCommitFailure,
) -> NeedsAttentionCommitResult {
    let task = match inner.db.find_runtime_task_by_id(task_id).await {
        Ok(Some(task)) => task,
        Ok(None) => {
            return NeedsAttentionCommitResult::Unrecoverable(TerminalCommitFailure {
                stage: "persist_needs_attention",
                detail: "cannot diagnose a missing durable Task".to_owned(),
            });
        }
        Err(error) => {
            return match db_failure("load_task_for_needs_attention", &error) {
                TerminalCommitResult::Retryable(failure) => {
                    NeedsAttentionCommitResult::Retryable(failure)
                }
                TerminalCommitResult::Permanent(failure) => {
                    NeedsAttentionCommitResult::Unrecoverable(failure)
                }
                _ => unreachable!("db_failure returns only failure variants"),
            };
        }
    };
    if task.status == DurableTaskStatus::NeedsAttention {
        return NeedsAttentionCommitResult::AlreadyMarked;
    }
    if task.status.is_terminal() {
        return match existing_terminal_result(inner, task_id, run_id).await {
            TerminalCommitResult::AlreadyDurable => NeedsAttentionCommitResult::AlreadyDurable,
            TerminalCommitResult::Retryable(failure) => {
                NeedsAttentionCommitResult::Retryable(failure)
            }
            TerminalCommitResult::Permanent(failure) => {
                NeedsAttentionCommitResult::Unrecoverable(failure)
            }
            _ => unreachable!("terminal result inspection cannot commit"),
        };
    }
    if task.current_run_id.as_deref() != Some(run_id) {
        return NeedsAttentionCommitResult::Unrecoverable(TerminalCommitFailure {
            stage: "persist_needs_attention",
            detail: "refusing to diagnose a stale execution over a newer Run".to_owned(),
        });
    }

    let ledger_cleanup = match inner.db.run_cleanup_status(run_id).await {
        Ok(status) => status,
        Err(error) => {
            return match db_failure("read_cleanup_for_needs_attention", &error) {
                TerminalCommitResult::Retryable(failure) => {
                    NeedsAttentionCommitResult::Retryable(failure)
                }
                TerminalCommitResult::Permanent(failure) => {
                    NeedsAttentionCommitResult::Unrecoverable(failure)
                }
                _ => unreachable!("db_failure returns only failure variants"),
            };
        }
    };
    let cleanup_status = effective_cleanup_status(requested_cleanup, ledger_cleanup);

    let diagnostic = bounded_summary(
        &format!(
            "TERMINAL_RESULT_COMMIT_FAILED at {}: {}",
            failure.stage, failure.detail
        ),
        2048,
    );
    let task_id_owned = task_id.to_owned();
    let run_id_owned = run_id.to_owned();
    let diagnostic_for_write = diagnostic.clone();
    let expected_version = task.version;
    let marked = inner
        .db
        .with_writer(move |connection| {
            let tx = connection.transaction()?;
            let now = zk_db::time::format_rfc3339_micros(zk_db::time::now_millis());
            let task_updated = tx.execute(
                "UPDATE tasks SET status='needsAttention',reason=?1,cleanup_status=?2,
                    verification_status='blocked',updated_at=?3,version=version+1
                 WHERE id=?4 AND current_run_id=?5 AND version=?6 AND status IN
                    ('queued','running','waitingDependencies','waitingInteraction','cancelling')",
                (
                    diagnostic_for_write.clone(),
                    cleanup_status.as_db(),
                    now.clone(),
                    task_id_owned.clone(),
                    run_id_owned.clone(),
                    expected_version,
                ),
            )?;
            if task_updated != 1 {
                return Ok(false);
            }
            let run_updated = tx.execute(
                "UPDATE run_envelopes SET status='interrupted',exit_reason='internalError',
                    error_summary=?1,cleanup_status=?2,verification_status='blocked',
                    finished_at=?3,terminal_at=?3,updated_at=?3,version=version+1
                 WHERE id=?4 AND task_id=?5 AND status IN
                    ('queued','running','waitingDependencies','waitingInteraction','cancelling')",
                (
                    diagnostic_for_write.clone(),
                    cleanup_status.as_db(),
                    now,
                    run_id_owned.clone(),
                    task_id_owned.clone(),
                ),
            )?;
            if run_updated != 1 {
                return Err(zk_db::DbError::Invalid(
                    "TASK_RUN_NEEDS_ATTENTION_MISMATCH".to_owned(),
                ));
            }
            zk_db::run::append_event_in_current_write(
                &tx,
                &run_id_owned,
                "task_needs_attention",
                None,
                &serde_json::json!({
                    "taskId": task_id_owned,
                    "runId": run_id_owned,
                    "reason": diagnostic_for_write,
                }),
            )?;
            tx.commit()?;
            Ok(true)
        })
        .await;
    match marked {
        Ok(true) => {
            let mut marked_task = task;
            marked_task.status = DurableTaskStatus::NeedsAttention;
            marked_task.reason = Some(diagnostic);
            marked_task.cleanup_status = cleanup_status;
            marked_task.verification_status = VerificationStatus::Blocked;
            marked_task.version += 1;
            NeedsAttentionCommitResult::Marked(Box::new(marked_task))
        }
        Ok(false) => NeedsAttentionCommitResult::Retryable(TerminalCommitFailure {
            stage: "persist_needs_attention_cas",
            detail: "Task changed while recording the terminal commit failure".to_owned(),
        }),
        Err(error) => match db_failure("persist_needs_attention", &error) {
            TerminalCommitResult::Retryable(failure) => {
                NeedsAttentionCommitResult::Retryable(failure)
            }
            TerminalCommitResult::Permanent(failure) => {
                NeedsAttentionCommitResult::Unrecoverable(failure)
            }
            _ => unreachable!("db_failure returns only failure variants"),
        },
    }
}

fn terminal_commit_retry_delay(failure_count: u32) -> Duration {
    #[cfg(test)]
    const MAX_DELAY_MS: u64 = 20;
    #[cfg(not(test))]
    const MAX_DELAY_MS: u64 = 1_000;

    let exponent = failure_count.min(6);
    Duration::from_millis((10_u64.saturating_mul(1_u64 << exponent)).min(MAX_DELAY_MS))
}

fn log_terminal_commit_retry(
    task_id: &str,
    run_id: &str,
    failure_count: u32,
    failure: &TerminalCommitFailure,
) {
    if failure_count == 0 || failure_count.is_power_of_two() {
        error!(
            task_id,
            run_id,
            stage = failure.stage,
            detail = %failure.detail,
            failure_count,
            "retaining TaskRuntime owner while terminal persistence is retried"
        );
    }
}

async fn publish_committed_outcome(
    inner: &Arc<TaskRuntimeInner>,
    root_session_id: &str,
    task_id: &str,
    mut task: RuntimeTaskRecord,
    result: &zk_db::TaskResultRecord,
    content: &str,
    cleanup_status: CleanupStatus,
) {
    task.status = match result.status {
        ResultStatus::Complete => DurableTaskStatus::Succeeded,
        ResultStatus::Partial => DurableTaskStatus::Partial,
        ResultStatus::Error => DurableTaskStatus::Failed,
        ResultStatus::Cancelled => DurableTaskStatus::Cancelled,
    };
    task.cleanup_status = cleanup_status;
    task.version += 1;
    inner
        .sink
        .push(
            root_session_id,
            ServerMessage::TaskUpdate {
                task_id: task_id.to_owned(),
                status: task.status.as_db().to_owned(),
                progress: None,
                output: matches!(
                    result.status,
                    ResultStatus::Complete | ResultStatus::Partial
                )
                .then(|| bounded_summary(content, zk_db::INLINE_RESULT_LIMIT)),
            },
        )
        .await;
    record_task_event(
        &inner.observability,
        root_session_id,
        task_id,
        "complete",
        if result.status == ResultStatus::Complete {
            "ok"
        } else {
            "error"
        },
    );
    schedule_parent_resolution(inner, &task, result.result_version, content);
}

async fn publish_needs_attention(
    inner: &Arc<TaskRuntimeInner>,
    root_session_id: &str,
    task: &RuntimeTaskRecord,
) {
    inner
        .sink
        .push(
            root_session_id,
            ServerMessage::TaskUpdate {
                task_id: task.id.clone(),
                status: DurableTaskStatus::NeedsAttention.as_db().to_owned(),
                progress: None,
                output: None,
            },
        )
        .await;
    record_task_event(
        &inner.observability,
        root_session_id,
        &task.id,
        "complete",
        "needsAttention",
    );
}

fn notify_terminal_state(inner: &TaskRuntimeInner, task_id: &str) {
    // A terminal child result must unblock the Agent/TaskCreate tool before its parent
    // receipt can be inserted: the tool_result is itself the safe-boundary prerequisite.
    inner
        .result_notify
        .entry(task_id.to_owned())
        .or_insert_with(|| Arc::new(Notify::new()))
        .notify_waiters();
}

/// Persistent in-process finalizer. It owns the completed executor until `SQLite` has a
/// durable terminal fact. Retry delay is exponentially backed off but capped; there is
/// deliberately no retry-count cutoff which could recreate an ownerless running ghost.
async fn reap_outcome_until_durable(
    inner: &Arc<TaskRuntimeInner>,
    task_id: &str,
    run_id: &str,
    root_session_id: &str,
    outcome: TaskExecutionResult,
    cleanup_status: CleanupStatus,
) -> DurableTerminalization {
    let mut failure_count = 0_u32;
    let mut permanent_failure = None;
    loop {
        if let Some(failure) = permanent_failure.as_ref() {
            match persist_commit_failure_as_needs_attention(
                inner,
                task_id,
                run_id,
                cleanup_status,
                failure,
            )
            .await
            {
                NeedsAttentionCommitResult::Marked(task) => {
                    publish_needs_attention(inner, root_session_id, &task).await;
                    notify_terminal_state(inner, task_id);
                    return DurableTerminalization::NeedsAttention;
                }
                NeedsAttentionCommitResult::AlreadyMarked => {
                    notify_terminal_state(inner, task_id);
                    return DurableTerminalization::NeedsAttention;
                }
                NeedsAttentionCommitResult::AlreadyDurable => {
                    notify_terminal_state(inner, task_id);
                    return DurableTerminalization::ResultAlreadyDurable;
                }
                NeedsAttentionCommitResult::Retryable(retry)
                | NeedsAttentionCommitResult::Unrecoverable(retry) => {
                    log_terminal_commit_retry(task_id, run_id, failure_count, &retry);
                }
            }
        } else {
            match commit_outcome(inner, task_id, run_id, &outcome, cleanup_status).await {
                TerminalCommitResult::Committed {
                    task,
                    result,
                    content,
                    cleanup_status,
                } => {
                    publish_committed_outcome(
                        inner,
                        root_session_id,
                        task_id,
                        *task,
                        &result,
                        &content,
                        cleanup_status,
                    )
                    .await;
                    notify_terminal_state(inner, task_id);
                    return DurableTerminalization::ResultCommitted;
                }
                TerminalCommitResult::AlreadyDurable => {
                    notify_terminal_state(inner, task_id);
                    return DurableTerminalization::ResultAlreadyDurable;
                }
                TerminalCommitResult::AlreadyNeedsAttention => {
                    notify_terminal_state(inner, task_id);
                    return DurableTerminalization::NeedsAttention;
                }
                TerminalCommitResult::Retryable(retry) => {
                    log_terminal_commit_retry(task_id, run_id, failure_count, &retry);
                }
                TerminalCommitResult::Permanent(failure) => {
                    error!(
                        task_id,
                        run_id,
                        stage = failure.stage,
                        detail = %failure.detail,
                        "terminal result cannot be committed; persisting needsAttention"
                    );
                    permanent_failure = Some(failure);
                    continue;
                }
            }
        }
        failure_count = failure_count.saturating_add(1);
        sleep(terminal_commit_retry_delay(failure_count)).await;
    }
}

fn schedule_parent_resolution(
    inner: &Arc<TaskRuntimeInner>,
    child: &RuntimeTaskRecord,
    result_version: i64,
    content: &str,
) {
    if child.parent_task_id.is_none() {
        return;
    }
    let resolver_key = format!("{}:{result_version}", child.id);
    match inner.parent_resolvers.entry(resolver_key.clone()) {
        dashmap::mapref::entry::Entry::Occupied(_) => return,
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(());
        }
    }

    let inner = Arc::clone(inner);
    let child = child.clone();
    let summary = bounded_summary(content, 4096);
    tokio::spawn(async move {
        resolve_parent(&inner, &child, result_version, &summary).await;
        inner.parent_resolvers.remove(&resolver_key);
    });
}

async fn resolve_parent(
    inner: &Arc<TaskRuntimeInner>,
    child: &RuntimeTaskRecord,
    result_version: i64,
    summary: &str,
) {
    let Some(parent_id) = child.parent_task_id.as_deref() else {
        return;
    };
    let mut retry = 0_u32;
    loop {
        match inner
            .db
            .ingest_task_result_at_safe_boundary(parent_id, &child.id, result_version, summary)
            .await
        {
            Ok(Some(_receipt)) => {
                inner
                    .result_notify
                    .entry(parent_id.to_owned())
                    .or_insert_with(|| Arc::new(Notify::new()))
                    .notify_waiters();
                return;
            }
            Ok(None) => {}
            Err(zk_db::DbError::Invalid(error)) => {
                error!(task_id = %child.id, parent_task_id = parent_id, %error, "child result cannot be ingested");
                return;
            }
            Err(error) => {
                if retry == 0 || retry.is_power_of_two() {
                    error!(task_id = %child.id, parent_task_id = parent_id, %error, "retrying child result ingestion");
                }
            }
        }

        match inner.db.find_runtime_task_by_id(parent_id).await {
            Ok(Some(parent)) if parent.status == DurableTaskStatus::WaitingDependencies => {}
            Ok(Some(parent))
                if parent.status == DurableTaskStatus::Cancelling
                    || parent.status == DurableTaskStatus::NeedsAttention
                    || parent.status.is_terminal() =>
            {
                return;
            }
            Ok(Some(_) | None) => return,
            Err(error) => {
                if retry == 0 || retry.is_power_of_two() {
                    error!(task_id = %child.id, parent_task_id = parent_id, %error, "retrying parent state lookup");
                }
            }
        }
        retry = retry.saturating_add(1);
        let delay_ms = 10_u64.saturating_mul(1_u64 << retry.min(5));
        sleep(Duration::from_millis(delay_ms)).await;
    }
}

fn bounded_summary(content: &str, max_bytes: usize) -> String {
    if content.len() <= max_bytes {
        return content.to_owned();
    }
    let mut end = max_bytes;
    while !content.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n[result summary truncated; use TaskOutput]",
        &content[..end]
    )
}

fn record_task_event(
    recorder: &Arc<dyn ObservabilityRecorder>,
    session_id: &str,
    task_id: &str,
    action: &str,
    outcome: &str,
) {
    let mut event = ObservabilityEvent::new("task", action, outcome);
    event.session_id = Some(session_id.to_owned());
    event.attributes.insert(
        "taskId".to_owned(),
        serde_json::Value::String(task_id.to_owned()),
    );
    recorder.record(event);
}

impl Drop for TaskRuntimeInner {
    fn drop(&mut self) {
        // Do not abort futures: cancellation starts executor-owned cleanup. During normal app
        // shutdown the Tokio runtime continues driving these handles until its own deadline.
        for active in &self.active {
            active.cancel.cancel();
        }
        for external in &self.external_tasks {
            external.cancel.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use futures::future::BoxFuture;
    use zk_db::{
        CreateTaskWithRun, MessageRole, NewMessage, NewToolInvocation, StoredBlock,
        TaskStatus as DbTaskStatus, ToolInvocationStatus,
    };
    use zk_tools::{Tool, ToolContext, ToolOutput};

    use super::*;

    struct NoopSink;

    #[test]
    fn timeout_recovery_uses_last_assistant_text_only() {
        let messages = serde_json::json!([
            {"role":"assistant","content":[{"type":"text","text":"source-backed finding"}]},
            {"role":"tool","content":"raw tool output"},
            {"role":"assistant","content":[{"type":"thinking","thinking":"private reasoning"}]}
        ]);
        assert_eq!(timeout_checkpoint_text(&messages), "source-backed finding");
        assert!(timeout_checkpoint_text(&serde_json::json!([])).is_empty());
    }

    #[tokio::test]
    async fn timeout_preserves_executor_output_as_partial() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let mut request = submission(&session, &root, &root_run_id);
        request.timeout = Duration::from_millis(25);
        let receipt = runtime
            .submit_child(request, |context| async move {
                context.cancel.cancelled().await;
                TaskExecutionResult::Complete("verified intermediate finding".to_owned())
            })
            .await
            .expect("submit");
        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: receipt.task.id,
                wait_ms: 5000,
                result_version: None,
                cursor: 0,
                max_bytes: 4096,
            })
            .await
            .expect("output");
        assert_eq!(output.task.status, DbTaskStatus::Partial);
        assert_eq!(output.task.reason.as_deref(), Some("timeout"));
        let result = output.result.expect("durable partial result");
        assert_eq!(
            result.result.error_code.as_deref(),
            Some("SUBAGENT_DEADLINE_EXCEEDED")
        );
    }

    impl MessageSink for NoopSink {
        fn push<'a>(&'a self, _session_id: &'a str, _message: ServerMessage) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        pushes: AtomicUsize,
    }

    impl MessageSink for RecordingSink {
        fn push<'a>(&'a self, _session_id: &'a str, _message: ServerMessage) -> BoxFuture<'a, ()> {
            self.pushes.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {})
        }
    }

    struct ShutdownOrderProbeTool {
        db: zk_db::Db,
        run_id: String,
        entered: Arc<Notify>,
        observed_pre_reconcile: Arc<AtomicBool>,
    }

    impl Tool for ShutdownOrderProbeTool {
        fn name(&self) -> &'static str {
            "ShutdownOrderProbe"
        }

        fn description(&self) -> &'static str {
            "observes durable Run state at the leaf cleanup boundary"
        }

        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({ "type": "object" })
        }

        fn is_read_only(&self, _input: &serde_json::Value) -> bool {
            true
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            let db = self.db.clone();
            let run_id = self.run_id.clone();
            let entered = Arc::clone(&self.entered);
            let observed_pre_reconcile = Arc::clone(&self.observed_pre_reconcile);
            Box::pin(async move {
                entered.notify_one();
                ctx.cancel.cancelled().await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                let still_cancelling = db
                    .find_run_by_id(&run_id)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|run| run.status == "cancelling");
                observed_pre_reconcile.store(still_cancelling, Ordering::SeqCst);
                ToolOutput::ok("cleanup boundary observed")
            })
        }
    }

    fn id() -> String {
        Uuid::new_v4().to_string()
    }

    async fn fixture_with_sink(
        sink: Arc<dyn MessageSink>,
    ) -> (TaskRuntime, String, RuntimeTaskRecord, String) {
        let db = zk_db::Db::open_in_memory().expect("database");
        let session = db
            .create_session("test-model", "/tmp/task-runtime")
            .await
            .expect("root session");
        let root_task_id = id();
        let root_run_id = id();
        let root = db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: root_task_id,
                run_id: root_run_id.clone(),
                root_session_id: session.id.clone(),
                transcript_session_id: session.id.clone(),
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: "root".to_owned(),
                prompt: Some("root prompt".to_owned()),
                task_type: "agent".to_owned(),
                model: "test-model".to_owned(),
                working_dir: "/tmp/task-runtime".to_owned(),
                execution_config_json: serde_json::json!({
                    "budget": {
                        "tokenLimit": 1_000_000,
                        "costLimitNanosUsd": 1_000_000_000_000_i64,
                        "deadlineAtMs": zk_db::time::now_millis() + 60_000,
                    }
                })
                .to_string(),
                startup_epoch: 1,
            })
            .await
            .expect("root task");
        assert_eq!(
            db.claim_task_run_cas(&root.task.id, &root_run_id, root.task.version)
                .await
                .expect("claim root task/run"),
            CasOutcome::Applied
        );
        let root = db
            .find_runtime_task_by_id(&root.task.id)
            .await
            .expect("root query")
            .expect("root");
        (TaskRuntime::new(db, sink), session.id, root, root_run_id)
    }

    async fn fixture() -> (TaskRuntime, String, RuntimeTaskRecord, String) {
        fixture_with_sink(Arc::new(NoopSink)).await
    }

    fn submission(session: &str, root: &RuntimeTaskRecord, run_id: &str) -> ChildTaskSubmission {
        ChildTaskSubmission::attached(
            session,
            &root.id,
            run_id,
            "tool-1",
            "child",
            "do work",
            "test-model",
            "/tmp/task-runtime",
        )
    }

    fn request_ten_percent_child_budget(request: &mut ChildTaskSubmission) {
        request.execution_config_json = serde_json::json!({
            "isolation": "readOnly",
            "lifecycle": "attached",
            "budget": {
                "tokenLimit": 100_000,
                "costLimitNanosUsd": 100_000_000_000_i64,
            }
        })
        .to_string();
    }

    #[test]
    fn attached_submission_defaults_to_the_thirty_minute_ceiling() {
        let request = ChildTaskSubmission::attached(
            "root-session",
            "parent-task",
            "parent-run",
            "tool-use",
            "child",
            "do work",
            "test-model",
            "/tmp/task-runtime",
        );
        assert_eq!(request.timeout, Duration::from_mins(30));
    }

    #[tokio::test]
    async fn precreated_cron_uses_the_unified_driver_and_result_store() {
        let db = zk_db::Db::open_in_memory().expect("database");
        let session = db
            .create_session("test-model", "/tmp/task-runtime-cron")
            .await
            .expect("root session");
        let task_id = id();
        let run_id = id();
        let created = db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                root_session_id: session.id.clone(),
                transcript_session_id: session.id.clone(),
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: "scheduled job".to_owned(),
                prompt: Some("inspect repository".to_owned()),
                task_type: "cron".to_owned(),
                model: "test-model".to_owned(),
                working_dir: "/tmp/task-runtime-cron".to_owned(),
                execution_config_json: r#"{"source":"cron","isolation":"readOnly"}"#.to_owned(),
                startup_epoch: 1,
            })
            .await
            .expect("pre-create Cron identity");
        assert_eq!(created.task.status, DbTaskStatus::Queued);
        let runtime = TaskRuntime::new(db.clone(), Arc::new(NoopSink));
        let observed = Arc::new(AtomicBool::new(false));
        let observed_in_driver = Arc::clone(&observed);
        let db_in_driver = db.clone();
        assert!(
            runtime
                .dispatch_precreated_cron(&task_id, Duration::from_secs(5), move |context| {
                    let db = db_in_driver.clone();
                    async move {
                        let task = db
                            .find_runtime_task_by_id(&context.task_id)
                            .await
                            .expect("query from driver")
                            .expect("Task is durable before dispatch");
                        assert_eq!(task.task_type, "cron");
                        assert_eq!(context.run_id, run_id);
                        observed_in_driver.store(true, Ordering::SeqCst);
                        TaskExecutionResult::complete("scheduled result")
                    }
                })
                .await
                .expect("attach Cron task")
        );
        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session.id,
                task_id,
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 65_536,
            })
            .await
            .expect("read immutable Cron result");
        assert_eq!(output.task.status, DbTaskStatus::Succeeded);
        assert_eq!(
            output.result.expect("Cron result").content,
            "scheduled result"
        );
        assert!(observed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn swarm_adapter_rejects_legacy_task_type_before_persistence() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let mut request = submission(&session, &root, &root_run_id);
        request.task_type = "swarm:team:worker".to_owned();
        let worker_id = id();
        let error = runtime
            .register_external_task(&worker_id, request, CancellationToken::new())
            .await
            .expect_err("legacy Swarm type must fail closed");
        assert!(error.contains("UNSUPPORTED_CAPABILITY"), "{error}");
        assert!(
            runtime
                .db()
                .find_runtime_task_by_id(&worker_id)
                .await
                .expect("task lookup")
                .is_none()
        );
    }

    #[tokio::test]
    async fn submit_is_durable_before_executor_and_result_is_waitable() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let observed = Arc::new(AtomicBool::new(false));
        let observed_in_task = Arc::clone(&observed);
        let db = runtime.db().clone();
        let receipt = runtime
            .submit_child(submission(&session, &root, &root_run_id), move |context| {
                let db = db.clone();
                async move {
                    let durable = db
                        .find_runtime_task_by_id(&context.task_id)
                        .await
                        .expect("query from executor")
                        .expect("persisted before executor");
                    assert_eq!(
                        durable.current_run_id.as_deref(),
                        Some(context.run_id.as_str())
                    );
                    observed_in_task.store(true, Ordering::SeqCst);
                    TaskExecutionResult::complete("done")
                }
            })
            .await
            .expect("submit");
        assert!(Uuid::parse_str(&receipt.task.id).is_ok());
        assert!(Uuid::parse_str(&receipt.run_id).is_ok());
        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session.clone(),
                task_id: receipt.task.id.clone(),
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 65_536,
            })
            .await
            .expect("output");
        assert!(!output.wait_expired);
        assert_eq!(output.task.status, DbTaskStatus::Succeeded);
        assert_eq!(output.result.expect("result").content, "done");
        assert!(observed.load(Ordering::SeqCst));

        let parent = runtime
            .get_owned(&session, &root.id)
            .await
            .expect("parent query")
            .expect("parent");
        assert_eq!(parent.status, DbTaskStatus::Running);
    }

    #[tokio::test]
    async fn child_output_unblocks_before_parent_tool_result_then_resolver_ingests_in_order() {
        let (runtime, session, root, root_run_id) = fixture().await;
        runtime
            .db()
            .append_message(
                &session,
                NewMessage {
                    role: MessageRole::Assistant,
                    content: vec![StoredBlock::ToolUse {
                        id: "tool-1".to_owned(),
                        name: "Agent".to_owned(),
                        input: serde_json::json!({"prompt":"child"}),
                    }],
                    stop_reason: None,
                    input_tokens: 0,
                    output_tokens: 0,
                },
            )
            .await
            .expect("tool use message");
        let invocation_id = id();
        runtime
            .db()
            .create_tool_invocation(&NewToolInvocation {
                invocation_id: invocation_id.clone(),
                task_id: root.id.clone(),
                run_id: root_run_id.clone(),
                tool_use_id: "tool-1".to_owned(),
                tool_name: "Agent".to_owned(),
                input_json: None,
                side_effect_class: "none".to_owned(),
                directory_generation: None,
                connection_generation: None,
            })
            .await
            .expect("invocation");
        assert_eq!(
            runtime
                .db()
                .transition_tool_invocation_cas(
                    &invocation_id,
                    0,
                    ToolInvocationStatus::Running,
                    Some(r#"{"prompt":"child"}"#),
                    None,
                    None,
                    CleanupStatus::NotRequired,
                )
                .await
                .expect("invocation running"),
            CasOutcome::Applied
        );

        let receipt = runtime
            .submit_child(submission(&session, &root, &root_run_id), |_| async {
                TaskExecutionResult::complete("done")
            })
            .await
            .expect("submit");
        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session.clone(),
                task_id: receipt.task.id.clone(),
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("child output must not wait for its own tool_result");
        assert_eq!(output.task.status, DbTaskStatus::Succeeded);
        let waiting_parent = runtime
            .get_owned(&session, &root.id)
            .await
            .expect("parent query")
            .expect("parent");
        assert_eq!(waiting_parent.status, DbTaskStatus::WaitingDependencies);
        let parent_id = root.id.clone();
        let receipt_count: i64 = runtime
            .db()
            .with_conn_blocking(move |connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM task_result_receipts WHERE consumer_task_id=?1",
                        [parent_id],
                        |row| row.get(0),
                    )
                    .map_err(Into::into)
            })
            .expect("receipt count");
        assert_eq!(receipt_count, 0);

        runtime
            .db()
            .append_message(
                &session,
                NewMessage {
                    role: MessageRole::User,
                    content: vec![StoredBlock::ToolResult {
                        tool_use_id: "tool-1".to_owned(),
                        content: "Agent completed".to_owned(),
                        is_error: false,
                        metadata: None,
                    }],
                    stop_reason: None,
                    input_tokens: 0,
                    output_tokens: 0,
                },
            )
            .await
            .expect("tool result message");
        assert_eq!(
            runtime
                .db()
                .transition_tool_invocation_cas(
                    &invocation_id,
                    1,
                    ToolInvocationStatus::Succeeded,
                    Some(r#"{"prompt":"child"}"#),
                    Some("task-result"),
                    None,
                    CleanupStatus::Confirmed,
                )
                .await
                .expect("invocation completed"),
            CasOutcome::Applied
        );

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let parent = runtime
                .get_owned(&session, &root.id)
                .await
                .expect("parent query")
                .expect("parent");
            if parent.status == DbTaskStatus::Running {
                break;
            }
            assert!(Instant::now() < deadline, "parent resolver did not wake");
            sleep(Duration::from_millis(5)).await;
        }

        let session_id = session.clone();
        let parent_id = root.id.clone();
        let (tool_result_seq, task_result_seq, receipts) = runtime
            .db()
            .with_conn_blocking(move |connection| {
                let tool_result_seq = connection.query_row(
                    "SELECT message.seq_num FROM messages message,json_each(message.content_json) block
                     WHERE message.session_id=?1 AND json_extract(block.value,'$.type')='tool_result'",
                    [&session_id],
                    |row| row.get::<_, i64>(0),
                )?;
                let task_result_seq = connection.query_row(
                    "SELECT seq_num FROM messages WHERE task_id=?1 AND origin='task_result'",
                    [&parent_id],
                    |row| row.get::<_, i64>(0),
                )?;
                let receipts = connection.query_row(
                    "SELECT COUNT(*) FROM task_result_receipts WHERE consumer_task_id=?1",
                    [&parent_id],
                    |row| row.get::<_, i64>(0),
                )?;
                Ok((tool_result_seq, task_result_seq, receipts))
            })
            .expect("ordered messages");
        assert!(tool_result_seq < task_result_seq);
        assert_eq!(receipts, 1);
    }

    #[tokio::test]
    async fn idempotent_replay_does_not_spawn_second_executor() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let request = submission(&session, &root, &root_run_id);
        let first = runtime
            .submit_child(request.clone(), |_| async {
                sleep(Duration::from_millis(20)).await;
                TaskExecutionResult::complete("first")
            })
            .await
            .expect("first");
        let second_ran = Arc::new(AtomicBool::new(false));
        let second_ran_in_task = Arc::clone(&second_ran);
        let second = runtime
            .submit_child(request, move |_| async move {
                second_ran_in_task.store(true, Ordering::SeqCst);
                TaskExecutionResult::complete("second")
            })
            .await
            .expect("replay");
        assert!(!second.created);
        assert_eq!(second.task.id, first.task.id);
        let _ = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: first.task.id,
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("output");
        assert!(!second_ran.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn idempotent_replay_reattaches_a_committed_queued_run_after_dispatch_crash() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let request = submission(&session, &root, &root_run_id);
        let execution_config_json =
            durable_submission_config(&request).expect("durable execution config");
        let committed = runtime
            .db()
            .create_task_with_run(&CreateTaskWithRun {
                task_id: id(),
                run_id: id(),
                root_session_id: request.root_session_id.clone(),
                transcript_session_id: id(),
                parent_task_id: Some(request.parent_task_id.clone()),
                parent_run_id: Some(request.parent_run_id.clone()),
                creator_tool_use_id: Some(request.creator_tool_use_id.clone()),
                ordinal: request.ordinal,
                description: request.description.clone(),
                prompt: Some(request.prompt.clone()),
                task_type: request.task_type.clone(),
                model: request.model.clone(),
                working_dir: request.working_dir.clone(),
                execution_config_json,
                startup_epoch: request.startup_epoch,
            })
            .await
            .expect("commit child before simulated process crash");
        assert!(committed.created);
        assert_eq!(runtime.local_active_count(), 0);

        let executions = Arc::new(AtomicUsize::new(0));
        let executions_in_task = Arc::clone(&executions);
        let replay = runtime
            .submit_child(request, move |_| async move {
                executions_in_task.fetch_add(1, Ordering::SeqCst);
                TaskExecutionResult::complete("recovered queued dispatch")
            })
            .await
            .expect("queued replay attaches driver");
        assert!(!replay.created);
        assert_eq!(replay.task.id, committed.task.id);
        assert_eq!(replay.run_id, committed.run_id);

        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: replay.task.id,
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("reattached result");
        assert_eq!(output.task.status, DbTaskStatus::Succeeded);
        assert_eq!(
            output.result.expect("immutable result").content,
            "recovered queued dispatch"
        );
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_idempotent_submissions_install_exactly_one_driver() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let request = submission(&session, &root, &root_run_id);
        let contenders = 24;
        let start = Arc::new(tokio::sync::Barrier::new(contenders));
        let executions = Arc::new(AtomicUsize::new(0));
        let mut submissions = tokio::task::JoinSet::new();
        for _ in 0..contenders {
            let runtime = runtime.clone();
            let request = request.clone();
            let start = Arc::clone(&start);
            let executions = Arc::clone(&executions);
            submissions.spawn(async move {
                start.wait().await;
                runtime
                    .submit_child(request, move |_| async move {
                        executions.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(30)).await;
                        TaskExecutionResult::complete("one physical execution")
                    })
                    .await
            });
        }

        let mut receipts = Vec::new();
        while let Some(joined) = submissions.join_next().await {
            receipts.push(joined.expect("submission task").expect("submission"));
        }
        assert_eq!(receipts.iter().filter(|receipt| receipt.created).count(), 1);
        let task_id = receipts[0].task.id.clone();
        let run_id = receipts[0].run_id.clone();
        assert!(receipts.iter().all(|receipt| receipt.task.id == task_id));
        assert!(receipts.iter().all(|receipt| receipt.run_id == run_id));

        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id,
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("single result");
        assert_eq!(output.task.status, DbTaskStatus::Succeeded);
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn replay_of_an_already_claimed_durable_run_is_observation_only() {
        let (runtime, _session, root, root_run_id) = fixture().await;
        let request = submission(&root.session_id, &root, &root_run_id);
        let execution_config_json =
            durable_submission_config(&request).expect("durable execution config");
        let committed = runtime
            .db()
            .create_task_with_run(&CreateTaskWithRun {
                task_id: id(),
                run_id: id(),
                root_session_id: request.root_session_id.clone(),
                transcript_session_id: id(),
                parent_task_id: Some(request.parent_task_id.clone()),
                parent_run_id: Some(request.parent_run_id.clone()),
                creator_tool_use_id: Some(request.creator_tool_use_id.clone()),
                ordinal: request.ordinal,
                description: request.description.clone(),
                prompt: Some(request.prompt.clone()),
                task_type: request.task_type.clone(),
                model: request.model.clone(),
                working_dir: request.working_dir.clone(),
                execution_config_json,
                startup_epoch: request.startup_epoch,
            })
            .await
            .expect("committed child");
        assert_eq!(
            runtime
                .db()
                .claim_task_run_cas(
                    &committed.task.id,
                    &committed.run_id,
                    committed.task.version,
                )
                .await
                .expect("claim outside runtime"),
            CasOutcome::Applied
        );

        let executions = Arc::new(AtomicUsize::new(0));
        let executions_in_task = Arc::clone(&executions);
        let replay = runtime
            .submit_child(request, move |_| async move {
                executions_in_task.fetch_add(1, Ordering::SeqCst);
                TaskExecutionResult::complete("must not run")
            })
            .await
            .expect("claimed replay is readable");
        assert!(!replay.created);
        assert_eq!(replay.task.id, committed.task.id);
        assert_eq!(replay.run_id, committed.run_id);
        tokio::task::yield_now().await;
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.local_active_count(), 0);
    }

    #[tokio::test]
    async fn idempotency_key_reuse_with_different_request_fails_closed() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let request = submission(&session, &root, &root_run_id);
        let first = runtime
            .submit_child(request.clone(), |_| async {
                TaskExecutionResult::complete("original")
            })
            .await
            .expect("original submission");

        let mut mismatched = request;
        mismatched.timeout = Duration::from_mins(20);
        let error = runtime
            .submit_child(mismatched, |_| async {
                TaskExecutionResult::complete("must not execute")
            })
            .await
            .expect_err("mismatched idempotent retry must be rejected");
        assert_eq!(error.code, "TASK_IDEMPOTENCY_MISMATCH");
        assert!(!error.retryable);

        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: first.task.id,
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("original result");
        assert_eq!(output.task.status, DbTaskStatus::Succeeded);
        assert_eq!(
            output.result.expect("original immutable result").content,
            "original"
        );
    }

    #[tokio::test]
    async fn terminal_commit_reaper_retains_owner_past_legacy_retry_limit_then_recovers_once() {
        let sink = Arc::new(RecordingSink::default());
        let (runtime, session, root, root_run_id) = fixture_with_sink(sink.clone()).await;
        runtime
            .inner
            .terminal_commit_failpoint
            .mode
            .store(1, Ordering::SeqCst);

        let receipt = runtime
            .submit_child(submission(&session, &root, &root_run_id), |_| async {
                TaskExecutionResult::complete("durable after storage recovery")
            })
            .await
            .expect("submit child");

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let attempts = runtime
                .inner
                .terminal_commit_failpoint
                .attempts
                .load(Ordering::SeqCst);
            if attempts > 12 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "terminal reaper did not retry beyond the removed twelve-attempt cutoff"
            );
            sleep(Duration::from_millis(5)).await;
        }

        let while_storage_is_down = runtime
            .get_owned(&session, &receipt.task.id)
            .await
            .expect("task query")
            .expect("child task");
        assert_eq!(while_storage_is_down.status, DbTaskStatus::Running);
        assert_eq!(runtime.local_active_count(), 1);
        assert!(
            runtime
                .db()
                .read_task_result(&receipt.task.id, None, 0, 1024)
                .await
                .expect("result query")
                .is_none()
        );
        assert_eq!(sink.pushes.load(Ordering::SeqCst), 0);

        let task_id = receipt.task.id.clone();
        let run_id = receipt.run_id.clone();
        let assistant_messages: i64 = runtime
            .db()
            .with_conn_blocking(move |connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM messages
                         WHERE task_id=?1 AND run_id=?2 AND role='assistant'",
                        (&task_id, &run_id),
                        |row| row.get(0),
                    )
                    .map_err(Into::into)
            })
            .expect("assistant message count");
        assert_eq!(assistant_messages, 0);

        runtime
            .inner
            .terminal_commit_failpoint
            .mode
            .store(0, Ordering::SeqCst);
        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: receipt.task.id.clone(),
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("result after recovery");
        assert_eq!(output.task.status, DbTaskStatus::Succeeded);
        assert_eq!(
            output.result.expect("immutable result").content,
            "durable after storage recovery"
        );

        let deadline = Instant::now() + Duration::from_secs(2);
        while runtime.local_active_count() != 0 {
            assert!(
                Instant::now() < deadline,
                "owner was not released after commit"
            );
            sleep(Duration::from_millis(5)).await;
        }
        let task_id = receipt.task.id;
        let run_id = receipt.run_id;
        let (results, assistant_messages): (i64, i64) = runtime
            .db()
            .with_conn_blocking(move |connection| {
                let results = connection.query_row(
                    "SELECT COUNT(*) FROM task_results WHERE task_id=?1 AND run_id=?2",
                    (&task_id, &run_id),
                    |row| row.get(0),
                )?;
                let messages = connection.query_row(
                    "SELECT COUNT(*) FROM messages
                     WHERE task_id=?1 AND run_id=?2 AND role='assistant'",
                    (&task_id, &run_id),
                    |row| row.get(0),
                )?;
                Ok((results, messages))
            })
            .expect("terminal row counts");
        assert_eq!(results, 1);
        assert_eq!(assistant_messages, 1);
        assert_eq!(sink.pushes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn permanent_terminal_commit_failure_becomes_durable_needs_attention_not_success() {
        let sink = Arc::new(RecordingSink::default());
        let (runtime, session, root, root_run_id) = fixture_with_sink(sink.clone()).await;
        runtime
            .inner
            .terminal_commit_failpoint
            .mode
            .store(2, Ordering::SeqCst);

        let receipt = runtime
            .submit_child(submission(&session, &root, &root_run_id), |_| async {
                TaskExecutionResult::complete("must not be presented as delivered")
            })
            .await
            .expect("submit child");
        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: receipt.task.id.clone(),
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("needs-attention projection");
        assert_eq!(output.task.status, DbTaskStatus::NeedsAttention);
        assert!(
            output
                .task
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("TERMINAL_RESULT_COMMIT_FAILED"))
        );
        assert!(output.result.is_none());

        let deadline = Instant::now() + Duration::from_secs(2);
        while runtime.local_active_count() != 0 {
            assert!(
                Instant::now() < deadline,
                "owner was not released after durable diagnosis"
            );
            sleep(Duration::from_millis(5)).await;
        }
        let run = runtime
            .db()
            .find_run_by_id(&receipt.run_id)
            .await
            .expect("run query")
            .expect("run");
        assert_eq!(run.status, "interrupted");
        assert_eq!(run.exit_reason.as_deref(), Some("internalError"));
        assert_eq!(sink.pushes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancellation_waits_for_cooperative_cleanup_before_cancelled_result() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let receipt = runtime
            .submit_child(
                submission(&session, &root, &root_run_id),
                move |context| async move {
                    context.cancel.cancelled().await;
                    sleep(Duration::from_millis(10)).await;
                    TaskExecutionResult::Cancelled {
                        message: "clean".to_owned(),
                    }
                },
            )
            .await
            .expect("submit");
        let stop = runtime
            .cancel_owned(&session, &receipt.task.id, "userCancelled")
            .await
            .expect("cancel");
        assert!(stop.cancel_requested);
        assert_eq!(stop.task.status, DbTaskStatus::Cancelling);
        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: receipt.task.id,
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("cancel result");
        assert_eq!(output.task.status, DbTaskStatus::Cancelled);
        assert!(matches!(
            output.task.cleanup_status,
            CleanupStatus::NotRequired | CleanupStatus::Confirmed
        ));
        let run = runtime
            .db()
            .find_run_by_id(&receipt.run_id)
            .await
            .expect("run lookup")
            .expect("cancelled run");
        assert_eq!(
            run.requested_exit_reason.as_deref(),
            Some(zk_db::run::EXIT_USER_CANCELLED)
        );
        assert_eq!(
            run.exit_reason.as_deref(),
            Some(zk_db::run::EXIT_USER_CANCELLED)
        );
    }

    #[tokio::test]
    async fn needs_attention_never_signals_a_stale_execution_token() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let token_observed = Arc::new(AtomicBool::new(false));
        let token_observed_in_task = Arc::clone(&token_observed);
        let receipt = runtime
            .submit_child(
                submission(&session, &root, &root_run_id),
                move |context| async move {
                    let _ = started_tx.send(());
                    tokio::select! {
                        () = context.cancel.cancelled() => {
                            token_observed_in_task.store(true, Ordering::SeqCst);
                            TaskExecutionResult::Cancelled {
                                message: "unexpected cancellation signal".to_owned(),
                            }
                        }
                        _ = release_rx => TaskExecutionResult::complete("released"),
                    }
                },
            )
            .await
            .expect("submit child");
        started_rx.await.expect("child execution started");

        let running = runtime
            .get_owned(&session, &receipt.task.id)
            .await
            .expect("running task query")
            .expect("running task");
        assert_eq!(running.status, DbTaskStatus::Running);
        assert_eq!(
            runtime
                .db()
                .mark_task_run_needs_attention(
                    &running.id,
                    &receipt.run_id,
                    running.version,
                    "durability invariant failed",
                    CleanupStatus::Unconfirmed,
                )
                .await
                .expect("quarantine task"),
            zk_db::MarkTaskNeedsAttentionOutcome::Marked
        );

        let stop = runtime
            .cancel_owned(&session, &receipt.task.id, "late stop")
            .await
            .expect("read quarantined cancellation state");
        assert!(!stop.cancel_requested);
        assert_eq!(stop.task.status, DbTaskStatus::NeedsAttention);
        sleep(Duration::from_millis(25)).await;
        assert!(
            !token_observed.load(Ordering::SeqCst),
            "needsAttention is not a durable cancellation boundary"
        );

        let _ = release_tx.send(());
        tokio::time::timeout(Duration::from_secs(1), async {
            while runtime.inner.active.contains_key(&receipt.task.id) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("execution owner released");
        assert!(!token_observed.load(Ordering::SeqCst));
        assert!(
            runtime
                .db()
                .read_task_result(&receipt.task.id, None, 0, 1024)
                .await
                .expect("result query")
                .is_none(),
            "quarantine must not manufacture a TaskResult"
        );
    }

    #[tokio::test]
    async fn deadline_retries_persistence_before_signalling_execution() {
        let (runtime, session, root, root_run_id) = fixture().await;
        runtime
            .inner
            .cancellation_persist_failpoint
            .remaining_failures
            .store(2, Ordering::SeqCst);
        let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
        let db = runtime.db().clone();
        let mut request = submission(&session, &root, &root_run_id);
        request.timeout = Duration::from_millis(50);
        let receipt = runtime
            .submit_child(request, move |context| async move {
                context.cancel.cancelled().await;
                let task = db
                    .find_runtime_task_by_id(&context.task_id)
                    .await
                    .expect("task query at cancellation boundary")
                    .expect("task at cancellation boundary");
                let run = db
                    .find_run_by_id(&context.run_id)
                    .await
                    .expect("run query at cancellation boundary")
                    .expect("run at cancellation boundary");
                let _ = observed_tx.send((task, run));
                TaskExecutionResult::Cancelled {
                    message: "deadline token observed".to_owned(),
                }
            })
            .await
            .expect("submit deadline child");

        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: receipt.task.id,
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("deadline result");
        let (task_at_signal, run_at_signal) = observed_rx.await.expect("token observation");
        assert_eq!(task_at_signal.status, DbTaskStatus::Cancelling);
        assert_eq!(task_at_signal.cleanup_status, CleanupStatus::Pending);
        assert_eq!(run_at_signal.status, "cancelling");
        assert_eq!(
            run_at_signal.requested_exit_reason.as_deref(),
            Some(zk_db::run::EXIT_TIMEOUT)
        );
        assert!(
            runtime
                .inner
                .cancellation_persist_failpoint
                .attempts
                .load(Ordering::SeqCst)
                >= 3
        );
        assert_eq!(output.task.status, DbTaskStatus::Failed);
        assert_eq!(
            output.result.expect("timeout result").result.status,
            ResultStatus::Error
        );
    }

    #[tokio::test]
    async fn root_cancellation_preserves_parent_cancelled_on_attached_child() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let child = runtime
            .submit_child(
                submission(&session, &root, &root_run_id),
                |context| async move {
                    context.cancel.cancelled().await;
                    TaskExecutionResult::Cancelled {
                        message: "cancelled with parent".to_owned(),
                    }
                },
            )
            .await
            .expect("submit child");

        let stop = runtime
            .cancel_owned(&session, &root.id, "user cancelled the root Task")
            .await
            .expect("cancel root");
        assert!(stop.cancel_requested);
        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: child.task.id,
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("child cancellation result");
        assert_eq!(output.task.status, DbTaskStatus::Cancelled);
        let child_run = runtime
            .db()
            .find_run_by_id(&child.run_id)
            .await
            .expect("run lookup")
            .expect("child run");
        assert_eq!(
            child_run.requested_exit_reason.as_deref(),
            Some(zk_db::run::EXIT_PARENT_CANCELLED)
        );
        assert_eq!(
            child_run.exit_reason.as_deref(),
            Some(zk_db::run::EXIT_PARENT_CANCELLED)
        );
    }

    #[tokio::test]
    async fn parent_tool_cancellation_is_not_misreported_as_user_cancelled() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let child = runtime
            .submit_child(
                submission(&session, &root, &root_run_id),
                |context| async move {
                    context.cancel.cancelled().await;
                    TaskExecutionResult::Cancelled {
                        message: "parent stopped".to_owned(),
                    }
                },
            )
            .await
            .expect("submit child");

        runtime
            .cancel_attached_from_parent(&session, &child.task.id, "parentToolCancelled")
            .await
            .expect("cancel from parent");
        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: child.task.id,
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("cancel result");
        assert_eq!(output.task.status, DbTaskStatus::Cancelled);
        let run = runtime
            .db()
            .find_run_by_id(&child.run_id)
            .await
            .expect("run lookup")
            .expect("child run");
        assert_eq!(
            run.requested_exit_reason.as_deref(),
            Some(zk_db::run::EXIT_PARENT_CANCELLED)
        );
        assert_eq!(
            run.exit_reason.as_deref(),
            Some(zk_db::run::EXIT_PARENT_CANCELLED)
        );
    }

    #[tokio::test]
    async fn background_child_observes_parent_terminal_state_after_tool_future_returns() {
        let (runtime, session, root, root_run_id) = fixture().await;
        runtime
            .inner
            .cancellation_persist_failpoint
            .remaining_failures
            .store(2, Ordering::SeqCst);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
        let db = runtime.db().clone();
        let child = runtime
            .submit_child(
                submission(&session, &root, &root_run_id),
                move |context| async move {
                    let _ = started_tx.send(());
                    context.cancel.cancelled().await;
                    let task = db
                        .find_runtime_task_by_id(&context.task_id)
                        .await
                        .expect("task query at parent-stop boundary")
                        .expect("task at parent-stop boundary");
                    let run = db
                        .find_run_by_id(&context.run_id)
                        .await
                        .expect("run query at parent-stop boundary")
                        .expect("run at parent-stop boundary");
                    let _ = observed_tx.send((task, run));
                    TaskExecutionResult::Cancelled {
                        message: "durable parent cancellation observed".to_owned(),
                    }
                },
            )
            .await
            .expect("submit background child");
        started_rx.await.expect("child execution started");

        let parent = runtime
            .get_owned(&session, &root.id)
            .await
            .expect("parent query")
            .expect("parent");
        assert_eq!(parent.status, DbTaskStatus::WaitingDependencies);
        let outcome = runtime
            .db()
            .commit_task_result(&CommitTaskResult {
                task_id: parent.id,
                run_id: root_run_id,
                expected_task_version: parent.version,
                status: ResultStatus::Cancelled,
                content: "root cancelled outside the Agent tool future".to_owned(),
                media_type: "text/markdown".to_owned(),
                error_code: Some("USER_CANCELLED".to_owned()),
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect("terminalize parent");
        assert!(matches!(outcome, CommitTaskResultOutcome::Committed { .. }));

        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: child.task.id,
                wait_ms: 5_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("child terminal result");
        assert_eq!(output.task.status, DbTaskStatus::Cancelled);
        let (task_at_signal, run_at_signal) = observed_rx.await.expect("token observation");
        assert_eq!(task_at_signal.status, DbTaskStatus::Cancelling);
        assert_eq!(task_at_signal.cleanup_status, CleanupStatus::Pending);
        assert_eq!(run_at_signal.status, "cancelling");
        assert_eq!(
            run_at_signal.requested_exit_reason.as_deref(),
            Some(zk_db::run::EXIT_PARENT_CANCELLED)
        );
        assert!(
            runtime
                .inner
                .cancellation_persist_failpoint
                .attempts
                .load(Ordering::SeqCst)
                >= 3
        );
        let run = runtime
            .db()
            .find_run_by_id(&child.run_id)
            .await
            .expect("run lookup")
            .expect("child run");
        assert_eq!(
            run.exit_reason.as_deref(),
            Some(zk_db::run::EXIT_PARENT_CANCELLED)
        );
    }

    #[tokio::test]
    async fn root_admission_never_executes_more_than_four_children() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let running = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let mut ids = Vec::new();
        for ordinal in 0..8 {
            let mut request = submission(&session, &root, &root_run_id);
            request.ordinal = ordinal;
            request.creator_tool_use_id = format!("tool-{ordinal}");
            // Eight queued identities must fit inside the root's 80% child pool;
            // the independent execution semaphore still proves only four run at once.
            request_ten_percent_child_budget(&mut request);
            let running_in_task = Arc::clone(&running);
            let maximum_in_task = Arc::clone(&maximum);
            let receipt = runtime
                .submit_child(request, move |_| async move {
                    let now = running_in_task.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum_in_task.fetch_max(now, Ordering::SeqCst);
                    sleep(Duration::from_millis(40)).await;
                    running_in_task.fetch_sub(1, Ordering::SeqCst);
                    TaskExecutionResult::complete("ok")
                })
                .await
                .expect("submit");
            ids.push(receipt.task.id);
        }
        for task_id in ids {
            let _ = runtime
                .read_output(TaskOutputRequest {
                    root_session_id: session.clone(),
                    task_id,
                    wait_ms: 5_000,
                    result_version: None,
                    cursor: 0,
                    max_bytes: 1024,
                })
                .await
                .expect("output");
        }
        assert!(maximum.load(Ordering::SeqCst) <= ROOT_AGENT_LIMIT);
    }

    /// Regression fixture distilled from the September multi-agent incident: four
    /// callers wait for a result while four keep running in the background.  Every
    /// returned identifier must already be durable, and stopping the background
    /// half must leave a readable immutable result instead of `TASK_NOT_FOUND` or
    /// a process-local ghost.
    #[tokio::test]
    async fn four_waited_and_four_background_children_remain_queryable_and_terminal() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let mut waited = Vec::new();
        let mut background = Vec::new();

        for ordinal in 0..8 {
            let mut request = submission(&session, &root, &root_run_id);
            request.ordinal = ordinal;
            request.creator_tool_use_id = format!("incident-tool-{ordinal}");
            let receipt = if ordinal < 4 {
                runtime
                    .submit_child(request, move |_| async move {
                        TaskExecutionResult::complete(format!("foreground-{ordinal}"))
                    })
                    .await
                    .expect("submit waited child")
            } else {
                runtime
                    .submit_child(request, move |context| async move {
                        context.cancel.cancelled().await;
                        TaskExecutionResult::Cancelled {
                            message: format!("background-{ordinal}-cancelled"),
                        }
                    })
                    .await
                    .expect("submit background child")
            };

            let durable = runtime
                .get_owned(&session, &receipt.task.id)
                .await
                .expect("query just-returned task")
                .expect("task must be durable before submit returns");
            assert_eq!(durable.id, receipt.task.id);
            if ordinal < 4 {
                waited.push(receipt.task.id);
            } else {
                background.push(receipt.task.id);
            }
        }

        for (ordinal, task_id) in waited.into_iter().enumerate() {
            let output = runtime
                .read_output(TaskOutputRequest {
                    root_session_id: session.clone(),
                    task_id,
                    wait_ms: 5_000,
                    result_version: None,
                    cursor: 0,
                    max_bytes: 1024,
                })
                .await
                .expect("waited child output");
            assert_eq!(output.task.status, DbTaskStatus::Succeeded);
            assert_eq!(
                output.result.expect("immutable success result").content,
                format!("foreground-{ordinal}")
            );
        }

        for task_id in &background {
            let stop = runtime
                .cancel_owned(&session, task_id, zk_db::run::EXIT_USER_CANCELLED)
                .await
                .expect("stop background child");
            assert!(stop.cancel_requested || stop.task.status.is_terminal());
        }
        for task_id in background {
            let output = runtime
                .read_output(TaskOutputRequest {
                    root_session_id: session.clone(),
                    task_id,
                    wait_ms: 5_000,
                    result_version: None,
                    cursor: 0,
                    max_bytes: 1024,
                })
                .await
                .expect("cancelled background output");
            assert_eq!(output.task.status, DbTaskStatus::Cancelled);
            assert_eq!(
                output
                    .result
                    .expect("immutable cancellation result")
                    .result
                    .status,
                ResultStatus::Cancelled
            );
        }
    }

    #[tokio::test]
    async fn hard_deadline_includes_time_waiting_for_an_execution_slot() {
        let (runtime, session, root, root_run_id) = fixture().await;
        assert_eq!(
            runtime
                .configure_root_budget(
                    &session,
                    &root.id,
                    0,
                    &TaskBudgetLimits {
                        token_limit: Some(1_000_000),
                        cost_limit_nanos_usd: Some(1_000_000_000_000),
                        deadline_at_ms: Some(zk_db::time::now_millis() + 500),
                    },
                )
                .await
                .expect("configure deadline"),
            CasOutcome::Applied
        );

        let started = Arc::new(AtomicUsize::new(0));
        let mut running_ids = Vec::new();
        for ordinal in 0..ROOT_AGENT_LIMIT {
            let mut request = submission(&session, &root, &root_run_id);
            request.ordinal = i64::try_from(ordinal).expect("small ordinal");
            request.creator_tool_use_id = format!("deadline-holder-{ordinal}");
            request_ten_percent_child_budget(&mut request);
            let started_in_task = Arc::clone(&started);
            let receipt = runtime
                .submit_child(request, move |context| async move {
                    started_in_task.fetch_add(1, Ordering::SeqCst);
                    context.cancel.cancelled().await;
                    TaskExecutionResult::Cancelled {
                        message: "deadline observed".to_owned(),
                    }
                })
                .await
                .expect("submit holder");
            running_ids.push(receipt.task.id);
        }
        let wait_until = Instant::now() + Duration::from_millis(250);
        while started.load(Ordering::SeqCst) != ROOT_AGENT_LIMIT && Instant::now() < wait_until {
            sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(started.load(Ordering::SeqCst), ROOT_AGENT_LIMIT);

        let fifth_ran = Arc::new(AtomicBool::new(false));
        let fifth_ran_in_task = Arc::clone(&fifth_ran);
        let mut queued = submission(&session, &root, &root_run_id);
        queued.ordinal = i64::try_from(ROOT_AGENT_LIMIT).expect("small ordinal");
        queued.creator_tool_use_id = "deadline-queued".to_owned();
        request_ten_percent_child_budget(&mut queued);
        let queued = runtime
            .submit_child(queued, move |_| async move {
                fifth_ran_in_task.store(true, Ordering::SeqCst);
                TaskExecutionResult::complete("must not execute")
            })
            .await
            .expect("submit queued child");
        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session.clone(),
                task_id: queued.task.id,
                wait_ms: 2_000,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect("deadline result");
        assert_eq!(output.task.status, DbTaskStatus::Failed);
        assert_eq!(output.task.reason.as_deref(), Some("timeout"));
        assert!(!fifth_ran.load(Ordering::SeqCst));

        for task_id in running_ids {
            let _ = runtime
                .read_output(TaskOutputRequest {
                    root_session_id: session.clone(),
                    task_id,
                    wait_ms: 2_000,
                    result_version: None,
                    cursor: 0,
                    max_bytes: 1024,
                })
                .await
                .expect("holder deadline result");
        }
    }

    #[tokio::test]
    async fn ownership_and_output_wait_validation_fail_closed() {
        let (runtime, session, root, root_run_id) = fixture().await;
        assert!(
            runtime
                .resolve_parent_task(&session, &root_run_id)
                .await
                .is_ok()
        );
        let denied = runtime
            .get_owned("another-session", &root.id)
            .await
            .expect_err("cross-session task lookup is denied");
        assert_eq!(denied.code, "TASK_ACCESS_DENIED");
        assert!(!denied.retryable);
        assert!(
            runtime
                .get_owned(&session, "00000000-0000-4000-8000-000000000000")
                .await
                .expect("missing lookup is not an authorization failure")
                .is_none()
        );
        let denied_output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: "another-session".to_owned(),
                task_id: root.id.clone(),
                wait_ms: 0,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect_err("cross-session result read is denied");
        assert_eq!(denied_output.code, "TASK_ACCESS_DENIED");
        let denied_stop = runtime
            .cancel_owned("another-session", &root.id, "not-owned")
            .await
            .expect_err("cross-session cancellation is denied");
        assert_eq!(denied_stop.code, "TASK_ACCESS_DENIED");
        let denied_message = runtime
            .send_message("another-session", &root.id, None, "hello")
            .await
            .expect_err("cross-session message is denied");
        assert_eq!(denied_message.code, "TASK_ACCESS_DENIED");
        let receipt = runtime
            .submit_child(submission(&session, &root, &root_run_id), |_| async {
                TaskExecutionResult::complete("done")
            })
            .await
            .expect("submit");
        let error = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: receipt.task.id,
                wait_ms: 30_001,
                result_version: None,
                cursor: 0,
                max_bytes: 1024,
            })
            .await
            .expect_err("invalid wait");
        assert_eq!(error.code, "TASK_WAIT_INVALID");
    }

    #[tokio::test]
    async fn shutdown_closes_execution_intake_before_durable_submission() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let report = runtime
            .shutdown(Duration::from_millis(100))
            .await
            .expect("shutdown");
        assert!(report.intake_closed);
        assert!(!runtime.accepts_new_execution());

        let error = runtime
            .submit_child(submission(&session, &root, &root_run_id), |_| async {
                TaskExecutionResult::complete("must not run")
            })
            .await
            .expect_err("closed runtime must reject new durable execution");
        assert_eq!(error.code, "RUNTIME_SHUTTING_DOWN");
        assert_eq!(
            runtime
                .list_owned(&session, None)
                .await
                .expect("task tree")
                .len(),
            1,
            "rejected submission must not create a Task/Run"
        );
    }

    #[tokio::test]
    async fn shutdown_persists_service_restart_before_signalling_executor() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let started = Arc::new(Notify::new());
        let observed_durable_boundary = Arc::new(AtomicBool::new(false));
        let db = runtime.db().clone();
        let started_in_task = Arc::clone(&started);
        let observed_in_task = Arc::clone(&observed_durable_boundary);
        let receipt = runtime
            .submit_child(
                submission(&session, &root, &root_run_id),
                move |context| async move {
                    started_in_task.notify_one();
                    context.cancel.cancelled().await;
                    let task = db
                        .find_runtime_task_by_id(&context.task_id)
                        .await
                        .expect("task query at cancellation boundary")
                        .expect("task");
                    let run = db
                        .find_run_by_id(&context.run_id)
                        .await
                        .expect("run query at cancellation boundary")
                        .expect("run");
                    observed_in_task.store(
                        task.status == DbTaskStatus::Cancelling
                            && run.status == "cancelling"
                            && run.requested_exit_reason.as_deref() == Some("serviceRestart"),
                        Ordering::SeqCst,
                    );
                    TaskExecutionResult::Cancelled {
                        message: "service stopping".to_owned(),
                    }
                },
            )
            .await
            .expect("child");
        started.notified().await;

        let report = runtime
            .shutdown(Duration::from_secs(1))
            .await
            .expect("shutdown");
        assert!(report.drained);
        assert!(observed_durable_boundary.load(Ordering::SeqCst));
        let task = runtime
            .get_owned(&session, &receipt.task.id)
            .await
            .expect("task query")
            .expect("task");
        let run = runtime
            .db()
            .find_run_by_id(&receipt.run_id)
            .await
            .expect("run query")
            .expect("run");
        assert_eq!(task.status, DbTaskStatus::NeedsAttention);
        assert_eq!(run.status, "interrupted");
        assert_eq!(run.exit_reason.as_deref(), Some("serviceRestart"));
        assert!(
            runtime
                .db()
                .read_task_result(&task.id, None, 0, 1024)
                .await
                .expect("result lookup")
                .is_none(),
            "shutdown must not manufacture a user-cancelled result"
        );
    }

    #[tokio::test]
    async fn shutdown_normally_drains_cancellation_aware_driver() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let started = Arc::new(Notify::new());
        let started_in_task = Arc::clone(&started);
        let receipt = runtime
            .submit_child(
                submission(&session, &root, &root_run_id),
                move |context| async move {
                    started_in_task.notify_one();
                    context.cancel.cancelled().await;
                    TaskExecutionResult::Cancelled {
                        message: "drained".to_owned(),
                    }
                },
            )
            .await
            .expect("child");
        started.notified().await;

        let report = runtime
            .shutdown(Duration::from_secs(1))
            .await
            .expect("shutdown");
        assert!(report.drained);
        assert_eq!(report.local_owners_timed_out, 0);
        assert_eq!(runtime.local_active_count(), 0);
        let run = runtime
            .db()
            .find_run_by_id(&receipt.run_id)
            .await
            .expect("run query")
            .expect("run");
        assert_eq!(run.status, "interrupted");
        assert_ne!(run.cleanup_status, "unconfirmed");
    }

    #[tokio::test]
    async fn shutdown_intent_failure_still_cancels_and_drains_local_owners() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let started = Arc::new(Notify::new());
        let cancellation_observed = Arc::new(AtomicBool::new(false));
        let started_in_task = Arc::clone(&started);
        let observed_in_task = Arc::clone(&cancellation_observed);
        let receipt = runtime
            .submit_child(
                submission(&session, &root, &root_run_id),
                move |context| async move {
                    started_in_task.notify_one();
                    context.cancel.cancelled().await;
                    observed_in_task.store(true, Ordering::SeqCst);
                    TaskExecutionResult::Cancelled {
                        message: "shutdown signal observed".to_owned(),
                    }
                },
            )
            .await
            .expect("child");
        started.notified().await;
        runtime
            .inner
            .shutdown_intent_failpoint
            .remaining_failures
            .store(1, Ordering::SeqCst);

        let error = runtime
            .shutdown(Duration::from_secs(1))
            .await
            .expect_err("the ordered intent failure remains visible to the caller");
        assert_eq!(error.code, "SHUTDOWN_INTENT_PERSISTENCE_INJECTED");
        assert!(cancellation_observed.load(Ordering::SeqCst));
        assert_eq!(runtime.local_active_count(), 0);

        let task = runtime
            .get_owned(&session, &receipt.task.id)
            .await
            .expect("task query")
            .expect("task");
        let run = runtime
            .db()
            .find_run_by_id(&receipt.run_id)
            .await
            .expect("run query")
            .expect("run");
        assert_eq!(task.status, DbTaskStatus::NeedsAttention);
        assert_eq!(run.status, "interrupted");
        assert_eq!(run.exit_reason.as_deref(), Some("serviceRestart"));
        assert!(
            runtime
                .db()
                .read_task_result(&task.id, None, 0, 1024)
                .await
                .expect("result lookup")
                .is_none(),
            "failed shutdown intent must not manufacture a cancellation result"
        );
    }

    #[tokio::test]
    async fn unified_shutdown_drains_leaf_owner_before_reconciliation() {
        let (runtime, _session, _root, root_run_id) = fixture().await;
        let supervisor = ExecutionSupervisor::new(runtime.db().clone());
        let entered = Arc::new(Notify::new());
        let observed_pre_reconcile = Arc::new(AtomicBool::new(false));
        let parent_cancel = CancellationToken::new();
        let _events = supervisor.executor().spawn_call(
            Arc::new(ShutdownOrderProbeTool {
                db: runtime.db().clone(),
                run_id: root_run_id.clone(),
                entered: Arc::clone(&entered),
                observed_pre_reconcile: Arc::clone(&observed_pre_reconcile),
            }),
            "shutdown-order-probe".to_owned(),
            serde_json::json!({}),
            &parent_cancel,
        );
        entered.notified().await;

        let report = runtime
            .shutdown_with_supervisor(&supervisor, Duration::from_secs(1))
            .await
            .expect("unified shutdown");
        assert!(report.drained);
        assert_eq!(report.leaf_owners_requested, 1);
        assert_eq!(report.leaf_owners_remaining, 0);
        assert!(
            observed_pre_reconcile.load(Ordering::SeqCst),
            "leaf cleanup must observe cancelling before reconciliation"
        );
        let run = runtime
            .db()
            .find_run_by_id(&root_run_id)
            .await
            .expect("run lookup")
            .expect("root run");
        assert_eq!(run.status, "interrupted");
    }

    #[tokio::test]
    async fn shutdown_timeout_never_claims_stuck_future_cleanup_is_confirmed() {
        let (runtime, session, root, root_run_id) = fixture().await;
        let started = Arc::new(Notify::new());
        let started_in_task = Arc::clone(&started);
        let receipt = runtime
            .submit_child(
                submission(&session, &root, &root_run_id),
                move |_| async move {
                    started_in_task.notify_one();
                    std::future::pending::<TaskExecutionResult>().await
                },
            )
            .await
            .expect("child");
        started.notified().await;

        let report = runtime
            .shutdown(Duration::from_millis(50))
            .await
            .expect("shutdown");
        assert!(!report.drained);
        assert!(report.local_owners_timed_out >= 1);
        assert!(report.cleanup_unconfirmed >= 1);
        let task = runtime
            .get_owned(&session, &receipt.task.id)
            .await
            .expect("task query")
            .expect("task");
        let run = runtime
            .db()
            .find_run_by_id(&receipt.run_id)
            .await
            .expect("run query")
            .expect("run");
        assert_eq!(task.status, DbTaskStatus::NeedsAttention);
        assert_eq!(task.cleanup_status, CleanupStatus::Unconfirmed);
        assert_eq!(run.status, "interrupted");
        assert_eq!(run.exit_reason.as_deref(), Some("serviceRestart"));
        assert_eq!(run.cleanup_status, "unconfirmed");
        assert!(
            runtime
                .db()
                .read_task_result(&task.id, None, 0, 1024)
                .await
                .expect("result lookup")
                .is_none()
        );
    }
}
