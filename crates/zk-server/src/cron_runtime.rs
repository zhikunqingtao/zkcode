//! Persistent Cron composition and scheduler.
//!
//! The scheduler owns no execution state. It claims `SQLite` occurrences, then
//! hands the already-created root Task/Run/Session to [`zk_engine::TaskRuntime`].
//! Occurrence status is only a projection of that authoritative runtime.
#![allow(clippy::missing_errors_doc, clippy::too_many_lines, missing_docs)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::task::JoinHandle;
use zk_db::{
    ClaimCronOccurrence, CronClaimOutcome, CronJobRecord, CronOccurrenceRecord, Db, NewCronJob,
};
use zk_engine::agent::{
    ChildExecutionContext, PersistedChildExecution, READ_ONLY_CHILD_TOOLS, SubAgentExecutor,
};
use zk_engine::{AgentRequest, AgentStatus, IsolationMode, TaskExecutionResult, TaskRuntime};
use zk_tools::cron::{format_timestamp_ms, next_run_after_ms};
use zk_tools::{CronCreateRequest, CronDeleteReceipt, CronPortError, CronTask, CronTaskPort};

const SCAN_INTERVAL: Duration = Duration::from_secs(15);
const SCAN_BATCH: usize = 128;
const CRON_TASK_TIMEOUT: Duration = Duration::from_mins(30);

fn cron_allowed_tools() -> BTreeSet<String> {
    READ_ONLY_CHILD_TOOLS
        .iter()
        .map(|name| (*name).to_owned())
        .collect()
}

pub trait CronClock: Send + Sync {
    fn now_ms(&self) -> i64;
}

#[derive(Debug)]
struct SystemCronClock;

impl CronClock for SystemCronClock {
    fn now_ms(&self) -> i64 {
        zk_db::time::now_millis()
    }
}

/// Unique server-owned implementation of the tool-side reverse port.
pub struct SqliteCronService {
    db: Db,
    clock: Arc<dyn CronClock>,
}

impl std::fmt::Debug for SqliteCronService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteCronService")
            .finish_non_exhaustive()
    }
}

impl SqliteCronService {
    #[must_use]
    pub fn new(db: Db) -> Self {
        Self {
            db,
            clock: Arc::new(SystemCronClock),
        }
    }

    #[cfg(test)]
    fn with_clock(db: Db, clock: Arc<dyn CronClock>) -> Self {
        Self { db, clock }
    }

    fn now_ms(&self) -> i64 {
        self.clock.now_ms()
    }
}

fn task_view(record: CronJobRecord) -> CronTask {
    CronTask {
        job_id: record.job_id,
        cron: record.cron_expression,
        timezone: record.timezone,
        prompt: record.prompt,
        recurring: record.recurring,
        overlap_policy: record.overlap_policy,
        missed_policy: record.missed_policy,
        status: record.status,
        next_scheduled_at: record.next_scheduled_at_ms.map(format_timestamp_ms),
        created_at: format_timestamp_ms(record.created_at_ms),
        updated_at: format_timestamp_ms(record.updated_at_ms),
    }
}

fn port_error(error: zk_db::DbError) -> CronPortError {
    match error {
        zk_db::DbError::Invalid(code) => {
            let message = match code.as_str() {
                "CRON_JOB_LIMIT_REACHED" => "Maximum number of scheduled jobs reached (50)",
                "CRON_OWNER_SESSION_NOT_FOUND" => "Cron jobs require an active root session",
                _ => "Cron request was rejected by the durable runtime",
            };
            CronPortError::new(code, message, false)
        }
        other => CronPortError::new(
            "CRON_STORAGE_ERROR",
            format!("Cron storage operation failed: {other}"),
            true,
        ),
    }
}

impl CronTaskPort for SqliteCronService {
    fn create(&self, request: CronCreateRequest) -> BoxFuture<'_, Result<CronTask, CronPortError>> {
        Box::pin(async move {
            let now_ms = self.now_ms();
            let next_scheduled_at_ms =
                next_run_after_ms(&request.cron_expression, &request.timezone, now_ms)
                    .map_err(|message| CronPortError::new("INVALID_CRON", message, false))?;
            let record = self
                .db
                .create_cron_job(&NewCronJob {
                    job_id: uuid::Uuid::new_v4().to_string(),
                    owner_session_id: request.owner_session_id,
                    cron_expression: request.cron_expression,
                    timezone: request.timezone,
                    prompt: request.prompt,
                    recurring: request.recurring,
                    overlap_policy: request.overlap_policy,
                    missed_policy: request.missed_policy,
                    next_scheduled_at_ms,
                    now_ms,
                })
                .await
                .map_err(port_error)?;
            Ok(task_view(record))
        })
    }

    fn list(
        &self,
        owner_session_id: String,
    ) -> BoxFuture<'_, Result<Vec<CronTask>, CronPortError>> {
        Box::pin(async move {
            self.db
                .list_cron_jobs(&owner_session_id)
                .await
                .map(|records| records.into_iter().map(task_view).collect())
                .map_err(port_error)
        })
    }

    fn delete(
        &self,
        owner_session_id: String,
        job_id: String,
    ) -> BoxFuture<'_, Result<Option<CronDeleteReceipt>, CronPortError>> {
        Box::pin(async move {
            let deleted = self
                .db
                .delete_cron_job(&owner_session_id, &job_id, self.now_ms())
                .await
                .map_err(port_error)?;
            let Some(deleted) = deleted else {
                return Ok(None);
            };
            let remaining = self
                .db
                .count_cron_jobs(&owner_session_id)
                .await
                .map_err(port_error)?;
            Ok(Some(CronDeleteReceipt {
                task: task_view(deleted),
                remaining: usize::try_from(remaining).unwrap_or(usize::MAX),
            }))
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CronScanReport {
    pub due: usize,
    pub submitted: usize,
    pub skipped: usize,
    pub stale: usize,
    pub dispatch_failures: usize,
}

/// One process-wide scanner. The `AtomicBool` rejects accidental duplicate loops;
/// database CAS and the occurrence unique key remain the cross-process defense.
pub struct CronScheduler {
    db: Db,
    runtime: Arc<TaskRuntime>,
    executor: Arc<SubAgentExecutor>,
    startup_cutoff_ms: i64,
    startup_epoch: i64,
    running: AtomicBool,
}

impl CronScheduler {
    #[must_use]
    pub fn new(
        db: Db,
        runtime: Arc<TaskRuntime>,
        executor: Arc<SubAgentExecutor>,
        startup_cutoff_ms: i64,
    ) -> Self {
        Self {
            db,
            runtime,
            executor,
            startup_cutoff_ms,
            startup_epoch: startup_cutoff_ms,
            running: AtomicBool::new(false),
        }
    }

    /// Execute one deterministic scan. Tests pass a virtual `now_ms`; the
    /// production loop supplies wall clock time.
    pub async fn scan_at(&self, now_ms: i64) -> Result<CronScanReport, zk_db::DbError> {
        let _ = self.db.reconcile_cron_occurrences(now_ms).await?;
        let jobs = self.db.find_due_cron_jobs(now_ms, SCAN_BATCH).await?;
        let mut report = CronScanReport {
            due: jobs.len(),
            ..CronScanReport::default()
        };
        for job in jobs {
            let scheduled_at_ms = job.next_scheduled_at_ms.ok_or_else(|| {
                zk_db::DbError::Invalid("CRON_ACTIVE_JOB_WITHOUT_NEXT_SCHEDULE".to_owned())
            })?;
            let next_scheduled_at_ms = if job.recurring {
                Some(
                    next_run_after_ms(
                        &job.cron_expression,
                        &job.timezone,
                        now_ms.max(scheduled_at_ms),
                    )
                    .map_err(|_| zk_db::DbError::Invalid("CRON_SCHEDULE_CORRUPT".to_owned()))?,
                )
            } else {
                None
            };
            let claim = self
                .db
                .claim_cron_occurrence(&ClaimCronOccurrence {
                    job_id: job.job_id.clone(),
                    expected_job_version: job.version,
                    scheduled_at_ms,
                    next_scheduled_at_ms,
                    startup_cutoff_ms: self.startup_cutoff_ms,
                    now_ms,
                    occurrence_id: uuid::Uuid::new_v4().to_string(),
                    session_id: uuid::Uuid::new_v4().to_string(),
                    task_id: uuid::Uuid::new_v4().to_string(),
                    run_id: uuid::Uuid::new_v4().to_string(),
                    startup_epoch: self.startup_epoch,
                })
                .await?;
            match claim {
                CronClaimOutcome::Stale => report.stale += 1,
                CronClaimOutcome::Skipped(_) => report.skipped += 1,
                CronClaimOutcome::Submitted {
                    occurrence,
                    task,
                    run_id,
                    session_id,
                } => {
                    if self
                        .dispatch(job, occurrence, task.id, run_id, session_id, now_ms)
                        .await
                    {
                        report.submitted += 1;
                    } else {
                        report.dispatch_failures += 1;
                    }
                }
            }
        }
        Ok(report)
    }

    async fn dispatch(
        &self,
        job: CronJobRecord,
        occurrence: CronOccurrenceRecord,
        task_id: String,
        run_id: String,
        session_id: String,
        now_ms: i64,
    ) -> bool {
        let executor = Arc::clone(&self.executor);
        let db = self.db.clone();
        let occurrence_id = occurrence.occurrence_id.clone();
        let occurrence_for_start = occurrence_id.clone();
        let prompt = job.prompt.clone();
        let model = job.model.clone();
        let working_dir = job.working_dir.clone();
        let result = self
            .runtime
            .dispatch_precreated_cron(&task_id, CRON_TASK_TIMEOUT, move |execution| async move {
                let _ = db
                    .mark_cron_occurrence_started(&occurrence_for_start, now_ms)
                    .await;
                let persisted = match PersistedChildExecution::try_new(
                    execution.task_id.clone(),
                    execution.run_id.clone(),
                    execution.transcript_session_id.clone(),
                ) {
                    Ok(identity) => identity,
                    Err(error) => return TaskExecutionResult::failed(error),
                };
                let request = AgentRequest::new(
                    execution.task_id.clone(),
                    prompt,
                    Some("general-purpose".to_owned()),
                    Some(model),
                    IsolationMode::None,
                    false,
                );
                let context = ChildExecutionContext {
                    parent_session_id: execution.root_session_id,
                    parent_run_id: execution.run_id,
                    working_directory: working_dir.into(),
                    tool_use_id: occurrence_for_start,
                    // Cron always executes the persisted readOnly contract,
                    // even when ordinary child Agents have passed the write gate.
                    allowed_tools: Some(cron_allowed_tools()),
                    allow_write_tools: false,
                    write_tool_allowlist: None,
                    include_project_prompt: true,
                };
                let result = executor
                    .execute_precreated_with_cancel(
                        &request,
                        &context,
                        &persisted,
                        execution.budget,
                        execution.cancel,
                    )
                    .await;
                agent_result(result)
            })
            .await;
        match result {
            Ok(dispatched) => dispatched,
            Err(error) => {
                tracing::error!(
                    %task_id,
                    %run_id,
                    %session_id,
                    %occurrence_id,
                    code = %error.code,
                    "failed to attach Cron occurrence to TaskRuntime"
                );
                false
            }
        }
    }

    /// Start exactly one production loop for this scheduler instance.
    pub fn spawn(self: &Arc<Self>) -> Option<JoinHandle<()>> {
        if self.running.swap(true, Ordering::AcqRel) {
            return None;
        }
        let scheduler = Arc::clone(self);
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(SCAN_INTERVAL);
            loop {
                interval.tick().await;
                let now_ms = zk_db::time::now_millis();
                match scheduler.scan_at(now_ms).await {
                    Ok(report) if report.due > 0 => tracing::info!(
                        due = report.due,
                        submitted = report.submitted,
                        skipped = report.skipped,
                        stale = report.stale,
                        dispatch_failures = report.dispatch_failures,
                        "Cron scheduler scan completed"
                    ),
                    Ok(_) => {}
                    Err(error) => tracing::error!(%error, "Cron scheduler scan failed"),
                }
            }
        }))
    }
}

fn agent_result(result: zk_engine::AgentResult) -> TaskExecutionResult {
    let error_code = result.error_code;
    let content = result.result.unwrap_or_default();
    match result.status {
        AgentStatus::Completed => TaskExecutionResult::Complete(content),
        AgentStatus::MaxTurns => TaskExecutionResult::Partial {
            content,
            code: error_code.unwrap_or_else(|| "MAX_TURNS".to_owned()),
        },
        AgentStatus::BudgetExhausted => TaskExecutionResult::Partial {
            content,
            code: error_code.unwrap_or_else(|| "BUDGET_EXHAUSTED".to_owned()),
        },
        AgentStatus::Timeout => TaskExecutionResult::Failed {
            message: content,
            code: error_code.unwrap_or_else(|| "TIMEOUT".to_owned()),
        },
        AgentStatus::Interrupted => TaskExecutionResult::Cancelled { message: content },
        AgentStatus::Failed | AgentStatus::AsyncLaunched => TaskExecutionResult::Failed {
            message: content,
            code: error_code.unwrap_or_else(|| "CRON_AGENT_EXECUTION_FAILED".to_owned()),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use zk_tools::{CronCreateTool, CronDeleteTool, CronListTool, CronTaskPort, Tool, ToolContext};

    use super::{CronClock, SqliteCronService, cron_allowed_tools};

    struct FixedClock(i64);

    impl CronClock for FixedClock {
        fn now_ms(&self) -> i64 {
            self.0
        }
    }

    fn context(session_id: &str) -> ToolContext {
        let (progress, _receiver) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), progress).with_session_id(session_id.to_owned())
    }

    #[test]
    fn cron_execution_contract_allows_reads_and_rejects_writes() {
        let allowed = cron_allowed_tools();
        for name in ["Read", "Grep", "Glob", "ListDir", "WebSearch", "WebFetch"] {
            assert!(allowed.contains(name), "required read tool missing: {name}");
        }
        for name in zk_engine::agent::WRITE_CHILD_TOOLS {
            assert!(
                !allowed.contains(*name),
                "Cron readOnly contract leaked write tool: {name}"
            );
        }
        for name in ["Agent", "TaskCreate", "CronCreate", "SendMessage"] {
            assert!(
                !allowed.contains(name),
                "Cron must not recursively delegate or schedule: {name}"
            );
        }
    }

    #[tokio::test]
    async fn cron_tools_crud_through_the_real_sqlite_port() {
        let db = zk_db::Db::open_in_memory().expect("open SQLite");
        let session = db
            .create_session("test-model", "/tmp/zkcode-cron-tool-test")
            .await
            .expect("create root session");
        let concrete = Arc::new(SqliteCronService::with_clock(
            db.clone(),
            Arc::new(FixedClock(1_800_000_000_000)),
        ));
        let port: Arc<dyn CronTaskPort> = concrete;
        let create = CronCreateTool::new(Arc::clone(&port));
        let list = CronListTool::new(Arc::clone(&port));
        let delete = CronDeleteTool::new(port);

        let created = create
            .execute(
                serde_json::json!({
                    "cron": "0 9 * * *",
                    "prompt": "produce a daily read-only status report",
                    "timezone": "Asia/Shanghai",
                    "overlapPolicy": "skip",
                    "missedPolicy": "skip"
                }),
                context(&session.id),
            )
            .await;
        assert!(!created.is_error, "{}", created.content);
        let created: serde_json::Value =
            serde_json::from_str(&created.content).expect("lowerCamelCase create response");
        let job_id = created["jobId"].as_str().expect("jobId").to_owned();
        assert!(created.get("job_id").is_none());
        assert_eq!(created["timezone"], "Asia/Shanghai");
        assert_eq!(created["overlapPolicy"], "skip");

        let persisted = db
            .find_cron_job(&session.id, &job_id)
            .await
            .expect("query SQLite")
            .expect("job persisted");
        assert_eq!(persisted.timezone, "Asia/Shanghai");
        assert_eq!(persisted.status, "active");

        let listed = list
            .execute(serde_json::json!({}), context(&session.id))
            .await;
        assert!(!listed.is_error, "{}", listed.content);
        let listed: serde_json::Value =
            serde_json::from_str(&listed.content).expect("list response");
        assert_eq!(listed["total"], 1);
        assert_eq!(listed["tasks"][0]["jobId"], job_id);

        let deleted = delete
            .execute(serde_json::json!({"jobId": job_id}), context(&session.id))
            .await;
        assert!(!deleted.is_error, "{}", deleted.content);
        let deleted: serde_json::Value =
            serde_json::from_str(&deleted.content).expect("delete response");
        assert_eq!(deleted["status"], "deleted");
        assert_eq!(deleted["remaining"], 0);
        assert!(
            db.list_cron_jobs(&session.id)
                .await
                .expect("list SQLite jobs")
                .is_empty()
        );
    }
}
