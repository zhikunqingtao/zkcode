//! Greenfield `TaskRuntime` repository.
//!
//! The database is authoritative. Submission, terminal result commit, result receipt,
//! and inbox state changes are transaction boundaries; process-local trackers may only
//! cache these rows.
#![allow(
    missing_docs,
    clippy::missing_errors_doc,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]
// Public record fields mirror the self-describing final schema; transaction methods stay
// intentionally cohesive because splitting them would obscure the commit boundary.

use std::fmt::Write as _;

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::Db;
use crate::error::DbError;
use crate::model::{StoredBlock, parse_blocks};
use crate::task_budget::{
    parse_execution_budget, reserve_child_budget_in_current_write,
    settle_task_budget_in_current_write,
};
use crate::time::{format_rfc3339_micros, now_millis};

/// Results up to this size stay in the result row.
pub const INLINE_RESULT_LIMIT: usize = 64 * 1024;
/// A single persisted result cannot exceed 16 MiB. Larger inputs become explicit partial results.
pub const RESULT_HARD_LIMIT: usize = 16 * 1024 * 1024;

#[path = "generated/task_runtime_v4.rs"]
mod generated_contract;

pub use generated_contract::{
    CleanupStatus, ExitReason, ResultStatus, RunStatus, TaskStatus, VerificationStatus,
};

#[derive(Clone, Debug)]
pub struct CreateTaskWithRun {
    pub task_id: String,
    pub run_id: String,
    /// Root user-facing session that owns the whole task tree.
    pub root_session_id: String,
    /// Child transcript session. For a root task, set this equal to `root_session_id`.
    pub transcript_session_id: String,
    pub parent_task_id: Option<String>,
    pub parent_run_id: Option<String>,
    pub creator_tool_use_id: Option<String>,
    pub ordinal: i64,
    pub description: String,
    pub prompt: Option<String>,
    pub task_type: String,
    pub model: String,
    pub working_dir: String,
    pub execution_config_json: String,
    pub startup_epoch: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeTaskRecord {
    pub id: String,
    pub session_id: String,
    pub parent_task_id: Option<String>,
    pub root_task_id: String,
    pub current_run_id: Option<String>,
    pub creator_run_id: Option<String>,
    pub creator_tool_use_id: Option<String>,
    pub ordinal: i64,
    pub description: String,
    pub prompt: Option<String>,
    pub task_type: String,
    pub status: TaskStatus,
    pub reason: Option<String>,
    pub plan_json: Option<String>,
    pub execution_config_json: String,
    pub lifecycle_policy: String,
    pub reported_progress: f64,
    pub cleanup_status: CleanupStatus,
    pub verification_status: VerificationStatus,
    pub token_budget_limit: Option<i64>,
    pub cost_budget_nanos_usd: Option<i64>,
    pub deadline_at_ms: Option<i64>,
    pub budget_reserved_tokens: i64,
    pub budget_reserved_cost_nanos_usd: i64,
    pub budget_consumed_tokens: i64,
    pub budget_consumed_cost_nanos_usd: i64,
    pub budget_version: i64,
    pub usage_complete: bool,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
    pub terminal_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTaskWithRunOutcome {
    pub task: RuntimeTaskRecord,
    pub run_id: String,
    pub transcript_session_id: String,
    pub created: bool,
}

struct ExistingSubmission {
    task_id: String,
    run_id: String,
    transcript_session_id: String,
    root_session_id: String,
    parent_task_id: Option<String>,
    creator_tool_use_id: Option<String>,
    ordinal: i64,
    description: String,
    prompt: Option<String>,
    task_type: String,
    execution_config_json: String,
    startup_epoch: i64,
    parent_run_id: Option<String>,
    model: String,
    working_dir: String,
}

struct ParentSubmissionState {
    root_task_id: String,
    run_task_id: String,
    parent_task_id: Option<String>,
    current_run_id: Option<String>,
    task_status: String,
    run_status: String,
}

fn map_existing_submission(row: &Row<'_>) -> Result<ExistingSubmission, rusqlite::Error> {
    Ok(ExistingSubmission {
        task_id: row.get(0)?,
        run_id: row.get(1)?,
        transcript_session_id: row.get(2)?,
        root_session_id: row.get(3)?,
        parent_task_id: row.get(4)?,
        creator_tool_use_id: row.get(5)?,
        ordinal: row.get(6)?,
        description: row.get(7)?,
        prompt: row.get(8)?,
        task_type: row.get(9)?,
        execution_config_json: row.get(10)?,
        startup_epoch: row.get(11)?,
        parent_run_id: row.get(12)?,
        model: row.get(13)?,
        working_dir: row.get(14)?,
    })
}

fn map_parent_submission_state(row: &Row<'_>) -> Result<ParentSubmissionState, rusqlite::Error> {
    Ok(ParentSubmissionState {
        root_task_id: row.get(0)?,
        run_task_id: row.get(1)?,
        parent_task_id: row.get(2)?,
        current_run_id: row.get(3)?,
        task_status: row.get(4)?,
        run_status: row.get(5)?,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CasOutcome {
    Applied,
    VersionConflict,
    InvalidTransition,
    NotFound,
}

/// Outcome of the fail-closed transition used when an execution can no longer
/// prove that its externally observable result is durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkTaskNeedsAttentionOutcome {
    Marked,
    AlreadyMarked,
    VersionConflict,
    InvalidRun,
    AlreadyTerminal,
    NotFound,
}

pub(crate) const RUNTIME_TASK_COLUMNS: &str =
    "id,session_id,parent_task_id,root_task_id,current_run_id,
    creator_run_id,creator_tool_use_id,ordinal,description,prompt,task_type,status,reason,
    plan_json,execution_config_json,lifecycle_policy,reported_progress,cleanup_status,
    verification_status,token_budget_limit,cost_budget_nanos_usd,deadline_at_ms,
    budget_reserved_tokens,budget_reserved_cost_nanos_usd,budget_consumed_tokens,
    budget_consumed_cost_nanos_usd,budget_version,usage_complete,version,created_at,
    updated_at,terminal_at";

pub(crate) fn map_runtime_task(row: &Row<'_>) -> Result<RuntimeTaskRecord, rusqlite::Error> {
    let status: String = row.get(11)?;
    let cleanup: String = row.get(17)?;
    let verification: String = row.get(18)?;
    Ok(RuntimeTaskRecord {
        id: row.get(0)?,
        session_id: row.get(1)?,
        parent_task_id: row.get(2)?,
        root_task_id: row.get(3)?,
        current_run_id: row.get(4)?,
        creator_run_id: row.get(5)?,
        creator_tool_use_id: row.get(6)?,
        ordinal: row.get(7)?,
        description: row.get(8)?,
        prompt: row.get(9)?,
        task_type: row.get(10)?,
        // Values are protected by CHECK constraints; conversion failures are surfaced by
        // the public reader after this row mapper returns.
        status: TaskStatus::parse(&status).map_err(invalid_to_sql_error)?,
        reason: row.get(12)?,
        plan_json: row.get(13)?,
        execution_config_json: row.get(14)?,
        lifecycle_policy: row.get(15)?,
        reported_progress: row.get(16)?,
        cleanup_status: CleanupStatus::parse(&cleanup).map_err(invalid_to_sql_error)?,
        verification_status: VerificationStatus::parse(&verification)
            .map_err(invalid_to_sql_error)?,
        token_budget_limit: row.get(19)?,
        cost_budget_nanos_usd: row.get(20)?,
        deadline_at_ms: row.get(21)?,
        budget_reserved_tokens: row.get(22)?,
        budget_reserved_cost_nanos_usd: row.get(23)?,
        budget_consumed_tokens: row.get(24)?,
        budget_consumed_cost_nanos_usd: row.get(25)?,
        budget_version: row.get(26)?,
        usage_complete: row.get::<_, i64>(27)? != 0,
        version: row.get(28)?,
        created_at: row.get(29)?,
        updated_at: row.get(30)?,
        terminal_at: row.get(31)?,
    })
}

fn invalid_to_sql_error(error: DbError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn require_uuid_v4(id: &str, field: &str) -> Result<(), DbError> {
    let parsed = uuid::Uuid::parse_str(id)
        .map_err(|_| DbError::Invalid(format!("{field}_MUST_BE_UUID_V4")))?;
    if parsed.get_version() != Some(uuid::Version::Random) || id != parsed.hyphenated().to_string()
    {
        return Err(DbError::Invalid(format!("{field}_MUST_BE_UUID_V4")));
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

impl Db {
    /// Atomically quarantine a Task and its current Run after a durability
    /// invariant fails. This transition deliberately creates no `TaskResult`:
    /// callers must not manufacture a business failure (or success) when the
    /// storage boundary itself is uncertain.
    pub async fn mark_task_run_needs_attention(
        &self,
        task_id: &str,
        run_id: &str,
        expected_task_version: i64,
        reason: &str,
        cleanup_status: CleanupStatus,
    ) -> Result<MarkTaskNeedsAttentionOutcome, DbError> {
        if reason.trim().is_empty() {
            return Err(DbError::Invalid(
                "TASK_NEEDS_ATTENTION_REASON_REQUIRED".to_owned(),
            ));
        }
        let task_id = task_id.to_owned();
        let run_id = run_id.to_owned();
        let reason = reason.chars().take(2048).collect::<String>();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let task: Option<(String, i64, Option<String>)> = tx
                .query_row(
                    "SELECT status,version,current_run_id FROM tasks WHERE id=?1",
                    params![task_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            let Some((status, version, current_run_id)) = task else {
                return Ok(MarkTaskNeedsAttentionOutcome::NotFound);
            };
            if status == TaskStatus::NeedsAttention.as_db() {
                return Ok(MarkTaskNeedsAttentionOutcome::AlreadyMarked);
            }
            if TaskStatus::parse(&status)?.is_terminal() {
                return Ok(MarkTaskNeedsAttentionOutcome::AlreadyTerminal);
            }
            if version != expected_task_version {
                return Ok(MarkTaskNeedsAttentionOutcome::VersionConflict);
            }
            if current_run_id.as_deref() != Some(run_id.as_str()) {
                return Ok(MarkTaskNeedsAttentionOutcome::InvalidRun);
            }
            let run_status: Option<String> = tx
                .query_row(
                    "SELECT status FROM run_envelopes WHERE id=?1 AND task_id=?2",
                    params![run_id, task_id],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(run_status) = run_status else {
                return Ok(MarkTaskNeedsAttentionOutcome::InvalidRun);
            };
            if RunStatus::parse(&run_status)?.is_terminal() {
                return Ok(MarkTaskNeedsAttentionOutcome::InvalidRun);
            }

            let now = format_rfc3339_micros(now_millis());
            let updated = tx.execute(
                "UPDATE tasks SET status='needsAttention',reason=?1,cleanup_status=?2,
                    verification_status='blocked',updated_at=?3,version=version+1
                 WHERE id=?4 AND version=?5 AND current_run_id=?6 AND status IN
                    ('queued','running','waitingDependencies','waitingInteraction','cancelling')",
                params![
                    reason,
                    cleanup_status.as_db(),
                    now,
                    task_id,
                    expected_task_version,
                    run_id,
                ],
            )?;
            if updated != 1 {
                return Ok(MarkTaskNeedsAttentionOutcome::VersionConflict);
            }
            let run_updated = tx.execute(
                "UPDATE run_envelopes SET status='interrupted',exit_reason='internalError',
                    error_summary=?1,cleanup_status=?2,verification_status='blocked',
                    finished_at=?3,terminal_at=?3,updated_at=?3,version=version+1
                 WHERE id=?4 AND task_id=?5 AND status IN
                    ('queued','running','waitingDependencies','waitingInteraction','cancelling')",
                params![reason, cleanup_status.as_db(), now, run_id, task_id],
            )?;
            if run_updated != 1 {
                return Err(DbError::Invalid(
                    "TASK_RUN_NEEDS_ATTENTION_MISMATCH".to_owned(),
                ));
            }
            crate::run::append_event_in_current_write(
                &tx,
                &run_id,
                "task_needs_attention",
                None,
                &serde_json::json!({
                    "protocolVersion": 4,
                    "taskId": task_id,
                    "runId": run_id,
                    "reason": reason,
                }),
            )?;
            tx.commit()?;
            Ok(MarkTaskNeedsAttentionOutcome::Marked)
        })
        .await
    }

    /// Atomically creates the logical Task, its first queued Run, the internal transcript
    /// session (for children), dependency edge, and durable creation event.
    ///
    /// A retry with the same `(parentRunId, creatorToolUseId, ordinal)` returns the
    /// previously committed objects. A reused UUID with different identity fails closed.
    pub async fn create_task_with_run(
        &self,
        request: &CreateTaskWithRun,
    ) -> Result<CreateTaskWithRunOutcome, DbError> {
        require_uuid_v4(&request.task_id, "TASK_ID")?;
        require_uuid_v4(&request.run_id, "RUN_ID")?;
        if request.transcript_session_id != request.root_session_id {
            require_uuid_v4(&request.transcript_session_id, "TRANSCRIPT_SESSION_ID")?;
        }
        if request.ordinal < 0 || request.startup_epoch < 0 {
            return Err(DbError::Invalid(
                "TASK_SUBMISSION_NEGATIVE_COUNTER".to_owned(),
            ));
        }
        if !matches!(request.task_type.as_str(), "agent" | "cron") {
            return Err(DbError::Invalid("UNSUPPORTED_CAPABILITY".to_owned()));
        }
        serde_json::from_str::<serde_json::Value>(&request.execution_config_json)?;
        let requested_budget = parse_execution_budget(&request.execution_config_json)?;
        let request = request.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;

            if let (Some(parent_run_id), Some(tool_use_id)) =
                (request.parent_run_id.as_deref(), request.creator_tool_use_id.as_deref())
            {
                let existing: Option<ExistingSubmission> = tx
                    .query_row(
                        "SELECT t.id,r.id,r.session_id,t.session_id,t.parent_task_id,t.creator_tool_use_id,
                                t.ordinal,t.description,t.prompt,t.task_type,t.execution_config_json,
                                r.startup_epoch,r.parent_run_id,r.model,s.working_dir
                         FROM tasks t
                         JOIN run_envelopes r ON r.id=t.current_run_id
                         JOIN sessions s ON s.id=r.session_id
                         WHERE t.creator_run_id=?1 AND t.creator_tool_use_id=?2 AND t.ordinal=?3",
                        params![parent_run_id, tool_use_id, request.ordinal],
                        map_existing_submission,
                    )
                    .optional()?;
                if let Some(existing) = existing {
                    let equivalent = existing.root_session_id == request.root_session_id
                        && existing.parent_task_id == request.parent_task_id
                        && existing.parent_run_id == request.parent_run_id
                        && existing.creator_tool_use_id == request.creator_tool_use_id
                        && existing.ordinal == request.ordinal
                        && existing.description == request.description
                        && existing.prompt == request.prompt
                        && existing.task_type == request.task_type
                        && existing.execution_config_json == request.execution_config_json
                        && existing.startup_epoch == request.startup_epoch
                        && existing.model == request.model
                        && existing.working_dir == request.working_dir;
                    if !equivalent {
                        return Err(DbError::Invalid(
                            "TASK_IDEMPOTENCY_MISMATCH".to_owned(),
                        ));
                    }
                    let sql = format!("SELECT {RUNTIME_TASK_COLUMNS} FROM tasks WHERE id=?1");
                    let task = tx.query_row(&sql, params![existing.task_id], map_runtime_task)?;
                    tx.commit()?;
                    return Ok(CreateTaskWithRunOutcome {
                        task,
                        run_id: existing.run_id,
                        transcript_session_id: existing.transcript_session_id,
                        created: false,
                    });
                }
            }

            let root_exists: i64 = tx.query_row(
                "SELECT COUNT(*) FROM sessions WHERE id=?1 AND kind='root'",
                params![request.root_session_id],
                |row| row.get(0),
            )?;
            if root_exists != 1 {
                return Err(DbError::Invalid("ROOT_SESSION_NOT_FOUND".to_owned()));
            }

            let is_child = request.parent_task_id.is_some() || request.parent_run_id.is_some();
            if is_child != (request.parent_task_id.is_some() && request.parent_run_id.is_some()) {
                return Err(DbError::Invalid("TASK_PARENT_IDENTITY_INCOMPLETE".to_owned()));
            }
            if is_child && request.task_type == "cron" {
                return Err(DbError::Invalid(
                    "CRON_ROOT_TASK_REQUIRED".to_owned(),
                ));
            }
            let (root_task_id, creator_run_id, parent_execution_state) = if let (
                Some(parent_task_id),
                Some(parent_run_id),
            ) = (
                request.parent_task_id.as_deref(),
                request.parent_run_id.as_deref(),
            ) {
                // v1 is intentionally one child level only.
                let parent: Option<ParentSubmissionState> = tx
                    .query_row(
                        "SELECT t.root_task_id,r.task_id,t.parent_task_id,t.current_run_id,
                                t.status,r.status
                         FROM tasks t JOIN run_envelopes r ON r.id=?2
                         WHERE t.id=?1 AND t.session_id=?3",
                        params![parent_task_id, parent_run_id, request.root_session_id],
                        map_parent_submission_state,
                    )
                    .optional()?;
                let Some(parent) = parent
                else {
                    return Err(DbError::Invalid("TASK_PARENT_NOT_FOUND".to_owned()));
                };
                if parent.run_task_id != parent_task_id || parent.parent_task_id.is_some() {
                    return Err(DbError::Invalid("RECURSIVE_DELEGATION_DISABLED".to_owned()));
                }
                if parent.current_run_id.as_deref() != Some(parent_run_id) {
                    return Err(DbError::Invalid("PARENT_RUN_STALE".to_owned()));
                }
                if parent.task_status != parent.run_status
                    || !matches!(
                        parent.task_status.as_str(),
                        "queued" | "running" | "waitingDependencies"
                    )
                {
                    return Err(DbError::Invalid("PARENT_NOT_RUNNABLE".to_owned()));
                }
                (
                    parent.root_task_id,
                    Some(parent_run_id.to_owned()),
                    Some((parent.task_status, parent.run_status)),
                )
            } else {
                if request.transcript_session_id != request.root_session_id {
                    return Err(DbError::Invalid("ROOT_TASK_TRANSCRIPT_MISMATCH".to_owned()));
                }
                (request.task_id.clone(), None, None)
            };

            let now_ms = now_millis();
            if !is_child
                && requested_budget
                    .deadline_at_ms
                    .is_some_and(|deadline| deadline <= now_ms)
            {
                return Err(DbError::Invalid("TASK_DEADLINE_EXCEEDED".to_owned()));
            }
            let now = format_rfc3339_micros(now_ms);
            let root_token_limit = (!is_child).then_some(requested_budget.token_limit).flatten();
            let root_cost_limit = (!is_child)
                .then_some(requested_budget.cost_limit_nanos_usd)
                .flatten();
            let root_deadline = (!is_child)
                .then_some(requested_budget.deadline_at_ms)
                .flatten();
            tx.execute(
                "INSERT INTO tasks
                    (id,session_id,parent_task_id,root_task_id,current_run_id,creator_run_id,
                     creator_tool_use_id,ordinal,description,prompt,task_type,status,
                     execution_config_json,lifecycle_policy,reported_progress,cleanup_status,
                     verification_status,token_budget_limit,cost_budget_nanos_usd,
                     deadline_at_ms,usage_complete,version,created_at,updated_at)
                 VALUES(?1,?2,?3,?4,NULL,?5,?6,?7,?8,?9,?10,'queued',?11,
                        'attached',0.0,'notRequired','notRequested',?12,?13,?14,1,0,?15,?15)",
                params![
                    request.task_id,
                    request.root_session_id,
                    request.parent_task_id,
                    root_task_id,
                    creator_run_id,
                    request.creator_tool_use_id,
                    request.ordinal,
                    request.description,
                    request.prompt,
                    request.task_type,
                    request.execution_config_json,
                    root_token_limit,
                    root_cost_limit,
                    root_deadline,
                    now,
                ],
            )
            .map_err(|error| map_submission_conflict(&tx, &request, error))?;

            if is_child {
                reserve_child_budget_in_current_write(
                    &tx,
                    &root_task_id,
                    &request.task_id,
                    &requested_budget,
                    &now,
                )?;
            }

            if is_child {
                if request.transcript_session_id == request.root_session_id {
                    return Err(DbError::Invalid("CHILD_TRANSCRIPT_MUST_BE_INTERNAL".to_owned()));
                }
                tx.execute(
                    "INSERT INTO sessions
                        (id,kind,parent_session_id,parent_task_id,model,working_dir,status,
                         created_at,updated_at)
                     VALUES(?1,'internal',?2,?3,?4,?5,'active',?6,?6)",
                    params![
                        request.transcript_session_id,
                        request.root_session_id,
                        request.task_id,
                        request.model,
                        request.working_dir,
                        now,
                    ],
                )?;
            }

            tx.execute(
                "INSERT INTO run_envelopes
                    (id,session_id,task_id,attempt,startup_epoch,parent_run_id,status,version,
                     agent_type,model,started_at,verification_status,cleanup_status,created_at,updated_at)
                 VALUES(?1,?2,?3,1,?4,?5,'queued',0,?6,?7,?8,
                        'notRequested','notRequired',?8,?8)",
                params![
                    request.run_id,
                    request.transcript_session_id,
                    request.task_id,
                    request.startup_epoch,
                    request.parent_run_id,
                    if is_child {
                        "subagent"
                    } else if request.task_type == "cron" {
                        "cron"
                    } else {
                        "query"
                    },
                    request.model,
                    now,
                ],
            )?;
            tx.execute(
                "UPDATE tasks SET current_run_id=?1 WHERE id=?2",
                params![request.run_id, request.task_id],
            )?;
            if let Some(parent_task_id) = request.parent_task_id.as_deref() {
                tx.execute(
                    "INSERT INTO task_dependencies
                        (parent_task_id,child_task_id,lifecycle_policy,required,created_at,updated_at)
                     VALUES(?1,?2,'attached',1,?3,?3)",
                    params![parent_task_id, request.task_id, now],
                )?;

                let (parent_status, parent_run_status) = parent_execution_state
                    .as_ref()
                    .ok_or_else(|| DbError::Invalid("TASK_PARENT_STATE_MISSING".to_owned()))?;
                if parent_status != "waitingDependencies" {
                    let parent_run_id = request
                        .parent_run_id
                        .as_deref()
                        .ok_or_else(|| DbError::Invalid("PARENT_RUN_NOT_FOUND".to_owned()))?;
                    let task_updated = tx.execute(
                        "UPDATE tasks SET status='waitingDependencies',
                            reason='attachedChildrenPending',updated_at=?1,version=version+1
                         WHERE id=?2 AND current_run_id=?3 AND status=?4",
                        params![now, parent_task_id, parent_run_id, parent_status],
                    )?;
                    let run_updated = tx.execute(
                        "UPDATE run_envelopes SET status='waitingDependencies',
                            waiting_reason='attachedChildrenPending',updated_at=?1,version=version+1
                         WHERE id=?2 AND task_id=?3 AND status=?4",
                        params![now, parent_run_id, parent_task_id, parent_run_status],
                    )?;
                    if task_updated != 1 || run_updated != 1 {
                        return Err(DbError::Invalid(
                            "PARENT_WAITING_DEPENDENCIES_MISMATCH".to_owned(),
                        ));
                    }
                    crate::run::append_event_in_current_write(
                        &tx,
                        parent_run_id,
                        "task_waiting_dependencies",
                        None,
                        &serde_json::json!({
                            "taskId": parent_task_id,
                            "childTaskId": request.task_id,
                            "reason": "attachedChildrenPending",
                        }),
                    )?;
                }
            }
            tx.execute(
                "INSERT INTO run_event_log(run_id,seq,event_type,event_data,ts)
                 VALUES(?1,0,'task_created',?2,?3)",
                params![
                    request.run_id,
                    serde_json::json!({
                        "protocolVersion": 4,
                        "taskId": request.task_id,
                        "runId": request.run_id,
                        "parentTaskId": request.parent_task_id,
                    })
                    .to_string(),
                    now_millis(),
                ],
            )?;
            let sql = format!("SELECT {RUNTIME_TASK_COLUMNS} FROM tasks WHERE id=?1");
            let task = tx.query_row(&sql, params![request.task_id], map_runtime_task)?;
            tx.commit()?;
            Ok(CreateTaskWithRunOutcome {
                task,
                run_id: request.run_id,
                transcript_session_id: request.transcript_session_id,
                created: true,
            })
        })
        .await
    }

    pub async fn find_runtime_task_by_id(
        &self,
        task_id: &str,
    ) -> Result<Option<RuntimeTaskRecord>, DbError> {
        let task_id = task_id.to_owned();
        self.with_reader(move |conn| {
            let sql = format!("SELECT {RUNTIME_TASK_COLUMNS} FROM tasks WHERE id=?1");
            conn.query_row(&sql, params![task_id], map_runtime_task)
                .optional()
                .map_err(Into::into)
        })
        .await
    }

    /// Returns only tasks owned by the supplied root session, ordered as a stable tree.
    pub async fn find_task_tree_owned(
        &self,
        root_session_id: &str,
    ) -> Result<Vec<RuntimeTaskRecord>, DbError> {
        let root_session_id = root_session_id.to_owned();
        self.with_reader(move |conn| {
            let sql = format!(
                "SELECT {RUNTIME_TASK_COLUMNS} FROM tasks
                 WHERE session_id=?1 ORDER BY root_task_id,created_at,ordinal,id"
            );
            let mut stmt = conn.prepare(&sql)?;
            Ok(stmt
                .query_map(params![root_session_id], map_runtime_task)?
                .collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// Optimistic non-terminal transition. Every terminal transition is owned by
    /// [`Self::commit_task_result`], which atomically binds the Task, current Run,
    /// immutable result and final Assistant message. Generic callers cannot bypass
    /// that boundary by supplying a terminal target.
    pub async fn transition_task_cas(
        &self,
        task_id: &str,
        expected_version: i64,
        expected_statuses: &[TaskStatus],
        target: TaskStatus,
        reason: Option<&str>,
        cleanup_status: CleanupStatus,
        verification_status: VerificationStatus,
    ) -> Result<CasOutcome, DbError> {
        if target.is_terminal() {
            return Ok(CasOutcome::InvalidTransition);
        }
        let task_id = task_id.to_owned();
        let expected = expected_statuses
            .iter()
            .map(|status| status.as_db().to_owned())
            .collect::<Vec<_>>();
        let reason = reason.map(str::to_owned);
        self.with_writer(move |conn| {
            let current: Option<(String, i64)> = conn
                .query_row(
                    "SELECT status,version FROM tasks WHERE id=?1",
                    params![task_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((current_status, current_version)) = current else {
                return Ok(CasOutcome::NotFound);
            };
            if TaskStatus::parse(&current_status)?.is_terminal() {
                return Ok(CasOutcome::InvalidTransition);
            }
            if current_version != expected_version {
                return Ok(CasOutcome::VersionConflict);
            }
            if !expected.iter().any(|status| status == &current_status) {
                return Ok(CasOutcome::InvalidTransition);
            }
            let now = format_rfc3339_micros(now_millis());
            let updated = conn.execute(
                "UPDATE tasks SET status=?1,reason=?2,cleanup_status=?3,
                    verification_status=?4,terminal_at=NULL,updated_at=?5,version=version+1
                 WHERE id=?6 AND version=?7 AND status=?8",
                params![
                    target.as_db(),
                    reason,
                    cleanup_status.as_db(),
                    verification_status.as_db(),
                    now,
                    task_id,
                    expected_version,
                    current_status,
                ],
            )?;
            Ok(if updated == 1 {
                CasOutcome::Applied
            } else {
                CasOutcome::VersionConflict
            })
        })
        .await
    }

    /// Atomically claims a queued Task and its queued current Run for execution.
    /// Neither row is changed when either half of the ownership/status CAS fails.
    pub async fn claim_task_run_cas(
        &self,
        task_id: &str,
        run_id: &str,
        expected_task_version: i64,
    ) -> Result<CasOutcome, DbError> {
        let task_id = task_id.to_owned();
        let run_id = run_id.to_owned();
        self.with_writer(move |connection| {
            let tx = connection.transaction()?;
            let now = format_rfc3339_micros(now_millis());
            let task_updated = tx.execute(
                "UPDATE tasks SET status='running',reason=NULL,updated_at=?1,version=version+1
                 WHERE id=?2 AND current_run_id=?3 AND version=?4 AND status='queued'",
                params![now, task_id, run_id, expected_task_version],
            )?;
            if task_updated != 1 {
                let current: Option<(i64, String)> = tx
                    .query_row(
                        "SELECT version,status FROM tasks WHERE id=?1",
                        params![task_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                return Ok(match current {
                    None => CasOutcome::NotFound,
                    Some((version, _)) if version != expected_task_version => {
                        CasOutcome::VersionConflict
                    }
                    Some(_) => CasOutcome::InvalidTransition,
                });
            }
            let run_updated = tx.execute(
                "UPDATE run_envelopes SET status='running',updated_at=?1,version=version+1
                 WHERE id=?2 AND task_id=?3 AND status='queued'",
                params![now, run_id, task_id],
            )?;
            if run_updated != 1 {
                return Ok(CasOutcome::InvalidTransition);
            }
            crate::run::append_event_in_current_write(
                &tx,
                &run_id,
                "task_claimed",
                None,
                &serde_json::json!({"taskId": task_id, "status": "running"}),
            )?;
            tx.commit()?;
            Ok(CasOutcome::Applied)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BudgetReservationStatus, LlmCallBudgetReservation, LlmUsageCompletion, MessageAttribution,
        MessageRole, NewLlmCall, NewMessage, NewToolInvocation, StoredBlock, TaskBudgetLimits,
        ToolInvocationStatus,
    };

    fn id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn root_request(session_id: &str) -> CreateTaskWithRun {
        CreateTaskWithRun {
            task_id: id(),
            run_id: id(),
            root_session_id: session_id.to_owned(),
            transcript_session_id: session_id.to_owned(),
            parent_task_id: None,
            parent_run_id: None,
            creator_tool_use_id: None,
            ordinal: 0,
            description: "root task".to_owned(),
            prompt: Some("root prompt".to_owned()),
            task_type: "agent".to_owned(),
            model: "test-model".to_owned(),
            working_dir: "/tmp/zk-task-runtime".to_owned(),
            execution_config_json: serde_json::json!({
                "budget": {
                    "tokenLimit": 1_000_000,
                    "costLimitNanosUsd": 1_000_000_000_000_i64,
                    "deadlineAtMs": now_millis() + 60_000,
                }
            })
            .to_string(),
            startup_epoch: 1,
        }
    }

    fn child_request(session_id: &str, parent: &CreateTaskWithRunOutcome) -> CreateTaskWithRun {
        CreateTaskWithRun {
            task_id: id(),
            run_id: id(),
            root_session_id: session_id.to_owned(),
            transcript_session_id: id(),
            parent_task_id: Some(parent.task.id.clone()),
            parent_run_id: Some(parent.run_id.clone()),
            creator_tool_use_id: Some("tool-use-1".to_owned()),
            ordinal: 0,
            description: "child task".to_owned(),
            prompt: Some("child prompt".to_owned()),
            task_type: "agent".to_owned(),
            model: "test-model".to_owned(),
            working_dir: "/tmp/zk-task-runtime".to_owned(),
            execution_config_json: r#"{"isolation":"readOnly"}"#.to_owned(),
            startup_epoch: 1,
        }
    }

    async fn set_parent_execution_state(db: &Db, task_id: &str, run_id: &str, status: TaskStatus) {
        let task_id = task_id.to_owned();
        let run_id = run_id.to_owned();
        let status = status.as_db().to_owned();
        db.with_writer(move |connection| {
            let tx = connection.transaction()?;
            let now = format_rfc3339_micros(now_millis());
            assert_eq!(
                tx.execute(
                    "UPDATE tasks SET status=?1,updated_at=?2,version=version+1 WHERE id=?3",
                    params![status, now, task_id],
                )?,
                1
            );
            assert_eq!(
                tx.execute(
                    "UPDATE run_envelopes SET status=?1,updated_at=?2,version=version+1
                     WHERE id=?3 AND task_id=?4",
                    params![status, now, run_id, task_id],
                )?,
                1
            );
            tx.commit()?;
            Ok(())
        })
        .await
        .expect("parent state");
    }

    async fn commit_complete_child(db: &Db, child: &CreateTaskWithRunOutcome, content: &str) {
        append_final_assistant(db, &child.task.id, &child.run_id, content).await;
        let committed = db
            .commit_task_result(&CommitTaskResult {
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                expected_task_version: child.task.version,
                status: ResultStatus::Complete,
                content: content.to_owned(),
                media_type: "text/markdown".to_owned(),
                error_code: None,
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect("child result");
        assert!(matches!(
            committed,
            CommitTaskResultOutcome::Committed { .. }
        ));
    }

    async fn append_final_assistant(db: &Db, task_id: &str, run_id: &str, content: &str) {
        let run = db
            .find_run_by_id(run_id)
            .await
            .expect("run lookup")
            .expect("run");
        db.append_attributed_message(
            &run.session_id,
            NewMessage {
                role: MessageRole::Assistant,
                content: vec![StoredBlock::Text {
                    text: content.to_owned(),
                }],
                stop_reason: Some("end_turn".to_owned()),
                input_tokens: 0,
                output_tokens: 0,
            },
            MessageAttribution {
                task_id: Some(task_id.to_owned()),
                run_id: Some(run_id.to_owned()),
                origin: "conversation".to_owned(),
                source_task_id: None,
            },
        )
        .await
        .expect("final assistant message");
    }

    async fn child_with_started_llm_call(
        db: &Db,
        working_dir: &str,
    ) -> (CreateTaskWithRunOutcome, CreateTaskWithRunOutcome, String) {
        let session = db.create_session("m", working_dir).await.expect("session");
        let mut request = root_request(&session.id);
        request.execution_config_json = serde_json::json!({
            "budget": {
                "tokenLimit": 1_000,
                "costLimitNanosUsd": 10_000,
                "deadlineAtMs": now_millis() + 60_000,
            }
        })
        .to_string();
        let root = db.create_task_with_run(&request).await.expect("root");
        let child = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("child");
        assert_eq!(
            db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
                .await
                .expect("claim child"),
            CasOutcome::Applied
        );
        let call_id = id();
        db.start_llm_call_with_budget(
            &NewLlmCall {
                call_id: call_id.clone(),
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                provider: "script".to_owned(),
                model: "m".to_owned(),
                route: None,
                provider_request_id: None,
            },
            &LlmCallBudgetReservation {
                input_tokens: 50,
                output_tokens: 30,
                cost_nanos_usd: 750,
            },
        )
        .await
        .expect("call start");
        (root, child, call_id)
    }

    async fn llm_call_status(db: &Db, call_id: &str) -> String {
        let call_id = call_id.to_owned();
        db.with_reader(move |connection| {
            connection
                .query_row(
                    "SELECT status FROM llm_calls WHERE call_id=?1",
                    params![call_id],
                    |row| row.get(0),
                )
                .map_err(Into::into)
        })
        .await
        .expect("llm call status")
    }

    async fn task_result_count(db: &Db, task_id: &str) -> i64 {
        let task_id = task_id.to_owned();
        db.with_reader(move |connection| {
            connection
                .query_row(
                    "SELECT COUNT(*) FROM task_results WHERE task_id=?1",
                    params![task_id],
                    |row| row.get(0),
                )
                .map_err(Into::into)
        })
        .await
        .expect("task result count")
    }

    #[tokio::test]
    async fn terminal_usage_fallback_projects_run_and_session_exactly_once() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/usage-fallback")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root");
        append_final_assistant(&db, &root.task.id, &root.run_id, "done").await;
        let request = CommitTaskResult {
            task_id: root.task.id.clone(),
            run_id: root.run_id.clone(),
            expected_task_version: root.task.version,
            status: ResultStatus::Complete,
            content: "done".to_owned(),
            media_type: "text/markdown".to_owned(),
            error_code: None,
            cleanup_status: CleanupStatus::Confirmed,
            verification_status: VerificationStatus::NotRequested,
        };
        let fallback = RunUsageFallback {
            input_tokens: 120,
            output_tokens: 30,
            cache_read_tokens: 10,
            cache_create_tokens: 4,
            cost_nanos_usd: 987_654,
            usage_complete: true,
        };
        assert!(matches!(
            db.commit_task_result_with_run_usage_fallback(&request, fallback)
                .await
                .expect("first commit"),
            CommitTaskResultOutcome::Committed { .. }
        ));

        let run = db
            .find_run_by_id(&root.run_id)
            .await
            .expect("run query")
            .expect("run");
        assert_eq!(run.input_tokens, 120);
        assert_eq!(run.output_tokens, 30);
        assert_eq!(run.cache_read_tokens, 10);
        assert_eq!(run.cache_create_tokens, 4);
        assert_eq!(run.total_tokens, 150);
        assert_eq!(run.cost_nanos_usd, 987_654);
        assert!((run.total_cost_usd - 0.000_987_654).abs() < f64::EPSILON);
        assert!(
            db.read_llm_usage_integrity(&root.task.id, &root.run_id)
                .await
                .expect("usage integrity")
                .expect("current run")
                .is_complete()
        );
        let detail = db
            .get_session(&session.id)
            .await
            .expect("session query")
            .expect("session");
        assert_eq!(detail.total_usage.input_tokens, 120);
        assert_eq!(detail.total_usage.output_tokens, 30);
        assert!((detail.total_cost_usd - 0.000_987_654).abs() < f64::EPSILON);

        assert_eq!(
            db.commit_task_result_with_run_usage_fallback(&request, fallback)
                .await
                .expect("idempotent replay"),
            CommitTaskResultOutcome::AlreadyTerminal
        );
        let detail = db
            .get_session(&session.id)
            .await
            .expect("session replay query")
            .expect("session");
        assert_eq!(detail.total_usage.input_tokens, 120);
        assert_eq!(detail.total_usage.output_tokens, 30);
        assert!((detail.total_cost_usd - 0.000_987_654).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn incomplete_usage_fallback_rejects_root_complete_without_partial_writes() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/incomplete-usage-fallback")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root");
        append_final_assistant(&db, &root.task.id, &root.run_id, "done").await;
        let before_task = db
            .find_runtime_task_by_id(&root.task.id)
            .await
            .expect("task")
            .expect("root exists");
        let before_run = db
            .find_run_by_id(&root.run_id)
            .await
            .expect("run")
            .expect("run exists");
        let before_budget = db
            .read_task_budget(&root.task.id)
            .await
            .expect("budget")
            .expect("root budget exists");

        let error = db
            .commit_task_result_with_run_usage_fallback(
                &CommitTaskResult {
                    task_id: root.task.id.clone(),
                    run_id: root.run_id.clone(),
                    expected_task_version: before_task.version,
                    status: ResultStatus::Complete,
                    content: "done".to_owned(),
                    media_type: "text/markdown".to_owned(),
                    error_code: None,
                    cleanup_status: CleanupStatus::Confirmed,
                    verification_status: VerificationStatus::NotRequested,
                },
                RunUsageFallback {
                    input_tokens: 120,
                    output_tokens: 30,
                    cache_read_tokens: 10,
                    cache_create_tokens: 4,
                    cost_nanos_usd: 987_654,
                    usage_complete: false,
                },
            )
            .await
            .expect_err("incomplete fallback cannot publish success");
        assert!(matches!(
            error,
            DbError::Invalid(ref code) if code == "BUDGET_USAGE_INCOMPLETE"
        ));

        let after_task = db
            .find_runtime_task_by_id(&root.task.id)
            .await
            .expect("task")
            .expect("root exists");
        assert_eq!(after_task, before_task);
        let after_run = db
            .find_run_by_id(&root.run_id)
            .await
            .expect("run")
            .expect("run exists");
        assert_eq!(after_run.status, before_run.status);
        assert_eq!(after_run.version, before_run.version);
        assert_eq!(after_run.total_tokens, before_run.total_tokens);
        assert_eq!(after_run.cost_nanos_usd, before_run.cost_nanos_usd);
        assert_eq!(after_run.usage_complete, before_run.usage_complete);
        assert_eq!(
            db.read_task_budget(&root.task.id)
                .await
                .expect("budget")
                .expect("root budget exists"),
            before_budget
        );
        assert_eq!(task_result_count(&db, &root.task.id).await, 0);
        let detail = db
            .get_session(&session.id)
            .await
            .expect("session")
            .expect("session exists");
        assert_eq!(detail.total_usage.input_tokens, 0);
        assert_eq!(detail.total_usage.output_tokens, 0);
        assert!(detail.total_cost_usd.abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn complete_without_fallback_or_llm_calls_uses_authoritative_run_state() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/direct-provider-complete")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root");
        append_final_assistant(&db, &root.task.id, &root.run_id, "done").await;

        let committed = db
            .commit_task_result(&CommitTaskResult {
                task_id: root.task.id.clone(),
                run_id: root.run_id.clone(),
                expected_task_version: root.task.version,
                status: ResultStatus::Complete,
                content: "done".to_owned(),
                media_type: "text/markdown".to_owned(),
                error_code: None,
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect("complete authoritative run");
        assert!(matches!(
            committed,
            CommitTaskResultOutcome::Committed { .. }
        ));
        let task = db
            .find_runtime_task_by_id(&root.task.id)
            .await
            .expect("task")
            .expect("root exists");
        assert_eq!(task.status, TaskStatus::Succeeded);
        assert!(task.usage_complete);
    }

    #[tokio::test]
    async fn physical_llm_ledger_wins_over_terminal_usage_fallback() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/usage-ledger")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root");
        assert_eq!(
            db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                .await
                .expect("claim"),
            CasOutcome::Applied
        );
        let call_id = id();
        db.start_llm_call_with_budget(
            &NewLlmCall {
                call_id: call_id.clone(),
                task_id: root.task.id.clone(),
                run_id: root.run_id.clone(),
                provider: "script".to_owned(),
                model: "m".to_owned(),
                route: None,
                provider_request_id: None,
            },
            &LlmCallBudgetReservation {
                input_tokens: 12,
                output_tokens: 3,
                cost_nanos_usd: 456_789,
            },
        )
        .await
        .expect("call start");
        assert_eq!(
            db.finish_llm_call(
                &call_id,
                "completed",
                &LlmUsageCompletion {
                    input_tokens: Some(12),
                    output_tokens: Some(3),
                    cache_read_tokens: Some(2),
                    cache_create_tokens: Some(1),
                    cost_nanos_usd: Some(456_789),
                    usage_complete: true,
                    error_code: None,
                },
            )
            .await
            .expect("call finish"),
            CasOutcome::Applied
        );
        append_final_assistant(&db, &root.task.id, &root.run_id, "done").await;
        let current = db
            .find_runtime_task_by_id(&root.task.id)
            .await
            .expect("task query")
            .expect("task");
        let request = CommitTaskResult {
            task_id: root.task.id.clone(),
            run_id: root.run_id.clone(),
            expected_task_version: current.version,
            status: ResultStatus::Complete,
            content: "done".to_owned(),
            media_type: "text/markdown".to_owned(),
            error_code: None,
            cleanup_status: CleanupStatus::Confirmed,
            verification_status: VerificationStatus::NotRequested,
        };
        db.commit_task_result_with_run_usage_fallback(
            &request,
            RunUsageFallback {
                input_tokens: 999,
                output_tokens: 999,
                cache_read_tokens: 999,
                cache_create_tokens: 999,
                cost_nanos_usd: 999_999_999,
                usage_complete: true,
            },
        )
        .await
        .expect("commit");

        let run = db
            .find_run_by_id(&root.run_id)
            .await
            .expect("run query")
            .expect("run");
        assert_eq!(run.input_tokens, 12);
        assert_eq!(run.output_tokens, 3);
        assert_eq!(run.cache_read_tokens, 2);
        assert_eq!(run.cache_create_tokens, 1);
        assert_eq!(run.total_tokens, 15);
        assert_eq!(run.cost_nanos_usd, 456_789);
        let detail = db
            .get_session(&session.id)
            .await
            .expect("session query")
            .expect("session");
        assert_eq!(detail.total_usage.input_tokens, 12);
        assert_eq!(detail.total_usage.output_tokens, 3);
        assert!((detail.total_cost_usd - 0.000_456_789).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn unlimited_root_grants_child_without_token_or_cost_ceiling() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/unlimited-root")
            .await
            .expect("session");
        let mut request = root_request(&session.id);
        request.execution_config_json = serde_json::json!({
            "budget": {
                "deadlineAtMs": now_millis() + 60_000,
            }
        })
        .to_string();
        let root = db.create_task_with_run(&request).await.expect("root");
        assert_eq!(
            db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                .await
                .expect("claim root"),
            CasOutcome::Applied
        );

        let child = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("unlimited child reservation");
        assert_eq!(child.task.token_budget_limit, None);
        assert_eq!(child.task.cost_budget_nanos_usd, None);
        let budget = db
            .read_task_budget(&child.task.id)
            .await
            .expect("child budget query")
            .expect("child budget");
        let reservation = budget.reservation.expect("durable reservation");
        assert_eq!(reservation.reserved_tokens, None);
        assert_eq!(reservation.reserved_cost_nanos_usd, None);
        assert_eq!(reservation.status, BudgetReservationStatus::Active);
    }

    #[tokio::test]
    async fn root_physical_calls_and_children_share_one_atomic_budget_account() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/root-shared-budget")
            .await
            .expect("session");
        let mut request = root_request(&session.id);
        request.execution_config_json = serde_json::json!({
            "budget": {
                "tokenLimit": 1_000,
                "costLimitNanosUsd": 10_000,
                "deadlineAtMs": now_millis() + 60_000,
            }
        })
        .to_string();
        let root = db.create_task_with_run(&request).await.expect("root");
        assert_eq!(
            db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                .await
                .expect("claim root"),
            CasOutcome::Applied
        );
        let child = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("20% child reservation");
        assert_eq!(child.task.token_budget_limit, Some(200));

        let oversized = db
            .start_llm_call_with_budget(
                &NewLlmCall {
                    call_id: id(),
                    task_id: root.task.id.clone(),
                    run_id: root.run_id.clone(),
                    provider: "script".to_owned(),
                    model: "m".to_owned(),
                    route: None,
                    provider_request_id: None,
                },
                &LlmCallBudgetReservation {
                    input_tokens: 100,
                    output_tokens: 701,
                    cost_nanos_usd: 8_000,
                },
            )
            .await
            .expect_err("child reservation must remain charged to root admission");
        assert!(oversized.to_string().contains("TOKEN_BUDGET_EXHAUSTED"));

        let call_id = id();
        db.start_llm_call_with_budget(
            &NewLlmCall {
                call_id: call_id.clone(),
                task_id: root.task.id.clone(),
                run_id: root.run_id.clone(),
                provider: "script".to_owned(),
                model: "m".to_owned(),
                route: None,
                provider_request_id: None,
            },
            &LlmCallBudgetReservation {
                input_tokens: 100,
                output_tokens: 700,
                cost_nanos_usd: 8_000,
            },
        )
        .await
        .expect("exact remaining root budget is admissible");

        let mut second_child = child_request(&session.id, &root);
        second_child.ordinal = 2;
        second_child.creator_tool_use_id = Some(id());
        let unavailable = db
            .create_task_with_run(&second_child)
            .await
            .expect_err("active physical reservation must block child oversubscription");
        assert!(unavailable.to_string().contains("TOKEN_BUDGET_UNAVAILABLE"));

        assert_eq!(
            db.finish_llm_call(
                &call_id,
                "completed",
                &LlmUsageCompletion {
                    input_tokens: Some(100),
                    output_tokens: Some(700),
                    cache_read_tokens: Some(0),
                    cache_create_tokens: Some(0),
                    cost_nanos_usd: Some(8_000),
                    usage_complete: true,
                    error_code: None,
                },
            )
            .await
            .expect("finish call"),
            CasOutcome::Applied
        );
    }

    #[tokio::test]
    async fn authoritative_child_overrun_terminalizes_without_poisoning_usage() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/child-provider-overrun")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root");
        assert_eq!(
            db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                .await
                .expect("claim root"),
            CasOutcome::Applied
        );
        let child = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("child");
        assert_eq!(
            db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
                .await
                .expect("claim child"),
            CasOutcome::Applied
        );
        let child_cost_limit = child.task.cost_budget_nanos_usd.expect("child cost limit");
        let call_id = id();
        db.start_llm_call_with_budget(
            &NewLlmCall {
                call_id: call_id.clone(),
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                provider: "script".to_owned(),
                model: "m".to_owned(),
                route: None,
                provider_request_id: None,
            },
            &LlmCallBudgetReservation {
                input_tokens: 10,
                output_tokens: 10,
                cost_nanos_usd: 10,
            },
        )
        .await
        .expect("estimated request is admitted");
        assert_eq!(
            db.finish_llm_call(
                &call_id,
                "completed",
                &LlmUsageCompletion {
                    input_tokens: Some(10),
                    output_tokens: Some(10),
                    cache_read_tokens: Some(0),
                    cache_create_tokens: Some(0),
                    cost_nanos_usd: Some(child_cost_limit + 1),
                    usage_complete: true,
                    error_code: None,
                },
            )
            .await
            .expect("provider usage is authoritative"),
            CasOutcome::Applied
        );
        let overrun = db
            .assert_task_run_budget_within_limits(&child.task.id, &child.run_id)
            .await
            .expect_err("post-turn gate must catch provider overrun");
        assert!(overrun.to_string().contains("COST_BUDGET_EXHAUSTED"));

        let current = db
            .find_runtime_task_by_id(&child.task.id)
            .await
            .expect("child read")
            .expect("child");
        let committed = db
            .commit_task_result(&CommitTaskResult {
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                expected_task_version: current.version,
                status: ResultStatus::Partial,
                content: "COST_BUDGET_EXHAUSTED".to_owned(),
                media_type: "text/plain".to_owned(),
                error_code: Some("COST_BUDGET_EXHAUSTED".to_owned()),
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect("overrun must not block terminal persistence");
        assert!(matches!(
            committed,
            CommitTaskResultOutcome::Committed { .. }
        ));

        let child_after = db
            .find_runtime_task_by_id(&child.task.id)
            .await
            .expect("child read")
            .expect("child");
        assert_eq!(child_after.status, TaskStatus::Partial);
        assert!(child_after.usage_complete);
        let root_budget = db
            .read_task_budget(&root.task.id)
            .await
            .expect("root budget")
            .expect("root");
        assert!(root_budget.usage_complete);
        assert_eq!(
            root_budget.reserved_cost_nanos_usd, child_cost_limit,
            "an overrun may not release budget for unsafe reuse"
        );
        assert_eq!(
            root_budget.consumed_cost_nanos_usd, 1,
            "known spend above the immutable grant must also be charged"
        );
        let reservation = db
            .read_task_budget(&child.task.id)
            .await
            .expect("child budget")
            .expect("child")
            .reservation
            .expect("reservation");
        assert_eq!(reservation.status, BudgetReservationStatus::Incomplete);
        assert_eq!(reservation.used_cost_nanos_usd, Some(child_cost_limit + 1));
        assert!(reservation.usage_complete);

        db.assert_llm_usage_complete(&root.task.id, &root.run_id)
            .await
            .expect("known child overrun must not poison parent synthesis");
        let root_call_id = id();
        db.start_llm_call_with_budget(
            &NewLlmCall {
                call_id: root_call_id.clone(),
                task_id: root.task.id.clone(),
                run_id: root.run_id.clone(),
                provider: "script".to_owned(),
                model: "m".to_owned(),
                route: None,
                provider_request_id: None,
            },
            &LlmCallBudgetReservation {
                input_tokens: 1,
                output_tokens: 1,
                cost_nanos_usd: 1,
            },
        )
        .await
        .expect("root may spend its proven remainder to synthesize partial child results");
        assert_eq!(
            db.finish_llm_call(
                &root_call_id,
                "completed",
                &LlmUsageCompletion {
                    input_tokens: Some(1),
                    output_tokens: Some(1),
                    cache_read_tokens: Some(0),
                    cache_create_tokens: Some(0),
                    cost_nanos_usd: Some(1),
                    usage_complete: true,
                    error_code: None,
                },
            )
            .await
            .expect("finish root synthesis call"),
            CasOutcome::Applied
        );
    }

    #[tokio::test]
    async fn child_overrun_saturates_root_remainder_without_breaking_terminal_commit() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/child-provider-overrun-cap")
            .await
            .expect("session");
        let mut request = root_request(&session.id);
        request.execution_config_json = serde_json::json!({
            "budget": {
                "tokenLimit": 1_000,
                "costLimitNanosUsd": 10_000,
                "deadlineAtMs": now_millis() + 60_000,
            }
        })
        .to_string();
        let root = db.create_task_with_run(&request).await.expect("root");
        assert_eq!(
            db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                .await
                .expect("claim root"),
            CasOutcome::Applied
        );

        let root_call_id = id();
        db.start_llm_call_with_budget(
            &NewLlmCall {
                call_id: root_call_id.clone(),
                task_id: root.task.id.clone(),
                run_id: root.run_id.clone(),
                provider: "script".to_owned(),
                model: "m".to_owned(),
                route: None,
                provider_request_id: None,
            },
            &LlmCallBudgetReservation {
                input_tokens: 1,
                output_tokens: 1,
                cost_nanos_usd: 7_500,
            },
        )
        .await
        .expect("root pre-child call");
        db.finish_llm_call(
            &root_call_id,
            "completed",
            &LlmUsageCompletion {
                input_tokens: Some(1),
                output_tokens: Some(1),
                cache_read_tokens: Some(0),
                cache_create_tokens: Some(0),
                cost_nanos_usd: Some(7_500),
                usage_complete: true,
                error_code: None,
            },
        )
        .await
        .expect("finish root pre-child call");

        let child = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("remaining budget admits one child");
        assert_eq!(child.task.cost_budget_nanos_usd, Some(2_000));
        assert_eq!(
            db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
                .await
                .expect("claim child"),
            CasOutcome::Applied
        );
        let child_call_id = id();
        db.start_llm_call_with_budget(
            &NewLlmCall {
                call_id: child_call_id.clone(),
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                provider: "script".to_owned(),
                model: "m".to_owned(),
                route: None,
                provider_request_id: None,
            },
            &LlmCallBudgetReservation {
                input_tokens: 10,
                output_tokens: 10,
                cost_nanos_usd: 100,
            },
        )
        .await
        .expect("child call");
        db.finish_llm_call(
            &child_call_id,
            "completed",
            &LlmUsageCompletion {
                input_tokens: Some(10),
                output_tokens: Some(10),
                cache_read_tokens: Some(0),
                cache_create_tokens: Some(0),
                cost_nanos_usd: Some(2_500),
                usage_complete: true,
                error_code: None,
            },
        )
        .await
        .expect("authoritative child usage");
        let child_current = db
            .find_runtime_task_by_id(&child.task.id)
            .await
            .expect("child")
            .expect("child exists");
        db.commit_task_result(&CommitTaskResult {
            task_id: child.task.id.clone(),
            run_id: child.run_id.clone(),
            expected_task_version: child_current.version,
            status: ResultStatus::Partial,
            content: "COST_BUDGET_EXHAUSTED".to_owned(),
            media_type: "text/plain".to_owned(),
            error_code: Some("COST_BUDGET_EXHAUSTED".to_owned()),
            cleanup_status: CleanupStatus::Confirmed,
            verification_status: VerificationStatus::NotRequested,
        })
        .await
        .expect("child overrun terminalizes");

        let budget = db
            .read_task_budget(&root.task.id)
            .await
            .expect("root budget")
            .expect("root exists");
        assert!(budget.usage_complete);
        assert_eq!(budget.reserved_cost_nanos_usd, 2_000);
        assert_eq!(budget.consumed_cost_nanos_usd, 500);
        let exhausted = db
            .start_llm_call_with_budget(
                &NewLlmCall {
                    call_id: id(),
                    task_id: root.task.id.clone(),
                    run_id: root.run_id.clone(),
                    provider: "script".to_owned(),
                    model: "m".to_owned(),
                    route: None,
                    provider_request_id: None,
                },
                &LlmCallBudgetReservation {
                    input_tokens: 1,
                    output_tokens: 1,
                    cost_nanos_usd: 1,
                },
            )
            .await
            .expect_err("known overrun must saturate, not exceed, the root account");
        assert!(exhausted.to_string().contains("COST_BUDGET_EXHAUSTED"));

        let root_current = db
            .find_runtime_task_by_id(&root.task.id)
            .await
            .expect("root")
            .expect("root exists");
        let committed = db
            .commit_task_result(&CommitTaskResult {
                task_id: root.task.id.clone(),
                run_id: root.run_id.clone(),
                expected_task_version: root_current.version,
                status: ResultStatus::Partial,
                content: "COST_BUDGET_EXHAUSTED".to_owned(),
                media_type: "text/plain".to_owned(),
                error_code: Some("COST_BUDGET_EXHAUSTED".to_owned()),
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect("saturated root account must still terminalize");
        assert!(matches!(
            committed,
            CommitTaskResultOutcome::Committed { .. }
        ));
    }

    #[test]
    fn result_exit_reason_uses_only_the_public_lower_camel_contract() {
        let cases = [
            (
                ResultStatus::Complete,
                None,
                crate::run::EXIT_MODEL_FINISHED,
            ),
            (ResultStatus::Partial, None, crate::run::EXIT_MODEL_FINISHED),
            (
                ResultStatus::Error,
                Some("TIMEOUT"),
                crate::run::EXIT_TIMEOUT,
            ),
            (
                ResultStatus::Error,
                Some("MAX_TURNS"),
                crate::run::EXIT_MAX_TURNS,
            ),
            (
                ResultStatus::Error,
                Some("BUDGET_EXHAUSTED"),
                crate::run::EXIT_BUDGET_EXHAUSTED,
            ),
            (
                ResultStatus::Partial,
                Some("TOKEN_BUDGET_EXHAUSTED"),
                crate::run::EXIT_BUDGET_EXHAUSTED,
            ),
            (
                ResultStatus::Partial,
                Some("COST_BUDGET_EXHAUSTED"),
                crate::run::EXIT_BUDGET_EXHAUSTED,
            ),
            (
                ResultStatus::Error,
                Some("PROVIDER_ERROR"),
                crate::run::EXIT_PROVIDER_ERROR,
            ),
            (
                ResultStatus::Error,
                Some("TOOL_ERROR"),
                crate::run::EXIT_TOOL_ERROR,
            ),
            (
                ResultStatus::Cancelled,
                Some("USER_CANCELLED"),
                crate::run::EXIT_USER_CANCELLED,
            ),
            (
                ResultStatus::Cancelled,
                Some("PARENT_CANCELLED"),
                crate::run::EXIT_PARENT_CANCELLED,
            ),
            (
                ResultStatus::Error,
                Some("SERVICE_RESTART"),
                crate::run::EXIT_SERVICE_RESTART,
            ),
            (
                ResultStatus::Error,
                Some("UNCLASSIFIED"),
                crate::run::EXIT_INTERNAL_ERROR,
            ),
        ];
        for (status, error_code, expected) in cases {
            assert_eq!(result_exit_reason(status, error_code), expected);
        }
    }

    #[tokio::test]
    async fn submission_is_atomic_idempotent_and_internal_sessions_are_hidden() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/zk-task-runtime")
            .await
            .expect("session");
        let root_request = root_request(&session.id);
        let root = db
            .create_task_with_run(&root_request)
            .await
            .expect("root task");
        assert!(root.created);
        let child_request = child_request(&session.id, &root);
        let child = db
            .create_task_with_run(&child_request)
            .await
            .expect("child task");
        assert!(child.created);
        assert_eq!(
            child.task.parent_task_id.as_deref(),
            Some(root.task.id.as_str())
        );

        let mut retry = child_request.clone();
        retry.task_id = id();
        retry.run_id = id();
        retry.transcript_session_id = id();
        let replay = db
            .create_task_with_run(&retry)
            .await
            .expect("idempotent retry");
        assert!(!replay.created);
        assert_eq!(replay.task.id, child.task.id);
        assert_eq!(replay.run_id, child.run_id);

        let sessions = db.list_sessions(None, 20).await.expect("sessions");
        assert_eq!(
            sessions.sessions.len(),
            1,
            "internal transcript must stay hidden"
        );
        let tree = db
            .find_task_tree_owned(&session.id)
            .await
            .expect("task tree");
        assert_eq!(tree.len(), 2);

        assert!(db.delete_session(&session.id).await.expect("delete root"));
        let remaining: (i64, i64, i64) = db
            .with_conn_blocking(|conn| {
                Ok((
                    conn.query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))?,
                    conn.query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))?,
                    conn.query_row("SELECT COUNT(*) FROM run_envelopes", [], |row| row.get(0))?,
                ))
            })
            .expect("cascade counts");
        assert_eq!(
            remaining,
            (0, 0, 0),
            "root delete cascades the complete task tree"
        );
    }

    #[tokio::test]
    async fn cancelled_result_preserves_the_requested_parent_exit_reason() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/zk-parent-cancel")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root task");
        let child = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("child task");

        let task_id = child.task.id.clone();
        let run_id = child.run_id.clone();
        db.with_writer(move |connection| {
            let tx = connection.transaction()?;
            let now = format_rfc3339_micros(now_millis());
            assert_eq!(
                tx.execute(
                    "UPDATE tasks SET status='cancelling',cleanup_status='pending',
                        updated_at=?1,version=version+1 WHERE id=?2 AND status='queued'",
                    params![now, task_id],
                )?,
                1
            );
            assert_eq!(
                tx.execute(
                    "UPDATE run_envelopes SET status='cancelling',
                        requested_exit_reason=?1,cleanup_status='pending',updated_at=?2,
                        version=version+1 WHERE id=?3 AND status='queued'",
                    params![crate::run::EXIT_PARENT_CANCELLED, now, run_id],
                )?,
                1
            );
            tx.commit()?;
            Ok(())
        })
        .await
        .expect("request parent cancellation");
        let current = db
            .find_runtime_task_by_id(&child.task.id)
            .await
            .expect("task")
            .expect("child task");

        let committed = db
            .commit_task_result(&CommitTaskResult {
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                expected_task_version: current.version,
                status: ResultStatus::Cancelled,
                content: "cancelled with parent".to_owned(),
                media_type: "text/markdown".to_owned(),
                error_code: Some("USER_CANCELLED".to_owned()),
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect("commit cancelled result");
        assert!(matches!(
            committed,
            CommitTaskResultOutcome::Committed { .. }
        ));
        let run = db
            .find_run_by_id(&child.run_id)
            .await
            .expect("run")
            .expect("child run");
        assert_eq!(
            run.requested_exit_reason.as_deref(),
            Some(crate::run::EXIT_PARENT_CANCELLED)
        );
        assert_eq!(
            run.exit_reason.as_deref(),
            Some(crate::run::EXIT_PARENT_CANCELLED)
        );
    }

    #[tokio::test]
    async fn result_blob_paging_receipt_and_inbox_form_a_durable_closed_loop() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/zk-task-runtime")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root task");
        let child = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("child task");

        let inbox = db
            .enqueue_task_message(
                &session.id,
                &child.task.id,
                Some(&root.task.id),
                "report progress",
            )
            .await
            .expect("enqueue");
        assert_eq!(inbox.status, InboxStatus::Queued);
        assert_eq!(
            db.mark_task_inbox_message(
                &inbox.message_id,
                InboxStatus::Queued,
                InboxStatus::Delivered,
                None,
            )
            .await
            .expect("deliver"),
            CasOutcome::Applied
        );
        assert_eq!(
            db.mark_task_inbox_message(
                &inbox.message_id,
                InboxStatus::Delivered,
                InboxStatus::Consumed,
                None,
            )
            .await
            .expect("consume"),
            CasOutcome::Applied
        );

        let content = "测".repeat(40_000); // >64 KiB, exercises blob-backed content.
        append_final_assistant(&db, &child.task.id, &child.run_id, &content).await;
        let committed = db
            .commit_task_result(&CommitTaskResult {
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                expected_task_version: 0,
                status: ResultStatus::Complete,
                content: content.clone(),
                media_type: "text/markdown".to_owned(),
                error_code: None,
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect("commit");
        let CommitTaskResultOutcome::Committed {
            result,
            limit_exceeded,
        } = committed
        else {
            panic!("expected committed result");
        };
        assert!(!limit_exceeded);
        assert_eq!(result.result_version, 1);

        let first = db
            .read_task_result(&child.task.id, Some(1), 0, 1024)
            .await
            .expect("read")
            .expect("result");
        assert_eq!(first.content.len(), 1023, "page ends at a UTF-8 boundary");
        assert!(first.next_cursor.is_some());
        let latest = db
            .read_task_result(&child.task.id, None, 0, INLINE_RESULT_LIMIT)
            .await
            .expect("read latest")
            .expect("result");
        assert!(latest.content.len() <= INLINE_RESULT_LIMIT);

        let receipt = db
            .insert_task_result_receipt(&root.task.id, &child.task.id, 1, "child done")
            .await
            .expect("receipt");
        assert!(receipt.created);
        let replay = db
            .insert_task_result_receipt(&root.task.id, &child.task.id, 1, "ignored replay")
            .await
            .expect("receipt replay");
        assert!(!replay.created);
        assert_eq!(replay.message_id, receipt.message_id);

        let terminal = db
            .enqueue_task_message(&session.id, &child.task.id, None, "late")
            .await
            .expect_err("terminal target is explicit");
        assert!(terminal.to_string().contains("TASK_TERMINAL:succeeded"));
    }

    #[tokio::test]
    async fn oversized_result_is_explicit_partial_with_a_stable_hard_limit() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/zk-task-result-limit")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root task");
        let content = "x".repeat(RESULT_HARD_LIMIT + 4096);

        let committed = db
            .commit_task_result(&CommitTaskResult {
                task_id: root.task.id.clone(),
                run_id: root.run_id.clone(),
                expected_task_version: root.task.version,
                status: ResultStatus::Complete,
                content,
                media_type: "text/markdown".to_owned(),
                error_code: None,
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect("oversized result must be durably represented");
        let CommitTaskResultOutcome::Committed {
            result,
            limit_exceeded,
        } = committed
        else {
            panic!("expected committed partial result");
        };
        assert!(limit_exceeded);
        assert_eq!(result.status, ResultStatus::Partial);
        assert_eq!(
            result.byte_len,
            i64::try_from(RESULT_HARD_LIMIT).expect("result hard limit fits i64")
        );
        assert_eq!(result.error_code.as_deref(), Some("RESULT_LIMIT_EXCEEDED"));
        assert!(result.final_message_id.is_none());
        let task = db
            .find_runtime_task_by_id(&root.task.id)
            .await
            .expect("task")
            .expect("task exists");
        assert_eq!(task.status, TaskStatus::Partial);
        let first = db
            .read_task_result(&root.task.id, None, 0, INLINE_RESULT_LIMIT)
            .await
            .expect("result")
            .expect("result exists");
        assert_eq!(first.content.len(), INLINE_RESULT_LIMIT);
        assert!(first.partial);
        assert_eq!(first.next_cursor, Some(INLINE_RESULT_LIMIT));
    }

    #[tokio::test]
    async fn task_cas_rejects_wrong_version_and_all_terminal_targets() {
        let db = Db::open_in_memory().expect("db");
        let session = db.create_session("m", "/tmp/cas").await.expect("session");
        let task = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("task");
        assert_eq!(
            db.transition_task_cas(
                &task.task.id,
                7,
                &[TaskStatus::Queued],
                TaskStatus::Running,
                None,
                CleanupStatus::NotRequired,
                VerificationStatus::NotRequested,
            )
            .await
            .expect("cas"),
            CasOutcome::VersionConflict
        );
        for target in [
            TaskStatus::Succeeded,
            TaskStatus::Partial,
            TaskStatus::Failed,
            TaskStatus::Cancelled,
        ] {
            assert_eq!(
                db.transition_task_cas(
                    &task.task.id,
                    0,
                    &[TaskStatus::Queued],
                    target,
                    None,
                    CleanupStatus::Confirmed,
                    VerificationStatus::Passed,
                )
                .await
                .expect("terminal target is rejected as an ordinary CAS outcome"),
                CasOutcome::InvalidTransition
            );
        }

        append_final_assistant(&db, &task.task.id, &task.run_id, "actual output").await;
        let mismatch = db
            .commit_task_result(&CommitTaskResult {
                task_id: task.task.id.clone(),
                run_id: task.run_id.clone(),
                expected_task_version: task.task.version,
                status: ResultStatus::Complete,
                content: "different output".to_owned(),
                media_type: "text/markdown".to_owned(),
                error_code: None,
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect_err("successful result must match its bound Assistant message");
        assert!(
            mismatch
                .to_string()
                .contains("TASK_RESULT_FINAL_MESSAGE_CONTENT_MISMATCH")
        );
    }

    #[tokio::test]
    async fn terminal_trigger_rejects_failed_and_cancelled_without_result_commit() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/terminal-trigger")
            .await
            .expect("session");
        let task = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("task");
        let task_id = task.task.id.clone();
        db.with_writer(move |connection| {
            for status in ["failed", "cancelled"] {
                let error = connection
                    .execute(
                        "UPDATE tasks SET status=?1,terminal_at=?2,updated_at=?2
                         WHERE id=?3",
                        params![status, format_rfc3339_micros(now_millis()), task_id],
                    )
                    .expect_err("a raw terminal Task update must fail closed");
                assert!(
                    error
                        .to_string()
                        .contains("TASK_TERMINAL_REQUIRES_CURRENT_RUN_RESULT"),
                    "unexpected trigger error: {error}"
                );
            }
            Ok(())
        })
        .await
        .expect("trigger assertions");
    }

    #[tokio::test]
    async fn service_restart_cannot_manufacture_a_task_result() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/service-restart-result")
            .await
            .expect("session");
        let created = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("task");
        let shutdown = db
            .request_runtime_shutdown()
            .await
            .expect("shutdown intent");
        assert_eq!(shutdown.tasks_requested, 1);
        assert_eq!(shutdown.runs_requested, 1);

        let current = db
            .find_runtime_task_by_id(&created.task.id)
            .await
            .expect("task query")
            .expect("task");
        let task_id = current.id.clone();
        let run_id = created.run_id.clone();
        let error = db
            .commit_task_result(&CommitTaskResult {
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                expected_task_version: current.version,
                status: ResultStatus::Error,
                content: "server restarted".to_owned(),
                media_type: "text/plain".to_owned(),
                error_code: Some("SERVICE_RESTART".to_owned()),
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect_err("service restart is an interrupted attempt, not a result");
        assert!(
            error
                .to_string()
                .contains("SERVICE_RESTART_REQUIRES_RECONCILIATION")
        );

        db.with_writer(move |connection| {
            let now = format_rfc3339_micros(now_millis());
            let error = connection
                .execute(
                    "INSERT INTO task_results(
                        result_id,task_id,run_id,result_version,status,inline_text,byte_len,
                        content_sha256,media_type,error_code,created_at
                     ) VALUES(?1,?2,?3,1,'error','server restarted',16,?4,
                              'text/plain','SERVICE_RESTART',?5)",
                    params![id(), task_id, run_id, "0".repeat(64), now],
                )
                .expect_err("the schema must also reject a raw restart result");
            assert!(
                error
                    .to_string()
                    .contains("SERVICE_RESTART_REQUIRES_RECONCILIATION")
            );
            Ok(())
        })
        .await
        .expect("schema trigger assertion");
    }

    #[tokio::test]
    async fn terminal_trigger_rejects_wrong_run_and_result_status_mapping() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/terminal-mapping")
            .await
            .expect("session");

        let status_mismatch = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("status-mismatch task");
        let mismatch_task_id = status_mismatch.task.id.clone();
        let mismatch_run_id = status_mismatch.run_id.clone();
        db.with_writer(move |connection| {
            let now = format_rfc3339_micros(now_millis());
            connection.execute(
                "UPDATE run_envelopes SET status='failed',finished_at=?1,terminal_at=?1,
                    exit_reason='internalError',updated_at=?1 WHERE id=?2",
                params![now, mismatch_run_id],
            )?;
            connection.execute(
                "INSERT INTO task_results(
                    result_id,task_id,run_id,result_version,status,inline_text,byte_len,
                    content_sha256,media_type,created_at
                 ) VALUES(?1,?2,?3,1,'cancelled','cancelled',9,?4,'text/plain',?5)",
                params![id(), mismatch_task_id, mismatch_run_id, "0".repeat(64), now],
            )?;
            let error = connection
                .execute(
                    "UPDATE tasks SET status='failed',terminal_at=?1,updated_at=?1
                     WHERE id=?2",
                    params![now, mismatch_task_id],
                )
                .expect_err("wrong ResultStatus must not authorize Task.failed");
            assert!(
                error
                    .to_string()
                    .contains("TASK_TERMINAL_REQUIRES_CURRENT_RUN_RESULT"),
                "unexpected status-mapping error: {error}"
            );
            Ok(())
        })
        .await
        .expect("status mismatch fixture");

        let wrong_run = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("wrong-run task");
        let wrong_task_id = wrong_run.task.id.clone();
        let current_run_id = wrong_run.run_id.clone();
        let result_run_id = id();
        let root_session_id = session.id.clone();
        db.with_writer(move |connection| {
            let now = format_rfc3339_micros(now_millis());
            connection.execute(
                "UPDATE run_envelopes SET status='failed',finished_at=?1,terminal_at=?1,
                    exit_reason='internalError',updated_at=?1 WHERE id=?2",
                params![now, current_run_id],
            )?;
            connection.execute(
                "INSERT INTO run_envelopes(
                    id,session_id,task_id,attempt,startup_epoch,status,version,model,
                    started_at,finished_at,terminal_at,exit_reason,created_at,updated_at
                 ) VALUES(?1,?2,?3,2,1,'failed',0,'m',?4,?4,?4,'internalError',?4,?4)",
                params![result_run_id, root_session_id, wrong_task_id, now],
            )?;
            connection.execute(
                "INSERT INTO task_results(
                    result_id,task_id,run_id,result_version,status,inline_text,byte_len,
                    content_sha256,media_type,error_code,created_at
                 ) VALUES(?1,?2,?3,1,'error','error',5,?4,'text/plain',
                          'INTERNAL_ERROR',?5)",
                params![id(), wrong_task_id, result_run_id, "1".repeat(64), now],
            )?;
            let error = connection
                .execute(
                    "UPDATE tasks SET status='failed',terminal_at=?1,updated_at=?1
                     WHERE id=?2",
                    params![now, wrong_task_id],
                )
                .expect_err("a Result for a non-current Run must not authorize terminal state");
            assert!(
                error
                    .to_string()
                    .contains("TASK_TERMINAL_REQUIRES_CURRENT_RUN_RESULT"),
                "unexpected current-Run mapping error: {error}"
            );
            Ok(())
        })
        .await
        .expect("wrong run fixture");
    }

    #[tokio::test]
    async fn concurrent_child_reservations_preserve_twenty_percent_for_root() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/budget")
            .await
            .expect("session");
        let mut request = root_request(&session.id);
        request.execution_config_json = serde_json::json!({
            "budget": {
                "tokenLimit": 1_000,
                "costLimitNanosUsd": 10_000,
                "deadlineAtMs": now_millis() + 60_000,
            }
        })
        .to_string();
        let root = db
            .create_task_with_run(&request)
            .await
            .expect("root with budget");

        let mut joins = Vec::new();
        for ordinal in 0..5 {
            let db = db.clone();
            let mut child = child_request(&session.id, &root);
            child.ordinal = ordinal;
            child.creator_tool_use_id = Some(format!("budget-tool-{ordinal}"));
            joins.push(tokio::spawn(async move {
                db.create_task_with_run(&child).await
            }));
        }
        let mut created = Vec::new();
        let mut errors = Vec::new();
        for join in joins {
            match join.await.expect("join") {
                Ok(task) => created.push(task),
                Err(error) => errors.push(error.to_string()),
            }
        }
        assert_eq!(created.len(), 4);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("TOKEN_BUDGET_UNAVAILABLE"));
        for child in &created {
            assert_eq!(child.task.token_budget_limit, Some(200));
            assert_eq!(child.task.cost_budget_nanos_usd, Some(2_000));
            assert_eq!(
                child.task.deadline_at_ms,
                request
                    .execution_config_json
                    .parse::<serde_json::Value>()
                    .expect("json")["budget"]["deadlineAtMs"]
                    .as_i64()
            );
        }
        let budget = db
            .read_task_budget(&root.task.id)
            .await
            .expect("budget query")
            .expect("budget");
        assert_eq!(budget.reserved_tokens, 800);
        assert_eq!(budget.reserved_cost_nanos_usd, 8_000);
        assert_eq!(budget.available_tokens, Some(200));
        assert_eq!(budget.available_cost_nanos_usd, Some(2_000));
    }

    #[tokio::test]
    async fn authoritative_usage_settles_and_releases_only_unused_reservation() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/budget-settle")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root");
        assert_eq!(
            db.configure_root_task_budget_cas(
                &root.task.id,
                0,
                &TaskBudgetLimits {
                    token_limit: Some(1_000),
                    cost_limit_nanos_usd: Some(10_000),
                    deadline_at_ms: Some(now_millis() + 60_000),
                },
            )
            .await
            .expect("configure"),
            CasOutcome::Applied
        );
        let child = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("child");
        assert_eq!(
            db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
                .await
                .expect("claim child"),
            CasOutcome::Applied
        );
        let call_id = id();
        db.start_llm_call_with_budget(
            &NewLlmCall {
                call_id: call_id.clone(),
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                provider: "script".to_owned(),
                model: "m".to_owned(),
                route: None,
                provider_request_id: None,
            },
            &LlmCallBudgetReservation {
                input_tokens: 50,
                output_tokens: 30,
                cost_nanos_usd: 750,
            },
        )
        .await
        .expect("call start");
        assert_eq!(
            db.finish_llm_call(
                &call_id,
                "completed",
                &LlmUsageCompletion {
                    input_tokens: Some(50),
                    output_tokens: Some(30),
                    cache_read_tokens: Some(0),
                    cache_create_tokens: Some(0),
                    cost_nanos_usd: Some(750),
                    usage_complete: true,
                    error_code: None,
                },
            )
            .await
            .expect("call finish"),
            CasOutcome::Applied
        );
        append_final_assistant(&db, &child.task.id, &child.run_id, "done").await;
        let committed = db
            .commit_task_result(&CommitTaskResult {
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                expected_task_version: 1,
                status: ResultStatus::Complete,
                content: "done".to_owned(),
                media_type: "text/markdown".to_owned(),
                error_code: None,
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect("commit");
        assert!(matches!(
            committed,
            CommitTaskResultOutcome::Committed { .. }
        ));

        let root_budget = db
            .read_task_budget(&root.task.id)
            .await
            .expect("root budget")
            .expect("root budget row");
        assert_eq!(root_budget.reserved_tokens, 0);
        assert_eq!(root_budget.reserved_cost_nanos_usd, 0);
        assert_eq!(root_budget.consumed_tokens, 80);
        assert_eq!(root_budget.consumed_cost_nanos_usd, 750);
        assert_eq!(root_budget.available_tokens, Some(920));
        let child_budget = db
            .read_task_budget(&child.task.id)
            .await
            .expect("child budget")
            .expect("child budget row");
        let reservation = child_budget.reservation.expect("reservation");
        assert_eq!(reservation.status, BudgetReservationStatus::Settled);
        assert_eq!(reservation.used_tokens, Some(80));
        assert_eq!(reservation.used_cost_nanos_usd, Some(750));
        assert!(reservation.usage_complete);
    }

    #[tokio::test]
    async fn missing_usage_keeps_reservation_charged_and_marks_root_incomplete() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/budget-incomplete")
            .await
            .expect("session");
        let mut request = root_request(&session.id);
        request.execution_config_json = serde_json::json!({
            "budget": {
                "tokenLimit": 1_000,
                "costLimitNanosUsd": 10_000,
                "deadlineAtMs": now_millis() + 60_000,
            }
        })
        .to_string();
        let root = db.create_task_with_run(&request).await.expect("root");
        let child = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("child");
        assert_eq!(
            db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
                .await
                .expect("claim child"),
            CasOutcome::Applied
        );
        let call_id = id();
        db.start_llm_call_with_budget(
            &NewLlmCall {
                call_id: call_id.clone(),
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                provider: "script".to_owned(),
                model: "m".to_owned(),
                route: None,
                provider_request_id: None,
            },
            &LlmCallBudgetReservation {
                input_tokens: 50,
                output_tokens: 30,
                cost_nanos_usd: 750,
            },
        )
        .await
        .expect("call start");
        db.finish_llm_call(
            &call_id,
            "failed",
            &LlmUsageCompletion {
                usage_complete: false,
                error_code: Some("PROVIDER_DISCONNECTED".to_owned()),
                ..LlmUsageCompletion::default()
            },
        )
        .await
        .expect("call failure");
        db.commit_task_result(&CommitTaskResult {
            task_id: child.task.id.clone(),
            run_id: child.run_id.clone(),
            expected_task_version: 1,
            status: ResultStatus::Error,
            content: "provider disconnected".to_owned(),
            media_type: "text/plain".to_owned(),
            error_code: Some("PROVIDER_DISCONNECTED".to_owned()),
            cleanup_status: CleanupStatus::Confirmed,
            verification_status: VerificationStatus::NotRequested,
        })
        .await
        .expect("terminal result");

        let budget = db
            .read_task_budget(&child.task.id)
            .await
            .expect("budget")
            .expect("budget row");
        assert_eq!(budget.reserved_tokens, 200);
        assert_eq!(budget.reserved_cost_nanos_usd, 2_000);
        assert_eq!(budget.consumed_tokens, 0);
        assert!(!budget.usage_complete);
        let reservation = budget.reservation.expect("reservation");
        assert_eq!(reservation.status, BudgetReservationStatus::Incomplete);
        assert_eq!(reservation.used_tokens, None);
        assert!(!reservation.usage_complete);
        let integrity = db
            .assert_llm_usage_complete(&root.task.id, &root.run_id)
            .await
            .expect_err("genuinely missing usage must still poison the root");
        assert!(integrity.to_string().contains("BUDGET_USAGE_INCOMPLETE"));
    }

    #[tokio::test]
    async fn complete_with_terminal_incomplete_call_preserves_child_budget_state() {
        let db = Db::open_in_memory().expect("db");
        let (root, child, call_id) =
            child_with_started_llm_call(&db, "/tmp/terminal-incomplete-complete").await;
        assert_eq!(
            db.finish_llm_call(
                &call_id,
                "failed",
                &LlmUsageCompletion {
                    usage_complete: false,
                    error_code: Some("PROVIDER_DISCONNECTED".to_owned()),
                    ..LlmUsageCompletion::default()
                },
            )
            .await
            .expect("terminal incomplete call"),
            CasOutcome::Applied
        );
        append_final_assistant(&db, &child.task.id, &child.run_id, "done").await;
        let before_task = db
            .find_runtime_task_by_id(&child.task.id)
            .await
            .expect("child task")
            .expect("child exists");
        let before_run = db
            .find_run_by_id(&child.run_id)
            .await
            .expect("child run")
            .expect("run exists");
        let before_root_budget = db
            .read_task_budget(&root.task.id)
            .await
            .expect("root budget")
            .expect("root budget exists");
        let before_child_budget = db
            .read_task_budget(&child.task.id)
            .await
            .expect("child budget")
            .expect("child budget exists");

        let error = db
            .commit_task_result(&CommitTaskResult {
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                expected_task_version: before_task.version,
                status: ResultStatus::Complete,
                content: "done".to_owned(),
                media_type: "text/markdown".to_owned(),
                error_code: None,
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect_err("terminal incomplete usage cannot publish success");
        assert!(matches!(
            error,
            DbError::Invalid(ref code) if code == "BUDGET_USAGE_INCOMPLETE"
        ));

        let after_task = db
            .find_runtime_task_by_id(&child.task.id)
            .await
            .expect("child task")
            .expect("child exists");
        assert_eq!(after_task, before_task);
        assert_eq!(after_task.status, TaskStatus::Running);
        assert!(!after_task.usage_complete);
        let after_run = db
            .find_run_by_id(&child.run_id)
            .await
            .expect("child run")
            .expect("run exists");
        assert_eq!(after_run.status, before_run.status);
        assert_eq!(after_run.version, before_run.version);
        assert!(!after_run.usage_complete);
        assert_eq!(
            db.read_task_budget(&root.task.id)
                .await
                .expect("root budget")
                .expect("root budget exists"),
            before_root_budget
        );
        assert_eq!(
            db.read_task_budget(&child.task.id)
                .await
                .expect("child budget")
                .expect("child budget exists"),
            before_child_budget
        );
        assert_eq!(
            before_child_budget
                .reservation
                .as_ref()
                .expect("reservation")
                .status,
            BudgetReservationStatus::Active
        );
        assert_eq!(llm_call_status(&db, &call_id).await, "failed");
        assert_eq!(task_result_count(&db, &child.task.id).await, 0);
    }

    #[tokio::test]
    async fn sibling_usage_poison_blocks_complete_without_touching_healthy_child() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/sibling-usage-poison")
            .await
            .expect("session");
        let mut request = root_request(&session.id);
        request.execution_config_json = serde_json::json!({
            "budget": {
                "tokenLimit": 1_000,
                "costLimitNanosUsd": 10_000,
                "deadlineAtMs": now_millis() + 60_000,
            }
        })
        .to_string();
        let root = db.create_task_with_run(&request).await.expect("root");
        let first = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("first child");
        let mut second_request = child_request(&session.id, &root);
        second_request.creator_tool_use_id = Some("tool-use-2".to_owned());
        second_request.ordinal = 1;
        let second = db
            .create_task_with_run(&second_request)
            .await
            .expect("second child");
        for child in [&first, &second] {
            assert_eq!(
                db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
                    .await
                    .expect("claim child"),
                CasOutcome::Applied
            );
        }

        let call_id = id();
        db.start_llm_call_with_budget(
            &NewLlmCall {
                call_id: call_id.clone(),
                task_id: first.task.id.clone(),
                run_id: first.run_id.clone(),
                provider: "script".to_owned(),
                model: "m".to_owned(),
                route: None,
                provider_request_id: None,
            },
            &LlmCallBudgetReservation {
                input_tokens: 50,
                output_tokens: 30,
                cost_nanos_usd: 750,
            },
        )
        .await
        .expect("first child call");
        assert_eq!(
            db.finish_llm_call(
                &call_id,
                "failed",
                &LlmUsageCompletion {
                    usage_complete: false,
                    error_code: Some("PROVIDER_DISCONNECTED".to_owned()),
                    ..LlmUsageCompletion::default()
                },
            )
            .await
            .expect("poison root usage"),
            CasOutcome::Applied
        );

        append_final_assistant(&db, &second.task.id, &second.run_id, "done").await;
        let before_second_task = db
            .find_runtime_task_by_id(&second.task.id)
            .await
            .expect("second task")
            .expect("second exists");
        let before_second_run = db
            .find_run_by_id(&second.run_id)
            .await
            .expect("second run")
            .expect("second run exists");
        assert!(before_second_task.usage_complete);
        assert!(before_second_run.usage_complete);
        let before_root_budget = db
            .read_task_budget(&root.task.id)
            .await
            .expect("root budget")
            .expect("root budget exists");
        assert!(!before_root_budget.usage_complete);
        assert_eq!(before_root_budget.reserved_tokens, 400);
        assert_eq!(before_root_budget.reserved_cost_nanos_usd, 4_000);
        let before_second_budget = db
            .read_task_budget(&second.task.id)
            .await
            .expect("second budget")
            .expect("second budget exists");

        let error = db
            .commit_task_result(&CommitTaskResult {
                task_id: second.task.id.clone(),
                run_id: second.run_id.clone(),
                expected_task_version: before_second_task.version,
                status: ResultStatus::Complete,
                content: "done".to_owned(),
                media_type: "text/markdown".to_owned(),
                error_code: None,
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect_err("root usage poison must block sibling success");
        assert!(matches!(
            error,
            DbError::Invalid(ref code) if code == "BUDGET_USAGE_INCOMPLETE"
        ));

        let after_second_task = db
            .find_runtime_task_by_id(&second.task.id)
            .await
            .expect("second task")
            .expect("second exists");
        assert_eq!(after_second_task, before_second_task);
        let after_second_run = db
            .find_run_by_id(&second.run_id)
            .await
            .expect("second run")
            .expect("second run exists");
        assert_eq!(after_second_run.status, before_second_run.status);
        assert_eq!(after_second_run.version, before_second_run.version);
        assert!(after_second_run.usage_complete);
        assert_eq!(
            db.read_task_budget(&root.task.id)
                .await
                .expect("root budget")
                .expect("root budget exists"),
            before_root_budget
        );
        assert_eq!(
            db.read_task_budget(&second.task.id)
                .await
                .expect("second budget")
                .expect("second budget exists"),
            before_second_budget
        );
        assert_eq!(
            before_second_budget
                .reservation
                .as_ref()
                .expect("second reservation")
                .status,
            BudgetReservationStatus::Active
        );
        assert_eq!(task_result_count(&db, &second.task.id).await, 0);
    }

    #[tokio::test]
    async fn failure_with_started_llm_call_keeps_child_reservation_charged() {
        let db = Db::open_in_memory().expect("db");
        let (root, child, call_id) =
            child_with_started_llm_call(&db, "/tmp/started-call-failure").await;
        let current = db
            .find_runtime_task_by_id(&child.task.id)
            .await
            .expect("child task")
            .expect("child exists");

        let committed = db
            .commit_task_result(&CommitTaskResult {
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                expected_task_version: current.version,
                status: ResultStatus::Error,
                content: "observer terminal persistence failed".to_owned(),
                media_type: "text/plain".to_owned(),
                error_code: Some("BUDGET_USAGE_INCOMPLETE".to_owned()),
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect("failure result remains publishable");
        assert!(matches!(
            committed,
            CommitTaskResultOutcome::Committed { .. }
        ));

        let root_budget = db
            .read_task_budget(&root.task.id)
            .await
            .expect("root budget")
            .expect("root budget exists");
        assert_eq!(root_budget.reserved_tokens, 200);
        assert_eq!(root_budget.reserved_cost_nanos_usd, 2_000);
        assert_eq!(root_budget.consumed_tokens, 0);
        assert_eq!(root_budget.consumed_cost_nanos_usd, 0);
        assert!(!root_budget.usage_complete);

        let child_budget = db
            .read_task_budget(&child.task.id)
            .await
            .expect("child budget")
            .expect("child budget exists");
        let reservation = child_budget.reservation.expect("child reservation");
        assert_eq!(reservation.status, BudgetReservationStatus::Incomplete);
        assert_eq!(reservation.used_tokens, None);
        assert_eq!(reservation.used_cost_nanos_usd, None);
        assert!(!reservation.usage_complete);

        let root_task = db
            .find_runtime_task_by_id(&root.task.id)
            .await
            .expect("root task")
            .expect("root exists");
        assert!(!root_task.usage_complete);
        let child_task = db
            .find_runtime_task_by_id(&child.task.id)
            .await
            .expect("child task")
            .expect("child exists");
        assert_eq!(child_task.status, TaskStatus::Failed);
        assert!(!child_task.usage_complete);
        let run = db
            .find_run_by_id(&child.run_id)
            .await
            .expect("run")
            .expect("run exists");
        assert_eq!(run.status, "failed");
        assert!(!run.usage_complete);
        assert_eq!(llm_call_status(&db, &call_id).await, "started");
    }

    #[tokio::test]
    async fn complete_with_started_llm_call_is_rejected_without_partial_writes() {
        let db = Db::open_in_memory().expect("db");
        let (root, child, call_id) =
            child_with_started_llm_call(&db, "/tmp/started-call-complete").await;
        append_final_assistant(&db, &child.task.id, &child.run_id, "done").await;
        let current = db
            .find_runtime_task_by_id(&child.task.id)
            .await
            .expect("child task")
            .expect("child exists");

        let error = db
            .commit_task_result(&CommitTaskResult {
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                expected_task_version: current.version,
                status: ResultStatus::Complete,
                content: "done".to_owned(),
                media_type: "text/markdown".to_owned(),
                error_code: None,
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect_err("success cannot cover an unterminated physical call");
        assert!(matches!(
            error,
            DbError::Invalid(ref code) if code == "BUDGET_USAGE_INCOMPLETE"
        ));

        let child_task = db
            .find_runtime_task_by_id(&child.task.id)
            .await
            .expect("child task")
            .expect("child exists");
        assert_eq!(child_task.status, TaskStatus::Running);
        assert!(child_task.usage_complete);
        assert_eq!(child_task.version, current.version);
        let run = db
            .find_run_by_id(&child.run_id)
            .await
            .expect("run")
            .expect("run exists");
        assert_eq!(run.status, "running");
        assert!(run.usage_complete);
        assert!(run.finished_at.is_none());

        let root_budget = db
            .read_task_budget(&root.task.id)
            .await
            .expect("root budget")
            .expect("root budget exists");
        assert_eq!(root_budget.reserved_tokens, 200);
        assert_eq!(root_budget.reserved_cost_nanos_usd, 2_000);
        assert_eq!(root_budget.consumed_tokens, 0);
        assert_eq!(root_budget.consumed_cost_nanos_usd, 0);
        assert!(root_budget.usage_complete);
        let child_budget = db
            .read_task_budget(&child.task.id)
            .await
            .expect("child budget")
            .expect("child budget exists");
        let reservation = child_budget.reservation.expect("child reservation");
        assert_eq!(reservation.status, BudgetReservationStatus::Active);
        assert!(!reservation.usage_complete);
        assert_eq!(llm_call_status(&db, &call_id).await, "started");

        let task_id = child.task.id.clone();
        let result_count: i64 = db
            .with_reader(move |connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM task_results WHERE task_id=?1",
                        params![task_id],
                        |row| row.get(0),
                    )
                    .map_err(Into::into)
            })
            .await
            .expect("task result count");
        assert_eq!(result_count, 0);
    }

    #[tokio::test]
    async fn safe_boundary_orders_tool_result_before_child_result_message() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/result-order")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root");
        let child = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("child");
        let tool_use_id = child
            .task
            .creator_tool_use_id
            .clone()
            .expect("creator tool");
        // A provider may reuse tool-use IDs across physical Runs. A prior result in the
        // same Session must not satisfy the current invocation's pairing boundary.
        db.append_message(
            &session.id,
            NewMessage {
                role: MessageRole::User,
                content: vec![StoredBlock::ToolResult {
                    tool_use_id: tool_use_id.clone(),
                    content: "stale result from a prior run".to_owned(),
                    is_error: false,
                    metadata: None,
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .expect("stale tool result message");
        db.append_message(
            &session.id,
            NewMessage {
                role: MessageRole::Assistant,
                content: vec![StoredBlock::ToolUse {
                    id: tool_use_id.clone(),
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
        db.create_tool_invocation(&NewToolInvocation {
            invocation_id: invocation_id.clone(),
            task_id: root.task.id.clone(),
            run_id: root.run_id.clone(),
            tool_use_id: tool_use_id.clone(),
            tool_name: "Agent".to_owned(),
            input_json: None,
            side_effect_class: "none".to_owned(),
            directory_generation: None,
            connection_generation: None,
        })
        .await
        .expect("invocation");
        assert_eq!(
            db.transition_tool_invocation_cas(
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
        set_parent_execution_state(
            &db,
            &root.task.id,
            &root.run_id,
            TaskStatus::WaitingDependencies,
        )
        .await;
        commit_complete_child(&db, &child, "child done").await;

        assert!(
            db.ingest_task_result_at_safe_boundary(&root.task.id, &child.task.id, 1, "child done",)
                .await
                .expect("defer while running")
                .is_none()
        );
        assert_eq!(
            db.transition_tool_invocation_cas(
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
        assert!(
            db.ingest_task_result_at_safe_boundary(&root.task.id, &child.task.id, 1, "child done",)
                .await
                .expect("stale result must not satisfy the current invocation")
                .is_none()
        );
        db.append_message(
            &session.id,
            NewMessage {
                role: MessageRole::User,
                content: vec![StoredBlock::ToolResult {
                    tool_use_id: tool_use_id.clone(),
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
        .expect("current tool result message");
        let receipt = db
            .ingest_task_result_at_safe_boundary(&root.task.id, &child.task.id, 1, "child done")
            .await
            .expect("ingest")
            .expect("safe receipt");
        assert!(receipt.created);

        let session_id = session.id.clone();
        let (tool_use_seq, tool_result_seq, task_result_seq, parent_status, run_status) = db
            .with_conn_blocking(move |connection| {
                let tool_use_seq: i64 = connection.query_row(
                    "SELECT message.seq_num FROM messages message,json_each(message.content_json) block
                     WHERE message.session_id=?1 AND json_extract(block.value,'$.type')='tool_use'",
                    params![session_id],
                    |row| row.get(0),
                )?;
                let tool_result_seq: i64 = connection.query_row(
                    "SELECT message.seq_num FROM messages message,json_each(message.content_json) block
                     WHERE message.session_id=?1
                       AND json_extract(block.value,'$.type')='tool_result'
                       AND json_extract(block.value,'$.content')='Agent completed'",
                    params![session_id],
                    |row| row.get(0),
                )?;
                let task_result_seq: i64 = connection.query_row(
                    "SELECT seq_num FROM messages WHERE session_id=?1 AND origin='task_result'",
                    params![session_id],
                    |row| row.get(0),
                )?;
                let parent_status: String = connection.query_row(
                    "SELECT status FROM tasks WHERE id=?1",
                    params![root.task.id],
                    |row| row.get(0),
                )?;
                let run_status: String = connection.query_row(
                    "SELECT status FROM run_envelopes WHERE id=?1",
                    params![root.run_id],
                    |row| row.get(0),
                )?;
                Ok((
                    tool_use_seq,
                    tool_result_seq,
                    task_result_seq,
                    parent_status,
                    run_status,
                ))
            })
            .expect("ordered projection");
        assert!(tool_use_seq < tool_result_seq);
        assert!(tool_result_seq < task_result_seq);
        assert_eq!(parent_status, "running");
        assert_eq!(run_status, "running");
    }

    #[tokio::test]
    async fn concurrent_child_receipts_wake_waiting_parent_exactly_once() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/result-race")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root");
        let first = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("first child");
        let mut second_request = child_request(&session.id, &root);
        second_request.creator_tool_use_id = Some("tool-use-2".to_owned());
        second_request.ordinal = 1;
        let second = db
            .create_task_with_run(&second_request)
            .await
            .expect("second child");
        set_parent_execution_state(
            &db,
            &root.task.id,
            &root.run_id,
            TaskStatus::WaitingDependencies,
        )
        .await;
        commit_complete_child(&db, &first, "first done").await;
        commit_complete_child(&db, &second, "second done").await;

        let first_db = db.clone();
        let first_parent = root.task.id.clone();
        let first_child = first.task.id.clone();
        let first_ingest = tokio::spawn(async move {
            first_db
                .ingest_task_result_at_safe_boundary(&first_parent, &first_child, 1, "first done")
                .await
        });
        let second_db = db.clone();
        let second_parent = root.task.id.clone();
        let second_child = second.task.id.clone();
        let second_ingest = tokio::spawn(async move {
            second_db
                .ingest_task_result_at_safe_boundary(
                    &second_parent,
                    &second_child,
                    1,
                    "second done",
                )
                .await
        });
        assert!(
            first_ingest
                .await
                .expect("first join")
                .expect("first ingest")
                .is_some()
        );
        assert!(
            second_ingest
                .await
                .expect("second join")
                .expect("second ingest")
                .is_some()
        );

        let parent_id = root.task.id.clone();
        let parent_run_id = root.run_id.clone();
        let (receipts, messages, consumed, wake_events, task_status, run_status) = db
            .with_conn_blocking(move |connection| {
                let receipts: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM task_result_receipts WHERE consumer_task_id=?1",
                    params![parent_id],
                    |row| row.get(0),
                )?;
                let messages: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM messages WHERE task_id=?1 AND origin='task_result'",
                    params![parent_id],
                    |row| row.get(0),
                )?;
                let consumed: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM task_dependencies
                     WHERE parent_task_id=?1 AND consumed_result_version=1",
                    params![parent_id],
                    |row| row.get(0),
                )?;
                let wake_events: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM run_event_log
                     WHERE run_id=?1 AND event_type='task_dependencies_resolved'",
                    params![parent_run_id],
                    |row| row.get(0),
                )?;
                let task_status: String = connection.query_row(
                    "SELECT status FROM tasks WHERE id=?1",
                    params![parent_id],
                    |row| row.get(0),
                )?;
                let run_status: String = connection.query_row(
                    "SELECT status FROM run_envelopes WHERE id=?1",
                    params![parent_run_id],
                    |row| row.get(0),
                )?;
                Ok((
                    receipts,
                    messages,
                    consumed,
                    wake_events,
                    task_status,
                    run_status,
                ))
            })
            .expect("race projection");
        assert_eq!(receipts, 2);
        assert_eq!(messages, 2);
        assert_eq!(consumed, 2);
        assert_eq!(wake_events, 1);
        assert_eq!(task_status, "running");
        assert_eq!(run_status, "running");
    }

    #[tokio::test]
    async fn cancelling_parent_does_not_consume_or_wake_late_child_result() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/result-cancel")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&root_request(&session.id))
            .await
            .expect("root");
        let child = db
            .create_task_with_run(&child_request(&session.id, &root))
            .await
            .expect("child");
        set_parent_execution_state(
            &db,
            &root.task.id,
            &root.run_id,
            TaskStatus::WaitingDependencies,
        )
        .await;
        commit_complete_child(&db, &child, "late result is still durable").await;
        set_parent_execution_state(&db, &root.task.id, &root.run_id, TaskStatus::Cancelling).await;
        assert!(
            db.ingest_task_result_at_safe_boundary(
                &root.task.id,
                &child.task.id,
                1,
                "must not be consumed",
            )
            .await
            .expect("inactive parent is a no-op")
            .is_none()
        );

        let parent_id = root.task.id.clone();
        let child_id = child.task.id.clone();
        let (results, receipts, messages, consumed, parent_status) = db
            .with_conn_blocking(move |connection| {
                let results: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM task_results WHERE task_id=?1",
                    params![child_id],
                    |row| row.get(0),
                )?;
                let receipts: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM task_result_receipts WHERE consumer_task_id=?1",
                    params![parent_id],
                    |row| row.get(0),
                )?;
                let messages: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM messages WHERE task_id=?1 AND origin='task_result'",
                    params![parent_id],
                    |row| row.get(0),
                )?;
                let consumed: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM task_dependencies
                     WHERE parent_task_id=?1 AND consumed_result_version IS NOT NULL",
                    params![parent_id],
                    |row| row.get(0),
                )?;
                let parent_status: String = connection.query_row(
                    "SELECT status FROM tasks WHERE id=?1",
                    params![parent_id],
                    |row| row.get(0),
                )?;
                Ok((results, receipts, messages, consumed, parent_status))
            })
            .expect("cancel projection");
        assert_eq!(results, 1, "the child result remains durable");
        assert_eq!(receipts, 0);
        assert_eq!(messages, 0);
        assert_eq!(consumed, 0);
        assert_eq!(parent_status, "cancelling");
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskResultReceiptRecord {
    pub receipt_id: String,
    pub consumer_task_id: String,
    pub producer_task_id: String,
    pub result_version: i64,
    pub message_id: String,
    pub result_sha256: String,
    pub created_at: String,
    pub created: bool,
}

impl Db {
    /// Test-only primitive for the result/blob repository test. Production callers must
    /// use `ingest_task_result_at_safe_boundary`, which also enforces parent execution
    /// ordering and the single-wakeup invariant.
    #[cfg(test)]
    async fn insert_task_result_receipt(
        &self,
        consumer_task_id: &str,
        producer_task_id: &str,
        result_version: i64,
        summary: &str,
    ) -> Result<TaskResultReceiptRecord, DbError> {
        let consumer_task_id = consumer_task_id.to_owned();
        let producer_task_id = producer_task_id.to_owned();
        let summary = summary.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            if let Some(existing) = tx
                .query_row(
                    "SELECT receipt_id,consumer_task_id,producer_task_id,result_version,
                            message_id,result_sha256,created_at
                     FROM task_result_receipts
                     WHERE consumer_task_id=?1 AND producer_task_id=?2 AND result_version=?3",
                    params![consumer_task_id, producer_task_id, result_version],
                    |row| {
                        Ok(TaskResultReceiptRecord {
                            receipt_id: row.get(0)?,
                            consumer_task_id: row.get(1)?,
                            producer_task_id: row.get(2)?,
                            result_version: row.get(3)?,
                            message_id: row.get(4)?,
                            result_sha256: row.get(5)?,
                            created_at: row.get(6)?,
                            created: false,
                        })
                    },
                )
                .optional()?
            {
                tx.commit()?;
                return Ok(existing);
            }
            let dependency: i64 = tx.query_row(
                "SELECT COUNT(*) FROM task_dependencies
                 WHERE parent_task_id=?1 AND child_task_id=?2",
                params![consumer_task_id, producer_task_id],
                |row| row.get(0),
            )?;
            if dependency != 1 {
                return Err(DbError::Invalid("TASK_RESULT_NOT_OWNED".to_owned()));
            }
            let (result_sha256, producer_status): (String, String) = tx
                .query_row(
                    "SELECT content_sha256,status FROM task_results
                     WHERE task_id=?1 AND result_version=?2",
                    params![producer_task_id, result_version],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .ok_or_else(|| DbError::Invalid("TASK_RESULT_NOT_FOUND".to_owned()))?;
            let (session_id, run_id): (String, Option<String>) = tx.query_row(
                "SELECT session_id,current_run_id FROM tasks WHERE id=?1",
                params![consumer_task_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let message_id = uuid::Uuid::new_v4().to_string();
            let receipt_id = uuid::Uuid::new_v4().to_string();
            let now = format_rfc3339_micros(now_millis());
            let seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(seq_num),0)+1 FROM messages WHERE session_id=?1",
                params![session_id],
                |row| row.get(0),
            )?;
            let content_json = serde_json::to_string(&vec![StoredBlock::Text {
                text: format!(
                    "<task-result taskId=\"{producer_task_id}\" resultVersion=\"{result_version}\" status=\"{producer_status}\" sha256=\"{result_sha256}\">\n{summary}\n</task-result>"
                ),
            }])?;
            tx.execute(
                "INSERT INTO messages
                    (id,session_id,role,content_json,input_tokens,output_tokens,task_id,run_id,
                     origin,source_task_id,created_at,seq_num)
                 VALUES(?1,?2,'user',?3,0,0,?4,?5,'task_result',?6,?7,?8)",
                params![
                    message_id,
                    session_id,
                    content_json,
                    consumer_task_id,
                    run_id,
                    producer_task_id,
                    now,
                    seq,
                ],
            )?;
            tx.execute(
                "INSERT INTO task_result_receipts
                    (receipt_id,consumer_task_id,producer_task_id,result_version,message_id,
                     result_sha256,created_at)
                 VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    receipt_id,
                    consumer_task_id,
                    producer_task_id,
                    result_version,
                    message_id,
                    result_sha256,
                    now,
                ],
            )?;
            tx.execute(
                "UPDATE task_dependencies SET consumed_result_version=?1,updated_at=?2
                 WHERE parent_task_id=?3 AND child_task_id=?4",
                params![result_version, now, consumer_task_id, producer_task_id],
            )?;
            tx.execute(
                "UPDATE sessions SET updated_at=?1 WHERE id=?2",
                params![now, session_id],
            )?;
            tx.commit()?;
            Ok(TaskResultReceiptRecord {
                receipt_id,
                consumer_task_id,
                producer_task_id,
                result_version,
                message_id,
                result_sha256,
                created_at: now,
                created: true,
            })
        })
        .await
    }

    /// At the parent execution safe boundary, atomically appends the child result
    /// message, records its exactly-once receipt, and wakes the parent only when every
    /// attached dependency is terminal and consumed.
    ///
    /// `Ok(None)` is a normal deferred/no-op outcome: the parent still has an active or
    /// not-yet-paired tool invocation, is not currently waiting for dependencies, or is
    /// cancelling/needs-attention/terminal. The caller may retry only while the parent
    /// remains `waitingDependencies`.
    pub async fn ingest_task_result_at_safe_boundary(
        &self,
        consumer_task_id: &str,
        producer_task_id: &str,
        result_version: i64,
        summary: &str,
    ) -> Result<Option<TaskResultReceiptRecord>, DbError> {
        let consumer_task_id = consumer_task_id.to_owned();
        let producer_task_id = producer_task_id.to_owned();
        let summary = summary.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            if let Some(existing) = tx
                .query_row(
                    "SELECT receipt_id,consumer_task_id,producer_task_id,result_version,
                            message_id,result_sha256,created_at
                     FROM task_result_receipts
                     WHERE consumer_task_id=?1 AND producer_task_id=?2 AND result_version=?3",
                    params![consumer_task_id, producer_task_id, result_version],
                    |row| {
                        Ok(TaskResultReceiptRecord {
                            receipt_id: row.get(0)?,
                            consumer_task_id: row.get(1)?,
                            producer_task_id: row.get(2)?,
                            result_version: row.get(3)?,
                            message_id: row.get(4)?,
                            result_sha256: row.get(5)?,
                            created_at: row.get(6)?,
                            created: false,
                        })
                    },
                )
                .optional()?
            {
                tx.commit()?;
                return Ok(Some(existing));
            }

            let parent: Option<(String, Option<String>, String, i64)> = tx
                .query_row(
                    "SELECT session_id,current_run_id,status,version FROM tasks WHERE id=?1",
                    params![consumer_task_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let Some((session_id, parent_run_id, parent_status, parent_version)) = parent else {
                return Err(DbError::Invalid("PARENT_TASK_NOT_FOUND".to_owned()));
            };
            if parent_status != TaskStatus::WaitingDependencies.as_db() {
                tx.commit()?;
                return Ok(None);
            }
            let Some(parent_run_id) = parent_run_id else {
                return Err(DbError::Invalid("PARENT_RUN_NOT_FOUND".to_owned()));
            };

            // Tool invocations become terminal only after their durable tool_result in the
            // normal execution path. The message check additionally fails closed around
            // denial/error paths that may transition their invocation first.
            let open_invocations: i64 = tx.query_row(
                "SELECT COUNT(*) FROM tool_invocations
                 WHERE run_id=?1 AND status IN ('preparing','queued','running')",
                params![parent_run_id],
                |row| row.get(0),
            )?;
            if open_invocations != 0 {
                tx.commit()?;
                return Ok(None);
            }
            let unpaired_invocations: i64 = tx.query_row(
                "SELECT COUNT(*) FROM tool_invocations invocation
                 WHERE invocation.run_id=?1 AND NOT EXISTS(
                     SELECT 1
                     FROM messages result_message,json_each(result_message.content_json) result_block
                     WHERE result_message.session_id=?2 AND result_message.role='user'
                       AND json_extract(result_block.value,'$.type')='tool_result'
                       AND json_extract(result_block.value,'$.tool_use_id')=invocation.tool_use_id
                       AND result_message.seq_num>(
                           SELECT MAX(use_message.seq_num)
                           FROM messages use_message,json_each(use_message.content_json) use_block
                           WHERE use_message.session_id=?2 AND use_message.role='assistant'
                             AND json_extract(use_block.value,'$.type')='tool_use'
                             AND json_extract(use_block.value,'$.id')=invocation.tool_use_id
                       )
                 )",
                params![parent_run_id, session_id],
                |row| row.get(0),
            )?;
            if unpaired_invocations != 0 {
                tx.commit()?;
                return Ok(None);
            }
            let pending_tool_postprocessing: i64 = tx.query_row(
                "SELECT COUNT(*) FROM tool_result_postprocessing
                 WHERE run_id=?1 AND status='pending'",
                params![parent_run_id],
                |row| row.get(0),
            )?;
            if pending_tool_postprocessing != 0 {
                tx.commit()?;
                return Ok(None);
            }

            let dependency: i64 = tx.query_row(
                "SELECT COUNT(*) FROM task_dependencies
                 WHERE parent_task_id=?1 AND child_task_id=?2
                   AND lifecycle_policy='attached'",
                params![consumer_task_id, producer_task_id],
                |row| row.get(0),
            )?;
            if dependency != 1 {
                return Err(DbError::Invalid("TASK_RESULT_NOT_OWNED".to_owned()));
            }
            let (result_sha256, producer_status): (String, String) = tx
                .query_row(
                    "SELECT content_sha256,status FROM task_results
                     WHERE task_id=?1 AND result_version=?2",
                    params![producer_task_id, result_version],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .ok_or_else(|| DbError::Invalid("TASK_RESULT_NOT_FOUND".to_owned()))?;
            let producer_terminal: i64 = tx.query_row(
                "SELECT COUNT(*) FROM tasks WHERE id=?1
                 AND status IN ('succeeded','partial','failed','cancelled')",
                params![producer_task_id],
                |row| row.get(0),
            )?;
            if producer_terminal != 1 {
                return Err(DbError::Invalid("TASK_RESULT_PRODUCER_NOT_TERMINAL".to_owned()));
            }

            let message_id = uuid::Uuid::new_v4().to_string();
            let receipt_id = uuid::Uuid::new_v4().to_string();
            let now = format_rfc3339_micros(now_millis());
            let seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(seq_num),0)+1 FROM messages WHERE session_id=?1",
                params![session_id],
                |row| row.get(0),
            )?;
            let content_json = serde_json::to_string(&vec![StoredBlock::Text {
                text: format!(
                    "<task-result taskId=\"{producer_task_id}\" resultVersion=\"{result_version}\" status=\"{producer_status}\" sha256=\"{result_sha256}\">\n{summary}\n</task-result>"
                ),
            }])?;
            tx.execute(
                "INSERT INTO messages
                    (id,session_id,role,content_json,input_tokens,output_tokens,task_id,run_id,
                     origin,source_task_id,created_at,seq_num)
                 VALUES(?1,?2,'user',?3,0,0,?4,?5,'task_result',?6,?7,?8)",
                params![
                    message_id,
                    session_id,
                    content_json,
                    consumer_task_id,
                    parent_run_id,
                    producer_task_id,
                    now,
                    seq,
                ],
            )?;
            tx.execute(
                "INSERT INTO task_result_receipts
                    (receipt_id,consumer_task_id,producer_task_id,result_version,message_id,
                     result_sha256,created_at)
                 VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    receipt_id,
                    consumer_task_id,
                    producer_task_id,
                    result_version,
                    message_id,
                    result_sha256,
                    now,
                ],
            )?;
            tx.execute(
                "UPDATE task_dependencies SET consumed_result_version=?1,updated_at=?2
                 WHERE parent_task_id=?3 AND child_task_id=?4",
                params![result_version, now, consumer_task_id, producer_task_id],
            )?;
            tx.execute(
                "UPDATE sessions SET updated_at=?1 WHERE id=?2",
                params![now, session_id],
            )?;

            let unresolved: i64 = tx.query_row(
                "SELECT COUNT(*) FROM task_dependencies dependency
                 JOIN tasks child ON child.id=dependency.child_task_id
                 WHERE dependency.parent_task_id=?1
                   AND dependency.lifecycle_policy='attached'
                   AND (child.status NOT IN ('succeeded','partial','failed','cancelled')
                        OR dependency.consumed_result_version IS NULL)",
                params![consumer_task_id],
                |row| row.get(0),
            )?;
            if unresolved == 0 {
                let task_updated = tx.execute(
                    "UPDATE tasks SET status='running',reason='attachedChildrenResolved',
                        updated_at=?1,version=version+1
                     WHERE id=?2 AND version=?3 AND status='waitingDependencies'",
                    params![now, consumer_task_id, parent_version],
                )?;
                if task_updated != 1 {
                    return Err(DbError::Invalid("PARENT_TASK_WAKE_CONFLICT".to_owned()));
                }
                let run_updated = tx.execute(
                    "UPDATE run_envelopes SET status='running',waiting_reason=NULL,
                        updated_at=?1,version=version+1
                     WHERE id=?2 AND task_id=?3 AND status='waitingDependencies'",
                    params![now, parent_run_id, consumer_task_id],
                )?;
                if run_updated != 1 {
                    return Err(DbError::Invalid(
                        "PARENT_TASK_RUN_WAKE_MISMATCH".to_owned(),
                    ));
                }
                crate::run::append_event_in_current_write(
                    &tx,
                    &parent_run_id,
                    "task_dependencies_resolved",
                    None,
                    &serde_json::json!({
                        "taskId": consumer_task_id,
                        "sourceTaskId": producer_task_id,
                        "resultVersion": result_version,
                    }),
                )?;
            }
            tx.commit()?;
            Ok(Some(TaskResultReceiptRecord {
                receipt_id,
                consumer_task_id,
                producer_task_id,
                result_version,
                message_id,
                result_sha256,
                created_at: now,
                created: true,
            }))
        })
        .await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InboxStatus {
    Queued,
    Delivered,
    Consumed,
    Rejected,
}

impl InboxStatus {
    #[must_use]
    pub const fn as_db(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Delivered => "delivered",
            Self::Consumed => "consumed",
            Self::Rejected => "rejected",
        }
    }

    fn parse(value: &str) -> Result<Self, DbError> {
        match value {
            "queued" => Ok(Self::Queued),
            "delivered" => Ok(Self::Delivered),
            "consumed" => Ok(Self::Consumed),
            "rejected" => Ok(Self::Rejected),
            other => Err(DbError::Invalid(format!("INBOX_STATUS_CORRUPT:{other}"))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskInboxMessage {
    pub message_id: String,
    pub task_id: String,
    pub target_run_id: Option<String>,
    pub sender_task_id: Option<String>,
    pub content: String,
    pub status: InboxStatus,
    pub delivery_generation: i64,
    pub created_at: String,
    pub delivered_at: Option<String>,
    pub consumed_at: Option<String>,
    pub rejection_reason: Option<String>,
}

fn map_inbox_row(row: &Row<'_>) -> rusqlite::Result<TaskInboxMessage> {
    let status: String = row.get(5)?;
    Ok(TaskInboxMessage {
        message_id: row.get(0)?,
        task_id: row.get(1)?,
        target_run_id: row.get(2)?,
        sender_task_id: row.get(3)?,
        content: row.get(4)?,
        status: InboxStatus::parse(&status).map_err(invalid_to_sql_error)?,
        delivery_generation: row.get(6)?,
        created_at: row.get(7)?,
        delivered_at: row.get(8)?,
        consumed_at: row.get(9)?,
        rejection_reason: row.get(10)?,
    })
}

const INBOX_COLUMNS: &str = "message_id,task_id,target_run_id,sender_task_id,content,status,
    delivery_generation,created_at,delivered_at,consumed_at,rejection_reason";

impl Db {
    /// Enqueues a durable message after verifying both tasks belong to the caller's root
    /// session. Terminal targets return an explicit terminal error, never agent-not-found.
    pub async fn enqueue_task_message(
        &self,
        root_session_id: &str,
        target_task_id: &str,
        sender_task_id: Option<&str>,
        content: &str,
    ) -> Result<TaskInboxMessage, DbError> {
        if content.trim().is_empty() {
            return Err(DbError::Invalid("TASK_MESSAGE_EMPTY".to_owned()));
        }
        let root_session_id = root_session_id.to_owned();
        let target_task_id = target_task_id.to_owned();
        let sender_task_id = sender_task_id.map(str::to_owned);
        let content = content.to_owned();
        self.with_writer(move |conn| {
            let (status, target_run_id): (String, Option<String>) = conn
                .query_row(
                    "SELECT status,current_run_id FROM tasks WHERE id=?1 AND session_id=?2",
                    params![target_task_id, root_session_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .ok_or_else(|| DbError::Invalid("TASK_NOT_FOUND_OR_NOT_OWNED".to_owned()))?;
            if TaskStatus::parse(&status)?.is_terminal() {
                return Err(DbError::Invalid(format!("TASK_TERMINAL:{status}")));
            }
            if let Some(sender) = sender_task_id.as_deref() {
                let owned: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM tasks WHERE id=?1 AND session_id=?2",
                    params![sender, root_session_id],
                    |row| row.get(0),
                )?;
                if owned != 1 {
                    return Err(DbError::Invalid("SENDER_TASK_NOT_OWNED".to_owned()));
                }
            }
            let message_id = uuid::Uuid::new_v4().to_string();
            let now = format_rfc3339_micros(now_millis());
            conn.execute(
                "INSERT INTO task_inbox_messages
                    (message_id,task_id,target_run_id,sender_task_id,content,status,created_at)
                 VALUES(?1,?2,?3,?4,?5,'queued',?6)",
                params![
                    message_id,
                    target_task_id,
                    target_run_id,
                    sender_task_id,
                    content,
                    now
                ],
            )?;
            let sql =
                format!("SELECT {INBOX_COLUMNS} FROM task_inbox_messages WHERE message_id=?1");
            conn.query_row(&sql, params![message_id], map_inbox_row)
                .map_err(Into::into)
        })
        .await
    }

    pub async fn read_task_inbox(
        &self,
        task_id: &str,
        statuses: &[InboxStatus],
        limit: usize,
    ) -> Result<Vec<TaskInboxMessage>, DbError> {
        if limit == 0 || limit > 1000 {
            return Err(DbError::Invalid("INBOX_LIMIT_INVALID".to_owned()));
        }
        let limit_i64 =
            i64::try_from(limit).map_err(|_| DbError::Invalid("INBOX_LIMIT_INVALID".to_owned()))?;
        let task_id = task_id.to_owned();
        let statuses = statuses.iter().map(|s| s.as_db()).collect::<Vec<_>>();
        self.with_reader(move |conn| {
            let mut sql =
                format!("SELECT {INBOX_COLUMNS} FROM task_inbox_messages WHERE task_id=?1");
            if !statuses.is_empty() {
                let quoted = statuses
                    .iter()
                    .map(|status| format!("'{status}'"))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = write!(sql, " AND status IN ({quoted})");
            }
            sql.push_str(" ORDER BY created_at,message_id LIMIT ?2");
            let mut stmt = conn.prepare(&sql)?;
            Ok(stmt
                .query_map(params![task_id, limit_i64], map_inbox_row)?
                .collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    /// CAS transition for the inbox delivery lifecycle.
    pub async fn mark_task_inbox_message(
        &self,
        message_id: &str,
        expected: InboxStatus,
        target: InboxStatus,
        rejection_reason: Option<&str>,
    ) -> Result<CasOutcome, DbError> {
        self.mark_task_inbox_message_scoped(None, message_id, expected, target, rejection_reason)
            .await
    }

    /// CAS transition scoped to the owning Task.  This is the executor-facing
    /// form: ownership is checked by the same indexed statement that reads and
    /// updates the row, so delivery never depends on a bounded inbox listing.
    pub async fn mark_task_inbox_message_for_task(
        &self,
        task_id: &str,
        message_id: &str,
        expected: InboxStatus,
        target: InboxStatus,
        rejection_reason: Option<&str>,
    ) -> Result<CasOutcome, DbError> {
        self.mark_task_inbox_message_scoped(
            Some(task_id),
            message_id,
            expected,
            target,
            rejection_reason,
        )
        .await
    }

    async fn mark_task_inbox_message_scoped(
        &self,
        task_id: Option<&str>,
        message_id: &str,
        expected: InboxStatus,
        target: InboxStatus,
        rejection_reason: Option<&str>,
    ) -> Result<CasOutcome, DbError> {
        let allowed = matches!(
            (expected, target),
            (
                InboxStatus::Queued,
                InboxStatus::Delivered | InboxStatus::Rejected,
            ) | (
                InboxStatus::Delivered,
                InboxStatus::Consumed | InboxStatus::Rejected,
            )
        );
        if !allowed || (target == InboxStatus::Rejected) != rejection_reason.is_some() {
            return Err(DbError::Invalid("INBOX_TRANSITION_INVALID".to_owned()));
        }
        let task_id = task_id.map(str::to_owned);
        let message_id = message_id.to_owned();
        let rejection_reason = rejection_reason.map(str::to_owned);
        self.with_writer(move |conn| {
            let exists: Option<String> = if let Some(task_id) = task_id.as_deref() {
                conn.query_row(
                    "SELECT status FROM task_inbox_messages
                     WHERE message_id=?1 AND task_id=?2",
                    params![message_id, task_id],
                    |row| row.get(0),
                )
                .optional()?
            } else {
                conn.query_row(
                    "SELECT status FROM task_inbox_messages WHERE message_id=?1",
                    params![message_id],
                    |row| row.get(0),
                )
                .optional()?
            };
            let Some(current) = exists else {
                return Ok(CasOutcome::NotFound);
            };
            if current != expected.as_db() {
                return Ok(CasOutcome::VersionConflict);
            }
            let now = format_rfc3339_micros(now_millis());
            let changed = if let Some(task_id) = task_id.as_deref() {
                conn.execute(
                    "UPDATE task_inbox_messages SET status=?1,
                        delivered_at=CASE WHEN ?1='delivered' THEN ?2 ELSE delivered_at END,
                        consumed_at=CASE WHEN ?1='consumed' THEN ?2 ELSE consumed_at END,
                        rejection_reason=?3,
                        delivery_generation=delivery_generation+CASE WHEN ?1='delivered' THEN 1 ELSE 0 END
                     WHERE message_id=?4 AND task_id=?5 AND status=?6",
                    params![
                        target.as_db(),
                        now,
                        rejection_reason,
                        message_id,
                        task_id,
                        expected.as_db()
                    ],
                )?
            } else {
                conn.execute(
                    "UPDATE task_inbox_messages SET status=?1,
                        delivered_at=CASE WHEN ?1='delivered' THEN ?2 ELSE delivered_at END,
                        consumed_at=CASE WHEN ?1='consumed' THEN ?2 ELSE consumed_at END,
                        rejection_reason=?3,
                        delivery_generation=delivery_generation+CASE WHEN ?1='delivered' THEN 1 ELSE 0 END
                     WHERE message_id=?4 AND status=?5",
                    params![target.as_db(), now, rejection_reason, message_id, expected.as_db()],
                )?
            };
            Ok(if changed == 1 {
                CasOutcome::Applied
            } else {
                CasOutcome::VersionConflict
            })
        })
        .await
    }
}

fn map_submission_conflict(
    conn: &Connection,
    request: &CreateTaskWithRun,
    error: rusqlite::Error,
) -> DbError {
    if matches!(
        error,
        rusqlite::Error::SqliteFailure(ref details, _) if details.extended_code == 1555 || details.extended_code == 2067
    ) {
        let existing: Option<(String, String)> = conn
            .query_row(
                "SELECT session_id,description FROM tasks WHERE id=?1",
                params![request.task_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .ok()
            .flatten();
        if existing.is_some() {
            return DbError::Invalid("TASK_IDEMPOTENCY_CONFLICT".to_owned());
        }
    }
    DbError::Sqlite(error)
}

#[derive(Clone, Debug)]
pub struct CommitTaskResult {
    pub task_id: String,
    pub run_id: String,
    pub expected_task_version: i64,
    pub status: ResultStatus,
    pub content: String,
    pub media_type: String,
    pub error_code: Option<String>,
    pub cleanup_status: CleanupStatus,
    pub verification_status: VerificationStatus,
}

/// Aggregate usage observed by an Engine that is not backed by the production
/// physical-call observer. It is used only when a Run has no `llm_calls` rows;
/// an existing physical ledger always remains authoritative.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunUsageFallback {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_create_tokens: i64,
    pub cost_nanos_usd: i64,
    pub usage_complete: bool,
}

impl RunUsageFallback {
    fn validate(self) -> Result<(), DbError> {
        if self.input_tokens < 0
            || self.output_tokens < 0
            || self.cache_read_tokens < 0
            || self.cache_create_tokens < 0
            || self.cost_nanos_usd < 0
        {
            return Err(DbError::Invalid("RUN_USAGE_NEGATIVE".to_owned()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskResultRecord {
    pub result_id: String,
    pub task_id: String,
    pub run_id: String,
    pub result_version: i64,
    pub status: ResultStatus,
    pub byte_len: i64,
    pub content_sha256: String,
    pub media_type: String,
    pub error_code: Option<String>,
    pub final_message_id: Option<String>,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskResultChunk {
    pub result: TaskResultRecord,
    pub content: String,
    pub cursor: usize,
    pub next_cursor: Option<usize>,
    pub partial: bool,
}

#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)] // public outcome keeps the committed record directly accessible
pub enum CommitTaskResultOutcome {
    Committed {
        result: TaskResultRecord,
        limit_exceeded: bool,
    },
    VersionConflict,
    InvalidRun,
    AlreadyTerminal,
    NotFound,
}

fn map_result_row(row: &Row<'_>) -> rusqlite::Result<TaskResultRecord> {
    let status: String = row.get(4)?;
    Ok(TaskResultRecord {
        result_id: row.get(0)?,
        task_id: row.get(1)?,
        run_id: row.get(2)?,
        result_version: row.get(3)?,
        status: ResultStatus::parse(&status).map_err(invalid_to_sql_error)?,
        byte_len: row.get(5)?,
        content_sha256: row.get(6)?,
        media_type: row.get(7)?,
        error_code: row.get(8)?,
        final_message_id: row.get(9)?,
        created_at: row.get(10)?,
    })
}

fn truncate_utf8_at_limit(input: &str, limit: usize) -> &str {
    if input.len() <= limit {
        return input;
    }
    let mut end = limit;
    while !input.is_char_boundary(end) {
        end -= 1;
    }
    &input[..end]
}

fn result_exit_reason(status: ResultStatus, error_code: Option<&str>) -> &'static str {
    match error_code {
        Some("TIMEOUT" | "SUBAGENT_DEADLINE_EXCEEDED") => crate::run::EXIT_TIMEOUT,
        Some("MAX_TURNS") => crate::run::EXIT_MAX_TURNS,
        Some("BUDGET_EXHAUSTED" | "TOKEN_BUDGET_EXHAUSTED" | "COST_BUDGET_EXHAUSTED") => {
            crate::run::EXIT_BUDGET_EXHAUSTED
        }
        Some("PROVIDER_ERROR") => crate::run::EXIT_PROVIDER_ERROR,
        Some("TOOL_ERROR" | "TOOL_RETURNED_ERROR") => crate::run::EXIT_TOOL_ERROR,
        Some("USER_CANCELLED") => crate::run::EXIT_USER_CANCELLED,
        Some("PARENT_CANCELLED") => crate::run::EXIT_PARENT_CANCELLED,
        Some("SERVICE_RESTART") => crate::run::EXIT_SERVICE_RESTART,
        _ => match status {
            ResultStatus::Complete | ResultStatus::Partial => crate::run::EXIT_MODEL_FINISHED,
            ResultStatus::Cancelled => crate::run::EXIT_USER_CANCELLED,
            ResultStatus::Error => crate::run::EXIT_INTERNAL_ERROR,
        },
    }
}

impl Db {
    /// Commits Run terminal state, immutable `TaskResult`, Task terminal state, and durable
    /// event in one transaction. Inputs over 16 MiB are explicitly persisted as partial
    /// with `RESULT_LIMIT_EXCEEDED`; the returned flag prevents silent truncation.
    pub async fn commit_task_result(
        &self,
        request: &CommitTaskResult,
    ) -> Result<CommitTaskResultOutcome, DbError> {
        self.commit_task_result_inner(request, None).await
    }

    /// Commit a terminal result with an aggregate Run-usage fallback.
    ///
    /// The fallback supports provider implementations that expose stream usage
    /// but do not execute the physical-call observer. When any `llm_calls` row
    /// exists for the Run, its accumulated projection is authoritative and the
    /// fallback is ignored. In both cases the Run's final direct usage is added
    /// to its Session exactly once in the same transaction that terminalizes
    /// the Task, so retries cannot double-charge the Session.
    pub async fn commit_task_result_with_run_usage_fallback(
        &self,
        request: &CommitTaskResult,
        fallback: RunUsageFallback,
    ) -> Result<CommitTaskResultOutcome, DbError> {
        fallback.validate()?;
        self.commit_task_result_inner(request, Some(fallback)).await
    }

    async fn commit_task_result_inner(
        &self,
        request: &CommitTaskResult,
        usage_fallback: Option<RunUsageFallback>,
    ) -> Result<CommitTaskResultOutcome, DbError> {
        let request = request.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let task: Option<(String, i64, Option<String>, String)> = tx
                .query_row(
                    "SELECT status,version,current_run_id,verification_status
                     FROM tasks WHERE id=?1",
                    params![request.task_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let Some((task_status, version, current_run_id, persisted_verification)) = task else {
                return Ok(CommitTaskResultOutcome::NotFound);
            };
            if TaskStatus::parse(&task_status)?.is_terminal() {
                return Ok(CommitTaskResultOutcome::AlreadyTerminal);
            }
            if version != request.expected_task_version {
                return Ok(CommitTaskResultOutcome::VersionConflict);
            }
            if current_run_id.as_deref() != Some(request.run_id.as_str()) {
                return Ok(CommitTaskResultOutcome::InvalidRun);
            }
            let run_state: Option<(String, Option<String>, String)> = tx
                .query_row(
                    "SELECT status,requested_exit_reason,session_id FROM run_envelopes
                     WHERE id=?1 AND task_id=?2",
                    params![request.run_id, request.task_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            let Some((run_status, requested_exit_reason, run_session_id)) = run_state else {
                return Ok(CommitTaskResultOutcome::InvalidRun);
            };
            // A service restart is not a logical Task failure.  Once the
            // ordered shutdown transaction has claimed this Run, only startup
            // reconciliation may move the attempt to `interrupted` and the
            // Task to `needsAttention`; no executor may invent a result while
            // cleanup ownership is still being drained.
            if requested_exit_reason.as_deref() == Some(crate::run::EXIT_SERVICE_RESTART) {
                return Err(DbError::Invalid(
                    "SERVICE_RESTART_REQUIRES_RECONCILIATION".to_owned(),
                ));
            }
            if matches!(
                run_status.as_str(),
                "completed" | "failed" | "cancelled" | "interrupted"
            ) {
                return Ok(CommitTaskResultOutcome::AlreadyTerminal);
            }
            let pending_tool_postprocessing: i64 = tx.query_row(
                "SELECT COUNT(*) FROM tool_result_postprocessing
                 WHERE run_id=?1 AND status='pending'",
                params![request.run_id],
                |row| row.get(0),
            )?;
            if pending_tool_postprocessing != 0 {
                return Err(DbError::Invalid(
                    "TASK_TERMINAL_HAS_PENDING_TOOL_POSTPROCESSING".to_owned(),
                ));
            }
            let effective_verification = if request.verification_status
                == VerificationStatus::NotRequested
            {
                VerificationStatus::parse(&persisted_verification)?
            } else {
                request.verification_status
            };

            let limit_exceeded = request.content.len() > RESULT_HARD_LIMIT;
            let persisted_content = truncate_utf8_at_limit(&request.content, RESULT_HARD_LIMIT);
            let effective_status = if limit_exceeded {
                ResultStatus::Partial
            } else {
                request.status
            };
            let error_code = if limit_exceeded {
                Some("RESULT_LIMIT_EXCEEDED")
            } else {
                request.error_code.as_deref()
            };

            // A physical call that is still `started` has no durable terminal
            // usage record. Failure results remain publishable, but their
            // enclosing Run is made usage-incomplete in this same transaction
            // before budget settlement so a child reservation cannot be
            // released as settled. Success is gated below after any fallback
            // has been projected into the authoritative Run row.
            let started_llm_calls: i64 = tx.query_row(
                "SELECT COUNT(*) FROM llm_calls
                 WHERE run_id=?1 AND task_id=?2 AND status='started'",
                params![request.run_id, request.task_id],
                |row| row.get(0),
            )?;

            // The physical-call ledger is the primary source of Run usage. A
            // fallback is admitted only when no physical call was registered,
            // which covers narrow/direct ChatProvider implementations without
            // overwriting retries, fallbacks or summaries observed in production.
            if let Some(fallback) = usage_fallback {
                let physical_calls: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM llm_calls WHERE run_id=?1",
                    params![request.run_id],
                    |row| row.get(0),
                )?;
                if physical_calls == 0 {
                    tx.execute(
                        "UPDATE run_envelopes SET input_tokens=?1,output_tokens=?2,
                            cache_read_tokens=?3,cache_create_tokens=?4,cost_nanos_usd=?5,
                            total_tokens=?1+?2,total_cost_usd=?5/1000000000.0,
                            usage_complete=?6,updated_at=?7,version=version+1
                         WHERE id=?8 AND task_id=?9",
                        params![
                            fallback.input_tokens,
                            fallback.output_tokens,
                            fallback.cache_read_tokens,
                            fallback.cache_create_tokens,
                            fallback.cost_nanos_usd,
                            fallback.usage_complete,
                            format_rfc3339_micros(now_millis()),
                            request.run_id,
                            request.task_id,
                        ],
                    )?;
                }
            }

            // A successful logical result may only be published when all three
            // durable usage authorities agree after the optional fallback has
            // been applied. Reading the transaction state (rather than trusting
            // the caller's fallback value) also catches a terminal incomplete
            // call and a root poisoned by a sibling Run. A still-started call is
            // independently incomplete even though startup reconciliation has
            // not yet projected that fact onto the Run row.
            if effective_status == ResultStatus::Complete {
                let usage_integrity: Option<(bool, bool, bool)> = tx
                    .query_row(
                        "SELECT run.usage_complete,task.usage_complete,root.usage_complete
                         FROM run_envelopes run
                         JOIN tasks task
                           ON task.id=run.task_id AND task.current_run_id=run.id
                         JOIN tasks root ON root.id=task.root_task_id
                         WHERE run.id=?1 AND task.id=?2",
                        params![request.run_id, request.task_id],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)? != 0,
                                row.get::<_, i64>(1)? != 0,
                                row.get::<_, i64>(2)? != 0,
                            ))
                        },
                    )
                    .optional()?;
                let Some((run_complete, task_complete, root_complete)) = usage_integrity else {
                    return Ok(CommitTaskResultOutcome::InvalidRun);
                };
                if started_llm_calls != 0
                    || !run_complete
                    || ((!task_complete || !root_complete)
                        && !crate::runtime_ledger::timeout_usage_exception(&tx, &request.run_id)?)
                {
                    return Err(DbError::Invalid("BUDGET_USAGE_INCOMPLETE".to_owned()));
                }
            }
            let final_message: Option<(String, String)> = tx
                .query_row(
                    "SELECT id,content_json FROM messages
                     WHERE task_id=?1 AND run_id=?2 AND role='assistant'
                     ORDER BY seq_num DESC LIMIT 1",
                    params![request.task_id, request.run_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let final_message_id = final_message.as_ref().map(|(id, _)| id.clone());
            if effective_status == ResultStatus::Complete && final_message_id.is_none() {
                return Err(DbError::Invalid(
                    "TASK_SUCCESS_REQUIRES_FINAL_ASSISTANT".to_owned(),
                ));
            }
            if effective_status == ResultStatus::Complete
                && let Some((_, content_json)) = final_message.as_ref()
            {
                let assistant_text = parse_blocks(content_json)
                    .iter()
                    .filter_map(|block| match block {
                        StoredBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                if assistant_text != persisted_content {
                    return Err(DbError::Invalid(
                        "TASK_RESULT_FINAL_MESSAGE_CONTENT_MISMATCH".to_owned(),
                    ));
                }
            }
            let bytes = persisted_content.as_bytes();
            let byte_len = i64::try_from(bytes.len())
                .map_err(|_| DbError::Invalid("RESULT_LIMIT_EXCEEDED".to_owned()))?;
            let digest = sha256_hex(bytes);
            let now = format_rfc3339_micros(now_millis());
            if started_llm_calls != 0 {
                let updated = tx.execute(
                    "UPDATE run_envelopes SET usage_complete=0,updated_at=?1
                     WHERE id=?2 AND task_id=?3",
                    params![now, request.run_id, request.task_id],
                )?;
                if updated != 1 {
                    return Ok(CommitTaskResultOutcome::InvalidRun);
                }
            }
            let result_version: i64 = tx.query_row(
                "SELECT COALESCE(MAX(result_version),0)+1 FROM task_results WHERE task_id=?1",
                params![request.task_id],
                |row| row.get(0),
            )?;
            let result_id = uuid::Uuid::new_v4().to_string();
            let (inline, blob) = if bytes.len() <= INLINE_RESULT_LIMIT {
                (Some(persisted_content), None)
            } else {
                tx.execute(
                    "INSERT INTO task_result_blobs(sha256,payload,byte_len,created_at)
                     VALUES(?1,?2,?3,?4) ON CONFLICT(sha256) DO NOTHING",
                    params![
                        digest,
                        bytes,
                        byte_len,
                        now
                    ],
                )?;
                (None, Some(digest.as_str()))
            };
            tx.execute(
                "INSERT INTO task_results
                    (result_id,task_id,run_id,result_version,status,inline_text,blob_sha256,
                     byte_len,content_sha256,media_type,error_code,final_message_id,created_at)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
                params![
                    result_id,
                    request.task_id,
                    request.run_id,
                    result_version,
                    effective_status.as_db(),
                    inline,
                    blob,
                    byte_len,
                    digest,
                    request.media_type,
                    error_code,
                    final_message_id,
                    now,
                ],
            )?;

            let (run_target, task_target) = match effective_status {
                ResultStatus::Complete => ("completed", TaskStatus::Succeeded),
                ResultStatus::Partial => ("completed", TaskStatus::Partial),
                ResultStatus::Error => ("failed", TaskStatus::Failed),
                ResultStatus::Cancelled => ("cancelled", TaskStatus::Cancelled),
            };
            // Cancellation intent is recorded before the executor is signalled. Preserve
            // that structural reason when the cancellation completes, including the
            // partial-result representation used when cleanup cannot be confirmed.
            let preserve_requested_reason = effective_status == ResultStatus::Cancelled
                || (effective_status == ResultStatus::Partial
                    && error_code == Some("CLEANUP_UNCONFIRMED"));
            let exit_reason = if preserve_requested_reason {
                requested_exit_reason
                    .as_deref()
                    .unwrap_or_else(|| result_exit_reason(effective_status, error_code))
            } else {
                result_exit_reason(effective_status, error_code)
            };
            let cancellation_reason = matches!(exit_reason, "userCancelled" | "parentCancelled")
                .then_some(exit_reason);
            let run_error_summary =
                (effective_status == ResultStatus::Error).then_some(persisted_content);

            // Project this Run's direct usage to its own Session once. The
            // enclosing Task non-terminal/version guard makes the additive
            // update idempotent together with terminal result creation.
            let direct_usage: (i64, i64, i64, i64, i64) = tx.query_row(
                "SELECT input_tokens,output_tokens,cache_read_tokens,cache_create_tokens,
                        cost_nanos_usd
                   FROM run_envelopes WHERE id=?1 AND task_id=?2",
                params![request.run_id, request.task_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )?;
            let session_updated = tx.execute(
                "UPDATE sessions SET
                    total_input_tokens=total_input_tokens+?1,
                    total_output_tokens=total_output_tokens+?2,
                    total_cache_read=total_cache_read+?3,
                    total_cache_create=total_cache_create+?4,
                    total_cost_usd=total_cost_usd+(?5/1000000000.0),updated_at=?6
                 WHERE id=?7",
                params![
                    direct_usage.0,
                    direct_usage.1,
                    direct_usage.2,
                    direct_usage.3,
                    direct_usage.4,
                    format_rfc3339_micros(now_millis()),
                    run_session_id,
                ],
            )?;
            if session_updated != 1 {
                return Err(DbError::Invalid("RUN_SESSION_NOT_FOUND".to_owned()));
            }
            settle_task_budget_in_current_write(
                &tx,
                &request.task_id,
                &request.run_id,
                &now,
            )?;
            tx.execute(
                "UPDATE run_envelopes SET status=?1,finished_at=?2,terminal_at=?2,
                    exit_reason=?3,requested_exit_reason=COALESCE(requested_exit_reason,?4),
                    abort_reason=COALESCE(abort_reason,?5),error_summary=?6,
                    cleanup_status=?7,verification_status=?8,updated_at=?2,version=version+1
                 WHERE id=?9 AND task_id=?10
                   AND status IN ('queued','running','waitingDependencies','waitingInteraction','cancelling')",
                params![
                    run_target,
                    now,
                    exit_reason,
                    cancellation_reason,
                    cancellation_reason,
                    run_error_summary,
                    request.cleanup_status.as_db(),
                    effective_verification.as_db(),
                    request.run_id,
                    request.task_id,
                ],
            )?;
            let updated = tx.execute(
                "UPDATE tasks SET status=?1,reason=?2,cleanup_status=?3,
                    verification_status=?4,terminal_at=?5,updated_at=?5,version=version+1
                 WHERE id=?6 AND version=?7 AND status=?8",
                params![
                    task_target.as_db(),
                    exit_reason,
                    request.cleanup_status.as_db(),
                    effective_verification.as_db(),
                    now,
                    request.task_id,
                    request.expected_task_version,
                    task_status,
                ],
            )?;
            if updated != 1 {
                return Ok(CommitTaskResultOutcome::VersionConflict);
            }
            let next_seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(seq),-1)+1 FROM run_event_log WHERE run_id=?1",
                params![request.run_id],
                |row| row.get(0),
            )?;
            tx.execute(
                "INSERT INTO run_event_log(run_id,seq,event_type,event_data,ts)
                 VALUES(?1,?2,'task_result_available',?3,?4)",
                params![
                    request.run_id,
                    next_seq,
                    serde_json::json!({
                        "protocolVersion": 4,
                        "taskId": request.task_id,
                        "runId": request.run_id,
                        "resultVersion": result_version,
                        "resultStatus": effective_status,
                        "contentSha256": digest,
                        "partial": effective_status == ResultStatus::Partial,
                    })
                    .to_string(),
                    now_millis(),
                ],
            )?;
            let result = TaskResultRecord {
                result_id,
                task_id: request.task_id,
                run_id: request.run_id,
                result_version,
                status: effective_status,
                byte_len,
                content_sha256: digest,
                media_type: request.media_type,
                error_code: error_code.map(str::to_owned),
                final_message_id,
                created_at: now,
            };
            tx.commit()?;
            Ok(CommitTaskResultOutcome::Committed {
                result,
                limit_exceeded,
            })
        })
        .await
    }

    /// Reads one immutable result version in bounded UTF-8 chunks. `result_version=None`
    /// selects the latest version; reads never create consumption receipts.
    pub async fn read_task_result(
        &self,
        task_id: &str,
        result_version: Option<i64>,
        cursor: usize,
        max_bytes: usize,
    ) -> Result<Option<TaskResultChunk>, DbError> {
        if max_bytes == 0 || max_bytes > INLINE_RESULT_LIMIT {
            return Err(DbError::Invalid("RESULT_PAGE_SIZE_INVALID".to_owned()));
        }
        let task_id = task_id.to_owned();
        self.with_reader(move |conn| {
            let sql = if result_version.is_some() {
                "SELECT tr.result_id,tr.task_id,tr.run_id,tr.result_version,tr.status,tr.byte_len,
                        tr.content_sha256,tr.media_type,tr.error_code,tr.final_message_id,tr.created_at,
                        COALESCE(tr.inline_text,CAST(tb.payload AS TEXT))
                 FROM task_results tr LEFT JOIN task_result_blobs tb ON tb.sha256=tr.blob_sha256
                 WHERE tr.task_id=?1 AND tr.result_version=?2"
            } else {
                "SELECT tr.result_id,tr.task_id,tr.run_id,tr.result_version,tr.status,tr.byte_len,
                        tr.content_sha256,tr.media_type,tr.error_code,tr.final_message_id,tr.created_at,
                        COALESCE(tr.inline_text,CAST(tb.payload AS TEXT))
                 FROM task_results tr LEFT JOIN task_result_blobs tb ON tb.sha256=tr.blob_sha256
                 WHERE tr.task_id=?1 ORDER BY tr.result_version DESC LIMIT 1"
            };
            let row: Option<(TaskResultRecord, String)> = if let Some(version) = result_version {
                conn.query_row(sql, params![task_id, version], |row| {
                    Ok((map_result_row(row)?, row.get(11)?))
                })
                .optional()?
            } else {
                conn.query_row(sql, params![task_id], |row| {
                    Ok((map_result_row(row)?, row.get(11)?))
                })
                .optional()?
            };
            let Some((result, content)) = row else {
                return Ok(None);
            };
            if cursor > content.len() || !content.is_char_boundary(cursor) {
                return Err(DbError::Invalid("RESULT_CURSOR_INVALID".to_owned()));
            }
            let mut end = cursor.saturating_add(max_bytes).min(content.len());
            while end > cursor && !content.is_char_boundary(end) {
                end -= 1;
            }
            if end == cursor && cursor < content.len() {
                end = content[cursor..]
                    .char_indices()
                    .nth(1)
                    .map_or(content.len(), |(offset, _)| cursor + offset);
            }
            Ok(Some(TaskResultChunk {
                partial: result.status == ResultStatus::Partial,
                result,
                content: content[cursor..end].to_owned(),
                cursor,
                next_cursor: (end < content.len()).then_some(end),
            }))
        })
        .await
    }
}
