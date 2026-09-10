//! Durable physical-execution ledgers used by the unified `TaskRuntime`.
#![allow(missing_docs, clippy::missing_errors_doc, clippy::too_many_lines)]
// Records intentionally mirror the self-describing SQL columns.

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::message::insert_message_in_current_write;
use crate::task_runtime::CasOutcome;
use crate::time::{format_rfc3339_micros, now_millis};
use crate::{
    CleanupStatus, Db, DbError, MessageAttribution, MessageRecord, MessageRole, NewMessage,
    StoredBlock,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ToolInvocationStatus {
    Preparing,
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
}

impl ToolInvocationStatus {
    #[must_use]
    pub const fn as_db(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }

    const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

#[derive(Clone, Debug)]
pub struct NewToolInvocation {
    pub invocation_id: String,
    pub task_id: String,
    pub run_id: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub input_json: Option<String>,
    pub side_effect_class: String,
    pub directory_generation: Option<i64>,
    pub connection_generation: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolInvocationRecord {
    pub invocation_id: String,
    pub task_id: String,
    pub run_id: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub status: String,
    pub input_json: Option<String>,
    pub output_ref: Option<String>,
    pub error_code: Option<String>,
    pub side_effect_class: String,
    pub cleanup_status: String,
    pub directory_generation: Option<i64>,
    pub connection_generation: Option<i64>,
    pub version: i64,
    pub started_at: Option<String>,
    pub terminal_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Immutable terminal payload committed together with one invocation CAS.
#[derive(Clone, Debug)]
pub struct CommitToolInvocationResult {
    pub invocation_id: String,
    pub expected_version: i64,
    pub session_id: String,
    pub target: ToolInvocationStatus,
    pub input_json: Option<String>,
    pub content: String,
    pub is_error: bool,
    pub metadata: Option<Value>,
    pub output_sha256: Option<String>,
    pub error_code: Option<String>,
    pub cleanup_status: CleanupStatus,
    /// Replayable receipt payload for Artifact/Research/Evidence projections.
    /// `None` means this result has no required durable post-processing.
    pub postprocessing: Option<Value>,
}

/// Both durable facts returned only after their shared transaction commits.
#[derive(Clone, Debug)]
pub struct CommittedToolInvocationResult {
    pub invocation: ToolInvocationRecord,
    pub message: MessageRecord,
}

/// CAS outcome for an invocation/result terminal transaction.
#[derive(Clone, Debug)]
pub enum CommitToolInvocationResultOutcome {
    Committed(Box<CommittedToolInvocationResult>),
    VersionConflict,
    InvalidTransition,
    NotFound,
}

#[derive(Clone, Debug)]
struct CurrentToolInvocation {
    status: String,
    version: i64,
    task_id: String,
    run_id: String,
    tool_use_id: String,
    input_json: Option<String>,
    session_id: String,
}

const MAX_TOOL_POSTPROCESSING_BYTES: usize = 256 * 1024;

const TOOL_COLUMNS: &str = "invocation_id,task_id,run_id,tool_use_id,tool_name,status,
    input_json,output_ref,error_code,side_effect_class,cleanup_status,directory_generation,
    connection_generation,version,started_at,terminal_at,created_at,updated_at";

fn map_tool(row: &Row<'_>) -> rusqlite::Result<ToolInvocationRecord> {
    Ok(ToolInvocationRecord {
        invocation_id: row.get(0)?,
        task_id: row.get(1)?,
        run_id: row.get(2)?,
        tool_use_id: row.get(3)?,
        tool_name: row.get(4)?,
        status: row.get(5)?,
        input_json: row.get(6)?,
        output_ref: row.get(7)?,
        error_code: row.get(8)?,
        side_effect_class: row.get(9)?,
        cleanup_status: row.get(10)?,
        directory_generation: row.get(11)?,
        connection_generation: row.get(12)?,
        version: row.get(13)?,
        started_at: row.get(14)?,
        terminal_at: row.get(15)?,
        created_at: row.get(16)?,
        updated_at: row.get(17)?,
    })
}

impl Db {
    pub async fn create_tool_invocation(
        &self,
        record: &NewToolInvocation,
    ) -> Result<ToolInvocationRecord, DbError> {
        let record = record.clone();
        if !matches!(
            record.side_effect_class.as_str(),
            "none" | "read" | "write" | "unknown"
        ) {
            return Err(DbError::Invalid(
                "TOOL_SIDE_EFFECT_CLASS_INVALID".to_owned(),
            ));
        }
        if let Some(input) = record.input_json.as_deref() {
            serde_json::from_str::<serde_json::Value>(input)?;
        }
        self.with_writer(move |conn| {
            let owned: i64 = conn.query_row(
                "SELECT COUNT(*) FROM run_envelopes WHERE id=?1 AND task_id=?2",
                params![record.run_id, record.task_id],
                |row| row.get(0),
            )?;
            if owned != 1 {
                return Err(DbError::Invalid("TOOL_RUN_NOT_OWNED".to_owned()));
            }
            let now = format_rfc3339_micros(now_millis());
            conn.execute(
                "INSERT INTO tool_invocations
                    (invocation_id,task_id,run_id,tool_use_id,tool_name,status,input_json,
                     side_effect_class,cleanup_status,directory_generation,connection_generation,
                     created_at,updated_at)
                 VALUES(?1,?2,?3,?4,?5,'preparing',?6,?7,'notRequired',?8,?9,?10,?10)",
                params![
                    record.invocation_id,
                    record.task_id,
                    record.run_id,
                    record.tool_use_id,
                    record.tool_name,
                    record.input_json,
                    record.side_effect_class,
                    record.directory_generation,
                    record.connection_generation,
                    now,
                ],
            )?;
            let sql = format!("SELECT {TOOL_COLUMNS} FROM tool_invocations WHERE invocation_id=?1");
            conn.query_row(&sql, params![record.invocation_id], map_tool)
                .map_err(Into::into)
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn transition_tool_invocation_cas(
        &self,
        invocation_id: &str,
        expected_version: i64,
        target: ToolInvocationStatus,
        input_json: Option<&str>,
        output_ref: Option<&str>,
        error_code: Option<&str>,
        cleanup_status: CleanupStatus,
    ) -> Result<CasOutcome, DbError> {
        if matches!(
            target,
            ToolInvocationStatus::Queued
                | ToolInvocationStatus::Running
                | ToolInvocationStatus::Succeeded
        ) && input_json.is_none()
        {
            return Err(DbError::Invalid("TOOL_INPUT_REQUIRED".to_owned()));
        }
        if let Some(input) = input_json {
            serde_json::from_str::<serde_json::Value>(input)?;
        }
        let invocation_id = invocation_id.to_owned();
        let input_json = input_json.map(str::to_owned);
        let output_ref = output_ref.map(str::to_owned);
        let error_code = error_code.map(str::to_owned);
        self.with_writer(move |conn| {
            let current: Option<(String, i64)> = conn
                .query_row(
                    "SELECT status,version FROM tool_invocations WHERE invocation_id=?1",
                    params![invocation_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((status, version)) = current else {
                return Ok(CasOutcome::NotFound);
            };
            if matches!(
                status.as_str(),
                "succeeded" | "failed" | "cancelled" | "interrupted"
            ) {
                return Ok(CasOutcome::InvalidTransition);
            }
            if version != expected_version {
                return Ok(CasOutcome::VersionConflict);
            }
            let now = format_rfc3339_micros(now_millis());
            let terminal_at = target.is_terminal().then_some(now.as_str());
            let started_at = matches!(
                target,
                ToolInvocationStatus::Running | ToolInvocationStatus::Succeeded
            )
            .then_some(now.as_str());
            let changed = conn.execute(
                "UPDATE tool_invocations SET status=?1,input_json=COALESCE(?2,input_json),
                    output_ref=?3,error_code=?4,cleanup_status=?5,
                    started_at=COALESCE(started_at,?6),terminal_at=?7,updated_at=?8,
                    version=version+1 WHERE invocation_id=?9 AND version=?10",
                params![
                    target.as_db(),
                    input_json,
                    output_ref,
                    error_code,
                    cleanup_status.as_db(),
                    started_at,
                    terminal_at,
                    now,
                    invocation_id,
                    expected_version,
                ],
            )?;
            Ok(if changed == 1 {
                CasOutcome::Applied
            } else {
                CasOutcome::VersionConflict
            })
        })
        .await
    }

    /// Atomically append the attributed `tool_result` and move its physical
    /// invocation to a terminal state. Neither row is observable when message
    /// allocation, attribution validation, or the terminal CAS fails.
    pub async fn commit_tool_invocation_result(
        &self,
        request: &CommitToolInvocationResult,
    ) -> Result<CommitToolInvocationResultOutcome, DbError> {
        if !request.target.is_terminal() {
            return Err(DbError::Invalid(
                "TOOL_RESULT_TARGET_MUST_BE_TERMINAL".to_owned(),
            ));
        }
        if request.expected_version < 0 {
            return Err(DbError::Invalid(
                "TOOL_INVOCATION_VERSION_INVALID".to_owned(),
            ));
        }
        if let Some(input) = request.input_json.as_deref() {
            serde_json::from_str::<Value>(input)?;
        }
        if let Some(hash) = request.output_sha256.as_deref()
            && (hash.len() != 64
                || !hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
        {
            return Err(DbError::Invalid("TOOL_RESULT_SHA256_INVALID".to_owned()));
        }
        let postprocessing_json = request
            .postprocessing
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        if postprocessing_json
            .as_ref()
            .is_some_and(|payload| payload.len() > MAX_TOOL_POSTPROCESSING_BYTES)
        {
            return Err(DbError::Invalid(
                "TOOL_POSTPROCESSING_PAYLOAD_TOO_LARGE".to_owned(),
            ));
        }

        let request = request.clone();
        let message_id = uuid::Uuid::new_v4().to_string();
        self.with_writer(move |connection| {
            let tx = connection.transaction()?;
            let current: Option<CurrentToolInvocation> = tx
                .query_row(
                    "SELECT invocation.status,invocation.version,invocation.task_id,
                            invocation.run_id,invocation.tool_use_id,invocation.input_json,
                            run.session_id
                     FROM tool_invocations invocation
                     JOIN run_envelopes run
                       ON run.id=invocation.run_id AND run.task_id=invocation.task_id
                     WHERE invocation.invocation_id=?1",
                    params![request.invocation_id],
                    |row| {
                        Ok(CurrentToolInvocation {
                            status: row.get(0)?,
                            version: row.get(1)?,
                            task_id: row.get(2)?,
                            run_id: row.get(3)?,
                            tool_use_id: row.get(4)?,
                            input_json: row.get(5)?,
                            session_id: row.get(6)?,
                        })
                    },
                )
                .optional()?;
            let Some(current) = current else {
                return Ok(CommitToolInvocationResultOutcome::NotFound);
            };
            if matches!(
                current.status.as_str(),
                "succeeded" | "failed" | "cancelled" | "interrupted"
            ) {
                return Ok(CommitToolInvocationResultOutcome::InvalidTransition);
            }
            if current.version != request.expected_version {
                return Ok(CommitToolInvocationResultOutcome::VersionConflict);
            }
            if current.session_id != request.session_id {
                return Err(DbError::Invalid("TOOL_RESULT_SESSION_NOT_OWNED".to_owned()));
            }
            if request.target == ToolInvocationStatus::Succeeded
                && request.input_json.is_none()
                && current.input_json.is_none()
            {
                return Err(DbError::Invalid("TOOL_INPUT_REQUIRED".to_owned()));
            }

            let message = NewMessage {
                role: MessageRole::User,
                content: vec![StoredBlock::ToolResult {
                    tool_use_id: current.tool_use_id.clone(),
                    content: request.content.clone(),
                    is_error: request.is_error,
                    metadata: request.metadata.clone(),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            };
            let inserted = insert_message_in_current_write(
                &tx,
                &message_id,
                &request.session_id,
                &message,
                &MessageAttribution {
                    task_id: Some(current.task_id.clone()),
                    run_id: Some(current.run_id.clone()),
                    origin: "tool_result".to_owned(),
                    source_task_id: None,
                },
            )?
            .ok_or_else(|| DbError::Invalid(format!("MESSAGE_ID_COLLISION:{message_id}")))?;

            let output_ref = request.output_sha256.as_ref().map_or_else(
                || format!("message:{message_id}"),
                |hash| format!("message:{message_id}#sha256:{hash}"),
            );
            let now = format_rfc3339_micros(now_millis());
            let started_at =
                (request.target == ToolInvocationStatus::Succeeded).then_some(now.as_str());
            let changed = tx.execute(
                "UPDATE tool_invocations
                 SET status=?1,input_json=COALESCE(?2,input_json),output_ref=?3,error_code=?4,
                     cleanup_status=?5,started_at=COALESCE(started_at,?6),terminal_at=?7,
                     updated_at=?7,version=version+1
                 WHERE invocation_id=?8 AND version=?9
                   AND status NOT IN ('succeeded','failed','cancelled','interrupted')",
                params![
                    request.target.as_db(),
                    request.input_json,
                    output_ref,
                    request.error_code,
                    request.cleanup_status.as_db(),
                    started_at,
                    now,
                    request.invocation_id,
                    request.expected_version,
                ],
            )?;
            if changed != 1 {
                return Ok(CommitToolInvocationResultOutcome::VersionConflict);
            }
            if let Some(payload_json) = postprocessing_json {
                tx.execute(
                    "INSERT INTO tool_result_postprocessing
                        (invocation_id,task_id,run_id,result_message_id,payload_json,status,
                         version,created_at,updated_at)
                     VALUES(?1,?2,?3,?4,?5,'pending',0,?6,?6)",
                    params![
                        request.invocation_id,
                        current.task_id,
                        current.run_id,
                        message_id,
                        payload_json,
                        now,
                    ],
                )?;
            }
            let sql = format!("SELECT {TOOL_COLUMNS} FROM tool_invocations WHERE invocation_id=?1");
            let invocation = tx.query_row(&sql, params![request.invocation_id], map_tool)?;
            tx.commit()?;
            Ok(CommitToolInvocationResultOutcome::Committed(Box::new(
                CommittedToolInvocationResult {
                    invocation,
                    message: inserted,
                },
            )))
        })
        .await
    }

    /// Mark all required Artifact/Research/Evidence projections for an invocation
    /// durable. The invocation and result message are already immutable; this
    /// orthogonal CAS is the only operation which opens the parent safe boundary.
    pub async fn complete_tool_result_postprocessing_cas(
        &self,
        invocation_id: &str,
        expected_version: i64,
    ) -> Result<CasOutcome, DbError> {
        if expected_version < 0 {
            return Err(DbError::Invalid(
                "TOOL_POSTPROCESSING_VERSION_INVALID".to_owned(),
            ));
        }
        let invocation_id = invocation_id.to_owned();
        self.with_writer(move |connection| {
            let current: Option<(String, i64)> = connection
                .query_row(
                    "SELECT status,version FROM tool_result_postprocessing
                     WHERE invocation_id=?1",
                    params![invocation_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((status, version)) = current else {
                return Ok(CasOutcome::NotFound);
            };
            if status == "completed" {
                return Ok(CasOutcome::InvalidTransition);
            }
            if version != expected_version {
                return Ok(CasOutcome::VersionConflict);
            }
            let now = format_rfc3339_micros(now_millis());
            let changed = connection.execute(
                "UPDATE tool_result_postprocessing
                 SET status='completed',version=version+1,updated_at=?1,completed_at=?1
                 WHERE invocation_id=?2 AND status='pending' AND version=?3",
                params![now, invocation_id, expected_version],
            )?;
            Ok(if changed == 1 {
                CasOutcome::Applied
            } else {
                CasOutcome::VersionConflict
            })
        })
        .await
    }

    /// Atomically enter physical execution only while the invocation still
    /// belongs to the current executable Task/Run attempt and all three usage
    /// authorities (Run, owning Task, root Task) remain complete. An attached
    /// child moves its parent to `waitingDependencies` before sibling calls in
    /// the same prepared batch have necessarily crossed this boundary, so that
    /// state remains executable for those already-owned invocations.
    ///
    /// This stricter transition is used by external execution surfaces (for
    /// example reverse MCP) which do not own the Run driver. It closes the
    /// check/start race: a concurrent Task stop or terminal Run wins before
    /// any side effect can be spawned.
    pub async fn start_tool_invocation_for_active_run_cas(
        &self,
        invocation_id: &str,
        expected_version: i64,
        input_json: &str,
        side_effect_class: &str,
    ) -> Result<CasOutcome, DbError> {
        serde_json::from_str::<serde_json::Value>(input_json)?;
        if !matches!(side_effect_class, "none" | "read" | "write" | "unknown") {
            return Err(DbError::Invalid(
                "TOOL_SIDE_EFFECT_CLASS_INVALID".to_owned(),
            ));
        }
        let invocation_id = invocation_id.to_owned();
        let input_json = input_json.to_owned();
        let side_effect_class = side_effect_class.to_owned();
        self.with_writer(move |conn| {
            let current: Option<(String, i64, i64, LlmUsageIntegrity)> = conn
                .query_row(
                    "SELECT invocation.status,invocation.version,
                            CASE WHEN task.current_run_id=run.id
                                      AND task.status IN ('running','waitingDependencies')
                                      AND run.status IN ('running','waitingDependencies')
                                 THEN 1 ELSE 0 END,
                            run.usage_complete,task.usage_complete,root.usage_complete
                     FROM tool_invocations invocation
                     JOIN tasks task ON task.id=invocation.task_id
                     JOIN run_envelopes run
                       ON run.id=invocation.run_id AND run.task_id=task.id
                     JOIN tasks root ON root.id=task.root_task_id
                     WHERE invocation.invocation_id=?1",
                    params![invocation_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            LlmUsageIntegrity {
                                run_usage_complete: row.get::<_, i64>(3)? != 0,
                                task_usage_complete: row.get::<_, i64>(4)? != 0,
                                root_task_usage_complete: row.get::<_, i64>(5)? != 0,
                            },
                        ))
                    },
                )
                .optional()?;
            let Some((status, version, active, usage_integrity)) = current else {
                return Ok(CasOutcome::NotFound);
            };
            // This is the physical side-effect linearization point. A prior
            // read-only gate is advisory only: a sibling can poison the shared
            // root account while hooks/admission are awaiting. Preserve the
            // stable accounting code instead of collapsing it into an ordinary
            // lifecycle/CAS failure.
            let invocation_run: String = conn.query_row(
                "SELECT run_id FROM tool_invocations WHERE invocation_id=?1",
                [&invocation_id],
                |row| row.get(0),
            )?;
            if !usage_integrity.is_complete() && !timeout_usage_exception(conn, &invocation_run)? {
                return Err(DbError::Invalid("BUDGET_USAGE_INCOMPLETE".to_owned()));
            }
            if status != ToolInvocationStatus::Preparing.as_db() || active != 1 {
                return Ok(CasOutcome::InvalidTransition);
            }
            if version != expected_version {
                return Ok(CasOutcome::VersionConflict);
            }
            let now = format_rfc3339_micros(now_millis());
            let changed = conn.execute(
                "UPDATE tool_invocations
                 SET status='running',input_json=?1,side_effect_class=?2,
                     output_ref=NULL,error_code=NULL,cleanup_status='pending',
                     started_at=COALESCE(started_at,?3),terminal_at=NULL,
                     updated_at=?3,version=version+1
                 WHERE invocation_id=?4 AND version=?5 AND status='preparing'
                   AND EXISTS(
                     SELECT 1 FROM tasks task JOIN run_envelopes run
                       ON run.id=task.current_run_id AND run.task_id=task.id
                     JOIN tasks root ON root.id=task.root_task_id
                     WHERE task.id=tool_invocations.task_id
                       AND run.id=tool_invocations.run_id
                       AND task.status IN ('running','waitingDependencies')
                       AND run.status IN ('running','waitingDependencies')
                   )",
                params![
                    input_json,
                    side_effect_class,
                    now,
                    invocation_id,
                    expected_version
                ],
            )?;
            if changed == 1 {
                return Ok(CasOutcome::Applied);
            }
            // The guarded UPDATE is authoritative even if another SQLite
            // connection wrote between the diagnostic SELECT and this CAS.
            // Usage completeness is monotonic (there is no in-place backfill),
            // so a losing statement can recover the precise stable cause.
            let usage_after_cas: Option<LlmUsageIntegrity> = conn
                .query_row(
                    "SELECT run.usage_complete,task.usage_complete,root.usage_complete
                     FROM tool_invocations invocation
                     JOIN tasks task ON task.id=invocation.task_id
                     JOIN run_envelopes run
                       ON run.id=invocation.run_id AND run.task_id=task.id
                     JOIN tasks root ON root.id=task.root_task_id
                     WHERE invocation.invocation_id=?1",
                    params![invocation_id],
                    |row| {
                        Ok(LlmUsageIntegrity {
                            run_usage_complete: row.get::<_, i64>(0)? != 0,
                            task_usage_complete: row.get::<_, i64>(1)? != 0,
                            root_task_usage_complete: row.get::<_, i64>(2)? != 0,
                        })
                    },
                )
                .optional()?;
            if usage_after_cas.is_some_and(|integrity| !integrity.is_complete()) {
                return Err(DbError::Invalid("BUDGET_USAGE_INCOMPLETE".to_owned()));
            }
            Ok(CasOutcome::VersionConflict)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CommitTaskResult, CreateTaskWithRun, CreateTaskWithRunOutcome, ResultStatus, TaskStatus,
        VerificationStatus,
    };

    fn id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    #[tokio::test]
    async fn timeout_unknown_usage_allows_only_unbounded_cleaned_descendants() {
        for (bounded, call_status, call_error, allowed) in [
            (false, "cancelled", Some("STREAM_DROPPED"), true),
            (true, "cancelled", Some("STREAM_DROPPED"), false),
            (false, "completed", Some("STREAM_DROPPED"), false),
            (false, "cancelled", None, false),
        ] {
            let db = Db::open_in_memory().expect("db");
            let session = db.create_session("m", "/tmp/timeout-usage").await.unwrap();
            let mut root_request = budgeted_root_request(&session.id);
            if !bounded {
                root_request.execution_config_json = serde_json::json!({"budget": {
                    "deadlineAtMs": now_millis() + 600_000
                }})
                .to_string();
            }
            let root = db.create_task_with_run(&root_request).await.unwrap();
            db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                .await
                .unwrap();
            let child = db
                .create_task_with_run(&budgeted_child_request(&session.id, &root, 0))
                .await
                .unwrap();
            db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
                .await
                .unwrap();
            let call_id = id();
            db.start_llm_call_with_budget(
                &budgeted_call(&child.task.id, &child.run_id, &call_id),
                &LlmCallBudgetReservation {
                    input_tokens: 10,
                    output_tokens: 20,
                    cost_nanos_usd: 100,
                },
            )
            .await
            .unwrap();
            db.finish_llm_call(
                &call_id,
                call_status,
                &LlmUsageCompletion {
                    usage_complete: false,
                    error_code: call_error.map(str::to_owned),
                    ..LlmUsageCompletion::default()
                },
            )
            .await
            .unwrap();
            let timeout_run = child.run_id.clone();
            db.with_writer(move |conn| {
                conn.execute(
                    "UPDATE run_envelopes SET requested_exit_reason='timeout' WHERE id=?1",
                    [timeout_run],
                )?;
                Ok(())
            })
            .await
            .unwrap();
            let task = db
                .find_runtime_task_by_id(&child.task.id)
                .await
                .unwrap()
                .unwrap();
            db.commit_task_result(&CommitTaskResult {
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                expected_task_version: task.version,
                status: ResultStatus::Partial,
                content: "partial evidence".to_owned(),
                media_type: "text/plain".to_owned(),
                error_code: Some("SUBAGENT_DEADLINE_EXCEEDED".to_owned()),
                cleanup_status: CleanupStatus::Confirmed,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .unwrap();
            assert_eq!(
                db.assert_llm_usage_complete(&root.task.id, &root.run_id)
                    .await
                    .is_ok(),
                allowed,
                "bounded={bounded} status={call_status} error={call_error:?}"
            );
            assert!(
                !db.read_llm_usage_integrity(&root.task.id, &root.run_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_complete()
            );
            let next = db
                .start_llm_call_with_budget(
                    &budgeted_call(&root.task.id, &root.run_id, &id()),
                    &LlmCallBudgetReservation {
                        input_tokens: 10,
                        output_tokens: 20,
                        cost_nanos_usd: 100,
                    },
                )
                .await;
            assert_eq!(next.is_ok(), allowed);
        }
    }

    fn budgeted_root_request(session_id: &str) -> CreateTaskWithRun {
        CreateTaskWithRun {
            task_id: id(),
            run_id: id(),
            root_session_id: session_id.to_owned(),
            transcript_session_id: session_id.to_owned(),
            parent_task_id: None,
            parent_run_id: None,
            creator_tool_use_id: None,
            ordinal: 0,
            description: "usage integrity root".to_owned(),
            prompt: Some("root prompt".to_owned()),
            task_type: "agent".to_owned(),
            model: "m".to_owned(),
            working_dir: "/tmp/usage-integrity".to_owned(),
            execution_config_json: serde_json::json!({
                "budget": {
                    "tokenLimit": 10_000,
                    "costLimitNanosUsd": 10_000_000,
                    "deadlineAtMs": now_millis() + 600_000,
                }
            })
            .to_string(),
            startup_epoch: 1,
        }
    }

    fn budgeted_child_request(
        session_id: &str,
        parent: &CreateTaskWithRunOutcome,
        ordinal: i64,
    ) -> CreateTaskWithRun {
        CreateTaskWithRun {
            task_id: id(),
            run_id: id(),
            root_session_id: session_id.to_owned(),
            transcript_session_id: id(),
            parent_task_id: Some(parent.task.id.clone()),
            parent_run_id: Some(parent.run_id.clone()),
            creator_tool_use_id: Some(id()),
            ordinal,
            description: format!("usage integrity child {ordinal}"),
            prompt: Some("child prompt".to_owned()),
            task_type: "agent".to_owned(),
            model: "m".to_owned(),
            working_dir: "/tmp/usage-integrity".to_owned(),
            execution_config_json: r#"{"isolation":"readOnly"}"#.to_owned(),
            startup_epoch: 1,
        }
    }

    fn budgeted_call(task_id: &str, run_id: &str, call_id: &str) -> NewLlmCall {
        NewLlmCall {
            call_id: call_id.to_owned(),
            task_id: task_id.to_owned(),
            run_id: run_id.to_owned(),
            provider: "test".to_owned(),
            model: "m".to_owned(),
            route: None,
            provider_request_id: None,
        }
    }

    fn assert_usage_incomplete(error: DbError) {
        match error {
            DbError::Invalid(code) => assert_eq!(code, "BUDGET_USAGE_INCOMPLETE"),
            other => panic!("unexpected usage-integrity error: {other}"),
        }
    }

    async fn assert_no_llm_call(db: &Db, call_id: &str) {
        let call_id = call_id.to_owned();
        let count: i64 = db
            .with_reader(move |conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM llm_calls WHERE call_id=?1",
                    params![call_id],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .await
            .expect("llm call residue query");
        assert_eq!(
            count, 0,
            "rejected admission must not reserve or start a call"
        );
    }

    #[tokio::test]
    async fn owning_task_incomplete_blocks_budgeted_llm_start_without_a_call_row() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/owning-task-incomplete")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&budgeted_root_request(&session.id))
            .await
            .expect("root");
        assert_eq!(
            db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                .await
                .expect("claim root"),
            CasOutcome::Applied
        );
        let child = db
            .create_task_with_run(&budgeted_child_request(&session.id, &root, 0))
            .await
            .expect("child");
        assert_eq!(
            db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
                .await
                .expect("claim child"),
            CasOutcome::Applied
        );

        let child_task_id = child.task.id.clone();
        db.with_writer(move |conn| {
            assert_eq!(
                conn.execute(
                    "UPDATE tasks SET usage_complete=0 WHERE id=?1",
                    params![child_task_id],
                )?,
                1
            );
            Ok(())
        })
        .await
        .expect("poison owning task only");

        let integrity = db
            .read_llm_usage_integrity(&child.task.id, &child.run_id)
            .await
            .expect("integrity read")
            .expect("owned current run");
        assert!(integrity.run_usage_complete);
        assert!(!integrity.task_usage_complete);
        assert!(integrity.root_task_usage_complete);
        assert!(!integrity.is_complete());
        assert_usage_incomplete(
            db.assert_llm_usage_complete(&child.task.id, &child.run_id)
                .await
                .expect_err("owning Task poison must fail the reusable assertion"),
        );

        let call_id = id();
        assert_usage_incomplete(
            db.start_llm_call_with_budget(
                &budgeted_call(&child.task.id, &child.run_id, &call_id),
                &LlmCallBudgetReservation {
                    input_tokens: 10,
                    output_tokens: 20,
                    cost_nanos_usd: 100,
                },
            )
            .await
            .expect_err("owning Task poison must block physical admission"),
        );
        assert_no_llm_call(&db, &call_id).await;
    }

    #[tokio::test]
    async fn sibling_usage_poison_blocks_other_child_without_a_call_row() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/sibling-usage-poison")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&budgeted_root_request(&session.id))
            .await
            .expect("root");
        assert_eq!(
            db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                .await
                .expect("claim root"),
            CasOutcome::Applied
        );
        let failed_child = db
            .create_task_with_run(&budgeted_child_request(&session.id, &root, 0))
            .await
            .expect("failed child");
        let waiting_child = db
            .create_task_with_run(&budgeted_child_request(&session.id, &root, 1))
            .await
            .expect("waiting child");
        assert_eq!(
            db.claim_task_run_cas(
                &failed_child.task.id,
                &failed_child.run_id,
                failed_child.task.version,
            )
            .await
            .expect("claim failed child"),
            CasOutcome::Applied
        );
        assert_eq!(
            db.claim_task_run_cas(
                &waiting_child.task.id,
                &waiting_child.run_id,
                waiting_child.task.version,
            )
            .await
            .expect("claim waiting child"),
            CasOutcome::Applied
        );

        let failed_call_id = id();
        db.start_llm_call_with_budget(
            &budgeted_call(&failed_child.task.id, &failed_child.run_id, &failed_call_id),
            &LlmCallBudgetReservation {
                input_tokens: 10,
                output_tokens: 20,
                cost_nanos_usd: 100,
            },
        )
        .await
        .expect("first child call starts");
        assert_eq!(
            db.finish_llm_call(
                &failed_call_id,
                "failed",
                &LlmUsageCompletion {
                    usage_complete: false,
                    error_code: Some("BUDGET_USAGE_INCOMPLETE".to_owned()),
                    ..LlmUsageCompletion::default()
                },
            )
            .await
            .expect("first child incomplete finish"),
            CasOutcome::Applied
        );
        let failed_child_after_call = db
            .find_runtime_task_by_id(&failed_child.task.id)
            .await
            .expect("failed child read")
            .expect("failed child");
        db.commit_task_result(&CommitTaskResult {
            task_id: failed_child.task.id.clone(),
            run_id: failed_child.run_id.clone(),
            expected_task_version: failed_child_after_call.version,
            status: ResultStatus::Error,
            content: "usage missing".to_owned(),
            media_type: "text/plain".to_owned(),
            error_code: Some("BUDGET_USAGE_INCOMPLETE".to_owned()),
            cleanup_status: CleanupStatus::Confirmed,
            verification_status: VerificationStatus::NotRequested,
        })
        .await
        .expect("terminal child result poisons root budget account");

        let parent_integrity = db
            .read_llm_usage_integrity(&root.task.id, &root.run_id)
            .await
            .expect("parent integrity read")
            .expect("owned parent current run");
        assert!(parent_integrity.run_usage_complete);
        assert!(!parent_integrity.task_usage_complete);
        assert!(!parent_integrity.root_task_usage_complete);

        let rejected_parent_call_id = id();
        assert_usage_incomplete(
            db.start_llm_call_with_budget(
                &budgeted_call(&root.task.id, &root.run_id, &rejected_parent_call_id),
                &LlmCallBudgetReservation {
                    input_tokens: 10,
                    output_tokens: 20,
                    cost_nanos_usd: 100,
                },
            )
            .await
            .expect_err("a poisoned root Task must not continue its still-complete Run"),
        );
        assert_no_llm_call(&db, &rejected_parent_call_id).await;

        let integrity = db
            .read_llm_usage_integrity(&waiting_child.task.id, &waiting_child.run_id)
            .await
            .expect("sibling integrity read")
            .expect("owned current run");
        assert!(integrity.run_usage_complete);
        assert!(integrity.task_usage_complete);
        assert!(!integrity.root_task_usage_complete);
        assert!(!integrity.is_complete());

        let rejected_call_id = id();
        assert_usage_incomplete(
            db.start_llm_call_with_budget(
                &budgeted_call(
                    &waiting_child.task.id,
                    &waiting_child.run_id,
                    &rejected_call_id,
                ),
                &LlmCallBudgetReservation {
                    input_tokens: 10,
                    output_tokens: 20,
                    cost_nanos_usd: 100,
                },
            )
            .await
            .expect_err("root poison from a sibling must block physical admission"),
        );
        assert_no_llm_call(&db, &rejected_call_id).await;
    }

    #[tokio::test]
    async fn incomplete_child_call_immediately_blocks_sibling_tool_before_task_result() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/child-call-root-poison")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&budgeted_root_request(&session.id))
            .await
            .expect("root");
        assert_eq!(
            db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                .await
                .expect("claim root"),
            CasOutcome::Applied
        );
        let sibling = db
            .create_task_with_run(&budgeted_child_request(&session.id, &root, 0))
            .await
            .expect("sibling child");
        let incomplete = db
            .create_task_with_run(&budgeted_child_request(&session.id, &root, 1))
            .await
            .expect("incomplete child");
        assert_eq!(
            db.claim_task_run_cas(&sibling.task.id, &sibling.run_id, sibling.task.version)
                .await
                .expect("claim sibling"),
            CasOutcome::Applied
        );
        assert_eq!(
            db.claim_task_run_cas(
                &incomplete.task.id,
                &incomplete.run_id,
                incomplete.task.version,
            )
            .await
            .expect("claim incomplete child"),
            CasOutcome::Applied
        );

        let invocation_id = id();
        db.create_tool_invocation(&NewToolInvocation {
            invocation_id: invocation_id.clone(),
            task_id: sibling.task.id.clone(),
            run_id: sibling.run_id.clone(),
            tool_use_id: id(),
            tool_name: "Echo".to_owned(),
            input_json: Some(r#"{"text":"must-not-run"}"#.to_owned()),
            side_effect_class: "read".to_owned(),
            directory_generation: Some(1),
            connection_generation: None,
        })
        .await
        .expect("preparing sibling invocation");

        let call_id = id();
        db.start_llm_call_with_budget(
            &budgeted_call(&incomplete.task.id, &incomplete.run_id, &call_id),
            &LlmCallBudgetReservation {
                input_tokens: 10,
                output_tokens: 20,
                cost_nanos_usd: 100,
            },
        )
        .await
        .expect("incomplete child call starts");
        assert_eq!(
            db.finish_llm_call(
                &call_id,
                "failed",
                &LlmUsageCompletion {
                    usage_complete: false,
                    error_code: Some("BUDGET_USAGE_INCOMPLETE".to_owned()),
                    ..LlmUsageCompletion::default()
                },
            )
            .await
            .expect("incomplete child terminal write"),
            CasOutcome::Applied
        );

        let incomplete_task_id = incomplete.task.id.clone();
        let result_count: i64 = db
            .with_reader(move |connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM task_results WHERE task_id=?1",
                        params![incomplete_task_id],
                        |row| row.get(0),
                    )
                    .map_err(Into::into)
            })
            .await
            .expect("child result count");
        assert_eq!(
            result_count, 0,
            "the root must be poisoned by the physical-call terminal transaction"
        );

        let incomplete_integrity = db
            .read_llm_usage_integrity(&incomplete.task.id, &incomplete.run_id)
            .await
            .expect("incomplete child integrity")
            .expect("incomplete child current run");
        assert!(!incomplete_integrity.run_usage_complete);
        assert!(!incomplete_integrity.task_usage_complete);
        assert!(!incomplete_integrity.root_task_usage_complete);

        let sibling_integrity = db
            .read_llm_usage_integrity(&sibling.task.id, &sibling.run_id)
            .await
            .expect("sibling integrity")
            .expect("sibling current run");
        assert!(sibling_integrity.run_usage_complete);
        assert!(sibling_integrity.task_usage_complete);
        assert!(!sibling_integrity.root_task_usage_complete);
        assert_usage_incomplete(
            db.start_tool_invocation_for_active_run_cas(
                &invocation_id,
                0,
                r#"{"text":"must-not-run"}"#,
                "read",
            )
            .await
            .expect_err("root poison must win before sibling physical execution"),
        );

        let invocation_key = invocation_id.clone();
        let invocation: (String, i64, Option<String>) = db
            .with_reader(move |connection| {
                connection
                    .query_row(
                        "SELECT status,version,started_at FROM tool_invocations
                         WHERE invocation_id=?1",
                        params![invocation_key],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(Into::into)
            })
            .await
            .expect("sibling invocation read");
        assert_eq!(invocation.0, "preparing");
        assert_eq!(invocation.1, 0);
        assert!(invocation.2.is_none());
    }

    #[tokio::test]
    async fn complete_child_call_does_not_poison_root_or_sibling_tool_start() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/complete-child-call")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&budgeted_root_request(&session.id))
            .await
            .expect("root");
        assert_eq!(
            db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                .await
                .expect("claim root"),
            CasOutcome::Applied
        );
        let sibling = db
            .create_task_with_run(&budgeted_child_request(&session.id, &root, 0))
            .await
            .expect("sibling child");
        let complete = db
            .create_task_with_run(&budgeted_child_request(&session.id, &root, 1))
            .await
            .expect("complete child");
        assert_eq!(
            db.claim_task_run_cas(&sibling.task.id, &sibling.run_id, sibling.task.version)
                .await
                .expect("claim sibling"),
            CasOutcome::Applied
        );
        assert_eq!(
            db.claim_task_run_cas(&complete.task.id, &complete.run_id, complete.task.version)
                .await
                .expect("claim complete child"),
            CasOutcome::Applied
        );

        let invocation_id = id();
        db.create_tool_invocation(&NewToolInvocation {
            invocation_id: invocation_id.clone(),
            task_id: sibling.task.id.clone(),
            run_id: sibling.run_id.clone(),
            tool_use_id: id(),
            tool_name: "Echo".to_owned(),
            input_json: Some(r#"{"text":"may-run"}"#.to_owned()),
            side_effect_class: "read".to_owned(),
            directory_generation: Some(1),
            connection_generation: None,
        })
        .await
        .expect("preparing sibling invocation");

        let call_id = id();
        db.start_llm_call_with_budget(
            &budgeted_call(&complete.task.id, &complete.run_id, &call_id),
            &LlmCallBudgetReservation {
                input_tokens: 10,
                output_tokens: 20,
                cost_nanos_usd: 100,
            },
        )
        .await
        .expect("complete child call starts");
        assert_eq!(
            db.finish_llm_call(
                &call_id,
                "completed",
                &LlmUsageCompletion {
                    input_tokens: Some(4),
                    output_tokens: Some(5),
                    cache_read_tokens: Some(0),
                    cache_create_tokens: Some(0),
                    cost_nanos_usd: Some(80),
                    usage_complete: true,
                    error_code: None,
                },
            )
            .await
            .expect("complete child terminal write"),
            CasOutcome::Applied
        );

        for (task_id, run_id) in [
            (&root.task.id, &root.run_id),
            (&sibling.task.id, &sibling.run_id),
            (&complete.task.id, &complete.run_id),
        ] {
            let integrity = db
                .read_llm_usage_integrity(task_id, run_id)
                .await
                .expect("usage integrity")
                .expect("current run");
            assert!(integrity.is_complete());
        }
        assert_eq!(
            db.start_tool_invocation_for_active_run_cas(
                &invocation_id,
                0,
                r#"{"text":"may-run"}"#,
                "read",
            )
            .await
            .expect("complete usage permits sibling start"),
            CasOutcome::Applied
        );

        let invocation_key = invocation_id.clone();
        let invocation: (String, i64, Option<String>) = db
            .with_reader(move |connection| {
                connection
                    .query_row(
                        "SELECT status,version,started_at FROM tool_invocations
                         WHERE invocation_id=?1",
                        params![invocation_key],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(Into::into)
            })
            .await
            .expect("sibling invocation read");
        assert_eq!(invocation.0, "running");
        assert_eq!(invocation.1, 1);
        assert!(invocation.2.is_some());
    }

    #[tokio::test]
    async fn prepared_sibling_start_is_allowed_while_parent_waits_for_attached_child() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/parallel-attached-tool-start")
            .await
            .expect("session");
        let root = db
            .create_task_with_run(&budgeted_root_request(&session.id))
            .await
            .expect("root");
        assert_eq!(
            db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                .await
                .expect("claim root"),
            CasOutcome::Applied
        );

        let first_invocation_id = id();
        let sibling_invocation_id = id();
        for (invocation_id, tool_use_id) in [
            (&first_invocation_id, "parallel-agent-1"),
            (&sibling_invocation_id, "parallel-agent-2"),
        ] {
            db.create_tool_invocation(&NewToolInvocation {
                invocation_id: invocation_id.clone(),
                task_id: root.task.id.clone(),
                run_id: root.run_id.clone(),
                tool_use_id: tool_use_id.to_owned(),
                tool_name: "Agent".to_owned(),
                input_json: Some(r#"{"waitMode":"terminal"}"#.to_owned()),
                side_effect_class: "read".to_owned(),
                directory_generation: Some(1),
                connection_generation: None,
            })
            .await
            .expect("prepare parallel Agent invocation");
        }
        assert_eq!(
            db.start_tool_invocation_for_active_run_cas(
                &first_invocation_id,
                0,
                r#"{"waitMode":"terminal"}"#,
                "read",
            )
            .await
            .expect("start first Agent invocation"),
            CasOutcome::Applied
        );

        db.create_task_with_run(&budgeted_child_request(&session.id, &root, 0))
            .await
            .expect("first Agent creates attached child");
        let waiting_root = db
            .find_runtime_task_by_id(&root.task.id)
            .await
            .expect("read waiting root")
            .expect("root exists");
        let waiting_run = db
            .find_run_by_id(&root.run_id)
            .await
            .expect("read waiting root Run")
            .expect("root Run exists");
        assert_eq!(waiting_root.status, TaskStatus::WaitingDependencies);
        assert_eq!(waiting_run.status, "waitingDependencies");

        assert_eq!(
            db.start_tool_invocation_for_active_run_cas(
                &sibling_invocation_id,
                0,
                r#"{"waitMode":"terminal"}"#,
                "read",
            )
            .await
            .expect("waiting parent still owns prepared sibling"),
            CasOutcome::Applied
        );
        let sibling_key = sibling_invocation_id.clone();
        let sibling: (String, i64, Option<String>) = db
            .with_reader(move |connection| {
                connection
                    .query_row(
                        "SELECT status,version,started_at FROM tool_invocations
                         WHERE invocation_id=?1",
                        params![sibling_key],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(Into::into)
            })
            .await
            .expect("read sibling invocation");
        assert_eq!(sibling.0, "running");
        assert_eq!(sibling.1, 1);
        assert!(sibling.2.is_some());
    }

    #[tokio::test]
    async fn tool_start_rechecks_run_task_and_root_usage_at_the_physical_boundary() {
        for poisoned_authority in ["run", "task", "root"] {
            let db = Db::open_in_memory().expect("db");
            let session = db
                .create_session("m", "/tmp/tool-usage-linearization")
                .await
                .expect("session");
            let root = db
                .create_task_with_run(&budgeted_root_request(&session.id))
                .await
                .expect("root");
            assert_eq!(
                db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                    .await
                    .expect("claim root"),
                CasOutcome::Applied
            );
            let child = db
                .create_task_with_run(&budgeted_child_request(&session.id, &root, 0))
                .await
                .expect("child");
            assert_eq!(
                db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
                    .await
                    .expect("claim child"),
                CasOutcome::Applied
            );
            let invocation_id = id();
            db.create_tool_invocation(&NewToolInvocation {
                invocation_id: invocation_id.clone(),
                task_id: child.task.id.clone(),
                run_id: child.run_id.clone(),
                tool_use_id: id(),
                tool_name: "Echo".to_owned(),
                input_json: Some(r#"{"text":"must-not-run"}"#.to_owned()),
                side_effect_class: "read".to_owned(),
                directory_generation: Some(1),
                connection_generation: None,
            })
            .await
            .expect("preparing invocation");

            let child_task_id = child.task.id.clone();
            let child_run_id = child.run_id.clone();
            let root_task_id = root.task.id.clone();
            let authority = poisoned_authority.to_owned();
            db.with_writer(move |connection| {
                let changed = match authority.as_str() {
                    "run" => connection.execute(
                        "UPDATE run_envelopes SET usage_complete=0 WHERE id=?1",
                        params![child_run_id],
                    )?,
                    "task" => connection.execute(
                        "UPDATE tasks SET usage_complete=0 WHERE id=?1",
                        params![child_task_id],
                    )?,
                    "root" => connection.execute(
                        "UPDATE tasks SET usage_complete=0 WHERE id=?1",
                        params![root_task_id],
                    )?,
                    _ => unreachable!(),
                };
                assert_eq!(changed, 1);
                Ok(())
            })
            .await
            .expect("poison usage authority");

            assert_usage_incomplete(
                db.start_tool_invocation_for_active_run_cas(
                    &invocation_id,
                    0,
                    r#"{"text":"must-not-run"}"#,
                    "read",
                )
                .await
                .expect_err("incomplete usage must win at physical start"),
            );
            let invocation_key = invocation_id.clone();
            let invocation: (String, i64, Option<String>) = db
                .with_reader(move |connection| {
                    connection
                        .query_row(
                            "SELECT status,version,started_at FROM tool_invocations
                             WHERE invocation_id=?1",
                            params![invocation_key],
                            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                        )
                        .map_err(Into::into)
                })
                .await
                .expect("invocation read");
            assert_eq!(
                invocation.0, "preparing",
                "{poisoned_authority} poison crossed physical boundary"
            );
            assert_eq!(invocation.1, 0);
            assert!(invocation.2.is_none());
        }
    }

    #[tokio::test]
    async fn tool_resource_and_llm_ledgers_are_owned_and_terminal_once() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/ledger")
            .await
            .expect("session");
        let created = db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: id(),
                run_id: id(),
                root_session_id: session.id.clone(),
                transcript_session_id: session.id,
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: "ledger".to_owned(),
                prompt: None,
                task_type: "agent".to_owned(),
                model: "m".to_owned(),
                working_dir: "/tmp/ledger".to_owned(),
                execution_config_json: "{}".to_owned(),
                startup_epoch: 1,
            })
            .await
            .expect("task");
        assert_eq!(created.task.status, TaskStatus::Queued);
        assert_eq!(
            db.claim_task_run_cas(&created.task.id, &created.run_id, created.task.version)
                .await
                .expect("claim task/run"),
            CasOutcome::Applied
        );

        let invocation_id = id();
        let invocation = db
            .create_tool_invocation(&NewToolInvocation {
                invocation_id: invocation_id.clone(),
                task_id: created.task.id.clone(),
                run_id: created.run_id.clone(),
                tool_use_id: "tool-1".to_owned(),
                tool_name: "Read".to_owned(),
                input_json: None,
                side_effect_class: "read".to_owned(),
                directory_generation: Some(2),
                connection_generation: None,
            })
            .await
            .expect("tool");
        assert_eq!(invocation.status, "preparing");
        assert_eq!(
            db.transition_tool_invocation_cas(
                &invocation_id,
                0,
                ToolInvocationStatus::Running,
                Some(r#"{"path":"a"}"#),
                None,
                None,
                CleanupStatus::NotRequired,
            )
            .await
            .expect("running"),
            CasOutcome::Applied
        );
        assert_eq!(
            db.transition_tool_invocation_cas(
                &invocation_id,
                1,
                ToolInvocationStatus::Succeeded,
                Some(r#"{"path":"a"}"#),
                Some("inline:ok"),
                None,
                CleanupStatus::Confirmed,
            )
            .await
            .expect("done"),
            CasOutcome::Applied
        );

        let reserved_resource_id = id();
        db.register_execution_resource(&NewExecutionResource {
            resource_id: reserved_resource_id.clone(),
            task_id: created.task.id.clone(),
            run_id: created.run_id.clone(),
            invocation_id: Some(invocation_id.clone()),
            resource_kind: "processGroup".to_owned(),
            external_id: None,
            metadata_json: r#"{"phase":"reserved"}"#.to_owned(),
        })
        .await
        .expect("resource reservation");
        assert_eq!(
            db.bind_execution_resource_external(&reserved_resource_id, "41")
                .await
                .expect("bind external id"),
            CasOutcome::Applied
        );
        assert_eq!(
            db.bind_execution_resource_external(&reserved_resource_id, "41")
                .await
                .expect("idempotent bind"),
            CasOutcome::Applied
        );
        assert_eq!(
            db.bind_execution_resource_external(&reserved_resource_id, "different")
                .await
                .expect("conflicting bind"),
            CasOutcome::InvalidTransition
        );
        let expected_reserved_resource_id = reserved_resource_id.clone();
        let bound_external_id: Option<String> = db
            .with_conn_blocking(move |conn| {
                conn.query_row(
                    "SELECT external_id FROM execution_resources WHERE resource_id=?1",
                    params![expected_reserved_resource_id],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .expect("bound external id");
        assert_eq!(bound_external_id.as_deref(), Some("41"));
        db.finalize_execution_resource(&reserved_resource_id, ExecutionResourceStatus::Released)
            .await
            .expect("release reserved resource");

        let resource_id = id();
        db.register_execution_resource(&NewExecutionResource {
            resource_id: resource_id.clone(),
            task_id: created.task.id.clone(),
            run_id: created.run_id.clone(),
            invocation_id: Some(invocation_id.clone()),
            resource_kind: "process".to_owned(),
            external_id: Some("42".to_owned()),
            metadata_json: "{}".to_owned(),
        })
        .await
        .expect("resource");
        assert_eq!(
            db.finalize_execution_resource(&resource_id, ExecutionResourceStatus::Released)
                .await
                .expect("release"),
            CasOutcome::Applied
        );
        assert_eq!(
            db.finalize_execution_resource(&resource_id, ExecutionResourceStatus::Released)
                .await
                .expect("idempotent release"),
            CasOutcome::Applied
        );

        let uncertain_resource_id = id();
        db.register_execution_resource(&NewExecutionResource {
            resource_id: uncertain_resource_id.clone(),
            task_id: created.task.id.clone(),
            run_id: created.run_id.clone(),
            invocation_id: Some(invocation_id.clone()),
            resource_kind: "processGroup".to_owned(),
            external_id: Some("43".to_owned()),
            metadata_json: "{}".to_owned(),
        })
        .await
        .expect("uncertain resource");
        assert_eq!(
            db.finalize_execution_resource(
                &uncertain_resource_id,
                ExecutionResourceStatus::Unconfirmed,
            )
            .await
            .expect("unconfirmed"),
            CasOutcome::Applied
        );
        let cleanup_status: String = db
            .with_conn_blocking(move |conn| {
                conn.query_row(
                    "SELECT cleanup_status FROM tool_invocations WHERE invocation_id=?1",
                    params![invocation_id],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .expect("cleanup projection");
        assert_eq!(cleanup_status, "unconfirmed");

        let call_id = id();
        db.start_llm_call(&NewLlmCall {
            call_id: call_id.clone(),
            task_id: created.task.id.clone(),
            run_id: created.run_id.clone(),
            provider: "test".to_owned(),
            model: "m".to_owned(),
            route: None,
            provider_request_id: Some("request-1".to_owned()),
        })
        .await
        .expect("llm start");
        assert_eq!(
            db.finish_llm_call(
                &call_id,
                "completed",
                &LlmUsageCompletion {
                    input_tokens: Some(10),
                    output_tokens: Some(4),
                    cache_read_tokens: Some(2),
                    cache_create_tokens: Some(0),
                    cost_nanos_usd: Some(123),
                    usage_complete: true,
                    error_code: None,
                },
            )
            .await
            .expect("llm finish"),
            CasOutcome::Applied
        );
        let totals: (i64, i64, i64) = db
            .with_conn_blocking(move |conn| {
                conn.query_row(
                    "SELECT input_tokens,output_tokens,cost_nanos_usd FROM run_envelopes WHERE id=?1",
                    params![created.run_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(Into::into)
            })
            .expect("totals");
        assert_eq!(totals, (10, 4, 123));
    }

    #[tokio::test]
    async fn terminal_invocation_and_tool_result_roll_back_together_at_kill_window() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("m", "/tmp/atomic-tool-result")
            .await
            .expect("session");
        let created = db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: id(),
                run_id: id(),
                root_session_id: session.id.clone(),
                transcript_session_id: session.id.clone(),
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: "atomic tool result".to_owned(),
                prompt: None,
                task_type: "agent".to_owned(),
                model: "m".to_owned(),
                working_dir: "/tmp/atomic-tool-result".to_owned(),
                execution_config_json: "{}".to_owned(),
                startup_epoch: 1,
            })
            .await
            .expect("task");
        assert_eq!(
            db.claim_task_run_cas(&created.task.id, &created.run_id, created.task.version)
                .await
                .expect("claim"),
            CasOutcome::Applied
        );
        let invocation_id = id();
        db.create_tool_invocation(&NewToolInvocation {
            invocation_id: invocation_id.clone(),
            task_id: created.task.id.clone(),
            run_id: created.run_id.clone(),
            tool_use_id: "tool-atomic".to_owned(),
            tool_name: "Write".to_owned(),
            input_json: Some(r#"{"path":"a"}"#.to_owned()),
            side_effect_class: "write".to_owned(),
            directory_generation: Some(1),
            connection_generation: None,
        })
        .await
        .expect("invocation");
        assert_eq!(
            db.transition_tool_invocation_cas(
                &invocation_id,
                0,
                ToolInvocationStatus::Running,
                Some(r#"{"path":"a"}"#),
                None,
                None,
                CleanupStatus::Pending,
            )
            .await
            .expect("running"),
            CasOutcome::Applied
        );

        db.with_conn_blocking(|connection| {
            connection.execute_batch(
                "CREATE TRIGGER fail_terminal_tool_result
                 BEFORE UPDATE OF status ON tool_invocations
                 WHEN NEW.status='succeeded'
                 BEGIN SELECT RAISE(ABORT,'FAILPOINT_AFTER_TOOL_RESULT_INSERT'); END;",
            )?;
            Ok(())
        })
        .expect("install failpoint");
        let request = CommitToolInvocationResult {
            invocation_id: invocation_id.clone(),
            expected_version: 1,
            session_id: session.id.clone(),
            target: ToolInvocationStatus::Succeeded,
            input_json: Some(r#"{"path":"a"}"#.to_owned()),
            content: "written".to_owned(),
            is_error: false,
            metadata: Some(serde_json::json!({"receipt": "durable"})),
            output_sha256: Some("a".repeat(64)),
            error_code: None,
            cleanup_status: CleanupStatus::Confirmed,
            postprocessing: Some(serde_json::json!({"artifact": {"path": "a"}})),
        };
        assert!(db.commit_tool_invocation_result(&request).await.is_err());
        let after_failure: (String, i64, i64, i64) = db
            .with_conn_blocking({
                let invocation_id = invocation_id.clone();
                move |connection| {
                    connection
                        .query_row(
                            "SELECT invocation.status,invocation.version,
                                    (SELECT COUNT(*) FROM messages WHERE origin='tool_result'),
                                    (SELECT COUNT(*) FROM tool_result_postprocessing)
                             FROM tool_invocations invocation WHERE invocation_id=?1",
                            params![invocation_id],
                            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                        )
                        .map_err(Into::into)
                }
            })
            .expect("rolled back facts");
        assert_eq!(after_failure, ("running".to_owned(), 1, 0, 0));

        db.with_conn_blocking(|connection| {
            connection.execute_batch("DROP TRIGGER fail_terminal_tool_result")?;
            Ok(())
        })
        .expect("remove failpoint");
        let committed = db
            .commit_tool_invocation_result(&request)
            .await
            .expect("commit succeeds after retry");
        let CommitToolInvocationResultOutcome::Committed(committed) = committed else {
            panic!("expected committed outcome")
        };
        assert_eq!(committed.invocation.status, "succeeded");
        assert_eq!(committed.message.session_id, session.id);
        assert!(matches!(
            db.commit_tool_invocation_result(&request)
                .await
                .expect("idempotent conflict"),
            CommitToolInvocationResultOutcome::InvalidTransition
        ));
        assert_eq!(
            db.complete_tool_result_postprocessing_cas(&invocation_id, 0)
                .await
                .expect("complete postprocessing"),
            CasOutcome::Applied
        );
        let counts: (i64, String) = db
            .with_conn_blocking(move |connection| {
                connection
                    .query_row(
                        "SELECT (SELECT COUNT(*) FROM messages WHERE origin='tool_result'),status
                         FROM tool_result_postprocessing WHERE invocation_id=?1",
                        params![invocation_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(Into::into)
            })
            .expect("final facts");
        assert_eq!(counts, (1, "completed".to_owned()));
    }
}

#[derive(Clone, Debug)]
pub struct NewExecutionResource {
    pub resource_id: String,
    pub task_id: String,
    pub run_id: String,
    pub invocation_id: Option<String>,
    pub resource_kind: String,
    pub external_id: Option<String>,
    pub metadata_json: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExecutionResourceStatus {
    Allocated,
    Stopping,
    Released,
    Unconfirmed,
}

impl ExecutionResourceStatus {
    #[must_use]
    pub const fn as_db(self) -> &'static str {
        match self {
            Self::Allocated => "allocated",
            Self::Stopping => "stopping",
            Self::Released => "released",
            Self::Unconfirmed => "unconfirmed",
        }
    }
}

impl Db {
    pub async fn register_execution_resource(
        &self,
        resource: &NewExecutionResource,
    ) -> Result<(), DbError> {
        let resource = resource.clone();
        serde_json::from_str::<serde_json::Value>(&resource.metadata_json)?;
        self.with_writer(move |conn| {
            let run_owned: i64 = conn.query_row(
                "SELECT COUNT(*) FROM run_envelopes WHERE id=?1 AND task_id=?2",
                params![resource.run_id, resource.task_id],
                |row| row.get(0),
            )?;
            if run_owned != 1 {
                return Err(DbError::Invalid("RESOURCE_RUN_NOT_OWNED".to_owned()));
            }
            if let Some(invocation_id) = resource.invocation_id.as_deref() {
                let invocation_owned: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM tool_invocations
                      WHERE invocation_id=?1 AND run_id=?2 AND task_id=?3",
                    params![invocation_id, resource.run_id, resource.task_id],
                    |row| row.get(0),
                )?;
                if invocation_owned != 1 {
                    return Err(DbError::Invalid("RESOURCE_INVOCATION_NOT_OWNED".to_owned()));
                }
            }
            let now = format_rfc3339_micros(now_millis());
            conn.execute(
                "INSERT INTO execution_resources
                    (resource_id,task_id,run_id,invocation_id,resource_kind,external_id,status,
                     metadata_json,created_at,updated_at)
                 VALUES(?1,?2,?3,?4,?5,?6,'allocated',?7,?8,?8)",
                params![
                    resource.resource_id,
                    resource.task_id,
                    resource.run_id,
                    resource.invocation_id,
                    resource.resource_kind,
                    resource.external_id,
                    resource.metadata_json,
                    now,
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// Idempotently bind an OS/transport identifier to a reservation which was
    /// committed before the physical resource was created.
    pub async fn bind_execution_resource_external(
        &self,
        resource_id: &str,
        external_id: &str,
    ) -> Result<CasOutcome, DbError> {
        if external_id.trim().is_empty() {
            return Err(DbError::Invalid(
                "EXECUTION_RESOURCE_EXTERNAL_ID_REQUIRED".to_owned(),
            ));
        }
        let resource_id = resource_id.to_owned();
        let external_id = external_id.to_owned();
        self.with_writer(move |conn| {
            let current: Option<(String, Option<String>)> = conn
                .query_row(
                    "SELECT status,external_id FROM execution_resources WHERE resource_id=?1",
                    params![resource_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((status, current_external_id)) = current else {
                return Ok(CasOutcome::NotFound);
            };
            if current_external_id.as_deref() == Some(external_id.as_str()) {
                return Ok(CasOutcome::Applied);
            }
            if current_external_id.is_some() || status != "allocated" {
                return Ok(CasOutcome::InvalidTransition);
            }
            let now = format_rfc3339_micros(now_millis());
            let changed = conn.execute(
                "UPDATE execution_resources SET external_id=?1,updated_at=?2,version=version+1
                 WHERE resource_id=?3 AND status='allocated' AND external_id IS NULL",
                params![external_id, now, resource_id],
            )?;
            Ok(if changed == 1 {
                CasOutcome::Applied
            } else {
                CasOutcome::VersionConflict
            })
        })
        .await
    }

    pub async fn transition_execution_resource_cas(
        &self,
        resource_id: &str,
        expected_version: i64,
        target: ExecutionResourceStatus,
    ) -> Result<CasOutcome, DbError> {
        let resource_id = resource_id.to_owned();
        self.with_writer(move |conn| {
            let now = format_rfc3339_micros(now_millis());
            let changed = conn.execute(
                "UPDATE execution_resources SET status=?1,
                    released_at=CASE WHEN ?1='released' THEN ?2 ELSE NULL END,
                    updated_at=?2,version=version+1
                 WHERE resource_id=?3 AND version=?4 AND status NOT IN ('released','unconfirmed')",
                params![target.as_db(), now, resource_id, expected_version],
            )?;
            if changed == 1 {
                return Ok(CasOutcome::Applied);
            }
            let exists: Option<i64> = conn
                .query_row(
                    "SELECT version FROM execution_resources WHERE resource_id=?1",
                    params![resource_id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(match exists {
                None => CasOutcome::NotFound,
                Some(version) if version != expected_version => CasOutcome::VersionConflict,
                Some(_) => CasOutcome::InvalidTransition,
            })
        })
        .await
    }

    /// Idempotently commit a physical resource cleanup terminal and refresh the
    /// owning invocation's orthogonal cleanup projection.
    ///
    /// This API intentionally does not require an in-memory version cursor: the
    /// cleanup supervisor may outlive (or be detached from) the tool future that
    /// created the resource. A proven `released` resource can never be degraded
    /// to `unconfirmed`, and an unconfirmed terminal can never later be promoted
    /// without explicit operator reconciliation.
    pub async fn finalize_execution_resource(
        &self,
        resource_id: &str,
        target: ExecutionResourceStatus,
    ) -> Result<CasOutcome, DbError> {
        if !matches!(
            target,
            ExecutionResourceStatus::Released | ExecutionResourceStatus::Unconfirmed
        ) {
            return Err(DbError::Invalid(
                "EXECUTION_RESOURCE_TERMINAL_REQUIRED".to_owned(),
            ));
        }
        let resource_id = resource_id.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let current: Option<(String, Option<String>)> = tx
                .query_row(
                    "SELECT status,invocation_id FROM execution_resources WHERE resource_id=?1",
                    params![resource_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((status, invocation_id)) = current else {
                return Ok(CasOutcome::NotFound);
            };
            let target_status = target.as_db();
            if matches!(status.as_str(), "released" | "unconfirmed") && status != target_status {
                return Ok(CasOutcome::InvalidTransition);
            }
            let now = format_rfc3339_micros(now_millis());
            if status != target_status {
                tx.execute(
                    "UPDATE execution_resources SET status=?1,
                        released_at=CASE WHEN ?1='released' THEN ?2 ELSE NULL END,
                        updated_at=?2,version=version+1 WHERE resource_id=?3",
                    params![target_status, now, resource_id],
                )?;
            }
            if let Some(invocation_id) = invocation_id {
                // cleanup_status is orthogonal to the invocation lifecycle CAS,
                // so this projection must not advance invocation.version while
                // the Engine still owns its status transition cursor.
                tx.execute(
                    "UPDATE tool_invocations SET cleanup_status=(
                        CASE
                          WHEN EXISTS(
                            SELECT 1 FROM execution_resources
                             WHERE invocation_id=?1 AND status='unconfirmed'
                          ) THEN 'unconfirmed'
                          WHEN EXISTS(
                            SELECT 1 FROM execution_resources
                             WHERE invocation_id=?1 AND status IN ('allocated','stopping')
                          ) THEN 'pending'
                          WHEN EXISTS(
                            SELECT 1 FROM execution_resources WHERE invocation_id=?1
                          ) THEN 'confirmed'
                          ELSE cleanup_status
                        END
                    ),updated_at=?2 WHERE invocation_id=?1",
                    params![invocation_id, now],
                )?;
            }
            tx.commit()?;
            Ok(CasOutcome::Applied)
        })
        .await
    }

    /// Project the worst cleanup state for a Run from its physical invocation and
    /// resource ledgers. `unconfirmed` dominates `pending`, which dominates a
    /// proven `confirmed` cleanup; a Run with no cleanup-bearing work is
    /// `notRequired`.
    pub async fn run_cleanup_status(&self, run_id: &str) -> Result<CleanupStatus, DbError> {
        let run_id = run_id.to_owned();
        self.with_reader(move |conn| {
            let (unconfirmed, pending, confirmed): (i64, i64, i64) = conn.query_row(
                "SELECT
                    EXISTS(SELECT 1 FROM tool_invocations
                           WHERE run_id=?1 AND cleanup_status='unconfirmed')
                    OR EXISTS(SELECT 1 FROM execution_resources
                              WHERE run_id=?1 AND status='unconfirmed'),
                    EXISTS(SELECT 1 FROM tool_invocations
                           WHERE run_id=?1 AND (status IN ('preparing','queued','running')
                                               OR cleanup_status='pending'))
                    OR EXISTS(SELECT 1 FROM execution_resources
                              WHERE run_id=?1 AND status IN ('allocated','stopping')),
                    EXISTS(SELECT 1 FROM tool_invocations
                           WHERE run_id=?1 AND cleanup_status='confirmed')
                    OR EXISTS(SELECT 1 FROM execution_resources
                              WHERE run_id=?1 AND status='released')",
                params![run_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            Ok(if unconfirmed != 0 {
                CleanupStatus::Unconfirmed
            } else if pending != 0 {
                CleanupStatus::Pending
            } else if confirmed != 0 {
                CleanupStatus::Confirmed
            } else {
                CleanupStatus::NotRequired
            })
        })
        .await
    }
}

#[derive(Clone, Debug)]
pub struct NewLlmCall {
    pub call_id: String,
    pub task_id: String,
    pub run_id: String,
    pub provider: String,
    pub model: String,
    pub route: Option<String>,
    pub provider_request_id: Option<String>,
}

/// Worst-case resources atomically reserved before a physical provider stream is polled.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LlmCallBudgetReservation {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cost_nanos_usd: i64,
}

#[derive(Clone, Debug, Default)]
pub struct LlmUsageCompletion {
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_create_tokens: Option<i64>,
    pub cost_nanos_usd: Option<i64>,
    pub usage_complete: bool,
    pub error_code: Option<String>,
}

/// Durable usage-integrity facts which must all remain complete before another
/// physical provider call can be admitted for a Task's current Run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LlmUsageIntegrity {
    pub run_usage_complete: bool,
    pub task_usage_complete: bool,
    pub root_task_usage_complete: bool,
}

impl LlmUsageIntegrity {
    #[must_use]
    pub const fn is_complete(self) -> bool {
        self.run_usage_complete && self.task_usage_complete && self.root_task_usage_complete
    }
}

/// Unknown usage is admissible only for cleaned-up, timed-out descendants of
/// an unbounded root. This never changes the accounting facts themselves.
pub(crate) fn timeout_usage_exception(conn: &Connection, run_id: &str) -> Result<bool, DbError> {
    conn.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM run_envelopes current
           JOIN tasks owner ON owner.id=current.task_id AND owner.current_run_id=current.id
           JOIN tasks root ON root.id=owner.root_task_id
           WHERE current.id=?1 AND current.usage_complete=1
             AND root.token_budget_limit IS NULL AND root.cost_budget_nanos_usd IS NULL
             AND EXISTS(SELECT 1 FROM llm_calls c JOIN tasks t ON t.id=c.task_id
                        WHERE t.root_task_id=root.id AND c.usage_complete=0)
             AND NOT EXISTS(
               SELECT 1 FROM llm_calls c JOIN tasks t ON t.id=c.task_id
               JOIN run_envelopes r ON r.id=c.run_id
               WHERE t.root_task_id=root.id AND c.usage_complete=0
                 AND COALESCE((t.id<>owner.id AND t.parent_task_id IS NOT NULL
                   AND t.status IN ('partial','failed') AND t.reason='timeout'
                   AND t.cleanup_status='confirmed'
                   AND r.requested_exit_reason='timeout'
                   AND c.status='cancelled' AND c.error_code='STREAM_DROPPED'
                   AND EXISTS(SELECT 1 FROM task_results result
                     WHERE result.task_id=t.id AND result.run_id=r.id
                       AND result.error_code='SUBAGENT_DEADLINE_EXCEEDED')),0)=0)
        )",
        [run_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn read_llm_usage_integrity_in_current_read(
    conn: &Connection,
    task_id: &str,
    run_id: &str,
) -> Result<Option<LlmUsageIntegrity>, DbError> {
    conn.query_row(
        "SELECT run.usage_complete,task.usage_complete,root.usage_complete
         FROM run_envelopes run
         JOIN tasks task ON task.id=run.task_id AND task.current_run_id=run.id
         JOIN tasks root ON root.id=task.root_task_id
         WHERE run.id=?1 AND task.id=?2",
        params![run_id, task_id],
        |row| {
            Ok(LlmUsageIntegrity {
                run_usage_complete: row.get::<_, i64>(0)? != 0,
                task_usage_complete: row.get::<_, i64>(1)? != 0,
                root_task_usage_complete: row.get::<_, i64>(2)? != 0,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

struct LlmCallOwner {
    token_limit: Option<i64>,
    cost_limit_nanos_usd: Option<i64>,
    deadline_at_ms: Option<i64>,
    run_tokens: i64,
    run_cost_nanos_usd: i64,
    usage_integrity: LlmUsageIntegrity,
    task_status: String,
    run_status: String,
    is_root: bool,
    task_consumed_tokens: i64,
    task_consumed_cost_nanos_usd: i64,
    root_reserved_tokens: i64,
    root_reserved_cost_nanos_usd: i64,
    root_consumed_tokens: i64,
    root_consumed_cost_nanos_usd: i64,
}

impl Db {
    /// Read the three durable usage-completeness authorities for a Task's
    /// current Run. A missing row means the Run is not the Task's current Run.
    pub async fn read_llm_usage_integrity(
        &self,
        task_id: &str,
        run_id: &str,
    ) -> Result<Option<LlmUsageIntegrity>, DbError> {
        let task_id = task_id.to_owned();
        let run_id = run_id.to_owned();
        self.with_reader(move |conn| {
            read_llm_usage_integrity_in_current_read(conn, &task_id, &run_id)
        })
        .await
    }

    /// Fail closed unless the current Run, its owning Task, and the root budget
    /// Task all have complete usage.
    pub async fn assert_llm_usage_complete(
        &self,
        task_id: &str,
        run_id: &str,
    ) -> Result<(), DbError> {
        let task_id = task_id.to_owned();
        let run_id = run_id.to_owned();
        self.with_reader(move |conn| {
            let integrity = read_llm_usage_integrity_in_current_read(conn, &task_id, &run_id)?
                .ok_or_else(|| DbError::Invalid("LLM_RUN_NOT_OWNED".to_owned()))?;
            if !integrity.is_complete() && !timeout_usage_exception(conn, &run_id)? {
                return Err(DbError::Invalid("BUDGET_USAGE_INCOMPLETE".to_owned()));
            }
            Ok(())
        })
        .await
    }

    /// Fail closed after a provider response unless its exact durable usage is
    /// complete and still within the owning Task's hard token, cost, and time
    /// limits. This gate runs before any model-requested tool side effect.
    pub async fn assert_task_run_budget_within_limits(
        &self,
        task_id: &str,
        run_id: &str,
    ) -> Result<(), DbError> {
        let task_id = task_id.to_owned();
        let run_id = run_id.to_owned();
        self.with_reader(move |conn| {
            let owner: Option<LlmCallOwner> = conn
                .query_row(
                    "SELECT task.token_budget_limit,task.cost_budget_nanos_usd,
                            task.deadline_at_ms,run.total_tokens,run.cost_nanos_usd,
                            run.usage_complete,task.usage_complete,root.usage_complete,
                            task.status,run.status,task.parent_task_id IS NULL,
                            task.budget_consumed_tokens,
                            task.budget_consumed_cost_nanos_usd,
                            root.budget_reserved_tokens,root.budget_reserved_cost_nanos_usd,
                            root.budget_consumed_tokens,root.budget_consumed_cost_nanos_usd
                     FROM run_envelopes run
                     JOIN tasks task ON task.id=run.task_id AND task.current_run_id=run.id
                     JOIN tasks root ON root.id=task.root_task_id
                     WHERE run.id=?1 AND task.id=?2",
                    params![run_id, task_id],
                    |row| {
                        Ok(LlmCallOwner {
                            token_limit: row.get(0)?,
                            cost_limit_nanos_usd: row.get(1)?,
                            deadline_at_ms: row.get(2)?,
                            run_tokens: row.get(3)?,
                            run_cost_nanos_usd: row.get(4)?,
                            usage_integrity: LlmUsageIntegrity {
                                run_usage_complete: row.get::<_, i64>(5)? != 0,
                                task_usage_complete: row.get::<_, i64>(6)? != 0,
                                root_task_usage_complete: row.get::<_, i64>(7)? != 0,
                            },
                            task_status: row.get(8)?,
                            run_status: row.get(9)?,
                            is_root: row.get::<_, i64>(10)? != 0,
                            task_consumed_tokens: row.get(11)?,
                            task_consumed_cost_nanos_usd: row.get(12)?,
                            root_reserved_tokens: row.get(13)?,
                            root_reserved_cost_nanos_usd: row.get(14)?,
                            root_consumed_tokens: row.get(15)?,
                            root_consumed_cost_nanos_usd: row.get(16)?,
                        })
                    },
                )
                .optional()?;
            let Some(owner) = owner else {
                return Err(DbError::Invalid("LLM_RUN_NOT_OWNED".to_owned()));
            };
            if !owner.usage_integrity.is_complete() && !timeout_usage_exception(conn, &run_id)? {
                return Err(DbError::Invalid("BUDGET_USAGE_INCOMPLETE".to_owned()));
            }
            if owner
                .deadline_at_ms
                .is_some_and(|deadline| deadline <= now_millis())
            {
                return Err(DbError::Invalid("TASK_DEADLINE_EXCEEDED".to_owned()));
            }
            let prior_tokens = if owner.is_root {
                owner
                    .root_reserved_tokens
                    .saturating_add(owner.root_consumed_tokens)
            } else {
                owner.task_consumed_tokens
            };
            let prior_cost = if owner.is_root {
                owner
                    .root_reserved_cost_nanos_usd
                    .saturating_add(owner.root_consumed_cost_nanos_usd)
            } else {
                owner.task_consumed_cost_nanos_usd
            };
            if owner
                .token_limit
                .is_some_and(|limit| owner.run_tokens.saturating_add(prior_tokens) > limit)
            {
                return Err(DbError::Invalid("TOKEN_BUDGET_EXHAUSTED".to_owned()));
            }
            if owner
                .cost_limit_nanos_usd
                .is_some_and(|limit| owner.run_cost_nanos_usd.saturating_add(prior_cost) > limit)
            {
                return Err(DbError::Invalid("COST_BUDGET_EXHAUSTED".to_owned()));
            }
            Ok(())
        })
        .await
    }

    pub async fn start_llm_call(&self, call: &NewLlmCall) -> Result<(), DbError> {
        self.start_llm_call_inner(call, None).await
    }

    /// Atomically admits and reserves one physical provider attempt against the
    /// durable Task limits. Concurrent attempts cannot both spend the same remainder.
    pub async fn start_llm_call_with_budget(
        &self,
        call: &NewLlmCall,
        reservation: &LlmCallBudgetReservation,
    ) -> Result<(), DbError> {
        if reservation.input_tokens < 0
            || reservation.output_tokens <= 0
            || reservation.cost_nanos_usd < 0
        {
            return Err(DbError::Invalid(
                "LLM_BUDGET_RESERVATION_INVALID".to_owned(),
            ));
        }
        self.start_llm_call_inner(call, Some(reservation.clone()))
            .await
    }

    async fn start_llm_call_inner(
        &self,
        call: &NewLlmCall,
        reservation: Option<LlmCallBudgetReservation>,
    ) -> Result<(), DbError> {
        let call = call.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let owner: Option<LlmCallOwner> = tx
                .query_row(
                    "SELECT task.token_budget_limit,task.cost_budget_nanos_usd,
                            task.deadline_at_ms,run.total_tokens,run.cost_nanos_usd,
                            run.usage_complete,task.usage_complete,root.usage_complete,
                            task.status,run.status,
                            task.parent_task_id IS NULL,
                            task.budget_consumed_tokens,
                            task.budget_consumed_cost_nanos_usd,
                            root.budget_reserved_tokens,root.budget_reserved_cost_nanos_usd,
                            root.budget_consumed_tokens,root.budget_consumed_cost_nanos_usd
                     FROM run_envelopes run
                     JOIN tasks task ON task.id=run.task_id AND task.current_run_id=run.id
                     JOIN tasks root ON root.id=task.root_task_id
                     WHERE run.id=?1 AND run.task_id=?2",
                    params![call.run_id, call.task_id],
                    |row| {
                        Ok(LlmCallOwner {
                            token_limit: row.get(0)?,
                            cost_limit_nanos_usd: row.get(1)?,
                            deadline_at_ms: row.get(2)?,
                            run_tokens: row.get(3)?,
                            run_cost_nanos_usd: row.get(4)?,
                            usage_integrity: LlmUsageIntegrity {
                                run_usage_complete: row.get::<_, i64>(5)? != 0,
                                task_usage_complete: row.get::<_, i64>(6)? != 0,
                                root_task_usage_complete: row.get::<_, i64>(7)? != 0,
                            },
                            task_status: row.get(8)?,
                            run_status: row.get(9)?,
                            is_root: row.get::<_, i64>(10)? != 0,
                            task_consumed_tokens: row.get(11)?,
                            task_consumed_cost_nanos_usd: row.get(12)?,
                            root_reserved_tokens: row.get(13)?,
                            root_reserved_cost_nanos_usd: row.get(14)?,
                            root_consumed_tokens: row.get(15)?,
                            root_consumed_cost_nanos_usd: row.get(16)?,
                        })
                    },
                )
                .optional()?;
            let Some(owner) = owner else {
                return Err(DbError::Invalid("LLM_RUN_NOT_OWNED".to_owned()));
            };
            let usage_admissible =
                owner.usage_integrity.is_complete() || timeout_usage_exception(&tx, &call.run_id)?;
            if reservation.is_some() && !usage_admissible {
                return Err(DbError::Invalid("BUDGET_USAGE_INCOMPLETE".to_owned()));
            }
            if !matches!(
                owner.task_status.as_str(),
                "running" | "waitingDependencies"
            ) || !matches!(owner.run_status.as_str(), "running" | "waitingDependencies")
            {
                return Err(DbError::Invalid("LLM_RUN_NOT_ACTIVE".to_owned()));
            }
            if reservation.is_some() {
                if owner.deadline_at_ms.is_none() {
                    return Err(DbError::Invalid("LLM_BUDGET_NOT_CONFIGURED".to_owned()));
                }
                if owner
                    .deadline_at_ms
                    .is_some_and(|deadline| deadline <= now_millis())
                {
                    return Err(DbError::Invalid("TASK_DEADLINE_EXCEEDED".to_owned()));
                }
            } else if owner.token_limit.is_some()
                || owner.cost_limit_nanos_usd.is_some()
                || owner.deadline_at_ms.is_some()
            {
                return Err(DbError::Invalid(
                    "LLM_BUDGET_RESERVATION_REQUIRED".to_owned(),
                ));
            }
            let require_complete_usage = reservation.is_some();
            let reservation = reservation.unwrap_or_default();
            let active: (i64, i64) = tx.query_row(
                "SELECT COALESCE(SUM(reserved_input_tokens+reserved_output_tokens),0),
                        COALESCE(SUM(reserved_cost_nanos_usd),0)
                 FROM llm_calls WHERE run_id=?1 AND status='started'",
                params![call.run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let requested_tokens = reservation
                .input_tokens
                .saturating_add(reservation.output_tokens);
            let prior_token_charge = if owner.is_root {
                owner
                    .root_reserved_tokens
                    .saturating_add(owner.root_consumed_tokens)
            } else {
                owner.task_consumed_tokens
            };
            let prior_cost_charge = if owner.is_root {
                owner
                    .root_reserved_cost_nanos_usd
                    .saturating_add(owner.root_consumed_cost_nanos_usd)
            } else {
                owner.task_consumed_cost_nanos_usd
            };
            if owner.token_limit.is_some_and(|limit| {
                owner
                    .run_tokens
                    .saturating_add(prior_token_charge)
                    .saturating_add(active.0)
                    .saturating_add(requested_tokens)
                    > limit
            }) {
                return Err(DbError::Invalid("TOKEN_BUDGET_EXHAUSTED".to_owned()));
            }
            if owner.cost_limit_nanos_usd.is_some_and(|limit| {
                owner
                    .run_cost_nanos_usd
                    .saturating_add(prior_cost_charge)
                    .saturating_add(active.1)
                    .saturating_add(reservation.cost_nanos_usd)
                    > limit
            }) {
                return Err(DbError::Invalid("COST_BUDGET_EXHAUSTED".to_owned()));
            }
            let now = format_rfc3339_micros(now_millis());
            let inserted = tx.execute(
                "INSERT INTO llm_calls
                    (call_id,task_id,run_id,provider,model,route,provider_request_id,status,
                     reserved_input_tokens,reserved_output_tokens,reserved_cost_nanos_usd,
                     usage_complete,started_at,created_at,updated_at)
                 SELECT ?1,?2,?3,?4,?5,?6,?7,'started',?8,?9,?10,0,?11,?11,?11
                 WHERE EXISTS(
                    SELECT 1 FROM run_envelopes run
                    JOIN tasks task
                      ON task.id=run.task_id AND task.current_run_id=run.id
                    JOIN tasks root ON root.id=task.root_task_id
                    WHERE run.id=?3 AND task.id=?2
                      AND (?12=0 OR ?13=1)
                 )",
                params![
                    call.call_id,
                    call.task_id,
                    call.run_id,
                    call.provider,
                    call.model,
                    call.route,
                    call.provider_request_id,
                    reservation.input_tokens,
                    reservation.output_tokens,
                    reservation.cost_nanos_usd,
                    now,
                    require_complete_usage,
                    usage_admissible,
                ],
            )?;
            if inserted != 1 {
                if require_complete_usage
                    && read_llm_usage_integrity_in_current_read(&tx, &call.task_id, &call.run_id)?
                        .is_some_and(|integrity| !integrity.is_complete())
                {
                    return Err(DbError::Invalid("BUDGET_USAGE_INCOMPLETE".to_owned()));
                }
                return Err(DbError::Invalid("LLM_RUN_NOT_OWNED".to_owned()));
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn finish_llm_call(
        &self,
        call_id: &str,
        status: &str,
        usage: &LlmUsageCompletion,
    ) -> Result<CasOutcome, DbError> {
        if !matches!(status, "completed" | "failed" | "cancelled") {
            return Err(DbError::Invalid("LLM_CALL_STATUS_INVALID".to_owned()));
        }
        if usage.usage_complete
            && (usage.input_tokens.is_none()
                || usage.output_tokens.is_none()
                || usage.cache_read_tokens.is_none()
                || usage.cache_create_tokens.is_none())
        {
            return Err(DbError::Invalid("LLM_USAGE_INCOMPLETE".to_owned()));
        }
        let call_id = call_id.to_owned();
        let status = status.to_owned();
        let usage = usage.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let run_id: Option<String> = tx
                .query_row(
                    "SELECT run_id FROM llm_calls WHERE call_id=?1 AND status='started'",
                    params![call_id],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(run_id) = run_id else {
                let exists: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM llm_calls WHERE call_id=?1",
                    params![call_id],
                    |row| row.get(0),
                )?;
                return Ok(if exists == 0 {
                    CasOutcome::NotFound
                } else {
                    CasOutcome::InvalidTransition
                });
            };
            let now = format_rfc3339_micros(now_millis());
            tx.execute(
                "UPDATE llm_calls SET status=?1,input_tokens=?2,output_tokens=?3,
                    cache_read_tokens=?4,cache_create_tokens=?5,cost_nanos_usd=?6,
                    usage_complete=?7,error_code=?8,finished_at=?9,updated_at=?9
                 WHERE call_id=?10 AND status='started'",
                params![
                    status,
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.cache_read_tokens,
                    usage.cache_create_tokens,
                    usage.cost_nanos_usd,
                    usage.usage_complete,
                    usage.error_code,
                    now,
                    call_id,
                ],
            )?;
            tx.execute(
                "UPDATE run_envelopes SET
                    input_tokens=input_tokens+COALESCE(?1,0),
                    output_tokens=output_tokens+COALESCE(?2,0),
                    cache_read_tokens=cache_read_tokens+COALESCE(?3,0),
                    cache_create_tokens=cache_create_tokens+COALESCE(?4,0),
                    cost_nanos_usd=cost_nanos_usd+COALESCE(?5,0),
                    total_tokens=total_tokens+COALESCE(?1,0)+COALESCE(?2,0),
                    total_cost_usd=total_cost_usd+(COALESCE(?5,0)/1000000000.0),
                    usage_complete=CASE WHEN ?6 THEN usage_complete ELSE 0 END,
                    updated_at=?7 WHERE id=?8",
                params![
                    usage.input_tokens,
                    usage.output_tokens,
                    usage.cache_read_tokens,
                    usage.cache_create_tokens,
                    usage.cost_nanos_usd,
                    usage.usage_complete,
                    now,
                    run_id,
                ],
            )?;
            if !usage.usage_complete {
                tx.execute(
                    "UPDATE tasks SET usage_complete=0,updated_at=?1
                     WHERE id IN (
                       SELECT owner.id
                       FROM run_envelopes run
                       JOIN tasks owner ON owner.id=run.task_id
                       WHERE run.id=?2
                       UNION
                       SELECT owner.root_task_id
                       FROM run_envelopes run
                       JOIN tasks owner ON owner.id=run.task_id
                       WHERE run.id=?2
                     )",
                    params![now, run_id],
                )?;
            }
            tx.commit()?;
            Ok(CasOutcome::Applied)
        })
        .await
    }
}
