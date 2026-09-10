//! Durable child-agent checkpoints stored in the primary `SQLite` database.

use std::fmt::Write as _;

use rusqlite::OptionalExtension;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::time::{format_rfc3339_micros, now_millis};
use crate::{Db, DbError};

/// Stored checkpoint projection.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentCheckpointRecord {
    /// Checkpoint identifier.
    pub id: String,
    /// Durable child run identifier.
    pub run_id: String,
    /// Child session identifier.
    pub session_id: String,
    /// Runtime agent identifier.
    pub agent_id: String,
    /// Monotonic checkpoint sequence within the run.
    pub seq: i64,
    /// Serialized conversation state.
    pub messages: Value,
    /// Optional file-state projection.
    pub file_state: Option<Value>,
    /// Number of completed tool calls.
    pub tool_call_count: i64,
    /// Number of completed LLM turns.
    pub turn_count: i64,
    /// Tokens consumed through this checkpoint.
    pub tokens_consumed: i64,
    /// Authorized workspace at checkpoint time.
    pub working_dir: Option<String>,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
}

impl Db {
    /// Insert or replace the unique `(run_id, seq)` checkpoint.
    ///
    /// # Errors
    /// Returns [`DbError`] when checkpoint JSON serialization or the `SQLite` write fails.
    pub async fn save_agent_checkpoint(
        &self,
        checkpoint: &AgentCheckpointRecord,
    ) -> Result<(), DbError> {
        let checkpoint = checkpoint.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let mut messages = checkpoint.messages.clone();
            if matches!(
                messages.get("kind").and_then(Value::as_str),
                Some("contextCheckpoint")
            ) {
                let owner: Option<CheckpointRecoveryOwner> = tx
                    .query_row(
                        "SELECT run.task_id,run.startup_epoch,task.parent_task_id,
                                task.task_type,task.execution_config_json,
                                task.token_budget_limit,task.cost_budget_nanos_usd,
                                task.deadline_at_ms,task.budget_consumed_tokens,
                                task.budget_consumed_cost_nanos_usd,task.usage_complete,
                                session.working_dir
                         FROM run_envelopes run
                         JOIN tasks task ON task.id=run.task_id
                         JOIN sessions session ON session.id=run.session_id
                         WHERE run.id=?1 AND run.session_id=?2",
                        rusqlite::params![checkpoint.run_id, checkpoint.session_id],
                        |row| {
                            Ok(CheckpointRecoveryOwner {
                                task_id: row.get(0)?,
                                startup_epoch: row.get(1)?,
                                parent_task_id: row.get(2)?,
                                task_type: row.get(3)?,
                                execution_config_json: row.get(4)?,
                                token_budget_limit: row.get(5)?,
                                cost_budget_nanos_usd: row.get(6)?,
                                deadline_at_ms: row.get(7)?,
                                consumed_tokens: row.get(8)?,
                                consumed_cost_nanos_usd: row.get(9)?,
                                usage_complete: row.get::<_, i64>(10)? != 0,
                                working_dir: row.get(11)?,
                            })
                        },
                    )
                    .optional()?;
                if let Some(owner) = owner {
                    let proof = recovery_proof(&checkpoint, &owner)?;
                    let object = messages.as_object_mut().ok_or_else(|| {
                        DbError::Invalid("CONTEXT_CHECKPOINT_NOT_OBJECT".to_owned())
                    })?;
                    object.insert("recoveryProof".to_owned(), proof);
                }
            }
            let messages_json = serde_json::to_string(&messages)?;
            let file_state_json = checkpoint
                .file_state
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            tx.execute(
                "INSERT INTO agent_checkpoints \
                 (id, run_id, session_id, agent_id, seq, messages_json, file_state_json, \
                  tool_call_count, turn_count, tokens_consumed, working_dir, created_at) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) \
                 ON CONFLICT(run_id, seq) DO UPDATE SET \
                   id=excluded.id, session_id=excluded.session_id, agent_id=excluded.agent_id, \
                   messages_json=excluded.messages_json, file_state_json=excluded.file_state_json, \
                   tool_call_count=excluded.tool_call_count, turn_count=excluded.turn_count, \
                   tokens_consumed=excluded.tokens_consumed, working_dir=excluded.working_dir, \
                   created_at=excluded.created_at",
                rusqlite::params![
                    checkpoint.id,
                    checkpoint.run_id,
                    checkpoint.session_id,
                    checkpoint.agent_id,
                    checkpoint.seq,
                    messages_json,
                    file_state_json,
                    checkpoint.tool_call_count,
                    checkpoint.turn_count,
                    checkpoint.tokens_consumed,
                    checkpoint.working_dir,
                    checkpoint.created_at,
                ],
            )?;
            let owned = tx.execute(
                "UPDATE run_envelopes SET checkpoint_id=?1, updated_at=?2 \
                 WHERE id=?3 AND session_id=?4",
                rusqlite::params![
                    checkpoint.id,
                    checkpoint.created_at,
                    checkpoint.run_id,
                    checkpoint.session_id,
                ],
            )?;
            if owned != 1 {
                return Err(DbError::Invalid(
                    "CHECKPOINT_RUN_OWNERSHIP_MISMATCH".to_owned(),
                ));
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Load the newest checkpoint for a run.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` query or checkpoint JSON decoding fails.
    pub async fn latest_agent_checkpoint(
        &self,
        run_id: &str,
    ) -> Result<Option<AgentCheckpointRecord>, DbError> {
        let run_id = run_id.to_owned();
        self.with_reader(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, run_id, session_id, agent_id, seq, messages_json, file_state_json, \
                 tool_call_count, turn_count, tokens_consumed, working_dir, created_at \
                 FROM agent_checkpoints WHERE run_id=?1 ORDER BY seq DESC LIMIT 1",
            )?;
            let mut rows = stmt.query([run_id])?;
            let Some(row) = rows.next()? else {
                return Ok(None);
            };
            let messages_json: String = row.get(5)?;
            let file_state_json: Option<String> = row.get(6)?;
            Ok(Some(AgentCheckpointRecord {
                id: row.get(0)?,
                run_id: row.get(1)?,
                session_id: row.get(2)?,
                agent_id: row.get(3)?,
                seq: row.get(4)?,
                messages: serde_json::from_str(&messages_json)?,
                file_state: file_state_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?,
                tool_call_count: row.get(7)?,
                turn_count: row.get(8)?,
                tokens_consumed: row.get(9)?,
                working_dir: row.get(10)?,
                created_at: row.get(11)?,
            }))
        })
        .await
    }
}

#[derive(Debug)]
struct CheckpointRecoveryOwner {
    task_id: String,
    startup_epoch: i64,
    parent_task_id: Option<String>,
    task_type: String,
    execution_config_json: String,
    token_budget_limit: Option<i64>,
    cost_budget_nanos_usd: Option<i64>,
    deadline_at_ms: Option<i64>,
    consumed_tokens: i64,
    consumed_cost_nanos_usd: i64,
    usage_complete: bool,
    working_dir: String,
}

fn recovery_proof(
    checkpoint: &AgentCheckpointRecord,
    owner: &CheckpointRecoveryOwner,
) -> Result<Value, DbError> {
    let execution_config: Value = serde_json::from_str(&owner.execution_config_json)?;
    let allowed_tools = execution_config.get("allowedTools");
    let permission_policy = json!({
        "version": execution_config.get("permissionPolicyVersion"),
        "isolation": execution_config.get("isolation"),
        "allowWriteTools": execution_config.get("allowWriteTools"),
        "allowedTools": allowed_tools,
    });
    let permission_policy_json = serde_json::to_vec(&permission_policy)?;
    let supported = owner.task_type == "agent"
        && owner.parent_task_id.is_some()
        && execution_config
            .get("permissionPolicyVersion")
            .and_then(Value::as_u64)
            == Some(1)
        && execution_config.get("isolation").and_then(Value::as_str) == Some("readOnly")
        && execution_config
            .get("allowWriteTools")
            .and_then(Value::as_bool)
            == Some(false)
        && allowed_tools.is_some_and(Value::is_array)
        && checkpoint.working_dir.as_deref() == Some(owner.working_dir.as_str());
    Ok(json!({
        "proofVersion": 1,
        "supported": supported,
        "taskId": owner.task_id,
        "runId": checkpoint.run_id,
        "sessionId": checkpoint.session_id,
        "startupEpoch": owner.startup_epoch,
        "executionConfigSha256": sha256_hex(owner.execution_config_json.as_bytes()),
        "permissionFingerprint": sha256_hex(&permission_policy_json),
        "workspaceBindingSha256": sha256_hex(owner.working_dir.as_bytes()),
        "budget": {
            "tokenLimit": owner.token_budget_limit,
            "costLimitNanosUsd": owner.cost_budget_nanos_usd,
            "deadlineAtMs": owner.deadline_at_ms,
            "consumedTokens": owner.consumed_tokens,
            "consumedCostNanosUsd": owner.consumed_cost_nanos_usd,
            "usageComplete": owner.usage_complete,
        },
    }))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

/// Build a checkpoint with a generated id and current timestamp.
#[must_use]
pub fn new_agent_checkpoint(
    run_id: &str,
    session_id: &str,
    agent_id: &str,
    seq: i64,
    messages: Value,
) -> AgentCheckpointRecord {
    AgentCheckpointRecord {
        id: uuid::Uuid::new_v4().to_string(),
        run_id: run_id.to_owned(),
        session_id: session_id.to_owned(),
        agent_id: agent_id.to_owned(),
        seq,
        messages,
        file_state: None,
        tool_call_count: 0,
        turn_count: 0,
        tokens_consumed: 0,
        working_dir: None,
        created_at: format_rfc3339_micros(now_millis()),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn latest_checkpoint_is_durable_and_idempotent_per_sequence() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("model", "/tmp/checkpoint")
            .await
            .expect("session");
        db.start_run("run-1", &session.id, None, Some("query"), "model")
            .await
            .expect("run");
        let mut first = new_agent_checkpoint("run-1", &session.id, "agent-1", 1, json!(["a"]));
        db.save_agent_checkpoint(&first).await.expect("save first");
        first.messages = json!(["replaced"]);
        db.save_agent_checkpoint(&first)
            .await
            .expect("replace first");
        let second = new_agent_checkpoint("run-1", &session.id, "agent-1", 2, json!(["b"]));
        db.save_agent_checkpoint(&second)
            .await
            .expect("save second");

        let loaded = db
            .latest_agent_checkpoint("run-1")
            .await
            .expect("load")
            .expect("checkpoint");
        assert_eq!(loaded.seq, 2);
        assert_eq!(loaded.messages, json!(["b"]));
        assert_eq!(
            db.find_run_by_id("run-1")
                .await
                .expect("run lookup")
                .expect("run exists")
                .checkpoint_id
                .as_deref(),
            Some(second.id.as_str())
        );
        assert!(
            db.latest_agent_checkpoint("missing")
                .await
                .expect("missing query")
                .is_none()
        );
    }
}
