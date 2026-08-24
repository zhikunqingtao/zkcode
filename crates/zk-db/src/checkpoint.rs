//! Durable child-agent checkpoints stored in the primary `SQLite` database.

use serde_json::Value;

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
            let messages_json = serde_json::to_string(&checkpoint.messages)?;
            let file_state_json = checkpoint
                .file_state
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            conn.execute(
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
        let mut first = new_agent_checkpoint("run-1", "session-1", "agent-1", 1, json!(["a"]));
        db.save_agent_checkpoint(&first).await.expect("save first");
        first.messages = json!(["replaced"]);
        db.save_agent_checkpoint(&first)
            .await
            .expect("replace first");
        let second = new_agent_checkpoint("run-1", "session-1", "agent-1", 2, json!(["b"]));
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
        assert!(
            db.latest_agent_checkpoint("missing")
                .await
                .expect("missing query")
                .is_none()
        );
    }
}
