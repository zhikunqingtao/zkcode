//! Durable Swarm history backed by the existing `activities` business table.
//!
//! The live scheduler remains process-local, while this projection makes completed
//! history queryable after restart and lets startup close any previously-active Swarm
//! as `INTERRUPTED`.  No second database or extra business table is introduced.

use crate::Db;
use crate::error::DbError;
use crate::time::{format_rfc3339_micros, now_millis};

const OPERATION_TYPE: &str = "swarm_state";

/// Restart-safe REST projection for one Swarm.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SwarmRecord {
    /// Stable Swarm/team identifier.
    pub swarm_id: String,
    /// Authorized parent session.
    pub session_id: String,
    /// Configured worker capacity.
    pub max_workers: usize,
    /// Durable lifecycle phase.
    pub phase: String,
    /// Dispatched worker count.
    pub total_tasks: usize,
    /// Currently running worker count.
    pub active_workers: usize,
    /// Successfully completed worker count.
    pub completed_tasks: usize,
    /// Original creation timestamp.
    pub created_at: String,
    /// Last lifecycle update timestamp.
    pub updated_at: String,
}

impl SwarmRecord {
    /// Construct the initial `CREATED` projection.
    #[must_use]
    pub fn created(swarm_id: &str, session_id: &str, max_workers: usize) -> Self {
        let now = format_rfc3339_micros(now_millis());
        Self {
            swarm_id: swarm_id.to_owned(),
            session_id: session_id.to_owned(),
            max_workers,
            phase: "CREATED".to_owned(),
            total_tasks: 0,
            active_workers: 0,
            completed_tasks: 0,
            created_at: now.clone(),
            updated_at: now,
        }
    }
}

impl Db {
    /// Insert or update one durable Swarm projection.
    ///
    /// # Errors
    /// Returns [`DbError`] when serialization or the `SQLite` write fails.
    pub async fn save_swarm(&self, record: &SwarmRecord) -> Result<(), DbError> {
        let mut record = record.clone();
        record.updated_at = format_rfc3339_micros(now_millis());
        let payload = serde_json::to_string(&record)
            .map_err(|error| DbError::Invalid(format!("invalid Swarm projection: {error}")))?;
        self.with_writer(move |conn| {
            conn.execute(
                "INSERT INTO activities \
                 (id,session_id,operation_type,summary,status,timestamp,file_count,\
                  tool_result_json,created_at,updated_at) \
                 VALUES (?1,?2,?3,?4,?5,?6,0,?7,?8,?9) \
                 ON CONFLICT(id) DO UPDATE SET \
                    session_id=excluded.session_id,summary=excluded.summary,\
                    status=excluded.status,timestamp=excluded.timestamp,\
                    tool_result_json=excluded.tool_result_json,updated_at=excluded.updated_at",
                rusqlite::params![
                    format!("swarm:{}", record.swarm_id),
                    record.session_id,
                    OPERATION_TYPE,
                    format!("Swarm {}", record.swarm_id),
                    record.phase,
                    now_millis(),
                    payload,
                    record.created_at,
                    record.updated_at,
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// Find one Swarm projection by stable identifier.
    ///
    /// # Errors
    /// Returns [`DbError`] when `SQLite` or stored JSON is invalid.
    pub async fn find_swarm(&self, swarm_id: &str) -> Result<Option<SwarmRecord>, DbError> {
        let id = format!("swarm:{swarm_id}");
        self.with_reader(move |conn| {
            let payload = conn
                .query_row(
                    "SELECT tool_result_json FROM activities \
                     WHERE id=?1 AND operation_type=?2",
                    rusqlite::params![id, OPERATION_TYPE],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            payload.map(|json| parse_record(&json)).transpose()
        })
        .await
    }

    /// List all persisted Swarms, newest first.
    ///
    /// # Errors
    /// Returns [`DbError`] when `SQLite` or any stored JSON is invalid.
    pub async fn list_swarms(&self) -> Result<Vec<SwarmRecord>, DbError> {
        self.with_reader(move |conn| {
            let mut statement = conn.prepare(
                "SELECT tool_result_json FROM activities WHERE operation_type=?1 \
                 ORDER BY timestamp DESC,id DESC",
            )?;
            let rows = statement.query_map([OPERATION_TYPE], |row| row.get::<_, String>(0))?;
            let mut records = Vec::new();
            for row in rows {
                records.push(parse_record(&row?)?);
            }
            Ok(records)
        })
        .await
    }

    /// Delete one durable Swarm projection. The operation is idempotent.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` delete fails.
    pub async fn delete_swarm(&self, swarm_id: &str) -> Result<bool, DbError> {
        let id = format!("swarm:{swarm_id}");
        self.with_writer(move |conn| {
            let changed = conn.execute(
                "DELETE FROM activities WHERE id=?1 AND operation_type=?2",
                rusqlite::params![id, OPERATION_TYPE],
            )?;
            Ok(changed > 0)
        })
        .await
    }

    /// Mark Swarms left active by an earlier process as `INTERRUPTED`.
    ///
    /// # Errors
    /// Returns [`DbError`] when reading or updating the durable projections fails.
    pub async fn interrupt_active_swarms(&self) -> Result<usize, DbError> {
        let records = self.list_swarms().await?;
        let mut changed = 0;
        for mut record in records {
            if matches!(
                record.phase.as_str(),
                "CREATED" | "RUNNING" | "ABORTING" | "SHUTTING_DOWN"
            ) {
                record.phase = String::from("INTERRUPTED");
                record.active_workers = 0;
                self.save_swarm(&record).await?;
                changed += 1;
            }
        }
        Ok(changed)
    }
}

fn parse_record(payload: &str) -> Result<SwarmRecord, DbError> {
    serde_json::from_str(payload)
        .map_err(|error| DbError::Invalid(format!("invalid persisted Swarm projection: {error}")))
}

use rusqlite::OptionalExtension as _;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn history_survives_reopen_and_active_state_is_interrupted() {
        let path = std::env::temp_dir().join(format!(
            "zk-swarm-history-real-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let db = Db::open(&path).expect("open file db");
        let session = db
            .create_session("model", "/tmp/zk-swarm-history")
            .await
            .expect("session");
        let mut record = SwarmRecord::created("durable", &session.id, 2);
        record.phase = String::from("RUNNING");
        record.total_tasks = 2;
        record.active_workers = 2;
        db.save_swarm(&record).await.expect("save");
        drop(db);

        let reopened = Db::open(&path).expect("reopen same file");
        assert_eq!(
            reopened.interrupt_active_swarms().await.expect("recover"),
            1
        );
        let recovered = reopened
            .find_swarm("durable")
            .await
            .expect("query")
            .expect("record");
        assert_eq!(recovered.phase, "INTERRUPTED");
        assert_eq!(recovered.active_workers, 0);
        assert_eq!(recovered.total_tasks, 2);
    }
}
