//! Durable Coordinator/Swarm anomaly event repository.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Db, DbError};

const MAX_MESSAGE_BYTES: usize = 16 * 1024;
const MAX_CONTEXT_BYTES: usize = 64 * 1024;

/// One durable anomaly detected while coordinating a Swarm worker.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnomalyEventRecord {
    /// Stable event identifier.
    pub id: String,
    /// Runtime Swarm identifier retained for historical lookup.
    pub swarm_id: String,
    /// Runtime worker identifier.
    pub worker_id: String,
    /// Stable detector rule identifier.
    pub rule_id: String,
    /// `info`, `warning`, `error`, or `critical`.
    pub severity: String,
    /// Redacted human-readable description.
    pub message: String,
    /// Detection time as Unix milliseconds.
    pub detected_at: i64,
    /// Resolution time as Unix milliseconds.
    pub resolved_at: Option<i64>,
    /// Redacted resolution summary.
    pub resolution: Option<String>,
    /// Bounded structured diagnostic context.
    pub context_snapshot: Option<Value>,
}

impl Db {
    /// Insert one immutable detection event.
    ///
    /// # Errors
    /// Returns [`DbError`] for invalid bounded input or a database failure.
    pub async fn save_anomaly_event(&self, event: &AnomalyEventRecord) -> Result<(), DbError> {
        validate_event(event)?;
        let event = event.clone();
        self.with_writer(move |conn| {
            let context = event
                .context_snapshot
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            conn.execute(
                "INSERT INTO anomaly_events \
                 (id,swarm_id,worker_id,rule_id,severity,message,detected_at,resolved_at,\
                  resolution,context_snapshot) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                rusqlite::params![
                    event.id,
                    event.swarm_id,
                    event.worker_id,
                    event.rule_id,
                    event.severity,
                    event.message,
                    event.detected_at,
                    event.resolved_at,
                    event.resolution,
                    context,
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// Resolve an event exactly once. Returns false when missing or already resolved.
    ///
    /// # Errors
    /// Returns [`DbError`] for an invalid resolution or a database failure.
    pub async fn resolve_anomaly_event(
        &self,
        id: &str,
        resolution: &str,
        resolved_at: i64,
    ) -> Result<bool, DbError> {
        if resolution.trim().is_empty() || resolution.len() > MAX_MESSAGE_BYTES {
            return Err(DbError::Invalid("invalid anomaly resolution".into()));
        }
        let id = id.to_owned();
        let resolution = resolution.to_owned();
        self.with_writer(move |conn| {
            Ok(conn.execute(
                "UPDATE anomaly_events SET resolved_at=?1,resolution=?2 \
                 WHERE id=?3 AND resolved_at IS NULL",
                rusqlite::params![resolved_at, resolution, id],
            )? > 0)
        })
        .await
    }

    /// Read all events for a Swarm in deterministic detection order.
    ///
    /// # Errors
    /// Returns [`DbError`] when the query or stored JSON decoding fails.
    pub async fn find_anomalies_by_swarm(
        &self,
        swarm_id: &str,
    ) -> Result<Vec<AnomalyEventRecord>, DbError> {
        let swarm_id = swarm_id.to_owned();
        self.with_reader(move |conn| {
            let mut statement = conn.prepare(
                "SELECT id,swarm_id,worker_id,rule_id,severity,message,detected_at,\
                 resolved_at,resolution,context_snapshot FROM anomaly_events \
                 WHERE swarm_id=?1 ORDER BY detected_at ASC,id ASC",
            )?;
            let rows = statement.query_map([swarm_id], map_event)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await
    }

    /// Read unresolved events, newest first, with a hard query limit.
    ///
    /// # Errors
    /// Returns [`DbError`] when the query or stored JSON decoding fails.
    pub async fn find_unresolved_anomalies(
        &self,
        limit: usize,
    ) -> Result<Vec<AnomalyEventRecord>, DbError> {
        let limit = i64::try_from(limit.clamp(1, 256))
            .map_err(|_| DbError::Invalid("invalid anomaly query limit".into()))?;
        self.with_reader(move |conn| {
            let mut statement = conn.prepare(
                "SELECT id,swarm_id,worker_id,rule_id,severity,message,detected_at,\
                 resolved_at,resolution,context_snapshot FROM anomaly_events \
                 WHERE resolved_at IS NULL ORDER BY detected_at DESC,id DESC LIMIT ?1",
            )?;
            let rows = statement.query_map([limit], map_event)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await
    }
}

fn map_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<AnomalyEventRecord> {
    let context: Option<String> = row.get(9)?;
    let context_snapshot = context
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                9,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
    Ok(AnomalyEventRecord {
        id: row.get(0)?,
        swarm_id: row.get(1)?,
        worker_id: row.get(2)?,
        rule_id: row.get(3)?,
        severity: row.get(4)?,
        message: row.get(5)?,
        detected_at: row.get(6)?,
        resolved_at: row.get(7)?,
        resolution: row.get(8)?,
        context_snapshot,
    })
}

fn validate_event(event: &AnomalyEventRecord) -> Result<(), DbError> {
    if event.id.is_empty()
        || event.swarm_id.is_empty()
        || event.worker_id.is_empty()
        || event.rule_id.is_empty()
        || event.message.trim().is_empty()
        || event.message.len() > MAX_MESSAGE_BYTES
        || !matches!(
            event.severity.as_str(),
            "info" | "warning" | "error" | "critical"
        )
    {
        return Err(DbError::Invalid("invalid anomaly event".into()));
    }
    if event.context_snapshot.as_ref().is_some_and(|value| {
        serde_json::to_vec(value).map_or(true, |bytes| bytes.len() > MAX_CONTEXT_BYTES)
    }) {
        return Err(DbError::Invalid("anomaly context exceeds 64 KiB".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn anomaly_detection_resolution_survives_reopen() {
        let directory =
            std::env::temp_dir().join(format!("zkcode-anomaly-restart-{}", uuid::Uuid::new_v4()));
        let path = directory.join("data.db");
        let event = AnomalyEventRecord {
            id: "anomaly-1".into(),
            swarm_id: "swarm-1".into(),
            worker_id: "worker-1".into(),
            rule_id: "worker-stalled".into(),
            severity: "warning".into(),
            message: "worker made no progress".into(),
            detected_at: 100,
            resolved_at: None,
            resolution: None,
            context_snapshot: Some(serde_json::json!({"progress": 0.0})),
        };

        let db = Db::open(&path).expect("open database");
        db.save_anomaly_event(&event).await.expect("save event");
        assert_eq!(db.find_unresolved_anomalies(10).await.unwrap(), vec![event]);
        assert!(
            db.resolve_anomaly_event("anomaly-1", "worker cancelled", 200)
                .await
                .expect("resolve event")
        );
        assert!(db.find_unresolved_anomalies(10).await.unwrap().is_empty());
        drop(db);

        let reopened = Db::open(&path).expect("reopen database");
        let persisted = reopened
            .find_anomalies_by_swarm("swarm-1")
            .await
            .expect("read persisted event");
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].resolved_at, Some(200));
        assert_eq!(persisted[0].resolution.as_deref(), Some("worker cancelled"));
        drop(reopened);
        let _ = std::fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn anomaly_payload_limits_fail_closed() {
        let db = Db::open_in_memory().expect("database");
        let oversized = AnomalyEventRecord {
            id: "a".into(),
            swarm_id: "s".into(),
            worker_id: "w".into(),
            rule_id: "r".into(),
            severity: "unknown".into(),
            message: "message".into(),
            detected_at: 1,
            resolved_at: None,
            resolution: None,
            context_snapshot: None,
        };
        assert!(db.save_anomaly_event(&oversized).await.is_err());
    }
}
