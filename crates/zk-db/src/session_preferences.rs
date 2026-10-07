//! Session-local execution preferences; unrelated session metadata stays intact.
use crate::{Db, DbError};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

/// Persistent opt-in chat settings; absence preserves native defaults.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionExecutionPreferences {
    /// Monotonic compare-and-swap revision.
    pub revision: u64,
    /// Explicit reasoning strength, or None for the model's existing policy.
    pub effort: Option<String>,
    /// Use the explicitly configured fast model for ordinary chat.
    pub fast: bool,
}

fn decode(
    raw: Option<String>,
) -> Result<(serde_json::Value, SessionExecutionPreferences), DbError> {
    let metadata: serde_json::Value = raw.map_or_else(
        || Ok(serde_json::json!({})),
        |raw| serde_json::from_str(&raw),
    )?;
    if !metadata.is_object() {
        return Err(DbError::Invalid(
            "session metadata must be an object".into(),
        ));
    }
    let preferences = metadata.get("executionPreferences").cloned().map_or_else(
        || Ok(SessionExecutionPreferences::default()),
        serde_json::from_value,
    )?;
    Ok((metadata, preferences))
}

impl Db {
    /// Load only this session's preferences, retaining corrupt-state errors.
    /// # Errors
    /// Returns missing-session, storage, or malformed-metadata errors.
    pub async fn session_execution_preferences(
        &self,
        session_id: &str,
    ) -> Result<SessionExecutionPreferences, DbError> {
        let id = session_id.to_owned();
        self.with_reader(move |conn| {
            let raw = conn
                .query_row(
                    "SELECT metadata_json FROM sessions WHERE id=?1",
                    [&id],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()?
                .ok_or_else(|| DbError::SessionNotFound(id.clone()))?;
            Ok(decode(crate::content::load_optional(conn, &id, raw)?)?.1)
        })
        .await
    }

    /// Replace a validated pair atomically; never overwrites a newer preference.
    /// # Errors
    /// Returns conflict, missing-session, malformed metadata, or storage failures.
    pub async fn set_session_execution_preferences(
        &self,
        session_id: &str,
        expected_revision: u64,
        mut value: SessionExecutionPreferences,
    ) -> Result<SessionExecutionPreferences, DbError> {
        let id = session_id.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let raw = tx
                .query_row(
                    "SELECT metadata_json FROM sessions WHERE id=?1",
                    [&id],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()?
                .ok_or_else(|| DbError::SessionNotFound(id.clone()))?;
            let (mut metadata, prior) = decode(crate::content::load_optional(&tx, &id, raw)?)?;
            if prior.revision != expected_revision {
                return Err(DbError::Conflict(
                    "SESSION_EXECUTION_PREFERENCES_CHANGED".into(),
                ));
            }
            value.revision = prior
                .revision
                .checked_add(1)
                .ok_or_else(|| DbError::Invalid("preference revision exhausted".into()))?;
            metadata["executionPreferences"] = serde_json::to_value(&value)?;
            let sealed = crate::content::store_text(&tx, &id, &metadata.to_string())?;
            tx.execute(
                "UPDATE sessions SET metadata_json=?2 WHERE id=?1",
                params![id, sealed],
            )?;
            tx.commit()?;
            Ok(value)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn preferences_are_session_scoped_atomic_and_preserve_metadata() {
        let db = Db::open_in_memory().unwrap();
        let first = db
            .create_session("deepseek-flash", "/tmp")
            .await
            .unwrap()
            .id;
        let second = db
            .create_session("deepseek-flash", "/tmp")
            .await
            .unwrap()
            .id;
        let id = first.clone();
        db.with_writer(move |conn| {
            conn.execute(
                r#"UPDATE sessions SET metadata_json='{"keep":42}' WHERE id=?1"#,
                [&id],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        let saved = db
            .set_session_execution_preferences(
                &first,
                0,
                SessionExecutionPreferences {
                    revision: 0,
                    effort: Some("low".into()),
                    fast: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(saved.revision, 1);
        assert!(matches!(
            db.set_session_execution_preferences(&first, 0, SessionExecutionPreferences::default())
                .await,
            Err(DbError::Conflict(_))
        ));
        assert_eq!(
            db.session_execution_preferences(&first).await.unwrap(),
            saved
        );
        assert_eq!(
            db.session_execution_preferences(&second).await.unwrap(),
            SessionExecutionPreferences::default()
        );
        db.with_reader(move |conn| {
            assert_eq!(
                conn.query_row(
                    "SELECT json_extract(metadata_json,'$.keep') FROM sessions WHERE id=?1",
                    [&first],
                    |row| row.get::<_, i64>(0)
                )?,
                42
            );
            Ok(())
        })
        .await
        .unwrap();
    }
}
