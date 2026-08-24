//! Durable project-context snapshots used by Coordinator prompts.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Db, DbError};

const MAX_SNAPSHOT_BYTES: usize = 256 * 1024;

/// Latest bounded context snapshot for one canonical working-directory hash.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectContextRecord {
    /// Stable record identifier.
    pub id: String,
    /// SHA-256 of the canonical working directory; raw paths are not stored here.
    pub working_dir_hash: String,
    /// Bounded structured project context.
    pub snapshot: Value,
    /// Optional externally supplied source revision; this repository never invokes Git.
    pub git_head_sha: Option<String>,
    /// RFC 3339 update time.
    pub updated_at: String,
}

impl Db {
    /// Upsert the latest context by canonical working-directory hash.
    ///
    /// # Errors
    /// Returns [`DbError`] for invalid bounded input or a database failure.
    pub async fn save_project_context(&self, record: &ProjectContextRecord) -> Result<(), DbError> {
        validate_record(record)?;
        let record = record.clone();
        self.with_writer(move |conn| {
            let snapshot_json = serde_json::to_string(&record.snapshot)?;
            conn.execute(
                "INSERT INTO project_context \
                 (id,working_dir_hash,snapshot_json,git_head_sha,updated_at) \
                 VALUES (?1,?2,?3,?4,?5) ON CONFLICT(working_dir_hash) DO UPDATE SET \
                 id=excluded.id,snapshot_json=excluded.snapshot_json,\
                 git_head_sha=excluded.git_head_sha,updated_at=excluded.updated_at",
                rusqlite::params![
                    record.id,
                    record.working_dir_hash,
                    snapshot_json,
                    record.git_head_sha,
                    record.updated_at,
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// Read the latest project context by canonical working-directory hash.
    ///
    /// # Errors
    /// Returns [`DbError`] when the query or stored JSON decoding fails.
    pub async fn find_project_context(
        &self,
        working_dir_hash: &str,
    ) -> Result<Option<ProjectContextRecord>, DbError> {
        let working_dir_hash = working_dir_hash.to_owned();
        self.with_reader(move |conn| {
            let mut statement = conn.prepare(
                "SELECT id,working_dir_hash,snapshot_json,git_head_sha,updated_at \
                 FROM project_context WHERE working_dir_hash=?1",
            )?;
            match statement.query_row([working_dir_hash], map_record) {
                Ok(record) => Ok(Some(record)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(error) => Err(error.into()),
            }
        })
        .await
    }
}

fn map_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProjectContextRecord> {
    let snapshot_json: String = row.get(2)?;
    let snapshot = serde_json::from_str(&snapshot_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(ProjectContextRecord {
        id: row.get(0)?,
        working_dir_hash: row.get(1)?,
        snapshot,
        git_head_sha: row.get(3)?,
        updated_at: row.get(4)?,
    })
}

fn validate_record(record: &ProjectContextRecord) -> Result<(), DbError> {
    let valid_hash = record.working_dir_hash.len() == 64
        && record
            .working_dir_hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit());
    let snapshot_size = serde_json::to_vec(&record.snapshot)?.len();
    if record.id.is_empty()
        || !valid_hash
        || record.updated_at.is_empty()
        || snapshot_size > MAX_SNAPSHOT_BYTES
    {
        return Err(DbError::Invalid("invalid project context record".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn latest_project_context_survives_reopen_without_git_access() {
        let directory = std::env::temp_dir().join(format!(
            "zkcode-project-context-restart-{}",
            uuid::Uuid::new_v4()
        ));
        let path = directory.join("data.db");
        let hash = "a".repeat(64);
        let db = Db::open(&path).expect("open database");
        db.save_project_context(&ProjectContextRecord {
            id: "context-1".into(),
            working_dir_hash: hash.clone(),
            snapshot: serde_json::json!({"summary": "first"}),
            git_head_sha: None,
            updated_at: "2026-08-22T00:00:00.000000Z".into(),
        })
        .await
        .expect("save first context");
        db.save_project_context(&ProjectContextRecord {
            id: "context-2".into(),
            working_dir_hash: hash.clone(),
            snapshot: serde_json::json!({"summary": "latest", "files": ["src/lib.rs"]}),
            git_head_sha: None,
            updated_at: "2026-08-22T00:01:00.000000Z".into(),
        })
        .await
        .expect("replace context");
        drop(db);

        let reopened = Db::open(&path).expect("reopen database");
        let persisted = reopened
            .find_project_context(&hash)
            .await
            .expect("read context")
            .expect("context exists");
        assert_eq!(persisted.id, "context-2");
        assert_eq!(persisted.snapshot["summary"], "latest");
        assert!(persisted.git_head_sha.is_none());
        drop(reopened);
        let _ = std::fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn project_context_requires_canonical_hash_and_bounded_snapshot() {
        let db = Db::open_in_memory().expect("database");
        let invalid = ProjectContextRecord {
            id: "context".into(),
            working_dir_hash: "raw/path".into(),
            snapshot: serde_json::json!({}),
            git_head_sha: None,
            updated_at: "now".into(),
        };
        assert!(db.save_project_context(&invalid).await.is_err());
    }
}
