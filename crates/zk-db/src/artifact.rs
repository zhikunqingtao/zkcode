//! Artifact manifest repository backed by the primary `SQLite` database.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Db, DbError};

/// One declared artifact path.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactEntryRecord {
    /// Artifact identifier.
    pub artifact_id: String,
    /// Tool call that declared the file.
    pub tool_use_id: String,
    /// Canonical absolute file path.
    pub canonical_path: String,
    /// `created`, `modified` or `deleted`.
    pub operation: String,
    /// Entry lifecycle state.
    pub state: String,
    /// Digest captured when sealed.
    pub sealed_hash: Option<String>,
    /// Digest observed by the latest verification.
    pub actual_hash: Option<String>,
    /// Observed file size.
    pub file_size: Option<i64>,
    /// Required validator identifier.
    pub required_validator_id: Option<String>,
    /// Validator result.
    pub validator_result: Option<Value>,
    /// Stable verification failure code.
    pub failure_code: Option<String>,
    /// RFC 3339 creation time.
    pub created_at: String,
    /// RFC 3339 update time.
    pub updated_at: String,
}

/// Run-scoped artifact manifest.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactManifestRecord {
    /// Manifest identifier.
    pub manifest_id: String,
    /// Owning root/child run.
    pub run_id: String,
    /// Owning session.
    pub session_id: String,
    /// Canonical authorized workspace.
    pub workspace_root: String,
    /// Aggregate lifecycle state.
    pub state: String,
    /// RFC 3339 creation time.
    pub created_at: String,
    /// RFC 3339 update time.
    pub updated_at: String,
    /// Stable artifact entries.
    pub entries: Vec<ArtifactEntryRecord>,
}

impl Db {
    /// Upsert a complete manifest and its entries in one transaction.
    ///
    /// # Errors
    /// Returns [`DbError`] when serialization or the `SQLite` transaction fails.
    pub async fn save_artifact_manifest(
        &self,
        manifest: &ArtifactManifestRecord,
    ) -> Result<(), DbError> {
        let manifest = manifest.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT INTO artifact_manifests \
                 (manifest_id,run_id,session_id,workspace_root,state,created_at,updated_at) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7) \
                 ON CONFLICT(manifest_id) DO UPDATE SET run_id=excluded.run_id, \
                 session_id=excluded.session_id, workspace_root=excluded.workspace_root, \
                 state=excluded.state, updated_at=excluded.updated_at",
                rusqlite::params![
                    manifest.manifest_id,
                    manifest.run_id,
                    manifest.session_id,
                    manifest.workspace_root,
                    manifest.state,
                    manifest.created_at,
                    manifest.updated_at,
                ],
            )?;
            tx.execute(
                "DELETE FROM artifact_entries WHERE manifest_id=?1",
                [&manifest.manifest_id],
            )?;
            for entry in manifest.entries {
                let validator_result_json = entry
                    .validator_result
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?;
                tx.execute(
                    "INSERT INTO artifact_entries \
                     (artifact_id,manifest_id,tool_use_id,canonical_path,operation,state,sealed_hash, \
                      actual_hash,file_size,required_validator_id,validator_result_json,failure_code, \
                      created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                    rusqlite::params![
                        entry.artifact_id,
                        manifest.manifest_id,
                        entry.tool_use_id,
                        entry.canonical_path,
                        entry.operation,
                        entry.state,
                        entry.sealed_hash,
                        entry.actual_hash,
                        entry.file_size,
                        entry.required_validator_id,
                        validator_result_json,
                        entry.failure_code,
                        entry.created_at,
                        entry.updated_at,
                    ],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Find the manifest associated with a run.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` query or stored JSON decoding fails.
    pub async fn find_artifact_manifest_by_run(
        &self,
        run_id: &str,
    ) -> Result<Option<ArtifactManifestRecord>, DbError> {
        let run_id = run_id.to_owned();
        self.with_reader(move |conn| {
            let id = conn
                .query_row(
                    "SELECT manifest_id FROM artifact_manifests WHERE run_id=?1",
                    [run_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            id.map_or(Ok(None), |id| load_manifest(conn, &id))
        })
        .await
    }

    /// Find a manifest by primary key.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` query or stored JSON decoding fails.
    pub async fn find_artifact_manifest(
        &self,
        manifest_id: &str,
    ) -> Result<Option<ArtifactManifestRecord>, DbError> {
        let manifest_id = manifest_id.to_owned();
        self.with_reader(move |conn| load_manifest(conn, &manifest_id))
            .await
    }

    /// Update aggregate manifest state.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` update fails.
    pub async fn update_artifact_manifest_state(
        &self,
        manifest_id: &str,
        state: &str,
    ) -> Result<bool, DbError> {
        let manifest_id = manifest_id.to_owned();
        let state = state.to_owned();
        self.with_writer(move |conn| {
            let now = crate::time::format_rfc3339_micros(crate::time::now_millis());
            Ok(conn.execute(
                "UPDATE artifact_manifests SET state=?1,updated_at=?2 WHERE manifest_id=?3",
                rusqlite::params![state, now, manifest_id],
            )? > 0)
        })
        .await
    }
}

use rusqlite::OptionalExtension;

fn load_manifest(
    conn: &rusqlite::Connection,
    manifest_id: &str,
) -> Result<Option<ArtifactManifestRecord>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT manifest_id,run_id,session_id,workspace_root,state,created_at,updated_at \
         FROM artifact_manifests WHERE manifest_id=?1",
    )?;
    let Some(mut manifest) = stmt
        .query_row([manifest_id], |row| {
            Ok(ArtifactManifestRecord {
                manifest_id: row.get(0)?,
                run_id: row.get(1)?,
                session_id: row.get(2)?,
                workspace_root: row.get(3)?,
                state: row.get(4)?,
                created_at: row.get(5)?,
                updated_at: row.get(6)?,
                entries: Vec::new(),
            })
        })
        .optional()?
    else {
        return Ok(None);
    };
    let mut entry_stmt = conn.prepare(
        "SELECT artifact_id,tool_use_id,canonical_path,operation,state,sealed_hash,actual_hash, \
         file_size,required_validator_id,validator_result_json,failure_code,created_at,updated_at \
         FROM artifact_entries WHERE manifest_id=?1 ORDER BY canonical_path ASC",
    )?;
    let rows = entry_stmt.query_map([manifest_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<i64>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, String>(11)?,
            row.get::<_, String>(12)?,
        ))
    })?;
    for row in rows {
        let (
            artifact_id,
            tool_use_id,
            canonical_path,
            operation,
            state,
            sealed_hash,
            actual_hash,
            file_size,
            required_validator_id,
            validator_result_json,
            failure_code,
            created_at,
            updated_at,
        ) = row?;
        manifest.entries.push(ArtifactEntryRecord {
            artifact_id,
            tool_use_id,
            canonical_path,
            operation,
            state,
            sealed_hash,
            actual_hash,
            file_size,
            required_validator_id,
            validator_result: validator_result_json
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?,
            failure_code,
            created_at,
            updated_at,
        });
    }
    Ok(Some(manifest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn manifest_round_trip_by_run() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("model", "/tmp/artifacts")
            .await
            .expect("session");
        db.start_run("run-a", &session.id, None, Some("query"), "model")
            .await
            .expect("run");
        let now = "2026-08-21T00:00:00.000000Z".to_owned();
        let manifest = ArtifactManifestRecord {
            manifest_id: "manifest-a".into(),
            run_id: "run-a".into(),
            session_id: session.id,
            workspace_root: "/tmp/artifacts".into(),
            state: "open".into(),
            created_at: now.clone(),
            updated_at: now.clone(),
            entries: vec![ArtifactEntryRecord {
                artifact_id: "artifact-a".into(),
                tool_use_id: "tool-a".into(),
                canonical_path: "/tmp/artifacts/a.txt".into(),
                operation: "created".into(),
                state: "declared".into(),
                sealed_hash: None,
                actual_hash: None,
                file_size: None,
                required_validator_id: None,
                validator_result: None,
                failure_code: None,
                created_at: now.clone(),
                updated_at: now,
            }],
        };
        db.save_artifact_manifest(&manifest).await.expect("save");
        let loaded = db
            .find_artifact_manifest_by_run("run-a")
            .await
            .expect("query")
            .expect("manifest");
        assert_eq!(loaded, manifest);
        assert!(
            db.update_artifact_manifest_state("manifest-a", "sealed")
                .await
                .expect("update")
        );
    }
}
