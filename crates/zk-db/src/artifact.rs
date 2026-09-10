//! Artifact manifest repository backed by the primary `SQLite` database.

use std::path::Path;

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
    /// Durable physical invocation that produced the artifact.
    pub producer_invocation_id: Option<String>,
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
    /// File size captured when the artifact was sealed.
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

/// One committed file effect emitted by a built-in file tool.  This is an
/// append/update command for the `SQLite` artifact authority, not a second
/// manifest representation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProducedFileArtifactRecord {
    /// Owning physical Run.
    pub run_id: String,
    /// Transcript Session that owns the Run.
    pub session_id: String,
    /// Canonical authorized workspace root.
    pub workspace_root: String,
    /// Provider tool-use identifier.
    pub tool_use_id: String,
    /// Durable succeeded invocation identifier.
    pub producer_invocation_id: String,
    /// Canonical absolute path written by the tool.
    pub canonical_path: String,
    /// `created` or `modified`.
    pub operation: String,
    /// SHA-256 of the committed bytes.
    pub sealed_hash: String,
    /// Committed byte count.
    pub file_size: i64,
}

impl Db {
    /// Register a successful built-in file write in the run's `SQLite` manifest.
    ///
    /// The physical invocation must already be durably `succeeded`; a failed,
    /// invented or cross-run invocation is rejected. Concurrent writes are
    /// serialized by the database writer and entries are upserted by canonical
    /// path. Any prior verified artifact whose bytes changed is invalidated in
    /// the same transaction.
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] when the record is invalid, its producing invocation
    /// is not an authorized successful write, or the atomic database update fails.
    #[allow(clippy::too_many_lines)]
    pub async fn record_produced_file_artifact(
        &self,
        produced: &ProducedFileArtifactRecord,
    ) -> Result<ArtifactManifestRecord, DbError> {
        validate_produced_file_artifact(produced)?;
        let produced = produced.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let owner = tx
                .query_row(
                    "SELECT invocation.run_id,run.session_id,invocation.tool_use_id \
                     FROM tool_invocations invocation \
                     JOIN run_envelopes run ON run.id=invocation.run_id \
                     WHERE invocation.invocation_id=?1 \
                       AND invocation.status='succeeded' \
                       AND invocation.side_effect_class='write'",
                    [&produced.producer_invocation_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?;
            if owner
                != Some((
                    produced.run_id.clone(),
                    produced.session_id.clone(),
                    produced.tool_use_id.clone(),
                ))
            {
                return Err(DbError::Invalid(
                    "ARTIFACT_PRODUCER_INVOCATION_MISMATCH".to_owned(),
                ));
            }

            // Exact retries are idempotent and must not invalidate a manifest
            // which may have been verified after the first commit.
            let existing_manifest_id = tx
                .query_row(
                    "SELECT manifest.manifest_id FROM artifact_manifests manifest \
                     JOIN artifact_entries entry ON entry.manifest_id=manifest.manifest_id \
                     WHERE manifest.run_id=?1 AND entry.canonical_path=?2 \
                       AND entry.producer_invocation_id=?3 AND entry.tool_use_id=?4 \
                       AND entry.operation=?5 AND entry.sealed_hash=?6 AND entry.file_size=?7",
                    rusqlite::params![
                        &produced.run_id,
                        &produced.canonical_path,
                        &produced.producer_invocation_id,
                        &produced.tool_use_id,
                        &produced.operation,
                        &produced.sealed_hash,
                        produced.file_size,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(manifest_id) = existing_manifest_id {
                let manifest = load_manifest(&tx, &manifest_id)?.ok_or_else(|| {
                    DbError::Invalid("ARTIFACT_MANIFEST_DISAPPEARED".to_owned())
                })?;
                tx.commit()?;
                return Ok(manifest);
            }

            // Invalidate every verified projection that relied on different
            // bytes at this absolute path, including a previous Run's manifest.
            let affected = {
                let mut statement = tx.prepare(
                    "SELECT DISTINCT manifest.manifest_id,manifest.run_id \
                     FROM artifact_manifests manifest \
                     JOIN artifact_entries entry ON entry.manifest_id=manifest.manifest_id \
                     WHERE entry.canonical_path=?1 \
                       AND (manifest.state='verified' OR entry.state IN ('integrity_verified','content_verified')) \
                       AND (entry.sealed_hash IS NOT ?2 OR entry.file_size IS NOT ?3)",
                )?;
                statement
                    .query_map(
                        rusqlite::params![
                            &produced.canonical_path,
                            &produced.sealed_hash,
                            produced.file_size,
                        ],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                    )?
                    .collect::<Result<Vec<_>, _>>()?
            };
            let now = crate::time::format_rfc3339_micros(crate::time::now_millis());
            for (manifest_id, run_id) in affected {
                invalidate_verification_in_current_write(&tx, &manifest_id, &run_id)?;
                tx.execute(
                    "UPDATE artifact_manifests SET state='unverified',updated_at=?1 \
                     WHERE manifest_id=?2",
                    rusqlite::params![&now, &manifest_id],
                )?;
                tx.execute(
                    "UPDATE artifact_entries SET state='unverified',actual_hash=?1, \
                     failure_code='ARTIFACT_CHANGED_AFTER_VERIFICATION',updated_at=?2 \
                     WHERE manifest_id=?3 AND canonical_path=?4",
                    rusqlite::params![
                        &produced.sealed_hash,
                        &now,
                        &manifest_id,
                        &produced.canonical_path,
                    ],
                )?;
            }

            let candidate_manifest_id = uuid::Uuid::new_v4().to_string();
            tx.execute(
                "INSERT OR IGNORE INTO artifact_manifests \
                 (manifest_id,run_id,session_id,workspace_root,state,created_at,updated_at) \
                 VALUES(?1,?2,?3,?4,'sealed',?5,?5)",
                rusqlite::params![
                    &candidate_manifest_id,
                    &produced.run_id,
                    &produced.session_id,
                    &produced.workspace_root,
                    &now,
                ],
            )?;
            let (manifest_id, manifest_session, manifest_workspace): (String, String, String) = tx
                .query_row(
                    "SELECT manifest_id,session_id,workspace_root FROM artifact_manifests \
                     WHERE run_id=?1",
                    [&produced.run_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )?;
            if manifest_session != produced.session_id
                || manifest_workspace != produced.workspace_root
            {
                return Err(DbError::Invalid(
                    "ARTIFACT_MANIFEST_OWNER_MISMATCH".to_owned(),
                ));
            }
            tx.execute(
                "UPDATE artifact_manifests SET state='sealed',updated_at=?1 \
                 WHERE manifest_id=?2",
                rusqlite::params![&now, &manifest_id],
            )?;
            let artifact_id = uuid::Uuid::new_v4().to_string();
            tx.execute(
                "INSERT INTO artifact_entries \
                 (artifact_id,manifest_id,tool_use_id,producer_invocation_id,canonical_path, \
                  operation,state,sealed_hash,actual_hash,file_size,required_validator_id, \
                  validator_result_json,failure_code,created_at,updated_at) \
                 VALUES(?1,?2,?3,?4,?5,?6,'sealed',?7,NULL,?8,NULL,NULL,NULL,?9,?9) \
                 ON CONFLICT(manifest_id,canonical_path) DO UPDATE SET \
                  tool_use_id=excluded.tool_use_id, \
                  producer_invocation_id=excluded.producer_invocation_id, \
                  operation=excluded.operation,state='sealed',sealed_hash=excluded.sealed_hash, \
                  actual_hash=NULL,file_size=excluded.file_size,required_validator_id=NULL, \
                  validator_result_json=NULL,failure_code=NULL,updated_at=excluded.updated_at",
                rusqlite::params![
                    &artifact_id,
                    &manifest_id,
                    &produced.tool_use_id,
                    &produced.producer_invocation_id,
                    &produced.canonical_path,
                    &produced.operation,
                    &produced.sealed_hash,
                    produced.file_size,
                    &now,
                ],
            )?;
            crate::run::append_event_in_current_write(
                &tx,
                &produced.run_id,
                "artifact_recorded",
                Some(&produced.tool_use_id),
                &serde_json::json!({
                    "manifestId": manifest_id,
                    "artifactId": artifact_id,
                    "canonicalPath": produced.canonical_path,
                    "operation": produced.operation,
                    "sealedHash": produced.sealed_hash,
                    "fileSize": produced.file_size,
                    "producerInvocationId": produced.producer_invocation_id,
                }),
            )?;
            let manifest = load_manifest(&tx, &manifest_id)?
                .ok_or_else(|| DbError::Invalid("ARTIFACT_MANIFEST_DISAPPEARED".to_owned()))?;
            tx.commit()?;
            Ok(manifest)
        })
        .await
    }

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
            save_manifest_in_current_write(&tx, &manifest)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Persist the latest integrity check and atomically invalidate all durable
    /// verification projections that depended on a previously verified artifact.
    ///
    /// `invalidates_prior_verification` must only be set when this check changed a
    /// previously `verified` manifest to a non-verified state.  The owning Run and
    /// Task become `stale`; passing evidence produced by the Run becomes `stale`,
    /// and acceptance criteria backed by that evidence return to `not_verified`.
    ///
    /// # Errors
    /// Returns [`DbError`] when any manifest or invalidation write fails. No partial
    /// projection is committed.
    pub async fn save_artifact_verification(
        &self,
        manifest: &ArtifactManifestRecord,
        invalidates_prior_verification: bool,
    ) -> Result<(), DbError> {
        let manifest = manifest.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            save_manifest_in_current_write(&tx, &manifest)?;
            if invalidates_prior_verification {
                invalidate_verification_in_current_write(
                    &tx,
                    &manifest.manifest_id,
                    &manifest.run_id,
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

    /// Resolve the completed physical invocation for an artifact declaration.
    ///
    /// Artifact APIs use this before sealing so a model-provided `toolUseId`
    /// cannot masquerade as proof that a write actually ran.
    ///
    /// # Errors
    /// Returns [`DbError`] when the query fails.
    pub async fn find_artifact_producer_invocation(
        &self,
        run_id: &str,
        tool_use_id: &str,
    ) -> Result<Option<String>, DbError> {
        let run_id = run_id.to_owned();
        let tool_use_id = tool_use_id.to_owned();
        self.with_reader(move |conn| {
            conn.query_row(
                "SELECT invocation_id FROM tool_invocations \
                 WHERE run_id=?1 AND tool_use_id=?2 AND status='succeeded' \
                   AND side_effect_class IN ('write','unknown')",
                rusqlite::params![run_id, tool_use_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
        })
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

fn validate_produced_file_artifact(produced: &ProducedFileArtifactRecord) -> Result<(), DbError> {
    if !matches!(produced.operation.as_str(), "created" | "modified") {
        return Err(DbError::Invalid("ARTIFACT_OPERATION_INVALID".to_owned()));
    }
    if !Path::new(&produced.canonical_path).is_absolute()
        || !Path::new(&produced.workspace_root).is_absolute()
        || !Path::new(&produced.canonical_path).starts_with(&produced.workspace_root)
    {
        return Err(DbError::Invalid("ARTIFACT_PATH_INVALID".to_owned()));
    }
    if produced.sealed_hash.len() != 64
        || !produced
            .sealed_hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || produced.file_size < 0
    {
        return Err(DbError::Invalid("ARTIFACT_SEAL_INVALID".to_owned()));
    }
    Ok(())
}

fn save_manifest_in_current_write(
    conn: &rusqlite::Connection,
    manifest: &ArtifactManifestRecord,
) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO artifact_manifests \
         (manifest_id,run_id,session_id,workspace_root,state,created_at,updated_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7) \
         ON CONFLICT(manifest_id) DO UPDATE SET run_id=excluded.run_id, \
         session_id=excluded.session_id, workspace_root=excluded.workspace_root, \
         state=excluded.state, updated_at=excluded.updated_at",
        rusqlite::params![
            &manifest.manifest_id,
            &manifest.run_id,
            &manifest.session_id,
            &manifest.workspace_root,
            &manifest.state,
            &manifest.created_at,
            &manifest.updated_at,
        ],
    )?;
    conn.execute(
        "DELETE FROM artifact_entries WHERE manifest_id=?1",
        [&manifest.manifest_id],
    )?;
    for entry in &manifest.entries {
        let validator_result_json = entry
            .validator_result
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        conn.execute(
            "INSERT INTO artifact_entries \
             (artifact_id,manifest_id,tool_use_id,producer_invocation_id,canonical_path,operation,state,sealed_hash, \
              actual_hash,file_size,required_validator_id,validator_result_json,failure_code, \
              created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
            rusqlite::params![
                &entry.artifact_id,
                &manifest.manifest_id,
                &entry.tool_use_id,
                &entry.producer_invocation_id,
                &entry.canonical_path,
                &entry.operation,
                &entry.state,
                &entry.sealed_hash,
                &entry.actual_hash,
                entry.file_size,
                &entry.required_validator_id,
                validator_result_json,
                &entry.failure_code,
                &entry.created_at,
                &entry.updated_at,
            ],
        )?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one SQLite transaction invalidates the artifact and every ancestor projection"
)]
fn invalidate_verification_in_current_write(
    conn: &rusqlite::Connection,
    manifest_id: &str,
    source_run_id: &str,
) -> Result<(), DbError> {
    let now = crate::time::format_rfc3339_micros(crate::time::now_millis());

    // A child artifact contributes to every ancestor's delivery. Invalidate the
    // owning Task/Run and the complete ancestor chain, not merely the leaf.
    conn.execute_batch(
        "DROP TABLE IF EXISTS temp.artifact_stale_runs;\
         DROP TABLE IF EXISTS temp.artifact_stale_tasks;\
         CREATE TEMP TABLE artifact_stale_runs(id TEXT PRIMARY KEY);\
         CREATE TEMP TABLE artifact_stale_tasks(id TEXT PRIMARY KEY);",
    )?;
    conn.execute(
        "WITH RECURSIVE ancestors(id) AS ( \
             SELECT task_id FROM run_envelopes WHERE id=?1 \
             UNION \
             SELECT child.parent_task_id FROM tasks child \
             JOIN ancestors ON child.id=ancestors.id \
             WHERE child.parent_task_id IS NOT NULL \
         ) INSERT OR IGNORE INTO artifact_stale_tasks SELECT id FROM ancestors",
        [source_run_id],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO artifact_stale_runs \
         SELECT id FROM run_envelopes \
         WHERE task_id IN (SELECT id FROM artifact_stale_tasks)",
        [],
    )?;
    let mut changed_runs = conn.prepare(
        "SELECT stale.id FROM artifact_stale_runs stale \
         JOIN run_envelopes run ON run.id=stale.id \
         WHERE run.verification_status<>'stale' ORDER BY stale.id",
    )?;
    let run_ids = changed_runs
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(changed_runs);

    // Reset criteria before changing evidence verdicts so the relationship is
    // evaluated from one consistent pre-invalidation snapshot.
    conn.execute(
        "UPDATE run_acceptance_criteria SET status='not_verified',updated_at=?1 \
         WHERE status<>'not_verified' AND evidence_bundle_id IN ( \
             SELECT bundle_id FROM evidence_bundles \
             WHERE run_id IN (SELECT id FROM artifact_stale_runs) \
         )",
        [&now],
    )?;
    let stale_evidence = {
        let mut statement = conn.prepare(
            "SELECT bundle_id,effective_origin FROM ( \
                 SELECT bundle.bundle_id, \
                        COALESCE((SELECT event.verdict FROM evidence_verdict_events event \
                                  WHERE event.bundle_id=bundle.bundle_id \
                                  ORDER BY event.version DESC LIMIT 1),bundle.verdict) AS effective_verdict, \
                        COALESCE((SELECT event.effective_origin FROM evidence_verdict_events event \
                                  WHERE event.bundle_id=bundle.bundle_id \
                                  ORDER BY event.version DESC LIMIT 1),bundle.origin) AS effective_origin \
                 FROM evidence_bundles bundle \
                 WHERE bundle.run_id IN (SELECT id FROM artifact_stale_runs) \
             ) projected \
             WHERE effective_origin IN ('machine','human') \
               AND effective_verdict NOT IN ('pending','stale') \
             ORDER BY bundle_id",
        )?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (bundle_id, effective_origin) in stale_evidence {
        let effective_origin = crate::EvidenceOrigin::from_db(&effective_origin)?;
        crate::evidence::append_evidence_verdict_event_in_current_write(
            conn,
            &bundle_id,
            "stale",
            "artifactIntegrity",
            effective_origin,
            "artifact_integrity_changed",
            &now,
        )?;
    }
    conn.execute(
        "UPDATE tasks SET verification_status='stale',updated_at=?1,version=version+1 \
         WHERE id IN (SELECT id FROM artifact_stale_tasks) \
           AND verification_status<>'stale'",
        [&now],
    )?;
    conn.execute(
        "UPDATE run_envelopes SET verification_status='stale',updated_at=?1,version=version+1 \
         WHERE id IN (SELECT id FROM artifact_stale_runs) \
           AND verification_status<>'stale'",
        [&now],
    )?;
    for run_id in run_ids {
        crate::run::append_event_in_current_write(
            conn,
            &run_id,
            "verification_stale",
            None,
            &serde_json::json!({
                "manifestId": manifest_id,
                "sourceRunId": source_run_id,
                "reason": "artifact_integrity_changed",
            }),
        )?;
    }
    conn.execute_batch(
        "DROP TABLE artifact_stale_runs;\
         DROP TABLE artifact_stale_tasks;",
    )?;
    Ok(())
}

pub(crate) fn load_manifest(
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
        "SELECT artifact_id,tool_use_id,producer_invocation_id,canonical_path,operation,state,sealed_hash,actual_hash, \
         file_size,required_validator_id,validator_result_json,failure_code,created_at,updated_at \
         FROM artifact_entries WHERE manifest_id=?1 ORDER BY canonical_path ASC",
    )?;
    let rows = entry_stmt.query_map([manifest_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<i64>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, Option<String>>(11)?,
            row.get::<_, String>(12)?,
            row.get::<_, String>(13)?,
        ))
    })?;
    for row in rows {
        let (
            artifact_id,
            tool_use_id,
            producer_invocation_id,
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
            producer_invocation_id,
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
    use crate::{
        AcceptanceCriterionRecord, CleanupStatus, EvidenceBundleRecord, EvidenceOrigin,
        NewToolInvocation, ToolInvocationStatus, VerificationStatus,
    };

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
                producer_invocation_id: None,
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

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn stale_artifact_invalidates_run_task_evidence_and_acceptance_atomically() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("model", "/tmp/artifacts")
            .await
            .expect("session");
        db.start_run("root-stale", &session.id, None, Some("query"), "model")
            .await
            .expect("root run");
        db.start_run(
            "run-stale",
            &session.id,
            Some("root-stale"),
            Some("subagent"),
            "model",
        )
        .await
        .expect("child run");
        db.with_conn_blocking(|conn| {
            conn.execute(
                "UPDATE run_envelopes SET verification_status='passed' \
                 WHERE id IN ('root-stale','run-stale')",
                [],
            )?;
            conn.execute(
                "UPDATE tasks SET verification_status='passed' \
                 WHERE id IN ('root-stale','run-stale')",
                [],
            )?;
            Ok(())
        })
        .expect("seed verification");

        db.save_evidence_bundle(&EvidenceBundleRecord {
            bundle_id: "evidence-stale".into(),
            session_id: session.id.clone(),
            agent_id: None,
            kind: "verify".into(),
            claim: Some("artifact is valid".into()),
            origin: EvidenceOrigin::Human,
            producer_invocation_id: None,
            verdict: "verified".into(),
            created_at: "2026-09-08T00:00:00.000000Z".into(),
            run_id: Some("run-stale".into()),
            items: Vec::new(),
        })
        .await
        .expect("evidence");
        assert!(
            db.update_evidence_verdict("evidence-stale", "verified")
                .await
                .expect("human review event")
        );
        db.save_evidence_bundle(&EvidenceBundleRecord {
            bundle_id: "root-evidence-stale".into(),
            session_id: session.id.clone(),
            agent_id: None,
            kind: "verify".into(),
            claim: Some("child artifact supports root delivery".into()),
            origin: EvidenceOrigin::Human,
            producer_invocation_id: None,
            verdict: "verified".into(),
            created_at: "2026-09-08T00:00:00.000000Z".into(),
            run_id: Some("root-stale".into()),
            items: Vec::new(),
        })
        .await
        .expect("root evidence");
        db.replace_acceptance_criteria(
            "root-stale",
            &[AcceptanceCriterionRecord {
                criterion_id: "criterion-stale".into(),
                root_run_id: "root-stale".into(),
                ordinal: 0,
                criterion_type: "business".into(),
                source_text: "deliver a valid artifact".into(),
                status: "passed".into(),
                evidence_bundle_id: Some("root-evidence-stale".into()),
                created_at: "2026-09-08T00:00:00.000000Z".into(),
                updated_at: "2026-09-08T00:00:00.000000Z".into(),
            }],
        )
        .await
        .expect("criterion");

        let manifest = ArtifactManifestRecord {
            manifest_id: "manifest-stale".into(),
            run_id: "run-stale".into(),
            session_id: session.id,
            workspace_root: "/tmp/artifacts".into(),
            state: "unverified".into(),
            created_at: "2026-09-08T00:00:00.000000Z".into(),
            updated_at: "2026-09-08T00:01:00.000000Z".into(),
            entries: vec![ArtifactEntryRecord {
                artifact_id: "artifact-stale".into(),
                tool_use_id: "tool-stale".into(),
                producer_invocation_id: None,
                canonical_path: "/tmp/artifacts/output.txt".into(),
                operation: "created".into(),
                state: "unverified".into(),
                sealed_hash: Some("original".into()),
                actual_hash: Some("changed".into()),
                file_size: Some(8),
                required_validator_id: None,
                validator_result: None,
                failure_code: Some("ARTIFACT_HASH_MISMATCH".into()),
                created_at: "2026-09-08T00:00:00.000000Z".into(),
                updated_at: "2026-09-08T00:01:00.000000Z".into(),
            }],
        };
        db.save_artifact_verification(&manifest, true)
            .await
            .expect("atomic invalidation");

        assert_eq!(
            db.find_artifact_manifest("manifest-stale")
                .await
                .expect("manifest query")
                .expect("manifest")
                .state,
            "unverified"
        );
        assert_eq!(
            db.find_run_by_id("run-stale")
                .await
                .expect("run query")
                .expect("run")
                .verification_status,
            "stale"
        );
        assert_eq!(
            db.find_runtime_task_by_id("run-stale")
                .await
                .expect("task query")
                .expect("task")
                .verification_status,
            VerificationStatus::Stale
        );
        assert_eq!(
            db.find_run_by_id("root-stale")
                .await
                .expect("root run query")
                .expect("root run")
                .verification_status,
            "stale"
        );
        assert_eq!(
            db.find_runtime_task_by_id("root-stale")
                .await
                .expect("root task query")
                .expect("root task")
                .verification_status,
            VerificationStatus::Stale
        );
        assert_eq!(
            db.find_evidence_bundle("evidence-stale")
                .await
                .expect("evidence query")
                .expect("evidence")
                .verdict,
            "stale"
        );
        assert_eq!(
            db.find_evidence_bundle("root-evidence-stale")
                .await
                .expect("root evidence query")
                .expect("root evidence")
                .verdict,
            "stale"
        );
        let (criterion_status, event_count) = db
            .with_conn_blocking(|conn| {
                let status = conn.query_row(
                    "SELECT status FROM run_acceptance_criteria WHERE criterion_id='criterion-stale'",
                    [],
                    |row| row.get::<_, String>(0),
                )?;
                let events = conn.query_row(
                    "SELECT COUNT(*) FROM run_event_log \
                     WHERE run_id IN ('root-stale','run-stale') \
                       AND event_type='verification_stale'",
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                Ok((status, events))
            })
            .expect("projection query");
        assert_eq!(criterion_status, "not_verified");
        assert_eq!(event_count, 2);

        let child_verdicts = db
            .find_evidence_verdict_events("evidence-stale")
            .await
            .expect("child verdict history");
        assert_eq!(child_verdicts.len(), 2);
        assert_eq!(child_verdicts[0].origin, "human");
        assert_eq!(child_verdicts[1].origin, "artifactIntegrity");
        assert_eq!(child_verdicts[1].verdict, "stale");
        assert_eq!(
            child_verdicts[1].supersedes_event_id.as_deref(),
            Some(child_verdicts[0].event_id.as_str())
        );
        assert_eq!(child_verdicts[1].reason, "artifact_integrity_changed");
        let root_verdicts = db
            .find_evidence_verdict_events("root-evidence-stale")
            .await
            .expect("root verdict history");
        assert_eq!(root_verdicts.len(), 1);
        assert_eq!(root_verdicts[0].origin, "artifactIntegrity");
        let raw_verdicts = db
            .with_conn_blocking(|conn| {
                let child = conn.query_row(
                    "SELECT verdict FROM evidence_bundles WHERE bundle_id='evidence-stale'",
                    [],
                    |row| row.get::<_, String>(0),
                )?;
                let root = conn.query_row(
                    "SELECT verdict FROM evidence_bundles WHERE bundle_id='root-evidence-stale'",
                    [],
                    |row| row.get::<_, String>(0),
                )?;
                Ok((child, root))
            })
            .expect("immutable base verdicts");
        assert_eq!(raw_verdicts, ("verified".into(), "verified".into()));
    }

    async fn seed_file_invocation(
        db: &Db,
        run_id: &str,
        tool_use_id: &str,
        status: ToolInvocationStatus,
    ) -> String {
        let run = db
            .find_run_by_id(run_id)
            .await
            .expect("run query")
            .expect("run");
        let invocation_id = uuid::Uuid::new_v4().to_string();
        db.create_tool_invocation(&NewToolInvocation {
            invocation_id: invocation_id.clone(),
            task_id: run.task_id,
            run_id: run_id.to_owned(),
            tool_use_id: tool_use_id.to_owned(),
            tool_name: "Write".to_owned(),
            input_json: Some("{}".to_owned()),
            side_effect_class: "write".to_owned(),
            directory_generation: Some(1),
            connection_generation: None,
        })
        .await
        .expect("create invocation");
        assert_eq!(
            db.transition_tool_invocation_cas(
                &invocation_id,
                0,
                status,
                Some("{}"),
                Some("toolResult:test"),
                None,
                CleanupStatus::NotRequired,
            )
            .await
            .expect("terminal invocation"),
            crate::CasOutcome::Applied
        );
        invocation_id
    }

    fn produced(
        session_id: &str,
        invocation_id: &str,
        tool_use_id: &str,
        hash_byte: char,
    ) -> ProducedFileArtifactRecord {
        ProducedFileArtifactRecord {
            run_id: "run-produced".to_owned(),
            session_id: session_id.to_owned(),
            workspace_root: "/tmp/artifacts".to_owned(),
            tool_use_id: tool_use_id.to_owned(),
            producer_invocation_id: invocation_id.to_owned(),
            canonical_path: "/tmp/artifacts/output.txt".to_owned(),
            operation: "modified".to_owned(),
            sealed_hash: hash_byte.to_string().repeat(64),
            file_size: 7,
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn produced_file_requires_succeeded_invocation_and_rewrite_stales_verification() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("model", "/tmp/artifacts")
            .await
            .expect("session");
        db.start_run("run-produced", &session.id, None, Some("query"), "model")
            .await
            .expect("run");

        let failed = seed_file_invocation(
            &db,
            "run-produced",
            "write-failed",
            ToolInvocationStatus::Failed,
        )
        .await;
        let error = db
            .record_produced_file_artifact(&produced(&session.id, &failed, "write-failed", 'a'))
            .await
            .expect_err("failed invocation cannot produce an artifact");
        assert!(
            error
                .to_string()
                .contains("ARTIFACT_PRODUCER_INVOCATION_MISMATCH")
        );
        assert!(
            db.find_artifact_manifest_by_run("run-produced")
                .await
                .expect("manifest query")
                .is_none()
        );

        let first = seed_file_invocation(
            &db,
            "run-produced",
            "write-first",
            ToolInvocationStatus::Succeeded,
        )
        .await;
        let manifest = db
            .record_produced_file_artifact(&produced(&session.id, &first, "write-first", 'a'))
            .await
            .expect("first artifact");
        assert_eq!(manifest.state, "sealed");
        assert_eq!(manifest.entries.len(), 1);
        assert_eq!(
            manifest.entries[0].producer_invocation_id.as_deref(),
            Some(first.as_str())
        );

        db.with_conn_blocking(|conn| {
            conn.execute(
                "UPDATE artifact_manifests SET state='verified' WHERE manifest_id=?1",
                [&manifest.manifest_id],
            )?;
            conn.execute(
                "UPDATE artifact_entries SET state='integrity_verified' WHERE manifest_id=?1",
                [&manifest.manifest_id],
            )?;
            conn.execute(
                "UPDATE run_envelopes SET verification_status='passed' WHERE id='run-produced'",
                [],
            )?;
            conn.execute(
                "UPDATE tasks SET verification_status='passed' WHERE id='run-produced'",
                [],
            )?;
            Ok(())
        })
        .expect("seed verified projection");

        let second = seed_file_invocation(
            &db,
            "run-produced",
            "write-second",
            ToolInvocationStatus::Succeeded,
        )
        .await;
        let updated = db
            .record_produced_file_artifact(&produced(&session.id, &second, "write-second", 'b'))
            .await
            .expect("updated artifact");
        assert_eq!(updated.state, "sealed");
        assert_eq!(updated.entries.len(), 1, "canonical path is upserted");
        assert_eq!(updated.entries[0].sealed_hash, Some("b".repeat(64)));
        assert_eq!(
            updated.entries[0].producer_invocation_id.as_deref(),
            Some(second.as_str())
        );
        assert_eq!(
            db.find_run_by_id("run-produced")
                .await
                .expect("run query")
                .expect("run")
                .verification_status,
            "stale"
        );
        assert_eq!(
            db.find_runtime_task_by_id("run-produced")
                .await
                .expect("task query")
                .expect("task")
                .verification_status,
            VerificationStatus::Stale
        );
    }
}
