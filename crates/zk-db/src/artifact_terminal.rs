//! Idempotent terminal integrity checks with a snapshot compare-and-swap.
use crate::{ArtifactManifestRecord, Db, DbError};
use rusqlite::OptionalExtension;
use serde::Serialize;

/// Outcome of atomically saving a read-only integrity observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactCheckCommit {
    /// The observation and its idempotency receipt were saved together.
    Saved,
    /// A prior observer already finished this manifest.
    AlreadyChecked,
    /// The manifest changed; the caller must read and verify a fresh snapshot.
    Changed,
}

/// Content-free receipt; this does not claim a test or browser journey passed.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminalArtifactCheck {
    /// Manifest whose immutable seals were checked.
    pub manifest_id: String,
    /// Owning physical execution.
    pub run_id: String,
    /// `verified`, `failed`, or `unavailable`.
    pub status: String,
    /// Fixed diagnostic category, without paths or content hashes.
    pub diagnostic_code: Option<String>,
    /// Observation time.
    pub checked_at: String,
}

impl Db {
    /// Read the durable terminal check without reading potentially expired content.
    /// # Errors
    /// Returns a storage error if the receipt cannot be read.
    pub async fn artifact_terminal_check(
        &self,
        run_id: &str,
    ) -> Result<Option<TerminalArtifactCheck>, DbError> {
        let run_id = run_id.to_owned();
        self.with_reader(move |conn| conn.query_row(
            "SELECT manifest_id,run_id,status,diagnostic_code,checked_at FROM artifact_terminal_checks WHERE run_id=?1",
            [run_id], |r| Ok(TerminalArtifactCheck {manifest_id:r.get(0)?,run_id:r.get(1)?,status:r.get(2)?,diagnostic_code:r.get(3)?,checked_at:r.get(4)?})
        ).optional().map_err(Into::into)).await
    }

    /// Find bounded terminal manifests whose integrity projection was interrupted.
    /// This only schedules read-only checks, never tools or external Hooks.
    /// # Errors
    /// Returns a storage error if terminal ownership cannot be read.
    pub async fn pending_terminal_artifact_runs(
        &self,
        limit: usize,
    ) -> Result<Vec<String>, DbError> {
        let limit = i64::try_from(limit.clamp(1, 100)).unwrap_or(100);
        self.with_reader(move |conn| {
            let mut stmt=conn.prepare("SELECT m.run_id FROM artifact_manifests m JOIN run_envelopes r ON r.id=m.run_id WHERE r.status IN ('completed','failed','cancelled','interrupted') AND r.cleanup_status IN ('notRequired','confirmed') AND NOT EXISTS(SELECT 1 FROM artifact_terminal_checks c WHERE c.manifest_id=m.manifest_id) ORDER BY r.updated_at,m.manifest_id LIMIT ?1")?;
            stmt.query_map([limit],|r|r.get(0))?.collect::<Result<Vec<_>,_>>().map_err(Into::into)
        }).await
    }

    /// Save an explicitly requested check only if its frozen manifest still matches.
    /// The original seal and producer identity cannot be rewritten by a projection.
    /// # Errors
    /// Returns a conflict when another write or verifier changed the manifest.
    pub async fn save_artifact_verification_cas(
        &self,
        expected: &ArtifactManifestRecord,
        observed: &ArtifactManifestRecord,
    ) -> Result<(), DbError> {
        validate_projection(expected, observed)?;
        let (expected, observed) = (expected.clone(), observed.clone());
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            if crate::artifact::load_manifest(&tx, &expected.manifest_id)?.as_ref()
                != Some(&expected)
            {
                return Err(DbError::Conflict("ARTIFACT_CHECK_CONFLICT".into()));
            }
            crate::artifact::save_manifest_in_current_write(&tx, &observed)?;
            if expected.state == "verified" && observed.state != "verified" {
                crate::artifact::invalidate_verification_in_current_write(
                    &tx,
                    &expected.manifest_id,
                    &expected.run_id,
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Save exactly the observation made from `expected`, preserving every seal.
    /// Concurrent file receipts or verification invalidations cause a retry.
    /// # Errors
    /// Rejects nonterminal runs, changed identities, and storage failures.
    pub async fn commit_terminal_artifact_check(
        &self,
        expected: &ArtifactManifestRecord,
        observed: &ArtifactManifestRecord,
    ) -> Result<ArtifactCheckCommit, DbError> {
        validate_projection(expected, observed)?;
        let (expected, observed) = (expected.clone(), observed.clone());
        self.with_writer(move |conn| {
            let tx=conn.transaction()?;
            require_terminal(&tx,&expected.run_id)?;
            if tx.query_row("SELECT EXISTS(SELECT 1 FROM artifact_terminal_checks WHERE manifest_id=?1)",[&expected.manifest_id],|r|r.get::<_,bool>(0))? {
                return Ok(ArtifactCheckCommit::AlreadyChecked);
            }
            let current=crate::artifact::load_manifest(&tx,&expected.manifest_id)?;
            if current.as_ref()!=Some(&expected) {return Ok(ArtifactCheckCommit::Changed);}
            crate::artifact::save_manifest_in_current_write(&tx,&observed)?;
            if expected.state=="verified" && observed.state!="verified" {
                crate::artifact::invalidate_verification_in_current_write(&tx,&expected.manifest_id,&expected.run_id)?;
            }
            let (status,code)=if observed.state=="verified" {("verified",None)}else{("failed",Some("ARTIFACT_INTEGRITY_FAILED"))};
            tx.execute("INSERT INTO artifact_terminal_checks(manifest_id,run_id,status,diagnostic_code,checked_at) VALUES(?1,?2,?3,?4,?5)",rusqlite::params![observed.manifest_id,observed.run_id,status,code,observed.updated_at])?;
            tx.commit()?;
            Ok(ArtifactCheckCommit::Saved)
        }).await
    }

    /// Record explicitly unavailable ephemeral seals after their content has expired.
    /// No digest, body, or unavailable value is replaced with invented content.
    /// # Errors
    /// Rejects a live/persistent content store or nonterminal run.
    pub async fn mark_terminal_artifact_content_unavailable(
        &self,
        run_id: &str,
    ) -> Result<(), DbError> {
        let run_id = run_id.to_owned();
        let content = self.memory_content_store();
        self.with_writer(move |conn| {
            let tx=conn.transaction()?;
            require_terminal(&tx,&run_id)?;
            let Some((manifest,session))=tx.query_row("SELECT manifest_id,session_id FROM artifact_manifests WHERE run_id=?1",[&run_id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).optional()? else{return Ok(())};
            let retention:String=tx.query_row("SELECT content_retention FROM sessions WHERE id=?1",[&session],|r|r.get(0))?;
            if retention!="ephemeral" {return Err(DbError::Invalid("ARTIFACT_CONTENT_STILL_REQUIRED".into()));}
            if content.has_live_scope(&session) {return Err(DbError::Invalid("ARTIFACT_CONTENT_STILL_AVAILABLE".into()));}
            tx.execute("INSERT OR IGNORE INTO artifact_terminal_checks(manifest_id,run_id,status,diagnostic_code,checked_at) VALUES(?1,?2,'unavailable','ARTIFACT_CONTENT_UNAVAILABLE',?3)",rusqlite::params![manifest,run_id,crate::time::format_rfc3339_micros(crate::time::now_millis())])?;
            tx.commit()?;
            Ok(())
        }).await
    }
}

fn require_terminal(conn: &rusqlite::Connection, run_id: &str) -> Result<(), DbError> {
    let state = conn.query_row(
        "SELECT status IN ('completed','failed','cancelled','interrupted'), cleanup_status IN ('notRequired','confirmed') FROM run_envelopes WHERE id=?1",
        [run_id], |row| Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?)),
    ).optional()?;
    match state {
        Some((true, true)) => Ok(()),
        Some((true, false)) => Err(DbError::Invalid("ARTIFACT_CLEANUP_UNCONFIRMED".into())),
        _ => Err(DbError::Invalid("ARTIFACT_RUN_NOT_TERMINAL".into())),
    }
}

fn validate_projection(
    expected: &ArtifactManifestRecord,
    observed: &ArtifactManifestRecord,
) -> Result<(), DbError> {
    let mut projection = observed.clone();
    projection.state.clone_from(&expected.state);
    projection.updated_at.clone_from(&expected.updated_at);
    if projection.entries.len() != expected.entries.len() {
        return Err(DbError::Invalid("ARTIFACT_SEAL_CHANGED".into()));
    }
    for (actual, original) in projection.entries.iter_mut().zip(&expected.entries) {
        actual.state.clone_from(&original.state);
        actual.updated_at.clone_from(&original.updated_at);
        actual.actual_hash.clone_from(&original.actual_hash);
        actual.failure_code.clone_from(&original.failure_code);
        actual
            .validator_result
            .clone_from(&original.validator_result);
    }
    if &projection != expected {
        return Err(DbError::Invalid("ARTIFACT_SEAL_CHANGED".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ArtifactEntryRecord, CleanupStatus, CommitTaskResult, ResultStatus, VerificationStatus,
    };

    async fn fixture(
        ephemeral: bool,
    ) -> (
        Db,
        ArtifactManifestRecord,
        Option<crate::content::EphemeralContentLease>,
    ) {
        fixture_in(Db::open_in_memory().unwrap(), ephemeral).await
    }

    async fn fixture_in(
        db: Db,
        ephemeral: bool,
    ) -> (
        Db,
        ArtifactManifestRecord,
        Option<crate::content::EphemeralContentLease>,
    ) {
        let (session, lease) = if ephemeral {
            let (session, lease) = db
                .create_ephemeral_session("model", "/tmp/artifact-terminal", "DONT_ASK")
                .await
                .unwrap();
            (session, Some(lease))
        } else {
            (
                db.create_session("model", "/tmp/artifact-terminal")
                    .await
                    .unwrap()
                    .id,
                None,
            )
        };
        db.start_run("terminal-run", &session, None, Some("query"), "model")
            .await
            .unwrap();
        let now = crate::time::format_rfc3339_micros(crate::time::now_millis());
        let manifest = ArtifactManifestRecord {
            manifest_id: "manifest".into(),
            run_id: "terminal-run".into(),
            session_id: session,
            workspace_root: "/tmp/artifact-terminal".into(),
            state: "sealed".into(),
            created_at: now.clone(),
            updated_at: now.clone(),
            entries: vec![ArtifactEntryRecord {
                artifact_id: "artifact".into(),
                tool_use_id: "write".into(),
                producer_invocation_id: None,
                canonical_path: "/tmp/artifact-terminal/report.txt".into(),
                operation: "created".into(),
                state: "sealed".into(),
                sealed_hash: Some("a".repeat(64)),
                actual_hash: None,
                file_size: Some(10),
                required_validator_id: None,
                validator_result: None,
                failure_code: None,
                created_at: now.clone(),
                updated_at: now,
            }],
        };
        db.save_artifact_manifest(&manifest).await.unwrap();
        (db, manifest, lease)
    }
    async fn finish(db: &Db) {
        finish_with_cleanup(db, CleanupStatus::Confirmed).await;
    }

    async fn finish_with_cleanup(db: &Db, cleanup_status: CleanupStatus) {
        let run = db.find_run_by_id("terminal-run").await.unwrap().unwrap();
        db.ensure_task_final_assistant(&run.task_id, &run.id, "done")
            .await
            .unwrap();
        let task = db
            .find_runtime_task_by_id(&run.task_id)
            .await
            .unwrap()
            .unwrap();
        db.commit_task_result_with_run_usage_fallback(
            &CommitTaskResult {
                task_id: task.id,
                run_id: run.id,
                expected_task_version: task.version,
                status: ResultStatus::Complete,
                content: "done".into(),
                media_type: "text/plain".into(),
                error_code: None,
                cleanup_status,
                verification_status: VerificationStatus::NotRequested,
            },
            crate::RunUsageFallback {
                usage_complete: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    fn passed(original: &ArtifactManifestRecord) -> ArtifactManifestRecord {
        let mut result = original.clone();
        result.state = "verified".into();
        result.entries[0].state = "integrity_verified".into();
        result.entries[0].actual_hash = result.entries[0].sealed_hash.clone();
        result
    }

    #[tokio::test]
    async fn check_is_terminal_cas_idempotent_and_preserves_immutable_result() {
        let (db, original, _) = fixture(false).await;
        assert!(
            db.commit_terminal_artifact_check(&original, &passed(&original))
                .await
                .is_err()
        );
        finish(&db).await;
        assert_eq!(
            db.pending_terminal_artifact_runs(1).await.unwrap(),
            vec!["terminal-run"]
        );
        let mut newer = original.clone();
        newer.entries[0].sealed_hash = Some("b".repeat(64));
        db.save_artifact_manifest(&newer).await.unwrap();
        assert_eq!(
            db.commit_terminal_artifact_check(&original, &passed(&original))
                .await
                .unwrap(),
            ArtifactCheckCommit::Changed
        );
        assert_eq!(
            db.find_artifact_manifest("manifest")
                .await
                .unwrap()
                .unwrap()
                .entries[0]
                .sealed_hash,
            newer.entries[0].sealed_hash
        );
        assert!(
            db.commit_terminal_artifact_check(&original, &passed(&newer))
                .await
                .is_err()
        );
        assert!(matches!(
            db.save_artifact_verification_cas(&original, &passed(&original))
                .await,
            Err(DbError::Conflict(_))
        ));
        let observed = passed(&newer);
        let (one, two) = tokio::join!(
            db.commit_terminal_artifact_check(&newer, &observed),
            db.commit_terminal_artifact_check(&newer, &observed)
        );
        let outcomes = [one.unwrap(), two.unwrap()];
        assert!(outcomes.contains(&ArtifactCheckCommit::Saved));
        assert!(outcomes.contains(&ArtifactCheckCommit::AlreadyChecked));
        assert!(
            db.pending_terminal_artifact_runs(10)
                .await
                .unwrap()
                .is_empty()
        );
        let run = db.find_run_by_id("terminal-run").await.unwrap().unwrap();
        assert_eq!(
            db.read_task_result(&run.task_id, None, 0, 100)
                .await
                .unwrap()
                .unwrap()
                .content,
            "done"
        );
        assert_eq!(
            db.artifact_terminal_check(&run.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "verified"
        );
    }

    #[tokio::test]
    async fn expired_seals_are_unavailable_and_never_count_as_verified() {
        let (db, original, lease) = fixture(true).await;
        finish(&db).await;
        assert!(
            db.mark_terminal_artifact_content_unavailable(&original.run_id)
                .await
                .is_err()
        );
        drop(lease);
        db.mark_terminal_artifact_content_unavailable(&original.run_id)
            .await
            .unwrap();
        db.mark_terminal_artifact_content_unavailable(&original.run_id)
            .await
            .unwrap();
        let receipt = db
            .artifact_terminal_check(&original.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.status, "unavailable");
        assert_eq!(
            receipt.diagnostic_code.as_deref(),
            Some("ARTIFACT_CONTENT_UNAVAILABLE")
        );
        assert_eq!(db.memory_content_store().retained_bytes(), 0);
        assert!(db.find_artifact_manifest("manifest").await.is_err());
    }

    #[tokio::test]
    async fn unresolved_resource_cleanup_cannot_publish_a_terminal_integrity_receipt() {
        let (db, original, _) = fixture(false).await;
        finish_with_cleanup(&db, CleanupStatus::Unconfirmed).await;
        assert!(
            db.pending_terminal_artifact_runs(10)
                .await
                .unwrap()
                .is_empty()
        );
        let error = db
            .commit_terminal_artifact_check(&original, &passed(&original))
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::Invalid(code) if code == "ARTIFACT_CLEANUP_UNCONFIRMED"));
        assert!(
            db.artifact_terminal_check(&original.run_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn restart_finds_unobserved_terminal_manifests_and_preserves_completed_receipts() {
        let directory =
            std::env::temp_dir().join(format!("zk-artifact-restart-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("data.sqlite");
        let (db, original, _) = fixture_in(Db::open(&path).unwrap(), false).await;
        finish(&db).await;
        drop(db);
        let db = Db::open(&path).unwrap();
        assert_eq!(
            db.pending_terminal_artifact_runs(10).await.unwrap(),
            vec![original.run_id.clone()]
        );
        let sealed = db
            .find_artifact_manifest(&original.manifest_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sealed, original);
        assert_eq!(
            db.commit_terminal_artifact_check(&sealed, &passed(&sealed))
                .await
                .unwrap(),
            ArtifactCheckCommit::Saved
        );
        let receipt = db
            .artifact_terminal_check(&sealed.run_id)
            .await
            .unwrap()
            .unwrap();
        drop(db);
        let db = Db::open(&path).unwrap();
        assert!(
            db.pending_terminal_artifact_runs(10)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            db.commit_terminal_artifact_check(&sealed, &passed(&sealed))
                .await
                .unwrap(),
            ArtifactCheckCommit::AlreadyChecked
        );
        let reopened = db
            .artifact_terminal_check(&sealed.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reopened.checked_at, receipt.checked_at);
        assert_eq!(reopened.status, "verified");
        drop(db);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
