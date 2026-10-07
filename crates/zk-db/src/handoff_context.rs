//! Verified, root-owned handoff material for a transient model request.
use crate::{Db, DbError};
use rusqlite::{OptionalExtension, params};
use sha2::{Digest, Sha256};

impl Db {
    /// Read only a completed handoff bound to this Run's authoritative root.
    /// Ordinary sessions return `None`; summaries are never read from UI messages.
    /// # Errors
    /// Rejects forged Run ownership, broken bindings and modified sealed content.
    pub async fn handoff_brief_for_run(
        &self,
        session: &str,
        run: &str,
    ) -> Result<Option<(String, String)>, DbError> {
        let (session, run) = (session.to_owned(), run.to_owned());
        self.with_reader(move |conn| {
            let root: String = conn.query_row(
                "SELECT COALESCE(root.session_id,r.session_id) FROM run_envelopes r LEFT JOIN tasks t ON t.id=r.task_id LEFT JOIN tasks root ON root.id=t.root_task_id WHERE r.id=?1 AND r.session_id=?2",
                params![run,session], |row| row.get(0),
            ).optional()?.ok_or_else(||DbError::Invalid("HANDOFF_RUN_SESSION_MISMATCH".into()))?;
            let metadata: Option<String> = conn.query_row("SELECT metadata_json FROM sessions WHERE id=?1", [&root], |row|row.get(0))?;
            let metadata: serde_json::Value = metadata.map(|s|serde_json::from_str(&s)).transpose()?.unwrap_or_default();
            let Some(id) = metadata.get("mergeOperationId").and_then(serde_json::Value::as_str) else { return Ok(None); };
            let (body,hash):(String,String)=conn.query_row(
                "SELECT summary_body,summary_hash FROM session_merges WHERE id=?1 AND target_session_id=?2 AND status='completed' AND snapshot_sealed=1",
                params![id,root], |row|Ok((row.get(0)?,row.get(1)?)),
            ).optional()?.ok_or_else(||DbError::Invalid("HANDOFF_BINDING_MISMATCH".into()))?;
            if format!("{:x}",Sha256::digest(body.as_bytes()))!=hash { return Err(DbError::Invalid("MERGE_SUMMARY_HASH_MISMATCH".into())); }
            Ok(Some((id.to_owned(),body)))
        }).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{Db, SessionMergeRequest};
    use sha2::{Digest, Sha256};

    #[tokio::test]
    async fn brief_requires_published_root_binding_and_original_hash() {
        let db = Db::open_in_memory().unwrap();
        for id in ["a", "b", "ordinary"] {
            db.create_session_with_id(id, "model", "/tmp")
                .await
                .unwrap();
        }
        db.start_run("ordinary-run", "ordinary", None, None, "model")
            .await
            .unwrap();
        assert!(
            db.handoff_brief_for_run("ordinary", "ordinary-run")
                .await
                .unwrap()
                .is_none()
        );
        let merge = db
            .start_session_merge(
                "brief".into(),
                SessionMergeRequest {
                    source_session_ids: vec!["a".into(), "b".into()],
                    primary_session_id: "a".into(),
                    title: None,
                    model: None,
                },
            )
            .await
            .unwrap();
        let body = "sealed historical summary";
        db.publish_merge_summary(
            &merge.operation_id,
            merge.run_epoch,
            body.into(),
            serde_json::json!({}),
            format!("{:x}", Sha256::digest(body.as_bytes())),
        )
        .await
        .unwrap();
        db.complete_session_merge(&merge.operation_id, merge.run_epoch)
            .await
            .unwrap();
        db.start_run("merged-run", &merge.target_session_id, None, None, "model")
            .await
            .unwrap();
        assert_eq!(
            db.handoff_brief_for_run(&merge.target_session_id, "merged-run")
                .await
                .unwrap(),
            Some((merge.operation_id.clone(), body.into()))
        );
        assert!(
            db.handoff_brief_for_run("ordinary", "merged-run")
                .await
                .is_err()
        );
        db.with_writer(move |conn| {
            conn.execute(
                "UPDATE session_merges SET summary_body='modified' WHERE id=?1",
                [merge.operation_id],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        assert!(
            db.handoff_brief_for_run(&merge.target_session_id, "merged-run")
                .await
                .unwrap_err()
                .to_string()
                .contains("HASH_MISMATCH")
        );
    }
}
