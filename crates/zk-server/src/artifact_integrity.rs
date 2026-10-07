//! Read-only terminal artifact projection. No external validator or journey is replayed.
use futures::future::BoxFuture;
use zk_db::{ArtifactCheckCommit, Db};
use zk_engine::RunTerminalObserver;

pub(crate) struct ArtifactIntegrityObserver(pub(crate) Db);

impl ArtifactIntegrityObserver {
    pub(crate) async fn check(&self, run_id: &str) -> Result<(), String> {
        if self
            .0
            .artifact_terminal_check(run_id)
            .await
            .map_err(|error| diagnostic(&error))?
            .is_some()
        {
            return Ok(());
        }
        let Some(run) = self
            .0
            .find_run_by_id(run_id)
            .await
            .map_err(|error| diagnostic(&error))?
        else {
            return Err("ARTIFACT_RUN_NOT_FOUND".into());
        };
        if !matches!(
            run.status.as_str(),
            "completed" | "failed" | "cancelled" | "interrupted"
        ) {
            return Ok(());
        }
        if matches!(run.cleanup_status.as_str(), "pending" | "unconfirmed") {
            return Ok(());
        }
        let ephemeral = self
            .0
            .session_retention(&run.session_id)
            .await
            .map_err(|error| diagnostic(&error))?
            == zk_db::content::ContentRetention::Ephemeral;
        if ephemeral
            && !self
                .0
                .memory_content_store()
                .has_live_scope(&run.session_id)
        {
            return self
                .0
                .mark_terminal_artifact_content_unavailable(run_id)
                .await
                .map_err(|error| diagnostic(&error));
        }
        for _ in 0..3 {
            let Some(expected) = self
                .0
                .find_artifact_manifest_by_run(run_id)
                .await
                .map_err(|error| diagnostic(&error))?
            else {
                return Ok(());
            };
            let snapshot = expected.clone();
            let observed = tokio::task::spawn_blocking(move || {
                crate::api::artifact::check_manifest_snapshot(snapshot)
            })
            .await
            .map_err(|_| "ARTIFACT_CHECK_WORKER_FAILED".to_owned())?;
            match self
                .0
                .commit_terminal_artifact_check(&expected, &observed)
                .await
                .map_err(|error| diagnostic(&error))?
            {
                ArtifactCheckCommit::Saved | ArtifactCheckCommit::AlreadyChecked => return Ok(()),
                ArtifactCheckCommit::Changed => {}
            }
        }
        Err("ARTIFACT_CHECK_CONFLICT".into())
    }
}

fn diagnostic(error: &zk_db::DbError) -> String {
    error.diagnostic_code().to_owned()
}

impl RunTerminalObserver for ArtifactIntegrityObserver {
    fn observe<'a>(&'a self, run_id: &'a str) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(self.check(run_id))
    }
}
