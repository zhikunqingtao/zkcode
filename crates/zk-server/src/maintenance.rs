//! Process-owned maintenance of disposable, unreferenced runtime projections.
use std::time::Duration;

/// Start bounded batches; abort the returned task during server shutdown.
#[must_use]
pub fn spawn(db: zk_db::Db) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_mins(15));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            // Restart/interruption reconciliation is read-only and idempotent.
            match db.pending_terminal_artifact_runs(16).await {
                Ok(runs) => {
                    for run in runs {
                        let observer =
                            crate::artifact_integrity::ArtifactIntegrityObserver(db.clone());
                        if !matches!(
                            tokio::time::timeout(Duration::from_secs(10), observer.check(&run))
                                .await,
                            Ok(Ok(()))
                        ) {
                            tracing::warn!(
                                run_id = run,
                                error_code = "ARTIFACT_TERMINAL_CHECK_PENDING",
                                "artifact projection retained for later inspection"
                            );
                        }
                    }
                }
                Err(error) => tracing::warn!(
                    code = error.diagnostic_code(),
                    "terminal artifact scan failed"
                ),
            }
            for _ in 0..8 {
                match db
                    .maintain_runtime_projections(zk_db::time::now_millis(), 250)
                    .await
                {
                    Ok(report) if report.is_empty() => break,
                    Ok(report) => tracing::info!(
                        orphan_checkpoints = report.orphan_checkpoints,
                        resolved_anomalies = report.resolved_anomalies,
                        terminal_projections = report.terminal_projections,
                        "disposable runtime projections pruned"
                    ),
                    Err(error) => {
                        tracing::warn!(
                            code = error.diagnostic_code(),
                            "runtime maintenance failed; retained projections will be retried later"
                        );
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        }
    })
}
