//! Periodic low-cardinality metrics projection for durable runtime health.

use std::time::Duration;

use zk_db::{Db, RuntimeHealthSnapshot};

/// Interval between runtime-health database snapshots.
pub const RUNTIME_HEALTH_REFRESH_INTERVAL: Duration = Duration::from_secs(15);

/// Start the runtime-health projector. The first snapshot is immediate; later
/// snapshots run every 15 seconds without catch-up bursts.
///
/// The returned handle is owned by the process composition root and must be
/// aborted during shutdown.
#[must_use]
pub fn spawn(db: Db) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(RUNTIME_HEALTH_REFRESH_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if refresh_once(&db).await.is_err() {
                metrics::counter!(
                    "zk_runtime_health_refresh_total",
                    "outcome" => "error"
                )
                .increment(1);
                // Do not attach the database error: this operational loop does
                // not need SQL text or values in ordinary production logs.
                tracing::warn!("runtime health metrics refresh failed");
            }
        }
    })
}

async fn refresh_once(db: &Db) -> Result<RuntimeHealthSnapshot, zk_db::DbError> {
    let snapshot = db.runtime_health_snapshot().await?;
    publish_snapshot(snapshot);
    metrics::counter!(
        "zk_runtime_health_refresh_total",
        "outcome" => "success"
    )
    .increment(1);
    Ok(snapshot)
}

fn publish_snapshot(snapshot: RuntimeHealthSnapshot) {
    metrics::gauge!("zk_runtime_orphan_active_runs")
        .set(count_as_gauge(snapshot.orphan_active_runs));
    metrics::gauge!("zk_runtime_unconsumed_child_results")
        .set(count_as_gauge(snapshot.unconsumed_child_results));
    metrics::gauge!("zk_runtime_cleanup_unconfirmed")
        .set(count_as_gauge(snapshot.cleanup_unconfirmed));
    metrics::gauge!("zk_runtime_terminal_llm_usage_missing")
        .set(count_as_gauge(snapshot.terminal_llm_usage_missing));
    metrics::gauge!("zk_runtime_needs_attention_tasks")
        .set(count_as_gauge(snapshot.needs_attention_tasks));
    metrics::gauge!("zk_runtime_recovery_anomalies")
        .set(count_as_gauge(snapshot.recovery_anomalies));
    metrics::gauge!("zk_runtime_queued_runs").set(count_as_gauge(snapshot.queued_runs));
    metrics::gauge!("zk_runtime_max_queue_wait_seconds")
        .set(Duration::from_millis(snapshot.max_queue_wait_ms).as_secs_f64());
}

#[allow(clippy::cast_precision_loss)]
fn count_as_gauge(value: u64) -> f64 {
    // Prometheus gauges are f64. Runtime row counts above 2^53 are impossible
    // for this local SQLite application, so the conversion is exact in practice.
    value as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn database_snapshot_renders_as_low_cardinality_prometheus_metrics() {
        crate::metrics_recorder::install_once();
        let database = Db::open_in_memory().expect("database");
        let empty = refresh_once(&database).await.expect("refresh");
        assert_eq!(empty, RuntimeHealthSnapshot::default());

        publish_snapshot(RuntimeHealthSnapshot {
            orphan_active_runs: 1,
            unconsumed_child_results: 2,
            cleanup_unconfirmed: 3,
            terminal_llm_usage_missing: 4,
            needs_attention_tasks: 5,
            recovery_anomalies: 6,
            queued_runs: 7,
            max_queue_wait_ms: 8_500,
        });
        let output = crate::metrics_recorder::render_snapshot();
        for expected in [
            "zk_runtime_orphan_active_runs 1",
            "zk_runtime_unconsumed_child_results 2",
            "zk_runtime_cleanup_unconfirmed 3",
            "zk_runtime_terminal_llm_usage_missing 4",
            "zk_runtime_needs_attention_tasks 5",
            "zk_runtime_recovery_anomalies 6",
            "zk_runtime_queued_runs 7",
            "zk_runtime_max_queue_wait_seconds 8.5",
            "zk_runtime_health_refresh_total{outcome=\"success\"} ",
        ] {
            assert!(
                output.contains(expected),
                "missing metric: {expected}\n{output}"
            );
        }
        for line in output
            .lines()
            .filter(|line| line.starts_with("zk_runtime_"))
        {
            assert!(!line.contains("taskId="));
            assert!(!line.contains("runId="));
        }
    }
}
