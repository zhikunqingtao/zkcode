//! Bounded removal of disposable projections. Authoritative results, evidence,
//! usage, resource ownership and the latest recovery checkpoint are never pruned.

use crate::{Db, DbError};
use serde::Serialize;

const DAY_MS: i64 = 86_400_000;

/// Counts from one bounded, atomic maintenance batch.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceReport {
    /// Unreferenced checkpoints older than one day.
    pub orphan_checkpoints: usize,
    /// Resolved diagnostics older than thirty days.
    pub resolved_anomalies: usize,
    /// Old transient transport deltas; authoritative facts remain.
    pub terminal_projections: usize,
}

impl MaintenanceReport {
    /// Whether no disposable rows were found in this batch.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.orphan_checkpoints == 0
            && self.resolved_anomalies == 0
            && self.terminal_projections == 0
    }
}

impl Db {
    /// Delete at most `batch` disposable rows per category in one transaction.
    /// Checkpoint age is 24 hours; resolved anomalies and terminal transport
    /// deltas are retained for 30 days. Recovery/cleanup uncertainty blocks pruning.
    ///
    /// # Errors
    /// Invalid bounds and any storage failure abort the entire batch.
    pub async fn maintain_runtime_projections(
        &self,
        now: i64,
        batch: usize,
    ) -> Result<MaintenanceReport, DbError> {
        if now < 30 * DAY_MS || !(1..=1000).contains(&batch) {
            return Err(DbError::Invalid("MAINTENANCE_BOUNDS_INVALID".into()));
        }
        let limit = i64::try_from(batch)
            .map_err(|_| DbError::Invalid("MAINTENANCE_BOUNDS_INVALID".into()))?;
        let checkpoint_before = crate::time::format_rfc3339_micros(now - DAY_MS);
        let projection_before = crate::time::format_rfc3339_micros(now - 30 * DAY_MS);
        self.with_writer(move |conn| {
            let tx=conn.transaction()?;
            let orphan_checkpoints=tx.execute(
                "DELETE FROM agent_checkpoints WHERE id IN (
                   SELECT checkpoint.id FROM agent_checkpoints checkpoint
                   WHERE checkpoint.created_at<?1
                     AND NOT EXISTS(SELECT 1 FROM run_envelopes owner WHERE owner.checkpoint_id=checkpoint.id)
                     AND NOT EXISTS(SELECT 1 FROM run_envelopes owner
                         JOIN tasks task ON task.id=owner.task_id
                         WHERE owner.id=checkpoint.run_id AND
                           (owner.status NOT IN ('completed','failed','cancelled','interrupted')
                            OR owner.cleanup_status NOT IN ('notRequired','confirmed')
                            OR task.status NOT IN ('succeeded','partial','failed','cancelled')))
                   ORDER BY checkpoint.created_at,checkpoint.id LIMIT ?2
                 )",rusqlite::params![checkpoint_before,limit])?;
            let resolved_anomalies=tx.execute(
                "DELETE FROM anomaly_events WHERE id IN (
                   SELECT id FROM anomaly_events WHERE resolved_at IS NOT NULL AND resolved_at<?1
                   ORDER BY resolved_at,id LIMIT ?2
                 )",rusqlite::params![now-30*DAY_MS,limit])?;
            let mut statement=tx.prepare(
                "SELECT event.id,event.seq,event.run_id FROM run_event_log event
                 JOIN run_envelopes run ON run.id=event.run_id
                 JOIN tasks task ON task.id=run.task_id
                 JOIN tasks root ON root.id=task.root_task_id
                 WHERE event.ts<?1 AND run.terminal_at<?2
                   AND run.status IN ('completed','failed','cancelled')
                   AND run.cleanup_status IN ('notRequired','confirmed')
                   AND root.status IN ('succeeded','partial','failed','cancelled')
                   AND root.cleanup_status IN ('notRequired','confirmed')
                   AND NOT EXISTS(SELECT 1 FROM task_dependencies dep
                       JOIN task_results result ON result.task_id=dep.child_task_id
                       WHERE dep.child_task_id=task.id AND
                         COALESCE(dep.consumed_result_version,0)<result.result_version)
                   AND event.event_type IN ('ws_stream_delta','ws_thinking_delta','ws_tool_input_delta','ws_tool_use_progress')
                   AND event.seq<(SELECT MAX(tail.seq) FROM run_event_log tail WHERE tail.run_id=event.run_id)
                 ORDER BY event.id LIMIT ?3")?;
            let disposable=statement.query_map(rusqlite::params![now-30*DAY_MS,projection_before,limit],
                |row|Ok((row.get::<_,i64>(0)?,row.get::<_,i64>(1)?,row.get::<_,String>(2)?)))?
                .collect::<Result<Vec<_>,_>>()?;
            drop(statement);
            let mut terminal_projections=0;
            for (id,seq,run) in disposable {
                tx.execute("INSERT INTO run_event_retention(run_id,through_event_id,through_seq) VALUES(?1,?2,?3)
                            ON CONFLICT(run_id) DO UPDATE SET
                              through_event_id=MAX(through_event_id,excluded.through_event_id),
                              through_seq=MAX(through_seq,excluded.through_seq)",rusqlite::params![run,id,seq])?;
                terminal_projections+=tx.execute("DELETE FROM run_event_log WHERE id=?1",[id])?;
            }
            tx.commit()?;
            Ok(MaintenanceReport {orphan_checkpoints,resolved_anomalies,terminal_projections})
        }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AnomalyEventRecord, new_agent_checkpoint};
    use serde_json::json;

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "End-to-end retention fixture checks protected and disposable rows in the same maintenance pass"
    )]
    async fn maintenance_prunes_only_old_disposable_rows_and_expires_replay_cursors() {
        let db = Db::open_in_memory().unwrap();
        let now = crate::time::now_millis();
        let old = now - 40 * DAY_MS;
        let session = db.create_session("fixture", "/tmp").await.unwrap().id;
        db.start_run("maintenance-run", &session, None, Some("query"), "fixture")
            .await
            .unwrap();
        // An initial cursor is valid until actual retention removed a range.
        assert!(db.get_run_events("maintenance-run", -1, 100).await.is_ok());
        assert!(db.get_ws_outbox_events_after(&session, -1).await.is_ok());
        let mut first = new_agent_checkpoint(
            "maintenance-run",
            &session,
            "agent",
            1,
            json!({"saved":"old"}),
        );
        first.created_at = crate::time::format_rfc3339_micros(old);
        db.save_agent_checkpoint(&first).await.unwrap();
        let mut latest = new_agent_checkpoint(
            "maintenance-run",
            &session,
            "agent",
            2,
            json!({"saved":"referenced"}),
        );
        latest.created_at = first.created_at.clone();
        db.save_agent_checkpoint(&latest).await.unwrap();
        for event in [
            "ws_stream_delta",
            "ws_thinking_delta",
            "tool_result",
            "ws_message_complete",
        ] {
            db.append_run_event("maintenance-run", event, None, &json!({"original":event}))
                .await
                .unwrap();
        }
        let task = db
            .find_run_by_id("maintenance-run")
            .await
            .unwrap()
            .unwrap()
            .task_id;
        let task_copy = task.clone();
        db.ensure_task_final_assistant(&task, "maintenance-run", "completed result")
            .await
            .unwrap();
        let task_record = db.find_runtime_task_by_id(&task).await.unwrap().unwrap();
        db.commit_task_result_with_run_usage_fallback(
            &crate::CommitTaskResult {
                task_id: task.clone(),
                run_id: "maintenance-run".into(),
                expected_task_version: task_record.version,
                status: crate::ResultStatus::Complete,
                content: "completed result".into(),
                media_type: "text/plain".into(),
                error_code: None,
                cleanup_status: crate::CleanupStatus::Confirmed,
                verification_status: crate::VerificationStatus::NotRequested,
            },
            crate::RunUsageFallback {
                input_tokens: 10,
                output_tokens: 2,
                cost_nanos_usd: 991,
                usage_complete: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.with_writer(move |conn| {
            let old_text = crate::time::format_rfc3339_micros(old);
            conn.execute(
                "UPDATE run_envelopes SET terminal_at=?1,finished_at=?1 WHERE id='maintenance-run'",
                [&old_text],
            )?;
            conn.execute(
                "UPDATE tasks SET terminal_at=?1 WHERE id=?2",
                rusqlite::params![old_text, task_copy],
            )?;
            conn.execute(
                "UPDATE run_event_log SET ts=?1 WHERE run_id='maintenance-run'",
                [old],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        for (id, resolved) in [
            ("old-resolved", Some(old)),
            ("unresolved", None),
            ("new-resolved", Some(now)),
        ] {
            db.save_anomaly_event(&AnomalyEventRecord {
                id: id.into(),
                swarm_id: "fixture".into(),
                worker_id: "worker".into(),
                rule_id: "fixture".into(),
                severity: "info".into(),
                message: "diagnostic".into(),
                detected_at: old,
                resolved_at: resolved,
                resolution: resolved.map(|_| "resolved".into()),
                context_snapshot: None,
            })
            .await
            .unwrap();
        }
        let high = db
            .get_run_events("maintenance-run", 0, 100)
            .await
            .unwrap()
            .last()
            .unwrap()
            .seq;
        let first_page = db.maintain_runtime_projections(now, 1).await.unwrap();
        assert_eq!(first_page.orphan_checkpoints, 1);
        assert_eq!(first_page.resolved_anomalies, 1);
        assert_eq!(first_page.terminal_projections, 1);
        assert!(
            matches!(db.get_run_events("maintenance-run",0,100).await,Err(DbError::Conflict(code)) if code=="RUNTIME_CURSOR_EXPIRED")
        );
        assert!(
            matches!(db.get_ws_outbox_events_after(&session,0).await,Err(DbError::Conflict(code)) if code=="RUNTIME_CURSOR_EXPIRED")
        );
        let second_page = db.maintain_runtime_projections(now, 1).await.unwrap();
        assert_eq!(second_page.terminal_projections, 1);
        assert!(
            db.maintain_runtime_projections(now, 1)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            db.latest_agent_checkpoint("maintenance-run")
                .await
                .unwrap()
                .unwrap()
                .id,
            latest.id
        );
        assert_eq!(
            db.find_anomalies_by_swarm("fixture").await.unwrap().len(),
            2
        );
        let (count,tail,cost):(i64,i64,i64)=db.with_reader(|conn| Ok((
            conn.query_row("SELECT COUNT(*) FROM run_event_log WHERE run_id='maintenance-run' AND event_type='tool_result'",[],|row|row.get(0))?,
            conn.query_row("SELECT MAX(seq) FROM run_event_log WHERE run_id='maintenance-run'",[],|row|row.get(0))?,
            conn.query_row("SELECT cost_nanos_usd FROM run_envelopes WHERE id='maintenance-run'",[],|row|row.get(0))?,
        ))).await.unwrap();
        assert_eq!((count, tail, cost), (1, high, 991));
        assert_eq!(
            db.read_task_result(&task, None, 0, 100)
                .await
                .unwrap()
                .unwrap()
                .content,
            "completed result"
        );
        db.append_run_event(
            "maintenance-run",
            "verification_stale",
            None,
            &json!({"reason":"changed"}),
        )
        .await
        .unwrap();
        assert_eq!(
            db.get_run_events("maintenance-run", high, 10)
                .await
                .unwrap()[0]
                .seq,
            high + 1
        );
    }

    #[tokio::test]
    async fn active_or_cleanup_uncertain_runs_keep_their_checkpoint_and_event_projections() {
        let db = Db::open_in_memory().unwrap();
        let now = crate::time::now_millis();
        let old = now - 40 * DAY_MS;
        for run in ["active", "uncertain"] {
            let session = db.create_session("fixture", "/tmp").await.unwrap().id;
            db.start_run(run, &session, None, Some("query"), "fixture")
                .await
                .unwrap();
            for seq in 1..=2 {
                let mut checkpoint =
                    new_agent_checkpoint(run, &session, "agent", seq, json!({"checkpoint":seq}));
                checkpoint.created_at = crate::time::format_rfc3339_micros(old);
                db.save_agent_checkpoint(&checkpoint).await.unwrap();
            }
            db.append_run_event(run, "ws_stream_delta", None, &json!({"delta":"pending"}))
                .await
                .unwrap();
        }
        db.with_writer(move |conn| {
            conn.execute("UPDATE run_event_log SET ts=?1",[old])?;
            conn.execute("UPDATE run_envelopes SET status='cancelled',terminal_at=?1,cleanup_status='unconfirmed' WHERE id='uncertain'",[crate::time::format_rfc3339_micros(old)])?;
            Ok(())
        }).await.unwrap();
        assert!(
            db.maintain_runtime_projections(now, 500)
                .await
                .unwrap()
                .is_empty()
        );
        let checkpoints: i64 = db
            .with_reader(|conn| {
                Ok(
                    conn.query_row("SELECT COUNT(*) FROM agent_checkpoints", [], |row| {
                        row.get(0)
                    })?,
                )
            })
            .await
            .unwrap();
        assert_eq!(checkpoints, 4);
        assert!(
            !db.get_run_events("active", 0, 100)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
