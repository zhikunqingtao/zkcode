//! One authoritative startup reconciliation for the durable execution runtime.
//!
//! The data-directory process lock guarantees that no executor can still own the
//! rows reconciled here. All lifecycle, cleanup, usage, and outbox projections are
//! committed together, so a crash cannot leave a half-reconciled Task/Run pair.

#![allow(clippy::missing_errors_doc, missing_docs)]

use rusqlite::{Connection, params};
use serde::Serialize;
use serde_json::json;

use crate::time::{format_rfc3339_micros, now_millis};
use crate::{Db, DbError};

const ACTIVE_RUN_STATUSES: &str =
    "'queued','running','waitingDependencies','waitingInteraction','cancelling'";
const RESTART_REASON: &str = "Execution interrupted by service restart";

/// Exact row counts changed by one startup reconciliation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestartReconciliationReport {
    pub tasks_needing_attention: usize,
    pub runs_interrupted: usize,
    pub invocations_interrupted: usize,
    pub resources_unconfirmed: usize,
    pub llm_calls_failed: usize,
    pub budget_reservations_incomplete: usize,
    /// Replayable websocket events written; diagnostic Run events are excluded.
    pub outbox_events: usize,
}

/// Rows moved across the durable shutdown-intent boundary before any in-process
/// cancellation token is signalled.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeShutdownIntentReport {
    pub tasks_requested: usize,
    pub runs_requested: usize,
}

#[derive(Debug)]
struct ActiveRun {
    run_id: String,
    task_id: String,
    previous_status: String,
}

fn active_runs(connection: &Connection) -> Result<Vec<ActiveRun>, DbError> {
    let sql = format!(
        "SELECT id,task_id,status FROM run_envelopes
         WHERE status IN ({ACTIVE_RUN_STATUSES}) ORDER BY created_at,id"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement
        .query_map([], |row| {
            Ok(ActiveRun {
                run_id: row.get(0)?,
                task_id: row.get(1)?,
                previous_status: row.get(2)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn run_cleanup_status(connection: &Connection, run_id: &str) -> Result<&'static str, DbError> {
    let (unconfirmed, pending, confirmed): (i64, i64, i64) = connection.query_row(
        "SELECT
            EXISTS(SELECT 1 FROM tool_invocations
                   WHERE run_id=?1 AND cleanup_status='unconfirmed')
            OR EXISTS(SELECT 1 FROM execution_resources
                      WHERE run_id=?1 AND status='unconfirmed'),
            EXISTS(SELECT 1 FROM tool_invocations
                   WHERE run_id=?1 AND cleanup_status='pending')
            OR EXISTS(SELECT 1 FROM execution_resources
                      WHERE run_id=?1 AND status IN ('allocated','stopping')),
            EXISTS(SELECT 1 FROM tool_invocations
                   WHERE run_id=?1 AND cleanup_status='confirmed')
            OR EXISTS(SELECT 1 FROM execution_resources
                      WHERE run_id=?1 AND status='released')",
        params![run_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    Ok(if unconfirmed != 0 {
        "unconfirmed"
    } else if pending != 0 {
        "pending"
    } else if confirmed != 0 {
        "confirmed"
    } else {
        "notRequired"
    })
}

fn append_v4_event(
    connection: &Connection,
    run_id: &str,
    event_type: &str,
    payload: &serde_json::Value,
    timestamp_ms: i64,
) -> Result<(), DbError> {
    let sequence: i64 = connection.query_row(
        "SELECT COALESCE(MAX(seq),-1)+1 FROM run_event_log WHERE run_id=?1",
        params![run_id],
        |row| row.get(0),
    )?;
    let envelope = json!({
        "schemaVersion": 4,
        "entityId": run_id,
        "data": payload,
    });
    connection.execute(
        "INSERT INTO run_event_log(run_id,seq,event_type,event_data,ts)
         VALUES(?1,?2,?3,?4,?5)",
        params![
            run_id,
            sequence,
            event_type,
            envelope.to_string(),
            timestamp_ms
        ],
    )?;
    Ok(())
}

impl Db {
    /// Persist process-wide `serviceRestart` cancellation intent for every
    /// non-terminal Task/Run in one transaction.
    ///
    /// The caller must close execution intake before entering this boundary and
    /// may signal cancellation tokens only after this method returns. Keeping
    /// the transition database-wide also quarantines a durable queued Run which
    /// lost its process-local scheduler registration.
    pub async fn request_runtime_shutdown(&self) -> Result<RuntimeShutdownIntentReport, DbError> {
        self.with_writer(move |connection| {
            let tx = connection.transaction()?;
            let runs = active_runs(&tx)?;
            if runs.is_empty() {
                tx.commit()?;
                return Ok(RuntimeShutdownIntentReport::default());
            }

            let timestamp_ms = now_millis();
            let now = format_rfc3339_micros(timestamp_ms);
            let mut report = RuntimeShutdownIntentReport::default();
            for run in runs {
                let task_updated = tx.execute(
                    "UPDATE tasks
                     SET status='cancelling',reason=?1,cleanup_status='pending',
                         updated_at=?2,version=version+1
                     WHERE id=?3 AND current_run_id=?4
                       AND status IN ('queued','running','waitingDependencies',
                                      'waitingInteraction','cancelling')",
                    params![RESTART_REASON, now, run.task_id, run.run_id],
                )?;
                if task_updated != 1 {
                    return Err(DbError::Invalid(
                        "SHUTDOWN_TASK_RUN_INTENT_MISMATCH".to_owned(),
                    ));
                }
                let run_updated = tx.execute(
                    &format!(
                        "UPDATE run_envelopes
                         SET status='cancelling',requested_exit_reason='serviceRestart',
                             abort_reason='serviceRestart',waiting_reason=NULL,
                             cleanup_status='pending',updated_at=?1,version=version+1
                         WHERE id=?2 AND task_id=?3 AND status IN ({ACTIVE_RUN_STATUSES})"
                    ),
                    params![now, run.run_id, run.task_id],
                )?;
                if run_updated != 1 {
                    return Err(DbError::Invalid("SHUTDOWN_RUN_INTENT_CONFLICT".to_owned()));
                }
                append_v4_event(
                    &tx,
                    &run.run_id,
                    "task_cancelling",
                    &json!({
                        "protocolVersion": 4,
                        "taskId": run.task_id,
                        "runId": run.run_id,
                        "exitReason": "serviceRestart",
                        "reason": RESTART_REASON,
                    }),
                    timestamp_ms,
                )?;
                report.tasks_requested += 1;
                report.runs_requested += 1;
            }
            tx.commit()?;
            Ok(report)
        })
        .await
    }

    /// Conservatively mark local execution owners which missed the shutdown
    /// deadline. Startup-style reconciliation has already made the Run
    /// `interrupted`; this second CAS records that future/resource cleanup was
    /// not observed before the process deadline.
    pub async fn mark_shutdown_cleanup_unconfirmed(
        &self,
        task_runs: &[(String, String)],
    ) -> Result<usize, DbError> {
        let task_runs = task_runs.to_vec();
        self.with_writer(move |connection| {
            let tx = connection.transaction()?;
            let timestamp_ms = now_millis();
            let now = format_rfc3339_micros(timestamp_ms);
            let mut updated = 0_usize;
            for (task_id, run_id) in task_runs {
                let run_updated = tx.execute(
                    "UPDATE run_envelopes SET cleanup_status='unconfirmed',
                        updated_at=?1,version=version+1
                     WHERE id=?2 AND task_id=?3 AND status='interrupted'
                       AND exit_reason='serviceRestart'
                       AND cleanup_status<>'unconfirmed'",
                    params![now, run_id, task_id],
                )?;
                if run_updated == 0 {
                    continue;
                }
                let task_updated = tx.execute(
                    "UPDATE tasks SET cleanup_status='unconfirmed',updated_at=?1,
                        version=version+1
                     WHERE id=?2 AND current_run_id=?3 AND status='needsAttention'",
                    params![now, task_id, run_id],
                )?;
                if task_updated != 1 {
                    return Err(DbError::Invalid(
                        "SHUTDOWN_UNCONFIRMED_TASK_RUN_MISMATCH".to_owned(),
                    ));
                }
                append_v4_event(
                    &tx,
                    &run_id,
                    "run_cleanup_unconfirmed",
                    &json!({
                        "protocolVersion": 4,
                        "taskId": task_id,
                        "runId": run_id,
                        "cleanupStatus": "unconfirmed",
                        "exitReason": "serviceRestart",
                    }),
                    timestamp_ms,
                )?;
                updated += 1;
            }
            tx.commit()?;
            Ok(updated)
        })
        .await
    }

    /// Reconcile every execution attempt owned by a previous process in one
    /// transaction. Repeating this method after a successful commit is a no-op.
    #[allow(clippy::too_many_lines)] // one transaction intentionally owns every restart projection
    pub async fn reconcile_runtime_after_restart(
        &self,
    ) -> Result<RestartReconciliationReport, DbError> {
        self.with_writer(move |connection| {
            let tx = connection.transaction()?;
            let runs = active_runs(&tx)?;
            let timestamp_ms = now_millis();
            let now = format_rfc3339_micros(timestamp_ms);

            let resources_unconfirmed = tx.execute(
                &format!(
                    "UPDATE execution_resources
                     SET status='unconfirmed',released_at=NULL,updated_at=?1,version=version+1
                     WHERE status IN ('allocated','stopping')
                       AND run_id IN (
                           SELECT id FROM run_envelopes
                           WHERE status IN ({ACTIVE_RUN_STATUSES})
                       )"
                ),
                params![now],
            )?;

            let invocations_interrupted = tx.execute(
                &format!(
                    "UPDATE tool_invocations
                     SET status='interrupted',
                         error_code=COALESCE(error_code,'SERVICE_RESTART'),
                         cleanup_status=CASE
                           WHEN EXISTS(
                             SELECT 1 FROM execution_resources resource
                             WHERE resource.invocation_id=tool_invocations.invocation_id
                               AND resource.status='unconfirmed'
                           ) THEN 'unconfirmed'
                           WHEN EXISTS(
                             SELECT 1 FROM execution_resources resource
                             WHERE resource.invocation_id=tool_invocations.invocation_id
                               AND resource.status='released'
                           ) THEN 'confirmed'
                           WHEN status IN ('preparing','queued') THEN 'notRequired'
                           WHEN side_effect_class IN ('write','unknown') THEN 'unconfirmed'
                           ELSE 'notRequired'
                         END,
                         terminal_at=?1,updated_at=?1,version=version+1
                     WHERE status IN ('preparing','queued','running')
                       AND run_id IN (
                           SELECT id FROM run_envelopes
                           WHERE status IN ({ACTIVE_RUN_STATUSES})
                       )"
                ),
                params![now],
            )?;

            tx.execute(
                &format!(
                    "UPDATE tool_invocations
                     SET cleanup_status=CASE
                           WHEN EXISTS(
                             SELECT 1 FROM execution_resources resource
                             WHERE resource.invocation_id=tool_invocations.invocation_id
                               AND resource.status='released'
                           ) AND NOT EXISTS(
                             SELECT 1 FROM execution_resources resource
                             WHERE resource.invocation_id=tool_invocations.invocation_id
                               AND resource.status='unconfirmed'
                           ) THEN 'confirmed'
                           ELSE 'unconfirmed'
                         END,
                         updated_at=?1
                     WHERE cleanup_status='pending'
                       AND status IN ('succeeded','failed','cancelled','interrupted')
                       AND run_id IN (
                           SELECT id FROM run_envelopes
                           WHERE status IN ({ACTIVE_RUN_STATUSES})
                       )"
                ),
                params![now],
            )?;

            let unknown_usage_runs = {
                // The process/data-directory lock proves that every still-started
                // physical call belongs to the previous executor.  Do not scope
                // this to active Runs: a stream can be dropped immediately after
                // its Run commits terminal state, then lose the asynchronous
                // completion write to process shutdown or a transient SQLite
                // failure.  Such a row must not remain `started` forever.
                let mut statement =
                    tx.prepare("SELECT DISTINCT run_id FROM llm_calls WHERE status='started'")?;
                statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?
            };

            let llm_calls_failed = tx.execute(
                "UPDATE llm_calls
                 SET status='failed',usage_complete=0,
                     error_code=COALESCE(error_code,'SERVICE_RESTART'),
                     finished_at=?1,updated_at=?1
                 WHERE status='started'",
                params![now],
            )?;

            for run_id in &unknown_usage_runs {
                tx.execute(
                    "UPDATE run_envelopes SET usage_complete=0,updated_at=?1
                     WHERE id=?2",
                    params![now, run_id],
                )?;
                tx.execute(
                    "UPDATE tasks SET usage_complete=0,budget_version=budget_version+1,
                        updated_at=?1
                     WHERE id IN (
                         SELECT task.id FROM tasks task
                         JOIN run_envelopes run ON run.task_id=task.id
                         WHERE run.id=?2
                         UNION
                         SELECT task.root_task_id FROM tasks task
                         JOIN run_envelopes run ON run.task_id=task.id
                         WHERE run.id=?2
                     )",
                    params![now, run_id],
                )?;
            }

            // needsAttention is deliberately recoverable rather than terminal. Keep a
            // child's active allocation when every physical LLM call has authoritative
            // usage so a later operator-approved attempt reuses the same hard ceiling.
            // If a stream died without final usage, freeze the allocation as incomplete:
            // it becomes non-active but no reserved tokens or cost are returned to root.
            let budget_reservations_incomplete = tx.execute(
                "UPDATE task_budget_reservations
                 SET status='incomplete',usage_complete=0,settled_at=?1,version=version+1
                 WHERE status='active'
                   AND child_task_id IN (
                       SELECT DISTINCT call.task_id FROM llm_calls call
                       WHERE call.status='failed'
                         AND call.usage_complete=0
                         AND call.finished_at=?1
                   )",
                params![now],
            )?;

            let mut report = RestartReconciliationReport {
                invocations_interrupted,
                resources_unconfirmed,
                llm_calls_failed,
                budget_reservations_incomplete,
                ..RestartReconciliationReport::default()
            };

            for run in runs {
                let cleanup_status = run_cleanup_status(&tx, &run.run_id)?;
                let run_updated = tx.execute(
                    &format!(
                        "UPDATE run_envelopes
                         SET status='interrupted',finished_at=?1,terminal_at=?1,
                             exit_reason='serviceRestart',abort_reason='serviceRestart',
                             waiting_reason=NULL,error_summary=COALESCE(error_summary,?2),
                             cleanup_status=?3,updated_at=?1,version=version+1
                         WHERE id=?4 AND status IN ({ACTIVE_RUN_STATUSES})"
                    ),
                    params![now, RESTART_REASON, cleanup_status, run.run_id],
                )?;
                if run_updated != 1 {
                    return Err(DbError::Invalid(
                        "RESTART_RUN_RECONCILIATION_CONFLICT".to_owned(),
                    ));
                }
                let task_updated = tx.execute(
                    "UPDATE tasks
                     SET status='needsAttention',reason=?1,cleanup_status=?2,
                         terminal_at=NULL,updated_at=?3,version=version+1
                     WHERE id=?4 AND current_run_id=?5
                       AND status IN ('queued','running','waitingDependencies',
                                      'waitingInteraction','cancelling')",
                    params![RESTART_REASON, cleanup_status, now, run.task_id, run.run_id],
                )?;
                if task_updated != 1 {
                    return Err(DbError::Invalid(
                        "RESTART_TASK_RUN_RECONCILIATION_MISMATCH".to_owned(),
                    ));
                }
                append_v4_event(
                    &tx,
                    &run.run_id,
                    "run_status_changed",
                    &json!({
                        "protocolVersion": 4,
                        "taskId": run.task_id,
                        "runId": run.run_id,
                        "from": run.previous_status,
                        "to": "interrupted",
                        "exitReason": "serviceRestart",
                        "cleanupStatus": cleanup_status,
                    }),
                    timestamp_ms,
                )?;
                append_v4_event(
                    &tx,
                    &run.run_id,
                    "ws_task_update",
                    &json!({
                        "type": "task_update",
                        "taskId": run.task_id,
                        "status": "needsAttention",
                        "progress": RESTART_REASON,
                    }),
                    timestamp_ms,
                )?;
                report.runs_interrupted += 1;
                report.tasks_needing_attention += 1;
                report.outbox_events += 1;
            }

            tx.commit()?;
            Ok(report)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CasOutcome, CleanupStatus, CommitTaskResult, CommitTaskResultOutcome, CreateTaskWithRun,
        LlmCallBudgetReservation, MessageAttribution, MessageRole, NewExecutionResource,
        NewLlmCall, NewMessage, NewToolInvocation, ResultStatus, StoredBlock, ToolInvocationStatus,
        VerificationStatus,
    };
    use sha2::{Digest, Sha256};

    fn id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn request(
        session_id: &str,
        task_id: String,
        run_id: String,
        parent: Option<(&str, &str)>,
    ) -> CreateTaskWithRun {
        let child = parent.is_some();
        CreateTaskWithRun {
            task_id,
            run_id,
            root_session_id: session_id.to_owned(),
            transcript_session_id: if child { id() } else { session_id.to_owned() },
            parent_task_id: parent.map(|value| value.0.to_owned()),
            parent_run_id: parent.map(|value| value.1.to_owned()),
            creator_tool_use_id: child.then(id),
            ordinal: i64::from(child),
            description: "restart fixture".to_owned(),
            prompt: Some("fixture prompt".to_owned()),
            task_type: "agent".to_owned(),
            model: "test-model".to_owned(),
            working_dir: "/tmp/restart-reconciliation".to_owned(),
            execution_config_json: if child {
                "{}".to_owned()
            } else {
                serde_json::json!({
                    "budget": {
                        "tokenLimit": 1_000,
                        "costLimitNanosUsd": 10_000,
                        "deadlineAtMs": crate::time::now_millis() + 60_000,
                    }
                })
                .to_string()
            },
            startup_epoch: 1,
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // verifies the complete atomic restart projection
    async fn reconciliation_is_atomic_durable_and_idempotent() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/restart-reconciliation")
            .await
            .expect("session");
        let root_task_id = id();
        let root_run_id = id();
        let root = db
            .create_task_with_run(&request(
                &session.id,
                root_task_id.clone(),
                root_run_id.clone(),
                None,
            ))
            .await
            .expect("root");
        assert_eq!(
            db.claim_task_run_cas(&root_task_id, &root_run_id, root.task.version)
                .await
                .expect("claim root"),
            CasOutcome::Applied
        );
        let child_task_id = id();
        let child_run_id = id();
        let child = db
            .create_task_with_run(&request(
                &session.id,
                child_task_id.clone(),
                child_run_id.clone(),
                Some((&root_task_id, &root_run_id)),
            ))
            .await
            .expect("child");
        assert_eq!(
            db.claim_task_run_cas(&child_task_id, &child_run_id, child.task.version)
                .await
                .expect("claim child"),
            CasOutcome::Applied
        );

        let preparing_id = id();
        let read_id = id();
        let write_id = id();
        for (invocation_id, tool_use_id, side_effect) in [
            (&preparing_id, "preparing", "unknown"),
            (&read_id, "read", "read"),
            (&write_id, "write", "write"),
        ] {
            db.create_tool_invocation(&NewToolInvocation {
                invocation_id: invocation_id.clone(),
                task_id: child_task_id.clone(),
                run_id: child_run_id.clone(),
                tool_use_id: tool_use_id.to_owned(),
                tool_name: "fixture".to_owned(),
                input_json: Some("{}".to_owned()),
                side_effect_class: side_effect.to_owned(),
                directory_generation: None,
                connection_generation: None,
            })
            .await
            .expect("invocation");
        }
        for invocation_id in [&read_id, &write_id] {
            assert_eq!(
                db.transition_tool_invocation_cas(
                    invocation_id,
                    0,
                    ToolInvocationStatus::Running,
                    Some("{}"),
                    None,
                    None,
                    CleanupStatus::Pending,
                )
                .await
                .expect("run invocation"),
                CasOutcome::Applied
            );
        }
        let resource_id = id();
        db.register_execution_resource(&NewExecutionResource {
            resource_id: resource_id.clone(),
            task_id: child_task_id.clone(),
            run_id: child_run_id.clone(),
            invocation_id: Some(write_id.clone()),
            resource_kind: "processGroup".to_owned(),
            external_id: Some("4242".to_owned()),
            metadata_json: "{}".to_owned(),
        })
        .await
        .expect("resource");

        let call_id = id();
        db.start_llm_call_with_budget(
            &NewLlmCall {
                call_id: call_id.clone(),
                task_id: child_task_id.clone(),
                run_id: child_run_id.clone(),
                provider: "script".to_owned(),
                model: "test-model".to_owned(),
                route: None,
                provider_request_id: None,
            },
            &LlmCallBudgetReservation {
                input_tokens: 10,
                output_tokens: 10,
                cost_nanos_usd: 100,
            },
        )
        .await
        .expect("llm call");
        let call_for_seed = call_id.clone();
        db.with_writer(move |connection| {
            connection.execute(
                "UPDATE llm_calls SET input_tokens=9 WHERE call_id=?1",
                params![call_for_seed],
            )?;
            Ok(())
        })
        .await
        .expect("seed partial usage");

        let message = db
            .append_attributed_message(
                &child.transcript_session_id,
                NewMessage {
                    role: MessageRole::Assistant,
                    content: vec![StoredBlock::Text {
                        text: "durable partial evidence".to_owned(),
                    }],
                    stop_reason: None,
                    input_tokens: 0,
                    output_tokens: 0,
                },
                MessageAttribution {
                    task_id: Some(child_task_id.clone()),
                    run_id: Some(child_run_id.clone()),
                    origin: "runtime".to_owned(),
                    source_task_id: None,
                },
            )
            .await
            .expect("message");
        let partial = "already committed partial result";
        let digest = format!("{:x}", Sha256::digest(partial.as_bytes()));
        let result_id = id();
        let child_task_for_result = child_task_id.clone();
        let child_run_for_result = child_run_id.clone();
        let digest_for_result = digest.clone();
        db.with_writer(move |connection| {
            connection.execute(
                "INSERT INTO task_results
                    (result_id,task_id,run_id,result_version,status,inline_text,byte_len,
                     content_sha256,media_type,created_at)
                 VALUES(?1,?2,?3,1,'partial',?4,?5,?6,'text/plain',?7)",
                params![
                    result_id,
                    child_task_for_result,
                    child_run_for_result,
                    partial,
                    i64::try_from(partial.len()).expect("fixture length"),
                    digest_for_result,
                    format_rfc3339_micros(now_millis()),
                ],
            )?;
            Ok(())
        })
        .await
        .expect("partial result");

        let report = db
            .reconcile_runtime_after_restart()
            .await
            .expect("reconcile");
        assert_eq!(
            report,
            RestartReconciliationReport {
                tasks_needing_attention: 2,
                runs_interrupted: 2,
                invocations_interrupted: 3,
                resources_unconfirmed: 1,
                llm_calls_failed: 1,
                budget_reservations_incomplete: 1,
                outbox_events: 2,
            }
        );

        let task_states: (String, String, i64, i64) = db
            .with_conn_blocking(|connection| {
                Ok((
                    connection.query_row(
                        "SELECT status FROM tasks WHERE id=?1",
                        params![root_task_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT status FROM tasks WHERE id=?1",
                        params![child_task_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT usage_complete FROM tasks WHERE id=?1",
                        params![root_task_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT usage_complete FROM tasks WHERE id=?1",
                        params![child_task_id],
                        |row| row.get(0),
                    )?,
                ))
            })
            .expect("task states");
        assert_eq!(
            task_states,
            (
                "needsAttention".to_owned(),
                "needsAttention".to_owned(),
                0,
                0
            )
        );
        let ledger_states: (
            String,
            String,
            String,
            String,
            String,
            Option<i64>,
            Option<i64>,
            String,
        ) = db
            .with_conn_blocking(|connection| {
                Ok((
                    connection.query_row(
                        "SELECT cleanup_status FROM tool_invocations WHERE invocation_id=?1",
                        params![preparing_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT cleanup_status FROM tool_invocations WHERE invocation_id=?1",
                        params![read_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT cleanup_status FROM tool_invocations WHERE invocation_id=?1",
                        params![write_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT status FROM execution_resources WHERE resource_id=?1",
                        params![resource_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT status FROM llm_calls WHERE call_id=?1",
                        params![call_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT input_tokens FROM llm_calls WHERE call_id=?1",
                        params![call_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT output_tokens FROM llm_calls WHERE call_id=?1",
                        params![call_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT status FROM task_budget_reservations WHERE child_task_id=?1",
                        params![child_task_id],
                        |row| row.get(0),
                    )?,
                ))
            })
            .expect("ledger states");
        assert_eq!(ledger_states.0, "notRequired");
        assert_eq!(ledger_states.1, "notRequired");
        assert_eq!(ledger_states.2, "unconfirmed");
        assert_eq!(ledger_states.3, "unconfirmed");
        assert_eq!(ledger_states.4, "failed");
        assert_eq!(ledger_states.5, Some(9));
        assert_eq!(ledger_states.6, None, "unknown usage must remain NULL");
        assert_eq!(ledger_states.7, "incomplete");

        let preserved: (i64, String, i64) = db
            .with_conn_blocking(|connection| {
                Ok((
                    connection.query_row(
                        "SELECT COUNT(*) FROM messages WHERE id=?1",
                        params![message.id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT content_sha256 FROM task_results
                         WHERE task_id=?1 AND result_version=1",
                        params![child_task_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT COUNT(*) FROM task_results WHERE task_id=?1",
                        params![child_task_id],
                        |row| row.get(0),
                    )?,
                ))
            })
            .expect("preserved facts");
        assert_eq!(preserved, (1, digest, 1));

        let replay = db
            .get_ws_outbox_events_after(&session.id, 0)
            .await
            .expect("outbox replay");
        assert_eq!(replay.len(), 2);
        assert!(replay.iter().all(|event| {
            event.payload["type"] == "task_update" && event.payload["status"] == "needsAttention"
        }));

        let event_count_before = db
            .with_conn_blocking(|connection| {
                connection
                    .query_row("SELECT COUNT(*) FROM run_event_log", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(Into::into)
            })
            .expect("event count");
        assert_eq!(
            db.reconcile_runtime_after_restart()
                .await
                .expect("idempotent replay"),
            RestartReconciliationReport::default()
        );
        let event_count_after = db
            .with_conn_blocking(|connection| {
                connection
                    .query_row("SELECT COUNT(*) FROM run_event_log", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(Into::into)
            })
            .expect("event count");
        assert_eq!(event_count_after, event_count_before);
    }

    #[tokio::test]
    async fn known_usage_keeps_recoverable_child_allocation_reserved() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/restart-known-usage")
            .await
            .expect("session");
        let root_task_id = id();
        let root_run_id = id();
        let mut root_request =
            request(&session.id, root_task_id.clone(), root_run_id.clone(), None);
        root_request.execution_config_json = serde_json::json!({
            "budget": {
                "tokenLimit": 1_000,
                "costLimitNanosUsd": 10_000,
                "deadlineAtMs": crate::time::now_millis() + 60_000,
            }
        })
        .to_string();
        db.create_task_with_run(&root_request)
            .await
            .expect("root task/run");
        let child_task_id = id();
        let child_run_id = id();
        db.create_task_with_run(&request(
            &session.id,
            child_task_id.clone(),
            child_run_id,
            Some((&root_task_id, &root_run_id)),
        ))
        .await
        .expect("child task/run");

        let report = db
            .reconcile_runtime_after_restart()
            .await
            .expect("reconcile");
        assert_eq!(report.budget_reservations_incomplete, 0);
        let allocation: (String, Option<String>, i64, i64) = db
            .with_conn_blocking(|connection| {
                Ok((
                    connection.query_row(
                        "SELECT status FROM task_budget_reservations WHERE child_task_id=?1",
                        params![child_task_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT settled_at FROM task_budget_reservations WHERE child_task_id=?1",
                        params![child_task_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT budget_reserved_tokens FROM tasks WHERE id=?1",
                        params![root_task_id],
                        |row| row.get(0),
                    )?,
                    connection.query_row(
                        "SELECT budget_reserved_cost_nanos_usd FROM tasks WHERE id=?1",
                        params![root_task_id],
                        |row| row.get(0),
                    )?,
                ))
            })
            .expect("allocation");
        assert_eq!(allocation, ("active".to_owned(), None, 200, 2_000));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // one complete terminal-Run orphan-call reconciliation fixture
    async fn reconciliation_closes_started_llm_call_owned_by_terminal_run() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/restart-terminal-call")
            .await
            .expect("session");
        let run_id = id();
        db.start_run(&run_id, &session.id, None, Some("query"), "test-model")
            .await
            .expect("run");
        let call_id = id();
        db.start_llm_call(&NewLlmCall {
            call_id: call_id.clone(),
            task_id: run_id.clone(),
            run_id: run_id.clone(),
            provider: "script".to_owned(),
            model: "test-model".to_owned(),
            route: None,
            provider_request_id: None,
        })
        .await
        .expect("start physical call");

        // Reproduce the narrow crash window through the only legal terminal
        // boundary: Task/Run/immutable result commit succeeds, while the lazy
        // physical stream's completion write is still lost to process exit.
        let task = db
            .find_runtime_task_by_id(&run_id)
            .await
            .expect("task")
            .expect("task exists");
        assert!(matches!(
            db.commit_task_result(&CommitTaskResult {
                task_id: run_id.clone(),
                run_id: run_id.clone(),
                expected_task_version: task.version,
                status: ResultStatus::Error,
                content: "fixture terminal failure".to_owned(),
                media_type: "text/plain".to_owned(),
                error_code: Some("INTERNAL_ERROR".to_owned()),
                cleanup_status: CleanupStatus::NotRequired,
                verification_status: VerificationStatus::NotRequested,
            })
            .await
            .expect("terminalize owner before call completion"),
            CommitTaskResultOutcome::Committed { .. }
        ));

        let report = db
            .reconcile_runtime_after_restart()
            .await
            .expect("reconcile terminal owner");
        assert_eq!(report.llm_calls_failed, 1);
        assert_eq!(report.runs_interrupted, 0);
        assert_eq!(report.tasks_needing_attention, 0);
        let call_for_read = call_id.clone();
        let run_for_read = run_id.clone();
        let state: (
            String,
            i64,
            Option<i64>,
            Option<i64>,
            Option<String>,
            i64,
            i64,
        ) = db
            .with_conn_blocking(move |connection| {
                let call = connection.query_row(
                    "SELECT status,usage_complete,input_tokens,output_tokens,error_code
                     FROM llm_calls WHERE call_id=?1",
                    params![call_for_read],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                            row.get::<_, Option<String>>(4)?,
                        ))
                    },
                )?;
                let run_usage = connection.query_row(
                    "SELECT usage_complete FROM run_envelopes WHERE id=?1",
                    params![run_for_read],
                    |row| row.get::<_, i64>(0),
                )?;
                let task_usage = connection.query_row(
                    "SELECT usage_complete FROM tasks WHERE id=?1",
                    params![run_for_read],
                    |row| row.get::<_, i64>(0),
                )?;
                Ok((
                    call.0, call.1, call.2, call.3, call.4, run_usage, task_usage,
                ))
            })
            .expect("reconciled ledger state");
        assert_eq!(
            state,
            (
                "failed".to_owned(),
                0,
                None,
                None,
                Some("SERVICE_RESTART".to_owned()),
                0,
                0,
            ),
            "unknown provider usage must remain NULL and incomplete"
        );
        assert_eq!(
            db.reconcile_runtime_after_restart()
                .await
                .expect("idempotent replay"),
            RestartReconciliationReport::default()
        );
    }

    #[tokio::test]
    async fn concurrent_reconciliation_changes_each_run_once() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/restart-race")
            .await
            .expect("session");
        let task_id = id();
        let run_id = id();
        db.create_task_with_run(&request(&session.id, task_id, run_id, None))
            .await
            .expect("root");

        let (left, right) = tokio::join!(
            db.reconcile_runtime_after_restart(),
            db.reconcile_runtime_after_restart()
        );
        let left = left.expect("left");
        let right = right.expect("right");
        assert_eq!(left.runs_interrupted + right.runs_interrupted, 1);
        assert_eq!(
            left.tasks_needing_attention + right.tasks_needing_attention,
            1
        );
        assert_eq!(left.outbox_events + right.outbox_events, 1);
    }
}
