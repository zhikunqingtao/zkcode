//! Read-only operational projection for the durable task runtime.
//!
//! The projection intentionally returns counts and elapsed time only. It never
//! selects prompts, messages, result bodies, tool input, resource metadata, or
//! provider credentials, so it is safe to feed into low-cardinality metrics.

use crate::time::{now_millis, parse_rfc3339_millis};
use crate::{Db, error::DbError};

/// Point-in-time health of the durable task runtime.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeHealthSnapshot {
    /// Active runs that are not the owning task's matching current attempt.
    pub orphan_active_runs: u64,
    /// Child result versions for which the parent has no durable receipt.
    pub unconsumed_child_results: u64,
    /// Task, run, invocation, and resource rows whose cleanup is unconfirmed.
    pub cleanup_unconfirmed: u64,
    /// Terminal physical `LLM` calls without complete usage and price data.
    pub terminal_llm_usage_missing: u64,
    /// Tasks explicitly waiting for operator attention.
    pub needs_attention_tasks: u64,
    /// Broken `needsAttention`/`serviceRestart` task-run relationships.
    pub recovery_anomalies: u64,
    /// Runs currently waiting in the durable dispatch queue.
    pub queued_runs: u64,
    /// Age in milliseconds of the oldest currently queued run.
    pub max_queue_wait_ms: u64,
}

const RUNTIME_HEALTH_SQL: &str = r"
SELECT
    (SELECT COUNT(*)
       FROM run_envelopes AS run
       LEFT JOIN tasks AS task ON task.id=run.task_id
      WHERE run.status IN
            ('queued','running','waitingDependencies','waitingInteraction','cancelling')
        AND (task.id IS NULL
          OR task.current_run_id IS NULL
          OR task.current_run_id<>run.id
          OR NOT ((run.status='queued' AND task.status='queued')
               OR (run.status='running' AND task.status='running')
               OR (run.status='waitingDependencies' AND task.status='waitingDependencies')
               OR (run.status='waitingInteraction' AND task.status='waitingInteraction')
               OR (run.status='cancelling' AND task.status='cancelling')))),
    (SELECT COUNT(*)
       FROM task_results AS result
       JOIN task_dependencies AS dependency
         ON dependency.child_task_id=result.task_id
       LEFT JOIN task_result_receipts AS receipt
         ON receipt.consumer_task_id=dependency.parent_task_id
        AND receipt.producer_task_id=result.task_id
        AND receipt.result_version=result.result_version
      WHERE receipt.receipt_id IS NULL),
    (SELECT COUNT(*) FROM tasks WHERE cleanup_status='unconfirmed')
      + (SELECT COUNT(*) FROM run_envelopes WHERE cleanup_status='unconfirmed')
      + (SELECT COUNT(*) FROM tool_invocations WHERE cleanup_status='unconfirmed')
      + (SELECT COUNT(*) FROM execution_resources WHERE status='unconfirmed'),
    (SELECT COUNT(*)
       FROM llm_calls
      WHERE status IN ('completed','failed','cancelled')
        AND (usage_complete=0
          OR input_tokens IS NULL
          OR output_tokens IS NULL
          OR cache_read_tokens IS NULL
          OR cache_create_tokens IS NULL
          OR cost_nanos_usd IS NULL
          OR finished_at IS NULL)),
    (SELECT COUNT(*) FROM tasks WHERE status='needsAttention'),
    (SELECT COUNT(*)
       FROM tasks AS task
       LEFT JOIN run_envelopes AS current_run ON current_run.id=task.current_run_id
      WHERE task.status='needsAttention'
        AND (current_run.id IS NULL
          OR current_run.status<>'interrupted'
          OR COALESCE(current_run.exit_reason,'')<>'serviceRestart'))
      +
    (SELECT COUNT(*)
       FROM run_envelopes AS interrupted_run
       JOIN tasks AS task ON task.id=interrupted_run.task_id
      WHERE interrupted_run.status='interrupted'
        AND interrupted_run.exit_reason='serviceRestart'
        AND (task.status<>'needsAttention'
          OR task.current_run_id IS NULL
          OR task.current_run_id<>interrupted_run.id)),
    (SELECT COUNT(*) FROM run_envelopes WHERE status='queued'),
    (SELECT MIN(created_at) FROM run_envelopes WHERE status='queued')
";

impl Db {
    /// Read every runtime-health signal from one reader statement and one
    /// `SQLite` snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] when the query fails or the oldest queued timestamp
    /// is not a valid RFC 3339 value.
    pub async fn runtime_health_snapshot(&self) -> Result<RuntimeHealthSnapshot, DbError> {
        self.runtime_health_snapshot_at(now_millis()).await
    }

    async fn runtime_health_snapshot_at(
        &self,
        observed_at_ms: i64,
    ) -> Result<RuntimeHealthSnapshot, DbError> {
        self.with_reader(move |connection| {
            let raw = connection.query_row(RUNTIME_HEALTH_SQL, [], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            })?;
            let max_queue_wait_ms = match raw.7 {
                Some(created_at) => {
                    let created_at_ms = parse_rfc3339_millis(&created_at).ok_or_else(|| {
                        DbError::Invalid("RUNTIME_HEALTH_QUEUE_TIMESTAMP_INVALID".to_owned())
                    })?;
                    u64::try_from(observed_at_ms.saturating_sub(created_at_ms).max(0))
                        .unwrap_or(u64::MAX)
                }
                None => 0,
            };
            Ok(RuntimeHealthSnapshot {
                orphan_active_runs: nonnegative_count(raw.0, "orphan_active_runs")?,
                unconsumed_child_results: nonnegative_count(raw.1, "unconsumed_child_results")?,
                cleanup_unconfirmed: nonnegative_count(raw.2, "cleanup_unconfirmed")?,
                terminal_llm_usage_missing: nonnegative_count(raw.3, "terminal_llm_usage_missing")?,
                needs_attention_tasks: nonnegative_count(raw.4, "needs_attention_tasks")?,
                recovery_anomalies: nonnegative_count(raw.5, "recovery_anomalies")?,
                queued_runs: nonnegative_count(raw.6, "queued_runs")?,
                max_queue_wait_ms,
            })
        })
        .await
    }
}

fn nonnegative_count(value: i64, field: &str) -> Result<u64, DbError> {
    u64::try_from(value).map_err(|_| DbError::Invalid(format!("RUNTIME_HEALTH_NEGATIVE_{field}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: &str = "1970-01-01T00:16:40.000000Z";
    const T1: &str = "1970-01-01T00:16:45.000000Z";
    const NOW: &str = "1970-01-01T00:16:47.500000Z";

    #[tokio::test]
    async fn empty_runtime_has_zero_health_signals() {
        let database = Db::open_in_memory().expect("database");
        let snapshot = database
            .runtime_health_snapshot_at(parse_rfc3339_millis(NOW).expect("now"))
            .await
            .expect("snapshot");
        assert_eq!(snapshot, RuntimeHealthSnapshot::default());
    }

    #[tokio::test]
    async fn one_reader_snapshot_projects_runtime_anomalies_without_payloads() {
        let database = Db::open_in_memory().expect("database");
        seed_runtime_health_fixture(&database).await;
        let snapshot = database
            .runtime_health_snapshot_at(parse_rfc3339_millis(NOW).expect("now"))
            .await
            .expect("snapshot");

        assert_eq!(snapshot.orphan_active_runs, 1);
        assert_eq!(snapshot.unconsumed_child_results, 1);
        assert_eq!(snapshot.cleanup_unconfirmed, 4);
        assert_eq!(snapshot.terminal_llm_usage_missing, 1);
        assert_eq!(snapshot.needs_attention_tasks, 2);
        assert_eq!(snapshot.recovery_anomalies, 2);
        assert_eq!(snapshot.queued_runs, 2);
        assert_eq!(snapshot.max_queue_wait_ms, 7_500);

        insert_result_receipt(&database).await;
        let consumed = database
            .runtime_health_snapshot_at(parse_rfc3339_millis(NOW).expect("now"))
            .await
            .expect("snapshot after receipt");
        assert_eq!(consumed.unconsumed_child_results, 0);
    }

    async fn seed_runtime_health_fixture(database: &Db) {
        database
            .with_writer(|connection| {
                connection.execute_batch(&fixture_sql())?;
                Ok(())
            })
            .await
            .expect("seed runtime health fixture");
    }

    #[allow(clippy::too_many_lines)] // one declarative cross-table consistency fixture
    fn fixture_sql() -> String {
        format!(
            r"
            INSERT INTO sessions(id,model,working_dir,status,created_at,updated_at)
              VALUES('health-session','model','/tmp','active','{T0}','{T0}');

            INSERT INTO tasks(id,session_id,root_task_id,description,status,created_at,updated_at)
              VALUES('orphan-task','health-session','orphan-task','orphan','running','{T1}','{T1}');
            INSERT INTO run_envelopes(id,session_id,task_id,status,model,started_at,created_at,updated_at)
              VALUES('orphan-run','health-session','orphan-task','running','model','{T1}','{T1}','{T1}');

            INSERT INTO tasks(id,session_id,root_task_id,description,status,created_at,updated_at)
              VALUES('queue-a','health-session','queue-a','queue a','queued','{T0}','{T0}');
            INSERT INTO run_envelopes(id,session_id,task_id,status,model,started_at,created_at,updated_at)
              VALUES('queue-run-a','health-session','queue-a','queued','model','{T0}','{T0}','{T0}');
            UPDATE tasks SET current_run_id='queue-run-a' WHERE id='queue-a';

            INSERT INTO tasks(id,session_id,root_task_id,description,status,created_at,updated_at)
              VALUES('queue-b','health-session','queue-b','queue b','queued','{T1}','{T1}');
            INSERT INTO run_envelopes(id,session_id,task_id,status,model,started_at,created_at,updated_at)
              VALUES('queue-run-b','health-session','queue-b','queued','model','{T1}','{T1}','{T1}');
            UPDATE tasks SET current_run_id='queue-run-b' WHERE id='queue-b';

            INSERT INTO tasks(id,session_id,root_task_id,description,status,created_at,updated_at)
              VALUES('parent-task','health-session','parent-task','parent','waitingDependencies','{T1}','{T1}');
            INSERT INTO run_envelopes(id,session_id,task_id,status,model,started_at,created_at,updated_at)
              VALUES('parent-run','health-session','parent-task','waitingDependencies','model','{T1}','{T1}','{T1}');
            UPDATE tasks SET current_run_id='parent-run' WHERE id='parent-task';

            INSERT INTO tasks(id,session_id,parent_task_id,root_task_id,description,status,
                              created_at,updated_at)
              VALUES('child-task','health-session','parent-task','parent-task','child','queued',
                     '{T1}','{T1}');
            INSERT INTO run_envelopes(id,session_id,task_id,parent_run_id,status,model,started_at,
                                      finished_at,terminal_at,exit_reason,cleanup_status,created_at,updated_at)
              VALUES('child-run','health-session','child-task','parent-run','completed','model','{T1}',
                     '{T1}','{T1}','modelFinished','unconfirmed','{T1}','{T1}');
            UPDATE tasks SET current_run_id='child-run' WHERE id='child-task';
            INSERT INTO task_dependencies(parent_task_id,child_task_id,created_at,updated_at)
              VALUES('parent-task','child-task','{T1}','{T1}');
            INSERT INTO task_results(result_id,task_id,run_id,result_version,status,inline_text,
                                     byte_len,content_sha256,created_at)
              VALUES('child-result','child-task','child-run',1,'partial','x',1,
                     'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','{T1}');
            UPDATE tasks
               SET status='partial',cleanup_status='unconfirmed',terminal_at='{T1}',updated_at='{T1}'
             WHERE id='child-task';
            INSERT INTO tool_invocations(invocation_id,task_id,run_id,tool_use_id,tool_name,status,
                                         side_effect_class,cleanup_status,terminal_at,created_at,updated_at)
              VALUES('health-tool','child-task','child-run','tool-use','Bash','interrupted','unknown',
                     'unconfirmed','{T1}','{T1}','{T1}');
            INSERT INTO execution_resources(resource_id,task_id,run_id,invocation_id,resource_kind,
                                            status,created_at,updated_at)
              VALUES('health-resource','child-task','child-run','health-tool','process','unconfirmed',
                     '{T1}','{T1}');
            INSERT INTO llm_calls(call_id,task_id,run_id,provider,model,status,usage_complete,
                                  started_at,finished_at,created_at,updated_at)
              VALUES('health-call','child-task','child-run','provider','model','failed',0,
                     '{T1}','{T1}','{T1}','{T1}');

            INSERT INTO tasks(id,session_id,root_task_id,description,status,created_at,updated_at)
              VALUES('recovery-ok','health-session','recovery-ok','recovery ok','needsAttention','{T1}','{T1}');
            INSERT INTO run_envelopes(id,session_id,task_id,status,model,started_at,finished_at,
                                      terminal_at,exit_reason,created_at,updated_at)
              VALUES('recovery-run-ok','health-session','recovery-ok','interrupted','model','{T1}',
                     '{T1}','{T1}','serviceRestart','{T1}','{T1}');
            UPDATE tasks SET current_run_id='recovery-run-ok' WHERE id='recovery-ok';

            INSERT INTO tasks(id,session_id,root_task_id,description,status,created_at,updated_at)
              VALUES('recovery-bad','health-session','recovery-bad','recovery bad','needsAttention','{T1}','{T1}');
            INSERT INTO run_envelopes(id,session_id,task_id,status,model,started_at,finished_at,
                                      terminal_at,exit_reason,created_at,updated_at)
              VALUES('recovery-run-bad','health-session','recovery-bad','failed','model','{T1}',
                     '{T1}','{T1}','toolError','{T1}','{T1}');
            UPDATE tasks SET current_run_id='recovery-run-bad' WHERE id='recovery-bad';

            INSERT INTO tasks(id,session_id,root_task_id,description,status,created_at,updated_at)
              VALUES('stale-owner','health-session','stale-owner','stale owner','running','{T1}','{T1}');
            INSERT INTO run_envelopes(id,session_id,task_id,status,model,started_at,finished_at,
                                      terminal_at,exit_reason,created_at,updated_at)
              VALUES('stale-run','health-session','stale-owner','interrupted','model','{T1}',
                     '{T1}','{T1}','serviceRestart','{T1}','{T1}');
            UPDATE tasks SET current_run_id='stale-run' WHERE id='stale-owner';
            "
        )
    }

    async fn insert_result_receipt(database: &Db) {
        database
            .with_writer(|connection| {
                connection.execute_batch(&format!(
                    r"
                    INSERT INTO messages(id,session_id,role,content_json,task_id,run_id,origin,
                                         source_task_id,created_at,seq_num)
                      VALUES('receipt-message','health-session','user','[]','parent-task','parent-run',
                             'task_result','child-task','{T1}',1);
                    INSERT INTO task_result_receipts(receipt_id,consumer_task_id,producer_task_id,
                                                     result_version,message_id,result_sha256,created_at)
                      VALUES('receipt','parent-task','child-task',1,'receipt-message',
                             'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','{T1}');
                    UPDATE task_dependencies SET consumed_result_version=1,updated_at='{T1}'
                     WHERE parent_task_id='parent-task' AND child_task_id='child-task';
                    "
                ))?;
                Ok(())
            })
            .await
            .expect("insert result receipt");
    }
}
