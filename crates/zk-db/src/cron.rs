//! SQLite-authoritative Cron job and occurrence repository.
//!
//! A scheduling claim is deliberately wider than a queue insert: advancing the
//! job cursor, recording the occurrence, and (when executable) creating the root
//! Session/Task/Run happen in one writer transaction. A process crash can
//! therefore leave either the old due job or one queryable Task, never an
//! untraceable in-between state.
#![allow(missing_docs, clippy::missing_errors_doc, clippy::too_many_lines)]

use rusqlite::{OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};

use crate::error::DbError;
use crate::task_runtime::{RUNTIME_TASK_COLUMNS, map_runtime_task};
use crate::time::{format_rfc3339_micros, now_millis};
use crate::{Db, RuntimeTaskRecord};

pub const MAX_CRON_JOBS: i64 = 50;

#[derive(Clone, Debug)]
pub struct NewCronJob {
    pub job_id: String,
    pub owner_session_id: String,
    pub cron_expression: String,
    pub timezone: String,
    pub prompt: String,
    pub recurring: bool,
    pub overlap_policy: String,
    pub missed_policy: String,
    pub next_scheduled_at_ms: i64,
    pub now_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronJobRecord {
    pub job_id: String,
    pub owner_session_id: String,
    pub cron_expression: String,
    pub timezone: String,
    pub prompt: String,
    pub recurring: bool,
    pub overlap_policy: String,
    pub missed_policy: String,
    pub status: String,
    pub model: String,
    pub working_dir: String,
    pub next_scheduled_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub version: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronOccurrenceRecord {
    pub occurrence_id: String,
    pub job_id: String,
    pub scheduled_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub task_id: Option<String>,
    pub status: String,
    pub reason: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug)]
pub struct ClaimCronOccurrence {
    pub job_id: String,
    pub expected_job_version: i64,
    pub scheduled_at_ms: i64,
    pub next_scheduled_at_ms: Option<i64>,
    pub startup_cutoff_ms: i64,
    pub now_ms: i64,
    pub occurrence_id: String,
    pub session_id: String,
    pub task_id: String,
    pub run_id: String,
    pub startup_epoch: i64,
}

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)] // preserves the public durable claim contract
pub enum CronClaimOutcome {
    Stale,
    Skipped(CronOccurrenceRecord),
    Submitted {
        occurrence: CronOccurrenceRecord,
        task: RuntimeTaskRecord,
        run_id: String,
        session_id: String,
    },
}

const JOB_COLUMNS: &str = "job_id,owner_session_id,cron_expression,timezone,prompt,recurring,\
 overlap_policy,missed_policy,status,model,working_dir,next_scheduled_at_ms,created_at_ms,\
 updated_at_ms,version";

const OCCURRENCE_COLUMNS: &str = "occurrence_id,job_id,scheduled_at_ms,started_at_ms,\
 finished_at_ms,task_id,status,reason,created_at_ms,updated_at_ms";

fn map_job(row: &Row<'_>) -> rusqlite::Result<CronJobRecord> {
    Ok(CronJobRecord {
        job_id: row.get(0)?,
        owner_session_id: row.get(1)?,
        cron_expression: row.get(2)?,
        timezone: row.get(3)?,
        prompt: row.get(4)?,
        recurring: row.get::<_, i64>(5)? != 0,
        overlap_policy: row.get(6)?,
        missed_policy: row.get(7)?,
        status: row.get(8)?,
        model: row.get(9)?,
        working_dir: row.get(10)?,
        next_scheduled_at_ms: row.get(11)?,
        created_at_ms: row.get(12)?,
        updated_at_ms: row.get(13)?,
        version: row.get(14)?,
    })
}

fn map_occurrence(row: &Row<'_>) -> rusqlite::Result<CronOccurrenceRecord> {
    Ok(CronOccurrenceRecord {
        occurrence_id: row.get(0)?,
        job_id: row.get(1)?,
        scheduled_at_ms: row.get(2)?,
        started_at_ms: row.get(3)?,
        finished_at_ms: row.get(4)?,
        task_id: row.get(5)?,
        status: row.get(6)?,
        reason: row.get(7)?,
        created_at_ms: row.get(8)?,
        updated_at_ms: row.get(9)?,
    })
}

fn require_uuid_v4(value: &str, field: &str) -> Result<(), DbError> {
    let id = uuid::Uuid::parse_str(value)
        .map_err(|_| DbError::Invalid(format!("{field}_MUST_BE_UUID_V4")))?;
    if id.get_version() != Some(uuid::Version::Random) || id.hyphenated().to_string() != value {
        return Err(DbError::Invalid(format!("{field}_MUST_BE_UUID_V4")));
    }
    Ok(())
}

fn validate_policy(value: &str, field: &str) -> Result<(), DbError> {
    if value == "skip" {
        Ok(())
    } else {
        Err(DbError::Invalid(format!("{field}_UNSUPPORTED")))
    }
}

impl Db {
    pub async fn create_cron_job(&self, request: &NewCronJob) -> Result<CronJobRecord, DbError> {
        require_uuid_v4(&request.job_id, "CRON_JOB_ID")?;
        if request.cron_expression.trim().is_empty()
            || request.timezone.trim().is_empty()
            || request.prompt.trim().is_empty()
            || request.next_scheduled_at_ms <= request.now_ms
            || request.now_ms <= 0
        {
            return Err(DbError::Invalid("CRON_JOB_INVALID".to_owned()));
        }
        validate_policy(&request.overlap_policy, "CRON_OVERLAP_POLICY")?;
        validate_policy(&request.missed_policy, "CRON_MISSED_POLICY")?;
        let request = request.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let session: Option<(String, String)> = tx
                .query_row(
                    "SELECT model,working_dir FROM sessions
                     WHERE id=?1 AND kind='root' AND status='active'",
                    [&request.owner_session_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((model, working_dir)) = session else {
                return Err(DbError::Invalid("CRON_OWNER_SESSION_NOT_FOUND".to_owned()));
            };
            let count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM cron_jobs
                 WHERE owner_session_id=?1 AND status!='deleted'",
                [&request.owner_session_id],
                |row| row.get(0),
            )?;
            if count >= MAX_CRON_JOBS {
                return Err(DbError::Invalid("CRON_JOB_LIMIT_REACHED".to_owned()));
            }
            tx.execute(
                "INSERT INTO cron_jobs
                    (job_id,owner_session_id,cron_expression,timezone,prompt,recurring,
                     overlap_policy,missed_policy,status,model,working_dir,next_scheduled_at_ms,
                     created_at_ms,updated_at_ms,version)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'active',?9,?10,?11,?12,?12,0)",
                params![
                    request.job_id,
                    request.owner_session_id,
                    request.cron_expression,
                    request.timezone,
                    request.prompt,
                    i64::from(request.recurring),
                    request.overlap_policy,
                    request.missed_policy,
                    model,
                    working_dir,
                    request.next_scheduled_at_ms,
                    request.now_ms,
                ],
            )?;
            let sql = format!("SELECT {JOB_COLUMNS} FROM cron_jobs WHERE job_id=?1");
            let job = tx.query_row(&sql, [&request.job_id], map_job)?;
            tx.commit()?;
            Ok(job)
        })
        .await
    }

    pub async fn list_cron_jobs(
        &self,
        owner_session_id: &str,
    ) -> Result<Vec<CronJobRecord>, DbError> {
        let owner = owner_session_id.to_owned();
        self.with_reader(move |conn| {
            let sql = format!(
                "SELECT {JOB_COLUMNS} FROM cron_jobs
                 WHERE owner_session_id=?1 AND status!='deleted'
                 ORDER BY created_at_ms,job_id"
            );
            let mut statement = conn.prepare(&sql)?;
            let rows = statement.query_map([owner], map_job)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await
    }

    pub async fn find_cron_job(
        &self,
        owner_session_id: &str,
        job_id: &str,
    ) -> Result<Option<CronJobRecord>, DbError> {
        let owner = owner_session_id.to_owned();
        let job_id = job_id.to_owned();
        self.with_reader(move |conn| {
            let sql = format!(
                "SELECT {JOB_COLUMNS} FROM cron_jobs
                 WHERE owner_session_id=?1 AND job_id=?2 AND status!='deleted'"
            );
            conn.query_row(&sql, params![owner, job_id], map_job)
                .optional()
                .map_err(Into::into)
        })
        .await
    }

    pub async fn delete_cron_job(
        &self,
        owner_session_id: &str,
        job_id: &str,
        now_ms: i64,
    ) -> Result<Option<CronJobRecord>, DbError> {
        let owner = owner_session_id.to_owned();
        let job_id = job_id.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let sql = format!(
                "SELECT {JOB_COLUMNS} FROM cron_jobs
                 WHERE owner_session_id=?1 AND job_id=?2 AND status!='deleted'"
            );
            let existing = tx
                .query_row(&sql, params![owner, job_id], map_job)
                .optional()?;
            let Some(job) = existing else {
                tx.commit()?;
                return Ok(None);
            };
            tx.execute(
                "UPDATE cron_jobs SET status='deleted',next_scheduled_at_ms=NULL,
                    updated_at_ms=?1,version=version+1 WHERE job_id=?2",
                params![now_ms, job.job_id],
            )?;
            tx.commit()?;
            Ok(Some(job))
        })
        .await
    }

    pub async fn count_cron_jobs(&self, owner_session_id: &str) -> Result<i64, DbError> {
        let owner = owner_session_id.to_owned();
        self.with_reader(move |conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM cron_jobs
                 WHERE owner_session_id=?1 AND status!='deleted'",
                [owner],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .await
    }

    pub async fn find_due_cron_jobs(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> Result<Vec<CronJobRecord>, DbError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.with_reader(move |conn| {
            let sql = format!(
                "SELECT {JOB_COLUMNS} FROM cron_jobs
                 WHERE status='active' AND next_scheduled_at_ms<=?1
                 ORDER BY next_scheduled_at_ms,job_id LIMIT ?2"
            );
            let mut statement = conn.prepare(&sql)?;
            let rows = statement.query_map(params![now_ms, limit], map_job)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await
    }

    /// Claim one due occurrence and create its root execution atomically.
    pub async fn claim_cron_occurrence(
        &self,
        request: &ClaimCronOccurrence,
    ) -> Result<CronClaimOutcome, DbError> {
        for (value, field) in [
            (&request.occurrence_id, "CRON_OCCURRENCE_ID"),
            (&request.session_id, "CRON_SESSION_ID"),
            (&request.task_id, "CRON_TASK_ID"),
            (&request.run_id, "CRON_RUN_ID"),
        ] {
            require_uuid_v4(value, field)?;
        }
        if request.now_ms <= 0
            || request.scheduled_at_ms <= 0
            || request.startup_epoch < 0
            || request
                .next_scheduled_at_ms
                .is_some_and(|next| next <= request.scheduled_at_ms || next <= request.now_ms)
        {
            return Err(DbError::Invalid("CRON_OCCURRENCE_INVALID".to_owned()));
        }
        let request = request.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let sql = format!("SELECT {JOB_COLUMNS} FROM cron_jobs WHERE job_id=?1");
            let job = tx
                .query_row(&sql, [&request.job_id], map_job)
                .optional()?;
            let Some(job) = job else {
                tx.commit()?;
                return Ok(CronClaimOutcome::Stale);
            };
            if job.status != "active"
                || job.version != request.expected_job_version
                || job.next_scheduled_at_ms != Some(request.scheduled_at_ms)
            {
                tx.commit()?;
                return Ok(CronClaimOutcome::Stale);
            }

            let missed = request.scheduled_at_ms < request.startup_cutoff_ms;
            let overlap: i64 = if missed {
                0
            } else {
                tx.query_row(
                    "SELECT COUNT(*) FROM cron_occurrences o
                     JOIN tasks t ON t.id=o.task_id
                     WHERE o.job_id=?1 AND o.status IN ('submitted','running')
                       AND t.status IN ('queued','running','waitingDependencies',
                                        'waitingInteraction','cancelling')",
                    [&request.job_id],
                    |row| row.get(0),
                )?
            };
            let skip_reason = if missed {
                Some("missedPolicySkip")
            } else if overlap > 0 {
                Some("overlapPolicySkip")
            } else {
                None
            };

            let (new_status, next_at) = if job.recurring {
                let Some(next) = request.next_scheduled_at_ms else {
                    return Err(DbError::Invalid("CRON_NEXT_SCHEDULE_REQUIRED".to_owned()));
                };
                ("active", Some(next))
            } else {
                ("paused", None)
            };
            let updated = tx.execute(
                "UPDATE cron_jobs SET status=?1,next_scheduled_at_ms=?2,updated_at_ms=?3,
                    version=version+1
                 WHERE job_id=?4 AND version=?5 AND status='active'
                   AND next_scheduled_at_ms=?6",
                params![
                    new_status,
                    next_at,
                    request.now_ms,
                    request.job_id,
                    request.expected_job_version,
                    request.scheduled_at_ms,
                ],
            )?;
            if updated != 1 {
                tx.rollback()?;
                return Ok(CronClaimOutcome::Stale);
            }

            if let Some(reason) = skip_reason {
                tx.execute(
                    "INSERT INTO cron_occurrences
                        (occurrence_id,job_id,scheduled_at_ms,finished_at_ms,status,reason,
                         created_at_ms,updated_at_ms)
                     VALUES(?1,?2,?3,?4,'skipped',?5,?4,?4)",
                    params![
                        request.occurrence_id,
                        request.job_id,
                        request.scheduled_at_ms,
                        request.now_ms,
                        reason,
                    ],
                )?;
                let sql = format!(
                    "SELECT {OCCURRENCE_COLUMNS} FROM cron_occurrences WHERE occurrence_id=?1"
                );
                let occurrence = tx.query_row(&sql, [&request.occurrence_id], map_occurrence)?;
                tx.commit()?;
                return Ok(CronClaimOutcome::Skipped(occurrence));
            }

            let now_text = format_rfc3339_micros(request.now_ms);
            tx.execute(
                "INSERT INTO sessions
                    (id,kind,title,model,working_dir,status,created_at,updated_at)
                 VALUES(?1,'root',?2,?3,?4,'active',?5,?5)",
                params![
                    request.session_id,
                    format!("Scheduled: {}", job.cron_expression),
                    job.model,
                    job.working_dir,
                    now_text,
                ],
            )?;
            let execution_config = serde_json::json!({
                "source": "cron",
                "jobId": job.job_id,
                "occurrenceId": request.occurrence_id,
                "scheduledAtMs": request.scheduled_at_ms,
                "timezone": job.timezone,
                "isolation": "readOnly",
                "lifecycle": "attached"
            })
            .to_string();
            tx.execute(
                "INSERT INTO tasks
                    (id,session_id,parent_task_id,root_task_id,current_run_id,ordinal,
                     description,prompt,task_type,status,execution_config_json,lifecycle_policy,
                     cleanup_status,verification_status,created_at,updated_at)
                 VALUES(?1,?2,NULL,?1,?3,0,?4,?5,'cron','queued',?6,'attached',
                        'notRequired','notRequested',?7,?7)",
                params![
                    request.task_id,
                    request.session_id,
                    request.run_id,
                    format!("Scheduled job {}", job.job_id),
                    job.prompt,
                    execution_config,
                    now_text,
                ],
            )?;
            tx.execute(
                "INSERT INTO run_envelopes
                    (id,session_id,task_id,attempt,startup_epoch,status,version,agent_type,model,
                     started_at,verification_status,cleanup_status,created_at,updated_at)
                 VALUES(?1,?2,?3,1,?4,'queued',0,'cron',?5,?6,
                        'notRequested','notRequired',?6,?6)",
                params![
                    request.run_id,
                    request.session_id,
                    request.task_id,
                    request.startup_epoch,
                    job.model,
                    now_text,
                ],
            )?;
            tx.execute(
                "INSERT INTO run_event_log(run_id,seq,event_type,event_data,ts)
                 VALUES(?1,0,'task_created',?2,?3)",
                params![
                    request.run_id,
                    serde_json::json!({
                        "protocolVersion": 4,
                        "taskId": request.task_id,
                        "runId": request.run_id,
                        "parentTaskId": null,
                        "source": "cron",
                        "jobId": request.job_id,
                        "scheduledAtMs": request.scheduled_at_ms,
                    })
                    .to_string(),
                    now_millis(),
                ],
            )?;
            tx.execute(
                "INSERT INTO cron_occurrences
                    (occurrence_id,job_id,scheduled_at_ms,task_id,status,created_at_ms,updated_at_ms)
                 VALUES(?1,?2,?3,?4,'submitted',?5,?5)",
                params![
                    request.occurrence_id,
                    request.job_id,
                    request.scheduled_at_ms,
                    request.task_id,
                    request.now_ms,
                ],
            )?;
            let task_sql = format!("SELECT {RUNTIME_TASK_COLUMNS} FROM tasks WHERE id=?1");
            let task = tx.query_row(&task_sql, [&request.task_id], map_runtime_task)?;
            let occurrence_sql = format!(
                "SELECT {OCCURRENCE_COLUMNS} FROM cron_occurrences WHERE occurrence_id=?1"
            );
            let occurrence =
                tx.query_row(&occurrence_sql, [&request.occurrence_id], map_occurrence)?;
            tx.commit()?;
            Ok(CronClaimOutcome::Submitted {
                occurrence,
                task,
                run_id: request.run_id,
                session_id: request.session_id,
            })
        })
        .await
    }

    pub async fn mark_cron_occurrence_started(
        &self,
        occurrence_id: &str,
        now_ms: i64,
    ) -> Result<bool, DbError> {
        let id = occurrence_id.to_owned();
        self.with_writer(move |conn| {
            Ok(conn.execute(
                "UPDATE cron_occurrences SET status='running',started_at_ms=COALESCE(started_at_ms,?1),
                    updated_at_ms=?1 WHERE occurrence_id=?2 AND status='submitted'",
                params![now_ms, id],
            )? == 1)
        })
        .await
    }

    /// Project Task terminal states back to occurrence history. This is a
    /// projection only; `TaskRuntime` remains the execution authority.
    pub async fn reconcile_cron_occurrences(&self, now_ms: i64) -> Result<usize, DbError> {
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let pairs = {
                let mut statement = tx.prepare(
                    "SELECT o.occurrence_id,o.status,t.status,t.reason
                     FROM cron_occurrences o JOIN tasks t ON t.id=o.task_id
                     WHERE o.status IN ('submitted','running')",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })?;
                rows.collect::<Result<Vec<_>, _>>()?
            };
            let mut changed = 0;
            for (occurrence_id, occurrence_status, task_status, task_reason) in pairs {
                let terminal = match task_status.as_str() {
                    "succeeded" => Some(("succeeded", None)),
                    "partial" => Some(("partial", task_reason.as_deref())),
                    "failed" => Some(("failed", task_reason.as_deref())),
                    "cancelled" => Some(("cancelled", task_reason.as_deref())),
                    "needsAttention" => Some(("failed", Some("taskNeedsAttention"))),
                    _ => None,
                };
                if let Some((status, reason)) = terminal {
                    changed += tx.execute(
                        "UPDATE cron_occurrences SET status=?1,reason=?2,finished_at_ms=?3,
                            updated_at_ms=?3 WHERE occurrence_id=?4
                            AND status IN ('submitted','running')",
                        params![status, reason, now_ms, occurrence_id],
                    )?;
                } else if task_status != "queued" && occurrence_status == "submitted" {
                    changed += tx.execute(
                        "UPDATE cron_occurrences SET status='running',
                            started_at_ms=COALESCE(started_at_ms,?1),updated_at_ms=?1
                         WHERE occurrence_id=?2 AND status='submitted'",
                        params![now_ms, occurrence_id],
                    )?;
                }
            }
            tx.commit()?;
            Ok(changed)
        })
        .await
    }

    pub async fn list_cron_occurrences(
        &self,
        job_id: &str,
    ) -> Result<Vec<CronOccurrenceRecord>, DbError> {
        let job_id = job_id.to_owned();
        self.with_reader(move |conn| {
            let sql = format!(
                "SELECT {OCCURRENCE_COLUMNS} FROM cron_occurrences
                 WHERE job_id=?1 ORDER BY scheduled_at_ms,occurrence_id"
            );
            let mut statement = conn.prepare(&sql)?;
            let rows = statement.query_map([job_id], map_occurrence)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE_MS: i64 = 1_800_000_000_000;

    async fn create_job(db: &Db, recurring: bool) -> (String, CronJobRecord) {
        let session = db
            .create_session("test-model", "/tmp/zkcode-cron-test")
            .await
            .expect("create root session");
        let job = db
            .create_cron_job(&NewCronJob {
                job_id: uuid::Uuid::new_v4().to_string(),
                owner_session_id: session.id.clone(),
                cron_expression: "* * * * *".to_owned(),
                timezone: "UTC".to_owned(),
                prompt: "inspect repository".to_owned(),
                recurring,
                overlap_policy: "skip".to_owned(),
                missed_policy: "skip".to_owned(),
                next_scheduled_at_ms: BASE_MS + 60_000,
                now_ms: BASE_MS,
            })
            .await
            .expect("create cron job");
        (session.id, job)
    }

    fn claim(job: &CronJobRecord, now_ms: i64, startup_cutoff_ms: i64) -> ClaimCronOccurrence {
        let scheduled_at_ms = job.next_scheduled_at_ms.expect("active job has next time");
        ClaimCronOccurrence {
            job_id: job.job_id.clone(),
            expected_job_version: job.version,
            scheduled_at_ms,
            next_scheduled_at_ms: job.recurring.then_some(scheduled_at_ms + 60_000),
            startup_cutoff_ms,
            now_ms,
            occurrence_id: uuid::Uuid::new_v4().to_string(),
            session_id: uuid::Uuid::new_v4().to_string(),
            task_id: uuid::Uuid::new_v4().to_string(),
            run_id: uuid::Uuid::new_v4().to_string(),
            startup_epoch: BASE_MS,
        }
    }

    #[tokio::test]
    async fn concurrent_claim_and_restart_scan_create_exactly_one_task() {
        let db = Db::open_in_memory().expect("open database");
        let (_owner, job) = create_job(&db, true).await;
        let due = job.next_scheduled_at_ms.expect("due");
        let first = claim(&job, due, BASE_MS);
        let second = claim(&job, due, BASE_MS);
        let (first, second) = tokio::join!(
            db.claim_cron_occurrence(&first),
            db.claim_cron_occurrence(&second)
        );
        let outcomes = [first.expect("first claim"), second.expect("second claim")];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, CronClaimOutcome::Submitted { .. }))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, CronClaimOutcome::Stale))
                .count(),
            1
        );

        let occurrence = db
            .list_cron_occurrences(&job.job_id)
            .await
            .expect("list occurrences");
        assert_eq!(occurrence.len(), 1);
        let task_id = occurrence[0].task_id.as_deref().expect("submitted task");
        let task = db
            .find_runtime_task_by_id(task_id)
            .await
            .expect("query task")
            .expect("task exists at claim return");
        assert_eq!(task.task_type, "cron");
        assert_eq!(task.status.as_db(), "queued");

        // A scanner starting again with the pre-crash cursor cannot recreate
        // either the occurrence or its root Task.
        let restart = claim(&job, due, BASE_MS);
        assert!(matches!(
            db.claim_cron_occurrence(&restart)
                .await
                .expect("restart claim"),
            CronClaimOutcome::Stale
        ));
        assert_eq!(
            db.list_cron_occurrences(&job.job_id)
                .await
                .expect("occurrences after restart")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn active_occurrence_causes_no_overlap_skip_without_a_second_task() {
        let db = Db::open_in_memory().expect("open database");
        let (owner, job) = create_job(&db, true).await;
        let due = job.next_scheduled_at_ms.expect("first due");
        assert!(matches!(
            db.claim_cron_occurrence(&claim(&job, due, BASE_MS))
                .await
                .expect("first claim"),
            CronClaimOutcome::Submitted { .. }
        ));
        let advanced = db
            .find_cron_job(&owner, &job.job_id)
            .await
            .expect("read advanced job")
            .expect("advanced job");
        let second_due = advanced.next_scheduled_at_ms.expect("second due");
        let outcome = db
            .claim_cron_occurrence(&claim(&advanced, second_due, BASE_MS))
            .await
            .expect("second claim");
        let CronClaimOutcome::Skipped(skipped) = outcome else {
            panic!("active occurrence must skip overlap");
        };
        assert_eq!(skipped.reason.as_deref(), Some("overlapPolicySkip"));
        assert!(skipped.task_id.is_none());
        let occurrences = db
            .list_cron_occurrences(&job.job_id)
            .await
            .expect("list occurrences");
        assert_eq!(occurrences.len(), 2);
        assert_eq!(
            occurrences
                .iter()
                .filter(|row| row.task_id.is_some())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn startup_missed_occurrence_is_recorded_and_never_backfilled() {
        let db = Db::open_in_memory().expect("open database");
        let (_owner, job) = create_job(&db, true).await;
        let due = job.next_scheduled_at_ms.expect("due");
        let outcome = db
            .claim_cron_occurrence(&claim(&job, due + 1_000, due + 1))
            .await
            .expect("missed claim");
        let CronClaimOutcome::Skipped(skipped) = outcome else {
            panic!("startup-missed occurrence must be skipped");
        };
        assert_eq!(skipped.status, "skipped");
        assert_eq!(skipped.reason.as_deref(), Some("missedPolicySkip"));
        assert!(skipped.task_id.is_none());
        assert_eq!(skipped.finished_at_ms, Some(due + 1_000));
    }

    #[tokio::test]
    async fn one_shot_job_pauses_when_its_occurrence_is_claimed() {
        let db = Db::open_in_memory().expect("open database");
        let (owner, job) = create_job(&db, false).await;
        let due = job.next_scheduled_at_ms.expect("due");
        assert!(matches!(
            db.claim_cron_occurrence(&claim(&job, due, BASE_MS))
                .await
                .expect("claim"),
            CronClaimOutcome::Submitted { .. }
        ));
        let paused = db
            .find_cron_job(&owner, &job.job_id)
            .await
            .expect("read job")
            .expect("job remains queryable");
        assert_eq!(paused.status, "paused");
        assert_eq!(paused.next_scheduled_at_ms, None);
    }
}
