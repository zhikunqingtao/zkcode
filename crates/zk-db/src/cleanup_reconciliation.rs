//! Reconcile orthogonal cleanup metadata using retained physical facts only.
use crate::{CleanupStatus, Db, DbError};
use rusqlite::{OptionalExtension, params};

type ResourceOwnerRow = (String, String, Option<String>, Option<String>, i64, String);

impl Db {
    /// Confirm cleanup of a terminal Run after its actual resource owners finish.
    /// Never changes task outcome, immutable result, resource states or tool results.
    ///
    /// # Errors
    /// Unknown/nonterminal Runs and storage failures are reported explicitly.
    pub async fn retry_confirmed_run_cleanup(
        &self,
        run_id: &str,
    ) -> Result<CleanupStatus, DbError> {
        let run = run_id.to_owned();
        self.with_writer(move |conn| {
            let tx=conn.transaction()?;
            let (task,status,old):(String,String,String)=tx.query_row(
                "SELECT task_id,status,cleanup_status FROM run_envelopes WHERE id=?1",[&run],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?
                ;
            if !matches!(status.as_str(),"completed"|"failed"|"cancelled"|"interrupted") {
                return Err(DbError::Conflict("CLEANUP_RUN_NOT_TERMINAL".into()));
            }
            let (unconfirmed,pending,proof):(bool,bool,bool)=tx.query_row(
                "SELECT
                 EXISTS(SELECT 1 FROM execution_resources WHERE run_id=?1 AND status='unconfirmed')
                 OR EXISTS(SELECT 1 FROM tool_invocations WHERE run_id=?1 AND cleanup_status='unconfirmed')
                 OR EXISTS(SELECT 1 FROM tasks WHERE creator_run_id=?1 AND lifecycle_policy='attached' AND cleanup_status='unconfirmed'),
                 EXISTS(SELECT 1 FROM execution_resources WHERE run_id=?1 AND status<>'released')
                 OR EXISTS(SELECT 1 FROM tool_invocations WHERE run_id=?1 AND (status IN ('preparing','queued','running') OR cleanup_status='pending'))
                 OR EXISTS(SELECT 1 FROM tasks WHERE creator_run_id=?1 AND lifecycle_policy='attached' AND (status NOT IN ('succeeded','partial','failed','cancelled') OR cleanup_status='pending')),
                 EXISTS(SELECT 1 FROM execution_resources WHERE run_id=?1 AND status='released')
                 OR EXISTS(SELECT 1 FROM tool_invocations WHERE run_id=?1 AND cleanup_status='confirmed')",
                [&run],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
            if unconfirmed {return Ok(CleanupStatus::Unconfirmed);}
            if pending {return Ok(CleanupStatus::Pending);}
            if !proof {
                return Ok(if old=="notRequired" {CleanupStatus::NotRequired} else {CleanupStatus::Unconfirmed});
            }
            if old!="confirmed" {
                let now=crate::time::format_rfc3339_micros(crate::time::now_millis());
                tx.execute("UPDATE run_envelopes SET cleanup_status='confirmed',updated_at=?2,version=version+1 WHERE id=?1",params![run,now])?;
                tx.execute("UPDATE tasks SET cleanup_status='confirmed',updated_at=?3,version=version+1 WHERE id=?1 AND current_run_id=?2 AND status IN ('succeeded','partial','failed','cancelled','needsAttention')",params![task,run,now])?;
                crate::run::append_event_in_current_write(&tx,&run,"run_cleanup_confirmed",None,&serde_json::json!({"taskId":task,"cleanupStatus":"confirmed"}))?;
            }
            tx.commit()?;
            Ok(CleanupStatus::Confirmed)
        }).await
    }

    /// Verify positive release evidence for exactly one invocation after its executor
    /// has finished. An empty ledger does not prove that an interrupted tool had no
    /// effect; pending, unconfirmed, foreign or missing owners return false.
    ///
    /// # Errors
    /// Storage failures propagate without inventing cleanup confirmation.
    pub async fn invocation_resources_released(
        &self,
        invocation_id: &str,
        run_id: &str,
    ) -> Result<bool, DbError> {
        let (invocation, run) = (invocation_id.to_owned(), run_id.to_owned());
        self.with_reader(move |conn| {
            Ok(conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM tool_invocations WHERE invocation_id=?1 AND run_id=?2)
                AND EXISTS(SELECT 1 FROM execution_resources WHERE invocation_id=?1 AND run_id=?2)
                AND NOT EXISTS(SELECT 1 FROM execution_resources WHERE invocation_id=?1 AND (run_id<>?2 OR status<>'released'))",
                params![invocation, run], |row| row.get(0),
            )?)
        }).await
    }

    /// Read only durable identifiers, including after an ephemeral scope expires.
    ///
    /// # Errors
    /// Database errors propagate; a missing Run returns None.
    pub async fn cleanup_run_task_id(&self, run_id: &str) -> Result<Option<String>, DbError> {
        let run = run_id.to_owned();
        self.with_reader(move |conn| {
            Ok(conn
                .query_row(
                    "SELECT task_id FROM run_envelopes WHERE id=?1",
                    [run],
                    |row| row.get(0),
                )
                .optional()?)
        })
        .await
    }
}

/// Exact retained ownership and version used to acknowledge a newly confirmed close.
/// This is metadata only and remains readable after a temporary content lease expires.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionResourceReleaseProof {
    /// Physical resource identity allocated before spawn.
    pub resource_id: String,
    /// Original logical owner.
    pub task_id: String,
    /// Original physical execution attempt.
    pub run_id: String,
    /// Original invocation owner, if any.
    pub invocation_id: Option<String>,
    /// Exact bound transport/process identity; a recycled unrelated PID is not proof.
    pub external_id: String,
    /// Optimistic version read before reconciling retained physical ownership.
    pub version: i64,
}

impl Db {
    /// Read the exact identity needed by a retained physical owner for cleanup retry.
    ///
    /// # Errors
    /// Storage errors propagate; an unbound or unknown resource has no proof.
    pub async fn execution_resource_release_proof(
        &self,
        resource_id: &str,
    ) -> Result<Option<ExecutionResourceReleaseProof>, DbError> {
        let id = resource_id.to_owned();
        self.with_reader(move |conn| {
            Ok(conn
                .query_row(
                    "SELECT resource_id,task_id,run_id,invocation_id,external_id,version
             FROM execution_resources WHERE resource_id=?1 AND external_id IS NOT NULL",
                    [&id],
                    |row| {
                        Ok(ExecutionResourceReleaseProof {
                            resource_id: row.get(0)?,
                            task_id: row.get(1)?,
                            run_id: row.get(2)?,
                            invocation_id: row.get(3)?,
                            external_id: row.get(4)?,
                            version: row.get(5)?,
                        })
                    },
                )
                .optional()?)
        })
        .await
    }

    /// Reconcile only a retained owner's positively confirmed physical close.
    /// Ordinary finalization intentionally cannot promote an unconfirmed resource.
    /// All ownership, external identity and version fields must still match.
    ///
    /// # Errors
    /// Storage failures propagate; mismatched ownership is an explicit conflict.
    pub async fn reconcile_execution_resource_release(
        &self,
        proof: &ExecutionResourceReleaseProof,
    ) -> Result<crate::CasOutcome, DbError> {
        let proof = proof.clone();
        self.with_writer(move |conn| {
            let tx=conn.transaction()?;
            let current:Option<ResourceOwnerRow>=tx.query_row(
                "SELECT task_id,run_id,invocation_id,external_id,version,status FROM execution_resources WHERE resource_id=?1",
                [&proof.resource_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
            let Some((task,run,invocation,external,version,status))=current else {return Ok(crate::CasOutcome::NotFound);};
            if task!=proof.task_id || run!=proof.run_id || invocation!=proof.invocation_id || external.as_deref()!=Some(proof.external_id.as_str()) {
                return Err(DbError::Conflict("RESOURCE_RECONCILIATION_OWNER_MISMATCH".into()));
            }
            if version!=proof.version {return Ok(crate::CasOutcome::VersionConflict);}
            if status=="released" {return Ok(crate::CasOutcome::Applied);}
            if status!="unconfirmed" {return Ok(crate::CasOutcome::InvalidTransition);}
            let now=crate::time::format_rfc3339_micros(crate::time::now_millis());
            let changed=tx.execute("UPDATE execution_resources SET status='released',released_at=?1,updated_at=?1,version=version+1 WHERE resource_id=?2 AND version=?3 AND status='unconfirmed'",params![now,proof.resource_id,proof.version])?;
            if changed!=1 {return Ok(crate::CasOutcome::VersionConflict);}
            if let Some(invocation)=proof.invocation_id {
                tx.execute("UPDATE tool_invocations SET cleanup_status=CASE
                    WHEN EXISTS(SELECT 1 FROM execution_resources WHERE invocation_id=?1 AND status='unconfirmed') THEN 'unconfirmed'
                    WHEN EXISTS(SELECT 1 FROM execution_resources WHERE invocation_id=?1 AND status IN ('allocated','stopping')) THEN 'pending'
                    ELSE 'confirmed' END,updated_at=?2 WHERE invocation_id=?1",params![invocation,now])?;
            }
            tx.commit()?;
            Ok(crate::CasOutcome::Applied)
        }).await
    }

    /// Confirm a retained scope after cancellation already sealed its invocation.
    /// The lifecycle status, error and body are immutable; only cleanup is refreshed.
    /// The caller must first close every retained scope and verify physical resources.
    ///
    /// # Errors
    /// A wrong owner/kind/nonterminal invocation or unreleased resource is rejected.
    pub async fn reconcile_run_scope_cleanup(
        &self,
        invocation_id: &str,
        run_id: &str,
    ) -> Result<crate::CasOutcome, DbError> {
        let (invocation, run) = (invocation_id.to_owned(), run_id.to_owned());
        self.with_writer(move |conn| {
            let tx=conn.transaction()?;
            let row:Option<(String,i64)>=tx.query_row("SELECT status,version FROM tool_invocations WHERE invocation_id=?1 AND run_id=?2 AND invocation_kind='runtimeScope'",params![invocation,run],|row|Ok((row.get(0)?,row.get(1)?))).optional()?;
            let Some((status,version))=row else {return Ok(crate::CasOutcome::NotFound);};
            if !matches!(status.as_str(),"succeeded"|"failed"|"cancelled"|"interrupted") {return Ok(crate::CasOutcome::InvalidTransition);}
            if tx.query_row("SELECT EXISTS(SELECT 1 FROM execution_resources WHERE invocation_id=?1 AND status<>'released')",[&invocation],|row|row.get::<_,bool>(0))? {
                return Err(DbError::Conflict("RUN_SCOPE_CLEANUP_UNCONFIRMED".into()));
            }
            let now=crate::time::format_rfc3339_micros(crate::time::now_millis());
            let changed=tx.execute("UPDATE tool_invocations SET cleanup_status='confirmed',updated_at=?3 WHERE invocation_id=?1 AND run_id=?2 AND version=?4",params![invocation,run,now,version])?;
            tx.commit()?;
            Ok(if changed==1 {crate::CasOutcome::Applied} else {crate::CasOutcome::VersionConflict})
        }).await
    }
}
