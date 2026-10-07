//! Durable team configuration and scheduling queue. `TaskRuntime` owns execution.
#![allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::missing_errors_doc
)]
use crate::time::{format_rfc3339_micros, now_millis};
use crate::{Db, DbError};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Logical team policy; its status is an intake switch, not an execution result.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TeamDefinition {
    /// Stable team name.
    pub id: String,
    /// Root session scope.
    pub session_id: String,
    /// Validated immutable worker policy.
    pub config: Value,
    /// open, stopping, or shutdown.
    pub status: String,
    /// Intake revision.
    pub revision: i64,
}
/// One immutable task specification with CAS claim and optional durable Task binding.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TeamWorkItem {
    /// Queue identity, also the `TaskRuntime` idempotency source.
    pub id: String,
    /// Team name.
    pub team_id: String,
    /// Original durable parent Task.
    pub parent_task_id: String,
    /// Original durable parent Run; never follows a new session Run.
    pub parent_run_id: String,
    /// Validated prompt and execution configuration.
    pub payload: Value,
    /// Queue disposition, separate from the linked Task's execution state.
    pub status: String,
    /// Unique claim capability generated transactionally.
    pub claim_id: Option<String>,
    /// Runtime task identity when bound.
    pub task_id: Option<String>,
    /// Explicit scheduling failure, if any.
    pub error_code: Option<String>,
}
const ITEM_COLUMNS: &str =
    "id,team_id,parent_task_id,parent_run_id,payload_json,status,claim_id,task_id,error_code";
fn item(row: &rusqlite::Row<'_>) -> rusqlite::Result<TeamWorkItem> {
    let raw: String = row.get(4)?;
    let payload = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(TeamWorkItem {
        id: row.get(0)?,
        team_id: row.get(1)?,
        parent_task_id: row.get(2)?,
        parent_run_id: row.get(3)?,
        payload,
        status: row.get(5)?,
        claim_id: row.get(6)?,
        task_id: row.get(7)?,
        error_code: row.get(8)?,
    })
}
fn definition(conn: &Connection, id: &str) -> Result<Option<TeamDefinition>, DbError> {
    let raw = conn
        .query_row(
            "SELECT session_id,config_json,status,revision FROM team_definitions WHERE id=?1",
            [id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;
    raw.map(|(session_id, config, status, revision)| {
        Ok(TeamDefinition {
            id: id.into(),
            session_id,
            config: serde_json::from_str(&config)?,
            status,
            revision,
        })
    })
    .transpose()
}
fn active_capacity(conn: &Connection, team: &str) -> Result<i64, DbError> {
    Ok(conn.query_row("SELECT COUNT(*) FROM team_work_items q LEFT JOIN tasks t ON t.id=q.task_id WHERE q.team_id=?1 AND (q.status='claimed' OR (q.status='bound' AND t.status NOT IN ('succeeded','partial','failed','cancelled')))",[team],|row|row.get(0))?)
}
fn pending_run_work(conn: &Connection, run_id: &str) -> Result<bool, DbError> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM team_work_items q LEFT JOIN tasks t ON t.id=q.task_id LEFT JOIN task_dependencies d ON d.parent_task_id=q.parent_task_id AND d.child_task_id=q.task_id WHERE q.parent_run_id=?1 AND (q.status IN ('queued','claimed') OR (q.status='bound' AND (t.status NOT IN ('succeeded','partial','failed','cancelled') OR d.consumed_result_version IS NULL))))",
        [run_id], |row| row.get(0))?)
}
impl Db {
    /// Unclaimed team work and unconsumed bound results are dependencies of the
    /// original parent Run, even before a queued item has a Task identity.
    pub async fn pending_team_work(&self, run_id: &str) -> Result<bool, DbError> {
        let run_id = run_id.to_owned();
        self.with_reader(move |conn| pending_run_work(conn, &run_id))
            .await
    }
    /// Atomically close team admission at the root's natural final-answer boundary.
    /// False means an accepted dependency must finish before finalization.
    pub async fn seal_team_dispatch_if_quiescent(&self, run_id: &str) -> Result<bool, DbError> {
        let run_id = run_id.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            if pending_run_work(&tx, &run_id)? {
                return Ok(false);
            }
            tx.execute(
                "INSERT OR IGNORE INTO team_run_closures(run_id) VALUES(?1)",
                [&run_id],
            )?;
            tx.commit()?;
            Ok(true)
        })
        .await
    }
    /// Physical worker activity, read from the authoritative ledgers. Token count
    /// is the sum of reported usage; missing provider usage remains visible in
    /// the LLM ledger rather than being synthesized here.
    pub async fn team_worker_activity(&self, task_id: &str) -> Result<(i64, i64), DbError> {
        let task_id = task_id.to_owned();
        self.with_reader(move |conn| {
            let tools = conn.query_row("SELECT COUNT(*) FROM tool_invocations WHERE task_id=?1 AND invocation_kind='tool' AND started_at IS NOT NULL", [&task_id], |row| row.get(0))?;
            let tokens = conn.query_row("SELECT COALESCE(SUM(input_tokens+output_tokens),0) FROM llm_calls WHERE task_id=?1", [&task_id], |row| row.get(0))?;
            Ok((tools, tokens))
        }).await
    }
    /// Delete only after every claimed or bound worker is durably quiescent.
    pub async fn delete_team_if_quiescent(&self, id: &str) -> Result<(), DbError> {
        let id = id.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            if active_capacity(&tx, &id)? > 0 {
                return Err(DbError::Conflict("TEAM_CLEANUP_PENDING".into()));
            }
            tx.execute(
                "DELETE FROM team_definitions WHERE id=?1 AND status<>'open'",
                [id],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }
    /// Reconcile the binding crash window without replaying worker side effects.
    /// Old unclaimed work is interrupted once its original parent is no longer live.
    pub async fn reconcile_team_queue(&self, startup_epoch: i64) -> Result<(), DbError> {
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            tx.execute("UPDATE team_work_items SET task_id=(SELECT t.id FROM tasks t WHERE t.creator_tool_use_id=team_work_items.id AND t.creator_run_id=team_work_items.parent_run_id AND t.parent_task_id=team_work_items.parent_task_id),status='bound' WHERE status='claimed' AND EXISTS(SELECT 1 FROM tasks t WHERE t.creator_tool_use_id=team_work_items.id AND t.creator_run_id=team_work_items.parent_run_id AND t.parent_task_id=team_work_items.parent_task_id)",[])?;
            tx.execute("UPDATE team_work_items SET status='interrupted',error_code='TEAM_PROCESS_INTERRUPTED' WHERE (status='claimed' AND claim_epoch<>?1) OR (status='queued' AND NOT EXISTS(SELECT 1 FROM tasks t JOIN run_envelopes r ON r.id=t.current_run_id WHERE t.id=team_work_items.parent_task_id AND r.id=team_work_items.parent_run_id AND r.startup_epoch=?1 AND t.status IN ('running','waitingDependencies','waitingInteraction') AND r.status IN ('running','waitingDependencies','waitingInteraction')))",[startup_epoch])?;
            tx.commit()?;
            Ok(())
        }).await
    }
    /// Create a team atomically; no in-memory lifecycle is installed first.
    pub async fn create_team(
        &self,
        id: &str,
        session_id: &str,
        config: Value,
    ) -> Result<TeamDefinition, DbError> {
        if !config["maxWorkers"]
            .as_u64()
            .is_some_and(|value| (1..=20).contains(&value))
            || !config["taskQueueSize"]
                .as_u64()
                .is_some_and(|value| (1..=200).contains(&value))
            || id.is_empty()
            || id.len() > 64
        {
            return Err(DbError::Validation("TEAM_CONFIG_INVALID".into()));
        }
        let (id, session_id) = (id.to_owned(), session_id.to_owned());
        self.with_writer(move|conn| {
            let tx=conn.transaction()?;
            crate::content::require_persistent_session(&tx, &session_id)?;
            let owned:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1 AND kind='root')",[&session_id],|row|row.get(0))?;
            if !owned{return Err(DbError::SessionNotFound(session_id));}
            if definition(&tx,&id)?.is_some(){return Err(DbError::Conflict("TEAM_ALREADY_EXISTS".into()));}
            tx.execute("INSERT INTO team_definitions(id,session_id,config_json,status,created_at) VALUES(?1,?2,?3,'open',?4)",params![id,session_id,config.to_string(),format_rfc3339_micros(now_millis())])?;
            let result=definition(&tx,&id)?.ok_or_else(||DbError::Invalid("TEAM_CREATE_FAILED".into()))?;
            tx.commit()?;Ok(result)
        }).await
    }
    /// Read policy directly from `SQLite`.
    pub async fn find_team(&self, id: &str) -> Result<Option<TeamDefinition>, DbError> {
        let id = id.to_owned();
        self.with_reader(move |conn| definition(conn, &id)).await
    }
    /// List stable team definitions.
    pub async fn list_teams(&self) -> Result<Vec<TeamDefinition>, DbError> {
        self.with_reader(|conn| {
            let ids = conn
                .prepare("SELECT id FROM team_definitions ORDER BY created_at DESC,id")?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            ids.into_iter()
                .map(|id| {
                    definition(conn, &id)?.ok_or_else(|| DbError::Invalid("TEAM_MISSING".into()))
                })
                .collect()
        })
        .await
    }
    /// Freeze a complete dispatch batch, or replay the same request without duplication.
    pub async fn enqueue_team_work(
        &self,
        id: &str,
        session_id: &str,
        parent_task_id: &str,
        parent_run_id: &str,
        request_id: &str,
        payloads: Vec<Value>,
    ) -> Result<Vec<TeamWorkItem>, DbError> {
        if payloads.is_empty()
            || payloads.len() > 200
            || request_id.is_empty()
            || request_id.len() > 128
        {
            return Err(DbError::Validation("TEAM_DISPATCH_INVALID".into()));
        }
        let (id, session_id, parent_task_id, parent_run_id, request_id) = (
            id.to_owned(),
            session_id.to_owned(),
            parent_task_id.to_owned(),
            parent_run_id.to_owned(),
            request_id.to_owned(),
        );
        self.with_writer(move|conn|{
            let tx=conn.transaction()?;
            crate::content::require_persistent_session(&tx, &session_id)?;
            let team=definition(&tx,&id)?.filter(|team|team.session_id==session_id).ok_or_else(||DbError::Invalid("TEAM_NOT_OWNED".into()))?;
            let sql=format!("SELECT {ITEM_COLUMNS} FROM team_work_items WHERE team_id=?1 AND request_id=?2 ORDER BY ordinal");
            let prior=tx.prepare(&sql)?.query_map(params![id,request_id],item)?.collect::<Result<Vec<_>,_>>()?;
            if !prior.is_empty(){
                if prior.len()!=payloads.len()||prior.iter().zip(&payloads).any(|(prior,payload)|prior.payload!=*payload||prior.parent_run_id!=parent_run_id){return Err(DbError::Conflict("TEAM_DISPATCH_IDEMPOTENCY_MISMATCH".into()));}
                return Ok(prior);
            }
            if team.status!="open"{return Err(DbError::Conflict("TEAM_INTAKE_CLOSED".into()));}
            let closed: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM team_run_closures WHERE run_id=?1)", [&parent_run_id], |row| row.get(0))?;
            if closed { return Err(DbError::Conflict("TEAM_PARENT_ADMISSION_CLOSED".into())); }

            let parent:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM tasks t JOIN run_envelopes r ON r.id=t.current_run_id WHERE t.id=?1 AND r.id=?2 AND t.session_id=?3 AND t.status IN ('running','waitingDependencies','waitingInteraction') AND r.status IN ('running','waitingDependencies','waitingInteraction'))",params![parent_task_id,parent_run_id,session_id],|row|row.get(0))?;
            if !parent{return Err(DbError::Invalid("TEAM_PARENT_NOT_ACTIVE".into()));}
            let outstanding:i64=tx.query_row("SELECT COUNT(*) FROM team_work_items WHERE team_id=?1 AND status IN ('queued','claimed')",[&id],|row|row.get(0))?;
            let capacity=team.config["taskQueueSize"].as_i64().unwrap_or(50);
            if outstanding+i64::try_from(payloads.len()).unwrap_or(i64::MAX)>capacity{return Err(DbError::Conflict("TEAM_QUEUE_FULL".into()));}
            let now=format_rfc3339_micros(now_millis());
            for (ordinal,payload) in payloads.into_iter().enumerate(){tx.execute("INSERT INTO team_work_items(id,team_id,request_id,ordinal,payload_json,parent_task_id,parent_run_id,status,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,'queued',?8)",params![uuid::Uuid::new_v4().to_string(),id,request_id,i64::try_from(ordinal).unwrap_or(i64::MAX),payload.to_string(),parent_task_id,parent_run_id,now])?;}
            let rows=tx.prepare(&sql)?.query_map(params![id,request_id],item)?.collect::<Result<Vec<_>,_>>()?;
            tx.commit()?;Ok(rows)
        }).await
    }
    /// Claim at most one queue item while enforcing team capacity in the same transaction.
    pub async fn claim_team_work(
        &self,
        id: &str,
        startup_epoch: i64,
    ) -> Result<Option<TeamWorkItem>, DbError> {
        let id = id.to_owned();
        self.with_writer(move|conn|{
            let tx=conn.transaction()?;let Some(team)=definition(&tx,&id)? else{return Ok(None)};
            if team.status!="open"{return Ok(None)}
            tx.execute("UPDATE team_work_items SET status='interrupted',error_code='TEAM_PARENT_NOT_ACTIVE' WHERE team_id=?1 AND status='queued' AND NOT EXISTS(SELECT 1 FROM tasks t JOIN run_envelopes r ON r.id=t.current_run_id WHERE t.id=team_work_items.parent_task_id AND r.id=team_work_items.parent_run_id AND t.status IN ('running','waitingDependencies','waitingInteraction') AND r.status IN ('running','waitingDependencies','waitingInteraction'))",[&id])?;
            if active_capacity(&tx,&id)? >= team.config["maxWorkers"].as_i64().unwrap_or(5){tx.commit()?;return Ok(None)}
            let next:Option<String>=tx.query_row("SELECT id FROM team_work_items WHERE team_id=?1 AND status='queued' ORDER BY created_at,rowid LIMIT 1",[&id],|row|row.get(0)).optional()?;
            let Some(next)=next else{tx.commit()?;return Ok(None)};
            let claim=uuid::Uuid::new_v4().to_string();
            tx.execute("UPDATE team_work_items SET status='claimed',claim_id=?2,claim_epoch=?3 WHERE id=?1 AND status='queued'",params![next,claim,startup_epoch])?;
            let result=tx.query_row(&format!("SELECT {ITEM_COLUMNS} FROM team_work_items WHERE id=?1"),[next],item)?;
            tx.commit()?;Ok(Some(result))
        }).await
    }
    /// Bind a claimed queue item to the exact `TaskRuntime` child. Replays are idempotent.
    pub async fn bind_team_work(
        &self,
        id: &str,
        claim: &str,
        task_id: &str,
    ) -> Result<(), DbError> {
        let (id, claim, task_id) = (id.to_owned(), claim.to_owned(), task_id.to_owned());
        self.with_writer(move|conn|{
            let changed=conn.execute("UPDATE team_work_items SET status='bound',task_id=?3 WHERE id=?1 AND claim_id=?2 AND status IN ('claimed','bound') AND (task_id IS NULL OR task_id=?3) AND EXISTS(SELECT 1 FROM tasks t WHERE t.id=?3 AND t.creator_tool_use_id=team_work_items.id AND t.creator_run_id=team_work_items.parent_run_id AND t.parent_task_id=team_work_items.parent_task_id)",params![id,claim,task_id])?;
            if changed!=1{return Err(DbError::Conflict("TEAM_CLAIM_LOST".into()));}Ok(())
        }).await
    }
    /// Record a scheduling rejection; it is never presented as an executed Task result.
    pub async fn reject_team_work(
        &self,
        id: &str,
        claim: &str,
        error: &str,
    ) -> Result<(), DbError> {
        let (id, claim, error) = (id.to_owned(), claim.to_owned(), error.to_owned());
        self.with_writer(move|conn|{conn.execute("UPDATE team_work_items SET status='rejected',error_code=?3 WHERE id=?1 AND claim_id=?2 AND status='claimed'",params![id,claim,error])?;Ok(())}).await
    }
    /// Return queue rows; callers project execution state from bound Task records.
    pub async fn team_work_items(&self, id: &str) -> Result<Vec<TeamWorkItem>, DbError> {
        let id = id.to_owned();
        self.with_reader(move |conn| {
            let sql = format!(
                "SELECT {ITEM_COLUMNS} FROM team_work_items WHERE team_id=?1 ORDER BY created_at,rowid"
            );
            Ok(conn
                .prepare(&sql)?
                .query_map([id], item)?
                .collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }
    /// Close intake first; claimed children remain visible for supervised cancellation.
    pub async fn stop_team(&self, id: &str, shutdown: bool) -> Result<(), DbError> {
        let id = id.to_owned();
        self.with_writer(move|conn|{let tx=conn.transaction()?;let changed=tx.execute("UPDATE team_definitions SET status=?2,revision=revision+1 WHERE id=?1",params![id,if shutdown{"shutdown"}else{"stopping"}])?;if changed==0{return Err(DbError::Invalid("TEAM_NOT_FOUND".into()));}tx.execute("UPDATE team_work_items SET status='cancelled',error_code='TEAM_STOPPED_BEFORE_CLAIM' WHERE team_id=?1 AND status='queued'",[id])?;tx.commit()?;Ok(())}).await
    }
    /// Freeze broadcast recipients and insert all inbox rows atomically. Retrying an ID
    /// returns the original receiver snapshot, even if membership has since changed.
    pub async fn broadcast_team(
        &self,
        id: &str,
        session_id: &str,
        request_id: &str,
        content: &str,
    ) -> Result<Vec<String>, DbError> {
        if request_id.is_empty()
            || request_id.len() > 128
            || content.trim().is_empty()
            || content.len() > 32768
        {
            return Err(DbError::Validation("TEAM_BROADCAST_INVALID".into()));
        }
        let (id, session_id, request_id, content) = (
            id.to_owned(),
            session_id.to_owned(),
            request_id.to_owned(),
            content.to_owned(),
        );
        self.with_writer(move|conn|{
            let tx=conn.transaction()?;let team=definition(&tx,&id)?.filter(|team|team.session_id==session_id).ok_or_else(||DbError::Invalid("TEAM_NOT_OWNED".into()))?;
            let digest=format!("{:x}",Sha256::digest(content.as_bytes()));
            let prior:Option<(String,String)>=tx.query_row("SELECT content_sha256,receivers_json FROM team_broadcasts WHERE team_id=?1 AND request_id=?2",params![id,request_id],|row|Ok((row.get(0)?,row.get(1)?))).optional()?;
            if let Some((previous,receivers))=prior{if previous!=digest{return Err(DbError::Conflict("TEAM_BROADCAST_IDEMPOTENCY_MISMATCH".into()));}return Ok(serde_json::from_str(&receivers)?)}
            if team.status!="open"{return Err(DbError::Conflict("TEAM_INTAKE_CLOSED".into()));}
            let recipients=tx.prepare("SELECT t.id,t.current_run_id FROM team_work_items q JOIN tasks t ON t.id=q.task_id JOIN run_envelopes r ON r.id=t.current_run_id WHERE q.team_id=?1 AND q.status='bound' AND t.session_id=?2 AND t.status IN ('queued','running','waitingDependencies','waitingInteraction') AND r.status IN ('queued','running','waitingDependencies','waitingInteraction') AND r.requested_exit_reason IS NULL ORDER BY t.id")?.query_map(params![id,session_id],|row|Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?)))?.collect::<Result<Vec<_>,_>>()?;
            let now=format_rfc3339_micros(now_millis());let mut receivers=Vec::new();
            for (task,run) in recipients{tx.execute("INSERT INTO task_inbox_messages(message_id,task_id,target_run_id,content,status,created_at) VALUES(?1,?2,?3,?4,'queued',?5)",params![uuid::Uuid::new_v4().to_string(),task,run,content,now])?;receivers.push(task);}
            tx.execute("INSERT INTO team_broadcasts(team_id,request_id,content_sha256,receivers_json,created_at) VALUES(?1,?2,?3,?4,?5)",params![id,request_id,digest,serde_json::to_string(&receivers)?,now])?;tx.commit()?;Ok(receivers)
        }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CreateTaskWithRun, CreateTaskWithRunOutcome};
    fn id() -> String {
        uuid::Uuid::new_v4().to_string()
    }
    async fn parent(db: &Db) -> CreateTaskWithRunOutcome {
        let session = db.create_session("test", "/tmp").await.unwrap().id;
        let root = db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: id(),
                run_id: id(),
                root_session_id: session.clone(),
                transcript_session_id: session,
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: "parent".into(),
                prompt: Some("user goal".into()),
                task_type: "agent".into(),
                model: "test".into(),
                working_dir: "/tmp".into(),
                execution_config_json:
                    serde_json::json!({"budget":{"deadlineAtMs":now_millis()+60000}}).to_string(),
                startup_epoch: 1,
            })
            .await
            .unwrap();
        assert_eq!(
            db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
                .await
                .unwrap(),
            crate::CasOutcome::Applied
        );
        root
    }
    async fn child(
        db: &Db,
        parent: &CreateTaskWithRunOutcome,
        item: &TeamWorkItem,
    ) -> CreateTaskWithRunOutcome {
        db.create_task_with_run(&CreateTaskWithRun {
            task_id: id(),
            run_id: id(),
            root_session_id: parent.task.session_id.clone(),
            transcript_session_id: id(),
            parent_task_id: Some(parent.task.id.clone()),
            parent_run_id: Some(parent.run_id.clone()),
            creator_tool_use_id: Some(item.id.clone()),
            ordinal: 0,
            description: "worker".into(),
            prompt: Some("work".into()),
            task_type: "agent".into(),
            model: "test".into(),
            working_dir: "/tmp".into(),
            execution_config_json: r#"{"isolation":"readOnly","lifecycle":"attached"}"#.into(),
            startup_epoch: 1,
        })
        .await
        .unwrap()
    }
    async fn enqueue(
        db: &Db,
        root: &CreateTaskWithRunOutcome,
        request: &str,
        count: usize,
    ) -> Result<Vec<TeamWorkItem>, DbError> {
        db.enqueue_team_work(
            "team",
            &root.task.session_id,
            &root.task.id,
            &root.run_id,
            request,
            (0..count)
                .map(|i| serde_json::json!({"prompt":format!("work {i}")}))
                .collect(),
        )
        .await
    }

    #[tokio::test]
    async fn queue_capacity_claim_cas_and_idempotency_are_one_transaction() {
        let db = Db::open_in_memory().unwrap();
        let root = parent(&db).await;
        db.create_team(
            "team",
            &root.task.session_id,
            serde_json::json!({"maxWorkers":1,"taskQueueSize":2}),
        )
        .await
        .unwrap();
        let first = enqueue(&db, &root, "request", 2).await.unwrap();
        assert_eq!(
            enqueue(&db, &root, "request", 2).await.unwrap()[0].id,
            first[0].id
        );
        assert!(matches!(
            enqueue(&db, &root, "request", 1).await,
            Err(DbError::Conflict(_))
        ));
        assert!(matches!(
            enqueue(&db, &root, "overflow", 1).await,
            Err(DbError::Conflict(_))
        ));
        let (a, b) = tokio::join!(db.claim_team_work("team", 1), db.claim_team_work("team", 1));
        let a = a.unwrap();
        let b = b.unwrap();
        assert_eq!(usize::from(a.is_some()) + usize::from(b.is_some()), 1);
        let claimed = a.or(b).unwrap();
        assert_eq!(claimed.id, first[0].id);
        let task = child(&db, &root, &claimed).await;
        assert!(
            db.bind_team_work(&claimed.id, "wrong-claim", &task.task.id)
                .await
                .is_err()
        );
        db.bind_team_work(
            &claimed.id,
            claimed.claim_id.as_ref().unwrap(),
            &task.task.id,
        )
        .await
        .unwrap();
        assert!(db.claim_team_work("team", 1).await.unwrap().is_none());
        db.stop_team("team", false).await.unwrap();
        assert_eq!(
            db.team_work_items("team").await.unwrap()[1].status,
            "cancelled"
        );
        assert!(db.claim_team_work("team", 1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn final_answer_seal_and_enqueue_are_atomic_and_ephemeral_is_rejected() {
        for _ in 0..8 {
            let db = Db::open_in_memory().unwrap();
            let root = parent(&db).await;
            db.create_team(
                "team",
                &root.task.session_id,
                serde_json::json!({"maxWorkers":1,"taskQueueSize":3}),
            )
            .await
            .unwrap();
            let (sealed, queued) = tokio::join!(
                db.seal_team_dispatch_if_quiescent(&root.run_id),
                enqueue(&db, &root, "request", 1)
            );
            match (sealed.unwrap(), queued) {
                (true, Err(DbError::Conflict(code))) => {
                    assert_eq!(code, "TEAM_PARENT_ADMISSION_CLOSED");
                }
                (false, Ok(items)) => {
                    assert_eq!(items.len(), 1);
                    assert!(db.pending_team_work(&root.run_id).await.unwrap());
                    db.stop_team("team", false).await.unwrap();
                    assert!(
                        db.seal_team_dispatch_if_quiescent(&root.run_id)
                            .await
                            .unwrap()
                    );
                }
                other => panic!("accepted work and finalization raced: {other:?}"),
            }
        }
        let db = Db::open_in_memory().unwrap();
        let root = parent(&db).await;
        db.create_team(
            "team",
            &root.task.session_id,
            serde_json::json!({"maxWorkers":1,"taskQueueSize":3}),
        )
        .await
        .unwrap();
        let (session, _lease) = db
            .create_ephemeral_session("test", "/tmp", "DONT_ASK")
            .await
            .unwrap();
        assert!(
            matches!(db.enqueue_team_work("team", &session, &root.task.id, &root.run_id, "private", vec![serde_json::json!({"prompt":"must remain private"})]).await, Err(DbError::Validation(code)) if code=="EPHEMERAL_OPERATION_UNSUPPORTED")
        );
        assert!(
            matches!(db.create_team("private", &session, serde_json::json!({"maxWorkers":1,"taskQueueSize":3})).await, Err(DbError::Validation(code)) if code=="EPHEMERAL_OPERATION_UNSUPPORTED")
        );
        assert!(db.team_work_items("team").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn broadcast_freezes_membership_and_restart_repairs_only_binding() {
        let db = Db::open_in_memory().unwrap();
        let root = parent(&db).await;
        db.create_team(
            "team",
            &root.task.session_id,
            serde_json::json!({"maxWorkers":3,"taskQueueSize":4}),
        )
        .await
        .unwrap();
        enqueue(&db, &root, "request", 4).await.unwrap();
        let first = db.claim_team_work("team", 1).await.unwrap().unwrap();
        let initial_child = child(&db, &root, &first).await;
        // Simulate a crash after TaskRuntime committed but before queue binding.
        db.reconcile_team_queue(1).await.unwrap();
        assert_eq!(
            db.team_work_items("team").await.unwrap()[0]
                .task_id
                .as_deref(),
            Some(initial_child.task.id.as_str())
        );
        let recipients = db
            .broadcast_team(
                "team",
                &root.task.session_id,
                "broadcast",
                "read current requirements",
            )
            .await
            .unwrap();
        assert_eq!(recipients, vec![initial_child.task.id.clone()]);
        let second = db.claim_team_work("team", 1).await.unwrap().unwrap();
        let later_child = child(&db, &root, &second).await;
        db.bind_team_work(
            &second.id,
            second.claim_id.as_deref().unwrap(),
            &later_child.task.id,
        )
        .await
        .unwrap();

        assert_eq!(
            db.broadcast_team(
                "team",
                &root.task.session_id,
                "broadcast",
                "read current requirements"
            )
            .await
            .unwrap(),
            recipients
        );
        assert!(
            db.broadcast_team("team", "another-session", "foreign", "text")
                .await
                .is_err()
        );
        assert!(
            db.broadcast_team("team", &root.task.session_id, "broadcast", "changed")
                .await
                .is_err()
        );
        let inbox = db
            .read_task_inbox(&initial_child.task.id, &[crate::InboxStatus::Queued], 20)
            .await
            .unwrap();
        assert_eq!(inbox.len(), 1);
        assert!(
            db.read_task_inbox(&later_child.task.id, &[crate::InboxStatus::Queued], 20)
                .await
                .unwrap()
                .is_empty(),
            "a retry cannot expand the original frozen recipient set"
        );
        let claimed = db.claim_team_work("team", 1).await.unwrap().unwrap();
        db.reconcile_team_queue(2).await.unwrap();
        let rows = db.team_work_items("team").await.unwrap();
        assert_eq!(
            rows.iter().find(|row| row.id == claimed.id).unwrap().status,
            "interrupted"
        );
        assert_eq!(rows[3].status, "interrupted");
        assert_eq!(
            rows[0].task_id.as_deref(),
            Some(initial_child.task.id.as_str())
        );
    }
}
