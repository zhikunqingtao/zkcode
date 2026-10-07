//! Hook resources require current Run admission both before allocation and before physical dispatch.
use crate::{CasOutcome, Db, DbError, NewExecutionResource};
use rusqlite::{Connection, OptionalExtension, params};

fn require_active(conn: &Connection, task: &str, run: &str) -> Result<(), DbError> {
    let allowed:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM run_envelopes r JOIN tasks t ON t.id=r.task_id WHERE r.id=?1 AND r.task_id=?2 AND t.current_run_id=r.id AND r.status IN ('running','waitingDependencies','waitingInteraction') AND t.status IN ('running','waitingDependencies','waitingInteraction') AND r.requested_exit_reason IS NULL AND (t.deadline_at_ms IS NULL OR t.deadline_at_ms>?3))",params![run,task,crate::time::now_millis()],|row|row.get(0))?;
    if !allowed {
        return Err(DbError::Invalid("HOOK_RUN_NOT_ACTIVE".into()));
    }
    Ok(())
}
impl Db {
    /// Reserve a Hook-only leaf inside the same transaction as its exact Run admission.
    /// # Errors
    /// Missing/stale/cancelled/terminal/expired ownership, invalid metadata or storage failure reject dispatch.
    pub async fn register_hook_execution_resource(
        &self,
        resource: &NewExecutionResource,
    ) -> Result<(), DbError> {
        let resource = resource.clone();
        serde_json::from_str::<serde_json::Value>(&resource.metadata_json)?;
        self.with_writer(move|conn|{
            let tx=conn.transaction()?;
            require_active(&tx,&resource.task_id,&resource.run_id)?;
            if let Some(invocation)=resource.invocation_id.as_deref(){
                let owned:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM tool_invocations WHERE invocation_id=?1 AND run_id=?2 AND task_id=?3)",params![invocation,resource.run_id,resource.task_id],|row|row.get(0))?;
                if !owned{return Err(DbError::Invalid("HOOK_INVOCATION_NOT_OWNED".into()));}
            }
            let now=crate::time::format_rfc3339_micros(crate::time::now_millis());
            tx.execute("INSERT INTO execution_resources(resource_id,task_id,run_id,invocation_id,resource_kind,external_id,status,metadata_json,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,'allocated',?7,?8,?8)",params![resource.resource_id,resource.task_id,resource.run_id,resource.invocation_id,resource.resource_kind,resource.external_id,resource.metadata_json,now])?;
            tx.commit()?;Ok(())
        }).await
    }
    /// Revalidate admission when releasing a command start gate or beginning an HTTP request.
    /// # Errors
    /// A closed parent, changed owner or storage failure leaves the physical operation unstarted.
    pub async fn bind_hook_execution_resource_external(
        &self,
        resource_id: &str,
        external_id: &str,
    ) -> Result<CasOutcome, DbError> {
        if external_id.trim().is_empty() {
            return Err(DbError::Invalid("HOOK_EXTERNAL_ID_REQUIRED".into()));
        }
        let resource = resource_id.to_owned();
        let external = external_id.to_owned();
        self.with_writer(move|conn|{
            let tx=conn.transaction()?;
            let current:Option<(String,String,String,Option<String>)>=tx.query_row("SELECT task_id,run_id,status,external_id FROM execution_resources WHERE resource_id=?1",[&resource],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).optional()?;
            let Some((task,run,status,current))=current else{return Ok(CasOutcome::NotFound);};
            require_active(&tx,&task,&run)?;
            if status!="allocated" || current.as_deref().is_some_and(|value|value!=external) {return Ok(CasOutcome::InvalidTransition);}
            if current.is_none(){tx.execute("UPDATE execution_resources SET external_id=?2,updated_at=?3,version=version+1 WHERE resource_id=?1",params![resource,external,crate::time::format_rfc3339_micros(crate::time::now_millis())])?;}
            tx.commit()?;Ok(CasOutcome::Applied)
        }).await
    }
}
