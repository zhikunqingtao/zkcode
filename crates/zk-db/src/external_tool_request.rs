//! Explicit operation identities prevent replaying external tool side effects.
use crate::{CleanupStatus, Db, DbError};
use rusqlite::{OptionalExtension, params};
use serde_json::Value;

/// Admission result for a host-bound operation identity, not a JSON-RPC transport id.
#[derive(Clone, Debug)]
pub enum ExternalToolAdmission {
    /// This caller owns the first execution attempt.
    Accepted {
        /// Newly allocated physical tool-use identity.
        tool_use_id: String,
    },
    /// The original operation has immutable tool facts and completed derived facts.
    Completed {
        /// Original physical tool-use identity.
        tool_use_id: String,
        /// Original immutable result message.
        message_id: String,
    },
    /// Execution may still be running or have unknown effects; never automatically repeat it.
    Unconfirmed,
}

/// Durable terminal facts for an external operation's immutable tool result.
#[derive(Clone, Debug)]
pub struct ExternalToolFacts {
    /// Physical invocation identity.
    pub invocation_id: String,
    /// Exact canonical result message.
    pub message_id: String,
    /// Tool lifecycle terminal, independent of the containing service lifetime.
    pub status: String,
    /// Confirmed or still unresolved cleanup.
    pub cleanup_status: CleanupStatus,
}

impl Db {
    /// Reserve one explicit operation while the bound MCP service is live.
    /// Content comparison uses the same retention scope, never a persistent body hash.
    ///
    /// # Errors
    /// Rejects identity reuse with different content, inactive/wrong owners and storage failures.
    pub async fn admit_external_tool_operation(
        &self,
        session: &str,
        run: &str,
        operation: &str,
        tool: &str,
        input: &Value,
    ) -> Result<ExternalToolAdmission, DbError> {
        if operation.is_empty()
            || operation.len() > 128
            || !operation
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-:.".contains(&b))
        {
            return Err(DbError::Validation("EXTERNAL_OPERATION_ID_INVALID".into()));
        }
        let (session, run, operation, tool, input) = (
            session.to_owned(),
            run.to_owned(),
            operation.to_owned(),
            tool.to_owned(),
            input.clone(),
        );
        self.with_writer(move |conn| {
            let tx=conn.transaction()?;
            let owned:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM tasks t JOIN run_envelopes r ON r.task_id=t.id JOIN sessions s ON s.id=r.session_id WHERE r.id=?1 AND r.session_id=?2 AND t.session_id=?2 AND t.task_type='mcp' AND t.current_run_id=r.id)",params![run,session],|row|row.get(0))?;
            if !owned {return Err(DbError::Invalid("EXTERNAL_OPERATION_OWNER_MISMATCH".into()));}
            let old:Option<(String,String,String,Option<String>)>=tx.query_row("SELECT tool_name,input_json,tool_use_id,result_message_id FROM external_tool_requests WHERE run_id=?1 AND operation_id=?2",params![run,operation],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).optional()?;
            if let Some((name,body,tool_use_id,result))=old {
                let body:Value=serde_json::from_str(&crate::content::load_text(&tx,&session,&body)?)?;
                if name!=tool||body!=input {return Err(DbError::Conflict("EXTERNAL_OPERATION_CONFLICT".into()));}
                return Ok(match result {Some(message_id)=>ExternalToolAdmission::Completed {tool_use_id,message_id},None=>ExternalToolAdmission::Unconfirmed});
            }
            let active:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM run_envelopes r JOIN tasks t ON t.id=r.task_id JOIN sessions s ON s.id=r.session_id WHERE r.id=?1 AND r.status='running' AND r.requested_exit_reason IS NULL AND t.status='running' AND s.status='active')",[&run],|row|row.get(0))?;
            if !active {return Err(DbError::Conflict("EXTERNAL_OPERATION_RUN_CLOSED".into()));}
            let tool_use_id=format!("mcp_{}",uuid::Uuid::new_v4());
            let encoded=crate::content::store_text(&tx,&session,&input.to_string())?;
            tx.execute("INSERT INTO external_tool_requests(run_id,operation_id,session_id,tool_name,input_json,tool_use_id) VALUES(?1,?2,?3,?4,?5,?6)",params![run,operation,session,tool,encoded,tool_use_id])?;
            tx.commit()?;Ok(ExternalToolAdmission::Accepted {tool_use_id})
        }).await
    }

    /// Publish replayability only after the real result and all required projections exist.
    /// Does not alter tool status, result content or the containing Task/Run outcome.
    ///
    /// # Errors
    /// Unknown/unfinished/unpaired operations and incomplete post-processing remain explicit.
    pub async fn complete_external_tool_operation(
        &self,
        session: &str,
        run: &str,
        operation: &str,
        message: &str,
    ) -> Result<ExternalToolFacts, DbError> {
        let (session, run, operation, message) = (
            session.to_owned(),
            run.to_owned(),
            operation.to_owned(),
            message.to_owned(),
        );
        self.with_writer(move |conn| {
            let tx=conn.transaction()?;
            let row:Option<(String,String,String,Option<String>,String,String)>=tx.query_row(
                "SELECT i.invocation_id,i.status,i.cleanup_status,i.output_ref,q.tool_use_id,q.tool_name FROM external_tool_requests q JOIN tool_invocations i ON i.run_id=q.run_id AND i.tool_use_id=q.tool_use_id AND i.tool_name=q.tool_name JOIN run_envelopes r ON r.id=i.run_id AND r.task_id=i.task_id WHERE q.session_id=?1 AND q.run_id=?2 AND q.operation_id=?3 AND i.invocation_kind='tool'",
                params![session,run,operation],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?))).optional()?;
            let Some((invocation_id,status,cleanup,reference,tool_use_id,_name))=row else {return Err(DbError::Conflict("EXTERNAL_OPERATION_FACTS_MISSING".into()));};
            if !matches!(status.as_str(),"succeeded"|"failed"|"cancelled"|"interrupted") {return Err(DbError::Conflict("EXTERNAL_OPERATION_UNCONFIRMED".into()));}
            let reference=reference.ok_or_else(||DbError::Conflict("EXTERNAL_OPERATION_RESULT_MISSING".into()))?;
            let reference=crate::content::load_text(&tx,&session,&reference)?;
            if reference.split('#').next()!=Some(format!("message:{message}").as_str()) {return Err(DbError::Invalid("EXTERNAL_OPERATION_RESULT_MISMATCH".into()));}
            let owned:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM messages m WHERE m.id=?1 AND m.session_id=?2 AND m.run_id=?3 AND m.origin='tool_result')",params![message,session,run],|row|row.get(0))?;
            if !owned {return Err(DbError::Invalid("EXTERNAL_OPERATION_RESULT_MISMATCH".into()));}
            let pending:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM tool_result_postprocessing WHERE invocation_id=?1 AND status<>'completed')",[&invocation_id],|row|row.get(0))?;
            if pending {return Err(DbError::Conflict("EXTERNAL_OPERATION_PROJECTION_PENDING".into()));}
            let changed=tx.execute("UPDATE external_tool_requests SET result_message_id=?1 WHERE run_id=?2 AND operation_id=?3 AND tool_use_id=?4 AND (result_message_id IS NULL OR result_message_id=?1)",params![message,run,operation,tool_use_id])?;
            if changed!=1 {return Err(DbError::Conflict("EXTERNAL_OPERATION_RESULT_CONFLICT".into()));}
            tx.commit()?;
            Ok(ExternalToolFacts {invocation_id,message_id:message,status,cleanup_status:CleanupStatus::parse(&cleanup)?})
        }).await
    }
}
