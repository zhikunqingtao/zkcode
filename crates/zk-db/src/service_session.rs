//! Dedicated external service sessions cannot also accept ordinary conversations.
use crate::{Db, DbError};
use rusqlite::Connection;

pub(crate) fn require_conversation(conn: &Connection, session: &str) -> Result<(), DbError> {
    let dedicated=conn.query_row("SELECT EXISTS(SELECT 1 FROM tasks WHERE session_id=?1 AND task_type='mcp' AND parent_task_id IS NULL)",[session],|r|r.get::<_,bool>(0))?;
    if dedicated {
        Err(DbError::Validation("MCP_SESSION_DEDICATED".into()))
    } else {
        Ok(())
    }
}

pub(crate) fn require_service(conn: &Connection, session: &str) -> Result<(), DbError> {
    let ordinary=conn.query_row("SELECT EXISTS(SELECT 1 FROM tasks WHERE session_id=?1 AND parent_task_id IS NULL AND task_type<>'mcp') OR (EXISTS(SELECT 1 FROM messages WHERE session_id=?1) AND NOT EXISTS(SELECT 1 FROM tasks WHERE session_id=?1 AND task_type='mcp'))",[session],|r|r.get::<_,bool>(0))?;
    if ordinary {
        Err(DbError::Validation("MCP_SESSION_NOT_EMPTY".into()))
    } else {
        Ok(())
    }
}

impl Db {
    /// Reject a dedicated external MCP Session before accepting a Query body.
    /// The execution writer repeats this check in the same transaction as admission.
    /// # Errors
    /// Returns a validation error for service sessions, or a database read error.
    pub async fn require_conversation_session(&self, session: &str) -> Result<(), DbError> {
        let session = session.to_owned();
        self.with_reader(move |conn| require_conversation(conn, &session))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CreateTaskWithRun, TaskBudgetLimits};
    fn request(session: &str) -> CreateTaskWithRun {
        CreateTaskWithRun{task_id:uuid::Uuid::new_v4().to_string(),run_id:uuid::Uuid::new_v4().to_string(),root_session_id:session.into(),transcript_session_id:session.into(),parent_task_id:None,parent_run_id:None,creator_tool_use_id:None,ordinal:0,description:"Local MCP".into(),prompt:None,task_type:"mcp".into(),model:"model".into(),working_dir:"/tmp".into(),execution_config_json:serde_json::json!({"executor":"localMcp","isolation":"readOnly","budget":{"deadlineAtMs":crate::time::now_millis()+60_000}}).to_string(),startup_epoch:0}
    }
    #[tokio::test]
    async fn service_and_query_admission_cannot_both_win_the_same_session() {
        let db = Db::open_in_memory().unwrap();
        for _ in 0..8 {
            let session = db.create_session("model", "/tmp").await.unwrap().id;
            let service = request(&session);
            let run = uuid::Uuid::new_v4().to_string();
            let limits = TaskBudgetLimits {
                token_limit: Some(100),
                cost_limit_nanos_usd: Some(1_000),
                deadline_at_ms: Some(crate::time::now_millis() + 60_000),
            };
            let (mcp, query) = tokio::join!(
                db.create_task_with_run(&service),
                db.start_root_run_with_budget(&run, &session, Some("query"), "model", &limits)
            );
            assert_ne!(
                mcp.is_ok(),
                query.is_ok(),
                "exactly one session kind may win"
            );
            if mcp.is_ok() {
                assert!(db.require_conversation_session(&session).await.is_err());
                assert!(
                    db.start_run("cannot-bypass", &session, None, Some("query"), "model")
                        .await
                        .is_err()
                );
            } else {
                assert!(db.create_task_with_run(&request(&session)).await.is_err());
            }
            let count = db
                .with_reader(move |conn| {
                    Ok(conn.query_row(
                        "SELECT COUNT(*) FROM tasks WHERE session_id=?1",
                        [session],
                        |r| r.get::<_, i64>(0),
                    )?)
                })
                .await
                .unwrap();
            assert_eq!(count, 1);
        }
    }
}
