//! Atomic, idempotent forks of idle persistent conversation snapshots.

use std::collections::HashSet;

use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{Db, DbError, MessageRole, StoredBlock};

const MAX_FORK_BYTES: i64 = 64 * 1024 * 1024;
const MAX_FORK_MESSAGES: i64 = 20_000;

/// Caller intent. Execution options and once-only grants are deliberately absent.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionForkRequest {
    /// Existing idle root session whose history is explicitly selected.
    pub source_session_id: String,
    /// Optional title for the independent target.
    pub title: Option<String>,
}

/// Durable fork identity. An identical request ID returns this original result.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionForkResult {
    /// New independent session, never the source session ID.
    pub session_id: String,
    /// Provenance only; no cross-session permission grant.
    pub source_session_id: String,
    /// Hash of the transactionally sealed source snapshot.
    pub snapshot_sha256: String,
    /// Number of original history messages copied.
    pub message_count: usize,
}

#[derive(Serialize)]
struct SealedSession {
    id: String,
    title: Option<String>,
    model: String,
    working_dir: String,
    permission_mode: Option<String>,
    summary: Option<String>,
    metadata: Option<String>,
    messages: Vec<SealedMessage>,
}

#[derive(Serialize)]
struct SealedMessage {
    id: String,
    role: String,
    content_json: String,
    metadata_json: Option<String>,
    stop_reason: Option<String>,
    created_at: String,
    seq_num: i64,
}

fn validate_history(messages: &[SealedMessage]) -> Result<(), DbError> {
    let mut pending = HashSet::new();
    for message in messages {
        let role = MessageRole::parse(&message.role)
            .ok_or_else(|| DbError::Invalid("FORK_INVALID_HISTORY_ROLE".into()))?;
        // Unlike permissive UI recovery, a fork must not seal malformed or
        // silently dropped blocks as an apparently complete new conversation.
        let blocks: Vec<StoredBlock> = serde_json::from_str(&message.content_json)
            .map_err(|_| DbError::Invalid("FORK_INVALID_HISTORY_CONTENT".into()))?;
        let results = blocks
            .iter()
            .filter(|block| matches!(block, StoredBlock::ToolResult { .. }))
            .count();
        if !pending.is_empty()
            && (role == MessageRole::Assistant || role == MessageRole::User && results == 0)
        {
            return Err(DbError::Conflict("FORK_INCOMPLETE_TOOL_BATCH".into()));
        }
        for block in blocks {
            match block {
                StoredBlock::ToolUse { id, .. } => {
                    if role != MessageRole::Assistant || id.is_empty() || !pending.insert(id) {
                        return Err(DbError::Conflict("FORK_INVALID_TOOL_BATCH".into()));
                    }
                }
                StoredBlock::ToolResult { tool_use_id, .. }
                    if role != MessageRole::User || !pending.remove(&tool_use_id) =>
                {
                    return Err(DbError::Conflict("FORK_INVALID_TOOL_BATCH".into()));
                }
                _ => {}
            }
        }
        if let Some(metadata) = &message.metadata_json {
            serde_json::from_str::<Value>(metadata)
                .map_err(|_| DbError::Invalid("FORK_INVALID_HISTORY_METADATA".into()))?;
        }
    }
    if pending.is_empty() {
        Ok(())
    } else {
        Err(DbError::Conflict("FORK_INCOMPLETE_TOOL_BATCH".into()))
    }
}

impl Db {
    /// Seal an idle source and publish its independent copy in one write transaction.
    ///
    /// # Errors
    /// Rejects missing/non-root/busy sources, malformed history, reused IDs with
    /// different intent, and replay after the original target was deleted.
    #[allow(clippy::too_many_lines)] // Atomic publication and idempotency share one transaction.
    pub async fn fork_session(
        &self,
        request_id: &str,
        request: SessionForkRequest,
    ) -> Result<SessionForkResult, DbError> {
        if request_id.is_empty()
            || request_id.len() > 128
            || request_id.chars().any(char::is_control)
            || request.source_session_id.is_empty()
            || request
                .title
                .as_ref()
                .is_some_and(|title| title.len() > 1024 || title.trim().is_empty())
        {
            return Err(DbError::Validation("FORK_INVALID_REQUEST".into()));
        }
        let request_id = request_id.to_owned();
        let request_json = serde_json::to_string(&request)?;
        self.with_writer(move |conn| {
            let tx=conn.transaction()?;
            let prior:Option<(String,String)>=tx.query_row("SELECT request_json,result_json FROM session_forks WHERE request_id=?1",[&request_id],|row|Ok((row.get(0)?,row.get(1)?))).optional()?;
            if let Some((saved,result))=prior {
                if saved!=request_json { return Err(DbError::Conflict("FORK_REQUEST_ID_CONFLICT".into())); }
                let result:SessionForkResult=serde_json::from_str(&result)?;
                let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1)",[&result.session_id],|row|row.get(0))?;
                if !exists {return Err(DbError::Conflict("FORK_TARGET_DELETED".into()));}
                return Ok(result);
            }
            let source=&request.source_session_id;
            let mut snapshot=tx.query_row("SELECT id,title,model,working_dir,permission_mode,summary,metadata_json FROM sessions WHERE id=?1 AND kind='root'",[source],|row|Ok(SealedSession {
                id:row.get(0)?,title:row.get(1)?,model:row.get(2)?,working_dir:row.get(3)?,permission_mode:row.get(4)?,summary:row.get(5)?,metadata:row.get(6)?,messages:Vec::new(),
            })).optional()?.ok_or_else(||DbError::SessionNotFound(source.clone()))?;
            crate::session_merge::ensure_idle(&tx,source)?;
            let reserved:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM session_merge_locks WHERE session_id=?1)",[source],|row|row.get(0))?;
            if reserved {return Err(DbError::Conflict("FORK_SOURCE_RESERVED".into()));}
            crate::content::require_persistent_session(&tx,source)?;
            let (count,bytes):(i64,i64)=tx.query_row("SELECT COUNT(*),COALESCE(SUM(length(CAST(content_json AS BLOB))+COALESCE(length(CAST(metadata_json AS BLOB)),0)),0) FROM messages WHERE session_id=?1",[source],|row|Ok((row.get(0)?,row.get(1)?)))?;
            if count>MAX_FORK_MESSAGES || bytes>MAX_FORK_BYTES {return Err(DbError::Validation("FORK_SNAPSHOT_LIMIT".into()));}
            snapshot.messages=tx.prepare("SELECT id,role,content_json,metadata_json,stop_reason,created_at,seq_num FROM messages WHERE session_id=?1 ORDER BY seq_num")?
                .query_map([source],|row|Ok(SealedMessage {id:row.get(0)?,role:row.get(1)?,content_json:row.get(2)?,metadata_json:row.get(3)?,stop_reason:row.get(4)?,created_at:row.get(5)?,seq_num:row.get(6)?}))?.collect::<Result<Vec<_>,_>>()?;
            validate_history(&snapshot.messages)?;
            let sealed=serde_json::to_string(&snapshot)?;
            let hash=format!("{:x}",Sha256::digest(sealed.as_bytes()));
            let target=uuid::Uuid::new_v4().to_string();
            let now=crate::time::format_rfc3339_micros(crate::time::now_millis());
            let target_metadata=json!({"forkSourceSessionId":source,"forkRequestId":request_id,"forkSnapshotSha256":hash,"historicalReference":true});
            tx.execute("INSERT INTO sessions(id,title,model,working_dir,permission_mode,summary,metadata_json,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?8)",params![target,request.title.as_ref().or(snapshot.title.as_ref()),snapshot.model,snapshot.working_dir,snapshot.permission_mode,snapshot.summary,target_metadata.to_string(),now])?;
            for message in &snapshot.messages {
                let original:Value=message.metadata_json.as_ref().map(|raw|serde_json::from_str(raw)).transpose()?.unwrap_or(Value::Null);
                let mut meta=json!({"historicalReference":true,"forkSourceSessionId":source,"forkSourceMessageId":message.id,"forkSnapshotSha256":hash,"forkSourceMetadata":original});
                // Preserve passive image identities and compact history markers,
                // never reactivate steering, skills, task boundaries or grants.
                if let Some(images)=original.get("referencedImages") {meta["referencedImages"]=images.clone();}
                if let Some(subtype)=original.get("subtype").and_then(Value::as_str).filter(|kind|matches!(*kind,"compact_summary"|"compact_omission"|"COMPACT_SUMMARY"|"COMPACT_OMISSION"|"session_merge")) {meta["subtype"]=json!(if subtype=="session_merge" {"compact_summary"} else {subtype});}
                tx.execute("INSERT INTO messages(id,session_id,role,content_json,metadata_json,stop_reason,input_tokens,output_tokens,created_at,seq_num) VALUES(?1,?2,?3,?4,?5,?6,0,0,?7,?8)",params![uuid::Uuid::new_v4().to_string(),target,message.role,message.content_json,meta.to_string(),message.stop_reason,message.created_at,message.seq_num])?;
            }
            let result=SessionForkResult {session_id:target.clone(),source_session_id:source.clone(),snapshot_sha256:hash.clone(),message_count:snapshot.messages.len()};
            tx.execute("INSERT INTO session_forks(request_id,request_json,target_session_id,result_json,created_at) VALUES(?1,?2,?3,?4,?5)",params![request_id,request_json,target,serde_json::to_string(&result)?,now])?;
            tx.execute("INSERT INTO session_fork_snapshots(request_id,target_session_id,snapshot_json,snapshot_sha256) VALUES(?1,?2,?3,?4)",params![request_id,target,sealed,hash])?;
            tx.commit()?;
            Ok(result)
        }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NewMessage, model::ImageSource};

    async fn fixture() -> (Db, SessionForkRequest) {
        fixture_with_db(Db::open_in_memory().unwrap()).await
    }

    async fn fixture_with_db(db: Db) -> (Db, SessionForkRequest) {
        db.create_session_with_id("source", "gpt-5.4-mini", "/tmp/fork")
            .await
            .unwrap();
        db.set_session_permission_mode("source".into(), "DONT_ASK".into())
            .await
            .unwrap();
        let original = NewMessage {
            role: MessageRole::User,
            content: vec![
                StoredBlock::Text {
                    text: "original requirements".into(),
                },
                StoredBlock::Image {
                    source: ImageSource {
                        kind: "base64".into(),
                        media_type: Some("image/png".into()),
                        data: Some("sealed-original-image".into()),
                        url: None,
                    },
                    width: Some(2),
                    height: Some(3),
                },
            ],
            meta: Some(
                json!({"steering":true,"skillDirective":{"allowedTools":["Bash"]},"referencedImages":[{"sourceDigest":"original","payloadDigest":"payload"}]}),
            ),
            stop_reason: None,
            input_tokens: 12,
            output_tokens: 3,
        };
        db.append_message("source", original).await.unwrap();
        (
            db,
            SessionForkRequest {
                source_session_id: "source".into(),
                title: Some("Fork".into()),
            },
        )
    }

    #[tokio::test]
    async fn sealed_fork_survives_database_reopen_without_recopying_source() {
        let root = std::env::temp_dir().join(format!("zk-fork-reopen-{}", uuid::Uuid::new_v4()));
        let path = root.join("db.sqlite");
        let (db, request) = fixture_with_db(Db::open(&path).unwrap()).await;
        let original = db.get_session("source").await.unwrap().unwrap();
        let result = db
            .fork_session("durable-request", request.clone())
            .await
            .unwrap();
        let target_before = db.get_session(&result.session_id).await.unwrap().unwrap();
        db.set_session_permission_mode("source".into(), "AUTO_APPROVE".into())
            .await
            .unwrap();
        db.append_message(
            "source",
            NewMessage {
                role: MessageRole::User,
                content: vec![StoredBlock::Text {
                    text: "later source change".into(),
                }],
                meta: None,
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .unwrap();
        drop(db);

        let reopened = Db::open(&path).unwrap();
        assert_eq!(
            reopened
                .fork_session("durable-request", request)
                .await
                .unwrap(),
            result
        );
        let target = reopened
            .get_session(&result.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(target.messages.len(), 1);
        assert_eq!(target.messages[0].id, target_before.messages[0].id);
        assert_eq!(target.messages[0].content, original.messages[0].content);
        assert_eq!(target.messages[0].meta, target_before.messages[0].meta);
        assert_eq!(target.total_usage, zk_protocol::Usage::default());
        assert_eq!(target.total_cost_usd.to_bits(), 0.0_f64.to_bits());
        assert_eq!(
            reopened
                .list_sessions(None, 100)
                .await
                .unwrap()
                .sessions
                .len(),
            2
        );
        let target_id = result.session_id.clone();
        let (permission, snapshot, hash): (String, String, String) = reopened.with_conn_blocking(move |conn| {
            conn.query_row("SELECT s.permission_mode,f.snapshot_json,f.snapshot_sha256 FROM sessions s JOIN session_fork_snapshots f ON f.target_session_id=s.id WHERE s.id=?1", [target_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).map_err(Into::into)
        }).unwrap();
        assert_eq!(permission, "DONT_ASK");
        assert_eq!(hash, result.snapshot_sha256);
        assert_eq!(format!("{:x}", Sha256::digest(snapshot.as_bytes())), hash);
        assert!(!snapshot.contains("later source change"));
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn fork_preserves_sealed_history_and_permissions_but_not_execution_or_cost() {
        let (db, request) = fixture().await;
        let original = db.get_session("source").await.unwrap().unwrap();
        let result = db
            .fork_session("fork-request", request.clone())
            .await
            .unwrap();
        let target = db.get_session(&result.session_id).await.unwrap().unwrap();
        assert_ne!(target.session_id, original.session_id);
        assert_eq!(target.model, original.model);
        assert_eq!(target.working_dir, original.working_dir);
        assert_eq!(target.total_usage, zk_protocol::Usage::default());
        assert_eq!(target.total_cost_usd.to_bits(), 0.0_f64.to_bits());
        assert_eq!(target.messages.len(), original.messages.len());
        let message = &target.messages[0];
        assert_ne!(message.id, original.messages[0].id);
        assert_eq!(message.content, original.messages[0].content);
        assert_eq!((message.input_tokens, message.output_tokens), (0, 0));
        let meta = message.meta.as_ref().unwrap();
        assert_eq!(meta["historicalReference"], true);
        assert!(meta.get("skillDirective").is_none());
        assert!(meta.get("steering").is_none());
        assert_eq!(meta["referencedImages"][0]["sourceDigest"], "original");
        let id = result.session_id.clone();
        db.with_conn_blocking(move|conn| {
            let (mode,tasks,grants,calls):(String,i64,i64,i64)=conn.query_row("SELECT permission_mode,(SELECT COUNT(*) FROM tasks WHERE session_id=?1),(SELECT COUNT(*) FROM permission_grants WHERE root_session_id=?1),(SELECT COUNT(*) FROM messages WHERE session_id=?1 AND (task_id IS NOT NULL OR run_id IS NOT NULL OR source_task_id IS NOT NULL)) FROM sessions WHERE id=?1",[id],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)))?;
            assert_eq!((mode.as_str(),tasks,grants,calls),("DONT_ASK",0,0,0));
            Ok(())
        }).unwrap();
        assert_eq!(
            db.fork_session("fork-request", request.clone())
                .await
                .unwrap(),
            result
        );
        assert!(matches!(
            db.fork_session(
                "fork-request",
                SessionForkRequest {
                    title: Some("different".into()),
                    ..request
                }
            )
            .await,
            Err(DbError::Conflict(_))
        ));
        let snapshot: (String, String) = db
            .with_conn_blocking(|conn| {
                conn.query_row(
                    "SELECT snapshot_json,snapshot_sha256 FROM session_fork_snapshots",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(snapshot.0.as_bytes())),
            snapshot.1
        );
        assert_eq!(snapshot.1, result.snapshot_sha256);
    }

    #[tokio::test]
    async fn active_and_unclosed_sources_fail_without_any_target() {
        let (db, request) = fixture().await;
        db.start_run("running", "source", None, None, "gpt-5.4-mini")
            .await
            .unwrap();
        assert!(matches!(
            db.fork_session("busy", request.clone()).await,
            Err(DbError::Conflict(_))
        ));
        let (db, request) = fixture().await;
        db.append_message(
            "source",
            NewMessage {
                role: MessageRole::Assistant,
                content: vec![StoredBlock::ToolUse {
                    id: "call".into(),
                    name: "Read".into(),
                    input: json!({"path":"file"}),
                }],
                meta: None,
                stop_reason: Some("tool_use".into()),
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            db.fork_session("unclosed", request.clone()).await,
            Err(DbError::Conflict(_))
        ));
        assert_eq!(db.list_sessions(None, 100).await.unwrap().sessions.len(), 1);
        db.append_message(
            "source",
            NewMessage {
                role: MessageRole::User,
                content: vec![StoredBlock::ToolResult {
                    tool_use_id: "call".into(),
                    content: "data".into(),
                    is_error: false,
                    metadata: None,
                }],
                meta: None,
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            db.fork_session("closed", request)
                .await
                .unwrap()
                .message_count,
            3
        );
    }

    #[tokio::test]
    async fn atomic_failure_rolls_back_and_deleted_target_is_not_recreated() {
        let (db, request) = fixture().await;
        db.with_conn_blocking(|conn| {
            conn.execute_batch("CREATE TRIGGER reject_fork_message BEFORE INSERT ON messages WHEN NEW.session_id != 'source' BEGIN SELECT RAISE(ABORT,'fixture failure'); END;")?;Ok(())
        }).unwrap();
        assert!(db.fork_session("retry", request.clone()).await.is_err());
        assert_eq!(db.list_sessions(None, 100).await.unwrap().sessions.len(), 1);
        db.with_conn_blocking(|conn| {
            conn.execute_batch("DROP TRIGGER reject_fork_message;")?;
            Ok(())
        })
        .unwrap();
        let result = db.fork_session("retry", request.clone()).await.unwrap();
        let id = result.session_id.clone();
        db.with_conn_blocking(move |conn| {
            conn.execute("DELETE FROM sessions WHERE id=?1", [id])?;
            Ok(())
        })
        .unwrap();
        assert!(
            matches!(db.fork_session("retry",request).await,Err(DbError::Conflict(error)) if error=="FORK_TARGET_DELETED")
        );
        let count: i64 = db
            .with_conn_blocking(|conn| {
                conn.query_row("SELECT COUNT(*) FROM session_fork_snapshots", [], |r| {
                    r.get(0)
                })
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn concurrent_identical_requests_publish_one_target_and_ephemeral_sources_are_rejected() {
        let (db, request) = fixture().await;
        let (first, second) = tokio::join!(
            db.fork_session("same", request.clone()),
            db.fork_session("same", request.clone())
        );
        assert_eq!(first.unwrap(), second.unwrap());
        assert_eq!(db.list_sessions(None, 100).await.unwrap().sessions.len(), 2);
        db.with_conn_blocking(|conn| {
            conn.execute(
                "INSERT INTO sessions(id,model,working_dir,content_retention,created_at,updated_at)
                 VALUES('ephemeral','gpt-5.4-mini','/tmp/fork','ephemeral','now','now')",
                [],
            )?;
            Ok(())
        })
        .unwrap();
        assert!(
            db.fork_session(
                "ephemeral",
                SessionForkRequest {
                    source_session_id: "ephemeral".into(),
                    title: None,
                }
            )
            .await
            .is_err()
        );
        let forks: i64 = db
            .with_conn_blocking(|conn| {
                conn.query_row("SELECT COUNT(*) FROM session_forks", [], |row| row.get(0))
                    .map_err(Into::into)
            })
            .unwrap();
        assert_eq!(forks, 1);
    }

    #[tokio::test]
    async fn merged_source_summary_remains_history_without_copying_handoff_authority() {
        let (db, request) = fixture().await;
        db.with_conn_blocking(|conn| {
            conn.execute(
                "UPDATE sessions SET metadata_json=?1 WHERE id='source'",
                [json!({"mergeOperationId":"old-operation"}).to_string()],
            )?;
            Ok(())
        })
        .unwrap();
        db.append_message(
            "source",
            NewMessage {
                role: MessageRole::System,
                content: vec![StoredBlock::Text {
                    text: "sealed merge summary".into(),
                }],
                meta: Some(json!({"subtype":"session_merge","operationId":"old-operation"})),
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .unwrap();
        let result = db.fork_session("merged", request).await.unwrap();
        let target = db.get_session(&result.session_id).await.unwrap().unwrap();
        assert!(target.config.get("mergeOperationId").is_none());
        assert_eq!(
            target.messages[1].content,
            vec![StoredBlock::Text {
                text: "sealed merge summary".into()
            }]
        );
        assert_eq!(
            target.messages[1].meta.as_ref().unwrap()["subtype"],
            "compact_summary"
        );
        assert!(
            target.messages[1]
                .meta
                .as_ref()
                .unwrap()
                .get("operationId")
                .is_none()
        );
    }
}
