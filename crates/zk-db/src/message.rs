//! 消息域仓储——[`Db`] 的 messages 表读写（多文件 impl 之一）。
//!
//! 语义来源（旧仓库只读，2026-08-15 冻结）：
//! - `SessionManager.addMessage / addMessageWithId`（`seq_num` 子查询原子分配、
//!   写后 touch 会话 `updated_at`、INSERT OR IGNORE 幂等）
//! - `MessageRepository.findBySessionId / deleteAfterSeqNum`
//! - `SessionController.getMessages`（P0 索引游标：`Base64(十进制)`、
//!   limit 默认 50、无效游标回退 0）

use rusqlite::{Connection, OptionalExtension, params};

use crate::cursor::{decode_message_cursor, encode_message_cursor};
use crate::error::{DbError, map_fk_violation};
use crate::model::{
    MessagePage, MessageRecord, MessageRole, NewMessage, StoredBlock, parse_blocks,
};
use crate::time::{format_rfc3339_micros, now_millis, parse_rfc3339_millis};

/// 追加消息 SQL：`INSERT OR IGNORE`（主键幂等）+ `seq_num` 子查询原子分配 +
/// `RETURNING seq_num`（见 [`insert_message`]）。
const INSERT_MESSAGE_SQL: &str = "INSERT OR IGNORE INTO messages (
    id, session_id, role, content_json, stop_reason,
    input_tokens, output_tokens, task_id, run_id, origin, source_task_id, created_at, metadata_json, seq_num)
 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
    (SELECT COALESCE(MAX(seq_num), 0) + 1 FROM messages WHERE session_id = ?2))
 RETURNING seq_num";

/// Runtime ownership attached to a persisted message. Conversation messages use
/// [`Default::default`]; tool/task/runtime messages carry explicit Task/Run identity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MessageAttribution {
    /// Logical task that consumes this message.
    pub task_id: Option<String>,
    /// Run attempt that consumes or produced this message.
    pub run_id: Option<String>,
    /// `conversation`, `tool_result`, `task_result`, or `runtime`.
    pub origin: String,
    /// Producing child task for a parent-visible task result.
    pub source_task_id: Option<String>,
}

impl MessageAttribution {
    /// Attribution for a regular human/assistant/system conversation message.
    #[must_use]
    pub fn conversation() -> Self {
        Self {
            origin: "conversation".to_owned(),
            ..Self::default()
        }
    }
}

/// 事务内追加消息并 touch 会话（`append_message*` 的 blocking 主体）。
///
/// - `seq_num` 由 `SELECT COALESCE(MAX(seq_num), 0) + 1` 子查询在 INSERT 语句内
///   原子分配（对齐旧 `addMessage`；`UNIQUE(session_id, seq_num)` 兜底）；
/// - INSERT 与 touch 会话 `updated_at` 同事务（比旧系统两条独立 UPDATE 更紧）；
/// - 已存在 ID 仅在身份、正文、metadata、用量及归属完全相等时幂等跳过；
///   冲突失败且不 touch；JSON 对象键顺序不影响等价判定。
/// - 外键违例归一为 [`DbError::SessionNotFound`]（会话不存在）。
pub(crate) fn insert_message_in_current_write(
    conn: &Connection,
    message_id: &str,
    session_id: &str,
    msg: &NewMessage,
    attribution: &MessageAttribution,
) -> Result<Option<MessageRecord>, DbError> {
    let now_ms = now_millis();
    let now_iso = format_rfc3339_micros(now_ms);
    if conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM messages WHERE id=?1)",
        [message_id],
        |row| row.get::<_, bool>(0),
    )? {
        return if matches_existing_message(conn, message_id, session_id, msg, attribution)? {
            Ok(None)
        } else {
            Err(DbError::Conflict("MESSAGE_ID_CONFLICT".into()))
        };
    }
    let content_json =
        crate::content::store_text(conn, session_id, &serde_json::to_string(&msg.content)?)?;
    let metadata_json = crate::content::store_optional(
        conn,
        session_id,
        msg.meta
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?
            .as_deref(),
    )?;
    let seq_num: Option<i64> = conn
        .query_row(
            INSERT_MESSAGE_SQL,
            params![
                message_id,
                session_id,
                msg.role.as_str(),
                content_json,
                msg.stop_reason,
                msg.input_tokens,
                msg.output_tokens,
                attribution.task_id,
                attribution.run_id,
                if attribution.origin.is_empty() {
                    "conversation"
                } else {
                    attribution.origin.as_str()
                },
                attribution.source_task_id,
                now_iso,
                metadata_json
            ],
            |row| row.get(0),
        )
        .map(Some)
        .or_else(|err| match err {
            // INSERT OR IGNORE 命中重复主键时 query_row 报 QueryReturnedNoRows。
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            other => Err(map_fk_violation(session_id, other)),
        })?;
    let Some(seq_num) = seq_num else {
        if !matches_existing_message(conn, message_id, session_id, msg, attribution)? {
            return Err(DbError::Conflict("MESSAGE_ID_CONFLICT".to_owned()));
        }
        return Ok(None);
    };
    conn.execute(
        "UPDATE sessions SET updated_at = ?1 WHERE id = ?2",
        params![now_iso, session_id],
    )?;
    Ok(Some(MessageRecord {
        meta: msg.meta.clone(),
        id: message_id.to_owned(),
        session_id: session_id.to_owned(),
        role: msg.role,
        content: msg.content.clone(),
        stop_reason: msg.stop_reason.clone(),
        input_tokens: msg.input_tokens,
        output_tokens: msg.output_tokens,
        seq_num,
        created_at: now_ms,
    }))
}

fn matches_existing_message(
    conn: &Connection,
    message_id: &str,
    session_id: &str,
    message: &NewMessage,
    attribution: &MessageAttribution,
) -> Result<bool, DbError> {
    let content = serde_json::to_value(&message.content)?;
    let metadata = message.meta.clone().unwrap_or(serde_json::Value::Null);
    let origin = if attribution.origin.is_empty() {
        "conversation"
    } else {
        &attribution.origin
    };
    Ok(conn.query_row(
        "SELECT session_id,role,content_json,stop_reason,input_tokens,output_tokens,metadata_json,task_id,run_id,origin,source_task_id FROM messages WHERE id=?1",
        [message_id],
        |row| {
            let owner:String=row.get(0)?;
            let saved_content = serde_json::from_str::<serde_json::Value>(&crate::content::load_row_text(conn,&owner,row.get(2)?)?).ok();
            let saved_metadata = crate::content::load_optional(conn,&owner,row.get(6)?)?.map_or(Some(serde_json::Value::Null), |raw|serde_json::from_str(&raw).ok());
            Ok(row.get::<_,String>(0)? == session_id
                && row.get::<_,String>(1)? == message.role.as_str()
                && saved_content.as_ref() == Some(&content)
                && row.get::<_,Option<String>>(3)? == message.stop_reason
                && row.get::<_,i64>(4)? == message.input_tokens
                && row.get::<_,i64>(5)? == message.output_tokens
                && saved_metadata.as_ref() == Some(&metadata)
                && row.get::<_,Option<String>>(7)? == attribution.task_id
                && row.get::<_,Option<String>>(8)? == attribution.run_id
                && row.get::<_,String>(9)? == origin
                && row.get::<_,Option<String>>(10)? == attribution.source_task_id)
        },
    ).optional()?.unwrap_or(false))
}

fn insert_message(
    conn: &mut Connection,
    message_id: &str,
    session_id: &str,
    msg: &NewMessage,
    attribution: &MessageAttribution,
) -> Result<Option<MessageRecord>, DbError> {
    let tx = conn.transaction()?;
    let inserted = insert_message_in_current_write(&tx, message_id, session_id, msg, attribution)?;
    tx.commit()?;
    Ok(inserted)
}

/// 同步加载会话全量消息（`seq_num` 升序；供详情与消息分页复用）。
///
/// 未知 role 的行跳过（对齐 `mapRowToMessage` 的 catch→empty 语义）；
/// `content_json` 宽容解析见 [`parse_blocks`]。
pub(super) fn load_message_rows(
    conn: &Connection,
    session_id: &str,
) -> Result<Vec<MessageRecord>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, session_id, role, content_json, stop_reason,
                input_tokens, output_tokens, created_at, seq_num, metadata_json
         FROM messages WHERE session_id = ?1 ORDER BY seq_num ASC",
    )?;
    let rows = stmt
        .query_map(params![session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                crate::content::load_row_text(conn, &row.get::<_, String>(1)?, row.get(3)?)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, i64>(8)?,
                parse_metadata(crate::content::load_optional(
                    conn,
                    &row.get::<_, String>(1)?,
                    row.get(9)?,
                )?)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .filter_map(
            |(
                id,
                sid,
                role,
                content_json,
                stop_reason,
                in_tok,
                out_tok,
                created_iso,
                seq,
                meta,
            )| {
                let role = MessageRole::parse(&role)?;
                Some(MessageRecord {
                    meta,
                    id,
                    session_id: sid,
                    role,
                    content: parse_blocks(&content_json),
                    stop_reason,
                    input_tokens: in_tok,
                    output_tokens: out_tok,
                    seq_num: seq,
                    created_at: parse_rfc3339_millis(&created_iso).unwrap_or(0),
                })
            },
        )
        .collect())
}

/// Decorate display copies only; stored messages and model history stay immutable.
pub(super) fn project_runtime_diagnostics(
    conn: &Connection,
    session_id: &str,
    messages: &mut [MessageRecord],
) -> Result<(), DbError> {
    let mut stmt = conn.prepare(
        "SELECT r.id,r.task_id,r.status,r.exit_reason,r.error_summary,r.cleanup_status,
                (SELECT result.error_code FROM task_results result
                 WHERE result.run_id=r.id AND result.task_id=r.task_id
                 ORDER BY result.result_version DESC LIMIT 1)
         FROM messages m
         JOIN run_envelopes r ON r.id=m.run_id AND r.task_id=m.task_id
         JOIN tasks t ON t.id=r.task_id AND t.session_id=r.session_id
         WHERE m.id=?1 AND m.session_id=?2 AND r.session_id=?2
           AND m.role='system' AND m.origin='runtime' AND m.source_task_id IS NULL
           AND r.parent_run_id IS NULL AND t.parent_task_id IS NULL",
    )?;
    for message in messages {
        if message.session_id != session_id || message.role != MessageRole::System {
            continue;
        }
        let Some(meta) = message
            .meta
            .as_mut()
            .and_then(serde_json::Value::as_object_mut)
        else {
            continue;
        };
        // Never accept a diagnostic supplied in persisted/user-controlled metadata.
        meta.remove("runtimeDiagnostic");
        if meta.get("subtype").and_then(serde_json::Value::as_str) != Some("task_boundary")
            || meta
                .get("boundary_kind")
                .and_then(serde_json::Value::as_str)
                != Some("run")
        {
            continue;
        }
        let diagnostic = stmt
            .query_row(params![message.id, session_id], |row| {
                let status: String = row.get(2)?;
                let code = crate::content::load_diagnostic(conn, session_id, row.get(6)?)?;
                if !matches!(status.as_str(), "failed" | "cancelled" | "interrupted")
                    && code.as_deref() != Some("CLEANUP_UNCONFIRMED")
                {
                    return Ok(None);
                }
                let summary = crate::content::load_diagnostic(conn, session_id, row.get(4)?)?;
                Ok(Some(serde_json::json!({
                    "runId":row.get::<_,String>(0)?,
                    "taskId":row.get::<_,String>(1)?,
                    "status":status,
                    "exitReason":row.get::<_,Option<String>>(3)?,
                    "code":code,
                    "message":summary.map(|value|value.chars().take(4096).collect::<String>()),
                    "cleanupStatus":row.get::<_,String>(5)?,
                })))
            })
            .optional()?
            .flatten();
        if let Some(diagnostic) = diagnostic {
            meta.insert("runtimeDiagnostic".to_owned(), diagnostic);
        }
    }
    Ok(())
}

fn validate_attribution(
    conn: &Connection,
    session_id: &str,
    attribution: &MessageAttribution,
) -> Result<(), DbError> {
    if !matches!(
        attribution.origin.as_str(),
        "conversation" | "tool_result" | "task_result" | "runtime"
    ) {
        return Err(DbError::Invalid("MESSAGE_ORIGIN_INVALID".to_owned()));
    }
    if let Some(task_id) = attribution.task_id.as_deref() {
        let owned: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tasks t
         JOIN sessions s ON s.id=?1
         WHERE t.id=?2 AND (t.session_id=s.id OR s.parent_session_id=t.session_id)",
            params![session_id, task_id],
            |row| row.get(0),
        )?;
        if owned != 1 {
            return Err(DbError::Invalid("MESSAGE_TASK_NOT_OWNED".to_owned()));
        }
    }
    if let Some(run_id) = attribution.run_id.as_deref() {
        let owned: i64 = conn.query_row(
            "SELECT COUNT(*) FROM run_envelopes r
         WHERE r.id=?1 AND (?2 IS NULL OR r.task_id=?2)",
            params![run_id, attribution.task_id],
            |row| row.get(0),
        )?;
        if owned != 1 {
            return Err(DbError::Invalid("MESSAGE_RUN_NOT_OWNED".to_owned()));
        }
    }
    Ok(())
}

impl crate::Db {
    /// Attach authoritative root-run diagnostics to UI copies under one read snapshot.
    ///
    /// # Errors
    /// Query or content-ownership failures propagate; no message is persisted.
    pub async fn project_message_runtime_diagnostics(
        &self,
        session_id: &str,
        mut messages: Vec<MessageRecord>,
    ) -> Result<Vec<MessageRecord>, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| {
            let tx = conn.transaction()?;
            project_runtime_diagnostics(&tx, &session_id, &mut messages)?;
            tx.commit()?;
            Ok(messages)
        })
        .await
    }

    /// Idempotently queue a user instruction on its active root Run.
    /// # Errors
    /// Rejects changed payloads, cross-session ids and inactive/child Runs.
    pub async fn append_steering_message(
        &self,
        session_id: &str,
        run_id: &str,
        input_id: &str,
        text: &str,
        meta: Option<serde_json::Value>,
    ) -> Result<Option<MessageRecord>, DbError> {
        if input_id.trim().is_empty() || input_id.len() > 200 || text.trim().is_empty() {
            return Err(DbError::Validation(
                "steering requires a stable request id and nonblank text".into(),
            ));
        }
        let (session_id, run_id, message_id, text) = (
            session_id.to_owned(),
            run_id.to_owned(),
            format!("steering:{input_id}"),
            text.to_owned(),
        );
        let mut metadata = meta
            .and_then(|m| m.as_object().cloned())
            .unwrap_or_default();
        metadata.insert("steering".into(), serde_json::Value::Bool(true));
        let message = NewMessage {
            meta: Some(serde_json::Value::Object(metadata)),
            role: MessageRole::User,
            content: vec![StoredBlock::Text { text }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        };
        self.with_writer(move |conn| {
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let existing: Option<(String,String,String,Option<String>)> = tx.query_row("SELECT session_id,run_id,content_json,metadata_json FROM messages WHERE id=?1", [&message_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
            if let Some((owner, run, content, metadata)) = existing {
                let content=crate::content::load_text(&tx,&owner,&content)?;
                let metadata=crate::content::load_optional(&tx,&owner,metadata)?;
                if owner != session_id || run != run_id || parse_blocks(&content) != message.content || metadata.map(|raw|serde_json::from_str::<serde_json::Value>(&raw)).transpose()? != message.meta {
                    return Err(DbError::Conflict("steering request id already used with different ownership or payload".into()));
                }
                return Ok(None);
            }
            let task: Option<String> = tx.query_row("SELECT r.task_id FROM run_envelopes r JOIN tasks t ON t.id=r.task_id WHERE r.id=?1 AND r.session_id=?2 AND t.parent_task_id IS NULL AND r.status IN ('running','waitingDependencies','waitingInteraction')", params![run_id,session_id], |r|r.get(0)).optional()?;
            let task_id = task.ok_or_else(|| DbError::Conflict("steering requires an active root run".into()))?;
            let inserted = insert_message_in_current_write(&tx, &message_id, &session_id, &message, &MessageAttribution { task_id: Some(task_id), run_id: Some(run_id), origin: "conversation".into(), source_task_id: None })?;
            tx.commit()?;
            Ok(inserted)
        }).await
    }
    /// 追加消息（`POST resume` 后 WS 流程 / compact / export 的数据来源）。
    ///
    /// 生成 `UUIDv4` 主键；事务内分配 `seq_num` 并 touch 会话。
    ///
    /// # Errors
    ///
    /// 会话不存在返回 [`DbError::SessionNotFound`]；底层 `SQLite` 写入失败
    /// 返回 [`DbError::Sqlite`]。
    ///
    /// # Panics
    ///
    /// 全新 `UUIDv4` 撞上既有主键时 panic——仅在 UUID 生成器异常时可达
    /// （概率意义上不可能）。
    pub async fn append_message(
        &self,
        session_id: &str,
        msg: NewMessage,
    ) -> Result<MessageRecord, DbError> {
        let message_id = uuid::Uuid::new_v4().to_string();
        let record = self
            .append_message_with_id(&message_id, session_id, msg)
            .await?;
        // 全新 UUIDv4 不可能命中既有主键（概率意义上），OR IGNORE 不会触发。
        Ok(record.expect("fresh UUIDv4 cannot collide with an existing primary key"))
    }

    /// 幂等追加消息（对齐 `addMessageWithId`）：外部传入主键，
    /// `INSERT OR IGNORE` 保证重复写入静默跳过。
    ///
    /// 返回 `Ok(None)` = 主键已存在（未插入、未 touch）；
    /// `Ok(Some(record))` = 已插入。
    ///
    /// # Errors
    ///
    /// 会话不存在返回 [`DbError::SessionNotFound`]；底层 `SQLite` 写入失败
    /// 返回 [`DbError::Sqlite`]。
    pub async fn append_message_with_id(
        &self,
        message_id: &str,
        session_id: &str,
        msg: NewMessage,
    ) -> Result<Option<MessageRecord>, DbError> {
        let (message_id, session_id) = (message_id.to_owned(), session_id.to_owned());
        self.with_writer(move |conn| {
            insert_message(
                conn,
                &message_id,
                &session_id,
                &msg,
                &MessageAttribution::conversation(),
            )
        })
        .await
    }

    /// Append a message carrying structured Task/Run ownership.
    ///
    /// The repository verifies the referenced task and run belong to `session_id`'s root
    /// task tree before the insert, then persists attribution and session touch atomically.
    ///
    /// # Errors
    /// Returns [`DbError`] when ownership validation, JSON encoding, or the atomic insert fails.
    pub async fn append_attributed_message(
        &self,
        session_id: &str,
        msg: NewMessage,
        attribution: MessageAttribution,
    ) -> Result<MessageRecord, DbError> {
        let message_id = uuid::Uuid::new_v4().to_string();
        let message_id_for_insert = message_id.clone();
        let session_id = session_id.to_owned();
        let inserted = self
            .with_writer(move |conn| {
                validate_attribution(conn, &session_id, &attribution)?;
                insert_message(
                    conn,
                    &message_id_for_insert,
                    &session_id,
                    &msg,
                    &attribution,
                )
            })
            .await?;
        inserted.ok_or_else(|| DbError::Invalid(format!("MESSAGE_ID_COLLISION:{message_id}")))
    }

    /// Append one input batch atomically while preserving each user message boundary.
    /// # Errors
    /// Ownership, capacity, or any insert failure leaves the entire batch unchanged.
    pub async fn append_user_input_batch(
        &self,
        session_id: &str,
        messages: Vec<NewMessage>,
        attribution: MessageAttribution,
    ) -> Result<Vec<MessageRecord>, DbError> {
        if messages.is_empty()
            || messages.len() > 257
            || messages.iter().any(|msg| msg.role != MessageRole::User)
        {
            return Err(DbError::Validation("USER_INPUT_BATCH_INVALID".into()));
        }
        let session_id = session_id.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            validate_attribution(&tx, &session_id, &attribution)?;
            let mut records = Vec::with_capacity(messages.len());
            for message in messages {
                records.push(
                    insert_message_in_current_write(
                        &tx,
                        &uuid::Uuid::new_v4().to_string(),
                        &session_id,
                        &message,
                        &attribution,
                    )?
                    .ok_or_else(|| DbError::Invalid("MESSAGE_ID_COLLISION".into()))?,
                );
            }
            tx.commit()?;
            Ok(records)
        })
        .await
    }

    /// Ensure a successful Task/Run has one durable final Assistant message.
    ///
    /// Executors that already persisted their streamed Assistant output are left
    /// untouched. A non-streaming executor may return its final content directly to
    /// `TaskRuntime`; in that case this method persists that actual output before the
    /// immutable `TaskResult` is committed. The writer actor serializes the lookup and
    /// insert, preventing duplicate fallback messages for one execution driver.
    ///
    /// # Errors
    /// Returns [`DbError`] when the Task/Run identity is invalid or persistence fails.
    pub async fn ensure_task_final_assistant(
        &self,
        task_id: &str,
        run_id: &str,
        content: &str,
    ) -> Result<String, DbError> {
        let task_id = task_id.to_owned();
        let run_id = run_id.to_owned();
        let content = content.to_owned();
        self.with_writer(move |conn| {
            let session_id: String = conn
                .query_row(
                    "SELECT session_id FROM run_envelopes WHERE id=?1 AND task_id=?2",
                    params![run_id, task_id],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(|| DbError::Invalid("TASK_RUN_NOT_FOUND".to_owned()))?;
            if let Some((message_id, content_json)) = conn
                .query_row(
                    "SELECT id,content_json FROM messages
                     WHERE task_id=?1 AND run_id=?2 AND role='assistant'
                     ORDER BY seq_num DESC LIMIT 1",
                    params![task_id, run_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?
            {
                let content_json = crate::content::load_text(conn, &session_id, &content_json)?;
                let persisted = parse_blocks(&content_json)
                    .iter()
                    .filter_map(|block| match block {
                        StoredBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                if persisted != content {
                    return Err(DbError::Invalid(
                        "FINAL_ASSISTANT_CONTENT_MISMATCH".to_owned(),
                    ));
                }
                return Ok(message_id);
            }
            let message_id = uuid::Uuid::new_v4().to_string();
            let inserted = insert_message(
                conn,
                &message_id,
                &session_id,
                &NewMessage {
                    meta: None,
                    role: MessageRole::Assistant,
                    content: vec![StoredBlock::Text { text: content }],
                    stop_reason: Some("end_turn".to_owned()),
                    input_tokens: 0,
                    output_tokens: 0,
                },
                &MessageAttribution {
                    task_id: Some(task_id),
                    run_id: Some(run_id),
                    origin: "runtime".to_owned(),
                    source_task_id: None,
                },
            )?;
            inserted
                .map(|record| record.id)
                .ok_or_else(|| DbError::Invalid(format!("MESSAGE_ID_COLLISION:{message_id}")))
        })
        .await
    }

    /// 消息列表游标分页（`GET /api/sessions/{id}/messages` 数据源）。
    ///
    /// - 会话不存在 → `Ok(None)`（zk-server 映射 404，对齐
    ///   `getSessionOrThrow`）；
    /// - 游标 = `Base64(十进制索引)`（P0 语义：索引即 `seq` 升序偏移），
    ///   无效游标回退索引 0；
    /// - 越界索引（如 rewind 后游标失效）返回空页而非报错（SQL OFFSET
    ///   天然语义；旧系统此处抛 500）；
    /// - `limit+1` 探测 `has_more`；`next_cursor = Base64(offset + limit)`。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回 [`DbError::Sqlite`]。
    pub async fn list_messages(
        &self,
        session_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<Option<MessagePage>, DbError> {
        self.list_messages_inner(session_id, cursor, limit, false)
            .await
    }

    /// Read a UI page with root diagnostics, preserving its original cursor and size.
    ///
    /// # Errors
    /// Returns query or content-ownership errors without fabricating success.
    pub async fn list_messages_for_display(
        &self,
        session_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<Option<MessagePage>, DbError> {
        self.list_messages_inner(session_id, cursor, limit, true)
            .await
    }

    async fn list_messages_inner(
        &self,
        session_id: &str,
        cursor: Option<&str>,
        limit: u32,
        display: bool,
    ) -> Result<Option<MessagePage>, DbError> {
        let session_id = session_id.to_owned();
        let offset = cursor.and_then(decode_message_cursor).unwrap_or(0);
        if cursor.is_some() && cursor.map(decode_message_cursor) == Some(None) {
            tracing::warn!(cursor = ?cursor, "invalid message list cursor, restarting at 0");
        }
        let fetch = i64::from(limit) + 1;
        // u64（游标索引）→ i64（SQLite 参数）；越界游标饱和为 i64::MAX 即空页。
        let offset = i64::try_from(offset).unwrap_or(i64::MAX);
        self.with_reader(move |conn| {
            let tx = conn.transaction()?;
            let conn = &tx;
            let exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
                params![session_id],
                |row| row.get(0),
            )?;
            if !exists {
                return Ok(None);
            }
            let mut stmt = conn.prepare(
                "SELECT id, session_id, role, content_json, stop_reason,
                        input_tokens, output_tokens, created_at, seq_num, metadata_json
                 FROM messages WHERE session_id = ?1
                 ORDER BY seq_num ASC LIMIT ?2 OFFSET ?3",
            )?;
            let rows = stmt
                .query_map(params![session_id, fetch, offset], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        crate::content::load_row_text(
                            conn,
                            &row.get::<_, String>(1)?,
                            row.get(3)?,
                        )?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, i64>(8)?,
                        parse_metadata(crate::content::load_optional(
                            conn,
                            &row.get::<_, String>(1)?,
                            row.get(9)?,
                        )?)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let has_more = rows.len() > limit as usize;
            let mut messages: Vec<MessageRecord> = rows
                .into_iter()
                .take(limit as usize)
                .filter_map(
                    |(
                        id,
                        sid,
                        role,
                        content_json,
                        stop_reason,
                        in_tok,
                        out_tok,
                        created_iso,
                        seq,
                        meta,
                    )| {
                        let role = MessageRole::parse(&role)?;
                        Some(MessageRecord {
                            meta,
                            id,
                            session_id: sid,
                            role,
                            content: parse_blocks(&content_json),
                            stop_reason,
                            input_tokens: in_tok,
                            output_tokens: out_tok,
                            seq_num: seq,
                            created_at: parse_rfc3339_millis(&created_iso).unwrap_or(0),
                        })
                    },
                )
                .collect();
            drop(stmt);
            if display {
                project_runtime_diagnostics(conn, &session_id, &mut messages)?;
            }
            tx.commit()?;
            let next_cursor = has_more.then(|| {
                encode_message_cursor(u64::try_from(offset + i64::from(limit)).unwrap_or(u64::MAX))
            });
            Ok(Some(MessagePage {
                messages,
                has_more,
                next_cursor,
            }))
        })
        .await
    }

    /// 删除会话内指定序号**之后**的消息（rewind 语义；对齐
    /// `MessageRepository.deleteAfterSeqNum`，严格大于）。返回删除行数。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 写入失败时返回 [`DbError::Sqlite`]。
    pub async fn delete_messages_after(
        &self,
        session_id: &str,
        seq_num: i64,
    ) -> Result<u64, DbError> {
        let session_id = session_id.to_owned();
        self.with_writer(move |conn| {
            let rows = conn.execute(
                "DELETE FROM messages WHERE session_id = ?1 AND seq_num > ?2",
                params![session_id, seq_num],
            )?;
            Ok(u64::try_from(rows).unwrap_or(u64::MAX))
        })
        .await
    }

    /// 按主键查单条消息（快照 / 排障用）。不存在返回 `None`。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回 [`DbError::Sqlite`]。
    pub async fn get_message_by_id(
        &self,
        message_id: &str,
    ) -> Result<Option<MessageRecord>, DbError> {
        let message_id = message_id.to_owned();
        self.with_reader(move |conn| {
            let row = conn
                .query_row(
                    "SELECT id, session_id, role, content_json, stop_reason,
                            input_tokens, output_tokens, created_at, seq_num, metadata_json
                     FROM messages WHERE id = ?1",
                    params![message_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            crate::content::load_row_text(
                                conn,
                                &row.get::<_, String>(1)?,
                                row.get(3)?,
                            )?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, i64>(6)?,
                            row.get::<_, String>(7)?,
                            row.get::<_, i64>(8)?,
                            parse_metadata(crate::content::load_optional(
                                conn,
                                &row.get::<_, String>(1)?,
                                row.get(9)?,
                            )?)?,
                        ))
                    },
                )
                .optional()?;
            Ok(row.and_then(
                |(
                    id,
                    sid,
                    role,
                    content_json,
                    stop_reason,
                    in_tok,
                    out_tok,
                    created_iso,
                    seq,
                    meta,
                )| {
                    let role = MessageRole::parse(&role)?;
                    Some(MessageRecord {
                        meta,
                        id,
                        session_id: sid,
                        role,
                        content: parse_blocks(&content_json),
                        stop_reason,
                        input_tokens: in_tok,
                        output_tokens: out_tok,
                        seq_num: seq,
                        created_at: parse_rfc3339_millis(&created_iso).unwrap_or(0),
                    })
                },
            ))
        })
        .await
    }
}

fn parse_metadata(raw: Option<String>) -> rusqlite::Result<Option<serde_json::Value>> {
    raw.map(|raw| {
        serde_json::from_str(&raw).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                9,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    })
    .transpose()
}

#[cfg(test)]
mod display_tests {
    use super::*;
    use crate::{
        CleanupStatus, CommitTaskResult, CreateTaskWithRun, ResultStatus, VerificationStatus,
    };

    async fn failed_boundary(db: &crate::Db, session: &str) -> MessageRecord {
        let task = db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: uuid::Uuid::new_v4().to_string(),
                run_id: uuid::Uuid::new_v4().to_string(),
                root_session_id: session.to_owned(),
                transcript_session_id: session.to_owned(),
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: "diagnostic regression".into(),
                prompt: None,
                task_type: "agent".into(),
                model: "fixture".into(),
                working_dir: "/tmp".into(),
                execution_config_json: "{}".into(),
                startup_epoch: 1,
            })
            .await
            .unwrap();
        let message = db.append_attributed_message(session, NewMessage {
            meta: Some(serde_json::json!({"subtype":"task_boundary","boundary_kind":"run","task_id":"not-the-run-id"})),
            role: MessageRole::System,
            content: vec![StoredBlock::Text {text:"request".into()}],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        }, MessageAttribution {
            task_id: Some(task.task.id.clone()),
            run_id: Some(task.run_id.clone()),
            origin: "runtime".into(),
            source_task_id: None,
        }).await.unwrap();
        db.commit_task_result(&CommitTaskResult {
            task_id: task.task.id,
            run_id: task.run_id,
            expected_task_version: task.task.version,
            status: ResultStatus::Error,
            content: "provider rejected the request".into(),
            media_type: "text/plain".into(),
            error_code: Some("PROVIDER_FAILED".into()),
            cleanup_status: CleanupStatus::Confirmed,
            verification_status: VerificationStatus::NotRequested,
        })
        .await
        .unwrap();
        message
    }

    #[tokio::test]
    async fn restore_projects_failed_run_without_mutating_canonical_messages() {
        let db = crate::Db::open_in_memory().unwrap();
        db.create_session_with_id("display-failure", "fixture", "/tmp")
            .await
            .unwrap();
        let original = failed_boundary(&db, "display-failure").await;
        let restore = db
            .get_session_runtime_restore("display-failure")
            .await
            .unwrap()
            .unwrap();
        let diagnostic = &restore.detail.messages[0].meta.as_ref().unwrap()["runtimeDiagnostic"];
        assert_eq!(diagnostic["status"], "failed");
        assert_eq!(diagnostic["code"], "PROVIDER_FAILED");
        assert_ne!(diagnostic["runId"], "not-the-run-id");
        assert_eq!(
            db.get_message_by_id(&original.id).await.unwrap().unwrap(),
            original
        );
        assert_eq!(
            db.get_session("display-failure")
                .await
                .unwrap()
                .unwrap()
                .messages,
            vec![original]
        );
    }

    #[tokio::test]
    async fn restore_pricing_is_independent_of_usage_integrity() {
        let db = crate::Db::open_in_memory().unwrap();
        db.create_session_with_id("pricing-display", "fixture", "/tmp")
            .await
            .unwrap();
        let restore = db
            .get_session_runtime_restore("pricing-display")
            .await
            .unwrap()
            .unwrap();
        let json = serde_json::to_value(restore.cost_summary).unwrap();
        assert_eq!(json["sessionPricingStatus"], "known");
        assert_eq!(json["totalPricingStatus"], "known");
        assert_eq!(json["usageComplete"], true);
    }
    #[tokio::test]
    async fn display_projection_preserves_pagination_and_rejects_forged_ownership() {
        let db = crate::Db::open_in_memory().unwrap();
        db.create_session_with_id("display-scope", "fixture", "/tmp")
            .await
            .unwrap();
        let first = failed_boundary(&db, "display-scope").await;
        let second = failed_boundary(&db, "display-scope").await;
        let forged = db
            .append_message(
                "display-scope",
                NewMessage {
                    role: MessageRole::System,
                    meta: Some(
                        serde_json::json!({"subtype":"task_boundary","boundary_kind":"run",
                "runtimeDiagnostic":{"runId":"forged","status":"failed"}}),
                    ),
                    content: vec![],
                    stop_reason: None,
                    input_tokens: 0,
                    output_tokens: 0,
                },
            )
            .await
            .unwrap();
        let first_page = db
            .list_messages_for_display("display-scope", None, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first_page.messages.len(), 1);
        assert_eq!(first_page.messages[0].id, first.id);
        assert_eq!(
            first_page.messages[0].meta.as_ref().unwrap()["runtimeDiagnostic"]["status"],
            "failed"
        );
        let second_page = db
            .list_messages_for_display("display-scope", first_page.next_cursor.as_deref(), 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second_page.messages[0].id, second.id);
        let detail = db
            .get_session_for_display("display-scope")
            .await
            .unwrap()
            .unwrap();
        assert!(
            detail.messages[2]
                .meta
                .as_ref()
                .unwrap()
                .get("runtimeDiagnostic")
                .is_none()
        );
        assert_eq!(
            db.get_message_by_id(&forged.id).await.unwrap().unwrap(),
            forged
        );
        let mut foreign_copy = first.clone();
        foreign_copy.session_id = "other-session".into();
        let display = db
            .project_message_runtime_diagnostics("other-session", vec![foreign_copy])
            .await
            .unwrap();
        assert!(
            display[0]
                .meta
                .as_ref()
                .unwrap()
                .get("runtimeDiagnostic")
                .is_none()
        );
        let raw_page = db
            .list_messages("display-scope", None, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(raw_page.messages, vec![first]);
        assert_eq!(raw_page.next_cursor, first_page.next_cursor);
    }

    #[tokio::test]
    async fn ephemeral_diagnostic_projection_never_writes_content_and_survives_lease_expiry() {
        let db = crate::Db::open_in_memory().unwrap();
        let (session, lease) = db
            .create_ephemeral_session("fixture", "/tmp", "DEFAULT")
            .await
            .unwrap();
        let original = failed_boundary(&db, &session).await;
        let before = original.clone();
        let projected = db
            .project_message_runtime_diagnostics(&session, vec![original.clone()])
            .await
            .unwrap();
        assert_eq!(
            projected[0].meta.as_ref().unwrap()["runtimeDiagnostic"]["status"],
            "failed"
        );
        drop(lease);
        let expired = db
            .project_message_runtime_diagnostics(&session, vec![original])
            .await
            .unwrap();
        assert_eq!(
            expired[0].meta.as_ref().unwrap()["runtimeDiagnostic"]["message"],
            "EPHEMERAL_CONTENT_UNAVAILABLE"
        );
        assert_eq!(expired[0].content, before.content);
        db.with_reader(move |conn| {
            let (count,contains_text):(i64,bool)=conn.query_row(
                "SELECT COUNT(*),COALESCE(MAX(content_json LIKE '%request%' OR metadata_json LIKE '%runtimeDiagnostic%'),0) FROM messages WHERE session_id=?1",
                [session],|r|Ok((r.get(0)?,r.get(1)?)))?;
            assert_eq!(count,1);
            assert!(!contains_text);
            Ok(())
        }).await.unwrap();
    }

    #[tokio::test]
    async fn pricing_status_counts_finished_unknown_costs_without_changing_usage_or_subtotals() {
        let db = crate::Db::open_in_memory().unwrap();
        db.create_session_with_id("pricing-calls", "fixture", "/tmp")
            .await
            .unwrap();
        db.create_session_with_id("pricing-other", "fixture", "/tmp")
            .await
            .unwrap();
        let message = failed_boundary(&db, "pricing-calls").await;
        db.with_writer(move |conn| {
            conn.execute("INSERT INTO llm_calls(call_id,task_id,run_id,provider,model,status,input_tokens,output_tokens,cache_read_tokens,cache_create_tokens,cost_nanos_usd,usage_complete,started_at,finished_at,created_at,updated_at)
                SELECT 'unknown-call',m.task_id,m.run_id,'fixture','unknown','completed',12,3,0,0,NULL,1,'now','now','now','now' FROM messages m WHERE m.id=?1",[message.id])?;
            Ok(())
        }).await.unwrap();
        assert_eq!(
            db.get_pricing_status("pricing-calls").await.unwrap(),
            (true, true)
        );
        assert_eq!(
            db.get_pricing_status("pricing-other").await.unwrap(),
            (false, true)
        );
        let restore = db
            .get_session_runtime_restore("pricing-calls")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restore.cost_summary.session_pricing_status, "unknown");
        assert_eq!(restore.cost_summary.total_pricing_status, "unknown");
        assert!(restore.cost_summary.usage_complete);
        assert!(restore.cost_summary.session_cost.abs() < f64::EPSILON);
        db.with_writer(|conn| {
            conn.execute("UPDATE llm_calls SET status='started',finished_at=NULL WHERE call_id='unknown-call'",[])?;
            Ok(())
        }).await.unwrap();
        assert_eq!(
            db.get_pricing_status("pricing-calls").await.unwrap(),
            (false, false)
        );
    }
}
