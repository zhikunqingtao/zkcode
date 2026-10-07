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
        let session_id = session_id.to_owned();
        let offset = cursor.and_then(decode_message_cursor).unwrap_or(0);
        if cursor.is_some() && cursor.map(decode_message_cursor) == Some(None) {
            tracing::warn!(cursor = ?cursor, "invalid message list cursor, restarting at 0");
        }
        let fetch = i64::from(limit) + 1;
        // u64（游标索引）→ i64（SQLite 参数）；越界游标饱和为 i64::MAX 即空页。
        let offset = i64::try_from(offset).unwrap_or(i64::MAX);
        self.with_reader(move |conn| {
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
            let messages: Vec<MessageRecord> = rows
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
