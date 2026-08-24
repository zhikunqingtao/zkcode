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
use crate::model::{MessagePage, MessageRecord, MessageRole, NewMessage, parse_blocks};
use crate::time::{format_rfc3339_micros, now_millis, parse_rfc3339_millis};

/// 追加消息 SQL：`INSERT OR IGNORE`（主键幂等）+ `seq_num` 子查询原子分配 +
/// `RETURNING seq_num`（见 [`insert_message`]）。
const INSERT_MESSAGE_SQL: &str = "INSERT OR IGNORE INTO messages (
    id, session_id, role, content_json, stop_reason,
    input_tokens, output_tokens, created_at, seq_num)
 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
    (SELECT COALESCE(MAX(seq_num), 0) + 1 FROM messages WHERE session_id = ?2))
 RETURNING seq_num";

/// 事务内追加消息并 touch 会话（`append_message*` 的 blocking 主体）。
///
/// - `seq_num` 由 `SELECT COALESCE(MAX(seq_num), 0) + 1` 子查询在 INSERT 语句内
///   原子分配（对齐旧 `addMessage`；`UNIQUE(session_id, seq_num)` 兜底）；
/// - INSERT 与 touch 会话 `updated_at` 同事务（比旧系统两条独立 UPDATE 更紧）；
/// - `INSERT OR IGNORE`：主键已存在时静默跳过且**不** touch（对齐旧
///   `addMessageWithId` 的 `rows > 0` 判定；`append_message` 的全新 UUID
///   不会触发该分支）；
/// - 外键违例归一为 [`DbError::SessionNotFound`]（会话不存在）。
fn insert_message(
    conn: &mut Connection,
    message_id: &str,
    session_id: &str,
    msg: &NewMessage,
) -> Result<Option<MessageRecord>, DbError> {
    let now_ms = now_millis();
    let now_iso = format_rfc3339_micros(now_ms);
    let content_json = serde_json::to_string(&msg.content)?;
    let tx = conn.transaction()?;
    let seq_num: Option<i64> = tx
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
                now_iso
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
        tx.commit()?;
        return Ok(None);
    };
    tx.execute(
        "UPDATE sessions SET updated_at = ?1 WHERE id = ?2",
        params![now_iso, session_id],
    )?;
    tx.commit()?;
    Ok(Some(MessageRecord {
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

/// 同步加载会话全量消息（`seq_num` 升序；供详情与消息分页复用）。
///
/// 未知 role 的行跳过（对齐 `mapRowToMessage` 的 catch→empty 语义）；
/// `content_json` 宽容解析见 [`parse_blocks`]。
pub(super) fn load_message_rows(
    conn: &mut Connection,
    session_id: &str,
) -> Result<Vec<MessageRecord>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, session_id, role, content_json, stop_reason,
                input_tokens, output_tokens, created_at, seq_num
         FROM messages WHERE session_id = ?1 ORDER BY seq_num ASC",
    )?;
    let rows = stmt
        .query_map(params![session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, i64>(8)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .filter_map(
            |(id, sid, role, content_json, stop_reason, in_tok, out_tok, created_iso, seq)| {
                let role = MessageRole::parse(&role)?;
                Some(MessageRecord {
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

impl crate::Db {
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
        self.with_writer(move |conn| insert_message(conn, &message_id, &session_id, &msg))
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
                        input_tokens, output_tokens, created_at, seq_num
                 FROM messages WHERE session_id = ?1
                 ORDER BY seq_num ASC LIMIT ?2 OFFSET ?3",
            )?;
            let rows = stmt
                .query_map(params![session_id, fetch, offset], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, i64>(8)?,
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
                    )| {
                        let role = MessageRole::parse(&role)?;
                        Some(MessageRecord {
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
                            input_tokens, output_tokens, created_at, seq_num
                     FROM messages WHERE id = ?1",
                    params![message_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, i64>(6)?,
                            row.get::<_, String>(7)?,
                            row.get::<_, i64>(8)?,
                        ))
                    },
                )
                .optional()?;
            Ok(row.and_then(
                |(id, sid, role, content_json, stop_reason, in_tok, out_tok, created_iso, seq)| {
                    let role = MessageRole::parse(&role)?;
                    Some(MessageRecord {
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
