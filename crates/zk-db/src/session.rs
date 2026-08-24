//! 会话域仓储——[`Db`] 的 sessions 表读写（多文件 impl 之一）。
//!
//! 语义来源（旧仓库只读，2026-08-15 冻结）：
//! - `SessionRepository.create/findById/listAll/updateTitle/updateUsage/
//!   updateStatus/delete`
//! - `SessionManager.listSessionsPaginated`（游标锚点 + limit+1 探测 +
//!   subagent 过滤）、`loadSession`（详情含全量消息与 metadata 解析）
//! - `SessionController.listSessions`（无效游标回退、nextCursor 编码）

use rusqlite::{Connection, OptionalExtension, params};

use zk_protocol::model::Usage;

use crate::cursor::{decode_session_cursor, encode_session_cursor};
use crate::error::DbError;
use crate::model::{SessionDetail, SessionPage, SessionSummary, goal_preview};
use crate::time::{format_rfc3339_micros, now_millis, parse_rfc3339_millis};

/// 单事务快照恢复结果。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotRestoreOutcome {
    /// 会话与消息已整体替换。
    Applied,
    /// 目标会话不存在。
    NotFound,
    /// 快照声明的工作区与数据库中的授权工作区不一致。
    WorkspaceMismatch,
}

/// 摘要行 SELECT（子查询与过滤条件逐字照抄旧 `listSessions` SQL）。
const SUMMARY_SELECT: &str = r#"
    SELECT s.id, s.title, s.model, s.working_dir, s.total_cost_usd,
           s.created_at, s.updated_at,
           (SELECT m.content_json FROM messages m
            WHERE m.session_id = s.id AND m.role = 'user'
            ORDER BY m.seq_num ASC LIMIT 1) AS first_user_content,
           (SELECT COUNT(*) FROM messages m WHERE m.session_id = s.id) AS message_count
    FROM sessions s
    WHERE (s.metadata_json IS NULL
           OR s.metadata_json NOT LIKE '%"type":"subagent"%')
"#;

/// 库内 RFC 3339 → epoch 毫秒（旧库畸形时间戳兜底 0，不阻断读取）。
fn iso_to_millis(iso: &str) -> i64 {
    parse_rfc3339_millis(iso).unwrap_or(0)
}

/// 摘要行映射（含游标编码所需的 `updated_at` ISO 原文）。
fn map_summary_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(SessionSummary, String)> {
    let id: String = row.get("id")?;
    let title: Option<String> = row.get("title")?;
    let model: String = row.get("model")?;
    let working_dir: String = row.get("working_dir")?;
    let cost_usd: f64 = row.get("total_cost_usd")?;
    let created_iso: String = row.get("created_at")?;
    let updated_iso: String = row.get("updated_at")?;
    let first_user_content: Option<String> = row.get("first_user_content")?;
    let message_count: i64 = row.get("message_count")?;
    Ok((
        SessionSummary {
            id,
            title,
            goal_preview: goal_preview(first_user_content.as_deref()),
            model,
            working_directory: working_dir,
            message_count,
            cost_usd,
            created_at: iso_to_millis(&created_iso),
            updated_at: iso_to_millis(&updated_iso),
        },
        updated_iso,
    ))
}

/// 无锚点查询：从最新开始取 `fetch` 条（首屏与锚点失效回退共用）。
fn query_from_latest(
    conn: &mut Connection,
    fetch: i64,
) -> Result<Vec<(SessionSummary, String)>, DbError> {
    let sql = format!("{SUMMARY_SELECT} ORDER BY s.updated_at DESC LIMIT ?1");
    let mut stmt = conn.prepare(&sql)?;
    Ok(stmt
        .query_map(params![fetch], map_summary_row)?
        .collect::<rusqlite::Result<_>>()?)
}

impl crate::Db {
    /// 创建会话（`POST /api/sessions` 数据源；对齐 `SessionRepository.create`）。
    ///
    /// 生成 `UUIDv4` 主键，`status='active'`，`created_at = updated_at = now`；
    /// 返回完整摘要（`message_count=0`、无预览）。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 写入失败（IO / 磁盘满等）时返回 [`DbError::Sqlite`]。
    pub async fn create_session(
        &self,
        model: &str,
        working_dir: &str,
    ) -> Result<SessionSummary, DbError> {
        let id = uuid::Uuid::new_v4().to_string();
        self.create_session_with_id(&id, model, working_dir).await
    }

    /// 创建具有调用方指定 ID 的会话。仅供需要先生成稳定关联键的内部编排器
    /// （例如子 Agent）使用；重复 ID 按 `SQLite` 唯一约束失败关闭。
    ///
    /// # Errors
    /// 底层 `SQLite` 写入失败或 `session_id` 重复时返回 [`DbError`]。
    pub async fn create_session_with_id(
        &self,
        session_id: &str,
        model: &str,
        working_dir: &str,
    ) -> Result<SessionSummary, DbError> {
        let id = session_id.to_owned();
        let model = model.to_owned();
        let working_dir = working_dir.to_owned();
        self.with_writer(move |conn| {
            let now_iso = format_rfc3339_micros(now_millis());
            conn.execute(
                "INSERT INTO sessions (id, model, working_dir, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, 'active', ?4, ?4)",
                params![id, model, working_dir, now_iso],
            )?;
            let now_ms = iso_to_millis(&now_iso);
            Ok(SessionSummary {
                title: None,
                goal_preview: None,
                message_count: 0,
                cost_usd: 0.0,
                created_at: now_ms,
                updated_at: now_ms,
                id,
                model,
                working_directory: working_dir,
            })
        })
        .await
    }

    /// 会话列表游标分页（`GET /api/sessions` 数据源）。
    ///
    /// - 无游标 / 游标无效 / 游标锚点会话已被删 → 从最新开始（旧系统对
    ///   前两者 log.warn 后回退；对第三者抛 500，此处统一回退，更稳）；
    /// - 有游标 → 取锚点会话 `updated_at`，查严格更早的 `limit+1` 条；
    /// - `has_more` = 探测多出的 1 条；`next_cursor` 由本页末条编码生成
    ///   （`Base64("updated_at|id")`，与 `SessionController` 逐字一致）。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回 [`DbError::Sqlite`]。
    ///
    /// # Panics
    ///
    /// `has_more` 为真时本页必非空，末行 `expect` 不会触发；若内部逻辑
    /// 失配则 panic（属程序缺陷）。
    pub async fn list_sessions(
        &self,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<SessionPage, DbError> {
        let before_id = cursor
            .map(String::from)
            .and_then(|c| decode_session_cursor(&c));
        if cursor.is_some() && before_id.is_none() {
            tracing::warn!(cursor = ?cursor, "invalid session list cursor, anchoring to latest");
        }
        let fetch = i64::from(limit) + 1;
        self.with_reader(move |conn| {
            let mut rows: Vec<(SessionSummary, String)> = if let Some(id) = before_id.as_deref()
            {
                let anchor_time: Option<String> = conn
                    .query_row(
                        "SELECT updated_at FROM sessions WHERE id = ?1",
                        params![id],
                        |row| row.get(0),
                    )
                    .optional()?;
                if let Some(anchor_time) = anchor_time {
                    let sql = format!(
                        "{SUMMARY_SELECT} AND s.updated_at < ?1
                         ORDER BY s.updated_at DESC LIMIT ?2"
                    );
                    let mut stmt = conn.prepare(&sql)?;
                    stmt.query_map(params![anchor_time, fetch], map_summary_row)?
                        .collect::<rusqlite::Result<_>>()?
                } else {
                    // 锚点会话已删除：旧系统此处抛空结果异常（500），
                    // 回退「从最新开始」对翻页客户端更友好。
                    tracing::warn!(session_id = %id, "cursor anchor session gone, anchoring to latest");
                    query_from_latest(conn, fetch)?
                }
            } else {
                query_from_latest(conn, fetch)?
            };
            let has_more = rows.len() > limit as usize;
            rows.truncate(limit as usize);
            let next_cursor = if has_more && !rows.is_empty() {
                let (last, updated_iso) = rows.last().expect("checked non-empty");
                Some(encode_session_cursor(updated_iso, &last.id))
            } else {
                None
            };
            Ok(SessionPage {
                sessions: rows.into_iter().map(|(s, _)| s).collect(),
                has_more,
                next_cursor,
            })
        })
        .await
    }

    /// 加载完整会话（`GET /api/sessions/{id}` 详情 / `resume` / `export`
    /// 数据源；对齐 `SessionManager.loadSession`）。
    ///
    /// 不存在返回 `None`（zk-server 映射 404）。消息按 `seq_num` 升序全量
    /// 加载；`metadata_json` 损坏时回退空 map（对齐 `parseJsonMap`）。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回 [`DbError::Sqlite`]。
    pub async fn get_session(&self, session_id: &str) -> Result<Option<SessionDetail>, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| load_session_detail(conn, &session_id))
            .await
    }

    /// 更新会话标题（对齐 `SessionRepository.updateTitle`，同步 touch
    /// `updated_at`）。返回是否存在（false = 无此会话，0 行受影响）。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 写入失败时返回 [`DbError::Sqlite`]。
    pub async fn update_session_title(
        &self,
        session_id: &str,
        title: &str,
    ) -> Result<bool, DbError> {
        let (session_id, title) = (session_id.to_owned(), title.to_owned());
        self.with_writer(move |conn| set_session_column(conn, &session_id, "title", &title))
            .await
    }

    /// 更新会话默认模型（WS `set_model` 落库位；对齐旧
    /// `SessionManager.updateSessionModel`）。返回是否存在。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 写入失败时返回 [`DbError::Sqlite`]。
    pub async fn update_session_model(
        &self,
        session_id: &str,
        model: &str,
    ) -> Result<bool, DbError> {
        let (session_id, model) = (session_id.to_owned(), model.to_owned());
        self.with_writer(move |conn| set_session_column(conn, &session_id, "model", &model))
            .await
    }

    /// 更新会话状态（`active` / `closed`…小写存储；对齐
    /// `SessionRepository.updateStatus`）。返回是否存在。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 写入失败时返回 [`DbError::Sqlite`]。
    pub async fn update_session_status(
        &self,
        session_id: &str,
        status: &str,
    ) -> Result<bool, DbError> {
        let (session_id, status) = (session_id.to_owned(), status.to_owned());
        self.with_writer(move |conn| set_session_column(conn, &session_id, "status", &status))
            .await
    }

    /// 追加累计 token 用量与成本（增量 UPDATE，对齐
    /// `SessionRepository.updateUsage` 的 `col = col + ?` 形式）。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 写入失败时返回 [`DbError::Sqlite`]。
    pub async fn add_session_usage(
        &self,
        session_id: &str,
        delta: &Usage,
        cost_usd_delta: f64,
    ) -> Result<bool, DbError> {
        let session_id = session_id.to_owned();
        let delta = *delta;
        self.with_writer(move |conn| {
            let now_iso = format_rfc3339_micros(now_millis());
            let rows = conn.execute(
                "UPDATE sessions SET
                    total_input_tokens  = total_input_tokens  + ?1,
                    total_output_tokens = total_output_tokens + ?2,
                    total_cache_read    = total_cache_read    + ?3,
                    total_cache_create  = total_cache_create  + ?4,
                    total_cost_usd      = total_cost_usd      + ?5,
                    updated_at = ?6
                 WHERE id = ?7",
                params![
                    delta.input_tokens,
                    delta.output_tokens,
                    delta.cache_read_input_tokens,
                    delta.cache_creation_input_tokens,
                    cost_usd_delta,
                    now_iso,
                    session_id
                ],
            )?;
            Ok(rows > 0)
        })
        .await
    }

    /// 更新上下文压缩摘要（compact 落库位；`summary` 列 + touch）。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 写入失败时返回 [`DbError::Sqlite`]。
    pub async fn update_session_summary(
        &self,
        session_id: &str,
        summary: &str,
    ) -> Result<bool, DbError> {
        let (session_id, summary) = (session_id.to_owned(), summary.to_owned());
        self.with_writer(move |conn| set_session_column(conn, &session_id, "summary", &summary))
            .await
    }

    /// 删除会话（`DELETE /api/sessions/{id}` 数据源）。
    ///
    /// 消息经 `ON DELETE CASCADE` 级联清除（依赖连接期
    /// `PRAGMA foreign_keys=ON`，见 `Db::init`）。返回是否存在。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 写入失败时返回 [`DbError::Sqlite`]。
    pub async fn delete_session(&self, session_id: &str) -> Result<bool, DbError> {
        let session_id = session_id.to_owned();
        self.with_writer(move |conn| {
            let rows = conn.execute("DELETE FROM sessions WHERE id = ?1", params![session_id])?;
            Ok(rows > 0)
        })
        .await
    }

    /// 在一个 `SQLite` 写事务中恢复会话元数据与完整消息序列。
    ///
    /// 重复调用会先删除再插入同一组稳定消息 ID，不产生重复消息；任一消息
    /// 序列化/约束失败时整个事务回滚。工作区在事务内复检，避免校验与写入间
    /// 被替换。
    ///
    /// # Errors
    /// 会话不存在、工作区不匹配、消息约束/序列化失败或事务写入失败时返回
    /// [`DbError`]。
    #[allow(clippy::too_many_arguments)]
    pub async fn restore_session_snapshot(
        &self,
        session_id: &str,
        expected_working_dir: &str,
        model: &str,
        status: &str,
        title: Option<&str>,
        input_tokens: i64,
        output_tokens: i64,
        cost_usd: f64,
        messages: Vec<crate::MessageRecord>,
    ) -> Result<SnapshotRestoreOutcome, DbError> {
        let session_id = session_id.to_owned();
        let expected_working_dir = expected_working_dir.to_owned();
        let model = model.to_owned();
        let status = status.to_owned();
        let title = title.map(str::to_owned);
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let working_dir: Option<String> = tx
                .query_row(
                    "SELECT working_dir FROM sessions WHERE id = ?1",
                    params![&session_id],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(working_dir) = working_dir else {
                return Ok(SnapshotRestoreOutcome::NotFound);
            };
            if working_dir != expected_working_dir {
                return Ok(SnapshotRestoreOutcome::WorkspaceMismatch);
            }
            if messages
                .iter()
                .any(|message| message.session_id != session_id)
            {
                return Err(DbError::Invalid(
                    "snapshot message session mismatch".to_owned(),
                ));
            }

            tx.execute(
                "DELETE FROM messages WHERE session_id = ?1",
                params![&session_id],
            )?;
            for message in messages {
                let content_json = serde_json::to_string(&message.content)?;
                tx.execute(
                    "INSERT INTO messages (
                        id, session_id, role, content_json, stop_reason,
                        input_tokens, output_tokens, created_at, seq_num
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        message.id,
                        &session_id,
                        message.role.as_str(),
                        content_json,
                        message.stop_reason,
                        message.input_tokens,
                        message.output_tokens,
                        format_rfc3339_micros(message.created_at),
                        message.seq_num,
                    ],
                )?;
            }
            let now = format_rfc3339_micros(now_millis());
            tx.execute(
                "UPDATE sessions SET title = ?1, model = ?2, status = ?3,
                    total_input_tokens = ?4, total_output_tokens = ?5,
                    total_cache_read = 0, total_cache_create = 0,
                    total_cost_usd = ?6, updated_at = ?7
                 WHERE id = ?8",
                params![
                    title,
                    model,
                    status,
                    input_tokens,
                    output_tokens,
                    cost_usd,
                    now,
                    &session_id,
                ],
            )?;
            tx.commit()?;
            Ok(SnapshotRestoreOutcome::Applied)
        })
        .await
    }
}

/// touch 式单列 UPDATE：`SET <col> = ?, updated_at = ? WHERE id = ?`。
///
/// 列名来自本文件硬编码调用点（title / status / summary / model），不接触用户输入，
/// 无注入面。
fn set_session_column(
    conn: &mut Connection,
    session_id: &str,
    column: &'static str,
    value: &str,
) -> Result<bool, DbError> {
    let now_iso = format_rfc3339_micros(now_millis());
    let sql = format!("UPDATE sessions SET {column} = ?1, updated_at = ?2 WHERE id = ?3");
    let rows = conn.execute(&sql, params![value, now_iso, session_id])?;
    Ok(rows > 0)
}

/// 同步加载会话详情（`get_session` 的 blocking 主体，亦供消息域复用）。
fn load_session_detail(
    conn: &mut Connection,
    session_id: &str,
) -> Result<Option<SessionDetail>, DbError> {
    let row = conn
        .query_row(
            "SELECT id, model, working_dir, title, status, summary, metadata_json,
                    total_input_tokens, total_output_tokens, total_cache_read,
                    total_cache_create, total_cost_usd, created_at, updated_at
             FROM sessions WHERE id = ?1",
            params![session_id],
            |row| {
                Ok((
                    row.get::<_, String>("id")?,
                    row.get::<_, String>("model")?,
                    row.get::<_, String>("working_dir")?,
                    row.get::<_, Option<String>>("title")?,
                    row.get::<_, String>("status")?,
                    row.get::<_, Option<String>>("summary")?,
                    row.get::<_, Option<String>>("metadata_json")?,
                    row.get::<_, i64>("total_input_tokens")?,
                    row.get::<_, i64>("total_output_tokens")?,
                    row.get::<_, i64>("total_cache_read")?,
                    row.get::<_, i64>("total_cache_create")?,
                    row.get::<_, f64>("total_cost_usd")?,
                    row.get::<_, String>("created_at")?,
                    row.get::<_, String>("updated_at")?,
                ))
            },
        )
        .optional()?;
    let Some((
        id,
        model,
        working_dir,
        title,
        status,
        summary,
        metadata_json,
        in_tok,
        out_tok,
        cache_read,
        cache_create,
        cost_usd,
        created_iso,
        updated_iso,
    )) = row
    else {
        return Ok(None);
    };
    let messages = crate::message::load_message_rows(conn, &id)?;
    let config = metadata_json
        .as_deref()
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default();
    Ok(Some(SessionDetail {
        session_id: id,
        model,
        working_dir,
        title,
        status,
        messages,
        config,
        total_usage: Usage {
            input_tokens: in_tok,
            output_tokens: out_tok,
            cache_read_input_tokens: cache_read,
            cache_creation_input_tokens: cache_create,
        },
        total_cost_usd: cost_usd,
        summary,
        created_at: iso_to_millis(&created_iso),
        updated_at: iso_to_millis(&updated_iso),
    }))
}
