//! 会话域仓储——[`Db`] 的 sessions 表读写（多文件 impl 之一）。
//!
//! 语义来源（旧仓库只读，2026-08-15 冻结）：
//! - `SessionRepository.create/findById/listAll/updateTitle/updateUsage/
//!   updateStatus/delete`
//! - `SessionManager.listSessionsPaginated`（游标锚点 + limit+1 探测 +
//!   subagent 过滤）、`loadSession`（详情含全量消息与 metadata 解析）
//! - `SessionController.listSessions`（无效游标回退、nextCursor 编码）

use std::collections::HashSet;

use rusqlite::{Connection, OptionalExtension, params};

use zk_protocol::model::Usage;

use crate::cursor::{decode_session_cursor, encode_session_cursor};
use crate::error::DbError;
use crate::model::{
    MessageRecord, MessageRole, SessionDetail, SessionPage, SessionSummary, StoredBlock,
    goal_preview,
};
use crate::run::{RunEnvelopeView, map_envelope_row};
use crate::task_runtime::{RUNTIME_TASK_COLUMNS, RuntimeTaskRecord, map_runtime_task};
use crate::time::{format_rfc3339_micros, now_millis, parse_rfc3339_millis};

/// 单事务快照恢复结果。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotRestoreOutcome {
    /// 展示属性已恢复；相同消息保留原行，安全的普通后缀已截除。
    Applied,
    /// 目标会话不存在。
    NotFound,
    /// 快照声明的工作区与数据库中的授权工作区不一致。
    WorkspaceMismatch,
    /// 快照不是当前历史的原样前缀，或截尾会损伤已有执行事实/依赖。
    HistoryConflict,
    /// 快照消息的会话、身份或序号结构非法。
    InvalidMessages,
}

/// One `SQLite` snapshot used to rebuild every session-scoped live projection.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRuntimeRestore {
    /// Root Session metadata and committed transcript.
    pub detail: SessionDetail,
    /// Most recent root Run, when the Session has executed.
    pub run_snapshot: Option<RunEnvelopeView>,
    /// Complete Task tree owned by the root Session at the same snapshot boundary.
    pub task_tree: Vec<RuntimeTaskRecord>,
    /// Global `run_event_log.id` high-water mark for the complete root Run tree.
    pub snapshot_event_seq: i64,
    /// Non-terminal invocations across the recursive Run tree.
    pub active_tool_calls: Vec<RestoredToolCall>,
    /// Recursive direct usage plus application-wide cost.
    pub cost_summary: RestoreCostSummary,
}

/// Active invocation projected into `session_restored`.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoredToolCall {
    /// Provider tool call identity.
    pub tool_use_id: String,
    /// Catalog tool name.
    pub tool_name: String,
    /// Fully persisted invocation input.
    pub input: serde_json::Value,
    /// First execution time in epoch milliseconds, when admitted.
    pub started_at: Option<i64>,
    /// UI phase: `preparing` or `running`.
    pub phase: String,
    /// Complete root/source attribution for partitioned restoration.
    pub event_context: serde_json::Value,
}

/// Usage and cost values captured in the same restore snapshot.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreCostSummary {
    /// Cost of the current recursive Run tree in USD.
    pub session_cost: f64,
    /// Cost of all persisted physical calls in USD.
    pub total_cost: f64,
    /// Token usage of the current recursive Run tree.
    pub usage: Usage,
    /// False when any physical call lacks authoritative usage.
    pub usage_complete: bool,
    /// `known`, `unknown`, or `unavailable`; independent of token usage integrity.
    pub session_pricing_status: String,
    /// Application-wide pricing availability, independent of the displayed subtotal.
    pub total_pricing_status: String,
}

fn pricing_status(conn: &Connection, session_id: &str) -> Result<(bool, bool), DbError> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM llm_calls c JOIN tasks t ON t.id=c.task_id
                       WHERE t.session_id=?1 AND c.status!='started' AND c.cost_nanos_usd IS NULL),
                EXISTS(SELECT 1 FROM llm_calls WHERE status!='started' AND cost_nanos_usd IS NULL)",
        [session_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?)
}

#[allow(clippy::cast_precision_loss)] // UI projection intentionally converts exact nanos to USD
fn nanos_to_usd(nanos: i64) -> f64 {
    nanos as f64 / 1_000_000_000.0
}

/// 摘要行 SELECT。会话类型是结构化列，只有 root 会话进入用户列表；
/// internal transcript 不再依赖易漏标的 metadata JSON 模糊匹配。
const SUMMARY_SELECT: &str = r"
    SELECT s.id, s.title, s.model, s.working_dir, s.total_cost_usd, s.permission_mode,
           COALESCE((SELECT r.status='running' FROM run_envelopes r WHERE r.session_id=s.id AND r.parent_run_id IS NULL ORDER BY r.created_at DESC,r.id DESC LIMIT 1),0) AS running,
           (SELECT operation_id FROM session_merge_locks l WHERE l.session_id=s.id) AS merge_operation_id,
           s.created_at, s.updated_at,
           (SELECT m.content_json FROM messages m
            WHERE m.session_id = s.id AND m.role = 'user'
            ORDER BY m.seq_num ASC LIMIT 1) AS first_user_content,
           (SELECT COUNT(*) FROM messages m WHERE m.session_id = s.id) AS message_count,
           EXISTS(SELECT 1 FROM tasks t WHERE t.session_id=s.id AND t.task_type='mcp' AND t.parent_task_id IS NULL) AS is_mcp
    FROM sessions s
    WHERE s.kind = 'root' AND s.content_retention = 'persistent'
";

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
            purpose: if row.get::<_, bool>("is_mcp")? {
                zk_protocol::SessionPurpose::Mcp
            } else {
                zk_protocol::SessionPurpose::Chat
            },
            running: row.get("running")?,
            merge_operation_id: row.get("merge_operation_id")?,
            permission_mode: row.get("permission_mode")?,
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

fn ensure_snapshot_restore_admitted(conn: &Connection, session_id: &str) -> Result<(), DbError> {
    // Restore can change session preferences and truncate ordinary history.
    // Keep admission in the same transaction as those writes.
    crate::session_merge::ensure_idle(conn, session_id)?;
    let merge_reserved: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_merge_locks WHERE session_id=?1)",
        [session_id],
        |row| row.get(0),
    )?;
    if merge_reserved {
        return Err(DbError::Conflict(
            "session is reserved by an active merge".into(),
        ));
    }
    // Preserve the existing pending/sealed admission policy, including restores
    // that retain every message. Retaining rows already prevents FK cascades;
    // relaxing this additional policy is outside this repair. Scope the guard
    // to this transcript rather than unrelated descendant transcripts.
    let messages_required: bool = conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM tool_result_postprocessing p
            JOIN messages m ON m.id=p.result_message_id
            WHERE m.session_id=?1 AND (
                p.status='pending' OR EXISTS(
                    SELECT 1 FROM execution_resources r
                    JOIN tool_invocations i ON i.invocation_id=r.invocation_id
                        AND i.run_id=r.run_id AND i.task_id=r.task_id
                    WHERE r.invocation_id=p.invocation_id
                        AND r.run_id=p.run_id AND r.task_id=p.task_id
                        AND i.status='succeeded'
                        AND json_extract(r.metadata_json,'$.recordingFinalization.phase')='sealed'
                )
            )
        )",
        [session_id],
        |row| row.get(0),
    )?;
    if messages_required {
        return Err(DbError::Conflict(
            "SESSION_SNAPSHOT_MESSAGE_DEPENDENCY_PENDING".into(),
        ));
    }
    Ok(())
}

fn snapshot_messages_well_formed(messages: &[MessageRecord], session_id: &str) -> bool {
    let mut ids = HashSet::with_capacity(messages.len());
    let mut previous_seq = None;
    messages.iter().all(|message| {
        let valid = message.session_id == session_id
            && !message.id.trim().is_empty()
            && ids.insert(message.id.as_str())
            && previous_seq.is_none_or(|seq| message.seq_num > seq);
        previous_seq = Some(message.seq_num);
        valid
    })
}

fn normalize_snapshot_message(message: &mut MessageRecord) {
    if message
        .meta
        .as_ref()
        .is_some_and(serde_json::Value::is_null)
    {
        message.meta = None;
    }
    for block in &mut message.content {
        if let StoredBlock::ToolResult { metadata, .. } = block
            && metadata.as_ref().is_some_and(serde_json::Value::is_null)
        {
            *metadata = None;
        }
    }
}

fn snapshot_message_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Option<MessageRecord>> {
    let role: String = row.get(2)?;
    let content: String = row.get(3)?;
    let created_at: String = row.get(7)?;
    let metadata: Option<String> = row.get(9)?;
    let (Some(role), Ok(content), Some(created_at)) = (
        MessageRole::parse(&role),
        serde_json::from_str::<Vec<StoredBlock>>(&content),
        parse_rfc3339_millis(&created_at),
    ) else {
        return Ok(None);
    };
    let meta = match metadata
        .as_deref()
        .map(serde_json::from_str::<Option<serde_json::Value>>)
        .transpose()
    {
        Ok(meta) => meta.flatten(),
        Err(_) => return Ok(None),
    };
    Ok(Some(MessageRecord {
        id: row.get(0)?,
        session_id: row.get(1)?,
        role,
        content,
        stop_reason: row.get(4)?,
        input_tokens: row.get(5)?,
        output_tokens: row.get(6)?,
        created_at,
        seq_num: row.get(8)?,
        meta,
    }))
}

// Unlike the UI loader, restoring must not skip unknown roles/blocks or replace
// malformed timestamps/metadata with defaults. A lossy read cannot prove that a
// snapshot preserves the current history. Persistent content is read in place;
// its original JSON and timestamps are never rewritten after comparison.
fn load_snapshot_history(
    conn: &Connection,
    session_id: &str,
) -> Result<Option<Vec<MessageRecord>>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id,session_id,role,content_json,stop_reason,input_tokens,output_tokens,
                created_at,seq_num,metadata_json
           FROM messages WHERE session_id=?1 ORDER BY seq_num",
    )?;
    let mut rows = stmt.query([session_id])?;
    let mut messages = Vec::new();
    while let Some(row) = rows.next()? {
        let message = match snapshot_message_from_row(row) {
            Ok(message) => message,
            Err(
                rusqlite::Error::InvalidColumnType(..)
                | rusqlite::Error::FromSqlConversionFailure(..)
                | rusqlite::Error::IntegralValueOutOfRange(..),
            ) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let Some(message) = message else {
            return Ok(None);
        };
        messages.push(message);
    }
    Ok(snapshot_messages_well_formed(&messages, session_id).then_some(messages))
}

// A Run can read the entire transcript before writing its first attributed
// message, and can reload it later. Without a durable read boundary, ownership
// of the last message is not proof that an unbound suffix was never consumed.
// Task.session_id is root ownership: Runs in another transcript alone do not
// freeze this one. A Task with no surviving Run has no resolvable read scope.
fn snapshot_history_has_execution_facts(
    conn: &Connection,
    session_id: &str,
) -> Result<bool, DbError> {
    let execution: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM run_envelopes WHERE session_id=?1)
             OR EXISTS(SELECT 1 FROM messages WHERE session_id=?1
                       AND (task_id IS NOT NULL OR run_id IS NOT NULL
                            OR source_task_id IS NOT NULL OR origin<>'conversation'))
             OR EXISTS(SELECT 1 FROM tasks t
                       WHERE NOT EXISTS(SELECT 1 FROM run_envelopes r WHERE r.task_id=t.id)
                         AND (t.session_id=?1 OR EXISTS(
                             SELECT 1 FROM sessions s WHERE s.id=?1 AND s.parent_task_id=t.id)))",
        [session_id],
        |row| row.get(0),
    )?;
    if execution {
        return Ok(true);
    }
    let references: bool = conn.query_row(
        "WITH target_messages AS (SELECT id FROM messages WHERE session_id=?1)
         SELECT EXISTS(SELECT 1 FROM task_results r
                       JOIN target_messages m ON m.id=r.final_message_id)
             OR EXISTS(SELECT 1 FROM task_result_receipts r
                       JOIN target_messages m ON m.id=r.message_id)
             OR EXISTS(SELECT 1 FROM tool_result_postprocessing p
                       JOIN target_messages m ON m.id=p.result_message_id)
             OR EXISTS(SELECT 1 FROM run_workbench_bindings b
                       JOIN target_messages m ON m.id=b.request_message_id
                                               OR m.id=b.result_message_id)
             OR EXISTS(SELECT 1 FROM external_tool_requests q
                       JOIN target_messages m ON m.id=q.result_message_id)",
        [session_id],
        |row| row.get(0),
    )?;
    if references {
        return Ok(true);
    }
    // The normal producer uses message:<id>[#sha256:<digest>]. Include any
    // suffix after '#' conservatively: a malformed digest must not hide an
    // otherwise resolvable dependency. Target-owned invocations (including
    // opaque/invalid references) are already covered by their Run above.
    // output_ref has no index. Joining it to each target message would scan the
    // global invocation table repeatedly while holding the writer. Read it once
    // and resolve links against the target's message identities instead.
    let target_ids: HashSet<String> = conn
        .prepare("SELECT id FROM messages WHERE session_id=?1")?
        .query_map([session_id], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let max_id_len = target_ids.iter().map(String::len).max().unwrap_or(0);
    let mut statement = conn.prepare(
        "SELECT output_ref FROM tool_invocations WHERE substr(output_ref,1,8)='message:'",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let reference: String = row.get(0)?;
        let Some(message_id) = reference.strip_prefix("message:") else {
            continue;
        };
        if (message_id.len() <= max_id_len && target_ids.contains(message_id))
            || message_id
                .match_indices('#')
                .take_while(|(index, _)| *index <= max_id_len)
                .any(|(index, _)| target_ids.contains(&message_id[..index]))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

// File snapshots contain caller-provided file bytes and a message-local link;
// they do not read the transcript. References to a retained prefix stay valid.
// Null/dangling/foreign links in this session cannot establish a safe scope.
fn snapshot_tail_has_file_dependencies(
    conn: &Connection,
    session_id: &str,
    last_retained_seq: Option<i64>,
) -> Result<bool, DbError> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM file_snapshots f
                       LEFT JOIN messages m ON m.id=f.message_id
                       WHERE (m.session_id=?1 AND (?2 IS NULL OR m.seq_num>?2))
                          OR (f.session_id=?1 AND (m.id IS NULL OR m.session_id<>?1)))",
        params![session_id, last_retained_seq],
        |row| row.get(0),
    )?)
}

impl crate::Db {
    /// Stable keyset search over titles and literal text blocks; filtering occurs
    /// before pagination so matching older sessions are not omitted.
    /// # Errors
    /// Database and malformed persisted row errors are surfaced to the caller.
    pub async fn search_sessions(
        &self,
        cursor: Option<&str>,
        limit: u32,
        query: &str,
    ) -> Result<SessionPage, DbError> {
        let position = cursor.and_then(crate::cursor::decode_session_position);
        let (anchor, before) = position.map_or((None, None), |(time, id)| (Some(time), Some(id)));
        let query = query.trim().to_owned();
        if query.len() > 4096 || !(1..=500).contains(&limit) {
            return Err(DbError::Validation(
                "session search query or limit exceeds capacity".into(),
            ));
        }
        self.with_reader(move|conn|{
            let sql=format!("{SUMMARY_SELECT} AND (?1='' OR instr(lower(COALESCE(s.title,'')),lower(?1))>0 OR EXISTS(
                SELECT 1 FROM json_each(CASE WHEN json_valid(first_user_content) THEN
                  CASE WHEN json_type(first_user_content)='array' THEN first_user_content ELSE '[]' END ELSE '[]' END) b
                WHERE json_extract(CASE WHEN b.type='object' THEN b.value ELSE '{{}}' END,'$.type')='text'
                  AND json_type(CASE WHEN b.type='object' THEN b.value ELSE '{{}}' END,'$.text')='text'
                  AND instr(lower(json_extract(CASE WHEN b.type='object' THEN b.value ELSE '{{}}' END,'$.text')),lower(?1))>0
                )) AND (?2 IS NULL OR s.updated_at<?2 OR (s.updated_at=?2 AND s.id<?3)) ORDER BY s.updated_at DESC,s.id DESC LIMIT ?4");
            let mut stmt=conn.prepare(&sql)?;
            let mut rows=stmt.query_map(params![query,anchor,before,i64::from(limit)+1],map_summary_row)?.collect::<Result<Vec<_>,_>>()?;
            let has_more=rows.len()>limit as usize;
            rows.truncate(limit as usize);
            let next_cursor=if has_more {rows.last().map(|(s,iso)|encode_session_cursor(iso,&s.id))} else {None};
            Ok(SessionPage {sessions:rows.into_iter().map(|(s,_)|s).collect(),has_more,next_cursor})
        }).await
    }
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
        self.create_session_with_permission(session_id, model, working_dir, None)
            .await
    }

    /// Create identity and initial authorization mode in one atomic insert.
    /// # Errors
    /// Invalid modes or database errors leave no partial session behind.
    pub async fn create_session_with_permission(
        &self,
        session_id: &str,
        model: &str,
        working_dir: &str,
        permission_mode: Option<&str>,
    ) -> Result<SessionSummary, DbError> {
        let permission_mode = permission_mode.map(str::to_owned);
        let id = session_id.to_owned();
        let model = model.to_owned();
        let working_dir = working_dir.to_owned();
        self.with_writer(move |conn| {
            let now_iso = format_rfc3339_micros(now_millis());
            conn.execute(
                "INSERT INTO sessions (id, model, working_dir, status, created_at, updated_at, permission_mode)
                 VALUES (?1, ?2, ?3, 'active', ?4, ?4, ?5)",
                params![id, model, working_dir, now_iso, permission_mode],
            )?;
            let now_ms = iso_to_millis(&now_iso);
            Ok(SessionSummary {
                purpose: zk_protocol::SessionPurpose::Chat,
                running: false,
                merge_operation_id: None,
                permission_mode,
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

    /// 创建不出现在顶层会话列表的内部执行 transcript。
    ///
    /// 父 Task 和父 Session 必须已持久化；重复 ID 失败关闭。
    ///
    /// # Errors
    /// 父对象不存在、不属于同一 Task 树或 `SQLite` 写入失败时返回错误。
    pub async fn create_internal_session_with_id(
        &self,
        session_id: &str,
        model: &str,
        working_dir: &str,
        parent_session_id: &str,
        parent_task_id: &str,
    ) -> Result<SessionSummary, DbError> {
        let parsed = uuid::Uuid::parse_str(session_id)
            .map_err(|_| DbError::Invalid("SESSION_ID_MUST_BE_UUID_V4".to_owned()))?;
        if parsed.get_version() != Some(uuid::Version::Random)
            || session_id != parsed.hyphenated().to_string()
        {
            return Err(DbError::Invalid("SESSION_ID_MUST_BE_UUID_V4".to_owned()));
        }
        let id = session_id.to_owned();
        let parent_session_id = parent_session_id.to_owned();
        let parent_task_id = parent_task_id.to_owned();
        let model = model.to_owned();
        let working_dir = working_dir.to_owned();
        self.with_writer(move |conn| {
            let owns_task: i64 = conn.query_row(
                "SELECT COUNT(*) FROM tasks t
                 JOIN sessions s ON s.id=?1
                 WHERE t.id=?2 AND t.session_id=CASE
                     WHEN s.kind='root' THEN s.id ELSE s.parent_session_id END",
                params![parent_session_id, parent_task_id],
                |row| row.get(0),
            )?;
            if owns_task != 1 {
                return Err(DbError::Invalid("INTERNAL_SESSION_PARENT_NOT_FOUND".to_owned()));
            }
            let now_iso = format_rfc3339_micros(now_millis());
            let retention=crate::content::attach_session(conn,&parent_session_id,&id)?;
            conn.execute(
                "INSERT INTO sessions
                    (id,kind,parent_session_id,parent_task_id,model,working_dir,status,created_at,updated_at,content_retention)
                 VALUES (?1,'internal',?2,?3,?4,?5,'active',?6,?6,?7)",
                params![id, parent_session_id, parent_task_id, model, working_dir, now_iso,retention.as_str()],
            )?;
            let now_ms = iso_to_millis(&now_iso);
            Ok(SessionSummary {
                purpose: zk_protocol::SessionPurpose::Chat,
                running: false,
                merge_operation_id: None,
                permission_mode: None,
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

    /// Read canonical content with display-only root-run diagnostics.
    ///
    /// # Errors
    /// Query and content ownership errors propagate without modifying stored content.
    pub async fn get_session_for_display(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionDetail>, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| {
            let tx = conn.transaction()?;
            let mut detail = load_session_detail(&tx, &session_id)?;
            if let Some(detail) = detail.as_mut() {
                crate::message::project_runtime_diagnostics(
                    &tx,
                    &session_id,
                    &mut detail.messages,
                )?;
            }
            tx.commit()?;
            Ok(detail)
        })
        .await
    }

    /// Whether finished physical calls have unknown prices (session, application).
    ///
    /// # Errors
    /// Errors must be shown as unavailable by callers, never converted to known zero.
    pub async fn get_pricing_status(&self, session_id: &str) -> Result<(bool, bool), DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| pricing_status(conn, &session_id))
            .await
    }

    /// Read messages, root Run, event high-water mark, active tool invocations,
    /// and subtree usage under one deferred `SQLite` snapshot. Empty collections
    /// and zero summaries are authoritative and must overwrite stale UI state.
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] when any query in the consistent restore snapshot fails.
    #[allow(clippy::too_many_lines)] // one transaction must project a consistent restore snapshot
    pub async fn get_session_runtime_restore(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionRuntimeRestore>, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| {
            let tx = conn.transaction()?;
            let Some(mut detail) = load_session_detail(&tx, &session_id)? else {
                tx.commit()?;
                return Ok(None);
            };
            crate::message::project_runtime_diagnostics(&tx, &session_id, &mut detail.messages)?;
            let run_snapshot = tx
                .query_row(
                    "SELECT * FROM run_envelopes
                     WHERE session_id=?1 AND parent_run_id IS NULL
                     ORDER BY started_at DESC,id DESC LIMIT 1",
                    params![session_id],
                    |row| map_envelope_row(&tx, row),
                )
                .optional()?;

            let task_tree = {
                let sql = format!(
                    "SELECT {RUNTIME_TASK_COLUMNS} FROM tasks
                     WHERE session_id=?1 ORDER BY root_task_id,created_at,ordinal,id"
                );
                let mut stmt = tx.prepare(&sql)?;
                let tasks = stmt
                    .query_map(params![session_id], |row| map_runtime_task(&tx, row))?
                    .collect::<Result<Vec<_>, _>>()?;
                drop(stmt);
                tasks
            };

            let mut snapshot_event_seq = 0;
            let mut active_tool_calls = Vec::new();
            let mut session_cost_nanos = 0_i64;
            let mut usage = Usage::default();
            let mut usage_complete = true;
            if let Some(root_run) = run_snapshot.as_ref() {
                snapshot_event_seq = tx.query_row(
                    "WITH RECURSIVE tree(id) AS (
                         SELECT id FROM run_envelopes WHERE id=?1
                         UNION ALL
                         SELECT child.id FROM run_envelopes child
                         JOIN tree parent ON child.parent_run_id=parent.id
                     )
                     SELECT COALESCE(MAX(event.id),0)
                     FROM run_event_log event JOIN tree ON tree.id=event.run_id",
                    params![root_run.id],
                    |row| row.get(0),
                )?;

                let mut stmt = tx.prepare(
                    "WITH RECURSIVE tree(id) AS (
                         SELECT id FROM run_envelopes WHERE id=?1
                         UNION ALL
                         SELECT child.id FROM run_envelopes child
                         JOIN tree parent ON child.parent_run_id=parent.id
                     )
                     SELECT invocation.tool_use_id,invocation.tool_name,
                            invocation.input_json,invocation.started_at,invocation.status,
                            invocation.task_id,invocation.run_id
                     FROM tool_invocations invocation
                     JOIN tree ON tree.id=invocation.run_id
                     WHERE invocation.status IN ('preparing','queued','running')
                     ORDER BY invocation.created_at,invocation.invocation_id",
                )?;
                let rows = stmt.query_map(params![root_run.id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                })?;
                for row in rows {
                    let (tool_use_id, tool_name, input_json, started_at, status, task_id, run_id) =
                        row?;
                    let input = input_json
                        .as_deref()
                        .and_then(|value| serde_json::from_str(value).ok())
                        .unwrap_or_else(|| serde_json::json!({}));
                    active_tool_calls.push(RestoredToolCall {
                        event_context: serde_json::json!({
                            "protocolVersion": zk_protocol::WS_PROTOCOL_VERSION,
                            "eventId": format!("snapshot:{run_id}:{tool_use_id}"),
                            "sessionId": session_id,
                            "taskId": root_run.task_id,
                            "runId": root_run.id,
                            "sourceTaskId": task_id,
                            "sourceRunId": run_id,
                            "toolUseId": tool_use_id,
                        }),
                        tool_use_id,
                        tool_name,
                        input,
                        started_at: started_at.as_deref().map(iso_to_millis),
                        phase: if status == "running" {
                            "running".to_owned()
                        } else {
                            "preparing".to_owned()
                        },
                    });
                }
                drop(stmt);

                let ledger: (i64, i64, i64, i64, i64, i64) = tx.query_row(
                    "WITH RECURSIVE tree(id) AS (
                         SELECT id FROM run_envelopes WHERE id=?1
                         UNION ALL
                         SELECT child.id FROM run_envelopes child
                         JOIN tree parent ON child.parent_run_id=parent.id
                     )
                     SELECT COALESCE(SUM(call.input_tokens),0),
                            COALESCE(SUM(call.output_tokens),0),
                            COALESCE(SUM(call.cache_read_tokens),0),
                            COALESCE(SUM(call.cache_create_tokens),0),
                            COALESCE(SUM(call.cost_nanos_usd),0),
                            COALESCE(MIN(call.usage_complete),1)
                     FROM llm_calls call JOIN tree ON tree.id=call.run_id",
                    params![root_run.id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    },
                )?;
                usage = Usage {
                    input_tokens: ledger.0,
                    output_tokens: ledger.1,
                    cache_read_input_tokens: ledger.2,
                    cache_creation_input_tokens: ledger.3,
                };
                session_cost_nanos = ledger.4;
                usage_complete = ledger.5 == 1;
            }
            let total_cost_nanos: i64 = tx.query_row(
                "SELECT COALESCE(SUM(cost_nanos_usd),0) FROM llm_calls",
                [],
                |row| row.get(0),
            )?;
            let (session_pricing_status, total_pricing_status) =
                match pricing_status(&tx, &session_id) {
                    Ok((session_unknown, total_unknown)) => (
                        if session_unknown { "unknown" } else { "known" }.to_owned(),
                        if total_unknown { "unknown" } else { "known" }.to_owned(),
                    ),
                    Err(error) => {
                        tracing::warn!(
                            code = error.diagnostic_code(),
                            "pricing display status unavailable"
                        );
                        ("unavailable".to_owned(), "unavailable".to_owned())
                    }
                };
            tx.commit()?;
            Ok(Some(SessionRuntimeRestore {
                detail,
                run_snapshot,
                task_tree,
                snapshot_event_seq,
                active_tool_calls,
                cost_summary: RestoreCostSummary {
                    session_cost: nanos_to_usd(session_cost_nanos),
                    total_cost: nanos_to_usd(total_cost_nanos),
                    usage,
                    usage_complete,
                    session_pricing_status,
                    total_pricing_status,
                },
            }))
        })
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

    /// 仅当会话仍使用预期模型时更新模型，避免恢复流程覆盖并发选择。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 写入失败时返回 [`DbError::Sqlite`]。
    pub async fn update_session_model_if_current(
        &self,
        session_id: &str,
        expected_model: &str,
        model: &str,
    ) -> Result<bool, DbError> {
        let session_id = session_id.to_owned();
        let expected_model = expected_model.to_owned();
        let model = model.to_owned();
        self.with_writer(move |conn| {
            let now_iso = format_rfc3339_micros(now_millis());
            let rows = conn.execute(
                "UPDATE sessions SET model = ?1, updated_at = ?2
                 WHERE id = ?3 AND model = ?4",
                params![model, now_iso, session_id, expected_model],
            )?;
            Ok(rows > 0)
        })
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
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            crate::session_merge::ensure_idle(&tx, &session_id)?;
            crate::browser_recordings::ensure_recordings_consumed(&tx, &session_id)?;
            let billing: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1 AND kind='merge_billing')",
                [&session_id],
                |r| r.get(0),
            )?;
            if billing {
                return Err(DbError::Validation(
                    "merge accounting sessions are not user sessions".into(),
                ));
            }
            let rows = tx.execute("DELETE FROM sessions WHERE id = ?1", params![session_id])?;
            tx.commit()?;
            Ok(rows > 0)
        })
        .await
    }

    /// 在一个 `SQLite` 写事务中恢复展示属性与可安全截尾的消息历史。
    ///
    /// 相同历史保留全部原行；只允许删除没有执行事实或依赖的普通消息后缀。
    /// 不补回消息、不覆盖消息内容或归属，也不回退累计用量。工作区、运行、
    /// 合并、工具后处理和历史依赖均在同一事务内复检。
    ///
    /// # Errors
    /// 活跃工作、内容保留策略或数据库操作失败时返回 [`DbError`]；消息结构
    /// 非法或历史不能安全恢复时返回对应的 [`SnapshotRestoreOutcome`]。
    pub async fn restore_session_snapshot(
        &self,
        session_id: &str,
        expected_working_dir: &str,
        model: &str,
        status: &str,
        title: Option<&str>,
        mut messages: Vec<MessageRecord>,
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
            crate::content::require_persistent_session(&tx, &session_id)?;
            if working_dir != expected_working_dir {
                return Ok(SnapshotRestoreOutcome::WorkspaceMismatch);
            }
            if !snapshot_messages_well_formed(&messages, &session_id) {
                return Ok(SnapshotRestoreOutcome::InvalidMessages);
            }

            ensure_snapshot_restore_admitted(&tx, &session_id)?;
            let Some(current) = load_snapshot_history(&tx, &session_id)? else {
                return Ok(SnapshotRestoreOutcome::HistoryConflict);
            };
            for message in &mut messages {
                normalize_snapshot_message(message);
            }
            if !current.starts_with(&messages) {
                return Ok(SnapshotRestoreOutcome::HistoryConflict);
            }
            if messages.len() < current.len() {
                let last_retained_seq = messages.last().map(|message| message.seq_num);
                if snapshot_history_has_execution_facts(&tx, &session_id)?
                    || snapshot_tail_has_file_dependencies(&tx, &session_id, last_retained_seq)?
                {
                    return Ok(SnapshotRestoreOutcome::HistoryConflict);
                }
                tx.execute(
                    "DELETE FROM messages WHERE session_id=?1 AND (?2 IS NULL OR seq_num>?2)",
                    params![&session_id, last_retained_seq],
                )?;
            }
            let now = format_rfc3339_micros(now_millis());
            tx.execute(
                "UPDATE sessions SET title = ?1, model = ?2, status = ?3,
                    updated_at = ?4 WHERE id = ?5",
                params![title, model, status, now, &session_id],
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
    let stored;
    let value = if matches!(column, "title" | "summary" | "metadata_json") {
        if !conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE id=?1)",
            [session_id],
            |row| row.get::<_, bool>(0),
        )? {
            return Ok(false);
        }
        stored = crate::content::store_text(conn, session_id, value)?;
        stored.as_str()
    } else {
        value
    };
    let sql = format!("UPDATE sessions SET {column} = ?1, updated_at = ?2 WHERE id = ?3");
    let rows = conn.execute(&sql, params![value, now_iso, session_id])?;
    Ok(rows > 0)
}

/// 同步加载会话详情（`get_session` 的 blocking 主体，亦供消息域复用）。
fn load_session_detail(
    conn: &Connection,
    session_id: &str,
) -> Result<Option<SessionDetail>, DbError> {
    let row = conn
        .query_row(
            "SELECT id, model, working_dir, title, status, summary, metadata_json,
                    total_input_tokens, total_output_tokens, total_cache_read,
                    total_cache_create, total_cost_usd, created_at, updated_at,
                    EXISTS(SELECT 1 FROM tasks t WHERE t.session_id=sessions.id AND t.task_type='mcp' AND t.parent_task_id IS NULL) AS is_mcp
             FROM sessions WHERE id = ?1",
            params![session_id],
            |row| {
                Ok((
                    row.get::<_, String>("id")?,
                    row.get::<_, String>("model")?,
                    row.get::<_, String>("working_dir")?,
                    crate::content::load_optional(conn,session_id,row.get("title")?)?,
                    row.get::<_, String>("status")?,
                    crate::content::load_optional(conn,session_id,row.get("summary")?)?,
                    crate::content::load_optional(conn,session_id,row.get("metadata_json")?)?,
                    row.get::<_, i64>("total_input_tokens")?,
                    row.get::<_, i64>("total_output_tokens")?,
                    row.get::<_, i64>("total_cache_read")?,
                    row.get::<_, i64>("total_cache_create")?,
                    row.get::<_, f64>("total_cost_usd")?,
                    row.get::<_, String>("created_at")?,
                    row.get::<_, String>("updated_at")?,
                    row.get::<_, bool>("is_mcp")?,
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
        is_mcp,
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
        purpose: if is_mcp {
            zk_protocol::SessionPurpose::Mcp
        } else {
            zk_protocol::SessionPurpose::Chat
        },
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
