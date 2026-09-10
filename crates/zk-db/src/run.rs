//! Run 生命周期存储层——`run_envelopes` / `run_event_log` 的**唯一** SQL 权威。
//!
//! 旧源 `run/RunControlService.java`（340 行）+ `run/RunEnvelope.java`（起始态
//! 与枚举字面量）+ `run/RunTracker.java`（编排适配器）。完整 Run 控制面
//! （`setVerification` 等写控制面）属 **M-RUN 里程碑**，不在本模块范围
//! （`RunEnvelope` 只读视图 API 于 Batch 2 端点域随 `RunController` 移植入本
//! 模块，见文末 [`RunEnvelopeView`]）；此处移植的是「Run 必须先存在、结束时必须
//! 落终态」这一生命周期骨架，以及授权链/交互链无法绕开的状态机迁移。
//!
//! | 本模块 | 旧源 |
//! |---|---|
//! | [`start_in_current_write`] | `RunControlService.java:65-96` + `RunEnvelope.start` L48-54 |
//! | [`transition_in_current_write`] | `RunControlService.java:190-255` |
//! | [`append_event_in_current_write`] | `RunControlService.java:261-274` + `serializeEvent` L276-303 |
//! | [`mark_waiting_in_current_write`] | L103-106 |
//! | [`mark_running_in_current_write`] | L113-116 |
//! | [`request_cancel_in_current_write`] | L118-122（`transition` → 有界写内联版） |
//! | [`complete_in_current_write`] | L124-128 |
//! | [`fail_in_current_write`] | L130-133 |
//! | [`cancel_in_current_write`] | L135-139 |
//! | [`interrupt_in_current_write`] | L141-145 |
//! | [`finish_in_current_write`] | `RunTerminationCoordinator.terminate` L69-73 |
//! | [`terminate_in_current_write`] | `RunTerminationCoordinator.terminate` L40-75（Run 状态部分） |
//! | [`Db::start_run`] | `RunTracker.startRun` L43-44 |
//! | [`Db::complete_run`] | `RunTracker.completeRun` L74-89（`control.complete` 部分） |
//! | [`Db::terminate_run`] | `RunTracker.failRun` L91-93 / `abortRun` L95-107 |
//! | [`Db::finish_run`] | `RunTerminationCoordinator.terminate` L69-73 |
//!
//! # 层级归属（依赖铁律）
//!
//! `zk-engine`（run 生命周期的生产调用方）与 `zk-server`（交互链 / 授权诊断
//! 事件出口）都必须写这两张表，而铁律禁止 `zk-engine → zk-server`。故 SQL
//! 下沉到二者共同依赖的 `zk-db`：单一实现、零逻辑分叉、零反向依赖。
//! `zk_server::interaction::runs` 现为本模块的再导出 + `RunEventSink` 适配。
//!
//! # 偏离留痕
//!
//! - **R-01**（承接自 zk-server 原实现）：旧源 `transition` / `appendEvent` 各自
//!   开 `write()` 事务；本模块的 `*_in_current_write` 只接受调用方已持有的
//!   `&Connection`，事务边界由调用方持有（[`Db`] 上的 async 方法各自开一层事务，
//!   与旧源的非 `InCurrentWrite` 变体等价）。
//! - **R-02**：旧 `serializeEvent` 可选注入 `SensitiveDataFilter` 做全树文本脱敏；
//!   zkcode 的敏感数据过滤器属 zk-tools（zk-db 不可反向依赖），且事件载荷只含
//!   哈希/枚举/摘要而非工具原始入参，故不接过滤器。DEFERRED。
//! - **R-03**：旧 `statusEvent` 为 `LinkedHashMap`（插入序），`serde_json::Map`
//!   默认 `BTreeMap` 故键序为字典序；`run_event_log` 消费方按键取值，不依赖键序。
//!   旧源的 `errorLength` / `errorFingerprint` 两键依赖未移植的 `SafeLogValue`，
//!   本实现不产出（诊断辅助字段，无状态机语义）。

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::Db;
use crate::error::DbError;
use crate::task_budget::{TaskBudgetLimits, validate_limits};
use crate::task_runtime::{ExitReason, RunStatus};
use crate::time;

/// 单条 run 事件 JSON 上限（旧 `MAX_EVENT_BYTES`，`RunControlService.java:44`）。
const MAX_EVENT_BYTES: usize = 10 * 1024;

/// 根 Run 的起始态（旧 `RunEnvelope.start` L50 `RunStatus.RUNNING`）。
pub const STATUS_RUNNING: &str = RunStatus::Running.as_db();

/// The model completed its execution loop normally.
pub const EXIT_MODEL_FINISHED: &str = ExitReason::ModelFinished.as_db();
/// The Run exceeded its configured deadline.
pub const EXIT_TIMEOUT: &str = ExitReason::Timeout.as_db();
/// The Run exhausted its maximum turn count.
pub const EXIT_MAX_TURNS: &str = ExitReason::MaxTurns.as_db();
/// The Run exhausted its token or cost budget.
pub const EXIT_BUDGET_EXHAUSTED: &str = ExitReason::BudgetExhausted.as_db();
/// The model Provider failed before the Run could complete.
pub const EXIT_PROVIDER_ERROR: &str = ExitReason::ProviderError.as_db();
/// A tool invocation failed the Run.
pub const EXIT_TOOL_ERROR: &str = ExitReason::ToolError.as_db();
/// The user requested cancellation of the Run.
pub const EXIT_USER_CANCELLED: &str = ExitReason::UserCancelled.as_db();
/// An attached parent requested cancellation of the Run.
pub const EXIT_PARENT_CANCELLED: &str = ExitReason::ParentCancelled.as_db();
/// The service restarted while the Run was active.
pub const EXIT_SERVICE_RESTART: &str = ExitReason::ServiceRestart.as_db();
/// An internal runtime failure ended the Run.
pub const EXIT_INTERNAL_ERROR: &str = ExitReason::InternalError.as_db();

/// Interaction deadline exhaustion is represented by the public `timeout`
/// reason; delivery-vs-decision detail remains in `error_summary`.
pub const EXIT_INTERACTION_EXPIRED: &str = EXIT_TIMEOUT;

/// Interaction-capacity failure has no separate public exit variant.
pub const EXIT_INTERACTION_CAPACITY_EXCEEDED: &str = EXIT_INTERNAL_ERROR;

fn validate_exit_reason(reason: &str) -> Result<(), DbError> {
    ExitReason::parse(reason).map(|_| ())
}

/// 根 Run 的 `agent_type`（旧 `QueryEngine.execute` L288-289：无
/// `parentSessionId` 即 `"query"`，子代理为 `"subagent"`）。
pub const AGENT_TYPE_QUERY: &str = "query";
/// 子 Agent Run 类型；其 `parent_run_id` 必须指向已存在的父 Run。
pub const AGENT_TYPE_SUBAGENT: &str = "subagent";

/// 状态机迁移结果（旧 `TransitionResult`，`RunControlService.java:337`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionResult {
    /// 迁移已落库。
    Applied,
    /// 当前状态已是终态。
    AlreadyTerminal,
    /// 乐观锁版本冲突。
    VersionConflict,
    /// 当前状态不在 `expected` 集合内且非终态。
    InvalidTransition,
    /// Run 不存在。
    NotFound,
}

impl TransitionResult {
    /// 诊断字符串（旧 `String.valueOf(TransitionResult)`）。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "APPLIED",
            Self::AlreadyTerminal => "ALREADY_TERMINAL",
            Self::VersionConflict => "VERSION_CONFLICT",
            Self::InvalidTransition => "INVALID_TRANSITION",
            Self::NotFound => "NOT_FOUND",
        }
    }
}

/// 旧 `isTerminal(String)`（L335）。
fn is_terminal(status: &str) -> bool {
    RunStatus::parse(status).is_ok_and(RunStatus::is_terminal)
}

/// 旧 `value(Object)`（L336）：`null` → `"unknown"`。
fn value(input: Option<&str>) -> String {
    input.map_or_else(|| "unknown".to_owned(), str::to_owned)
}

/// 裸 SHA-256 小写 hex（对照旧 `MessageDigest.getInstance("SHA-256")` +
/// `HexFormat.of().formatHex`，见 `RunControlService.serializeEvent` 截断分支）。
fn sha256_hex(input: &str) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(input.as_bytes());
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        // 写入 String 永不失败。
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// 旧 `serializeEvent(runId, toolUseId, data)`（`RunControlService.java:276-303`）。
///
/// 信封 `{schemaVersion:2, entityId, [toolUseId], data}`；超过 `MAX_EVENT_BYTES`
/// 时 `data` 替换为截断摘要（`truncated` / `originalBytes` / `payloadSha256` /
/// `payloadPreview` 前 2048 字节）。
fn serialize_event(run_id: &str, tool_use_id: Option<&str>, data: &Value) -> String {
    let mut envelope = Map::new();
    envelope.insert("schemaVersion".to_owned(), json!(2));
    envelope.insert("entityId".to_owned(), json!(run_id));
    if let Some(tool_use_id) = tool_use_id {
        envelope.insert("toolUseId".to_owned(), json!(tool_use_id));
    }
    envelope.insert("data".to_owned(), data.clone());
    // 载荷恒为可序列化的 serde_json::Value，`to_string` 不会失败；旧源的
    // `RUN_EVENT_SERIALIZATION_FAILED` 分支在 Rust 静态不可达。
    let json_text = Value::Object(envelope.clone()).to_string();
    let bytes = json_text.as_bytes();
    if bytes.len() <= MAX_EVENT_BYTES {
        return json_text;
    }
    let preview_end = json_text
        .char_indices()
        .map(|(idx, _)| idx)
        .chain(std::iter::once(json_text.len()))
        .take_while(|idx| *idx <= 2048)
        .last()
        .unwrap_or(0);
    envelope.insert(
        "data".to_owned(),
        json!({
            "truncated": true,
            "originalBytes": bytes.len(),
            "payloadSha256": sha256_hex(&json_text),
            "payloadPreview": &json_text[..preview_end],
        }),
    );
    Value::Object(envelope).to_string()
}

/// 旧 `appendEventInCurrentWrite(runId, type, toolUseId, data)`（L261-274）。
///
/// 返回分配到的 `seq`（旧源返回 `RunEvent`，zkcode 无调用方消费该视图）。
///
/// # Errors
/// `run_event_log` 插入或 `tool_call_count` 自增失败时返回 [`DbError`]。
pub fn append_event_in_current_write(
    conn: &Connection,
    run_id: &str,
    event_type: &str,
    tool_use_id: Option<&str>,
    data: &Value,
) -> Result<i64, DbError> {
    let max: i64 = conn.query_row(
        "SELECT COALESCE(MAX(seq),0) FROM run_event_log WHERE run_id=?1",
        params![run_id],
        |row| row.get(0),
    )?;
    let seq = max + 1;
    let json_text = serialize_event(run_id, tool_use_id, data);
    let ts = time::now_millis();
    conn.execute(
        "INSERT INTO run_event_log(run_id,seq,event_type,event_data,ts) VALUES(?1,?2,?3,?4,?5)",
        params![run_id, seq, event_type, json_text, ts],
    )?;
    if event_type == "tool_started" || event_type == "tool_call" {
        conn.execute(
            "UPDATE run_envelopes SET tool_call_count=tool_call_count+1,updated_at=?1 WHERE id=?2",
            params![time::format_rfc3339_micros(ts), run_id],
        )?;
    }
    Ok(seq)
}

/// `run_envelopes` 迁移前快照，对应旧源 `transitionInCurrentWrite` 首条 SELECT
/// 的 8 列（`status` / `version` / `parent_run_id` / `agent_type` /
/// `requested_exit_reason` / `total_tokens` / `total_cost_usd` / `turn_count`）。
type RunSnapshot = (
    String,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    i64,
    f64,
    i64,
);

/// 旧 `transitionInCurrentWrite(...)`（`RunControlService.java:190-255`）。
///
/// `tokens` / `cost` / `turns` 逐值对齐旧源的 `CASE WHEN ?>0` 语义：传 0（或
/// 0.0）保留原值，正值覆盖。
///
/// # Errors
/// 查询或更新失败时返回 [`DbError`]。
#[expect(
    clippy::too_many_arguments,
    reason = "逐参对齐旧 RunControlService.transitionInCurrentWrite 的 11 参签名"
)]
#[expect(
    clippy::too_many_lines,
    reason = "单条状态机迁移的 SELECT/UPDATE/授权撤销/状态事件四步逐行对齐旧源，拆分反而割裂对照"
)]
pub fn transition_in_current_write(
    conn: &Connection,
    run_id: &str,
    expected: &[&str],
    target: &str,
    exit_reason: Option<&str>,
    requested_reason: Option<&str>,
    waiting_reason: Option<&str>,
    abort_reason: Option<&str>,
    error: Option<&str>,
    tokens: i64,
    cost: f64,
    turns: i64,
) -> Result<TransitionResult, DbError> {
    for reason in [exit_reason, requested_reason, abort_reason]
        .into_iter()
        .flatten()
    {
        validate_exit_reason(reason)?;
    }
    let row: Option<RunSnapshot> = conn
        .query_row(
            "SELECT status,version,parent_run_id,agent_type,requested_exit_reason,\
                    total_tokens,total_cost_usd,turn_count \
             FROM run_envelopes WHERE id=?1",
            params![run_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .optional()?;
    let Some((
        current,
        version,
        parent_run_id,
        agent_type,
        stored_requested,
        stored_tokens,
        stored_cost,
        stored_turns,
    )) = row
    else {
        return Ok(TransitionResult::NotFound);
    };
    if !expected.contains(&current.as_str()) {
        return Ok(if is_terminal(&current) {
            TransitionResult::AlreadyTerminal
        } else {
            TransitionResult::InvalidTransition
        });
    }
    let now = time::format_rfc3339_micros(time::now_millis());
    let terminal = is_terminal(target);
    // 旧源把 `expected` 列表以字面量拼进 SQL 的 `status IN (...)`；此处同样
    // 拼接——`expected` 全为本模块内的编译期常量，无外部输入注入面。
    let in_clause = expected
        .iter()
        .map(|status| format!("'{status}'"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "UPDATE run_envelopes SET status=?1, exit_reason=?2, \
           requested_exit_reason=COALESCE(?3,requested_exit_reason), \
           waiting_reason=?4, abort_reason=?5, error_summary=?6, \
           total_tokens=CASE WHEN ?7>0 THEN ?7 ELSE total_tokens END, \
           total_cost_usd=CASE WHEN ?8>0 THEN ?8 ELSE total_cost_usd END, \
           turn_count=CASE WHEN ?9>0 THEN ?9 ELSE turn_count END, \
           finished_at=?10, terminal_at=?11, version=version+1, updated_at=?12 \
         WHERE id=?13 AND version=?14 AND status IN ({in_clause})"
    );
    let terminal_at = if terminal { Some(now.as_str()) } else { None };
    let updated = conn.execute(
        &sql,
        params![
            target,
            exit_reason,
            requested_reason,
            waiting_reason,
            abort_reason,
            error,
            tokens,
            cost,
            turns,
            terminal_at,
            terminal_at,
            now,
            run_id,
            version
        ],
    )?;
    if updated != 1 {
        return Ok(TransitionResult::VersionConflict);
    }
    if terminal {
        // 旧源 L226-231：RUN 授权属于根 Run；子 Run 进入终态时不得撤销根 Run 的授权。
        conn.execute(
            "UPDATE permission_grants SET expires_at=COALESCE(expires_at,?1), \
               revoked_at=COALESCE(revoked_at,?2) \
             WHERE scope='RUN' AND root_run_id=?3 AND revoked_at IS NULL \
               AND EXISTS(SELECT 1 FROM run_envelopes r WHERE r.id=?4 AND r.parent_run_id IS NULL)",
            params![now, now, run_id, run_id],
        )?;
    }
    let error_present = error.is_some_and(|text| !text.trim().is_empty());
    let status_event = json!({
        "from": current,
        "to": target,
        "parentRunId": value(parent_run_id.as_deref()),
        "agentType": value(agent_type.as_deref()),
        "exitReason": value(exit_reason),
        "requestedExitReason": value(requested_reason.or(stored_requested.as_deref())),
        "waitingReason": value(waiting_reason),
        "abortReason": value(abort_reason),
        "errorPresent": error_present,
        "errorCategory": if error_present { value(exit_reason) } else { "none".to_owned() },
        "totalTokens": if tokens > 0 { tokens } else { stored_tokens },
        "totalCostUsd": if cost > 0.0 { cost } else { stored_cost },
        "turnCount": if turns > 0 { turns } else { stored_turns },
        "totalsAvailable": true,
    });
    append_event_in_current_write(conn, run_id, "run_status_changed", None, &status_event)?;
    Ok(TransitionResult::Applied)
}

/// 旧 `markWaitingInCurrentWrite(runId, reason)`（L103-106）。
///
/// # Errors
/// 见 [`transition_in_current_write`]。
pub fn mark_waiting_in_current_write(
    conn: &Connection,
    run_id: &str,
    reason: &str,
) -> Result<TransitionResult, DbError> {
    transition_in_current_write(
        conn,
        run_id,
        &[STATUS_RUNNING],
        "waitingInteraction",
        None,
        None,
        Some(reason),
        None,
        None,
        0,
        0.0,
        0,
    )
}

/// 旧 `markRunningInCurrentWrite(runId)`（L113-116）。
///
/// # Errors
/// 见 [`transition_in_current_write`]。
pub fn mark_running_in_current_write(
    conn: &Connection,
    run_id: &str,
) -> Result<TransitionResult, DbError> {
    transition_in_current_write(
        conn,
        run_id,
        &["waitingInteraction"],
        STATUS_RUNNING,
        None,
        None,
        None,
        None,
        None,
        0,
        0.0,
        0,
    )
}

/// 旧 `requestCancel(runId, reason)`（L118-122）的有界写内联版。
///
/// # Errors
/// 见 [`transition_in_current_write`]。
pub fn request_cancel_in_current_write(
    conn: &Connection,
    run_id: &str,
    exit_reason: &str,
) -> Result<TransitionResult, DbError> {
    transition_in_current_write(
        conn,
        run_id,
        &[STATUS_RUNNING, "waitingInteraction"],
        "cancelling",
        None,
        Some(exit_reason),
        None,
        None,
        None,
        0,
        0.0,
        0,
    )
}

/// 旧 `complete(runId, tokens, cost, turns)`（L124-128）的有界写内联版。
///
/// # Errors
/// 见 [`transition_in_current_write`]。
pub fn complete_in_current_write(
    conn: &Connection,
    run_id: &str,
    tokens: i64,
    cost: f64,
    turns: i64,
) -> Result<TransitionResult, DbError> {
    transition_in_current_write(
        conn,
        run_id,
        &[STATUS_RUNNING],
        "completed",
        Some(EXIT_MODEL_FINISHED),
        None,
        None,
        None,
        None,
        tokens,
        cost,
        turns,
    )
}

/// 旧 `fail(runId, reason, error)`（L130-133）的有界写内联版。
///
/// # Errors
/// 见 [`transition_in_current_write`]。
pub fn fail_in_current_write(
    conn: &Connection,
    run_id: &str,
    exit_reason: &str,
    error: Option<&str>,
) -> Result<TransitionResult, DbError> {
    transition_in_current_write(
        conn,
        run_id,
        &[STATUS_RUNNING, "waitingInteraction", "cancelling"],
        "failed",
        Some(exit_reason),
        None,
        None,
        None,
        error,
        0,
        0.0,
        0,
    )
}

/// 旧 `cancel(runId)`（L135-139）的有界写内联版。
///
/// `abort_reason` 使用公共契约字面量 [`EXIT_USER_CANCELLED`]。
///
/// # Errors
/// 见 [`transition_in_current_write`]。
pub fn cancel_in_current_write(
    conn: &Connection,
    run_id: &str,
) -> Result<TransitionResult, DbError> {
    transition_in_current_write(
        conn,
        run_id,
        &["cancelling", STATUS_RUNNING, "waitingInteraction"],
        "cancelled",
        Some(EXIT_USER_CANCELLED),
        None,
        None,
        Some(EXIT_USER_CANCELLED),
        None,
        0,
        0.0,
        0,
    )
}

/// 旧 `interrupt(runId, reason)`（L141-145）的有界写内联版。
///
/// # Errors
/// 见 [`transition_in_current_write`]。
pub fn interrupt_in_current_write(
    conn: &Connection,
    run_id: &str,
    exit_reason: &str,
) -> Result<TransitionResult, DbError> {
    transition_in_current_write(
        conn,
        run_id,
        &["queued", STATUS_RUNNING, "waitingInteraction", "cancelling"],
        "interrupted",
        Some(exit_reason),
        None,
        None,
        Some(exit_reason),
        None,
        0,
        0.0,
        0,
    )
}

/// 旧 `start(sessionId, parentRunId, agentType, model)`（L65-96）的有界写内联版。
///
/// 旧源用 `RunEnvelope.start` 生成 id 与时间戳；此处 id 由调用方给出——引擎在
/// turn 开始时即分配 `run_id` 并沿工具上下文传播（`engine.rs::prepare_run`），
/// 与旧源 `QueryEngine` 把 `startRun` 返回的 `RunEnvelope.id` 赋给
/// `currentRunId` 等价。`started_at` / `created_at` / `updated_at` 取同一时刻，
/// 逐值对齐旧 `RunEnvelope.start` 的单一 `Instant now`。
///
/// # Errors
/// 根 Run 的会话不存在返回 `RUN_ROOT_SESSION_NOT_FOUND`、父 Run 不存在返回
/// `RUN_PARENT_NOT_FOUND`（均为 [`DbError::Invalid`]）；插入失败原样上抛。
pub fn start_in_current_write(
    conn: &Connection,
    run_id: &str,
    session_id: &str,
    parent_run_id: Option<&str>,
    agent_type: Option<&str>,
    model: &str,
) -> Result<(), DbError> {
    let parent_task: Option<(String, String)> = if let Some(parent) = parent_run_id {
        let found = conn
            .query_row(
                "SELECT task_id, t.root_task_id
                   FROM run_envelopes r JOIN tasks t ON t.id=r.task_id
                  WHERE r.id=?1",
                params![parent],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if found.is_none() {
            return Err(DbError::Invalid("RUN_PARENT_NOT_FOUND".to_owned()));
        }
        found
    } else {
        let sessions: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sessions WHERE id=?1",
            params![session_id],
            |row| row.get(0),
        )?;
        if sessions != 1 {
            return Err(DbError::Invalid("RUN_ROOT_SESSION_NOT_FOUND".to_owned()));
        }
        None
    };
    let now = time::format_rfc3339_micros(time::now_millis());
    // Compatibility entry point: every legacy `start_run` is still represented by a
    // real Task before the Run becomes observable. New TaskRuntime callers create the
    // task explicitly and use the dedicated repository APIs.
    let task_id = run_id;
    let (parent_task_id, root_task_id, ordinal) = if let Some((parent_task_id, root)) = parent_task
    {
        let ordinal: i64 = conn.query_row(
            "SELECT COALESCE(MAX(ordinal),-1)+1 FROM tasks WHERE parent_task_id=?1",
            params![parent_task_id],
            |row| row.get(0),
        )?;
        (Some(parent_task_id), root, ordinal)
    } else {
        (None, task_id.to_owned(), 0)
    };
    conn.execute(
        "INSERT INTO tasks
            (id,session_id,parent_task_id,root_task_id,creator_run_id,ordinal,
             description,task_type,status,execution_config_json,lifecycle_policy,
             cleanup_status,verification_status,created_at,updated_at)
         VALUES(?1,?2,?3,?4,?5,?6,?7,'agent','running','{}','attached',
                'notRequired','notRequested',?8,?8)",
        params![
            task_id,
            session_id,
            parent_task_id,
            root_task_id,
            parent_run_id,
            ordinal,
            format!("{} run", agent_type.unwrap_or("query")),
            now,
        ],
    )?;
    conn.execute(
        "INSERT INTO run_envelopes \
         (id,session_id,task_id,attempt,startup_epoch,parent_run_id,status,version,agent_type,model,started_at, \
          total_tokens,total_cost_usd,tool_call_count,turn_count,verification_status,cleanup_status, \
          created_at,updated_at) \
         VALUES(?1,?2,?3,1,0,?4,'running',0,?5,?6,?7,0,0,0,0,'notRequested','notRequired',?8,?9)",
        params![
            run_id,
            session_id,
            task_id,
            parent_run_id,
            agent_type,
            model,
            now,
            now,
            now
        ],
    )?;
    conn.execute(
        "UPDATE tasks SET current_run_id=?1 WHERE id=?2",
        params![run_id, task_id],
    )?;
    if let Some(parent_task_id) = parent_task_id {
        conn.execute(
            "INSERT INTO task_dependencies
                (parent_task_id,child_task_id,lifecycle_policy,required,created_at,updated_at)
             VALUES(?1,?2,'attached',1,?3,?3)",
            params![parent_task_id, task_id, now],
        )?;
    }
    append_event_in_current_write(
        conn,
        run_id,
        "run_started",
        None,
        &json!({
            "sessionId": session_id,
            "parentRunId": value(parent_run_id),
            "agentType": value(agent_type),
            "model": model,
        }),
    )?;
    Ok(())
}

/// 旧 `RunTerminationCoordinator.terminate` 的**落终态一步**（L69-73）。
///
/// 前置条件：Run 已由 [`request_cancel_in_current_write`] 切到 `cancelling`
/// （旧源把这一步交给 `interactions.beginRunTermination`）。分派逐值对齐旧源——
/// [`EXIT_USER_CANCELLED`] → [`cancel_in_current_write`]（旧 L70），其余 →
/// [`fail_in_current_write`]（旧 L72 `runs.fail(runId, reason, detail)`）。
///
/// `detail` **原样透传**：旧源只在第一步（`beginRunTermination` 的交互
/// `terminal_reason`）做 `detail == null ? reason.dbValue() : detail` 兜底
/// （L42），落终态这一步不兜底，故 `None` 即 `error_summary IS NULL`。
///
/// # 偏离留痕（R-04）
///
/// 旧源在两步之间停子系统并做静默确认（`executions.beginTermination` /
/// `abortRun` / `tools.cancelRunDetailed` / `processes.cancelRunDetailed` /
/// `awaitQuiescence`，未静默则改判
/// `TOOL_TERMINATION_UNCONFIRMED` / `PROCESS_TERMINATION_UNCONFIRMED`）。
/// zkcode 的 Run 执行登记表与受管进程静默确认属 **M-RUN 里程碑**，本模块不含
/// 该分支：终止仍由引擎侧的三层取消令牌树（session → run → 单次工具调用）驱动，
/// 状态回写取旧源「已静默」路径。DEFERRED。
///
/// # Errors
/// 见 [`transition_in_current_write`]。
pub fn finish_in_current_write(
    conn: &Connection,
    run_id: &str,
    exit_reason: &str,
    detail: Option<&str>,
) -> Result<TransitionResult, DbError> {
    if exit_reason == EXIT_USER_CANCELLED {
        cancel_in_current_write(conn, run_id)
    } else {
        fail_in_current_write(conn, run_id, exit_reason, detail)
    }
}

/// 旧 `RunTerminationCoordinator.terminate(runId, reason, detail)`（L40-75）的
/// **Run 状态部分**：`requestCancel` + 落终态两步串联。
///
/// 迁移结果非 `APPLIED` 即原样返回、**不落终态**（旧源 L43-46：已在终态或已有
/// 其它终止流程在跑时不重复终止）；`APPLIED` 后走 [`finish_in_current_write`]。
///
/// 本函数是**引擎侧**（`zk-engine` 无交互域可达性）的终止入口，故第一步只做
/// `requestCancel`，不含旧源第一步里「级联取消该 Run 全部待决交互」的部分——
/// 那部分需要交互服务的容量信号量与唤醒表，由 `zk-server` 的
/// `RunTerminationCoordinator` 承载（旧源同样是
/// `interactions.beginRunTermination` 而非 `runs.requestCancel`）。
///
/// # Errors
/// 见 [`transition_in_current_write`]。
pub fn terminate_in_current_write(
    conn: &Connection,
    run_id: &str,
    exit_reason: &str,
    detail: Option<&str>,
) -> Result<TransitionResult, DbError> {
    let requested = request_cancel_in_current_write(conn, run_id, exit_reason)?;
    if requested != TransitionResult::Applied {
        return Ok(requested);
    }
    finish_in_current_write(conn, run_id, exit_reason, detail)
}

impl Db {
    /// Atomically create a production root Task/Run with durable execution limits.
    ///
    /// Unlike [`Self::start_run`], this entry point always persists a finite deadline.
    /// Token and cost ceilings are optional; usage integrity remains fail-closed.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Invalid`] when a configured limit is invalid or the deadline is absent,
    /// or the same persistence errors as [`start_in_current_write`].
    pub async fn start_root_run_with_budget(
        &self,
        run_id: &str,
        session_id: &str,
        agent_type: Option<&str>,
        model: &str,
        limits: &TaskBudgetLimits,
    ) -> Result<(), DbError> {
        self.start_root_run_with_budget_inner(run_id, session_id, agent_type, model, limits, 0)
            .await
    }

    /// Atomically create a production root Task/Run with durable execution limits
    /// and stamp it with the current durable startup epoch.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Invalid`] when `startup_epoch` is not positive or a
    /// configured limit is invalid or the deadline is absent, plus persistence errors from
    /// [`start_in_current_write`].
    pub async fn start_root_run_with_budget_at_epoch(
        &self,
        run_id: &str,
        session_id: &str,
        agent_type: Option<&str>,
        model: &str,
        limits: &TaskBudgetLimits,
        startup_epoch: i64,
    ) -> Result<(), DbError> {
        if startup_epoch <= 0 {
            return Err(DbError::Invalid("STARTUP_EPOCH_INVALID".to_owned()));
        }
        self.start_root_run_with_budget_inner(
            run_id,
            session_id,
            agent_type,
            model,
            limits,
            startup_epoch,
        )
        .await
    }

    async fn start_root_run_with_budget_inner(
        &self,
        run_id: &str,
        session_id: &str,
        agent_type: Option<&str>,
        model: &str,
        limits: &TaskBudgetLimits,
        startup_epoch: i64,
    ) -> Result<(), DbError> {
        validate_limits(limits)?;
        if limits.deadline_at_ms.is_none() {
            return Err(DbError::Invalid("ROOT_BUDGET_NOT_CONFIGURED".to_owned()));
        }
        let run_id = run_id.to_owned();
        let session_id = session_id.to_owned();
        let agent_type = agent_type.map(str::to_owned);
        let model = model.to_owned();
        let limits = limits.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            start_in_current_write(
                &tx,
                &run_id,
                &session_id,
                None,
                agent_type.as_deref(),
                &model,
            )?;
            let run_updated = tx.execute(
                "UPDATE run_envelopes SET startup_epoch=?1,updated_at=?2
                 WHERE id=?3 AND task_id=?3 AND attempt=1 AND startup_epoch=0",
                params![
                    startup_epoch,
                    time::format_rfc3339_micros(time::now_millis()),
                    run_id,
                ],
            )?;
            if run_updated != 1 {
                return Err(DbError::Invalid(
                    "ROOT_STARTUP_EPOCH_INITIALIZATION_FAILED".to_owned(),
                ));
            }
            let execution_config_json = serde_json::json!({
                "budget": limits,
                "source": "conversation",
                "startupEpoch": startup_epoch,
            })
            .to_string();
            let updated = tx.execute(
                "UPDATE tasks SET token_budget_limit=?1,cost_budget_nanos_usd=?2,
                    deadline_at_ms=?3,execution_config_json=?4,budget_version=budget_version+1,
                    updated_at=?5 WHERE id=?6 AND parent_task_id IS NULL AND root_task_id=id",
                params![
                    limits.token_limit,
                    limits.cost_limit_nanos_usd,
                    limits.deadline_at_ms,
                    execution_config_json,
                    time::format_rfc3339_micros(time::now_millis()),
                    run_id,
                ],
            )?;
            if updated != 1 {
                return Err(DbError::Invalid(
                    "ROOT_BUDGET_INITIALIZATION_FAILED".to_owned(),
                ));
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// 写入根/子 Run 记录（旧 `RunTracker.startRun` L43-44 → `control.start`）。
    ///
    /// **Run 必须先落库，工具阶段的授权祖先链才能解析**：`zk-authz` 的
    /// `AuthorizationSubjectResolver::load_root` 逐层上溯 `run_envelopes`，缺行
    /// 即 `Run ancestry contains a missing parent` 并拒绝全部工具调用。
    ///
    /// `parent_run_id` 目前生产调用方（`engine.rs::prepare_run`）恒传 `None`
    /// （根 Run）。子代理（`agent_type="subagent"`）需把父 run 沿调用链传入，
    /// 属后续扩展点——参数已就位，无需改签名。
    ///
    /// # Errors
    /// 见 [`start_in_current_write`]。
    pub async fn start_run(
        &self,
        run_id: &str,
        session_id: &str,
        parent_run_id: Option<&str>,
        agent_type: Option<&str>,
        model: &str,
    ) -> Result<(), DbError> {
        let run_id = run_id.to_owned();
        let session_id = session_id.to_owned();
        let parent_run_id = parent_run_id.map(str::to_owned);
        let agent_type = agent_type.map(str::to_owned);
        let model = model.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            start_in_current_write(
                &tx,
                &run_id,
                &session_id,
                parent_run_id.as_deref(),
                agent_type.as_deref(),
                &model,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Persist the latest observed turn count while a Run is still active.
    /// Terminalization remains owned by the atomic `TaskResult` commit.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Invalid`] for a negative count or [`DbError`] when
    /// the database update fails.
    pub async fn update_run_turn_count(
        &self,
        run_id: &str,
        turn_count: i64,
    ) -> Result<(), DbError> {
        if turn_count < 0 {
            return Err(DbError::Invalid("RUN_TURN_COUNT_NEGATIVE".to_owned()));
        }
        let run_id = run_id.to_owned();
        self.with_writer(move |conn| {
            conn.execute(
                "UPDATE run_envelopes SET turn_count=?1,updated_at=?2,version=version+1
                 WHERE id=?3 AND status NOT IN ('completed','failed','cancelled','interrupted')",
                params![
                    turn_count,
                    time::format_rfc3339_micros(time::now_millis()),
                    run_id
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// Run 正常完成时落终态（旧 `RunTracker.completeRun` L74-89 的
    /// `control.complete` 部分）。
    ///
    /// 旧源在此之前有 `executions.beginCompletion` + `awaitQuiescence`，未静默
    /// 则改走 `terminate(TOOL_TERMINATION_UNCONFIRMED, ...)`；同 R-04，属
    /// M-RUN，未移植。旧源的 `RunCompletedEvent` 发布亦属 M-RUN
    /// （zkcode 无该事件的订阅方）。
    ///
    /// # Errors
    /// 见 [`transition_in_current_write`]。
    pub async fn complete_run(
        &self,
        run_id: &str,
        tokens: i64,
        cost: f64,
        turns: i64,
    ) -> Result<TransitionResult, DbError> {
        let run_id = run_id.to_owned();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let result = complete_in_current_write(&tx, &run_id, tokens, cost, turns)?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }

    /// Run 异常结束时落终态（旧 `RunTracker.failRun` L91-93 /
    /// `abortRun` L95-107 → `RunTerminationCoordinator`）。
    ///
    /// `exit_reason` 取 [`EXIT_INTERNAL_ERROR`]（旧 `failRun`）或
    /// [`EXIT_USER_CANCELLED`]（旧 `abortRun` 的 `USER_INTERRUPT` /
    /// `SUBMIT_INTERRUPT` → `cancelByUser`）。
    ///
    /// # Errors
    /// 见 [`transition_in_current_write`]。
    pub async fn terminate_run(
        &self,
        run_id: &str,
        exit_reason: &str,
        detail: Option<&str>,
    ) -> Result<TransitionResult, DbError> {
        let run_id = run_id.to_owned();
        let exit_reason = exit_reason.to_owned();
        let detail = detail.map(str::to_owned);
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let result = terminate_in_current_write(&tx, &run_id, &exit_reason, detail.as_deref())?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }

    /// 把**已在 `cancelling`** 的 Run 推到终态（旧
    /// `RunTerminationCoordinator.terminate` L69-73）。
    ///
    /// 供已自行完成第一步（`requestCancel` + 级联取消待决交互）的调用方收尾——
    /// 即 `zk-server` 的 `RunTerminationCoordinator`，其第一步走
    /// `DurableInteractionService::begin_run_termination`（旧源 L41-42 同构）。
    /// 单步调用方请用 [`Self::terminate_run`]。
    ///
    /// # Errors
    /// 见 [`transition_in_current_write`]。
    pub async fn finish_run(
        &self,
        run_id: &str,
        exit_reason: &str,
        detail: Option<&str>,
    ) -> Result<TransitionResult, DbError> {
        let run_id = run_id.to_owned();
        let exit_reason = exit_reason.to_owned();
        let detail = detail.map(str::to_owned);
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let result = finish_in_current_write(&tx, &run_id, &exit_reason, detail.as_deref())?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
}

// ─── Run 只读视图查询（M-RUN 读端：`RunController` 三读端点的 SQL 权威）───
//
// 偏离留痕 R-04：旧 `RunEnvelopeRepository.ROW_MAPPER` 读列后经
// `Instant.parse(...).toString()` 再规范化时间戳；本实现直接透传存储的
// RFC 3339 文本（见 [`start_in_current_write`]），与 `ProjectRecord.created_at`
// 约定一致——避免解析/再序列化引入格式漂移与无谓开销。

/// `RunController` 三读端点的运行信封行视图（旧 `run/RunEnvelope.java` record，
/// 字段序与 JSON 键名逐字对齐）。
///
/// - Jackson `NON_NULL`：null 字段整体剥离（`skip_serializing_if`）。
/// - `status` / `verification_status` / 退出原因均按 V4 公共契约原样返回
///   lowerCamelCase。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEnvelopeView {
    /// Run 主键。
    pub id: String,
    /// 归属会话。
    pub session_id: String,
    /// Stable logical task across attempts.
    pub task_id: String,
    /// One-based execution attempt.
    pub attempt: i64,
    /// Service-start generation that claimed this attempt.
    pub startup_epoch: i64,
    /// Latest typed checkpoint, when one has been committed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_id: Option<String>,
    /// 父 Run（子代理场景，null 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    /// Canonical lower-camel Run status.
    pub status: String,
    /// 代理类型（`query` / `subagent`，null 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_type: Option<String>,
    /// 模型标识（NOT NULL）。
    pub model: String,
    /// 提示词哈希（null 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_hash: Option<String>,
    /// 开始时刻（RFC 3339 文本）。
    pub started_at: String,
    /// 结束时刻（未终态时 null 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    /// 中止原因（null 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abort_reason: Option<String>,
    /// 累计 token。
    pub total_tokens: i64,
    /// 累计成本（美元）。
    pub total_cost_usd: f64,
    /// 工具调用计数。
    pub tool_call_count: i64,
    /// 轮次计数。
    pub turn_count: i64,
    /// 错误摘要（null 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_summary: Option<String>,
    /// 创建时刻（RFC 3339 文本）。
    pub created_at: String,
    /// 更新时刻（RFC 3339 文本）。
    pub updated_at: String,
    /// 乐观锁版本。
    pub version: i64,
    /// Canonical lower-camel exit reason（null 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_reason: Option<String>,
    /// Canonical lower-camel requested exit reason（null 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_exit_reason: Option<String>,
    /// Canonical lower-camel verification status.
    pub verification_status: String,
    /// Orthogonal external-resource cleanup state.
    pub cleanup_status: String,
    /// Direct physical model input tokens attributed to this run.
    pub input_tokens: i64,
    /// Direct physical model output tokens attributed to this run.
    pub output_tokens: i64,
    /// Direct cache-read tokens.
    pub cache_read_tokens: i64,
    /// Direct cache-create tokens.
    pub cache_create_tokens: i64,
    /// Exact cost ledger projection in billionths of one USD.
    pub cost_nanos_usd: i64,
    /// False when any physical call omitted usage or pricing.
    pub usage_complete: bool,
    /// 终态落定时刻（null 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_at: Option<String>,
    /// 等待原因（null 剥离）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub waiting_reason: Option<String>,
}

impl RunEnvelopeView {
    /// 是否已达终态（旧 `RunStatus.terminal()`）。
    ///
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        is_terminal(&self.status)
    }
}

/// `RunController.getEvents` 的事件行视图（旧 `run/RunEvent.java` record）。
///
/// `event_data` 为 JSON 文本，按字符串（转义）序列化，与旧 `String eventData`
/// 的 Jackson 输出一致。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEventView {
    /// 自增主键。
    pub id: i64,
    /// 归属 Run。
    pub run_id: String,
    /// 序号（游标）。
    pub seq: i64,
    /// 事件类型。
    pub event_type: String,
    /// 事件载荷（JSON 文本）。
    pub event_data: String,
    /// 事件时刻（epoch 毫秒）。
    pub ts: i64,
}

/// Durable ownership returned after one outbound WS event has been inserted.
/// `id` is the global `run_event_log` cursor and is therefore the only valid
/// `eventId` for a Run-attributed production frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsOutboxEvent {
    /// Global auto-increment outbox cursor.
    pub id: i64,
    /// Sequence local to the source Run.
    pub seq: i64,
    /// Persisted event time in epoch milliseconds.
    pub ts: i64,
    /// Visible root Session receiving the frame.
    pub root_session_id: String,
    /// Root Task owning the projection.
    pub root_task_id: String,
    /// Current Run of the root Task.
    pub root_run_id: String,
    /// Task that physically produced the event.
    pub source_task_id: String,
    /// Run that physically produced the event.
    pub source_run_id: String,
}

/// One replayable websocket outbox row with its original runtime ownership.
#[derive(Debug, Clone, PartialEq)]
pub struct WsReplayEvent {
    /// Global `run_event_log.id`, used verbatim as the v4 `eventId`.
    pub id: i64,
    /// Persisted event time in epoch milliseconds.
    pub ts: i64,
    /// Visible root Session receiving the frame.
    pub root_session_id: String,
    /// Root Task owning the projection.
    pub root_task_id: String,
    /// Root Run owning the projection.
    pub root_run_id: String,
    /// Task that physically produced the event.
    pub source_task_id: String,
    /// Run that physically produced the event.
    pub source_run_id: String,
    /// Related tool invocation when present.
    pub tool_use_id: Option<String>,
    /// Original flattened [`ServerMessage`](zk_protocol::ServerMessage) payload.
    pub payload: Value,
}

/// 行 → [`RunEnvelopeView`]（旧 `RunEnvelopeRepository.ROW_MAPPER`）。
pub(crate) fn map_envelope_row(row: &Row<'_>) -> rusqlite::Result<RunEnvelopeView> {
    let status: String = row.get("status")?;
    let verification_status: String = row.get("verification_status")?;
    let exit_reason: Option<String> = row.get("exit_reason")?;
    let requested_exit_reason: Option<String> = row.get("requested_exit_reason")?;
    Ok(RunEnvelopeView {
        id: row.get("id")?,
        session_id: row.get("session_id")?,
        task_id: row.get("task_id")?,
        attempt: row.get("attempt")?,
        startup_epoch: row.get("startup_epoch")?,
        checkpoint_id: row.get("checkpoint_id")?,
        parent_run_id: row.get("parent_run_id")?,
        status,
        agent_type: row.get("agent_type")?,
        model: row.get("model")?,
        prompt_hash: row.get("prompt_hash")?,
        started_at: row.get("started_at")?,
        finished_at: row.get("finished_at")?,
        abort_reason: row.get("abort_reason")?,
        total_tokens: row.get("total_tokens")?,
        total_cost_usd: row.get("total_cost_usd")?,
        tool_call_count: row.get("tool_call_count")?,
        turn_count: row.get("turn_count")?,
        error_summary: row.get("error_summary")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        version: row.get("version")?,
        exit_reason,
        requested_exit_reason,
        verification_status,
        cleanup_status: row.get("cleanup_status")?,
        input_tokens: row.get("input_tokens")?,
        output_tokens: row.get("output_tokens")?,
        cache_read_tokens: row.get("cache_read_tokens")?,
        cache_create_tokens: row.get("cache_create_tokens")?,
        cost_nanos_usd: row.get("cost_nanos_usd")?,
        usage_complete: row.get::<_, i64>("usage_complete")? != 0,
        terminal_at: row.get("terminal_at")?,
        waiting_reason: row.get("waiting_reason")?,
    })
}

/// 行 → [`RunEventView`]（旧 `RunEventRepository.ROW_MAPPER`）。
fn map_event_row(row: &Row<'_>) -> rusqlite::Result<RunEventView> {
    Ok(RunEventView {
        id: row.get("id")?,
        run_id: row.get("run_id")?,
        seq: row.get("seq")?,
        event_type: row.get("event_type")?,
        event_data: row.get("event_data")?,
        ts: row.get("ts")?,
    })
}

impl Db {
    /// Resolve a source Session/Task/Run, insert the outbound payload into the
    /// transactional Run outbox, and return its global row identity.
    ///
    /// Explicit Run/Task hints win over the source Session. This is required
    /// for terminal events, which are emitted after their Run stops being
    /// active. A route mismatch fails closed rather than leaking a child event
    /// into an unrelated root Session.
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] when attribution is invalid, the payload cannot be
    /// persisted, or the outbox transaction fails.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn append_ws_outbox_event(
        &self,
        root_session_id: &str,
        source_session_id: &str,
        source_task_id_hint: Option<&str>,
        source_run_id_hint: Option<&str>,
        event_type: &str,
        tool_use_id: Option<&str>,
        payload: &Value,
    ) -> Result<Option<WsOutboxEvent>, DbError> {
        let root_session_id = root_session_id.to_owned();
        let source_session_id = source_session_id.to_owned();
        let source_task_id_hint = source_task_id_hint.map(str::to_owned);
        let source_run_id_hint = source_run_id_hint.map(str::to_owned);
        let event_type = event_type.to_owned();
        let tool_use_id = tool_use_id.map(str::to_owned);
        let payload = payload.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            let source: Option<(String, String, String)> = if let Some(run_id) =
                source_run_id_hint.as_deref()
            {
                tx.query_row(
                    "SELECT r.id,r.task_id,t.root_task_id
                     FROM run_envelopes r JOIN tasks t ON t.id=r.task_id
                     WHERE r.id=?1",
                    params![run_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?
            } else if let Some(task_id) = source_task_id_hint.as_deref() {
                tx.query_row(
                    "SELECT r.id,t.id,t.root_task_id
                     FROM tasks t JOIN run_envelopes r ON r.id=t.current_run_id
                     WHERE t.id=?1",
                    params![task_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?
            } else {
                let invocation_source = if let Some(tool_use_id) = tool_use_id.as_deref() {
                    // Tool payloads may contain taskId/runId fields describing the
                    // object operated on (Agent and Task* are the important cases).
                    // Once admitted, resolve the actor exclusively from the durable
                    // invocation ledger scoped to the emitting transcript Session.
                    tx.query_row(
                        "SELECT r.id,r.task_id,t.root_task_id
                         FROM tool_invocations invocation
                         JOIN run_envelopes r ON r.id=invocation.run_id
                         JOIN tasks t ON t.id=invocation.task_id
                         WHERE invocation.tool_use_id=?1 AND r.session_id=?2
                         ORDER BY invocation.created_at DESC,invocation.invocation_id DESC LIMIT 1",
                        params![tool_use_id, source_session_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()?
                } else {
                    None
                };
                if invocation_source.is_some() {
                    invocation_source
                } else {
                    // `tool_use_start` is the preparing phase and intentionally
                    // precedes invocation admission. Its actor is nevertheless
                    // provable from the source transcript's current Run. Falling
                    // back here keeps start/input/result in one caller partition.
                    tx.query_row(
                        "SELECT r.id,r.task_id,t.root_task_id
                         FROM run_envelopes r JOIN tasks t ON t.id=r.task_id
                         WHERE r.session_id=?1
                         ORDER BY CASE WHEN r.status IN
                             ('queued','running','waitingDependencies','waitingInteraction','cancelling')
                             THEN 0 ELSE 1 END, r.started_at DESC, r.id DESC LIMIT 1",
                        params![source_session_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()?
                }
            };
            let Some((source_run_id, source_task_id, root_task_id)) = source else {
                return Ok(None);
            };
            let root: Option<(String, Option<String>)> = tx
                .query_row(
                    "SELECT session_id,current_run_id FROM tasks WHERE id=?1",
                    params![root_task_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((owned_session_id, root_run_id)) = root else {
                return Err(DbError::Invalid("WS_OUTBOX_ROOT_TASK_NOT_FOUND".to_owned()));
            };
            if owned_session_id != root_session_id {
                return Err(DbError::Invalid("WS_OUTBOX_ROUTE_MISMATCH".to_owned()));
            }
            let Some(root_run_id) = root_run_id else {
                return Err(DbError::Invalid("WS_OUTBOX_ROOT_RUN_NOT_FOUND".to_owned()));
            };

            // WS rows are a replay outbox rather than a diagnostic preview. Keep the
            // complete payload: applying the generic 10 KiB diagnostic truncation
            // would turn a successfully published tool result into an unreplayable row.
            let seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(seq),0)+1 FROM run_event_log WHERE run_id=?1",
                params![source_run_id],
                |row| row.get(0),
            )?;
            let mut envelope = Map::new();
            envelope.insert("schemaVersion".to_owned(), json!(4));
            envelope.insert("entityId".to_owned(), json!(source_run_id));
            if let Some(tool_use_id) = tool_use_id.as_deref() {
                envelope.insert("toolUseId".to_owned(), json!(tool_use_id));
            }
            envelope.insert("data".to_owned(), payload);
            let ts = time::now_millis();
            tx.execute(
                "INSERT INTO run_event_log(run_id,seq,event_type,event_data,ts)
                 VALUES(?1,?2,?3,?4,?5)",
                params![
                    source_run_id,
                    seq,
                    event_type,
                    Value::Object(envelope).to_string(),
                    ts
                ],
            )?;
            let id = tx.last_insert_rowid();
            tx.commit()?;
            Ok(Some(WsOutboxEvent {
                id,
                seq,
                ts,
                root_session_id,
                root_task_id,
                root_run_id,
                source_task_id,
                source_run_id,
            }))
        })
        .await
    }

    /// Read the finite websocket outbox delta committed after `after_event_id`.
    ///
    /// The high-water mark and rows are selected in one read transaction. Events
    /// committed after that snapshot are delivered through the already-bound live
    /// connection, so reconnect recovery neither chases an unbounded stream nor
    /// leaves a bind-time gap. Only the latest root Run tree is included because
    /// `session_restored` already materializes all earlier attempts.
    ///
    /// # Errors
    /// Returns [`DbError`] for invalid persisted payloads or `SQLite` failures.
    pub async fn get_ws_outbox_events_after(
        &self,
        root_session_id: &str,
        after_event_id: i64,
    ) -> Result<Vec<WsReplayEvent>, DbError> {
        let root_session_id = root_session_id.to_owned();
        self.with_reader(move |conn| {
            let tx = conn.transaction()?;
            let root: Option<(String, String)> = tx
                .query_row(
                    "SELECT r.id,r.task_id FROM run_envelopes r
                     WHERE r.session_id=?1 AND r.parent_run_id IS NULL
                     ORDER BY r.started_at DESC,r.id DESC LIMIT 1",
                    params![root_session_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((root_run_id, root_task_id)) = root else {
                tx.commit()?;
                return Ok(Vec::new());
            };
            let through_event_id: i64 = tx.query_row(
                "WITH RECURSIVE tree(id) AS (
                     SELECT ?1
                     UNION ALL
                     SELECT child.id FROM run_envelopes child
                     JOIN tree parent ON child.parent_run_id=parent.id
                 )
                 SELECT COALESCE(MAX(event.id),0) FROM run_event_log event
                 JOIN tree ON tree.id=event.run_id
                 WHERE event.event_type LIKE 'ws_%'",
                params![root_run_id],
                |row| row.get(0),
            )?;
            if through_event_id <= after_event_id {
                tx.commit()?;
                return Ok(Vec::new());
            }
            let mut stmt = tx.prepare(
                "WITH RECURSIVE tree(id) AS (
                     SELECT ?1
                     UNION ALL
                     SELECT child.id FROM run_envelopes child
                     JOIN tree parent ON child.parent_run_id=parent.id
                 )
                 SELECT event.id,event.ts,run.task_id,run.id,event.event_data
                 FROM run_event_log event
                 JOIN tree ON tree.id=event.run_id
                 JOIN run_envelopes run ON run.id=event.run_id
                 WHERE event.event_type LIKE 'ws_%'
                   AND event.id>?2 AND event.id<=?3
                 ORDER BY event.id",
            )?;
            let rows = stmt
                .query_map(
                    params![root_run_id, after_event_id.max(0), through_event_id],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    },
                )?
                .collect::<Result<Vec<_>, _>>()?;
            drop(stmt);
            let mut events = Vec::with_capacity(rows.len());
            for (id, ts, source_task_id, source_run_id, event_data) in rows {
                let mut envelope: Value = serde_json::from_str(&event_data).map_err(|error| {
                    DbError::Invalid(format!("WS_OUTBOX_PAYLOAD_INVALID:{id}:{error}"))
                })?;
                let tool_use_id = envelope
                    .get("toolUseId")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let payload = envelope
                    .get_mut("data")
                    .map(Value::take)
                    .ok_or_else(|| DbError::Invalid(format!("WS_OUTBOX_PAYLOAD_MISSING:{id}")))?;
                events.push(WsReplayEvent {
                    id,
                    ts,
                    root_session_id: root_session_id.clone(),
                    root_task_id: root_task_id.clone(),
                    root_run_id: root_run_id.clone(),
                    source_task_id,
                    source_run_id,
                    tool_use_id,
                    payload,
                });
            }
            tx.commit()?;
            Ok(events)
        })
        .await
    }

    /// 旧 `RunEnvelopeRepository.findById`（`SELECT * ... WHERE id = ?`）：
    /// 命中返回视图，未命中返回 `None`。只读路径。
    ///
    /// # Errors
    /// 底层查询失败时返回 [`DbError`]。
    pub async fn find_run_by_id(&self, run_id: &str) -> Result<Option<RunEnvelopeView>, DbError> {
        let run_id = run_id.to_owned();
        self.with_reader(move |conn| {
            conn.query_row(
                "SELECT * FROM run_envelopes WHERE id = ?1",
                params![run_id],
                map_envelope_row,
            )
            .optional()
            .map_err(Into::into)
        })
        .await
    }

    /// 旧 `RunEnvelopeRepository.findBySession`（`... WHERE session_id = ?
    /// ORDER BY started_at DESC LIMIT ?`）：按开始时刻倒序取会话最近 Run。
    /// 只读路径。
    ///
    /// # Errors
    /// 底层查询失败时返回 [`DbError`]。
    pub async fn find_runs_by_session(
        &self,
        session_id: &str,
        limit: i64,
    ) -> Result<Vec<RunEnvelopeView>, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT * FROM run_envelopes WHERE session_id = ?1 \
                 ORDER BY started_at DESC LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(params![session_id, limit], map_envelope_row)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    /// Read a complete recursive run tree, including child runs whose
    /// transcripts live in internal sessions. Session-scoped queries cannot
    /// provide this projection because internal child runs intentionally use a
    /// different `session_id` from the root transcript.
    ///
    /// # Errors
    /// Returns [`DbError`] when the recursive `SQLite` query fails.
    pub async fn find_run_tree(&self, root_run_id: &str) -> Result<Vec<RunEnvelopeView>, DbError> {
        let root_run_id = root_run_id.to_owned();
        self.with_reader(move |conn| {
            let mut stmt = conn.prepare(
                "WITH RECURSIVE run_tree(id, depth) AS (
                     SELECT id, 0 FROM run_envelopes WHERE id = ?1
                     UNION ALL
                     SELECT child.id, parent.depth + 1
                     FROM run_envelopes child
                     JOIN run_tree parent ON child.parent_run_id = parent.id
                 )
                 SELECT run_envelopes.*
                 FROM run_envelopes
                 JOIN run_tree ON run_tree.id = run_envelopes.id
                 ORDER BY run_tree.depth ASC,
                          run_envelopes.started_at ASC,
                          run_envelopes.id ASC",
            )?;
            stmt.query_map([root_run_id], map_envelope_row)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await
    }

    /// Return the most recently started root run for one session.
    ///
    /// This is the durable source for the global workbench task list: child
    /// agent runs must never replace their owning root run in that projection.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` query fails.
    pub async fn find_latest_root_run_by_session(
        &self,
        session_id: &str,
    ) -> Result<Option<RunEnvelopeView>, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| {
            conn.query_row(
                "SELECT * FROM run_envelopes WHERE session_id = ?1 \
                 AND parent_run_id IS NULL ORDER BY started_at DESC LIMIT 1",
                [session_id],
                map_envelope_row,
            )
            .optional()
            .map_err(Into::into)
        })
        .await
    }

    /// Append a durable run event using the same sequence and truncation rules as lifecycle writes.
    ///
    /// # Errors
    /// Returns [`DbError`] when event serialization, validation, or the `SQLite`
    /// transaction fails.
    pub async fn append_run_event(
        &self,
        run_id: &str,
        event_type: &str,
        tool_use_id: Option<&str>,
        data: &Value,
    ) -> Result<(), DbError> {
        let run_id = run_id.to_owned();
        let event_type = event_type.to_owned();
        let tool_use_id = tool_use_id.map(str::to_owned);
        let data = data.clone();
        self.with_writer(move |conn| {
            let tx = conn.transaction()?;
            append_event_in_current_write(
                &tx,
                &run_id,
                &event_type,
                tool_use_id.as_deref(),
                &data,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// 旧 `RunEventRepository.getEvents`（`... WHERE run_id = ? AND seq > ?
    /// ORDER BY seq ASC LIMIT ?`）：增量拉取事件流。只读路径。
    ///
    /// # Errors
    /// 底层查询失败时返回 [`DbError`]。
    pub async fn get_run_events(
        &self,
        run_id: &str,
        after_seq: i64,
        limit: i64,
    ) -> Result<Vec<RunEventView>, DbError> {
        let run_id = run_id.to_owned();
        self.with_reader(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT * FROM run_event_log WHERE run_id = ?1 AND seq > ?2 \
                 ORDER BY seq ASC LIMIT ?3",
            )?;
            let rows = stmt
                .query_map(params![run_id, after_seq, limit], map_event_row)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AGENT_TYPE_QUERY, EXIT_INTERNAL_ERROR, EXIT_MODEL_FINISHED, EXIT_USER_CANCELLED,
        STATUS_RUNNING, TransitionResult,
    };
    use crate::Db;

    /// `run_envelopes` 起始行的读回视图（逐列对齐旧 `RunEnvelope.start`）。
    struct StartedRow {
        status: String,
        version: i64,
        parent_run_id: Option<String>,
        agent_type: Option<String>,
        model: String,
        started_at: String,
        created_at: String,
        updated_at: String,
        total_tokens: i64,
        total_cost_usd: f64,
        tool_call_count: i64,
        turn_count: i64,
        verification_status: String,
        exit_reason: Option<String>,
        terminal_at: Option<String>,
    }

    fn read_row(db: &Db, run_id: &str) -> StartedRow {
        let run_id = run_id.to_owned();
        db.with_conn_blocking(move |conn| {
            conn.query_row(
                "SELECT status,version,parent_run_id,agent_type,model,started_at,created_at,\
                        updated_at,total_tokens,total_cost_usd,tool_call_count,turn_count,\
                        verification_status,exit_reason,terminal_at \
                 FROM run_envelopes WHERE id=?1",
                rusqlite::params![run_id],
                |row| {
                    Ok(StartedRow {
                        status: row.get(0)?,
                        version: row.get(1)?,
                        parent_run_id: row.get(2)?,
                        agent_type: row.get(3)?,
                        model: row.get(4)?,
                        started_at: row.get(5)?,
                        created_at: row.get(6)?,
                        updated_at: row.get(7)?,
                        total_tokens: row.get(8)?,
                        total_cost_usd: row.get(9)?,
                        tool_call_count: row.get(10)?,
                        turn_count: row.get(11)?,
                        verification_status: row.get(12)?,
                        exit_reason: row.get(13)?,
                        terminal_at: row.get(14)?,
                    })
                },
            )
            .map_err(Into::into)
        })
        .expect("run row readable")
    }

    fn event_types(db: &Db, run_id: &str) -> Vec<String> {
        let run_id = run_id.to_owned();
        db.with_conn_blocking(move |conn| {
            let mut stmt =
                conn.prepare("SELECT event_type FROM run_event_log WHERE run_id=?1 ORDER BY seq")?;
            let rows = stmt
                .query_map(rusqlite::params![run_id], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .expect("events readable")
    }

    async fn seeded() -> (Db, String) {
        let db = Db::open_in_memory().expect("boot");
        let session = db
            .create_session("claude-sonnet-4-5", "/tmp/zkcode-run-tests")
            .await
            .expect("session");
        (db, session.id)
    }

    /// 起始行逐列对齐旧 `RunEnvelope.start`（L48-54）+ `RunControlService.start`
    /// 的 INSERT（L79-90）：`running` / version 0 / 累计量全 0 /
    /// `not_requested` / 三时间戳同刻 / `run_started` 事件。
    #[tokio::test]
    async fn start_run_writes_java_baseline_row() {
        let (db, session_id) = seeded().await;
        db.start_run(
            "run-1",
            &session_id,
            None,
            Some(AGENT_TYPE_QUERY),
            "claude-sonnet-4-5",
        )
        .await
        .expect("start");

        let row = read_row(&db, "run-1");
        assert_eq!(row.status, "running");
        assert_eq!(row.version, 0);
        assert_eq!(row.parent_run_id, None);
        assert_eq!(row.agent_type.as_deref(), Some("query"));
        assert_eq!(row.model, "claude-sonnet-4-5");
        assert_eq!(row.total_tokens, 0);
        assert!(row.total_cost_usd.abs() < f64::EPSILON);
        assert_eq!(row.tool_call_count, 0);
        assert_eq!(row.turn_count, 0);
        assert_eq!(row.verification_status, "notRequested");
        assert_eq!(row.exit_reason, None);
        assert_eq!(row.terminal_at, None);
        // 旧 RunEnvelope.start 用单一 Instant now 填三个时间戳。
        assert_eq!(row.started_at, row.created_at);
        assert_eq!(row.started_at, row.updated_at);
        assert_eq!(event_types(&db, "run-1"), vec!["run_started".to_owned()]);
    }

    /// P0 回归护栏：授权祖先链（`zk-authz` 的 `load_root`）依赖 `run_envelopes`
    /// 有行。缺行时 `SELECT` 返回 0 行 → 上层报
    /// `Run ancestry contains a missing parent` 并拒绝全部工具调用；`start_run`
    /// 之后同一查询必须命中且 `parent_run_id` 为 `NULL`（根 Run 终止上溯）。
    #[tokio::test]
    async fn started_run_resolves_authorization_ancestry() {
        let (db, session_id) = seeded().await;
        let lookup = |db: &Db| {
            db.with_conn_blocking(|conn| {
                let found = conn
                    .query_row(
                        "SELECT session_id,parent_run_id FROM run_envelopes WHERE id=?1",
                        rusqlite::params!["run-anc"],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
                    )
                    .ok();
                Ok(found)
            })
            .expect("lookup")
        };
        assert!(lookup(&db).is_none(), "缺 run 行即触发 P0 缺父错误");

        db.start_run("run-anc", &session_id, None, Some(AGENT_TYPE_QUERY), "m")
            .await
            .expect("start");
        let (found_session, found_parent) = lookup(&db).expect("祖先链可解析");
        assert_eq!(found_session, session_id);
        assert_eq!(found_parent, None, "根 Run 的 parent 为 NULL，上溯在此终止");
    }

    /// 旧 `start` L69-77 的两条前置校验错误码。
    #[tokio::test]
    async fn start_run_rejects_missing_session_and_parent() {
        let (db, session_id) = seeded().await;
        let missing_session = db
            .start_run(
                "run-x",
                "no-such-session",
                None,
                Some(AGENT_TYPE_QUERY),
                "m",
            )
            .await
            .expect_err("root session must exist");
        assert!(
            missing_session
                .to_string()
                .contains("RUN_ROOT_SESSION_NOT_FOUND"),
            "unexpected: {missing_session}"
        );
        let missing_parent = db
            .start_run(
                "run-y",
                &session_id,
                Some("no-such-run"),
                Some("subagent"),
                "m",
            )
            .await
            .expect_err("parent run must exist");
        assert!(
            missing_parent.to_string().contains("RUN_PARENT_NOT_FOUND"),
            "unexpected: {missing_parent}"
        );
    }

    /// 旧 `complete` L124-128：`running` → `completed` / `modelFinished` /
    /// 累计三列覆盖（`CASE WHEN ?>0`）/ 终态时间戳非空 / version+1。
    #[tokio::test]
    async fn complete_run_records_totals_and_terminal_stamp() {
        let (db, session_id) = seeded().await;
        db.start_run("run-2", &session_id, None, Some(AGENT_TYPE_QUERY), "m")
            .await
            .expect("start");

        let result = db
            .complete_run("run-2", 4321, 0.0, 3)
            .await
            .expect("complete");
        assert_eq!(result, TransitionResult::Applied);

        let row = read_row(&db, "run-2");
        assert_eq!(row.status, "completed");
        assert_eq!(row.exit_reason.as_deref(), Some(EXIT_MODEL_FINISHED));
        assert_eq!(row.total_tokens, 4321);
        assert_eq!(row.turn_count, 3);
        // 旧源 cost 传 0.0 时 `CASE WHEN ?>0` 保留原值（QueryEngine 恒传 0.0）。
        assert!(row.total_cost_usd.abs() < f64::EPSILON);
        assert!(
            row.terminal_at.is_some(),
            "终态必须落 terminal_at（DDL CHECK）"
        );
        assert_eq!(row.version, 1);
        assert_eq!(
            event_types(&db, "run-2"),
            vec!["run_started".to_owned(), "run_status_changed".to_owned()]
        );

        let public = db
            .find_run_by_id("run-2")
            .await
            .expect("public projection")
            .expect("completed run");
        let json = serde_json::to_value(public).expect("serialize public projection");
        assert_eq!(json["status"], "completed");
        assert_eq!(json["verificationStatus"], "notRequested");
        assert_eq!(json["exitReason"], EXIT_MODEL_FINISHED);

        // 终态后再 complete 不改状态（旧 ALREADY_TERMINAL）。
        assert_eq!(
            db.complete_run("run-2", 1, 0.0, 1).await.expect("second"),
            TransitionResult::AlreadyTerminal
        );
        assert_eq!(read_row(&db, "run-2").total_tokens, 4321);
    }

    /// 旧 `RunTerminationCoordinator.terminate` 非用户取消分支（L72）：
    /// `requestCancel` → `cancelling` → `fail`，两次迁移故 version 增 2。
    #[tokio::test]
    async fn terminate_run_failure_path_records_error_summary() {
        let (db, session_id) = seeded().await;
        db.start_run("run-3", &session_id, None, Some(AGENT_TYPE_QUERY), "m")
            .await
            .expect("start");

        let result = db
            .terminate_run("run-3", EXIT_INTERNAL_ERROR, Some("provider stream failed"))
            .await
            .expect("terminate");
        assert_eq!(result, TransitionResult::Applied);

        let row = read_row(&db, "run-3");
        assert_eq!(row.status, "failed");
        assert_eq!(row.exit_reason.as_deref(), Some(EXIT_INTERNAL_ERROR));
        assert!(row.terminal_at.is_some());
        assert_eq!(row.version, 2, "requestCancel + fail 两次迁移");

        let summary: Option<String> = db
            .with_conn_blocking(|conn| {
                conn.query_row(
                    "SELECT error_summary FROM run_envelopes WHERE id='run-3'",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .expect("summary");
        assert_eq!(summary.as_deref(), Some("provider stream failed"));
    }

    /// 旧 `terminate` 用户取消分支（L71）→ `runs.cancel`：`cancelled` /
    /// `userCancelled` 退出原因 + 同值 `abort_reason`。
    #[tokio::test]
    async fn terminate_run_user_cancel_path_matches_java_literals() {
        let (db, session_id) = seeded().await;
        db.start_run("run-4", &session_id, None, Some(AGENT_TYPE_QUERY), "m")
            .await
            .expect("start");

        assert_eq!(
            db.terminate_run("run-4", EXIT_USER_CANCELLED, None)
                .await
                .expect("terminate"),
            TransitionResult::Applied
        );
        let row = read_row(&db, "run-4");
        assert_eq!(row.status, "cancelled");
        assert_eq!(row.exit_reason.as_deref(), Some(EXIT_USER_CANCELLED));

        let (abort, requested): (Option<String>, Option<String>) = db
            .with_conn_blocking(|conn| {
                conn.query_row(
                    "SELECT abort_reason,requested_exit_reason FROM run_envelopes WHERE id='run-4'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(Into::into)
            })
            .expect("reasons");
        assert_eq!(abort.as_deref(), Some(EXIT_USER_CANCELLED));
        assert_eq!(requested.as_deref(), Some(EXIT_USER_CANCELLED));
    }

    #[tokio::test]
    async fn snake_case_exit_reason_is_rejected_without_mutating_the_run() {
        let (db, session_id) = seeded().await;
        db.start_run(
            "run-invalid-exit",
            &session_id,
            None,
            Some(AGENT_TYPE_QUERY),
            "m",
        )
        .await
        .expect("start");

        let error = db
            .terminate_run(
                "run-invalid-exit",
                "internal_error",
                Some("legacy spelling"),
            )
            .await
            .expect_err("snake_case exit reason must be rejected");
        assert!(
            error
                .to_string()
                .contains("RUN_EXIT_REASON_INVALID:internal_error")
        );
        let row = read_row(&db, "run-invalid-exit");
        assert_eq!(row.status, STATUS_RUNNING);
        assert_eq!(row.version, 0);
        assert!(row.exit_reason.is_none());
    }

    /// 旧 `transition` 首条 SELECT 空结果 → `NOT_FOUND`；终态后再终止 →
    /// `requestCancel` 即返回 `ALREADY_TERMINAL`，不二次落终态（旧 L43-46）。
    #[tokio::test]
    async fn transitions_on_unknown_and_terminal_runs_are_inert() {
        let (db, session_id) = seeded().await;
        assert_eq!(
            db.terminate_run("ghost", EXIT_INTERNAL_ERROR, Some("x"))
                .await
                .expect("terminate"),
            TransitionResult::NotFound
        );
        assert_eq!(
            db.complete_run("ghost", 1, 0.0, 1).await.expect("complete"),
            TransitionResult::NotFound
        );

        db.start_run("run-5", &session_id, None, Some(AGENT_TYPE_QUERY), "m")
            .await
            .expect("start");
        db.complete_run("run-5", 10, 0.0, 1)
            .await
            .expect("complete");
        assert_eq!(
            db.terminate_run("run-5", EXIT_INTERNAL_ERROR, Some("late"))
                .await
                .expect("terminate"),
            TransitionResult::AlreadyTerminal
        );
        let row = read_row(&db, "run-5");
        assert_eq!(row.status, "completed", "终态不可被后续终止改写");
        assert_eq!(row.version, 1);
    }

    /// 旧 `appendEventInCurrentWrite` L268-272：`tool_started` / `tool_call`
    /// 事件自增 `tool_call_count`，其它事件不增。
    #[tokio::test]
    async fn tool_events_increment_tool_call_count() {
        let (db, session_id) = seeded().await;
        db.start_run("run-6", &session_id, None, Some(AGENT_TYPE_QUERY), "m")
            .await
            .expect("start");
        db.with_conn_blocking(|conn| {
            let tx = conn.transaction()?;
            super::append_event_in_current_write(
                &tx,
                "run-6",
                "tool_started",
                Some("tu-1"),
                &serde_json::json!({"name": "Bash"}),
            )?;
            super::append_event_in_current_write(
                &tx,
                "run-6",
                "tool_finished",
                Some("tu-1"),
                &serde_json::json!({"ok": true}),
            )?;
            tx.commit()?;
            Ok(())
        })
        .expect("events");
        assert_eq!(read_row(&db, "run-6").tool_call_count, 1);
    }

    #[tokio::test]
    async fn production_root_run_requires_a_deadline_but_not_spend_limits() {
        let (db, session_id) = seeded().await;
        let run_id = uuid::Uuid::new_v4().to_string();
        let limits = crate::TaskBudgetLimits {
            token_limit: Some(500_000),
            cost_limit_nanos_usd: Some(4_000_000_000),
            deadline_at_ms: Some(crate::time::now_millis() + 60_000),
        };
        db.start_root_run_with_budget(
            &run_id,
            &session_id,
            Some(AGENT_TYPE_QUERY),
            "qwen3.8-max-0902",
            &limits,
        )
        .await
        .expect("atomic root start");

        let budget = db
            .read_task_budget(&run_id)
            .await
            .expect("budget query")
            .expect("root task");
        assert_eq!(budget.token_limit, limits.token_limit);
        assert_eq!(budget.cost_limit_nanos_usd, limits.cost_limit_nanos_usd);
        assert_eq!(budget.deadline_at_ms, limits.deadline_at_ms);
        let task = db
            .find_runtime_task_by_id(&run_id)
            .await
            .expect("task query")
            .expect("task");
        assert!(task.execution_config_json.contains("\"budget\""));

        let unlimited_id = uuid::Uuid::new_v4().to_string();
        let unlimited = crate::TaskBudgetLimits {
            token_limit: None,
            cost_limit_nanos_usd: None,
            deadline_at_ms: Some(crate::time::now_millis() + 60_000),
        };
        db.start_root_run_with_budget(
            &unlimited_id,
            &session_id,
            Some(AGENT_TYPE_QUERY),
            "qwen3.8-max-0902",
            &unlimited,
        )
        .await
        .expect("spend-unlimited root with deadline");
        let persisted = db
            .read_task_budget(&unlimited_id)
            .await
            .expect("unlimited budget query")
            .expect("unlimited root task");
        assert_eq!(persisted.token_limit, None);
        assert_eq!(persisted.cost_limit_nanos_usd, None);
        assert_eq!(persisted.deadline_at_ms, unlimited.deadline_at_ms);

        let missing = db
            .start_root_run_with_budget(
                &uuid::Uuid::new_v4().to_string(),
                &session_id,
                Some(AGENT_TYPE_QUERY),
                "qwen3.8-max-0902",
                &crate::TaskBudgetLimits::default(),
            )
            .await
            .expect_err("production root without deadline must fail");
        assert!(missing.to_string().contains("ROOT_BUDGET_NOT_CONFIGURED"));
    }

    /// 旧 `serializeEvent` L288-300：超 `MAX_EVENT_BYTES` 时 `data` 换为截断
    /// 摘要（`payloadSha256` + 前 2048 字节预览），信封 `schemaVersion=2`。
    #[test]
    fn oversized_event_payload_is_truncated_with_digest() {
        let payload = serde_json::json!({ "blob": "x".repeat(20 * 1024) });
        let text = super::serialize_event("run-7", Some("tu-9"), &payload);
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert_eq!(parsed["schemaVersion"], 2);
        assert_eq!(parsed["entityId"], "run-7");
        assert_eq!(parsed["toolUseId"], "tu-9");
        assert_eq!(parsed["data"]["truncated"], true);
        assert_eq!(
            parsed["data"]["payloadSha256"]
                .as_str()
                .expect("digest")
                .len(),
            64
        );
        assert!(
            parsed["data"]["payloadPreview"]
                .as_str()
                .expect("preview")
                .len()
                <= 2048
        );

        let small = super::serialize_event("run-7", None, &serde_json::json!({"a": 1}));
        let parsed: serde_json::Value = serde_json::from_str(&small).expect("json");
        assert_eq!(parsed["data"]["a"], 1);
        assert!(parsed.get("toolUseId").is_none());
    }
}
