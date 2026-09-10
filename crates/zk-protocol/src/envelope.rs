//! WebSocket v4 消息信封。
//!
//! ## 下行形状（与旧系统线上格式逐字段一致）
//!
//! ```jsonc
//! {
//!   "type": "...",          // 顶层 type（ServerMessage serde tag）
//!   "ts": 1755000000000,    // 毫秒时间戳
//!   "seq": 42,              // [zkcode 新增] 顶层递增序列号（增量兼容，前端不消费不报错）
//!   "eventContext": {       // v4 必须的运行时归属与去重信息
//!     "protocolVersion": 4,
//!     "eventId": "...",
//!     "sessionId": "...",
//!     "taskId": "...",
//!     "runId": "...",
//!     "sourceTaskId": "...",
//!     "sourceRunId": "...",
//!     "toolUseId": "..."
//!   },
//!   ...其余字段平铺...,      // variant 字段直接放顶层（serde flatten）
//!   "_sessionId": "...",       // 会话路由标记（可缺省）
//!   "_bindingEpoch": 0         // 连接绑定纪元（可缺省）
//! }
//! ```
//!
//! 来源核验：`WebSocketController.push`（L228-243）按 `type` → `ts` → 字段平铺 →
//! `_sessionId` → `_bindingEpoch` 顺序组装 `LinkedHashMap`；`pushToPrincipal`
//!（L246-252）不含路由字段。`seq` 是 U1 新增字段。
//!
//! ## 未知 type 约定
//!
//! 反序列化遇到未收录 type 直接返回 Err（见 [`crate::error::ProtocolError`]），
//! 由 ws 层捕获后 WARN + 丢弃，语义对齐旧前端白名单跳过行为。
//!
//! ## serde 方案取舍（flatten + internally-tagged 强类型优先）
//!
//! 优先且已采用**强类型枚举方案**：`#[serde(flatten)] msg: ServerMessage` +
//! enum `#[serde(tag = "type")]`。已核查的边角前提：
//! 1. **字段冲突**：envelope 侧字段名（`ts` / `seq` / `_sessionId` /
//!    `_bindingEpoch`）与全部 57 个 variant 的字段名无交集（variant 侧是
//!    `sessionId` 带不带下划线的差异，字符串不同名），无覆盖风险；
//! 2. **推断代价**：flatten + internally-tagged 组合经 serde `Content` 缓冲，
//!    数字保留原精度（u64/i64/f64 不混淆），代价是中转拷贝——对 WS 消息体量
//!    （KB 级）可接受；
//! 3. **备选回退**：若后续出现新的冲突字段（新增 variant 命名为 `ts` 等），
//!    回退两步解析（先解 `{type: String}` 壳再二次 match），当前无此需求，
//!    不预建死代码。
//!
//! ## 已知线上差异记录
//!
//! - `verify_attention` 旧路径（`NotificationService` 直接发 record）**不携带
//!   `ts`**；zkcode 新实现恒发 `ts`。本信封将 `ts` 定为必填强类型——解析旧
//!   存量抓包中的 `verify_attention` 帧会失败，属已知且接受的边界（该类型
//!   Phase 2+ 未激活，届时如需解析旧流量再行决策）。
//! - `swarm_state_update` / `worker_progress` 旧 `pushToUser` record 路径嵌套于
//!   `payload` 键；本信封统一平铺（见 `server_message` 模块文档差异 1）。

use crate::{ClientMessage, ServerMessage};
use serde::{Deserialize, Deserializer, Serialize, de};
use std::sync::atomic::{AtomicU64, Ordering};

/// 当前 WebSocket 信封协议版本。v4 是一次性切换，不再生成 v3 信封。
pub const WS_PROTOCOL_VERSION: u16 = 4;

static EPHEMERAL_EVENT_ID: AtomicU64 = AtomicU64::new(1);

/// 一条下行事件的稳定身份和运行时归属。
///
/// 字段始终被序列化；对不属于 Task/Run/工具的控制事件，相应字段为
/// `null`。投递层应使用事务 outbox ID 构造 `event_id`；
/// [`RuntimeEventContext::ephemeral`] 只是为尚未进入 outbox 的进程内控制事件提供
/// 不重复的降级 ID。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEventContext {
    /// 信封协议版本，恒为 [`WS_PROTOCOL_VERSION`]。
    #[serde(deserialize_with = "deserialize_v4")]
    pub protocol_version: u16,
    /// 全局去重 ID；持久事件使用 outbox ID。
    pub event_id: String,
    /// 对话所属的根 Session。
    pub session_id: Option<String>,
    /// 对话当前投影的 Task。
    pub task_id: Option<String>,
    /// 对话当前投影的 Run。
    pub run_id: Option<String>,
    /// 真正产生该事件的 Task；根 Task 事件与 `task_id` 相同。
    pub source_task_id: Option<String>,
    /// 真正产生该事件的 Run；根 Run 事件与 `run_id` 相同。
    pub source_run_id: Option<String>,
    /// 相关工具调用 ID；非工具事件为 `None`。
    pub tool_use_id: Option<String>,
}

fn deserialize_v4<'de, D>(deserializer: D) -> Result<u16, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u16::deserialize(deserializer)?;
    if version == WS_PROTOCOL_VERSION {
        Ok(version)
    } else {
        Err(de::Error::custom(format!(
            "unsupported websocket protocol version {version}; expected {WS_PROTOCOL_VERSION}",
        )))
    }
}

impl RuntimeEventContext {
    /// 使用持久化 outbox ID 构造权威事件上下文。
    #[must_use]
    pub fn durable(event_id: impl Into<String>) -> Self {
        Self {
            protocol_version: WS_PROTOCOL_VERSION,
            event_id: event_id.into(),
            session_id: None,
            task_id: None,
            run_id: None,
            source_task_id: None,
            source_run_id: None,
            tool_use_id: None,
        }
    }

    /// 构造进程内、不会被重放的控制事件上下文。
    #[must_use]
    pub fn ephemeral(ts: i64, seq: Option<u64>) -> Self {
        let nonce = EPHEMERAL_EVENT_ID.fetch_add(1, Ordering::Relaxed);
        let seq = seq.map_or_else(|| "direct".to_owned(), |value| value.to_string());
        Self::durable(format!("ephemeral:{ts}:{seq}:{nonce}"))
    }

    /// 填充运行时归属。
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn with_actor(
        mut self,
        session_id: Option<String>,
        task_id: Option<String>,
        run_id: Option<String>,
        source_task_id: Option<String>,
        source_run_id: Option<String>,
        tool_use_id: Option<String>,
    ) -> Self {
        self.session_id = session_id;
        self.task_id = task_id;
        self.run_id = run_id;
        self.source_task_id = source_task_id;
        self.source_run_id = source_run_id;
        self.tool_use_id = tool_use_id;
        self
    }
}

/// 下行信封——U1 扁平格式。
///
/// `msg` 展平到顶层（type 与全部 payload 字段）；`ts` / `seq` / 路由字段为
/// envelope 侧字段。序列化键序：type → payload 字段 → ts → seq? → 路由字段?
///（键序由 serde 输出顺序决定，JSON 语义与键序无关；前端消费按键取值）。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServerEnvelope {
    /// 下行消息本体（`type` tag 与 payload 字段全部平铺到信封顶层）。
    #[serde(flatten)]
    pub msg: ServerMessage,
    /// 服务端毫秒时间戳（旧系统 `System.currentTimeMillis()`）。
    pub ts: i64,
    /// 顶层递增序列号（**zkcode 新增**，U1；None 时不出现在 JSON 中）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    /// v4 必须的事件身份和 Task/Run 归属。
    #[serde(rename = "eventContext")]
    pub event_context: RuntimeEventContext,
    /// 会话路由标记（`/user/queue/messages` 会话定向路径携带）。
    #[serde(rename = "_sessionId", skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// 连接绑定纪元（防跨连接错投）。
    #[serde(rename = "_bindingEpoch", skip_serializing_if = "Option::is_none")]
    pub binding_epoch: Option<u64>,
}

/// 上行信封——同样扁平（新协议上行携带顶层 `type` 作为路由键，替代 STOMP
/// destination；见 `client_message` 模块文档）。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientEnvelope {
    /// 上行消息本体（`type` tag 与 payload 字段全部平铺到信封顶层）。
    #[serde(flatten)]
    pub msg: ClientMessage,
}

impl ServerEnvelope {
    /// 返回内嵌消息的 `type` 字符串（与旧白名单逐字一致）。
    #[must_use]
    pub fn kind(&self) -> &'static str {
        self.msg.kind()
    }

    /// 以指定 ts / seq 构造最简信封（无路由字段；ws 层发送前按需补
    /// `_sessionId` / `_bindingEpoch`）。
    #[must_use]
    pub fn new(msg: ServerMessage, ts: i64, seq: Option<u64>) -> Self {
        let tool_use_id = msg.tool_use_id().map(ToOwned::to_owned);
        let mut event_context = RuntimeEventContext::ephemeral(ts, seq);
        event_context.tool_use_id = tool_use_id;
        Self {
            msg,
            ts,
            seq,
            event_context,
            session_id: None,
            binding_epoch: None,
        }
    }

    /// 用持久化运行时事件上下文替换构造器生成的进程内上下文。
    #[must_use]
    pub fn with_event_context(mut self, event_context: RuntimeEventContext) -> Self {
        self.event_context = event_context;
        self
    }
}

impl ClientEnvelope {
    /// 返回内嵌消息的 `type` 字符串（上行路由键）。
    #[must_use]
    pub fn kind(&self) -> &'static str {
        self.msg.kind()
    }

    /// 包装一条上行消息为信封。
    #[must_use]
    pub fn new(msg: ClientMessage) -> Self {
        Self { msg }
    }
}
