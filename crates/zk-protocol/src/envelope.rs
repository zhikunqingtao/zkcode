//! WebSocket 消息信封（U1 扁平格式，`docs/architecture.md` 决策 #1）。
//!
//! ## 下行形状（与旧系统线上格式逐字段一致）
//!
//! ```jsonc
//! {
//!   "type": "...",          // 顶层 type（ServerMessage serde tag）
//!   "ts": 1755000000000,    // 毫秒时间戳
//!   "seq": 42,              // [zkcode 新增] 顶层递增序列号（增量兼容，前端不消费不报错）
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
use serde::{Deserialize, Serialize};

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
        Self {
            msg,
            ts,
            seq,
            session_id: None,
            binding_epoch: None,
        }
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
