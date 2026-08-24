//! ws——S8 原生 WebSocket 通道（替换旧 SockJS/STOMP 体系，方案 §4.2/D9/D1）。
//!
//! # 架构（对照旧 `WebSocketController` + `WebSocketSessionManager`）
//!
//! ```text
//! inbound(ClientMessage 16 variant)              outbound(ServerMessage 54 variant)
//!        │                                              ▲
//!        ▼                                              │ push(session_id, msg)
//! connection.rs 读循环 ──► inbound.rs 分发 ──► hub.rs WsHub ──► mpsc(双档背压)
//!                          │   bind/ping/ack 本地处理     │
//!                          └── 其余 ──► EngineHook(S9 挂点)│
//!                                                       ▼
//!                                              connection.rs 写循环（10s Ping 心跳）
//! ```
//!
//! - **端点**：`GET /ws`（axum 原生 WS upgrade，非 SockJS）；Router 层
//!   `localnet_guard` 对升级请求同样生效（D7 层级 1；层级 2/3 的 Bearer/Cookie
//!   与 REST 侧一致 Phase 1 未启用，见 S7 `api::system`）。
//! - **principal**：连接内 UUID（`ConnId`），替代旧 Spring 用户名体系——
//!   localhost 单用户模式下连接即主体。
//! - **会话绑定**：`bind_session` 上行绑定连接到应用会话；`bindingEpoch`
//!   客户端提供、服务端单调校验（旧 `STALE_BINDING_EPOCH` 语义）；换绑 =
//!   再次 bind 到新会话；unbind = 连接关闭清理（16 variant 契约无 unbind
//!   上行，对齐旧系统——旧 `unbindSession` 亦为内部方法）。
//! - **背压双档**（D9，方案 §4.3 决策）：delta 类 `try_send` 满则丢弃+计数；
//!   critical 类 `send` 带 200ms 超时，超时则该订阅者转 degraded（后续
//!   critical 全入 per-session pending，等 bind 重放）。
//! - **心跳双轨**（D1）：写循环 10s WS 协议层 Ping 帧（旧 STOMP heartbeat
//!   10s 语义）；应用层 `ping` 上行 → `pong` 下行（未绑定时携带
//!   `bindRequired`，对齐旧 `handlePing` L1550-1566）。
//! - **可观测**：连接数 gauge / 每类消息收发计数 / delta 丢弃 / critical
//!   超时 / pending 深度与重放计数（metrics facade，§17）；日志不落消息体。
//!
//! # 模块导航
//!
//! - [`config`]：`WsConfig`（心跳 / TTL / cleanup / 背压参数，测试可注入）。
//! - [`hub`]：`WsHub`（注册表 / 会话路由 / epoch / pending / push API）。
//! - [`connection`]：升级握手与读写双循环。
//! - [`inbound`]：16 variant 上行分发与 bind 握手。
//! - [`restore`]：`session_restored` 组装（`MessageRecord` → 协议 `Message`）。
//! - [`delivery`]：交互投递闸门（INITIAL/RETRY 档 + 启动期对账与两个定时任务）。
//! - `metrics`：WS 打点辅助（进程内 recorder 注册）。

pub mod config;
pub mod connection;
pub mod delivery;
pub mod hub;
pub mod inbound;
mod metrics;
pub mod restore;

use zk_protocol::ClientMessage;

pub use config::WsConfig;
pub use delivery::{
    InteractionDelivery, deliver_interaction, spawn_interaction_lifecycle,
    spawn_interaction_redelivery,
};
pub use hub::{BindError, WsHub};

/// WS 协议版本（旧 `WebSocketController.WS_PROTOCOL_VERSION`）。
pub const WS_PROTOCOL_VERSION: i64 = 3;

/// 旧 `WebSocketController.CRITICAL_MESSAGE_TYPES`（L254-260）**15 条**逐字快照
///（任务规格表述「14 种」与旧源码实查不符，以代码为准：旧集合实为 15 条）。
///
/// 其中 `tool_finished` / `run_completed` / `run_failed` 不在 zk-protocol 54
/// variant 集合内（旧系统幽灵类型，无直推调用点），字符串保留以忠实旧集，
/// 实际不可达——与 54 variant 的有效交集为 12 条。
///
/// 语义：critical = 离线/背压时必须暂存重放（per-session pending 队列）；
/// 其余（含 `stream_delta` / `thinking_delta` 高频 delta 类）= 可丢弃。
pub const CRITICAL_MESSAGE_TYPES: [&str; 15] = [
    "permission_request",
    "tool_result",
    "tool_finished",
    "message_complete",
    "run_completed",
    "run_failed",
    "error",
    "cost_update",
    "interaction_created",
    "interaction_terminal",
    "interaction_updated",
    "permission_mode_changed",
    "tool_use_start",
    "run_input_applied",
    "run_input_rejected",
];

/// 消息类型是否属于 critical 档（旧 `isCriticalMessage` 语义）。
#[must_use]
pub fn is_critical_message(kind: &str) -> bool {
    CRITICAL_MESSAGE_TYPES.contains(&kind)
}

/// S9 引擎挂点——上行 chat/控制类消息的转发接口。
///
/// # 形态裁定（object-safe 同步入口）
///
/// 与 D-S6-1（zk-llm trait 风格）一致：不使用 async fn in trait，接口为
/// object-safe 同步方法；实现方需要异步处理时自行 spawn 或内部走 channel。
/// S9 zk-engine 接入时经 [`WsHub::set_engine`] 注入真实实现——实现体持
/// `WsHub` 克隆即可把下行响应 push 回来（hub 的 `push` 即引擎侧唯一推送
/// 入口），天然完成「engine ↔ ws」双向解耦。
///
/// Phase 1 默认 [`NoopEngineHook`]：解析成功即 debug 日志（对齐任务规格
/// 「其余解析成功即回 appropriate ack 或忽略并 debug 日志」）。
pub trait EngineHook: Send + Sync {
    /// 已绑定会话的连接收到非通道类上行消息（`user_message` / `interrupt` /
    /// `set_model` 等 13 种）。`bind_session` / `ping` / `interaction_ack`
    /// 由 ws 层本地处理，不经此接口。
    fn on_client_message(&self, session_id: &str, message: ClientMessage);
}

/// Phase 1 空实现（S9 前的占位挂点）。
pub struct NoopEngineHook;

impl EngineHook for NoopEngineHook {
    fn on_client_message(&self, session_id: &str, message: ClientMessage) {
        tracing::debug!(
            session_id,
            kind = message.kind(),
            "engine hook not attached yet (S9 pending), message acknowledged and dropped"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// critical 集合与旧源码逐字互锁（15 条 + 3 条幽灵类型说明）。
    #[test]
    fn critical_set_matches_legacy_snapshot() {
        for kind in CRITICAL_MESSAGE_TYPES {
            assert!(is_critical_message(kind), "{kind} must be critical");
        }
        // 幽灵类型（不在 54 variant，但旧集合收录）。
        assert!(is_critical_message("tool_finished"));
        assert!(is_critical_message("run_completed"));
        assert!(is_critical_message("run_failed"));
        // 高频 delta 类必须是非 critical（可丢弃档）。
        for delta in [
            "stream_delta",
            "thinking_delta",
            "tool_use_progress",
            "pong",
        ] {
            assert!(!is_critical_message(delta), "{delta} must be droppable");
        }
    }
}
