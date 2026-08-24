//! zk-protocol：zkcode 协议契约层——WebSocket 消息（**57 下行 + 15 上行**，
//! 含 42 record + 17 直推的收敛并集）与领域模型的全量建模，是跨 crate 通信的
//! 单一事实来源（D5）。
//!
//! 9-crate 拓扑定位：最底层契约 crate，被 `zk-db` / `zk-llm` / `zk-engine` / `zk-server`
//! 依赖，自身不依赖任何兄弟 crate。REST 契约由 utoipa 从本 crate 正向生成。
//!
//! # 命名规则（契约冻结）
//!
//! - **variant**：PascalCase；variant 名经 `#[serde(tag = "type",
//!   rename_all = "snake_case")]` 自动派生的 tag 值与旧白名单 type 字符串
//!   **逐字一致**（如 `StreamDelta` → `stream_delta`），禁止手写 `#[serde(rename)]`
//!   偏离白名单（会破坏 `kind()` 一致性核查）。
//! - **字段**：Rust 侧 `snake_case`，经 variant 级 `rename_all = "camelCase"` 输出
//!   旧线上 JSON 的 camelCase（如 `tool_use_id` → `toolUseId`）。
//! - **type**：旧名 `snake_case` 直取白名单字符串（下行 57 条 =
//!   `stompClient.ts` `VALID_MESSAGE_TYPES` 逐字清单；上行 15 条对齐改造方案
//!   §11.2 + 4 个 Map 入口语义名）。
//! - **数值**：忠实旧类型——Java `int`/`long` → `i64`，`double` → `f64`，
//!   可空引用 → `Option`；`Option` 缺省时 `skip_serializing_if` 不输出键
//!   （对齐旧 Jackson 仅在非空时 put 的行为）。
//!
//! # 信封（U1 扁平格式）
//!
//! 下行 `{type, ts, seq?, ...字段平铺, _sessionId?, _bindingEpoch?}`，与旧系统
//! 线上格式一致；上行同扁平（新协议以顶层 `type` 为路由键）。见 [`envelope`]。
//!
//! # 三类 channel 预留说明
//!
//! 旧系统推送目的地共三类：`/user/queue/messages`（主下行）、
//! `/topic/mcp/progress`（MCP 进度广播）、`/user/queue/coordinator/{sessionId}`
//!（Coordinator/Swarm 图事件）。Phase 2+ 多路复用改造时，将以**顶层可选字段
//! `channel`** 引入信封（默认缺省 = `main`），消息类型本身不变——本 Phase 1
//! 契约不出现该字段（U1 边界），扩展点已在 [`envelope::ServerEnvelope`] 的
//! 字段布局中预留（新字段只增不改，增量兼容）。
//!
//! # 模块导航
//!
//! - [`model`]：`Message` / `ContentBlock` / `Usage` / `FlexEpoch`（领域模型）。
//! - [`server_message`]：`ServerMessage`（57 variant）+ 6 个共享 struct。
//! - [`client_message`]：`ClientMessage`（16 variant，含 D9 修复位）。
//! - [`envelope`]：`ServerEnvelope` / `ClientEnvelope`（U1 扁平信封）。
//! - [`error`]：`ProtocolError`（未知 type = 反序列化错误的约定载体）。

pub mod client_message;
pub mod envelope;
pub mod error;
pub mod model;
pub mod server_message;

pub use client_message::{Attachment, ClientMessage, Reference};
pub use envelope::{ClientEnvelope, ServerEnvelope};
pub use error::ProtocolError;
pub use model::{ContentBlock, FlexEpoch, Message, Usage};
pub use server_message::{
    ElicitationOption, InteractionView, McpToolInfo, ServerMessage, SessionMetadata,
    ToolResultContent, WorkerSnapshot,
};

/// 旧前端 `VALID_MESSAGE_TYPES` 白名单的逐字快照（57 条）。
///
/// 用途：`tests/roundtrip.rs` 逐条断言 [`ServerMessage::kind`] 的输出域与白名单
/// **完全相等**（无缺失、无多余），保证契约不漂移。此常量本身不做运行时路由，
/// 仅为契约自描述。数组顺序与旧源码 `stompClient.ts` L22-42 声明顺序一致。
pub const VALID_SERVER_MESSAGE_TYPES: [&str; 57] = [
    "stream_delta",
    "thinking_delta",
    "tool_use_start",
    "tool_use_input",
    "tool_use_progress",
    "tool_result",
    "error",
    "compact_complete",
    "message_complete",
    "compact_start",
    "rate_limit",
    "permission_request",
    "tool_permission_denied",
    "cost_update",
    "task_update",
    "agent_spawn",
    "agent_update",
    "agent_complete",
    "agent_started",
    "agent_completed",
    "agent_failed",
    "elicitation",
    "prompt_suggestion",
    "speculation_result",
    "bridge_status",
    "notification",
    "teammate_message",
    "mcp_tool_update",
    "mcp_tool_progress",
    "mcp_health_status",
    "session_restored",
    "pong",
    "compact_event",
    "token_warning",
    "interrupt_ack",
    "run_input_queued",
    "run_input_applied",
    "run_input_rejected",
    "model_changed",
    "model_routed",
    "permission_mode_changed",
    "command_result",
    "rewind_complete",
    "token_budget_nudge",
    "plan_update",
    "swarm_state_update",
    "worker_progress",
    "workflow_phase_update",
    "session_list_updated",
    "visualization",
    "verification_result",
    "verify_progress",
    "verify_attention",
    "protocol_error",
    "interaction_created",
    "interaction_terminal",
    "interaction_updated",
];
