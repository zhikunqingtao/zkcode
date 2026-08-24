//! 持久交互闭环（旧 `interaction/` 包）+ 授权链所需的 Run 生命周期子集。
//!
//! # 模块划分
//!
//! | 模块 | 旧源 | 职责 |
//! |------|------|------|
//! | [`service`] | `interaction/DurableInteractionService.java`（782 行） | 交互落库 / 投递代次 / ACK / 决策 CAS / 期限过期 / 容量核对 |
//! | [`runs`] | `run/RunControlService.java`（340 行，**子集**） | `run_envelopes` 状态迁移 + `run_event_log` 事件信封 |
//! | [`elicitation`] | `engine/ElicitationService.java`（141 行） | `ELICITATION` 发问的持久实现（[`zk_tools::ElicitationSink`] 端口） |
//!
//! # 闭环边界
//!
//! 本模块只负责**持久层与状态机**；WS 下行由 [`crate::ws`] 的
//! [`WsInteractionPublisher`] 适配，工具执行前拦截由 [`crate::engine_bridge`] 注入，
//! [`RunTerminationRequest`] 端口的生产实现在 [`crate::run_termination`]。
//! 依赖方向：`zk-server::interaction → (zk-authz, zk-db, zk-protocol)`。

pub mod elicitation;
pub mod runs;
pub mod service;

pub use elicitation::DurableElicitationSink;
pub use runs::{RunEvents, TransitionResult};
pub use service::{
    ACK_WINDOW_SECONDS, CancellationResult, DECISION_SECONDS, DELIVERY_WINDOW_SECONDS,
    DurableInteractionService, InteractionCreateSpec, InteractionPublisher, MAX_WAITING,
    NoopInteractionPublisher, RunTerminationRequest,
};
