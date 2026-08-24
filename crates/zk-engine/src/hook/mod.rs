//! Hook 系统（Batch 8B / WP-12）：事件通知与工具执行前安全裁决。
//!
//! 对照旧 `com.aicodeassistant.hook` 包。核心语义边界（详见各子模块偏离留痕）：
//! 通知 / 展示 hook 仍是错误隔离的外部副作用；PRE transform / security hook
//! 可以安全改写或拒绝工具输入。改写值始终被视为不可信，并在工具执行前重新经过
//! [`crate::admission::ToolAdmission`] 的完整路径、命令和敏感数据检查。Security
//! hook 的配置、执行或协议错误失败关闭。
//!
//! 组成：
//! - [`event`]：[`HookEvent`] 8 观测点 + [`HookConfig`] 声明数据模型。
//! - [`registry`]：[`HookRegistry`] 按事件索引，从 `.zk/hooks.toml` 加载。
//! - [`service`]：[`HookService::fire`] 触发 + 本地命令（sync/async）分派 +
//!   [`HookContext`] 上下文。
//! - [`http_executor`]：[`HttpHookExecutor`] HTTP 通道 + SSRF 防护（H-02）。

pub mod event;
pub mod http_executor;
pub mod registry;
pub mod service;

pub use event::{HookConfig, HookEvent, HookRole};
pub use http_executor::{HttpHookError, HttpHookExecutor, is_blocked_address};
pub use registry::HookRegistry;
pub use service::{HookContext, HookService, PreHookDecision};
