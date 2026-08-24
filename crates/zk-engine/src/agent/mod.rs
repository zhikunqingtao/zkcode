//! 子代理核心引擎——对照旧 `tool/agent/` 包。

#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_panics_doc)]
//!
//! # 模块导航
//!
//! - [`types`]：数据模型（`IsolationMode` / `AgentStatus` / `AgentRequest` /
//!   `AgentResult` / `AgentDefinition`，对照旧 `SubAgentExecutor` 内部类）。
//! - [`worktree`]：Git Worktree 管理器（对照旧 `WorktreeManager.java`）。
//! - [`executor`]：子代理执行器（对照旧 `SubAgentExecutor.executeSync`）。
//! - [`tracker`]：后台代理追踪器（对照旧 `BackgroundAgentTracker.java`）。
//!
//! # 依赖方向
//!
//! 本模块位于 zk-engine 内部，不依赖 zk-server。子代理的引擎创建经
//! [`executor::SubAgentEngineFactory`] trait 反转注入（解耦循环依赖：
//! SubAgentExecutor 需要创建子代理引擎实例，但 Engine 自身不持有
//! SubAgentExecutor 引用以避免循环）。

pub mod executor;
pub mod tracker;
pub mod types;
pub mod worktree;

pub use executor::{
    AgentMailboxMessage, AgentMailboxRouter, AgentTimeoutConfig, ChildExecutionContext,
    RealSubAgentEngineFactory, SUB_AGENT_TOOL_NAMES, SubAgentEngineFactory, SubAgentExecutor,
    build_sub_agent_registry, build_sub_agent_registry_with_policy,
};
pub use tracker::{AgentTrackingState, BackgroundAgentTracker};
pub use types::{
    AgentDefinition, AgentRequest, AgentResult, AgentStatus, IsolationMode, MAX_RESULT_SIZE_CHARS,
};
pub use worktree::{GitCommandOutput, GitCommandRunner, SystemGitCommandRunner, WorktreeManager};
