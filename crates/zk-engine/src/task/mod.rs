//! 任务系统——对照旧 `tool/task/` 包。
//!
//! # 模块导航
//!
//! - [`state`]：`TaskStatus` 枚举 + `TaskState` 状态记录
//!   （对照旧 `TaskStatus.java` + `TaskState.java`）。
//! - [`coordinator`]：任务编排服务（对照旧 `TaskCoordinator.java`）。
//!
//! # 依赖方向
//!
//! 本模块位于 zk-engine 内部。`TaskCoordinator` 的下行推送经
//! [`crate::MessageSink`] 窄接口（与引擎同款解耦模式）。任务持久化
//! 经 [`zk_db::Db`] 的 `tasks` 表（由 `zk-db/src/task.rs` 提供仓储）。

pub mod coordinator;
pub mod state;

pub use coordinator::TaskCoordinator;
pub use state::{TaskState, TaskStatus};
