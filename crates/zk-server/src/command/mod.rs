//! 斜杠命令域（Batch 3 起：命令框架 + 内建命令族，Batch 8A 达 21 个）。
//!
//! 语义来源（旧仓库只读，`backend/src/main/java/com/aicodeassistant/command/`）：
//!
//! | 本模块 | 旧类 |
//! |---|---|
//! | [`traits`] | `Command.java` / `CommandType.java` / `CommandResult.java` |
//! | [`registry`] | `CommandRegistry.java` |
//! | [`context`] | `CommandContext.java` |
//! | [`builtin`] | `command/impl/*.java`（21 个，清单见 [`builtin`]） |
//!
//! 上行入口是 WS 的 `slash_command`（[`crate::ws::inbound`]），对照旧
//! `WebSocketController.handleSlashCommand` L1341-1443：注册表取命令 → 构造
//! 上下文 → `execute` → 按 [`CommandResult`] 分支推下行。`/skill` 不走本域
//! （3B.7 已有独立拦截路径，保持不动）。
//!
//! # 本批未移植的旧成员（留痕）
//!
//! - `Command` 接口的 `isMcp()` / `getLoadedFrom()` / `isSensitive()` /
//!   `getAvailability()` 与 `CommandAvailability` 枚举——只有 MCP 命令族与
//!   CLI 非交互模式消费，两者均归后续 Batch；
//! - `PromptCommand` 接口的扩展字段——`getContentLength()` /
//!   `getArgNames()` / `getAllowedTools()` / `getModel()` / `getSource()`
//!   未建模：zkcode 只有单一 [`Command`] trait，而旧侧这些字段的唯一消费点是
//!   `/help <name>` 详情块的 `Content:` / `Tools:` 两行与
//!   `executePromptCommand` 的工具/模型覆盖；本域三个 `PROMPT` 命令
//!   （`/git-review`、`/init`、`/retry`）在旧侧均未声明工具/模型覆盖，故执行
//!   语义无差，仅 `/help` 详情少两行（见 [`builtin`] 的 `help` 留痕）；
//! - `CommandRegistry` 的动态命令源与远程/桥接安全集——见 [`registry`] 模块
//!   文档的偏离表。

mod builtin;
mod context;
mod registry;
mod traits;

pub use context::CommandContext;
pub use registry::{CommandNotFound, CommandRegistry};
pub use traits::{Command, CommandResult, CommandType};
