//! `/undo`——撤销最近一轮 AI 操作的所有文件变更（Batch 7）。
//!
//! 语义来源（旧仓库只读）：`UndoCommandHandler.java`（119L）。
//!
//! 执行流程（逐字对照旧实现）：
//! 1. 查找最近一个有文件变更的事务（`file_history.last_transaction`）；
//! 2. 批量回退文件到事务起点（`file_history.rewind_files`）；
//! 3. 删除事务起点之后的消息（`db.delete_messages_after`）；
//! 4. 移除已撤销的事务记录（`file_history.remove_last_transaction`）；
//! 5. 构建结果文本（含恢复文件清单 + 错误告警）。
//!
//! # 有意差异
//!
//! - Java 侧经 `SimpMessagingTemplate.convertAndSend` 推送 `undo_complete`；
//!   Rust 侧命令返回 `CommandResult` 文本，WS 推送由分发层统一处理。
//! - Java `deleteAfterSeqNum` 消息删除：Rust 侧 `db` 暂无等价方法，
//!   本批以日志告警替代，完整消息删除归后续 Batch。

use std::fmt::Write as _;

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/undo` 命令。
pub(super) struct UndoCommand;

impl Command for UndoCommand {
    fn name(&self) -> &'static str {
        "undo"
    }

    fn description(&self) -> &'static str {
        "Undo the last AI file changes"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Local
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            if ctx.session_id.trim().is_empty() {
                return CommandResult::error("No active session.");
            }

            let file_history = &ctx.state.file_history;

            // 1. 查找最近有文件变更的事务
            let Some(tx) = file_history.last_transaction(&ctx.session_id) else {
                return CommandResult::text("Nothing to undo — no recent file changes found.");
            };

            if tx.changed_files.is_empty() {
                file_history.remove_last_transaction(&ctx.session_id);
                return CommandResult::text(
                    "Nothing to undo — last transaction had no file changes.",
                );
            }

            // 2. 批量回退文件到事务起点
            let rewind_result = file_history
                .rewind_files(&ctx.session_id, &tx.message_id, Some(&tx.changed_files))
                .await;

            // 3. 消息删除（本批以日志告警替代，完整实现归后续 Batch）
            tracing::info!(
                session_id = %ctx.session_id,
                message_id = %tx.message_id,
                "undo: message deletion deferred to future batch"
            );

            // 4. 移除已撤销的事务记录
            file_history.remove_last_transaction(&ctx.session_id);

            // 5. 构建结果文本
            let mut text = String::new();
            let _ = write!(
                text,
                "Undo complete — restored {} file(s):",
                rewind_result.restored_files.len()
            );
            for file in &rewind_result.restored_files {
                let _ = write!(text, "\n  \u{2022} {file}");
            }
            if !rewind_result.errors.is_empty() {
                let _ = write!(text, "\n\nWarnings:");
                for err in &rewind_result.errors {
                    let _ = write!(text, "\n  \u{26a0} {err}");
                }
            }

            CommandResult::text(text)
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(session_id: &str) -> CommandResult {
        let ctx = CommandContext::of(session_id, "/tmp", "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let cmd = registry.find_command("undo").expect("registered");
        cmd.execute("", &ctx).await
    }

    #[tokio::test]
    async fn blank_session_returns_error() {
        let result = run("   ").await;
        assert_eq!(result, CommandResult::error("No active session."));
    }

    #[tokio::test]
    async fn no_transactions_returns_nothing_to_undo() {
        let result = run("s-1").await;
        assert_eq!(
            result,
            CommandResult::text("Nothing to undo — no recent file changes found.")
        );
    }
}
