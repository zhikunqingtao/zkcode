//! `/exit`——退出应用（Batch 8A）。
//!
//! 语义来源（旧仓库只读）：`ExitCommand.java`（27L，`LOCAL`）——记一行日志后
//! 返回 `"Goodbye!"`；注释明确「Web 场景: 关闭 STOMP 连接，由前端处理重定向」，
//! 即服务端不主动断连。
//!
//! # 有意差异
//!
//! 无。任务书草案写「发送 WS 关闭消息 + 清理会话资源」，本端不实现：
//!
//! - **不由服务端发关闭帧**：连接归属前端，旧侧同样把断连交给前端；服务端单方
//!   面关帧会触发前端自动重连逻辑（`wsClient` 断线重试），出现「点了 /exit 又
//!   连上来」的怪相；
//! - **不额外清理会话资源**：会话状态是持久化的（`sessions` / `messages` 表），
//!   连接级资源（订阅、心跳、绑定）已由 [`crate::ws`] 的断连回收路径统一释放，
//!   命令里再清一遍等于两处事实源。

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/exit` 命令。
pub(super) struct ExitCommand;

impl Command for ExitCommand {
    fn name(&self) -> &'static str {
        "exit"
    }

    fn description(&self) -> &'static str {
        "Exit the application"
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
            // 旧 `log.info("Exit requested for session: {}", context.sessionId())`。
            tracing::info!(session_id = %ctx.session_id, "Exit requested");
            CommandResult::text("Goodbye!")
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::command::traits::{CommandResult, CommandType};
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    /// 旧文案逐字：`LOCAL` + `"Goodbye!"`，且不受参数影响。
    #[tokio::test]
    async fn returns_the_legacy_goodbye_text() {
        let ctx = CommandContext::of("s-exit", "/tmp", "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry.find_command("exit").expect("registered");
        assert_eq!(command.command_type(), CommandType::Local);
        for args in ["", "now", "  --force"] {
            match command.execute(args, &ctx).await {
                CommandResult::Text(text) => assert_eq!(text, "Goodbye!"),
                other => panic!("expected text for {args:?}, got {other:?}"),
            }
        }
    }
}
