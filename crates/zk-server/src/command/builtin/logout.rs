//! `/logout`——注销当前认证（旧 `LogoutCommand.java`）。
//!
//! 类型 `LOCAL`：直接回显注销结果文本。

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/logout` 命令。
pub(super) struct LogoutCommand;

impl Command for LogoutCommand {
    fn name(&self) -> &'static str {
        "logout"
    }

    fn description(&self) -> &'static str {
        "Sign out of your account"
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
            if ctx.is_authenticated {
                tracing::info!(session_id = %ctx.session_id, "Logout requested");
                CommandResult::text("Successfully logged out.")
            } else {
                CommandResult::text("Not currently authenticated.")
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    #[tokio::test]
    async fn logout_when_not_authenticated() {
        let ctx = CommandContext::of("s-1", "/tmp", "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry.find_command("logout").expect("registered");
        let result = command.execute("", &ctx).await;
        assert_eq!(result, CommandResult::text("Not currently authenticated."));
    }
}
