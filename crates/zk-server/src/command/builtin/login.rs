//! `/login`——OAuth 登录或 API Key 输入（旧 `LoginCommand.java`）。
//!
//! 类型 `LOCAL_JSX`：前端渲染 API Key 输入对话框或切换账户界面。
//! `isSensitive() = true`：日志中不记录参数。

use futures::future::BoxFuture;
use serde_json::json;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/login` 命令。
pub(super) struct LoginCommand;

impl Command for LoginCommand {
    fn name(&self) -> &'static str {
        "login"
    }

    fn description(&self) -> &'static str {
        "Sign in to your account"
    }

    fn command_type(&self) -> CommandType {
        CommandType::LocalJsx
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            if ctx.is_authenticated {
                CommandResult::jsx(json!({
                    "action": "switchAccount",
                    "currentlyAuthenticated": true
                }))
            } else {
                tracing::info!("Login requested — showing API key input");
                CommandResult::jsx(json!({
                    "action": "login",
                    "currentlyAuthenticated": false
                }))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(session_id: &str, state: AppState) -> CommandResult {
        let ctx = CommandContext::of(session_id, "/tmp", "kimi-k3", state);
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry.find_command("login").expect("registered");
        command.execute("", &ctx).await
    }

    #[tokio::test]
    async fn unauthenticated_shows_login_action() {
        let result = run("s-1", AppState::for_tests()).await;
        match result {
            CommandResult::Jsx(data) => {
                assert_eq!(data["action"], "login");
                assert_eq!(data["currentlyAuthenticated"], false);
            }
            other => panic!("expected JSX, got {other:?}"),
        }
    }
}
