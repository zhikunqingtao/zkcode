//! `/plan [on|off]`——显示或隐藏规划面板，不改变会话权限。
//!
//! 用法：
//! - `/plan on [planName]` — 显示规划面板；
//! - `/plan off` — 隐藏规划面板；
//! - `/plan [planName]` — 显示规划面板。

use futures::future::BoxFuture;
use zk_protocol::ServerMessage;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/plan` 命令。
pub(super) struct PlanCommand;

impl Command for PlanCommand {
    fn name(&self) -> &'static str {
        "plan"
    }

    fn description(&self) -> &'static str {
        "Show or hide the planning UI panel; session permissions are unchanged"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Local
    }

    fn execute<'a>(
        &'a self,
        args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            let trimmed = args.trim();

            let name = plan_name(trimmed);
            let output = if trimmed == "off" {
                "Planning panel closed. Session permissions are unchanged.".to_owned()
            } else if trimmed.split_whitespace().next() == Some("on") {
                format!("Planning panel opened: {name}. Session permissions are unchanged.")
            } else {
                "Planning panel opened. Session permissions are unchanged.".to_owned()
            };
            let open = trimmed != "off";
            ctx.state
                .hub
                .push(
                    &ctx.session_id,
                    ServerMessage::PlanUpdate {
                        is_plan_mode: open,
                        plan_name: open.then(|| name.to_owned()),
                        plan_overview: open.then(String::new),
                    },
                )
                .await;
            CommandResult::text(output)
        })
    }
}

fn plan_name(args: &str) -> &str {
    let name = if args.split_whitespace().next() == Some("on") {
        args[2..].trim()
    } else {
        args
    };
    if name.is_empty() { "New Plan" } else { name }
}

#[cfg(test)]
mod tests {
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(args: &str) -> CommandResult {
        let ctx = CommandContext::of("s-1", "/tmp", "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let plan = registry.find_command("plan").expect("registered");
        plan.execute(args, &ctx).await
    }

    #[tokio::test]
    async fn plan_on_returns_enabled_text() {
        let result = run("on My Plan").await;
        assert_eq!(
            result,
            CommandResult::text(
                "Planning panel opened: My Plan. Session permissions are unchanged."
            )
        );
    }

    #[tokio::test]
    async fn plan_on_without_name_uses_default() {
        let result = run("on").await;
        assert_eq!(
            result,
            CommandResult::text(
                "Planning panel opened: New Plan. Session permissions are unchanged."
            )
        );
    }

    #[tokio::test]
    async fn plan_off_returns_disabled_text() {
        let result = run("off").await;
        assert_eq!(
            result,
            CommandResult::text("Planning panel closed. Session permissions are unchanged.")
        );
    }

    #[tokio::test]
    async fn plan_no_args_opens_panel() {
        let result = run("").await;
        assert_eq!(
            result,
            CommandResult::text("Planning panel opened. Session permissions are unchanged.")
        );
    }

    #[test]
    fn names_starting_with_on_are_not_truncated() {
        assert_eq!(super::plan_name("onboarding"), "onboarding");
        assert_eq!(super::plan_name("on\tNamed plan"), "Named plan");
    }
}
