//! `/plan [on|off]`——切换 Plan Mode 规划模式（Batch 7）。
//!
//! 语义来源（旧仓库只读）：`PlanCommand.java`（62L）。
//!
//! 用法：
//! - `/plan on [planName]` — 进入规划模式；
//! - `/plan off` — 退出规划模式；
//! - `/plan` — 切换当前模式。
//!
//! # 有意差异
//!
//! - Java 侧经 `WebSocketController.sendPlanUpdate` 推送 WS 事件；
//!   Rust 侧命令返回 `CommandResult` 文本，WS 推送由分发层统一处理。

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/plan` 命令。
pub(super) struct PlanCommand;

impl Command for PlanCommand {
    fn name(&self) -> &'static str {
        "plan"
    }

    fn description(&self) -> &'static str {
        "Toggle Plan Mode for step-by-step task planning"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Local
    }

    fn execute<'a>(
        &'a self,
        args: &'a str,
        _ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            let trimmed = args.trim();

            if trimmed.starts_with("on") {
                let plan_name = trimmed.strip_prefix("on").unwrap_or("").trim();
                let name = if plan_name.is_empty() {
                    "New Plan"
                } else {
                    plan_name
                };
                CommandResult::text(format!("Plan Mode enabled: {name}"))
            } else if trimmed == "off" {
                CommandResult::text("Plan Mode disabled")
            } else {
                // toggle
                let name = if trimmed.is_empty() {
                    "New Plan"
                } else {
                    trimmed
                };
                CommandResult::text(format!("Plan Mode toggled: {name}"))
            }
        })
    }
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
        assert_eq!(result, CommandResult::text("Plan Mode enabled: My Plan"));
    }

    #[tokio::test]
    async fn plan_on_without_name_uses_default() {
        let result = run("on").await;
        assert_eq!(result, CommandResult::text("Plan Mode enabled: New Plan"));
    }

    #[tokio::test]
    async fn plan_off_returns_disabled_text() {
        let result = run("off").await;
        assert_eq!(result, CommandResult::text("Plan Mode disabled"));
    }

    #[tokio::test]
    async fn plan_no_args_toggles() {
        let result = run("").await;
        assert_eq!(result, CommandResult::text("Plan Mode toggled: New Plan"));
    }
}
