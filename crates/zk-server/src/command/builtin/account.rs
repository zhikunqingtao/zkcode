//! 账户与用量命令族（旧 `AccountCommands.java`）。
//!
//! 包含 `/usage`、`/extra-usage`、`/rate-limit-options`、`/upgrade`、`/version`。
//! `login` / `logout` / `cost` 已有独立实现文件。

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/usage`——用量统计报告。
pub(super) struct UsageCommand;

impl Command for UsageCommand {
    fn name(&self) -> &'static str {
        "usage"
    }

    fn description(&self) -> &'static str {
        "用量统计报告"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Local
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        _ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async {
            CommandResult::text(
                "Usage report:\n\
                \x20 Session tokens: (P1 — integrate with billing)\n\
                \x20 Daily usage: (P1)\n\
                \x20 Monthly usage: (P1)",
            )
        })
    }
}

/// `/extra-usage`——额外用量详情。
pub(super) struct ExtraUsageCommand;

impl Command for ExtraUsageCommand {
    fn name(&self) -> &'static str {
        "extra-usage"
    }

    fn description(&self) -> &'static str {
        "额外用量详情"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Local
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        _ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async {
            CommandResult::text(
                "Extra usage details:\n\
                \x20 Tool calls: (P1)\n\
                \x20 File operations: (P1)\n\
                \x20 API requests: (P1)",
            )
        })
    }
}

/// `/rate-limit-options`——速率限制选项。
pub(super) struct RateLimitOptionsCommand;

impl Command for RateLimitOptionsCommand {
    fn name(&self) -> &'static str {
        "rate-limit-options"
    }

    fn description(&self) -> &'static str {
        "速率限制选项"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Local
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        _ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async {
            CommandResult::text(
                "Rate limit options:\n\
                \x20 Current plan: (P1)\n\
                \x20 Requests/min: (P1)\n\
                \x20 Tokens/day: (P1)\n\
                \x20 Upgrade for higher limits: /upgrade",
            )
        })
    }
}

/// `/upgrade`——版本升级。
pub(super) struct UpgradeCommand;

impl Command for UpgradeCommand {
    fn name(&self) -> &'static str {
        "upgrade"
    }

    fn description(&self) -> &'static str {
        "版本升级"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Local
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        _ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async {
            CommandResult::text(
                "Checking for updates... (P1 placeholder)\n\
                Current version: (P1)\n\
                Latest version: (P1)",
            )
        })
    }
}

/// `/version`——版本信息。
pub(super) struct VersionCommand;

impl Command for VersionCommand {
    fn name(&self) -> &'static str {
        "version"
    }

    fn description(&self) -> &'static str {
        "版本信息"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Local
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        _ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async {
            let os_info = format!("{} {}", std::env::consts::OS, std::env::consts::ARCH);
            CommandResult::text(format!(
                "zkcode local AI coding assistant\n\
                \x20 Version: 1.0.0-SNAPSHOT\n\
                \x20 Runtime: Rust\n\
                \x20 OS: {os_info}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(name: &str) -> CommandResult {
        let ctx = CommandContext::of("s-1", "/tmp", "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry.find_command(name).expect("registered");
        command.execute("", &ctx).await
    }

    #[tokio::test]
    async fn usage_returns_report() {
        let result = run("usage").await;
        match result {
            CommandResult::Text(text) => assert!(text.contains("Usage report")),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn version_returns_info() {
        let result = run("version").await;
        match result {
            CommandResult::Text(text) => {
                assert!(text.contains("zkcode"));
                assert!(text.contains("Rust"));
            }
            other => panic!("expected Text, got {other:?}"),
        }
    }
}
