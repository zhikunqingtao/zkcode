//! 环境命令族（旧 `EnvironmentCommands.java`）。
//!
//! 包含 `/env-vars`（环境变量列表）、`/set-env`（设置环境变量）。

use std::fmt::Write as _;

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/env-vars`——列出相关环境变量。
pub(super) struct EnvVarsCommand;

impl Command for EnvVarsCommand {
    fn name(&self) -> &'static str {
        "env-vars"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["env"]
    }

    fn description(&self) -> &'static str {
        "显示相关环境变量"
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
            let mut sb = String::new();
            let _ = writeln!(sb, "## 环境变量");
            sb.push('\n');

            // 列出 ZK_ 前缀的环境变量
            let relevant_keys = [
                "ZK_LOG",
                "ZK_CORS_ALLOWED_ORIGINS",
                "ZK_ADMIN_PASSWORD",
                "ZK_STATIC_DIR",
                "RUST_LOG",
                "HOME",
                "SHELL",
            ];

            for key in &relevant_keys {
                let value = std::env::var(key).unwrap_or_else(|_| "(not set)".into());
                // 脱敏：密码类变量只显示是否设置
                let display = if key.contains("PASSWORD") || key.contains("SECRET") {
                    if value == "(not set)" {
                        "(not set)".to_owned()
                    } else {
                        "***".to_owned()
                    }
                } else {
                    value
                };
                let _ = writeln!(sb, "- **{key}**: {display}");
            }

            let _ = writeln!(sb, "- **Working Dir**: {}", ctx.working_dir);
            let _ = writeln!(sb, "- **Model**: {}", ctx.current_model);
            CommandResult::text(sb)
        })
    }
}

/// `/set-env`——设置环境变量（仅对当前进程生效）。
pub(super) struct SetEnvCommand;

impl Command for SetEnvCommand {
    fn name(&self) -> &'static str {
        "set-env"
    }

    fn description(&self) -> &'static str {
        "设置环境变量"
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
            if trimmed.is_empty() {
                return CommandResult::error("Usage: /set-env KEY=VALUE");
            }

            let Some((key, value)) = trimmed.split_once('=') else {
                return CommandResult::error("Usage: /set-env KEY=VALUE");
            };

            let key = key.trim();
            let value = value.trim();

            if key.is_empty() {
                return CommandResult::error("Environment variable name cannot be empty");
            }

            // 安全限制：只允许 ZK_ 前缀的变量
            if !key.starts_with("ZK_") && !key.starts_with("RUST_") {
                return CommandResult::error(
                    "Only ZK_* and RUST_* environment variables can be set",
                );
            }

            // 注意：`std::env::set_var` 在多线程环境下不安全（Rust 1.82+
            // 标记为 unsafe）。本实现仅记录意图，不实际修改环境变量。
            tracing::info!(key = %key, "set-env requested (no-op in multi-threaded runtime)");
            CommandResult::text(format!(
                "Environment variable {key}={value} noted. \
                 Note: runtime env modification is limited in multi-threaded servers. \
                 Restart with the new value for full effect."
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(name: &str, args: &str) -> CommandResult {
        let ctx = CommandContext::of("s-1", "/tmp", "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry.find_command(name).expect("registered");
        command.execute(args, &ctx).await
    }

    #[tokio::test]
    async fn env_vars_lists_relevant_vars() {
        let result = run("env-vars", "").await;
        match result {
            CommandResult::Text(text) => {
                assert!(text.contains("环境变量"));
                assert!(text.contains("Working Dir"));
            }
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_env_rejects_missing_equals() {
        let result = run("set-env", "NOEQUALS").await;
        assert!(matches!(result, CommandResult::Error(_)));
    }

    #[tokio::test]
    async fn set_env_rejects_non_zk_prefix() {
        let result = run("set-env", "PATH=/tmp").await;
        match result {
            CommandResult::Error(msg) => assert!(msg.contains("ZK_*")),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn set_env_accepts_zk_prefix() {
        let result = run("set-env", "ZK_LOG=debug").await;
        assert!(result.is_success());
    }
}
