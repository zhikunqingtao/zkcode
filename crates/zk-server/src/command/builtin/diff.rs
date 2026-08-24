//! `/diff [staged]`——显示 Git 差异（Batch 7）。
//!
//! 语义来源（旧仓库只读）：`DiffCommand.java`（70L）。
//!
//! 用法：
//! - `/diff` — 显示工作区差异；
//! - `/diff staged` — 显示暂存区差异。
//!
//! # 有意差异
//!
//! - Java 侧经 `GitService.execGitPublic` 执行 Git 命令；
//!   Rust 侧直接 `tokio::process::Command` 调用 `git`。

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// 输出截断上限（对照旧 `truncate(diff, 10000)`）。
const MAX_DIFF_LENGTH: usize = 10_000;

/// `/diff` 命令。
pub(super) struct DiffCommand;

impl Command for DiffCommand {
    fn name(&self) -> &'static str {
        "diff"
    }

    fn description(&self) -> &'static str {
        "显示 Git 差异"
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
            if ctx.working_dir.trim().is_empty() {
                return CommandResult::error("工作目录未设置");
            }
            let work_dir = &ctx.working_dir;
            // 系统目录保护（对照旧 `workDirStr` 检查）。
            if work_dir == "/" || work_dir.starts_with("/etc") || work_dir.starts_with("/usr") {
                return CommandResult::error("不允许在系统目录中执行 Git 操作");
            }

            let staged = args.contains("staged");

            let stat_args: Vec<&str> = if staged {
                vec!["diff", "--cached", "--stat"]
            } else {
                vec!["diff", "--stat"]
            };
            let diff_args: Vec<&str> = if staged {
                vec!["diff", "--cached"]
            } else {
                vec!["diff"]
            };

            let stat = run_git(work_dir, &stat_args).await;
            let diff = run_git(work_dir, &diff_args).await;

            let stat = stat.unwrap_or_default();
            let diff = diff.unwrap_or_default();

            if stat.trim().is_empty() && diff.trim().is_empty() {
                return CommandResult::text("无差异");
            }

            let truncated_diff = if diff.chars().count() > MAX_DIFF_LENGTH {
                let truncated: String = diff.chars().take(MAX_DIFF_LENGTH).collect();
                format!("{truncated}\n...(已截断)")
            } else {
                diff.clone()
            };

            let file_count = if stat.trim().is_empty() {
                0
            } else {
                stat.lines().count().saturating_sub(1)
            };

            CommandResult::jsx(serde_json::json!({
                "action": "gitDiffView",
                "staged": staged,
                "stat": stat,
                "diff": truncated_diff,
                "fileCount": file_count
            }))
        })
    }
}

async fn run_git(working_dir: &str, args: &[&str]) -> Option<String> {
    tokio::process::Command::new("git")
        .args(args)
        .current_dir(working_dir)
        .output()
        .await
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(args: &str, working_dir: &str) -> CommandResult {
        let ctx = CommandContext::of("s-1", working_dir, "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let cmd = registry.find_command("diff").expect("registered");
        cmd.execute(args, &ctx).await
    }

    #[tokio::test]
    async fn empty_working_dir_returns_error() {
        let result = run("", "").await;
        assert!(matches!(result, CommandResult::Error(_)));
    }

    #[tokio::test]
    async fn system_directory_is_rejected() {
        let result = run("", "/etc").await;
        assert!(matches!(result, CommandResult::Error(_)));
    }
}
