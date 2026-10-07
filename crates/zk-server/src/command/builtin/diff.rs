//! `/diff [unstaged|staged|--staged]`: explicit scoped Git diff preview.
//! Failed reads are errors; only successful empty reads mean no changes.

use futures::future::BoxFuture;

use super::git_review::{require_repository_root, run_git, truncate};

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

    fn aliases(&self) -> &'static [&'static str] {
        &["changes"]
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
            if let Some(denied) = require_repository_root(ctx).await {
                return denied;
            }
            let work_dir = &ctx.working_dir;
            let staged = match args.trim().to_ascii_lowercase().as_str() {
                "" | "unstaged" => false,
                "staged" | "--staged" => true,
                _ => return CommandResult::error("用法：/diff [unstaged|staged|--staged]"),
            };

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

            let (Some(stat), Some(diff)) = (stat, diff) else {
                return CommandResult::error(
                    "读取 Git 差异失败，无法确认是否存在差异；请检查仓库后重试。",
                );
            };

            if stat.trim().is_empty() && diff.trim().is_empty() {
                return CommandResult::text("无差异");
            }

            let file_count = if stat.trim().is_empty() {
                0
            } else {
                stat.lines().count().saturating_sub(1)
            };

            CommandResult::jsx(serde_json::json!({
                "action": "gitDiffView",
                "staged": staged,
                "stat": truncate(&stat, MAX_DIFF_LENGTH),
                "diff": truncate(&diff, MAX_DIFF_LENGTH),
                "fileCount": file_count
            }))
        })
    }
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
