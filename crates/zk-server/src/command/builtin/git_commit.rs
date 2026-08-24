//! `/git-commit [message]`——AI 辅助 Git 提交（Batch 8A）。
//!
//! 语义来源（旧仓库只读）：`GitCommitCommand.java`（75L），守卫与 Git 执行
//! 复用 [`super::git_review`] 里的旧 `GitCommandGuard` / `GitService.execGit`
//! 等价物。
//!
//! 用法：
//! - `/git-commit` — 提交预览（JSX：状态 / 暂存差异 / 变更文件清单）；
//! - `/git-commit <message>` — 以该消息提交**已暂存**的变更。
//!
//! # 有意差异
//!
//! - 命令名取 `git-commit`（旧 `getName()` 为 `commit`），旧名注册为别名，
//!   理由同 [`super::git_review`]。
//! - **不自动 `git add -A`**：旧实现只 `commit -m`，暂存区由用户自己决定。
//!   替用户全量暂存会把无关脏文件卷进提交（且提交后难以拆分），故照抄旧行为
//!   ——无暂存内容时 `git commit` 自身非零退出，落到旧的失败文案。

use futures::future::BoxFuture;

use super::git_review::{require_repository_root, run_git, truncate};
use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// 预览里详细差异的截断上限（旧 `truncate(detailedDiff, 5000)`）。
const MAX_DETAILED_DIFF_LENGTH: usize = 5_000;

/// `/git-commit` 命令。
pub(super) struct GitCommitCommand;

impl Command for GitCommitCommand {
    fn name(&self) -> &'static str {
        "git-commit"
    }

    fn aliases(&self) -> &'static [&'static str] {
        // 旧 `GitCommitCommand.getName()`，保留为别名。
        &["commit"]
    }

    fn description(&self) -> &'static str {
        "AI 辅助 Git 提交"
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
            // 旧 L25-38：三段守卫与 `/git-review` 同源。
            if let Some(denied) = require_repository_root(ctx).await {
                return denied;
            }
            let work_dir = ctx.working_dir.as_str();

            // 旧 L40-43：`status --porcelain` 为空即无可提交内容（`text` 非 `error`）。
            let status = run_git(work_dir, &["status", "--porcelain"])
                .await
                .unwrap_or_default();
            if status.trim().is_empty() {
                return CommandResult::text("没有可提交的变更");
            }

            // 旧 L45-51：带消息 → 直接提交；`execGit` 回 null（非零退出 / 超时）
            // 即失败文案（钩子拒绝、无暂存内容均落此分支）。
            let message = args.trim();
            if !message.is_empty() {
                return match run_git(work_dir, &["commit", "-m", message]).await {
                    Some(output) if !output.trim().is_empty() => {
                        CommandResult::text(format!("✅ 已提交:\n{output}"))
                    }
                    _ => CommandResult::error("git commit 失败，请检查 Git 输出或本地钩子。"),
                };
            }

            // 旧 L53-68：无消息 → 预览（键名与 `action` 逐字）。
            let staged_diff = run_git(work_dir, &["diff", "--cached", "--stat"])
                .await
                .unwrap_or_default();
            let detailed_diff = run_git(work_dir, &["diff", "--cached"])
                .await
                .unwrap_or_default();
            let changed_files = changed_files(&status);
            CommandResult::jsx(serde_json::json!({
                "action": "gitCommitPreview",
                "status": status,
                "stagedDiff": staged_diff,
                "detailedDiff": truncate(&detailed_diff, MAX_DETAILED_DIFF_LENGTH),
                "changedFiles": changed_files,
                "fileCount": changed_files.len()
            }))
        })
    }
}

/// 从 `status --porcelain` 提取变更文件名（旧 L56-59：长度 > 3 的行取
/// `substring(3).trim()`，即跳过两位状态码与分隔空格）。
fn changed_files(status: &str) -> Vec<String> {
    status
        .split('\n')
        .filter(|line| line.len() > 3)
        // 状态码与分隔符恒为 ASCII，故第 3 字节必是字符边界；`get` 兜掉理论
        // 上的非边界输入（旧 `substring` 在 UTF-16 上不会失败）。
        .filter_map(|line| line.get(3..))
        .map(|path| path.trim().to_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(args: &str, working_dir: &str) -> CommandResult {
        let ctx = CommandContext::of("s-1", working_dir, "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let cmd = registry.find_command("git-commit").expect("registered");
        cmd.execute(args, &ctx).await
    }

    fn git_gate_enabled() -> bool {
        std::env::var("ZK_RUN_GIT_TESTS").as_deref() == Ok("true")
    }

    /// 建一个带身份配置的空仓库，返回 (目录, canonical 路径串)。
    fn seed_repository(tag: &str) -> (std::path::PathBuf, String) {
        let dir = std::env::temp_dir().join(format!("zk-git-commit-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        for args in [
            ["init", "-q", ""],
            ["config", "user.email", "zk@example.com"],
            ["config", "user.name", "zk"],
            ["config", "commit.gpgsign", "false"],
        ] {
            let args: Vec<&str> = args.iter().copied().filter(|arg| !arg.is_empty()).collect();
            let status = std::process::Command::new("git")
                .args(&args)
                .current_dir(&dir)
                .output()
                .expect("git runs")
                .status;
            assert!(status.success(), "git {args:?} failed");
        }
        let canonical = std::fs::canonicalize(&dir).expect("canonical temp dir");
        let path = canonical.to_str().expect("utf-8 path").to_owned();
        (dir, path)
    }

    /// 旧命令名仍可解析（别名索引）。
    #[test]
    fn legacy_name_resolves_to_the_prefixed_command() {
        let registry = CommandRegistry::with_builtin_commands();
        let cmd = registry.find_command("commit").expect("alias resolves");
        assert_eq!(cmd.name(), "git-commit");
    }

    #[tokio::test]
    async fn empty_working_dir_returns_error() {
        assert_eq!(run("", "").await, CommandResult::error("工作目录未设置"));
    }

    /// 干净仓库 → 旧 `text` 文案（非 error）。
    #[tokio::test]
    async fn clean_repository_reports_nothing_to_commit() {
        if !git_gate_enabled() {
            return;
        }
        let (dir, work_dir) = seed_repository("clean");
        let result = run("", &work_dir).await;
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(result, CommandResult::text("没有可提交的变更"));
    }

    /// 无消息 → `gitCommitPreview` JSX（六键齐全，文件清单剥掉状态码）。
    #[tokio::test]
    async fn without_message_returns_the_preview_payload() {
        if !git_gate_enabled() {
            return;
        }
        let (dir, work_dir) = seed_repository("preview");
        std::fs::write(dir.join("a.txt"), "hello\n").expect("seed file");
        let _ = std::process::Command::new("git")
            .args(["add", "a.txt"])
            .current_dir(&dir)
            .output()
            .expect("git runs");
        let result = run("", &work_dir).await;
        let _ = std::fs::remove_dir_all(&dir);
        let CommandResult::Jsx(data) = result else {
            panic!("staged changes without message must preview");
        };
        assert_eq!(data["action"], "gitCommitPreview");
        assert_eq!(data["changedFiles"], serde_json::json!(["a.txt"]));
        assert_eq!(data["fileCount"], 1);
        assert!(
            data["detailedDiff"]
                .as_str()
                .expect("detailedDiff str")
                .contains("+hello")
        );
    }

    /// 带消息 → 提交已暂存内容并回显 Git 输出。
    #[tokio::test]
    async fn with_message_commits_the_staged_changes() {
        if !git_gate_enabled() {
            return;
        }
        let (dir, work_dir) = seed_repository("commit");
        std::fs::write(dir.join("a.txt"), "hello\n").expect("seed file");
        let _ = std::process::Command::new("git")
            .args(["add", "a.txt"])
            .current_dir(&dir)
            .output()
            .expect("git runs");
        let result = run("feat: seed", &work_dir).await;
        let log = std::process::Command::new("git")
            .args(["log", "--oneline"])
            .current_dir(&dir)
            .output()
            .expect("git runs");
        let _ = std::fs::remove_dir_all(&dir);
        let CommandResult::Text(output) = result else {
            panic!("commit with message must return text, got other variant");
        };
        assert!(output.starts_with("✅ 已提交:\n"), "unexpected: {output}");
        assert!(
            String::from_utf8_lossy(&log.stdout).contains("feat: seed"),
            "commit not recorded"
        );
    }

    /// 只有未跟踪文件（暂存区空）→ `git commit` 非零退出 → 旧失败文案。
    #[tokio::test]
    async fn unstaged_only_reports_the_legacy_failure() {
        if !git_gate_enabled() {
            return;
        }
        let (dir, work_dir) = seed_repository("unstaged");
        std::fs::write(dir.join("a.txt"), "hello\n").expect("seed file");
        let result = run("feat: seed", &work_dir).await;
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            result,
            CommandResult::error("git commit 失败，请检查 Git 输出或本地钩子。")
        );
    }

    /// 状态行解析：短行丢弃、状态码剥离、路径去空白。
    #[test]
    fn changed_files_strips_the_status_prefix() {
        let files = super::changed_files(" M src/a.rs\n?? b.txt\nXY\n A  spaced.txt");
        assert_eq!(files, ["src/a.rs", "b.txt", "spaced.txt"]);
    }
}
