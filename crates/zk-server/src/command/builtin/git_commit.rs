//! `/git-commit [message]` (alias `/commit`) acts only on the existing index.
//! Preview uses complete NUL-delimited staged names. Uncertain commit outcomes
//! require checking Git state before a user decides whether to retry.

use futures::future::BoxFuture;

use super::git_review::{require_repository_root, run_git, run_git_raw, truncate};
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

            let Some(status) = run_git(work_dir, &["status", "--porcelain"]).await else {
                return CommandResult::error("读取 Git 状态失败，请检查仓库后重试。");
            };
            let Some(names) = run_git_raw(
                work_dir,
                &[
                    "diff",
                    "--cached",
                    "--ignore-submodules=none",
                    "--name-only",
                    "-z",
                ],
            )
            .await
            else {
                return CommandResult::error("读取暂存文件列表失败，请检查仓库后重试。");
            };
            let Some(changed_files) = staged_files(&names) else {
                return CommandResult::error("暂存文件列表不完整，未执行提交；请稍后重试。");
            };
            if changed_files.is_empty() {
                let Some(merge_head) =
                    run_git(work_dir, &["rev-parse", "--git-path", "MERGE_HEAD"])
                        .await
                        .filter(|path| !path.is_empty())
                else {
                    return CommandResult::error("读取 Git 合并状态失败，请检查仓库后重试。");
                };
                if !std::path::Path::new(work_dir).join(merge_head).is_file() {
                    return CommandResult::text("没有已暂存的变更；请先暂存需要提交的文件。");
                }
            }
            if !args.trim().is_empty() {
                return match run_git(work_dir, &["commit", "-m", args]).await {
                    Some(output) if !output.trim().is_empty() => {
                        CommandResult::text(format!("✅ 已提交:\n{output}"))
                    }
                    _ => CommandResult::error(
                        "提交结果不确定：可能未生效，也可能已成功。请先核对 git log / git status，再决定是否重试；若被提交钩子拒绝，请先修复钩子。",
                    ),
                };
            }
            let staged_diff = run_git(
                work_dir,
                &["diff", "--cached", "--ignore-submodules=none", "--stat"],
            )
            .await;
            let detailed_diff =
                run_git(work_dir, &["diff", "--cached", "--ignore-submodules=none"]).await;
            let (Some(staged_diff), Some(detailed_diff)) = (staged_diff, detailed_diff) else {
                return CommandResult::error("读取暂存差异失败，未完成预览；请检查仓库后重试。");
            };
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

/// Reject incomplete output instead of inventing a partial staged-file list.
fn staged_files(names: &str) -> Option<Vec<String>> {
    if names.is_empty() {
        return Some(Vec::new());
    }
    let complete = names.strip_suffix('\0')?;
    let files = complete.split('\0').map(str::to_owned).collect::<Vec<_>>();
    files.iter().all(|file| !file.is_empty()).then_some(files)
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
        assert_eq!(
            result,
            CommandResult::text("没有已暂存的变更；请先暂存需要提交的文件。")
        );
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
            CommandResult::text("没有已暂存的变更；请先暂存需要提交的文件。")
        );
    }

    #[test]
    fn staged_names_preserve_whitespace_and_reject_incomplete_protocol() {
        assert_eq!(
            super::staged_files(" leading\nname \0next\0"),
            Some(vec![" leading\nname ".into(), "next".into()])
        );
        assert_eq!(super::staged_files("partial"), None);
        assert_eq!(super::staged_files("good\0partial"), None);
        assert_eq!(super::staged_files("good\0\0"), None);
        assert_eq!(super::staged_files(""), Some(Vec::new()));
    }
}
