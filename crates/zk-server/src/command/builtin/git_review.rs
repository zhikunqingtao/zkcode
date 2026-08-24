//! `/git-review`——AI 代码审查当前变更（Batch 8A）。
//!
//! 语义来源（旧仓库只读）：`GitReviewCommand.java`（68L）+ 其共用守卫
//! `GitCommandGuard.java`（32L）+ `GitService.execGit`（`execGitPublic` 的实现：
//! 5s 超时 / 非零退出码回 null / 输出 trim）。
//!
//! 用法：`/git-review` — 取工作区与暂存区全量差异，渲染审查提示词注入对话。
//!
//! # 有意差异
//!
//! - 命令名取 `git-review`（旧 `getName()` 为 `review`），旧名注册为别名——
//!   `/review` 这类裸动词在补全列表里语义含糊，`git-` 前缀让 `/git-review` /
//!   `/git-commit` / `/diff` 三个 Git 命令成组可见；两个名字经
//!   [`CommandRegistry`](crate::command::CommandRegistry) 都能解析，行为同一。
//! - 旧 `GitService` 经 `ProcessBuilder` 同步执行 Git；此处走
//!   `tokio::process::Command`（与既有 [`super::diff`] 一致）。
//! - 截断按 `char` 计数（旧 `String.substring` 按 UTF-16 码元），中文差异下
//!   本端截得略长，但不会把多字节字符切半。

use std::path::Path;
use std::time::Duration;

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// 审查提示词里差异正文的截断上限（旧 `truncate(fullDiff, 8000)`）。
const MAX_REVIEW_DIFF_LENGTH: usize = 8_000;

/// 单条 Git 命令的执行上限（旧 `process.waitFor(5, TimeUnit.SECONDS)`）。
const GIT_TIMEOUT: Duration = Duration::from_secs(5);

/// `/git-review` 命令。
pub(super) struct GitReviewCommand;

impl Command for GitReviewCommand {
    fn name(&self) -> &'static str {
        "git-review"
    }

    fn aliases(&self) -> &'static [&'static str] {
        // 旧 `GitReviewCommand.getName()`，保留为别名（见模块文档的有意差异）。
        &["review"]
    }

    fn description(&self) -> &'static str {
        "AI 代码审查当前变更"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Prompt
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            // 旧 L24-37：工作目录三段守卫（未设置 / 系统目录 / 非仓库根）。
            if let Some(denied) = require_repository_root(ctx).await {
                return denied;
            }
            let work_dir = ctx.working_dir.as_str();

            // 旧 L39-41：工作区差异 + 暂存区差异拼接（null → 空串）。
            let diff = run_git(work_dir, &["diff"]).await.unwrap_or_default();
            let staged = run_git(work_dir, &["diff", "--cached"])
                .await
                .unwrap_or_default();
            let full_diff = format!("{diff}\n{staged}");

            // 旧 L43-45：拼接串恒含换行，故 `isBlank()` 即两侧皆空。
            if full_diff.trim().is_empty() {
                return CommandResult::text("没有待审查的变更");
            }

            // 旧 L47-60 的提示词逐字（Java 文本块的公共缩进已由编译器剥除）。
            CommandResult::text(format!(
                "请对以下代码变更进行审查，从以下维度评估:\n\
                 1. 🐛 Bug 风险：空指针、资源泄漏、逻辑错误\n\
                 2. 🔒 安全漏洞：注入、越权、敏感数据暴露\n\
                 3. ⚡ 性能问题：N+1 查询、内存分配、死循环\n\
                 4. 📐 代码规范：命名、结构、重复代码、单一职责\n\
                 5. 🧪 测试覆盖建议：缺失的边界场景、回归测试\n\
                 \n\
                 对每个发现给出严重级别（高/中/低）和具体修复建议。\n\
                 \n\
                 ```diff\n\
                 {diff}\n\
                 ```\n",
                diff = truncate(&full_diff, MAX_REVIEW_DIFF_LENGTH)
            ))
        })
    }
}

/// Git 命令的公共守卫（旧 `GitCommandGuard.requireRepositoryRoot` + 各命令
/// 自带的系统目录检查）：放行返回 `None`，拒绝返回待下行的失败结果。
///
/// 与 [`super::git_commit`] 共用——旧侧同样是一个共享类，两个命令的守卫文案
/// 必须逐字同源。
pub(super) async fn require_repository_root(ctx: &CommandContext) -> Option<CommandResult> {
    // 旧 `GitReviewCommand` L24-26 / `GitCommandGuard` L17-20 同一文案。
    if ctx.working_dir.trim().is_empty() {
        return Some(CommandResult::error("工作目录未设置"));
    }
    let work_dir = ctx.working_dir.as_str();
    // 旧 L29-33：规范化后拒绝根与两个系统前缀（上下文里的路径已是绝对形式）。
    if work_dir == "/" || work_dir.starts_with("/etc") || work_dir.starts_with("/usr") {
        return Some(CommandResult::error("不允许在系统目录中执行 Git 操作"));
    }
    // 旧 `GitService.isGitRepositoryRoot`：向上找 `.git` 得仓库根，要求它
    // **等于**工作目录本身（子目录一律拒绝）。等价实现为
    // `rev-parse --show-toplevel` 后按真实路径比对；Git 不可用 / 非仓库时该
    // 命令非零退出 → `None` → 与旧侧同样 fail-closed。
    let top_level = run_git(work_dir, &["rev-parse", "--show-toplevel"]).await;
    if top_level.is_some_and(|top| same_real_path(top.trim(), work_dir)) {
        return None;
    }
    Some(CommandResult::error(
        "仅允许在当前授权的 Git 仓库根目录执行 Git 命令",
    ))
}

/// 两个路径是否指向同一目录（旧 `toRealPath()` 后 `equals` 的等价物：符号
/// 链接差异不该误判成「不在仓库根」）。
fn same_real_path(left: &str, right: &str) -> bool {
    let canonical = |path: &str| std::fs::canonicalize(Path::new(path)).ok();
    match (canonical(left), canonical(right)) {
        (Some(left), Some(right)) => left == right,
        // 任一端不可解析时退回字面比较（旧侧此时抛异常 → fail-closed；本端仍
        // 给字面相等一次机会，差异只影响不可 stat 的路径）。
        _ => left == right,
    }
}

/// 执行一条 Git 命令（旧 `GitService.execGit`）。
///
/// 返回 `None` = 进程起不来 / 5s 超时 / 非零退出码（旧实现三种情形均回
/// null）；`Some(output)` 为 trim 后的输出。旧 `redirectErrorStream(true)` 把
/// stderr 并入 stdout，此处在成功分支同样拼接两股（非零退出已回 `None`，故
/// 实际只影响带告警的成功输出）。
pub(super) async fn run_git(working_dir: &str, args: &[&str]) -> Option<String> {
    let output = tokio::time::timeout(
        GIT_TIMEOUT,
        tokio::process::Command::new("git")
            .args(args)
            .current_dir(working_dir)
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    if !output.status.success() {
        return None;
    }
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    Some(text.trim().to_owned())
}

/// 旧各命令私有的 `truncate(text, maxLen)`（超长追加 `\n...(已截断)`）。
pub(super) fn truncate(text: &str, max_len: usize) -> String {
    if text.chars().count() <= max_len {
        return text.to_owned();
    }
    let head: String = text.chars().take(max_len).collect();
    format!("{head}\n...(已截断)")
}

#[cfg(test)]
mod tests {
    use crate::command::traits::{CommandResult, CommandType};
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(working_dir: &str) -> CommandResult {
        let ctx = CommandContext::of("s-1", working_dir, "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let cmd = registry.find_command("git-review").expect("registered");
        cmd.execute("", &ctx).await
    }

    fn git_gate_enabled() -> bool {
        std::env::var("ZK_RUN_GIT_TESTS").as_deref() == Ok("true")
    }

    /// PROMPT 类型 + 旧命令名别名（分发侧据类型决定注入对话而非回显）。
    #[test]
    fn is_a_prompt_command_reachable_under_the_legacy_name() {
        let registry = CommandRegistry::with_builtin_commands();
        let cmd = registry.find_command("review").expect("alias resolves");
        assert_eq!(cmd.name(), "git-review");
        assert_eq!(cmd.command_type(), CommandType::Prompt);
    }

    #[tokio::test]
    async fn empty_working_dir_returns_error() {
        assert_eq!(run("").await, CommandResult::error("工作目录未设置"));
    }

    #[tokio::test]
    async fn system_directory_is_rejected() {
        assert_eq!(
            run("/etc").await,
            CommandResult::error("不允许在系统目录中执行 Git 操作")
        );
    }

    /// 非仓库目录 → 守卫文案（旧 `GitCommandGuard` 逐字）。
    #[tokio::test]
    async fn non_repository_directory_is_rejected() {
        if !git_gate_enabled() {
            return;
        }
        let dir = std::env::temp_dir().join(format!("zk-git-review-plain-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let result = run(dir.to_str().expect("utf-8 path")).await;
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            result,
            CommandResult::error("仅允许在当前授权的 Git 仓库根目录执行 Git 命令")
        );
    }

    /// 仓库根：无变更 → 旧文案；有变更 → 五维审查提示词含差异正文。
    #[tokio::test]
    async fn repository_root_reviews_or_reports_no_changes() {
        if !git_gate_enabled() {
            return;
        }
        let dir = std::env::temp_dir().join(format!("zk-git-review-repo-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let status = std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(&dir)
            .output()
            .expect("git runs")
            .status;
        assert!(status.success(), "git init failed");
        let canonical = std::fs::canonicalize(&dir).expect("canonical temp dir");
        let work_dir = canonical.to_str().expect("utf-8 path").to_owned();

        assert_eq!(
            run(&work_dir).await,
            CommandResult::text("没有待审查的变更")
        );

        std::fs::write(dir.join("a.txt"), "hello\n").expect("seed file");
        let _ = std::process::Command::new("git")
            .args(["add", "a.txt"])
            .current_dir(&dir)
            .output()
            .expect("git runs");
        let reviewed = run(&work_dir).await;
        let _ = std::fs::remove_dir_all(&dir);
        let CommandResult::Text(prompt) = reviewed else {
            panic!("staged changes must render the review prompt");
        };
        assert!(prompt.starts_with("请对以下代码变更进行审查，从以下维度评估:\n"));
        assert!(prompt.contains("5. 🧪 测试覆盖建议：缺失的边界场景、回归测试"));
        assert!(prompt.contains("+hello"), "diff body missing: {prompt}");
    }

    /// 截断在超限时追加旧后缀，未超限时原样返回。
    #[test]
    fn truncate_appends_the_legacy_suffix_only_when_over_the_limit() {
        assert_eq!(super::truncate("abc", 3), "abc");
        assert_eq!(super::truncate("abcd", 3), "abc\n...(已截断)");
        // 多字节字符按 char 计数，不会切出非法 UTF-8。
        assert_eq!(super::truncate("中文差异", 2), "中文\n...(已截断)");
    }
}
