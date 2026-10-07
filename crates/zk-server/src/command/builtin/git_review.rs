//! Explicitly scoped, read-only Git review prompts and shared Git execution.
//! Unknown Git outcomes stay errors; untracked material is never assumed reviewed.

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
        args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            // 旧 L24-37：工作目录三段守卫（未设置 / 系统目录 / 非仓库根）。
            if let Some(denied) = require_repository_root(ctx).await {
                return denied;
            }
            let work_dir = ctx.working_dir.as_str();

            if !args.trim().is_empty() {
                return CommandResult::text(format!(
                    "请按以下用户要求进行代码审查，只审查、不修改文件。\n\
                     以用户指定的比较对象、文件范围和排除项为准。\n\
                     只有未指定比较对象时，才默认审查范围内的当前本地变更：已暂存、未暂存及未跟踪文件；明确排除的部分不纳入。\n\
                     先确定范围，再通过现有工具读取必要内容，不预取排除文件的正文。\n\
                     无法读取指定比较对象时说明原因，不自行换成其他比较对象。\n\
                     工具失败或材料不完整时说明未覆盖部分，不能声称全部审完。\n\n本次用户要求：\n{args}"
                ));
            }
            let diff = run_git(work_dir, &["diff"]).await;
            let staged = run_git(work_dir, &["diff", "--cached"]).await;
            let (Some(diff), Some(staged)) = (diff, staged) else {
                return CommandResult::error(
                    "读取未暂存或已暂存差异失败，本次尚未完成审查，请检查仓库后重试。",
                );
            };
            let diff_chars = diff.chars().count().min(
                MAX_REVIEW_DIFF_LENGTH - staged.chars().count().min(MAX_REVIEW_DIFF_LENGTH / 2),
            );
            let staged_chars = staged
                .chars()
                .count()
                .min(MAX_REVIEW_DIFF_LENGTH - diff_chars);
            let section = |label, text: &str, limit| {
                if text.trim().is_empty() {
                    format!("{label}差异预览：\n该部分未见差异")
                } else {
                    format!("{label}差异预览：\n```diff\n{}\n```", truncate(text, limit))
                }
            };
            CommandResult::text(format!(
                "请对以下代码变更进行审查，从以下维度评估:\n\
                 以下只是已跟踪文件的未暂存/已暂存差异预览，未跟踪文件尚未核验。\n\
                 先核对文件范围；使用 git status --short 与 git ls-files --others --exclude-standard 等现有工具检查未跟踪文件，再按必要性读取正文。不要自动上传全部未跟踪文件。\n\
                 预览标记“已截断”时，按文件继续读取必要 diff；未暂存与已暂存版本分别判断，同一文件存在两种版本时，不把工作区文件内容当作暂存区内容。\n\
                 只审查、不修改；范围外资料、读取失败和未完成部分必须明确说明。\n\
                 仅在相关集合均检查后才能说“没有待审查的变更”或“全部审完”。\n\
                 1. 🐛 Bug 风险：空指针、资源泄漏、逻辑错误\n\
                 2. 🔒 安全漏洞：注入、越权、敏感数据暴露\n\
                 3. ⚡ 性能问题：N+1 查询、内存分配、死循环\n\
                 4. 📐 代码规范：命名、结构、重复代码、单一职责\n\
                 5. 🧪 测试覆盖建议：缺失的边界场景、回归测试\n\n\
                 对每个发现给出严重级别（高/中/低）和具体修复建议。\n\n{}\n\n{}\n",
                section("未暂存", &diff, diff_chars),
                section("已暂存", &staged, staged_chars)
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

/// Read stdout without merging diagnostic stderr or removing filename whitespace.
/// Nonzero exit, invalid UTF-8, launch failure and timeout all fail closed.
pub(super) async fn run_git_raw(working_dir: &str, args: &[&str]) -> Option<String> {
    let cancel = tokio_util::sync::CancellationToken::new();
    let _cancel_on_drop = cancel.clone().drop_guard();
    let (progress, _receiver) = tokio::sync::mpsc::channel(1);
    // Slash commands have no model Run: do not invent task/tool attribution.
    // The retained Git anchor still supervises the owned hook group to quiescence.
    let context = zk_tools::ToolContext::with_bounded_progress(cancel, progress)
        .with_working_dir(working_dir);
    // Human diffs and explicit commit reports (including hook output) may exceed
    // the machine-record budget. Path/raw protocols retain the smaller bound.
    let human_diff = args.first() == Some(&"diff")
        && !args.iter().any(|arg| {
            matches!(
                *arg,
                "-z" | "--name-only" | "--name-status" | "--numstat" | "--raw"
            )
        });
    let args = args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
    let output = if human_diff || args.first().is_some_and(|arg| arg == "commit") {
        zk_tools::process::run_git_human_program(
            &args,
            Path::new(working_dir),
            GIT_TIMEOUT,
            &context,
        )
        .await
        .ok()?
    } else {
        zk_tools::process::run_git_program(&args, Path::new(working_dir), GIT_TIMEOUT, &context)
            .await
            .ok()?
    };
    if output.exit_code != 0
        || !output.termination_confirmed
        || output.timed_out
        || output.cancelled
        || output.truncated
        || output.stdout.contains('\u{fffd}')
    {
        return None;
    }
    Some(output.stdout)
}

/// Human-readable successful stdout; never use this for NUL path protocols.
pub(super) async fn run_git(working_dir: &str, args: &[&str]) -> Option<String> {
    run_git_raw(working_dir, args)
        .await
        .map(|text| text.trim().to_owned())
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

        let CommandResult::Text(prompt) = run(&work_dir).await else {
            panic!("review prompt");
        };
        assert!(prompt.contains("未跟踪文件尚未核验"));
        assert!(prompt.contains("该部分未见差异"));

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
