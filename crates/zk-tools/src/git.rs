//! Git 只读工具族——`GitDiff` / `GitLog` / `GitStatus`（本地 `git` CLI）。
//!
//! 对照旧 `tool/impl/GitTool.java`（只读权威规格）：旧为**单**工具 `Git`
//! （`action` ∈ {diff, log, blame}），经 Java → Python → gitpython 桥执行，
//! 双门控（feature flag `GIT_ENHANCED_TOOL` + Python 能力域 `GIT_ENHANCED`），
//! 超时 300 000 ms，只读、无权限确认；参数默认值 `ref1 = HEAD~1` /
//! `ref2 = HEAD` / `max_count = 20`（上限 100）逐条沿用。
//!
//! 差异（留痕 docs/compatibility.md §4）：
//! - 旧单工具 + `action` 分派 → 本阶段拆为三个独立工具（任务判据要求
//!   `GitDiff` / `GitLog` / `GitStatus`），LLM 侧选择面更清晰；
//! - 旧走 Python gitpython 桥（结构化语义 diff），本阶段直调本地 `git` CLI
//!   （Python 侧车归 2.6），输出为 git 原生文本而非结构化对象；
//! - 旧 `blame` 未移植（本阶段判据不含），旧无 `status`（旧 prompt 明示
//!   「简单 git 操作用 `BashTool`」）——`GitStatus` 为新增；
//! - 全部经 `argv` 直调（不过 shell），且拒绝以 `-` 开头的参数值，避免
//!   ref / 分支名被当作 git 选项注入。

use std::path::Path;
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::json;

use crate::input::{bool_or, failure, optional_str, optional_usize, resolve_path, truncate_chars};
use crate::process::{ProcessOutcome, run_program};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// Git 命令超时（旧 `GitTool.getMaxExecutionTimeMs() = 300_000`）。
const GIT_TIMEOUT: Duration = Duration::from_mins(5);

/// 输出字符上限（与 `Bash` 预览上限同档）。
pub const MAX_GIT_OUTPUT_CHARS: usize = 30_000;

/// `GitLog` 默认条数（旧 `max_count` 默认 20）。
pub const DEFAULT_GIT_LOG_MAX_COUNT: usize = 20;

/// `GitLog` 条数上限（旧 `max_count` 上限 100）。
pub const MAX_GIT_LOG_MAX_COUNT: usize = 100;

/// 输出截断标记。
const OUTPUT_TRUNCATED: &str = "\n[Output truncated]";

/// `git diff` 工具（工作区 / 暂存区 / ref 区间三态）。
#[derive(Clone, Copy, Debug, Default)]
pub struct GitDiffTool;

/// `git log` 工具（结构化单行格式）。
#[derive(Clone, Copy, Debug, Default)]
pub struct GitLogTool;

/// `git status` 工具（`--porcelain=v1` + 分支头）。
#[derive(Clone, Copy, Debug, Default)]
pub struct GitStatusTool;

/// 仓库路径入参片段（三工具共用的 `repo_path` 描述）。
fn repo_path_schema() -> serde_json::Value {
    json!({
        "type": "string",
        "description": "Repository path (default: the session working directory)."
    })
}

impl Tool for GitDiffTool {
    fn name(&self) -> &'static str {
        "GitDiff"
    }

    fn description(&self) -> &'static str {
        "Show git changes: working tree by default, staged changes with staged=true, \
         or the diff between two refs when ref1/ref2 are given."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "repo_path": repo_path_schema(),
                "ref1": { "type": "string", "description": "Start ref (default HEAD~1 when refs are used)." },
                "ref2": { "type": "string", "description": "End ref (default HEAD when refs are used)." },
                "file_path": { "type": "string", "description": "Limit the diff to this path." },
                "staged": { "type": "boolean", "description": "Diff the staging area instead of the working tree." },
                "stat": { "type": "boolean", "description": "Return a --stat summary instead of the full patch." }
            }
        })
    }

    fn timeout(&self) -> Duration {
        GIT_TIMEOUT
    }

    /// 只读工具（旧 `GitTool.java:120` `isReadOnly` → `true`）。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            match diff_args(&input) {
                Ok(args) => invoke(args, input, ctx).await,
                Err(output) => output,
            }
        })
    }
}

impl Tool for GitLogTool {
    fn name(&self) -> &'static str {
        "GitLog"
    }

    fn description(&self) -> &'static str {
        "List recent git commits as `hash|author|date|subject` lines."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "repo_path": repo_path_schema(),
                "branch": { "type": "string", "description": "Branch / ref to log (default: current HEAD)." },
                "file_path": { "type": "string", "description": "Only commits touching this path." },
                "max_count": { "type": "integer", "description": "Max entries (default 20, max 100)." }
            }
        })
    }

    fn timeout(&self) -> Duration {
        GIT_TIMEOUT
    }

    /// 只读工具（旧 `GitTool.java:120` `isReadOnly` → `true`）。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            match log_args(&input) {
                Ok(args) => invoke(args, input, ctx).await,
                Err(output) => output,
            }
        })
    }
}

impl Tool for GitStatusTool {
    fn name(&self) -> &'static str {
        "GitStatus"
    }

    fn description(&self) -> &'static str {
        "Show the working tree status in porcelain format, including the branch header."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": { "repo_path": repo_path_schema() }
        })
    }

    fn timeout(&self) -> Duration {
        GIT_TIMEOUT
    }

    /// 只读工具（旧 `GitTool.java:120` `isReadOnly` → `true`）。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { invoke(status_args(), input, ctx).await })
    }
}

/// `git diff` argv 组装（ref 区间 / 暂存区 / 工作区三态 + 路径限定）。
fn diff_args(input: &serde_json::Value) -> Result<Vec<String>, ToolOutput> {
    let mut args = vec!["diff".to_owned()];
    if bool_or(input, "stat", false) {
        args.push("--stat".to_owned());
    }
    let ref1 = optional_str(input, "ref1");
    let ref2 = optional_str(input, "ref2");
    if ref1.is_some() || ref2.is_some() {
        args.push(safe_value(ref1.unwrap_or("HEAD~1"))?);
        args.push(safe_value(ref2.unwrap_or("HEAD"))?);
    } else if bool_or(input, "staged", false) {
        args.push("--cached".to_owned());
    }
    push_path_filter(input, &mut args)?;
    Ok(args)
}

/// `git log` argv 组装（条数钳制 + 单行格式 + 分支 / 路径限定）。
fn log_args(input: &serde_json::Value) -> Result<Vec<String>, ToolOutput> {
    let max_count = optional_usize(input, "max_count")
        .unwrap_or(DEFAULT_GIT_LOG_MAX_COUNT)
        .clamp(1, MAX_GIT_LOG_MAX_COUNT);
    let mut args = vec![
        "log".to_owned(),
        format!("--max-count={max_count}"),
        "--date=iso".to_owned(),
        "--pretty=format:%h|%an|%ad|%s".to_owned(),
    ];
    if let Some(branch) = optional_str(input, "branch") {
        args.push(safe_value(branch)?);
    }
    push_path_filter(input, &mut args)?;
    Ok(args)
}

/// `git status` argv 组装（固定参数，无用户输入）。
fn status_args() -> Vec<String> {
    vec![
        "status".to_owned(),
        "--porcelain=v1".to_owned(),
        "--branch".to_owned(),
    ]
}

/// 追加 `-- <file_path>` 路径限定段。
fn push_path_filter(input: &serde_json::Value, args: &mut Vec<String>) -> Result<(), ToolOutput> {
    if let Some(file_path) = optional_str(input, "file_path") {
        let value = safe_value(file_path)?;
        args.push("--".to_owned());
        args.push(value);
    }
    Ok(())
}

/// 参数值卫生检查：拒绝以 `-` 开头的值（否则会被 git 当作选项解析）。
fn safe_value(value: &str) -> Result<String, ToolOutput> {
    if value.starts_with('-') {
        return Err(failure(
            "GIT_ARGUMENT_INVALID",
            format!("Argument must not start with '-': {value}"),
        ));
    }
    Ok(value.to_owned())
}

/// 统一执行出口（仓库目录解析 → argv 直调 `git` → 输出组装）。
async fn invoke(args: Vec<String>, input: serde_json::Value, ctx: ToolContext) -> ToolOutput {
    let repo = match optional_str(&input, "repo_path") {
        Some(raw) => resolve_path(raw, &ctx),
        None => ctx.working_dir().to_path_buf(),
    };
    if !repo.is_dir() {
        return failure(
            "GIT_REPO_NOT_FOUND",
            format!("Not a directory: {}", repo.display()),
        );
    }
    match run_program("git", &args, &repo, GIT_TIMEOUT, &ctx).await {
        Ok(outcome) => finish(&args, &repo, outcome),
        Err(error) => failure("GIT_SPAWN_FAILED", format!("git: {error}")),
    }
}

/// 结果组装（非零退出 → 错误结果携带 stderr；成功 → 截断后的 stdout）。
fn finish(args: &[String], repo: &Path, outcome: ProcessOutcome) -> ToolOutput {
    if outcome.timed_out {
        return failure("GIT_TIMEOUT", format!("git {} timed out", args.join(" ")));
    }
    if outcome.exit_code != 0 {
        let detail = if outcome.stderr.trim().is_empty() {
            outcome.stdout.clone()
        } else {
            outcome.stderr.clone()
        };
        return failure(
            "GIT_COMMAND_FAILED",
            format!("exit {}: {}", outcome.exit_code, detail.trim()),
        );
    }
    let (mut body, char_truncated) = truncate_chars(outcome.stdout, MAX_GIT_OUTPUT_CHARS);
    let truncated = char_truncated || outcome.truncated;
    if truncated {
        body.push_str(OUTPUT_TRUNCATED);
    }
    if body.trim().is_empty() {
        body.clear();
        body.push_str("(no output)");
    }
    let mut output = ToolOutput::ok(body);
    output.metadata = Some(json!({
        "structuredResult": {
            "repoPath": repo.display().to_string(),
            "argv": args,
            "truncated": truncated,
        }
    }));
    output
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx(working_dir: &std::path::Path) -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx).with_working_dir(working_dir)
    }

    fn plain_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-git-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        dir
    }

    fn git_gate_enabled() -> bool {
        std::env::var("ZK_RUN_GIT_TESTS").as_deref() == Ok("true")
    }

    #[test]
    fn diff_args_cover_three_modes() {
        assert_eq!(diff_args(&json!({})).expect("args"), ["diff"]);
        assert_eq!(
            diff_args(&json!({ "staged": true })).expect("args"),
            ["diff", "--cached"]
        );
        assert_eq!(
            diff_args(&json!({ "ref1": "abc", "stat": true })).expect("args"),
            ["diff", "--stat", "abc", "HEAD"]
        );
        assert_eq!(
            diff_args(&json!({ "file_path": "src/lib.rs" })).expect("args"),
            ["diff", "--", "src/lib.rs"]
        );
    }

    #[test]
    fn log_args_clamp_max_count_and_keep_format() {
        let args = log_args(&json!({})).expect("args");
        assert_eq!(args[0], "log");
        assert_eq!(args[1], "--max-count=20");
        assert!(args.contains(&"--pretty=format:%h|%an|%ad|%s".to_owned()));

        let clamped = log_args(&json!({ "max_count": 5000 })).expect("args");
        assert_eq!(clamped[1], "--max-count=100");

        let branch = log_args(&json!({ "branch": "main", "file_path": "a.rs" })).expect("args");
        assert_eq!(branch[branch.len() - 3..], ["main", "--", "a.rs"]);
    }

    #[test]
    fn option_like_argument_values_are_rejected() {
        for input in [
            json!({ "ref1": "--upload-pack=evil" }),
            json!({ "file_path": "-x" }),
        ] {
            let error = diff_args(&input).expect_err("rejected");
            assert!(error.is_error);
            assert!(error.content.starts_with("GIT_ARGUMENT_INVALID: "));
        }
        let error = log_args(&json!({ "branch": "--exec=evil" })).expect_err("rejected");
        assert!(error.content.starts_with("GIT_ARGUMENT_INVALID: "));
    }

    #[test]
    fn status_args_are_fixed_and_read_only() {
        assert_eq!(status_args(), ["status", "--porcelain=v1", "--branch"]);
    }

    #[tokio::test]
    async fn reports_missing_repository_directory() {
        let dir = plain_dir("missing");
        let output = GitStatusTool
            .execute(json!({ "repo_path": "no-such-dir" }), ctx(&dir))
            .await;
        assert!(output.is_error);
        assert!(output.content.starts_with("GIT_REPO_NOT_FOUND: "));
    }

    #[tokio::test]
    async fn surfaces_git_failure_outside_a_repository() {
        if !git_gate_enabled() {
            return;
        }
        // 只读失败路径：临时目录非 git 仓库，git 以非零码退出。
        let dir = plain_dir("not-a-repo");
        let output = GitStatusTool.execute(json!({}), ctx(&dir)).await;
        assert!(output.is_error, "{}", output.content);
        assert!(
            output.content.starts_with("GIT_COMMAND_FAILED: ")
                || output.content.starts_with("GIT_SPAWN_FAILED: "),
            "{}",
            output.content
        );
    }
}
