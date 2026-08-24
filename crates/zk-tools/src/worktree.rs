//! `Worktree` 工具——git worktree 的 `add` / `list` / `remove` 三子命令。
//!
//! 对照旧 `tool/impl/WorktreeTool.java`（209L，只读权威规格）：
//! `subcommand` ∈ {add, list, remove} 必填、超时 300 000 ms、分组 `git`、
//! 权限 `ALWAYS_ASK`、`isReadOnly` 仅 `list` 为真、`isConcurrencySafe` 与
//! `isReadOnly` 同值；校验期缺 `agent_id` / `path` 分别回
//! `MISSING_AGENT_ID` / `MISSING_PATH`，未知子命令回 `INVALID_SUBCOMMAND`。
//! `list` 直调 `git worktree list` 并把输出裹进 `"Git Worktrees:\n…"`。
//!
//! 差异（留痕 docs/compatibility.md §4）：
//! - 旧 `add` / `remove` 委托 `WorktreeManager`（Spring bean，自持活跃映射）。
//!   zk-tools 不得依赖 zk-engine（依赖方向铁律），故本实现直调 `git worktree`
//!   CLI，与 `GitDiff` / `GitLog` / `GitStatus` 同一模式；「管理中的活跃
//!   worktree 数」一行随之退场（无自持映射可报）；
//! - `add` 的主入参由旧 `agent_id` 改为 `branch`（本批判据：`add <branch>
//!   [path]`），但**仍接受** `agent_id`——给出时分支名按旧
//!   `WorktreeManager.createWorktree` 的 `agent-{id}-{millis}` 形态生成，
//!   旧调用点无需改字；
//! - 路径缺省值对齐旧 `WorktreeManager`：`$TMPDIR/.zhikun-agent-<key>`。
//! - 全部经 `argv` 直调（不过 shell），且拒绝以 `-` 开头的入参值，避免分支名
//!   / 路径被 git 当作选项注入（同 [`crate::git`] 的 `safe_value` 口径）。

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::json;

use crate::input::{failure, optional_str, required_str, resolve_path, truncate_chars};
use crate::process::{ProcessOutcome, run_program};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// worktree 命令超时（旧 `WorktreeTool.getMaxExecutionTimeMs() = 300_000`）。
const WORKTREE_TIMEOUT: Duration = Duration::from_mins(5);

/// 输出字符上限（与 [`crate::git`] 同档）。
pub const MAX_WORKTREE_OUTPUT_CHARS: usize = 30_000;

/// worktree 临时目录前缀（对照旧 `WorktreeManager.WORKTREE_PREFIX`）。
pub const WORKTREE_PREFIX: &str = ".zhikun-agent-";

/// 三个合法子命令（旧 `SUBCOMMANDS`）。
pub const SUBCOMMANDS: [&str; 3] = ["add", "list", "remove"];

/// git worktree 管理工具（旧 `WorktreeTool`，名 `Worktree`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct WorktreeTool;

/// 解析后的子命令 + argv + 人类可读的成功前缀。
struct Plan {
    /// 传给 `git` 的完整 argv（含 `worktree` 首段）。
    argv: Vec<String>,
    /// 成功时置于输出首行的标题（`None` = 原样返回 git 输出）。
    heading: Option<String>,
}

impl Tool for WorktreeTool {
    fn name(&self) -> &'static str {
        "Worktree"
    }

    fn description(&self) -> &'static str {
        "Manage Git worktrees for parallel branch development. \
         Supports add, list, and remove operations."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "required": ["subcommand"],
            "properties": {
                "subcommand": {
                    "type": "string",
                    "enum": SUBCOMMANDS,
                    "description": "Worktree operation to perform: add, list, or remove"
                },
                "branch": {
                    "type": "string",
                    "description": "Branch to create for the new worktree (required for 'add' unless agent_id is given)"
                },
                "agent_id": {
                    "type": "string",
                    "description": "Agent identifier; the branch name is derived as agent-<id>-<millis>"
                },
                "path": {
                    "type": "string",
                    "description": "Worktree directory (required for 'remove'; defaults to a temp directory for 'add')"
                },
                "repo_path": {
                    "type": "string",
                    "description": "Repository path (default: the session working directory)."
                }
            }
        })
    }

    fn timeout(&self) -> Duration {
        WORKTREE_TIMEOUT
    }

    /// 仅 `list` 只读（旧 `isReadOnly` → `"list".equals(sub)`）。
    fn is_read_only(&self, input: &serde_json::Value) -> bool {
        optional_str(input, "subcommand") == Some("list")
    }

    /// `add` / `remove` 动工作树（旧权限要求 `ALWAYS_ASK`）；`list` 只读。
    fn is_destructive(&self, input: &serde_json::Value) -> bool {
        !self.is_read_only(input)
    }

    /// 自报路径入参（`remove` / 显式 `add` 的目标目录），供授权链的
    /// `FileAnalyzer` 优先采信。
    fn path_of(&self, input: &serde_json::Value) -> Option<String> {
        optional_str(input, "path").map(str::to_owned)
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            match plan(&input) {
                Ok(plan) => invoke(plan, &input, ctx).await,
                Err(output) => output,
            }
        })
    }
}

/// 子命令校验 + argv 组装（旧 `validateInput` 与 `call` 的合并等价物）。
fn plan(input: &serde_json::Value) -> Result<Plan, ToolOutput> {
    let subcommand = required_str(input, "subcommand").map_err(|_| {
        failure(
            "INVALID_SUBCOMMAND",
            format!("subcommand must be one of: {}", SUBCOMMANDS.join(", ")),
        )
    })?;
    match subcommand {
        "list" => Ok(Plan {
            argv: vec![
                "worktree".to_owned(),
                "list".to_owned(),
                "--porcelain".to_owned(),
            ],
            heading: Some("Git Worktrees:".to_owned()),
        }),
        "add" => plan_add(input),
        "remove" => plan_remove(input),
        other => Err(failure(
            "WORKTREE_SUBCOMMAND_INVALID",
            format!("Unknown subcommand: {other}"),
        )),
    }
}

/// `add`：分支名取 `branch`，缺省时由 `agent_id` 派生；路径缺省落临时目录。
fn plan_add(input: &serde_json::Value) -> Result<Plan, ToolOutput> {
    let (branch, key) = if let Some(branch) = optional_str(input, "branch") {
        (safe_value(branch)?, safe_value(branch)?)
    } else {
        let agent_id = optional_str(input, "agent_id").ok_or_else(|| {
            failure(
                "MISSING_AGENT_ID",
                "branch (or agent_id) is required for 'add' subcommand",
            )
        })?;
        let agent_id = safe_value(agent_id)?;
        (derive_branch(&agent_id), agent_id)
    };
    let path = match optional_str(input, "path") {
        Some(raw) => safe_value(raw)?,
        None => default_worktree_path(&key).to_string_lossy().into_owned(),
    };
    Ok(Plan {
        argv: vec![
            "worktree".to_owned(),
            "add".to_owned(),
            "-b".to_owned(),
            branch.clone(),
            path.clone(),
            "HEAD".to_owned(),
        ],
        heading: Some(format!("Worktree created.\nBranch: {branch}\nPath: {path}")),
    })
}

/// `remove`：路径必填（旧 `MISSING_PATH`），`--force` 对齐旧
/// `WorktreeManager.removeWorktree`。
fn plan_remove(input: &serde_json::Value) -> Result<Plan, ToolOutput> {
    let path = optional_str(input, "path")
        .ok_or_else(|| failure("MISSING_PATH", "path is required for 'remove' subcommand"))?;
    let path = safe_value(path)?;
    Ok(Plan {
        argv: vec![
            "worktree".to_owned(),
            "remove".to_owned(),
            "--force".to_owned(),
            path.clone(),
        ],
        heading: Some(format!("Worktree removed: {path}")),
    })
}

/// 由 agent id 派生分支名（对照旧 `agent-{agentId}-{timestamp}`）。
fn derive_branch(agent_id: &str) -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    format!("agent-{agent_id}-{millis}")
}

/// worktree 路径缺省值（对照旧 `tmpdir/.zhikun-agent-<key>`）；`key` 中的
/// 路径分隔符先剥掉，避免落到 `$TMPDIR` 之外。
fn default_worktree_path(key: &str) -> PathBuf {
    let slug: String = key
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-' || *ch == '_')
        .collect();
    std::env::temp_dir().join(format!("{WORKTREE_PREFIX}{slug}"))
}

/// 参数值卫生检查：拒绝以 `-` 开头的值（同 [`crate::git`] 口径）。
fn safe_value(value: &str) -> Result<String, ToolOutput> {
    if value.starts_with('-') {
        return Err(failure(
            "WORKTREE_ARGUMENT_INVALID",
            format!("Argument must not start with '-': {value}"),
        ));
    }
    Ok(value.to_owned())
}

/// 统一执行出口（仓库目录解析 → argv 直调 `git` → 输出组装）。
async fn invoke(plan: Plan, input: &serde_json::Value, ctx: ToolContext) -> ToolOutput {
    let repo = match optional_str(input, "repo_path") {
        Some(raw) => resolve_path(raw, &ctx),
        None => ctx.working_dir().to_path_buf(),
    };
    if !repo.is_dir() {
        return failure(
            "WORKTREE_REPO_NOT_FOUND",
            format!("Not a directory: {}", repo.display()),
        );
    }
    match run_program("git", &plan.argv, &repo, WORKTREE_TIMEOUT, &ctx).await {
        Ok(outcome) => finish(&plan, &repo, outcome),
        Err(error) => failure("WORKTREE_SPAWN_FAILED", format!("git: {error}")),
    }
}

/// 结果组装（超时 / 非零退出 → 错误；成功 → 标题 + 截断后的 git 输出）。
fn finish(plan: &Plan, repo: &Path, outcome: ProcessOutcome) -> ToolOutput {
    if outcome.timed_out {
        return failure(
            "WORKTREE_TIMEOUT",
            format!("git {} timed out", plan.argv.join(" ")),
        );
    }
    if outcome.exit_code != 0 {
        let detail = if outcome.stderr.trim().is_empty() {
            outcome.stdout.clone()
        } else {
            outcome.stderr.clone()
        };
        return failure(
            "WORKTREE_COMMAND_FAILED",
            format!("exit {}: {}", outcome.exit_code, detail.trim()),
        );
    }
    let (body, char_truncated) = truncate_chars(outcome.stdout, MAX_WORKTREE_OUTPUT_CHARS);
    let truncated = char_truncated || outcome.truncated;
    let mut text = match &plan.heading {
        Some(heading) => {
            let trimmed = body.trim_end();
            if trimmed.is_empty() {
                heading.clone()
            } else {
                format!("{heading}\n{trimmed}")
            }
        }
        None => body,
    };
    if truncated {
        text.push_str("\n[Output truncated]");
    }
    let mut output = ToolOutput::ok(text);
    output.metadata = Some(json!({
        "structuredResult": {
            "repoPath": repo.display().to_string(),
            "argv": plan.argv,
            "truncated": truncated,
        }
    }));
    output
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx(working_dir: &Path) -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx).with_working_dir(working_dir)
    }

    fn plain_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-worktree-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        dir
    }

    /// `list` 走 porcelain 固定 argv，无用户入参参与。
    #[test]
    fn list_argv_is_fixed() {
        let plan = plan(&json!({ "subcommand": "list" })).expect("plan");
        assert_eq!(plan.argv, ["worktree", "list", "--porcelain"]);
        assert_eq!(plan.heading.as_deref(), Some("Git Worktrees:"));
        assert!(WorktreeTool.is_read_only(&json!({ "subcommand": "list" })));
        assert!(!WorktreeTool.is_destructive(&json!({ "subcommand": "list" })));
    }

    /// `add` 显式分支 + 显式路径 → argv 逐段可预期。
    #[test]
    fn add_argv_uses_explicit_branch_and_path() {
        let plan = plan(&json!({
            "subcommand": "add",
            "branch": "feature/x",
            "path": "/tmp/zk-wt-explicit"
        }))
        .expect("plan");
        assert_eq!(
            plan.argv,
            [
                "worktree",
                "add",
                "-b",
                "feature/x",
                "/tmp/zk-wt-explicit",
                "HEAD"
            ]
        );
        assert!(WorktreeTool.is_destructive(&json!({ "subcommand": "add" })));
        assert_eq!(
            WorktreeTool.path_of(&json!({ "subcommand": "add", "path": "/tmp/zk-wt-explicit" })),
            Some("/tmp/zk-wt-explicit".to_owned())
        );
    }

    /// `add` 缺 `branch` 时由 `agent_id` 派生分支 + 临时目录路径（旧形态）。
    #[test]
    fn add_derives_branch_and_path_from_agent_id() {
        let plan = plan(&json!({ "subcommand": "add", "agent_id": "coder-1" })).expect("plan");
        assert_eq!(plan.argv[0..3], ["worktree", "add", "-b"]);
        assert!(
            plan.argv[3].starts_with("agent-coder-1-"),
            "{}",
            plan.argv[3]
        );
        let expected = std::env::temp_dir().join(format!("{WORKTREE_PREFIX}coder-1"));
        assert_eq!(plan.argv[4], expected.to_string_lossy());
        assert_eq!(plan.argv[5], "HEAD");
    }

    /// `remove` 强制 `--force`（旧 `WorktreeManager.removeWorktree`）。
    #[test]
    fn remove_argv_forces_removal() {
        let plan =
            plan(&json!({ "subcommand": "remove", "path": "/tmp/zk-wt-gone" })).expect("plan");
        assert_eq!(
            plan.argv,
            ["worktree", "remove", "--force", "/tmp/zk-wt-gone"]
        );
    }

    /// 缺参 / 未知子命令的错误码逐条对齐旧 `validateInput`。
    #[test]
    fn missing_and_invalid_parameters_match_legacy_codes() {
        for (input, code) in [
            (json!({}), "INVALID_SUBCOMMAND"),
            (json!({ "subcommand": "  " }), "INVALID_SUBCOMMAND"),
            (json!({ "subcommand": "add" }), "MISSING_AGENT_ID"),
            (json!({ "subcommand": "remove" }), "MISSING_PATH"),
            (
                json!({ "subcommand": "prune" }),
                "WORKTREE_SUBCOMMAND_INVALID",
            ),
        ] {
            let error = plan(&input).err().expect("rejected");
            assert!(error.is_error);
            assert!(
                error.content.starts_with(&format!("{code}: ")),
                "{} did not start with {code}",
                error.content
            );
        }
    }

    /// 选项形参数值被拒（分支名 / 路径 / agent id 三处）。
    #[test]
    fn option_like_argument_values_are_rejected() {
        for input in [
            json!({ "subcommand": "add", "branch": "--upload-pack=evil" }),
            json!({ "subcommand": "add", "branch": "ok", "path": "-x" }),
            json!({ "subcommand": "add", "agent_id": "-x" }),
            json!({ "subcommand": "remove", "path": "--force" }),
        ] {
            let error = plan(&input).err().expect("rejected");
            assert!(
                error.content.starts_with("WORKTREE_ARGUMENT_INVALID: "),
                "{}",
                error.content
            );
        }
    }

    /// 缺省路径不因 `agent_id` 含分隔符而逃出 `$TMPDIR`。
    #[test]
    fn default_path_slug_strips_separators() {
        let path = default_worktree_path("../../etc/x");
        assert_eq!(
            path,
            std::env::temp_dir().join(format!("{WORKTREE_PREFIX}etcx"))
        );
    }

    /// 仓库目录不存在 → `WORKTREE_REPO_NOT_FOUND`。
    #[tokio::test]
    async fn reports_missing_repository_directory() {
        let dir = plain_dir("missing");
        let output = WorktreeTool
            .execute(
                json!({ "subcommand": "list", "repo_path": "no-such-dir" }),
                ctx(&dir),
            )
            .await;
        assert!(output.is_error);
        assert!(output.content.starts_with("WORKTREE_REPO_NOT_FOUND: "));
    }

    /// 非 git 仓库内执行 → git 非零退出被如实上报。
    #[tokio::test]
    async fn surfaces_git_failure_outside_a_repository() {
        let dir = plain_dir("not-a-repo");
        let output = WorktreeTool
            .execute(json!({ "subcommand": "list" }), ctx(&dir))
            .await;
        assert!(output.is_error, "{}", output.content);
        assert!(
            output.content.starts_with("WORKTREE_COMMAND_FAILED: ")
                || output.content.starts_with("WORKTREE_SPAWN_FAILED: "),
            "{}",
            output.content
        );
    }
}
