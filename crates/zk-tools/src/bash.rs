//! Native Bash: authorization-bound cwd, supervised process groups and explicit artifacts.
//!
//! Foreground commands retain persistent session cwd; temporary sessions use a
//! RAM scope and an anonymous control socket. A host adapter maps background
//! requests to attached Shell Tasks. Command recovery is diagnostic only.

pub mod ast;
pub mod blacklist;
pub mod category;
pub mod classifier;
pub mod heredoc;
mod javastr;
pub mod lexer;
pub mod parser;
pub mod path_validator;
pub mod security;
pub mod sed_validator;
pub mod shell_state;

use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::json;

use crate::input::{failure, optional_usize, required_str, truncate_chars};
use crate::process::{ProcessOutcome, TIMEOUT_EXIT_CODE, run_shell};
use crate::tool::{MAX_TOOL_TIMEOUT, Tool, ToolContext, ToolOutput};

use self::ast::ParseForSecurityResult;
use self::blacklist::{BlockLevel, CommandBlacklistService};
use self::classifier::BashCommandClassifier;
use self::security::BashSecurityAnalyzer;
use self::shell_state::ShellStateManager;
mod background;
mod declared_outputs;
pub use declared_outputs::DeclaredOutputReceipt;
mod memory_state;
mod recovery;
pub use background::{BackgroundBashTool, BackgroundShellPort};

/// 共享安全解析器（旧 `BashTool` 由 Spring 注入单例 `BashSecurityAnalyzer`）。
static BASH_SECURITY: LazyLock<BashSecurityAnalyzer> = LazyLock::new(BashSecurityAnalyzer::new);

/// 共享命令黑名单服务（旧 `BashTool` 注入单例 `CommandBlacklistService`）。
static BASH_BLACKLIST: LazyLock<CommandBlacklistService> =
    LazyLock::new(CommandBlacklistService::new);

/// 共享正则分类器（旧 `BashTool` 注入单例 `BashCommandClassifier`；无状态）。
static BASH_CLASSIFIER: BashCommandClassifier = BashCommandClassifier::new();

/// Process-wide shell-state initializer. The wrapper creates its private CWD
/// snapshot with `mktemp`, so the parent directory must exist before every
/// first Bash invocation.
static SHELL_STATE: LazyLock<ShellStateManager> = LazyLock::new(ShellStateManager::new);

/// 默认超时（旧 `BASH_DEFAULT_TIMEOUT_MS = 120_000`）。
pub const BASH_DEFAULT_TIMEOUT_MS: u64 = 120_000;

/// 超时上限（旧 `BASH_MAX_TIMEOUT_MS = 600_000`）。
pub const BASH_MAX_TIMEOUT_MS: u64 = 600_000;

/// 输出预览字符上限（旧 `MAX_OUTPUT_CHARS = 30_000`）。
pub const BASH_MAX_OUTPUT_CHARS: usize = 30_000;

/// 输出截断标记（旧超限提示逐字）。
const OUTPUT_TRUNCATED: &str = "\n[Output truncated]";

/// shell 命令执行工具（名 `Bash`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct BashTool;

impl BashTool {
    /// Add the production `TaskRuntime` adapter without changing ordinary Bash callers.
    pub fn with_background_backend(
        port: std::sync::Arc<dyn BackgroundShellPort>,
    ) -> BackgroundBashTool {
        BackgroundBashTool::new(port)
    }
}

impl Tool for BashTool {
    fn name(&self) -> &'static str {
        "Bash"
    }

    fn description(&self) -> &'static str {
        "Execute a shell command in the session working directory. \
         Output combines stdout and stderr; long output is truncated."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "command": { "type": "string", "description": "The shell command to execute." },
                "timeout": {
                    "type": "integer",
                    "description": "Timeout in milliseconds. Explicit values take precedence; otherwise build/install use 300000, tests use 600000 and other commands retain at least 120000. Max 600000, further bounded by the owning task deadline."
                },
                "declared_outputs": {"type":"array","maxItems":32,"description":"Foreground-only file effects frozen before execution and sealed afterward. Paths are relative to the actual shell cwd; no undeclared effect becomes an artifact.","items":{"type":"object","properties":{"path":{"type":"string"},"operation":{"type":"string","enum":["created","modified","deleted"]},"requiredValidatorId":{"type":"string"}},"required":["path","operation"]}},
                "is_background": {"type":"boolean", "description":"Create an attached managed shell task; use TaskOutput/TaskStop for output and cancellation."},
                "description": {
                    "type": "string",
                    "description": "Short human-readable description of what the command does."
                }
            },
            "required": ["command"]
        })
    }

    /// 返回执行器侧上限（600s）而非默认 120s：per-call 超时由本工具自身
    /// 按入参 `timeout` 精确执行（并负责进程组终止），执行器只做兜底，
    /// 不得抢先在 120s 处掐断一个合法的 600s 长命令。
    fn timeout(&self) -> Duration {
        MAX_TOOL_TIMEOUT
    }

    /// 只读判定（旧 `BashTool.java:269-295` [v1.57.0 G2]）：AST 遍历全部子命令
    /// argv[0]，全为 search/read/list（或整条命令过 `isReadOnlyCommand` 二次
    /// 判定）→ 整体只读；解析失败 / too-complex → 降级到正则分类器。
    fn is_read_only(&self, input: &serde_json::Value) -> bool {
        if input
            .get("declared_outputs")
            .is_some_and(|value| value.as_array().is_none_or(|items| !items.is_empty()))
        {
            return false;
        }
        let command = input.get("command").and_then(serde_json::Value::as_str);
        if let ParseForSecurityResult::Simple { commands } =
            BASH_SECURITY.parse_for_security(command)
        {
            return commands.iter().all(|cmd| {
                let argv0 = cmd.argv.first().map_or("", String::as_str);
                if BASH_CLASSIFIER.is_search_or_read_command(Some(argv0)) {
                    return true;
                }
                // 二次判定：拼接完整命令调用 is_read_only_command
                // （覆盖 git/docker/gh 等只读子命令）。
                let full_cmd = cmd.argv.join(" ");
                BASH_CLASSIFIER.is_read_only_command(Some(&full_cmd))
            });
        }
        BASH_CLASSIFIER.classify(command).is_read_only()
    }

    /// 破坏性判定（旧 `BashTool.java:302-312` [v1.57.0 G3]）：统一走命令黑名单，
    /// `HIGH_RISK_ASK` 或 `ABSOLUTE_DENY` 即视为破坏性。
    fn is_destructive(&self, input: &serde_json::Value) -> bool {
        let command = input.get("command").and_then(serde_json::Value::as_str);
        let Some(command) = command else { return false };
        if command.trim().is_empty() {
            return false;
        }
        let level = BASH_BLACKLIST.check_command(command).level;
        level == BlockLevel::HighRiskAsk || level == BlockLevel::AbsoluteDeny
    }

    fn produces_declared_artifacts(&self) -> bool {
        true
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { run(input, ctx).await })
    }
}

/// Preserve the Rust default floor while activating the existing source classifier.
pub(super) fn resolve_timeout(input: &serde_json::Value, command: &str) -> u64 {
    u64::try_from(optional_usize(input, "timeout").unwrap_or(0))
        .ok()
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            BASH_CLASSIFIER
                .classify_for_timeout(Some(command))
                .recommended_timeout_ms()
                .max(BASH_DEFAULT_TIMEOUT_MS)
        })
        .min(BASH_MAX_TIMEOUT_MS)
}

/// 执行主体（入参校验 → 绝对禁止最终防线 → Shell 状态包装 → 受控执行 →
/// 输出合并 / 截断 → 结果组装）。
async fn run(input: serde_json::Value, ctx: ToolContext) -> ToolOutput {
    if input
        .get("is_background")
        .is_some_and(|value| !value.is_boolean())
    {
        return failure("BASH_BACKGROUND_INVALID", "is_background must be a boolean");
    }
    if input
        .get("is_background")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        return failure(
            "BASH_BACKGROUND_UNAVAILABLE",
            "Managed background tasks are unavailable in this execution scope",
        );
    }
    let command = match required_str(&input, "command") {
        Ok(value) => value.to_owned(),
        Err(output) => return output,
    };
    let timeout_ms = resolve_timeout(&input, &command);
    // 旧 `BashTool.java:337-341`：绝对禁止命令最终防线（纵深防御）——即使权限
    // 管线被绕过，ABSOLUTE_DENY 在此仍不可通行（硬安全不变量）。
    let block = BASH_BLACKLIST.check_command(&command);
    if block.level == BlockLevel::AbsoluteDeny {
        return failure(
            "COMMAND_ABSOLUTELY_DENIED",
            block.reason.unwrap_or_default(),
        );
    }
    if ctx.is_ephemeral() {
        return run_memory(&input, &command, timeout_ms, &ctx).await;
    }
    LazyLock::force(&SHELL_STATE);
    // 旧 `BashTool.java:344-346`：Shell 状态包装 + 跨调用 CWD 解析。
    // `context.sessionId()` 为 null 时旧源的字符串拼接产出 `"null.cwd"`，
    // 此处以 `"null"` 占位逐字对齐。
    let session_id = ctx.session_id().unwrap_or("null").to_owned();
    let wrapped = ShellStateManager::wrap_command(&command, &session_id);
    let working_dir = PathBuf::from(ShellStateManager::resolve_working_directory(
        &session_id,
        &ctx.working_dir().to_string_lossy(),
    ));
    if !authorized_cwd_matches(&ctx, &working_dir) {
        return failure(
            "BASH_WORKING_DIRECTORY_CHANGED",
            "Shell cwd changed after authorization; authorize the command again",
        );
    }
    let declared = match declared_outputs::freeze(&input, &working_dir, ctx.working_dir()) {
        Ok(value) => value,
        Err(error) => return error,
    };
    let outcome = run_shell(
        &wrapped,
        &working_dir,
        Duration::from_millis(timeout_ms),
        &ctx,
    )
    .await;
    match outcome {
        Ok(outcome) => {
            // 旧 `BashTool.java:408`：非超时路径确认本次 CWD 状态已更新。
            if !outcome.timed_out && outcome.termination_confirmed {
                ShellStateManager::update_state_from_snapshot(&session_id);
            }
            declared_outputs::seal(declared, finish(&outcome, timeout_ms))
        }
        Err(error) => failure("BASH_SPAWN_FAILED", format!("{command}: {error}")),
    }
}

async fn run_memory(
    input: &serde_json::Value,
    command: &str,
    timeout_ms: u64,
    ctx: &ToolContext,
) -> ToolOutput {
    let state = match memory_state::acquire(ctx) {
        Ok(state) => state,
        Err(code) => return failure(code, "Temporary shell scope is not active"),
    };
    // Do not silently execute a queued command against a cwd changed by another
    // call after authorization. The caller can retry and obtain fresh facts.
    let Ok(_serial) = state.serial.try_lock() else {
        return failure(
            "SHELL_COMMAND_ALREADY_RUNNING",
            "Another command owns this session shell state",
        );
    };
    let cwd = match state.cwd() {
        Ok(cwd) => cwd,
        Err(code) => {
            return failure(
                code,
                "Shell cwd is unavailable; explicitly reset the working directory",
            );
        }
    };
    if !authorized_cwd_matches(ctx, &cwd) {
        return failure(
            "BASH_WORKING_DIRECTORY_CHANGED",
            "Shell cwd changed after authorization; authorize the command again",
        );
    }
    let declared = match declared_outputs::freeze(input, &cwd, ctx.working_dir()) {
        Ok(value) => value,
        Err(error) => return error,
    };
    match crate::process::run_shell_memory(command, &cwd, Duration::from_millis(timeout_ms), ctx)
        .await
    {
        Ok((outcome, cwd)) => {
            let updated = state.update(cwd);
            let mut output = finish(&outcome, timeout_ms);
            if let Err(code) = updated {
                output.is_error = true;
                let _ = write!(
                    output.content,
                    "\n{code}: final working directory was not confirmed; reset it before another command. Command effects must not be retried automatically."
                );
                if let Some(metadata) = output.metadata.as_mut() {
                    metadata["structuredResult"]["retryability"] = json!("NEVER");
                    metadata["structuredResult"]["shellStateConfirmed"] = json!(false);
                }
            }
            declared_outputs::seal(declared, output)
        }
        Err(error) => failure("BASH_SPAWN_FAILED", error.to_string()),
    }
}

fn authorized_cwd_matches(ctx: &ToolContext, cwd: &std::path::Path) -> bool {
    ctx.authorized_shell_cwd().is_none_or(|authorized| {
        cwd.canonicalize()
            .is_ok_and(|current| current == authorized)
    })
}

/// 结果组装（合并规则 + 30 000 字符预览 + `Exit code:` 前缀）。
fn finish(outcome: &ProcessOutcome, timeout_ms: u64) -> ToolOutput {
    let merged = merge(&outcome.stdout, &outcome.stderr);
    let (mut body, char_truncated) = truncate_chars(merged, BASH_MAX_OUTPUT_CHARS);
    if char_truncated || outcome.truncated {
        body.push_str(OUTPUT_TRUNCATED);
    }
    let content = if !outcome.termination_confirmed {
        format!(
            "PROCESS_TERMINATION_UNCONFIRMED: command effects are unknown; inspect before retrying\n{body}"
        )
    } else if outcome.timed_out {
        format!("Command timed out after {timeout_ms} ms\nExit code: {TIMEOUT_EXIT_CODE}\n{body}")
    } else if outcome.exit_code == 0 {
        body
    } else {
        format!("Exit code: {}\n{body}", outcome.exit_code)
    };
    let mut output = if outcome.exit_code == 0 && outcome.termination_confirmed {
        ToolOutput::ok(content)
    } else {
        ToolOutput::error(content)
    };
    output.metadata = Some(json!({
        "structuredResult": {
            "exitCode": outcome.exit_code,
            "terminationConfirmed": outcome.termination_confirmed,
            "retryability": if outcome.termination_confirmed { "unspecified" } else { "NEVER" },
            "effectState": if outcome.termination_confirmed { "confirmed" } else { "UNKNOWN" },
            "timedOut": outcome.timed_out,
            "cancelled": outcome.cancelled,
            "truncated": char_truncated || outcome.truncated,
        }
    }));
    if output.is_error
        && let Some(metadata) = output.metadata.as_mut()
    {
        let recovery = recovery::classify(outcome);
        metadata["failure_category"] = json!(recovery.category);
        metadata["failure_suggestion"] = json!(recovery.suggestion);
        metadata["structuredResult"]["code"] = json!(recovery.code);
        metadata["structuredResult"]["retryability"] = json!("NEVER");
        metadata["structuredResult"]["effectState"] = json!("UNKNOWN");
    }
    output
}

/// stdout / stderr 合并（旧规则逐字：仅在两者皆非空时插入换行）。
fn merge(stdout: &str, stderr: &str) -> String {
    if stderr.is_empty() {
        return stdout.to_owned();
    }
    if stdout.is_empty() {
        return stderr.to_owned();
    }
    format!("{stdout}\n{stderr}")
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
            .with_working_dir(std::env::temp_dir())
            .with_session_id(format!("bash-unit-{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn temporary_shell_has_ram_cwd_literal_commands_and_no_snapshot_files() {
        let root = std::env::temp_dir().join(format!("zk-memory-shell-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("child 空格")).unwrap();
        let root = root.canonicalize().unwrap();
        let session = uuid::Uuid::new_v4().to_string();
        let guard = memory_state::fixture_scope(&session, &root);
        let context = ctx()
            .with_session_id(&session)
            .with_working_dir(&root)
            .with_ephemeral_content(true);
        let cwd_file = ShellStateManager::cwd_tracking_path(&session);
        let first = BashTool.execute(json!({"command": "cd 'child 空格'; printf '%s' 'literal $(touch unexpected)'; printf err >&2; exit 7"}), context.clone()).await;
        assert!(first.is_error);
        assert_eq!(
            first.content,
            "Exit code: 7\nliteral $(touch unexpected)\nerr"
        );
        assert!(!root.join("child 空格/unexpected").exists());
        assert_eq!(
            ShellStateManager::resolve_working_directory(&session, root.to_str().unwrap()),
            root.join("child 空格").to_str().unwrap()
        );
        let stale = BashTool.execute(json!({"command":"touch must-not-run", "authorized_shell_cwd":root.join("child 空格")}),
            context.clone().with_authorized_shell_cwd(&root)).await;
        assert!(stale.is_error && stale.content.contains("BASH_WORKING_DIRECTORY_CHANGED"));
        assert!(!root.join("child 空格/must-not-run").exists());
        let second = BashTool
            .execute(
                json!({"command": "pwd -P; printf 'authorized file' > result.txt"}),
                context.clone(),
            )
            .await;
        assert!(!second.is_error, "{}", second.content);
        assert_eq!(
            second.content,
            format!("{}\n", root.join("child 空格").display())
        );
        assert_eq!(
            std::fs::read_to_string(root.join("child 空格/result.txt")).unwrap(),
            "authorized file"
        );
        assert!(!cwd_file.exists());
        if let Ok(entries) = std::fs::read_dir(ShellStateManager::state_directory()) {
            assert!(
                !entries
                    .flatten()
                    .any(|entry| entry.file_name().to_string_lossy().starts_with(&session))
            );
        }
        drop(guard);
        let closed = BashTool
            .execute(json!({"command": "touch must-not-run"}), context)
            .await;
        assert!(closed.is_error && closed.content.contains("SHELL_MEMORY_SCOPE_REQUIRED"));
        assert!(!root.join("must-not-run").exists());
        assert!(root.join("child 空格/result.txt").is_file());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn temporary_shell_missing_cwd_is_explicit_and_reset_stays_in_memory() {
        let root =
            std::env::temp_dir().join(format!("zk-memory-shell-reset-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let session = uuid::Uuid::new_v4().to_string();
        let _guard = memory_state::fixture_scope(&session, &root);
        let context = ctx()
            .with_session_id(&session)
            .with_working_dir(&root)
            .with_ephemeral_content(true);
        let first = BashTool
            .execute(
                json!({"command": "trap - EXIT; printf done"}),
                context.clone(),
            )
            .await;
        assert!(first.is_error && first.content.starts_with("done\nSHELL_CWD_UNCONFIRMED"));
        let denied = BashTool
            .execute(json!({"command": "touch must-not-run"}), context.clone())
            .await;
        assert!(denied.is_error && !root.join("must-not-run").exists());
        ShellStateManager::reset_cwd(&session, root.to_str().unwrap());
        let valid = BashTool
            .execute(json!({"command": "printf ready"}), context)
            .await;
        assert!(!valid.is_error, "{}", valid.content);
        assert_eq!(valid.content, "ready");
        assert!(!ShellStateManager::cwd_tracking_path(&session).exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn timeout_recommendations_preserve_explicit_values_and_the_existing_default_floor() {
        for (command, expected) in [
            ("cat file", 120_000),
            ("cargo build", 300_000),
            ("npm install", 300_000),
            ("cargo test", 600_000),
        ] {
            assert_eq!(resolve_timeout(&json!({}), command), expected);
            assert_eq!(resolve_timeout(&json!({"timeout":7000}), command), 7000);
            assert_eq!(
                resolve_timeout(&json!({"timeout":900_000}), command),
                600_000
            );
        }
    }

    #[tokio::test]
    async fn recovery_suggestions_classify_facts_without_retrying_or_confusing_signals_with_timeouts()
     {
        for (command, code, category) in [
            (
                "printf 'no space left on device' >&2; exit 1",
                "BASH_DISK_FULL",
                "NEEDS_HUMAN",
            ),
            (
                "printf 'connection refused' >&2; exit 1",
                "BASH_NETWORK_ERROR",
                "RETRYABLE",
            ),
            ("exit 143", "BASH_SIGNAL_TERMINATED", "NON_RETRYABLE"),
            ("exit 127", "BASH_COMMAND_NOT_FOUND", "NON_RETRYABLE"),
        ] {
            let output = BashTool.execute(json!({"command":command}), ctx()).await;
            assert!(output.is_error);
            let meta = output.metadata.unwrap();
            assert_eq!(meta["failure_category"], category);
            assert_eq!(meta["structuredResult"]["code"], code);
            assert_eq!(meta["structuredResult"]["retryability"], "NEVER");
            assert_eq!(meta["structuredResult"]["effectState"], "UNKNOWN");
            assert!(meta["failure_suggestion"].as_str().unwrap().len() > 10);
        }
    }

    #[tokio::test]
    async fn runs_simple_command_in_working_directory() {
        let output = BashTool.execute(json!({ "command": "pwd" }), ctx()).await;
        assert!(!output.is_error, "{}", output.content);
        let expected = std::fs::canonicalize(std::env::temp_dir()).expect("canonical");
        assert_eq!(
            output.content.trim(),
            expected.to_string_lossy().trim_end_matches('/')
        );
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["structuredResult"]["exitCode"], 0);
    }

    #[tokio::test]
    async fn merges_stderr_and_reports_exit_code() {
        let output = BashTool
            .execute(
                json!({ "command": "echo out; echo err 1>&2; exit 7" }),
                ctx(),
            )
            .await;
        assert!(output.is_error);
        assert_eq!(output.content, "Exit code: 7\nout\n\nerr\n");
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["structuredResult"]["exitCode"], 7);
    }

    #[tokio::test]
    async fn timeout_yields_error_with_exit_code_137() {
        let output = BashTool
            .execute(json!({ "command": "sleep 30", "timeout": 200 }), ctx())
            .await;
        assert!(output.is_error);
        assert!(
            output
                .content
                .starts_with("Command timed out after 200 ms\n")
        );
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["structuredResult"]["timedOut"], true);
        assert_eq!(metadata["structuredResult"]["exitCode"], TIMEOUT_EXIT_CODE);
    }

    #[tokio::test]
    async fn truncates_large_output_at_preview_cap() {
        let output = BashTool
            .execute(
                json!({ "command": "for i in $(seq 1 5000); do echo 0123456789; done" }),
                ctx(),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.ends_with(OUTPUT_TRUNCATED));
        assert!(output.content.chars().count() <= BASH_MAX_OUTPUT_CHARS + OUTPUT_TRUNCATED.len());
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["structuredResult"]["truncated"], true);
    }

    #[tokio::test]
    async fn rejects_missing_command_and_clamps_timeout() {
        let missing = BashTool.execute(json!({}), ctx()).await;
        assert!(missing.is_error);
        assert!(missing.content.starts_with("MISSING_PARAMETER: "));

        // 超上限的 timeout 被钳制到 600 000 ms，命令照常执行。
        let clamped = BashTool
            .execute(json!({ "command": "echo ok", "timeout": 9_000_000 }), ctx())
            .await;
        assert_eq!(clamped.content, "ok\n");
    }

    #[test]
    fn merge_follows_legacy_rule() {
        assert_eq!(merge("a\n", ""), "a\n");
        assert_eq!(merge("", "b\n"), "b\n");
        assert_eq!(merge("a\n", "b\n"), "a\n\nb\n");
        assert_eq!(merge("", ""), "");
    }
    #[test]
    fn unconfirmed_scope_is_never_a_success_or_retryable_result() {
        let outcome = crate::process::ProcessOutcome {
            stdout: "partial evidence".into(),
            stderr: String::new(),
            exit_code: 0,
            timed_out: false,
            cancelled: false,
            truncated: false,
            termination_confirmed: false,
        };
        let output = super::finish(&outcome, 1000);
        assert!(output.is_error);
        assert!(output.content.contains("PROCESS_TERMINATION_UNCONFIRMED"));
        assert!(output.content.contains("partial evidence"));
        let result = &output.metadata.unwrap()["structuredResult"];
        assert_eq!(result["terminationConfirmed"], false);
        assert_eq!(result["retryability"], "NEVER");
        assert_eq!(result["effectState"], "UNKNOWN");
    }
}
