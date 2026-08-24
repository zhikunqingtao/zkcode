//! `Bash` 工具——在会话工作目录执行 shell 命令（直通模式）。
//!
//! 对照旧 `tool/impl/BashTool.java`（只读权威规格）：工具名 `Bash`、入参
//! `command` / `timeout`（毫秒）/ `description`、默认 120 000 ms、上限
//! 600 000 ms、`bash -c` 执行、输出预览上限 30 000 字符、stdout/stderr
//! 合并规则（`stdout + (stderr 空 ? "" : (stdout 空 ? "" : "\n") + stderr)`）、
//! 失败文本前缀 `"Exit code: N\n"`、超时退出码 137。
//!
//! **直通模式**（本阶段范围界定）：旧 `BashTool` 前置四层安全解析
//! （`BashCommandParser` → 命令白/黑名单 → 路径鉴权 → 注入检测）与权限
//! 询问链路属子阶段 2.4 / 2.5，本阶段**不做任何命令内容检查**，命令原样
//! 交给 [`crate::process::run_shell`]；2.4 落地后在 [`BashTool::run`] 的
//! 入口处接安全裁决，届时本注释一并更新（留痕 docs/compatibility.md §4）。

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
                    "description": "Timeout in milliseconds (default 120000, max 600000)."
                },
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

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { run(input, ctx).await })
    }
}

/// 执行主体（入参校验 → 绝对禁止最终防线 → Shell 状态包装 → 受控执行 →
/// 输出合并 / 截断 → 结果组装）。
async fn run(input: serde_json::Value, ctx: ToolContext) -> ToolOutput {
    let command = match required_str(&input, "command") {
        Ok(value) => value.to_owned(),
        Err(output) => return output,
    };
    let timeout_ms = u64::try_from(optional_usize(&input, "timeout").unwrap_or(0))
        .ok()
        .filter(|value| *value > 0)
        .unwrap_or(BASH_DEFAULT_TIMEOUT_MS)
        .min(BASH_MAX_TIMEOUT_MS);
    // 旧 `BashTool.java:337-341`：绝对禁止命令最终防线（纵深防御）——即使权限
    // 管线被绕过，ABSOLUTE_DENY 在此仍不可通行（硬安全不变量）。
    let block = BASH_BLACKLIST.check_command(&command);
    if block.level == BlockLevel::AbsoluteDeny {
        return failure(
            "COMMAND_ABSOLUTELY_DENIED",
            block.reason.unwrap_or_default(),
        );
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
            if !outcome.timed_out {
                ShellStateManager::update_state_from_snapshot(&session_id);
            }
            finish(&outcome, timeout_ms)
        }
        Err(error) => failure("BASH_SPAWN_FAILED", format!("{command}: {error}")),
    }
}

/// 结果组装（合并规则 + 30 000 字符预览 + `Exit code:` 前缀）。
fn finish(outcome: &ProcessOutcome, timeout_ms: u64) -> ToolOutput {
    let merged = merge(&outcome.stdout, &outcome.stderr);
    let (mut body, char_truncated) = truncate_chars(merged, BASH_MAX_OUTPUT_CHARS);
    if char_truncated || outcome.truncated {
        body.push_str(OUTPUT_TRUNCATED);
    }
    let content = if outcome.timed_out {
        format!("Command timed out after {timeout_ms} ms\nExit code: {TIMEOUT_EXIT_CODE}\n{body}")
    } else if outcome.exit_code == 0 {
        body
    } else {
        format!("Exit code: {}\n{body}", outcome.exit_code)
    };
    let mut output = if outcome.exit_code == 0 {
        ToolOutput::ok(content)
    } else {
        ToolOutput::error(content)
    };
    output.metadata = Some(json!({
        "structuredResult": {
            "exitCode": outcome.exit_code,
            "timedOut": outcome.timed_out,
            "cancelled": outcome.cancelled,
            "truncated": char_truncated || outcome.truncated,
        }
    }));
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
        ToolContext::new(CancellationToken::new(), tx).with_working_dir(std::env::temp_dir())
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
}
