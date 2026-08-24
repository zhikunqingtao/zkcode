//! `VerifyPlanExecution` 工具——验证计划模式下的任务执行结果（Batch 7）。
//!
//! 语义来源（旧仓库只读）：`VerifyPlanExecutionTool.java`（277L）。
//! 四种验证类型：`file_exists` / `content_matches` / `test_passes` / `command_succeeds`。
//!
//! # 有意差异
//!
//! - Java `ProcessBuilder` → Rust `tokio::process::Command`（异步超时）；
//! - Java `readAllBytes` 同步排空 → Rust 异步 `output()` 自带排空；
//! - 命令解析器（`parseSimpleCommand`）逐字对齐：拒绝管道 `|`、重定向
//!   `>`/`<`、分号 `;`、`&&`、`||`、`$(`、反引号。

use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::input::{failure, optional_str, required_str, resolve_path};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 命令执行超时（对照旧 `COMMAND_TIMEOUT_SECONDS = 60`）。
const COMMAND_TIMEOUT: Duration = Duration::from_mins(1);

/// 输出截断上限（对照旧 `MAX_OUTPUT_LENGTH = 10_000`）。
const MAX_OUTPUT_LENGTH: usize = 10_000;

/// `VerifyPlanExecution` 工具。
#[derive(Clone, Copy, Debug, Default)]
pub struct VerifyPlanExecutionTool;

impl Tool for VerifyPlanExecutionTool {
    fn name(&self) -> &'static str {
        "VerifyPlanExecution"
    }

    fn description(&self) -> &'static str {
        "验证计划模式下的任务执行结果是否符合预期"
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "required": ["verification_type"],
            "properties": {
                "verification_type": {
                    "type": "string",
                    "description": "file_exists|content_matches|test_passes|command_succeeds"
                },
                "expected_outcome": {
                    "type": "string",
                    "description": "预期结果"
                },
                "file_path": {
                    "type": "string",
                    "description": "文件路径（content_matches 时使用）"
                },
                "command": {
                    "type": "string",
                    "description": "要执行的命令（test_passes/command_succeeds 时使用）"
                },
                "content_pattern": {
                    "type": "string",
                    "description": "内容匹配模式"
                }
            }
        })
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { run(input, ctx).await })
    }
}

async fn run(input: Value, ctx: ToolContext) -> ToolOutput {
    let verification_type = match required_str(&input, "verification_type") {
        Ok(v) => v,
        Err(e) => return e,
    };

    match verification_type {
        "file_exists" => verify_file_exists(&input, &ctx).await,
        "content_matches" => verify_content_match(&input, &ctx).await,
        "test_passes" | "command_succeeds" => verify_command_execution(&input, &ctx).await,
        other => failure(
            "PLAN_VERIFICATION_TYPE_UNSUPPORTED",
            format!("Unknown verification type: {other}"),
        ),
    }
}

async fn verify_file_exists(input: &Value, ctx: &ToolContext) -> ToolOutput {
    let Some(file_path) =
        optional_str(input, "file_path").or_else(|| optional_str(input, "expected_outcome"))
    else {
        return failure("PLAN_FILE_MISSING", "File path not specified");
    };
    let path = resolve_path(file_path, ctx);
    if tokio::fs::metadata(&path).await.is_ok() {
        ToolOutput::ok("验证通过: 文件存在")
    } else {
        failure(
            "PLAN_FILE_MISSING",
            format!("File does not exist: {}", path.display()),
        )
    }
}

async fn verify_content_match(input: &Value, ctx: &ToolContext) -> ToolOutput {
    let Some(file_path) = optional_str(input, "file_path") else {
        return failure("PLAN_FILE_MISSING", "File path not specified");
    };
    let expected = optional_str(input, "expected_outcome").unwrap_or("");
    let path = resolve_path(file_path, ctx);

    let content = match tokio::fs::read_to_string(&path).await {
        Ok(c) => c,
        Err(e) => {
            return failure(
                "PLAN_VERIFICATION_EXECUTION_ERROR",
                format!("Cannot read file: {e}"),
            );
        }
    };

    if content.contains(expected) {
        ToolOutput::ok("验证通过: 内容匹配")
    } else {
        failure(
            "PLAN_CONTENT_MISMATCH",
            "Content does not match expected outcome",
        )
    }
}

async fn verify_command_execution(input: &Value, ctx: &ToolContext) -> ToolOutput {
    let command = optional_str(input, "command")
        .or_else(|| optional_str(input, "expected_outcome"))
        .unwrap_or("");
    if command.is_empty() {
        return failure("PLAN_VERIFICATION_FAILED", "No command specified");
    }

    let Some(argv) = parse_simple_command(command) else {
        return failure(
            "PLAN_VERIFICATION_COMMAND_REJECTED",
            format!(
                "Command contains unsupported shell features (pipes, redirects, semicolons, etc.): {command}"
            ),
        );
    };

    if argv.is_empty() {
        return failure("PLAN_VERIFICATION_COMMAND_REJECTED", "Empty command");
    }

    let result = tokio::time::timeout(COMMAND_TIMEOUT, async {
        tokio::process::Command::new(&argv[0])
            .args(&argv[1..])
            .current_dir(ctx.working_dir())
            .output()
            .await
    })
    .await;

    match result {
        Err(_) => failure(
            "PLAN_VERIFICATION_TIMEOUT",
            format!("Command timed out after {}s", COMMAND_TIMEOUT.as_secs()),
        ),
        Ok(Err(e)) => failure(
            "PLAN_VERIFICATION_EXECUTION_ERROR",
            format!("Execution error: {e}"),
        ),
        Ok(Ok(output)) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let mut combined = format!("{stdout}{stderr}");
            if combined.len() > MAX_OUTPUT_LENGTH {
                combined.truncate(MAX_OUTPUT_LENGTH);
                combined.push_str("\n... [truncated]");
            }
            if output.status.success() {
                ToolOutput::ok(if combined.is_empty() {
                    "验证通过: 命令执行成功".to_owned()
                } else {
                    format!("验证通过\n{combined}")
                })
            } else {
                let code = output.status.code().unwrap_or(-1);
                failure(
                    "PLAN_VERIFICATION_FAILED",
                    format!("Command exited with code {code}:\n{combined}"),
                )
            }
        }
    }
}

/// 将命令字符串解析为 argv 列表。
///
/// 支持：空格分割、单引号/双引号。
/// 拒绝（返回 `None`）：管道 `|`、分号 `;`、`&&`、`||`、反引号、`$(`、`>`、`<`。
fn parse_simple_command(command: &str) -> Option<Vec<String>> {
    if command.trim().is_empty() {
        return None;
    }
    // 快速检测不安全 shell 元字符。
    for ch in command.chars() {
        if matches!(ch, '|' | ';' | '`' | '<' | '>') {
            return None;
        }
    }
    if command.contains("$(") || command.contains("&&") || command.contains("||") {
        return None;
    }

    let mut argv: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = command.chars().peekable();

    while let Some(c) = chars.next() {
        if in_single {
            if c == '\'' {
                in_single = false;
            } else {
                current.push(c);
            }
        } else if in_double {
            if c == '"' {
                in_double = false;
            } else if c == '\\' {
                if let Some(&next) = chars.peek() {
                    if matches!(next, '"' | '\\' | '$' | '`') {
                        current.push(next);
                        chars.next();
                    } else {
                        current.push(c);
                    }
                } else {
                    current.push(c);
                }
            } else {
                current.push(c);
            }
        } else if c == '\'' {
            in_single = true;
        } else if c == '"' {
            in_double = true;
        } else if c == '\\' {
            if let Some(next) = chars.next() {
                current.push(next);
            }
        } else if c.is_whitespace() {
            if !current.is_empty() {
                argv.push(std::mem::take(&mut current));
            }
        } else {
            current.push(c);
        }
    }

    if in_single || in_double {
        return None;
    }
    if !current.is_empty() {
        argv.push(current);
    }
    if argv.is_empty() { None } else { Some(argv) }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx).with_working_dir("/tmp")
    }

    #[tokio::test]
    async fn file_exists_passes() {
        let dir = std::env::temp_dir().join(format!("zk-verify-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("exists.txt");
        std::fs::write(&path, "content").expect("write");
        let tool = VerifyPlanExecutionTool;
        let output = tool
            .execute(
                json!({
                    "verification_type": "file_exists",
                    "file_path": path.to_str().unwrap()
                }),
                ctx(),
            )
            .await;
        assert!(!output.is_error);
    }

    #[tokio::test]
    async fn file_exists_fails() {
        let tool = VerifyPlanExecutionTool;
        let output = tool
            .execute(
                json!({
                    "verification_type": "file_exists",
                    "file_path": "/nope/zk-missing.txt"
                }),
                ctx(),
            )
            .await;
        assert!(output.is_error);
        assert!(output.content.contains("PLAN_FILE_MISSING"));
    }

    #[tokio::test]
    async fn content_matches_passes() {
        let dir = std::env::temp_dir().join(format!("zk-verify-content-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("match.txt");
        std::fs::write(&path, "hello world\nfoo bar\n").expect("write");
        let tool = VerifyPlanExecutionTool;
        let output = tool
            .execute(
                json!({
                    "verification_type": "content_matches",
                    "file_path": path.to_str().unwrap(),
                    "expected_outcome": "foo bar"
                }),
                ctx(),
            )
            .await;
        assert!(!output.is_error);
    }

    #[tokio::test]
    async fn command_succeeds_passes() {
        let tool = VerifyPlanExecutionTool;
        let output = tool
            .execute(
                json!({
                    "verification_type": "command_succeeds",
                    "command": "echo hello"
                }),
                ctx(),
            )
            .await;
        assert!(!output.is_error);
        assert!(output.content.contains("hello"));
    }

    #[tokio::test]
    async fn command_with_pipe_is_rejected() {
        let tool = VerifyPlanExecutionTool;
        let output = tool
            .execute(
                json!({
                    "verification_type": "command_succeeds",
                    "command": "echo hello | grep hello"
                }),
                ctx(),
            )
            .await;
        assert!(output.is_error);
        assert!(
            output
                .content
                .contains("PLAN_VERIFICATION_COMMAND_REJECTED")
        );
    }

    #[test]
    fn parse_simple_command_valid() {
        assert_eq!(
            parse_simple_command("cargo test --lib"),
            Some(vec!["cargo".into(), "test".into(), "--lib".into()])
        );
    }

    #[test]
    fn parse_simple_command_rejects_pipe() {
        assert!(parse_simple_command("echo a | cat").is_none());
    }

    #[test]
    fn parse_simple_command_rejects_redirect() {
        assert!(parse_simple_command("echo a > file").is_none());
    }

    #[test]
    fn parse_simple_command_rejects_and() {
        assert!(parse_simple_command("a && b").is_none());
    }

    #[test]
    fn parse_simple_command_quoted() {
        assert_eq!(
            parse_simple_command(r#"echo "hello world""#),
            Some(vec!["echo".into(), "hello world".into()])
        );
    }

    #[tokio::test]
    async fn unsupported_verification_type() {
        let tool = VerifyPlanExecutionTool;
        let output = tool
            .execute(
                json!({
                    "verification_type": "magic_check"
                }),
                ctx(),
            )
            .await;
        assert!(output.is_error);
        assert!(
            output
                .content
                .contains("PLAN_VERIFICATION_TYPE_UNSUPPORTED")
        );
    }
}
