//! `REPL` 工具——把代码投进长驻解释器会话并取回输出。
//!
//! 对照旧 `tool/repl/REPLTool.java`（176L，只读权威规格）：名 `REPL`、
//! 入参 `language`（默认 `python`）/ `code`（必填）/ `sessionId`（默认新 UUID）、
//! 元数据 `replSessionId` / `language` / `stderr`（非空才带）、
//! 权限 `ALWAYS_ASK`、`isDestructive = true`、`isConcurrencySafe = false`、
//! `isReadOnly = false`、分组 `bash`。
//!
//! 错误码逐条对齐旧 catch 三分支：
//! - `REPL_OPERATION_UNSUPPORTED`——语言不在白名单；
//! - `REPL_SESSION_ERROR`——解释器进程起不来（旧 `ReplException`）；
//! - `REPL_EXECUTION_FAILED`——写 stdin 失败等其余异常。

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::json;

use super::{EXEC_TIMEOUT, LANGUAGES, MAX_OUTPUT_BYTES, ReplError, ReplManager, truncate_output};
use crate::input::{failure, optional_str, required_str};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 缺省语言（旧 `input.getString("language", "python")`）。
const DEFAULT_LANGUAGE: &str = "python";

/// 交互式解释器工具（旧 `REPLTool`，名 `REPL`）。
#[derive(Debug)]
pub struct REPLTool {
    /// 共享进程池（旧构造注入的 `ReplManager` Spring bean）。
    manager: Arc<ReplManager>,
}

impl REPLTool {
    /// 绑定进程池。
    #[must_use]
    pub fn new(manager: Arc<ReplManager>) -> Self {
        Self { manager }
    }

    /// 暴露进程池——组合根关停时调 `destroy_all`（旧 `@PreDestroy cleanup`）。
    #[must_use]
    pub fn manager(&self) -> &Arc<ReplManager> {
        &self.manager
    }
}

impl Tool for REPLTool {
    fn name(&self) -> &'static str {
        "REPL"
    }

    fn description(&self) -> &'static str {
        // 逐字取自旧 `getDescription()` 三段拼接。
        "Start an interactive code interpreter session. \
         Execute code in a persistent REPL environment with \
         session support for maintaining state across calls."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "required": ["code"],
            "properties": {
                "language": {
                    "type": "string",
                    "enum": LANGUAGES,
                    "description": "Programming language for the REPL"
                },
                "code": {
                    "type": "string",
                    "description": "Code to execute in the REPL"
                },
                "session_id": {
                    "type": "string",
                    "description": "Session ID to reuse an existing REPL session"
                }
            }
        })
    }

    /// 单次执行超时（旧 `ReplManager.EXEC_TIMEOUT`），再留一个静默窗余量给
    /// 收敛读，避免工具层超时先于内层判定触发。
    fn timeout(&self) -> Duration {
        EXEC_TIMEOUT.saturating_add(Duration::from_secs(10))
    }

    /// 旧 `isDestructive(input) → true`（解释器内可任意副作用）。
    fn is_destructive(&self, _input: &serde_json::Value) -> bool {
        true
    }

    /// 旧 `isReadOnly(input) → false`。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        false
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let code = match required_str(&input, "code") {
                Ok(code) => code.to_owned(),
                Err(output) => return output,
            };
            let language = optional_str(&input, "language")
                .unwrap_or(DEFAULT_LANGUAGE)
                .to_owned();
            // 旧入参名 `sessionId`；两种写法都收，缺省落新 UUID。
            let session_id = optional_str(&input, "session_id")
                .or_else(|| optional_str(&input, "sessionId"))
                .map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);

            let session =
                match self
                    .manager
                    .get_or_create(&session_id, &language, ctx.working_dir())
                {
                    Ok(session) => session,
                    Err(ReplError::UnsupportedLanguage(language)) => {
                        return failure(
                            "REPL_OPERATION_UNSUPPORTED",
                            format!(
                                "Unsupported REPL language: {language}. Supported: {}",
                                LANGUAGES.join(", ")
                            ),
                        );
                    }
                    Err(error @ ReplError::Spawn(_)) => {
                        return failure("REPL_SESSION_ERROR", format!("REPL error: {error}"));
                    }
                };

            if let Err(error) = session.write_stdin(&code).await {
                // 写 stdin 失败 = 解释器已崩 → 把它踢出池，下次调用重开。
                self.manager.destroy_session(&session_id);
                return failure(
                    "REPL_EXECUTION_FAILED",
                    format!("REPL execution failed: {error}"),
                );
            }

            let stdout = session.read_available_output(EXEC_TIMEOUT).await;
            let stderr = session.read_available_stderr();

            let mut output = ToolOutput::ok(truncate_output(&stdout));
            let mut metadata = json!({
                "replSessionId": session_id,
                "language": language,
                "activeSessions": self.manager.active_session_count(),
            });
            if !stderr.is_empty() {
                metadata["stderr"] = json!(truncate_output(&stderr));
            }
            output.metadata = Some(metadata);
            output
        })
    }
}

/// 元数据里回报的输出上限，供文档与测试引用。
pub const REPORTED_OUTPUT_LIMIT: usize = MAX_OUTPUT_BYTES;

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        let dir = std::env::temp_dir().join(format!("zk-repl-tool-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        ToolContext::new(CancellationToken::new(), tx).with_working_dir(&dir)
    }

    fn tool() -> REPLTool {
        REPLTool::new(Arc::new(ReplManager::new()))
    }

    /// 工具规格：名 / 必填项 / 语言枚举 / 破坏性标记对齐旧实现。
    #[test]
    fn spec_matches_legacy_shape() {
        let tool = tool();
        let spec = tool.spec();
        assert_eq!(spec.name, "REPL");
        assert_eq!(spec.parameters["required"], json!(["code"]));
        assert_eq!(
            spec.parameters["properties"]["language"]["enum"],
            json!(["python", "node", "ruby"])
        );
        assert!(tool.is_destructive(&json!({})));
        assert!(!tool.is_read_only(&json!({})));
        assert_eq!(REPORTED_OUTPUT_LIMIT, 102_400);
    }

    /// 缺 `code` → 共用 `MISSING_PARAMETER` 通道（旧 `getString` 抛校验错）。
    #[tokio::test]
    async fn missing_code_is_rejected() {
        let output = tool().execute(json!({}), ctx()).await;
        assert!(output.is_error);
        assert!(output.content.starts_with("MISSING_PARAMETER: "));
    }

    /// 未知语言 → `REPL_OPERATION_UNSUPPORTED`（旧同名错误码）。
    #[tokio::test]
    async fn unsupported_language_maps_to_legacy_code() {
        let output = tool()
            .execute(json!({ "code": "1", "language": "perl" }), ctx())
            .await;
        assert!(output.is_error);
        assert!(
            output.content.starts_with("REPL_OPERATION_UNSUPPORTED: "),
            "{}",
            output.content
        );
    }

    /// 真跑一轮 python：状态跨调用保持（同 `session_id` 复用进程）。
    #[tokio::test]
    async fn python_session_keeps_state_across_calls() {
        let tool = tool();
        let first = tool
            .execute(
                json!({ "code": "zk_marker = 41", "session_id": "state-test" }),
                ctx(),
            )
            .await;
        if first.is_error {
            return; // 无 python3 的环境跳过。
        }
        let second = tool
            .execute(
                json!({ "code": "print(zk_marker + 1)", "session_id": "state-test" }),
                ctx(),
            )
            .await;
        assert!(!second.is_error, "{}", second.content);
        assert!(second.content.contains("42"), "{}", second.content);

        let meta = second.metadata.expect("metadata");
        assert_eq!(meta["replSessionId"], "state-test");
        assert_eq!(meta["language"], "python");

        tool.manager().destroy_all();
    }

    /// 旧入参名 `sessionId` 仍被接受（不破旧调用点）。
    #[tokio::test]
    async fn legacy_camel_case_session_id_is_accepted() {
        let tool = tool();
        let output = tool
            .execute(json!({ "code": "pass", "sessionId": "camel" }), ctx())
            .await;
        if output.is_error {
            return; // 无 python3 的环境跳过。
        }
        assert_eq!(output.metadata.expect("metadata")["replSessionId"], "camel");
        tool.manager().destroy_all();
    }
}
