//! `CodeIntel` 工具——tree-sitter 多语言静态分析桥（旧 `CodeIntelTool.java`）。
//!
//! 逐字对照旧源：工具名 `CodeIntel`、中文 description、group
//! `code_intelligence`、10 种支持语言、4 个 action → endpoint 映射、
//! 三个错误码与文案。

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::json;
use zk_tools::{Tool, ToolContext, ToolOutput};

use super::{CODE_INTEL, failure, opt_str};
use crate::python::client::{Correlation, PythonClient};

/// 支持语言（旧 `SUPPORTED_LANGUAGES`，:15-18；此处按字典序固定顺序以获得
/// 确定性文案与 schema——Java `Set.of` 迭代序不稳定）。
pub const SUPPORTED_LANGUAGES: [&str; 10] = [
    "c",
    "cpp",
    "go",
    "java",
    "javascript",
    "php",
    "python",
    "ruby",
    "rust",
    "typescript",
];

/// 语言枚举文案（schema description 与错误提示共用，旧 :44 的顺序）。
const LANGUAGE_HINT: &str = "python|javascript|typescript|java|go|rust|c|cpp|ruby|php";

/// action 枚举文案（旧 :41）。
const ACTION_HINT: &str = "parse|symbols|code-map|dependencies";

/// `CodeIntel` 工具（Python `tree-sitter` 桥）。
pub struct CodeIntelTool {
    client: Arc<PythonClient>,
}

impl CodeIntelTool {
    /// 装配工具（注入组装根持有的 [`PythonClient`]）。
    #[must_use]
    pub fn new(client: Arc<PythonClient>) -> Self {
        Self { client }
    }
}

/// action → Python 端点（旧 :60-66 的 switch，未知 action → `None`）。
fn endpoint_for(action: &str) -> Option<&'static str> {
    match action {
        "parse" => Some("/api/code-intel/parse"),
        "symbols" => Some("/api/code-intel/symbols"),
        "code-map" => Some("/api/code-intel/code-map"),
        "dependencies" => Some("/api/code-intel/dependencies"),
        _ => None,
    }
}

impl Tool for CodeIntelTool {
    fn name(&self) -> &'static str {
        "CodeIntel"
    }

    fn description(&self) -> &'static str {
        "查询代码符号结构、代码地图和依赖关系（基于 tree-sitter 的多语言静态分析）"
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "required": ["action", "content", "language"],
            "properties": {
                "action": {
                    "type": "string",
                    "description": format!("操作类型: {ACTION_HINT}")
                },
                "content": { "type": "string", "description": "要分析的源代码内容" },
                "language": {
                    "type": "string",
                    "description": format!("编程语言: {LANGUAGE_HINT}")
                }
            }
        })
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            // 旧 :51-53 取值序：action → content → language（language 默认 ""）。
            let Some(action) = opt_str(&input, "action") else {
                return failure(
                    "MISSING_PARAMETER",
                    "Required parameter 'action' is missing or not a string",
                );
            };
            let Some(content) = opt_str(&input, "content") else {
                return failure(
                    "MISSING_PARAMETER",
                    "Required parameter 'content' is missing or not a string",
                );
            };
            let language = opt_str(&input, "language").unwrap_or("").to_lowercase();

            if !SUPPORTED_LANGUAGES.contains(&language.as_str()) {
                return failure(
                    "CODE_INTEL_LANGUAGE_UNSUPPORTED",
                    format!(
                        "Language '{language}' not supported by code-intel. Supported: [{}]. \
                         Use Read tool instead.",
                        SUPPORTED_LANGUAGES.join(", ")
                    ),
                );
            }

            let Some(endpoint) = endpoint_for(action) else {
                return failure(
                    "CODE_INTEL_ACTION_INVALID",
                    format!("未知 CodeIntel 操作: {action}（支持: {ACTION_HINT}）"),
                );
            };

            let body = json!({ "content": content, "language": language });
            let correlation = Correlation::for_session(ctx.session_id());
            // 旧 :73 以 `String.class` 反序列化 JSON **对象**——Jackson
            // `MismatchedInputException`，该工具在旧端实际恒降级
            // （MUST_FIX-fixed，见 §6 偏离表）。此处以 Value 承接后整体输出，
            // 与旧代码的**意图**（返回整个响应体文本）一致。
            let response: Option<serde_json::Value> = self
                .client
                .call_if_available(CODE_INTEL, endpoint, &body, &correlation)
                .await;
            match response {
                Some(value) => ToolOutput::ok(value.to_string()),
                None => failure(
                    "CODE_INTEL_UNAVAILABLE",
                    "CodeIntel 能力当前不可用（Python 服务未就绪或缺少 tree-sitter 依赖），\
                     请改用 Read 工具。",
                ),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    fn tool() -> CodeIntelTool {
        let socket = std::env::temp_dir().join("zk-code-intel-absent.sock");
        CodeIntelTool::new(Arc::new(PythonClient::new(socket)))
    }

    fn ctx() -> ToolContext {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    /// 名称 / 描述 / schema 逐字对齐旧 `CodeIntelTool`。
    #[test]
    fn spec_matches_baseline() {
        let tool = tool();
        assert_eq!(tool.name(), "CodeIntel");
        assert_eq!(
            tool.description(),
            "查询代码符号结构、代码地图和依赖关系（基于 tree-sitter 的多语言静态分析）"
        );
        let schema = tool.parameters();
        assert_eq!(schema["required"], json!(["action", "content", "language"]));
        assert!(
            schema["properties"]["action"]["description"]
                .as_str()
                .expect("action description")
                .contains("parse|symbols|code-map|dependencies")
        );
        // 默认超时沿用 zk-tools 默认 2min（旧端未覆写 getMaxExecutionTimeMs）。
        assert_eq!(tool.timeout(), zk_tools::DEFAULT_TOOL_TIMEOUT);
    }

    /// 4 个 action 的端点映射逐条对齐旧 :60-66。
    #[test]
    fn endpoint_mapping_matches_baseline() {
        assert_eq!(endpoint_for("parse"), Some("/api/code-intel/parse"));
        assert_eq!(endpoint_for("symbols"), Some("/api/code-intel/symbols"));
        assert_eq!(endpoint_for("code-map"), Some("/api/code-intel/code-map"));
        assert_eq!(
            endpoint_for("dependencies"),
            Some("/api/code-intel/dependencies")
        );
        assert_eq!(endpoint_for("outline"), None);
    }

    /// 语言白名单校验先于 action 校验（旧 :55 在 :60 之前）。
    #[tokio::test]
    async fn unsupported_language_is_rejected_before_action() {
        let out = tool()
            .execute(
                json!({ "action": "nope", "content": "x", "language": "cobol" }),
                ctx(),
            )
            .await;
        assert!(out.is_error);
        assert!(out.content.starts_with("CODE_INTEL_LANGUAGE_UNSUPPORTED: "));
        assert!(out.content.contains("Use Read tool instead."));
    }

    /// 语言缺失 → 旧端 `getString("language","")` 默认空串 → 同一分支。
    #[tokio::test]
    async fn missing_language_defaults_to_empty_and_is_unsupported() {
        let out = tool()
            .execute(json!({ "action": "parse", "content": "x" }), ctx())
            .await;
        assert!(out.content.contains("Language '' not supported"));
    }

    /// 语言大小写归一（旧 `.toLowerCase()`）+ 未知 action 报 `ACTION_INVALID`。
    #[tokio::test]
    async fn language_is_lowercased_and_unknown_action_rejected() {
        let out = tool()
            .execute(
                json!({ "action": "outline", "content": "x", "language": "PYTHON" }),
                ctx(),
            )
            .await;
        assert!(out.is_error);
        assert!(out.content.starts_with("CODE_INTEL_ACTION_INVALID: "));
        assert!(out.content.contains("未知 CodeIntel 操作: outline"));
    }

    /// 无 Python 侧车 → 降级文案逐字对齐旧 :76-78，且不 panic。
    #[tokio::test]
    async fn missing_sidecar_degrades_with_baseline_message() {
        let out = tool()
            .execute(
                json!({ "action": "symbols", "content": "def f(): pass", "language": "python" }),
                ctx(),
            )
            .await;
        assert!(out.is_error);
        assert_eq!(
            out.content,
            "CODE_INTEL_UNAVAILABLE: CodeIntel 能力当前不可用（Python 服务未就绪或缺少 \
             tree-sitter 依赖），请改用 Read 工具。"
        );
    }

    /// 必填入参缺失走 `MISSING_PARAMETER`（旧 `getString(key)` 抛异常的等价物）。
    #[tokio::test]
    async fn missing_required_parameters_are_reported() {
        let out = tool().execute(json!({}), ctx()).await;
        assert!(out.content.contains("'action'"));
        let out = tool().execute(json!({ "action": "parse" }), ctx()).await;
        assert!(out.content.contains("'content'"));
    }
}
