//! 内置工具——本子阶段（2.2）仅含 Echo 测试工具。
//!
//! 真实工具族（Read / Edit / Write / Bash 基座 / Git…）归子阶段 2.3，
//! 严禁在此提前实现。

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::tool::{Tool, ToolContext, ToolOutput};

/// Echo 测试工具：原样回显 `text` 字段（框架全链路验证专用）。
pub struct EchoTool;

impl Tool for EchoTool {
    fn name(&self) -> &'static str {
        "Echo"
    }

    fn description(&self) -> &'static str {
        "Echo back the provided text. Test-only builtin for the tool framework."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Text to echo back"
                }
            },
            "required": ["text"]
        })
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            match input.get("text").and_then(Value::as_str) {
                Some(text) => ToolOutput::ok(text.to_owned()),
                None => ToolOutput::error("Required field 'text' is missing or not a string"),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> ToolContext {
        let (progress, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), progress)
    }

    #[tokio::test]
    async fn echo_returns_text() {
        let output = EchoTool.execute(json!({ "text": "hello" }), ctx()).await;
        assert_eq!(output.content, "hello");
        assert!(!output.is_error);
        assert!(output.metadata.is_none());
    }

    #[tokio::test]
    async fn echo_missing_text_is_error() {
        let output = EchoTool.execute(json!({}), ctx()).await;
        assert!(output.is_error);
        assert!(output.content.contains("Required field 'text'"));
        // 非字符串同样报错。
        let output = EchoTool.execute(json!({ "text": 42 }), ctx()).await;
        assert!(output.is_error);
    }

    #[test]
    fn echo_spec_exports_schema() {
        let spec = EchoTool.spec();
        assert_eq!(spec.name, "Echo");
        assert_eq!(spec.parameters["required"][0], "text");
        assert_eq!(spec.parameters["properties"]["text"]["type"], "string");
    }
}
