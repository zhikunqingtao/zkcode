//! `CtxInspect` 工具——检查当前会话上下文状态（Batch 7）。
//!
//! 语义来源（旧仓库只读）：`CtxInspectTool.java`（40L）。
//! 返回会话 ID、工作目录、嵌套深度、消息数与 Token 用量。
//!
//! # 有意差异
//!
//! - Java 侧从 `ToolUseContext` 直接取 `sessionId` / `workingDirectory` /
//!   `nestingDepth`；Rust 侧 `ToolContext` 无 `nestingDepth` 字段，故经
//!   [`ContextInfoPort`] 端口反转注入（生产实现由 `zk-server` 组合根提供）。
//!   端口不可用时回退为 `ToolContext` 自带的 `session_id` / `working_dir`。

use std::fmt::Write as _;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::input::optional_str;
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 上下文信息端口——反转依赖（zk-tools 不依赖 zk-engine）。
///
/// 生产实现由组合根注入，从 `AppState` 取 session 级 Token 统计。
pub trait ContextInfoPort: Send + Sync {
    /// 获取指定会话的上下文信息。
    fn get_context_info(&self, session_id: &str) -> ContextInfo;
}

/// 上下文信息快照（对照旧 `CtxInspectTool` 输出字段）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextInfo {
    /// 会话 ID。
    pub session_id: String,
    /// 消息数。
    pub message_count: u32,
    /// 累计输入 Token 数。
    pub total_input_tokens: u64,
    /// 累计输出 Token 数。
    pub total_output_tokens: u64,
    /// 嵌套深度（子代理调用层数）。
    pub nesting_depth: u32,
    /// 工作目录。
    pub working_directory: String,
}

/// `CtxInspect` 工具（名 `CtxInspect`）。
pub struct CtxInspectTool {
    port: Option<Arc<dyn ContextInfoPort>>,
}

impl CtxInspectTool {
    /// 构造（无端口时回退为 `ToolContext` 自带字段）。
    #[must_use]
    pub fn new(port: Option<Arc<dyn ContextInfoPort>>) -> Self {
        Self { port }
    }
}

impl Tool for CtxInspectTool {
    fn name(&self) -> &'static str {
        "CtxInspect"
    }

    fn description(&self) -> &'static str {
        "检查当前会话上下文状态（消息数、Token 用量、工具调用统计）"
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "detail_level": {
                    "type": "string",
                    "description": "summary|detailed, 默认 summary"
                }
            }
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let _level = optional_str(&input, "detail_level").unwrap_or("summary");
        let session_id = ctx.session_id().unwrap_or("unknown");
        let working_dir = ctx.working_dir().display().to_string();

        let info = if let Some(port) = &self.port {
            port.get_context_info(session_id)
        } else {
            ContextInfo {
                session_id: session_id.to_owned(),
                message_count: 0,
                total_input_tokens: 0,
                total_output_tokens: 0,
                nesting_depth: 0,
                working_directory: working_dir,
            }
        };

        let mut output = String::from("## 上下文检查\n\n");
        let _ = writeln!(output, "- 会话 ID: {}", info.session_id);
        let _ = writeln!(output, "- 工作目录: {}", info.working_directory);
        let _ = writeln!(output, "- 嵌套深度: {}", info.nesting_depth);
        let _ = writeln!(output, "- 消息数: {}", info.message_count);
        let _ = writeln!(output, "- 输入 Token: {}", info.total_input_tokens);
        let _ = writeln!(output, "- 输出 Token: {}", info.total_output_tokens);

        Box::pin(futures::future::ready(ToolOutput::ok(output)))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
            .with_session_id("test-session")
            .with_working_dir("/tmp/zk-ctx")
    }

    struct MockPort;

    impl ContextInfoPort for MockPort {
        fn get_context_info(&self, session_id: &str) -> ContextInfo {
            ContextInfo {
                session_id: session_id.to_owned(),
                message_count: 42,
                total_input_tokens: 1000,
                total_output_tokens: 500,
                nesting_depth: 1,
                working_directory: "/tmp/zk-ctx".to_owned(),
            }
        }
    }

    #[tokio::test]
    async fn without_port_uses_context_defaults() {
        let tool = CtxInspectTool::new(None);
        let output = tool.execute(json!({}), ctx()).await;
        assert!(!output.is_error);
        assert!(output.content.contains("test-session"));
        assert!(output.content.contains("/tmp/zk-ctx"));
    }

    #[tokio::test]
    async fn with_port_uses_port_data() {
        let tool = CtxInspectTool::new(Some(Arc::new(MockPort)));
        let output = tool.execute(json!({}), ctx()).await;
        assert!(!output.is_error);
        assert!(output.content.contains("42"));
        assert!(output.content.contains("1000"));
        assert!(output.content.contains("500"));
    }
}
