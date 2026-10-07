//! `EnterPlanMode` / `ExitPlanMode` 工具——计划模式切换（Batch 7）。
//!
//! 语义来源（旧仓库只读）：
//! - `EnterPlanModeTool.java`（100L）——切换到只读规划阶段，
//!   `readOnly=true`，`isConcurrencySafe=true`，metadata `{"mode":"plan"}`；
//! - `ExitPlanModeTool.java`（87L）——退出计划模式恢复 Default，
//!   metadata `{"mode":"default"}`。
//!
//! # 有意差异
//!
//! - 使用指导按实际权限切换能力描述，不承诺计划文件展示或整份计划审批；
//! - Java `ToolResult.withMetadata` → Rust `ToolOutput.metadata` 以
//!   `serde_json::json!` 承载，引擎侧按 metadata `"mode"` 字段触发权限模式切换。

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::input::optional_str;
use crate::tool::{Tool, ToolContext, ToolOutput};

// ────────────────────── EnterPlanMode ──────────────────────

/// `EnterPlanMode` 工具（名 `EnterPlanMode`）——进入只读规划阶段。
///
/// LLM 主动调用此工具进入计划模式。在该模式下只允许只读工具自动执行，
/// 写入工具仍可执行但需要确认（对照旧 `EnterPlanModeTool`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct EnterPlanModeTool;

impl Tool for EnterPlanModeTool {
    fn name(&self) -> &'static str {
        "EnterPlanMode"
    }

    fn description(&self) -> &'static str {
        "Switch to plan mode for read-only planning. Write operations will require confirmation in this mode."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": "Reason for entering plan mode"
                }
            },
            "required": []
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let reason = optional_str(&input, "reason").unwrap_or("Planning phase");
        let content =
            format!("Entered plan mode. Write operations require confirmation. Reason: {reason}");
        let mut output = ToolOutput::ok(content);
        output.metadata = Some(json!({ "mode": "plan" }));
        Box::pin(futures::future::ready(output))
    }
}

// ────────────────────── ExitPlanMode ──────────────────────

/// `ExitPlanMode` 工具（名 `ExitPlanMode`）——退出计划模式，恢复到 Default 权限模式。
///
/// 如果提供 `plan_summary`，记录到结果消息中（对照旧 `ExitPlanModeTool`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct ExitPlanModeTool;

impl Tool for ExitPlanModeTool {
    fn name(&self) -> &'static str {
        "ExitPlanMode"
    }

    fn description(&self) -> &'static str {
        "Exit plan mode and return to the default permission mode."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "plan_summary": {
                    "type": "string",
                    "description": "Summary of the plan"
                }
            },
            "required": []
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let summary = optional_str(&input, "plan_summary")
            .map(|s| format!(" Plan summary: {s}"))
            .unwrap_or_default();
        let content = format!("Exited plan mode. Permission mode is now default.{summary}");
        let mut output = ToolOutput::ok(content);
        output.metadata = Some(json!({ "mode": "default" }));
        Box::pin(futures::future::ready(output))
    }
}

// ────────────────────── prompt() 等价物 ──────────────────────

/// Guidance for the implemented session permission transition.
pub const ENTER_PLAN_MODE_PROMPT: &str = r"Use this tool for a planning phase when exploring requirements or designing a non-trivial implementation.

The tool changes the session permission mode to PLAN. Read-only exploration remains available; write operations require confirmation under that mode. Present the plan in the conversation and use AskUserQuestion for unresolved requirements.

Entering plan mode does not create a plan file or approve an implementation. Use ExitPlanMode when planning is complete and the user has authorized implementation. The /plan command only controls the planning panel and does not change permissions.";

/// Guidance for the implemented return to Default, without approval UI promises.
pub const EXIT_PLAN_MODE_PROMPT: &str = r"Use this tool when planning is complete and implementation is authorized. It switches the session permission mode to DEFAULT and records an optional plan_summary in the tool result.

Present the plan to the user in the conversation before using this tool. Resolve questions with AskUserQuestion. This tool does not display a plan file, request approval for the whole plan, or restore a previously selected permission mode. Subsequent operations follow DEFAULT permission checks.";

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    #[tokio::test]
    async fn enter_plan_mode_returns_plan_metadata() {
        let tool = EnterPlanModeTool;
        let output = tool.execute(json!({}), ctx()).await;
        assert!(!output.is_error);
        assert!(output.content.contains("Entered plan mode"));
        assert!(output.content.contains("Planning phase"));
        let metadata = output.metadata.expect("metadata must be set");
        assert_eq!(metadata["mode"], "plan");
    }

    #[tokio::test]
    async fn enter_plan_mode_includes_custom_reason() {
        let tool = EnterPlanModeTool;
        let output = tool
            .execute(json!({ "reason": "Complex refactor" }), ctx())
            .await;
        assert!(!output.is_error);
        assert!(output.content.contains("Complex refactor"));
    }

    #[tokio::test]
    async fn exit_plan_mode_returns_default_metadata() {
        let tool = ExitPlanModeTool;
        let output = tool.execute(json!({}), ctx()).await;
        assert!(!output.is_error);
        assert!(output.content.contains("Exited plan mode"));
        let metadata = output.metadata.expect("metadata must be set");
        assert_eq!(metadata["mode"], "default");
    }

    #[tokio::test]
    async fn exit_plan_mode_includes_summary() {
        let tool = ExitPlanModeTool;
        let output = tool
            .execute(json!({ "plan_summary": "Implement auth module" }), ctx())
            .await;
        assert!(!output.is_error);
        assert!(output.content.contains("Implement auth module"));
    }

    #[test]
    fn both_tools_are_read_only() {
        let enter = EnterPlanModeTool;
        let exit = ExitPlanModeTool;
        assert!(enter.is_read_only(&json!({})));
        assert!(exit.is_read_only(&json!({})));
    }
}
