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
//! - Java 侧 `prompt()` 用 text block 嵌入大量使用指导，Rust 侧以
//!   常量字符串逐字对齐（内容不变，仅语言绑定差异）；
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

/// `ExitPlanMode` 工具（名 `ExitPlanMode`）——退出计划模式，恢复到之前的权限模式。
///
/// 如果提供 `plan_summary`，记录到结果消息中（对照旧 `ExitPlanModeTool`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct ExitPlanModeTool;

impl Tool for ExitPlanModeTool {
    fn name(&self) -> &'static str {
        "ExitPlanMode"
    }

    fn description(&self) -> &'static str {
        "Exit plan mode and restore the previous permission mode."
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
        let content = format!("Exited plan mode.{summary}");
        let mut output = ToolOutput::ok(content);
        output.metadata = Some(json!({ "mode": "default" }));
        Box::pin(futures::future::ready(output))
    }
}

// ────────────────────── prompt() 等价物 ──────────────────────

/// `EnterPlanModeTool.prompt()` 的使用指导文本（逐字对照旧 Java L32-68）。
pub const ENTER_PLAN_MODE_PROMPT: &str = r"Use this tool proactively when you're about to start a non-trivial implementation task. Getting user sign-off on your approach before writing code prevents wasted effort and ensures alignment. This tool transitions you into plan mode where you can explore the codebase and design an implementation approach for user approval.

## When to Use This Tool
**Prefer using EnterPlanMode** for implementation tasks unless they're simple. Use it when ANY of these conditions apply:
1. **New Feature Implementation**: Adding meaningful new functionality
2. **Multiple Valid Approaches**: The task can be solved in several different ways
3. **Code Modifications**: Changes that affect existing behavior or structure
4. **Architectural Decisions**: Choosing between patterns or technologies
5. **Multi-File Changes**: The task will likely touch more than 2-3 files
6. **Unclear Requirements**: You need to explore before understanding the full scope
7. **User Preferences Matter**: The implementation could reasonably go multiple ways

## When NOT to Use This Tool
Only skip EnterPlanMode for simple tasks:
- Single-line or few-line fixes (typos, obvious bugs, small tweaks)
- Adding a single function with clear requirements
- Tasks where the user has given very specific, detailed instructions
- Pure research/exploration tasks (use the Agent tool instead)

## What Happens in Plan Mode
In plan mode, you'll:
1. Thoroughly explore the codebase using Glob, Grep, and Read tools
2. Understand existing patterns and architecture
3. Design an implementation approach
4. Present your plan to the user for approval
5. Use AskUserQuestion if you need to clarify approaches
6. Exit plan mode with ExitPlanMode when ready to implement

## Important Notes
- This tool REQUIRES user approval - they must consent to entering plan mode
- If unsure whether to use it, err on the side of planning";

/// `ExitPlanModeTool.prompt()` 的使用指导文本（逐字对照旧 Java L29-53）。
pub const EXIT_PLAN_MODE_PROMPT: &str = r#"Use this tool when you are in plan mode and have finished writing your plan and are ready for user approval.

## How This Tool Works
- You should have already written your plan to the plan file
- This tool simply signals that you're done planning and ready for the user to review
- The user will see the contents of your plan when they review it

## When to Use This Tool
IMPORTANT: Only use this tool when the task requires planning the implementation steps of a task that requires writing code. For research tasks where you're gathering information, searching files, reading files or trying to understand the codebase - do NOT use this tool.

## Before Using This Tool
Ensure your plan is complete and unambiguous:
- If you have unresolved questions about requirements or approach, use AskUserQuestion first
- Once your plan is finalized, use THIS tool to request approval

**Important:** Do NOT use AskUserQuestion to ask "Is this plan okay?" or "Should I proceed?" - that's exactly what THIS tool does."#;

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
