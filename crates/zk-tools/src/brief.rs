//! `Brief` 工具——项目/会话简报 + 长文本摘要（Batch 8E）。
//!
//! 语义来源（旧仓库只读）：`tool/interaction/BriefTool.java`（141L）——
//! `scope` ∈ {project, session, custom} 三分支、`custom` 缺 `topic` 即
//! `BRIEF_TOPIC_REQUIRED`、未知 scope 即 `BRIEF_SCOPE_INVALID`、
//! `PermissionRequirement.NONE` + `isConcurrencySafe = true`。
//!
//! # 有意差异
//!
//! - 旧实现三分支均为 **P1 占位**（正文里写明「GitService / `SessionService` /
//!   `LlmClient` 集成后完善」），故此处逐字保留其结构与占位提示，不擅自补
//!   Git/LLM 调用——那属于后续批次。
//! - 本移植**追加** `content` 摘要形态（`max_lines` + `strategy` ∈
//!   {head, tail, smart}）：`content` 给出时走摘要，缺省时回落旧 scope 简报。
//!   追加原因是旧占位分支对模型无实际信息量，而「长输出裁剪成简报」是本
//!   workspace 明确需要的能力；两形态互不干扰，旧行为零回归。

use std::fmt::Write as _;

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::input::{failure, optional_str, optional_usize};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 摘要保留行数默认值。
pub const DEFAULT_MAX_LINES: usize = 20;

/// `smart` 策略的首尾各保留行数。
pub const SMART_EDGE_LINES: usize = 5;

/// 省略标记（`smart` / `head` / `tail` 裁剪处的占位行）。
const ELLIPSIS: &str = "…";

/// `Brief` 工具（名 `Brief`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct BriefTool;

impl Tool for BriefTool {
    fn name(&self) -> &'static str {
        "Brief"
    }

    fn description(&self) -> &'static str {
        "Generate a project status brief in Markdown format. \
         Supports project, session, and custom scopes; \
         pass 'content' to condense long text instead."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "scope": {
                    "type": "string",
                    "enum": ["project", "session", "custom"],
                    "description": "Scope of the brief (default: project)"
                },
                "topic": {
                    "type": "string",
                    "description": "Custom topic (required when scope=custom)"
                },
                "content": {
                    "type": "string",
                    "description": "长文本；给出时改为生成该文本的简报（截断摘要）"
                },
                "max_lines": {
                    "type": "integer",
                    "description": "摘要保留行数，默认 20（仅 content 形态生效）"
                },
                "strategy": {
                    "type": "string",
                    "enum": ["head", "tail", "smart"],
                    "description": "截断策略，默认 smart（仅 content 形态生效）"
                }
            }
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(futures::future::ready(run(&input, &ctx)))
    }
}

fn run(input: &Value, ctx: &ToolContext) -> ToolOutput {
    if let Some(content) = input.get("content").and_then(Value::as_str) {
        return condense(input, content);
    }
    scope_brief(input, ctx)
}

/// 旧 scope 三分支（逐字保留占位文案）。
fn scope_brief(input: &Value, ctx: &ToolContext) -> ToolOutput {
    let scope = optional_str(input, "scope").unwrap_or("project");
    let topic = optional_str(input, "topic").unwrap_or_default();
    let working_dir = ctx.working_dir().display();
    let session = ctx.session_id().unwrap_or("-");

    let mut out = String::new();
    match scope {
        "project" => {
            out.push_str("## Project Brief\n\n");
            let _ = writeln!(out, "Working directory: {working_dir}");
            let _ = writeln!(out, "Session: {session}\n");
            out.push_str(
                "*Git status and recent changes will be available after GitService integration.*\n",
            );
        }
        "session" => {
            out.push_str("## Session Brief\n\n");
            let _ = writeln!(out, "Session: {session}\n");
            out.push_str("*Session summary will be available after SessionService integration.*\n");
        }
        "custom" => {
            if topic.trim().is_empty() {
                return failure(
                    "BRIEF_TOPIC_REQUIRED",
                    "'topic' is required for custom scope.",
                );
            }
            let _ = writeln!(out, "## Custom Brief: {topic}\n");
            let _ = writeln!(out, "Working directory: {working_dir}\n");
            out.push_str("*Detailed analysis will be available after LlmClient integration.*\n");
        }
        other => {
            return failure(
                "BRIEF_SCOPE_INVALID",
                format!("Unknown scope: {other}. Use: project, session, or custom."),
            );
        }
    }
    ToolOutput::ok(out)
}

/// `content` 摘要形态。
fn condense(input: &Value, content: &str) -> ToolOutput {
    let max_lines = optional_usize(input, "max_lines")
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_LINES);
    let strategy = optional_str(input, "strategy").unwrap_or("smart");
    if !matches!(strategy, "head" | "tail" | "smart") {
        return failure(
            "BRIEF_STRATEGY_INVALID",
            format!("Unknown strategy: {strategy}. Use: head, tail, or smart."),
        );
    }

    let lines: Vec<&str> = content.lines().collect();
    let total = lines.len();
    if total <= max_lines {
        return ToolOutput {
            content: content.to_owned(),
            is_error: false,
            metadata: Some(json!({
                "strategy": strategy,
                "totalLines": total,
                "keptLines": total,
                "truncated": false,
            })),
        };
    }

    let kept: Vec<String> = match strategy {
        "head" => head_slice(&lines, max_lines),
        "tail" => tail_slice(&lines, max_lines),
        _ => smart_slice(&lines, max_lines),
    };
    let kept_lines = kept.iter().filter(|line| *line != ELLIPSIS).count();
    ToolOutput {
        content: kept.join("\n"),
        is_error: false,
        metadata: Some(json!({
            "strategy": strategy,
            "totalLines": total,
            "keptLines": kept_lines,
            "truncated": true,
        })),
    }
}

fn head_slice(lines: &[&str], max_lines: usize) -> Vec<String> {
    let mut kept: Vec<String> = lines
        .iter()
        .take(max_lines)
        .map(|line| (*line).to_owned())
        .collect();
    kept.push(ELLIPSIS.to_owned());
    kept
}

fn tail_slice(lines: &[&str], max_lines: usize) -> Vec<String> {
    let mut kept = vec![ELLIPSIS.to_owned()];
    kept.extend(
        lines
            .iter()
            .skip(lines.len() - max_lines)
            .map(|line| (*line).to_owned()),
    );
    kept
}

/// 首 [`SMART_EDGE_LINES`] 行 + 中间省略号 + 末 [`SMART_EDGE_LINES`] 行；
/// `max_lines` 不足两段时按比例收缩，仍保证首尾对称。
fn smart_slice(lines: &[&str], max_lines: usize) -> Vec<String> {
    let edge = SMART_EDGE_LINES.min(max_lines / 2).max(1);
    let mut kept: Vec<String> = lines
        .iter()
        .take(edge)
        .map(|line| (*line).to_owned())
        .collect();
    kept.push(ELLIPSIS.to_owned());
    kept.extend(
        lines
            .iter()
            .skip(lines.len() - edge)
            .map(|line| (*line).to_owned()),
    );
    kept
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
            .with_working_dir("/tmp/zk-brief")
            .with_session_id("sess-1")
    }

    #[tokio::test]
    async fn default_scope_is_project_brief() {
        let output = BriefTool.execute(json!({}), ctx()).await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.starts_with("## Project Brief"));
        assert!(output.content.contains("Working directory: /tmp/zk-brief"));
        assert!(output.content.contains("Session: sess-1"));
        assert!(output.content.contains("GitService integration"));
    }

    #[tokio::test]
    async fn session_and_custom_scopes_follow_the_legacy_shape() {
        let session = BriefTool
            .execute(json!({ "scope": "session" }), ctx())
            .await;
        assert!(session.content.starts_with("## Session Brief"));
        assert!(session.content.contains("SessionService integration"));

        let custom = BriefTool
            .execute(json!({ "scope": "custom", "topic": "release" }), ctx())
            .await;
        assert!(custom.content.starts_with("## Custom Brief: release"));
        assert!(custom.content.contains("LlmClient integration"));
    }

    #[tokio::test]
    async fn custom_scope_requires_topic_and_unknown_scope_is_rejected() {
        let missing = BriefTool.execute(json!({ "scope": "custom" }), ctx()).await;
        assert!(missing.is_error);
        assert!(missing.content.starts_with("BRIEF_TOPIC_REQUIRED: "));

        let unknown = BriefTool.execute(json!({ "scope": "weekly" }), ctx()).await;
        assert!(unknown.is_error);
        assert!(unknown.content.starts_with("BRIEF_SCOPE_INVALID: "));
    }

    #[tokio::test]
    async fn smart_strategy_keeps_both_edges() {
        let content = (1..=40)
            .map(|index| format!("line{index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let output = BriefTool
            .execute(json!({ "content": content }), ctx())
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.starts_with("line1\n"));
        assert!(output.content.contains(ELLIPSIS));
        assert!(output.content.ends_with("line40"));
        assert!(!output.content.contains("line20"));
        let metadata = output.metadata.expect("condense metadata");
        assert_eq!(metadata["totalLines"], 40);
        assert_eq!(metadata["keptLines"], 10);
        assert_eq!(metadata["truncated"], true);
    }

    #[tokio::test]
    async fn head_and_tail_strategies_slice_one_side() {
        let content = (1..=10)
            .map(|index| format!("line{index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let head = BriefTool
            .execute(
                json!({ "content": content.clone(), "max_lines": 3, "strategy": "head" }),
                ctx(),
            )
            .await;
        assert_eq!(head.content, format!("line1\nline2\nline3\n{ELLIPSIS}"));

        let tail = BriefTool
            .execute(
                json!({ "content": content, "max_lines": 2, "strategy": "tail" }),
                ctx(),
            )
            .await;
        assert_eq!(tail.content, format!("{ELLIPSIS}\nline9\nline10"));
    }

    #[tokio::test]
    async fn short_content_is_returned_verbatim_and_bad_strategy_rejected() {
        let intact = BriefTool
            .execute(json!({ "content": "a\nb", "max_lines": 20 }), ctx())
            .await;
        assert_eq!(intact.content, "a\nb");
        assert_eq!(
            intact.metadata.expect("metadata")["truncated"],
            json!(false)
        );

        let bad = BriefTool
            .execute(json!({ "content": "a\nb", "strategy": "middle" }), ctx())
            .await;
        assert!(bad.is_error);
        assert!(bad.content.starts_with("BRIEF_STRATEGY_INVALID: "));
    }
}
