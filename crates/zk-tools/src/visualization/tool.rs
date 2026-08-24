//! `Visualization` 工具——图表定义源码 → 标准化可视化载荷。
//!
//! 语义来源（旧仓库只读）：`tool/impl/VisualizationTool.java`（179L）——
//! 「白名单校验 + props 规范化 + 经 `VisualizationPayloadBuilder` 出站」三步，
//! 不承担业务数据查询（数据由调用方或前端组件自行拉取）。
//!
//! # 有意差异
//!
//! - 旧工具 `shouldDefer() == true`（不进默认 tool prompt，仅由
//!   `VisualizationAutoRouter` 内部调用）；本 workspace 的
//!   [`crate::registry::ToolRegistry`] 无延迟加载面，故该工具对模型可见——
//!   `ToolSearch`（见 [`crate::tool_search`]）承担旧「按需发现」的等价职责。
//! - 旧 `sessionId` 缺失即 `VISUALIZATION_SESSION_REQUIRED` 拒绝（推送必须有
//!   单播目标）；本工具不做推送，载荷随工具结果的 `metadata` 上抛，故不要求
//!   会话——无会话时结果仍可用（人读文本 + 结构化载荷）。

use std::fmt::Write as _;

use futures::future::BoxFuture;
use serde_json::{Value, json};

use super::VisualizationPayload;
use crate::input::{failure, optional_str, required_str};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 允许的渲染载体白名单（对照旧 `ALLOWED_VIEW_TYPES`：不在表内即拒绝）。
pub const ALLOWED_DIAGRAM_TYPES: [&str; 3] = ["mermaid", "plantuml", "d3_json"];

/// 图表源码字节上限——超限拒绝（旧无此闸门；本移植追加，避免把整个文件
/// 当图表源码塞进结果与前端渲染器）。
pub const MAX_DIAGRAM_BYTES: usize = 64 * 1024;

/// `Visualization` 工具（名 `Visualization`）。
#[derive(Clone, Copy, Debug, Default)]
pub struct VisualizationTool;

impl Tool for VisualizationTool {
    fn name(&self) -> &'static str {
        "Visualization"
    }

    fn description(&self) -> &'static str {
        "生成可视化图表载荷（mermaid / plantuml / d3_json），供前端按 viewType 渲染"
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "required": ["diagram_type", "content"],
            "properties": {
                "diagram_type": {
                    "type": "string",
                    "enum": ALLOWED_DIAGRAM_TYPES,
                    "description": "渲染载体：mermaid | plantuml | d3_json"
                },
                "content": {
                    "type": "string",
                    "description": "图表定义源码（mermaid/plantuml 文本；d3_json 为 JSON）"
                },
                "title": { "type": "string", "description": "图表标题（可选）" }
            }
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(futures::future::ready(run(&input)))
    }
}

fn run(input: &Value) -> ToolOutput {
    let diagram_type = match required_str(input, "diagram_type") {
        Ok(value) => value,
        Err(rejection) => return rejection,
    };
    let content = match required_str(input, "content") {
        Ok(value) => value,
        Err(rejection) => return rejection,
    };
    if content.len() > MAX_DIAGRAM_BYTES {
        return failure(
            "VISUALIZATION_CONTENT_TOO_LARGE",
            format!(
                "content exceeds {MAX_DIAGRAM_BYTES} bytes limit: {} bytes",
                content.len()
            ),
        );
    }

    let title = optional_str(input, "title");
    let Some(payload) = VisualizationPayload::build(diagram_type, title, content) else {
        return failure(
            "VISUALIZATION_TYPE_UNSUPPORTED",
            format!(
                "Unsupported diagram_type: {diagram_type} (allowed: {})",
                ALLOWED_DIAGRAM_TYPES.join(" / ")
            ),
        );
    };
    // `d3_json` 的源码必须是可解析 JSON——否则前端渲染器必然失败，早拒更清晰。
    if payload.view_type == "d3_json" && serde_json::from_str::<Value>(&payload.source).is_err() {
        return failure(
            "VISUALIZATION_CONTENT_INVALID",
            "content is not valid JSON while diagram_type=d3_json",
        );
    }

    let mut text = String::new();
    if let Some(title) = &payload.title {
        let _ = writeln!(text, "## {title}\n");
    }
    let _ = writeln!(text, "```{}", payload.fence_language());
    let _ = writeln!(text, "{}", payload.source.trim_end());
    let _ = writeln!(text, "```\n");
    let _ = writeln!(text, "- viewType: {}", payload.view_type);
    let _ = writeln!(text, "- renderHint: {}", payload.render_hint);

    let envelope = payload.to_envelope();
    ToolOutput {
        content: text,
        is_error: false,
        metadata: Some(json!({
            "visualization": envelope,
            "structuredResult": {
                "schema": "visualization",
                "viewType": payload.view_type,
                "props": {
                    "source": payload.source,
                    "title": payload.title,
                    "renderHint": payload.render_hint,
                }
            }
        })),
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
    }

    #[tokio::test]
    async fn mermaid_source_yields_fenced_text_and_envelope() {
        let output = VisualizationTool
            .execute(
                json!({
                    "diagram_type": "mermaid",
                    "content": "flowchart TD\n    a --> b",
                    "title": "Login flow"
                }),
                ctx(),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.contains("## Login flow"));
        assert!(output.content.contains("```mermaid"));
        assert!(output.content.contains("a --> b"));
        let metadata = output.metadata.expect("payload envelope");
        let envelope = &metadata["visualization"];
        assert_eq!(envelope["viewType"], "mermaid");
        assert_eq!(envelope["props"]["title"], "Login flow");
        assert_eq!(metadata["structuredResult"]["schema"], "visualization");
    }

    #[tokio::test]
    async fn unknown_diagram_type_is_rejected() {
        let output = VisualizationTool
            .execute(
                json!({ "diagram_type": "graphviz", "content": "digraph {}" }),
                ctx(),
            )
            .await;
        assert!(output.is_error);
        assert!(
            output
                .content
                .starts_with("VISUALIZATION_TYPE_UNSUPPORTED: "),
            "{}",
            output.content
        );
    }

    #[tokio::test]
    async fn missing_parameters_are_rejected() {
        for input in [
            json!({ "content": "flowchart TD" }),
            json!({ "diagram_type": "mermaid" }),
            json!({ "diagram_type": "mermaid", "content": "   " }),
        ] {
            let output = VisualizationTool.execute(input, ctx()).await;
            assert!(output.is_error);
            assert!(output.content.starts_with("MISSING_PARAMETER: "));
        }
    }

    #[tokio::test]
    async fn d3_json_requires_parsable_json() {
        let bad = VisualizationTool
            .execute(
                json!({ "diagram_type": "d3_json", "content": "nodes: []" }),
                ctx(),
            )
            .await;
        assert!(bad.is_error);
        assert!(bad.content.starts_with("VISUALIZATION_CONTENT_INVALID: "));

        let good = VisualizationTool
            .execute(
                json!({ "diagram_type": "d3_json", "content": "{\"nodes\": []}" }),
                ctx(),
            )
            .await;
        assert!(!good.is_error, "{}", good.content);
        assert!(good.content.contains("```json"));
    }

    #[tokio::test]
    async fn oversized_content_is_rejected() {
        let output = VisualizationTool
            .execute(
                json!({
                    "diagram_type": "mermaid",
                    "content": "x".repeat(MAX_DIAGRAM_BYTES + 1)
                }),
                ctx(),
            )
            .await;
        assert!(output.is_error);
        assert!(
            output
                .content
                .starts_with("VISUALIZATION_CONTENT_TOO_LARGE: ")
        );
    }

    #[test]
    fn spec_declares_the_whitelist_and_read_only_nature() {
        let spec = VisualizationTool.spec();
        assert_eq!(spec.name, "Visualization");
        assert_eq!(
            spec.parameters["properties"]["diagram_type"]["enum"],
            json!(ALLOWED_DIAGRAM_TYPES)
        );
        assert!(VisualizationTool.is_read_only(&json!({})));
        assert!(!VisualizationTool.is_destructive(&json!({})));
    }
}
