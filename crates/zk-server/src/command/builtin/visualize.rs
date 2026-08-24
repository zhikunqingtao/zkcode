//! `/visualize`——按描述或显式类型生成可视化图表（Batch 8E）。
//!
//! 语义来源（旧仓库只读）：`engine/VisualizationAutoRouter.java`（112L）注释
//! 明确「与 `/visualize` 命令走同一 `VisualizationPayloadBuilder`」——本命令
//! 即那条入口：分类器选型 → 模板/正文 → 标准化载荷。
//!
//! 用法：
//! - `/visualize <description>` — 关键词分类（流程 / 序列 / 类 / ER / 饼 /
//!   甘特）后生成 mermaid 骨架；
//! - `/visualize <flowchart|sequence|class_diagram|er_diagram|pie|gantt> <label>`
//!   — 跳过分类，按指定**种类**生成骨架；
//! - `/visualize <mermaid|plantuml|d3_json> <source>` — 按指定**渲染载体**
//!   原样承载已有图表源码。
//!
//! # 有意差异
//!
//! - 旧路由器在引擎循环内调 `Visualization` 工具出站；本命令不经工具执行器
//!   （命令层无 `ToolContext` 可构造），而直接复用同一
//!   [`VisualizationPayload`] 构造器——载荷字节同构。
//! - 无关键词命中时旧路由器静默不出站；命令必须有回答，故回落
//!   [`DiagramKind::Flowchart`] 骨架并在 `props.note` 说明。

use futures::future::BoxFuture;
use serde_json::json;
use zk_tools::visualization::{
    ALLOWED_DIAGRAM_TYPES, DiagramKind, VisualizationAutoRouter, VisualizationPayload,
    mermaid_template, sanitize_label,
};

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// 无参时的用法提示。
const USAGE: &str = "Usage: /visualize <description> | /visualize <type> <content>";

/// 未命中关键词时的回落说明（落在 `props.note`）。
const FALLBACK_NOTE: &str = "no diagram keyword matched; defaulted to flowchart";

/// `/visualize` 命令（持路由器缓存，故非单元结构体）。
#[derive(Debug, Default)]
pub(super) struct VisualizeCommand {
    router: VisualizationAutoRouter,
}

impl Command for VisualizeCommand {
    fn name(&self) -> &'static str {
        "visualize"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["viz"]
    }

    fn description(&self) -> &'static str {
        "Generate a diagram (mermaid / plantuml / d3_json) from a description or explicit source"
    }

    fn command_type(&self) -> CommandType {
        CommandType::LocalJsx
    }

    fn execute<'a>(
        &'a self,
        args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move { self.run(args, ctx) })
    }
}

impl VisualizeCommand {
    fn run(&self, args: &str, ctx: &CommandContext) -> CommandResult {
        let trimmed = args.trim();
        if trimmed.is_empty() {
            return CommandResult::error(USAGE);
        }

        let (head, rest) = split_head(trimmed);
        // 1) 显式渲染载体：正文原样承载。
        if ALLOWED_DIAGRAM_TYPES.contains(&head.to_ascii_lowercase().as_str()) {
            if rest.is_empty() {
                return CommandResult::error(format!("Missing diagram source. {USAGE}"));
            }
            let Some(payload) = VisualizationPayload::build(head, None, rest) else {
                return CommandResult::error(format!("Unsupported diagram type: {head}"));
            };
            return render(&payload, None, None);
        }

        // 2) 显式图表种类：按模板生成骨架。
        if let Some(kind) = DiagramKind::parse(head) {
            let label = sanitize_label(if rest.is_empty() { head } else { rest });
            let source = mermaid_template(kind, &label);
            let Some(payload) = VisualizationPayload::build("mermaid", Some(&label), source) else {
                return CommandResult::error("mermaid carrier unavailable");
            };
            return render(&payload, Some(kind), None);
        }

        // 3) 自然语言描述：走分类器 + 缓存。
        let routed = self.router.route(Some(&ctx.session_id), trimmed);
        let label = sanitize_label(trimmed);
        let (kind, source, note) = match routed {
            Some(routed) => (routed.kind, routed.source, None),
            None => (
                DiagramKind::Flowchart,
                mermaid_template(DiagramKind::Flowchart, &label),
                Some(FALLBACK_NOTE),
            ),
        };
        let Some(payload) = VisualizationPayload::build("mermaid", Some(&label), source) else {
            return CommandResult::error("mermaid carrier unavailable");
        };
        render(&payload, Some(kind), note)
    }
}

/// 首词与其余部分（首词用于判定显式类型）。
fn split_head(trimmed: &str) -> (&str, &str) {
    match trimmed.split_once(char::is_whitespace) {
        Some((head, rest)) => (head, rest.trim()),
        None => (trimmed, ""),
    }
}

/// 出站——`JSX` 载荷即旧 Builder 的信封，附 `kind` / `note` / `markdown` 便于
/// 前端无渲染器时降级展示。
fn render(
    payload: &VisualizationPayload,
    kind: Option<DiagramKind>,
    note: Option<&str>,
) -> CommandResult {
    let mut envelope = payload.to_envelope();
    if let Some(props) = envelope
        .get_mut("props")
        .and_then(serde_json::Value::as_object_mut)
    {
        props.insert(
            "kind".to_owned(),
            kind.map_or(serde_json::Value::Null, |kind| json!(kind.as_str())),
        );
        props.insert("note".to_owned(), json!(note));
        props.insert(
            "markdown".to_owned(),
            json!(format!(
                "```{}\n{}\n```",
                payload.fence_language(),
                payload.source.trim_end()
            )),
        );
    }
    CommandResult::jsx(envelope)
}

#[cfg(test)]
mod tests {
    use super::{FALLBACK_NOTE, USAGE};
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(args: &str) -> CommandResult {
        let ctx = CommandContext::of("s-1", "/tmp", "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry.find_command("visualize").expect("registered");
        command.execute(args, &ctx).await
    }

    fn props(result: &CommandResult) -> &serde_json::Value {
        match result {
            CommandResult::Jsx(data) => &data["props"],
            other => panic!("expected JSX, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_args_return_usage_error() {
        assert_eq!(run("   ").await, CommandResult::error(USAGE));
    }

    #[tokio::test]
    async fn description_is_classified_into_a_diagram_kind() {
        let result = run("画一下下单的流程").await;
        let props = props(&result);
        assert_eq!(props["kind"], "flowchart");
        assert!(props["note"].is_null());
        assert!(
            props["source"]
                .as_str()
                .expect("source")
                .starts_with("flowchart TD")
        );
        assert!(
            props["markdown"]
                .as_str()
                .expect("markdown")
                .starts_with("```mermaid")
        );
    }

    #[tokio::test]
    async fn unmatched_description_falls_back_to_flowchart_with_a_note() {
        let result = run("随便看看").await;
        let props = props(&result);
        assert_eq!(props["kind"], "flowchart");
        assert_eq!(props["note"], FALLBACK_NOTE);
    }

    #[tokio::test]
    async fn explicit_kind_skips_classification() {
        let result = run("gantt 发布排期").await;
        let props = props(&result);
        assert_eq!(props["kind"], "gantt");
        assert!(props["source"].as_str().expect("source").contains("gantt"));
        assert_eq!(props["title"], "发布排期");
    }

    #[tokio::test]
    async fn explicit_carrier_passes_the_source_through() {
        let result = run("plantuml @startuml\nA -> B\n@enduml").await;
        let props = props(&result);
        assert!(props["kind"].is_null(), "carrier mode has no kind");
        assert_eq!(props["renderHint"], "plantuml-server");
        assert!(props["source"].as_str().expect("source").contains("A -> B"));

        let missing = run("mermaid").await;
        assert!(matches!(missing, CommandResult::Error(_)));
    }

    #[tokio::test]
    async fn alias_resolves_to_the_same_command() {
        let registry = CommandRegistry::with_builtin_commands();
        let aliased = registry.find_command("viz").expect("alias registered");
        assert_eq!(aliased.name(), "visualize");
    }
}
