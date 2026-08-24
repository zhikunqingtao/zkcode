//! `ToolSearch` 工具——按意图检索/推荐已注册工具。
//!
//! 语义来源（旧仓库只读）：`tool/impl/ToolSearchTool.java`（214L）——三种查询
//! 形态、命中后回传完整 JSON Schema、无命中时的引导文案、3 分钟执行超时。
//!
//! # 有意差异
//!
//! - 旧工具持 `@Lazy ToolRegistry` 并在命中 deferred 工具后调
//!   `toolRegistry.activate(sessionId, names)` 激活；本 workspace 的注册表无
//!   延迟加载/激活面（所有工具恒可见），故激活一步不存在——`ToolSearch` 在此
//!   是**发现与推荐**工具（大工具池下帮模型选对工具），命中即可直接调用。
//! - 旧工具直接依赖注册表实例；zk-tools 侧经 [`ToolCatalogPort`] 端口反转
//!   （与 [`crate::ctx_inspect::ContextInfoPort`] 同一模式），生产实现由
//!   zk-server 组合根在装配完其余工具后以快照注入——避免注册表构造期自引用。

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::{Value, json};

use super::strategy::{DEFAULT_MAX_RESULTS, MAX_RESULTS, ScoredTool, ToolDescriptor, search};
use crate::input::{optional_usize, required_str};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 工具名（组合根注册键；亦是自描述条目的名字）。
pub const TOOL_SEARCH_NAME: &str = "ToolSearch";

/// 工具描述（对照旧 `getDescription()` 的语义：按关键词发现工具）。
pub const TOOL_SEARCH_DESCRIPTION: &str =
    "按关键词搜索可用工具，返回相关度排序的工具清单、入参 Schema 与调用建议";

/// 无命中时的引导文案（对照旧 `"No tools found matching: " + query`）。
pub const NO_TOOLS_FOUND: &str = "No tools found matching";

/// 执行超时（对照旧 `getMaxExecutionTimeMs() = 180_000L`）。
const SEARCH_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(3);

/// 工具清单端口——反转依赖（zk-tools 不依赖组合根的注册表实例）。
pub trait ToolCatalogPort: Send + Sync {
    /// 返回可被搜索的工具清单（实现方决定是否含自身）。
    fn catalog(&self) -> Vec<ToolDescriptor>;
}

/// 静态清单实现——组合根装配完注册表后的一次性快照。
#[derive(Clone, Debug, Default)]
pub struct StaticToolCatalog {
    entries: Vec<ToolDescriptor>,
}

impl StaticToolCatalog {
    /// 由描述符列表构造。
    #[must_use]
    pub fn new(entries: Vec<ToolDescriptor>) -> Self {
        Self { entries }
    }
}

impl ToolCatalogPort for StaticToolCatalog {
    fn catalog(&self) -> Vec<ToolDescriptor> {
        self.entries.clone()
    }
}

/// `ToolSearch` 工具（名 `ToolSearch`）。
pub struct ToolSearchTool {
    catalog: Arc<dyn ToolCatalogPort>,
}

impl ToolSearchTool {
    /// 构造（清单经端口注入）。
    #[must_use]
    pub fn new(catalog: Arc<dyn ToolCatalogPort>) -> Self {
        Self { catalog }
    }

    /// 自描述条目——组合根可把它并入快照，使 `ToolSearch` 能搜到自己
    /// （对照旧 `alwaysLoad() == true` 的「自身始终可见」语义）。
    #[must_use]
    pub fn descriptor() -> ToolDescriptor {
        ToolDescriptor::new(
            TOOL_SEARCH_NAME,
            TOOL_SEARCH_DESCRIPTION,
            parameters_schema(),
        )
    }
}

fn parameters_schema() -> Value {
    json!({
        "type": "object",
        "required": ["query"],
        "properties": {
            "query": {
                "type": "string",
                "description": "搜索词；支持 'select:Read,Edit' 精确取用与 '+notebook jupyter' 名称必含形态"
            },
            "max_results": {
                "type": "integer",
                "description": format!("最大返回条数，默认 {DEFAULT_MAX_RESULTS}，上限 {MAX_RESULTS}")
            }
        }
    })
}

impl Tool for ToolSearchTool {
    fn name(&self) -> &'static str {
        TOOL_SEARCH_NAME
    }

    fn description(&self) -> &'static str {
        TOOL_SEARCH_DESCRIPTION
    }

    fn parameters(&self) -> Value {
        parameters_schema()
    }

    fn timeout(&self) -> std::time::Duration {
        SEARCH_TIMEOUT
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let catalog = Arc::clone(&self.catalog);
        Box::pin(async move { run(&input, catalog.as_ref()) })
    }
}

fn run(input: &Value, catalog: &dyn ToolCatalogPort) -> ToolOutput {
    let query = match required_str(input, "query") {
        Ok(value) => value,
        Err(rejection) => return rejection,
    };
    let max_results = optional_usize(input, "max_results").unwrap_or(DEFAULT_MAX_RESULTS);
    let entries = catalog.catalog();
    let hits = search(&entries, query, max_results);

    if hits.is_empty() {
        return ToolOutput::ok(format!(
            "{NO_TOOLS_FOUND}: {query}\n\n\
             Try different keywords, 'select:<Name>' for an exact tool, or /help for commands."
        ));
    }
    ToolOutput {
        content: render(query, &hits),
        is_error: false,
        metadata: Some(json!({
            "query": query,
            "matches": hits
                .iter()
                .map(|hit| json!({
                    "name": hit.name,
                    "score": hit.score,
                    "reason": hit.reason,
                }))
                .collect::<Vec<Value>>(),
        })),
    }
}

/// 人读渲染（对照旧 `StringBuilder` 逐条 `**Name**` / 描述 / Schema 的形状；
/// Schema 取紧凑单行而非 pretty，避免大工具池下结果体积膨胀）。
fn render(query: &str, hits: &[ScoredTool]) -> String {
    use std::fmt::Write as _;

    let mut out = format!("Found {} tool(s) matching '{query}':\n\n", hits.len());
    for hit in hits {
        let _ = writeln!(
            out,
            "**{}** (score={:.2}, via {})",
            hit.name, hit.score, hit.reason
        );
        let _ = writeln!(out, "  {}", hit.description);
        let schema = serde_json::to_string(&hit.parameters).unwrap_or_else(|_| "{}".to_owned());
        let _ = writeln!(out, "  Schema: {schema}\n");
    }
    if let Some(best) = hits.first() {
        let _ = writeln!(out, "调用建议：优先使用 `{}`（相关度最高）。", best.name);
    }
    out
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    fn tool() -> ToolSearchTool {
        ToolSearchTool::new(Arc::new(StaticToolCatalog::new(vec![
            ToolDescriptor::new("Read", "读取文件内容", json!({ "type": "object" })),
            ToolDescriptor::new("NotebookEdit", "编辑 Jupyter notebook 的 cell", json!({})),
            ToolSearchTool::descriptor(),
        ])))
    }

    #[tokio::test]
    async fn keyword_query_returns_ranked_matches_with_schema() {
        let output = tool().execute(json!({ "query": "jupyter" }), ctx()).await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.contains("Found 1 tool(s)"));
        assert!(output.content.contains("**NotebookEdit**"));
        assert!(output.content.contains("Schema: {}"));
        assert!(output.content.contains("调用建议"));
        let metadata = output.metadata.expect("structured matches");
        assert_eq!(metadata["matches"][0]["name"], "NotebookEdit");
        assert_eq!(metadata["matches"][0]["reason"], "description-match");
    }

    #[tokio::test]
    async fn select_form_takes_named_tools() {
        let output = tool()
            .execute(json!({ "query": "select:Read,ToolSearch" }), ctx())
            .await;
        assert!(output.content.contains("**Read**"));
        assert!(
            output.content.contains("**ToolSearch**"),
            "self descriptor is searchable"
        );
    }

    #[tokio::test]
    async fn empty_result_returns_guidance_not_error() {
        let output = tool()
            .execute(json!({ "query": "kubernetes" }), ctx())
            .await;
        assert!(!output.is_error, "no match is a successful empty answer");
        assert!(
            output
                .content
                .starts_with("No tools found matching: kubernetes")
        );
        assert!(output.metadata.is_none());
    }

    #[tokio::test]
    async fn missing_query_is_rejected() {
        let output = tool().execute(json!({}), ctx()).await;
        assert!(output.is_error);
        assert!(output.content.starts_with("MISSING_PARAMETER: "));
    }

    #[tokio::test]
    async fn max_results_caps_the_listing() {
        let output = tool()
            .execute(
                json!({ "query": "select:Read,NotebookEdit,ToolSearch", "max_results": 1 }),
                ctx(),
            )
            .await;
        assert!(output.content.contains("Found 1 tool(s)"));
    }

    #[test]
    fn timeout_matches_the_legacy_three_minutes() {
        let tool = tool();
        assert_eq!(tool.timeout(), std::time::Duration::from_mins(3));
        assert_eq!(tool.name(), TOOL_SEARCH_NAME);
        assert!(tool.is_read_only(&json!({})));
    }
}
