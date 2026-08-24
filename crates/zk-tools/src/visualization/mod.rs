//! 可视化工具族——`Visualization` 工具 + 意图分类器 + 自动路由（Batch 8E）。
//!
//! 语义来源（旧仓库只读权威规格）：
//! - `tool/impl/VisualizationTool.java`（179L）——白名单校验 + props 透传 +
//!   经 Builder 出站，见 [`tool`]；
//! - `service/VisualizationPayloadBuilder.java`（77L）——`{ type, ts, uuid,
//!   viewType, props }` 消息信封，见本模块的 [`VisualizationPayload`]；
//! - `engine/VisualizationIntentClassifier.java`（257L）——关键词闸门 +
//!   缓存闸门，见 [`classifier`]；
//! - `engine/VisualizationAutoRouter.java`（112L）——描述 → 图表类型 → 出站
//!   的适配器，见 [`router`]。
//!
//! # 有意差异
//!
//! - 旧 `VisualizationPayloadBuilder` 直接 `convertAndSendToUser` 单播 WS 消息；
//!   zk-tools 不依赖 zk-protocol / WS 层，故本模块只**构造**信封
//!   （[`VisualizationPayload::to_envelope`]），出站由 [`crate::tool::ToolOutput`]
//!   的 `metadata` 随工具结果上抛，组合根/前端按 `viewType` 分派——与旧
//!   「独立消息路线」相比少一跳单播，可观察数据同构。
//! - 旧分类器第三步调 fast-model（`SideQueryService`）判别意图；本移植取
//!   **纯启发式**关键词表（[`classifier::DiagramKind::keywords`]），无 LLM
//!   往返、无 API 成本，故旧「开关闸门」（`visualization.auto-routing.enabled`
//!   默认 false 的零开销直返）不需要——启发式本身即零成本。
//! - 旧 `viewType` 白名单是 7 个前端组件名（`git-timeline` / `schema-viewer`
//!   / …）；本移植的 `diagram_type` 是三种**渲染载体**（`mermaid` /
//!   `plantuml` / `d3_json`），图表**种类**（流程 / 序列 / 类 / ER / 饼 /
//!   甘特）落在 [`classifier::DiagramKind`]——载体与种类分离，旧的 `mermaid`
//!   分支得以承载全部六种 mermaid 图。

pub mod classifier;
pub mod router;
pub mod tool;

pub use classifier::{DiagramKind, IntentClassifier, VisualizationHint};
pub use router::{
    HINT_CACHE_TTL, MAX_HINT_CACHE_ENTRIES, MAX_LABEL_CHARS, RoutedVisualization,
    VisualizationAutoRouter, mermaid_template, sanitize_label,
};
pub use tool::{ALLOWED_DIAGRAM_TYPES, MAX_DIAGRAM_BYTES, VisualizationTool};

use serde_json::{Value, json};

/// 可视化载荷（对照旧 `VisualizationPayloadBuilder.publish` 入参三元组
/// `sessionId` / `viewType` / `props` 中的后两项）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisualizationPayload {
    /// 渲染载体（[`ALLOWED_DIAGRAM_TYPES`] 之一，小写）。
    pub view_type: String,
    /// 图表标题（旧 props 的自由字段之一；缺省 `None`）。
    pub title: Option<String>,
    /// 图表定义源码（旧 `mermaid` 分支的 `props.source`）。
    pub source: String,
    /// 前端渲染提示（本移植追加：告知前端该用哪个渲染器）。
    pub render_hint: &'static str,
}

impl VisualizationPayload {
    /// 构造载荷；`view_type` 不在白名单内返回 `None`（对照旧
    /// `ALLOWED_VIEW_TYPES.contains` 校验）。
    #[must_use]
    pub fn build(view_type: &str, title: Option<&str>, source: impl Into<String>) -> Option<Self> {
        let normalized = view_type.trim().to_ascii_lowercase();
        let hint = render_hint(&normalized)?;
        Some(Self {
            view_type: normalized,
            title: title
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            source: source.into(),
            render_hint: hint,
        })
    }

    /// 导出消息信封（对照旧 `{ type, ts, uuid, viewType, props }` 逐键同构；
    /// `props` 内为 `source` / `title` / `renderHint`）。
    #[must_use]
    pub fn to_envelope(&self) -> Value {
        json!({
            "type": "visualization",
            "ts": chrono::Utc::now().timestamp_millis(),
            "uuid": uuid::Uuid::new_v4().to_string(),
            "viewType": self.view_type,
            "props": {
                "source": self.source,
                "title": self.title,
                "renderHint": self.render_hint,
            }
        })
    }

    /// Markdown 围栏语言（前端不渲染时的降级展示口径）。
    #[must_use]
    pub fn fence_language(&self) -> &'static str {
        match self.view_type.as_str() {
            "plantuml" => "plantuml",
            "d3_json" => "json",
            _ => "mermaid",
        }
    }
}

/// 渲染提示表——白名单即事实源（不在表内即非法 `view_type`）。
#[must_use]
pub fn render_hint(view_type: &str) -> Option<&'static str> {
    match view_type {
        "mermaid" => Some("mermaid"),
        "plantuml" => Some("plantuml-server"),
        "d3_json" => Some("d3-force"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_rejects_types_outside_the_whitelist() {
        assert!(VisualizationPayload::build("graphviz", None, "digraph {}").is_none());
        for view_type in ALLOWED_DIAGRAM_TYPES {
            assert!(VisualizationPayload::build(view_type, None, "x").is_some());
        }
    }

    #[test]
    fn build_normalizes_case_and_blank_title() {
        let payload = VisualizationPayload::build("  MERMAID ", Some("   "), "flowchart TD")
            .expect("whitelisted");
        assert_eq!(payload.view_type, "mermaid");
        assert_eq!(payload.title, None);
        assert_eq!(payload.render_hint, "mermaid");
        assert_eq!(payload.fence_language(), "mermaid");
    }

    #[test]
    fn envelope_matches_the_legacy_key_set() {
        let payload =
            VisualizationPayload::build("d3_json", Some("Deps"), "{\"nodes\":[]}").expect("built");
        let envelope = payload.to_envelope();
        assert_eq!(envelope["type"], "visualization");
        assert_eq!(envelope["viewType"], "d3_json");
        assert_eq!(envelope["props"]["title"], "Deps");
        assert_eq!(envelope["props"]["renderHint"], "d3-force");
        assert!(envelope["ts"].as_i64().is_some_and(|ts| ts > 0));
        assert_eq!(
            envelope["uuid"].as_str().map(str::len),
            Some(36),
            "uuid is a hyphenated v4"
        );
        assert_eq!(payload.fence_language(), "json");
    }
}
