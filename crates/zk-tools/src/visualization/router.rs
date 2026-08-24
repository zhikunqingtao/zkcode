//! 可视化自动路由——「自然语言描述 → 图表种类 → mermaid 模板」。
//!
//! 语义来源（旧仓库只读）：`engine/VisualizationAutoRouter.java`（112L）的
//! 适配器职责（调分类器 → 命中则以 `Visualization` 工具为统一出口）+
//! `engine/VisualizationIntentClassifier.java` 的**闸门 3**
//! （`sha256(sessionId + userQuestion + toolSummary)` 键、10 分钟 TTL、
//! 空哨兵亦入缓存以免重复判别）。
//!
//! # 有意差异
//!
//! - 旧路由器从 `QueryLoopState` 倒序抽取最近一条用户消息的首个 `TextBlock`
//!   作为待判别文本；zk-tools 不依赖引擎的消息模型，故描述由调用方
//!   （`/visualize` 命令 / 引擎侧）显式传入。
//! - 旧缓存是 Caffeine（`expireAfterWrite` 10 分钟 + 容量上限）；本移植取
//!   [`dashmap::DashMap`] + 读路径惰性过期，超过 [`MAX_HINT_CACHE_ENTRIES`]
//!   时整表清空（无后台清理线程；命中率的短期回落换取零常驻任务）。
//! - 旧「空哨兵入缓存」是为省下重复的 LLM 调用；本移植的判别是纯内存启发式，
//!   重算成本可忽略，故只缓存**命中**结果。

use std::time::{Duration, Instant};

use dashmap::DashMap;

use super::classifier::{DiagramKind, IntentClassifier};

/// 缓存条目存活时长（对照旧 Caffeine 的 10 分钟 `expireAfterWrite`）。
pub const HINT_CACHE_TTL: Duration = Duration::from_mins(10);

/// 缓存条目上限——超过即整表清空（对照旧 Caffeine 的容量上限语义）。
pub const MAX_HINT_CACHE_ENTRIES: usize = 256;

/// 模板标签的最大字符数（超出按字符边界截断并加省略号）。
pub const MAX_LABEL_CHARS: usize = 60;

/// 标签为空时的兜底文案。
const FALLBACK_LABEL: &str = "Diagram";

/// 路由结果——图表种类 + 可直接交给 `Visualization` 工具的 mermaid 源码。
#[derive(Clone, Debug, PartialEq)]
pub struct RoutedVisualization {
    /// 选中的图表种类。
    pub kind: DiagramKind,
    /// 渲染载体（本路由器只产 mermaid；`plantuml` / `d3_json` 需调用方显式指定）。
    pub view_type: &'static str,
    /// 生成的图表定义源码。
    pub source: String,
    /// 分类置信度（见 `VisualizationHint::confidence`）。
    pub confidence: f64,
    /// 命中的关键词（判定依据留痕）。
    pub matched: Vec<&'static str>,
}

/// 自动路由器（对照旧 `@Component VisualizationAutoRouter`；持缓存故非无状态）。
#[derive(Debug, Default)]
pub struct VisualizationAutoRouter {
    cache: DashMap<String, (Instant, RoutedVisualization)>,
}

impl VisualizationAutoRouter {
    /// 构造空缓存路由器。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 路由入口——无关键词命中返回 `None`（对照旧「命中才出站」）。
    ///
    /// `session_id` 参与缓存键（对照旧 `sha256(sessionId + …)`），`None` 时
    /// 以空串参与——跨会话共享同一条缓存，语义上等价于「无会话归属」。
    #[must_use]
    pub fn route(
        &self,
        session_id: Option<&str>,
        description: &str,
    ) -> Option<RoutedVisualization> {
        let key = Self::cache_key(session_id, description);
        if let Some(hit) = self.lookup(&key) {
            return Some(hit);
        }

        let hint = IntentClassifier::classify(description)?;
        let routed = RoutedVisualization {
            kind: hint.kind,
            view_type: "mermaid",
            source: mermaid_template(hint.kind, &sanitize_label(description)),
            confidence: hint.confidence,
            matched: hint.matched,
        };
        self.store(key, routed.clone());
        Some(routed)
    }

    /// 当前缓存条目数（含未清理的过期条目；仅供诊断与单测）。
    #[must_use]
    pub fn cached_entries(&self) -> usize {
        self.cache.len()
    }

    /// 缓存键（对照旧 `sha256(sessionId + 0x00 + userQuestion + …)` 的
    /// 零字节分隔口径）。
    fn cache_key(session_id: Option<&str>, description: &str) -> String {
        let mut material = Vec::new();
        material.extend_from_slice(session_id.unwrap_or("").as_bytes());
        material.push(0);
        material.extend_from_slice(description.as_bytes());
        crate::atomic::sha256_hex(&material)
    }

    /// 读路径惰性过期：命中但已超 [`HINT_CACHE_TTL`] 即移除并视作未命中。
    fn lookup(&self, key: &str) -> Option<RoutedVisualization> {
        let fresh = self.cache.get(key).and_then(|entry| {
            let (stored_at, routed) = entry.value();
            (stored_at.elapsed() < HINT_CACHE_TTL).then(|| routed.clone())
        });
        if fresh.is_none() {
            self.cache.remove(key);
        }
        fresh
    }

    fn store(&self, key: String, routed: RoutedVisualization) {
        if self.cache.len() >= MAX_HINT_CACHE_ENTRIES {
            self.cache.clear();
        }
        self.cache.insert(key, (Instant::now(), routed));
    }
}

/// 生成图表模板——各种类一份最小可渲染骨架，`label` 作为标题/主节点文案。
#[must_use]
pub fn mermaid_template(kind: DiagramKind, label: &str) -> String {
    match kind {
        DiagramKind::Flowchart => format!(
            "flowchart TD\n    start([Start]) --> step[{label}]\n    step --> done([Done])\n"
        ),
        DiagramKind::Sequence => format!(
            "sequenceDiagram\n    autonumber\n    participant User\n    participant System\n    \
             User->>System: {label}\n    System-->>User: result\n"
        ),
        DiagramKind::ClassDiagram => format!(
            "classDiagram\n    class Subject {{\n        +String name\n        +describe()\n    }}\n    \
             note for Subject \"{label}\"\n"
        ),
        DiagramKind::ErDiagram => format!(
            "erDiagram\n    %% {label}\n    OWNER ||--o{{ ITEM : owns\n    OWNER {{\n        \
             int id\n        string name\n    }}\n"
        ),
        DiagramKind::Pie => {
            format!("pie title {label}\n    \"Part A\" : 60\n    \"Part B\" : 40\n")
        }
        DiagramKind::Gantt => format!(
            "gantt\n    title {label}\n    dateFormat YYYY-MM-DD\n    section Phase 1\n    \
             Task A :a1, 2026-01-01, 7d\n    section Phase 2\n    Task B :after a1, 7d\n"
        ),
    }
}

/// 标签净化——剔除会破坏 mermaid 语法的字符、压缩空白、按字符边界截断。
#[must_use]
pub fn sanitize_label(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|ch| {
            if ch.is_control() || "[]{}()<>\"'|;#%`".contains(ch) {
                ' '
            } else {
                ch
            }
        })
        .collect();
    let collapsed = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return FALLBACK_LABEL.to_owned();
    }
    if collapsed.chars().count() <= MAX_LABEL_CHARS {
        return collapsed;
    }
    let kept: String = collapsed.chars().take(MAX_LABEL_CHARS).collect();
    format!("{kept}…")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_produces_a_mermaid_template_for_the_classified_kind() {
        let router = VisualizationAutoRouter::new();
        let hit = router
            .route(Some("s-1"), "画一下登录流程")
            .expect("classified");
        assert_eq!(hit.kind, DiagramKind::Flowchart);
        assert_eq!(hit.view_type, "mermaid");
        assert!(hit.source.starts_with("flowchart TD"));
        assert!(hit.source.contains("登录流程"));
    }

    #[test]
    fn route_returns_none_without_keyword_hit() {
        let router = VisualizationAutoRouter::new();
        assert!(router.route(None, "今天天气不错").is_none());
        assert_eq!(router.cached_entries(), 0, "misses are not cached");
    }

    #[test]
    fn identical_description_hits_the_cache() {
        let router = VisualizationAutoRouter::new();
        let first = router
            .route(Some("s-1"), "接口调用链 sequence")
            .expect("classified");
        assert_eq!(router.cached_entries(), 1);
        let second = router
            .route(Some("s-1"), "接口调用链 sequence")
            .expect("cached");
        assert_eq!(first, second);
        assert_eq!(router.cached_entries(), 1);
        // 会话不同 → 缓存键不同（对照旧 sessionId 参与摘要）。
        router
            .route(Some("s-2"), "接口调用链 sequence")
            .expect("classified");
        assert_eq!(router.cached_entries(), 2);
    }

    #[test]
    fn cache_clears_when_capacity_is_reached() {
        let router = VisualizationAutoRouter::new();
        for index in 0..=MAX_HINT_CACHE_ENTRIES {
            router
                .route(Some("s"), &format!("流程 {index}"))
                .expect("classified");
        }
        assert!(
            router.cached_entries() <= MAX_HINT_CACHE_ENTRIES,
            "capacity guard keeps the table bounded"
        );
    }

    #[test]
    fn every_kind_renders_a_non_empty_template() {
        for kind in DiagramKind::ALL {
            let source = mermaid_template(kind, "Label");
            assert!(source.contains("Label"), "{kind}");
            assert!(source.ends_with('\n'), "{kind}");
        }
    }

    #[test]
    fn sanitize_label_strips_hostile_chars_and_truncates() {
        assert_eq!(sanitize_label("a[b]{c}\nd"), "a b c d");
        assert_eq!(sanitize_label("  []  "), FALLBACK_LABEL);
        let long = "字".repeat(MAX_LABEL_CHARS + 10);
        let label = sanitize_label(&long);
        assert_eq!(
            label.chars().count(),
            MAX_LABEL_CHARS + 1,
            "kept + ellipsis"
        );
        assert!(label.ends_with('…'));
    }
}
