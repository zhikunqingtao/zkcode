//! 可视化意图分类器——关键词启发式「自然语言描述 → 图表种类」。
//!
//! 语义来源（旧仓库只读）：`engine/VisualizationIntentClassifier.java`（257L）
//! 的**闸门 2**（`KEYWORD_PATTERN` 关键词命中才放行）与它的 `viewType`
//! 取值集合。
//!
//! # 有意差异
//!
//! - 旧闸门 2 只做「是否放行」的布尔判定，真正的类型选择交给 fast-model；
//!   本移植把关键词表按**图表种类**分桶，命中数即得分，直接选出类型——省掉
//!   一次 LLM 往返（旧 `TIMEOUT_MS = 15_000` 的 side-query）。
//! - 旧 `VisualizationHint` 有 `dataSource` 字段（供前端拉数据）；本分类器
//!   不产出数据源（模板自持示例数据），故该字段未移植。

use std::fmt;

/// 图表种类（对照旧 `SYSTEM_PROMPT` 允许的 `viewType` 取值集合中的
/// mermaid 可承载部分：流程 / 序列 / 类 / ER / 饼 / 甘特）。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum DiagramKind {
    /// 流程图（旧 `mermaid` 分支的 generic flow）。
    Flowchart,
    /// 序列图（旧 `api-sequence-diagram`）。
    Sequence,
    /// 类图（旧 `change-impact-graph` 的类继承视角）。
    ClassDiagram,
    /// 实体关系图（旧 `schema-viewer`）。
    ErDiagram,
    /// 饼图（占比分布）。
    Pie,
    /// 甘特图（旧 `git-timeline` 的时间线视角）。
    Gantt,
}

impl DiagramKind {
    /// 全部种类——声明序即打平票时的优先序（越靠前越优先）。
    pub const ALL: [Self; 6] = [
        Self::Flowchart,
        Self::Sequence,
        Self::ClassDiagram,
        Self::ErDiagram,
        Self::Pie,
        Self::Gantt,
    ];

    /// 稳定字面量（`/visualize <type>` 的显式取值、`metadata.kind` 的落值）。
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Flowchart => "flowchart",
            Self::Sequence => "sequence",
            Self::ClassDiagram => "class_diagram",
            Self::ErDiagram => "er_diagram",
            Self::Pie => "pie",
            Self::Gantt => "gantt",
        }
    }

    /// 由字面量解析（大小写不敏感；同时接受 mermaid 侧的别名如
    /// `sequenceDiagram` / `classDiagram` / `erDiagram`）。
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "flowchart" | "flow" | "graph" => Some(Self::Flowchart),
            "sequence" | "sequencediagram" => Some(Self::Sequence),
            "class_diagram" | "class" | "classdiagram" => Some(Self::ClassDiagram),
            "er_diagram" | "er" | "erdiagram" => Some(Self::ErDiagram),
            "pie" => Some(Self::Pie),
            "gantt" => Some(Self::Gantt),
            _ => None,
        }
    }

    /// 该种类的触发关键词（中英混排，全小写；对照旧 `KEYWORD_PATTERN`
    /// 的中英双语词面并按种类分桶）。
    #[must_use]
    pub fn keywords(self) -> &'static [&'static str] {
        match self {
            Self::Flowchart => &[
                "流程",
                "流程图",
                "工作流",
                "步骤",
                "flow",
                "process",
                "workflow",
            ],
            Self::Sequence => &["序列", "时序", "交互", "调用链", "sequence", "interaction"],
            Self::ClassDiagram => &["类图", "类", "继承", "接口", "class", "inherit"],
            Self::ErDiagram => &["实体", "关系", "表结构", "schema", "entity", "er", "ddl"],
            Self::Pie => &["饼", "饼图", "占比", "比例", "分布", "pie", "ratio"],
            Self::Gantt => &[
                "甘特",
                "时间线",
                "排期",
                "里程碑",
                "gantt",
                "timeline",
                "roadmap",
            ],
        }
    }
}

impl fmt::Display for DiagramKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 分类结果（对照旧 `engine/VisualizationHint.java` 的 `viewType` +
/// `params`；`dataSource` 未移植，见模块级差异说明）。
#[derive(Clone, Debug, PartialEq)]
pub struct VisualizationHint {
    /// 选中的图表种类。
    pub kind: DiagramKind,
    /// 置信度 = 胜出种类命中词数 / 全部命中词数（`0.0..=1.0`）。
    pub confidence: f64,
    /// 命中的关键词（按关键词表声明序，便于解释判定依据）。
    pub matched: Vec<&'static str>,
}

/// 关键词启发式分类器（无状态，对照旧 `@Service` 单例）。
#[derive(Clone, Copy, Debug, Default)]
pub struct IntentClassifier;

impl IntentClassifier {
    /// 分类入口——无任何关键词命中时返回 `None`（对照旧闸门 2 的
    /// 「关键词未命中即 `VisualizationHint.EMPTY`」）。
    #[must_use]
    pub fn classify(description: &str) -> Option<VisualizationHint> {
        let haystack = description.to_lowercase();
        if haystack.trim().is_empty() {
            return None;
        }

        let mut best: Option<(DiagramKind, Vec<&'static str>)> = None;
        let mut total_hits = 0_usize;
        for kind in DiagramKind::ALL {
            let hits: Vec<&'static str> = kind
                .keywords()
                .iter()
                .copied()
                .filter(|keyword| haystack.contains(*keyword))
                .collect();
            total_hits += hits.len();
            // 严格大于：打平票时保留先声明者（[`DiagramKind::ALL`] 的优先序）。
            let better = best
                .as_ref()
                .is_none_or(|(_, current)| hits.len() > current.len());
            if better && !hits.is_empty() {
                best = Some((kind, hits));
            }
        }

        let (kind, matched) = best?;
        Some(VisualizationHint {
            kind,
            confidence: ratio(matched.len(), total_hits),
            matched,
        })
    }
}

/// `part / whole` 的安全比值（`whole == 0` → `0.0`）。
///
/// 经 `u32` 中转而非 `as f64` 直转：命中词数量级远小于 `u32::MAX`，
/// 转换无损且不触发 `clippy::cast_precision_loss`。
fn ratio(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        return 0.0;
    }
    let part = f64::from(u32::try_from(part).unwrap_or(u32::MAX));
    let whole = f64::from(u32::try_from(whole).unwrap_or(u32::MAX));
    part / whole
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyword_table_routes_each_kind() {
        for (description, expected) in [
            ("画一下登录流程", DiagramKind::Flowchart),
            ("show me the request sequence", DiagramKind::Sequence),
            ("这些类的继承关系", DiagramKind::ClassDiagram),
            ("数据库 schema 的实体关系", DiagramKind::ErDiagram),
            ("各模块代码占比", DiagramKind::Pie),
            ("给出迭代排期甘特", DiagramKind::Gantt),
        ] {
            let hint = IntentClassifier::classify(description)
                .unwrap_or_else(|| panic!("must classify: {description}"));
            assert_eq!(hint.kind, expected, "{description}");
            assert!(!hint.matched.is_empty());
            assert!(hint.confidence > 0.0);
        }
    }

    #[test]
    fn no_keyword_yields_none() {
        assert!(IntentClassifier::classify("随便聊点什么").is_none());
        assert!(IntentClassifier::classify("   ").is_none());
    }

    #[test]
    fn confidence_is_one_when_only_one_kind_matches() {
        let hint = IntentClassifier::classify("gantt roadmap 排期").expect("classified");
        assert_eq!(hint.kind, DiagramKind::Gantt);
        assert!((hint.confidence - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn confidence_drops_when_kinds_compete() {
        let hint = IntentClassifier::classify("流程 与 序列 与 时序 的对照").expect("classified");
        assert_eq!(hint.kind, DiagramKind::Sequence, "2 hits beat 1 hit");
        assert!(hint.confidence < 1.0);
    }

    #[test]
    fn parse_accepts_literals_and_mermaid_aliases() {
        assert_eq!(DiagramKind::parse("ER"), Some(DiagramKind::ErDiagram));
        assert_eq!(
            DiagramKind::parse("classDiagram"),
            Some(DiagramKind::ClassDiagram)
        );
        assert_eq!(DiagramKind::parse("nope"), None);
        for kind in DiagramKind::ALL {
            assert_eq!(DiagramKind::parse(kind.as_str()), Some(kind));
            assert_eq!(kind.to_string(), kind.as_str());
        }
    }

    #[test]
    fn ties_prefer_the_earlier_declared_kind() {
        // 「流程」（Flowchart）与「甘特」（Gantt）各 1 命中 → 取先声明者。
        let hint = IntentClassifier::classify("流程 甘特").expect("classified");
        assert_eq!(hint.kind, DiagramKind::Flowchart);
    }
}
