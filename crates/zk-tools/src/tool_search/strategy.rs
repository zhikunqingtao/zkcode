//! 工具搜索策略——三策略路由 + 名称/描述打分。
//!
//! 语义来源（旧仓库只读）：
//! - `tool/impl/ToolSearchTool.java`（214L）的三种查询形态
//!   （`select:Read,Edit` / `+slack send` / 关键词），见 [`SearchStrategy`]；
//! - `tool/search/SearchStrategyRouter.java`（214L）的「分层加权 → 去重 →
//!   按相关度降序 → 截断」骨架，见 [`search`]。
//!
//! # 有意差异
//!
//! - 旧 `SearchStrategyRouter` 的分层对象是**文件**（当前目录 1.0 / 最近编辑
//!   0.8 / git 变更 0.6 / 全局 0.4），依赖 `FileSearchService` 与 `GitService`；
//!   本移植的分层对象是**工具**（名称精确 1.0 / 名称包含 0.8 / 描述 tf-idf
//!   ≤ 0.6），无外部服务依赖——加权阶梯与「去重后按相关度排序截断」的形状
//!   逐层对应。
//! - 旧 `Strategy.SCOPE_AWARE` 受 `SEARCH_STRATEGY_ROUTER` feature flag 门控，
//!   关闭即回落 `DEFAULT`；本移植的策略由**查询前缀**决定（`select:` / `+`），
//!   无 flag——三形态都是旧 `ToolSearchTool` 的既有行为，不需要灰度。

use serde_json::Value;

/// 结果条数默认值（旧 `ToolSearchTool` 无上限；本移植取 5，见
/// [`MAX_RESULTS`] 的硬上限说明）。
pub const DEFAULT_MAX_RESULTS: usize = 5;

/// 结果条数硬上限（对照旧 `SearchStrategyRouter.MAX_RESULTS = 20`）。
pub const MAX_RESULTS: usize = 20;

/// 名称精确命中权重（对照旧 Layer 1 的 `boost 1.0`）。
const NAME_EXACT_SCORE: f64 = 1.0;

/// 名称包含命中权重（对照旧 Layer 2 的 `boost 0.8`）。
const NAME_PARTIAL_SCORE: f64 = 0.8;

/// 描述命中权重上限（对照旧 Layer 3 的 `boost 0.6`）。
const DESCRIPTION_SCORE_CAP: f64 = 0.6;

/// tf 饱和常数（BM25 的 `k1` 简化口径）。
const TF_SATURATION: f64 = 1.5;

/// 工具描述符——搜索索引的最小条目（由 [`ToolCatalogPort`] 提供）。
///
/// [`ToolCatalogPort`]: super::ToolCatalogPort
#[derive(Clone, Debug, PartialEq)]
pub struct ToolDescriptor {
    /// 工具名（注册表键）。
    pub name: String,
    /// 工具描述（供关键词匹配）。
    pub description: String,
    /// 入参 JSON Schema（命中后随结果回传，模型据此即可调用）。
    pub parameters: Value,
}

impl ToolDescriptor {
    /// 由三元组构造。
    #[must_use]
    pub fn new(name: impl Into<String>, description: impl Into<String>, parameters: Value) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }
}

/// 查询策略（对照旧 `ToolSearchTool.call` 的三个分支）。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SearchStrategy {
    /// `select:Read,Edit,Grep`——按名称精确取用（旧 `findByNameOptional`）。
    Select {
        /// 待取用的工具名（原样保留大小写，比较时不敏感）。
        names: Vec<String>,
    },
    /// `+slack send`——名称必须包含首词，其余词参与排名。
    NameRequired {
        /// 名称必含的词（小写）。
        required: String,
        /// 其余排名词（小写）。
        terms: Vec<String>,
    },
    /// 关键词搜索（旧 `matchesQuery`：名称 / 描述 / 分组 / 别名）。
    Keyword {
        /// 查询词（小写）。
        terms: Vec<String>,
    },
}

/// 命中的工具及其相关度（对照旧 `SearchMatch(filePath, relevance, source)`）。
#[derive(Clone, Debug, PartialEq)]
pub struct ScoredTool {
    /// 工具名。
    pub name: String,
    /// 工具描述。
    pub description: String,
    /// 入参 JSON Schema。
    pub parameters: Value,
    /// 相关度（`0.0..=1.0`，降序排序键）。
    pub score: f64,
    /// 命中来源（对照旧 `SearchMatch.source`：`local` / `recent` / `global`）。
    pub reason: &'static str,
}

/// 按查询前缀选择策略（对照旧 `query.startsWith("select:")` /
/// `query.startsWith("+")` 的分支顺序）。
#[must_use]
pub fn select_strategy(query: &str) -> SearchStrategy {
    let trimmed = query.trim();
    if let Some(rest) = trimmed.strip_prefix("select:") {
        return SearchStrategy::Select {
            names: rest
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(ToOwned::to_owned)
                .collect(),
        };
    }
    if let Some(rest) = trimmed.strip_prefix('+') {
        let mut terms = lowercase_terms(rest);
        if !terms.is_empty() {
            let required = terms.remove(0);
            return SearchStrategy::NameRequired { required, terms };
        }
    }
    SearchStrategy::Keyword {
        terms: lowercase_terms(trimmed),
    }
}

/// 搜索入口——选策略 → 打分 → 去重排序 → 截断（对照旧
/// `scopeAwareSearch` → `deduplicateAndSort` → `limit(MAX_RESULTS)`）。
#[must_use]
pub fn search(catalog: &[ToolDescriptor], query: &str, max_results: usize) -> Vec<ScoredTool> {
    let limit = max_results.clamp(1, MAX_RESULTS);
    let strategy = select_strategy(query);
    let mut hits: Vec<ScoredTool> = match &strategy {
        SearchStrategy::Select { names } => catalog
            .iter()
            .filter(|tool| {
                names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&tool.name))
            })
            .map(|tool| scored(tool, NAME_EXACT_SCORE, "name-exact"))
            .collect(),
        SearchStrategy::NameRequired { required, terms } => {
            let idf = inverse_document_frequency(catalog, terms);
            catalog
                .iter()
                .filter(|tool| tool.name.to_lowercase().contains(required))
                .map(|tool| {
                    let bonus = description_score(tool, terms, &idf);
                    scored(
                        tool,
                        (NAME_PARTIAL_SCORE + bonus).min(NAME_EXACT_SCORE),
                        "name-required",
                    )
                })
                .collect()
        }
        SearchStrategy::Keyword { terms } => {
            let idf = inverse_document_frequency(catalog, terms);
            catalog
                .iter()
                .filter_map(|tool| keyword_hit(tool, terms, &idf))
                .collect()
        }
    };

    // 同名去重保留最高分（对照旧 `best.merge(filePath, …)`），再按分数降序、
    // 同分按名称升序（旧靠 `LinkedHashMap` 的插入序，本移植取字典序以稳定输出）。
    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.name.cmp(&right.name))
    });
    hits.dedup_by(|left, right| left.name == right.name);
    hits.truncate(limit);
    hits
}

fn scored(tool: &ToolDescriptor, score: f64, reason: &'static str) -> ScoredTool {
    ScoredTool {
        name: tool.name.clone(),
        description: tool.description.clone(),
        parameters: tool.parameters.clone(),
        score,
        reason,
    }
}

/// 关键词策略下的单工具打分：名称精确 → 名称包含 → 描述 tf-idf，取最高档。
fn keyword_hit(
    tool: &ToolDescriptor,
    terms: &[String],
    idf: &[(String, f64)],
) -> Option<ScoredTool> {
    if terms.is_empty() {
        return None;
    }
    let name = tool.name.to_lowercase();
    if terms.contains(&name) {
        return Some(scored(tool, NAME_EXACT_SCORE, "name-exact"));
    }
    if terms.iter().any(|term| name.contains(term.as_str())) {
        let bonus = description_score(tool, terms, idf);
        return Some(scored(
            tool,
            (NAME_PARTIAL_SCORE + bonus).min(NAME_EXACT_SCORE),
            "name-partial",
        ));
    }
    let score = description_score(tool, terms, idf);
    (score > 0.0).then(|| scored(tool, score, "description-match"))
}

/// 描述侧的简化 BM25：`Σ idf(t) · tf/(tf + k1)`，归一化到
/// `0..=DESCRIPTION_SCORE_CAP`。
fn description_score(tool: &ToolDescriptor, terms: &[String], idf: &[(String, f64)]) -> f64 {
    let haystack = tool.description.to_lowercase();
    let mut raw = 0.0_f64;
    for term in terms {
        let tf = count_occurrences(&haystack, term);
        if tf == 0 {
            continue;
        }
        let tf = to_f64(tf);
        let weight = idf
            .iter()
            .find(|(candidate, _)| candidate == term)
            .map_or(1.0, |(_, value)| *value);
        raw += weight * (tf / (tf + TF_SATURATION));
    }
    if raw <= 0.0 {
        return 0.0;
    }
    DESCRIPTION_SCORE_CAP * (raw / (raw + 1.0))
}

/// 逆文档频率——`ln(1 + (N - df + 0.5) / (df + 0.5))`（BM25 口径）。
fn inverse_document_frequency(catalog: &[ToolDescriptor], terms: &[String]) -> Vec<(String, f64)> {
    let total = to_f64(catalog.len());
    terms
        .iter()
        .map(|term| {
            let df = to_f64(
                catalog
                    .iter()
                    .filter(|tool| {
                        tool.description.to_lowercase().contains(term.as_str())
                            || tool.name.to_lowercase().contains(term.as_str())
                    })
                    .count(),
            );
            let idf = (1.0 + (total - df + 0.5) / (df + 0.5)).ln();
            (term.clone(), idf)
        })
        .collect()
}

fn count_occurrences(haystack: &str, needle: &str) -> usize {
    if needle.is_empty() {
        return 0;
    }
    haystack.matches(needle).count()
}

fn lowercase_terms(raw: &str) -> Vec<String> {
    raw.split_whitespace()
        .map(str::to_lowercase)
        .filter(|term| !term.is_empty())
        .collect()
}

/// `usize` → `f64`（词频/文档数量级远小于 `u32::MAX`，转换无损且不触发
/// `clippy::cast_precision_loss`）。
fn to_f64(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn catalog() -> Vec<ToolDescriptor> {
        vec![
            ToolDescriptor::new("Read", "读取文件内容", json!({ "type": "object" })),
            ToolDescriptor::new("NotebookEdit", "编辑 Jupyter notebook 的 cell", json!({})),
            ToolDescriptor::new("REPL", "长驻 python 解释器会话", json!({})),
            ToolDescriptor::new("Sleep", "延时等待指定秒数", json!({})),
        ]
    }

    #[test]
    fn select_prefix_takes_tools_by_exact_name() {
        let strategy = select_strategy("select:Read, Sleep ,");
        assert_eq!(
            strategy,
            SearchStrategy::Select {
                names: vec!["Read".to_owned(), "Sleep".to_owned()]
            }
        );
        let hits = search(&catalog(), "select:read,SLEEP", DEFAULT_MAX_RESULTS);
        let names: Vec<&str> = hits.iter().map(|hit| hit.name.as_str()).collect();
        assert_eq!(names, ["Read", "Sleep"], "name match is case-insensitive");
        assert!(hits.iter().all(|hit| hit.reason == "name-exact"));
    }

    #[test]
    fn plus_prefix_requires_the_first_term_in_the_name() {
        assert_eq!(
            select_strategy("+note jupyter"),
            SearchStrategy::NameRequired {
                required: "note".to_owned(),
                terms: vec!["jupyter".to_owned()]
            }
        );
        let hits = search(&catalog(), "+note jupyter", DEFAULT_MAX_RESULTS);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "NotebookEdit");
        assert!(
            hits[0].score > NAME_PARTIAL_SCORE,
            "matching terms add bonus"
        );
    }

    #[test]
    fn keyword_search_ranks_name_over_description() {
        let hits = search(&catalog(), "repl 解释器", DEFAULT_MAX_RESULTS);
        assert_eq!(hits[0].name, "REPL");
        assert_eq!(hits[0].reason, "name-exact");
    }

    #[test]
    fn description_only_matches_stay_below_name_matches() {
        let hits = search(&catalog(), "jupyter", DEFAULT_MAX_RESULTS);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "NotebookEdit");
        assert_eq!(hits[0].reason, "description-match");
        assert!(hits[0].score > 0.0 && hits[0].score <= DESCRIPTION_SCORE_CAP);
    }

    #[test]
    fn no_match_yields_empty_result() {
        assert!(search(&catalog(), "kubernetes", DEFAULT_MAX_RESULTS).is_empty());
        assert!(search(&catalog(), "   ", DEFAULT_MAX_RESULTS).is_empty());
    }

    #[test]
    fn results_are_capped_and_sorted_deterministically() {
        let hits = search(&catalog(), "select:Read,Sleep,REPL", 2);
        assert_eq!(hits.len(), 2, "limit applies");
        assert_eq!(hits[0].name, "REPL", "same score → name ascending");
        let hits = search(&catalog(), "select:Read", MAX_RESULTS + 100);
        assert_eq!(hits.len(), 1, "clamped limit still returns matches");
    }
}
