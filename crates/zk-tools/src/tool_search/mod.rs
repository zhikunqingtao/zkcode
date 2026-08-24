//! 工具搜索族——按意图检索/推荐已注册工具（Batch 8E）。
//!
//! 语义来源（旧仓库只读）：
//! - `tool/impl/ToolSearchTool.java`（214L）——工具面；
//! - `tool/search/SearchStrategyRouter.java`（214L）——分层加权 → 去重 →
//!   降序 → 截断的策略骨架。
//!
//! 模块划分：
//! - [`strategy`]：查询形态识别（`select:` / `+` / 关键词）与相关度打分；
//! - [`tool`]：[`Tool`] 实现 + [`ToolCatalogPort`] 端口。
//!
//! [`Tool`]: crate::tool::Tool

pub mod strategy;
pub mod tool;

pub use strategy::{
    DEFAULT_MAX_RESULTS, MAX_RESULTS, ScoredTool, SearchStrategy, ToolDescriptor, search,
    select_strategy,
};
pub use tool::{
    NO_TOOLS_FOUND, StaticToolCatalog, TOOL_SEARCH_DESCRIPTION, TOOL_SEARCH_NAME, ToolCatalogPort,
    ToolSearchTool,
};
