//! prompt caching 缓存断点：系统提示分段 + 工具定义分界（`Anthropic` 专属）。
//!
//! 逐字段对照旧仓库（只读权威规格）：
//!
//! - `llm/SystemPrompt.java`（record `SystemPrompt(List<Segment>)`，
//!   `Segment(String text, boolean cacheControl)`，静态工厂 `of` / `ofCached`，
//!   `toPlainText()` 以 `"\n"` 连接全段）；
//! - `prompt/SystemPromptBuilder.java` L279-313 `buildSystemPromptWithCacheControl`：
//!   以 `__SYSTEM_PROMPT_DYNAMIC_BOUNDARY__` 标记切「静态前缀 / 动态后缀」——
//!   标记之前 `cacheControl=true`（跨会话不变、可缓存），之后 `cacheControl=false`
//!   （会话特定）；空白段跳过；无标记时整体单段 `cacheControl=false`；
//!   `__TOOL_CACHE_BREAKPOINT__` 标记先被滤除（仅服务于工具定义层，见下）；
//! - `tool/ToolRegistry.java` L131-165 `getToolDefinitionsWithCacheBreakpoint`：
//!   工具排序（内建在前、MCP 在后、组内按名，L209-214）后在**最后一个内建工具**
//!   的定义上追加 `cache_control`，且**仅当存在 MCP 工具**时才追加——使 MCP 工具
//!   变更不冲掉内建工具部分的缓存命中；
//! - `service/PromptCacheBreakDetector.java` L114-132 `computeBreakpointPosition`：
//!   返回首个 MCP 工具下标（`-1` = 无 MCP 工具 = 无需断点），供提示构建侧判定
//!   是否插入 `TOOL_CACHE_BREAKPOINT` 标记；
//! - `llm/impl/AnthropicProvider.java` L158-163：`cache_control` 的 wire 形状恒为
//!   `{"type":"ephemeral"}`（旧仓库唯一取值）。
//!
//! # 两个断点（相互独立）
//!
//! 1. **系统提示断点**——落在静态前缀段末尾（[`SystemPrompt::segmented`]），由
//!    [`crate::provider::ChatRequest::system_segments`] 承载；
//! 2. **工具定义断点**——落在最后一个内建工具定义上（[`tool_cache_breakpoint`]），
//!    由 [`crate::provider::ChatRequest::tool_cache_breakpoint`] 承载。
//!
//! 两者各自可控：任一为空 / `None` 时对应断点不注入，另一路不受影响。
//!
//! # Provider 门控
//!
//! 与旧实现同构——`cache_control` 的注入点只存在于 `Anthropic` 原生请求构造
//! （旧 `AnthropicProvider` 是 `@ConditionalOnProperty(llm.provider=anthropic)`
//! 的独立类，旧 `OpenAiCompatibleProvider.buildOpenAiRequest` 全程无该字段）。
//! 本实现同样只在 [`crate::anthropic`] 的请求构建里读这两个字段；
//! [`crate::openai_compat`] 仅经 [`crate::provider::ChatRequest::system_text`]
//! 取拍平纯文本（对照旧 `toPlainText()` 桥接），永不产出 `cache_control`。
//!
//! # 提示构建侧的 feature flag（不在本层）
//!
//! 旧仓库中「是否分段」由提示构建侧的 feature flag 决定：
//! `SystemPromptBuilder.shouldUseGlobalCacheScope()`（L1176-1178）=
//! `FeatureFlagService.isEnabled("PROMPT_CACHE_GLOBAL_SCOPE")`，未注册键默认
//! `false`（`getFeatureValue(key, false)`）——关闭时不插入 boundary 标记，
//! `buildSystemPromptWithCacheControl` 便退化为「整体单段」。本层不复制该 flag：
//! zk-llm 以「调用方是否提供分段」表达同一门控（不提供 = 单段旧行为），flag
//! 归属提示构建域（zk-engine 侧系统提示装配）。
//!
//! # 与旧实现的差异（留痕）
//!
//! 1. 旧强类型桥接 `LlmProvider.streamChat`（L38-54）把 `SystemPrompt` 拍平成
//!    `toPlainText()` 再交给 provider，故旧 `Anthropic` 路径**丢失**分段信息、
//!    恒对整段打单一断点。本实现把分段透传到 wire（每段一个 text block，可缓存
//!    段带 `cache_control`）——是旧分段能力的接线补齐，而非语义改写：**无分段
//!    输入时逐字段等价旧单块行为**（同 `{type,text,cache_control}` 三键顺序）。
//! 2. 工具来源标记不落在 [`crate::provider::ToolSpec`] 上（该结构由 zk-engine
//!    构造，本步不改其形状），改以 [`ToolOrigin`] 序列经 [`tool_cache_breakpoint`]
//!    折算为下标随请求传递——断点落点规则与旧
//!    `getToolDefinitionsWithCacheBreakpoint` 逐条等价。

use serde_json::{Value, json};

/// `cache_control` 的类型值（旧仓库唯一取值 `ephemeral`）。
pub const EPHEMERAL: &str = "ephemeral";

/// `cache_control` 断点对象 `{"type":"ephemeral"}`（旧 `AnthropicProvider` L163）。
#[must_use]
pub fn ephemeral_cache_control() -> Value {
    json!({ "type": EPHEMERAL })
}

/// 系统提示的一段（文本 + 该段末尾是否打缓存断点）。
///
/// 对照旧 `SystemPrompt.Segment(String text, boolean cacheControl)`：
/// `cache_control=true` 表示「该段末尾是缓存分界」——静态前缀（跨会话不变）
/// 用 `true`，动态后缀（会话特定）用 `false`。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SystemPromptSegment {
    /// 段文本。
    pub text: String,
    /// 是否在本段末尾插入 `cache_control` 断点。
    pub cache_control: bool,
}

impl SystemPromptSegment {
    /// 可缓存段（段末打断点，对照旧 `SystemPromptSegment.cached`）。
    #[must_use]
    pub fn cached(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            cache_control: true,
        }
    }

    /// 动态段（不打断点，对照旧 `SystemPromptSegment.dynamic`）。
    #[must_use]
    pub fn dynamic(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            cache_control: false,
        }
    }
}

/// 分段系统提示（对照旧 `llm/SystemPrompt.java` record）。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SystemPrompt {
    segments: Vec<SystemPromptSegment>,
}

impl SystemPrompt {
    /// 以显式分段构造（段序即 wire 上 `system` 数组的块序）。
    #[must_use]
    pub fn new(segments: Vec<SystemPromptSegment>) -> Self {
        Self { segments }
    }

    /// 单段、不打断点（对照旧 `SystemPrompt.of`）。
    #[must_use]
    pub fn of(text: impl Into<String>) -> Self {
        Self::new(vec![SystemPromptSegment::dynamic(text)])
    }

    /// 单段 + 段末断点（对照旧 `SystemPrompt.ofCached`）。
    #[must_use]
    pub fn of_cached(text: impl Into<String>) -> Self {
        Self::new(vec![SystemPromptSegment::cached(text)])
    }

    /// 静态前缀 + 动态后缀（对照旧 `buildSystemPromptWithCacheControl`
    /// L300-312）：前缀 `cache_control=true`、后缀 `false`，**空白段跳过**。
    ///
    /// 两段皆空白时得到空提示（渲染侧不发 `system` 字段，对齐旧 `isBlank()`
    /// 判定）。
    #[must_use]
    pub fn segmented(static_prefix: impl Into<String>, dynamic_suffix: impl Into<String>) -> Self {
        let prefix = static_prefix.into();
        let suffix = dynamic_suffix.into();
        let mut segments = Vec::with_capacity(2);
        if !prefix.trim().is_empty() {
            segments.push(SystemPromptSegment::cached(prefix));
        }
        if !suffix.trim().is_empty() {
            segments.push(SystemPromptSegment::dynamic(suffix));
        }
        Self::new(segments)
    }

    /// 分段视图（只读）。
    #[must_use]
    pub fn segments(&self) -> &[SystemPromptSegment] {
        &self.segments
    }

    /// 是否无任何段。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// 拍平为纯文本（对照旧 `toPlainText()`：全段以 `"\n"` 连接）。
    #[must_use]
    pub fn to_plain_text(&self) -> String {
        self.segments
            .iter()
            .map(|segment| segment.text.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// 交出分段（供请求构造转移所有权）。
    #[must_use]
    pub fn into_segments(self) -> Vec<SystemPromptSegment> {
        self.segments
    }
}

/// 工具来源（对照旧 `Tool.isMcp()`，L173 默认 `false` = 内建）。
///
/// 决定工具定义层缓存断点的落点：内建工具定义稳定（可缓存），MCP 工具定义随
/// 服务器连接动态增删，故断点落在两者分界处。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolOrigin {
    /// 内建工具（旧 `isMcp() == false`）。
    BuiltIn,
    /// MCP 服务器工具（旧 `isMcp() == true`）。
    Mcp,
}

impl ToolOrigin {
    /// 是否为 MCP 来源（对照旧 `Tool.isMcp()`）。
    #[must_use]
    pub fn is_mcp(self) -> bool {
        matches!(self, Self::Mcp)
    }
}

/// 工具定义层缓存断点下标（对照旧 `getToolDefinitionsWithCacheBreakpoint`
/// L143-156）。
///
/// 规则逐条等价：
/// 1. 取**最后一个**内建工具的下标（旧 `lastBuiltInIdx` 循环不 break——故内建
///    工具即便散落在 MCP 工具之后也取最后一个）；
/// 2. 仅当序列中**存在** MCP 工具时才成立（旧 `hasMcpTools` 与条件
///    `i == lastBuiltInIdx && hasMcpTools`）；
/// 3. 无内建工具或无 MCP 工具 → `None`（无断点）。
///
/// `origins` 需与 [`crate::provider::ChatRequest::tools`] 同序（旧
/// `getEnabledToolsSorted`：内建在前、MCP 在后、组内按名）。
#[must_use]
pub fn tool_cache_breakpoint(origins: &[ToolOrigin]) -> Option<usize> {
    let mut last_built_in = None;
    let mut has_mcp = false;
    for (index, origin) in origins.iter().enumerate() {
        if origin.is_mcp() {
            has_mcp = true;
        } else {
            last_built_in = Some(index);
        }
    }
    if has_mcp { last_built_in } else { None }
}

/// 首个 MCP 工具下标（对照旧 `PromptCacheBreakDetector.computeBreakpointPosition`
/// L123-132；旧返回 `-1` ⇒ 本实现 `None`）。
///
/// 旧提示构建侧据此判定是否插入 `TOOL_CACHE_BREAKPOINT` 标记
/// （`SystemPromptBuilder` L268-271：`breakpointPos >= 0` 才插入）。
#[must_use]
pub fn first_mcp_index(origins: &[ToolOrigin]) -> Option<usize> {
    origins.iter().position(|origin| origin.is_mcp())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ephemeral_cache_control_shape_matches_legacy() {
        // 旧 L163：Map.of("type", "ephemeral")。
        assert_eq!(ephemeral_cache_control(), json!({ "type": "ephemeral" }));
        assert_eq!(EPHEMERAL, "ephemeral");
    }

    #[test]
    fn segment_constructors_set_cache_control() {
        assert!(SystemPromptSegment::cached("static").cache_control);
        assert!(!SystemPromptSegment::dynamic("volatile").cache_control);
    }

    #[test]
    fn system_prompt_factories_match_legacy_record() {
        // 旧 SystemPrompt.of → cacheControl=false；ofCached → true。
        let plain = SystemPrompt::of("hello");
        assert_eq!(plain.segments().len(), 1);
        assert!(!plain.segments()[0].cache_control);
        let cached = SystemPrompt::of_cached("hello");
        assert!(cached.segments()[0].cache_control);
        assert!(SystemPrompt::default().is_empty());
    }

    #[test]
    fn segmented_marks_static_prefix_only_and_skips_blank() {
        let prompt = SystemPrompt::segmented("static core", "session state");
        let segments = prompt.segments();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].text, "static core");
        assert!(segments[0].cache_control, "静态前缀末尾是缓存分界");
        assert_eq!(segments[1].text, "session state");
        assert!(!segments[1].cache_control, "动态后缀不打断点");

        // 空白段跳过（旧 L306-311 的 isBlank 判定）。
        let only_static = SystemPrompt::segmented("static core", "   \n ");
        assert_eq!(only_static.segments().len(), 1);
        assert!(only_static.segments()[0].cache_control);
        let only_dynamic = SystemPrompt::segmented("", "session state");
        assert_eq!(only_dynamic.segments().len(), 1);
        assert!(!only_dynamic.segments()[0].cache_control);
        assert!(SystemPrompt::segmented("", "  ").is_empty());
    }

    #[test]
    fn to_plain_text_joins_with_newline_like_legacy() {
        // 旧 toPlainText：Collectors.joining("\n")。
        let prompt = SystemPrompt::segmented("a", "b");
        assert_eq!(prompt.to_plain_text(), "a\nb");
        assert_eq!(SystemPrompt::of("solo").to_plain_text(), "solo");
        assert_eq!(SystemPrompt::default().to_plain_text(), "");
        assert_eq!(prompt.clone().into_segments().len(), 2);
    }

    #[test]
    fn tool_breakpoint_lands_on_last_builtin_only_with_mcp() {
        use ToolOrigin::{BuiltIn, Mcp};
        // 内建在前、MCP 在后 → 断点 = 最后一个内建工具下标。
        assert_eq!(
            tool_cache_breakpoint(&[BuiltIn, BuiltIn, Mcp, Mcp]),
            Some(1)
        );
        // 无 MCP 工具 → 无断点（旧 hasMcpTools 条件）。
        assert_eq!(tool_cache_breakpoint(&[BuiltIn, BuiltIn]), None);
        // 无内建工具 → 无断点（旧 lastBuiltInIdx 保持 -1）。
        assert_eq!(tool_cache_breakpoint(&[Mcp, Mcp]), None);
        assert_eq!(tool_cache_breakpoint(&[]), None);
        // 旧 lastBuiltInIdx 循环不 break：内建工具散落在 MCP 之后时取最后一个。
        assert_eq!(tool_cache_breakpoint(&[BuiltIn, Mcp, BuiltIn]), Some(2));
    }

    #[test]
    fn first_mcp_index_matches_legacy_compute_breakpoint_position() {
        use ToolOrigin::{BuiltIn, Mcp};
        assert_eq!(first_mcp_index(&[BuiltIn, BuiltIn, Mcp]), Some(2));
        assert_eq!(first_mcp_index(&[Mcp, BuiltIn]), Some(0));
        // 旧实现返回 -1（无 MCP 工具）。
        assert_eq!(first_mcp_index(&[BuiltIn]), None);
        assert_eq!(first_mcp_index(&[]), None);
        assert!(Mcp.is_mcp());
        assert!(!BuiltIn.is_mcp());
    }
}
