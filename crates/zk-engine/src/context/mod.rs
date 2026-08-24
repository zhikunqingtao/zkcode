//! 上下文级联压缩域——token 估算、阈值判定、六层级联编排与增量折叠状态。
//!
//! 建模来源（旧仓库只读权威规格，`main@581d407b`）：
//! - `engine/ContextCascade.java`：级联编排器（`executePreApiCascade` /
//!   `executeErrorRecoveryCascade` / `calculateTokenWarningState` /
//!   `AutoCompactTrackingState`）；
//! - `engine/TokenCounter.java`：字符比估算（`estimateTokens(messages, model)`
//!   = 总字符 / `tokenCharRatio` + 消息数 × 4，向上取整）；
//! - `engine/SnipService.java`：L0 工具结果首尾截断；
//! - `engine/MicroCompactService.java`：L1 白名单旧工具结果清除；
//! - `engine/ContextCollapseService.java`：L2 三级渐进折叠；
//! - `engine/CompactService.java`：L3 三区划分 + 关键消息选择 + 尾部截断；
//! - `engine/IncrementalCollapseManager.java`：会话级累计轮次折叠触发。
//!
//! # 六层级联
//!
//! | 层 | 名称 | 触发时机 | 旧对照 |
//! |---|---|---|---|
//! | L0 | Snip | 每次 API 调用前无条件 | `SnipService.snipToolResults` |
//! | L1 | `MicroCompact` | 每次 API 调用前无条件 | `MicroCompactService.compactMessages` |
//! | L2 | `ContextCollapse` | 每次 API 调用前无条件 | `ContextCollapseService.progressiveCollapse` |
//! | L3 | `AutoCompact` | buffer-based 阈值命中 | `CompactService.compact(.., false)` |
//! | L4 | `CollapseDrain` | 413 恢复 Phase 1 | `CompactService.compact(cw × 0.5, true)` |
//! | L5 | `ReactiveCompact` | 413 恢复 Phase 2 | `CompactService.reactiveCompact` |
//!
//! L0-L2 代价极低（纯字符操作，无 LLM 调用）故每轮无条件执行；L3 仅在
//! buffer-based 阈值命中时执行；L4/L5 仅在 413 错误恢复路径执行。用户要求
//! 「追求最强推理」，故压缩策略以保留推理质量为先——阈值判定沿用旧
//! buffer-based 算法（预留 25% 摘要空间 + 13k 缓冲），未超限不动上下文。
//!
//! # 消息模型映射（旧 `Message` → [`zk_llm::ChatMessage`]）
//!
//! 旧四类消息与新 [`zk_llm::Role`] 构成双射，压缩逻辑逐条按此映射移植：
//!
//! | 旧 | 新 |
//! |---|---|
//! | `Message.SystemMessage` | [`Role::System`] |
//! | `Message.UserMessage`（`toolUseResult == null`） | [`Role::User`] |
//! | `Message.UserMessage`（`toolUseResult != null`） | [`Role::Tool`] |
//! | `Message.AssistantMessage` | [`Role::Assistant`] |
//!
//! 旧 `ContentBlock.ImageBlock` 在本层无对应载体（[`zk_llm::ChatMessage`]
//! 为纯文本，多模态归 Phase 2+ 待办），故媒体相关处理见
//! [`crate::recovery`] 的 Phase 3 说明。

#![allow(
    clippy::module_name_repetitions,
    reason = "ContextCascade / ContextCollapse 等命名对齐旧领域术语，不按模块前缀裁剪"
)]

pub mod cascade;
pub mod compact;
// Batch 0 P0-15：图片 / Base64 上下文预算守卫（对照旧 `TokenBudgetGuard`）。
pub mod image_budget;
pub mod incremental;
pub mod token_counter;

use std::sync::OnceLock;

use zk_llm::{ChatMessage, Role};

pub use cascade::{
    AutoCompactTrackingState, CLEARED_MESSAGE, COMPACTABLE_TOOLS, CascadeLevel, CascadeResult,
    ContextCascade, MAX_CONSECUTIVE_FAILURES,
};
pub use compact::{
    COMPACT_SUMMARY_MARKER, CompactResult, CompactionPlan, MessagePriority, Summarizer,
    compact_messages, reactive_compact,
};
pub use incremental::{
    COLLAPSE_SEGMENT_TURNS, CollapsedSegment, IncrementalCollapseManager, SESSION_TIMEOUT_MS,
};
pub use token_counter::{
    DEFAULT_CHARS_PER_TOKEN, TokenCountTier, count_text, install_feature_flags, precise_enabled,
};

/// 级联总开关环境变量（`false` / `0` / `off` → 关闭，其余含缺省 → 开启）。
///
/// 关闭时引擎行为与接入前逐字一致：不执行任何压缩、413 直接走既有失败序列。
pub const CASCADE_ENABLED_ENV: &str = "ZK_CONTEXT_CASCADE_ENABLED";

/// buffer-based 自动压缩缓冲 token（逐字对照旧
/// `ContextCascade.AUTOCOMPACT_BUFFER_TOKENS_DEFAULT`）。
pub const AUTOCOMPACT_BUFFER_TOKENS: u32 = 13_000;

/// 工具结果预算占上下文窗口比例（逐字对照旧
/// `ContextCascade.TOOL_RESULT_BUDGET_RATIO`）。
pub const TOOL_RESULT_BUDGET_RATIO: f64 = 0.3;

/// `MicroCompact` 保护尾部消息数（逐字对照旧
/// `ContextCascade.MICRO_COMPACT_PROTECTED_TAIL`）。
pub const MICRO_COMPACT_PROTECTED_TAIL: usize = 10;

/// 每条消息的边界开销 token（逐字对照旧 `TokenCounter` 的 `size() * 4`）。
const PER_MESSAGE_OVERHEAD_TOKENS: u64 = 4;

/// 工具结果块的结构开销字符（逐字对照旧 `estimateBlockChars` 的
/// `ToolResultBlock → content.length() + 10`）。
const TOOL_RESULT_BLOCK_OVERHEAD_CHARS: u64 = 10;

/// 工具调用块的 JSON 结构开销字符（逐字对照旧 `estimateBlockChars` 的
/// `ToolUseBlock → name + input + 20`）。
const TOOL_USE_BLOCK_OVERHEAD_CHARS: u64 = 20;

/// 级联总开关（进程级一次性读取环境变量；缺省开启）。
///
/// 默认启用；`ZK_CONTEXT_CASCADE_ENABLED=false`（或 `0` / `off` / `no`，
/// 大小写不敏感）时关闭，用于在热路径回归时一键退回接入前行为。
#[must_use]
pub fn cascade_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var(CASCADE_ENABLED_ENV) {
        Ok(raw) => !matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "false" | "0" | "off" | "no"
        ),
        Err(_) => true,
    })
}

/// 模型上下文窗口（取自 zk-llm 能力表；未知模型走
/// [`zk_llm::DEFAULT_CAPABILITIES`] 的 8192）。
///
/// 对照旧 `ModelRegistry.getContextWindowForModel(model)`。
#[must_use]
pub fn context_window_for(model: &str) -> u32 {
    zk_llm::capabilities_for(model).context_window
}

/// 模型字符/token 比（对照旧 `ModelRegistry.getTokenCharRatio(model)`；
/// 未知模型为 3.5，与旧 `TokenCounter.DEFAULT_CHARS_PER_TOKEN` 同值）。
#[must_use]
pub fn token_char_ratio(model: &str) -> f64 {
    let ratio = zk_llm::capabilities_for(model).token_char_ratio;
    if ratio > 0.0 { ratio } else { 3.5 }
}

/// 单条消息的等效字符数（逐条对照旧 `TokenCounter.estimateMessageChars`
/// 对各 `ContentBlock` 的求和）。
///
/// - 文本正文 → 字符数（旧 `TextBlock`）；
/// - 每个 `tool_calls` 项 → 名称 + 入参 + 20（旧 `ToolUseBlock`）；
/// - [`Role::Tool`] 消息 → 额外 +10（旧 `ToolResultBlock`）。
///
/// 字符计数取 Unicode 标量数而非字节数——旧 Java `String.length()` 为
/// UTF-16 码元数，二者在 BMP（含全部 CJK 常用字）一致。
#[must_use]
pub fn message_chars(message: &ChatMessage) -> u64 {
    let mut chars = char_count(&message.content);
    for call in &message.tool_calls {
        chars +=
            char_count(&call.name) + char_count(&call.arguments) + TOOL_USE_BLOCK_OVERHEAD_CHARS;
    }
    if message.role == Role::Tool {
        chars += TOOL_RESULT_BLOCK_OVERHEAD_CHARS;
    }
    chars
}

/// 字符数（Unicode 标量计数，饱和到 [`u64::MAX`]）。
fn char_count(text: &str) -> u64 {
    u64::try_from(text.chars().count()).unwrap_or(u64::MAX)
}

/// 消息列表 token 估算（逐条对照旧
/// `TokenCounter.estimateTokens(List<Message>, String modelId)`）。
///
/// 算法：`ceil(总字符 / tokenCharRatio + 消息数 × 4)`，饱和到
/// [`u32::MAX`]（旧 `saturatingTokens`）。
///
/// Batch 0 P0-21 起改由 [`token_counter::count`] 承载四级回退链：
/// `PRECISE_TOKENIZER` 关闭（出厂默认）时行为与上式**逐字一致**——即 L2 模型
/// 比率 / L3 全局 3.5；开启后 L1 走 `tiktoken` `cl100k_base` 精确编码。
#[must_use]
pub fn estimate_tokens(messages: &[ChatMessage], model: &str) -> u32 {
    token_counter::count(messages, model)
}

/// 单条消息 token 估算（旧 `estimateTokens(List.of(msg))` 的等价便捷式）。
#[must_use]
pub fn estimate_message_tokens(message: &ChatMessage, model: &str) -> u32 {
    estimate_tokens(std::slice::from_ref(message), model)
}

/// 向上取整并饱和到 [`u32::MAX`]（逐字对照旧
/// `TokenCounter.saturatingTokens(double)`）。
#[must_use]
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "上界已由 f64::from(u32::MAX) 比较夹紧、下界由 <= 0 分支拦截，转换无溢出"
)]
pub fn saturating_tokens(value: f64) -> u32 {
    if !value.is_finite() || value >= f64::from(u32::MAX) {
        return u32::MAX;
    }
    if value <= 0.0 {
        return 0;
    }
    value.ceil() as u32
}

/// Batch 0 Step 0-6 遥测辅助：`(压缩释放百分比, 剩余 token 数)`。
///
/// 由 `before_tokens` / `after_tokens` 派生：`percent = (before - after) *
/// 100 / before`（`before` 为 0 时返回 0，避免除零；`after > before` 时
/// 走 `saturating_sub` 归 0，与旧 `totalTokensFreed()` 的 `Math.max(0, ...)`
/// 语义一致）。**仅**供 `engine::push_compact_event` 与 `CascadeResult::
/// telemetry_snapshot` 使用，不改动级联判定链。
#[must_use]
pub fn compact_percent_freed(before_tokens: u32, after_tokens: u32) -> (i64, i64) {
    let freed = before_tokens.saturating_sub(after_tokens);
    let percent = if before_tokens == 0 {
        0
    } else {
        i64::from(freed) * 100 / i64::from(before_tokens)
    };
    (percent, i64::from(after_tokens))
}

/// u64 → f64（token 量级远低于 f64 精确整数上界；旧实现同为整型提升）。
#[expect(
    clippy::cast_precision_loss,
    reason = "token / 字符计数量级远低于 f64 精确整数上界（2^53），旧实现同为 long→double 提升"
)]
fn as_f64(value: u64) -> f64 {
    value as f64
}

/// 按比例缩放 token 阈值（旧 `(int)(threshold * ratio)` 的等价式）。
#[must_use]
fn scale_tokens(tokens: u32, ratio: f64) -> u32 {
    saturating_tokens(f64::from(tokens) * ratio)
}

/// Token 警告状态（逐字段对照旧 `ContextCascade.TokenWarningState`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenWarningState {
    /// 第一级：超过自动压缩阈值的 70%。
    pub above_warning_threshold: bool,
    /// 第二级：超过自动压缩阈值的 90%。
    pub above_error_threshold: bool,
    /// 第三级：超过自动压缩阈值 → 触发 L3 `AutoCompact`。
    pub above_auto_compact_threshold: bool,
    /// 当前估算 token 数。
    pub current_tokens: u32,
    /// 模型上下文窗口。
    pub context_window: u32,
    /// 自动压缩阈值。
    pub auto_compact_threshold: u32,
}

/// 计算 Token 警告状态（逐条对照旧
/// `ContextCascade.calculateTokenWarningState`）。
///
/// 算法：
/// 1. `effectiveWindow = contextWindow - contextWindow / 4`（预留 25% 摘要空间）；
/// 2. `bufferTokens = max(13_000, contextWindow × 0.10)`；
/// 3. `autoCompactThreshold = effectiveWindow - bufferTokens`。
///
/// # 与旧实现的一处偏离（缺陷修正，留痕）
///
/// 旧 `calculateTokenWarningState` 未对 `effectiveWindow` 设下限：小窗口模型
/// （如未知模型的 8192）会算出 `6144 - 13000 = -6856` 的负阈值，令
/// `currentTokens > threshold` 恒真、每轮无条件触发 L3。旧仓库的同族方法
/// `CompactService.shouldAutoCompactBufferBased`（L209-218）对此有
/// `minimumEffectiveWindow = AUTOCOMPACT_BUFFER_TOKENS × 2` 的钳制，本实现
/// 取该钳制以消除该退化路径——符合「仅在 token 超限时才压缩」的取向。
#[must_use]
pub fn calculate_token_warning_state(messages: &[ChatMessage], model: &str) -> TokenWarningState {
    let context_window = context_window_for(model);
    let reserved_for_summary = context_window / 4;
    let effective_window = context_window.saturating_sub(reserved_for_summary);
    let buffer_tokens = AUTOCOMPACT_BUFFER_TOKENS.max(scale_tokens(context_window, 0.10));
    // 下限钳制（见方法文档「与旧实现的一处偏离」）。
    let minimum_effective_window = AUTOCOMPACT_BUFFER_TOKENS.saturating_mul(2);
    let effective_window = effective_window.max(minimum_effective_window);
    let auto_compact_threshold = effective_window.saturating_sub(buffer_tokens);
    let current_tokens = estimate_tokens(messages, model);
    TokenWarningState {
        above_warning_threshold: current_tokens > scale_tokens(auto_compact_threshold, 0.7),
        above_error_threshold: current_tokens > scale_tokens(auto_compact_threshold, 0.9),
        above_auto_compact_threshold: current_tokens > auto_compact_threshold,
        current_tokens,
        context_window,
        auto_compact_threshold,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        calculate_token_warning_state, cascade_enabled, context_window_for, estimate_tokens,
        message_chars, saturating_tokens, token_char_ratio,
    };
    use zk_llm::{ChatMessage, ToolCallRequest};

    #[test]
    fn message_chars_matches_legacy_block_accounting() {
        // 纯文本：正文字符数（旧 TextBlock）。
        assert_eq!(message_chars(&ChatMessage::user("abcde")), 5);
        // 工具结果：正文 + 10（旧 ToolResultBlock）。
        assert_eq!(message_chars(&ChatMessage::tool("t1", "abcde")), 15);
        // 工具调用：正文 + 名称 + 入参 + 20（旧 ToolUseBlock）。
        let assistant = ChatMessage::assistant_tool_calls(
            "ab",
            vec![ToolCallRequest {
                id: "t1".into(),
                name: "Read".into(),
                arguments: "{}".into(),
            }],
        );
        assert_eq!(message_chars(&assistant), 2 + 4 + 2 + 20);
    }

    #[test]
    fn char_counting_uses_scalar_values_not_bytes() {
        // 中文按字符计数（旧 Java String.length() 在 BMP 与标量数一致）。
        assert_eq!(message_chars(&ChatMessage::user("中文三字")), 4);
    }

    #[test]
    fn estimate_tokens_matches_legacy_formula() {
        // kimi-k3 的 tokenCharRatio 为 3.5：ceil(700/3.5 + 2×4) = 208。
        let messages = vec![
            ChatMessage::user("x".repeat(350)),
            ChatMessage::assistant("y".repeat(350)),
        ];
        assert!((token_char_ratio("kimi-k3") - 3.5).abs() < f64::EPSILON);
        assert_eq!(estimate_tokens(&messages, "kimi-k3"), 208);
        // 空列表恒 0（旧 messages == null || isEmpty 分支）。
        assert_eq!(estimate_tokens(&[], "kimi-k3"), 0);
    }

    #[test]
    fn saturating_tokens_ceils_and_clamps() {
        assert_eq!(saturating_tokens(0.1), 1);
        assert_eq!(saturating_tokens(-5.0), 0);
        assert_eq!(saturating_tokens(f64::NAN), u32::MAX);
        assert_eq!(saturating_tokens(f64::INFINITY), u32::MAX);
    }

    #[test]
    fn warning_state_thresholds_follow_buffer_algorithm() {
        // kimi-k3 上下文窗口 1_000_000：effective = 1_000_000 - 250_000 = 750_000，
        // buffer = max(13000, 100_000) = 100_000 → threshold = 650_000。
        let state = calculate_token_warning_state(&[], "kimi-k3");
        assert_eq!(state.context_window, context_window_for("kimi-k3"));
        assert_eq!(state.auto_compact_threshold, 750_000 - 100_000);
        assert!(!state.above_auto_compact_threshold);
        assert!(!state.above_warning_threshold);
    }

    #[test]
    fn small_window_models_get_clamped_positive_threshold() {
        // 未知模型窗口 8192：旧实现算出负阈值恒触发；钳制后阈值 = 26000-13000。
        let state = calculate_token_warning_state(&[ChatMessage::user("hi")], "nope-9000");
        assert_eq!(state.auto_compact_threshold, 13_000);
        assert!(!state.above_auto_compact_threshold);
    }

    #[test]
    fn threshold_crossing_flips_flags_in_order() {
        // 构造超过阈值的消息量：70% / 90% / 100% 三级依次为真
        // （阈值 650_000 tokens ≈ 2.275M 字符，取 2.4M 稳超）。
        let big = ChatMessage::user("x".repeat(2_400_000));
        let state = calculate_token_warning_state(std::slice::from_ref(&big), "kimi-k3");
        assert!(state.above_warning_threshold);
        assert!(state.above_error_threshold);
        assert!(state.above_auto_compact_threshold);
    }

    #[test]
    fn cascade_enabled_defaults_to_true() {
        // 进程内未设置该环境变量时缺省开启（测试进程不设置）。
        assert!(cascade_enabled());
    }
}
