//! L3/L4/L5 压缩内核——三区划分 + 三级降级 + 关键消息选择。
//!
//! 建模来源：旧 `engine/CompactService.java`（`main@581d407b`）。逐条对照：
//!
//! | 旧成员 | 本模块 |
//! |---|---|
//! | `planCompaction` | [`plan_compaction`] |
//! | `findTurnBoundary` | [`find_turn_boundary`] |
//! | `shouldAutoCompactBufferBased` | [`should_auto_compact`] |
//! | `compact(msgs, cw, isReactive)` | [`compact_messages`] |
//! | `reactiveCompact` | [`reactive_compact`] |
//! | `fallbackKeyMessageSelection` | [`fallback_key_message_selection`] |
//! | `classifyPriority` | [`classify_priority`] |
//! | `ensureToolPairIntegrity` | [`ensure_tool_pair_integrity`] |
//! | `extractStructuredSummary` | [`extract_structured_summary`] |
//! | `validateSummaryQuality` | [`validate_summary_quality`] |
//! | `CompactHook` | 未移植（旧仅用于指标埋点，本域指标经 `tracing` 出） |
//!
//! # 压缩边界标记
//!
//! 旧 `Message.SystemMessage` 带 `SystemMessageType.COMPACT_SUMMARY` 枚举标签，
//! 三区划分靠该标签定位「冻结区」。[`zk_llm::ChatMessage`] 无类型标签，故本
//! 实现改用内容前缀 [`COMPACT_SUMMARY_MARKER`]（与旧边界文案字面量同源）作为
//! 等价标记——**LLM 摘要消息同样带该前缀**，否则下一轮无法识别冻结区。
//!
//! # Rust 侧硬化：工具配对修复
//!
//! 旧 Java 侧 `tool_result` 是 `UserMessage` 的一个内容块，压缩后孤立块只是
//! 语义损失。OpenAI 兼容协议下 [`zk_llm::Role::Tool`] 消息**必须**紧跟携带
//! 匹配 `tool_calls` 的 assistant 消息，否则 provider 直接 400。故本模块在每
//! 个压缩产物落地前统一过 [`repair_tool_pairs`]（旧无对应物，见其文档）。

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use zk_llm::{ChatMessage, Role};

use super::{estimate_message_tokens, estimate_tokens, scale_tokens};

/// 压缩边界标记（旧 `SystemMessageType.COMPACT_SUMMARY` 的内容前缀等价物；
/// 字面量取自旧 `CompactService` 关键消息选择分支的边界文案）。
pub const COMPACT_SUMMARY_MARKER: &str = "[对话历史已压缩]";

/// 压缩目标占上下文窗口比例（逐字对照旧 `COMPACT_TARGET_RATIO`）。
pub const COMPACT_TARGET_RATIO: f64 = 0.50;

/// 摘要 token 上限（逐字对照旧 `SUMMARY_MAX_TOKENS`）。
pub const SUMMARY_MAX_TOKENS: u32 = 4096;

/// 摘要 token 下限（逐字对照旧 `planCompaction` 的 `Math.max(.., 512)`）。
pub const MIN_SUMMARY_TOKENS: u32 = 512;

/// 常规压缩保留的最近轮次（逐字对照旧 `PRESERVED_RECENT_TURNS`）。
pub const PRESERVED_RECENT_TURNS: usize = 3;

/// 反应式压缩保留的最近轮次（逐字对照旧 `REACTIVE_PRESERVED_TURNS`）。
pub const REACTIVE_PRESERVED_TURNS: usize = 1;

/// 触发压缩的最低消息数（逐字对照旧 `MIN_MESSAGES_FOR_COMPACT`）。
pub const MIN_MESSAGES_FOR_COMPACT: usize = 5;

/// 输出预留 token（逐字对照旧 `MAX_OUTPUT_RESERVE`）。
pub const MAX_OUTPUT_RESERVE: u32 = 20_000;

/// 摘要质量校验的最短长度（逐字对照旧 `validateSummaryQuality` 的 `< 100`）。
const MIN_SUMMARY_CHARS: usize = 100;

/// 配对修复占位文案（Rust 侧硬化，旧无对应物）。
const DROPPED_TOOL_RESULT_PLACEHOLDER: &str = "[Tool result dropped during context compaction]";

/// 消息优先级（逐字段对照旧 `CompactService.MessagePriority`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessagePriority {
    /// 系统消息——恒保留。
    P0System,
    /// 携带工具调用的助手消息（旧 `P1_FILE_OPERATION`）。
    P1FileOperation,
    /// 含错误文本的工具结果（旧 `P2_ERROR_CONTEXT`）。
    P2ErrorContext,
    /// 用户意图消息（旧 `P3_USER_INTENT`）。
    P3UserIntent,
    /// 成功的工具结果（旧 `P4_TOOL_SUCCESS`）。
    P4ToolSuccess,
    /// 中间态助手文本（旧 `P5_INTERMEDIATE`）。
    P5Intermediate,
}

/// 消息优先级分类（逐条对照旧 `CompactService.classifyPriority`）。
#[must_use]
pub fn classify_priority(message: &ChatMessage) -> MessagePriority {
    match message.role {
        Role::System => MessagePriority::P0System,
        Role::Tool => {
            let body = &message.content;
            if body.contains("error")
                || body.contains("Error")
                || body.contains("failed")
                || body.contains("Failed")
            {
                MessagePriority::P2ErrorContext
            } else {
                MessagePriority::P4ToolSuccess
            }
        }
        Role::User => MessagePriority::P3UserIntent,
        Role::Assistant => {
            if message.tool_calls.is_empty() {
                MessagePriority::P5Intermediate
            } else {
                MessagePriority::P1FileOperation
            }
        }
    }
}

/// LLM 摘要端口（旧 `CompactService.generateLlmSummary` 的窄化反转）。
///
/// 旧实现直接调 `provider.chatSync(model, systemPrompt, prompt, 4096, null,
/// 90_000)`；Rust 侧 [`zk_llm::ChatProvider`] 只暴露流式 `chat_stream`，同步
/// 摘要需要额外的「收流成串 + 独立超时 + 快模型选路」编排，归后续子阶段。故
/// 本 trait 为占位端口，缺省实现 [`NoopSummarizer`] 恒返回 `None`，压缩直接
/// 走 Level 2（关键消息选择）/ Level 3（尾部截断）确定性降级路径。
pub trait Summarizer: Send + Sync {
    /// 生成摘要文本；返回 `None` 表示不可用（触发降级）。
    ///
    /// `target_tokens` 为期望摘要长度（旧 `plan.targetSummaryTokens()`）。
    fn summarize(&self, messages: &[ChatMessage], target_tokens: u32) -> Option<String>;
}

/// 不产出摘要的占位实现（缺省装配）。
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSummarizer;

impl Summarizer for NoopSummarizer {
    fn summarize(&self, _messages: &[ChatMessage], _target_tokens: u32) -> Option<String> {
        None
    }
}

/// 缺省摘要端口（`Arc<dyn Summarizer>` 的占位实例）。
#[must_use]
pub fn no_summarizer() -> Arc<dyn Summarizer> {
    Arc::new(NoopSummarizer)
}

/// 压缩计划三区（逐字段对照旧 `CompactService.CompactionPlan`）。
#[derive(Debug, Clone, Default)]
pub struct CompactionPlan {
    /// 冻结区：末个压缩边界（含）之前的消息，恒原样保留。
    pub frozen: Vec<ChatMessage>,
    /// 压缩区：待摘要 / 待筛选的消息。
    pub compaction: Vec<ChatMessage>,
    /// 保留区：最近 N 轮消息，恒原样保留。
    pub preserved: Vec<ChatMessage>,
    /// 压缩区 token 估算（旧 `compactionTokens`）。
    pub compaction_tokens: u32,
    /// 目标摘要 token（旧 `targetSummaryTokens`）。
    pub target_summary_tokens: u32,
}

/// 压缩落地层级（旧三级降级策略的显式化）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactLevel {
    /// Level 1：LLM 摘要（需装配 [`Summarizer`]）。
    LlmSummary,
    /// Level 2：关键消息选择。
    KeyMessageSelection,
    /// Level 3：尾部截断（最后手段）。
    TailTruncation,
}

/// 压缩成功结果（逐字段对照旧 `CompactService.CompactResult` 的成功分支）。
#[derive(Debug, Clone)]
pub struct CompactResult {
    /// 压缩后消息序列。
    pub messages: Vec<ChatMessage>,
    /// 压缩前 token 估算。
    pub before_tokens: u32,
    /// 压缩后 token 估算。
    pub after_tokens: u32,
    /// 被压缩掉的消息数。
    pub compacted_count: usize,
    /// 压缩率（`(before - after) / before`）。
    pub ratio: f64,
    /// 落地层级。
    pub level: CompactLevel,
}

impl CompactResult {
    /// 节省的 token 数。
    #[must_use]
    pub fn tokens_saved(&self) -> u32 {
        self.before_tokens.saturating_sub(self.after_tokens)
    }
}

/// 压缩未落地的原因（对照旧 `CompactResult.notNeeded()` /
/// `skipped("no_token_savings")` / `failed(n)` 三类非成功分支）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactSkip {
    /// 压缩区为空——无可压缩内容（旧 `notNeeded()`）。
    NotNeeded,
    /// 所有层级都未减少 token，保留原上下文（旧 `skipped("no_token_savings")`）。
    NoTokenSavings,
    /// 反应式压缩已尝试过，拒绝再次执行以防死亡螺旋（旧 `failed(1)`）。
    AlreadyAttempted,
}

/// 基于 buffer 的自动压缩阈值判定（逐条对照旧
/// `CompactService.shouldAutoCompactBufferBased`）。
///
/// 两道守卫（旧 L194-207）先于阈值计算：消息数 ≥ [`MIN_MESSAGES_FOR_COMPACT`]、
/// 且 user/assistant 消息数 ≥ 2（排除纯系统提示词上下文）。阈值算法与
/// [`super::calculate_token_warning_state`] 同源（含 `minimumEffectiveWindow`
/// 钳制）。
#[must_use]
pub fn should_auto_compact(messages: &[ChatMessage], model: &str) -> bool {
    if messages.len() < MIN_MESSAGES_FOR_COMPACT {
        return false;
    }
    let conversational = messages
        .iter()
        .filter(|m| matches!(m.role, Role::User | Role::Assistant | Role::Tool))
        .count();
    if conversational < 2 {
        return false;
    }
    super::calculate_token_warning_state(messages, model).above_auto_compact_threshold
}

/// 查找轮次边界——从末尾回溯 N 轮，返回保留区起点下标（逐条对照旧
/// `CompactService.findTurnBoundary`）。
///
/// 旧实现计 `instanceof Message.UserMessage`；该类型在旧模型里**同时**承载
/// 纯用户消息与工具结果，故本实现计 [`Role::User`] 与 [`Role::Tool`] 两者以
/// 保持结构等价（见 [`super`] 模块的消息模型映射表）。
#[must_use]
pub fn find_turn_boundary(messages: &[ChatMessage], turns: usize) -> usize {
    if turns == 0 {
        return messages.len();
    }
    let mut turn_count = 0_usize;
    for (index, message) in messages.iter().enumerate().rev() {
        if matches!(message.role, Role::User | Role::Tool) {
            turn_count += 1;
            if turn_count >= turns {
                return index;
            }
        }
    }
    0
}

/// 计算压缩计划三区边界（逐条对照旧 `CompactService.planCompaction`）。
#[must_use]
pub fn plan_compaction(
    messages: &[ChatMessage],
    model: &str,
    context_window: u32,
    preserve_turns: usize,
) -> CompactionPlan {
    // 1. 冻结区：末个压缩边界之后开始为可动区（旧 compactBoundaryIndex + 1）。
    let split_point = messages
        .iter()
        .rposition(is_compact_boundary)
        .map_or(0, |index| index + 1);
    let (frozen, remaining) = messages.split_at(split_point);

    // 2. 保留区：从可动区末尾取最近 N 轮。
    let preserve_start = find_turn_boundary(remaining, preserve_turns).min(remaining.len());
    let (compaction, preserved) = remaining.split_at(preserve_start);

    // 3. token 预算（旧 min(cw × 0.5 - frozen - preserved, 4096) 再钳到 512）。
    let frozen_tokens = estimate_tokens(frozen, model);
    let preserved_tokens = estimate_tokens(preserved, model);
    let budget = scale_tokens(context_window, COMPACT_TARGET_RATIO)
        .saturating_sub(frozen_tokens)
        .saturating_sub(preserved_tokens);
    let target_summary_tokens = budget.clamp(MIN_SUMMARY_TOKENS, SUMMARY_MAX_TOKENS);

    CompactionPlan {
        frozen: frozen.to_vec(),
        compaction: compaction.to_vec(),
        preserved: preserved.to_vec(),
        compaction_tokens: estimate_tokens(compaction, model),
        target_summary_tokens,
    }
}

/// 是否为压缩边界消息（[`COMPACT_SUMMARY_MARKER`] 前缀的 system 消息）。
fn is_compact_boundary(message: &ChatMessage) -> bool {
    message.role == Role::System && message.content.starts_with(COMPACT_SUMMARY_MARKER)
}

/// 执行压缩——三级降级（逐条对照旧 `CompactService.compact`）。
///
/// - Level 1：LLM 摘要（经 [`Summarizer`] 端口；缺省不可用）；
/// - Level 2：关键消息选择（[`fallback_key_message_selection`]）；
/// - Level 3：尾部截断（保留 `max(preserved.len(), total / 3)` 条）。
///
/// 每级都以「token 必须真的下降」为落地前置条件（旧 `afterTokens <
/// beforeTokens` 校验），否则继续降级。
///
/// # Errors
///
/// - [`CompactSkip::NotNeeded`]：压缩区为空；
/// - [`CompactSkip::NoTokenSavings`]：三级都未能减少 token。
pub fn compact_messages(
    messages: &[ChatMessage],
    model: &str,
    context_window: u32,
    is_reactive: bool,
    summarizer: &dyn Summarizer,
) -> Result<CompactResult, CompactSkip> {
    let preserve_turns = if is_reactive {
        REACTIVE_PRESERVED_TURNS
    } else {
        PRESERVED_RECENT_TURNS
    };
    let plan = plan_compaction(messages, model, context_window, preserve_turns);
    if plan.compaction.is_empty() {
        return Err(CompactSkip::NotNeeded);
    }
    let plan = ensure_tool_pair_integrity(plan, model);
    let before_tokens = estimate_tokens(messages, model);

    if let Some(result) = try_llm_summary(&plan, model, before_tokens, summarizer) {
        return Ok(result);
    }
    if let Some(result) = try_key_message_selection(&plan, model, context_window, before_tokens) {
        return Ok(result);
    }
    tail_truncation(messages, &plan, model, before_tokens)
}

/// 反应式压缩——413 恢复用紧急模式（逐条对照旧
/// `CompactService.reactiveCompact`）。
///
/// # Errors
///
/// 除 [`compact_messages`] 的两类原因外，`has_attempted == true` 时直接返回
/// [`CompactSkip::AlreadyAttempted`]（旧 `failed(1)`，防死亡螺旋）。
pub fn reactive_compact(
    messages: &[ChatMessage],
    model: &str,
    context_window: u32,
    has_attempted: bool,
    summarizer: &dyn Summarizer,
) -> Result<CompactResult, CompactSkip> {
    if has_attempted {
        return Err(CompactSkip::AlreadyAttempted);
    }
    compact_messages(messages, model, context_window, true, summarizer)
}

/// Level 1：LLM 摘要（旧 `generateLlmSummary` + `extractStructuredSummary` +
/// `validateSummaryQuality` + `buildCompactResultWithSummary` 的合成）。
fn try_llm_summary(
    plan: &CompactionPlan,
    model: &str,
    before_tokens: u32,
    summarizer: &dyn Summarizer,
) -> Option<CompactResult> {
    let raw = summarizer.summarize(&plan.compaction, plan.target_summary_tokens)?;
    let summary = extract_structured_summary(&raw);
    if !validate_summary_quality(&summary, &plan.compaction) {
        tracing::warn!("LLM 摘要质量不足，降级到关键消息选择");
        return None;
    }
    let mut candidate = plan.frozen.clone();
    candidate.push(ChatMessage::system(format!(
        "{COMPACT_SUMMARY_MARKER}\n{summary}"
    )));
    candidate.extend(plan.preserved.iter().cloned());
    finish(
        candidate,
        model,
        before_tokens,
        plan.compaction.len(),
        CompactLevel::LlmSummary,
    )
}

/// Level 2：关键消息选择（旧 `compact` 的 Level 2 分支）。
fn try_key_message_selection(
    plan: &CompactionPlan,
    model: &str,
    context_window: u32,
    before_tokens: u32,
) -> Option<CompactResult> {
    let token_budget = scale_tokens(context_window, COMPACT_TARGET_RATIO);
    let selected = fallback_key_message_selection(&plan.compaction, model, token_budget);
    if selected.is_empty() {
        return None;
    }
    let mut candidate = plan.frozen.clone();
    candidate.push(ChatMessage::system(format!(
        "{COMPACT_SUMMARY_MARKER} 保留 {}/{} 条关键消息",
        selected.len(),
        plan.compaction.len()
    )));
    let dropped = plan.compaction.len().saturating_sub(selected.len());
    candidate.extend(selected);
    candidate.extend(plan.preserved.iter().cloned());
    finish(
        candidate,
        model,
        before_tokens,
        dropped,
        CompactLevel::KeyMessageSelection,
    )
}

/// Level 3：尾部截断（旧 `compact` 的最后手段分支）。
fn tail_truncation(
    messages: &[ChatMessage],
    plan: &CompactionPlan,
    model: &str,
    before_tokens: u32,
) -> Result<CompactResult, CompactSkip> {
    tracing::warn!("降级策略: 尾部截断——保留最近消息，丢弃最早消息");
    let keep_count = plan
        .preserved
        .len()
        .max(messages.len() / 3)
        .min(messages.len());
    let truncated = messages[messages.len() - keep_count..].to_vec();
    finish(
        truncated,
        model,
        before_tokens,
        messages.len() - keep_count,
        CompactLevel::TailTruncation,
    )
    .ok_or(CompactSkip::NoTokenSavings)
}

/// 候选落地：配对修复 → token 下降校验 → 成功结果（旧各级共有的收尾）。
fn finish(
    candidate: Vec<ChatMessage>,
    model: &str,
    before_tokens: u32,
    compacted_count: usize,
    level: CompactLevel,
) -> Option<CompactResult> {
    let messages = repair_tool_pairs(candidate);
    let after_tokens = estimate_tokens(&messages, model);
    if after_tokens >= before_tokens {
        tracing::warn!(
            before_tokens,
            after_tokens,
            ?level,
            "压缩候选未减少 token，拒绝应用并降级"
        );
        return None;
    }
    let ratio = if before_tokens > 0 {
        f64::from(before_tokens - after_tokens) / f64::from(before_tokens)
    } else {
        0.0
    };
    Some(CompactResult {
        messages,
        before_tokens,
        after_tokens,
        compacted_count,
        ratio,
        level,
    })
}

/// 结构化摘要解析（逐条对照旧 `CompactService.extractStructuredSummary`）。
#[must_use]
pub fn extract_structured_summary(raw: &str) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }
    if let Some(start) = raw.find("<summary>")
        && let Some(end) = raw.find("</summary>")
    {
        let body_start = start + "<summary>".len();
        if end > body_start {
            return raw[body_start..end].trim().to_owned();
        }
    }
    if let Some(end) = raw.find("</analysis>") {
        return raw[end + "</analysis>".len()..].trim().to_owned();
    }
    raw.trim().to_owned()
}

/// 摘要质量校验（逐条对照旧 `CompactService.validateSummaryQuality`）。
///
/// 长度 ≥ 100 字符；若原始消息含文件/命令类工具调用（旧
/// `name().contains("File") || name().contains("Bash")`）则摘要必须至少有一行
/// 同时含 `/` 与常见源码后缀，否则判定为丢失关键路径信息。
#[must_use]
pub fn validate_summary_quality(summary: &str, original: &[ChatMessage]) -> bool {
    if summary.chars().count() < MIN_SUMMARY_CHARS {
        return false;
    }
    let file_path_lines = summary
        .lines()
        .filter(|line| {
            line.contains('/')
                && (line.contains(".java")
                    || line.contains(".ts")
                    || line.contains(".py")
                    || line.contains(".md"))
        })
        .count();
    let has_file_ops = original.iter().any(|message| {
        message
            .tool_calls
            .iter()
            .any(|call| call.name.contains("File") || call.name.contains("Bash"))
    });
    if has_file_ops && file_path_lines == 0 {
        tracing::warn!("摘要质量不足：原始消息包含文件操作但摘要中无文件路径");
        return false;
    }
    true
}

/// 关键消息选择——按优先级填充直到 token 预算耗尽（逐条对照旧
/// `CompactService.fallbackKeyMessageSelection`）。
///
/// 三阶段（旧 L486-563）：
/// 1. 无条件保留全部 system 消息；
/// 2. 识别工具调用对（assistant 带 `tool_calls` + 紧随的 [`Role::Tool`] 结果），
///    从**最近**的一对开始填充，成对进出以免破坏配对；
/// 3. 先填最近的纯用户消息，再填其余剩余消息。
///
/// 最终按**原始下标**升序输出。旧实现用 `selected.sort(comparingInt(
/// originalOrder::indexOf))`，`indexOf` 对内容相等的重复消息会给出同一下标而
/// 打乱顺序；本实现全程以下标集合（[`BTreeSet`]）为载体，天然稳定——这是一处
/// 行为更强的等价替换。
#[must_use]
pub fn fallback_key_message_selection(
    messages: &[ChatMessage],
    model: &str,
    token_budget: u32,
) -> Vec<ChatMessage> {
    let mut selected: BTreeSet<usize> = BTreeSet::new();

    // Phase 1：system 消息无条件保留。
    for (index, message) in messages.iter().enumerate() {
        if message.role == Role::System {
            selected.insert(index);
        }
    }
    let mut used = subset_tokens(messages, &selected, model);

    // Phase 2：工具调用对，从最近一对起填充。
    let mut pairs: Vec<(usize, usize)> = Vec::new();
    let mut paired: HashSet<usize> = HashSet::new();
    for (index, message) in messages.iter().enumerate() {
        if message.role != Role::Assistant || message.tool_calls.is_empty() {
            continue;
        }
        if messages
            .get(index + 1)
            .is_some_and(|m| m.role == Role::Tool)
        {
            pairs.push((index, index + 1));
            paired.insert(index);
            paired.insert(index + 1);
        }
    }
    for &(head, tail) in pairs.iter().rev() {
        let pair = [messages[head].clone(), messages[tail].clone()];
        let pair_tokens = estimate_tokens(&pair, model);
        if used.saturating_add(pair_tokens) <= token_budget {
            selected.insert(head);
            selected.insert(tail);
            used += pair_tokens;
        }
    }

    // Phase 3：剩余消息（先最近的纯用户消息，再其余）。
    let remaining: Vec<usize> = (0..messages.len())
        .filter(|index| !paired.contains(index) && messages[*index].role != Role::System)
        .collect();
    let mut consumed: HashSet<usize> = HashSet::new();
    for &index in remaining.iter().rev() {
        if messages[index].role != Role::User {
            continue;
        }
        let tokens = estimate_message_tokens(&messages[index], model);
        if used.saturating_add(tokens) <= token_budget {
            selected.insert(index);
            used += tokens;
            consumed.insert(index);
        }
    }
    for &index in remaining.iter().rev() {
        if consumed.contains(&index) {
            continue;
        }
        let tokens = estimate_message_tokens(&messages[index], model);
        if used.saturating_add(tokens) <= token_budget {
            selected.insert(index);
            used += tokens;
        }
    }

    selected
        .into_iter()
        .filter_map(|index| messages.get(index).cloned())
        .collect()
}

/// 下标子集的 token 估算。
fn subset_tokens(messages: &[ChatMessage], indices: &BTreeSet<usize>, model: &str) -> u32 {
    let subset: Vec<ChatMessage> = indices
        .iter()
        .filter_map(|index| messages.get(*index).cloned())
        .collect();
    estimate_tokens(&subset, model)
}

/// 工具配对完整性保证——把跨区孤立的保留区消息下沉进压缩区（逐条对照旧
/// `CompactService.ensureToolPairIntegrity`）。
///
/// 孤立判定：保留区里的工具结果其调用落在压缩区（`tool_call_id` ∈ 压缩区
/// `tool_calls` 的 id 集），或保留区里的工具调用其结果落在压缩区。
#[must_use]
pub fn ensure_tool_pair_integrity(plan: CompactionPlan, model: &str) -> CompactionPlan {
    let mut tool_use_ids: HashSet<&str> = HashSet::new();
    let mut tool_result_ids: HashSet<&str> = HashSet::new();
    for message in &plan.compaction {
        for call in &message.tool_calls {
            tool_use_ids.insert(call.id.as_str());
        }
        if let Some(id) = &message.tool_call_id {
            tool_result_ids.insert(id.as_str());
        }
    }

    let mut orphans: HashSet<String> = HashSet::new();
    for message in &plan.preserved {
        if let Some(id) = &message.tool_call_id
            && tool_use_ids.contains(id.as_str())
        {
            orphans.insert(id.clone());
        }
        for call in &message.tool_calls {
            if tool_result_ids.contains(call.id.as_str()) {
                orphans.insert(call.id.clone());
            }
        }
    }
    if orphans.is_empty() {
        return plan;
    }
    tracing::info!(
        orphans = orphans.len(),
        "检测到跨区孤立工具配对，调整压缩边界"
    );

    let CompactionPlan {
        frozen,
        mut compaction,
        preserved,
        target_summary_tokens,
        ..
    } = plan;
    let mut kept: Vec<ChatMessage> = Vec::with_capacity(preserved.len());
    for message in preserved {
        let is_orphan = message
            .tool_call_id
            .as_ref()
            .is_some_and(|id| orphans.contains(id))
            || message.tool_calls.iter().any(|c| orphans.contains(&c.id));
        if is_orphan {
            compaction.push(message);
        } else {
            kept.push(message);
        }
    }
    let compaction_tokens = estimate_tokens(&compaction, model);
    CompactionPlan {
        frozen,
        compaction,
        preserved: kept,
        compaction_tokens,
        target_summary_tokens,
    }
}

/// 工具配对修复（**Rust 侧硬化，旧无对应物**）。
///
/// `OpenAI` 兼容协议要求：每条 [`Role::Tool`] 消息必须能在其之前找到携带匹配
/// `tool_calls` 项的 assistant 消息；反之 assistant 声明的每个 `tool_calls`
/// 项都必须有对应的工具结果。压缩/折叠可能切断任一侧，届时 provider 返回
/// 400 而非 413，恢复链路无从下手。故：
///
/// 1. 丢弃找不到调用方的孤立工具结果；
/// 2. 为缺失结果的调用补一条占位工具结果（保留调用语义，避免整条 assistant
///    被丢弃而损失推理链）。
#[must_use]
pub fn repair_tool_pairs(messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
    let mut announced: HashSet<String> = HashSet::new();
    let mut kept: Vec<ChatMessage> = Vec::with_capacity(messages.len());
    for message in messages {
        if message.role == Role::Tool {
            let matched = message
                .tool_call_id
                .as_ref()
                .is_some_and(|id| announced.contains(id));
            if matched {
                kept.push(message);
            }
            continue;
        }
        for call in &message.tool_calls {
            announced.insert(call.id.clone());
        }
        kept.push(message);
    }

    let satisfied: HashSet<String> = kept
        .iter()
        .filter(|m| m.role == Role::Tool)
        .filter_map(|m| m.tool_call_id.clone())
        .collect();
    let mut repaired: Vec<ChatMessage> = Vec::with_capacity(kept.len());
    for message in kept {
        let missing: Vec<String> = message
            .tool_calls
            .iter()
            .filter(|call| !satisfied.contains(&call.id))
            .map(|call| call.id.clone())
            .collect();
        repaired.push(message);
        for id in missing {
            repaired.push(ChatMessage::tool(id, DROPPED_TOOL_RESULT_PLACEHOLDER));
        }
    }
    repaired
}

#[cfg(test)]
mod tests {
    use super::{
        COMPACT_SUMMARY_MARKER, CompactLevel, CompactSkip, CompactionPlan, MessagePriority,
        NoopSummarizer, Summarizer, classify_priority, compact_messages,
        ensure_tool_pair_integrity, extract_structured_summary, fallback_key_message_selection,
        find_turn_boundary, plan_compaction, reactive_compact, repair_tool_pairs,
        should_auto_compact, validate_summary_quality,
    };
    use zk_llm::{ChatMessage, Role, ToolCallRequest};

    const MODEL: &str = "kimi-k3";

    fn call(id: &str) -> ToolCallRequest {
        ToolCallRequest {
            id: id.to_owned(),
            name: "Read".to_owned(),
            arguments: "{}".to_owned(),
        }
    }

    /// 长会话：system + 若干 user/assistant 轮。
    fn long_history(turns: usize) -> Vec<ChatMessage> {
        let mut messages = vec![ChatMessage::system("sys")];
        for index in 0..turns {
            messages.push(ChatMessage::user(format!(
                "user-{index} {}",
                "u".repeat(200)
            )));
            messages.push(ChatMessage::assistant(format!(
                "assistant-{index} {}",
                "a".repeat(200)
            )));
        }
        messages
    }

    #[test]
    fn turn_boundary_counts_user_and_tool_messages() {
        // 旧 findTurnBoundary 计 UserMessage（含工具结果）；本实现计 User|Tool。
        let messages = vec![
            ChatMessage::user("u1"),
            ChatMessage::assistant("a1"),
            ChatMessage::tool("t1", "r1"),
            ChatMessage::assistant("a2"),
            ChatMessage::user("u2"),
        ];
        assert_eq!(find_turn_boundary(&messages, 1), 4);
        assert_eq!(find_turn_boundary(&messages, 2), 2);
        assert_eq!(find_turn_boundary(&messages, 3), 0);
        // 轮次多于实际 → 0（旧兜底 return 0）。
        assert_eq!(find_turn_boundary(&messages, 99), 0);
    }

    #[test]
    fn plan_splits_frozen_compaction_preserved() {
        let mut messages = long_history(4);
        messages.insert(3, ChatMessage::system(COMPACT_SUMMARY_MARKER));
        let plan = plan_compaction(&messages, MODEL, 200_000, 1);
        // 冻结区 = 边界（含）之前全部。
        assert_eq!(plan.frozen.len(), 4);
        // 保留区自末个 user 起。
        assert!(plan.preserved.first().is_some_and(|m| m.role == Role::User));
        assert!(!plan.compaction.is_empty());
        assert!(plan.target_summary_tokens >= super::MIN_SUMMARY_TOKENS);
    }

    #[test]
    fn plan_target_summary_tokens_clamped_to_bounds() {
        // 窗口极小 → 预算被下限 512 钳制；窗口极大 → 上限 4096。
        let messages = long_history(6);
        assert_eq!(
            plan_compaction(&messages, MODEL, 64, 1).target_summary_tokens,
            super::MIN_SUMMARY_TOKENS
        );
        assert_eq!(
            plan_compaction(&messages, MODEL, 4_000_000, 1).target_summary_tokens,
            super::SUMMARY_MAX_TOKENS
        );
    }

    #[test]
    fn auto_compact_guards_reject_short_and_system_only_history() {
        // 守卫 1：消息数 < 5。
        assert!(!should_auto_compact(&long_history(1), MODEL));
        // 守卫 2：纯 system 上下文。
        let system_only = vec![
            ChatMessage::system("a"),
            ChatMessage::system("b"),
            ChatMessage::system("c"),
            ChatMessage::system("d"),
            ChatMessage::system("e"),
        ];
        assert!(!should_auto_compact(&system_only, MODEL));
        // 未超阈值的正常会话同样不触发（「仅在 token 超限时才压缩」）。
        assert!(!should_auto_compact(&long_history(10), MODEL));
    }

    #[test]
    fn auto_compact_triggers_above_threshold() {
        let mut messages = long_history(3);
        // 阈值 650_000 tokens（kimi-k3 1M 窗口）≈ 2.275M 字符，取 2.4M 稳超。
        messages.push(ChatMessage::user("x".repeat(2_400_000)));
        assert!(should_auto_compact(&messages, MODEL));
    }

    #[test]
    fn compact_falls_back_to_key_message_selection() {
        // 缺省 Summarizer 不产摘要 → 落 Level 2。
        let messages = long_history(20);
        let result =
            compact_messages(&messages, MODEL, 4_096, false, &NoopSummarizer).expect("压缩应落地");
        assert_eq!(result.level, CompactLevel::KeyMessageSelection);
        assert!(result.after_tokens < result.before_tokens);
        assert!(result.tokens_saved() > 0);
        // 边界标记出现且 system 消息被保留。
        assert!(
            result
                .messages
                .iter()
                .any(|m| m.content.starts_with(COMPACT_SUMMARY_MARKER))
        );
    }

    #[test]
    fn compact_reports_not_needed_when_nothing_compactable() {
        // 保留区吃掉全部可动区 → 压缩区为空。
        let messages = vec![ChatMessage::user("hi"), ChatMessage::assistant("yo")];
        assert_eq!(
            compact_messages(&messages, MODEL, 200_000, false, &NoopSummarizer).unwrap_err(),
            CompactSkip::NotNeeded
        );
    }

    #[test]
    fn reactive_compact_refuses_second_attempt() {
        let messages = long_history(20);
        assert_eq!(
            reactive_compact(&messages, MODEL, 4_096, true, &NoopSummarizer).unwrap_err(),
            CompactSkip::AlreadyAttempted
        );
        assert!(reactive_compact(&messages, MODEL, 4_096, false, &NoopSummarizer).is_ok());
    }

    #[test]
    fn llm_summary_level_applies_when_summarizer_available() {
        struct Fixed;
        impl Summarizer for Fixed {
            fn summarize(&self, _messages: &[ChatMessage], _target: u32) -> Option<String> {
                Some("summary ".repeat(20))
            }
        }
        let messages = long_history(20);
        let result =
            compact_messages(&messages, MODEL, 200_000, false, &Fixed).expect("压缩应落地");
        assert_eq!(result.level, CompactLevel::LlmSummary);
        // 摘要消息带压缩边界前缀，供下一轮识别冻结区。
        assert!(
            result
                .messages
                .iter()
                .any(|m| m.role == Role::System && m.content.starts_with(COMPACT_SUMMARY_MARKER))
        );
    }

    #[test]
    fn low_quality_summary_is_rejected_and_degrades() {
        struct TooShort;
        impl Summarizer for TooShort {
            fn summarize(&self, _messages: &[ChatMessage], _target: u32) -> Option<String> {
                Some("too short".to_owned())
            }
        }
        let messages = long_history(20);
        let result =
            compact_messages(&messages, MODEL, 4_096, false, &TooShort).expect("压缩应落地");
        assert_eq!(result.level, CompactLevel::KeyMessageSelection);
    }

    #[test]
    fn structured_summary_extraction_handles_both_envelopes() {
        assert_eq!(
            extract_structured_summary("noise<summary> body </summary>tail"),
            "body"
        );
        assert_eq!(
            extract_structured_summary("<analysis>x</analysis>\n  body  "),
            "body"
        );
        assert_eq!(extract_structured_summary("  plain  "), "plain");
        assert_eq!(extract_structured_summary("   "), "");
    }

    #[test]
    fn summary_quality_requires_file_paths_when_history_touched_files() {
        let history = vec![ChatMessage::assistant_tool_calls(
            "",
            vec![ToolCallRequest {
                id: "t1".to_owned(),
                name: "ReadFile".to_owned(),
                arguments: "{}".to_owned(),
            }],
        )];
        let long = "x".repeat(200);
        assert!(!validate_summary_quality(&long, &history));
        let with_path = format!("{long}\nsrc/main/java/App.java 已修改");
        assert!(validate_summary_quality(&with_path, &history));
        // 无文件操作时长度达标即可。
        assert!(validate_summary_quality(&long, &[]));
        assert!(!validate_summary_quality("短", &[]));
    }

    #[test]
    fn priority_classification_matches_legacy_mapping() {
        assert_eq!(
            classify_priority(&ChatMessage::system("s")),
            MessagePriority::P0System
        );
        assert_eq!(
            classify_priority(&ChatMessage::assistant_tool_calls("", vec![call("t1")])),
            MessagePriority::P1FileOperation
        );
        assert_eq!(
            classify_priority(&ChatMessage::tool("t1", "Error: boom")),
            MessagePriority::P2ErrorContext
        );
        assert_eq!(
            classify_priority(&ChatMessage::user("u")),
            MessagePriority::P3UserIntent
        );
        assert_eq!(
            classify_priority(&ChatMessage::tool("t1", "ok")),
            MessagePriority::P4ToolSuccess
        );
        assert_eq!(
            classify_priority(&ChatMessage::assistant("a")),
            MessagePriority::P5Intermediate
        );
    }

    #[test]
    fn key_selection_keeps_system_and_pairs_and_original_order() {
        let messages = vec![
            ChatMessage::system("sys"),
            ChatMessage::user("u1"),
            ChatMessage::assistant_tool_calls("", vec![call("t1")]),
            ChatMessage::tool("t1", "r1"),
            ChatMessage::user("u2"),
        ];
        let selected = fallback_key_message_selection(&messages, MODEL, 10_000);
        assert_eq!(selected, messages);
        // 预算仅够 system → 只剩 system。
        let tight = fallback_key_message_selection(&messages, MODEL, 5);
        assert_eq!(tight.len(), 1);
        assert_eq!(tight[0].role, Role::System);
    }

    #[test]
    fn tool_pair_integrity_moves_orphan_preserved_into_compaction() {
        let plan = CompactionPlan {
            frozen: Vec::new(),
            compaction: vec![ChatMessage::assistant_tool_calls("", vec![call("t1")])],
            preserved: vec![ChatMessage::tool("t1", "r1"), ChatMessage::user("u")],
            compaction_tokens: 0,
            target_summary_tokens: 512,
        };
        let fixed = ensure_tool_pair_integrity(plan, MODEL);
        assert_eq!(fixed.compaction.len(), 2);
        assert_eq!(fixed.preserved.len(), 1);
        assert_eq!(fixed.preserved[0].role, Role::User);
        assert!(fixed.compaction_tokens > 0);
    }

    #[test]
    fn tool_pair_integrity_is_identity_without_orphans() {
        let plan = CompactionPlan {
            frozen: Vec::new(),
            compaction: vec![ChatMessage::user("u1")],
            preserved: vec![ChatMessage::user("u2")],
            compaction_tokens: 7,
            target_summary_tokens: 512,
        };
        let fixed = ensure_tool_pair_integrity(plan, MODEL);
        assert_eq!(fixed.compaction_tokens, 7);
        assert_eq!(fixed.preserved.len(), 1);
    }

    #[test]
    fn repair_drops_orphan_results_and_backfills_missing_ones() {
        let repaired = repair_tool_pairs(vec![
            ChatMessage::tool("gone", "orphan result"),
            ChatMessage::assistant_tool_calls("", vec![call("t1"), call("t2")]),
            ChatMessage::tool("t1", "r1"),
        ]);
        // 孤立结果被丢弃；t2 补占位。
        assert_eq!(repaired.len(), 3);
        assert_eq!(repaired[0].role, Role::Assistant);
        assert_eq!(repaired[1].tool_call_id.as_deref(), Some("t2"));
        assert!(repaired[1].content.contains("dropped"));
        assert_eq!(repaired[2].tool_call_id.as_deref(), Some("t1"));
    }

    #[test]
    fn repair_is_identity_for_well_formed_history() {
        let messages = vec![
            ChatMessage::system("s"),
            ChatMessage::assistant_tool_calls("", vec![call("t1")]),
            ChatMessage::tool("t1", "r1"),
            ChatMessage::assistant("done"),
        ];
        assert_eq!(repair_tool_pairs(messages.clone()), messages);
    }
}
