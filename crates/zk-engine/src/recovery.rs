//! 413 三阶段上下文恢复 + 模型层级冷却（对照旧
//! `QueryEngine` 的 `isContextLimitError` 413 分支、`PromptTooLongRecovery`、
//! `ReactiveStripStrategy`、`ModelTierService`）。
//!
//! # 三阶段（对照 `QueryEngine.queryLoop` L862-913）
//!
//! - **Phase 1 `CollapseDrain`**：半窗口（`context_window / 2`）激进压缩，
//!   受 `last_transition_reason != "collapse_drain_retry"` 守卫防重复。
//! - **Phase 2 `ReactiveCompact`**：反应式压缩，受单次 `has_attempted_reactive`
//!   守卫防死亡螺旋。
//! - **Phase 3 `MediaRecovery`**：仅当 [`is_media_related_error`] 为真时摘除媒体
//!   载荷（旧剥离 `ImageBlock`）。
//!
//! # 消息模型差异留痕
//!
//! 旧 `ReactiveStripStrategy` / `tryStripMediaBlocks` 操作结构化 `ContentBlock`
//! （`ImageBlock` / `TextBlock`）。Rust 侧 [`ChatMessage`] 为纯文本，媒体以内嵌
//! `data:<mime>;base64,<payload>` 形式出现，故 Phase 3 适配为剥离 data URI 载荷；
//! 若某 [`Role::User`] 消息去除后正文为空则替换为占位文案（对照旧 `filteredBlocks
//! .isEmpty()` 分支）。[`strip_largest_block`] 为 `ReactiveStripStrategy
//! .stripLargestBlock` 的等价移植（旧仓库为独立策略类，未接入 `QueryEngine` 的
//! 413 主分支，此处同样仅作独立工具函数导出）。
//!
//! # Feature Flag
//!
//! 调用方（`engine`）在进入恢复分支前应先检查 [`crate::context::cascade_enabled`]；
//! 关闭时 413 直接透传报错（无恢复），与改动前行为一致。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use zk_llm::{ChatMessage, Role};

use crate::context::compact::{Summarizer, compact_messages_scoped, no_summarizer};
use crate::context::estimate_tokens;
use crate::llm_summarizer::SummaryExecution;

/// 默认冷却时长（对照旧 `DEFAULT_COOLDOWN = Duration.ofMinutes(30)`）。
pub const DEFAULT_COOLDOWN: Duration = Duration::from_mins(30);
/// 短期重试阈值——低于此值不触发冷却（旧 `SHORT_RETRY_THRESHOLD = 20s`）。
pub const SHORT_RETRY_THRESHOLD: Duration = Duration::from_secs(20);
/// 最短冷却时长（旧 `MIN_COOLDOWN = Duration.ofMinutes(10)`）。
pub const MIN_COOLDOWN: Duration = Duration::from_mins(10);
/// 连续成功恢复阈值（旧 `RECOVERY_SUCCESS_THRESHOLD = 3`）。
pub const RECOVERY_SUCCESS_THRESHOLD: u32 = 3;

/// 媒体摘除后的占位文案（逐字对照旧 `tryStripMediaBlocks` 空块兜底）。
pub const MEDIA_PLACEHOLDER: &str = "[Media content was present but removed to reduce context size. The original content included image(s) that exceeded size limits.]";

// ============ 错误分类 ============

/// 判断错误是否为上下文超限（413）类型（逐条对照旧
/// `QueryEngine.isContextLimitError`：`status == 413` 或错误文案命中五个子串）。
#[must_use]
pub fn is_context_limit_error(status: Option<u16>, message: &str) -> bool {
    if status == Some(413) {
        return true;
    }
    let lower = message.to_ascii_lowercase();
    lower.contains("prompt_too_long")
        || lower.contains("prompt is too long")
        || lower.contains("context length")
        || lower.contains("maximum context")
        || lower.contains("token limit")
        || lower.contains("context_budget_exceeded")
        || lower.contains("payload_too_large")
}

/// 判断错误是否与媒体相关（逐条对照旧 `QueryEngine.isMediaRelatedError`）。
#[must_use]
pub fn is_media_related_error(message: &str) -> bool {
    let msg = message.to_ascii_lowercase();
    (msg.contains("image") && (msg.contains("invalid") || msg.contains("too_large")))
        || msg.contains("file_too_large")
        || msg.contains("invalid_image")
        || msg.contains("could not process image")
        || msg.contains("media_type_not_supported")
}

// ============ 恢复结果 ============

/// 三阶段恢复中实际生效的阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPhase {
    /// Phase 1：半窗口激进压缩。
    CollapseDrain,
    /// Phase 2：反应式压缩（单次）。
    ReactiveCompact,
    /// Phase 3：媒体载荷摘除。
    MediaRecovery,
}

impl RecoveryPhase {
    /// 遥测事件 `phase` 字段名（`compact_event.phase`，Batch 0 Step 0-6）。
    ///
    /// 命名对齐旧 `QueryEngine.tryContextCollapseDrain` / `tryReactiveCompact`
    /// 的 `handler.onCompactEvent(type, ...)` 传入的 `type` 字符串：
    /// `context_collapse_drain` / `reactive_compact` / `media_strip`
    /// （`QueryEngine.java` L1594 / L1626 / L1873）。
    #[must_use]
    pub fn telemetry_name(self) -> &'static str {
        match self {
            Self::CollapseDrain => "context_collapse_drain",
            Self::ReactiveCompact => "reactive_compact",
            Self::MediaRecovery => "media_strip",
        }
    }
}

/// 单次恢复的产出。
#[derive(Debug, Clone)]
pub enum RecoveryOutcome {
    /// 恢复成功——携带压缩后的消息与生效阶段。
    Recovered {
        /// 恢复后的完整消息序列。
        messages: Vec<ChatMessage>,
        /// 实际生效的阶段。
        phase: RecoveryPhase,
        /// 恢复前估算 token。
        before_tokens: u32,
        /// 恢复后估算 token。
        after_tokens: u32,
    },
    /// 所有策略耗尽——调用方应释放扣留错误并终止循环。
    Exhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LocalRecoveryKind {
    Image,
    Payload,
}

/// 跨轮次持久化的恢复簿记（对照旧 `QueryLoopState` 的三个 413 相关字段）。
#[derive(Debug, Default, Clone)]
pub struct RecoveryState {
    /// Tool-summary compression is assessed at the next physical request too.
    pub(crate) pending_compaction_before_tokens: Option<u32>,
    /// Successful provider turns confirm only tool image sources actually dispatched.
    pub(crate) confirmed_image_sources: std::collections::HashSet<String>,
    /// At most one local retry per cause, independent of remote recovery stages.
    local_attempts: std::collections::HashSet<LocalRecoveryKind>,
    /// A notice is durably published once per Run and reason.
    pub(crate) image_notice_keys: std::collections::HashSet<String>,
    /// The pre-request compaction and its recovery retries share one summary attempt.
    pub(crate) summary_attempt: Arc<std::sync::atomic::AtomicBool>,
    /// One supplementary answer per continuous empty/placeholder response streak.
    pub final_answer_recovery_attempted: bool,
    /// 上一次状态转移原因（防止 Phase 1 重复触发，旧 `lastTransitionReason`）。
    pub last_transition_reason: Option<String>,
    /// 是否已尝试过反应式压缩（旧 `hasAttemptedReactiveCompact` 单次 guard）。
    pub has_attempted_reactive: bool,
    /// 恢复是否已耗尽（旧 `recoveryExhausted`）。
    pub recovery_exhausted: bool,
}

// ============ 三阶段恢复编排器 ============

/// 413 三阶段恢复编排器。
pub struct ContextRecovery {
    summarizer: Arc<dyn Summarizer>,
}

impl std::fmt::Debug for ContextRecovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextRecovery").finish_non_exhaustive()
    }
}

impl Default for ContextRecovery {
    fn default() -> Self {
        Self::new()
    }
}

impl ContextRecovery {
    /// Prepare a request copy, with one local history compaction before an image budget failure.
    pub(crate) async fn prepare_images(
        &self,
        request: &mut zk_llm::ChatRequest,
        state: &mut RecoveryState,
        execution: &SummaryExecution,
    ) -> Result<bool, String> {
        suppress_confirmed_tool_images(&mut request.messages, &state.confirmed_image_sources);
        let before = image_count(&request.messages);
        if let Err(error) = crate::context::image_budget::enforce_request(request).await {
            if !error.contains("IMAGE_BUDGET_EXCEEDED")
                || state.local_attempts.contains(&LocalRecoveryKind::Image)
            {
                return Err(error);
            }
            state.local_attempts.insert(LocalRecoveryKind::Image);
            let compacted = compact_messages_scoped(
                &request.messages,
                &request.model,
                crate::context::context_window_for(&request.model),
                true,
                self.summarizer.as_ref(),
                Some(execution),
            )
            .map_err(|_| error)?;
            request.messages = compacted.messages;
            crate::normalize::normalize(&mut request.messages);
            crate::context::image_budget::enforce_request(request).await?;
        }
        Ok(image_count(&request.messages) < before)
    }

    /// 构造使用 [`no_summarizer`]（确定性降级）的恢复器。
    #[must_use]
    pub fn new() -> Self {
        Self {
            summarizer: no_summarizer(),
        }
    }

    /// 注入自定义摘要器（未来接入真实 LLM 摘要时使用）。
    #[must_use]
    pub fn with_summarizer(summarizer: Arc<dyn Summarizer>) -> Self {
        Self { summarizer }
    }

    /// 执行三阶段恢复（对照旧 `QueryEngine` 413 分支的 Phase 1/2/3 顺序与守卫）。
    ///
    /// `error_message` 用于 Phase 3 的媒体相关性判定。恢复成功返回
    /// [`RecoveryOutcome::Recovered`] 并更新 `state.last_transition_reason`；
    /// 全部失败返回 [`RecoveryOutcome::Exhausted`] 并置 `state.recovery_exhausted`。
    #[must_use]
    pub fn recover(
        &self,
        messages: &[ChatMessage],
        model: &str,
        context_window: u32,
        error_message: &str,
        state: &mut RecoveryState,
    ) -> RecoveryOutcome {
        self.recover_scoped(messages, model, context_window, error_message, state, None)
    }

    /// Recover with explicit durable attribution for physical LLM summaries.
    #[must_use]
    pub fn recover_scoped(
        &self,
        messages: &[ChatMessage],
        model: &str,
        context_window: u32,
        error_message: &str,
        state: &mut RecoveryState,
        execution: Option<&SummaryExecution>,
    ) -> RecoveryOutcome {
        let before_tokens = estimate_tokens(messages, model);

        if error_message.contains("CONTEXT_BUDGET_EXCEEDED")
            || error_message.contains("PAYLOAD_TOO_LARGE")
        {
            if state.local_attempts.contains(&LocalRecoveryKind::Payload) {
                state.recovery_exhausted = true;
                return RecoveryOutcome::Exhausted;
            }
            state.local_attempts.insert(LocalRecoveryKind::Payload);
            return compact_messages_scoped(
                messages,
                model,
                context_window,
                true,
                self.summarizer.as_ref(),
                execution,
            )
            .map_or_else(
                |_| {
                    state.recovery_exhausted = true;
                    RecoveryOutcome::Exhausted
                },
                |result| RecoveryOutcome::Recovered {
                    messages: result.messages,
                    phase: RecoveryPhase::ReactiveCompact,
                    before_tokens: result.before_tokens,
                    after_tokens: result.after_tokens,
                },
            );
        }

        // Phase 1: CollapseDrain（半窗口目标；防重复守卫）
        if state.last_transition_reason.as_deref() != Some("collapse_drain_retry")
            && let Ok(result) = compact_messages_scoped(
                messages,
                model,
                context_window / 2,
                true,
                self.summarizer.as_ref(),
                execution,
            )
        {
            state.last_transition_reason = Some("collapse_drain_retry".to_owned());
            return RecoveryOutcome::Recovered {
                messages: result.messages,
                phase: RecoveryPhase::CollapseDrain,
                before_tokens: result.before_tokens,
                after_tokens: result.after_tokens,
            };
        }

        // Phase 2: ReactiveCompact（单次 guard）
        if !state.has_attempted_reactive {
            state.has_attempted_reactive = true;
            if let Ok(result) = compact_messages_scoped(
                messages,
                model,
                context_window,
                true,
                self.summarizer.as_ref(),
                execution,
            ) {
                state.last_transition_reason = Some("reactive_compact_retry".to_owned());
                return RecoveryOutcome::Recovered {
                    messages: result.messages,
                    phase: RecoveryPhase::ReactiveCompact,
                    before_tokens: result.before_tokens,
                    after_tokens: result.after_tokens,
                };
            }
        }

        // Phase 3: MediaRecovery（仅媒体相关错误）
        if is_media_related_error(error_message)
            && let Some(stripped) = if execution.is_some() {
                strip_transient_media(messages)
            } else {
                strip_media_payloads(messages)
            }
        {
            let after_tokens = estimate_tokens(&stripped, model);
            state.last_transition_reason = Some("media_strip_retry".to_owned());
            return RecoveryOutcome::Recovered {
                messages: stripped,
                phase: RecoveryPhase::MediaRecovery,
                before_tokens,
                after_tokens,
            };
        }

        tracing::error!(
            "ContextRecovery: 413 恢复策略已耗尽（collapse drain / reactive compact / media strip 均失败）"
        );
        state.recovery_exhausted = true;
        RecoveryOutcome::Exhausted
    }
}

fn image_count(messages: &[ChatMessage]) -> usize {
    messages.iter().map(|message| message.images.len()).sum()
}

/// Confirm only the last physical attempt's actual image projection, after a complete successful turn.
pub(crate) fn confirm_request_images(request: &mut zk_llm::ChatRequest, state: &mut RecoveryState) {
    if let Ok(delivered) = request.delivered_image_sources.lock() {
        state
            .confirmed_image_sources
            .extend(delivered.iter().cloned());
    }
    suppress_confirmed_tool_images(&mut request.messages, &state.confirmed_image_sources);
}

fn suppress_confirmed_tool_images(
    messages: &mut [ChatMessage],
    confirmed: &std::collections::HashSet<String>,
) {
    for message in messages {
        let Some(metadata) = message
            .metadata
            .as_mut()
            .and_then(serde_json::Value::as_object_mut)
        else {
            continue;
        };
        if metadata
            .get("syntheticToolImages")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        {
            continue;
        }
        let Some(digests) = metadata
            .get_mut("imageSourceDigests")
            .and_then(serde_json::Value::as_array_mut)
        else {
            continue;
        };
        for index in (0..message.images.len()).rev() {
            if digests
                .get(index)
                .and_then(serde_json::Value::as_str)
                .is_some_and(|source| confirmed.contains(source))
            {
                message.images.remove(index);
                digests.remove(index);
            }
        }
    }
}

// ============ 媒体载荷剥离（Phase 3 载体适配） ============

fn strip_transient_media(messages: &[ChatMessage]) -> Option<Vec<ChatMessage>> {
    let mut result = messages.to_vec();
    let mut changed = false;
    for message in &mut result {
        if !message.images.is_empty()
            && message
                .metadata
                .as_ref()
                .is_some_and(|meta| meta["syntheticToolImages"] == true)
        {
            message.images.clear();
            message
                .content
                .push_str("\n[Tool image omitted for this request after provider media rejection]");
            changed = true;
        }
    }
    changed.then_some(result)
}

/// 剥离所有 [`Role::User`] 消息中的 `data:<mime>;base64,<payload>` 载荷；正文全空时
/// 替换为 [`MEDIA_PLACEHOLDER`]。无任何改动返回 `None`（对照旧 `tryStripMediaBlocks`
/// 的 `stripped` 布尔）。
#[must_use]
pub fn strip_media_payloads(messages: &[ChatMessage]) -> Option<Vec<ChatMessage>> {
    let mut changed_any = false;
    let mut out = Vec::with_capacity(messages.len());
    for message in messages {
        if message.role == Role::User {
            let (cleaned, changed) = strip_data_uris(&message.content);
            let had_images = !message.images.is_empty();
            if changed || had_images {
                changed_any = true;
                let body = if cleaned.trim().is_empty() {
                    MEDIA_PLACEHOLDER.to_owned()
                } else {
                    cleaned
                };
                let mut stripped = message.clone();
                stripped.content = body;
                stripped.images.clear();
                out.push(stripped);
                continue;
            }
        }
        out.push(message.clone());
    }
    if changed_any { Some(out) } else { None }
}

/// 剥离单段文本中的 base64 data URI 载荷；返回 `(清理后文本, 是否有改动)`。
fn strip_data_uris(text: &str) -> (String, bool) {
    const NEEDLE: &str = "data:";
    const HEADER: &str = ";base64,";
    let mut out = String::with_capacity(text.len());
    let mut changed = false;
    let mut search_from = 0usize;
    loop {
        let Some(rel) = text[search_from..].find(NEEDLE) else {
            out.push_str(&text[search_from..]);
            break;
        };
        let start = search_from + rel;
        let mut window_end = (start + 128).min(text.len());
        while !text.is_char_boundary(window_end) {
            window_end -= 1;
        }
        if let Some(header_rel) = text[start..window_end].find(HEADER) {
            // 命中 data URI：输出前缀，跳过载荷直到分隔符/空白/结尾。
            out.push_str(&text[search_from..start]);
            let payload_start = start + header_rel + HEADER.len();
            let mut end = text.len();
            for (i, ch) in text[payload_start..].char_indices() {
                if ch.is_whitespace() || matches!(ch, ')' | '"' | '\'' | ']' | '}' | '>' | ',') {
                    end = payload_start + i;
                    break;
                }
            }
            changed = true;
            search_from = end;
        } else {
            // 非 data URI：原样保留 "data:" 后继续。
            let keep_end = start + NEEDLE.len();
            out.push_str(&text[search_from..keep_end]);
            search_from = keep_end;
        }
    }
    (out, changed)
}

/// 定位并替换最大消息块为摘要占位（`ReactiveStripStrategy.stripLargestBlock` 等价移植）。
///
/// 跳过前三条内的首个 [`Role::System`]（含）之前的引导，保护尾部 `protected_tail`
/// 条不被剥离；在可剥离区内找 token 最大的一条替换为占位文案。无可剥离对象时原样返回。
#[must_use]
pub fn strip_largest_block(
    messages: &[ChatMessage],
    model: &str,
    protected_tail: usize,
) -> Vec<ChatMessage> {
    let total = messages.len();
    let strippable_end = total.saturating_sub(protected_tail);
    if strippable_end <= 1 {
        return messages.to_vec();
    }
    // 跳过前三条内的首个 System（对照旧 startIdx 逻辑）。
    let mut start_idx = 0usize;
    for (i, message) in messages.iter().take(3).enumerate() {
        if message.role == Role::System {
            start_idx = i + 1;
            break;
        }
    }
    if start_idx >= strippable_end {
        return messages.to_vec();
    }
    let mut max_index: Option<usize> = None;
    let mut max_tokens = 0u32;
    for (i, message) in messages
        .iter()
        .enumerate()
        .take(strippable_end)
        .skip(start_idx)
    {
        let tokens = estimate_tokens(std::slice::from_ref(message), model);
        if tokens > max_tokens {
            max_tokens = tokens;
            max_index = Some(i);
        }
    }
    let Some(index) = max_index else {
        return messages.to_vec();
    };
    let mut result = messages.to_vec();
    result[index] = ChatMessage::user(format!(
        "[Content compressed: original {max_tokens} tokens stripped for context recovery]"
    ));
    result
}

// ============ 模型层级冷却（ModelTierService） ============

#[derive(Debug, Clone)]
struct CooldownState {
    cooldown_until: Instant,
    reason: String,
    consecutive_successes: u32,
}

/// 模型性能层级冷却/降级/恢复管理（对照旧 `ModelTierService`）。
#[derive(Debug, Default)]
pub struct ModelTierService {
    cooldowns: Mutex<HashMap<String, CooldownState>>,
}

impl ModelTierService {
    /// 构造空冷却表。
    #[must_use]
    pub fn new() -> Self {
        Self {
            cooldowns: Mutex::new(HashMap::new()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, CooldownState>> {
        self.cooldowns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// 选择当前应使用的模型——首选不在冷却期则用首选，否则沿降级链取首个可用，
    /// 全部冷却则返回降级链末位（对照旧 `resolveModel`）。
    #[must_use]
    pub fn resolve_model(&self, preferred: &str, tier_chain: &[String]) -> String {
        if tier_chain.is_empty() {
            return preferred.to_owned();
        }
        if !self.is_in_cooldown(preferred) {
            return preferred.to_owned();
        }
        for tier in tier_chain {
            if !self.is_in_cooldown(tier) {
                return tier.clone();
            }
        }
        tier_chain
            .last()
            .cloned()
            .unwrap_or_else(|| preferred.to_owned())
    }

    /// 触发冷却——`retry_after` 存在且短于 [`SHORT_RETRY_THRESHOLD`] 时跳过；否则
    /// 冷却时长为 `max(retry_after, MIN_COOLDOWN)`（有 retry-after）或
    /// [`DEFAULT_COOLDOWN`]（无）（对照旧 `triggerCooldown`）。
    pub fn trigger_cooldown(
        &self,
        model: &str,
        retry_after: Option<Duration>,
        reason: impl Into<String>,
    ) {
        if let Some(retry) = retry_after
            && retry < SHORT_RETRY_THRESHOLD
        {
            return;
        }
        let cooldown = match retry_after {
            Some(retry) => retry.max(MIN_COOLDOWN),
            None => DEFAULT_COOLDOWN,
        };
        let until = Instant::now()
            .checked_add(cooldown)
            .unwrap_or_else(Instant::now);
        self.lock().insert(
            model.to_owned(),
            CooldownState {
                cooldown_until: until,
                reason: reason.into(),
                consecutive_successes: 0,
            },
        );
    }

    /// 报告成功调用——冷却期已过后累计连续成功数，达 [`RECOVERY_SUCCESS_THRESHOLD`]
    /// 移除冷却（对照旧 `reportSuccess`）。
    pub fn report_success(&self, model: &str) {
        let mut guard = self.lock();
        let Some(state) = guard.get(model).cloned() else {
            return;
        };
        if Instant::now() > state.cooldown_until {
            let new_count = state.consecutive_successes + 1;
            if new_count >= RECOVERY_SUCCESS_THRESHOLD {
                guard.remove(model);
            } else {
                guard.insert(
                    model.to_owned(),
                    CooldownState {
                        consecutive_successes: new_count,
                        ..state
                    },
                );
            }
        }
    }

    /// 返回冷却原因（用于可观测/UI 展示，对照旧 `getCooldownInfo` 的 `reason`
    /// 字段）；无记录返回 `None`。
    #[must_use]
    pub fn cooldown_reason(&self, model: &str) -> Option<String> {
        self.lock().get(model).map(|state| state.reason.clone())
    }

    /// 检查模型是否在冷却期（冷却期结束但未恢复时返回 `false`，允许探测；对照旧
    /// `isInCooldown`）。
    #[must_use]
    pub fn is_in_cooldown(&self, model: &str) -> bool {
        let guard = self.lock();
        guard
            .get(model)
            .is_some_and(|state| Instant::now() <= state.cooldown_until)
    }

    #[cfg(test)]
    fn seed_expired_cooldown(&self, model: &str, reason: &str) {
        let past = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .unwrap_or_else(Instant::now);
        self.lock().insert(
            model.to_owned(),
            CooldownState {
                cooldown_until: past,
                reason: reason.to_owned(),
                consecutive_successes: 0,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(content: &str) -> ChatMessage {
        ChatMessage::user(content)
    }

    fn assistant(content: &str) -> ChatMessage {
        ChatMessage::assistant(content)
    }

    // ---- 错误分类 ----

    #[test]
    fn context_limit_error_detects_413_and_known_substrings() {
        assert!(is_context_limit_error(Some(413), ""));
        assert!(is_context_limit_error(None, "prompt_too_long"));
        assert!(is_context_limit_error(None, "This PROMPT is too long"));
        assert!(is_context_limit_error(None, "context length exceeded"));
        assert!(is_context_limit_error(None, "maximum context reached"));
        assert!(is_context_limit_error(None, "token limit hit"));
        assert!(!is_context_limit_error(Some(500), "internal error"));
        assert!(!is_context_limit_error(None, "rate limited"));
    }

    #[test]
    fn media_error_detects_image_and_media_substrings() {
        assert!(is_media_related_error("Image is invalid"));
        assert!(is_media_related_error("image too_large"));
        assert!(is_media_related_error("file_too_large"));
        assert!(is_media_related_error("invalid_image"));
        assert!(is_media_related_error("Could not process image"));
        assert!(is_media_related_error("media_type_not_supported"));
        assert!(!is_media_related_error("image present")); // 无 invalid/too_large
        assert!(!is_media_related_error("prompt too long"));
    }

    // ---- 三阶段编排 ----

    fn long_history() -> Vec<ChatMessage> {
        // 构造足够长、含可压缩中段的历史。
        let mut msgs = vec![ChatMessage::system("system rules")];
        for i in 0..40 {
            msgs.push(user(&format!("user turn {i}: {}", "x".repeat(40))));
            msgs.push(assistant(&format!(
                "assistant turn {i}: {}",
                "y".repeat(400)
            )));
        }
        msgs
    }

    #[test]
    fn recover_phase1_collapse_drain_first() {
        let recovery = ContextRecovery::new();
        let mut state = RecoveryState::default();
        let msgs = long_history();
        let outcome = recovery.recover(&msgs, "gpt-4o", 8192, "prompt is too long", &mut state);
        match outcome {
            RecoveryOutcome::Recovered {
                phase,
                after_tokens,
                before_tokens,
                ..
            } => {
                assert_eq!(phase, RecoveryPhase::CollapseDrain);
                assert!(after_tokens <= before_tokens);
            }
            RecoveryOutcome::Exhausted => panic!("expected phase 1 recovery"),
        }
        assert_eq!(
            state.last_transition_reason.as_deref(),
            Some("collapse_drain_retry")
        );
        assert!(!state.recovery_exhausted);
    }

    #[test]
    fn recover_phase2_reactive_when_drain_already_done() {
        let recovery = ContextRecovery::new();
        let mut state = RecoveryState {
            final_answer_recovery_attempted: false,
            last_transition_reason: Some("collapse_drain_retry".to_owned()),
            ..RecoveryState::default()
        };
        let msgs = long_history();
        let outcome = recovery.recover(&msgs, "gpt-4o", 8192, "prompt is too long", &mut state);
        match outcome {
            RecoveryOutcome::Recovered { phase, .. } => {
                assert_eq!(phase, RecoveryPhase::ReactiveCompact);
            }
            RecoveryOutcome::Exhausted => panic!("expected phase 2 recovery"),
        }
        assert!(state.has_attempted_reactive);
        assert_eq!(
            state.last_transition_reason.as_deref(),
            Some("reactive_compact_retry")
        );
    }

    #[test]
    fn recover_phase3_media_strip_when_media_error() {
        let recovery = ContextRecovery::new();
        // 短历史 + 已尝试 reactive → Phase1/2 均无功，仅媒体剥离可用。
        let mut state = RecoveryState {
            final_answer_recovery_attempted: false,
            last_transition_reason: Some("collapse_drain_retry".to_owned()),
            has_attempted_reactive: true,
            recovery_exhausted: false,
            ..RecoveryState::default()
        };
        let msgs = vec![
            ChatMessage::system("rules"),
            user("here is an image data:image/png;base64,AAAABBBBCCCCDDDD and some text"),
        ];
        let outcome = recovery.recover(&msgs, "gpt-4o", 8192, "invalid_image", &mut state);
        match outcome {
            RecoveryOutcome::Recovered {
                phase, messages, ..
            } => {
                assert_eq!(phase, RecoveryPhase::MediaRecovery);
                assert!(!messages[1].content.contains("base64"));
                assert!(messages[1].content.contains("and some text"));
            }
            RecoveryOutcome::Exhausted => panic!("expected media recovery"),
        }
        assert_eq!(
            state.last_transition_reason.as_deref(),
            Some("media_strip_retry")
        );
    }

    #[test]
    fn recover_exhausts_when_no_strategy_applies() {
        let recovery = ContextRecovery::new();
        let mut state = RecoveryState {
            final_answer_recovery_attempted: false,
            last_transition_reason: Some("collapse_drain_retry".to_owned()),
            has_attempted_reactive: true,
            recovery_exhausted: false,
            ..RecoveryState::default()
        };
        // 非媒体错误 + 无可剥离媒体 → 耗尽。
        let msgs = vec![ChatMessage::system("rules"), user("plain text only")];
        let outcome = recovery.recover(&msgs, "gpt-4o", 8192, "prompt is too long", &mut state);
        assert!(matches!(outcome, RecoveryOutcome::Exhausted));
        assert!(state.recovery_exhausted);
    }

    // ---- 媒体剥离 ----

    #[test]
    fn strip_data_uris_removes_payload_and_keeps_surrounding_text() {
        let (out, changed) = strip_data_uris("before data:image/png;base64,QUJD after");
        assert!(changed);
        assert!(!out.contains("QUJD"));
        assert!(out.contains("before"));
        assert!(out.contains("after"));
    }

    #[test]
    fn strip_data_uris_is_noop_without_base64_uri() {
        let (out, changed) = strip_data_uris("visit data: the colon prefix only");
        assert!(!changed);
        assert_eq!(out, "visit data: the colon prefix only");
    }

    #[test]
    fn strip_media_payloads_replaces_empty_user_with_placeholder() {
        let msgs = vec![user("data:image/png;base64,QUJDREVG")];
        let stripped = strip_media_payloads(&msgs).expect("should change");
        assert_eq!(stripped[0].content, MEDIA_PLACEHOLDER);
    }

    #[test]
    fn strip_media_payloads_returns_none_when_no_media() {
        assert!(strip_media_payloads(&[user("just text"), assistant("reply")]).is_none());
    }

    #[test]
    fn strip_media_payloads_removes_structured_images_not_only_data_uris() {
        let msgs = vec![ChatMessage::user_with_images(
            "inspect this",
            vec![zk_llm::ImageSource {
                media_type: "image/png".into(),
                data: Some("QUJD".into()),
                url: None,
            }],
        )];

        let stripped = strip_media_payloads(&msgs).expect("structured image must be removed");

        assert_eq!(stripped[0].content, "inspect this");
        assert!(stripped[0].images.is_empty());
    }

    #[test]
    fn strip_media_payloads_only_touches_user_messages() {
        let msgs = vec![
            assistant("assistant with data:image/png;base64,QUJD inline"),
            user("user with data:image/png;base64,WXYZ inline"),
        ];
        let stripped = strip_media_payloads(&msgs).expect("user changed");
        assert!(stripped[0].content.contains("QUJD")); // assistant untouched
        assert!(!stripped[1].content.contains("WXYZ")); // user stripped
    }

    // ---- strip_largest_block ----

    #[test]
    fn strip_largest_block_replaces_biggest_non_protected_message() {
        let msgs = vec![
            ChatMessage::system("rules"),
            user("small"),
            assistant(&"HUGE ".repeat(200)),
            user("recent tail"),
        ];
        let out = strip_largest_block(&msgs, "gpt-4o", 1);
        assert!(out[2].content.contains("stripped for context recovery"));
        assert_eq!(out[3].content, "recent tail"); // protected tail intact
        assert_eq!(out[0].content, "rules"); // system intact
    }

    #[test]
    fn strip_largest_block_is_noop_for_tiny_history() {
        let msgs = vec![ChatMessage::system("rules"), user("only")];
        let out = strip_largest_block(&msgs, "gpt-4o", 1);
        assert_eq!(out, msgs);
    }

    // ---- ModelTierService ----

    #[test]
    fn resolve_model_prefers_available_then_falls_down_chain() {
        let svc = ModelTierService::new();
        let chain = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        assert_eq!(svc.resolve_model("a", &chain), "a");
        svc.trigger_cooldown("a", None, "overload");
        assert_eq!(svc.resolve_model("a", &chain), "b");
        svc.trigger_cooldown("b", None, "overload");
        assert_eq!(svc.resolve_model("a", &chain), "c");
        svc.trigger_cooldown("c", None, "overload");
        // 全冷却 → 末位。
        assert_eq!(svc.resolve_model("a", &chain), "c");
    }

    #[test]
    fn cooldown_reason_exposes_trigger_reason() {
        let svc = ModelTierService::new();
        assert!(svc.cooldown_reason("m").is_none());
        svc.trigger_cooldown("m", None, "capacity");
        assert_eq!(svc.cooldown_reason("m").as_deref(), Some("capacity"));
    }

    #[test]
    fn trigger_cooldown_skips_short_retry_after() {
        let svc = ModelTierService::new();
        svc.trigger_cooldown("m", Some(Duration::from_secs(5)), "short");
        assert!(!svc.is_in_cooldown("m"));
        svc.trigger_cooldown("m", Some(Duration::from_mins(1)), "long");
        assert!(svc.is_in_cooldown("m"));
    }

    #[test]
    fn report_success_recovers_after_threshold_when_cooldown_elapsed() {
        let svc = ModelTierService::new();
        svc.seed_expired_cooldown("m", "overload");
        assert!(!svc.is_in_cooldown("m")); // 冷却期已过，允许探测
        svc.report_success("m");
        svc.report_success("m");
        assert_eq!(
            svc.resolve_model("m", &["m".to_owned(), "n".to_owned()]),
            "m"
        );
        svc.report_success("m"); // 第 3 次 → 移除
        // 移除后 resolve 直接返回首选。
        assert_eq!(svc.resolve_model("m", &["m".to_owned()]), "m");
        assert!(!svc.is_in_cooldown("m"));
    }

    #[test]
    fn recover_outcome_is_marked_must_use() {
        // 编译期约束：recover 返回值带 #[must_use]，此处显式消费以示意。
        let recovery = ContextRecovery::default();
        let mut state = RecoveryState::default();
        let _ = recovery.recover(&[user("x")], "gpt-4o", 8192, "", &mut state);
    }

    #[test]
    fn local_payload_recovery_is_bounded_once_even_when_rebuild_cannot_fit() {
        let recovery = ContextRecovery::default();
        let mut state = RecoveryState::default();
        let messages = vec![user("mandatory original text")];
        let _ = recovery.recover(
            &messages,
            "gpt-4o",
            8192,
            "CONTEXT_BUDGET_EXCEEDED",
            &mut state,
        );
        assert!(
            state
                .local_attempts
                .contains(&super::LocalRecoveryKind::Payload)
        );
        assert!(matches!(
            recovery.recover(&messages, "gpt-4o", 8192, "PAYLOAD_TOO_LARGE", &mut state),
            RecoveryOutcome::Exhausted
        ));
        assert!(state.recovery_exhausted);
        assert_eq!(messages[0].content, "mandatory original text");
    }
}
