//! 六层级联编排器 + L0/L1/L2 三个纯字符层的实现。
//!
//! 建模来源（`main@581d407b`）：
//!
//! | 旧类/成员 | 本模块 |
//! |---|---|
//! | `ContextCascade.executePreApiCascade` | [`ContextCascade::execute_pre_api_cascade`] |
//! | `ContextCascade.executeErrorRecoveryCascade` | [`ContextCascade::execute_error_recovery_cascade`] |
//! | `ContextCascade.CascadeResult` | [`CascadeResult`] |
//! | `ContextCascade.AutoCompactTrackingState` | [`AutoCompactTrackingState`] |
//! | `SnipService.snipIfNeeded` / `snipToolResults` | [`snip_if_needed`] / [`snip_tool_results`] |
//! | `MicroCompactService.compactMessages` | [`micro_compact`] |
//! | `ContextCollapseService.progressiveCollapse` | [`progressive_collapse`] |
//! | `CollapseLevel`（sealed interface 三实现） | [`CollapseLevel`] |
//!
//! # 未移植项（留痕）
//!
//! - `SnipService.budgetToolResults` / `persistToolResult`：把超大工具结果落盘再
//!   以 `<persisted-output>` 路径回填，依赖旧的落盘目录约定与 `ToolResultBlock`
//!   多块结构；Rust 侧 [`zk_llm::ChatMessage`] 一条消息只承载一个工具结果，无
//!   「单消息内多结果求和」场景，故不移植；
//! - `ContextCollapseService.collapseMessages`（固定尾部保护的单级折叠）：旧
//!   `ContextCascade` 只走 `progressiveCollapse`，前者无调用点，不移植；
//! - `fingerprint` / `logProgressiveDiagnostics` 等诊断：以 `tracing` 结构化字段
//!   等价表达，不复刻 SHA-256 指纹采样。

use std::sync::Arc;

use zk_llm::{ChatMessage, Role};

use super::compact::{
    CompactResult, Summarizer, compact_messages, no_summarizer, reactive_compact,
    should_auto_compact,
};
use super::{
    MICRO_COMPACT_PROTECTED_TAIL, TOOL_RESULT_BUDGET_RATIO, char_count, context_window_for,
    estimate_tokens, saturating_tokens, scale_tokens, token_char_ratio,
};

/// 熔断阈值：连续失败达此次数后停止尝试 `AutoCompact`（逐字对照旧
/// `ContextCascade.MAX_CONSECUTIVE_FAILURES`）。
pub const MAX_CONSECUTIVE_FAILURES: u32 = 3;

/// 微压缩清除后的占位文案（逐字对照旧 `MicroCompactService.CLEARED_MESSAGE`）。
pub const CLEARED_MESSAGE: &str = "[Old tool result content cleared]";

/// 可微压缩工具白名单（逐字对照旧 `MicroCompactService.COMPACTABLE_TOOLS`）。
///
/// 白名单外的工具结果**永不清除**——旧实现的保守取向（无法确定工具名时同样
/// 不清除），本实现照搬。
pub const COMPACTABLE_TOOLS: [&str; 8] = [
    "Read",
    "Bash",
    "Grep",
    "Glob",
    "WebSearch",
    "WebFetch",
    "Edit",
    "Write",
];

/// 截断标记预留字符数（逐字对照旧 `SnipService.snipIfNeeded` 的 `- 80`）。
const TRUNCATION_MARKER_RESERVE: usize = 80;

/// `SummaryRetention` 级保留的前缀字符数（旧 `SummaryRetention(30, 500)`）。
const SUMMARY_KEEP_CHARS: usize = 500;

/// `SkeletonRetention` 级的最短生效长度（旧 `length() <= 50` 直接返回）。
const SKELETON_MIN_CHARS: usize = 50;

/// `SkeletonRetention` 级保留的首行字符数（旧 `Math.min(newline, 80)`）。
const SKELETON_FIRST_LINE_CHARS: usize = 80;

/// `SkeletonRetention` 级前缀（旧 `"[skeleton] "`）。
const SKELETON_PREFIX: &str = "[skeleton] ";

/// 级联层级标识（六层的显式枚举；旧仓库以日志文案区分）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeLevel {
    /// L0：单条工具结果首尾截断。
    Snip,
    /// L1：白名单旧工具结果清除。
    MicroCompact,
    /// L2：三级渐进折叠。
    ContextCollapse,
    /// L3：阈值触发的自动压缩。
    AutoCompact,
    /// L4：413 恢复 Phase 1——半窗口激进压缩。
    CollapseDrain,
    /// L5：413 恢复 Phase 2——反应式压缩（单次）。
    ReactiveCompact,
}

/// `AutoCompact` 决策（逐字对照旧 `autoCompactDecision` 的四个字面量）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoCompactDecision {
    /// 折叠已生效且重评估后已低于阈值（旧 `SKIP_BELOW_THRESHOLD`）。
    SkipBelowThreshold,
    /// 达到阈值并尝试压缩（旧 `ATTEMPT`）。
    Attempt,
    /// 熔断打开，拒绝尝试（旧 `CIRCUIT_OPEN`）。
    CircuitOpen,
    /// 折叠未生效且未达阈值（旧 `NOT_NEEDED_NO_COLLAPSE`）。
    NotNeededNoCollapse,
}

/// 自动压缩追踪状态——跨轮次持久化（逐字段对照旧
/// `ContextCascade.AutoCompactTrackingState`）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AutoCompactTrackingState {
    /// 本轮是否已压缩过。
    pub compacted_this_turn: bool,
    /// 成功压缩计数。
    pub turn_counter: u32,
    /// 上次成功压缩的轮次 ID。
    pub last_turn_id: String,
    /// 连续失败次数（达 [`MAX_CONSECUTIVE_FAILURES`] 熔断）。
    pub consecutive_failures: u32,
}

impl AutoCompactTrackingState {
    /// 初始态（旧 `initial()`）。
    #[must_use]
    pub fn initial() -> Self {
        Self::default()
    }

    /// 熔断态——`AutoCompact` 被配置关闭时使用（旧 `QueryEngine` 在 autoCompact
    /// 关闭时传 `consecutiveFailures = Integer.MAX_VALUE` 强制熔断）。
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            consecutive_failures: u32::MAX,
            ..Self::default()
        }
    }

    /// 记一次失败（旧 `withFailure()`）。
    #[must_use]
    pub fn with_failure(&self) -> Self {
        Self {
            compacted_this_turn: false,
            turn_counter: self.turn_counter,
            last_turn_id: self.last_turn_id.clone(),
            consecutive_failures: self.consecutive_failures.saturating_add(1),
        }
    }

    /// 记一次成功（旧 `withSuccess(turnId)`，失败计数归零）。
    #[must_use]
    pub fn with_success(&self, turn_id: impl Into<String>) -> Self {
        Self {
            compacted_this_turn: true,
            turn_counter: self.turn_counter.saturating_add(1),
            last_turn_id: turn_id.into(),
            consecutive_failures: 0,
        }
    }

    /// 熔断是否已打开（旧 `isCircuitBroken()`）。
    #[must_use]
    pub fn is_circuit_broken(&self) -> bool {
        self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES
    }
}

/// 前置级联结果（逐字段对照旧 `ContextCascade.CascadeResult`）。
#[derive(Debug, Clone)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "逐字段对照旧 Java ContextCascade.CascadeResult record 的 4 个 bool 观测位"
)]
pub struct CascadeResult {
    /// 级联后消息序列。
    pub messages: Vec<ChatMessage>,
    /// 级联前 token 估算。
    pub original_tokens: u32,
    /// 级联后 token 估算。
    pub final_tokens: u32,
    /// L0 是否生效。
    pub snip_executed: bool,
    /// L0 释放 token 数。
    pub snip_tokens_freed: u32,
    /// L1 是否生效。
    pub micro_compact_executed: bool,
    /// L1 释放 token 数。
    pub micro_compact_tokens_freed: u32,
    /// L2 是否生效。
    pub context_collapse_executed: bool,
    /// L2 释放字符数。
    pub context_collapse_chars_freed: u64,
    /// L3 是否达阈值并尝试。
    pub auto_compact_attempted: bool,
    /// L3 是否真正落地。
    pub auto_compact_executed: bool,
    /// L3 决策。
    pub auto_compact_decision: AutoCompactDecision,
    /// L3 压缩结果明细。
    pub auto_compact_result: Option<CompactResult>,
}

impl CascadeResult {
    /// 保守下界的净释放 token 数（逐字对照旧 `totalTokensFreed()` 的
    /// `Math.max(0, original - final)`——估算策略变动时负值无意义）。
    #[must_use]
    pub fn total_tokens_freed(&self) -> u32 {
        self.original_tokens.saturating_sub(self.final_tokens)
    }

    /// 本次生效的层级清单。
    #[must_use]
    pub fn levels_applied(&self) -> Vec<CascadeLevel> {
        let mut levels = Vec::with_capacity(4);
        if self.snip_executed {
            levels.push(CascadeLevel::Snip);
        }
        if self.micro_compact_executed {
            levels.push(CascadeLevel::MicroCompact);
        }
        if self.context_collapse_executed {
            levels.push(CascadeLevel::ContextCollapse);
        }
        if self.auto_compact_executed {
            levels.push(CascadeLevel::AutoCompact);
        }
        levels
    }

    /// 单行摘要（逐条对照旧 `summary()`）。
    #[must_use]
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let mut text = format!(
            "ContextCascade: {} → {} tokens",
            self.original_tokens, self.final_tokens
        );
        if self.snip_executed {
            let _ = write!(text, ", Snip: -{}", self.snip_tokens_freed);
        }
        if self.micro_compact_executed {
            let _ = write!(text, ", MicroCompact: -{}", self.micro_compact_tokens_freed);
        }
        if self.context_collapse_executed {
            let _ = write!(
                text,
                ", Collapse: -{}chars",
                self.context_collapse_chars_freed
            );
        }
        if self.auto_compact_attempted && !self.auto_compact_executed {
            text.push_str(", AutoCompact: ATTEMPTED_FAILED");
        }
        if self.auto_compact_executed
            && let Some(result) = &self.auto_compact_result
        {
            let _ = write!(
                text,
                ", AutoCompact: {} → {} tokens",
                result.before_tokens, result.after_tokens
            );
        }
        text
    }

    /// Batch 0 Step 0-6 遥测：本次级联的**主导层级**名（供 `compact_event.phase`
    /// 使用）。选择序对齐旧 `handler.onCompactEvent(type, ...)` 各调用点的
    /// `type` 字面量：优先级最高的 L3 `auto_compact` → L2 `context_collapse` →
    /// L1 `micro_compact` → L0 `tool_result_snip`（旧命名 `smc_compact` 属
    /// SMC 分支，Rust 侧当前 L3 单一路径 → 归 `auto_compact`）。
    #[must_use]
    pub fn telemetry_phase(&self) -> &'static str {
        if self.auto_compact_executed {
            "auto_compact"
        } else if self.context_collapse_executed {
            "context_collapse"
        } else if self.micro_compact_executed {
            "micro_compact"
        } else if self.snip_executed {
            "tool_result_snip"
        } else {
            "noop"
        }
    }

    /// Batch 0 Step 0-6 遥测：`(已释放百分比, 剩余 token 数)`。
    ///
    /// 已释放百分比 = `(original - final) * 100 / original`（`original` 为 0
    /// 时返回 0，避免除零）；剩余 = `final_tokens`。前端 `compact_event`
    /// 消费方（`stompClient.ts` `VALID_MESSAGE_TYPES` 已白名单）以 `usage_percent`
    /// 展示压缩幅度、`current_tokens` 展示剩余上下文规模。
    #[must_use]
    pub fn telemetry_snapshot(&self) -> (i64, i64) {
        (
            super::compact_percent_freed(self.original_tokens, self.final_tokens).0,
            i64::from(self.final_tokens),
        )
    }
}

/// L3 评估产出（内部载体）。
struct AutoCompactOutcome {
    decision: AutoCompactDecision,
    attempted: bool,
    executed: bool,
    result: Option<CompactResult>,
}

/// 六层级联编排器（旧 `ContextCascade` 的等价实现）。
///
/// 旧类经构造注入 5 个协作 Service；本实现把 L0/L1/L2 落为本模块的纯函数、
/// L3/L4/L5 落为 [`super::compact`] 的纯函数，唯一需要注入的可变依赖是 LLM
/// 摘要端口 [`Summarizer`]（缺省为不产摘要的占位实现）。
pub struct ContextCascade {
    summarizer: Arc<dyn Summarizer>,
}

impl std::fmt::Debug for ContextCascade {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextCascade").finish_non_exhaustive()
    }
}

impl Default for ContextCascade {
    fn default() -> Self {
        Self {
            summarizer: no_summarizer(),
        }
    }
}

impl ContextCascade {
    /// 新建编排器（缺省摘要端口）。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 新建编排器并装配摘要端口。
    #[must_use]
    pub fn with_summarizer(summarizer: Arc<dyn Summarizer>) -> Self {
        Self { summarizer }
    }

    /// 执行前置级联——L0 + L1 + L2 + L3（逐条对照旧
    /// `executePreApiCascade`，在旧 `QueryEngine.queryLoop()` 的 Step 1 调用）。
    ///
    /// L0/L1/L2 每轮无条件执行（纯字符操作，无 LLM 调用）；L3 只在阈值命中且
    /// 熔断未打开时尝试，且遵循旧的「Collapse 生效则重新评估阈值」互斥协调。
    #[must_use]
    pub fn execute_pre_api_cascade(
        &self,
        messages: Vec<ChatMessage>,
        model: &str,
        tracking: &AutoCompactTrackingState,
    ) -> CascadeResult {
        let context_window = context_window_for(model);
        let mut current = messages;

        // ===== 前置 P0-15：图片 / Base64 预算守卫 Phase 1 =====
        // 旧调用点在 `QueryEngine` Step 3，位于级联之后但在 API 转换之前；本实现
        // 前置于 L0，使级联各层看到的都是已清理 Base64 的上下文。`image-budget`
        // feature 关闭时为空转，级联行为与接入前逐字一致。
        super::image_budget::apply(&mut current, model);

        let original_tokens = estimate_tokens(&current, model);

        // ===== L0 Snip：单条工具结果首尾截断 =====
        let budget = tool_result_budget_chars(context_window, model);
        let after_snip = snip_tool_results(&current, budget);
        let snip_before = estimate_tokens(&current, model);
        let snip_after = estimate_tokens(&after_snip, model);
        let snip_executed = snip_after < snip_before;
        let snip_tokens_freed = if snip_executed {
            current = after_snip;
            snip_before - snip_after
        } else {
            0
        };

        // ===== L1 MicroCompact：白名单旧工具结果清除 =====
        let micro = micro_compact(&current, MICRO_COMPACT_PROTECTED_TAIL, model);
        let micro_compact_executed = micro.tokens_freed > 0;
        let micro_compact_tokens_freed = if micro_compact_executed {
            current = micro.messages;
            micro.tokens_freed
        } else {
            0
        };

        // ===== L2 ContextCollapse：三级渐进折叠 =====
        let collapse = progressive_collapse(&current);
        let context_collapse_executed = collapse.collapsed_count > 0;
        let context_collapse_chars_freed = if context_collapse_executed {
            current = collapse.messages;
            collapse.estimated_chars_freed
        } else {
            0
        };

        // ===== L3 AutoCompact：含 Collapse 互斥协调 =====
        let outcome = self.evaluate_auto_compact(
            &mut current,
            model,
            context_window,
            tracking,
            context_collapse_executed,
        );

        let final_tokens = estimate_tokens(&current, model);
        let result = CascadeResult {
            messages: current,
            original_tokens,
            final_tokens,
            snip_executed,
            snip_tokens_freed,
            micro_compact_executed,
            micro_compact_tokens_freed,
            context_collapse_executed,
            context_collapse_chars_freed,
            auto_compact_attempted: outcome.attempted,
            auto_compact_executed: outcome.executed,
            auto_compact_decision: outcome.decision,
            auto_compact_result: outcome.result,
        };
        if result.total_tokens_freed() > 0 {
            tracing::info!(
                decision = ?result.auto_compact_decision,
                "{}",
                result.summary()
            );
        }
        result
    }

    /// L3 决策与执行（逐条对照旧 `executePreApiCascade` 的 Level 2 分支树）。
    ///
    /// # 与旧实现的一处偏离（留痕）
    ///
    /// 旧 `ContextCascade` 的阈值判定只调 `calculateTokenWarningState`，未过
    /// `CompactService.shouldAutoCompactBufferBased` 的两道守卫（消息数 ≥ 5、
    /// user/assistant 消息 ≥ 2）。本实现统一走
    /// [`should_auto_compact`]（含守卫），使短会话与纯系统提示上下文不被压缩
    /// ——与「仅在 token 超限时才触发压缩」的取向一致。
    fn evaluate_auto_compact(
        &self,
        current: &mut Vec<ChatMessage>,
        model: &str,
        context_window: u32,
        tracking: &AutoCompactTrackingState,
        collapse_executed: bool,
    ) -> AutoCompactOutcome {
        // 分支结构照搬旧实现（含两支在「同时低于阈值且熔断打开」时给出不同
        // 决策标签的差异）。
        if collapse_executed {
            if !should_auto_compact(current, model) {
                return AutoCompactOutcome::skipped(AutoCompactDecision::SkipBelowThreshold);
            }
            if tracking.is_circuit_broken() {
                return AutoCompactOutcome::skipped(AutoCompactDecision::CircuitOpen);
            }
        } else {
            if tracking.is_circuit_broken() {
                return AutoCompactOutcome::skipped(AutoCompactDecision::CircuitOpen);
            }
            if !should_auto_compact(current, model) {
                return AutoCompactOutcome::skipped(AutoCompactDecision::NotNeededNoCollapse);
            }
        }

        match compact_messages(
            current,
            model,
            context_window,
            false,
            self.summarizer.as_ref(),
        ) {
            Ok(result) => {
                current.clear();
                current.extend(result.messages.iter().cloned());
                AutoCompactOutcome {
                    decision: AutoCompactDecision::Attempt,
                    attempted: true,
                    executed: true,
                    result: Some(result),
                }
            }
            Err(skip) => {
                tracing::warn!(?skip, "L3 AutoCompact 未落地");
                AutoCompactOutcome {
                    decision: AutoCompactDecision::Attempt,
                    attempted: true,
                    executed: false,
                    result: None,
                }
            }
        }
    }

    /// 执行错误恢复级联——L4 + L5（逐条对照旧
    /// `executeErrorRecoveryCascade`，在旧 `QueryEngine` 捕获 413 后调用）。
    ///
    /// 返回 `Some((messages, level))` 表示已产出可重试的更小上下文；`None`
    /// 表示所有恢复策略耗尽（旧返回 `null`）。相比旧签名额外回传落地层级，
    /// 供调用方推下行与打点。
    #[must_use]
    pub fn execute_error_recovery_cascade(
        &self,
        messages: &[ChatMessage],
        model: &str,
        context_window: u32,
        has_attempted_reactive: bool,
    ) -> Option<(Vec<ChatMessage>, CascadeLevel)> {
        // L4 CollapseDrain：以半窗口为目标的激进压缩。
        let drain_window = scale_tokens(context_window, 0.5);
        match compact_messages(
            messages,
            model,
            drain_window,
            true,
            self.summarizer.as_ref(),
        ) {
            Ok(result) => {
                tracing::info!(
                    before = result.before_tokens,
                    after = result.after_tokens,
                    "L4 CollapseDrain 落地"
                );
                return Some((result.messages, CascadeLevel::CollapseDrain));
            }
            Err(skip) => tracing::warn!(?skip, "L4 CollapseDrain 未落地"),
        }

        // L5 ReactiveCompact：最后手段，单次。
        if !has_attempted_reactive {
            match reactive_compact(
                messages,
                model,
                context_window,
                false,
                self.summarizer.as_ref(),
            ) {
                Ok(result) => {
                    tracing::info!(
                        before = result.before_tokens,
                        after = result.after_tokens,
                        "L5 ReactiveCompact 落地"
                    );
                    return Some((result.messages, CascadeLevel::ReactiveCompact));
                }
                Err(skip) => tracing::warn!(?skip, "L5 ReactiveCompact 未落地"),
            }
        }

        tracing::error!("ContextCascade: 所有恢复策略已耗尽");
        None
    }
}

impl AutoCompactOutcome {
    fn skipped(decision: AutoCompactDecision) -> Self {
        Self {
            decision,
            attempted: false,
            executed: false,
            result: None,
        }
    }
}

/// L0 的工具结果字符预算（逐字对照旧 `contextWindow × 0.3 × tokenCharRatio`）。
#[must_use]
pub fn tool_result_budget_chars(context_window: u32, model: &str) -> usize {
    let budget = f64::from(context_window) * TOOL_RESULT_BUDGET_RATIO * token_char_ratio(model);
    usize::try_from(saturating_tokens(budget)).unwrap_or(usize::MAX)
}

/// L0：单条内容首尾截断（逐条对照旧 `SnipService.snipIfNeeded`）。
///
/// 返回 `None` 表示无需截断（长度未超预算）。头部取预算的一半，尾部取
/// 「预算 - 头部 - 80」（预留标记空间）；尾部无空间时直接取前 `budget_chars`
/// 个字符。字符边界按 Unicode 标量切分（旧 Java 按 UTF-16 码元）。
#[must_use]
pub fn snip_if_needed(content: &str, budget_chars: usize) -> Option<String> {
    let total = content.chars().count();
    if total <= budget_chars {
        return None;
    }
    if budget_chars == 0 {
        return Some(String::new());
    }
    let head_size = budget_chars / 2;
    let tail_size = budget_chars
        .saturating_sub(head_size)
        .saturating_sub(TRUNCATION_MARKER_RESERVE);
    if tail_size == 0 {
        return Some(take_chars(content, budget_chars));
    }
    let head = take_chars(content, head_size);
    let tail = last_chars(content, tail_size);
    Some(format!(
        "{head}\n\n[... content truncated ({total}/{budget_chars} chars) ...]\n\n{tail}"
    ))
}

/// L0：遍历消息列表对超预算的工具结果应用截断（逐条对照旧
/// `SnipService.snipToolResults`）。
#[must_use]
pub fn snip_tool_results(messages: &[ChatMessage], budget_chars: usize) -> Vec<ChatMessage> {
    messages
        .iter()
        .map(|message| {
            if message.role != Role::Tool {
                return message.clone();
            }
            match snip_if_needed(&message.content, budget_chars) {
                Some(snipped) => {
                    let mut next = message.clone();
                    next.content = snipped;
                    next
                }
                None => message.clone(),
            }
        })
        .collect()
}

/// L1 结果（逐字段对照旧 `MicroCompactService.MicroCompactResult`）。
#[derive(Debug, Clone)]
pub struct MicroCompactResult {
    /// 微压缩后消息序列。
    pub messages: Vec<ChatMessage>,
    /// 释放的估算 token 数。
    pub tokens_freed: u32,
    /// 被清除的工具结果条数。
    pub cleared_count: usize,
}

/// L1：清除保护尾部之前的白名单工具结果（逐条对照旧
/// `MicroCompactService.compactMessages`）。
///
/// 预扫描 assistant 的 `tool_calls` 建立「白名单工具 → 调用 ID」集合；仅当
/// 工具结果的 `tool_call_id` 命中该集合时才清除。无法确定工具名的结果一律
/// 保守保留（旧同）。
#[must_use]
pub fn micro_compact(
    messages: &[ChatMessage],
    protected_tail: usize,
    model: &str,
) -> MicroCompactResult {
    let compactable = collect_compactable_tool_ids(messages);
    let boundary = messages.len().saturating_sub(protected_tail);
    let mut tokens_freed = 0_u32;
    let mut cleared_count = 0_usize;
    let mut result = Vec::with_capacity(messages.len());

    for (index, message) in messages.iter().enumerate() {
        let clearable = index < boundary
            && message.role == Role::Tool
            && message.content != CLEARED_MESSAGE
            && message
                .tool_call_id
                .as_ref()
                .is_some_and(|id| compactable.contains(id.as_str()));
        if clearable {
            tokens_freed = tokens_freed.saturating_add(string_tokens(&message.content, model));
            cleared_count += 1;
            let mut next = message.clone();
            CLEARED_MESSAGE.clone_into(&mut next.content);
            result.push(next);
        } else {
            result.push(message.clone());
        }
    }

    if tokens_freed > 0 {
        tracing::debug!(
            cleared_count,
            tokens_freed,
            "L1 MicroCompact 清除旧工具结果"
        );
    }
    MicroCompactResult {
        messages: result,
        tokens_freed,
        cleared_count,
    }
}

/// 预扫描白名单工具的调用 ID（旧 `collectCompactableToolIds`）。
fn collect_compactable_tool_ids(messages: &[ChatMessage]) -> std::collections::HashSet<&str> {
    let mut ids = std::collections::HashSet::new();
    for message in messages {
        for call in &message.tool_calls {
            if COMPACTABLE_TOOLS.contains(&call.name.as_str()) {
                ids.insert(call.id.as_str());
            }
        }
    }
    ids
}

/// 单串 token 估算（旧 `TokenCounter.estimateTokens(String)`）。
fn string_tokens(text: &str, model: &str) -> u32 {
    let chars = char_count(text);
    saturating_tokens(super::as_f64(chars) / token_char_ratio(model))
}

/// 前 `count` 个字符。
fn take_chars(text: &str, count: usize) -> String {
    text.chars().take(count).collect()
}

/// 后 `count` 个字符。
fn last_chars(text: &str, count: usize) -> String {
    let total = text.chars().count();
    text.chars().skip(total.saturating_sub(count)).collect()
}

/// 折叠级别（逐字对照旧 `CollapseLevel` sealed interface 的三个实现）。
///
/// 覆盖区间按「距尾部距离 < `max_age_messages`」自前向后首次命中判定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollapseLevel {
    /// Level A：尾部 10 条完整保留（旧 `FullRetention(10)`）。
    FullRetention,
    /// Level B：倒数 10-30 条，长文本保留前 500 字符（旧 `SummaryRetention(30, 500)`）。
    SummaryRetention,
    /// Level C：30 条以前，骨架化为首行摘要（旧 `SkeletonRetention(MAX_VALUE)`）。
    SkeletonRetention,
}

/// 默认级别序列（旧 `ContextCollapseService.DEFAULT_LEVELS`）。
pub const DEFAULT_COLLAPSE_LEVELS: [CollapseLevel; 3] = [
    CollapseLevel::FullRetention,
    CollapseLevel::SummaryRetention,
    CollapseLevel::SkeletonRetention,
];

impl CollapseLevel {
    /// 该级别覆盖的最大「距尾部距离」（旧 `maxAgeMessages()`）。
    #[must_use]
    pub fn max_age_messages(self) -> usize {
        match self {
            Self::FullRetention => 10,
            Self::SummaryRetention => 30,
            Self::SkeletonRetention => usize::MAX,
        }
    }

    /// 对内容执行折叠；`None` 表示原样保留（旧 `collapse(originalContent)`
    /// 返回入参的等价表达）。
    #[must_use]
    pub fn collapse(self, content: &str) -> Option<String> {
        match self {
            Self::FullRetention => None,
            Self::SummaryRetention => {
                let total = content.chars().count();
                if total <= SUMMARY_KEEP_CHARS {
                    return None;
                }
                Some(format!(
                    "{}\n...[summary-collapsed: {total} chars]",
                    take_chars(content, SUMMARY_KEEP_CHARS)
                ))
            }
            Self::SkeletonRetention => {
                let total = content.chars().count();
                if total <= SKELETON_MIN_CHARS || content.starts_with(SKELETON_PREFIX) {
                    return None;
                }
                let first_line = content
                    .split('\n')
                    .next()
                    .map_or(content, |line| line)
                    .chars()
                    .take(SKELETON_FIRST_LINE_CHARS)
                    .collect::<String>();
                let candidate = format!("{SKELETON_PREFIX}{first_line}...");
                if candidate.chars().count() < total {
                    Some(candidate)
                } else {
                    None
                }
            }
        }
    }
}

/// L2 结果（逐字段对照旧 `ContextCollapseService.CollapseResult`）。
#[derive(Debug, Clone)]
pub struct CollapseResult {
    /// 折叠后消息序列。
    pub messages: Vec<ChatMessage>,
    /// 被折叠的消息条数。
    pub collapsed_count: usize,
    /// 估算释放字符数。
    pub estimated_chars_freed: u64,
}

/// L2：三级渐进折叠（逐条对照旧
/// `ContextCollapseService.progressiveCollapse`）。
///
/// 关键规则（旧原文注释）：**所有非工具结果的用户消息永远保留原文**，防止模型
/// 丢失用户反馈（如「不要用 Redux」）。在新模型里即 [`Role::User`] 一律跳过。
///
/// 每条候选消息折叠后都做「确实变短」校验（旧 `charsFreed <= 0` 则回退原文），
/// 故骨架化反而变长的退化情形不会落地。
#[must_use]
pub fn progressive_collapse(messages: &[ChatMessage]) -> CollapseResult {
    if messages.is_empty() {
        return CollapseResult {
            messages: Vec::new(),
            collapsed_count: 0,
            estimated_chars_freed: 0,
        };
    }
    let total = messages.len();
    let mut result = Vec::with_capacity(total);
    let mut collapsed_count = 0_usize;
    let mut estimated_chars_freed = 0_u64;

    for (index, message) in messages.iter().enumerate() {
        // 规则：纯用户消息永远保留原文。
        if message.role == Role::User {
            result.push(message.clone());
            continue;
        }
        let distance_from_tail = total - 1 - index;
        let level = DEFAULT_COLLAPSE_LEVELS
            .iter()
            .copied()
            .find(|level| distance_from_tail < level.max_age_messages())
            .unwrap_or(CollapseLevel::SkeletonRetention);
        if level == CollapseLevel::FullRetention {
            result.push(message.clone());
            continue;
        }
        match collapse_message(message, level) {
            Some(collapsed) => {
                let before = collapse_relevant_chars(message);
                let after = collapse_relevant_chars(&collapsed);
                if after >= before {
                    result.push(message.clone());
                } else {
                    estimated_chars_freed += before - after;
                    collapsed_count += 1;
                    result.push(collapsed);
                }
            }
            None => result.push(message.clone()),
        }
    }

    if collapsed_count > 0 {
        tracing::debug!(
            collapsed_count,
            estimated_chars_freed,
            "L2 ContextCollapse 渐进折叠"
        );
    }
    CollapseResult {
        messages: result,
        collapsed_count,
        estimated_chars_freed,
    }
}

/// 单条消息折叠（旧 `collapseMessage`：仅动内容，保留 role 与
/// `tool_call_id` / `tool_calls` 结构）。
fn collapse_message(message: &ChatMessage, level: CollapseLevel) -> Option<ChatMessage> {
    if !matches!(message.role, Role::Assistant | Role::Tool) {
        return None;
    }
    let collapsed = level.collapse(&message.content)?;
    let mut next = message.clone();
    next.content = collapsed;
    Some(next)
}

/// 折叠统计口径的字符数（旧 `estimateMessageChars`：只计助手文本与工具结果
/// 正文，工具调用块与 system 消息不计）。
fn collapse_relevant_chars(message: &ChatMessage) -> u64 {
    if matches!(message.role, Role::Assistant | Role::Tool) {
        char_count(&message.content)
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AutoCompactDecision, AutoCompactTrackingState, CLEARED_MESSAGE, CascadeLevel,
        CollapseLevel, ContextCascade, MAX_CONSECUTIVE_FAILURES, micro_compact,
        progressive_collapse, snip_if_needed, snip_tool_results, tool_result_budget_chars,
    };
    use zk_llm::{ChatMessage, Role, ToolCallRequest};

    const MODEL: &str = "kimi-k3";

    fn call(id: &str, name: &str) -> ToolCallRequest {
        ToolCallRequest {
            id: id.to_owned(),
            name: name.to_owned(),
            arguments: "{}".to_owned(),
        }
    }

    #[test]
    fn snip_keeps_head_and_tail_with_marker() {
        let content = "H".repeat(100) + &"T".repeat(100);
        let snipped = snip_if_needed(&content, 120).expect("超预算应截断");
        // head = 60，tail = 120 - 60 - 80 → 饱和为 0 → 直接取前 120 字符。
        assert_eq!(snipped.chars().count(), 120);
        // 预算足够容纳标记时保留首尾。
        let snipped = snip_if_needed(&content, 180).expect("超预算应截断");
        assert!(snipped.starts_with("HHH"));
        assert!(snipped.ends_with("TTT"));
        assert!(snipped.contains("content truncated (200/180 chars)"));
    }

    #[test]
    fn snip_is_noop_within_budget() {
        assert!(snip_if_needed("short", 100).is_none());
        assert!(snip_if_needed("", 0).is_none());
        // 预算 0 且内容非空 → 空串（旧 budgetChars <= 0 分支）。
        assert_eq!(snip_if_needed("x", 0), Some(String::new()));
    }

    #[test]
    fn snip_respects_char_boundaries_for_multibyte_content() {
        let content = "中".repeat(300);
        let snipped = snip_if_needed(&content, 200).expect("超预算应截断");
        // 未 panic 即证明按字符边界切分；且首尾都是完整汉字。
        assert!(snipped.starts_with('中'));
        assert!(snipped.ends_with('中'));
    }

    #[test]
    fn snip_tool_results_only_touches_tool_messages() {
        let long = "x".repeat(500);
        let messages = vec![
            ChatMessage::user(long.clone()),
            ChatMessage::assistant(long.clone()),
            ChatMessage::tool("t1", long.clone()),
        ];
        let snipped = snip_tool_results(&messages, 200);
        assert_eq!(snipped[0].content.chars().count(), 500);
        assert_eq!(snipped[1].content.chars().count(), 500);
        assert!(snipped[2].content.chars().count() < 500);
        assert_eq!(snipped[2].tool_call_id.as_deref(), Some("t1"));
    }

    #[test]
    fn tool_result_budget_follows_window_and_ratio() {
        // kimi-k3：262144 × 0.3 × 3.5 = 275251.2 → 向上取整 275252。
        assert_eq!(tool_result_budget_chars(262_144, MODEL), 275_252);
    }

    #[test]
    fn micro_compact_clears_whitelisted_old_tool_results() {
        let mut messages = vec![
            ChatMessage::assistant_tool_calls("", vec![call("t1", "Read")]),
            ChatMessage::tool("t1", "y".repeat(400)),
        ];
        // 尾部填充 10 条使前两条落在 boundary 之前。
        for index in 0..10 {
            messages.push(ChatMessage::user(format!("u{index}")));
        }
        let result = micro_compact(&messages, 10, MODEL);
        assert_eq!(result.cleared_count, 1);
        assert!(result.tokens_freed > 0);
        assert_eq!(result.messages[1].content, CLEARED_MESSAGE);
        // tool_call_id 结构保留。
        assert_eq!(result.messages[1].tool_call_id.as_deref(), Some("t1"));
    }

    #[test]
    fn micro_compact_never_clears_non_whitelisted_or_protected_tail() {
        // 非白名单工具：不清除。
        let mut messages = vec![
            ChatMessage::assistant_tool_calls("", vec![call("t1", "MysteryTool")]),
            ChatMessage::tool("t1", "y".repeat(400)),
        ];
        for index in 0..10 {
            messages.push(ChatMessage::user(format!("u{index}")));
        }
        assert_eq!(micro_compact(&messages, 10, MODEL).cleared_count, 0);
        // 白名单但落在保护尾部：不清除。
        let protected = vec![
            ChatMessage::assistant_tool_calls("", vec![call("t1", "Read")]),
            ChatMessage::tool("t1", "y".repeat(400)),
        ];
        assert_eq!(micro_compact(&protected, 10, MODEL).cleared_count, 0);
    }

    #[test]
    fn micro_compact_is_idempotent() {
        let mut messages = vec![
            ChatMessage::assistant_tool_calls("", vec![call("t1", "Bash")]),
            ChatMessage::tool("t1", "y".repeat(400)),
        ];
        for index in 0..10 {
            messages.push(ChatMessage::user(format!("u{index}")));
        }
        let once = micro_compact(&messages, 10, MODEL);
        let twice = micro_compact(&once.messages, 10, MODEL);
        assert_eq!(twice.cleared_count, 0);
        assert_eq!(twice.tokens_freed, 0);
    }

    #[test]
    fn collapse_levels_match_legacy_thresholds() {
        assert_eq!(CollapseLevel::FullRetention.max_age_messages(), 10);
        assert_eq!(CollapseLevel::SummaryRetention.max_age_messages(), 30);
        assert!(CollapseLevel::FullRetention.collapse("anything").is_none());
        // Summary：>500 字符截断并加尾注。
        let long = "x".repeat(600);
        let summary = CollapseLevel::SummaryRetention
            .collapse(&long)
            .expect("应折叠");
        assert!(summary.contains("summary-collapsed: 600 chars"));
        assert!(CollapseLevel::SummaryRetention.collapse("x").is_none());
        // Skeleton：首行 + 前缀；已骨架化的内容不再处理。
        let skeleton = CollapseLevel::SkeletonRetention
            .collapse(&format!("first line\n{}", "x".repeat(200)))
            .expect("应折叠");
        assert!(skeleton.starts_with("[skeleton] first line"));
        assert!(
            CollapseLevel::SkeletonRetention
                .collapse(&skeleton)
                .is_none()
        );
        assert!(CollapseLevel::SkeletonRetention.collapse("short").is_none());
    }

    #[test]
    fn progressive_collapse_always_preserves_user_messages() {
        let mut messages = Vec::new();
        for index in 0..40 {
            messages.push(ChatMessage::user(format!("用户反馈 {index} 不要用 Redux")));
            messages.push(ChatMessage::assistant("a".repeat(1_000)));
        }
        let result = progressive_collapse(&messages);
        assert!(result.collapsed_count > 0);
        assert!(result.estimated_chars_freed > 0);
        for (before, after) in messages.iter().zip(result.messages.iter()) {
            if before.role == Role::User {
                assert_eq!(before.content, after.content, "用户消息必须保留原文");
            }
        }
    }

    #[test]
    fn progressive_collapse_keeps_tail_ten_intact() {
        let mut messages = Vec::new();
        for _ in 0..40 {
            messages.push(ChatMessage::assistant("a".repeat(1_000)));
        }
        let result = progressive_collapse(&messages);
        // 距尾部 < 10 的消息完整保留。
        for index in messages.len() - 10..messages.len() {
            assert_eq!(result.messages[index].content.chars().count(), 1_000);
        }
        // 更老的消息被折叠。
        assert!(result.messages[0].content.chars().count() < 1_000);
    }

    #[test]
    fn progressive_collapse_handles_empty_input() {
        let result = progressive_collapse(&[]);
        assert!(result.messages.is_empty());
        assert_eq!(result.collapsed_count, 0);
    }

    #[test]
    fn tracking_state_transitions_and_circuit_breaker() {
        let mut state = AutoCompactTrackingState::initial();
        assert!(!state.is_circuit_broken());
        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            state = state.with_failure();
        }
        assert!(state.is_circuit_broken());
        let recovered = state.with_success("turn-7");
        assert!(!recovered.is_circuit_broken());
        assert!(recovered.compacted_this_turn);
        assert_eq!(recovered.turn_counter, 1);
        assert_eq!(recovered.last_turn_id, "turn-7");
        // 关闭态恒熔断（旧 autoCompact 关闭时传 MAX_VALUE）。
        assert!(AutoCompactTrackingState::disabled().is_circuit_broken());
    }

    #[test]
    fn pre_api_cascade_is_noop_for_small_context() {
        let cascade = ContextCascade::new();
        let messages = vec![ChatMessage::user("hi"), ChatMessage::assistant("yo")];
        let result = cascade.execute_pre_api_cascade(
            messages.clone(),
            MODEL,
            &AutoCompactTrackingState::initial(),
        );
        assert_eq!(result.messages, messages);
        assert!(result.levels_applied().is_empty());
        assert_eq!(result.total_tokens_freed(), 0);
        assert_eq!(
            result.auto_compact_decision,
            AutoCompactDecision::NotNeededNoCollapse
        );
    }

    #[test]
    fn pre_api_cascade_applies_lower_levels_on_long_history() {
        let cascade = ContextCascade::new();
        let mut messages = vec![ChatMessage::system("sys")];
        for index in 0..40 {
            messages.push(ChatMessage::assistant_tool_calls(
                "",
                vec![call(&format!("t{index}"), "Read")],
            ));
            messages.push(ChatMessage::tool(format!("t{index}"), "r".repeat(2_000)));
        }
        let result =
            cascade.execute_pre_api_cascade(messages, MODEL, &AutoCompactTrackingState::initial());
        let levels = result.levels_applied();
        assert!(levels.contains(&CascadeLevel::MicroCompact));
        assert!(result.total_tokens_freed() > 0);
        assert!(result.summary().contains("ContextCascade:"));
    }

    #[test]
    fn pre_api_cascade_respects_circuit_breaker() {
        let cascade = ContextCascade::new();
        let mut messages = vec![ChatMessage::system("sys")];
        // 6 条 400K 字符用户消息 ≈ 685K tokens，稳超阈值 650_000（kimi-k3 1M 窗口）。
        for index in 0..6 {
            messages.push(ChatMessage::user(format!(
                "u{index} {}",
                "x".repeat(400_000)
            )));
            messages.push(ChatMessage::assistant("a"));
        }
        let result =
            cascade.execute_pre_api_cascade(messages, MODEL, &AutoCompactTrackingState::disabled());
        assert_eq!(
            result.auto_compact_decision,
            AutoCompactDecision::CircuitOpen
        );
        assert!(!result.auto_compact_attempted);
    }

    #[test]
    fn pre_api_cascade_attempts_auto_compact_above_threshold() {
        let cascade = ContextCascade::new();
        let mut messages = vec![ChatMessage::system("sys")];
        // 6 条 400K 字符用户消息 ≈ 685K tokens，稳超阈值 650_000（kimi-k3 1M 窗口）。
        for index in 0..6 {
            messages.push(ChatMessage::user(format!(
                "u{index} {}",
                "x".repeat(400_000)
            )));
            messages.push(ChatMessage::assistant("a"));
        }
        let result =
            cascade.execute_pre_api_cascade(messages, MODEL, &AutoCompactTrackingState::initial());
        assert_eq!(result.auto_compact_decision, AutoCompactDecision::Attempt);
        assert!(result.auto_compact_attempted);
        assert!(result.auto_compact_executed);
        assert!(result.final_tokens < result.original_tokens);
    }

    #[test]
    fn error_recovery_cascade_drains_then_reacts() {
        let cascade = ContextCascade::new();
        let mut messages = vec![ChatMessage::system("sys")];
        for index in 0..20 {
            messages.push(ChatMessage::user(format!("u{index} {}", "x".repeat(2_000))));
            messages.push(ChatMessage::assistant("a".repeat(2_000)));
        }
        let (recovered, level) = cascade
            .execute_error_recovery_cascade(&messages, MODEL, 262_144, false)
            .expect("应可恢复");
        assert_eq!(level, CascadeLevel::CollapseDrain);
        assert!(recovered.len() < messages.len());
    }

    #[test]
    fn error_recovery_cascade_exhausts_on_tiny_history() {
        let cascade = ContextCascade::new();
        let messages = vec![ChatMessage::user("hi")];
        assert!(
            cascade
                .execute_error_recovery_cascade(&messages, MODEL, 262_144, true)
                .is_none()
        );
    }
}
