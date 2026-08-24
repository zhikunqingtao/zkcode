//! Agent Loop 终止策略——每轮末尾评估是否继续 / 终止 / 请求用户输入。
//!
//! 对齐旧 `engine/strategy/TerminationStrategy.java`（接口 + `LoopContext` /
//! `ToolCallRecord` record）、`engine/strategy/TerminationDecision.java`（枚举）
//! 与 `engine/strategy/DefaultTerminationStrategy.java`（默认实现）。
//!
//! # 与 Java 基线的有意差异（已核对，本 Batch 决策）
//!
//! 1. **无 `SWITCH_STRATEGY` 决策**——旧 `TerminationDecision` 有 6 个值，其中
//!    `SWITCH_STRATEGY` 服务于旧仓「多策略切换」框架（连续错误 ≥3 时切换恢复
//!    策略）。Rust 端未移植策略切换框架，故本枚举仅 5 值；连续错误改为直接以
//!    更高阈值（10）触发 [`TerminationDecision::TerminateError`]。
//! 2. **固定阈值取代旧动态/比例阈值**——旧默认策略用 `TOKEN_BUDGET_THRESHOLD
//!    =0.95`、`CONSECUTIVE_ERROR_THRESHOLD=3`（→SWITCH）、动态
//!    `maxTurns + errors*2`。本 Batch 按任务规格改为一组固定判据：
//!    - `token_budget > 0 && total_tokens >= token_budget` → `TerminateBudget`
//!      （旧为 `> budget*0.95`；此处收紧到「用满即止」）；
//!    - `consecutive_errors >= 10` → `TerminateError`（旧 3→SWITCH）；
//!    - `turn >= max_turns` → `TerminateError`（旧为动态上界）；
//!    - `is_window_all_failed(5)` → `RequestUserInput`（与旧一致）。
//! 3. **正常成功判据保留**——`stop_reason ∈ {end_turn, stop} && !has_tool_calls`
//!    → `TerminateSuccess`，逐字对齐旧 evaluate 第 3 分支。
//!
//! # 评估优先级
//!
//! 预算 → 连续错误 → 正常成功 → 轮次上界 → 滑动窗口 → 继续。与旧 evaluate
//! 的短路顺序一致（仅把 `SWITCH_STRATEGY` 分支替换为 `TerminateError`）。

use crate::tool_tracker::ToolCallRecord;

/// 连续错误阈值——达到即以错误终止（本 Batch 固定值，旧为 `3→SWITCH_STRATEGY`）。
pub const CONSECUTIVE_ERROR_THRESHOLD: u32 = 10;

/// 滑动窗口大小——最近 N 次恢复相关工具调用（对齐旧 `SLIDING_WINDOW_SIZE`）。
pub const SLIDING_WINDOW_SIZE: usize = 5;

/// 终止决策——对齐旧 `TerminationDecision`（去 `SWITCH_STRATEGY`，见模块文档）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminationDecision {
    /// 继续下一轮。
    Continue,
    /// 正常成功终止（模型 `end_turn`/`stop` 且无工具调用）。
    TerminateSuccess,
    /// Token 预算耗尽终止。
    TerminateBudget,
    /// 错误终止（连续错误超阈 / 轮次上界）。
    TerminateError,
    /// 请求用户输入（滑动窗口全失败）。
    RequestUserInput,
}

/// Agent Loop 上下文——封装终止评估所需的全部状态。
///
/// 对齐旧 `TerminationStrategy.LoopContext`（Java record）。字段命名按 Rust
/// 习惯（`current_turn` → `turn`、`last_response_has_tool_calls` →
/// `has_tool_calls`），语义逐一对应。
#[derive(Clone, Debug)]
pub struct LoopContext {
    /// 当前轮次（从 1 计）。
    pub turn: u32,
    /// 最大轮次限制。
    pub max_turns: u32,
    /// 连续错误计数。
    pub consecutive_errors: u32,
    /// 本轮工具调用数。
    pub tool_calls_this_turn: u32,
    /// 上一次响应是否包含工具调用。
    pub has_tool_calls: bool,
    /// API 返回的停止原因。
    pub stop_reason: Option<String>,
    /// 已使用的总 token 数。
    pub total_tokens: u64,
    /// token 预算上限（0 表示不设限）。
    pub token_budget: u64,
    /// 最近的工具调用记录（用于滑动窗口判定）。
    pub recent_records: Vec<ToolCallRecord>,
}

/// 判定最近 `window` 次**恢复相关**记录是否全部失败。
///
/// 对齐旧 `DefaultTerminationStrategy` 第 5 分支：先按 `recovery_relevant` 过滤，
/// 恢复相关记录不足 `window` 时返回 `false`，否则要求尾窗无任何成功。
fn window_all_failed(records: &[ToolCallRecord], window: usize) -> bool {
    if window == 0 {
        return false;
    }
    let relevant: Vec<&ToolCallRecord> = records.iter().filter(|r| r.recovery_relevant).collect();
    if relevant.len() < window {
        return false;
    }
    relevant[relevant.len() - window..]
        .iter()
        .all(|r| !r.success)
}

/// 评估当前循环状态，返回终止决策。
///
/// 对齐旧 `DefaultTerminationStrategy.evaluate`（短路顺序见模块文档）。
#[must_use]
pub fn evaluate(ctx: &LoopContext) -> TerminationDecision {
    // 1. Token 预算：用满即止（旧为 >budget*0.95，本 Batch 收紧）。
    if ctx.token_budget > 0 && ctx.total_tokens >= ctx.token_budget {
        return TerminationDecision::TerminateBudget;
    }

    // 2. 连续错误：达阈直接错误终止（旧 3→SWITCH_STRATEGY，本 Batch 10→ERROR）。
    if ctx.consecutive_errors >= CONSECUTIVE_ERROR_THRESHOLD {
        return TerminationDecision::TerminateError;
    }

    // 3. 正常成功：stop_reason end_turn/stop + 无工具调用。
    if matches!(ctx.stop_reason.as_deref(), Some("end_turn" | "stop")) && !ctx.has_tool_calls {
        return TerminationDecision::TerminateSuccess;
    }

    // 4. 轮次上界（旧为动态 max + errors*2，本 Batch 固定 max_turns）。
    if ctx.turn >= ctx.max_turns {
        return TerminationDecision::TerminateError;
    }

    // 5. 滑动窗口全失败 → 请求用户输入。
    if window_all_failed(&ctx.recent_records, SLIDING_WINDOW_SIZE) {
        return TerminationDecision::RequestUserInput;
    }

    TerminationDecision::Continue
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn base_ctx() -> LoopContext {
        LoopContext {
            turn: 1,
            max_turns: 1024,
            consecutive_errors: 0,
            tool_calls_this_turn: 0,
            has_tool_calls: false,
            stop_reason: None,
            total_tokens: 0,
            token_budget: 0,
            recent_records: Vec::new(),
        }
    }

    fn failed(recovery_relevant: bool) -> ToolCallRecord {
        ToolCallRecord {
            tool_name: "Bash".to_owned(),
            success: false,
            error_message: Some("e".to_owned()),
            timestamp: Instant::now(),
            recovery_relevant,
        }
    }

    #[test]
    fn budget_exhausted_terminates_budget() {
        let mut ctx = base_ctx();
        ctx.token_budget = 100;
        ctx.total_tokens = 100;
        assert_eq!(evaluate(&ctx), TerminationDecision::TerminateBudget);
        // 未设限（budget=0）不触发。
        ctx.token_budget = 0;
        assert_ne!(evaluate(&ctx), TerminationDecision::TerminateBudget);
    }

    #[test]
    fn consecutive_errors_terminate_error() {
        let mut ctx = base_ctx();
        ctx.consecutive_errors = CONSECUTIVE_ERROR_THRESHOLD;
        assert_eq!(evaluate(&ctx), TerminationDecision::TerminateError);
        ctx.consecutive_errors = CONSECUTIVE_ERROR_THRESHOLD - 1;
        assert_ne!(evaluate(&ctx), TerminationDecision::TerminateError);
    }

    #[test]
    fn success_when_stop_and_no_tool_calls() {
        let mut ctx = base_ctx();
        ctx.stop_reason = Some("end_turn".to_owned());
        ctx.has_tool_calls = false;
        assert_eq!(evaluate(&ctx), TerminationDecision::TerminateSuccess);
        // 有工具调用则不算成功终止。
        ctx.has_tool_calls = true;
        assert_eq!(evaluate(&ctx), TerminationDecision::Continue);
    }

    #[test]
    fn max_turns_terminate_error() {
        let mut ctx = base_ctx();
        ctx.turn = 10;
        ctx.max_turns = 10;
        assert_eq!(evaluate(&ctx), TerminationDecision::TerminateError);
    }

    #[test]
    fn window_all_failed_requests_user_input() {
        let mut ctx = base_ctx();
        ctx.recent_records = (0..SLIDING_WINDOW_SIZE).map(|_| failed(true)).collect();
        assert_eq!(evaluate(&ctx), TerminationDecision::RequestUserInput);
        // 非恢复相关的失败不计入窗口。
        ctx.recent_records = (0..SLIDING_WINDOW_SIZE).map(|_| failed(false)).collect();
        assert_eq!(evaluate(&ctx), TerminationDecision::Continue);
    }

    #[test]
    fn continues_by_default() {
        assert_eq!(evaluate(&base_ctx()), TerminationDecision::Continue);
    }
}
