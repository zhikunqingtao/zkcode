//! 工具调用追踪器——记录工具调用历史并追踪连续错误状态。
//!
//! 对齐旧 `engine/tracking/ToolCallTracker.java`（137 行）与其内嵌记录类型
//! `engine/strategy/TerminationStrategy.ToolCallRecord`（Java record）。
//!
//! # 职责
//!
//! - 记录每次工具调用结果（成功/失败）与是否属于「需触发故障恢复」的错误；
//! - 追踪连续错误计数（成功或非恢复相关错误时重置）；
//! - 提供滑动窗口检查（最近 N 次恢复相关调用是否全失败）；
//! - 检测相同错误是否连续重复。
//!
//! # 并发策略（与 Java 的有意差异，已核对）
//!
//! 旧实现用 `Collections.synchronizedList` + `AtomicInteger` 防跨线程串扰，因其
//! 在 Spring 多线程栈中被并发触及。本实现每个 run 顺序驱动其循环（单 run 内工具
//! 顺序执行、无并发 record），故 [`ToolCallTracker`] **无锁**：由调用方 `&mut self`
//! 独占保证时序安全，等价于旧实现的临界区语义。每个查询会话应持有独立实例
//! （在引擎循环开头 `new` 创建），避免跨会话状态串扰。
//!
//! # `recovery_relevant` 语义（逐字对齐旧 record）
//!
//! 用户拒绝等「预期权限终态」为 `false`——不计入 `consecutive_errors`、不参与滑动
//! 窗口/相同错误统计；真正需要 Agent 故障恢复的系统/工具错误为 `true`。旧
//! `record(name, ToolResult)` 依 `failureType() != PERMISSION` 推导，本实现由调用
//! 方在 record 时显式传入（见 [`ToolCallTracker::record_with_recovery`]），双参
//! 便捷入口 [`ToolCallTracker::record`] 沿用旧 `record(name, success, err)` 的
//! `recoveryRelevant = !success` 缺省。

use std::time::Instant;

/// 工具调用记录——追踪单次工具执行的结果。
///
/// 逐字对齐旧 `TerminationStrategy.ToolCallRecord`（Java record）。
#[derive(Clone, Debug)]
pub struct ToolCallRecord {
    /// 工具名称。
    pub tool_name: String,
    /// 是否成功。
    pub success: bool,
    /// 错误信息（成功时为 `None`）。
    pub error_message: Option<String>,
    /// 执行时间戳。
    pub timestamp: Instant,
    /// 是否属于需要触发 Agent 故障恢复的系统/工具错误；用户拒绝等预期权限
    /// 终态为 `false`。
    pub recovery_relevant: bool,
}

/// 工具调用追踪器——单 run 内顺序驱动，无锁。
///
/// 对齐旧 `ToolCallTracker`：`history` 全量记录、`consecutive_errors` 随成功/
/// 非恢复相关错误重置。
#[derive(Debug, Default)]
pub struct ToolCallTracker {
    history: Vec<ToolCallRecord>,
    consecutive_errors: u32,
}

impl ToolCallTracker {
    /// 新建空追踪器（等价旧构造：空 history + 计数 0）。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 记录一次工具调用结果（便捷入口）。
    ///
    /// 对齐旧 `record(toolName, success, errorMessage)`，委托至
    /// [`record_with_recovery`](Self::record_with_recovery) 并以 `recovery_relevant
    /// = !success` 缺省（旧 `record(name, success, err)` 的 `!success` 语义）。
    pub fn record(
        &mut self,
        tool_name: impl Into<String>,
        success: bool,
        error_message: Option<String>,
    ) {
        let recovery_relevant = !success;
        self.record_with_recovery(tool_name, success, error_message, recovery_relevant);
    }

    /// 记录一次工具调用结果并显式给定 `recovery_relevant`。
    ///
    /// 对齐旧私有 `record(toolName, success, errorMessage, recoveryRelevant)`：
    /// 成功或非恢复相关错误 → 连续错误计数归零；否则递增。
    pub fn record_with_recovery(
        &mut self,
        tool_name: impl Into<String>,
        success: bool,
        error_message: Option<String>,
        recovery_relevant: bool,
    ) {
        self.history.push(ToolCallRecord {
            tool_name: tool_name.into(),
            success,
            error_message,
            timestamp: Instant::now(),
            recovery_relevant,
        });
        if success || !recovery_relevant {
            self.consecutive_errors = 0;
        } else {
            self.consecutive_errors += 1;
        }
    }

    /// 获取连续错误计数（对齐旧 `getConsecutiveErrors`）。
    #[must_use]
    pub fn consecutive_errors(&self) -> u32 {
        self.consecutive_errors
    }

    /// 获取总调用记录数（对齐旧 `getTotalRecords`）。
    #[must_use]
    pub fn total_records(&self) -> usize {
        self.history.len()
    }

    /// 获取最近 N 条记录（不足 N 条时返回全部）。
    ///
    /// 对齐旧 `getRecentRecords`：`start = max(0, size - n)`。
    #[must_use]
    pub fn recent_records(&self, n: usize) -> &[ToolCallRecord] {
        let start = self.history.len().saturating_sub(n);
        &self.history[start..]
    }

    /// 检查滑动窗口——最近 `window_size` 次**恢复相关**调用是否全部失败。
    ///
    /// 对齐旧 `isWindowAllFailed`：先 `filter(recoveryRelevant)` 再取窗口；
    /// 恢复相关记录不足 `window_size` 时返回 `false`。
    #[must_use]
    pub fn is_window_all_failed(&self, window_size: usize) -> bool {
        if window_size == 0 {
            return false;
        }
        let relevant: Vec<&ToolCallRecord> = self
            .history
            .iter()
            .filter(|r| r.recovery_relevant)
            .collect();
        if relevant.len() < window_size {
            return false;
        }
        let window = &relevant[relevant.len() - window_size..];
        window.iter().all(|r| !r.success)
    }

    /// 检查相同错误是否连续出现 `threshold` 次。
    ///
    /// 对齐旧 `isSameErrorRepeated`：先 `filter(recoveryRelevant)` 取尾窗；
    /// 窗内任一成功 → `false`；首条错误信息为 `None` → `false`；否则要求全窗
    /// `error_message` 与首条相等。
    #[must_use]
    pub fn is_same_error_repeated(&self, threshold: usize) -> bool {
        if threshold == 0 {
            return false;
        }
        let relevant: Vec<&ToolCallRecord> = self
            .history
            .iter()
            .filter(|r| r.recovery_relevant)
            .collect();
        if relevant.len() < threshold {
            return false;
        }
        let tail = &relevant[relevant.len() - threshold..];
        if tail.iter().any(|r| r.success) {
            return false;
        }
        let Some(first_error) = tail[0].error_message.as_deref() else {
            return false;
        };
        tail.iter()
            .all(|r| r.error_message.as_deref() == Some(first_error))
    }

    /// 重置追踪器——清除所有历史记录与计数器（对齐旧 `reset`）。
    pub fn reset(&mut self) {
        self.history.clear();
        self.consecutive_errors = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 场景 1：连续（恢复相关）错误使 `consecutive_errors` 递增。
    #[test]
    fn consecutive_errors_increment_on_recovery_relevant_failures() {
        let mut tracker = ToolCallTracker::new();
        tracker.record("Bash", false, Some("boom".to_owned()));
        assert_eq!(tracker.consecutive_errors(), 1);
        tracker.record("Bash", false, Some("boom".to_owned()));
        tracker.record("Bash", false, Some("boom".to_owned()));
        assert_eq!(tracker.consecutive_errors(), 3);
        assert_eq!(tracker.total_records(), 3);
    }

    /// 场景 2：成功调用重置连续错误计数。
    #[test]
    fn success_resets_consecutive_errors() {
        let mut tracker = ToolCallTracker::new();
        tracker.record("Bash", false, Some("boom".to_owned()));
        tracker.record("Bash", false, Some("boom".to_owned()));
        assert_eq!(tracker.consecutive_errors(), 2);
        tracker.record("Read", true, None);
        assert_eq!(tracker.consecutive_errors(), 0);
    }

    /// 场景 3：滑动窗口全失败检测（先按 `recovery_relevant` 过滤再取窗）。
    #[test]
    fn window_all_failed_detects_recovery_relevant_streak() {
        let mut tracker = ToolCallTracker::new();
        // 不足窗口 → false。
        for _ in 0..4 {
            tracker.record("Bash", false, Some("e".to_owned()));
        }
        assert!(!tracker.is_window_all_failed(5));
        tracker.record("Bash", false, Some("e".to_owned()));
        assert!(tracker.is_window_all_failed(5));
        // 成功记录 recovery_relevant=false（record 三参入口按 !success 推导），
        // 被 is_window_all_failed 的 recovery_relevant 过滤排除，故窗口内的
        // 5 条恢复相关失败不受影响，仍判「全失败」（逐字对齐旧 Java 语义）。
        tracker.record("Read", true, None);
        assert!(tracker.is_window_all_failed(5));
    }

    /// 场景 4：相同错误连续重复检测。
    #[test]
    fn same_error_repeated_requires_identical_messages() {
        let mut tracker = ToolCallTracker::new();
        tracker.record("Bash", false, Some("same".to_owned()));
        tracker.record("Bash", false, Some("same".to_owned()));
        tracker.record("Bash", false, Some("same".to_owned()));
        assert!(tracker.is_same_error_repeated(3));
        // 尾部错误信息不同 → false。
        tracker.record("Bash", false, Some("different".to_owned()));
        assert!(!tracker.is_same_error_repeated(3));
    }

    /// 场景 5：权限（非恢复相关）错误不计入 `consecutive_errors`、不入窗口/相同错误统计。
    #[test]
    fn permission_errors_excluded_from_consecutive_and_windows() {
        let mut tracker = ToolCallTracker::new();
        // 三条权限终态：success=false 但 recovery_relevant=false。
        for _ in 0..3 {
            tracker.record_with_recovery("Write", false, Some("denied".to_owned()), false);
        }
        // 不递增连续错误。
        assert_eq!(tracker.consecutive_errors(), 0);
        // 不入滑动窗口（恢复相关记录为 0）。
        assert!(!tracker.is_window_all_failed(3));
        // 不入相同错误统计。
        assert!(!tracker.is_same_error_repeated(3));
        // recent_records 仍全量保留（history 不过滤）。
        assert_eq!(tracker.recent_records(10).len(), 3);
    }

    /// 补充：`recent_records` 越界安全（不足 N 返回全部）+ reset 清空。
    #[test]
    fn recent_records_saturates_and_reset_clears() {
        let mut tracker = ToolCallTracker::new();
        tracker.record("A", true, None);
        tracker.record("B", true, None);
        assert_eq!(tracker.recent_records(10).len(), 2);
        assert_eq!(tracker.recent_records(1).len(), 1);
        tracker.reset();
        assert_eq!(tracker.total_records(), 0);
        assert_eq!(tracker.consecutive_errors(), 0);
    }
}
