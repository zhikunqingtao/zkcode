//! 增量折叠状态机——会话级累计轮次驱动的分段折叠。
//!
//! 建模来源：旧 `engine/IncrementalCollapseManager.java`（`main@581d407b`）。
//! 逐条对照：
//!
//! | 旧成员 | 本模块 |
//! |---|---|
//! | `COLLAPSE_SEGMENT_TURNS = 10` | [`COLLAPSE_SEGMENT_TURNS`] |
//! | `SESSION_TIMEOUT_MS = 30min` | [`SESSION_TIMEOUT_MS`] |
//! | `CollapsedSegment` | [`CollapsedSegment`] |
//! | `shouldCollapse(sessionId, currentTurn)` | [`IncrementalCollapseManager::should_collapse`] |
//! | `recordCollapse(sessionId, segment)` | [`IncrementalCollapseManager::record_collapse`] |
//! | `getSegments(sessionId)` | [`IncrementalCollapseManager::segments`] |
//! | `cleanupExpiredSessions()`（`@Scheduled` 5min） | [`IncrementalCollapseManager::cleanup_expired_sessions`] |
//!
//! # 触发语义
//!
//! 旧实现的「每 10 轮」不是取模，而是**距上次折叠的累计增量**：会话级
//! `cumulativeTurns` 逐轮累加，当增量 `cumulativeTurns - lastCollapsedAt >= 10`
//! 即触发；折叠落地后把水位推到当前累计值。故一次长跑与多次短跑跨越
//! 同一会话时轮次连续计数，不会因单次 run 的 `turnCount` 归零而漏折叠。
//!
//! # 与旧实现的差异
//!
//! 旧仓库为该管理器挂了 `@ConditionalOnProperty(name =
//! "context.cascade.incremental-collapse.enabled", havingValue = "true")`——
//! **默认关闭**。本实现把开关合并进 [`super::cascade_enabled`] 统一门控（调用
//! 方在级联总开关关闭时不触碰本管理器），不再单列一个默认关闭的子开关。
//!
//! 旧 `@Scheduled(fixedDelay = 300_000)` 的定时清理由调用方在自然轮次里顺带
//! 调用 [`IncrementalCollapseManager::cleanup_expired_sessions`] 替代——引擎侧
//! 不额外起后台任务（会话数量有限，清理是 O(n) 的纯内存扫描）。

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// 触发一次折叠所需的累计轮次增量（逐字对照旧 `COLLAPSE_SEGMENT_TURNS`）。
pub const COLLAPSE_SEGMENT_TURNS: u32 = 10;

/// 会话状态过期毫秒数（逐字对照旧 `SESSION_TIMEOUT_MS`）。
pub const SESSION_TIMEOUT_MS: u64 = 30 * 60 * 1000;

/// 一个已折叠的历史分段（逐字段对照旧
/// `IncrementalCollapseManager.CollapsedSegment`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollapsedSegment {
    /// 分段起始轮次（累计轮次坐标）。
    pub turn_start: u32,
    /// 分段结束轮次（累计轮次坐标）。
    pub turn_end: u32,
    /// 分段摘要文本。
    pub summary_text: String,
    /// 折叠前 token 估算。
    pub original_tokens: u32,
    /// 摘要 token 估算。
    pub summary_tokens: u32,
    /// 被折叠的原始消息 ID（供审计/回溯）。
    pub original_message_ids: Vec<String>,
}

impl CollapsedSegment {
    /// 该分段节省的 token 数。
    #[must_use]
    pub fn tokens_saved(&self) -> u32 {
        self.original_tokens.saturating_sub(self.summary_tokens)
    }
}

/// 单会话折叠状态（逐字段对照旧内部类 `CollapseState`）。
#[derive(Debug)]
struct CollapseState {
    /// 已折叠分段（旧 `CopyOnWriteArrayList`）。
    segments: Vec<CollapsedSegment>,
    /// 会话级累计轮次（旧 `AtomicInteger cumulativeTurns`）。
    cumulative_turns: u32,
    /// 上次折叠时的累计轮次水位（旧 `lastCollapsedAtCumulativeTurn`）。
    last_collapsed_at: u32,
    /// 上次访问时刻（旧 `lastAccessTime`，用于过期清理）。
    last_access: Instant,
}

impl CollapseState {
    fn new() -> Self {
        Self {
            segments: Vec::new(),
            cumulative_turns: 0,
            last_collapsed_at: 0,
            last_access: Instant::now(),
        }
    }
}

/// 增量折叠管理器（旧 `IncrementalCollapseManager` 的等价实现）。
///
/// 以 `Arc<IncrementalCollapseManager>` 共享给引擎；内部用毒锁降级恢复的
/// [`Mutex`] 保护会话表（内层为纯状态数据，恒可继续）。
#[derive(Debug, Default)]
pub struct IncrementalCollapseManager {
    states: Mutex<HashMap<String, CollapseState>>,
}

impl IncrementalCollapseManager {
    /// 新建空管理器。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 锁会话表（毒锁降级恢复；与 `engine::lock_runs` 同惯例）。
    fn lock(&self) -> MutexGuard<'_, HashMap<String, CollapseState>> {
        match self.states.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// 累加本次轮次并判定是否应折叠（逐条对照旧 `shouldCollapse`）。
    ///
    /// `turns` 为本次要计入的轮次数（旧调用点恒传 1，即「每轮加一」）。返回
    /// `true` 表示距上次折叠已累计 ≥ [`COLLAPSE_SEGMENT_TURNS`] 轮。
    ///
    /// 注意本方法**有副作用**（推进累计计数器），与旧实现一致：旧
    /// `shouldCollapse` 内部就是 `cumulativeTurns.addAndGet(currentTurn)`。
    pub fn should_collapse(&self, session_id: &str, turns: u32) -> bool {
        let mut states = self.lock();
        let state = states
            .entry(session_id.to_owned())
            .or_insert_with(CollapseState::new);
        state.last_access = Instant::now();
        state.cumulative_turns = state.cumulative_turns.saturating_add(turns);
        state
            .cumulative_turns
            .saturating_sub(state.last_collapsed_at)
            >= COLLAPSE_SEGMENT_TURNS
    }

    /// 记录一次折叠落地并推进水位（逐条对照旧 `recordCollapse`）。
    pub fn record_collapse(&self, session_id: &str, segment: CollapsedSegment) {
        let mut states = self.lock();
        let state = states
            .entry(session_id.to_owned())
            .or_insert_with(CollapseState::new);
        state.last_access = Instant::now();
        state.segments.push(segment);
        state.last_collapsed_at = state.cumulative_turns;
    }

    /// 会话已折叠分段快照（旧 `getSegments`）。
    #[must_use]
    pub fn segments(&self, session_id: &str) -> Vec<CollapsedSegment> {
        let mut states = self.lock();
        states
            .get_mut(session_id)
            .map(|state| {
                state.last_access = Instant::now();
                state.segments.clone()
            })
            .unwrap_or_default()
    }

    /// 会话累计轮次（旧 `getCumulativeTurns`）。
    #[must_use]
    pub fn cumulative_turns(&self, session_id: &str) -> u32 {
        self.lock()
            .get(session_id)
            .map_or(0, |state| state.cumulative_turns)
    }

    /// 清除单会话状态（旧 `clearSession`；会话结束/重置时调用）。
    pub fn clear_session(&self, session_id: &str) {
        self.lock().remove(session_id);
    }

    /// 活跃会话数（旧 `getActiveSessionCount`，用于指标）。
    #[must_use]
    pub fn active_session_count(&self) -> usize {
        self.lock().len()
    }

    /// 清理过期会话，返回被清理条数（逐条对照旧 `cleanupExpiredSessions`）。
    pub fn cleanup_expired_sessions(&self) -> usize {
        let timeout = Duration::from_millis(SESSION_TIMEOUT_MS);
        let now = Instant::now();
        let mut states = self.lock();
        let before = states.len();
        states.retain(|_, state| now.duration_since(state.last_access) < timeout);
        let removed = before - states.len();
        if removed > 0 {
            tracing::debug!(removed, "清理过期增量折叠会话状态");
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::{COLLAPSE_SEGMENT_TURNS, CollapsedSegment, IncrementalCollapseManager};

    fn segment(start: u32, end: u32) -> CollapsedSegment {
        CollapsedSegment {
            turn_start: start,
            turn_end: end,
            summary_text: "s".to_owned(),
            original_tokens: 100,
            summary_tokens: 30,
            original_message_ids: vec!["m1".to_owned()],
        }
    }

    #[test]
    fn collapse_triggers_every_segment_of_ten_turns() {
        let manager = IncrementalCollapseManager::new();
        for turn in 1..COLLAPSE_SEGMENT_TURNS {
            assert!(!manager.should_collapse("s1", 1), "第 {turn} 轮不应触发");
        }
        // 第 10 轮达阈值。
        assert!(manager.should_collapse("s1", 1));
        assert_eq!(manager.cumulative_turns("s1"), COLLAPSE_SEGMENT_TURNS);
        // 未记录折叠 → 水位未推进，仍为真（旧语义：判定只看水位差）。
        assert!(manager.should_collapse("s1", 1));
        manager.record_collapse("s1", segment(1, 11));
        assert!(!manager.should_collapse("s1", 1));
    }

    #[test]
    fn cumulative_counter_is_per_session() {
        let manager = IncrementalCollapseManager::new();
        assert!(!manager.should_collapse("a", 5));
        assert!(!manager.should_collapse("b", 5));
        assert_eq!(manager.cumulative_turns("a"), 5);
        assert_eq!(manager.cumulative_turns("b"), 5);
        // 会话 a 再累 5 轮达阈值，b 不受影响。
        assert!(manager.should_collapse("a", 5));
        assert!(!manager.should_collapse("b", 1));
        assert_eq!(manager.active_session_count(), 2);
    }

    #[test]
    fn segments_accumulate_and_expose_savings() {
        let manager = IncrementalCollapseManager::new();
        manager.record_collapse("s1", segment(1, 10));
        manager.record_collapse("s1", segment(11, 20));
        let segments = manager.segments("s1");
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].tokens_saved(), 70);
        assert_eq!(segments[1].turn_start, 11);
        // 未知会话返回空快照。
        assert!(manager.segments("nope").is_empty());
    }

    #[test]
    fn clear_session_drops_state() {
        let manager = IncrementalCollapseManager::new();
        assert!(!manager.should_collapse("s1", 3));
        manager.clear_session("s1");
        assert_eq!(manager.cumulative_turns("s1"), 0);
        assert_eq!(manager.active_session_count(), 0);
    }

    #[test]
    fn cleanup_keeps_fresh_sessions() {
        let manager = IncrementalCollapseManager::new();
        assert!(!manager.should_collapse("s1", 1));
        // 刚访问过 → 30min 超时未到，不清理。
        assert_eq!(manager.cleanup_expired_sessions(), 0);
        assert_eq!(manager.active_session_count(), 1);
    }
}
