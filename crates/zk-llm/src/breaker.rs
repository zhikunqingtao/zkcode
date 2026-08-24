//! Provider 熔断器——连续 5xx 降级 + 时间窗恢复（对照旧 `ApiCircuitBreaker`）。
//!
//! # 状态机（三态，对齐旧 CLOSED / OPEN / `HALF_OPEN`）
//!
//! ```text
//!            连续 FAILURE_THRESHOLD 次 5xx
//!   Closed ────────────────────────────────▶ Degraded
//!     ▲                                        │ 窗口 DEGRADE_WINDOW_MS 到期
//!     │ 探测成功                                ▼
//!     └──────────────────────────────────── HalfOpen
//!                                              │ 探测再失败
//!                                              └──▶ Degraded（窗口重置）
//! ```
//!
//! 路由语义：[`CircuitBreaker::is_available`] 为 `false`（即 Degraded）期间，
//! [`crate::registry::ProviderRegistry`] 不向该 provider 路由新请求（存在
//! 其他候选时）；`HalfOpen` 视为可用——它就是「放一个探测请求过去」的语义。
//!
//! # 与旧实现的差异（留痕 docs/compatibility.md §5）
//!
//! - 窗口时长取 2.7 规格的 **30s**（旧 `RECOVERY_TIMEOUT` 为 60s）；
//! - 失败计数**仅统计 5xx**（旧实现对任意失败计数）——429 属限流，由
//!   [`crate::secret::ApiKeyRing`] 的密钥冷却承接，不应触发 provider 降级。

use std::sync::{Mutex, PoisonError};

use crate::clock::mono_millis;

/// 连续 5xx 失败阈值（对齐旧 `FAILURE_THRESHOLD = 3`）。
pub const FAILURE_THRESHOLD: u32 = 3;

/// degraded 窗口毫秒（2.7 规格 30s）。
pub const DEGRADE_WINDOW_MS: u64 = 30_000;

/// 熔断三态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BreakerState {
    /// 正常——可路由。
    Closed,
    /// 降级——窗口内不路由新请求。
    Degraded,
    /// 半开——窗口已过，放行探测请求。
    HalfOpen,
}

/// 单 provider 熔断器（内部可变，`Arc` 共享）。
#[derive(Debug, Default)]
pub struct CircuitBreaker {
    inner: Mutex<BreakerInner>,
}

/// 熔断内部状态：连续失败计数 + 进入降级的时刻。
#[derive(Debug, Default)]
struct BreakerInner {
    consecutive_failures: u32,
    degraded_at: Option<u64>,
}

impl CircuitBreaker {
    /// 新建（初始 `Closed`）。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 当前状态（按当前时钟推导）。
    #[must_use]
    pub fn state(&self) -> BreakerState {
        self.state_at(mono_millis())
    }

    /// 指定时刻的状态（单测入口——无 sleep 依赖）。
    #[must_use]
    pub fn state_at(&self, now_ms: u64) -> BreakerState {
        let inner = self.lock();
        match inner.degraded_at {
            None => BreakerState::Closed,
            Some(at) if now_ms.saturating_sub(at) >= DEGRADE_WINDOW_MS => BreakerState::HalfOpen,
            Some(_) => BreakerState::Degraded,
        }
    }

    /// 是否可路由（`Degraded` 为 `false`；`HalfOpen` 放行探测）。
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.is_available_at(mono_millis())
    }

    /// 指定时刻的可路由判定。
    #[must_use]
    pub fn is_available_at(&self, now_ms: u64) -> bool {
        self.state_at(now_ms) != BreakerState::Degraded
    }

    /// 记一次成功（清零计数并解除降级——对齐旧 `HALF_OPEN` 成功 → CLOSED）。
    pub fn record_success(&self) {
        let mut inner = self.lock();
        inner.consecutive_failures = 0;
        inner.degraded_at = None;
    }

    /// 记一次服务端失败；仅 5xx 计入（其余状态码原样忽略，不改变状态）。
    pub fn record_status(&self, status: u16) {
        self.record_status_at(status, mono_millis());
    }

    /// 指定时刻记一次服务端失败（单测入口）。
    pub fn record_status_at(&self, status: u16, now_ms: u64) {
        if status < 500 {
            return;
        }
        let half_open = self.state_at(now_ms) == BreakerState::HalfOpen;
        let mut inner = self.lock();
        if half_open {
            // 半开探测失败 → 立刻回到降级并重置窗口（旧 HALF_OPEN → OPEN）。
            inner.consecutive_failures = FAILURE_THRESHOLD;
            inner.degraded_at = Some(now_ms);
            return;
        }
        inner.consecutive_failures = inner.consecutive_failures.saturating_add(1);
        if inner.consecutive_failures >= FAILURE_THRESHOLD && inner.degraded_at.is_none() {
            inner.degraded_at = Some(now_ms);
        }
    }

    /// 按 [`crate::error::ProviderError`] 记录结果（5xx 计入失败，其余忽略）。
    pub fn record_error(&self, error: &crate::error::ProviderError) {
        if let crate::error::ProviderError::Http { status, .. } = error {
            self.record_status(*status);
        }
    }

    /// 连续失败计数（诊断 / 单测用）。
    #[must_use]
    pub fn consecutive_failures(&self) -> u32 {
        self.lock().consecutive_failures
    }

    /// 取内部锁（毒化时取回内层值——熔断状态不是安全关键不变量）。
    fn lock(&self) -> std::sync::MutexGuard<'_, BreakerInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::{BreakerState, CircuitBreaker, DEGRADE_WINDOW_MS, FAILURE_THRESHOLD};

    #[test]
    fn closed_to_degraded_to_recovered_three_state_transition() {
        let breaker = CircuitBreaker::new();
        // 1. 正常态。
        assert_eq!(breaker.state_at(0), BreakerState::Closed);
        assert!(breaker.is_available_at(0));

        // 2. 连续 3 次 5xx → degraded（前两次仍可路由）。
        breaker.record_status_at(500, 10);
        assert_eq!(breaker.state_at(10), BreakerState::Closed);
        breaker.record_status_at(502, 20);
        assert_eq!(breaker.state_at(20), BreakerState::Closed);
        breaker.record_status_at(503, 30);
        assert_eq!(breaker.consecutive_failures(), FAILURE_THRESHOLD);
        assert_eq!(breaker.state_at(30), BreakerState::Degraded);
        assert!(!breaker.is_available_at(30));
        // 窗口内始终不可路由。
        assert!(!breaker.is_available_at(30 + DEGRADE_WINDOW_MS - 1));

        // 3. 窗口结束 → 半开（放行探测），探测成功 → 回到正常。
        let after = 30 + DEGRADE_WINDOW_MS;
        assert_eq!(breaker.state_at(after), BreakerState::HalfOpen);
        assert!(breaker.is_available_at(after));
        breaker.record_success();
        assert_eq!(breaker.state_at(after), BreakerState::Closed);
        assert_eq!(breaker.consecutive_failures(), 0);
    }

    #[test]
    fn half_open_failure_reopens_window() {
        let breaker = CircuitBreaker::new();
        for tick in 0..u64::from(FAILURE_THRESHOLD) {
            breaker.record_status_at(500, tick);
        }
        let half_open_at = DEGRADE_WINDOW_MS + 2;
        assert_eq!(breaker.state_at(half_open_at), BreakerState::HalfOpen);
        // 探测失败 → 立刻重回 degraded，窗口从探测时刻重新计时。
        breaker.record_status_at(500, half_open_at);
        assert_eq!(breaker.state_at(half_open_at), BreakerState::Degraded);
        assert_eq!(
            breaker.state_at(half_open_at + DEGRADE_WINDOW_MS),
            BreakerState::HalfOpen
        );
    }

    #[test]
    fn success_resets_failure_streak() {
        let breaker = CircuitBreaker::new();
        breaker.record_status_at(500, 1);
        breaker.record_status_at(500, 2);
        breaker.record_success();
        breaker.record_status_at(500, 3);
        assert_eq!(breaker.consecutive_failures(), 1);
        assert_eq!(breaker.state_at(3), BreakerState::Closed);
    }

    #[test]
    fn non_5xx_never_degrades_provider() {
        let breaker = CircuitBreaker::new();
        for status in [400u16, 401, 403, 404, 429] {
            for _ in 0..5 {
                breaker.record_status_at(status, 1);
            }
        }
        assert_eq!(breaker.consecutive_failures(), 0);
        assert_eq!(breaker.state_at(1), BreakerState::Closed);
    }

    #[test]
    fn record_error_maps_http_status_only() {
        let breaker = CircuitBreaker::new();
        breaker.record_error(&crate::error::ProviderError::Network {
            message: "connect timeout".into(),
        });
        assert_eq!(breaker.consecutive_failures(), 0);
        for _ in 0..FAILURE_THRESHOLD {
            breaker.record_error(&crate::error::ProviderError::http(
                503,
                "overloaded".into(),
                None,
            ));
        }
        assert_eq!(breaker.state(), BreakerState::Degraded);
    }
}
