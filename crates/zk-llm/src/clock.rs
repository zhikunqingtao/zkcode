//! 进程内单调时钟——熔断窗口与密钥冷却的共用刻度。
//!
//! 私有模块：对外只暴露 `*_at(now_ms)` 形态的显式时刻入参 API（见
//! [`crate::breaker`] 与 [`crate::secret::ApiKeyRing`]），单测因此既不依赖
//! `sleep`，也不依赖 `Instant` 的可减性（`Instant::now() - 30s` 在部分平台
//! 会 panic）。

use std::sync::LazyLock;
use std::time::Instant;

/// 进程起点（首次取用时刻固定）。
static PROCESS_START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// 进程启动至今的单调毫秒。
pub(crate) fn mono_millis() -> u64 {
    u64::try_from(PROCESS_START.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::mono_millis;

    #[test]
    fn mono_millis_never_goes_backwards() {
        let first = mono_millis();
        let second = mono_millis();
        assert!(second >= first, "monotonic clock must not go backwards");
    }
}
