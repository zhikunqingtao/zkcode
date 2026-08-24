//! API 密钥脱敏 newtype 与轮换环——Debug / 日志 / 错误链全路径不泄漏明文。

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// 脱敏 API 密钥。
///
/// 刻意不实现 [`std::fmt::Display`]（堵死 `format!("{key}")` 类打印路径），
/// [`fmt::Debug`] 输出恒为占位符；唯一取值出口是 [`ApiKey::expose`]。
/// 相比引入 `secrecy` crate：本 crate 只需「零依赖的窄出口」语义，手置
/// newtype 更贴合 workspace「不提前引入依赖」原则（决策 D-S6-4）。
///
/// ```
/// use zk_llm::ApiKey;
/// let key = ApiKey::new("sk-plaintext-secret");
/// assert_eq!(format!("{key:?}"), "ApiKey(\"***REDACTED***\")");
/// assert_eq!(key.expose(), "sk-plaintext-secret");
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKey(String);

impl ApiKey {
    /// 以明文构造（密钥仅来自环境变量 / 配置注入，不落盘）。
    #[must_use]
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// 唯一明文出口——仅在构造 `Authorization` 请求头时调用。
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// 是否为空串（未配置密钥的诊断判定）。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKey(\"***REDACTED***\")")
    }
}

/// 默认限流冷却毫秒（对齐旧 `ApiKeyRotationManager.DEFAULT_COOLDOWN` = 60s）。
pub const KEY_COOLDOWN_MS: u64 = 60_000;

/// API 密钥轮换环（对照旧 `ApiKeyRotationManager`）。
///
/// 语义逐条对齐旧实现：
/// - 多密钥 **round-robin**（`currentIndex.getAndUpdate(i -> (i+1)%size)`）；
/// - 429 限流的密钥进入冷却期，轮换时跳过（[`ApiKeyRing::mark_rate_limited`]）；
/// - 全部密钥冷却时返回**最先解冻**的那一把（不空转、不 panic）；
/// - 单密钥退化为恒返回该密钥（旧 `size == 1` 短路分支）。
///
/// `Clone` 共享同一轮换游标与冷却表（内部 `Arc`）——配置克隆不会把轮换状态
/// 分叉，这是「同一 provider 的多把 key 全局轮换」的正确语义。
///
/// # 密钥安全
///
/// [`fmt::Debug`] 只输出密钥**数量**，不含任何明文；取值出口仅
/// [`ApiKeyRing::next_key`]（返回 [`ApiKey`]，其自身亦脱敏）。
///
/// ```
/// use zk_llm::{ApiKey, ApiKeyRing};
/// let ring = ApiKeyRing::from_csv("sk-a, sk-b");
/// assert_eq!(ring.len(), 2);
/// assert_eq!(format!("{ring:?}"), "ApiKeyRing { keys: 2 }");
/// // round-robin：连续取用轮转到下一把。
/// assert_eq!(ring.next_key().map(|k| k.expose().to_owned()), Some("sk-a".to_owned()));
/// assert_eq!(ring.next_key().map(|k| k.expose().to_owned()), Some("sk-b".to_owned()));
/// assert_eq!(ring.next_key().map(|k| k.expose().to_owned()), Some("sk-a".to_owned()));
/// let _ = ApiKey::new("sk-a");
/// ```
#[derive(Clone)]
pub struct ApiKeyRing {
    inner: Arc<RingInner>,
}

/// 轮换环内部状态（游标 + per-key 冷却截止刻度，0 = 无冷却）。
struct RingInner {
    keys: Vec<ApiKey>,
    cursor: AtomicUsize,
    cooldown_until: Mutex<Vec<u64>>,
}

impl ApiKeyRing {
    /// 以密钥列表构造（空白项过滤，对齐旧 `filter(k -> !k.isBlank())`）。
    #[must_use]
    pub fn new(keys: Vec<ApiKey>) -> Self {
        let keys: Vec<ApiKey> = keys
            .into_iter()
            .filter(|key| !key.expose().trim().is_empty())
            .collect();
        let cooldown_until = vec![0_u64; keys.len()];
        Self {
            inner: Arc::new(RingInner {
                keys,
                cursor: AtomicUsize::new(0),
                cooldown_until: Mutex::new(cooldown_until),
            }),
        }
    }

    /// 单密钥便捷构造（旧单参构造器等价）。
    #[must_use]
    pub fn single(key: ApiKey) -> Self {
        Self::new(vec![key])
    }

    /// 逗号分隔明文构造（环境变量 `LLM_PROVIDER_<NAME>_API_KEY` 的多 key 形态）。
    #[must_use]
    pub fn from_csv(raw: &str) -> Self {
        Self::new(
            raw.split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(ApiKey::new)
                .collect(),
        )
    }

    /// 密钥数量。
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.keys.len()
    }

    /// 是否未配置任何密钥。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.keys.is_empty()
    }

    /// 取下一把可用密钥（round-robin + 跳过冷却）；未配置密钥时 `None`。
    #[must_use]
    pub fn next_key(&self) -> Option<ApiKey> {
        self.next_key_at(crate::clock::mono_millis())
    }

    /// 指定时刻取下一把可用密钥（单测入口——无 sleep 依赖）。
    #[must_use]
    pub fn next_key_at(&self, now_ms: u64) -> Option<ApiKey> {
        let size = self.inner.keys.len();
        if size == 0 {
            return None;
        }
        if size == 1 {
            return self.inner.keys.first().cloned();
        }
        let cooldown = self.cooldown();
        for _ in 0..size {
            let index = self
                .inner
                .cursor
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some((current + 1) % size)
                })
                .unwrap_or(0);
            let until = cooldown.get(index).copied().unwrap_or(0);
            if until == 0 || now_ms >= until {
                return self.inner.keys.get(index).cloned();
            }
        }
        // 全部冷却中 → 取最先解冻的一把（旧 min(cooldownUntil) 分支）。
        let earliest = cooldown
            .iter()
            .enumerate()
            .min_by_key(|(_, until)| **until)
            .map_or(0, |(index, _)| index);
        self.inner.keys.get(earliest).cloned()
    }

    /// 标记某把密钥被限流（默认冷却 [`KEY_COOLDOWN_MS`]）。
    pub fn mark_rate_limited(&self, key: &ApiKey) {
        self.mark_rate_limited_at(key, crate::clock::mono_millis(), KEY_COOLDOWN_MS);
    }

    /// 指定时刻与时长标记限流（单测入口）。
    pub fn mark_rate_limited_at(&self, key: &ApiKey, now_ms: u64, cooldown_ms: u64) {
        let Some(index) = self
            .inner
            .keys
            .iter()
            .position(|candidate| candidate == key)
        else {
            return;
        };
        let mut cooldown = self
            .inner
            .cooldown_until
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(slot) = cooldown.get_mut(index) {
            *slot = now_ms.saturating_add(cooldown_ms);
        }
    }

    /// 是否存在未冷却密钥（诊断用，对齐旧 `hasAvailableKey`）。
    #[must_use]
    pub fn has_available_key_at(&self, now_ms: u64) -> bool {
        !self.is_empty()
            && self
                .cooldown()
                .iter()
                .any(|until| *until == 0 || now_ms >= *until)
    }

    /// 冷却表快照。
    fn cooldown(&self) -> Vec<u64> {
        self.inner
            .cooldown_until
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl fmt::Debug for ApiKeyRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiKeyRing")
            .field("keys", &self.inner.keys.len())
            .finish()
    }
}

impl From<ApiKey> for ApiKeyRing {
    fn from(key: ApiKey) -> Self {
        Self::single(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLAIN: &str = "sk-test-0123456789abcdef";

    #[test]
    fn debug_output_is_redacted() {
        let key = ApiKey::new(PLAIN);
        let rendered = format!("{key:?}");
        assert_eq!(rendered, "ApiKey(\"***REDACTED***\")");
        assert!(!rendered.contains(PLAIN));
        // 嵌套结构（如 provider 配置）Debug 也不泄漏。
        let wrapper = vec![key.clone()];
        assert!(!format!("{wrapper:?}").contains(PLAIN));
    }

    #[test]
    fn expose_returns_plaintext() {
        let key = ApiKey::new(PLAIN);
        assert_eq!(key.expose(), PLAIN);
        assert!(!key.is_empty());
        assert!(ApiKey::new("").is_empty());
    }

    #[test]
    fn clone_and_equality_hold_plaintext_semantics() {
        let a = ApiKey::new(PLAIN);
        let b = a.clone();
        assert_eq!(a, b);
        assert_ne!(a, ApiKey::new("other"));
    }

    #[test]
    fn ring_round_robins_across_keys() {
        // 对齐旧 ApiKeyRotationManager：(i + 1) % size 的严格轮转。
        let ring = ApiKeyRing::from_csv("k1,k2,k3");
        assert_eq!(ring.len(), 3);
        let picked: Vec<String> = (0..7)
            .map(|_| {
                ring.next_key_at(0)
                    .map(|key| key.expose().to_owned())
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(picked, vec!["k1", "k2", "k3", "k1", "k2", "k3", "k1"]);
    }

    #[test]
    fn ring_filters_blank_and_handles_single_key() {
        let ring = ApiKeyRing::from_csv(" , sk-only ,  ");
        assert_eq!(ring.len(), 1);
        assert!(!ring.is_empty());
        // 单密钥短路：恒返回同一把且不推进游标语义。
        for _ in 0..3 {
            assert_eq!(ring.next_key_at(0).expect("key").expose(), "sk-only");
        }
        assert!(ApiKeyRing::from_csv("  ").is_empty());
        assert!(ApiKeyRing::from_csv("").next_key_at(0).is_none());
    }

    #[test]
    fn ring_skips_rate_limited_key_until_cooldown_expires() {
        let ring = ApiKeyRing::from_csv("k1,k2");
        let k1 = ApiKey::new("k1");
        ring.mark_rate_limited_at(&k1, 1_000, 60_000);
        // 冷却期内 k1 被跳过——连续取用恒为 k2。
        for _ in 0..4 {
            assert_eq!(ring.next_key_at(2_000).expect("key").expose(), "k2");
        }
        assert!(ring.has_available_key_at(2_000));
        // 冷却到期后重新参与轮换。
        let after: Vec<String> = (0..2)
            .map(|_| ring.next_key_at(61_001).expect("key").expose().to_owned())
            .collect();
        assert!(after.contains(&"k1".to_owned()));
    }

    #[test]
    fn ring_returns_earliest_unfrozen_key_when_all_cooling() {
        let ring = ApiKeyRing::from_csv("k1,k2");
        ring.mark_rate_limited_at(&ApiKey::new("k1"), 0, 10_000);
        ring.mark_rate_limited_at(&ApiKey::new("k2"), 0, 50_000);
        assert!(!ring.has_available_key_at(1_000));
        // 全冷却 → 最先解冻的 k1（旧 min(cooldownUntil) 分支）。
        assert_eq!(ring.next_key_at(1_000).expect("key").expose(), "k1");
    }

    #[test]
    fn ring_debug_reveals_only_key_count() {
        let ring = ApiKeyRing::from_csv(&format!("{PLAIN},{PLAIN}-2"));
        let rendered = format!("{ring:?}");
        assert_eq!(rendered, "ApiKeyRing { keys: 2 }");
        assert!(!rendered.contains(PLAIN));
        // 嵌套（provider 配置 Debug）同样不泄漏。
        assert!(!format!("{:?}", vec![ring]).contains(PLAIN));
    }

    #[test]
    fn ring_from_api_key_is_single_key_ring() {
        let ring: ApiKeyRing = ApiKey::new(PLAIN).into();
        assert_eq!(ring.len(), 1);
        assert_eq!(ring.next_key_at(0).expect("key").expose(), PLAIN);
    }
}
