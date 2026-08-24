//! 同提供商重试——指数退避 + jitter（对照旧 `ApiRetryService` +
//! `ModelAwareRetryPolicy`）。
//!
//! # 旧实现对照（`engine/ApiRetryService.java`）
//!
//! `executeWithRetry` 的循环语义逐条移植：
//!
//! 1. **一次逻辑调用只做一次熔断准入**（旧 `circuitBreaker.allowRequest()` 在
//!    循环之外）——内层重试不重复消耗配额；对应侧亦只在**放弃重试时**记一次
//!    provider 失败（旧 `recordFailure` 只在终态分支调用）；
//! 2. **取消是控制流、不是 provider 故障**：等待期间被取消 → 直接终止，不计
//!    熔断失败（旧注释：一次用户取消不应拖累复用同一 provider 的其他 Run）；
//! 3. **529 容量超限单独限额**（旧 `MAX_529_RETRIES = 3`），其余可重试错误走
//!    `DEFAULT_MAX_RETRIES = 10`；
//! 4. **延迟计算**（旧 `calculateDelayWithModelAwareness`）：服务端
//!    `Retry-After` 优先（模型族允许时，上限 30s），否则模型感知指数退避 +
//!    25% jitter。
//!
//! # 与旧实现的差异（留痕）
//!
//! - 旧 529 分支还会调 `ModelTierService.triggerCooldown` 触发模型冷却——本
//!   crate 无模型分层组件，该副作用无对应面（限额与源分类语义均已移植：
//!   见 [`FOREGROUND_529_RETRY_SOURCES`] 与 [`RetryPolicy::should_retry_529`]，
//!   注册表按前台源 [`FOREGROUND_QUERY_SOURCE`] 装配）；
//! - 旧 `isRetryableError` 三条判定中，`isRetryable()` 标志与
//!   `statusCode >= 500` 已如实移植（[`RetryPolicy::is_retryable_error`]）；
//!   第三条 `errorType ∈ {overloaded_error, rate_limit_error, api_error}`
//!   无对应数据源（本 crate 不解析响应体 `error.type`），而这三类在旧系统
//!   均伴随 429 / 5xx 状态码出现，实际覆盖不缺口；
//! - 旧模型族配置存于 `Map`（迭代序不定），此处为固定序切片
//!   [`retry_config_for`]（仅当模型 ID 同时含两族关键字时才有差异）；
//! - jitter 随机源：旧 `ThreadLocalRandom.current().nextDouble()` → 线程私有
//!   xorshift64（整数取模，无浮点截断；jitter 不是安全敏感量，不引入 `rand`
//!   依赖）。

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;

use crate::error::ProviderError;

/// 标准可重试错误的重试上限（旧 `ApiRetryService.DEFAULT_MAX_RETRIES = 10`）。
///
/// 语义：计数**失败次数**，第 10 次失败即放弃（共 10 次调用 = 首次 + 9 次重试），
/// 对齐旧 `if (attempt >= DEFAULT_MAX_RETRIES) throw`。
pub const DEFAULT_MAX_RETRIES: u32 = 10;

/// 529 容量超限的重试上限（旧 `ApiRetryService.MAX_529_RETRIES = 3`）。
pub const MAX_529_RETRIES: u32 = 3;

/// 退避基数毫秒（旧 `ApiRetryService.BASE_DELAY_MS = 500`，亦为未知模型族基数）。
pub const BASE_DELAY_MS: u64 = 500;

/// 退避上限毫秒（旧 `ApiRetryService.MAX_DELAY_MS = 30_000`）。
pub const MAX_DELAY_MS: u64 = 30_000;

/// 退避倍率（旧 `ApiRetryService.BACKOFF_MULTIPLIER = 2.0`）。
pub const BACKOFF_MULTIPLIER: u64 = 2;

/// jitter 百分比（旧 `ModelAwareRetryPolicy.JITTER_FACTOR = 0.25`，整数表达）。
pub const JITTER_PERCENT: u64 = 25;

/// 容量超限状态码（`Anthropic` 系 overloaded——旧 529 分支的判定值）。
pub const CAPACITY_LIMIT_STATUS: u16 = 529;

/// 允许重试 529 的查询源白名单（逐项对照旧
/// `ApiRetryService.FOREGROUND_529_RETRY_SOURCES`，11 项）。
///
/// 旧实现只让**前台**（用户在等）的查询源重试容量超限：后台批处理重试
/// 529 只会加剧过载。匹配规则见 [`RetryPolicy::should_retry_529`]。
pub const FOREGROUND_529_RETRY_SOURCES: &[&str] = &[
    "repl_main_thread",
    "sdk",
    "agent:custom",
    "agent:default",
    "agent:builtin",
    "compact",
    "hook_agent",
    "hook_prompt",
    "verification_agent",
    "side_question",
    "auto_mode",
];

/// zk-llm 当前唯一的调用场景标识：前台交互对话。
///
/// 旧系统的用户可见主循环即 `repl_main_thread`（白名单内），zkcode 的
/// WebSocket 对话是同一角色，故 [`crate::registry::ProviderRegistry`] 默认
/// 以该源装配策略。将来 `ChatRequest` 若携带子代理 / 压缩 / 钩子等非前台
/// 源，改为逐请求透传即可，本常量退化为默认值。
pub const FOREGROUND_QUERY_SOURCE: &str = "repl_main_thread";

/// 模型族重试配置（对照旧 `ModelAwareRetryPolicy.RetryConfig`）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModelRetryConfig {
    /// 该模型族的重试上限。
    ///
    /// 注意：旧 `ApiRetryService` 的循环**固定用** [`DEFAULT_MAX_RETRIES`]，
    /// 不读该字段；此处保留以完整对齐旧配置面。
    pub max_retries: u32,
    /// 退避基数毫秒。
    pub base_delay_ms: u64,
    /// 是否遵循服务端 `Retry-After`。
    pub respect_retry_after: bool,
}

/// 未知模型族的配置（旧 `DEFAULT_CONFIG`：10 次 / 500ms / 遵循 retry-after）。
pub const DEFAULT_MODEL_RETRY_CONFIG: ModelRetryConfig = ModelRetryConfig {
    max_retries: DEFAULT_MAX_RETRIES,
    base_delay_ms: BASE_DELAY_MS,
    respect_retry_after: true,
};

/// 模型族配置表（旧 `MODEL_CONFIGS`，键为模型 ID 关键字，小写匹配）。
const MODEL_RETRY_CONFIGS: &[(&str, ModelRetryConfig)] = &[
    (
        "claude",
        ModelRetryConfig {
            max_retries: 5,
            base_delay_ms: 60_000,
            respect_retry_after: true,
        },
    ),
    (
        "qwen",
        ModelRetryConfig {
            max_retries: 8,
            base_delay_ms: 10_000,
            respect_retry_after: false,
        },
    ),
    (
        "deepseek",
        ModelRetryConfig {
            max_retries: 6,
            base_delay_ms: 20_000,
            respect_retry_after: true,
        },
    ),
];

/// 取模型的重试配置（小写子串模糊匹配，未命中取
/// [`DEFAULT_MODEL_RETRY_CONFIG`]——对照旧 `getRetryConfig`）。
#[must_use]
pub fn retry_config_for(model: &str) -> ModelRetryConfig {
    if model.trim().is_empty() {
        return DEFAULT_MODEL_RETRY_CONFIG;
    }
    let lowered = model.to_lowercase();
    for (keyword, config) in MODEL_RETRY_CONFIGS {
        if lowered.contains(keyword) {
            return *config;
        }
    }
    DEFAULT_MODEL_RETRY_CONFIG
}

/// 重试策略参数（常量默认值 = 旧 `ApiRetryService` 常量面）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// 标准可重试错误的失败上限（旧 `DEFAULT_MAX_RETRIES`）。
    pub max_retries: u32,
    /// 529 容量超限的重试上限（旧 `MAX_529_RETRIES`）。
    pub max_capacity_retries: u32,
    /// 查询源标识（旧 `executeWithRetry(…, querySource, …)`）——529 源
    /// 分类的唯一依据；`None` 对齐旧 `querySource == null`（529 不重试）。
    pub query_source: Option<String>,
    /// 延迟覆写毫秒：`Some(ms)` 时**所有**等待恒为该值（含 `Retry-After`
    /// 分支）——低延迟场景与单测的确定性入口；`None` 走旧模型感知计算。
    pub delay_override_ms: Option<u64>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            max_capacity_retries: MAX_529_RETRIES,
            query_source: None,
            delay_override_ms: None,
        }
    }
}

impl RetryPolicy {
    /// 旧常量面默认策略（10 次 / 529 限 3 次 / 模型感知退避）。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 禁用重试（首次失败即放弃）——保留纯降级链语义的入口。
    #[must_use]
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            max_capacity_retries: 0,
            query_source: None,
            delay_override_ms: Some(0),
        }
    }

    /// 零延迟重试（退避等待恒为 0ms）——单测与低延迟场景入口。
    #[must_use]
    pub fn immediate(max_retries: u32) -> Self {
        Self {
            max_retries,
            max_capacity_retries: max_retries.min(MAX_529_RETRIES),
            query_source: Some(FOREGROUND_QUERY_SOURCE.to_owned()),
            delay_override_ms: Some(0),
        }
    }

    /// 前台交互源策略（529 可重试）——zk-llm 的 WebSocket 对话等价于旧
    /// `repl_main_thread` 主循环。
    #[must_use]
    pub fn foreground() -> Self {
        Self::new().with_query_source(FOREGROUND_QUERY_SOURCE)
    }

    /// 设置查询源（链式；529 源分类依据）。
    #[must_use]
    pub fn with_query_source(mut self, query_source: impl Into<String>) -> Self {
        self.query_source = Some(query_source.into());
        self
    }

    /// 529 是否还能重试（旧 `shouldRetry529`：预算未尽 **且** 源在前台
    /// 白名单内）。
    ///
    /// 源匹配与旧实现逐字一致：与白名单项全等，或以 `项:` 为前缀（故
    /// `agent:custom` 覆盖 `agent:custom:reviewer`，但**不**覆盖
    /// `agent:customx`）。`None` 源一律不重试（旧 `querySource == null`）。
    #[must_use]
    pub fn should_retry_529(&self, capacity_retries: u32) -> bool {
        if capacity_retries >= self.max_capacity_retries {
            return false;
        }
        let Some(source) = self.query_source.as_deref() else {
            return false;
        };
        FOREGROUND_529_RETRY_SOURCES.iter().any(|pattern| {
            source == *pattern
                || source
                    .strip_prefix(*pattern)
                    .is_some_and(|rest| rest.starts_with(':'))
        })
    }

    /// 是否为可重试的标准错误（旧 `isRetryableError`）。
    ///
    /// 旧实现三条：`isRetryable()` 标志 → `statusCode >= 500` → `errorType`
    /// 白名单。前两条如实移植（第二条覆盖标志未置位的冷门 5xx）；第三条
    /// 无对应数据源，见模块文档差异留痕。
    #[must_use]
    pub fn is_retryable_error(error: &ProviderError) -> bool {
        error.is_retryable()
            || matches!(error, ProviderError::Http { status, .. } if *status >= 500)
    }

    /// 模型的退避基数毫秒（覆写优先，否则取模型族配置）。
    #[must_use]
    pub fn base_delay_ms(&self, model: &str) -> u64 {
        self.delay_override_ms
            .unwrap_or_else(|| retry_config_for(model).base_delay_ms)
    }

    /// 是否采用服务端 `Retry-After`（对照旧 `shouldRespectRetryAfter`：
    /// `retryAfterMs > 0` 且模型族允许）。
    #[must_use]
    pub fn respect_retry_after(&self, model: &str, retry_after_ms: u64) -> bool {
        retry_after_ms > 0 && retry_config_for(model).respect_retry_after
    }

    /// 第 `attempt` 次失败后的等待毫秒（`attempt` 为 1 基失败次数，对齐旧
    /// `calculateDelayWithModelAwareness(attempt, …)`）；jitter 由进程内伪随机
    /// 源抽取。
    #[must_use]
    pub fn delay_ms(&self, model: &str, attempt: u32, retry_after_ms: Option<u64>) -> u64 {
        let jitter_ceiling = jitter_ceiling_ms(self.backoff_ms(model, attempt));
        self.delay_ms_with_jitter(model, attempt, retry_after_ms, random_below(jitter_ceiling))
    }

    /// [`RetryPolicy::delay_ms`] 的确定性内核（jitter 显式入参——单测入口，
    /// 与本 crate `*_at(now_ms)` 的注入约定一致）。
    #[must_use]
    pub fn delay_ms_with_jitter(
        &self,
        model: &str,
        attempt: u32,
        retry_after_ms: Option<u64>,
        jitter_ms: u64,
    ) -> u64 {
        if let Some(fixed) = self.delay_override_ms {
            return fixed;
        }
        // ① 服务端 retry-after 优先（模型族允许时），上限 30s。
        if let Some(after) = retry_after_ms
            && self.respect_retry_after(model, after)
        {
            return after.min(MAX_DELAY_MS);
        }
        // ② 模型感知指数退避 + 25% jitter，总量再次封顶 30s（旧实现的外层 min）。
        self.backoff_ms(model, attempt)
            .saturating_add(jitter_ms)
            .min(MAX_DELAY_MS)
    }

    /// 纯指数退避段：`min(baseDelay × 2^(attempt-1), MAX_DELAY_MS)`。
    fn backoff_ms(&self, model: &str, attempt: u32) -> u64 {
        let exponent = attempt.saturating_sub(1);
        let factor = BACKOFF_MULTIPLIER.checked_pow(exponent).unwrap_or(u64::MAX);
        self.base_delay_ms(model)
            .saturating_mul(factor)
            .min(MAX_DELAY_MS)
    }
}

/// 单次逻辑调用的重试账本（失败计数 + 529 计数 + 策略 + 模型）。
#[derive(Clone, Debug)]
pub struct RetryState {
    policy: RetryPolicy,
    model: String,
    attempt: u32,
    capacity_retries: u32,
}

impl RetryState {
    /// 新账本（对应旧 `executeWithRetry` 的一次逻辑调用）。
    #[must_use]
    pub fn new(policy: RetryPolicy, model: impl Into<String>) -> Self {
        Self {
            policy,
            model: model.into(),
            attempt: 0,
            capacity_retries: 0,
        }
    }

    /// 已发生的失败次数（诊断——对齐旧 `lastAttemptCount` 的用途）。
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// 已消耗的 529 重试次数。
    #[must_use]
    pub fn capacity_retries(&self) -> u32 {
        self.capacity_retries
    }

    /// 策略参数。
    #[must_use]
    pub fn policy(&self) -> &RetryPolicy {
        &self.policy
    }

    /// 一次失败后的决策：`Some(delay_ms)` = 等待该毫秒后重试；`None` = 放弃。
    ///
    /// 判定序严格对齐旧 `executeWithRetry`：529 限额 → 可重试性 → 总次数上限
    /// → 延迟计算。
    pub fn on_error(&mut self, error: &ProviderError) -> Option<u64> {
        let (status, retry_after_ms) = match error {
            ProviderError::Http {
                status,
                retry_after_ms,
                ..
            } => (Some(*status), *retry_after_ms),
            _ => (None, None),
        };
        self.attempt = self.attempt.saturating_add(1);

        if status == Some(CAPACITY_LIMIT_STATUS) {
            // 旧实现此处还触发模型冷却（ModelTierService）——本 crate 无对应
            // 组件，源分类 + 限额语义如实保留（见模块文档差异留痕）。
            if !self.policy.should_retry_529(self.capacity_retries) {
                return None;
            }
            self.capacity_retries = self.capacity_retries.saturating_add(1);
        } else if !RetryPolicy::is_retryable_error(error) {
            return None;
        }

        if self.attempt >= self.policy.max_retries {
            return None;
        }
        Some(
            self.policy
                .delay_ms(&self.model, self.attempt, retry_after_ms),
        )
    }
}

/// 协作式退避等待：取消优先（`biased` select），返回 `false` 表示等待期间被
/// 取消（调用方须放弃重试且**不**记熔断失败——旧实现视取消为控制流）。
pub async fn wait_for_retry(delay_ms: u64, cancel: &CancellationToken) -> bool {
    if cancel.is_cancelled() {
        return false;
    }
    tokio::select! {
        biased;
        () = cancel.cancelled() => false,
        () = tokio::time::sleep(Duration::from_millis(delay_ms)) => true,
    }
}

/// jitter 上限：退避段的 [`JITTER_PERCENT`]%（旧 `delay × JITTER_FACTOR`）。
fn jitter_ceiling_ms(backoff_ms: u64) -> u64 {
    backoff_ms.saturating_mul(JITTER_PERCENT) / 100
}

thread_local! {
    /// 线程私有 xorshift64 状态（`0` = 未播种）。
    static JITTER_RNG: Cell<u64> = const { Cell::new(0) };
}

/// 播种去相关计数器（同一纳秒内多线程首次播种不重合）。
static SEED_COUNTER: AtomicU64 = AtomicU64::new(0);

/// `[0, ceiling)` 区间的伪随机毫秒（`ceiling == 0` → 恒 0）。
fn random_below(ceiling: u64) -> u64 {
    if ceiling == 0 {
        return 0;
    }
    next_u64() % ceiling
}

/// xorshift64 下一个值（非密码学用途——仅用于退避 jitter）。
fn next_u64() -> u64 {
    JITTER_RNG.with(|state| {
        let mut value = state.get();
        if value == 0 {
            value = fresh_seed();
        }
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        state.set(value);
        value
    })
}

/// 墙钟纳秒 + 去相关计数器混合播种（末位置 1 → 恒非零）。
fn fresh_seed() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    let nanos = u64::from(now.subsec_nanos());
    let counter = SEED_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mixed = (now.as_secs() << 20)
        ^ nanos.wrapping_mul(6_364_136_223_846_793_005)
        ^ counter.wrapping_add(0x9E37_79B9_7F4A_7C15);
    mixed | 1
}

#[cfg(test)]
mod tests {
    use super::{
        BASE_DELAY_MS, DEFAULT_MAX_RETRIES, DEFAULT_MODEL_RETRY_CONFIG,
        FOREGROUND_529_RETRY_SOURCES, FOREGROUND_QUERY_SOURCE, MAX_529_RETRIES, MAX_DELAY_MS,
        ModelRetryConfig, RetryPolicy, RetryState, jitter_ceiling_ms, random_below,
        retry_config_for, wait_for_retry,
    };
    use crate::error::ProviderError;
    use tokio_util::sync::CancellationToken;

    fn http(status: u16) -> ProviderError {
        ProviderError::http(status, format!("HTTP {status}"), None)
    }

    #[test]
    fn exponential_backoff_matches_legacy_formula() {
        // 旧公式：min(500 × 2^(attempt-1), 30s)，jitter = 0 时逐档对照。
        let policy = RetryPolicy::new();
        let expected = [
            (1u32, 500u64),
            (2, 1_000),
            (3, 2_000),
            (4, 4_000),
            (5, 8_000),
            (6, 16_000),
            (7, MAX_DELAY_MS),
            (64, MAX_DELAY_MS),
        ];
        for (attempt, delay) in expected {
            assert_eq!(
                policy.delay_ms_with_jitter("kimi-k3", attempt, None, 0),
                delay,
                "attempt {attempt} backoff mismatch"
            );
        }
        assert_eq!(BASE_DELAY_MS, DEFAULT_MODEL_RETRY_CONFIG.base_delay_ms);
    }

    #[test]
    fn jitter_adds_at_most_twenty_five_percent_and_stays_capped() {
        let policy = RetryPolicy::new();
        // 25% 上加（旧 JITTER_FACTOR = 0.25）。
        assert_eq!(jitter_ceiling_ms(2_000), 500);
        assert_eq!(policy.delay_ms_with_jitter("kimi-k3", 3, None, 500), 2_500);
        // 封顶后 jitter 不得越过 30s（旧实现的外层 min）。
        assert_eq!(
            policy.delay_ms_with_jitter("kimi-k3", 9, None, jitter_ceiling_ms(MAX_DELAY_MS)),
            MAX_DELAY_MS
        );
        // 随机路径落在 [base, base × 1.25] 带内。
        for _ in 0..64 {
            let delay = policy.delay_ms("kimi-k3", 3, None);
            assert!(
                (2_000..=2_500).contains(&delay),
                "jitter out of band: {delay}"
            );
        }
        assert_eq!(random_below(0), 0, "zero ceiling must not draw");
    }

    #[test]
    fn retry_after_is_respected_and_capped_per_model_family() {
        let policy = RetryPolicy::new();
        // 未知模型族遵循 retry-after（旧 DEFAULT_CONFIG.respectRetryAfter = true）。
        assert_eq!(
            policy.delay_ms_with_jitter("kimi-k3", 1, Some(1_200), 0),
            1_200
        );
        // 上限 30s。
        assert_eq!(
            policy.delay_ms_with_jitter("kimi-k3", 1, Some(600_000), 0),
            MAX_DELAY_MS
        );
        // qwen 族不遵循 retry-after → 回到自身基数 10s 的指数退避。
        assert!(!policy.respect_retry_after("qwen3.7-max", 1_200));
        assert_eq!(
            policy.delay_ms_with_jitter("qwen3.7-max", 1, Some(1_200), 0),
            10_000
        );
        // retryAfterMs <= 0 视为缺失（旧 shouldRespectRetryAfter 首行短路）。
        assert!(!policy.respect_retry_after("kimi-k3", 0));
        assert_eq!(policy.delay_ms_with_jitter("kimi-k3", 1, Some(0), 0), 500);
    }

    #[test]
    fn model_family_table_matches_legacy_configs() {
        assert_eq!(
            retry_config_for("claude-sonnet-4-6"),
            ModelRetryConfig {
                max_retries: 5,
                base_delay_ms: 60_000,
                respect_retry_after: true,
            }
        );
        assert_eq!(retry_config_for("qwen3.7-max").base_delay_ms, 10_000);
        assert_eq!(retry_config_for("deepseek-v4-pro").base_delay_ms, 20_000);
        // 大小写无关的子串匹配（旧 toLowerCase + startsWith/contains）。
        assert_eq!(retry_config_for("Anthropic/CLAUDE-opus").max_retries, 5);
        // 未命中 / 空白 → 默认配置。
        assert_eq!(retry_config_for("kimi-k3"), DEFAULT_MODEL_RETRY_CONFIG);
        assert_eq!(retry_config_for("   "), DEFAULT_MODEL_RETRY_CONFIG);
        // claude 族基数 60s 首次退避即被 30s 封顶。
        assert_eq!(
            RetryPolicy::new().delay_ms_with_jitter("claude-sonnet-4-6", 1, None, 0),
            MAX_DELAY_MS
        );
    }

    #[test]
    fn fatal_errors_are_never_retried() {
        for error in [
            http(400),
            http(401),
            http(403),
            http(404),
            ProviderError::Parse {
                message: "bad json".into(),
            },
            ProviderError::Config {
                message: "no api key".into(),
            },
            ProviderError::Cancelled,
        ] {
            let mut state = RetryState::new(RetryPolicy::new(), "kimi-k3");
            assert_eq!(state.on_error(&error), None, "{error} must be fatal");
            assert_eq!(state.attempt(), 1, "failure still counted");
        }
    }

    #[test]
    fn retryable_statuses_and_network_errors_are_retried() {
        for error in [
            http(429),
            http(500),
            http(502),
            http(503),
            ProviderError::Network {
                message: "connect timeout".into(),
            },
        ] {
            let mut state = RetryState::new(RetryPolicy::immediate(3), "kimi-k3");
            assert_eq!(state.on_error(&error), Some(0), "{error} must be retryable");
        }
    }

    #[test]
    fn budget_exhausts_at_default_max_retries() {
        // 旧语义：第 DEFAULT_MAX_RETRIES 次失败即放弃（共 10 次调用 / 9 次重试）。
        let mut state = RetryState::new(RetryPolicy::immediate(DEFAULT_MAX_RETRIES), "kimi-k3");
        let mut granted = 0;
        while state.on_error(&http(503)).is_some() {
            granted += 1;
            assert!(granted < 100, "retry loop must terminate");
        }
        assert_eq!(granted, DEFAULT_MAX_RETRIES - 1);
        assert_eq!(state.attempt(), DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn capacity_limit_529_is_capped_independently() {
        // 旧 MAX_529_RETRIES = 3：529 只重试 3 次，即使总额度还有剩余。
        let mut state = RetryState::new(RetryPolicy::foreground(), "kimi-k3");
        for _ in 0..MAX_529_RETRIES {
            assert!(state.on_error(&http(529)).is_some());
        }
        assert_eq!(state.capacity_retries(), MAX_529_RETRIES);
        assert_eq!(
            state.on_error(&http(529)),
            None,
            "529 budget must be capped"
        );
        assert!(
            state.attempt() < DEFAULT_MAX_RETRIES,
            "总额度未耗尽，是 529 限额先生效"
        );
    }

    #[test]
    fn disabled_policy_gives_up_on_first_failure() {
        let mut state = RetryState::new(RetryPolicy::none(), "kimi-k3");
        assert_eq!(state.on_error(&http(503)), None);
        assert_eq!(state.policy().max_retries, 0);
    }

    #[tokio::test]
    async fn wait_for_retry_yields_to_cancellation_first() {
        // 已取消 → 立即返回（不消耗 30s 退避）。
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(!wait_for_retry(MAX_DELAY_MS, &cancel).await);

        // 等待中被取消 → 同样立即返回 false。
        let cancel = CancellationToken::new();
        let waiter = cancel.clone();
        let handle = tokio::spawn(async move { wait_for_retry(MAX_DELAY_MS, &waiter).await });
        cancel.cancel();
        assert!(!handle.await.expect("join"));
    }

    #[tokio::test]
    async fn wait_for_retry_completes_when_not_cancelled() {
        assert!(wait_for_retry(0, &CancellationToken::new()).await);
    }

    #[test]
    fn capacity_529_is_gated_by_foreground_query_source() {
        // 白名单逐项对照旧 FOREGROUND_529_RETRY_SOURCES（11 项，含前台默认源）。
        assert_eq!(FOREGROUND_529_RETRY_SOURCES.len(), 11);
        assert!(FOREGROUND_529_RETRY_SOURCES.contains(&FOREGROUND_QUERY_SOURCE));
        for source in FOREGROUND_529_RETRY_SOURCES {
            assert!(
                RetryPolicy::new()
                    .with_query_source(*source)
                    .should_retry_529(0),
                "source '{source}' must retry 529"
            );
        }
        // `pattern:` 前缀命中（旧 startsWith(pattern + ":")）。
        assert!(
            RetryPolicy::new()
                .with_query_source("agent:custom:reviewer")
                .should_retry_529(0)
        );
        // 非前台源 / 无冒号分隔的近似前缀 → 不重试。
        for source in ["background_indexing", "agent:customx", "sdk_batch"] {
            assert!(
                !RetryPolicy::new()
                    .with_query_source(source)
                    .should_retry_529(0),
                "source '{source}' must not retry 529"
            );
        }
        // 源缺失（旧 querySource == null）→ 529 一次都不重试。
        let mut anonymous = RetryState::new(RetryPolicy::new(), "kimi-k3");
        assert_eq!(anonymous.on_error(&http(529)), None);
        assert_eq!(anonymous.capacity_retries(), 0);
        // 但同一策略下的普通 5xx 照常重试——闸门只作用于 529。
        let mut standard = RetryState::new(RetryPolicy::new(), "kimi-k3");
        assert!(standard.on_error(&http(503)).is_some());
    }

    #[test]
    fn flagless_5xx_is_retryable_like_legacy_status_clause() {
        // 旧 isRetryableError 第二条：statusCode >= 500 即可重试，与
        // retryable 标志无关（覆盖标志未置位的冷门 5xx）。
        let flagless = ProviderError::Http {
            status: 507,
            message: "insufficient storage".into(),
            retry_after_ms: None,
            retryable: false,
        };
        assert!(!flagless.is_retryable());
        assert!(RetryPolicy::is_retryable_error(&flagless));
        // 4xx 致命面不受该分支影响。
        for status in [400u16, 401, 403, 404] {
            assert!(!RetryPolicy::is_retryable_error(&http(status)));
        }
    }
}
