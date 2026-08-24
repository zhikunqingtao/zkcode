//! Python 服务能力感知客户端——逐条对照旧 `PythonCapabilityAwareClient.java`
//! （§4.14 / §2.4.3）。
//!
//! 语义（旧源逐行对照见 `docs/compatibility.md` §6.4）：
//!
//! - 启动时探测能力域清单，仅调用已安装的能力域；缺依赖的域优雅降级
//!   （返回 `None` 而非报错，对照旧 `Optional.empty()`）；
//! - 能力清单内存缓存：**成功** 5 分钟 TTL / **失败** 30 秒 TTL；
//! - 三档超时：连接 3s、常规读 30s、重型读 120s；
//! - 失败重试：最多 3 次、500 ms 起指数退避，且**仅**只读端点白名单可重试；
//! - 4xx 永久错误不重试，直接降级；
//! - 关联诊断头 `X-Request-Id` / `X-Attempt` / `X-Run-Id` / `X-Session-Id`，
//!   值须过 `^[A-Za-z0-9._:-]{1,128}$` 白名单才发出。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use hyper::Method;
use hyper::header::HeaderName;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use super::uds::{self, TransportError, UdsRequest, UdsResponse};
use crate::iso::now_millis;

/// 连接超时（旧 `CONNECT_TIMEOUT = Duration.ofSeconds(3)`）。
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// 常规读超时（旧 `READ_TIMEOUT = Duration.ofSeconds(30)`）。
pub const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// 重型读超时（旧 `HEAVY_READ_TIMEOUT = Duration.ofSeconds(120)`，
/// 用于 `journey/run` 一类长任务端点）。
pub const HEAVY_READ_TIMEOUT: Duration = Duration::from_mins(2);

/// 最大重试次数（旧 `MAX_RETRIES = 3`；循环 `attempt <= MAX_RETRIES` 故
/// 总尝试 4 次）。
pub const MAX_RETRIES: u32 = 3;

/// 退避基数（旧 `RETRY_BASE_DELAY = Duration.ofMillis(500)`；实际睡眠序列
/// 500 / 1000 / 2000 ms——旧代码在 `attempt == MAX_RETRIES` 时不再睡眠，
/// 其注释写的 "500, 1000, 2000, 4000" 比代码多一档，此处对齐**代码**）。
pub const RETRY_BASE_DELAY: Duration = Duration::from_millis(500);

/// 能力清单成功缓存 TTL（旧 `SUCCESS_CACHE_TTL = Duration.ofMinutes(5)`）。
pub const SUCCESS_CACHE_TTL: Duration = Duration::from_mins(5);

/// 能力清单失败缓存 TTL（旧 `FAILURE_CACHE_TTL = Duration.ofSeconds(30)`）。
pub const FAILURE_CACHE_TTL: Duration = Duration::from_secs(30);

/// 能力探测超时（旧 `refreshCapabilities` 的 `timeout(Duration.ofSeconds(5))`）。
pub const CAPABILITY_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// 健康探测超时（旧 `isHealthy` 的 `timeout(Duration.ofSeconds(3))`）。
pub const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// 启动后首次能力刷新的延迟（旧 `onApplicationReady` 的 `Thread.sleep(5000)`）。
pub const STARTUP_REFRESH_DELAY: Duration = Duration::from_secs(5);

/// 能力清单端点（旧 `GET /api/health/capabilities`）。
const CAPABILITIES_ENDPOINT: &str = "/api/health/capabilities";

/// 健康端点（旧 `isHealthy` 的 `GET /api/health`）。
const HEALTH_ENDPOINT: &str = "/api/health";

/// 只读端点白名单前缀（旧 `isReadOnlyEndpoint` 逐条照抄；未列入者一律
/// **不重试**——「未知 POST 端点永不重试」的保守裁决）。
const READ_ONLY_PREFIXES: [&str; 5] = [
    "/api/analysis/",
    "/api/code-intel/",
    "/api/code-path/",
    "/api/code-diagram/",
    "/api/git/",
];

/// 单个能力域的可用状态（旧 `record CapabilityStatus(name, available, reason)`）。
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct CapabilityStatus {
    /// 能力域显示名称（如「代码智能」）。
    #[serde(default)]
    pub name: String,
    /// 是否可用。
    #[serde(default)]
    pub available: bool,
    /// 不可用原因（`available = true` 时为 `None`）。
    #[serde(default)]
    pub reason: Option<String>,
}

impl CapabilityStatus {
    /// 是否可用（旧 `isAvailable()`）。
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.available
    }
}

/// 调用关联上下文——旧端从 `MDC` 读 `runId` / `sessionId`，Rust 无 `MDC`
/// 等价物（`tracing` span 字段不可反向读取），故改为显式传参。
#[derive(Clone, Debug, Default)]
pub struct Correlation {
    /// 运行 ID（旧 `MDC.get("runId")` → `X-Run-Id`）。
    pub run_id: Option<String>,
    /// 会话 ID（旧 `MDC.get("sessionId")` → `X-Session-Id`）。
    pub session_id: Option<String>,
}

impl Correlation {
    /// 只带会话 ID 的关联上下文（工具侧从 `ToolContext::session_id` 取）。
    #[must_use]
    pub fn for_session(session_id: Option<&str>) -> Self {
        Self {
            run_id: None,
            session_id: session_id.map(str::to_owned),
        }
    }
}

/// Python 服务能力感知客户端（UDS 传输 + 能力缓存 + 退避重试 + 优雅降级）。
///
/// 线程安全：能力缓存以 `RwLock` 守护、时间戳与成败标记为原子量；克隆经
/// `Arc` 共享同一实例（对照旧单例 `@Service`）。
pub struct PythonClient {
    /// 侧车 socket 路径（`ZK_PYTHON_UDS`，默认 `~/.zkcode/python.sock`）。
    socket: PathBuf,
    /// 能力域缓存（`BTreeMap` 保证 `/api/health` 上报键序稳定）。
    capabilities: RwLock<BTreeMap<String, CapabilityStatus>>,
    /// 上次刷新时刻（epoch 毫秒；0 = 从未刷新）。
    last_refresh_millis: AtomicI64,
    /// 上次刷新是否成功（决定用 5 分钟还是 30 秒 TTL）。
    last_refresh_success: AtomicBool,
}

impl PythonClient {
    /// 以 socket 路径装配客户端（不发起任何 IO）。
    #[must_use]
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            capabilities: RwLock::new(BTreeMap::new()),
            last_refresh_millis: AtomicI64::new(0),
            last_refresh_success: AtomicBool::new(false),
        }
    }

    /// 侧车 socket 路径。
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    // ═══ 能力探测 ═══

    /// 刷新能力清单——`GET /api/health/capabilities`。
    ///
    /// 成功缓存 5 分钟、失败缓存 30 秒（旧 `refreshCapabilities` 逐条对齐：
    /// 非 200 与异常同样刷新时间戳但标记失败，走短缓存）。
    pub async fn refresh_capabilities(&self) {
        let outcome = uds::send(UdsRequest {
            socket: &self.socket,
            method: Method::GET,
            path: CAPABILITIES_ENDPOINT,
            headers: Vec::new(),
            body: None,
            connect_timeout: CONNECT_TIMEOUT,
            read_timeout: CAPABILITY_PROBE_TIMEOUT,
        })
        .await;
        match outcome {
            Ok(response) if response.status.as_u16() == 200 => {
                let parsed = parse_capabilities(&response.body);
                let count = parsed.len();
                *self
                    .capabilities
                    .write()
                    .unwrap_or_else(PoisonError::into_inner) = parsed;
                self.mark_refresh(true);
                tracing::info!(domains = count, "python capability registry refreshed");
            }
            Ok(response) => {
                self.mark_refresh(false);
                tracing::warn!(
                    status = response.status.as_u16(),
                    "python capability probe returned non-200; short cache enabled"
                );
            }
            Err(error) => {
                self.mark_refresh(false);
                tracing::warn!(
                    error_type = error.error_type(),
                    "python capability probe failed; short cache enabled"
                );
            }
        }
    }

    /// 缓存过期即刷新（旧 `refreshIfStale`：TTL 按上次刷新成败切换）。
    pub async fn refresh_if_stale(&self) {
        let elapsed = now_millis().saturating_sub(self.last_refresh_millis.load(Ordering::Acquire));
        let ttl = if self.last_refresh_success.load(Ordering::Acquire) {
            SUCCESS_CACHE_TTL
        } else {
            FAILURE_CACHE_TTL
        };
        if elapsed > i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX) {
            self.refresh_capabilities().await;
        }
    }

    /// 某能力域是否可用（旧 `isCapabilityAvailable`）。
    ///
    /// 缓存未命中时**立即刷新后重查**（而非直接判 false）——旧端同语义，
    /// 保证侧车迟到就绪时首次调用即可恢复能力。
    pub async fn is_capability_available(&self, domain: &str) -> bool {
        self.refresh_if_stale().await;
        if let Some(status) = self.cached_status(domain) {
            return status.is_available();
        }
        self.refresh_capabilities().await;
        self.cached_status(domain)
            .is_some_and(|status| status.is_available())
    }

    /// 全量能力域状态快照（旧 `getCapabilities` 的 `Map.copyOf`）。
    #[must_use]
    pub fn capabilities(&self) -> BTreeMap<String, CapabilityStatus> {
        self.capabilities
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// 只读缓存判断，不触发 UDS IO。
    ///
    /// 组合根用它把模型工具目录与最近一次成功探测的实际能力保持一致；刷新
    /// 本身由生命周期任务负责，目录读路径不会因此承担网络等待。
    #[must_use]
    pub fn cached_capability_available(&self, domain: &str) -> bool {
        self.cached_status(domain)
            .is_some_and(|status| status.is_available())
    }

    /// 上次能力刷新是否成功（`/api/health` 聚合上报用）。
    #[must_use]
    pub fn last_refresh_succeeded(&self) -> bool {
        self.last_refresh_success.load(Ordering::Acquire)
    }

    /// 上次能力刷新时刻（epoch 毫秒；0 = 从未刷新）。
    #[must_use]
    pub fn last_refresh_millis(&self) -> i64 {
        self.last_refresh_millis.load(Ordering::Acquire)
    }

    /// 启动后延迟 5 秒做首次能力刷新（旧
    /// `@EventListener(ApplicationReadyEvent)` + `CompletableFuture.runAsync`
    /// + `Thread.sleep(5000)` 的等价物；失败只告警不影响进程）。
    pub fn spawn_startup_refresh(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tokio::time::sleep(STARTUP_REFRESH_DELAY).await;
            self.refresh_capabilities().await;
            tracing::info!("python capability startup refresh completed");
        })
    }

    // ═══ 安全调用 ═══

    /// 能力域门控调用——能力不可用时返回 `None` 而非报错（旧
    /// `callIfAvailable`，常规 30s 读超时）。
    pub async fn call_if_available<T: DeserializeOwned>(
        &self,
        domain: &str,
        endpoint: &str,
        body: &serde_json::Value,
        correlation: &Correlation,
    ) -> Option<T> {
        self.call_if_available_with_timeout(domain, endpoint, body, correlation, READ_TIMEOUT)
            .await
    }

    /// 能力域门控调用（自定义超时；旧 `callIfAvailable(..., Duration)`
    /// 重载，`UserJourneyVerifier` 一类长任务用 [`HEAVY_READ_TIMEOUT`]）。
    pub async fn call_if_available_with_timeout<T: DeserializeOwned>(
        &self,
        domain: &str,
        endpoint: &str,
        body: &serde_json::Value,
        correlation: &Correlation,
        timeout: Duration,
    ) -> Option<T> {
        if !self.is_capability_available(domain).await {
            tracing::debug!(
                domain,
                endpoint,
                "python capability unavailable; skipping call"
            );
            return None;
        }
        self.call_with_retry(endpoint, body, correlation, timeout)
            .await
    }

    /// 直接 POST（不查能力域；旧 `post(endpoint, body, type)`）。
    pub async fn post<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
        correlation: &Correlation,
    ) -> Option<T> {
        self.call_with_retry(endpoint, body, correlation, READ_TIMEOUT)
            .await
    }

    /// 带指数退避的 POST（旧 `callWithRetry`；旧两个重载仅日志一行之差，
    /// 此处合一并以 `timeout` 参数区分）。
    ///
    /// 分支语义逐条对齐：2xx → 反序列化返回；4xx → 永久错误**不重试**；
    /// 非只读端点 → **不重试**；其余（5xx / 传输异常）→ 退避重试至上限。
    pub async fn call_with_retry<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
        correlation: &Correlation,
        timeout: Duration,
    ) -> Option<T> {
        let retry_allowed = is_read_only_endpoint(endpoint);
        let request_id = uuid::Uuid::new_v4().to_string();
        let Ok(json_body) = serde_json::to_string(body) else {
            tracing::warn!(endpoint, "python request body is not serialisable");
            return None;
        };
        for attempt in 0..=MAX_RETRIES {
            tracing::debug!(
                endpoint,
                request_id = %request_id,
                attempt = attempt + 1,
                body_length = json_body.len(),
                "python POST request"
            );
            let outcome = uds::send(UdsRequest {
                socket: &self.socket,
                method: Method::POST,
                path: endpoint,
                headers: correlation_headers(correlation, &request_id, attempt + 1),
                body: Some(json_body.clone()),
                connect_timeout: CONNECT_TIMEOUT,
                read_timeout: timeout,
            })
            .await;
            let failure_type = match outcome {
                Ok(response) if response.is_success() => {
                    match serde_json::from_str::<T>(&response.body) {
                        Ok(value) => return Some(value),
                        // 旧端 `objectMapper.readValue` 抛异常落入 catch 分支，
                        // 与传输异常同路径处理（可重试端点走退避）。
                        Err(_) => "ResponseDeserializationFailed",
                    }
                }
                Ok(response) => {
                    tracing::warn!(
                        endpoint,
                        request_id = %request_id,
                        attempt = attempt + 1,
                        status = response.status.as_u16(),
                        response_length = response.body.len(),
                        "python POST failed"
                    );
                    if response.is_client_error() {
                        tracing::info!(
                            endpoint,
                            status = response.status.as_u16(),
                            "permanent python client error is not retryable"
                        );
                        return None;
                    }
                    if !retry_allowed {
                        tracing::info!(
                            endpoint,
                            "python side-effecting/unknown endpoint is not retried"
                        );
                        return None;
                    }
                    "HttpStatus"
                }
                Err(error) => error.error_type(),
            };
            if !retry_allowed {
                tracing::warn!(
                    endpoint,
                    request_id = %request_id,
                    error_type = failure_type,
                    "python side-effecting/unknown endpoint failed without retry"
                );
                return None;
            }
            if attempt < MAX_RETRIES {
                let delay = RETRY_BASE_DELAY * 2u32.pow(attempt);
                tracing::debug!(
                    endpoint,
                    request_id = %request_id,
                    attempt = attempt + 1,
                    max_retries = MAX_RETRIES,
                    delay_ms = delay.as_millis(),
                    error_type = failure_type,
                    "python POST retry"
                );
                tokio::time::sleep(delay).await;
            } else {
                tracing::error!(
                    endpoint,
                    request_id = %request_id,
                    retries = MAX_RETRIES,
                    error_type = failure_type,
                    "python POST exhausted"
                );
            }
        }
        None
    }

    /// 单次直通转发——反向代理（[`super::proxy`]）唯一的出口。
    ///
    /// 与 [`Self::call_with_retry`] 的分工：代理服务的是浏览器同步请求，
    /// 故**不查能力域、不重试、不改写状态码**——侧车的状态码与响应体原样
    /// 交还调用方，重发与否由前端面板自行决定（服务端重试只会放大面板延迟，
    /// 且代理面覆盖 POST 端点，重试语义不安全）。传输层失败以
    /// [`TransportError`] 上抛，由代理层折叠为 503 / 504 信封。
    ///
    /// `target` 为 origin-form 请求目标（含 query）；`body` 恒按
    /// `application/json` 发出（四条代理前缀的 Python 端点均为 JSON 契约）。
    pub(crate) async fn forward(
        &self,
        method: Method,
        target: &str,
        body: Option<String>,
        correlation: &Correlation,
    ) -> Result<UdsResponse, TransportError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        tracing::debug!(
            target,
            method = %method,
            request_id = %request_id,
            body_length = body.as_ref().map_or(0, String::len),
            "python proxy forward"
        );
        uds::send(UdsRequest {
            socket: &self.socket,
            method,
            path: target,
            headers: correlation_headers(correlation, &request_id, 1),
            body,
            connect_timeout: CONNECT_TIMEOUT,
            read_timeout: READ_TIMEOUT,
        })
        .await
    }

    /// 直接 GET（返回原始响应体；旧 `get(endpoint)`，常规 30s 读超时）。
    pub async fn get(&self, endpoint: &str) -> Option<String> {
        match uds::send(UdsRequest {
            socket: &self.socket,
            method: Method::GET,
            path: endpoint,
            headers: Vec::new(),
            body: None,
            connect_timeout: CONNECT_TIMEOUT,
            read_timeout: READ_TIMEOUT,
        })
        .await
        {
            Ok(response) if response.is_success() => Some(response.body),
            Ok(response) => {
                tracing::warn!(
                    endpoint,
                    status = response.status.as_u16(),
                    "python GET returned non-2xx"
                );
                None
            }
            Err(error) => {
                tracing::error!(
                    endpoint,
                    error_type = error.error_type(),
                    "python GET failed"
                );
                None
            }
        }
    }

    /// 健康探测——`GET /api/health`，3 秒超时（旧 `isHealthy`）。
    pub async fn is_healthy(&self) -> bool {
        matches!(
            uds::send(UdsRequest {
                socket: &self.socket,
                method: Method::GET,
                path: HEALTH_ENDPOINT,
                headers: Vec::new(),
                body: None,
                connect_timeout: CONNECT_TIMEOUT,
                read_timeout: HEALTH_PROBE_TIMEOUT,
            })
            .await,
            Ok(response) if response.status.as_u16() == 200
        )
    }

    /// 清空能力缓存并标记为「从未刷新」——侧车重启后强制重新探测。
    pub(crate) fn invalidate_capabilities(&self) {
        self.capabilities
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.last_refresh_millis.store(0, Ordering::Release);
        self.last_refresh_success.store(false, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn replace_capabilities_for_tests(
        &self,
        capabilities: BTreeMap<String, CapabilityStatus>,
    ) {
        *self
            .capabilities
            .write()
            .unwrap_or_else(PoisonError::into_inner) = capabilities;
        self.mark_refresh(true);
    }

    /// 读缓存中某域状态（不触发刷新）。
    fn cached_status(&self, domain: &str) -> Option<CapabilityStatus> {
        self.capabilities
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(domain)
            .cloned()
    }

    /// 记一次刷新的时刻与成败。
    fn mark_refresh(&self, success: bool) {
        self.last_refresh_millis
            .store(now_millis(), Ordering::Release);
        self.last_refresh_success.store(success, Ordering::Release);
    }
}

/// 端点是否在只读白名单内（旧 `isReadOnlyEndpoint`：显式保守白名单，
/// 未知端点一律不重试，避免对有副作用端点重复触发）。
fn is_read_only_endpoint(endpoint: &str) -> bool {
    READ_ONLY_PREFIXES
        .iter()
        .any(|prefix| endpoint.starts_with(prefix))
}

/// 解析能力清单 JSON（旧 `parseCapabilities`：逐字段 `path(...).asText("")`
/// 兜底，解析失败返回空表并只记日志）。
fn parse_capabilities(json: &str) -> BTreeMap<String, CapabilityStatus> {
    let Ok(serde_json::Value::Object(root)) = serde_json::from_str::<serde_json::Value>(json)
    else {
        tracing::error!("python capability registry parse failed");
        return BTreeMap::new();
    };
    root.into_iter()
        .map(|(domain, value)| {
            let status = CapabilityStatus {
                name: value
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                available: value
                    .get("available")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
                reason: value
                    .get("reason")
                    .filter(|reason| !reason.is_null())
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
            };
            (domain, status)
        })
        .collect()
}

/// 关联诊断头（旧 `addCorrelationHeaders`：四个头逐个过白名单后才发出；
/// 诊断头永不使一个本来合法的请求失败）。
fn correlation_headers(
    correlation: &Correlation,
    request_id: &str,
    attempt: u32,
) -> Vec<(HeaderName, String)> {
    let attempt_text = attempt.to_string();
    let candidates: [(HeaderName, Option<&str>); 4] = [
        (HeaderName::from_static("x-request-id"), Some(request_id)),
        (
            HeaderName::from_static("x-attempt"),
            Some(attempt_text.as_str()),
        ),
        (
            HeaderName::from_static("x-run-id"),
            correlation.run_id.as_deref(),
        ),
        (
            HeaderName::from_static("x-session-id"),
            correlation.session_id.as_deref(),
        ),
    ];
    candidates
        .into_iter()
        .filter_map(|(name, value)| {
            value
                .filter(|raw| is_safe_correlation_value(raw))
                .map(|raw| (name, raw.to_owned()))
        })
        .collect()
}

/// 关联头值白名单（旧 `SAFE_CORRELATION_HEADER = ^[A-Za-z0-9._:-]{1,128}$`，
/// 手写字符判定以免为一条正则引入 `regex` 依赖）。
fn is_safe_correlation_value(value: &str) -> bool {
    let length = value.chars().count();
    (1..=128).contains(&length)
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 只读白名单逐条对齐旧 `isReadOnlyEndpoint`；未列入端点一律不可重试。
    #[test]
    fn read_only_allow_list_matches_baseline() {
        for endpoint in [
            "/api/analysis/impact",
            "/api/code-intel/parse",
            "/api/code-path/trace",
            "/api/code-diagram/flow",
            "/api/git/diff",
        ] {
            assert!(
                is_read_only_endpoint(endpoint),
                "{endpoint} must be retryable"
            );
        }
        for endpoint in [
            "/api/browser/navigate",
            "/api/files/detect",
            "/api/http/probe",
            "",
        ] {
            assert!(
                !is_read_only_endpoint(endpoint),
                "{endpoint} must not be retried"
            );
        }
    }

    /// 关联头白名单：合法值放行、越界/非法字符拒发。
    #[test]
    fn correlation_values_pass_safe_pattern_only() {
        assert!(is_safe_correlation_value("run-1_a.b:c"));
        assert!(is_safe_correlation_value(&"a".repeat(128)));
        assert!(!is_safe_correlation_value(""));
        assert!(!is_safe_correlation_value(&"a".repeat(129)));
        assert!(!is_safe_correlation_value("has space"));
        assert!(!is_safe_correlation_value("hello\nworld"));
        assert!(!is_safe_correlation_value("中文"));
    }

    /// 四个头按旧序发出；非法 `run_id` 被静默丢弃而不影响其余头。
    #[test]
    fn correlation_headers_drop_unsafe_values_only() {
        let correlation = Correlation {
            run_id: Some("bad value".to_owned()),
            session_id: Some("sess-1".to_owned()),
        };
        let headers = correlation_headers(&correlation, "req-1", 2);
        let names: Vec<&str> = headers.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, ["x-request-id", "x-attempt", "x-session-id"]);
        assert_eq!(headers[1].1, "2");
    }

    /// 能力清单解析：`reason: null` → `None`，缺字段走兜底。
    #[test]
    fn capabilities_parse_with_null_reason_and_defaults() {
        let parsed = parse_capabilities(
            r#"{"CODE_INTEL":{"name":"代码智能","available":true,"reason":null},
                "GIT_ENHANCED":{"name":"Git 增强","available":false,"reason":"缺少 Python 包: git"},
                "ODD":{}}"#,
        );
        assert_eq!(parsed.len(), 3);
        let code_intel = &parsed["CODE_INTEL"];
        assert!(code_intel.is_available());
        assert_eq!(code_intel.reason, None);
        assert_eq!(
            parsed["GIT_ENHANCED"].reason.as_deref(),
            Some("缺少 Python 包: git")
        );
        assert!(!parsed["ODD"].available);
        assert!(parsed["ODD"].name.is_empty());
        assert!(parse_capabilities("not json").is_empty());
        assert!(parse_capabilities("[1,2]").is_empty());
    }

    /// 退避序列 500 / 1000 / 2000 ms（旧代码实际睡眠三档）。
    #[test]
    fn backoff_sequence_matches_baseline_code() {
        let delays: Vec<u128> = (0..MAX_RETRIES)
            .map(|attempt| (RETRY_BASE_DELAY * 2u32.pow(attempt)).as_millis())
            .collect();
        assert_eq!(delays, [500, 1000, 2000]);
    }

    /// 无侧车（socket 缺失）：能力查询恒 false、健康恒 false——核心对话不受影响。
    #[tokio::test]
    async fn missing_sidecar_degrades_without_error() {
        let client = PythonClient::new("/tmp/zk-python-no-such-socket-9f8e7d.sock");
        assert!(!client.is_capability_available("CODE_INTEL").await);
        assert!(!client.is_healthy().await);
        assert!(client.capabilities().is_empty());
        assert!(!client.last_refresh_succeeded());
        assert!(client.get("/api/health").await.is_none());
        let none: Option<serde_json::Value> = client
            .call_if_available(
                "CODE_INTEL",
                "/api/code-intel/parse",
                &serde_json::json!({ "content": "x", "language": "python" }),
                &Correlation::default(),
            )
            .await;
        assert!(none.is_none());
    }

    /// 缓存失效后时间戳与成败标记复位（侧车重启路径）。
    #[test]
    fn invalidate_resets_cache_state() {
        let client = PythonClient::new("/tmp/zk-python-invalidate.sock");
        client.mark_refresh(true);
        assert!(client.last_refresh_succeeded());
        assert!(client.last_refresh_millis() > 0);
        client.invalidate_capabilities();
        assert!(!client.last_refresh_succeeded());
        assert_eq!(client.last_refresh_millis(), 0);
    }
}
