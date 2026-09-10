//! MCP 服务器连接 — 握手、工具/资源/prompt 发现、工具调用与反向请求应答。
//!
//! 逐字对照 Java 基线 `mcp/McpServerConnection.java`（638 行）：
//! - `connect()`：关旧传输 → 传输工厂 → （SSE）注入断连回调 → `connect()` 限
//!   时 30s → 置 `CONNECTED` → 协议握手 → **最后**注册通知/反向请求分发器；
//!   任一步异常 → `FAILED`；
//! - `performProtocolHandshake()`：`initialize`（`protocolVersion 2024-11-05`
//!   带 `clientInfo` 与 `capabilities`）→ `notifications/initialized` →
//!   `tools/list`；失败 → `DEGRADED` 后按 `500ms << retry` 重试 3 次
//!   `tools/list`，任一次拿到工具即回 `CONNECTED`，全败则 warn；
//! - `discoverTools/discoverResources/listPrompts/readResource`：容错解析
//!   （缺 `name`/`uri` 的条目跳过、描述缺省空串、协议错误仅告警返回空）；
//! - `callTool`：非 `CONNECTED` 或无传输 → `SERVER_NOT_INITIALIZED`；参数为
//!   `{name, arguments}`，`progressToken` 非空时注入 `_meta`；
//! - `discoverResources` 结果缓存 TTL 5 分钟（含空结果）；
//! - `connectWithRetry()`：最多 4 次尝试（0..=3），退避 `500 * 2^attempt`；
//! - `dispatchNotification`：`notifications/progress` → 进度追踪器；
//!   `roots/list` → 以当前 roots 回写响应（无 `id` 视为通知丢弃）；其余 debug。
//!
//! 形态差异（Rust 侧必然）：
//! - Java 靠 `instanceof McpSseTransport` 向下转型注入断连回调；此处统一走
//!   [`McpTransport::set_disconnect_callback`]（非 SSE 传输默认空实现）；
//! - Java 的通知处理器是 `this::dispatchNotification` 强引用（GC 处理环）；
//!   Rust 用 [`std::sync::Weak`] 打断「连接 → 传输 → 闭包 → 连接」引用环，故 `connect`
//!   接收 `&Arc<Self>`；应答 `roots/list` 需 `await`，闭包内 `tokio::spawn`；
//! - Caffeine 缓存 → `Mutex<Option<(Instant, Vec<_>)>>`（`maximumSize(1)` 等价
//!   单槽）；
//! - `Thread.sleep` → `tokio::time::sleep`。
//!
//! 行为偏离（在 `docs/compatibility.md` 留痕）：
//! 1. **HTTP 不重复握手**：传输若自报 [`McpTransport::performs_own_handshake`]
//!    则跳过连接层的 `initialize` + `initialized`（见 [`crate::streamable_http`]）。
//! 2. **`SDK` / `ZHIKUN_AI_PROXY` 归入「不支持」分支**；与 Java 的
//!    `default` 分支同构：仅 warn
//!    并把状态置 `CONNECTED`（保持 Java「不支持类型不触发重连风暴」的取舍）。
//! 3. `prompts/get` 的解析从 `McpPromptAdapter.execute` 上提到连接层
//!    （Java 把协议解析写在适配器里），行为一致：`role` 缺省 `user`、
//!    `content` 支持字符串 / `{type,text}` 两形态。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::config::{McpServerConfig, McpTransportType};
use crate::error::{self, JsonRpcError, McpProtocolError};
use crate::jsonrpc::RequestId;
use crate::protocol::{
    CLIENT_NAME, METHOD_CANCELLED, METHOD_INITIALIZE, METHOD_INITIALIZED, METHOD_PROGRESS,
    METHOD_PROMPTS_GET, METHOD_PROMPTS_LIST, METHOD_RESOURCES_LIST, METHOD_RESOURCES_LIST_CHANGED,
    METHOD_RESOURCES_READ, METHOD_ROOTS_LIST, METHOD_TOOLS_CALL, METHOD_TOOLS_LIST,
    METHOD_TOOLS_LIST_CHANGED, PROTOCOL_VERSION, ProgressTracker, PromptArgument, PromptDefinition,
    PromptMessage, ResourceDefinition, RootsProvider, ToolDefinition, client_capabilities,
    client_info,
};
use crate::sse::{SseTransport, lock, read_lock, write_lock};
use crate::stdio::StdioTransport;
use crate::streamable_http::StreamableHttpTransport;
use crate::transport::{DEFAULT_REQUEST_TIMEOUT, McpTransport, timeout_or_default};
use crate::websocket::WebSocketTransport;

/// 传输层 `connect()` 的上限（对照 Java `transport.connect().get(30, SECONDS)`）。
const TRANSPORT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// 握手失败后的 `tools/list` 重试次数（对照 Java `for (retry = 0; retry < 3)`）。
const HANDSHAKE_RETRIES: u32 = 3;

/// 握手重试与重连退避基数（对照 Java `500L * (1L << retry)` 与
/// `500 * Math.pow(2, attempt)`）。
const BACKOFF_BASE_MS: u64 = 500;

/// `connectWithRetry` 的最大重连次数（对照 Java `MAX_RECONNECT_ATTEMPTS = 3`，
/// 循环 `0..=3` 共 4 次尝试）。
pub const MAX_RECONNECT_ATTEMPTS: u32 = 3;

/// 资源列表缓存 TTL（对照 Java Caffeine `expireAfterWrite(5, MINUTES)`）。
const RESOURCE_CACHE_TTL: Duration = Duration::from_mins(5);

/// `initialize` 响应日志的截断长度（对照 Java `substring(0, min(200, len))`）。
const INIT_LOG_LIMIT: usize = 200;

/// 工具列表变更回调（对照 Java `onToolsChanged(Runnable)`）。
pub type ToolsChangedCallback = Arc<dyn Fn() + Send + Sync>;

type ToolCallFuture<'a> = BoxFuture<'a, Result<Option<Value>, McpProtocolError>>;
type StartedToolCall<'a> = (RequestId, Arc<dyn McpTransport>, ToolCallFuture<'a>);

/// MCP 服务器连接状态 — 6 种（逐字对照 `McpConnectionStatus.java`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum McpConnectionStatus {
    /// 成功连接，工具/资源可用。
    Connected,
    /// 连接失败（启动错误、网络不可达）。
    Failed,
    /// 需要 OAuth 认证。
    NeedsAuth,
    /// 正在连接中（支持重连尝试）。
    Pending,
    /// 被用户/配置禁用。
    Disabled,
    /// 传输连接正常但协议握手失败，功能受限。
    Degraded,
}

impl McpConnectionStatus {
    /// 线协议字面量（与 Java 枚举名一致，前端契约依赖）。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "CONNECTED",
            Self::Failed => "FAILED",
            Self::NeedsAuth => "NEEDS_AUTH",
            Self::Pending => "PENDING",
            Self::Disabled => "DISABLED",
            Self::Degraded => "DEGRADED",
        }
    }
}

impl std::fmt::Display for McpConnectionStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// 单个 MCP 服务器的连接实例。
pub struct McpServerConnection {
    config: McpServerConfig,
    status: RwLock<McpConnectionStatus>,
    tools: RwLock<Vec<ToolDefinition>>,
    resources: RwLock<Vec<ResourceDefinition>>,
    reconnect_attempts: AtomicU32,
    /// Monotonic identity of the installed transport.  A reconnect reuses this
    /// connection object, so pointer identity alone cannot reject callbacks and
    /// responses emitted by the previous transport.
    transport_generation: AtomicU64,
    transport: RwLock<Option<Arc<dyn McpTransport>>>,
    lifecycle: tokio::sync::Mutex<()>,
    resource_cache: Mutex<Option<(Instant, Vec<ResourceDefinition>)>>,
    roots_provider: RwLock<Option<Arc<RootsProvider>>>,
    progress_tracker: RwLock<Option<Arc<dyn ProgressTracker>>>,
    tools_changed: RwLock<Option<ToolsChangedCallback>>,
}

impl McpServerConnection {
    /// 创建连接实例（状态 `PENDING`，尚未建立传输）。
    ///
    /// 返回 [`Arc`]：通知/反向请求分发器需以 [`std::sync::Weak`] 回指本实例。
    #[must_use]
    pub fn new(config: McpServerConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            status: RwLock::new(McpConnectionStatus::Pending),
            tools: RwLock::new(Vec::new()),
            resources: RwLock::new(Vec::new()),
            reconnect_attempts: AtomicU32::new(0),
            transport_generation: AtomicU64::new(0),
            transport: RwLock::new(None),
            lifecycle: tokio::sync::Mutex::new(()),
            resource_cache: Mutex::new(None),
            roots_provider: RwLock::new(None),
            progress_tracker: RwLock::new(None),
            tools_changed: RwLock::new(None),
        })
    }

    /// 注入 Roots 提供者（响应服务端 `roots/list` 反向请求）。
    pub fn set_roots_provider(&self, provider: Arc<RootsProvider>) {
        *write_lock(&self.roots_provider) = Some(provider);
    }

    /// 注入进度追踪器（转发 `notifications/progress`）。
    pub fn set_progress_tracker(&self, tracker: Arc<dyn ProgressTracker>) {
        *write_lock(&self.progress_tracker) = Some(tracker);
    }

    /// 注册工具列表变更监听器。
    pub fn on_tools_changed(&self, callback: ToolsChangedCallback) {
        *write_lock(&self.tools_changed) = Some(callback);
    }

    /// 触发工具变更通知。
    pub fn notify_tools_changed(&self) {
        let callback = read_lock(&self.tools_changed).clone();
        if let Some(callback) = callback {
            callback();
        }
    }

    /// 服务器配置。
    #[must_use]
    pub fn config(&self) -> &McpServerConfig {
        &self.config
    }

    /// 服务器名。
    #[must_use]
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// 当前状态。
    #[must_use]
    pub fn status(&self) -> McpConnectionStatus {
        *read_lock(&self.status)
    }

    /// 覆写状态（管理器在健康检查/重连编排中使用）。
    pub fn set_status(&self, status: McpConnectionStatus) {
        *write_lock(&self.status) = status;
    }

    /// 已发现的工具快照。
    #[must_use]
    pub fn tools(&self) -> Vec<ToolDefinition> {
        read_lock(&self.tools).clone()
    }

    /// 覆写工具列表。
    pub fn set_tools(&self, tools: Vec<ToolDefinition>) {
        *write_lock(&self.tools) = tools;
    }

    /// 已发现的资源快照。
    #[must_use]
    pub fn resources(&self) -> Vec<ResourceDefinition> {
        read_lock(&self.resources).clone()
    }

    /// 覆写资源列表。
    pub fn set_resources(&self, resources: Vec<ResourceDefinition>) {
        *write_lock(&self.resources) = resources;
    }

    /// 累计重连次数。
    #[must_use]
    pub fn reconnect_attempts(&self) -> u32 {
        self.reconnect_attempts.load(Ordering::Acquire)
    }

    /// 重连计数 +1。
    pub fn increment_reconnect_attempts(&self) {
        self.reconnect_attempts.fetch_add(1, Ordering::AcqRel);
    }

    /// 重置重连计数。
    pub fn reset_reconnect_attempts(&self) {
        self.reconnect_attempts.store(0, Ordering::Release);
    }

    /// 当前传输实例（无传输时 `None`）。
    #[must_use]
    pub fn transport(&self) -> Option<Arc<dyn McpTransport>> {
        read_lock(&self.transport).clone()
    }

    /// 连接是否存活（对照 Java `isAlive()`）。
    #[must_use]
    pub fn is_alive(&self) -> bool {
        self.transport()
            .is_some_and(|transport| transport.is_connected())
    }

    /// 测试专用：直接注入传输替身，跳过 [`Self::connect`] 的真实建连与握手
    /// （供 `tool_adapter` / `manager` 的跨模块单元测试构造已连接实例）。
    #[cfg(test)]
    pub(crate) fn set_transport_for_test(self: &Arc<Self>, transport: Arc<dyn McpTransport>) {
        let installed = Arc::clone(&transport);
        let generation = {
            let mut slot = write_lock(&self.transport);
            let generation = self.next_transport_generation();
            *slot = Some(transport);
            generation
        };
        self.bind_disconnect_handler(&installed, generation);
        self.bind_notification_handler(&installed, generation);
    }

    fn next_transport_generation(&self) -> u64 {
        self.transport_generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn transport_generation(&self) -> u64 {
        self.transport_generation.load(Ordering::Acquire)
    }

    /// Generation of the currently executable transport session.  Adapters
    /// persist this value with a tool invocation and revalidate it after any
    /// asynchronous authorization wait.
    #[must_use]
    pub fn current_transport_generation(&self) -> Option<u64> {
        let (generation, transport) = self.transport_snapshot()?;
        (self.status() == McpConnectionStatus::Connected && transport.is_connected())
            .then_some(generation)
    }

    /// Whether the captured generation still names the installed, connected
    /// transport session.
    #[must_use]
    pub fn is_transport_generation_current(&self, generation: u64) -> bool {
        let Some((current, transport)) = self.transport_snapshot() else {
            return false;
        };
        current == generation
            && self.status() == McpConnectionStatus::Connected
            && transport.is_connected()
    }

    /// Read the installed transport and its generation as one snapshot.
    /// Generation changes are serialized under the transport write lock, so a
    /// request can never accidentally pair an old transport with a new ID
    /// namespace.
    fn transport_snapshot(&self) -> Option<(u64, Arc<dyn McpTransport>)> {
        let slot = read_lock(&self.transport);
        let transport = slot.as_ref().map(Arc::clone)?;
        Some((self.transport_generation(), transport))
    }

    /// Execute a synchronous state transition only while `transport` is still
    /// the installed session.  Holding the read lock through the transition
    /// orders it before any reconnect/close generation change.
    fn with_current_transport<R>(
        &self,
        generation: u64,
        transport: &Arc<dyn McpTransport>,
        action: impl FnOnce() -> R,
    ) -> Option<R> {
        let current = read_lock(&self.transport);
        let matches = self.transport_generation() == generation
            && current
                .as_ref()
                .is_some_and(|value| Arc::ptr_eq(value, transport));
        if !matches {
            return None;
        }
        let result = action();
        drop(current);
        Some(result)
    }

    fn is_current_transport(&self, generation: u64, transport: &Arc<dyn McpTransport>) -> bool {
        self.with_current_transport(generation, transport, || ())
            .is_some()
    }

    /// Remove every capability derived from a transport session.  Resource
    /// declarations are security state: carrying them across reconnect would
    /// allow a new server session to read a URI it never declared.
    fn clear_transport_capabilities(&self, notify_tools: bool) {
        self.set_tools(Vec::new());
        self.set_resources(Vec::new());
        *lock(&self.resource_cache) = None;
        if notify_tools {
            self.notify_tools_changed();
        }
    }

    fn bind_disconnect_handler(
        self: &Arc<Self>,
        transport: &Arc<dyn McpTransport>,
        generation: u64,
    ) {
        let weak = Arc::downgrade(self);
        let weak_transport = Arc::downgrade(transport);
        transport.set_disconnect_callback(Arc::new(move || {
            if let (Some(connection), Some(transport)) = (weak.upgrade(), weak_transport.upgrade())
            {
                let handled = connection.with_current_transport(generation, &transport, || {
                    connection.set_status(McpConnectionStatus::Failed);
                    connection.clear_transport_capabilities(true);
                });
                if handled.is_some() {
                    tracing::debug!(
                        server = %connection.name(),
                        generation,
                        "MCP transport disconnect callback triggered"
                    );
                } else {
                    tracing::debug!(
                        server = %connection.name(),
                        generation,
                        "Ignored disconnect from stale MCP transport"
                    );
                }
            }
        }));
    }

    fn bind_notification_handler(
        self: &Arc<Self>,
        transport: &Arc<dyn McpTransport>,
        generation: u64,
    ) {
        let weak = Arc::downgrade(self);
        let weak_transport = Arc::downgrade(transport);
        transport.set_notification_handler(Arc::new(move |node| {
            if let (Some(connection), Some(transport)) = (weak.upgrade(), weak_transport.upgrade())
            {
                tokio::spawn(async move {
                    if connection.is_current_transport(generation, &transport) {
                        connection
                            .dispatch_notification_from(node, &transport, generation)
                            .await;
                    } else {
                        tracing::debug!(
                            server = %connection.name(),
                            generation,
                            "Ignored notification from stale MCP transport"
                        );
                    }
                });
            }
        }));
    }

    /// 建立连接并完成 MCP 协议握手（对照 Java `connect()`）。
    pub async fn connect(self: &Arc<Self>) {
        let _lifecycle = self.lifecycle.lock().await;
        self.set_status(McpConnectionStatus::Pending);

        // Invalidate the old session before awaiting its close.  Late
        // responses/callbacks can no longer mutate this connection or its
        // dynamic directory entries.
        let (generation, previous) = {
            let mut slot = write_lock(&self.transport);
            let generation = self.next_transport_generation();
            (generation, slot.take())
        };
        self.clear_transport_capabilities(true);
        if let Some(previous) = previous {
            previous.close().await;
        }
        let Some(transport) = create_transport(&self.config) else {
            // 不支持的传输类型 — 容错标记为 CONNECTED（与 Java 原始行为一致）。
            self.set_status(McpConnectionStatus::Connected);
            return;
        };
        *write_lock(&self.transport) = Some(Arc::clone(&transport));

        self.bind_disconnect_handler(&transport, generation);

        match tokio::time::timeout(TRANSPORT_CONNECT_TIMEOUT, transport.connect()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::debug!(server = %self.name(), %error, "Failed to connect MCP server");
                if self.is_current_transport(generation, &transport) {
                    self.set_status(McpConnectionStatus::Failed);
                    self.clear_transport_capabilities(true);
                    write_lock(&self.transport).take();
                }
                transport.close().await;
                return;
            }
            Err(_) => {
                tracing::debug!(
                    server = %self.name(),
                    "Failed to connect MCP server: transport connect timed out"
                );
                if self.is_current_transport(generation, &transport) {
                    self.set_status(McpConnectionStatus::Failed);
                    self.clear_transport_capabilities(true);
                    write_lock(&self.transport).take();
                }
                transport.close().await;
                return;
            }
        }
        if !self.is_current_transport(generation, &transport) {
            transport.close().await;
            return;
        }
        self.set_status(McpConnectionStatus::Connected);
        tracing::info!(
            server = %self.name(),
            transport = %self.config.transport,
            "MCP server connected"
        );

        self.perform_protocol_handshake_for_generation(&transport, Some(generation))
            .await;
        if !self.is_current_transport(generation, &transport) {
            transport.close().await;
            return;
        }

        // 通知/反向请求分发器最后注册（对照 Java 顺序：握手期间的通知不处理）。
        self.bind_notification_handler(&transport, generation);
    }

    /// 带指数退避的连接重试（对照 Java `connectWithRetry()`）。
    pub async fn connect_with_retry(self: &Arc<Self>) {
        for attempt in 0..=MAX_RECONNECT_ATTEMPTS {
            self.connect().await;
            if self.status() == McpConnectionStatus::Connected {
                self.reset_reconnect_attempts();
                return;
            }
            self.increment_reconnect_attempts();
            if attempt < MAX_RECONNECT_ATTEMPTS {
                let delay = Duration::from_millis(BACKOFF_BASE_MS << attempt);
                tracing::warn!(
                    server = %self.name(),
                    attempt = attempt + 1,
                    total = MAX_RECONNECT_ATTEMPTS + 1,
                    delay_ms = delay.as_millis(),
                    "MCP server connection attempt failed, retrying"
                );
                tokio::time::sleep(delay).await;
            }
        }
        tracing::error!(
            server = %self.name(),
            attempts = MAX_RECONNECT_ATTEMPTS + 1,
            "MCP server connection failed"
        );
    }

    /// MCP 协议握手（对照 Java `performProtocolHandshake()`）。
    #[cfg(test)]
    async fn perform_protocol_handshake(&self, transport: &Arc<dyn McpTransport>) {
        self.perform_protocol_handshake_for_generation(transport, None)
            .await;
    }

    async fn perform_protocol_handshake_for_generation(
        &self,
        transport: &Arc<dyn McpTransport>,
        generation: Option<u64>,
    ) {
        let Err(error) = self.handshake_once(transport, generation).await else {
            return;
        };
        if generation.is_some_and(|value| !self.is_current_transport(value, transport)) {
            return;
        }
        tracing::error!(server = %self.name(), %error, "MCP protocol handshake failed");
        self.set_status(McpConnectionStatus::Degraded);
        for retry in 0..HANDSHAKE_RETRIES {
            tokio::time::sleep(Duration::from_millis(BACKOFF_BASE_MS << retry)).await;
            if generation.is_some_and(|value| !self.is_current_transport(value, transport)) {
                return;
            }
            self.discover_tools_with(transport, generation).await;
            if !read_lock(&self.tools).is_empty() {
                self.set_status(McpConnectionStatus::Connected);
                tracing::info!(
                    server = %self.name(),
                    retry = retry + 1,
                    "MCP handshake retry succeeded"
                );
                return;
            }
        }
        tracing::warn!(
            server = %self.name(),
            "All MCP handshake retries exhausted — tools unavailable"
        );
    }

    /// 单次握手：`initialize` → `notifications/initialized` → `tools/list`。
    async fn handshake_once(
        &self,
        transport: &Arc<dyn McpTransport>,
        generation: Option<u64>,
    ) -> Result<(), McpProtocolError> {
        if !transport.performs_own_handshake() {
            let params = json!({
                "protocolVersion": PROTOCOL_VERSION,
                "clientInfo": client_info(CLIENT_NAME),
                "capabilities": client_capabilities(),
            });
            let result = transport
                .send_request(
                    transport.next_request_id(),
                    METHOD_INITIALIZE,
                    Some(params),
                    DEFAULT_REQUEST_TIMEOUT,
                )
                .await?;
            if generation.is_some_and(|value| !self.is_current_transport(value, transport)) {
                return Err(McpProtocolError::not_initialized(
                    "MCP transport generation changed during initialize",
                ));
            }
            tracing::info!(
                server = %self.name(),
                response = %truncate_for_log(result.as_ref()),
                "MCP server initialize response"
            );
            transport.send_notification(METHOD_INITIALIZED, None).await;
            tracing::info!(server = %self.name(), "MCP initialized notification sent");
        }
        self.discover_tools_with(transport, generation).await;
        Ok(())
    }

    /// 发现工具（对照 Java `discoverTools()`：协议错误仅告警）。
    pub async fn discover_tools(&self) {
        let Some(transport) = self.transport() else {
            tracing::warn!(server = %self.name(), "Cannot discover tools — no transport");
            return;
        };
        let generation = self.transport_generation();
        self.discover_tools_with(&transport, Some(generation)).await;
    }

    async fn discover_tools_with(
        &self,
        transport: &Arc<dyn McpTransport>,
        generation: Option<u64>,
    ) {
        let outcome = transport
            .send_request(
                transport.next_request_id(),
                METHOD_TOOLS_LIST,
                Some(json!({})),
                DEFAULT_REQUEST_TIMEOUT,
            )
            .await;
        if generation.is_some_and(|value| !self.is_current_transport(value, transport)) {
            tracing::debug!(
                server = %self.name(),
                "Ignored tools/list response from stale MCP transport"
            );
            return;
        }
        let result = match outcome {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(server = %self.name(), %error, "tools/list failed for MCP server");
                return;
            }
        };
        let Some(items) = result
            .as_ref()
            .and_then(|result| result.get("tools"))
            .and_then(Value::as_array)
        else {
            tracing::info!(server = %self.name(), "MCP tools/list returned no tools");
            return;
        };
        let discovered: Vec<ToolDefinition> = items
            .iter()
            .filter_map(|node| {
                let name = text_opt(node, "name")?;
                Some(ToolDefinition {
                    name,
                    description: text_or(node, "description", ""),
                    input_schema: node
                        .get("inputSchema")
                        .cloned()
                        .unwrap_or_else(|| json!({})),
                })
            })
            .collect();
        tracing::info!(
            server = %self.name(),
            count = discovered.len(),
            tools = ?discovered.iter().map(|tool| tool.name.as_str()).collect::<Vec<_>>(),
            "MCP tools/list discovered tools"
        );
        if let Some(generation) = generation {
            if self
                .with_current_transport(generation, transport, || self.set_tools(discovered))
                .is_none()
            {
                tracing::debug!(
                    server = %self.name(),
                    "Ignored parsed tools/list result from stale MCP transport"
                );
            }
        } else {
            self.set_tools(discovered);
        }
    }

    /// 调用 MCP 工具（对照 Java `callTool(name, args, timeoutMs, progressToken)`）。
    ///
    /// # Errors
    /// 非 `CONNECTED`、无传输、通信失败或服务端返回 `error` 时返回。
    pub async fn call_tool(
        &self,
        tool_name: &str,
        arguments: Option<Value>,
        timeout: Duration,
        progress_token: Option<&str>,
    ) -> Result<Option<Value>, McpProtocolError> {
        let (_, _, call) = self.start_tool_call(tool_name, arguments, timeout, progress_token)?;
        call.await
    }

    /// Prepare a `tools/call` and expose the exact JSON-RPC request ID before
    /// the request future is polled.  Cancellation notifications must refer to
    /// this ID, never to the unrelated progress token.
    pub(crate) fn start_tool_call<'a>(
        &'a self,
        tool_name: &str,
        arguments: Option<Value>,
        timeout: Duration,
        progress_token: Option<&str>,
    ) -> Result<StartedToolCall<'a>, McpProtocolError> {
        let status = self.status();
        let snapshot = self.transport_snapshot();
        let (Some((generation, transport)), McpConnectionStatus::Connected) = (snapshot, status)
        else {
            return Err(McpProtocolError::not_initialized(format!(
                "Server '{}' not connected (status: {status})",
                self.name()
            )));
        };
        let mut params = json!({
            "name": tool_name,
            "arguments": arguments.unwrap_or_else(|| json!({})),
        });
        if let Some(token) = progress_token.filter(|token| !token.is_empty()) {
            params["_meta"] = json!({ "progressToken": token });
        }
        let request_id = transport.next_request_id();
        let sent_id = request_id.clone();
        let request_transport = Arc::clone(&transport);
        let future = Box::pin(async move {
            let outcome = request_transport
                .send_request(
                    sent_id,
                    METHOD_TOOLS_CALL,
                    Some(params),
                    timeout_or_default(timeout),
                )
                .await;
            if self.is_current_transport(generation, &request_transport) {
                outcome
            } else {
                Err(McpProtocolError::not_initialized(
                    "MCP transport generation changed during tools/call",
                ))
            }
        });
        Ok((request_id, transport, future))
    }

    /// 通用 JSON-RPC 请求（对照 Java `request(method, params, timeoutMs)`）。
    ///
    /// # Errors
    /// 无传输、通信失败或服务端返回 `error` 时返回。
    pub async fn request(
        &self,
        method: &str,
        params: Option<Value>,
        timeout: Duration,
    ) -> Result<Option<Value>, McpProtocolError> {
        self.request_with_session(method, params, timeout)
            .await
            .map(|(result, _, _)| result)
    }

    async fn request_with_session(
        &self,
        method: &str,
        params: Option<Value>,
        timeout: Duration,
    ) -> Result<(Option<Value>, u64, Arc<dyn McpTransport>), McpProtocolError> {
        let Some((generation, transport)) = self.transport_snapshot() else {
            return Err(McpProtocolError::not_initialized(format!(
                "Server '{}' has no transport",
                self.name()
            )));
        };
        let outcome = transport
            .send_request(
                transport.next_request_id(),
                method,
                params,
                timeout_or_default(timeout),
            )
            .await;
        if self.is_current_transport(generation, &transport) {
            outcome.map(|result| (result, generation, transport))
        } else {
            Err(McpProtocolError::not_initialized(
                "MCP transport generation changed during request",
            ))
        }
    }

    /// 发送通知（无传输时静默丢弃，对照 Java `if (transport != null)`）。
    pub async fn send_notification(&self, method: &str, params: Option<Value>) {
        if let Some(transport) = self.transport() {
            transport.send_notification(method, params).await;
        }
    }

    /// 发送 `notifications/cancelled`（对照 Java `sendCancelNotification`）。
    pub async fn send_cancel_notification(&self, request_id: &RequestId, reason: Option<&str>) {
        let Some((_, transport)) = self.transport_snapshot() else {
            return;
        };
        self.send_cancel_notification_on(&transport, request_id, reason)
            .await;
    }

    /// Send cancellation to the exact transport that owns `request_id`.
    /// Reconnect may install a new session while a call is in flight; routing
    /// an old numeric ID to that new session could cancel an unrelated call.
    pub(crate) async fn send_cancel_notification_on(
        &self,
        transport: &Arc<dyn McpTransport>,
        request_id: &RequestId,
        reason: Option<&str>,
    ) {
        let reason = reason.unwrap_or("user_cancelled");
        transport
            .send_notification(
                METHOD_CANCELLED,
                Some(json!({ "requestId": request_id, "reason": reason })),
            )
            .await;
        tracing::info!(
            server = %self.name(),
            request_id = %request_id,
            reason,
            "MCP cancel notification sent"
        );
    }

    /// 健康探测（无传输 → `false`，对照 Java `sendHealthPing()`）。
    pub async fn send_health_ping(&self) -> bool {
        let Some((generation, transport)) = self.transport_snapshot() else {
            return false;
        };
        let healthy = transport.send_health_ping().await;
        healthy && self.is_current_transport(generation, &transport)
    }

    /// 关闭连接并清空能力（对照 Java `close()`）。
    pub async fn close(&self) {
        let _lifecycle = self.lifecycle.lock().await;
        self.set_status(McpConnectionStatus::Disabled);
        let transport = {
            let mut slot = write_lock(&self.transport);
            self.next_transport_generation();
            slot.take()
        };
        self.clear_transport_capabilities(true);
        if let Some(transport) = transport {
            transport.close().await;
        }
    }

    /// 发现资源（TTL 5 分钟缓存，含空结果；对照 Java `discoverResources()`）。
    pub async fn discover_resources(&self) -> Vec<ResourceDefinition> {
        let status = self.status();
        if status != McpConnectionStatus::Connected {
            tracing::warn!(
                server = %self.name(),
                %status,
                "Cannot discover resources — server not connected"
            );
            return Vec::new();
        }
        if let Some(cached) = self.cached_resources() {
            return cached;
        }
        let outcome = self
            .request_with_session(
                METHOD_RESOURCES_LIST,
                Some(json!({})),
                DEFAULT_REQUEST_TIMEOUT,
            )
            .await;
        let (result, generation, transport) = match outcome {
            Ok(session) => session,
            Err(error) => {
                tracing::warn!(
                    server = %self.name(),
                    %error,
                    "resources/list not supported by MCP server"
                );
                return Vec::new();
            }
        };
        let items = result
            .as_ref()
            .and_then(|result| result.get("resources"))
            .and_then(Value::as_array);
        let Some(items) = items else {
            tracing::info!(server = %self.name(), "MCP resources/list returned no resources");
            self.with_current_transport(generation, &transport, || {
                self.set_resources(Vec::new());
                self.store_resources_cache(Vec::new());
            });
            return Vec::new();
        };
        let discovered: Vec<ResourceDefinition> = items
            .iter()
            .filter_map(|node| {
                let uri = text_opt(node, "uri")?;
                Some(ResourceDefinition {
                    uri,
                    name: text_or(node, "name", ""),
                    mime_type: text_opt(node, "mimeType"),
                    description: text_opt(node, "description"),
                })
            })
            .collect();
        tracing::info!(
            server = %self.name(),
            count = discovered.len(),
            "MCP resources/list discovered resources"
        );
        if self
            .with_current_transport(generation, &transport, || {
                self.set_resources(discovered.clone());
                self.store_resources_cache(discovered.clone());
            })
            .is_some()
        {
            discovered
        } else {
            tracing::debug!(
                server = %self.name(),
                "Ignored resources/list response from stale MCP transport"
            );
            Vec::new()
        }
    }

    /// 使资源缓存失效（对照 Java `invalidateResourceCache()`）。
    pub fn invalidate_resource_cache(&self) {
        *lock(&self.resource_cache) = None;
    }

    fn cached_resources(&self) -> Option<Vec<ResourceDefinition>> {
        let mut guard = lock(&self.resource_cache);
        match guard.as_ref() {
            Some((stored_at, entries)) if stored_at.elapsed() < RESOURCE_CACHE_TTL => {
                Some(entries.clone())
            }
            Some(_) => {
                *guard = None;
                None
            }
            None => None,
        }
    }

    fn store_resources_cache(&self, entries: Vec<ResourceDefinition>) {
        *lock(&self.resource_cache) = Some((Instant::now(), entries));
    }

    /// 读取资源内容（对照 Java `readResource(uri)`）。
    ///
    /// # Errors
    /// 无传输、通信失败，或响应缺 `contents` 数组时返回。
    pub async fn read_resource(&self, uri: &str) -> Result<String, McpProtocolError> {
        let response = self
            .request(
                METHOD_RESOURCES_READ,
                Some(json!({ "uri": uri })),
                DEFAULT_REQUEST_TIMEOUT,
            )
            .await?;
        let contents = response
            .as_ref()
            .and_then(|response| response.get("contents"))
            .and_then(Value::as_array)
            .ok_or_else(|| {
                McpProtocolError::from_rpc(JsonRpcError::new(
                    error::INVALID_REQUEST,
                    "Invalid resources/read response: missing 'contents' array",
                ))
            })?;
        let mut buffer = String::new();
        for content in contents {
            if let Some(text) = content.get("text") {
                buffer.push_str(&value_text(text));
            } else if content.get("blob").is_some() {
                buffer.push_str("[Binary content: ");
                buffer.push_str(&text_or(content, "mimeType", "unknown"));
                buffer.push(']');
            }
        }
        Ok(buffer)
    }

    /// 列出 prompt 模板（对照 Java `listPrompts()`）。
    pub async fn list_prompts(&self) -> Vec<PromptDefinition> {
        let status = self.status();
        if status != McpConnectionStatus::Connected {
            tracing::warn!(
                server = %self.name(),
                %status,
                "Cannot list prompts — server not connected"
            );
            return Vec::new();
        }
        let outcome = self
            .request(METHOD_PROMPTS_LIST, None, DEFAULT_REQUEST_TIMEOUT)
            .await;
        let result = match outcome {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(
                    server = %self.name(),
                    %error,
                    "prompts/list not supported by MCP server"
                );
                return Vec::new();
            }
        };
        let Some(items) = result
            .as_ref()
            .and_then(|result| result.get("prompts"))
            .and_then(Value::as_array)
        else {
            return Vec::new();
        };
        let prompts: Vec<PromptDefinition> = items
            .iter()
            .filter_map(|node| {
                let name = text_opt(node, "name")?;
                let arguments = node
                    .get("arguments")
                    .and_then(Value::as_array)
                    .map(|args| {
                        args.iter()
                            .map(|arg| PromptArgument {
                                name: text_or(arg, "name", ""),
                                description: text_or(arg, "description", ""),
                                required: arg
                                    .get("required")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(false),
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Some(PromptDefinition {
                    name,
                    description: text_or(node, "description", ""),
                    arguments,
                })
            })
            .collect();
        tracing::info!(
            server = %self.name(),
            count = prompts.len(),
            "Discovered prompts from MCP server"
        );
        prompts
    }

    /// 渲染 prompt 模板（`prompts/get`，对照 Java `McpPromptAdapter.execute`
    /// 的协议部分：`role` 缺省 `user`，`content` 支持字符串与 `{type,text}`）。
    ///
    /// # Errors
    /// 无传输、通信失败或服务端返回 `error` 时返回。
    pub async fn get_prompt(
        &self,
        name: &str,
        arguments: &BTreeMap<String, String>,
    ) -> Result<Vec<PromptMessage>, McpProtocolError> {
        let result = self
            .request(
                METHOD_PROMPTS_GET,
                Some(json!({ "name": name, "arguments": arguments })),
                DEFAULT_REQUEST_TIMEOUT,
            )
            .await?;
        let Some(items) = result
            .as_ref()
            .and_then(|result| result.get("messages"))
            .and_then(Value::as_array)
        else {
            tracing::warn!(
                server = %self.name(),
                prompt = name,
                "Prompt returned non-array messages"
            );
            return Ok(Vec::new());
        };
        Ok(items
            .iter()
            .map(|message| PromptMessage {
                role: text_or(message, "role", "user"),
                content: extract_content(message.get("content")),
            })
            .collect())
    }

    /// Test/direct-entry wrapper around the generation-bound dispatcher.
    #[cfg(test)]
    async fn dispatch_notification(&self, notification: Value) {
        let Some((generation, transport)) = self.transport_snapshot() else {
            return;
        };
        self.dispatch_notification_from(notification, &transport, generation)
            .await;
    }

    /// Dispatch a notification only within the transport session that emitted
    /// it.  In particular, reverse requests must never be answered on a newly
    /// reconnected transport that happens to reuse the same numeric ID.
    async fn dispatch_notification_from(
        &self,
        notification: Value,
        transport: &Arc<dyn McpTransport>,
        generation: u64,
    ) {
        if !self.is_current_transport(generation, transport) {
            return;
        }
        let method = text_or(&notification, "method", "");
        match method.as_str() {
            METHOD_PROGRESS => {
                self.with_current_transport(generation, transport, || {
                    self.handle_progress(&notification);
                });
            }
            METHOD_TOOLS_LIST_CHANGED => {
                // A changed directory is untrusted until the replacement list
                // succeeds.  Clear first so a failed refresh cannot leave a
                // removed remote tool executable.
                if self
                    .with_current_transport(generation, transport, || self.set_tools(Vec::new()))
                    .is_none()
                {
                    return;
                }
                self.notify_tools_changed();
                self.discover_tools_with(transport, Some(generation)).await;
                self.with_current_transport(generation, transport, || {
                    self.notify_tools_changed();
                });
            }
            METHOD_RESOURCES_LIST_CHANGED => {
                self.with_current_transport(generation, transport, || {
                    self.set_resources(Vec::new());
                    self.invalidate_resource_cache();
                });
            }
            METHOD_ROOTS_LIST => {
                self.handle_roots_list_from(&notification, transport, generation)
                    .await;
            }
            other => tracing::debug!(method = other, "Unhandled MCP notification"),
        }
    }

    /// 转发进度通知；未注入追踪器时静默忽略（对照 Java `handleProgress`）。
    fn handle_progress(&self, notification: &Value) {
        let tracker = read_lock(&self.progress_tracker).clone();
        if let Some(tracker) = tracker {
            tracker.handle_progress_notification(notification);
        } else {
            tracing::debug!(
                notification = %notification,
                "MCP progress notification dropped (no tracker bound)"
            );
        }
    }

    /// 应答 `roots/list` 反向请求（对照 Java `handleRootsList`）。
    async fn handle_roots_list_from(
        &self,
        notification: &Value,
        transport: &Arc<dyn McpTransport>,
        generation: u64,
    ) {
        let Some(id) = notification
            .get("id")
            .filter(|id| !id.is_null())
            .and_then(|id| serde_json::from_value::<RequestId>(id.clone()).ok())
        else {
            tracing::debug!("Received roots/list without id, ignoring (treated as notification)");
            return;
        };
        if !self.is_current_transport(generation, transport) {
            return;
        }
        let roots = read_lock(&self.roots_provider)
            .as_ref()
            .map(|provider| provider.current_roots())
            .unwrap_or_default();
        let count = roots.len();
        transport.send_response(id, json!({ "roots": roots })).await;
        tracing::debug!(server = %self.name(), roots = count, "Responded to roots/list");
    }
}

/// 传输工厂（对照 Java `createTransport`）。
///
/// `None` = 不支持的传输类型：调用方按 Java 语义把状态置 `CONNECTED` 而不重试。
fn create_transport(config: &McpServerConfig) -> Option<Arc<dyn McpTransport>> {
    match config.transport {
        McpTransportType::Stdio => Some(Arc::new(StdioTransport::new(config))),
        McpTransportType::Sse | McpTransportType::SseIde => {
            match SseTransport::from_config(config) {
                Ok(transport) => Some(Arc::new(transport)),
                Err(error) => {
                    tracing::warn!(
                        server = %config.name,
                        %error,
                        "Failed to create SSE transport for MCP server"
                    );
                    None
                }
            }
        }
        McpTransportType::Http => match StreamableHttpTransport::from_config(config) {
            Ok(transport) => Some(Arc::new(transport)),
            Err(error) => {
                tracing::warn!(
                    server = %config.name,
                    %error,
                    "Failed to create Streamable HTTP transport for MCP server"
                );
                None
            }
        },
        McpTransportType::Ws | McpTransportType::WsIde => {
            match WebSocketTransport::from_config(config) {
                Ok(transport) => Some(Arc::new(transport)),
                Err(error) => {
                    tracing::warn!(
                        server = %config.name,
                        %error,
                        "Failed to create WebSocket transport for MCP server"
                    );
                    None
                }
            }
        }
        other => {
            tracing::warn!(
                transport = %other,
                server = %config.name,
                "Unsupported transport type for MCP server, no transport created"
            );
            None
        }
    }
}

/// 对照 Jackson `path(field).asText(default)`：缺失/`null` → `default`；文本节点
/// 取原值；其他标量取字面量。
fn text_or(node: &Value, field: &str, default: &str) -> String {
    match node.get(field) {
        None | Some(Value::Null) => default.to_owned(),
        Some(value) => value_text(value),
    }
}

/// 对照 Jackson `path(field).asText(null)`：缺失/`null` → `None`。
fn text_opt(node: &Value, field: &str) -> Option<String> {
    match node.get(field) {
        None | Some(Value::Null) => None,
        Some(value) => Some(value_text(value)),
    }
}

/// 单值文本化（字符串取原值，其余取 JSON 字面量，对照 Jackson `asText()`）。
fn value_text(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), ToOwned::to_owned)
}

/// 提取 prompt `content`（对照 Java `McpPromptAdapter.extractContent`：文本节点
/// 取原值、对象取 `text`（缺省空串）、其余取字面量）。
fn extract_content(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Object(_)) => content
            .and_then(|content| content.get("text"))
            .map_or_else(String::new, value_text),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

/// `initialize` 响应日志截断（对照 Java `substring(0, min(200, len))`；`null`
/// 打印 `"null"`）。
fn truncate_for_log(result: Option<&Value>) -> String {
    let Some(result) = result else {
        return "null".to_owned();
    };
    let text = result.to_string();
    match text.char_indices().nth(INIT_LOG_LIMIT) {
        Some((index, _)) => text[..index].to_owned(),
        None => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{DisconnectCallback, NotificationHandler};
    use futures::future::BoxFuture;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;

    type StubResponder =
        Arc<dyn Fn(&str, Option<Value>) -> Result<Option<Value>, McpProtocolError> + Send + Sync>;

    /// 可编程传输替身 — 记录请求/通知/反向响应，按方法名返回预置结果。
    struct StubTransport {
        responder: StubResponder,
        connected: AtomicBool,
        requests: Mutex<Vec<(String, Option<Value>)>>,
        notifications: Mutex<Vec<(String, Option<Value>)>>,
        responses: Mutex<Vec<(RequestId, Value)>>,
        notification_handler: RwLock<Option<NotificationHandler>>,
        disconnect_callback: RwLock<Option<DisconnectCallback>>,
    }

    impl StubTransport {
        fn new(responder: StubResponder) -> Arc<Self> {
            Arc::new(Self {
                responder,
                connected: AtomicBool::new(true),
                requests: Mutex::new(Vec::new()),
                notifications: Mutex::new(Vec::new()),
                responses: Mutex::new(Vec::new()),
                notification_handler: RwLock::new(None),
                disconnect_callback: RwLock::new(None),
            })
        }

        /// 只按 `method → result` 映射作答，未登记的方法回 `METHOD_NOT_FOUND`。
        fn scripted(script: HashMap<&'static str, Result<Value, McpProtocolError>>) -> Arc<Self> {
            Self::new(Arc::new(move |method: &str, _params| {
                match script.get(method) {
                    Some(Ok(result)) => Ok(Some(result.clone())),
                    Some(Err(error)) => Err(error.clone()),
                    None => Err(McpProtocolError::from_rpc(JsonRpcError::method_not_found(
                        method,
                    ))),
                }
            }))
        }

        fn requests(&self) -> Vec<(String, Option<Value>)> {
            lock(&self.requests).clone()
        }

        fn notifications(&self) -> Vec<(String, Option<Value>)> {
            lock(&self.notifications).clone()
        }

        fn responses(&self) -> Vec<(RequestId, Value)> {
            lock(&self.responses).clone()
        }

        fn emit_notification(&self, value: Value) {
            if let Some(handler) = read_lock(&self.notification_handler).clone() {
                handler(value);
            }
        }

        fn disconnect(&self) {
            self.connected.store(false, Ordering::Release);
            if let Some(callback) = read_lock(&self.disconnect_callback).clone() {
                callback();
            }
        }
    }

    impl McpTransport for StubTransport {
        fn next_request_id(&self) -> RequestId {
            RequestId::Number(1)
        }

        fn connect(&self) -> BoxFuture<'_, Result<(), McpProtocolError>> {
            Box::pin(async { Ok(()) })
        }

        fn send_request<'a>(
            &'a self,
            _request_id: RequestId,
            method: &'a str,
            params: Option<Value>,
            _timeout: Duration,
        ) -> BoxFuture<'a, Result<Option<Value>, McpProtocolError>> {
            Box::pin(async move {
                lock(&self.requests).push((method.to_owned(), params.clone()));
                (self.responder)(method, params)
            })
        }

        fn send_notification<'a>(
            &'a self,
            method: &'a str,
            params: Option<Value>,
        ) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                lock(&self.notifications).push((method.to_owned(), params));
            })
        }

        fn send_response(&self, id: RequestId, result: Value) -> BoxFuture<'_, ()> {
            Box::pin(async move {
                lock(&self.responses).push((id, result));
            })
        }

        fn is_connected(&self) -> bool {
            self.connected.load(Ordering::Acquire)
        }

        fn set_notification_handler(&self, handler: NotificationHandler) {
            *write_lock(&self.notification_handler) = Some(handler);
        }

        fn set_disconnect_callback(&self, callback: DisconnectCallback) {
            *write_lock(&self.disconnect_callback) = Some(callback);
        }

        fn close(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {
                self.connected.store(false, Ordering::Release);
            })
        }
    }

    fn connected_with(transport: Arc<dyn McpTransport>) -> Arc<McpServerConnection> {
        let connection = McpServerConnection::new(McpServerConfig::stdio(
            "fake",
            "/bin/true",
            Vec::<String>::new(),
        ));
        *write_lock(&connection.transport) = Some(transport);
        connection.set_status(McpConnectionStatus::Connected);
        connection
    }

    fn script(
        pairs: Vec<(&'static str, Result<Value, McpProtocolError>)>,
    ) -> HashMap<&'static str, Result<Value, McpProtocolError>> {
        pairs.into_iter().collect()
    }

    #[test]
    fn status_wire_names_match_java_enum() {
        assert_eq!(McpConnectionStatus::NeedsAuth.as_str(), "NEEDS_AUTH");
        assert_eq!(McpConnectionStatus::Degraded.to_string(), "DEGRADED");
        assert_eq!(
            serde_json::to_value(McpConnectionStatus::Connected).expect("serialize"),
            json!("CONNECTED")
        );
    }

    #[test]
    fn jackson_text_semantics_are_preserved() {
        let node = json!({"a": "x", "b": 5, "c": null, "d": true});
        assert_eq!(text_or(&node, "a", "z"), "x");
        assert_eq!(text_or(&node, "b", "z"), "5");
        assert_eq!(text_or(&node, "c", "z"), "z");
        assert_eq!(text_or(&node, "missing", "z"), "z");
        assert_eq!(text_opt(&node, "c"), None);
        assert_eq!(text_opt(&node, "d"), Some("true".to_owned()));
        assert_eq!(extract_content(Some(&json!("plain"))), "plain");
        assert_eq!(
            extract_content(Some(&json!({"type": "text", "text": "wrapped"}))),
            "wrapped"
        );
        assert_eq!(extract_content(Some(&json!({"type": "image"}))), "");
        assert_eq!(extract_content(None), "");
    }

    #[tokio::test]
    async fn stdio_handshake_discovers_tools_and_sends_initialized() {
        let connection = McpServerConnection::new(McpServerConfig::stdio(
            "fake",
            "/bin/sh",
            vec!["-c".to_owned(), fake_server_script()],
        ));
        connection.connect().await;
        assert_eq!(connection.status(), McpConnectionStatus::Connected);
        assert!(connection.is_alive());
        let tools = connection.tools();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "echo");
        assert_eq!(tools[0].description, "echo tool");
        assert_eq!(tools[0].input_schema, json!({"type": "object"}));

        let result = connection
            .call_tool(
                "echo",
                Some(json!({"text": "hi"})),
                Duration::from_secs(5),
                None,
            )
            .await
            .expect("call");
        assert_eq!(
            result,
            Some(json!({"content": [{"type": "text", "text": "ok"}]}))
        );

        connection.close().await;
        assert_eq!(connection.status(), McpConnectionStatus::Disabled);
        assert!(connection.tools().is_empty());
        assert!(!connection.is_alive());
    }

    /// 最小 STDIO MCP 服务端：按方法名回写固定响应，忽略通知。
    fn fake_server_script() -> String {
        r#"while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"serverInfo":{"name":"fake"}}}\n' "$id" ;;
    *'"method":"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"echo","description":"echo tool","inputSchema":{"type":"object"}}]}}\n' "$id" ;;
    *'"method":"tools/call"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"ok"}]}}\n' "$id" ;;
  esac
done"#
            .to_owned()
    }

    #[tokio::test]
    async fn unsupported_sdk_transport_is_marked_connected_without_transport() {
        let mut config = McpServerConfig::sse("ws-server", "http://127.0.0.1:9/mcp");
        config.transport = McpTransportType::Sdk;
        let connection = McpServerConnection::new(config);
        connection.connect().await;
        // 对照 Java：不支持类型容错标记 CONNECTED 且不创建传输。
        assert_eq!(connection.status(), McpConnectionStatus::Connected);
        assert!(connection.transport().is_none());
        assert!(!connection.is_alive());
    }

    #[tokio::test]
    async fn call_tool_requires_connected_status() {
        let transport = StubTransport::scripted(script(vec![]));
        let connection = connected_with(transport);
        connection.set_status(McpConnectionStatus::Degraded);
        let error = connection
            .call_tool("echo", None, Duration::from_secs(1), None)
            .await
            .expect_err("must fail");
        assert_eq!(error.code(), error::SERVER_NOT_INITIALIZED);
        assert_eq!(
            error.message(),
            "Server 'fake' not connected (status: DEGRADED)"
        );
    }

    #[tokio::test]
    async fn call_tool_shapes_params_and_injects_progress_token() {
        let transport = StubTransport::scripted(script(vec![(
            METHOD_TOOLS_CALL,
            Ok(json!({"content": []})),
        )]));
        let connection = connected_with(Arc::clone(&transport) as Arc<dyn McpTransport>);

        connection
            .call_tool("echo", None, Duration::ZERO, Some(""))
            .await
            .expect("call");
        connection
            .call_tool(
                "echo",
                Some(json!({"text": "hi"})),
                Duration::from_secs(3),
                Some("tok-1"),
            )
            .await
            .expect("call");

        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        // 空 progressToken 不注入 _meta；arguments 缺省为空对象。
        assert_eq!(
            requests[0].1,
            Some(json!({"name": "echo", "arguments": {}}))
        );
        assert_eq!(
            requests[1].1,
            Some(json!({
                "name": "echo",
                "arguments": {"text": "hi"},
                "_meta": {"progressToken": "tok-1"}
            }))
        );
    }

    #[tokio::test]
    async fn request_without_transport_reports_missing_transport() {
        let connection = McpServerConnection::new(McpServerConfig::stdio(
            "fake",
            "/bin/true",
            Vec::<String>::new(),
        ));
        let error = connection
            .request(METHOD_TOOLS_LIST, None, Duration::from_secs(1))
            .await
            .expect_err("must fail");
        assert_eq!(error.code(), error::SERVER_NOT_INITIALIZED);
        assert_eq!(error.message(), "Server 'fake' has no transport");
    }

    #[tokio::test]
    async fn handshake_failure_degrades_then_recovers_on_retry() {
        let transport = StubTransport::scripted(script(vec![
            (
                METHOD_INITIALIZE,
                Err(McpProtocolError::timeout("Request timeout: initialize")),
            ),
            (
                METHOD_TOOLS_LIST,
                Ok(json!({"tools": [{"name": "late", "description": "d"}]})),
            ),
        ]));
        let connection = McpServerConnection::new(McpServerConfig::stdio(
            "fake",
            "/bin/true",
            Vec::<String>::new(),
        ));
        let dynamic: Arc<dyn McpTransport> = Arc::clone(&transport) as Arc<dyn McpTransport>;
        connection.perform_protocol_handshake(&dynamic).await;
        assert_eq!(connection.status(), McpConnectionStatus::Connected);
        assert_eq!(connection.tools().len(), 1);
        assert_eq!(connection.tools()[0].input_schema, json!({}));
    }

    #[tokio::test]
    async fn tools_list_skips_entries_without_name() {
        let transport = StubTransport::scripted(script(vec![(
            METHOD_TOOLS_LIST,
            Ok(json!({"tools": [{"description": "no name"}, {"name": "ok"}]})),
        )]));
        let connection = connected_with(transport);
        connection.discover_tools().await;
        let tools = connection.tools();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "ok");
        assert_eq!(tools[0].description, "");
    }

    #[tokio::test]
    async fn resources_are_cached_until_invalidated() {
        let transport = StubTransport::scripted(script(vec![(
            METHOD_RESOURCES_LIST,
            Ok(json!({"resources": [
                {"uri": "file:///a", "name": "a", "mimeType": "text/plain"},
                {"name": "missing uri"}
            ]})),
        )]));
        let connection = connected_with(Arc::clone(&transport) as Arc<dyn McpTransport>);

        let first = connection.discover_resources().await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].uri, "file:///a");
        assert_eq!(first[0].mime_type.as_deref(), Some("text/plain"));
        assert_eq!(first[0].description, None);

        // 第二次命中缓存 → 不再发请求。
        let second = connection.discover_resources().await;
        assert_eq!(second, first);
        assert_eq!(transport.requests().len(), 1);

        connection.invalidate_resource_cache();
        let third = connection.discover_resources().await;
        assert_eq!(third, first);
        assert_eq!(transport.requests().len(), 2);
    }

    #[tokio::test]
    async fn resource_discovery_requires_connected_status() {
        let transport = StubTransport::scripted(script(vec![]));
        let connection = connected_with(Arc::clone(&transport) as Arc<dyn McpTransport>);
        connection.set_status(McpConnectionStatus::Failed);
        assert!(connection.discover_resources().await.is_empty());
        assert!(connection.list_prompts().await.is_empty());
        assert!(transport.requests().is_empty());
    }

    #[tokio::test]
    async fn read_resource_concatenates_text_and_blob_placeholder() {
        let transport = StubTransport::scripted(script(vec![(
            METHOD_RESOURCES_READ,
            Ok(json!({"contents": [
                {"text": "hello "},
                {"blob": "AAAA", "mimeType": "image/png"},
                {"blob": "BBBB"}
            ]})),
        )]));
        let connection = connected_with(transport);
        let text = connection.read_resource("file:///a").await.expect("read");
        assert_eq!(
            text,
            "hello [Binary content: image/png][Binary content: unknown]"
        );
    }

    #[tokio::test]
    async fn read_resource_rejects_missing_contents_array() {
        let transport = StubTransport::scripted(script(vec![(
            METHOD_RESOURCES_READ,
            Ok(json!({"unexpected": true})),
        )]));
        let connection = connected_with(transport);
        let error = connection
            .read_resource("file:///a")
            .await
            .expect_err("must fail");
        assert_eq!(error.code(), error::INVALID_REQUEST);
        assert_eq!(
            error.message(),
            "Invalid resources/read response: missing 'contents' array"
        );
    }

    #[tokio::test]
    async fn prompts_are_parsed_with_defaults() {
        let transport = StubTransport::scripted(script(vec![
            (
                METHOD_PROMPTS_LIST,
                Ok(json!({"prompts": [
                    {"name": "review", "arguments": [{"name": "path", "required": true}]},
                    {"description": "no name"}
                ]})),
            ),
            (
                METHOD_PROMPTS_GET,
                Ok(json!({"messages": [
                    {"content": "bare"},
                    {"role": "assistant", "content": {"type": "text", "text": "rich"}}
                ]})),
            ),
        ]));
        let connection = connected_with(Arc::clone(&transport) as Arc<dyn McpTransport>);

        let prompts = connection.list_prompts().await;
        assert_eq!(prompts.len(), 1);
        assert_eq!(prompts[0].name, "review");
        assert_eq!(prompts[0].description, "");
        assert_eq!(prompts[0].arguments.len(), 1);
        assert_eq!(prompts[0].arguments[0].name, "path");
        assert_eq!(prompts[0].arguments[0].description, "");
        assert!(prompts[0].arguments[0].required);

        let mut arguments = BTreeMap::new();
        arguments.insert("path".to_owned(), "src/lib.rs".to_owned());
        let messages = connection
            .get_prompt("review", &arguments)
            .await
            .expect("prompt");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, "user");
        assert_eq!(messages[0].content, "bare");
        assert_eq!(messages[1].role, "assistant");
        assert_eq!(messages[1].content, "rich");
        let request = transport
            .requests()
            .into_iter()
            .find(|(method, _)| method == METHOD_PROMPTS_GET)
            .expect("prompts/get request");
        assert_eq!(
            request.1,
            Some(json!({"name": "review", "arguments": {"path": "src/lib.rs"}}))
        );
    }

    #[tokio::test]
    async fn roots_list_reverse_request_is_answered_with_current_roots() {
        let transport = StubTransport::scripted(script(vec![]));
        let connection = connected_with(Arc::clone(&transport) as Arc<dyn McpTransport>);
        let roots = Arc::new(RootsProvider::new());
        roots.update_roots("/Users/dev/project", "project");
        connection.set_roots_provider(roots);

        connection
            .dispatch_notification(json!({"jsonrpc": "2.0", "id": 7, "method": "roots/list"}))
            .await;
        let responses = transport.responses();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].0, RequestId::Number(7));
        assert_eq!(
            responses[0].1,
            json!({"roots": [{"uri": "file:///Users/dev/project", "name": "project"}]})
        );

        // 无 id → 视为通知丢弃，不产生响应。
        connection
            .dispatch_notification(json!({"jsonrpc": "2.0", "method": "roots/list"}))
            .await;
        assert_eq!(transport.responses().len(), 1);
    }

    #[tokio::test]
    async fn progress_notification_reaches_tracker() {
        struct RecordingTracker {
            seen: Mutex<Vec<Value>>,
        }
        impl ProgressTracker for RecordingTracker {
            fn register_progress(
                &self,
                _token: &str,
                _session_id: &str,
                _server_name: &str,
                _tool_name: &str,
            ) {
            }
            fn unregister_progress(&self, _token: &str) {}
            fn handle_progress_notification(&self, notification: &Value) {
                lock(&self.seen).push(notification.clone());
            }
        }

        let transport = StubTransport::scripted(script(vec![]));
        let connection = connected_with(transport);
        let tracker = Arc::new(RecordingTracker {
            seen: Mutex::new(Vec::new()),
        });
        connection.set_progress_tracker(Arc::clone(&tracker) as Arc<dyn ProgressTracker>);

        let notification =
            json!({"method": "notifications/progress", "params": {"progressToken": "t"}});
        connection.dispatch_notification(notification.clone()).await;
        assert_eq!(lock(&tracker.seen).clone(), vec![notification]);

        // 未知方法只记日志，不 panic。
        connection
            .dispatch_notification(json!({"method": "notifications/unknown"}))
            .await;
    }

    #[tokio::test]
    async fn tools_list_changed_replaces_directory_fail_closed() {
        let transport = StubTransport::scripted(script(vec![(
            METHOD_TOOLS_LIST,
            Ok(json!({"tools": [{
                "name": "replacement",
                "description": "new",
                "inputSchema": {"type": "object"}
            }]})),
        )]));
        let connection = connected_with(Arc::clone(&transport) as Arc<dyn McpTransport>);
        connection.set_tools(vec![ToolDefinition {
            name: "removed".to_owned(),
            description: "old".to_owned(),
            input_schema: json!({}),
        }]);
        let changes = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&changes);
        connection.on_tools_changed(Arc::new(move || {
            seen.fetch_add(1, Ordering::AcqRel);
        }));

        connection
            .dispatch_notification(json!({"method": METHOD_TOOLS_LIST_CHANGED}))
            .await;

        assert_eq!(
            connection
                .tools()
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            vec!["replacement"]
        );
        assert_eq!(changes.load(Ordering::Acquire), 2);
        assert_eq!(transport.requests()[0].0, METHOD_TOOLS_LIST);
    }

    #[tokio::test]
    async fn resources_list_changed_revokes_cached_declarations() {
        let transport = StubTransport::scripted(script(vec![]));
        let connection = connected_with(transport);
        let old = vec![ResourceDefinition {
            uri: "file:///old".to_owned(),
            name: "old".to_owned(),
            mime_type: None,
            description: None,
        }];
        connection.set_resources(old.clone());
        connection.store_resources_cache(old);

        connection
            .dispatch_notification(json!({"method": METHOD_RESOURCES_LIST_CHANGED}))
            .await;

        assert!(connection.resources().is_empty());
        assert!(connection.cached_resources().is_none());
    }

    #[tokio::test]
    async fn stale_transport_disconnects_and_notifications_are_ignored() {
        struct CountingTracker(AtomicU32);
        impl ProgressTracker for CountingTracker {
            fn register_progress(&self, _t: &str, _s: &str, _sv: &str, _tn: &str) {}
            fn unregister_progress(&self, _token: &str) {}
            fn handle_progress_notification(&self, _notification: &Value) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }

        let connection = McpServerConnection::new(McpServerConfig::stdio(
            "fake",
            "/bin/true",
            Vec::<String>::new(),
        ));
        let tracker = Arc::new(CountingTracker(AtomicU32::new(0)));
        connection.set_progress_tracker(tracker.clone() as Arc<dyn ProgressTracker>);
        let old = StubTransport::scripted(script(vec![]));
        connection.set_transport_for_test(old.clone() as Arc<dyn McpTransport>);
        let current = StubTransport::scripted(script(vec![]));
        connection.set_transport_for_test(current.clone() as Arc<dyn McpTransport>);
        connection.set_status(McpConnectionStatus::Connected);
        connection.set_tools(vec![ToolDefinition {
            name: "current".to_owned(),
            description: String::new(),
            input_schema: json!({}),
        }]);

        old.emit_notification(
            json!({"method": METHOD_PROGRESS, "params": {"progressToken": "old"}}),
        );
        old.emit_notification(json!({
            "jsonrpc": "2.0",
            "id": 42,
            "method": METHOD_ROOTS_LIST
        }));
        old.disconnect();
        tokio::task::yield_now().await;
        assert_eq!(connection.status(), McpConnectionStatus::Connected);
        assert_eq!(connection.tools().len(), 1);
        assert_eq!(tracker.0.load(Ordering::Acquire), 0);
        assert!(old.responses().is_empty());
        assert!(current.responses().is_empty());

        current.emit_notification(
            json!({"method": METHOD_PROGRESS, "params": {"progressToken": "current"}}),
        );
        tokio::task::yield_now().await;
        assert_eq!(tracker.0.load(Ordering::Acquire), 1);
        current.disconnect();
        assert_eq!(connection.status(), McpConnectionStatus::Failed);
        assert!(connection.tools().is_empty());
    }

    #[tokio::test]
    async fn old_response_after_generation_change_is_rejected() {
        struct GatedTransport {
            started: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
        }

        impl McpTransport for GatedTransport {
            fn next_request_id(&self) -> RequestId {
                RequestId::Number(1)
            }

            fn connect(&self) -> BoxFuture<'_, Result<(), McpProtocolError>> {
                Box::pin(async { Ok(()) })
            }

            fn send_request<'a>(
                &'a self,
                _request_id: RequestId,
                _method: &'a str,
                _params: Option<Value>,
                _timeout: Duration,
            ) -> BoxFuture<'a, Result<Option<Value>, McpProtocolError>> {
                Box::pin(async move {
                    self.started.notify_one();
                    self.release.notified().await;
                    Ok(Some(json!({"from": "old"})))
                })
            }

            fn send_notification<'a>(
                &'a self,
                _method: &'a str,
                _params: Option<Value>,
            ) -> BoxFuture<'a, ()> {
                Box::pin(async {})
            }

            fn send_response(&self, _id: RequestId, _result: Value) -> BoxFuture<'_, ()> {
                Box::pin(async {})
            }

            fn is_connected(&self) -> bool {
                true
            }

            fn set_notification_handler(&self, _handler: NotificationHandler) {}

            fn close(&self) -> BoxFuture<'_, ()> {
                Box::pin(async {})
            }
        }

        let old = Arc::new(GatedTransport {
            started: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        });
        let connection = McpServerConnection::new(McpServerConfig::stdio(
            "fake",
            "/bin/true",
            Vec::<String>::new(),
        ));
        connection.set_transport_for_test(Arc::clone(&old) as Arc<dyn McpTransport>);
        connection.set_status(McpConnectionStatus::Connected);

        let caller = Arc::clone(&connection);
        let request = tokio::spawn(async move {
            caller
                .request("custom/read", None, Duration::from_secs(5))
                .await
        });
        old.started.notified().await;
        let current = StubTransport::scripted(script(vec![]));
        connection.set_transport_for_test(current as Arc<dyn McpTransport>);
        connection.set_status(McpConnectionStatus::Connected);
        old.release.notify_one();

        let error = request
            .await
            .expect("request task joins")
            .expect_err("old response must be rejected");
        assert_eq!(error.code(), error::SERVER_NOT_INITIALIZED);
        assert_eq!(
            error.message(),
            "MCP transport generation changed during request"
        );
    }

    #[tokio::test]
    async fn cancel_notification_uses_default_reason() {
        let transport = StubTransport::scripted(script(vec![]));
        let connection = connected_with(Arc::clone(&transport) as Arc<dyn McpTransport>);
        connection
            .send_cancel_notification(&RequestId::Text("tok-1".to_owned()), None)
            .await;
        connection
            .send_cancel_notification(&RequestId::Text("tok-2".to_owned()), Some("timeout"))
            .await;
        let notifications = transport.notifications();
        assert_eq!(notifications[0].0, METHOD_CANCELLED);
        assert_eq!(
            notifications[0].1,
            Some(json!({"requestId": "tok-1", "reason": "user_cancelled"}))
        );
        assert_eq!(
            notifications[1].1,
            Some(json!({"requestId": "tok-2", "reason": "timeout"}))
        );
    }

    #[tokio::test]
    async fn reconnect_counters_track_attempts() {
        let connection = McpServerConnection::new(McpServerConfig::stdio(
            "fake",
            "/bin/true",
            Vec::<String>::new(),
        ));
        assert_eq!(connection.reconnect_attempts(), 0);
        connection.increment_reconnect_attempts();
        connection.increment_reconnect_attempts();
        assert_eq!(connection.reconnect_attempts(), 2);
        connection.reset_reconnect_attempts();
        assert_eq!(connection.reconnect_attempts(), 0);

        let flag = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&flag);
        connection.on_tools_changed(Arc::new(move || {
            observed.store(true, Ordering::Release);
        }));
        connection.notify_tools_changed();
        assert!(flag.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn health_ping_without_transport_is_false() {
        let connection = McpServerConnection::new(McpServerConfig::stdio(
            "fake",
            "/bin/true",
            Vec::<String>::new(),
        ));
        assert!(!connection.send_health_ping().await);
        let transport = StubTransport::scripted(script(vec![]));
        let connection = connected_with(transport);
        assert!(connection.send_health_ping().await);
    }

    #[test]
    fn initialize_log_is_truncated_to_200_chars() {
        let long = json!({"text": "x".repeat(400)});
        assert_eq!(truncate_for_log(Some(&long)).chars().count(), 200);
        assert_eq!(truncate_for_log(None), "null");
    }
}
