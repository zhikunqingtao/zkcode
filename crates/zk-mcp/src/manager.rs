//! MCP 客户端管理器 — 所有 MCP 服务器连接的生命周期编排。
//!
//! 逐字对照 Java `McpClientManager.java`（810 行）与 `SseHealthChecker.java`
//! （92 行）：批量初始化（配置解析器 → 宿主静态配置 → 能力注册表三段）、动态
//! 增删、重启、工具动态注册与频道权限过滤、健康检查（被动 `is_alive` + 主动
//! `ping`）、指数退避重连（1s→30s，±25% 抖动，上限 5 次）、代际保护，以及优雅
//! 关闭。
//!
//! # 与 Java 的结构性差异（均为运行时/框架落差，非行为偏离）
//!
//! - Spring `SmartLifecycle`（`start` / `stop` / `isRunning` / `getPhase`）→
//!   本类型只提供同名方法与 [`PHASE`] 常量，由组合根（Batch 4B 的 zk-server）
//!   在启动序列中调用；
//! - `@Scheduled(fixedDelay = 30000)` 的 `healthCheck()` 与
//!   `@Scheduled(fixedRate = 30_000, initialDelay = 30_000)` 的
//!   `SseHealthChecker.performActiveHealthCheck()` 是两个独立调度器，此处合并为
//!   单个 [`McpClientManager::health_check`]（先被动、后主动），由
//!   [`McpClientManager::spawn_health_check_loop`] 以 30s 间隔驱动；
//! - `newFixedThreadPool(2)`（重连工作线程）→ [`tokio::sync::Semaphore`] 并发
//!   许可 2 + `tokio::spawn`；`newSingleThreadScheduledExecutor`（延迟调度）→
//!   `tokio::spawn` + `sleep`；`ScheduledFuture.cancel(false/true)` →
//!   [`CancellationToken`] / [`JoinHandle::abort`]；
//! - 宿主能力全部端口化（依赖倒置，zk-mcp 不依赖 zk-server）：
//!   `McpApprovalService` → [`ApprovalPort`]、`ToolRegistry` → [`McpToolSink`]、
//!   `SimpMessagingTemplate` + `WebSocketSessionManager` → [`McpHealthObserver`]；
//! - `IllegalStateException` / `IllegalArgumentException` → [`ManagerError`]；
//! - `McpConfiguration.toMcpServerConfigs()`（`application.yml`）→ 构建器的
//!   [`McpClientManagerBuilder::static_configs`]（zkcode 无 Spring 配置绑定，
//!   由组合根注入等价配置，授权来源串仍为 `APPLICATION_CONFIG`）；
//! - MCP Prompt 会在连接建立后由 [`McpClientManager::discover_prompts`] 动态发现，
//!   并以 [`McpPromptAdapter`] 注册到同一工具入口；断连时随服务器工具一并摘除；
//! - `SchemaCompressor` 为不做项，适配器直出原始 schema；
//! - `abortContextLookup()` 无对应物——取消经 `zk_tools::ToolContext::cancel`
//!   传递（见 `tool_adapter` 模块文档）。

mod oauth;
mod services;
pub use services::{McpServicePreferenceStore, McpServiceView};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zk_tools::Tool;

use crate::capability_registry::{
    McpCapabilityDefinition, McpCapabilityRegistry, expand_env_placeholders,
    is_blank_or_placeholder, java_is_blank,
};
use crate::config::{McpConfigScope, McpConfigurationResolver, McpServerConfig, McpTransportType};
use crate::connection::{McpConnectionStatus, McpServerConnection};
use crate::prompt_adapter::McpPromptAdapter;
use crate::protocol::{
    METHOD_ROOTS_LIST_CHANGED, ProgressTracker, PromptDefinition, RootsProvider,
};
use crate::security::{
    McpCredentialResolver, default_credential_resolver, resolve_capability_credential,
    validate_capability_destination,
};
use crate::sse::lock;
use crate::tool_adapter::{MCP_TOOL_NAME_PREFIX, McpToolAdapter, ResultCache};

/// 单个服务器的最大重连次数（对照 Java `MAX_RECONNECT_ATTEMPTS = 5`）。
///
/// 注意与 `connection::MAX_RECONNECT_ATTEMPTS`（首次建连的握手重试，3 次）不同
/// 层级：此处是「连接建立后掉线」的重连预算。
pub const MAX_RECONNECT_ATTEMPTS: u32 = 5;

/// 首次退避基数（对照 Java `INITIAL_BACKOFF_MS = 1000`）。
pub const INITIAL_BACKOFF_MS: u64 = 1000;

/// 退避上限（对照 Java `MAX_BACKOFF_MS = 30_000`）。
pub const MAX_BACKOFF_MS: u64 = 30_000;

/// 健康检查周期（对照 Java 两个 `@Scheduled` 的 30s）。
pub const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// 启动阶段序号（对照 Java `getPhase() == 2`：Python 服务 1 之后、
/// `FeatureFlagService` 3 之前）。
pub const PHASE: i32 = 2;

/// 主动 ping 连续失败阈值（对照 Java `SseHealthChecker` 的 `failures >= 2`）。
const HEALTH_PING_FAILURE_THRESHOLD: u32 = 2;

/// 重连并发上限（对照 Java `Executors.newFixedThreadPool(2)`）。
const RECONNECT_CONCURRENCY: usize = 2;

/// 关闭时等待重连任务收敛的宽限（对照 Java `awaitTermination(5, SECONDS)`）。
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// 管理器生命周期错误（对照 Java 抛出的两类运行时异常）。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManagerError {
    /// Resource-bound OAuth failed without exposing credentials.
    #[error("{0}")]
    OAuth(#[from] crate::oauth::OAuthError),
    /// Explicitly disabled services cannot be reactivated by another entry point.
    #[error("MCP_SERVICE_DISABLED: {0}")]
    ServiceDisabled(String),
    /// Durable preferences could not be loaded or committed.
    #[error("MCP_SERVICE_STORAGE_UNAVAILABLE")]
    ServiceStorageUnavailable,
    /// 对照 `IllegalStateException("MCP_CLIENT_MANAGER_NOT_RUNNING")`。
    #[error("MCP_CLIENT_MANAGER_NOT_RUNNING")]
    NotRunning,
    /// 对照 `IllegalStateException("MCP_CLIENT_MANAGER_CANNOT_RESTART_AFTER_SHUTDOWN")`。
    #[error("MCP_CLIENT_MANAGER_CANNOT_RESTART_AFTER_SHUTDOWN")]
    CannotRestartAfterShutdown,
    /// 对照 `IllegalStateException("MCP_CONNECTION_LIFECYCLE_CHANGED")`。
    #[error("MCP_CONNECTION_LIFECYCLE_CHANGED")]
    LifecycleChanged,
    /// 对照 `IllegalArgumentException("MCP server not found: " + name)`。
    #[error("MCP server not found: {0}")]
    ServerNotFound(String),
    /// A user-editable capability attempted an unsafe outbound connection.
    #[error("MCP_CAPABILITY_ENDPOINT_REJECTED: {0}")]
    UnsafeCapabilityEndpoint(String),
    /// Two configurations would share an externally visible tool namespace.
    #[error("MCP_TOOL_NAMESPACE_COLLISION: {0}")]
    ToolNamespaceCollision(String),
    /// An existing process or durable cleanup lease is still owned.
    #[error("MCP_CLEANUP_PENDING: {0}")]
    CleanupPending(String),
}

/// 服务器信任判定端口（对照 Java `McpApprovalService`）。
///
/// zk-mcp 不实现落盘授权表（`~/.zhikun/mcp-trusted.json` 等价物属 zk-server
/// 的配置域）；组合根必须注入实现，否则无法构造管理器。
pub trait ApprovalPort: Send + Sync {
    /// 该配置是否已被信任（对照 `isTrusted(config)`）。
    fn is_trusted(&self, config: &McpServerConfig) -> bool;

    /// 记录一次自动/交互授权，`source` 为来源串（如 `LOCAL` / `REGISTRY` /
    /// `APPLICATION_CONFIG` / `USER_RUNTIME`，对照 `recordApproval`）。
    fn record_approval(&self, config: &McpServerConfig, source: &str);
}

/// 工具动态注册端口（对照 Java `ToolRegistry.registerDynamic` /
/// `unregisterByPrefix`）。
///
/// zkcode 既有 `zk_tools::ToolRegistry` 是 `&mut self` 的同步注册表，不含动态
/// 增删；由 zk-server（Batch 4B）以内部可变容器实现本端口。
pub trait McpToolSink: Send + Sync {
    /// 注册（或按同名覆盖）一个 MCP 工具适配器。
    fn register_dynamic(&self, tool: Arc<dyn Tool>);

    /// 按 `mcp__<server>__` 前缀批量注销。
    fn unregister_by_prefix(&self, prefix: &str);

    /// Publish the final, policy-filtered directory snapshot for one server.
    /// Registration is intentionally separate so hosts can update execution
    /// and UI/catalog views from the same authoritative list.
    fn publish_server_tools(&self, _server_id: &str, _tools: Vec<crate::protocol::ToolDefinition>) {
    }
}

/// 连接健康状态观察端口（对照 Java `broadcastHealthStatus` 的 WebSocket 广播）。
///
/// Java 侧在此处组装 `{type: "mcp_health_status", ts, serverName, status,
/// timestamp}` 并逐会话 `convertAndSendToUser`；报文组装与会话遍历属 zk-server
/// 的 WS 域，故端口只传递「服务器名 + 状态」。
pub trait McpHealthObserver: Send + Sync {
    /// 某服务器的连接状态发生变化。
    fn on_health_status(&self, server_name: &str, status: McpConnectionStatus);
}

/// 一个在飞的重连任务（对照 Java `ScheduledReconnect` / `ActiveReconnect` 两个
/// record——二者字段同构，此处合并为一个类型）。
struct ReconnectTask {
    /// 任务身份，用于「仅移除自己那一条」——对照 Java
    /// `scheduledReconnects.remove(serverId, scheduled)` 的引用相等语义。
    task_id: u64,
    connection: Arc<McpServerConnection>,
    generation: u64,
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

impl ReconnectTask {
    /// 对照 `future.isDone()`。
    fn is_done(&self) -> bool {
        self.handle.is_finished()
    }

    /// 对照 `future.cancel(false)`：不打断已开始执行的任务，只阻止其继续。
    fn cancel_gracefully(&self) {
        self.cancel.cancel();
    }

    /// 对照 `future.cancel(true)`：打断正在执行的任务。
    fn cancel_forcefully(&self) {
        self.cancel.cancel();
        self.handle.abort();
    }
}

/// 计算指数退避延迟（无抖动，对照 Java `calculateBackoff`）。
///
/// `attempt` 从 1 起计；`attempt == 0` 按 1 处理（Java 的
/// `1L << (attempt - 1)` 在 0 时为未定义移位，Rust 侧不复制该缺陷）。
#[must_use]
pub fn calculate_backoff(attempt: u32) -> u64 {
    let shift = attempt.saturating_sub(1).min(u32::BITS - 1);
    INITIAL_BACKOFF_MS
        .saturating_mul(1u64 << shift)
        .min(MAX_BACKOFF_MS)
}

/// 指数退避 + ±25% 随机抖动（对照 Java `calculateBackoffWithJitter`）。
///
/// 随机源用 [`getrandom`]（操作系统熵源）替代 `ThreadLocalRandom`：取 53 位构造
/// `[0, 1)` 的双精度，与 `nextDouble()` 同分布；熵源不可用时退化为无抖动。
#[must_use]
pub fn calculate_backoff_with_jitter(attempt: u32) -> u64 {
    let base = calculate_backoff(attempt);
    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        reason = "对照 Java `(long)(base * 0.25 * (nextDouble() * 2 - 1))` 的浮点运算"
    )]
    let jitter = (base as f64 * 0.25 * (next_double() * 2.0 - 1.0)) as i64;
    let jittered = i64::try_from(base)
        .unwrap_or(i64::MAX)
        .saturating_add(jitter);
    u64::try_from(jittered).unwrap_or(0).max(INITIAL_BACKOFF_MS)
}

/// `[0, 1)` 均匀分布随机双精度（对照 `ThreadLocalRandom.nextDouble()`）。
fn next_double() -> f64 {
    let mut bytes = [0u8; 8];
    if getrandom::fill(&mut bytes).is_err() {
        return 0.0;
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "取高 53 位构造 [0,1) 双精度，与 Java nextDouble 同构"
    )]
    let value = (u64::from_le_bytes(bytes) >> 11) as f64;
    value / 9_007_199_254_740_992.0 // 2^53
}

/// MCP 客户端管理器。
///
/// 内部状态用 [`std::sync::Mutex`] 保护并在任何 `.await` 前释放；跨 `await` 的
/// 一致性由「代际号 + 连接引用相等」双重校验保证（对照 Java 的
/// `isCurrentConnection`），而非长时持锁。
pub struct McpClientManager {
    scope_context: Option<zk_tools::ToolContext>,
    scope_parent: Option<std::sync::Weak<McpClientManager>>,
    oauth: Option<Arc<crate::oauth::OAuthCoordinator>>,
    services: services::ServiceState,
    resolver: McpConfigurationResolver,
    static_configs: Vec<McpServerConfig>,
    registry: Option<Arc<McpCapabilityRegistry>>,
    approval: Arc<dyn ApprovalPort>,
    tool_sink: Arc<dyn McpToolSink>,
    health_observer: Option<Arc<dyn McpHealthObserver>>,
    roots_provider: Arc<RootsProvider>,
    progress_tracker: Option<Arc<dyn ProgressTracker>>,
    channel_permissions: BTreeMap<String, Vec<String>>,
    result_cache: Arc<ResultCache>,
    credential_resolver: Arc<dyn McpCredentialResolver>,

    connections: Mutex<HashMap<String, Arc<McpServerConnection>>>,
    connection_generations: Mutex<HashMap<String, Arc<AtomicU64>>>,
    reconnecting_servers: Mutex<HashMap<String, (u64, Arc<McpServerConnection>)>>,
    scheduled_reconnects: Mutex<HashMap<String, ReconnectTask>>,
    active_reconnects: Mutex<HashMap<String, ReconnectTask>>,
    consecutive_failures: Mutex<HashMap<String, u32>>,
    last_successful_ping: Mutex<HashMap<String, SystemTime>>,
    registry_owned_servers: Mutex<HashSet<String>>,

    reconnect_permits: Semaphore,
    next_task_id: AtomicU64,
    credential_refresh_running: AtomicBool,
    credential_refresh_pending: AtomicBool,
    running: AtomicBool,
    shutdown_done: AtomicBool,
}

impl std::fmt::Debug for McpClientManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpClientManager")
            .field("running", &self.is_running())
            .field("connections", &self.connection_count())
            .finish_non_exhaustive()
    }
}

/// [`McpClientManager`] 的构建器 — 替代 Java 的 12 参数构造函数注入。
pub struct McpClientManagerBuilder {
    scope_context: Option<zk_tools::ToolContext>,
    scope_parent: Option<std::sync::Weak<McpClientManager>>,
    oauth: Option<Arc<crate::oauth::OAuthCoordinator>>,
    service_preferences: Option<Arc<dyn McpServicePreferenceStore>>,
    resolver: McpConfigurationResolver,
    static_configs: Vec<McpServerConfig>,
    registry: Option<Arc<McpCapabilityRegistry>>,
    approval: Arc<dyn ApprovalPort>,
    tool_sink: Arc<dyn McpToolSink>,
    health_observer: Option<Arc<dyn McpHealthObserver>>,
    roots_provider: Arc<RootsProvider>,
    progress_tracker: Option<Arc<dyn ProgressTracker>>,
    channel_permissions: BTreeMap<String, Vec<String>>,
    result_cache: Option<Arc<ResultCache>>,
    credential_resolver: Arc<dyn McpCredentialResolver>,
}

impl McpClientManagerBuilder {
    /// 两个必需端口：信任判定与工具注册。
    #[must_use]
    pub fn new(approval: Arc<dyn ApprovalPort>, tool_sink: Arc<dyn McpToolSink>) -> Self {
        Self {
            scope_context: None,
            scope_parent: None,
            service_preferences: None,
            oauth: None,
            resolver: McpConfigurationResolver::default(),
            static_configs: Vec::new(),
            registry: None,
            approval,
            tool_sink,
            health_observer: None,
            roots_provider: Arc::new(RootsProvider::new()),
            progress_tracker: None,
            channel_permissions: BTreeMap::new(),
            result_cache: None,
            credential_resolver: default_credential_resolver(),
        }
    }

    /// 多来源配置解析器（默认工作目录取进程 CWD）。
    #[must_use]
    pub fn resolver(mut self, resolver: McpConfigurationResolver) -> Self {
        self.resolver = resolver;
        self
    }

    /// 宿主静态配置（对照 `application.yml` 的 `mcp.servers`）。
    #[must_use]
    pub fn static_configs(mut self, configs: Vec<McpServerConfig>) -> Self {
        self.static_configs = configs;
        self
    }

    /// 能力注册表（缺省时跳过注册表激活与描述/超时覆盖）。
    #[must_use]
    pub fn registry(mut self, registry: Arc<McpCapabilityRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// 健康状态观察者（缺省时不广播）。
    #[must_use]
    pub fn health_observer(mut self, observer: Arc<dyn McpHealthObserver>) -> Self {
        self.health_observer = Some(observer);
        self
    }

    /// Roots 提供者（缺省新建，`start()` 时以 CWD 初始化）。
    #[must_use]
    pub fn roots_provider(mut self, provider: Arc<RootsProvider>) -> Self {
        self.roots_provider = provider;
        self
    }

    /// 进度追踪端口（缺省时连接层静默丢弃 `notifications/progress`）。
    #[must_use]
    pub fn progress_tracker(mut self, tracker: Arc<dyn ProgressTracker>) -> Self {
        self.progress_tracker = Some(tracker);
        self
    }

    /// 频道权限黑名单（`服务器名 → 被屏蔽工具名`，`"*"` 表示整服务器屏蔽；
    /// 对照 Java `McpConfiguration.getChannelPermissions()`）。
    #[must_use]
    pub fn channel_permissions(mut self, permissions: BTreeMap<String, Vec<String>>) -> Self {
        self.channel_permissions = permissions;
        self
    }

    /// 适配器降级缓存（缺省用进程级共享实例）。
    #[must_use]
    pub fn result_cache(mut self, cache: Arc<ResultCache>) -> Self {
        self.result_cache = Some(cache);
        self
    }

    /// Resolve known provider credential identities for capability-registry
    /// connections.  The registry cannot select arbitrary environment names.
    #[must_use]
    pub fn credential_resolver(mut self, resolver: Arc<dyn McpCredentialResolver>) -> Self {
        self.credential_resolver = resolver;
        self
    }

    /// Inject durable service switches independently of capability preferences.
    #[must_use]
    pub fn service_preferences(mut self, store: Arc<dyn McpServicePreferenceStore>) -> Self {
        self.service_preferences = Some(store);
        self
    }

    /// Attach the host's Keychain-backed OAuth coordinator.
    #[must_use]
    pub fn oauth(mut self, coordinator: Arc<crate::oauth::OAuthCoordinator>) -> Self {
        self.oauth = Some(coordinator);
        self
    }

    /// Isolate a run's transports and inherit live global service/tool gates.
    #[must_use]
    pub fn run_scope(
        mut self,
        context: zk_tools::ToolContext,
        parent: &Arc<McpClientManager>,
    ) -> Self {
        self.scope_context = Some(context);
        self.scope_parent = Some(Arc::downgrade(parent));
        self
    }

    /// 装配管理器。
    #[must_use]
    pub fn build(self) -> Arc<McpClientManager> {
        Arc::new(McpClientManager {
            scope_context: self.scope_context,
            scope_parent: self.scope_parent,
            services: services::ServiceState::new(self.service_preferences),
            oauth: self.oauth,
            resolver: self.resolver,
            static_configs: self.static_configs,
            registry: self.registry,
            approval: self.approval,
            tool_sink: self.tool_sink,
            health_observer: self.health_observer,
            roots_provider: self.roots_provider,
            progress_tracker: self.progress_tracker,
            channel_permissions: self.channel_permissions,
            result_cache: self
                .result_cache
                .unwrap_or_else(crate::tool_adapter::shared_result_cache),
            credential_resolver: self.credential_resolver,
            connections: Mutex::new(HashMap::new()),
            connection_generations: Mutex::new(HashMap::new()),
            reconnecting_servers: Mutex::new(HashMap::new()),
            scheduled_reconnects: Mutex::new(HashMap::new()),
            active_reconnects: Mutex::new(HashMap::new()),
            consecutive_failures: Mutex::new(HashMap::new()),
            last_successful_ping: Mutex::new(HashMap::new()),
            registry_owned_servers: Mutex::new(HashSet::new()),
            reconnect_permits: Semaphore::new(RECONNECT_CONCURRENCY),
            next_task_id: AtomicU64::new(0),
            credential_refresh_running: AtomicBool::new(false),
            credential_refresh_pending: AtomicBool::new(false),
            running: AtomicBool::new(false),
            shutdown_done: AtomicBool::new(false),
        })
    }
}

// ===== 生命周期（对照 Java SmartLifecycle）=====

impl McpClientManager {
    /// 构建器入口。
    #[must_use]
    pub fn builder(
        approval: Arc<dyn ApprovalPort>,
        tool_sink: Arc<dyn McpToolSink>,
    ) -> McpClientManagerBuilder {
        McpClientManagerBuilder::new(approval, tool_sink)
    }

    /// 是否处于运行态（对照 `isRunning()`）。
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// 启动：置运行态 → 初始化默认 roots → 批量建连（对照 `start()`）。
    ///
    /// 已在运行时为幂等空操作。
    ///
    /// # Errors
    ///
    /// [`ManagerError::CannotRestartAfterShutdown`]：[`Self::shutdown`] 之后不可
    /// 重启（对照 Java 检查 `reconnectPool.isShutdown()`）。
    pub async fn start(self: &Arc<Self>) -> Result<(), ManagerError> {
        self.load_service_preferences().await?;
        if self.shutdown_done.load(Ordering::Acquire) {
            return Err(ManagerError::CannotRestartAfterShutdown);
        }
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        tracing::info!("McpClientManager starting — initializing MCP connections");
        self.initialize_default_roots();
        self.initialize_all().await;
        Ok(())
    }

    /// Start only explicitly supplied run-local configurations. Never reads
    /// user/project files, environment configuration, or the global capability catalog.
    /// # Errors
    /// Invalid configuration, disabled services or failed scoped setup return errors.
    pub async fn start_scoped(
        self: &Arc<Self>,
        configs: Vec<McpServerConfig>,
    ) -> Result<(), ManagerError> {
        self.load_service_preferences().await?;
        if self.shutdown_done.load(Ordering::Acquire) {
            return Err(ManagerError::CannotRestartAfterShutdown);
        }
        if self.running.swap(true, Ordering::AcqRel) {
            return Err(ManagerError::LifecycleChanged);
        }
        if let Some(context) = &self.scope_context {
            self.roots_provider
                .update_roots(&context.working_dir().to_string_lossy(), "workspace");
        }
        for config in configs {
            let connection = self.add_server(config).await?;
            if connection.status() != McpConnectionStatus::Connected {
                return Err(ManagerError::LifecycleChanged);
            }
        }
        Ok(())
    }

    /// 停止（对照 `stop()`）。
    pub async fn stop(&self) {
        tracing::info!("McpClientManager stopping — closing all MCP connections");
        self.shutdown().await;
    }

    /// 从三个来源加载并连接所有 MCP 服务器（对照 `initializeAll()`）。
    pub async fn initialize_all(self: &Arc<Self>) {
        let from_resolver = self.initialize_resolved_configs().await;
        self.initialize_static_configs().await;
        tracing::info!(
            connections = self.connection_count(),
            from_resolver,
            from_application_config = self.static_configs.len(),
            "MCP initialization complete"
        );
        self.activate_registry_capabilities().await;
    }

    /// 多来源配置文件段（对照 `initializeAll()` 第 1 段）：来自配置文件的服务器
    /// 一律自动信任，授权来源串取其 scope 名。返回解析到的配置条数。
    async fn initialize_resolved_configs(self: &Arc<Self>) -> usize {
        let resolved = self.resolver.resolve_all();
        for config in &resolved {
            if !self.approval.is_trusted(config) {
                self.approval.record_approval(config, config.scope.as_str());
                tracing::info!(
                    server = %config.name,
                    scope = %config.scope,
                    "Auto-trusted config-file MCP server"
                );
            }
            self.add_server_logged(config.clone()).await;
        }
        resolved.len()
    }

    /// 宿主静态配置段（对照 `initializeAll()` 第 2 段）：仅补齐未被上一段覆盖的
    /// 服务器，授权来源串为 `APPLICATION_CONFIG`。
    async fn initialize_static_configs(self: &Arc<Self>) {
        for config in self.static_configs.clone() {
            if self.get_connection(&config.name).is_some() {
                continue;
            }
            if !self.approval.is_trusted(&config) {
                self.approval.record_approval(&config, "APPLICATION_CONFIG");
                tracing::info!(server = %config.name, "Auto-trusted application.yml MCP server");
            }
            self.add_server_logged(config).await;
        }
    }

    /// 注册表已启用条目的自动激活（对照 `initializeAll()` 第 3 段）。
    async fn activate_registry_capabilities(self: &Arc<Self>) {
        let Some(registry) = self.registry.clone() else {
            return;
        };
        if registry.size() == 0 {
            return;
        }
        let enabled = registry.list_enabled();
        let mut activated = 0usize;
        let mut attempted_servers = HashSet::new();
        for capability in &enabled {
            let server_key = capability.extract_server_key();
            if self.get_connection(&server_key).is_some()
                || !attempted_servers.insert(server_key.clone())
            {
                continue;
            }
            // 连接前置校验（对照 Java v1.1 `initializeAll` 注册表段）：端点 URL
            // 或声明必需的 API Key 未配置时跳过，避免以残缺配置反复建连失败。
            let candidate = self.build_resolved_config_from_registry(capability);
            if candidate.url.as_deref().is_none_or(java_is_blank) {
                tracing::info!(
                    server = %server_key,
                    "Skipping MCP registry server — endpoint URL is not configured"
                );
                continue;
            }
            let requires_api_key = capability
                .api_key_config
                .as_deref()
                .is_some_and(|value| !java_is_blank(value))
                || capability
                    .api_key_default
                    .as_deref()
                    .is_some_and(|value| !java_is_blank(value));
            if requires_api_key && !candidate.headers.contains_key("Authorization") {
                tracing::info!(
                    server = %server_key,
                    "Skipping MCP registry server — API key is not configured"
                );
                continue;
            }
            match self.enable_from_registry(capability).await {
                Ok(_) => activated += 1,
                Err(error) => tracing::warn!(
                    capability = %capability.id,
                    %error,
                    "Failed to enable registry capability"
                ),
            }
        }
        tracing::info!(
            activated,
            enabled_entries = enabled.len(),
            "MCP registry activation complete"
        );
    }

    /// Rebuild capability-registry connections after the host changes provider
    /// credentials.  Existing connections must be closed first because their
    /// transport owns an immutable Authorization header map.
    pub async fn refresh_registry_credentials(self: &Arc<Self>) {
        if !self.is_running() {
            return;
        }
        if self.registry.is_none() {
            return;
        }
        // Only connections created by enable_from_registry are eligible.  A
        // manually configured server with the same name must never be removed.
        let server_keys = lock(&self.registry_owned_servers).clone();
        for server_key in server_keys {
            let _removed = self.remove_server(&server_key).await;
        }
        self.activate_registry_capabilities().await;
    }

    /// Queue a non-blocking credential refresh. Concurrent updates are
    /// coalesced, but an update arriving during a refresh always schedules one
    /// more pass so the newest key wins.
    pub fn schedule_registry_credential_refresh(self: &Arc<Self>) {
        self.credential_refresh_pending
            .store(true, Ordering::Release);
        if self
            .credential_refresh_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                manager
                    .credential_refresh_pending
                    .store(false, Ordering::Release);
                manager.refresh_registry_credentials().await;
                if !manager.credential_refresh_pending.load(Ordering::Acquire) {
                    break;
                }
            }
            manager
                .credential_refresh_running
                .store(false, Ordering::Release);
            // Close the small race between the last pending check and clearing
            // running: a concurrent scheduler may have observed running=true.
            if manager.credential_refresh_pending.load(Ordering::Acquire) {
                manager.schedule_registry_credential_refresh();
            }
        });
    }

    /// `addServer` 的日志包装 —— Java 在 `initializeAll` 中直接调用 `addServer`
    /// 且不捕获异常；Rust 侧把生命周期错误降级为一条 warn，避免单个服务器的竞态
    /// 中断整批初始化。
    async fn add_server_logged(self: &Arc<Self>, config: McpServerConfig) {
        let name = config.name.clone();
        if let Err(error) = self.add_server(config).await {
            tracing::warn!(server = %name, %error, "Failed to add MCP server");
        }
    }

    /// 优雅关闭所有连接与重连任务（对照 `shutdown()`）。
    pub async fn shutdown(&self) {
        self.running.store(false, Ordering::Release);
        for cancel in lock(&self.services.cancellations).values() {
            cancel.cancel();
        }
        for generation in lock(&self.connection_generations).values() {
            generation.fetch_add(1, Ordering::AcqRel);
        }

        let scheduled: Vec<ReconnectTask> = lock(&self.scheduled_reconnects)
            .drain()
            .map(|(_, task)| task)
            .collect();
        for task in &scheduled {
            task.cancel_gracefully();
        }
        let active: Vec<ReconnectTask> = lock(&self.active_reconnects)
            .drain()
            .map(|(_, task)| task)
            .collect();
        for task in &active {
            task.cancel_forcefully();
        }
        let connections = {
            let directory = lock(&self.services.directory);
            lock(&self.reconnecting_servers).clear();
            let connections = self.list_connections();
            for connection in &connections {
                connection.set_status(McpConnectionStatus::Disabled);
                self.clear_tool_directory_locked(&directory, connection.name());
            }
            connections
        };
        for connection in &connections {
            connection.close().await;
            if connection.cleanup_confirmed() {
                remove_if_same(&self.connections, connection.name(), connection);
            }
        }

        await_reconnect_tasks(scheduled, "reconnect scheduler").await;
        await_reconnect_tasks(active, "reconnect worker").await;
        self.shutdown_done.store(true, Ordering::Release);
    }

    /// 对照 `requireRunning()`。
    fn require_running(&self) -> Result<(), ManagerError> {
        if self.is_running() {
            Ok(())
        } else {
            Err(ManagerError::NotRunning)
        }
    }
}

// ===== 连接管理 =====

impl McpClientManager {
    /// 动态添加 MCP 服务器（对照 `addServer(config)`）。
    ///
    /// Trust provenance is assigned only by the explicit configuration-source
    /// loaders and registry activation path.  This generic runtime method never
    /// auto-trusts from caller-controlled `config.scope`; an unapproved config
    /// is installed as `NEEDS_AUTH` and no transport is opened.
    ///
    /// Java 用 `config.scope() != null` 区分「配置文件解析」与「手动添加」；
    /// zkcode 的 `scope` 是非可空枚举，故以 [`McpConfigScope::Dynamic`]
    /// （「运行时动态注册」）承担 Java `null` 的语义 —— 见模块级偏离 1。
    ///
    /// Rust installs the managed connection before transport startup so deletion
    /// and cancellation retain its cleanup owner even during a failed handshake.
    /// `McpServerConnection::connect` records failures in the connection status.
    ///
    /// # Errors
    ///
    /// - [`ManagerError::NotRunning`]：管理器未启动；
    /// - [`ManagerError::LifecycleChanged`]：建连期间该服务器被移除/重启（代际
    ///   已推进），新连接已被关闭丢弃。
    pub async fn add_server(
        self: &Arc<Self>,
        config: McpServerConfig,
    ) -> Result<Arc<McpServerConnection>, ManagerError> {
        self.add_server_from(config, false).await
    }

    async fn add_server_from(
        self: &Arc<Self>,
        config: McpServerConfig,
        from_registry: bool,
    ) -> Result<Arc<McpServerConnection>, ManagerError> {
        self.add_server_from_owner(config, from_registry, None)
            .await
    }

    // Reserve the generation and withdraw its predecessor's tools atomically.
    fn prepare_server_replacement(
        &self,
        config: &McpServerConfig,
        from_registry: bool,
        expected: Option<(Option<&Arc<McpServerConnection>>, u64)>,
    ) -> Result<(u64, CancellationToken, Option<Arc<McpServerConnection>>), ManagerError> {
        let directory = lock(&self.services.directory);
        if expected.is_some_and(|(connection, generation)| {
            !self.is_expected_connection(&config.name, connection, generation)
        }) {
            return Err(ManagerError::LifecycleChanged);
        }
        self.reserve_server_configuration_locked(config)?;
        let generation = self.next_generation(&config.name);
        {
            let mut registry = lock(&self.services.registry_configs);
            if from_registry {
                registry.insert(config.name.clone());
            } else {
                registry.remove(&config.name);
            }
        }
        if !self.is_service_enabled(&config.name) {
            return Err(ManagerError::ServiceDisabled(config.name.clone()));
        }
        let cancel = self.replace_service_cancellation_locked(&config.name);
        let previous = self.get_connection(&config.name);
        if previous.is_some() {
            self.clear_tool_directory_locked(&directory, &config.name);
        }
        Ok((generation, cancel, previous))
    }

    // An automatic OAuth restart may replace only the owner it observed before I/O.
    async fn add_server_from_owner(
        self: &Arc<Self>,
        mut config: McpServerConfig,
        from_registry: bool,
        expected: Option<(Option<&Arc<McpServerConnection>>, u64)>,
    ) -> Result<Arc<McpServerConnection>, ManagerError> {
        self.require_running()?;
        let (generation, cancel, previous) =
            self.prepare_server_replacement(&config, from_registry, expected)?;

        // Retain the old owner until cleanup is confirmed, including if this future is cancelled.
        if let Some(previous) = previous {
            if !previous
                .close_if(|| self.owns_connection_cleanup(&config.name, generation, &previous))
                .await
            {
                return Err(ManagerError::LifecycleChanged);
            }
            if !previous.cleanup_confirmed() {
                return Err(ManagerError::CleanupPending(config.name.clone()));
            }
            let _directory = lock(&self.services.directory);
            if self.generation_of(&config.name) != generation {
                return Err(ManagerError::LifecycleChanged);
            }
            remove_if_same(&self.connections, &config.name, &previous);
        }
        if self.generation_of(&config.name) != generation || cancel.is_cancelled() {
            return Err(ManagerError::LifecycleChanged);
        }

        if !self.approval.is_trusted(&config) {
            tracing::info!(server = %config.name, "MCP server not trusted, pending approval");
            let connection = self.new_connection(config.clone());
            connection.set_status(McpConnectionStatus::NeedsAuth);
            self.install_connection(&config.name, generation, &connection)
                .await?;
            return Ok(connection);
        }

        if let Some(oauth) = &self.oauth
            && matches!(
                config.transport,
                McpTransportType::Http | McpTransportType::Sse | McpTransportType::SseIde
            )
            && let Some(resource) = &config.url
        {
            match oauth.authorization_header(&config.name, resource).await {
                Ok(Some(header)) => {
                    config
                        .headers
                        .retain(|name, _| !name.eq_ignore_ascii_case("authorization"));
                    config.headers.insert("Authorization".into(), header);
                }
                Ok(None) => {}
                Err(error) => {
                    let connection = self.new_connection(config.clone());
                    connection.set_status(McpConnectionStatus::NeedsAuth);
                    self.install_connection(&config.name, generation, &connection)
                        .await?;
                    return Err(error.into());
                }
            }
        }
        let connection = self.new_connection(config.clone());
        // Publish the cleanup owner before any transport can start. Cancellation
        // and explicit deletion can now stop and await an in-flight handshake.
        self.install_connection(&config.name, generation, &connection)
            .await?;
        self.connect_managed(&config.name, generation, &connection, &cancel)
            .await?;
        let directory = lock(&self.services.directory);
        if !self.is_current_connection(&config.name, &connection, generation) {
            return Err(ManagerError::LifecycleChanged);
        }
        if connection.status() == McpConnectionStatus::Connected {
            self.register_tools_locked(&directory, &connection);
            tracing::info!(server = %config.name, "MCP server connected");
        } else {
            tracing::warn!(
                server = %config.name,
                status = %connection.status(),
                "MCP server did not reach CONNECTED state"
            );
        }
        Ok(connection)
    }

    async fn connect_managed(
        &self,
        name: &str,
        generation: u64,
        connection: &Arc<McpServerConnection>,
        cancel: &CancellationToken,
    ) -> Result<(), ManagerError> {
        let scope_cancel = self
            .scope_context
            .as_ref()
            .map_or_else(CancellationToken::new, |context| context.cancel.clone());
        tokio::select! {
            biased;
            () = scope_cancel.cancelled() => {
                self.close_owned_connection(name, generation, connection).await;
                return Err(ManagerError::LifecycleChanged);
            }
            () = cancel.cancelled() => {
                self.close_owned_connection(name, generation, connection).await;
                return Err(if self.is_service_enabled(name) {
                    ManagerError::LifecycleChanged
                } else {
                    ManagerError::ServiceDisabled(name.to_owned())
                });
            }
            () = connection.connect_if(|| !cancel.is_cancelled()
                && !scope_cancel.is_cancelled()
                && self.is_current_connection(name, connection, generation)) => {}
        }
        if !self.is_current_connection(name, connection, generation) {
            self.close_owned_connection(name, generation, connection)
                .await;
            return Err(ManagerError::LifecycleChanged);
        }
        Ok(())
    }

    async fn close_owned_connection(
        &self,
        name: &str,
        generation: u64,
        connection: &Arc<McpServerConnection>,
    ) {
        let closed = connection
            .close_if(|| self.owns_connection_cleanup(name, generation, connection))
            .await;
        let _directory = lock(&self.services.directory);
        if closed && self.generation_of(name) == generation && connection.cleanup_confirmed() {
            remove_if_same(&self.connections, name, connection);
        }
    }

    fn owns_connection_cleanup(
        &self,
        name: &str,
        generation: u64,
        connection: &Arc<McpServerConnection>,
    ) -> bool {
        let _directory = lock(&self.services.directory);
        self.generation_of(name) == generation
            || self
                .get_connection(name)
                .is_none_or(|current| !Arc::ptr_eq(&current, connection))
    }

    // Called with the directory lock. New generations revoke the old startup
    // token, while later explicit startup remains possible for enabled services.
    fn replace_service_cancellation_locked(&self, name: &str) -> CancellationToken {
        let cancel = CancellationToken::new();
        if let Some(previous) =
            lock(&self.services.cancellations).insert(name.to_owned(), cancel.clone())
        {
            previous.cancel();
        }
        cancel
    }

    fn cancel_service_start_locked(&self, name: &str) {
        if let Some(cancel) = lock(&self.services.cancellations).remove(name) {
            cancel.cancel();
        }
    }

    // Caller holds the directory lock through reservation and generation assignment.
    fn reserve_server_configuration_locked(
        &self,
        config: &McpServerConfig,
    ) -> Result<(), ManagerError> {
        let prefix = tool_prefix(&config.name);
        let mut configs = lock(&self.services.configs);
        if configs.keys().any(|name| {
            if name == &config.name {
                return false;
            }
            let other = tool_prefix(name);
            prefix.starts_with(&other) || other.starts_with(&prefix)
        }) {
            return Err(ManagerError::ToolNamespaceCollision(config.name.clone()));
        }
        configs.insert(config.name.clone(), config.clone());
        Ok(())
    }

    /// 建连接实例并注入 roots / progress 端口（对照 Java 的三行装配）。
    fn new_connection(&self, config: McpServerConfig) -> Arc<McpServerConnection> {
        let authorizer = self.oauth.as_ref().and_then(|oauth| {
            config.url.as_ref().map(|resource| {
                let cancel = lock(&self.services.cancellations)
                    .entry(config.name.clone())
                    .or_default()
                    .clone();
                oauth.request_authorizer(config.name.clone(), resource.clone(), cancel)
            })
        });
        let connection = McpServerConnection::new(config);
        if let Some(context) = &self.scope_context {
            connection.set_execution_context(context.clone());
        }
        if let Some(authorizer) = authorizer {
            connection.set_request_authorizer(authorizer);
        }
        connection.set_roots_provider(Arc::clone(&self.roots_provider));
        if let Some(tracker) = &self.progress_tracker {
            connection.set_progress_tracker(Arc::clone(tracker));
        }
        connection
    }

    /// Disconnect a server while retaining its configuration for refresh, logout or re-enable.
    pub async fn remove_server(&self, name: &str) -> bool {
        let (generation, connection) = {
            let directory = lock(&self.services.directory);
            let generation = self.next_generation(name);
            self.cancel_service_start_locked(name);
            lock(&self.registry_owned_servers).remove(name);
            self.cancel_reconnect_work(name);
            lock(&self.reconnecting_servers).remove(name);
            self.clear_tool_directory_locked(&directory, name);
            (generation, self.get_connection(name))
        };
        let Some(connection) = connection else {
            return false;
        };
        if !connection
            .close_if(|| self.owns_connection_cleanup(name, generation, &connection))
            .await
        {
            return false;
        }
        let _directory = lock(&self.services.directory);
        if self.generation_of(name) != generation {
            return false;
        }
        if connection.cleanup_confirmed() {
            remove_if_same(&self.connections, name, &connection);
        }
        self.broadcast_health_status(name, McpConnectionStatus::Disabled);
        tracing::info!(server = name, "MCP server removed");
        true
    }

    /// Explicit deletion releases runtime configuration only after confirmed cleanup.
    /// File and registry configuration retain their independent source of truth.
    /// # Errors
    /// Unconfirmed cleanup retains the namespace; concurrent replacement is never deleted.
    pub async fn delete_server(&self, name: &str) -> Result<(), ManagerError> {
        let (generation, connection, runtime_config) = {
            let directory = lock(&self.services.directory);
            let generation = self.next_generation(name);
            self.cancel_service_start_locked(name);
            let dynamic_config = lock(&self.services.configs)
                .get(name)
                .is_some_and(|config| config.scope == McpConfigScope::Dynamic);
            let from_registry = lock(&self.services.registry_configs).contains(name);
            let runtime_config = dynamic_config
                && !from_registry
                && !self.static_configs.iter().any(|config| config.name == name);
            lock(&self.registry_owned_servers).remove(name);
            self.cancel_reconnect_work(name);
            self.clear_tool_directory_locked(&directory, name);
            (generation, self.get_connection(name), runtime_config)
        };
        if let Some(connection) = &connection {
            if !connection
                .close_if(|| self.owns_connection_cleanup(name, generation, connection))
                .await
            {
                return Err(ManagerError::LifecycleChanged);
            }
            if !connection.cleanup_confirmed() {
                return Err(ManagerError::CleanupPending(name.to_owned()));
            }
        }
        let _directory = lock(&self.services.directory);
        if self.generation_of(name) != generation
            || self.get_connection(name).is_some_and(|current| {
                connection
                    .as_ref()
                    .is_none_or(|old| !Arc::ptr_eq(old, &current))
            })
        {
            return Err(ManagerError::LifecycleChanged);
        }
        if let Some(connection) = &connection {
            lock(&self.reconnecting_servers).remove(name);
            remove_if_same(&self.connections, name, connection);
        }
        if runtime_config {
            lock(&self.services.configs).remove(name);
        }
        self.broadcast_health_status(name, McpConnectionStatus::Disabled);
        Ok(())
    }

    /// 指定服务器的连接（对照 `getConnection(name)`）。
    #[must_use]
    pub fn get_connection(&self, name: &str) -> Option<Arc<McpServerConnection>> {
        lock(&self.connections).get(name).map(Arc::clone)
    }

    /// 全部连接（对照 `listConnections()`）。
    #[must_use]
    pub fn list_connections(&self) -> Vec<Arc<McpServerConnection>> {
        lock(&self.connections).values().map(Arc::clone).collect()
    }

    /// 全部 `CONNECTED` 连接（对照 `getConnectedServers()`）。
    #[must_use]
    pub fn connected_servers(&self) -> Vec<Arc<McpServerConnection>> {
        lock(&self.connections)
            .values()
            .filter(|connection| {
                self.is_service_enabled(connection.name())
                    && connection.status() == McpConnectionStatus::Connected
            })
            .map(Arc::clone)
            .collect()
    }

    /// 连接数量（对照包私有 `connectionCount()`）。
    #[must_use]
    pub fn connection_count(&self) -> usize {
        lock(&self.connections).len()
    }

    /// 重启指定服务器（对照 `restartServer(name)`）。
    ///
    /// # Errors
    ///
    /// - [`ManagerError::NotRunning`]：管理器未启动；
    /// - [`ManagerError::ServerNotFound`]：无该服务器。
    pub async fn restart_server(self: &Arc<Self>, name: &str) -> Result<(), ManagerError> {
        self.require_running()?;
        if !self.is_service_enabled(name) {
            return Err(ManagerError::ServiceDisabled(name.to_owned()));
        }
        let (connection, generation) = {
            let _directory = lock(&self.services.directory);
            let connection = self
                .get_connection(name)
                .ok_or_else(|| ManagerError::ServerNotFound(name.to_owned()))?;
            (connection, self.generation_of(name))
        };
        self.restart_server_if_current(name, &connection, generation)
            .await
    }

    async fn restart_server_if_current(
        self: &Arc<Self>,
        name: &str,
        connection: &Arc<McpServerConnection>,
        expected_generation: u64,
    ) -> Result<(), ManagerError> {
        if !self.is_current_connection(name, connection, expected_generation) {
            return Err(ManagerError::LifecycleChanged);
        }
        if let Some(oauth) = &self.oauth
            && oauth.has_binding(name).await?
        {
            let (config, from_registry) = {
                let _directory = lock(&self.services.directory);
                if !self.is_current_connection(name, connection, expected_generation) {
                    return Err(ManagerError::LifecycleChanged);
                }
                let config = lock(&self.services.configs)
                    .get(name)
                    .cloned()
                    .ok_or_else(|| ManagerError::ServerNotFound(name.to_owned()))?;
                (config, lock(&self.services.registry_configs).contains(name))
            };
            self.add_server_from_owner(
                config,
                from_registry,
                Some((Some(connection), expected_generation)),
            )
            .await?;
            return Ok(());
        }
        let (generation, cancel) = {
            let directory = lock(&self.services.directory);
            if !self.is_current_connection(name, connection, expected_generation) {
                return Err(ManagerError::LifecycleChanged);
            }
            let generation = self.next_generation(name);
            let cancel = self.replace_service_cancellation_locked(name);
            self.cancel_reconnect_work(name);
            self.clear_tool_directory_locked(&directory, name);
            (generation, cancel)
        };
        tracing::info!(server = name, "Restarting MCP server");
        if !connection
            .close_if(|| self.owns_connection_cleanup(name, generation, connection))
            .await
        {
            return Err(ManagerError::LifecycleChanged);
        }

        if !connection.cleanup_confirmed() {
            return Err(ManagerError::CleanupPending(name.to_owned()));
        }

        {
            let _directory = lock(&self.services.directory);
            if !self.is_current_connection(name, connection, generation) {
                return Err(ManagerError::LifecycleChanged);
            }
            if let Some(oauth) = &self.oauth
                && let Some(resource) = connection.config().url.as_ref()
            {
                connection.set_request_authorizer(oauth.request_authorizer(
                    name.to_owned(),
                    resource.clone(),
                    cancel.clone(),
                ));
            }
        }
        self.connect_managed(name, generation, connection, &cancel)
            .await?;
        let directory = lock(&self.services.directory);
        if !self.is_current_connection(name, connection, generation) {
            return Err(ManagerError::LifecycleChanged);
        }
        if connection.status() == McpConnectionStatus::Connected {
            connection.reset_reconnect_attempts();
            self.register_tools_locked(&directory, connection);
            tracing::info!(server = name, "MCP server restarted");
        } else {
            tracing::warn!(
                server = name,
                status = %connection.status(),
                "MCP server restart did not reach CONNECTED state"
            );
        }
        Ok(())
    }

    /// 服务器日志（对照 `getServerLogs(name, lines)` 的 P0 占位：返回基本状态四
    /// 行；Java 亦未使用 `lines` 参数，此处同样保留形参以对齐 REST 契约）。
    #[must_use]
    pub fn server_logs(&self, name: &str, _lines: usize) -> Vec<String> {
        let Some(connection) = self.get_connection(name) else {
            return vec![format!("MCP server not found: {name}")];
        };
        vec![
            format!("Server: {name}"),
            format!("Status: {}", connection.status()),
            format!("Transport: {}", connection.config().transport),
            format!("Tools: {}", connection.tools().len()),
        ]
    }

    /// 代际推进（对照 `nextGeneration`）。
    fn next_generation(&self, server_id: &str) -> u64 {
        let counter = Arc::clone(
            lock(&self.connection_generations)
                .entry(server_id.to_owned())
                .or_insert_with(|| Arc::new(AtomicU64::new(0))),
        );
        counter.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// 当前代际（对照 `generationOf`：缺失为 0）。
    fn generation_of(&self, server_id: &str) -> u64 {
        lock(&self.connection_generations)
            .get(server_id)
            .map_or(0, |counter| counter.load(Ordering::Acquire))
    }

    /// 安装连接（对照 `installConnection`）。
    async fn install_connection(
        &self,
        server_id: &str,
        generation: u64,
        connection: &Arc<McpServerConnection>,
    ) -> Result<(), ManagerError> {
        let previous = {
            let _directory = lock(&self.services.directory);
            if self.is_running()
                && self.is_service_enabled(server_id)
                && self.generation_of(server_id) == generation
            {
                Some(self.get_connection(server_id))
            } else {
                None
            }
        };
        let Some(previous) = previous else {
            connection.close().await;
            return Err(ManagerError::LifecycleChanged);
        };
        if let Some(previous) = previous
            && !Arc::ptr_eq(&previous, connection)
        {
            if !previous
                .close_if(|| self.owns_connection_cleanup(server_id, generation, &previous))
                .await
            {
                connection.close().await;
                return Err(ManagerError::LifecycleChanged);
            }
            if !previous.cleanup_confirmed() {
                connection.close().await;
                return Err(ManagerError::CleanupPending(server_id.to_owned()));
            }
        }
        let installed = {
            let _directory = lock(&self.services.directory);
            if !self.is_running()
                || !self.is_service_enabled(server_id)
                || self.generation_of(server_id) != generation
            {
                None
            } else {
                Some(lock(&self.connections).insert(server_id.to_owned(), Arc::clone(connection)))
            }
        };
        let Some(previous) = installed else {
            connection.close().await;
            return Err(ManagerError::LifecycleChanged);
        };
        debug_assert!(previous.is_none_or(
            |previous| Arc::ptr_eq(&previous, connection) || previous.cleanup_confirmed()
        ));
        Ok(())
    }

    /// 代际 + 引用双重校验（对照 `isCurrentConnection`）。
    fn is_current_connection(
        &self,
        server_id: &str,
        connection: &Arc<McpServerConnection>,
        generation: u64,
    ) -> bool {
        self.is_expected_connection(server_id, Some(connection), generation)
    }

    fn is_expected_connection(
        &self,
        server_id: &str,
        connection: Option<&Arc<McpServerConnection>>,
        generation: u64,
    ) -> bool {
        self.is_running()
            && self.is_service_enabled(server_id)
            && self.generation_of(server_id) == generation
            && match (lock(&self.connections).get(server_id), connection) {
                (Some(current), Some(expected)) => Arc::ptr_eq(current, expected),
                (None, None) => true,
                _ => false,
            }
    }

    /// 取消该服务器上所有在飞重连（对照 `cancelReconnectWork`）。
    fn cancel_reconnect_work(&self, server_id: &str) {
        if let Some(task) = lock(&self.scheduled_reconnects).remove(server_id) {
            task.cancel_gracefully();
        }
        if let Some(task) = lock(&self.active_reconnects).remove(server_id) {
            task.cancel_forcefully();
        }
    }

    // The caller must keep its owner/generation decision and every sink side effect
    // in the same directory critical section. This helper never acquires the lock.
    fn clear_tool_directory_locked(&self, _directory: &MutexGuard<'_, ()>, server_id: &str) {
        self.tool_sink.unregister_by_prefix(&tool_prefix(server_id));
        self.tool_sink.publish_server_tools(server_id, Vec::new());
    }
}

// ===== 工具与 prompt 发现 =====

impl McpClientManager {
    /// 包装所有已连接服务器的工具（对照 `discoverAndWrapTools()`）。
    #[must_use]
    pub fn discover_and_wrap_tools(self: &Arc<Self>) -> Vec<Arc<dyn Tool>> {
        self.connected_servers()
            .iter()
            .flat_map(|connection| self.wrap_mcp_tools(connection))
            .collect()
    }

    /// Discovery and registration share the same service/capability policy.
    fn wrap_mcp_tools(
        self: &Arc<Self>,
        connection: &Arc<McpServerConnection>,
    ) -> Vec<Arc<dyn Tool>> {
        connection
            .tools()
            .iter()
            .filter(|tool| self.is_tool_allowed(connection.name(), &tool.name))
            .map(|tool| self.build_adapter(connection, tool))
            .collect()
    }

    /// 装配单个适配器（注册表覆盖 + 进度端口 + 共享降级缓存）。
    fn build_adapter(
        self: &Arc<Self>,
        connection: &Arc<McpServerConnection>,
        tool: &crate::protocol::ToolDefinition,
    ) -> Arc<dyn Tool> {
        let capability = self
            .registry
            .as_ref()
            .and_then(|registry| registry.find_enabled_by_tool_name(connection.name(), &tool.name));
        let (enhanced_description, timeout_ms) =
            capability.as_ref().map_or((None, 0), |capability| {
                (capability.description.clone(), capability.timeout_ms)
            });
        let manager = Arc::downgrade(self);
        let current_connection = Arc::downgrade(connection);
        let server_name = connection.name().to_owned();
        let original_tool = tool.name.clone();
        let mut adapter = McpToolAdapter::new(
            build_external_tool_name(connection.name(), &tool.name, capability.as_ref()),
            Some(tool.description.clone()),
            Some(tool.input_schema.clone()),
            Arc::clone(connection),
            tool.name.clone(),
        )
        .with_execution_gate(Arc::new(move || {
            let (Some(manager), Some(connection)) =
                (manager.upgrade(), current_connection.upgrade())
            else {
                return false;
            };
            manager.is_current_connection(
                &server_name,
                &connection,
                manager.generation_of(&server_name),
            ) && manager.is_tool_allowed(&server_name, &original_tool)
        }))
        .with_registry_overrides(enhanced_description, timeout_ms)
        .with_capability_identity(
            capability.as_ref().map(|value| value.id.clone()),
            capability.as_ref().and_then(|value| value.domain.clone()),
        )
        .with_child_access(
            capability
                .as_ref()
                .and_then(|value| value.child_agent_access)
                .unwrap_or_default(),
        )
        .with_result_cache(Arc::clone(&self.result_cache));
        if let Some(tracker) = &self.progress_tracker {
            adapter = adapter.with_progress_tracker(Arc::clone(tracker));
        }
        Arc::new(adapter)
    }

    /// 注册某连接的全部工具并挂载变更监听（对照
    /// `registerToolsFromConnection`）。
    ///
    /// 监听回调持 `Weak` 引用（管理器与连接均是），避免
    /// `connection → callback → connection` 的 `Arc` 环。
    #[cfg(test)]
    fn register_tools_from_connection(self: &Arc<Self>, connection: &Arc<McpServerConnection>) {
        let directory = lock(&self.services.directory);
        self.register_tools_locked(&directory, connection);
    }

    fn tool_namespace_has_collision(
        self: &Arc<Self>,
        connection: &Arc<McpServerConnection>,
    ) -> bool {
        let candidates = self.wrap_mcp_tools(connection);
        let mut names = HashSet::new();
        let mut collision = candidates
            .iter()
            .any(|tool| !names.insert(tool.name().to_owned()));
        for other in self.connected_servers() {
            if other.name() == connection.name() {
                continue;
            }
            collision |= self
                .wrap_mcp_tools(&other)
                .iter()
                .any(|tool| names.contains(tool.name()));
        }
        collision
    }

    fn register_tools_locked(
        self: &Arc<Self>,
        directory: &MutexGuard<'_, ()>,
        connection: &Arc<McpServerConnection>,
    ) {
        if !self.is_current_connection(
            connection.name(),
            connection,
            self.generation_of(connection.name()),
        ) || connection.status() != McpConnectionStatus::Connected
        {
            return;
        }
        // Validate the whole final directory before publishing any adapter. Distinct
        // remote names can normalize (or hash/truncate) to the same model tool name.
        if self.tool_namespace_has_collision(connection) {
            self.clear_tool_directory_locked(directory, connection.name());
            connection.set_status(McpConnectionStatus::Failed);
            tracing::error!(
                server = connection.name(),
                code = "MCP_TOOL_NAMESPACE_COLLISION",
                "MCP tool directory rejected"
            );
            return;
        }
        let mut published = Vec::new();
        for tool in connection.tools() {
            if !self.is_tool_allowed(connection.name(), &tool.name) {
                tracing::info!(
                    server = connection.name(),
                    tool = %tool.name,
                    "MCP tool blocked by channel permissions"
                );
                continue;
            }
            self.tool_sink
                .register_dynamic(self.build_adapter(connection, &tool));
            published.push(tool);
        }
        self.tool_sink
            .publish_server_tools(connection.name(), published);

        // Prompt discovery performs protocol I/O. Register adapters asynchronously and
        // reject stale connection generations before touching the shared tool directory.
        let weak_manager = Arc::downgrade(self);
        let weak_connection = Arc::downgrade(connection);
        let generation = self.generation_of(connection.name());
        let transport_generation = connection.current_transport_generation();
        tokio::spawn(async move {
            let (Some(manager), Some(connection)) =
                (weak_manager.upgrade(), weak_connection.upgrade())
            else {
                return;
            };
            let name = connection.name().to_owned();
            let prompts = connection.list_prompts().await;
            let directory = lock(&manager.services.directory);
            if !manager.is_current_connection(&name, &connection, generation)
                || transport_generation.is_none_or(|generation| {
                    !connection.is_transport_generation_current(generation)
                })
            {
                return;
            }
            let mut names = manager
                .connected_servers()
                .iter()
                .flat_map(|server| manager.wrap_mcp_tools(server))
                .map(|tool| tool.name().to_owned())
                .collect::<HashSet<_>>();
            for prompt in prompts {
                let adapter = Arc::new(McpPromptAdapter::new(Arc::clone(&connection), prompt));
                if !names.insert(adapter.name().to_owned()) {
                    manager.clear_tool_directory_locked(&directory, &name);
                    connection.set_status(McpConnectionStatus::Failed);
                    tracing::error!(server = %name, code = "MCP_TOOL_NAMESPACE_COLLISION", "MCP prompt directory rejected");
                    return;
                }
                manager.tool_sink.register_dynamic(adapter);
            }
        });

        let weak_manager = Arc::downgrade(self);
        let weak_connection = Arc::downgrade(connection);
        connection.on_tools_changed(Arc::new(move || {
            let (Some(manager), Some(connection)) =
                (weak_manager.upgrade(), weak_connection.upgrade())
            else {
                return;
            };
            let name = connection.name().to_owned();
            if !manager.is_current_connection(&name, &connection, manager.generation_of(&name)) {
                return;
            }
            let directory = lock(&manager.services.directory);
            if !manager.is_current_connection(&name, &connection, manager.generation_of(&name)) {
                return;
            }
            manager.clear_tool_directory_locked(&directory, &name);
            manager.register_tools_locked(&directory, &connection);
            tracing::info!(server = %name, "MCP tools refreshed for server");
        }));
    }

    /// 工具放行判定（对照 `isToolAllowed`）：频道权限黑名单（空表放行；`"*"`
    /// 屏蔽整服务器）叠加注册表 allowlist——注册表管理的服务器仅放行已启用
    /// 条目，防止远端 `tools/list` 暴露未启用工具。
    fn is_tool_allowed(&self, server_name: &str, tool_name: &str) -> bool {
        if !self.is_service_enabled(server_name) {
            return false;
        }
        if let Some(parent) = &self.scope_parent
            && !parent
                .upgrade()
                .is_some_and(|parent| parent.is_tool_allowed(server_name, tool_name))
        {
            return false;
        }
        let channel_blocked = !self.channel_permissions.is_empty()
            && self
                .channel_permissions
                .get(server_name)
                .is_some_and(|blocked| {
                    blocked
                        .iter()
                        .any(|entry| entry == tool_name || entry == "*")
                });
        if channel_blocked {
            return false;
        }
        self.registry.as_ref().is_none_or(|registry| {
            !registry.has_definitions_for_server(server_name)
                || registry
                    .find_enabled_by_tool_name(server_name, tool_name)
                    .is_some()
        })
    }

    /// 发现所有已连接服务器的 prompt 模板（对照 `discoverPrompts()`）。
    pub async fn discover_prompts(&self) -> Vec<(String, PromptDefinition)> {
        let servers = self.connected_servers();
        let mut discovered = Vec::new();
        for connection in &servers {
            for prompt in connection.list_prompts().await {
                discovered.push((connection.name().to_owned(), prompt));
            }
        }
        tracing::info!(
            prompts = discovered.len(),
            servers = servers.len(),
            "Discovered MCP prompts"
        );
        discovered
    }
}

// ===== M3 Roots =====

impl McpClientManager {
    /// Roots 提供者（供 zk-server 在工程切换时复用）。
    #[must_use]
    pub fn roots_provider(&self) -> Arc<RootsProvider> {
        Arc::clone(&self.roots_provider)
    }

    /// 以进程工作目录初始化默认 roots（对照 `initializeDefaultRoots()` 的
    /// `System.getProperty("user.dir")`）。
    fn initialize_default_roots(&self) {
        let Ok(workspace) = std::env::current_dir() else {
            tracing::warn!("user.dir not available — MCP roots left empty");
            return;
        };
        let workspace_path = workspace.to_string_lossy().to_string();
        if workspace_path.trim().is_empty() {
            tracing::warn!("user.dir not available — MCP roots left empty");
            return;
        }
        let project_name = workspace.file_name().map_or_else(
            || "workspace".to_owned(),
            |name| name.to_string_lossy().to_string(),
        );
        self.roots_provider
            .update_roots(&workspace_path, &project_name);
        tracing::info!(
            workspace = %workspace_path,
            name = %project_name,
            "MCP roots initialized"
        );
    }

    /// 工程切换：更新 roots 并通知所有已连接服务器（对照
    /// `onWorkspaceChanged(event)`）。
    pub async fn on_workspace_changed(&self, workspace_path: &str, project_name: &str) {
        self.roots_provider
            .update_roots(workspace_path, project_name);
        tracing::info!(
            workspace = workspace_path,
            name = project_name,
            "Workspace changed — roots updated"
        );
        for connection in self.connected_servers() {
            connection
                .send_notification(METHOD_ROOTS_LIST_CHANGED, None)
                .await;
        }
    }
}

// ===== 健康检查 + 退避重连 =====

impl McpClientManager {
    /// 以 30s 间隔驱动 [`Self::health_check`]（对照两个 `@Scheduled`：均为 30s，
    /// 主动探测的 `initialDelay` 亦为 30s，故循环先睡后跑）。
    ///
    /// 管理器退出运行态后任务自行结束。
    pub fn spawn_health_check_loop(self: &Arc<Self>) -> JoinHandle<()> {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(HEALTH_CHECK_INTERVAL).await;
                if !manager.is_running() {
                    return;
                }
                manager.health_check().await;
            }
        })
    }

    /// 一轮健康检查：被动状态巡检 + 主动 ping 探测。
    ///
    /// 第一段对照 `McpClientManager.healthCheck()`（`isAlive()` 标志位 → FAILED
    /// → 非 stdio 则调度重连）；第二段对照
    /// `SseHealthChecker.performActiveHealthCheck()`（`sendHealthPing()` 连续失败
    /// 2 次 → DEGRADED + 立即重连）。
    pub async fn health_check(self: &Arc<Self>) {
        if !self.is_running() {
            return;
        }
        for connection in self.list_connections() {
            self.check_passive_health(&connection);
        }

        for connection in self.list_connections() {
            let name = connection.name().to_owned();
            let (generation, transport_generation) = {
                let _directory = lock(&self.services.directory);
                let generation = self.generation_of(&name);
                if !self.is_current_connection(&name, &connection, generation)
                    || connection.status() != McpConnectionStatus::Connected
                {
                    continue;
                }
                let Some(transport_generation) = connection.current_transport_generation() else {
                    continue;
                };
                (generation, transport_generation)
            };
            let healthy = connection.send_health_ping().await;
            let directory = lock(&self.services.directory);
            if !self.is_current_connection(&name, &connection, generation)
                || !connection.is_transport_generation_current(transport_generation)
            {
                continue;
            }
            if healthy {
                lock(&self.consecutive_failures).insert(name.clone(), 0);
                lock(&self.last_successful_ping).insert(name, SystemTime::now());
                continue;
            }
            let failures = {
                let mut counters = lock(&self.consecutive_failures);
                let counter = counters.entry(name.clone()).or_insert(0);
                *counter += 1;
                *counter
            };
            tracing::warn!(server = %name, failures, "Health ping failed");
            if failures >= HEALTH_PING_FAILURE_THRESHOLD {
                self.schedule_reconnect_locked(&directory, &name, &connection, generation);
            }
        }
    }

    fn check_passive_health(self: &Arc<Self>, connection: &Arc<McpServerConnection>) {
        let directory = lock(&self.services.directory);
        let name = connection.name();
        if !self.is_current_connection(name, connection, self.generation_of(name)) {
            return;
        }
        // Inspect the current transport while holding the publication lock: a
        // same-Arc restart cannot turn this observation into a stale clear.
        if connection.status() == McpConnectionStatus::Connected && !connection.is_alive() {
            tracing::warn!(server = name, "MCP server connection lost");
            connection.set_status(McpConnectionStatus::Failed);
            self.clear_tool_directory_locked(&directory, name);
        }
        if connection.status() == McpConnectionStatus::Failed
            && connection.config().transport != McpTransportType::Stdio
        {
            self.schedule_delayed_reconnect_locked(
                &directory,
                name,
                connection,
                self.generation_of(name),
            );
        }
    }

    /// 主动 ping 的连续失败次数（对照
    /// `SseHealthChecker.getConsecutiveFailures`）。
    #[must_use]
    pub fn consecutive_failures(&self, server_name: &str) -> u32 {
        lock(&self.consecutive_failures)
            .get(server_name)
            .copied()
            .unwrap_or(0)
    }

    /// 最近一次成功 ping 的时刻（对照
    /// `SseHealthChecker.getLastSuccessfulPing`）。
    #[must_use]
    pub fn last_successful_ping(&self, server_name: &str) -> Option<SystemTime> {
        lock(&self.last_successful_ping).get(server_name).copied()
    }

    /// 对 `FAILED` / `PENDING` 的非 stdio 连接批量调度重连（对照
    /// `reconnectFailed()`）。
    pub fn reconnect_failed(self: &Arc<Self>) {
        for connection in self.list_connections() {
            let directory = lock(&self.services.directory);
            let generation = self.generation_of(connection.name());
            if !self.is_current_connection(connection.name(), &connection, generation) {
                continue;
            }
            let status = connection.status();
            if status != McpConnectionStatus::Failed && status != McpConnectionStatus::Pending {
                continue;
            }
            if connection.config().transport == McpTransportType::Stdio {
                continue;
            }
            if connection.reconnect_attempts() >= MAX_RECONNECT_ATTEMPTS {
                continue;
            }
            let name = connection.name().to_owned();
            self.schedule_delayed_reconnect_locked(&directory, &name, &connection, generation);
        }
    }

    /// 立即调度一次重连并置 `DEGRADED`（对照 `scheduleReconnect(name)`）。
    pub fn schedule_reconnect(self: &Arc<Self>, connection_name: &str) {
        let directory = lock(&self.services.directory);
        let Some(connection) = self.get_connection(connection_name) else {
            return;
        };
        let generation = self.generation_of(connection_name);
        self.schedule_reconnect_locked(&directory, connection_name, &connection, generation);
    }

    fn schedule_reconnect_locked(
        self: &Arc<Self>,
        directory: &MutexGuard<'_, ()>,
        connection_name: &str,
        connection: &Arc<McpServerConnection>,
        generation: u64,
    ) {
        if !self.is_current_connection(connection_name, connection, generation) {
            return;
        }
        connection.set_status(McpConnectionStatus::Degraded);
        self.clear_tool_directory_locked(directory, connection_name);
        self.broadcast_health_status(connection_name, McpConnectionStatus::Degraded);
        self.submit_reconnect_locked(directory, connection_name, connection, generation);
    }

    /// 延迟重连调度（对照 `scheduleDelayedReconnect`）。
    ///
    /// The directory guard keeps the owner check and scheduler replacement atomic.
    fn schedule_delayed_reconnect_locked(
        self: &Arc<Self>,
        _directory: &MutexGuard<'_, ()>,
        server_id: &str,
        connection: &Arc<McpServerConnection>,
        generation: u64,
    ) {
        if !self.is_current_connection(server_id, connection, generation) {
            return;
        }
        let attempt = connection.reconnect_attempts();
        if attempt >= MAX_RECONNECT_ATTEMPTS {
            tracing::warn!(
                server = server_id,
                max_attempts = MAX_RECONNECT_ATTEMPTS,
                "MCP server exceeded max reconnect attempts, giving up"
            );
            return;
        }
        let backoff_ms = calculate_backoff_with_jitter(attempt + 1);

        let mut scheduled = lock(&self.scheduled_reconnects);
        if let Some(existing) = scheduled.get(server_id) {
            if Arc::ptr_eq(&existing.connection, connection)
                && existing.generation == generation
                && !existing.is_done()
            {
                return;
            }
            existing.cancel_gracefully();
        }
        tracing::info!(
            server = server_id,
            backoff_ms,
            attempt = attempt + 1,
            max_attempts = MAX_RECONNECT_ATTEMPTS,
            "Scheduling reconnect for MCP server"
        );
        let task_id = self.next_task_id.fetch_add(1, Ordering::AcqRel);
        let cancel = CancellationToken::new();
        let handle = self.spawn_delayed_reconnect(
            server_id.to_owned(),
            Arc::clone(connection),
            generation,
            Duration::from_millis(backoff_ms),
            task_id,
            cancel.clone(),
        );
        scheduled.insert(
            server_id.to_owned(),
            ReconnectTask {
                task_id,
                connection: Arc::clone(connection),
                generation,
                cancel,
                handle,
            },
        );
    }

    /// 退避等待 → 摘除自身条目 → 提交重连（对照 Java 调度器上的 lambda）。
    fn spawn_delayed_reconnect(
        self: &Arc<Self>,
        server_id: String,
        connection: Arc<McpServerConnection>,
        generation: u64,
        backoff: Duration,
        task_id: u64,
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(backoff) => {}
            }
            let directory = lock(&manager.services.directory);
            remove_task_by_id(&manager.scheduled_reconnects, &server_id, task_id);
            manager.submit_reconnect_locked(&directory, &server_id, &connection, generation);
        })
    }

    /// 提交重连到并发受限的工作池（对照 `submitReconnect`）。
    fn submit_reconnect_locked(
        self: &Arc<Self>,
        _directory: &MutexGuard<'_, ()>,
        server_id: &str,
        connection: &Arc<McpServerConnection>,
        generation: u64,
    ) {
        if !self.is_current_connection(server_id, connection, generation) {
            return;
        }
        let mut active = lock(&self.active_reconnects);
        if let Some(existing) = active.get(server_id) {
            if Arc::ptr_eq(&existing.connection, connection)
                && existing.generation == generation
                && !existing.is_done()
            {
                return;
            }
            existing.cancel_forcefully();
        }
        let task_id = self.next_task_id.fetch_add(1, Ordering::AcqRel);
        let cancel = CancellationToken::new();
        let handle = self.spawn_reconnect_worker(
            server_id.to_owned(),
            Arc::clone(connection),
            generation,
            task_id,
            cancel.clone(),
        );
        active.insert(
            server_id.to_owned(),
            ReconnectTask {
                task_id,
                connection: Arc::clone(connection),
                generation,
                cancel,
                handle,
            },
        );
    }

    /// 重连工作任务（对照 `FutureTask` + `finally` 摘除自身条目）。
    ///
    /// 任务体在取许可后才执行，`Semaphore(2)` 等价 Java 的固定 2 线程池。
    fn spawn_reconnect_worker(
        self: &Arc<Self>,
        server_id: String,
        connection: Arc<McpServerConnection>,
        generation: u64,
        task_id: u64,
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            if let Ok(_permit) = manager.reconnect_permits.acquire().await {
                tokio::select! {
                    () = cancel.cancelled() => {}
                    () = manager.attempt_reconnect(&server_id, &connection, generation) => {}
                }
            }
            remove_task_by_id(&manager.active_reconnects, &server_id, task_id);
        })
    }

    /// 实际重连（对照 `attemptReconnect`）。
    async fn attempt_reconnect(
        self: &Arc<Self>,
        server_id: &str,
        connection: &Arc<McpServerConnection>,
        generation: u64,
    ) {
        {
            let _directory = lock(&self.services.directory);
            if !self.is_current_connection(server_id, connection, generation) {
                return;
            }
            let mut reconnecting = lock(&self.reconnecting_servers);
            if reconnecting
                .get(server_id)
                .is_some_and(|(current_generation, current)| {
                    *current_generation == generation && Arc::ptr_eq(current, connection)
                })
            {
                tracing::debug!(
                    server = server_id,
                    "Reconnect already in progress, skipping"
                );
                return;
            }
            reconnecting.insert(server_id.to_owned(), (generation, Arc::clone(connection)));
        }
        self.reconnect_once(server_id, connection, generation).await;
        let _directory = lock(&self.services.directory);
        let mut reconnecting = lock(&self.reconnecting_servers);
        if reconnecting
            .get(server_id)
            .is_some_and(|(current_generation, current)| {
                *current_generation == generation && Arc::ptr_eq(current, connection)
            })
        {
            reconnecting.remove(server_id);
        }
    }

    /// `attemptReconnect` 的 try 块主体（拆分以保证 `finally` 语义总被执行）。
    async fn reconnect_once(
        self: &Arc<Self>,
        server_id: &str,
        connection: &Arc<McpServerConnection>,
        generation: u64,
    ) {
        if !self.is_current_connection(server_id, connection, generation) {
            return;
        }
        if let Some(oauth) = &self.oauth {
            match oauth.has_binding(server_id).await {
                Ok(true) => {
                    if self
                        .restart_server_if_current(server_id, connection, generation)
                        .await
                        .is_err()
                    {
                        let _directory = lock(&self.services.directory);
                        if self.is_current_connection(server_id, connection, generation) {
                            connection.increment_reconnect_attempts();
                        }
                    }
                    return;
                }
                Ok(false) => {}
                Err(_) => {
                    let _directory = lock(&self.services.directory);
                    if self.is_current_connection(server_id, connection, generation) {
                        connection.set_status(McpConnectionStatus::NeedsAuth);
                    }
                    return;
                }
            }
        }
        let cancel = {
            let _directory = lock(&self.services.directory);
            if !self.is_current_connection(server_id, connection, generation) {
                return;
            }
            lock(&self.services.cancellations)
                .entry(server_id.to_owned())
                .or_default()
                .clone()
        };
        if self
            .connect_managed(server_id, generation, connection, &cancel)
            .await
            .is_err()
        {
            return;
        }
        self.finish_reconnect(server_id, connection, generation);
    }

    fn finish_reconnect(
        self: &Arc<Self>,
        server_id: &str,
        connection: &Arc<McpServerConnection>,
        generation: u64,
    ) {
        let directory = lock(&self.services.directory);
        if !self.is_current_connection(server_id, connection, generation) {
            return;
        }
        if connection.status() == McpConnectionStatus::Connected {
            connection.reset_reconnect_attempts();
            // A reconnect is a complete directory replacement.  Tools and
            // prompts removed by the new server session must not survive.
            self.clear_tool_directory_locked(&directory, server_id);
            self.register_tools_locked(&directory, connection);
            tracing::info!(server = server_id, "MCP server reconnected successfully");
            self.broadcast_health_status(server_id, McpConnectionStatus::Connected);
        } else {
            connection.increment_reconnect_attempts();
            tracing::debug!(
                server = server_id,
                status = %connection.status(),
                "Reconnect failed"
            );
        }
    }

    /// 状态广播（对照 `broadcastHealthStatus`：无观察者时静默）。
    fn broadcast_health_status(&self, server_name: &str, status: McpConnectionStatus) {
        if let Some(observer) = &self.health_observer {
            observer.on_health_status(server_name, status);
        }
    }
}

// ===== 能力注册表集成 =====

impl McpClientManager {
    /// 从注册表条目启用一个 MCP 服务器（对照 `enableFromRegistry`）。
    ///
    /// # Errors
    ///
    /// 同 [`Self::add_server`]。
    pub async fn enable_from_registry(
        self: &Arc<Self>,
        definition: &McpCapabilityDefinition,
    ) -> Result<Arc<McpServerConnection>, ManagerError> {
        if !self.is_service_enabled(&definition.extract_server_key()) {
            return Err(ManagerError::ServiceDisabled(
                definition.extract_server_key(),
            ));
        }
        validate_capability_destination(definition)
            .await
            .map_err(|error| ManagerError::UnsafeCapabilityEndpoint(error.to_string()))?;
        let config = self.build_resolved_config_from_registry(definition);
        tracing::info!(
            capability = %definition.id,
            server = %config.name,
            "Enabling MCP capability"
        );
        if !self.approval.is_trusted(&config) {
            self.approval.record_approval(&config, "REGISTRY");
            tracing::info!(capability = %definition.id, "Auto-trusted registry capability");
        }
        let server_name = config.name.clone();
        let connection = self.add_server_from(config, true).await?;
        lock(&self.registry_owned_servers).insert(server_name);
        Ok(connection)
    }

    /// 由注册表条目构建服务器配置（对照 `buildConfigFromRegistry`；传输类型取
    /// [`McpCapabilityDefinition::resolved_transport_type`]，v1.0 条目保持 SSE）。
    ///
    /// API Key 经受限 [`McpCredentialResolver`] 解析：注册表只能选择已知
    /// `DashScope` provider 身份，不会把任意 `apiKeyConfig` 转换成环境变量名。
    /// 端点 URL 同样过占位符展开，空白或
    /// 未解析的整串 `${...}` 归 `None`（对照 Java v1.1 对 `endpointUrl` 的
    /// `resolvePlaceholders` + 过滤）。
    #[must_use]
    pub fn build_config_from_registry(definition: &McpCapabilityDefinition) -> McpServerConfig {
        let resolver = default_credential_resolver();
        Self::build_config_from_registry_with(definition, resolver.as_ref())
    }

    /// Build a capability config with the host-injected credential resolver.
    #[must_use]
    pub fn build_resolved_config_from_registry(
        &self,
        definition: &McpCapabilityDefinition,
    ) -> McpServerConfig {
        Self::build_config_from_registry_with(definition, self.credential_resolver.as_ref())
    }

    fn build_config_from_registry_with(
        definition: &McpCapabilityDefinition,
        resolver: &dyn McpCredentialResolver,
    ) -> McpServerConfig {
        let endpoint_url = definition
            .url
            .as_deref()
            .map(expand_env_placeholders)
            .filter(|value| !is_blank_or_placeholder(value));
        let mut headers = BTreeMap::new();
        if let Some(api_key) = resolve_capability_credential(definition, resolver) {
            headers.insert("Authorization".to_owned(), format!("Bearer {api_key}"));
        }
        McpServerConfig {
            name: definition.extract_server_key(),
            transport: definition.resolved_transport_type(),
            command: None,
            args: Vec::new(),
            env: BTreeMap::new(),
            url: endpoint_url,
            headers,
            scope: McpConfigScope::Dynamic,
        }
    }
}

/// `mcp__<server>__` 前缀（对照 Java v1.1 `mcpToolPrefix`：服务器名先经
/// [`safe_tool_name_component`] 安全化）。
fn tool_prefix(server_name: &str) -> String {
    format!(
        "{MCP_TOOL_NAME_PREFIX}{}__",
        safe_tool_name_component(Some(server_name), "server")
    )
}

/// 对外工具名长度上限（对照 Java `buildExternalToolName` 的 64——`OpenAI`
/// 风格 function-name 约束）。
const MAX_EXTERNAL_TOOL_NAME_LEN: usize = 64;

/// 对外工具名（对照 Java `buildExternalToolName`）：必须兼容 `OpenAI` 风格的
/// function-name 约束（`[A-Za-z0-9_-]`，≤64 字符）。远端原始工具名仍保存在
/// [`McpToolAdapter`] 中，实际 `tools/call` 不受重命名影响。
///
/// 原始工具名不满足约束时依次回落：注册表条目 id（剥 `mcp_` 前缀）→
/// `tool_<hash>`；拼接超长则截断并追加 `_<hash>` 后缀（候选名仅含 ASCII，
/// 按字节截断即 Java 的 `substring` 语义）。
fn build_external_tool_name(
    server_name: &str,
    original_tool_name: &str,
    capability: Option<&McpCapabilityDefinition>,
) -> String {
    let safe_server = safe_tool_name_component(Some(server_name), "server");
    // 对照 Java `matches("[A-Za-z0-9_-]+")`：整串合法才直接采用。
    let mut safe_tool = Some(original_tool_name)
        .filter(|name| {
            !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        })
        .map(str::to_owned);
    if safe_tool.as_deref().is_none_or(java_is_blank) {
        safe_tool = capability.map(|capability| {
            capability
                .id
                .strip_prefix("mcp_")
                .unwrap_or(&capability.id)
                .to_owned()
        });
    }
    let fallback = format!("tool_{}", short_name_hash(original_tool_name));
    let safe_tool = safe_tool_name_component(safe_tool.as_deref(), &fallback);
    let candidate = format!("{MCP_TOOL_NAME_PREFIX}{safe_server}__{safe_tool}");
    if candidate.len() <= MAX_EXTERNAL_TOOL_NAME_LEN {
        return candidate;
    }
    let suffix = format!("_{}", short_name_hash(&candidate));
    format!(
        "{}{suffix}",
        &candidate[..MAX_EXTERNAL_TOOL_NAME_LEN - suffix.len()]
    )
}

/// 对照 Java `safeToolNameComponent`：空值/空白 → fallback；非法字符替换为
/// `_` 并折叠连续 `_`（`replaceAll("[^A-Za-z0-9_-]", "_").replaceAll("_+",
/// "_")`）；替换后仍空白 → fallback。
fn safe_tool_name_component(value: Option<&str>, fallback: &str) -> String {
    let Some(value) = value.filter(|value| !java_is_blank(value)) else {
        return fallback.to_owned();
    };
    let mut safe = String::with_capacity(value.len());
    for c in value.chars() {
        let mapped = if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
            c
        } else {
            '_'
        };
        if mapped == '_' && safe.ends_with('_') {
            continue;
        }
        safe.push(mapped);
    }
    if java_is_blank(&safe) {
        fallback.to_owned()
    } else {
        safe
    }
}

/// 对照 Java `shortNameHash`：`String.format("%08x", value.hashCode())`——
/// 负 hash 按 32 位补码输出（如 `Integer.MIN_VALUE` → `80000000`）。
fn short_name_hash(value: &str) -> String {
    format!("{:08x}", java_string_hashcode(value).cast_unsigned())
}

/// 对照 Java `String.hashCode()`：按 UTF-16 码元累加
/// `s[0]·31^(n-1) + … + s[n-1]`，32 位有符号回绕。
fn java_string_hashcode(value: &str) -> i32 {
    let mut hash = 0i32;
    for unit in value.encode_utf16() {
        hash = hash.wrapping_mul(31).wrapping_add(i32::from(unit));
    }
    hash
}

/// 仅当映射中该键仍指向同一连接时移除（对照 `Map.remove(key, value)`）。
fn remove_if_same(
    map: &Mutex<HashMap<String, Arc<McpServerConnection>>>,
    key: &str,
    connection: &Arc<McpServerConnection>,
) {
    let mut guard = lock(map);
    if guard
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, connection))
    {
        guard.remove(key);
    }
}

/// 仅当映射中该键仍是本任务时移除（对照 Java 用引用相等/字段相等做的自摘除）。
fn remove_task_by_id(map: &Mutex<HashMap<String, ReconnectTask>>, key: &str, task_id: u64) {
    let mut guard = lock(map);
    if guard.get(key).is_some_and(|task| task.task_id == task_id) {
        guard.remove(key);
    }
}

/// 等待一批重连任务收敛（对照 `awaitTermination(executor, label)`）。
async fn await_reconnect_tasks(tasks: Vec<ReconnectTask>, label: &str) {
    if tasks.is_empty() {
        return;
    }
    let joined = async {
        for task in tasks {
            let _ = task.handle.await;
        }
    };
    if tokio::time::timeout(SHUTDOWN_GRACE, joined).await.is_err() {
        tracing::warn!(
            component = label,
            "MCP executor did not terminate within 5 seconds"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use futures::future::BoxFuture;
    use serde_json::{Value, json};

    use super::*;
    use crate::error::McpProtocolError;
    use crate::jsonrpc::RequestId;
    use crate::protocol::ToolDefinition;
    use crate::tool_adapter::MCP_TOOL_MAX_EXECUTION;
    use crate::transport::{McpTransport, NotificationHandler};

    #[derive(Default)]
    struct MemoryPreferences {
        values: Mutex<BTreeMap<String, bool>>,
        fail_load: AtomicBool,
        fail_save: AtomicBool,
    }

    impl McpServicePreferenceStore for MemoryPreferences {
        fn load(&self) -> BoxFuture<'_, Result<BTreeMap<String, bool>, String>> {
            Box::pin(async move {
                if self.fail_load.load(Ordering::Acquire) {
                    return Err("storage unavailable".into());
                }
                Ok(lock(&self.values).clone())
            })
        }
        fn save(&self, values: BTreeMap<String, bool>) -> BoxFuture<'_, Result<(), String>> {
            Box::pin(async move {
                if self.fail_save.load(Ordering::Acquire) {
                    return Err("disk full".into());
                }
                *lock(&self.values) = values;
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn service_switch_persists_across_managers_and_rejects_all_reactivation_paths() {
        let store = Arc::new(MemoryPreferences::default());
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let config = config_with("srv", McpTransportType::Sdk, McpConfigScope::User);
        let manager = builder(&approval, &sink)
            .service_preferences(store.clone())
            .static_configs(vec![config.clone()])
            .build();
        running(&manager);
        manager.load_service_preferences().await.unwrap();
        let connection = add_trusted(&manager, &approval, config.clone()).await;
        connection.set_tools(vec![tool_def("echo")]);
        let stale = manager.discover_and_wrap_tools().remove(0);
        manager.register_tools_from_connection(&connection);
        let view = manager.set_service_enabled("srv", false).await.unwrap();
        assert!(!view.enabled);
        assert_eq!(view.status, "disabled");
        assert_eq!(view.tool_count, 0);
        assert!(manager.get_connection("srv").is_none());
        assert!(manager.discover_and_wrap_tools().is_empty());
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let output = stale
            .execute(
                json!({}),
                zk_tools::ToolContext::new(CancellationToken::new(), tx),
            )
            .await;
        assert!(output.is_error);
        assert!(output.content.contains("disabled"));
        let published = sink.published().len();
        connection.set_tools(vec![tool_def("late")]);
        connection.notify_tools_changed();
        assert_eq!(
            sink.published().len(),
            published,
            "stale discovery may not resurrect tools"
        );
        assert!(matches!(
            manager.add_server(config.clone()).await,
            Err(ManagerError::ServiceDisabled(_))
        ));
        assert!(matches!(
            manager.restart_server("srv").await,
            Err(ManagerError::ServiceDisabled(_))
        ));
        let recovered = builder(&approval, &sink)
            .service_preferences(store)
            .static_configs(vec![config])
            .build();
        running(&recovered);
        let views = recovered.list_services().await.unwrap();
        assert!(
            !views
                .iter()
                .find(|service| service.name == "srv")
                .unwrap()
                .enabled
        );
        recovered.initialize_static_configs().await;
        assert!(recovered.get_connection("srv").is_none());
    }

    #[tokio::test]
    async fn service_storage_errors_fail_closed_and_do_not_change_last_valid_state() {
        let store = Arc::new(MemoryPreferences::default());
        store.fail_load.store(true, Ordering::Release);
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let config = config_with("srv", McpTransportType::Sdk, McpConfigScope::User);
        let manager = builder(&approval, &sink)
            .service_preferences(store.clone())
            .static_configs(vec![config.clone()])
            .build();
        assert_eq!(
            manager.start().await,
            Err(ManagerError::ServiceStorageUnavailable)
        );
        assert!(!manager.is_service_enabled("srv"));
        assert_eq!(manager.connection_count(), 0);
        store.fail_load.store(false, Ordering::Release);
        manager.load_service_preferences().await.unwrap();
        running(&manager);
        let connection = add_trusted(&manager, &approval, config).await;
        store.fail_save.store(true, Ordering::Release);
        assert!(matches!(
            manager.set_service_enabled("srv", false).await,
            Err(ManagerError::ServiceStorageUnavailable)
        ));
        assert!(manager.is_service_enabled("srv"));
        assert!(Arc::ptr_eq(
            &manager.get_connection("srv").unwrap(),
            &connection
        ));
        assert!(lock(&store.values).is_empty());
    }

    #[tokio::test]
    async fn service_disable_cancels_an_inflight_real_http_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let accepted = Arc::new(tokio::sync::Notify::new());
        let signal = accepted.clone();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            signal.notify_one();
            std::future::pending::<()>().await;
        });
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let config = McpServerConfig::sse("slow", format!("http://{addr}/mcp"));
        approval.record_approval(&config, "TEST");
        let connecting_manager = manager.clone();
        let connect = tokio::spawn(async move { connecting_manager.add_server(config).await });
        tokio::time::timeout(Duration::from_secs(3), accepted.notified())
            .await
            .unwrap();
        manager.set_service_enabled("slow", false).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), connect)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(ManagerError::ServiceDisabled(_))));
        assert!(manager.get_connection("slow").is_none());
        assert!(sink.registered().is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn superseded_service_disable_keeps_the_owner_for_a_newer_restart() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::Dynamic),
        )
        .await;
        let gate = Arc::new(Semaphore::new(0));
        connection.set_transport_for_test(Arc::new(StubTransport {
            connected: true,
            close_gate: Some(gate.clone()),
            ..StubTransport::default()
        }));
        let mut disabling = Box::pin(manager.set_service_enabled("srv", false));
        assert!(futures::poll!(&mut disabling).is_pending());
        let mut enabling = Box::pin(manager.set_service_enabled("srv", true));
        assert!(futures::poll!(&mut enabling).is_pending());
        let mut restarting = Box::pin(manager.restart_server("srv"));
        assert!(futures::poll!(&mut restarting).is_pending());

        gate.add_permits(1);
        let disabled = disabling.await;
        assert!(
            manager
                .get_connection("srv")
                .is_some_and(|current| Arc::ptr_eq(&current, &connection)),
            "a superseded disable removed the newer restart's cleanup owner"
        );
        assert!(matches!(disabled, Err(ManagerError::LifecycleChanged)));
        assert!(matches!(
            enabling.await,
            Err(ManagerError::LifecycleChanged)
        ));
        restarting.await.unwrap();
        assert!(manager.is_service_enabled("srv"));
        assert!(Arc::ptr_eq(
            &manager.get_connection("srv").unwrap(),
            &connection
        ));
    }

    #[tokio::test]
    async fn capability_switch_revokes_retained_adapter_and_discovery_without_changing_service_preference()
     {
        let path = std::env::temp_dir().join(format!(
            "zk-service-capabilities-{}.json",
            uuid::Uuid::new_v4()
        ));
        let registry = Arc::new(McpCapabilityRegistry::new(path.clone()));
        for name in ["first", "second"] {
            let mut definition = McpCapabilityDefinition::new(name);
            definition.server_key = Some("srv".into());
            definition.tool_name = Some(name.into());
            definition.enabled = true;
            registry.add_capability(definition).unwrap();
        }
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).registry(registry.clone()).build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;
        connection.set_tools(vec![tool_def("first"), tool_def("second")]);
        let retained = manager.discover_and_wrap_tools().remove(0);
        registry.toggle_enabled("first", false).unwrap();
        manager.refresh_service_tools("srv");
        assert!(manager.is_service_enabled("srv"));
        assert_eq!(manager.discover_and_wrap_tools().len(), 1);
        assert_eq!(sink.published().last().unwrap().1, vec!["second"]);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        assert!(
            retained
                .execute(
                    json!({}),
                    zk_tools::ToolContext::new(CancellationToken::new(), tx)
                )
                .await
                .is_error
        );
        manager.set_service_enabled("srv", false).await.unwrap();
        manager.set_service_enabled("srv", true).await.unwrap();
        assert!(!registry.find_by_id("first").unwrap().enabled);
        assert!(registry.find_by_id("second").unwrap().enabled);
        registry.save();
        std::fs::remove_file(path).unwrap();
    }

    // ===== 端口替身 =====

    /// 记录式信任端口：`record_approval` 后该服务器即视为可信（对照
    /// `McpApprovalService` 落盘后 `isTrusted` 转真）。
    #[derive(Default)]
    struct RecordingApproval {
        trusted: Mutex<HashSet<String>>,
        approvals: Mutex<Vec<(String, String)>>,
    }

    impl RecordingApproval {
        fn shared() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn sources_for(&self, name: &str) -> Vec<String> {
            lock(&self.approvals)
                .iter()
                .filter(|(server, _)| server == name)
                .map(|(_, source)| source.clone())
                .collect()
        }
    }

    impl ApprovalPort for RecordingApproval {
        fn is_trusted(&self, config: &McpServerConfig) -> bool {
            lock(&self.trusted).contains(&config.name)
        }

        fn record_approval(&self, config: &McpServerConfig, source: &str) {
            lock(&self.trusted).insert(config.name.clone());
            lock(&self.approvals).push((config.name.clone(), source.to_owned()));
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        registered: Mutex<Vec<String>>,
        unregistered: Mutex<Vec<String>>,
        published: Mutex<Vec<(String, Vec<String>)>>,
    }

    impl RecordingSink {
        fn shared() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn registered(&self) -> Vec<String> {
            lock(&self.registered).clone()
        }

        fn unregistered(&self) -> Vec<String> {
            lock(&self.unregistered).clone()
        }

        fn published(&self) -> Vec<(String, Vec<String>)> {
            lock(&self.published).clone()
        }
    }

    impl McpToolSink for RecordingSink {
        fn register_dynamic(&self, tool: Arc<dyn Tool>) {
            lock(&self.registered).push(tool.name().to_owned());
        }

        fn unregister_by_prefix(&self, prefix: &str) {
            lock(&self.unregistered).push(prefix.to_owned());
        }

        fn publish_server_tools(&self, server_id: &str, tools: Vec<ToolDefinition>) {
            lock(&self.published).push((
                server_id.to_owned(),
                tools.into_iter().map(|tool| tool.name).collect(),
            ));
        }
    }

    #[derive(Default)]
    struct RecordingObserver {
        events: Mutex<Vec<(String, McpConnectionStatus)>>,
    }

    impl RecordingObserver {
        fn shared() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn saw(&self, server: &str, status: McpConnectionStatus) -> bool {
            lock(&self.events)
                .iter()
                .any(|(name, seen)| name == server && *seen == status)
        }
    }

    impl McpHealthObserver for RecordingObserver {
        fn on_health_status(&self, server_name: &str, status: McpConnectionStatus) {
            lock(&self.events).push((server_name.to_owned(), status));
        }
    }

    /// 可编程传输替身：`connected` 决定 `is_alive`，`ping` 决定主动探测结果。
    #[derive(Default)]
    struct StubTransport {
        connected: bool,
        ping: bool,
        ping_gate: Option<Arc<Semaphore>>,
        prompt_name: Option<&'static str>,
        prompt_observed: Option<Arc<tokio::sync::Notify>>,
        cleanup: Option<Arc<AtomicBool>>,
        close_gate: Option<Arc<Semaphore>>,
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
            _params: Option<Value>,
            _timeout: Duration,
        ) -> BoxFuture<'a, Result<Option<Value>, McpProtocolError>> {
            Box::pin(async move {
                if method == "prompts/list"
                    && let Some(name) = self.prompt_name
                {
                    if let Some(observed) = &self.prompt_observed {
                        observed.notify_one();
                    }
                    return Ok(Some(json!({"prompts":[{"name":name,"arguments":[]}]})));
                }
                Ok(None)
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

        fn send_health_ping(&self) -> BoxFuture<'_, bool> {
            Box::pin(async move {
                if let Some(gate) = &self.ping_gate {
                    gate.acquire().await.unwrap().forget();
                }
                self.ping
            })
        }

        fn is_connected(&self) -> bool {
            self.connected
        }

        fn set_notification_handler(&self, _handler: NotificationHandler) {}

        fn close(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {
                if let Some(gate) = &self.close_gate {
                    gate.acquire().await.unwrap().forget();
                }
            })
        }

        fn cleanup_confirmed(&self) -> bool {
            self.cleanup
                .as_ref()
                .is_none_or(|value| value.load(Ordering::Acquire))
        }
    }

    // ===== 装配辅助 =====

    /// Build a configuration without performing network or process I/O.
    fn config_with(
        name: &str,
        transport: McpTransportType,
        scope: McpConfigScope,
    ) -> McpServerConfig {
        McpServerConfig {
            name: name.to_owned(),
            transport,
            command: None,
            args: Vec::new(),
            env: BTreeMap::new(),
            url: None,
            headers: BTreeMap::new(),
            scope,
        }
    }

    fn builder(
        approval: &Arc<RecordingApproval>,
        sink: &Arc<RecordingSink>,
    ) -> McpClientManagerBuilder {
        McpClientManager::builder(
            Arc::clone(approval) as Arc<dyn ApprovalPort>,
            Arc::clone(sink) as Arc<dyn McpToolSink>,
        )
        .resolver(McpConfigurationResolver::new(None))
        .result_cache(Arc::new(ResultCache::new()))
    }

    /// 置运行态但不跑 `initialize_all` —— 后者会读 `MCP_SERVERS` 环境变量与
    /// `~/.zk/mcp.json`，测试不应依赖开发机上的真实配置。
    fn running(manager: &Arc<McpClientManager>) {
        manager.running.store(true, Ordering::Release);
    }

    /// Tests that exercise post-connection behavior must explicitly establish
    /// trust first. The production `add_server` path intentionally never
    /// derives trust from caller-controlled scope.
    async fn add_trusted(
        manager: &Arc<McpClientManager>,
        approval: &Arc<RecordingApproval>,
        config: McpServerConfig,
    ) -> Arc<McpServerConnection> {
        let mock = config.transport == McpTransportType::Sdk;
        approval.record_approval(&config, "TEST");
        let connection = manager
            .add_server(config)
            .await
            .expect("trusted test server");
        // Unsupported transports fail in production. These unit cases inject
        // a connected fixture explicitly to exercise post-handshake behavior.
        if mock {
            connection.set_status(McpConnectionStatus::Connected);
        }
        connection
    }

    fn tool_def(name: &str) -> ToolDefinition {
        ToolDefinition {
            name: name.to_owned(),
            description: format!("{name} description"),
            input_schema: json!({"type": "object"}),
        }
    }

    /// 虚拟时钟下的条件等待（`start_paused` 时 `sleep` 会自动推进时钟）。
    async fn wait_until(mut predicate: impl FnMut() -> bool) -> bool {
        for _ in 0..200 {
            if predicate() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        predicate()
    }

    // ===== 退避与前缀 =====

    #[test]
    fn backoff_matches_java_golden_values() {
        assert_eq!(calculate_backoff(1), 1_000);
        assert_eq!(calculate_backoff(2), 2_000);
        assert_eq!(calculate_backoff(3), 4_000);
        assert_eq!(calculate_backoff(4), 8_000);
        assert_eq!(calculate_backoff(5), 16_000);
        assert_eq!(calculate_backoff(6), MAX_BACKOFF_MS);
        assert_eq!(calculate_backoff(64), MAX_BACKOFF_MS);
        // Java 的 `1L << (attempt - 1)` 在 attempt == 0 时未定义，Rust 归一到 1 次。
        assert_eq!(calculate_backoff(0), INITIAL_BACKOFF_MS);
    }

    #[test]
    fn backoff_with_jitter_stays_within_java_band() {
        for attempt in 1..=6 {
            let base = calculate_backoff(attempt);
            for _ in 0..64 {
                let jittered = calculate_backoff_with_jitter(attempt);
                assert!(jittered >= INITIAL_BACKOFF_MS, "jittered={jittered}");
                assert!(jittered <= base + base / 4 + 1, "jittered={jittered}");
            }
        }
    }

    #[test]
    fn tool_prefix_matches_java_concatenation() {
        assert_eq!(tool_prefix("github"), "mcp__github__");
        assert_eq!(PHASE, 2);
        assert_eq!(MAX_RECONNECT_ATTEMPTS, 5);
    }

    #[test]
    fn channel_permissions_block_specific_tool_and_wildcard() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let mut permissions = BTreeMap::new();
        permissions.insert("alpha".to_owned(), vec!["dangerous".to_owned()]);
        permissions.insert("beta".to_owned(), vec!["*".to_owned()]);
        let manager = builder(&approval, &sink)
            .channel_permissions(permissions)
            .build();

        assert!(manager.is_tool_allowed("alpha", "safe"));
        assert!(!manager.is_tool_allowed("alpha", "dangerous"));
        assert!(!manager.is_tool_allowed("beta", "anything"));
        assert!(manager.is_tool_allowed("gamma", "anything"));

        // 空表 → 全放行（对照 `permissions == null || isEmpty()`）。
        let open = builder(&approval, &sink).build();
        assert!(open.is_tool_allowed("beta", "anything"));
    }

    // ===== 连接生命周期 =====

    #[tokio::test]
    async fn add_server_requires_running() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        let Err(error) = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::Local,
            ))
            .await
        else {
            panic!("未启动时必须拒绝")
        };
        assert_eq!(error, ManagerError::NotRunning);
        assert_eq!(error.to_string(), "MCP_CLIENT_MANAGER_NOT_RUNNING");
    }

    #[tokio::test]
    async fn ambiguous_server_names_are_rejected_before_replacing_the_directory() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        manager
            .add_server(config_with(
                "alpha beta",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .unwrap();
        assert!(
            manager
                .add_server(config_with(
                    "alpha_beta",
                    McpTransportType::Sdk,
                    McpConfigScope::User
                ))
                .await
                .is_err()
        );
        assert_eq!(manager.connection_count(), 1);
        assert!(manager.get_connection("alpha beta").is_some());
        assert!(!manager.service_configs().contains_key("alpha_beta"));
    }

    #[tokio::test]
    async fn add_server_never_trusts_caller_claimed_config_file_scope() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);

        let connection = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("untrusted config is retained for approval");
        assert_eq!(connection.status(), McpConnectionStatus::NeedsAuth);
        assert!(approval.sources_for("srv").is_empty());
        assert_eq!(manager.connection_count(), 1);
    }

    #[tokio::test]
    async fn add_server_keeps_dynamic_scope_pending_approval() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);

        let connection = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::Dynamic,
            ))
            .await
            .expect("未信任也要落连接对象");
        assert_eq!(connection.status(), McpConnectionStatus::NeedsAuth);
        assert!(approval.sources_for("srv").is_empty());
    }

    #[tokio::test]
    async fn install_connection_rejects_stale_generation() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);

        let connection = manager.new_connection(config_with(
            "srv",
            McpTransportType::Sdk,
            McpConfigScope::Local,
        ));
        let generation = manager.next_generation("srv");
        manager.next_generation("srv");

        let error = manager
            .install_connection("srv", generation, &connection)
            .await
            .expect_err("代际已推进必须拒绝安装");
        assert_eq!(error, ManagerError::LifecycleChanged);
        assert_eq!(connection.status(), McpConnectionStatus::Disabled);
        assert_eq!(manager.connection_count(), 0);
    }

    #[tokio::test]
    async fn explicit_runtime_delete_wins_before_managed_connect_can_spawn() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let marker =
            std::env::temp_dir().join(format!("zkmcp-no-late-start-{}", uuid::Uuid::new_v4()));
        let mut config =
            McpServerConfig::stdio("srv", "/usr/bin/touch", vec![marker.display().to_string()]);
        config.scope = McpConfigScope::Dynamic;
        // Stage an owner before its trusted startup is allowed to run.
        let connection = manager.add_server(config.clone()).await.unwrap();
        approval.record_approval(&config, "TEST");
        let generation = manager.generation_of("srv");
        let cancel = lock(&manager.services.cancellations)
            .get("srv")
            .unwrap()
            .clone();
        manager.delete_server("srv").await.unwrap();
        connection
            .connect_if(|| manager.is_current_connection("srv", &connection, generation))
            .await;
        assert_eq!(
            manager
                .connect_managed("srv", generation, &connection, &cancel)
                .await,
            Err(ManagerError::LifecycleChanged)
        );
        let spawned = marker.exists();
        if spawned {
            std::fs::remove_file(&marker).unwrap();
        }
        assert!(
            !spawned,
            "a startup admitted before deletion spawned after deletion completed"
        );
        assert_eq!(connection.status(), McpConnectionStatus::Disabled);
        assert!(manager.get_connection("srv").is_none());
    }

    #[tokio::test]
    async fn managed_old_cleanup_does_not_close_a_newer_restart_of_the_same_connection() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::Dynamic,
            ))
            .await
            .unwrap();
        let old_generation = manager.generation_of("srv");
        manager.restart_server("srv").await.unwrap();
        connection.set_transport_for_test(Arc::new(StubTransport {
            connected: true,
            ..StubTransport::default()
        }));
        connection.set_status(McpConnectionStatus::Connected);
        manager
            .close_owned_connection("srv", old_generation, &connection)
            .await;
        assert!(Arc::ptr_eq(
            &manager.get_connection("srv").unwrap(),
            &connection
        ));
        assert_eq!(connection.status(), McpConnectionStatus::Connected);
        assert!(connection.is_alive());
    }

    #[tokio::test]
    async fn credential_refresh_retains_configuration_and_allows_a_fresh_start_token() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let registry = Arc::new(McpCapabilityRegistry::new(
            std::env::temp_dir().join(format!("zkmcp-refresh-empty-{}.json", uuid::Uuid::new_v4())),
        ));
        let manager = builder(&approval, &sink).registry(registry).build();
        running(&manager);
        let config = config_with("registry", McpTransportType::Sdk, McpConfigScope::Dynamic);
        manager.add_server_from(config.clone(), true).await.unwrap();
        lock(&manager.registry_owned_servers).insert("registry".into());
        let old_cancel = lock(&manager.services.cancellations)
            .get("registry")
            .unwrap()
            .clone();
        manager.refresh_registry_credentials().await;
        assert!(old_cancel.is_cancelled());
        assert!(lock(&manager.services.configs).contains_key("registry"));
        manager.add_server_from(config, true).await.unwrap();
        assert!(
            !lock(&manager.services.cancellations)
                .get("registry")
                .unwrap()
                .is_cancelled()
        );
        assert!(manager.get_connection("registry").is_some());
    }

    #[tokio::test]
    async fn explicit_runtime_delete_owns_and_cancels_the_first_trusted_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(tokio::sync::Notify::new());
        let signal = accepted.clone();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            signal.notify_one();
            std::future::pending::<()>().await;
        });
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let mut config = McpServerConfig::sse("slow runtime", format!("http://{addr}/mcp"));
        config.scope = McpConfigScope::Dynamic;
        approval.record_approval(&config, "TEST");
        let connecting_manager = manager.clone();
        let mut connect = tokio::spawn(async move { connecting_manager.add_server(config).await });
        tokio::time::timeout(Duration::from_secs(3), accepted.notified())
            .await
            .unwrap();
        let owner = manager.get_connection("slow runtime");
        let deleted = tokio::time::timeout(
            Duration::from_secs(3),
            manager.delete_server("slow runtime"),
        )
        .await;
        let stopped = tokio::time::timeout(Duration::from_secs(1), &mut connect).await;
        // Both tasks belong only to this test, including the red-test cleanup path.
        server.abort();
        connect.abort();
        assert!(
            owner.is_some(),
            "a trusted handshake must already have a cleanup owner"
        );
        assert!(deleted.unwrap().is_ok());
        assert!(matches!(
            stopped.unwrap().unwrap(),
            Err(ManagerError::LifecycleChanged)
        ));
        assert!(owner.unwrap().cleanup_confirmed());
        assert!(manager.get_connection("slow runtime").is_none());
        manager
            .add_server(config_with(
                "slow_runtime",
                McpTransportType::Sdk,
                McpConfigScope::Dynamic,
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn explicit_runtime_delete_releases_normalized_namespace() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        manager
            .add_server(config_with(
                "alpha beta",
                McpTransportType::Sdk,
                McpConfigScope::Dynamic,
            ))
            .await
            .unwrap();

        manager.delete_server("alpha beta").await.unwrap();
        assert!(manager.get_connection("alpha beta").is_none());
        manager
            .add_server(config_with(
                "alpha_beta",
                McpTransportType::Sdk,
                McpConfigScope::Dynamic,
            ))
            .await
            .expect("explicit deletion must release the old namespace");
        assert!(!manager.service_configs().contains_key("alpha beta"));
    }

    #[tokio::test]
    async fn explicit_runtime_delete_keeps_namespace_until_cleanup_confirmed() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = manager
            .add_server(config_with(
                "alpha beta",
                McpTransportType::Sdk,
                McpConfigScope::Dynamic,
            ))
            .await
            .unwrap();
        let cleanup = Arc::new(AtomicBool::new(false));
        connection.set_transport_for_test(Arc::new(StubTransport {
            cleanup: Some(cleanup.clone()),
            ..StubTransport::default()
        }));
        assert_eq!(
            manager.delete_server("alpha beta").await,
            Err(ManagerError::CleanupPending("alpha beta".into()))
        );
        assert!(Arc::ptr_eq(
            &manager.get_connection("alpha beta").unwrap(),
            &connection
        ));
        assert!(matches!(
            manager
                .add_server(config_with(
                    "alpha_beta",
                    McpTransportType::Sdk,
                    McpConfigScope::Dynamic,
                ))
                .await,
            Err(ManagerError::ToolNamespaceCollision(_))
        ));
        cleanup.store(true, Ordering::Release);
        manager.delete_server("alpha beta").await.unwrap();
        manager
            .add_server(config_with(
                "alpha_beta",
                McpTransportType::Sdk,
                McpConfigScope::Dynamic,
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn explicit_runtime_delete_does_not_remove_new_generation() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let old_config = config_with("srv", McpTransportType::Sdk, McpConfigScope::Dynamic);
        let connection = manager.add_server(old_config.clone()).await.unwrap();
        let gate = Arc::new(Semaphore::new(0));
        connection.set_transport_for_test(Arc::new(StubTransport {
            close_gate: Some(gate.clone()),
            ..StubTransport::default()
        }));
        let mut deleting = Box::pin(manager.delete_server("srv"));
        assert!(futures::poll!(&mut deleting).is_pending());
        let mut new_config = old_config;
        new_config.args.push("new configuration".into());
        let mut replacing = Box::pin(manager.add_server(new_config.clone()));
        assert!(futures::poll!(&mut replacing).is_pending());
        gate.add_permits(2);
        assert_eq!(deleting.await, Err(ManagerError::LifecycleChanged));
        let replacement = replacing.await.unwrap();
        assert!(Arc::ptr_eq(
            &manager.get_connection("srv").unwrap(),
            &replacement
        ));
        assert_eq!(manager.service_configs().get("srv"), Some(&new_config));
    }

    #[tokio::test]
    async fn explicit_runtime_delete_cancellation_retains_configuration_for_retry() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::Dynamic,
            ))
            .await
            .unwrap();
        let gate = Arc::new(Semaphore::new(0));
        connection.set_transport_for_test(Arc::new(StubTransport {
            close_gate: Some(gate.clone()),
            ..StubTransport::default()
        }));
        let mut deleting = Box::pin(manager.delete_server("srv"));
        assert!(futures::poll!(&mut deleting).is_pending());
        drop(deleting);
        assert!(manager.service_configs().contains_key("srv"));
        assert!(manager.get_connection("srv").is_some());
        gate.add_permits(1);
        manager.delete_server("srv").await.unwrap();
        assert!(!manager.service_configs().contains_key("srv"));
    }

    #[tokio::test]
    async fn explicit_runtime_delete_releases_disconnected_configuration_only() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        for (name, scope, registry) in [
            ("runtime", McpConfigScope::Dynamic, false),
            ("file", McpConfigScope::User, false),
            ("registry", McpConfigScope::Dynamic, true),
        ] {
            manager
                .add_server_from(config_with(name, McpTransportType::Sdk, scope), registry)
                .await
                .unwrap();
            // Credential refresh and OAuth logout use this retaining disconnect path.
            manager.remove_server(name).await;
            assert!(lock(&manager.services.configs).contains_key(name));
            manager.delete_server(name).await.unwrap();
            assert_eq!(
                lock(&manager.services.configs).contains_key(name),
                name != "runtime"
            );
        }
        manager.delete_server("missing").await.unwrap();
    }

    #[tokio::test]
    async fn remove_server_unregisters_tools_and_closes() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;

        assert!(manager.remove_server("srv").await);
        assert_eq!(sink.unregistered(), vec!["mcp__srv__".to_owned()]);
        assert_eq!(connection.status(), McpConnectionStatus::Disabled);
        assert_eq!(manager.connection_count(), 0);
        assert!(!manager.remove_server("srv").await);
    }

    #[tokio::test]
    async fn server_logs_reports_status_or_not_found() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;
        connection.set_tools(vec![tool_def("alpha")]);

        assert_eq!(
            manager.server_logs("srv", 100),
            vec![
                "Server: srv".to_owned(),
                "Status: CONNECTED".to_owned(),
                "Transport: SDK".to_owned(),
                "Tools: 1".to_owned(),
            ]
        );
        assert_eq!(
            manager.server_logs("ghost", 10),
            vec!["MCP server not found: ghost".to_owned()]
        );
    }

    #[tokio::test]
    async fn restart_server_validates_lifecycle_and_name() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        assert_eq!(
            manager.restart_server("srv").await,
            Err(ManagerError::NotRunning)
        );

        running(&manager);
        assert_eq!(
            manager.restart_server("srv").await,
            Err(ManagerError::ServerNotFound("srv".to_owned()))
        );

        manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");
        manager.restart_server("srv").await.expect("重启应成功");
        assert_eq!(sink.unregistered(), vec!["mcp__srv__".to_owned()]);
        assert_eq!(
            manager.get_connection("srv").map(|c| c.status()),
            Some(McpConnectionStatus::Failed)
        );
    }

    // ===== 工具注册 =====

    #[tokio::test]
    async fn colliding_final_tool_names_publish_no_ambiguous_adapter() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;
        connection.set_tools(vec![tool_def("read__file"), tool_def("read_file")]);
        manager.register_tools_from_connection(&connection);
        assert!(sink.registered().is_empty());
        assert!(manager.discover_and_wrap_tools().is_empty());
        assert_ne!(connection.status(), McpConnectionStatus::Connected);
    }

    #[tokio::test]
    async fn registers_prefixed_adapters_and_honours_permissions() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let mut permissions = BTreeMap::new();
        permissions.insert("srv".to_owned(), vec!["blocked".to_owned()]);
        let manager = builder(&approval, &sink)
            .channel_permissions(permissions)
            .build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;
        connection.set_tools(vec![tool_def("allowed"), tool_def("blocked")]);

        manager.register_tools_from_connection(&connection);
        assert_eq!(sink.registered(), vec!["mcp__srv__allowed".to_owned()]);
        assert_eq!(
            sink.published().last(),
            Some(&("srv".to_owned(), vec!["allowed".to_owned()]))
        );
    }

    #[tokio::test]
    async fn tools_changed_callback_refreshes_registration() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;

        connection.set_tools(vec![tool_def("first")]);
        manager.register_tools_from_connection(&connection);
        connection.set_tools(vec![tool_def("first"), tool_def("second")]);
        connection.notify_tools_changed();

        assert_eq!(sink.unregistered(), vec!["mcp__srv__".to_owned()]);
        assert_eq!(
            sink.registered(),
            vec![
                "mcp__srv__first".to_owned(),
                "mcp__srv__first".to_owned(),
                "mcp__srv__second".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn tools_changed_callback_ignores_stale_generation() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;
        connection.set_tools(vec![tool_def("first")]);
        manager.register_tools_from_connection(&connection);
        lock(&sink.registered).clear();

        // 移除服务器 → 代际推进 + 从 connections 摘除，旧回调必须变成空操作。
        manager.remove_server("srv").await;
        lock(&sink.unregistered).clear();
        connection.set_tools(vec![tool_def("first")]);
        connection.notify_tools_changed();

        assert!(sink.registered().is_empty());
        assert!(sink.unregistered().is_empty());
    }

    #[tokio::test]
    async fn reconnect_replaces_directory_and_does_not_keep_removed_tools() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;
        connection.set_tools(vec![tool_def("removed_after_reconnect")]);
        manager.register_tools_from_connection(&connection);
        lock(&sink.registered).clear();
        lock(&sink.unregistered).clear();

        let generation = manager.generation_of("srv");
        manager.reconnect_once("srv", &connection, generation).await;

        assert!(
            sink.registered().is_empty(),
            "a tool absent from the new session must not be re-registered"
        );
        assert!(
            sink.unregistered()
                .iter()
                .any(|prefix| prefix == "mcp__srv__"),
            "reconnect must replace, not merge, the dynamic directory"
        );
    }

    #[tokio::test]
    async fn discover_and_wrap_tools_skips_disconnected_servers() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let live = add_trusted(
            &manager,
            &approval,
            config_with("live", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;
        live.set_tools(vec![tool_def("alpha")]);
        let dead = add_trusted(
            &manager,
            &approval,
            config_with("dead", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;
        dead.set_tools(vec![tool_def("beta")]);
        dead.set_status(McpConnectionStatus::Failed);

        let names: Vec<String> = manager
            .discover_and_wrap_tools()
            .iter()
            .map(|tool| tool.name().to_owned())
            .collect();
        assert_eq!(names, vec!["mcp__live__alpha".to_owned()]);
    }

    #[tokio::test]
    async fn registry_overrides_description_and_timeout() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let registry = Arc::new(McpCapabilityRegistry::new(
            std::env::temp_dir().join("zkmcp-manager-registry-absent.json"),
        ));
        let mut definition = McpCapabilityDefinition::new("mcp_alpha");
        definition.tool_name = Some("alpha".to_owned());
        definition.url = Some("http://127.0.0.1:1/srv/sse".to_owned());
        definition.description = Some("registry description".to_owned());
        definition.timeout_ms = 7_000;
        definition.enabled = true;
        registry.add_capability(definition).expect("注册表条目写入");

        let manager = builder(&approval, &sink)
            .registry(Arc::clone(&registry))
            .build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;
        connection.set_tools(vec![tool_def("alpha")]);

        let tools = manager.discover_and_wrap_tools();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].description(), "registry description");
        assert_eq!(tools[0].timeout(), MCP_TOOL_MAX_EXECUTION);
    }

    // ===== 健康检查 =====

    #[tokio::test]
    async fn directory_owner_queued_prompt_keeps_creation_generation() {
        for change_manager_generation in [true, false] {
            let approval = RecordingApproval::shared();
            let sink = RecordingSink::shared();
            let manager = builder(&approval, &sink).build();
            running(&manager);
            let connection = add_trusted(
                &manager,
                &approval,
                config_with("srv", McpTransportType::Sdk, McpConfigScope::Dynamic),
            )
            .await;
            let observed = Arc::new(tokio::sync::Notify::new());
            connection.set_transport_for_test(Arc::new(StubTransport {
                connected: true,
                prompt_name: Some("old_prompt"),
                prompt_observed: Some(observed.clone()),
                ..StubTransport::default()
            }));
            manager.register_tools_from_connection(&connection);
            // Reserve a same-Arc restart before the queued prompt task first runs.
            // The old transport can remain usable while close waits for lifecycle.
            {
                let directory = lock(&manager.services.directory);
                if change_manager_generation {
                    manager.next_generation("srv");
                } else {
                    connection.set_transport_for_test(Arc::new(StubTransport {
                        connected: true,
                        prompt_name: Some("old_prompt"),
                        prompt_observed: Some(observed.clone()),
                        ..StubTransport::default()
                    }));
                }
                manager.clear_tool_directory_locked(&directory, "srv");
            }
            let registered = sink.registered();
            tokio::time::timeout(Duration::from_secs(3), observed.notified())
                .await
                .unwrap();
            assert_eq!(
                sink.registered(),
                registered,
                "old prompt work adopted the new generation"
            );
            manager.register_tools_from_connection(&connection);
            tokio::time::timeout(Duration::from_secs(3), observed.notified())
                .await
                .unwrap();
            assert!(
                sink.registered()
                    .contains(&"mcp__srv__prompt__old_prompt".to_owned())
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn directory_owner_replacement_clear_serializes_with_publication() {
        struct GateSink {
            live: Mutex<HashSet<String>>,
            published: Mutex<Vec<Vec<String>>>,
            armed: AtomicBool,
            reached: tokio::sync::Notify,
            release: (Mutex<bool>, std::sync::Condvar),
        }
        impl McpToolSink for GateSink {
            fn register_dynamic(&self, tool: Arc<dyn Tool>) {
                lock(&self.live).insert(tool.name().to_owned());
            }
            fn unregister_by_prefix(&self, prefix: &str) {
                if self.armed.swap(false, Ordering::AcqRel) {
                    self.reached.notify_one();
                    let released = lock(&self.release.0);
                    let (_released, _) = self
                        .release
                        .1
                        .wait_timeout_while(released, Duration::from_secs(5), |released| !*released)
                        .unwrap();
                }
                lock(&self.live).retain(|name| !name.starts_with(prefix));
            }
            fn publish_server_tools(&self, _: &str, tools: Vec<ToolDefinition>) {
                lock(&self.published).push(tools.into_iter().map(|tool| tool.name).collect());
            }
        }
        let sink = Arc::new(GateSink {
            live: Mutex::default(),
            published: Mutex::default(),
            armed: AtomicBool::new(false),
            reached: tokio::sync::Notify::new(),
            release: (Mutex::new(false), std::sync::Condvar::new()),
        });
        let approval = RecordingApproval::shared();
        let manager = McpClientManager::builder(approval.clone(), sink.clone())
            .resolver(McpConfigurationResolver::new(None))
            .build();
        running(&manager);
        let config = config_with("srv", McpTransportType::Sdk, McpConfigScope::Dynamic);
        manager.add_server(config.clone()).await.unwrap();
        sink.armed.store(true, Ordering::Release);
        let manager_a = manager.clone();
        let config_a = config.clone();
        let a = tokio::spawn(async move { manager_a.add_server(config_a).await });
        tokio::time::timeout(Duration::from_secs(3), sink.reached.notified())
            .await
            .unwrap();
        let held_during_clear = manager.services.directory.try_lock().is_err();
        let manager_b = manager.clone();
        let starting = Arc::new(tokio::sync::Notify::new());
        let started = starting.clone();
        let b = tokio::spawn(async move {
            started.notify_one();
            manager_b.add_server(config).await
        });
        tokio::time::timeout(Duration::from_secs(3), starting.notified())
            .await
            .unwrap();
        // Release A before awaiting B: the correct lock deliberately prevents
        // B from publishing while A is paused inside clear.
        *lock(&sink.release.0) = true;
        sink.release.1.notify_all();
        let replacement = tokio::time::timeout(Duration::from_secs(3), b)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        publish_connected_fixture(&manager, &replacement);
        let result_a = tokio::time::timeout(Duration::from_secs(3), a)
            .await
            .unwrap()
            .unwrap();
        assert!(result_a.is_ok() || matches!(result_a, Err(ManagerError::LifecycleChanged)));
        assert!(
            held_during_clear,
            "replacement clear must hold the publication lock"
        );
        assert!(Arc::ptr_eq(
            &manager.get_connection("srv").unwrap(),
            &replacement
        ));
        assert_eq!(replacement.status(), McpConnectionStatus::Connected);
        assert!(lock(&sink.live).contains("mcp__srv__new"));
        assert_eq!(
            lock(&sink.published).last().unwrap(),
            &vec!["new".to_owned()]
        );
    }

    fn publish_connected_fixture(
        manager: &Arc<McpClientManager>,
        connection: &Arc<McpServerConnection>,
    ) {
        connection.set_transport_for_test(Arc::new(StubTransport {
            connected: true,
            ping: true,
            ..StubTransport::default()
        }));
        connection.set_status(McpConnectionStatus::Connected);
        connection.set_tools(vec![tool_def("new")]);
        manager.register_tools_from_connection(connection);
    }

    #[tokio::test]
    async fn directory_owner_stale_schedule_and_completion_keep_new_directory() {
        for reuse_connection in [false, true] {
            let approval = RecordingApproval::shared();
            let sink = RecordingSink::shared();
            let manager = builder(&approval, &sink).build();
            running(&manager);
            let config = config_with("srv", McpTransportType::Sdk, McpConfigScope::Dynamic);
            let original = add_trusted(&manager, &approval, config.clone()).await;
            let generation = manager.generation_of("srv");
            let old_config = config.clone();
            let replacement = if reuse_connection {
                manager.restart_server("srv").await.unwrap();
                original.clone()
            } else {
                manager.add_server(config).await.unwrap()
            };
            publish_connected_fixture(&manager, &replacement);
            replacement.increment_reconnect_attempts();
            let published = sink.published();
            let cleared = sink.unregistered();
            if !reuse_connection {
                // A passive-health snapshot may outlive the old owner's close.
                original.set_status(McpConnectionStatus::Connected);
            }
            manager.check_passive_health(&original);
            {
                let directory = lock(&manager.services.directory);
                manager.schedule_reconnect_locked(&directory, "srv", &original, generation);
            }
            manager.finish_reconnect("srv", &original, generation);
            assert!(matches!(
                manager
                    .add_server_from_owner(old_config, false, Some((Some(&original), generation)))
                    .await,
                Err(ManagerError::LifecycleChanged)
            ));
            assert_eq!(replacement.status(), McpConnectionStatus::Connected);
            assert_eq!(replacement.reconnect_attempts(), 1);
            assert_eq!(sink.unregistered(), cleared);
            assert_eq!(sink.published(), published);
            assert!(lock(&manager.active_reconnects).is_empty());
        }
    }

    #[tokio::test]
    async fn directory_owner_stale_scheduler_keeps_new_tasks() {
        for reuse_connection in [false, true] {
            let approval = RecordingApproval::shared();
            let sink = RecordingSink::shared();
            let manager = builder(&approval, &sink).build();
            running(&manager);
            // Keep spawned workers queued while checking their exact task identities.
            let permits = manager.reconnect_permits.acquire_many(2).await.unwrap();
            let config = config_with("srv", McpTransportType::Sdk, McpConfigScope::Dynamic);
            let original = add_trusted(&manager, &approval, config.clone()).await;
            let old_generation = manager.generation_of("srv");
            {
                let directory = lock(&manager.services.directory);
                manager.schedule_delayed_reconnect_locked(
                    &directory,
                    "srv",
                    &original,
                    old_generation,
                );
            }
            let old_scheduled = lock(&manager.scheduled_reconnects)["srv"].task_id;
            let current = if reuse_connection {
                // Reuse the Arc while preserving the old queued entry so the
                // scheduler must compare its generation as well as its pointer.
                let _directory = lock(&manager.services.directory);
                manager.next_generation("srv");
                original.clone()
            } else {
                manager.add_server(config).await.unwrap()
            };
            let generation = manager.generation_of("srv");
            {
                let directory = lock(&manager.services.directory);
                manager.schedule_delayed_reconnect_locked(&directory, "srv", &current, generation);
                manager.submit_reconnect_locked(&directory, "srv", &current, generation);
            }
            let scheduled = lock(&manager.scheduled_reconnects)["srv"].task_id;
            assert_ne!(scheduled, old_scheduled);
            let active = lock(&manager.active_reconnects)["srv"].task_id;
            {
                let directory = lock(&manager.services.directory);
                manager.schedule_delayed_reconnect_locked(
                    &directory,
                    "srv",
                    &original,
                    old_generation,
                );
                manager.submit_reconnect_locked(&directory, "srv", &original, old_generation);
            }
            // Exercise the actual timer continuation after it wakes up stale.
            manager
                .spawn_delayed_reconnect(
                    "srv".into(),
                    original.clone(),
                    old_generation,
                    Duration::ZERO,
                    old_scheduled,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            manager
                .attempt_reconnect("srv", &original, old_generation)
                .await;
            assert_eq!(
                lock(&manager.scheduled_reconnects)["srv"].task_id,
                scheduled
            );
            assert_eq!(lock(&manager.active_reconnects)["srv"].task_id, active);
            assert!(
                !lock(&manager.active_reconnects)["srv"]
                    .cancel
                    .is_cancelled()
            );
            assert!(lock(&manager.reconnecting_servers).is_empty());
            manager.cancel_reconnect_work("srv");
            drop(permits);
        }
    }

    #[tokio::test]
    async fn directory_owner_old_worker_cannot_remove_same_arc_new_marker() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let gate = Arc::new(Semaphore::new(0));
        let oauth = OAuthCoordinator::new(
            Arc::new(DelayedOAuthBindings {
                calls: AtomicU64::new(0),
                delay_at: 0,
                gate: gate.clone(),
            }),
            Arc::new(RejectingOAuthSecrets),
        );
        let manager = builder(&approval, &sink).oauth(oauth).build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::Dynamic),
        )
        .await;
        let old_generation = manager.generation_of("srv");
        let mut old = Box::pin(manager.attempt_reconnect("srv", &connection, old_generation));
        assert!(futures::poll!(&mut old).is_pending());
        let generation = {
            let _directory = lock(&manager.services.directory);
            manager.next_generation("srv")
        };
        let mut new = Box::pin(manager.attempt_reconnect("srv", &connection, generation));
        assert!(futures::poll!(&mut new).is_pending());
        assert_eq!(lock(&manager.reconnecting_servers)["srv"].0, generation);
        gate.add_permits(1);
        tokio::time::timeout(Duration::from_secs(3), old)
            .await
            .unwrap();
        assert_eq!(lock(&manager.reconnecting_servers)["srv"].0, generation);
        gate.add_permits(2);
        tokio::time::timeout(Duration::from_secs(3), new)
            .await
            .unwrap();
        assert!(lock(&manager.reconnecting_servers).is_empty());
    }

    #[tokio::test]
    async fn directory_owner_late_consent_cannot_replace_or_approve_new_owner() {
        for (has_owner, same_generation) in [(true, false), (false, false), (false, true)] {
            let approval = RecordingApproval::shared();
            let sink = RecordingSink::shared();
            let manager = builder(&approval, &sink).build();
            running(&manager);
            let config = config_with("srv", McpTransportType::Sdk, McpConfigScope::Dynamic);
            manager.add_server(config.clone()).await.unwrap();
            if !has_owner {
                manager.remove_server("srv").await;
            }
            let connection = manager.get_connection("srv");
            let generation = manager.generation_of("srv");
            let cancel = lock(&manager.services.cancellations)
                .entry("srv".into())
                .or_default()
                .clone();
            let (complete, completed) = tokio::sync::oneshot::channel();
            let callback = manager.spawn_oauth_reconnect(
                config.clone(),
                false,
                connection,
                generation,
                cancel,
                completed,
            );
            let replacement = if same_generation {
                // An initial add can install an owner in the previously empty
                // slot without advancing its already reserved generation.
                let replacement = manager.new_connection(config);
                manager
                    .install_connection("srv", generation, &replacement)
                    .await
                    .unwrap();
                replacement
            } else {
                manager.add_server(config).await.unwrap()
            };
            publish_connected_fixture(&manager, &replacement);
            let current_generation = manager.generation_of("srv");
            let published = sink.published();
            let cleared = sink.unregistered();
            complete.send(Ok(())).unwrap();
            callback.await.unwrap();
            assert!(approval.sources_for("srv").is_empty());
            assert_eq!(manager.generation_of("srv"), current_generation);
            assert!(Arc::ptr_eq(
                &manager.get_connection("srv").unwrap(),
                &replacement
            ));
            assert_eq!(sink.published(), published);
            assert_eq!(sink.unregistered(), cleared);
        }
    }

    #[tokio::test]
    async fn directory_owner_current_consent_connects_present_and_empty_slots() {
        for has_owner in [false, true] {
            let approval = RecordingApproval::shared();
            let sink = RecordingSink::shared();
            let manager = builder(&approval, &sink).build();
            running(&manager);
            let config = config_with("srv", McpTransportType::Sdk, McpConfigScope::Dynamic);
            manager.add_server(config.clone()).await.unwrap();
            if !has_owner {
                manager.remove_server("srv").await;
            }
            let owner = manager.get_connection("srv");
            let generation = manager.generation_of("srv");
            let cancel = lock(&manager.services.cancellations)
                .entry("srv".into())
                .or_default()
                .clone();
            let (complete, completed) = tokio::sync::oneshot::channel();
            let callback =
                manager.spawn_oauth_reconnect(config, false, owner, generation, cancel, completed);
            complete.send(Ok(())).unwrap();
            callback.await.unwrap();
            assert_eq!(approval.sources_for("srv"), vec!["OAUTH_USER"]);
            assert_eq!(manager.generation_of("srv"), generation + 1);
            // SDK intentionally has no physical transport. Reaching Failed
            // rather than NeedsAuth proves consent reached the trusted add path.
            assert_eq!(
                manager.get_connection("srv").unwrap().status(),
                McpConnectionStatus::Failed
            );
        }
    }

    use crate::oauth::storage::{
        OAuthBinding, OAuthBindingState, OAuthBindingStore, OAuthSecretStore, OAuthSecrets,
    };
    use crate::oauth::{OAuthCoordinator, OAuthError};

    struct DelayedOAuthBindings {
        calls: AtomicU64,
        delay_at: u64,
        gate: Arc<Semaphore>,
    }
    impl OAuthBindingStore for DelayedOAuthBindings {
        fn load<'a>(
            &'a self,
            _: &'a str,
        ) -> BoxFuture<'a, Result<Option<OAuthBinding>, OAuthError>> {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::AcqRel) + 1 == self.delay_at
                    || self.delay_at == 0
                {
                    self.gate.acquire().await.unwrap().forget();
                }
                Ok(Some(OAuthBinding {
                    state: OAuthBindingState::Active,
                    resource: "https://example.invalid/mcp".into(),
                    issuer: "https://example.invalid".into(),
                    token_endpoint: "https://example.invalid/token".into(),
                    revocation_endpoint: None,
                    client_id: "test".into(),
                    credential_ref: "test".into(),
                    scope: None,
                    expires_at: None,
                }))
            })
        }
        fn save<'a>(
            &'a self,
            _: &'a str,
            _: Option<OAuthBinding>,
        ) -> BoxFuture<'a, Result<(), OAuthError>> {
            Box::pin(async { panic!("directory test must not write OAuth storage") })
        }
    }
    struct RejectingOAuthSecrets;
    impl OAuthSecretStore for RejectingOAuthSecrets {
        fn load<'a>(
            &'a self,
            _: &'a str,
        ) -> BoxFuture<'a, Result<Option<OAuthSecrets>, OAuthError>> {
            Box::pin(async { panic!("directory test must not read credentials") })
        }
        fn save<'a>(
            &'a self,
            _: &'a str,
            _: OAuthSecrets,
        ) -> BoxFuture<'a, Result<(), OAuthError>> {
            Box::pin(async { panic!("directory test must not write credentials") })
        }
        fn delete<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<(), OAuthError>> {
            Box::pin(async { panic!("directory test must not delete credentials") })
        }
    }

    #[tokio::test]
    async fn directory_owner_delayed_oauth_cannot_restart_replacement() {
        // Pause the explicit restart lookup, and both lookups along automatic
        // OAuth reconnect. No network or OS credential store is involved.
        for (automatic, delay_at) in [(false, 1), (true, 1), (true, 2)] {
            let gate = Arc::new(Semaphore::new(0));
            let approval = RecordingApproval::shared();
            let sink = RecordingSink::shared();
            let oauth = OAuthCoordinator::new(
                Arc::new(DelayedOAuthBindings {
                    calls: AtomicU64::new(0),
                    delay_at,
                    gate: gate.clone(),
                }),
                Arc::new(RejectingOAuthSecrets),
            );
            let manager = builder(&approval, &sink).oauth(oauth).build();
            running(&manager);
            let config = config_with("srv", McpTransportType::Sdk, McpConfigScope::Dynamic);
            let original = add_trusted(&manager, &approval, config.clone()).await;
            let generation = manager.generation_of("srv");
            let mut restarting = Box::pin(async {
                if automatic {
                    manager.reconnect_once("srv", &original, generation).await;
                    Ok(())
                } else {
                    manager.restart_server("srv").await
                }
            });
            assert!(futures::poll!(&mut restarting).is_pending());
            let replacement = manager.add_server(config).await.unwrap();
            publish_connected_fixture(&manager, &replacement);
            let new_generation = manager.generation_of("srv");
            let published = sink.published();
            let cleared = sink.unregistered();
            gate.add_permits(1);
            let outcome = restarting.await;
            if !automatic {
                assert_eq!(outcome, Err(ManagerError::LifecycleChanged));
            }
            assert_eq!(manager.generation_of("srv"), new_generation);
            assert!(Arc::ptr_eq(
                &manager.get_connection("srv").unwrap(),
                &replacement
            ));
            assert_eq!(replacement.status(), McpConnectionStatus::Connected);
            assert_eq!(sink.unregistered(), cleared);
            assert_eq!(sink.published(), published);
        }
    }

    #[tokio::test]
    async fn directory_owner_stale_ping_cannot_clear_replacement() {
        for replacement_kind in [0, 1, 2] {
            let approval = RecordingApproval::shared();
            let sink = RecordingSink::shared();
            let manager = builder(&approval, &sink).build();
            running(&manager);
            let config = config_with("srv", McpTransportType::Sdk, McpConfigScope::Dynamic);
            let original = add_trusted(&manager, &approval, config.clone()).await;
            let gate = Arc::new(Semaphore::new(0));
            original.set_transport_for_test(Arc::new(StubTransport {
                connected: true,
                ping_gate: Some(gate.clone()),
                ..StubTransport::default()
            }));
            lock(&manager.consecutive_failures).insert("srv".into(), 1);
            let mut checking = Box::pin(manager.health_check());
            assert!(futures::poll!(&mut checking).is_pending());
            let replacement = match replacement_kind {
                0 => manager.add_server(config).await.unwrap(),
                1 => {
                    manager.restart_server("srv").await.unwrap();
                    original.clone()
                }
                // Automatic reconnect changes the transport session without
                // replacing this Arc or advancing the manager generation.
                _ => original.clone(),
            };
            replacement.set_transport_for_test(Arc::new(StubTransport {
                connected: true,
                ping: true,
                ..StubTransport::default()
            }));
            replacement.set_status(McpConnectionStatus::Connected);
            replacement.set_tools(vec![tool_def("new")]);
            manager.register_tools_from_connection(&replacement);
            let published = sink.published();
            let cleared = sink.unregistered();
            gate.add_permits(1);
            checking.await;
            manager.cancel_reconnect_work("srv");
            assert_eq!(replacement.status(), McpConnectionStatus::Connected);
            assert_eq!(sink.unregistered(), cleared);
            assert_eq!(sink.published(), published);
            assert_eq!(manager.consecutive_failures("srv"), 1);
        }
    }

    #[tokio::test]
    async fn health_check_marks_dead_connection_failed_and_schedules_reconnect() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;

        manager.health_check().await;

        assert_eq!(connection.status(), McpConnectionStatus::Failed);
        assert!(lock(&manager.scheduled_reconnects).contains_key("srv"));
        manager.cancel_reconnect_work("srv");
    }

    #[tokio::test]
    async fn health_check_never_reconnects_stdio() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let mut config = config_with("srv", McpTransportType::Stdio, McpConfigScope::Local);
        config.command = Some("/nonexistent/zkmcp-test-binary".to_owned());
        let connection = add_trusted(&manager, &approval, config).await;
        assert_eq!(connection.status(), McpConnectionStatus::Failed);

        manager.health_check().await;

        assert!(lock(&manager.scheduled_reconnects).is_empty());
    }

    #[tokio::test]
    async fn health_check_resets_counter_on_successful_ping() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;
        connection.set_transport_for_test(Arc::new(StubTransport {
            connected: true,
            ping: true,
            ..StubTransport::default()
        }));

        manager.health_check().await;

        assert_eq!(connection.status(), McpConnectionStatus::Connected);
        assert_eq!(manager.consecutive_failures("srv"), 0);
        assert!(manager.last_successful_ping("srv").is_some());
    }

    #[tokio::test]
    async fn health_check_degrades_after_two_failed_pings() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let observer = RecordingObserver::shared();
        let manager = builder(&approval, &sink)
            .health_observer(Arc::clone(&observer) as Arc<dyn McpHealthObserver>)
            .build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;
        connection.set_transport_for_test(Arc::new(StubTransport {
            connected: true,
            ping: false,
            ..StubTransport::default()
        }));

        manager.health_check().await;
        assert_eq!(manager.consecutive_failures("srv"), 1);
        assert!(!observer.saw("srv", McpConnectionStatus::Degraded));

        manager.health_check().await;
        assert_eq!(manager.consecutive_failures("srv"), 2);
        assert!(observer.saw("srv", McpConnectionStatus::Degraded));
        manager.cancel_reconnect_work("srv");
    }

    #[tokio::test]
    async fn reconnect_failed_skips_stdio_and_exhausted_attempts() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);

        let mut stdio = config_with("stdio-srv", McpTransportType::Stdio, McpConfigScope::Local);
        stdio.command = Some("/nonexistent/zkmcp-test-binary".to_owned());
        manager.add_server(stdio).await.expect("落连接对象");

        let exhausted = manager
            .add_server(config_with(
                "http-srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");
        exhausted.set_status(McpConnectionStatus::Failed);
        for _ in 0..MAX_RECONNECT_ATTEMPTS {
            exhausted.increment_reconnect_attempts();
        }

        manager.reconnect_failed();

        assert!(lock(&manager.scheduled_reconnects).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_reconnect_runs_after_backoff() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let observer = RecordingObserver::shared();
        let manager = builder(&approval, &sink)
            .health_observer(Arc::clone(&observer) as Arc<dyn McpHealthObserver>)
            .build();
        running(&manager);
        let connection = add_trusted(
            &manager,
            &approval,
            config_with("srv", McpTransportType::Sdk, McpConfigScope::User),
        )
        .await;

        // CONNECTED 但无传输 → is_alive() 为假 → FAILED → 调度延迟重连。
        manager.health_check().await;
        assert_eq!(connection.status(), McpConnectionStatus::Failed);

        // The scheduled attempt executes after backoff, but an unsupported
        // transport remains failed instead of fabricating a successful handshake.
        assert!(
            wait_until(|| connection.reconnect_attempts() > 0).await,
            "backoff must trigger a real reconnect attempt"
        );
        assert_eq!(connection.status(), McpConnectionStatus::Failed);
        assert!(!observer.saw("srv", McpConnectionStatus::Connected));
        assert!(
            wait_until(|| lock(&manager.scheduled_reconnects).is_empty()
                && lock(&manager.active_reconnects).is_empty())
            .await,
            "任务完成后必须摘除自身条目"
        );
    }

    #[tokio::test]
    async fn schedule_reconnect_broadcasts_degraded() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let observer = RecordingObserver::shared();
        let manager = builder(&approval, &sink)
            .health_observer(Arc::clone(&observer) as Arc<dyn McpHealthObserver>)
            .build();
        running(&manager);
        manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");

        manager.schedule_reconnect("srv");
        assert!(observer.saw("srv", McpConnectionStatus::Degraded));

        // 未知服务器静默返回（对照 `getConnection(...).ifPresent`）。
        manager.schedule_reconnect("ghost");
        assert!(!observer.saw("ghost", McpConnectionStatus::Degraded));
        manager.cancel_reconnect_work("srv");
    }

    // ===== 关闭 =====

    #[tokio::test]
    async fn shutdown_closes_connections_and_blocks_restart() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let connection = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");

        manager.stop().await;

        assert!(!manager.is_running());
        assert_eq!(manager.connection_count(), 0);
        assert_eq!(connection.status(), McpConnectionStatus::Disabled);
        assert_eq!(
            manager.start().await,
            Err(ManagerError::CannotRestartAfterShutdown)
        );
    }

    // ===== 配置来源与注册表 =====

    #[tokio::test]
    async fn static_configs_are_auto_trusted_as_application_config() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink)
            .static_configs(vec![config_with(
                "app-srv",
                McpTransportType::Sdk,
                McpConfigScope::Project,
            )])
            .build();
        running(&manager);

        manager.initialize_static_configs().await;

        assert_eq!(
            approval.sources_for("app-srv"),
            vec!["APPLICATION_CONFIG".to_owned()]
        );
        assert_eq!(
            manager.get_connection("app-srv").map(|c| c.status()),
            Some(McpConnectionStatus::Failed)
        );

        // 已存在同名连接时第二段不再处理（对照 `!connections.containsKey`）。
        manager.initialize_static_configs().await;
        assert_eq!(approval.sources_for("app-srv").len(), 1);
    }

    #[test]
    fn build_config_from_registry_maps_sse_url_and_authorization() {
        let mut definition = McpCapabilityDefinition::new("mcp_alpha");
        definition.url =
            Some("https://dashscope.aliyuncs.com/api/v1/mcps/registry-server/sse".to_owned());
        definition.api_key_config = Some(crate::security::DASHSCOPE_API_KEY_CONFIG.to_owned());

        let resolver = |provider: &str| {
            (provider == crate::security::DASHSCOPE_PROVIDER).then(|| "secret-key".to_owned())
        };
        let config = McpClientManager::build_config_from_registry_with(&definition, &resolver);

        assert_eq!(config.name, "registry-server");
        assert_eq!(config.transport, McpTransportType::Sse);
        assert_eq!(config.scope, McpConfigScope::Dynamic);
        assert_eq!(
            config.url.as_deref(),
            Some("https://dashscope.aliyuncs.com/api/v1/mcps/registry-server/sse")
        );
        assert_eq!(
            config.headers.get("Authorization").map(String::as_str),
            Some("Bearer secret-key")
        );
        assert!(config.command.is_none());
        assert!(config.args.is_empty());
    }

    #[test]
    fn build_config_from_registry_omits_authorization_without_key() {
        let mut definition = McpCapabilityDefinition::new("mcp_alpha");
        definition.url = Some("https://mcp.example.com/registry-server/sse".to_owned());

        let config = McpClientManager::build_config_from_registry(&definition);
        assert!(config.headers.is_empty());
    }

    #[test]
    fn injected_provider_key_becomes_the_next_registry_authorization_header() {
        let keys = Arc::new(Mutex::new(BTreeMap::<String, String>::new()));
        let resolver_keys = Arc::clone(&keys);
        let resolver: Arc<dyn McpCredentialResolver> =
            Arc::new(move |provider: &str| lock(&resolver_keys).get(provider).cloned());
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink)
            .credential_resolver(resolver)
            .build();
        let mut definition = McpCapabilityDefinition::new("mcp_search");
        definition.url =
            Some("https://dashscope.aliyuncs.com/api/v1/mcps/zhipu-websearch/sse".to_owned());
        definition.api_key_config = Some(crate::security::DASHSCOPE_API_KEY_CONFIG.to_owned());

        let before = manager.build_resolved_config_from_registry(&definition);
        assert!(!before.headers.contains_key("Authorization"));

        lock(&keys).insert(
            crate::security::DASHSCOPE_TOKEN_PLAN_PROVIDER.to_owned(),
            "new-subscription-key".to_owned(),
        );
        let after = manager.build_resolved_config_from_registry(&definition);
        assert_eq!(
            after.headers.get("Authorization").map(String::as_str),
            Some("Bearer new-subscription-key")
        );
    }

    #[tokio::test]
    async fn credential_refresh_never_removes_same_named_manual_server() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let registry = Arc::new(McpCapabilityRegistry::new(
            std::env::temp_dir().join("zkmcp-refresh-manual-preserved.json"),
        ));
        let mut definition = McpCapabilityDefinition::new("mcp_search");
        definition.server_key = Some("shared-name".to_owned());
        definition.url =
            Some("https://dashscope.aliyuncs.com/api/v1/mcps/zhipu-websearch/sse".to_owned());
        definition.api_key_config = Some(crate::security::DASHSCOPE_API_KEY_CONFIG.to_owned());
        definition.enabled = true;
        registry.add_capability(definition).expect("add capability");
        let manager = builder(&approval, &sink)
            .registry(registry)
            .credential_resolver(Arc::new(|_: &str| None))
            .build();
        running(&manager);

        let manual = config_with("shared-name", McpTransportType::Sdk, McpConfigScope::Local);
        let connection = manager.add_server(manual).await.expect("manual server");
        manager.refresh_registry_credentials().await;

        let preserved = manager
            .get_connection("shared-name")
            .expect("manual server must remain");
        assert!(Arc::ptr_eq(&connection, &preserved));
        assert!(lock(&manager.registry_owned_servers).is_empty());
    }

    #[tokio::test]
    async fn enable_from_registry_rejects_loopback_before_connect_or_approval() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let mut definition = McpCapabilityDefinition::new("mcp_alpha");
        // 127.0.0.1:1 立即 ECONNREFUSED —— 不产生真实外网访问。
        definition.url = Some("http://127.0.0.1:1/registry-server/sse".to_owned());
        definition.enabled = true;

        let Err(error) = manager.enable_from_registry(&definition).await else {
            panic!("loopback capability must be rejected");
        };

        assert!(matches!(error, ManagerError::UnsafeCapabilityEndpoint(_)));
        assert!(manager.get_connection("registry-server").is_none());
        assert!(approval.sources_for("registry-server").is_empty());
    }

    // ===== Roots / prompts =====

    #[tokio::test]
    async fn workspace_change_updates_roots() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let roots = Arc::new(RootsProvider::new());
        let manager = builder(&approval, &sink)
            .roots_provider(Arc::clone(&roots))
            .build();
        running(&manager);
        manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");

        manager
            .on_workspace_changed("/tmp/zkmcp-project", "zkmcp-project")
            .await;

        let current = roots.current_roots();
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].name, "zkmcp-project");
        assert!(Arc::ptr_eq(&manager.roots_provider(), &roots));
    }

    #[tokio::test]
    async fn default_roots_initialise_from_working_directory() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let roots = Arc::new(RootsProvider::new());
        let manager = builder(&approval, &sink)
            .roots_provider(Arc::clone(&roots))
            .build();

        manager.initialize_default_roots();

        assert_eq!(roots.current_roots().len(), 1);
    }

    #[tokio::test]
    async fn discover_prompts_is_empty_without_transport() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");

        assert!(manager.discover_prompts().await.is_empty());
    }

    // ===== 对外工具名与注册表 allowlist（v1.1）=====

    #[test]
    fn java_string_hashcode_matches_jdk_golden_values() {
        // 黄金值来自 JDK 21 `String.hashCode()` 实测对拍。
        assert_eq!(java_string_hashcode(""), 0);
        assert_eq!(java_string_hashcode("hello"), 99_162_322);
        assert_eq!(java_string_hashcode("zkcode-mcp"), 801_921_003);
        assert_eq!(java_string_hashcode("图像编辑"), 684_371_084);
        // 32 位有符号回绕：该串的 hashCode 恰为 Integer.MIN_VALUE。
        assert_eq!(java_string_hashcode("polygenelubricants"), i32::MIN);
        assert_eq!(short_name_hash("hello"), "05e918d2");
        assert_eq!(short_name_hash(""), "00000000");
        assert_eq!(short_name_hash("polygenelubricants"), "80000000");
    }

    #[test]
    fn external_tool_name_sanitizes_and_falls_back() {
        // 合法名原样拼接。
        assert_eq!(
            build_external_tool_name("srv", "tool", None),
            "mcp__srv__tool"
        );
        // 服务器名非法字符替换为 `_`（对照 Java 实测：尾随 `_` 与分隔符相邻）。
        assert_eq!(
            build_external_tool_name("my server!", "tool", None),
            "mcp__my_server___tool"
        );
        // 非法工具名回落注册表条目 id（剥 `mcp_` 前缀）。
        let capability = McpCapabilityDefinition::new("mcp_image_edit");
        assert_eq!(
            build_external_tool_name("srv", "图像编辑", Some(&capability)),
            "mcp__srv__image_edit"
        );
        // 无注册表条目时回落 `tool_<hash>`（hash 为原始名的 Java hashCode）。
        assert_eq!(
            build_external_tool_name("srv", "图像编辑", None),
            "mcp__srv__tool_28caac8c"
        );
    }

    #[test]
    fn external_tool_name_truncates_with_hash_suffix() {
        let long_tool = "a".repeat(60);
        let renamed = build_external_tool_name("dashscope-media", &long_tool, None);
        assert_eq!(renamed.len(), MAX_EXTERNAL_TOOL_NAME_LEN);
        // 黄金值来自 JDK 21 对拍：候选名 82 字符 → 截断 55 + `_` + 8 位 hash。
        assert_eq!(
            renamed,
            format!("mcp__dashscope-media__{}_8c74417f", "a".repeat(33))
        );
    }

    #[test]
    fn tool_prefix_sanitizes_server_name() {
        assert_eq!(tool_prefix("my server!"), "mcp__my_server___");
    }

    #[tokio::test]
    async fn registry_allowlist_blocks_unlisted_tools() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let registry = Arc::new(McpCapabilityRegistry::new(
            std::env::temp_dir().join("zkmcp-manager-allowlist-absent.json"),
        ));
        let mut listed = McpCapabilityDefinition::new("mcp_alpha");
        listed.tool_name = Some("alpha".to_owned());
        listed.server_key = Some("srv".to_owned());
        listed.enabled = true;
        registry.add_capability(listed).expect("注册表条目写入");
        let mut disabled = McpCapabilityDefinition::new("mcp_beta");
        disabled.tool_name = Some("beta".to_owned());
        disabled.server_key = Some("srv".to_owned());
        registry.add_capability(disabled).expect("注册表条目写入");

        let manager = builder(&approval, &sink)
            .registry(Arc::clone(&registry))
            .build();
        // 注册表管理的服务器：仅放行已启用条目。
        assert!(manager.is_tool_allowed("srv", "alpha"));
        assert!(!manager.is_tool_allowed("srv", "beta"));
        assert!(!manager.is_tool_allowed("srv", "gamma"));
        // 非注册表管理的服务器不受 allowlist 影响。
        assert!(manager.is_tool_allowed("other", "anything"));
    }

    #[tokio::test]
    async fn registry_activation_skips_unconfigured_servers() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let registry = Arc::new(McpCapabilityRegistry::new(
            std::env::temp_dir().join("zkmcp-manager-skip-absent.json"),
        ));
        // 无端点 URL → 跳过。
        let mut no_url = McpCapabilityDefinition::new("mcp_no_url");
        no_url.server_key = Some("no-url-srv".to_owned());
        no_url.enabled = true;
        registry.add_capability(no_url).expect("注册表条目写入");
        // 声明了 API Key 需求但未配置（默认值是未解析的整串占位）→ 跳过。
        let mut no_key = McpCapabilityDefinition::new("mcp_no_key");
        no_key.url = Some("http://127.0.0.1:1/no-key-srv/sse".to_owned());
        no_key.api_key_default = Some("${__ZK_MCP_TEST_UNSET_KEY__}".to_owned());
        no_key.enabled = true;
        registry.add_capability(no_key).expect("注册表条目写入");

        let manager = builder(&approval, &sink)
            .registry(Arc::clone(&registry))
            .build();
        running(&manager);
        manager.activate_registry_capabilities().await;

        assert!(manager.get_connection("no-url-srv").is_none());
        assert!(manager.get_connection("no-key-srv").is_none());
        assert!(approval.sources_for("no-url-srv").is_empty());
        assert!(approval.sources_for("no-key-srv").is_empty());
    }

    #[test]
    fn build_config_from_registry_resolves_transport_and_filters_placeholder_url() {
        let mut definition = McpCapabilityDefinition::new("mcp_alpha");
        definition.server_key = Some("srv".to_owned());
        definition.url = Some("${__ZK_MCP_TEST_UNSET_URL__}".to_owned());
        definition.transport_type = Some(McpTransportType::Http);

        let config = McpClientManager::build_config_from_registry(&definition);

        assert_eq!(config.transport, McpTransportType::Http);
        assert!(config.url.is_none());
    }
}
