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

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zk_tools::Tool;

use crate::capability_registry::{McpCapabilityDefinition, McpCapabilityRegistry};
use crate::config::{McpConfigScope, McpConfigurationResolver, McpServerConfig, McpTransportType};
use crate::connection::{McpConnectionStatus, McpServerConnection};
use crate::prompt_adapter::McpPromptAdapter;
use crate::protocol::{
    METHOD_ROOTS_LIST_CHANGED, ProgressTracker, PromptDefinition, RootsProvider,
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

    connections: Mutex<HashMap<String, Arc<McpServerConnection>>>,
    connection_generations: Mutex<HashMap<String, Arc<AtomicU64>>>,
    reconnecting_servers: Mutex<HashMap<String, Arc<McpServerConnection>>>,
    scheduled_reconnects: Mutex<HashMap<String, ReconnectTask>>,
    active_reconnects: Mutex<HashMap<String, ReconnectTask>>,
    consecutive_failures: Mutex<HashMap<String, u32>>,
    last_successful_ping: Mutex<HashMap<String, SystemTime>>,

    reconnect_permits: Semaphore,
    next_task_id: AtomicU64,
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
}

impl McpClientManagerBuilder {
    /// 两个必需端口：信任判定与工具注册。
    #[must_use]
    pub fn new(approval: Arc<dyn ApprovalPort>, tool_sink: Arc<dyn McpToolSink>) -> Self {
        Self {
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

    /// 装配管理器。
    #[must_use]
    pub fn build(self) -> Arc<McpClientManager> {
        Arc::new(McpClientManager {
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
            connections: Mutex::new(HashMap::new()),
            connection_generations: Mutex::new(HashMap::new()),
            reconnecting_servers: Mutex::new(HashMap::new()),
            scheduled_reconnects: Mutex::new(HashMap::new()),
            active_reconnects: Mutex::new(HashMap::new()),
            consecutive_failures: Mutex::new(HashMap::new()),
            last_successful_ping: Mutex::new(HashMap::new()),
            reconnect_permits: Semaphore::new(RECONNECT_CONCURRENCY),
            next_task_id: AtomicU64::new(0),
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
        for capability in &enabled {
            if self
                .get_connection(&capability.extract_server_key())
                .is_some()
            {
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
        lock(&self.reconnecting_servers).clear();

        let connections: Vec<Arc<McpServerConnection>> = lock(&self.connections)
            .drain()
            .map(|(_, connection)| connection)
            .collect();
        for connection in &connections {
            connection.close().await;
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
    /// 信任分三档，逐字对照 Java：
    /// 1. 来源可信（非运行时手动添加）且尚未信任 → 自动记录 `{SCOPE}_RUNTIME`；
    /// 2. 仍不可信 → 建连接对象但置 `NEEDS_AUTH`，不发起连接；
    /// 3. 可信 → 建连 + 握手 + 注册工具。
    ///
    /// Java 用 `config.scope() != null` 区分「配置文件解析」与「手动添加」；
    /// zkcode 的 `scope` 是非可空枚举，故以 [`McpConfigScope::Dynamic`]
    /// （「运行时动态注册」）承担 Java `null` 的语义 —— 见模块级偏离 1。
    ///
    /// Java 的 `catch (Exception e) { setStatus(FAILED); installConnection(...) }`
    /// 分支在 Rust 侧被吸收：`McpServerConnection::connect` 不返回错误，失败即把
    /// 状态置为 `FAILED`，随后走同一条 install 路径。
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
        self.require_running()?;
        let generation = self.next_generation(&config.name);

        if !self.approval.is_trusted(&config) && config.scope != McpConfigScope::Dynamic {
            self.approval
                .record_approval(&config, &format!("{}_RUNTIME", config.scope.as_str()));
            tracing::info!(
                server = %config.name,
                scope = %config.scope,
                "Auto-trusted runtime MCP server"
            );
        }

        if !self.approval.is_trusted(&config) {
            tracing::info!(server = %config.name, "MCP server not trusted, pending approval");
            let connection = self.new_connection(config.clone());
            connection.set_status(McpConnectionStatus::NeedsAuth);
            self.install_connection(&config.name, generation, &connection)
                .await?;
            return Ok(connection);
        }

        let connection = self.new_connection(config.clone());
        connection.connect().await;
        self.install_connection(&config.name, generation, &connection)
            .await?;
        if connection.status() == McpConnectionStatus::Connected {
            self.register_tools_from_connection(&connection);
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

    /// 建连接实例并注入 roots / progress 端口（对照 Java 的三行装配）。
    fn new_connection(&self, config: McpServerConfig) -> Arc<McpServerConnection> {
        let connection = McpServerConnection::new(config);
        connection.set_roots_provider(Arc::clone(&self.roots_provider));
        if let Some(tracker) = &self.progress_tracker {
            connection.set_progress_tracker(Arc::clone(tracker));
        }
        connection
    }

    /// 移除 MCP 服务器（对照 `removeServer(name)`）。
    pub async fn remove_server(&self, name: &str) -> bool {
        self.next_generation(name);
        let connection = lock(&self.connections).remove(name);
        self.cancel_reconnect_work(name);
        let Some(connection) = connection else {
            return false;
        };
        remove_if_same(&self.reconnecting_servers, name, &connection);
        self.tool_sink.unregister_by_prefix(&tool_prefix(name));
        connection.close().await;
        tracing::info!(server = name, "MCP server removed");
        true
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
            .filter(|connection| connection.status() == McpConnectionStatus::Connected)
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
        let Some(connection) = self.get_connection(name) else {
            return Err(ManagerError::ServerNotFound(name.to_owned()));
        };
        let generation = self.next_generation(name);
        self.cancel_reconnect_work(name);
        tracing::info!(server = name, "Restarting MCP server");
        self.tool_sink.unregister_by_prefix(&tool_prefix(name));
        connection.close().await;

        connection.connect().await;
        if !self.is_current_connection(name, &connection, generation) {
            connection.close().await;
            return Ok(());
        }
        if connection.status() == McpConnectionStatus::Connected {
            connection.reset_reconnect_attempts();
            self.register_tools_from_connection(&connection);
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
        if !self.is_running() || self.generation_of(server_id) != generation {
            connection.close().await;
            return Err(ManagerError::LifecycleChanged);
        }
        let previous = lock(&self.connections).insert(server_id.to_owned(), Arc::clone(connection));
        if let Some(previous) = previous
            && !Arc::ptr_eq(&previous, connection)
        {
            previous.close().await;
        }
        Ok(())
    }

    /// 代际 + 引用双重校验（对照 `isCurrentConnection`）。
    fn is_current_connection(
        &self,
        server_id: &str,
        connection: &Arc<McpServerConnection>,
        generation: u64,
    ) -> bool {
        self.is_running()
            && self.generation_of(server_id) == generation
            && lock(&self.connections)
                .get(server_id)
                .is_some_and(|current| Arc::ptr_eq(current, connection))
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
}

// ===== 工具与 prompt 发现 =====

impl McpClientManager {
    /// 包装所有已连接服务器的工具（对照 `discoverAndWrapTools()`）。
    #[must_use]
    pub fn discover_and_wrap_tools(&self) -> Vec<Arc<dyn Tool>> {
        self.connected_servers()
            .iter()
            .flat_map(|connection| self.wrap_mcp_tools(connection))
            .collect()
    }

    /// 单连接的工具包装（对照 `wrapMcpTools`：不做频道权限过滤）。
    fn wrap_mcp_tools(&self, connection: &Arc<McpServerConnection>) -> Vec<Arc<dyn Tool>> {
        connection
            .tools()
            .iter()
            .map(|tool| self.build_adapter(connection, tool))
            .collect()
    }

    /// 装配单个适配器（注册表覆盖 + 进度端口 + 共享降级缓存）。
    fn build_adapter(
        &self,
        connection: &Arc<McpServerConnection>,
        tool: &crate::protocol::ToolDefinition,
    ) -> Arc<dyn Tool> {
        let capability = self
            .registry
            .as_ref()
            .and_then(|registry| registry.find_by_tool_name(connection.name(), &tool.name));
        let (enhanced_description, timeout_ms) =
            capability.as_ref().map_or((None, 0), |capability| {
                (capability.description.clone(), capability.timeout_ms)
            });
        let mut adapter = McpToolAdapter::new(
            format!("{}{}", tool_prefix(connection.name()), tool.name),
            Some(tool.description.clone()),
            Some(tool.input_schema.clone()),
            Arc::clone(connection),
            tool.name.clone(),
        )
        .with_registry_overrides(enhanced_description, timeout_ms)
        .with_capability_identity(
            capability.as_ref().map(|value| value.id.clone()),
            capability.and_then(|value| value.domain),
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
    fn register_tools_from_connection(self: &Arc<Self>, connection: &Arc<McpServerConnection>) {
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
        }

        // Prompt discovery performs protocol I/O. Register adapters asynchronously and
        // reject stale connection generations before touching the shared tool directory.
        let weak_manager = Arc::downgrade(self);
        let weak_connection = Arc::downgrade(connection);
        tokio::spawn(async move {
            let (Some(manager), Some(connection)) =
                (weak_manager.upgrade(), weak_connection.upgrade())
            else {
                return;
            };
            let name = connection.name().to_owned();
            let generation = manager.generation_of(&name);
            let prompts = connection.list_prompts().await;
            if !manager.is_current_connection(&name, &connection, generation) {
                return;
            }
            for prompt in prompts {
                manager
                    .tool_sink
                    .register_dynamic(Arc::new(McpPromptAdapter::new(
                        Arc::clone(&connection),
                        prompt,
                    )));
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
            manager.tool_sink.unregister_by_prefix(&tool_prefix(&name));
            manager.register_tools_from_connection(&connection);
            tracing::info!(server = %name, "MCP tools refreshed for server");
        }));
    }

    /// 频道权限判定（对照 `isToolAllowed`：空表放行；`"*"` 屏蔽整服务器）。
    fn is_tool_allowed(&self, server_name: &str, tool_name: &str) -> bool {
        if self.channel_permissions.is_empty() {
            return true;
        }
        self.channel_permissions
            .get(server_name)
            .is_none_or(|blocked| {
                !blocked
                    .iter()
                    .any(|entry| entry == tool_name || entry == "*")
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
            let name = connection.name().to_owned();
            if connection.status() == McpConnectionStatus::Connected && !connection.is_alive() {
                tracing::warn!(server = %name, "MCP server connection lost");
                connection.set_status(McpConnectionStatus::Failed);
            }
            if connection.status() == McpConnectionStatus::Failed
                && connection.config().transport != McpTransportType::Stdio
            {
                self.schedule_delayed_reconnect(&name, &connection);
            }
        }

        for connection in self.list_connections() {
            if connection.status() != McpConnectionStatus::Connected {
                continue;
            }
            let name = connection.name().to_owned();
            if connection.send_health_ping().await {
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
                connection.set_status(McpConnectionStatus::Degraded);
                self.schedule_reconnect(&name);
            }
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
            self.schedule_delayed_reconnect(&name, &connection);
        }
    }

    /// 立即调度一次重连并置 `DEGRADED`（对照 `scheduleReconnect(name)`）。
    pub fn schedule_reconnect(self: &Arc<Self>, connection_name: &str) {
        let Some(connection) = self.get_connection(connection_name) else {
            return;
        };
        let generation = self.generation_of(connection_name);
        if !self.is_current_connection(connection_name, &connection, generation) {
            return;
        }
        connection.set_status(McpConnectionStatus::Degraded);
        self.broadcast_health_status(connection_name, McpConnectionStatus::Degraded);
        self.submit_reconnect(connection_name, &connection, generation);
    }

    /// 延迟重连调度（对照 `scheduleDelayedReconnect`）。
    ///
    /// Java 在 `ConcurrentHashMap.compute` 内二次校验代际；此处把两次校验都放在
    /// 取 `scheduled_reconnects` 锁之前/之后的无锁区，保证任一时刻只持一把锁
    /// （避免与 `connections` 形成锁序环）。
    fn schedule_delayed_reconnect(
        self: &Arc<Self>,
        server_id: &str,
        connection: &Arc<McpServerConnection>,
    ) {
        let generation = self.generation_of(server_id);
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
            if Arc::ptr_eq(&existing.connection, connection) && !existing.is_done() {
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
            remove_task_by_id(&manager.scheduled_reconnects, &server_id, task_id);
            if manager.is_current_connection(&server_id, &connection, generation) {
                manager.submit_reconnect(&server_id, &connection, generation);
            }
        })
    }

    /// 提交重连到并发受限的工作池（对照 `submitReconnect`）。
    fn submit_reconnect(
        self: &Arc<Self>,
        server_id: &str,
        connection: &Arc<McpServerConnection>,
        generation: u64,
    ) {
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
        if !self.is_current_connection(server_id, connection, generation) {
            return;
        }
        {
            let mut reconnecting = lock(&self.reconnecting_servers);
            if reconnecting.contains_key(server_id) {
                tracing::debug!(
                    server = server_id,
                    "Reconnect already in progress, skipping"
                );
                return;
            }
            reconnecting.insert(server_id.to_owned(), Arc::clone(connection));
        }
        self.reconnect_once(server_id, connection, generation).await;
        remove_if_same(&self.reconnecting_servers, server_id, connection);
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
        connection.connect().await;
        if !self.is_current_connection(server_id, connection, generation) {
            connection.close().await;
            return;
        }
        if connection.status() == McpConnectionStatus::Connected {
            connection.reset_reconnect_attempts();
            self.register_tools_from_connection(connection);
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
        let config = Self::build_config_from_registry(definition);
        tracing::info!(
            capability = %definition.id,
            server = %config.name,
            "Enabling MCP capability"
        );
        if !self.approval.is_trusted(&config) {
            self.approval.record_approval(&config, "REGISTRY");
            tracing::info!(capability = %definition.id, "Auto-trusted registry capability");
        }
        self.add_server(config).await
    }

    /// 由注册表条目构建 SSE 服务器配置（对照 `buildConfigFromRegistry`）。
    ///
    /// API Key 经 [`McpCapabilityDefinition::resolve_api_key`] 解析（Java 走
    /// Spring `Environment`，zkcode 走环境变量 + `${VAR}` 展开，见
    /// `capability_registry` 模块偏离 3）。
    #[must_use]
    pub fn build_config_from_registry(definition: &McpCapabilityDefinition) -> McpServerConfig {
        let mut headers = BTreeMap::new();
        if let Some(api_key) = definition.resolve_api_key() {
            headers.insert("Authorization".to_owned(), format!("Bearer {api_key}"));
        }
        McpServerConfig {
            name: definition.extract_server_key(),
            transport: McpTransportType::Sse,
            command: None,
            args: Vec::new(),
            env: BTreeMap::new(),
            url: definition.sse_url.clone(),
            headers,
            scope: McpConfigScope::Dynamic,
        }
    }
}

/// `mcp__<server>__` 前缀（对照 Java 各处的字符串拼接）。
fn tool_prefix(server_name: &str) -> String {
    format!("{MCP_TOOL_NAME_PREFIX}{server_name}__")
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
    }

    impl McpToolSink for RecordingSink {
        fn register_dynamic(&self, tool: Arc<dyn Tool>) {
            lock(&self.registered).push(tool.name().to_owned());
        }

        fn unregister_by_prefix(&self, prefix: &str) {
            lock(&self.unregistered).push(prefix.to_owned());
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
    struct StubTransport {
        connected: bool,
        ping: bool,
    }

    impl McpTransport for StubTransport {
        fn connect(&self) -> BoxFuture<'_, Result<(), McpProtocolError>> {
            Box::pin(async { Ok(()) })
        }

        fn send_request<'a>(
            &'a self,
            _method: &'a str,
            _params: Option<Value>,
            _timeout: Duration,
        ) -> BoxFuture<'a, Result<Option<Value>, McpProtocolError>> {
            Box::pin(async { Ok(None) })
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
            Box::pin(async move { self.ping })
        }

        fn is_connected(&self) -> bool {
            self.connected
        }

        fn set_notification_handler(&self, _handler: NotificationHandler) {}

        fn close(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    // ===== 装配辅助 =====

    /// `SDK` 传输在 `create_transport` 中无实现 → `connect()` 直接置
    /// `CONNECTED` 且不持传输，是「已连接但 `is_alive() == false`」的理想替身。
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
    async fn add_server_auto_trusts_config_file_scope_and_connects() {
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
            .expect("可信配置应建连");
        assert_eq!(connection.status(), McpConnectionStatus::Connected);
        assert_eq!(approval.sources_for("srv"), vec!["USER_RUNTIME".to_owned()]);
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
    async fn remove_server_unregisters_tools_and_closes() {
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
        let connection = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");
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
            Some(McpConnectionStatus::Connected)
        );
    }

    // ===== 工具注册 =====

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
        let connection = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");
        connection.set_tools(vec![tool_def("allowed"), tool_def("blocked")]);

        manager.register_tools_from_connection(&connection);
        assert_eq!(sink.registered(), vec!["mcp__srv__allowed".to_owned()]);
    }

    #[tokio::test]
    async fn tools_changed_callback_refreshes_registration() {
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
        let connection = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");
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
    async fn discover_and_wrap_tools_skips_disconnected_servers() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let live = manager
            .add_server(config_with(
                "live",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");
        live.set_tools(vec![tool_def("alpha")]);
        let dead = manager
            .add_server(config_with(
                "dead",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");
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
        definition.sse_url = Some("http://127.0.0.1:1/srv/sse".to_owned());
        definition.description = Some("registry description".to_owned());
        definition.timeout_ms = 7_000;
        definition.enabled = true;
        registry.add_capability(definition).expect("注册表条目写入");

        let manager = builder(&approval, &sink)
            .registry(Arc::clone(&registry))
            .build();
        running(&manager);
        let connection = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");
        connection.set_tools(vec![tool_def("alpha")]);

        let tools = manager.discover_and_wrap_tools();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].description(), "registry description");
        assert_eq!(tools[0].timeout(), MCP_TOOL_MAX_EXECUTION);
    }

    // ===== 健康检查 =====

    #[tokio::test]
    async fn health_check_marks_dead_connection_failed_and_schedules_reconnect() {
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
        let connection = manager.add_server(config).await.expect("落连接对象");
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
        let connection = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");
        connection.set_transport_for_test(Arc::new(StubTransport {
            connected: true,
            ping: true,
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
        let connection = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");
        connection.set_transport_for_test(Arc::new(StubTransport {
            connected: true,
            ping: false,
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
        let connection = manager
            .add_server(config_with(
                "srv",
                McpTransportType::Sdk,
                McpConfigScope::User,
            ))
            .await
            .expect("建连");

        // CONNECTED 但无传输 → is_alive() 为假 → FAILED → 调度延迟重连。
        manager.health_check().await;
        assert_eq!(connection.status(), McpConnectionStatus::Failed);

        // SDK 传输的 connect() 恒置 CONNECTED，故重连必然成功并广播。
        assert!(
            wait_until(|| observer.saw("srv", McpConnectionStatus::Connected)).await,
            "退避到期后应完成一次重连"
        );
        assert_eq!(connection.status(), McpConnectionStatus::Connected);
        assert_eq!(connection.reconnect_attempts(), 0);
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
            Some(McpConnectionStatus::Connected)
        );

        // 已存在同名连接时第二段不再处理（对照 `!connections.containsKey`）。
        manager.initialize_static_configs().await;
        assert_eq!(approval.sources_for("app-srv").len(), 1);
    }

    #[test]
    fn build_config_from_registry_maps_sse_url_and_authorization() {
        let mut definition = McpCapabilityDefinition::new("mcp_alpha");
        definition.sse_url = Some("https://mcp.example.com/registry-server/sse".to_owned());
        definition.api_key_default = Some("secret-key".to_owned());

        let config = McpClientManager::build_config_from_registry(&definition);

        assert_eq!(config.name, "registry-server");
        assert_eq!(config.transport, McpTransportType::Sse);
        assert_eq!(config.scope, McpConfigScope::Dynamic);
        assert_eq!(
            config.url.as_deref(),
            Some("https://mcp.example.com/registry-server/sse")
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
        definition.sse_url = Some("https://mcp.example.com/registry-server/sse".to_owned());

        let config = McpClientManager::build_config_from_registry(&definition);
        assert!(config.headers.is_empty());
    }

    #[tokio::test]
    async fn enable_from_registry_records_registry_approval() {
        let approval = RecordingApproval::shared();
        let sink = RecordingSink::shared();
        let manager = builder(&approval, &sink).build();
        running(&manager);
        let mut definition = McpCapabilityDefinition::new("mcp_alpha");
        // 127.0.0.1:1 立即 ECONNREFUSED —— 不产生真实外网访问。
        definition.sse_url = Some("http://127.0.0.1:1/registry-server/sse".to_owned());
        definition.enabled = true;

        let connection = manager
            .enable_from_registry(&definition)
            .await
            .expect("注册表条目应落连接对象");

        assert_eq!(connection.name(), "registry-server");
        assert_eq!(connection.status(), McpConnectionStatus::Failed);
        assert_eq!(
            approval.sources_for("registry-server"),
            vec!["REGISTRY".to_owned()]
        );
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
}
