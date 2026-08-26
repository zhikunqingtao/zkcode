//! 应用状态——handler 层共享句柄（`Db` + 配置 + 启动时刻）。

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

use zk_core::FeatureFlags;
use zk_db::Db;
use zk_engine::{
    BoundedObservabilityRecorder, ConversationService, CoordinatorService, FileHistoryService,
    HookService, JsonlObservabilitySink, MemdirStore, ObservabilityRecorder, PluginManager,
    SessionSnapshotService,
};
use zk_llm::{ProviderRegistry, SwappableProvider};
use zk_mcp::{ApprovalPort, McpCapabilityRegistry, McpClientManager};
use zk_tools::ToolRegistry;

use crate::access_token::AccessTokenManager;
use crate::api::browser_replay::BrowserReplayStore;
use crate::api::dialog::DialogRegistry;
use crate::authz::AuthzStack;
use crate::command::CommandRegistry;
use crate::config::Config;
use crate::cost::AtomicCostTracker;
use crate::mcp::{
    HubHealthObserver, HubProgressTracker, InMemoryApproval, RegistryToolSink, TrustFileApproval,
};
use crate::python::{PythonClient, PythonSidecar};
use crate::skill::SkillRegistry;
use crate::tool_catalog::ToolSessionState;
use crate::ws::{WsConfig, WsHub};

/// 组装根装配的共享应用状态。
///
/// `Db` 内部已 `Arc<Mutex<Connection>>`（D6 单连接模型），克隆即共享，
/// 无需再包一层；`Config` 不可变故以 `Arc` 共享；`started_at` 供
/// `/api/health` 的 uptime 计算（对齐旧 `HealthController.START_TIME`）。
#[derive(Clone)]
pub struct AppState {
    /// `SQLite` 会话库句柄（zk-db Repository 入口）。
    pub db: Db,
    /// 运行配置（端口 / 默认模型 / CORS 白名单等）。
    pub config: Arc<Config>,
    /// 特性标志表（旧 `FeatureFlagService` 单例 Bean）。
    ///
    /// 与 `config.feature_flags` 是同一实例——此处平铺一份句柄，让 handler 与
    /// 引擎侧无需穿过 `Config` 取门控；运行时改写（旧 `setFeatureValue`）对两条
    /// 路径同时可见。
    pub feature_flags: Arc<FeatureFlags>,
    /// 进程启动时刻（uptime 数据源）。
    pub started_at: Instant,
    /// WS 通道中枢（S8：连接注册 / 会话路由 / critical 暂存；克隆即共享）。
    pub hub: WsHub,
    /// 多提供商注册表（2.7：`GET /api/models` 动态聚合数据源 + 引擎路由/熔断/
    /// 降级链承载；自身即 `ChatProvider`，克隆共享同一实例）。
    pub providers: Arc<SwappableProvider>,
    /// DB-backed provider credentials mirrored in memory for trusted local
    /// integrations such as the built-in MCP web-search client.  The map is
    /// never serialized into responses or logs; updates happen only after the
    /// corresponding runtime DB write and provider hot-swap have succeeded.
    llm_provider_keys: Arc<RwLock<BTreeMap<String, String>>>,
    /// Python 侧车 HTTP 客户端（2.6；UDS 传输 + 能力缓存，恒存在——侧车未
    /// 启动时所有调用走能力域门控优雅降级，不 panic 不阻断核心对话）。
    pub python: Arc<PythonClient>,
    /// Python 侧车进程管理器（2.6；`None` = 侧车关闭或未由本进程托管，
    /// `/api/health` 据此上报 `DISABLED`）。
    pub python_sidecar: Option<Arc<PythonSidecar>>,
    /// 权限管线聚合装配（2.5；交互权威 / 授权服务 / 准入网关 / 模式登记表 /
    /// grant 仓储。REST 控制器与引擎准入端口共用同一实例）。
    pub authz: Arc<AuthzStack>,
    /// 局域网 access token 与会话 Cookie 权威（Batch 2b；旧
    /// `RemoteAccessSecurityFilter` 的凭证半边）。
    ///
    /// 准入中间件（[`crate::middleware::access_guard`]）与 `/api/auth/token`
    /// 共用同一实例——中间件校验的 token 与端点下发的 token 必须同源。
    pub access_tokens: Arc<AccessTokenManager>,
    /// 技能注册表（3B.7；14 内置技能编译期嵌入故装配即非空，磁盘六级来源由
    /// main 启动期扫描后增量注册。REST 目录端点与 WS `slash_command` 共用同
    /// 一实例，热重载写入对两侧立即可见）。
    pub skills: Arc<SkillRegistry>,
    /// 工具注册表的惰性单例格（Batch 1 Step 1-5）。
    ///
    /// 经 [`AppState::tools`] 首次访问时装配，此后 REST 工具域端点与
    /// `wire_engine` 注入引擎/准入端口的**是同一实例**——对齐旧端
    /// `ToolController` 与 `ToolExecutionPipeline` 注入同一个 `ToolRegistry`
    /// bean 的事实（元数据与真正执行的工具不可分叉）。
    tools: Arc<OnceLock<Arc<ToolRegistry>>>,
    /// WS/REST/SSE/CLI 共用的对话服务，由 `wire_engine` 绑定唯一 Engine 后回填。
    conversation: Arc<OnceLock<Arc<ConversationService>>>,
    /// 工具会话级启用位覆盖表（Batch 1 Step 1-5；旧 `ToolSessionState`
    /// `@Component` 单例的等价物，`PATCH /api/tools/{name}` 写、
    /// `GET /api/tools?sessionId=` 读）。
    pub tool_session_state: Arc<ToolSessionState>,
    /// 斜杠命令注册表（Batch 3；旧 `CommandRegistry` `@Service` 单例的等价
    /// 物，装配即含全部内建命令）。WS `slash_command` 分发与后续命令目录面
    /// 共用同一实例——运行时 `register` / `unregister` 对两侧立即可见。
    pub commands: Arc<CommandRegistry>,
    /// 会话/全局费用累加器（Batch 0 Step 0-6；旧 `CostTrackerService` 单例）。
    ///
    /// 引擎的 `CostTracker` 端口（[`crate::engine_bridge::wire_engine`] 注入）
    /// 与 `/cost` 命令读**同一实例**：分叉会让命令恒读到零成本。
    pub costs: Arc<AtomicCostTracker>,
    /// MCP 客户端管理器的惰性单例格（Batch 4B；旧 `McpClientManager`
    /// `@Component` + `SmartLifecycle` 单例）。
    ///
    /// 之所以是 `OnceLock` 而非直接持 `Arc<McpClientManager>`：管理器要注入
    /// 的四个端口里，`McpToolSink` 依赖 `AppState::tools()`、`McpHealthObserver`
    /// 依赖 `hub` **与管理器自身**（读 `consecutive_failures`）——直接构造就成
    /// 环。故 sink 只持 `Arc<ToolRegistry>`、observer 只持这同一个 `OnceLock`，
    /// 由 [`AppState::mcp`] 首次访问时装配并回填。
    mcp: Arc<OnceLock<Arc<McpClientManager>>>,
    /// MCP 能力注册表（Batch 4B；旧 `McpCapabilityRegistryService` 单例）。
    ///
    /// `/api/mcp/capabilities` 域十端点与管理器的注册表激活（`enableFromRegistry`
    /// / 描述与超时覆盖）读**同一实例**：分叉会让 REST 改了能力而引擎照旧。
    pub mcp_capabilities: Arc<McpCapabilityRegistry>,
    /// MCP 工具调用进度追踪器（Batch 4B；旧 `McpProgressTracker` 单例）。
    ///
    /// 平铺一份句柄（而非只交给管理器）以便后续监控端点读 `active_count()`。
    pub mcp_progress: Arc<HubProgressTracker>,
    /// MCP 服务器信任表（Batch 4B；旧 `McpApprovalService` 单例）。
    pub mcp_approval: Arc<dyn ApprovalPort>,
    /// 用户级长期记忆存储（Batch 5；旧 `MemdirService` `@Service` 单例）。
    ///
    /// `Memory` 工具（经 [`zk_tools::MemoryStore`] 端口注入）与 `/api/memory`
    /// 域端点读写**同一实例**：分叉会让工具写入的记忆在 REST 侧不可见，且
    /// 内部写锁失去互斥意义（并发写会互相覆盖）。
    pub memdir: Arc<MemdirStore>,
    /// 文件写前快照事务与 Rewind（Batch 5 Step 5；旧 `FileHistoryService`
    /// `@Service` 单例）。
    ///
    /// 引擎的回合事务边界与 `/api/sessions/{id}/history/*` 端点读写**同一
    /// 实例**：三份台账（读时间戳 / 活跃事务 / 已提交事务）都在进程内存中，
    /// 分叉会让端点侧永远看不到引擎登记的回合变更集。
    pub file_history: Arc<FileHistoryService>,
    /// 会话快照单例。REST save/list/resume/delete 共享同一目录与校验规则。
    pub session_snapshots: Arc<SessionSnapshotService>,
    /// 唯一生产 Coordinator；团队、Swarm 运行态、取消表与事件总线均由其持有。
    pub coordinator: Arc<CoordinatorService>,
    /// Process-wide bounded operations recorder. Structured events are kept
    /// separate from `SQLite` Run recovery logs.
    pub observability: Arc<dyn ObservabilityRecorder>,
    /// Agent/Task/Swarm 共用的生产子代理执行器与任务服务，随 `ToolRegistry` 惰性装配。
    agent_runtime: Arc<OnceLock<Arc<crate::engine_bridge::AgentRuntime>>>,
    /// Hook 系统服务（Batch 8B；事件驱动外部副作用通知）。
    ///
    /// 经 [`crate::engine_bridge::wire_engine`] 的 `with_hooks` 注入引擎——引擎
    /// 触发点与此持**同一实例**。装配期从 `workspace_default_root` 的
    /// `.zk/hooks.toml` 加载（文件缺失 → 空注册表，`fire` 直接短路）。
    pub hooks: Arc<HookService>,
    /// Batch 8G：Plugin 管理器（manifest.toml 扫描 + install / uninstall / reload）。
    pub plugin_manager: Arc<PluginManager>,
    /// 浏览器回放原子 JSON 存储（`{workspace}/.zk/browser-replay`）。
    pub browser_replay: Arc<BrowserReplayStore>,
    /// 对话框决策登记表（Batch 8B；旧 `DialogController.pendingRequests`）。
    ///
    /// `/api/dialogs/*` 端点与后端登记 pending 请求的服务读写**同一实例**。
    pub(crate) dialogs: Arc<DialogRegistry>,
}

impl AppState {
    /// 装配应用状态。
    #[must_use]
    pub fn new(db: Db, config: Config) -> Self {
        Self::new_with_ws(db, config, WsConfig::default())
    }

    /// Forward the unique Coordinator event stream to the native WebSocket hub.
    #[must_use]
    pub fn spawn_coordinator_event_bridge(&self) -> tokio::task::JoinHandle<()> {
        let mut receiver = self.coordinator.event_bus().subscribe();
        let hub = self.hub.clone();
        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(event) => {
                        let session_id = event.session_id().to_owned();
                        hub.push(&session_id, event.to_server_message()).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                        tracing::warn!(dropped, "coordinator WS bridge lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }

    /// 注入多提供商注册表（main 启动序列：先装配 registry 再交给 handler 层，
    /// `GET /api/models` 与引擎共用同一实例）。
    #[must_use]
    pub fn with_providers(mut self, providers: ProviderRegistry) -> Self {
        self.providers = Arc::new(SwappableProvider::new(providers));
        self
    }

    /// 从 DB 读取 LLM 密钥并热替换 provider 注册表（启动时合并 DB 密钥）。
    ///
    /// 读取 `config` 表 `llm_provider_keys` 行，解析为 `BTreeMap<String, String>`。
    /// 若运行时 DB、provider 环境变量和 Phase 1 环境变量都没有密钥（首次
    /// 启动），从仓库随附的、只读且可公开提取的 demo 种子库复制凭据到私有
    /// 运行时 DB。此后合并环境变量密钥后重建 registry 并 `swap`。种子读取
    /// 失败只记日志，不伪造或回退到源码常量。
    ///
    /// 共享合并逻辑见 [`crate::api::llm_keys::build_merged_provider_configs`]。
    pub async fn merge_db_llm_keys(&self) {
        let stored = match self.db.get_config_value("llm_provider_keys").await {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "failed to read llm_provider_keys; keeping env-based providers"
                );
                return;
            }
        };
        let mut db_keys: BTreeMap<String, String> = match stored {
            Some(json) => match serde_json::from_str(&json) {
                Ok(keys) => keys,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "stored llm provider keys are invalid; preserving current providers"
                    );
                    return;
                }
            },
            None => BTreeMap::new(),
        };
        let env_configs = zk_llm::provider_configs_from_env();
        let legacy_key_configured = !self.config.llm_api_key.is_empty();
        if db_keys.is_empty() && env_configs.is_empty() && !legacy_key_configured {
            match crate::demo_credentials::load(&self.config.demo_credentials_path) {
                Ok(seed) => match serde_json::to_string(&seed) {
                    Ok(json) => {
                        if let Err(err) = self.db.put_config_value("llm_provider_keys", &json).await
                        {
                            tracing::warn!(error = %err, "failed to copy public demo credential into the runtime DB");
                        } else {
                            tracing::info!(
                                provider = "dashscope-token-plan",
                                "copied public first-run demo credential into the private runtime DB"
                            );
                            db_keys = seed;
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to serialize public demo credential");
                    }
                },
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        path = %self.config.demo_credentials_path.display(),
                        "public demo credential database is unavailable; continuing without it"
                    );
                }
            }
        }
        match crate::api::llm_keys::build_provider_registry(&self.config, &db_keys) {
            Ok(registry) => {
                let provider_count = registry.len();
                let model_count = registry.models().len();
                self.replace_llm_provider_keys(db_keys.clone());
                self.providers.swap(registry);
                tracing::info!(
                    db_key_count = db_keys.len(),
                    provider_count,
                    model_count,
                    "merged DB LLM keys into provider registry"
                );
            }
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "failed to rebuild provider registry from DB keys"
                );
            }
        }
    }

    /// Replace the DB-backed credential mirror after a successful durable
    /// update. Poison recovery deliberately keeps the last map available.
    pub(crate) fn replace_llm_provider_keys(&self, keys: BTreeMap<String, String>) {
        match self.llm_provider_keys.write() {
            Ok(mut current) => *current = keys,
            Err(poisoned) => *poisoned.into_inner() = keys,
        }
    }

    /// Shared credential mirror used by the MCP composition root.
    #[must_use]
    pub(crate) fn llm_provider_keys_handle(&self) -> Arc<RwLock<BTreeMap<String, String>>> {
        Arc::clone(&self.llm_provider_keys)
    }

    /// 注入 Python 侧车进程管理器（main 启动序列：`spawn` 侧车后回填，
    /// 供 `/api/health` 读取进程状态与重启计数）。
    #[must_use]
    pub fn with_python_sidecar(mut self, sidecar: Arc<PythonSidecar>) -> Self {
        self.python_sidecar = Some(sidecar);
        self
    }

    /// 指定 WS 参数装配（集成测试注入 `WsConfig::fast_for_tests`）。
    #[doc(hidden)]
    #[must_use]
    pub fn new_with_ws(db: Db, config: Config, ws: WsConfig) -> Self {
        let python = Arc::new(PythonClient::new(config.python_uds_path.clone()));
        let config = Arc::new(config);
        // 与 `Config` 共用同一张 flag 表（启动期装配一次，不在此处二次构造）。
        let feature_flags = Arc::clone(&config.feature_flags);
        // 凭证在 hub 之前装配：不依赖任何运行时组件，且启动日志（main 打印的
        // 局域网访问地址）要用到它的掩码预览。
        let access_tokens = Arc::new(AccessTokenManager::load_or_create(
            config.access_token_path.as_deref(),
        ));
        let hub = WsHub::new(ws);
        // 权限管线在 hub 之后装配：交互下行出口与 `permission_mode_changed`
        // 推送都要经同一 hub 实例（克隆即共享）。
        let authz = Arc::new(AuthzStack::build(&db, &config, Some(hub.clone())));
        // MCP 进度追踪器与健康观察者共用同一 hub（克隆即共享同一实例）。
        let mcp_progress = Arc::new(HubProgressTracker::new(hub.clone()));
        // 装配即 `load()`（旧 `@PostConstruct loadRegistry()`）：文件缺失只
        // warn，服务照常起。
        let mcp_capabilities =
            Arc::new(McpCapabilityRegistry::new(config.mcp_registry_path.clone()));
        mcp_capabilities.load();
        let mcp_approval: Arc<dyn ApprovalPort> = match config.mcp_trust_file.clone() {
            Some(path) => Arc::new(TrustFileApproval::new(path)),
            None => Arc::new(InMemoryApproval::new()),
        };
        // Batch 5 Step 5：快照落库与会话工作目录读取共用同一 `Db`（旧实现同样
        // 注入 `FileSnapshotRepository` + `SessionManager`）。先建实例再进结构
        // 体——`db` 随后被字段移动，此处不能反序。
        let file_history = Arc::new(FileHistoryService::new(db.clone()));
        let snapshot_dir = config.snapshot_dir.clone().unwrap_or_else(|| {
            std::env::temp_dir().join(format!("zkcode-test-snapshots-{}", uuid::Uuid::new_v4()))
        });
        let session_snapshots = Arc::new(SessionSnapshotService::with_dir(snapshot_dir));
        let observability: Arc<dyn ObservabilityRecorder> =
            Arc::new(BoundedObservabilityRecorder::new(
                4096,
                Arc::new(JsonlObservabilitySink::new(
                    std::path::Path::new(&config.workspace_default_root)
                        .join(".zk")
                        .join("observability-events.jsonl"),
                )),
            ));
        // Batch 8B：Hook 服务从 workspace 缺省根的 `.zk/hooks.toml` 加载（文件
        // 缺失只 warn 回空表）；与引擎触发点共用同一实例（`wire_engine` 注入）。
        let hooks = Arc::new(
            HookService::load_from_dir(std::path::Path::new(&config.workspace_default_root))
                .with_observability(Arc::clone(&observability)),
        );
        let browser_replay_dir = std::path::Path::new(&config.workspace_default_root)
            .join(".zk")
            .join("browser-replay");
        let coordinator = Arc::new(
            CoordinatorService::new(Arc::clone(&feature_flags))
                .with_observability(Arc::clone(&observability)),
        );
        Self {
            db,
            config,
            feature_flags,
            started_at: Instant::now(),
            hub,
            providers: Arc::new(SwappableProvider::new(ProviderRegistry::new())),
            llm_provider_keys: Arc::new(RwLock::new(BTreeMap::new())),
            python,
            python_sidecar: None,
            authz,
            access_tokens,
            // 仅含内置技能：磁盘扫描留给 main（集成测试不受运行目录内容影响，
            // 目录端点在测试中恒为确定的 14 条）。
            skills: Arc::new(SkillRegistry::with_builtin_skills()),
            tools: Arc::new(OnceLock::new()),
            conversation: Arc::new(OnceLock::new()),
            tool_session_state: Arc::new(ToolSessionState::new()),
            commands: Arc::new(CommandRegistry::with_builtin_commands()),
            costs: Arc::new(AtomicCostTracker::new()),
            mcp: Arc::new(OnceLock::new()),
            mcp_capabilities,
            mcp_progress,
            mcp_approval,
            // 目录取 `~/.zk`（旧默认构造器取用户主目录），非会话/项目维度。
            memdir: Arc::new(MemdirStore::new()),
            file_history,
            session_snapshots,
            coordinator,
            observability,
            agent_runtime: Arc::new(OnceLock::new()),
            hooks,
            plugin_manager: Arc::new(PluginManager::default()),
            browser_replay: Arc::new(BrowserReplayStore::new(browser_replay_dir)),
            dialogs: Arc::new(DialogRegistry::new()),
        }
    }

    /// 工具注册表（进程内单实例，首次访问惰性装配）。
    ///
    /// 装配委托 [`crate::engine_bridge::build_tool_registry`]——注册清单、
    /// feature flag 门控与快照 sink 注入全部只此一处，REST 目录端点因此永远
    /// 看到与引擎执行侧完全相同的工具集合。
    #[must_use]
    pub fn tools(&self) -> Arc<ToolRegistry> {
        Arc::clone(
            self.tools
                .get_or_init(|| Arc::new(crate::engine_bridge::build_tool_registry(self))),
        )
    }

    /// Shared production Agent runtime. Building tools initializes it when Agent is enabled.
    pub(crate) fn agent_runtime(&self) -> Option<Arc<crate::engine_bridge::AgentRuntime>> {
        let _ = self.tools();
        self.agent_runtime.get().cloned()
    }

    /// Bind the first production Agent runtime and reject accidental state forks.
    pub(crate) fn set_agent_runtime(&self, runtime: Arc<crate::engine_bridge::AgentRuntime>) {
        if self.agent_runtime.set(runtime).is_err() {
            tracing::warn!("agent runtime already bound; keeping first instance");
        }
    }

    /// 读取已装配的共享对话服务。
    #[must_use]
    pub fn conversation(&self) -> Option<Arc<ConversationService>> {
        self.conversation.get().cloned()
    }

    /// 绑定唯一生产对话服务。重复绑定保持首次实例。
    pub(crate) fn set_conversation(&self, service: Arc<ConversationService>) {
        if self.conversation.set(service).is_err() {
            tracing::warn!("conversation service already bound; keeping first instance");
        }
    }

    /// MCP 客户端管理器（进程内单实例，首次访问惰性装配）。
    ///
    /// 四端口注入：信任表 / 工具 sink / 健康观察者 / 进度追踪器，另挂能力注册
    /// 表与多来源配置解析器（工作目录取 `workspace_default_root`，对齐旧
    /// `McpConfigurationResolver` 读项目级 `.mcp.json` 的基准目录）。
    ///
    /// **注意**：本方法只装配，不 `start()`——生命周期由 `main` 启动序列驱动
    /// （对照旧 `SmartLifecycle`），集成测试因此不会拉起真实 MCP 子进程。
    #[must_use]
    pub fn mcp(&self) -> Arc<McpClientManager> {
        Arc::clone(self.mcp.get_or_init(|| {
            let tool_sink = Arc::new(RegistryToolSink::new(self.tools(), self.hub.clone()));
            let observer = Arc::new(HubHealthObserver::new(
                self.hub.clone(),
                Arc::clone(&self.mcp),
            ));
            let provider_keys = self.llm_provider_keys_handle();
            let environment_fallback = zk_mcp::security::default_credential_resolver();
            let credential_resolver: Arc<dyn zk_mcp::McpCredentialResolver> =
                Arc::new(move |provider: &str| {
                    let db_key = match provider_keys.read() {
                        Ok(keys) => keys.get(provider).cloned(),
                        Err(poisoned) => poisoned.into_inner().get(provider).cloned(),
                    };
                    db_key.or_else(|| environment_fallback.resolve(provider))
                });
            McpClientManager::builder(Arc::clone(&self.mcp_approval), tool_sink)
                    .resolver(zk_mcp::McpConfigurationResolver::new(Some(
                        std::path::PathBuf::from(&self.config.workspace_default_root),
                    )))
                    .registry(Arc::clone(&self.mcp_capabilities))
                    .credential_resolver(credential_resolver)
                    .health_observer(observer)
                    .progress_tracker(
                        Arc::clone(&self.mcp_progress) as Arc<dyn zk_mcp::ProgressTracker>
                    )
                    .build()
        }))
    }

    /// MCP 惰性单例槽，供工具目录绑定同一个 Manager 且避免构造环。
    #[must_use]
    pub(crate) fn mcp_slot(&self) -> Arc<OnceLock<Arc<McpClientManager>>> {
        Arc::clone(&self.mcp)
    }

    /// 集成测试装配（内存库 + 测试配置）。
    #[doc(hidden)]
    #[must_use]
    pub fn for_tests() -> Self {
        Self::new(
            Db::open_in_memory().expect("in-memory db boots with migrations"),
            Config::test_config(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use zk_core::FlagValue;
    use zk_core::feature_flags;

    use super::AppState;

    /// `AppState` 与 `Config` 共享同一张 flag 表——旧实现是单例 Bean，若此处各持
    /// 一份，`/config` 域的运行时改写就只能改到其中一条路径。
    #[test]
    fn state_and_config_share_one_flag_table() {
        let state = AppState::for_tests();
        assert!(Arc::ptr_eq(
            &state.feature_flags,
            &state.config.feature_flags
        ));

        state
            .feature_flags
            .set_value(feature_flags::TOOL_SEARCH, FlagValue::Bool(true));
        assert!(
            state
                .config
                .feature_flags
                .is_enabled(feature_flags::TOOL_SEARCH),
            "改写必须对两条路径同时可见"
        );
    }

    /// 工具注册表惰性单例：同一 `AppState` 及其克隆恒得同一实例（REST 与
    /// 引擎不可各持一份）。
    #[test]
    fn tool_registry_is_a_lazy_process_singleton() {
        let state = AppState::for_tests();
        let first = state.tools();
        let second = state.tools();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(&first, &state.clone().tools()));
        assert!(!first.is_empty(), "组合根注册清单非空");
    }

    #[tokio::test]
    async fn first_run_copies_the_tracked_seed_into_the_runtime_database() {
        if !zk_llm::provider_configs_from_env().is_empty() {
            // An explicit provider environment is authoritative by design; the
            // clean-first-run path is exercised only in its absence.
            return;
        }
        let mut config = crate::config::Config::test_config();
        config.demo_credentials_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../configuration/bootstrap/demo-credentials.db");
        let state = AppState::new(
            zk_db::Db::open_in_memory().expect("runtime SQLite opens"),
            config,
        );

        state.merge_db_llm_keys().await;

        let stored = state
            .db
            .get_config_value("llm_provider_keys")
            .await
            .expect("runtime SQLite read succeeds")
            .expect("clean first run copies the public seed");
        let stored: std::collections::BTreeMap<String, String> =
            serde_json::from_str(&stored).expect("runtime credential map is valid JSON");
        assert_eq!(stored.len(), 1);
        assert!(stored.contains_key("dashscope-token-plan"));
        let provider_keys = state.llm_provider_keys_handle();
        let has_seed_key = match provider_keys.read() {
            Ok(keys) => keys.contains_key("dashscope-token-plan"),
            Err(poisoned) => poisoned.into_inner().contains_key("dashscope-token-plan"),
        };
        assert!(has_seed_key);
        assert_eq!(
            state.providers.load().resolve_provider("qwen3.8-max"),
            Some("dashscope-token-plan")
        );
    }

    /// 命令注册表装配即含全部内建命令，且克隆共享同一实例（WS 分发与
    /// 命令目录面不可各持一份）。
    #[test]
    fn command_registry_is_wired_with_builtin_commands() {
        let state = AppState::for_tests();
        assert!(!state.commands.is_empty());
        assert!(state.commands.find_command("help").is_some());
        assert!(Arc::ptr_eq(&state.commands, &state.clone().commands));
    }

    /// 工具门控投影字段与 flag 表出厂默认值一致（`WEB_BROWSER_TOOL` /
    /// `GIT_ENHANCED_TOOL` 均为 `true`），避免两个事实源悄悄漂移。
    #[test]
    fn tool_gate_projection_matches_flag_table() {
        let state = AppState::for_tests();
        assert_eq!(
            state.config.feature_web_browser_tool,
            state
                .feature_flags
                .is_enabled(feature_flags::WEB_BROWSER_TOOL)
        );
        assert_eq!(
            state.config.feature_git_enhanced_tool,
            state
                .feature_flags
                .is_enabled(feature_flags::GIT_ENHANCED_TOOL)
        );
    }
}
