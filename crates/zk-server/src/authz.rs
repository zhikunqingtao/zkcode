//! 权限管线组装根（2.5）。
//!
//! 依赖方向铁律要求 `zk-authz → (zk-protocol, zk-db)`，因此 zk-authz 把旧 Spring
//! 注入的协作者全部反转为 trait 端口（见 [`zk_authz::tool_facts`]）。本模块是这些
//! 端口在真实进程里的唯一实现处，逐一对应旧 Java 的 `@Component`：
//!
//! | 本模块类型 | 实现端口 | 旧源协作者 |
//! |---|---|---|
//! | [`BashSecurityBridge`] | `BashSecurityPort` | `tool.bash.BashSecurityAnalyzer` |
//! | [`ShellStateBridge`] | `ShellStatePort` | `tool.bash.ShellStateManager` |
//! | [`PermissionModeRegistry`] | `ModeProvider` | `permission.PermissionModeManager` |
//! | [`ProjectWorkspaceTrust`] | `WorkspaceTrustProbe` | `service.ProjectWorkspaceService` |
//! | [`RegistryToolFacts`] | `ToolFacts` | `tool.Tool`（身份面子集） |
//! | [`WsInteractionPublisher`] | `InteractionPublisher` | `pushInteractionView` 直推点 |
//! | [`EngineAdmission`] | `zk_engine::ToolAdmission` | `ToolExecutionPipeline` 阶段 4/5 |
//!
//! [`AuthzStack`] 聚合上述装配（对应旧 Spring 容器里的一组单例 bean），由
//! [`crate::state::AppState`] 持有，`engine_bridge` 与 REST 控制器共用同一实例。

use std::path::Path;
use std::sync::Arc;

use serde_json::Value;
use zk_authz::analyzer::OperationAnalyzerRegistry;
use zk_authz::frozen::FrozenToolInputFactory;
use zk_authz::gateway::{GatewayError, ToolExecutionGateway};
use zk_authz::grants::GrantStore;
use zk_authz::model::{AuthzError, PermissionMode};
use zk_authz::path_security::{PathSecurityService, SystemScratchpadPathPolicy};
use zk_authz::sensitive::SensitiveDataFilter;
use zk_authz::service::AuthorizationService;
use zk_authz::subject::AuthorizationSubjectResolver;
use zk_authz::tool_facts::{
    BashParseOutcome, BashSecurityPort, ModeProvider, RunEventSink, SensitiveDataFilterPort,
    ShellStatePort, ToolFacts, ToolUseContext, WorkspaceTrustProbe,
};
use zk_db::Db;
use zk_engine::admission::{Admission, AdmissionRequest, ToolAdmission};
use zk_protocol::ServerMessage;
use zk_tools::Tool;
use zk_tools::bash::ast::ParseForSecurityResult;
use zk_tools::bash::security::BashSecurityAnalyzer;
use zk_tools::bash::shell_state::ShellStateManager;
use zk_tools::registry::ToolRegistry;

use crate::config::Config;
use crate::interaction::{DurableInteractionService, RunEvents};
use crate::run_termination::RunTerminationCoordinator;
use crate::ws::WsHub;

/// 旧 `TooComplex#nodeType()` 的黑名单绝对拒绝标记（`BashSecurityAnalyzer` 内联常量）。
const BLACKLIST_DENY_NODE_TYPE: &str = "command-blacklist-deny";

// ===================== BashSecurityPort =====================

/// [`BashSecurityPort`] 的真实实现：桥接 zk-tools 的四层安全解析器。
pub struct BashSecurityBridge {
    analyzer: BashSecurityAnalyzer,
}

impl std::fmt::Debug for BashSecurityBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BashSecurityBridge").finish_non_exhaustive()
    }
}

impl Default for BashSecurityBridge {
    fn default() -> Self {
        Self::new()
    }
}

impl BashSecurityBridge {
    /// 以默认内建黑名单构造（旧 `@Component` 单例）。
    #[must_use]
    pub fn new() -> Self {
        Self {
            analyzer: BashSecurityAnalyzer::new(),
        }
    }
}

impl BashSecurityPort for BashSecurityBridge {
    fn parse_for_security(
        &self,
        command: &str,
        cwd: &Path,
        authorization_root: &Path,
    ) -> BashParseOutcome {
        match self
            .analyzer
            .parse_for_security_with_context(Some(command), cwd, authorization_root)
        {
            // 旧源两个成功分支在授权链侧同构：`Simple` 与 `ParseUnavailable` 都不
            // 触发降级（`ParseUnavailable` 的护栏在解析器内部已按最保守处置）。
            ParseForSecurityResult::Simple { .. } | ParseForSecurityResult::ParseUnavailable => {
                BashParseOutcome::Parsed
            }
            ParseForSecurityResult::TooComplex { reason, node_type } => {
                if node_type == BLACKLIST_DENY_NODE_TYPE {
                    BashParseOutcome::BlacklistDeny { reason }
                } else {
                    BashParseOutcome::TooComplex { reason }
                }
            }
        }
    }

    fn inherited_environment_references(&self, command: &str) -> Vec<String> {
        self.analyzer
            .analyze_environment_references(Some(command))
            .inherited_references
            .into_iter()
            .collect()
    }
}

// ===================== ShellStatePort =====================

/// [`ShellStatePort`] 的真实实现：桥接 zk-tools 的 [`ShellStateManager`]。
#[derive(Debug, Default, Clone, Copy)]
pub struct ShellStateBridge;

impl ShellStatePort for ShellStateBridge {
    fn resolve_working_directory(&self, session_id: &str, configured: &str) -> String {
        ShellStateManager::resolve_working_directory(session_id, configured)
    }

    fn authorization_environment_facts(&self, inherited: &[String]) -> Value {
        // 旧 `Map<String,String>` → 端口的 JSON 对象；`BTreeMap` 保证键序稳定，
        // 与 `operationHash` 的 canonical JSON 递归键排序一致。
        let facts = ShellStateManager::authorization_environment_facts(inherited);
        Value::Object(
            facts
                .into_iter()
                .map(|(key, value)| (key, Value::String(value)))
                .collect(),
        )
    }
}

// ===================== ModeProvider =====================

/// 会话权限模式登记表——逐字移植旧 `PermissionModeManager`（76 行）。
///
/// 旧源以 `ConcurrentHashMap<String, PermissionMode>` 持有会话态，进程重启即丢
/// （非持久化，旧源同）。`set_mode` 在模式真实变化时推 `permission_mode_changed`
/// 下行，涉 `AUTO_APPROVE` 用 `warn` 否则 `info`（旧 L47-52）。
pub struct PermissionModeRegistry {
    modes: std::sync::Mutex<std::collections::HashMap<String, PermissionMode>>,
    hub: Option<WsHub>,
}

impl std::fmt::Debug for PermissionModeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PermissionModeRegistry")
            .finish_non_exhaustive()
    }
}

impl PermissionModeRegistry {
    /// 装配（`hub` 对应旧 `@Autowired(required = false) WebSocketPusher`）。
    #[must_use]
    pub fn new(hub: Option<WsHub>) -> Self {
        Self {
            modes: std::sync::Mutex::new(std::collections::HashMap::new()),
            hub,
        }
    }

    /// 旧 `getMode`（L28-30）：未登记时返回 `DEFAULT`。
    #[must_use]
    pub fn get_mode(&self, session_id: &str) -> PermissionMode {
        self.lock()
            .get(session_id)
            .copied()
            .unwrap_or(PermissionMode::Default)
    }

    /// 旧 `hasExplicitMode`（L35-37）：是否显式设置过（区别于回退默认）。
    #[must_use]
    pub fn has_explicit_mode(&self, session_id: &str) -> bool {
        self.lock().contains_key(session_id)
    }

    /// 旧 `setMode`（L42-62）：`put` 取旧值（null → `DEFAULT`），仅在变化时记日志
    /// 并推下行；推送失败旧源只 `debug`、不上抛（non-fatal）。
    pub async fn set_mode(&self, session_id: &str, mode: PermissionMode) {
        let stored_previous = self.lock().insert(session_id.to_owned(), mode);
        let previous = stored_previous.unwrap_or(PermissionMode::Default);
        if previous == mode {
            return;
        }
        if mode == PermissionMode::AutoApprove || previous == PermissionMode::AutoApprove {
            tracing::warn!(
                session_id,
                previous = previous.as_str(),
                mode = mode.as_str(),
                "permission mode changed"
            );
        } else {
            tracing::info!(
                session_id,
                previous = previous.as_str(),
                mode = mode.as_str(),
                "permission mode changed"
            );
        }
        if let Some(hub) = self.hub.as_ref() {
            hub.push(
                session_id,
                ServerMessage::PermissionModeChanged {
                    mode: mode.as_str().to_owned(),
                    previous: Some(previous.as_str().to_owned()),
                },
            )
            .await;
        }
    }

    /// 旧 `clearSession`（L67-69）。
    pub fn clear_session(&self, session_id: &str) {
        self.lock().remove(session_id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, PermissionMode>> {
        self.modes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl ModeProvider for PermissionModeRegistry {
    fn mode(&self, root_session_id: &str) -> PermissionMode {
        self.get_mode(root_session_id)
    }
}

// ===================== WorkspaceTrustProbe =====================

/// [`WorkspaceTrustProbe`] 的真实实现——旧
/// `ProjectWorkspaceService#isTrustedFileScope`：授权根必须是用户显式选定的
/// Project 工作区，且此刻绑定仍指向同一目录。
pub struct ProjectWorkspaceTrust {
    db: Db,
    config: Arc<Config>,
}

impl std::fmt::Debug for ProjectWorkspaceTrust {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectWorkspaceTrust")
            .finish_non_exhaustive()
    }
}

impl ProjectWorkspaceTrust {
    /// 装配。
    #[must_use]
    pub fn new(db: Db, config: Arc<Config>) -> Self {
        Self { db, config }
    }

    /// 判定主体：Project 查询以闭包注入，供自锁出口与当前写事务出口共用。
    fn trusted_with(
        &self,
        authorization_root: &Path,
        find_project: impl FnOnce(&str) -> Result<Option<zk_db::ProjectRecord>, zk_db::DbError>,
    ) -> bool {
        // 旧源先 canonicalize 授权根，再按 workspace_root 唯一索引查 Project；
        // 命中后必须复核绑定（消失/重绑定/越 allowed roots 均视为不可信）。
        let Ok(canonical) = std::fs::canonicalize(authorization_root) else {
            return false;
        };
        let key = canonical.to_string_lossy().to_string();
        let found = match find_project(&key) {
            Ok(found) => found,
            Err(error) => {
                tracing::warn!(
                    workspace_root = %key,
                    error = %error,
                    "project workspace trust probe failed (treated as untrusted)"
                );
                return false;
            }
        };
        let Some(record) = found else {
            return false;
        };
        crate::workspace::require_current_binding(&self.config, &record.workspace_root)
            .is_ok_and(|current| current == canonical)
    }
}

impl WorkspaceTrustProbe for ProjectWorkspaceTrust {
    fn is_trusted_file_scope(&self, authorization_root: &Path) -> bool {
        // 端口为同步 trait（判定链在同步分析器内部调用），故走 zk-db 同步出口。
        self.trusted_with(authorization_root, |key| {
            self.db.find_project_by_workspace_root_blocking(key)
        })
    }

    fn is_trusted_file_scope_in_current_write(
        &self,
        conn: &rusqlite::Connection,
        authorization_root: &Path,
    ) -> bool {
        // 终局复检在 `Db::with_writer` 闭包内调用（writer Mutex 不可重入），
        // 必须复用已持有的连接查询——走 `*_blocking` 自锁出口即重入死锁。
        self.trusted_with(authorization_root, |key| {
            zk_db::find_project_by_workspace_root_in_current_write(conn, key)
        })
    }
}

// ===================== ToolFacts =====================

/// [`ToolFacts`] 适配器：把注册表里的 [`Tool`] 收窄成授权判定所需的身份面。
pub struct RegistryToolFacts {
    tool: Arc<dyn Tool>,
}

impl std::fmt::Debug for RegistryToolFacts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryToolFacts")
            .field("name", &self.tool.name())
            .finish()
    }
}

impl RegistryToolFacts {
    /// 包裹一件已注册工具。
    #[must_use]
    pub fn new(tool: Arc<dyn Tool>) -> Self {
        Self { tool }
    }
}

impl ToolFacts for RegistryToolFacts {
    fn name(&self) -> &str {
        self.tool.name()
    }

    fn is_mcp(&self) -> bool {
        self.tool.mcp_identity().is_some()
    }

    fn mcp_server(&self) -> Option<&str> {
        self.tool
            .mcp_identity()
            .map(|identity| identity.server_name.as_str())
    }

    fn mcp_server_id(&self) -> Option<&str> {
        self.tool
            .mcp_identity()
            .map(|identity| identity.server_id.as_str())
    }

    fn mcp_tool_name(&self) -> Option<&str> {
        self.tool
            .mcp_identity()
            .map(|identity| identity.tool_name.as_str())
    }

    fn mcp_capability_id(&self) -> Option<&str> {
        self.tool
            .mcp_identity()
            .and_then(|identity| identity.capability_id.as_deref())
    }

    fn mcp_resource_scope(&self) -> Option<&str> {
        self.tool
            .mcp_identity()
            .and_then(|identity| identity.resource_scope.as_deref())
    }

    fn mcp_domain_scope(&self) -> Option<&str> {
        self.tool
            .mcp_identity()
            .and_then(|identity| identity.domain_scope.as_deref())
    }

    fn mcp_config_hash(&self) -> Option<&str> {
        self.tool
            .mcp_identity()
            .map(|identity| identity.config_hash.as_str())
    }

    fn is_destructive(&self, input: &Value) -> bool {
        self.tool.is_destructive(input)
    }

    fn is_read_only(&self, input: &Value) -> bool {
        self.tool.is_read_only(input)
    }

    fn path_of(&self, input: &Value) -> Option<String> {
        self.tool.path_of(input)
    }
}

/// 注册表缺该工具时的兜底身份面（只有名字，全部默认元数据）。
///
/// 旧管线不可能走到这里（`ToolExecutionPipeline` 先按名解析工具再授权），本类型
/// 只保证准入端口在注册表不一致时仍以最保守元数据判定，而不是绕过授权。
#[derive(Debug, Clone)]
pub struct UnknownToolFacts {
    name: String,
}

impl UnknownToolFacts {
    /// 以工具名构造。
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
        }
    }
}

impl ToolFacts for UnknownToolFacts {
    fn name(&self) -> &str {
        &self.name
    }
}

// ===================== InteractionPublisher =====================

/// 交互视图下行出口——旧 `@EventListener onInteractionCreated` /
/// `onInteractionTerminal` + `pushInteractionView`（`WebSocketController.java:266-315`，
/// 见 I-01）。
///
/// [`crate::interaction::InteractionPublisher`] 是同步 trait（`create_authorization`
/// 落库后在同步上下文里直推），而 [`WsHub::push`] 为 `async`（critical 档需入
/// pending 暂存）。故此处 `tokio::spawn` 一次投递：旧源亦为「推送失败只记日志、
/// 不回滚交互」的 best-effort 语义。
///
/// `interaction_created` 事件**不直推**，而是经
/// [`crate::ws::deliver_interaction`] 的 INITIAL 闸门（transport 存在性 +
/// `mark_dispatched` claim），逐字对齐旧 `onInteractionCreated` → `deliverInteraction`。
pub struct WsInteractionPublisher {
    hub: WsHub,
    /// 交互权威回指。`Weak` 断环：服务持 publisher（`Arc<dyn ...>`），publisher
    /// 反持服务；旧系统靠 Spring 事件总线天然解耦，这里以 `Weak` 等价（D-03）。
    interactions: std::sync::OnceLock<std::sync::Weak<DurableInteractionService>>,
}

impl std::fmt::Debug for WsInteractionPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsInteractionPublisher")
            .finish_non_exhaustive()
    }
}

impl WsInteractionPublisher {
    /// 装配。
    #[must_use]
    pub fn new(hub: WsHub) -> Self {
        Self {
            hub,
            interactions: std::sync::OnceLock::new(),
        }
    }

    /// 回填交互权威（[`AuthzStack::build`] 在服务构造后调用一次）。
    ///
    /// 未回填时 `interaction_created` 退化为直推视图（不 claim 投递），只出现在
    /// 「publisher 已建但服务构造失败」的不可达路径上。
    pub fn bind(&self, interactions: &Arc<DurableInteractionService>) {
        let _ = self.interactions.set(Arc::downgrade(interactions));
    }
}

impl crate::interaction::InteractionPublisher for WsInteractionPublisher {
    fn publish(
        &self,
        session_id: &str,
        event_type: &'static str,
        record: &zk_authz::interaction::InteractionRecord,
    ) {
        debug_assert_eq!(
            session_id, record.session_id,
            "publisher session must own the interaction row"
        );
        let hub = self.hub.clone();
        let record = record.clone();
        // 旧 L266-274：`InteractionCreatedEvent` → `deliverInteraction(INITIAL)`；
        // 旧 L276-283：`InteractionTerminalEvent` → `pushInteractionView` 直推。
        let service = if event_type == "interaction_created" {
            self.interactions.get().and_then(std::sync::Weak::upgrade)
        } else {
            None
        };
        tokio::spawn(async move {
            match service {
                Some(service) => {
                    crate::ws::deliver_interaction(
                        &hub,
                        &service,
                        &record,
                        crate::ws::InteractionDelivery::Initial,
                    )
                    .await;
                }
                None => {
                    crate::ws::delivery::push_interaction_view(&hub, &record, event_type).await;
                }
            }
        });
    }
}

// ===================== 聚合装配 =====================

/// 权限管线聚合装配（旧 Spring 容器里的一组授权单例）。
pub struct AuthzStack {
    /// 交互权威（落库 / CAS 决策 / 重投 / 恢复）。
    pub interactions: Arc<DurableInteractionService>,
    /// Run 终止协调器（旧 `RunTerminationCoordinator` bean，Run 终态唯一权威）。
    ///
    /// 与 `interactions` 在同一 `Arc::new_cyclic` 里循环装配（见
    /// [`crate::run_termination::assemble`]），故交互过期路径与 REST 远程中断
    /// （`POST /api/remote/interrupt`，旧 `RemoteControlController` L99）用的是
    /// **同一实例**——Run 终态不出现第二个写入者。
    pub terminations: Arc<RunTerminationCoordinator>,
    /// 唯一策略与裁决权威。
    pub authorization: Arc<AuthorizationService>,
    /// 执行准入网关（动态复检 + `tool_started` 短事务）。
    pub gateway: Arc<ToolExecutionGateway>,
    /// 会话权限模式登记表。
    pub modes: Arc<PermissionModeRegistry>,
    /// 授权记录仓储（REST `GET/DELETE /api/permissions/grants` 数据源）。
    pub grants: GrantStore,
    /// 入参冻结工厂（canonical JSON + `inputHash`）。
    pub frozen: FrozenToolInputFactory,
}

impl std::fmt::Debug for AuthzStack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthzStack").finish_non_exhaustive()
    }
}

impl AuthzStack {
    /// 装配全链（`hub` 为 `None` 时无下行出口，用于纯持久层测试）。
    #[must_use]
    pub fn build(db: &Db, config: &Arc<Config>, hub: Option<WsHub>) -> Self {
        let scratchpads = SystemScratchpadPathPolicy::new(&config.scratchpad_system_root);
        let path_security = Arc::new(PathSecurityService::new(scratchpads.clone()));
        let bash_security: Arc<dyn BashSecurityPort> = Arc::new(BashSecurityBridge::new());
        let sensitive: Arc<dyn SensitiveDataFilterPort> = Arc::new(SensitiveDataFilter);
        let shell_state: Arc<dyn ShellStatePort> = Arc::new(ShellStateBridge);
        let analyzers = Arc::new(OperationAnalyzerRegistry::new(
            Some(bash_security),
            sensitive,
            path_security,
            shell_state,
        ));
        let runs: Arc<dyn RunEventSink> = Arc::new(RunEvents::new(db.clone()));
        let ws_publisher = hub
            .clone()
            .map(|hub| Arc::new(WsInteractionPublisher::new(hub)));
        let publisher: Arc<dyn crate::interaction::InteractionPublisher> =
            match ws_publisher.clone() {
                Some(publisher) => publisher,
                None => Arc::new(crate::interaction::NoopInteractionPublisher),
            };
        // 交互服务 ↔ Run 终止协调器的循环装配（旧 Spring 容器 + 事件总线，见
        // `crate::run_termination` 模块文档）：交互过期 / 容量耗尽经
        // `RunTerminationRequest` 端口真正把 Run 推向终态。
        let (interactions, terminations) = crate::run_termination::assemble(db.clone(), publisher);
        // 投递闸门回指（旧 Spring 事件总线的等价接线，见 D-03）。
        if let Some(publisher) = &ws_publisher {
            publisher.bind(&interactions);
        }
        let modes = Arc::new(PermissionModeRegistry::new(hub));
        let authorization = Arc::new(AuthorizationService::new(
            AuthorizationSubjectResolver::new(db.clone()),
            analyzers,
            GrantStore::new(db.clone()),
            interactions.clone(),
            modes.clone(),
            runs.clone(),
            Arc::new(ProjectWorkspaceTrust::new(db.clone(), config.clone())),
            scratchpads,
        ));
        let gateway = Arc::new(ToolExecutionGateway::new(
            authorization.clone(),
            runs,
            db.clone(),
        ));
        Self {
            interactions,
            terminations,
            authorization,
            gateway,
            modes,
            grants: GrantStore::new(db.clone()),
            frozen: FrozenToolInputFactory::default(),
        }
    }
}

// ===================== ToolAdmission =====================

/// 旧 `ToolExecutionPipeline` 阶段 4/5 的准入实现。
///
/// 三段式：冻结入参 → `AuthorizationService::authorize`（分析 / 不变量 / 模式 /
/// grant 匹配 / 必要时开交互并等待用户）→ `ToolExecutionGateway::admit`（动态复检
/// + 授权记录复检 + `tool_started` 短事务）。任一段失败按旧 5 类 catch 分桶为
///   [`Admission::Denied`]（推 `tool_permission_denied`）或 [`Admission::Failed`]（不推）。
pub struct EngineAdmission {
    stack: Arc<AuthzStack>,
    tools: Arc<ToolRegistry>,
    mode_override: Option<PermissionMode>,
}

impl std::fmt::Debug for EngineAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineAdmission").finish_non_exhaustive()
    }
}

impl EngineAdmission {
    /// 装配（`tools` 与引擎共用同一注册表，保证 `ToolFacts` 与执行体同源）。
    #[must_use]
    pub fn new(stack: Arc<AuthzStack>, tools: Arc<ToolRegistry>) -> Self {
        Self {
            stack,
            tools,
            mode_override: None,
        }
    }

    /// Reverse MCP admission: preserve all analysis/grant/gateway checks but never create
    /// an interaction request when no interactive transport exists.
    #[must_use]
    pub fn new_dont_ask(stack: Arc<AuthzStack>, tools: Arc<ToolRegistry>) -> Self {
        Self {
            stack,
            tools,
            mode_override: Some(PermissionMode::DontAsk),
        }
    }

    async fn decide(&self, request: AdmissionRequest<'_>) -> Admission {
        let facts: Box<dyn ToolFacts> = match self.tools.get(request.tool_name) {
            Some(tool) => Box::new(RegistryToolFacts::new(tool)),
            None => Box::new(UnknownToolFacts::new(request.tool_name)),
        };
        // 旧 L384-386：冻结失败 = `ToolInputValidationException` → 不推下行。
        let frozen = match self.stack.frozen.freeze(request.tool_name, request.input) {
            Ok(frozen) => frozen,
            Err(error) => {
                return Admission::Failed {
                    code: "INVALID_TOOL_INPUT".to_owned(),
                    message: error.message,
                };
            }
        };
        let context = ToolUseContext::new(
            Some(request.run_id.to_owned()),
            Some(request.tool_use_id.to_owned()),
            Some(request.session_id.to_owned()),
        )
        .with_shell(
            Some(request.session_id.to_owned()),
            request.working_directory.map(str::to_owned),
        );
        let authorization = &self.stack.authorization;
        let allowed = match if let Some(mode) = self.mode_override {
            authorization
                .authorize_with_mode(
                    facts.as_ref(),
                    &frozen,
                    request.input.clone(),
                    &context,
                    mode,
                )
                .await
        } else {
            authorization
                .authorize(facts.as_ref(), &frozen, request.input.clone(), &context)
                .await
        } {
            Ok(allowed) => allowed,
            Err(error) => return classify_authz_error(error),
        };
        match self
            .stack
            .gateway
            .admit(facts.as_ref(), &allowed, &context)
            .await
        {
            Ok(()) => Admission::Allow {
                execution_input: allowed.execution_input,
            },
            // 旧 L335-343：网关内抛出的 `AuthorizationException` 同样推下行。
            Err(GatewayError::Denied(denied)) => classify_authz_error(denied),
            // 旧 L344-354：`AdmissionException` 只回 `ToolResult.failed(VALIDATION, ...)`。
            Err(GatewayError::Admission(rejection)) => Admission::Failed {
                code: rejection.code,
                message: format!(
                    "Tool admission failed before execution: {}",
                    rejection.message
                ),
            },
            // 旧 L355-368：`DatabaseWriteUnavailableException` 定文案。
            Err(GatewayError::Db(error)) => {
                tracing::warn!(
                    tool = request.tool_name,
                    error = %error,
                    "authorization write is unavailable"
                );
                Admission::Failed {
                    code: "AUTHORIZATION_STORE_UNAVAILABLE".to_owned(),
                    message: "Authorization state is temporarily unavailable".to_owned(),
                }
            }
        }
    }
}

/// 旧 5 类 catch 的错误码分桶（`ToolExecutionPipeline.java:335-386`）。
///
/// - `AUTHORIZATION_STORE_UNAVAILABLE`：旧 `DatabaseWriteUnavailableException`
///   （zk-authz 的 `From<DbError>` 统一失败关闭到此码），L355-368。
/// - 三个 `INTERACTION_*` 码：旧 `InteractionOperationException`，L369-383；
///   `INTERACTION_PAYLOAD_INVALID` 走 VALIDATION 文案，其余走 INTERNAL 文案。
/// - 其余全部是旧 `AuthorizationException`（含 `COMMAND_ABSOLUTELY_DENIED` /
///   `PERMISSION_USER_DENIED` / 超时 / 模式拒绝），L335-343 → 推下行。
fn classify_authz_error(error: AuthzError) -> Admission {
    match error.code.as_str() {
        "AUTHORIZATION_STORE_UNAVAILABLE" => Admission::Failed {
            code: error.code,
            message: "Authorization state is temporarily unavailable".to_owned(),
        },
        "INTERACTION_PAYLOAD_INVALID" => Admission::Failed {
            code: error.code,
            message: "Permission interaction payload is invalid".to_owned(),
        },
        "INTERACTION_STORE_FAILED" | "INTERACTION_RUN_NOT_RUNNING" => Admission::Failed {
            code: error.code,
            message: "Permission interaction could not be persisted".to_owned(),
        },
        _ => Admission::Denied {
            code: error.code,
            message: error.message,
        },
    }
}

impl ToolAdmission for EngineAdmission {
    fn admit<'a>(
        &'a self,
        request: AdmissionRequest<'a>,
    ) -> futures::future::BoxFuture<'a, Admission> {
        Box::pin(self.decide(request))
    }
}
