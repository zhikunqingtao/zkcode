//! 依赖反转端口（ports）。
//!
//! zk-authz 不得依赖 zk-tools / zk-engine / zk-server（依赖方向铁律），因此旧
//! Java 侧由 Spring 注入的协作者在此以 trait 形式反转，由 zk-server 组合根提供
//! 实现：
//!
//! | 本模块 trait | 旧源协作者 |
//! |---|---|
//! | [`ToolFacts`] | `com.aicodeassistant.tool.Tool`（只取授权判定所需的身份面） |
//! | [`ModeProvider`] | `permission.PermissionModeManager` |
//! | [`RunEventSink`] | `run.RunControlService#appendEventBounded` / `#appendEventInCurrentWrite` |
//! | [`WorkspaceTrustProbe`] | `service.ProjectWorkspaceService#isTrustedFileScope` |
//! | [`BashSecurityPort`] | `tool.bash.BashSecurityAnalyzer` |
//! | [`ShellStatePort`] | `tool.bash.ShellStateManager` |
//! | [`SensitiveDataFilterPort`] | `security.SensitiveDataFilter` |
//! | [`ArtifactPublicationPort`] | `artifact.publication.ArtifactPublicationPolicy`（旧源 `@Autowired(required = false)`，本 crate 以 `Option` 表达） |

use std::path::Path;

use crate::model::PermissionMode;

/// 授权判定所需的工具调用上下文。
///
/// 对应旧 `com.aicodeassistant.tool.ToolUseContext`，只保留授权链读取的三个字段
/// （`currentRunId` / `toolUseId` / `rootSessionId`）——其余字段（abort signal、
/// 输出通道等）属于执行面，与判定无关。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolUseContext {
    /// 当前 Run（可能是子代理的合成 Run）。为 `None` 时授权链拒绝。
    pub current_run_id: Option<String>,
    /// 本次 `tool_use` 的协议 ID；为 `None` 时旧源回退为工具名。
    pub tool_use_id: Option<String>,
    /// 顶层会话 ID（诊断与交互投递用）。
    pub root_session_id: Option<String>,
    /// 本次调用的会话 ID（`ShellStateManager` 的 cwd 记忆键）。
    pub session_id: Option<String>,
    /// 本次调用的工作目录（`ToolUseContext#workingDirectory`）。
    pub working_directory: Option<String>,
}

impl ToolUseContext {
    /// 构造上下文。
    #[must_use]
    pub fn new(
        current_run_id: Option<String>,
        tool_use_id: Option<String>,
        root_session_id: Option<String>,
    ) -> Self {
        Self {
            current_run_id,
            tool_use_id,
            root_session_id,
            session_id: None,
            working_directory: None,
        }
    }

    /// 追加会话 ID 与工作目录（builder 风格，避免 5 参构造）。
    #[must_use]
    pub fn with_shell(
        mut self,
        session_id: Option<String>,
        working_directory: Option<String>,
    ) -> Self {
        self.session_id = session_id;
        self.working_directory = working_directory;
        self
    }

    /// `context.toolUseId() == null ? tool.getName() : context.toolUseId()`
    /// （`AuthorizationService.java:369`、`ToolExecutionGateway` 同构）。
    #[must_use]
    pub fn tool_use_id_or(&self, tool_name: &str) -> String {
        self.tool_use_id
            .clone()
            .unwrap_or_else(|| tool_name.to_owned())
    }
}

/// 工具身份面：授权分析只需要名字与元数据，不需要执行能力。
pub trait ToolFacts: Send + Sync {
    /// `Tool#getName()`。
    fn name(&self) -> &str;

    /// 是否为 MCP 桥接工具（决定 `mcp-v1` 分析器路由）。
    ///
    /// 旧源 `OperationAnalyzerRegistry` 以 `tool instanceof McpTool` 判定。
    fn is_mcp(&self) -> bool {
        false
    }

    /// MCP 服务器名（仅 MCP 工具有值），用于 `resources` 的 `mcp` 资源项。
    fn mcp_server(&self) -> Option<&str> {
        None
    }

    /// MCP server 稳定标识。
    fn mcp_server_id(&self) -> Option<&str> {
        self.mcp_server()
    }

    /// MCP 远端原始工具名。
    fn mcp_tool_name(&self) -> Option<&str> {
        None
    }

    /// MCP 能力注册表 ID。
    fn mcp_capability_id(&self) -> Option<&str> {
        None
    }

    /// MCP 资源范围。
    fn mcp_resource_scope(&self) -> Option<&str> {
        None
    }

    /// MCP domain 范围。
    fn mcp_domain_scope(&self) -> Option<&str> {
        None
    }

    /// 不含秘密的 MCP 配置摘要。
    fn mcp_config_hash(&self) -> Option<&str> {
        None
    }

    /// `Tool#isDestructive(ToolInput)`：`BashAnalyzer` 据此判 `HIGH`。
    fn is_destructive(&self, _input: &serde_json::Value) -> bool {
        false
    }

    /// `Tool#isReadOnly(ToolInput)`：`BashAnalyzer` 据此判 `SAFE` 与 effects。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        false
    }

    /// `Tool#getPath(ToolInput)`：文件类工具自报的路径参数（优先于字段猜测）。
    fn path_of(&self, _input: &serde_json::Value) -> Option<String> {
        None
    }
}

/// Bash 安全解析端口（旧 `BashSecurityAnalyzer`；实现位于 zk-tools）。
pub trait BashSecurityPort: Send + Sync {
    /// `parseForSecurity(command, cwd, authorizationRoot)` 的判定摘要。
    fn parse_for_security(
        &self,
        command: &str,
        cwd: &Path,
        authorization_root: &Path,
    ) -> BashParseOutcome;

    /// `analyzeEnvironmentReferences(command).inheritedReferences()`（未排序）。
    fn inherited_environment_references(&self, command: &str) -> Vec<String>;
}

/// 旧 `ParseForSecurityResult` 中授权链实际读取的三种结局。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BashParseOutcome {
    /// 解析成功（`ParseForSecurityResult.Parsed`）。
    Parsed,
    /// 解析过于复杂，降级为 GUARDED（`TooComplex`，`nodeType != "command-blacklist-deny"`）。
    TooComplex {
        /// `TooComplex#reason()`，仅进日志。
        reason: String,
    },
    /// 命令黑名单绝对拒绝（`TooComplex` 且 `nodeType == "command-blacklist-deny"`）。
    ///
    /// 这是 `ABSOLUTE_DENY` 不变量的入口：授权链据此抛
    /// `COMMAND_ABSOLUTELY_DENIED`，走投无路不可绕过。
    BlacklistDeny {
        /// `TooComplex#reason()`，作为拒绝原因回传用户。
        reason: String,
    },
}

/// Shell 状态端口（旧 `ShellStateManager`；实现位于 zk-tools）。
pub trait ShellStatePort: Send + Sync {
    /// `resolveWorkingDirectory(sessionId, configured)`。
    fn resolve_working_directory(&self, session_id: &str, configured: &str) -> String;

    /// `authorizationEnvironmentFacts(inherited)`：继承变量有效值 + PATH 指纹事实。
    fn authorization_environment_facts(&self, inherited: &[String]) -> serde_json::Value;
}

/// 恒等 Shell 状态端口（无 cwd 记忆、无环境事实）。
#[derive(Debug, Clone, Copy, Default)]
pub struct StatelessShellState;

impl ShellStatePort for StatelessShellState {
    fn resolve_working_directory(&self, _session_id: &str, configured: &str) -> String {
        configured.to_owned()
    }

    fn authorization_environment_facts(&self, _inherited: &[String]) -> serde_json::Value {
        serde_json::Value::Object(serde_json::Map::new())
    }
}

/// 敏感数据过滤端口（旧 `SensitiveDataFilter#filter`）。
pub trait SensitiveDataFilterPort: Send + Sync {
    /// 过滤后的可展示文本。
    fn filter(&self, value: &str) -> String;
}

/// 直通过滤器（旧源无匹配规则时的等价行为）。
#[derive(Debug, Clone, Copy, Default)]
pub struct PassthroughFilter;

impl SensitiveDataFilterPort for PassthroughFilter {
    fn filter(&self, value: &str) -> String {
        value.to_owned()
    }
}

/// 产物发布快照（旧 `ArtifactPublicationPolicy.Snapshot`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationSnapshot {
    /// 工作区相对路径。
    pub relative_path: String,
    /// 字节数。
    pub size: i64,
    /// 内容 SHA-256。
    pub sha256: String,
    /// OSS bucket。
    pub bucket: String,
    /// 公网 URL。
    pub public_url: String,
    /// OSS endpoint（进 `endpoints` 前需脱敏）。
    pub endpoint: String,
    /// 参与 `operationHash` 的授权事实。
    pub authorization_facts: serde_json::Value,
}

/// 产物发布策略端口（旧 `ArtifactPublicationPolicy#inspect`）。
pub trait ArtifactPublicationPort: Send + Sync {
    /// 检查发布请求；拒绝时返回 `(code, message)`。
    ///
    /// # Errors
    /// 策略拒绝（不在允许目录 / 超出大小上限 / 未配置 OSS 等）时返回
    /// `(code, message)`，由 zk-server 适配器映射自旧源的三类 Java 异常。
    fn inspect(
        &self,
        file_path: &str,
        current_run_id: Option<&str>,
    ) -> Result<PublicationSnapshot, (String, String)>;
}

/// 会话权限模式提供者（旧 `PermissionModeManager#getMode`）。
pub trait ModeProvider: Send + Sync {
    /// 返回 root session 的当前权限模式；未设置时旧源返回 `DEFAULT`。
    fn mode(&self, root_session_id: &str) -> PermissionMode;
}

/// 永远返回 `DEFAULT` 的模式提供者（旧源未设置模式时的等价行为）。
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultModeProvider;

impl ModeProvider for DefaultModeProvider {
    fn mode(&self, _root_session_id: &str) -> PermissionMode {
        PermissionMode::Default
    }
}

/// Run 事件日志写入端口（旧 `RunControlService`）。
pub trait RunEventSink: Send + Sync {
    /// 有界写入一条 Run 事件（旧 `appendEventBounded`）。
    ///
    /// 旧源在诊断写入失败时只记日志、不抛出（`recordFinalDenial`），因此本方法
    /// 返回 `()` 而非 `Result`——实现方自行吞掉并记录失败。
    fn append_event(
        &self,
        run_id: &str,
        event_type: &str,
        tool_use_id: Option<&str>,
        payload: &serde_json::Value,
    );

    /// 在**调用方已持有的写事务**内追加一条 Run 事件（旧
    /// `appendEventInCurrentWrite`，`ToolExecutionGateway.java:62`）。
    ///
    /// 旧源把授权记录复检、准入动作、`tool_started` 事件三者放进同一个
    /// `executeBoundedWrite` 短事务：三者必须同生共死，否则会出现「事件已落库
    /// 但授权已被撤销」的窗口。Rust 侧无法用线程局部事务隐式传递，故把连接显式
    /// 下传——实现方**禁止**在此方法内另开事务。
    fn append_event_in_current_write(
        &self,
        conn: &rusqlite::Connection,
        run_id: &str,
        event_type: &str,
        tool_use_id: Option<&str>,
        payload: &serde_json::Value,
    );
}

/// 丢弃全部事件的 sink（单元测试与嵌入式调用用）。
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopRunEventSink;

impl RunEventSink for NoopRunEventSink {
    fn append_event(
        &self,
        _run_id: &str,
        _event_type: &str,
        _tool_use_id: Option<&str>,
        _payload: &serde_json::Value,
    ) {
    }

    fn append_event_in_current_write(
        &self,
        _conn: &rusqlite::Connection,
        _run_id: &str,
        _event_type: &str,
        _tool_use_id: Option<&str>,
        _payload: &serde_json::Value,
    ) {
    }
}

/// 工作区文件域信任探针（旧 `ProjectWorkspaceService#isTrustedFileScope`）。
pub trait WorkspaceTrustProbe: Send + Sync {
    /// 该授权根是否已由用户显式选定为 Project（持久化的有界文件域授权）。
    fn is_trusted_file_scope(&self, authorization_root: &Path) -> bool;

    /// [`Self::is_trusted_file_scope`] 的**当前写事务**变体：复用调用方已持有的
    /// 连接在同一事务内重验（旧源 `isTrustedFileScope` 走同线程 JDBC 事务上下文，
    /// 天然复用连接）。
    ///
    /// `AuthorizationService::final_grant_recheck_in_current_transaction` 在
    /// `Db::with_writer` 闭包内执行，writer `Mutex` 不可重入——实现方**必须**用
    /// 传入的 `conn` 查询，禁止另取连接或调用任何 `*_blocking` / `with_writer`
    /// DB 出口（重入即死锁）。
    fn is_trusted_file_scope_in_current_write(
        &self,
        conn: &rusqlite::Connection,
        authorization_root: &Path,
    ) -> bool;
}

/// 恒不信任的探针（无 Project 选定时的等价行为）。
#[derive(Debug, Clone, Copy, Default)]
pub struct UntrustedWorkspaceProbe;

impl WorkspaceTrustProbe for UntrustedWorkspaceProbe {
    fn is_trusted_file_scope(&self, _authorization_root: &Path) -> bool {
        false
    }

    fn is_trusted_file_scope_in_current_write(
        &self,
        _conn: &rusqlite::Connection,
        _authorization_root: &Path,
    ) -> bool {
        false
    }
}
