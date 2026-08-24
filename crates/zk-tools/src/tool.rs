//! `Tool` trait 与执行上下文——工具子系统的对象安全核心抽象。
//!
//! 对照旧 `tool/Tool.java`（name / description / inputSchema / execute）；
//! 超时常量对照旧 `BashTool.java` L51-54（`BASH_DEFAULT_TIMEOUT_MS = 120_000` /
//! `BASH_MAX_TIMEOUT_MS = 600_000`）。

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// 默认单工具执行超时（对照旧 `BASH_DEFAULT_TIMEOUT_MS = 120_000`）。
pub const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_mins(2);

/// 单工具执行超时硬上限（对照旧 `BASH_MAX_TIMEOUT_MS = 600_000`；
/// [`Tool::timeout`] 返回值超过此值时由执行器钳制）。
pub const MAX_TOOL_TIMEOUT: Duration = Duration::from_mins(10);

/// 工具规格三元组（对照旧 `ToolDefinition`：name / description / JSON Schema）。
///
/// 自持而不复用 `zk_llm::ToolSpec`：zk-tools 与 zk-llm 平级互不依赖
/// （依赖方向铁律），引擎侧完成同构转换。
#[derive(Clone, Debug, PartialEq)]
pub struct ToolSpec {
    /// 工具名（注册表键 / LLM function 名）。
    pub name: String,
    /// 工具描述（供 LLM 决策）。
    pub description: String,
    /// JSON Schema 入参定义。
    pub parameters: serde_json::Value,
}

/// MCP 工具的跨层授权身份。全部字段均为非秘密稳定元数据；配置摘要不得由调用方
/// 反解出 token/header。普通工具不提供该身份。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpToolIdentity {
    /// MCP server 稳定标识（当前为配置中的唯一 name）。
    pub server_id: String,
    /// MCP server 展示名。
    pub server_name: String,
    /// 远端原始工具名。
    pub tool_name: String,
    /// 能力注册表 ID（无注册表映射时为空）。
    pub capability_id: Option<String>,
    /// 资源范围（如 `mcp://server/domain`）。
    pub resource_scope: Option<String>,
    /// 功能域范围。
    pub domain_scope: Option<String>,
    /// 不含秘密的服务器配置 SHA-256。
    pub config_hash: String,
}

/// 工具执行结果（对照旧 `ToolResult`：content / isError / metadata）。
#[derive(Clone, Debug, PartialEq)]
pub struct ToolOutput {
    /// 结果文本（超限由执行器截断）。
    pub content: String,
    /// 是否出错。
    pub is_error: bool,
    /// 结构化元数据（引擎侧仅透传 `structuredResult` 键，对照旧
    /// `structuredResultMetadata` 过滤语义）。
    pub metadata: Option<serde_json::Value>,
}

impl ToolOutput {
    /// 构造成功结果。
    #[must_use]
    pub fn ok(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            is_error: false,
            metadata: None,
        }
    }

    /// 构造错误结果。
    #[must_use]
    pub fn error(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            is_error: true,
            metadata: None,
        }
    }
}

/// 工具执行上下文——取消令牌（三层树的 `tool_call` 层）+ 进度通道 +
/// 环境三元组（工作目录 / 会话 ID / 工具调用 ID）。
///
/// 环境三元组对照旧 `ToolUseContext.java`（`workingDirectory` / `sessionId` /
/// `toolUseId`；其余 11 个字段分属权限管线 / 子代理 / 后台进程域，归后续
/// 子阶段）。2.3 仅**追加**字段与访问器，[`Self::new`] 签名不变：未显式注入
/// 时 `working_dir` = 进程当前目录、`session_id` / `tool_use_id` = `None`。
#[derive(Clone, Debug)]
pub struct ToolContext {
    /// 本次调用的取消令牌（run 令牌的 child；工具实现应在长操作中协作检查）。
    pub cancel: CancellationToken,
    progress: mpsc::UnboundedSender<String>,
    working_dir: PathBuf,
    session_id: Option<String>,
    tool_use_id: Option<String>,
    run_id: Option<String>,
}

impl ToolContext {
    /// 装配上下文（执行器内部构造；测试可直构）。
    ///
    /// `working_dir` 取进程当前目录（取不到时回落 `.`），`session_id` /
    /// `tool_use_id` 为 `None`；按需以 [`Self::with_working_dir`] /
    /// [`Self::with_session_id`] / [`Self::with_tool_use_id`] 覆盖。
    #[must_use]
    pub fn new(cancel: CancellationToken, progress: mpsc::UnboundedSender<String>) -> Self {
        Self {
            cancel,
            progress,
            working_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            session_id: None,
            tool_use_id: None,
            run_id: None,
        }
    }

    /// 指定工作目录（对照旧 `ToolUseContext.workingDirectory`）。
    #[must_use]
    pub fn with_working_dir(mut self, working_dir: impl Into<PathBuf>) -> Self {
        self.working_dir = working_dir.into();
        self
    }

    /// 指定会话 ID（快照落库等会话维度副作用的归属键）。
    #[must_use]
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    /// 指定工具调用 ID（快照 `message_id` 列的写入值，对照旧
    /// `trackAppliedEdit(…, context.toolUseId(), …)`）。
    #[must_use]
    pub fn with_tool_use_id(mut self, tool_use_id: impl Into<String>) -> Self {
        self.tool_use_id = Some(tool_use_id.into());
        self
    }

    /// 工作目录（相对路径入参的解析基准）。
    #[must_use]
    pub fn working_dir(&self) -> &Path {
        &self.working_dir
    }

    /// 会话 ID（未注入时 `None`——快照等会话维度副作用应静默跳过）。
    #[must_use]
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// 工具调用 ID（未注入时 `None`）。
    #[must_use]
    pub fn tool_use_id(&self) -> Option<&str> {
        self.tool_use_id.as_deref()
    }

    /// 指定 Run ID（持久交互的归属 Run，对照旧 `ToolUseContext.currentRunId`）。
    ///
    /// 2.4 追加：`AskUserQuestion` 建 `ELICITATION` 交互必须携带 Run，缺失即被
    /// 持久交互服务以 `INTERACTION_REQUIRES_RUN` 拒绝。
    #[must_use]
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Run ID（未注入时 `None`，对照旧 `currentRunId` 为 null 的场景）。
    #[must_use]
    pub fn run_id(&self) -> Option<&str> {
        self.run_id.as_deref()
    }

    /// 上报执行进度（stdout 增量语义，映射下行 `tool_use_progress`）；
    /// 接收端关闭时静默丢弃（进度为尽力而为，不阻断执行）。
    pub fn report_progress(&self, text: impl Into<String>) {
        let _ = self.progress.send(text.into());
    }
}

/// 工具抽象（object-safe：注册表持有 `Arc<dyn Tool>`）。
///
/// 对照旧 `Tool.java` 接口形状；`execute` 返回 [`BoxFuture`] 而非
/// `async fn`（对象安全，形态与 zk-llm `ChatProvider` D-S6-1 裁决一致）。
pub trait Tool: Send + Sync {
    /// 工具名（注册表键 / LLM function 名）。
    fn name(&self) -> &str;

    /// 工具描述（供 LLM 决策）。
    fn description(&self) -> &str;

    /// JSON Schema 入参定义。
    fn parameters(&self) -> serde_json::Value;

    /// 本工具的执行超时（默认 [`DEFAULT_TOOL_TIMEOUT`]；执行器按
    /// [`MAX_TOOL_TIMEOUT`] 钳制上限）。
    fn timeout(&self) -> Duration {
        DEFAULT_TOOL_TIMEOUT
    }

    /// 执行工具（入参为 LLM 产出的 JSON；入参校验由实现自担，校验失败
    /// 返回 `is_error` 结果而非 panic）。
    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput>;

    /// 是否为破坏性调用（旧 `Tool.java:122` `default boolean isDestructive` → `false`）。
    ///
    /// 2.5 授权链的 `BashAnalyzer` 据此判 `HIGH`；实现方按入参内容动态回答。
    fn is_destructive(&self, _input: &serde_json::Value) -> bool {
        false
    }

    /// 是否为只读调用（旧 `Tool.java:128` `default boolean isReadOnly` → `false`）。
    ///
    /// 2.5 授权链据此判 `SAFE` 与 effects 集合。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        false
    }

    /// 工具自报的路径入参（旧 `Tool.java:138` `default String getPath` → `null`）。
    ///
    /// 2.5 的 `FileAnalyzer` 优先采信本值，其次才按字段名猜测。
    fn path_of(&self, _input: &serde_json::Value) -> Option<String> {
        None
    }

    /// MCP 专属授权身份；禁止仅凭 `mcp__` 名字前缀推断信任域。
    fn mcp_identity(&self) -> Option<&McpToolIdentity> {
        None
    }

    /// 导出规格（供注册表聚合下发 LLM tools 参数）。
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name().to_owned(),
            description: self.description().to_owned(),
            parameters: self.parameters(),
        }
    }
}
