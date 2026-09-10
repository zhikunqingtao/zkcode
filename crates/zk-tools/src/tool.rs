//! `Tool` trait 与执行上下文——工具子系统的对象安全核心抽象。
//!
//! 对照旧 `tool/Tool.java`（name / description / inputSchema / execute）；
//! 超时常量对照旧 `BashTool.java` L51-54（`BASH_DEFAULT_TIMEOUT_MS = 120_000` /
//! `BASH_MAX_TIMEOUT_MS = 600_000`）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::executor::ExecutionOwnerRegistry;

/// Explicit child-Agent exposure policy for runtime-discovered tools.
///
/// Dynamic tools are denied by default. A concrete adapter may opt in only
/// after trusted local configuration classifies its effect boundary; remote
/// names and descriptions alone never grant child access.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ChildToolAccess {
    /// Never expose this dynamic capability to a child Agent.
    #[default]
    Denied,
    /// Expose in the default read-only child directory.
    ReadOnly,
    /// Expose only when the separately gated child-write capability is active.
    WriteGated,
}

/// Stable ownership attached to every physical resource created by a tool.
/// The three IDs have already been committed by `TaskRuntime` before a process
/// is allowed to spawn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionResourceOwner {
    /// Logical task that owns the physical execution.
    pub task_id: String,
    /// Physical run attempt that owns the physical execution.
    pub run_id: String,
    /// Durable tool invocation that spawned the resource.
    pub invocation_id: String,
}

/// A physical resource allocation reported by a tool implementation.
#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionResourceAllocation {
    /// Full UUID v4 allocated before the registration write begins.
    pub resource_id: String,
    /// Closed resource-kind vocabulary from the database contract.
    pub resource_kind: String,
    /// Operating-system or transport identifier (for example a process-group ID).
    pub external_id: Option<String>,
    /// Non-secret diagnostic metadata.
    pub metadata: serde_json::Value,
}

/// Handle retained until a physical resource reaches a cleanup terminal state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionResourceLease {
    /// Stable resource UUID.
    pub resource_id: String,
}

/// Physical cleanup result. `Released` is used only after positive operating-
/// system confirmation; every ambiguous path is permanently `Unconfirmed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionResourceTerminal {
    /// The resource is proven absent/reclaimed.
    Released,
    /// Cleanup could not be proven inside the bounded cleanup window.
    Unconfirmed,
}

/// Aggregate cleanup state surfaced with the terminal tool event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolCleanupStatus {
    /// The invocation did not allocate a supervised physical resource.
    NotRequired,
    /// At least one resource has not reached a terminal cleanup state.
    Pending,
    /// Every allocated resource was positively released.
    Confirmed,
    /// At least one resource could not be positively released.
    Unconfirmed,
}

/// Dependency-inverted persistence port for physical execution resources.
///
/// `zk-tools` owns process lifetime but must not depend on `zk-db`; the engine
/// injects an implementation backed by the same database that owns Task/Run/
/// tool-invocation state. Futures are `'static` so cleanup can continue in a
/// detached supervisor task even when its caller future is dropped.
pub trait ExecutionResourceObserver: Send + Sync {
    /// Persist an allocation before returning control to the running process.
    fn register(
        &self,
        owner: ExecutionResourceOwner,
        allocation: ExecutionResourceAllocation,
    ) -> BoxFuture<'static, Result<ExecutionResourceLease, String>>;

    /// Bind the operating-system/transport identifier discovered only after a
    /// durable reservation has been committed.
    fn bind_external(
        &self,
        lease: ExecutionResourceLease,
        external_id: String,
    ) -> BoxFuture<'static, Result<(), String>>;

    /// Persist the immutable cleanup terminal state.
    fn finish(
        &self,
        lease: ExecutionResourceLease,
        terminal: ExecutionResourceTerminal,
    ) -> BoxFuture<'static, Result<(), String>>;
}

#[derive(Clone)]
struct ExecutionResourceBinding {
    owner: ExecutionResourceOwner,
    observer: Arc<dyn ExecutionResourceObserver>,
    tracker: Arc<ExecutionResourceTracker>,
}

#[derive(Default)]
struct ExecutionResourceTracker {
    leases: Mutex<HashMap<String, ExecutionResourceLease>>,
    saw_resource: std::sync::atomic::AtomicBool,
    unconfirmed: std::sync::atomic::AtomicBool,
}

impl ExecutionResourceTracker {
    fn insert(&self, lease: ExecutionResourceLease) {
        self.saw_resource
            .store(true, std::sync::atomic::Ordering::Release);
        self.leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(lease.resource_id.clone(), lease);
    }

    fn complete(&self, resource_id: &str, confirmed: bool) {
        self.leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(resource_id);
        if !confirmed {
            self.unconfirmed
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    fn status(&self) -> ToolCleanupStatus {
        if self.unconfirmed.load(std::sync::atomic::Ordering::Acquire) {
            return ToolCleanupStatus::Unconfirmed;
        }
        if !self
            .leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
        {
            return ToolCleanupStatus::Pending;
        }
        if self.saw_resource.load(std::sync::atomic::Ordering::Acquire) {
            ToolCleanupStatus::Confirmed
        } else {
            ToolCleanupStatus::NotRequired
        }
    }

    fn drain_pending_as_unconfirmed(&self) -> Vec<ExecutionResourceLease> {
        let mut leases = self
            .leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !leases.is_empty() {
            self.unconfirmed
                .store(true, std::sync::atomic::Ordering::Release);
        }
        leases.drain().map(|(_, lease)| lease).collect()
    }
}

/// 默认单工具执行超时（对照旧 `BASH_DEFAULT_TIMEOUT_MS = 120_000`）。
pub const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_mins(2);

/// 单工具执行超时硬上限（对照旧 `BASH_MAX_TIMEOUT_MS = 600_000`；
/// [`Tool::timeout`] 返回值超过此值时由执行器钳制）。
pub const MAX_TOOL_TIMEOUT: Duration = Duration::from_mins(10);

/// Trusted timeout ownership; runtime-managed tasks have their own durable deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolTimeoutPolicy {
    /// The executor applies the ordinary leaf-tool hard limit.
    Executor,
    /// `TaskRuntime` owns the deadline; the executor only supplies a watchdog.
    TaskRuntime,
}

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

/// Immutable receipt emitted by a built-in file tool after its atomic write has
/// been applied and verified.  The receipt is execution data, not an artifact
/// ledger: `zk-engine` consumes it exactly once and persists the authoritative
/// record in `SQLite`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileArtifactReceipt {
    /// Canonical absolute path observed immediately after the write.
    pub canonical_path: String,
    /// `created` or `modified`.
    pub operation: String,
    /// SHA-256 of the exact bytes committed by the atomic writer.
    pub sealed_hash: String,
    /// Byte count of the exact content passed to the atomic writer.
    pub file_size: u64,
}

impl FileArtifactReceipt {
    /// Build a receipt for a successfully applied regular-file write.
    ///
    /// `None` deliberately fails closed: the engine treats a successful built-in
    /// file tool without a valid receipt as an artifact-registration failure.
    pub async fn capture(
        path: &Path,
        operation: &str,
        sealed_hash: Option<&str>,
        file_size: usize,
    ) -> Option<Self> {
        if !matches!(operation, "created" | "modified") {
            return None;
        }
        let sealed_hash = sealed_hash?;
        if sealed_hash.len() != 64 || !sealed_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let sealed_hash = sealed_hash.to_ascii_lowercase();
        let metadata = tokio::fs::symlink_metadata(path).await.ok()?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return None;
        }
        let file_size = u64::try_from(file_size).ok()?;
        if metadata.len() != file_size {
            return None;
        }
        let canonical_path = tokio::fs::canonicalize(path)
            .await
            .ok()?
            .to_string_lossy()
            .into_owned();
        Some(Self {
            canonical_path,
            operation: operation.to_owned(),
            sealed_hash,
            file_size,
        })
    }
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

    /// Decode the standardized built-in file-write receipt, when present.
    #[must_use]
    pub fn file_artifact_receipt(&self) -> Option<FileArtifactReceipt> {
        let value = self
            .metadata
            .as_ref()?
            .get("structuredResult")?
            .get("artifact")?
            .clone();
        serde_json::from_value(value).ok()
    }
}

/// 工具执行上下文——取消令牌（三层树的 `tool_call` 层）+ 进度通道 +
/// 环境三元组（工作目录 / 会话 ID / 工具调用 ID）。
///
/// 环境三元组对照旧 `ToolUseContext.java`（`workingDirectory` / `sessionId` /
/// `toolUseId`；其余 11 个字段分属权限管线 / 子代理 / 后台进程域，归后续
/// 子阶段）。2.3 仅**追加**字段与访问器，[`Self::new`] 签名不变：未显式注入
/// 时 `working_dir` = 进程当前目录、`session_id` / `tool_use_id` = `None`。
#[derive(Clone)]
pub struct ToolContext {
    /// 本次调用的取消令牌（run 令牌的 child；工具实现应在长操作中协作检查）。
    pub cancel: CancellationToken,
    progress: ProgressSender,
    working_dir: PathBuf,
    session_id: Option<String>,
    tool_use_id: Option<String>,
    run_id: Option<String>,
    /// Canonical file target bound by the authorization decision for this
    /// invocation. Built-in writers compare this identity again immediately
    /// before rename so a post-authorization path swap cannot redirect writes.
    authorized_write_path: Option<PathBuf>,
    execution_resources: Option<ExecutionResourceBinding>,
    execution_owners: Option<ExecutionOwnerRegistry>,
    /// Invocation-scoped directory snapshot.  A filtered child registry puts
    /// only its effective capabilities here, so discovery tools cannot expose
    /// names that the caller is not authorized to execute.
    tool_catalog: Option<Arc<Vec<ToolSpec>>>,
}

#[derive(Clone, Debug)]
enum ProgressSender {
    Unbounded(mpsc::UnboundedSender<String>),
    Bounded(mpsc::Sender<String>),
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
            progress: ProgressSender::Unbounded(progress),
            working_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            session_id: None,
            tool_use_id: None,
            run_id: None,
            authorized_write_path: None,
            execution_resources: None,
            execution_owners: None,
            tool_catalog: None,
        }
    }

    /// Construct a context whose progress queue is bounded. When a producer
    /// outruns the consumer, intermediate progress is intentionally dropped;
    /// the tool result is delivered on a separate executor path and is never
    /// dropped with progress.
    #[must_use]
    pub fn with_bounded_progress(
        cancel: CancellationToken,
        progress: mpsc::Sender<String>,
    ) -> Self {
        Self {
            cancel,
            progress: ProgressSender::Bounded(progress),
            working_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            session_id: None,
            tool_use_id: None,
            run_id: None,
            authorized_write_path: None,
            execution_resources: None,
            execution_owners: None,
            tool_catalog: None,
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

    /// Bind the exact path approved by the authorization pipeline.
    #[must_use]
    pub fn with_authorized_write_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.authorized_write_path = Some(path.into());
        self
    }

    /// Exact path approved for this invocation, when it is a file writer.
    #[must_use]
    pub fn authorized_write_path(&self) -> Option<&Path> {
        self.authorized_write_path.as_deref()
    }

    /// Bind the exact effective tool-directory snapshot for this invocation.
    #[must_use]
    pub fn with_tool_catalog(mut self, catalog: Arc<Vec<ToolSpec>>) -> Self {
        self.tool_catalog = Some(catalog);
        self
    }

    /// Effective tool-directory snapshot visible to discovery tools.
    #[must_use]
    pub fn tool_catalog(&self) -> Option<&[ToolSpec]> {
        self.tool_catalog.as_deref().map(Vec::as_slice)
    }

    /// Attach the durable owner and observer used by process/MCP resource
    /// supervisors. This is injected only after the tool invocation is stored.
    #[must_use]
    pub fn with_execution_resources(
        mut self,
        owner: ExecutionResourceOwner,
        observer: Arc<dyn ExecutionResourceObserver>,
    ) -> Self {
        self.execution_resources = Some(ExecutionResourceBinding {
            owner,
            observer,
            tracker: Arc::new(ExecutionResourceTracker::default()),
        });
        self
    }

    pub(crate) fn with_execution_owner_registry(mut self, owners: ExecutionOwnerRegistry) -> Self {
        self.execution_owners = Some(owners);
        self
    }

    pub(crate) fn execution_owner_ready(&self) -> Result<(), String> {
        match self.execution_owners.as_ref() {
            Some(owners) if owners.accepts_new_execution() => Ok(()),
            Some(_) => Err("EXECUTION_SUPERVISOR_SHUTTING_DOWN".to_owned()),
            None if self.run_id.is_some() => Err("EXECUTION_TASK_SUPERVISOR_REQUIRED".to_owned()),
            None => Ok(()),
        }
    }

    pub(crate) fn spawn_owned_execution(
        &self,
        future: BoxFuture<'static, ()>,
    ) -> Result<(), String> {
        if let Some(owners) = self.execution_owners.as_ref() {
            return owners
                .spawn_owned(self.cancel.clone(), future)
                .map_err(str::to_owned);
        }
        if self.run_id.is_some() {
            return Err("EXECUTION_TASK_SUPERVISOR_REQUIRED".to_owned());
        }
        tokio::spawn(future);
        Ok(())
    }

    /// Persist a newly allocated physical resource. The lease is tracked before
    /// the async write starts so cancellation at the commit boundary remains an
    /// explicit unconfirmed cleanup responsibility.
    ///
    /// # Errors
    ///
    /// Returns an error when a runtime-owned invocation has no durable observer,
    /// registration fails, or the observer returns a mismatched resource ID.
    pub async fn register_execution_resource(
        &self,
        resource_kind: impl Into<String>,
        external_id: Option<String>,
        metadata: serde_json::Value,
    ) -> Result<Option<ExecutionResourceLease>, String> {
        let Some(binding) = self.execution_resources.as_ref() else {
            // A durable Run context without a supervisor would create an
            // unowned process. Keep standalone unit/CLI uses (no run_id)
            // available, but fail closed for every runtime-owned invocation.
            if self.run_id.is_some() {
                return Err("EXECUTION_RESOURCE_OBSERVER_REQUIRED".to_owned());
            }
            return Ok(None);
        };
        let lease = ExecutionResourceLease {
            resource_id: uuid::Uuid::new_v4().to_string(),
        };
        binding.tracker.insert(lease.clone());
        let allocation = ExecutionResourceAllocation {
            resource_id: lease.resource_id.clone(),
            resource_kind: resource_kind.into(),
            external_id,
            metadata,
        };
        match binding
            .observer
            .register(binding.owner.clone(), allocation)
            .await
        {
            Ok(registered) if registered.resource_id == lease.resource_id => Ok(Some(registered)),
            Ok(registered) => {
                binding.tracker.complete(&lease.resource_id, false);
                let _ = binding
                    .observer
                    .finish(registered, ExecutionResourceTerminal::Unconfirmed)
                    .await;
                Err("EXECUTION_RESOURCE_ID_MISMATCH".to_owned())
            }
            Err(error) => {
                // Registration may have committed immediately before its waiter
                // was cancelled. Persist the conservative terminal by stable ID.
                let _ = binding
                    .observer
                    .finish(lease.clone(), ExecutionResourceTerminal::Unconfirmed)
                    .await;
                binding.tracker.complete(&lease.resource_id, false);
                Err(format!("EXECUTION_RESOURCE_REGISTER_FAILED: {error}"))
            }
        }
    }

    /// Persist resource cleanup. A failed `Released` write is immediately
    /// retried as `Unconfirmed`; local aggregate state never reports confirmed
    /// unless the durable observer accepted `Released`.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable observer cannot persist the requested
    /// terminal cleanup state.
    pub async fn finish_execution_resource(
        &self,
        lease: ExecutionResourceLease,
        terminal: ExecutionResourceTerminal,
    ) -> Result<(), String> {
        let Some(binding) = self.execution_resources.as_ref() else {
            return Ok(());
        };
        match binding.observer.finish(lease.clone(), terminal).await {
            Ok(()) => {
                binding.tracker.complete(
                    &lease.resource_id,
                    terminal == ExecutionResourceTerminal::Released,
                );
                Ok(())
            }
            Err(error) => {
                if terminal == ExecutionResourceTerminal::Released {
                    let _ = binding
                        .observer
                        .finish(lease.clone(), ExecutionResourceTerminal::Unconfirmed)
                        .await;
                }
                binding.tracker.complete(&lease.resource_id, false);
                Err(format!("EXECUTION_RESOURCE_FINISH_FAILED: {error}"))
            }
        }
    }

    /// Attach an external process/transport identity to a reservation which was
    /// durably allocated before the physical resource could be created.
    ///
    /// # Errors
    ///
    /// Returns an error when the durable observer cannot bind the external ID.
    pub async fn bind_execution_resource_external(
        &self,
        lease: &ExecutionResourceLease,
        external_id: String,
    ) -> Result<(), String> {
        let Some(binding) = self.execution_resources.as_ref() else {
            return Ok(());
        };
        binding
            .observer
            .bind_external(lease.clone(), external_id)
            .await
            .map_err(|error| format!("EXECUTION_RESOURCE_BIND_FAILED: {error}"))
    }

    /// Current aggregate cleanup status for the invocation.
    #[must_use]
    pub fn execution_cleanup_status(&self) -> ToolCleanupStatus {
        self.execution_resources
            .as_ref()
            .map_or(ToolCleanupStatus::NotRequired, |binding| {
                binding.tracker.status()
            })
    }

    /// Close every still-owned lease as unconfirmed. The executor invokes this
    /// before abandoning a tool future after the bounded cleanup deadline.
    pub async fn force_unconfirmed_execution_resources(&self) {
        let Some(binding) = self.execution_resources.as_ref() else {
            return;
        };
        for lease in binding.tracker.drain_pending_as_unconfirmed() {
            if let Err(error) = binding
                .observer
                .finish(lease, ExecutionResourceTerminal::Unconfirmed)
                .await
            {
                tracing::error!(%error, "failed to persist unconfirmed execution resource");
            }
        }
    }

    /// 上报执行进度（stdout 增量语义，映射下行 `tool_use_progress`）；
    /// 接收端关闭时静默丢弃（进度为尽力而为，不阻断执行）。
    pub fn report_progress(&self, text: impl Into<String>) {
        let text = text.into();
        match &self.progress {
            ProgressSender::Unbounded(sender) => {
                let _ = sender.send(text);
            }
            ProgressSender::Bounded(sender) => {
                let _ = sender.try_send(text);
            }
        }
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

    /// Select the trusted owner of this tool's execution deadline.
    fn timeout_policy(&self) -> ToolTimeoutPolicy {
        ToolTimeoutPolicy::Executor
    }

    /// Whether this invocation consumes one of the bounded leaf execution slots.
    /// Durable orchestration and wait tools return `false`: they coordinate other
    /// work but do not themselves run a process/provider/MCP leaf operation.
    fn uses_execution_slot(&self) -> bool {
        true
    }

    /// Trusted local policy for exposing a runtime-discovered tool to child
    /// Agents. This is deliberately not inferred from a tool name or from
    /// untrusted remote metadata.
    fn child_access(&self) -> ChildToolAccess {
        ChildToolAccess::Denied
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

    /// Connection generation that owns this tool instance, when the tool is
    /// backed by a reconnectable transport such as MCP.
    fn connection_generation(&self) -> Option<u64> {
        None
    }

    /// Revalidate a previously captured connection generation immediately
    /// before execution.  Non-transport tools have no such revocation edge.
    fn is_connection_generation_current(&self, generation: u64) -> bool {
        self.connection_generation() == Some(generation)
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
