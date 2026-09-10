//! 受控工具执行器——并发上限 / 超时 / 取消 / 输出截断。
//!
//! 对照旧 `ToolExecutionPipeline.java` 语义骨架：每工具调用一
//! `tokio::spawn`；全局 `Semaphore(16)`（对照旧
//! `process.runner.max-concurrent`，`ManagedProcessRunner.java` L43）；
//! 超时默认 120s / 上限 600s（`BashTool.java` L51-54）；输出上限 1 MiB。
//!
//! # 取消语义（对齐 D-S6-5 的静默终止族）
//!
//! 取消（run 令牌 → 本调用 child 令牌）后任务直接退出、**不产出**
//! [`ToolEvent::Finished`]——事件通道随之关闭，消费方以「通道关闭且无
//! Finished」判定中断，由引擎按旧 FIX-02 语义合成
//! `<tool_use_error>Interrupted by user</tool_use_error>` 结果。
//!
//! Potential workspace writes additionally acquire a process-wide canonical
//! root lease after safety admission and before `Tool::execute`: CAS-backed
//! file writes share the lease, while broad or unknown mutations are exclusive.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::Duration;

use dashmap::DashMap;
use futures::{FutureExt, future::BoxFuture};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::tool::{
    ExecutionResourceObserver, ExecutionResourceOwner, MAX_TOOL_TIMEOUT, Tool, ToolCleanupStatus,
    ToolContext, ToolOutput, ToolSpec,
};
use crate::workspace_lease::{
    WorkspaceLeaseError, WorkspaceLeaseGuard, WorkspaceLeaseManager, WorkspaceLeaseMode,
};

/// 全局并发上限（对照旧 `process.runner.max-concurrent` 默认 16）。
pub const MAX_CONCURRENT_TOOLS: usize = 16;

/// 单工具输出采集上限（1 MiB；超限截断并追加标记）。
pub const MAX_TOOL_OUTPUT_BYTES: usize = 1024 * 1024;

/// Progress is advisory and may be coalesced/dropped under pressure. Terminal
/// results use the same bounded receiver but are sent with backpressure after
/// progress producers have stopped, so they cannot be lost behind an
/// unbounded stdout stream.
pub const TOOL_EVENT_QUEUE_CAPACITY: usize = 128;

/// The managed process path needs at most TERM(5s) plus a short KILL/pipe
/// collection window. Cancellation retains ownership of the tool future for
/// this whole interval instead of dropping it at the cancellation boundary.
pub const TOOL_CLEANUP_GRACE: Duration = Duration::from_secs(9);

/// Result of closing the leaf-execution intake and waiting for every owned
/// tool/process cleanup future to reach a terminal boundary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolExecutorShutdownReport {
    /// Number of physical owners which existed when shutdown began.
    pub owners_requested: usize,
    /// Number of physical owners still retained after the supplied deadline.
    pub owners_remaining: usize,
    /// True only when every retained owned future reached its completion boundary.
    pub drained: bool,
}

struct OwnedExecution {
    cancel: CancellationToken,
    driver: Mutex<Option<JoinHandle<()>>>,
}

struct ExecutionOwnerRegistryInner {
    accepting: AtomicBool,
    intake_gate: RwLock<()>,
    active: DashMap<String, Arc<OwnedExecution>>,
    changed: Notify,
}

/// Shared process-local ownership for leaf tools and their nested physical
/// resources. Every clone of one [`ToolExecutor`] points at the same registry.
#[derive(Clone)]
pub(crate) struct ExecutionOwnerRegistry {
    inner: Arc<ExecutionOwnerRegistryInner>,
}

impl ExecutionOwnerRegistry {
    fn new() -> Self {
        Self {
            inner: Arc::new(ExecutionOwnerRegistryInner {
                accepting: AtomicBool::new(true),
                intake_gate: RwLock::new(()),
                active: DashMap::new(),
                changed: Notify::new(),
            }),
        }
    }

    pub(crate) fn accepts_new_execution(&self) -> bool {
        self.inner.accepting.load(Ordering::Acquire)
    }

    fn close_intake(&self) {
        self.inner.accepting.store(false, Ordering::Release);
        // A writer barrier guarantees that every caller which passed the first
        // accepting check has either installed its owner or failed before this
        // method returns.
        drop(
            self.inner
                .intake_gate
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }

    pub(crate) fn spawn_owned(
        &self,
        cancel: CancellationToken,
        future: BoxFuture<'static, ()>,
    ) -> Result<(), &'static str> {
        if !self.accepts_new_execution() {
            return Err("EXECUTION_SUPERVISOR_SHUTTING_DOWN");
        }
        let _intake = self
            .inner
            .intake_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.accepts_new_execution() {
            return Err("EXECUTION_SUPERVISOR_SHUTTING_DOWN");
        }

        let owner_id = uuid::Uuid::new_v4().to_string();
        let owner = Arc::new(OwnedExecution {
            cancel,
            driver: Mutex::new(None),
        });
        self.inner
            .active
            .insert(owner_id.clone(), Arc::clone(&owner));
        let weak = Arc::downgrade(&self.inner);
        let driver = tokio::spawn(async move {
            let outcome = std::panic::AssertUnwindSafe(future).catch_unwind().await;
            if let Some(inner) = Weak::upgrade(&weak) {
                inner.active.remove(&owner_id);
                inner.changed.notify_waiters();
            }
            if outcome.is_err() {
                tracing::error!("owned leaf execution panicked");
            }
        });
        *owner
            .driver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(driver);
        Ok(())
    }

    fn active_count(&self) -> usize {
        self.inner.active.len()
    }

    async fn shutdown(&self, grace: Duration) -> ToolExecutorShutdownReport {
        self.close_intake();
        let owners_requested = self.active_count();
        for owner in &self.inner.active {
            owner.cancel.cancel();
            // Reading the retained handle is intentional: this registry owns
            // every driver until the driver's completion boundary removes its
            // entry. A finished handle can race that final removal, but is
            // never mistaken for a missing owner.
            let _driver_finished = owner
                .driver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_some_and(JoinHandle::is_finished);
        }

        let deadline = tokio::time::Instant::now() + grace;
        loop {
            if self.inner.active.is_empty() || tokio::time::Instant::now() >= deadline {
                break;
            }
            let changed = self.inner.changed.notified();
            if self.inner.active.is_empty() {
                break;
            }
            let _ = tokio::time::timeout_at(deadline, changed).await;
        }
        let owners_remaining = self.active_count();
        ToolExecutorShutdownReport {
            owners_requested,
            owners_remaining,
            drained: owners_remaining == 0,
        }
    }
}

static GLOBAL_TOOL_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

fn global_tool_semaphore() -> Arc<Semaphore> {
    Arc::clone(GLOBAL_TOOL_SEMAPHORE.get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_TOOLS))))
}

/// 截断标记（追加于截断点之后）。
const TRUNCATION_MARKER: &str = "\n... [output truncated at 1MB]";

/// 工具执行事件（每调用一事件通道；Progress 零到多次 + Finished 恰一次；
/// 取消路径通道直接关闭、无 Finished）。
#[derive(Clone, Debug, PartialEq)]
pub enum ToolEvent {
    /// 执行进度（stdout 增量语义 → 下行 `tool_use_progress`）。
    Progress {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 进度文本。
        text: String,
    },
    /// 执行完成（含超时合成的错误结果）。
    Finished {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 执行结果（输出已按上限截断）。
        output: ToolOutput,
        /// Aggregate physical-resource cleanup state at tool completion.
        cleanup_status: ToolCleanupStatus,
    },
}

/// 单次调用的环境注入（2.3 追加）——工作目录 / 会话 ID。
///
/// 引擎侧按 run 维度构造（`session_id` 恒有值，`working_dir` 缺省时沿用
/// 进程当前目录）；[`ToolExecutor::spawn_call`] 等价于全默认环境，故
/// Phase 1/2.2 既有调用方零改动。`tool_use_id` 不在此列——执行器已持有
/// 该参数，直接注入上下文。
#[derive(Clone, Default)]
pub struct CallEnv {
    working_dir: Option<PathBuf>,
    session_id: Option<String>,
    run_id: Option<String>,
    authorized_write_path: Option<PathBuf>,
    capability_revocation: Option<CancellationToken>,
    resource_owner: Option<ExecutionResourceOwner>,
    resource_observer: Option<Arc<dyn ExecutionResourceObserver>>,
    tool_catalog: Option<Arc<Vec<ToolSpec>>>,
}

impl CallEnv {
    /// 空环境（等价 [`Default`]）。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 指定工作目录（相对路径入参的解析基准）。
    #[must_use]
    pub fn with_working_dir(mut self, working_dir: impl Into<PathBuf>) -> Self {
        self.working_dir = Some(working_dir.into());
        self
    }

    /// 指定会话 ID（写前快照等会话维度副作用的归属键）。
    #[must_use]
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    /// 工作目录字符串视图（2.5 授权链的 `workingDirectory` 事实来源；
    /// 非 UTF-8 路径返回 `None`——授权链据此退回配置默认值）。
    #[must_use]
    pub fn working_dir_str(&self) -> Option<&str> {
        self.working_dir
            .as_deref()
            .and_then(std::path::Path::to_str)
    }

    /// 会话 ID 视图（2.5 授权链的 root session 事实来源）。
    #[must_use]
    pub fn session_id_str(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// 指定 Run ID（持久交互的归属 Run；见
    /// [`ToolContext::with_run_id`]）。
    #[must_use]
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Bind the canonical file target returned by authorization.
    #[must_use]
    pub fn with_authorized_write_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.authorized_write_path = Some(path.into());
        self
    }

    /// Attach the exact dynamic-directory binding lifetime for this call.
    #[must_use]
    pub fn with_capability_revocation(mut self, token: CancellationToken) -> Self {
        self.capability_revocation = Some(token);
        self
    }

    /// Run ID 视图。
    #[must_use]
    pub fn run_id_str(&self) -> Option<&str> {
        self.run_id.as_deref()
    }

    /// Resolve the workspace root used for write-lease partitioning. Potential
    /// writes fail closed if the process current directory is unavailable.
    fn workspace_root(&self) -> Result<PathBuf, String> {
        self.working_dir.clone().map_or_else(
            || std::env::current_dir().map_err(|error| error.to_string()),
            Ok,
        )
    }

    /// Attach durable Task/Run/invocation ownership for physical resources.
    /// The invocation must already exist before this method is used.
    #[must_use]
    pub fn with_execution_resources(
        mut self,
        owner: ExecutionResourceOwner,
        observer: Arc<dyn ExecutionResourceObserver>,
    ) -> Self {
        self.resource_owner = Some(owner);
        self.resource_observer = Some(observer);
        self
    }

    /// Bind the invocation to the effective (already permission-filtered) tool
    /// directory.  `ToolSearch` consumes this snapshot instead of its global
    /// fallback catalog.
    #[must_use]
    pub fn with_tool_catalog(mut self, catalog: Vec<ToolSpec>) -> Self {
        self.tool_catalog = Some(Arc::new(catalog));
        self
    }

    /// 施加到上下文（缺省项保持 [`ToolContext::new`] 的默认值）。
    fn apply(self, mut ctx: ToolContext, owners: ExecutionOwnerRegistry) -> ToolContext {
        if let Some(working_dir) = self.working_dir {
            ctx = ctx.with_working_dir(working_dir);
        }
        if let Some(session_id) = self.session_id {
            ctx = ctx.with_session_id(session_id);
        }
        if let Some(run_id) = self.run_id {
            ctx = ctx.with_run_id(run_id);
        }
        if let Some(path) = self.authorized_write_path {
            ctx = ctx.with_authorized_write_path(path);
        }
        if let (Some(owner), Some(observer)) = (self.resource_owner, self.resource_observer) {
            ctx = ctx.with_execution_resources(owner, observer);
        }
        if let Some(catalog) = self.tool_catalog {
            ctx = ctx.with_tool_catalog(catalog);
        }
        ctx.with_execution_owner_registry(owners)
    }
}

/// 工具参数安全守卫端口（旧 `service/ToolSafetyGuard.java`）。
///
/// 与授权系统**正交**：权限系统回答「用户允不允许」，本守卫回答「调用**参数
/// 本身**是否安全」。权威策略实现位于 zk-authz `tool_safety`（含 scratchpad
/// 写入边界），由 zk-server 组合根接线——依赖方向铁律禁止 `zk-tools → zk-authz`，
/// 故此处以 trait 反转（范式同 zk-authz `tool_facts` 的 `ToolFacts`）。
///
/// 注意：旧类的**环境安全层**不走本端口。子进程敏感环境变量清理在
/// [`crate::process`] 的 spawn 处**无条件**执行——旧源该守卫全仓零调用点，
/// 若把它做成可选接线就会留后门。
pub trait ToolSafetyGuard: Send + Sync {
    /// 检查一次工具调用的参数安全性；拒绝时返回可展示的原因文案。
    ///
    /// # Errors
    ///
    /// 参数越界（例如写入目标越出 scratchpad 边界、路径命中敏感黑名单）时
    /// 返回拒绝原因，执行器据此直接产出 `is_error` 结果、不调用工具。
    fn check_tool_call(
        &self,
        tool: &dyn Tool,
        input: &serde_json::Value,
        env: &CallEnv,
    ) -> Result<(), String>;
}

/// 受控执行器（可克隆共享；全部调用共享同一全局许可池）。
#[derive(Clone)]
pub struct ToolExecutor {
    semaphore: Arc<Semaphore>,
    /// Process-wide workspace write isolation. Production constructors always
    /// clone the same registry, including custom leaf-concurrency executors.
    workspace_leases: WorkspaceLeaseManager,
    /// 参数安全守卫（未接线时为 `None`——旧源默认形态，见
    /// [`ToolSafetyGuard`] 关于环境安全层不依赖接线的说明）。
    safety_guard: Option<Arc<dyn ToolSafetyGuard>>,
    /// JoinHandle/cancellation ownership shared by this executor and every clone.
    owners: ExecutionOwnerRegistry,
}

/// Owned state moved into the task spawned for one tool invocation.
struct SpawnedCall {
    tool: Arc<dyn Tool>,
    tool_use_id: String,
    input: serde_json::Value,
    cancel: CancellationToken,
    env: CallEnv,
    event_tx: mpsc::Sender<ToolEvent>,
    semaphore: Arc<Semaphore>,
    workspace_leases: WorkspaceLeaseManager,
    safety_guard: Option<Arc<dyn ToolSafetyGuard>>,
    owners: ExecutionOwnerRegistry,
}

/// RAII guards retained for the full lifetime of an admitted tool future.
struct ExecutionGuards {
    permit: Option<OwnedSemaphorePermit>,
    workspace_lease: Option<WorkspaceLeaseGuard>,
}

enum AdmissionFailure {
    /// Cancellation or executor shutdown preserves the silent-exit contract.
    SilentExit,
    /// A deterministic pre-execution failure must be returned to the caller.
    Rejected(ToolOutput),
}

impl Default for ToolExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolExecutor {
    /// 以默认并发上限（[`MAX_CONCURRENT_TOOLS`]）构造。
    #[must_use]
    pub fn new() -> Self {
        Self {
            semaphore: global_tool_semaphore(),
            workspace_leases: WorkspaceLeaseManager::process_wide(),
            safety_guard: None,
            owners: ExecutionOwnerRegistry::new(),
        }
    }

    /// 以指定并发上限构造（测试用）。
    #[must_use]
    pub fn with_concurrency(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            workspace_leases: WorkspaceLeaseManager::process_wide(),
            safety_guard: None,
            owners: ExecutionOwnerRegistry::new(),
        }
    }

    /// Whether this executor owns the process-wide production workspace-lease
    /// registry required before shared-workspace Agent writes may be exposed.
    #[must_use]
    pub fn workspace_leases_ready(&self) -> bool {
        self.workspace_leases.is_process_wide()
    }

    #[cfg(test)]
    fn with_concurrency_and_workspace_leases(
        max_concurrent: usize,
        workspace_leases: WorkspaceLeaseManager,
    ) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            workspace_leases,
            safety_guard: None,
            owners: ExecutionOwnerRegistry::new(),
        }
    }

    /// 接入参数安全守卫（zk-server 组合根注入 zk-authz 实现）。
    #[must_use]
    pub fn with_safety_guard(mut self, guard: Arc<dyn ToolSafetyGuard>) -> Self {
        self.safety_guard = Some(guard);
        self
    }

    /// Stop accepting physical leaf work. Existing owners continue cleanup and
    /// remain visible to [`Self::shutdown`].
    pub fn close_intake(&self) {
        self.owners.close_intake();
    }

    /// Whether new physical leaf work may still be registered.
    #[must_use]
    pub fn accepts_new_execution(&self) -> bool {
        self.owners.accepts_new_execution()
    }

    /// Number of tool/process `JoinHandle`s currently retained by this executor.
    #[must_use]
    pub fn active_owner_count(&self) -> usize {
        self.owners.active_count()
    }

    /// Cancel and drain all retained leaf/process owners inside `grace`.
    pub async fn shutdown(&self, grace: Duration) -> ToolExecutorShutdownReport {
        self.owners.shutdown(grace).await
    }

    /// Retain an operation-level finalizer in the same process-wide owner
    /// registry as physical leaf tools.
    ///
    /// This is intentionally a narrow composition hook for protocol surfaces
    /// which must remain responsible for durable terminalization after their
    /// client Future is dropped.  The supplied cancellation token is signalled
    /// during shutdown, but the Future is never aborted: the owner remains
    /// visible until the Future has completed its physical cleanup and durable
    /// commit boundary.
    ///
    /// # Errors
    ///
    /// Returns `EXECUTION_SUPERVISOR_SHUTTING_DOWN` after the process-wide
    /// execution intake has closed.
    pub fn spawn_owned_finalizer(
        &self,
        cancel: CancellationToken,
        future: BoxFuture<'static, ()>,
    ) -> Result<(), &'static str> {
        self.owners.spawn_owned(cancel, future)
    }

    /// 派发一次工具调用（每调用一 `tokio::spawn`），返回事件接收端。
    ///
    /// `parent_cancel` 为 run 层令牌；内部派生 `tool_call` 层 child 令牌
    /// （三层树第三层），排队 / 执行期间取消均即时退出（见模块文档取消语义）。
    #[must_use]
    pub fn spawn_call(
        &self,
        tool: Arc<dyn Tool>,
        tool_use_id: String,
        input: serde_json::Value,
        parent_cancel: &CancellationToken,
    ) -> mpsc::Receiver<ToolEvent> {
        self.spawn_call_in(tool, tool_use_id, input, parent_cancel, CallEnv::new())
    }

    /// 派发一次工具调用并注入调用环境（2.3 追加；语义同
    /// [`Self::spawn_call`]，额外把 [`CallEnv`] 与 `tool_use_id` 落入
    /// [`ToolContext`]）。
    #[must_use]
    pub fn spawn_call_in(
        &self,
        tool: Arc<dyn Tool>,
        tool_use_id: String,
        input: serde_json::Value,
        parent_cancel: &CancellationToken,
        env: CallEnv,
    ) -> mpsc::Receiver<ToolEvent> {
        let (event_tx, event_rx) = mpsc::channel(TOOL_EVENT_QUEUE_CAPACITY);
        let call = SpawnedCall {
            tool,
            tool_use_id,
            input,
            cancel: parent_cancel.child_token(),
            env,
            event_tx,
            semaphore: Arc::clone(&self.semaphore),
            workspace_leases: self.workspace_leases.clone(),
            safety_guard: self.safety_guard.clone(),
            owners: self.owners.clone(),
        };
        let cancel = call.cancel.clone();
        let _ = self
            .owners
            .spawn_owned(cancel, Box::pin(run_spawned_call(call)));
        event_rx
    }
}

async fn run_spawned_call(call: SpawnedCall) {
    let ExecutionGuards {
        permit,
        workspace_lease,
    } = match admit_call(&call).await {
        Ok(guards) => guards,
        Err(AdmissionFailure::SilentExit) => return,
        Err(AdmissionFailure::Rejected(output)) => {
            let _ = call
                .event_tx
                .send(ToolEvent::Finished {
                    tool_use_id: call.tool_use_id,
                    output,
                    cleanup_status: ToolCleanupStatus::NotRequired,
                })
                .await;
            return;
        }
    };

    let (progress_tx, mut progress_rx) = mpsc::channel(TOOL_EVENT_QUEUE_CAPACITY);
    let capability_revocation = call.env.capability_revocation.clone();
    let ctx = call.env.apply(
        ToolContext::with_bounded_progress(call.cancel.clone(), progress_tx)
            .with_tool_use_id(call.tool_use_id.clone()),
        call.owners.clone(),
    );
    // Keep a second handle to the per-invocation resource tracker after the
    // ToolContext itself moves into the tool future.
    let cleanup_ctx = ctx.clone();
    let timeout = match call.tool.timeout_policy() {
        crate::tool::ToolTimeoutPolicy::Executor => call.tool.timeout().min(MAX_TOOL_TIMEOUT),
        crate::tool::ToolTimeoutPolicy::TaskRuntime => Duration::from_mins(32),
    };
    let work = call.tool.execute(call.input, ctx);
    let Some(output) = drive_tool(
        work,
        timeout,
        &call.cancel,
        &cleanup_ctx,
        capability_revocation.as_ref(),
        &call.event_tx,
        &call.tool_use_id,
        &mut progress_rx,
    )
    .await
    else {
        drop(permit);
        return;
    };

    drop(workspace_lease);
    drop(permit);
    // 排干残余进度（保证 Progress 先于 Finished 的事件序）。
    while let Ok(text) = progress_rx.try_recv() {
        let _ = call.event_tx.try_send(ToolEvent::Progress {
            tool_use_id: call.tool_use_id.clone(),
            text,
        });
    }
    let _ = call
        .event_tx
        .send(ToolEvent::Finished {
            tool_use_id: call.tool_use_id,
            output: truncate_output(output),
            cleanup_status: cleanup_ctx.execution_cleanup_status(),
        })
        .await;
}

async fn admit_call(call: &SpawnedCall) -> Result<ExecutionGuards, AdmissionFailure> {
    // 只有真实叶子执行占用全局许可。Agent/TaskOutput 等持久编排等待
    // 不占工具槽，避免父任务等待子结果时反向饿死子任务所需工具。
    let permit = if call.tool.uses_execution_slot() {
        tokio::select! {
            biased;
            () = call.cancel.cancelled() => return Err(AdmissionFailure::SilentExit),
            permit = Arc::clone(&call.semaphore).acquire_owned() => match permit {
                Ok(permit) => Some(permit),
                // Semaphore 关闭（进程关停路径），静默退出。
                Err(_) => return Err(AdmissionFailure::SilentExit),
            },
        }
    } else {
        None
    };

    // 参数安全守卫先于工具执行判定参数本身是否安全。
    if let Some(guard) = call.safety_guard.as_ref()
        && let Err(reason) = guard.check_tool_call(call.tool.as_ref(), &call.input, &call.env)
    {
        tracing::warn!(tool = call.tool.name(), %reason, "tool call denied by safety guard");
        return Err(AdmissionFailure::Rejected(ToolOutput::error(reason)));
    }

    // Read-only invocations never enter workspace write arbitration. The three
    // built-in CAS writers share the root lease; every other mutation is exclusive.
    let workspace_lease = match workspace_lease_mode(call.tool.as_ref(), &call.input) {
        None => None,
        Some(mode) => {
            let root = call.env.workspace_root().map_err(|reason| {
                AdmissionFailure::Rejected(ToolOutput::error(format!(
                    "WORKSPACE_LEASE_ROOT_INVALID: {reason}"
                )))
            })?;
            match call
                .workspace_leases
                .acquire(&root, mode, &call.cancel)
                .await
            {
                Ok(lease) => Some(lease),
                Err(WorkspaceLeaseError::Cancelled) => {
                    return Err(AdmissionFailure::SilentExit);
                }
                Err(error @ WorkspaceLeaseError::InvalidRoot { .. }) => {
                    return Err(AdmissionFailure::Rejected(ToolOutput::error(format!(
                        "WORKSPACE_LEASE_ROOT_INVALID: {error}"
                    ))));
                }
            }
        }
    };
    Ok(ExecutionGuards {
        permit,
        workspace_lease,
    })
}

#[allow(clippy::too_many_arguments)] // one supervised call's immutable execution envelope
async fn drive_tool(
    mut work: BoxFuture<'_, ToolOutput>,
    timeout: Duration,
    cancel: &CancellationToken,
    cleanup_ctx: &ToolContext,
    capability_revocation: Option<&CancellationToken>,
    event_tx: &mpsc::Sender<ToolEvent>,
    tool_use_id: &str,
    progress_rx: &mut mpsc::Receiver<String>,
) -> Option<ToolOutput> {
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    let mut progress_open = true;
    let revocation = capability_revocation.cloned();
    let revoked = async move {
        match revocation {
            Some(token) => token.cancelled().await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(revoked);
    loop {
        tokio::select! {
            biased;
            () = &mut revoked => {
                cancel.cancel();
                if tokio::time::timeout(TOOL_CLEANUP_GRACE, &mut work)
                    .await
                    .is_err()
                {
                    cleanup_ctx.force_unconfirmed_execution_resources().await;
                }
                return Some(ToolOutput::error(
                    "TOOL_CAPABILITY_REVOKED: tool directory or connection changed during execution",
                ));
            }
            // 取消优先：不产出 Finished（通道关闭即中断信号）。
            () = cancel.cancelled() => {
                // Retain the process/MCP-owning future for the managed cleanup window.
                if tokio::time::timeout(TOOL_CLEANUP_GRACE, &mut work)
                    .await
                    .is_err()
                {
                    cleanup_ctx.force_unconfirmed_execution_resources().await;
                }
                return None;
            }
            () = &mut deadline => {
                // Trigger cooperative cleanup before reporting the timeout.
                cancel.cancel();
                if tokio::time::timeout(TOOL_CLEANUP_GRACE, &mut work)
                    .await
                    .is_err()
                {
                    cleanup_ctx.force_unconfirmed_execution_resources().await;
                }
                return Some(ToolOutput::error(format!(
                    "Tool execution timed out after {}ms",
                    timeout.as_millis()
                )));
            }
            progress = progress_rx.recv(), if progress_open => match progress {
                Some(text) => {
                    let _ = event_tx.try_send(ToolEvent::Progress {
                        tool_use_id: tool_use_id.to_owned(),
                        text,
                    });
                }
                None => progress_open = false,
            },
            output = &mut work => return Some(output),
        }
    }
}

/// Classify the workspace effect without trusting arbitrary tool names to opt
/// into shared writes. The allowlist is intentionally limited to the three
/// built-in CAS writers; an unknown non-read-only leaf is always exclusive.
fn workspace_lease_mode(tool: &dyn Tool, input: &serde_json::Value) -> Option<WorkspaceLeaseMode> {
    if !tool.uses_execution_slot() || tool.is_read_only(input) {
        return None;
    }
    match tool.name() {
        "Write" | "Edit" | "NotebookEdit" => Some(WorkspaceLeaseMode::SharedWrite),
        _ => Some(WorkspaceLeaseMode::ExclusiveWrite),
    }
}

/// 输出截断（按 char 边界回退，避免切碎多字节 UTF-8）。
fn truncate_output(mut output: ToolOutput) -> ToolOutput {
    if output.content.len() <= MAX_TOOL_OUTPUT_BYTES {
        return output;
    }
    let mut cut = MAX_TOOL_OUTPUT_BYTES;
    while !output.content.is_char_boundary(cut) {
        cut -= 1;
    }
    output.content.truncate(cut);
    output.content.push_str(TRUNCATION_MARKER);
    output
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::future::BoxFuture;
    use serde_json::json;

    use super::*;

    struct TempRoot(PathBuf);

    impl TempRoot {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "zk-tool-executor-lease-{label}-{}",
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir_all(&path).expect("create temp workspace root");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct LeaseProbeTool {
        name: &'static str,
        read_only: bool,
        uses_execution_slot: bool,
        calls: Arc<AtomicUsize>,
        entered: Arc<tokio::sync::Notify>,
        release: Option<Arc<tokio::sync::Notify>>,
    }

    impl Tool for LeaseProbeTool {
        fn name(&self) -> &'static str {
            self.name
        }

        fn description(&self) -> &'static str {
            "workspace lease probe"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        fn uses_execution_slot(&self) -> bool {
            self.uses_execution_slot
        }

        fn is_read_only(&self, _input: &serde_json::Value) -> bool {
            self.read_only
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            _ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            let calls = Arc::clone(&self.calls);
            let entered = Arc::clone(&self.entered);
            let release = self.release.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                entered.notify_one();
                if let Some(release) = release {
                    release.notified().await;
                }
                ToolOutput::ok("lease probe complete")
            })
        }
    }

    fn lease_probe(
        name: &'static str,
        read_only: bool,
        uses_execution_slot: bool,
        calls: &Arc<AtomicUsize>,
        entered: &Arc<tokio::sync::Notify>,
        release: Option<&Arc<tokio::sync::Notify>>,
    ) -> Arc<dyn Tool> {
        Arc::new(LeaseProbeTool {
            name,
            read_only,
            uses_execution_slot,
            calls: Arc::clone(calls),
            entered: Arc::clone(entered),
            release: release.cloned(),
        })
    }

    async fn give_spawned_calls_a_chance_to_enter() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    /// 并发追踪桩：进入时 current+1 并刷新 max，短暂驻留后退出。
    struct GateTool {
        current: Arc<AtomicUsize>,
        max_seen: Arc<AtomicUsize>,
    }

    impl Tool for GateTool {
        fn name(&self) -> &'static str {
            "Gate"
        }

        fn description(&self) -> &'static str {
            "concurrency probe"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        fn is_read_only(&self, _input: &serde_json::Value) -> bool {
            true
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            _ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            let current = Arc::clone(&self.current);
            let max_seen = Arc::clone(&self.max_seen);
            Box::pin(async move {
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                current.fetch_sub(1, Ordering::SeqCst);
                ToolOutput::ok("done")
            })
        }
    }

    /// 永不完成桩（短自定义超时，供超时/取消测试）。
    struct HangTool {
        timeout: Duration,
    }

    impl Tool for HangTool {
        fn name(&self) -> &'static str {
            "Hang"
        }

        fn description(&self) -> &'static str {
            "never completes"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        fn timeout(&self) -> Duration {
            self.timeout
        }

        fn is_read_only(&self, _input: &serde_json::Value) -> bool {
            true
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            _ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            Box::pin(std::future::pending())
        }
    }

    struct CancelCleanupTool {
        entered: Arc<tokio::sync::Notify>,
        cleaned: Arc<AtomicUsize>,
    }

    impl Tool for CancelCleanupTool {
        fn name(&self) -> &'static str {
            "CancelCleanup"
        }

        fn description(&self) -> &'static str {
            "proves the executor retains cleanup after its receiver is dropped"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        fn is_read_only(&self, _input: &serde_json::Value) -> bool {
            true
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            let entered = Arc::clone(&self.entered);
            let cleaned = Arc::clone(&self.cleaned);
            Box::pin(async move {
                entered.notify_one();
                ctx.cancel.cancelled().await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                cleaned.fetch_add(1, Ordering::SeqCst);
                ToolOutput::ok("cleaned")
            })
        }
    }

    struct OrchestrationWaitTool {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl Tool for OrchestrationWaitTool {
        fn name(&self) -> &'static str {
            "OrchestrationWait"
        }

        fn description(&self) -> &'static str {
            "waits durably without consuming a leaf slot"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        fn uses_execution_slot(&self) -> bool {
            false
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            _ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            let entered = Arc::clone(&self.entered);
            let release = Arc::clone(&self.release);
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                ToolOutput::ok("released")
            })
        }
    }

    /// 进度桩：两条进度后成功返回。
    struct ProgressTool;

    impl Tool for ProgressTool {
        fn name(&self) -> &'static str {
            "Progress"
        }

        fn description(&self) -> &'static str {
            "emits progress"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        fn is_read_only(&self, _input: &serde_json::Value) -> bool {
            true
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            Box::pin(async move {
                ctx.report_progress("step 1");
                ctx.report_progress("step 2");
                ToolOutput::ok("finished")
            })
        }
    }

    /// 执行计数桩：验证守卫拒绝时工具**未被调用**。
    struct CountingTool {
        calls: Arc<AtomicUsize>,
    }

    impl Tool for CountingTool {
        fn name(&self) -> &'static str {
            "Counting"
        }

        fn description(&self) -> &'static str {
            "counts executions"
        }

        fn parameters(&self) -> serde_json::Value {
            json!({ "type": "object" })
        }

        fn is_read_only(&self, _input: &serde_json::Value) -> bool {
            true
        }

        fn execute(
            &self,
            _input: serde_json::Value,
            _ctx: ToolContext,
        ) -> BoxFuture<'_, ToolOutput> {
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                ToolOutput::ok("executed")
            })
        }
    }

    /// 守卫桩：记录被检查的（工具名, 会话 ID），按 `deny` 决定放行/拒绝。
    struct RecordingGuard {
        deny: bool,
        seen: std::sync::Mutex<Vec<(String, Option<String>)>>,
    }

    impl ToolSafetyGuard for RecordingGuard {
        fn check_tool_call(
            &self,
            tool: &dyn Tool,
            _input: &serde_json::Value,
            env: &CallEnv,
        ) -> Result<(), String> {
            self.seen.lock().expect("guard lock").push((
                tool.name().to_owned(),
                env.session_id_str().map(str::to_owned),
            ));
            if self.deny {
                Err(
                    "scratchpad boundary violation: /etc/passwd is outside the scratchpad root"
                        .to_owned(),
                )
            } else {
                Ok(())
            }
        }
    }

    /// 守卫拒绝 → 直接产出 `is_error` 结果，且工具 execute 从未被调用。
    #[tokio::test]
    async fn safety_guard_denial_short_circuits_execution() {
        let guard = Arc::new(RecordingGuard {
            deny: true,
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let executor = ToolExecutor::with_concurrency(1)
            .with_safety_guard(Arc::clone(&guard) as Arc<dyn ToolSafetyGuard>);
        let calls = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn Tool> = Arc::new(CountingTool {
            calls: Arc::clone(&calls),
        });
        let events = collect(executor.spawn_call_in(
            tool,
            "call-1".to_owned(),
            json!({ "file_path": "/etc/passwd" }),
            &CancellationToken::new(),
            CallEnv::new().with_session_id("s-1"),
        ))
        .await;

        assert_eq!(events.len(), 1);
        match &events[0] {
            ToolEvent::Finished {
                tool_use_id,
                output,
                ..
            } => {
                assert_eq!(tool_use_id, "call-1");
                assert!(output.is_error);
                assert!(output.content.contains("scratchpad boundary violation"));
            }
            other @ ToolEvent::Progress { .. } => panic!("unexpected event: {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0, "tool must not execute");
        let seen = guard.seen.lock().expect("guard lock");
        assert_eq!(
            seen.as_slice(),
            [("Counting".to_owned(), Some("s-1".to_owned()))]
        );
    }

    /// 守卫放行 → 正常执行；未接线守卫时行为与放行一致（既有测试覆盖）。
    #[tokio::test]
    async fn safety_guard_allow_lets_execution_through() {
        let guard = Arc::new(RecordingGuard {
            deny: false,
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let executor = ToolExecutor::with_concurrency(1)
            .with_safety_guard(Arc::clone(&guard) as Arc<dyn ToolSafetyGuard>);
        let calls = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn Tool> = Arc::new(CountingTool {
            calls: Arc::clone(&calls),
        });
        let events = collect(executor.spawn_call(
            tool,
            "call-2".to_owned(),
            json!({}),
            &CancellationToken::new(),
        ))
        .await;

        assert_eq!(events.len(), 1);
        match &events[0] {
            ToolEvent::Finished { output, .. } => {
                assert!(!output.is_error);
                assert_eq!(output.content, "executed");
            }
            other @ ToolEvent::Progress { .. } => panic!("unexpected event: {other:?}"),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(guard.seen.lock().expect("guard lock").len(), 1);
    }

    async fn collect(mut rx: mpsc::Receiver<ToolEvent>) -> Vec<ToolEvent> {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    }

    #[tokio::test]
    async fn concurrency_never_exceeds_limit() {
        let executor = ToolExecutor::with_concurrency(4);
        let current = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let cancel = CancellationToken::new();
        let receivers: Vec<_> = (0..16)
            .map(|i| {
                let tool: Arc<dyn Tool> = Arc::new(GateTool {
                    current: Arc::clone(&current),
                    max_seen: Arc::clone(&max_seen),
                });
                executor.spawn_call(tool, format!("call-{i}"), json!({}), &cancel)
            })
            .collect();
        for rx in receivers {
            let events = collect(rx).await;
            assert!(matches!(
                events.last(),
                Some(ToolEvent::Finished { output, .. }) if !output.is_error
            ));
        }
        assert!(
            max_seen.load(Ordering::SeqCst) <= 4,
            "observed concurrency {} exceeds limit 4",
            max_seen.load(Ordering::SeqCst)
        );
        assert_eq!(current.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn orchestration_wait_does_not_consume_a_leaf_execution_slot() {
        let executor = ToolExecutor::with_concurrency(1);
        let cancel = CancellationToken::new();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let orchestration: Arc<dyn Tool> = Arc::new(OrchestrationWaitTool {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let orchestration_rx = executor.spawn_call(
            orchestration,
            "orchestration".to_owned(),
            json!({}),
            &cancel,
        );
        entered.notified().await;

        let calls = Arc::new(AtomicUsize::new(0));
        let leaf: Arc<dyn Tool> = Arc::new(CountingTool {
            calls: Arc::clone(&calls),
        });
        let leaf_events = tokio::time::timeout(
            Duration::from_secs(1),
            collect(executor.spawn_call(leaf, "leaf".to_owned(), json!({}), &cancel)),
        )
        .await
        .expect("leaf must run while orchestration is waiting");
        assert!(matches!(
            leaf_events.as_slice(),
            [ToolEvent::Finished { output, .. }] if !output.is_error
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        release.notify_one();
        let orchestration_events = collect(orchestration_rx).await;
        assert!(matches!(
            orchestration_events.as_slice(),
            [ToolEvent::Finished { output, .. }] if !output.is_error
        ));
    }

    #[tokio::test]
    async fn capability_revocation_cancels_an_active_tool_and_returns_terminal_error() {
        let executor = ToolExecutor::with_concurrency(1);
        let entered = Arc::new(tokio::sync::Notify::new());
        let cleaned = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn Tool> = Arc::new(CancelCleanupTool {
            entered: Arc::clone(&entered),
            cleaned: Arc::clone(&cleaned),
        });
        let revocation = CancellationToken::new();
        let rx = executor.spawn_call_in(
            tool,
            "revoked".to_owned(),
            json!({}),
            &CancellationToken::new(),
            CallEnv::new().with_capability_revocation(revocation.clone()),
        );
        entered.notified().await;
        revocation.cancel();

        let events = tokio::time::timeout(Duration::from_secs(1), collect(rx))
            .await
            .expect("revocation must terminate the invocation");
        assert!(matches!(
            events.as_slice(),
            [ToolEvent::Finished { output, .. }]
                if output.is_error && output.content.contains("TOOL_CAPABILITY_REVOKED")
        ));
        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn workspace_lease_classification_is_conservative() {
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let input = json!({});
        for name in ["Write", "Edit", "NotebookEdit"] {
            let tool = lease_probe(name, false, true, &calls, &entered, None);
            assert_eq!(
                workspace_lease_mode(tool.as_ref(), &input),
                Some(WorkspaceLeaseMode::SharedWrite)
            );
        }

        let unknown = lease_probe("UnknownMutation", false, true, &calls, &entered, None);
        assert_eq!(
            workspace_lease_mode(unknown.as_ref(), &input),
            Some(WorkspaceLeaseMode::ExclusiveWrite)
        );
        let read_only = lease_probe("UnknownRead", true, true, &calls, &entered, None);
        assert_eq!(workspace_lease_mode(read_only.as_ref(), &input), None);
        let orchestration = lease_probe("Agent", false, false, &calls, &entered, None);
        assert_eq!(workspace_lease_mode(orchestration.as_ref(), &input), None);
    }

    #[tokio::test]
    async fn unknown_mutation_exclusively_blocks_a_cas_writer() {
        let manager = WorkspaceLeaseManager::isolated();
        let executor = ToolExecutor::with_concurrency_and_workspace_leases(4, manager);
        let root = TempRoot::new("unknown-exclusive");
        let first_calls = Arc::new(AtomicUsize::new(0));
        let first_entered = Arc::new(tokio::sync::Notify::new());
        let first_release = Arc::new(tokio::sync::Notify::new());
        let first_rx = executor.spawn_call_in(
            lease_probe(
                "UnknownMutation",
                false,
                true,
                &first_calls,
                &first_entered,
                Some(&first_release),
            ),
            "exclusive".to_owned(),
            json!({}),
            &CancellationToken::new(),
            CallEnv::new().with_working_dir(root.path()),
        );
        first_entered.notified().await;

        let second_calls = Arc::new(AtomicUsize::new(0));
        let second_entered = Arc::new(tokio::sync::Notify::new());
        let second_rx = executor.spawn_call_in(
            lease_probe("Write", false, true, &second_calls, &second_entered, None),
            "cas-write".to_owned(),
            json!({}),
            &CancellationToken::new(),
            CallEnv::new().with_working_dir(root.path()),
        );
        give_spawned_calls_a_chance_to_enter().await;
        assert_eq!(
            second_calls.load(Ordering::SeqCst),
            0,
            "CAS writer must wait behind an unknown exclusive mutation"
        );

        first_release.notify_one();
        assert!(matches!(
            collect(first_rx).await.as_slice(),
            [ToolEvent::Finished { output, .. }] if !output.is_error
        ));
        assert!(matches!(
            collect(second_rx).await.as_slice(),
            [ToolEvent::Finished { output, .. }] if !output.is_error
        ));
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn two_cas_writers_execute_concurrently_on_one_root() {
        let manager = WorkspaceLeaseManager::isolated();
        let executor = ToolExecutor::with_concurrency_and_workspace_leases(4, manager);
        let root = TempRoot::new("cas-parallel");
        let first_calls = Arc::new(AtomicUsize::new(0));
        let first_entered = Arc::new(tokio::sync::Notify::new());
        let first_release = Arc::new(tokio::sync::Notify::new());
        let first_rx = executor.spawn_call_in(
            lease_probe(
                "Write",
                false,
                true,
                &first_calls,
                &first_entered,
                Some(&first_release),
            ),
            "cas-one".to_owned(),
            json!({}),
            &CancellationToken::new(),
            CallEnv::new().with_working_dir(root.path()),
        );
        first_entered.notified().await;

        let second_calls = Arc::new(AtomicUsize::new(0));
        let second_entered = Arc::new(tokio::sync::Notify::new());
        let second_rx = executor.spawn_call_in(
            lease_probe("Edit", false, true, &second_calls, &second_entered, None),
            "cas-two".to_owned(),
            json!({}),
            &CancellationToken::new(),
            CallEnv::new().with_working_dir(root.path()),
        );
        tokio::time::timeout(Duration::from_secs(1), second_entered.notified())
            .await
            .expect("second CAS writer should enter without waiting for the first writer");
        assert_eq!(
            second_calls.load(Ordering::SeqCst),
            1,
            "shared CAS writers must not serialize the entire workspace"
        );

        first_release.notify_one();
        assert!(matches!(
            collect(first_rx).await.as_slice(),
            [ToolEvent::Finished { output, .. }] if !output.is_error
        ));
        assert!(matches!(
            collect(second_rx).await.as_slice(),
            [ToolEvent::Finished { output, .. }] if !output.is_error
        ));
    }

    #[tokio::test]
    async fn exclusive_mutations_in_different_worktrees_do_not_block() {
        let manager = WorkspaceLeaseManager::isolated();
        let executor = ToolExecutor::with_concurrency_and_workspace_leases(4, manager);
        let first_root = TempRoot::new("worktree-one");
        let second_root = TempRoot::new("worktree-two");
        let first_calls = Arc::new(AtomicUsize::new(0));
        let first_entered = Arc::new(tokio::sync::Notify::new());
        let first_release = Arc::new(tokio::sync::Notify::new());
        let first_rx = executor.spawn_call_in(
            lease_probe(
                "UnknownMutation",
                false,
                true,
                &first_calls,
                &first_entered,
                Some(&first_release),
            ),
            "worktree-one".to_owned(),
            json!({}),
            &CancellationToken::new(),
            CallEnv::new().with_working_dir(first_root.path()),
        );
        first_entered.notified().await;

        let second_calls = Arc::new(AtomicUsize::new(0));
        let second_entered = Arc::new(tokio::sync::Notify::new());
        let second_rx = executor.spawn_call_in(
            lease_probe(
                "AnotherUnknownMutation",
                false,
                true,
                &second_calls,
                &second_entered,
                None,
            ),
            "worktree-two".to_owned(),
            json!({}),
            &CancellationToken::new(),
            CallEnv::new().with_working_dir(second_root.path()),
        );
        tokio::time::timeout(Duration::from_secs(1), second_entered.notified())
            .await
            .expect("an exclusive mutation in another worktree should enter independently");
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);

        first_release.notify_one();
        let _ = collect(first_rx).await;
        let _ = collect(second_rx).await;
    }

    #[tokio::test]
    async fn cancelling_while_waiting_for_workspace_lease_never_executes() {
        let manager = WorkspaceLeaseManager::isolated();
        let executor = ToolExecutor::with_concurrency_and_workspace_leases(4, manager);
        let root = TempRoot::new("cancel-wait");
        let first_calls = Arc::new(AtomicUsize::new(0));
        let first_entered = Arc::new(tokio::sync::Notify::new());
        let first_release = Arc::new(tokio::sync::Notify::new());
        let first_cancel = CancellationToken::new();
        let first_rx = executor.spawn_call_in(
            lease_probe(
                "UnknownMutation",
                false,
                true,
                &first_calls,
                &first_entered,
                Some(&first_release),
            ),
            "blocker".to_owned(),
            json!({}),
            &first_cancel,
            CallEnv::new().with_working_dir(root.path()),
        );
        first_entered.notified().await;

        let waiting_calls = Arc::new(AtomicUsize::new(0));
        let waiting_entered = Arc::new(tokio::sync::Notify::new());
        let waiting_cancel = CancellationToken::new();
        let waiting_rx = executor.spawn_call_in(
            lease_probe("Write", false, true, &waiting_calls, &waiting_entered, None),
            "waiting".to_owned(),
            json!({}),
            &waiting_cancel,
            CallEnv::new().with_working_dir(root.path()),
        );
        give_spawned_calls_a_chance_to_enter().await;
        waiting_cancel.cancel();
        give_spawned_calls_a_chance_to_enter().await;
        first_release.notify_one();

        assert!(collect(waiting_rx).await.is_empty());
        assert_eq!(waiting_calls.load(Ordering::SeqCst), 0);
        let _ = collect(first_rx).await;
    }

    #[tokio::test]
    async fn invalid_workspace_root_fails_before_mutating_tool_execution() {
        let manager = WorkspaceLeaseManager::isolated();
        let executor = ToolExecutor::with_concurrency_and_workspace_leases(1, manager);
        let parent = TempRoot::new("invalid-root");
        let missing_root = parent.path().join("missing");
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let events = collect(executor.spawn_call_in(
            lease_probe("UnknownMutation", false, true, &calls, &entered, None),
            "invalid-root".to_owned(),
            json!({}),
            &CancellationToken::new(),
            CallEnv::new().with_working_dir(missing_root),
        ))
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            events.as_slice(),
            [ToolEvent::Finished { output, .. }]
                if output.is_error && output.content.contains("WORKSPACE_LEASE_ROOT_INVALID")
        ));
    }

    #[test]
    fn default_executors_share_the_process_wide_leaf_limit() {
        let first = ToolExecutor::new();
        let second = ToolExecutor::new();
        let custom_concurrency = ToolExecutor::with_concurrency(3);
        assert!(Arc::ptr_eq(&first.semaphore, &second.semaphore));
        assert!(
            first
                .workspace_leases
                .shares_registry(&custom_concurrency.workspace_leases),
            "all production constructors must share one workspace lease registry"
        );
        assert!(first.workspace_leases_ready());
        assert!(custom_concurrency.workspace_leases_ready());
    }

    #[tokio::test]
    async fn timeout_yields_error_finished() {
        let executor = ToolExecutor::new();
        let cancel = CancellationToken::new();
        let tool: Arc<dyn Tool> = Arc::new(HangTool {
            timeout: Duration::from_millis(30),
        });
        let events = collect(executor.spawn_call(tool, "t1".into(), json!({}), &cancel)).await;
        assert_eq!(events.len(), 1);
        let ToolEvent::Finished {
            tool_use_id,
            output,
            ..
        } = &events[0]
        else {
            panic!("expected Finished, got {events:?}");
        };
        assert_eq!(tool_use_id, "t1");
        assert!(output.is_error);
        assert!(output.content.contains("timed out after 30ms"));
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_owned_wait_survives_leaf_timeout() {
        struct RuntimeWait;
        impl Tool for RuntimeWait {
            fn name(&self) -> &'static str {
                "RuntimeWait"
            }
            fn description(&self) -> &'static str {
                "runtime owned wait"
            }
            fn parameters(&self) -> serde_json::Value {
                json!({})
            }
            fn timeout(&self) -> Duration {
                Duration::from_mins(30)
            }
            fn timeout_policy(&self) -> crate::tool::ToolTimeoutPolicy {
                crate::tool::ToolTimeoutPolicy::TaskRuntime
            }
            fn is_read_only(&self, _: &serde_json::Value) -> bool {
                true
            }
            fn execute(&self, _: serde_json::Value, _: ToolContext) -> BoxFuture<'_, ToolOutput> {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_mins(11)).await;
                    ToolOutput::ok("partial result")
                })
            }
        }
        let executor = ToolExecutor::new();
        let cancel = CancellationToken::new();
        let events = collect(executor.spawn_call(
            Arc::new(RuntimeWait),
            "runtime-wait".into(),
            json!({}),
            &cancel,
        ))
        .await;
        assert!(
            matches!(&events[0], ToolEvent::Finished { output, .. } if !output.is_error && output.content == "partial result")
        );
    }

    #[tokio::test]
    async fn cancel_closes_channel_without_finished() {
        let executor = ToolExecutor::new();
        let cancel = CancellationToken::new();
        let tool: Arc<dyn Tool> = Arc::new(HangTool {
            timeout: Duration::from_mins(1),
        });
        let rx = executor.spawn_call(tool, "t2".into(), json!({}), &cancel);
        // 执行已开始后再取消（parent → child 树传播）。
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        let events = collect(rx).await;
        assert!(
            events.is_empty(),
            "cancelled call must not emit events: {events:?}"
        );
    }

    #[tokio::test]
    async fn shutdown_retains_and_drains_leaf_after_receiver_is_dropped() {
        let executor = ToolExecutor::with_concurrency(1);
        let entered = Arc::new(tokio::sync::Notify::new());
        let cleaned = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn Tool> = Arc::new(CancelCleanupTool {
            entered: Arc::clone(&entered),
            cleaned: Arc::clone(&cleaned),
        });
        let receiver = executor.spawn_call(
            tool,
            "owned-leaf".to_owned(),
            json!({}),
            &CancellationToken::new(),
        );
        entered.notified().await;
        drop(receiver);
        assert_eq!(executor.active_owner_count(), 1);

        let report = executor.shutdown(Duration::from_secs(1)).await;
        assert_eq!(report.owners_requested, 1);
        assert_eq!(report.owners_remaining, 0);
        assert!(report.drained);
        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
        assert!(!executor.accepts_new_execution());

        let calls = Arc::new(AtomicUsize::new(0));
        let mut rejected = executor.spawn_call(
            Arc::new(CountingTool {
                calls: Arc::clone(&calls),
            }),
            "after-shutdown".to_owned(),
            json!({}),
            &CancellationToken::new(),
        );
        assert!(rejected.recv().await.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn progress_events_precede_finished() {
        let executor = ToolExecutor::new();
        let cancel = CancellationToken::new();
        let events =
            collect(executor.spawn_call(Arc::new(ProgressTool), "t3".into(), json!({}), &cancel))
                .await;
        assert_eq!(events.len(), 3);
        assert_eq!(
            events[0],
            ToolEvent::Progress {
                tool_use_id: "t3".into(),
                text: "step 1".into()
            }
        );
        assert_eq!(
            events[1],
            ToolEvent::Progress {
                tool_use_id: "t3".into(),
                text: "step 2".into()
            }
        );
        assert!(
            matches!(&events[2], ToolEvent::Finished { output, .. } if output.content == "finished")
        );
    }

    #[test]
    fn truncate_output_respects_char_boundary() {
        // 未超限原样返回。
        let small = truncate_output(ToolOutput::ok("short"));
        assert_eq!(small.content, "short");
        // 超限：多字节字符跨界时回退到 char 边界再追加标记。
        let mut content = "x".repeat(MAX_TOOL_OUTPUT_BYTES - 1);
        content.push('你'); // 3 字节，跨越 1MiB 边界
        content.push_str("tail");
        let truncated = truncate_output(ToolOutput::ok(content));
        assert!(truncated.content.ends_with(TRUNCATION_MARKER));
        let kept = &truncated.content[..truncated.content.len() - TRUNCATION_MARKER.len()];
        assert_eq!(kept.len(), MAX_TOOL_OUTPUT_BYTES - 1); // '你' 整字符被回退丢弃
        assert!(kept.chars().all(|c| c == 'x'));
    }
}
