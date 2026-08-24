//! 子代理执行器——对照旧 `SubAgentExecutor.executeSync()`（~200L 核心）。

#![allow(clippy::doc_markdown)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]
//!
//! # 架构
//!
//! `SubAgentExecutor` 持有 `SubAgentEngineFactory` trait（反转注入），
//! 解耦「SubAgentExecutor 需要创建子代理引擎实例」与「Engine 自身不
//! 持有 SubAgentExecutor 引用」的循环依赖。
//!
//! # 执行流程（对照旧 `executeSyncInternal`）
//!
//! acquire_slot → resolveAgent → assembleToolPool → buildSystemPrompt →
//! createWorktree(if isolation) → engine_factory.create_and_run →
//! 超时包裹 → 结果截断(100_000 chars) → release_slot
//!
//! 有意差异：Java 的 checkpoint 恢复 / fork / team 路由属 Phase 2+，
//! 本批仅实现同步执行路径核心段。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::agent::types::{
    AgentDefinition, AgentRequest, AgentResult, AgentStatus, GLOBALLY_DENIED_TOOLS, IsolationMode,
};
use crate::agent::worktree::WorktreeManager;
use crate::concurrency::AgentConcurrencyController;

struct ChildRoutingSink {
    inner: Arc<dyn crate::MessageSink>,
    parent_session_id: String,
}

impl crate::MessageSink for ChildRoutingSink {
    fn push<'a>(
        &'a self,
        _child_session_id: &'a str,
        message: zk_protocol::ServerMessage,
    ) -> futures::future::BoxFuture<'a, ()> {
        self.inner.push(&self.parent_session_id, message)
    }
}

/// 子代理引擎工厂——反转注入端口，解耦循环依赖。
///
/// zk-server 组合根装配具体实现：创建子代理的 `Engine` 实例并执行
/// 一轮完整的多轮工具循环，返回最终 `stop_reason` 与助手回复文本。
pub trait SubAgentEngineFactory: Send + Sync {
    /// 创建子代理引擎并执行任务。
    ///
    /// # 参数
    /// - `agent_id`：子代理唯一标识
    /// - `session_id`：子代理会话 ID（`subagent-{agent_id}`）
    /// - `model`：模型 ID
    /// - `system_prompt`：系统提示
    /// - `user_prompt`：用户提示（任务描述）
    /// - `work_dir`：工作目录
    /// - `cancel`：调用方拥有的 Worker/Task 取消令牌
    /// - `max_turns`：最大轮次
    ///
    /// # 返回
    /// `(stop_reason: Option<String>, assistant_text: Option<String>, has_error: bool)`
    fn create_and_run(
        &self,
        agent_id: &str,
        session_id: &str,
        context: &ChildExecutionContext,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        work_dir: &str,
        mailbox: mpsc::UnboundedReceiver<AgentMailboxMessage>,
        cancel: CancellationToken,
        max_turns: u32,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = (Option<String>, Option<String>, bool)> + Send + '_>,
    >;
}

/// 单个子代理默认超时（对照旧 `PER_AGENT_TIMEOUT = 5min`）。
pub const PER_AGENT_TIMEOUT: Duration = Duration::from_mins(5);

/// Parent identity and authorized workspace inherited by a child execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildExecutionContext {
    /// Parent session whose authorization scope is inherited.
    pub parent_session_id: String,
    /// Parent run used for the durable run ancestry link.
    pub parent_run_id: String,
    /// Canonical authorized workspace.
    pub working_directory: PathBuf,
    /// Tool-use identifier that spawned this child.
    pub tool_use_id: String,
    /// Optional request-scoped restriction inherited from a Skill invocation.
    pub allowed_tools: Option<std::collections::BTreeSet<String>>,
}

/// One durable collaboration message routed to an active child agent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentMailboxMessage {
    /// Stable delivery identifier used by queued/consumed Run events.
    pub message_id: String,
    /// Parent Run that owns the durable queue event.
    pub parent_run_id: String,
    /// Sender identity shown in the collaboration timeline.
    pub from_id: String,
    /// Bounded message content injected as a user continuation.
    pub content: String,
}

/// Shared process-local delivery table for active child agents.
///
/// Queue and consumption facts remain durable in `run_event_log`; this router
/// only owns live senders and removes them when execution ends or is dropped.
#[derive(Default)]
pub struct AgentMailboxRouter {
    mailboxes: DashMap<String, mpsc::UnboundedSender<AgentMailboxMessage>>,
}

impl AgentMailboxRouter {
    /// Whether the target child still accepts continuation messages.
    #[must_use]
    pub fn has_active_agent(&self, agent_id: &str) -> bool {
        self.mailboxes.contains_key(agent_id)
    }

    /// Deliver to one active child. Missing/terminal targets fail closed.
    ///
    /// # Errors
    /// Returns an error when the target Agent is not active or its mailbox is closed.
    pub fn send_message(&self, agent_id: &str, message: AgentMailboxMessage) -> Result<(), String> {
        let Some(sender) = self.mailboxes.get(agent_id) else {
            return Err(format!("AGENT_NOT_FOUND: {agent_id}"));
        };
        sender
            .send(message)
            .map_err(|_| format!("AGENT_NOT_FOUND: {agent_id}"))
    }
}

struct MailboxRegistration<'a> {
    router: &'a AgentMailboxRouter,
    agent_id: String,
}

impl Drop for MailboxRegistration<'_> {
    fn drop(&mut self) {
        self.router.mailboxes.remove(&self.agent_id);
    }
}

/// 子代理执行器——连接 AgentTool 与引擎核心循环。
pub struct SubAgentExecutor {
    /// 并发控制器（全局 ≤30 / 会话 ≤10 / 嵌套 ≤3）。
    concurrency: Arc<AgentConcurrencyController>,
    /// 引擎工厂（反转注入）。
    engine_factory: Arc<dyn SubAgentEngineFactory>,
    /// Worktree 管理器。
    worktree_manager: WorktreeManager,
    /// 超时配置（基础 / 编码 2x / 验证 3x）。
    timeout_config: AgentTimeoutConfig,
    /// Active agent mailboxes. Durable facts remain in `run_event_log`.
    mailbox_router: Arc<AgentMailboxRouter>,
}

/// 子代理超时配置（对照旧 `AgentTimeoutConfig`）。
#[derive(Clone, Copy, Debug)]
pub struct AgentTimeoutConfig {
    /// 基础超时秒数。
    pub default_seconds: u64,
    /// 最大超时秒数。
    pub max_seconds: u64,
    /// 优雅关闭窗口秒数。
    pub graceful_shutdown_seconds: u64,
}

impl Default for AgentTimeoutConfig {
    fn default() -> Self {
        Self {
            default_seconds: 300,
            max_seconds: 1800,
            graceful_shutdown_seconds: 30,
        }
    }
}

impl SubAgentExecutor {
    /// 构造执行器。
    #[must_use]
    pub fn new(
        concurrency: Arc<AgentConcurrencyController>,
        engine_factory: Arc<dyn SubAgentEngineFactory>,
        worktree_manager: WorktreeManager,
        timeout_config: AgentTimeoutConfig,
    ) -> Self {
        Self::new_with_mailbox_router(
            concurrency,
            engine_factory,
            worktree_manager,
            timeout_config,
            Arc::new(AgentMailboxRouter::default()),
        )
    }

    /// Construct with a router shared by collaboration tools.
    #[must_use]
    pub fn new_with_mailbox_router(
        concurrency: Arc<AgentConcurrencyController>,
        engine_factory: Arc<dyn SubAgentEngineFactory>,
        worktree_manager: WorktreeManager,
        timeout_config: AgentTimeoutConfig,
        mailbox_router: Arc<AgentMailboxRouter>,
    ) -> Self {
        Self {
            concurrency,
            engine_factory,
            worktree_manager,
            timeout_config,
            mailbox_router,
        }
    }

    /// Whether the target child still accepts continuation messages.
    #[must_use]
    pub fn has_active_agent(&self, agent_id: &str) -> bool {
        self.mailbox_router.has_active_agent(agent_id)
    }

    /// Deliver to one active child. Missing/terminal targets fail closed.
    ///
    /// # Errors
    /// Returns `AGENT_NOT_FOUND` when the target is absent or closed.
    pub fn send_message(&self, agent_id: &str, message: AgentMailboxMessage) -> Result<(), String> {
        self.mailbox_router.send_message(agent_id, message)
    }

    /// 同步执行子代理——AgentTool.call() 的主入口
    /// （对照旧 `executeSync`）。
    ///
    /// 流程：acquire_slot → resolve_agent → assemble_system_prompt →
    /// engine_factory.create_and_run → 超时包裹 → 结果截断 → release_slot
    pub async fn execute_sync(
        &self,
        request: &AgentRequest,
        context: &ChildExecutionContext,
    ) -> AgentResult {
        self.execute_sync_with_cancel(request, context, CancellationToken::new())
            .await
    }

    /// Execute a child with a caller-owned cancellation token propagated through
    /// provider streaming and tool subprocesses.
    #[allow(clippy::too_many_lines)] // ordered validation, isolation, timeout and cleanup lifecycle
    pub async fn execute_sync_with_cancel(
        &self,
        request: &AgentRequest,
        context: &ChildExecutionContext,
        cancel: CancellationToken,
    ) -> AgentResult {
        let agent_id = &request.agent_id;
        info!(
            "Sub-agent {agent_id} starting (type={:?}, isolation={:?})",
            request.agent_type, request.isolation
        );

        // 0. 获取并发槽（对照旧 `concurrencyController.acquireSlot`）。
        let _slot = match self
            .concurrency
            .acquire_slot(agent_id, 0, &context.parent_session_id)
        {
            Ok(slot) => slot,
            Err(e) => {
                return AgentResult::failed(
                    format!("Concurrency limit reached: {}", e.message()),
                    &request.prompt,
                );
            }
        };

        // 1. 解析代理定义
        let agent_def = AgentDefinition::resolve(request.agent_type.as_deref());

        // 2. 解析模型。生产组合根必须把别名和继承关系解析成真实模型名；
        // executor 自身不再把 `premium` 伪装成可调用的模型。
        let Some(model) = request
            .model
            .clone()
            .or_else(|| agent_def.default_model.map(str::to_owned))
        else {
            return AgentResult::failed(
                "AGENT_MODEL_REQUIRED: child execution requires a resolved model",
                &request.prompt,
            );
        };

        // 3. 构建系统提示
        let system_prompt = build_agent_system_prompt(
            agent_def.system_prompt_template,
            &request.prompt,
            &context.working_directory.to_string_lossy(),
        );

        // 4. 创建隔离工作目录
        let work_dir = match request.isolation {
            IsolationMode::Worktree => {
                match self.worktree_manager.create_worktree(agent_id).await {
                    Ok(path) => path.to_string_lossy().into_owned(),
                    Err(e) => {
                        warn!("Worktree creation failed for {agent_id}: {e}, using parent dir");
                        context.working_directory.to_string_lossy().into_owned()
                    }
                }
            }
            _ => context.working_directory.to_string_lossy().into_owned(),
        };

        // 5. 子代理会话 ID
        let child_session_id = format!("subagent-{agent_id}");

        // 6. 计算超时
        let agent_timeout = self.resolve_timeout(request);

        // 7. 执行（带超时包裹）
        let (mailbox_sender, mailbox_receiver) = mpsc::unbounded_channel();
        self.mailbox_router
            .mailboxes
            .insert(agent_id.clone(), mailbox_sender);
        let _mailbox_registration = MailboxRegistration {
            router: &self.mailbox_router,
            agent_id: agent_id.clone(),
        };
        let future = self.engine_factory.create_and_run(
            agent_id,
            &child_session_id,
            context,
            &model,
            &system_prompt,
            &request.prompt,
            &work_dir,
            mailbox_receiver,
            cancel,
            agent_def.max_turns,
        );

        let outcome = timeout(agent_timeout, future).await;
        let Ok((stop_reason, assistant_text, has_error)) = outcome else {
            warn!(
                "Sub-agent {agent_id} timed out after {}s",
                agent_timeout.as_secs()
            );
            let result = AgentResult::timeout(
                format!(
                    "<tool_use_error>Sub-agent '{agent_id}' (type={:?}) timed out after {} seconds.</tool_use_error>",
                    request.agent_type,
                    agent_timeout.as_secs()
                ),
                &request.prompt,
            );
            // Worktree 清理
            if request.isolation == IsolationMode::Worktree {
                self.cleanup_worktree(&work_dir).await;
            }
            return result;
        };

        // 8. Worktree 清理
        if request.isolation == IsolationMode::Worktree {
            self.cleanup_worktree(&work_dir).await;
        }

        // 9. 分类状态并构建结果
        let status =
            AgentStatus::classify(stop_reason.as_deref(), assistant_text.is_some(), has_error);

        let result_text = assistant_text.unwrap_or_else(|| "子代理未返回响应。".to_owned());

        let mut result = match status {
            AgentStatus::Completed => AgentResult::completed(result_text, &request.prompt),
            // AgentStatus::Failed falls through to wildcard below
            AgentStatus::MaxTurns => AgentResult {
                status: AgentStatus::MaxTurns,
                result: Some(result_text),
                prompt: request.prompt.clone(),
                output_file: None,
            },
            AgentStatus::Interrupted => AgentResult {
                status: AgentStatus::Interrupted,
                result: Some(result_text),
                prompt: request.prompt.clone(),
                output_file: None,
            },
            _ => AgentResult::failed(result_text, &request.prompt),
        };

        // 10. 截断结果
        result.truncate_result();
        info!("Sub-agent {agent_id} completed: status={}", status.as_str());
        result
    }

    /// 计算代理超时（对照旧 `resolveAgentTimeout`）。
    fn resolve_timeout(&self, request: &AgentRequest) -> Duration {
        let base = self.timeout_config.default_seconds;
        let max = self.timeout_config.max_seconds;
        let calculated = match request
            .agent_type
            .as_ref()
            .map(|t| t.to_ascii_lowercase())
            .as_deref()
        {
            Some("coding" | "frontend-dev" | "backend-dev") => base * 2,
            Some("verify" | "qa" | "verification") => base * 3,
            _ => base,
        };
        Duration::from_secs(calculated.min(max))
    }

    /// Worktree 清理：检查变更 → 合并 → 移除（对照旧 finally 块）。
    async fn cleanup_worktree(&self, work_dir: &str) {
        let path = std::path::Path::new(work_dir);
        match self.worktree_manager.has_changes(path).await {
            Ok(true) => {
                if let Err(error) = self.worktree_manager.merge_back(path).await {
                    crate::agent::worktree::log_preserved_worktree(path, &error);
                    return;
                }
            }
            Ok(false) => {}
            Err(error) => {
                crate::agent::worktree::log_preserved_worktree(path, &error);
                return;
            }
        }
        if let Err(error) = self.worktree_manager.remove_worktree(path).await {
            crate::agent::worktree::log_preserved_worktree(path, &error);
        }
    }
}

/// 构建子代理系统提示（对照旧 `buildAgentSystemPrompt`）。
fn build_agent_system_prompt(template: &str, task_prompt: &str, work_dir: &str) -> String {
    format!(
        "{template}\n\n\
         你的任务：\n{task_prompt}\n\n\
         上下文：\n\
         - 工作目录：{work_dir}\n\
         - 你可以使用文件读写工具、搜索工具和 bash。\n\
         - 你无法使用：AgentTool、TaskCreateTool、TaskStopTool。\n\
         - 完成任务并返回清晰、简洁的结果。\n\
         - 如果你修改了文件，在最终回复中列出所有修改的文件路径。\n\
         - 不要尝试超出上述范围的任务。"
    )
}

/// 全局禁用工具名集合（供工具池过滤使用，对照旧 `assembleToolPool`）。
#[must_use]
pub fn is_globally_denied(tool_name: &str) -> bool {
    GLOBALLY_DENIED_TOOLS.contains(&tool_name)
}

// ═══════════════════════════════════════════════════════════════
// RealSubAgentEngineFactory（Batch 8C Step 2）
// ═══════════════════════════════════════════════════════════════

/// 子代理工具子集白名单（对照旧 `assembleToolPool` 的准入过滤）。
///
/// 注入基础文件 / 检索 / Bash / 只读 Git 工具；**排除**：
/// - `Agent` / `TaskCreate` / `TaskUpdate` / `TaskStop`（防子代理递归派生）；
/// - `Memory` / `CtxInspect` / `EnterPlanMode` / `ExitPlanMode`（父级专属）。
///
/// 名单外或 `source` 中不存在的工具不会进入子代理注册表。
pub const SUB_AGENT_TOOL_NAMES: &[&str] = &[
    "Read",
    "Write",
    "Edit",
    "Bash",
    "Glob",
    "Grep",
    "ListDir",
    "GitDiff",
    "GitLog",
    "GitStatus",
    "SendMessage",
];

/// 按 [`SUB_AGENT_TOOL_NAMES`] 从全量注册表过滤出子代理工具注册表。
///
/// 名单中不存在于 `source` 的工具静默跳过（组合根装配顺序无关）。
#[must_use]
pub fn build_sub_agent_registry(source: &zk_tools::ToolRegistry) -> Arc<zk_tools::ToolRegistry> {
    build_sub_agent_registry_with_policy(source, true)
}

/// 按安全冻结策略构造子代理工具视图。
///
/// `allow_writes=false` 时 Write/Edit/Bash 在注册期即被移除，模型看不到这些
/// 工具；这比执行期拒绝更能避免未验收能力被模型误判为可用。
#[must_use]
pub fn build_sub_agent_registry_with_policy(
    source: &zk_tools::ToolRegistry,
    allow_writes: bool,
) -> Arc<zk_tools::ToolRegistry> {
    let mut sub = zk_tools::ToolRegistry::new();
    for name in SUB_AGENT_TOOL_NAMES {
        if !allow_writes && matches!(*name, "Write" | "Edit" | "Bash") {
            continue;
        }
        if let Some(tool) = source.get(name) {
            sub.register(tool);
        }
    }
    Arc::new(sub)
}

fn narrow_child_registry(
    source: &Arc<zk_tools::ToolRegistry>,
    allowed: Option<&std::collections::BTreeSet<String>>,
) -> Arc<zk_tools::ToolRegistry> {
    let Some(allowed) = allowed else {
        return Arc::clone(source);
    };
    let mut narrowed = zk_tools::ToolRegistry::new();
    for name in allowed {
        if let Some(tool) = source.get(name) {
            narrowed.register(tool);
        }
    }
    Arc::new(narrowed)
}

/// 真实子代理引擎工厂——用 [`crate::engine::Engine::sub_session`] 创建独立子
/// 会话引擎并运行一轮完整的多轮工具循环（对照旧 `SubAgentEngineFactory`）。
///
/// 替换占位实现 `PlaceholderEngineFactory`（后者恒返回 Err）。持有 DB / provider
/// 句柄与预过滤的子代理工具注册表——**不持有父 `Engine` 引用**，避免
/// `Engine → tools → SubAgentExecutor → factory → Engine` 循环依赖。
pub struct RealSubAgentEngineFactory {
    /// DB 句柄（子会话不落库，仅满足 `sub_session` 签名——`Db` 是轻量 Clone 句柄）。
    db: zk_db::Db,
    /// 共享 LLM provider。
    provider: Arc<dyn zk_llm::ChatProvider>,
    /// 子代理工具子集（预过滤自全量注册表）。
    sub_tools: Arc<zk_tools::ToolRegistry>,
    sink: Arc<dyn crate::MessageSink>,
    admission: Arc<dyn crate::ToolAdmission>,
    cost_tracker: Arc<dyn crate::CostTracker>,
    file_history: Arc<crate::FileHistoryService>,
    hooks: Arc<crate::HookService>,
    observability: Arc<dyn crate::ObservabilityRecorder>,
    compact_summarizer: Arc<dyn crate::context::compact::Summarizer>,
    tool_summarizer: Arc<dyn crate::LightModelSummarizer>,
}

impl RealSubAgentEngineFactory {
    /// 从全量工具注册表按安全策略构造，并显式注入全部生产安全端口。
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_production_services(
        db: zk_db::Db,
        provider: Arc<dyn zk_llm::ChatProvider>,
        sub_tools: Arc<zk_tools::ToolRegistry>,
        sink: Arc<dyn crate::MessageSink>,
        admission: Arc<dyn crate::ToolAdmission>,
        cost_tracker: Arc<dyn crate::CostTracker>,
        file_history: Arc<crate::FileHistoryService>,
        hooks: Arc<crate::HookService>,
        observability: Arc<dyn crate::ObservabilityRecorder>,
        compact_summarizer: Arc<dyn crate::context::compact::Summarizer>,
        tool_summarizer: Arc<dyn crate::LightModelSummarizer>,
    ) -> Self {
        Self {
            db,
            provider,
            sub_tools,
            sink,
            admission,
            cost_tracker,
            file_history,
            hooks,
            observability,
            compact_summarizer,
            tool_summarizer,
        }
    }
}

impl SubAgentEngineFactory for RealSubAgentEngineFactory {
    #[allow(clippy::too_many_lines)] // child Session/Run/checkpoint/Engine lifecycle is one boundary
    fn create_and_run(
        &self,
        agent_id: &str,
        session_id: &str,
        context: &ChildExecutionContext,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        work_dir: &str,
        mailbox: mpsc::UnboundedReceiver<AgentMailboxMessage>,
        cancel: CancellationToken,
        max_turns: u32,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = (Option<String>, Option<String>, bool)> + Send + '_>,
    > {
        let db = self.db.clone();
        let provider = Arc::clone(&self.provider);
        let tools = narrow_child_registry(&self.sub_tools, context.allowed_tools.as_ref());
        let sink = Arc::clone(&self.sink);
        let admission = Arc::clone(&self.admission);
        let cost_tracker = Arc::clone(&self.cost_tracker);
        let file_history = Arc::clone(&self.file_history);
        let hooks = Arc::clone(&self.hooks);
        let observability = Arc::clone(&self.observability);
        let compact_summarizer = Arc::clone(&self.compact_summarizer);
        let tool_summarizer = Arc::clone(&self.tool_summarizer);
        let parent_run_id = context.parent_run_id.clone();
        let parent_session_id = context.parent_session_id.clone();
        let agent_id = agent_id.to_owned();
        let session_id = session_id.to_owned();
        let run_id = format!("subrun-{}", uuid::Uuid::new_v4());
        let model = model.to_owned();
        let system_prompt = system_prompt.to_owned();
        let user_prompt = user_prompt.to_owned();
        let work_dir = work_dir.to_owned();
        info!(
            agent = %agent_id,
            session = session_id,
            model,
            "sub-agent engine created (real factory)"
        );
        Box::pin(async move {
            let agent_started_at = std::time::Instant::now();
            let mut agent_started = crate::ObservabilityEvent::new("agent", "start", "running");
            agent_started.session_id = Some(session_id.clone());
            agent_started.run_id = Some(run_id.clone());
            agent_started.attributes.insert(
                "agentId".to_owned(),
                serde_json::Value::String(agent_id.clone()),
            );
            observability.record(agent_started);
            sink.push(
                &parent_session_id,
                zk_protocol::ServerMessage::AgentStarted {
                    agent_id: agent_id.clone(),
                    prompt: user_prompt.clone(),
                },
            )
            .await;
            if let Err(error) = db
                .create_session_with_id(&session_id, &model, &work_dir)
                .await
            {
                tracing::error!(%session_id, %error, "failed to persist child session");
                let mut event = crate::ObservabilityEvent::new("agent", "complete", "error");
                event.session_id = Some(session_id.clone());
                event.run_id = Some(run_id.clone());
                event.duration_ms =
                    Some(u64::try_from(agent_started_at.elapsed().as_millis()).unwrap_or(u64::MAX));
                observability.record(event);
                return (
                    Some("error".to_owned()),
                    Some("AGENT_CHILD_SESSION_CREATE_FAILED".to_owned()),
                    true,
                );
            }
            if let Err(error) = db
                .start_run(
                    &run_id,
                    &session_id,
                    Some(&parent_run_id),
                    Some(zk_db::run::AGENT_TYPE_SUBAGENT),
                    &model,
                )
                .await
            {
                tracing::error!(%run_id, %error, "failed to persist child run");
                let mut event = crate::ObservabilityEvent::new("agent", "complete", "error");
                event.session_id = Some(session_id.clone());
                event.run_id = Some(run_id.clone());
                event.duration_ms =
                    Some(u64::try_from(agent_started_at.elapsed().as_millis()).unwrap_or(u64::MAX));
                observability.record(event);
                return (
                    Some("error".to_owned()),
                    Some("AGENT_CHILD_RUN_CREATE_FAILED".to_owned()),
                    true,
                );
            }
            let mut initial_checkpoint = zk_db::new_agent_checkpoint(
                &run_id,
                &session_id,
                &agent_id,
                0,
                serde_json::json!([{ "role": "user", "content": user_prompt }]),
            );
            initial_checkpoint.working_dir = Some(work_dir.clone());
            if let Err(error) = db.save_agent_checkpoint(&initial_checkpoint).await {
                tracing::error!(%run_id, %error, "failed to persist initial agent checkpoint");
                let _ = db
                    .terminate_run(&run_id, zk_db::run::EXIT_INTERNAL_ERROR, Some("checkpoint"))
                    .await;
                let mut event = crate::ObservabilityEvent::new("agent", "complete", "error");
                event.session_id = Some(session_id.clone());
                event.run_id = Some(run_id.clone());
                event.duration_ms =
                    Some(u64::try_from(agent_started_at.elapsed().as_millis()).unwrap_or(u64::MAX));
                observability.record(event);
                return (
                    Some("error".to_owned()),
                    Some("AGENT_CHECKPOINT_STORE_FAILED".to_owned()),
                    true,
                );
            }
            let child_sink: Arc<dyn crate::MessageSink> = Arc::new(ChildRoutingSink {
                inner: Arc::clone(&sink),
                parent_session_id: parent_session_id.clone(),
            });
            let engine = crate::engine::Engine::sub_session(
                db.clone(),
                provider,
                child_sink,
                tools,
                admission,
                cost_tracker,
                file_history,
                hooks,
            )
            .with_summarizers(compact_summarizer, tool_summarizer)
            .with_observability(Arc::clone(&observability));
            let checkpoint_work_dir = work_dir.clone();
            let config = crate::engine::SubAgentRunConfig {
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                model,
                system_prompt,
                user_prompt,
                work_dir,
                max_turns,
                mailbox,
            };
            let outcome = engine.run_sub_agent(config, cancel).await;
            let mut terminal_checkpoint = zk_db::new_agent_checkpoint(
                &run_id,
                &session_id,
                &agent_id,
                1,
                serde_json::json!({
                    "assistant": outcome.assistant_text,
                    "stopReason": outcome.stop_reason,
                    "hasError": outcome.has_error,
                }),
            );
            terminal_checkpoint.working_dir = Some(checkpoint_work_dir);
            if let Err(error) = db.save_agent_checkpoint(&terminal_checkpoint).await {
                tracing::error!(%run_id, %error, "failed to persist terminal agent checkpoint");
            }
            if outcome.has_error || outcome.stop_reason.as_deref() == Some("cancelled") {
                let reason = if outcome.stop_reason.as_deref() == Some("cancelled") {
                    zk_db::run::EXIT_USER_CANCELLED
                } else {
                    zk_db::run::EXIT_INTERNAL_ERROR
                };
                let _ = db
                    .terminate_run(&run_id, reason, outcome.stop_reason.as_deref())
                    .await;
                sink.push(
                    &parent_session_id,
                    zk_protocol::ServerMessage::AgentFailed {
                        agent_id: agent_id.clone(),
                        error: outcome
                            .stop_reason
                            .clone()
                            .unwrap_or_else(|| "sub-agent failed".to_owned()),
                    },
                )
                .await;
            } else {
                let _ = db.complete_run(&run_id, 0, 0.0, 0).await;
                sink.push(
                    &parent_session_id,
                    zk_protocol::ServerMessage::AgentCompleted {
                        agent_id: agent_id.clone(),
                        result: outcome.assistant_text.clone().unwrap_or_default(),
                    },
                )
                .await;
            }
            let outcome_name = if outcome.stop_reason.as_deref() == Some("cancelled") {
                "cancelled"
            } else if outcome.has_error {
                "error"
            } else {
                "ok"
            };
            let mut event = crate::ObservabilityEvent::new("agent", "complete", outcome_name);
            event.session_id = Some(session_id.clone());
            event.run_id = Some(run_id.clone());
            event.duration_ms =
                Some(u64::try_from(agent_started_at.elapsed().as_millis()).unwrap_or(u64::MAX));
            event
                .attributes
                .insert("agentId".to_owned(), serde_json::Value::String(agent_id));
            observability.record(event);
            (
                outcome.stop_reason,
                outcome.assistant_text,
                outcome.has_error,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_agent_tool_names_exclude_denied_and_include_basics() {
        for denied in [
            "Agent",
            "TaskCreate",
            "TaskUpdate",
            "TaskStop",
            "Memory",
            "CtxInspect",
            "EnterPlanMode",
            "ExitPlanMode",
        ] {
            assert!(
                !SUB_AGENT_TOOL_NAMES.contains(&denied),
                "denied tool leaked into sub-agent set: {denied}"
            );
        }
        for allowed in ["Read", "Write", "Edit", "Bash", "Glob", "Grep", "ListDir"] {
            assert!(
                SUB_AGENT_TOOL_NAMES.contains(&allowed),
                "basic tool missing from sub-agent set: {allowed}"
            );
        }
    }

    #[test]
    fn build_sub_agent_registry_empty_source_is_empty() {
        let src = zk_tools::ToolRegistry::new();
        let sub = build_sub_agent_registry(&src);
        assert!(sub.is_empty());
    }

    #[test]
    fn build_sub_agent_registry_filters_by_whitelist() {
        use futures::future::BoxFuture;
        use zk_tools::{Tool, ToolContext, ToolOutput};

        struct StubTool(&'static str);
        impl Tool for StubTool {
            fn name(&self) -> &str {
                self.0
            }
            fn description(&self) -> &'static str {
                "stub"
            }
            fn parameters(&self) -> serde_json::Value {
                serde_json::json!({ "type": "object", "properties": {} })
            }
            fn execute(
                &self,
                _input: serde_json::Value,
                _ctx: ToolContext,
            ) -> BoxFuture<'_, ToolOutput> {
                Box::pin(futures::future::ready(ToolOutput::ok("stub")))
            }
        }

        let mut src = zk_tools::ToolRegistry::new();
        src.register(Arc::new(StubTool("Read")));
        src.register(Arc::new(StubTool("Bash")));
        src.register(Arc::new(StubTool("Agent"))); // denied → must be filtered out
        src.register(Arc::new(StubTool("Memory"))); // denied → must be filtered out

        let sub = build_sub_agent_registry(&src);
        assert!(sub.get("Read").is_some());
        assert!(sub.get("Bash").is_some());
        assert!(sub.get("Agent").is_none());
        assert!(sub.get("Memory").is_none());
        assert_eq!(sub.len(), 2);
    }

    #[test]
    fn frozen_sub_agent_registry_hides_write_edit_and_bash() {
        use futures::future::BoxFuture;
        use zk_tools::{Tool, ToolContext, ToolOutput};

        struct StubTool(&'static str);
        impl Tool for StubTool {
            fn name(&self) -> &str {
                self.0
            }
            fn description(&self) -> &'static str {
                "stub"
            }
            fn parameters(&self) -> serde_json::Value {
                serde_json::json!({"type": "object", "properties": {}})
            }
            fn execute(
                &self,
                _input: serde_json::Value,
                _ctx: ToolContext,
            ) -> BoxFuture<'_, ToolOutput> {
                Box::pin(futures::future::ready(ToolOutput::ok("stub")))
            }
        }

        let mut src = zk_tools::ToolRegistry::new();
        for name in [
            "Read", "ListDir", "Glob", "Grep", "GitDiff", "Write", "Edit", "Bash",
        ] {
            src.register(Arc::new(StubTool(name)));
        }
        let sub = build_sub_agent_registry_with_policy(&src, false);
        for name in ["Read", "ListDir", "Glob", "Grep", "GitDiff"] {
            assert!(sub.get(name).is_some(), "read-only tool missing: {name}");
        }
        for name in ["Write", "Edit", "Bash"] {
            assert!(sub.get(name).is_none(), "write-capable tool leaked: {name}");
        }
    }

    #[test]
    fn skill_policy_narrows_child_registry_without_adding_unknown_tools() {
        use futures::future::BoxFuture;
        use zk_tools::{Tool, ToolContext, ToolOutput};

        struct StubTool(&'static str);
        impl Tool for StubTool {
            fn name(&self) -> &str {
                self.0
            }
            fn description(&self) -> &'static str {
                "stub"
            }
            fn parameters(&self) -> serde_json::Value {
                serde_json::json!({"type": "object"})
            }
            fn execute(
                &self,
                _input: serde_json::Value,
                _ctx: ToolContext,
            ) -> BoxFuture<'_, ToolOutput> {
                Box::pin(futures::future::ready(ToolOutput::ok("stub")))
            }
        }

        let mut source = zk_tools::ToolRegistry::new();
        source.register(Arc::new(StubTool("Read")));
        source.register(Arc::new(StubTool("Write")));
        let source = Arc::new(source);
        let allowed =
            std::collections::BTreeSet::from(["Read".to_owned(), "NotRegistered".to_owned()]);
        let narrowed = narrow_child_registry(&source, Some(&allowed));
        assert_eq!(narrowed.names(), ["Read"]);
        assert!(narrowed.get("Write").is_none());
        assert!(narrowed.get("NotRegistered").is_none());
    }

    struct StubEngineFactory;

    fn child_context() -> ChildExecutionContext {
        ChildExecutionContext {
            parent_session_id: "parent-session".to_owned(),
            parent_run_id: "parent-run".to_owned(),
            working_directory: PathBuf::from("/tmp"),
            tool_use_id: "tool-use-1".to_owned(),
            allowed_tools: None,
        }
    }

    impl SubAgentEngineFactory for StubEngineFactory {
        fn create_and_run(
            &self,
            _agent_id: &str,
            _session_id: &str,
            _context: &ChildExecutionContext,
            _model: &str,
            _system_prompt: &str,
            _user_prompt: &str,
            _work_dir: &str,
            _mailbox: mpsc::UnboundedReceiver<AgentMailboxMessage>,
            _cancel: CancellationToken,
            _max_turns: u32,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = (Option<String>, Option<String>, bool)>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(async { (Some("end_turn".to_owned()), Some("done".to_owned()), false) })
        }
    }

    #[tokio::test]
    async fn execute_sync_completes() {
        let concurrency = Arc::new(AgentConcurrencyController::default());
        let executor = SubAgentExecutor::new(
            concurrency,
            Arc::new(StubEngineFactory),
            WorktreeManager::for_repo(
                std::env::current_dir().expect("current dir"),
                Arc::new(crate::agent::worktree::SystemGitCommandRunner),
            )
            .expect("worktree manager"),
            AgentTimeoutConfig::default(),
        );
        let request = AgentRequest::new(
            "test-1",
            "test prompt",
            Some("general-purpose".to_owned()),
            Some("test-model".to_owned()),
            IsolationMode::None,
            false,
        );
        let result = executor.execute_sync(&request, &child_context()).await;
        assert_eq!(result.status, AgentStatus::Completed);
        assert_eq!(result.result, Some("done".to_owned()));
    }

    struct MailboxFactory;

    impl SubAgentEngineFactory for MailboxFactory {
        fn create_and_run(
            &self,
            _agent_id: &str,
            _session_id: &str,
            _context: &ChildExecutionContext,
            _model: &str,
            _system_prompt: &str,
            _user_prompt: &str,
            _work_dir: &str,
            mut mailbox: mpsc::UnboundedReceiver<AgentMailboxMessage>,
            _cancel: CancellationToken,
            _max_turns: u32,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = (Option<String>, Option<String>, bool)>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(async move {
                let content = mailbox.recv().await.map(|message| message.content);
                (Some("end_turn".to_owned()), content, false)
            })
        }
    }

    fn mailbox_executor(router: Arc<AgentMailboxRouter>) -> Arc<SubAgentExecutor> {
        Arc::new(SubAgentExecutor::new_with_mailbox_router(
            Arc::new(AgentConcurrencyController::default()),
            Arc::new(MailboxFactory),
            WorktreeManager::for_repo(
                std::env::current_dir().expect("current dir"),
                Arc::new(crate::agent::worktree::SystemGitCommandRunner),
            )
            .expect("worktree manager"),
            AgentTimeoutConfig::default(),
            router,
        ))
    }

    async fn wait_until_active(router: &AgentMailboxRouter, agent_id: &str) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !router.has_active_agent(agent_id) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("agent becomes active");
    }

    #[tokio::test]
    async fn mailbox_delivers_only_while_child_is_active() {
        let router = Arc::new(AgentMailboxRouter::default());
        let executor = mailbox_executor(Arc::clone(&router));
        let request = AgentRequest::new(
            "mailbox-agent",
            "wait",
            None,
            Some("test-model".to_owned()),
            IsolationMode::None,
            false,
        );
        let context = child_context();
        let task = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.execute_sync(&request, &context).await }
        });
        wait_until_active(&router, "mailbox-agent").await;
        router
            .send_message(
                "mailbox-agent",
                AgentMailboxMessage {
                    message_id: "message-1".to_owned(),
                    parent_run_id: "parent-run".to_owned(),
                    from_id: "coordinator".to_owned(),
                    content: "continue with verification".to_owned(),
                },
            )
            .expect("active target accepts delivery");
        let result = task.await.expect("execution task");
        assert_eq!(result.result.as_deref(), Some("continue with verification"));
        assert!(!router.has_active_agent("mailbox-agent"));
        assert!(
            router
                .send_message(
                    "mailbox-agent",
                    AgentMailboxMessage {
                        message_id: "message-2".to_owned(),
                        parent_run_id: "parent-run".to_owned(),
                        from_id: "coordinator".to_owned(),
                        content: "too late".to_owned(),
                    },
                )
                .unwrap_err()
                .starts_with("AGENT_NOT_FOUND:")
        );
    }

    #[tokio::test]
    async fn aborted_execution_drops_mailbox_registration() {
        let router = Arc::new(AgentMailboxRouter::default());
        let executor = mailbox_executor(Arc::clone(&router));
        let request = AgentRequest::new(
            "aborted-agent",
            "wait forever",
            None,
            Some("test-model".to_owned()),
            IsolationMode::None,
            false,
        );
        let context = child_context();
        let task = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.execute_sync(&request, &context).await }
        });
        wait_until_active(&router, "aborted-agent").await;
        task.abort();
        let _ = task.await;
        assert!(!router.has_active_agent("aborted-agent"));
    }

    #[tokio::test]
    async fn caller_cancellation_reaches_the_child_engine_factory() {
        struct CancelAwareFactory;
        impl SubAgentEngineFactory for CancelAwareFactory {
            fn create_and_run(
                &self,
                _agent_id: &str,
                _session_id: &str,
                _context: &ChildExecutionContext,
                _model: &str,
                _system_prompt: &str,
                _user_prompt: &str,
                _work_dir: &str,
                _mailbox: mpsc::UnboundedReceiver<AgentMailboxMessage>,
                cancel: CancellationToken,
                _max_turns: u32,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = (Option<String>, Option<String>, bool)>
                        + Send
                        + '_,
                >,
            > {
                Box::pin(async move {
                    cancel.cancelled().await;
                    (Some("cancelled".to_owned()), None, false)
                })
            }
        }

        let executor = SubAgentExecutor::new(
            Arc::new(AgentConcurrencyController::default()),
            Arc::new(CancelAwareFactory),
            WorktreeManager::for_repo(
                std::env::current_dir().expect("current dir"),
                Arc::new(crate::agent::worktree::SystemGitCommandRunner),
            )
            .expect("worktree manager"),
            AgentTimeoutConfig::default(),
        );
        let request = AgentRequest::new(
            "cancel-test",
            "wait",
            None,
            Some("test-model".to_owned()),
            IsolationMode::None,
            false,
        );
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = executor
            .execute_sync_with_cancel(&request, &child_context(), cancel)
            .await;
        assert_eq!(result.status, AgentStatus::Interrupted);
    }

    #[tokio::test]
    async fn execute_sync_timeout() {
        struct SlowFactory;
        impl SubAgentEngineFactory for SlowFactory {
            fn create_and_run(
                &self,
                _a: &str,
                _b: &str,
                _context: &ChildExecutionContext,
                _c: &str,
                _d: &str,
                _e: &str,
                _f: &str,
                _mailbox: mpsc::UnboundedReceiver<AgentMailboxMessage>,
                _cancel: CancellationToken,
                _g: u32,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = (Option<String>, Option<String>, bool)>
                        + Send
                        + '_,
                >,
            > {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    (Some("end_turn".to_owned()), Some("late".to_owned()), false)
                })
            }
        }
        let concurrency = Arc::new(AgentConcurrencyController::default());
        let config = AgentTimeoutConfig {
            default_seconds: 1, // 1 second for test
            max_seconds: 1,
            ..Default::default()
        };
        let executor = SubAgentExecutor::new(
            concurrency,
            Arc::new(SlowFactory),
            WorktreeManager::for_repo(
                std::env::current_dir().expect("current dir"),
                Arc::new(crate::agent::worktree::SystemGitCommandRunner),
            )
            .expect("worktree manager"),
            config,
        );
        let request = AgentRequest::new(
            "timeout-test",
            "x",
            None,
            Some("test-model".to_owned()),
            IsolationMode::None,
            false,
        );
        let result = executor.execute_sync(&request, &child_context()).await;
        assert!(result.is_timeout());
    }

    #[test]
    fn timeout_config_defaults() {
        let config = AgentTimeoutConfig::default();
        assert_eq!(config.default_seconds, 300);
        assert_eq!(config.max_seconds, 1800);
        assert_eq!(config.graceful_shutdown_seconds, 30);
    }

    #[test]
    fn is_globally_denied_checks() {
        assert!(is_globally_denied("Agent"));
        assert!(is_globally_denied("TaskCreate"));
        assert!(!is_globally_denied("Read"));
    }

    #[test]
    fn system_prompt_includes_task() {
        let prompt = build_agent_system_prompt("template", "do stuff", "/work");
        assert!(prompt.contains("template"));
        assert!(prompt.contains("do stuff"));
        assert!(prompt.contains("/work"));
    }
}
