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
//! 超时包裹 → 完整结果交给统一 `TaskResult` 限额事务 → release_slot
//!
//! 有意差异：Java 的 checkpoint 恢复 / fork / team 路由属 Phase 2+，
//! 本批仅实现同步执行路径核心段。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use sha2::Digest as _;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use zk_db::TaskBudgetLimits;

use crate::agent::types::{
    AgentDefinition, AgentRequest, AgentResult, AgentStatus, GLOBALLY_DENIED_TOOLS, IsolationMode,
    READ_ONLY_CHILD_TOOLS, WRITE_CHILD_TOOLS,
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
        child_session_id: &'a str,
        message: zk_protocol::ServerMessage,
    ) -> futures::future::BoxFuture<'a, ()> {
        self.inner
            .push_from(&self.parent_session_id, child_session_id, message)
    }
}

/// IDs durably committed by `TaskRuntime` before a child executor is spawned.
///
/// The greenfield path uses UUIDv4 for all three identities. Keeping this value
/// separate from [`ChildExecutionContext`] lets the composition root move to
/// submit-before-spawn semantics without treating a process-local agent ID as a
/// durable Task or Run identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistedChildExecution {
    /// Logical Task identity.
    pub task_id: String,
    /// Execution attempt identity.
    pub run_id: String,
    /// Internal transcript Session identity.
    pub session_id: String,
    /// Typed context checkpoint copied onto a restart attempt. Ordinary first
    /// attempts leave this empty.
    pub recovery_checkpoint: Option<serde_json::Value>,
}

impl PersistedChildExecution {
    /// Validate and construct a pre-created execution identity.
    ///
    /// # Errors
    /// Returns a stable validation error if any identity is not a full UUIDv4.
    pub fn try_new(
        task_id: impl Into<String>,
        run_id: impl Into<String>,
        session_id: impl Into<String>,
    ) -> Result<Self, String> {
        let identity = Self {
            task_id: task_id.into(),
            run_id: run_id.into(),
            session_id: session_id.into(),
            recovery_checkpoint: None,
        };
        for (field, value) in [
            ("taskId", identity.task_id.as_str()),
            ("runId", identity.run_id.as_str()),
            ("sessionId", identity.session_id.as_str()),
        ] {
            let parsed = uuid::Uuid::parse_str(value)
                .map_err(|_| format!("AGENT_PRECREATED_{field}_INVALID"))?;
            if parsed.get_version() != Some(uuid::Version::Random) {
                return Err(format!("AGENT_PRECREATED_{field}_INVALID"));
            }
        }
        Ok(identity)
    }

    /// Bind the exact checkpoint that the database recovery transaction copied
    /// to this Run.
    #[must_use]
    pub fn with_recovery_checkpoint(mut self, checkpoint: serde_json::Value) -> Self {
        self.recovery_checkpoint = Some(checkpoint);
        self
    }
}

/// 子代理引擎工厂——反转注入端口，解耦循环依赖。
///
/// zk-server 组合根装配具体实现：创建子代理的 `Engine` 实例并执行
/// 一轮完整的多轮工具循环，返回最终 `stop_reason` 与助手回复文本。
pub trait SubAgentEngineFactory: Send + Sync {
    /// Return the exact tool names that would be sent for this child context.
    /// Production uses this to keep prompt capability claims aligned with the
    /// request tool directory; lightweight factories may expose none.
    fn available_tool_names(&self, _context: &ChildExecutionContext) -> Vec<String> {
        Vec::new()
    }

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

    /// Execute a child whose Task, Run and internal Session already exist.
    ///
    /// The default keeps lightweight test factories source-compatible.
    /// Production factories must override this and must not create a second
    /// Session or Run.
    fn create_and_run_precreated(
        &self,
        execution: &PersistedChildExecution,
        budget: TaskBudgetLimits,
        agent_id: &str,
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
        let _ = budget;
        self.create_and_run(
            agent_id,
            &execution.session_id,
            context,
            model,
            system_prompt,
            user_prompt,
            work_dir,
            mailbox,
            cancel,
            max_turns,
        )
    }

    /// Record why the executor asked the child to stop before cancellation is
    /// delivered. Implementations may use this to persist a precise terminal
    /// reason after the normal child loop observes its cancellation token.
    fn note_abort(&self, _session_id: &str, _reason: &'static str) {}

    /// Persist a terminal state when the child did not cooperate within the
    /// graceful shutdown window.
    ///
    /// This hook runs before the non-cooperative child future is dropped, so a
    /// timeout never relies on code after `run_sub_agent().await` being reached.
    fn force_terminalize<'a>(
        &'a self,
        _agent_id: &'a str,
        _session_id: &'a str,
        _parent_session_id: &'a str,
        _reason: &'static str,
    ) -> futures::future::BoxFuture<'a, ()> {
        Box::pin(futures::future::ready(()))
    }
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
    /// Whether this execution may see the separately gated write-capable child
    /// tool set. The composition root derives this from isolation and feature
    /// gates; Agent type policy may only narrow it.
    pub allow_write_tools: bool,
    /// Optional allowlist applied only to write-capable child tools. `None`
    /// means every write tool admitted by `allow_write_tools`; Agent type policy
    /// uses this to expose only `Bash` to an isolated Verification run.
    pub write_tool_allowlist: Option<std::collections::BTreeSet<String>>,
    /// Whether project rules may be added to the child system prompt. Agent
    /// definitions can only turn this off (`Explore` and `Plan` do so).
    pub include_project_prompt: bool,
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

/// Cancels only the child lifecycle token when its awaiting caller is dropped.
///
/// The inner lifecycle task is deliberately detached on caller abort so it can
/// terminalize durably; this guard supplies the cancellation edge that tells the
/// detached task to stop immediately instead of waiting for its full deadline.
struct CancelChildOnDrop {
    token: CancellationToken,
    armed: bool,
}

enum ChildExecutionEnd {
    Finished((Option<String>, Option<String>, bool)),
    TimedOut,
    Cancelled,
}

fn stable_child_error_code(stop_reason: Option<&str>) -> Option<&'static str> {
    match stop_reason {
        Some("BUDGET_EXHAUSTED") => Some("BUDGET_EXHAUSTED"),
        Some("TOKEN_BUDGET_EXHAUSTED") => Some("TOKEN_BUDGET_EXHAUSTED"),
        Some("COST_BUDGET_EXHAUSTED") => Some("COST_BUDGET_EXHAUSTED"),
        Some("BUDGET_USAGE_INCOMPLETE") => Some("BUDGET_USAGE_INCOMPLETE"),
        Some("BUDGET_PRICE_UNKNOWN") => Some("BUDGET_PRICE_UNKNOWN"),
        Some("LLM_BUDGET_NOT_CONFIGURED") => Some("LLM_BUDGET_NOT_CONFIGURED"),
        Some("TASK_DEADLINE_EXCEEDED" | "timeout") => Some("TIMEOUT"),
        Some("max_turns") => Some("MAX_TURNS"),
        _ => None,
    }
}

impl CancelChildOnDrop {
    fn new(token: CancellationToken) -> Self {
        Self { token, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelChildOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.token.cancel();
        }
    }
}

impl Drop for MailboxRegistration<'_> {
    fn drop(&mut self) {
        self.router.mailboxes.remove(&self.agent_id);
    }
}

/// 子代理执行器——连接 AgentTool 与引擎核心循环。
#[derive(Clone)]
pub struct SubAgentExecutor {
    /// 并发控制器（全局 ≤30 / 会话 ≤10 / 嵌套 ≤3）。
    concurrency: Arc<AgentConcurrencyController>,
    /// 引擎工厂（反转注入）。
    engine_factory: Arc<dyn SubAgentEngineFactory>,
    /// Worktree 管理器。
    worktree_manager: Arc<WorktreeManager>,
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
            default_seconds: 1800,
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
            worktree_manager: Arc::new(worktree_manager),
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
        self.execute_with_identity(request, context, None, None, cancel)
            .await
    }

    /// Execute a TaskRuntime submission that was committed before spawning.
    ///
    /// This is the only entrypoint the unified runtime should use. The legacy
    /// [`Self::execute_sync_with_cancel`] path remains temporarily for callers
    /// that have not yet moved submission into the composition root.
    pub async fn execute_precreated_with_cancel(
        &self,
        request: &AgentRequest,
        context: &ChildExecutionContext,
        execution: &PersistedChildExecution,
        budget: TaskBudgetLimits,
        cancel: CancellationToken,
    ) -> AgentResult {
        if let Err(error) = PersistedChildExecution::try_new(
            execution.task_id.clone(),
            execution.run_id.clone(),
            execution.session_id.clone(),
        ) {
            return AgentResult::failed(error, &request.prompt);
        }
        self.execute_with_identity(
            request,
            context,
            Some(execution.clone()),
            Some(budget),
            cancel,
        )
        .await
    }

    async fn execute_with_identity(
        &self,
        request: &AgentRequest,
        context: &ChildExecutionContext,
        execution: Option<PersistedChildExecution>,
        budget: Option<TaskBudgetLimits>,
        cancel: CancellationToken,
    ) -> AgentResult {
        // The lifecycle task owns the concurrency lease, child future and
        // worktree finalization. If an outer Tool/Task future is aborted, dropping
        // this JoinHandle detaches (rather than cancels) the lifecycle task, which
        // can still cancel at its own deadline and persist a terminal outcome.
        let owned = self.clone();
        let request = request.clone();
        let context = context.clone();
        let prompt = request.prompt.clone();
        let lifecycle_cancel = cancel.child_token();
        let mut cancel_on_drop = CancelChildOnDrop::new(lifecycle_cancel.clone());
        let joined = tokio::spawn(async move {
            owned
                .execute_owned(request, context, execution, budget, lifecycle_cancel)
                .await
        })
        .await;
        match joined {
            Ok(result) => {
                cancel_on_drop.disarm();
                result
            }
            Err(error) => {
                AgentResult::failed(format!("AGENT_LIFECYCLE_TASK_FAILED: {error}"), prompt)
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_owned(
        self,
        request: AgentRequest,
        context: ChildExecutionContext,
        execution: Option<PersistedChildExecution>,
        budget: Option<TaskBudgetLimits>,
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
        let effective_context = apply_agent_tool_policy(&context, agent_def, request.isolation);

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

        // 3. 创建隔离工作目录
        let work_dir = match request.isolation {
            IsolationMode::Worktree => {
                match self.worktree_manager.create_worktree(agent_id).await {
                    Ok(path) => path.to_string_lossy().into_owned(),
                    Err(e) => {
                        warn!("Worktree creation failed closed for {agent_id}: {e}");
                        return AgentResult::failed(
                            format!("AGENT_WORKTREE_CREATE_FAILED: {e}"),
                            &request.prompt,
                        );
                    }
                }
            }
            _ => context.working_directory.to_string_lossy().into_owned(),
        };

        // 4. Build the prompt from the same narrowed directory the production
        // factory will send. Capability statements must never outlive the tools
        // that make them true.
        let available_tools = self.engine_factory.available_tool_names(&effective_context);
        let system_prompt =
            build_agent_system_prompt(agent_def, &request.prompt, &work_dir, &available_tools);

        // 5. 子代理会话 ID
        let child_session_id = execution.as_ref().map_or_else(
            || format!("subagent-{agent_id}"),
            |identity| identity.session_id.clone(),
        );

        // 6. 计算超时
        let agent_timeout = self.resolve_timeout(&request);

        // 7. 执行（带超时包裹）
        let (mailbox_sender, mailbox_receiver) = mpsc::unbounded_channel();
        self.mailbox_router
            .mailboxes
            .insert(agent_id.clone(), mailbox_sender);
        let _mailbox_registration = MailboxRegistration {
            router: &self.mailbox_router,
            agent_id: agent_id.clone(),
        };
        let child_cancel = cancel.child_token();
        let mut future = match execution.as_ref() {
            Some(identity) => self.engine_factory.create_and_run_precreated(
                identity,
                budget.unwrap_or_default(),
                agent_id,
                &effective_context,
                &model,
                &system_prompt,
                &request.prompt,
                &work_dir,
                mailbox_receiver,
                child_cancel.clone(),
                agent_def.max_turns,
            ),
            None => self.engine_factory.create_and_run(
                agent_id,
                &child_session_id,
                &effective_context,
                &model,
                &system_prompt,
                &request.prompt,
                &work_dir,
                mailbox_receiver,
                child_cancel.clone(),
                agent_def.max_turns,
            ),
        };

        let deadline = tokio::time::sleep(agent_timeout);
        tokio::pin!(deadline);
        let end = tokio::select! {
            biased;
            () = cancel.cancelled() => ChildExecutionEnd::Cancelled,
            output = &mut future => ChildExecutionEnd::Finished(output),
            () = &mut deadline, if execution.is_none() => ChildExecutionEnd::TimedOut,
        };

        let (stop_reason, assistant_text, has_error) = match end {
            ChildExecutionEnd::Finished(output) => output,
            ChildExecutionEnd::TimedOut | ChildExecutionEnd::Cancelled => {
                let (reason, timed_out) = match end {
                    ChildExecutionEnd::TimedOut => ("timeout", true),
                    ChildExecutionEnd::Cancelled => ("cancelled", false),
                    ChildExecutionEnd::Finished(_) => unreachable!(),
                };
                self.engine_factory.note_abort(&child_session_id, reason);
                child_cancel.cancel();

                let graceful = Duration::from_secs(self.timeout_config.graceful_shutdown_seconds);
                let graceful_result = timeout(graceful, &mut future).await;
                if graceful_result.is_err() {
                    warn!(
                        "Sub-agent {agent_id} did not stop within {}s grace; forcing terminal state",
                        graceful.as_secs()
                    );
                    self.engine_factory
                        .force_terminalize(
                            agent_id,
                            &child_session_id,
                            &context.parent_session_id,
                            reason,
                        )
                        .await;
                }
                drop(future);

                if request.isolation == IsolationMode::Worktree {
                    self.finalize_worktree(&work_dir).await;
                }
                if execution.is_some()
                    && let Ok((_, Some(text), _)) = graceful_result
                    && !text.trim().is_empty()
                {
                    return AgentResult {
                        status: AgentStatus::Interrupted,
                        result: Some(text),
                        prompt: request.prompt.clone(),
                        output_file: None,
                        error_code: Some("SUBAGENT_STOPPED_PARTIAL".to_owned()),
                    };
                }
                if timed_out {
                    return AgentResult::timeout(
                        format!(
                            "<tool_use_error>Sub-agent '{agent_id}' (type={:?}) timed out after {} seconds.</tool_use_error>",
                            request.agent_type,
                            agent_timeout.as_secs()
                        ),
                        &request.prompt,
                    );
                }
                return AgentResult {
                    status: AgentStatus::Interrupted,
                    result: Some(format!("Sub-agent '{agent_id}' was cancelled.")),
                    prompt: request.prompt.clone(),
                    output_file: None,
                    error_code: Some("USER_CANCELLED".to_owned()),
                };
            }
        };

        // 8. Worktree 清理
        if request.isolation == IsolationMode::Worktree {
            self.finalize_worktree(&work_dir).await;
        }

        // 9. 分类状态并构建结果
        let status =
            AgentStatus::classify(stop_reason.as_deref(), assistant_text.is_some(), has_error);
        let error_code = stable_child_error_code(stop_reason.as_deref()).map(str::to_owned);

        let result_text = assistant_text.unwrap_or_else(|| "子代理未返回响应。".to_owned());

        let result = match status {
            AgentStatus::Completed => AgentResult::completed(result_text, &request.prompt),
            // AgentStatus::Failed falls through to wildcard below
            AgentStatus::MaxTurns => AgentResult {
                status: AgentStatus::MaxTurns,
                result: Some(result_text),
                prompt: request.prompt.clone(),
                output_file: None,
                error_code: Some("MAX_TURNS".to_owned()),
            },
            AgentStatus::Interrupted => AgentResult {
                status: AgentStatus::Interrupted,
                result: Some(result_text),
                prompt: request.prompt.clone(),
                output_file: None,
                error_code: Some("USER_CANCELLED".to_owned()),
            },
            AgentStatus::BudgetExhausted => AgentResult {
                status: AgentStatus::BudgetExhausted,
                result: Some(result_text),
                prompt: request.prompt.clone(),
                output_file: None,
                error_code: Some(error_code.unwrap_or_else(|| "BUDGET_EXHAUSTED".to_owned())),
            },
            AgentStatus::Timeout => AgentResult::timeout(result_text, &request.prompt),
            _ => AgentResult::failed_with_code(
                result_text,
                &request.prompt,
                error_code.unwrap_or_else(|| "AGENT_EXECUTION_FAILED".to_owned()),
            ),
        };

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

    /// Finalize an isolated worktree without ever merging into the parent.
    ///
    /// A changed worktree is preserved as an explicit hand-off artifact. An
    /// unchanged one is safe to remove. Inspection/removal failures also
    /// preserve it, because cleanup uncertainty must fail closed.
    async fn finalize_worktree(&self, work_dir: &str) {
        let path = std::path::Path::new(work_dir);
        match self.worktree_manager.has_changes(path).await {
            Ok(true) => {
                crate::agent::worktree::log_preserved_worktree(
                    path,
                    "isolated child produced changes; automatic merge is disabled",
                );
                return;
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

/// Build a child prompt whose capability statements come from the exact
/// narrowed tool directory. Role templates describe policy; this suffix states
/// what the concrete execution can actually do.
fn build_agent_system_prompt(
    definition: &AgentDefinition,
    task_prompt: &str,
    work_dir: &str,
    available_tools: &[String],
) -> String {
    let tools = available_tools
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let tool_directory = if tools.is_empty() {
        "（无）".to_owned()
    } else {
        tools.iter().copied().collect::<Vec<_>>().join(", ")
    };
    let has_bash = tools.contains("Bash");
    let has_write = tools
        .iter()
        .any(|name| matches!(*name, "Write" | "Edit" | "NotebookEdit"));
    let mut capability_rules = Vec::new();
    if has_write {
        capability_rules.push("写工具已提供；仅在任务要求且符合角色约束时修改文件。".to_owned());
    } else {
        capability_rules.push(
            "没有提供 Write、Edit 或 NotebookEdit；必须保持只读，不得声称已修改文件。".to_owned(),
        );
    }
    if has_bash {
        capability_rules.push("Bash 已提供；只运行符合当前角色约束的命令。".to_owned());
    } else {
        capability_rules
            .push("没有提供 Bash；不得声称已运行构建、测试、lint、类型检查或其他命令。".to_owned());
    }
    if definition.name == "Explore" {
        let mut search_sequence = Vec::new();
        for (tool, strategy) in [
            ("Glob", "按文件名或扩展名缩小候选范围"),
            ("Grep", "对候选目录做精确文本或正则匹配"),
            ("Read", "读取已确定的文件并核对上下文"),
        ] {
            if tools.contains(tool) {
                search_sequence.push(tool);
                capability_rules.push(format!("{tool} 已提供；可用于{strategy}。"));
            }
        }
        if search_sequence.is_empty() {
            capability_rules
                .push("没有提供基础代码搜索工具；只能使用当前目录中的其他只读能力。".to_owned());
        } else if search_sequence.len() > 1 {
            capability_rules.push(format!(
                "组合搜索时按 {} 的顺序逐步缩小范围。",
                search_sequence.join(" → ")
            ));
        }
        if tools.contains("CodeIntel") {
            capability_rules
                .push("CodeIntel 已在目录中，可用于符号、代码地图或依赖分析。".to_owned());
        }
    }
    if definition.name == "Verification" && !has_bash {
        capability_rules.push(
            "本次只能静态检查；静态确认缺陷时最终为 VERDICT: FAIL；若未确认缺陷，所有未执行项标为 UNVERIFIED，最终为 VERDICT: PARTIAL；不得输出 VERDICT: PASS。"
                .to_owned(),
        );
    }
    let capability_rules = capability_rules
        .into_iter()
        .map(|rule| format!("- {rule}"))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "{}\n\n\
         你的任务：\n{task_prompt}\n\n\
         上下文：\n\
         - 工作目录：{work_dir}\n\
         - 当前工具目录（唯一权威）：{tool_directory}\n\
         - 你无法递归启动 Agent、Task 或 Swarm。\n\
         - 完成任务并返回清晰、简洁的结果。\n\
         - 不要尝试超出上述范围的任务。\n\n\
         运行时能力约束：\n{capability_rules}",
        definition.system_prompt_template
    )
}

/// Resolve the final child system prompt exactly once per first attempt.
///
/// Project context is keyed by the parent's authorized workspace, not an
/// ephemeral worktree. A recovery checkpoint wins before any database or file
/// lookup, so resumed attempts cannot drift when project rules change.
async fn resolve_child_system_prompt(
    db: &zk_db::Db,
    generated_prompt: String,
    include_project_prompt: bool,
    authorized_working_dir: &str,
    recovery_checkpoint: Option<&serde_json::Value>,
) -> String {
    if let Some(checkpoint) = recovery_checkpoint {
        return checkpoint
            .get("systemPrompt")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(&generated_prompt)
            .to_owned();
    }
    if !include_project_prompt {
        return generated_prompt;
    }

    let working_dir_hash = format!(
        "{:x}",
        sha2::Sha256::digest(authorized_working_dir.as_bytes())
    );
    let durable_context = match db.find_project_context(&working_dir_hash).await {
        Ok(Some(record)) => match record.snapshot {
            serde_json::Value::String(text) => Some(text),
            value => serde_json::to_string_pretty(&value).ok(),
        },
        Ok(None) => None,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "failed to load child project context from SQLite; using file fallback"
            );
            None
        }
    };
    let loader = crate::prompt::ProjectPromptLoader::new();
    let Some(project_context) = crate::prompt::dynamic::project_context_section(
        durable_context.as_deref(),
        Some(&loader),
        authorized_working_dir,
    ) else {
        return generated_prompt;
    };
    format!("{generated_prompt}\n\n{project_context}")
}

/// 全局禁用工具名集合（供工具池过滤使用，对照旧 `assembleToolPool`）。
#[must_use]
pub fn is_globally_denied(tool_name: &str) -> bool {
    GLOBALLY_DENIED_TOOLS.contains(&tool_name)
}

// ═══════════════════════════════════════════════════════════════
// RealSubAgentEngineFactory（Batch 8C Step 2）
// ═══════════════════════════════════════════════════════════════

/// 子代理可用工具的公开安全合同。
///
/// 注入基础文件 / 检索 / Bash / 只读 Git 工具；**排除**：
/// - `Agent` / `TaskCreate` / `TaskUpdate` / `TaskStop`（防子代理递归派生）；
/// - `Memory` / `CtxInspect` / `EnterPlanMode` / `ExitPlanMode`（父级专属）。
///
/// 实际注册表由 [`build_sub_agent_registry_with_policy`] 在调用时遍历
/// `source` 快照生成；此常量只保留给目录/诊断 API 和契约测试。
pub const SUB_AGENT_TOOL_NAMES: &[&str] = &[
    "Read",
    "ListDir",
    "Glob",
    "Grep",
    "GitDiff",
    "GitLog",
    "GitStatus",
    "WebSearch",
    "WebFetch",
    "ListMcpResources",
    "ReadMcpResource",
    "Snip",
    "TerminalCapture",
    "ToolSearch",
    "Write",
    "Edit",
    "Bash",
    "NotebookEdit",
];

/// 按 [`SUB_AGENT_TOOL_NAMES`] 从全量注册表过滤出子代理工具注册表。
///
/// 名单中不存在于 `source` 的工具静默跳过（组合根装配顺序无关）。
#[must_use]
pub fn build_sub_agent_registry(source: &zk_tools::ToolRegistry) -> Arc<zk_tools::ToolRegistry> {
    build_sub_agent_registry_with_policy(source, false)
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
    Arc::new(source.filtered_by(move |name, tool| {
        if is_globally_denied(name) {
            return false;
        }
        READ_ONLY_CHILD_TOOLS.contains(&name)
            || matches!(tool.child_access(), zk_tools::ChildToolAccess::ReadOnly)
            || (allow_writes
                && (WRITE_CHILD_TOOLS.contains(&name)
                    || matches!(tool.child_access(), zk_tools::ChildToolAccess::WriteGated)))
    }))
}

/// 将 Agent 类型约束与调用方约束取交集。
///
/// `ChildExecutionContext.allowed_tools` 原本只表达 Skill/请求级约束；
/// 在进入工厂前把类型约束折叠进同一字段，便可在不复制全量
/// registry 的前提下真正执行 `AgentDefinition::is_tool_allowed`。
fn apply_agent_tool_policy(
    context: &ChildExecutionContext,
    definition: &AgentDefinition,
    isolation: IsolationMode,
) -> ChildExecutionContext {
    let mut effective = context.clone();
    effective.include_project_prompt &= !definition.omit_project_prompt;
    let uses_live_read_only_baseline = definition.allowed_tools == Some(READ_ONLY_CHILD_TOOLS);
    let verification_bash_allowed = definition.name == "Verification"
        && isolation == IsolationMode::Worktree
        && context.allow_write_tools;
    if uses_live_read_only_baseline {
        // The registry's live policy includes trusted dynamic read-only tools;
        // replacing it with the bootstrap names would freeze the directory.
        effective.allow_write_tools = false;
        effective.write_tool_allowlist = Some(std::collections::BTreeSet::new());
    }
    if verification_bash_allowed {
        // Verification remains file-read-only in every mode. A worktree run may
        // receive Bash only after both composition-root gates have already
        // produced `allow_write_tools=true`.
        effective.allow_write_tools = true;
        effective.write_tool_allowlist =
            Some(std::collections::BTreeSet::from(["Bash".to_owned()]));
    }
    let definition_allowed = if uses_live_read_only_baseline {
        None
    } else if definition.allowed_tools.is_some() || definition.denied_tools.is_some() {
        let declared = definition.allowed_tools.unwrap_or(SUB_AGENT_TOOL_NAMES);
        let candidates = if declared.contains(&"*") {
            SUB_AGENT_TOOL_NAMES
        } else {
            declared
        };
        Some(
            candidates
                .iter()
                .copied()
                .filter(|name| definition.is_tool_allowed(name) && !is_globally_denied(name))
                .map(str::to_owned)
                .collect::<std::collections::BTreeSet<_>>(),
        )
    } else {
        None
    };

    effective.allowed_tools = if uses_live_read_only_baseline {
        // The caller's catalog is already an upper bound, while `sub_tools` is
        // the authoritative live child-access view. Preserve that catalog here
        // so late `ChildToolAccess::ReadOnly` tools are not accidentally reduced
        // to the bootstrap names; `narrow_child_registry` still removes every
        // write-gated tool unless the isolation/type gates admit it.
        context.allowed_tools.clone()
    } else {
        match (&context.allowed_tools, definition_allowed) {
            (Some(request_allowed), Some(type_allowed)) => Some(
                request_allowed
                    .intersection(&type_allowed)
                    .cloned()
                    .collect(),
            ),
            (None, Some(type_allowed)) => Some(type_allowed),
            (Some(request_allowed), None) => Some(
                request_allowed
                    .iter()
                    .filter(|name| definition.is_tool_allowed(name) && !is_globally_denied(name))
                    .cloned()
                    .collect(),
            ),
            (None, None) => None,
        }
    };
    effective
}

fn narrow_child_registry(
    source: &Arc<zk_tools::ToolRegistry>,
    allowed: Option<&std::collections::BTreeSet<String>>,
    allow_write_tools: bool,
    write_tool_allowlist: Option<&std::collections::BTreeSet<String>>,
) -> Arc<zk_tools::ToolRegistry> {
    let allowed = allowed.cloned().map(Arc::new);
    let write_tool_allowlist = write_tool_allowlist.cloned().map(Arc::new);
    Arc::new(source.filtered_by(move |name, tool| {
        let write_capable = WRITE_CHILD_TOOLS.contains(&name)
            || matches!(tool.child_access(), zk_tools::ChildToolAccess::WriteGated);
        (!write_capable
            || (allow_write_tools
                && write_tool_allowlist
                    .as_ref()
                    .is_none_or(|names| names.contains(name))))
            && allowed.as_ref().is_none_or(|names| names.contains(name))
    }))
}

/// Process-local ownership needed only for fail-closed terminalization.
#[derive(Clone, Debug)]
struct ActiveChildRun {
    run_id: String,
    precreated: bool,
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
    execution_supervisor: Arc<crate::ExecutionSupervisor>,
    compact_summarizer: Arc<dyn crate::context::compact::Summarizer>,
    tool_summarizer: Arc<dyn crate::LightModelSummarizer>,
    /// Session-to-Run ownership used only for fail-closed hard terminalization.
    active_runs: Arc<DashMap<String, ActiveChildRun>>,
    /// Executor-supplied cancellation cause consumed by the normal terminal path.
    abort_reasons: Arc<DashMap<String, &'static str>>,
    /// One-shot handoff from `create_and_run_precreated` into the shared runner.
    precreated_executions: Arc<DashMap<String, (PersistedChildExecution, TaskBudgetLimits)>>,
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
        execution_supervisor: Arc<crate::ExecutionSupervisor>,
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
            execution_supervisor,
            compact_summarizer,
            tool_summarizer,
            active_runs: Arc::new(DashMap::new()),
            abort_reasons: Arc::new(DashMap::new()),
            precreated_executions: Arc::new(DashMap::new()),
        }
    }
}

impl SubAgentEngineFactory for RealSubAgentEngineFactory {
    fn available_tool_names(&self, context: &ChildExecutionContext) -> Vec<String> {
        narrow_child_registry(
            &self.sub_tools,
            context.allowed_tools.as_ref(),
            context.allow_write_tools,
            context.write_tool_allowlist.as_ref(),
        )
        .names()
    }

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
        let tools = narrow_child_registry(
            &self.sub_tools,
            context.allowed_tools.as_ref(),
            context.allow_write_tools,
            context.write_tool_allowlist.as_ref(),
        );
        let sink = Arc::clone(&self.sink);
        let admission = Arc::clone(&self.admission);
        let cost_tracker = Arc::clone(&self.cost_tracker);
        let file_history = Arc::clone(&self.file_history);
        let hooks = Arc::clone(&self.hooks);
        let observability = Arc::clone(&self.observability);
        let execution_supervisor = Arc::clone(&self.execution_supervisor);
        let compact_summarizer = Arc::clone(&self.compact_summarizer);
        let tool_summarizer = Arc::clone(&self.tool_summarizer);
        let active_runs = Arc::clone(&self.active_runs);
        let abort_reasons = Arc::clone(&self.abort_reasons);
        let precreated = self
            .precreated_executions
            .remove(session_id)
            .map(|(_, execution)| execution);
        let parent_run_id = context.parent_run_id.clone();
        let parent_session_id = context.parent_session_id.clone();
        let include_project_prompt = context.include_project_prompt;
        let authorized_working_dir = context.working_directory.to_string_lossy().into_owned();
        let agent_id = agent_id.to_owned();
        let session_id = session_id.to_owned();
        let run_id = precreated.as_ref().map_or_else(
            || uuid::Uuid::new_v4().to_string(),
            |(execution, _)| execution.run_id.clone(),
        );
        let model = model.to_owned();
        let system_prompt = system_prompt.to_owned();
        let user_prompt = user_prompt.to_owned();
        let work_dir = work_dir.to_owned();
        active_runs.insert(
            session_id.clone(),
            ActiveChildRun {
                run_id: run_id.clone(),
                precreated: precreated.is_some(),
            },
        );
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
            sink.push_from(
                &parent_session_id,
                &session_id,
                zk_protocol::ServerMessage::AgentStarted {
                    agent_id: agent_id.clone(),
                    prompt: user_prompt.clone(),
                },
            )
            .await;
            if let Some((execution, _)) = precreated.as_ref() {
                let existing = db.find_run_by_id(&run_id).await;
                let valid = matches!(
                    existing,
                    Ok(Some(ref run))
                        if run.session_id == session_id && run.status == "running"
                );
                if !valid {
                    active_runs.remove(&session_id);
                    abort_reasons.remove(&session_id);
                    tracing::error!(
                        task_id = %execution.task_id,
                        %run_id,
                        %session_id,
                        "pre-created child execution is missing, mismatched, or not running"
                    );
                    return (
                        Some("error".to_owned()),
                        Some("AGENT_PRECREATED_EXECUTION_NOT_RUNNING".to_owned()),
                        true,
                    );
                }
            } else if let Err(error) = db
                .create_session_with_id(&session_id, &model, &work_dir)
                .await
            {
                active_runs.remove(&session_id);
                abort_reasons.remove(&session_id);
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
            if precreated.is_none()
                && let Err(error) = db
                    .start_run(
                        &run_id,
                        &session_id,
                        Some(&parent_run_id),
                        Some(zk_db::run::AGENT_TYPE_SUBAGENT),
                        &model,
                    )
                    .await
            {
                active_runs.remove(&session_id);
                abort_reasons.remove(&session_id);
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
            let recovery_checkpoint = precreated
                .as_ref()
                .and_then(|(execution, _)| execution.recovery_checkpoint.clone());
            let system_prompt = resolve_child_system_prompt(
                &db,
                system_prompt,
                include_project_prompt,
                &authorized_working_dir,
                recovery_checkpoint.as_ref(),
            )
            .await;
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
            .with_execution_supervisor(&execution_supervisor)
            .with_summarizers(compact_summarizer, tool_summarizer)
            .with_observability(Arc::clone(&observability));
            let config = crate::engine::SubAgentRunConfig {
                agent_id: agent_id.clone(),
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                model,
                system_prompt,
                user_prompt,
                work_dir,
                max_turns,
                mailbox,
                budget: precreated
                    .as_ref()
                    .map_or_else(TaskBudgetLimits::default, |(_, budget)| budget.clone()),
                recovery_checkpoint,
            };
            let outcome = engine.run_sub_agent(config, cancel).await;
            let requested_abort = abort_reasons.remove(&session_id).map(|(_, reason)| reason);
            if outcome.has_error || outcome.stop_reason.as_deref() == Some("cancelled") {
                let (reason, detail) = match requested_abort {
                    Some("timeout") => (zk_db::run::EXIT_TIMEOUT, Some("timeout")),
                    Some("cancelled") => (zk_db::run::EXIT_USER_CANCELLED, Some("cancelled")),
                    _ if outcome.stop_reason.as_deref() == Some("TASK_DEADLINE_EXCEEDED") => {
                        (zk_db::run::EXIT_TIMEOUT, outcome.stop_reason.as_deref())
                    }
                    _ if matches!(
                        outcome.stop_reason.as_deref(),
                        Some(
                            "BUDGET_EXHAUSTED" | "TOKEN_BUDGET_EXHAUSTED" | "COST_BUDGET_EXHAUSTED"
                        )
                    ) =>
                    {
                        (
                            zk_db::run::EXIT_BUDGET_EXHAUSTED,
                            outcome.stop_reason.as_deref(),
                        )
                    }
                    _ if outcome.stop_reason.as_deref() == Some("cancelled") => (
                        zk_db::run::EXIT_USER_CANCELLED,
                        outcome.stop_reason.as_deref(),
                    ),
                    _ => (
                        zk_db::run::EXIT_INTERNAL_ERROR,
                        outcome.stop_reason.as_deref(),
                    ),
                };
                if precreated.is_none() {
                    let _ = db.terminate_run(&run_id, reason, detail).await;
                }
                sink.push_from(
                    &parent_session_id,
                    &session_id,
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
                if precreated.is_none() {
                    let _ = db.complete_run(&run_id, 0, 0.0, 0).await;
                }
                sink.push_from(
                    &parent_session_id,
                    &session_id,
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
            active_runs.remove(&session_id);
            (
                outcome.stop_reason,
                outcome.assistant_text,
                outcome.has_error,
            )
        })
    }

    fn create_and_run_precreated(
        &self,
        execution: &PersistedChildExecution,
        budget: TaskBudgetLimits,
        agent_id: &str,
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
        self.precreated_executions
            .insert(execution.session_id.clone(), (execution.clone(), budget));
        self.create_and_run(
            agent_id,
            &execution.session_id,
            context,
            model,
            system_prompt,
            user_prompt,
            work_dir,
            mailbox,
            cancel,
            max_turns,
        )
    }

    fn note_abort(&self, session_id: &str, reason: &'static str) {
        self.abort_reasons.insert(session_id.to_owned(), reason);
    }

    fn force_terminalize<'a>(
        &'a self,
        agent_id: &'a str,
        session_id: &'a str,
        parent_session_id: &'a str,
        reason: &'static str,
    ) -> futures::future::BoxFuture<'a, ()> {
        let active = self
            .active_runs
            .remove(session_id)
            .map(|(_, active)| active);
        self.abort_reasons.remove(session_id);
        let db = self.db.clone();
        let sink = Arc::clone(&self.sink);
        let observability = Arc::clone(&self.observability);
        let agent_id = agent_id.to_owned();
        let session_id = session_id.to_owned();
        let parent_session_id = parent_session_id.to_owned();
        Box::pin(async move {
            let Some(active) = active else {
                tracing::error!(%session_id, %agent_id, %reason, "missing active child Run during forced terminalization");
                return;
            };
            let run_id = active.run_id;
            let checkpoint_seq = match db.latest_agent_checkpoint(&run_id).await {
                Ok(Some(latest)) => latest.seq.saturating_add(1),
                Ok(None) => 0,
                Err(error) => {
                    tracing::error!(%run_id, %error, "failed to resolve forced checkpoint sequence");
                    0
                }
            };
            let mut checkpoint = zk_db::new_agent_checkpoint(
                &run_id,
                &session_id,
                &agent_id,
                checkpoint_seq,
                serde_json::json!({
                    "stopReason": reason,
                    "hasError": true,
                    "forced": true,
                }),
            );
            checkpoint.working_dir = None;
            if let Err(error) = db.save_agent_checkpoint(&checkpoint).await {
                tracing::error!(%run_id, %error, "failed to persist forced terminal checkpoint");
            }
            if active.precreated {
                if let Err(error) = db
                    .append_run_event(
                        &run_id,
                        "agent_force_stopped",
                        None,
                        &serde_json::json!({ "reason": reason, "forced": true }),
                    )
                    .await
                {
                    tracing::error!(%run_id, %error, "failed to persist forced stop event");
                }
            } else {
                let exit_reason = if reason == "cancelled" {
                    zk_db::run::EXIT_USER_CANCELLED
                } else {
                    zk_db::run::EXIT_INTERNAL_ERROR
                };
                if let Err(error) = db.terminate_run(&run_id, exit_reason, Some(reason)).await {
                    tracing::error!(%run_id, %error, "failed to force child Run terminal");
                }
            }
            sink.push_from(
                &parent_session_id,
                &session_id,
                zk_protocol::ServerMessage::AgentFailed {
                    agent_id: agent_id.clone(),
                    error: format!("sub-agent {reason}"),
                },
            )
            .await;
            let mut event = crate::ObservabilityEvent::new("agent", "complete", reason);
            event.session_id = Some(session_id);
            event.run_id = Some(run_id);
            event
                .attributes
                .insert("agentId".to_owned(), serde_json::Value::String(agent_id));
            event
                .attributes
                .insert("forced".to_owned(), serde_json::Value::Bool(true));
            observability.record(event);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_runtime_codes_are_preserved_without_collapsing_budget_categories() {
        for (reason, expected) in [
            ("BUDGET_EXHAUSTED", "BUDGET_EXHAUSTED"),
            ("TOKEN_BUDGET_EXHAUSTED", "TOKEN_BUDGET_EXHAUSTED"),
            ("COST_BUDGET_EXHAUSTED", "COST_BUDGET_EXHAUSTED"),
            ("BUDGET_USAGE_INCOMPLETE", "BUDGET_USAGE_INCOMPLETE"),
            ("BUDGET_PRICE_UNKNOWN", "BUDGET_PRICE_UNKNOWN"),
            ("LLM_BUDGET_NOT_CONFIGURED", "LLM_BUDGET_NOT_CONFIGURED"),
            ("TASK_DEADLINE_EXCEEDED", "TIMEOUT"),
        ] {
            assert_eq!(stable_child_error_code(Some(reason)), Some(expected));
        }
        assert_eq!(stable_child_error_code(Some("unrelated error")), None);
    }

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
        for allowed in [
            "Read",
            "Write",
            "Edit",
            "Bash",
            "Glob",
            "Grep",
            "ListDir",
            "WebSearch",
            "WebFetch",
        ] {
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
        src.register(Arc::new(StubTool("WebSearch")));
        src.register(Arc::new(StubTool("UnknownCatalogTool")));
        src.register(Arc::new(StubTool("Agent"))); // denied → must be filtered out
        src.register(Arc::new(StubTool("Memory"))); // denied → must be filtered out

        let sub = build_sub_agent_registry(&src);
        assert!(sub.get("Read").is_some());
        assert!(sub.get("WebSearch").is_some());
        assert!(sub.get("Bash").is_none());
        assert!(sub.get("Agent").is_none());
        assert!(sub.get("Memory").is_none());
        assert!(sub.get("UnknownCatalogTool").is_none());
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
        let writable = build_sub_agent_registry_with_policy(&src, true);
        for name in ["Write", "Edit", "Bash"] {
            assert!(
                writable.get(name).is_some(),
                "explicitly enabled child write tool missing: {name}"
            );
        }
    }

    #[test]
    fn child_registry_tracks_late_dynamic_tools_with_explicit_access_policy() {
        use futures::future::BoxFuture;
        use zk_tools::{ChildToolAccess, Tool, ToolContext, ToolOutput};

        struct DynamicStub {
            name: &'static str,
            access: ChildToolAccess,
        }
        impl Tool for DynamicStub {
            fn name(&self) -> &str {
                self.name
            }
            fn description(&self) -> &'static str {
                "dynamic stub"
            }
            fn parameters(&self) -> serde_json::Value {
                serde_json::json!({"type": "object"})
            }
            fn child_access(&self) -> ChildToolAccess {
                self.access
            }
            fn execute(
                &self,
                _input: serde_json::Value,
                _ctx: ToolContext,
            ) -> BoxFuture<'_, ToolOutput> {
                Box::pin(futures::future::ready(ToolOutput::ok("stub")))
            }
        }

        let source = zk_tools::ToolRegistry::new();
        let read_only = build_sub_agent_registry_with_policy(&source, false);
        let writable = build_sub_agent_registry_with_policy(&source, true);

        source.register_dynamic(Arc::new(DynamicStub {
            name: "CodeIntelLate",
            access: ChildToolAccess::ReadOnly,
        }));
        source.register_dynamic(Arc::new(DynamicStub {
            name: "BrowserLate",
            access: ChildToolAccess::WriteGated,
        }));
        source.register_dynamic(Arc::new(DynamicStub {
            name: "mcp__spoofed__dangerous",
            access: ChildToolAccess::Denied,
        }));

        assert!(read_only.get("CodeIntelLate").is_some());
        assert!(read_only.get("BrowserLate").is_none());
        assert!(writable.get("CodeIntelLate").is_some());
        assert!(writable.get("BrowserLate").is_some());
        assert!(read_only.get("mcp__spoofed__dangerous").is_none());
        assert!(writable.get("mcp__spoofed__dangerous").is_none());

        // Production AgentTool always supplies `Some(parent catalog)`. The
        // Explore policy must retain a trusted late read-only tool from that
        // catalog while the final child view still strips write-gated/denied
        // entries.
        let mut context = child_context();
        context.allowed_tools = Some(std::collections::BTreeSet::from([
            "CodeIntelLate".to_owned(),
            "BrowserLate".to_owned(),
            "mcp__spoofed__dangerous".to_owned(),
        ]));
        context.allow_write_tools = true;
        let effective =
            apply_agent_tool_policy(&context, &AgentDefinition::EXPLORE, IsolationMode::Worktree);
        let production_like = narrow_child_registry(
            &writable,
            effective.allowed_tools.as_ref(),
            effective.allow_write_tools,
            effective.write_tool_allowlist.as_ref(),
        );
        assert_eq!(production_like.names(), ["CodeIntelLate"]);
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
        let narrowed = narrow_child_registry(&source, Some(&allowed), false, None);
        assert_eq!(narrowed.names(), ["Read"]);
        assert!(narrowed.get("Write").is_none());
        assert!(narrowed.get("NotRegistered").is_none());
    }

    #[test]
    fn live_read_only_type_policy_preserves_the_caller_catalog() {
        let mut context = child_context();
        context.allowed_tools = Some(std::collections::BTreeSet::from([
            "Read".to_owned(),
            "Write".to_owned(),
            "WebSearch".to_owned(),
        ]));
        let effective =
            apply_agent_tool_policy(&context, &AgentDefinition::PLAN, IsolationMode::None);
        assert_eq!(
            effective.allowed_tools,
            Some(std::collections::BTreeSet::from([
                "Read".to_owned(),
                "Write".to_owned(),
                "WebSearch".to_owned(),
            ]))
        );
        assert!(!effective.allow_write_tools);
        assert_eq!(
            effective.write_tool_allowlist,
            Some(std::collections::BTreeSet::new())
        );
    }

    #[test]
    fn verification_tool_policy_matrix_is_fail_closed() {
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
        for name in ["Read", "Write", "Edit", "NotebookEdit", "Bash"] {
            source.register(Arc::new(StubTool(name)));
        }
        let source = Arc::new(source);
        let names_for = |isolation, gates_open| {
            let mut context = child_context();
            context.allowed_tools = Some(
                ["Read", "Write", "Edit", "NotebookEdit", "Bash"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
            );
            context.allow_write_tools = gates_open;
            let effective =
                apply_agent_tool_policy(&context, &AgentDefinition::VERIFICATION, isolation);
            narrow_child_registry(
                &source,
                effective.allowed_tools.as_ref(),
                effective.allow_write_tools,
                effective.write_tool_allowlist.as_ref(),
            )
            .names()
        };

        for (label, names) in [
            ("readOnly", names_for(IsolationMode::None, false)),
            ("sharedWorkspace", names_for(IsolationMode::None, true)),
            (
                "worktree-without-double-gate",
                names_for(IsolationMode::Worktree, false),
            ),
        ] {
            assert!(names.contains(&"Read".to_owned()), "case={label}");
            for forbidden in ["Bash", "Write", "Edit", "NotebookEdit"] {
                assert!(
                    !names.contains(&forbidden.to_owned()),
                    "{forbidden} leaked in case={label}: {names:?}"
                );
            }
        }

        let isolated = names_for(IsolationMode::Worktree, true);
        assert!(isolated.contains(&"Read".to_owned()));
        assert!(isolated.contains(&"Bash".to_owned()));
        for forbidden in ["Write", "Edit", "NotebookEdit"] {
            assert!(
                !isolated.contains(&forbidden.to_owned()),
                "{forbidden} leaked into isolated Verification: {isolated:?}"
            );
        }
    }

    #[test]
    fn agent_type_policy_controls_project_prompt_inclusion() {
        let context = child_context();
        for definition in [&AgentDefinition::EXPLORE, &AgentDefinition::PLAN] {
            let effective = apply_agent_tool_policy(&context, definition, IsolationMode::None);
            assert!(
                !effective.include_project_prompt,
                "agent={}",
                definition.name
            );
        }
        for definition in [
            &AgentDefinition::GENERAL_PURPOSE,
            &AgentDefinition::VERIFICATION,
            &AgentDefinition::GUIDE,
        ] {
            let effective = apply_agent_tool_policy(&context, definition, IsolationMode::None);
            assert!(
                effective.include_project_prompt,
                "agent={}",
                definition.name
            );
        }

        let mut caller_omits = context;
        caller_omits.include_project_prompt = false;
        let effective = apply_agent_tool_policy(
            &caller_omits,
            &AgentDefinition::GENERAL_PURPOSE,
            IsolationMode::None,
        );
        assert!(!effective.include_project_prompt);
    }

    #[test]
    fn prompts_follow_the_actual_tool_directory() {
        let explore_without_code_intel = build_agent_system_prompt(
            &AgentDefinition::EXPLORE,
            "map the code",
            "/work",
            &["Glob".to_owned(), "Grep".to_owned(), "Read".to_owned()],
        );
        assert!(!explore_without_code_intel.contains("CodeIntel"));
        for available in ["Glob", "Grep", "Read"] {
            assert!(
                explore_without_code_intel.contains(&format!("{available} 已提供")),
                "missing strategy for {available}"
            );
        }
        assert!(explore_without_code_intel.contains("Glob → Grep → Read"));
        for invented in ["search_codebase", "search_symbol"] {
            assert!(!explore_without_code_intel.contains(invented));
        }
        let explore_with_code_intel = build_agent_system_prompt(
            &AgentDefinition::EXPLORE,
            "map the code",
            "/work",
            &["Read".to_owned(), "CodeIntel".to_owned()],
        );
        assert!(!explore_with_code_intel.contains("Glob"));
        assert!(!explore_with_code_intel.contains("Grep"));
        assert!(explore_with_code_intel.contains("Read 已提供"));
        assert!(explore_with_code_intel.contains("CodeIntel 已在目录中"));

        let explore_without_primary_search = build_agent_system_prompt(
            &AgentDefinition::EXPLORE,
            "map the code",
            "/work",
            &["WebSearch".to_owned()],
        );
        for unavailable in ["Glob", "Grep", "Read"] {
            assert!(!explore_without_primary_search.contains(unavailable));
        }
        assert!(explore_without_primary_search.contains("没有提供基础代码搜索工具"));

        let static_verification = build_agent_system_prompt(
            &AgentDefinition::VERIFICATION,
            "verify",
            "/work",
            &["Read".to_owned()],
        );
        assert!(static_verification.contains("静态确认缺陷时最终为 VERDICT: FAIL"));
        assert!(static_verification.contains("若未确认缺陷"));
        assert!(static_verification.contains("最终为 VERDICT: PARTIAL"));
        assert!(!static_verification.contains("最终必须是 VERDICT: PARTIAL"));
        assert!(static_verification.contains("METHOD"));
        assert!(static_verification.contains("EVIDENCE"));

        let executable_verification = build_agent_system_prompt(
            &AgentDefinition::VERIFICATION,
            "verify",
            "/worktree",
            &["Read".to_owned(), "Bash".to_owned()],
        );
        assert!(!executable_verification.contains("本次只能静态检查"));
        assert!(executable_verification.contains("Bash 已提供"));
    }

    #[tokio::test]
    async fn child_project_prompt_uses_db_then_file_and_checkpoint_is_frozen() {
        use sha2::Digest as _;

        let db = zk_db::Db::open_in_memory().expect("db");
        let workspace =
            std::env::temp_dir().join(format!("zk-child-project-prompt-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&workspace).expect("workspace");
        std::fs::write(workspace.join("PROJECT.md"), "FILE_PROJECT_RULE").expect("project prompt");
        let workspace_text = workspace.to_string_lossy().into_owned();

        let from_file =
            resolve_child_system_prompt(&db, "base".to_owned(), true, &workspace_text, None).await;
        assert!(from_file.contains("FILE_PROJECT_RULE"));

        let hash = format!("{:x}", sha2::Sha256::digest(workspace_text.as_bytes()));
        db.save_project_context(&zk_db::ProjectContextRecord {
            id: uuid::Uuid::new_v4().to_string(),
            working_dir_hash: hash,
            snapshot: serde_json::Value::String("DB_PROJECT_RULE".to_owned()),
            git_head_sha: None,
            updated_at: zk_db::time::format_rfc3339_micros(zk_db::time::now_millis()),
        })
        .await
        .expect("durable context");
        let from_db =
            resolve_child_system_prompt(&db, "base".to_owned(), true, &workspace_text, None).await;
        assert!(from_db.contains("DB_PROJECT_RULE"));
        assert!(!from_db.contains("FILE_PROJECT_RULE"));

        let omitted =
            resolve_child_system_prompt(&db, "base".to_owned(), false, &workspace_text, None).await;
        assert_eq!(omitted, "base");

        let checkpoint = serde_json::json!({"systemPrompt": "FROZEN_SYSTEM_PROMPT"});
        let restored = resolve_child_system_prompt(
            &db,
            "freshly generated".to_owned(),
            true,
            &workspace_text,
            Some(&checkpoint),
        )
        .await;
        assert_eq!(restored, "FROZEN_SYSTEM_PROMPT");

        std::fs::remove_dir_all(workspace).ok();
    }

    struct StubEngineFactory;

    fn child_context() -> ChildExecutionContext {
        ChildExecutionContext {
            parent_session_id: "parent-session".to_owned(),
            parent_run_id: "parent-run".to_owned(),
            working_directory: PathBuf::from("/tmp"),
            tool_use_id: "tool-use-1".to_owned(),
            allowed_tools: None,
            allow_write_tools: false,
            write_tool_allowlist: None,
            include_project_prompt: true,
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
            AgentTimeoutConfig {
                graceful_shutdown_seconds: 0,
                ..AgentTimeoutConfig::default()
            },
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

    struct RuntimeFailureFactory(&'static str);

    impl SubAgentEngineFactory for RuntimeFailureFactory {
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
            let code = self.0.to_owned();
            Box::pin(async move { (Some(code.clone()), Some(code), true) })
        }
    }

    #[tokio::test]
    async fn stable_child_runtime_failures_keep_agent_result_classification_and_code() {
        for (code, expected_status, expected_result_code) in [
            (
                "COST_BUDGET_EXHAUSTED",
                AgentStatus::BudgetExhausted,
                "COST_BUDGET_EXHAUSTED",
            ),
            (
                "BUDGET_USAGE_INCOMPLETE",
                AgentStatus::Failed,
                "BUDGET_USAGE_INCOMPLETE",
            ),
            ("TASK_DEADLINE_EXCEEDED", AgentStatus::Timeout, "TIMEOUT"),
        ] {
            let executor = SubAgentExecutor::new(
                Arc::new(AgentConcurrencyController::default()),
                Arc::new(RuntimeFailureFactory(code)),
                WorktreeManager::for_repo(
                    std::env::current_dir().expect("current dir"),
                    Arc::new(crate::agent::worktree::SystemGitCommandRunner),
                )
                .expect("worktree manager"),
                AgentTimeoutConfig::default(),
            );
            let request = AgentRequest::new(
                uuid::Uuid::new_v4().to_string(),
                "runtime failure",
                None,
                Some("test-model".to_owned()),
                IsolationMode::None,
                false,
            );

            let result = executor.execute_sync(&request, &child_context()).await;

            assert_eq!(result.status, expected_status, "code={code}");
            assert_eq!(
                result.error_code.as_deref(),
                Some(expected_result_code),
                "code={code}"
            );
        }
    }

    struct FailingWorktreeRunner;

    impl crate::agent::worktree::GitCommandRunner for FailingWorktreeRunner {
        fn run<'a>(
            &'a self,
            _cwd: &'a std::path::Path,
            _args: Vec<String>,
        ) -> futures::future::BoxFuture<'a, Result<crate::agent::worktree::GitCommandOutput, String>>
        {
            Box::pin(futures::future::ready(Ok(
                crate::agent::worktree::GitCommandOutput {
                    status: 1,
                    stdout: String::new(),
                    stderr: "worktree rejected".to_owned(),
                },
            )))
        }
    }

    #[tokio::test]
    async fn worktree_creation_failure_never_falls_back_to_parent_directory() {
        let executor = SubAgentExecutor::new(
            Arc::new(AgentConcurrencyController::default()),
            Arc::new(StubEngineFactory),
            WorktreeManager::for_repo(
                std::env::current_dir().expect("current dir"),
                Arc::new(FailingWorktreeRunner),
            )
            .expect("worktree manager"),
            AgentTimeoutConfig::default(),
        );
        let request = AgentRequest::new(
            uuid::Uuid::new_v4().to_string(),
            "must be isolated",
            None,
            Some("test-model".to_owned()),
            IsolationMode::Worktree,
            false,
        );
        let result = executor.execute_sync(&request, &child_context()).await;
        assert_eq!(result.status, AgentStatus::Failed);
        assert!(
            result
                .result
                .as_deref()
                .is_some_and(|message| message.starts_with("AGENT_WORKTREE_CREATE_FAILED:"))
        );
    }

    struct PrecreatedFactory;

    impl SubAgentEngineFactory for PrecreatedFactory {
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
            Box::pin(async {
                (
                    Some("end_turn".to_owned()),
                    Some("legacy path".to_owned()),
                    false,
                )
            })
        }

        fn create_and_run_precreated(
            &self,
            execution: &PersistedChildExecution,
            _budget: TaskBudgetLimits,
            _agent_id: &str,
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
            let result = format!("precreated {}", execution.run_id);
            Box::pin(async move { (Some("end_turn".to_owned()), Some(result), false) })
        }
    }

    #[tokio::test]
    async fn precreated_execution_uses_durable_identity_path() {
        let executor = SubAgentExecutor::new(
            Arc::new(AgentConcurrencyController::default()),
            Arc::new(PrecreatedFactory),
            WorktreeManager::for_repo(
                std::env::current_dir().expect("current dir"),
                Arc::new(crate::agent::worktree::SystemGitCommandRunner),
            )
            .expect("worktree manager"),
            AgentTimeoutConfig::default(),
        );
        let request = AgentRequest::new(
            uuid::Uuid::new_v4().to_string(),
            "test prompt",
            None,
            Some("test-model".to_owned()),
            IsolationMode::None,
            false,
        );
        let identity = PersistedChildExecution::try_new(
            uuid::Uuid::new_v4().to_string(),
            uuid::Uuid::new_v4().to_string(),
            uuid::Uuid::new_v4().to_string(),
        )
        .expect("valid identity");
        let result = executor
            .execute_precreated_with_cancel(
                &request,
                &child_context(),
                &identity,
                TaskBudgetLimits::default(),
                CancellationToken::new(),
            )
            .await;
        assert_eq!(result.status, AgentStatus::Completed);
        assert_eq!(
            result.result.as_deref(),
            Some(format!("precreated {}", identity.run_id).as_str())
        );
    }

    #[test]
    fn precreated_execution_rejects_short_or_non_v4_ids() {
        let error = PersistedChildExecution::try_new(
            "short-task",
            uuid::Uuid::new_v4().to_string(),
            uuid::Uuid::new_v4().to_string(),
        )
        .unwrap_err();
        assert_eq!(error, "AGENT_PRECREATED_taskId_INVALID");
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
            AgentTimeoutConfig {
                graceful_shutdown_seconds: 0,
                ..AgentTimeoutConfig::default()
            },
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
        tokio::time::timeout(Duration::from_secs(1), async {
            while router.has_active_agent("aborted-agent") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached lifecycle observes cancellation and unregisters mailbox");
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
        use std::sync::atomic::{AtomicBool, Ordering};

        struct DropMarker(Arc<AtomicBool>);
        impl Drop for DropMarker {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        struct SlowFactory {
            forced: Arc<AtomicBool>,
            future_dropped: Arc<AtomicBool>,
        }
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
                let marker = DropMarker(Arc::clone(&self.future_dropped));
                Box::pin(async move {
                    let _marker = marker;
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    (Some("end_turn".to_owned()), Some("late".to_owned()), false)
                })
            }

            fn force_terminalize<'a>(
                &'a self,
                _agent_id: &'a str,
                _session_id: &'a str,
                _parent_session_id: &'a str,
                _reason: &'static str,
            ) -> futures::future::BoxFuture<'a, ()> {
                let forced = Arc::clone(&self.forced);
                let future_dropped = Arc::clone(&self.future_dropped);
                Box::pin(async move {
                    assert!(
                        !future_dropped.load(Ordering::SeqCst),
                        "terminal fallback must run before dropping the child future"
                    );
                    forced.store(true, Ordering::SeqCst);
                })
            }
        }
        let concurrency = Arc::new(AgentConcurrencyController::default());
        let forced = Arc::new(AtomicBool::new(false));
        let future_dropped = Arc::new(AtomicBool::new(false));
        let config = AgentTimeoutConfig {
            default_seconds: 1, // 1 second for test
            max_seconds: 1,
            graceful_shutdown_seconds: 0,
        };
        let executor = SubAgentExecutor::new(
            concurrency,
            Arc::new(SlowFactory {
                forced: Arc::clone(&forced),
                future_dropped: Arc::clone(&future_dropped),
            }),
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
        assert!(forced.load(Ordering::SeqCst));
        assert!(future_dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn timeout_config_defaults() {
        let config = AgentTimeoutConfig::default();
        assert_eq!(config.default_seconds, 1800);
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
        let prompt = build_agent_system_prompt(
            &AgentDefinition::GENERAL_PURPOSE,
            "do stuff",
            "/work",
            &["Read".to_owned()],
        );
        assert!(prompt.contains("通用 worker"));
        assert!(prompt.contains("do stuff"));
        assert!(prompt.contains("/work"));
        assert!(prompt.contains("当前工具目录（唯一权威）：Read"));
        assert!(prompt.contains("不得声称已修改文件"));
        assert!(prompt.contains("不得声称已运行构建、测试"));
    }
}
