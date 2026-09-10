//! S9 引擎接线——`EngineHook` 适配层 + `WsHub` 的 `MessageSink` 实现。
//!
//! 依赖方向：zk-server → zk-engine（组装根侧，合法）；引擎经
//! `MessageSink` 窄接口回推下行，不反向依赖本 crate。上行链路：
//! `inbound::dispatch` → `WsHub::dispatch_to_engine` → [`EngineBridge`]
//! → `Engine::handle_client_message`（每 run 一 spawn，同步入口不阻塞
//! WS 读循环）。

use std::sync::{Arc, OnceLock};

use futures::future::BoxFuture;
use zk_engine::admission::ToolAdmission;
use zk_engine::agent::{
    AgentDefinition, AgentMailboxMessage, AgentMailboxRouter, AgentRequest, AgentStatus,
    AgentTimeoutConfig, ChildExecutionContext, IsolationMode, PersistedChildExecution,
    READ_ONLY_CHILD_TOOLS, RealSubAgentEngineFactory, SubAgentExecutor, SystemGitCommandRunner,
    WorktreeManager, build_sub_agent_registry_with_policy,
};
use zk_engine::concurrency::AgentConcurrencyController;
use zk_engine::task::{
    ChildTaskSubmission, TaskExecutionResult, TaskOutputRequest as RuntimeOutputRequest,
    TaskRuntime, TaskRuntimeError,
};
use zk_engine::{ConversationService, CoordinatorEvent, Engine, LlmSummarizer, MessageSink};
use zk_llm::ChatProvider;
use zk_protocol::{ClientMessage, ServerMessage};
use zk_tools::{
    AgentInvocation, AgentTool, AgentToolBackend, AskUserQuestionTool, BashTool, ConfigTool,
    CronCreateTool, CronDeleteTool, CronListTool, CronTaskPort, CtxInspectTool, EditFileTool,
    EnterPlanModeTool, ExitPlanModeTool, GitDiffTool, GitLogTool, GitStatusTool, GlobTool,
    GrepTool, ListDirectoryTool, MemoryTool, ModelCatalog, MonitorTool, NotebookEditTool, REPLTool,
    ReadFileTool, ReplManager, SendMessageBackend, SendMessageInvocation, SendMessageReceipt,
    SendMessageTool, SleepTool, SnipTool, StaticToolCatalog, SyntheticOutputTool,
    TaskCoordinatorPort, TaskCreateTool, TaskGetTool, TaskInvocation, TaskListTool, TaskOutputPage,
    TaskOutputQuery, TaskOutputTool, TaskPortError, TaskSnapshot, TaskStopReceipt, TaskStopTool,
    TaskUpdateTool, TerminalCaptureTool, TodoWriteTool, ToolDescriptor, ToolRegistry,
    ToolSearchTool, VerifyPlanExecutionTool, VisualizationTool, WebFetchTool, WebSearchTool,
    WorktreeTool, WriteFileTool,
};

use crate::api::browser_replay::BrowserReplayStore;
use crate::authz::EngineAdmission;
use crate::http_fetch::SafeHttpFetchPort;
use crate::http_search::SearxngSearchBackend;
use crate::interaction::DurableElicitationSink;
use crate::mcp_search::McpSearchBackend;
use crate::mcp_tools::{ListMcpResourcesTool, ReadMcpResourceTool};
use crate::memory_store::DbMemoryStore;
use crate::python::{
    BrowserVerifyJourneyTool, CodeIntelTool, GitEnhancedTool, PythonClient, WebBrowserTool,
};
use crate::skill::SkillTool;
use crate::snapshot_sink::DbSnapshotSink;
use crate::state::AppState;
use crate::ws::{EngineHook, WsHub};

/// `WsHub` 的下行 sink 适配。
///
/// 引擎逐条 `await`，`hub.push` 完成信封组装 / seq 分配 / critical 暂存；
/// 不经 `tokio::spawn` 转发——保住 deltas 的推送顺序（D-S9-3）。
struct HubSink {
    hub: WsHub,
    db: zk_db::Db,
}

/// Build the process-wide durable task runtime before any transport or tool
/// adapter is assembled.
pub(crate) fn build_task_runtime(
    db: zk_db::Db,
    hub: WsHub,
    observability: Arc<dyn zk_engine::ObservabilityRecorder>,
) -> Arc<TaskRuntime> {
    Arc::new(
        TaskRuntime::new(db.clone(), Arc::new(HubSink { hub, db }))
            .with_observability(observability),
    )
}

impl MessageSink for HubSink {
    fn push<'a>(&'a self, session_id: &'a str, message: ServerMessage) -> BoxFuture<'a, ()> {
        self.push_from(session_id, session_id, message)
    }

    fn push_from<'a>(
        &'a self,
        route_session_id: &'a str,
        source_session_id: &'a str,
        message: ServerMessage,
    ) -> BoxFuture<'a, ()> {
        let hub = self.hub.clone();
        let db = self.db.clone();
        Box::pin(async move {
            hub.push_runtime_event(&db, route_session_id, source_session_id, message)
                .await;
        })
    }
}

/// `EngineHook` → [`Engine`] 桥（上行同步分发）。
struct EngineBridge {
    engine: Arc<Engine>,
}

impl EngineHook for EngineBridge {
    fn on_client_message(&self, session_id: &str, message: ClientMessage) {
        self.engine.handle_client_message(session_id, message);
    }
}

/// 组装引擎并注入 WS Hub（main 启动序列调用）。
///
/// provider 注入 [`AppState::providers`]——即 2.7 的
/// [`zk_llm::ProviderRegistry`]（自身实现 [`ChatProvider`]，承载 model →
/// provider 路由、熔断与模型降级链）。同一热替换代理还以
/// [`zk_llm::VisionProviderView`] 注入引擎，确保图片路由只选择当前已配置模型。
/// 密钥未配置不在此报错——请求期以 `query_error` 下行（行为可观察、进程仍可
/// 服务 REST）。
#[must_use]
pub fn wire_engine(state: &AppState) -> Arc<Engine> {
    let provider: Arc<dyn ChatProvider> = state.providers.clone();
    let vision_providers: Arc<dyn zk_llm::VisionProviderView> = state.providers.clone();
    // Batch 1 Step 1-5：注册表取自 `AppState` 的惰性单例——REST
    // `GET /api/tools` 与引擎/准入端口自此共用同一批工具实例。
    let tools = state.tools();
    // 2.5：工具执行准入端口注入。引擎与准入端口共用同一 `ToolRegistry`，
    // 保证 `ToolFacts`（isDestructive / isReadOnly / getPath）与真正执行的
    // 工具实例同源——旧 `ToolExecutionPipeline` 先按名解析 `Tool` bean，再把
    // 同一实例交给授权链与执行体。
    let admission: Arc<dyn ToolAdmission> =
        Arc::new(EngineAdmission::new(state.authz.clone(), tools.clone()));
    // Batch 0 Step 0-6：会话/全局费用累加器（对照旧 `CostTrackerService`
    // Spring bean 装配）。Batch 3 起 `/cost` 命令是它的读侧消费方，故实例上提
    // 到 `AppState`——引擎写入与命令读取必须同源，否则 `/cost` 恒读到零。
    let cost_tracker = state.costs.clone();
    let lightweight_model = select_lightweight_model(
        &state.providers.load(),
        std::env::var("ZK_LIGHTWEIGHT_MODEL").ok().as_deref(),
    );
    let llm_summarizer = Arc::new(LlmSummarizer::new(Arc::clone(&provider), lightweight_model));
    let compact_summarizer: Arc<dyn zk_engine::context::compact::Summarizer> =
        llm_summarizer.clone();
    let tool_summarizer: Arc<dyn zk_engine::LightModelSummarizer> = llm_summarizer;
    // Phase A7：剪贴板图片 URL 信任校验策略（旧 `OssPublishProperties.
    // isTrustedClipboardImageUrl`）。OSS 未配置时策略恒拒绝——url 附件在
    // 引擎入站即被整条拒绝（fail-closed，SSRF 红线）。
    let image_url_policy = crate::oss_trust::TrustedImageUrlPolicy::from_settings(
        state.config.oss_endpoint.as_deref(),
        state.config.oss_bucket.as_deref(),
        &state.config.oss_prefix,
    );
    let trusted_image_url: zk_engine::TrustedImageUrlCheck =
        Arc::new(move |value: &str| image_url_policy.is_trusted_clipboard_image_url(value));
    let engine = Arc::new(
        Engine::with_admission(
            state.db.clone(),
            provider,
            Arc::new(HubSink {
                hub: state.hub.clone(),
                db: state.db.clone(),
            }),
            tools,
            admission,
        )
        .with_execution_supervisor(&state.execution_supervisor)
        .with_task_runtime(Arc::clone(&state.task_runtime))
        .with_coordinator(Arc::clone(&state.coordinator))
        .with_run_cancellation(state.authz.terminations.clone())
        .with_root_task_budget_policy(state.config.root_task_budget_policy.clone())
        .with_startup_epoch(state.startup_epoch())
        .with_summarizers(compact_summarizer, tool_summarizer)
        .with_cost_tracker(cost_tracker)
        // Batch 5 Step 5：回合事务边界端口。实例上提到 `AppState`，与
        // `/api/sessions/{id}/history/*` 端点同源——否则端点侧读不到引擎
        // 登记的变更集。
        .with_file_history(state.file_history.clone())
        // Batch 8B：Hook 服务端口注入。引擎触发点（工具前后 / run 起止）与
        // `AppState::hooks` 共用同一实例，配置从 `.zk/hooks.toml` 装配期加载。
        .with_hooks(state.hooks.clone())
        .with_observability(Arc::clone(&state.observability))
        .with_trusted_image_url(trusted_image_url)
        .with_vision_provider_view(vision_providers),
    );
    state.hub.set_engine(Arc::new(EngineBridge {
        engine: Arc::clone(&engine),
    }));
    state.set_conversation(Arc::new(ConversationService::new(
        Arc::clone(&engine),
        state.db.clone(),
    )));
    engine
}

fn select_lightweight_model(
    providers: &zk_llm::ProviderRegistry,
    configured: Option<&str>,
) -> String {
    if let Some(configured) = configured.filter(|model| !model.trim().is_empty()) {
        return configured.trim().to_owned();
    }
    providers
        .models()
        .iter()
        .find(|model| model.eq_ignore_ascii_case("light"))
        .cloned()
        .unwrap_or_else(|| providers.default_model().to_owned())
}

/// [`ModelCatalog`] 的生产实现——`Config` 工具的 `model` 键取值面。
///
/// 旧 `ConfigTool` 直接注入 `LlmProviderRegistry` 并调
/// `listAvailableModels()`（`ConfigTool.java:186-196`）；zk-tools 不得依赖
/// zk-llm，故经端口反转，实现落本组合根。
struct RegistryModelCatalog {
    /// 2.7 的 provider 注册表（`model_order` 即旧 `listAvailableModels()`）。
    providers: Arc<zk_llm::SwappableProvider>,
}

impl ModelCatalog for RegistryModelCatalog {
    fn available_models(&self) -> Vec<String> {
        self.providers.load().models().to_vec()
    }
}

// ═══ Batch 6: Agent + Task 桥接 ═══

/// Batch 8H：占位实现已退场——`build_tool_registry` 中直接注入
/// [`RealSubAgentEngineFactory`]（Batch 8C 实现），真实子代理引擎创建已激活。
///
/// `AgentToolBackend` 的生产实现——桥接到 `SubAgentExecutor`。
struct AgentBackendBridge {
    task_port: Arc<TaskCoordinatorBridge>,
}

impl AgentToolBackend for AgentBackendBridge {
    fn execute_agent(
        &self,
        invocation: AgentInvocation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>> {
        Box::pin(async move { self.task_port.submit_agent(invocation, cancel).await })
    }
}

/// Persist collaboration messages in the `TaskRuntime` inbox before best-effort
/// live mailbox delivery.
struct SendMessageBackendBridge {
    router: Arc<AgentMailboxRouter>,
    runtime: Arc<TaskRuntime>,
    db: zk_db::Db,
    event_bus: Arc<zk_engine::CoordinatorEventBus>,
}

impl SendMessageBackend for SendMessageBackendBridge {
    #[allow(clippy::too_many_lines)] // validates sender ownership before the durable inbox transaction
    fn send_message(
        &self,
        invocation: SendMessageInvocation,
    ) -> BoxFuture<'_, Result<SendMessageReceipt, TaskPortError>> {
        Box::pin(async move {
            let sender_run = self
                .db
                .find_run_by_id(&invocation.parent_run_id)
                .await
                .map_err(|error| {
                    port_error(TaskRuntimeError::new(
                        "TASK_STORAGE_ERROR",
                        format!("parent run lookup failed: {error}"),
                        true,
                    ))
                })?
                .ok_or_else(|| {
                    TaskPortError::new(
                        "SEND_MESSAGE_CONTEXT_INVALID",
                        "parent run not found",
                        false,
                    )
                })?;
            if sender_run.session_id != invocation.parent_session_id {
                return Err(TaskPortError::new(
                    "SEND_MESSAGE_CONTEXT_INVALID",
                    "parent run/session mismatch",
                    false,
                ));
            }
            if !matches!(
                sender_run.status.as_str(),
                "running" | "waitingDependencies" | "waitingInteraction"
            ) {
                return Err(TaskPortError::new(
                    "SEND_MESSAGE_CONTEXT_INVALID",
                    "parent run is terminal",
                    false,
                ));
            }
            let sender_task_id = sender_run.task_id;
            let queued = match self
                .runtime
                .send_message(
                    &invocation.parent_session_id,
                    &invocation.target_task_id,
                    Some(&sender_task_id),
                    &invocation.message,
                )
                .await
            {
                Ok(message) => message,
                Err(error) if error.code.starts_with("TASK_TERMINAL") => {
                    let task = self
                        .runtime
                        .get_owned(&invocation.parent_session_id, &invocation.target_task_id)
                        .await
                        .map_err(port_error)?
                        .ok_or_else(|| {
                            TaskPortError::new(
                                "TASK_NOT_FOUND",
                                "target task is not owned by this root session",
                                false,
                            )
                        })?;
                    return Ok(SendMessageReceipt {
                        message_id: None,
                        delivery_status: "terminal".to_owned(),
                        status: task.status.as_db().to_owned(),
                    });
                }
                Err(error) => return Err(port_error(error)),
            };

            let from_id = sender_task_id.clone();
            let delivery = AgentMailboxMessage {
                message_id: queued.message_id.clone(),
                parent_run_id: invocation.parent_run_id.clone(),
                from_id: from_id.clone(),
                content: invocation.message.clone(),
            };
            let delivered = self
                .router
                .send_message(&invocation.target_task_id, delivery)
                .is_ok();
            if delivered {
                let _ = self
                    .runtime
                    .mark_inbox(
                        &invocation.parent_session_id,
                        &invocation.target_task_id,
                        &queued.message_id,
                        zk_db::InboxStatus::Queued,
                        zk_db::InboxStatus::Delivered,
                        None,
                    )
                    .await;
                let content = invocation.message.chars().take(512).collect();
                let _ = self.event_bus.publish(CoordinatorEvent::TeammateMessage {
                    session_id: invocation.parent_session_id.clone(),
                    from_id,
                    content,
                });
            }
            let task = self
                .runtime
                .get_owned(&invocation.parent_session_id, &invocation.target_task_id)
                .await
                .map_err(port_error)?
                .ok_or_else(|| {
                    TaskPortError::new(
                        "TASK_NOT_FOUND",
                        "target task disappeared after inbox commit",
                        false,
                    )
                })?;
            Ok(SendMessageReceipt {
                message_id: Some(queued.message_id),
                delivery_status: if delivered { "delivered" } else { "queued" }.to_owned(),
                status: task.status.as_db().to_owned(),
            })
        })
    }
}

struct ParentExecutionIdentity {
    model: String,
    task_id: String,
}

async fn validate_agent_invocation(
    db: &zk_db::Db,
    invocation: &AgentInvocation,
) -> Result<ParentExecutionIdentity, TaskPortError> {
    let session = db
        .get_session(&invocation.parent_session_id)
        .await
        .map_err(|_| {
            TaskPortError::new(
                "AGENT_CONTEXT_INVALID",
                "parent session lookup failed",
                true,
            )
        })?
        .ok_or_else(|| {
            TaskPortError::new("AGENT_CONTEXT_INVALID", "parent session not found", false)
        })?;
    if session.status != "active" {
        return Err(TaskPortError::new(
            "AGENT_CONTEXT_INVALID",
            "parent session is not active",
            false,
        ));
    }
    let run = db
        .find_run_by_id(&invocation.parent_run_id)
        .await
        .map_err(|_| TaskPortError::new("AGENT_CONTEXT_INVALID", "parent run lookup failed", true))?
        .ok_or_else(|| {
            TaskPortError::new("AGENT_CONTEXT_INVALID", "parent run not found", false)
        })?;
    if run.session_id != invocation.parent_session_id {
        return Err(TaskPortError::new(
            "AGENT_CONTEXT_INVALID",
            "parent run/session mismatch",
            false,
        ));
    }
    if !matches!(
        run.status.as_str(),
        "running" | "waitingDependencies" | "waitingInteraction"
    ) {
        return Err(TaskPortError::new(
            "AGENT_CONTEXT_INVALID",
            "parent run is terminal",
            false,
        ));
    }
    let authorized = std::fs::canonicalize(&session.working_dir).map_err(|_| {
        TaskPortError::new(
            "AGENT_CONTEXT_INVALID",
            "authorized workspace is unavailable",
            false,
        )
    })?;
    let requested = std::fs::canonicalize(&invocation.working_directory).map_err(|_| {
        TaskPortError::new(
            "AGENT_CONTEXT_INVALID",
            "invocation workspace is unavailable",
            false,
        )
    })?;
    if requested != authorized {
        return Err(TaskPortError::new(
            "AGENT_CONTEXT_INVALID",
            "workspace does not match parent session",
            false,
        ));
    }
    Ok(ParentExecutionIdentity {
        model: run.model,
        task_id: run.task_id,
    })
}

fn resolve_agent_model(
    providers: &zk_llm::ProviderRegistry,
    requested: Option<&str>,
    agent_type: Option<&str>,
    inherited: &str,
) -> Result<String, String> {
    let agent_default = AgentDefinition::resolve(agent_type).default_model;
    let requested = requested.map(str::trim).filter(|model| !model.is_empty());
    let resolved = match requested {
        Some("premium" | "default") => providers.default_model(),
        Some("inherit") => inherited,
        Some(model) => model,
        None => agent_default.unwrap_or(inherited),
    };
    if resolved.is_empty()
        || (!providers.models().is_empty()
            && !providers.models().iter().any(|model| model == resolved))
    {
        return Err(format!(
            "AGENT_MODEL_INVALID: unsupported model '{}'",
            requested.unwrap_or(resolved)
        ));
    }
    Ok(resolved.to_owned())
}

/// `TaskCoordinatorPort` 的生产实现——所有 Agent/TaskCreate 操作进入同一个
/// DB-authoritative [`TaskRuntime`]。
struct TaskCoordinatorBridge {
    runtime: Arc<TaskRuntime>,
    terminations: Arc<crate::run_termination::RunTerminationCoordinator>,
    executor: Arc<SubAgentExecutor>,
    db: zk_db::Db,
    providers: Arc<zk_llm::SwappableProvider>,
    worktree_enabled: bool,
    shared_workspace_enabled: bool,
    child_write_enabled: bool,
    startup_epoch: i64,
}

/// Single production Agent runtime shared by Agent tools, Task tools, and Swarm dispatch.
pub(crate) struct AgentRuntime {
    /// Production child-agent executor.
    pub(crate) executor: Arc<SubAgentExecutor>,
    /// DB-backed task coordinator.
    pub(crate) tasks: Arc<TaskRuntime>,
}

impl TaskCoordinatorBridge {
    #[allow(clippy::too_many_lines)] // One lifecycle boundary: validate, persist, dispatch, then wait/cancel.
    async fn submit_agent(
        &self,
        invocation: AgentInvocation,
        caller_cancel: tokio_util::sync::CancellationToken,
    ) -> Result<TaskSnapshot, TaskPortError> {
        if invocation.isolation == "worktree" && !self.worktree_enabled {
            return Err(TaskPortError::new(
                "FEATURE_NOT_READY",
                "worktree isolation has not passed the production gate",
                false,
            ));
        }
        if invocation.isolation == "sharedWorkspace" && !self.shared_workspace_enabled {
            return Err(TaskPortError::new(
                "FEATURE_NOT_READY",
                "sharedWorkspace writes are disabled until the write-lease gate passes",
                false,
            ));
        }
        let parent = validate_agent_invocation(&self.db, &invocation).await?;
        let model = resolve_agent_model(
            &self.providers.load(),
            invocation.model_override.as_deref(),
            invocation.subagent_type.as_deref(),
            &parent.model,
        )
        .map_err(|message| TaskPortError::new("AGENT_MODEL_INVALID", message, false))?;

        let (isolation, allow_write_tools) = match invocation.isolation.as_str() {
            "worktree" => (
                IsolationMode::Worktree,
                self.child_write_enabled && self.worktree_enabled,
            ),
            "sharedWorkspace" => (
                IsolationMode::None,
                self.child_write_enabled && self.shared_workspace_enabled,
            ),
            "readOnly" => (IsolationMode::None, false),
            _ => {
                return Err(TaskPortError::new(
                    "INVALID_ISOLATION",
                    "unknown Agent isolation mode",
                    false,
                ));
            }
        };
        if self.startup_epoch <= 0 {
            return Err(TaskPortError::new(
                "STARTUP_EPOCH_NOT_READY",
                "durable process startup has not completed",
                true,
            ));
        }
        let persisted_allowed_tools = invocation.allowed_tools.clone().map_or_else(
            || {
                READ_ONLY_CHILD_TOOLS
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect::<Vec<_>>()
            },
            |tools| tools.into_iter().collect::<Vec<_>>(),
        );
        let execution_config_json = serde_json::json!({
            "isolation": invocation.isolation,
            "subagentType": invocation.subagent_type,
            "model": model,
            "waitMode": invocation.wait_mode,
            "lifecycle": "attached",
            "allowWriteTools": allow_write_tools,
            "allowedTools": persisted_allowed_tools,
            "permissionPolicyVersion": 1,
        })
        .to_string();
        let mut submission = ChildTaskSubmission::attached(
            invocation.parent_session_id.clone(),
            parent.task_id,
            invocation.parent_run_id.clone(),
            invocation.tool_use_id.clone(),
            invocation.description.clone(),
            invocation.prompt.clone(),
            model.clone(),
            invocation.working_directory.to_string_lossy(),
        );
        submission.execution_config_json = execution_config_json;
        submission.startup_epoch = self.startup_epoch;
        let wait_mode = invocation.wait_mode.clone();
        let executor = Arc::clone(&self.executor);
        let agent_type = invocation.subagent_type.clone();
        let prompt = invocation.prompt.clone();
        let parent_session_id = invocation.parent_session_id.clone();
        let parent_run_id = invocation.parent_run_id.clone();
        let working_directory = invocation.working_directory.clone();
        let tool_use_id = invocation.tool_use_id.clone();
        let allowed_tools = invocation.allowed_tools.clone();
        let receipt = self
            .runtime
            .submit_child(submission, move |execution| async move {
                let request = AgentRequest::new(
                    execution.task_id.clone(),
                    prompt,
                    agent_type,
                    Some(model),
                    isolation,
                    false,
                );
                let context = ChildExecutionContext {
                    parent_session_id,
                    parent_run_id,
                    working_directory,
                    tool_use_id,
                    allowed_tools,
                    allow_write_tools,
                    write_tool_allowlist: None,
                    include_project_prompt: true,
                };
                let budget = execution.budget.clone();
                let persisted = match PersistedChildExecution::try_new(
                    execution.task_id,
                    execution.run_id,
                    execution.transcript_session_id,
                ) {
                    Ok(identity) => identity,
                    Err(error) => {
                        return TaskExecutionResult::Failed {
                            message: error,
                            code: "PERSISTED_EXECUTION_INVALID".to_owned(),
                        };
                    }
                };
                let result = executor
                    .execute_precreated_with_cancel(
                        &request,
                        &context,
                        &persisted,
                        budget,
                        execution.cancel,
                    )
                    .await;
                agent_result_to_task_result(result)
            })
            .await
            .map_err(port_error)?;
        let mut snapshot = runtime_snapshot(&self.db, receipt.task).await?;
        if wait_mode == "background" {
            return Ok(snapshot);
        }
        if snapshot_requires_attention(&snapshot) {
            return Err(needs_attention_error(&snapshot));
        }
        if snapshot_is_terminal(&snapshot) {
            return Ok(snapshot);
        }

        loop {
            let request = RuntimeOutputRequest {
                root_session_id: invocation.parent_session_id.clone(),
                task_id: snapshot.task_id.clone(),
                wait_ms: 30_000,
                result_version: None,
                cursor: 0,
                max_bytes: zk_db::INLINE_RESULT_LIMIT,
            };
            tokio::select! {
                () = caller_cancel.cancelled() => {
                    let _ = self.runtime.cancel_attached_from_parent(
                        &invocation.parent_session_id,
                        &snapshot.task_id,
                        "parentToolCancelled",
                    ).await;
                    return Err(TaskPortError::new(
                        "AGENT_WAIT_CANCELLED",
                        "waiting for child result was cancelled",
                        false,
                    ));
                }
                output = self.runtime.read_output(request) => {
                    let output = output.map_err(port_error)?;
                    snapshot = runtime_snapshot_with_result(&self.db, output.task, output.result).await?;
                    if snapshot_requires_attention(&snapshot) {
                        return Err(needs_attention_error(&snapshot));
                    }
                    if snapshot_is_terminal(&snapshot) {
                        return Ok(snapshot);
                    }
                }
            }
        }
    }
}

impl TaskCoordinatorPort for TaskCoordinatorBridge {
    fn submit_task(
        &self,
        invocation: TaskInvocation,
    ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>> {
        Box::pin(async move {
            self.submit_agent(
                AgentInvocation {
                    prompt: invocation.prompt,
                    description: invocation.description,
                    subagent_type: None,
                    model_override: None,
                    isolation: "readOnly".to_owned(),
                    wait_mode: "background".to_owned(),
                    parent_session_id: invocation.session_id,
                    parent_run_id: invocation.parent_run_id,
                    working_directory: invocation.working_directory,
                    tool_use_id: invocation.tool_use_id,
                    allowed_tools: None,
                },
                tokio_util::sync::CancellationToken::new(),
            )
            .await
        })
    }

    fn cancel_task(
        &self,
        task_id: String,
        session_id: String,
        reason: String,
    ) -> BoxFuture<'_, Result<TaskStopReceipt, TaskPortError>> {
        Box::pin(async move {
            let task = self
                .runtime
                .get_owned(&session_id, &task_id)
                .await
                .map_err(port_error)?
                .ok_or_else(|| {
                    TaskPortError::new(
                        "TASK_NOT_FOUND",
                        "Task does not exist in the current root session",
                        false,
                    )
                })?;
            let run_id = task.current_run_id.ok_or_else(|| {
                TaskPortError::new("TASK_RUN_NOT_FOUND", "Task has no current Run", false)
            })?;
            let transition = self
                .terminations
                .cancel_by_user(&run_id, Some(&reason))
                .await
                .map_err(|error| TaskPortError::new(error.code.clone(), error.to_string(), true))?;
            let task = self
                .runtime
                .get_owned(&session_id, &task_id)
                .await
                .map_err(port_error)?
                .ok_or_else(|| TaskPortError::new("TASK_NOT_FOUND", "Task disappeared", false))?;
            Ok(TaskStopReceipt {
                cancel_requested: transition == zk_db::run::TransitionResult::Applied,
                task: runtime_snapshot(&self.db, task).await?,
            })
        })
    }

    fn get_task(
        &self,
        task_id: String,
        session_id: String,
    ) -> BoxFuture<'_, Result<Option<TaskSnapshot>, TaskPortError>> {
        Box::pin(async move {
            let task = self
                .runtime
                .get_owned(&session_id, &task_id)
                .await
                .map_err(port_error)?;
            match task {
                Some(task) => runtime_snapshot(&self.db, task).await.map(Some),
                None => Ok(None),
            }
        })
    }

    fn list_tasks(
        &self,
        session_id: String,
        filter_status: Option<String>,
    ) -> BoxFuture<'_, Result<Vec<TaskSnapshot>, TaskPortError>> {
        Box::pin(async move {
            let filter = filter_status
                .as_deref()
                .map(parse_task_status)
                .transpose()?;
            let tasks = self
                .runtime
                .list_owned(&session_id, filter)
                .await
                .map_err(port_error)?;
            let mut snapshots = Vec::with_capacity(tasks.len());
            for task in tasks {
                snapshots.push(runtime_snapshot(&self.db, task).await?);
            }
            Ok(snapshots)
        })
    }

    fn update_task(
        &self,
        task_id: String,
        session_id: String,
        description: Option<String>,
        plan: Option<String>,
        reported_progress: Option<f64>,
    ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>> {
        Box::pin(async move {
            let task = self
                .runtime
                .update_advisory(
                    &session_id,
                    &task_id,
                    description.as_deref(),
                    plan.as_deref(),
                    reported_progress,
                )
                .await
                .map_err(port_error)?;
            runtime_snapshot(&self.db, task).await
        })
    }

    fn read_output(
        &self,
        query: TaskOutputQuery,
    ) -> BoxFuture<'_, Result<TaskOutputPage, TaskPortError>> {
        Box::pin(async move {
            let cursor = query
                .cursor
                .as_deref()
                .map(str::parse::<usize>)
                .transpose()
                .map_err(|_| {
                    TaskPortError::new("RESULT_CURSOR_INVALID", "cursor is invalid", false)
                })?
                .unwrap_or(0);
            let request = RuntimeOutputRequest {
                root_session_id: query.session_id,
                task_id: query.task_id,
                wait_ms: query.wait_ms,
                result_version: query.result_version,
                cursor,
                max_bytes: query.max_bytes,
            };
            let response = tokio::select! {
                () = query.cancel.cancelled() => {
                    return Err(TaskPortError::new("TASK_OUTPUT_CANCELLED", "result wait was cancelled", false));
                }
                response = self.runtime.read_output(request) => response.map_err(port_error)?,
            };
            let next_cursor = response
                .result
                .as_ref()
                .and_then(|result| result.next_cursor)
                .map(|cursor| cursor.to_string());
            let content = response
                .result
                .as_ref()
                .map(|result| result.content.clone());
            let mut task =
                runtime_snapshot_with_result(&self.db, response.task, response.result).await?;
            task.wait_expired = response.wait_expired;
            Ok(TaskOutputPage {
                task,
                content,
                next_cursor,
            })
        })
    }
}

fn port_error(error: TaskRuntimeError) -> TaskPortError {
    TaskPortError::new(error.code, error.message, error.retryable)
}

fn parse_task_status(value: &str) -> Result<zk_db::TaskStatus, TaskPortError> {
    match value {
        "queued" => Ok(zk_db::TaskStatus::Queued),
        "running" => Ok(zk_db::TaskStatus::Running),
        "waitingDependencies" => Ok(zk_db::TaskStatus::WaitingDependencies),
        "waitingInteraction" => Ok(zk_db::TaskStatus::WaitingInteraction),
        "cancelling" => Ok(zk_db::TaskStatus::Cancelling),
        "needsAttention" => Ok(zk_db::TaskStatus::NeedsAttention),
        "succeeded" => Ok(zk_db::TaskStatus::Succeeded),
        "partial" => Ok(zk_db::TaskStatus::Partial),
        "failed" => Ok(zk_db::TaskStatus::Failed),
        "cancelled" => Ok(zk_db::TaskStatus::Cancelled),
        _ => Err(TaskPortError::new(
            "INVALID_TASK_STATUS",
            "unknown task status",
            false,
        )),
    }
}

fn agent_result_to_task_result(result: zk_engine::agent::AgentResult) -> TaskExecutionResult {
    let error_code = result.error_code;
    let content = result.result.unwrap_or_default();
    if error_code.as_deref() == Some("SUBAGENT_STOPPED_PARTIAL") {
        return TaskExecutionResult::Partial {
            content,
            code: "SUBAGENT_STOPPED_PARTIAL".to_owned(),
        };
    }
    match result.status {
        AgentStatus::Completed => TaskExecutionResult::Complete(content),
        AgentStatus::MaxTurns => TaskExecutionResult::Partial {
            content,
            code: error_code.unwrap_or_else(|| "MAX_TURNS".to_owned()),
        },
        AgentStatus::BudgetExhausted => TaskExecutionResult::Partial {
            content,
            code: error_code.unwrap_or_else(|| "BUDGET_EXHAUSTED".to_owned()),
        },
        AgentStatus::Timeout => TaskExecutionResult::Failed {
            message: content,
            code: error_code.unwrap_or_else(|| "TIMEOUT".to_owned()),
        },
        AgentStatus::Interrupted => TaskExecutionResult::Cancelled { message: content },
        AgentStatus::Failed | AgentStatus::AsyncLaunched => TaskExecutionResult::Failed {
            message: content,
            code: error_code.unwrap_or_else(|| "AGENT_EXECUTION_FAILED".to_owned()),
        },
    }
}

fn snapshot_is_terminal(snapshot: &TaskSnapshot) -> bool {
    matches!(
        snapshot.status.as_str(),
        "succeeded" | "partial" | "failed" | "cancelled"
    )
}

fn snapshot_requires_attention(snapshot: &TaskSnapshot) -> bool {
    snapshot.status == "needsAttention"
}

fn needs_attention_error(snapshot: &TaskSnapshot) -> TaskPortError {
    TaskPortError::new(
        "AGENT_TASK_NEEDS_ATTENTION",
        snapshot
            .reason
            .clone()
            .unwrap_or_else(|| "child Task requires operator attention".to_owned()),
        false,
    )
}

async fn runtime_snapshot(
    db: &zk_db::Db,
    task: zk_db::RuntimeTaskRecord,
) -> Result<TaskSnapshot, TaskPortError> {
    let result = db
        .read_task_result(&task.id, None, 0, zk_db::INLINE_RESULT_LIMIT)
        .await
        .map_err(|error| TaskPortError::new("TASK_STORAGE_ERROR", error.to_string(), true))?;
    runtime_snapshot_with_result(db, task, result).await
}

async fn runtime_snapshot_with_result(
    db: &zk_db::Db,
    task: zk_db::RuntimeTaskRecord,
    result: Option<zk_db::TaskResultChunk>,
) -> Result<TaskSnapshot, TaskPortError> {
    let tree = db
        .find_task_tree_owned(&task.session_id)
        .await
        .map_err(|error| TaskPortError::new("TASK_STORAGE_ERROR", error.to_string(), true))?;
    let child_count = tree
        .iter()
        .filter(|candidate| candidate.parent_task_id.as_deref() == Some(task.id.as_str()))
        .count();
    let run = match task.current_run_id.as_deref() {
        Some(run_id) => db
            .find_run_by_id(run_id)
            .await
            .map_err(|error| TaskPortError::new("TASK_STORAGE_ERROR", error.to_string(), true))?,
        None => None,
    };
    let output = result
        .as_ref()
        .filter(|chunk| {
            matches!(
                chunk.result.status,
                zk_db::ResultStatus::Complete | zk_db::ResultStatus::Partial
            )
        })
        .map(|chunk| chunk.content.clone());
    let error = result
        .as_ref()
        .filter(|chunk| {
            matches!(
                chunk.result.status,
                zk_db::ResultStatus::Error | zk_db::ResultStatus::Cancelled
            )
        })
        .map(|chunk| chunk.content.clone());
    let result_version = result.as_ref().map(|chunk| chunk.result.result_version);
    let result_ref = result.as_ref().map(|chunk| {
        format!(
            "task-result:{}:{}:{}",
            task.id, chunk.result.result_version, chunk.result.content_sha256
        )
    });
    let partial = result.as_ref().is_some_and(|chunk| chunk.partial);
    let usage_summary = run.as_ref().map_or_else(
        || serde_json::json!({"complete": task.usage_complete}),
        |run| {
            serde_json::json!({
                "inputTokens": run.input_tokens,
                "outputTokens": run.output_tokens,
                "cacheReadTokens": run.cache_read_tokens,
                "cacheCreateTokens": run.cache_create_tokens,
                "costNanosUsd": run.cost_nanos_usd,
                "complete": run.usage_complete && task.usage_complete,
            })
        },
    );
    Ok(TaskSnapshot {
        task_id: task.id,
        session_id: task.session_id,
        parent_task_id: task.parent_task_id,
        run_id: task.current_run_id,
        status: task.status.as_db().to_owned(),
        reason: task.reason,
        description: Some(task.description),
        output,
        error,
        result_version,
        partial,
        result_ref,
        cleanup_status: task.cleanup_status.as_db().to_owned(),
        usage_summary,
        wait_expired: false,
        created_at: zk_db::time::parse_rfc3339_millis(&task.created_at).unwrap_or(0),
        child_count,
    })
}

/// 工具注册表装配（2.3 基础工具族 + Batch 2 工具域 + 2.6 Python 桥接族；
/// [`ToolRegistry`] 内部 `BTreeMap` 故 `tools` 声明序恒为工具名字典序）。
///
/// 2.3 的 9 件：文件族 5（`Read` / `Write` / `ListDir` / `Glob` / `Grep`）+
/// `Bash` 进程基座 + git 三件（`GitDiff` / `GitLog` / `GitStatus`）。`Write`
/// 注入 [`DbSnapshotSink`]——写前旧内容落 `file_snapshots`（best-effort，失败
/// 仅告警不阻断写入）。EchoTool 已退场（2.2 的链路占位物）。
///
/// Batch 2 追加 5 件（旧 `tool/impl/FileEditTool` / `tool/interaction/*` /
/// `tool/config/*`）：
///
/// - `Edit`：与 `Write` 共用同一 [`DbSnapshotSink`] 实例（写前快照同源）；
/// - `TodoWrite`：无外部端口（清单落 `.zk/todos.md` + 进程级存储）；
/// - `AskUserQuestion`：注入 [`DurableElicitationSink`]——发问经
///   `interaction_requests` 落库并等 WS 侧终态决策；未注入时工具走旧
///   `ERROR` 分支，故此处**必须**装配；
/// - `Config`：`model` 键的候选值经 [`RegistryModelCatalog`] 端口取
///   [`AppState::providers`] 的动态模型表；
/// - `SyntheticOutput`：`parameters()` 由运行期注入的 JSON Schema 决定，
///   注册的实例即 schema 持有者，故注册后不可替换（旧亦为单例 bean）。
///
/// 2.6 的 Python 桥接族由 [`sync_python_tool_registry`] 在首次能力探测后动态
/// 装配；构建期缓存为空时不会把不可用能力暴露给模型。
///
/// Batch 5 追加 1 件：
///
/// - `Memory`：经 `zk_tools::MemoryStore` 端口接到 [`AppState::db`]；工具与
///   `/api/memory` 域端点共享同一 `SQLite` 权威。
///
/// 安全裁决（Bash 四层解析器 / 权限管线）归 2.4-2.5：本阶段 `Bash` 为
/// 直通模式，只有进程树管理与超时/截断护栏。
pub(crate) fn build_tool_registry(state: &AppState) -> ToolRegistry {
    let search_endpoint = std::env::var("ZK_WEB_SEARCH_ENDPOINT")
        .ok()
        .filter(|endpoint| !endpoint.trim().is_empty());
    build_tool_registry_with_search_endpoint(state, search_endpoint.as_deref())
}

#[allow(clippy::too_many_lines)] // production composition root intentionally lists every tool
fn build_tool_registry_with_search_endpoint(
    state: &AppState,
    search_endpoint: Option<&str>,
) -> ToolRegistry {
    let snapshot_sink = Arc::new(DbSnapshotSink::new(state.db.clone()));
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(ReadFileTool));
    registry.register(Arc::new(WriteFileTool::with_snapshot_sink(
        snapshot_sink.clone(),
    )));
    registry.register(Arc::new(EditFileTool::with_snapshot_sink(
        snapshot_sink.clone(),
    )));
    registry.register(Arc::new(ListDirectoryTool));
    registry.register(Arc::new(GlobTool));
    registry.register(Arc::new(GrepTool));
    registry.register(Arc::new(BashTool));
    registry.register(Arc::new(GitDiffTool));
    registry.register(Arc::new(GitLogTool));
    registry.register(Arc::new(GitStatusTool));
    registry.register(Arc::new(TodoWriteTool));
    registry.register(Arc::new(AskUserQuestionTool::with_elicitation_sink(
        Arc::new(DurableElicitationSink::new(
            state.authz.interactions.clone(),
        )),
    )));
    registry.register(Arc::new(ConfigTool::with_model_catalog(Arc::new(
        RegistryModelCatalog {
            providers: state.providers.clone(),
        },
    ))));
    registry.register(Arc::new(MemoryTool::with_store(Arc::new(
        DbMemoryStore::new(state.db.clone()),
    ))));
    registry.register(Arc::new(SyntheticOutputTool::new()));
    let mcp_slot = state.mcp_slot();
    registry.register(Arc::new(ListMcpResourcesTool::new(Arc::clone(&mcp_slot))));
    registry.register(Arc::new(ReadMcpResourceTool::new(Arc::clone(&mcp_slot))));
    // Batch 7: 计划模式 + Snip + CtxInspect + VerifyPlanExecution（5 件）。
    registry.register(Arc::new(EnterPlanModeTool));
    registry.register(Arc::new(ExitPlanModeTool));
    registry.register(Arc::new(SnipTool));
    registry.register(Arc::new(CtxInspectTool::new(None)));
    registry.register(Arc::new(VerifyPlanExecutionTool));
    registry.register(Arc::new(NotebookEditTool::with_snapshot_sink(
        snapshot_sink,
    )));
    registry.register(Arc::new(SleepTool));
    registry.register(Arc::new(VisualizationTool));
    registry.register(Arc::new(BrowserVerifyJourneyTool::new(
        state.python.clone(),
        state.db.clone(),
    )));
    let safe_http: Arc<dyn zk_tools::WebFetchPort> = Arc::new(SafeHttpFetchPort::new());
    registry.register(Arc::new(WebFetchTool::new(Arc::clone(&safe_http))));
    let search_backend = select_search_backend(search_endpoint, Arc::clone(&safe_http), mcp_slot);
    registry.register(Arc::new(WebSearchTool::new(search_backend)));
    let mut skill_fork_backend: Option<Arc<dyn AgentToolBackend>> = None;
    // 子代理与 Task 在安全冻结解除前不进入生产目录。启用 Agent 但尚未启用
    // 写能力时，工厂在注册期裁掉 Write/Edit/Bash。
    if state.config.agent_enabled {
        let mailbox_router = Arc::new(AgentMailboxRouter::default());
        // Batch 8H：真实子代理引擎工厂注入（替代占位 `PlaceholderEngineFactory`）。
        // `RealSubAgentEngineFactory` 持有 DB / provider / 预过滤子代理工具注册表，
        // 不持有父 `Engine` 引用，避免循环依赖。`&registry` 在此处仅被读取
        // （`build_sub_agent_registry` 过滤白名单子集后 Clone 为独立 Arc），
        // 不影响后续工具注册。
        let child_tools =
            build_sub_agent_registry_with_policy(&registry, state.config.agent_write_enabled);
        let child_admission: Arc<dyn ToolAdmission> = Arc::new(EngineAdmission::new(
            state.authz.clone(),
            Arc::clone(&child_tools),
        ));
        let child_llm_summarizer = Arc::new(LlmSummarizer::new(
            state.providers.clone(),
            select_lightweight_model(
                &state.providers.load(),
                std::env::var("ZK_LIGHTWEIGHT_MODEL").ok().as_deref(),
            ),
        ));
        let child_compact_summarizer: Arc<dyn zk_engine::context::compact::Summarizer> =
            child_llm_summarizer.clone();
        let child_tool_summarizer: Arc<dyn zk_engine::LightModelSummarizer> = child_llm_summarizer;
        let factory = RealSubAgentEngineFactory::new_with_production_services(
            state.db.clone(),
            state.providers.clone(),
            child_tools,
            Arc::new(HubSink {
                hub: state.hub.clone(),
                db: state.db.clone(),
            }),
            child_admission,
            state.costs.clone(),
            state.file_history.clone(),
            state.hooks.clone(),
            Arc::clone(&state.observability),
            Arc::clone(&state.execution_supervisor),
            child_compact_summarizer,
            child_tool_summarizer,
        );
        let executor = Arc::new(SubAgentExecutor::new_with_mailbox_router(
            Arc::new(AgentConcurrencyController::default()),
            Arc::new(factory),
            WorktreeManager::for_repo(
                &state.config.workspace_default_root,
                Arc::new(SystemGitCommandRunner),
            )
            .expect("validated workspace_default_root for WorktreeManager"),
            AgentTimeoutConfig::default(),
            Arc::clone(&mailbox_router),
        ));
        let runtime = Arc::clone(&state.task_runtime);
        let agent_runtime = Arc::new(AgentRuntime {
            executor: Arc::clone(&executor),
            tasks: Arc::clone(&runtime),
        });
        state.set_agent_runtime(agent_runtime);
        let task_port = Arc::new(TaskCoordinatorBridge {
            runtime: Arc::clone(&runtime),
            terminations: Arc::clone(&state.authz.terminations),
            executor: Arc::clone(&executor),
            db: state.db.clone(),
            providers: Arc::clone(&state.providers),
            worktree_enabled: state.config.worktree_enabled,
            shared_workspace_enabled: state.shared_workspace_executable(),
            child_write_enabled: state.config.agent_write_enabled,
            startup_epoch: state.startup_epoch(),
        });
        registry.register(Arc::new(SendMessageTool::new(Arc::new(
            SendMessageBackendBridge {
                router: Arc::clone(&mailbox_router),
                runtime: Arc::clone(&runtime),
                db: state.db.clone(),
                event_bus: Arc::clone(state.coordinator.event_bus()),
            },
        ))));
        let agent_backend: Arc<dyn AgentToolBackend> = Arc::new(AgentBackendBridge {
            task_port: Arc::clone(&task_port),
        });
        registry.register(Arc::new(AgentTool::new(Arc::clone(&agent_backend))));
        skill_fork_backend = Some(agent_backend);
        let port: Arc<dyn TaskCoordinatorPort> = task_port;
        registry.register(Arc::new(TaskCreateTool::new(Arc::clone(&port))));
        registry.register(Arc::new(TaskUpdateTool::new(Arc::clone(&port))));
        registry.register(Arc::new(TaskListTool::new(Arc::clone(&port))));
        registry.register(Arc::new(TaskGetTool::new(Arc::clone(&port))));
        registry.register(Arc::new(TaskOutputTool::new(Arc::clone(&port))));
        registry.register(Arc::new(TaskStopTool::new(port)));
        // Cron is a gated adapter over this exact TaskRuntime/Executor pair.
        // Registering inside the successfully assembled Agent branch prevents
        // the model from seeing schedule tools that cannot execute jobs.
        if state.config.cron_enabled {
            let cron: Arc<dyn CronTaskPort> = state.cron_service.clone();
            registry.register(Arc::new(CronCreateTool::new(Arc::clone(&cron))));
            registry.register(Arc::new(CronListTool::new(Arc::clone(&cron))));
            registry.register(Arc::new(CronDeleteTool::new(cron)));
        }
    }
    // Batch 8D: P2 工具域第一波（4 件常驻 + flag 门控的 Cron 三件）。
    // `Monitor` 持 `FeatureFlags` 句柄走**执行期**门（旧
    // `isEnabled("RESOURCE_MONITOR")` 每次调用都问，运行时翻转立即生效）。
    registry.register(Arc::new(MonitorTool::new(Arc::clone(&state.feature_flags))));
    // 每注册表一个 `ReplManager`：会话表随注册表生命周期，`Drop` 时子进程
    // 经 `kill_on_drop` 回收（旧 `@PreDestroy → destroyAll()` 的等价物）。
    registry.register(Arc::new(REPLTool::new(Arc::new(ReplManager::new()))));
    if state.config.worktree_enabled {
        registry.register(Arc::new(WorktreeTool));
    }
    registry.register(Arc::new(TerminalCaptureTool));
    let mut skill_known_tools = registry.names();
    // 技能声明在能力暂时不可用时仍可加载；真正执行时，引擎目录只提供当前
    // 动态注册的交集，不会绕过 Python 能力门。
    skill_known_tools.extend(["WebBrowser", "CodeIntel", "Git"].map(str::to_owned));
    skill_known_tools.push("Skill".to_owned());
    registry.register(Arc::new(SkillTool::new(
        Arc::clone(&state.skills),
        Arc::clone(&state.providers),
        state.db.clone(),
        skill_known_tools,
        skill_fork_backend,
    )));
    let mut descriptors: Vec<ToolDescriptor> = registry
        .specs()
        .into_iter()
        .map(|spec| ToolDescriptor::new(spec.name, spec.description, spec.parameters))
        .collect();
    descriptors.push(ToolSearchTool::descriptor());
    registry.register(Arc::new(ToolSearchTool::new(Arc::new(
        StaticToolCatalog::new(descriptors),
    ))));
    registry
}

fn select_search_backend(
    search_endpoint: Option<&str>,
    safe_http: Arc<dyn zk_tools::WebFetchPort>,
    mcp_slot: Arc<OnceLock<Arc<zk_mcp::McpClientManager>>>,
) -> Arc<dyn zk_tools::SearchBackend> {
    if let Some(endpoint) = search_endpoint {
        match SearxngSearchBackend::new(endpoint, safe_http) {
            Ok(backend) => return Arc::new(backend),
            Err(error) => {
                tracing::warn!(
                    code = error.code,
                    "configured SearXNG endpoint rejected; falling back to MCP web search"
                );
            }
        }
    }
    Arc::new(McpSearchBackend::new(mcp_slot))
}

/// 将 Python 桥接工具目录与最近一次能力快照同步。
///
/// 只读缓存、无 UDS IO；调用者必须先完成 `refresh_capabilities`。动态变化后
/// 同步重建 `ToolSearch` 的静态快照，使 REST、LLM tools 与工具搜索同源。
pub fn sync_python_tool_registry(
    registry: &ToolRegistry,
    python: &Arc<PythonClient>,
    browser_replay: Arc<BrowserReplayStore>,
    web_browser_enabled: bool,
    git_enhanced_enabled: bool,
) {
    let probe_succeeded = python.last_refresh_succeeded();
    sync_python_tool(
        registry,
        "CodeIntel",
        probe_succeeded && python.cached_capability_available("CODE_INTEL"),
        || Arc::new(CodeIntelTool::new(Arc::clone(python))),
    );
    sync_python_tool(
        registry,
        "WebBrowser",
        probe_succeeded
            && web_browser_enabled
            && python.cached_capability_available("BROWSER_AUTOMATION"),
        || {
            Arc::new(WebBrowserTool::with_replay_store(
                Arc::clone(python),
                browser_replay,
            ))
        },
    );
    sync_python_tool(
        registry,
        "Git",
        probe_succeeded
            && git_enhanced_enabled
            && python.cached_capability_available("GIT_ENHANCED"),
        || Arc::new(GitEnhancedTool::new(Arc::clone(python))),
    );
    refresh_tool_search_catalog(registry);
}

fn sync_python_tool(
    registry: &ToolRegistry,
    name: &str,
    enabled: bool,
    build: impl FnOnce() -> Arc<dyn zk_tools::Tool>,
) {
    match (enabled, registry.get(name).is_some()) {
        (true, false) => registry.register_dynamic(build()),
        (false, true) => {
            registry.unregister(name);
        }
        _ => {}
    }
}

pub(crate) fn refresh_tool_search_catalog(registry: &ToolRegistry) {
    let mut descriptors: Vec<ToolDescriptor> = registry
        .specs()
        .into_iter()
        .filter(|spec| spec.name != "ToolSearch")
        .map(|spec| ToolDescriptor::new(spec.name, spec.description, spec.parameters))
        .collect();
    descriptors.push(ToolSearchTool::descriptor());
    registry.replace_dynamic(Arc::new(ToolSearchTool::new(Arc::new(
        StaticToolCatalog::new(descriptors),
    ))));
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;

    use super::{
        PersistedChildExecution, SendMessageBackendBridge, build_tool_registry,
        build_tool_registry_with_search_endpoint, needs_attention_error, resolve_agent_model,
        select_search_backend, snapshot_requires_attention, sync_python_tool_registry,
    };
    use crate::config::Config;
    use crate::python::CapabilityStatus;
    use crate::state::AppState;
    use futures::future::BoxFuture;
    use sha2::{Digest, Sha256};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use zk_db::Db;
    use zk_engine::{
        AgentConcurrencyController, AgentMailboxMessage, AgentMailboxRouter, AgentRequest,
        AgentTimeoutConfig, ChildExecutionContext, IsolationMode, MessageSink,
        SubAgentEngineFactory, SubAgentExecutor, SystemGitCommandRunner, WorktreeManager,
        task::{ChildTaskSubmission, TaskExecutionResult, TaskRuntime},
    };
    use zk_tools::{
        SearchRequest, SendMessageBackend, SendMessageInvocation, TaskSnapshot, WebFetchError,
        WebFetchPort, WebFetchRequest, WebFetchResponse,
    };

    fn test_root_budget() -> zk_db::TaskBudgetLimits {
        zk_db::TaskBudgetLimits {
            token_limit: Some(1_000_000),
            cost_limit_nanos_usd: Some(1_000_000_000_000),
            deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
        }
    }

    async fn install_test_startup_epoch(state: &AppState, db: &Db) -> i64 {
        let epoch = db
            .begin_runtime_startup_epoch()
            .await
            .expect("allocate test startup epoch");
        state
            .set_startup_epoch(epoch)
            .expect("install test startup epoch");
        epoch
    }

    struct SearchFixtureFetch;

    struct TestSink;

    impl MessageSink for TestSink {
        fn push<'a>(
            &'a self,
            _session_id: &'a str,
            _message: zk_protocol::ServerMessage,
        ) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
    }

    impl WebFetchPort for SearchFixtureFetch {
        fn fetch(
            &self,
            request: WebFetchRequest,
        ) -> BoxFuture<'_, Result<WebFetchResponse, WebFetchError>> {
            assert!(request.url.contains("q=rust+security"));
            Box::pin(futures::future::ready(Ok(WebFetchResponse {
                final_url: request.url,
                status: 200,
                content_type: "application/json".to_owned(),
                body: br#"{"results":[{"title":"SearX","url":"https://example.com/search","content":"preferred","engine":"fixture"}]}"#.to_vec(),
                truncated: false,
            })))
        }
    }

    /// 安全冻结期组合根注册清单：33 件真实工具。Agent、五件 Task 工具和
    /// Worktree 在各自显式安全开关开启前不会进入模型目录。
    /// Batch 7 的 5 件：`EnterPlanMode` / `ExitPlanMode` / `Snip` / `CtxInspect` /
    /// `VerifyPlanExecution` + Batch 8D 的 4 件常驻：`Monitor` / `REPL` /
    /// `TerminalCapture` / `Worktree`）。Cron 三件受 `AGENT_TRIGGERS` 注册期
    /// 门控（出厂关），故不在此清单——见
    /// [`tests::agent_triggers_flag_gates_the_cron_trio`]。
    #[test]
    fn registry_exposes_the_base_tool_family() {
        let state = AppState::for_tests();
        let registry = build_tool_registry(&state);
        let mut names = registry.names();
        names.sort_unstable();
        let flat: Vec<&str> = names.iter().map(String::as_str).collect();
        assert_eq!(
            flat,
            [
                "AskUserQuestion",
                "Bash",
                "Config",
                "CtxInspect",
                "Edit",
                "EnterPlanMode",
                "ExitPlanMode",
                "GitDiff",
                "GitLog",
                "GitStatus",
                "Glob",
                "Grep",
                "ListDir",
                "ListMcpResources",
                "Memory",
                "Monitor",
                "NotebookEdit",
                "REPL",
                "Read",
                "ReadMcpResource",
                "Skill",
                "Sleep",
                "Snip",
                "SyntheticOutput",
                "TerminalCapture",
                "TodoWrite",
                "ToolSearch",
                "VerifyJourney",
                "VerifyPlanExecution",
                "Visualization",
                "WebFetch",
                "WebSearch",
                "Write",
            ]
        );
        assert!(registry.get("Echo").is_none(), "EchoTool must be retired");
        for name in &names {
            let tool = registry.get(name).expect("registered");
            assert_eq!(tool.name(), name.as_str());
            assert!(!tool.description().is_empty());
        }
    }

    #[tokio::test]
    async fn production_registry_uses_shared_mcp_search_when_searxng_is_absent() {
        let state = AppState::for_tests();
        let slot = state.mcp_slot();
        assert!(Arc::ptr_eq(&slot, &state.mcp_slot()));
        let registry = build_tool_registry_with_search_endpoint(&state, None);
        let tool = registry.get("WebSearch").expect("WebSearch");
        let (tx, _rx) = mpsc::unbounded_channel();
        let output = tool
            .execute(
                serde_json::json!({"query": "rust", "limit": 1}),
                zk_tools::ToolContext::new(CancellationToken::new(), tx),
            )
            .await;
        assert!(output.is_error);
        assert_eq!(
            output.content,
            "WEB_SEARCH_UNAVAILABLE: MCP web search is not initialized"
        );
        assert!(slot.get().is_none(), "tool construction must stay lazy");
    }

    #[tokio::test]
    async fn configured_searxng_is_selected_before_the_mcp_backend() {
        let backend = select_search_backend(
            Some("https://search.example.com/search"),
            Arc::new(SearchFixtureFetch),
            Arc::new(OnceLock::new()),
        );
        let results = backend
            .search(SearchRequest {
                query: "rust security".to_owned(),
                limit: 1,
            })
            .await
            .expect("SearXNG result");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "SearX");
        assert_eq!(results[0].snippet, "preferred");
    }

    /// Names alone cannot detect an accidentally weakened input schema. Freeze a
    /// canonical digest over every production name/description/schema triple.
    #[test]
    fn base_tool_schema_sha256_is_stable() {
        let registry = build_tool_registry(&AppState::for_tests());
        let canonical: Vec<_> = registry
            .specs()
            .into_iter()
            .map(|spec| {
                serde_json::json!({
                    "description": spec.description,
                    "name": spec.name,
                    "parameters": spec.parameters,
                })
            })
            .collect();
        let encoded = serde_json::to_vec(&canonical).expect("canonical tool schemas");
        let digest = format!("{:x}", Sha256::digest(encoded));
        assert_eq!(
            digest,
            "c755caa676a8ecb5ed332ba1ba9577e410ea4f0932058768a3f3dfa6b29b5f9b"
        );
    }

    #[tokio::test]
    async fn production_notebook_tool_persists_pre_edit_snapshot() {
        let db = Db::open_in_memory().expect("db");
        let workspace = std::env::temp_dir().join(format!(
            "zk-notebook-production-snapshot-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&workspace).expect("workspace");
        let workspace = std::fs::canonicalize(workspace).expect("canonical workspace");
        let notebook = workspace.join("book.ipynb");
        let original = r#"{"cells":[{"cell_type":"markdown","metadata":{},"source":["old"]}],"metadata":{},"nbformat":4,"nbformat_minor":5}"#;
        std::fs::write(&notebook, original).expect("notebook");
        let session = db
            .create_session("model", workspace.to_str().expect("utf8 path"))
            .await
            .expect("session");
        let state = AppState::new(db.clone(), Config::test_config());
        let registry = build_tool_registry(&state);
        let read = registry.get("Read").expect("Read");
        let tool = registry.get("NotebookEdit").expect("NotebookEdit");
        let (tx, _rx) = mpsc::unbounded_channel();
        let context = zk_tools::ToolContext::new(CancellationToken::new(), tx)
            .with_working_dir(&workspace)
            .with_session_id(&session.id)
            .with_tool_use_id("notebook-call-1")
            .with_authorized_write_path(&notebook);
        let read_output = read
            .execute(serde_json::json!({"file_path": notebook}), context.clone())
            .await;
        assert!(!read_output.is_error, "{}", read_output.content);
        let output = tool
            .execute(
                serde_json::json!({
                    "notebook_path": notebook,
                    "command": "edit_cell",
                    "cell_index": 0,
                    "content": "new"
                }),
                context,
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        let snapshots = db
            .list_file_snapshots(&session.id)
            .await
            .expect("snapshots");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].content, original);
        assert_eq!(snapshots[0].operation, "notebook_edit");
        assert_eq!(snapshots[0].message_id.as_deref(), Some("notebook-call-1"));
        std::fs::remove_dir_all(workspace).expect("cleanup");
    }

    /// 配置只提供上限；成功能力探测后桥接族三件才进入目录，ToolSearch 同步
    /// 看见动态结果。
    #[tokio::test]
    async fn python_bridge_family_registers_only_after_capability_probe() {
        let state = state_with_python(true, true, true);
        let registry = build_tool_registry(&state);
        for name in ["WebBrowser", "CodeIntel", "Git"] {
            assert!(
                registry.get(name).is_none(),
                "unprobed {name} must stay hidden"
            );
        }
        seed_python_capabilities(
            &state,
            &["BROWSER_AUTOMATION", "CODE_INTEL", "GIT_ENHANCED"],
        );
        sync_python_tool_registry(
            &registry,
            &state.python,
            state.browser_replay.clone(),
            true,
            true,
        );
        let names = registry.names();
        assert_eq!(
            names,
            [
                "AskUserQuestion",
                "Bash",
                "CodeIntel",
                "Config",
                "CtxInspect",
                "Edit",
                "EnterPlanMode",
                "ExitPlanMode",
                "Git",
                "GitDiff",
                "GitLog",
                "GitStatus",
                "Glob",
                "Grep",
                "ListDir",
                "ListMcpResources",
                "Memory",
                "Monitor",
                "NotebookEdit",
                "REPL",
                "Read",
                "ReadMcpResource",
                "Skill",
                "Sleep",
                "Snip",
                "SyntheticOutput",
                "TerminalCapture",
                "TodoWrite",
                "ToolSearch",
                "VerifyJourney",
                "VerifyPlanExecution",
                "Visualization",
                "WebBrowser",
                "WebFetch",
                "WebSearch",
                "Write",
            ],
            "33 frozen base tools + 3 python bridge tools"
        );
        for name in ["WebBrowser", "CodeIntel", "Git"] {
            let tool = registry.get(name).expect("python bridge tool registered");
            assert_eq!(tool.name(), name);
            assert!(!tool.description().is_empty());
            assert!(tool.parameters().is_object());
        }
        let (tx, _rx) = mpsc::unbounded_channel();
        let output = registry
            .get("ToolSearch")
            .expect("ToolSearch")
            .execute(
                serde_json::json!({"query": "select:CodeIntel"}),
                zk_tools::ToolContext::new(CancellationToken::new(), tx),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.contains("CodeIntel"));
    }

    /// feature flag 关 → 对应工具不注册（旧 `isEnabled()` 注册期门等价物），
    /// 但 `CodeIntel` 无 flag 故仍在。
    #[test]
    fn feature_flags_gate_individual_bridge_tools() {
        let state = state_with_python(true, false, false);
        seed_python_capabilities(
            &state,
            &["BROWSER_AUTOMATION", "CODE_INTEL", "GIT_ENHANCED"],
        );
        let registry = build_tool_registry(&state);
        sync_python_tool_registry(
            &registry,
            &state.python,
            state.browser_replay.clone(),
            false,
            false,
        );
        let names = registry.names();
        assert_eq!(
            names.len(),
            34,
            "33 frozen base tools + CodeIntel (flag-less)"
        );
        assert!(names.contains(&"CodeIntel".to_owned()));
        assert!(registry.get("WebBrowser").is_none());
        assert!(registry.get("Git").is_none());
    }

    #[test]
    fn failed_capability_refresh_removes_stale_python_tools() {
        let state = state_with_python(true, true, true);
        seed_python_capabilities(
            &state,
            &["BROWSER_AUTOMATION", "CODE_INTEL", "GIT_ENHANCED"],
        );
        let registry = build_tool_registry(&state);
        sync_python_tool_registry(
            &registry,
            &state.python,
            state.browser_replay.clone(),
            true,
            true,
        );
        assert!(registry.get("CodeIntel").is_some());

        state.python.invalidate_capabilities();
        sync_python_tool_registry(
            &registry,
            &state.python,
            state.browser_replay.clone(),
            true,
            true,
        );
        for name in ["WebBrowser", "CodeIntel", "Git"] {
            assert!(registry.get(name).is_none(), "stale {name} must be removed");
        }
    }

    /// 侧车总开关关 → 桥接族全不注册，冻结期基础族 33 件完好。
    #[test]
    fn disabled_sidecar_leaves_base_family_intact() {
        let registry = build_tool_registry(&state_with_python(false, true, true));
        assert_eq!(registry.names().len(), 33);
        for name in ["WebBrowser", "CodeIntel", "Git"] {
            assert!(registry.get(name).is_none(), "{name} must not register");
        }
        assert!(registry.get("Bash").is_some(), "base family unaffected");
    }

    /// Cron requires both the explicit switch and a genuinely assembled Agent
    /// runtime. Either half missing keeps all three tools out of the catalog.
    #[test]
    fn cron_switch_and_agent_assembly_gate_the_cron_trio() {
        let mut unavailable = Config::test_config();
        unavailable.cron_enabled = true;
        let state = AppState::new(
            Db::open_in_memory().expect("in-memory db boots with migrations"),
            unavailable,
        );
        let registry = build_tool_registry(&state);
        for name in ["CronCreate", "CronList", "CronDelete"] {
            assert!(
                registry.get(name).is_none(),
                "{name} must stay hidden without an executable Agent runtime"
            );
        }

        let mut enabled = Config::test_config();
        enabled.agent_enabled = true;
        enabled.cron_enabled = true;
        let state = AppState::new(
            Db::open_in_memory().expect("in-memory db boots with migrations"),
            enabled,
        );
        let registry = build_tool_registry(&state);
        for name in ["CronCreate", "CronList", "CronDelete"] {
            assert!(registry.get(name).is_some(), "{name} must register");
        }
    }

    fn state_with_python(enabled: bool, web_browser: bool, git_enhanced: bool) -> AppState {
        let mut config = Config::test_config();
        config.python_enabled = enabled;
        config.feature_web_browser_tool = web_browser;
        config.feature_git_enhanced_tool = git_enhanced;
        AppState::new(
            Db::open_in_memory().expect("in-memory db boots with migrations"),
            config,
        )
    }

    fn seed_python_capabilities(state: &AppState, available: &[&str]) {
        let capabilities = available
            .iter()
            .map(|domain| {
                (
                    (*domain).to_owned(),
                    CapabilityStatus {
                        name: (*domain).to_owned(),
                        available: true,
                        reason: None,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        state.python.replace_capabilities_for_tests(capabilities);
    }

    #[test]
    fn explicit_readiness_flags_enable_agent_and_worktree_catalog_entries() {
        let mut config = Config::test_config();
        config.agent_enabled = true;
        config.worktree_enabled = true;
        let state = AppState::new(
            Db::open_in_memory().expect("in-memory db boots with migrations"),
            config,
        );
        let registry = build_tool_registry(&state);
        for name in [
            "Agent",
            "TaskCreate",
            "TaskUpdate",
            "TaskList",
            "TaskGet",
            "TaskOutput",
            "TaskStop",
            "SendMessage",
            "Worktree",
        ] {
            assert!(
                registry.get(name).is_some(),
                "enabled capability missing: {name}"
            );
        }
    }

    async fn invoke_production_shared_workspace(
        shared_workspace_enabled: bool,
    ) -> (bool, bool, serde_json::Value) {
        let mut config = Config::test_config();
        config.agent_enabled = true;
        config.agent_write_enabled = true;
        config.shared_workspace_enabled = shared_workspace_enabled;
        let db = Db::open_in_memory().expect("in-memory db boots with migrations");
        let state = AppState::new(db.clone(), config);
        let startup_epoch = install_test_startup_epoch(&state, &db).await;
        let registry = build_tool_registry(&state);
        let workspace = state.config.workspace_default_root.clone();
        let session = db
            .create_session("test-model", &workspace)
            .await
            .expect("root session");
        let run_id = uuid::Uuid::new_v4().to_string();
        db.start_root_run_with_budget_at_epoch(
            &run_id,
            &session.id,
            None,
            "test-model",
            &test_root_budget(),
            startup_epoch,
        )
        .await
        .expect("root task/run");
        let (tx, _rx) = mpsc::unbounded_channel();
        let output = registry
            .get("Agent")
            .expect("production Agent tool")
            .execute(
                serde_json::json!({
                    "prompt": "inspect the authorized workspace",
                    "waitMode": "background",
                    "isolation": "sharedWorkspace"
                }),
                zk_tools::ToolContext::new(CancellationToken::new(), tx)
                    .with_session_id(session.id)
                    .with_run_id(run_id)
                    .with_tool_use_id("shared-workspace-gate")
                    .with_working_dir(workspace),
            )
            .await;
        let structured =
            output.metadata.as_ref().expect("structured Agent response")["structuredResult"]
                .clone();
        (
            state.shared_workspace_executable(),
            output.is_error,
            structured,
        )
    }

    #[tokio::test]
    async fn shared_workspace_requires_its_independent_production_gate() {
        let (executable, is_error, response) = invoke_production_shared_workspace(false).await;
        assert!(!executable);
        assert!(is_error);
        assert_eq!(response["code"], "FEATURE_NOT_READY");

        let (executable, is_error, response) = invoke_production_shared_workspace(true).await;
        assert!(executable);
        assert!(!is_error, "{response}");
        assert!(response["taskId"].as_str().is_some());
    }

    #[tokio::test]
    async fn production_task_stop_returns_idempotent_v4_receipt() {
        let mut config = Config::test_config();
        config.agent_enabled = true;
        let db = Db::open_in_memory().expect("in-memory db boots with migrations");
        let state = AppState::new(db.clone(), config);
        let registry = build_tool_registry(&state);
        let session = db
            .create_session("test-model", &state.config.workspace_default_root)
            .await
            .expect("root session");
        let run_id = uuid::Uuid::new_v4().to_string();
        db.start_run(&run_id, &session.id, None, None, "test-model")
            .await
            .expect("root task/run");
        let tool = registry.get("TaskStop").expect("production TaskStop");

        let invoke = || {
            let (tx, _rx) = mpsc::unbounded_channel();
            zk_tools::ToolContext::new(CancellationToken::new(), tx)
                .with_session_id(session.id.clone())
        };
        let first = tool
            .execute(serde_json::json!({"taskId": run_id}), invoke())
            .await;
        assert!(!first.is_error, "{}", first.content);
        let first = &first.metadata.expect("structured receipt")["structuredResult"];
        assert_eq!(first["cancelRequested"], true);
        assert_eq!(first["status"], "cancelling");
        assert_eq!(first["cleanupStatus"], "pending");
        assert!(first.get("cancel_requested").is_none());
        assert!(first.get("cleanup_status").is_none());

        let second = tool
            .execute(serde_json::json!({"taskId": run_id}), invoke())
            .await;
        assert!(!second.is_error, "{}", second.content);
        let second = &second.metadata.expect("structured receipt")["structuredResult"];
        assert_eq!(second["cancelRequested"], false);
        assert_eq!(second["status"], "cancelling");
        assert_eq!(second["cleanupStatus"], "pending");
    }

    struct MailboxWaitingFactory;

    impl SubAgentEngineFactory for MailboxWaitingFactory {
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

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // Exercises persistence-before-delivery as one end-to-end fixture.
    async fn send_message_bridge_persists_before_delivery_and_emits_native_event() {
        let db = Db::open_in_memory().expect("db");
        db.create_session_with_id("parent-session", "test-model", "/tmp")
            .await
            .expect("parent session");
        let parent_task_id = uuid::Uuid::new_v4().to_string();
        let parent_run_id = uuid::Uuid::new_v4().to_string();
        let parent = db
            .create_task_with_run(&zk_db::CreateTaskWithRun {
                task_id: parent_task_id.clone(),
                run_id: parent_run_id.clone(),
                root_session_id: "parent-session".to_owned(),
                transcript_session_id: "parent-session".to_owned(),
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: "mailbox parent".to_owned(),
                prompt: Some("wait for child".to_owned()),
                task_type: "agent".to_owned(),
                model: "test-model".to_owned(),
                working_dir: "/tmp".to_owned(),
                execution_config_json: serde_json::json!({
                    "budget": {
                        "tokenLimit": 1_000_000,
                        "costLimitNanosUsd": 1_000_000_000_000_i64,
                        "deadlineAtMs": zk_db::time::now_millis() + 60_000,
                    }
                })
                .to_string(),
                startup_epoch: 1,
            })
            .await
            .expect("parent task/run");
        assert_eq!(
            db.claim_task_run_cas(&parent_task_id, &parent_run_id, parent.task.version)
                .await
                .expect("claim parent"),
            zk_db::CasOutcome::Applied
        );
        let target_task_id = uuid::Uuid::new_v4().to_string();
        let target_run_id = uuid::Uuid::new_v4().to_string();
        let target_session_id = uuid::Uuid::new_v4().to_string();
        let target = db
            .create_task_with_run(&zk_db::CreateTaskWithRun {
                task_id: target_task_id.clone(),
                run_id: target_run_id.clone(),
                root_session_id: "parent-session".to_owned(),
                transcript_session_id: target_session_id.clone(),
                parent_task_id: Some(parent_task_id.clone()),
                parent_run_id: Some(parent_run_id.clone()),
                creator_tool_use_id: Some(uuid::Uuid::new_v4().to_string()),
                ordinal: 0,
                description: "mailbox target".to_owned(),
                prompt: Some("wait".to_owned()),
                task_type: "agent".to_owned(),
                model: "test-model".to_owned(),
                working_dir: "/tmp".to_owned(),
                execution_config_json: "{}".to_owned(),
                startup_epoch: 1,
            })
            .await
            .expect("durable target task/run");
        assert_eq!(
            db.claim_task_run_cas(&target_task_id, &target_run_id, target.task.version)
                .await
                .expect("claim target"),
            zk_db::CasOutcome::Applied
        );
        let router = Arc::new(AgentMailboxRouter::default());
        let executor = Arc::new(SubAgentExecutor::new_with_mailbox_router(
            Arc::new(AgentConcurrencyController::default()),
            Arc::new(MailboxWaitingFactory),
            WorktreeManager::for_repo(
                std::env::current_dir().expect("current dir"),
                Arc::new(SystemGitCommandRunner),
            )
            .expect("worktree manager"),
            AgentTimeoutConfig::default(),
            Arc::clone(&router),
        ));
        let request = AgentRequest::new(
            target_task_id.clone(),
            "wait",
            None,
            Some("test-model".to_owned()),
            IsolationMode::None,
            false,
        );
        let context = ChildExecutionContext {
            parent_session_id: "parent-session".to_owned(),
            parent_run_id: parent_run_id.clone(),
            working_directory: std::path::PathBuf::from("/tmp"),
            tool_use_id: "spawn-tool".to_owned(),
            allowed_tools: None,
            allow_write_tools: false,
            write_tool_allowlist: None,
            include_project_prompt: true,
        };
        let persisted = PersistedChildExecution::try_new(
            target_task_id.clone(),
            target_run_id,
            target_session_id,
        )
        .expect("persisted target identity");
        let child = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move {
                executor
                    .execute_precreated_with_cancel(
                        &request,
                        &context,
                        &persisted,
                        zk_db::TaskBudgetLimits::default(),
                        CancellationToken::new(),
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !router.has_active_agent(&target_task_id) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("target becomes active");

        let event_bus = Arc::new(zk_engine::CoordinatorEventBus::new());
        let mut events = event_bus.subscribe();
        let runtime = Arc::new(TaskRuntime::new(db.clone(), Arc::new(TestSink)));
        let backend = SendMessageBackendBridge {
            router,
            runtime,
            db: db.clone(),
            event_bus,
        };
        backend
            .send_message(SendMessageInvocation {
                target_task_id: target_task_id.clone(),
                message: "continue".to_owned(),
                parent_session_id: "parent-session".to_owned(),
                parent_run_id,
                tool_use_id: "send-tool".to_owned(),
            })
            .await
            .expect("message queued and delivered");

        let durable = db
            .read_task_inbox(&target_task_id, &[], 10)
            .await
            .expect("durable inbox");
        assert_eq!(durable.len(), 1);
        assert_eq!(durable[0].content, "continue");
        assert_eq!(durable[0].status, zk_db::InboxStatus::Delivered);
        assert!(matches!(
            events.recv().await.expect("native event"),
            zk_engine::CoordinatorEvent::TeammateMessage { content, .. } if content == "continue"
        ));
        assert_eq!(
            child.await.expect("child task").result.as_deref(),
            Some("continue")
        );
    }

    #[tokio::test]
    async fn send_message_bridge_returns_terminal_receipt_without_fake_message_id() {
        let db = Db::open_in_memory().expect("db");
        db.create_session_with_id("parent-session", "test-model", "/tmp")
            .await
            .expect("parent session");
        db.start_run("parent-run", "parent-session", None, None, "test-model")
            .await
            .expect("parent run");
        let target_task_id = uuid::Uuid::new_v4().to_string();
        db.start_run(&target_task_id, "parent-session", None, None, "test-model")
            .await
            .expect("durable target task/run");
        let target = db
            .find_runtime_task_by_id(&target_task_id)
            .await
            .expect("target lookup")
            .expect("target task");
        db.commit_task_result(&zk_db::CommitTaskResult {
            task_id: target_task_id.clone(),
            run_id: target_task_id.clone(),
            expected_task_version: target.version,
            status: zk_db::ResultStatus::Error,
            content: "already failed".to_owned(),
            media_type: "text/plain".to_owned(),
            error_code: Some("TEST_TERMINAL".to_owned()),
            cleanup_status: zk_db::CleanupStatus::Confirmed,
            verification_status: zk_db::VerificationStatus::NotRequested,
        })
        .await
        .expect("commit terminal result");

        let backend = SendMessageBackendBridge {
            router: Arc::new(AgentMailboxRouter::default()),
            runtime: Arc::new(TaskRuntime::new(db.clone(), Arc::new(TestSink))),
            db: db.clone(),
            event_bus: Arc::new(zk_engine::CoordinatorEventBus::new()),
        };
        let invalid_sender = backend
            .send_message(SendMessageInvocation {
                target_task_id: target_task_id.clone(),
                message: "must not be delivered".to_owned(),
                parent_session_id: "parent-session".to_owned(),
                parent_run_id: "missing-parent-run".to_owned(),
                tool_use_id: "send-tool-invalid".to_owned(),
            })
            .await
            .expect_err("missing parent Run must fail closed");
        assert_eq!(invalid_sender.code, "SEND_MESSAGE_CONTEXT_INVALID");
        let receipt = backend
            .send_message(SendMessageInvocation {
                target_task_id: target_task_id.clone(),
                message: "late instruction".to_owned(),
                parent_session_id: "parent-session".to_owned(),
                parent_run_id: "parent-run".to_owned(),
                tool_use_id: "send-tool".to_owned(),
            })
            .await
            .expect("terminal target is a successful explicit acknowledgement");

        assert_eq!(receipt.message_id, None);
        assert_eq!(receipt.delivery_status, "terminal");
        assert_eq!(receipt.status, "failed");
        assert!(
            db.read_task_inbox(&target_task_id, &[], 10)
                .await
                .expect("read inbox")
                .is_empty(),
            "terminal acknowledgement must not invent a durable inbox row"
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // Keeps the real task/swarm JSONL path in one production fixture.
    async fn production_task_and_swarm_events_reach_real_jsonl_sink() {
        let root = std::env::temp_dir().join(format!(
            "zk-observability-runtime-real-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).expect("create real workspace");
        let input = root.join("task-input.txt");
        std::fs::write(&input, "durable task output\n").expect("write task input");

        let mut config = Config::test_config();
        config.agent_enabled = true;
        config.workspace_default_root = root.to_string_lossy().into_owned();
        let db = Db::open_in_memory().expect("real sqlite");
        let state = AppState::new(db.clone(), config);
        let startup_epoch = install_test_startup_epoch(&state, &db).await;
        let _ = state.tools();
        let runtime = state.agent_runtime().expect("production Agent runtime");
        let session = db
            .create_session("test-model", &state.config.workspace_default_root)
            .await
            .expect("create durable session");
        let parent_run_id = uuid::Uuid::new_v4().to_string();
        db.start_root_run_with_budget_at_epoch(
            &parent_run_id,
            &session.id,
            Some("query"),
            "test-model",
            &test_root_budget(),
            startup_epoch,
        )
        .await
        .expect("parent run");
        let submission = ChildTaskSubmission::attached(
            &session.id,
            &parent_run_id,
            &parent_run_id,
            "observability-tool",
            "read a real local file",
            "read the fixture",
            "test-model",
            &state.config.workspace_default_root,
        );
        let receipt = runtime
            .tasks
            .submit_child(submission, move |_| async move {
                match tokio::fs::read_to_string(input).await {
                    Ok(content) => TaskExecutionResult::Complete(content),
                    Err(error) => TaskExecutionResult::Failed {
                        message: error.to_string(),
                        code: "READ_FAILED".to_owned(),
                    },
                }
            })
            .await
            .expect("submit production task");
        for _ in 0..100 {
            let record = db
                .find_runtime_task_by_id(&receipt.task.id)
                .await
                .expect("query task")
                .expect("durable task");
            if record.status == zk_db::TaskStatus::Succeeded {
                let result = db
                    .read_task_result(&record.id, None, 0, zk_db::INLINE_RESULT_LIMIT)
                    .await
                    .expect("read result")
                    .expect("immutable result");
                assert_eq!(result.content, "durable task output\n");
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            db.find_runtime_task_by_id(&receipt.task.id)
                .await
                .expect("query terminal task")
                .expect("terminal task")
                .status,
            zk_db::TaskStatus::Succeeded
        );

        state
            .coordinator
            .create_swarm("observed-real-swarm", 1, &session.id)
            .expect("create production Swarm");

        let jsonl = root.join(".zk/observability-events.jsonl");
        let mut events = Vec::<serde_json::Value>::new();
        for _ in 0..100 {
            let contents = std::fs::read_to_string(&jsonl).unwrap_or_default();
            if !contents.is_empty() && !contents.ends_with('\n') {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
            events = contents
                .lines()
                .map(|line| serde_json::from_str(line).expect("parse complete JSONL line"))
                .collect();
            let has_task = events.iter().any(|event| event["domain"] == "task");
            let has_swarm = events.iter().any(|event| event["domain"] == "swarm");
            if has_task && has_swarm {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(events.iter().any(|event| {
            event["domain"] == "task" && event["action"] == "complete" && event["outcome"] == "ok"
        }));
        assert!(events.iter().any(|event| {
            event["domain"] == "swarm"
                && event["action"] == "create"
                && event["outcome"] == "created"
        }));
        let health = state.observability.health();
        assert!(health.accepted >= 4);
        assert_eq!(health.dropped, 0);
        assert_eq!(health.write_failures, 0);

        drop(runtime);
        drop(state);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn agent_model_aliases_resolve_before_child_execution() {
        let providers = zk_llm::ProviderRegistry::new().with_default_model("configured-default");
        assert_eq!(
            resolve_agent_model(&providers, Some("premium"), None, "parent-model")
                .expect("premium alias"),
            "configured-default"
        );
        assert_eq!(
            resolve_agent_model(&providers, Some("inherit"), None, "parent-model")
                .expect("inherit alias"),
            "parent-model"
        );
        assert_eq!(
            resolve_agent_model(&providers, None, Some("explore"), "parent-model")
                .expect("implicit inheritance"),
            "parent-model"
        );
    }

    #[test]
    fn needs_attention_child_is_returned_as_an_immediate_stable_error() {
        let snapshot = TaskSnapshot {
            task_id: uuid::Uuid::new_v4().to_string(),
            session_id: uuid::Uuid::new_v4().to_string(),
            parent_task_id: Some(uuid::Uuid::new_v4().to_string()),
            run_id: Some(uuid::Uuid::new_v4().to_string()),
            status: "needsAttention".to_owned(),
            reason: Some("durable terminal commit failed".to_owned()),
            description: None,
            output: None,
            error: None,
            result_version: None,
            partial: false,
            result_ref: None,
            cleanup_status: "confirmed".to_owned(),
            usage_summary: serde_json::json!({"complete": false}),
            wait_expired: false,
            created_at: 0,
            child_count: 0,
        };
        assert!(snapshot_requires_attention(&snapshot));
        let error = needs_attention_error(&snapshot);
        assert_eq!(error.code, "AGENT_TASK_NEEDS_ATTENTION");
        assert_eq!(error.message, "durable terminal commit failed");
        assert!(!error.retryable);
    }
}
