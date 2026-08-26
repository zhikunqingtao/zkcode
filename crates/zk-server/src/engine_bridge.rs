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
    AgentTimeoutConfig, BackgroundAgentTracker, ChildExecutionContext, IsolationMode,
    RealSubAgentEngineFactory, SubAgentExecutor, SystemGitCommandRunner, WorktreeManager,
    build_sub_agent_registry_with_policy,
};
use zk_engine::concurrency::AgentConcurrencyController;
use zk_engine::task::{TaskCoordinator, TaskStatus};
use zk_engine::{ConversationService, CoordinatorEvent, Engine, LlmSummarizer, MessageSink};
use zk_llm::ChatProvider;
use zk_protocol::{ClientMessage, ServerMessage};
use zk_tools::{
    AgentInvocation, AgentTool, AgentToolBackend, AskUserQuestionTool, BashTool, ConfigTool,
    CronCreateTool, CronDeleteTool, CronListTool, CronTaskService, CtxInspectTool, EditFileTool,
    EnterPlanModeTool, ExitPlanModeTool, GitDiffTool, GitLogTool, GitStatusTool, GlobTool,
    GrepTool, ListDirectoryTool, MemoryTool, ModelCatalog, MonitorTool, NotebookEditTool, REPLTool,
    ReadFileTool, ReplManager, SendMessageBackend, SendMessageInvocation, SendMessageTool,
    SleepTool, SnipTool, StaticToolCatalog, SyntheticOutputTool, TaskCoordinatorPort,
    TaskCreateTool, TaskGetTool, TaskInvocation, TaskListTool, TaskOutputTool, TaskSnapshot,
    TaskStopTool, TaskUpdateTool, TerminalCaptureTool, TodoWriteTool, ToolDescriptor, ToolRegistry,
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
}

impl MessageSink for HubSink {
    fn push<'a>(&'a self, session_id: &'a str, message: ServerMessage) -> BoxFuture<'a, ()> {
        Box::pin(self.hub.push(session_id, message))
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
            }),
            tools,
            admission,
        )
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
    executor: Arc<SubAgentExecutor>,
    db: zk_db::Db,
    providers: Arc<zk_llm::SwappableProvider>,
    background_agents: Arc<BackgroundAgentTracker>,
    worktree_enabled: bool,
}

impl AgentToolBackend for AgentBackendBridge {
    fn execute_agent(
        &self,
        invocation: AgentInvocation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> BoxFuture<'_, (String, Option<String>, Option<String>)> {
        Box::pin(async move {
            if invocation.isolation == "worktree" && !self.worktree_enabled {
                return (
                    "failed".to_owned(),
                    Some(
                        "FEATURE_NOT_READY: Worktree isolation has not passed real Git acceptance"
                            .to_owned(),
                    ),
                    None,
                );
            }
            let inherited_model = match validate_agent_invocation(&self.db, &invocation).await {
                Ok(model) => model,
                Err(message) => return ("failed".to_owned(), Some(message), None),
            };
            let model = match resolve_agent_model(
                &self.providers.load(),
                invocation.model_override.as_deref(),
                invocation.agent_type.as_deref(),
                &inherited_model,
            ) {
                Ok(model) => model,
                Err(message) => return ("failed".to_owned(), Some(message), None),
            };
            let agent_id = format!("agent-{}", &uuid::Uuid::new_v4().to_string()[..8]);
            let iso = IsolationMode::parse(&invocation.isolation);
            let request = AgentRequest::new(
                agent_id.clone(),
                invocation.prompt,
                invocation.agent_type,
                Some(model),
                iso,
                invocation.run_in_background,
            );
            let context = ChildExecutionContext {
                parent_session_id: invocation.parent_session_id,
                parent_run_id: invocation.parent_run_id,
                working_directory: invocation.working_directory,
                tool_use_id: invocation.tool_use_id,
                allowed_tools: invocation.allowed_tools,
            };
            if request.run_in_background {
                let executor = Arc::clone(&self.executor);
                let tracker = Arc::clone(&self.background_agents);
                let tracked_agent_id = agent_id.clone();
                tracker.register(
                    agent_id.clone(),
                    context.parent_session_id.clone(),
                    invocation.description,
                    Some(format!("agent:{agent_id}")),
                );
                tokio::spawn(async move {
                    let result = executor
                        .execute_sync_with_cancel(&request, &context, cancel)
                        .await;
                    if result.status == AgentStatus::Completed {
                        tracker.mark_completed(&tracked_agent_id, &result);
                    } else {
                        tracker.mark_failed(
                            &tracked_agent_id,
                            result.result.as_deref().unwrap_or("child execution failed"),
                        );
                    }
                });
                return (
                    "async_launched".to_owned(),
                    Some(agent_id.clone()),
                    Some(format!("agent:{agent_id}")),
                );
            }
            let result = self
                .executor
                .execute_sync_with_cancel(&request, &context, cancel)
                .await;
            (
                result.status.as_str().to_owned(),
                result.result,
                result.output_file,
            )
        })
    }
}

/// Persist collaboration messages before live delivery and project them onto
/// the native coordinator WebSocket stream.
struct SendMessageBackendBridge {
    router: Arc<AgentMailboxRouter>,
    db: zk_db::Db,
    event_bus: Arc<zk_engine::CoordinatorEventBus>,
}

impl SendMessageBackend for SendMessageBackendBridge {
    fn send_message(&self, invocation: SendMessageInvocation) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            if !self.router.has_active_agent(&invocation.target_agent_id) {
                return Err(format!("AGENT_NOT_FOUND: {}", invocation.target_agent_id));
            }
            let message_id = uuid::Uuid::new_v4().to_string();
            let from_id = invocation.parent_session_id.clone();
            let queued = serde_json::json!({
                "messageId": message_id,
                "targetAgentId": invocation.target_agent_id,
                "fromId": from_id,
                "content": invocation.message,
            });
            self.db
                .append_run_event(
                    &invocation.parent_run_id,
                    "teammate_message_queued",
                    Some(&invocation.tool_use_id),
                    &queued,
                )
                .await
                .map_err(|_| {
                    "SEND_MESSAGE_PERSIST_FAILED: queue event was not stored".to_owned()
                })?;

            let delivery = AgentMailboxMessage {
                message_id: message_id.clone(),
                parent_run_id: invocation.parent_run_id.clone(),
                from_id: from_id.clone(),
                content: invocation.message.clone(),
            };
            if let Err(error) = self
                .router
                .send_message(&invocation.target_agent_id, delivery)
            {
                let rejected = serde_json::json!({
                    "messageId": message_id,
                    "targetAgentId": invocation.target_agent_id,
                    "reason": "target_terminal",
                });
                let _ = self
                    .db
                    .append_run_event(
                        &invocation.parent_run_id,
                        "teammate_message_rejected",
                        Some(&invocation.tool_use_id),
                        &rejected,
                    )
                    .await;
                return Err(error);
            }

            let content = invocation.message.chars().take(512).collect();
            let _ = self.event_bus.publish(CoordinatorEvent::TeammateMessage {
                session_id: invocation.parent_session_id,
                from_id,
                content,
            });
            Ok(())
        })
    }
}

async fn validate_agent_invocation(
    db: &zk_db::Db,
    invocation: &AgentInvocation,
) -> Result<String, String> {
    let session = db
        .get_session(&invocation.parent_session_id)
        .await
        .map_err(|_| "AGENT_CONTEXT_INVALID: parent session lookup failed".to_owned())?
        .ok_or_else(|| "AGENT_CONTEXT_INVALID: parent session not found".to_owned())?;
    if session.status != "active" {
        return Err("AGENT_CONTEXT_INVALID: parent session is not active".to_owned());
    }
    let run = db
        .find_run_by_id(&invocation.parent_run_id)
        .await
        .map_err(|_| "AGENT_CONTEXT_INVALID: parent run lookup failed".to_owned())?
        .ok_or_else(|| "AGENT_CONTEXT_INVALID: parent run not found".to_owned())?;
    if run.session_id != invocation.parent_session_id {
        return Err("AGENT_CONTEXT_INVALID: parent run/session mismatch".to_owned());
    }
    if !matches!(run.status.as_str(), "RUNNING" | "WAITING") {
        return Err("AGENT_CONTEXT_INVALID: parent run is terminal".to_owned());
    }
    let authorized = std::fs::canonicalize(&session.working_dir)
        .map_err(|_| "AGENT_CONTEXT_INVALID: authorized workspace is unavailable".to_owned())?;
    let requested = std::fs::canonicalize(&invocation.working_directory)
        .map_err(|_| "AGENT_CONTEXT_INVALID: invocation workspace is unavailable".to_owned())?;
    if requested != authorized {
        return Err("AGENT_CONTEXT_INVALID: workspace does not match parent session".to_owned());
    }
    Ok(run.model)
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

/// `TaskCoordinatorPort` 的生产实现——桥接到 `TaskCoordinator`。
struct TaskCoordinatorBridge {
    coordinator: Arc<TaskCoordinator>,
    executor: Arc<SubAgentExecutor>,
    db: zk_db::Db,
    providers: Arc<zk_llm::SwappableProvider>,
}

/// Single production Agent runtime shared by Agent tools, Task tools, and Swarm dispatch.
pub(crate) struct AgentRuntime {
    /// Production child-agent executor.
    pub(crate) executor: Arc<SubAgentExecutor>,
    /// DB-backed task coordinator.
    pub(crate) tasks: Arc<TaskCoordinator>,
}

impl TaskCoordinatorPort for TaskCoordinatorBridge {
    fn submit_task(&self, invocation: TaskInvocation) -> BoxFuture<'_, Result<(), String>> {
        let coordinator = Arc::clone(&self.coordinator);
        let executor = Arc::clone(&self.executor);
        let db = self.db.clone();
        let providers = Arc::clone(&self.providers);
        Box::pin(async move {
            let validation = AgentInvocation {
                prompt: invocation.prompt.clone(),
                description: invocation.description.clone(),
                agent_type: Some(invocation.task_type.clone()),
                model_override: None,
                isolation: "none".to_owned(),
                run_in_background: true,
                parent_session_id: invocation.session_id.clone(),
                parent_run_id: invocation.parent_run_id.clone(),
                working_directory: invocation.working_directory.clone(),
                tool_use_id: invocation.tool_use_id.clone(),
                allowed_tools: None,
            };
            let inherited_model = validate_agent_invocation(&db, &validation).await?;
            let model = resolve_agent_model(
                &providers.load(),
                None,
                Some(&invocation.task_type),
                &inherited_model,
            )?;
            let request = AgentRequest::new(
                invocation.task_id.clone(),
                invocation.prompt,
                Some(invocation.task_type),
                Some(model),
                IsolationMode::None,
                false,
            );
            let context = ChildExecutionContext {
                parent_session_id: invocation.session_id.clone(),
                parent_run_id: invocation.parent_run_id,
                working_directory: invocation.working_directory,
                tool_use_id: invocation.tool_use_id,
                allowed_tools: None,
            };
            coordinator
                .submit_with_cancel(
                    invocation.task_id,
                    invocation.session_id,
                    invocation.description,
                    move |cancel| async move {
                        let context = ChildExecutionContext {
                            parent_session_id: context.parent_session_id,
                            parent_run_id: context.parent_run_id,
                            working_directory: context.working_directory,
                            tool_use_id: context.tool_use_id,
                            allowed_tools: context.allowed_tools,
                        };
                        let result = executor
                            .execute_sync_with_cancel(&request, &context, cancel)
                            .await;
                        match result.status {
                            AgentStatus::Completed | AgentStatus::MaxTurns => {
                                Ok(result.result.unwrap_or_default())
                            }
                            _ => Err(result.result.unwrap_or_else(|| "Task failed".to_owned())),
                        }
                    },
                )
                .await
                .map(|_| ())
        })
    }

    fn cancel_task(&self, task_id: String) -> BoxFuture<'_, bool> {
        Box::pin(async move { self.coordinator.cancel_task(&task_id).await })
    }

    fn get_task(&self, task_id: String) -> BoxFuture<'_, Option<TaskSnapshot>> {
        Box::pin(async move {
            self.coordinator
                .get_task(&task_id)
                .await
                .ok()
                .flatten()
                .map(task_snapshot)
        })
    }

    fn list_tasks(
        &self,
        session_id: String,
        filter_status: Option<String>,
    ) -> BoxFuture<'_, Vec<TaskSnapshot>> {
        Box::pin(async move {
            let filter = filter_status.as_deref().and_then(TaskStatus::parse);
            self.coordinator
                .list_tasks(&session_id, filter)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(task_snapshot)
                .collect()
        })
    }

    fn update_task(
        &self,
        task_id: String,
        status: Option<String>,
        output: Option<String>,
    ) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let Some(record) = self.coordinator.get_task(&task_id).await? else {
                return Err("Task not found".to_owned());
            };
            let status = status
                .as_deref()
                .and_then(TaskStatus::parse)
                .map_or(record.status, |status| status.as_str().to_owned());
            self.db
                .update_task_result(
                    &task_id,
                    &status,
                    output.as_deref().or(record.output.as_deref()),
                    record.error.as_deref(),
                    record.progress,
                )
                .await
                .map_err(|error| format!("Task store unavailable: {error}"))
        })
    }
}

fn task_snapshot(record: zk_db::TaskRecord) -> TaskSnapshot {
    let created_at = record.created_at_millis();
    TaskSnapshot {
        task_id: record.id,
        session_id: record.session_id,
        status: record.status,
        description: record.description,
        output: record.output,
        error: record.error,
        created_at,
        child_count: 0,
    }
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
/// Batch 5 追加 1 件（旧 `memdir/MemoryTool`）：
///
/// - `Memory`：经 `zk_tools::MemoryStore` 端口注入 [`AppState::memdir`]
///   （`~/.zk/MEMORY.md`）；与 `/api/memory` 域端点共用同一存储实例。
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
    registry.register(Arc::new(MemoryTool::with_store(state.memdir.clone())));
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
        registry.register(Arc::new(SendMessageTool::new(Arc::new(
            SendMessageBackendBridge {
                router: Arc::clone(&mailbox_router),
                db: state.db.clone(),
                event_bus: Arc::clone(state.coordinator.event_bus()),
            },
        ))));
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
            }),
            child_admission,
            state.costs.clone(),
            state.file_history.clone(),
            state.hooks.clone(),
            Arc::clone(&state.observability),
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
            mailbox_router,
        ));
        let coordinator = Arc::new(
            TaskCoordinator::new(
                state.db.clone(),
                Arc::new(HubSink {
                    hub: state.hub.clone(),
                }),
            )
            .with_observability(Arc::clone(&state.observability)),
        );
        let agent_runtime = Arc::new(AgentRuntime {
            executor: Arc::clone(&executor),
            tasks: Arc::clone(&coordinator),
        });
        state.set_agent_runtime(agent_runtime);
        let agent_backend: Arc<dyn AgentToolBackend> = Arc::new(AgentBackendBridge {
            executor: Arc::clone(&executor),
            db: state.db.clone(),
            providers: Arc::clone(&state.providers),
            background_agents: Arc::new(BackgroundAgentTracker::new()),
            worktree_enabled: state.config.worktree_enabled,
        });
        registry.register(Arc::new(AgentTool::new(Arc::clone(&agent_backend))));
        skill_fork_backend = Some(agent_backend);
        let port: Arc<dyn TaskCoordinatorPort> = Arc::new(TaskCoordinatorBridge {
            coordinator: Arc::clone(&coordinator),
            executor: Arc::clone(&executor),
            db: state.db.clone(),
            providers: Arc::clone(&state.providers),
        });
        registry.register(Arc::new(TaskCreateTool::new(Arc::clone(&port))));
        registry.register(Arc::new(TaskUpdateTool::new(Arc::clone(&port))));
        registry.register(Arc::new(TaskListTool::new(Arc::clone(&port))));
        registry.register(Arc::new(TaskGetTool::new(Arc::clone(&port))));
        registry.register(Arc::new(TaskOutputTool::new(Arc::clone(&port))));
        registry.register(Arc::new(TaskStopTool::new(port)));
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
    // Cron 三件的旧 `isEnabled()` 一律 `featureFlags.isEnabled("AGENT_TRIGGERS")`
    // （出厂 false）→ **注册期**门：flag 关则三件不进注册表，模型看不见。
    // 台账构造一次、`Arc` 三处共享——否则 `CronList` / `CronDelete` 看不到
    // `CronCreate` 建的任务。
    if state
        .feature_flags
        .is_enabled(zk_core::feature_flags::AGENT_TRIGGERS)
    {
        let cron = Arc::new(CronTaskService::new(std::path::Path::new(
            &state.config.workspace_default_root,
        )));
        registry.register(Arc::new(CronCreateTool::new(Arc::clone(&cron))));
        registry.register(Arc::new(CronListTool::new(Arc::clone(&cron))));
        registry.register(Arc::new(CronDeleteTool::new(cron)));
    }
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

fn refresh_tool_search_catalog(registry: &ToolRegistry) {
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
        SendMessageBackendBridge, build_tool_registry, build_tool_registry_with_search_endpoint,
        resolve_agent_model, select_search_backend, sync_python_tool_registry,
    };
    use crate::config::Config;
    use crate::python::CapabilityStatus;
    use crate::state::AppState;
    use futures::future::BoxFuture;
    use sha2::{Digest, Sha256};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use zk_core::feature_flags::FlagValue;
    use zk_db::Db;
    use zk_engine::{
        AgentConcurrencyController, AgentMailboxMessage, AgentMailboxRouter, AgentRequest,
        AgentTimeoutConfig, ChildExecutionContext, IsolationMode, SubAgentEngineFactory,
        SubAgentExecutor, SystemGitCommandRunner, WorktreeManager,
    };
    use zk_tools::{
        SearchRequest, SendMessageBackend, SendMessageInvocation, WebFetchError, WebFetchPort,
        WebFetchRequest, WebFetchResponse,
    };

    struct SearchFixtureFetch;

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
            "36cb708a3e9b113886acffd7799cfd7d7cb1da23e65045b415e4e608f9ff934c"
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
        let notebook = workspace.join("book.ipynb");
        let original = r#"{"cells":[{"cell_type":"markdown","metadata":{},"source":["old"]}],"metadata":{},"nbformat":4,"nbformat_minor":5}"#;
        std::fs::write(&notebook, original).expect("notebook");
        let session = db
            .create_session("model", workspace.to_str().expect("utf8 path"))
            .await
            .expect("session");
        let state = AppState::new(db.clone(), Config::test_config());
        let tool = build_tool_registry(&state)
            .get("NotebookEdit")
            .expect("NotebookEdit");
        let (tx, _rx) = mpsc::unbounded_channel();
        let context = zk_tools::ToolContext::new(CancellationToken::new(), tx)
            .with_working_dir(&workspace)
            .with_session_id(&session.id)
            .with_tool_use_id("notebook-call-1");
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

    /// `AGENT_TRIGGERS` 关 → Cron 三件不进注册表（旧 `isEnabled()` 注册期门）；
    /// 打开 → 三件到位且共享同一张台账（Create 建的任务 List 看得见）。
    #[test]
    fn agent_triggers_flag_gates_the_cron_trio() {
        let state = AppState::for_tests();
        let registry = build_tool_registry(&state);
        assert_eq!(
            registry.names().len(),
            33,
            "flag off → 33 frozen base tools"
        );
        for name in ["CronCreate", "CronList", "CronDelete"] {
            assert!(registry.get(name).is_none(), "{name} must stay hidden");
        }

        state.feature_flags.set_value(
            zk_core::feature_flags::AGENT_TRIGGERS,
            FlagValue::Bool(true),
        );
        let registry = build_tool_registry(&state);
        assert_eq!(registry.names().len(), 36, "flag on → +3 cron tools");
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
    async fn send_message_bridge_persists_before_delivery_and_emits_native_event() {
        let db = Db::open_in_memory().expect("db");
        db.create_session_with_id("parent-session", "test-model", "/tmp")
            .await
            .expect("parent session");
        db.start_run("parent-run", "parent-session", None, None, "test-model")
            .await
            .expect("parent run");
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
            "target-agent",
            "wait",
            None,
            Some("test-model".to_owned()),
            IsolationMode::None,
            false,
        );
        let context = ChildExecutionContext {
            parent_session_id: "parent-session".to_owned(),
            parent_run_id: "parent-run".to_owned(),
            working_directory: std::path::PathBuf::from("/tmp"),
            tool_use_id: "spawn-tool".to_owned(),
            allowed_tools: None,
        };
        let child = tokio::spawn({
            let executor = Arc::clone(&executor);
            async move { executor.execute_sync(&request, &context).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !router.has_active_agent("target-agent") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("target becomes active");

        let event_bus = Arc::new(zk_engine::CoordinatorEventBus::new());
        let mut events = event_bus.subscribe();
        let backend = SendMessageBackendBridge {
            router,
            db: db.clone(),
            event_bus,
        };
        backend
            .send_message(SendMessageInvocation {
                target_agent_id: "target-agent".to_owned(),
                message: "continue".to_owned(),
                parent_session_id: "parent-session".to_owned(),
                parent_run_id: "parent-run".to_owned(),
                tool_use_id: "send-tool".to_owned(),
            })
            .await
            .expect("message queued and delivered");

        let durable = db
            .get_run_events("parent-run", 0, 10)
            .await
            .expect("events");
        assert_eq!(durable.len(), 2, "run_started + queued event");
        assert_eq!(durable[1].event_type, "teammate_message_queued");
        assert!(durable[1].event_data.contains("target-agent"));
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
        let _ = state.tools();
        let runtime = state.agent_runtime().expect("production Agent runtime");
        let session = db
            .create_session("test-model", &state.config.workspace_default_root)
            .await
            .expect("create durable session");

        runtime
            .tasks
            .submit(
                "observed-real-task",
                &session.id,
                "read a real local file",
                async move {
                    tokio::fs::read_to_string(input)
                        .await
                        .map_err(|error| error.to_string())
                },
            )
            .await
            .expect("submit production task");
        for _ in 0..100 {
            let record = db
                .find_task_by_id("observed-real-task")
                .await
                .expect("query task")
                .expect("durable task");
            if record.status == "COMPLETED" {
                assert_eq!(record.output.as_deref(), Some("durable task output\n"));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            db.find_task_by_id("observed-real-task")
                .await
                .expect("query terminal task")
                .expect("terminal task")
                .status,
            "COMPLETED"
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
}
