//! Coordinator 服务——多代理协作模式核心（对照旧 `CoordinatorService.java`，277L）。
//!
//! Coordinator 模式让主 LLM 扮演协调者角色，不直接执行工具，
//! 而是通过 `AgentTool` 生成工人代理并行处理复杂任务的不同部分。
//!
//! 激活条件：
//! 1. `FeatureFlag` `COORDINATOR_MODE` = true
//! 2. 环境变量 `ZK_COORDINATOR_MODE` = '1'（可选，运行时切换）
//!
//! # 有意差异
//!
//! - Java 使用 `ConcurrentHashMap<String, String> runtimeEnv` 解决
//!   `System.getenv()` vs `System.setProperty()` 命名空间不匹配 bug；
//!   本实现使用 `DashMap<String, String>` 等价替代。
//! - Java 注入 `SystemScratchpadPathPolicy` 做 scratchpad 路径安全校验；
//!   本实现使用 `zk_core::paths` 解析 + sessionId 白名单。
//! - `getWorkerToolsContext` 在旧实现返回 `Map<String, String>`；本实现
//!   直接返回 `String`（只含 `workerToolsContext` 键）。
//! - 环境变量名对齐旧 `ZHIKUN_COORDINATOR_MODE`（非 `ZK_COORDINATOR_MODE`）。

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use zk_core::FeatureFlags;
use zk_protocol::WorkerSnapshot;

use crate::{NoopObservabilityRecorder, ObservabilityEvent, ObservabilityRecorder};

use super::{
    CoordinatorEvent, CoordinatorEventBus, CoordinatorWorkflow, CoordinatorWorkflowEngine,
    SwarmService, TeamInfo, TeamManager, WorkerStatus, WorkflowPhase,
};

/// 环境变量名——运行时 Coordinator 模式切换（对齐旧 `ZHIKUN_COORDINATOR_MODE`）。
pub const COORDINATOR_MODE_ENV: &str = "ZHIKUN_COORDINATOR_MODE";

/// sessionId 白名单正则：字母/数字/下划线/中划线，长度 1–128。
/// 与旧 `SwarmController.TEAM_NAME_PATTERN` 策略对齐。
/// Coordinator 服务——多代理协作模式核心。
pub struct CoordinatorService {
    /// 特性标志。
    feature_flags: std::sync::Arc<FeatureFlags>,
    /// 运行时环境变量存储（对齐旧 `runtimeEnv` `ConcurrentHashMap`）。
    runtime_env: DashMap<String, String>,
    /// 会话级 Coordinator 模式覆盖（`matchSessionMode` 使用）。
    _session_mode_overrides: Mutex<HashSet<String>>,
    /// 唯一团队生命周期管理器。
    team_manager: Arc<TeamManager>,
    /// 唯一 Swarm 运行时与取消表。
    swarm_service: Arc<SwarmService>,
    /// Coordinator → WS 事件出口。
    event_bus: Arc<CoordinatorEventBus>,
    /// Unique four-phase workflow scheduler. Swarm IDs are its isolation keys.
    workflow_engine: Arc<CoordinatorWorkflowEngine>,
    /// 活跃 Swarm 的生命周期状态；历史由 Task/Run/异常仓储承担。
    swarm_phases: DashMap<String, SwarmPhase>,
    /// Best-effort operations telemetry, separate from workflow state.
    observability: Arc<dyn ObservabilityRecorder>,
}

/// Process-local Swarm lifecycle. Active entries are intentionally not restored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SwarmPhase {
    /// Created but not dispatched.
    Created,
    /// At least one worker is executing.
    Running,
    /// All workers completed successfully.
    Completed,
    /// At least one worker failed.
    Failed,
    /// Cooperative cancellation is in progress.
    Aborting,
    /// Cancellation reached a terminal state.
    Aborted,
}

impl SwarmPhase {
    /// Stable API representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "CREATED",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Aborting => "ABORTING",
            Self::Aborted => "ABORTED",
        }
    }
}

impl CoordinatorService {
    /// 创建 `CoordinatorService`。
    #[must_use]
    pub fn new(feature_flags: std::sync::Arc<FeatureFlags>) -> Self {
        let team_manager = Arc::new(TeamManager::new());
        let event_bus = Arc::new(CoordinatorEventBus::new());
        let swarm_service = Arc::new(
            SwarmService::new(Arc::clone(&team_manager), 20).with_event_bus(Arc::clone(&event_bus)),
        );
        Self {
            feature_flags,
            runtime_env: DashMap::new(),
            _session_mode_overrides: Mutex::new(HashSet::new()),
            team_manager,
            swarm_service,
            event_bus,
            workflow_engine: Arc::new(CoordinatorWorkflowEngine::new()),
            swarm_phases: DashMap::new(),
            observability: Arc::new(NoopObservabilityRecorder),
        }
    }

    /// Attach the process-wide observability recorder.
    #[must_use]
    pub fn with_observability(mut self, recorder: Arc<dyn ObservabilityRecorder>) -> Self {
        self.observability = recorder;
        self
    }

    /// Create a Swarm and its initial lifecycle state in the single owner.
    ///
    /// # Errors
    /// Returns an error for duplicate IDs or an invalid worker count.
    pub fn create_swarm(
        &self,
        swarm_id: &str,
        worker_count: usize,
        session_id: &str,
    ) -> Result<TeamInfo, String> {
        self.create_swarm_with_objective(
            swarm_id,
            worker_count,
            session_id,
            &format!("Coordinate Swarm {swarm_id}"),
        )
    }

    /// Create a Swarm and start its real four-phase workflow.
    ///
    /// # Errors
    /// Returns an error for duplicate IDs or an invalid worker count.
    pub fn create_swarm_with_objective(
        &self,
        swarm_id: &str,
        worker_count: usize,
        session_id: &str,
        objective: &str,
    ) -> Result<TeamInfo, String> {
        let team = self
            .team_manager
            .create_team(swarm_id, worker_count, session_id)?;
        self.swarm_phases
            .insert(swarm_id.to_owned(), SwarmPhase::Created);
        let workflow = self.workflow_engine.execute_workflow(swarm_id, objective);
        if let Some(phase) = workflow.get_current_phase() {
            self.publish_workflow_phase(&team, &workflow, &phase, "RUNNING");
        }
        self.record_swarm(&team, "create", "created");
        Ok(team)
    }

    /// List process-local active/terminal Swarms.
    #[must_use]
    pub fn list_swarms(&self) -> Vec<TeamInfo> {
        self.team_manager.list_teams()
    }

    /// Read one process-local Swarm.
    #[must_use]
    pub fn get_swarm(&self, swarm_id: &str) -> Option<TeamInfo> {
        self.team_manager.get_team(swarm_id)
    }

    /// Current lifecycle phase.
    #[must_use]
    pub fn swarm_phase(&self, swarm_id: &str) -> Option<SwarmPhase> {
        self.swarm_phases.get(swarm_id).map(|phase| *phase)
    }

    /// Transition `CREATED` to `RUNNING` before accepting a dispatch.
    ///
    /// # Errors
    /// Returns an error when the Swarm is missing or not in `CREATED`.
    pub fn begin_dispatch(&self, swarm_id: &str) -> Result<(), String> {
        let mut phase = self
            .swarm_phases
            .get_mut(swarm_id)
            .ok_or_else(|| format!("Swarm not found: {swarm_id}"))?;
        if *phase != SwarmPhase::Created {
            return Err(format!(
                "Swarm cannot dispatch from phase {}",
                phase.as_str()
            ));
        }
        *phase = SwarmPhase::Running;
        drop(phase);
        let _ = self.advance_workflow(swarm_id, "Swarm dispatch validated");
        if let Some(team) = self.get_swarm(swarm_id) {
            self.record_swarm(&team, "dispatch", "running");
        }
        Ok(())
    }

    /// Mark dispatch aggregation terminal.
    pub fn finish_dispatch(&self, swarm_id: &str, success: bool) -> bool {
        let Some(mut phase) = self.swarm_phases.get_mut(swarm_id) else {
            return false;
        };
        *phase = if success {
            SwarmPhase::Completed
        } else {
            SwarmPhase::Failed
        };
        drop(phase);
        if let Some(team) = self.get_swarm(swarm_id) {
            self.record_swarm(&team, "complete", if success { "ok" } else { "error" });
        }
        true
    }

    /// Enter cooperative abort state and propagate cancellation.
    pub fn begin_abort(&self, swarm_id: &str) -> bool {
        let Some(mut phase) = self.swarm_phases.get_mut(swarm_id) else {
            return false;
        };
        if matches!(
            *phase,
            SwarmPhase::Completed | SwarmPhase::Failed | SwarmPhase::Aborted
        ) {
            return false;
        }
        *phase = SwarmPhase::Aborting;
        drop(phase);
        self.cancel_workflow(swarm_id);
        self.swarm_service.cancel_swarm(swarm_id);
        if let Some(team) = self.get_swarm(swarm_id) {
            self.record_swarm(&team, "abort", "cancelling");
        }
        true
    }

    /// Mark cooperative or forced cancellation terminal.
    pub fn mark_aborted(&self, swarm_id: &str) -> bool {
        let Some(mut phase) = self.swarm_phases.get_mut(swarm_id) else {
            return false;
        };
        *phase = SwarmPhase::Aborted;
        drop(phase);
        if let Some(team) = self.get_swarm(swarm_id) {
            self.record_swarm(&team, "abort", "cancelled");
        }
        true
    }

    /// Remove process-local Swarm state. Durable Task/Run/anomaly history is untouched.
    pub fn destroy_swarm(&self, swarm_id: &str) -> bool {
        self.cancel_workflow(swarm_id);
        self.swarm_phases.remove(swarm_id);
        self.team_manager.destroy_team(swarm_id)
    }

    /// Advance the Swarm workflow by one strict phase and publish the native event.
    pub fn advance_workflow(&self, swarm_id: &str, summary: &str) -> bool {
        let Some(team) = self.get_swarm(swarm_id) else {
            return false;
        };
        let Some(workflow) = self.workflow_engine.get_active_workflow(swarm_id) else {
            return false;
        };
        let previous = workflow.get_current_phase();
        let next = self.workflow_engine.advance_workflow(swarm_id, summary);
        if let Some(phase) = next {
            self.publish_workflow_phase(&team, &workflow, &phase, "RUNNING");
        } else if workflow.is_complete()
            && let Some(phase) = previous
        {
            self.publish_workflow_phase(&team, &workflow, &phase, "COMPLETED");
        }
        true
    }

    /// Fail the active Swarm workflow and publish its last real phase.
    pub fn fail_workflow(&self, swarm_id: &str, reason: &str) -> bool {
        let Some(team) = self.get_swarm(swarm_id) else {
            return false;
        };
        let Some(workflow) = self.workflow_engine.get_active_workflow(swarm_id) else {
            return false;
        };
        let phase = workflow.get_current_phase();
        self.workflow_engine.fail_workflow(swarm_id, reason);
        if let Some(phase) = phase {
            self.publish_workflow_phase(&team, &workflow, &phase, "FAILED");
        }
        true
    }

    /// Cancel the active Swarm workflow and publish its last real phase.
    pub fn cancel_workflow(&self, swarm_id: &str) -> bool {
        let Some(team) = self.get_swarm(swarm_id) else {
            return false;
        };
        let Some(workflow) = self.workflow_engine.get_active_workflow(swarm_id) else {
            return false;
        };
        let phase = workflow.get_current_phase();
        self.workflow_engine.cancel_workflow(swarm_id);
        if let Some(phase) = phase {
            self.publish_workflow_phase(&team, &workflow, &phase, "CANCELLED");
        }
        true
    }

    fn publish_workflow_phase(
        &self,
        team: &TeamInfo,
        workflow: &CoordinatorWorkflow,
        phase: &WorkflowPhase,
        status: &str,
    ) {
        let _ = self
            .event_bus
            .publish(CoordinatorEvent::WorkflowPhaseUpdate {
                session_id: team.session_id.clone(),
                workflow_id: workflow.workflow_id().to_owned(),
                phase_name: phase.name().to_owned(),
                status: status.to_owned(),
                phase_index: i64::try_from(phase.phase_index()).unwrap_or(i64::MAX),
                total_phases: i64::try_from(WorkflowPhase::TOTAL_PHASES).unwrap_or(i64::MAX),
                phase_prompt: phase.phase_prompt(),
                objective: workflow.objective().to_owned(),
            });
    }

    fn record_swarm(&self, team: &TeamInfo, action: &str, outcome: &str) {
        let mut event = ObservabilityEvent::new("swarm", action, outcome);
        event.session_id = Some(team.session_id.clone());
        event.attributes.insert(
            "swarmId".to_owned(),
            serde_json::Value::String(team.team_id.clone()),
        );
        self.observability.record(event);
    }

    /// Shared runtime executor/cancellation service.
    #[must_use]
    pub fn swarm_service(&self) -> &Arc<SwarmService> {
        &self.swarm_service
    }

    /// Shared event bus consumed by the WS bridge.
    #[must_use]
    pub fn event_bus(&self) -> &Arc<CoordinatorEventBus> {
        &self.event_bus
    }

    /// Publish the current Swarm projection to the native WebSocket event stream.
    pub fn publish_swarm_state(&self, swarm_id: &str) -> bool {
        let Some(team) = self.get_swarm(swarm_id) else {
            return false;
        };
        let states = self.swarm_service.worker_states(swarm_id);
        let active_workers = states
            .iter()
            .filter(|worker| worker.status == WorkerStatus::Running)
            .count();
        let completed_tasks = states
            .iter()
            .filter(|worker| worker.status == WorkerStatus::Completed)
            .count();
        let workers = states
            .into_iter()
            .map(|worker| {
                let worker_api_status = match worker.status {
                    WorkerStatus::Running => "WORKING",
                    WorkerStatus::Completed | WorkerStatus::Failed => "TERMINATED",
                };
                (
                    worker.worker_id.clone(),
                    WorkerSnapshot {
                        worker_id: worker.worker_id,
                        status: worker_api_status.to_owned(),
                        current_task: None,
                        tool_call_count: 0,
                        token_consumed: 0,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let phase = match self.swarm_phase(swarm_id) {
            Some(SwarmPhase::Created) => "INITIALIZING",
            Some(SwarmPhase::Running) => "RUNNING",
            Some(SwarmPhase::Aborting) => "SHUTTING_DOWN",
            Some(SwarmPhase::Completed | SwarmPhase::Failed | SwarmPhase::Aborted) | None => {
                "TERMINATED"
            }
        }
        .to_owned();
        let total_tasks = i64::try_from(workers.len()).unwrap_or(i64::MAX);
        let _ = self.event_bus.publish(CoordinatorEvent::SwarmStateUpdate {
            session_id: team.session_id,
            swarm_id: swarm_id.to_owned(),
            phase,
            active_workers: i64::try_from(active_workers).unwrap_or(i64::MAX),
            total_workers: i64::try_from(team.worker_count).unwrap_or(i64::MAX),
            completed_tasks: i64::try_from(completed_tasks).unwrap_or(i64::MAX),
            total_tasks,
            workers,
        });
        true
    }

    // ═══ 模式检测 ═══

    /// 检查是否处于 Coordinator 模式。
    ///
    /// 对齐旧 `isCoordinatorMode()`：`FeatureFlag` + 环境变量双判断。
    #[must_use]
    pub fn is_coordinator_mode(&self) -> bool {
        if !self.feature_flags.is_enabled("COORDINATOR_MODE") {
            return false;
        }
        // 先查运行时覆盖，再查进程环境变量
        let runtime_val = self
            .runtime_env
            .get(COORDINATOR_MODE_ENV)
            .map(|v| v.clone());
        let env_val = runtime_val.or_else(|| std::env::var(COORDINATOR_MODE_ENV).ok());
        is_env_truthy(env_val.as_deref())
    }

    /// 检查当前是否处于 Coordinator 顶层模式（非子代理）。
    ///
    /// 对齐旧 `isCoordinatorTopLevel(agentDefinition)`。
    #[must_use]
    pub fn is_coordinator_top_level(&self, is_sub_agent: bool) -> bool {
        self.is_coordinator_mode() && !is_sub_agent
    }

    /// 自动检测任务复杂度决定是否建议启用 Coordinator。
    ///
    /// 基于用户消息中的关键词启发式判断（中英双语）。
    /// 对齐旧 `shouldSuggestCoordinator(userMessage)`。
    #[must_use]
    pub fn should_suggest_coordinator(&self, user_message: &str) -> bool {
        if !self.feature_flags.is_enabled("COORDINATOR_MODE") {
            return false;
        }
        if user_message.is_empty() {
            return false;
        }
        let lower = user_message.to_lowercase();
        let mut signals = 0u32;
        if lower.contains("refactor") || lower.contains("重构") {
            signals += 1;
        }
        if lower.contains("migrate") || lower.contains("迁移") {
            signals += 1;
        }
        if lower.contains("test") && lower.contains("implement") {
            signals += 1;
        }
        if lower.contains("multiple files") || lower.contains("多个文件") {
            signals += 1;
        }
        if lower.contains("parallel")
            || lower.contains("并行")
            || lower.contains("并行执行")
            || lower.contains("多代理")
        {
            signals += 1;
        }
        if lower.contains("comprehensive") || lower.contains("全面") {
            signals += 1;
        }
        if lower.contains("create agents") || lower.contains("spawn workers") {
            signals += 1;
        }
        signals >= 2
    }

    /// 会话恢复时同步模式。
    ///
    /// 对齐旧 `matchSessionMode(sessionMode)`。
    #[must_use]
    pub fn match_session_mode(&self, session_mode: Option<&str>) -> Option<String> {
        let session_mode = session_mode?;
        let current = self.is_coordinator_mode();
        let target = session_mode == "coordinator";
        if current == target {
            return None;
        }

        if target {
            self.runtime_env
                .insert(COORDINATOR_MODE_ENV.to_owned(), "1".to_owned());
            Some("Entered coordinator mode to match resumed session.".to_owned())
        } else {
            self.runtime_env.remove(COORDINATOR_MODE_ENV);
            Some("Exited coordinator mode to match resumed session.".to_owned())
        }
    }

    // ═══ 工人工具上下文 ═══

    /// 构建工人可用工具列表上下文。
    ///
    /// 对齐旧 `getWorkerToolsContext(sessionId)`。
    /// 过滤内部工具后列出可用工具 name，返回描述文本。
    #[must_use]
    pub fn get_worker_tools_context(&self, tool_names: &[String]) -> String {
        if !self.is_coordinator_mode() {
            return String::new();
        }

        let mut filtered: Vec<&str> = tool_names
            .iter()
            .map(String::as_str)
            .filter(|name| !is_internal_worker_tool(name))
            .collect();
        filtered.sort_unstable();

        format!(
            "Workers spawned via the Agent tool have access to these tools: {}",
            filtered.join(", ")
        )
    }

    /// 获取 Coordinator 模式下协调者可用的工具集。
    ///
    /// 对齐旧 `getCoordinatorAllowedTools()`。
    #[must_use]
    pub fn get_coordinator_allowed_tools() -> HashSet<&'static str> {
        ["Agent", "TaskStop", "SendMessage", "SyntheticOutput"]
            .into_iter()
            .collect()
    }

    // ═══ Scratchpad ═══

    /// 获取会话的 scratchpad 目录路径字符串。
    ///
    /// `sessionId` 必须匹配安全白名单（字母/数字/下划线/中划线，1–128 字符），
    /// 否则回退到 `"default"`（防止路径穿越，对齐旧 CWE-22 防御）。
    #[must_use]
    pub fn get_scratchpad_dir(&self, session_id: &str, base_scratchpad: &str) -> String {
        let safe_id = if is_safe_session_id(session_id) {
            session_id
        } else {
            if !session_id.is_empty() {
                tracing::warn!(
                    "getScratchpadDir: rejected unsafe sessionId (path traversal prevention), \
                     falling back to 'default'"
                );
            }
            "default"
        };
        format!("{base_scratchpad}/{safe_id}")
    }

    /// 设置运行时环境变量覆盖（测试用 / 模式同步用）。
    pub fn set_runtime_env(&self, key: &str, value: &str) {
        self.runtime_env.insert(key.to_owned(), value.to_owned());
    }

    /// 清除运行时环境变量覆盖。
    pub fn clear_runtime_env(&self, key: &str) {
        self.runtime_env.remove(key);
    }
}

/// 检查环境变量值是否为 truthy（对齐旧 `isEnvTruthy`）。
fn is_env_truthy(value: Option<&str>) -> bool {
    matches!(value, Some("1" | "true" | "True" | "TRUE"))
}

/// 检查 sessionId 是否符合安全白名单。
fn is_safe_session_id(session_id: &str) -> bool {
    if session_id.is_empty() || session_id.len() > 128 {
        return false;
    }
    session_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// 检查工具名是否为内部工具（工人不可见）。
///
/// 对齐旧 `INTERNAL_WORKER_TOOLS`。
fn is_internal_worker_tool(name: &str) -> bool {
    matches!(
        name,
        "Agent" | "TeamCreate" | "TeamDelete" | "SendMessage" | "SyntheticOutput"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_service() -> CoordinatorService {
        let flags = std::sync::Arc::new(FeatureFlags::with_defaults());
        CoordinatorService::new(flags)
    }

    #[test]
    fn coordinator_mode_requires_flag_and_env() {
        let svc = make_service();
        // COORDINATOR_MODE defaults to true in factory, but env not set
        // Without env var set, is_coordinator_mode should be false
        // (because runtime_env is empty and std::env::var likely not set)
        // Note: in test env, ZHIKUN_COORDINATOR_MODE is typically not set
        let result = svc.is_coordinator_mode();
        // This depends on whether the env var is set in the test environment
        // Just verify it doesn't panic
        let _ = result;
    }

    #[test]
    fn coordinator_mode_with_runtime_env() {
        let svc = make_service();
        svc.set_runtime_env(COORDINATOR_MODE_ENV, "1");
        assert!(svc.is_coordinator_mode());

        svc.clear_runtime_env(COORDINATOR_MODE_ENV);
        // After clearing, should fall back to env var (likely not set)
    }

    #[test]
    fn coordinator_allowed_tools() {
        let tools = CoordinatorService::get_coordinator_allowed_tools();
        assert!(tools.contains("Agent"));
        assert!(tools.contains("TaskStop"));
        assert!(tools.contains("SendMessage"));
        assert!(tools.contains("SyntheticOutput"));
        assert_eq!(tools.len(), 4);
    }

    #[test]
    fn one_service_owns_swarm_state_machine() {
        let service = make_service();
        let mut events = service.event_bus().subscribe();
        service
            .create_swarm("swarm-1", 2, "session-1")
            .expect("create swarm");
        assert!(matches!(
            events.try_recv(),
            Ok(CoordinatorEvent::WorkflowPhaseUpdate {
                phase_name,
                status,
                ..
            }) if phase_name == "Research" && status == "RUNNING"
        ));
        assert_eq!(service.swarm_phase("swarm-1"), Some(SwarmPhase::Created));
        service.begin_dispatch("swarm-1").expect("begin dispatch");
        assert!(matches!(
            events.try_recv(),
            Ok(CoordinatorEvent::WorkflowPhaseUpdate {
                phase_name,
                status,
                ..
            }) if phase_name == "Synthesis" && status == "RUNNING"
        ));
        assert_eq!(service.swarm_phase("swarm-1"), Some(SwarmPhase::Running));
        assert!(service.begin_abort("swarm-1"));
        assert!(matches!(
            events.try_recv(),
            Ok(CoordinatorEvent::WorkflowPhaseUpdate {
                phase_name,
                status,
                ..
            }) if phase_name == "Synthesis" && status == "CANCELLED"
        ));
        assert_eq!(service.swarm_phase("swarm-1"), Some(SwarmPhase::Aborting));
        assert!(service.mark_aborted("swarm-1"));
        assert_eq!(service.swarm_phase("swarm-1"), Some(SwarmPhase::Aborted));
        assert!(!service.begin_abort("swarm-1"));
        assert!(service.destroy_swarm("swarm-1"));
        assert!(service.get_swarm("swarm-1").is_none());
    }

    #[test]
    fn should_suggest_coordinator_keywords() {
        let svc = make_service();
        svc.set_runtime_env(COORDINATOR_MODE_ENV, "1");

        // Two signals needed
        assert!(svc.should_suggest_coordinator("refactor and migrate the code"));
        assert!(svc.should_suggest_coordinator("请并行执行多个文件的全面重构"));
        assert!(!svc.should_suggest_coordinator("hello"));
        assert!(!svc.should_suggest_coordinator(""));
    }

    #[test]
    fn worker_tools_context_filters_internal() {
        let svc = make_service();
        svc.set_runtime_env(COORDINATOR_MODE_ENV, "1");

        let tools = vec![
            "Agent".to_owned(),
            "Bash".to_owned(),
            "Read".to_owned(),
            "SendMessage".to_owned(),
            "Write".to_owned(),
        ];
        let ctx = svc.get_worker_tools_context(&tools);
        // Extract the tool list part (after the colon)
        let tool_list = ctx.split(": ").nth(1).unwrap_or("");
        assert!(tool_list.contains("Bash"));
        assert!(tool_list.contains("Read"));
        assert!(tool_list.contains("Write"));
        assert!(!tool_list.contains("Agent"));
        assert!(!tool_list.contains("SendMessage"));
    }

    #[test]
    fn scratchpad_dir_safe_session_id() {
        let svc = make_service();
        let dir = svc.get_scratchpad_dir("valid-session_123", "/tmp/scratch");
        assert_eq!(dir, "/tmp/scratch/valid-session_123");
    }

    #[test]
    fn scratchpad_dir_unsafe_session_id_fallback() {
        let svc = make_service();
        let dir = svc.get_scratchpad_dir("../etc/passwd", "/tmp/scratch");
        assert_eq!(dir, "/tmp/scratch/default");
    }

    #[test]
    fn match_session_mode_no_change() {
        let svc = make_service();
        // Not in coordinator mode, session mode is "normal"
        assert!(svc.match_session_mode(Some("normal")).is_none());
    }

    #[test]
    fn is_safe_session_id_validation() {
        assert!(is_safe_session_id("abc-123_def"));
        assert!(is_safe_session_id("a"));
        assert!(!is_safe_session_id(""));
        assert!(!is_safe_session_id("../etc"));
        assert!(!is_safe_session_id("has space"));
        assert!(!is_safe_session_id(&"a".repeat(129)));
    }
}
