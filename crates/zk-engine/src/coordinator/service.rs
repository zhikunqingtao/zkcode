//! Coordinator 服务——多代理协作模式核心。
//!
//! 顶层 Coordinator 模式由组合根在启动期解析并冻结。服务构造时将显式
//! `ZHIKUN_COORDINATOR_MODE=1` 与 `COORDINATOR_MODE` feature flag 求交；此后既不
//! 读取进程环境，也不接受会话恢复或关键词启发式改写模式。Swarm 生命周期仍由
//! 本服务的进程内状态机管理，但不会反向改变顶层对话模式。

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use dashmap::DashMap;
use zk_core::{FeatureFlags, feature_flags};
use zk_protocol::WorkerSnapshot;

use crate::{NoopObservabilityRecorder, ObservabilityEvent, ObservabilityRecorder};

use super::{
    CoordinatorEvent, CoordinatorEventBus, CoordinatorWorkflow, CoordinatorWorkflowEngine,
    SwarmService, TeamInfo, TeamManager, WorkerStatus, WorkflowPhase,
};

/// 顶层 Coordinator 模式的启动期环境变量名。
pub const COORDINATOR_MODE_ENV: &str = "ZHIKUN_COORDINATOR_MODE";

/// sessionId 白名单正则：字母/数字/下划线/中划线，长度 1–128。
/// 与旧 `SwarmController.TEAM_NAME_PATTERN` 策略对齐。
/// Coordinator 服务——多代理协作模式核心。
pub struct CoordinatorService {
    /// 启动期冻结的有效模式：显式进程配置与 feature flag 必须同时开启。
    coordinator_mode_enabled: bool,
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

#[allow(
    clippy::must_use_candidate,
    reason = "legacy Swarm command methods return best-effort booleans that callers may intentionally ignore; Swarm API changes are outside this release"
)]
impl CoordinatorService {
    /// 创建 `CoordinatorService`。
    #[must_use]
    pub fn new(feature_flags: &FeatureFlags, coordinator_mode_enabled: bool) -> Self {
        let team_manager = Arc::new(TeamManager::new());
        let event_bus = Arc::new(CoordinatorEventBus::new());
        let swarm_service = Arc::new(
            SwarmService::new(Arc::clone(&team_manager), 20).with_event_bus(Arc::clone(&event_bus)),
        );
        Self {
            coordinator_mode_enabled: coordinator_mode_enabled
                && feature_flags.is_enabled(feature_flags::COORDINATOR_MODE),
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
    /// 该值由构造时的显式进程配置与 `COORDINATOR_MODE` feature flag 共同决定，
    /// 并在服务生命周期内保持不变。
    #[must_use]
    pub const fn is_coordinator_mode(&self) -> bool {
        self.coordinator_mode_enabled
    }

    /// 检查当前是否处于 Coordinator 顶层模式（非子代理）。
    ///
    /// 对齐旧 `isCoordinatorTopLevel(agentDefinition)`。
    #[must_use]
    pub fn is_coordinator_top_level(&self, is_sub_agent: bool) -> bool {
        self.is_coordinator_mode() && !is_sub_agent
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
        ["Agent", "TaskOutput", "TaskStop", "SendMessage"]
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
        "Agent"
            | "TaskCreate"
            | "TaskGet"
            | "TaskList"
            | "TaskOutput"
            | "TaskStop"
            | "TaskUpdate"
            | "TeamCreate"
            | "TeamDelete"
            | "SendMessage"
            | "SyntheticOutput"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_service(enabled: bool) -> CoordinatorService {
        let flags = std::sync::Arc::new(FeatureFlags::with_defaults());
        CoordinatorService::new(flags.as_ref(), enabled)
    }

    #[test]
    fn coordinator_mode_requires_explicit_startup_opt_in() {
        assert!(!make_service(false).is_coordinator_mode());
        assert!(make_service(true).is_coordinator_mode());
    }

    #[test]
    fn coordinator_mode_freezes_feature_flag_at_startup() {
        let flags = std::sync::Arc::new(FeatureFlags::with_defaults());
        let svc = CoordinatorService::new(flags.as_ref(), true);
        flags.set_value(
            feature_flags::COORDINATOR_MODE,
            zk_core::feature_flags::FlagValue::Bool(false),
        );
        assert!(svc.is_coordinator_mode());

        let disabled_flags = std::sync::Arc::new(FeatureFlags::with_defaults());
        disabled_flags.set_value(
            feature_flags::COORDINATOR_MODE,
            zk_core::feature_flags::FlagValue::Bool(false),
        );
        let disabled = CoordinatorService::new(disabled_flags.as_ref(), true);
        assert!(!disabled.is_coordinator_mode());
    }

    #[test]
    fn coordinator_allowed_tools() {
        let tools = CoordinatorService::get_coordinator_allowed_tools();
        assert!(tools.contains("Agent"));
        assert!(tools.contains("TaskOutput"));
        assert!(tools.contains("TaskStop"));
        assert!(tools.contains("SendMessage"));
        assert_eq!(tools.len(), 4);
    }

    #[test]
    fn one_service_owns_swarm_state_machine() {
        let service = make_service(false);
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
    fn worker_tools_context_filters_internal() {
        let svc = make_service(true);

        let tools = vec![
            "Agent".to_owned(),
            "Bash".to_owned(),
            "Read".to_owned(),
            "SendMessage".to_owned(),
            "TaskOutput".to_owned(),
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
        assert!(!tool_list.contains("TaskOutput"));
    }

    #[test]
    fn scratchpad_dir_safe_session_id() {
        let svc = make_service(false);
        let dir = svc.get_scratchpad_dir("valid-session_123", "/tmp/scratch");
        assert_eq!(dir, "/tmp/scratch/valid-session_123");
    }

    #[test]
    fn scratchpad_dir_unsafe_session_id_fallback() {
        let svc = make_service(false);
        let dir = svc.get_scratchpad_dir("../etc/passwd", "/tmp/scratch");
        assert_eq!(dir, "/tmp/scratch/default");
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
