//! 团队生命周期管理 + 进程内 Worker 后端——对照旧 `TeamManager.java`（138L）
//! + `InProcessBackend.java`（128L）。
//!
//! # `TeamManager`
//!
//! 创建、查询、销毁多 Agent 团队。每个团队包含一组 Worker Agent，由
//! Coordinator 调度。
//!
//! # `InProcessBackend`
//!
//! 进程内 Worker 后端——使用 `tokio::JoinSet` + `Semaphore` 并发执行
//! 子代理任务（对齐旧 Virtual Thread 池并发模型）。
//!
//! # 有意差异
//!
//! - Java 使用 Virtual Thread `ExecutorService`；本实现取 `tokio::JoinSet`
//!   + `Semaphore`（tokio 原生并发原语，等价语义）。
//! - Java `AgentRequest` / `AgentResult` 为 record 类型；本实现复用
//!   `crate::agent::types` 中的定义。
//! - 旧 `dispatchTasks` 依赖 `ToolUseContext`；本实现简化为通用接口。

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::coordinator::event_bus::{CoordinatorEvent, CoordinatorEventBus};

/// 每个团队最大 Worker 数（对齐旧 `workerCount < 1 || workerCount > 20`）。
pub const MAX_WORKERS_PER_TEAM: usize = 20;

/// 单个 Worker 最大执行时间（对齐旧 `WORKER_TIMEOUT_SECONDS = 300`）。
pub const WORKER_TIMEOUT_SECONDS: u64 = 300;

// ═══════════════════════════════════════════════════════════════
// TeamManager
// ═══════════════════════════════════════════════════════════════

/// 团队信息（对齐旧 `TeamManager.TeamInfo` record）。
#[derive(Clone, Debug)]
pub struct TeamInfo {
    /// 团队名称（唯一标识）。
    pub team_id: String,
    /// Worker 数量。
    pub worker_count: usize,
    /// 关联会话 ID。
    pub session_id: String,
    /// 创建时刻。
    pub created_at: Instant,
}

/// 任务规格（对齐旧 `TeamManager.TaskSpec` record）。
#[derive(Clone, Debug)]
pub struct TaskSpec {
    /// 任务 prompt。
    pub prompt: String,
    /// 代理类型（可选）。
    pub agent_type: Option<String>,
    /// 模型（可选）。
    pub model: Option<String>,
}

/// 代理执行结果（简化版，对齐旧 `AgentResult`）。
#[derive(Clone, Debug)]
pub struct AgentResult {
    /// 执行状态（`completed` / `failed` / `timeout`）。
    pub status: String,
    /// 结果文本。
    pub result: String,
    /// 原始 prompt。
    pub prompt: String,
}

/// 代理请求（简化版，对齐旧 `AgentRequest`）。
#[derive(Clone, Debug)]
pub struct AgentRequest {
    /// Agent ID。
    pub agent_id: String,
    /// 任务 prompt。
    pub prompt: String,
    /// 代理类型。
    pub agent_type: Option<String>,
    /// 模型。
    pub model: Option<String>,
}

/// 团队生命周期管理器。
///
/// 创建、查询、销毁多 Agent 团队。
pub struct TeamManager {
    /// 团队存储（`team_id` → `TeamInfo`）。
    teams: DashMap<String, TeamInfo>,
}

impl TeamManager {
    /// 创建 `TeamManager`。
    #[must_use]
    pub fn new() -> Self {
        Self {
            teams: DashMap::new(),
        }
    }

    /// 创建一个新团队。
    ///
    /// 对齐旧 `createTeam(teamName, workerCount, sessionId)`。
    ///
    /// # Errors
    ///
    /// - 团队已存在时返回 `Err`。
    /// - Worker 数量不在 1~20 范围时返回 `Err`。
    pub fn create_team(
        &self,
        team_id: &str,
        worker_count: usize,
        session_id: &str,
    ) -> Result<TeamInfo, String> {
        if self.teams.contains_key(team_id) {
            return Err(format!("Team already exists: {team_id}"));
        }
        if !(1..=MAX_WORKERS_PER_TEAM).contains(&worker_count) {
            return Err(format!(
                "Worker count must be between 1 and {MAX_WORKERS_PER_TEAM}, got: {worker_count}"
            ));
        }

        let info = TeamInfo {
            team_id: team_id.to_owned(),
            worker_count,
            session_id: session_id.to_owned(),
            created_at: Instant::now(),
        };
        self.teams.insert(team_id.to_owned(), info.clone());

        tracing::info!(
            team = team_id,
            workers = worker_count,
            session = session_id,
            "Team created"
        );
        Ok(info)
    }

    /// 获取团队信息。
    #[must_use]
    pub fn get_team(&self, team_id: &str) -> Option<TeamInfo> {
        self.teams.get(team_id).map(|e| e.value().clone())
    }

    /// 列出所有团队。
    #[must_use]
    pub fn list_teams(&self) -> Vec<TeamInfo> {
        self.teams.iter().map(|e| e.value().clone()).collect()
    }

    /// 销毁团队。
    pub fn destroy_team(&self, team_id: &str) -> bool {
        let removed = self.teams.remove(team_id).is_some();
        if removed {
            tracing::info!(team = team_id, "Team destroyed");
        }
        removed
    }

    /// 清除所有团队。
    pub fn destroy_all(&self) {
        self.teams.clear();
        tracing::info!("All teams destroyed");
    }
}

impl Default for TeamManager {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════
// InProcessBackend
// ═══════════════════════════════════════════════════════════════

/// 进程内 Worker 后端——使用 `tokio::JoinSet` + `Semaphore` 并发执行子代理任务。
///
/// 对齐旧 `InProcessBackend.java` 的 Virtual Thread 并发模型。
pub struct InProcessBackend {
    /// 并发信号量（限制同时执行的 worker 数）。
    semaphore: Arc<Semaphore>,
    /// 最大并发数。
    max_concurrency: usize,
}

impl InProcessBackend {
    /// 创建 `InProcessBackend`。
    ///
    /// `max_concurrency` 为 0 表示不限制。
    #[must_use]
    pub fn new(max_concurrency: usize) -> Self {
        let permits = if max_concurrency > 0 {
            max_concurrency
        } else {
            // 不限制时使用一个较大的值
            256
        };
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            max_concurrency,
        }
    }

    /// 并行执行多个子代理请求。
    ///
    /// 对齐旧 `executeParallel(requests, parentContext, maxConcurrency)`。
    ///
    /// `executor` 回调模拟每个 worker 的执行逻辑——实际接入时替换为
    /// `SubAgentExecutor.executeSync`。
    ///
    /// # Errors
    ///
    /// 单个 worker 失败不影响其他 worker；失败结果以 `AgentResult` 返回。
    pub async fn execute_parallel<F, Fut>(
        &self,
        requests: Vec<AgentRequest>,
        executor: F,
    ) -> Vec<AgentResult>
    where
        F: Fn(AgentRequest) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<String, String>> + Send,
    {
        if requests.is_empty() {
            return Vec::new();
        }

        let request_count = requests.len();
        tracing::info!(
            tasks = request_count,
            concurrency = self.max_concurrency,
            "InProcessBackend: executing tasks"
        );

        let executor = Arc::new(executor);
        let semaphore = Arc::clone(&self.semaphore);
        let timeout = Duration::from_secs(WORKER_TIMEOUT_SECONDS);

        let mut join_set: JoinSet<AgentResult> = JoinSet::new();

        for request in requests {
            let sem = Arc::clone(&semaphore);
            let exec = Arc::clone(&executor);
            join_set.spawn(async move {
                let Ok(_permit) = sem.acquire().await else {
                    return AgentResult {
                        status: "failed".to_owned(),
                        result: "Semaphore closed".to_owned(),
                        prompt: request.prompt.clone(),
                    };
                };

                let agent_id = request.agent_id.clone();
                let prompt = request.prompt.clone();

                match tokio::time::timeout(timeout, exec(request)).await {
                    Ok(Ok(result)) => {
                        tracing::debug!(agent = %agent_id, "Worker completed");
                        AgentResult {
                            status: "completed".to_owned(),
                            result,
                            prompt,
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::error!(agent = %agent_id, error = %e, "Worker failed");
                        AgentResult {
                            status: "failed".to_owned(),
                            result: format!("Worker execution failed: {e}"),
                            prompt,
                        }
                    }
                    Err(_) => {
                        tracing::warn!(
                            agent = %agent_id,
                            timeout_secs = WORKER_TIMEOUT_SECONDS,
                            "Worker timed out"
                        );
                        AgentResult {
                            status: "timeout".to_owned(),
                            result: format!(
                                "Worker timed out after {WORKER_TIMEOUT_SECONDS} seconds"
                            ),
                            prompt,
                        }
                    }
                }
            });
        }

        let mut results = Vec::with_capacity(request_count);
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(agent_result) => results.push(agent_result),
                Err(e) => {
                    tracing::error!(error = %e, "JoinSet task panicked");
                    results.push(AgentResult {
                        status: "failed".to_owned(),
                        result: format!("Worker panicked: {e}"),
                        prompt: String::new(),
                    });
                }
            }
        }

        tracing::info!(
            completed = results.len(),
            "InProcessBackend: all tasks completed"
        );
        results
    }

    /// 顺序执行多个子代理请求（测试用或低并发场景）。
    pub async fn execute_sequential<F, Fut>(
        &self,
        requests: Vec<AgentRequest>,
        executor: F,
    ) -> Vec<AgentResult>
    where
        F: Fn(AgentRequest) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<String, String>> + Send,
    {
        let mut results = Vec::with_capacity(requests.len());
        for request in requests {
            let prompt = request.prompt.clone();
            let agent_id = request.agent_id.clone();
            match executor(request).await {
                Ok(result) => results.push(AgentResult {
                    status: "completed".to_owned(),
                    result,
                    prompt,
                }),
                Err(e) => {
                    tracing::error!(agent = %agent_id, error = %e, "Sequential worker failed");
                    results.push(AgentResult {
                        status: "failed".to_owned(),
                        result: format!("Worker failed: {e}"),
                        prompt,
                    });
                }
            }
        }
        results
    }
}

// ═══════════════════════════════════════════════════════════════
// SwarmService（Batch 8C Step 4）
// ═══════════════════════════════════════════════════════════════

/// 单个 Worker 结果最大截取长度（对照旧 `ResultAggregator.MAX_RESULT_CHARS`）。
pub const MAX_RESULT_CHARS: usize = 50_000;

/// 聚合摘要最大总长度（对照旧 `ResultAggregator.MAX_AGGREGATE_CHARS`）。
pub const MAX_AGGREGATE_CHARS: usize = 200_000;

/// Worker 运行状态（对照旧 `SwarmState.WorkerStatus`，任务指定三态）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerStatus {
    /// 执行中。
    Running,
    /// 成功完成。
    Completed,
    /// 失败（含超时 / 取消）。
    Failed,
}

/// 单个 Worker 运行时状态（对照旧 `SwarmState.WorkerState` record）。
#[derive(Clone, Debug)]
pub struct WorkerState {
    /// Worker（子代理）ID。
    pub worker_id: String,
    /// 当前状态。
    pub status: WorkerStatus,
    /// 进度（0.0~1.0，未知时 `None`）。
    pub progress: Option<f32>,
    /// 最终输出（完成前 `None`）。
    pub output: Option<String>,
    /// 启动时刻。
    pub started_at: Instant,
    /// 完成时刻（未完成时 `None`）。
    pub completed_at: Option<Instant>,
}

/// Swarm 聚合结果（对照旧 `ResultAggregator.aggregate` 产物 + 计数）。
#[derive(Clone, Debug)]
pub struct SwarmResults {
    /// Swarm ID（= 团队名）。
    pub swarm_id: String,
    /// 各 Worker 原始结果（按完成序）。
    pub results: Vec<AgentResult>,
    /// 聚合后的结构化摘要文本。
    pub aggregated: String,
    /// 成功计数。
    pub success_count: usize,
    /// 失败计数。
    pub failure_count: usize,
}

/// Swarm 编排服务——并发派发 Worker、追踪运行时状态、聚合结果、优雅取消
/// （对照旧 `SwarmService.java` + `SwarmState.java` + `ResultAggregator.java`）。
///
/// # 与 `InProcessBackend` 的分工
///
/// `InProcessBackend` 是无状态的一次性并行执行原语（fire-and-collect）；
/// `SwarmService` 在其之上叠加**运行时状态追踪**（[`WorkerState`]）、
/// **事件广播**（[`CoordinatorEventBus`]）、**取消令牌树**与**异步结果
/// 收集**（[`Self::dispatch`] 派发 → [`Self::collect_results`] 等待聚合）。
pub struct SwarmService {
    /// 团队生命周期管理器。
    team_manager: Arc<TeamManager>,
    /// 事件总线（可选；未装配时 agent 生命周期事件不广播）。
    event_bus: Option<Arc<CoordinatorEventBus>>,
    /// 全局并发信号量。
    semaphore: Arc<Semaphore>,
    /// 各 Swarm 的 Worker 状态表（`swarm_id` → (`worker_id` → 状态)）。
    worker_states: DashMap<String, Arc<DashMap<String, WorkerState>>>,
    /// 各 Swarm 的待收集任务集（`swarm_id` → `JoinSet`）。
    pending: DashMap<String, Arc<Mutex<JoinSet<AgentResult>>>>,
    /// 各 Swarm 的取消令牌（`swarm_id` → token）。
    cancel_tokens: DashMap<String, CancellationToken>,
    /// 各 Swarm 的 Worker 定向取消令牌。
    worker_cancel_tokens: DashMap<String, Arc<DashMap<String, CancellationToken>>>,
    /// 已进入 shutdown、拒绝新任务的 Swarm。
    shutting_down: DashMap<String, ()>,
    /// 单 Worker 超时（默认 [`WORKER_TIMEOUT_SECONDS`]）。
    worker_timeout: Duration,
}

impl SwarmService {
    /// 创建 `SwarmService`。`max_concurrency` 为 0 时取 256（近似不限制）。
    #[must_use]
    pub fn new(team_manager: Arc<TeamManager>, max_concurrency: usize) -> Self {
        let permits = if max_concurrency > 0 {
            max_concurrency
        } else {
            256
        };
        Self {
            team_manager,
            event_bus: None,
            semaphore: Arc::new(Semaphore::new(permits)),
            worker_states: DashMap::new(),
            pending: DashMap::new(),
            cancel_tokens: DashMap::new(),
            worker_cancel_tokens: DashMap::new(),
            shutting_down: DashMap::new(),
            worker_timeout: Duration::from_secs(WORKER_TIMEOUT_SECONDS),
        }
    }

    /// 装配事件总线（链式）。
    #[must_use]
    pub fn with_event_bus(mut self, event_bus: Arc<CoordinatorEventBus>) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    /// 覆盖单 Worker 超时（链式，默认 5min）。
    #[must_use]
    pub fn with_worker_timeout(mut self, timeout: Duration) -> Self {
        self.worker_timeout = timeout;
        self
    }

    /// 关联的 `TeamManager`。
    #[must_use]
    pub fn team_manager(&self) -> &Arc<TeamManager> {
        &self.team_manager
    }

    /// 获取（或惰性创建）指定 Swarm 的取消令牌。
    #[must_use]
    pub fn cancel_token(&self, swarm_id: &str) -> CancellationToken {
        self.cancel_tokens
            .entry(swarm_id.to_owned())
            .or_default()
            .clone()
    }

    /// 取消指定 Swarm 的所有 Worker（令牌传播到子 Engine run loop）。
    pub fn cancel_swarm(&self, swarm_id: &str) {
        if let Some(token) = self.cancel_tokens.get(swarm_id) {
            token.cancel();
            tracing::info!(swarm = swarm_id, "Swarm cancelled");
        }
    }

    /// 只取消目标 Worker，不影响同一 Swarm 的其他 Worker。
    #[must_use]
    pub fn cancel_worker(&self, swarm_id: &str, worker_id: &str) -> bool {
        let Some(tokens) = self.worker_cancel_tokens.get(swarm_id) else {
            return false;
        };
        let Some(token) = tokens.get(worker_id) else {
            return false;
        };
        token.cancel();
        true
    }

    /// Create or reuse the target worker token so external `TaskService` cancellation shares it.
    #[must_use]
    pub fn worker_cancel_token(&self, swarm_id: &str, worker_id: &str) -> CancellationToken {
        let swarm_token = self.cancel_token(swarm_id);
        self.worker_cancel_tokens
            .entry(swarm_id.to_owned())
            .or_default()
            .entry(worker_id.to_owned())
            .or_insert_with(|| swarm_token.child_token())
            .clone()
    }

    /// 停止接收新任务，但允许已派发 Worker 自然完成。
    #[must_use]
    pub fn shutdown_swarm(&self, swarm_id: &str) -> bool {
        if self.team_manager.get_team(swarm_id).is_none() {
            return false;
        }
        self.shutting_down.insert(swarm_id.to_owned(), ());
        true
    }

    /// 强制取消全部 Worker 并终止仍受控的 `JoinHandle`。
    pub async fn force_stop_swarm(&self, swarm_id: &str) -> bool {
        if self.team_manager.get_team(swarm_id).is_none() {
            return false;
        }
        self.shutting_down.insert(swarm_id.to_owned(), ());
        self.cancel_swarm(swarm_id);
        if let Some(pending) = self.pending.get(swarm_id) {
            pending.lock().await.abort_all();
        }
        if let Some(states) = self.worker_states.get(swarm_id) {
            let now = Instant::now();
            for mut state in states.iter_mut() {
                if state.status == WorkerStatus::Running {
                    state.status = WorkerStatus::Failed;
                    state.progress = Some(1.0);
                    state.output = Some("Worker force-stopped".to_owned());
                    state.completed_at = Some(now);
                }
            }
        }
        true
    }

    /// Whether a Swarm has outstanding worker handles.
    #[must_use]
    pub fn has_pending(&self, swarm_id: &str) -> bool {
        self.pending.contains_key(swarm_id)
    }

    /// Whether the Swarm still accepts dispatch requests.
    #[must_use]
    pub fn accepts_dispatch(&self, swarm_id: &str) -> bool {
        !self.shutting_down.contains_key(swarm_id)
    }

    /// 快照指定 Swarm 的全部 Worker 状态。
    #[must_use]
    pub fn worker_states(&self, swarm_id: &str) -> Vec<WorkerState> {
        self.worker_states
            .get(swarm_id)
            .map(|states| states.iter().map(|e| e.value().clone()).collect())
            .unwrap_or_default()
    }

    /// 派发一批 Worker 任务到指定 Swarm（非阻塞——立即返回，结果经
    /// [`Self::collect_results`] 收集）。
    ///
    /// 每个 Worker：注册 `Running` 状态 + 广播 `AgentStarted` → 派生子取消
    /// 令牌 → 信号量限流 → 超时包裹执行 → 更新终态 + 广播
    /// `AgentCompleted` / `AgentFailed`。取消令牌传播到 `executor` 内部的子
    /// Engine run loop（`executor` 须检查其 [`CancellationToken`] 参数）。
    ///
    /// # Errors
    /// Returns an error for a missing/shutting-down Swarm, duplicate workers, or an active dispatch.
    #[allow(clippy::too_many_lines)]
    pub fn dispatch<F, Fut>(
        &self,
        swarm_id: &str,
        session_id: &str,
        tasks: Vec<AgentRequest>,
        executor: F,
    ) -> Result<(), String>
    where
        F: Fn(AgentRequest, CancellationToken) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
    {
        if self.team_manager.get_team(swarm_id).is_none() {
            return Err(format!("Swarm not found: {swarm_id}"));
        }
        if !self.accepts_dispatch(swarm_id) {
            return Err(format!("Swarm is shutting down: {swarm_id}"));
        }
        if self.pending.contains_key(swarm_id) {
            return Err(format!("Swarm already has pending workers: {swarm_id}"));
        }
        let mut unique_worker_ids = std::collections::HashSet::new();
        if tasks
            .iter()
            .any(|task| !unique_worker_ids.insert(task.agent_id.clone()))
        {
            return Err("Duplicate worker id in dispatch".to_owned());
        }
        let swarm_token = self.cancel_token(swarm_id);
        let worker_map = self
            .worker_states
            .entry(swarm_id.to_owned())
            .or_default()
            .clone();
        let executor = Arc::new(executor);
        let worker_timeout = self.worker_timeout;
        let mut join_set: JoinSet<AgentResult> = JoinSet::new();
        let worker_tokens = self
            .worker_cancel_tokens
            .entry(swarm_id.to_owned())
            .or_default()
            .clone();

        for request in tasks {
            let agent_id = request.agent_id.clone();
            let prompt = request.prompt.clone();

            // 注册 Running 状态 + 广播 AgentStarted（派发时刻，同步）。
            worker_map.insert(
                agent_id.clone(),
                WorkerState {
                    worker_id: agent_id.clone(),
                    status: WorkerStatus::Running,
                    progress: Some(0.0),
                    output: None,
                    started_at: Instant::now(),
                    completed_at: None,
                },
            );
            if let Some(bus) = &self.event_bus {
                let _ = bus.publish(CoordinatorEvent::AgentStarted {
                    session_id: session_id.to_owned(),
                    agent_id: agent_id.clone(),
                    prompt: prompt.clone(),
                });
                let _ = bus.publish(CoordinatorEvent::WorkerProgress {
                    session_id: session_id.to_owned(),
                    swarm_id: swarm_id.to_owned(),
                    worker_id: agent_id.clone(),
                    status: "WORKING".to_owned(),
                    current_task: Some(prompt.clone()),
                    progress_percent: Some(0),
                    error_message: None,
                    termination_reason: None,
                });
            }

            let sem = Arc::clone(&self.semaphore);
            let exec = Arc::clone(&executor);
            let child_token = worker_tokens
                .entry(agent_id.clone())
                .or_insert_with(|| swarm_token.child_token())
                .clone();
            let worker_map_task = Arc::clone(&worker_map);
            let worker_tokens_task = Arc::clone(&worker_tokens);
            let event_bus = self.event_bus.clone();
            let session = session_id.to_owned();
            let swarm = swarm_id.to_owned();
            let started_at = Instant::now();

            join_set.spawn(async move {
                let Ok(_permit) = sem.acquire().await else {
                    return AgentResult {
                        status: "failed".to_owned(),
                        result: "Semaphore closed".to_owned(),
                        prompt,
                    };
                };
                let (status, content) = tokio::select! {
                    biased;
                    () = child_token.cancelled() => {
                        ("failed".to_owned(), "Worker cancelled".to_owned())
                    }
                    outcome = tokio::time::timeout(
                        worker_timeout,
                        exec(request, child_token.clone()),
                    ) => match outcome {
                        Ok(Ok(text)) => ("completed".to_owned(), text),
                        Ok(Err(e)) => {
                            ("failed".to_owned(), format!("Worker execution failed: {e}"))
                        }
                        Err(_) => (
                            "timeout".to_owned(),
                            format!("Worker timed out after {} seconds", worker_timeout.as_secs()),
                        ),
                    },
                };

                let worker_status = if status == "completed" {
                    WorkerStatus::Completed
                } else {
                    WorkerStatus::Failed
                };
                worker_map_task.insert(
                    agent_id.clone(),
                    WorkerState {
                        worker_id: agent_id.clone(),
                        status: worker_status,
                        progress: Some(1.0),
                        output: Some(content.clone()),
                        started_at,
                        completed_at: Some(Instant::now()),
                    },
                );
                if let Some(bus) = &event_bus {
                    if worker_status == WorkerStatus::Completed {
                        let _ = bus.publish(CoordinatorEvent::AgentCompleted {
                            session_id: session.clone(),
                            agent_id: agent_id.clone(),
                            result: content.clone(),
                        });
                    } else {
                        let _ = bus.publish(CoordinatorEvent::AgentFailed {
                            session_id: session.clone(),
                            agent_id: agent_id.clone(),
                            error: content.clone(),
                        });
                    }
                    let _ = bus.publish(CoordinatorEvent::WorkerProgress {
                        session_id: session.clone(),
                        swarm_id: swarm,
                        worker_id: agent_id.clone(),
                        status: "TERMINATED".to_owned(),
                        current_task: None,
                        progress_percent: Some(100),
                        error_message: (worker_status == WorkerStatus::Failed)
                            .then(|| content.clone()),
                        termination_reason: Some(if worker_status == WorkerStatus::Completed {
                            "completed".to_owned()
                        } else {
                            "error".to_owned()
                        }),
                    });
                }
                worker_tokens_task.remove(&agent_id);

                AgentResult {
                    status,
                    result: content,
                    prompt,
                }
            });
        }

        self.pending
            .insert(swarm_id.to_owned(), Arc::new(Mutex::new(join_set)));
        tracing::info!(swarm = swarm_id, "Swarm tasks dispatched");
        Ok(())
    }

    /// 等待指定 Swarm 的全部 Worker 完成并聚合结果（对照旧
    /// `ResultAggregator.aggregate`）。
    ///
    /// 无 pending 任务时返回空聚合。收集完成后清理 pending 条目
    /// （`worker_states` 与 `cancel_tokens` 保留供事后查询 / 二次取消）。
    pub async fn collect_results(&self, swarm_id: &str) -> SwarmResults {
        let Some((_, join_set)) = self.pending.remove(swarm_id) else {
            return SwarmResults {
                swarm_id: swarm_id.to_owned(),
                results: Vec::new(),
                aggregated: format!("No results to aggregate for team: {swarm_id}"),
                success_count: 0,
                failure_count: 0,
            };
        };

        let mut results = Vec::new();
        {
            let mut guard = join_set.lock().await;
            while let Some(joined) = guard.join_next().await {
                match joined {
                    Ok(agent_result) => results.push(agent_result),
                    Err(e) => {
                        tracing::error!(swarm = swarm_id, error = %e, "Swarm worker panicked");
                        results.push(AgentResult {
                            status: "failed".to_owned(),
                            result: format!("Worker panicked: {e}"),
                            prompt: String::new(),
                        });
                    }
                }
            }
        }

        let (aggregated, success_count, failure_count) = aggregate_results(swarm_id, &results);
        self.worker_cancel_tokens.remove(swarm_id);
        tracing::info!(
            swarm = swarm_id,
            workers = results.len(),
            success = success_count,
            "Swarm results collected"
        );
        SwarmResults {
            swarm_id: swarm_id.to_owned(),
            results,
            aggregated,
            success_count,
            failure_count,
        }
    }
}

/// 聚合 Worker 结果为结构化摘要（对照旧 `ResultAggregator.aggregate`）。
///
/// 返回 `(聚合文本, 成功数, 失败数)`。空结果返回占位提示。
fn aggregate_results(team_name: &str, results: &[AgentResult]) -> (String, usize, usize) {
    use std::fmt::Write as _;

    if results.is_empty() {
        return (
            format!("No results to aggregate for team: {team_name}"),
            0,
            0,
        );
    }

    let mut sb = String::new();
    let _ = writeln!(sb, "## Team Results: {team_name}\n");

    let mut success_count = 0usize;
    let mut fail_count = 0usize;
    for (i, result) in results.iter().enumerate() {
        let is_success = result.status == "completed";
        if is_success {
            success_count += 1;
        } else {
            fail_count += 1;
        }
        let label = if is_success { "SUCCESS" } else { "PARTIAL" };
        let _ = writeln!(sb, "### Worker {} [{label}]", i + 1);
        let _ = writeln!(sb, "**Task**: {}", truncate_chars(&result.prompt, 200));
        sb.push_str("**Result**:\n");
        let content = if result.result.is_empty() {
            "(no output)"
        } else {
            &result.result
        };
        let _ = writeln!(sb, "{}\n", truncate_chars(content, MAX_RESULT_CHARS));
    }

    sb.push_str("---\n");
    let _ = writeln!(
        sb,
        "**Summary**: {} workers completed ({success_count} success, {fail_count} partial/failed)",
        results.len()
    );

    let aggregated = if sb.chars().count() > MAX_AGGREGATE_CHARS {
        let truncated: String = sb.chars().take(MAX_AGGREGATE_CHARS).collect();
        format!("{truncated}\n...[aggregation truncated]")
    } else {
        sb
    };
    (aggregated, success_count, fail_count)
}

/// 字符安全截断（对照旧 `ResultAggregator.truncate`——按字符而非字节，
/// 避免切断多字节 UTF-8）。
fn truncate_chars(text: &str, max_len: usize) -> String {
    if text.chars().count() <= max_len {
        text.to_owned()
    } else {
        let truncated: String = text.chars().take(max_len).collect();
        format!("{truncated}...[truncated]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_results_empty_returns_placeholder() {
        let (text, s, f) = aggregate_results("team-x", &[]);
        assert!(text.contains("No results to aggregate for team: team-x"));
        assert_eq!(s, 0);
        assert_eq!(f, 0);
    }

    #[test]
    fn aggregate_results_counts_success_and_failure() {
        let results = vec![
            AgentResult {
                status: "completed".into(),
                result: "ok".into(),
                prompt: "p1".into(),
            },
            AgentResult {
                status: "failed".into(),
                result: "boom".into(),
                prompt: "p2".into(),
            },
        ];
        let (text, s, f) = aggregate_results("team-y", &results);
        assert_eq!(s, 1);
        assert_eq!(f, 1);
        assert!(text.contains("## Team Results: team-y"));
        assert!(text.contains("[SUCCESS]"));
        assert!(text.contains("[PARTIAL]"));
        assert!(text.contains("2 workers completed (1 success, 1 partial/failed)"));
    }

    #[test]
    fn truncate_chars_respects_limit() {
        assert_eq!(truncate_chars("hello", 10), "hello");
        assert_eq!(truncate_chars("hello", 3), "hel...[truncated]");
    }

    #[tokio::test]
    async fn swarm_dispatch_and_collect_success() {
        let tm = Arc::new(TeamManager::new());
        tm.create_team("swarm-1", 2, "sess-1").unwrap();
        let swarm = SwarmService::new(Arc::clone(&tm), 4);
        let tasks = vec![
            AgentRequest {
                agent_id: "w1".into(),
                prompt: "task 1".into(),
                agent_type: None,
                model: None,
            },
            AgentRequest {
                agent_id: "w2".into(),
                prompt: "task 2".into(),
                agent_type: None,
                model: None,
            },
        ];
        swarm
            .dispatch("swarm-1", "sess-1", tasks, |req, _cancel| async move {
                Ok(format!("done: {}", req.prompt))
            })
            .expect("dispatch");
        let results = swarm.collect_results("swarm-1").await;
        assert_eq!(results.results.len(), 2);
        assert_eq!(results.success_count, 2);
        assert_eq!(results.failure_count, 0);
        assert!(results.aggregated.contains("2 workers completed"));
    }

    #[tokio::test]
    async fn swarm_worker_failure_is_tracked() {
        let tm = Arc::new(TeamManager::new());
        tm.create_team("swarm-2", 1, "sess-2").unwrap();
        let swarm = SwarmService::new(Arc::clone(&tm), 2);
        let tasks = vec![AgentRequest {
            agent_id: "w1".into(),
            prompt: "will fail".into(),
            agent_type: None,
            model: None,
        }];
        swarm
            .dispatch("swarm-2", "sess-2", tasks, |_req, _cancel| async {
                Err("boom".to_owned())
            })
            .expect("dispatch");
        let results = swarm.collect_results("swarm-2").await;
        assert_eq!(results.failure_count, 1);
        let states = swarm.worker_states("swarm-2");
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].status, WorkerStatus::Failed);
    }

    #[tokio::test]
    async fn swarm_cancel_marks_token() {
        let tm = Arc::new(TeamManager::new());
        let swarm = SwarmService::new(tm, 2);
        let token = swarm.cancel_token("swarm-3");
        assert!(!token.is_cancelled());
        swarm.cancel_swarm("swarm-3");
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn swarm_collect_without_dispatch_is_empty() {
        let tm = Arc::new(TeamManager::new());
        let swarm = SwarmService::new(tm, 2);
        let results = swarm.collect_results("nope").await;
        assert!(results.results.is_empty());
        assert!(results.aggregated.contains("No results to aggregate"));
        assert_eq!(results.success_count, 0);
    }

    #[tokio::test]
    async fn worker_abort_only_cancels_the_target_worker() {
        let tm = Arc::new(TeamManager::new());
        tm.create_team("swarm-directed", 2, "sess").unwrap();
        let swarm = SwarmService::new(tm, 2).with_worker_timeout(Duration::from_secs(2));
        let tasks = ["worker-a", "worker-b"]
            .into_iter()
            .map(|id| AgentRequest {
                agent_id: id.to_owned(),
                prompt: id.to_owned(),
                agent_type: None,
                model: None,
            })
            .collect();
        swarm
            .dispatch(
                "swarm-directed",
                "sess",
                tasks,
                |request, cancel| async move {
                    tokio::select! {
                        () = cancel.cancelled() => Err(format!("{} cancelled", request.agent_id)),
                        () = tokio::time::sleep(Duration::from_millis(80)) => {
                            Ok(format!("{} completed", request.agent_id))
                        }
                    }
                },
            )
            .expect("dispatch");
        assert!(swarm.cancel_worker("swarm-directed", "worker-a"));
        assert!(!swarm.cancel_worker("swarm-directed", "missing"));
        let results = swarm.collect_results("swarm-directed").await;
        assert_eq!(results.success_count, 1);
        assert_eq!(results.failure_count, 1);
        let states = swarm.worker_states("swarm-directed");
        assert_eq!(
            states
                .iter()
                .find(|state| state.worker_id == "worker-b")
                .unwrap()
                .status,
            WorkerStatus::Completed
        );
    }

    #[tokio::test]
    async fn shutdown_rejects_new_dispatch_and_force_stop_clears_workers() {
        let tm = Arc::new(TeamManager::new());
        tm.create_team("swarm-stop", 1, "sess").unwrap();
        let swarm = SwarmService::new(tm, 1).with_worker_timeout(Duration::from_secs(2));
        let task = || AgentRequest {
            agent_id: "worker".to_owned(),
            prompt: "wait".to_owned(),
            agent_type: None,
            model: None,
        };
        swarm
            .dispatch(
                "swarm-stop",
                "sess",
                vec![task()],
                |_request, cancel| async move {
                    cancel.cancelled().await;
                    Err("cancelled".to_owned())
                },
            )
            .expect("dispatch");
        assert!(swarm.force_stop_swarm("swarm-stop").await);
        assert!(!swarm.accepts_dispatch("swarm-stop"));
        let results = swarm.collect_results("swarm-stop").await;
        assert_eq!(results.failure_count, 1);
        assert!(!swarm.has_pending("swarm-stop"));
        assert!(
            swarm
                .dispatch(
                    "swarm-stop",
                    "sess",
                    vec![task()],
                    |_request, _cancel| async { Ok("unexpected".to_owned()) }
                )
                .is_err()
        );
    }

    #[test]
    fn swarm_with_event_bus_and_timeout() {
        let tm = Arc::new(TeamManager::new());
        let bus = Arc::new(CoordinatorEventBus::new());
        let swarm = SwarmService::new(tm, 4)
            .with_event_bus(bus)
            .with_worker_timeout(Duration::from_mins(1));
        assert_eq!(swarm.worker_timeout, Duration::from_mins(1));
        assert!(swarm.event_bus.is_some());
    }

    #[test]
    fn team_manager_create_and_get() {
        let tm = TeamManager::new();
        let info = tm.create_team("team-1", 5, "session-1").expect("create ok");
        assert_eq!(info.team_id, "team-1");
        assert_eq!(info.worker_count, 5);

        let got = tm.get_team("team-1").expect("exists");
        assert_eq!(got.team_id, "team-1");
    }

    #[test]
    fn team_manager_duplicate_errors() {
        let tm = TeamManager::new();
        tm.create_team("team-1", 3, "s1").expect("first ok");
        assert!(tm.create_team("team-1", 3, "s2").is_err());
    }

    #[test]
    fn team_manager_worker_count_validation() {
        let tm = TeamManager::new();
        assert!(tm.create_team("t", 0, "s").is_err());
        assert!(tm.create_team("t", 21, "s").is_err());
        assert!(tm.create_team("t", 1, "s").is_ok());
    }

    #[test]
    fn team_manager_destroy() {
        let tm = TeamManager::new();
        tm.create_team("team-1", 3, "s1").expect("create ok");
        assert!(tm.destroy_team("team-1"));
        assert!(!tm.destroy_team("team-1"));
        assert!(tm.get_team("team-1").is_none());
    }

    #[test]
    fn team_manager_list_and_destroy_all() {
        let tm = TeamManager::new();
        tm.create_team("t1", 2, "s").expect("ok");
        tm.create_team("t2", 3, "s").expect("ok");
        assert_eq!(tm.list_teams().len(), 2);

        tm.destroy_all();
        assert_eq!(tm.list_teams().len(), 0);
    }

    #[tokio::test]
    async fn in_process_backend_parallel_execution() {
        let backend = InProcessBackend::new(3);
        let requests = vec![
            AgentRequest {
                agent_id: "a1".into(),
                prompt: "task 1".into(),
                agent_type: None,
                model: None,
            },
            AgentRequest {
                agent_id: "a2".into(),
                prompt: "task 2".into(),
                agent_type: None,
                model: None,
            },
        ];

        let results = backend
            .execute_parallel(requests, |req| async move {
                Ok(format!("done: {}", req.prompt))
            })
            .await;

        assert_eq!(results.len(), 2);
        for r in &results {
            assert_eq!(r.status, "completed");
        }
    }

    #[tokio::test]
    async fn in_process_backend_empty_requests() {
        let backend = InProcessBackend::new(5);
        let results = backend
            .execute_parallel(Vec::new(), |_req| async { Ok("x".to_owned()) })
            .await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn in_process_backend_worker_failure() {
        let backend = InProcessBackend::new(2);
        let requests = vec![AgentRequest {
            agent_id: "fail-1".into(),
            prompt: "will fail".into(),
            agent_type: None,
            model: None,
        }];

        let results = backend
            .execute_parallel(requests, |_req| async { Err("boom".to_owned()) })
            .await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, "failed");
    }
}
