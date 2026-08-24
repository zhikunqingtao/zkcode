//! Swarm API 端点——Agent Swarms 多代理协作（对照旧 `SwarmController.java`，294L）。
//!
//! | Method | Path | Handler |
//! |--------|------|---------|
//! | GET | /api/swarm | `list_swarms` |
//! | POST | /api/swarm | `create_swarm` |
//! | POST | /api/swarm/{id}/dispatch | `dispatch_swarm` |
//! | GET | /api/swarm/{id} | `get_swarm` |
//! | DELETE | /api/swarm/{id} | `destroy_swarm` |
//! | POST | /api/swarm/{id}/abort | `abort_swarm` |
//! | POST | /api/swarm/{id}/shutdown | `shutdown_swarm` |
//! | POST | /api/swarm/{id}/force-stop | `force_stop_swarm` |
//! | POST | /api/swarm/{id}/worker/{workerId}/abort | `abort_worker` |
//!
//! Feature Flag 门控：`ENABLE_AGENT_SWARMS`（旧 `application.yml` L148，
//! 本仓库出厂默认 `true`）；未启用时所有端点返回 404。
//!
//! # 有意差异
//!
//! - Java 使用 `@RequestMapping("/api/swarm")` + 多个方法映射；本实现
//!   在 `routes.rs` 中逐条注册（axum 惯用法）。
//! - 全部端点只访问 `AppState.coordinator` 的单一生产实例；团队状态、Worker
//!   状态、取消令牌和事件总线不再由 REST 另建副本。
//! - Java 使用 `SwarmState` / `SwarmConfig` DTO；本实现使用
//!   `serde_json::Value` 直接构造响应（Phase 1 简化）。
//! - Java abort 端点含三层防御身份验证；本实现简化为 session 验证
//!   （多层防御后续按需补全）。

use axum::Json;
use axum::extract::{Path as AxumPath, State};
use serde_json::{Value, json};
use sha2::Digest as _;
use zk_db::SwarmRecord;
use zk_engine::coordinator::AgentRequest as SwarmAgentRequest;
use zk_engine::{AgentRequest, AgentStatus, ChildExecutionContext, IsolationMode, WorkerStatus};

use crate::error::ApiError;
use crate::state::AppState;

/// teamName 白名单：字母/数字/下划线/中划线，长度 1-64。
fn is_valid_team_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// `GET /api/swarm` — Coordinator 列表面；WP-11 完成前明确失败关闭。
pub(crate) async fn list_swarms(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    ensure_swarm_ready(&state)?;
    let mut swarms = state
        .db
        .list_swarms()
        .await?
        .into_iter()
        .map(|record| durable_swarm_projection(&record))
        .collect::<Vec<_>>();
    for team in state.coordinator.list_swarms() {
        swarms.retain(|value| value["swarmId"].as_str() != Some(team.team_id.as_str()));
        swarms.push(swarm_projection(&state, &team));
    }
    Ok(Json(json!({ "swarms": swarms })))
}

/// `POST /api/swarm`——创建 Swarm。
///
/// Body: `{ "teamName": "...", "maxWorkers": 5, "sessionId": "..." }`
pub(crate) async fn create_swarm(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    ensure_swarm_ready(&state)?;
    if !state.feature_flags.is_enabled("ENABLE_AGENT_SWARMS") {
        return Err(ApiError::not_found(
            "FEATURE_DISABLED",
            "Agent Swarms feature is disabled",
        ));
    }

    let team_name = body
        .get("teamName")
        .and_then(|v| v.as_str())
        .unwrap_or("swarm-team");
    if !is_valid_team_name(team_name) {
        return Err(ApiError::validation(
            "Invalid teamName: must match ^[A-Za-z0-9_-]{1,64}$ (path traversal prevention)",
        ));
    }

    let max_workers = body
        .get("maxWorkers")
        .and_then(serde_json::Value::as_u64)
        .map_or(5, |v| usize::try_from(v).unwrap_or(5));
    let session_id = body
        .get("sessionId")
        .and_then(|v| v.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::validation("sessionId is required"))?;
    let session = state
        .db
        .get_session(session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(session_id))?;
    if state.db.find_swarm(team_name).await?.is_some() {
        return Err(ApiError::validation(format!(
            "Swarm already exists: {team_name}"
        )));
    }
    if let Some(snapshot) = body.get("projectContext") {
        let working_dir_hash =
            format!("{:x}", sha2::Sha256::digest(session.working_dir.as_bytes()));
        let now = zk_db::time::format_rfc3339_micros(zk_db::time::now_millis());
        state
            .db
            .save_project_context(&zk_db::ProjectContextRecord {
                id: format!("project-context-{}", uuid::Uuid::new_v4()),
                working_dir_hash,
                snapshot: snapshot.clone(),
                git_head_sha: None,
                updated_at: now,
            })
            .await?;
    }

    let objective = body
        .get("objective")
        .and_then(Value::as_str)
        .or_else(|| {
            body.get("projectContext")
                .and_then(|context| context.get("objective"))
                .and_then(Value::as_str)
        })
        .unwrap_or("Coordinate the requested tasks safely");
    match state.coordinator.create_swarm_with_objective(
        team_name,
        max_workers,
        session_id,
        objective,
    ) {
        Ok(info) => {
            let record = SwarmRecord::created(team_name, session_id, max_workers);
            if let Err(error) = state.db.save_swarm(&record).await {
                state.coordinator.destroy_swarm(team_name);
                return Err(error.into());
            }
            state.coordinator.publish_swarm_state(&info.team_id);
            Ok(Json(json!({
                "swarmId": &info.team_id,
                "teamName": &info.team_id,
                "phase": "Research",
                "maxWorkers": max_workers
            })))
        }
        Err(e) => Err(ApiError::validation(e)),
    }
}

/// `POST /api/swarm/{swarmId}/dispatch`——分发任务给 Workers。
///
/// Body: `{ "tasks": [{"prompt": "...", "agentType": "..."}] }`
#[allow(clippy::too_many_lines)] // validation, durable binding, dispatch, and collector are one transaction boundary
pub(crate) async fn dispatch_swarm(
    State(state): State<AppState>,
    AxumPath(swarm_id): AxumPath<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    ensure_swarm_ready(&state)?;
    if !state.feature_flags.is_enabled("ENABLE_AGENT_SWARMS") {
        return Err(ApiError::not_found(
            "FEATURE_DISABLED",
            "Agent Swarms feature is disabled",
        ));
    }

    let team = state.coordinator.get_swarm(&swarm_id).ok_or_else(|| {
        ApiError::not_found("SWARM_NOT_FOUND", &format!("Swarm not found: {swarm_id}"))
    })?;

    let task_values = body
        .get("tasks")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if task_values.is_empty() || task_values.len() > team.worker_count {
        return Err(ApiError::validation(format!(
            "tasks must contain 1..={} items",
            team.worker_count
        )));
    }
    let run_id = body
        .get("runId")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::validation("runId is required"))?;
    let run = state
        .db
        .find_run_by_id(run_id)
        .await?
        .ok_or_else(|| ApiError::not_found("RUN_NOT_FOUND", "Parent Run not found"))?;
    if run.session_id != team.session_id {
        return Err(ApiError::validation(
            "runId does not belong to the Swarm session",
        ));
    }
    if !matches!(
        run.status.as_str(),
        "QUEUED" | "RUNNING" | "WAITING_INTERACTION"
    ) {
        return Err(ApiError::validation("Parent Run is already terminal"));
    }
    let session = state
        .db
        .get_session(&team.session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&team.session_id))?;
    let Some(runtime) = state.agent_runtime() else {
        return Err(ApiError::feature_not_ready(
            "Swarm dispatch",
            "ZK_AGENT_ENABLED and the shared production Agent runtime are enabled",
        ));
    };

    let mut seen = std::collections::HashSet::new();
    let mut requests = Vec::with_capacity(task_values.len());
    for (index, task) in task_values.iter().enumerate() {
        let prompt = task
            .get("prompt")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ApiError::validation("each task requires a non-empty prompt"))?;
        if prompt.len() > 32 * 1024 {
            return Err(ApiError::validation("task prompt exceeds 32 KiB"));
        }
        let requested_worker = task
            .get("workerId")
            .and_then(Value::as_str)
            .map_or_else(|| format!("worker-{}", index + 1), str::to_owned);
        if !is_valid_team_name(&requested_worker) || !seen.insert(requested_worker.clone()) {
            return Err(ApiError::validation(
                "workerId must be unique and path-safe",
            ));
        }
        let worker_id = format!("{swarm_id}-{requested_worker}");
        let agent_type = task
            .get("agentType")
            .and_then(Value::as_str)
            .unwrap_or("explore")
            .to_owned();
        let model = task
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or(&run.model)
            .to_owned();
        requests.push(SwarmAgentRequest {
            agent_id: worker_id,
            prompt: prompt.to_owned(),
            agent_type: Some(agent_type),
            model: Some(model),
        });
    }

    state
        .coordinator
        .begin_dispatch(&swarm_id)
        .map_err(ApiError::validation)?;
    let mut durable = state
        .db
        .find_swarm(&swarm_id)
        .await?
        .ok_or_else(|| ApiError::not_found("SWARM_NOT_FOUND", "Swarm history not found"))?;
    durable.phase = String::from("RUNNING");
    durable.total_tasks = requests.len();
    durable.active_workers = requests.len();
    durable.completed_tasks = 0;
    state.db.save_swarm(&durable).await?;
    let task_type = format!("swarm:{swarm_id}:worker");
    let mut registered_workers: Vec<String> = Vec::new();
    for request in &requests {
        let worker_cancel = state
            .coordinator
            .swarm_service()
            .worker_cancel_token(&swarm_id, &request.agent_id);
        if let Err(error) = runtime
            .tasks
            .register_external_task(
                &request.agent_id,
                &team.session_id,
                &task_type,
                &request.prompt,
                worker_cancel,
            )
            .await
        {
            for worker_id in &registered_workers {
                let failure = Err("Swarm dispatch registration failed".to_owned());
                let _ = runtime
                    .tasks
                    .finish_external_task(worker_id, &team.session_id, &failure)
                    .await;
            }
            state.coordinator.finish_dispatch(&swarm_id, false);
            state
                .coordinator
                .fail_workflow(&swarm_id, "Swarm task registration failed");
            durable.phase = String::from("FAILED");
            durable.active_workers = 0;
            let _ = state.db.save_swarm(&durable).await;
            return Err(ApiError::validation(error));
        }
        registered_workers.push(request.agent_id.clone());
    }

    let executor = runtime.executor.clone();
    let task_service = runtime.tasks.clone();
    let parent_session_id = team.session_id.clone();
    let parent_run_id = run_id.to_owned();
    let working_directory = std::path::PathBuf::from(session.working_dir);
    let dispatch = state.coordinator.swarm_service().dispatch(
        &swarm_id,
        &team.session_id,
        requests.clone(),
        move |request, cancel| {
            let executor = executor.clone();
            let task_service = task_service.clone();
            let parent_session_id = parent_session_id.clone();
            let parent_run_id = parent_run_id.clone();
            let working_directory = working_directory.clone();
            async move {
                task_service
                    .mark_external_task_running(&request.agent_id, &parent_session_id)
                    .await?;
                let child_request = AgentRequest::new(
                    request.agent_id.clone(),
                    request.prompt.clone(),
                    request.agent_type,
                    request.model,
                    IsolationMode::None,
                    true,
                );
                let context = ChildExecutionContext {
                    parent_session_id: parent_session_id.clone(),
                    parent_run_id,
                    working_directory,
                    tool_use_id: format!("swarm-{}", request.agent_id),
                    allowed_tools: None,
                };
                let result = executor
                    .execute_sync_with_cancel(&child_request, &context, cancel)
                    .await;
                let outcome = match result.status {
                    AgentStatus::Completed | AgentStatus::MaxTurns => {
                        Ok(result.result.unwrap_or_default())
                    }
                    _ => Err(result.result.unwrap_or_else(|| "Worker failed".to_owned())),
                };
                task_service
                    .finish_external_task(&request.agent_id, &parent_session_id, &outcome)
                    .await?;
                outcome
            }
        },
    );
    if let Err(error) = dispatch {
        state.coordinator.finish_dispatch(&swarm_id, false);
        state.coordinator.fail_workflow(&swarm_id, &error);
        durable.phase = String::from("FAILED");
        durable.active_workers = 0;
        let _ = state.db.save_swarm(&durable).await;
        return Err(ApiError::validation(error));
    }
    let _ = state
        .coordinator
        .advance_workflow(&swarm_id, "Workers dispatched and executing");
    state.coordinator.publish_swarm_state(&swarm_id);
    let coordinator = state.coordinator.clone();
    let anomaly_db = state.db.clone();
    let swarm_db = state.db.clone();
    let swarm_for_collect = swarm_id.clone();
    tokio::spawn(async move {
        let results = coordinator
            .swarm_service()
            .collect_results(&swarm_for_collect)
            .await;
        if matches!(
            coordinator.swarm_phase(&swarm_for_collect),
            Some(zk_engine::SwarmPhase::Aborting | zk_engine::SwarmPhase::Aborted)
        ) {
            coordinator.mark_aborted(&swarm_for_collect);
        } else {
            coordinator.finish_dispatch(
                &swarm_for_collect,
                results.failure_count == 0 && results.success_count > 0,
            );
        }
        if let Ok(Some(mut durable)) = swarm_db.find_swarm(&swarm_for_collect).await {
            durable.active_workers = 0;
            durable.total_tasks = results.success_count + results.failure_count;
            durable.completed_tasks = results.success_count;
            durable.phase = String::from(match coordinator.swarm_phase(&swarm_for_collect) {
                Some(zk_engine::SwarmPhase::Completed) => "COMPLETED",
                Some(zk_engine::SwarmPhase::Aborted) => "ABORTED",
                _ => "FAILED",
            });
            if let Err(error) = swarm_db.save_swarm(&durable).await {
                tracing::error!(%error, "failed to persist terminal Swarm projection");
            }
        }
        if results.failure_count == 0 && results.success_count > 0 {
            let _ = coordinator.advance_workflow(
                &swarm_for_collect,
                "Worker outputs aggregated for verification",
            );
            let _ = coordinator
                .advance_workflow(&swarm_for_collect, "All durable worker outcomes verified");
        } else if !matches!(
            coordinator.swarm_phase(&swarm_for_collect),
            Some(zk_engine::SwarmPhase::Aborted)
        ) {
            coordinator.fail_workflow(&swarm_for_collect, "One or more workers failed");
        }
        for worker in coordinator
            .swarm_service()
            .worker_states(&swarm_for_collect)
            .into_iter()
            .filter(|worker| worker.status == WorkerStatus::Failed)
        {
            let message = worker
                .output
                .clone()
                .unwrap_or_else(|| "Worker failed without output".to_owned());
            let rule_id = if message.to_ascii_lowercase().contains("cancel")
                || message.to_ascii_lowercase().contains("abort")
            {
                "worker-cancelled"
            } else {
                "worker-failed"
            };
            let event = zk_db::AnomalyEventRecord {
                id: format!("anomaly-{}", uuid::Uuid::new_v4()),
                swarm_id: swarm_for_collect.clone(),
                worker_id: worker.worker_id,
                rule_id: rule_id.to_owned(),
                severity: if rule_id == "worker-cancelled" {
                    "warning".to_owned()
                } else {
                    "error".to_owned()
                },
                message: message.clone(),
                detected_at: zk_db::time::now_millis(),
                resolved_at: None,
                resolution: None,
                context_snapshot: Some(json!({
                    "phase": coordinator
                        .swarm_phase(&swarm_for_collect)
                        .map_or("INTERRUPTED", zk_engine::SwarmPhase::as_str),
                    "output": message
                })),
            };
            if let Err(error) = anomaly_db.save_anomaly_event(&event).await {
                tracing::error!(%error, "failed to persist Swarm anomaly");
            }
        }
        coordinator.publish_swarm_state(&swarm_for_collect);
    });

    Ok(Json(json!({
        "swarmId": swarm_id,
        "teamName": team.team_id,
        "dispatched": requests.len(),
        "workerIds": requests.into_iter().map(|request| request.agent_id).collect::<Vec<_>>(),
        "status": "running"
    })))
}

/// `GET /api/swarm/{swarmId}`——查询 Swarm 状态。
pub(crate) async fn get_swarm(
    State(state): State<AppState>,
    AxumPath(swarm_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    ensure_swarm_ready(&state)?;
    if !state.feature_flags.is_enabled("ENABLE_AGENT_SWARMS") {
        return Err(ApiError::not_found(
            "FEATURE_DISABLED",
            "Agent Swarms feature is disabled",
        ));
    }

    let team = state.coordinator.get_swarm(&swarm_id).ok_or_else(|| {
        ApiError::not_found("SWARM_NOT_FOUND", &format!("Swarm not found: {swarm_id}"))
    });
    if let Ok(team) = team {
        return Ok(Json(swarm_projection(&state, &team)));
    }
    let record = state.db.find_swarm(&swarm_id).await?.ok_or_else(|| {
        ApiError::not_found("SWARM_NOT_FOUND", &format!("Swarm not found: {swarm_id}"))
    })?;
    Ok(Json(durable_swarm_projection(&record)))
}

/// `DELETE /api/swarm/{swarmId}`——销毁 Swarm。
pub(crate) async fn destroy_swarm(
    State(state): State<AppState>,
    AxumPath(swarm_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    ensure_swarm_ready(&state)?;
    let durable_exists = state.db.find_swarm(&swarm_id).await?.is_some();
    if !state.feature_flags.is_enabled("ENABLE_AGENT_SWARMS") {
        return Err(ApiError::not_found(
            "FEATURE_DISABLED",
            "Agent Swarms feature is disabled",
        ));
    }

    let running_workers = state
        .coordinator
        .swarm_service()
        .worker_states(&swarm_id)
        .into_iter()
        .filter(|worker| worker.status == WorkerStatus::Running)
        .map(|worker| worker.worker_id)
        .collect::<Vec<_>>();
    if state.coordinator.swarm_service().has_pending(&swarm_id) {
        let _ = state
            .coordinator
            .swarm_service()
            .force_stop_swarm(&swarm_id)
            .await;
        fail_worker_tasks(&state, &swarm_id, &running_workers, "Swarm destroyed").await;
    }
    state.coordinator.mark_aborted(&swarm_id);
    state.coordinator.publish_swarm_state(&swarm_id);
    let destroyed = state.coordinator.destroy_swarm(&swarm_id);
    let durable_deleted = state.db.delete_swarm(&swarm_id).await?;
    if !destroyed && !durable_exists && !durable_deleted {
        return Err(ApiError::not_found(
            "SWARM_NOT_FOUND",
            &format!("Swarm not found: {swarm_id}"),
        ));
    }

    Ok(Json(json!({
        "status": "destroyed",
        "swarmId": swarm_id
    })))
}

/// `POST /api/swarm/{swarmId}/abort`——中止 Swarm。
pub(crate) async fn abort_swarm(
    State(state): State<AppState>,
    AxumPath(swarm_id): AxumPath<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    ensure_swarm_ready(&state)?;
    if !state.feature_flags.is_enabled("ENABLE_AGENT_SWARMS") {
        return Err(ApiError::not_found(
            "FEATURE_DISABLED",
            "Agent Swarms feature is disabled",
        ));
    }

    let _team = state.coordinator.get_swarm(&swarm_id).ok_or_else(|| {
        ApiError::not_found("SWARM_NOT_FOUND", &format!("Swarm not found: {swarm_id}"))
    })?;

    let reason = body
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("no reason");

    if !state.coordinator.begin_abort(&swarm_id) {
        return Err(ApiError::validation("Swarm is already terminal"));
    }
    let running_workers = state
        .coordinator
        .swarm_service()
        .worker_states(&swarm_id)
        .into_iter()
        .filter(|worker| worker.status == WorkerStatus::Running)
        .map(|worker| worker.worker_id)
        .collect::<Vec<_>>();
    fail_worker_tasks(&state, &swarm_id, &running_workers, "Swarm aborted").await;
    let pending = state.coordinator.swarm_service().has_pending(&swarm_id);
    if !pending {
        state.coordinator.mark_aborted(&swarm_id);
    }
    if let Some(mut durable) = state.db.find_swarm(&swarm_id).await? {
        durable.phase = String::from(if pending { "ABORTING" } else { "ABORTED" });
        if !pending {
            durable.active_workers = 0;
        }
        state.db.save_swarm(&durable).await?;
    }
    state.coordinator.publish_swarm_state(&swarm_id);

    Ok(Json(json!({
        "swarmId": swarm_id,
        "status": if pending { "aborting" } else { "aborted" },
        "reason": reason
    })))
}

/// `POST /api/swarm/{swarmId}/shutdown` — WP-11 兼容入口。
pub(crate) async fn shutdown_swarm(
    State(state): State<AppState>,
    AxumPath(swarm_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    ensure_swarm_ready(&state)?;
    if !state.coordinator.swarm_service().shutdown_swarm(&swarm_id) {
        return Err(ApiError::not_found(
            "SWARM_NOT_FOUND",
            &format!("Swarm not found: {swarm_id}"),
        ));
    }
    if let Some(mut durable) = state.db.find_swarm(&swarm_id).await? {
        durable.phase = String::from("SHUTTING_DOWN");
        state.db.save_swarm(&durable).await?;
    }
    state.coordinator.publish_swarm_state(&swarm_id);
    Ok(Json(
        json!({"swarmId": swarm_id, "status": "shutting_down"}),
    ))
}

/// `POST /api/swarm/{swarmId}/force-stop` — WP-11 兼容入口。
pub(crate) async fn force_stop_swarm(
    State(state): State<AppState>,
    AxumPath(swarm_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    ensure_swarm_ready(&state)?;
    let running_workers = state
        .coordinator
        .swarm_service()
        .worker_states(&swarm_id)
        .into_iter()
        .filter(|worker| worker.status == WorkerStatus::Running)
        .map(|worker| worker.worker_id)
        .collect::<Vec<_>>();
    if !state
        .coordinator
        .swarm_service()
        .force_stop_swarm(&swarm_id)
        .await
    {
        return Err(ApiError::not_found(
            "SWARM_NOT_FOUND",
            &format!("Swarm not found: {swarm_id}"),
        ));
    }
    fail_worker_tasks(&state, &swarm_id, &running_workers, "Swarm force-stopped").await;
    state.coordinator.mark_aborted(&swarm_id);
    if let Some(mut durable) = state.db.find_swarm(&swarm_id).await? {
        durable.phase = String::from("ABORTED");
        durable.active_workers = 0;
        state.db.save_swarm(&durable).await?;
    }
    state.coordinator.publish_swarm_state(&swarm_id);
    Ok(Json(json!({"swarmId": swarm_id, "status": "aborted"})))
}

/// `POST /api/swarm/{swarmId}/worker/{workerId}/abort` — WP-11 兼容入口。
pub(crate) async fn abort_worker(
    State(state): State<AppState>,
    AxumPath((swarm_id, worker_id)): AxumPath<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    ensure_swarm_ready(&state)?;
    if state.coordinator.get_swarm(&swarm_id).is_none() {
        return Err(ApiError::not_found(
            "SWARM_NOT_FOUND",
            &format!("Swarm not found: {swarm_id}"),
        ));
    }
    if !state
        .coordinator
        .swarm_service()
        .cancel_worker(&swarm_id, &worker_id)
    {
        return Err(ApiError::not_found(
            "WORKER_NOT_FOUND",
            &format!("Worker not found: {worker_id}"),
        ));
    }
    fail_worker_tasks(
        &state,
        &swarm_id,
        std::slice::from_ref(&worker_id),
        "Worker aborted",
    )
    .await;
    if let Some(mut durable) = state.db.find_swarm(&swarm_id).await? {
        durable.active_workers = durable.active_workers.saturating_sub(1);
        state.db.save_swarm(&durable).await?;
    }
    state.coordinator.publish_swarm_state(&swarm_id);
    Ok(Json(json!({
        "swarmId": swarm_id,
        "workerId": worker_id,
        "status": "aborting"
    })))
}

fn swarm_projection(state: &AppState, team: &zk_engine::TeamInfo) -> Value {
    let workers = state
        .coordinator
        .swarm_service()
        .worker_states(&team.team_id);
    let active_workers = workers
        .iter()
        .filter(|worker| worker.status == WorkerStatus::Running)
        .count();
    let completed_tasks = workers
        .iter()
        .filter(|worker| worker.status == WorkerStatus::Completed)
        .count();
    json!({
        "swarmId": team.team_id,
        "teamName": team.team_id,
        "phase": state.coordinator.swarm_phase(&team.team_id)
            .map_or("INTERRUPTED", zk_engine::SwarmPhase::as_str),
        "maxWorkers": team.worker_count,
        "sessionId": team.session_id,
        "activeWorkers": active_workers,
        "totalWorkers": workers.len(),
        "completedTasks": completed_tasks,
        "totalTasks": workers.len()
    })
}

fn durable_swarm_projection(record: &SwarmRecord) -> Value {
    json!({
        "swarmId": record.swarm_id,
        "teamName": record.swarm_id,
        "phase": record.phase,
        "maxWorkers": record.max_workers,
        "sessionId": record.session_id,
        "activeWorkers": record.active_workers,
        "totalWorkers": record.total_tasks,
        "completedTasks": record.completed_tasks,
        "totalTasks": record.total_tasks
    })
}

async fn fail_worker_tasks(state: &AppState, swarm_id: &str, workers: &[String], reason: &str) {
    if workers.is_empty() {
        return;
    }
    let Some(team) = state.coordinator.get_swarm(swarm_id) else {
        return;
    };
    let Some(runtime) = state.agent_runtime() else {
        return;
    };
    for worker_id in workers {
        let result = Err(reason.to_owned());
        if let Err(error) = runtime
            .tasks
            .finish_external_task(worker_id, &team.session_id, &result)
            .await
        {
            tracing::error!(worker_id, %error, "failed to persist cancelled Swarm task");
        }
    }
}

fn ensure_swarm_ready(state: &AppState) -> Result<(), ApiError> {
    if state.config.swarm_enabled {
        Ok(())
    } else {
        Err(ApiError::feature_not_ready(
            "Swarm",
            "Coordinator dispatch, cancellation, persistence, and recovery gates pass",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::body::{Body, to_bytes};
    use axum::extract::ConnectInfo;
    use axum::http::{Method, Request, StatusCode, header};
    use futures::future::BoxFuture;
    use tower::ServiceExt;
    use zk_engine::{
        AgentConcurrencyController, AgentTimeoutConfig, MessageSink, SubAgentEngineFactory,
        SubAgentExecutor, SystemGitCommandRunner, TaskCoordinator, WorktreeManager,
    };

    struct StubFactory;

    impl SubAgentEngineFactory for StubFactory {
        fn create_and_run(
            &self,
            _agent_id: &str,
            _session_id: &str,
            _context: &ChildExecutionContext,
            _model: &str,
            _system_prompt: &str,
            user_prompt: &str,
            _work_dir: &str,
            _mailbox: tokio::sync::mpsc::UnboundedReceiver<zk_engine::AgentMailboxMessage>,
            cancel: tokio_util::sync::CancellationToken,
            _max_turns: u32,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = (Option<String>, Option<String>, bool)>
                    + Send
                    + '_,
            >,
        > {
            let output = format!("done: {user_prompt}");
            let delayed = user_prompt.contains("wait");
            let delay = if user_prompt.contains("force-wait") {
                std::time::Duration::from_secs(5)
            } else {
                std::time::Duration::from_millis(80)
            };
            Box::pin(async move {
                if delayed {
                    tokio::select! {
                        () = cancel.cancelled() => {
                            return (Some("cancelled".to_owned()), None, false);
                        }
                        () = tokio::time::sleep(delay) => {}
                    }
                }
                (Some("end_turn".to_owned()), Some(output), false)
            })
        }
    }

    struct NoopSink;

    impl MessageSink for NoopSink {
        fn push<'a>(
            &'a self,
            _session_id: &'a str,
            _message: zk_protocol::ServerMessage,
        ) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }
    }

    fn router_request(method: Method, path: &str, body: Option<Value>) -> Request<Body> {
        let peer: SocketAddr = "127.0.0.1:51717".parse().expect("loopback peer");
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .extension(ConnectInfo(peer));
        if body.is_some() {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
        }
        builder
            .body(Body::from(
                body.map_or_else(String::new, |value| value.to_string()),
            ))
            .expect("request")
    }

    async fn router_json(router: &axum::Router, request: Request<Body>) -> (StatusCode, Value) {
        let response = router.clone().oneshot(request).await.expect("router call");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("response body");
        let value = serde_json::from_slice(&bytes).expect("JSON response");
        (status, value)
    }

    #[test]
    fn team_name_validation() {
        assert!(is_valid_team_name("my-team_123"));
        assert!(is_valid_team_name("a"));
        assert!(!is_valid_team_name(""));
        assert!(!is_valid_team_name("../etc"));
        assert!(!is_valid_team_name("has space"));
        assert!(!is_valid_team_name(&"a".repeat(65)));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // full real Router lifecycle assertion
    async fn real_router_exposes_closed_gate_and_shared_swarm_state() {
        let closed_state = AppState::for_tests();
        let closed_router = crate::routes::build_router(closed_state);
        let (status, body) = router_json(
            &closed_router,
            router_request(Method::GET, "/api/swarm", None),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "FEATURE_NOT_READY");

        let mut config = crate::config::Config::test_config();
        config.swarm_enabled = true;
        config.agent_enabled = true;
        let db = zk_db::Db::open_in_memory().expect("database");
        let session = db
            .create_session("test-model", &config.workspace_default_root)
            .await
            .expect("session");
        db.start_run(
            "router-root-run",
            &session.id,
            None,
            Some("query"),
            "test-model",
        )
        .await
        .expect("root run");
        let state = AppState::new(db.clone(), config);
        let executor = Arc::new(SubAgentExecutor::new(
            Arc::new(AgentConcurrencyController::default()),
            Arc::new(StubFactory),
            WorktreeManager::for_repo(
                std::env::current_dir().expect("current directory"),
                Arc::new(SystemGitCommandRunner),
            )
            .expect("canonical test root"),
            AgentTimeoutConfig::default(),
        ));
        state.set_agent_runtime(Arc::new(crate::engine_bridge::AgentRuntime {
            executor,
            tasks: Arc::new(TaskCoordinator::new(db, Arc::new(NoopSink))),
        }));
        let router = crate::routes::build_router(state.clone());
        let (status, created) = router_json(
            &router,
            router_request(
                Method::POST,
                "/api/swarm",
                Some(json!({
                    "teamName": "router-swarm",
                    "maxWorkers": 2,
                    "sessionId": session.id,
                    "objective": "short Router lifecycle"
                })),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        assert_eq!(created["swarmId"], "router-swarm");

        let (status, listed) =
            router_json(&router, router_request(Method::GET, "/api/swarm", None)).await;
        assert_eq!(status, StatusCode::OK, "{listed}");
        assert_eq!(listed["swarms"][0]["swarmId"], "router-swarm");

        let (status, found) = router_json(
            &router,
            router_request(Method::GET, "/api/swarm/router-swarm", None),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{found}");
        assert_eq!(found["phase"], "CREATED");
        assert_eq!(found["maxWorkers"], 2);

        let (status, dispatched) = router_json(
            &router,
            router_request(
                Method::POST,
                "/api/swarm/router-swarm/dispatch",
                Some(json!({
                    "runId": "router-root-run",
                    "tasks": [
                        {"workerId": "reader-a", "prompt": "read alpha"},
                        {"workerId": "reader-b", "prompt": "read beta"}
                    ]
                })),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{dispatched}");
        assert_eq!(dispatched["dispatched"], 2);

        for _ in 0..30 {
            let (status, found) = router_json(
                &router,
                router_request(Method::GET, "/api/swarm/router-swarm", None),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{found}");
            if found["phase"] == "COMPLETED" {
                assert_eq!(found["totalWorkers"], 2);
                assert_eq!(found["completedTasks"], 2);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            state.coordinator.swarm_phase("router-swarm"),
            Some(zk_engine::SwarmPhase::Completed),
            "Router Swarm did not reach a real aggregate terminal state"
        );

        let (status, created) = router_json(
            &router,
            router_request(
                Method::POST,
                "/api/swarm",
                Some(json!({
                    "teamName": "router-force-stop",
                    "maxWorkers": 1,
                    "sessionId": session.id
                })),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        let (status, dispatched) = router_json(
            &router,
            router_request(
                Method::POST,
                "/api/swarm/router-force-stop/dispatch",
                Some(json!({
                    "runId": "router-root-run",
                    "tasks": [{"workerId": "slow", "prompt": "force-wait until cancelled"}]
                })),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{dispatched}");
        let (status, stopped) = router_json(
            &router,
            router_request(
                Method::POST,
                "/api/swarm/router-force-stop/force-stop",
                None,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{stopped}");
        assert_eq!(stopped["status"], "aborted");
        assert!(
            !state
                .coordinator
                .swarm_service()
                .has_pending("router-force-stop")
        );
        assert_eq!(
            state.coordinator.swarm_phase("router-force-stop"),
            Some(zk_engine::SwarmPhase::Aborted)
        );
        let task = state
            .db
            .find_task_by_id("router-force-stop-slow")
            .await
            .expect("task lookup")
            .expect("durable force-stop task");
        assert_eq!(task.status, "FAILED");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // dispatch, cancellation, persistence and anomaly assertions
    async fn two_read_only_workers_dispatch_through_shared_runtime_and_persist_tasks() {
        let mut config = crate::config::Config::test_config();
        config.swarm_enabled = true;
        config.agent_enabled = true;
        config.agent_write_enabled = false;
        let db = zk_db::Db::open_in_memory().expect("database");
        let state = AppState::new(db.clone(), config);
        let executor = Arc::new(SubAgentExecutor::new(
            Arc::new(AgentConcurrencyController::default()),
            Arc::new(StubFactory),
            WorktreeManager::for_repo(
                std::env::current_dir().expect("current directory"),
                Arc::new(SystemGitCommandRunner),
            )
            .expect("canonical test root"),
            AgentTimeoutConfig::default(),
        ));
        let tasks = Arc::new(TaskCoordinator::new(db.clone(), Arc::new(NoopSink)));
        state.set_agent_runtime(Arc::new(crate::engine_bridge::AgentRuntime {
            executor,
            tasks,
        }));
        let mut coordinator_events = state.coordinator.event_bus().subscribe();
        let session = db
            .create_session("test-model", &state.config.workspace_default_root)
            .await
            .expect("session");
        db.start_run("root-run", &session.id, None, Some("query"), "test-model")
            .await
            .expect("root run");

        let _ = create_swarm(
            State(state.clone()),
            Json(json!({
                "teamName": "short-swarm",
                "maxWorkers": 2,
                "sessionId": session.id,
                "projectContext": {"objective": "short read-only verification"}
            })),
        )
        .await
        .expect("create swarm");
        let context_hash = format!(
            "{:x}",
            sha2::Sha256::digest(state.config.workspace_default_root.as_bytes())
        );
        let context = db
            .find_project_context(&context_hash)
            .await
            .expect("read project context")
            .expect("project context persisted");
        assert_eq!(
            context.snapshot["objective"],
            "short read-only verification"
        );
        let Json(response) = dispatch_swarm(
            State(state.clone()),
            AxumPath("short-swarm".to_owned()),
            Json(json!({
                "runId": "root-run",
                "tasks": [
                    {"workerId": "reader-a", "prompt": "read alpha"},
                    {"workerId": "reader-b", "prompt": "read beta"}
                ]
            })),
        )
        .await
        .expect("dispatch swarm");
        assert_eq!(response["dispatched"], 2);

        let mut completed = false;
        for _ in 0..20 {
            let records = db
                .find_tasks_by_session(&session.id)
                .await
                .expect("read tasks");
            if records.len() == 2 && records.iter().all(|task| task.status == "COMPLETED") {
                assert!(records.iter().all(|task| {
                    task.output
                        .as_deref()
                        .is_some_and(|output| output.starts_with("done:"))
                }));
                assert_eq!(
                    state.coordinator.swarm_phase("short-swarm"),
                    Some(zk_engine::SwarmPhase::Completed)
                );
                completed = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            completed,
            "Swarm tasks did not reach durable terminal state"
        );
        let workflow_events = std::iter::from_fn(|| coordinator_events.try_recv().ok())
            .filter_map(|event| match event {
                zk_engine::CoordinatorEvent::WorkflowPhaseUpdate {
                    phase_name, status, ..
                } => Some((phase_name, status)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            workflow_events,
            [
                ("Research".to_owned(), "RUNNING".to_owned()),
                ("Synthesis".to_owned(), "RUNNING".to_owned()),
                ("Implementation".to_owned(), "RUNNING".to_owned()),
                ("Verification".to_owned(), "RUNNING".to_owned()),
                ("Verification".to_owned(), "COMPLETED".to_owned()),
            ]
        );

        let _ = create_swarm(
            State(state.clone()),
            Json(json!({
                "teamName": "abort-swarm",
                "maxWorkers": 2,
                "sessionId": session.id
            })),
        )
        .await
        .expect("create abort swarm");
        let _ = dispatch_swarm(
            State(state.clone()),
            AxumPath("abort-swarm".to_owned()),
            Json(json!({
                "runId": "root-run",
                "tasks": [
                    {"workerId": "reader-a", "prompt": "wait alpha"},
                    {"workerId": "reader-b", "prompt": "wait beta"}
                ]
            })),
        )
        .await
        .expect("dispatch abort swarm");
        let _ = abort_worker(
            State(state.clone()),
            AxumPath(("abort-swarm".to_owned(), "abort-swarm-reader-a".to_owned())),
        )
        .await
        .expect("abort target worker");

        for _ in 0..30 {
            let records = db
                .find_tasks_by_session(&session.id)
                .await
                .expect("read abort tasks");
            let aborted = records
                .iter()
                .find(|task| task.id == "abort-swarm-reader-a");
            let sibling = records
                .iter()
                .find(|task| task.id == "abort-swarm-reader-b");
            if aborted.is_some_and(|task| task.status == "FAILED")
                && sibling.is_some_and(|task| task.status == "COMPLETED")
            {
                let anomalies = db
                    .find_anomalies_by_swarm("abort-swarm")
                    .await
                    .expect("read anomalies");
                if state.coordinator.swarm_phase("abort-swarm")
                    == Some(zk_engine::SwarmPhase::Failed)
                    && anomalies.iter().any(|event| {
                        event.worker_id == "abort-swarm-reader-a"
                            && event.rule_id == "worker-cancelled"
                    })
                {
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("target worker abort did not preserve sibling completion");
    }

    #[tokio::test]
    async fn restart_interrupts_task_run_and_durable_swarm_without_rescheduling() {
        let directory =
            std::env::temp_dir().join(format!("zkcode-swarm-restart-{}", uuid::Uuid::new_v4()));
        let path = directory.join("data.db");
        let db = zk_db::Db::open(&path).expect("database");
        let session = db
            .create_session("test-model", "/tmp/zkcode-swarm-restart")
            .await
            .expect("session");
        db.start_run(
            "restart-run",
            &session.id,
            None,
            Some("query"),
            "test-model",
        )
        .await
        .expect("run");
        let mut task = zk_db::new_task_record("restart-task", &session.id, Some("active worker"));
        task.status = "RUNNING".to_owned();
        task.task_type = "swarm:restart-swarm:worker".to_owned();
        db.save_task(&task).await.expect("task");
        db.save_anomaly_event(&zk_db::AnomalyEventRecord {
            id: "restart-anomaly".to_owned(),
            swarm_id: "restart-swarm".to_owned(),
            worker_id: "restart-worker".to_owned(),
            rule_id: "worker-stalled".to_owned(),
            severity: "warning".to_owned(),
            message: "worker was active before restart".to_owned(),
            detected_at: 1,
            resolved_at: None,
            resolution: None,
            context_snapshot: None,
        })
        .await
        .expect("anomaly");
        let mut config = crate::config::Config::test_config();
        config.swarm_enabled = true;
        let state = AppState::new(db.clone(), config.clone());
        state
            .coordinator
            .create_swarm("restart-swarm", 1, &session.id)
            .expect("active swarm");
        let mut swarm = zk_db::SwarmRecord::created("restart-swarm", &session.id, 1);
        swarm.phase = String::from("RUNNING");
        swarm.total_tasks = 1;
        swarm.active_workers = 1;
        db.save_swarm(&swarm).await.expect("durable swarm");
        drop(state);
        drop(db);

        let reopened = zk_db::Db::open(&path).expect("reopen database");
        assert_eq!(reopened.interrupt_active_tasks().await.unwrap(), 1);
        assert_eq!(reopened.interrupt_active_swarms().await.unwrap(), 1);
        assert_eq!(
            reopened.interrupt_stale_runs().await.unwrap(),
            vec!["restart-run".to_owned()]
        );
        let restarted = AppState::new(reopened.clone(), config);
        // Scheduling is intentionally not resumed; only its SQLite history is restored.
        assert!(restarted.coordinator.list_swarms().is_empty());
        let recovered_swarm = reopened.find_swarm("restart-swarm").await.unwrap().unwrap();
        assert_eq!(recovered_swarm.phase, "INTERRUPTED");
        assert_eq!(recovered_swarm.active_workers, 0);
        assert_eq!(
            reopened
                .find_task_by_id("restart-task")
                .await
                .unwrap()
                .unwrap()
                .status,
            "KILLED"
        );
        assert_eq!(
            reopened
                .find_run_by_id("restart-run")
                .await
                .unwrap()
                .unwrap()
                .status,
            "INTERRUPTED"
        );
        assert_eq!(
            reopened
                .find_anomalies_by_swarm("restart-swarm")
                .await
                .unwrap()
                .len(),
            1
        );
        drop(restarted);
        drop(reopened);
        let _ = std::fs::remove_dir_all(directory);
    }
}
