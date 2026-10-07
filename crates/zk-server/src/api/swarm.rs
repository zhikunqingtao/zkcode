//! Durable team API. `TaskRuntime` owns worker execution, cancellation and results.
use crate::{error::ApiError, state::AppState};
use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::{Value, json};

fn ready(state: &AppState) -> Result<std::sync::Arc<crate::team_runtime::TeamRuntime>, ApiError> {
    if !state.swarm_executable() {
        return Err(ApiError::feature_not_ready(
            "Swarm",
            "the explicit team/Agent switches and the initialized durable runtime are available",
        ));
    }
    if !state.feature_flags.is_enabled("ENABLE_AGENT_SWARMS") {
        return Err(ApiError::not_found(
            "FEATURE_DISABLED",
            "Agent Swarms feature is disabled",
        ));
    }
    state.team_runtime().ok_or_else(|| {
        ApiError::feature_not_ready("Swarm", "the shared Agent executor is configured")
    })
}
fn failure(_message: String) -> ApiError {
    tracing::error!(
        error_code = "TEAM_OPERATION_FAILED",
        "team operation failed"
    );
    ApiError::validation_with_code(
        "TEAM_OPERATION_FAILED",
        "The team operation could not complete; inspect task diagnostics",
    )
}
async fn team(state: &AppState, id: &str) -> Result<zk_db::TeamDefinition, ApiError> {
    state
        .db
        .find_team(id)
        .await?
        .ok_or_else(|| ApiError::not_found("SWARM_NOT_FOUND", "Team does not exist"))
}
fn name_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}
fn names(value: Option<&Value>) -> Result<Vec<String>, ApiError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let values = value
        .as_array()
        .ok_or_else(|| ApiError::validation("Tool filters must be arrays"))?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty() && value.len() < 128)
                .map(str::to_owned)
                .ok_or_else(|| ApiError::validation("Invalid tool name"))
        })
        .collect()
}
async fn stored_config(state: &AppState) -> Result<Value, ApiError> {
    let value = state.db.get_config_value("user_config").await?;
    value.map_or_else(
        || Ok(json!({})),
        |value| {
            serde_json::from_str::<Value>(&value)
                .map(|value| value["swarm"].clone())
                .map_err(|_| ApiError::validation("Stored user configuration is invalid"))
        },
    )
}
fn bound(body: &Value, stored: &Value, key: &str, default: u64, max: u64) -> Result<u64, ApiError> {
    let value = body.get(key).or_else(|| stored.get(key));
    let number = value
        .map_or(Some(default), Value::as_u64)
        .filter(|value| (1..=max).contains(value))
        .ok_or_else(|| ApiError::validation(format!("{key} must be between 1 and {max}")))?;
    Ok(number)
}

pub(crate) async fn list_swarms(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let runtime = ready(&state)?;
    let mut projections = Vec::new();
    state.db.reconcile_team_queue(state.startup_epoch()).await?;
    for team in state.db.list_teams().await? {
        projections.push(runtime.projection(&team).await.map_err(failure)?);
    }
    Ok(Json(json!({"swarms":projections})))
}
pub(crate) async fn create_swarm(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let runtime = ready(&state)?;
    let name = body["teamName"].as_str().unwrap_or("swarm-team");
    if !name_valid(name) {
        return Err(ApiError::validation("Invalid teamName"));
    }
    let session_id = body["sessionId"]
        .as_str()
        .ok_or_else(|| ApiError::validation("sessionId is required"))?;
    let _session = state
        .db
        .get_session(session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(session_id))?;
    let stored = stored_config(&state).await?;
    let config = worker_config(&state, &body, &stored)?;
    let created = state.db.create_team(name, session_id, config).await?;
    runtime.reset_created(name);
    runtime.kick(name);
    Ok(Json(runtime.projection(&created).await.map_err(failure)?))
}
fn worker_config(state: &AppState, body: &Value, stored: &Value) -> Result<Value, ApiError> {
    let backend = body
        .get("backend")
        .or_else(|| stored.get("backend"))
        .and_then(Value::as_str)
        .unwrap_or("IN_PROCESS");
    if backend != "IN_PROCESS" {
        return Err(ApiError::validation(
            "Only IN_PROCESS team workers are supported",
        ));
    }
    let isolation = body
        .get("workerIsolation")
        .or_else(|| stored.get("workerIsolation"))
        .map_or(Ok("readOnly"), |value| {
            value
                .as_str()
                .ok_or_else(|| ApiError::validation("workerIsolation must be a string"))
        })?;
    if !matches!(isolation, "readOnly" | "worktree") {
        return Err(ApiError::validation(
            "workerIsolation must be readOnly or worktree",
        ));
    }
    if isolation == "worktree"
        && !(state.config.agent_write_enabled && state.config.worktree_enabled)
    {
        return Err(ApiError::feature_not_ready(
            "Team worktree writes",
            "both existing Agent write and Worktree configuration gates are enabled",
        ));
    }
    let max_workers = bound(body, stored, "maxWorkers", 5, 20)?;
    let queue_size = bound(body, stored, "taskQueueSize", 50, 200)?;
    let allow = names(
        body.get("workerToolAllowList")
            .or_else(|| stored.get("workerToolAllowList")),
    )?;
    let deny = names(
        body.get("workerToolDenyList")
            .or_else(|| stored.get("workerToolDenyList")),
    )?;
    let tools = state.tools();
    for tool in allow.iter().chain(&deny) {
        if tools.get(tool).is_none() {
            return Err(ApiError::validation(format!("Unknown tool: {tool}")));
        }
    }
    let effective: Vec<String> = if allow.is_empty() {
        zk_engine::agent::READ_ONLY_CHILD_TOOLS
            .iter()
            .chain(
                zk_engine::agent::WRITE_CHILD_TOOLS
                    .iter()
                    .filter(|_| isolation == "worktree"),
            )
            .filter(|name| tools.get(name).is_some())
            .map(|name| (*name).to_owned())
            .collect()
    } else {
        allow.clone()
    }
    .into_iter()
    .filter(|name| !deny.contains(name))
    .collect();
    if isolation == "readOnly"
        && effective.iter().any(|name| {
            zk_engine::agent::WRITE_CHILD_TOOLS.contains(&name.as_str())
                || tools.get(name).is_some_and(|tool| {
                    matches!(tool.child_access(), zk_tools::ChildToolAccess::WriteGated)
                })
        })
    {
        return Err(ApiError::validation(
            "Write tools require explicit workerIsolation=worktree",
        ));
    }
    let model = body
        .get("workerModel")
        .or_else(|| stored.get("workerModel"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    if model.is_some_and(|model| state.providers.load().model_owner(model).is_none()) {
        return Err(ApiError::validation(
            "workerModel must be a configured model",
        ));
    }
    Ok(
        json!({"backend":"IN_PROCESS","maxWorkers":max_workers,"taskQueueSize":queue_size,"workerModel":model,"workerIsolation":isolation,"workerToolAllowList":effective,"workerToolDenyList":deny}),
    )
}
pub(crate) async fn get_swarm(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let runtime = ready(&state)?;
    state.db.reconcile_team_queue(state.startup_epoch()).await?;
    Ok(Json(
        runtime
            .projection(&team(&state, &id).await?)
            .await
            .map_err(failure)?,
    ))
}
pub(crate) async fn dispatch_swarm(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let runtime = ready(&state)?;
    let team = team(&state, &id).await?;
    let run_id = body["runId"]
        .as_str()
        .ok_or_else(|| ApiError::validation("runId is required"))?;
    let run = state
        .db
        .find_run_by_id(run_id)
        .await?
        .filter(|run| run.session_id == team.session_id)
        .ok_or_else(|| {
            ApiError::not_found("RUN_NOT_FOUND", "Run is not owned by the team session")
        })?;
    let tasks = body["tasks"]
        .as_array()
        .filter(|tasks| !tasks.is_empty() && tasks.len() <= 200)
        .ok_or_else(|| ApiError::validation("tasks requires 1..=200 items"))?;
    let mut payloads = Vec::new();
    for task in tasks {
        let prompt = task["prompt"]
            .as_str()
            .filter(|text| !text.trim().is_empty() && text.len() <= 32768)
            .ok_or_else(|| ApiError::validation("Each task requires a prompt of at most 32 KiB"))?;
        let model = task["model"]
            .as_str()
            .or_else(|| team.config["workerModel"].as_str())
            .unwrap_or(&run.model);
        if state.providers.load().model_owner(model).is_none() {
            return Err(ApiError::validation("Task model must be configured"));
        }
        let default_agent = if team.config["workerIsolation"] == "worktree" {
            "general-purpose"
        } else {
            "explore"
        };
        let agent_type = task["agentType"].as_str().unwrap_or(default_agent);
        if !matches!(
            agent_type,
            "explore" | "plan" | "guide" | "verification" | "general-purpose"
        ) {
            return Err(ApiError::validation("Unknown worker agentType"));
        }
        payloads.push(json!({"prompt":prompt,"model":model,"agentType":agent_type}));
    }
    let generated = uuid::Uuid::new_v4().to_string();
    let request_id = body["requestId"].as_str().unwrap_or(&generated);
    let queued = state
        .db
        .enqueue_team_work(
            &id,
            &team.session_id,
            &run.task_id,
            run_id,
            request_id,
            payloads,
        )
        .await?;
    runtime.kick(&id);
    Ok(Json(
        json!({"swarmId":id,"requestId":request_id,"dispatched":queued.len(),"status":"queued","queueIds":queued.into_iter().map(|item|item.id).collect::<Vec<_>>()}),
    ))
}
async fn stop(state: &AppState, id: &str, shutdown: bool) -> Result<Json<Value>, ApiError> {
    let runtime = ready(state)?;
    let team = team(state, id).await?;
    runtime.stop_intake(id, shutdown);
    runtime.kick(id);
    let saved = state.db.stop_team(id, shutdown).await;
    let mut pending = false;
    // Signal every discoverable owned task even when closing intake failed to persist.
    if !shutdown {
        for item in state.db.team_work_items(id).await? {
            if let Some(task) = item.task_id
                && let Err(error) = state
                    .task_runtime
                    .cancel_owned(&team.session_id, &task, "Team stopped")
                    .await
            {
                pending = true;
                tracing::error!(
                    error_type = std::any::type_name_of_val(&error),
                    "team cancellation pending durable reconciliation"
                );
            }
        }
    }
    runtime.kick(id);
    saved?;
    if pending {
        return Err(ApiError {
            status: axum::http::StatusCode::SERVICE_UNAVAILABLE,
            code: "TEAM_CANCELLATION_PENDING".into(),
            message: "Local stop was requested; durable cancellation is still being reconciled"
                .into(),
        });
    }
    Ok(Json(
        json!({"swarmId":id,"status":if shutdown{"shutting_down"}else{"aborting"}}),
    ))
}
pub(crate) async fn abort_swarm(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(_body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    stop(&state, &id, false).await
}
pub(crate) async fn force_stop_swarm(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    stop(&state, &id, false).await
}
pub(crate) async fn shutdown_swarm(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    stop(&state, &id, true).await
}
pub(crate) async fn destroy_swarm(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let _ = stop(&state, &id, false).await?;
    let runtime = ready(&state)?;
    let projection = runtime
        .projection(&team(&state, &id).await?)
        .await
        .map_err(failure)?;
    if projection["activeWorkers"].as_u64().unwrap_or(1) > 0 {
        return Err(ApiError {
            status: axum::http::StatusCode::CONFLICT,
            code: "TEAM_CLEANUP_PENDING".into(),
            message: "Workers are stopping; retry after durable cleanup".into(),
        });
    }
    state.db.delete_team_if_quiescent(&id).await?;
    Ok(Json(json!({"swarmId":id,"status":"destroyed"})))
}
pub(crate) async fn abort_worker(
    State(state): State<AppState>,
    Path((id, worker)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    ready(&state)?;
    let team = team(&state, &id).await?;
    if !state
        .db
        .team_work_items(&id)
        .await?
        .iter()
        .any(|item| item.task_id.as_deref() == Some(&worker))
    {
        return Err(ApiError::not_found(
            "WORKER_NOT_FOUND",
            "Worker is not owned by this team",
        ));
    }
    state
        .task_runtime
        .cancel_owned(&team.session_id, &worker, "Team worker stopped")
        .await
        .map_err(|e| failure(e.to_string()))?;
    Ok(Json(
        json!({"swarmId":id,"workerId":worker,"status":"aborting"}),
    ))
}
pub(crate) async fn broadcast(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    ready(&state)?;
    let team = team(&state, &id).await?;
    let request = body["requestId"]
        .as_str()
        .ok_or_else(|| ApiError::validation("requestId is required"))?;
    let content = body["message"]
        .as_str()
        .ok_or_else(|| ApiError::validation("message is required"))?;
    let receivers = state
        .db
        .broadcast_team(&id, &team.session_id, request, content)
        .await?;
    Ok(Json(
        json!({"swarmId":id,"requestId":request,"receiverTaskIds":receivers,"status":"queued"}),
    ))
}
