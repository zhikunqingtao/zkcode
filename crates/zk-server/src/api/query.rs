//! Sync, SSE and continuous-conversation adapters over the shared `ConversationService`.

use std::collections::HashSet;
use std::convert::Infallible;
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::stream;
use serde::Deserialize;
use serde_json::Value;
use zk_authz::model::PermissionMode;
use zk_engine::{ConversationOutcome, ConversationRunOptions};

use crate::error::ApiError;
use crate::state::AppState;

const MAX_QUERY_TURNS: u32 = 4;
const MAX_QUERY_BUDGET_USD: f64 = 1.0;
const MAX_QUERY_TIMEOUT_SECONDS: u64 = 90;

/// Query request shared by sync, SSE and conversation modes.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct QueryRequest {
    prompt: String,
    model: Option<String>,
    #[serde(default)]
    system_prompt: Option<String>,
    #[serde(default)]
    append_system_prompt: Option<String>,
    #[serde(default)]
    permission_mode: Option<String>,
    #[serde(default = "default_max_turns")]
    max_turns: u32,
    #[serde(default = "default_budget")]
    max_budget_usd: f64,
    #[serde(default)]
    allowed_tools: Vec<String>,
    #[serde(default)]
    disallowed_tools: Vec<String>,
    project_id: Option<String>,
    session_id: Option<String>,
    working_directory: Option<Value>,
    #[serde(default = "default_timeout")]
    timeout_seconds: u64,
    #[serde(default)]
    output_format: Option<String>,
    #[serde(default)]
    context: Option<Value>,
    #[serde(default)]
    thinking: Option<String>,
}

const fn default_max_turns() -> u32 {
    MAX_QUERY_TURNS
}

const fn default_budget() -> f64 {
    MAX_QUERY_BUDGET_USD
}

const fn default_timeout() -> u64 {
    MAX_QUERY_TIMEOUT_SECONDS
}

/// `POST /api/query`.
pub(crate) async fn sync_query(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<ConversationOutcome>, ApiError> {
    Ok(Json(execute(&state, request, false).await?))
}

/// `POST /api/query/conversation` — requires an existing authorized session.
pub(crate) async fn conversation_query(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<ConversationOutcome>, ApiError> {
    Ok(Json(execute(&state, request, true).await?))
}

/// `POST /api/query/stream` — terminal events use the same payload as sync mode.
pub(crate) async fn stream_query(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let outcome = execute(&state, request, false).await?;
    let result_data = serde_json::to_string(&outcome).map_err(|_| ApiError::internal())?;
    let events = vec![
        Ok(Event::default().event("result").data(result_data)),
        Ok(Event::default().event("complete").data("{}")),
    ];
    Ok(Sse::new(stream::iter(events)).keep_alive(KeepAlive::default()))
}

async fn execute(
    state: &AppState,
    request: QueryRequest,
    require_existing_session: bool,
) -> Result<ConversationOutcome, ApiError> {
    validate_request(state, &request)?;
    let session_id = resolve_session(state, &request, require_existing_session).await?;
    let mode = request
        .permission_mode
        .as_deref()
        .map_or(Some(PermissionMode::DontAsk), PermissionMode::parse)
        .ok_or_else(|| {
            ApiError::validation_with_code(
                "INVALID_PERMISSION_MODE",
                "Query permissionMode is invalid",
            )
        })?;
    state.authz.modes.set_mode(&session_id, mode).await;
    if let Some(model) = request.model.as_deref() {
        let resolved = resolve_model(state, model)?;
        state.db.update_session_model(&session_id, resolved).await?;
    }
    let service = state.conversation().ok_or_else(|| {
        ApiError::feature_not_ready("Query", "the shared ConversationService is wired")
    })?;
    let options = ConversationRunOptions {
        max_turns: request.max_turns as usize,
        system_prompt: request.system_prompt,
        append_system_prompt: request.append_system_prompt,
        allowed_tools: (!request.allowed_tools.is_empty())
            .then(|| request.allowed_tools.into_iter().collect::<HashSet<_>>()),
        disallowed_tools: request.disallowed_tools.into_iter().collect(),
        thinking: request
            .thinking
            .as_deref()
            .map(parse_thinking_mode)
            .transpose()?,
    };
    let result = tokio::time::timeout(
        Duration::from_secs(request.timeout_seconds),
        service.execute_with_options(&session_id, request.prompt, options),
    )
    .await;
    let Ok(outcome) = result else {
        service.interrupt(&session_id, "QUERY_TIMEOUT");
        return Err(ApiError::validation_with_code(
            "QUERY_TIMEOUT",
            "Query timed out",
        ));
    };
    if outcome.cost_usd > request.max_budget_usd {
        return Err(ApiError::validation_with_code(
            "QUERY_BUDGET_EXCEEDED",
            "Query exceeded maxBudgetUsd",
        ));
    }
    Ok(outcome)
}

fn validate_request(state: &AppState, request: &QueryRequest) -> Result<(), ApiError> {
    if request.prompt.trim().is_empty() {
        return Err(ApiError::validation_with_code(
            "QUERY_PROMPT_REQUIRED",
            "Query prompt must not be blank",
        ));
    }
    if request
        .working_directory
        .as_ref()
        .is_some_and(|value| !value.is_null())
    {
        return Err(ApiError::validation_with_code(
            "QUERY_WORKING_DIRECTORY_FORBIDDEN",
            "workingDirectory must be resolved from an authorized Project or Session",
        ));
    }
    if !(1..=MAX_QUERY_TURNS).contains(&request.max_turns) {
        return Err(ApiError::validation_with_code(
            "QUERY_MAX_TURNS_INVALID",
            "maxTurns must be between 1 and 4",
        ));
    }
    if !request.max_budget_usd.is_finite()
        || request.max_budget_usd <= 0.0
        || request.max_budget_usd > MAX_QUERY_BUDGET_USD
    {
        return Err(ApiError::validation_with_code(
            "QUERY_BUDGET_INVALID",
            "maxBudgetUsd must be greater than 0 and at most 1.0",
        ));
    }
    if !(1..=MAX_QUERY_TIMEOUT_SECONDS).contains(&request.timeout_seconds) {
        return Err(ApiError::validation_with_code(
            "QUERY_TIMEOUT_INVALID",
            "timeoutSeconds must be between 1 and 90",
        ));
    }
    if !request.allowed_tools.is_empty() || !request.disallowed_tools.is_empty() {
        let known = state.tools().names();
        for tool in request
            .allowed_tools
            .iter()
            .chain(request.disallowed_tools.iter())
        {
            if !known.contains(tool) {
                return Err(ApiError::validation_with_code(
                    "QUERY_TOOL_UNKNOWN",
                    &format!("Unknown query tool: {tool}"),
                ));
            }
        }
    }
    let _ = (&request.output_format, &request.context);
    if let Some(mode) = request.thinking.as_deref() {
        parse_thinking_mode(mode)?;
    }
    Ok(())
}

fn parse_thinking_mode(mode: &str) -> Result<zk_llm::ThinkingMode, ApiError> {
    match mode.trim().to_ascii_lowercase().as_str() {
        "adaptive" => Ok(zk_llm::ThinkingMode::Adaptive),
        "enabled" => Ok(zk_llm::ThinkingMode::Enabled),
        "disabled" => Ok(zk_llm::ThinkingMode::Disabled),
        _ => Err(ApiError::validation_with_code(
            "QUERY_THINKING_MODE_INVALID",
            "thinking must be adaptive, enabled, or disabled",
        )),
    }
}

async fn resolve_session(
    state: &AppState,
    request: &QueryRequest,
    require_existing: bool,
) -> Result<String, ApiError> {
    if let Some(session_id) = request.session_id.as_deref() {
        let session = state
            .db
            .get_session(session_id)
            .await?
            .ok_or_else(|| ApiError::session_not_found(session_id))?;
        if let Some(project_id) = request.project_id.as_deref() {
            let project = state
                .db
                .get_project(project_id)
                .await?
                .ok_or_else(|| ApiError::not_found("PROJECT_NOT_FOUND", "Project not found"))?;
            if project.workspace_root != session.working_dir {
                return Err(ApiError::validation_with_code(
                    "QUERY_PROJECT_SESSION_MISMATCH",
                    "Project and Session resolve to different workspaces",
                ));
            }
        }
        return Ok(session_id.to_owned());
    }
    if require_existing {
        return Err(ApiError::validation_with_code(
            "QUERY_SESSION_REQUIRED",
            "conversation mode requires sessionId",
        ));
    }
    let project_id = request.project_id.as_deref().ok_or_else(|| {
        ApiError::validation_with_code(
            "QUERY_SCOPE_REQUIRED",
            "Query requires an authorized projectId or sessionId",
        )
    })?;
    let project = state
        .db
        .get_project(project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("PROJECT_NOT_FOUND", "Project not found"))?;
    let model = request
        .model
        .as_deref()
        .map(|model| resolve_model(state, model))
        .transpose()?
        .unwrap_or(&state.config.default_model);
    Ok(state
        .db
        .create_session(model, &project.workspace_root)
        .await?
        .id)
}

fn resolve_model<'a>(state: &'a AppState, requested: &'a str) -> Result<&'a str, ApiError> {
    let requested = requested.trim();
    let resolved = if matches!(requested, "premium" | "default" | "inherit") {
        state.providers.default_model()
    } else {
        requested
    };
    let models = state.providers.models();
    if resolved.is_empty() || (!models.is_empty() && !models.iter().any(|model| model == resolved))
    {
        return Err(ApiError::validation_with_code(
            "INVALID_MODEL",
            &format!("Unsupported model: {requested}"),
        ));
    }
    Ok(resolved)
}
