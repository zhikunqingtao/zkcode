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

use crate::api::session::{recover_retired_session_model, resolve_model};
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
    #[serde(default)]
    max_budget_usd: Option<f64>,
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
        let resolved = resolve_model(state, Some(model))?;
        state
            .db
            .update_session_model(&session_id, &resolved)
            .await?;
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
        token_budget: None,
        cost_budget_nanos_usd: request.max_budget_usd.map(usd_to_nanos).transpose()?,
        deadline: Some(Duration::from_secs(request.timeout_seconds)),
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
    if request
        .max_budget_usd
        .is_some_and(|limit| outcome.cost_usd > limit)
    {
        return Err(ApiError::validation_with_code(
            "QUERY_BUDGET_EXCEEDED",
            "Query exceeded maxBudgetUsd",
        ));
    }
    Ok(outcome)
}

fn usd_to_nanos(usd: f64) -> Result<i64, ApiError> {
    let nanos = usd * 1_000_000_000.0;
    // `validate_request` caps this transport field at $1, so every accepted
    // value is far below `i64::MAX` after nano-dollar conversion.
    if !nanos.is_finite() || nanos < 1.0 {
        return Err(ApiError::validation_with_code(
            "QUERY_BUDGET_INVALID",
            "maxBudgetUsd cannot be represented safely",
        ));
    }
    #[allow(clippy::cast_possible_truncation)]
    Ok(nanos.floor() as i64)
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
    if request
        .max_budget_usd
        .is_some_and(|budget| !budget.is_finite() || budget <= 0.0 || budget > MAX_QUERY_BUDGET_USD)
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
        let mut session = state
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
        if request.model.is_none() {
            recover_retired_session_model(state, &mut session).await?;
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
    let model = resolve_model(state, request.model.as_deref())?;
    Ok(state
        .db
        .create_session(&model, &project.workspace_root)
        .await?
        .id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::stream::BoxStream;
    use tokio_util::sync::CancellationToken;
    use zk_llm::{ChatProvider, ChatRequest, ProviderError, ProviderEvent, ProviderRegistry};

    use super::*;

    struct StubProvider;

    impl ChatProvider for StubProvider {
        fn provider_name(&self) -> &'static str {
            "stub"
        }

        fn chat_stream(
            &self,
            _request: ChatRequest,
            _cancel: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    fn strict_state() -> AppState {
        let mut providers = ProviderRegistry::new();
        providers.register("stub", Arc::new(StubProvider), vec!["current-model".into()]);
        AppState::for_tests().with_providers(providers.with_default_model("retired-default"))
    }

    #[test]
    fn omitted_query_budget_means_no_cost_ceiling() {
        let request: QueryRequest = serde_json::from_value(serde_json::json!({
            "prompt": "hello"
        }))
        .expect("query request");
        assert_eq!(request.max_budget_usd, None);
    }

    #[tokio::test]
    async fn existing_rest_session_without_model_recovers_retired_model() {
        let state = strict_state();
        let session = state
            .db
            .create_session("retired-model", "/tmp")
            .await
            .expect("session");
        let request: QueryRequest = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "sessionId": session.id,
        }))
        .expect("query request");

        let resolved = resolve_session(&state, &request, true)
            .await
            .expect("resolve existing session");

        assert_eq!(resolved, session.id);
        assert_eq!(
            state
                .db
                .get_session(&session.id)
                .await
                .expect("read session")
                .expect("session exists")
                .model,
            "current-model"
        );
    }
}
