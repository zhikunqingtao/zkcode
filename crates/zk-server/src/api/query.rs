//! REST and real SSE adapters over the shared, exclusively reserved conversation service.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::Value;
use zk_authz::model::PermissionMode;
use zk_engine::{
    ConversationCancellation, ConversationLease, ConversationOutcome, ConversationRunOptions,
    ConversationService,
};

use crate::api::session::{resolve_model, resolve_new_session_model};
use crate::error::ApiError;
use crate::state::AppState;

#[path = "query_stream.rs"]
mod transport;
pub(crate) use transport::QueryStreams;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueryContext {
    stdin: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum UserRole {
    User,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryUserMessage {
    role: UserRole,
    content: String,
}

/// No advertised option may be silently discarded by serde.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct QueryRequest {
    #[serde(default)]
    prompt: String,
    model: Option<String>,
    system_prompt: Option<String>,
    append_system_prompt: Option<String>,
    permission_mode: Option<String>,
    max_turns: Option<u32>,
    max_budget_usd: Option<f64>,
    allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    disallowed_tools: Vec<String>,
    tools: Option<Vec<String>>,
    project_id: Option<String>,
    session_id: Option<String>,
    #[serde(default)]
    fork_session: bool,
    #[serde(default)]
    no_session: bool,
    working_directory: Option<Value>,
    timeout_seconds: Option<u64>,
    output_format: Option<String>,
    context: Option<QueryContext>,
    thinking: Option<String>,
    effort: Option<zk_llm::ReasoningEffort>,
    fallback_model: Option<String>,
    #[serde(default)]
    stop_sequences: Vec<String>,
    #[serde(default)]
    messages: Vec<QueryUserMessage>,
    include_partial_messages: Option<bool>,
    name: Option<String>,
    json_schema: Option<Value>,
    mcp_config: Option<zk_mcp::run_scope::RunMcpConfig>,
    request_id: Option<String>,
}

pub(crate) async fn sync_query(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Result<Response, ApiError> {
    let execution = prepare(&state, request, false).await?.start();
    execution.finish_http().await
}

pub(crate) async fn conversation_query(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Result<Response, ApiError> {
    let execution = prepare(&state, request, true).await?.start();
    execution.finish_http().await
}

pub(crate) async fn stream_query(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Result<axum::response::Response, ApiError> {
    let include_partial = request.include_partial_messages.unwrap_or(true);
    let prepared = prepare(&state, request, false).await?;
    // Subscription is installed while the lease is held, before any event can be emitted.
    let events = state.hub.subscribe_events(prepared.lease.session_id());
    state.query_streams.start(prepared, events, include_partial)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResumeQuery {
    after: Option<String>,
}

/// Read an already active execution; never create a session or rerun input.
pub(crate) async fn resume_query_stream(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<ResumeQuery>,
    headers: HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    let id = uuid::Uuid::parse_str(&id)
        .map_err(|_| {
            ApiError::validation_with_code("QUERY_REQUEST_ID_INVALID", "requestId must be a UUID")
        })?
        .to_string();
    let header = headers
        .get("last-event-id")
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()
        .map_err(|_| {
            ApiError::validation_with_code("QUERY_CURSOR_INVALID", "Last-Event-ID must be text")
        })?;
    if header
        .as_ref()
        .zip(query.after.as_ref())
        .is_some_and(|(header, query)| header != query)
    {
        return Err(ApiError::validation_with_code(
            "QUERY_CURSOR_INVALID",
            "Conflicting replay cursors",
        ));
    }
    let cursor = query.after.or(header).ok_or_else(|| {
        ApiError::validation_with_code(
            "QUERY_CURSOR_REQUIRED",
            "Resume requires the last received event ID",
        )
    })?;
    state.query_streams.resume(&id, &cursor)
}

/// An explicit CLI cancellation request. The response is an acknowledgement of
/// the request, not a claim that cleanup or its durable transition has completed.
pub(crate) async fn cancel_query(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = uuid::Uuid::parse_str(&id)
        .map_err(|_| {
            ApiError::validation_with_code("QUERY_REQUEST_ID_INVALID", "requestId must be a UUID")
        })?
        .to_string();
    let service = state.conversation().ok_or_else(|| {
        ApiError::feature_not_ready("Query", "ConversationService is unavailable")
    })?;
    let cancellation = service.request_cancellation(&id);
    let active = service.cancel_request(&id).map_err(|code| ApiError {
        status: axum::http::StatusCode::SERVICE_UNAVAILABLE,
        code: code.into(),
        message: "Request cancellation could not be registered".into(),
    })?;
    // Stop the live execution first even if persisting cancellation later fails.
    let request_persistence_pending = state.db.cancel_query_request(&id).await.is_err();
    let run = if let Some(run_id) = cancellation.and_then(|handle| handle.run_id()) {
        state.db.find_run_by_id(&run_id).await.ok().flatten()
    } else {
        None
    };
    let persistence_pending = request_persistence_pending
        || (active
            && run.as_ref().is_none_or(|run| {
                run.requested_exit_reason.is_none() && run.finished_at.is_none()
            }));
    let cleanup_confirmed = run
        .as_ref()
        .is_some_and(|run| run.cleanup_status == "confirmed");
    Ok(Json(
        serde_json::json!({"requestId":id,"stopRequested":active,"admissionBlocked":true,"persistencePending":persistence_pending,"cleanupConfirmed":cleanup_confirmed}),
    ))
}

struct PreparedQuery {
    service: Arc<ConversationService>,
    lease: ConversationLease,
    options: ConversationRunOptions,
    prompt: String,
    request_id: String,
    deadline_at_ms: i64,
    registration: RequestRegistration,
    content_lease: Option<zk_db::content::EphemeralContentLease>,
}

struct QueryExecution {
    completion: tokio::sync::oneshot::Receiver<ConversationOutcome>,
    cancellation: ConversationCancellation,
    finished: bool,
    request_id: String,
}

impl Drop for QueryExecution {
    fn drop(&mut self) {
        if !self.finished {
            self.cancellation.cancel("QUERY_TRANSPORT_CLOSED");
        }
    }
}

impl QueryExecution {
    async fn finish_http(mut self) -> Result<Response, ApiError> {
        tokio::select! {
            biased;
            outcome = &mut self.completion => {
                let outcome = outcome.map_err(|_| ApiError::internal())?;
                self.finished = true;
                Ok(Json(outcome).into_response())
            }
            () = self.cancellation.wait_cancellation_pending() => {
                // Only this transport is done. The spawned worker still owns the
                // request registration, session lease, content and cleanup duty.
                self.finished = true;
                Ok((axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
                    "code":"CANCELLATION_PERSISTENCE_PENDING",
                    "message":"HTTP 等待已结束；原执行仍在保存取消状态或清理资源，请勿重发请求。",
                    "queryRequestId":self.request_id,
                    "sessionId":self.cancellation.session_id(),
                    "runId":self.cancellation.run_id(),
                    "terminal":false,
                }))).into_response())
            }
        }
    }
}

struct RequestRegistration {
    service: Arc<ConversationService>,
    id: String,
}

impl Drop for RequestRegistration {
    fn drop(&mut self) {
        self.service.unregister_request(&self.id);
    }
}

impl PreparedQuery {
    fn start(self) -> QueryExecution {
        let request_id = self.request_id.clone();
        let cancellation = self.service.cancellation(&self.lease);
        let worker_cancel = cancellation.clone();
        let (tx, completion) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _registration = self.registration;
            // Content stays alive through actual execution, cleanup and outcome
            // materialization; dropping this guard expires all attached bodies.
            let _content_lease = self.content_lease;
            let execution = self
                .service
                .execute_reserved(self.lease, self.prompt, self.options);
            tokio::pin!(execution);
            let outcome = tokio::select! {
                outcome = &mut execution => outcome,
                () = tokio::time::sleep(Duration::from_millis(
                    self.deadline_at_ms.saturating_sub(zk_db::time::now_millis()).max(0).unsigned_abs(),
                )) => {
                    worker_cancel.cancel_due_to_deadline();
                    // The engine retains cleanup and terminal persistence. Its actual
                    // result wins if completion or a user stop beat this timer.
                    let mut outcome = execution.await;
                    if outcome.run_id.is_none()
                        && outcome.error.as_deref() == Some("QUERY_NOT_STARTED")
                        && worker_cancel.deadline_requested()
                    {
                        outcome.error = Some("QUERY_TIMEOUT".to_owned());
                    }
                    outcome
                }
            };
            let mut outcome = outcome;
            if matches!(
                outcome.error.as_deref(),
                Some("timeout" | "TIMEOUT" | "TASK_DEADLINE_EXCEEDED")
            ) {
                outcome.error = Some("QUERY_TIMEOUT".to_owned());
            }
            let _ = tx.send(outcome);
        });
        QueryExecution {
            completion,
            cancellation,
            finished: false,
            request_id,
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "Keep the request claim, retention lease, session reservation and preference mutation in their required order"
)]
async fn prepare(
    state: &AppState,
    mut request: QueryRequest,
    require_existing_session: bool,
) -> Result<PreparedQuery, ApiError> {
    let accepted_at_ms = zk_db::time::now_millis();
    validate_request(state, &request)?;
    let request_id = request
        .request_id
        .as_deref()
        .map(uuid::Uuid::parse_str)
        .transpose()
        .map_err(|_| {
            ApiError::validation_with_code("QUERY_REQUEST_ID_INVALID", "requestId must be a UUID")
        })?
        .unwrap_or_else(uuid::Uuid::new_v4)
        .to_string();
    request.request_id = Some(request_id.clone());
    let structured_output = request
        .json_schema
        .clone()
        .map(zk_engine::structured_output::StructuredOutputContract::new)
        .transpose()
        .map_err(|code| {
            ApiError::validation_with_code(
                code,
                "jsonSchema must be a bounded valid local JSON Schema",
            )
        })?
        .map(Arc::new);
    let requested_mode = request
        .permission_mode
        .as_deref()
        .map(|mode| {
            PermissionMode::parse(mode).ok_or_else(|| {
                ApiError::validation_with_code(
                    "INVALID_PERMISSION_MODE",
                    "Query permissionMode is invalid",
                )
            })
        })
        .transpose()?;
    if require_existing_session && request.session_id.is_none() {
        return Err(ApiError::validation_with_code(
            "QUERY_SESSION_REQUIRED",
            "conversation mode requires sessionId",
        ));
    }
    if request.session_id.is_none() && request.project_id.is_none() {
        return Err(ApiError::validation_with_code(
            "QUERY_SCOPE_REQUIRED",
            "Query requires an authorized projectId or sessionId",
        ));
    }
    let service = state.conversation().ok_or_else(|| {
        ApiError::feature_not_ready("Query", "the shared ConversationService is wired")
    })?;
    service
        .claim_request(&request_id)
        .map_err(|code| ApiError {
            status: axum::http::StatusCode::CONFLICT,
            code: code.into(),
            message: "This request cannot start again".into(),
        })?;
    let registration = RequestRegistration {
        service: Arc::clone(&service),
        id: request_id.clone(),
    };
    state
        .db
        .claim_query_request(&request_id)
        .await
        .map_err(query_claim_error)?;
    let (session_id, content_lease) = if request.no_session {
        let project = state
            .db
            .get_project(request.project_id.as_deref().ok_or_else(|| {
                ApiError::validation_with_code(
                    "QUERY_SCOPE_REQUIRED",
                    "Temporary Query requires an authorized projectId",
                )
            })?)
            .await?
            .ok_or_else(|| ApiError::not_found("PROJECT_NOT_FOUND", "Project not found"))?;
        let workspace =
            crate::workspace::require_current_binding(&state.config, &project.workspace_root)?;
        let model = resolve_new_session_model(state, request.model.as_deref()).await?;
        let (session, lease) = state
            .db
            .create_ephemeral_session(
                &model,
                &workspace.to_string_lossy(),
                requested_mode.unwrap_or(PermissionMode::DontAsk).as_str(),
            )
            .await?;
        (session, Some(lease))
    } else {
        (
            resolve_session(state, &request, require_existing_session).await?,
            None,
        )
    };
    state
        .db
        .bind_query_request(&request_id, &session_id)
        .await
        .map_err(query_claim_error)?;
    let lease = service.reserve(&session_id).ok_or_else(|| ApiError {
        status: axum::http::StatusCode::CONFLICT,
        code: "QUERY_BUSY".into(),
        message: "The session already has an active query or mutation".into(),
    })?;
    service
        .register_request(&request_id, &lease)
        .map_err(|code| ApiError {
            status: axum::http::StatusCode::CONFLICT,
            code: code.into(),
            message: "This request cannot start again".into(),
        })?;
    let session = state
        .db
        .get_session(&session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&session_id))?;
    if (request.session_id.is_some() || state.authz.modes.has_explicit_mode(&session_id))
        && requested_mode.is_some_and(|mode| mode != state.authz.modes.get_mode(&session_id))
    {
        return Err(ApiError { status: axum::http::StatusCode::CONFLICT, code: "PERMISSION_MODE_CONFLICT".into(), message: "Resumed queries inherit the session permission mode; change it explicitly in the session first".into() });
    }
    let ValidatedModelOptions {
        model,
        thinking,
        fallback_models,
    } = validate_model_options(state, &request, &session.model)?;
    if request.session_id.is_none() && !state.authz.modes.has_explicit_mode(&session_id) {
        state
            .authz
            .modes
            .set_mode(
                &session_id,
                requested_mode.unwrap_or(PermissionMode::DontAsk),
            )
            .await?;
    }
    if model != session.model {
        state.db.update_session_model(&session_id, &model).await?;
    }
    if let Some(name) = request.name.as_deref() {
        state.db.update_session_title(&session_id, name).await?;
    }
    let deadline = request
        .timeout_seconds
        .map_or(
            state.config.root_task_budget_policy.deadline,
            Duration::from_secs,
        )
        .min(state.config.root_task_budget_policy.deadline);
    let notify_session_start = request.session_id.is_none() || request.fork_session;
    let deadline_at_ms =
        accepted_at_ms.saturating_add(i64::try_from(deadline.as_millis()).unwrap_or(i64::MAX));
    let (mut options, prompt) = build_run_options(
        state,
        request,
        thinking,
        fallback_models,
        structured_output,
        deadline,
    )?;
    options.notify_session_start = notify_session_start;
    options.deadline_at_ms = Some(deadline_at_ms);
    Ok(PreparedQuery {
        service,
        lease,
        options,
        prompt,
        request_id,
        deadline_at_ms,
        registration,
        content_lease,
    })
}

struct ValidatedModelOptions {
    model: String,
    thinking: Option<zk_llm::ThinkingMode>,
    fallback_models: Option<Vec<String>>,
}

fn validate_model_options(
    state: &AppState,
    request: &QueryRequest,
    saved_model: &str,
) -> Result<ValidatedModelOptions, ApiError> {
    // Resolve and validate every model option before mutating the saved preference.
    let model =
        resolve_model(state, request.model.as_deref().or(Some(saved_model))).or_else(|error| {
            if request.model.is_none() {
                resolve_model(state, None)
            } else {
                Err(error)
            }
        })?;
    let thinking = request
        .thinking
        .as_deref()
        .map(parse_thinking_mode)
        .transpose()?;
    if thinking.is_some_and(zk_llm::ThinkingMode::requires_support)
        && !zk_llm::capabilities_for(&model).supports_thinking
    {
        return Err(ApiError::validation_with_code(
            "QUERY_THINKING_UNSUPPORTED",
            "The selected model does not support the requested thinking mode",
        ));
    }
    let registry = state.providers.load();
    let mut option_check = zk_llm::ChatRequest::new(&model);
    option_check.thinking =
        thinking.unwrap_or(if zk_llm::capabilities_for(&model).supports_thinking {
            zk_llm::ThinkingMode::Adaptive
        } else {
            zk_llm::ThinkingMode::Disabled
        });
    option_check.reasoning_effort = request.effort;
    option_check
        .stop_sequences
        .clone_from(&request.stop_sequences);
    zk_llm::ChatProvider::validate_request_options(registry.as_ref(), &option_check).map_err(
        |error| ApiError::validation_with_code("QUERY_MODEL_OPTIONS_INVALID", &error.to_string()),
    )?;
    let fallback_models = request
        .fallback_model
        .as_deref()
        .map(|model| resolve_model(state, Some(model)).map(|model| vec![model]))
        .transpose()?;
    Ok(ValidatedModelOptions {
        model,
        thinking,
        fallback_models,
    })
}

fn build_run_options(
    state: &AppState,
    request: QueryRequest,
    thinking: Option<zk_llm::ThinkingMode>,
    fallback_models: Option<Vec<String>>,
    structured_output: Option<Arc<zk_engine::structured_output::StructuredOutputContract>>,
    deadline: Duration,
) -> Result<(ConversationRunOptions, String), ApiError> {
    let mut allowed_tools = request
        .tools
        .map(|tools| tools.into_iter().collect::<HashSet<_>>());
    if let Some(allowed) = request.allowed_tools {
        let allow: HashSet<_> = allowed.into_iter().collect();
        allowed_tools = Some(allowed_tools.map_or_else(
            || allow.clone(),
            |tools| tools.intersection(&allow).cloned().collect(),
        ));
    }
    let mut options = ConversationRunOptions {
        model_override: None,
        max_turns: request.max_turns.map_or_else(
            || ConversationRunOptions::default().max_turns,
            |value| value as usize,
        ),
        system_prompt: request.system_prompt,
        append_system_prompt: request.append_system_prompt,
        allowed_tools,
        disallowed_tools: request.disallowed_tools.into_iter().collect(),
        thinking,
        reasoning_effort: request.effort,
        fallback_models,
        stop_sequences: request.stop_sequences,
        input_messages: Vec::new(),
        structured_output,
        tool_scope_factory: request.mcp_config.map(|config| {
            Arc::new(zk_mcp::run_scope::RunMcpScopeFactory::new(
                config,
                state.mcp(),
            )) as Arc<dyn zk_tools::RunToolScopeFactory>
        }),
        token_budget: None,
        cost_budget_nanos_usd: request.max_budget_usd.map(usd_to_nanos).transpose()?,
        deadline: Some(deadline),
        deadline_at_ms: None,
        notify_session_start: false,
    };
    let mut prompt = request.prompt;
    if let Some(stdin) = request
        .context
        .and_then(|context| context.stdin)
        .filter(|stdin| !stdin.is_empty())
    {
        if !prompt.is_empty() {
            prompt.push_str("\n\n");
        }
        prompt.push_str(&stdin);
    }
    let mut messages: Vec<String> = request
        .messages
        .into_iter()
        .map(|message| message.content)
        .collect();
    if !prompt.is_empty() {
        messages.push(prompt);
    }
    prompt = messages.pop().ok_or_else(|| {
        ApiError::validation_with_code("QUERY_PROMPT_REQUIRED", "Query requires a user message")
    })?;
    options.input_messages = messages;
    Ok((options, prompt))
}

fn query_claim_error(error: zk_db::DbError) -> ApiError {
    match error {
        zk_db::DbError::Conflict(code) => ApiError {
            status: axum::http::StatusCode::CONFLICT,
            code,
            message: "This request cannot start again".into(),
        },
        error => error.into(),
    }
}

fn usd_to_nanos(usd: f64) -> Result<i64, ApiError> {
    let nanos = usd * 1_000_000_000.0;
    // Reject 2^63 itself: converting i64::MAX to f64 rounds up to this boundary.
    if !nanos.is_finite() || !(1.0..9_223_372_036_854_775_808.0).contains(&nanos) {
        return Err(ApiError::validation_with_code(
            "QUERY_BUDGET_INVALID",
            "maxBudgetUsd cannot be represented safely",
        ));
    }
    #[allow(clippy::cast_possible_truncation)]
    Ok(nanos.floor() as i64)
}

fn validate_request(state: &AppState, request: &QueryRequest) -> Result<(), ApiError> {
    if request.no_session && (request.session_id.is_some() || request.fork_session) {
        return Err(ApiError::validation_with_code(
            "EPHEMERAL_OPERATION_UNSUPPORTED",
            "Temporary execution cannot resume or fork a session",
        ));
    }
    if request.fork_session && request.session_id.is_none() {
        return Err(ApiError::validation_with_code(
            "QUERY_FORK_SOURCE_REQUIRED",
            "forkSession requires a source sessionId",
        ));
    }
    validate_input(request)?;
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
    if request.max_turns == Some(0) {
        return Err(ApiError::validation_with_code(
            "QUERY_MAX_TURNS_INVALID",
            "maxTurns must be positive",
        ));
    }
    if let Some(budget) = request.max_budget_usd {
        usd_to_nanos(budget)?;
    }
    if request.timeout_seconds == Some(0) {
        return Err(ApiError::validation_with_code(
            "QUERY_TIMEOUT_INVALID",
            "timeoutSeconds must be positive",
        ));
    }
    if request
        .request_id
        .as_deref()
        .is_some_and(|id| uuid::Uuid::parse_str(id).is_err())
    {
        return Err(ApiError::validation_with_code(
            "QUERY_REQUEST_ID_INVALID",
            "requestId must be a UUID",
        ));
    }
    let known = state.tools().names();
    for tool in request
        .allowed_tools
        .iter()
        .flatten()
        .chain(&request.disallowed_tools)
        .chain(request.tools.iter().flatten())
    {
        if !known.contains(tool)
            && tool != "LSP"
            && !request
                .mcp_config
                .as_ref()
                .is_some_and(|config| config.may_define_tool(tool))
        {
            return Err(ApiError::validation_with_code(
                "QUERY_TOOL_UNKNOWN",
                &format!("Unknown query tool: {tool}"),
            ));
        }
    }
    if request
        .output_format
        .as_deref()
        .is_some_and(|format| !matches!(format, "text" | "json" | "stream-json"))
    {
        return Err(ApiError::validation_with_code(
            "QUERY_OUTPUT_FORMAT_INVALID",
            "outputFormat must be text, json, or stream-json",
        ));
    }
    if let Some(mode) = request.thinking.as_deref() {
        parse_thinking_mode(mode)?;
    }
    Ok(())
}

fn validate_input(request: &QueryRequest) -> Result<(), ApiError> {
    if request.messages.is_empty()
        && request.prompt.trim().is_empty()
        && request
            .context
            .as_ref()
            .and_then(|context| context.stdin.as_deref())
            .is_none_or(|stdin| stdin.trim().is_empty())
    {
        return Err(ApiError::validation_with_code(
            "QUERY_PROMPT_REQUIRED",
            "Query requires a prompt or stdin content",
        ));
    }
    let input_bytes = request
        .prompt
        .len()
        .saturating_add(
            request
                .context
                .as_ref()
                .and_then(|context| context.stdin.as_ref())
                .map_or(0, String::len),
        )
        .saturating_add(
            request
                .messages
                .iter()
                .map(|message| message.content.len())
                .sum::<usize>(),
        );
    if request.messages.len() > 256
        || input_bytes > 1024 * 1024
        || request.messages.iter().any(|message| {
            let UserRole::User = message.role;
            message.content.trim().is_empty()
        })
    {
        return Err(ApiError::validation_with_code(
            "QUERY_INPUT_INVALID",
            "Query input must contain up to 256 non-empty user messages and at most 1 MiB of text",
        ));
    }
    if request
        .name
        .as_ref()
        .is_some_and(|name| name.trim().is_empty() || name.chars().count() > 200)
    {
        return Err(ApiError::validation_with_code(
            "QUERY_NAME_INVALID",
            "name must contain 1–200 characters",
        ));
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
        state.db.require_conversation_session(session_id).await?;
        if state.db.is_merge_billing_session(session_id).await? {
            return Err(ApiError::session_not_found(session_id));
        }
        if state.db.session_retention(session_id).await?
            != zk_db::content::ContentRetention::Persistent
        {
            return Err(ApiError::validation_with_code(
                "EPHEMERAL_OPERATION_UNSUPPORTED",
                "An ephemeral execution cannot be resumed or forked",
            ));
        }
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
        if request.fork_session {
            let service = state.conversation().ok_or_else(|| {
                ApiError::feature_not_ready("Query", "ConversationService is unavailable")
            })?;
            let _source_lease = service.reserve(session_id).ok_or_else(|| ApiError {
                status: axum::http::StatusCode::CONFLICT,
                code: "QUERY_BUSY".into(),
                message: "The fork source has an active query or mutation".into(),
            })?;
            let request_id = request
                .request_id
                .as_deref()
                .ok_or_else(ApiError::internal)?;
            let fork = state
                .db
                .fork_session(
                    request_id,
                    zk_db::SessionForkRequest {
                        source_session_id: session_id.to_owned(),
                        title: request.name.clone(),
                    },
                )
                .await?;
            return Ok(fork.session_id);
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
    let model = resolve_new_session_model(state, request.model.as_deref()).await?;
    Ok(state
        .db
        .create_session_with_permission(
            &uuid::Uuid::new_v4().to_string(),
            &model,
            &project.workspace_root,
            Some(
                request
                    .permission_mode
                    .as_deref()
                    .and_then(PermissionMode::parse)
                    .unwrap_or(PermissionMode::DontAsk)
                    .as_str(),
            ),
        )
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
        // Scope lookup is read-only; model recovery runs only under the query lease.
        let mut detail = state.db.get_session(&session.id).await.unwrap().unwrap();
        assert_eq!(detail.model, "retired-model");
        crate::api::session::recover_retired_session_model(&state, &mut detail)
            .await
            .unwrap();
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
