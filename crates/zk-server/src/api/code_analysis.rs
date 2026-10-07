//! Authorized frontend contracts for Python static analysis.
//! Saved Project/Session bindings authorize paths; request paths never grant access.

use crate::workspace::{failure, require_current_binding};
use crate::{
    error::ApiError,
    python::{Correlation, TransportError},
    state::AppState,
};
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, Method, StatusCode},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::PathBuf;
use utoipa::ToSchema;

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AnalysisScope {
    #[serde(default, alias = "project_id")]
    project_id: Option<String>,
    #[serde(default, alias = "session_id")]
    session_id: Option<String>,
    #[serde(default, alias = "project_root")]
    pub(crate) project_root: Option<String>,
    #[serde(default, alias = "request_id")]
    pub(crate) request_id: Option<String>,
}
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagramRequest {
    #[serde(flatten)]
    scope: AnalysisScope,
    #[serde(alias = "diagram_type")]
    diagram_type: String,
    target: String,
    #[serde(default = "diagram_depth")]
    depth: u8,
    #[serde(default)]
    options: Option<DiagramOptions>,
}
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagramOptions {
    #[serde(default = "diagram_depth")]
    depth: u8,
    #[serde(default, alias = "include_tests")]
    include_tests: bool,
}
const fn diagram_depth() -> u8 {
    3
}
const fn trace_depth() -> u8 {
    10
}
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct EndpointsRequest {
    #[serde(flatten)]
    scope: AnalysisScope,
    #[serde(default)]
    languages: Option<Vec<String>>,
}
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TraceRequest {
    #[serde(flatten)]
    scope: AnalysisScope,
    #[serde(alias = "entry_file")]
    entry_file: String,
    #[serde(alias = "entry_function")]
    entry_function: String,
    #[serde(default = "trace_depth", alias = "max_depth")]
    max_depth: u8,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all(serialize = "camelCase", deserialize = "snake_case"))]
#[schema(rename_all = "camelCase")]
pub(crate) struct DiagramResult {
    diagram_type: String,
    mermaid_syntax: String,
    confidence_score: f64,
    metadata: DiagramMetadata,
    warnings: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all(serialize = "camelCase", deserialize = "snake_case"))]
#[schema(rename_all = "camelCase")]
pub(crate) struct DiagramMetadata {
    nodes_count: u64,
    edges_count: u64,
    languages_analyzed: Vec<String>,
    analysis_time_ms: f64,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all(serialize = "camelCase", deserialize = "snake_case"))]
#[schema(rename_all = "camelCase")]
pub(crate) struct Endpoint {
    http_method: String,
    path: String,
    handler_function: String,
    handler_class: String,
    file_path: String,
    line_number: u64,
    language: String,
    parameters: Vec<BTreeMap<String, Value>>,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all(serialize = "camelCase", deserialize = "snake_case"))]
#[schema(rename_all = "camelCase")]
pub(crate) struct EndpointsResult {
    success: bool,
    endpoints: Vec<Endpoint>,
    total: usize,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all(serialize = "camelCase", deserialize = "snake_case"))]
#[schema(rename_all = "camelCase")]
pub(crate) struct PathNode {
    id: String,
    name: String,
    class_name: String,
    file_path: String,
    line_range: Vec<u64>,
    layer: String,
    node_type: String,
    annotations: Vec<String>,
    parameters: Vec<BTreeMap<String, Value>>,
    return_type: String,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all(serialize = "camelCase", deserialize = "snake_case"))]
#[schema(rename_all = "camelCase")]
pub(crate) struct PathEdge {
    source: String,
    target: String,
    call_type: String,
    parameter_mapping: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all(serialize = "camelCase", deserialize = "snake_case"))]
#[schema(rename_all = "camelCase")]
pub(crate) struct PathLayer {
    layer: String,
    node_count: u64,
    description: String,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all(serialize = "camelCase", deserialize = "snake_case"))]
#[schema(rename_all = "camelCase")]
pub(crate) struct TraceResult {
    nodes: Vec<PathNode>,
    edges: Vec<PathEdge>,
    layers: Vec<PathLayer>,
    entry_node: Option<String>,
    total_depth: u64,
    analysis_time_ms: f64,
    warnings: Vec<String>,
}

pub(crate) struct Binding {
    pub(crate) root: PathBuf,
    pub(crate) owner: String,
    session_id: Option<String>,
    pub(crate) request_id: String,
}

pub(crate) async fn bind(
    state: &AppState,
    headers: &HeaderMap,
    scope: &AnalysisScope,
) -> Result<Binding, ApiError> {
    let header_session = headers
        .get("x-session-id")
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| ApiError::validation("Invalid X-Session-Id"))
        })
        .transpose()?;
    if scope
        .session_id
        .as_ref()
        .zip(header_session.as_ref())
        .is_some_and(|(body, header)| body != header)
    {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "SESSION_CONTEXT_MISMATCH",
            "Session context does not match",
        ));
    }
    let session_id = scope.session_id.clone().or(header_session);
    let session_root = if let Some(id) = session_id.as_deref() {
        if state.db.is_merge_billing_session(id).await? {
            return Err(ApiError::session_not_found(id));
        }
        Some(
            state
                .db
                .get_session(id)
                .await?
                .ok_or_else(|| ApiError::session_not_found(id))?
                .working_dir,
        )
    } else {
        None
    };
    let project_root = if let Some(id) = scope.project_id.as_deref() {
        Some(
            state
                .db
                .get_project(id)
                .await?
                .ok_or_else(|| ApiError::not_found("PROJECT_NOT_FOUND", "Project is unavailable"))?
                .workspace_root,
        )
    } else {
        None
    };
    if session_root
        .as_ref()
        .zip(project_root.as_ref())
        .is_some_and(|(a, b)| a != b)
    {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "ANALYSIS_SCOPE_MISMATCH",
            "Project and Session have different workspaces",
        ));
    }
    let saved = project_root.or(session_root).ok_or_else(|| {
        ApiError::validation_with_code(
            "ANALYSIS_SCOPE_REQUIRED",
            "Select an authorized Project or Session",
        )
    })?;
    let root = validate_root(state, saved, scope.project_root.clone()).await?;
    let request_id = scope
        .request_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    if uuid::Uuid::parse_str(&request_id).is_err() {
        return Err(ApiError::validation("requestId must be a UUID"));
    }
    let owner = session_id.as_ref().map_or_else(
        || {
            format!(
                "project:{}",
                scope.project_id.as_deref().unwrap_or_default()
            )
        },
        |id| format!("session:{id}"),
    );
    Ok(Binding {
        root,
        owner,
        session_id,
        request_id,
    })
}

async fn validate_root(
    state: &AppState,
    saved: String,
    asserted: Option<String>,
) -> Result<PathBuf, ApiError> {
    let config = state.config.clone();
    tokio::task::spawn_blocking(move || {
        let root = require_current_binding(&config, &saved)?;
        if let Some(raw) = asserted.filter(|raw| !raw.trim().is_empty() && raw != ".") {
            let requested = PathBuf::from(raw);
            let candidate = if requested.is_absolute() {
                requested
            } else {
                root.join(requested)
            };
            let actual = std::fs::canonicalize(candidate).map_err(|_| {
                ApiError::validation_with_code(
                    "ANALYSIS_SCOPE_MISMATCH",
                    "Project root is unavailable",
                )
            })?;
            if actual != root {
                return Err(failure(
                    StatusCode::FORBIDDEN,
                    "ANALYSIS_SCOPE_MISMATCH",
                    "Analysis must use the authorized workspace root",
                ));
            }
        }
        Ok(root)
    })
    .await
    .map_err(|_| ApiError::internal())?
}

pub(crate) fn with_binding(mut body: Value, binding: &Binding) -> Result<Value, ApiError> {
    body["project_root"] = json!(
        binding
            .root
            .to_str()
            .ok_or_else(|| ApiError::validation("Workspace must be UTF-8"))?
    );
    body["request_id"] = json!(binding.request_id);
    body["analysis_owner"] = json!(binding.owner);
    Ok(body)
}

pub(crate) async fn forward(
    state: &AppState,
    path: &str,
    body: Value,
    binding: &Binding,
) -> Result<Value, ApiError> {
    if !state.config.python_enabled {
        return Err(failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "PYTHON_SERVICE_DISABLED",
            "Python service is disabled",
        ));
    }
    let response = state
        .python
        .forward(
            Method::POST,
            path,
            Some(body.to_string()),
            &Correlation::for_session(binding.session_id.as_deref()),
        )
        .await
        .map_err(|error| match error {
            TransportError::ReadTimeout => failure(
                StatusCode::GATEWAY_TIMEOUT,
                "PYTHON_SERVICE_TIMEOUT",
                "Python analysis timed out",
            ),
            _ => failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "PYTHON_SERVICE_UNAVAILABLE",
                "Python analysis is unavailable",
            ),
        })?;
    let value: Value = serde_json::from_str(&response.body).map_err(|_| invalid_response())?;
    if !response.status.is_success() {
        let message = value
            .get("detail")
            .and_then(Value::as_str)
            .unwrap_or("Python analysis failed");
        return Err(failure(response.status, "ANALYSIS_FAILED", message));
    }
    if value.get("success") == Some(&Value::Bool(false))
        || value.get("error").is_some_and(|e| !e.is_null())
    {
        return Err(invalid_response());
    }
    Ok(value)
}

pub(crate) fn invalid_response() -> ApiError {
    failure(
        StatusCode::BAD_GATEWAY,
        "ANALYSIS_RESPONSE_INVALID",
        "Python analysis returned an invalid result",
    )
}

// The pre-existing Python-shaped aliases remain snake_case on the wire. Only
// declared DTO fields are transformed; user parameter dictionaries are opaque.
fn python_shape(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(python_shape).collect()),
        Value::Object(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(key, value)| {
                    let opaque = matches!(key.as_str(), "parameters" | "parameterMapping");
                    let mut snake = String::new();
                    for ch in key.chars() {
                        if ch.is_ascii_uppercase() {
                            snake.push('_');
                            snake.push(ch.to_ascii_lowercase());
                        } else {
                            snake.push(ch);
                        }
                    }
                    (snake, if opaque { value } else { python_shape(value) })
                })
                .collect(),
        ),
        other => other,
    }
}

pub(crate) async fn python_diagram(
    state: State<AppState>,
    headers: HeaderMap,
    body: Json<DiagramRequest>,
) -> Result<Json<Value>, ApiError> {
    let Json(result) = generate_diagram(state, headers, body).await?;
    Ok(Json(python_shape(
        serde_json::to_value(result).map_err(|_| ApiError::internal())?,
    )))
}

pub(crate) async fn python_endpoints(
    state: State<AppState>,
    headers: HeaderMap,
    body: Json<EndpointsRequest>,
) -> Result<Json<Value>, ApiError> {
    let Json(result) = analyze_endpoints(state, headers, body).await?;
    Ok(Json(python_shape(
        serde_json::to_value(result).map_err(|_| ApiError::internal())?,
    )))
}

pub(crate) async fn python_trace(
    state: State<AppState>,
    headers: HeaderMap,
    body: Json<TraceRequest>,
) -> Result<Json<Value>, ApiError> {
    let Json(result) = trace_path(state, headers, body).await?;
    Ok(Json(
        json!({"success":true,"data":python_shape(serde_json::to_value(result).map_err(|_| ApiError::internal())?)}),
    ))
}

pub(crate) async fn recheck(
    state: &AppState,
    headers: &HeaderMap,
    scope: &AnalysisScope,
    original: &Binding,
) -> Result<(), ApiError> {
    let current = bind(state, headers, scope).await?;
    if current.root != original.root || current.owner != original.owner {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "ANALYSIS_SCOPE_CHANGED",
            "Analysis workspace authorization changed",
        ));
    }
    Ok(())
}

#[utoipa::path(post, path = "/api/code-diagrams/generate", tag = "analysis", request_body = DiagramRequest, responses((status = 200, body = DiagramResult), (status = 403, description = "Workspace boundary denied"), (status = 503, description = "Python unavailable")))]
pub(crate) async fn generate_diagram(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<DiagramRequest>,
) -> Result<Json<DiagramResult>, ApiError> {
    if !matches!(body.diagram_type.as_str(), "sequence" | "flowchart")
        || body.target.trim().is_empty()
    {
        return Err(ApiError::validation(
            "diagramType must be sequence or flowchart and target must not be blank",
        ));
    }
    let depth = body
        .options
        .as_ref()
        .map_or(body.depth, |options| options.depth);
    if !(1..=5).contains(&depth) {
        return Err(ApiError::validation("depth must be between 1 and 5"));
    }
    let binding = bind(&state, &headers, &body.scope).await?;
    let upstream = with_binding(
        json!({"diagram_type":body.diagram_type,"target":body.target,"options":{"depth":depth,"include_tests":body.options.is_some_and(|o| o.include_tests),"format":"mermaid"}}),
        &binding,
    )?;
    let result: DiagramResult = serde_json::from_value(
        forward(&state, "/api/analysis/generate-diagram", upstream, &binding).await?,
    )
    .map_err(|_| invalid_response())?;
    if result.mermaid_syntax.trim().is_empty() || !(0.0..=1.0).contains(&result.confidence_score) {
        return Err(invalid_response());
    }
    recheck(&state, &headers, &body.scope, &binding).await?;
    Ok(Json(result))
}

#[utoipa::path(post, path = "/api/code-path/endpoints", tag = "analysis", request_body = EndpointsRequest, responses((status = 200, body = EndpointsResult)))]
pub(crate) async fn analyze_endpoints(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<EndpointsRequest>,
) -> Result<Json<EndpointsResult>, ApiError> {
    if body.languages.as_ref().is_some_and(|languages| {
        languages.is_empty()
            || languages
                .iter()
                .any(|lang| !matches!(lang.as_str(), "python" | "java" | "typescript"))
    }) {
        return Err(ApiError::validation(
            "languages must contain python, java or typescript",
        ));
    }
    let binding = bind(&state, &headers, &body.scope).await?;
    let upstream = with_binding(json!({"languages":body.languages}), &binding)?;
    let result: EndpointsResult = serde_json::from_value(
        forward(&state, "/api/analysis/api-endpoints", upstream, &binding).await?,
    )
    .map_err(|_| invalid_response())?;
    if result.total != result.endpoints.len() {
        return Err(invalid_response());
    }
    recheck(&state, &headers, &body.scope, &binding).await?;
    Ok(Json(result))
}

#[utoipa::path(post, path = "/api/code-path/trace", tag = "analysis", request_body = TraceRequest, responses((status = 200, body = TraceResult)))]
pub(crate) async fn trace_path(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<TraceRequest>,
) -> Result<Json<TraceResult>, ApiError> {
    if !(1..=20).contains(&body.max_depth) || body.entry_function.trim().is_empty() {
        return Err(ApiError::validation(
            "maxDepth must be 1–20 and entryFunction must not be blank",
        ));
    }
    let binding = bind(&state, &headers, &body.scope).await?;
    let root = binding.root.clone();
    let entry = body.entry_file;
    let path =
        tokio::task::spawn_blocking(move || crate::file_access::resolve_within(&root, &entry))
            .await
            .map_err(|_| ApiError::internal())??;
    if !path.is_file() {
        return Err(ApiError::validation("entryFile must identify a file"));
    }
    let upstream = with_binding(
        json!({"entry_file":path,"entry_function":body.entry_function,"max_depth":body.max_depth}),
        &binding,
    )?;
    let value = forward(&state, "/api/analysis/code-path", upstream, &binding).await?;
    let result: TraceResult =
        serde_json::from_value(value.get("data").cloned().ok_or_else(invalid_response)?)
            .map_err(|_| invalid_response())?;
    recheck(&state, &headers, &body.scope, &binding).await?;
    Ok(Json(result))
}

#[utoipa::path(post, path = "/api/code-analysis/cancel", tag = "analysis", request_body = AnalysisScope, responses((status = 200, description = "Cancellation requested; requestId is required", body = Value), (status = 400, description = "Missing requestId or authorized scope"), (status = 403, description = "Scope mismatch")))]
pub(crate) async fn cancel_analysis(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(scope): Json<AnalysisScope>,
) -> Result<Json<Value>, ApiError> {
    if scope.request_id.is_none() {
        return Err(ApiError::validation("requestId is required"));
    }
    let binding = bind(&state, &headers, &scope).await?;
    Ok(Json(
        forward(
            &state,
            "/api/analysis/cancel",
            with_binding(json!({}), &binding)?,
            &binding,
        )
        .await?,
    ))
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ChangeImpactRequest {
    #[serde(flatten)]
    scope: AnalysisScope,
    #[serde(alias = "file_path")]
    file_path: String,
    #[serde(alias = "changed_lines")]
    changed_lines: Vec<u64>,
    #[serde(default = "diagram_depth")]
    depth: u8,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub(crate) struct ChangeImpactNode {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    name: String,
    file_path: String,
    line_range: Vec<u64>,
    impact_level: String,
    confidence: String,
    language: Option<String>,
}
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub(crate) struct ChangeImpactEdge {
    source: String,
    target: String,
    #[serde(rename = "type")]
    kind: String,
    weight: f64,
}
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub(crate) struct ChangeImpactSummary {
    direct_count: usize,
    indirect_count: usize,
    potential_count: usize,
    affected_apis: Vec<String>,
    affected_tasks: Vec<String>,
    #[serde(default)]
    confidence_breakdown: BTreeMap<String, usize>,
}
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub(crate) struct ChangeImpactResult {
    changed_file: String,
    changed_lines: Vec<u64>,
    impact_nodes: Vec<ChangeImpactNode>,
    impact_edges: Vec<ChangeImpactEdge>,
    summary: ChangeImpactSummary,
    truncated: bool,
    graph_stats: BTreeMap<String, u64>,
    analysis_kind: String,
    is_verification_evidence: bool,
}
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub(crate) struct ChangeImpactResponse {
    success: bool,
    data: ChangeImpactResult,
    elapsed_ms: f64,
}

#[utoipa::path(post,path="/api/analysis/change-impact",tag="analysis",request_body=ChangeImpactRequest,responses((status=200,body=ChangeImpactResponse),(status=403,description="Project boundary denied"),(status=503,description="Python unavailable")))]
pub(crate) async fn change_impact(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut body): Json<ChangeImpactRequest>,
) -> Result<Json<ChangeImpactResponse>, ApiError> {
    if !(1..=5).contains(&body.depth)
        || body.changed_lines.is_empty()
        || body.changed_lines.len() > 20_000
        || body
            .changed_lines
            .iter()
            .any(|&line| line == 0 || line > u64::from(u32::MAX))
    {
        return Err(ApiError::validation(
            "depth must be 1–5; changedLines must contain 1–20000 positive file line numbers",
        ));
    }
    body.changed_lines.sort_unstable();
    body.changed_lines.dedup();
    let binding = bind(&state, &headers, &body.scope).await?;
    let root = binding.root.clone();
    let file = body.file_path.clone();
    let path =
        tokio::task::spawn_blocking(move || crate::file_access::resolve_within(&root, &file))
            .await
            .map_err(|_| ApiError::internal())??;
    if !path.is_file() {
        return Err(ApiError::validation(
            "filePath must identify an existing source file",
        ));
    }
    if !matches!(
        path.extension().and_then(|v| v.to_str()),
        Some("py" | "java" | "ts" | "tsx" | "js" | "jsx")
    ) {
        return Err(ApiError::validation_with_code(
            "ANALYSIS_LANGUAGE_UNSUPPORTED",
            "Use the configured Rust LSP for Rust semantic requests; this graph analyzer supports Python, Java and TypeScript/JavaScript",
        ));
    }
    let upstream = with_binding(
        json!({"file_path":path,"changed_lines":body.changed_lines,"depth":body.depth}),
        &binding,
    )?;
    let value = forward(&state, "/api/analysis/change-impact", upstream, &binding).await?;
    let response: ChangeImpactResponse =
        serde_json::from_value(value).map_err(|_| invalid_response())?;
    let result = &response.data;
    let ids = result
        .impact_nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if !response.success
        || !response.elapsed_ms.is_finite()
        || response.elapsed_ms < 0.0
        || result.changed_file != path.to_string_lossy()
        || result.changed_lines != body.changed_lines
        || result.analysis_kind != "advisory"
        || result.is_verification_evidence
        || result.impact_nodes.len() > 20_000
        || result.impact_edges.len() > 100_000
        || ids.len() != result.impact_nodes.len()
        || result.impact_nodes.iter().any(|node| {
            node.id.is_empty()
                || !matches!(
                    node.impact_level.as_str(),
                    "direct" | "indirect" | "potential"
                )
                || !matches!(node.confidence.as_str(), "high" | "medium" | "low")
                || node.line_range.len() != 2
                || node.line_range[0] > node.line_range[1]
                || !std::path::Path::new(&node.file_path).starts_with(&binding.root)
        })
        || result.impact_edges.iter().any(|edge| {
            !ids.contains(edge.source.as_str())
                || !ids.contains(edge.target.as_str())
                || !edge.weight.is_finite()
        })
    {
        return Err(invalid_response());
    }
    recheck(&state, &headers, &body.scope, &binding).await?;
    Ok(Json(response))
}
