//! Reverse MCP JSON-RPC server. All tool calls share the production registry and admission.

use std::sync::{Arc, LazyLock};

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use zk_authz::model::PermissionMode;
use zk_engine::ObservabilityEvent;
use zk_engine::admission::{Admission, AdmissionRequest, ToolAdmission};
use zk_engine::{HookContext, PreHookDecision};
use zk_tools::{CallEnv, ToolEvent, ToolExecutor};

use crate::authz::EngineAdmission;
use crate::mcp_tools::{MAX_RESOURCE_BYTES, ResourcePolicyError, validate_declared_resource};
use crate::state::AppState;

const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_LISTED_RESOURCES: usize = 256;
const SERVER_NAME: &str = "zkcode";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
static CONCURRENCY: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(16));
static TOOL_EXECUTOR: LazyLock<ToolExecutor> = LazyLock::new(ToolExecutor::new);

/// Handle one JSON-RPC request or notification.
pub(crate) async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() > MAX_REQUEST_BYTES {
        return json_response(rpc_error(
            &Value::Null,
            -32600,
            "Request exceeds 1 MiB limit",
            None,
        ));
    }
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return json_response(rpc_error(&Value::Null, -32700, "Parse error", None)),
    };
    let Some(object) = value.as_object() else {
        return json_response(rpc_error(&Value::Null, -32600, "Invalid request", None));
    };
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        let invalid_id = object.get("id").cloned().unwrap_or(Value::Null);
        return json_response(rpc_error(&invalid_id, -32600, "jsonrpc must be 2.0", None));
    }
    let id = object.get("id").cloned();
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        let invalid_id = id.unwrap_or(Value::Null);
        return json_response(rpc_error(&invalid_id, -32600, "method is required", None));
    };
    let params = object.get("params").cloned().unwrap_or_else(|| json!({}));
    if id.is_none() {
        return notification(method);
    }
    let id = id.unwrap_or(Value::Null);
    let Ok(permit) = CONCURRENCY.try_acquire() else {
        return json_response(rpc_error(&id, -32001, "MCP server is busy", None));
    };
    let result = dispatch(&state, &headers, method, params).await;
    drop(permit);
    json_response(match result {
        Ok(result) => rpc_ok(&id, &result),
        Err(error) => rpc_error(&id, error.rpc_code, &error.message, error.data.as_ref()),
    })
}

fn notification(method: &str) -> Response {
    match method {
        "notifications/initialized" | "notifications/cancelled" => {
            StatusCode::ACCEPTED.into_response()
        }
        _ => StatusCode::ACCEPTED.into_response(),
    }
}

async fn dispatch(
    state: &AppState,
    headers: &HeaderMap,
    method: &str,
    params: Value,
) -> Result<Value, RpcFailure> {
    match method {
        "initialize" => Ok(json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {
                "tools": {"listChanged": true},
                "resources": {"listChanged": true}
            },
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION}
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({
            "tools": state
                .tools()
                .specs()
                .into_iter()
                .map(|spec| json!({
                    "name": spec.name,
                    "description": spec.description,
                    "inputSchema": spec.parameters,
                }))
                .collect::<Vec<_>>()
        })),
        "tools/call" => call_tool(state, headers, &params).await,
        "resources/list" => list_resources(state).await,
        "resources/read" => read_resource(state, &params).await,
        _ => Err(RpcFailure::new(
            -32601,
            format!("Method not found: {method}"),
        )),
    }
}

#[allow(clippy::too_many_lines)] // validation, admission, execution and telemetry are one RPC boundary
async fn call_tool(
    state: &AppState,
    headers: &HeaderMap,
    params: &Value,
) -> Result<Value, RpcFailure> {
    let object = params
        .as_object()
        .ok_or_else(|| RpcFailure::new(-32602, "tools/call params must be an object"))?;
    if object.contains_key("workingDirectory") {
        return Err(RpcFailure::new(
            -32602,
            "workingDirectory is not accepted; use the authorized session workspace",
        ));
    }
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .ok_or_else(|| RpcFailure::new(-32602, "tool name is required"))?;
    let input = object
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let metadata = object.get("_meta").and_then(Value::as_object);
    if metadata.is_some_and(|meta| meta.contains_key("workingDirectory")) {
        return Err(RpcFailure::new(
            -32602,
            "workingDirectory is not accepted; use the authorized session workspace",
        ));
    }
    let session_id = header_or_meta(headers, "x-session-id", metadata, "sessionId")
        .ok_or_else(|| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_REQUIRED: sessionId"))?;
    let run_id = header_or_meta(headers, "x-run-id", metadata, "runId")
        .ok_or_else(|| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_REQUIRED: runId"))?;
    let session = state
        .db
        .get_session(&session_id)
        .await
        .map_err(|_| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_UNAVAILABLE: session"))?
        .ok_or_else(|| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_INVALID: session"))?;
    let run = state
        .db
        .find_run_by_id(&run_id)
        .await
        .map_err(|_| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_UNAVAILABLE: run"))?
        .ok_or_else(|| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_INVALID: run"))?;
    if run.session_id != session_id {
        return Err(RpcFailure::new(
            -32003,
            "MCP_TOOL_CONTEXT_INVALID: run/session mismatch",
        ));
    }
    let workspace = std::fs::canonicalize(&session.working_dir)
        .map_err(|_| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_INVALID: workspace"))?;
    let workspace_text = workspace.to_string_lossy().into_owned();
    let hook_context = HookContext::new()
        .with_tool(name)
        .with_session(session_id.clone())
        .with_working_dir(workspace_text.clone());
    let pre_input = match state.hooks.evaluate_pre_tool(&hook_context, &input).await {
        PreHookDecision::Continue { input } => input,
        PreHookDecision::Deny { code, message } => {
            let mut telemetry = ObservabilityEvent::new("mcp", "tool_call", "denied");
            telemetry.session_id = Some(session_id.clone());
            telemetry.run_id = Some(run_id.clone());
            telemetry.security_audit = true;
            telemetry
                .attributes
                .insert("tool".to_owned(), Value::String(name.to_owned()));
            telemetry
                .attributes
                .insert("hook_code".to_owned(), Value::String(code.clone()));
            state.observability.record(telemetry);
            return Err(RpcFailure::with_data(
                -32003,
                message,
                json!({"code": code, "mode": PermissionMode::DontAsk.as_str()}),
            ));
        }
    };
    let started = std::time::Instant::now();
    let mut telemetry = ObservabilityEvent::new("mcp", "tool_call", "running");
    telemetry.session_id = Some(session_id.clone());
    telemetry.run_id = Some(run_id.clone());
    telemetry
        .attributes
        .insert("tool".to_owned(), Value::String(name.to_owned()));
    state.observability.record(telemetry);
    let tools = state.tools();
    let tool = tools
        .get(name)
        .ok_or_else(|| RpcFailure::new(-32601, format!("Tool not found: {name}")))?;
    let admission = EngineAdmission::new_dont_ask(state.authz.clone(), Arc::clone(&tools));
    let tool_use_id = format!("mcp_{}", uuid::Uuid::new_v4());
    let outcome = admission
        .admit(AdmissionRequest {
            session_id: &session_id,
            run_id: &run_id,
            tool_use_id: &tool_use_id,
            tool_name: name,
            input: &pre_input,
            working_directory: Some(&workspace_text),
        })
        .await;
    let execution_input = match outcome {
        Admission::Allow { execution_input } => execution_input,
        Admission::Denied { code, message } | Admission::Failed { code, message } => {
            let mut telemetry = ObservabilityEvent::new("mcp", "tool_call", "denied");
            telemetry.session_id = Some(session_id.clone());
            telemetry.run_id = Some(run_id.clone());
            telemetry.duration_ms =
                Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
            telemetry.security_audit = true;
            telemetry
                .attributes
                .insert("tool".to_owned(), Value::String(name.to_owned()));
            state.observability.record(telemetry);
            return Err(RpcFailure::with_data(
                -32003,
                message,
                json!({"code": code, "mode": PermissionMode::DontAsk.as_str()}),
            ));
        }
    };
    let cancel = CancellationToken::new();
    let env = CallEnv::new()
        .with_working_dir(workspace)
        .with_session_id(session_id.clone())
        .with_run_id(run_id.clone());
    let mut events = TOOL_EXECUTOR.spawn_call_in(tool, tool_use_id, execution_input, &cancel, env);
    let output = loop {
        match events.recv().await {
            Some(ToolEvent::Progress { .. }) => {}
            Some(ToolEvent::Finished { output, .. }) => break output,
            None => {
                return Err(RpcFailure::new(-32001, "Tool execution interrupted"));
            }
        }
    };
    if output.is_error
        && output
            .content
            .starts_with("Tool execution timed out after ")
    {
        let mut telemetry = ObservabilityEvent::new("mcp", "tool_call", "timeout");
        telemetry.session_id = Some(session_id.clone());
        telemetry.run_id = Some(run_id.clone());
        telemetry.duration_ms =
            Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
        telemetry
            .attributes
            .insert("tool".to_owned(), Value::String(name.to_owned()));
        state.observability.record(telemetry);
        return Err(RpcFailure::new(-32001, "Tool execution timed out"));
    }
    let mut telemetry = ObservabilityEvent::new(
        "mcp",
        "tool_call",
        if output.is_error { "error" } else { "ok" },
    );
    telemetry.session_id = Some(session_id);
    telemetry.run_id = Some(run_id);
    telemetry.duration_ms = Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
    telemetry
        .attributes
        .insert("tool".to_owned(), Value::String(name.to_owned()));
    state.observability.record(telemetry);
    Ok(json!({
        "content": [{"type": "text", "text": output.content}],
        "isError": output.is_error,
        "structuredContent": output.metadata,
    }))
}

async fn list_resources(state: &AppState) -> Result<Value, RpcFailure> {
    let mut resources = Vec::new();
    for connection in state.mcp().connected_servers() {
        for resource in connection.discover_resources().await {
            if resources.len() >= MAX_LISTED_RESOURCES {
                break;
            }
            resources.push(json!({
                "uri": resource.uri,
                "name": resource.name,
                "description": resource.description,
                "mimeType": resource.mime_type,
                "_meta": {"server": connection.name()}
            }));
        }
    }
    Ok(json!({"resources": resources}))
}

async fn read_resource(state: &AppState, params: &Value) -> Result<Value, RpcFailure> {
    let object = params
        .as_object()
        .ok_or_else(|| RpcFailure::new(-32602, "resources/read params must be an object"))?;
    let uri = object
        .get("uri")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcFailure::new(-32602, "resource uri is required"))?;
    let server = object
        .get("_meta")
        .and_then(Value::as_object)
        .and_then(|meta| meta.get("server"))
        .and_then(Value::as_str)
        .ok_or_else(|| RpcFailure::new(-32602, "resource _meta.server is required"))?;
    let connection = state
        .mcp()
        .get_connection(server)
        .ok_or_else(|| RpcFailure::new(-32004, "MCP resource server not found"))?;
    let resources = connection.discover_resources().await;
    let declared = validate_declared_resource(uri, &resources).map_err(|error| match error {
        ResourcePolicyError::UriRejected => {
            RpcFailure::new(-32602, "resource URI scheme or length rejected")
        }
        ResourcePolicyError::NotDeclared => RpcFailure::new(-32004, "MCP resource is not declared"),
        ResourcePolicyError::MimeRejected => {
            RpcFailure::new(-32004, "MCP resource MIME type rejected")
        }
    })?;
    let content = connection
        .read_resource(uri)
        .await
        .map_err(|error| RpcFailure::new(-32603, format!("Resource read failed: {error}")))?;
    if content.len() > MAX_RESOURCE_BYTES {
        return Err(RpcFailure::new(-32004, "MCP resource exceeds 1 MiB limit"));
    }
    Ok(json!({
        "contents": [{
            "uri": uri,
            "mimeType": declared.mime_type,
            "text": content
        }]
    }))
}

fn header_or_meta(
    headers: &HeaderMap,
    header: &str,
    metadata: Option<&serde_json::Map<String, Value>>,
    field: &str,
) -> Option<String> {
    headers
        .get(header)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| {
            metadata
                .and_then(|meta| meta.get(field))
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
        })
}

fn rpc_ok(id: &Value, result: &Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn rpc_error(id: &Value, code: i32, message: &str, data: Option<&Value>) -> Value {
    let mut error = json!({"code": code, "message": message});
    if let Some(data) = data {
        error["data"] = data.clone();
    }
    json!({"jsonrpc": "2.0", "id": id, "error": error})
}

fn json_response(value: Value) -> Response {
    (StatusCode::OK, Json(value)).into_response()
}

#[derive(Debug)]
struct RpcFailure {
    rpc_code: i32,
    message: String,
    data: Option<Value>,
}

impl RpcFailure {
    fn new(rpc_code: i32, message: impl Into<String>) -> Self {
        Self {
            rpc_code,
            message: message.into(),
            data: None,
        }
    }

    fn with_data(rpc_code: i32, message: impl Into<String>, data: Value) -> Self {
        Self {
            rpc_code,
            message: message.into(),
            data: Some(data),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn initialize_and_reverse_tool_list_match_registry() {
        let state = AppState::for_tests();
        let initialized = dispatch(&state, &HeaderMap::new(), "initialize", json!({}))
            .await
            .expect("initialize");
        assert_eq!(initialized["serverInfo"]["name"], SERVER_NAME);
        let listed = dispatch(&state, &HeaderMap::new(), "tools/list", json!({}))
            .await
            .expect("tools/list");
        let tools = listed["tools"].as_array().expect("tools array");
        assert_eq!(tools.len(), state.tools().specs().len());
        assert!(tools.iter().any(|tool| tool["name"] == "ReadMcpResource"));
    }

    #[tokio::test]
    async fn reverse_tool_call_rejects_working_directory_before_execution() {
        let state = AppState::for_tests();
        let failure = dispatch(
            &state,
            &HeaderMap::new(),
            "tools/call",
            json!({
                "name": "Write",
                "arguments": {"file_path": "x", "content": "x"},
                "workingDirectory": "/tmp/attacker"
            }),
        )
        .await
        .expect_err("must reject");
        assert_eq!(failure.rpc_code, -32602);
        assert!(failure.message.contains("workingDirectory"));
    }

    #[tokio::test]
    async fn reverse_write_without_persistent_context_is_denied() {
        let state = AppState::for_tests();
        let failure = dispatch(
            &state,
            &HeaderMap::new(),
            "tools/call",
            json!({
                "name": "Write",
                "arguments": {"file_path": "x", "content": "x"}
            }),
        )
        .await
        .expect_err("must reject");
        assert_eq!(failure.rpc_code, -32003);
        assert!(failure.message.contains("sessionId"));
    }
}
