//! Reverse MCP JSON-RPC server. All tool calls share the production registry and admission.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, LazyLock, Mutex};

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use zk_authz::model::PermissionMode;
use zk_db::{
    CasOutcome, CleanupStatus, CommitToolInvocationResult, CommitToolInvocationResultOutcome,
    NewToolInvocation, TaskStatus, ToolInvocationRecord, ToolInvocationStatus,
};
use zk_engine::ObservabilityEvent;
use zk_engine::admission::{Admission, AdmissionRequest, ToolAdmission};
use zk_engine::{HookContext, PreHookDecision};
use zk_tools::{CallEnv, ExecutionResourceOwner, ToolCleanupStatus, ToolEvent, ToolOutput};

use crate::authz::EngineAdmission;
use crate::mcp_tools::{MAX_RESOURCE_BYTES, ResourcePolicyError, validate_declared_resource};
use crate::state::AppState;

const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_LISTED_RESOURCES: usize = 256;
const SERVER_NAME: &str = "zkcode";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
static CONCURRENCY: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(16));
static IN_FLIGHT_REQUESTS: LazyLock<Mutex<HashMap<String, (String, CancellationToken)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
        return notification(&headers, method, &params);
    }
    let id = id.unwrap_or(Value::Null);
    let Ok(permit) = CONCURRENCY.try_acquire() else {
        return json_response(rpc_error(&id, -32001, "MCP server is busy", None));
    };
    let (request_cancel, _request_guard) = if method == "tools/call" {
        match register_in_flight_request(&headers, &id) {
            Ok(registered) => registered,
            Err(error) => {
                return json_response(rpc_error(
                    &id,
                    error.rpc_code,
                    &error.message,
                    error.data.as_ref(),
                ));
            }
        }
    } else {
        (CancellationToken::new(), None)
    };
    let result = dispatch(&state, &headers, method, params, request_cancel).await;
    drop(permit);
    json_response(match result {
        Ok(result) => rpc_ok(&id, &result),
        Err(error) => rpc_error(&id, error.rpc_code, &error.message, error.data.as_ref()),
    })
}

fn notification(headers: &HeaderMap, method: &str, params: &Value) -> Response {
    match method {
        "notifications/cancelled" => {
            let token = params
                .get("requestId")
                .and_then(|request_id| reverse_mcp_request_key(headers, request_id))
                .and_then(|key| {
                    IN_FLIGHT_REQUESTS
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get(&key)
                        .map(|(_, token)| token.clone())
                });
            if let Some(token) = token {
                token.cancel();
            }
            StatusCode::ACCEPTED.into_response()
        }
        _ => StatusCode::ACCEPTED.into_response(),
    }
}

struct InFlightRequestGuard {
    key: String,
    generation: String,
    cancel: CancellationToken,
}

impl Drop for InFlightRequestGuard {
    fn drop(&mut self) {
        self.cancel.cancel();
        let mut requests = IN_FLIGHT_REQUESTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if requests
            .get(&self.key)
            .is_some_and(|(generation, _)| generation == &self.generation)
        {
            requests.remove(&self.key);
        }
    }
}

fn reverse_mcp_request_key(headers: &HeaderMap, request_id: &Value) -> Option<String> {
    // `x-session-id` is the durable connection/session identity required by
    // `tools/call` below. Using the same mandatory header for registration and
    // cancellation avoids a request being registered under an optional
    // transport header while its cancellation arrives under the durable one.
    let connection_id = headers.get("x-session-id")?.to_str().ok()?;
    let request_id = serde_json::to_string(request_id).ok()?;
    Some(format!(
        "{}:{connection_id}:{}:{request_id}",
        connection_id.len(),
        request_id.len()
    ))
}

fn register_in_flight_request(
    headers: &HeaderMap,
    request_id: &Value,
) -> Result<(CancellationToken, Option<InFlightRequestGuard>), RpcFailure> {
    let cancel = CancellationToken::new();
    let Some(key) = reverse_mcp_request_key(headers, request_id) else {
        // A tools/call without its durable session header fails validation
        // before execution; no cancellable physical request can be started.
        return Ok((cancel, None));
    };
    let generation = uuid::Uuid::new_v4().to_string();
    let mut requests = IN_FLIGHT_REQUESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if requests.contains_key(&key) {
        return Err(RpcFailure::new(
            -32600,
            "Duplicate in-flight JSON-RPC request id for this MCP connection",
        ));
    }
    requests.insert(key.clone(), (generation.clone(), cancel.clone()));
    drop(requests);
    Ok((
        cancel.clone(),
        Some(InFlightRequestGuard {
            key,
            generation,
            cancel,
        }),
    ))
}

async fn dispatch(
    state: &AppState,
    headers: &HeaderMap,
    method: &str,
    params: Value,
    request_cancel: CancellationToken,
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
        "tools/call" => call_tool(state, headers, &params, request_cancel).await,
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
    cancel: CancellationToken,
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
    if matches!(
        name,
        "Write" | "Edit" | "NotebookEdit" | "WebSearch" | "WebFetch" | "VerifyJourney"
    ) {
        return Err(RpcFailure::new(
            -32004,
            "MCP_TOOL_REQUIRES_ENGINE_PROJECTION: invoke this tool through the Agent runtime",
        ));
    }
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
    let session_id = required_header(headers, "x-session-id")
        .ok_or_else(|| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_REQUIRED: sessionId"))?;
    let run_id = required_header(headers, "x-run-id")
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
    if run.session_id != session_id || run.task_id.is_empty() {
        return Err(RpcFailure::new(
            -32003,
            "MCP_TOOL_CONTEXT_INVALID: durable run/session ownership required",
        ));
    }
    let task = state
        .db
        .find_runtime_task_by_id(&run.task_id)
        .await
        .map_err(|_| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_UNAVAILABLE: task"))?
        .ok_or_else(|| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_INVALID: task"))?;
    if task.current_run_id.as_deref() != Some(run_id.as_str())
        || task.status != TaskStatus::Running
        || run.status != "running"
        || session.status != "active"
    {
        return Err(RpcFailure::new(
            -32003,
            "MCP_TOOL_CONTEXT_INACTIVE: current running Task/Run required",
        ));
    }
    let workspace = std::fs::canonicalize(&session.working_dir)
        .map_err(|_| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_INVALID: workspace"))?;
    let root_session = state
        .db
        .get_session(&task.session_id)
        .await
        .map_err(|_| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_UNAVAILABLE: root session"))?
        .ok_or_else(|| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_INVALID: root session"))?;
    let task_workspace = std::fs::canonicalize(&root_session.working_dir)
        .map_err(|_| RpcFailure::new(-32003, "MCP_TOOL_CONTEXT_INVALID: task workspace"))?;
    if workspace != task_workspace {
        return Err(RpcFailure::new(
            -32003,
            "MCP_TOOL_CONTEXT_INVALID: workspace ownership mismatch",
        ));
    }
    let workspace_text = workspace.to_string_lossy().into_owned();
    let tools = state.tools();
    let binding = tools
        .resolve(name)
        .ok_or_else(|| RpcFailure::new(-32601, format!("Tool not found: {name}")))?;
    let tool = binding.tool();
    let name = name.to_owned();
    let finalizer_state = state.clone();
    let finalizer_cancel = cancel.clone();
    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
    state
        .execution_supervisor
        .spawn_owned_finalizer(
            cancel,
            Box::pin(async move {
                let state = &finalizer_state;
                let cancel = finalizer_cancel;
                let name = name.as_str();
                let result: Result<Value, RpcFailure> = async move {
                    let tool_use_id = format!("mcp_{}", uuid::Uuid::new_v4());
                    let invocation_id = uuid::Uuid::new_v4().to_string();
                    let original_input_json = serialize_invocation_input(&input)?;
                    let mut invocation = state
                        .db
                        .create_tool_invocation(&NewToolInvocation {
                            invocation_id: invocation_id.clone(),
                            task_id: task.id.clone(),
                            run_id: run_id.clone(),
                            tool_use_id: tool_use_id.clone(),
                            tool_name: name.to_owned(),
                            input_json: Some(original_input_json),
                            // PRE hooks and admission may rewrite input. Keep the preparing row
                            // conservative and classify the final execution input in the atomic
                            // Running CAS below.
                            side_effect_class: "unknown".to_owned(),
                            directory_generation: Some(
                                i64::try_from(binding.directory_generation()).unwrap_or(i64::MAX),
                            ),
                            connection_generation: binding
                                .connection_generation()
                                .map(|generation| i64::try_from(generation).unwrap_or(i64::MAX)),
                        })
                        .await
                        .map_err(|error| {
                            storage_failure("persist reverse MCP invocation", &error)
                        })?;
                    let hook_context = HookContext::new()
                        .with_tool(name)
                        .with_session(session_id.clone())
                        .with_working_dir(workspace_text.clone());
                    let pre_input = match state.hooks.evaluate_pre_tool(&hook_context, &input).await
                    {
                        PreHookDecision::Continue { input } => input,
                        PreHookDecision::Deny { code, message } => {
                            commit_invocation_result(
                                state,
                                &session_id,
                                &mut invocation,
                                ToolInvocationStatus::Failed,
                                &input,
                                &ToolOutput::error(format!("{code}: {message}")),
                                Some(&code),
                                CleanupStatus::NotRequired,
                            )
                            .await?;
                            let mut telemetry =
                                ObservabilityEvent::new("mcp", "tool_call", "denied");
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
                    let admission =
                        EngineAdmission::new_dont_ask(state.authz.clone(), Arc::clone(&tools));
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
                        Admission::Denied { code, message }
                        | Admission::Failed { code, message } => {
                            commit_invocation_result(
                                state,
                                &session_id,
                                &mut invocation,
                                ToolInvocationStatus::Failed,
                                &pre_input,
                                &ToolOutput::error(format!("{code}: {message}")),
                                Some(&code),
                                CleanupStatus::NotRequired,
                            )
                            .await?;
                            let mut telemetry =
                                ObservabilityEvent::new("mcp", "tool_call", "denied");
                            telemetry.session_id = Some(session_id.clone());
                            telemetry.run_id = Some(run_id.clone());
                            telemetry.duration_ms = Some(
                                u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                            );
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
                    if !tools.is_binding_current(&binding) {
                        commit_invocation_result(
                            state,
                            &session_id,
                            &mut invocation,
                            ToolInvocationStatus::Failed,
                            &execution_input,
                            &ToolOutput::error(
                                "TOOL_CAPABILITY_REVOKED: registration changed during admission",
                            ),
                            Some("TOOL_CAPABILITY_REVOKED"),
                            CleanupStatus::NotRequired,
                        )
                        .await?;
                        return Err(RpcFailure::new(
                            -32003,
                            "TOOL_CAPABILITY_REVOKED: registration changed during admission",
                        ));
                    }
                    let side_effect_class = if tool.is_read_only(&execution_input) {
                        "read"
                    } else {
                        "write"
                    };
                    start_active_invocation(
                        state,
                        &session_id,
                        &mut invocation,
                        &execution_input,
                        side_effect_class,
                    )
                    .await?;
                    // The durable Running CAS is an await boundary. Recheck the exact tool
                    // instance and both catalog generations immediately before side effects.
                    if !tools.is_binding_current(&binding) {
                        commit_invocation_result(
                            state,
                            &session_id,
                            &mut invocation,
                            ToolInvocationStatus::Failed,
                            &execution_input,
                            &ToolOutput::error(
                                "TOOL_CAPABILITY_REVOKED: registration changed before execution",
                            ),
                            Some("TOOL_CAPABILITY_REVOKED"),
                            CleanupStatus::NotRequired,
                        )
                        .await?;
                        return Err(RpcFailure::new(
                            -32003,
                            "TOOL_CAPABILITY_REVOKED: registration changed before execution",
                        ));
                    }
                    let _cancel_on_drop = CancelOnDrop(cancel.clone());
                    let watcher_stop = CancellationToken::new();
                    let cancellation_watcher = spawn_run_cancellation_watch(
                        state.db.clone(),
                        task.id.clone(),
                        run_id.clone(),
                        cancel.clone(),
                        watcher_stop.clone(),
                    );
                    let env = CallEnv::new()
                        .with_working_dir(workspace)
                        .with_session_id(session_id.clone())
                        .with_run_id(run_id.clone())
                        .with_tool_catalog(tools.specs());
                    let owner = ExecutionResourceOwner {
                        task_id: task.id.clone(),
                        run_id: run_id.clone(),
                        invocation_id: invocation_id.clone(),
                    };
                    let mut events = state.execution_supervisor.spawn_call_in(
                        tool,
                        tool_use_id.clone(),
                        execution_input.clone(),
                        &cancel,
                        env,
                        owner,
                    );
                    // The finalizer owns the event drain and durable terminal transaction. If
                    // the HTTP response Future is dropped, its JoinHandle detaches while the
                    // request guard cancels the token; cleanup/result persistence therefore
                    // still reaches a factually provable terminal state.
                    let finalizer_state = state.clone();
                    let finalizer_session_id = session_id.clone();
                    let finalizer_cancel = cancel.clone();
                    let finalizer = tokio::spawn(async move {
                        let finished = loop {
                            match events.recv().await {
                                Some(ToolEvent::Progress { .. }) => {}
                                Some(ToolEvent::Finished {
                                    output,
                                    cleanup_status,
                                    ..
                                }) => break Some((output, cleanup_status)),
                                None => break None,
                            }
                        };
                        watcher_stop.cancel();
                        let _ = cancellation_watcher.await;
                        let Some((output, cleanup_status)) = finished else {
                            let interrupted = ToolOutput::error("Tool execution interrupted");
                            commit_invocation_result(
                                &finalizer_state,
                                &finalizer_session_id,
                                &mut invocation,
                                ToolInvocationStatus::Interrupted,
                                &execution_input,
                                &interrupted,
                                Some("TOOL_EXECUTION_INTERRUPTED"),
                                CleanupStatus::Unconfirmed,
                            )
                            .await?;
                            return Err(RpcFailure::new(-32001, "Tool execution interrupted"));
                        };
                        let cancelled = finalizer_cancel.is_cancelled();
                        let timed_out = !cancelled
                            && output.is_error
                            && output
                                .content
                                .starts_with("Tool execution timed out after ");
                        let terminal_status = if cancelled {
                            ToolInvocationStatus::Interrupted
                        } else if output.is_error {
                            ToolInvocationStatus::Failed
                        } else {
                            ToolInvocationStatus::Succeeded
                        };
                        let error_code = if cancelled {
                            Some("TOOL_EXECUTION_INTERRUPTED")
                        } else if timed_out {
                            Some("TOOL_TIMEOUT")
                        } else if output.is_error {
                            Some("TOOL_RETURNED_ERROR")
                        } else {
                            None
                        };
                        let (message, output_sha256) = commit_invocation_result(
                            &finalizer_state,
                            &finalizer_session_id,
                            &mut invocation,
                            terminal_status,
                            &execution_input,
                            &output,
                            error_code,
                            durable_cleanup_status(cleanup_status),
                        )
                        .await?;
                        Ok::<_, RpcFailure>((
                            output,
                            cleanup_status,
                            message,
                            output_sha256,
                            timed_out,
                            cancelled,
                        ))
                    });
                    let (output, cleanup_status, message, output_sha256, timed_out, cancelled) =
                        finalizer.await.map_err(|error| {
                            tracing::error!(%error, "reverse MCP terminal finalizer panicked");
                            RpcFailure::new(-32603, "MCP_TOOL_FINALIZER_FAILED")
                        })??;
                    if cancelled {
                        let mut telemetry =
                            ObservabilityEvent::new("mcp", "tool_call", "cancelled");
                        telemetry.session_id = Some(session_id.clone());
                        telemetry.run_id = Some(run_id.clone());
                        telemetry.duration_ms =
                            Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
                        telemetry
                            .attributes
                            .insert("tool".to_owned(), Value::String(name.to_owned()));
                        state.observability.record(telemetry);
                        return Err(RpcFailure::new(-32001, "Tool execution interrupted"));
                    }
                    if timed_out {
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
                    telemetry.duration_ms =
                        Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
                    telemetry
                        .attributes
                        .insert("tool".to_owned(), Value::String(name.to_owned()));
                    state.observability.record(telemetry);
                    Ok(json!({
                        "content": [{"type": "text", "text": output.content}],
                        "isError": output.is_error,
                        "structuredContent": output.metadata,
                        "_meta": {
                            "invocationId": invocation_id,
                            "messageId": message.id,
                            "outputSha256": output_sha256,
                            "cleanupStatus": durable_cleanup_status(cleanup_status).as_db(),
                        },
                    }))
                }
                .await;
                let _ = completion_tx.send(result);
            }),
        )
        .map_err(|_| RpcFailure::new(-32001, "EXECUTION_SUPERVISOR_SHUTTING_DOWN"))?;
    completion_rx
        .await
        .map_err(|_| RpcFailure::new(-32603, "MCP_TOOL_FINALIZER_FAILED"))?
}

fn serialize_invocation_input(input: &Value) -> Result<String, RpcFailure> {
    serde_json::to_string(input).map_err(|error| {
        tracing::error!(%error, "failed to serialize reverse MCP invocation input");
        RpcFailure::new(-32603, "MCP_TOOL_PERSISTENCE_FAILED")
    })
}

fn storage_failure(context: &str, error: &impl std::fmt::Display) -> RpcFailure {
    tracing::error!(%error, context, "reverse MCP durable state write failed");
    RpcFailure::new(-32603, "MCP_TOOL_PERSISTENCE_FAILED")
}

#[allow(clippy::too_many_arguments)]
async fn commit_invocation_result(
    state: &AppState,
    session_id: &str,
    invocation: &mut ToolInvocationRecord,
    target: ToolInvocationStatus,
    input: &Value,
    output: &ToolOutput,
    error_code: Option<&str>,
    cleanup_status: CleanupStatus,
) -> Result<(zk_db::MessageRecord, String), RpcFailure> {
    let input_json = serialize_invocation_input(input)?;
    let output_sha256 = hash_tool_output(output)?;
    let mut metadata = output.metadata.clone().unwrap_or_else(|| json!({}));
    if !metadata.is_object() {
        metadata = json!({"structuredResult": metadata});
    }
    metadata
        .as_object_mut()
        .expect("object normalized above")
        .insert(
            "outputSha256".to_owned(),
            Value::String(output_sha256.clone()),
        );
    let outcome = state
        .db
        .commit_tool_invocation_result(&CommitToolInvocationResult {
            invocation_id: invocation.invocation_id.clone(),
            expected_version: invocation.version,
            session_id: session_id.to_owned(),
            target,
            input_json: Some(input_json),
            content: output.content.clone(),
            is_error: output.is_error,
            metadata: Some(metadata),
            output_sha256: Some(output_sha256.clone()),
            error_code: error_code.map(str::to_owned),
            cleanup_status,
            postprocessing: None,
        })
        .await
        .map_err(|error| storage_failure("commit reverse MCP tool result", &error))?;
    let CommitToolInvocationResultOutcome::Committed(facts) = outcome else {
        tracing::error!(
            invocation_id = %invocation.invocation_id,
            ?target,
            ?outcome,
            "reverse MCP invocation/result transaction was not applied"
        );
        return Err(RpcFailure::new(-32603, "MCP_TOOL_PERSISTENCE_FAILED"));
    };
    *invocation = facts.invocation;
    Ok((facts.message, output_sha256))
}

async fn start_active_invocation(
    state: &AppState,
    session_id: &str,
    invocation: &mut ToolInvocationRecord,
    input: &Value,
    side_effect_class: &str,
) -> Result<(), RpcFailure> {
    let input_json = serialize_invocation_input(input)?;
    let outcome = state
        .db
        .start_tool_invocation_for_active_run_cas(
            &invocation.invocation_id,
            invocation.version,
            &input_json,
            side_effect_class,
        )
        .await
        .map_err(|error| storage_failure("start reverse MCP invocation", &error))?;
    match outcome {
        CasOutcome::Applied => {
            invocation.version = invocation.version.saturating_add(1);
            Ok(())
        }
        CasOutcome::InvalidTransition => {
            commit_invocation_result(
                state,
                session_id,
                invocation,
                ToolInvocationStatus::Failed,
                input,
                &ToolOutput::error("MCP_TOOL_CONTEXT_INACTIVE: Run stopped before execution"),
                Some("MCP_TOOL_CONTEXT_INACTIVE"),
                CleanupStatus::NotRequired,
            )
            .await?;
            Err(RpcFailure::new(
                -32003,
                "MCP_TOOL_CONTEXT_INACTIVE: Run stopped before execution",
            ))
        }
        CasOutcome::VersionConflict | CasOutcome::NotFound => {
            tracing::error!(
                invocation_id = %invocation.invocation_id,
                ?outcome,
                "reverse MCP invocation start CAS failed closed"
            );
            Err(RpcFailure::new(-32603, "MCP_TOOL_PERSISTENCE_FAILED"))
        }
    }
}

const fn durable_cleanup_status(status: ToolCleanupStatus) -> CleanupStatus {
    match status {
        ToolCleanupStatus::NotRequired => CleanupStatus::NotRequired,
        ToolCleanupStatus::Pending => CleanupStatus::Pending,
        ToolCleanupStatus::Confirmed => CleanupStatus::Confirmed,
        ToolCleanupStatus::Unconfirmed => CleanupStatus::Unconfirmed,
    }
}

fn hash_tool_output(output: &ToolOutput) -> Result<String, RpcFailure> {
    let canonical = serde_json::to_vec(&json!({
        "content": output.content,
        "isError": output.is_error,
        "metadata": output.metadata,
    }))
    .map_err(|error| {
        tracing::error!(%error, "failed to serialize reverse MCP tool output");
        RpcFailure::new(-32603, "MCP_TOOL_OUTPUT_INVALID")
    })?;
    let digest = Sha256::digest(canonical);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(encoded)
}

struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

fn spawn_run_cancellation_watch(
    db: zk_db::Db,
    task_id: String,
    run_id: String,
    cancel: CancellationToken,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(25));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                () = cancel.cancelled() => return,
                _ = interval.tick() => {
                    let active = match (
                        db.find_runtime_task_by_id(&task_id).await,
                        db.find_run_by_id(&run_id).await,
                    ) {
                        (Ok(Some(task)), Ok(Some(run))) => {
                            task.current_run_id.as_deref() == Some(run_id.as_str())
                                && task.status == TaskStatus::Running
                                && run.task_id == task_id
                                && run.status == "running"
                        }
                        (task_result, run_result) => {
                            tracing::error!(
                                ?task_result,
                                ?run_result,
                                %task_id,
                                %run_id,
                                "reverse MCP cancellation ownership check failed closed"
                            );
                            false
                        }
                    };
                    if !active {
                        cancel.cancel();
                        return;
                    }
                }
            }
        }
    })
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

fn required_header(headers: &HeaderMap, header: &str) -> Option<String> {
    headers
        .get(header)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
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
        let initialized = dispatch(
            &state,
            &HeaderMap::new(),
            "initialize",
            json!({}),
            CancellationToken::new(),
        )
        .await
        .expect("initialize");
        assert_eq!(initialized["serverInfo"]["name"], SERVER_NAME);
        let listed = dispatch(
            &state,
            &HeaderMap::new(),
            "tools/list",
            json!({}),
            CancellationToken::new(),
        )
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
            CancellationToken::new(),
        )
        .await
        .expect_err("must reject");
        assert_eq!(failure.rpc_code, -32602);
        assert!(failure.message.contains("workingDirectory"));
    }

    #[tokio::test]
    async fn reverse_write_requires_engine_projection() {
        let state = AppState::for_tests();
        let failure = dispatch(
            &state,
            &HeaderMap::new(),
            "tools/call",
            json!({
                "name": "Write",
                "arguments": {"file_path": "x", "content": "x"}
            }),
            CancellationToken::new(),
        )
        .await
        .expect_err("must reject");
        assert_eq!(failure.rpc_code, -32004);
        assert!(
            failure
                .message
                .contains("MCP_TOOL_REQUIRES_ENGINE_PROJECTION")
        );
    }

    #[test]
    fn cancellation_notification_targets_exact_request_and_guard_removes_registration() {
        let session_id = uuid::Uuid::new_v4().to_string();
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-session-id",
            session_id.parse().expect("valid session header"),
        );
        let request_id = json!(77);
        let (cancel, guard) =
            register_in_flight_request(&headers, &request_id).expect("register request");
        let guard = guard.expect("session-scoped guard");

        let _ = notification(
            &headers,
            "notifications/cancelled",
            &json!({"requestId": 78}),
        );
        assert!(
            !cancel.is_cancelled(),
            "another request id must stay isolated"
        );

        let _ = notification(
            &headers,
            "notifications/cancelled",
            &json!({"requestId": 77}),
        );
        assert!(
            cancel.is_cancelled(),
            "matching request id must be cancelled"
        );
        drop(guard);

        let key = reverse_mcp_request_key(&headers, &request_id).expect("request key");
        assert!(
            !IN_FLIGHT_REQUESTS
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&key),
            "RAII guard must remove completed/aborted requests"
        );
    }
}
