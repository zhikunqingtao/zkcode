//! Independent external MCP connections use the ordinary durable `TaskRuntime`.
pub(crate) mod capabilities;
use crate::{error::ApiError, state::AppState};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
pub(crate) use capabilities::{
    get as get_capabilities, refresh as refresh_capabilities, request as request_capabilities,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Mutex};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use zk_engine::{ExternalRootSubmission, TaskExecutionResult};

pub(crate) struct Context {
    pub session_id: String,
    pub run_id: String,
    token_hash: [u8; 32],
    capabilities: capabilities::Capabilities,
    pub cancel: CancellationToken,
    closing: AtomicBool,
    active: Arc<RwLock<()>>,
    last_seen: std::sync::Mutex<std::time::Instant>,
    _capacity: tokio::sync::OwnedSemaphorePermit,
}
pub(crate) struct Contexts {
    entries: Mutex<HashMap<String, Arc<Context>>>,
    capacity: Arc<Semaphore>,
}
impl Default for Contexts {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity: Arc::new(Semaphore::new(16)),
        }
    }
}
pub(crate) struct ActiveCall {
    pub context: Arc<Context>,
    _guard: OwnedRwLockReadGuard<()>,
}
impl Contexts {
    pub fn authorized(&self, headers: &HeaderMap) -> Result<Arc<Context>, ApiError> {
        let id = headers
            .get("x-run-id")
            .and_then(|h| h.to_str().ok())
            .ok_or_else(ApiError::access_denied)?;
        let entry = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
            .ok_or_else(ApiError::access_denied)?;
        let token = headers
            .get("x-mcp-context-token")
            .and_then(|h| h.to_str().ok())
            .ok_or_else(ApiError::access_denied)?;
        let supplied: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let equal = supplied
            .iter()
            .zip(entry.token_hash)
            .fold(0_u8, |difference, (left, right)| {
                difference | (left ^ right)
            })
            == 0;
        if !equal
            || headers.get("x-session-id").and_then(|h| h.to_str().ok())
                != Some(entry.session_id.as_str())
        {
            return Err(ApiError::access_denied());
        }
        *entry
            .last_seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = std::time::Instant::now();
        Ok(entry)
    }
    pub async fn acquire(&self, headers: &HeaderMap) -> Result<ActiveCall, ApiError> {
        let context = self.authorized(headers)?;
        let guard = context.active.clone().read_owned().await;
        if context.closing.load(Ordering::Acquire) || context.cancel.is_cancelled() {
            return Err(ApiError::access_denied());
        }
        Ok(ActiveCall {
            context,
            _guard: guard,
        })
    }
}

/// This ceiling is independent of both CLI defaults and client-supplied annotations.
pub(crate) fn allowed_tool(name: &str) -> bool {
    matches!(
        name,
        "Read" | "Grep" | "Glob" | "CodeIntel" | "LSP" | "ToolSearch"
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CreateContext {
    project_id: String,
    timeout_seconds: Option<u64>,
}

#[utoipa::path(post,path="/api/mcp/contexts",tag="mcp",responses((status=201,description="Dedicated bounded read-only MCP context")))]
#[allow(clippy::too_many_lines)] // Dedicated session admission and connection ownership must settle together.
pub(crate) async fn create(
    State(state): State<AppState>,
    Json(request): Json<CreateContext>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let permit = state
        .mcp_contexts
        .capacity
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            ApiError::validation_with_code(
                "MCP_CONTEXT_CAPACITY",
                "Too many active MCP connections",
            )
        })?;
    let timeout = request.timeout_seconds.unwrap_or(600);
    if !(1..=1800).contains(&timeout) {
        return Err(ApiError::validation("timeoutSeconds must be in 1..1800"));
    }
    let project = state
        .db
        .get_project(&request.project_id)
        .await?
        .ok_or_else(|| {
            ApiError::validation_with_code("PROJECT_NOT_FOUND", "A registered project is required")
        })?;
    let workspace =
        crate::workspace::require_current_binding(&state.config, &project.workspace_root)?;
    let session_id = uuid::Uuid::new_v4().to_string();
    state
        .db
        .create_session_with_permission(
            &session_id,
            &state.config.default_model,
            &workspace.to_string_lossy(),
            Some("DEFAULT"),
        )
        .await?;
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let token_hash = Sha256::digest(token.as_bytes()).into();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let lifetime = CancellationToken::new();
    // A disconnected create request cannot leave a queued connection alive.
    let mut cancellation = CancelCreate {
        token: Some(lifetime.clone()),
        run: None,
    };
    let host = state.clone();
    let receipt = state.task_runtime().submit_external_root(ExternalRootSubmission {
        session_id:session_id.clone(),startup_epoch:state.startup_epoch(),timeout:Duration::from_secs(timeout),
        // This context has no model route. Preserve the smallest positive
        // native ledger ceiling; granting network candidates never increases it.
        // Configured third-party tool services are not claimed to be free.
        budget:zk_db::TaskBudgetLimits {token_limit:Some(1),cost_limit_nanos_usd:Some(1),deadline_at_ms:None},
    },move |execution| async move {
        let context=Arc::new(Context {session_id:execution.root_session_id.clone(),run_id:execution.run_id.clone(),token_hash,capabilities:capabilities::Capabilities::default(),
            cancel:execution.cancel.child_token(),closing:AtomicBool::new(false),active:Arc::new(RwLock::new(())),last_seen:std::sync::Mutex::new(std::time::Instant::now()),_capacity:permit});
        let directory_context=context.clone();
        let base=Arc::new(host.tools().filtered_by(move |name,tool|directory_context.allows_tool(name,tool)));
        let directory=host.run_tool_scopes.prepare_for_execution(&host.db,&host.execution_supervisor,&execution,&workspace,base).await;
        if directory.is_err() {
            let _=ready_tx.send(Err("MCP_CONTEXT_SETUP_FAILED"));
            let cleanup=host.run_tool_scopes.cleanup(&host.db,&execution.run_id).await;
            return TaskExecutionResult::failed(if cleanup.is_ok() {"MCP_CONTEXT_SETUP_FAILED"} else {"MCP_CONTEXT_CLEANUP_UNCONFIRMED"});
        }
        host.mcp_contexts.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(execution.run_id.clone(),context.clone());
        if ready_tx.send(Ok(())).is_ok() {
            loop {
                tokio::select! { ()=lifetime.cancelled()=>break, ()=context.cancel.cancelled()=>break, ()=tokio::time::sleep(Duration::from_secs(10))=>{} }
                if context.last_seen.lock().unwrap_or_else(std::sync::PoisonError::into_inner).elapsed()>Duration::from_secs(30) {break;}
            }
        }
        context.closing.store(true,Ordering::Release);
        context.cancel.cancel();
        let _drained=context.active.write().await;
        host.task_runtime().seal_run_hook_notifications(&execution.run_id).await;
        host.hooks.drain_run(&execution.run_id).await;
        let cleanup=host.run_tool_scopes.cleanup(&host.db,&execution.run_id).await;
        if cleanup.is_ok() {host.mcp_contexts.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&execution.run_id);}
        if cleanup.is_err() {TaskExecutionResult::failed("MCP_CONTEXT_CLEANUP_UNCONFIRMED")}
        else {TaskExecutionResult::Cancelled {message:"MCP connection closed; all local resources released".into()}}
    }).await.map_err(|_|ApiError::validation_with_code("MCP_CONTEXT_SUBMISSION_FAILED","MCP task could not be admitted"))?;
    cancellation.run = Some((state.task_runtime(), receipt.run_id.clone()));
    if !matches!(
        tokio::time::timeout(Duration::from_secs(30), ready_rx).await,
        Ok(Ok(Ok(())))
    ) {
        let _ = state
            .task_runtime()
            .cancel_run_with_cause(
                &receipt.run_id,
                zk_db::run::EXIT_USER_CANCELLED,
                "MCP connection setup failed",
            )
            .await;
        return Err(ApiError::validation_with_code(
            "MCP_CONTEXT_NOT_READY",
            "MCP task did not become ready",
        ));
    }
    cancellation.token.take();
    cancellation.run.take();
    Ok((
        StatusCode::CREATED,
        Json(
            json!({"sessionId":session_id,"runId":receipt.run_id,"taskId":receipt.task.id,
        "contextToken":token,"permissionMode":"DEFAULT","capabilityCeiling":"readOnly","timeoutSeconds":timeout,"heartbeatIntervalSeconds":10,"idleTimeoutSeconds":30}),
        ),
    ))
}
struct CancelCreate {
    token: Option<CancellationToken>,
    run: Option<(Arc<zk_engine::TaskRuntime>, String)>,
}
impl Drop for CancelCreate {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            token.cancel();
        }
        if let Some((runtime, run)) = self.run.take()
            && let Ok(executor) = tokio::runtime::Handle::try_current()
        {
            executor.spawn(async move {
                let _ = runtime
                    .cancel_run_with_cause(
                        &run,
                        zk_db::run::EXIT_INTERNAL_ERROR,
                        "MCP context creation abandoned",
                    )
                    .await;
            });
        }
    }
}

#[utoipa::path(delete,path="/api/mcp/contexts/{runId}",tag="mcp",responses((status=202,description="Connection cancellation requested; TaskRuntime owns terminal cleanup")))]
pub(crate) async fn close(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let context = state.mcp_contexts.authorized(&headers)?;
    if context.run_id != run_id {
        return Err(ApiError::access_denied());
    }
    context.closing.store(true, Ordering::Release);
    context.cancel.cancel();
    let result = state
        .task_runtime()
        .cancel_run_with_cause(
            &run_id,
            zk_db::run::EXIT_USER_CANCELLED,
            "External MCP connection closed",
        )
        .await;
    if result.is_err() {
        return Err(ApiError::internal());
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"cancelRequested":true,"runId":run_id})),
    ))
}
