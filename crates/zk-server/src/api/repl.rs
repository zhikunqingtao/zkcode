//! Exact Session-owned REPL status and explicit stop. Reads never start a service.
use crate::{error::ApiError, state::AppState};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use serde_json::Value;
async fn authorized(state: &AppState, headers: &HeaderMap, session: &str) -> Result<(), ApiError> {
    if headers
        .get("x-session-id")
        .and_then(|value| value.to_str().ok())
        != Some(session)
    {
        return Err(ApiError::access_denied());
    }
    state
        .db
        .get_session(session)
        .await?
        .ok_or_else(|| ApiError::not_found("SESSION_NOT_FOUND", "Session not found"))?;
    Ok(())
}
#[utoipa::path(get,path="/api/sessions/{id}/repl-service",tag="session",params(("id"=String,Path,description="Owning Session"),("x-session-id"=String,Header,description="Must equal the path Session")),responses((status=200,description="Interpreter service status; never starts a process"),(status=403,description="Session mismatch")))]
pub(crate) async fn status(
    State(state): State<AppState>,
    Path(session): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorized(&state, &headers, &session).await?;
    state
        .repl_services
        .status(&session)
        .await
        .map(Json)
        .map_err(|_| ApiError::internal())
}
#[utoipa::path(delete,path="/api/sessions/{id}/repl-service",tag="session",params(("id"=String,Path,description="Owning Session"),("x-session-id"=String,Header,description="Must equal the path Session")),responses((status=202,description="Stop requested or cleanup retried; poll until confirmed"),(status=403,description="Session or local mutation authorization failed")))]
pub(crate) async fn stop(
    State(state): State<AppState>,
    Path(session): Path<String>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    authorized(&state, &headers, &session).await?;
    state
        .repl_services
        .stop(&session)
        .await
        .map(|value| (StatusCode::ACCEPTED, Json(value)))
        .map_err(|_| ApiError::internal())
}
