//! Session snapshot REST adapters backed by the singleton snapshot service.

use axum::Json;
use axum::extract::{Path, State};
use serde_json::{Value, json};
use zk_db::SnapshotRestoreOutcome;
use zk_engine::{SessionSnapshot, SessionSnapshotSummary};

use crate::error::ApiError;
use crate::state::AppState;

fn invalid_snapshot_id() -> ApiError {
    ApiError::validation_with_code("SNAPSHOT_ID_INVALID", "Snapshot session id is invalid")
}

fn snapshot_not_found(session_id: &str) -> ApiError {
    ApiError::not_found(
        "SNAPSHOT_NOT_FOUND",
        &format!("Snapshot not found for session: {session_id}"),
    )
}

/// `GET /api/sessions/snapshots`.
pub(crate) async fn list(State(state): State<AppState>) -> Json<Vec<SessionSnapshotSummary>> {
    Json(state.session_snapshots.list_snapshots().await)
}

/// `POST /api/sessions/{sessionId}/snapshot`.
pub(crate) async fn save(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionSnapshotSummary>, ApiError> {
    let detail = state
        .db
        .get_session(&session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&session_id))?;
    let snapshot = SessionSnapshot::from_session_detail(&detail);
    state
        .session_snapshots
        .save_snapshot(&session_id, &snapshot)
        .await
        .map_err(|_| invalid_snapshot_id())?;
    let persisted = state
        .session_snapshots
        .load_snapshot(&session_id)
        .await
        .map_err(|_| invalid_snapshot_id())?
        .ok_or_else(|| {
            ApiError::validation_with_code(
                "SNAPSHOT_WRITE_FAILED",
                "Snapshot could not be persisted",
            )
        })?;
    Ok(Json(persisted.summary()))
}

/// `POST /api/sessions/{sessionId}/snapshot/resume`.
pub(crate) async fn resume(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionSnapshotSummary>, ApiError> {
    let snapshot = state
        .session_snapshots
        .load_snapshot(&session_id)
        .await
        .map_err(|_| invalid_snapshot_id())?
        .ok_or_else(|| snapshot_not_found(&session_id))?;
    if snapshot.session_id.as_deref() != Some(session_id.as_str()) {
        return Err(ApiError::validation_with_code(
            "SNAPSHOT_SESSION_MISMATCH",
            "Snapshot session id does not match the requested session",
        ));
    }
    let current = state
        .db
        .get_session(&session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&session_id))?;
    let snapshot_workspace = metadata_string(&snapshot, "workingDir").ok_or_else(|| {
        ApiError::validation_with_code(
            "SNAPSHOT_WORKSPACE_MISSING",
            "Snapshot has no authorized workspace",
        )
    })?;
    let current_canonical = std::fs::canonicalize(&current.working_dir).map_err(|_| {
        ApiError::validation_with_code(
            "SNAPSHOT_WORKSPACE_UNAVAILABLE",
            "Session workspace is unavailable",
        )
    })?;
    let snapshot_canonical = std::fs::canonicalize(snapshot_workspace).map_err(|_| {
        ApiError::validation_with_code(
            "SNAPSHOT_WORKSPACE_UNAVAILABLE",
            "Snapshot workspace is unavailable",
        )
    })?;
    if current_canonical != snapshot_canonical {
        return Err(ApiError::validation_with_code(
            "SNAPSHOT_WORKSPACE_MISMATCH",
            "Snapshot belongs to a different workspace",
        ));
    }

    let model = snapshot.model.as_deref().ok_or_else(|| {
        ApiError::validation_with_code("SNAPSHOT_MODEL_MISSING", "Snapshot has no model")
    })?;
    let status = metadata_string(&snapshot, "status").unwrap_or("active");
    let title = snapshot.metadata.get("title").and_then(Value::as_str);
    let input_tokens = metadata_i64(&snapshot, "totalInputTokens");
    let output_tokens = metadata_i64(&snapshot, "totalOutputTokens");
    let cost_usd = snapshot
        .metadata
        .get("totalCostUsd")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    match state
        .db
        .restore_session_snapshot(
            &session_id,
            &current.working_dir,
            model,
            status,
            title,
            input_tokens,
            output_tokens,
            cost_usd,
            snapshot.messages.clone(),
        )
        .await?
    {
        SnapshotRestoreOutcome::Applied => Ok(Json(snapshot.summary())),
        SnapshotRestoreOutcome::NotFound => Err(ApiError::session_not_found(&session_id)),
        SnapshotRestoreOutcome::WorkspaceMismatch => Err(ApiError::validation_with_code(
            "SNAPSHOT_WORKSPACE_MISMATCH",
            "Snapshot belongs to a different workspace",
        )),
    }
}

/// `DELETE /api/sessions/snapshots/{sessionId}`.
pub(crate) async fn delete(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let deleted = state
        .session_snapshots
        .delete_snapshot(&session_id)
        .await
        .map_err(|_| invalid_snapshot_id())?;
    if !deleted {
        return Err(snapshot_not_found(&session_id));
    }
    Ok(Json(json!({ "sessionId": session_id, "deleted": true })))
}

fn metadata_string<'a>(snapshot: &'a SessionSnapshot, key: &str) -> Option<&'a str> {
    snapshot.metadata.get(key).and_then(Value::as_str)
}

fn metadata_i64(snapshot: &SessionSnapshot, key: &str) -> i64 {
    snapshot
        .metadata
        .get(key)
        .and_then(Value::as_i64)
        .unwrap_or(0)
}
