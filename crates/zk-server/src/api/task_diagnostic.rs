//! Read-only `TaskRuntime` diagnostic endpoint.

use axum::Json;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use zk_db::TaskDiagnostic;

use crate::error::ApiError;
use crate::session_access::require_session_header;
use crate::state::AppState;

/// Return one payload-free diagnostic snapshot after root-session object authorization.
pub(crate) async fn get_task_diagnostic(
    State(state): State<AppState>,
    AxumPath(task_id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<TaskDiagnostic>, ApiError> {
    let asserted_root_session = require_session_header(&headers)?;
    let diagnostic = state
        .db
        .find_task_diagnostic(&task_id)
        .await?
        .ok_or_else(|| ApiError::not_found("TASK_NOT_FOUND", "Task not found"))?;
    if diagnostic.task.root_session_id != asserted_root_session {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            code: "TASK_ACCESS_DENIED".to_owned(),
            message: "Task does not belong to the asserted root session".to_owned(),
        });
    }
    Ok(Json(diagnostic))
}
