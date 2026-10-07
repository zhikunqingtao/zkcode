//! Session merge HTTP orchestration over the durable `SQLite` coordinator.
use crate::{error::ApiError, state::AppState};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use zk_db::{SessionMergeOperation, SessionMergeRequest};

static WORKERS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<(String, i64)>>> =
    std::sync::OnceLock::new();
struct WorkerGuard((String, i64));
impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if let Some(workers) = WORKERS.get() {
            workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&self.0);
        }
    }
}
fn start_worker(state: AppState, operation: &SessionMergeOperation) {
    if operation.status != "preparing" {
        return;
    }
    let (id, epoch) = (operation.operation_id.clone(), operation.run_epoch);
    if !WORKERS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert((id.clone(), epoch))
    {
        return;
    }
    let guard = WorkerGuard((id.clone(), epoch));
    tokio::spawn(async move {
        let _guard = guard;
        let outcome = async {
            let operation = state
                .db
                .session_merge(&id)
                .await?
                .ok_or_else(|| zk_db::DbError::Invalid("merge disappeared".into()))?;
            if operation.run_epoch != epoch || operation.status != "preparing" {
                return Err(zk_db::DbError::Conflict(
                    "merge worker has been fenced".into(),
                ));
            }
            crate::session_merge_summary::prepare(&state, &operation).await?;
            state.db.complete_session_merge(&id, epoch).await
        }
        .await;
        match outcome {
            Ok(operation) => {
                // The database target already inherited the sealed primary mode.
                // Publish the same value into the synchronous authorization cache.
                if let Ok(modes) = state.db.permission_modes_at_startup()
                    && let Some(mode) = modes
                        .get(&operation.target_session_id)
                        .and_then(|m| zk_authz::model::PermissionMode::parse(m))
                {
                    state
                        .authz
                        .modes
                        .set_ephemeral_mode(&operation.target_session_id, mode);
                }
            }
            Err(zk_db::DbError::Conflict(_)) => {} // Cancellation/new epoch won.
            Err(error) => {
                tracing::error!(operation_id=%id,%error,"session merge paused");
                if let Err(persist) = state
                    .db
                    .pause_session_merge(
                        id,
                        epoch,
                        match &error {
                            zk_db::DbError::Invalid(code)
                                if code.starts_with("MERGE_") || code.starts_with("BUDGET_") =>
                            {
                                format!("{code}：合并进度已保留，可检查配置后恢复或取消。")
                            }
                            _ => "合并未完成，进度已保留；请检查配置后恢复或取消。".into(),
                        },
                    )
                    .await
                {
                    tracing::error!(%persist,"could not persist merge failure");
                }
            }
        }
    });
}

pub(crate) async fn create(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut request): Json<SessionMergeRequest>,
) -> Result<(StatusCode, Json<SessionMergeOperation>), ApiError> {
    let key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::validation("Idempotency-Key is required"))?
        .to_owned();
    if let Some(model) = request.model.as_deref().filter(|m| !m.is_empty()) {
        request.model = Some(super::session::resolve_model(&state, Some(model))?);
    } else {
        request.model = None;
    }
    let operation = state
        .db
        .start_session_merge_with_assets(
            key,
            request,
            Some(state.config.scratchpad_system_root.clone()),
        )
        .await
        .map_err(merge_start_error)?;
    start_worker(state, &operation);
    Ok((StatusCode::ACCEPTED, Json(operation)))
}

fn merge_start_error(error: zk_db::DbError) -> ApiError {
    match &error {
        zk_db::DbError::Invalid(code) if code == "MERGE_DISK_SPACE_LOW" => ApiError {
            status: StatusCode::INSUFFICIENT_STORAGE,
            code: code.clone(),
            message: "数据库所在磁盘空间不足；合并写入后必须至少保留 1 GiB 可用空间。此次创建已回滚，请释放空间后重试。".into(),
        },
        zk_db::DbError::Invalid(code) if code.starts_with("MERGE_DISK_SPACE_CHECK_FAILED") => {
            tracing::error!(%error, "could not verify merge disk reserve");
            ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "MERGE_DISK_SPACE_CHECK_FAILED".into(),
                message: "无法确认数据库所在磁盘的可用空间，此次合并创建已回滚，请检查磁盘后重试。".into(),
            }
        }
        _ => error.into(),
    }
}

pub(crate) async fn get(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<SessionMergeOperation>, ApiError> {
    Ok(Json(state.db.session_merge(&id).await?.ok_or_else(
        || ApiError::not_found("MERGE_NOT_FOUND", "Merge operation not found"),
    )?))
}

pub(crate) async fn active(State(state): State<AppState>) -> Result<Response, ApiError> {
    Ok(match state.db.active_session_merge().await? {
        Some(operation) => Json(operation).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ResumeRequest {
    expected_epoch: i64,
    model: Option<String>,
}

pub(crate) async fn resume(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<ResumeRequest>,
) -> Result<(StatusCode, Json<SessionMergeOperation>), ApiError> {
    let model = request
        .model
        .as_deref()
        .map(|model| super::session::resolve_model(&state, Some(model)))
        .transpose()?;
    let operation = state
        .db
        .transition_session_merge(&id, Some(request.expected_epoch), model, false)
        .await?;
    start_worker(state, &operation);
    Ok((StatusCode::ACCEPTED, Json(operation)))
}

pub(crate) async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<SessionMergeOperation>, ApiError> {
    Ok(Json(
        state
            .db
            .transition_session_merge(&id, None, None, true)
            .await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_space_failures_report_rollback_without_exposing_probe_details() {
        let low = merge_start_error(zk_db::DbError::Invalid("MERGE_DISK_SPACE_LOW".into()));
        assert_eq!(low.status, StatusCode::INSUFFICIENT_STORAGE);
        assert_eq!(low.code, "MERGE_DISK_SPACE_LOW");
        assert!(low.message.contains("已回滚"));
        let probe = merge_start_error(zk_db::DbError::Invalid(
            "MERGE_DISK_SPACE_CHECK_FAILED: /private/database/path".into(),
        ));
        assert_eq!(probe.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(probe.code, "MERGE_DISK_SPACE_CHECK_FAILED");
        assert!(!probe.message.contains("/private"));
    }
}
