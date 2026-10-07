//! 文件历史域 3 端点 handler（Batch 5 Step 6，旧 `FileHistoryController.java`
//! 96 行逐分支复刻；路由注册见 `routes`，服务层见 zk-engine
//! [`FileHistoryService`](zk_engine::FileHistoryService)）。
//!
//! # 端点对照（旧 `@RequestMapping("/api/sessions/{sessionId}/history")`）
//!
//! | 方法 + 路径 | 旧 handler | 响应 |
//! |---|---|---|
//! | `GET /snapshots` | `listSnapshots` | 200 `{"<messageId>":[{messageId,trackedFiles,fileCount,timestamp}]}` |
//! | `POST /rewind` | `rewindToSnapshot` | 200 `{success,restoredFiles,skippedFiles,errors}` |
//! | `GET /diff` | `getDiffStats` | 200 `{filesAdded,filesModified,filesDeleted,changedFiles}` |
//!
//! Read and preview endpoints require the matching Session identity and current
//! workspace binding. Restoration additionally requires explicit file selection,
//! a frozen single-use preview token and confirmation; checkpoint diffs compare
//! two real message snapshots, never an invented "current" checkpoint.

use std::collections::HashMap;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::HeaderMap;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::api::http_params::require_param;
use crate::error::ApiError;
use crate::state::AppState;

/// `POST /rewind` 请求体（旧 `RewindRequest` record：两字段皆可为 `null`）。
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RewindRequest {
    /// Only a reviewed, single-use server token can authorize writes.
    pub preview_token: Option<String>,
    #[serde(default)]
    pub confirmed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RewindPreviewRequest {
    message_id: String,
    file_paths: Vec<String>,
}

async fn require_scope(state: &AppState, id: &str, headers: &HeaderMap) -> Result<(), ApiError> {
    if crate::session_access::require_session_header(headers)? != id {
        return Err(ApiError::session_not_found(id));
    }
    let detail = state
        .db
        .get_session(id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(id))?;
    crate::workspace::require_current_binding(&state.config, &detail.working_dir)?;
    Ok(())
}

/// Freeze the exact checkpoint and explicitly selected files before confirmation.
#[utoipa::path(post,path="/api/sessions/{sessionId}/history/rewind/preview",tag="history",params(("sessionId"=String,Path),("X-Session-Id"=String,Header)),responses((status=200,description="File sizes and single-use five-minute preview token"),(status=400,description="Unsafe, missing or excessive files")))]
pub(crate) async fn preview_rewind(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    headers: HeaderMap,
    Json(input): Json<RewindPreviewRequest>,
) -> Result<Json<zk_engine::file_history::RewindPreview>, ApiError> {
    require_scope(&state, &session_id, &headers).await?;
    let preview = state
        .file_history
        .preview_rewind(&session_id, &input.message_id, &input.file_paths)
        .await
        .map_err(|code| {
            ApiError::validation_with_code(
                &code,
                "File rewind preview is unavailable; no files were changed",
            )
        })?;
    Ok(Json(preview))
}

/// `GET /api/sessions/{sessionId}/history/snapshots`——按 `messageId` 分组的
/// 快照清单（旧 `listSnapshots`）。
#[utoipa::path(
    get,
    path = "/api/sessions/{sessionId}/history/snapshots",
    tag = "history",
    params(("sessionId" = String, Path, description = "会话 ID"), ("X-Session-Id" = String, Header)),
    responses(
        (status = 200, description = "{\"<messageId>\":[{messageId,trackedFiles,fileCount,timestamp}]}")
    )
)]
pub(crate) async fn list_snapshots(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    require_scope(&state, &session_id, &headers).await?;
    tracing::debug!(session_id = %session_id, "Listing snapshots for session");
    let grouped = state
        .file_history
        .list_snapshots_by_session(&session_id)
        .await?;

    let mut result = Map::new();
    for turn in grouped {
        // 差异留痕 1：`None` 分组（旧 `null` 键）无法作为 JSON 对象键，跳过。
        let Some(message_id) = turn.message_id else {
            tracing::warn!(
                session_id = %session_id,
                file_count = turn.files.len(),
                "Skipping snapshot group without messageId"
            );
            continue;
        };
        let tracked_files: Vec<&str> = turn
            .files
            .iter()
            .map(|snapshot| snapshot.file_path.as_str())
            .collect();
        // 旧 `e.getValue().isEmpty() ? "" : e.getValue().getFirst().timestamp()`。
        let timestamp = turn
            .files
            .first()
            .map_or("", |snapshot| snapshot.timestamp.as_str());
        let summary = json!({
            "messageId": message_id,
            "trackedFiles": tracked_files,
            "fileCount": tracked_files.len(),
            "timestamp": timestamp,
        });
        // 旧 `List.of(new SnapshotSummary(...))`：每键恒单元素数组。
        result.insert(message_id, Value::Array(vec![summary]));
    }
    Ok(Json(Value::Object(result)))
}

/// `POST /api/sessions/{sessionId}/history/rewind`——回退到指定回合快照
/// （旧 `rewindToSnapshot`）。
#[utoipa::path(
    post,
    path = "/api/sessions/{sessionId}/history/rewind",
    tag = "history",
    params(("sessionId" = String, Path, description = "会话 ID"), ("X-Session-Id" = String, Header)),
    responses(
        (status = 200, description = "{success,restoredFiles,skippedFiles,errors}（失败亦 200）"),
        (status = 400, description = "体缺失或非法 JSON（INVALID_REQUEST_BODY）")
    )
)]
pub(crate) async fn rewind_to_snapshot(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    // 旧 `@RequestBody`（required 默认 true）：空体/非法 JSON →
    // `HttpMessageNotReadableException` → `INVALID_REQUEST_BODY` 400。
    let request = serde_json::from_slice::<RewindRequest>(&body)
        .map_err(|_| ApiError::invalid_request_body())?;
    let token = request
        .preview_token
        .filter(|token| !token.is_empty())
        .filter(|_| request.confirmed)
        .ok_or_else(|| {
            ApiError::validation_with_code(
                "REWIND_PREVIEW_REQUIRED",
                "Select files with /rewind and confirm a fresh preview",
            )
        })?;
    require_scope(&state, &session_id, &headers).await?;
    let conversation = state.conversation();
    let _reservation = conversation
        .as_ref()
        .map(|engine| {
            engine
                .try_reserve_session_mutation(&session_id)
                .ok_or_else(|| {
                    crate::workspace::failure(
                        axum::http::StatusCode::CONFLICT,
                        "SESSION_BUSY",
                        "Wait for the current task before rewinding files",
                    )
                })
        })
        .transpose()?;
    state.db.ensure_session_idle(&session_id).await?;

    let result = state.file_history.confirm_rewind(&session_id, &token).await;

    Ok(Json(json!({
        "success": result.success,
        "restoredFiles": result.restored_files,
        "skippedFiles": result.skipped_files,
        "errors": result.errors,
    })))
}

/// `GET /api/sessions/{sessionId}/history/diff`——两回合间的 diff 统计
/// （旧 `getDiffStats`）。
#[utoipa::path(
    get,
    path = "/api/sessions/{sessionId}/history/diff",
    tag = "history",
    params(
        ("sessionId" = String, Path, description = "会话 ID"),
        ("X-Session-Id" = String, Header),
        ("fromMessageId" = String, Query, description = "起点回合消息 ID（必填）"),
        ("toMessageId" = String, Query, description = "终点回合消息 ID（必填）")
    ),
    responses(
        (status = 200, description = "{filesAdded,filesModified,filesDeleted,changedFiles}"),
        (status = 400, description = "缺必填参数（MISSING_PARAMETER）")
    )
)]
pub(crate) async fn get_diff_stats(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let from_message_id = require_param(&params, "fromMessageId")?;
    let to_message_id = require_param(&params, "toMessageId")?;
    require_scope(&state, &session_id, &headers).await?;
    tracing::debug!(
        session_id = %session_id,
        from = %from_message_id,
        to = %to_message_id,
        "Diff request"
    );

    let diff = state
        .file_history
        .compute_diff_stats(&session_id, from_message_id, to_message_id)
        .await?;

    Ok(Json(json!({
        "filesAdded": diff.files_added,
        "filesModified": diff.files_modified,
        "filesDeleted": diff.files_deleted,
        "changedFiles": diff.changed_files,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Missing confirmation never defaults to authorization.
    #[test]
    fn rewind_body_fields_are_optional() {
        let request = serde_json::from_str::<RewindRequest>("{}").expect("empty object parses");
        assert_eq!(request.preview_token, None);
        assert!(!request.confirmed);
    }

    /// Preview selection and confirmed token have separate DTOs.
    #[test]
    fn rewind_body_binds_camel_case_keys() {
        let request = serde_json::from_str::<RewindPreviewRequest>(
            r#"{"messageId":"m-1","filePaths":["/tmp/a.txt"]}"#,
        )
        .expect("parses");
        assert_eq!(request.message_id, "m-1");
        assert_eq!(request.file_paths, vec!["/tmp/a.txt"]);
        let confirmation =
            serde_json::from_str::<RewindRequest>(r#"{"previewToken":"p-1","confirmed":true}"#)
                .unwrap();
        assert_eq!(confirmation.preview_token.as_deref(), Some("p-1"));
        assert!(confirmation.confirmed);
    }

    /// 未知键被忽略（对齐 Jackson 宽容解析，旧 record 同）。
    #[test]
    fn rewind_body_ignores_unknown_keys() {
        let request =
            serde_json::from_str::<RewindRequest>(r#"{"messageId":"m-2","extra":42}"#).expect("ok");
        assert!(request.preview_token.is_none());
        assert!(!request.confirmed);
    }
}
