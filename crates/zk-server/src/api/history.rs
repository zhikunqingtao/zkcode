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
//! # 关键行为（逐字对照）
//!
//! - **无** `X-Session-Id` 头校验、**无**会话归属校验（旧三个端点都只取
//!   `@PathVariable sessionId`；网络层由 `access_guard` 守住）。
//! - `/snapshots` 的每个 `messageId` 键对应一个**单元素**数组（旧
//!   `List.of(new SnapshotSummary(...))`，非该回合逐文件展开）：`trackedFiles`
//!   是组内全部 `filePath`，`fileCount = trackedFiles.size()`，`timestamp` 取
//!   组内**第一条**快照的 `createdAt`（组为空时 `""`）。
//! - `/rewind` 恒 200：失败信息在 `errors` 里，不转 HTTP 错误码（旧
//!   `ResponseEntity.ok(new RewindResponse(...))` 无条件）。
//! - `/diff` 的 `fromMessageId` / `toMessageId` 为必填 `@RequestParam`。
//!
//! # 差异留痕
//!
//! 1. **`null` 分组键跳过**（`/snapshots`）：旧
//!    `Collectors.groupingBy(FileSnapshot::messageId, ...)` 对 `messageId`
//!    为 `null` 的行抛 `NullPointerException` → 500。zk-engine 侧把该分组表达
//!    为 [`TurnSnapshots::message_id`](zk_engine::TurnSnapshots) 的 `None`，本层
//!    **跳过**这些分组——JSON 对象无 `null` 键可用，且崩溃属旧实现缺陷面而非
//!    契约面。正常写入路径恒带 `messageId`（引擎回合事务已开启），该分支仅在
//!    历史脏数据下触达。
//! 2. **键序**：旧 `Collectors.toMap` 收敛到 `HashMap`（迭代序不确定）；本层
//!    按 `serde_json::Map` 的既定序装配，前端按键取值不依赖键序。
//! 3. **`messageId` 缺省时的错误文案**（`/rewind`）：旧实现把 `null` 透传到
//!    仓储（SQL `= NULL` 恒不匹配），错误文案渲染为 `... messageId: null`；
//!    本层缺省取空串，文案为 `... messageId: `。仅退化输入下的文案差异，
//!    分支走向（session 校验先于快照查找）逐字一致。

use std::collections::HashMap;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::api::http_params::require_param;
use crate::error::ApiError;
use crate::state::AppState;

/// `POST /rewind` 请求体（旧 `RewindRequest` record：两字段皆可为 `null`）。
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RewindRequest {
    /// 目标回合的助手消息 ID。
    pub message_id: Option<String>,
    /// 待回退文件白名单；`null` 或空表示该回合全部文件（旧
    /// `rewindFiles` 内 `filePaths == null || filePaths.isEmpty()`）。
    pub file_paths: Option<Vec<String>>,
}

/// `GET /api/sessions/{sessionId}/history/snapshots`——按 `messageId` 分组的
/// 快照清单（旧 `listSnapshots`）。
#[utoipa::path(
    get,
    path = "/api/sessions/{sessionId}/history/snapshots",
    tag = "history",
    params(("sessionId" = String, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "{\"<messageId>\":[{messageId,trackedFiles,fileCount,timestamp}]}")
    )
)]
pub(crate) async fn list_snapshots(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
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
    params(("sessionId" = String, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "{success,restoredFiles,skippedFiles,errors}（失败亦 200）"),
        (status = 400, description = "体缺失或非法 JSON（INVALID_REQUEST_BODY）")
    )
)]
pub(crate) async fn rewind_to_snapshot(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    // 旧 `@RequestBody`（required 默认 true）：空体/非法 JSON →
    // `HttpMessageNotReadableException` → `INVALID_REQUEST_BODY` 400。
    let request = serde_json::from_slice::<RewindRequest>(&body)
        .map_err(|_| ApiError::invalid_request_body())?;
    tracing::info!(
        session_id = %session_id,
        message_id = ?request.message_id,
        file_paths = ?request.file_paths,
        "Rewind request"
    );

    let result = state
        .file_history
        .rewind_files(
            &session_id,
            request.message_id.as_deref().unwrap_or_default(),
            request.file_paths.as_deref(),
        )
        .await;

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
) -> Result<Json<Value>, ApiError> {
    let from_message_id = require_param(&params, "fromMessageId")?;
    let to_message_id = require_param(&params, "toMessageId")?;
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

    /// 请求体两字段皆可缺省（旧 record 允许 `null`）。
    #[test]
    fn rewind_body_fields_are_optional() {
        let request = serde_json::from_str::<RewindRequest>("{}").expect("empty object parses");
        assert_eq!(request.message_id, None);
        assert_eq!(request.file_paths, None);
    }

    /// camelCase 键绑定（`messageId` / `filePaths`）。
    #[test]
    fn rewind_body_binds_camel_case_keys() {
        let request = serde_json::from_str::<RewindRequest>(
            r#"{"messageId":"m-1","filePaths":["/tmp/a.txt"]}"#,
        )
        .expect("parses");
        assert_eq!(request.message_id.as_deref(), Some("m-1"));
        assert_eq!(
            request.file_paths.as_deref(),
            Some(["/tmp/a.txt".to_owned()].as_slice())
        );
    }

    /// 未知键被忽略（对齐 Jackson 宽容解析，旧 record 同）。
    #[test]
    fn rewind_body_ignores_unknown_keys() {
        let request =
            serde_json::from_str::<RewindRequest>(r#"{"messageId":"m-2","extra":42}"#).expect("ok");
        assert_eq!(request.message_id.as_deref(), Some("m-2"));
    }
}
