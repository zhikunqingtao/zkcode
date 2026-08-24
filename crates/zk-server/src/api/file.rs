//! File 域 3 端点 handler（Batch 2 Step 2-4，旧 `FileController.java`；路由
//! 注册见 `routes`，服务层见 [`crate::file_access`]）。
//!
//! # 端点对照
//!
//! | 方法 + 路径 | 旧 handler | 命中 |
//! |---|---|---|
//! | `GET /api/files/search` | `searchFiles` | 200 `[FileSearchResult]` |
//! | `GET /api/sessions/{sessionId}/files/preview` | `preview` | 200 文件流 |
//! | `POST /api/sessions/{sessionId}/files/reveal` | `reveal` | 200 `RevealResult` / 403 空体 |
//!
//! # 关键行为（逐字对照）
//!
//! - `searchFiles`：参数声明序 `query`（必填）/ `limit`（默认 20）/
//!   `sessionId`（必填）——镜像 Spring 解析序（`query` 缺 → 400
//!   `MISSING_PARAMETER`，`limit` 非法 → 500，`sessionId` 缺 → 400）。
//!   `loadSession` 缺 → 404；`requireCurrentBinding` → 409/403；随后
//!   `fuzzySearch`。旧 `fuzzySearch` 内 `.limit(limit)`：`limit < 0` 时
//!   Java `Stream.limit` 抛 `IllegalArgumentException` → 400 `INVALID_REQUEST`
//!   （消息即该负数）——本实现在绑定成功后、搜索前复刻该分支。
//! - `preview`：`X-Session-Id`（缺 → 400 `MISSING_HEADER`）→ `path` 参数
//!   （缺 → 400 `MISSING_PARAMETER 'path'`）→ `requireMatchingSession`
//!   （不等 → 403 `SESSION_CONTEXT_MISMATCH`）→ 服务层 `preview`（空 path →
//!   400 `FILE_PATH_REQUIRED`；会话 404；绑定 409/403；解析 404/403；
//!   415/413/409）。响应头：`Content-Type`（fallback 表）、`Content-Length`、
//!   `Content-Disposition: inline; filename="<去引号名>"`、
//!   `X-Content-Type-Options: nosniff`。
//! - `reveal`：`@RequestBody`（缺/坏 → 400 `INVALID_REQUEST_BODY`）→
//!   `X-Session-Id`（400 `MISSING_HEADER`）→ `requireMatchingSession`（403）→
//!   `userGesture != "reveal-file"` → **403 空体**；随后服务层 `reveal`：
//!   `localDesktopAccessAllowed` **优先**（false → 403 `FILE_REVEAL_LOCAL_ONLY`）
//!   → 空 path 400 → 会话 404 → 绑定 → 解析 → `open -R` 等（503 / 成功）。

use std::collections::HashMap;

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::api::http_params::{parse_spring_int, require_param};
use crate::error::ApiError;
use crate::file_access::{
    FileSearchResult, RevealResult, fuzzy_search, preview_target, require_path, resolve_within,
    reveal_path,
};
use crate::session_access::require_session_header;
use crate::state::AppState;
use crate::workspace::{failure, local_desktop_access_allowed, require_current_binding};

/// `POST /reveal` 请求体（旧 `FileController.RevealFileRequest` record）。
/// `path` 用 `Option`：缺失或显式 `null` 均归一为 `None`（Java `path()==null`），
/// 供服务层空值分支回 `FILE_PATH_REQUIRED`（不提前落 `INVALID_REQUEST_BODY`）。
#[derive(Debug, Deserialize)]
pub(crate) struct RevealFileRequest {
    /// 目标文件的工作区相对/绝对路径。
    pub path: Option<String>,
}

/// `spawn_blocking` join 失败归一（对齐 `project` handler 的 500 语义）。
fn join_internal(err: &tokio::task::JoinError) -> ApiError {
    tracing::error!(error = %err, "file-access blocking task failed");
    ApiError::internal()
}

/// 旧 `FileController.requireMatchingSession`（不等 → 403
/// `SESSION_CONTEXT_MISMATCH`，`ResponseStatusException` → code=message=reason）。
fn require_matching_session(session_id: &str, asserted: &str) -> Result<(), ApiError> {
    if session_id != asserted {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "SESSION_CONTEXT_MISMATCH",
            "SESSION_CONTEXT_MISMATCH",
        ));
    }
    Ok(())
}

/// `GET /api/files/search`——工作区模糊搜索（旧 `searchFiles`）。
#[utoipa::path(
    get,
    path = "/api/files/search",
    tag = "files",
    responses(
        (status = 200, description = "文件搜索结果数组（score 降序）"),
        (status = 400, description = "缺 query/sessionId（MISSING_PARAMETER）/ limit 负数（INVALID_REQUEST）"),
        (status = 404, description = "会话不存在（SESSION_NOT_FOUND）"),
        (status = 409, description = "工作区失效（WORKSPACE_UNAVAILABLE/REBOUND）")
    )
)]
pub(crate) async fn search_files(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Vec<FileSearchResult>>, ApiError> {
    let search_query = require_param(&query, "query")?.to_owned();
    let limit = parse_spring_int(query.get("limit").map(String::as_str), 20)?;
    let session_id = require_param(&query, "sessionId")?.to_owned();
    let session = state
        .db
        .get_session(&session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&session_id))?;
    let config = state.config.clone();
    let working_dir = session.working_dir.clone();
    let root = tokio::task::spawn_blocking(move || require_current_binding(&config, &working_dir))
        .await
        .map_err(|err| join_internal(&err))??;
    // 旧 `fuzzySearch` 内 `.limit(limit)`：负数在 Java 抛
    // `IllegalArgumentException` → `handleIllegalArgument` → 400 `INVALID_REQUEST`
    // （消息为该负数）。绑定成功后、搜索前复刻，保持与旧异常触发点同序。
    if limit < 0 {
        return Err(ApiError::validation(limit.to_string()));
    }
    let results = tokio::task::spawn_blocking(move || {
        fuzzy_search(&search_query, &root, usize::try_from(limit).unwrap_or(0))
    })
    .await
    .map_err(|err| join_internal(&err))?;
    Ok(Json(results))
}

/// `GET /api/sessions/{sessionId}/files/preview`——会话内文件内联预览（旧 `preview`）。
#[utoipa::path(
    get,
    path = "/api/sessions/{sessionId}/files/preview",
    tag = "files",
    params(("sessionId" = String, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "文件流（inline + nosniff）"),
        (status = 400, description = "MISSING_HEADER / MISSING_PARAMETER / FILE_PATH_REQUIRED"),
        (status = 403, description = "SESSION_CONTEXT_MISMATCH / SESSION_FILE_OUTSIDE_WORKSPACE"),
        (status = 404, description = "SESSION_NOT_FOUND / SESSION_FILE_NOT_FOUND"),
        (status = 413, description = "FILE_PREVIEW_TOO_LARGE"),
        (status = 415, description = "FILE_PREVIEW_REQUIRES_NATIVE_OPEN")
    )
)]
pub(crate) async fn preview(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let asserted = require_session_header(&headers)?;
    let raw_path = require_param(&query, "path")?.to_owned();
    require_matching_session(&session_id, &asserted)?;
    require_path(&raw_path)?;
    let session = state
        .db
        .get_session(&session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&session_id))?;
    let config = state.config.clone();
    let working_dir = session.working_dir.clone();
    let (content_type, size, safe_name, bytes) = tokio::task::spawn_blocking(move || {
        let root = require_current_binding(&config, &working_dir)?;
        let canonical = resolve_within(&root, &raw_path)?;
        let target = preview_target(&canonical)?;
        // 旧 `target.path().getFileName().toString().replace("\"", "")`。
        let safe_name = canonical
            .file_name()
            .map(|name| name.to_string_lossy().replace('"', ""))
            .unwrap_or_default();
        // 读取文件字节；IO 失败复刻旧 `preview` 的 409（元数据/读取不可用）。
        let bytes = std::fs::read(&target.path).map_err(|_| {
            failure(
                StatusCode::CONFLICT,
                "FILE_PREVIEW_UNAVAILABLE",
                "FILE_PREVIEW_UNAVAILABLE",
            )
        })?;
        Ok::<_, ApiError>((target.content_type, target.size, safe_name, bytes))
    })
    .await
    .map_err(|err| join_internal(&err))??;

    let mut response = Response::new(Body::from(bytes));
    let map = response.headers_mut();
    // content_type 源自 fallback 表（恒合法 ASCII）；防御性映射 500。
    map.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&content_type).map_err(|_| ApiError::internal())?,
    );
    map.insert(header::CONTENT_LENGTH, HeaderValue::from(size));
    // 文件名可能含非 ASCII（macOS）；`from_bytes` 接受 obs-text（latin1/UTF-8
    // 字节），比 `from_str` 更贴近 Spring 原样写入的行为。
    let disposition = format!("inline; filename=\"{safe_name}\"");
    map.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_bytes(disposition.as_bytes()).map_err(|_| ApiError::internal())?,
    );
    map.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    Ok(response)
}

/// `POST /api/sessions/{sessionId}/files/reveal`——原生文件管理器揭示（旧 `reveal`）。
#[utoipa::path(
    post,
    path = "/api/sessions/{sessionId}/files/reveal",
    tag = "files",
    params(("sessionId" = String, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "RevealResult{revealed,application}"),
        (status = 400, description = "INVALID_REQUEST_BODY / MISSING_HEADER / FILE_PATH_REQUIRED"),
        (status = 403, description = "SESSION_CONTEXT_MISMATCH / 手势缺失（空体）/ FILE_REVEAL_LOCAL_ONLY"),
        (status = 404, description = "SESSION_NOT_FOUND / SESSION_FILE_NOT_FOUND"),
        (status = 503, description = "FILE_REVEAL_UNAVAILABLE")
    )
)]
pub(crate) async fn reveal(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request = serde_json::from_slice::<RevealFileRequest>(&body)
        .map_err(|_| ApiError::invalid_request_body())?;
    let asserted = require_session_header(&headers)?;
    let gesture = headers
        .get("X-Zhikun-User-Gesture")
        .and_then(|value| value.to_str().ok());
    require_matching_session(&session_id, &asserted)?;
    if gesture != Some("reveal-file") {
        // 旧 `ResponseEntity.status(403).build()`（空体，非错误信封）。
        return Ok(StatusCode::FORBIDDEN.into_response());
    }
    // 服务层 `reveal` 首步 `localDesktopAccessAllowed(remoteAddr)`（回环 + 未
    // 设 allowed roots + 本地选择器启用）。loopback 判定同 `project` handler：
    // `localnet_guard` 已保证 socket 回环，转发头存在即视为非直连。
    let caller_loopback = !crate::api::project::has_forwarding_headers(&headers);
    if !local_desktop_access_allowed(&state.config, caller_loopback) {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "FILE_REVEAL_LOCAL_ONLY",
            "FILE_REVEAL_LOCAL_ONLY",
        ));
    }
    let raw_path = request.path.unwrap_or_default();
    require_path(&raw_path)?;
    let session = state
        .db
        .get_session(&session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&session_id))?;
    let config = state.config.clone();
    let working_dir = session.working_dir.clone();
    let result: RevealResult = tokio::task::spawn_blocking(move || {
        let root = require_current_binding(&config, &working_dir)?;
        let canonical = resolve_within(&root, &raw_path)?;
        reveal_path(&canonical)
    })
    .await
    .map_err(|err| join_internal(&err))??;
    Ok(Json(result).into_response())
}
