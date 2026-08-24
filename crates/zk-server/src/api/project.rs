//! Projects 域 5 端点 handler（2.1；路由注册见 `routes`）。
//!
//! 语义来源（旧仓库只读）：`ProjectController.java`（121 行逐分支复刻——
//! header 守卫 / 转发头守卫 / 空体分支 / 201 语义）；服务层校验与目录浏览
//! 见 [`crate::workspace`]。响应形状：`ProjectRecord` serde camelCase 直接
//! 序列化即旧 Jackson 形状（`{id,name,workspaceRoot,createdAt}`）。
//!
//! loopback 判定：`localnet_guard` 已保证对端 socket 为回环，故此处
//! `caller_loopback = !has_forwarding_headers`（旧 `workspaceCallerAddress`
//! 在转发头存在时回 null → 非 loopback 的等价表达）。

use std::collections::HashMap;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use zk_db::ProjectRecord;

use crate::error::ApiError;
use crate::state::AppState;
use crate::workspace::{self, DirectoryListing, PickerOutcome, failure};

/// `POST /api/projects` 请求体（旧 `CreateProjectRequest` record，全可选，
/// 缺失字段在校验层回稳定错误码）。
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateProjectRequest {
    /// 项目名（trim 后 1..=80 字符）。
    pub name: Option<String>,
    /// workspace 绝对路径。
    pub workspace_root: Option<String>,
}

/// `DELETE /api/projects/{projectId}` 200 响应（旧 `RevocationResult`
/// record：`{projectId,revoked}`，幂等——不存在时 `revoked:false`）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RevocationResult {
    /// 请求的项目 id（trim 后回显）。
    pub project_id: String,
    /// 是否实际删除了记录。
    pub revoked: bool,
}

/// 旧 `FORWARDED_HEADERS` 五头名单：任一存在即视为经代理转发。
const FORWARDING_HEADERS: [&str; 5] = [
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-real-ip",
];

/// 请求是否携带任一转发头（旧 `hasForwardingHeaders`）。
pub(crate) fn has_forwarding_headers(headers: &HeaderMap) -> bool {
    FORWARDING_HEADERS
        .iter()
        .any(|name| headers.contains_key(*name))
}

/// `spawn_blocking` join 失败归一（任务被取消/panic 已由 `CatchPanic` 之外
/// 的运行时路径触达，统一 500）。
fn join_internal(err: &tokio::task::JoinError) -> ApiError {
    tracing::error!(error = %err, "workspace blocking task failed");
    ApiError::internal()
}

/// `GET /api/projects`——项目列表（`created_at` 降序，旧 repository 语义）。
#[utoipa::path(
    get,
    path = "/api/projects",
    tag = "projects",
    responses((status = 200, description = "项目数组（{id,name,workspaceRoot,createdAt}）"))
)]
pub(crate) async fn list_projects(
    State(state): State<AppState>,
) -> Result<Json<Vec<ProjectRecord>>, ApiError> {
    Ok(Json(state.db.list_projects().await?))
}

/// `POST /api/projects`——创建项目（201；守卫 → 名称校验 → 路径规范化 →
/// 落库，UNIQUE 冲突 → 409 `PROJECT_PATH_DUPLICATE` 于错误映射层）。
#[utoipa::path(
    post,
    path = "/api/projects",
    tag = "projects",
    responses(
        (status = 201, description = "创建成功（{id,name,workspaceRoot,createdAt}）"),
        (status = 400, description = "体缺失（WORKSPACE_REQUIRED）/ 非法 JSON（INVALID_REQUEST_BODY）/ 名称或路径校验失败"),
        (status = 403, description = "LOCAL_PICKER_DISABLED / REMOTE_PROJECT_CREATE_FORBIDDEN / WORKSPACE_ACCESS_DENIED"),
        (status = 409, description = "workspace 已被占用（PROJECT_PATH_DUPLICATE）")
    )
)]
pub(crate) async fn create_project(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<ProjectRecord>), ApiError> {
    if body.is_empty() {
        // 旧 Controller 的 `request == null` 分支（required=false 空体）。
        return Err(failure(
            StatusCode::BAD_REQUEST,
            "WORKSPACE_REQUIRED",
            "Request body is required",
        ));
    }
    let request = serde_json::from_slice::<CreateProjectRequest>(&body)
        .map_err(|_| ApiError::invalid_request_body())?;
    let caller_loopback = !has_forwarding_headers(&headers);
    workspace::assert_create_allowed(&state.config, caller_loopback)?;
    let name = workspace::require_name(request.name.as_deref())?;
    let config = state.config.clone();
    let workspace_root = request.workspace_root.clone();
    let canonical = tokio::task::spawn_blocking(move || {
        workspace::canonicalize_for_create(&config, workspace_root.as_deref())
    })
    .await
    .map_err(|err| join_internal(&err))??;
    let record = state
        .db
        .create_project(&name, &canonical.to_string_lossy())
        .await?;
    Ok((StatusCode::CREATED, Json(record)))
}

/// `GET /api/projects/directories`——目录浏览（`?path=` 可选，缺省取默认根）。
#[utoipa::path(
    get,
    path = "/api/projects/directories",
    tag = "projects",
    params(("path" = Option<String>, Query, description = "目标目录（绝对路径，缺省取默认根/首根）")),
    responses(
        (status = 200, description = "目录清单（{roots,current,parent?,directories,nativePickerAvailable}）"),
        (status = 400, description = "路径非法（WORKSPACE_ABSOLUTE_REQUIRED / DIRECTORY_PATH_NOT_CANONICAL / WORKSPACE_NOT_FOUND）"),
        (status = 403, description = "LOCAL_PICKER_DISABLED / REMOTE_DIRECTORY_BROWSE_FORBIDDEN / DIRECTORY_BROWSE_OUTSIDE_ROOTS"),
        (status = 409, description = "目录失效或 symlink 重绑定（WORKSPACE_UNAVAILABLE / WORKSPACE_REBOUND）")
    )
)]
pub(crate) async fn browse_directories(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<DirectoryListing>, ApiError> {
    let caller_loopback = !has_forwarding_headers(&headers);
    let config = state.config.clone();
    let path = query.get("path").cloned();
    let listing = tokio::task::spawn_blocking(move || {
        workspace::browse_directories(&config, path.as_deref(), caller_loopback)
    })
    .await
    .map_err(|err| join_internal(&err))??;
    Ok(Json(listing))
}

/// `POST /api/projects/directories/pick`——原生目录选择器（macOS
/// osascript；取消 → 204；选中 → 该目录的浏览清单）。
#[utoipa::path(
    post,
    path = "/api/projects/directories/pick",
    tag = "projects",
    responses(
        (status = 200, description = "选中目录的浏览清单（同 directories 端点形状）"),
        (status = 204, description = "用户取消选择"),
        (status = 403, description = "NATIVE_PICKER_HEADER_REQUIRED / NATIVE_PICKER_FORWARDED_REQUEST / NATIVE_PICKER_FORBIDDEN"),
        (status = 409, description = "已有选择器打开（NATIVE_PICKER_BUSY）"),
        (status = 501, description = "选择器不可用（NATIVE_PICKER_UNAVAILABLE）"),
        (status = 503, description = "选择器运行失败（NATIVE_PICKER_UNAVAILABLE）"),
        (status = 504, description = "选择超时（NATIVE_PICKER_TIMEOUT）")
    )
)]
pub(crate) async fn pick_directory(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    // 旧 Controller 守卫顺序：意图头 → 转发头 → 服务层三条件 + 可用性。
    let intent = headers
        .get("x-zhikun-native-picker")
        .and_then(|value| value.to_str().ok());
    if intent != Some("1") {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "NATIVE_PICKER_HEADER_REQUIRED",
            "X-Zhikun-Native-Picker: 1 is required",
        ));
    }
    if has_forwarding_headers(&headers) {
        return Err(failure(
            StatusCode::FORBIDDEN,
            "NATIVE_PICKER_FORWARDED_REQUEST",
            "Native folder selection is unavailable through a proxy",
        ));
    }
    workspace::assert_native_picker_allowed(&state.config, true)?;
    let outcome = workspace::run_native_picker().await?;
    match outcome {
        PickerOutcome::Cancelled => Ok(StatusCode::NO_CONTENT.into_response()),
        PickerOutcome::Selected(selected) => {
            // 选中路径按创建标准复核后回其浏览清单（旧 pick 成功分支）。
            let config = state.config.clone();
            let listing = tokio::task::spawn_blocking(move || {
                let canonical = workspace::canonicalize_for_create(&config, Some(&selected))?;
                workspace::browse_directories(&config, Some(&canonical.to_string_lossy()), true)
            })
            .await
            .map_err(|err| join_internal(&err))??;
            Ok(Json(listing).into_response())
        }
    }
}

/// `DELETE /api/projects/{projectId}`——撤销项目（幂等；旧 `revoke`）。
#[utoipa::path(
    delete,
    path = "/api/projects/{projectId}",
    tag = "projects",
    params(("projectId" = String, Path, description = "项目 ID")),
    responses(
        (status = 200, description = "撤销结果（{projectId,revoked}，不存在时 revoked:false）"),
        (status = 400, description = "projectId 空白（PROJECT_ID_REQUIRED）")
    )
)]
pub(crate) async fn revoke_project(
    State(state): State<AppState>,
    AxumPath(project_id): AxumPath<String>,
) -> Result<Json<RevocationResult>, ApiError> {
    let trimmed = project_id.trim();
    if trimmed.is_empty() {
        return Err(failure(
            StatusCode::BAD_REQUEST,
            "PROJECT_ID_REQUIRED",
            "projectId is required",
        ));
    }
    let revoked = state.db.delete_project(trimmed).await?;
    Ok(Json(RevocationResult {
        project_id: trimmed.to_owned(),
        revoked,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwarding_headers_detected_case_insensitively() {
        let mut headers = HeaderMap::new();
        assert!(!has_forwarding_headers(&headers));
        headers.insert("X-Forwarded-For", "10.0.0.1".parse().expect("value"));
        assert!(has_forwarding_headers(&headers));

        let mut real_ip = HeaderMap::new();
        real_ip.insert("x-real-ip", "10.0.0.1".parse().expect("value"));
        assert!(has_forwarding_headers(&real_ip));
    }

    #[test]
    fn revocation_result_serializes_legacy_shape() {
        let value = serde_json::to_value(RevocationResult {
            project_id: "p1".into(),
            revoked: true,
        })
        .expect("json");
        let keys: Vec<&str> = value
            .as_object()
            .expect("obj")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, vec!["projectId", "revoked"]);
    }
}
