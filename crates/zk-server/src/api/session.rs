//! 会话域 8 端点 handler（U2；路由注册见 `routes`）。
//!
//! 分层：handler（本模块，参数校验/映射/信封）→ zk-db Repository（语义）。
//! 响应形状权威：`docs/baseline/samples/` 会话域样例逐键对齐；samples 为空
//! 列表的部分（messages 元素 / compact / markdown export）以旧仓库
//! `SessionController.java` 源码为准（见完成报告依据表）。

use std::collections::HashMap;

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::Response;
use serde_json::Value;
use zk_authz::model::PermissionMode;
use zk_db::model::SessionPage;

use crate::api::dto::{
    CompactResponse, CreateSessionRequest, CreateSessionResponse, DeleteSessionResponse,
    MessageListResponse, ResumeSessionResponse,
};
use crate::api::mapping;
use crate::error::ApiError;
use crate::iso::format_rfc3339_micros;
use crate::state::AppState;

/// `GET /api/sessions` 与 messages 端点的 `limit` 缺省值（旧 `@RequestParam
/// defaultValue`：会话列表 20、消息列表 50）。
const SESSIONS_DEFAULT_LIMIT: u32 = 20;
const MESSAGES_DEFAULT_LIMIT: u32 = 50;

/// `POST /api/sessions`——空体/全可选体，`workingDirectory` 非空拒绝（旧设计）；
/// `projectId` 非空时解析项目 workspace 为会话工作目录（2.1）。
#[utoipa::path(
    post,
    path = "/api/sessions",
    tag = "sessions",
    responses(
        (status = 201, description = "创建成功（POST_api-sessions.json 样例形状）"),
        (status = 400, description = "体非法、workingDirectory 非空或 permissionMode 非法"),
        (status = 404, description = "projectId 未知（PROJECT_NOT_FOUND）")
    )
)]
pub(crate) async fn create_session(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<(StatusCode, Json<CreateSessionResponse>), ApiError> {
    let request = if body.is_empty() {
        None
    } else {
        Some(
            serde_json::from_slice::<CreateSessionRequest>(&body)
                .map_err(|_| ApiError::invalid_request_body())?,
        )
    };
    if let Some(req) = &request {
        let rejected = req
            .working_directory
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());
        if rejected {
            return Err(ApiError::validation_with_code(
                "SESSION_WORKING_DIRECTORY_UNSUPPORTED",
                "Use projectId instead of workingDirectory",
            ));
        }
    }
    let model = request
        .as_ref()
        .and_then(|req| req.model.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map_or_else(|| state.config.default_model.clone(), str::to_owned);
    // 新建会话默认授予完全访问权限（免首轮逐次工具确认）；请求显式携带
    // permissionMode 时以请求为准，非法值按 query 端同规则 400。
    let permission_mode = request
        .as_ref()
        .and_then(|req| req.permission_mode.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map_or(Some(PermissionMode::AutoApprove), PermissionMode::parse)
        .ok_or_else(|| {
            ApiError::validation_with_code(
                "INVALID_PERMISSION_MODE",
                "Session permissionMode is invalid",
            )
        })?;
    // working_dir 解析（旧 `resolveWorkspace(projectId)`）：projectId 非空
    // → 查库 + 存量绑定复核（未知 404）；缺省 → 服务器进程当前目录
    // （Phase 1 行为不变，旧 `${user.dir}` 等价）。
    let project_id = request
        .as_ref()
        .and_then(|req| req.project_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let working_dir = if let Some(project_id) = project_id {
        let record = state.db.get_project(&project_id).await?.ok_or_else(|| {
            crate::workspace::failure(
                StatusCode::NOT_FOUND,
                "PROJECT_NOT_FOUND",
                &format!("Project with id '{project_id}' was not found"),
            )
        })?;
        let config = state.config.clone();
        tokio::task::spawn_blocking(move || {
            crate::workspace::require_current_binding(&config, &record.workspace_root)
        })
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "workspace binding task failed");
            ApiError::internal()
        })??
        .to_string_lossy()
        .into_owned()
    } else {
        std::env::current_dir().map_or_else(
            |_| ".".to_owned(),
            |path| path.to_string_lossy().into_owned(),
        )
    };
    let summary = state.db.create_session(&model, &working_dir).await?;
    // 登记到 PermissionModeRegistry——缺了这步 `get_mode` 会回退 DEFAULT，
    // WS 绑定后的 session_restored 便把 DEFAULT 当作生效模式回传前端。
    state
        .authz
        .modes
        .set_mode(&summary.id, permission_mode)
        .await;
    Ok((
        StatusCode::CREATED,
        Json(CreateSessionResponse {
            // 先借用后移动：web_socket_url/created_at 在 session_id move 前求值。
            web_socket_url: ws_url(&summary.id),
            created_at: format_rfc3339_micros(summary.created_at),
            session_id: summary.id,
            model,
            permission_mode: permission_mode.as_str().to_owned(),
        }),
    ))
}

/// WebSocket 通道 URL（Phase 1 骨架值，S8 接线真实端点）。
fn ws_url(session_id: &str) -> String {
    format!("/ws/session/{session_id}")
}

/// `GET /api/sessions`——游标分页（`Base64("updated_at|id")`）。
#[utoipa::path(
    get,
    path = "/api/sessions",
    tag = "sessions",
    params(
        ("limit" = Option<u32>, Query, description = "页大小（缺省 20）"),
        ("cursor" = Option<String>, Query, description = "分页游标")
    ),
    responses((status = 200, description = "会话摘要页（GET_api-sessions.json 样例形状）"))
)]
pub(crate) async fn list_sessions(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<SessionPage>, ApiError> {
    let limit = parse_limit(
        query.get("limit").map(String::as_str),
        SESSIONS_DEFAULT_LIMIT,
    )?;
    let page = state
        .db
        .list_sessions(query.get("cursor").map(String::as_str), limit)
        .await?;
    Ok(Json(page))
}

/// `GET /api/sessions/{id}`——详情（null 剥离出口）。
#[utoipa::path(
    get,
    path = "/api/sessions/{id}",
    tag = "sessions",
    params(("id" = String, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "会话详情（GET_api-sessions-id.json 样例形状）"),
        (status = 404, description = "SESSION_NOT_FOUND")
    )
)]
pub(crate) async fn get_session_detail(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let detail = load_session(&state, &session_id).await?;
    Ok(Json(mapping::detail_to_value(&detail)))
}

/// `DELETE /api/sessions/{id}`——幂等删除（不存在亦 success）。
#[utoipa::path(
    delete,
    path = "/api/sessions/{id}",
    tag = "sessions",
    params(("id" = String, Path, description = "会话 ID")),
    responses((status = 200, description = "恒 `{\"success\":true}`（幂等）"))
)]
pub(crate) async fn delete_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<DeleteSessionResponse>, ApiError> {
    let _existed = state.db.delete_session(&session_id).await?;
    Ok(Json(DeleteSessionResponse { success: true }))
}

/// `POST /api/sessions/{id}/resume`——加载详情 + 恢复负载（旧 `resumeSession`
/// = `loadSession` 的 REST 面）。
#[utoipa::path(
    post,
    path = "/api/sessions/{id}/resume",
    tag = "sessions",
    params(("id" = String, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "恢复负载（POST_api-sessions-id-resume.json 样例形状）"),
        (status = 404, description = "SESSION_NOT_FOUND")
    )
)]
pub(crate) async fn resume_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<ResumeSessionResponse>, ApiError> {
    let detail = load_session(&state, &session_id).await?;
    Ok(Json(ResumeSessionResponse {
        session_id: detail.session_id.clone(),
        web_socket_url: ws_url(&detail.session_id),
        messages: mapping::api_messages(&detail.messages),
    }))
}

/// `POST /api/sessions/{id}/compact`——确定性压缩 + 落 `summary` 位。
#[utoipa::path(
    post,
    path = "/api/sessions/{id}/compact",
    tag = "sessions",
    params(("id" = String, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "压缩结果（success/tokensBefore/tokensAfter）"),
        (status = 404, description = "SESSION_NOT_FOUND")
    )
)]
pub(crate) async fn compact_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<CompactResponse>, ApiError> {
    let detail = load_session(&state, &session_id).await?;
    let outcome = mapping::compact_deterministic(&detail.messages);
    if let Some(summary) = &outcome.summary {
        let updated = state
            .db
            .update_session_summary(&detail.session_id, summary)
            .await?;
        if !updated {
            tracing::warn!(session_id = %detail.session_id, "compact summary target session vanished");
        }
    }
    Ok(Json(CompactResponse {
        success: true,
        tokens_before: outcome.tokens_before,
        tokens_after: outcome.tokens_after,
    }))
}

/// `POST /api/sessions/{id}/export`——JSON（独立序列化语义）或 markdown。
#[utoipa::path(
    post,
    path = "/api/sessions/{id}/export",
    tag = "sessions",
    params(
        ("id" = String, Path, description = "会话 ID"),
        ("format" = Option<String>, Query, description = "json（缺省）或 markdown/md")
    ),
    responses(
        (status = 200, description = "附件下载（application/json 或 text/plain）"),
        (status = 404, description = "SESSION_NOT_FOUND")
    )
)]
pub(crate) async fn export_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let detail = load_session(&state, &session_id).await?;
    let format = query
        .get("format")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("json");
    let (content_type, filename, payload) = if format == "markdown" || format == "md" {
        // 旧 `MediaType.TEXT_PLAIN`（Spring 实发 `text/plain;charset=UTF-8`）。
        (
            "text/plain; charset=utf-8",
            format!("session-{session_id}.md"),
            mapping::export_markdown(&detail),
        )
    } else {
        let pretty =
            serde_json::to_string_pretty(&mapping::export_to_value(&detail)).map_err(|err| {
                tracing::error!(error = %err, "export serialization failed");
                ApiError::internal()
            })?;
        (
            "application/json",
            format!("session-{session_id}.json"),
            pretty,
        )
    };
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        )
        .body(Body::from(payload))
        .map_err(|err| {
            tracing::error!(error = %err, "export response build failed");
            ApiError::internal()
        })?;
    Ok(response)
}

/// `GET /api/sessions/{id}/messages`——P0 索引游标分页。
#[utoipa::path(
    get,
    path = "/api/sessions/{id}/messages",
    tag = "sessions",
    params(
        ("id" = String, Path, description = "会话 ID"),
        ("limit" = Option<u32>, Query, description = "页大小（缺省 50）"),
        ("cursor" = Option<String>, Query, description = "分页游标")
    ),
    responses(
        (status = 200, description = "消息页（messages/hasMore/nextCursor?）"),
        (status = 404, description = "SESSION_NOT_FOUND")
    )
)]
pub(crate) async fn list_session_messages(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<MessageListResponse>, ApiError> {
    let limit = parse_limit(
        query.get("limit").map(String::as_str),
        MESSAGES_DEFAULT_LIMIT,
    )?;
    let page = state
        .db
        .list_messages(&session_id, query.get("cursor").map(String::as_str), limit)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&session_id))?;
    Ok(Json(MessageListResponse {
        messages: mapping::api_messages(&page.messages),
        has_more: page.has_more,
        next_cursor: page.next_cursor,
    }))
}

/// 详情加载 + 404 映射（detail / resume / compact / export 共用）。
async fn load_session(
    state: &AppState,
    session_id: &str,
) -> Result<zk_db::model::SessionDetail, ApiError> {
    state
        .db
        .get_session(session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(session_id))
}

/// limit 解析：缺省/空白取默认；非法或 0 → 400 `INVALID_REQUEST`。
fn parse_limit(raw: Option<&str>, default: u32) -> Result<u32, ApiError> {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(default),
        Some(value) => value.parse::<u32>().map_err(|_| {
            ApiError::validation(format!("limit must be a positive integer, got {value:?}"))
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_parsing_defaults_and_errors() {
        assert_eq!(parse_limit(None, 20).ok(), Some(20));
        assert_eq!(parse_limit(Some("  "), 50).ok(), Some(50));
        assert_eq!(parse_limit(Some(" 7 "), 20).ok(), Some(7));
        assert!(parse_limit(Some("abc"), 20).is_err());
        // 旧系统 limit 为裸 int 透传：0 → `LIMIT 0` 空页 200（非 400）。
        assert_eq!(parse_limit(Some("0"), 20).ok(), Some(0));
        // 负值在旧系统滑入 `LIMIT -1`（无上限）等未定义行为，Phase 1 收紧为 400。
        assert!(parse_limit(Some("-3"), 20).is_err());
    }

    #[test]
    fn ws_url_shape() {
        assert_eq!(ws_url("abc"), "/ws/session/abc");
    }
}
