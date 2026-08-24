//! 已完成权限交互所创建授权的查询与撤销 API（2.5；旧
//! `controller/PermissionGrantController.java` 57 行逐分支复刻）。
//!
//! 两个端点：
//! - `GET /api/permissions/grants?limit=200`（旧 L34-40）
//! - `DELETE /api/permissions/grants/{grantId}`（旧 L42-57）
//!
//! 会话归属校验用 `canAccessSession(sessionId, sessionId)`——两参同值，等价于
//! 「该会话必须在库」（旧源如此；网络层已由 `localnet_guard` 守住）。
//!
//! 403 形状两端不同（逐字保留）：列表端点抛 `ResponseStatusException(403,
//! "SESSION_ACCESS_DENIED")`，经 `GlobalExceptionHandler.handleResponseStatus`
//!（L88-93）成为信封 `{"error":{"code":"SESSION_ACCESS_DENIED","message":
//! "SESSION_ACCESS_DENIED",...}}`；撤销端点直接 `status(403).build()` 空体。

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use zk_authz::grants::GrantView;

use crate::error::ApiError;
use crate::session_access::{can_access_session, require_session_header};
use crate::state::AppState;

/// 旧 `Map.of("grants", ...)` 单键信封。
#[derive(Debug, Serialize)]
pub(crate) struct GrantListBody {
    /// 该会话当前生效的授权（`created_at` 降序，上限见 `limit`）。
    grants: Vec<GrantView>,
}

/// 旧 `@RequestParam(defaultValue = "200") int limit`。
///
/// 非数字 `limit` 在旧 Spring 走 `MethodArgumentTypeMismatchException`（无专门
/// handler → `handleGeneric` 500）；这里沿用 `INVALID_REQUEST` 400，理由见 §8
/// 偏离表 PG-01（500 是旧实现的缺陷面，非契约面）。
fn parse_limit(query: &HashMap<String, String>) -> Result<i64, ApiError> {
    match query.get("limit") {
        None => Ok(200),
        Some(raw) => raw
            .parse::<i64>()
            .map_err(|_| ApiError::validation("Parameter 'limit' must be an integer")),
    }
}

/// `GET /api/permissions/grants`——当前会话生效授权列表（旧 L34-40）。
#[utoipa::path(
    get,
    path = "/api/permissions/grants",
    tag = "permissions",
    responses(
        (status = 200, description = "{\"grants\":[...]}（GrantView 八字段）"),
        (status = 403, description = "SESSION_ACCESS_DENIED 信封")
    )
)]
pub(crate) async fn list_active(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<GrantListBody>, ApiError> {
    let session_id = require_session_header(&headers)?;
    let limit = parse_limit(&query)?;
    // 旧 `requireAccessibleSession`（L59-65）。
    if !can_access_session(&state, &session_id, &session_id).await? {
        tracing::warn!(session_id = %session_id, "Permission grant list access denied");
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            code: "SESSION_ACCESS_DENIED".to_owned(),
            message: "SESSION_ACCESS_DENIED".to_owned(),
        });
    }
    let grants = state
        .authz
        .grants
        .list_active_for_session(&session_id, limit)
        .await?;
    Ok(Json(GrantListBody { grants }))
}

/// `DELETE /api/permissions/grants/{grantId}`——撤销单条授权（旧 L42-57）。
#[utoipa::path(
    delete,
    path = "/api/permissions/grants/{grantId}",
    tag = "permissions",
    responses(
        (status = 204, description = "已撤销"),
        (status = 400, description = "grantId 空白或超 128 字符（空体）"),
        (status = 403, description = "会话归属校验失败（空体）"),
        (status = 404, description = "该会话下无此授权（空体）")
    )
)]
pub(crate) async fn revoke(
    State(state): State<AppState>,
    AxumPath(grant_id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let session_id = require_session_header(&headers)?;
    // 旧 L45-47：空白或过长 → 400 空体（`isBlank` 含全空白串）。
    if grant_id.trim().is_empty() || grant_id.chars().count() > 128 {
        return Ok(StatusCode::BAD_REQUEST.into_response());
    }
    // 旧 L48-51：403 空体（与列表端点的信封形状不同，逐字保留）。
    if !can_access_session(&state, &session_id, &session_id).await? {
        tracing::warn!(
            grant_id = %grant_id,
            session_id = %session_id,
            "Permission grant revoke access denied"
        );
        return Ok(StatusCode::FORBIDDEN.into_response());
    }
    // 旧 L52-56：影响行数恰为 1 才 204，否则 404（撤销限定在本会话名下）。
    if state
        .authz
        .grants
        .revoke_for_session(&grant_id, &session_id)
        .await?
        == 1
    {
        tracing::info!(
            grant_id = %grant_id,
            session_id = %session_id,
            "Permission grant revoked through API"
        );
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    Ok(StatusCode::NOT_FOUND.into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_defaults_to_legacy_200() {
        assert_eq!(parse_limit(&HashMap::new()).expect("default"), 200);
    }

    #[test]
    fn limit_parsed_from_query() {
        let query = HashMap::from([("limit".to_owned(), "7".to_owned())]);
        assert_eq!(parse_limit(&query).expect("parsed"), 7);
    }

    #[test]
    fn non_numeric_limit_rejected() {
        let query = HashMap::from([("limit".to_owned(), "many".to_owned())]);
        let err = parse_limit(&query).expect_err("rejected");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }
}
