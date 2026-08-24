//! REST 错误模型——`DbError` → HTTP 映射与统一错误响应（§19）。
//!
//! 功能对齐后的公共契约为扁平结构：
//!
//! ```json
//! { "code": "SESSION_NOT_FOUND", "message": "...", "requestId": "..." }
//! ```
//! 映射表：
//!
//! | 来源 | HTTP | code |
//! |---|---|---|
//! | `DbError::SessionNotFound` | 404 | `SESSION_NOT_FOUND` |
//! | `DbError::WorkspaceRootTaken`（`projects.workspace_root` UNIQUE） | 409 | `PROJECT_PATH_DUPLICATE` |
//! | 会话域 `Ok(None)`（详情/消息/resume/export/compact） | 404 | `SESSION_NOT_FOUND` |
//! | 参数校验（workingDirectory / limit / body 非法 JSON） | 400 | `SESSION_WORKING_DIRECTORY_UNSUPPORTED` / `INVALID_REQUEST` / `INVALID_REQUEST_BODY` |
//! | 非 loopback 对端 | 403 | `ACCESS_DENIED` |
//! | handler panic（CatchPanic）与其余 `DbError` | 500 | `INTERNAL_ERROR` |
//!
//! §19 脱敏：500 的 `message` 恒为旧文案「An unexpected error occurred」，
//! 不透出 `DbError` 内部细节；底层错误仅进 tracing ERROR（结构化字段），
//! 不进入响应体。

use axum::Json;
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use zk_db::DbError;

static REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// REST API 错误——handler 统一 `Result<_, ApiError>` 出口（§19 panic 边界）。
#[derive(Debug, thiserror::Error)]
#[error("{status} {code}: {message}")]
pub struct ApiError {
    /// HTTP 状态码。
    pub status: StatusCode,
    /// 稳定错误码（旧 `ResourceNotFoundException.code` 语义）。
    pub code: String,
    /// 面向调用方的错误描述（不含内部细节）。
    pub message: String,
}

impl ApiError {
    /// 能力尚未通过生产验收（503）。不得用空实现或固定成功响应伪装可用。
    #[must_use]
    pub fn feature_not_ready(capability: &str, enable_when: &str) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "FEATURE_NOT_READY".into(),
            message: format!("{capability} is temporarily unavailable; enable after {enable_when}"),
        }
    }

    /// `SESSION_NOT_FOUND`（404，文案对齐旧 `SessionNotFoundException`）。
    #[must_use]
    pub fn session_not_found(session_id: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "SESSION_NOT_FOUND".into(),
            message: format!("Session with id '{session_id}' not found"),
        }
    }

    /// `INVALID_REQUEST`（400）。
    #[must_use]
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "INVALID_REQUEST".into(),
            message: message.into(),
        }
    }

    /// 旧系统带稳定 code 的参数校验失败（400，如
    /// `SESSION_WORKING_DIRECTORY_UNSUPPORTED`）。
    #[must_use]
    pub fn validation_with_code(code: &str, message: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: code.to_owned(),
            message: message.to_owned(),
        }
    }

    /// 请求体缺失/非法 JSON（400，对齐旧 `HttpMessageNotReadableException`
    /// 分支的 `INVALID_REQUEST_BODY`）。
    #[must_use]
    pub fn invalid_request_body() -> Self {
        Self::validation_with_code(
            "INVALID_REQUEST_BODY",
            "Request body is missing or malformed",
        )
    }

    /// `ACCESS_DENIED`（403，文案对齐旧 `RemoteAccessSecurityFilter`
    /// 「only private network allowed」）。
    #[must_use]
    pub fn access_denied() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "ACCESS_DENIED".into(),
            message: "Access denied: only private network allowed".into(),
        }
    }

    /// `AUTHENTICATION_REQUIRED`（401，文案逐字照抄旧
    /// `RemoteAccessSecurityFilter` L172-173 的 `sendError` 第二参）。
    #[must_use]
    pub fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "AUTHENTICATION_REQUIRED".into(),
            message: "Authentication required. Use token URL or Bearer header.".into(),
        }
    }

    /// 带稳定 code 的资源缺失（404，对齐旧 `ResourceNotFoundException`
    /// 双参构造：`code` 进信封 `code`，`message` 逐字照抄旧文案）。
    #[must_use]
    pub fn not_found(code: &str, message: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: code.to_owned(),
            message: message.to_owned(),
        }
    }

    /// `INTERNAL_ERROR`（500，文案对齐旧 `handleGeneric`，不透内部细节）。
    #[must_use]
    pub fn internal() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "INTERNAL_ERROR".into(),
            message: "An unexpected error occurred".into(),
        }
    }
}

impl From<DbError> for ApiError {
    fn from(err: DbError) -> Self {
        match err {
            // FK 违例归一产物（zk-db map_fk_violation）：映射 404。
            DbError::SessionNotFound(id) => Self::session_not_found(&id),
            // UNIQUE 违例归一产物（zk-db map_unique_violation）：映射 409，
            // 文案对齐旧 `ProjectWorkspaceService.create` 的 duplicate 分支。
            DbError::WorkspaceRootTaken(_) => Self {
                status: StatusCode::CONFLICT,
                code: "PROJECT_PATH_DUPLICATE".into(),
                message: "A Project already uses this workspace".into(),
            },
            // 其余（Sqlite/Migration/Join/Json/Io/Invalid）：500，细节只进日志。
            other => {
                tracing::error!(error = %other, "internal database failure");
                Self::internal()
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let request_id = uuid::Uuid::new_v4().to_string();
        let status = self.status;
        let mut response = (
            status,
            Json(ErrorResponse {
                code: self.code,
                message: self.message,
                request_id: request_id.clone(),
            }),
        )
            .into_response();
        response.headers_mut().insert(
            &REQUEST_ID_HEADER,
            HeaderValue::from_str(&request_id).expect("UUID is a valid header value"),
        );
        response
    }
}

/// 公共 REST 错误响应。`requestId` 与响应头 `x-request-id` 一致。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ErrorResponse {
    /// 稳定错误码。
    code: String,
    /// 错误描述。
    message: String,
    /// 可用于日志对账的请求标识。
    request_id: String,
}

/// `CatchPanic` 兜底响应（§19 panic 边界：panic 不跨 API，转 500 信封）。
///
/// panic 载荷不落日志（可能携带业务数据）；仅记「handler panicked」事实。
#[must_use]
pub fn panic_response() -> Response {
    tracing::error!("handler panicked, recovered to 500 envelope");
    ApiError::internal().into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_session_not_found_maps_404_with_legacy_text() {
        let err = ApiError::from(DbError::SessionNotFound("abc".into()));
        assert_eq!(err.status, StatusCode::NOT_FOUND);
        assert_eq!(err.code, "SESSION_NOT_FOUND");
        assert_eq!(err.message, "Session with id 'abc' not found");
    }

    #[test]
    fn db_workspace_root_taken_maps_409_duplicate() {
        let err = ApiError::from(DbError::WorkspaceRootTaken("/tmp/ws".into()));
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert_eq!(err.code, "PROJECT_PATH_DUPLICATE");
        assert_eq!(err.message, "A Project already uses this workspace");
    }

    #[test]
    fn db_internal_maps_500_with_generic_text() {
        let io_err = std::io::Error::other("disk on fire");
        let err = ApiError::from(DbError::Io(io_err));
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.code, "INTERNAL_ERROR");
        assert_eq!(err.message, "An unexpected error occurred");
    }

    #[test]
    fn response_serializes_flat_contract() {
        let body = serde_json::to_value(ErrorResponse {
            code: "SESSION_NOT_FOUND".into(),
            message: "Session with id 's1' not found".into(),
            request_id: "request-1".into(),
        })
        .expect("serializable");
        let mut keys: Vec<&str> = body
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["code", "message", "requestId"]);
        assert_eq!(body["code"], "SESSION_NOT_FOUND");
        assert_eq!(body["requestId"], "request-1");
    }
}
