//! 应用对象授权——旧 `security/SessionAccessAuthorizer.java`（37 行）逐行移植。
//!
//! 网络层鉴权由 `localnet_guard`（旧 `RemoteAccessSecurityFilter`）负责；本模块
//! 只回答「客户端自称的会话 id 是否确实存在、且与被访问对象归属一致」。
//!
//! 旧注释明确写下的不变量：**传输在线状态不是授权信号**——断线重连恢复必须能在
//! 会话暂时离线时工作，故这里只查 DB 存在性，不看 WS 通道。
//!
//! 旧 `accessibleRun(runId, assertedSessionId)`（L34-36）于 Batch 2 端点域随
//! `RunController` 移植（见 [`accessible_run`]）；其另两个调用方 Artifact /
//! Workbench 域（`ArtifactController.java:64,82`、`WorkbenchController.java:32`）
//! 仍未移植，随对应域一并补齐（见 `docs/compatibility.md` §8 偏离表 RA-01）。

use axum::http::HeaderMap;
use zk_db::{DbError, RunEnvelopeView};

use crate::error::ApiError;
use crate::state::AppState;

/// 旧 `canAccessSession(requestedSessionId, assertedSessionId)`（L29-32）。
///
/// 三个合取项逐字对齐：
/// 1. `requestedSessionId != null`——Rust 侧由 `&str` 类型消除（缺参/缺头在
///    extractor 层已回 400，见 [`require_session_header`]）；
/// 2. `requestedSessionId.equals(assertedSessionId)`——按字节相等，不做 trim；
/// 3. `sessions.loadSession(requestedSessionId).isPresent()`——会话必须在库。
///
/// # Errors
///
/// 会话查询失败时返回 [`DbError`]（旧实现同样让异常上抛 → 500 信封）。
pub(crate) async fn can_access_session(
    state: &AppState,
    requested_session_id: &str,
    asserted_session_id: &str,
) -> Result<bool, DbError> {
    if requested_session_id != asserted_session_id {
        return Ok(false);
    }
    Ok(state.db.get_session(requested_session_id).await?.is_some())
}

/// 旧 `SessionAccessAuthorizer.accessibleRun(runId, assertedSessionId)`
/// （L34-36）：Run 存在**且**其归属会话通过 [`can_access_session`] 时返回该
/// Run 视图，否则 `None`（调用方据此回 403/404，见 `api::run`）。
///
/// 旧实现先 `envelopeRepository.findById(runId)` 再 `canAccessSession(run
/// .sessionId(), asserted)`——本函数逐步对齐：先取信封再校验归属，二者
/// 任一不满足即 `None`。
///
/// # Errors
///
/// 底层查询失败时返回 [`DbError`]（旧实现同样让异常上抛 → 500 信封）。
pub(crate) async fn accessible_run(
    state: &AppState,
    run_id: &str,
    asserted_session_id: &str,
) -> Result<Option<RunEnvelopeView>, DbError> {
    let Some(run) = state.db.find_run_by_id(run_id).await? else {
        return Ok(None);
    };
    if can_access_session(state, &run.session_id, asserted_session_id).await? {
        Ok(Some(run))
    } else {
        Ok(None)
    }
}

/// 必填 `X-Session-Id` 头（旧 `@RequestHeader("X-Session-Id")` 缺失时由
/// `GlobalExceptionHandler.handleMissingHeader`（L80-85）回 400 `MISSING_HEADER`）。
///
/// # Errors
///
/// 头缺失或非 ASCII 可见字符时返回 400 `MISSING_HEADER`（旧 Spring 对不可解析
/// 头值同样走缺头分支）。
pub(crate) fn require_session_header(headers: &HeaderMap) -> Result<String, ApiError> {
    headers
        .get("X-Session-Id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| ApiError {
            status: axum::http::StatusCode::BAD_REQUEST,
            code: "MISSING_HEADER".to_owned(),
            message: "Required header 'X-Session-Id' is missing".to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_session_header_maps_400_legacy_code() {
        let err = require_session_header(&HeaderMap::new()).expect_err("missing header");
        assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "MISSING_HEADER");
        assert_eq!(err.message, "Required header 'X-Session-Id' is missing");
    }

    #[test]
    fn present_session_header_returned_verbatim() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Session-Id", "s-1".parse().expect("value"));
        assert_eq!(require_session_header(&headers).expect("header"), "s-1");
    }
}
