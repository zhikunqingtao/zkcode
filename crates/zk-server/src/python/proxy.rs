//! 前端面板 → Python 侧车的反向代理（六组白名单入口）。
//!
//! # 为何需要这层代理
//!
//! tokenizer、复杂度、文件分析/树、分析和 Git 面板原先经 vite dev proxy 把
//! `/api/tokenizer`、`/api/code-quality`、`/api/files/*`、`/api/analysis`、`/api/git` 直发
//! Python 的 TCP `:8000`。zkcode 的侧车按决策 D-P2-2 改走 Unix Domain Socket
//! （`uvicorn --uds`，**无 TCP 监听**），浏览器无法直连 UDS，这四组请求于是
//! 直接 `ECONNREFUSED`。本模块把它们收拢回 zk-server：请求原样转发到侧车、
//! 响应原样回传，前端不必知道传输层已换。
//!
//! # 与工具调用路径的语义分工
//!
//! [`super::client::PythonClient::call_with_retry`] 服务的是 LLM 工具调用（能力域门控 +
//! 指数退避 + 失败折叠为降级文案）；本模块服务的是浏览器同步请求，故：
//!
//! - **不查能力域**：能力缺失时 Python 自身回 404/503，面板据此提示用户，
//!   服务端不代为编造语义；
//! - **不重试**：代理面覆盖 POST 端点（重试语义不安全），且重发与否由前端
//!   决定，服务端重试只会放大面板延迟；
//! - **不改写状态码与响应体**：侧车的 2xx/4xx/5xx 逐字回传。
//!
//! # 失败矩阵（全部**不 panic**）
//!
//! | 场景 | HTTP | `code` |
//! |---|---|---|
//! | `ZK_PYTHON_ENABLED=false` | 503 | `PYTHON_SERVICE_DISABLED` |
//! | 侧车未启动 / socket 不可连 / 协议失败 | 503 | `PYTHON_SERVICE_UNAVAILABLE` |
//! | 侧车 30s 内未回（[`super::client::READ_TIMEOUT`]） | 504 | `PYTHON_SERVICE_TIMEOUT` |
//! | 请求体超 [`MAX_PROXY_BODY_BYTES`] 或读体失败 | 413 | `PROXY_BODY_TOO_LARGE` |
//! | 请求体非 UTF-8 | 400 | `INVALID_REQUEST_BODY` |
//! | 目标不在白名单（防御性复核） | 400 | `INVALID_REQUEST` |

use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use super::client::Correlation;
use super::uds::{TransportError, UdsResponse};
use crate::error::ApiError;
use crate::state::AppState;

/// 请求体上限（8 MiB）。tokenizer 计数与复杂度分析可能带整文件内容，故不能
/// 取小；但代理是内存缓冲的（UDS 客户端非流式），必须有明确上界以免单个
/// 请求把进程内存拖爆。
pub const MAX_PROXY_BODY_BYTES: usize = 8 * 1024 * 1024;

/// 无 `Content-Type` 时的兜底类型（四条前缀的 Python 端点均为 JSON 契约）。
const APPLICATION_JSON: &str = "application/json";

/// 会话关联头（转发时折叠进 `X-Session-Id`，便于两端日志对账）。
static SESSION_ID_HEADER: HeaderName = HeaderName::from_static("x-session-id");

/// 可代理前缀白名单——与 [`crate::routes::build_router`] 注册的通配路由一一
/// 对应。handler 内再复核一遍：将来若有人把通配路由注册宽了，本 handler 也
/// 不会退化成「任意 Python 端点开放代理」。
const FORWARDABLE_PREFIXES: [&str; 5] = [
    "/api/tokenizer/",
    "/api/code-quality/",
    "/api/files/analysis/",
    "/api/analysis/",
    "/api/git/",
];

/// 可代理的精确路径——文件树面板的 `POST /api/files/tree`（Python
/// `routers/file_processing.py` 的 `/tree`，不落在 `analysis` 子前缀下）。
/// 刻意不放开整个 `/api/files/`：`/api/files/search` 是会话感知的后端能力，
/// 不能被代理抢走。
const FORWARDABLE_PATHS: [&str; 1] = ["/api/files/tree"];

/// 通用 Python 侧车反向代理：将请求原样转发到 Python 侧对应路径。
///
/// 方法集由路由层（`get(..).post(..)`）钳制，其余方法由 axum 自动 405。
pub(crate) async fn proxy_to_python(State(state): State<AppState>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    // origin-form 目标（含 query）：Python 侧的 `?path=` 一类参数须原样带过去。
    let Some(target) = parts
        .uri
        .path_and_query()
        .map(|value| value.as_str().to_owned())
    else {
        return ApiError::validation("Proxy target path is missing").into_response();
    };
    if !is_forwardable_target(&target) {
        // §19：只记路径模板与拒绝事实，不落 query 与请求体。
        tracing::warn!(
            path = parts.uri.path(),
            "python proxy target is not allow-listed"
        );
        return ApiError::validation("Proxy target path is not forwardable").into_response();
    }
    if !state.config.python_enabled {
        return unavailable(
            StatusCode::SERVICE_UNAVAILABLE,
            "PYTHON_SERVICE_DISABLED",
            "Python service is disabled",
        )
        .into_response();
    }

    let body = match axum::body::to_bytes(body, MAX_PROXY_BODY_BYTES).await {
        // GET 与空体 POST → 无体转发（`Content-Type` 亦不发）。
        Ok(bytes) if bytes.is_empty() => None,
        Ok(bytes) => match String::from_utf8(bytes.into()) {
            Ok(text) => Some(text),
            Err(_) => return ApiError::invalid_request_body().into_response(),
        },
        // `to_bytes` 不区分「超限」与「读体中断」，统一按超限处理（两者都以
        // 不可继续转发收场，且都不该把细节回给调用方）。
        Err(_) => {
            return unavailable(
                StatusCode::PAYLOAD_TOO_LARGE,
                "PROXY_BODY_TOO_LARGE",
                "Request body is too large to proxy",
            )
            .into_response();
        }
    };

    let session_id = parts
        .headers
        .get(&SESSION_ID_HEADER)
        .and_then(|value| value.to_str().ok());
    let correlation = Correlation::for_session(session_id);
    match state
        .python
        .forward(parts.method.clone(), &target, body, &correlation)
        .await
    {
        Ok(response) => forwarded_response(response),
        Err(error) => {
            tracing::warn!(
                path = parts.uri.path(),
                method = %parts.method,
                error_type = error.error_type(),
                "python proxy forward failed"
            );
            transport_failure(&error).into_response()
        }
    }
}

/// 侧车响应 → 客户端响应（状态码与体逐字回传，`Content-Type` 原样透传）。
fn forwarded_response(response: UdsResponse) -> Response {
    let content_type = response
        .content_type
        .as_deref()
        .and_then(|value| HeaderValue::from_str(value).ok())
        .unwrap_or_else(|| HeaderValue::from_static(APPLICATION_JSON));
    let mut forwarded = (response.status, response.body).into_response();
    // `(StatusCode, String)` 默认落 text/plain，覆盖为侧车给的类型。
    forwarded
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type);
    forwarded
}

/// 传输层失败 → HTTP 信封：读超时是「侧车在但慢」故 504，其余归「侧车不可
/// 用」故 503（前端面板据状态码区分「重试可能有用」与「先起侧车」）。
fn transport_failure(error: &TransportError) -> ApiError {
    match error {
        TransportError::ReadTimeout => unavailable(
            StatusCode::GATEWAY_TIMEOUT,
            "PYTHON_SERVICE_TIMEOUT",
            "Python service did not respond in time",
        ),
        _ => unavailable(
            StatusCode::SERVICE_UNAVAILABLE,
            "PYTHON_SERVICE_UNAVAILABLE",
            "Python service is unavailable",
        ),
    }
}

/// 构造代理侧错误信封（形状复用 [`ApiError`]，与其余 REST 端点一致）。
fn unavailable(status: StatusCode, code: &str, message: &str) -> ApiError {
    ApiError {
        status,
        code: code.to_owned(),
        message: message.to_owned(),
    }
}

/// 目标是否可代理：去掉 query 后须命中白名单，且不含 `..` 段。
///
/// `..` 判定是防御性的——HTTP 目标不经路径规范化即交给侧车，虽然 `FastAPI` 的
/// 路由不映射文件系统，仍不给「跳出前缀」留任何形式的入口。
fn is_forwardable_target(target: &str) -> bool {
    let path = target.split('?').next().unwrap_or(target);
    if path.split('/').any(|segment| segment == "..") {
        return false;
    }
    FORWARDABLE_PATHS.contains(&path)
        || FORWARDABLE_PREFIXES
            .iter()
            .any(|prefix| path.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::super::client::READ_TIMEOUT;
    use super::*;

    /// 五条前缀 + 文件树精确路径放行；query 不影响判定。
    #[test]
    fn allow_listed_targets_pass() {
        for target in [
            "/api/tokenizer/count",
            "/api/code-quality/complexity",
            "/api/code-quality/health",
            "/api/files/analysis/summary",
            "/api/analysis/openapi/merged",
            "/api/files/tree",
            "/api/files/tree?depth=2",
            "/api/git/diff",
            "/api/git/log?limit=20",
        ] {
            assert!(is_forwardable_target(target), "{target} must be proxied");
        }
    }

    /// 前缀外路径、`/api/files` 其余端点与 `..` 跳段一律拒绝。
    #[test]
    fn non_allow_listed_targets_are_rejected() {
        for target in [
            "/api/sessions",
            "/api/health",
            "/api/files/search?query=readme",
            "/api/files/safe-read",
            "/api/tokenizer",
            "/api/git",
            "/api/git/../health",
            "/api/tokenizer/../../etc/passwd",
            "",
            "/",
        ] {
            assert!(
                !is_forwardable_target(target),
                "{target} must not be proxied"
            );
        }
    }

    /// 侧车不可用 → 503；仅读超时 → 504（面板重试提示的分界）。
    #[test]
    fn transport_failures_map_to_503_except_timeout() {
        for error in [
            TransportError::Connect,
            TransportError::ConnectTimeout,
            TransportError::Http,
            TransportError::Request,
        ] {
            let mapped = transport_failure(&error);
            assert_eq!(mapped.status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(mapped.code, "PYTHON_SERVICE_UNAVAILABLE");
        }
        let timeout = transport_failure(&TransportError::ReadTimeout);
        assert_eq!(timeout.status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(timeout.code, "PYTHON_SERVICE_TIMEOUT");
    }

    /// 读超时门限即客户端常规读超时（30s），代理不另立一档。
    #[test]
    fn proxy_uses_regular_read_timeout() {
        assert_eq!(READ_TIMEOUT.as_secs(), 30);
    }

    /// 响应 `Content-Type` 缺失时兜底 JSON；给出时原样透传。
    #[test]
    fn content_type_passes_through_with_json_fallback() {
        let fallback = forwarded_response(UdsResponse {
            status: StatusCode::OK,
            content_type: None,
            body: "{}".to_owned(),
        });
        assert_eq!(
            fallback.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("application/json"))
        );
        let passed = forwarded_response(UdsResponse {
            status: StatusCode::NOT_FOUND,
            content_type: Some("text/plain; charset=utf-8".to_owned()),
            body: "missing".to_owned(),
        });
        assert_eq!(passed.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            passed
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("text/plain; charset=utf-8")
        );
    }
}
