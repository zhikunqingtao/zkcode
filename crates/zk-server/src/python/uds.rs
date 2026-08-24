//! Python 侧车 IPC 传输层——HTTP/1.1 over Unix Domain Socket（决策 D-P2-2）。
//!
//! 旧端（`PythonCapabilityAwareClient.java` L73）以
//! `http://${python.service.host}:${python.service.port}` 走 TCP 环回；zkcode
//! 改走 UDS：`uvicorn --uds ~/.zkcode/python.sock`。**HTTP 契约完全不变**
//! （同样的 method / path / `Content-Type` / JSON 体 / 状态码语义），仅换传输
//! 层——故 `python-service` 本体零改动。
//!
//! 实现说明：`reqwest` 0.12 不开放自定义 connector，无法走 UDS，故直接用
//! `hyper` 1 的 http1 客户端握手 + `hyper_util::rt::TokioIo` 适配
//! `tokio::net::UnixStream`。每次调用一条短连接（UDS 连接建立在微秒级，
//! 无 TCP 三次握手/TLS 开销；旧端 `HttpClient` 的连接池语义在 UDS 下无收益）。
//!
//! 超时语义与旧端逐条对齐：连接超时独立计时（旧
//! `HttpClient.connectTimeout(CONNECT_TIMEOUT)`），请求-响应整体超时独立计时
//! （旧 `HttpRequest.timeout(READ_TIMEOUT)`）。

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::{CONTENT_TYPE, HOST, HeaderName, HeaderValue};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;

/// 请求体 `Content-Type`（旧 `header("Content-Type", "application/json")`）。
const APPLICATION_JSON: &str = "application/json";

/// UDS 上无真实 authority，`Host` 取固定占位值（HTTP/1.1 要求该头存在）。
const HOST_PLACEHOLDER: &str = "localhost";

/// 传输层失败分类。
///
/// 变体粒度只服务于「错误**类型**可记日志、错误**内容**不可记」——对照旧
/// `SafeLogValue.errorType(e)`（只落异常类名，不落 message/body，§19 脱敏）。
#[derive(Debug)]
pub(crate) enum TransportError {
    /// 连接阶段超时（socket 存在但 accept 队列不响应）。
    ConnectTimeout,
    /// 连接失败（socket 文件不存在 / 权限不足 / 侧车未启动）。
    Connect,
    /// 请求-响应整体超时（对照旧 `HttpTimeoutException`）。
    ReadTimeout,
    /// HTTP 协议层失败（握手 / 发送 / 收体）。
    Http,
    /// 请求构造失败（不合法 path / 头值）——本地编程错误面。
    Request,
}

impl TransportError {
    /// 可安全落日志的错误类型名（对照旧 `SafeLogValue.errorType`）。
    pub(crate) fn error_type(&self) -> &'static str {
        match self {
            Self::ConnectTimeout => "ConnectTimeout",
            Self::Connect => "ConnectFailed",
            Self::ReadTimeout => "ReadTimeout",
            Self::Http => "HttpExchangeFailed",
            Self::Request => "RequestBuildFailed",
        }
    }
}

/// 一次 UDS HTTP 往返的响应（状态码 + `Content-Type` + 文本体）。
#[derive(Debug)]
pub(crate) struct UdsResponse {
    /// HTTP 状态码。
    pub(crate) status: StatusCode,
    /// 侧车回的 `Content-Type`（`None` = 未给该头）。仅反向代理
    /// （[`super::proxy`]）需要原样回传，工具调用路径不读此字段。
    pub(crate) content_type: Option<String>,
    /// 响应体（UTF-8 有损解码——JSON 契约恒 UTF-8）。
    pub(crate) body: String,
}

impl UdsResponse {
    /// 是否 2xx（对照旧 `statusCode() >= 200 && < 300`）。
    pub(crate) fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// 是否 4xx 永久性客户端错误（旧端据此**不重试**并立即返回 empty）。
    pub(crate) fn is_client_error(&self) -> bool {
        self.status.is_client_error()
    }
}

/// 一次 UDS HTTP 请求的全部输入（打包成结构体以免参数表过长）。
pub(crate) struct UdsRequest<'a> {
    /// 侧车监听的 socket 文件路径。
    pub(crate) socket: &'a Path,
    /// HTTP 方法。
    pub(crate) method: Method,
    /// origin-form 请求目标（如 `/api/health/capabilities`）。
    pub(crate) path: &'a str,
    /// 关联诊断头（`X-Request-Id` / `X-Attempt` / `X-Run-Id` / `X-Session-Id`）。
    pub(crate) headers: Vec<(HeaderName, String)>,
    /// JSON 请求体（`None` 即 GET 无体）。
    pub(crate) body: Option<String>,
    /// 连接超时（旧 `CONNECT_TIMEOUT = 3s`）。
    pub(crate) connect_timeout: Duration,
    /// 请求-响应整体超时（旧 `READ_TIMEOUT` / `HEAVY_READ_TIMEOUT`）。
    pub(crate) read_timeout: Duration,
}

/// 发一次 UDS HTTP 请求并读全响应体。
pub(crate) async fn send(request: UdsRequest<'_>) -> Result<UdsResponse, TransportError> {
    let stream =
        match tokio::time::timeout(request.connect_timeout, UnixStream::connect(request.socket))
            .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(_)) => return Err(TransportError::Connect),
            Err(_) => return Err(TransportError::ConnectTimeout),
        };
    match tokio::time::timeout(request.read_timeout, exchange(stream, &request)).await {
        Ok(result) => result,
        Err(_) => Err(TransportError::ReadTimeout),
    }
}

/// http1 握手 → 发请求 → 聚合响应体（超时由 [`send`] 在外层统一钳制）。
async fn exchange(
    stream: UnixStream,
    request: &UdsRequest<'_>,
) -> Result<UdsResponse, TransportError> {
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|_| TransportError::Http)?;
    // 连接驱动任务：随 sender drop 自然收尾（短连接，无池化）。
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });

    let mut builder = Request::builder()
        .method(request.method.clone())
        .uri(request.path)
        .header(HOST, HOST_PLACEHOLDER);
    for (name, value) in &request.headers {
        if let Ok(header) = HeaderValue::from_str(value) {
            builder = builder.header(name, header);
        }
    }
    let body = match &request.body {
        Some(json) => {
            builder = builder.header(CONTENT_TYPE, APPLICATION_JSON);
            Full::new(Bytes::from(json.clone()))
        }
        None => Full::new(Bytes::new()),
    };
    let Ok(http_request) = builder.body(body) else {
        driver.abort();
        return Err(TransportError::Request);
    };

    let result = async {
        let response = sender
            .send_request(http_request)
            .await
            .map_err(|_| TransportError::Http)?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|_| TransportError::Http)?
            .to_bytes();
        Ok(UdsResponse {
            status,
            content_type,
            body: String::from_utf8_lossy(&bytes).into_owned(),
        })
    }
    .await;
    drop(sender);
    driver.abort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 错误类型名恒为可落日志的静态短名（不含异常内容，§19 脱敏）。
    #[test]
    fn error_types_are_static_short_names() {
        for error in [
            TransportError::ConnectTimeout,
            TransportError::Connect,
            TransportError::ReadTimeout,
            TransportError::Http,
            TransportError::Request,
        ] {
            let name = error.error_type();
            assert!(!name.is_empty());
            assert!(name.chars().all(char::is_alphanumeric));
        }
    }

    /// socket 文件不存在 → `Connect`（不是超时；侧车未启动的常态路径）。
    #[tokio::test]
    async fn missing_socket_reports_connect_failure() {
        let error = send(UdsRequest {
            socket: Path::new("/tmp/zk-python-absent-1a2b3c.sock"),
            method: Method::GET,
            path: "/api/health",
            headers: Vec::new(),
            body: None,
            connect_timeout: Duration::from_millis(200),
            read_timeout: Duration::from_millis(200),
        })
        .await
        .expect_err("absent socket cannot connect");
        assert_eq!(error.error_type(), "ConnectFailed");
    }
}
