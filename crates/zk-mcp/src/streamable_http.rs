//! MCP Streamable HTTP 传输 — MCP 2025-03-26 规范的单端点双向通道。
//!
//! 逐字对照 Java 基线 `mcp/McpStreamableHttpTransport.java`（332 行）：
//! - 构造：剥除 `baseUrl` 尾部 `/`；HTTP 客户端 connect 10s / read 不限时
//!   （Java v1.1 `readTimeout(Duration.ZERO)`）/ write 10s；
//! - `connect()`：POST `initialize`（`protocolVersion = 2025-03-26`、
//!   `clientInfo = {zkcode-mcp-client, 1.0.0}`）→ 置连接态 → 发
//!   `notifications/initialized`；失败则置断连并返回错误；
//! - `sendRequest`：POST JSON-RPC，`Accept: application/json, text/event-stream`；
//!   响应按 `Content-Type` 二分——纯 JSON 直接解析，`text/event-stream` 则逐
//!   `data: ` 行扫描出与请求 `id` 匹配的响应（其间遇到的通知投递给处理器），
//!   扫完未命中 → `INTERNAL_ERROR "No matching response in SSE stream"`；
//! - 会话：服务端任一响应头带 `Mcp-Session-Id` 即刷新会话 ID，后续所有请求
//!   携带该头；
//! - `close()`：`compareAndSet(true, false)` 成功时向服务端发 `DELETE`
//!   （带会话头）终止会话，异常仅 debug。
//!
//! 形态差异（Rust 侧必然）：
//! - Java 用 okhttp 同步 `execute()`；此处 `reqwest` 异步，超时由
//!   `tokio::time::timeout` 施加在整个「POST + 读完响应体」上（Java v1.1 起
//!   由 per-call `call.timeout()` 施加同粒度超时）；
//! - Java 的 `pendingRequests` 从未被写入（响应同步取自 POST 响应体），仅在
//!   `close()` 里清空；此处不保留该永空集合。
//!
//! 行为偏离（均在 `docs/compatibility.md` 留痕）：
//! 1. **不重复 `initialize`**：Java 传输层 `connect()` 发一次，连接层
//!    `performProtocolHandshake()` 又无条件发一次——对同一会话二次
//!    `initialize` 违反 MCP 规范。此处以
//!    [`McpTransport::performs_own_handshake`] 让连接层跳过重复握手。
//! 2. **握手带真实客户端能力**：Java 的 `setClientCapabilities` 全仓无调用方，
//!    故实际发送空 `capabilities {}`，服务端会误判客户端不支持
//!    roots/prompts/resources；此处统一用 [`client_capabilities`]（与 STDIO /
//!    SSE 两条链路一致）。
//! 3. **透传配置 headers**：Java `createTransport` 的 HTTP 分支丢弃
//!    `config.headers`（仅 SSE 分支应用），导致需要 `Authorization` 的远端
//!    无法接入；此处对所有请求应用。
//! 4. **新增 GET 通知流**（Java 类注释声称「通知接收: GET → SSE 流」但无实现）：
//!    握手成功后后台起一条 `GET` SSE 流承接服务端主动通知；服务端不支持
//!    （常见 `405`）时仅 debug 记录，不影响连接状态、不触发重连。
//! 5. **`timeoutMs == 0` 时以 [`DEFAULT_REQUEST_TIMEOUT`] 兜底**（Java v1.1 起
//!    per-call `call.timeout()` 已让正超时真正生效，但 0 值时 okhttp 不限时；
//!    Rust 侧经 [`timeout_or_default`] 统一取 30s 兜底）。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::McpServerConfig;
use crate::error::{self, JsonRpcError, McpProtocolError};
use crate::jsonrpc::{JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, RequestId};
use crate::protocol::{
    HTTP_CLIENT_NAME, METHOD_INITIALIZE, METHOD_INITIALIZED, PROTOCOL_VERSION_STREAMABLE_HTTP,
    client_capabilities, client_info,
};
use crate::sse::{SseDecoder, lock, read_lock, value_key, write_lock};
use crate::transport::{
    DEFAULT_REQUEST_TIMEOUT, McpTransport, NotificationHandler, timeout_or_default,
};

/// HTTP 连接建立超时（对照 Java `connectTimeout(Duration.ofSeconds(10))`）。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// MCP 会话 ID 头（对照 Java `"Mcp-Session-Id"`）。
const SESSION_HEADER: &str = "Mcp-Session-Id";

/// MCP Streamable HTTP 传输实现。
pub struct StreamableHttpTransport {
    base_url: String,
    client: reqwest::Client,
    headers: BTreeMap<String, String>,
    request_id: AtomicI64,
    connected: AtomicBool,
    session_id: RwLock<Option<String>>,
    notification_handler: RwLock<Option<NotificationHandler>>,
    listener: Mutex<Option<JoinHandle<()>>>,
    cancel: CancellationToken,
}

impl StreamableHttpTransport {
    /// 用默认 HTTP 客户端（connect 10s，read 不限时——对照 Java v1.1
    /// `readTimeout(Duration.ZERO)`；per-call 超时由请求路径统一施加）构造。
    ///
    /// # Errors
    /// HTTP 客户端构建失败时返回。
    pub fn new(
        base_url: &str,
        headers: BTreeMap<String, String>,
    ) -> Result<Self, McpProtocolError> {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            // Capability destinations are validated before connect.  Following
            // redirects would bypass that boundary and could forward headers.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| {
                McpProtocolError::wrapped(format!("Failed to build HTTP client: {error}"))
            })?;
        Ok(Self::with_client(base_url, headers, client))
    }

    /// 用调用方提供的 HTTP 客户端构造（测试与自定义超时用）。
    #[must_use]
    pub fn with_client(
        base_url: &str,
        headers: BTreeMap<String, String>,
        client: reqwest::Client,
    ) -> Self {
        Self {
            base_url: base_url.strip_suffix('/').unwrap_or(base_url).to_owned(),
            client,
            headers,
            request_id: AtomicI64::new(1),
            connected: AtomicBool::new(false),
            session_id: RwLock::new(None),
            notification_handler: RwLock::new(None),
            listener: Mutex::new(None),
            cancel: CancellationToken::new(),
        }
    }

    /// 按服务器配置构造（对照 Java `createTransport` 的 `HTTP` 分支，额外透传
    /// `config.headers`——见模块级偏离 3）。
    ///
    /// # Errors
    /// 配置缺 `url` 或 HTTP 客户端构建失败时返回。
    pub fn from_config(config: &McpServerConfig) -> Result<Self, McpProtocolError> {
        let url = config.url.as_deref().ok_or_else(|| {
            McpProtocolError::wrapped(format!(
                "MCP server '{}' has HTTP transport but no url",
                config.name
            ))
        })?;
        Self::new(url, config.headers.clone())
    }

    /// 当前会话 ID（对照 Java `getSessionId()`）。
    #[must_use]
    pub fn session_id(&self) -> Option<String> {
        read_lock(&self.session_id).clone()
    }

    fn notification_handler(&self) -> Option<NotificationHandler> {
        read_lock(&self.notification_handler).clone()
    }

    /// 附加自定义 headers 与会话头。
    fn decorate(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let builder = self.headers.iter().fold(builder, |builder, (name, value)| {
            builder.header(name, value)
        });
        match self.session_id() {
            Some(session) => builder.header(SESSION_HEADER, session),
            None => builder,
        }
    }

    fn post(&self, payload: &impl Serialize) -> Result<reqwest::RequestBuilder, McpProtocolError> {
        let json = serde_json::to_string(payload).map_err(|error| {
            McpProtocolError::wrapped(format!("Failed to serialize JSON-RPC payload: {error}"))
        })?;
        Ok(self
            .decorate(self.client.post(&self.base_url))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(
                reqwest::header::ACCEPT,
                "application/json, text/event-stream",
            )
            .body(json))
    }

    /// POST JSON-RPC 请求并解析响应（对照 Java `sendRequestInternal`）。
    async fn send_request_internal(
        &self,
        method: &str,
        params: Option<Value>,
        timeout: Duration,
    ) -> Result<Option<Value>, McpProtocolError> {
        let id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let request = JsonRpcRequest::new(RequestId::Number(id), method, params);
        let builder = self.post(&request)?;
        match tokio::time::timeout(timeout, self.exchange(builder, &id.to_string())).await {
            Ok(outcome) => outcome,
            Err(_) => Err(McpProtocolError::timeout(format!(
                "Request timeout: {method}"
            ))),
        }
    }

    /// 执行一次 POST 往返：发送 → 校验状态 → 刷新会话 ID → 按 `Content-Type`
    /// 分流解析。
    async fn exchange(
        &self,
        builder: reqwest::RequestBuilder,
        expected_id: &str,
    ) -> Result<Option<Value>, McpProtocolError> {
        let response = builder.send().await.map_err(|error| {
            McpProtocolError::wrapped(format!("HTTP communication error: {error}"))
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(McpProtocolError::internal(format!(
                "HTTP {}: {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or("Unknown")
            )));
        }
        if let Some(session) = header_value(response.headers(), SESSION_HEADER) {
            *write_lock(&self.session_id) = Some(session);
        }
        let content_type = header_value(response.headers(), reqwest::header::CONTENT_TYPE.as_str())
            .unwrap_or_default();
        let body = response.text().await.map_err(|error| {
            McpProtocolError::wrapped(format!("HTTP communication error: {error}"))
        })?;
        if content_type.contains("text/event-stream") {
            self.parse_sse_response(&body, expected_id)
        } else {
            parse_json_response(&body)
        }
    }

    /// 解析 SSE 形态的响应体（对照 Java `parseSseResponse`：逐 `data: ` 行扫描，
    /// 命中 `id` 即返回；途中遇到通知则投递处理器）。
    fn parse_sse_response(
        &self,
        body: &str,
        expected_id: &str,
    ) -> Result<Option<Value>, McpProtocolError> {
        let handler = self.notification_handler();
        for line in body.lines() {
            let Some(data) = line.strip_prefix("data: ") else {
                continue;
            };
            let data = data.trim();
            if data.is_empty() {
                continue;
            }
            let node: Value = serde_json::from_str(data).map_err(|error| {
                McpProtocolError::wrapped(format!("Failed to parse SSE response: {error}"))
            })?;
            let matches_id = node
                .get("id")
                .is_some_and(|id| value_key(id) == expected_id);
            if matches_id {
                return extract_result(&node);
            }
            if let (true, Some(handler)) = (node.get("method").is_some(), handler.as_ref()) {
                handler(node);
            }
        }
        Err(McpProtocolError::internal(
            "No matching response in SSE stream",
        ))
    }

    /// 启动 GET 通知流（模块级偏离 4）。服务端不支持时静默退出。
    fn start_notification_stream(&self) {
        let builder = self
            .decorate(self.client.get(&self.base_url))
            .header(reqwest::header::ACCEPT, "text/event-stream");
        let handle = tokio::spawn(notification_stream(
            builder,
            self.notification_handler(),
            self.cancel.clone(),
            self.base_url.clone(),
        ));
        if let Some(previous) = lock(&self.listener).replace(handle) {
            previous.abort();
        }
    }
}

/// 取响应头（HTTP 头名大小写不敏感，`reqwest` 已归一）。
fn header_value(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

/// 解析纯 JSON 响应（对照 Java `parseJsonResponse`）。
fn parse_json_response(body: &str) -> Result<Option<Value>, McpProtocolError> {
    let node: Value = serde_json::from_str(body).map_err(|error| {
        McpProtocolError::wrapped(format!("Failed to parse JSON response: {error}"))
    })?;
    extract_result(&node)
}

/// `error` 优先于 `result`（对照 Java 两处相同的取值顺序）。
fn extract_result(node: &Value) -> Result<Option<Value>, McpProtocolError> {
    if let Some(raw) = node.get("error").filter(|value| !value.is_null()) {
        let rpc = serde_json::from_value::<JsonRpcError>(raw.clone()).unwrap_or_else(|_| {
            JsonRpcError::new(
                error::INTERNAL_ERROR,
                format!("Malformed JSON-RPC error object: {raw}"),
            )
        });
        return Err(McpProtocolError::from_rpc(rpc));
    }
    Ok(node.get("result").filter(|value| !value.is_null()).cloned())
}

/// GET 通知流任务 — 仅承接服务端通知；响应形态报文（带 `id`）无处可投，记
/// `trace`。
async fn notification_stream(
    builder: reqwest::RequestBuilder,
    handler: Option<NotificationHandler>,
    cancel: CancellationToken,
    base_url: String,
) {
    let response = tokio::select! {
        () = cancel.cancelled() => return,
        result = builder.send() => result,
    };
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            tracing::debug!(url = %base_url, %error, "MCP HTTP notification stream unavailable");
            return;
        }
    };
    if !response.status().is_success() {
        tracing::debug!(
            url = %base_url,
            status = response.status().as_u16(),
            "MCP HTTP server does not expose a GET notification stream"
        );
        return;
    }
    tracing::debug!(url = %base_url, "MCP HTTP notification stream open");

    let mut decoder = SseDecoder::default();
    let mut stream = response.bytes_stream();
    loop {
        let chunk = tokio::select! {
            () = cancel.cancelled() => return,
            item = stream.next() => item,
        };
        match chunk {
            Some(Ok(bytes)) => {
                for event in decoder.push(bytes.as_ref()) {
                    let node: Value = match serde_json::from_str(&event.data) {
                        Ok(node) => node,
                        Err(error) => {
                            tracing::warn!(%error, "Failed to process MCP HTTP stream event");
                            continue;
                        }
                    };
                    if let (Some(_), Some(handler)) = (node.get("method"), handler.as_ref()) {
                        handler(node);
                    } else {
                        tracing::trace!("Dropped MCP HTTP stream payload without method");
                    }
                }
            }
            Some(Err(error)) => {
                tracing::debug!(url = %base_url, %error, "MCP HTTP notification stream failed");
                return;
            }
            None => {
                tracing::debug!(url = %base_url, "MCP HTTP notification stream closed");
                return;
            }
        }
    }
}

impl McpTransport for StreamableHttpTransport {
    fn connect(&self) -> BoxFuture<'_, Result<(), McpProtocolError>> {
        Box::pin(async move {
            let params = json!({
                "protocolVersion": PROTOCOL_VERSION_STREAMABLE_HTTP,
                "capabilities": client_capabilities(),
                "clientInfo": client_info(HTTP_CLIENT_NAME),
            });
            match self
                .send_request_internal(METHOD_INITIALIZE, Some(params), DEFAULT_REQUEST_TIMEOUT)
                .await
            {
                Ok(_) => {}
                Err(failure) => {
                    self.connected.store(false, Ordering::Release);
                    return Err(McpProtocolError::wrapped(format!(
                        "Failed to connect via Streamable HTTP: {}",
                        failure.message()
                    )));
                }
            }
            self.connected.store(true, Ordering::Release);
            tracing::info!(
                url = %self.base_url,
                session = ?self.session_id(),
                "MCP Streamable HTTP connected"
            );
            self.send_notification(METHOD_INITIALIZED, None).await;
            self.start_notification_stream();
            Ok(())
        })
    }

    fn send_request<'a>(
        &'a self,
        method: &'a str,
        params: Option<Value>,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Option<Value>, McpProtocolError>> {
        Box::pin(async move {
            if !self.connected.load(Ordering::Acquire) && method != METHOD_INITIALIZE {
                return Err(McpProtocolError::not_initialized(
                    "HTTP transport not connected",
                ));
            }
            self.send_request_internal(method, params, timeout_or_default(timeout))
                .await
        })
    }

    fn send_notification<'a>(
        &'a self,
        method: &'a str,
        params: Option<Value>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let notification = JsonRpcNotification::new(method, params);
            let builder = match self.post(&notification) {
                Ok(builder) => builder,
                Err(error) => {
                    tracing::warn!(method, %error, "Failed to send MCP HTTP notification");
                    return;
                }
            };
            match builder.send().await {
                Ok(response) if !response.status().is_success() => {
                    tracing::warn!(
                        method,
                        status = response.status().as_u16(),
                        "MCP HTTP notification failed"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(method, %error, "Failed to send MCP HTTP notification");
                }
            }
        })
    }

    fn send_response(&self, id: RequestId, result: Value) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let response = JsonRpcResponse::success(id.clone(), result);
            let builder = match self.post(&response) {
                Ok(builder) => builder,
                Err(error) => {
                    tracing::warn!(request_id = %id, %error, "Failed to send MCP HTTP response");
                    return;
                }
            };
            match builder.send().await {
                Ok(response) if !response.status().is_success() => {
                    tracing::warn!(
                        request_id = %id,
                        status = response.status().as_u16(),
                        "MCP HTTP response failed"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(request_id = %id, %error, "Failed to send MCP HTTP response");
                }
            }
        })
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn set_notification_handler(&self, handler: NotificationHandler) {
        *write_lock(&self.notification_handler) = Some(handler);
    }

    fn performs_own_handshake(&self) -> bool {
        true
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.cancel.cancel();
            if let Some(handle) = lock(&self.listener).take() {
                handle.abort();
            }
            // 对照 Java `compareAndSet(true, false)`：仅首次关闭发 DELETE。
            if self
                .connected
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let session = self.session_id();
                if let Some(session) = session.as_deref() {
                    let request = self
                        .decorate(self.client.delete(&self.base_url))
                        .header(SESSION_HEADER, session);
                    if let Err(error) = request.send().await {
                        tracing::debug!(%error, "Failed to send MCP session close");
                    }
                }
                tracing::info!(
                    url = %self.base_url,
                    session = ?session,
                    "MCP Streamable HTTP transport closed"
                );
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::mpsc as std_mpsc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;

    /// 收到的一条 HTTP 请求（方法 + 头 + JSON 正文）。
    #[derive(Debug)]
    struct Captured {
        method: String,
        headers: HashMap<String, String>,
        body: Value,
    }

    /// 最小 Streamable HTTP 测试服务端：
    /// - `POST` 带 `id` → 从 `responses` 队列取一条预置原始 HTTP 响应回写；
    ///   队列空则回 `202 Accepted`（通知同此路径）；
    /// - `GET` → SSE 流，把测试推入 `events` 的帧原样写出；
    /// - `DELETE` → `200 OK`。
    struct TestServer {
        base_url: String,
        responses: mpsc::UnboundedSender<String>,
        events: mpsc::UnboundedSender<String>,
        requests: mpsc::UnboundedReceiver<Captured>,
    }

    async fn start_server() -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (responses_tx, mut responses_rx) = mpsc::unbounded_channel::<String>();
        let (events_tx, events_rx) = mpsc::unbounded_channel::<String>();
        let (requests_tx, requests_rx) = mpsc::unbounded_channel::<Captured>();

        tokio::spawn(async move {
            let mut events_rx = Some(events_rx);
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let Some(captured) = read_request(&mut socket).await else {
                    continue;
                };
                if captured.method == "GET" {
                    let Some(mut events) = events_rx.take() else {
                        continue;
                    };
                    tokio::spawn(async move {
                        let header = concat!(
                            "HTTP/1.1 200 OK\r\n",
                            "Content-Type: text/event-stream\r\n\r\n"
                        );
                        if socket.write_all(header.as_bytes()).await.is_err() {
                            return;
                        }
                        while let Some(frame) = events.recv().await {
                            if socket.write_all(frame.as_bytes()).await.is_err() {
                                return;
                            }
                        }
                    });
                    continue;
                }

                let canned = if captured.body.get("id").is_some() {
                    responses_rx.try_recv().ok()
                } else {
                    None
                };
                let _ = requests_tx.send(captured);
                let response = canned.unwrap_or_else(|| {
                    "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_owned()
                });
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.flush().await;
            }
        });

        TestServer {
            base_url: format!("http://{addr}"),
            responses: responses_tx,
            events: events_tx,
            requests: requests_rx,
        }
    }

    async fn read_request(socket: &mut TcpStream) -> Option<Captured> {
        let mut head = Vec::new();
        let mut byte = [0_u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if socket.read(&mut byte).await.ok()? == 0 {
                return None;
            }
            head.push(byte[0]);
        }
        let head = String::from_utf8_lossy(&head).into_owned();
        let mut lines = head.lines();
        let request_line = lines.next().unwrap_or_default().to_owned();
        let method = request_line
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_owned();
        let mut headers = HashMap::new();
        for line in lines {
            if let Some((name, value)) = line.split_once(':') {
                headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
            }
        }
        let length = headers
            .get("content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = vec![0_u8; length];
        if length > 0 {
            socket.read_exact(&mut body).await.ok()?;
        }
        let body = serde_json::from_slice(&body).unwrap_or(Value::Null);
        Some(Captured {
            method,
            headers,
            body,
        })
    }

    fn json_response(session: Option<&str>, body: &str) -> String {
        let session = session.map_or_else(String::new, |id| format!("Mcp-Session-Id: {id}\r\n"));
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n{session}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn sse_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn transport(base_url: &str) -> StreamableHttpTransport {
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("loopback test client");
        StreamableHttpTransport::with_client(base_url, BTreeMap::new(), client)
    }

    #[test]
    fn strips_trailing_slash_from_base_url() {
        let transport = transport("http://127.0.0.1:9000/mcp/");
        assert_eq!(transport.base_url, "http://127.0.0.1:9000/mcp");
        assert!(transport.performs_own_handshake());
    }

    #[test]
    fn json_error_payload_becomes_protocol_error() {
        let error = parse_json_response(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found: x"}}"#,
        )
        .expect_err("must fail");
        assert_eq!(error.code(), error::METHOD_NOT_FOUND);
        assert_eq!(error.message(), "Method not found: x");
    }

    #[test]
    fn unparsable_body_reports_parse_failure() {
        let error = parse_json_response("not json").expect_err("must fail");
        assert!(
            error
                .to_string()
                .starts_with("Failed to parse JSON response:"),
            "unexpected message: {error}"
        );
    }

    #[tokio::test]
    async fn request_before_connect_reports_not_initialized() {
        let transport = transport("http://127.0.0.1:1");
        let error = transport
            .send_request("tools/list", None, Duration::from_millis(50))
            .await
            .expect_err("must fail");
        assert_eq!(error.code(), error::SERVER_NOT_INITIALIZED);
        assert_eq!(error.message(), "HTTP transport not connected");
    }

    #[tokio::test]
    async fn connect_captures_session_and_sends_initialized() {
        let mut server = start_server().await;
        server
            .responses
            .send(json_response(
                Some("sess-1"),
                r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26"}}"#,
            ))
            .expect("queue");
        let transport = transport(&server.base_url);
        transport.connect().await.expect("connect");
        assert!(transport.is_connected());
        assert_eq!(transport.session_id().as_deref(), Some("sess-1"));

        let initialize = server.requests.recv().await.expect("initialize");
        assert_eq!(initialize.method, "POST");
        assert_eq!(initialize.body["method"], json!("initialize"));
        assert_eq!(
            initialize.body["params"]["protocolVersion"],
            json!("2025-03-26")
        );
        assert_eq!(
            initialize.body["params"]["clientInfo"]["name"],
            json!("zkcode-mcp-client")
        );
        // 偏离 2：能力集非空。
        assert_eq!(
            initialize.body["params"]["capabilities"]["roots"]["listChanged"],
            json!(true)
        );
        assert!(!initialize.headers.contains_key("mcp-session-id"));

        let initialized = server.requests.recv().await.expect("initialized");
        assert_eq!(
            initialized.body["method"],
            json!("notifications/initialized")
        );
        // 会话 ID 已在首个响应里捕获 → 后续请求必须携带。
        assert_eq!(
            initialized
                .headers
                .get("mcp-session-id")
                .map(String::as_str),
            Some("sess-1")
        );

        transport.close().await;
        let deleted = server.requests.recv().await.expect("delete");
        assert_eq!(deleted.method, "DELETE");
        assert_eq!(
            deleted.headers.get("mcp-session-id").map(String::as_str),
            Some("sess-1")
        );
        assert!(!transport.is_connected());
    }

    #[tokio::test]
    async fn sse_shaped_response_is_matched_by_id_and_notifications_dispatched() {
        let server = start_server().await;
        server
            .responses
            .send(json_response(
                None,
                r#"{"jsonrpc":"2.0","id":1,"result":{}}"#,
            ))
            .expect("queue");
        server
            .responses
            .send(sse_response(concat!(
                "event: message\n",
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n",
                "event: message\n",
                "data: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":[]}}\n\n"
            )))
            .expect("queue");

        let transport = transport(&server.base_url);
        let (seen_tx, seen_rx) = std_mpsc::channel::<Value>();
        transport.set_notification_handler(Arc::new(move |node| {
            let _ = seen_tx.send(node);
        }));
        transport.connect().await.expect("connect");

        let result = transport
            .send_request("tools/list", None, Duration::from_secs(5))
            .await
            .expect("result");
        assert_eq!(result, Some(json!({"tools": []})));
        let seen = seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("notification");
        assert_eq!(seen["method"], json!("notifications/progress"));
        transport.close().await;
    }

    #[tokio::test]
    async fn sse_stream_without_matching_id_is_internal_error() {
        let server = start_server().await;
        server
            .responses
            .send(json_response(
                None,
                r#"{"jsonrpc":"2.0","id":1,"result":{}}"#,
            ))
            .expect("queue");
        server
            .responses
            .send(sse_response(
                "data: {\"jsonrpc\":\"2.0\",\"id\":99,\"result\":{}}\n\n",
            ))
            .expect("queue");

        let transport = transport(&server.base_url);
        transport.connect().await.expect("connect");
        let error = transport
            .send_request("tools/list", None, Duration::from_secs(5))
            .await
            .expect_err("must fail");
        assert_eq!(error.code(), error::INTERNAL_ERROR);
        assert_eq!(error.message(), "No matching response in SSE stream");
    }

    #[tokio::test]
    async fn non_success_status_becomes_internal_error() {
        let server = start_server().await;
        server
            .responses
            .send(
                "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_owned(),
            )
            .expect("queue");
        let transport = transport(&server.base_url);
        let error = transport.connect().await.expect_err("must fail");
        assert!(
            error
                .to_string()
                .starts_with("Failed to connect via Streamable HTTP: HTTP 503:"),
            "unexpected message: {error}"
        );
        assert!(!transport.is_connected());
    }

    #[tokio::test]
    async fn get_stream_delivers_server_notifications() {
        let server = start_server().await;
        server
            .responses
            .send(json_response(
                None,
                r#"{"jsonrpc":"2.0","id":1,"result":{}}"#,
            ))
            .expect("queue");
        let transport = transport(&server.base_url);
        let (seen_tx, seen_rx) = std_mpsc::channel::<Value>();
        transport.set_notification_handler(Arc::new(move |node| {
            let _ = seen_tx.send(node);
        }));
        transport.connect().await.expect("connect");

        // GET 流由 connect() 起，等其建立后推送一条通知。
        tokio::time::sleep(Duration::from_millis(150)).await;
        server
            .events
            .send("data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\"}\n\n".to_owned())
            .expect("push");
        let seen = tokio::task::spawn_blocking(move || {
            seen_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("notification")
        })
        .await
        .expect("join");
        assert_eq!(seen["method"], json!("notifications/message"));
        transport.close().await;
    }
}
