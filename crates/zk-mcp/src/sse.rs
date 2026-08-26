//! MCP SSE 传输 — 接收走 Server-Sent Events（GET），发送走 HTTP POST。
//!
//! 逐字对照 Java 基线 `mcp/McpSseTransport.java`（353 行）：
//! - 构造：`sseUrl = baseUrl + "/sse"`、`postUrl = baseUrl`、
//!   `baseOrigin = scheme://host[:port]`（URL 非法 → 构造失败）；
//!   HTTP 客户端 connect 10s / **read 90s（3 倍心跳间隔）** / write 10s；
//! - `connect()`：GET `sseUrl` 带 `Accept: text/event-stream`，收到
//!   `event: endpoint` 帧后视为握手完成（该帧的 `data` 给出后续 POST 的
//!   会话端点）；
//! - `sendRequest`：POST 到 `sessionEndpoint`（未收到 endpoint 帧则退化为
//!   `postUrl`），响应由 SSE 流按 `id` 回填；
//! - `sendNotification` / `sendResponse` 吞异常仅告警；`sendHealthPing` 发
//!   `notifications/ping` 通知并以 HTTP 状态判定健康；
//! - `close()`：置断连、终止 SSE 流、以 `Transport closed` 失败所有在途请求。
//!
//! 形态差异（Rust 侧必然）：
//! - Java 用 okhttp `EventSources` 的 `EventSourceListener` 回调；Rust 无对应
//!   SSE 客户端库，故按 SSE 规范手写 [`SseDecoder`]（字节流 → 行 → 事件帧），
//!   消费 `reqwest::Response::bytes_stream()`；
//! - Java 的 `eventSource.cancel()` 由 [`CancellationToken`] 替代（同时用于
//!   区分「主动 close」与「被动断开」两条路径——前者不触发断连回调）；
//! - Java `close()` 末尾释放 `OkHttpClient` 线程池/连接池；`reqwest::Client`
//!   随 `Drop` 自行回收连接池，无对应动作。
//!
//! 行为偏离（均在 `docs/compatibility.md` 留痕）：
//! 1. **干净 EOF 也触发断连回调**：Java `onClosed` 仅置 `connected=false`，
//!    既不完成 `connectFuture`（依赖外层 30s 超时兜底）也不触发
//!    `disconnectCallback`——服务端正常关流后连接永久僵死且不重连。此处对
//!    EOF 与失败一视同仁：完成 `connect()` 为错误 + 触发断连回调。
//! 2. **断开时立即失败在途请求**（`SSE stream closed`）。Java 让在途请求空等
//!    到各自超时（最长 30s）。与 [`crate::stdio`] 同一裁决。
//! 3. **`error` 对象反序列化失败有兜底**：Java `treeToValue` 抛异常被外层
//!    `catch` 吞掉 → 该请求永不完成、只能超时；此处退化为
//!    `INTERNAL_ERROR` + 原始报文，请求立即失败。

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::McpServerConfig;
use crate::error::{self, JsonRpcError, McpProtocolError};
use crate::jsonrpc::{JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, RequestId};
use crate::protocol::METHOD_PING;
use crate::transport::{DisconnectCallback, McpTransport, NotificationHandler, timeout_or_default};

/// SSE 流读超时（对照 Java `readTimeout(Duration.ofSeconds(90))`——3 倍心跳
/// 间隔，兼顾长连接与死连接检测）。
const READ_TIMEOUT: Duration = Duration::from_secs(90);

/// HTTP 连接建立超时（对照 Java `connectTimeout(Duration.ofSeconds(10))`）。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// SSE 默认事件类型 — 报文未给 `event:` 字段时的类型名（SSE 规范）。
const DEFAULT_EVENT_TYPE: &str = "message";

/// 会话端点事件类型（对照 Java `"endpoint".equals(type)`）。
const ENDPOINT_EVENT_TYPE: &str = "endpoint";

/// 在途请求的一次性应答通道。
pub(crate) type PendingResponse = oneshot::Sender<Result<Option<Value>, McpProtocolError>>;

/// 中毒降级取锁 — 传输层状态是「已断连 / 待回填」这类可重建信息，毒化后继续
/// 使用比 panic 传播更安全（与 [`crate::stdio`] 同一范式）。
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// 中毒降级读锁。
pub(crate) fn read_lock<T>(cell: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    cell.read().unwrap_or_else(PoisonError::into_inner)
}

/// 中毒降级写锁。
pub(crate) fn write_lock<T>(cell: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    cell.write().unwrap_or_else(PoisonError::into_inner)
}

/// SSE 单帧事件（对照 okhttp `EventSourceListener.onEvent(id, type, data)`；
/// `id` / `retry` 字段 Java 侧未使用，此处同样丢弃）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SseEvent {
    /// 事件类型（`event:` 字段；缺省为 `message`）。
    pub(crate) event_type: String,
    /// 事件数据（多行 `data:` 以 `\n` 连接）。
    pub(crate) data: String,
}

/// SSE 字节流解码器 — 按 SSE 规范做「字节 → 行 → 事件帧」两级切分。
///
/// 跨 chunk 的半行 / 半帧由内部缓冲承接（`reqwest` 的 chunk 边界与 SSE 帧
/// 边界无任何对齐保证）。
#[derive(Debug, Default)]
pub(crate) struct SseDecoder {
    buffer: Vec<u8>,
    event_type: Option<String>,
    data: Vec<String>,
}

impl SseDecoder {
    /// 喂入一段字节，返回本次新解出的完整事件帧（按到达顺序）。
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        self.buffer.extend_from_slice(chunk);
        let mut events = Vec::new();
        while let Some(index) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let mut line: Vec<u8> = self.buffer.drain(..=index).collect();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = String::from_utf8_lossy(&line).into_owned();
            if let Some(event) = self.push_line(&line) {
                events.push(event);
            }
        }
        events
    }

    fn push_line(&mut self, line: &str) -> Option<SseEvent> {
        if line.is_empty() {
            return self.dispatch();
        }
        // 以 ':' 开头的行是注释（SSE 心跳常用），直接丢弃。
        if line.starts_with(':') {
            return None;
        }
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line, ""),
        };
        match field {
            "event" => self.event_type = Some(value.to_owned()),
            "data" => self.data.push(value.to_owned()),
            // `id` / `retry` / 未知字段：Java 侧同样不使用。
            _ => {}
        }
        None
    }

    fn dispatch(&mut self) -> Option<SseEvent> {
        let event_type = self.event_type.take();
        if self.data.is_empty() {
            return None;
        }
        let data = std::mem::take(&mut self.data).join("\n");
        Some(SseEvent {
            event_type: event_type.unwrap_or_else(|| DEFAULT_EVENT_TYPE.to_owned()),
            data,
        })
    }
}

/// Parse the session endpoint while preserving the original three relative
/// shapes.  Absolute endpoints are accepted only when they stay on the
/// validated SSE origin; otherwise a server could redirect credential-bearing
/// POSTs to an internal or attacker-controlled host.
pub(crate) fn resolve_session_endpoint(
    base_origin: &str,
    post_url: &str,
    data: &str,
) -> Result<String, McpProtocolError> {
    let endpoint = if data.starts_with("http") {
        data.to_owned()
    } else if data.starts_with('/') {
        format!("{base_origin}{data}")
    } else {
        format!("{post_url}/{data}")
    };
    let base = reqwest::Url::parse(base_origin)
        .map_err(|_| McpProtocolError::wrapped("Invalid MCP SSE base origin"))?;
    let parsed = reqwest::Url::parse(&endpoint)
        .map_err(|_| McpProtocolError::wrapped("Invalid MCP SSE session endpoint"))?;
    let same_origin = parsed.scheme() == base.scheme()
        && parsed.host_str() == base.host_str()
        && parsed.port_or_known_default() == base.port_or_known_default();
    if !same_origin || !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(McpProtocolError::wrapped(
            "MCP SSE session endpoint must remain on the configured origin",
        ));
    }
    Ok(endpoint)
}

/// JSON-RPC `id` → pending 表键（对照 Java `idNode.asText()`：数字与字符串
/// 归一到同一键空间）。
pub(crate) fn value_key(id: &Value) -> String {
    id.as_str()
        .map_or_else(|| id.to_string(), ToOwned::to_owned)
}

/// 把一条入站报文派发给在途请求或通知处理器。
///
/// 对照 Java `handleSseEvent` 的三分支：
/// 1. 有非 null `id` 且命中 pending → 回填响应（`error` 优先于 `result`）；
/// 2. 有非 null `id` 未命中 pending 但带 `method` → 服务端反向请求（如
///    `roots/list`），交通知处理器；
/// 3. 无 `id` 但有 `method` → 服务端通知。
///
/// 其余情形按 Java 落到 `trace`（可能是服务端 keepalive）。
pub(crate) fn dispatch_incoming(
    pending: &Mutex<HashMap<String, PendingResponse>>,
    handler: Option<&NotificationHandler>,
    node: Value,
) {
    let id_key = node.get("id").filter(|id| !id.is_null()).map(value_key);
    let Some(key) = id_key else {
        if let (Some(_), Some(handler)) = (node.get("method"), handler) {
            handler(node);
        } else {
            tracing::trace!("Dropped SSE payload without id and method");
        }
        return;
    };

    if let Some(sender) = lock(pending).remove(&key) {
        let outcome = match node.get("error").filter(|value| !value.is_null()) {
            Some(raw) => Err(McpProtocolError::from_rpc(parse_rpc_error(raw))),
            None => Ok(node.get("result").filter(|value| !value.is_null()).cloned()),
        };
        // 接收端已随超时丢弃时发送失败，属正常竞态。
        let _ = sender.send(outcome);
        return;
    }

    if let (Some(_), Some(handler)) = (node.get("method"), handler) {
        handler(node);
    } else {
        tracing::trace!(
            request_id = %key,
            "Received response for unknown request id (may be server keepalive)"
        );
    }
}

/// 反序列化服务端 `error` 对象；非法结构退化为 `INTERNAL_ERROR` + 原始报文
/// （Java 侧此处抛异常并被吞，导致请求只能超时）。
fn parse_rpc_error(raw: &Value) -> JsonRpcError {
    serde_json::from_value::<JsonRpcError>(raw.clone()).unwrap_or_else(|_| {
        JsonRpcError::new(
            error::INTERNAL_ERROR,
            format!("Malformed JSON-RPC error object: {raw}"),
        )
    })
}

/// 以给定错误失败并清空全部在途请求（对照 Java `close()` 的 pending 清理）。
pub(crate) fn fail_all_pending(pending: &Mutex<HashMap<String, PendingResponse>>, message: &str) {
    let drained: Vec<PendingResponse> = lock(pending).drain().map(|(_, sender)| sender).collect();
    for sender in drained {
        let _ = sender.send(Err(McpProtocolError::internal(message)));
    }
}

/// POST 失败的两种成因 — Java 侧对二者的处置不同（HTTP 状态码 vs 异常消息）。
#[derive(Debug)]
enum PostFailure {
    /// 服务端返回非 2xx。
    Status { code: u16, reason: String },
    /// 网络异常或报文序列化失败。
    Network(String),
}

impl PostFailure {
    /// 对照 Java `sendRequest`：非 2xx → `INTERNAL_ERROR "HTTP {code}: {msg}"`；
    /// 其他异常 → `McpProtocolException("SSE request error: ...", cause)`。
    fn into_request_error(self) -> McpProtocolError {
        match self {
            Self::Status { code, reason } => {
                McpProtocolError::internal(format!("HTTP {code}: {reason}"))
            }
            Self::Network(message) => {
                McpProtocolError::wrapped(format!("SSE request error: {message}"))
            }
        }
    }
}

/// 跨任务共享的连接状态（读流任务与调用方各持一份 `Arc`）。
struct SseShared {
    sse_url: String,
    post_url: String,
    base_origin: String,
    connected: AtomicBool,
    pending: Mutex<HashMap<String, PendingResponse>>,
    session_endpoint: RwLock<Option<String>>,
    notification_handler: RwLock<Option<NotificationHandler>>,
    disconnect_callback: RwLock<Option<DisconnectCallback>>,
}

impl SseShared {
    /// POST 目标（对照 Java `sessionEndpoint != null ? sessionEndpoint : postUrl`）。
    fn target_url(&self) -> String {
        read_lock(&self.session_endpoint)
            .clone()
            .unwrap_or_else(|| self.post_url.clone())
    }

    fn notification_handler(&self) -> Option<NotificationHandler> {
        read_lock(&self.notification_handler).clone()
    }

    fn disconnect_callback(&self) -> Option<DisconnectCallback> {
        read_lock(&self.disconnect_callback).clone()
    }

    /// 断开善后：置断连 → 失败在途请求 → 触发断连回调（供管理器重连）。
    fn mark_disconnected(&self, reason: &str) {
        self.connected.store(false, Ordering::Release);
        fail_all_pending(&self.pending, reason);
        if let Some(callback) = self.disconnect_callback() {
            callback();
        }
    }
}

/// MCP SSE 传输实现。
pub struct SseTransport {
    client: reqwest::Client,
    headers: BTreeMap<String, String>,
    request_id: AtomicI64,
    shared: Arc<SseShared>,
    reader: Mutex<Option<JoinHandle<()>>>,
    cancel: CancellationToken,
}

impl SseTransport {
    /// 用默认 HTTP 客户端（connect 10s / read 90s）构造。
    ///
    /// `base_url` 为**不含** `/sse` 后缀的基址：SSE 流地址为
    /// `{base_url}/sse`，POST 地址在收到 `endpoint` 事件前为 `base_url`。
    ///
    /// # Errors
    /// `base_url` 无法解析出 `scheme://host` 时返回（对照 Java 构造函数抛
    /// `IllegalArgumentException("Invalid base URL: ...")`）。
    pub fn new(
        base_url: &str,
        headers: BTreeMap<String, String>,
    ) -> Result<Self, McpProtocolError> {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            // Do not let a validated public capability endpoint redirect the
            // credential-bearing request to a private or different host.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| {
                McpProtocolError::wrapped(format!("Failed to build SSE HTTP client: {error}"))
            })?;
        Self::with_client(base_url, headers, client)
    }

    /// 用调用方提供的 HTTP 客户端构造（测试与自定义超时用）。
    ///
    /// # Errors
    /// 同 [`SseTransport::new`]。
    pub fn with_client(
        base_url: &str,
        headers: BTreeMap<String, String>,
        client: reqwest::Client,
    ) -> Result<Self, McpProtocolError> {
        let base_origin = base_origin(base_url)?;
        Ok(Self {
            client,
            headers,
            request_id: AtomicI64::new(1),
            shared: Arc::new(SseShared {
                sse_url: format!("{base_url}/sse"),
                post_url: base_url.to_owned(),
                base_origin,
                connected: AtomicBool::new(false),
                pending: Mutex::new(HashMap::new()),
                session_endpoint: RwLock::new(None),
                notification_handler: RwLock::new(None),
                disconnect_callback: RwLock::new(None),
            }),
            reader: Mutex::new(None),
            cancel: CancellationToken::new(),
        })
    }

    /// 按服务器配置构造（对照 Java `McpServerConnection.createSseTransport`：
    /// 配置里的 URL 若已带 `/sse` 后缀需先剥除，自定义 headers 附加到每个请求）。
    ///
    /// # Errors
    /// 配置缺 `url` 或 URL 非法时返回。
    pub fn from_config(config: &McpServerConfig) -> Result<Self, McpProtocolError> {
        let url = config.url.as_deref().ok_or_else(|| {
            McpProtocolError::wrapped(format!(
                "MCP server '{}' has SSE transport but no url",
                config.name
            ))
        })?;
        let base_url = url.strip_suffix("/sse").unwrap_or(url);
        Self::new(base_url, config.headers.clone())
    }

    /// 当前会话端点（未收到 `endpoint` 事件时为 POST 基址）。
    #[must_use]
    pub fn target_url(&self) -> String {
        self.shared.target_url()
    }

    fn apply_headers(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        self.headers.iter().fold(builder, |builder, (name, value)| {
            builder.header(name, value)
        })
    }

    /// POST 一条 JSON-RPC 报文到会话端点。
    async fn post_payload(&self, payload: &impl Serialize) -> Result<(), PostFailure> {
        let json = serde_json::to_string(payload)
            .map_err(|error| PostFailure::Network(error.to_string()))?;
        let request = self
            .apply_headers(self.client.post(self.shared.target_url()))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(json);
        let response = request
            .send()
            .await
            .map_err(|error| PostFailure::Network(error.to_string()))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        Err(PostFailure::Status {
            code: status.as_u16(),
            reason: status.canonical_reason().unwrap_or("Unknown").to_owned(),
        })
    }

    /// 发出请求并等待 SSE 流回填（`finally` 语义的 pending 清理由调用方负责）。
    async fn await_response(
        &self,
        request: &JsonRpcRequest,
        receiver: oneshot::Receiver<Result<Option<Value>, McpProtocolError>>,
        timeout: Duration,
    ) -> Result<Option<Value>, McpProtocolError> {
        if let Err(failure) = self.post_payload(request).await {
            return Err(failure.into_request_error());
        }
        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(outcome)) => outcome,
            // 发送端随传输关闭而丢弃（正常路径由 fail_all_pending 先行回填）。
            Ok(Err(_)) => Err(McpProtocolError::internal("SSE response channel closed")),
            Err(_) => Err(McpProtocolError::timeout(format!(
                "Request timeout: {}",
                request.method
            ))),
        }
    }
}

/// 提取 `scheme://host[:port]`（对照 Java `new URI(baseUrl)` 三段拼接；显式
/// 默认端口被 `reqwest::Url` 归一化省略，与 Java `getPort() > 0` 等价）。
fn base_origin(base_url: &str) -> Result<String, McpProtocolError> {
    let url = reqwest::Url::parse(base_url)
        .map_err(|_| McpProtocolError::wrapped(format!("Invalid base URL: {base_url}")))?;
    let host = url
        .host_str()
        .ok_or_else(|| McpProtocolError::wrapped(format!("Invalid base URL: {base_url}")))?;
    let port = url
        .port()
        .map_or_else(String::new, |port| format!(":{port}"));
    Ok(format!("{}://{host}{port}", url.scheme()))
}

impl McpTransport for SseTransport {
    fn connect(&self) -> BoxFuture<'_, Result<(), McpProtocolError>> {
        Box::pin(async move {
            let (sender, receiver) = oneshot::channel();
            let request = self
                .apply_headers(self.client.get(&self.shared.sse_url))
                .header(reqwest::header::ACCEPT, "text/event-stream");
            let handle = tokio::spawn(read_loop(
                request,
                Arc::clone(&self.shared),
                self.cancel.clone(),
                sender,
            ));
            if let Some(previous) = lock(&self.reader).replace(handle) {
                previous.abort();
            }
            receiver
                .await
                .unwrap_or_else(|_| Err(McpProtocolError::wrapped("SSE connection failed")))
        })
    }

    fn send_request<'a>(
        &'a self,
        method: &'a str,
        params: Option<Value>,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Option<Value>, McpProtocolError>> {
        Box::pin(async move {
            if !self.shared.connected.load(Ordering::Acquire) {
                return Err(McpProtocolError::not_initialized("SSE not connected"));
            }
            let id = self.request_id.fetch_add(1, Ordering::Relaxed);
            let key = id.to_string();
            let (sender, receiver) = oneshot::channel();
            lock(&self.shared.pending).insert(key.clone(), sender);
            let request = JsonRpcRequest::new(RequestId::Number(id), method, params);
            let outcome = self
                .await_response(&request, receiver, timeout_or_default(timeout))
                .await;
            lock(&self.shared.pending).remove(&key);
            outcome
        })
    }

    fn send_notification<'a>(
        &'a self,
        method: &'a str,
        params: Option<Value>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let notification = JsonRpcNotification::new(method, params);
            match self.post_payload(&notification).await {
                Ok(()) => {}
                Err(PostFailure::Status { code, .. }) => {
                    tracing::warn!(method, status = code, "MCP SSE notification failed");
                }
                Err(PostFailure::Network(message)) => {
                    tracing::warn!(method, reason = %message, "Failed to send MCP SSE notification");
                }
            }
        })
    }

    fn send_response(&self, id: RequestId, result: Value) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if !self.shared.connected.load(Ordering::Acquire) {
                tracing::warn!("Cannot send SSE response — transport not connected");
                return;
            }
            let response = JsonRpcResponse::success(id.clone(), result);
            match self.post_payload(&response).await {
                Ok(()) => {}
                Err(PostFailure::Status { code, .. }) => {
                    tracing::warn!(request_id = %id, status = code, "MCP SSE response failed");
                }
                Err(PostFailure::Network(message)) => {
                    tracing::warn!(request_id = %id, reason = %message, "Failed to send MCP SSE response");
                }
            }
        })
    }

    fn send_health_ping(&self) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            if !self.shared.connected.load(Ordering::Acquire) {
                return false;
            }
            let notification = JsonRpcNotification::new(METHOD_PING, None);
            match self.post_payload(&notification).await {
                Ok(()) => true,
                Err(PostFailure::Status { code, .. }) => {
                    tracing::warn!(status = code, "MCP SSE health ping failed");
                    false
                }
                Err(PostFailure::Network(message)) => {
                    tracing::warn!(reason = %message, "MCP SSE health ping exception");
                    false
                }
            }
        })
    }

    fn is_connected(&self) -> bool {
        self.shared.connected.load(Ordering::Acquire)
    }

    fn set_notification_handler(&self, handler: NotificationHandler) {
        *write_lock(&self.shared.notification_handler) = Some(handler);
    }

    fn set_disconnect_callback(&self, callback: DisconnectCallback) {
        *write_lock(&self.shared.disconnect_callback) = Some(callback);
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.shared.connected.store(false, Ordering::Release);
            // 先取消：读流任务据此区分「主动关闭」与「被动断开」，前者不触发
            // 断连回调（否则管理器会为一次正常关闭排重连）。
            self.cancel.cancel();
            if let Some(handle) = lock(&self.reader).take() {
                handle.abort();
            }
            fail_all_pending(&self.shared.pending, "Transport closed");
            tracing::debug!(url = %self.shared.sse_url, "McpSseTransport closed");
        })
    }
}

/// SSE 读流任务 — 建立 GET 流、解码事件帧、派发响应/通知，直至断开或被取消。
async fn read_loop(
    request: reqwest::RequestBuilder,
    shared: Arc<SseShared>,
    cancel: CancellationToken,
    connect_sender: oneshot::Sender<Result<(), McpProtocolError>>,
) {
    let mut connect_sender = Some(connect_sender);

    let response = tokio::select! {
        () = cancel.cancelled() => return,
        result = request.send() => result,
    };
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            let message = format!("SSE connection failed: {error}");
            tracing::debug!(url = %shared.sse_url, reason = %message, "MCP SSE connect error");
            complete_connect(&mut connect_sender, Err(McpProtocolError::wrapped(message)));
            shared.mark_disconnected("SSE connection failed");
            return;
        }
    };
    let status = response.status();
    if !status.is_success() {
        let message = format!(
            "SSE connection failed: HTTP {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("Unknown")
        );
        tracing::debug!(url = %shared.sse_url, reason = %message, "MCP SSE connect rejected");
        complete_connect(&mut connect_sender, Err(McpProtocolError::wrapped(message)));
        shared.mark_disconnected("SSE connection failed");
        return;
    }

    shared.connected.store(true, Ordering::Release);
    tracing::info!(url = %shared.sse_url, "MCP SSE connected");

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
                    handle_sse_event(&shared, &event, &mut connect_sender);
                }
            }
            Some(Err(error)) => {
                let message = format!("SSE stream error: {error}");
                tracing::debug!(url = %shared.sse_url, reason = %message, "MCP SSE stream failed");
                complete_connect(&mut connect_sender, Err(McpProtocolError::wrapped(message)));
                shared.mark_disconnected("SSE stream closed");
                return;
            }
            None => {
                tracing::info!(url = %shared.sse_url, "MCP SSE connection closed");
                complete_connect(
                    &mut connect_sender,
                    Err(McpProtocolError::wrapped(
                        "SSE stream closed before handshake",
                    )),
                );
                shared.mark_disconnected("SSE stream closed");
                return;
            }
        }
    }
}

fn complete_connect(
    sender: &mut Option<oneshot::Sender<Result<(), McpProtocolError>>>,
    outcome: Result<(), McpProtocolError>,
) {
    if let Some(sender) = sender.take() {
        // 调用方已放弃等待时发送失败，属正常竞态。
        let _ = sender.send(outcome);
    }
}

/// 处理单个 SSE 事件帧（对照 Java `handleSseEvent`）。
fn handle_sse_event(
    shared: &Arc<SseShared>,
    event: &SseEvent,
    connect_sender: &mut Option<oneshot::Sender<Result<(), McpProtocolError>>>,
) {
    if event.event_type == ENDPOINT_EVENT_TYPE {
        let endpoint =
            match resolve_session_endpoint(&shared.base_origin, &shared.post_url, &event.data) {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    complete_connect(connect_sender, Err(error));
                    shared.mark_disconnected("Unsafe SSE session endpoint rejected");
                    return;
                }
            };
        tracing::info!(endpoint = %endpoint, "MCP SSE endpoint");
        *write_lock(&shared.session_endpoint) = Some(endpoint);
        complete_connect(connect_sender, Ok(()));
        return;
    }
    let node: Value = match serde_json::from_str(&event.data) {
        Ok(node) => node,
        Err(error) => {
            tracing::warn!(%error, "Failed to process SSE event");
            return;
        }
    };
    dispatch_incoming(
        &shared.pending,
        shared.notification_handler().as_ref(),
        node,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::mpsc as std_mpsc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    /// 最小 SSE 测试服务端：
    /// - `GET /sse` → 200 `text/event-stream` + `event: endpoint` 帧，随后把
    ///   测试通过 `events` 推入的原始帧原样写入流；
    /// - 其余（POST）→ 把 `(路径, 报文)` 投递给测试并回 `202 Accepted`。
    struct TestServer {
        base_url: String,
        events: mpsc::UnboundedSender<String>,
        requests: mpsc::UnboundedReceiver<(String, Value)>,
    }

    async fn start_server(endpoint_data: &'static str) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (events_tx, events_rx) = mpsc::unbounded_channel::<String>();
        let (requests_tx, requests_rx) = mpsc::unbounded_channel::<(String, Value)>();

        tokio::spawn(async move {
            let mut events_rx = Some(events_rx);
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let Some((head, body)) = read_http_message(&mut socket).await else {
                    continue;
                };
                let mut parts = head.split_whitespace();
                let method = parts.next().unwrap_or_default().to_owned();
                let path = parts.next().unwrap_or_default().to_owned();

                if method == "GET" {
                    let Some(mut events) = events_rx.take() else {
                        continue;
                    };
                    tokio::spawn(async move {
                        let header = concat!(
                            "HTTP/1.1 200 OK\r\n",
                            "Content-Type: text/event-stream\r\n",
                            "Cache-Control: no-cache\r\n",
                            "Connection: keep-alive\r\n\r\n"
                        );
                        if socket.write_all(header.as_bytes()).await.is_err() {
                            return;
                        }
                        let endpoint_frame = format!("event: endpoint\ndata: {endpoint_data}\n\n");
                        if socket.write_all(endpoint_frame.as_bytes()).await.is_err() {
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

                let payload: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
                let _ = requests_tx.send((path, payload));
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
            }
        });

        TestServer {
            base_url: format!("http://{addr}"),
            events: events_tx,
            requests: requests_rx,
        }
    }

    /// 读一条 HTTP 报文，返回（请求行, 正文）。
    async fn read_http_message(socket: &mut tokio::net::TcpStream) -> Option<(String, String)> {
        let mut buffer = Vec::new();
        let mut byte = [0_u8; 1];
        while !buffer.ends_with(b"\r\n\r\n") {
            let read = socket.read(&mut byte).await.ok()?;
            if read == 0 {
                return None;
            }
            buffer.push(byte[0]);
        }
        let head = String::from_utf8_lossy(&buffer).into_owned();
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if !name.eq_ignore_ascii_case("content-length") {
                    return None;
                }
                value.trim().parse::<usize>().ok()
            })
            .unwrap_or(0);
        let mut body = vec![0_u8; length];
        if length > 0 {
            socket.read_exact(&mut body).await.ok()?;
        }
        let request_line = head.lines().next().unwrap_or_default().to_owned();
        Some((request_line, String::from_utf8_lossy(&body).into_owned()))
    }

    fn transport(base_url: &str) -> Arc<SseTransport> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("loopback test client");
        Arc::new(SseTransport::with_client(base_url, BTreeMap::new(), client).expect("transport"))
    }

    #[test]
    fn decoder_splits_frames_across_chunk_boundaries() {
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(b"event: endp").is_empty());
        assert!(decoder.push(b"oint\ndata: /mes").is_empty());
        let events = decoder.push(b"sages\n\n");
        assert_eq!(
            events,
            vec![SseEvent {
                event_type: "endpoint".to_owned(),
                data: "/messages".to_owned(),
            }]
        );
    }

    #[test]
    fn decoder_joins_multiline_data_and_defaults_event_type() {
        let mut decoder = SseDecoder::default();
        let events = decoder.push(b": keepalive\r\ndata: {\"a\":1,\r\ndata: \"b\":2}\r\n\r\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "message");
        assert_eq!(events[0].data, "{\"a\":1,\n\"b\":2}");
    }

    #[test]
    fn decoder_drops_frames_without_data() {
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(b"event: ping\n\n").is_empty());
        // 事件类型不得泄漏到下一帧。
        let events = decoder.push(b"data: {}\n\n");
        assert_eq!(events[0].event_type, "message");
    }

    #[test]
    fn resolves_same_origin_endpoint_shapes_and_rejects_cross_origin() {
        let origin = "http://127.0.0.1:9000";
        let post = "http://127.0.0.1:9000/mcp";
        assert_eq!(
            resolve_session_endpoint(origin, post, "http://other/x")
                .expect_err("cross-origin endpoint must fail")
                .to_string(),
            "MCP SSE session endpoint must remain on the configured origin"
        );
        assert_eq!(
            resolve_session_endpoint(origin, post, "/messages?id=1").expect("same origin"),
            "http://127.0.0.1:9000/messages?id=1"
        );
        assert_eq!(
            resolve_session_endpoint(origin, post, "messages").expect("same origin"),
            "http://127.0.0.1:9000/mcp/messages"
        );
    }

    #[test]
    fn rejects_invalid_base_url() {
        let error = SseTransport::new("not a url", BTreeMap::new())
            .err()
            .expect("must fail");
        assert_eq!(error.to_string(), "Invalid base URL: not a url");
    }

    #[tokio::test]
    async fn request_before_connect_reports_not_initialized() {
        let transport = transport("http://127.0.0.1:1");
        let error = transport
            .send_request("tools/list", None, Duration::from_millis(50))
            .await
            .expect_err("must fail");
        assert_eq!(error.code(), error::SERVER_NOT_INITIALIZED);
        assert_eq!(error.message(), "SSE not connected");
    }

    #[tokio::test]
    async fn connect_resolves_endpoint_and_round_trips_request() {
        let mut server = start_server("/messages?session=abc").await;
        let transport = transport(&server.base_url);
        transport.connect().await.expect("connect");
        assert!(transport.is_connected());
        assert_eq!(
            transport.target_url(),
            format!("{}/messages?session=abc", server.base_url)
        );

        let caller = Arc::clone(&transport);
        let handle = tokio::spawn(async move {
            caller
                .send_request("tools/list", Some(json!({})), Duration::from_secs(5))
                .await
        });

        let (path, payload) = server.requests.recv().await.expect("request");
        assert_eq!(path, "/messages?session=abc");
        assert_eq!(payload["method"], json!("tools/list"));
        assert_eq!(payload["jsonrpc"], json!("2.0"));
        let id = payload["id"].clone();
        server
            .events
            .send(format!(
                "data: {}\n\n",
                json!({"jsonrpc": "2.0", "id": id, "result": {"tools": []}})
            ))
            .expect("push");

        let result = handle.await.expect("join").expect("result");
        assert_eq!(result, Some(json!({"tools": []})));
        transport.close().await;
        assert!(!transport.is_connected());
    }

    #[tokio::test]
    async fn error_response_becomes_protocol_error() {
        let mut server = start_server("/messages").await;
        let transport = transport(&server.base_url);
        transport.connect().await.expect("connect");

        let caller = Arc::clone(&transport);
        let handle = tokio::spawn(async move {
            caller
                .send_request("tools/call", None, Duration::from_secs(5))
                .await
        });
        let (_, payload) = server.requests.recv().await.expect("request");
        server
            .events
            .send(format!(
                "data: {}\n\n",
                json!({
                    "jsonrpc": "2.0",
                    "id": payload["id"].clone(),
                    "error": {"code": -32601, "message": "Method not found: tools/call"}
                })
            ))
            .expect("push");

        let error = handle.await.expect("join").expect_err("must fail");
        assert_eq!(error.code(), error::METHOD_NOT_FOUND);
        assert_eq!(error.message(), "Method not found: tools/call");
        transport.close().await;
    }

    #[tokio::test]
    async fn notifications_and_reverse_requests_reach_handler() {
        let server = start_server("/messages").await;
        let transport = transport(&server.base_url);
        let (seen_tx, seen_rx) = std_mpsc::channel::<Value>();
        transport.set_notification_handler(Arc::new(move |node| {
            let _ = seen_tx.send(node);
        }));
        transport.connect().await.expect("connect");

        server
            .events
            .send(
                "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n".to_owned(),
            )
            .expect("push");
        // 带 id 但未命中 pending 且有 method → 服务端反向请求，同样进处理器。
        server
            .events
            .send("data: {\"jsonrpc\":\"2.0\",\"id\":77,\"method\":\"roots/list\"}\n\n".to_owned())
            .expect("push");

        let first = tokio::task::spawn_blocking(move || {
            let first = seen_rx.recv_timeout(Duration::from_secs(5)).expect("first");
            let second = seen_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("second");
            (first, second)
        })
        .await
        .expect("join");
        assert_eq!(first.0["method"], json!("notifications/progress"));
        assert_eq!(first.1["method"], json!("roots/list"));
        assert_eq!(first.1["id"], json!(77));
        transport.close().await;
    }

    #[tokio::test]
    async fn unanswered_request_times_out() {
        let server = start_server("/messages").await;
        let transport = transport(&server.base_url);
        transport.connect().await.expect("connect");
        let error = transport
            .send_request("tools/list", None, Duration::from_millis(150))
            .await
            .expect_err("must fail");
        assert_eq!(error.code(), error::REQUEST_TIMEOUT);
        assert_eq!(error.message(), "Request timeout: tools/list");
        transport.close().await;
    }

    #[tokio::test]
    async fn close_fails_inflight_requests() {
        let server = start_server("/messages").await;
        let transport = transport(&server.base_url);
        transport.connect().await.expect("connect");

        let caller = Arc::clone(&transport);
        let handle = tokio::spawn(async move {
            caller
                .send_request("tools/list", None, Duration::from_secs(10))
                .await
        });
        // 等请求登记进 pending 后再关闭。
        tokio::time::sleep(Duration::from_millis(100)).await;
        transport.close().await;

        let error = handle.await.expect("join").expect_err("must fail");
        assert_eq!(error.code(), error::INTERNAL_ERROR);
        assert_eq!(error.message(), "Transport closed");
    }

    #[tokio::test]
    async fn stream_failure_triggers_disconnect_callback() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            // 建立 SSE 流并发出 endpoint 事件后直接断开 → 触发断连回调。
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut scratch = [0_u8; 1024];
            let _ = socket.read(&mut scratch).await;
            let _ = socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\nevent: endpoint\ndata: /messages\n\n",
                )
                .await;
            let _ = socket.flush().await;
            let _ = socket.shutdown().await;
        });

        let transport = transport(&format!("http://{addr}"));
        let (notify_tx, notify_rx) = std_mpsc::channel::<()>();
        transport.set_disconnect_callback(Arc::new(move || {
            let _ = notify_tx.send(());
        }));
        transport.connect().await.expect("connect");

        tokio::task::spawn_blocking(move || {
            notify_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("disconnect callback");
        })
        .await
        .expect("join");
        assert!(!transport.is_connected());
    }

    #[tokio::test]
    async fn health_ping_posts_notification() {
        let mut server = start_server("/messages").await;
        let transport = transport(&server.base_url);
        assert!(!transport.send_health_ping().await);
        transport.connect().await.expect("connect");
        assert!(transport.send_health_ping().await);
        let (_, payload) = server.requests.recv().await.expect("ping");
        assert_eq!(payload["method"], json!("notifications/ping"));
        assert!(payload.get("id").is_none());
        transport.close().await;
    }
}
