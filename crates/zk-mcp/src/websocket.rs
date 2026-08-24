//! MCP WebSocket 客户端传输。
//!
//! WS 与 `WS_IDE` 共用这一实现。连接层负责 MCP initialize 握手；本层负责带上限
//! 的 WebSocket 建连、JSON-RPC 请求关联、通知/反向请求、ping/pong、超时与关闭。
//! 配置 headers 只写入握手请求，任何日志都不记录 header 名或值。

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_util::sync::CancellationToken;

use crate::config::McpServerConfig;
use crate::error::McpProtocolError;
use crate::jsonrpc::{JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, RequestId};
use crate::sse::{
    PendingResponse, dispatch_incoming, fail_all_pending, lock, read_lock, write_lock,
};
use crate::transport::{DisconnectCallback, McpTransport, NotificationHandler, timeout_or_default};

const MAX_FRAME_SIZE: usize = 256 * 1024;
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;
const OUTBOUND_QUEUE: usize = 128;

struct Shared {
    connected: AtomicBool,
    pending: Mutex<HashMap<String, PendingResponse>>,
    notification_handler: RwLock<Option<NotificationHandler>>,
    disconnect_callback: RwLock<Option<DisconnectCallback>>,
    outbound: RwLock<Option<mpsc::Sender<Message>>>,
}

impl Shared {
    fn mark_disconnected(&self, reason: &str, notify: bool) {
        let was_connected = self.connected.swap(false, Ordering::AcqRel);
        *write_lock(&self.outbound) = None;
        fail_all_pending(&self.pending, reason);
        if was_connected
            && notify
            && let Some(callback) = read_lock(&self.disconnect_callback).clone()
        {
            callback();
        }
    }
}

/// WebSocket MCP transport with bounded frames/messages and a single ordered writer.
pub struct WebSocketTransport {
    url: String,
    headers: BTreeMap<String, String>,
    request_id: AtomicI64,
    shared: Arc<Shared>,
    io_task: Mutex<Option<JoinHandle<()>>>,
    cancel: CancellationToken,
}

impl WebSocketTransport {
    /// Build a transport. Only `ws://` and `wss://` URLs are accepted.
    ///
    /// # Errors
    /// Returns an error for an invalid URL or a scheme other than WS/WSS.
    pub fn new(
        url: impl Into<String>,
        headers: BTreeMap<String, String>,
    ) -> Result<Self, McpProtocolError> {
        let url = url.into();
        let parsed = reqwest::Url::parse(&url)
            .map_err(|_| McpProtocolError::wrapped("Invalid MCP WebSocket URL"))?;
        if !matches!(parsed.scheme(), "ws" | "wss") || parsed.host_str().is_none() {
            return Err(McpProtocolError::wrapped(
                "MCP WebSocket URL must use ws:// or wss://",
            ));
        }
        Ok(Self {
            url,
            headers,
            request_id: AtomicI64::new(1),
            shared: Arc::new(Shared {
                connected: AtomicBool::new(false),
                pending: Mutex::new(HashMap::new()),
                notification_handler: RwLock::new(None),
                disconnect_callback: RwLock::new(None),
                outbound: RwLock::new(None),
            }),
            io_task: Mutex::new(None),
            cancel: CancellationToken::new(),
        })
    }

    /// Build from an MCP server configuration without logging configured headers.
    ///
    /// # Errors
    /// Returns an error when URL/header configuration is invalid.
    pub fn from_config(config: &McpServerConfig) -> Result<Self, McpProtocolError> {
        let url = config.url.clone().ok_or_else(|| {
            McpProtocolError::wrapped(format!(
                "MCP server '{}' has WebSocket transport but no url",
                config.name
            ))
        })?;
        Self::new(url, config.headers.clone())
    }

    fn request(
        &self,
    ) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, McpProtocolError> {
        let mut request = self.url.as_str().into_client_request().map_err(|error| {
            McpProtocolError::wrapped(format!("Invalid WebSocket request: {error}"))
        })?;
        for (name, value) in &self.headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| McpProtocolError::wrapped("Invalid MCP WebSocket header name"))?;
            let value = HeaderValue::from_str(value)
                .map_err(|_| McpProtocolError::wrapped("Invalid MCP WebSocket header value"))?;
            request.headers_mut().insert(name, value);
        }
        Ok(request)
    }

    fn send_value(&self, payload: &impl Serialize) -> Result<(), McpProtocolError> {
        if !self.shared.connected.load(Ordering::Acquire) {
            return Err(McpProtocolError::not_initialized("WebSocket not connected"));
        }
        let text = serde_json::to_string(payload).map_err(|error| {
            McpProtocolError::wrapped(format!("Failed to serialize JSON-RPC payload: {error}"))
        })?;
        if text.len() > MAX_MESSAGE_SIZE {
            return Err(McpProtocolError::internal(
                "WebSocket message exceeds 1 MiB limit",
            ));
        }
        let sender = read_lock(&self.shared.outbound)
            .clone()
            .ok_or_else(|| McpProtocolError::not_initialized("WebSocket not connected"))?;
        sender
            .try_send(Message::Text(text.into()))
            .map_err(|_| McpProtocolError::internal("WebSocket outbound queue unavailable"))
    }
}

impl McpTransport for WebSocketTransport {
    fn connect(&self) -> BoxFuture<'_, Result<(), McpProtocolError>> {
        Box::pin(async move {
            if self.cancel.is_cancelled() {
                return Err(McpProtocolError::not_initialized(
                    "Closed WebSocket transport cannot reconnect",
                ));
            }
            let request = self.request()?;
            let config = WebSocketConfig::default()
                .max_frame_size(Some(MAX_FRAME_SIZE))
                .max_message_size(Some(MAX_MESSAGE_SIZE));
            let (socket, _) =
                tokio_tungstenite::connect_async_with_config(request, Some(config), true)
                    .await
                    .map_err(|error| {
                        McpProtocolError::wrapped(format!(
                            "MCP WebSocket connection failed: {error}"
                        ))
                    })?;
            let (sender, receiver) = mpsc::channel(OUTBOUND_QUEUE);
            *write_lock(&self.shared.outbound) = Some(sender);
            self.shared.connected.store(true, Ordering::Release);
            let task = tokio::spawn(io_loop(
                socket,
                receiver,
                Arc::clone(&self.shared),
                self.cancel.clone(),
            ));
            if let Some(previous) = lock(&self.io_task).replace(task) {
                previous.abort();
            }
            tracing::info!(url = %self.url, "MCP WebSocket connected");
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
            let id = self.request_id.fetch_add(1, Ordering::Relaxed);
            let key = id.to_string();
            let (sender, receiver) = oneshot::channel();
            lock(&self.shared.pending).insert(key.clone(), sender);
            let request = JsonRpcRequest::new(RequestId::Number(id), method, params);
            if let Err(error) = self.send_value(&request) {
                lock(&self.shared.pending).remove(&key);
                return Err(error);
            }
            let outcome = match tokio::time::timeout(timeout_or_default(timeout), receiver).await {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(_)) => Err(McpProtocolError::internal(
                    "WebSocket response channel closed",
                )),
                Err(_) => Err(McpProtocolError::timeout(format!(
                    "Request timeout: {method}"
                ))),
            };
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
            if let Err(error) = self.send_value(&notification) {
                tracing::warn!(method, %error, "Failed to send MCP WebSocket notification");
            }
        })
    }

    fn send_response(&self, id: RequestId, result: Value) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let response = JsonRpcResponse::success(id.clone(), result);
            if let Err(error) = self.send_value(&response) {
                tracing::warn!(request_id = %id, %error, "Failed to send MCP WebSocket response");
            }
        })
    }

    fn send_health_ping(&self) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            if !self.shared.connected.load(Ordering::Acquire) {
                return false;
            }
            let Some(sender) = read_lock(&self.shared.outbound).clone() else {
                return false;
            };
            sender.try_send(Message::Ping(Vec::new().into())).is_ok()
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
            if let Some(sender) = read_lock(&self.shared.outbound).clone() {
                let _ = sender.try_send(Message::Close(None));
            }
            self.cancel.cancel();
            self.shared.mark_disconnected("Transport closed", false);
            let task = { lock(&self.io_task).take() };
            if let Some(task) = task {
                let _ = task.await;
            }
            tracing::debug!(url = %self.url, "MCP WebSocket closed");
        })
    }
}

async fn io_loop(
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    mut outbound: mpsc::Receiver<Message>,
    shared: Arc<Shared>,
    cancel: CancellationToken,
) {
    let (mut sink, mut stream) = socket.split();
    let reason = loop {
        tokio::select! {
            () = cancel.cancelled() => {
                let _ = sink.send(Message::Close(None)).await;
                break "WebSocket transport closed";
            }
            outbound_message = outbound.recv() => {
                let Some(message) = outbound_message else {
                    break "WebSocket writer closed";
                };
                if sink.send(message).await.is_err() {
                    break "WebSocket write failed";
                }
            }
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<Value>(text.as_str()) {
                            Ok(node) => {
                                let handler = read_lock(&shared.notification_handler).clone();
                                dispatch_incoming(&shared.pending, handler.as_ref(), node);
                            }
                            Err(error) => tracing::warn!(%error, "Invalid JSON from MCP WebSocket"),
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {
                        tracing::warn!("Ignored binary MCP WebSocket message");
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if sink.send(Message::Pong(payload)).await.is_err() {
                            break "WebSocket pong failed";
                        }
                    }
                    Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(_))) | None => break "WebSocket peer closed",
                    Some(Err(error)) => {
                        tracing::debug!(%error, "MCP WebSocket read failed");
                        break "WebSocket read failed";
                    }
                }
            }
        }
    };
    shared.mark_disconnected(reason, !cancel.is_cancelled());
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[test]
    fn rejects_non_websocket_urls_without_echoing_headers() {
        let mut headers = BTreeMap::new();
        headers.insert("Authorization".to_owned(), "secret-token".to_owned());
        let error = WebSocketTransport::new("https://example.test/mcp", headers)
            .err()
            .expect("must reject");
        assert!(!error.to_string().contains("secret-token"));
    }

    #[tokio::test]
    async fn correlates_response_for_real_local_websocket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            while let Some(Ok(message)) = socket.next().await {
                if let Message::Text(text) = message {
                    let request: Value = serde_json::from_str(text.as_str()).unwrap();
                    let response = json!({
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "result": {"ok": true}
                    });
                    socket
                        .send(Message::Text(response.to_string().into()))
                        .await
                        .unwrap();
                    break;
                }
            }
        });
        let transport =
            WebSocketTransport::new(format!("ws://{address}/mcp"), BTreeMap::new()).unwrap();
        transport.connect().await.unwrap();
        let response = transport
            .send_request("tools/list", None, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(response, Some(json!({"ok": true})));
        transport.close().await;
        server.await.unwrap();
    }
}
