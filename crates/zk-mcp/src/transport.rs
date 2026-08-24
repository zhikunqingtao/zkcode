//! MCP 传输层统一契约 — SSE / Streamable HTTP / STDIO 的公共接口。
//!
//! 逐字对照 Java 基线 `mcp/McpTransport.java`（67 行）的七个方法：
//! `connect` / `sendRequest` / `sendNotification` / `sendResponse` /
//! `sendHealthPing`（默认实现 = `isConnected`）/ `isConnected` /
//! `setNotificationHandler` / `close`。
//!
//! 形态差异（Rust 侧必然）：
//! - Java 用 `CompletableFuture<Void>` + 阻塞 `get(timeout)`；Rust 用
//!   `async` + [`BoxFuture`]（对象安全，与 zk-tools `Tool`、zk-llm
//!   `ChatProvider` 同形——D-S6-1 既定裁决），超时由
//!   `tokio::time::timeout` 施加；
//! - Java 的 `sendNotification` / `sendResponse` / `close` 是同步 void 且内部
//!   吞异常；Rust 保持「不返回错误、内部 `warn` 记录」的等价语义，仅因需要
//!   `await` 写 I/O 而返回 future；
//! - 所有方法取 `&self`（而非 `&mut self`）：传输实例被 `Arc` 多处共享
//!   （连接层持有 + 后台读流任务持有），内部状态一律以
//!   `Mutex` / `RwLock` / 原子量承载；
//! - Java 的 `disconnectCallback` 只存在于 `McpSseTransport` 的具体类上
//!   （`McpServerConnection` 靠 `instanceof` 向下转型注入），Rust 侧提升为
//!   trait 的默认空实现方法，避免 `dyn Any` 向下转型。

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::Value;

use crate::error::McpProtocolError;
use crate::jsonrpc::RequestId;

/// 默认请求超时（对照 Java 各传输的 `DEFAULT_TIMEOUT_MS = 30_000`）。
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// 服务端入站通知 / 反向请求处理器（对照 Java `Consumer<JsonNode>`）。
pub type NotificationHandler = Arc<dyn Fn(Value) + Send + Sync>;

/// 传输断开回调（对照 Java `McpSseTransport.setDisconnectCallback`）。
pub type DisconnectCallback = Arc<dyn Fn() + Send + Sync>;

/// MCP 传输层统一接口。
pub trait McpTransport: Send + Sync {
    /// 建立连接。
    ///
    /// # Errors
    /// 连接失败（进程启动失败 / HTTP 握手失败 / SSE 流建立失败）时返回。
    fn connect(&self) -> BoxFuture<'_, Result<(), McpProtocolError>>;

    /// 发送 JSON-RPC 请求并等待响应，返回 `result` 字段（服务端可省略 →
    /// `None`，对照 Java `node.has("result") ? ... : null`）。
    ///
    /// # Errors
    /// 未连接、超时、服务端返回 `error` 对象、或底层 I/O 失败时返回。
    fn send_request<'a>(
        &'a self,
        method: &'a str,
        params: Option<Value>,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Option<Value>, McpProtocolError>>;

    /// 发送 JSON-RPC 通知 — 不期望响应，失败仅记录日志。
    fn send_notification<'a>(&'a self, method: &'a str, params: Option<Value>)
    -> BoxFuture<'a, ()>;

    /// 发送 JSON-RPC 响应 — 回复服务端发起的反向请求（如 `roots/list`）。
    ///
    /// `id` 必须原样回写（含 String / Number 类型）。
    fn send_response(&self, id: RequestId, result: Value) -> BoxFuture<'_, ()>;

    /// 健康探测 — 默认返回连接状态（对照 Java `default sendHealthPing()`）。
    fn send_health_ping(&self) -> BoxFuture<'_, bool> {
        Box::pin(async move { self.is_connected() })
    }

    /// 连接是否存活。
    fn is_connected(&self) -> bool;

    /// 注册服务端通知 / 反向请求处理器。
    fn set_notification_handler(&self, handler: NotificationHandler);

    /// 注册断开回调 — 仅 SSE 传输有实际行为（默认空实现）。
    fn set_disconnect_callback(&self, callback: DisconnectCallback) {
        let _ = callback;
    }

    /// 传输是否在 [`McpTransport::connect`] 内部自行完成 `initialize` +
    /// `notifications/initialized` 握手（默认 `false`）。
    ///
    /// 仅 Streamable HTTP 为 `true`：MCP 2025-03-26 的会话 ID 只能从
    /// `initialize` 响应头 `Mcp-Session-Id` 获得，故握手必须在传输层完成。
    /// 连接层据此跳过重复的 `initialize`——Java 侧连接层无条件再发一次，
    /// 对同一会话构成协议违规（见 [`crate::streamable_http`] 偏离说明）。
    fn performs_own_handshake(&self) -> bool {
        false
    }

    /// 关闭连接并释放资源。
    fn close(&self) -> BoxFuture<'_, ()>;
}

/// 将超时归一为「≤0 视为默认值」（对照 Java `timeoutMs > 0 ? timeoutMs :
/// DEFAULT_REQUEST_TIMEOUT_MS`——Rust 的 [`Duration`] 无负值，零值等价）。
#[must_use]
pub fn timeout_or_default(timeout: Duration) -> Duration {
    if timeout.is_zero() {
        DEFAULT_REQUEST_TIMEOUT
    } else {
        timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_timeout_falls_back_to_default() {
        assert_eq!(timeout_or_default(Duration::ZERO), DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(
            timeout_or_default(Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }

    /// 默认 `send_health_ping` 必须等于 `is_connected`（对照 Java 默认方法）。
    #[tokio::test]
    async fn default_health_ping_mirrors_connected_state() {
        struct Stub(bool);
        impl McpTransport for Stub {
            fn connect(&self) -> BoxFuture<'_, Result<(), McpProtocolError>> {
                Box::pin(async { Ok(()) })
            }
            fn send_request<'a>(
                &'a self,
                _method: &'a str,
                _params: Option<Value>,
                _timeout: Duration,
            ) -> BoxFuture<'a, Result<Option<Value>, McpProtocolError>> {
                Box::pin(async { Ok(None) })
            }
            fn send_notification<'a>(
                &'a self,
                _method: &'a str,
                _params: Option<Value>,
            ) -> BoxFuture<'a, ()> {
                Box::pin(async {})
            }
            fn send_response(&self, _id: RequestId, _result: Value) -> BoxFuture<'_, ()> {
                Box::pin(async {})
            }
            fn is_connected(&self) -> bool {
                self.0
            }
            fn set_notification_handler(&self, _handler: NotificationHandler) {}
            fn close(&self) -> BoxFuture<'_, ()> {
                Box::pin(async {})
            }
        }

        assert!(Stub(true).send_health_ping().await);
        assert!(!Stub(false).send_health_ping().await);
    }
}
