//! 连接生命周期——升级握手、读写双循环与协议层心跳（D1）。
//!
//! 每连接两任务（任务规格 §1）：
//!
//! ```text
//! 读循环（read_loop，连接任务本体）           写循环（write_loop，spawn 独立任务）
//!   Text ──► inbound::handle_text              mpsc rx ──► sink.send
//!   Ping ──► pong_tx.try_send(Pong)            ticker ──► sink.send(Ping，10s)
//!   任意帧 ──► hub.touch                       （写阻塞 = mpsc 填充 = 背压作用点）
//!   timeout(idle) ──► 判死 ──► disconnect + writer.abort()
//! ```
//!
//! # 判死窗口
//!
//! `idle_timeout`（默认 30s = 3× 心跳）：读循环对 `stream.next()` 施加超时，
//! 任何入帧（含 Pong 应答与上行消息）刷新活跃；超时未收到任何帧即断开清理
//! ——旧 STOMP 场景的空闲判死由 broker 心跳协商承担，原生 WS 下由本层接管。
//!
//! # 客户端 Ping 的应答
//!
//! 显式经 mpsc 回送 Pong（不依赖底层自动应答；通道满则丢弃 + debug——
//! Ping 回复非 critical，不构成可靠性承诺）。

use std::net::SocketAddr;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::response::Response;
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::hub::OutboundFrame;
use super::inbound;
use crate::state::AppState;

/// `GET /ws`——原生 WebSocket 升级入口（非 SockJS；方案 §4.2）。
///
/// 鉴权由 Router 层 `localnet_guard` 承担：请求必须来自 loopback，且携带
/// 受信任的精确 Origin 或本地 Bearer 凭据；仅 loopback 来源本身不构成授权。
/// 不满足条件的升级请求不会进入本 handler。
/// principal 即连接内 UUID（`ConnId`），替代旧 Spring 用户名体系。
pub async fn handle_ws(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Response {
    tracing::debug!(peer = %addr, "ws upgrade accepted");
    ws.on_upgrade(move |socket| run_connection(state, socket))
}

/// 单连接生命周期：注册 → 写循环（spawn）→ 读循环（本体）→ 清理。
async fn run_connection(state: AppState, socket: WebSocket) {
    let config = state.hub.config().clone();
    let conn_id = Uuid::new_v4().to_string();
    let (tx, rx) = mpsc::channel::<OutboundFrame>(config.outbound_capacity);
    state.hub.register(&conn_id, tx.clone());

    let (sink, stream) = socket.split();
    let writer = tokio::spawn(write_loop(sink, rx, config.heartbeat_interval));

    let reason = read_loop(&state, &conn_id, stream, tx, config.idle_timeout).await;

    state.hub.disconnect(&conn_id);
    writer.abort();
    tracing::info!(conn_id, reason, "ws connection closed");
}

/// 读循环：上行分发 + 活跃刷新 + 空闲判死。返回结束原因（仅日志用）。
async fn read_loop(
    state: &AppState,
    conn_id: &str,
    mut stream: futures::stream::SplitStream<WebSocket>,
    pong_tx: mpsc::Sender<OutboundFrame>,
    idle_timeout: Duration,
) -> &'static str {
    loop {
        let frame = match tokio::time::timeout(idle_timeout, stream.next()).await {
            Err(_elapsed) => return "idle timeout",
            Ok(None) => return "stream closed",
            Ok(Some(Err(_))) => return "protocol error",
            Ok(Some(Ok(frame))) => frame,
        };
        state.hub.touch(conn_id);
        match frame {
            Message::Text(text) => {
                inbound::handle_text(state, conn_id, text.as_str()).await;
            }
            Message::Ping(_) => {
                if pong_tx.try_send(OutboundFrame::Pong).is_err() {
                    tracing::debug!(conn_id, "pong reply dropped (backpressure)");
                }
            }
            // Pong 应答：touch 已刷新活跃，无需处理。
            Message::Pong(_) => {}
            Message::Close(_) => return "close frame",
            // 契约纯文本（U1）：二进制帧不构成上行消息，忽略。
            Message::Binary(_) => tracing::debug!(conn_id, "binary frame ignored"),
        }
    }
}

/// 写循环：mpsc 出队发送 + 心跳 Ping 帧（D1 协议层一轨，10s 间隔）。
///
/// 写侧阻塞（客户端不读）会使 mpsc 填充——这正是 hub 双档背压的作用点。
async fn write_loop(
    mut sink: futures::stream::SplitSink<WebSocket, Message>,
    mut rx: mpsc::Receiver<OutboundFrame>,
    heartbeat: Duration,
) {
    let mut ticker = tokio::time::interval(heartbeat);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // interval 首拍立即完成，吃掉（连接建立即发 Ping 无意义）。
    loop {
        tokio::select! {
            frame = rx.recv() => {
                match frame {
                    Some(OutboundFrame::Text(text)) => {
                        if sink.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(OutboundFrame::Pong) => {
                        if sink.send(Message::Pong(Bytes::new())).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            _ = ticker.tick() => {
                if sink.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
            }
        }
    }
}
