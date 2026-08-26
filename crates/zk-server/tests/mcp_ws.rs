//! Batch 4B 集成测试——WS `mcp_operation` 四操作（真实 Router + 随机端口）。
//!
//! 语义来源（旧仓库只读）：`websocket/WebSocketController.handleMcpOperation`
//! （L1449-1503）——`connect` 仅做存在性探测、`disconnect` 恒回成功通知、
//! `refresh` 失败落 `MCP_ERROR`、`list` 回推服务器清单、未知操作
//! `INVALID_MCP_OP`。
//!
//! # 生命周期前提
//!
//! `AppState::mcp()` 只装配不 `start()`（启动由 `main` 驱动），故本测试里管理
//! 器处于「未运行」态：`restart_server` 先撞 `require_running()` →
//! `MCP_CLIENT_MANAGER_NOT_RUNNING`，恰好落进旧实现的 catch 分支
//! （`MCP_ERROR`）；`get_connection` / `remove_server` 不受运行态影响。
//! 集成测试因此不会拉起任何真实 MCP 子进程。

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use zk_server::config::Config;
use zk_server::routes::build_router;
use zk_server::state::AppState;
use zk_server::ws::WsConfig;

/// 测试客户端流类型。
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// 起真实 server；返回 (地址, 已建会话 id)。
async fn spawn_server() -> (SocketAddr, String) {
    let db = zk_db::Db::open_in_memory().expect("in-memory db");
    let state = AppState::new_with_ws(
        db.clone(),
        Config::test_config(),
        WsConfig::fast_for_tests(),
    );
    // 与 `skill_ws` 同因：工作目录必须落 canonical 形式，否则会话绑定被判
    // `WORKSPACE_REBOUND`。
    let workspace = std::fs::canonicalize(std::env::temp_dir()).expect("canonical temp dir");
    let session_id = db
        .create_session("qwen3.7-max", &workspace.to_string_lossy())
        .await
        .expect("seed session")
        .id;
    let app: Router = build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .expect("serve");
    });
    (addr, session_id)
}

/// 连接 `/ws` 并完成 bind 握手（消费 `session_restored`）。
async fn connect_bound(addr: SocketAddr, session_id: &str) -> WsStream {
    let (mut ws, _) = connect_async(trusted_ws_request(addr))
        .await
        .expect("ws connect");
    send_json(
        &mut ws,
        &serde_json::json!({
            "type": "bind_session",
            "sessionId": session_id,
            "bindRequestId": "br-mcp",
            "bindingEpoch": 1,
            "protocolVersion": 3,
        }),
    )
    .await;
    let restored = next_json(&mut ws).await;
    assert_eq!(restored["type"], "session_restored");
    ws
}

fn trusted_ws_request(addr: SocketAddr) -> tokio_tungstenite::tungstenite::http::Request<()> {
    let mut request = format!("ws://{addr}/ws")
        .into_client_request()
        .expect("ws request");
    request.headers_mut().insert(
        "Origin",
        "http://localhost:5273".parse().expect("trusted origin"),
    );
    request
}

/// 发送一帧 JSON 文本。
async fn send_json(ws: &mut WsStream, value: &serde_json::Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .expect("send json frame");
}

/// 读下一个文本帧（跳过控制帧；5s 超时防挂死）。
async fn next_json(ws: &mut WsStream) -> serde_json::Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("frame within 5s")
            .expect("stream still open")
            .expect("ws read ok");
        match frame {
            Message::Text(text) => {
                return serde_json::from_str(text.as_str()).expect("frame is json");
            }
            Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
            Message::Close(_) => panic!("unexpected close while waiting for text frame"),
        }
    }
}

/// `mcp_operation` 上行帧。
fn mcp_frame(operation: &str, server_id: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "mcp_operation",
        "operation": operation,
        "serverId": server_id,
    })
}

// ── connect：仅存在性探测，未知服务器 → MCP_NOT_FOUND ──────────────────────

#[tokio::test]
async fn connect_on_unknown_server_reports_not_found() {
    let (addr, session_id) = spawn_server().await;
    let mut ws = connect_bound(addr, &session_id).await;

    send_json(&mut ws, &mcp_frame("connect", "weather")).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["type"], "error");
    assert_eq!(frame["code"], "MCP_NOT_FOUND");
    assert_eq!(frame["message"], "MCP server not found: weather");
    assert_eq!(frame["retryable"], false);
    // 走会话推送（带路由字段）。
    assert_eq!(frame["_sessionId"], session_id);
}

// ── disconnect：返回值丢弃，恒推成功通知 ───────────────────────────────────

#[tokio::test]
async fn disconnect_always_reports_success_notification() {
    let (addr, session_id) = spawn_server().await;
    let mut ws = connect_bound(addr, &session_id).await;

    send_json(&mut ws, &mcp_frame("disconnect", "weather")).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["type"], "notification");
    assert_eq!(frame["key"], "mcp-disconnect");
    assert_eq!(frame["level"], "info");
    assert_eq!(frame["message"], "MCP server disconnected: weather");
    assert_eq!(frame["timeout"], 3000);
}

// ── refresh：重启失败落 MCP_ERROR（此处因管理器未运行） ────────────────────

#[tokio::test]
async fn refresh_failure_reports_mcp_error() {
    let (addr, session_id) = spawn_server().await;
    let mut ws = connect_bound(addr, &session_id).await;

    send_json(&mut ws, &mcp_frame("refresh", "weather")).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["type"], "error");
    assert_eq!(frame["code"], "MCP_ERROR");
    assert_eq!(frame["message"], "MCP_CLIENT_MANAGER_NOT_RUNNING");
    assert_eq!(frame["retryable"], false);
}

// ── list：无连接则一帧不推（下一帧仍是随后 ping 的 pong） ──────────────────

#[tokio::test]
async fn list_pushes_nothing_when_no_connections() {
    let (addr, session_id) = spawn_server().await;
    let mut ws = connect_bound(addr, &session_id).await;

    send_json(&mut ws, &mcp_frame("list", "")).await;
    // 同一连接上紧随其后的 ping：若 `list` 误推了帧，先到的就不会是 pong。
    send_json(&mut ws, &serde_json::json!({ "type": "ping" })).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(
        frame["type"], "pong",
        "unexpected frame before pong: {frame}"
    );
}

// ── 未知操作 → INVALID_MCP_OP ─────────────────────────────────────────────

#[tokio::test]
async fn unknown_operation_reports_invalid_op() {
    let (addr, session_id) = spawn_server().await;
    let mut ws = connect_bound(addr, &session_id).await;

    send_json(&mut ws, &mcp_frame("bogus", "weather")).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["type"], "error");
    assert_eq!(frame["code"], "INVALID_MCP_OP");
    assert_eq!(frame["message"], "Unknown MCP operation: bogus");
}

// ── 未绑定会话的 mcp_operation 被丢弃（旧 L1452-1455 的 warn 分支） ────────

#[tokio::test]
async fn unbound_connection_drops_mcp_operation() {
    let (addr, session_id) = spawn_server().await;
    let (mut ws, _) = connect_async(trusted_ws_request(addr))
        .await
        .expect("ws connect");

    send_json(&mut ws, &mcp_frame("disconnect", "weather")).await;
    // 未绑定 → 无任何下行；随后完成绑定，首帧必是 `session_restored`。
    send_json(
        &mut ws,
        &serde_json::json!({
            "type": "bind_session",
            "sessionId": session_id,
            "bindRequestId": "br-mcp-late",
            "bindingEpoch": 1,
            "protocolVersion": 3,
        }),
    )
    .await;
    let frame = next_json(&mut ws).await;
    assert_eq!(
        frame["type"], "session_restored",
        "unbound mcp_operation leaked a frame: {frame}"
    );
}
