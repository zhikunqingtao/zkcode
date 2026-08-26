//! WS 通道集成测试——tokio-tungstenite 客户端打真实 Router（随机端口）。
//!
//! 覆盖（任务规格 §7）：
//! 1. 未绑定 ping → `pong{bindRequired:true,serverNow}`（无路由字段）；
//! 2. bind → `session_restored`（bindRequestId 回显 / bindingEpoch / 消息恢复）
//!    → hub.push 收扁平信封（`_sessionId`/`_bindingEpoch`/`seq`/`ts` 顶层）；
//! 3. 同会话重绑 epoch 递增；非递增 → `STALE_BINDING_EPOCH`（回显当前）；
//! 4. 未知 type → `protocol_error{UNKNOWN_MESSAGE_TYPE:<原 type>}`；
//!    非 JSON → `MALFORMED_MESSAGE`；
//! 5. 不读客户端 + 海量 delta → 丢弃计数增长且推送不阻塞；
//! 6. 订阅者消失 → critical 入 pending → 重连 bind 后按序重放（带新 epoch）；
//! 7. 心跳双轨：保活（回 Pong 连接存活）与判死（不响应被清理）；
//! 8. bind 不存在会话 → `SESSION_NOT_FOUND`。

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use zk_db::model::{MessageRole, NewMessage, StoredBlock};
use zk_server::config::Config;
use zk_server::routes::build_router;
use zk_server::state::AppState;
use zk_server::ws::{WsConfig, WsHub};

/// 测试客户端流类型。
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// 起真实 server（随机端口 + fast WS 参数）；返回 (地址, hub 句柄, db)。
async fn spawn_server() -> (SocketAddr, WsHub, zk_db::Db) {
    let db = zk_db::Db::open_in_memory().expect("in-memory db");
    let state = AppState::new_with_ws(
        db.clone(),
        Config::test_config(),
        WsConfig::fast_for_tests(),
    );
    let hub = state.hub.clone();
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
    (addr, hub, db)
}

/// 连接 `/ws`（tungstenite 0.28 默认 auto-pong：read 到 Ping 自动 queue Pong，
/// flush 后发出——贴近浏览器行为）。
async fn connect(addr: SocketAddr) -> WsStream {
    let mut request = format!("ws://{addr}/ws")
        .into_client_request()
        .expect("ws request");
    request.headers_mut().insert(
        "Origin",
        "http://localhost:5273".parse().expect("trusted origin"),
    );
    let (stream, _) = connect_async(request).await.expect("ws connect");
    stream
}

/// 发送一帧 JSON 文本。
async fn send_json(ws: &mut WsStream, value: &serde_json::Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .expect("send json frame");
}

/// 读下一个文本帧（跳过 Ping/Pong 控制帧；5s 超时防挂死）。
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

/// `bind_session` 上行帧。
fn bind_frame(session_id: &str, request_id: &str, epoch: i64) -> serde_json::Value {
    serde_json::json!({
        "type": "bind_session",
        "sessionId": session_id,
        "bindRequestId": request_id,
        "bindingEpoch": epoch,
        "protocolVersion": 3,
    })
}

/// 建库种子会话（一条 user 文本消息），返回 session id。
async fn seed_session(db: &zk_db::Db) -> String {
    let summary = db
        .create_session("qwen3.7-max", "/tmp")
        .await
        .expect("seed session");
    db.append_message(
        &summary.id,
        NewMessage {
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: "帮我看下报错".to_owned(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .expect("seed message");
    summary.id
}

/// 轮询等待条件成立（3s 上限，防挂死）。
async fn wait_for(what: &str, mut probe: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !probe() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met in time: {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// error 下行（critical 档代表）。
fn error_msg(code: &str) -> zk_protocol::ServerMessage {
    zk_protocol::ServerMessage::Error {
        code: code.to_owned(),
        message: "test".to_owned(),
        retryable: false,
    }
}

// ── 场景 1：未绑定 ping → bindRequired pong（pushToPrincipal 语义） ─────────

#[tokio::test]
async fn websocket_upgrade_rejects_untrusted_browser_origin() {
    let (addr, _hub, _db) = spawn_server().await;
    let mut request = format!("ws://{addr}/ws")
        .into_client_request()
        .expect("ws request");
    request.headers_mut().insert(
        "Origin",
        "https://attacker.example".parse().expect("hostile origin"),
    );
    let error = connect_async(request)
        .await
        .expect_err("cross-origin websocket must not upgrade");
    let response = match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => response,
        other => panic!("expected HTTP rejection, got {other}"),
    };
    assert_eq!(response.status(), 403);
}

#[tokio::test]
async fn unbound_ping_gets_bind_required_pong() {
    let (addr, _hub, _db) = spawn_server().await;
    let mut ws = connect(addr).await;
    send_json(&mut ws, &serde_json::json!({"type": "ping"})).await;
    let pong = next_json(&mut ws).await;
    assert_eq!(pong["type"], "pong");
    assert_eq!(pong["bindRequired"], true);
    assert!(pong["serverNow"].as_i64().is_some());
    assert!(pong.get("_sessionId").is_none(), "no routing fields");
    assert!(pong.get("_bindingEpoch").is_none());
}

// ── 场景 2：bind → session_restored → push 扁平信封 ────────────────────────

#[tokio::test]
async fn bind_restores_then_push_delivers_flat_envelope() {
    let (addr, hub, db) = spawn_server().await;
    let session_id = seed_session(&db).await;
    let mut ws = connect(addr).await;

    send_json(&mut ws, &bind_frame(&session_id, "br-1", 1)).await;
    let restored = next_json(&mut ws).await;
    assert_eq!(restored["type"], "session_restored");
    assert_eq!(restored["bindRequestId"], "br-1");
    assert_eq!(restored["bindingEpoch"], 1);
    assert_eq!(restored["protocolVersion"], 3);
    assert_eq!(restored["metadata"]["sessionId"], session_id);
    assert_eq!(restored["metadata"]["permissionMode"], "DEFAULT");
    assert_eq!(restored["messages"][0]["type"], "user");
    assert_eq!(
        restored["messages"][0]["content"][0]["text"],
        "帮我看下报错"
    );
    assert!(
        restored.get("_sessionId").is_none(),
        "restored is direct push"
    );

    // 引擎侧推送（S9 将调用的 API）：扁平信封全部字段在顶层。
    hub.push(
        &session_id,
        zk_protocol::ServerMessage::StreamDelta {
            delta: "hello".to_owned(),
        },
    )
    .await;
    let delta = next_json(&mut ws).await;
    assert_eq!(delta["type"], "stream_delta");
    assert_eq!(delta["delta"], "hello");
    assert_eq!(delta["_sessionId"], session_id);
    assert_eq!(delta["_bindingEpoch"], 1);
    assert_eq!(delta["seq"], 1, "session-scoped monotonic seq");
    assert!(delta["ts"].as_i64().is_some());

    // 绑定后的应用层 ping 走会话推送（带路由字段与 seq）。
    send_json(&mut ws, &serde_json::json!({"type": "ping"})).await;
    let pong = next_json(&mut ws).await;
    assert_eq!(pong["type"], "pong");
    assert_eq!(pong["_sessionId"], session_id);
    assert_eq!(pong["_bindingEpoch"], 1);
    assert_eq!(pong["seq"], 2);
}

// ── 场景 3：重绑 epoch 递增 / 非递增 STALE ─────────────────────────────────

#[tokio::test]
async fn rebind_increments_epoch_and_stale_is_rejected() {
    let (addr, _hub, db) = spawn_server().await;
    let session_id = seed_session(&db).await;
    let mut ws = connect(addr).await;

    send_json(&mut ws, &bind_frame(&session_id, "br-1", 1)).await;
    let first = next_json(&mut ws).await;
    assert_eq!(first["bindingEpoch"], 1);

    // 递增重绑成功。
    send_json(&mut ws, &bind_frame(&session_id, "br-2", 5)).await;
    let second = next_json(&mut ws).await;
    assert_eq!(second["bindingEpoch"], 5);
    assert_eq!(second["bindRequestId"], "br-2");

    // 非递增（<= 当前）→ STALE_BINDING_EPOCH，回显当前 epoch。
    send_json(&mut ws, &bind_frame(&session_id, "br-3", 5)).await;
    let stale = next_json(&mut ws).await;
    assert_eq!(stale["type"], "protocol_error");
    assert_eq!(stale["code"], "STALE_BINDING_EPOCH");
    assert_eq!(stale["bindingEpoch"], 5);
    assert_eq!(stale["supportedVersion"], 3);
}

// ── 场景 4：未知 type / 非 JSON ─────────────────────────────────────────────

#[tokio::test]
async fn unknown_type_and_malformed_frame_get_protocol_error() {
    let (addr, _hub, _db) = spawn_server().await;
    let mut ws = connect(addr).await;

    send_json(&mut ws, &serde_json::json!({"type": "wibble"})).await;
    let unknown = next_json(&mut ws).await;
    assert_eq!(unknown["type"], "protocol_error");
    assert_eq!(unknown["code"], "UNKNOWN_MESSAGE_TYPE:wibble");

    // 已知 type 但字段坏 → INVALID_MESSAGE。
    send_json(
        &mut ws,
        &serde_json::json!({"type": "bind_session", "sessionId": 42}),
    )
    .await;
    let invalid = next_json(&mut ws).await;
    assert_eq!(invalid["code"], "INVALID_MESSAGE");

    // 非 JSON 文本帧 → MALFORMED_MESSAGE。
    ws.send(Message::Text("this is not json".into()))
        .await
        .expect("send raw text");
    let malformed = next_json(&mut ws).await;
    assert_eq!(malformed["code"], "MALFORMED_MESSAGE");
}

// ── 场景 5：不读客户端 + 海量 delta → 丢弃计数增长且不阻塞 ─────────────────

#[tokio::test]
async fn delta_backpressure_drops_without_blocking() {
    let (addr, hub, db) = spawn_server().await;
    let session_id = seed_session(&db).await;
    let mut ws = connect(addr).await;
    send_json(&mut ws, &bind_frame(&session_id, "br-1", 1)).await;
    let _restored = next_json(&mut ws).await;

    // 之后客户端完全停止读取：TCP 缓冲 + mpsc(容量 4) 填满后 delta 丢弃。
    let started = tokio::time::Instant::now();
    for i in 0..20_000 {
        hub.push(
            &session_id,
            zk_protocol::ServerMessage::StreamDelta {
                delta: format!("payload-{i:05}"),
            },
        )
        .await;
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "delta pushes blocked: {elapsed:?}"
    );
    assert!(
        hub.delta_dropped_total() > 0,
        "drops must be counted once buffers saturate"
    );
    assert_eq!(hub.pending_len(&session_id), 0, "delta never pends");
}

// ── 场景 6：critical pending → 重连 bind 后按序重放 ─────────────────────────

#[tokio::test]
async fn critical_pends_then_replays_in_order_after_reconnect() {
    let (addr, hub, db) = spawn_server().await;
    let session_id = seed_session(&db).await;

    // 客户端 1：bind 后直接消失。
    let mut first = connect(addr).await;
    send_json(&mut first, &bind_frame(&session_id, "br-1", 1)).await;
    let _restored = next_json(&mut first).await;
    drop(first);
    wait_for("first client disconnect", || hub.connection_count() == 0).await;

    // 无订阅者：critical 全部入 pending。
    for code in ["E1", "E2", "E3"] {
        hub.push(&session_id, error_msg(code)).await;
    }
    assert_eq!(hub.pending_len(&session_id), 3);

    // 客户端 2：更大 epoch 重绑 → restored 先行，pending FIFO 随后。
    let mut second = connect(addr).await;
    send_json(&mut second, &bind_frame(&session_id, "br-2", 2)).await;
    let restored = next_json(&mut second).await;
    assert_eq!(restored["type"], "session_restored");
    assert_eq!(restored["bindingEpoch"], 2);

    let mut codes = Vec::new();
    for _ in 0..3 {
        let frame = next_json(&mut second).await;
        assert_eq!(frame["type"], "error");
        assert_eq!(frame["_bindingEpoch"], 2, "replay carries new epoch");
        assert_eq!(frame["_sessionId"], session_id);
        codes.push(frame["code"].as_str().expect("code").to_owned());
    }
    assert_eq!(codes, vec!["E1", "E2", "E3"], "FIFO replay order");
    assert_eq!(hub.pending_len(&session_id), 0);
}

// ── 场景 7：心跳双轨（保活 / 判死） ─────────────────────────────────────────

#[tokio::test]
async fn heartbeat_pings_keep_alive_when_ponged() {
    let (addr, hub, _db) = spawn_server().await;
    let mut ws = connect(addr).await;

    // fast 配置 50ms 心跳：600ms 窗口内应见多个 Ping；每次读后 flush 把
    // auto-pong 刷到线上（对齐浏览器自动应答行为）。
    let mut pings = 0u32;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(600);
    while tokio::time::Instant::now() < deadline {
        let frame = tokio::time::timeout(Duration::from_millis(200), ws.next())
            .await
            .expect("frame within 200ms")
            .expect("connection alive");
        let frame = frame.expect("ws read ok");
        match frame {
            Message::Ping(_) => {
                pings += 1;
                ws.flush().await.expect("flush pong");
            }
            Message::Pong(_)
            | Message::Close(_)
            | Message::Text(_)
            | Message::Binary(_)
            | Message::Frame(_) => {}
        }
    }
    assert!(pings >= 2, "expected periodic pings, got {pings}");
    assert_eq!(hub.connection_count(), 1, "connection survives with pongs");
}

#[tokio::test]
async fn unresponsive_client_is_reaped_by_idle_timeout() {
    let (addr, hub, _db) = spawn_server().await;
    // 完全不读不写：tungstenite 的 auto-pong 仅在 read 路径触发，不 read 即
    // 不应答——服务端 idle_timeout（fast 配置 400ms）后判死清理。
    let mut silent = connect(addr).await;

    wait_for("silent client reaped", || hub.connection_count() == 0).await;
    // 客户端侧感知断开（Close 帧或流结束/错误）。判死前服务端写循环已排入
    // 的 Ping 帧可能仍躺在 TCP 缓冲里，读到时跳过——只等终止信号。
    let ended = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match silent.next().await {
                None | Some(Err(_) | Ok(Message::Close(_))) => return,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "close within 2s");
}

// ── 场景 8：bind 不存在会话 ─────────────────────────────────────────────────

#[tokio::test]
async fn bind_unknown_session_is_rejected() {
    let (addr, _hub, _db) = spawn_server().await;
    let mut ws = connect(addr).await;
    send_json(&mut ws, &bind_frame("no-such-session", "br-1", 1)).await;
    let rejected = next_json(&mut ws).await;
    assert_eq!(rejected["type"], "protocol_error");
    assert_eq!(rejected["code"], "SESSION_NOT_FOUND");
    assert_eq!(rejected["bindRequestId"], "br-1");
}
