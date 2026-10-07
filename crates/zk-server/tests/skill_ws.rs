//! 3B.7 集成测试——WS `slash_command` 的 `/skill` 分支（真实 Router + 随机端口）。
//!
//! 语义来源（旧仓库只读，`581d407b`）：`command/impl/SkillCommand.execute`
//! 与 `WebSocketController.handleSlashCommand` 的 PROMPT 分支——先推
//! `command_result{resultType:"prompt"}`（正文不下行），再把渲染后的提示词
//! 当用户文本注入对话；失败走 `error{code:"COMMAND_ERROR",retryable:false}`。
//!
//! 覆盖：
//! 1. 命中技能 → `command_result` 形状 + 引擎收到渲染后的 `user_message`；
//! 2. 带参数 → `arguments` 定义的位置参数替换进模板；
//! 3. 未知技能 → `COMMAND_ERROR` + 可用技能清单；
//! 4. 空参数 → `COMMAND_ERROR` 用法提示；
//! 5. 非 `/skill` 的 slash 命令仍原样转发引擎（回归）；
//! 6. 注册表内的 `PROMPT` 命令（Batch 8A 的 `/retry`）走同一 PROMPT 分支。

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use zk_protocol::ClientMessage;
use zk_server::config::Config;
use zk_server::routes::build_router;
use zk_server::state::AppState;
use zk_server::ws::{EngineHook, WsConfig};

/// 测试客户端流类型。
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// 记录型引擎挂点——把 `dispatch_to_engine` 的入参攒下来供断言。
#[derive(Default)]
struct RecordingEngine {
    /// 已收到的上行消息（按到达序）。
    seen: Mutex<Vec<ClientMessage>>,
}

impl RecordingEngine {
    /// 取快照（锁不外泄）。
    fn snapshot(&self) -> Vec<ClientMessage> {
        self.seen.lock().expect("engine log lock").clone()
    }
}

impl EngineHook for RecordingEngine {
    fn on_client_message(&self, _session_id: &str, message: ClientMessage) {
        self.seen.lock().expect("engine log lock").push(message);
    }
}

/// 起真实 server + 记录型引擎；返回 (地址, 已绑定会话 id, 引擎句柄)。
async fn spawn_bound() -> (SocketAddr, String, Arc<RecordingEngine>) {
    let (addr, session_id, engine, _db) = spawn_bound_with_db().await;
    (addr, session_id, engine)
}

/// 同上，另外交出库句柄——供需要预置会话消息的用例（如 `/retry`）。
async fn spawn_bound_with_db() -> (SocketAddr, String, Arc<RecordingEngine>, zk_db::Db) {
    let db = zk_db::Db::open_in_memory().expect("in-memory db");
    let state = AppState::new_with_ws(
        db.clone(),
        Config::test_config(),
        WsConfig::fast_for_tests(),
    );
    let engine = Arc::new(RecordingEngine::default());
    state.hub.set_engine(engine.clone());
    // 工作目录必须落 canonical 形式：`/tmp` 在 macOS 上解析为 `/private/tmp`，
    // 会被 `require_current_binding` 判成 `WORKSPACE_REBOUND`（真实建会话路径
    // 存的即 canonical 路径），令本地执行的斜杠命令拿不到上下文。
    let workspace = std::fs::canonicalize(std::env::temp_dir()).expect("canonical temp dir");
    let session_id = db
        .create_session("qwen3.8-max-0902", &workspace.to_string_lossy())
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
    (addr, session_id, engine, db)
}

/// 连接 `/ws` 并完成 bind 握手（消费 `session_restored`）。
async fn connect_bound(addr: SocketAddr, session_id: &str) -> WsStream {
    let mut request = format!("ws://{addr}/ws")
        .into_client_request()
        .expect("ws request");
    request.headers_mut().insert(
        "Origin",
        "http://localhost:5273".parse().expect("trusted origin"),
    );
    let (mut ws, _) = connect_async(request).await.expect("ws connect");
    send_json(
        &mut ws,
        &serde_json::json!({
            "type": "bind_session",
            "sessionId": session_id,
            "bindRequestId": "br-skill",
            "bindingEpoch": 1,
            "protocolVersion": 4,
        }),
    )
    .await;
    let restored = next_json(&mut ws).await;
    assert_eq!(restored["type"], "session_restored");
    ws
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

/// `slash_command` 上行帧。
fn slash_frame(command: &str, args: &str) -> serde_json::Value {
    serde_json::json!({"type": "slash_command", "command": command, "args": args})
}

/// 轮询等待引擎收到至少 `n` 条消息（3s 上限）。
async fn wait_for_engine(engine: &RecordingEngine, n: usize) -> Vec<ClientMessage> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let seen = engine.snapshot();
        if seen.len() >= n {
            return seen;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "engine did not receive {n} message(s) in time"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ── 场景 1：命中技能 → prompt 通知 + 渲染结果注入引擎 ──────────────────────

#[tokio::test]
async fn skill_command_pushes_prompt_notice_and_injects_rendered_text() {
    let (addr, session_id, engine) = spawn_bound().await;
    let mut ws = connect_bound(addr, &session_id).await;

    send_json(&mut ws, &slash_frame("skill", "commit")).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["type"], "command_result");
    assert_eq!(frame["command"], "skill");
    assert_eq!(frame["resultType"], "prompt");
    // 旧实现「不含完整提示词」——`output` / `data` 均不下行。
    assert!(
        frame.get("output").is_none(),
        "prompt notice carries output"
    );
    assert!(frame.get("data").is_none(), "prompt notice carries data");
    // 走会话推送（带路由字段与 seq）。
    assert_eq!(frame["_sessionId"], session_id);
    assert_eq!(frame["_bindingEpoch"], 1);

    let seen = wait_for_engine(&engine, 1).await;
    match &seen[0] {
        ClientMessage::UserMessage {
            text,
            attachments,
            references,
        } => {
            assert!(!text.is_empty(), "rendered prompt must not be empty");
            assert!(!text.starts_with("---"), "frontmatter leaked into prompt");
            assert!(attachments.is_none());
            assert!(references.is_none());
        }
        other => panic!("expected user_message injection, got {}", other.kind()),
    }
}

// ── 场景 2：位置参数替换进模板 ─────────────────────────────────────────────

#[tokio::test]
async fn skill_command_substitutes_declared_arguments() {
    let (addr, session_id, engine) = spawn_bound().await;
    let mut ws = connect_bound(addr, &session_id).await;

    // `debug.md` 声明 `arguments: error_description`。
    send_json(&mut ws, &slash_frame("skill", "debug   missing-module")).await;
    assert_eq!(next_json(&mut ws).await["resultType"], "prompt");

    let seen = wait_for_engine(&engine, 1).await;
    let ClientMessage::UserMessage { text, .. } = &seen[0] else {
        panic!("expected user_message injection");
    };
    assert!(text.contains("missing-module"), "argument not substituted");
    assert!(
        !text.contains("{{error_description}}"),
        "placeholder left unreplaced"
    );
}

// ── 场景 3/4：错误路径（未知技能 / 空参数）→ COMMAND_ERROR ─────────────────

#[tokio::test]
async fn unknown_skill_reports_command_error_with_catalog() {
    let (addr, session_id, engine) = spawn_bound().await;
    let mut ws = connect_bound(addr, &session_id).await;

    send_json(&mut ws, &slash_frame("skill", "nope")).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["type"], "error");
    assert_eq!(frame["code"], "COMMAND_ERROR");
    assert_eq!(frame["retryable"], false);
    let message = frame["message"].as_str().expect("message str");
    assert!(
        message.starts_with("技能未找到: nope。可用技能: "),
        "unexpected message: {message}"
    );
    assert!(message.contains("commit"), "catalog missing: {message}");
    // 失败不注入对话。
    assert!(engine.snapshot().is_empty());
}

#[tokio::test]
async fn blank_skill_args_reports_usage() {
    let (addr, session_id, engine) = spawn_bound().await;
    let mut ws = connect_bound(addr, &session_id).await;

    send_json(&mut ws, &slash_frame("skill", "   ")).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["type"], "error");
    assert_eq!(frame["code"], "COMMAND_ERROR");
    assert_eq!(frame["message"], "用法: /skill <技能名称> [参数]");
    assert!(engine.snapshot().is_empty());
}

// ── 场景 5：其余 slash 命令改由命令注册表本地执行（Batch 3） ───────────────
//
// 此前 `/skill` 之外的 slash 命令原样转发 `EngineHook`，而引擎侧并无
// `SlashCommand` 消费分支——等同静默丢弃。Batch 3 起交
// `CommandRegistry`（旧 `handleSlashCommand` 的语义），故断言改为「本地
// 执行 + 不再转发引擎」。

/// 已注册命令本地执行：`command_result` 后追推 `session_list_updated`。
#[tokio::test]
async fn registered_slash_command_executes_locally() {
    let (addr, session_id, engine) = spawn_bound().await;
    let mut ws = connect_bound(addr, &session_id).await;

    send_json(&mut ws, &slash_frame("status", "")).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["type"], "command_result", "unexpected frame: {frame}");
    assert_eq!(frame["command"], "status");
    assert_eq!(frame["resultType"], "text");
    let output = frame["output"].as_str().expect("output");
    assert!(
        output.starts_with("## 系统状态\n\n- 会话 ID: "),
        "unexpected output: {output}"
    );
    // 旧 L1428-1431：命令可能改动会话列表，成功后追推刷新信号。
    let follow = next_json(&mut ws).await;
    assert_eq!(follow["type"], "session_list_updated");
    assert!(engine.snapshot().is_empty());
}

/// `/plan` changes only the bound session's panel, including when permissions
/// already use PLAN. The command never starts an engine run or changes mode.
#[tokio::test]
async fn plan_panel_events_are_session_scoped_and_do_not_change_permissions() {
    let (addr, session_id, engine, db) = spawn_bound_with_db().await;
    let first = db.get_session(&session_id).await.unwrap().unwrap();
    let other = db
        .create_session(&first.model, &first.working_dir)
        .await
        .unwrap();
    let mut ws = connect_bound(addr, &session_id).await;
    let mut other_ws = connect_bound(addr, &other.id).await;
    for (args, mode, open, name) in [
        (
            "on Release plan",
            "ACCEPT_EDITS",
            true,
            Some("Release plan"),
        ),
        ("off", "PLAN", false, None),
        ("", "PLAN", true, Some("New Plan")),
        ("onboarding", "DEFAULT", true, Some("onboarding")),
    ] {
        db.set_session_permission_mode(session_id.clone(), mode.into())
            .await
            .unwrap();
        send_json(&mut ws, &slash_frame("plan", args)).await;
        let panel = next_json(&mut ws).await;
        assert_eq!(panel["type"], "plan_update");
        assert_eq!(panel["_sessionId"], session_id);
        assert_eq!(panel["isPlanMode"], open);
        assert_eq!(
            panel.get("planName").and_then(serde_json::Value::as_str),
            name
        );
        assert_eq!(
            panel
                .get("planOverview")
                .and_then(serde_json::Value::as_str),
            open.then_some("")
        );
        let result = next_json(&mut ws).await;
        assert_eq!(result["type"], "command_result");
        assert!(
            result["output"]
                .as_str()
                .unwrap()
                .contains("Session permissions are unchanged.")
        );
        assert_eq!(next_json(&mut ws).await["type"], "session_list_updated");
        assert_eq!(
            db.session_permission_mode(&session_id).unwrap().as_deref(),
            Some(mode)
        );
    }
    // Binding the second session must not receive the first session's panel.
    while let Ok(Some(frame)) =
        tokio::time::timeout(Duration::from_millis(50), other_ws.next()).await
    {
        if let Message::Text(text) = frame.unwrap() {
            let event: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();
            assert_eq!(event["type"], "session_list_updated");
        }
    }
    assert!(engine.snapshot().is_empty());
}

/// 未注册命令 → `COMMAND_NOT_FOUND`（含模糊建议），同样不转发引擎。
#[tokio::test]
async fn unregistered_slash_command_reports_not_found() {
    let (addr, session_id, engine) = spawn_bound().await;
    let mut ws = connect_bound(addr, &session_id).await;

    send_json(&mut ws, &slash_frame("xyzzy", "")).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["type"], "error");
    assert_eq!(frame["code"], "COMMAND_NOT_FOUND");
    let message = frame["message"].as_str().expect("message");
    assert!(
        message.starts_with("Unknown command: /xyzzy."),
        "unexpected message: {message}"
    );
    assert!(engine.snapshot().is_empty());
}

// ── 场景 6：注册表内的 PROMPT 命令（Batch 8A）─────────────────────────────
//
// `/git-review`、`/init`、`/retry` 是本域首批 `PROMPT` 命令，旧
// `handleSlashCommand` L1361-1403 的分支要求：先推
// `command_result{resultType:"prompt"}`（正文不下行），再把提示词按用户文本注入
// 引擎，最后照旧追推 `session_list_updated`（旧 sendSessionListUpdated 在 switch
// 之外，PROMPT 分支同样执行）。

/// `/retry` → prompt 通知 + 最后一条用户消息重新注入引擎。
#[tokio::test]
async fn registered_prompt_command_pushes_notice_and_injects_prompt() {
    let (addr, session_id, engine, db) = spawn_bound_with_db().await;
    db.append_message(
        &session_id,
        zk_db::NewMessage {
            meta: None,
            role: zk_db::MessageRole::User,
            content: vec![zk_db::StoredBlock::Text {
                text: "再跑一次构建".to_owned(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .expect("seed user message");
    let mut ws = connect_bound(addr, &session_id).await;

    send_json(&mut ws, &slash_frame("retry", "")).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["type"], "command_result", "unexpected frame: {frame}");
    assert_eq!(frame["command"], "retry");
    assert_eq!(frame["resultType"], "prompt");
    // 与 `/skill` 一致：完整提示词不下行。
    assert!(
        frame.get("output").is_none(),
        "prompt notice carries output"
    );
    assert!(frame.get("data").is_none(), "prompt notice carries data");

    let follow = next_json(&mut ws).await;
    assert_eq!(follow["type"], "session_list_updated");

    let seen = wait_for_engine(&engine, 1).await;
    match &seen[0] {
        ClientMessage::UserMessage {
            text,
            attachments,
            references,
        } => {
            assert_eq!(text, "再跑一次构建");
            assert!(attachments.is_none());
            assert!(references.is_none());
        }
        other => panic!("expected user_message injection, got {}", other.kind()),
    }
}

/// PROMPT 命令失败（会话无消息）→ `COMMAND_ERROR`，不推 prompt 通知、不注入。
#[tokio::test]
async fn failing_prompt_command_reports_command_error_without_injection() {
    let (addr, session_id, engine) = spawn_bound().await;
    let mut ws = connect_bound(addr, &session_id).await;

    send_json(&mut ws, &slash_frame("retry", "")).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["type"], "error", "unexpected frame: {frame}");
    assert_eq!(frame["code"], "COMMAND_ERROR");
    assert_eq!(frame["message"], "当前会话无消息，无法重试");
    assert_eq!(frame["retryable"], false);
    assert!(engine.snapshot().is_empty());
}

/// Real WS aliases resolve only against the bound session, including the legacy
/// fix alias and shared global-disable preference after a project hot reload.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn project_aliases_do_not_cross_ws_sessions_and_global_disable_applies_to_both() {
    let root = std::env::temp_dir().join(format!("zk-skill-ws-{}", uuid::Uuid::new_v4()));
    for project in ["a", "b"] {
        let skills = root.join(project).join(".zkcode/skills");
        std::fs::create_dir_all(&skills).unwrap();
        std::fs::write(skills.join("fix.md"), format!("{project} project fix")).unwrap();
    }
    std::fs::write(
        root.join("a/.zkcode/skills/private.md"),
        "---\nname: my-private-alias\n---\nA private skill",
    )
    .unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    let state = AppState::new_with_ws(
        zk_db::Db::open_in_memory().unwrap(),
        Config::test_config(),
        WsConfig::fast_for_tests(),
    );
    let engine = Arc::new(RecordingEngine::default());
    state.hub.set_engine(engine.clone());
    let a = state
        .db
        .create_session("m", root.join("a").to_str().unwrap())
        .await
        .unwrap();
    let b = state
        .db
        .create_session("m", root.join("b").to_str().unwrap())
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = build_router(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let mut wa = connect_bound(addr, &a.id).await;
    let mut wb = connect_bound(addr, &b.id).await;
    for (index, ws, session, expected) in [
        (0, &mut wa, &a.id, "a project fix"),
        (1, &mut wb, &b.id, "b project fix"),
    ] {
        send_json(ws, &slash_frame("fix", "")).await;
        let frame = next_json(ws).await;
        assert_eq!(frame["resultType"], "prompt");
        assert_eq!(frame["_sessionId"], *session);
        let seen = wait_for_engine(&engine, index + 1).await;
        let ClientMessage::UserMessage { text, .. } = &seen[index] else {
            panic!("skill prompt expected")
        };
        assert_eq!(text, expected);
    }
    send_json(&mut wb, &slash_frame("my-private-alias", "")).await;
    assert_eq!(next_json(&mut wb).await["code"], "COMMAND_NOT_FOUND");
    assert_eq!(engine.snapshot().len(), 2);
    send_json(&mut wa, &slash_frame("my-private-alias", "")).await;
    assert_eq!(next_json(&mut wa).await["resultType"], "prompt");
    let seen = wait_for_engine(&engine, 3).await;
    let ClientMessage::UserMessage { text, .. } = &seen[2] else {
        panic!("skill prompt expected")
    };
    assert_eq!(text, "A private skill");
    state
        .skill_catalog
        .view(Some(&a.id), None)
        .await
        .unwrap()
        .set_enabled("fix", false)
        .await
        .unwrap();
    for ws in [&mut wa, &mut wb] {
        send_json(ws, &slash_frame("fix", "")).await;
        assert_eq!(next_json(ws).await["code"], "COMMAND_ERROR");
    }
    assert_eq!(
        engine.snapshot().len(),
        3,
        "disabled aliases never invoke the engine"
    );
    drop((wa, wb));
    server.abort();
    let _ = server.await;
    std::fs::remove_dir_all(root).unwrap();
}
