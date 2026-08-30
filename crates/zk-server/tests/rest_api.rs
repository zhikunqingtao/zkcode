//! REST 会话域集成测试——`tower::ServiceExt::oneshot` 直打 Router（内存库）。
//!
//! 覆盖：8 端点全生命周期 + `GET /api/health`（`system_api.rs`）；响应形状对
//! `docs/baseline/samples/` 逐键 diff（`assert_same_shape`）；samples 为空
//! 列表的部分（messages 元素、compact 有值路径、markdown export）按旧仓库
//! 源码语义断言（`Message.java` / `ContentBlock.java` / `SessionController`）。

mod common;

use axum::http::{StatusCode, header};
use serde_json::{Value, json};
use zk_db::model::{MessageRole, NewMessage, StoredBlock};

use common::{
    app_with_db, assert_same_shape, call, json_body, local_delete, local_get, local_post,
    preflight, remote_get, sample,
};

/// 新建会话并返回其 id。
async fn create_session(router: &mut axum::Router) -> String {
    let (status, _, body) = call(router, local_post("/api/sessions", None)).await;
    assert_eq!(status, StatusCode::CREATED);
    json_body(&body)["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned()
}

/// 种子消息（`user` 文本 + `tool_use` / `assistant` 无 `stopReason` / `system`）。
async fn seed_messages(db: &zk_db::Db, session_id: &str) {
    db.append_message(
        session_id,
        NewMessage {
            role: MessageRole::User,
            content: vec![
                StoredBlock::Text {
                    text: "帮我看下这个报错".into(),
                },
                StoredBlock::ToolUse {
                    id: "toolu_1".into(),
                    name: "Read".into(),
                    input: json!({"path": "/tmp/a.rs"}),
                },
            ],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .expect("seed user");
    db.append_message(
        session_id,
        NewMessage {
            role: MessageRole::Assistant,
            content: vec![StoredBlock::Text {
                text: "我来看看".into(),
            }],
            // 库内 null → REST 出口 "end_turn" 兜底。
            stop_reason: None,
            input_tokens: 12,
            output_tokens: 34,
        },
    )
    .await
    .expect("seed assistant");
    db.append_message(
        session_id,
        NewMessage {
            role: MessageRole::System,
            content: vec![StoredBlock::Text {
                text: "系统提示".into(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .expect("seed system");
}

/// 8 端点全生命周期：每步响应形状对样例逐键 diff（一链贯通，不按行数拆断）。
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn lifecycle_matches_baseline_samples() {
    let (mut router, _db) = app_with_db();

    // POST /api/sessions → 201（样例 POST_api-sessions.json）。
    let (status, headers, body) = call(&mut router, local_post("/api/sessions", None)).await;
    assert_eq!(status, StatusCode::CREATED);
    let created = json_body(&body);
    assert_same_shape(&sample("POST_api-sessions.json"), &created, "create");
    // 观测中间件：响应携带 request-id。
    assert!(headers.get("x-request-id").is_some(), "x-request-id header");
    let session_id = created["sessionId"].as_str().expect("sessionId").to_owned();
    assert_eq!(created["model"], "qwen3.8-max");
    // 新建会话默认完全访问权限（登记入 PermissionModeRegistry 后回传生效值）。
    assert_eq!(created["permissionMode"], "AUTO_APPROVE");
    assert_eq!(created["webSocketUrl"], format!("/ws/session/{session_id}"));

    // 第二个会话：让列表进入翻页路径（hasMore/nextCursor 与样例同为有值）。
    // 间隔 2ms 避开同毫秒 updated_at——游标锚点 `updated_at <` 语义会跳过
    // 同时间戳行（与旧系统一致的已知隐患，Phase 2 待办）。
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    let (status, _, _) = call(&mut router, local_post("/api/sessions", None)).await;
    assert_eq!(status, StatusCode::CREATED);

    // GET /api/sessions → 200（样例 GET_api-sessions.json；数组首元素逐键）。
    let (status, _, body) = call(&mut router, local_get("/api/sessions?limit=1")).await;
    assert_eq!(status, StatusCode::OK);
    let listed = json_body(&body);
    assert_same_shape(&sample("GET_api-sessions.json"), &listed, "list");
    assert_eq!(listed["sessions"].as_array().expect("array").len(), 1);
    assert_eq!(listed["hasMore"], true);
    let cursor = listed["nextCursor"]
        .as_str()
        .expect("nextCursor")
        .to_owned();
    // 翻第二页：尾页无 nextCursor、含另一会话（游标锚点生效）。
    let (status, _, body) = call(
        &mut router,
        local_get(&format!("/api/sessions?limit=1&cursor={cursor}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let page2 = json_body(&body);
    assert_eq!(page2["hasMore"], false);
    assert!(page2.get("nextCursor").is_none());
    assert_eq!(page2["sessions"].as_array().expect("array").len(), 1);

    // GET /api/sessions/{id} → 200（样例 GET_api-sessions-id.json；title/summary
    // null 被剥离——NON_NULL 语义）。
    let (status, _, body) = call(
        &mut router,
        local_get(&format!("/api/sessions/{session_id}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let detail = json_body(&body);
    assert_same_shape(&sample("GET_api-sessions-id.json"), &detail, "detail");
    assert!(detail.get("title").is_none(), "null title stripped");
    assert!(detail.get("summary").is_none(), "null summary stripped");
    assert_eq!(
        detail["workingDir"],
        Value::String(
            std::env::current_dir()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        )
    );

    // GET /api/sessions/{id}/messages → 200（样例 GET_api-sessions-id-messages.json）。
    let (status, _, body) = call(
        &mut router,
        local_get(&format!("/api/sessions/{session_id}/messages")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_same_shape(
        &sample("GET_api-sessions-id-messages.json"),
        &json_body(&body),
        "messages",
    );

    // POST /api/sessions/{id}/resume → 200（样例 POST_api-sessions-id-resume.json）。
    let (status, _, body) = call(
        &mut router,
        local_post(&format!("/api/sessions/{session_id}/resume"), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_same_shape(
        &sample("POST_api-sessions-id-resume.json"),
        &json_body(&body),
        "resume",
    );

    // POST /api/sessions/{id}/compact → 200（样例 POST_api-sessions-id-compact.json）。
    let (status, _, body) = call(
        &mut router,
        local_post(&format!("/api/sessions/{session_id}/compact"), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_same_shape(
        &sample("POST_api-sessions-id-compact.json"),
        &json_body(&body),
        "compact",
    );

    // POST /api/sessions/{id}/export?format=json → 200（样例 export；独立序列化：
    // null 保留 + epoch 浮点秒 + Content-Disposition）。
    let (status, headers, body) = call(
        &mut router,
        local_post(&format!("/api/sessions/{session_id}/export"), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let exported = json_body(&body);
    assert_same_shape(
        &sample("POST_api-sessions-id-export-json.json"),
        &exported,
        "export",
    );
    assert!(exported["title"].is_null(), "export keeps null title");
    assert!(
        exported["createdAt"].is_number(),
        "export time is epoch seconds"
    );
    assert_eq!(
        headers
            .get(header::CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok()),
        Some(format!("attachment; filename=\"session-{session_id}.json\"").as_str())
    );

    // DELETE /api/sessions/{id} → 200（样例 DELETE_api-sessions-id.json；幂等）。
    let (status, _, body) = call(
        &mut router,
        local_delete(&format!("/api/sessions/{session_id}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_same_shape(
        &sample("DELETE_api-sessions-id.json"),
        &json_body(&body),
        "delete",
    );
    // 再删一次仍 success（幂等语义）。
    let (status, _, body) = call(
        &mut router,
        local_delete(&format!("/api/sessions/{session_id}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["success"], true);

    // 删除后详情 → 404 扁平错误响应（{code,message,requestId}）。
    let (status, _, body) = call(
        &mut router,
        local_get(&format!("/api/sessions/{session_id}")),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let error = json_body(&body);
    let envelope = error.as_object().expect("error response");
    let mut keys: Vec<&str> = envelope.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["code", "message", "requestId"]);
    assert_eq!(envelope["code"], "SESSION_NOT_FOUND");
}

/// 错误路径：workingDirectory 拒绝 / 非法体 / 非法 limit / 不存在会话。
#[tokio::test]
async fn error_paths_and_envelope_shape() {
    let (mut router, _db) = app_with_db();

    // workingDirectory → 400 SESSION_WORKING_DIRECTORY_UNSUPPORTED。
    let (status, _, body) = call(
        &mut router,
        local_post(
            "/api/sessions",
            Some(json!({"workingDirectory": "/somewhere"}).to_string()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = json_body(&body)
        .as_object()
        .expect("error response")
        .clone();
    assert_eq!(error["code"], "SESSION_WORKING_DIRECTORY_UNSUPPORTED");
    assert_eq!(
        error["message"],
        "Use projectId instead of workingDirectory"
    );

    // 非法 JSON 体 → 400 INVALID_REQUEST_BODY。
    let (status, _, body) = call(
        &mut router,
        local_post("/api/sessions", Some("{not json".to_owned())),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&body)["code"], "INVALID_REQUEST_BODY");

    // limit 非数字 → 400 INVALID_REQUEST。
    let (status, _, body) = call(&mut router, local_get("/api/sessions?limit=abc")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&body)["code"], "INVALID_REQUEST");

    // 不存在会话：detail / messages / resume / export / compact → 404。
    let missing = "00000000-0000-4000-8000-000000000000";
    for (label, request) in [
        ("detail", local_get(&format!("/api/sessions/{missing}"))),
        (
            "messages",
            local_get(&format!("/api/sessions/{missing}/messages")),
        ),
        (
            "resume",
            local_post(&format!("/api/sessions/{missing}/resume"), None),
        ),
        (
            "export",
            local_post(&format!("/api/sessions/{missing}/export"), None),
        ),
        (
            "compact",
            local_post(&format!("/api/sessions/{missing}/compact"), None),
        ),
    ] {
        let (status, _, body) = call(&mut router, request).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{label} should 404");
        assert_eq!(json_body(&body)["code"], "SESSION_NOT_FOUND", "{label}");
    }
}

/// 有消息会话的 REST 线上形状（samples 为空列表，按 `Message.java` /
/// `ContentBlock.java` 源码断言）+ P0 游标分页。
#[tokio::test]
async fn message_shapes_and_pagination() {
    let (mut router, db) = app_with_db();
    let session_id = create_session(&mut router).await;
    seed_messages(&db, &session_id).await;

    let (status, _, body) = call(
        &mut router,
        local_get(&format!("/api/sessions/{session_id}/messages")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let page = json_body(&body);
    let messages = page["messages"].as_array().expect("messages array");
    assert_eq!(messages.len(), 3);

    // user：{type,uuid,timestamp,content}（toolUseResult 等占位 None 剥离）。
    let user = &messages[0];
    assert_eq!(user["type"], "user");
    let mut keys: Vec<&str> = user
        .as_object()
        .expect("obj")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["content", "timestamp", "type", "uuid"]);
    let blocks = user["content"].as_array().expect("blocks");
    assert_eq!(blocks[0]["type"], "text");
    assert_eq!(blocks[0]["text"], "帮我看下这个报错");
    // tool_use：REST 即存储蛇形 {type,id,name,input}。
    assert_eq!(blocks[1]["type"], "tool_use");
    let mut tu_keys: Vec<&str> = blocks[1]
        .as_object()
        .expect("obj")
        .keys()
        .map(String::as_str)
        .collect();
    tu_keys.sort_unstable();
    assert_eq!(tu_keys, vec!["id", "input", "name", "type"]);
    assert_eq!(blocks[1]["input"]["path"], "/tmp/a.rs");

    // assistant：stopReason null → "end_turn"；usage 四键恒出。
    let assistant = &messages[1];
    assert_eq!(assistant["type"], "assistant");
    assert_eq!(assistant["stopReason"], "end_turn");
    assert_eq!(assistant["usage"]["inputTokens"], 12);
    assert_eq!(assistant["usage"]["outputTokens"], 34);

    // system：content 为纯字符串。
    let system = &messages[2];
    assert_eq!(system["type"], "system");
    assert_eq!(system["content"], "系统提示");

    // P0 分页：limit=1 → hasMore + nextCursor；翻页取回 assistant。
    let (status, _, body) = call(
        &mut router,
        local_get(&format!("/api/sessions/{session_id}/messages?limit=1")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let first_page = json_body(&body);
    assert_eq!(first_page["hasMore"], true);
    let cursor = first_page["nextCursor"]
        .as_str()
        .expect("nextCursor")
        .to_owned();
    let (status, _, body) = call(
        &mut router,
        local_get(&format!(
            "/api/sessions/{session_id}/messages?limit=1&cursor={cursor}"
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let second_page = json_body(&body);
    assert_eq!(second_page["messages"][0]["type"], "assistant");

    // resume 带同形状历史。
    let (status, _, body) = call(
        &mut router,
        local_post(&format!("/api/sessions/{session_id}/resume"), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let resumed = json_body(&body);
    assert_eq!(resumed["messages"].as_array().expect("arr").len(), 3);
    assert_eq!(resumed["webSocketUrl"], format!("/ws/session/{session_id}"));

    // export 的 user 消息保留 toolUseResult:null（独立 mapper ALWAYS）。
    let (status, _, body) = call(
        &mut router,
        local_post(&format!("/api/sessions/{session_id}/export"), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let exported = json_body(&body);
    let export_user = &exported["messages"][0];
    assert!(
        export_user.get("toolUseResult").is_some(),
        "export keeps null"
    );
    assert!(export_user["toolUseResult"].is_null());
    assert!(export_user["timestamp"].is_number());
}

/// markdown export：text/plain + attachment 头 + 旧 `exportAsMarkdown` 骨架。
#[tokio::test]
async fn markdown_export_format() {
    let (mut router, db) = app_with_db();
    let session_id = create_session(&mut router).await;
    seed_messages(&db, &session_id).await;

    let (status, headers, body) = call(
        &mut router,
        local_post(
            &format!("/api/sessions/{session_id}/export?format=markdown"),
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/plain"))
    );
    assert_eq!(
        headers
            .get(header::CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok()),
        Some(format!("attachment; filename=\"session-{session_id}.md\"").as_str())
    );
    let markdown = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(markdown.starts_with(&format!("# Session: {session_id}\n\n")));
    assert!(markdown.contains("- **Messages**: 3\n"));
    assert!(markdown.contains("## User\n\n帮我看下这个报错\n\n"));
    assert!(markdown.contains("## Assistant\n\n我来看看\n\n"));
    assert!(markdown.contains("## System\n\n系统提示\n\n"));
}

/// compact 有值路径：tokensBefore > 0 且 summary 落库（详情可见）。
#[tokio::test]
async fn compact_persists_summary() {
    let (mut router, db) = app_with_db();
    let session_id = create_session(&mut router).await;
    // 5 轮 user/assistant，超过 MIN_MESSAGES_FOR_COMPACT 与保留轮次。
    for round in 0..5_u32 {
        db.append_message(
            &session_id,
            NewMessage {
                role: MessageRole::User,
                content: vec![StoredBlock::Text {
                    text: format!("user turn {round} with sufficiently long body text to compress"),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .expect("seed user");
        db.append_message(
            &session_id,
            NewMessage {
                role: MessageRole::Assistant,
                content: vec![StoredBlock::Text {
                    // 超过摘要摘录上限（500 字符），确保确定性压缩有净收益。
                    text: "assistant reply ".repeat(60),
                }],
                stop_reason: Some("end_turn".into()),
                input_tokens: 10,
                output_tokens: 20,
            },
        )
        .await
        .expect("seed assistant");
    }
    let (status, _, body) = call(
        &mut router,
        local_post(&format!("/api/sessions/{session_id}/compact"), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let compact = json_body(&body);
    assert_eq!(compact["success"], true);
    assert!(
        compact["tokensBefore"].as_i64().expect("i64") > 0,
        "tokensBefore positive: {compact}"
    );
    assert!(
        compact["tokensAfter"].as_i64().expect("i64")
            < compact["tokensBefore"].as_i64().expect("i64")
    );

    // summary 已落库：详情出现非 null summary（NON_NULL 下键存在）。
    let (status, _, body) = call(
        &mut router,
        local_get(&format!("/api/sessions/{session_id}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let detail = json_body(&body);
    let summary = detail["summary"].as_str().expect("summary persisted");
    assert!(summary.contains("[User] user turn 0"));
}

/// 准入守卫：公网对端 → 403 扁平错误响应（`ACCESS_DENIED`）。
#[tokio::test]
async fn non_loopback_denied() {
    let (mut router, _db) = app_with_db();
    let (status, headers, body) = call(&mut router, remote_get("/api/sessions")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let error = json_body(&body);
    assert_eq!(error["code"], "ACCESS_DENIED");
    let mut keys: Vec<&str> = error
        .as_object()
        .expect("envelope")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["code", "message", "requestId"]);
    assert_eq!(
        headers
            .get("x-request-id")
            .expect("request id header")
            .to_str()
            .expect("ASCII request id"),
        error["requestId"].as_str().expect("request id body")
    );
}

/// CORS 预检：白名单 Origin 放行并回显（5273 dev 直连场景）。
#[tokio::test]
async fn cors_preflight_for_dev_frontend() {
    let (mut router, _db) = app_with_db();
    let (status, headers, _body) = call(
        &mut router,
        preflight("/api/sessions", "http://localhost:5273"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some("http://localhost:5273")
    );
    assert!(
        headers
            .get(header::ACCESS_CONTROL_ALLOW_METHODS)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("POST"))
    );
}
