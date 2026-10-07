//! Batch 5 Step 6 集成测试——记忆域 5 端点（旧 `MemoryController`）与文件历史
//! 域 3 端点（旧 `FileHistoryController`）的端到端契约。
//!
//! 覆盖点：记忆 CRUD 往返与 `PUT` 的 upsert 语义、SQLite 唯一权威、
//! 快照按 `messageId` 分组的单元素数组形状、`rewind` 恒 200 与真实文件恢复、
//! `diff` 的必填参数守卫，以及两域同栈过 `access_guard`。

mod common;

use axum::http::{Method, StatusCode};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zk_db::MemoryTarget;
use zk_server::config::Config;
use zk_server::routes::build_router;
use zk_server::state::AppState;
use zk_tools::ToolContext;

use common::{
    app_with_db, call, json_body, local_delete, local_get, local_post, local_put,
    local_with_headers, remote_get,
};

/// 独占工作区（rewind 要真实写盘；`canonicalize` 化解 macOS 的
/// `/var`→`/private/var` 符号链接，否则触 workspace 边界校验）。
fn workspace(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("zk-hist-api-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("mkdir workspace");
    std::fs::canonicalize(&root).expect("canonicalize workspace")
}

/// 造一条最小合法记忆体（三个 `NOT NULL` 列齐备）。
fn memory_body(id: Option<&str>, title: &str) -> String {
    let id_field = id.map_or_else(String::new, |value| format!("\"id\":\"{value}\","));
    format!(
        "{{{id_field}\"category\":\"USER_PREFERENCE\",\"title\":\"{title}\",\
         \"content\":\"body of {title}\",\"keywords\":\"rust\"}}"
    )
}

/// `POST` → `GET` → `PUT` → `DELETE` 往返：201 携 id、列表降序、更新命中、
/// 204 后重复删除 404 空体。
#[tokio::test]
async fn memory_crud_round_trip() {
    let (mut router, _db) = app_with_db();

    let (status, _headers, body) = call(
        &mut router,
        local_post("/api/memory", Some(memory_body(None, "prefer rust"))),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let created = json_body(&body);
    assert_eq!(created["success"], true);
    let id = created["id"].as_str().expect("id string").to_owned();
    assert!(!id.is_empty());

    let (status, _headers, body) = call(&mut router, local_get("/api/memory")).await;
    assert_eq!(status, StatusCode::OK);
    let listed = json_body(&body);
    let entries = listed["entries"].as_array().expect("entries array");
    assert_eq!(entries.len(), 1);
    let mut keys: Vec<&str> = entries[0]
        .as_object()
        .expect("entry object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "category",
            "content",
            "createdAt",
            "id",
            "keywords",
            "projectPath",
            "scope",
            "source",
            "title",
            "updatedAt"
        ]
    );
    assert_eq!(entries[0]["id"], id.as_str());
    // 缺省作用域是当前项目；global 必须显式请求。
    assert_eq!(entries[0]["scope"], "project");
    assert!(entries[0]["projectPath"].as_str().is_some());
    assert_eq!(entries[0]["source"], "USER");

    // PUT 是逐条 upsert：命中已有 id 时改标题，不新增行。
    let (status, _headers, body) = call(
        &mut router,
        local_put(
            "/api/memory",
            Some(format!(
                "{{\"entries\":[{}]}}",
                memory_body(Some(&id), "prefer rust 2")
            )),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["success"], true);

    let (_status, _headers, body) = call(&mut router, local_get("/api/memory")).await;
    let entries = json_body(&body);
    let entries = entries["entries"].as_array().expect("entries").clone();
    assert_eq!(entries.len(), 1, "upsert must not duplicate rows");
    assert_eq!(entries[0]["title"], "prefer rust 2");

    let (status, _headers, body) =
        call(&mut router, local_delete(&format!("/api/memory/{id}"))).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty(), "204 must carry no body");

    // 重复删除 → 404 空体（非错误信封，旧 `notFound().build()`）。
    let (status, _headers, body) =
        call(&mut router, local_delete(&format!("/api/memory/{id}"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.is_empty(), "404 must carry no body");
}

/// `PUT` 未命中 id → 兜底 INSERT（旧 `updated == 0` 分支）。
#[tokio::test]
async fn memory_put_inserts_when_id_absent() {
    let (mut router, _db) = app_with_db();
    let (status, _headers, _body) = call(
        &mut router,
        local_put(
            "/api/memory",
            Some(format!(
                "{{\"entries\":[{}]}}",
                memory_body(Some("mem-ghost"), "brand new")
            )),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_status, _headers, body) = call(&mut router, local_get("/api/memory")).await;
    let listed = json_body(&body);
    let entries = listed["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["id"], "mem-ghost");
    assert_eq!(entries[0]["scope"], "project");
}

/// 差异留痕 1：`PUT {}` 视作 0 条更新 → 200（旧实现为 NPE → 500）。
#[tokio::test]
async fn memory_put_without_entries_succeeds() {
    let (mut router, _db) = app_with_db();
    let (status, _headers, body) =
        call(&mut router, local_put("/api/memory", Some("{}".to_owned()))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["success"], true);
}

/// 空体 / 非法 JSON → 400 `INVALID_REQUEST_BODY`（旧
/// `HttpMessageNotReadableException` 分支）。
#[tokio::test]
async fn memory_write_endpoints_reject_malformed_body() {
    let (mut router, _db) = app_with_db();
    for request in [
        local_post("/api/memory", None),
        local_post("/api/memory", Some("not json".to_owned())),
        local_put("/api/memory", None),
        local_put("/api/memory", Some("[".to_owned())),
    ] {
        let (status, _headers, body) = call(&mut router, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json_body(&body)["code"], "INVALID_REQUEST_BODY");
    }
}

/// `/all` 只返回所选 `SQLite` scope，不再暴露 `MEMORY.md` 双源。
#[tokio::test]
async fn memory_all_is_sqlite_only() {
    let (mut router, _db) = app_with_db();
    call(
        &mut router,
        local_post("/api/memory", Some(memory_body(Some("mem-a"), "alpha"))),
    )
    .await;

    let (status, _headers, body) = call(&mut router, local_get("/api/memory/all")).await;
    assert_eq!(status, StatusCode::OK);
    let all = json_body(&body);
    assert_eq!(
        all.as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["entries"]
    );
    let entries = all["entries"].as_array().expect("entries array");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["id"], "mem-a");
}

/// Production composition proof: the `Memory` tool and HTTP API share the
/// same real `SQLite` handle, default to the project, and hide global rows until
/// the caller explicitly selects `global`.
#[tokio::test]
async fn memory_tool_and_api_share_scoped_sqlite_authority() {
    let project = workspace("memory-sqlite-authority");
    let mut config = Config::test_config();
    config.workspace_default_root = project.to_string_lossy().into_owned();
    let db = zk_db::Db::open_in_memory().expect("sqlite");
    let state = AppState::new(db.clone(), config);
    let memory = state.tools().get("Memory").expect("Memory tool registered");
    let (progress, _progress_rx) = mpsc::unbounded_channel();
    let ctx = ToolContext::new(CancellationToken::new(), progress).with_working_dir(&project);

    let written = memory
        .execute(
            serde_json::json!({"action":"write", "content":"use cargo nextest"}),
            ctx.clone(),
        )
        .await;
    assert!(!written.is_error, "{}", written.content);
    let project_rows = db
        .list_memories(MemoryTarget::project(project.to_string_lossy()).unwrap())
        .await
        .expect("project rows");
    assert_eq!(project_rows.len(), 1);
    assert_eq!(project_rows[0].source, "TOOL");

    let mut router = build_router(state);
    let (status, _, _) = call(
        &mut router,
        local_post(
            "/api/memory",
            Some(
                serde_json::json!({
                    "id": "global-only",
                    "category": "USER_PREFERENCE",
                    "title": "global",
                    "content": "global preference",
                    "scope": "global"
                })
                .to_string(),
            ),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        db.list_memories(MemoryTarget::global())
            .await
            .expect("global rows")
            .len(),
        1
    );

    let (status, _, body) = call(&mut router, local_get("/api/memory")).await;
    assert_eq!(status, StatusCode::OK);
    let default_rows = json_body(&body);
    assert_eq!(default_rows["entries"].as_array().unwrap().len(), 1);
    assert_eq!(default_rows["entries"][0]["source"], "TOOL");

    let (status, _, body) = call(&mut router, local_get("/api/memory?scope=global")).await;
    assert_eq!(status, StatusCode::OK);
    let global_rows = json_body(&body);
    assert_eq!(global_rows["entries"].as_array().unwrap().len(), 1);
    assert_eq!(global_rows["entries"][0]["id"], "global-only");

    let read_back = memory
        .execute(serde_json::json!({"action":"read"}), ctx)
        .await;
    assert!(!read_back.is_error);
    assert!(read_back.content.contains("use cargo nextest"));
    assert!(!read_back.content.contains("global preference"));
    cleanup(&project);
}

/// 快照按 `messageId` 分组，每键恒单元素数组；`fileCount` 与 `trackedFiles`
/// 长度一致，`timestamp` 取组内首条。
#[tokio::test]
async fn history_snapshots_group_by_message_id() {
    let (mut router, db) = app_with_db();
    let session = db
        .create_session(
            "claude-sonnet-4",
            std::fs::canonicalize("/tmp").unwrap().to_str().unwrap(),
        )
        .await
        .expect("create session");
    for (message_id, file_path) in [
        ("m-1", "/tmp/a.txt"),
        ("m-1", "/tmp/b.txt"),
        ("m-2", "/tmp/c.txt"),
    ] {
        db.insert_file_snapshot(&session.id, Some(message_id), file_path, "old", "write")
            .await
            .expect("insert snapshot");
    }

    let (status, _headers, body) = call(
        &mut router,
        local_with_headers(
            &format!("/api/sessions/{}/history/snapshots", session.id),
            Method::GET,
            None,
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let grouped = json_body(&body);
    let grouped = grouped.as_object().expect("grouped object");
    assert_eq!(grouped.len(), 2);

    let first = grouped["m-1"].as_array().expect("array");
    assert_eq!(first.len(), 1, "each key carries exactly one summary");
    let summary = &first[0];
    let mut keys: Vec<&str> = summary
        .as_object()
        .expect("summary object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["fileCount", "messageId", "timestamp", "trackedFiles"]
    );
    assert_eq!(summary["messageId"], "m-1");
    assert_eq!(summary["fileCount"], 2);
    assert_eq!(
        summary["trackedFiles"]
            .as_array()
            .expect("tracked files")
            .len(),
        2
    );
    assert!(
        summary["timestamp"]
            .as_str()
            .expect("timestamp string")
            .ends_with('Z')
    );
    assert_eq!(grouped["m-2"][0]["fileCount"], 1);
}

/// `diff` 三类计数：仅 `to` 侧（added）/ 两侧内容不同（modified）/ 仅 `from`
/// 侧（deleted）。
#[tokio::test]
async fn history_diff_classifies_added_modified_deleted() {
    let (mut router, db) = app_with_db();
    let session = db
        .create_session(
            "claude-sonnet-4",
            std::fs::canonicalize("/tmp").unwrap().to_str().unwrap(),
        )
        .await
        .expect("create session");
    for (message_id, file_path, content) in [
        ("from", "/tmp/same.txt", "v1"),
        ("from", "/tmp/gone.txt", "v1"),
        ("to", "/tmp/same.txt", "v2"),
        ("to", "/tmp/fresh.txt", "v1"),
    ] {
        db.insert_file_snapshot(&session.id, Some(message_id), file_path, content, "write")
            .await
            .expect("insert snapshot");
    }

    let (status, _headers, body) = call(
        &mut router,
        local_with_headers(
            &format!(
                "/api/sessions/{}/history/diff?fromMessageId=from&toMessageId=to",
                session.id
            ),
            Method::GET,
            None,
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let diff = json_body(&body);
    let mut keys: Vec<&str> = diff
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "changedFiles",
            "filesAdded",
            "filesDeleted",
            "filesModified"
        ]
    );
    assert_eq!(diff["filesAdded"], 1);
    assert_eq!(diff["filesModified"], 1);
    assert_eq!(diff["filesDeleted"], 1);
    assert_eq!(
        diff["changedFiles"]
            .as_array()
            .expect("changed files")
            .len(),
        3
    );
}

/// 缺必填 `@RequestParam` → 400 `MISSING_PARAMETER`。
#[tokio::test]
async fn history_diff_requires_both_message_ids() {
    let (mut router, db) = app_with_db();
    let session = db
        .create_session(
            "claude-sonnet-4",
            std::fs::canonicalize("/tmp").unwrap().to_str().unwrap(),
        )
        .await
        .expect("create session");
    for query in ["", "?fromMessageId=a", "?toMessageId=b"] {
        let (status, _headers, body) = call(
            &mut router,
            local_get(&format!("/api/sessions/{}/history/diff{query}", session.id)),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "query: {query}");
        assert_eq!(json_body(&body)["code"], "MISSING_PARAMETER");
    }
}

/// `rewind` 端到端：磁盘文件被改写后经端点恢复为快照内容，并对当前状态再存
/// 一份二次快照（使回退本身可再回退）。
#[tokio::test]
async fn history_rewind_restores_file_content() {
    let root = workspace("rewind");
    let target = root.join("code.txt");
    std::fs::write(&target, "current").expect("seed current content");
    let target_path = target.to_string_lossy().to_string();

    let (mut router, db) = app_with_db();
    let session = db
        .create_session("claude-sonnet-4", &root.to_string_lossy())
        .await
        .expect("create session");
    db.insert_file_snapshot(
        &session.id,
        Some("turn-1"),
        &target_path,
        "original",
        "write",
    )
    .await
    .expect("insert snapshot");

    let (status, _, preview) = call(
        &mut router,
        local_with_headers(
            &format!("/api/sessions/{}/history/rewind/preview", session.id),
            Method::POST,
            Some(serde_json::json!({"messageId":"turn-1","filePaths":[target_path]}).to_string()),
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", json_body(&preview));
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "current");
    let (status, _headers, body) = call(&mut router, local_with_headers(
        &format!("/api/sessions/{}/history/rewind",session.id), Method::POST,
        Some(serde_json::json!({"previewToken":json_body(&preview)["previewToken"],"confirmed":true}).to_string()),
        &[("X-Session-Id",&session.id)],
    )).await;
    assert_eq!(status, StatusCode::OK);
    let result = json_body(&body);
    let mut keys: Vec<&str> = result
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["errors", "restoredFiles", "skippedFiles", "success"]
    );
    assert_eq!(result["success"], true, "body: {result}");
    assert_eq!(result["restoredFiles"][0], target_path.as_str());
    assert!(result["errors"].as_array().expect("errors").is_empty());
    assert_eq!(
        std::fs::read_to_string(&target).expect("read restored"),
        "original"
    );

    // 二次快照：回退前的 `current` 也进了库（`operation = rewind`）。
    let snapshots = db
        .list_file_snapshots(&session.id)
        .await
        .expect("list snapshots");
    assert!(
        snapshots
            .iter()
            .any(|record| record.content == "current" && record.operation == "rewind"),
        "missing pre-rewind snapshot: {snapshots:?}"
    );

    cleanup(&root);
}

/// Unreviewed legacy requests are refused before any mutation; missing session
/// ownership remains a normal 404 when a valid-shaped confirmation is supplied.
#[tokio::test]
async fn history_rewind_requires_reviewed_confirmation_and_session() {
    let (mut router, _) = app_with_db();
    let (status, _, body) = call(
        &mut router,
        local_post(
            "/api/sessions/ghost-session/history/rewind",
            Some("{\"messageId\":\"turn-1\"}".into()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&body)["code"], "REWIND_PREVIEW_REQUIRED");
    let (status, _, body) = call(
        &mut router,
        local_with_headers(
            "/api/sessions/ghost-session/history/rewind",
            Method::POST,
            Some("{\"previewToken\":\"unknown\",\"confirmed\":true}".into()),
            &[("X-Session-Id", "ghost-session")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_body(&body)["code"], "SESSION_NOT_FOUND");
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "One preview token lifecycle verifies preflight, a concurrent edit, foreign confirmation, selected restore and replay rejection"
)]
async fn history_rewind_preflight_rejects_changes_without_partial_writes_then_restores_only_selected_files()
 {
    let root = workspace("rewind-cas");
    let first = root.join("first.txt");
    let second = root.join("second.txt");
    std::fs::write(&first, "new first").unwrap();
    std::fs::write(&second, "new second").unwrap();
    let (mut router, db) = app_with_db();
    let session = db
        .create_session("fixture", root.to_str().unwrap())
        .await
        .unwrap();
    for (file, old) in [(&first, "old first"), (&second, "old second")] {
        db.insert_file_snapshot(
            &session.id,
            Some("checkpoint"),
            file.to_str().unwrap(),
            old,
            "edit",
        )
        .await
        .unwrap();
    }
    let preview_path = format!("/api/sessions/{}/history/rewind/preview", session.id);
    let confirm_path = format!("/api/sessions/{}/history/rewind", session.id);
    let (status, _, body) = call(
        &mut router,
        local_with_headers(
            &preview_path,
            Method::POST,
            Some(
                serde_json::json!({"messageId":"checkpoint","filePaths":[first,second]})
                    .to_string(),
            ),
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    let token = json_body(&body)["previewToken"].clone();
    std::fs::write(&second, "external concurrent change").unwrap();
    let (status, _, body) = call(
        &mut router,
        local_with_headers(
            &confirm_path,
            Method::POST,
            Some(serde_json::json!({"previewToken":token,"confirmed":true}).to_string()),
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["errors"][0], "REWIND_FILE_CHANGED");
    assert_eq!(std::fs::read_to_string(&first).unwrap(), "new first");
    assert_eq!(
        std::fs::read_to_string(&second).unwrap(),
        "external concurrent change"
    );
    let (status, _, body) = call(
        &mut router,
        local_with_headers(
            &preview_path,
            Method::POST,
            Some(serde_json::json!({"messageId":"checkpoint","filePaths":[first]}).to_string()),
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body =
        serde_json::json!({"previewToken":json_body(&body)["previewToken"],"confirmed":true})
            .to_string();
    let (status, _, result) = call(
        &mut router,
        local_with_headers(
            &confirm_path,
            Method::POST,
            Some(body.clone()),
            &[("X-Session-Id", "foreign")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{}", json_body(&result));
    let (_, _, result) = call(
        &mut router,
        local_with_headers(
            &confirm_path,
            Method::POST,
            Some(body.clone()),
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(
        json_body(&result)["success"],
        true,
        "{}",
        json_body(&result)
    );
    assert_eq!(std::fs::read_to_string(&first).unwrap(), "old first");
    assert_eq!(
        std::fs::read_to_string(&second).unwrap(),
        "external concurrent change"
    );
    let (_, _, result) = call(
        &mut router,
        local_with_headers(
            &confirm_path,
            Method::POST,
            Some(body),
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(json_body(&result)["errors"][0], "REWIND_PREVIEW_EXPIRED");
    cleanup(&root);
}

/// `rewind` 空体 / 非法 JSON → 400 `INVALID_REQUEST_BODY`。
#[tokio::test]
async fn history_rewind_rejects_malformed_body() {
    let (mut router, _db) = app_with_db();
    for body in [None, Some("nope".to_owned())] {
        let (status, _headers, payload) = call(
            &mut router,
            local_post("/api/sessions/s1/history/rewind", body),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json_body(&payload)["code"], "INVALID_REQUEST_BODY");
    }
}

/// 两域同栈过 `access_guard`（公网对端 403）。
#[tokio::test]
async fn memory_and_history_reject_remote_peer() {
    let (mut router, _db) = app_with_db();
    for uri in [
        "/api/memory",
        "/api/memory/all",
        "/api/sessions/s1/history/snapshots",
    ] {
        let (status, _headers, body) = call(&mut router, remote_get(uri)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "uri {uri}");
        assert_eq!(json_body(&body)["code"], "ACCESS_DENIED");
    }
    // DELETE 同栈（`local_with_headers` 走 loopback，此处只验方法可达性）。
    let (status, _headers, _body) = call(
        &mut router,
        local_with_headers("/api/memory/absent", Method::DELETE, None, &[]),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// `OpenAPI` 文档收录记忆 3 路径与文件历史 3 路径（与 `api::openapi` 单测的
/// 62 条计数互锁）。
#[tokio::test]
async fn openapi_document_lists_memory_and_history_paths() {
    let (mut router, _db) = app_with_db();
    let (status, _headers, body) = call(&mut router, local_get("/api/openapi.json")).await;
    assert_eq!(status, StatusCode::OK);
    let document = json_body(&body);
    let paths = document["paths"].as_object().expect("paths object");
    for path in [
        "/api/memory",
        "/api/memory/all",
        "/api/memory/{memoryId}",
        "/api/sessions/{sessionId}/history/snapshots",
        "/api/sessions/{sessionId}/history/rewind",
        "/api/sessions/{sessionId}/history/diff",
    ] {
        assert!(paths.contains_key(path), "missing {path}");
    }
    // `/api/memory` 一条路径承载 GET/PUT/POST 三方法。
    let memory_path = &paths["/api/memory"];
    for method in ["get", "put", "post"] {
        assert!(memory_path.get(method).is_some(), "missing {method}");
    }
}

/// 清理工作区（保留开关同 `file_snapshot` 测试的惯例）。
fn cleanup(root: &Path) {
    if std::env::var_os("ZK_KEEP_SNAPSHOT_DB").is_none() {
        let _ = std::fs::remove_dir_all(root);
    } else {
        eprintln!("kept history fixture at {}", root.display());
    }
}

#[tokio::test]
async fn history_read_routes_require_the_exact_session_identity() {
    let (mut router, db) = app_with_db();
    let root = std::fs::canonicalize("/tmp").unwrap();
    let session = db
        .create_session("fixture", root.to_str().unwrap())
        .await
        .unwrap();
    db.insert_file_snapshot(
        &session.id,
        Some("m1"),
        "/tmp/private-source",
        "private snapshot body",
        "edit",
    )
    .await
    .unwrap();
    for suffix in ["snapshots", "diff?fromMessageId=m1&toMessageId=m1"] {
        let uri = format!("/api/sessions/{}/history/{suffix}", session.id);
        for headers in [vec![], vec![("X-Session-Id", "foreign")]] {
            let (status, _, body) = call(
                &mut router,
                local_with_headers(&uri, Method::GET, None, &headers),
            )
            .await;
            assert!(!status.is_success());
            assert!(!String::from_utf8_lossy(&body).contains("private-source"));
        }
        let (status, _, _) = call(
            &mut router,
            local_with_headers(&uri, Method::GET, None, &[("X-Session-Id", &session.id)]),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
}
