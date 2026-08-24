//! Batch 5 Step 6 集成测试——记忆域 5 端点（旧 `MemoryController`）与文件历史
//! 域 3 端点（旧 `FileHistoryController`）的端到端契约。
//!
//! 覆盖点：记忆 CRUD 往返与 `PUT` 的 upsert 语义、`/all` 双源信封、
//! 快照按 `messageId` 分组的单元素数组形状、`rewind` 恒 200 与真实文件恢复、
//! `diff` 的必填参数守卫，以及两域同栈过 `access_guard`。

mod common;

use axum::http::{Method, StatusCode};
use std::path::{Path, PathBuf};

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
            "scope",
            "source",
            "title",
            "updatedAt"
        ]
    );
    assert_eq!(entries[0]["id"], id.as_str());
    // 缺省兜底：INSERT 路径的 scope=global / source=USER。
    assert_eq!(entries[0]["scope"], "global");
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
    assert_eq!(entries[0]["scope"], "global");
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

/// `/all` 双源信封：`sqlite` 复用同一降序查询，`memoryMd` 为四键条目数组
/// （`~/.zk/MEMORY.md` 缺失时为空数组，测试不写用户目录）。
#[tokio::test]
async fn memory_all_merges_sqlite_and_memory_md() {
    let (mut router, _db) = app_with_db();
    call(
        &mut router,
        local_post("/api/memory", Some(memory_body(Some("mem-a"), "alpha"))),
    )
    .await;

    let (status, _headers, body) = call(&mut router, local_get("/api/memory/all")).await;
    assert_eq!(status, StatusCode::OK);
    let all = json_body(&body);
    let mut keys: Vec<&str> = all
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["memoryMd", "sqlite"]);
    let sqlite = all["sqlite"].as_array().expect("sqlite array");
    assert_eq!(sqlite.len(), 1);
    assert_eq!(sqlite[0]["id"], "mem-a");
    for item in all["memoryMd"].as_array().expect("memoryMd array") {
        let mut item_keys: Vec<&str> = item
            .as_object()
            .expect("md entry object")
            .keys()
            .map(String::as_str)
            .collect();
        item_keys.sort_unstable();
        assert_eq!(
            item_keys,
            vec!["category", "content", "source", "timestamp"],
            "entry: {item}"
        );
    }
}

/// 快照按 `messageId` 分组，每键恒单元素数组；`fileCount` 与 `trackedFiles`
/// 长度一致，`timestamp` 取组内首条。
#[tokio::test]
async fn history_snapshots_group_by_message_id() {
    let (mut router, db) = app_with_db();
    let session = db
        .create_session("claude-sonnet-4", "/tmp")
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
        local_get(&format!("/api/sessions/{}/history/snapshots", session.id)),
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
        .create_session("claude-sonnet-4", "/tmp")
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
        local_get(&format!(
            "/api/sessions/{}/history/diff?fromMessageId=from&toMessageId=to",
            session.id
        )),
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
        .create_session("claude-sonnet-4", "/tmp")
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

    let (status, _headers, body) = call(
        &mut router,
        local_post(
            &format!("/api/sessions/{}/history/rewind", session.id),
            Some(format!(
                "{{\"messageId\":\"turn-1\",\"filePaths\":[\"{target_path}\"]}}"
            )),
        ),
    )
    .await;
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

/// `rewind` 失败恒 200：错误只进 `errors`（会话缺失 / 快照缺失两分支）。
#[tokio::test]
async fn history_rewind_reports_failures_with_ok_status() {
    let (mut router, db) = app_with_db();

    let (status, _headers, body) = call(
        &mut router,
        local_post(
            "/api/sessions/ghost-session/history/rewind",
            Some("{\"messageId\":\"turn-1\"}".to_owned()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let result = json_body(&body);
    assert_eq!(result["success"], false);
    assert_eq!(result["errors"][0], "SESSION_NOT_FOUND");

    let session = db
        .create_session("claude-sonnet-4", "/tmp")
        .await
        .expect("create session");
    let (status, _headers, body) = call(
        &mut router,
        local_post(
            &format!("/api/sessions/{}/history/rewind", session.id),
            Some("{\"messageId\":\"nope\"}".to_owned()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let result = json_body(&body);
    assert_eq!(result["success"], false);
    assert_eq!(
        result["errors"][0],
        "No snapshots found for messageId: nope"
    );
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
/// 58 条计数互锁）。
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
