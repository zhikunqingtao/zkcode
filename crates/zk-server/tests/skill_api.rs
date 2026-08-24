//! 3B.7 集成测试——技能目录与详情端点（旧 `SkillController` 两端点）。
//!
//! 前端契约（旧仓库 `frontend/src/App.tsx:89` 与
//! `components/skills/SkillDetailModal.tsx`）：列表三键 `name` /
//! `description` / `source`，详情五键再加 `content` / `filePath`；未命中
//! 404（前端显示「Skill not found」）。

mod common;

use axum::http::StatusCode;

use common::{app, call, json_body, local_get, remote_get};
use zk_server::skill::BUILTIN_SKILL_NAMES;

/// 目录端点 200 且恰为 14 条内置技能（消除前端 404）；每项三键、`source`
/// 恒 `BUNDLED`、`description` 非空、按展示名升序。
#[tokio::test]
async fn skills_list_returns_fourteen_bundled_items() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/skills")).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    let items = body.as_array().expect("json array");
    assert_eq!(items.len(), BUILTIN_SKILL_NAMES.len());
    assert_eq!(items.len(), 14);

    let mut names: Vec<&str> = Vec::new();
    for item in items {
        let object = item.as_object().expect("object item");
        let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["description", "name", "source"], "item: {item}");
        assert_eq!(item["source"], "BUNDLED");
        assert!(
            !item["description"].as_str().expect("str").is_empty(),
            "empty description: {item}"
        );
        names.push(item["name"].as_str().expect("str"));
    }
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted, "list must be sorted by effective name");
}

/// 详情端点 200：五键齐全、`filePath` 为 null（内置技能）、`content` 非空
/// 且不含 frontmatter 分隔符（正文已切分）。
#[tokio::test]
async fn skill_detail_returns_content_with_null_file_path() {
    let mut router = app();
    for name in BUILTIN_SKILL_NAMES {
        let (status, _headers, body) =
            call(&mut router, local_get(&format!("/api/skills/{name}"))).await;
        assert_eq!(status, StatusCode::OK, "skill {name}");
        let body = json_body(&body);
        let mut keys: Vec<&str> = body
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["content", "description", "filePath", "name", "source"],
            "skill {name}"
        );
        assert_eq!(body["source"], "BUNDLED", "skill {name}");
        assert!(body["filePath"].is_null(), "skill {name}");
        let content = body["content"].as_str().expect("content str");
        assert!(!content.is_empty(), "skill {name} has empty content");
        assert!(
            !content.starts_with("---"),
            "skill {name} still carries frontmatter"
        );
    }
}

/// `resolve` 归一：`/` 前缀剥离 + 大小写不敏感（旧 `SkillRegistry.resolve`）。
#[tokio::test]
async fn skill_detail_normalizes_slash_prefix_and_case() {
    let mut router = app();
    for uri in ["/api/skills/COMMIT", "/api/skills/%2Fcommit"] {
        let (status, _headers, body) = call(&mut router, local_get(uri)).await;
        assert_eq!(status, StatusCode::OK, "uri {uri}");
        assert_eq!(json_body(&body)["name"], "commit", "uri {uri}");
    }
}

/// 未命中 → 404 `SKILL_NOT_FOUND` 信封（文案逐字对齐旧
/// `ResourceNotFoundException("SKILL_NOT_FOUND", "Skill not found: " + name)`）。
#[tokio::test]
async fn unknown_skill_returns_not_found_envelope() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/skills/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let body = json_body(&body);
    assert_eq!(body["code"], "SKILL_NOT_FOUND");
    assert_eq!(body["message"], "Skill not found: nope");
    uuid::Uuid::parse_str(body["requestId"].as_str().expect("request id"))
        .expect("requestId is a UUID");
}

/// 技能端点同栈过 `access_guard`（公网对端 403）。
#[tokio::test]
async fn skills_reject_remote_peer() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, remote_get("/api/skills")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json_body(&body)["code"], "ACCESS_DENIED");
}

/// `OpenAPI` 文档收录技能两路径（与 `api::openapi` 单测的 24 条计数互锁）。
#[tokio::test]
async fn openapi_document_lists_skill_paths() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/openapi.json")).await;
    assert_eq!(status, StatusCode::OK);
    let paths = json_body(&body);
    let paths = paths["paths"].as_object().expect("paths object");
    assert!(paths.contains_key("/api/skills"));
    assert!(paths.contains_key("/api/skills/{name}"));
}
