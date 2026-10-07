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

/// 目录端点 200 且恰为 13 条内置技能（消除前端 404）；每项三键、`source`
/// 恒 `BUNDLED`、`description` 非空、按展示名升序。
#[tokio::test]
async fn skills_list_returns_applicable_bundled_items() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/skills")).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    let items = body.as_array().expect("json array");
    assert_eq!(items.len(), BUILTIN_SKILL_NAMES.len());
    assert_eq!(items.len(), 13);

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

#[tokio::test]
async fn disabled_runtime_details_stay_hidden_while_canonical_management_remains_available() {
    use zk_server::{
        routes::build_router,
        skill::{SkillDefinition, SkillSource},
        state::AppState,
    };
    let state = AppState::for_tests();
    state.skills.register(SkillDefinition::from_markdown(
        "manage.md",
        "---\nname: DisplayAlias\n---\nMANAGEMENT_BODY",
        SkillSource::User,
        None,
    ));
    let mut router = build_router(state.clone());
    assert_eq!(
        call(&mut router, local_get("/api/skills/detail/manage"))
            .await
            .0,
        StatusCode::OK
    );
    state.skills.set_enabled("manage", false).await.unwrap();
    for path in [
        "/api/skills/detail/manage",
        "/api/skills/detail/DisplayAlias",
        "/api/skills/DisplayAlias",
    ] {
        assert_eq!(
            call(&mut router, local_get(path)).await.0,
            StatusCode::NOT_FOUND,
            "{path}"
        );
    }
    let (status, _, body) = call(&mut router, local_get("/api/skills/manage/manage")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["enabled"], false);
    assert_eq!(
        call(&mut router, local_get("/api/skills/manage/DisplayAlias"))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}

struct SkillWorkspaces(std::path::PathBuf);
impl SkillWorkspaces {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("zk-skill-api-{}", uuid::Uuid::new_v4()));
        for project in ["a", "b"] {
            std::fs::create_dir_all(root.join(project).join(".zkcode/skills")).unwrap();
        }
        Self(root.canonicalize().unwrap())
    }
    fn write(&self, project: &str, name: &str, body: &str) {
        std::fs::write(
            self.0
                .join(project)
                .join(".zkcode/skills")
                .join(format!("{name}.md")),
            body,
        )
        .unwrap();
    }
}
impl Drop for SkillWorkspaces {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn scoped_request(uri: &str, session: &str, patch: bool) -> axum::http::Request<axum::body::Body> {
    let mut request = if patch {
        common::local_patch(uri, None)
    } else {
        local_get(uri)
    };
    request
        .headers_mut()
        .insert("x-session-id", session.parse().unwrap());
    request
}

#[cfg(unix)]
#[tokio::test]
async fn rest_skill_sources_cannot_escape_through_root_aliases() {
    use std::os::unix::fs::symlink;
    use zk_server::{routes::build_router, state::AppState};
    let files = SkillWorkspaces::new();
    files.write("b", "private", "UNAUTHORIZED_PROJECT_SKILL_BODY");
    std::fs::remove_dir(files.0.join("a/.zkcode/skills")).unwrap();
    symlink(
        files.0.join("b/.zkcode/skills"),
        files.0.join("a/.zkcode/skills"),
    )
    .unwrap();
    let state = AppState::for_tests();
    let session = state
        .db
        .create_session("m", files.0.join("a").to_str().unwrap())
        .await
        .unwrap();
    let mut router = build_router(state);
    for uri in ["/api/skills", "/api/skills/manage"] {
        let (status, _, body) = call(&mut router, scoped_request(uri, &session.id, false)).await;
        assert_eq!(status, StatusCode::OK);
        let body = json_body(&body);
        assert!(!body.to_string().contains("UNAUTHORIZED_PROJECT_SKILL_BODY"));
        assert!(
            !body
                .as_array()
                .unwrap()
                .iter()
                .any(|skill| skill["name"] == "private")
        );
    }
    for uri in ["/api/skills/private", "/api/skills/manage/private"] {
        let (status, _, body) = call(&mut router, scoped_request(uri, &session.id, false)).await;
        assert!(!status.is_success());
        let body = json_body(&body);
        assert!(!body.to_string().contains("UNAUTHORIZED_PROJECT_SKILL_BODY"));
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn project_scope_rest_isolation_global_switches_and_unknown_scope_are_authoritative() {
    use zk_server::{routes::build_router, state::AppState};
    let files = SkillWorkspaces::new();
    files.write("a", "shared", "---\nname: AliasA\n---\nA confidential body");
    files.write("b", "shared", "---\nname: AliasB\n---\nB confidential body");
    files.write("a", "private", "A only");
    let state = AppState::for_tests();
    let a = state
        .db
        .create_session("m", files.0.join("a").to_str().unwrap())
        .await
        .unwrap();
    let b = state
        .db
        .create_session("m", files.0.join("b").to_str().unwrap())
        .await
        .unwrap();
    let project = state
        .db
        .create_project("project-b", files.0.join("b").to_str().unwrap())
        .await
        .unwrap();
    let mut router = build_router(state.clone());
    for (session, alias, body) in [
        (&a.id, "AliasA", "A confidential body"),
        (&b.id, "AliasB", "B confidential body"),
    ] {
        let (status, _, data) = call(
            &mut router,
            scoped_request(&format!("/api/skills/{alias}"), session, false),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json_body(&data)["content"], body);
    }
    for (uri, session) in [
        ("/api/skills/private", &b.id),
        ("/api/skills/AliasA", &b.id),
        ("/api/skills/AliasB", &a.id),
    ] {
        assert_eq!(
            call(&mut router, scoped_request(uri, session, false))
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        call(&mut router, local_get("/api/skills/shared")).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&mut router, scoped_request("/api/skills", "missing", false))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let project_uri = format!("/api/skills/shared?projectId={}", project.id);
    let (status, _, data) = call(&mut router, local_get(&project_uri)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&data)["content"], "B confidential body");
    assert_eq!(
        call(&mut router, scoped_request(&project_uri, &a.id, false))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    // A visible canonical identity authorizes the preference operation; the
    // preference applies to same-named definitions without exposing their bodies.
    let (status, _, data) = call(
        &mut router,
        scoped_request(
            "/api/skills/manage/shared/toggle?enabled=false",
            &a.id,
            true,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&data)["scope"], "global");
    assert_eq!(json_body(&data)["enabled"], false);
    for session in [&a.id, &b.id] {
        assert_eq!(
            call(
                &mut router,
                scoped_request("/api/skills/shared", session, false)
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        let (status, _, data) = call(
            &mut router,
            scoped_request("/api/skills/manage/shared", session, false),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json_body(&data)["enabled"], false);
    }
    assert!(state.skills.resolve_including_disabled("shared").is_none());
    assert_eq!(
        call(&mut router, local_get("/api/skills/manage/shared"))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    // A project-specific name unavailable in B cannot be enabled via B.
    assert_eq!(
        call(
            &mut router,
            scoped_request(
                "/api/skills/manage/private/toggle?enabled=false",
                &b.id,
                true
            )
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}
