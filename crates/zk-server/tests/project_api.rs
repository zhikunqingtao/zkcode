//! Projects 域集成测试（2.1）——7 端点 + session projectId 扩展 + openapi。
//!
//! 语义权威：旧 `ProjectController.java` / `ProjectWorkspaceService.java` /
//! `ConfigController.java`；响应形状权威：`docs/baseline/samples/`
//! `GET_api-config-project.json` / `PUT_api-config-project.json`。
//! 原生选择器仅测守卫路径（403 三分支），不真启 osascript（CI/无 GUI 安全）。

mod common;

use axum::http::{Method, StatusCode};
use common::{
    app, app_with_config, call, json_body, local_delete, local_get, local_post, local_put,
    local_with_headers, sample,
};
use std::path::PathBuf;
use zk_server::config::Config;

/// 独占临时目录（macOS `/tmp` 为 symlink，必须 canonicalize 再用）。
fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("zk-papi-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    std::fs::canonicalize(&dir).expect("canonicalize temp dir")
}

/// 本地选择器开启的配置（直连本机创建/浏览放行）。
fn picker_enabled_config() -> Config {
    let mut config = Config::test_config();
    config.local_picker_enabled = true;
    config
}

/// allowed roots 配置（选择器关闭亦放行，旧守卫短路语义）。
fn rooted_config(roots: Vec<PathBuf>) -> Config {
    let mut config = Config::test_config();
    config.workspace_allowed_roots = roots;
    config
}

/// 错误信封的稳定错误码。
fn error_code(bytes: &[u8]) -> String {
    json_body(bytes)["code"]
        .as_str()
        .expect("error code string")
        .to_owned()
}

#[tokio::test]
async fn projects_list_empty_then_crud_roundtrip() {
    let ws = temp_dir("crud");
    let (mut app, _db) = app_with_config(picker_enabled_config());

    // 初始列表为空数组（前端 404 降级分支据此切回）。
    let (status, _, bytes) = call(&mut app, local_get("/api/projects")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&bytes), serde_json::json!([]));

    // 创建 → 201，四键 camelCase（旧 Project record 形状）。
    let body = serde_json::json!({"name": "Demo", "workspaceRoot": ws.to_string_lossy()});
    let (status, _, bytes) = call(
        &mut app,
        local_post("/api/projects", Some(body.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{bytes:?}");
    let created = json_body(&bytes);
    let mut keys: Vec<&str> = created
        .as_object()
        .expect("obj")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["createdAt", "id", "name", "workspaceRoot"]);
    assert_eq!(created["name"], "Demo");
    assert_eq!(created["workspaceRoot"], ws.to_string_lossy().as_ref());
    let id = created["id"].as_str().expect("id").to_owned();

    // 列表含新建项目。
    let (status, _, bytes) = call(&mut app, local_get("/api/projects")).await;
    assert_eq!(status, StatusCode::OK);
    let list = json_body(&bytes);
    assert_eq!(list.as_array().expect("array").len(), 1);
    assert_eq!(list[0]["id"], id.as_str());

    // 撤销 → {projectId,revoked:true}；再次撤销幂等 revoked:false。
    let (status, _, bytes) = call(&mut app, local_delete(&format!("/api/projects/{id}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_body(&bytes),
        serde_json::json!({"projectId": id, "revoked": true})
    );
    let (status, _, bytes) = call(&mut app, local_delete(&format!("/api/projects/{id}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&bytes)["revoked"], false);

    let _ = std::fs::remove_dir_all(&ws);
}

#[tokio::test]
async fn create_project_validation_errors() {
    let (mut app, _db) = app_with_config(picker_enabled_config());

    // 空体 → 400 WORKSPACE_REQUIRED（旧 required=false null 分支）。
    let (status, _, bytes) = call(&mut app, local_post("/api/projects", None)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&bytes), "WORKSPACE_REQUIRED");
    assert_eq!(json_body(&bytes)["message"], "Request body is required");

    // 非法 JSON → 400 INVALID_REQUEST_BODY。
    let (status, _, bytes) = call(
        &mut app,
        local_post("/api/projects", Some("{not json".into())),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&bytes), "INVALID_REQUEST_BODY");

    // 名称缺失/超长 → PROJECT_NAME_INVALID（名称校验先于路径，旧顺序）。
    for name in [serde_json::json!(null), serde_json::json!("x".repeat(81))] {
        let body = serde_json::json!({"name": name, "workspaceRoot": "/tmp"});
        let (status, _, bytes) = call(
            &mut app,
            local_post("/api/projects", Some(body.to_string())),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(error_code(&bytes), "PROJECT_NAME_INVALID");
    }

    // 相对路径 / 不存在 → 对应稳定错误码。
    let cases = [
        ("relative/path", "WORKSPACE_ABSOLUTE_REQUIRED"),
        ("/no/such/zk-dir-xyz", "WORKSPACE_NOT_FOUND"),
    ];
    for (path, code) in cases {
        let body = serde_json::json!({"name": "Demo", "workspaceRoot": path});
        let (status, _, bytes) = call(
            &mut app,
            local_post("/api/projects", Some(body.to_string())),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "path: {path}");
        assert_eq!(error_code(&bytes), code, "path: {path}");
    }
}

#[tokio::test]
async fn create_project_duplicate_workspace_conflicts() {
    let ws = temp_dir("dup");
    let (mut app, _db) = app_with_config(picker_enabled_config());
    let body = serde_json::json!({"name": "Demo", "workspaceRoot": ws.to_string_lossy()});

    let (status, _, _) = call(
        &mut app,
        local_post("/api/projects", Some(body.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // 同 workspace 再建 → 409 PROJECT_PATH_DUPLICATE（UNIQUE → 映射层）。
    let retry = serde_json::json!({"name": "Other", "workspaceRoot": ws.to_string_lossy()});
    let (status, _, bytes) = call(
        &mut app,
        local_post("/api/projects", Some(retry.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error_code(&bytes), "PROJECT_PATH_DUPLICATE");
    assert_eq!(
        json_body(&bytes)["message"],
        "A Project already uses this workspace"
    );

    let _ = std::fs::remove_dir_all(&ws);
}

#[tokio::test]
async fn guards_reject_when_picker_disabled_or_forwarded() {
    // 默认配置（选择器关闭 + 无 allowed roots）：创建/浏览均 403。
    let mut disabled = app();
    let body = serde_json::json!({"name": "Demo", "workspaceRoot": "/tmp"});
    let (status, _, bytes) = call(
        &mut disabled,
        local_post("/api/projects", Some(body.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&bytes), "LOCAL_PICKER_DISABLED");
    let (status, _, bytes) = call(&mut disabled, local_get("/api/projects/directories")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&bytes), "LOCAL_PICKER_DISABLED");

    // 选择器开启但带转发头（经代理）→ REMOTE_* 拒绝。
    let (mut enabled, _db) = app_with_config(picker_enabled_config());
    let (status, _, bytes) = call(
        &mut enabled,
        local_with_headers(
            "/api/projects",
            Method::POST,
            Some(body.to_string()),
            &[("X-Forwarded-For", "10.0.0.1")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&bytes), "REMOTE_PROJECT_CREATE_FORBIDDEN");
    let (status, _, bytes) = call(
        &mut enabled,
        local_with_headers(
            "/api/projects/directories",
            Method::GET,
            None,
            &[("X-Real-IP", "10.0.0.1")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&bytes), "REMOTE_DIRECTORY_BROWSE_FORBIDDEN");
}

#[tokio::test]
async fn browse_directories_lists_sorted_with_parent() {
    let root = temp_dir("browse");
    std::fs::create_dir_all(root.join("beta")).expect("mkdir");
    std::fs::create_dir_all(root.join("Alpha/inner")).expect("mkdir");
    std::fs::write(root.join("plain.txt"), "x").expect("write");
    let (mut app, _db) = app_with_config(rooted_config(vec![root.clone()]));

    // 缺省路径 → 首根；current==root 时 parent 剥离；子目录大小写不敏感排序。
    let (status, _, bytes) = call(&mut app, local_get("/api/projects/directories")).await;
    assert_eq!(status, StatusCode::OK);
    let listing = json_body(&bytes);
    assert_eq!(
        listing["roots"],
        serde_json::json!([root.to_string_lossy()])
    );
    assert_eq!(listing["current"], root.to_string_lossy().as_ref());
    assert!(listing.get("parent").is_none(), "parent must be stripped");
    assert_eq!(listing["nativePickerAvailable"], false);
    let names: Vec<&str> = listing["directories"]
        .as_array()
        .expect("array")
        .iter()
        .map(|entry| entry["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, vec!["Alpha", "beta"]);

    // 进入子目录 → parent 指向上级。
    let child = root.join("Alpha");
    let (status, _, bytes) = call(
        &mut app,
        local_get(&format!(
            "/api/projects/directories?path={}",
            child.to_string_lossy()
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let listing = json_body(&bytes);
    assert_eq!(listing["current"], child.to_string_lossy().as_ref());
    assert_eq!(listing["parent"], root.to_string_lossy().as_ref());

    // 相对段路径 → 400 DIRECTORY_PATH_NOT_CANONICAL。
    let (status, _, bytes) = call(
        &mut app,
        local_get(&format!(
            "/api/projects/directories?path={}/Alpha/../beta",
            root.to_string_lossy()
        )),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&bytes), "DIRECTORY_PATH_NOT_CANONICAL");

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn browse_and_create_outside_allowed_roots_rejected() {
    let inside = temp_dir("in");
    let outside = temp_dir("out");
    let (mut app, _db) = app_with_config(rooted_config(vec![inside.clone()]));

    // 越根浏览 → 403 DIRECTORY_BROWSE_OUTSIDE_ROOTS。
    let (status, _, bytes) = call(
        &mut app,
        local_get(&format!(
            "/api/projects/directories?path={}",
            outside.to_string_lossy()
        )),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&bytes), "DIRECTORY_BROWSE_OUTSIDE_ROOTS");

    // 越根创建 → 403 WORKSPACE_ACCESS_DENIED（allowed roots 边界）。
    let body = serde_json::json!({"name": "Demo", "workspaceRoot": outside.to_string_lossy()});
    let (status, _, bytes) = call(
        &mut app,
        local_post("/api/projects", Some(body.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&bytes), "WORKSPACE_ACCESS_DENIED");

    // 根内创建放行（选择器关闭亦可，旧守卫短路）。
    let body = serde_json::json!({"name": "Demo", "workspaceRoot": inside.to_string_lossy()});
    let (status, _, _) = call(
        &mut app,
        local_post("/api/projects", Some(body.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let _ = std::fs::remove_dir_all(&inside);
    let _ = std::fs::remove_dir_all(&outside);
}

#[tokio::test]
async fn pick_directory_guards() {
    // 意图头缺失 → 403 NATIVE_PICKER_HEADER_REQUIRED（最先判定）。
    let (mut enabled, _db) = app_with_config(picker_enabled_config());
    let (status, _, bytes) = call(
        &mut enabled,
        local_post("/api/projects/directories/pick", None),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&bytes), "NATIVE_PICKER_HEADER_REQUIRED");
    assert_eq!(
        json_body(&bytes)["message"],
        "X-Zhikun-Native-Picker: 1 is required"
    );

    // 意图头 + 转发头 → 403 NATIVE_PICKER_FORWARDED_REQUEST。
    let (status, _, bytes) = call(
        &mut enabled,
        local_with_headers(
            "/api/projects/directories/pick",
            Method::POST,
            None,
            &[
                ("X-Zhikun-Native-Picker", "1"),
                ("Forwarded", "for=10.0.0.1"),
            ],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&bytes), "NATIVE_PICKER_FORWARDED_REQUEST");

    // 意图头 + 选择器关闭 → 403 NATIVE_PICKER_FORBIDDEN（不触真 picker）。
    let mut disabled = app();
    let (status, _, bytes) = call(
        &mut disabled,
        local_with_headers(
            "/api/projects/directories/pick",
            Method::POST,
            None,
            &[("X-Zhikun-Native-Picker", "1")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&bytes), "NATIVE_PICKER_FORBIDDEN");
}

#[tokio::test]
async fn project_config_matches_samples_and_persists() {
    let mut app = app();

    // GET 默认值逐键对齐样例。
    let (status, _, bytes) = call(&mut app, local_get("/api/config/project")).await;
    assert_eq!(status, StatusCode::OK);
    common::assert_same_shape(
        &sample("GET_api-config-project.json"),
        &json_body(&bytes),
        "$",
    );

    // PUT PATCH 语义：顶层键合并、回显 {success,config}、逐键对齐样例形状。
    let patch = serde_json::json!({"lastModel": "kimi-k2", "lastCost": 1.5});
    let (status, _, bytes) = call(
        &mut app,
        local_put("/api/config/project", Some(patch.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let updated = json_body(&bytes);
    assert_eq!(updated["success"], true);
    assert_eq!(updated["config"]["lastModel"], "kimi-k2");
    assert_eq!(updated["config"]["lastCost"], 1.5);
    common::assert_same_shape(
        &sample("PUT_api-config-project.json")["config"],
        &serde_json::json!({
            "lastCost": updated["config"]["lastCost"],
            "projectAlwaysAllowRules": updated["config"]["projectAlwaysAllowRules"],
            "projectMcpServers": updated["config"]["projectMcpServers"],
            "customSettings": updated["config"]["customSettings"],
        }),
        "$.config",
    );

    // GET 回读持久化（同库句柄内合并结果保留）。
    let (status, _, bytes) = call(&mut app, local_get("/api/config/project")).await;
    assert_eq!(status, StatusCode::OK);
    let stored = json_body(&bytes);
    assert_eq!(stored["lastModel"], "kimi-k2");
    assert_eq!(stored["lastCost"], 1.5);

    // 非对象体 → 400 INVALID_REQUEST_BODY。
    let (status, _, bytes) = call(
        &mut app,
        local_put("/api/config/project", Some("[1,2]".into())),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_code(&bytes), "INVALID_REQUEST_BODY");

    // 字段类型非法（lastCost 传字符串）→ 500 INTERNAL_ERROR（旧
    // `updateProjectConfig` 的 `RuntimeException` 经 `handleGeneric` 归一）。
    let bad = serde_json::json!({"lastCost": "free"});
    let (status, _, bytes) = call(
        &mut app,
        local_put("/api/config/project", Some(bad.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error_code(&bytes), "INTERNAL_ERROR");
}

#[tokio::test]
async fn session_create_with_project_id() {
    let ws = temp_dir("sess");
    let (mut app, _db) = app_with_config(picker_enabled_config());

    // 未知 projectId → 404 PROJECT_NOT_FOUND（旧 resolveWorkspace 文案）。
    let body = serde_json::json!({"projectId": "nope"});
    let (status, _, bytes) = call(
        &mut app,
        local_post("/api/sessions", Some(body.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(error_code(&bytes), "PROJECT_NOT_FOUND");
    assert_eq!(
        json_body(&bytes)["message"],
        "Project with id 'nope' was not found"
    );

    // 创建项目 → 带 projectId 建会话 → workingDirectory 绑定项目 workspace。
    let create = serde_json::json!({"name": "Demo", "workspaceRoot": ws.to_string_lossy()});
    let (status, _, bytes) = call(
        &mut app,
        local_post("/api/projects", Some(create.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let project_id = json_body(&bytes)["id"].as_str().expect("id").to_owned();

    let body = serde_json::json!({"projectId": project_id});
    let (status, _, _) = call(
        &mut app,
        local_post("/api/sessions", Some(body.to_string())),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _, bytes) = call(&mut app, local_get("/api/sessions")).await;
    assert_eq!(status, StatusCode::OK);
    let page = json_body(&bytes);
    assert_eq!(
        page["sessions"][0]["workingDirectory"],
        ws.to_string_lossy().as_ref()
    );

    // 缺省 projectId：Phase 1 行为不变（进程当前目录）。
    let (status, _, _) = call(&mut app, local_post("/api/sessions", None)).await;
    assert_eq!(status, StatusCode::CREATED);

    let _ = std::fs::remove_dir_all(&ws);
}

#[tokio::test]
async fn openapi_includes_project_paths() {
    let mut app = app();
    let (status, _, bytes) = call(&mut app, local_get("/api/openapi.json")).await;
    assert_eq!(status, StatusCode::OK);
    let doc = json_body(&bytes);
    let paths = doc["paths"].as_object().expect("paths object");
    for path in [
        "/api/projects",
        "/api/projects/directories",
        "/api/projects/directories/pick",
        "/api/projects/{projectId}",
        "/api/config/project",
        // 2.5 交互域 + 授权域（旧 InteractionController / PermissionGrantController）。
        "/api/interactions/pending",
        "/api/interactions/{interactionId}/decisions",
        "/api/permissions/grants",
        "/api/permissions/grants/{grantId}",
        // 3B.7 技能域（旧 SkillController）。
        "/api/skills",
        "/api/skills/{name}",
        // Batch 1 Step 1-5 / 1-6 工具域与诊断端点（旧 ToolController /
        // HealthController.doctor）。
        "/api/tools",
        "/api/tools/{toolName}",
        "/api/doctor",
        // Batch 2 端点域（旧 RunController / FileController / ActivityController /
        // AttachmentController）。
        "/api/runs/session/{sessionId}",
        "/api/runs/{runId}",
        "/api/runs/{runId}/events",
        "/api/files/search",
        "/api/sessions/{sessionId}/files/preview",
        "/api/sessions/{sessionId}/files/reveal",
        "/api/sessions/{sessionId}/activities",
        "/api/attachments/upload",
        "/api/attachments/{fileUuid}",
        // Batch 4B MCP 域（旧 McpController / McpCapabilityController）。
        "/api/mcp/servers",
        "/api/mcp/servers/{name}",
        "/api/mcp/servers/{name}/restart",
        "/api/mcp/servers/{name}/logs",
        "/api/mcp/reconnect",
        "/api/mcp/resources",
        "/api/mcp/resources/read",
        "/api/mcp/prompts",
        "/api/mcp/prompts/execute",
        "/api/mcp/capabilities",
        "/api/mcp/capabilities/{id}",
        "/api/mcp/capabilities/{id}/toggle",
        "/api/mcp/capabilities/domains",
        "/api/mcp/capabilities/{id}/server-tools",
        "/api/mcp/capabilities/{id}/test",
        "/api/mcp/capabilities/{id}/invoke",
        // Batch 5 Step 6 记忆与历史域（旧 MemoryController /
        // FileHistoryController）。`/api/memory` 一条路径承载 GET/PUT/POST
        // 三方法（旧 controller 三注解无子路径）。
        "/api/memory",
        "/api/memory/all",
        "/api/memory/{memoryId}",
        "/api/sessions/{sessionId}/history/snapshots",
        "/api/sessions/{sessionId}/history/rewind",
        "/api/sessions/{sessionId}/history/diff",
        "/api/asr/status",
        "/api/asr/recognize",
        "/api/tts/status",
        "/api/tts/synthesize",
    ] {
        assert!(paths.contains_key(path), "missing {path}");
    }
    assert_eq!(paths.len(), 62);
}
