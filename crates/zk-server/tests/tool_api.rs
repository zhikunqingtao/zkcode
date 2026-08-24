//! Batch 1 Step 1-5 集成测试——工具域 3 端点（旧 `ToolController` 97 行）。
//!
//! 契约权威：`backend/src/main/java/com/aicodeassistant/controller/
//! ToolController.java`（`listTools` L30-57 / `getToolDetail` L60-69 /
//! `toggleTool` L72-87）+ `GlobalExceptionHandler`（异常→状态码）。
//!
//! 三处旧行为怪癖在此逐条钉死，防后续"顺手修正"：
//! `?toolName=` 只校验不筛选、详情未命中是 400（非 404）、空白 `sessionId`
//! 等价缺省（不落状态但仍 200）。

mod common;

use axum::http::StatusCode;

use common::{app, call, json_body, local_get, local_patch, remote_get};

/// 目录端点在测试装配（侧车关 + Agent/Worktree 冻结 + `AGENT_TRIGGERS`
/// 关）下恒为基础族 33 件，字典序稳定。未验收的 Agent、Task 与
/// Worktree 工具不向模型暴露。
///
/// Cron 三件受 `AGENT_TRIGGERS` **注册期**门控（出厂关），故不在此名单。
const BASE_TOOLS: [&str; 33] = [
    "AskUserQuestion",
    "Bash",
    "Config",
    "CtxInspect",
    "Edit",
    "EnterPlanMode",
    "ExitPlanMode",
    "GitDiff",
    "GitLog",
    "GitStatus",
    "Glob",
    "Grep",
    "ListDir",
    "ListMcpResources",
    "Memory",
    "Monitor",
    "NotebookEdit",
    "REPL",
    "Read",
    "ReadMcpResource",
    "Skill",
    "Sleep",
    "Snip",
    "SyntheticOutput",
    "TerminalCapture",
    "TodoWrite",
    "ToolSearch",
    "VerifyJourney",
    "VerifyPlanExecution",
    "Visualization",
    "WebFetch",
    "WebSearch",
    "Write",
];

/// `GET /api/tools` 200：非空、逐项五键、`category`/`permissionLevel` 取值域
/// 对齐旧 `Tool.getGroup()` / `PermissionRequirement`，未设会话覆盖时全启用。
#[tokio::test]
async fn tools_list_returns_registered_catalog() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/tools")).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    let items = body["tools"].as_array().expect("tools array");
    assert!(!items.is_empty(), "catalog must not be empty");

    let mut names: Vec<&str> = Vec::new();
    for item in items {
        let object = item.as_object().expect("object item");
        let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "category",
                "description",
                "enabled",
                "name",
                "permissionLevel"
            ],
            "item: {item}"
        );
        assert!(
            !item["description"].as_str().expect("str").is_empty(),
            "empty description: {item}"
        );
        assert_eq!(item["enabled"], true, "registered tool is enabled: {item}");
        let permission = item["permissionLevel"].as_str().expect("str");
        assert!(
            ["NONE", "ALWAYS_ASK", "CONDITIONAL"].contains(&permission),
            "unknown PermissionRequirement: {item}"
        );
        names.push(item["name"].as_str().expect("str"));
    }
    assert_eq!(
        names, BASE_TOOLS,
        "33 frozen base tools in registry key order"
    );
}

/// 元数据逐字对照旧仓库：`Bash` = `bash`/`CONDITIONAL`、`Write` =
/// `edit`/`ALWAYS_ASK`、只读族 = `read`/`NONE`。
#[tokio::test]
async fn tool_metadata_matches_legacy_groups_and_permissions() {
    let mut router = app();
    let (_status, _headers, body) = call(&mut router, local_get("/api/tools")).await;
    let body = json_body(&body);
    let items = body["tools"].as_array().expect("tools array").clone();
    let find = |name: &str| {
        items
            .iter()
            .find(|item| item["name"] == name)
            .cloned()
            .unwrap_or_else(|| panic!("tool {name} missing from catalog"))
    };

    let bash = find("Bash");
    assert_eq!(bash["category"], "bash");
    assert_eq!(bash["permissionLevel"], "CONDITIONAL");

    let write = find("Write");
    assert_eq!(write["category"], "edit");
    assert_eq!(write["permissionLevel"], "ALWAYS_ASK");

    for name in ["Read", "Glob", "Grep", "ListDir", "GitStatus"] {
        let tool = find(name);
        assert_eq!(tool["category"], "read", "tool {name}");
        assert_eq!(tool["permissionLevel"], "NONE", "tool {name}");
    }
}

/// 旧怪癖 ①：`?toolName=` 只做存在性校验——命中仍返回**全量**列表，
/// 未命中 404 `TOOL_NOT_FOUND`。
#[tokio::test]
async fn tool_name_query_validates_without_filtering() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/tools?toolName=Read")).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    assert_eq!(
        body["tools"].as_array().expect("array").len(),
        BASE_TOOLS.len(),
        "toolName must not filter the list (legacy ToolController:34-47)"
    );

    let (status, _headers, body) = call(&mut router, local_get("/api/tools?toolName=Nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let body = json_body(&body);
    assert_eq!(body["code"], "TOOL_NOT_FOUND");
    assert_eq!(body["message"], "Tool not found: Nope");
}

/// 旧怪癖 ③：空白 `sessionId` / `toolName`（旧 `isBlank`）等价缺省——
/// 不触发校验、不读会话覆盖。
#[tokio::test]
async fn blank_query_params_are_treated_as_absent() {
    let mut router = app();
    let (status, _headers, body) = call(
        &mut router,
        local_get("/api/tools?sessionId=%20&toolName=%20"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_body(&body)["tools"].as_array().expect("array").len(),
        BASE_TOOLS.len()
    );
}

/// 详情端点 200：五键含 `inputSchema`（旧 `Tool.getInputSchema()`）。
#[tokio::test]
async fn tool_detail_exposes_input_schema() {
    let mut router = app();
    for name in BASE_TOOLS {
        let (status, _headers, body) =
            call(&mut router, local_get(&format!("/api/tools/{name}"))).await;
        assert_eq!(status, StatusCode::OK, "tool {name}");
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
            vec![
                "category",
                "description",
                "inputSchema",
                "name",
                "permissionLevel"
            ],
            "tool {name}"
        );
        assert_eq!(body["name"], name);
        assert!(
            body["inputSchema"].is_object(),
            "tool {name} schema must be a JSON Schema object"
        );
    }
}

/// 旧怪癖 ②：详情端点未命中走 `findByName` → `IllegalArgumentException` →
/// `GlobalExceptionHandler:107` **400 `INVALID_REQUEST`**（**不是** 404，
/// 与同资源的列表/PATCH 路径状态码在旧端本就不一致）。
#[tokio::test]
async fn unknown_tool_detail_returns_invalid_request() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/tools/Nope")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body = json_body(&body);
    assert_eq!(body["code"], "INVALID_REQUEST");
    assert_eq!(body["message"], "Unknown tool: Nope");
}

/// PATCH 200 回显 `{tool,enabled}`，且会话级禁用对**同会话**的列表可见、
/// 对未带 `sessionId` 的列表不可见（旧 `ToolSessionState` 双层映射）。
#[tokio::test]
async fn toggle_tool_persists_per_session_override() {
    let mut router = app();
    let (status, _headers, body) = call(
        &mut router,
        local_patch(
            "/api/tools/Bash",
            Some(r#"{"sessionId":"s-1","enabled":false}"#.to_owned()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    assert_eq!(body["tool"], "Bash");
    assert_eq!(body["enabled"], false);

    let enabled_of = |value: &serde_json::Value, name: &str| -> bool {
        value["tools"]
            .as_array()
            .expect("array")
            .iter()
            .find(|item| item["name"] == name)
            .unwrap_or_else(|| panic!("tool {name} missing"))["enabled"]
            .as_bool()
            .expect("bool")
    };

    let (_status, _headers, scoped) =
        call(&mut router, local_get("/api/tools?sessionId=s-1")).await;
    let scoped = json_body(&scoped);
    assert!(!enabled_of(&scoped, "Bash"), "session override applies");
    assert!(enabled_of(&scoped, "Read"), "other tools untouched");

    let (_status, _headers, other) = call(&mut router, local_get("/api/tools?sessionId=s-2")).await;
    assert!(
        enabled_of(&json_body(&other), "Bash"),
        "override must not leak across sessions"
    );

    let (_status, _headers, global) = call(&mut router, local_get("/api/tools")).await;
    assert!(
        enabled_of(&json_body(&global), "Bash"),
        "no sessionId → global enabled bit"
    );

    // 再次 PATCH 回 true：覆盖可翻转。
    let (status, _headers, _body) = call(
        &mut router,
        local_patch(
            "/api/tools/Bash",
            Some(r#"{"sessionId":"s-1","enabled":true}"#.to_owned()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_status, _headers, scoped) =
        call(&mut router, local_get("/api/tools?sessionId=s-1")).await;
    assert!(enabled_of(&json_body(&scoped), "Bash"), "override flipped");
}

/// `sessionId` 缺省/空白时仍回 200 但**不落状态**（旧 L82-84）。
#[tokio::test]
async fn toggle_without_session_id_is_accepted_without_persisting() {
    let mut router = app();
    let (status, _headers, body) = call(
        &mut router,
        local_patch("/api/tools/Read", Some(r#"{"enabled":false}"#.to_owned())),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["enabled"], false);

    let (_status, _headers, body) = call(&mut router, local_get("/api/tools?sessionId=s-9")).await;
    let body = json_body(&body);
    let read = body["tools"]
        .as_array()
        .expect("array")
        .iter()
        .find(|item| item["name"] == "Read")
        .expect("Read present");
    assert_eq!(read["enabled"], true, "nothing was persisted");
}

/// PATCH 未注册工具 → 404 `TOOL_NOT_FOUND`（旧 `findByNameOptional` 分支，
/// 与详情端点的 400 形成有意的不对称）。
#[tokio::test]
async fn toggle_unknown_tool_returns_not_found() {
    let mut router = app();
    let (status, _headers, body) = call(
        &mut router,
        local_patch(
            "/api/tools/Nope",
            Some(r#"{"sessionId":"s-1","enabled":false}"#.to_owned()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let body = json_body(&body);
    assert_eq!(body["code"], "TOOL_NOT_FOUND");
    assert_eq!(body["message"], "Tool not found: Nope");
}

/// 体缺失 / 非法 JSON → 400 `INVALID_REQUEST_BODY`（旧 `@RequestBody`
/// required + `HttpMessageNotReadableException`）；校验先于工具存在性。
#[tokio::test]
async fn toggle_rejects_missing_or_malformed_body() {
    let mut router = app();
    for body in [None, Some("{".to_owned()), Some(String::new())] {
        let (status, _headers, response) =
            call(&mut router, local_patch("/api/tools/Bash", body.clone())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body {body:?}");
        let response = json_body(&response);
        assert_eq!(response["code"], "INVALID_REQUEST_BODY");
        assert_eq!(response["message"], "Request body is missing or malformed");
    }
}

/// 缺省 `enabled` 对齐 Jackson 基元默认（`boolean` → `false`）。
#[tokio::test]
async fn absent_enabled_flag_defaults_to_false() {
    let mut router = app();
    let (status, _headers, body) = call(
        &mut router,
        local_patch("/api/tools/Bash", Some(r#"{"sessionId":"s-3"}"#.to_owned())),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["enabled"], false);
}

/// 工具端点同栈过 `access_guard`（公网对端 403）。
#[tokio::test]
async fn tools_reject_remote_peer() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, remote_get("/api/tools")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json_body(&body)["code"], "ACCESS_DENIED");
}

/// `OpenAPI` 文档收录工具两路径（与 `api::openapi` 单测的 27 条计数互锁）。
#[tokio::test]
async fn openapi_document_lists_tool_paths() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/openapi.json")).await;
    assert_eq!(status, StatusCode::OK);
    let document = json_body(&body);
    let paths = document["paths"].as_object().expect("paths object");
    assert!(paths.contains_key("/api/tools"));
    let detail = paths
        .get("/api/tools/{toolName}")
        .expect("detail path documented");
    assert!(detail.get("get").is_some(), "GET documented");
    assert!(detail.get("patch").is_some(), "PATCH documented");
}
