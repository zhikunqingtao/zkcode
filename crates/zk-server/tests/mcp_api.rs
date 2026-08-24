//! Batch 4B 集成测试——`/api/mcp/**` 二十端点的路由接线与错误语义。
//!
//! 语义来源（旧仓库只读）：`controller/McpController`（10 端点）+
//! `controller/McpCapabilityController`（10 端点）。单测已逐条覆盖纯函数
//! （视图裁剪 / 体解析 / 超时优先级），本文件补的是**真实 Router 之后**的部
//! 分：路径与方法是否真接上、状态码是否与旧端一致、失败体是否为空体。
//!
//! # 生命周期前提
//!
//! `AppState::mcp()` 只装配不 `start()`（启动由 `main` 驱动）：`add_server` /
//! `restart_server` 先撞 `require_running()` → `NotRunning` → 旧端
//! `IllegalStateException` 语义的 500 `INTERNAL_ERROR`；只读端点与
//! `remove_server` 不受运行态影响。集成测试因此不拉起真实 MCP 子进程，也不
//! 发起任何出网连接。
//!
//! 能力注册表的增删改**落盘**，故每个用例把 `mcp_registry_path` 指向独立的
//! temp 文件（`Config::test_config()` 的默认值是进程共享路径，并发用例会互相
//! 干扰）。

mod common;

use std::sync::atomic::{AtomicU32, Ordering};

use axum::Router;
use axum::http::StatusCode;
use common::{call, json_body, local_delete, local_get, local_patch, local_post, local_put};
use zk_server::config::Config;

/// temp 文件名去重计数器（同一进程内多用例并发）。
static SEQ: AtomicU32 = AtomicU32::new(0);

/// 独立注册表落盘路径的测试应用。
fn app_with_isolated_registry() -> Router {
    let mut config = Config::test_config();
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    config.mcp_registry_path = std::env::temp_dir()
        .join("zkcode-test-mcp")
        .join(format!("registry-{}-{seq}.json", std::process::id()));
    common::app_with_config(config).0
}

/// 最小可用的能力定义体（旧注册表 `mcp_tools[]` 的一条）。
fn capability_body(id: &str, enabled: bool) -> String {
    serde_json::json!({
        "id": id,
        "name": "天气查询",
        "toolName": "get_forecast",
        "sseUrl": "http://127.0.0.1:9/weather/sse",
        "domain": "weather",
        "category": "MCP_TOOL",
        "briefDescription": "查询天气",
        "timeoutMs": 15000,
        "enabled": enabled,
        "videoCallEnabled": false,
    })
    .to_string()
}

// ── McpController：只读端点在空状态下的形状 ────────────────────────────────

#[tokio::test]
async fn server_read_endpoints_report_empty_state() {
    let mut app = app_with_isolated_registry();

    let (status, _, body) = call(&mut app, local_get("/api/mcp/servers")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body), serde_json::json!({ "servers": [] }));

    let (status, _, body) = call(&mut app, local_get("/api/mcp/resources")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_body(&body),
        serde_json::json!({ "resources": {}, "totalCount": 0 })
    );

    let (status, _, body) = call(&mut app, local_get("/api/mcp/prompts")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_body(&body),
        serde_json::json!({ "prompts": {}, "totalCount": 0 })
    );

    // 旧 `getServerLogs`：服务器不存在也回 200，正文是单元素说明行。
    let (status, _, body) = call(&mut app, local_get("/api/mcp/servers/nope/logs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_body(&body),
        serde_json::json!({ "logs": ["MCP server not found: nope"] })
    );
}

/// 旧 `deleteServer` 丢弃 `removeServer` 的返回值——不存在同样 `success:true`。
#[tokio::test]
async fn delete_server_always_reports_success() {
    let mut app = app_with_isolated_registry();
    let (status, _, body) = call(&mut app, local_delete("/api/mcp/servers/nope")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body), serde_json::json!({ "success": true }));
}

/// 生命周期未启动 → 旧 `IllegalStateException` 语义的 500 信封。
#[tokio::test]
async fn lifecycle_gated_endpoints_report_internal_error() {
    let mut app = app_with_isolated_registry();

    let (status, _, body) = call(
        &mut app,
        local_post(
            "/api/mcp/servers",
            Some(serde_json::json!({ "name": "weather", "type": "SSE", "url": "http://127.0.0.1:9/sse" }).to_string()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(json_body(&body)["code"], "INTERNAL_ERROR");

    let (status, _, body) = call(
        &mut app,
        local_post("/api/mcp/servers/weather/restart", None),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(json_body(&body)["code"], "INTERNAL_ERROR");
}

/// 体缺失 → 400 `INVALID_REQUEST_BODY`（旧端 Jackson 空体 400 语义）。
#[tokio::test]
async fn add_server_rejects_missing_body() {
    let mut app = app_with_isolated_registry();
    let (status, _, body) = call(&mut app, local_post("/api/mcp/servers", None)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&body)["code"], "INVALID_REQUEST_BODY");
}

/// 必填 query 缺省 → 400 `MISSING_PARAMETER`（Spring
/// `MissingServletRequestParameterException`）。
#[tokio::test]
async fn required_query_params_report_missing_parameter() {
    let mut app = app_with_isolated_registry();

    let (status, _, body) = call(&mut app, local_get("/api/mcp/resources/read")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let payload = json_body(&body);
    assert_eq!(payload["code"], "MISSING_PARAMETER");
    assert_eq!(payload["message"], "Required parameter 'uri' is missing");

    let (status, _, body) = call(&mut app, local_post("/api/mcp/reconnect", None)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let payload = json_body(&body);
    assert_eq!(payload["code"], "MISSING_PARAMETER");
    assert_eq!(payload["message"], "Required parameter 'server' is missing");
}

/// 服务器不存在 → 统一扁平 REST 错误响应。
#[tokio::test]
async fn unknown_server_yields_bare_error_body() {
    let mut app = app_with_isolated_registry();

    let (status, _, body) = call(
        &mut app,
        local_get("/api/mcp/resources/read?uri=file:///a&server=nope"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let payload = json_body(&body);
    assert_eq!(payload["code"], "MCP_REQUEST_FAILED");
    assert_eq!(payload["message"], "MCP server not found: nope");
    assert!(payload["requestId"].is_string());

    let (status, _, body) =
        call(&mut app, local_post("/api/mcp/reconnect?server=nope", None)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let payload = json_body(&body);
    assert_eq!(payload["code"], "MCP_REQUEST_FAILED");
    assert_eq!(payload["message"], "MCP server not found: nope");
    assert!(payload["requestId"].is_string());
}

/// prompt 域的 400 同样使用统一错误结构。
#[tokio::test]
async fn execute_prompt_validation_carries_success_flag() {
    let mut app = app_with_isolated_registry();

    let (status, _, body) = call(
        &mut app,
        local_post(
            "/api/mcp/prompts/execute",
            Some(serde_json::json!({ "server": "weather" }).to_string()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let payload = json_body(&body);
    assert_eq!(payload["code"], "MCP_PROMPT_INVALID");
    assert_eq!(payload["message"], "Prompt name is required");
    assert!(payload["requestId"].is_string());

    let (status, _, body) = call(
        &mut app,
        local_post(
            "/api/mcp/prompts/execute",
            Some(serde_json::json!({ "promptName": "review" }).to_string()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let payload = json_body(&body);
    assert_eq!(payload["code"], "MCP_PROMPT_INVALID");
    assert_eq!(payload["message"], "Server name is required");
    assert!(payload["requestId"].is_string());
}

// ── McpCapabilityController：CRUD 全链路 ───────────────────────────────────

/// 新增 → 查询 → 更新 → 停用 → 删除，逐步核对状态码与计数字段。
#[tokio::test]
async fn capability_crud_round_trip() {
    let mut app = app_with_isolated_registry();

    // 新增：201 + 裸定义回显（`null` 字段省略，对齐 `@JsonInclude(NON_NULL)`）。
    let (status, _, body) = call(
        &mut app,
        local_post(
            "/api/mcp/capabilities",
            Some(capability_body("mcp_weather", true)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let created = json_body(&body);
    assert_eq!(created["id"], "mcp_weather");
    assert_eq!(created["enabled"], true);
    assert!(
        created.get("description").is_none(),
        "null 字段未被省略: {created}"
    );

    // 列表：`total` / `enabledCount` 取自整表。
    let (status, _, body) = call(&mut app, local_get("/api/mcp/capabilities")).await;
    assert_eq!(status, StatusCode::OK);
    let listed = json_body(&body);
    assert_eq!(listed["total"], 1);
    assert_eq!(listed["enabledCount"], 1);
    assert_eq!(listed["capabilities"][0]["id"], "mcp_weather");

    // 过滤：`domain` 命中 / 未命中都回 200，计数字段不随过滤变化。
    let (status, _, body) = call(&mut app, local_get("/api/mcp/capabilities?domain=nope")).await;
    assert_eq!(status, StatusCode::OK);
    let filtered = json_body(&body);
    assert_eq!(filtered["capabilities"].as_array().expect("array").len(), 0);
    assert_eq!(filtered["total"], 1);

    // 功能域清单。
    let (status, _, body) = call(&mut app, local_get("/api/mcp/capabilities/domains")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_body(&body),
        serde_json::json!({ "domains": ["weather"] })
    );

    // 单条读取。
    let (status, _, body) = call(&mut app, local_get("/api/mcp/capabilities/mcp_weather")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["toolName"], "get_forecast");

    // 更新：体缺 `id` 时以 URL id 补齐（偏离 B4B-09）。
    let patch = serde_json::json!({
        "name": "天气查询 v2",
        "toolName": "get_forecast",
        "sseUrl": "http://127.0.0.1:9/weather/sse",
        "domain": "weather",
        "timeoutMs": 20000,
        "enabled": true,
    })
    .to_string();
    let (status, _, body) = call(
        &mut app,
        local_put("/api/mcp/capabilities/mcp_weather", Some(patch)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let updated = json_body(&body);
    assert_eq!(updated["id"], "mcp_weather");
    assert_eq!(updated["name"], "天气查询 v2");
    assert_eq!(updated["timeoutMs"], 20000);

    // 停用：无其他能力共享该 serverKey → 顺带摘除服务器；状态回 `disabled`。
    let (status, _, body) = call(
        &mut app,
        local_patch(
            "/api/mcp/capabilities/mcp_weather/toggle?enabled=false",
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_body(&body),
        serde_json::json!({ "id": "mcp_weather", "enabled": false, "status": "disabled" })
    );

    // 删除 → 再读 404（空体）。
    let (status, _, body) = call(&mut app, local_delete("/api/mcp/capabilities/mcp_weather")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body), serde_json::json!({ "success": true }));

    let (status, _, body) = call(&mut app, local_get("/api/mcp/capabilities/mcp_weather")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.is_empty(), "404 应为空体，实得 {body:?}");
}

/// 缺失 id 的失败体一律为**空体**（旧 `.notFound().build()` /
/// `.badRequest().build()`），前端只判 `resp.ok`。
#[tokio::test]
async fn missing_capability_failures_are_empty_bodies() {
    let mut app = app_with_isolated_registry();

    for request in [
        local_get("/api/mcp/capabilities/ghost"),
        local_delete("/api/mcp/capabilities/ghost"),
        local_get("/api/mcp/capabilities/ghost/server-tools"),
        local_post("/api/mcp/capabilities/ghost/test", None),
    ] {
        let (status, _, body) = call(&mut app, request).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.is_empty(), "404 应为空体，实得 {body:?}");
    }

    // 更新不存在的 id 同样 404 空体（旧 catch(IllegalArgumentException)）。
    let (status, _, body) = call(
        &mut app,
        local_put(
            "/api/mcp/capabilities/ghost",
            Some(capability_body("ghost", false)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.is_empty(), "404 应为空体，实得 {body:?}");

    // id 重复的新增 → 400 空体。
    let (status, _, _) = call(
        &mut app,
        local_post("/api/mcp/capabilities", Some(capability_body("dup", false))),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _, body) = call(
        &mut app,
        local_post("/api/mcp/capabilities", Some(capability_body("dup", false))),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.is_empty(), "400 应为空体，实得 {body:?}");
}

/// `toggle` 的 `enabled` 是 Spring **基元** `boolean`：缺省 400、空串 500、
/// 不可识别 500（见 `http_params::require_spring_bool`）。
#[tokio::test]
async fn toggle_binds_primitive_boolean_like_spring() {
    let mut app = app_with_isolated_registry();
    let (status, _, _) = call(
        &mut app,
        local_post(
            "/api/mcp/capabilities",
            Some(capability_body("mcp_toggle", false)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _, body) = call(
        &mut app,
        local_patch("/api/mcp/capabilities/mcp_toggle/toggle", None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let payload = json_body(&body);
    assert_eq!(payload["code"], "MISSING_PARAMETER");
    assert_eq!(
        payload["message"],
        "Required parameter 'enabled' is missing"
    );

    for uri in [
        "/api/mcp/capabilities/mcp_toggle/toggle?enabled=",
        "/api/mcp/capabilities/mcp_toggle/toggle?enabled=maybe",
    ] {
        let (status, _, body) = call(&mut app, local_patch(uri, None)).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "uri={uri}");
        assert_eq!(json_body(&body)["code"], "INTERNAL_ERROR");
    }

    // `on` 属 Spring 真值集合——启用要经管理器，未启动 → 500 信封。
    let (status, _, body) = call(
        &mut app,
        local_patch("/api/mcp/capabilities/mcp_toggle/toggle?enabled=on", None),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(json_body(&body)["code"], "INTERNAL_ERROR");
}

/// `server-tools`：id 存在但服务器未连接 → 200 `{status:"not_connected"}`。
#[tokio::test]
async fn server_tools_reports_not_connected_without_connection() {
    let mut app = app_with_isolated_registry();
    let (status, _, _) = call(
        &mut app,
        local_post(
            "/api/mcp/capabilities",
            Some(capability_body("mcp_tools", false)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _, body) = call(
        &mut app,
        local_get("/api/mcp/capabilities/mcp_tools/server-tools"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let payload = json_body(&body);
    assert_eq!(payload["id"], "mcp_tools");
    assert_eq!(payload["status"], "not_connected");
    // `extractServerKey` 取 URL 倒数第二段（`/weather/sse` → `weather`）。
    assert_eq!(payload["serverKey"], "weather");
}

/// `invoke`：`arguments` 非对象 → 200 + `{status:"error"}`（偏离 B4B-13）。
#[tokio::test]
async fn invoke_rejects_non_object_arguments_with_error_status() {
    let mut app = app_with_isolated_registry();
    let (status, _, _) = call(
        &mut app,
        local_post(
            "/api/mcp/capabilities",
            Some(capability_body("mcp_invoke", false)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _, body) = call(
        &mut app,
        local_post(
            "/api/mcp/capabilities/mcp_invoke/invoke",
            Some(serde_json::json!({ "arguments": "oops" }).to_string()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_body(&body),
        serde_json::json!({
            "id": "mcp_invoke",
            "status": "error",
            "error": "Invalid 'arguments': expected a JSON object",
        })
    );
}
