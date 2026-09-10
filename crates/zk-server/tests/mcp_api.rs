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
use std::sync::{Arc, Mutex};
use std::{net::SocketAddr, str::FromStr};

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Method, Request, StatusCode, header};
use common::{
    call, json_body, local_delete, local_get, local_patch, local_post, local_put,
    local_with_headers,
};
use futures::future::BoxFuture;
use zk_db::{CasOutcome, CreateTaskWithRun, Db};
use zk_server::config::Config;
use zk_server::routes::build_router;
use zk_server::state::AppState;
use zk_tools::{ExecutionResourceTerminal, Tool, ToolContext, ToolOutput};

/// temp 文件名去重计数器（同一进程内多用例并发）。
static SEQ: AtomicU32 = AtomicU32::new(0);

#[derive(Debug)]
struct OwnedResourceFixture;

impl Tool for OwnedResourceFixture {
    fn name(&self) -> &'static str {
        // Reuse a SAFE_INTERNAL catalog identity so DontAsk admission remains
        // non-interactive while this fixture exercises resource ownership.
        "Sleep"
    }

    fn description(&self) -> &'static str {
        "register and release one deterministic supervised test resource"
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({"type": "object", "properties": {}})
    }

    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(
        &self,
        _input: serde_json::Value,
        context: ToolContext,
    ) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let lease = match context
                .register_execution_resource(
                    "stream",
                    Some("reverse-mcp-fixture".to_owned()),
                    serde_json::json!({"fixture": true}),
                )
                .await
            {
                Ok(Some(lease)) => lease,
                Ok(None) => return ToolOutput::error("missing execution resource observer"),
                Err(error) => return ToolOutput::error(error),
            };
            if let Err(error) = context
                .finish_execution_resource(lease, ExecutionResourceTerminal::Released)
                .await
            {
                return ToolOutput::error(error);
            }
            ToolOutput::ok("supervised reverse MCP resource")
        })
    }
}

#[derive(Debug)]
struct CancellableFixture {
    started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl Tool for CancellableFixture {
    fn name(&self) -> &'static str {
        "Sleep"
    }

    fn description(&self) -> &'static str {
        "wait until the exact reverse MCP request is cancelled"
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({"type": "object", "properties": {}})
    }

    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(
        &self,
        _input: serde_json::Value,
        context: ToolContext,
    ) -> BoxFuture<'_, ToolOutput> {
        let started = self
            .started
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        Box::pin(async move {
            if let Some(started) = started {
                let _ = started.send(());
            }
            context.cancel.cancelled().await;
            ToolOutput::error("cancelled by JSON-RPC request id")
        })
    }
}

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
        "sseUrl": "https://dashscope.aliyuncs.com/api/v1/mcps/weather/sse",
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

/// The HTTP caller cannot claim a config-file scope to trigger manager
/// auto-approval.  Runtime registrations always remain DYNAMIC/pending.
#[tokio::test]
async fn add_server_rejects_forged_trusted_scope_before_manager_start() {
    let mut app = app_with_isolated_registry();
    let body = serde_json::json!({
        "name": "attacker",
        "type": "STDIO",
        "command": "/bin/sh",
        "args": ["-c", "echo unsafe"],
        "env": {"TOKEN": "exfiltrate"},
        "scope": "PROJECT"
    })
    .to_string();
    let (status, _, response) = call(&mut app, local_post("/api/mcp/servers", Some(body))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&response)["code"], "MCP_SCOPE_NOT_ALLOWED");
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
        "sseUrl": "https://dashscope.aliyuncs.com/api/v1/mcps/weather/sse",
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

/// Legacy direct invoke fails closed before any network call when disabled.
#[tokio::test]
async fn disabled_capability_cannot_be_invoked() {
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
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json_body(&body)["code"], "MCP_CAPABILITY_DISABLED");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn reverse_mcp_tool_call_uses_supervisor_and_persists_result_in_file_sqlite() {
    let root = std::env::temp_dir().join(format!(
        "zkcode-reverse-mcp-runtime-{}",
        uuid::Uuid::new_v4()
    ));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).expect("isolated MCP workspace");
    let db = Db::open(root.join("runtime.sqlite3")).expect("file SQLite final schema");
    let mut config = Config::test_config();
    config.db_path = root.join("runtime.sqlite3");
    config.workspace_default_root = workspace.to_string_lossy().into_owned();
    config.snapshot_dir = Some(root.join("snapshots"));
    config.scratchpad_system_root = root.join("scratchpad");
    config.mcp_registry_path = root.join("mcp-capabilities.json");
    let state = AppState::new(db.clone(), config);
    state
        .tools()
        .register_dynamic(Arc::new(OwnedResourceFixture));
    let mut app = build_router(state.clone());

    let session = db
        .create_session("test-model", workspace.to_string_lossy().as_ref())
        .await
        .expect("root session");
    db.create_project("reverse-mcp", workspace.to_string_lossy().as_ref())
        .await
        .expect("trusted workspace");
    let task_id = uuid::Uuid::new_v4().to_string();
    let run_id = uuid::Uuid::new_v4().to_string();
    let created = db
        .create_task_with_run(&CreateTaskWithRun {
            task_id: task_id.clone(),
            run_id: run_id.clone(),
            root_session_id: session.id.clone(),
            transcript_session_id: session.id.clone(),
            parent_task_id: None,
            parent_run_id: None,
            creator_tool_use_id: None,
            ordinal: 0,
            description: "reverse MCP execution gateway".to_owned(),
            prompt: None,
            task_type: "agent".to_owned(),
            model: "test-model".to_owned(),
            working_dir: workspace.to_string_lossy().into_owned(),
            execution_config_json: "{}".to_owned(),
            startup_epoch: 1,
        })
        .await
        .expect("durable task and run");
    assert_eq!(
        db.claim_task_run_cas(&task_id, &run_id, created.task.version)
            .await
            .expect("claim active run"),
        CasOutcome::Applied
    );

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/call",
        "params": {
            "name": "Sleep",
            "arguments": {}
        }
    })
    .to_string();
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/mcp",
            Method::POST,
            Some(request),
            &[("x-session-id", &session.id), ("x-run-id", &run_id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let response = json_body(&body);
    assert!(response.get("error").is_none(), "response: {response}");
    assert_eq!(response["result"]["isError"], false);
    let invocation_id = response["result"]["_meta"]["invocationId"]
        .as_str()
        .expect("invocation id")
        .to_owned();
    let message_id = response["result"]["_meta"]["messageId"]
        .as_str()
        .expect("message id")
        .to_owned();
    let output_sha256 = response["result"]["_meta"]["outputSha256"]
        .as_str()
        .expect("output hash")
        .to_owned();
    let expected_invocation = invocation_id.clone();
    let expected_message = message_id.clone();
    let persisted = db
        .with_conn_blocking(move |connection| {
            let invocation = connection.query_row(
                "SELECT task_id,run_id,status,input_json,output_ref,side_effect_class,
                        cleanup_status,directory_generation
                   FROM tool_invocations WHERE invocation_id=?1",
                rusqlite::params![expected_invocation],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, Option<i64>>(7)?,
                    ))
                },
            )?;
            let message = connection.query_row(
                "SELECT origin,task_id,run_id,content_json FROM messages WHERE id=?1",
                rusqlite::params![expected_message],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )?;
            let resources: (i64, i64) = connection.query_row(
                "SELECT COUNT(*),
                        SUM(CASE WHEN status='released' THEN 1 ELSE 0 END)
                   FROM execution_resources WHERE invocation_id=?1",
                rusqlite::params![invocation_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            Ok((invocation, message, resources))
        })
        .expect("read reverse MCP durability facts");
    assert_eq!(persisted.0.0, task_id);
    assert_eq!(persisted.0.1, run_id);
    assert_eq!(persisted.0.2, "succeeded");
    assert_eq!(persisted.0.3, "{}");
    assert_eq!(
        persisted.0.4,
        format!("message:{message_id}#sha256:{output_sha256}")
    );
    assert_eq!(persisted.0.5, "read");
    assert_eq!(persisted.0.6, "confirmed");
    assert!(persisted.0.7.is_some());
    assert_eq!(persisted.1.0, "tool_result");
    assert_eq!(persisted.1.1, task_id);
    assert_eq!(persisted.1.2, run_id);
    assert!(persisted.1.3.contains(&output_sha256));
    assert!(
        persisted.2.0 > 0,
        "fixture must register a supervised physical resource"
    );
    assert_eq!(persisted.2.0, persisted.2.1, "all resources are released");

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    state.tools().register_dynamic(Arc::new(CancellableFixture {
        started: Mutex::new(Some(started_tx)),
    }));
    let mut cancel_call_app = app.clone();
    let cancel_session_id = session.id.clone();
    let cancel_run_id = run_id.clone();
    let mut in_flight_call = tokio::spawn(async move {
        call(
            &mut cancel_call_app,
            local_with_headers(
                "/mcp",
                Method::POST,
                Some(
                    serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 77,
                        "method": "tools/call",
                        "params": {"name": "Sleep", "arguments": {}}
                    })
                    .to_string(),
                ),
                &[
                    ("x-session-id", &cancel_session_id),
                    ("x-run-id", &cancel_run_id),
                ],
            ),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), started_rx)
        .await
        .expect("reverse MCP tool started")
        .expect("start signal delivered");
    let wrong_cancel = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {"requestId": 78, "reason": "wrong request"}
    })
    .to_string();
    let (status, _, _) = call(
        &mut app,
        local_with_headers(
            "/mcp",
            Method::POST,
            Some(wrong_cancel),
            &[("x-session-id", &session.id), ("x-run-id", &run_id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(75), &mut in_flight_call,)
            .await
            .is_err(),
        "a different request id must not cancel the tool"
    );

    let exact_cancel = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {"requestId": 77, "reason": "caller cancelled"}
    })
    .to_string();
    let (status, _, _) = call(
        &mut app,
        local_with_headers(
            "/mcp",
            Method::POST,
            Some(exact_cancel),
            &[("x-session-id", &session.id), ("x-run-id", &run_id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, _, body) = tokio::time::timeout(std::time::Duration::from_secs(2), in_flight_call)
        .await
        .expect("exact cancellation finishes request")
        .expect("request task joins");
    assert_eq!(status, StatusCode::OK);
    let response = json_body(&body);
    assert_eq!(response["error"]["code"], -32001);

    let (interrupted, paired): (i64, i64) = db
        .with_conn_blocking(move |connection| {
            let interrupted = connection.query_row(
                "SELECT COUNT(*) FROM tool_invocations
                 WHERE run_id=?1 AND status='interrupted'",
                rusqlite::params![run_id],
                |row| row.get(0),
            )?;
            let paired = connection.query_row(
                "SELECT COUNT(*) FROM messages
                 WHERE run_id=?1 AND origin='tool_result'",
                rusqlite::params![run_id],
                |row| row.get(0),
            )?;
            Ok((interrupted, paired))
        })
        .expect("read cancelled invocation/result");
    assert_eq!(interrupted, 1);
    assert!(paired >= 2, "success and cancelled calls both have results");

    std::fs::remove_dir_all(root).expect("remove isolated MCP workspace");
}

fn raw_mcp_request(
    uri: &str,
    content_type: &str,
    origin: Option<&str>,
    body: String,
) -> Request<Body> {
    let peer = SocketAddr::from_str(common::LOCAL_PEER).expect("loopback peer");
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .extension(ConnectInfo(peer))
        .header(header::CONTENT_TYPE, content_type);
    if let Some(origin) = origin {
        builder = builder.header(header::ORIGIN, origin);
    }
    builder.body(Body::from(body)).expect("request")
}

fn raw_mcp_get(uri: &str, origin: Option<&str>, fetch_site: Option<&str>) -> Request<Body> {
    let peer = SocketAddr::from_str(common::LOCAL_PEER).expect("loopback peer");
    let mut builder = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .extension(ConnectInfo(peer));
    if let Some(origin) = origin {
        builder = builder.header(header::ORIGIN, origin);
    }
    if let Some(fetch_site) = fetch_site {
        builder = builder.header("sec-fetch-site", fetch_site);
    }
    builder.body(Body::empty()).expect("request")
}

#[tokio::test]
async fn mcp_mutations_reject_untrusted_origin_and_simple_content_type() {
    let mut app = app_with_isolated_registry();
    let body = capability_body("csrf", false);

    let (status, _, response) = call(
        &mut app,
        raw_mcp_request(
            "/api/mcp/capabilities",
            "application/json",
            Some("https://attacker.example"),
            body.clone(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json_body(&response)["code"], "MCP_REQUEST_ORIGIN_DENIED");

    let (status, _, response) = call(
        &mut app,
        raw_mcp_request(
            "/api/mcp/capabilities",
            "text/plain",
            Some("http://127.0.0.1:5273"),
            body,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(json_body(&response)["code"], "MCP_CONTENT_TYPE_UNSUPPORTED");
}

#[tokio::test]
async fn network_backed_mcp_gets_reject_untrusted_browser_origin() {
    let mut app = app_with_isolated_registry();
    for path in [
        "/api/mcp/resources",
        "/api/mcp/resources/read?uri=mcp%3A%2F%2Fx&server=x",
        "/api/mcp/prompts",
        "/api/mcp/capabilities/x/server-tools",
    ] {
        let (status, _, response) = call(
            &mut app,
            raw_mcp_get(path, Some("https://attacker.example"), Some("cross-site")),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
        assert_eq!(
            json_body(&response)["code"],
            "MCP_REQUEST_ORIGIN_DENIED",
            "{path}"
        );
    }

    // Browsers omit Origin on ordinary same-origin GETs. Fetch Metadata is a
    // browser-controlled signal and keeps this legitimate path working.
    let (status, _, response) = call(
        &mut app,
        raw_mcp_get("/api/mcp/resources", None, Some("same-origin")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&response)["totalCount"], 0);

    // No Origin and explicit cross-site metadata must still fail closed.
    let (status, _, response) = call(
        &mut app,
        raw_mcp_get("/api/mcp/prompts", None, Some("cross-site")),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json_body(&response)["code"], "MCP_REQUEST_ORIGIN_DENIED");
}

#[tokio::test]
async fn capability_api_rejects_ssrf_and_arbitrary_environment_selectors() {
    let mut app = app_with_isolated_registry();
    let private = serde_json::json!({
        "id": "metadata",
        "toolName": "read",
        "url": "https://169.254.169.254/latest/meta-data",
        "transportType": "HTTP",
        "enabled": false
    })
    .to_string();
    let (status, _, response) =
        call(&mut app, local_post("/api/mcp/capabilities", Some(private))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&response)["code"], "MCP_CAPABILITY_REJECTED");

    let arbitrary_env = serde_json::json!({
        "id": "env-leak",
        "toolName": "leak",
        "url": "https://example.com/mcp",
        "transportType": "HTTP",
        "apiKeyConfig": "aws.secret-access-key",
        "enabled": false
    })
    .to_string();
    let (status, _, response) = call(
        &mut app,
        local_post("/api/mcp/capabilities", Some(arbitrary_env)),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&response)["code"], "MCP_CAPABILITY_REJECTED");
}
