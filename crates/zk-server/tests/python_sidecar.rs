//! 2.6 Python 侧车集成测试——真实 `UnixListener` 上跑 `FastAPI` 契约 stub，
//! 逐条验证 UDS 传输、能力域门控与三件桥接工具的降级矩阵。
//!
//! 为什么用 stub 而非真实 uvicorn：CI 无 Python 依赖（playwright / tree-sitter /
//! gitpython 均为 python-service 的可选依赖），而本层要验证的是 **Rust 侧**
//! 的传输 / 门控 / 降级契约。真实进程生命周期（启动、`kill` 后自动重启、
//! 优雅停止）以手工实测证据记录于 `docs/compatibility.md` §6。

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use axum::extract::Path as AxumPath;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router as AxumRouter};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use zk_server::config::Config;
use zk_server::python::{CodeIntelTool, GitEnhancedTool, PythonClient, WebBrowserTool};
use zk_tools::{Tool, ToolContext, ToolOutput};

use common::{app_with_config, call, json_body, local_get, local_post, local_with_headers};

/// 每个测试独占 socket 路径（同进程并发跑测试不互踩）。
fn unique_socket(tag: &str) -> PathBuf {
    let unique = format!(
        "zkcode-py-{tag}-{}-{:?}.sock",
        std::process::id(),
        std::thread::current().id()
    );
    std::env::temp_dir().join(unique.replace(['(', ')', ' '], ""))
}

/// `FastAPI` 契约 stub（`python-service/src/main.py` 的端点子集）。
///
/// `browser_available` 用于构造「侧车在线但能力域不可用」——即 playwright
/// 未安装的真实生产形态，验证 `BROWSER_AUTOMATION_UNAVAILABLE` 降级路径。
fn stub_router(browser_available: bool) -> Router {
    let capabilities = json!({
        "CODE_INTEL": { "name": "代码智能", "available": true, "reason": null },
        "GIT_ENHANCED": { "name": "Git 增强", "available": true, "reason": null },
        "FILE_PROCESSING": { "name": "文件处理", "available": true, "reason": null },
        "BROWSER_AUTOMATION": {
            "name": "浏览器自动化",
            "available": browser_available,
            "reason": if browser_available { Value::Null } else { json!("playwright not installed") },
        },
        "CODE_QUALITY": { "name": "代码质量", "available": true, "reason": null },
        "ANALYSIS": { "name": "分析", "available": true, "reason": null },
        "HTTP_API": { "name": "HTTP API", "available": true, "reason": null },
    });
    AxumRouter::new()
        .route(
            "/api/health",
            get(|| async { Json(json!({ "status": "healthy", "service": "python-service" })) }),
        )
        .route(
            "/api/health/capabilities",
            get(move || {
                let capabilities = capabilities.clone();
                async move { Json(capabilities) }
            }),
        )
        .route(
            "/api/browser/{action}",
            post(
                |AxumPath(action): AxumPath<String>, Json(body): Json<Value>| async move {
                    Json(json!({
                        "success": true,
                        "data": { "action": action, "echo": body },
                        "error_code": Value::Null,
                        "error_message": Value::Null,
                    }))
                },
            ),
        )
        .route(
            "/api/code-intel/{action}",
            post(
                |AxumPath(action): AxumPath<String>, Json(body): Json<Value>| async move {
                    Json(json!({
                        "success": true,
                        "data": { "action": action, "symbols": ["main"], "echo": body },
                        "error_code": Value::Null,
                        "error_message": Value::Null,
                    }))
                },
            ),
        )
        .route(
            "/api/git/{action}",
            post(
                |AxumPath(action): AxumPath<String>, Json(body): Json<Value>| async move {
                    if body.get("repo_path").and_then(Value::as_str) == Some("/nonexistent") {
                        return Json(json!({
                            "success": false,
                            "data": Value::Null,
                            "error_code": "REPO_NOT_FOUND",
                            "error_message": "not a git repository",
                        }));
                    }
                    Json(json!({
                        "success": true,
                        "data": { "action": action, "entries": [] },
                        "error_code": Value::Null,
                        "error_message": Value::Null,
                    }))
                },
            ),
        )
        // ── 反向代理面（四前缀）：回显方法 / 目标 / query / 体 / 关联头，
        // 供代理契约逐项断言。
        .route("/api/tokenizer/count", post(echo_proxy))
        .route("/api/code-quality/complexity", post(echo_proxy))
        .route("/api/files/tree", post(echo_proxy))
        .route("/api/files/analysis/summary", post(echo_proxy))
        .route("/api/analysis/openapi/merged", get(echo_proxy))
        .route("/api/git/log/detail", get(echo_proxy))
        .route("/api/code-quality/broken", post(broken_proxy))
}

/// 代理回显端点：把侧车真实收到的内容原样送回，便于断言「原样转发」。
async fn echo_proxy(
    method: axum::http::Method,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    body: String,
) -> Json<Value> {
    Json(json!({
        "method": method.as_str(),
        "path": uri.path(),
        "query": uri.query(),
        "body": body,
        "session_id": headers
            .get("x-session-id")
            .and_then(|value| value.to_str().ok()),
        "content_type": headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
    }))
}

/// 侧车业务错误端点：验证 4xx 与非 JSON `Content-Type` 亦逐字回传。
async fn broken_proxy() -> impl axum::response::IntoResponse {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )],
        "unsupported language",
    )
}

/// 在给定 socket 上拉起 stub，返回可 abort 的句柄。`bind` 是同步的，函数
/// 返回即已监听成功，后续客户端调用不存在竞态（无需额外等待）。
fn spawn_stub(socket: &Path, browser_available: bool) -> tokio::task::JoinHandle<()> {
    let _ = std::fs::remove_file(socket);
    let listener = tokio::net::UnixListener::bind(socket).expect("bind uds");
    let router = stub_router(browser_available);
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    })
}

fn ctx() -> ToolContext {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    ToolContext::new(CancellationToken::new(), tx)
}

/// 侧车在线：能力清单 7 域解析到位，三件桥接工具经 UDS 真实往返成功。
#[tokio::test]
async fn bridge_tools_round_trip_over_uds() {
    let socket = unique_socket("ok");
    let stub = spawn_stub(&socket, true);
    let client = Arc::new(PythonClient::new(socket.clone()));

    assert!(client.is_healthy().await, "stub /api/health answers");
    client.refresh_capabilities().await;
    assert!(client.last_refresh_succeeded());
    let capabilities = client.capabilities();
    assert_eq!(capabilities.len(), 7, "7 capability domains (main.py 契约)");
    assert!(client.is_capability_available("CODE_INTEL").await);
    assert!(client.is_capability_available("BROWSER_AUTOMATION").await);
    assert!(
        !client.is_capability_available("NO_SUCH_DOMAIN").await,
        "未知能力域恒不可用"
    );

    let code_intel = CodeIntelTool::new(client.clone());
    let out = code_intel
        .execute(
            json!({ "action": "symbols", "content": "fn main() {}", "language": "Rust" }),
            ctx(),
        )
        .await;
    assert!(!out.is_error, "code intel ok: {}", out.content);
    assert!(out.content.contains("\"symbols\""));
    // 语言大小写归一（旧 `.toLowerCase()`）：`Rust` → `rust` 落到请求体。
    assert!(out.content.contains("\"language\":\"rust\""));

    let git = GitEnhancedTool::new(client.clone());
    let out = git
        .execute(json!({ "action": "log", "repo_path": "/tmp/repo" }), ctx())
        .await;
    assert!(!out.is_error, "git enhanced ok: {}", out.content);
    assert!(out.content.contains("\"entries\""));

    let browser = WebBrowserTool::new(client.clone());
    let out = browser
        .execute(
            json!({ "action": "navigate", "url": "https://example.com" }),
            ctx(),
        )
        .await;
    assert!(!out.is_error, "browser navigate ok: {}", out.content);
    assert!(out.content.contains("\"navigate\""));
    // session_id 兜底：入参未给且 ctx 无会话 → `"default"`（旧端会写 JSON
    // null 触发 Pydantic 422，此处为 MUST_FIX 修正点）。
    assert!(out.content.contains("\"session_id\":\"default\""));

    stub.abort();
    let _ = std::fs::remove_file(&socket);
}

/// 侧车在线但能力域不可用（playwright 未装）：`WebBrowser` 优雅降级，
/// 其余能力域工具不受影响——能力降级矩阵第三行。
#[tokio::test]
async fn unavailable_capability_degrades_only_its_own_tool() {
    let socket = unique_socket("nobrowser");
    let stub = spawn_stub(&socket, false);
    let client = Arc::new(PythonClient::new(socket.clone()));
    client.refresh_capabilities().await;

    let browser = WebBrowserTool::new(client.clone());
    let out = browser
        .execute(
            json!({ "action": "navigate", "url": "https://example.com" }),
            ctx(),
        )
        .await;
    assert!(out.is_error);
    assert!(
        out.content.starts_with("BROWSER_AUTOMATION_UNAVAILABLE: "),
        "降级文案首段为错误码：{}",
        out.content
    );

    let code_intel = CodeIntelTool::new(client.clone());
    let out = code_intel
        .execute(
            json!({ "action": "parse", "content": "x = 1", "language": "python" }),
            ctx(),
        )
        .await;
    assert!(!out.is_error, "CODE_INTEL 仍可用：{}", out.content);

    stub.abort();
    let _ = std::fs::remove_file(&socket);
}

/// 无 Python（socket 不存在）：三件桥接工具全部优雅降级，不 panic 不挂起
/// ——能力降级矩阵第二行，「无 Python 时核心对话正常」的工具侧证据。
#[tokio::test]
async fn absent_sidecar_degrades_every_bridge_tool() {
    let socket = unique_socket("absent");
    let _ = std::fs::remove_file(&socket);
    let client = Arc::new(PythonClient::new(socket.clone()));

    assert!(!client.is_healthy().await);
    client.refresh_capabilities().await;
    assert!(!client.last_refresh_succeeded());
    assert!(client.capabilities().is_empty());

    let expectations: [(Box<dyn Tool>, Value, &str); 3] = [
        (
            Box::new(WebBrowserTool::new(client.clone())),
            json!({ "action": "navigate", "url": "https://example.com" }),
            "BROWSER_AUTOMATION_UNAVAILABLE: ",
        ),
        (
            Box::new(CodeIntelTool::new(client.clone())),
            json!({ "action": "parse", "content": "x = 1", "language": "python" }),
            "CODE_INTEL_UNAVAILABLE: ",
        ),
        (
            Box::new(GitEnhancedTool::new(client.clone())),
            json!({ "action": "log", "repo_path": "/tmp/repo" }),
            "GIT_ANALYSIS_UNAVAILABLE: ",
        ),
    ];
    for (tool, input, prefix) in expectations {
        let out: ToolOutput = tool.execute(input, ctx()).await;
        assert!(out.is_error, "{} must degrade", tool.name());
        assert!(
            out.content.starts_with(prefix),
            "{} degradation text: {}",
            tool.name(),
            out.content
        );
    }
}

/// 入参校验在**调用侧车之前**发生（socket 不存在也照样报校验错误），
/// 逐条对齐旧 `validateInput` 的先验语义。
#[tokio::test]
async fn input_validation_precedes_sidecar_call() {
    let client = Arc::new(PythonClient::new(unique_socket("validate")));
    let browser = WebBrowserTool::new(client.clone());
    for (input, prefix) in [
        (json!({ "action": "teleport" }), "INVALID_ACTION: "),
        (json!({ "action": "navigate" }), "MISSING_URL: "),
        (
            json!({ "action": "navigate", "url": "file:///etc/passwd" }),
            "UNSAFE_PROTOCOL: ",
        ),
        (json!({ "action": "click" }), "MISSING_SELECTOR: "),
    ] {
        let out = browser.execute(input, ctx()).await;
        assert!(out.is_error);
        assert!(out.content.starts_with(prefix), "got {}", out.content);
    }

    let git = GitEnhancedTool::new(client.clone());
    let out = git.execute(json!({ "action": "log" }), ctx()).await;
    assert!(out.content.starts_with("MISSING_REPO_PATH: "));

    let code_intel = CodeIntelTool::new(client);
    let out = code_intel
        .execute(
            json!({ "action": "parse", "content": "x", "language": "cobol" }),
            ctx(),
        )
        .await;
    assert!(out.content.starts_with("CODE_INTEL_LANGUAGE_UNSUPPORTED: "));
}

/// 侧车业务失败（`success: false`）沿用 Python 的 `error_code` /
/// `error_message`，逐条对齐旧 `GitTool` :168-173。
#[tokio::test]
async fn sidecar_business_failure_propagates_python_error_code() {
    let socket = unique_socket("bizfail");
    let stub = spawn_stub(&socket, true);
    let client = Arc::new(PythonClient::new(socket.clone()));
    client.refresh_capabilities().await;

    let git = GitEnhancedTool::new(client);
    let out = git
        .execute(
            json!({ "action": "log", "repo_path": "/nonexistent" }),
            ctx(),
        )
        .await;
    assert!(out.is_error);
    assert_eq!(out.content, "REPO_NOT_FOUND: not a git repository");

    stub.abort();
    let _ = std::fs::remove_file(&socket);
}

/// `/api/health` 聚合：`python` 子系统两键形状；侧车 DOWN **不拉低** overall
/// （UP + HTTP 200），核心链路可用性不因 Python 缺失而下降。
#[tokio::test]
async fn health_reports_python_without_lowering_overall() {
    let mut config = Config::test_config();
    config.python_enabled = true;
    config.python_uds_path = unique_socket("health");
    let (mut router, _db) = app_with_config(config);
    let (status, _headers, body) = call(&mut router, local_get("/api/health")).await;
    assert_eq!(status, StatusCode::OK);
    let health = json_body(&body);
    assert_eq!(health["status"], "UP", "python DOWN 不影响 overall");
    let python = &health["subsystems"]["python"];
    assert_eq!(python["status"], "DOWN");
    assert!(
        python["message"]
            .as_str()
            .is_some_and(|m| m.contains("capabilities 0/0 available")),
        "message: {python:?}"
    );
}

/// 侧车总开关关闭 → `python` 子系统 `DISABLED`，其余子系统不变。
#[tokio::test]
async fn health_reports_disabled_when_sidecar_switched_off() {
    let (mut router, _db) = app_with_config(Config::test_config());
    let (status, _headers, body) = call(&mut router, local_get("/api/health")).await;
    assert_eq!(status, StatusCode::OK);
    let health = json_body(&body);
    assert_eq!(health["subsystems"]["python"]["status"], "DISABLED");
    assert_eq!(health["subsystems"]["database"]["status"], "UP");
}

// ───── 反向代理四前缀（回归修复：前端面板此前直接 ECONNREFUSED）─────

/// 侧车在线：四条前缀全部原样转发（方法 / 路径 / query / JSON 体 / 会话头），
/// 响应状态码与体逐字回传。
#[tokio::test]
async fn proxy_forwards_four_prefixes_verbatim() {
    let socket = unique_socket("proxy-ok");
    let stub = spawn_stub(&socket, true);
    let mut config = Config::test_config();
    config.python_enabled = true;
    config.python_uds_path = socket.clone();
    let (mut router, _db) = app_with_config(config);

    for path in [
        "/api/tokenizer/count",
        "/api/code-quality/complexity",
        "/api/files/tree",
        "/api/files/analysis/summary",
    ] {
        let (status, headers, body) = call(
            &mut router,
            local_post(path, Some(r#"{"text":"hello"}"#.to_owned())),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path} must be proxied");
        assert_eq!(
            headers
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json"),
            "{path} keeps the sidecar content type"
        );
        let echoed = json_body(&body);
        assert_eq!(echoed["method"], "POST");
        assert_eq!(echoed["path"], path, "path reaches the sidecar unchanged");
        assert_eq!(echoed["body"], r#"{"text":"hello"}"#);
        assert_eq!(echoed["content_type"], "application/json");
    }

    // API 契约面板的 GET 端点同样直通。
    let (status, _headers, body) =
        call(&mut router, local_get("/api/analysis/openapi/merged")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["path"], "/api/analysis/openapi/merged");

    // GET + query：查询串必须原样带到侧车（面板的分页/limit 依赖它）。
    let (status, _headers, body) =
        call(&mut router, local_get("/api/git/log/detail?limit=20")).await;
    assert_eq!(status, StatusCode::OK);
    let echoed = json_body(&body);
    assert_eq!(echoed["method"], "GET");
    assert_eq!(echoed["path"], "/api/git/log/detail");
    assert_eq!(echoed["query"], "limit=20");
    assert_eq!(echoed["body"], "", "GET 无体转发");

    stub.abort();
    let _ = std::fs::remove_file(&socket);
}

/// `X-Session-Id` 折叠为关联头随转发发出（两端日志可对账）。
#[tokio::test]
async fn proxy_forwards_session_correlation_header() {
    let socket = unique_socket("proxy-corr");
    let stub = spawn_stub(&socket, true);
    let mut config = Config::test_config();
    config.python_enabled = true;
    config.python_uds_path = socket.clone();
    let (mut router, _db) = app_with_config(config);

    let (status, _headers, body) = call(
        &mut router,
        local_with_headers(
            "/api/tokenizer/count",
            axum::http::Method::POST,
            Some("{}".to_owned()),
            &[("x-session-id", "sess-42")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["session_id"], "sess-42");

    stub.abort();
    let _ = std::fs::remove_file(&socket);
}

/// 侧车的非 2xx 与非 JSON `Content-Type` 亦逐字回传（代理不改写语义）。
#[tokio::test]
async fn proxy_passes_sidecar_error_status_through() {
    let socket = unique_socket("proxy-4xx");
    let stub = spawn_stub(&socket, true);
    let mut config = Config::test_config();
    config.python_enabled = true;
    config.python_uds_path = socket.clone();
    let (mut router, _db) = app_with_config(config);

    let (status, headers, body) = call(
        &mut router,
        local_post("/api/code-quality/broken", Some("{}".to_owned())),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/plain; charset=utf-8")
    );
    assert_eq!(String::from_utf8_lossy(&body), "unsupported language");

    stub.abort();
    let _ = std::fs::remove_file(&socket);
}

/// 侧车未启动（socket 缺席）：503 + 错误信封，不 panic、不挂起。
#[tokio::test]
async fn proxy_returns_503_when_sidecar_absent() {
    let socket = unique_socket("proxy-absent");
    let _ = std::fs::remove_file(&socket);
    let mut config = Config::test_config();
    config.python_enabled = true;
    config.python_uds_path = socket;
    let (mut router, _db) = app_with_config(config);

    let (status, _headers, body) = call(
        &mut router,
        local_post("/api/tokenizer/count", Some("{}".to_owned())),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let envelope = json_body(&body);
    assert_eq!(envelope["code"], "PYTHON_SERVICE_UNAVAILABLE");
    assert!(envelope["requestId"].as_str().is_some());
}

/// 侧车总开关关闭：503 `PYTHON_SERVICE_DISABLED`（不做无谓的连接尝试）。
#[tokio::test]
async fn proxy_returns_503_when_sidecar_disabled() {
    let (mut router, _db) = app_with_config(Config::test_config());
    let (status, _headers, body) = call(
        &mut router,
        local_post("/api/git/diff", Some("{}".to_owned())),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json_body(&body)["code"], "PYTHON_SERVICE_DISABLED");
}

/// 代理不越界：`/api/files/search`（会话感知的后端能力）与其余前缀外路径
/// 不落到代理 handler；`PUT` 一类方法 405。
#[tokio::test]
async fn proxy_does_not_capture_neighbouring_routes() {
    let mut config = Config::test_config();
    config.python_enabled = true;
    config.python_uds_path = unique_socket("proxy-scope");
    let (mut router, _db) = app_with_config(config);

    // `/api/files/search` 现有真实后端 handler（Batch 2 Step 2-4）：缺 sessionId
    // 时 400 `MISSING_PARAMETER`（旧 FileController 语义），足证未落代理（代理在
    // 侧车缺席时回 503）。`/api/files/safe-read` 无对应路由 → 404。
    for (path, expected) in [
        ("/api/files/search?query=readme", StatusCode::BAD_REQUEST),
        ("/api/files/safe-read", StatusCode::NOT_FOUND),
    ] {
        let (status, _headers, _body) = call(&mut router, local_get(path)).await;
        assert_eq!(status, expected, "{path} must not reach the python proxy");
    }
    let (status, _headers, _body) = call(
        &mut router,
        local_with_headers(
            "/api/tokenizer/count",
            axum::http::Method::PUT,
            Some("{}".to_owned()),
            &[],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}
