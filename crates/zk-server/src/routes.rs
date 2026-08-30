//! Router 组装与中间件栈。
//!
//! 层序（请求流向，后 `.layer` 者为外层）：handler → `observe`（最内，含
//! `MatchedPath`）→ `access_guard` → CORS → `CatchPanic`（最外）。CORS
//! 在鉴权之外，浏览器预检（OPTIONS）无需通过准入守卫即可应答。
//!
//! Python 侧车反向代理前缀（`/api/tokenizer`、`/api/code-quality`、
//! `/api/files/analysis` + `/api/files/tree`、`/api/analysis`、`/api/git`）
//! 以通配路由挂在同一
//! 棵路由树上，与业务端点共用中间件栈（准入守卫 + 观测 + CORS）；转发
//! 语义与降级矩阵见 [`crate::python::proxy`]。刻意不用 `nest`：`nest` 会把
//! 前缀从内层 `Request::uri` 上剥掉，而代理 handler 需要**完整** origin-form
//! 目标才能原样转发。
//!
//! CORS 使用 loopback 白名单并允许通过 `ZK_CORS_ALLOWED_ORIGINS` 追加本地
//! 开发来源；methods GET/POST/PUT/DELETE/OPTIONS；
//! headers Authorization / Content-Type / X-Session-Id；credentials；expose
//! Set-Cookie / X-Session-Id；maxAge 3600。
//!
//! 静态资源（Batch 2b Step 2b-7）以 `ServeDir` 挂 fallback：未命中任何 API
//! 路由时按 `ZK_STATIC_DIR` 找文件，缺失则 404。刻意用 fallback 而非
//! `nest_service("/", ..)`——后者会抢占根路径的路由匹配。fallback 亦被
//! `Router::layer` 覆盖，故 `remote.html` 同样过准入守卫（手机端首访必须带
//! `?token=`，命中后换 Cookie 并 302 去参）。

use std::time::Duration;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderValue, Method, header};
use axum::middleware::{from_fn, from_fn_with_state};
use axum::routing::{delete, get, patch, post};
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

use crate::api::activity;
use crate::api::admin;
use crate::api::artifact;
use crate::api::attachment;
use crate::api::browser_replay;
use crate::api::code_analysis;
use crate::api::command;
use crate::api::config as config_api;
use crate::api::dialog;
use crate::api::doctor;
use crate::api::evidence;
use crate::api::file;
use crate::api::grant;
use crate::api::history;
use crate::api::interaction;
use crate::api::llm_keys;
use crate::api::mcp;
use crate::api::mcp_capability;
use crate::api::mcp_server;
use crate::api::memory;
use crate::api::models;
use crate::api::openapi;
use crate::api::plugin as plugin_api;
use crate::api::project;
use crate::api::query;
use crate::api::remote;
use crate::api::run;
use crate::api::session;
use crate::api::session_snapshot;
use crate::api::skill;
use crate::api::speech;
use crate::api::swarm;
use crate::api::system;
use crate::api::tool;
use crate::api::verify;
use crate::api::workbench;
use crate::error;
use crate::python::proxy as python_proxy;
use crate::state::AppState;

/// CORS 白名单基础四来源（形制沿用旧 `SecurityConfig`）。
pub(crate) const BASE_CORS_ORIGINS: [&str; 6] = [
    "http://localhost:5273",
    "http://localhost:8080",
    "http://localhost:8082",
    "http://127.0.0.1:5273",
    "http://127.0.0.1:8080",
    "http://127.0.0.1:8082",
];

/// 组装应用 Router（U2 八端点 + 系统域 + 模型/配置域 + Projects 域 +
/// 交互域 + 授权域 + `OpenAPI` 文档 + 指标 + S8 WS 通道）。
#[allow(clippy::too_many_lines)] // 路由表：逐条 .route() 装配，拆分反而割裂契约全貌
pub fn build_router(state: AppState) -> Router {
    Router::new()
        // Reverse MCP JSON-RPC endpoint. It shares access_guard, ToolRegistry and Admission.
        .route(
            "/mcp",
            post(mcp_server::handle).layer(DefaultBodyLimit::max(1024 * 1024)),
        )
        // ── 会话域（U2，S5 源码核实的 8 端点集，勿改）──
        .route(
            "/api/sessions",
            post(session::create_session).get(session::list_sessions),
        )
        .route(
            "/api/sessions/{id}",
            get(session::get_session_detail).delete(session::delete_session),
        )
        .route("/api/sessions/{id}/resume", post(session::resume_session))
        .route("/api/sessions/{id}/compact", post(session::compact_session))
        .route("/api/sessions/{id}/export", post(session::export_session))
        .route(
            "/api/sessions/{id}/messages",
            get(session::list_session_messages),
        )
        .route("/api/sessions/snapshots", get(session_snapshot::list))
        .route(
            "/api/sessions/{sessionId}/snapshot",
            post(session_snapshot::save),
        )
        .route(
            "/api/sessions/{sessionId}/snapshot/resume",
            post(session_snapshot::resume),
        )
        .route(
            "/api/sessions/snapshots/{sessionId}",
            delete(session_snapshot::delete),
        )
        // ── 系统域 ──
        .route("/api/health", get(system::health))
        .route("/api/health/live", get(system::health_live))
        .route("/api/health/ready", get(system::health_ready))
        .route("/api/auth/status", get(system::auth_status))
        .route("/api/auth/token", get(system::auth_token))
        .route("/api/doctor", get(doctor::doctor))
        .route("/api/query", post(query::sync_query))
        .route("/api/query/stream", post(query::stream_query))
        .route("/api/query/conversation", post(query::conversation_query))
        .route("/metrics", get(system::prometheus_metrics))
        // ── 模型/配置域（S7b）──
        .route("/api/models", get(models::list_models))
        .route(
            "/api/config",
            get(config_api::get_config).put(config_api::put_config),
        )
        .route(
            "/api/config/project",
            get(config_api::get_project_config).put(config_api::put_project_config),
        )
        // ── LLM 密钥管理域（Task 4 Step 5，GET/PUT /api/llm-keys）──
        .route(
            "/api/llm-keys",
            get(llm_keys::get_llm_keys).put(llm_keys::put_llm_keys),
        )
        // ── Speech 域（DashScope 标准通道；Token Plan 不支持 ASR/TTS）──
        .route("/api/asr/status", get(speech::asr_status))
        .route(
            "/api/asr/recognize",
            post(speech::recognize)
                .layer(DefaultBodyLimit::max(crate::speech::MAX_MULTIPART_BYTES)),
        )
        .route("/api/tts/status", get(speech::tts_status))
        .route("/api/tts/synthesize", post(speech::synthesize))
        // ── Projects 域（2.1，旧 ProjectController 5 端点）──
        .route(
            "/api/projects",
            get(project::list_projects).post(project::create_project),
        )
        .route(
            "/api/projects/directories",
            get(project::browse_directories),
        )
        .route(
            "/api/projects/directories/pick",
            post(project::pick_directory),
        )
        .route("/api/projects/{projectId}", delete(project::revoke_project))
        // ── 交互域（2.5，旧 InteractionController 2 端点）──
        .route("/api/interactions/pending", get(interaction::pending))
        .route(
            "/api/interactions/{interactionId}/decisions",
            post(interaction::decide),
        )
        // ── 授权域（2.5，旧 PermissionGrantController 2 端点）──
        .route("/api/permissions/grants", get(grant::list_active))
        .route("/api/permissions/grants/{grantId}", delete(grant::revoke))
        // ── 技能域（3B.7，旧 SkillController 2 端点）──
        .route("/api/skills", get(skill::list_skills))
        .route("/api/skills/{name}", get(skill::get_skill))
        // ── 工具域（Batch 1 Step 1-5，旧 ToolController 3 端点）──
        .route("/api/tools", get(tool::list_tools))
        .route(
            "/api/tools/{toolName}",
            get(tool::get_tool_detail).patch(tool::toggle_tool),
        )
        // ── Slash Command 机器目录（与 WS 执行共用同一 CommandRegistry）──
        .route("/api/commands", get(command::list))
        .route("/api/commands/{name}", get(command::get))
        // ── Run 域（Batch 2 Step 2-5，旧 RunController 3 读端点，不含 /cancel）──
        .route("/api/runs/session/{sessionId}", get(run::list_runs))
        .route("/api/runs/{runId}", get(run::get_run))
        .route("/api/runs/{runId}/events", get(run::get_events))
        .route("/api/runs/{runId}/cancel", post(run::cancel_run))
        // ── Evidence 域（WP-06；统一 SQLite + workspace blob store）──
        .route("/api/evidence", post(evidence::create_evidence))
        .route("/api/evidence/{bundleId}", get(evidence::get_evidence))
        .route(
            "/api/evidence/{bundleId}/verify",
            post(evidence::verify_evidence),
        )
        .route(
            "/api/evidence/session/{sessionId}",
            get(evidence::list_session_evidence),
        )
        .route("/api/evidence/blob/{sha256}", get(evidence::get_blob))
        // ── Artifact 域（WP-06；密封清单 + 本地完整性复验）──
        .route("/api/artifacts/manifests", post(artifact::create_manifest))
        .route(
            "/api/artifacts/manifests/{manifestId}/verify",
            post(artifact::verify_manifest),
        )
        .route(
            "/api/runs/{runId}/manifest",
            get(artifact::get_run_manifest),
        )
        .route(
            "/api/runs/{runId}/manifest/verify",
            post(artifact::verify_run_manifest),
        )
        // ── Workbench 域（WP-06；请求/结果绑定 + Evidence-backed 验收）──
        .route("/api/workbench/tasks", get(workbench::search_tasks))
        .route(
            "/api/workbench/{runId}",
            get(workbench::get_workbench).put(workbench::update_workbench),
        )
        .route(
            "/api/sessions/{sessionId}/workbench/current",
            get(workbench::get_current_workbench),
        )
        .route("/api/verify/run-checks", post(verify::run_checks))
        // ── File 域（Batch 2 Step 2-4，旧 FileController 3 端点）──
        .route("/api/files/search", get(file::search_files))
        .route("/api/sessions/{id}/files/preview", get(file::preview))
        .route("/api/sessions/{id}/files/reveal", post(file::reveal))
        // ── Activity 域（Batch 2 Step 2-6，旧 ActivityController 1 端点）──
        .route(
            "/api/sessions/{id}/activities",
            get(activity::get_activities),
        )
        // ── Attachment 域（Batch 2 Step 2-7，旧 AttachmentController 2 端点）──
        // upload 挂 64 MiB 传输护栏（决策 A-ATT），使 handler 内 10MB 判定成主门。
        .route(
            "/api/attachments/upload",
            post(attachment::upload).layer(DefaultBodyLimit::max(64 * 1024 * 1024)),
        )
        .route("/api/attachments/{fileUuid}", get(attachment::download))
        // ── 文件历史域（Batch 5 Step 6，旧 FileHistoryController 3 端点）──
        .route(
            "/api/sessions/{id}/history/snapshots",
            get(history::list_snapshots),
        )
        .route(
            "/api/sessions/{id}/history/rewind",
            post(history::rewind_to_snapshot),
        )
        .route(
            "/api/sessions/{id}/history/diff",
            get(history::get_diff_stats),
        )
        // ── 记忆域（Batch 5 Step 6，旧 MemoryController 5 端点）──
        // 三方法共用 `/api/memory` 一条路径（旧 controller 的
        // `@GetMapping` / `@PutMapping` / `@PostMapping` 无子路径）。
        .route(
            "/api/memory",
            get(memory::get_memories)
                .put(memory::update_memories)
                .post(memory::create_memory),
        )
        .route("/api/memory/all", get(memory::get_all_memories))
        .route("/api/memory/{memoryId}", delete(memory::delete_memory))
        // ── 远程控制域（Batch 2b Step 2b-6，旧 RemoteControlController 2 端点）──
        .route("/api/remote/status", get(remote::status))
        .route("/api/remote/interrupt", post(remote::interrupt))
        // ── Admin 认证域（Batch 8B Step 7，旧 AdminController 3 端点）──
        // ── Batch 8G：Plugin / BrowserReplay / CodeAnalysis 端点 ──
        .route("/api/plugins", get(plugin_api::list_plugins))
        .route("/api/plugins/install", post(plugin_api::install_plugin))
        .route("/api/plugins/{id}", delete(plugin_api::uninstall_plugin))
        .route("/api/plugins/reload", post(plugin_api::reload_plugins))
        .route(
            "/api/browser/replay/{id}",
            get(browser_replay::get_replay).delete(browser_replay::delete_replay),
        )
        .route(
            "/api/code-diagrams/generate",
            post(code_analysis::generate_diagram),
        )
        .route(
            "/api/code-path/endpoints",
            post(code_analysis::analyze_endpoints),
        )
        .route("/api/code-path/trace", post(code_analysis::trace_path))
        .route("/api/admin/login", post(admin::login))
        .route("/api/admin/status", get(admin::status))
        .route("/api/admin/logout", post(admin::logout))
        // ── Dialog 决策域（Batch 8B Step 8，旧 DialogController 2 端点）──
        .route(
            "/api/dialogs/snapshot-update/{requestId}/decision",
            post(dialog::resolve_snapshot_update),
        )
        .route(
            "/api/dialogs/plugin-permission/{requestId}/decision",
            post(dialog::resolve_plugin_permission),
        )
        // ── MCP 服务器域（Batch 4B Step 9，旧 McpController 10 端点）──
        .route(
            "/api/mcp/servers",
            get(mcp::list_servers).post(mcp::add_server),
        )
        .route("/api/mcp/servers/{name}", delete(mcp::delete_server))
        .route("/api/mcp/servers/{name}/restart", post(mcp::restart_server))
        .route("/api/mcp/servers/{name}/logs", get(mcp::server_logs))
        .route("/api/mcp/reconnect", post(mcp::reconnect_server))
        .route("/api/mcp/resources", get(mcp::list_resources))
        .route("/api/mcp/resources/read", get(mcp::read_resource))
        .route("/api/mcp/prompts", get(mcp::list_prompts))
        .route("/api/mcp/prompts/execute", post(mcp::execute_prompt))
        // ── MCP 能力注册表域（Batch 4B Step 9，旧 McpCapabilityController 10
        // 端点）。`/capabilities/domains` 必须先于 `/capabilities/{id}` 声明的
        // 直觉不成立——axum 的 matchit 静态段优先于占位段，两者顺序无关，此处
        // 仍按旧 controller 的声明序排列。
        .route(
            "/api/mcp/capabilities",
            get(mcp_capability::list_capabilities).post(mcp_capability::add_capability),
        )
        .route(
            "/api/mcp/capabilities/domains",
            get(mcp_capability::list_domains),
        )
        .route(
            "/api/mcp/capabilities/{id}",
            get(mcp_capability::get_capability)
                .put(mcp_capability::update_capability)
                .delete(mcp_capability::delete_capability),
        )
        .route(
            "/api/mcp/capabilities/{id}/toggle",
            patch(mcp_capability::toggle_capability),
        )
        .route(
            "/api/mcp/capabilities/{id}/server-tools",
            get(mcp_capability::list_server_tools),
        )
        .route(
            "/api/mcp/capabilities/{id}/test",
            post(mcp_capability::test_capability),
        )
        .route(
            "/api/mcp/capabilities/{id}/invoke",
            post(mcp_capability::invoke_capability),
        )
        // ── Swarm 多代理协作域（Batch 6 Phase C Step 6-16，旧 SwarmController 5 端点）──
        .route(
            "/api/swarm",
            get(swarm::list_swarms).post(swarm::create_swarm),
        )
        .route(
            "/api/swarm/{swarmId}",
            get(swarm::get_swarm).delete(swarm::destroy_swarm),
        )
        .route("/api/swarm/{swarmId}/dispatch", post(swarm::dispatch_swarm))
        .route("/api/swarm/{swarmId}/abort", post(swarm::abort_swarm))
        .route("/api/swarm/{swarmId}/shutdown", post(swarm::shutdown_swarm))
        .route(
            "/api/swarm/{swarmId}/force-stop",
            post(swarm::force_stop_swarm),
        )
        .route(
            "/api/swarm/{swarmId}/worker/{workerId}/abort",
            post(swarm::abort_worker),
        )
        // ── Python 侧车反向代理（2.6 回归修复：四个前端面板的前缀直通）──
        // 侧车只监听 UDS，浏览器无法直连，故经此转发；侧车缺席时 503 而非
        // panic。方法集钳到 GET/POST（Python 侧四组端点的全集），其余 405。
        .route(
            "/api/tokenizer/{*rest}",
            get(python_proxy::proxy_to_python).post(python_proxy::proxy_to_python),
        )
        .route(
            "/api/code-quality/{*rest}",
            get(python_proxy::proxy_to_python).post(python_proxy::proxy_to_python),
        )
        .route(
            "/api/files/analysis/{*rest}",
            get(python_proxy::proxy_to_python).post(python_proxy::proxy_to_python),
        )
        // 文件树面板打的是 Python `routers/file_processing.py` 的 `/tree`
        // （不在 analysis 子前缀下）。只放开这一条精确路径而非整个
        // `/api/files/`：`/api/files/search` 是会话感知的后端能力，不可被代理。
        .route(
            "/api/files/tree",
            get(python_proxy::proxy_to_python).post(python_proxy::proxy_to_python),
        )
        // API 契约面板打的是 Python `routers/analysis.py` 的
        // `/api/analysis/openapi/{merged,java,python}`。
        .route(
            "/api/analysis/{*rest}",
            get(python_proxy::proxy_to_python).post(python_proxy::proxy_to_python),
        )
        .route(
            "/api/git/{*rest}",
            get(python_proxy::proxy_to_python).post(python_proxy::proxy_to_python),
        )
        // ── OpenAPI 文档（S7b 运维端点，同 /metrics 定位）──
        // 代理四前缀不进 OpenAPI：契约的权威在 python-service 自身的
        // `/openapi.json`，此处只是传输层直通（同 `/metrics` 的定位）。
        .route("/api/openapi.json", get(openapi::openapi_json))
        // ── S8 原生 WS 通道（升级请求同栈过 access_guard）──
        .route("/ws", get(crate::ws::connection::handle_ws))
        // ── 静态资源（Batch 2b Step 2b-7，旧 classpath:/static 资源处理器）──
        // 本机 `remote.html` 由此出站；未命中文件时 404（同旧端）。
        .fallback_service(ServeDir::new(state.config.static_dir.clone()))
        .layer(from_fn(crate::middleware::observe))
        // Loopback is not a CSRF boundary.  MCP mutations additionally require
        // an exact trusted Origin or the local Bearer token, and JSON-bearing
        // routes reject browser-simple content types.
        .layer(from_fn_with_state(
            state.clone(),
            crate::middleware::mcp_mutation_guard,
        ))
        // 准入守卫要读凭证，故经 `from_fn_with_state` 拿 `AppState`。
        .layer(from_fn_with_state(
            state.clone(),
            crate::middleware::access_guard,
        ))
        .layer(cors_layer(&state))
        .layer(CatchPanicLayer::custom(|_panic| error::panic_response()))
        .with_state(state)
}

/// CORS 配置（旧 `SecurityConfig.corsConfigurationSource` 复刻）。
fn cors_layer(state: &AppState) -> CorsLayer {
    let mut origins: Vec<HeaderValue> = BASE_CORS_ORIGINS
        .iter()
        .filter_map(|origin| origin.parse().ok())
        .collect();
    origins.extend(
        state
            .config
            .extra_cors_origins
            .iter()
            .filter_map(|origin| origin.parse().ok()),
    );
    CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::HeaderName::from_static("x-session-id"),
        ])
        .allow_credentials(true)
        .expose_headers([
            header::SET_COOKIE,
            header::HeaderName::from_static("x-session-id"),
        ])
        .max_age(Duration::from_hours(1))
}
