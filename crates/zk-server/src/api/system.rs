//! 系统域端点——健康检查、鉴权三元组、Prometheus 指标。
//!
//! - `GET /api/health`：键集对齐样例，`java` → `runtime`（非会话域样例，
//!   Rust 化偏离已在 S7 报告声明）；
//! - `GET /api/health/live` / `ready`：k8s 双探针（S7b；text/plain，
//!   `OK` / `READY`|`NOT_READY`，对齐旧 `HealthController` 双探针）；
//! - `GET /api/auth/status`：按 `auth_mode` 三分支（旧 `AuthController.status`
//!   的 `switch`：localhost 固定三元组 / `lan_token` 按 Bearer 校验 / 其它模式
//!   恒未认证）；
//! - `GET /api/auth/token`：三态（非 `lan_token`→404、非 loopback→403、
//!   否则 200 携 token）——顶层 `{"error": "..."}` 而非错误信封，逐字保留旧
//!   `AuthController.getToken` 的特殊形状；
//! - `GET /metrics`：新增运维端点（旧系统无对应），Prometheus 文本。

use axum::Json;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::json;
use std::fmt::Write as _;
use std::net::SocketAddr;

use crate::iso::{format_rfc3339_micros, now_millis};
use crate::metrics_recorder;
use crate::python::ProcessState;
use crate::state::AppState;

/// `GET /api/health`——数据库连通性探测（轻量只读查询）。
#[utoipa::path(
    get,
    path = "/api/health",
    tag = "system",
    responses(
        (status = 200, description = "综合健康（status/service/version/uptime/subsystems/timestamp）")
    )
)]
pub(crate) async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    // 旧 `checkDatabase` 对内嵌 SQLite 恒 UP；此处以一次真实只读查询落实探测。
    let db_ok = state.db.list_sessions(None, 1).await.is_ok();
    let uptime_secs = state.started_at.elapsed().as_secs();
    // 2.6：Python 侧车状态并入 `subsystems` 但**不参与** overall 计算——侧车
    // 是可选能力扩展（能力降级矩阵：无 Python 时核心对话正常），若纳入
    // `allHealthy` 会让未装 Python 的部署整体 DEGRADED/503，与降级判据冲突。
    let overall = if db_ok { "UP" } else { "DOWN" };
    let db_status = if db_ok { "UP" } else { "DOWN" };
    let agent_assembled = state.agent_runtime().is_some();
    let tools = state.tools();
    let execution_runtime_ready = execution_runtime_ready(&state);
    let agent_executable =
        execution_runtime_ready && agent_assembled && tools.get("Agent").is_some();
    Json(json!({
        "status": overall,
        "service": "zk-server",
        "version": env!("CARGO_PKG_VERSION"),
        "build": {
            "gitSha": crate::BUILD_GIT_SHA,
            "builtAtUnixSeconds": crate::BUILD_UNIX_SECONDS,
            "schemaVersion": crate::DB_SCHEMA_VERSION,
            "protocolVersion": zk_protocol::WS_PROTOCOL_VERSION,
        },
        "uptime": uptime_secs,
        "subsystems": {
            "database": {
                "status": db_status,
                "message": "SQLite embedded database available",
                "schema": {
                    "version": zk_db::GREENFIELD_SCHEMA_VERSION,
                    "kind": zk_db::GREENFIELD_SCHEMA_KIND,
                    "migrationMode": zk_db::DATABASE_MIGRATION_MODE,
                    "legacyWriteCompatibility": zk_db::LEGACY_WRITE_COMPATIBILITY,
                },
            },
            "runtime": {
                "status": "UP",
                "message": format!("process uptime: {uptime_secs}s"),
            },
            "python": python_subsystem(&state),
        },
        "capabilities": {
            "agent": capability_readiness(
                state.config.agent_enabled,
                agent_executable,
                "WP-01 child context, admission, persistence, and recovery gates pass"
            ),
            "agentWrite": capability_readiness(
                state.config.agent_enabled && state.config.agent_write_enabled,
                agent_executable && state.config.agent_write_enabled,
                "child Write/Edit/Bash admission and short Kimi safety gates pass"
            ),
            "sharedWorkspace": capability_readiness(
                state.config.shared_workspace_enabled,
                execution_runtime_ready && state.shared_workspace_executable(),
                "Agent, child writes, the independent shared-workspace gate, and the production workspace lease are all ready"
            ),
            "autoResumeSafeTasks": capability_readiness(
                state.config.auto_resume_safe_tasks,
                state.safe_recovery_executable(),
                "typed checkpoint proof plus atomic root-and-attached-child continuation must be assembled; child-only recovery is fail-closed"
            ),
            "worktree": capability_readiness(
                state.config.worktree_enabled,
                execution_runtime_ready && agent_assembled && tools.get("Worktree").is_some(),
                "real Git worktree isolation gates pass"
            ),
            "swarm": capability_readiness(
                state.config.swarm_enabled,
                execution_runtime_ready && state.swarm_executable(),
                "the legacy process-local coordinator is removed and TaskRuntime result, receipt, cancellation, budget, recovery, event, and verification gates pass"
            ),
            "cron": capability_readiness(
                state.config.cron_enabled,
                execution_runtime_ready && state.cron_executable(),
                "persistent SQLite scheduler and unified TaskRuntime are both assembled"
            ),
        },
        "timestamp": format_rfc3339_micros(now_millis()),
    }))
}

/// Execution readiness is stricter than component assembly: a runtime built
/// before the durable startup epoch exists, or one whose intake is draining,
/// cannot truthfully accept Agent/Task work.
fn execution_runtime_ready(state: &AppState) -> bool {
    state.startup_epoch() > 0 && state.task_runtime.accepts_new_execution()
}

fn capability_readiness(
    configured: bool,
    executable: bool,
    enable_when: &str,
) -> serde_json::Value {
    json!({
        "configured": configured,
        "executable": executable,
        "code": if configured && executable { "READY" } else { "FEATURE_NOT_READY" },
        "enableWhen": enable_when,
    })
}

/// Python 侧车子系统状态（2.6；两键形状与 `database` / `runtime` 一致）。
///
/// 只读进程内缓存（`AtomicU8` 状态 + 能力缓存快照），**不发起网络探测**——
/// `/api/health` 保持轻量，真实探测由侧车 30s 轮询与 1s liveness 巡检承担。
fn python_subsystem(state: &AppState) -> serde_json::Value {
    if !state.config.python_enabled {
        return json!({
            "status": "DISABLED",
            "message": "Python sidecar disabled (ZK_PYTHON_ENABLED=false)",
        });
    }
    let capabilities = state.python.capabilities();
    let available = capabilities
        .values()
        .filter(|cap| cap.is_available())
        .count();
    let total = capabilities.len();
    let socket = state.python.socket().display();
    let Some(sidecar) = state.python_sidecar.as_ref() else {
        // 侧车非本进程托管（集成测试 / 外部 uvicorn）：退化为客户端能力缓存视角。
        let status = if state.python.last_refresh_succeeded() {
            "UP"
        } else {
            "DOWN"
        };
        return json!({
            "status": status,
            "message": format!(
                "unmanaged sidecar at {socket}; capabilities {available}/{total} available"
            ),
        });
    };
    let process = sidecar.state();
    let status = match process {
        ProcessState::Running => "UP",
        ProcessState::Starting | ProcessState::Restarting | ProcessState::HealthCheckFailed => {
            "DEGRADED"
        }
        ProcessState::Stopped | ProcessState::Failed => "DOWN",
    };
    json!({
        "status": status,
        "message": format!(
            "process {} at {socket}; capabilities {available}/{total} available; restarts {}",
            process.as_str(),
            sidecar.restart_count()
        ),
    })
}

/// `GET /api/health/live`——存活探针（S7b；旧 `liveness` 恒 200 `OK`，
/// 进程能应答即存活，无任何依赖检查）。
#[utoipa::path(
    get,
    path = "/api/health/live",
    tag = "system",
    responses((status = 200, description = "存活（text/plain 恒 `OK`）"))
)]
pub(crate) async fn health_live() -> &'static str {
    "OK"
}

/// `GET /api/health/ready`——就绪探针（S7b；旧 `readiness` 检查数据库可用，
/// 此处以一次真实只读查询落实——与 `/api/health` 同探测强度）。
#[utoipa::path(
    get,
    path = "/api/health/ready",
    tag = "system",
    responses(
        (status = 200, description = "就绪（text/plain `READY`）"),
        (status = 503, description = "数据库不可用（text/plain `NOT_READY`）")
    )
)]
pub(crate) async fn health_ready(State(state): State<AppState>) -> (StatusCode, &'static str) {
    let db_ready = state.db.list_sessions(None, 1).await.is_ok();
    let execution_ready = !state.config.agent_enabled
        || (execution_runtime_ready(&state)
            && state.agent_runtime().is_some()
            && state.tools().get("Agent").is_some());
    let cron_ready = !state.config.cron_enabled || state.cron_executable();
    let shared_workspace_ready =
        !state.config.shared_workspace_enabled || state.shared_workspace_executable();
    let swarm_ready = !state.config.swarm_enabled || state.swarm_executable();
    let auto_resume_ready =
        !state.config.auto_resume_safe_tasks || state.safe_recovery_executable();
    if db_ready
        && execution_ready
        && cron_ready
        && shared_workspace_ready
        && swarm_ready
        && auto_resume_ready
    {
        (StatusCode::OK, "READY")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "NOT_READY")
    }
}

/// `GET /api/auth/status`——认证状态三元组（旧 `AuthController.status`）。
///
/// 逐字对齐旧 `switch (authMode)`：
/// - `localhost` → `(true, "localhost", "localhost-user")`；
/// - `lan_token` → 按 `Authorization: Bearer {token}` 判定，
///   `(valid, "lan_token", valid ? "lan-user" : null)`；
/// - 其它 → `(false, authMode, null)`。
///
/// `username` 为 null 时**整键剥离**——旧端 `default-property-inclusion:
/// non_null`（application.yml L17）对该 record 生效，键不出现在 JSON 中。
#[utoipa::path(
    get,
    path = "/api/auth/status",
    tag = "system",
    responses((status = 200, description = "认证状态（authenticated/authMode/username?）"))
)]
pub(crate) async fn auth_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Json<serde_json::Value> {
    let mode = state.config.auth_mode.as_str();
    let (authenticated, username) = match mode {
        crate::config::AUTH_MODE_LOCALHOST => (true, Some("localhost-user")),
        crate::config::AUTH_MODE_LAN_TOKEN => {
            let valid = headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
                .is_some_and(|token| state.access_tokens.matches(token));
            (valid, if valid { Some("lan-user") } else { None })
        }
        _ => (false, None),
    };
    let mut body = json!({
        "authenticated": authenticated,
        "authMode": mode,
    });
    if let Some(username) = username {
        body["username"] = json!(username);
    }
    Json(body)
}

/// `GET /api/auth/token`——取局域网访问 token（旧 `AuthController.getToken`）。
///
/// 三态与旧源同判序（先模式后来源）：非 `lan_token` 模式 → 404；对端非
/// loopback → 403；否则 200 携 token。响应体为顶层 `{"error"|"token": ...}`，
/// 不套项目错误信封——旧端此处直接 `Map.of(...)`。
///
/// 偏离 B2B-15：token 取自 `AccessTokenManager`（与准入守卫同源）。旧端此处读
/// 独立配置项 `auth.lan-token`，与过滤器自持的 `access-token` 文件并非同一
/// 份——照抄会让端点吐出守卫不认的 token，属旧端缺陷，不复刻。
#[utoipa::path(
    get,
    path = "/api/auth/token",
    tag = "system",
    responses(
        (status = 200, description = "token 明文（顶层 `{\"token\":...}`，非错误信封）"),
        (status = 403, description = "非 loopback 来源（顶层 `{\"error\":...}`）"),
        (status = 404, description = "未启用 lan_token 模式（顶层 `{\"error\":...}`）")
    )
)]
pub(crate) async fn auth_token(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
) -> (StatusCode, Json<serde_json::Value>) {
    if !state.config.is_lan_token_mode() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Token auth not enabled" })),
        );
    }
    if !peer.ip().is_loopback() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Token can only be retrieved from localhost" })),
        );
    }
    (
        StatusCode::OK,
        Json(json!({ "token": state.access_tokens.token() })),
    )
}

/// `GET /metrics`——Prometheus 文本快照（请求计数 / 延迟与观测写入健康）。
pub(crate) async fn prometheus_metrics(State(state): State<AppState>) -> Response {
    let health = state.observability.health();
    let mut snapshot = metrics_recorder::render_snapshot();
    let _ = write!(
        snapshot,
        "# TYPE zk_observability_accepted_total counter\n\
         zk_observability_accepted_total {}\n\
         # TYPE zk_observability_dropped_total counter\n\
         zk_observability_dropped_total {}\n\
         # TYPE zk_observability_write_failures_total counter\n\
         zk_observability_write_failures_total {}\n\
         # TYPE zk_observability_audit_failures_total counter\n\
         zk_observability_audit_failures_total {}\n",
        health.accepted, health.dropped, health.write_failures, health.audit_failures
    );
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        snapshot,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    /// 指定 `auth_mode` 的测试态（内存库 + 不落盘 token）。
    fn state_with_mode(mode: &str) -> AppState {
        let mut config = crate::config::Config::test_config();
        config.auth_mode = mode.to_owned();
        AppState::new(
            zk_db::Db::open_in_memory().expect("in-memory db boots"),
            config,
        )
    }

    async fn install_runtime_startup_epoch(state: &AppState) {
        let epoch = state
            .db
            .begin_runtime_startup_epoch()
            .await
            .expect("allocate durable startup epoch");
        state
            .set_startup_epoch(epoch)
            .expect("install runtime startup epoch");
    }

    /// `Authorization: Bearer {token}` 单头。
    fn bearer(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header"),
        );
        headers
    }

    const LOOPBACK: &str = "127.0.0.1:51717";
    const LAN: &str = "192.168.1.5:51717";

    #[tokio::test]
    async fn agent_readiness_requires_startup_epoch_and_open_runtime_intake() {
        let mut configured = crate::config::Config::test_config();
        configured.agent_enabled = true;

        let missing_epoch = AppState::new(
            zk_db::Db::open_in_memory().expect("in-memory db boots"),
            configured.clone(),
        );
        let body = health(State(missing_epoch.clone())).await;
        assert_eq!(body.0["capabilities"]["agent"]["configured"], true);
        assert_eq!(body.0["capabilities"]["agent"]["executable"], false);
        assert_eq!(
            health_ready(State(missing_epoch)).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );

        let ready = AppState::new(
            zk_db::Db::open_in_memory().expect("in-memory db boots"),
            configured,
        );
        install_runtime_startup_epoch(&ready).await;
        let body = health(State(ready.clone())).await;
        assert_eq!(body.0["capabilities"]["agent"]["executable"], true);
        assert_eq!(health_ready(State(ready.clone())).await.0, StatusCode::OK);

        let report = ready
            .task_runtime()
            .shutdown(std::time::Duration::from_secs(1))
            .await
            .expect("empty runtime shuts down cleanly");
        assert!(report.drained);
        let body = health(State(ready.clone())).await;
        assert_eq!(body.0["capabilities"]["agent"]["executable"], false);
        assert_eq!(
            health_ready(State(ready)).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn cron_health_reports_configured_and_executable_independently() {
        let mut unavailable = crate::config::Config::test_config();
        unavailable.cron_enabled = true;
        let unavailable = AppState::new(
            zk_db::Db::open_in_memory().expect("in-memory db boots"),
            unavailable,
        );
        let body = health(State(unavailable.clone())).await;
        assert_eq!(body.0["capabilities"]["cron"]["configured"], true);
        assert_eq!(body.0["capabilities"]["cron"]["executable"], false);
        assert_eq!(
            health_ready(State(unavailable)).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );

        let mut ready = crate::config::Config::test_config();
        ready.agent_enabled = true;
        ready.cron_enabled = true;
        let ready = AppState::new(
            zk_db::Db::open_in_memory().expect("in-memory db boots"),
            ready,
        );
        install_runtime_startup_epoch(&ready).await;
        let _scheduler = ready.cron_scheduler().expect("Cron scheduler assembled");
        let body = health(State(ready.clone())).await;
        assert_eq!(body.0["capabilities"]["cron"]["configured"], true);
        assert_eq!(body.0["capabilities"]["cron"]["executable"], true);
        assert_eq!(health_ready(State(ready)).await.0, StatusCode::OK);
    }

    #[tokio::test]
    async fn shared_workspace_and_auto_resume_fail_closed_independently() {
        let mut write_only = crate::config::Config::test_config();
        write_only.agent_enabled = true;
        write_only.agent_write_enabled = true;
        let write_only = AppState::new(
            zk_db::Db::open_in_memory().expect("in-memory db boots"),
            write_only,
        );
        install_runtime_startup_epoch(&write_only).await;
        let body = health(State(write_only.clone())).await;
        assert_eq!(
            body.0["capabilities"]["sharedWorkspace"]["configured"],
            false
        );
        assert_eq!(
            body.0["capabilities"]["sharedWorkspace"]["executable"],
            false
        );
        assert_eq!(
            body.0["capabilities"]["autoResumeSafeTasks"]["configured"],
            false
        );
        assert_eq!(
            body.0["capabilities"]["autoResumeSafeTasks"]["executable"],
            false
        );
        assert_eq!(health_ready(State(write_only)).await.0, StatusCode::OK);

        let mut shared = crate::config::Config::test_config();
        shared.agent_enabled = true;
        shared.agent_write_enabled = true;
        shared.shared_workspace_enabled = true;
        let shared = AppState::new(
            zk_db::Db::open_in_memory().expect("in-memory db boots"),
            shared,
        );
        install_runtime_startup_epoch(&shared).await;
        let body = health(State(shared.clone())).await;
        assert_eq!(
            body.0["capabilities"]["sharedWorkspace"]["configured"],
            true
        );
        assert_eq!(
            body.0["capabilities"]["sharedWorkspace"]["executable"],
            true
        );
        assert_eq!(health_ready(State(shared)).await.0, StatusCode::OK);

        let mut unsupported_resume = crate::config::Config::test_config();
        unsupported_resume.auto_resume_safe_tasks = true;
        let unsupported_resume = AppState::new(
            zk_db::Db::open_in_memory().expect("in-memory db boots"),
            unsupported_resume,
        );
        let body = health(State(unsupported_resume.clone())).await;
        assert_eq!(
            body.0["capabilities"]["autoResumeSafeTasks"]["configured"],
            true
        );
        assert!(
            body.0["capabilities"]["autoResumeSafeTasks"]
                .get("requested")
                .is_none()
        );
        assert_eq!(
            body.0["capabilities"]["autoResumeSafeTasks"]["executable"],
            false
        );
        assert_eq!(
            health_ready(State(unsupported_resume)).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    /// 非 `lan_token` 模式：无论来源恒 404（旧源先判模式后判来源）。
    #[tokio::test]
    async fn auth_token_rejects_when_mode_disabled() {
        let state = state_with_mode(crate::config::AUTH_MODE_LOCALHOST);
        for peer in [LOOPBACK, LAN] {
            let (status, body) = auth_token(
                State(state.clone()),
                ConnectInfo(peer.parse().expect("peer")),
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{peer}");
            assert_eq!(body.0, json!({ "error": "Token auth not enabled" }));
        }
    }

    /// `lan_token` 模式 + 非 loopback 来源 → 403（不泄 token）。
    #[tokio::test]
    async fn auth_token_rejects_non_loopback_source() {
        let state = state_with_mode(crate::config::AUTH_MODE_LAN_TOKEN);
        let (status, body) = auth_token(
            State(state.clone()),
            ConnectInfo(LAN.parse().expect("peer")),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(
            body.0,
            json!({ "error": "Token can only be retrieved from localhost" })
        );
    }

    /// `lan_token` 模式 + loopback → 200 携守卫同源 token。
    #[tokio::test]
    async fn auth_token_returns_guard_token_from_loopback() {
        let state = state_with_mode(crate::config::AUTH_MODE_LAN_TOKEN);
        let expected = state.access_tokens.token().to_owned();
        let (status, body) = auth_token(
            State(state.clone()),
            ConnectInfo(LOOPBACK.parse().expect("peer")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.0, json!({ "token": expected }));
    }

    /// localhost 模式：固定三元组，不看请求头。
    #[tokio::test]
    async fn auth_status_localhost_mode_is_fixed() {
        let state = state_with_mode(crate::config::AUTH_MODE_LOCALHOST);
        let body = auth_status(State(state), HeaderMap::new()).await;
        assert_eq!(
            body.0,
            json!({
                "authenticated": true,
                "authMode": "localhost",
                "username": "localhost-user",
            })
        );
    }

    /// `lan_token` 模式：Bearer 命中→`lan-user`；否则剥离 `username` 键。
    #[tokio::test]
    async fn auth_status_lan_token_mode_follows_bearer() {
        let state = state_with_mode(crate::config::AUTH_MODE_LAN_TOKEN);
        let token = state.access_tokens.token().to_owned();
        let ok = auth_status(State(state.clone()), bearer(&token)).await;
        assert_eq!(
            ok.0,
            json!({
                "authenticated": true,
                "authMode": "lan_token",
                "username": "lan-user",
            })
        );
        for headers in [HeaderMap::new(), bearer("wrong-token")] {
            let denied = auth_status(State(state.clone()), headers).await;
            assert_eq!(
                denied.0,
                json!({ "authenticated": false, "authMode": "lan_token" })
            );
        }
    }

    /// 未知模式：恒未认证 + 回显模式名 + 无 `username` 键。
    #[tokio::test]
    async fn auth_status_unknown_mode_is_unauthenticated() {
        let state = state_with_mode("jwt");
        let body = auth_status(State(state), HeaderMap::new()).await;
        assert_eq!(body.0, json!({ "authenticated": false, "authMode": "jwt" }));
    }
}
