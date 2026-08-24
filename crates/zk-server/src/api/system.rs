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
    Json(json!({
        "status": overall,
        "service": "zk-server",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime": uptime_secs,
        "subsystems": {
            "database": {
                "status": db_status,
                "message": "SQLite embedded database available",
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
                "WP-01 child context, admission, persistence, and recovery gates pass"
            ),
            "agentWrite": capability_readiness(
                state.config.agent_enabled && state.config.agent_write_enabled,
                "child Write/Edit/Bash admission and short Kimi safety gates pass"
            ),
            "worktree": capability_readiness(
                state.config.worktree_enabled,
                "real Git worktree isolation gates pass"
            ),
            "swarm": capability_readiness(
                state.config.swarm_enabled,
                "WP-11 dispatch, cancellation, persistence, and event gates pass"
            ),
        },
        "timestamp": format_rfc3339_micros(now_millis()),
    }))
}

fn capability_readiness(enabled: bool, enable_when: &str) -> serde_json::Value {
    json!({
        "enabled": enabled,
        "code": if enabled { "READY" } else { "FEATURE_NOT_READY" },
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
    if state.db.list_sessions(None, 1).await.is_ok() {
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
