//! 系统域集成测试——health（含 live/ready 双探针）/ auth 三元组 /
//! auth token / metrics。

mod common;

use axum::http::StatusCode;
use serde_json::json;

use common::{app, assert_same_shape, call, json_body, local_get, sample};
use zk_server::metrics_recorder;

/// `GET /api/health`：键集对齐样例（`java` → `runtime` 偏离已声明）。
#[tokio::test]
async fn health_shape() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/health")).await;
    assert_eq!(status, StatusCode::OK);
    let health = json_body(&body);
    let mut keys: Vec<&str> = health
        .as_object()
        .expect("obj")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "capabilities",
            "service",
            "status",
            "subsystems",
            "timestamp",
            "uptime",
            "version"
        ]
    );
    assert_eq!(health["status"], "UP");
    assert_eq!(health["capabilities"]["agent"]["enabled"], false);
    assert_eq!(
        health["capabilities"]["worktree"]["code"],
        "FEATURE_NOT_READY"
    );
    assert!(health["uptime"].as_u64().is_some());
    let subsystems = &health["subsystems"];
    let mut sub_keys: Vec<&str> = subsystems
        .as_object()
        .expect("obj")
        .keys()
        .map(String::as_str)
        .collect();
    sub_keys.sort_unstable();
    // 2.6：`python` 子系统并入 `subsystems`（两键形状与既有子系统一致）；
    // 测试配置 `ZK_PYTHON_ENABLED` 等效关闭 → `DISABLED`，且**不拉低** overall。
    assert_eq!(sub_keys, vec!["database", "python", "runtime"]);
    assert_eq!(subsystems["database"]["status"], "UP");
    assert_eq!(subsystems["python"]["status"], "DISABLED");
    let mut python_keys: Vec<&str> = subsystems["python"]
        .as_object()
        .expect("obj")
        .keys()
        .map(String::as_str)
        .collect();
    python_keys.sort_unstable();
    assert_eq!(python_keys, vec!["message", "status"]);
    assert!(
        subsystems["database"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("SQLite"))
    );
    assert!(
        health["timestamp"]
            .as_str()
            .is_some_and(|t| t.ends_with('Z'))
    );
}

/// `GET /api/health/live`：200 + text/plain + 字面 `OK`
/// （GET_api-health-live.json 样例：response 即裸文本）。
#[tokio::test]
async fn health_live_plain_ok() {
    let mut router = app();
    let (status, headers, body) = call(&mut router, local_get("/api/health/live")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/plain"))
    );
    assert_eq!(String::from_utf8(body.to_vec()).expect("utf8"), "OK");
}

/// `GET /api/health/ready`：数据库可用 → 200 + 字面 `READY`
/// （GET_api-health-ready.json 样例）。
#[tokio::test]
async fn health_ready_plain_ready() {
    let mut router = app();
    let (status, headers, body) = call(&mut router, local_get("/api/health/ready")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/plain"))
    );
    assert_eq!(String::from_utf8(body.to_vec()).expect("utf8"), "READY");
}

/// `GET /api/auth/status`：localhost 三元组与样例逐键、逐值一致。
#[tokio::test]
async fn auth_status_matches_sample_exactly() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/auth/status")).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    assert_same_shape(&sample("GET_api-auth-status.json"), &body, "auth_status");
    assert_eq!(
        body,
        json!({"authenticated": true, "authMode": "localhost", "username": "localhost-user"})
    );
}

/// `GET /api/auth/token`：404 顶层 error 字符串（样例逐键一致）。
#[tokio::test]
async fn auth_token_not_enabled() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/auth/token")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_body(&body), json!({"error": "Token auth not enabled"}));
}

/// `GET /metrics`：请求后有 Prometheus 文本（counter + 直方图带 route 标签）。
#[tokio::test]
async fn metrics_exposes_request_counters() {
    metrics_recorder::install_once();
    let mut router = app();
    // 先打两个请求制造样本。
    let _ = call(&mut router, local_get("/api/health")).await;
    let _ = call(&mut router, local_get("/api/auth/status")).await;
    let (status, headers, body) = call(&mut router, local_get("/metrics")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/plain"))
    );
    let text = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(text.contains("# TYPE http_requests_total counter"));
    assert!(text.contains("route=\"/api/health\""));
    assert!(text.contains("# TYPE http_request_duration_seconds histogram"));
    assert!(text.contains("http_request_duration_seconds_count"));
}
