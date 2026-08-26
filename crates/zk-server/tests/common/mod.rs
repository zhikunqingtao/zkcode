//! 集成测试共享助手（每测试二进制经 `mod common;` 引入；仅测试使用）。
#![allow(dead_code)]

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::ConnectInfo;
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use std::net::SocketAddr;
use tower::ServiceExt;
use zk_server::config::Config;
use zk_server::routes::build_router;
use zk_server::state::AppState;

/// 测试应用（内存库 + 测试配置）与同库 `Db` 句柄（种子数据用）。
pub fn app_with_db() -> (Router, zk_db::Db) {
    let state = AppState::for_tests();
    let db = state.db.clone();
    (build_router(state), db)
}

/// 指定运行配置的测试应用（Projects 域守卫/allowed roots 场景用）。
pub fn app_with_config(config: Config) -> (Router, zk_db::Db) {
    let state = AppState::new(
        zk_db::Db::open_in_memory().expect("in-memory db boots with migrations"),
        config,
    );
    let db = state.db.clone();
    (build_router(state), db)
}

/// 仅 HTTP 交互的测试应用。
pub fn app() -> Router {
    build_router(AppState::for_tests())
}

/// loopback 对端（`access_guard` 档 1 免认证放行）。
pub const LOCAL_PEER: &str = "127.0.0.1:51717";
/// 局域网对端（`access_guard` 档 2 放过网段判定，缺凭证时 401）。
pub const LAN_PEER: &str = "192.168.1.5:51717";
/// 公网对端（`access_guard` 档 2 直接 403；`203.0.113.0/24` 是 RFC 5737 文档
/// 用网段，永不路由到真实主机）。
pub const PUBLIC_PEER: &str = "203.0.113.7:51717";

/// 构造带对端地址的请求。
fn request(uri: &str, method: Method, body: Option<String>, loopback: bool) -> Request<Body> {
    let peer: SocketAddr = if loopback { LOCAL_PEER } else { PUBLIC_PEER }
        .parse()
        .expect("valid peer");
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .extension(ConnectInfo(peer));
    if loopback && builder.method_ref() == Some(&Method::GET) {
        // Real same-origin browser GETs often omit Origin, but carry this
        // browser-controlled fetch-metadata header.
        builder = builder.header("sec-fetch-site", "same-origin");
    } else if loopback {
        builder = builder.header(header::ORIGIN, "http://127.0.0.1:5273");
    }
    if body.is_some() {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }
    builder
        .body(Body::from(body.unwrap_or_default()))
        .expect("valid request")
}

/// loopback GET。
pub fn local_get(uri: &str) -> Request<Body> {
    request(uri, Method::GET, None, true)
}

/// loopback POST（可选 JSON 体）。
pub fn local_post(uri: &str, body: Option<String>) -> Request<Body> {
    request(uri, Method::POST, body, true)
}

/// loopback PUT（可选 JSON 体）。
pub fn local_put(uri: &str, body: Option<String>) -> Request<Body> {
    request(uri, Method::PUT, body, true)
}

/// loopback DELETE。
pub fn local_delete(uri: &str) -> Request<Body> {
    request(uri, Method::DELETE, None, true)
}

/// loopback PATCH（可选 JSON 体；工具域 `PATCH /api/tools/{name}` 用）。
pub fn local_patch(uri: &str, body: Option<String>) -> Request<Body> {
    request(uri, Method::PATCH, body, true)
}

/// loopback 请求 + 自定义头（Projects 域：pick 的 `X-Zhikun-Native-Picker`
/// 与转发头守卫场景）。
pub fn local_with_headers(
    uri: &str,
    method: Method,
    body: Option<String>,
    headers: &[(&str, &str)],
) -> Request<Body> {
    let peer: SocketAddr = LOCAL_PEER.parse().expect("valid peer");
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .extension(ConnectInfo(peer))
        .header(header::ORIGIN, "http://127.0.0.1:5273");
    if body.is_some() {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder
        .body(Body::from(body.unwrap_or_default()))
        .expect("valid request")
}

/// 公网对端 GET（守卫 403 拒绝路径——非私有网段无论带何凭证都不可达）。
pub fn remote_get(uri: &str) -> Request<Body> {
    request(uri, Method::GET, None, false)
}

/// 指定对端 + 自定义头的请求（Batch 2b 准入矩阵：局域网带/不带凭证）。
pub fn request_from(
    peer: &str,
    uri: &str,
    method: Method,
    headers: &[(axum::http::HeaderName, String)],
) -> Request<Body> {
    let peer: SocketAddr = peer.parse().expect("valid peer");
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .extension(ConnectInfo(peer));
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    builder.body(Body::empty()).expect("valid request")
}

/// 局域网对端 GET（无凭证）。
pub fn lan_get(uri: &str) -> Request<Body> {
    request_from(LAN_PEER, uri, Method::GET, &[])
}

/// CORS 预检请求（浏览器 OPTIONS + Origin + 预检头）。
pub fn preflight(uri: &str, origin: &str) -> Request<Body> {
    let peer: SocketAddr = LOCAL_PEER.parse().expect("valid peer");
    Request::builder()
        .method(Method::OPTIONS)
        .uri(uri)
        .extension(ConnectInfo(peer))
        .header(header::ORIGIN, origin)
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::empty())
        .expect("valid preflight")
}

/// 发送请求并收集（状态 / 头 / 体字节）。
pub async fn call(router: &mut Router, request: Request<Body>) -> (StatusCode, HeaderMap, Bytes) {
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot dispatch");
    let (parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("read body");
    (parts.status, parts.headers, bytes)
}

/// 解析 JSON 体。
pub fn json_body(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).expect("json body")
}

/// 读取 baseline 样例的 `response` 字段（响应形状逐键权威）。
pub fn sample(name: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../docs/baseline/samples/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("cannot read sample {name} at {path}: {err}"));
    let parsed: serde_json::Value = serde_json::from_str(&raw).expect("sample is valid json");
    parsed
        .get("response")
        .cloned()
        .unwrap_or_else(|| panic!("sample {name} has no response field"))
}

/// 响应形状逐键 diff：对象比键集合并递归；数组比首元素形状（样例空数组
/// 只要求 actual 亦为数组）；标量比 JSON 类型（值本身可不同）。
pub fn assert_same_shape(sample: &serde_json::Value, actual: &serde_json::Value, path: &str) {
    match (sample, actual) {
        (serde_json::Value::Object(expected), serde_json::Value::Object(got)) => {
            let mut expected_keys: Vec<&str> = expected.keys().map(String::as_str).collect();
            let mut got_keys: Vec<&str> = got.keys().map(String::as_str).collect();
            expected_keys.sort_unstable();
            got_keys.sort_unstable();
            assert_eq!(
                expected_keys, got_keys,
                "key set mismatch at {path}:\nsample: {sample}\nactual: {actual}"
            );
            for key in expected_keys {
                assert_same_shape(&expected[key], &got[key], &format!("{path}.{key}"));
            }
        }
        (serde_json::Value::Array(expected), serde_json::Value::Array(got)) => {
            if let Some(first) = expected.first() {
                let got_first = got
                    .first()
                    .unwrap_or_else(|| panic!("actual array empty at {path} but sample has items"));
                assert_same_shape(first, got_first, &format!("{path}[0]"));
            }
        }
        (serde_json::Value::String(_), serde_json::Value::String(_))
        | (serde_json::Value::Number(_), serde_json::Value::Number(_))
        | (serde_json::Value::Bool(_), serde_json::Value::Bool(_))
        | (serde_json::Value::Null, serde_json::Value::Null) => {}
        (expected, got) => panic!("type mismatch at {path}: sample {expected} vs actual {got}"),
    }
}
