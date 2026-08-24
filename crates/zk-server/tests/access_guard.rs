//! Batch 2b 准入矩阵集成测试（旧 `RemoteAccessSecurityFilter` 全档序 +
//! `AuthController` 三态 + `RemoteControlController` 2 端点 + `remote.html`
//! 出站）。
//!
//! 认证是每个请求都要过的热路径，档序错一位就会把局域网访问敞开或把本机
//! 挡死，故此处按档逐条钉死：静态资源免检 / localhost 免认证 /
//! 非私有网段 403 / Cookie / Bearer / URL token / 全落空 401。

mod common;

use axum::http::{Method, StatusCode, header};

use common::{
    LAN_PEER, LOCAL_PEER, PUBLIC_PEER, call, json_body, lan_get, local_get, request_from,
};
use zk_server::config::Config;
use zk_server::routes::build_router;
use zk_server::state::AppState;

/// 指定 `auth_mode` 的测试态（内存库 + 不落盘 token）。
fn state_with(auth_mode: &str) -> AppState {
    let mut config = Config::test_config();
    auth_mode.clone_into(&mut config.auth_mode);
    AppState::new(
        zk_db::Db::open_in_memory().expect("in-memory db boots"),
        config,
    )
}

/// 默认态（localhost 模式）。
fn default_state() -> AppState {
    state_with("localhost")
}

/// `Authorization: Bearer {token}` 头对。
fn bearer(token: &str) -> Vec<(header::HeaderName, String)> {
    vec![(header::AUTHORIZATION, format!("Bearer {token}"))]
}

/// `Cookie: ai-coder-session={id}` 头对。
fn cookie(raw: &str) -> Vec<(header::HeaderName, String)> {
    vec![(header::COOKIE, raw.to_owned())]
}

/// 从响应头取 `Set-Cookie` 原文。
fn set_cookie(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get(header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

// ── 档 1：localhost 免认证 ───────────────────────────────────────────────

/// loopback 对端不带任何凭证即放行，且**不**签发会话 Cookie（旧 L116-120
/// 直接 `filterChain.doFilter` 返回）。
#[tokio::test]
async fn loopback_passes_without_credentials() {
    let mut router = build_router(default_state());
    let (status, headers, _body) = call(&mut router, local_get("/api/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(set_cookie(&headers), None);
}

// ── 档 2：非私有网段 403 ────────────────────────────────────────────────

/// 公网对端恒 403 `ACCESS_DENIED`——即便携带正确 token（旧 L133-139 在凭证
/// 校验之前返回）。
#[tokio::test]
async fn public_peer_is_denied_even_with_valid_token() {
    let state = default_state();
    let token = state.access_tokens.token().to_owned();
    let mut router = build_router(state);
    for headers in [Vec::new(), bearer(&token)] {
        let (status, _headers, body) = call(
            &mut router,
            request_from(PUBLIC_PEER, "/api/health", Method::GET, &headers),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(json_body(&body)["code"], "ACCESS_DENIED");
    }
}

// ── 档 3~6：局域网凭证链 ────────────────────────────────────────────────

/// 局域网无凭证 → 401 `AUTHENTICATION_REQUIRED`（旧 L171-173，文案逐字）。
#[tokio::test]
async fn lan_without_credentials_is_unauthorized() {
    let mut router = build_router(default_state());
    let (status, headers, body) = call(&mut router, lan_get("/api/health")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(set_cookie(&headers), None);
    let error = json_body(&body);
    assert_eq!(error["code"], "AUTHENTICATION_REQUIRED");
    assert_eq!(
        error["message"],
        "Authentication required. Use token URL or Bearer header."
    );
}

/// 局域网 + 错误 Bearer → 401（不因「带了头」而放宽）。
#[tokio::test]
async fn lan_with_wrong_bearer_is_unauthorized() {
    let mut router = build_router(default_state());
    let (status, _headers, body) = call(
        &mut router,
        request_from(
            LAN_PEER,
            "/api/health",
            Method::GET,
            &bearer("not-the-token"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(json_body(&body)["code"], "AUTHENTICATION_REQUIRED");
}

/// 局域网 + 正确 Bearer → 200 且换发会话 Cookie，属性逐条对齐旧
/// `issueSessionCookie`（`HttpOnly` / `Max-Age` 30 天 / `Path=/` /
/// `SameSite=Lax`）。
#[tokio::test]
async fn lan_with_valid_bearer_passes_and_issues_cookie() {
    let state = default_state();
    let token = state.access_tokens.token().to_owned();
    let mut router = build_router(state.clone());
    let (status, headers, _body) = call(
        &mut router,
        request_from(LAN_PEER, "/api/health", Method::GET, &bearer(&token)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let issued = set_cookie(&headers).expect("session cookie issued");
    assert!(issued.starts_with("ai-coder-session="), "{issued}");
    assert!(issued.contains("Max-Age=2592000"), "{issued}");
    assert!(issued.contains("Path=/"), "{issued}");
    assert!(issued.contains("HttpOnly"), "{issued}");
    assert!(issued.contains("SameSite=Lax"), "{issued}");
    // 旧源不带 `Secure`（sslEnabled 缺省 false），本端同（偏离 B2B-04 已留痕）。
    assert!(!issued.contains("Secure"), "{issued}");
    assert_eq!(state.access_tokens.session_count(), 1);
}

/// 换发的 Cookie 可复用：后续请求凭 Cookie 放行且不再签发新 Cookie
/// （旧 L143-148 命中即 `doFilter` 返回）。
#[tokio::test]
async fn issued_cookie_authenticates_subsequent_requests() {
    let state = default_state();
    let token = state.access_tokens.token().to_owned();
    let mut router = build_router(state.clone());
    let (_status, headers, _body) = call(
        &mut router,
        request_from(LAN_PEER, "/api/health", Method::GET, &bearer(&token)),
    )
    .await;
    let issued = set_cookie(&headers).expect("session cookie issued");
    let pair = issued.split(';').next().expect("cookie pair").to_owned();

    let (status, headers, _body) = call(
        &mut router,
        request_from(LAN_PEER, "/api/health", Method::GET, &cookie(&pair)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(set_cookie(&headers), None);
    // 复用不新增会话（容量不被正常访问打满）。
    assert_eq!(state.access_tokens.session_count(), 1);
}

/// 伪造的会话 id → 401（TTL store 未登记）。
#[tokio::test]
async fn forged_session_cookie_is_unauthorized() {
    let mut router = build_router(default_state());
    let (status, _headers, body) = call(
        &mut router,
        request_from(
            LAN_PEER,
            "/api/health",
            Method::GET,
            &cookie("ai-coder-session=00000000-0000-0000-0000-000000000000"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(json_body(&body)["code"], "AUTHENTICATION_REQUIRED");
}

/// 局域网 + `?token=` 正确 → 302 去参重定向 + 换发 Cookie（旧 L161-169）。
#[tokio::test]
async fn url_token_redirects_without_token_param() {
    let state = default_state();
    let token = state.access_tokens.token().to_owned();
    let mut router = build_router(state);
    let (status, headers, _body) = call(
        &mut router,
        request_from(
            LAN_PEER,
            &format!("/remote.html?token={token}&view=1"),
            Method::GET,
            &[],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(
        headers
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok()),
        Some("/remote.html?view=1")
    );
    assert!(
        set_cookie(&headers).is_some_and(|value| value.starts_with("ai-coder-session=")),
        "重定向响应须同时换发 Cookie"
    );
}

/// URL token 唯一参数时重定向目标不留问号（旧 `cleanQuery == null` 分支）。
#[tokio::test]
async fn url_token_only_query_redirects_to_bare_path() {
    let state = default_state();
    let token = state.access_tokens.token().to_owned();
    let mut router = build_router(state);
    let (status, headers, _body) = call(
        &mut router,
        request_from(
            LAN_PEER,
            &format!("/remote.html?token={token}"),
            Method::GET,
            &[],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(
        headers
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok()),
        Some("/remote.html")
    );
}

// ── 档 0：静态资源免检 ──────────────────────────────────────────────────

/// `/assets/` `/icons/` `/favicon.ico` 局域网无凭证也不 401（旧
/// `shouldNotFilter`）——静态壳允许加载，业务数据仍需认证。
#[tokio::test]
async fn static_assets_skip_authentication() {
    let mut router = build_router(default_state());
    for path in ["/assets/app.js", "/icons/192.png", "/favicon.ico"] {
        let (status, _headers, _body) = call(&mut router, lan_get(path)).await;
        assert_ne!(status, StatusCode::UNAUTHORIZED, "{path}");
        assert_ne!(status, StatusCode::FORBIDDEN, "{path}");
        // 测试静态根内无这些文件，故落 ServeDir 的 404。
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
    }
}

/// `remote.html` **不**在免检名单内：局域网首访必须带 token（否则 401）。
#[tokio::test]
async fn remote_page_is_guarded() {
    let mut router = build_router(default_state());
    let (status, _headers, _body) = call(&mut router, lan_get("/remote.html")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ── Step 2b-7：静态文件服务 ─────────────────────────────────────────────

/// `remote.html` 经 `ServeDir` 出站（loopback 直取），内容含红色 STOP 按钮与
/// 3s 轮询逻辑。
#[tokio::test]
async fn remote_page_is_served_from_static_dir() {
    let mut router = build_router(default_state());
    let (status, headers, body) = call(&mut router, local_get("/remote.html")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/html")
    );
    let html = String::from_utf8(body.to_vec()).expect("utf-8 html");
    assert!(html.contains("zkcode Local Control"));
    assert!(html.contains("class=\"stop-btn\""));
    assert!(html.contains("/api/remote/status"));
    assert!(html.contains("/api/remote/interrupt"));
    assert!(html.contains("setInterval(fetchStatus, 3000)"));
}

/// 静态根内不存在的文件 → 404（fallback 不吞 API 路由）。
#[tokio::test]
async fn unknown_static_path_is_not_found() {
    let mut router = build_router(default_state());
    let (status, _headers, _body) = call(&mut router, local_get("/does-not-exist.html")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ── Step 2b-4：auth 端点三态（经完整中间件栈）─────────────────────────

/// `lan_token` 模式 + loopback → 200 携守卫同源 token。
#[tokio::test]
async fn auth_token_served_to_loopback_in_lan_token_mode() {
    let state = state_with("lan_token");
    let expected = state.access_tokens.token().to_owned();
    let mut router = build_router(state);
    let (status, _headers, body) = call(&mut router, local_get("/api/auth/token")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["token"], expected);
}

/// `localhost` 模式 + loopback → 404 顶层 error（非错误信封）。
#[tokio::test]
async fn auth_token_not_enabled_in_localhost_mode() {
    let mut router = build_router(default_state());
    let (status, _headers, body) = call(&mut router, local_get("/api/auth/token")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        json_body(&body),
        serde_json::json!({ "error": "Token auth not enabled" })
    );
}

/// `lan_token` 模式 + 局域网带正确 Bearer → 403 顶层 error（守卫放行后由
/// 端点自身的来源判定拦下，token 不外泄给非本机调用方）。
#[tokio::test]
async fn auth_token_denies_authenticated_lan_caller() {
    let state = state_with("lan_token");
    let token = state.access_tokens.token().to_owned();
    let mut router = build_router(state);
    let (status, _headers, body) = call(
        &mut router,
        request_from(LAN_PEER, "/api/auth/token", Method::GET, &bearer(&token)),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        json_body(&body),
        serde_json::json!({ "error": "Token can only be retrieved from localhost" })
    );
}

/// `/api/auth/status`：`lan_token` 模式下按 Bearer 判定，未认证时剥离
/// `username` 键。
#[tokio::test]
async fn auth_status_reflects_lan_token_credentials() {
    let state = state_with("lan_token");
    let token = state.access_tokens.token().to_owned();
    let mut router = build_router(state);
    let (status, _headers, body) = call(
        &mut router,
        request_from(LOCAL_PEER, "/api/auth/status", Method::GET, &bearer(&token)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_body(&body),
        serde_json::json!({
            "authenticated": true,
            "authMode": "lan_token",
            "username": "lan-user",
        })
    );

    let (status, _headers, body) = call(&mut router, local_get("/api/auth/status")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_body(&body),
        serde_json::json!({ "authenticated": false, "authMode": "lan_token" })
    );
}

// ── Step 2b-6：RemoteControl 端点（经完整中间件栈）────────────────────

/// `GET /api/remote/status`：无活跃会话时的三键形状（`serverUptime` 为
/// `Xm` / `Xh Ym` 文本）。
#[tokio::test]
async fn remote_status_returns_snapshot_shape() {
    let mut router = build_router(default_state());
    let (status, _headers, body) = call(&mut router, local_get("/api/remote/status")).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    let mut keys: Vec<&str> = body
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["activeSessions", "serverUptime", "sessions"]);
    assert_eq!(body["activeSessions"], 0);
    assert_eq!(body["sessions"], serde_json::json!([]));
    assert!(
        body["serverUptime"]
            .as_str()
            .is_some_and(|text| text.ends_with('m')),
        "{body}"
    );
}

/// `POST /api/remote/interrupt`：无会话时 `interrupted=true` / `sessionCount=0`
/// 且不带 `partialFailure` 键（旧 `activeIds.isEmpty()` 分支）。
#[tokio::test]
async fn remote_interrupt_succeeds_without_sessions() {
    let mut router = build_router(default_state());
    let (status, _headers, body) = call(
        &mut router,
        request_from(LOCAL_PEER, "/api/remote/interrupt", Method::POST, &[]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_body(&body),
        serde_json::json!({ "interrupted": true, "sessionCount": 0 })
    );
}

/// 远程控制端点同栈过准入守卫：局域网无凭证 401、公网 403。
#[tokio::test]
async fn remote_endpoints_are_guarded() {
    let mut router = build_router(default_state());
    let (status, _headers, _body) = call(&mut router, lan_get("/api/remote/status")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _headers, _body) = call(
        &mut router,
        request_from(PUBLIC_PEER, "/api/remote/interrupt", Method::POST, &[]),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}
