//! Admin 管理面板认证端点（Batch 8B Step 7，旧 `controller/AdminController.java`
//! 173 行逐分支复刻）。
//!
//! 三端点：
//! - `POST /api/admin/login`：SHA-256 校验密码 → 签发 `ai-coder-admin-session`
//!   `HttpOnly` Cookie（8 小时有效期）。
//! - `GET /api/admin/status`：按 Cookie 或 `Bearer` token 判定当前登录态。
//! - `POST /api/admin/logout`：`Max-Age=0` 清空同名 Cookie。
//!
//! # 认证模型
//!
//! 逐字沿用旧源：Cookie 的**值本身**即 `SHA-256(password)` 的十六进制串，服务端
//! 不另建 session store——校验只是把请求携带的哈希与配置哈希做定长比较。密码从
//! 环境变量 `ZK_ADMIN_PASSWORD` 装配（[`crate::config::Config::admin_password_hash`]
//! 启动期一次性哈希）；未配置时 `login` 回 503、`status.configured=false`。
//!
//! # 与 [`crate::access_token`] 的关系
//!
//! 仅复用其定长比较原语 [`crate::access_token::constant_time_eq`]，**不**共享
//! `AccessTokenManager` 的会话表——admin 与局域网访问是两套独立凭证域，混用会
//! 让持局域网 Cookie 者意外获得 admin 视图。

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::access_token::constant_time_eq;
use crate::state::AppState;

/// Admin session Cookie 名称（旧 `ADMIN_COOKIE_NAME`）。
const ADMIN_COOKIE_NAME: &str = "ai-coder-admin-session";

/// Admin Cookie 有效期秒数（旧 `Duration.ofHours(8)`）。
const ADMIN_COOKIE_MAX_AGE_SECS: u64 = 8 * 60 * 60;

/// `POST /api/admin/login` 请求体（旧 `record LoginRequest(String password)`）。
///
/// `password` 以 `Option` 表达 Java 的可空字段——缺键 / 显式 `null` 均落
/// `None`，与 `password().isBlank()` 前的 null 判定同形。
#[derive(Debug, Default, Deserialize)]
pub(crate) struct LoginRequest {
    /// 明文密码（空 / 空白 / 缺失均视为未提供）。
    #[serde(default)]
    password: Option<String>,
}

/// `SHA-256` 十六进制摘要（旧 `hashPassword`：`MessageDigest` + `%02x`）。
///
/// 供本模块登录校验与 [`crate::config`] 启动期装配 `admin_password_hash` 共用，
/// 保证「配置侧算的哈希」与「请求侧算的哈希」出自同一实现。
#[must_use]
pub(crate) fn sha256_hex(input: &str) -> String {
    Sha256::digest(input.as_bytes())
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// 当前 ISO-8601 时间戳（旧 `Instant.now().toString()`）。
fn now_iso() -> String {
    crate::iso::format_rfc3339_micros(crate::iso::now_millis())
}

/// `POST /api/admin/login`——密码校验 + 签发 Cookie（旧 `login` L61-98）。
///
/// 分支逐字对齐：未配置 → 503；密码缺失 / 空白 → 400；哈希不匹配 → 401（且
/// warn 记录尝试）；匹配 → Set-Cookie（值即 `providedHash`）+ 200。
#[utoipa::path(
    post,
    path = "/api/admin/login",
    tag = "admin",
    responses((status = 200, description = "登录成功（并 Set-Cookie）；503 未配置 / 400 缺密码 / 401 密码错误"))
)]
pub(crate) async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Response {
    let Some(configured_hash) = state.config.admin_password_hash.as_deref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "success": false, "message": "Admin authentication not configured" })),
        )
            .into_response();
    };

    let password = req.password.as_deref().unwrap_or_default();
    if password.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "success": false, "message": "Password is required" })),
        )
            .into_response();
    }

    let provided_hash = sha256_hex(password);
    if !constant_time_eq(configured_hash.as_bytes(), provided_hash.as_bytes()) {
        tracing::warn!(at = %now_iso(), "Failed admin login attempt");
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "success": false, "message": "Invalid password" })),
        )
            .into_response();
    }

    // 签发 admin session cookie：值即 providedHash（旧同）。secure(false) → 不带
    // Secure 标志，故此处只列 HttpOnly / SameSite=Lax / Path=/ / Max-Age。
    let cookie = format!(
        "{ADMIN_COOKIE_NAME}={provided_hash}; Max-Age={ADMIN_COOKIE_MAX_AGE_SECS}; Path=/; HttpOnly; SameSite=Lax"
    );
    tracing::info!(at = %now_iso(), "Successful admin login");
    (
        StatusCode::OK,
        [(header::SET_COOKIE, cookie)],
        Json(json!({
            "success": true,
            "message": "Login successful",
            "admin": true,
            "timestamp": now_iso(),
        })),
    )
        .into_response()
}

/// `GET /api/admin/status`——登录态查询（旧 `status` L103-129）。
///
/// 认证判定二选一：Cookie 值 == 配置哈希，或 `Bearer` token 的 `SHA-256` ==
/// 配置哈希。未配置密码时 `authenticated` 恒 `false`、`configured=false`。
#[utoipa::path(
    get,
    path = "/api/admin/status",
    tag = "admin",
    responses((status = 200, description = "当前登录态 / 是否已配置 / 时间戳"))
)]
pub(crate) async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Json<serde_json::Value> {
    let configured_hash = state.config.admin_password_hash.as_deref();
    let mut authenticated = false;

    if let Some(hash) = configured_hash {
        // Cookie 校验：值直接就是哈希，定长比较。
        if let Some(cookie) = admin_cookie_value(&headers)
            && constant_time_eq(hash.as_bytes(), cookie.as_bytes())
        {
            authenticated = true;
        }
        // Bearer token 校验：先哈希 token 再比较（旧 L116-122）。
        if !authenticated && let Some(token) = bearer_token(&headers) {
            let token_hash = sha256_hex(&token);
            if constant_time_eq(hash.as_bytes(), token_hash.as_bytes()) {
                authenticated = true;
            }
        }
    }

    Json(json!({
        "authenticated": authenticated,
        "configured": configured_hash.is_some(),
        "timestamp": now_iso(),
    }))
}

/// `POST /api/admin/logout`——清空 Cookie（旧 `logout` L134-151）。
///
/// `Max-Age=0` 即令浏览器立即过期；服务端无 session store 故无额外状态可清。
#[utoipa::path(
    post,
    path = "/api/admin/logout",
    tag = "admin",
    responses((status = 200, description = "已注销（Set-Cookie Max-Age=0）"))
)]
pub(crate) async fn logout() -> Response {
    let cookie = format!("{ADMIN_COOKIE_NAME}=; Max-Age=0; Path=/; HttpOnly; SameSite=Lax");
    tracing::info!(at = %now_iso(), "Admin logout");
    (
        StatusCode::OK,
        [(header::SET_COOKIE, cookie)],
        Json(json!({ "success": true, "message": "Logged out successfully" })),
    )
        .into_response()
}

/// 取 admin session Cookie 值（同 [`crate::middleware`] 的会话 Cookie 解析：
/// 多头与单头内多对都展开）。
fn admin_cookie_value(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(name, _)| *name == ADMIN_COOKIE_NAME)
        .map(|(_, value)| value.to_owned())
}

/// 取 `Bearer` token（scheme 大小写敏感，同旧 `startsWith("Bearer ")`）。
fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_owned)
}
