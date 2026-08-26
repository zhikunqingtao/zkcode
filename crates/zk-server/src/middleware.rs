//! HTTP 中间件——请求观测（§17）与三层递进准入守卫（D7 / Batch 2b）。
//!
//! # 观测（`observe`）
//!
//! 每请求记录 method / path / route（`MatchedPath` 低基数模板）/ status /
//! `elapsed_ms` / `request_id`（透传上游 `x-request-id` 或生成 UUIDv4），并打入
//! metrics facade 计数器与直方图。§19 脱敏：**不**记录请求/响应体；仅路径与
//! 状态等无业务载荷字段。
//!
//! # 三层递进准入（`access_guard`）
//!
//! 旧 `config/RemoteAccessSecurityFilter.doFilterInternal`（L108-174）的逐档复刻。
//! 对端地址经 `into_make_service_with_connect_info` 注入（旧
//! `request.getRemoteAddr()`）；缺失视为不可信直接 403——只有装配错误才会让它缺失，
//! 宁可拒绝也不放过。
//!
//! 判定顺序（命中即返回；顺序本身是安全语义，不得重排）：
//!
//! | 档 | 条件 | 结果 | 旧源 |
//! |---|---|---|---|
//! | 0 | 路径命中静态资源前缀 | 免检放行 | `shouldNotFilter` L300-304 |
//! | 1 | 对端 loopback | 放行 | L116-120 |
//! | 2 | 非私有网段 | 403 `ACCESS_DENIED` | L134-139 |
//! | 3 | Cookie `ai-coder-session` 有效 | 放行 | L144-148 |
//! | 4 | `Authorization: Bearer {token}` 命中 | 签发 Cookie + 放行 | L151-159 |
//! | 5 | `?token={token}` 命中 | 签发 Cookie + 302 去参重定向 | L162-169 |
//! | 6 | 以上全落空 | 401 `AUTHENTICATION_REQUIRED` | L172-173 |
//!
//! 档 1 的判定复刻旧源三个字符串常量（`127.0.0.1` / `0:0:0:0:0:0:0:1` / `::1`）
//! ——它们在 `SocketAddr` 解析下即 `is_loopback()`。档 5 的重定向是防泄露措施：
//! token 留在地址栏 / 浏览历史 / `Referer` 里等于长期凭证外泄，故一旦换成 Cookie
//! 就立刻把 URL 上的 token 抹掉。
//!
//! # 偏离留痕
//!
//! - **B2B-11 `?token=` 只解析 query string**：旧
//!   `request.getParameter("token")` 同时读 query 与
//!   `application/x-www-form-urlencoded` 请求体。中间件读请求体要缓冲整包并破坏
//!   流式转发（`/api/attachments/upload` 传输上限 64 MiB），而 zkcode 的 API 全为
//!   JSON 体、token 只可能出现在 URL 上（`remote.html?token=` 与手机端首访），故只
//!   解析 query string。
//! - **B2B-12 重定向 `Location` 用相对形式**：旧
//!   `sendRedirect(getRequestURL() + "?" + cleanQuery)` 给绝对 URL（scheme / host
//!   取自请求行与 `Host` 头）。本实现回 `path[?query]` 相对形式——RFC 7231 §7.1.2
//!   明确允许，且不依赖可被伪造的 `Host` 头。
//! - **B2B-13 403 / 401 走项目错误响应**：旧 `sendError` 把渲染交给容器（HTML
//!   错误页）。本实现回 `{code,message,requestId}`（message 逐字照抄旧文案），
//!   与其余端点同形；`remote.html` 与前端都只看状态码。
//! - **B2B-14 URL token 不做百分号解码**：旧 `getParameter` 返回解码后的值。token
//!   取值域是 `Base64URL` 字母表（`A-Za-z0-9-_`）——全为 URL 未保留字符，编码前后逐
//!   字节相同，故直接按原文比较；畸形编码值只会匹配失败落到 401。

use std::net::SocketAddr;
use std::time::Instant;

use axum::body::HttpBody as _;
use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::access_token::COOKIE_NAME;
use crate::error::ApiError;
use crate::state::AppState;

/// 响应头：请求标识（客户端可回传以便排障对账）。
static REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// 免准入检查的静态资源前缀（旧 `shouldNotFilter` L302）。
const STATIC_ASSET_PREFIXES: [&str; 2] = ["/assets/", "/icons/"];

/// 免准入检查的单文件路径（旧 L303 `path.equals("/favicon.ico")`）。
const FAVICON_PATH: &str = "/favicon.ico";

/// URL token 参数名（旧 L162 `request.getParameter("token")`）。
const TOKEN_QUERY_PARAM: &str = "token";

/// 请求观测中间件（日志 + metrics；注册顺序见 `routes`）。
pub(crate) async fn observe(request: Request, next: Next) -> Response {
    let started = Instant::now();
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| path.clone(), |matched| matched.as_str().to_owned());
    let request_id = request
        .headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);

    let mut response = next.run(request).await;

    let status = response.status();
    let elapsed = started.elapsed();
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    record_request_metrics(&method, &route, status, elapsed);
    if response.headers().get(&REQUEST_ID_HEADER).is_none()
        && let Ok(value) = HeaderValue::from_str(&request_id)
    {
        response.headers_mut().insert(&REQUEST_ID_HEADER, value);
    }
    tracing::info!(
        method = %method,
        path = %path,
        route = %route,
        status = status.as_u16(),
        elapsed_ms,
        request_id = %request_id,
        "http_request"
    );
    response
}

/// 请求指标打点（显式 `Key::from_parts`：method/route/status 为动态标签值，
/// `counter!` 宏的静态缓存臂不支持运行时标签值，故走 `with_recorder`）。
fn record_request_metrics(
    method: &axum::http::Method,
    route: &str,
    status: axum::http::StatusCode,
    elapsed: std::time::Duration,
) {
    let metadata = metrics::Metadata::new(
        env!("CARGO_CRATE_NAME"),
        metrics::Level::INFO,
        Some(module_path!()),
    );
    metrics::with_recorder(|recorder| {
        let counter_key = metrics::Key::from_parts(
            "http_requests_total",
            vec![
                metrics::Label::new("method", method.as_str().to_owned()),
                metrics::Label::new("route", route.to_owned()),
                metrics::Label::new("status", status.as_u16().to_string()),
            ],
        );
        recorder
            .register_counter(&counter_key, &metadata)
            .increment(1);
        let histogram_key = metrics::Key::from_parts(
            "http_request_duration_seconds",
            vec![metrics::Label::new("route", route.to_owned())],
        );
        recorder
            .register_histogram(&histogram_key, &metadata)
            .record(elapsed.as_secs_f64());
    });
}

/// 三层递进准入中间件（旧 `RemoteAccessSecurityFilter.doFilterInternal`
/// L108-174；档序与逐行对照见模块文档）。
pub(crate) async fn access_guard(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    // 档 0：静态资源不拦（旧 `shouldNotFilter` L300-304）。
    if is_static_asset(request.uri().path()) {
        return next.run(request).await;
    }
    let Some(peer) = peer_addr(&request) else {
        // §19：仅记拒绝事实与对端地址，不落任何请求内容。
        tracing::warn!("peer address missing from request extensions, denying");
        return ApiError::access_denied().into_response();
    };
    // 档 1：localhost 免认证（旧 L116-120）。
    if is_loopback(peer) {
        return next.run(request).await;
    }
    // 档 2：非私有网段直接拒（旧 L133-139）。
    if !crate::network::is_private_network(peer.ip()) {
        tracing::warn!(peer = %peer, "Access denied for non-private IP");
        return ApiError::access_denied().into_response();
    }
    // 档 3：会话 Cookie（旧 L143-148）。
    let session_cookie = session_cookie_value(request.headers());
    if let Some(session_id) = session_cookie
        && state.access_tokens.validate_session(&session_id)
    {
        return next.run(request).await;
    }
    // 档 4：`Authorization: Bearer {token}`（旧 L150-159）——命中即换发 Cookie，
    // 后续请求不必再带 token。
    let bearer = bearer_token(request.headers());
    if let Some(token) = bearer
        && state.access_tokens.matches(&token)
    {
        let cookie = state.access_tokens.issue_session_cookie();
        let mut response = next.run(request).await;
        attach_session_cookie(&mut response, &cookie);
        return response;
    }
    // 档 5：`?token={token}`（旧 L161-169）——换发 Cookie 后 302 去参重定向。
    let url_token = query_token(request.uri());
    if let Some((token, legacy_name)) = url_token
        && state.access_tokens.matches(&token)
    {
        if legacy_name {
            tracing::warn!(
                metric = "ws_access_token_query_deprecated",
                "access_token query parameter is deprecated; use token"
            );
        }
        let location = location_without_token(request.uri());
        let cookie = state.access_tokens.issue_session_cookie();
        let mut response = (StatusCode::FOUND, [(header::LOCATION, location)]).into_response();
        attach_session_cookie(&mut response, &cookie);
        return response;
    }
    // 档 6：全部落空（旧 L171-173）。
    tracing::warn!(peer = %peer, "authentication required, denying");
    ApiError::unauthorized().into_response()
}

/// CSRF/content-type boundary for state-changing MCP routes and the WebSocket
/// upgrade endpoint.
///
/// Loopback alone is not sufficient here: a hostile web page can send a
/// browser request to `127.0.0.1`.  Every MCP mutation and WebSocket upgrade
/// therefore needs either an exact trusted browser Origin or the server's
/// local Bearer access token.
/// JSON-bearing endpoints additionally reject simple-request content types,
/// so a cross-origin form/text request cannot reach a parser before CORS is
/// considered by the browser.
pub(crate) async fn mcp_mutation_guard(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    if !is_mcp_protected_request(request.method(), request.uri().path()) {
        return next.run(request).await;
    }

    let trusted_origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|origin| is_trusted_mcp_origin(&state, origin));
    let trusted_token =
        bearer_token(request.headers()).is_some_and(|token| state.access_tokens.matches(&token));
    // Same-origin browser GETs commonly omit Origin. `Sec-Fetch-Site` is a
    // forbidden request header in browsers, so page JavaScript cannot forge
    // `same-origin`.  This fallback is limited to the read endpoints that can
    // trigger outbound MCP calls; mutations and WebSocket upgrades still need
    // an exact Origin or Bearer token.
    let trusted_same_origin_fetch =
        is_network_backed_mcp_get(request.method(), request.uri().path())
            && request.headers().get(header::ORIGIN).is_none()
            && request
                .headers()
                .get("sec-fetch-site")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value == "same-origin");
    if !trusted_origin && !trusted_token && !trusted_same_origin_fetch {
        tracing::warn!(
            path = request.uri().path(),
            "MCP request rejected: trusted Origin or Bearer token required"
        );
        return ApiError {
            status: StatusCode::FORBIDDEN,
            code: "MCP_REQUEST_ORIGIN_DENIED".to_owned(),
            message: "MCP network operations require a trusted local Origin or Bearer access token"
                .to_owned(),
        }
        .into_response();
    }

    // Preserve the endpoint's stable INVALID_REQUEST_BODY response for an
    // actually empty body. A non-empty JSON-bearing request must still carry
    // application/json, which blocks browser-simple CSRF payloads.
    let body_is_empty = request.body().size_hint().exact() == Some(0);
    if !body_is_empty
        && mcp_route_requires_json(request.method(), request.uri().path())
        && !has_json_content_type(request.headers())
    {
        return ApiError {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            code: "MCP_CONTENT_TYPE_UNSUPPORTED".to_owned(),
            message: "MCP request Content-Type must be application/json".to_owned(),
        }
        .into_response();
    }
    next.run(request).await
}

fn is_mcp_protected_request(method: &Method, path: &str) -> bool {
    (path == "/ws" && *method == Method::GET)
        || (path == "/api/llm-keys" && *method == Method::PUT)
        || path == "/mcp"
        || is_network_backed_mcp_get(method, path)
        || (path.starts_with("/api/mcp/")
            && (method == Method::POST
                || method == Method::PUT
                || method == Method::PATCH
                || method == Method::DELETE))
}

fn is_network_backed_mcp_get(method: &Method, path: &str) -> bool {
    *method == Method::GET
        && (matches!(
            path,
            "/api/mcp/resources" | "/api/mcp/resources/read" | "/api/mcp/prompts"
        ) || (path.starts_with("/api/mcp/capabilities/") && path.ends_with("/server-tools")))
}

fn mcp_route_requires_json(method: &Method, path: &str) -> bool {
    (path == "/api/llm-keys" && *method == Method::PUT)
        || path == "/mcp"
        || (*method == Method::POST
            && matches!(
                path,
                "/api/mcp/servers" | "/api/mcp/prompts/execute" | "/api/mcp/capabilities"
            ))
        || (*method == Method::PUT && path.starts_with("/api/mcp/capabilities/"))
        || (*method == Method::POST
            && path.starts_with("/api/mcp/capabilities/")
            && path.ends_with("/invoke"))
}

fn has_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

fn is_trusted_mcp_origin(state: &AppState, origin: &str) -> bool {
    crate::routes::BASE_CORS_ORIGINS.contains(&origin)
        || origin == format!("http://127.0.0.1:{}", state.config.port)
        || origin == format!("http://localhost:{}", state.config.port)
        || state
            .config
            .extra_cors_origins
            .iter()
            .any(|allowed| origin == allowed)
}

/// 对端地址（`into_make_service_with_connect_info` 注入；旧
/// `request.getRemoteAddr()`）。
fn peer_addr(request: &Request) -> Option<SocketAddr> {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0)
}

/// 对端是否 loopback（旧三值 `127.0.0.1` / `0:0:0:0:0:0:0:1` / `::1`
/// 在标准解析下即 `is_loopback()`）。
fn is_loopback(addr: SocketAddr) -> bool {
    addr.ip().is_loopback()
}

/// 路径是否免准入检查（旧 `shouldNotFilter` L300-304）。
fn is_static_asset(path: &str) -> bool {
    STATIC_ASSET_PREFIXES
        .iter()
        .any(|prefix| path.starts_with(prefix))
        || path == FAVICON_PATH
}

/// 取会话 Cookie 值（旧 `getCookieValue(request, COOKIE_NAME)` L205-213）。
///
/// 多个 `Cookie` 头与单头内多对都要展开——旧端由 Servlet 容器统一解析成
/// `Cookie[]`，此处自持等价拆分。
fn session_cookie_value(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(name, _)| *name == COOKIE_NAME)
        .map(|(_, value)| value.to_owned())
}

/// 取 Bearer token（旧 L151-153 `startsWith("Bearer ")` + `substring(7)`；
/// scheme 大小写敏感，同旧源）。
fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_owned)
}

/// 取 URL token（旧 `getParameter("token")` 的 query 半边，取首个同名参数；
/// 解析范围与解码差异见偏离 B2B-11 / B2B-14）。
fn query_token(uri: &Uri) -> Option<(String, bool)> {
    let pairs: Vec<(&str, &str)> = uri
        .query()?
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .collect();
    pairs
        .iter()
        .find(|(name, _)| *name == TOKEN_QUERY_PARAM)
        .map(|(_, value)| ((*value).to_owned(), false))
        .or_else(|| {
            pairs
                .iter()
                .find(|(name, _)| *name == "access_token")
                .map(|(_, value)| ((*value).to_owned(), true))
        })
}

/// 抹掉 `token` 参数后的重定向目标（旧 `removeTokenParam` L217-228；相对形式见
/// 偏离 B2B-12）。
///
/// 逐字复刻旧源的前缀过滤语义：只丢弃以 `token=` 开头的片段——`tokens=1` 与
/// 光秃秃的 `token`（无 `=`）都保留。清空后不留问号，同旧源 `cleanQuery == null`
/// 分支。
fn location_without_token(uri: &Uri) -> String {
    let path = uri.path();
    let Some(query) = uri.query() else {
        return path.to_owned();
    };
    let kept: Vec<&str> = query
        .split('&')
        .filter(|pair| !pair.starts_with("token=") && !pair.starts_with("access_token="))
        .collect();
    if kept.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{}", kept.join("&"))
    }
}

/// 追加 `Set-Cookie`（旧 L192 `response.addHeader`——append 而非覆盖，不影响
/// handler 自己写的其它 Cookie）。
fn attach_session_cookie(response: &mut Response, cookie: &str) {
    match HeaderValue::from_str(cookie) {
        Ok(value) => {
            response.headers_mut().append(header::SET_COOKIE, value);
        }
        // 会话 id 是 UUID、其余属性为字面量，此分支不可达；仍显式记账而非静默。
        Err(error) => tracing::error!(%error, "session cookie is not a valid header value"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_detection() {
        for ok in ["127.0.0.1:9", "[::1]:9"] {
            let addr: SocketAddr = ok.parse().expect("valid addr");
            assert!(is_loopback(addr), "{ok} should be loopback");
        }
        for deny in ["192.168.1.5:9", "10.0.0.2:9", "[2001:db8::1]:9"] {
            let addr: SocketAddr = deny.parse().expect("valid addr");
            assert!(!is_loopback(addr), "{deny} should not be loopback");
        }
    }

    /// 构造请求头表（同名头允许多条，模拟浏览器多 `Cookie` 头）。
    fn headers_from(pairs: &[(HeaderName, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.append(name.clone(), HeaderValue::from_str(value).expect("header"));
        }
        headers
    }

    /// 静态资源免检清单逐字对齐旧 `shouldNotFilter`（前缀两条 + favicon 精确匹配）。
    #[test]
    fn static_asset_exemptions_match_legacy_list() {
        for exempt in ["/assets/app.js", "/icons/192.png", "/favicon.ico"] {
            assert!(is_static_asset(exempt), "{exempt} 应免检");
        }
        for guarded in [
            "/assets",
            "/icons",
            "/favicon.ico/x",
            "/remote.html",
            "/api/health",
            "/",
        ] {
            assert!(!is_static_asset(guarded), "{guarded} 必须走准入");
        }
    }

    /// Cookie 提取：跨多头 / 同头多对 / 名称精确匹配。
    #[test]
    fn session_cookie_is_extracted_across_headers() {
        let headers = headers_from(&[
            (header::COOKIE, "theme=dark; other=1"),
            (header::COOKIE, "ai-coder-session=abc-123; extra=2"),
        ]);
        assert_eq!(session_cookie_value(&headers).as_deref(), Some("abc-123"));
        let unrelated = headers_from(&[(header::COOKIE, "ai-coder-sessions=abc; x=1")]);
        assert_eq!(session_cookie_value(&unrelated), None);
        assert_eq!(session_cookie_value(&HeaderMap::new()), None);
    }

    /// Bearer 提取：scheme 大小写敏感、必须带单空格分隔（同旧 `startsWith`）。
    #[test]
    fn bearer_token_requires_exact_scheme() {
        let ok = headers_from(&[(header::AUTHORIZATION, "Bearer secret-value")]);
        assert_eq!(bearer_token(&ok).as_deref(), Some("secret-value"));
        for bad in ["bearer secret", "Bearer", "Basic secret", "Bearer\tsecret"] {
            let headers = headers_from(&[(header::AUTHORIZATION, bad)]);
            assert_eq!(bearer_token(&headers), None, "{bad} 不应被当作 Bearer");
        }
        // 空 token 会被取出，但 `matches` 恒拒（token 长 43 字符）。
        let empty = headers_from(&[(header::AUTHORIZATION, "Bearer ")]);
        assert_eq!(bearer_token(&empty).as_deref(), Some(""));
    }

    /// URL token 提取：取首个同名参数、不与同前缀参数混淆。
    #[test]
    fn query_token_takes_first_exact_parameter() {
        let cases = [
            ("/remote.html?token=abc", Some(("abc", false))),
            ("/x?a=1&token=abc&token=def", Some(("abc", false))),
            ("/x?access_token=legacy", Some(("legacy", true))),
            (
                "/x?access_token=legacy&token=current",
                Some(("current", false)),
            ),
            ("/x?tokens=abc", None),
            ("/x?token", None),
            ("/x", None),
        ];
        for (raw, expected) in cases {
            let uri: Uri = raw.parse().expect("uri");
            assert_eq!(
                query_token(&uri)
                    .as_ref()
                    .map(|(value, legacy)| (value.as_str(), *legacy)),
                expected,
                "{raw}"
            );
        }
    }

    /// 去参重定向目标逐字对齐旧 `removeTokenParam`（含「清空后不留问号」）。
    #[test]
    fn location_drops_only_token_parameter() {
        let cases = [
            ("/remote.html?token=abc", "/remote.html"),
            ("/remote.html?token=abc&view=1", "/remote.html?view=1"),
            ("/x?a=1&token=abc&b=2", "/x?a=1&b=2"),
            ("/x?tokens=1&token=abc", "/x?tokens=1"),
            ("/ws?access_token=abc", "/ws"),
            ("/x?token", "/x?token"),
            ("/x", "/x"),
        ];
        for (raw, expected) in cases {
            let uri: Uri = raw.parse().expect("uri");
            assert_eq!(location_without_token(&uri), expected, "{raw}");
        }
    }

    #[test]
    fn websocket_upgrade_is_protected_but_does_not_require_json() {
        assert!(is_mcp_protected_request(&Method::GET, "/ws"));
        assert!(!mcp_route_requires_json(&Method::GET, "/ws"));
    }

    #[test]
    fn outbound_mcp_gets_are_protected() {
        for path in [
            "/api/mcp/resources",
            "/api/mcp/resources/read",
            "/api/mcp/prompts",
            "/api/mcp/capabilities/weather/server-tools",
        ] {
            assert!(
                is_mcp_protected_request(&Method::GET, path),
                "{path} can trigger a network request"
            );
        }
        assert!(!is_mcp_protected_request(&Method::GET, "/api/mcp/servers"));
    }
}
