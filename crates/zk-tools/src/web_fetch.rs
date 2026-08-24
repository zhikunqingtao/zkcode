//! Safe, model-callable `WebFetch` facade.
//!
//! Network I/O is inverted through [`WebFetchPort`]. The production server port
//! owns DNS pinning, redirect revalidation, response limits, and timeouts; this
//! crate owns the stable tool schema, content-type policy, and HTML-to-text filter.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::{Value, json};
use url::Url;

use crate::input::{failure, optional_usize, required_str};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// Maximum bytes accepted from a remote response.
/// 对齐旧 `ZhikunCode` `WebFetchTool.java` 实测响应体上限 `10MB`（`10_485_760`）。
pub const MAX_FETCH_BYTES: usize = 10_485_760;
/// Maximum redirect hops. Every hop must be independently resolved and checked.
/// 对齐旧 `ZhikunCode` `OkHttp` 默认重定向跟随上限 20 跳。
pub const MAX_REDIRECTS: usize = 20;
/// Maximum extracted text returned to the model.
/// 对齐旧 `ZhikunCode` 渲染字符上限 `100K`。
pub const MAX_RENDERED_CHARS: usize = 100_000;

/// Bounded request passed to the production HTTP port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebFetchRequest {
    /// Initial HTTP(S) URL.
    pub url: String,
    /// Response body byte cap.
    pub max_bytes: usize,
    /// Redirect hop cap.
    pub max_redirects: usize,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// End-to-end timeout across DNS, redirects, and body streaming.
    pub total_timeout: Duration,
}

/// Bounded response returned by the injected HTTP port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebFetchResponse {
    /// URL after validated redirects.
    pub final_url: String,
    /// HTTP status code.
    pub status: u16,
    /// Normalized response content type without parameters.
    pub content_type: String,
    /// Response bytes, never larger than the requested cap.
    pub body: Vec<u8>,
    /// Whether the remote body exceeded the byte cap.
    pub truncated: bool,
}

/// Stable network failure crossing the server/tool dependency boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebFetchError {
    /// Machine-readable error code.
    pub code: &'static str,
    /// Redacted user-facing reason.
    pub message: String,
}

impl WebFetchError {
    /// Constructs a port error without recording response bodies or credentials.
    #[must_use]
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Server-provided safe HTTP implementation.
pub trait WebFetchPort: Send + Sync {
    /// Fetches one URL with all limits and redirect security enforced.
    fn fetch(
        &self,
        request: WebFetchRequest,
    ) -> BoxFuture<'_, Result<WebFetchResponse, WebFetchError>>;
}

/// Model-callable `WebFetch` tool.
#[derive(Clone)]
pub struct WebFetchTool {
    port: Arc<dyn WebFetchPort>,
}

impl std::fmt::Debug for WebFetchTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebFetchTool")
            .finish_non_exhaustive()
    }
}

impl WebFetchTool {
    /// Constructs a `WebFetch` tool around a server-owned secure transport.
    #[must_use]
    pub fn new(port: Arc<dyn WebFetchPort>) -> Self {
        Self { port }
    }
}

impl Tool for WebFetchTool {
    fn name(&self) -> &'static str {
        "WebFetch"
    }

    fn description(&self) -> &'static str {
        "Fetch public HTTP(S) text content with DNS, redirect, size, and timeout safeguards."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["url"],
            "properties": {
                "url": { "type": "string", "description": "Public http:// or https:// URL" },
                "max_bytes": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_FETCH_BYTES,
                    "description": "Optional response byte cap"
                }
            }
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }

    fn timeout(&self) -> Duration {
        // 对齐旧 ZhikunCode 工具级最大执行 300s（5 分钟）。执行器钳制值为
        // MAX_TOOL_TIMEOUT = 600s（2026-08-23 查证 executor.rs 的
        // `tool.timeout().min(MAX_TOOL_TIMEOUT)`），300s 低于上限可完整生效，
        // 与 zk-mcp 的 MCP_TOOL_MAX_EXECUTION = 5min 保持一致。
        Duration::from_mins(5)
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let Ok(raw_url) = required_str(&input, "url") else {
                return failure("WEB_FETCH_URL_INVALID", "url is required");
            };
            let parsed = match Url::parse(raw_url) {
                Ok(url) if matches!(url.scheme(), "http" | "https") && url.host().is_some() => url,
                _ => {
                    return failure(
                        "WEB_FETCH_URL_INVALID",
                        "only absolute http:// and https:// URLs are allowed",
                    );
                }
            };
            if !parsed.username().is_empty() || parsed.password().is_some() {
                return failure(
                    "WEB_FETCH_URL_INVALID",
                    "URLs containing embedded credentials are not allowed",
                );
            }
            let max_bytes = optional_usize(&input, "max_bytes").unwrap_or(MAX_FETCH_BYTES);
            if max_bytes == 0 || max_bytes > MAX_FETCH_BYTES {
                return failure(
                    "WEB_FETCH_LIMIT_INVALID",
                    format!("max_bytes must be between 1 and {MAX_FETCH_BYTES}"),
                );
            }
            let request = WebFetchRequest {
                url: parsed.to_string(),
                max_bytes,
                max_redirects: MAX_REDIRECTS,
                // 对齐旧 ZhikunCode OkHttp connect/read 各 30s；total 300s 覆盖
                // 最多 20 跳重定向下单跳 connect+read 的最坏情形，并与工具级
                // 5 分钟执行上限一致。
                connect_timeout: Duration::from_secs(30),
                total_timeout: Duration::from_mins(5),
            };
            let response = match self.port.fetch(request).await {
                Ok(response) => response,
                Err(error) => return failure(error.code, error.message),
            };
            let media_type = response
                .content_type
                .split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if !allowed_content_type(&media_type) {
                return failure(
                    "WEB_FETCH_CONTENT_TYPE_BLOCKED",
                    format!("unsupported response content type: {media_type}"),
                );
            }
            let raw = String::from_utf8_lossy(&response.body);
            let extracted = if media_type == "text/html" || media_type == "application/xhtml+xml" {
                html_to_text(&raw)
            } else {
                raw.into_owned()
            };
            let (content, render_truncated) = truncate_chars(&extracted, MAX_RENDERED_CHARS);
            let truncated = response.truncated || render_truncated;
            ToolOutput {
                content,
                // 对齐旧 ZhikunCode：非 2xx 不算工具错误，错误页正文照常返回，
                // 状态码仅透出到 metadata 供模型自我纠正后重试。
                is_error: false,
                metadata: Some(json!({
                    "finalUrl": response.final_url,
                    "status": response.status,
                    "contentType": media_type,
                    "truncated": truncated,
                    "bytes": response.body.len(),
                })),
            }
        })
    }
}

fn allowed_content_type(media_type: &str) -> bool {
    matches!(
        media_type,
        "text/html"
            | "application/xhtml+xml"
            | "text/plain"
            | "text/markdown"
            | "text/csv"
            | "application/json"
            | "application/xml"
            | "text/xml"
    )
}

/// Rejects addresses that must never be targets of model-originated web requests.
/// The production resolver requires every answer for a hostname to pass this test.
#[must_use]
pub fn is_public_web_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_v4(ip),
        IpAddr::V6(ip) => is_public_v6(ip),
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_unspecified()
        || ip == Ipv4Addr::BROADCAST
        || a == 0
        || a >= 240
        || (a == 100 && (64..=127).contains(&b))
        || (a == 198 && (b == 18 || b == 19))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113))
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_public_v4(v4);
    }
    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] == 0x2001 && segments[1] == 0x0db8))
}

fn html_to_text(input: &str) -> String {
    let mut cleaned = input.to_owned();
    for tag in ["script", "style", "noscript", "template"] {
        cleaned = remove_element(&cleaned, tag);
    }
    let mut text = String::with_capacity(cleaned.len());
    let mut in_tag = false;
    for character in cleaned.chars() {
        match character {
            '<' => {
                in_tag = true;
                text.push(' ');
            }
            '>' if in_tag => {
                in_tag = false;
                text.push(' ');
            }
            _ if !in_tag => text.push(character),
            _ => {}
        }
    }
    let decoded = text
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'");
    decoded.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn remove_element(input: &str, tag: &str) -> String {
    let mut output = input.to_owned();
    loop {
        let lower = output.to_ascii_lowercase();
        let Some(start) = lower.find(&format!("<{tag}")) else {
            return output;
        };
        let Some(relative_end) = lower[start..].find(&format!("</{tag}>")) else {
            output.truncate(start);
            return output;
        };
        let end = start + relative_end + tag.len() + 3;
        output.replace_range(start..end, " ");
    }
}

fn truncate_chars(value: &str, limit: usize) -> (String, bool) {
    if value.chars().count() <= limit {
        return (value.to_owned(), false);
    }
    let mut output: String = value.chars().take(limit).collect();
    output.push_str("\n[truncated]");
    (output, true)
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    struct FakePort(Result<WebFetchResponse, WebFetchError>);

    impl WebFetchPort for FakePort {
        fn fetch(
            &self,
            _request: WebFetchRequest,
        ) -> BoxFuture<'_, Result<WebFetchResponse, WebFetchError>> {
            Box::pin(futures::future::ready(self.0.clone()))
        }
    }

    fn context() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    #[test]
    fn blocks_local_private_metadata_and_reserved_addresses() {
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.2.3",
            "192.168.1.2",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "224.0.0.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(!is_public_web_ip(address.parse().expect("IP")), "{address}");
        }
        for address in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
            assert!(is_public_web_ip(address.parse().expect("IP")), "{address}");
        }
    }

    #[tokio::test]
    async fn rejects_non_http_and_embedded_credentials_before_port() {
        let tool = WebFetchTool::new(Arc::new(FakePort(Err(WebFetchError::new(
            "MUST_NOT_CALL",
            "port called",
        )))));
        for url in ["file:///etc/passwd", "http://user:pass@example.com/"] {
            let output = tool.execute(json!({ "url": url }), context()).await;
            assert!(output.is_error);
            assert!(output.content.starts_with("WEB_FETCH_URL_INVALID: "));
        }
    }

    #[tokio::test]
    async fn filters_active_html_and_returns_structured_metadata() {
        let tool = WebFetchTool::new(Arc::new(FakePort(Ok(WebFetchResponse {
            final_url: "https://example.com/final".to_owned(),
            status: 200,
            content_type: "text/html; charset=utf-8".to_owned(),
            body: b"<h1>Hello</h1><script>steal()</script><p>world &amp; friends</p>".to_vec(),
            truncated: false,
        }))));
        let output = tool
            .execute(json!({ "url": "https://example.com" }), context())
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert_eq!(output.content, "Hello world & friends");
        assert!(!output.content.contains("steal"));
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["finalUrl"], "https://example.com/final");
        assert_eq!(metadata["status"], 200);
        assert_eq!(metadata["truncated"], false);
    }

    #[tokio::test]
    async fn blocks_binary_content_types() {
        let tool = WebFetchTool::new(Arc::new(FakePort(Ok(WebFetchResponse {
            final_url: "https://example.com/a.zip".to_owned(),
            status: 200,
            content_type: "application/zip".to_owned(),
            body: vec![1, 2, 3],
            truncated: false,
        }))));
        let output = tool
            .execute(json!({ "url": "https://example.com/a.zip" }), context())
            .await;
        assert!(output.is_error);
        assert!(
            output
                .content
                .starts_with("WEB_FETCH_CONTENT_TYPE_BLOCKED: ")
        );
    }

    /// 逐项对齐旧 `ZhikunCode` `WebFetchTool.java` 实测值（2026-08-23 对齐）：
    /// 响应体 `10MB`、重定向 20 跳（`OkHttp` 默认）、渲染字符 `100K`、
    /// 工具级最大执行 `300s`（`OkHttp` connect/read `30s` 由端口侧断言）。
    #[test]
    fn limits_and_timeout_match_legacy_zhikuncode_webfetch() {
        assert_eq!(MAX_FETCH_BYTES, 10_485_760);
        assert_eq!(MAX_REDIRECTS, 20);
        assert_eq!(MAX_RENDERED_CHARS, 100_000);
        let tool = WebFetchTool::new(Arc::new(FakePort(Err(WebFetchError::new(
            "MUST_NOT_CALL",
            "port called",
        )))));
        assert_eq!(tool.timeout(), Duration::from_mins(5));
    }

    /// 对齐旧 `ZhikunCode`：非 2xx 不算工具错误，错误页正文照常返回，
    /// 状态码仅透出到 metadata 供模型自我纠正后重试。
    #[tokio::test]
    async fn non_2xx_status_returns_body_without_error() {
        let tool = WebFetchTool::new(Arc::new(FakePort(Ok(WebFetchResponse {
            final_url: "https://example.com/missing".to_owned(),
            status: 404,
            content_type: "text/plain; charset=utf-8".to_owned(),
            body: b"the page you requested was not found".to_vec(),
            truncated: false,
        }))));
        let output = tool
            .execute(json!({ "url": "https://example.com/missing" }), context())
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert_eq!(output.content, "the page you requested was not found");
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["status"], 404);
        assert_eq!(metadata["finalUrl"], "https://example.com/missing");
        assert_eq!(metadata["truncated"], false);
    }
}
