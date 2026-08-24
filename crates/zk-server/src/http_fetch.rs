//! Production `WebFetch` HTTP port with DNS pinning and per-redirect SSRF checks.

use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};

use futures::{StreamExt, future::BoxFuture};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue, LOCATION};
use reqwest::{Client, StatusCode, Url, redirect::Policy};
use zk_tools::{WebFetchError, WebFetchPort, WebFetchRequest, WebFetchResponse, is_public_web_ip};

/// Stateless secure HTTP implementation injected into `WebFetchTool`.
#[derive(Clone, Copy, Debug, Default)]
pub struct SafeHttpFetchPort;

impl SafeHttpFetchPort {
    /// Constructs the production port. A new pinned client is built for every hop.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl WebFetchPort for SafeHttpFetchPort {
    fn fetch(
        &self,
        request: WebFetchRequest,
    ) -> BoxFuture<'_, Result<WebFetchResponse, WebFetchError>> {
        Box::pin(async move {
            let total_timeout = request.total_timeout;
            let request_url = request.url.clone();
            match tokio::time::timeout(total_timeout, fetch_bounded(request)).await {
                Ok(result) => result,
                Err(_) => Err(WebFetchError::new(
                    "WEB_FETCH_TIMEOUT",
                    // 文案带 URL 与超时时长（URL 为模型输入，非解析地址），
                    // 对齐旧 ZhikunCode 错误信息可让模型自我纠正重试。
                    format!(
                        "web fetch timed out after {}s: {}",
                        total_timeout.as_secs(),
                        request_url
                    ),
                )),
            }
        })
    }
}

async fn fetch_bounded(request: WebFetchRequest) -> Result<WebFetchResponse, WebFetchError> {
    let mut current = parse_safe_url(&request.url)?;
    for hop in 0..=request.max_redirects {
        let (host, addresses) = resolve_public_addresses(&current).await?;
        let client = pinned_client(&host, &addresses, request.connect_timeout)?;
        let response = client.get(current.clone()).send().await.map_err(|error| {
            let code = if error.is_timeout() {
                "WEB_FETCH_TIMEOUT"
            } else {
                "WEB_FETCH_NETWORK_ERROR"
            };
            // 透出 reqwest 错误摘要（含 URL 与 connection refused / dns error 等
            // 底层原因，不含响应体与凭证），帮助模型自我纠正 URL 后重试。
            WebFetchError::new(code, format!("remote request failed: {error}"))
        })?;
        if is_redirect(response.status()) {
            if hop == request.max_redirects {
                return Err(WebFetchError::new(
                    "WEB_FETCH_REDIRECT_LIMIT",
                    "remote response exceeded the redirect limit",
                ));
            }
            let location = response.headers().get(LOCATION).ok_or_else(|| {
                WebFetchError::new(
                    "WEB_FETCH_REDIRECT_INVALID",
                    "redirect response did not include a valid Location",
                )
            })?;
            let location = location.to_str().map_err(|_| {
                WebFetchError::new(
                    "WEB_FETCH_REDIRECT_INVALID",
                    "redirect Location is not valid text",
                )
            })?;
            current = parse_safe_url(
                current
                    .join(location)
                    .map_err(|_| {
                        WebFetchError::new(
                            "WEB_FETCH_REDIRECT_INVALID",
                            "redirect Location is not a valid URL",
                        )
                    })?
                    .as_str(),
            )?;
            continue;
        }

        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let declared_too_large = response
            .content_length()
            .is_some_and(|length| length > request.max_bytes as u64);
        let mut body = Vec::with_capacity(
            usize::try_from(
                response
                    .content_length()
                    .unwrap_or_default()
                    .min(u64::try_from(request.max_bytes).unwrap_or(u64::MAX)),
            )
            .unwrap_or(request.max_bytes),
        );
        let mut stream = response.bytes_stream();
        let mut truncated = declared_too_large;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| {
                WebFetchError::new(
                    "WEB_FETCH_NETWORK_ERROR",
                    "remote response body could not be read",
                )
            })?;
            let remaining = request.max_bytes.saturating_sub(body.len());
            if chunk.len() > remaining {
                body.extend_from_slice(&chunk[..remaining]);
                truncated = true;
                break;
            }
            body.extend_from_slice(&chunk);
            if body.len() == request.max_bytes {
                if stream.next().await.is_some() {
                    truncated = true;
                }
                break;
            }
        }
        return Ok(WebFetchResponse {
            final_url: current.to_string(),
            status,
            content_type,
            body,
            truncated,
        });
    }
    Err(WebFetchError::new(
        "WEB_FETCH_REDIRECT_LIMIT",
        "remote response exceeded the redirect limit",
    ))
}

fn parse_safe_url(raw: &str) -> Result<Url, WebFetchError> {
    let url = Url::parse(raw)
        .map_err(|_| WebFetchError::new("WEB_FETCH_URL_INVALID", "remote URL is not valid"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
        return Err(WebFetchError::new(
            "WEB_FETCH_URL_INVALID",
            "only absolute http:// and https:// URLs are allowed",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(WebFetchError::new(
            "WEB_FETCH_URL_INVALID",
            "URLs containing embedded credentials are not allowed",
        ));
    }
    Ok(url)
}

async fn resolve_public_addresses(url: &Url) -> Result<(String, Vec<SocketAddr>), WebFetchError> {
    let raw_host = url
        .host_str()
        .ok_or_else(|| WebFetchError::new("WEB_FETCH_URL_INVALID", "remote URL has no host"))?;
    let host = raw_host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(raw_host);
    let port = url.port_or_known_default().ok_or_else(|| {
        WebFetchError::new("WEB_FETCH_URL_INVALID", "remote URL has no usable port")
    })?;
    let addresses = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, port)]
    } else {
        tokio::net::lookup_host((host, port))
            .await
            .map_err(|_| {
                // 文案只带模型输入的主机名，不拼接解析器内部错误串。
                WebFetchError::new(
                    "WEB_FETCH_DNS_FAILED",
                    format!("remote host {host:?} could not be resolved"),
                )
            })?
            .collect()
    };
    if addresses.is_empty() {
        return Err(WebFetchError::new(
            "WEB_FETCH_DNS_FAILED",
            format!("remote host {host:?} resolved to no addresses"),
        ));
    }
    if addresses
        .iter()
        .any(|address| !is_public_web_ip(address.ip()))
    {
        return Err(WebFetchError::new(
            "WEB_FETCH_SSRF_BLOCKED",
            "remote host resolves to a non-public address",
        ));
    }
    let unique: BTreeSet<SocketAddr> = addresses.into_iter().collect();
    Ok((host.to_owned(), unique.into_iter().collect()))
}

fn pinned_client(
    host: &str,
    addresses: &[SocketAddr],
    connect_timeout: std::time::Duration,
) -> Result<Client, WebFetchError> {
    // 对齐旧 ZhikunCode OkHttp 请求头：UA "AI-Code-Assistant/1.0"、
    // Accept "text/html,application/xhtml+xml,*/*"（2026-08-23 对齐）。
    let mut default_headers = HeaderMap::new();
    default_headers.insert(
        ACCEPT,
        HeaderValue::from_static("text/html,application/xhtml+xml,*/*"),
    );
    Client::builder()
        .redirect(Policy::none())
        .connect_timeout(connect_timeout)
        .resolve_to_addrs(host, addresses)
        .user_agent("AI-Code-Assistant/1.0")
        .default_headers(default_headers)
        .build()
        .map_err(|_| {
            WebFetchError::new(
                "WEB_FETCH_NETWORK_ERROR",
                "secure HTTP client could not be initialized",
            )
        })
}

fn is_redirect(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn literal_loopback_and_metadata_addresses_are_rejected_before_connect() {
        for url in [
            "http://127.0.0.1:8080/secret",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::1]/",
        ] {
            let error = resolve_public_addresses(&Url::parse(url).expect("URL"))
                .await
                .expect_err("blocked");
            assert_eq!(error.code, "WEB_FETCH_SSRF_BLOCKED");
        }
    }

    #[test]
    fn redirect_url_policy_rejects_non_http_and_credentials() {
        for url in ["file:///etc/passwd", "http://user:secret@example.com/"] {
            assert!(parse_safe_url(url).is_err(), "{url}");
        }
    }

    /// 对齐旧 `ZhikunCode` 错误语义：DNS 失败文案必须带模型输入的主机名，
    /// 让模型能自我纠正换 URL 重试；不拼接解析器内部错误串。
    /// `.invalid` 为 RFC 2606 保留 TLD，任何环境下都不会解析成功。
    #[tokio::test]
    async fn dns_failure_message_names_the_requested_host() {
        let error = resolve_public_addresses(
            &Url::parse("https://no-such-host-zkcode.invalid/").expect("URL"),
        )
        .await
        .expect_err("dns failure");
        assert_eq!(error.code, "WEB_FETCH_DNS_FAILED");
        assert!(
            error.message.contains("no-such-host-zkcode.invalid"),
            "{}",
            error.message
        );
    }
}
