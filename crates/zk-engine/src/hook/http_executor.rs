//! HTTP 通道 hook 执行器 + SSRF 防护（Batch 8B Step 4）。
//!
//! 对照旧 `hook/HttpHookExecutor.java`（177L）+ `hook/SsrfGuard.java`。
//!
//! 语义偏离留痕：
//!
//! - **H-02 回环从严拒绝**：旧 `SsrfGuard.isBlockedV4` 对 `127.0.0.0/8` 与 `::1`
//!   **刻意放行**（注释「本地开发服务器是主要用例」）。本端按 Batch 8B 规格把
//!   回环纳入拒绝集——任务明列 `127.0.0.0/8` 为封锁网段，SSRF 防护是安全硬需求，
//!   宁可牺牲「hook 打到本机端口」这一便利，也不给本机服务面留 SSRF 缺口。其余
//!   封锁网段（`0.0.0.0/8` / `10.0.0.0/8` / `169.254.0.0/16` / `172.16.0.0/12` /
//!   `100.64.0.0/10` CGNAT / `192.168.0.0/16` / IPv6 ULA `fc00::/7` / link-local
//!   `fe80::/10` / IPv4-mapped）与旧逐位对齐。
//! - **H-02b 载荷简化**：旧执行器含环境变量白名单插值、CRLF 清理、HMAC-SHA256
//!   签名头。Batch 8B Step 4 规格仅要求「POST JSON payload → target URL」，故
//!   不移植 HMAC / env 插值——payload 由调用方（[`crate::hook::HookService`]）
//!   构造，本执行器只负责安全出站。
//!
//! **TOCTOU 消除**：DNS 解析在本模块内完成并逐 IP 校验，随后经 reqwest
//! `resolve_to_addrs` 把**已校验的 IP** 钉进客户端，reqwest 出站时不再重解析 —
//! 与旧 `SsrfGuard` 的自定义 `DnsResolver`（解析即校验即绑定）等价，堵住 DNS
//! rebinding 窗口。叠加禁重定向（旧 `disableRedirectHandling`）+ 10s 超时。

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use reqwest::Url;
use serde_json::Value;
use thiserror::Error;

/// HTTP 通道 hook 的请求超时（连接 + 响应合计上限）。
///
/// 对齐 Batch 8B Step 4「timeout(10s)」；旧执行器为 30s，本端收紧——hook 是
/// fire-and-forget 通知，不值得为慢端点长时间占用热路径。
const HTTP_HOOK_TIMEOUT: Duration = Duration::from_secs(10);

/// HTTP hook 出站失败原因（均由 [`crate::hook::HookService`] 降级为 `warn!`，
/// 绝不冒泡进主流程——hook 失败必须隔离不阻塞）。
#[derive(Debug, Error)]
pub enum HttpHookError {
    /// URL 无法解析。
    #[error("invalid hook url: {0}")]
    InvalidUrl(String),
    /// scheme 非 `http` / `https`（拒绝 `file://` / `gopher://` 等 SSRF 载体）。
    #[error("unsupported url scheme {0:?} (only http/https allowed)")]
    UnsupportedScheme(String),
    /// DNS 解析失败或无地址返回。
    #[error("dns resolution failed for {host}")]
    Dns {
        /// 解析目标主机名。
        host: String,
    },
    /// 解析所得 IP 命中封锁网段（SSRF 防护拦截）。
    #[error("ssrf blocked: {host} resolves to {ip}")]
    SsrfBlocked {
        /// 目标主机名。
        host: String,
        /// 命中封锁网段的具体 IP。
        ip: IpAddr,
    },
    /// 请求构建 / 发送失败（超时、连接拒绝、TLS 等）。
    #[error("http hook request failed: {0}")]
    Request(String),
    /// 端点返回非 2xx 状态码。
    #[error("hook endpoint returned non-success status {0}")]
    Status(u16),
}

/// HTTP 通道 hook 执行器（无状态；每次出站按目标主机新建钉 IP 的客户端）。
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpHookExecutor;

impl HttpHookExecutor {
    /// 向 `url` POST 一个 JSON payload，带完整 SSRF 防护。
    ///
    /// 流程：解析 URL → 校验 scheme → 异步 DNS 解析 → 逐 IP 封锁网段校验 → 钉
    /// 已校验 IP 建客户端（禁重定向 + 10s 超时）→ POST → 校验状态码。
    ///
    /// 不重试（fire-and-forget 语义）。
    ///
    /// # Errors
    ///
    /// URL 非法 / scheme 不受支持 / DNS 失败 / 命中封锁网段 / 请求失败 / 非 2xx
    /// 时返回对应的 [`HttpHookError`]。
    pub async fn send(&self, url: &str, payload: &Value) -> Result<(), HttpHookError> {
        let parsed =
            Url::parse(url).map_err(|error| HttpHookError::InvalidUrl(error.to_string()))?;
        let scheme = parsed.scheme();
        if scheme != "http" && scheme != "https" {
            return Err(HttpHookError::UnsupportedScheme(scheme.to_owned()));
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| HttpHookError::InvalidUrl("missing host".to_owned()))?
            .to_owned();
        let port = parsed
            .port_or_known_default()
            .ok_or_else(|| HttpHookError::InvalidUrl("missing port".to_owned()))?;

        // 异步 DNS 解析（不阻塞 runtime）；逐 IP 校验后钉入客户端。
        let resolved: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|_| HttpHookError::Dns { host: host.clone() })?
            .collect();
        if resolved.is_empty() {
            return Err(HttpHookError::Dns { host });
        }
        for addr in &resolved {
            if is_blocked_address(addr.ip()) {
                return Err(HttpHookError::SsrfBlocked {
                    host,
                    ip: addr.ip(),
                });
            }
        }

        // 钉住已校验 IP：reqwest 出站不再重解析，消除 DNS rebinding TOCTOU。
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(HTTP_HOOK_TIMEOUT)
            .resolve_to_addrs(&host, &resolved)
            .build()
            .map_err(|error| HttpHookError::Request(error.to_string()))?;
        let response = client
            .post(parsed)
            .json(payload)
            .send()
            .await
            .map_err(|error| HttpHookError::Request(error.to_string()))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(HttpHookError::Status(response.status().as_u16()))
        }
    }
}

/// 判定 IP 是否命中封锁网段（SSRF 防护核心；见模块文档 H-02）。
#[must_use]
pub fn is_blocked_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_blocked_v4(v4),
        IpAddr::V6(v6) => is_blocked_v6(v6),
    }
}

/// IPv4 封锁判定（逐位对照旧 `SsrfGuard.isBlockedV4`，`127/8` 改为拒绝，H-02）。
fn is_blocked_v4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    a == 127                                    // 127.0.0.0/8 回环（H-02：从严拒绝）
        || a == 0                               // 0.0.0.0/8
        || a == 10                              // 10.0.0.0/8
        || (a == 169 && b == 254)               // 169.254.0.0/16 link-local
        || (a == 172 && (16..=31).contains(&b)) // 172.16.0.0/12
        || (a == 100 && (64..=127).contains(&b)) // 100.64.0.0/10 CGNAT
        || (a == 192 && b == 168) // 192.168.0.0/16
}

/// IPv6 封锁判定（逐位对照旧 `SsrfGuard.isBlockedV6`，`::1` 改为拒绝，H-02）。
fn is_blocked_v6(ip: Ipv6Addr) -> bool {
    if ip == Ipv6Addr::LOCALHOST {
        return true; // ::1 回环（H-02：从严拒绝）
    }
    if ip.is_unspecified() {
        return true; // :: 未指定
    }
    // IPv4-mapped（::ffff:x.x.x.x）——提取内嵌 IPv4 复用 v4 判定。
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_blocked_v4(v4);
    }
    let seg0 = ip.segments()[0];
    if (seg0 & 0xfe00) == 0xfc00 {
        return true; // fc00::/7 ULA
    }
    if (seg0 & 0xffc0) == 0xfe80 {
        return true; // fe80::/10 link-local
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn blocked(literal: &str) -> bool {
        is_blocked_address(IpAddr::from_str(literal).expect("valid ip"))
    }

    #[test]
    fn blocks_private_and_special_v4_ranges() {
        for literal in [
            "127.0.0.1", // 回环（H-02）
            "127.255.255.255",
            "0.0.0.0",         // 0/8
            "10.1.2.3",        // 10/8
            "172.16.0.1",      // 172.16/12 下界
            "172.31.255.255",  // 172.16/12 上界
            "100.64.0.1",      // CGNAT 下界
            "100.127.255.255", // CGNAT 上界
            "169.254.1.1",     // link-local
            "192.168.0.1",     // 192.168/16
        ] {
            assert!(blocked(literal), "expected blocked: {literal}");
        }
    }

    #[test]
    fn allows_public_v4() {
        for literal in [
            "8.8.8.8",
            "1.1.1.1",
            "172.15.0.1",  // 172.16/12 之外
            "172.32.0.1",  // 172.16/12 之外
            "100.63.0.1",  // CGNAT 之外
            "100.128.0.1", // CGNAT 之外
            "11.0.0.1",
        ] {
            assert!(!blocked(literal), "expected allowed: {literal}");
        }
    }

    #[test]
    fn blocks_special_v6() {
        for literal in [
            "::1",              // 回环（H-02）
            "::",               // 未指定
            "::ffff:127.0.0.1", // mapped 回环
            "::ffff:10.0.0.1",  // mapped 私网
            "fc00::1",          // ULA
            "fd12:3456::1",     // ULA
            "fe80::1",          // link-local
        ] {
            assert!(blocked(literal), "expected blocked: {literal}");
        }
    }

    #[test]
    fn allows_public_v6_and_mapped_public() {
        assert!(!blocked("2001:4860:4860::8888")); // 公网 v6
        assert!(!blocked("::ffff:8.8.8.8")); // mapped 公网 v4
    }
}
