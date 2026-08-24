//! 局域网可达地址探测与私有网段判定（旧
//! `config/RemoteAccessSecurityFilter.java` L235-243 / L273-289 与
//! `config/SecurityConfig.java` L119-136）。
//!
//! 旧源把「取本机局域网 IP」实现了两份（过滤器取**首个**用于启动日志，安全配置取
//! **全部**用于 CORS 白名单），本模块把两者收在一处：[`local_ip`] 对应前者，
//! [`site_local_ipv4s`] 对应后者，两者共用同一份网卡筛选谓词，避免两处口径漂移。
//!
//! # 逐行对照
//!
//! | 本模块 | 旧源 |
//! |---|---|
//! | [`local_ip`] | `RemoteAccessSecurityFilter.getLocalIp()` L273-289 |
//! | [`site_local_ipv4s`] | `SecurityConfig.getLocalIps()` L119-136 |
//! | [`is_private_network`] | `RemoteAccessSecurityFilter.isPrivateNetwork` L235-243 |
//! | [`LOCAL_IP_FALLBACK`] | L288 `return "localhost"` |
//! | `interface_is_usable` | L278 `if (ni.isLoopback() \|\| !ni.isUp()) continue;` |
//!
//! # 偏离留痕
//!
//! - **B2B-08 网卡枚举底座**：旧源用 `NetworkInterface.getNetworkInterfaces()`
//!   （JDK 内部即 `getifaddrs(3)`）。本实现直接调 `nix::ifaddrs::getifaddrs`，
//!   与 [`crate::python::sidecar`] 复用同一个 nix 依赖，不为此引入新 crate。
//!   `unsafe_code = "forbid"` 下 nix 的安全封装是唯一可行路径。
//! - **B2B-09 `isUp()` 语义**：`OpenJDK` 的 `NetworkInterface.isUp()` 要求
//!   `IFF_UP` **与** `IFF_RUNNING` 同时置位，本实现按此双条件判定（只判 `IFF_UP`
//!   会把已配置但链路断开的网卡算作可用，导致启动日志给出不可达地址）。
//! - **B2B-10 枚举顺序**：旧源"首个站点本地 IPv4"依赖 OS 返回顺序，
//!   `getifaddrs` 顺序与 JDK 一致（同一系统调用），故 [`local_ip`] 保持
//!   **不排序**以逐字复刻旧行为；[`site_local_ipv4s`] 按枚举序去重（旧源不去重，
//!   去重只影响 CORS 白名单里的重复项，无行为差异）。

use std::net::{IpAddr, Ipv4Addr};

use nix::net::if_::InterfaceFlags;

/// 无可用站点本地地址时的兜底值（旧 L288 `return "localhost"`）。
pub const LOCAL_IP_FALLBACK: &str = "localhost";

/// 本机首个站点本地 IPv4 的点分十进制表示，无则 `"localhost"`
/// （旧 `getLocalIp()` L273-289）。
///
/// 用于启动期的两条局域网访问提示日志。
#[must_use]
pub fn local_ip() -> String {
    site_local_ipv4s()
        .first()
        .map_or_else(|| LOCAL_IP_FALLBACK.to_owned(), Ipv4Addr::to_string)
}

/// 本机全部站点本地 IPv4（旧 `SecurityConfig.getLocalIps()` L119-136）。
///
/// 筛选口径：跳过 loopback 与未 up 的网卡（旧 L125/L278），只取
/// `isSiteLocalAddress()` 的 IPv4（旧 L129 用
/// `!getHostAddress().contains(":")` 排除 IPv6，本实现直接按地址族判定）。
#[must_use]
pub fn site_local_ipv4s() -> Vec<Ipv4Addr> {
    // 旧源整段包在 `catch (Exception ignored)` 里，枚举失败即回落空列表。
    let Ok(addresses) = nix::ifaddrs::getifaddrs() else {
        tracing::warn!("failed to enumerate network interfaces");
        return Vec::new();
    };
    let mut found: Vec<Ipv4Addr> = Vec::new();
    for entry in addresses {
        if !interface_is_usable(entry.flags) {
            continue;
        }
        let Some(ipv4) = entry
            .address
            .as_ref()
            .and_then(nix::sys::socket::SockaddrStorage::as_sockaddr_in)
            .map(nix::sys::socket::SockaddrIn::ip)
        else {
            continue;
        };
        if ipv4.is_private() && !found.contains(&ipv4) {
            found.push(ipv4);
        }
    }
    found
}

/// 对端是否位于私有网段（旧 `isPrivateNetwork` L235-243：站点本地 ∨ loopback ∨
/// 链路本地）。
///
/// IPv4-mapped IPv6（`::ffff:a.b.c.d`）先归一到 IPv4 再判定——旧源经
/// `InetAddress.getByName` 解析时 JDK 会把它折叠成 `Inet4Address`，行为一致。
#[must_use]
pub fn is_private_network(ip: IpAddr) -> bool {
    let ip = ip.to_canonical();
    is_site_local(ip) || ip.is_loopback() || is_link_local(ip)
}

/// `InetAddress.isSiteLocalAddress()` 的等价谓词。
///
/// IPv4：10/8、172.16/12、192.168/16（即 [`Ipv4Addr::is_private`]）。
/// IPv6：`fec0::/10`——已废弃的 site-local 前缀，**不含** `fc00::/7` ULA，
/// 与 JDK `Inet6Address.isSiteLocalAddress()` 的位判定逐位一致。
fn is_site_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_private(),
        IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) == 0xfec0,
    }
}

/// `InetAddress.isLinkLocalAddress()` 的等价谓词（IPv4 `169.254/16`，
/// IPv6 `fe80::/10`）。
fn is_link_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) == 0xfe80,
    }
}

/// 网卡是否参与地址探测（旧 L278 `ni.isLoopback() || !ni.isUp()` 的取反；
/// `isUp()` 的双标志语义见偏离 B2B-09）。
fn interface_is_usable(flags: InterfaceFlags) -> bool {
    !flags.contains(InterfaceFlags::IFF_LOOPBACK)
        && flags.contains(InterfaceFlags::IFF_UP)
        && flags.contains(InterfaceFlags::IFF_RUNNING)
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use super::*;

    /// 站点本地判定覆盖 IPv4 三段私有前缀与其边界外地址。
    #[test]
    fn site_local_ipv4_matches_three_private_blocks() {
        for private in ["10.0.0.1", "172.16.0.1", "172.31.255.254", "192.168.1.5"] {
            let ip: IpAddr = private.parse().expect("parses");
            assert!(is_site_local(ip), "{private} 应为站点本地");
        }
        for public in ["8.8.8.8", "172.15.0.1", "172.32.0.1", "191.168.1.5"] {
            let ip: IpAddr = public.parse().expect("parses");
            assert!(!is_site_local(ip), "{public} 不应为站点本地");
        }
    }

    /// IPv6 站点本地只认 `fec0::/10`；ULA `fc00::/7` 不算（同 JDK）。
    #[test]
    fn site_local_ipv6_matches_fec0_only() {
        assert!(is_site_local(IpAddr::V6(
            "fec0::1".parse::<Ipv6Addr>().expect("parses")
        )));
        assert!(is_site_local(IpAddr::V6(
            "feff::1".parse::<Ipv6Addr>().expect("parses")
        )));
        assert!(!is_site_local(IpAddr::V6(
            "fd00::1".parse::<Ipv6Addr>().expect("parses")
        )));
        assert!(!is_site_local(IpAddr::V6(
            "fe80::1".parse::<Ipv6Addr>().expect("parses")
        )));
    }

    /// 私有网段 = 站点本地 ∨ loopback ∨ 链路本地（旧三条并联）。
    #[test]
    fn private_network_covers_loopback_and_link_local() {
        for private in [
            "127.0.0.1",
            "::1",
            "192.168.1.5",
            "169.254.1.1",
            "fe80::1",
            "fec0::1",
        ] {
            let ip: IpAddr = private.parse().expect("parses");
            assert!(is_private_network(ip), "{private} 应判为私有");
        }
        for public in ["8.8.8.8", "1.1.1.1", "2001:4860:4860::8888", "fd00::1"] {
            let ip: IpAddr = public.parse().expect("parses");
            assert!(!is_private_network(ip), "{public} 应判为公网");
        }
    }

    /// IPv4-mapped IPv6 先归一再判定（axum 在双栈监听下会给出这种对端地址）。
    #[test]
    fn ipv4_mapped_peer_is_canonicalised() {
        let mapped: IpAddr = "::ffff:192.168.1.5".parse().expect("parses");
        assert!(is_private_network(mapped));
        let mapped_public: IpAddr = "::ffff:8.8.8.8".parse().expect("parses");
        assert!(!is_private_network(mapped_public));
        let mapped_loopback: IpAddr = "::ffff:127.0.0.1".parse().expect("parses");
        assert!(is_private_network(mapped_loopback));
    }

    /// 网卡筛选：loopback 排除、未 up / 未 running 排除。
    #[test]
    fn interface_filter_requires_up_and_running_non_loopback() {
        let up_running = InterfaceFlags::IFF_UP | InterfaceFlags::IFF_RUNNING;
        assert!(interface_is_usable(up_running));
        assert!(!interface_is_usable(
            up_running | InterfaceFlags::IFF_LOOPBACK
        ));
        assert!(!interface_is_usable(InterfaceFlags::IFF_UP));
        assert!(!interface_is_usable(InterfaceFlags::IFF_RUNNING));
        assert!(!interface_is_usable(InterfaceFlags::empty()));
    }

    /// 探测结果自洽：全部条目均为站点本地且互不重复；`local_ip` 与首项一致，
    /// 空列表时退化为 `"localhost"`（旧 L288）。
    #[test]
    fn probe_results_are_self_consistent() {
        let ips = site_local_ipv4s();
        for ip in &ips {
            assert!(ip.is_private(), "{ip} 必须是站点本地 IPv4");
        }
        let mut deduped = ips.clone();
        deduped.dedup();
        assert_eq!(deduped.len(), ips.len(), "枚举结果不得含重复项");
        let expected = ips
            .first()
            .map_or_else(|| LOCAL_IP_FALLBACK.to_owned(), Ipv4Addr::to_string);
        assert_eq!(local_ip(), expected);
    }
}
