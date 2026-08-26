//! Security policy for user-editable MCP capability definitions.
//!
//! Capability records are a lower-trust input than the user's local
//! `.zk/mcp.json`: the local HTTP UI can create and update them at runtime and
//! the manager may attach a provider credential before connecting.  This module
//! therefore keeps credential identity and outbound destination validation in
//! one place.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

use url::{Host, Url};

use crate::capability_registry::{McpCapabilityDefinition, java_is_blank};
use crate::config::McpTransportType;

/// The only credential selector accepted from the tracked capability registry.
pub const DASHSCOPE_API_KEY_CONFIG: &str = "llm.providers.dashscope.api-key";

/// The only host to which a `DashScope` credential may be attached.
pub const DASHSCOPE_MCP_HOST: &str = "dashscope.aliyuncs.com";

/// Provider identities understood by the host-side resolver.
pub const DASHSCOPE_PROVIDER: &str = "dashscope";
/// `DashScope`'s subscription/token-plan provider identity.
pub const DASHSCOPE_TOKEN_PLAN_PROVIDER: &str = "dashscope-token-plan";

/// Synchronous credential lookup supplied by the host application.
///
/// The capability JSON never controls an environment-variable name.  It can
/// select only a known logical identity, which this resolver maps to the
/// application's already-loaded provider-key state.
pub trait McpCredentialResolver: Send + Sync {
    /// Return a credential for one known provider identity.
    fn resolve(&self, provider: &str) -> Option<String>;
}

impl<F> McpCredentialResolver for F
where
    F: Fn(&str) -> Option<String> + Send + Sync,
{
    fn resolve(&self, provider: &str) -> Option<String> {
        self(provider)
    }
}

/// Restricted environment fallback used outside zk-server composition.
///
/// No value from capability `apiKeyConfig` is converted into an environment
/// variable name.  Only the documented `DashScope` variables are read.
#[derive(Debug, Default)]
pub struct DashscopeEnvironmentCredentialResolver;

impl McpCredentialResolver for DashscopeEnvironmentCredentialResolver {
    fn resolve(&self, provider: &str) -> Option<String> {
        let names: &[&str] = match provider {
            DASHSCOPE_PROVIDER => &["LLM_PROVIDER_DASHSCOPE_API_KEY", "DASHSCOPE_API_KEY"],
            DASHSCOPE_TOKEN_PLAN_PROVIDER => &["LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY"],
            _ => return None,
        };
        names.iter().find_map(|name| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
    }
}

/// Default restricted resolver.
#[must_use]
pub fn default_credential_resolver() -> Arc<dyn McpCredentialResolver> {
    Arc::new(DashscopeEnvironmentCredentialResolver)
}

/// Resolve the credential selected by a capability definition.
///
/// `DashScope` subscription credentials are accepted as a fallback because the
/// shipped first-run provider is `dashscope-token-plan`, while the MCP endpoint
/// itself uses the regular `DashScope` identity.
#[must_use]
pub fn resolve_capability_credential(
    definition: &McpCapabilityDefinition,
    resolver: &dyn McpCredentialResolver,
) -> Option<String> {
    match definition
        .api_key_config
        .as_deref()
        .filter(|value| !java_is_blank(value))
    {
        Some(DASHSCOPE_API_KEY_CONFIG) => resolver
            .resolve(DASHSCOPE_PROVIDER)
            .or_else(|| resolver.resolve(DASHSCOPE_TOKEN_PLAN_PROVIDER))
            .filter(|value| !value.trim().is_empty()),
        _ => None,
    }
}

/// Stable security-policy rejection for a capability definition.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CapabilitySecurityError {
    /// No endpoint URL was supplied.
    #[error("MCP capability endpoint URL is required")]
    MissingUrl,
    /// Runtime environment expansion is forbidden for endpoint URLs.
    #[error("MCP capability endpoint URL must not contain environment placeholders")]
    UrlPlaceholder,
    /// URL parsing or host extraction failed.
    #[error("MCP capability endpoint URL is invalid")]
    InvalidUrl,
    /// A network transport did not use TLS.
    #[error("MCP capability endpoint must use HTTPS (or WSS for WebSocket)")]
    InsecureScheme,
    /// URL userinfo could disclose embedded credentials.
    #[error("MCP capability endpoint must not contain embedded credentials")]
    EmbeddedUrlCredential,
    /// Fragments are not part of HTTP requests and are forbidden for clarity.
    #[error("MCP capability endpoint must not contain a fragment")]
    FragmentNotAllowed,
    /// The destination is loopback, link-local, private, or otherwise local.
    #[error("MCP capability endpoint targets a local or private address")]
    PrivateDestination,
    /// Capability records may use only supported network transports.
    #[error("MCP capability transport is not a network transport")]
    UnsupportedTransport,
    /// The record selected a credential identity outside the allowlist.
    #[error("MCP capability apiKeyConfig is not an allowed credential identity")]
    UnknownCredentialIdentity,
    /// Inline/default secrets are forbidden in the mutable registry.
    #[error("MCP capability apiKeyDefault is not accepted; configure the provider key instead")]
    InlineCredentialNotAllowed,
    /// A `DashScope` credential was paired with another host.
    #[error("DashScope credentials may only be sent to dashscope.aliyuncs.com")]
    CredentialHostMismatch,
    /// Mutable capability records may target only the built-in provider host.
    /// This closes the DNS-validation/dial gap for attacker-controlled names.
    #[error("MCP capability endpoint host is not in the outbound allowlist")]
    HostNotAllowed,
}

/// Parse and validate the non-network portion of the outbound policy.
///
/// This rejects local/private literal addresses immediately and pins mutable
/// capability endpoints to the built-in provider hostname.
///
/// # Errors
///
/// Returns [`CapabilitySecurityError`] when the transport, URL, credential
/// identity, or outbound host violates the mutable-registry policy.
pub fn validate_capability_definition(
    definition: &McpCapabilityDefinition,
) -> Result<Url, CapabilitySecurityError> {
    let transport = definition.resolved_transport_type();
    if !matches!(
        transport,
        McpTransportType::Sse
            | McpTransportType::SseIde
            | McpTransportType::Http
            | McpTransportType::Ws
            | McpTransportType::WsIde
    ) {
        return Err(CapabilitySecurityError::UnsupportedTransport);
    }

    let raw = definition
        .url
        .as_deref()
        .filter(|value| !java_is_blank(value))
        .ok_or(CapabilitySecurityError::MissingUrl)?;
    if raw.contains("${") {
        return Err(CapabilitySecurityError::UrlPlaceholder);
    }
    let url = Url::parse(raw).map_err(|_| CapabilitySecurityError::InvalidUrl)?;
    let valid_scheme = match transport {
        McpTransportType::Ws | McpTransportType::WsIde => url.scheme() == "wss",
        _ => url.scheme() == "https",
    };
    if !valid_scheme {
        return Err(CapabilitySecurityError::InsecureScheme);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(CapabilitySecurityError::EmbeddedUrlCredential);
    }
    if url.fragment().is_some() {
        return Err(CapabilitySecurityError::FragmentNotAllowed);
    }
    let host = url.host().ok_or(CapabilitySecurityError::InvalidUrl)?;
    match host {
        Host::Domain(domain) if is_local_hostname(domain) => {
            return Err(CapabilitySecurityError::PrivateDestination);
        }
        Host::Ipv4(address) if is_forbidden_ip(IpAddr::V4(address)) => {
            return Err(CapabilitySecurityError::PrivateDestination);
        }
        Host::Ipv6(address) if is_forbidden_ip(IpAddr::V6(address)) => {
            return Err(CapabilitySecurityError::PrivateDestination);
        }
        _ => {}
    }

    if definition
        .api_key_default
        .as_deref()
        .is_some_and(|value| !java_is_blank(value))
    {
        return Err(CapabilitySecurityError::InlineCredentialNotAllowed);
    }
    if let Some(selector) = definition
        .api_key_config
        .as_deref()
        .filter(|value| !java_is_blank(value))
    {
        if selector != DASHSCOPE_API_KEY_CONFIG {
            return Err(CapabilitySecurityError::UnknownCredentialIdentity);
        }
        if url
            .host_str()
            .is_none_or(|host| !host.eq_ignore_ascii_case(DASHSCOPE_MCP_HOST))
        {
            return Err(CapabilitySecurityError::CredentialHostMismatch);
        }
    }
    // Capability definitions are mutable through the local web API.  A
    // resolve-then-dial policy alone leaves a DNS-rebinding gap because the
    // HTTP/WebSocket client performs its own lookup.  The built-in registry is
    // DashScope-only, so fail closed to that non-attacker-controlled hostname.
    // Custom MCP servers remain available through the explicit mcp.json
    // configuration/approval path, where no provider credential is injected.
    if url
        .host_str()
        .is_none_or(|host| !host.eq_ignore_ascii_case(DASHSCOPE_MCP_HOST))
    {
        return Err(CapabilitySecurityError::HostNotAllowed);
    }
    Ok(url)
}

/// Revalidate the fixed outbound policy immediately before a
/// capability-registry connection is opened.
///
/// # Errors
///
/// Returns [`CapabilitySecurityError`] when definition validation fails.
/// Mutable definitions are already pinned to [`DASHSCOPE_MCP_HOST`]; a
/// separate DNS lookup here would not pin the transport's later lookup and
/// would only add a TOCTOU gap and an unnecessary network dependency.
pub async fn validate_capability_destination(
    definition: &McpCapabilityDefinition,
) -> Result<(), CapabilitySecurityError> {
    validate_capability_definition(definition)?;
    Ok(())
}

fn is_local_hostname(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == "localhost"
        || host
            .rsplit('.')
            .next()
            .is_some_and(|label| matches!(label, "localhost" | "local" | "internal" | "home"))
}

fn is_forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_forbidden_v4(ip),
        IpAddr::V6(ip) => is_forbidden_v6(ip),
    }
}

fn is_forbidden_v4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_broadcast()
        || octets[0] == 0
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        || (octets[0] == 198 && matches!(octets[1], 18 | 19))
}

fn is_forbidden_v6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || (segments[0] & 0xfe00) == 0xfc00 // unique-local fc00::/7
        || (segments[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
        || ip.to_ipv4_mapped().is_some_and(is_forbidden_v4)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capability(url: &str) -> McpCapabilityDefinition {
        let mut definition = McpCapabilityDefinition::new("mcp_test");
        definition.url = Some(url.to_owned());
        definition.transport_type = Some(McpTransportType::Http);
        definition
    }

    #[test]
    fn arbitrary_environment_selector_is_rejected_without_reading_it() {
        let mut definition = capability("https://example.com/mcp");
        definition.api_key_config = Some("aws.secret-access-key".to_owned());
        assert_eq!(
            validate_capability_definition(&definition),
            Err(CapabilitySecurityError::UnknownCredentialIdentity)
        );
        assert_eq!(
            resolve_capability_credential(&definition, &|_: &str| {
                Some("must-not-leak".to_owned())
            }),
            None
        );
    }

    #[test]
    fn dashscope_credential_is_pinned_to_dashscope_host() {
        let mut definition = capability("https://evil.example/mcp");
        definition.api_key_config = Some(DASHSCOPE_API_KEY_CONFIG.to_owned());
        assert_eq!(
            validate_capability_definition(&definition),
            Err(CapabilitySecurityError::CredentialHostMismatch)
        );

        definition.url = Some("https://dashscope.aliyuncs.com/api/v1/mcps/test/mcp".to_owned());
        assert!(validate_capability_definition(&definition).is_ok());
    }

    #[test]
    fn private_loopback_and_credential_urls_are_rejected() {
        for url in [
            "http://example.com/mcp",
            "https://127.0.0.1/mcp",
            "https://10.0.0.8/mcp",
            "https://169.254.169.254/latest/meta-data",
            "https://[::1]/mcp",
            "https://localhost/mcp",
            "https://user:pass@example.com/mcp",
        ] {
            assert!(
                validate_capability_definition(&capability(url)).is_err(),
                "{url} must be rejected"
            );
        }
    }

    #[test]
    fn inline_credentials_and_url_placeholders_are_rejected() {
        let mut inline = capability("https://example.com/mcp");
        inline.api_key_default = Some("literal-secret".to_owned());
        assert_eq!(
            validate_capability_definition(&inline),
            Err(CapabilitySecurityError::InlineCredentialNotAllowed)
        );

        let placeholder = capability("https://${AWS_SECRET_ACCESS_KEY}.example/mcp");
        assert_eq!(
            validate_capability_definition(&placeholder),
            Err(CapabilitySecurityError::UrlPlaceholder)
        );
    }

    #[test]
    fn known_resolver_prefers_regular_then_subscription_identity() {
        let mut definition = capability("https://dashscope.aliyuncs.com/api/v1/mcps/test/mcp");
        definition.api_key_config = Some(DASHSCOPE_API_KEY_CONFIG.to_owned());
        let resolver = |provider: &str| match provider {
            DASHSCOPE_TOKEN_PLAN_PROVIDER => Some("subscription-key".to_owned()),
            _ => None,
        };
        assert_eq!(
            resolve_capability_credential(&definition, &resolver).as_deref(),
            Some("subscription-key")
        );
    }

    #[test]
    fn mutable_capabilities_fail_closed_to_dashscope_host_allowlist() {
        assert_eq!(
            validate_capability_definition(&capability("https://public.example/mcp")),
            Err(CapabilitySecurityError::HostNotAllowed)
        );
        assert!(
            validate_capability_definition(&capability(
                "https://dashscope.aliyuncs.com/api/v1/mcps/test/mcp"
            ))
            .is_ok()
        );
    }
}
