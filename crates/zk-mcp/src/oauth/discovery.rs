use serde::Deserialize;
use url::Url;

use super::{OAuthError, http::OAuthHttp};

#[derive(Clone, Deserialize)]
pub(super) struct AuthorizationServer {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub registration_endpoint: Option<String>,
    pub revocation_endpoint: Option<String>,
    #[serde(default)]
    pub code_challenge_methods_supported: Vec<String>,
    #[serde(default)]
    pub token_endpoint_auth_methods_supported: Vec<String>,
}

#[derive(Deserialize)]
struct ProtectedResource {
    resource: String,
    authorization_servers: Vec<String>,
    #[serde(default)]
    scopes_supported: Vec<String>,
}

pub(super) async fn discover(
    http: &OAuthHttp,
    resource: &str,
) -> Result<(AuthorizationServer, Option<String>), OAuthError> {
    let resource_url = http.validate_url(resource)?;
    let challenge = http.challenge(resource).await?;
    let advertised = challenge
        .as_deref()
        .and_then(|value| bearer_parameter(value, "resource_metadata"));
    let metadata_urls = if let Some(advertised) = advertised {
        vec![advertised]
    } else {
        well_known_candidates(&resource_url, "oauth-protected-resource", false)
    };
    let mut protected = None;
    for metadata_url in metadata_urls {
        if let Ok(value) = http.get_json(&metadata_url).await {
            let record: ProtectedResource =
                serde_json::from_value(value).map_err(|_| OAuthError::InvalidMetadata)?;
            if Url::parse(&record.resource).ok().as_ref() != Some(&resource_url) {
                return Err(OAuthError::BindingMismatch);
            }
            protected = Some(record);
            break;
        }
    }
    let protected = protected.ok_or(OAuthError::DiscoveryUnavailable)?;
    // Choosing a different issuer after a failed discovery could silently
    // change the authorization party. The user sees this issuer before consent.
    let issuer = protected
        .authorization_servers
        .first()
        .ok_or(OAuthError::InvalidMetadata)?;
    let issuer_url = http.validate_url(issuer)?;
    if issuer_url.query().is_some() {
        return Err(OAuthError::InvalidMetadata);
    }
    let mut candidates = well_known_candidates(&issuer_url, "oauth-authorization-server", false);
    // OAuth issuer discovery does not fall back to the root for a path issuer.
    candidates.truncate(1);
    let mut oidc = well_known_candidates(&issuer_url, "openid-configuration", true);
    candidates.append(&mut oidc);
    let mut authorization = None;
    for candidate in candidates {
        if let Ok(value) = http.get_json(&candidate).await {
            let server: AuthorizationServer =
                serde_json::from_value(value).map_err(|_| OAuthError::InvalidMetadata)?;
            if Url::parse(&server.issuer).ok().as_ref() != Some(&issuer_url) {
                return Err(OAuthError::BindingMismatch);
            }
            if !server
                .code_challenge_methods_supported
                .iter()
                .any(|method| method == "S256")
            {
                return Err(OAuthError::PkceUnsupported);
            }
            for endpoint in [&server.authorization_endpoint, &server.token_endpoint]
                .into_iter()
                .chain(server.registration_endpoint.iter())
                .chain(server.revocation_endpoint.iter())
            {
                http.validate_url(endpoint)?;
            }
            authorization = Some(server);
            break;
        }
    }
    let scope = challenge
        .as_deref()
        .and_then(|value| bearer_parameter(value, "scope"))
        .or_else(|| {
            (!protected.scopes_supported.is_empty()).then(|| protected.scopes_supported.join(" "))
        });
    Ok((
        authorization.ok_or(OAuthError::DiscoveryUnavailable)?,
        scope,
    ))
}

fn well_known_candidates(issuer: &Url, document: &str, oidc_append: bool) -> Vec<String> {
    let path = issuer.path().trim_end_matches('/');
    let mut first = issuer.clone();
    first.set_query(None);
    first.set_path(&format!("/.well-known/{document}{path}"));
    let mut candidates = vec![first.to_string()];
    if !path.is_empty() {
        let mut second = issuer.clone();
        second.set_query(None);
        second.set_path(&if oidc_append {
            format!("{path}/.well-known/{document}")
        } else {
            format!("/.well-known/{document}")
        });
        candidates.push(second.to_string());
    }
    candidates
}

/// Quoted challenge parameters may contain commas or escaped quotes.
fn bearer_parameter(header: &str, key: &str) -> Option<String> {
    let lower = header.to_ascii_lowercase();
    let start = lower.find("bearer ")? + 7;
    let mut parts = Vec::new();
    let mut part = String::new();
    let mut quoted = false;
    let mut escaped = false;
    for character in header[start..].chars() {
        if escaped {
            part.push(character);
            escaped = false;
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
            continue;
        }
        if character == '"' {
            quoted = !quoted;
            part.push(character);
            continue;
        }
        if character == ',' && !quoted {
            parts.push(std::mem::take(&mut part));
        } else {
            part.push(character);
        }
    }
    if quoted || escaped {
        return None;
    }
    parts.push(part);
    for part in parts {
        let Some((name, value)) = part.trim().split_once('=') else {
            break;
        };
        if name.trim().eq_ignore_ascii_case(key) {
            return Some(value.trim().trim_matches('"').to_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn discovery_paths_preserve_tenant_and_resource_specificity() {
        assert_eq!(
            well_known_candidates(
                &Url::parse("https://example.com/tenant").unwrap(),
                "openid-configuration",
                true
            ),
            vec![
                "https://example.com/.well-known/openid-configuration/tenant",
                "https://example.com/tenant/.well-known/openid-configuration"
            ]
        );
        assert_eq!(
            well_known_candidates(
                &Url::parse("https://example.com/mcp").unwrap(),
                "oauth-protected-resource",
                false
            ),
            vec![
                "https://example.com/.well-known/oauth-protected-resource/mcp",
                "https://example.com/.well-known/oauth-protected-resource"
            ]
        );
    }
    #[test]
    fn challenge_parser_preserves_quoted_commas_and_ignores_bad_quotes() {
        assert_eq!(
            bearer_parameter(
                "Bearer resource_metadata=\"https://example.com/meta?a=b,c\", scope=\"read write\"",
                "scope"
            ),
            Some("read write".into())
        );
        assert_eq!(
            bearer_parameter(
                "Bearer resource_metadata=\"unterminated",
                "resource_metadata"
            ),
            None
        );
    }
}
