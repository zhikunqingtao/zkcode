//! Bounded OAuth HTTP with DNS pinning and no redirects or ambient proxies.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use reqwest::{Client, Response};
use serde_json::Value;
use url::Url;

use super::OAuthError;

pub(super) struct OAuthHttp {
    #[cfg(test)]
    pub allow_loopback: bool,
}

impl OAuthHttp {
    pub fn production() -> Self {
        Self {
            #[cfg(test)]
            allow_loopback: false,
        }
    }

    #[allow(clippy::unused_self)] // Test-only loopback fixture policy is instance-scoped; production always requires HTTPS.
    pub fn validate_url(&self, raw: &str) -> Result<Url, OAuthError> {
        let url = Url::parse(raw).map_err(|_| OAuthError::UnsafeEndpoint)?;
        let loopback_test = {
            #[cfg(test)]
            {
                self.allow_loopback && url.host_str() == Some("127.0.0.1") && url.scheme() == "http"
            }
            #[cfg(not(test))]
            {
                false
            }
        };
        if (!loopback_test && url.scheme() != "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(OAuthError::UnsafeEndpoint);
        }
        Ok(url)
    }

    async fn client(&self, url: &Url) -> Result<Client, OAuthError> {
        self.pinned_client(url, false).await
    }

    pub(super) async fn resource_client(&self, url: &Url) -> Result<Client, OAuthError> {
        self.validate_url(url.as_str())?;
        self.pinned_client(url, true).await
    }

    async fn pinned_client(&self, url: &Url, stream: bool) -> Result<Client, OAuthError> {
        let host = url.host_str().ok_or(OAuthError::UnsafeEndpoint)?;
        let port = url
            .port_or_known_default()
            .ok_or(OAuthError::UnsafeEndpoint)?;
        let addresses: Vec<SocketAddr> = match url.host() {
            Some(url::Host::Ipv4(ip)) => vec![SocketAddr::new(IpAddr::V4(ip), port)],
            Some(url::Host::Ipv6(ip)) => vec![SocketAddr::new(IpAddr::V6(ip), port)],
            Some(url::Host::Domain(domain)) => tokio::time::timeout(
                Duration::from_secs(5),
                tokio::net::lookup_host((domain, port)),
            )
            .await
            .map_err(|_| OAuthError::Network)?
            .map_err(|_| OAuthError::Network)?
            .collect(),
            None => return Err(OAuthError::UnsafeEndpoint),
        };
        if addresses.is_empty()
            || addresses.iter().any(|address| {
                #[cfg(test)]
                if self.allow_loopback && address.ip().is_loopback() {
                    return false;
                }
                crate::security::is_forbidden_ip(address.ip())
            })
        {
            return Err(OAuthError::UnsafeEndpoint);
        }
        let builder = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .resolve_to_addrs(host, &addresses);
        let builder = if stream {
            builder.read_timeout(Duration::from_secs(90))
        } else {
            builder.timeout(Duration::from_secs(30))
        };
        builder.build().map_err(|_| OAuthError::Network)
    }

    pub async fn challenge(&self, resource: &str) -> Result<Option<String>, OAuthError> {
        let url = self.validate_url(resource)?;
        let response = self
            .client(&url)
            .await?
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|_| OAuthError::Network)?;
        // Do not consume a possible SSE stream while looking for a challenge.
        Ok(response
            .headers()
            .get_all(reqwest::header::WWW_AUTHENTICATE)
            .iter()
            .filter_map(|header| header.to_str().ok())
            .find(|header| header.to_ascii_lowercase().contains("bearer "))
            .map(str::to_owned))
    }

    pub async fn get_json(&self, raw: &str) -> Result<Value, OAuthError> {
        let url = self.validate_url(raw)?;
        let response = self
            .client(&url)
            .await?
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|_| OAuthError::Network)?;
        read_json(response).await
    }

    pub async fn post_json(&self, raw: &str, value: &Value) -> Result<Value, OAuthError> {
        let url = self.validate_url(raw)?;
        let response = self
            .client(&url)
            .await?
            .post(url)
            .json(value)
            .send()
            .await
            .map_err(|_| OAuthError::Network)?;
        read_json(response).await
    }

    pub async fn post_form(
        &self,
        raw: &str,
        fields: &[(&str, String)],
        client_secret: Option<(&str, &str)>,
    ) -> Result<Value, OAuthError> {
        let url = self.validate_url(raw)?;
        let mut request = self.client(&url).await?.post(url).form(fields);
        if let Some((client_id, secret)) = client_secret {
            request = request.basic_auth(client_id, Some(secret));
        }
        read_json(request.send().await.map_err(|_| OAuthError::Network)?).await
    }

    pub async fn revoke(
        &self,
        raw: &str,
        fields: &[(&str, String)],
        client_secret: Option<(&str, &str)>,
    ) -> Result<(), OAuthError> {
        let url = self.validate_url(raw)?;
        let mut request = self.client(&url).await?.post(url).form(fields);
        if let Some((client_id, secret)) = client_secret {
            request = request.basic_auth(client_id, Some(secret));
        }
        let response = request.send().await.map_err(|_| OAuthError::Network)?;
        // RFC 7009 revocation success commonly has an empty body.
        if response.status().is_success() {
            Ok(())
        } else {
            Err(OAuthError::RemoteRejected)
        }
    }
}

async fn read_json(mut response: Response) -> Result<Value, OAuthError> {
    const LIMIT: usize = 256 * 1024;
    if !response.status().is_success() {
        return Err(OAuthError::RemoteRejected);
    }
    if response
        .content_length()
        .is_some_and(|length| length > LIMIT as u64)
    {
        return Err(OAuthError::InvalidMetadata);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| OAuthError::Network)? {
        if chunk.len() > LIMIT.saturating_sub(bytes.len()) {
            return Err(OAuthError::InvalidMetadata);
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| OAuthError::InvalidMetadata)
}
