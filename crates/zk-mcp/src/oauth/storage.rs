//! Credential storage ports and native macOS Keychain implementation.

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use super::OAuthError;

/// Public, resource-bound registration metadata. No credential belongs here.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OAuthBinding {
    /// Exact MCP endpoint; credentials cannot migrate to a different resource.
    pub resource: String,
    /// Verified authorization server issuer.
    pub issuer: String,
    /// Discovered token endpoint.
    pub token_endpoint: String,
    /// Optional revocation endpoint.
    pub revocation_endpoint: Option<String>,
    /// Actual preregistered or dynamically registered client identity.
    pub client_id: String,
    /// Keychain account key; never a token or client secret.
    pub credential_ref: String,
    /// Granted scope, when supplied by the authorization server.
    pub scope: Option<String>,
    /// Unix expiration time, when supplied by the authorization server.
    pub expires_at: Option<u64>,
}

/// Host-provided transactional metadata storage (`SQLite` in the local app).
pub trait OAuthBindingStore: Send + Sync {
    /// Read the binding for a named MCP service.
    fn load<'a>(
        &'a self,
        server: &'a str,
    ) -> BoxFuture<'a, Result<Option<OAuthBinding>, OAuthError>>;
    /// Atomically replace or remove the service binding.
    fn save<'a>(
        &'a self,
        server: &'a str,
        binding: Option<OAuthBinding>,
    ) -> BoxFuture<'a, Result<(), OAuthError>>;
}

/// Credential payload deliberately has no Debug implementation.
#[derive(Clone, Serialize, Deserialize)]
pub struct OAuthSecrets {
    /// Access token, passed only as an HTTP Authorization header.
    pub access_token: String,
    /// Rotated refresh token, when available.
    pub refresh_token: Option<String>,
    /// Optional preregistered confidential client secret.
    pub client_secret: Option<String>,
}

/// Secret storage must be encrypted by the OS. There is no file fallback.
pub trait OAuthSecretStore: Send + Sync {
    /// Read a credential; absence is distinct from an unavailable keychain.
    fn load<'a>(
        &'a self,
        reference: &'a str,
    ) -> BoxFuture<'a, Result<Option<OAuthSecrets>, OAuthError>>;
    /// Atomically replace a credential (including rotating refresh tokens).
    fn save<'a>(
        &'a self,
        reference: &'a str,
        secrets: OAuthSecrets,
    ) -> BoxFuture<'a, Result<(), OAuthError>>;
    /// Delete only the credential owned by this binding.
    fn delete<'a>(&'a self, reference: &'a str) -> BoxFuture<'a, Result<(), OAuthError>>;
}

/// macOS generic-password storage under a dedicated application service.
pub struct KeychainSecretStore;

#[cfg(target_os = "macos")]
impl OAuthSecretStore for KeychainSecretStore {
    fn load<'a>(
        &'a self,
        reference: &'a str,
    ) -> BoxFuture<'a, Result<Option<OAuthSecrets>, OAuthError>> {
        let reference = reference.to_owned();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let options = security_framework::passwords::PasswordOptions::new_generic_password(
                    "dev.zkcode.mcp.oauth",
                    &reference,
                );
                match security_framework::passwords::generic_password(options) {
                    Ok(bytes) => serde_json::from_slice(&bytes)
                        .map(Some)
                        .map_err(|_| OAuthError::SecretStorage),
                    Err(error) if error.code() == -25300 => Ok(None), // errSecItemNotFound
                    Err(_) => Err(OAuthError::SecretStorage),
                }
            })
            .await
            .map_err(|_| OAuthError::SecretStorage)?
        })
    }
    fn save<'a>(
        &'a self,
        reference: &'a str,
        secrets: OAuthSecrets,
    ) -> BoxFuture<'a, Result<(), OAuthError>> {
        let reference = reference.to_owned();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let bytes = serde_json::to_vec(&secrets).map_err(|_| OAuthError::SecretStorage)?;
                security_framework::passwords::set_generic_password(
                    "dev.zkcode.mcp.oauth",
                    &reference,
                    &bytes,
                )
                .map_err(|_| OAuthError::SecretStorage)
            })
            .await
            .map_err(|_| OAuthError::SecretStorage)?
        })
    }
    fn delete<'a>(&'a self, reference: &'a str) -> BoxFuture<'a, Result<(), OAuthError>> {
        let reference = reference.to_owned();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                match security_framework::passwords::delete_generic_password(
                    "dev.zkcode.mcp.oauth",
                    &reference,
                ) {
                    Ok(()) => Ok(()),
                    Err(error) if error.code() == -25300 => Ok(()),
                    Err(_) => Err(OAuthError::SecretStorage),
                }
            })
            .await
            .map_err(|_| OAuthError::SecretStorage)?
        })
    }
}

#[cfg(not(target_os = "macos"))]
impl OAuthSecretStore for KeychainSecretStore {
    fn load<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Option<OAuthSecrets>, OAuthError>> {
        Box::pin(async { Err(OAuthError::SecretStorage) })
    }
    fn save<'a>(&'a self, _: &'a str, _: OAuthSecrets) -> BoxFuture<'a, Result<(), OAuthError>> {
        Box::pin(async { Err(OAuthError::SecretStorage) })
    }
    fn delete<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<(), OAuthError>> {
        Box::pin(async { Err(OAuthError::SecretStorage) })
    }
}
