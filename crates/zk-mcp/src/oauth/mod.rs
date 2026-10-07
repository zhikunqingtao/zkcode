//! Native OAuth authorization for explicitly configured HTTP MCP services.
//! Secrets remain behind an OS credential-store port; `SQLite` stores bindings only.

mod callback;
mod discovery;
mod http;
pub mod storage;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio_util::sync::CancellationToken;

use crate::sse::lock;
use storage::{OAuthBinding, OAuthBindingStore, OAuthSecretStore, OAuthSecrets};

const AUTH_LIFETIME: Duration = Duration::from_mins(5);

/// Safe diagnostic codes: none include tokens, authorization codes or response bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum OAuthError {
    /// Endpoint violates outbound policy.
    #[error("OAuth endpoint must use HTTPS and resolve to a public address")]
    UnsafeEndpoint,
    /// Resource or issuer differs from its discovered identity.
    #[error("OAuth resource or issuer binding does not match")]
    BindingMismatch,
    /// Discovery could not find a valid metadata document.
    #[error("OAuth discovery is unavailable for this service")]
    DiscoveryUnavailable,
    /// Invalid or oversized metadata.
    #[error("OAuth server returned invalid metadata")]
    InvalidMetadata,
    /// Mandatory PKCE support is absent.
    #[error("OAuth server does not advertise PKCE S256")]
    PkceUnsupported,
    /// No legitimate client identity is available.
    #[error(
        "Enter a preregistered OAuth client ID; this server does not support dynamic registration"
    )]
    ClientRegistrationRequired,
    /// Remote rejection, without untrusted response text.
    #[error("OAuth server rejected the request")]
    RemoteRejected,
    /// Network failure, without leaking request URLs.
    #[error("OAuth network request failed")]
    Network,
    /// OS credential store is unavailable.
    #[error("macOS Keychain could not read or save the OAuth credential")]
    SecretStorage,
    /// Durable non-secret metadata could not be committed.
    #[error("OAuth binding could not be read or saved")]
    BindingStorage,
    /// Credentials are absent or no longer refreshable.
    #[error("OAuth authorization is required")]
    AuthorizationRequired,
    /// User/service cancellation or expiration.
    #[error("OAuth authorization was cancelled or expired")]
    Cancelled,
}

/// User-supplied preregistration is optional when the server supports DCR.
/// No Debug implementation: a client secret may be present.
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OAuthOptions {
    /// Issued by the authorization server, never synthesized from a service name.
    pub client_id: Option<String>,
    /// Optional secret for a preregistered confidential client.
    pub client_secret: Option<String>,
}

/// Safe browser flow descriptor. PKCE verifier and tokens are never returned.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OAuthStart {
    /// User must open this URL to review the authorization server's consent page.
    pub authorization_url: String,
    /// The selected authorization party.
    pub issuer: String,
    /// Exact MCP resource receiving the token.
    pub resource: String,
    /// Actual loopback redirect, useful for preregistered native clients.
    pub redirect_uri: String,
    /// Requested scopes, when supplied by the resource server.
    pub scope: Option<String>,
    /// Total authorization deadline in seconds.
    pub expires_in: u64,
}

/// Management view. Authorized intent is separate from transport connectivity.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OAuthStatus {
    /// idle, pending, authorized, or error.
    pub state: String,
    /// Redacted, local diagnostic message.
    pub error: Option<String>,
}

struct Pending {
    id: String,
    cancel: CancellationToken,
    status: OAuthStatus,
}

/// Per-service serialized token rotation and one-time browser authorization.
pub struct OAuthCoordinator {
    bindings: Arc<dyn OAuthBindingStore>,
    secrets: Arc<dyn OAuthSecretStore>,
    http: http::OAuthHttp,
    locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    pending: Mutex<HashMap<String, Pending>>,
}

impl OAuthCoordinator {
    /// Refresh before each request without reconnecting or replaying a tool call.
    pub fn request_authorizer(
        self: &Arc<Self>,
        name: String,
        resource: String,
        cancel: CancellationToken,
    ) -> Arc<dyn crate::transport::RequestAuthorizer> {
        Arc::new(OAuthRequestAuthorizer {
            coordinator: self.clone(),
            name,
            resource,
            cancel,
        })
    }
    /// Construct the production coordinator with durable host storage.
    #[must_use]
    pub fn new(
        bindings: Arc<dyn OAuthBindingStore>,
        secrets: Arc<dyn OAuthSecretStore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            bindings,
            secrets,
            http: http::OAuthHttp::production(),
            locks: Mutex::default(),
            pending: Mutex::default(),
        })
    }

    fn service_lock(&self, name: &str) -> Arc<AsyncMutex<()>> {
        lock(&self.locks)
            .entry(name.to_owned())
            .or_default()
            .clone()
    }

    /// Check whether reconnect must resolve an OAuth token rather than reuse old headers.
    /// # Errors
    /// Unavailable binding storage returns a typed OAuth error.
    pub async fn has_binding(&self, name: &str) -> Result<bool, OAuthError> {
        Ok(self.bindings.load(name).await?.is_some())
    }

    /// A disabled service invalidates its outstanding callback immediately.
    pub fn cancel(&self, name: &str) {
        if let Some(pending) = lock(&self.pending).get_mut(name) {
            pending.cancel.cancel();
            pending.status = OAuthStatus {
                state: "idle".into(),
                error: None,
            };
        }
    }

    /// Read safe status without opening the Keychain or exposing token metadata.
    /// # Errors
    /// Unavailable binding storage returns a typed OAuth error.
    pub async fn status(&self, name: &str) -> Result<OAuthStatus, OAuthError> {
        if let Some(status) = lock(&self.pending).get(name).map(|p| p.status.clone())
            && status.state != "idle"
        {
            return Ok(status);
        }
        Ok(OAuthStatus {
            state: if self.bindings.load(name).await?.is_some() {
                "authorized"
            } else {
                "idle"
            }
            .into(),
            error: None,
        })
    }

    /// Begin a native-app flow. The completion receiver fires only after both
    /// Keychain storage and the durable public binding have succeeded.
    /// # Errors
    /// Invalid endpoints, metadata, cancelled setup or local callback bind failures return errors.
    #[allow(clippy::too_many_lines)] // Keep native callback setup, PKCE and cancellation ownership in sequence.
    pub async fn begin(
        self: &Arc<Self>,
        name: &str,
        resource: &str,
        options: OAuthOptions,
        service_cancel: CancellationToken,
    ) -> Result<(OAuthStart, oneshot::Receiver<Result<(), OAuthError>>), OAuthError> {
        let service_lock = self.service_lock(name);
        let _guard = service_lock.lock().await;
        self.cancel(name);
        let resource_url = self.http.validate_url(resource)?;
        if resource_url.query().is_some() {
            return Err(OAuthError::UnsafeEndpoint);
        }
        let resource = resource_url.to_string();
        let (metadata, scope) = self
            .cancellable(&service_cancel, discovery::discover(&self.http, &resource))
            .await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| OAuthError::Network)?;
        let flow_id = random_secret()?;
        let redirect_uri = format!(
            "http://127.0.0.1:{}/mcp/oauth/{flow_id}",
            listener
                .local_addr()
                .map_err(|_| OAuthError::Network)?
                .port()
        );
        let (client_id, client_secret) = if let Some(id) =
            options.client_id.filter(|id| !id.trim().is_empty())
        {
            if id.len() > 2048
                || options
                    .client_secret
                    .as_ref()
                    .is_some_and(|s| s.len() > 8192)
            {
                return Err(OAuthError::InvalidMetadata);
            }
            (id, options.client_secret)
        } else {
            let endpoint = metadata
                .registration_endpoint
                .as_deref()
                .ok_or(OAuthError::ClientRegistrationRequired)?;
            let result = self.cancellable(&service_cancel, self.http.post_json(endpoint, &json!({
                    "client_name":"zkcode", "redirect_uris":[redirect_uri],
                    "grant_types":["authorization_code","refresh_token"], "response_types":["code"],
                    "token_endpoint_auth_method":"none"
                }))).await?;
            let id = result
                .get("client_id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or(OAuthError::InvalidMetadata)?
                .to_owned();
            if result
                .get("token_endpoint_auth_method")
                .and_then(Value::as_str)
                .is_some_and(|method| method != "none")
            {
                return Err(OAuthError::InvalidMetadata);
            }
            (id, None)
        };
        let method = if client_secret.is_some() {
            "client_secret_basic"
        } else {
            "none"
        };
        if !metadata.token_endpoint_auth_methods_supported.is_empty()
            && !metadata
                .token_endpoint_auth_methods_supported
                .iter()
                .any(|m| m == method)
        {
            return Err(OAuthError::InvalidMetadata);
        }
        let verifier = random_secret()?;
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let state = random_secret()?;
        let mut authorization_url = self.http.validate_url(&metadata.authorization_endpoint)?;
        authorization_url.query_pairs_mut().extend_pairs([
            ("response_type", "code"),
            ("client_id", client_id.as_str()),
            ("redirect_uri", redirect_uri.as_str()),
            ("state", state.as_str()),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("resource", resource.as_str()),
        ]);
        if let Some(scope) = &scope {
            authorization_url
                .query_pairs_mut()
                .append_pair("scope", scope);
        }
        let start = OAuthStart {
            authorization_url: authorization_url.into(),
            issuer: metadata.issuer.clone(),
            resource: resource.clone(),
            redirect_uri: redirect_uri.clone(),
            scope: scope.clone(),
            expires_in: AUTH_LIFETIME.as_secs(),
        };
        let binding = OAuthBinding {
            resource,
            issuer: metadata.issuer,
            token_endpoint: metadata.token_endpoint,
            revocation_endpoint: metadata.revocation_endpoint,
            client_id,
            credential_ref: uuid::Uuid::new_v4().to_string(),
            scope,
            expires_at: None,
        };
        let cancel = service_cancel.child_token();
        let (finished, receiver) = oneshot::channel();
        lock(&self.pending).insert(
            name.to_owned(),
            Pending {
                id: flow_id.clone(),
                cancel: cancel.clone(),
                status: OAuthStatus {
                    state: "pending".into(),
                    error: None,
                },
            },
        );
        let coordinator = Arc::clone(self);
        let name = name.to_owned();
        tokio::spawn(async move {
            let exchange = async {
                let code =
                    callback::wait_code(listener, &redirect_uri, &state, &binding.issuer).await?;
                let fields = vec![
                    ("grant_type", "authorization_code".into()),
                    ("code", code),
                    ("client_id", binding.client_id.clone()),
                    ("redirect_uri", redirect_uri),
                    ("code_verifier", verifier),
                    ("resource", binding.resource.clone()),
                ];
                let token = coordinator
                    .http
                    .post_form(
                        &binding.token_endpoint,
                        &fields,
                        client_secret
                            .as_deref()
                            .map(|secret| (binding.client_id.as_str(), secret)),
                    )
                    .await?;
                parse_token(binding, &token, None, client_secret)
            };
            let exchanged = coordinator
                .cancellable(&cancel, async {
                    tokio::time::timeout(AUTH_LIFETIME, exchange)
                        .await
                        .map_err(|_| OAuthError::Cancelled)?
                })
                .await;
            // Once a Keychain/SQLite transaction starts, finish its explicit
            // rollback instead of dropping its future on cancellation.
            let result = match exchanged {
                Ok((mut binding, secrets)) => {
                    coordinator
                        .persist_new(&name, &mut binding, secrets, &cancel)
                        .await
                }
                Err(error) => Err(error),
            };
            if let Some(pending) = lock(&coordinator.pending).get_mut(&name)
                && pending.id == flow_id
            {
                pending.status = OAuthStatus {
                    state: if result.is_ok() {
                        "authorized"
                    } else if result == Err(OAuthError::Cancelled) {
                        "idle"
                    } else {
                        "error"
                    }
                    .into(),
                    error: result.as_ref().err().map(ToString::to_string),
                };
            }
            let _ = finished.send(result);
        });
        Ok((start, receiver))
    }

    async fn persist_new(
        &self,
        name: &str,
        binding: &mut OAuthBinding,
        secrets: OAuthSecrets,
        cancel: &CancellationToken,
    ) -> Result<(), OAuthError> {
        let service_lock = self.service_lock(name);
        let _guard = service_lock.lock().await;
        if cancel.is_cancelled() {
            return Err(OAuthError::Cancelled);
        }
        let previous = self.bindings.load(name).await?;
        self.secrets.save(&binding.credential_ref, secrets).await?;
        // Cleanup is awaited explicitly; dropping this future midway must not
        // be used as the cancellation mechanism during the storage transaction.
        if cancel.is_cancelled() {
            self.secrets.delete(&binding.credential_ref).await?;
            return Err(OAuthError::Cancelled);
        }
        if let Err(error) = self.bindings.save(name, Some(binding.clone())).await {
            self.secrets.delete(&binding.credential_ref).await?;
            return Err(error);
        }
        if cancel.is_cancelled() {
            self.bindings.save(name, previous).await?;
            self.secrets.delete(&binding.credential_ref).await?;
            return Err(OAuthError::Cancelled);
        }
        if let Some(previous) = previous
            && previous.credential_ref != binding.credential_ref
        {
            self.secrets.delete(&previous.credential_ref).await?;
        }
        Ok(())
    }

    /// Obtain a resource-bound token, rotating at most once per serialized
    /// refresh. Callers must still check service enablement before transport I/O.
    /// # Errors
    /// Invalid or expired bindings, token refresh and credential-store failures return errors.
    pub async fn authorization_header(
        &self,
        name: &str,
        resource: &str,
    ) -> Result<Option<String>, OAuthError> {
        let service_lock = self.service_lock(name);
        let _guard = service_lock.lock().await;
        let Some(binding) = self.bindings.load(name).await? else {
            return Ok(None);
        };
        let configured = self.http.validate_url(resource)?;
        if self.http.validate_url(&binding.resource)? != configured {
            return Err(OAuthError::BindingMismatch);
        }
        let secrets = self
            .secrets
            .load(&binding.credential_ref)
            .await?
            .ok_or(OAuthError::AuthorizationRequired)?;
        if binding
            .expires_at
            .is_none_or(|expiry| expiry > unix_time().saturating_add(60))
        {
            return Ok(Some(format!("Bearer {}", secrets.access_token)));
        }
        let refresh = secrets
            .refresh_token
            .as_ref()
            .ok_or(OAuthError::AuthorizationRequired)?;
        let fields = [
            ("grant_type", "refresh_token".into()),
            ("refresh_token", refresh.clone()),
            ("client_id", binding.client_id.clone()),
            ("resource", binding.resource.clone()),
        ];
        let token = self
            .http
            .post_form(
                &binding.token_endpoint,
                &fields,
                secrets
                    .client_secret
                    .as_deref()
                    .map(|secret| (binding.client_id.as_str(), secret)),
            )
            .await?;
        let (binding, secrets) = parse_token(
            binding,
            &token,
            secrets.refresh_token,
            secrets.client_secret,
        )?;
        self.secrets
            .save(&binding.credential_ref, secrets.clone())
            .await?;
        self.bindings.save(name, Some(binding)).await?;
        Ok(Some(format!("Bearer {}", secrets.access_token)))
    }

    /// Cancel pending consent and remove the local binding even if the server
    /// does not provide a revocation endpoint. Returns whether remote revocation succeeded.
    /// # Errors
    /// Revocation, binding persistence or credential removal failures return errors.
    pub async fn logout(&self, name: &str) -> Result<bool, OAuthError> {
        self.cancel(name);
        let service_lock = self.service_lock(name);
        let _guard = service_lock.lock().await;
        let Some(binding) = self.bindings.load(name).await? else {
            return Ok(false);
        };
        let secrets = self.secrets.load(&binding.credential_ref).await?;
        self.bindings.save(name, None).await?;
        self.secrets.delete(&binding.credential_ref).await?;
        let Some(endpoint) = binding.revocation_endpoint else {
            return Ok(false);
        };
        let Some(secrets) = secrets else {
            return Ok(false);
        };
        let fields = [
            (
                "token",
                secrets.refresh_token.unwrap_or(secrets.access_token),
            ),
            ("client_id", binding.client_id.clone()),
        ];
        Ok(self
            .http
            .revoke(
                &endpoint,
                &fields,
                secrets
                    .client_secret
                    .as_deref()
                    .map(|secret| (binding.client_id.as_str(), secret)),
            )
            .await
            .is_ok())
    }

    async fn cancellable<T>(
        &self,
        cancel: &CancellationToken,
        future: impl std::future::Future<Output = Result<T, OAuthError>>,
    ) -> Result<T, OAuthError> {
        tokio::select! { biased; () = cancel.cancelled() => Err(OAuthError::Cancelled), result = future => result }
    }
}

struct OAuthRequestAuthorizer {
    coordinator: Arc<OAuthCoordinator>,
    name: String,
    resource: String,
    cancel: CancellationToken,
}

impl crate::transport::RequestAuthorizer for OAuthRequestAuthorizer {
    fn authorize(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> futures::future::BoxFuture<'_, Result<reqwest::RequestBuilder, crate::McpProtocolError>>
    {
        Box::pin(async move {
            let prepare = async {
                if self.cancel.is_cancelled() {
                    return Err(OAuthError::Cancelled);
                }
                let Some(header) = self
                    .coordinator
                    .authorization_header(&self.name, &self.resource)
                    .await?
                else {
                    return Ok(builder);
                };
                if self.cancel.is_cancelled() {
                    return Err(OAuthError::Cancelled);
                }
                let mut request = builder.build().map_err(|_| OAuthError::InvalidMetadata)?;
                let resource = self.coordinator.http.validate_url(&self.resource)?;
                if request.url().origin() != resource.origin() {
                    return Err(OAuthError::BindingMismatch);
                }
                // Each credential-bearing dispatch revalidates and pins DNS;
                // an advertised SSE endpoint may differ in path, never origin.
                let client = self.coordinator.http.resource_client(request.url()).await?;
                request.headers_mut().insert(
                    reqwest::header::AUTHORIZATION,
                    header.parse().map_err(|_| OAuthError::InvalidMetadata)?,
                );
                if self.cancel.is_cancelled() {
                    return Err(OAuthError::Cancelled);
                }
                Ok(reqwest::RequestBuilder::from_parts(client, request))
            };
            prepare
                .await
                .map_err(|error| crate::McpProtocolError::wrapped(error.to_string()))
        })
    }
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn random_secret() -> Result<String, OAuthError> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| OAuthError::SecretStorage)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn parse_token(
    mut binding: OAuthBinding,
    value: &Value,
    previous_refresh: Option<String>,
    client_secret: Option<String>,
) -> Result<(OAuthBinding, OAuthSecrets), OAuthError> {
    if value
        .get("token_type")
        .and_then(Value::as_str)
        .is_none_or(|kind| !kind.eq_ignore_ascii_case("bearer"))
    {
        return Err(OAuthError::InvalidMetadata);
    }
    let access_token = value
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty() && token.len() <= 16_384 && !token.contains(['\r', '\n']))
        .ok_or(OAuthError::InvalidMetadata)?
        .to_owned();
    binding.expires_at = match value.get("expires_in") {
        None => None,
        Some(value) => Some(
            unix_time()
                .checked_add(value.as_u64().ok_or(OAuthError::InvalidMetadata)?)
                .ok_or(OAuthError::InvalidMetadata)?,
        ),
    };
    if let Some(scope) = value.get("scope").and_then(Value::as_str) {
        binding.scope = Some(scope.into());
    }
    let refresh_token = value
        .get("refresh_token")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or(previous_refresh);
    Ok((
        binding,
        OAuthSecrets {
            access_token,
            refresh_token,
            client_secret,
        },
    ))
}
