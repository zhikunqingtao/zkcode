use std::sync::Arc;

use super::{ManagerError, McpClientManager};
use crate::McpTransportType;
use crate::oauth::{OAuthError, OAuthOptions, OAuthStart, OAuthStatus};
use crate::sse::lock;

impl McpClientManager {
    /// Start explicit browser consent for a configured HTTP service.
    /// # Errors
    /// Disabled/unknown services, unsupported transports and OAuth setup failures are rejected.
    pub async fn begin_oauth(
        self: &Arc<Self>,
        name: &str,
        options: OAuthOptions,
    ) -> Result<OAuthStart, ManagerError> {
        self.require_running()?;
        self.load_service_preferences().await?;
        let config = self
            .service_configs()
            .remove(name)
            .ok_or_else(|| ManagerError::ServerNotFound(name.to_owned()))?;
        if !matches!(
            config.transport,
            McpTransportType::Http | McpTransportType::Sse | McpTransportType::SseIde
        ) {
            return Err(OAuthError::UnsafeEndpoint.into());
        }
        let oauth = self.oauth.as_ref().ok_or(OAuthError::SecretStorage)?;
        let cancel = {
            let _directory = lock(&self.services.directory);
            if !self.is_service_enabled(name) {
                return Err(ManagerError::ServiceDisabled(name.to_owned()));
            }
            lock(&self.services.cancellations)
                .entry(name.to_owned())
                .or_default()
                .clone()
        };
        let resource = config.url.as_deref().ok_or(OAuthError::UnsafeEndpoint)?;
        let generation = self.generation_of(name);
        let (start, completed) = oauth.begin(name, resource, options, cancel.clone()).await?;
        let weak = Arc::downgrade(self);
        let name = name.to_owned();
        tokio::spawn(async move {
            if !matches!(completed.await, Ok(Ok(()))) || cancel.is_cancelled() {
                return;
            }
            let Some(manager) = weak.upgrade() else {
                return;
            };
            if !manager.is_service_enabled(&name)
                || !manager.is_running()
                || manager.generation_of(&name) != generation
            {
                return;
            }
            // Explicit, completed OAuth consent authorizes this configuration's
            // connection. Each tool call still enters the existing Admission chain.
            manager.approval.record_approval(&config, "OAUTH_USER");
            let from_registry = lock(&manager.services.registry_configs).contains(&name);
            if let Err(error) = manager.add_server_from(config, from_registry).await {
                tracing::warn!(server = %name, %error, "OAuth completed but MCP reconnect failed");
            }
        });
        Ok(start)
    }

    /// Public OAuth status never includes credential values.
    /// # Errors
    /// Unknown services and unavailable OAuth metadata stores return errors.
    pub async fn oauth_status(&self, name: &str) -> Result<OAuthStatus, ManagerError> {
        if !self.service_configs().contains_key(name) {
            return Err(ManagerError::ServerNotFound(name.to_owned()));
        }
        Ok(self
            .oauth
            .as_ref()
            .ok_or(OAuthError::SecretStorage)?
            .status(name)
            .await?)
    }

    /// Logout first removes the active transport, preventing its old header from
    /// being reused even when remote revocation or Keychain access fails.
    /// # Errors
    /// Unknown services, revocation, metadata and credential-store failures return errors.
    pub async fn logout_oauth(&self, name: &str) -> Result<bool, ManagerError> {
        if !self.service_configs().contains_key(name) {
            return Err(ManagerError::ServerNotFound(name.to_owned()));
        }
        let oauth = self.oauth.as_ref().ok_or(OAuthError::SecretStorage)?;
        oauth.cancel(name);
        self.remove_server(name).await;
        Ok(oauth.logout(name).await?)
    }
}
