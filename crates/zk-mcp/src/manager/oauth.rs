use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{ManagerError, McpClientManager};
use crate::oauth::{OAuthError, OAuthOptions, OAuthStart, OAuthStatus};
use crate::sse::lock;
use crate::{McpServerConfig, McpServerConnection, McpTransportType};

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
        let (config, from_registry, connection, generation, cancel) = {
            let _directory = lock(&self.services.directory);
            if !self.is_service_enabled(name) {
                return Err(ManagerError::ServiceDisabled(name.to_owned()));
            }
            let config = self
                .service_configs()
                .remove(name)
                .ok_or_else(|| ManagerError::ServerNotFound(name.to_owned()))?;
            let cancel = lock(&self.services.cancellations)
                .entry(name.to_owned())
                .or_default()
                .clone();
            (
                config,
                lock(&self.services.registry_configs).contains(name),
                self.get_connection(name),
                self.generation_of(name),
                cancel,
            )
        };
        if !matches!(
            config.transport,
            McpTransportType::Http | McpTransportType::Sse | McpTransportType::SseIde
        ) {
            return Err(OAuthError::UnsafeEndpoint.into());
        }
        let oauth = self.oauth.as_ref().ok_or(OAuthError::SecretStorage)?;
        let resource = config.url.as_deref().ok_or(OAuthError::UnsafeEndpoint)?;
        let (start, completed) = oauth.begin(name, resource, options, cancel.clone()).await?;
        self.spawn_oauth_reconnect(
            config,
            from_registry,
            connection,
            generation,
            cancel,
            completed,
        );
        Ok(start)
    }

    // The real consent continuation is independently testable without discovery,
    // a browser callback listener, or access to the operating system keychain.
    pub(super) fn spawn_oauth_reconnect(
        self: &Arc<Self>,
        config: McpServerConfig,
        from_registry: bool,
        connection: Option<Arc<McpServerConnection>>,
        generation: u64,
        cancel: CancellationToken,
        completed: oneshot::Receiver<Result<(), OAuthError>>,
    ) -> JoinHandle<()> {
        let weak = Arc::downgrade(self);
        let name = config.name.clone();
        tokio::spawn(async move {
            if !matches!(completed.await, Ok(Ok(()))) || cancel.is_cancelled() {
                return;
            }
            let Some(manager) = weak.upgrade() else {
                return;
            };
            {
                let _directory = lock(&manager.services.directory);
                if cancel.is_cancelled()
                    || !manager.is_expected_connection(&name, connection.as_ref(), generation)
                {
                    return;
                }
                // Consent authorizes only the configuration captured before I/O.
                manager.approval.record_approval(&config, "OAUTH_USER");
            }
            if let Err(error) = manager
                .add_server_from_owner(
                    config,
                    from_registry,
                    Some((connection.as_ref(), generation)),
                )
                .await
            {
                tracing::warn!(server = %name, %error, "OAuth completed but MCP reconnect failed");
            }
        })
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
        let revoked = oauth.logout(name).await?;
        if self
            .get_connection(name)
            .is_some_and(|connection| !connection.cleanup_confirmed())
        {
            return Err(ManagerError::CleanupPending(name.to_owned()));
        }
        Ok(revoked)
    }
}
