//! Persistent service switches, independent from each capability's preference.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use super::{ManagerError, McpClientManager};
use crate::sse::lock;
use crate::{McpConnectionStatus, McpServerConfig};

/// Host persistence port. A failed save must leave the prior value intact.
pub trait McpServicePreferenceStore: Send + Sync {
    /// Load explicit preferences; absence preserves existing connection defaults.
    fn load(&self) -> BoxFuture<'_, Result<BTreeMap<String, bool>, String>>;
    /// Atomically replace the complete preference map.
    fn save(&self, values: BTreeMap<String, bool>) -> BoxFuture<'_, Result<(), String>>;
}

pub(super) struct ServiceState {
    pub preferences: Mutex<BTreeMap<String, bool>>,
    pub store: Option<Arc<dyn McpServicePreferenceStore>>,
    pub loaded: AtomicBool,
    pub mutation: tokio::sync::Mutex<()>,
    pub directory: Mutex<()>,
    pub configs: Mutex<BTreeMap<String, McpServerConfig>>,
    pub registry_configs: Mutex<HashSet<String>>,
    pub cancellations: Mutex<BTreeMap<String, CancellationToken>>,
}

impl ServiceState {
    pub fn new(store: Option<Arc<dyn McpServicePreferenceStore>>) -> Self {
        Self {
            loaded: AtomicBool::new(store.is_none()),
            store,
            preferences: Mutex::default(),
            mutation: tokio::sync::Mutex::new(()),
            directory: Mutex::default(),
            configs: Mutex::default(),
            registry_configs: Mutex::default(),
            cancellations: Mutex::default(),
        }
    }
}

/// Secret-free authoritative service view for the management UI.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServiceView {
    /// Stable server key shared by connection and capability catalogs.
    pub name: String,
    /// Transport type, without command arguments, headers or credentials.
    pub transport: String,
    /// Configuration provenance assigned by the host.
    pub scope: String,
    /// Service preference; tool preferences remain independent.
    pub enabled: bool,
    /// Observed state, distinct from enabled intent.
    pub status: String,
    /// Currently callable, policy-filtered tools.
    pub tool_count: usize,
}

impl McpClientManager {
    /// Load preferences before any transport can start. Invalid storage fails closed.
    /// # Errors
    /// Unavailable or malformed durable preferences fail closed.
    pub async fn load_service_preferences(&self) -> Result<(), ManagerError> {
        let _change = self.services.mutation.lock().await;
        if self.services.loaded.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(store) = &self.services.store {
            let values = store
                .load()
                .await
                .map_err(|_| ManagerError::ServiceStorageUnavailable)?;
            *lock(&self.services.preferences) = values;
        }
        self.services.loaded.store(true, Ordering::Release);
        Ok(())
    }

    /// Effective service gate, used by all connection and publication paths.
    #[must_use]
    pub fn is_service_enabled(&self, name: &str) -> bool {
        self.services.loaded.load(Ordering::Acquire)
            && self
                .scope_context
                .as_ref()
                .is_none_or(|context| !context.cancel.is_cancelled())
            && self.scope_parent.as_ref().is_none_or(|parent| {
                parent
                    .upgrade()
                    .is_some_and(|parent| parent.is_service_enabled(name))
            })
            && lock(&self.services.preferences)
                .get(name)
                .copied()
                .unwrap_or(true)
    }

    /// A diagnostic connection obeys the same disable/cancellation gate as a
    /// persistent service and is always closed before returning.
    /// # Errors
    /// Disabled services, invalid registry configuration and failed probes return errors.
    pub async fn probe_registry_service(
        &self,
        definition: &crate::McpCapabilityDefinition,
    ) -> Result<bool, ManagerError> {
        self.load_service_preferences().await?;
        let name = definition.extract_server_key();
        let cancel = {
            let _directory = lock(&self.services.directory);
            if !self.is_service_enabled(&name) {
                return Err(ManagerError::ServiceDisabled(name));
            }
            lock(&self.services.cancellations)
                .entry(name.clone())
                .or_default()
                .clone()
        };
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(ManagerError::ServiceDisabled(name)),
            result = tokio::time::timeout(std::time::Duration::from_secs(5), crate::security::validate_capability_destination(definition)) => {
                result.map_err(|_| ManagerError::UnsafeCapabilityEndpoint("MCP destination validation timed out".into()))?
                    .map_err(|error| ManagerError::UnsafeCapabilityEndpoint(error.to_string()))?;
            }
        }
        let connection = self.new_connection(self.build_resolved_config_from_registry(definition));
        tokio::select! {
            biased;
            () = cancel.cancelled() => { connection.close().await; return Err(ManagerError::ServiceDisabled(name)); }
            () = connection.connect() => {}
        }
        let alive = connection.is_alive();
        connection.close().await;
        if cancel.is_cancelled() {
            return Err(ManagerError::ServiceDisabled(name));
        }
        Ok(alive)
    }

    pub(super) fn service_configs(&self) -> BTreeMap<String, McpServerConfig> {
        let mut configs = BTreeMap::new();
        if let Some(registry) = &self.registry {
            for definition in registry.list_all() {
                let config = self.build_resolved_config_from_registry(&definition);
                configs.entry(config.name.clone()).or_insert(config);
            }
        }
        for config in self
            .static_configs
            .iter()
            .cloned()
            .chain(self.resolver.resolve_all())
        {
            configs.insert(config.name.clone(), config);
        }
        let registry_names = lock(&self.services.registry_configs);
        configs.extend(
            lock(&self.services.configs)
                .iter()
                .filter(|(name, _)| !registry_names.contains(*name))
                .map(|(name, config)| (name.clone(), config.clone())),
        );
        configs
    }

    /// Includes disabled services, which intentionally have no live connection.
    /// # Errors
    /// A failed preference restore is surfaced instead of showing every service as enabled.
    pub async fn list_services(&self) -> Result<Vec<McpServiceView>, ManagerError> {
        self.load_service_preferences().await?;
        Ok(self
            .service_configs()
            .into_values()
            .map(|config| {
                let enabled = self.is_service_enabled(&config.name);
                let connection = self.get_connection(&config.name);
                let connected = enabled
                    && connection
                        .as_ref()
                        .is_some_and(|c| c.status() == McpConnectionStatus::Connected);
                McpServiceView {
                    name: config.name.clone(),
                    transport: config.transport.as_str().to_owned(),
                    scope: config.scope.as_str().to_owned(),
                    enabled,
                    status: if enabled {
                        connection.as_ref().map_or_else(
                            || "disconnected".to_owned(),
                            |c| c.status().as_str().to_ascii_lowercase(),
                        )
                    } else {
                        "disabled".to_owned()
                    },
                    tool_count: if connected {
                        connection.map_or(0, |c| {
                            c.tools()
                                .iter()
                                .filter(|tool| self.is_tool_allowed(&config.name, &tool.name))
                                .count()
                        })
                    } else {
                        0
                    },
                }
            })
            .collect())
    }

    /// Persist intent before applying it. Disabling revokes the catalog and any
    /// pending connect; enabling never changes individual capability preferences.
    /// # Errors
    /// Unknown services or failed transactional persistence leave the previous preference intact.
    pub async fn set_service_enabled(
        self: &Arc<Self>,
        name: &str,
        enabled: bool,
    ) -> Result<McpServiceView, ManagerError> {
        self.load_service_preferences().await?;
        self.require_running()?;
        let config = self
            .service_configs()
            .remove(name)
            .ok_or_else(|| ManagerError::ServerNotFound(name.to_owned()))?;
        let removed = {
            let _change = self.services.mutation.lock().await;
            let mut next = lock(&self.services.preferences).clone();
            next.insert(name.to_owned(), enabled);
            if let Some(store) = &self.services.store {
                store
                    .save(next.clone())
                    .await
                    .map_err(|_| ManagerError::ServiceStorageUnavailable)?;
            }
            let _directory = lock(&self.services.directory);
            *lock(&self.services.preferences) = next;
            if enabled {
                let mut tokens = lock(&self.services.cancellations);
                if tokens
                    .get(name)
                    .is_some_and(CancellationToken::is_cancelled)
                {
                    tokens.insert(name.to_owned(), CancellationToken::new());
                }
                None
            } else {
                if let Some(oauth) = &self.oauth {
                    oauth.cancel(name);
                }
                lock(&self.services.cancellations)
                    .entry(name.to_owned())
                    .or_default()
                    .cancel();
                self.next_generation(name);
                self.clear_tool_directory(name);
                self.cancel_reconnect_work(name);
                lock(&self.registry_owned_servers).remove(name);
                lock(&self.connections).remove(name)
            }
        };
        if !enabled {
            if let Some(connection) = removed {
                connection.close().await;
            }
            if !self.is_service_enabled(name) {
                self.broadcast_health_status(name, McpConnectionStatus::Disabled);
            }
        } else if self
            .get_connection(name)
            .is_none_or(|c| c.status() != McpConnectionStatus::Connected)
        {
            // Registry credentials must be resolved afresh. Explicit file/runtime
            // configuration continues to take priority over the registry.
            let registry_definition = if lock(&self.services.registry_configs).contains(name)
                || !lock(&self.services.configs).contains_key(name)
            {
                self.registry.as_ref().and_then(|r| {
                    r.list_all()
                        .into_iter()
                        .find(|d| d.extract_server_key() == name)
                })
            } else {
                None
            };
            if let Some(definition) = registry_definition {
                self.enable_from_registry(&definition).await?;
            } else {
                self.add_server(config).await?;
            }
        }
        self.list_services()
            .await?
            .into_iter()
            .find(|s| s.name == name)
            .ok_or_else(|| ManagerError::ServerNotFound(name.to_owned()))
    }

    /// Rebuild a live service directory after a capability preference changes.
    pub fn refresh_service_tools(self: &Arc<Self>, name: &str) {
        let _directory = lock(&self.services.directory);
        self.clear_tool_directory(name);
        if let Some(connection) = self.get_connection(name) {
            self.register_tools_locked(&connection);
        }
    }
}
