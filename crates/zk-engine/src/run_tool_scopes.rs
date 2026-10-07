//! Run-owned directory overlays. Credentials remain in factories in process memory.
use dashmap::DashMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use zk_db::{CasOutcome, CleanupStatus, Db, NewToolInvocation, ToolInvocationStatus};
use zk_tools::{
    CallEnv, ExecutionResourceOwner, RunToolScope, RunToolScopeFactory, ToolExecutor, ToolRegistry,
};

struct ScopeState {
    invocation_id: String,
    version: i64,
    scopes: Vec<Arc<dyn RunToolScope>>,
    failed: bool,
    terminal: bool,
}
struct Entry {
    factories: Vec<Arc<dyn RunToolScopeFactory>>,
    directory: std::sync::RwLock<Arc<ToolRegistry>>,
    state: Mutex<ScopeState>,
}

/// Shared by root and child factories; the durable Task/Run remains the authority.
#[derive(Default)]
pub struct RunToolScopes {
    defaults: Vec<Arc<dyn RunToolScopeFactory>>,
    entries: DashMap<String, Arc<Entry>>,
    local_directories: DashMap<String, Arc<ToolRegistry>>,
    available_names: DashMap<String, BTreeSet<String>>,
}

impl RunToolScopes {
    /// Default factories prepare lightweight descriptors; lazy services start on use.
    #[must_use]
    pub fn new(defaults: Vec<Arc<dyn RunToolScopeFactory>>) -> Self {
        Self {
            defaults,
            entries: DashMap::new(),
            local_directories: DashMap::new(),
            available_names: DashMap::new(),
        }
    }

    /// Prepare host defaults for an already claimed non-model `TaskRuntime` execution.
    /// The adapter supplies its authorized Session workspace, never a client cwd.
    /// # Errors
    /// Invalid ownership, closed Runs or failed setup/ledger writes return fixed error codes.
    pub async fn prepare_for_execution(
        &self,
        db: &Db,
        supervisor: &crate::ExecutionSupervisor,
        execution: &crate::TaskExecutionContext,
        workspace: &std::path::Path,
        base: Arc<ToolRegistry>,
    ) -> Result<Arc<ToolRegistry>, String> {
        let ephemeral = db
            .session_retention(&execution.transcript_session_id)
            .await
            .map_err(|_| "RUN_SCOPE_CONTENT_POLICY_UNAVAILABLE")?
            == zk_db::content::ContentRetention::Ephemeral;
        let env = CallEnv::new()
            .with_session_id(&execution.transcript_session_id)
            .with_run_id(&execution.run_id)
            .with_working_dir(workspace)
            .with_ephemeral_content(ephemeral);
        self.prepare(
            db,
            &supervisor.executor(),
            &execution.run_id,
            env,
            execution.cancel.clone(),
            base,
            self.factories(None, None),
        )
        .await
    }

    pub(crate) fn factories(
        &self,
        parent: Option<&str>,
        extra: Option<Arc<dyn RunToolScopeFactory>>,
    ) -> Vec<Arc<dyn RunToolScopeFactory>> {
        let mut factories = parent
            .and_then(|id| self.entries.get(id).map(|entry| entry.factories.clone()))
            .unwrap_or_else(|| self.defaults.clone());
        factories.extend(extra);
        factories
    }

    /// Resolve an exact active Run's directory; callers never substitute another session.
    #[must_use]
    pub fn directory(&self, run: &str) -> Option<Arc<ToolRegistry>> {
        self.entries
            .get(run)
            .map(|entry| {
                entry
                    .directory
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            })
            .or_else(|| self.local_directories.get(run).map(|entry| entry.clone()))
    }

    pub(crate) fn knows_tool(&self, run: &str, name: &str) -> bool {
        self.available_names
            .get(run)
            .is_some_and(|names| names.contains(name))
    }

    pub(crate) fn narrow(&self, run: &str, directory: Arc<ToolRegistry>) {
        if let Some(entry) = self.entries.get(run) {
            *entry
                .directory
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = directory;
        } else if self.local_directories.contains_key(run) {
            self.local_directories.insert(run.into(), directory);
        }
    }

    /// Create a real, long-lived setup invocation before any process is started.
    #[allow(clippy::too_many_arguments)] // Shared root/child adapter passes independently authorized runtime components.
    #[allow(
        clippy::too_many_lines,
        reason = "Setup keeps ownership, persisted policy, scope invocation and cleanup ownership in one failure path"
    )]
    pub(crate) async fn prepare(
        &self,
        db: &Db,
        executor: &ToolExecutor,
        run_id: &str,
        env: CallEnv,
        cancel: CancellationToken,
        base: Arc<ToolRegistry>,
        factories: Vec<Arc<dyn RunToolScopeFactory>>,
    ) -> Result<Arc<ToolRegistry>, String> {
        let ceiling = db
            .run_tool_ceiling(run_id)
            .await
            .map_err(|_| "RUN_TOOL_CEILING_UNAVAILABLE")?;
        let run = db
            .find_run_by_id(run_id)
            .await
            .map_err(|_| "RUN_SCOPE_STORE_FAILED")?
            .ok_or("RUN_SCOPE_OWNER_MISSING")?;
        if env.run_id_str() != Some(run_id) || env.session_id_str() != Some(run.session_id.as_str())
        {
            return Err("RUN_SCOPE_OWNER_MISMATCH".into());
        }
        self.available_names
            .insert(run_id.into(), base.names().into_iter().collect());
        if factories.is_empty() {
            let directory = apply_ceiling(base, &ceiling);
            self.local_directories
                .insert(run_id.into(), directory.clone());
            return Ok(directory);
        }
        let id = uuid::Uuid::new_v4().to_string();
        let entry = Arc::new(Entry {
            factories: factories.clone(),
            directory: std::sync::RwLock::new(base.clone()),
            state: Mutex::new(ScopeState {
                invocation_id: id.clone(),
                version: 0,
                scopes: Vec::new(),
                failed: false,
                terminal: false,
            }),
        });
        let mut state = entry.state.lock().await;
        match self.entries.entry(run_id.into()) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                return Err("RUN_SCOPE_ALREADY_PREPARED".into());
            }
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(entry.clone());
            }
        }
        let record = db
            .create_run_scope_invocation(&NewToolInvocation {
                invocation_id: id.clone(),
                task_id: run.task_id.clone(),
                run_id: run_id.into(),
                tool_use_id: uuid::Uuid::new_v4().to_string(),
                tool_name: "RunToolScope".into(),
                input_json: Some("{}".into()),
                side_effect_class: "write".into(),
                directory_generation: None,
                connection_generation: None,
            })
            .await;
        if let Ok(record) = record {
            state.version = record.version;
        } else {
            self.entries.remove(run_id);
            return Err("RUN_SCOPE_STORE_FAILED".into());
        }
        if cancel.is_cancelled() {
            state.failed = true;
            return Err("RUN_SCOPE_CANCELLED".into());
        }
        if let Ok(CasOutcome::Applied) = db
            .start_tool_invocation_for_active_run_cas(&id, state.version, "{}", "write")
            .await
        {
            state.version += 1;
        } else {
            state.failed = true;
            return Err("RUN_SCOPE_ADMISSION_FAILED".into());
        }
        let context = executor.process_context(
            cancel,
            env.with_execution_resources(
                ExecutionResourceOwner {
                    task_id: run.task_id,
                    run_id: run_id.into(),
                    invocation_id: id,
                },
                crate::execution_resources::DbExecutionResourceObserver::shared(db.clone()),
            ),
        );
        let mut directory = base;
        for factory in factories {
            if let Ok(scope) = factory.prepare(context.clone(), directory.clone()).await {
                directory = scope.registry();
                state.scopes.push(scope);
            } else {
                state.failed = true;
                return Err("RUN_SCOPE_SETUP_FAILED".into());
            }
        }
        self.available_names
            .insert(run_id.into(), directory.names().into_iter().collect());
        let directory = apply_ceiling(directory, &ceiling);
        *entry
            .directory
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = directory.clone();
        Ok(directory)
    }

    /// Physical cleanup and its invocation CAS precede the enclosing Run's terminal commit.
    /// Keep the entry on failure so resources and credentials still have an owner.
    /// # Errors
    /// Physical or durable cleanup failures retain the owned entry for a truthful retry.
    pub async fn cleanup(&self, db: &Db, run_id: &str) -> Result<(), String> {
        self.local_directories.remove(run_id);
        self.available_names.remove(run_id);
        let Some(entry) = self.entries.get(run_id).map(|entry| entry.clone()) else {
            return Ok(());
        };
        let mut state = entry.state.lock().await;
        // Another caller can hold this Arc after the first cleanup removed it
        // from the map. A completed CAS remains idempotent under that race.
        if state.terminal {
            return Ok(());
        }
        let mut failed = false;
        for scope in state.scopes.iter().rev() {
            if scope.cleanup().await.is_err() {
                failed = true;
            }
        }
        if failed {
            return Err("RUN_SCOPE_CLEANUP_UNCONFIRMED".into());
        }
        let invocation = state.invocation_id.clone();
        let live=db.with_reader(move |connection| Ok(connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_resources WHERE invocation_id=?1 AND status!='released')",[invocation],|row|row.get::<_,bool>(0))?
        )).await.map_err(|_| "RUN_SCOPE_CLEANUP_STORE_FAILED")?;
        if live {
            return Err("RUN_SCOPE_CLEANUP_UNCONFIRMED".into());
        }
        let (target, code) = if state.failed {
            (ToolInvocationStatus::Failed, Some("RUN_SCOPE_SETUP_FAILED"))
        } else {
            (ToolInvocationStatus::Succeeded, None)
        };
        for attempt in 0..8 {
            match db
                .finish_run_scope_invocation(
                    &state.invocation_id,
                    state.version,
                    target,
                    code,
                    CleanupStatus::Confirmed,
                )
                .await
            {
                Ok(CasOutcome::Applied) => {
                    state.version += 1;
                    state.terminal = true;
                    self.entries.remove(run_id);
                    return Ok(());
                }
                Ok(CasOutcome::InvalidTransition | CasOutcome::VersionConflict) => {
                    match db
                        .reconcile_run_scope_cleanup(&state.invocation_id, run_id)
                        .await
                    {
                        Ok(CasOutcome::Applied) => {
                            state.terminal = true;
                            self.entries.remove(run_id);
                            return Ok(());
                        }
                        _ => return Err("RUN_SCOPE_TERMINAL_CONFLICT".into()),
                    }
                }
                Ok(_) => return Err("RUN_SCOPE_TERMINAL_CONFLICT".into()),
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(25_u64 << attempt.min(5)))
                        .await;
                }
            }
        }
        Err("RUN_SCOPE_TERMINAL_STORE_FAILED".into())
    }
}

fn apply_ceiling(
    directory: Arc<ToolRegistry>,
    ceiling: &zk_db::tool_ceiling::ToolCeiling,
) -> Arc<ToolRegistry> {
    if ceiling.unrestricted() {
        return directory;
    }
    let ceiling = ceiling.clone();
    Arc::new(ToolRegistry::overlay(directory).filtered_by(move |name, _| ceiling.allows(name)))
}
