//! Session-authorized Skill views: shared preferences, isolated project content.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Instant;

use futures::future::BoxFuture;
use zk_db::{Db, DbError};
use zk_tools::{RunToolScope, RunToolScopeFactory, ToolContext, ToolRegistry};

use super::loader::{self, SkillDir, SkillSnapshot};
use super::{SkillDefinition, SkillRegistry, SkillSource};

const MAX_CACHED_WORKSPACES: usize = 128;

#[derive(Debug)]
struct ProjectSkills {
    registry: Arc<SkillRegistry>,
    snapshot: SkillSnapshot,
    dirs: Vec<SkillDir>,
    used: Instant,
    error: Arc<RwLock<Option<String>>>,
    authority: super::filesystem::SourceRoot,
}

/// The one global switch authority plus bounded independent project views.
pub struct SkillCatalog {
    global: Arc<SkillRegistry>,
    db: Db,
    projects: Mutex<HashMap<PathBuf, Arc<Mutex<ProjectSkills>>>>,
}

/// A view never stores project definitions in the global registry.
#[derive(Clone, Debug)]
pub struct SkillView {
    global: Arc<SkillRegistry>,
    project: Option<Arc<SkillRegistry>>,
    error: Arc<RwLock<Option<String>>>,
    // Active Runs retain their refreshed candidate cache through this lease.
    _lease: Option<Arc<Mutex<ProjectSkills>>>,
}

impl std::fmt::Debug for SkillCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SkillCatalog")
            .finish_non_exhaustive()
    }
}

impl SkillView {
    /// A read failure never overwrites the last valid project definitions.
    #[must_use]
    pub fn state_error(&self) -> Option<String> {
        self.error
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .or_else(|| {
                self.project
                    .as_ref()
                    .and_then(|registry| registry.state_error())
            })
            .or_else(|| self.global.state_error())
    }

    /// All visible definitions, including disabled ones, in stable display order.
    #[must_use]
    pub fn manage_skills(&self) -> Vec<SkillDefinition> {
        let mut skills = BTreeMap::<String, SkillDefinition>::new();
        for skill in self
            .global
            .manage_skills()
            .into_iter()
            .filter(|skill| !matches!(skill.source, SkillSource::Project | SkillSource::Plugin))
            .chain(
                self.project
                    .iter()
                    .flat_map(|project| project.manage_skills()),
            )
        {
            let key = skill.name.to_lowercase();
            if skills
                .get(&key)
                .is_none_or(|old| old.source.priority() <= skill.source.priority())
            {
                skills.insert(key, skill);
            }
        }
        let mut skills = skills.into_values().collect::<Vec<_>>();
        skills.sort_by(|left, right| {
            left.effective_name()
                .cmp(right.effective_name())
                .then_with(|| left.name.cmp(&right.name))
        });
        skills
    }

    /// Execution and discovery share the global canonical-name switch.
    #[must_use]
    pub fn all_skills(&self) -> Vec<SkillDefinition> {
        self.manage_skills()
            .into_iter()
            .filter(|skill| self.global.is_enabled(&skill.name))
            .collect()
    }

    /// Resolve aliases only within this authorized view.
    #[must_use]
    pub fn resolve_including_disabled(&self, name: &str) -> Option<SkillDefinition> {
        let name = name.strip_prefix('/').unwrap_or(name);
        let skills = self.manage_skills();
        skills
            .iter()
            .find(|skill| skill.name.eq_ignore_ascii_case(name))
            .cloned()
            .or_else(|| {
                skills
                    .into_iter()
                    .find(|skill| skill.effective_name().eq_ignore_ascii_case(name))
            })
    }

    /// Resolve an enabled definition for actual invocation.
    #[must_use]
    pub fn resolve(&self, name: &str) -> Option<SkillDefinition> {
        self.resolve_including_disabled(name)
            .filter(|skill| self.global.is_enabled(&skill.name))
    }

    /// Publish a global switch only after resolving its identity in this view.
    ///
    /// # Errors
    /// Returns scope/identity validation or the authoritative switch transaction error.
    pub async fn set_enabled(&self, canonical: &str, enabled: bool) -> Result<(), DbError> {
        let skill = self
            .resolve_including_disabled(canonical)
            .filter(|skill| skill.name.eq_ignore_ascii_case(canonical))
            .ok_or_else(|| DbError::Validation("SKILL_NOT_FOUND".into()))?;
        self.global.set_known_enabled(&skill.name, enabled).await
    }

    /// A live preference filter over existing bindings; it cannot add tools.
    #[must_use]
    pub fn filter_tools(&self, base: Arc<ToolRegistry>) -> Arc<ToolRegistry> {
        let view = self.clone();
        Arc::new(
            ToolRegistry::overlay(base)
                .filtered_by(move |name, _| name != "Skill" || !view.all_skills().is_empty()),
        )
    }
}

impl SkillCatalog {
    /// Bind the catalog to the existing authoritative DB and global switches.
    #[must_use]
    pub fn new(global: Arc<SkillRegistry>, db: Db) -> Self {
        Self {
            global,
            db,
            projects: Mutex::new(HashMap::new()),
        }
    }

    /// Global-only view for requests without a session or selected project.
    #[must_use]
    pub fn global_view(&self) -> SkillView {
        SkillView {
            global: self.global.clone(),
            project: None,
            error: Arc::default(),
            _lease: None,
        }
    }

    /// Resolve the trusted scope before reading files. Callers never supply paths.
    ///
    /// # Errors
    /// Returns invalid/unknown scope, exhausted active-view capacity, or database failures.
    pub async fn view(
        &self,
        session_id: Option<&str>,
        project_id: Option<&str>,
    ) -> Result<SkillView, DbError> {
        if session_id.is_some() && project_id.is_some() {
            return Err(DbError::Validation("SKILL_SCOPE_AMBIGUOUS".into()));
        }
        let root = if let Some(session_id) = session_id {
            let id = session_id.to_owned();
            self.db
                .with_reader(move |conn| {
                    use rusqlite::OptionalExtension;
                    conn.query_row(
                        "SELECT working_dir FROM sessions WHERE id=?1",
                        [&id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .ok_or(DbError::SessionNotFound(id))
                })
                .await?
        } else if let Some(project_id) = project_id {
            self.db
                .get_project(project_id)
                .await?
                .ok_or_else(|| DbError::Validation("SKILL_PROJECT_NOT_FOUND".into()))?
                .workspace_root
        } else {
            return Ok(self.global_view());
        };
        // Lexical identity survives deletion/read errors so the last valid view
        // remains available. It comes exclusively from persisted workspace authority.
        let root = std::path::absolute(root)
            .map_err(|_| DbError::Invalid("SKILL_WORKSPACE_INVALID".into()))?;
        let project = {
            let mut projects = self.projects.lock().unwrap_or_else(PoisonError::into_inner);
            if !projects.contains_key(&root) && projects.len() >= MAX_CACHED_WORKSPACES {
                let oldest = projects
                    .iter()
                    .filter(|(_, view)| Arc::strong_count(view) == 1)
                    .filter_map(|(path, view)| view.try_lock().ok().map(|view| (path, view.used)))
                    .min_by_key(|(_, used)| *used)
                    .map(|(path, _)| path.clone());
                let Some(oldest) = oldest else {
                    return Err(DbError::Invalid("SKILL_SCOPE_CAPACITY".into()));
                };
                projects.remove(&oldest);
            }
            projects
                .entry(root.clone())
                .or_insert_with(|| {
                    Arc::new(Mutex::new(ProjectSkills {
                        registry: Arc::new(SkillRegistry::new()),
                        snapshot: SkillSnapshot::default(),
                        dirs: Vec::new(),
                        used: Instant::now(),
                        error: Arc::default(),
                        authority: super::filesystem::SourceRoot::persisted(&root),
                    }))
                })
                .clone()
        };
        let lease = project.clone();
        let (registry, error) =
            tokio::task::spawn_blocking(move || refresh_project(&root, &project))
                .await
                .map_err(|_| DbError::Invalid("SKILL_SCOPE_REFRESH_FAILED".into()))?;
        Ok(SkillView {
            global: self.global.clone(),
            project: Some(registry),
            error,
            _lease: Some(lease),
        })
    }

    /// Refresh already-authorized cached workspaces without switching global content.
    /// The caller owns this task and aborts it together with the global watcher.
    pub fn spawn_watcher(
        self: &Arc<Self>,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        let catalog = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                timer.tick().await;
                let Some(catalog) = catalog.upgrade() else {
                    return;
                };
                let projects = catalog
                    .projects
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .iter()
                    .map(|(root, project)| (root.clone(), project.clone()))
                    .collect::<Vec<_>>();
                drop(catalog);
                if tokio::task::spawn_blocking(move || {
                    for (root, project) in projects {
                        refresh_project(&root, &project);
                    }
                })
                .await
                .is_err()
                {
                    tracing::warn!(
                        code = "SKILL_SCOPE_REFRESH_FAILED",
                        "project Skill watcher failed; retaining last valid views"
                    );
                }
            }
        })
    }

    /// Validate durable Run/Session ownership before a model's contextual lookup.
    ///
    /// # Errors
    /// Rejects mismatched/unknown Run ownership and propagates scope lookup errors.
    pub async fn for_tool(&self, context: &ToolContext) -> Result<SkillView, DbError> {
        if let Some(run_id) = context.run_id() {
            let run = self
                .db
                .find_run_by_id(run_id)
                .await?
                .ok_or_else(|| DbError::Invalid("SKILL_RUN_NOT_FOUND".into()))?;
            if Some(run.session_id.as_str()) != context.session_id() {
                return Err(DbError::Invalid("SKILL_RUN_SCOPE_MISMATCH".into()));
            }
        }
        self.view(context.session_id(), None).await
    }
}

fn refresh_project(
    root: &std::path::Path,
    project: &Mutex<ProjectSkills>,
) -> (Arc<SkillRegistry>, Arc<RwLock<Option<String>>>) {
    let mut project = project.lock().unwrap_or_else(PoisonError::into_inner);
    let current = match loader::project_skill_dirs(root, &project.authority) {
        Ok(current) => current,
        Err(error) => {
            *project
                .error
                .write()
                .unwrap_or_else(PoisonError::into_inner) = Some(error.to_owned());
            project.used = Instant::now();
            return (project.registry.clone(), project.error.clone());
        }
    };
    // Retain old roots so deleted plugin directories remove their old definitions.
    for dir in &current {
        if !project.dirs.contains(dir) {
            project.dirs.push(dir.clone());
        }
    }
    project.dirs.sort_by(|left, right| {
        left.source
            .priority()
            .cmp(&right.source.priority())
            .then_with(|| compatibility_rank(&left.path).cmp(&compatibility_rank(&right.path)))
            .then_with(|| left.path.cmp(&right.path))
    });
    let registry = project.registry.clone();
    let dirs = project.dirs.clone();
    let error = loader::poll_checked(&registry, &dirs, &mut project.snapshot)
        .err()
        .map(str::to_owned);
    if error.is_none() {
        // Removed plugin roots have now been reconciled out of the snapshot;
        // do not retain an unbounded history of directories in a live workspace.
        project.dirs.retain(|dir| current.contains(dir));
    }
    *project
        .error
        .write()
        .unwrap_or_else(PoisonError::into_inner) = error;
    project.used = Instant::now();
    (registry, project.error.clone())
}

fn compatibility_rank(path: &std::path::Path) -> u8 {
    match path
        .parent()
        .and_then(std::path::Path::file_name)
        .and_then(|name| name.to_str())
    {
        Some(zk_core::paths::LEGACY_CONFIG_DIR_NAME) => 0,
        Some(zk_core::paths::CONFIG_DIR_NAME) => 1,
        _ => 2,
    }
}

/// Installs the exact per-Run discovery filter without changing the host registry.
#[derive(Debug)]
pub struct SkillScopeFactory(pub Arc<SkillCatalog>);

struct SkillScope(Arc<ToolRegistry>);

impl RunToolScopeFactory for SkillScopeFactory {
    fn prepare(
        &self,
        context: ToolContext,
        base: Arc<ToolRegistry>,
    ) -> BoxFuture<'_, Result<Arc<dyn RunToolScope>, String>> {
        Box::pin(async move {
            let view = self
                .0
                .for_tool(&context)
                .await
                .map_err(|_| "SKILL_SCOPE_UNAVAILABLE".to_owned())?;
            Ok(Arc::new(SkillScope(view.filter_tools(base))) as Arc<dyn RunToolScope>)
        })
    }
}

impl RunToolScope for SkillScope {
    fn registry(&self) -> Arc<ToolRegistry> {
        self.0.clone()
    }
    fn cleanup(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Workspace(PathBuf);
    impl Workspace {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("zk-skill-scope-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path.canonicalize().unwrap())
        }
        fn write(&self, relative: &str, body: impl AsRef<[u8]>) {
            let path = self.0.join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, body).unwrap();
        }
    }
    impl Drop for Workspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn project_root_aliases_cannot_import_another_registered_project() {
        use std::os::unix::fs::symlink;
        let a = Workspace::new();
        let b = Workspace::new();
        b.write("private/escape.md", "another project's private Skill");
        a.write("inside/valid.md", "authorized alias");
        std::fs::create_dir_all(a.0.join(".zkcode")).unwrap();
        symlink(b.0.join("private"), a.0.join(".zkcode/skills")).unwrap();
        let db = Db::open_in_memory().unwrap();
        let global = Arc::new(SkillRegistry::new());
        let catalog = SkillCatalog::new(global, db.clone());
        let sa = db.create_session("m", a.0.to_str().unwrap()).await.unwrap();
        let _sb = db.create_session("m", b.0.to_str().unwrap()).await.unwrap();
        let rejected = catalog.view(Some(&sa.id), None).await.unwrap();
        assert!(rejected.resolve("escape").is_none());
        assert!(rejected.manage_skills().is_empty());
        assert_eq!(
            rejected.state_error().as_deref(),
            Some("SKILL_SOURCE_UNAUTHORIZED")
        );
        std::fs::remove_file(a.0.join(".zkcode/skills")).unwrap();
        symlink(a.0.join("inside"), a.0.join(".zkcode/skills")).unwrap();
        let recovered = catalog.view(Some(&sa.id), None).await.unwrap();
        assert_eq!(
            recovered.resolve("valid").unwrap().content,
            "authorized alias"
        );
        assert!(recovered.state_error().is_none());
        // An already leased view must reject revoked source authority before polling.
        std::fs::remove_file(a.0.join(".zkcode/skills")).unwrap();
        symlink(b.0.join("private"), a.0.join(".zkcode/skills")).unwrap();
        assert!(recovered.resolve("valid").is_none());
        assert!(recovered.manage_skills().is_empty());
        assert!(
            catalog
                .view(Some(&sa.id), None)
                .await
                .unwrap()
                .resolve("escape")
                .is_none()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ancestor_and_plugin_root_aliases_follow_the_same_project_authority() {
        use std::os::unix::fs::symlink;
        let a = Workspace::new();
        let b = Workspace::new();
        b.write("config/skills/ancestor.md", "outside ancestor");
        b.write("plugins/pack/skills/plugin.md", "outside plugin");
        symlink(b.0.join("config"), a.0.join(".zkcode")).unwrap();
        let db = Db::open_in_memory().unwrap();
        let catalog = SkillCatalog::new(Arc::new(SkillRegistry::new()), db.clone());
        let session = db.create_session("m", a.0.to_str().unwrap()).await.unwrap();
        assert!(
            catalog
                .view(Some(&session.id), None)
                .await
                .unwrap()
                .resolve("ancestor")
                .is_none()
        );
        std::fs::remove_file(a.0.join(".zkcode")).unwrap();
        std::fs::create_dir_all(a.0.join(".zkcode")).unwrap();
        symlink(b.0.join("plugins"), a.0.join(".zkcode/plugins")).unwrap();
        assert!(
            catalog
                .view(Some(&session.id), None)
                .await
                .unwrap()
                .resolve("plugin")
                .is_none()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn persisted_workspace_rebound_before_first_lookup_is_rejected() {
        use std::os::unix::fs::symlink;
        let a = Workspace::new();
        let b = Workspace::new();
        b.write(".zkcode/skills/escaped.md", "must never be imported");
        let db = Db::open_in_memory().unwrap();
        let session = db.create_session("m", a.0.to_str().unwrap()).await.unwrap();
        let catalog = SkillCatalog::new(Arc::new(SkillRegistry::new()), db);
        std::fs::remove_dir(&a.0).unwrap();
        symlink(&b.0, &a.0).unwrap();
        let view = catalog.view(Some(&session.id), None).await.unwrap();
        assert!(view.resolve("escaped").is_none());
        assert_eq!(
            view.state_error().as_deref(),
            Some("SKILL_SOURCE_UNAUTHORIZED")
        );
        std::fs::remove_file(&a.0).unwrap();
    }

    #[tokio::test]
    async fn persisted_workspace_file_replacement_revokes_cached_skill() {
        let workspace = Workspace::new();
        workspace.write("project/.zkcode/skills/local.md", "previously authorized");
        let project = workspace.0.join("project");
        let db = Db::open_in_memory().unwrap();
        let session = db
            .create_session("m", project.to_str().unwrap())
            .await
            .unwrap();
        let catalog = SkillCatalog::new(Arc::new(SkillRegistry::new()), db);
        let held = catalog.view(Some(&session.id), None).await.unwrap();
        assert!(held.resolve("local").is_some());
        std::fs::rename(&project, workspace.0.join("previous")).unwrap();
        std::fs::write(&project, "a file is no longer an authorized directory").unwrap();
        assert!(
            held.resolve("local").is_none(),
            "structural root replacement must revoke cached execution"
        );
        assert_eq!(
            held.state_error().as_deref(),
            Some("SKILL_SOURCE_UNAUTHORIZED")
        );
        let refreshed = catalog.view(Some(&session.id), None).await.unwrap();
        assert!(refreshed.manage_skills().is_empty());
        assert_eq!(
            refreshed.state_error().as_deref(),
            Some("SKILL_SOURCE_UNAUTHORIZED")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn persisted_workspace_symlink_loop_revokes_cached_skill() {
        let workspace = Workspace::new();
        workspace.write("project/.zkcode/skills/local.md", "previously authorized");
        let project = workspace.0.join("project");
        let db = Db::open_in_memory().unwrap();
        let session = db
            .create_session("m", project.to_str().unwrap())
            .await
            .unwrap();
        let catalog = SkillCatalog::new(Arc::new(SkillRegistry::new()), db);
        let held = catalog.view(Some(&session.id), None).await.unwrap();
        assert!(held.resolve("local").is_some());
        std::fs::rename(&project, workspace.0.join("previous")).unwrap();
        std::os::unix::fs::symlink(&project, &project).unwrap();
        assert!(
            held.resolve("local").is_none(),
            "a symlink loop is revoked source identity, not transient I/O"
        );
        assert_eq!(
            held.state_error().as_deref(),
            Some("SKILL_SOURCE_UNAUTHORIZED")
        );
        let refreshed = catalog.view(Some(&session.id), None).await.unwrap();
        assert!(refreshed.manage_skills().is_empty());
        assert_eq!(
            refreshed.state_error().as_deref(),
            Some("SKILL_SOURCE_UNAUTHORIZED")
        );
    }

    #[tokio::test]
    async fn persisted_workspace_temporarily_missing_preserves_verified_snapshot() {
        let workspace = Workspace::new();
        workspace.write("project/.zkcode/skills/local.md", "previously authorized");
        let project = workspace.0.join("project");
        let previous = workspace.0.join("previous");
        let db = Db::open_in_memory().unwrap();
        let session = db
            .create_session("m", project.to_str().unwrap())
            .await
            .unwrap();
        let catalog = SkillCatalog::new(Arc::new(SkillRegistry::new()), db);
        let held = catalog.view(Some(&session.id), None).await.unwrap();
        assert_eq!(
            held.resolve("local").unwrap().content,
            "previously authorized"
        );
        std::fs::rename(&project, &previous).unwrap();
        let unavailable = catalog.view(Some(&session.id), None).await.unwrap();
        assert_eq!(
            unavailable.resolve("local").unwrap().content,
            "previously authorized"
        );
        assert_eq!(
            unavailable.state_error().as_deref(),
            Some("SKILL_SCAN_FAILED")
        );
        std::fs::rename(&previous, &project).unwrap();
        let recovered = catalog.view(Some(&session.id), None).await.unwrap();
        assert_eq!(
            recovered.resolve("local").unwrap().content,
            "previously authorized"
        );
        assert!(recovered.state_error().is_none());
    }

    #[tokio::test]
    async fn parallel_projects_share_only_switches_and_reload_last_valid_candidates() {
        let a = Workspace::new();
        let b = Workspace::new();
        a.write(".zk/skills/local.md", "A legacy");
        a.write(
            ".zkcode/skills/local.md",
            "---\nname: LocalAlias\n---\nA current",
        );
        a.write(".zkcode/plugins/example/skills/plugin.md", "A plugin");
        b.write(".zkcode/skills/local.md", "B current");
        b.write(".zkcode/skills/b-only.md", "B private");
        let db = Db::open_in_memory().unwrap();
        let global = Arc::new(SkillRegistry::with_persisted_state(db.clone()));
        let catalog = SkillCatalog::new(global.clone(), db.clone());
        let sa = db.create_session("m", a.0.to_str().unwrap()).await.unwrap();
        let sb = db.create_session("m", b.0.to_str().unwrap()).await.unwrap();
        let (va, vb) = tokio::join!(
            catalog.view(Some(&sa.id), None),
            catalog.view(Some(&sb.id), None)
        );
        let (va, vb) = (va.unwrap(), vb.unwrap());
        assert_eq!(va.resolve("LocalAlias").unwrap().content, "A current");
        assert_eq!(vb.resolve("local").unwrap().content, "B current");
        assert!(vb.resolve("plugin").is_none());
        assert!(va.resolve("b-only").is_none());
        assert!(catalog.global_view().resolve("local").is_none());
        assert!(global.resolve("local").is_none());
        va.set_enabled("local", false).await.unwrap();
        assert!(va.resolve("LocalAlias").is_none());
        assert!(vb.resolve("local").is_none());
        assert_eq!(
            db.skill_states_at_startup().unwrap().get("local"),
            Some(&false)
        );
        a.write(".zkcode/skills/local.md", [0xff, 0xfe]);
        let failed = catalog.view(Some(&sa.id), None).await.unwrap();
        assert_eq!(failed.state_error().as_deref(), Some("SKILL_READ_FAILED"));
        assert_eq!(
            failed.resolve_including_disabled("local").unwrap().content,
            "A current"
        );
        std::fs::remove_file(a.0.join(".zkcode/skills/local.md")).unwrap();
        let recovered = catalog.view(Some(&sa.id), None).await.unwrap();
        assert!(recovered.state_error().is_none());
        assert_eq!(
            recovered
                .resolve_including_disabled("local")
                .unwrap()
                .content,
            "A legacy"
        );
        assert!(recovered.resolve("local").is_none());
        recovered.set_enabled("local", true).await.unwrap();
        assert_eq!(vb.resolve("local").unwrap().content, "B current");
        // New plugins are discovered; deleting a winning plugin removes its body.
        a.write(".zkcode/plugins/new/skills/new.md", "new plugin");
        assert!(
            catalog
                .view(Some(&sa.id), None)
                .await
                .unwrap()
                .resolve("new")
                .is_some()
        );
        std::fs::remove_dir_all(a.0.join(".zkcode/plugins/new")).unwrap();
        assert!(
            catalog
                .view(Some(&sa.id), None)
                .await
                .unwrap()
                .resolve("new")
                .is_none()
        );
    }

    #[tokio::test]
    async fn watcher_updates_active_view_without_another_request_and_preserves_disabled_state() {
        let workspace = Workspace::new();
        workspace.write(".zkcode/skills/live.md", "before");
        let db = Db::open_in_memory().unwrap();
        let global = Arc::new(SkillRegistry::with_persisted_state(db.clone()));
        let catalog = Arc::new(SkillCatalog::new(global, db.clone()));
        let session = db
            .create_session("m", workspace.0.to_str().unwrap())
            .await
            .unwrap();
        let view = catalog.view(Some(&session.id), None).await.unwrap();
        view.set_enabled("live", false).await.unwrap();
        let watcher = catalog.spawn_watcher(std::time::Duration::from_millis(10));
        workspace.write(".zkcode/skills/live.md", "after a real edit");
        let updated = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while view.resolve_including_disabled("live").unwrap().content != "after a real edit" {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        watcher.abort();
        updated.expect("cached active view receives the hot reload");
        assert!(view.resolve("live").is_none());
    }

    #[tokio::test]
    async fn model_and_run_catalog_use_authoritative_session_instead_of_caller_cwd() {
        let a = Workspace::new();
        let b = Workspace::new();
        a.write(".zkcode/skills/only.md", "A authorized body");
        b.write(".zkcode/skills/only.md", "B other body");
        let state = crate::state::AppState::for_tests();
        for skill in state.skills.manage_skills() {
            state.skills.set_enabled(&skill.name, false).await.unwrap();
        }
        let a_session = state
            .db
            .create_session("m", a.0.to_str().unwrap())
            .await
            .unwrap();
        let b_session = state
            .db
            .create_session("m", b.0.to_str().unwrap())
            .await
            .unwrap();
        state
            .db
            .start_run("skill-a-run", &a_session.id, None, Some("query"), "m")
            .await
            .unwrap();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let context = ToolContext::new(tokio_util::sync::CancellationToken::new(), tx)
            .with_session_id(&a_session.id)
            .with_run_id("skill-a-run")
            .with_working_dir(&b.0)
            .with_tool_use_id("skill-call");
        let base = state.tools();
        assert!(
            state
                .skill_catalog
                .global_view()
                .filter_tools(base.clone())
                .get("Skill")
                .is_none()
        );
        let scope = SkillScopeFactory(state.skill_catalog.clone())
            .prepare(context.clone(), base.clone())
            .await
            .unwrap();
        let tool = scope
            .registry()
            .get("Skill")
            .expect("project-only Skill remains discoverable");
        let output = tool.execute(json!({"name":"only"}), context.clone()).await;
        assert!(!output.is_error, "{}", output.content);
        assert_eq!(output.content, "A authorized body");
        let forged = context.clone().with_session_id(&b_session.id);
        assert!(tool.execute(json!({"name":"only"}), forged).await.is_error);
        state
            .skill_catalog
            .view(Some(&a_session.id), None)
            .await
            .unwrap()
            .set_enabled("only", false)
            .await
            .unwrap();
        assert!(scope.registry().get("Skill").is_none());
        assert!(tool.execute(json!({"name":"only"}), context).await.is_error);
        assert!(
            state.skills.resolve("only").is_none(),
            "project body never enters global registry"
        );
        scope.cleanup().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn revoked_skill_source_disappears_from_discovery_and_model_invocation() {
        use std::os::unix::fs::symlink;
        let a = Workspace::new();
        let b = Workspace::new();
        a.write(".zkcode/skills/only.md", "A authorized body");
        b.write(".zkcode/skills/only.md", "B private body");
        let state = crate::state::AppState::for_tests();
        for skill in state.skills.manage_skills() {
            state.skills.set_enabled(&skill.name, false).await.unwrap();
        }
        let session = state
            .db
            .create_session("m", a.0.to_str().unwrap())
            .await
            .unwrap();
        state
            .db
            .start_run("source-run", &session.id, None, Some("query"), "m")
            .await
            .unwrap();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let context = ToolContext::new(tokio_util::sync::CancellationToken::new(), tx)
            .with_session_id(&session.id)
            .with_run_id("source-run")
            .with_tool_use_id("source-call");
        let scope = SkillScopeFactory(state.skill_catalog.clone())
            .prepare(context.clone(), state.tools())
            .await
            .unwrap();
        let tool = scope.registry().get("Skill").unwrap();
        assert_eq!(
            tool.execute(json!({"name":"only"}), context.clone())
                .await
                .content,
            "A authorized body"
        );
        std::fs::rename(a.0.join(".zkcode/skills"), a.0.join("prior")).unwrap();
        symlink(b.0.join(".zkcode/skills"), a.0.join(".zkcode/skills")).unwrap();
        assert!(scope.registry().get("Skill").is_none());
        let output = tool.execute(json!({"name":"only"}), context).await;
        assert!(output.is_error);
        assert!(!output.content.contains("B private body"));
        scope.cleanup().await.unwrap();
    }
}
