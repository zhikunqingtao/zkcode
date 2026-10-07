//! Captured Git snapshots with explicit, recoverable delivery. A task completing
//! never implicitly stages, commits, merges, resets or discards a worktree.
use dashmap::DashMap;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use zk_tools::ToolContext;

/// Captured raw Git result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitCommandOutput {
    /// Exit status.
    pub status: i32,
    /// Standard output.
    pub stdout: String,
    /// Standard error.
    pub stderr: String,
}
/// Injectable Git process boundary.
pub trait GitCommandRunner: Send + Sync {
    /// Execute Git under an isolated process supervisor.
    fn run<'a>(
        &'a self,
        cwd: &'a Path,
        args: Vec<String>,
    ) -> BoxFuture<'a, Result<GitCommandOutput, String>>;
    /// Production attaches the same durable Run resource owner used by tools.
    fn run_scoped<'a>(
        &'a self,
        cwd: &'a Path,
        args: Vec<String>,
        _ctx: Option<&'a ToolContext>,
    ) -> BoxFuture<'a, Result<GitCommandOutput, String>> {
        self.run(cwd, args)
    }
}
/// Retained-anchor Git runner, including deadline and hook supervision.
pub struct SystemGitCommandRunner;
impl GitCommandRunner for SystemGitCommandRunner {
    fn run<'a>(
        &'a self,
        cwd: &'a Path,
        args: Vec<String>,
    ) -> BoxFuture<'a, Result<GitCommandOutput, String>> {
        Box::pin(async move {
            let (progress, _receiver) = tokio::sync::mpsc::unbounded_channel();
            let ctx = ToolContext::new(CancellationToken::new(), progress).with_working_dir(cwd);
            self.run_scoped(cwd, args, Some(&ctx)).await
        })
    }
    fn run_scoped<'a>(
        &'a self,
        cwd: &'a Path,
        args: Vec<String>,
        ctx: Option<&'a ToolContext>,
    ) -> BoxFuture<'a, Result<GitCommandOutput, String>> {
        Box::pin(async move {
            let ctx = ctx.ok_or_else(|| "GIT_EXECUTION_CONTEXT_REQUIRED".to_owned())?;
            let output = zk_tools::process::run_git_program(
                &args,
                cwd,
                std::time::Duration::from_mins(2),
                ctx,
            )
            .await
            .map_err(|error| error.to_string())?;
            if output.cancelled || output.timed_out || output.truncated {
                return Err(
                    "GIT_EXECUTION_INCOMPLETE: cancellation, timeout, or truncated output"
                        .to_owned(),
                );
            }
            Ok(GitCommandOutput {
                status: output.exit_code,
                stdout: output.stdout,
                stderr: output.stderr,
            })
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Entry {
    root: PathBuf,
    path: PathBuf,
    cwd: PathBuf,
    parent_cwd: PathBuf,
    target_ref: String,
    baseline: String,
    branch: String,
    owner_run: Option<String>,
    owner_session: Option<String>,
    worker_active: bool,
    phase: String,
    detail: String,
}

/// One process-shared manager; SQLite stores every registered snapshot so a
/// restart cannot make retained work disappear from the managed inventory.
pub struct WorktreeManager {
    repo_root: PathBuf,
    runner: Arc<dyn GitCommandRunner>,
    active: DashMap<PathBuf, Entry>,
    locks: DashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>,
    runtime: Option<(zk_db::Db, Arc<crate::ExecutionSupervisor>)>,
}
impl WorktreeManager {
    /// Construct with a canonical fallback directory. Actual creation resolves
    /// the calling session's repository, never this global fallback by accident.
    ///
    /// # Errors
    /// Returns an error if repository identity, ownership, cleanup, Git execution or persistence cannot be confirmed.
    pub fn for_repo(
        root: impl AsRef<Path>,
        runner: Arc<dyn GitCommandRunner>,
    ) -> Result<Self, String> {
        let repo_root = std::fs::canonicalize(root).map_err(|error| error.to_string())?;
        if !repo_root.is_dir() {
            return Err("WORKTREE_REPO_ROOT_INVALID".into());
        }
        Ok(Self {
            repo_root,
            runner,
            active: DashMap::new(),
            locks: DashMap::new(),
            runtime: None,
        })
    }
    /// Bind production SQLite persistence and physical-resource supervision.
    #[must_use]
    pub fn with_runtime(
        mut self,
        db: zk_db::Db,
        supervisor: Arc<crate::ExecutionSupervisor>,
    ) -> Self {
        self.runtime = Some((db, supervisor));
        self
    }
    /// Canonical fallback directory.
    #[must_use]
    pub fn repo_root(&self) -> &Path {
        &self.repo_root
    }
    /// Test/backwards-compatible creation entry point.
    ///
    /// # Errors
    /// Returns an error if repository identity, ownership, cleanup, Git execution or persistence cannot be confirmed.
    pub async fn create_worktree(&self, agent_id: &str) -> Result<PathBuf, String> {
        self.create_worktree_in(agent_id, &self.repo_root, None, CancellationToken::new())
            .await
    }
    /// Create a bound snapshot of committed HEAD in the actual execution repo.
    ///
    /// # Errors
    /// Returns an error if repository identity, ownership, cleanup, Git execution or persistence cannot be confirmed.
    pub async fn create_worktree_in(
        &self,
        agent_id: &str,
        cwd: &Path,
        execution: Option<&super::PersistedChildExecution>,
        cancel: CancellationToken,
    ) -> Result<PathBuf, String> {
        let scope = match (&self.runtime, execution) {
            (Some((_, supervisor)), Some(identity)) => Some(supervisor.process_context(
                &identity.task_id,
                &identity.run_id,
                &identity.session_id,
                cwd,
                cancel,
            )),
            (Some(_), None) => return Err("WORKTREE_RUN_IDENTITY_REQUIRED".into()),
            _ => None,
        };
        self.create(agent_id, cwd, scope.as_ref(), true)
            .await
            .map(|entry| entry.cwd)
    }
    async fn output(
        &self,
        cwd: &Path,
        args: &[&str],
        ctx: Option<&ToolContext>,
    ) -> Result<GitCommandOutput, String> {
        let args = args.iter().map(|item| (*item).to_owned()).collect();
        if ctx.is_some() {
            self.runner.run_scoped(cwd, args, ctx).await
        } else {
            self.runner.run(cwd, args).await
        }
    }
    async fn command(
        &self,
        cwd: &Path,
        args: &[&str],
        ctx: Option<&ToolContext>,
    ) -> Result<String, String> {
        let output = self.output(cwd, args, ctx).await?;
        ensure_success(args.first().copied().unwrap_or("git"), &output)?;
        // Git terminates records with LF. Unicode whitespace is a valid part of
        // a path or ref and must remain part of the captured identity.
        Ok(output
            .stdout
            .strip_suffix('\n')
            .unwrap_or(&output.stdout)
            .to_owned())
    }
    async fn repo(&self, cwd: &Path, ctx: Option<&ToolContext>) -> Result<PathBuf, String> {
        let root = self
            .command(cwd, &["rev-parse", "--show-toplevel"], ctx)
            .await?;
        std::fs::canonicalize(root).map_err(|error| format!("WORKTREE_REPOSITORY_INVALID: {error}"))
    }
    async fn create(
        &self,
        agent_id: &str,
        cwd: &Path,
        ctx: Option<&ToolContext>,
        worker: bool,
    ) -> Result<Entry, String> {
        self.create_named(agent_id, cwd, ctx, worker, None, None)
            .await
    }
    #[expect(
        clippy::too_many_lines,
        reason = "Creation captures and durably registers the snapshot before any Git mutation."
    )]
    async fn create_named(
        &self,
        agent_id: &str,
        cwd: &Path,
        ctx: Option<&ToolContext>,
        worker: bool,
        requested_branch: Option<&str>,
        requested_path: Option<&str>,
    ) -> Result<Entry, String> {
        if agent_id.is_empty()
            || !agent_id
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
        {
            return Err("WORKTREE_AGENT_ID_INVALID".into());
        }
        let cwd = std::fs::canonicalize(cwd).map_err(|error| error.to_string())?;
        let root = self.repo(&cwd, ctx).await?;
        let lock = self.locks.entry(root.clone()).or_default().clone();
        let _guard = lock.lock().await;
        self.ensure_settled(&root).await?;
        self.ensure_no_operation(&root, ctx).await?;
        let target_ref = self
            .command(&root, &["symbolic-ref", "--quiet", "HEAD"], ctx)
            .await?;
        let baseline = self
            .command(&root, &["rev-parse", "--verify", "HEAD^{commit}"], ctx)
            .await?;
        let suffix = uuid::Uuid::new_v4();
        let branch =
            requested_branch.map_or_else(|| format!("agent-{agent_id}-{suffix}"), str::to_owned);
        if branch.starts_with('-') || branch.trim().is_empty() {
            return Err("WORKTREE_BRANCH_INVALID".into());
        }
        self.command(&root, &["check-ref-format", "--branch", &branch], ctx)
            .await?;
        let path = if let Some(raw) = requested_path {
            if raw.starts_with('-') || raw.trim().is_empty() {
                return Err("WORKTREE_PATH_INVALID".into());
            }
            let desired = if Path::new(raw).is_absolute() {
                PathBuf::from(raw)
            } else {
                cwd.join(raw)
            };
            if desired.exists() {
                return Err("WORKTREE_PATH_EXISTS".into());
            }
            let parent = desired
                .parent()
                .ok_or("WORKTREE_PATH_INVALID")?
                .canonicalize()
                .map_err(|error| error.to_string())?;
            parent.join(desired.file_name().ok_or("WORKTREE_PATH_INVALID")?)
        } else {
            std::env::temp_dir()
                .canonicalize()
                .map_err(|error| error.to_string())?
                .join(format!(".zhikun-agent-{suffix}"))
        };
        let relative = cwd
            .strip_prefix(&root)
            .map_err(|_| "WORKTREE_CWD_OUTSIDE_REPO".to_owned())?;
        let mut entry = Entry { root, cwd:path.join(relative), path, parent_cwd:cwd.clone(), target_ref, baseline, branch,
            owner_run:ctx.and_then(ToolContext::run_id).map(str::to_owned),
            owner_session:ctx.and_then(ToolContext::session_id).map(str::to_owned),
            worker_active:worker, phase:"creating".into(), detail:"Snapshot contains committed HEAD only; staged, unstaged and untracked parent files are not copied.".into() };
        self.save(&entry).await?;
        let created = self
            .command(
                &entry.root,
                &[
                    "worktree",
                    "add",
                    "-b",
                    &entry.branch,
                    entry.path.to_string_lossy().as_ref(),
                    &entry.baseline,
                ],
                ctx,
            )
            .await;
        if let Err(error) = created {
            entry.worker_active = false;
            entry.phase = "retained".into();
            let _ = write!(entry.detail, " Creation failed: {error}");
            self.save(&entry).await?;
            return Err(format!(
                "{} Path: {} Branch: {}",
                entry.detail,
                entry.path.display(),
                entry.branch
            ));
        }
        let validation = self.validate_agent(&entry, ctx).await.and_then(|()| {
            if entry.cwd.is_dir() {
                Ok(())
            } else {
                Err("WORKTREE_SUBDIRECTORY_ABSENT".into())
            }
        });
        if let Err(error) = validation {
            entry.worker_active = false;
            entry.phase = "retained".into();
            let _ = write!(entry.detail, " {error}");
            self.save(&entry).await?;
            return Err(format!("{error}: retained {}", entry.path.display()));
        }
        entry.phase = if worker { "executing" } else { "idle" }.into();
        self.save(&entry).await?;
        Ok(entry)
    }
    /// All automatic endings preserve the snapshot with a user-visible receipt.
    pub async fn retain(&self, execution_dir: &Path, reason: &str) -> String {
        let entry = self
            .active
            .iter()
            .find(|entry| entry.cwd == execution_dir || entry.path == execution_dir)
            .map(|entry| entry.value().clone());
        let Some(mut entry) = entry else {
            return format!("Worktree retained: {}. {reason}", execution_dir.display());
        };
        entry.worker_active = false;
        entry.phase = "retained".into();
        entry.detail = format!("{} {reason}", entry.detail);
        let saved = self.save(&entry).await;
        format!(
            "Worktree retained. Path: {}\nBranch: {}\nBaseline: {}\nDelivery: not merged. Commit and merge require explicit Worktree operations. {}{}",
            entry.path.display(),
            entry.branch,
            entry.baseline,
            entry.detail,
            saved
                .err()
                .map(|error| format!(" Registry persistence failed: {error}"))
                .unwrap_or_default()
        )
    }
    /// True for uncommitted content OR commits not present in the captured target.
    ///
    /// # Errors
    /// Returns an error if repository identity, ownership, cleanup, Git execution or persistence cannot be confirmed.
    pub async fn has_changes(&self, path: &Path) -> Result<bool, String> {
        let entry = self.require_entry(path).await?;
        self.validate_agent(&entry, None).await?;
        self.validate_target(&entry, None).await?;
        if !self
            .command(
                &entry.path,
                &[
                    "status",
                    "--porcelain",
                    "--untracked-files=all",
                    "--ignore-submodules=none",
                ],
                None,
            )
            .await?
            .is_empty()
        {
            return Ok(true);
        }
        Ok(!self.is_integrated(&entry, None).await?)
    }
    /// Explicit safe cleanup; refuses dirty, undelivered, active or uncertain trees.
    ///
    /// # Errors
    /// Returns an error if repository identity, ownership, cleanup, Git execution or persistence cannot be confirmed.
    pub async fn remove_worktree(&self, path: &Path) -> Result<(), String> {
        let entry = self.require_entry(path).await?;
        self.remove(entry, None).await
    }
    async fn remove(&self, entry: Entry, ctx: Option<&ToolContext>) -> Result<(), String> {
        let lock = self.locks.entry(entry.root.clone()).or_default().clone();
        let _guard = lock.lock().await;
        self.ensure_idle(&entry, ctx).await?;
        self.ensure_settled(&entry.root).await?;
        self.validate_target(&entry, ctx).await?;
        self.validate_agent(&entry, ctx).await?;
        self.ensure_no_operation(&entry.path, ctx).await?;
        if !self
            .command(
                &entry.path,
                &[
                    "status",
                    "--porcelain",
                    "--untracked-files=all",
                    "--ignore-submodules=none",
                ],
                ctx,
            )
            .await?
            .is_empty()
            || !self.is_integrated(&entry, ctx).await?
        {
            return Err("WORKTREE_UNDELIVERED: dirty or unmerged content retained; remove never discards it".into());
        }
        self.command(
            &entry.root,
            &["worktree", "remove", entry.path.to_string_lossy().as_ref()],
            ctx,
        )
        .await?;
        // Keep the branch as recoverable evidence; deleting the checkout does not
        // authorize deleting refs, even after successful delivery.
        let mut retained = entry;
        retained.phase = "removed".into();
        retained.detail = "Checkout removed safely; branch retained.".into();
        self.save(&retained).await?;
        self.active.remove(&retained.path);
        Ok(())
    }
    async fn validate_target(
        &self,
        entry: &Entry,
        ctx: Option<&ToolContext>,
    ) -> Result<(), String> {
        if self.repo(&entry.root, ctx).await? != entry.root
            || self
                .command(&entry.root, &["symbolic-ref", "--quiet", "HEAD"], ctx)
                .await?
                != entry.target_ref
        {
            return Err("WORKTREE_TARGET_CHANGED: target checkout/branch changed".into());
        }
        let ancestry = self
            .output(
                &entry.root,
                &["merge-base", "--is-ancestor", &entry.baseline, "HEAD"],
                ctx,
            )
            .await?;
        match ancestry.status {
            0 => self.ensure_no_operation(&entry.root, ctx).await,
            1 => Err("WORKTREE_TARGET_HISTORY_CHANGED: captured baseline is no longer an ancestor of the target".into()),
            _ => Err("WORKTREE_TARGET_HISTORY_UNKNOWN: target ancestry cannot be confirmed".into()),
        }
    }
    async fn validate_agent(&self, entry: &Entry, ctx: Option<&ToolContext>) -> Result<(), String> {
        if self.repo(&entry.path, ctx).await? != entry.path
            || self
                .command(&entry.path, &["symbolic-ref", "--quiet", "HEAD"], ctx)
                .await?
                != format!("refs/heads/{}", entry.branch)
        {
            return Err("WORKTREE_IDENTITY_CHANGED".into());
        }
        let common = self
            .command(
                &entry.root,
                &["rev-parse", "--path-format=absolute", "--git-common-dir"],
                ctx,
            )
            .await?;
        let child_common = self
            .command(
                &entry.path,
                &["rev-parse", "--path-format=absolute", "--git-common-dir"],
                ctx,
            )
            .await?;
        let common = std::fs::canonicalize(common).map_err(|error| error.to_string())?;
        let child_common =
            std::fs::canonicalize(child_common).map_err(|error| error.to_string())?;
        if common != child_common {
            return Err("WORKTREE_REPOSITORY_CHANGED".into());
        }
        let output = self
            .output(
                &entry.path,
                &["merge-base", "--is-ancestor", &entry.baseline, "HEAD"],
                ctx,
            )
            .await?;
        if output.status != 0 {
            return Err("WORKTREE_BASELINE_CHANGED".into());
        }
        Ok(())
    }
    async fn ensure_no_operation(
        &self,
        root: &Path,
        ctx: Option<&ToolContext>,
    ) -> Result<(), String> {
        for marker in [
            "MERGE_HEAD",
            "CHERRY_PICK_HEAD",
            "REVERT_HEAD",
            "rebase-merge",
            "rebase-apply",
            "BISECT_START",
        ] {
            let path = self
                .command(
                    root,
                    &["rev-parse", "--path-format=absolute", "--git-path", marker],
                    ctx,
                )
                .await?;
            if Path::new(&path).exists() {
                return Err(format!("WORKTREE_GIT_OPERATION_ACTIVE: {marker}"));
            }
        }
        Ok(())
    }
    async fn is_integrated(
        &self,
        entry: &Entry,
        ctx: Option<&ToolContext>,
    ) -> Result<bool, String> {
        let tip = self
            .command(
                &entry.path,
                &["rev-parse", "--verify", "HEAD^{commit}"],
                ctx,
            )
            .await?;
        let output = self
            .output(
                &entry.root,
                &["merge-base", "--is-ancestor", &tip, "HEAD"],
                ctx,
            )
            .await?;
        match output.status {
            0 => Ok(true),
            1 => Ok(false),
            _ => Err("WORKTREE_INTEGRATION_UNKNOWN".into()),
        }
    }
    async fn ensure_idle(&self, entry: &Entry, ctx: Option<&ToolContext>) -> Result<(), String> {
        if let Some((db, _)) = &self.runtime {
            let path = entry.path.to_string_lossy().into_owned();
            let prefix = format!("{path}/");
            let current = ctx.and_then(ToolContext::run_id).unwrap_or("").to_owned();
            let occupied=db.with_reader(move|conn|Ok(conn.query_row(
                "SELECT COUNT(*) FROM sessions s JOIN run_envelopes r ON r.session_id=s.id WHERE (s.working_dir=?1 OR substr(s.working_dir,1,length(?2))=?2) AND r.id!=?3 AND (r.status NOT IN ('completed','failed','cancelled','interrupted') OR EXISTS(SELECT 1 FROM execution_resources e WHERE e.run_id=r.id AND e.status!='released'))",
                (path,prefix,current),|row|row.get::<_,i64>(0))?)).await.map_err(|error|error.to_string())?;
            if occupied > 0 {
                return Err("WORKTREE_SESSION_OCCUPIED".into());
            }
        }
        if entry.worker_active && self.runtime.is_none() {
            return Err("WORKTREE_WORKER_ACTIVE".into());
        }
        if let (Some((db, _)), Some(run_id)) = (&self.runtime, &entry.owner_run) {
            // A manual operation may reuse its creator Run; managed child Runs
            // must have a durable terminal outcome before delivery or cleanup.
            if ctx.and_then(ToolContext::run_id) != Some(run_id.as_str()) {
                let run = db
                    .find_run_by_id(run_id)
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or("WORKTREE_OWNER_UNAVAILABLE")?;
                if !matches!(
                    run.status.as_str(),
                    "completed" | "failed" | "cancelled" | "interrupted"
                ) {
                    return Err("WORKTREE_OWNER_NOT_TERMINAL".into());
                }
            }
            let run_id = run_id.clone();
            let blocked=db.with_reader(move|conn|Ok(conn.query_row(
                "SELECT COUNT(*) FROM execution_resources WHERE run_id=?1 AND status!='released'",[run_id],|row|row.get::<_,i64>(0))?)).await.map_err(|error|error.to_string())?;
            if blocked > 0 {
                return Err("WORKTREE_RESOURCES_UNCONFIRMED".into());
            }
        }
        Ok(())
    }
    async fn ensure_settled(&self, root: &Path) -> Result<(), String> {
        self.load().await?;
        if let Some((db, _)) = &self.runtime {
            let roots = self
                .active
                .iter()
                .filter(|entry| entry.root == root)
                .filter_map(|entry| entry.owner_run.clone())
                .collect::<Vec<_>>();
            for run in roots {
                let count=db.with_reader(move|conn|Ok(conn.query_row("SELECT COUNT(*) FROM execution_resources WHERE run_id=?1 AND resource_kind='processGroup' AND status='unconfirmed'",[run],|row|row.get::<_,i64>(0))?)).await.map_err(|error|error.to_string())?;
                if count > 0 {
                    return Err(
                        "WORKTREE_GIT_SCOPE_UNCONFIRMED: prior operation requires inspection"
                            .into(),
                    );
                }
            }
        }
        Ok(())
    }
    async fn schema(&self) -> Result<(), String> {
        if let Some((db, _)) = &self.runtime {
            db.with_reader(|conn| {
                conn.prepare("SELECT path,record_json FROM managed_worktrees LIMIT 0")?;
                Ok(())
            })
            .await
            .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
    async fn save(&self, entry: &Entry) -> Result<(), String> {
        if let Some((db, _)) = &self.runtime {
            self.schema().await?;
            let path = entry.path.to_string_lossy().into_owned();
            let data = serde_json::to_string(entry).map_err(|error| error.to_string())?;
            db.with_writer(move|conn|{conn.execute("INSERT INTO managed_worktrees(path,record_json) VALUES(?1,?2) ON CONFLICT(path) DO UPDATE SET record_json=excluded.record_json",(path,data))?;Ok(())}).await.map_err(|error|error.to_string())?;
        }
        self.active.insert(entry.path.clone(), entry.clone());
        Ok(())
    }
    async fn load(&self) -> Result<(), String> {
        if let Some((db, _)) = &self.runtime {
            self.schema().await?;
            let records = db
                .with_reader(|conn| {
                    let mut statement =
                        conn.prepare("SELECT record_json FROM managed_worktrees")?;
                    let records = statement
                        .query_map([], |row| row.get::<_, String>(0))?
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(records)
                })
                .await
                .map_err(|error| error.to_string())?;
            for data in records {
                let entry: Entry =
                    serde_json::from_str(&data).map_err(|error| error.to_string())?;
                if entry.phase != "removed" {
                    self.active.insert(entry.path.clone(), entry);
                }
            }
        }
        Ok(())
    }
    async fn require_entry(&self, path: &Path) -> Result<Entry, String> {
        self.load().await?;
        let canonical = std::fs::canonicalize(path).map_err(|error| error.to_string())?;
        self.active
            .get(&canonical)
            .map(|entry| entry.clone())
            .ok_or_else(|| "WORKTREE_NOT_MANAGED: refusing unknown checkout".into())
    }
    /// Count of known active/retained checkouts in this process.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active.len()
    }
}
fn ensure_success(operation: &str, output: &GitCommandOutput) -> Result<(), String> {
    if output.status == 0 {
        Ok(())
    } else {
        Err(format!(
            "git {operation} failed ({}): {}{}",
            output.status, output.stderr, output.stdout
        ))
    }
}

impl WorktreeManager {
    /// Persist the final delivery receipt as part of the child's real transcript.
    ///
    /// # Errors
    /// Returns an error if repository identity, ownership, cleanup, Git execution or persistence cannot be confirmed.
    pub async fn record_delivery_receipt(
        &self,
        identity: &super::PersistedChildExecution,
        text: &str,
    ) -> Result<(), String> {
        if let Some((db, _)) = &self.runtime {
            db.append_attributed_message(
                &identity.session_id,
                zk_db::NewMessage {
                    role: zk_db::MessageRole::Assistant,
                    content: vec![zk_db::StoredBlock::Text {
                        text: text.to_owned(),
                    }],
                    stop_reason: Some("end_turn".into()),
                    input_tokens: 0,
                    output_tokens: 0,
                    meta: Some(serde_json::json!({"worktreeDelivery":true})),
                },
                zk_db::MessageAttribution {
                    task_id: Some(identity.task_id.clone()),
                    run_id: Some(identity.run_id.clone()),
                    origin: "runtime".into(),
                    source_task_id: None,
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
    #[expect(
        clippy::too_many_lines,
        reason = "Explicit delivery gates and retained failure states stay adjacent to their corresponding Git mutations."
    )]
    async fn explicit_operation(
        &self,
        input: serde_json::Value,
        ctx: ToolContext,
    ) -> Result<String, String> {
        let operation = input["subcommand"]
            .as_str()
            .ok_or("WORKTREE_SUBCOMMAND_REQUIRED")?;
        let repo = self.repo(ctx.working_dir(), Some(&ctx)).await?;
        if let Some(override_path) = input["repo_path"].as_str() {
            let requested = if Path::new(override_path).is_absolute() {
                PathBuf::from(override_path)
            } else {
                ctx.working_dir().join(override_path)
            };
            if self.repo(&requested, Some(&ctx)).await? != repo {
                return Err("WORKTREE_REPO_OVERRIDE_OUTSIDE_SESSION".into());
            }
        }
        if operation == "list" {
            self.load().await?;
            let listing = self
                .command(&repo, &["worktree", "list", "--porcelain"], Some(&ctx))
                .await?;
            let entries = self
                .active
                .iter()
                .filter(|entry| entry.root == repo)
                .map(|entry| {
                    format!(
                        "{} | {} | {} | {}",
                        entry.path.display(),
                        entry.branch,
                        entry.phase,
                        entry.detail
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            return Ok(format!(
                "Git Worktrees:\n{listing}\nManaged snapshots:\n{entries}"
            ));
        }
        if operation == "add" {
            let id = input["agent_id"].as_str().unwrap_or("manual");
            let entry = self
                .create_named(
                    id,
                    ctx.working_dir(),
                    Some(&ctx),
                    false,
                    input["branch"].as_str(),
                    input["path"].as_str(),
                )
                .await?;
            return Ok(format!(
                "Worktree created.\nPath: {}\nWorking directory: {}\nBranch: {}\n{}",
                entry.path.display(),
                entry.cwd.display(),
                entry.branch,
                entry.detail
            ));
        }
        let path = input["path"].as_str().ok_or("WORKTREE_PATH_REQUIRED")?;
        let path = if Path::new(path).is_absolute() {
            PathBuf::from(path)
        } else {
            ctx.working_dir().join(path)
        };
        let mut entry = self.require_entry(&path).await?;
        if entry.root != repo {
            return Err("WORKTREE_PROJECT_MISMATCH".into());
        }
        if operation == "inspect" {
            self.validate_agent(&entry, Some(&ctx)).await?;
            let status = self
                .command(
                    &entry.path,
                    &[
                        "status",
                        "--short",
                        "--branch",
                        "--untracked-files=all",
                        "--ignore-submodules=none",
                    ],
                    Some(&ctx),
                )
                .await?;
            let pending = !self.is_integrated(&entry, Some(&ctx)).await?;
            return Ok(format!(
                "Path: {}\nBranch: {}\nPhase: {}\nUnmerged commits: {pending}\n{}\n{status}",
                entry.path.display(),
                entry.branch,
                entry.phase,
                entry.detail
            ));
        }
        if operation == "remove" {
            self.remove(entry, Some(&ctx)).await?;
            return Ok("Checkout removed safely. Its branch remains available.".into());
        }
        if !matches!(operation, "commit" | "merge") {
            return Err("WORKTREE_SUBCOMMAND_INVALID".into());
        }
        let lock = self.locks.entry(repo.clone()).or_default().clone();
        let _guard = lock.lock().await;
        self.ensure_idle(&entry, Some(&ctx)).await?;
        self.ensure_settled(&repo).await?;
        self.validate_target(&entry, Some(&ctx)).await?;
        self.validate_agent(&entry, Some(&ctx)).await?;
        self.ensure_no_operation(&entry.path, Some(&ctx)).await?;
        if operation == "commit" {
            let message = input["commit_message"]
                .as_str()
                .filter(|text| !text.trim().is_empty())
                .ok_or("WORKTREE_COMMIT_MESSAGE_REQUIRED")?;
            if message.len() > 4096 {
                return Err("WORKTREE_COMMIT_MESSAGE_TOO_LONG".into());
            }
            self.command(&entry.path, &["add", "-A"], Some(&ctx))
                .await?;
            self.command(&entry.path, &["commit", "-m", message], Some(&ctx))
                .await?;
            self.validate_agent(&entry, Some(&ctx)).await?;
            entry.phase = "committed".into();
            entry.detail = "Explicit commit completed; parent has not been merged.".into();
            self.save(&entry).await?;
            return Ok(format!(
                "{}\nPath: {}\nBranch: {}",
                entry.detail,
                entry.path.display(),
                entry.branch
            ));
        }
        if !self
            .command(
                &entry.path,
                &[
                    "status",
                    "--porcelain",
                    "--untracked-files=all",
                    "--ignore-submodules=none",
                ],
                Some(&ctx),
            )
            .await?
            .is_empty()
        {
            return Err("WORKTREE_UNCOMMITTED: commit explicitly before merging".into());
        }
        if !self
            .command(
                &entry.root,
                &[
                    "status",
                    "--porcelain",
                    "--untracked-files=all",
                    "--ignore-submodules=none",
                ],
                Some(&ctx),
            )
            .await?
            .is_empty()
        {
            return Err("WORKTREE_TARGET_DIRTY: parent changes retained; merge refused".into());
        }
        let target = self
            .command(&entry.root, &["rev-parse", "HEAD"], Some(&ctx))
            .await?;
        let tip = self
            .command(&entry.path, &["rev-parse", "HEAD"], Some(&ctx))
            .await?;
        self.validate_target(&entry, Some(&ctx)).await?;
        if self
            .command(&entry.root, &["rev-parse", "HEAD"], Some(&ctx))
            .await?
            != target
        {
            return Err("WORKTREE_TARGET_CHANGED".into());
        }
        entry.phase = "merging".into();
        entry.detail = "Explicit merge started; inspect target after any uncertain exit.".into();
        self.save(&entry).await?;
        if let Some(session) = ctx.session_id() {
            zk_tools::file_state::global().remove_session(session);
        }
        let merge_result = self
            .command(
                &entry.root,
                &[
                    "merge",
                    "--ff",
                    "--commit",
                    "--no-squash",
                    "--no-edit",
                    "--no-stat",
                    &tip,
                ],
                Some(&ctx),
            )
            .await;
        // Revoke any evidence observed during merge even when Git reports an
        // error or unknown hook completion. CAS still guards later disk changes.
        if let Some(session) = ctx.session_id() {
            zk_tools::file_state::global().remove_session(session);
        }
        if let Err(error) = merge_result {
            entry.phase = "retained".into();
            entry.detail = format!(
                "Merge failed; target may contain a merge in progress. No reset or abort was attempted. {error}"
            );
            self.save(&entry).await?;
            return Err(entry.detail);
        }
        self.validate_target(&entry, Some(&ctx)).await?;
        if !self.is_integrated(&entry, Some(&ctx)).await?
            || !self
                .command(
                    &entry.root,
                    &[
                        "status",
                        "--porcelain",
                        "--untracked-files=all",
                        "--ignore-submodules=none",
                    ],
                    Some(&ctx),
                )
                .await?
                .is_empty()
        {
            return Err("WORKTREE_MERGE_UNCONFIRMED: retain both workspaces for inspection".into());
        }
        entry.phase = "delivered".into();
        entry.detail = "Explicit merge confirmed; checkout and branch retained for review.".into();
        self.save(&entry).await?;
        Ok(entry.detail)
    }
}
impl zk_tools::worktree::WorktreeBackend for WorktreeManager {
    fn execute(
        &self,
        input: serde_json::Value,
        ctx: ToolContext,
    ) -> BoxFuture<'_, zk_tools::ToolOutput> {
        Box::pin(async move {
            match self.explicit_operation(input, ctx).await {
                Ok(text) => zk_tools::ToolOutput::ok(text),
                Err(error) => zk_tools::ToolOutput::error(error),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn repo() -> PathBuf {
        let path = std::env::temp_dir().join(format!("zk-worktree-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(path.join("nested")).unwrap();
        for args in [
            vec!["init", "-q"],
            vec!["config", "user.name", "Test"],
            vec!["config", "user.email", "test@example.invalid"],
        ] {
            git(&path, &args);
        }
        std::fs::write(path.join("nested/file.txt"), "base").unwrap();
        git(&path, &["add", "."]);
        git(&path, &["commit", "-qm", "base"]);
        path.canonicalize().unwrap()
    }
    fn git(path: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .current_dir(path)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().into()
    }
    fn ctx(path: &Path) -> ToolContext {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx).with_working_dir(path)
    }
    #[tokio::test]
    async fn captures_committed_snapshot_and_relative_directory_of_actual_repository() {
        let fallback = repo();
        let actual = repo();
        std::fs::write(actual.join("nested/file.txt"), "parent dirty").unwrap();
        let manager =
            WorktreeManager::for_repo(fallback, Arc::new(SystemGitCommandRunner)).unwrap();
        let cwd = manager
            .create_worktree_in(
                "child",
                &actual.join("nested"),
                None,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(cwd.join("file.txt")).unwrap(),
            "base"
        );
        assert!(cwd.ends_with("nested"));
        let receipt = manager.retain(&cwd, "done").await;
        assert!(receipt.contains("not merged"));
        assert_eq!(
            std::fs::read_to_string(actual.join("nested/file.txt")).unwrap(),
            "parent dirty"
        );
    }
    #[tokio::test]
    async fn clean_but_unmerged_commits_are_never_deleted_or_merged_implicitly() {
        let root = repo();
        let manager = WorktreeManager::for_repo(&root, Arc::new(SystemGitCommandRunner)).unwrap();
        let path = manager.create_worktree("child").await.unwrap();
        std::fs::write(path.join("new.txt"), "child").unwrap();
        git(&path, &["add", "."]);
        git(&path, &["commit", "-qm", "child"]);
        manager.retain(&path, "done").await;
        assert!(manager.has_changes(&path).await.unwrap());
        assert!(
            manager
                .remove_worktree(&path)
                .await
                .unwrap_err()
                .contains("UNDELIVERED")
        );
        assert!(path.exists());
        assert!(!root.join("new.txt").exists());
    }
    #[tokio::test]
    async fn explicit_commit_merge_and_remove_are_separate_and_preserve_branch() {
        let root = repo();
        let manager = WorktreeManager::for_repo(&root, Arc::new(SystemGitCommandRunner)).unwrap();
        let path = manager.create_worktree("child").await.unwrap();
        let branch = git(&path, &["branch", "--show-current"]);
        std::fs::write(path.join("new.txt"), "child").unwrap();
        manager.retain(&path, "done").await;
        assert!(
            manager
                .explicit_operation(
                    serde_json::json!({"subcommand":"merge","path":path}),
                    ctx(&root)
                )
                .await
                .unwrap_err()
                .contains("UNCOMMITTED")
        );
        manager
            .explicit_operation(
                serde_json::json!({"subcommand":"commit","path":path,"commit_message":"deliver"}),
                ctx(&root),
            )
            .await
            .unwrap();
        assert!(!root.join("new.txt").exists());
        std::fs::write(root.join("parent.txt"), "unrelated").unwrap();
        assert!(
            manager
                .explicit_operation(
                    serde_json::json!({"subcommand":"merge","path":path}),
                    ctx(&root)
                )
                .await
                .unwrap_err()
                .contains("TARGET_DIRTY")
        );
        std::fs::remove_file(root.join("parent.txt")).unwrap();
        let session = uuid::Uuid::new_v4().to_string();
        let observed = root.join("nested/file.txt").to_string_lossy().into_owned();
        zk_tools::file_state::global().mark_read(&session, &observed, "base", None, None, false);
        manager
            .explicit_operation(
                serde_json::json!({"subcommand":"merge","path":path}),
                ctx(&root).with_session_id(&session),
            )
            .await
            .unwrap();
        assert!(path.exists());
        assert!(
            zk_tools::file_state::global()
                .read_hash(&session, &observed)
                .is_none()
        );
        assert_eq!(
            std::fs::read_to_string(root.join("new.txt")).unwrap(),
            "child"
        );
        manager
            .explicit_operation(
                serde_json::json!({"subcommand":"remove","path":path}),
                ctx(&root),
            )
            .await
            .unwrap();
        assert!(!path.exists());
        assert!(!git(&root, &["rev-parse", &branch]).is_empty());
    }
    #[tokio::test]
    async fn explicit_merge_conflict_preserves_target_and_child_for_manual_resolution() {
        let root = repo();
        let manager = WorktreeManager::for_repo(&root, Arc::new(SystemGitCommandRunner)).unwrap();
        let path = manager.create_worktree("child").await.unwrap();
        std::fs::write(path.join("nested/file.txt"), "child").unwrap();
        git(&path, &["add", "."]);
        git(&path, &["commit", "-qm", "child"]);
        manager.retain(&path, "done").await;
        std::fs::write(root.join("nested/file.txt"), "parent").unwrap();
        git(&root, &["add", "."]);
        git(&root, &["commit", "-qm", "parent"]);
        let error = manager
            .explicit_operation(
                serde_json::json!({"subcommand":"merge","path":path}),
                ctx(&root),
            )
            .await
            .unwrap_err();
        assert!(error.contains("No reset or abort"));
        assert!(root.join(".git/MERGE_HEAD").exists());
        assert!(path.exists());
    }
    #[tokio::test]
    async fn sqlite_inventory_survives_database_and_manager_restart() {
        let root = repo();
        let database = root
            .parent()
            .unwrap()
            .join(format!("zk-worktree-state-{}.db", uuid::Uuid::new_v4()));
        let db = zk_db::Db::open(&database).unwrap();
        let manager = WorktreeManager::for_repo(&root, Arc::new(SystemGitCommandRunner))
            .unwrap()
            .with_runtime(
                db.clone(),
                Arc::new(crate::ExecutionSupervisor::new(db.clone())),
            );
        let created = manager.create("manual", &root, None, false).await.unwrap();
        drop(manager);
        drop(db);
        let db = zk_db::Db::open(&database).unwrap();
        let manager = WorktreeManager::for_repo(&root, Arc::new(SystemGitCommandRunner))
            .unwrap()
            .with_runtime(db.clone(), Arc::new(crate::ExecutionSupervisor::new(db)));
        let restored = manager.require_entry(&created.path).await.unwrap();
        assert_eq!(restored.baseline, created.baseline);
        assert_eq!(restored.target_ref, created.target_ref);
        assert_eq!(manager.active_count(), 1);
        manager.remove_worktree(&created.path).await.unwrap();
    }
    #[tokio::test]
    async fn active_worker_and_switched_target_are_fail_closed() {
        let root = repo();
        let manager = WorktreeManager::for_repo(&root, Arc::new(SystemGitCommandRunner)).unwrap();
        let path = manager.create_worktree("child").await.unwrap();
        assert!(
            manager
                .remove_worktree(&path)
                .await
                .unwrap_err()
                .contains("WORKER_ACTIVE")
        );
        manager.retain(&path, "done").await;
        git(&root, &["checkout", "-qb", "other"]);
        assert!(
            manager
                .remove_worktree(&path)
                .await
                .unwrap_err()
                .contains("TARGET_CHANGED")
        );
        assert!(path.exists());
    }

    #[tokio::test]
    async fn unicode_ref_suffix_remains_part_of_captured_target_identity() {
        let root = repo();
        git(&root, &["checkout", "-qb", "topic\u{3000}"]);
        let manager = WorktreeManager::for_repo(&root, Arc::new(SystemGitCommandRunner)).unwrap();
        let path = manager.create_worktree("unicode-ref").await.unwrap();
        manager.retain(&path, "done").await;
        assert!(!manager.has_changes(&path).await.unwrap());
        git(&root, &["checkout", "-qb", "topic"]);
        assert!(
            manager
                .has_changes(&path)
                .await
                .unwrap_err()
                .contains("TARGET_CHANGED")
        );
        assert!(
            manager
                .remove_worktree(&path)
                .await
                .unwrap_err()
                .contains("TARGET_CHANGED")
        );
        assert!(path.exists());
    }

    #[tokio::test]
    async fn rewritten_target_history_blocks_delivery_without_mutating_either_tree() {
        let root = repo();
        std::fs::write(root.join("new-baseline.txt"), "baseline").unwrap();
        git(&root, &["add", "."]);
        git(&root, &["commit", "-qm", "new baseline"]);
        let manager = WorktreeManager::for_repo(&root, Arc::new(SystemGitCommandRunner)).unwrap();
        let path = manager.create_worktree("rewritten-target").await.unwrap();
        manager.retain(&path, "done").await;
        git(&root, &["reset", "--hard", "HEAD~1"]);
        let before = git(&root, &["rev-parse", "HEAD"]);
        let error = manager
            .explicit_operation(
                serde_json::json!({"subcommand":"merge","path":path}),
                ctx(&root),
            )
            .await
            .unwrap_err();
        assert!(error.contains("TARGET_HISTORY_CHANGED"), "{error}");
        assert_eq!(git(&root, &["rev-parse", "HEAD"]), before);
        assert!(!root.join("new-baseline.txt").exists());
        assert_eq!(
            std::fs::read_to_string(path.join("new-baseline.txt")).unwrap(),
            "baseline"
        );
        assert!(
            manager
                .remove_worktree(&path)
                .await
                .unwrap_err()
                .contains("TARGET_HISTORY_CHANGED")
        );
    }

    #[tokio::test]
    async fn dirty_submodules_cannot_be_hidden_by_user_git_configuration() {
        let root = repo();
        let submodule = repo();
        git(
            &root,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                submodule.to_str().unwrap(),
                "module",
            ],
        );
        git(&root, &["commit", "-qam", "add submodule"]);
        git(&root, &["config", "diff.ignoreSubmodules", "all"]);
        git(&root, &["config", "submodule.module.ignore", "all"]);
        let manager = WorktreeManager::for_repo(&root, Arc::new(SystemGitCommandRunner)).unwrap();
        let path = manager.create_worktree("dirty-submodule").await.unwrap();
        git(
            &path,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "update",
                "--init",
            ],
        );
        std::fs::write(path.join("module/nested/file.txt"), "retained change").unwrap();
        std::fs::write(path.join("module/untracked.txt"), "retained untracked").unwrap();
        assert!(git(&path, &["status", "--porcelain"]).is_empty());
        manager.retain(&path, "done").await;
        assert!(manager.has_changes(&path).await.unwrap());
        let error = manager.remove_worktree(&path).await.unwrap_err();
        assert!(error.contains("UNDELIVERED"), "{error}");
        assert_eq!(
            std::fs::read_to_string(path.join("module/nested/file.txt")).unwrap(),
            "retained change"
        );
        assert!(path.join("module/untracked.txt").exists());
    }
}
