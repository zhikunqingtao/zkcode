//! Git Worktree manager with an explicit canonical repository root and an
//! injectable command port. Production can use [`SystemGitCommandRunner`];
//! tests use a recorder and never execute Git.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use futures::future::BoxFuture;
use tokio::process::Command;
use tracing::warn;

const WORKTREE_PREFIX: &str = ".zhikun-agent-";

/// Captured command result independent of `std::process::Output`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitCommandOutput {
    /// Numeric exit status (`-1` when unavailable).
    pub status: i32,
    /// Standard output decoded lossily as UTF-8.
    pub stdout: String,
    /// Standard error decoded lossily as UTF-8.
    pub stderr: String,
}

/// Port used for all Git command execution.
pub trait GitCommandRunner: Send + Sync {
    /// Execute `git <args>` in the exact supplied canonical directory.
    fn run<'a>(
        &'a self,
        cwd: &'a Path,
        args: Vec<String>,
    ) -> BoxFuture<'a, Result<GitCommandOutput, String>>;
}

/// Real command runner. It is never used by the normal test suite.
pub struct SystemGitCommandRunner;

impl GitCommandRunner for SystemGitCommandRunner {
    fn run<'a>(
        &'a self,
        cwd: &'a Path,
        args: Vec<String>,
    ) -> BoxFuture<'a, Result<GitCommandOutput, String>> {
        Box::pin(async move {
            let output = Command::new("git")
                .current_dir(cwd)
                .args(&args)
                .output()
                .await
                .map_err(|error| format!("failed to spawn git {args:?}: {error}"))?;
            Ok(GitCommandOutput {
                status: output.status.code().unwrap_or(-1),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            })
        })
    }
}

/// Worktree manager bound to one canonical repository root.
pub struct WorktreeManager {
    repo_root: PathBuf,
    runner: Arc<dyn GitCommandRunner>,
    active: DashMap<PathBuf, String>,
}

impl WorktreeManager {
    /// Bind a manager to an existing canonical repository root.
    ///
    /// # Errors
    /// Returns an error when the configured root cannot be canonicalized to a directory.
    pub fn for_repo(
        repo_root: impl AsRef<Path>,
        runner: Arc<dyn GitCommandRunner>,
    ) -> Result<Self, String> {
        let repo_root = std::fs::canonicalize(repo_root.as_ref())
            .map_err(|error| format!("WORKTREE_REPO_ROOT_INVALID: {error}"))?;
        if !repo_root.is_dir() {
            return Err("WORKTREE_REPO_ROOT_INVALID: root is not a directory".to_owned());
        }
        Ok(Self {
            repo_root,
            runner,
            active: DashMap::new(),
        })
    }

    /// Canonical repository root used by every command.
    #[must_use]
    pub fn repo_root(&self) -> &Path {
        &self.repo_root
    }

    /// Create an isolated worktree through the injected command port.
    ///
    /// # Errors
    /// Returns an error for an unsafe Agent ID, a command failure, or a failed canonical
    /// path recheck. This API remains disabled until a real Git gate is authorized.
    pub async fn create_worktree(&self, agent_id: &str) -> Result<PathBuf, String> {
        if agent_id.is_empty()
            || !agent_id
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
        {
            return Err("WORKTREE_AGENT_ID_INVALID".to_owned());
        }
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis());
        let branch_name = format!("agent-{agent_id}-{suffix}");
        let worktree_path =
            std::env::temp_dir().join(format!("{WORKTREE_PREFIX}{agent_id}-{suffix}"));
        let output = self
            .runner
            .run(
                &self.repo_root,
                vec![
                    "worktree".to_owned(),
                    "add".to_owned(),
                    "-b".to_owned(),
                    branch_name.clone(),
                    worktree_path.to_string_lossy().into_owned(),
                    "HEAD".to_owned(),
                ],
            )
            .await?;
        ensure_success("worktree add", &output)?;
        let canonical = std::fs::canonicalize(&worktree_path)
            .map_err(|error| format!("WORKTREE_CREATE_TOCTOU_CHECK_FAILED: {error}"))?;
        self.active.insert(canonical.clone(), branch_name);
        Ok(canonical)
    }

    /// Check changes; failures are never interpreted as a clean tree.
    ///
    /// # Errors
    /// Returns an error when the path is not an active canonical worktree or the command fails.
    pub async fn has_changes(&self, worktree_path: &Path) -> Result<bool, String> {
        let canonical = self.validate_active(worktree_path)?;
        let output = self
            .runner
            .run(
                &canonical,
                vec!["status".to_owned(), "--porcelain".to_owned()],
            )
            .await?;
        ensure_success("status --porcelain", &output)?;
        Ok(!output.stdout.trim().is_empty())
    }

    /// Commit isolated changes and merge the recorded branch into the root.
    ///
    /// # Errors
    /// Returns an error when validation or any command fails; no failure is treated as success.
    pub async fn merge_back(&self, worktree_path: &Path) -> Result<(), String> {
        let canonical = self.validate_active(worktree_path)?;
        let branch_name = self
            .active
            .get(&canonical)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| "WORKTREE_NOT_ACTIVE".to_owned())?;
        self.exec(&canonical, vec!["add".to_owned(), "-A".to_owned()])
            .await?;
        self.exec(
            &canonical,
            vec![
                "commit".to_owned(),
                "-m".to_owned(),
                format!("Agent work: {branch_name}"),
            ],
        )
        .await?;
        self.exec(
            &self.repo_root,
            vec!["merge".to_owned(), branch_name, "--no-edit".to_owned()],
        )
        .await
    }

    /// Remove a validated active worktree and its temporary branch.
    ///
    /// # Errors
    /// Returns an error when validation or any removal/branch command fails.
    pub async fn remove_worktree(&self, worktree_path: &Path) -> Result<(), String> {
        let canonical = self.validate_active(worktree_path)?;
        let branch_name = self
            .active
            .get(&canonical)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| "WORKTREE_NOT_ACTIVE".to_owned())?;
        self.exec(
            &self.repo_root,
            vec![
                "worktree".to_owned(),
                "remove".to_owned(),
                "--force".to_owned(),
                canonical.to_string_lossy().into_owned(),
            ],
        )
        .await?;
        self.exec(
            &self.repo_root,
            vec!["branch".to_owned(), "-D".to_owned(), branch_name],
        )
        .await?;
        self.active.remove(&canonical);
        Ok(())
    }

    /// Active worktree count.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    fn validate_active(&self, path: &Path) -> Result<PathBuf, String> {
        let canonical = std::fs::canonicalize(path)
            .map_err(|error| format!("WORKTREE_PATH_INVALID: {error}"))?;
        if !self.active.contains_key(&canonical) {
            return Err("WORKTREE_NOT_ACTIVE".to_owned());
        }
        Ok(canonical)
    }

    async fn exec(&self, cwd: &Path, args: Vec<String>) -> Result<(), String> {
        let output = self.runner.run(cwd, args.clone()).await?;
        ensure_success(&format!("git {args:?}"), &output)
    }
}

fn ensure_success(operation: &str, output: &GitCommandOutput) -> Result<(), String> {
    if output.status == 0 {
        Ok(())
    } else {
        Err(format!(
            "{operation} failed (exit {}): {}{}",
            output.status, output.stderr, output.stdout
        ))
    }
}

/// Preserve the isolated tree when safety checks fail.
pub(crate) fn log_preserved_worktree(path: &Path, error: &str) {
    warn!(worktree = %path.display(), %error, "worktree preserved after fail-closed cleanup");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn test_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("zkcode-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("create test dir");
        path
    }

    #[derive(Default)]
    struct RecordingRunner {
        calls: Mutex<Vec<(PathBuf, Vec<String>)>>,
    }

    impl GitCommandRunner for RecordingRunner {
        fn run<'a>(
            &'a self,
            cwd: &'a Path,
            args: Vec<String>,
        ) -> BoxFuture<'a, Result<GitCommandOutput, String>> {
            self.calls
                .lock()
                .expect("calls")
                .push((cwd.to_owned(), args.clone()));
            Box::pin(async move {
                if args.starts_with(&["worktree".to_owned(), "add".to_owned()]) {
                    std::fs::create_dir_all(PathBuf::from(&args[4]))
                        .map_err(|error| error.to_string())?;
                }
                Ok(GitCommandOutput {
                    status: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            })
        }
    }

    #[tokio::test]
    async fn mock_runner_receives_canonical_root_and_no_real_git_is_used() {
        let root = test_dir("worktree-root");
        let runner = Arc::new(RecordingRunner::default());
        let manager = WorktreeManager::for_repo(&root, runner.clone()).expect("manager");
        let path = manager.create_worktree("agent_1").await.expect("create");
        assert!(manager.has_changes(&path).await.is_ok());
        let calls = runner.calls.lock().expect("calls");
        assert_eq!(calls[0].0, std::fs::canonicalize(&root).expect("canonical"));
        assert_eq!(&calls[0].1[0..3], ["worktree", "add", "-b"]);
    }

    #[tokio::test]
    async fn unknown_path_fails_closed_before_command_execution() {
        let root = test_dir("worktree-root");
        let foreign = test_dir("worktree-foreign");
        let runner = Arc::new(RecordingRunner::default());
        let manager = WorktreeManager::for_repo(&root, runner.clone()).expect("manager");
        assert_eq!(
            manager.has_changes(&foreign).await.unwrap_err(),
            "WORKTREE_NOT_ACTIVE"
        );
        assert!(runner.calls.lock().expect("calls").is_empty());
    }
}
