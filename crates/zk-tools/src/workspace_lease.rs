//! Process-wide workspace write leases.
//!
//! A canonical workspace root is the isolation boundary. CAS-backed file
//! writers take a shared lease so writes to different paths may proceed in
//! parallel, while every other non-read-only leaf operation takes an exclusive
//! lease. The executor owns the guard across the whole tool future, including
//! its bounded cleanup path.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};

use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};
use tokio_util::sync::CancellationToken;

/// Sweep dead weak entries before the registry can grow without bound during
/// a long-running process that visits many isolated worktrees.
const REGISTRY_SWEEP_THRESHOLD: usize = 1024;

static PROCESS_WIDE_MANAGER: OnceLock<WorkspaceLeaseManager> = OnceLock::new();

/// The write isolation requested by one leaf tool invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WorkspaceLeaseMode {
    /// A CAS-backed file write. Other CAS writers may run concurrently.
    SharedWrite,
    /// An operation with broad or unknown write effects.
    ExclusiveWrite,
}

/// Failure to establish a workspace lease before a potentially mutating tool.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WorkspaceLeaseError {
    /// The run was cancelled while canonicalizing or waiting for the lease.
    Cancelled,
    /// The workspace root could not be proven to be an existing directory.
    InvalidRoot { root: PathBuf, reason: String },
}

impl fmt::Display for WorkspaceLeaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("workspace lease acquisition cancelled"),
            Self::InvalidRoot { root, reason } => write!(
                formatter,
                "workspace root '{}' is not a canonical directory: {reason}",
                root.display()
            ),
        }
    }
}

#[derive(Default)]
struct WorkspaceLeaseRegistry {
    locks: Mutex<HashMap<PathBuf, Weak<RwLock<()>>>>,
}

/// Cloneable handle to the process-wide canonical-root lease registry.
///
/// Production executors obtain this handle from [`Self::process_wide`]. The
/// registry stores only weak references; the final guard for a root removes
/// its dead entry eagerly, with a threshold sweep as a second bound.
#[derive(Clone)]
pub(crate) struct WorkspaceLeaseManager {
    registry: Arc<WorkspaceLeaseRegistry>,
}

impl WorkspaceLeaseManager {
    /// Return the single manager shared by every production `ToolExecutor`.
    pub(crate) fn process_wide() -> Self {
        PROCESS_WIDE_MANAGER
            .get_or_init(|| Self {
                registry: Arc::new(WorkspaceLeaseRegistry::default()),
            })
            .clone()
    }

    /// Whether this handle is backed by the single production registry.
    pub(crate) fn is_process_wide(&self) -> bool {
        PROCESS_WIDE_MANAGER
            .get()
            .is_some_and(|manager| Arc::ptr_eq(&self.registry, &manager.registry))
    }

    #[cfg(test)]
    /// Build an isolated registry for deterministic concurrency tests.
    pub(crate) fn isolated() -> Self {
        Self {
            registry: Arc::new(WorkspaceLeaseRegistry::default()),
        }
    }

    /// Canonicalize `root` and acquire its requested lease.
    ///
    /// Cancellation wins at every async boundary. A missing, unreadable, or
    /// non-directory root is rejected instead of falling back to a coarser or
    /// unrelated path, because doing so could allow an uncoordinated write.
    pub(crate) async fn acquire(
        &self,
        root: &Path,
        mode: WorkspaceLeaseMode,
        cancel: &CancellationToken,
    ) -> Result<WorkspaceLeaseGuard, WorkspaceLeaseError> {
        let canonical_root = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(WorkspaceLeaseError::Cancelled),
            result = tokio::fs::canonicalize(root) => result.map_err(|error| {
                WorkspaceLeaseError::InvalidRoot {
                    root: root.to_path_buf(),
                    reason: error.to_string(),
                }
            })?,
        };
        let metadata = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(WorkspaceLeaseError::Cancelled),
            result = tokio::fs::metadata(&canonical_root) => result.map_err(|error| {
                WorkspaceLeaseError::InvalidRoot {
                    root: root.to_path_buf(),
                    reason: error.to_string(),
                }
            })?,
        };
        if !metadata.is_dir() {
            return Err(WorkspaceLeaseError::InvalidRoot {
                root: root.to_path_buf(),
                reason: "not a directory".to_owned(),
            });
        }

        let lock = self.lock_for(&canonical_root);
        let weak_lock = Arc::downgrade(&lock);
        let held = match mode {
            WorkspaceLeaseMode::SharedWrite => {
                let waiter = Arc::clone(&lock).read_owned();
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => None,
                    guard = waiter => Some(HeldWorkspaceLease::Shared { _guard: guard }),
                }
            }
            WorkspaceLeaseMode::ExclusiveWrite => {
                let waiter = Arc::clone(&lock).write_owned();
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => None,
                    guard = waiter => Some(HeldWorkspaceLease::Exclusive { _guard: guard }),
                }
            }
        };
        drop(lock);
        let Some(held) = held else {
            self.remove_if_dead(&canonical_root, &weak_lock);
            return Err(WorkspaceLeaseError::Cancelled);
        };

        Ok(WorkspaceLeaseGuard {
            canonical_root,
            held: Some(held),
            manager: self.clone(),
            weak_lock,
        })
    }

    fn lock_for(&self, canonical_root: &Path) -> Arc<RwLock<()>> {
        let mut locks = self
            .registry
            .locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if locks.len() >= REGISTRY_SWEEP_THRESHOLD {
            locks.retain(|_, lock| lock.strong_count() > 0);
        }
        if let Some(lock) = locks.get(canonical_root).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(RwLock::new(()));
        locks.insert(canonical_root.to_path_buf(), Arc::downgrade(&lock));
        lock
    }

    fn remove_if_dead(&self, canonical_root: &Path, expected: &Weak<RwLock<()>>) {
        let mut locks = self
            .registry
            .locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let is_same_dead_entry = locks
            .get(canonical_root)
            .is_some_and(|current| Weak::ptr_eq(current, expected) && current.strong_count() == 0);
        if is_same_dead_entry {
            locks.remove(canonical_root);
        }
    }

    #[cfg(test)]
    fn entry_count(&self) -> usize {
        self.registry
            .locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    #[cfg(test)]
    /// Whether two handles point at the same registry allocation.
    pub(crate) fn shares_registry(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry, &other.registry)
    }
}

enum HeldWorkspaceLease {
    Shared { _guard: OwnedRwLockReadGuard<()> },
    Exclusive { _guard: OwnedRwLockWriteGuard<()> },
}

/// RAII ownership of a workspace lease.
pub(crate) struct WorkspaceLeaseGuard {
    canonical_root: PathBuf,
    held: Option<HeldWorkspaceLease>,
    manager: WorkspaceLeaseManager,
    weak_lock: Weak<RwLock<()>>,
}

impl Drop for WorkspaceLeaseGuard {
    fn drop(&mut self) {
        // Release the RwLock before deciding whether its weak registry entry is
        // dead. Concurrent guards and queued acquirers retain their own Arcs.
        drop(self.held.take());
        self.manager
            .remove_if_dead(&self.canonical_root, &self.weak_lock);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::{Notify, oneshot};

    use super::*;

    struct TempRoot(PathBuf);

    impl TempRoot {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "zk-workspace-lease-{label}-{}",
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir_all(&path).expect("create temp workspace root");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn exclusive_blocks_shared_cas_writer() {
        let manager = WorkspaceLeaseManager::isolated();
        let root = TempRoot::new("exclusive-blocks-shared");
        let blocker = manager
            .acquire(
                root.path(),
                WorkspaceLeaseMode::ExclusiveWrite,
                &CancellationToken::new(),
            )
            .await
            .expect("exclusive lease");

        let attempted = Arc::new(Notify::new());
        let (acquired_tx, mut acquired_rx) = oneshot::channel();
        let waiter_manager = manager.clone();
        let waiter_root = root.path().to_path_buf();
        let waiter_attempted = Arc::clone(&attempted);
        let waiter = tokio::spawn(async move {
            waiter_attempted.notify_one();
            let result = waiter_manager
                .acquire(
                    &waiter_root,
                    WorkspaceLeaseMode::SharedWrite,
                    &CancellationToken::new(),
                )
                .await;
            let _ = acquired_tx.send(result);
        });
        attempted.notified().await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            acquired_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        drop(blocker);
        let acquired = acquired_rx.await.expect("waiter result");
        drop(acquired.expect("shared lease after exclusive release"));
        waiter.await.expect("waiter task");
    }

    #[tokio::test]
    async fn two_shared_cas_writers_can_hold_the_same_root() {
        let manager = WorkspaceLeaseManager::isolated();
        let root = TempRoot::new("shared-parallel");
        let first = manager
            .acquire(
                root.path(),
                WorkspaceLeaseMode::SharedWrite,
                &CancellationToken::new(),
            )
            .await
            .expect("first shared lease");
        let second = manager
            .acquire(
                root.path(),
                WorkspaceLeaseMode::SharedWrite,
                &CancellationToken::new(),
            )
            .await
            .expect("second shared lease must not block");
        drop((first, second));
    }

    #[tokio::test]
    async fn canonical_worktree_roots_are_independent_partitions() {
        let manager = WorkspaceLeaseManager::isolated();
        let first_root = TempRoot::new("worktree-a");
        let second_root = TempRoot::new("worktree-b");
        let first = manager
            .acquire(
                first_root.path(),
                WorkspaceLeaseMode::ExclusiveWrite,
                &CancellationToken::new(),
            )
            .await
            .expect("first worktree lease");
        let second = manager
            .acquire(
                second_root.path(),
                WorkspaceLeaseMode::ExclusiveWrite,
                &CancellationToken::new(),
            )
            .await
            .expect("different worktree must not block");
        drop((first, second));
    }

    #[tokio::test]
    async fn cancelling_a_waiter_does_not_leak_a_lease() {
        let manager = WorkspaceLeaseManager::isolated();
        let root = TempRoot::new("cancel-waiter");
        let blocker = manager
            .acquire(
                root.path(),
                WorkspaceLeaseMode::ExclusiveWrite,
                &CancellationToken::new(),
            )
            .await
            .expect("exclusive blocker");
        let cancel = CancellationToken::new();
        let attempted = Arc::new(Notify::new());
        let waiter_manager = manager.clone();
        let waiter_root = root.path().to_path_buf();
        let waiter_cancel = cancel.clone();
        let waiter_attempted = Arc::clone(&attempted);
        let waiter = tokio::spawn(async move {
            waiter_attempted.notify_one();
            waiter_manager
                .acquire(
                    &waiter_root,
                    WorkspaceLeaseMode::SharedWrite,
                    &waiter_cancel,
                )
                .await
        });
        attempted.notified().await;
        cancel.cancel();
        assert!(matches!(
            waiter.await.expect("waiter task"),
            Err(WorkspaceLeaseError::Cancelled)
        ));

        drop(blocker);
        let after_cancel = manager
            .acquire(
                root.path(),
                WorkspaceLeaseMode::ExclusiveWrite,
                &CancellationToken::new(),
            )
            .await
            .expect("cancelled waiter must not retain the lease");
        drop(after_cancel);
    }

    #[tokio::test]
    async fn canonicalization_failure_is_closed() {
        let manager = WorkspaceLeaseManager::isolated();
        let root = TempRoot::new("missing-root-parent");
        let missing = root.path().join("does-not-exist");
        let result = manager
            .acquire(
                &missing,
                WorkspaceLeaseMode::ExclusiveWrite,
                &CancellationToken::new(),
            )
            .await;
        assert!(matches!(
            result,
            Err(WorkspaceLeaseError::InvalidRoot { .. })
        ));
        assert_eq!(manager.entry_count(), 0);
    }

    #[tokio::test]
    async fn dead_registry_entries_are_reclaimed_on_guard_drop() {
        let manager = WorkspaceLeaseManager::isolated();
        let root = TempRoot::new("entry-reclaim");
        let lease = manager
            .acquire(
                root.path(),
                WorkspaceLeaseMode::SharedWrite,
                &CancellationToken::new(),
            )
            .await
            .expect("lease");
        assert_eq!(manager.entry_count(), 1);
        drop(lease);
        assert_eq!(manager.entry_count(), 0);
    }
}
