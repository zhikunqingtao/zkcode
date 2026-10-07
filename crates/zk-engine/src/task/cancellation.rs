//! Run-scoped safety fences and cancellation-intent reconciliation.
//!
//! A failed database write must not keep user code running. Local fences stop
//! admission and signal only owned executions; they never masquerade as durable
//! lifecycle state. Executors retain cleanup ownership and cannot commit a result
//! before the corresponding cancellation intent has been reconciled.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::time::sleep;
use zk_db::{RuntimeTaskRecord, TaskStatus as DurableTaskStatus};

use super::{
    CancelReceipt, TaskRuntime, TaskRuntimeError, TaskRuntimeInner, persist_cancelling,
    terminal_commit_retry_delay,
};

pub(super) struct CancellationFence {
    task: RuntimeTaskRecord,
    exit_reason: String,
    detail: String,
    reconciled: AtomicBool,
    reaper_started: AtomicBool,
}

pub(super) fn release_active(inner: &TaskRuntimeInner, task_id: &str) {
    if let Some((_, active)) = inner.active.remove(task_id)
        && inner
            .cancellation_fences
            .get(&active.run_id)
            .is_some_and(|fence| fence.reconciled.load(Ordering::Acquire))
    {
        inner.cancellation_fences.remove(&active.run_id);
    }
}

pub(super) fn admission_closed() -> TaskRuntimeError {
    TaskRuntimeError::new(
        "TASK_CANCELLATION_PENDING",
        "This Run has stopped admitting execution while cancellation is reconciled",
        true,
    )
}

fn install(
    inner: &Arc<TaskRuntimeInner>,
    task: &RuntimeTaskRecord,
    exit_reason: &str,
    detail: &str,
) -> Option<Arc<CancellationFence>> {
    let run_id = task.current_run_id.as_deref()?;
    let fence = inner
        .cancellation_fences
        .entry(run_id.to_owned())
        .or_insert_with(|| {
            Arc::new(CancellationFence {
                task: task.clone(),
                exit_reason: exit_reason.to_owned(),
                detail: detail.to_owned(),
                reconciled: AtomicBool::new(false),
                reaper_started: AtomicBool::new(false),
            })
        })
        .clone();
    if let Some(active) = inner.active.get(&task.id)
        && active.run_id == run_id
    {
        active.cancel.cancel();
    }
    if let Some(external) = inner.external_tasks.get(&task.id)
        && external.run_id == run_id
    {
        external.cancel.cancel();
    }
    Some(fence)
}

/// Catch registration races after the cancellation snapshot. Only attached
/// edges inherit a stop; detached descendants remain independent.
pub(super) fn inherit_and_signal(inner: &Arc<TaskRuntimeInner>, task: &RuntimeTaskRecord) {
    let own = task
        .current_run_id
        .as_deref()
        .and_then(|run| inner.cancellation_fences.get(run).map(|f| f.clone()));
    if let Some(fence) = own {
        install(inner, task, &fence.exit_reason, &fence.detail);
    } else if task.lifecycle_policy == "attached"
        && task
            .creator_run_id
            .as_deref()
            .is_some_and(|run| inner.cancellation_fences.contains_key(run))
        && let Some(fence) = install(
            inner,
            task,
            zk_db::run::EXIT_PARENT_CANCELLED,
            "attached parent stopped",
        )
    {
        start_reconciler(inner, fence);
    }
}

pub(super) async fn reconcile(
    inner: &Arc<TaskRuntimeInner>,
    task_id: &str,
    run_id: &str,
) -> Result<(), TaskRuntimeError> {
    let Some(fence) = inner.cancellation_fences.get(run_id).map(|f| f.clone()) else {
        return Ok(());
    };
    if fence.task.id != task_id {
        return Err(TaskRuntimeError::new(
            "TASK_RUN_IDENTITY_INVALID",
            "cancellation owner mismatch",
            false,
        ));
    }
    if fence.reconciled.load(Ordering::Acquire) {
        return Ok(());
    }
    for _ in 0..4 {
        let task = inner
            .db
            .find_runtime_task_by_id(task_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| {
                TaskRuntimeError::new("TASK_NOT_FOUND", "cancelled Task no longer exists", false)
            })?;
        if task.current_run_id.as_deref() != Some(run_id) {
            return Err(TaskRuntimeError::new(
                "TASK_RUN_STALE",
                "cancellation belongs to an older attempt",
                false,
            ));
        }
        if task.status.is_terminal()
            || matches!(
                task.status,
                DurableTaskStatus::Cancelling | DurableTaskStatus::NeedsAttention
            )
            || persist_cancelling(inner, &task, &fence.exit_reason, &fence.detail).await?
        {
            fence.reconciled.store(true, Ordering::Release);
            if !inner.active.contains_key(task_id) && !inner.external_tasks.contains_key(task_id) {
                inner.cancellation_fences.remove(run_id);
            }
            return Ok(());
        }
    }
    Err(TaskRuntimeError::new(
        "TASK_VERSION_CONFLICT",
        "Task changed while reconciling cancellation",
        true,
    ))
}

fn start_reconciler(inner: &Arc<TaskRuntimeInner>, fence: Arc<CancellationFence>) {
    if fence.reaper_started.swap(true, Ordering::AcqRel) {
        return;
    }
    let inner = Arc::clone(inner);
    tokio::spawn(async move {
        let Some(run_id) = fence.task.current_run_id.as_deref() else {
            return;
        };
        let mut failures = 0_u32;
        loop {
            match reconcile(&inner, &fence.task.id, run_id).await {
                Ok(()) => return,
                Err(error) => {
                    if failures == 0 || failures.is_power_of_two() {
                        tracing::error!(task_id = %fence.task.id, run_id, code = %error.code,
                            "local execution stopped; cancellation persistence remains unconfirmed");
                    }
                    // A newer Run is never touched by an old safety fence.
                    if !error.retryable {
                        return;
                    }
                    failures = failures.saturating_add(1);
                    sleep(terminal_commit_retry_delay(failures)).await;
                }
            }
        }
    });
}

pub(super) async fn stop_active(
    inner: &Arc<TaskRuntimeInner>,
    task_id: &str,
    exit_reason: &str,
    detail: &str,
) {
    let task = inner.active.get(task_id).map(|active| active.task.clone());
    if let Some(task) = task
        && let Some(fence) = install(inner, &task, exit_reason, detail)
        && let Some(run_id) = task.current_run_id.as_deref()
        && reconcile(inner, task_id, run_id).await.is_err()
    {
        start_reconciler(inner, fence);
    }
}

fn attached_subtree(
    root: &RuntimeTaskRecord,
    tasks: &[RuntimeTaskRecord],
) -> Vec<RuntimeTaskRecord> {
    let mut selected = vec![root.clone()];
    let mut known = std::collections::HashSet::from([root.id.clone()]);
    loop {
        let before = selected.len();
        for task in tasks {
            if task.session_id == root.session_id
                && task.lifecycle_policy == "attached"
                && task
                    .parent_task_id
                    .as_ref()
                    .is_some_and(|parent| known.contains(parent))
                && known.insert(task.id.clone())
            {
                selected.push(task.clone());
            }
        }
        if selected.len() == before {
            return selected;
        }
    }
}

impl TaskRuntime {
    /// Reconcile a local stop before committing a root conversation's terminal
    /// result. Failure is retryable persistence work, not successful cancellation.
    pub async fn reconcile_local_cancellation(
        &self,
        task_id: &str,
        run_id: &str,
    ) -> Result<(), TaskRuntimeError> {
        reconcile(&self.inner, task_id, run_id).await
    }

    /// Reconcile a registered Run without relying on a database read to
    /// rediscover its owner during an outage. Unknown Runs add no new authority.
    pub async fn reconcile_run_cancellation(&self, run_id: &str) -> Result<(), TaskRuntimeError> {
        let task_id = self
            .inner
            .cancellation_fences
            .get(run_id)
            .map(|fence| fence.task.id.clone());
        if let Some(task_id) = task_id {
            reconcile(&self.inner, &task_id, run_id).await?;
        }
        Ok(())
    }

    pub(super) async fn cancel_scoped(
        &self,
        root_session_id: &str,
        task_id: &str,
        exit_reason: &str,
        reason: &str,
        cascade: bool,
        expected_run_id: Option<&str>,
    ) -> Result<CancelReceipt, TaskRuntimeError> {
        // These snapshots were authorized when an execution owner was attached.
        // They are usable for safety signalling during a read outage, never for
        // answering lifecycle queries or authorizing an unrelated caller.
        let local = self
            .inner
            .active
            .get(task_id)
            .filter(|a| a.task.session_id == root_session_id)
            .map(|a| a.task.clone());
        let newly_requested_locally = local.as_ref().is_some_and(|task| {
            !task.status.is_terminal()
                && task
                    .current_run_id
                    .as_deref()
                    .is_some_and(|run| !self.inner.cancellation_fences.contains_key(run))
        });
        if let Some(local) = &local
            && expected_run_id.is_none_or(|run| local.current_run_id.as_deref() == Some(run))
            && (exit_reason != zk_db::run::EXIT_PARENT_CANCELLED
                || local.lifecycle_policy == "attached")
        {
            let targets = if cascade {
                attached_subtree(
                    local,
                    &self
                        .inner
                        .active
                        .iter()
                        .map(|a| a.task.clone())
                        .collect::<Vec<_>>(),
                )
            } else {
                vec![local.clone()]
            };
            for target in targets {
                install(
                    &self.inner,
                    &target,
                    if target.id == task_id {
                        exit_reason
                    } else {
                        zk_db::run::EXIT_PARENT_CANCELLED
                    },
                    reason,
                );
            }
        }
        let mut first_error = None;
        let task = match self.get_owned(root_session_id, task_id).await {
            Ok(Some(task)) => task,
            Ok(None) => {
                return Err(TaskRuntimeError::new(
                    "TASK_NOT_FOUND",
                    "Task not found in this session",
                    false,
                ));
            }
            Err(error) => {
                let Some(task) = local else {
                    return Err(error);
                };
                first_error = Some(error);
                task
            }
        };
        if expected_run_id.is_some_and(|run| task.current_run_id.as_deref() != Some(run)) {
            return Err(TaskRuntimeError::new(
                "TASK_RUN_STALE",
                "Cancellation belongs to an older Run",
                false,
            ));
        }
        if exit_reason == zk_db::run::EXIT_PARENT_CANCELLED && task.lifecycle_policy != "attached" {
            return Ok(CancelReceipt {
                cancel_requested: false,
                task,
            });
        }
        let requested = newly_requested_locally
            || (!task.status.is_terminal() && task.status != DurableTaskStatus::Cancelling);
        let local_tasks = self
            .inner
            .active
            .iter()
            .map(|a| a.task.clone())
            .collect::<Vec<_>>();
        let mut targets = if cascade {
            attached_subtree(&task, &local_tasks)
        } else {
            vec![task.clone()]
        };
        // Close admission and signal all known owned tokens before the first write.
        for target in &targets {
            install(
                &self.inner,
                target,
                if target.id == task.id {
                    exit_reason
                } else {
                    zk_db::run::EXIT_PARENT_CANCELLED
                },
                reason,
            );
        }
        if cascade {
            match self.list_owned(root_session_id, None).await {
                Ok(tasks) => targets = attached_subtree(&task, &tasks),
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        for target in targets {
            let Some(fence) = install(
                &self.inner,
                &target,
                if target.id == task.id {
                    exit_reason
                } else {
                    zk_db::run::EXIT_PARENT_CANCELLED
                },
                reason,
            ) else {
                continue;
            };
            if let Some(run_id) = target.current_run_id.as_deref()
                && let Err(error) = reconcile(&self.inner, &target.id, run_id).await
            {
                start_reconciler(&self.inner, fence);
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            return Err(TaskRuntimeError::new(
                "TASK_CANCELLATION_PERSISTENCE_PENDING",
                format!(
                    "Local stop requested for the owned Run and attached subtree; durable cancellation is unconfirmed: {error}"
                ),
                true,
            ));
        }
        let current = self
            .get_owned(root_session_id, task_id)
            .await?
            .ok_or_else(|| {
                TaskRuntimeError::new("TASK_NOT_FOUND", "Task disappeared after local stop", false)
            })?;
        Ok(CancelReceipt {
            cancel_requested: requested && current.status != DurableTaskStatus::NeedsAttention,
            task: current,
        })
    }
}
