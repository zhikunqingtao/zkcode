//! A cleanup retry may acknowledge retained facts, never manufacture them.
use super::{TaskRuntime, TaskRuntimeError};
use zk_db::CleanupStatus;

impl TaskRuntime {
    /// Refresh a terminal Run's cleanup projection after its actual owners close.
    /// Unfinished execution/reaper ownership or unresolved resource evidence remains pending.
    ///
    /// # Errors
    /// Unknown/nonterminal Runs and storage failures remain explicit.
    pub async fn retry_confirmed_cleanup(
        &self,
        run_id: &str,
    ) -> Result<CleanupStatus, TaskRuntimeError> {
        let task = self
            .inner
            .db
            .cleanup_run_task_id(run_id)
            .await
            .map_err(TaskRuntimeError::storage)?
            .ok_or_else(|| {
                TaskRuntimeError::new("RUN_NOT_FOUND", "Cleanup Run does not exist", false)
            })?;
        if self
            .inner
            .active
            .get(&task)
            .is_some_and(|active| active.run_id == run_id)
            || self
                .inner
                .cleanup_reapers
                .get(&task)
                .is_some_and(|handle| !handle.is_finished())
        {
            return Ok(CleanupStatus::Pending);
        }
        self.inner
            .db
            .retry_confirmed_run_cleanup(run_id)
            .await
            .map_err(TaskRuntimeError::storage)
    }
}
