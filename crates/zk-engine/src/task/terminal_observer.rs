//! Bounded, read-only post-commit projections while the execution content lease is alive.
use super::{TaskRuntime, TaskRuntimeInner};
use futures::future::BoxFuture;
use std::sync::Arc;

/// Observer of an immutable terminal result. Implementations may record derived
/// facts, but must never execute tools or mutate the original result/evidence.
/// Database projections must be idempotent for a repeated `run_id`.
pub trait RunTerminalObserver: Send + Sync {
    /// Read terminal facts and persist an independently attributed projection.
    /// An implementation should record its own bounded failure outcome.
    fn observe<'a>(&'a self, run_id: &'a str) -> BoxFuture<'a, Result<(), String>>;
}

impl TaskRuntime {
    /// Install the application's terminal projection once on the shared runtime.
    pub fn configure_terminal_observer(&self, observer: Arc<dyn RunTerminalObserver>) -> bool {
        self.inner.terminal_observer.set(observer).is_ok()
    }

    /// Project an already committed Run before releasing its content lease.
    /// A slow or failed projection cannot reopen or indefinitely block the result.
    pub async fn observe_terminal_run(&self, run_id: &str) {
        observe(&self.inner, run_id).await;
    }
}

pub(super) async fn observe(inner: &TaskRuntimeInner, run_id: &str) {
    let Some(observer) = inner.terminal_observer.get() else {
        return;
    };
    match tokio::time::timeout(std::time::Duration::from_secs(10), observer.observe(run_id)).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => tracing::error!(
            run_id,
            error_code = "TERMINAL_OBSERVER_FAILED",
            "terminal result retained; derived projection failed"
        ),
        Err(_) => tracing::error!(
            run_id,
            error_code = "TERMINAL_OBSERVER_TIMEOUT",
            "terminal result retained; derived projection exceeded its deadline"
        ),
    }
}
