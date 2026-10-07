//! Optional run-owned resources and tool directory overlays.
//!
//! The engine supplies a durable setup invocation context. Implementations must
//! reserve physical resources through that context before starting them, and
//! must never persist configuration secrets in task messages or diagnostics.

use std::{fmt::Debug, sync::Arc};

use futures::future::BoxFuture;

use crate::{ToolContext, ToolRegistry};

/// A secret-redacted factory, prepared once after root Run admission.
pub trait RunToolScopeFactory: Send + Sync + Debug {
    /// Prepare a directory over `base`, preserving its live visibility policy.
    /// `context` must belong to a real, already committed setup invocation.
    fn prepare(
        &self,
        context: ToolContext,
        base: Arc<ToolRegistry>,
    ) -> BoxFuture<'_, Result<Arc<dyn RunToolScope>, String>>;
}

/// Resources shared with attached children and reclaimed before Run release.
pub trait RunToolScope: Send + Sync {
    /// The isolated live directory used for model discovery and execution.
    fn registry(&self) -> Arc<ToolRegistry>;

    /// Idempotent cleanup. An unconfirmed physical or durable cleanup fails.
    fn cleanup(&self) -> BoxFuture<'_, Result<(), String>>;
}
