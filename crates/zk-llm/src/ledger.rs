//! Provider-neutral physical LLM-call accounting contracts.
//!
//! The LLM crate owns only the call-boundary contract. Persistence remains an
//! application concern: callers attach an [`LlmCallObserver`] and durable
//! execution attribution to a [`crate::ChatRequest`]. [`crate::ProviderRegistry`]
//! invokes the observer once for every physical provider attempt, including
//! transparent retries and model fallbacks.

use std::fmt::Debug;

use futures::future::BoxFuture;
use zk_protocol::Usage;

/// Durable owner and purpose of a logical model request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LlmExecutionAttribution {
    /// Stable logical Task owning this request.
    pub task_id: String,
    /// Physical Run attempt owning this request.
    pub run_id: String,
    /// Request purpose, for example `conversation`, `subAgent`, or `summary`.
    pub kind: String,
}

impl LlmExecutionAttribution {
    /// Construct execution attribution without imposing an application enum on
    /// provider-neutral code.
    #[must_use]
    pub fn new(
        task_id: impl Into<String>,
        run_id: impl Into<String>,
        kind: impl Into<String>,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            run_id: run_id.into(),
            kind: kind.into(),
        }
    }
}

/// Start of one physical provider attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LlmCallStarted {
    /// UUID v4 generated before the attempt can be polled.
    pub call_id: String,
    /// Durable owner copied from the request.
    pub attribution: LlmExecutionAttribution,
    /// Concrete provider selected by the registry (not the registry proxy).
    pub provider: String,
    /// Concrete model selected for this attempt (including fallback models).
    pub model: String,
    /// Self-describing JSON route containing request model, purpose, and attempt.
    pub route: String,
    /// Provider-issued request ID when the transport exposes one.
    pub provider_request_id: Option<String>,
}

/// Terminal state of one physical provider attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LlmCallStatus {
    /// Provider stream completed normally.
    Completed,
    /// Establishment or stream processing failed.
    Failed,
    /// Cancellation ended the stream before completion.
    Cancelled,
}

impl LlmCallStatus {
    /// Stable lower-camel database/wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Completion of one physical provider attempt.
#[derive(Clone, Debug, PartialEq)]
pub struct LlmCallFinished {
    /// Matches [`LlmCallStarted::call_id`].
    pub call_id: String,
    /// Concrete model used by this physical attempt, for price lookup.
    pub model: String,
    /// Terminal attempt state.
    pub status: LlmCallStatus,
    /// Provider-reported usage. `None` must remain unknown, never synthetic zero.
    pub usage: Option<Usage>,
    /// Stable local error category; provider response bodies are intentionally excluded.
    pub error_code: Option<String>,
}

/// Observer for durable physical-call accounting.
///
/// Callbacks are asynchronous so a production sink can durably commit `started`
/// before the lazy provider stream is first polled. Implementations must make
/// completion idempotent by `call_id`: a failed delivery is retried with the
/// exact same immutable payload, so callback invocation count is not a business
/// event count.
pub trait LlmCallObserver: Send + Sync + Debug {
    /// Persist the physical attempt before network execution begins.
    fn call_started(&self, call: LlmCallStarted) -> BoxFuture<'static, Result<(), String>>;

    /// Persist the terminal state and any authoritative usage.
    fn call_finished(&self, call: LlmCallFinished) -> BoxFuture<'static, Result<(), String>>;
}
