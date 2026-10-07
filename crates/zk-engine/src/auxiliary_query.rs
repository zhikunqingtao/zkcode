//! Bounded, tool-free auxiliary requests using the ordinary physical-call ledger.

use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use zk_db::{Db, TaskBudgetLimits};
use zk_llm::{
    ChatMessage, ChatProvider, ChatRequest, LlmExecutionAttribution, ProviderEvent, ThinkingMode,
};

use crate::{DbSummaryObserverFactory, SummaryObserverFactory};

/// Content-free diagnostic. Provider response bodies never enter logs here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuxiliaryError {
    /// Local limits or ownership could not be honored.
    #[error("AUXILIARY_INVALID_REQUEST")]
    InvalidRequest,
    /// Provider setup, stream, or accounting failed.
    #[error("AUXILIARY_PROVIDER_FAILED")]
    Provider,
    /// The caller cancelled the owning execution.
    #[error("AUXILIARY_CANCELLED")]
    Cancelled,
    /// The bounded request deadline elapsed.
    #[error("AUXILIARY_TIMEOUT")]
    Timeout,
    /// Empty, incomplete, tool-bearing, or oversized output.
    #[error("AUXILIARY_INVALID_OUTPUT")]
    InvalidOutput,
}

/// Explicit ownership required for every production auxiliary request.
pub struct AuxiliaryExecution<'a> {
    /// `SQLite` remains the usage and task-budget authority.
    pub db: &'a Db,
    /// Real Task/Run identity, never a synthetic independent budget.
    pub attribution: LlmExecutionAttribution,
    /// The same limits as the owning execution.
    pub limits: TaskBudgetLimits,
    /// Cancelling the caller cancels the provider, too.
    pub cancel: &'a CancellationToken,
}

/// A deliberately configured auxiliary model. This adds no automatic model choice.
pub struct AuxiliaryQuery {
    provider: Arc<dyn ChatProvider>,
    model: String,
}

impl AuxiliaryQuery {
    /// The composition root must bind a registered model and its existing key policy.
    #[must_use]
    pub fn new(provider: Arc<dyn ChatProvider>, model: String) -> Self {
        Self { provider, model }
    }

    /// Execute bounded plain text; structured callers must additionally validate the DTO.
    ///
    /// # Errors
    /// Returns a content-free failure and never retries at this layer.
    pub async fn query(
        &self,
        system: &str,
        input: String,
        max_tokens: u32,
        timeout: Duration,
        execution: AuxiliaryExecution<'_>,
    ) -> Result<String, AuxiliaryError> {
        if input.len().saturating_add(system.len()) > 128 * 1024
            || !(1..=4096).contains(&max_tokens)
            || execution.attribution.task_id.is_empty()
            || execution.attribution.run_id.is_empty()
        {
            return Err(AuxiliaryError::InvalidRequest);
        }
        let timeout = execution.limits.deadline_at_ms.map_or(timeout, |deadline| {
            let remaining = deadline.saturating_sub(zk_db::time::now_millis());
            timeout.min(Duration::from_millis(u64::try_from(remaining).unwrap_or(0)))
        });
        if timeout.is_zero() {
            return Err(AuxiliaryError::Timeout);
        }
        let request = ChatRequest::new(&self.model)
            .with_message(ChatMessage::user(input))
            .with_system_prompt(Some(system.to_owned()))
            .with_tools(Vec::new())
            .with_thinking(ThinkingMode::Disabled)
            .with_max_tokens(max_tokens);
        let factory = DbSummaryObserverFactory::new(execution.db.clone(), execution.limits);
        let observer = factory.observer_for(&request);
        let request = request.with_execution(execution.attribution.clone(), observer);
        let output = collect_auxiliary(
            self.provider.as_ref(),
            request,
            execution.cancel,
            timeout,
            64 * 1024,
        )
        .await?;
        execution
            .db
            .assert_llm_usage_complete(
                &execution.attribution.task_id,
                &execution.attribution.run_id,
            )
            .await
            .map_err(|_| AuxiliaryError::Provider)?;
        execution
            .db
            .assert_task_run_budget_within_limits(
                &execution.attribution.task_id,
                &execution.attribution.run_id,
            )
            .await
            .map_err(|_| AuxiliaryError::Provider)?;
        Ok(output)
    }
}

/// Common collector for summaries and auxiliary queries. It drains a normal stream
/// through trailing usage, and never accepts a partial response as success.
pub(crate) async fn collect_auxiliary(
    provider: &dyn ChatProvider,
    request: ChatRequest,
    parent_cancel: &CancellationToken,
    timeout: Duration,
    max_bytes: usize,
) -> Result<String, AuxiliaryError> {
    if parent_cancel.is_cancelled() {
        return Err(AuxiliaryError::Cancelled);
    }
    if !request.tools.is_empty() || timeout.is_zero() || max_bytes == 0 {
        return Err(AuxiliaryError::InvalidRequest);
    }
    let cancel = parent_cancel.child_token();
    let mut stream = provider
        .chat_stream(request, cancel.clone())
        .map_err(|_| AuxiliaryError::Provider)?;
    let collect = async {
        let mut output = String::new();
        let mut finished = false;
        while let Some(event) = stream.next().await {
            match event {
                ProviderEvent::TextDelta { text } => {
                    if finished || output.len().saturating_add(text.len()) > max_bytes {
                        return Err(AuxiliaryError::InvalidOutput);
                    }
                    output.push_str(&text);
                }
                ProviderEvent::Finish { finish_reason, .. } => {
                    if finished || finish_reason.as_str() != "end_turn" {
                        return Err(AuxiliaryError::InvalidOutput);
                    }
                    finished = true;
                }
                ProviderEvent::ToolUseStart { .. } | ProviderEvent::ToolInputDelta { .. } => {
                    return Err(AuxiliaryError::InvalidOutput);
                }
                ProviderEvent::Error { .. } => return Err(AuxiliaryError::Provider),
                _ => {}
            }
        }
        if finished && !output.trim().is_empty() {
            Ok(output)
        } else {
            Err(AuxiliaryError::InvalidOutput)
        }
    };
    let result = tokio::select! {
        biased;
        () = cancel.cancelled() => Err(AuxiliaryError::Cancelled),
        result = tokio::time::timeout(timeout, collect) => result.unwrap_or(Err(AuxiliaryError::Timeout)),
    };
    cancel.cancel();
    if result.is_err() {
        // Let physical-call observers settle cancellation before dropping the stream.
        let _ = tokio::time::timeout(Duration::from_millis(250), async {
            while stream.next().await.is_some() {}
        })
        .await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::{self, BoxStream};
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use zk_llm::{FinishReason, ProviderError, ProviderRegistry};

    struct Script {
        events: Mutex<Vec<ProviderEvent>>,
        calls: AtomicUsize,
    }
    impl ChatProvider for Script {
        fn provider_name(&self) -> &'static str {
            "aux-test"
        }
        fn chat_stream(
            &self,
            _: ChatRequest,
            _: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(Box::pin(stream::iter(std::mem::take(
                &mut *self.events.lock().unwrap(),
            ))))
        }
    }
    fn script(events: Vec<ProviderEvent>) -> Arc<Script> {
        Arc::new(Script {
            events: Mutex::new(events),
            calls: AtomicUsize::new(0),
        })
    }
    fn text(value: &str) -> ProviderEvent {
        ProviderEvent::TextDelta { text: value.into() }
    }
    fn finish(reason: FinishReason) -> ProviderEvent {
        ProviderEvent::Finish {
            finish_reason: reason,
            usage: None,
        }
    }

    struct WaitingProvider(Mutex<Option<CancellationToken>>);
    impl ChatProvider for WaitingProvider {
        fn provider_name(&self) -> &'static str {
            "waiting"
        }
        fn chat_stream(
            &self,
            _: ChatRequest,
            cancel: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            *self.0.lock().unwrap() = Some(cancel.clone());
            Ok(Box::pin(stream::unfold(cancel, |cancel| async move {
                cancel.cancelled().await;
                None::<(ProviderEvent, CancellationToken)>
            })))
        }
    }

    #[tokio::test]
    async fn deadline_and_owner_cancellation_cancel_the_provider() {
        let provider = WaitingProvider(Mutex::new(None));
        let cancel = CancellationToken::new();
        assert_eq!(
            collect_auxiliary(
                &provider,
                ChatRequest::new("test"),
                &cancel,
                Duration::from_millis(5),
                8
            )
            .await,
            Err(AuxiliaryError::Timeout)
        );
        assert!(provider.0.lock().unwrap().as_ref().unwrap().is_cancelled());
        let parent = cancel.clone();
        let interrupt = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            parent.cancel();
        });
        assert_eq!(
            collect_auxiliary(
                &provider,
                ChatRequest::new("test"),
                &cancel,
                Duration::from_secs(1),
                8
            )
            .await,
            Err(AuxiliaryError::Cancelled)
        );
        interrupt.await.unwrap();
        assert!(provider.0.lock().unwrap().as_ref().unwrap().is_cancelled());
    }

    #[tokio::test]
    async fn rejects_partial_truncated_tool_and_oversized_streams() {
        let cancel = CancellationToken::new();
        for events in [
            vec![text("partial")],
            vec![text("truncated"), finish(FinishReason::MaxTokens)],
            vec![
                ProviderEvent::ToolUseStart {
                    id: "x".into(),
                    name: "Bash".into(),
                },
                finish(FinishReason::EndTurn),
            ],
            vec![text("oversized"), finish(FinishReason::EndTurn)],
            vec![text("ok"), finish(FinishReason::EndTurn), text("late")],
        ] {
            assert_eq!(
                collect_auxiliary(
                    script(events).as_ref(),
                    ChatRequest::new("test"),
                    &cancel,
                    Duration::from_secs(1),
                    8
                )
                .await,
                Err(AuxiliaryError::InvalidOutput)
            );
        }
        let provider = script(vec![text("no call")]);
        cancel.cancel();
        assert_eq!(
            collect_auxiliary(
                provider.as_ref(),
                ChatRequest::new("test"),
                &cancel,
                Duration::from_secs(1),
                8
            )
            .await,
            Err(AuxiliaryError::Cancelled)
        );
        assert_eq!(provider.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn real_ledger_accounts_trailing_usage_and_denies_insufficient_budget() {
        const MODEL: &str = "gpt-5.4-mini";
        let db = Db::open_in_memory().unwrap();
        let session = db.create_session(MODEL, "/tmp/aux-ledger").await.unwrap();
        let limits = TaskBudgetLimits {
            token_limit: Some(1000),
            cost_limit_nanos_usd: Some(1_000_000_000),
            deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
        };
        db.start_root_run_with_budget("aux-run", &session.id, None, MODEL, &limits)
            .await
            .unwrap();
        let task = db.find_run_by_id("aux-run").await.unwrap().unwrap().task_id;
        let provider = script(vec![
            text("ranked"),
            finish(FinishReason::EndTurn),
            ProviderEvent::UsageUpdate {
                usage: zk_protocol::Usage {
                    input_tokens: 12,
                    output_tokens: 4,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                },
            },
        ]);
        let mut registry = ProviderRegistry::new();
        registry.register("aux-test", provider.clone(), vec![MODEL.into()]);
        let query = AuxiliaryQuery::new(Arc::new(registry), MODEL.into());
        let cancel = CancellationToken::new();
        let execution = || AuxiliaryExecution {
            db: &db,
            attribution: LlmExecutionAttribution::new(&task, "aux-run", "memory_rerank"),
            limits: limits.clone(),
            cancel: &cancel,
        };
        assert_eq!(
            query
                .query(
                    "rank",
                    "content".into(),
                    32,
                    Duration::from_secs(1),
                    execution()
                )
                .await
                .unwrap(),
            "ranked"
        );
        let row: (String, i64, i64, i64, String) = db
            .with_conn_blocking(|conn| {
                conn.query_row(
                    "SELECT status,usage_complete,input_tokens,output_tokens,route FROM llm_calls",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
                )
                .map_err(Into::into)
            })
            .unwrap();
        assert_eq!((&*row.0, row.1, row.2, row.3), ("completed", 1, 12, 4));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&row.4).unwrap()["kind"],
            "memory_rerank"
        );
        let cost: i64 = db
            .with_conn_blocking(|conn| {
                conn.query_row("SELECT cost_nanos_usd FROM llm_calls", [], |row| row.get(0))
                    .map_err(Into::into)
            })
            .unwrap();
        assert!(cost > 0);
        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
        // The root's durable remaining budget is authoritative even if a caller
        // hands the query the same initially generous limit snapshot again.
        assert!(
            query
                .query(
                    "rank",
                    "content".into(),
                    2048,
                    Duration::from_secs(1),
                    execution()
                )
                .await
                .is_err()
        );
        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    }
}
