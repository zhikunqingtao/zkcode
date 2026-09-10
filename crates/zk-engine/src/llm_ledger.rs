//! Production adapter from provider-neutral physical LLM events to `SQLite`.

use std::sync::Arc;

use futures::future::BoxFuture;
use zk_db::{Db, LlmCallBudgetReservation, LlmUsageCompletion, NewLlmCall, TaskBudgetLimits};
use zk_llm::{
    ChatMessage, ChatRequest, LlmCallFinished, LlmCallObserver, LlmCallStarted, is_known_model,
};

use crate::llm_summarizer::SummaryObserverFactory;
use crate::query_config::usage_cost_usd;

/// Pricing must be both model-specific and non-zero. A registry entry with zero
/// rates is not sufficient evidence that a physical call is free.
#[must_use]
pub(crate) fn has_known_price(model: &str) -> bool {
    if !is_known_model(model) {
        return false;
    }
    let capabilities = zk_llm::capabilities_for(model);
    capabilities.cost_per_1k_input > 0.0 || capabilities.cost_per_1k_output > 0.0
}

/// Durable observer used by root and child engines, retries, fallbacks, and
/// summary requests.
#[derive(Clone)]
pub struct DbLlmCallObserver {
    db: Db,
    budget: Option<PhysicalCallBudget>,
}

/// Per-logical-request worst-case reservation copied into every physical retry/fallback.
#[derive(Clone, Debug)]
struct PhysicalCallBudget {
    limits: TaskBudgetLimits,
    input_tokens: i64,
    output_tokens: i64,
}

impl std::fmt::Debug for DbLlmCallObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DbLlmCallObserver")
            .finish_non_exhaustive()
    }
}

impl DbLlmCallObserver {
    /// Bind the observer to the process-wide database handle.
    #[must_use]
    pub fn new(db: Db) -> Self {
        Self { db, budget: None }
    }

    /// Erase the concrete type for request attachment.
    #[must_use]
    pub fn shared(db: Db) -> Arc<dyn LlmCallObserver> {
        Arc::new(Self::new(db))
    }

    /// Attach durable admission to each physical attempt represented by this observer.
    #[must_use]
    pub fn shared_budgeted(
        db: Db,
        limits: TaskBudgetLimits,
        input_tokens: i64,
        output_tokens: i64,
    ) -> Arc<dyn LlmCallObserver> {
        Arc::new(Self {
            db,
            budget: Some(PhysicalCallBudget {
                limits,
                input_tokens,
                output_tokens,
            }),
        })
    }
}

/// Creates a durable observer after the exact summary request is assembled, so
/// reservations use the summary prompt and output ceiling rather than the parent
/// conversation request.
#[derive(Clone)]
pub struct DbSummaryObserverFactory {
    db: Db,
    limits: TaskBudgetLimits,
}

impl std::fmt::Debug for DbSummaryObserverFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DbSummaryObserverFactory")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl DbSummaryObserverFactory {
    /// Bind the factory to one Run's durable budget limits.
    #[must_use]
    pub fn new(db: Db, limits: TaskBudgetLimits) -> Self {
        Self { db, limits }
    }
}

impl SummaryObserverFactory for DbSummaryObserverFactory {
    fn observer_for(&self, request: &ChatRequest) -> Arc<dyn LlmCallObserver> {
        if self.limits.token_limit.is_none()
            && self.limits.cost_limit_nanos_usd.is_none()
            && self.limits.deadline_at_ms.is_none()
        {
            return DbLlmCallObserver::shared(self.db.clone());
        }
        DbLlmCallObserver::shared_budgeted(
            self.db.clone(),
            self.limits.clone(),
            conservative_request_tokens(request),
            i64::from(request.max_tokens.max(1)),
        )
    }
}

/// Reserve above the model-specific estimator because provider tokenizers and
/// request-envelope accounting are not identical to the local approximation.
/// The margin is applied after replayed reasoning and tool schemas are counted.
pub(crate) fn conservative_request_tokens(request: &ChatRequest) -> i64 {
    let mut messages = Vec::with_capacity(request.messages.len().saturating_add(2));
    if let Some(system) = request.system_text() {
        messages.push(ChatMessage::system(system.into_owned()));
    }
    messages.extend(request.messages.iter().cloned());
    if !request.tools.is_empty() {
        let tools = request
            .tools
            .iter()
            .map(|tool| {
                serde_json::json!({
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters,
                })
            })
            .collect::<Vec<_>>();
        messages.push(ChatMessage::system(
            serde_json::to_string(&tools).unwrap_or_else(|_| "[]".to_owned()),
        ));
    }
    let estimated = i64::from(crate::context::estimate_tokens(&messages, &request.model));
    estimated.saturating_mul(5).saturating_add(3) / 4
}

impl LlmCallObserver for DbLlmCallObserver {
    fn call_started(&self, call: LlmCallStarted) -> BoxFuture<'static, Result<(), String>> {
        let db = self.db.clone();
        let budget = self.budget.clone();
        Box::pin(async move {
            let physical_model = call.model.clone();
            let persisted = NewLlmCall {
                call_id: call.call_id,
                task_id: call.attribution.task_id,
                run_id: call.attribution.run_id,
                provider: call.provider,
                model: call.model,
                route: Some(call.route),
                provider_request_id: call.provider_request_id,
            };
            if let Some(budget) = budget {
                if (budget.limits.token_limit.is_some()
                    || budget.limits.cost_limit_nanos_usd.is_some())
                    && !has_known_price(&physical_model)
                {
                    return Err("BUDGET_PRICE_UNKNOWN".to_owned());
                }
                let reserved_cost = if budget.limits.cost_limit_nanos_usd.is_some() {
                    estimate_cost_nanos(&physical_model, budget.input_tokens, budget.output_tokens)
                        .ok_or_else(|| "BUDGET_PRICE_UNKNOWN".to_owned())?
                } else {
                    0
                };
                db.start_llm_call_with_budget(
                    &persisted,
                    &LlmCallBudgetReservation {
                        input_tokens: budget.input_tokens,
                        output_tokens: budget.output_tokens,
                        cost_nanos_usd: reserved_cost,
                    },
                )
                .await
                .map_err(|error| error.to_string())
            } else {
                db.start_llm_call(&persisted)
                    .await
                    .map_err(|error| error.to_string())
            }
        })
    }

    fn call_finished(&self, call: LlmCallFinished) -> BoxFuture<'static, Result<(), String>> {
        let db = self.db.clone();
        Box::pin(async move {
            let pricing_known = has_known_price(&call.model);
            let cost_nanos_usd = call
                .usage
                .as_ref()
                .filter(|_| pricing_known)
                .and_then(|usage| usd_to_nanos(usage_cost_usd(&call.model, usage)));
            let usage = LlmUsageCompletion {
                input_tokens: call.usage.as_ref().map(|usage| usage.input_tokens),
                output_tokens: call.usage.as_ref().map(|usage| usage.output_tokens),
                cache_read_tokens: call
                    .usage
                    .as_ref()
                    .map(|usage| usage.cache_read_input_tokens),
                cache_create_tokens: call
                    .usage
                    .as_ref()
                    .map(|usage| usage.cache_creation_input_tokens),
                cost_nanos_usd,
                usage_complete: call.usage.is_some() && pricing_known && cost_nanos_usd.is_some(),
                error_code: call.error_code,
            };
            let outcome = db
                .finish_llm_call(&call.call_id, call.status.as_str(), &usage)
                .await
                .map_err(|error| error.to_string())?;
            match outcome {
                zk_db::CasOutcome::Applied | zk_db::CasOutcome::InvalidTransition => Ok(()),
                other => Err(format!("LLM_CALL_FINISH_{other:?}")),
            }
        })
    }
}

/// Conservative input + output price used for pre-request reservations. Cache
/// discounts are deliberately ignored because they are not guaranteed in advance.
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
pub(crate) fn estimate_cost_nanos(
    model: &str,
    input_tokens: i64,
    output_tokens: i64,
) -> Option<i64> {
    if input_tokens < 0 || output_tokens < 0 || !has_known_price(model) {
        return None;
    }
    let caps = zk_llm::capabilities_for(model);
    let nanos = input_tokens as f64 * caps.cost_per_1k_input * 1_000_000.0
        + output_tokens as f64 * caps.cost_per_1k_output * 1_000_000.0;
    if !nanos.is_finite() || nanos < 0.0 || nanos > i64::MAX as f64 {
        return None;
    }
    Some(nanos.ceil() as i64)
}

/// Maximum output tokens affordable after reserving the request's input cost.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub(crate) fn affordable_output_tokens(
    model: &str,
    input_tokens: i64,
    remaining_cost_nanos: i64,
) -> Option<u32> {
    if remaining_cost_nanos < 0 || !has_known_price(model) {
        return None;
    }
    let caps = zk_llm::capabilities_for(model);
    let input_cost = input_tokens as f64 * caps.cost_per_1k_input * 1_000_000.0;
    if !input_cost.is_finite() || input_cost > remaining_cost_nanos as f64 {
        return Some(0);
    }
    let per_output_token = caps.cost_per_1k_output * 1_000_000.0;
    if per_output_token <= 0.0 {
        return Some(u32::MAX);
    }
    let affordable = ((remaining_cost_nanos as f64 - input_cost) / per_output_token).floor();
    if !affordable.is_finite() || affordable <= 0.0 {
        return Some(0);
    }
    Some(affordable.min(f64::from(u32::MAX)) as u32)
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
pub(crate) fn usd_to_nanos(usd: f64) -> Option<i64> {
    if !usd.is_finite() || usd < 0.0 || usd > i64::MAX as f64 / 1_000_000_000.0 {
        return None;
    }
    Some((usd * 1_000_000_000.0).round() as i64)
}

#[cfg(test)]
mod tests {
    use zk_llm::{LlmCallStatus, LlmExecutionAttribution};
    use zk_protocol::Usage;

    use super::*;

    #[test]
    fn request_reservation_counts_reasoning_and_adds_tokenizer_margin() {
        let request = ChatRequest::new("qwen3.8-max-0902").with_message(
            ChatMessage::assistant("answer")
                .with_thinking(Some("reasoning that is replayed".repeat(40))),
        );
        let direct = i64::from(crate::context::estimate_tokens(
            &[ChatMessage::assistant("answer")
                .with_thinking(Some("reasoning that is replayed".repeat(40)))],
            "qwen3.8-max-0902",
        ));
        assert_eq!(
            conservative_request_tokens(&request),
            direct.saturating_mul(5).saturating_add(3) / 4
        );
        assert!(conservative_request_tokens(&request) > direct);
    }

    #[tokio::test]
    async fn persists_exact_usage_and_marks_unknown_price_incomplete() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("gpt-5.4-mini", "/tmp/llm-ledger")
            .await
            .expect("session");
        let run_id = uuid::Uuid::new_v4().to_string();
        db.start_run(&run_id, &session.id, None, Some("query"), "gpt-5.4-mini")
            .await
            .expect("run");
        let observer = DbLlmCallObserver::new(db.clone());
        let attribution = LlmExecutionAttribution::new(&run_id, &run_id, "conversation");

        for (call_id, model) in [("known-call", "gpt-5.4-mini"), ("unknown-call", "custom")] {
            observer
                .call_started(LlmCallStarted {
                    call_id: call_id.into(),
                    attribution: attribution.clone(),
                    provider: "script".into(),
                    model: model.into(),
                    route: "{}".into(),
                    provider_request_id: None,
                })
                .await
                .expect("start");
            observer
                .call_finished(LlmCallFinished {
                    call_id: call_id.into(),
                    model: model.into(),
                    status: LlmCallStatus::Completed,
                    usage: Some(Usage {
                        input_tokens: 10,
                        output_tokens: 4,
                        cache_read_input_tokens: 2,
                        cache_creation_input_tokens: 1,
                    }),
                    error_code: None,
                })
                .await
                .expect("finish");
        }

        #[allow(clippy::type_complexity)]
        let rows: Vec<(String, String, Option<i64>, Option<i64>, Option<i64>, i64)> = db
            .with_conn_blocking(|conn| {
                let mut query = conn.prepare(
                    "SELECT call_id,status,input_tokens,output_tokens,cost_nanos_usd,usage_complete
                     FROM llm_calls ORDER BY call_id",
                )?;
                query
                    .query_map([], |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(Into::into)
            })
            .expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "known-call");
        assert_eq!(rows[0].1, "completed");
        assert_eq!(rows[0].2, Some(10));
        assert_eq!(rows[0].3, Some(4));
        assert!(rows[0].4.is_some());
        assert_eq!(rows[0].5, 1);
        assert_eq!(
            rows[1],
            (
                "unknown-call".into(),
                "completed".into(),
                Some(10),
                Some(4),
                None,
                0,
            )
        );
        assert!(
            !db.find_run_by_id(&run_id)
                .await
                .expect("run")
                .unwrap()
                .usage_complete
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn missing_usage_is_incomplete_and_terminal_finish_is_idempotent() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("gpt-5.4-mini", "/tmp/llm-ledger-terminal")
            .await
            .expect("session");
        let run_id = uuid::Uuid::new_v4().to_string();
        db.start_run(&run_id, &session.id, None, Some("query"), "gpt-5.4-mini")
            .await
            .expect("run");
        let observer = DbLlmCallObserver::new(db.clone());
        let attribution = LlmExecutionAttribution::new(&run_id, &run_id, "conversation");

        observer
            .call_started(LlmCallStarted {
                call_id: "failed-call".into(),
                attribution: attribution.clone(),
                provider: "script".into(),
                model: "gpt-5.4-mini".into(),
                route: r#"{"reason":"primary"}"#.into(),
                provider_request_id: Some("provider-failed".into()),
            })
            .await
            .expect("start failed call");
        observer
            .call_finished(LlmCallFinished {
                call_id: "failed-call".into(),
                model: "gpt-5.4-mini".into(),
                status: LlmCallStatus::Failed,
                usage: None,
                error_code: Some("HTTP_503".into()),
            })
            .await
            .expect("finish failed call");

        observer
            .call_started(LlmCallStarted {
                call_id: "idempotent-call".into(),
                attribution,
                provider: "script".into(),
                model: "gpt-5.4-mini".into(),
                route: r#"{"reason":"retry"}"#.into(),
                provider_request_id: Some("provider-success".into()),
            })
            .await
            .expect("start idempotent call");
        let authoritative_usage = Usage {
            input_tokens: 20,
            output_tokens: 5,
            cache_read_input_tokens: 1,
            cache_creation_input_tokens: 2,
        };
        observer
            .call_finished(LlmCallFinished {
                call_id: "idempotent-call".into(),
                model: "gpt-5.4-mini".into(),
                status: LlmCallStatus::Completed,
                usage: Some(authoritative_usage),
                error_code: None,
            })
            .await
            .expect("first terminal finish");
        // Observer completion is idempotent by call_id: a duplicate/later
        // terminal signal must not rewrite the immutable row or double-count.
        observer
            .call_finished(LlmCallFinished {
                call_id: "idempotent-call".into(),
                model: "gpt-5.4-mini".into(),
                status: LlmCallStatus::Failed,
                usage: None,
                error_code: Some("LATE_ERROR".into()),
            })
            .await
            .expect("duplicate terminal finish");

        #[allow(clippy::type_complexity)]
        let rows: Vec<(
            String,
            String,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            i64,
            Option<String>,
        )> = db
            .with_conn_blocking(|conn| {
                let mut query = conn.prepare(
                    "SELECT call_id,status,input_tokens,output_tokens,cost_nanos_usd,
                            usage_complete,error_code
                     FROM llm_calls ORDER BY call_id",
                )?;
                query
                    .query_map([], |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(Into::into)
            })
            .expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            (
                "failed-call".into(),
                "failed".into(),
                None,
                None,
                None,
                0,
                Some("HTTP_503".into()),
            )
        );
        assert_eq!(rows[1].0, "idempotent-call");
        assert_eq!(rows[1].1, "completed");
        assert_eq!(rows[1].2, Some(20));
        assert_eq!(rows[1].3, Some(5));
        assert!(rows[1].4.is_some());
        assert_eq!(rows[1].5, 1);
        assert_eq!(rows[1].6, None, "late error must not replace terminal data");

        let aggregate: (i64, i64, i64, i64) = db
            .with_conn_blocking(move |conn| {
                conn.query_row(
                    "SELECT input_tokens,output_tokens,total_tokens,usage_complete
                     FROM run_envelopes WHERE id=?1",
                    [run_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(Into::into)
            })
            .expect("aggregate");
        assert_eq!(aggregate, (20, 5, 25, 0));
    }
}
