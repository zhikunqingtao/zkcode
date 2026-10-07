//! Compression effectiveness measured against the assembled request budget.
//!
//! The 70% target is advisory: mandatory user text and images are never removed
//! to satisfy it. The hard history allowance includes system/tool overhead,
//! output reservation and wire margin, and final image payloads count as input.

use base64::Engine as _;
use serde::Serialize;
use zk_llm::{ChatMessage, ChatRequest};

/// Bounded outcome labels for context metrics and request admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum QualityOutcome {
    /// The request is within the preferred headroom target.
    TargetSatisfied,
    /// Above the advisory target, but safe to send without destroying evidence.
    SafeAboveTarget,
    /// Compression did not release tokens, although the request still fits.
    NoGain,
    /// The assembled input cannot fit after reserving output and wire overhead.
    HardBudgetExceeded,
}

impl QualityOutcome {
    /// Stable, low-cardinality observability label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TargetSatisfied => "targetSatisfied",
            Self::SafeAboveTarget => "safeAboveTarget",
            Self::NoGain => "noGain",
            Self::HardBudgetExceeded => "hardBudgetExceeded",
        }
    }
}

/// Diagnostic facts; no message text, file paths, image data or credentials.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextQuality {
    /// Input estimate before the bounded compression phase.
    pub before_tokens: u32,
    /// Input estimate after compression and final image preparation.
    pub after_tokens: u32,
    /// Space available to history after real request overhead is reserved.
    pub history_budget: u32,
    /// Preferred headroom threshold, 70% of the history budget.
    pub target_tokens: u32,
    /// Additional savings required to reach the advisory target.
    pub tokens_to_release: u32,
    /// Actual nonnegative savings.
    pub tokens_saved: u32,
    /// A configured or built-in capability, rather than an unknown-model guess.
    pub model_budget_known: bool,
    /// Effectiveness/safety classification.
    pub outcome: QualityOutcome,
}

impl ContextQuality {
    /// Assess final prepared input without mutating messages or performing an
    /// additional summarization request.
    #[must_use]
    pub fn for_request(before_tokens: u32, request: &ChatRequest, attempted: bool) -> Self {
        Self::evaluate(
            before_tokens,
            history_tokens(&request.messages, &request.model),
            super::request_history_budget(request),
            attempted,
            zk_llm::is_known_model(&request.model),
        )
    }

    /// Whether the established model budget proves that dispatch must stop.
    /// Unknown-model fallback estimates remain diagnostic and do not override a
    /// previously working user provider configuration.
    #[must_use]
    pub fn blocks_dispatch(&self) -> bool {
        self.model_budget_known && self.outcome == QualityOutcome::HardBudgetExceeded
    }

    fn evaluate(before: u32, after: u32, budget: u32, attempted: bool, known: bool) -> Self {
        let target = u32::try_from(u64::from(budget) * 7 / 10).unwrap_or(u32::MAX);
        let outcome = if after > budget {
            QualityOutcome::HardBudgetExceeded
        } else if attempted && after >= before {
            QualityOutcome::NoGain
        } else if after > target {
            QualityOutcome::SafeAboveTarget
        } else {
            QualityOutcome::TargetSatisfied
        };
        Self {
            before_tokens: before,
            after_tokens: after,
            history_budget: budget,
            target_tokens: target,
            tokens_to_release: after.saturating_sub(target),
            tokens_saved: before.saturating_sub(after),
            model_budget_known: known,
            outcome,
        }
    }
}

/// Count text/replayed reasoning/tool transactions plus the actual prepared
/// image payload. Invalid images cannot produce an optimistic budget estimate.
#[must_use]
pub fn history_tokens(messages: &[ChatMessage], model: &str) -> u32 {
    messages.iter().flat_map(|message| &message.images).fold(
        super::estimate_tokens(messages, model),
        |total, image| {
            let tokens = image
                .data
                .as_deref()
                .map_or(Ok(1024), |encoded| {
                    // The token estimator alone intentionally handles malformed
                    // legacy base64 conservatively; admission must not mistake
                    // those fallback character counts for a validated image.
                    if encoded.len() > super::image_budget::MAX_DECODED_IMAGE_BYTES.div_ceil(3) * 4
                    {
                        return Err(());
                    }
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .map_err(|_| ())?;
                    zk_llm::payload_guard::complete_image_media_type(&bytes).map_err(|_| ())?;
                    zk_llm::payload_guard::inline_image_tokens(encoded).map_err(|_| ())
                })
                .ok()
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or(u32::MAX);
            total.saturating_add(tokens)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advisory_target_never_rejects_safe_mandatory_context() {
        let report = ContextQuality::evaluate(12_000, 8_000, 10_000, true, true);
        assert_eq!(report.outcome, QualityOutcome::SafeAboveTarget);
        assert_eq!(report.target_tokens, 7_000);
        assert_eq!(report.tokens_to_release, 1_000);
        assert!(!report.blocks_dispatch());
        assert!(ContextQuality::evaluate(12_000, 10_001, 10_000, true, true).blocks_dispatch());
        assert_eq!(
            ContextQuality::evaluate(8_000, 8_000, 10_000, true, true).outcome,
            QualityOutcome::NoGain
        );
        assert!(!ContextQuality::evaluate(12_000, 10_001, 10_000, true, false).blocks_dispatch());
    }

    #[test]
    fn real_output_and_tool_overhead_reduce_the_history_allowance() {
        let mut request =
            ChatRequest::new("gpt-5.4-mini").with_message(ChatMessage::user("keep original"));
        let baseline = ContextQuality::for_request(100, &request, false);
        request.max_tokens = request.max_tokens.saturating_add(4096);
        let with_output = ContextQuality::for_request(100, &request, false);
        assert_eq!(
            baseline
                .history_budget
                .saturating_sub(with_output.history_budget),
            4096
        );
        request.system_prompt = Some("system overhead ".repeat(2048));
        let with_system = ContextQuality::for_request(100, &request, false);
        assert!(with_system.history_budget < with_output.history_budget);
        request.tools.push(zk_llm::ToolSpec {
            name: "Read".into(),
            description: "required tool schema ".repeat(1024),
            parameters: serde_json::json!({"type":"object"}),
        });
        assert!(
            ContextQuality::for_request(100, &request, false).history_budget
                < with_system.history_budget
        );
        assert_eq!(request.messages[0].content, "keep original");
    }

    #[test]
    fn image_cost_is_not_silently_dropped_from_quality_metrics() {
        let mut messages = vec![ChatMessage::user("original instruction")];
        let baseline = history_tokens(&messages, "gpt-5.4-mini");
        messages[0].images.push(zk_llm::ImageSource {
            media_type: "image/png".into(),
            data: Some("invalid-image".into()),
            url: None,
        });
        assert_eq!(history_tokens(&messages, "gpt-5.4-mini"), u32::MAX);
        messages[0].images[0].data = None;
        messages[0].images[0].url = Some("https://approved.example/image.png".into());
        assert_eq!(history_tokens(&messages, "gpt-5.4-mini"), baseline + 1024);
        assert_eq!(messages[0].content, "original instruction");
    }
}
