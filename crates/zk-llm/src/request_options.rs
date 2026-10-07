//! Explicit request options share one conservative capability policy. Defaults
//! stay in the adapters; an unsupported override is rejected before dispatch.

use crate::{ChatRequest, ProviderError};

/// Portable names for model reasoning effort (not a token budget).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningEffort {
    /// Low reasoning effort.
    Low,
    /// Medium reasoning effort.
    Medium,
    /// High reasoning effort.
    High,
    /// Extra-high reasoning effort.
    XHigh,
    /// Maximum reasoning effort.
    Max,
}

impl ReasoningEffort {
    /// Provider wire value, with no implicit conversion between max and xhigh.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
        }
    }
}

/// Known explicit effort levels for the model's built-in transport. Do not infer
/// effort support merely from `supports_thinking`: budget-based models differ.
#[must_use]
pub fn supported_reasoning_efforts(model: &str) -> &'static [ReasoningEffort] {
    use ReasoningEffort::{High, Low, Max, XHigh};
    match model {
        "deepseek-v4-flash"
        | "deepseek-v4-pro"
        | "deepseek-flash"
        | "deepseek-v4.1-flash"
        | "deepseek-v4-pro-0813"
        | "deepseek-v4-flash-0731" => &[Low, High, Max],
        "openai/gpt-5.6-sol" | "openai/gpt-6-astra" => &[XHigh],
        "google/gemini-3.8-flash" | "x-ai/grok-4.6" => &[High],
        // OpenRouter's namespaced routes are deliberately exact: custom routes
        // must not inherit a claim of support from an arbitrary string prefix.
        "gpt-5.6-sol"
        | "gpt-6-astra"
        | "kimi-k3"
        | "kimi-for-coding"
        | "k3"
        | "glm-5.3"
        | "glm-5.3-flash"
        | "bailian/glm-5.3"
        | "openrouter/anthropic/claude-fable-5.1"
        | "openrouter/openai/gpt-6-astra" => &[Max],
        _ => &[],
    }
}

/// The same model can have a different wire vocabulary on another transport.
#[must_use]
pub fn supported_reasoning_efforts_for_provider(
    provider: &str,
    model: &str,
) -> &'static [ReasoningEffort] {
    if provider == "anthropic" {
        return &[];
    }
    if model.starts_with("openai/") && !crate::responses::uses_responses(provider, model) {
        return match model {
            "openai/gpt-5.6-sol" | "openai/gpt-6-astra" => &[ReasoningEffort::Max],
            _ => &[],
        };
    }
    supported_reasoning_efforts(model)
}

/// Validate an explicit effort against the built-in model's default transport.
///
/// # Errors
/// Unsupported models or levels return an actionable configuration diagnostic.
pub fn validate_reasoning_effort(model: &str, effort: ReasoningEffort) -> Result<(), String> {
    validate_effort(model, effort, supported_reasoning_efforts(model))
}

fn validate_effort(
    model: &str,
    effort: ReasoningEffort,
    supported: &[ReasoningEffort],
) -> Result<(), String> {
    if supported.contains(&effort) {
        Ok(())
    } else {
        Err(format!(
            "UNSUPPORTED_REASONING_EFFORT: model '{model}' does not support '{}'",
            effort.as_str()
        ))
    }
}

/// Validate explicit options before reserving a physical call or opening HTTP.
///
/// # Errors
/// Rejects unsupported effort, contradictory thinking, malformed stops, and
/// Responses stop sequences (that protocol has no stop-sequence parameter).
pub fn validate_request_options(
    request: &ChatRequest,
    provider: &str,
) -> Result<(), ProviderError> {
    let reject = |message| ProviderError::Preflight { message };
    let thinking_disabled = match request.summary_thinking {
        Some(crate::SummaryThinkingMode::Off) => true,
        Some(crate::SummaryThinkingMode::Low | crate::SummaryThinkingMode::Max) => false,
        None => !request.thinking.requires_support(),
    };
    if thinking_disabled
        && (matches!(request.model.as_str(), "glm-5.3" | "glm-5.3-flash")
            || (provider == "dashscope-token-plan" && request.model == "bailian/glm-5.3"))
    {
        return Err(reject(
            "UNSUPPORTED_THINKING_DISABLED: this GLM model requires thinking mode".into(),
        ));
    }
    if thinking_disabled && provider == "openai" && request.model == "gpt-6-astra" {
        return Err(reject(
            "UNSUPPORTED_THINKING_DISABLED: direct GPT-6 Astra requires thinking mode".into(),
        ));
    }
    if let Some(effort) = request.reasoning_effort {
        if !request.thinking.requires_support() || request.summary_thinking.is_some() {
            return Err(reject("REASONING_OPTIONS_CONFLICT: explicit effort requires enabled thinking and cannot override independent summary settings".into()));
        }
        validate_effort(
            &request.model,
            effort,
            supported_reasoning_efforts_for_provider(provider, &request.model),
        )
        .map_err(reject)?;
    }
    if request.stop_sequences.len() > 4
        || request
            .stop_sequences
            .iter()
            .any(|stop| stop.is_empty() || stop.len() > 1024)
    {
        return Err(reject("INVALID_STOP_SEQUENCES: use at most four nonempty sequences of at most 1024 UTF-8 bytes".into()));
    }
    if !request.stop_sequences.is_empty()
        && crate::responses::uses_responses(provider, &request.model)
    {
        return Err(reject(
            "UNSUPPORTED_STOP_SEQUENCES: Responses transport does not accept stop sequences".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ThinkingMode;

    #[test]
    fn explicit_options_fail_closed_without_changing_defaults() {
        let mut request = ChatRequest::new("custom-model");
        assert!(validate_request_options(&request, "custom").is_ok());
        request.thinking = ThinkingMode::Enabled;
        request.reasoning_effort = Some(ReasoningEffort::Max);
        assert!(validate_request_options(&request, "custom").is_err());
        request.model = "deepseek-flash".into();
        assert!(validate_request_options(&request, "deepseek").is_ok());
        request.reasoning_effort = Some(ReasoningEffort::Medium);
        assert!(validate_request_options(&request, "deepseek").is_err());
    }

    #[test]
    fn transport_effort_and_stop_contracts_are_not_silently_converted() {
        let mut request =
            ChatRequest::new("openai/gpt-6-astra").with_thinking(ThinkingMode::Enabled);
        request.reasoning_effort = Some(ReasoningEffort::XHigh);
        assert!(validate_request_options(&request, "zenmux").is_ok());
        assert!(validate_request_options(&request, "openai").is_err());
        request.stop_sequences = vec!["END".into()];
        assert!(validate_request_options(&request, "zenmux").is_err());
        request.model = "deepseek-flash".into();
        request.reasoning_effort = Some(ReasoningEffort::Low);
        assert!(validate_request_options(&request, "deepseek").is_ok());
        request.stop_sequences.push(String::new());
        assert!(validate_request_options(&request, "deepseek").is_err());
    }
}
