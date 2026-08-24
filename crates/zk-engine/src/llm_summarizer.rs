//! Bounded LLM adapter shared by conversation compaction and tool-result summaries.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use zk_llm::{ChatMessage, ChatProvider, ChatRequest, ProviderEvent, ThinkingMode};

use crate::context::compact::Summarizer;
use crate::summarizer::LightModelSummarizer;

/// Hard wall clock limit for a production summary request.
pub const SUMMARY_TIMEOUT: Duration = Duration::from_secs(30);
/// Raw prompt ceiling. Callers retain their deterministic fallback if this is exceeded.
pub const MAX_SUMMARY_INPUT_CHARS: usize = 400_000;
const COMPACT_SYSTEM_PROMPT: &str = "Summarize the conversation for continuation. Preserve decisions, requirements, file paths, errors, tool outcomes, and unresolved work. Do not invent facts.";

/// Coarse failure counters suitable for later observability export without recording content.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SummarizerMetrics {
    /// Provider setup or stream failures.
    pub provider_failures: u64,
    /// Requests that exceeded their wall clock limit.
    pub timeouts: u64,
    /// Empty model responses.
    pub empty_responses: u64,
    /// Inputs rejected by the hard character ceiling.
    pub oversized_inputs: u64,
}

#[derive(Default)]
struct Counters {
    provider_failures: AtomicU64,
    timeouts: AtomicU64,
    empty_responses: AtomicU64,
    oversized_inputs: AtomicU64,
}

/// Synchronous narrow-port adapter around the streaming provider interface.
///
/// The existing compaction ports are synchronous. Summary calls therefore execute on a
/// dedicated short-lived thread with their own current-thread Tokio runtime, preventing a
/// nested-runtime panic when invoked from the async engine. Calls are serialized because
/// compaction is exceptional and must not create an unbounded background queue.
pub struct LlmSummarizer {
    provider: Arc<dyn ChatProvider>,
    model: String,
    timeout: Duration,
    gate: Mutex<()>,
    counters: Counters,
}

impl std::fmt::Debug for LlmSummarizer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LlmSummarizer")
            .field("model", &self.model)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl LlmSummarizer {
    /// Build a production summarizer for an already resolved lightweight model.
    #[must_use]
    pub fn new(provider: Arc<dyn ChatProvider>, model: impl Into<String>) -> Self {
        Self::with_timeout(provider, model, SUMMARY_TIMEOUT)
    }

    /// Deterministic constructor used by short tests.
    #[must_use]
    pub fn with_timeout(
        provider: Arc<dyn ChatProvider>,
        model: impl Into<String>,
        timeout: Duration,
    ) -> Self {
        Self {
            provider,
            model: model.into(),
            timeout,
            gate: Mutex::new(()),
            counters: Counters::default(),
        }
    }

    /// Snapshot failure counters without exposing summarized content.
    #[must_use]
    pub fn metrics(&self) -> SummarizerMetrics {
        SummarizerMetrics {
            provider_failures: self.counters.provider_failures.load(Ordering::Relaxed),
            timeouts: self.counters.timeouts.load(Ordering::Relaxed),
            empty_responses: self.counters.empty_responses.load(Ordering::Relaxed),
            oversized_inputs: self.counters.oversized_inputs.load(Ordering::Relaxed),
        }
    }

    fn complete(&self, system: &str, user: String, max_tokens: u32) -> Option<String> {
        if user.chars().count() > MAX_SUMMARY_INPUT_CHARS {
            self.counters
                .oversized_inputs
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let _single_flight = self
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let provider = Arc::clone(&self.provider);
        let model = self.model.clone();
        let system = system.to_owned();
        let timeout = self.timeout;
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let spawned = std::thread::Builder::new()
            .name("zk-llm-summary".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build();
                let result = runtime.ok().and_then(|runtime| {
                    runtime.block_on(async move {
                        let cancel = CancellationToken::new();
                        let request = ChatRequest::new(model)
                            .with_message(ChatMessage::user(user))
                            .with_system_prompt(Some(system))
                            .with_tools(Vec::new())
                            .with_max_tokens(max_tokens.max(1))
                            .with_thinking(ThinkingMode::Disabled);
                        let mut stream = provider.chat_stream(request, cancel.clone()).ok()?;
                        let collect = async move {
                            let mut output = String::new();
                            while let Some(event) = stream.next().await {
                                match event {
                                    ProviderEvent::TextDelta { text } => output.push_str(&text),
                                    ProviderEvent::Error { .. } => return None,
                                    _ => {}
                                }
                            }
                            Some(output)
                        };
                        tokio::time::timeout(timeout, collect).await.ok().flatten()
                    })
                });
                let _ = sender.send(result);
            });
        if spawned.is_err() {
            self.counters
                .provider_failures
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
        match receiver.recv_timeout(timeout + Duration::from_millis(250)) {
            Ok(Some(output)) if !output.trim().is_empty() => Some(output.trim().to_owned()),
            Ok(Some(_)) => {
                self.counters
                    .empty_responses
                    .fetch_add(1, Ordering::Relaxed);
                None
            }
            Ok(None) => {
                self.counters
                    .provider_failures
                    .fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(_) => {
                self.counters.timeouts.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }
}

impl Summarizer for LlmSummarizer {
    fn summarize(&self, messages: &[ChatMessage], target_tokens: u32) -> Option<String> {
        let mut prompt = String::from("Conversation to summarize:\n");
        for message in messages {
            prompt.push_str(message.role.as_str());
            prompt.push_str(": ");
            prompt.push_str(&message.content);
            prompt.push('\n');
        }
        self.complete(COMPACT_SYSTEM_PROMPT, prompt, target_tokens.min(4096))
    }
}

impl LightModelSummarizer for LlmSummarizer {
    fn summarize(&self, system_prompt: &str, user_prompt: &str, max_tokens: u32) -> Option<String> {
        self.complete(system_prompt, user_prompt.to_owned(), max_tokens.min(4096))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::stream::{self, BoxStream};
    use zk_llm::{FinishReason, ProviderError};

    use super::*;

    struct RecordingProvider {
        request: Mutex<Option<ChatRequest>>,
        fail: bool,
    }

    impl ChatProvider for RecordingProvider {
        fn provider_name(&self) -> &'static str {
            "summary-test"
        }

        fn chat_stream(
            &self,
            request: ChatRequest,
            _cancel: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            *self.request.lock().expect("request") = Some(request);
            if self.fail {
                return Err(ProviderError::Config {
                    message: "unavailable".into(),
                });
            }
            Ok(Box::pin(stream::iter(vec![
                ProviderEvent::TextDelta {
                    text: " durable summary ".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: None,
                },
            ])))
        }
    }

    #[test]
    fn summary_request_is_toolless_bounded_and_thinking_disabled() {
        let provider = Arc::new(RecordingProvider {
            request: Mutex::new(None),
            fail: false,
        });
        let summarizer = LlmSummarizer::with_timeout(
            Arc::clone(&provider) as Arc<dyn ChatProvider>,
            "light-model",
            Duration::from_millis(200),
        );
        assert_eq!(
            Summarizer::summarize(&summarizer, &[ChatMessage::user("hello")], 700),
            Some("durable summary".into())
        );
        let request = provider
            .request
            .lock()
            .expect("request")
            .clone()
            .expect("call");
        assert_eq!(request.model, "light-model");
        assert!(request.tools.is_empty());
        assert_eq!(request.max_tokens, 700);
        assert_eq!(request.thinking, ThinkingMode::Disabled);
    }

    #[test]
    fn provider_failure_and_oversized_input_return_none_with_metrics() {
        let provider = Arc::new(RecordingProvider {
            request: Mutex::new(None),
            fail: true,
        });
        let summarizer =
            LlmSummarizer::with_timeout(provider, "light-model", Duration::from_millis(200));
        assert!(LightModelSummarizer::summarize(&summarizer, "system", "input", 10).is_none());
        assert!(
            LightModelSummarizer::summarize(
                &summarizer,
                "system",
                &"x".repeat(MAX_SUMMARY_INPUT_CHARS + 1),
                10,
            )
            .is_none()
        );
        assert_eq!(
            summarizer.metrics(),
            SummarizerMetrics {
                provider_failures: 1,
                oversized_inputs: 1,
                ..SummarizerMetrics::default()
            }
        );
    }
}
