//! Bounded LLM adapter shared by conversation compaction and tool-result summaries.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use zk_llm::{
    ChatMessage, ChatProvider, ChatRequest, LlmCallObserver, LlmExecutionAttribution,
    ProviderEvent, ThinkingMode,
};

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

/// Explicit durable owner attached to every physical request made by a scoped
/// summarizer.  Callers create one per active Run; no thread-local state is used.
#[derive(Clone, Debug)]
pub struct SummaryExecution {
    attribution: LlmExecutionAttribution,
    observer_factory: Arc<dyn SummaryObserverFactory>,
}

impl SummaryExecution {
    /// Bind summary calls to a real Task/Run and its durable call observer.
    #[must_use]
    pub fn new(attribution: LlmExecutionAttribution, observer: Arc<dyn LlmCallObserver>) -> Self {
        Self::with_observer_factory(
            attribution,
            Arc::new(FixedSummaryObserverFactory { observer }),
        )
    }

    /// Bind summary calls to a factory that observes the fully assembled summary
    /// request before choosing its durable budget reservation.
    #[must_use]
    pub fn with_observer_factory(
        attribution: LlmExecutionAttribution,
        observer_factory: Arc<dyn SummaryObserverFactory>,
    ) -> Self {
        Self {
            attribution,
            observer_factory,
        }
    }

    fn attach(&self, request: ChatRequest) -> ChatRequest {
        let observer = self.observer_factory.observer_for(&request);
        request.with_execution(self.attribution.clone(), observer)
    }
}

/// Creates the observer for one fully assembled summary request. Implementations
/// can therefore reserve exactly that request's estimated input and output budget.
pub trait SummaryObserverFactory: Send + Sync + std::fmt::Debug {
    /// Return the durable observer that must admit and account for `request`.
    fn observer_for(&self, request: &ChatRequest) -> Arc<dyn LlmCallObserver>;
}

#[derive(Clone, Debug)]
struct FixedSummaryObserverFactory {
    observer: Arc<dyn LlmCallObserver>,
}

impl SummaryObserverFactory for FixedSummaryObserverFactory {
    fn observer_for(&self, _request: &ChatRequest) -> Arc<dyn LlmCallObserver> {
        Arc::clone(&self.observer)
    }
}

/// Run-scoped view of [`LlmSummarizer`].  It implements both summary ports so
/// conversation compaction and tool-result summarization share identical
/// attribution and physical retry/fallback accounting.
#[derive(Clone, Debug)]
pub struct RunScopedLlmSummarizer {
    inner: Arc<LlmSummarizer>,
    execution: SummaryExecution,
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

    /// Create a run-scoped adapter. The supplied observer should be budgeted for
    /// this summary request by the caller once its concrete prompt is known.
    #[must_use]
    pub fn scoped(
        self: &Arc<Self>,
        attribution: LlmExecutionAttribution,
        observer: Arc<dyn LlmCallObserver>,
    ) -> RunScopedLlmSummarizer {
        RunScopedLlmSummarizer {
            inner: Arc::clone(self),
            execution: SummaryExecution::new(attribution, observer),
        }
    }

    fn complete(
        &self,
        system: &str,
        user: String,
        max_tokens: u32,
        execution: Option<&SummaryExecution>,
    ) -> Option<String> {
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
        let execution = execution.cloned();
        let timeout = self.timeout;
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let spawned = std::thread::Builder::new()
            .name("zk-llm-summary".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build();
                let result = runtime.ok().and_then(|runtime| {
                    runtime.block_on(async move {
                        let cancel = CancellationToken::new();
                        let mut request = ChatRequest::new(model)
                            .with_message(ChatMessage::user(user))
                            .with_system_prompt(Some(system))
                            .with_tools(Vec::new())
                            .with_max_tokens(max_tokens.max(1))
                            .with_thinking(ThinkingMode::Disabled);
                        if let Some(execution) = execution {
                            request = execution.attach(request);
                        }
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
        self.complete(COMPACT_SYSTEM_PROMPT, prompt, target_tokens.min(4096), None)
    }

    fn summarize_scoped(
        &self,
        messages: &[ChatMessage],
        target_tokens: u32,
        execution: &SummaryExecution,
    ) -> Option<String> {
        let mut prompt = String::from("Conversation to summarize:\n");
        for message in messages {
            prompt.push_str(message.role.as_str());
            prompt.push_str(": ");
            prompt.push_str(&message.content);
            prompt.push('\n');
        }
        self.complete(
            COMPACT_SYSTEM_PROMPT,
            prompt,
            target_tokens.min(4096),
            Some(execution),
        )
    }
}

impl LightModelSummarizer for LlmSummarizer {
    fn summarize(&self, system_prompt: &str, user_prompt: &str, max_tokens: u32) -> Option<String> {
        self.complete(
            system_prompt,
            user_prompt.to_owned(),
            max_tokens.min(4096),
            None,
        )
    }

    fn summarize_scoped(
        &self,
        system_prompt: &str,
        user_prompt: &str,
        max_tokens: u32,
        execution: &SummaryExecution,
    ) -> Option<String> {
        self.complete(
            system_prompt,
            user_prompt.to_owned(),
            max_tokens.min(4096),
            Some(execution),
        )
    }
}

impl Summarizer for RunScopedLlmSummarizer {
    fn summarize(&self, messages: &[ChatMessage], target_tokens: u32) -> Option<String> {
        let mut prompt = String::from("Conversation to summarize:\n");
        for message in messages {
            prompt.push_str(message.role.as_str());
            prompt.push_str(": ");
            prompt.push_str(&message.content);
            prompt.push('\n');
        }
        self.inner.complete(
            COMPACT_SYSTEM_PROMPT,
            prompt,
            target_tokens.min(4096),
            Some(&self.execution),
        )
    }
}

impl LightModelSummarizer for RunScopedLlmSummarizer {
    fn summarize(&self, system_prompt: &str, user_prompt: &str, max_tokens: u32) -> Option<String> {
        self.inner.complete(
            system_prompt,
            user_prompt.to_owned(),
            max_tokens.min(4096),
            Some(&self.execution),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::net::{TcpListener, TcpStream};
    use std::sync::Mutex;

    use futures::stream::{self, BoxStream};
    use zk_db::{CasOutcome, CreateTaskWithRun, Db, TaskBudgetLimits};
    use zk_llm::{FinishReason, ProviderError, ProviderRegistry};

    use super::*;
    use crate::DbSummaryObserverFactory;

    struct RecordingProvider {
        request: Mutex<Option<ChatRequest>>,
        fail: bool,
        usage: Option<zk_protocol::Usage>,
    }

    struct IoRuntimeProbeProvider {
        socket: Mutex<Option<TcpStream>>,
    }

    impl ChatProvider for IoRuntimeProbeProvider {
        fn provider_name(&self) -> &'static str {
            "summary-io-probe"
        }

        fn chat_stream(
            &self,
            _request: ChatRequest,
            _cancel: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            let socket = self
                .socket
                .lock()
                .expect("probe socket")
                .take()
                .expect("single summary request");
            socket
                .set_nonblocking(true)
                .expect("configure probe socket");
            let _runtime_owned_socket =
                tokio::net::TcpStream::from_std(socket).expect("summary runtime has IO enabled");
            Ok(Box::pin(stream::iter(vec![
                ProviderEvent::TextDelta {
                    text: "network summary".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(zk_protocol::Usage {
                        input_tokens: 1,
                        output_tokens: 1,
                        ..zk_protocol::Usage::default()
                    }),
                },
            ])))
        }
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
                    usage: self.usage,
                },
            ])))
        }
    }

    #[test]
    fn summary_request_is_toolless_bounded_and_thinking_disabled() {
        let provider = Arc::new(RecordingProvider {
            request: Mutex::new(None),
            fail: false,
            usage: Some(zk_protocol::Usage {
                input_tokens: 12,
                output_tokens: 4,
                ..zk_protocol::Usage::default()
            }),
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
    fn summary_runtime_enables_network_io_for_real_providers() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind probe listener");
        let client = TcpStream::connect(listener.local_addr().expect("probe address"))
            .expect("connect probe socket");
        let (_server, _) = listener.accept().expect("accept probe socket");
        let summarizer = LlmSummarizer::with_timeout(
            Arc::new(IoRuntimeProbeProvider {
                socket: Mutex::new(Some(client)),
            }),
            "light-model",
            Duration::from_millis(200),
        );

        assert_eq!(
            LightModelSummarizer::summarize(&summarizer, "system", "input", 10),
            Some("network summary".into())
        );
    }

    #[test]
    fn provider_failure_and_oversized_input_return_none_with_metrics() {
        let provider = Arc::new(RecordingProvider {
            request: Mutex::new(None),
            fail: true,
            usage: None,
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

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn scoped_summary_persists_attribution_and_authoritative_usage() {
        const MODEL: &str = "gpt-5.4-mini";
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session(MODEL, "/tmp/summary-ledger")
            .await
            .expect("session");
        let task_id = uuid::Uuid::new_v4().to_string();
        let run_id = uuid::Uuid::new_v4().to_string();
        assert_ne!(task_id, run_id);
        let created = db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                root_session_id: session.id.clone(),
                transcript_session_id: session.id,
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: "summary ledger".to_owned(),
                prompt: Some("root prompt".to_owned()),
                task_type: "agent".to_owned(),
                model: MODEL.to_owned(),
                working_dir: "/tmp/summary-ledger".to_owned(),
                execution_config_json: "{}".to_owned(),
                startup_epoch: 1,
            })
            .await
            .expect("task/run");
        assert_eq!(
            db.claim_task_run_cas(&task_id, &run_id, created.task.version)
                .await
                .expect("claim"),
            CasOutcome::Applied
        );

        let raw_provider = Arc::new(RecordingProvider {
            request: Mutex::new(None),
            fail: false,
            usage: Some(zk_protocol::Usage {
                input_tokens: 12,
                output_tokens: 4,
                cache_read_input_tokens: 2,
                cache_creation_input_tokens: 1,
            }),
        });
        let mut registry = ProviderRegistry::new();
        registry.register("summary-test", raw_provider, vec![MODEL.to_owned()]);
        let summarizer =
            LlmSummarizer::with_timeout(Arc::new(registry), MODEL, Duration::from_secs(1));
        let execution = SummaryExecution::with_observer_factory(
            LlmExecutionAttribution::new(&task_id, &run_id, "summary"),
            Arc::new(DbSummaryObserverFactory::new(
                db.clone(),
                TaskBudgetLimits::default(),
            )),
        );
        assert_eq!(
            LightModelSummarizer::summarize_scoped(
                &summarizer,
                "Summarize",
                "payload",
                64,
                &execution,
            ),
            Some("durable summary".to_owned())
        );

        let persisted: (String, String, String, i64, Option<String>, i64, i64) = db
            .with_conn_blocking(|connection| {
                connection
                    .query_row(
                        "SELECT task_id,run_id,status,usage_complete,route,input_tokens,output_tokens FROM llm_calls",
                        [],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                                row.get(5)?,
                                row.get(6)?,
                            ))
                        },
                    )
                    .map_err(Into::into)
            })
            .expect("llm call");
        assert_eq!(persisted.0, task_id);
        assert_eq!(persisted.1, run_id);
        assert_eq!(persisted.2, "completed");
        assert_eq!(persisted.3, 1);
        assert_eq!(persisted.5, 12);
        assert_eq!(persisted.6, 4);
        let route: serde_json::Value =
            serde_json::from_str(persisted.4.as_deref().expect("route")).expect("route json");
        assert_eq!(route["kind"], "summary");
        assert!(
            db.find_run_by_id(&run_id)
                .await
                .expect("run")
                .expect("run row")
                .usage_complete
        );
        assert!(
            db.find_runtime_task_by_id(&task_id)
                .await
                .expect("task")
                .expect("task row")
                .usage_complete
        );
    }
}
