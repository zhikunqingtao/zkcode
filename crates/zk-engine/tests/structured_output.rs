//! Real engine execution: one bounded format repair, original evidence and physical-call billing.
use futures::{
    StreamExt,
    future::BoxFuture,
    stream::{self, BoxStream},
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio_util::sync::CancellationToken;
use zk_db::{Db, StoredBlock};
use zk_engine::structured_output::StructuredOutputContract;
use zk_engine::{ConversationRunOptions, ConversationService, Engine, MessageSink};
use zk_llm::{
    ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent, ProviderRegistry,
};
use zk_protocol::{ServerMessage, Usage};
use zk_tools::{Tool, ToolContext, ToolOutput, ToolRegistry};

#[derive(Default)]
struct Sink;
impl MessageSink for Sink {
    fn push<'a>(&'a self, _: &'a str, _: ServerMessage) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}
struct Provider {
    scripts: Mutex<VecDeque<Vec<ProviderEvent>>>,
    requests: Mutex<Vec<ChatRequest>>,
}
impl ChatProvider for Provider {
    fn provider_name(&self) -> &'static str {
        "format-fixture"
    }
    fn chat_stream(
        &self,
        request: ChatRequest,
        _: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        self.requests.lock().unwrap().push(request);
        Ok(stream::iter(
            self.scripts
                .lock()
                .unwrap()
                .pop_front()
                .expect("no unbounded format retry"),
        )
        .boxed())
    }
}
struct Counted(Arc<AtomicUsize>);
impl Tool for Counted {
    fn name(&self) -> &'static str {
        "Counted"
    }
    fn description(&self) -> &'static str {
        "Count actual effects"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    fn execute(&self, _: Value, _: ToolContext) -> BoxFuture<'_, ToolOutput> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { ToolOutput::ok("done") })
    }
}
fn reply(text: &str) -> Vec<ProviderEvent> {
    vec![
        ProviderEvent::TextDelta { text: text.into() },
        finish(FinishReason::EndTurn),
    ]
}
fn finish(reason: FinishReason) -> ProviderEvent {
    ProviderEvent::Finish {
        finish_reason: reason,
        usage: Some(Usage {
            input_tokens: 20,
            output_tokens: 5,
            ..Usage::default()
        }),
    }
}
fn contract() -> Arc<StructuredOutputContract> {
    Arc::new(StructuredOutputContract::new(json!({"type":"object","required":["ok"],"properties":{"ok":{"const":true}},"additionalProperties":false})).unwrap())
}
async fn execute(
    scripts: Vec<Vec<ProviderEvent>>,
    max_turns: usize,
) -> (
    zk_engine::ConversationOutcome,
    Arc<Provider>,
    Arc<AtomicUsize>,
    Db,
) {
    let db = Db::open_in_memory().unwrap();
    let session = db.create_session("qwen3.8-max-0902", "/tmp").await.unwrap();
    let provider = Arc::new(Provider {
        scripts: Mutex::new(scripts.into()),
        requests: Mutex::new(Vec::new()),
    });
    let mut providers = ProviderRegistry::new();
    providers.register(
        "format-fixture",
        provider.clone(),
        vec!["qwen3.8-max-0902".into()],
    );
    let executions = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(Arc::new(Counted(executions.clone())));
    let engine = Arc::new(Engine::with_tools(
        db.clone(),
        Arc::new(providers),
        Arc::new(Sink),
        Arc::new(tools),
    ));
    let service = ConversationService::new(engine, db.clone());
    let outcome = service
        .execute_with_options(
            &session.id,
            "Reply with the requested shape".into(),
            ConversationRunOptions {
                structured_output: Some(contract()),
                max_turns,
                ..ConversationRunOptions::default()
            },
        )
        .await;
    (outcome, provider, executions, db)
}

#[tokio::test]
async fn repair_is_billed_and_preserves_original_answer() {
    let (outcome, provider, effects, db) = execute(
        vec![reply("original invalid answer"), reply(r#"{"ok":true}"#)],
        10,
    )
    .await;
    assert_eq!(outcome.error, None);
    assert_eq!(outcome.result, r#"{"ok":true}"#);
    assert_eq!(outcome.usage.input_tokens, 40);
    assert_eq!(outcome.usage.output_tokens, 10);
    assert!(outcome.cost_usd > 0.0 && outcome.usage_complete);
    {
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(!requests[0].tools.is_empty());
        assert!(requests[1].tools.is_empty());
        assert_eq!(effects.load(Ordering::SeqCst), 0);
    }
    let messages = db
        .get_session(&outcome.session_id)
        .await
        .unwrap()
        .unwrap()
        .messages;
    assert!(
        messages
            .iter()
            .flat_map(|m| &m.content)
            .any(|b| matches!(b,StoredBlock::Text{text} if text=="original invalid answer"))
    );
    assert_eq!(
        messages
            .iter()
            .filter(|m| m
                .meta
                .as_ref()
                .is_some_and(|v| v["runtimeProjection"] == "json_schema_repair"))
            .count(),
        1
    );
}

#[tokio::test]
async fn invalid_repair_fails_without_a_third_call() {
    let (outcome, provider, _, _) = execute(vec![reply("bad"), reply(r#"{"ok":false}"#)], 10).await;
    assert_eq!(
        outcome.error.as_deref(),
        Some("JSON_SCHEMA_VALIDATION_FAILED")
    );
    assert_eq!(provider.requests.lock().unwrap().len(), 2);
    assert_eq!(outcome.usage.input_tokens, 40);
}

#[tokio::test]
async fn repair_cannot_execute_a_tool_or_continue_the_loop() {
    let call = vec![
        ProviderEvent::ToolUseStart {
            id: "forbidden".into(),
            name: "Counted".into(),
        },
        ProviderEvent::ToolInputDelta {
            id: "forbidden".into(),
            delta: "{}".into(),
        },
        finish(FinishReason::ToolUse),
    ];
    let (outcome, provider, effects, _) = execute(vec![reply("bad"), call], 10).await;
    assert_eq!(
        outcome.error.as_deref(),
        Some("JSON_SCHEMA_REPAIR_TOOL_CALL")
    );
    assert_eq!(provider.requests.lock().unwrap().len(), 2);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn exhausted_turn_limit_does_not_grant_a_repair_call() {
    let (outcome, provider, _, _) = execute(vec![reply("bad")], 1).await;
    assert!(outcome.error.is_some());
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
}
