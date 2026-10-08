//! Production summary registry + `SQLite` budget observer regressions. The provider
//! fixture speaks the real provider event protocol and never performs network IO.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use futures::stream::{self, BoxStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use zk_db::{
    CasOutcome, CleanupStatus, CommitTaskResult, CommitTaskResultOutcome, CreateTaskWithRun, Db,
    LlmCallBudgetReservation, MessageAttribution, MessageRole, NewLlmCall, NewMessage,
    ResultStatus, StoredBlock, TaskBudgetLimits, VerificationStatus,
};
use zk_llm::{
    ApiKey, ChatMessage, ChatProvider, ChatRequest, FinishReason, LlmExecutionAttribution,
    OpenAiCompatProvider, ProviderConfig, ProviderError, ProviderEvent, ProviderRegistry,
    SummaryThinkingMode,
};
use zk_protocol::Usage;

use super::{LlmSummarizer, SummaryExecution};
use crate::context::compact::{CompactLevel, compact_messages_scoped};
use crate::summarizer::LightModelSummarizer;
use crate::{DbLlmCallObserver, DbSummaryObserverFactory};

const MODEL: &str = "deepseek-flash";

#[derive(Default)]
struct ProtocolFixture {
    model: String,
    calls: AtomicUsize,
    attempts: Mutex<VecDeque<Vec<ProviderEvent>>>,
}

impl ProtocolFixture {
    fn new(attempts: Vec<Vec<ProviderEvent>>) -> Arc<Self> {
        Self::for_model(MODEL, attempts)
    }

    fn for_model(model: &str, attempts: Vec<Vec<ProviderEvent>>) -> Arc<Self> {
        Arc::new(Self {
            model: model.into(),
            calls: AtomicUsize::new(0),
            attempts: Mutex::new(attempts.into()),
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl ChatProvider for ProtocolFixture {
    fn provider_name(&self) -> &'static str {
        "summary-ledger-protocol-fixture"
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        _cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        assert_eq!(request.model, self.model);
        assert!(request.tools.is_empty());
        self.calls.fetch_add(1, Ordering::SeqCst);
        let events = self
            .attempts
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected physical request");
        Ok(Box::pin(stream::iter(events)))
    }
}

struct BudgetedRun {
    db: Db,
    task: String,
    run: String,
    limits: TaskBudgetLimits,
    execution: SummaryExecution,
}

impl BudgetedRun {
    async fn new() -> Self {
        let db = Db::open_in_memory().unwrap();
        let session = db
            .create_session(MODEL, "/tmp/summary-ledger-fixture")
            .await
            .unwrap();
        let task = uuid::Uuid::new_v4().to_string();
        let run = uuid::Uuid::new_v4().to_string();
        let created = db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: task.clone(),
                run_id: run.clone(),
                root_session_id: session.id.clone(),
                transcript_session_id: session.id,
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: "real summary ledger fixture".into(),
                prompt: Some("local fixture".into()),
                task_type: "agent".into(),
                model: MODEL.into(),
                working_dir: "/tmp/summary-ledger-fixture".into(),
                execution_config_json: "{}".into(),
                startup_epoch: 1,
            })
            .await
            .unwrap();
        let limits = TaskBudgetLimits {
            token_limit: Some(200_000),
            cost_limit_nanos_usd: Some(1_000_000_000),
            deadline_at_ms: Some(zk_db::time::now_millis() + 30_000),
        };
        assert_eq!(
            db.configure_root_task_budget_cas(&task, created.task.budget_version, &limits)
                .await
                .unwrap(),
            CasOutcome::Applied
        );
        assert_eq!(
            db.claim_task_run_cas(&task, &run, created.task.version)
                .await
                .unwrap(),
            CasOutcome::Applied
        );
        let execution = SummaryExecution::with_observer_factory(
            LlmExecutionAttribution::new(&task, &run, "summary"),
            Arc::new(DbSummaryObserverFactory::new(db.clone(), limits.clone())),
        );
        Self {
            db,
            task,
            run,
            limits,
            execution,
        }
    }

    fn calls(&self) -> Vec<CallRow> {
        self.db.with_conn_blocking(|conn| {
            let mut query = conn.prepare("SELECT status,usage_complete,input_tokens,output_tokens,cost_nanos_usd,error_code,route FROM llm_calls ORDER BY created_at,call_id")?;
            let rows = query.query_map([], |row| Ok(CallRow {
                status: row.get(0)?, complete: row.get::<_, i64>(1)? != 0,
                input: row.get(2)?, output: row.get(3)?, cost: row.get(4)?,
                error: row.get(5)?, route: row.get(6)?,
            }))?.collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        }).unwrap()
    }

    async fn assert_live_charge_and_terminal_settlement(&self, calls: &[CallRow]) {
        let tokens = calls
            .iter()
            .map(|call| call.input.unwrap() + call.output.unwrap())
            .sum::<i64>();
        let cost = calls.iter().map(|call| call.cost.unwrap()).sum::<i64>();
        let live = self.db.find_run_by_id(&self.run).await.unwrap().unwrap();
        assert!(live.usage_complete);
        assert_eq!(live.total_tokens, tokens);
        assert_eq!(live.cost_nanos_usd, cost);

        // The current Run is charged directly during admission. Task consumed
        // counters are a terminal projection, not a second copy of live usage.
        let budget = self.db.read_task_budget(&self.task).await.unwrap().unwrap();
        assert_eq!(budget.consumed_tokens, 0);
        assert_eq!(budget.consumed_cost_nanos_usd, 0);
        self.db
            .assert_task_run_budget_within_limits(&self.task, &self.run)
            .await
            .unwrap();
        for (code, reservation) in [
            (
                "TOKEN_BUDGET_EXHAUSTED",
                LlmCallBudgetReservation {
                    input_tokens: self.limits.token_limit.unwrap() - tokens,
                    output_tokens: 1,
                    cost_nanos_usd: 1,
                },
            ),
            (
                "COST_BUDGET_EXHAUSTED",
                LlmCallBudgetReservation {
                    input_tokens: 0,
                    output_tokens: 1,
                    cost_nanos_usd: self.limits.cost_limit_nanos_usd.unwrap() - cost + 1,
                },
            ),
        ] {
            let denied = self
                .db
                .start_llm_call_with_budget(
                    &NewLlmCall {
                        call_id: uuid::Uuid::new_v4().to_string(),
                        task_id: self.task.clone(),
                        run_id: self.run.clone(),
                        provider: "ledger-boundary-fixture".into(),
                        model: MODEL.into(),
                        route: None,
                        provider_request_id: None,
                    },
                    &reservation,
                )
                .await
                .unwrap_err();
            assert!(denied.to_string().contains(code), "{denied}");
        }
        assert_eq!(self.calls().len(), calls.len());

        self.assert_terminal_settlement(&live.session_id, tokens, cost)
            .await;
    }

    async fn assert_terminal_settlement(&self, session: &str, tokens: i64, cost: i64) {
        let content = "Completed local ledger verification";
        self.db
            .append_attributed_message(
                session,
                NewMessage {
                    meta: None,
                    role: MessageRole::Assistant,
                    content: vec![StoredBlock::Text {
                        text: content.into(),
                    }],
                    stop_reason: Some("end_turn".into()),
                    input_tokens: 0,
                    output_tokens: 0,
                },
                MessageAttribution {
                    task_id: Some(self.task.clone()),
                    run_id: Some(self.run.clone()),
                    origin: "conversation".into(),
                    source_task_id: None,
                },
            )
            .await
            .unwrap();
        let task = self
            .db
            .find_runtime_task_by_id(&self.task)
            .await
            .unwrap()
            .unwrap();
        let terminal = CommitTaskResult {
            task_id: self.task.clone(),
            run_id: self.run.clone(),
            expected_task_version: task.version,
            status: ResultStatus::Complete,
            content: content.into(),
            media_type: "text/markdown".into(),
            error_code: None,
            cleanup_status: CleanupStatus::Confirmed,
            verification_status: VerificationStatus::NotRequested,
        };
        assert!(matches!(
            self.db.commit_task_result(&terminal).await.unwrap(),
            CommitTaskResultOutcome::Committed { .. }
        ));
        assert_eq!(
            self.db.commit_task_result(&terminal).await.unwrap(),
            CommitTaskResultOutcome::AlreadyTerminal
        );
        let settled = self.db.read_task_budget(&self.task).await.unwrap().unwrap();
        assert!(settled.usage_complete);
        assert_eq!(settled.consumed_tokens, tokens);
        assert_eq!(settled.consumed_cost_nanos_usd, cost);
        assert_eq!(settled.reserved_tokens, 0);
        assert_eq!(settled.reserved_cost_nanos_usd, 0);
    }
}

#[derive(Debug)]
struct CallRow {
    status: String,
    complete: bool,
    input: Option<i64>,
    output: Option<i64>,
    cost: Option<i64>,
    error: Option<String>,
    route: Option<String>,
}

fn usage() -> Usage {
    Usage {
        input_tokens: 12,
        output_tokens: 4,
        ..Usage::default()
    }
}

fn limited(known_usage: bool) -> Vec<ProviderEvent> {
    let mut events = Vec::new();
    if known_usage {
        events.push(ProviderEvent::UsageUpdate { usage: usage() });
    }
    events.push(ProviderEvent::Error {
        error: ProviderError::Http {
            status: 429,
            message: "local protocol fixture".into(),
            retry_after_ms: Some(0),
            retryable: true,
        },
    });
    events
}

fn success() -> Vec<ProviderEvent> {
    vec![
        ProviderEvent::TextDelta { text: "<summary>Durable summary retains the user requirement, records the completed investigation and pending verification, and grants no new execution permission.</summary>".into() },
        ProviderEvent::Finish { finish_reason: FinishReason::EndTurn, usage: Some(usage()) },
    ]
}

fn registry(fixture: &Arc<ProtocolFixture>) -> Arc<ProviderRegistry> {
    let mut registry = ProviderRegistry::new();
    registry.register(fixture.provider_name(), fixture.clone(), vec![MODEL.into()]);
    Arc::new(registry)
}

fn summarizer(fixture: &Arc<ProtocolFixture>) -> LlmSummarizer {
    summarizer_with_registry(registry(fixture))
}

fn summarizer_with_registry(registry: Arc<ProviderRegistry>) -> LlmSummarizer {
    let mut summarizer = LlmSummarizer::with_timeout(registry, MODEL, Duration::from_secs(3));
    // Match the independent production summary path, not the ordinary-query retry path.
    summarizer.summary_thinking = Some(SummaryThinkingMode::Max);
    summarizer.summary_generation_tokens = Some(8192);
    summarizer.summary_max_tokens = 4096;
    summarizer
}

async fn request_events(
    registry: &ProviderRegistry,
    execution: &SummaryExecution,
) -> Vec<ProviderEvent> {
    let mut request = ChatRequest::new(MODEL)
        .with_message(ChatMessage::user("bounded local summary fixture"))
        .with_max_tokens(128);
    request.summary_thinking = Some(SummaryThinkingMode::Max);
    registry
        .chat_stream(execution.attach(request), CancellationToken::new())
        .unwrap()
        .collect()
        .await
}

async fn ordinary_request_events(
    fixture: &Arc<ProtocolFixture>,
    run: &BudgetedRun,
) -> Vec<ProviderEvent> {
    let mut registry = ProviderRegistry::new().with_retry_policy(zk_llm::RetryPolicy::immediate(2));
    registry.register(fixture.provider_name(), fixture.clone(), vec![MODEL.into()]);
    ordinary_events_with_registry(&registry, run).await
}

async fn ordinary_events_with_registry(
    registry: &ProviderRegistry,
    run: &BudgetedRun,
) -> Vec<ProviderEvent> {
    let request = ChatRequest::new(MODEL)
        .with_message(ChatMessage::user("ordinary conversation fixture"))
        .with_max_tokens(128);
    assert!(request.summary_thinking.is_none());
    let input_reservation = crate::llm_ledger::conservative_request_tokens(&request);
    let observer = DbLlmCallObserver::shared_budgeted(
        run.db.clone(),
        run.limits.clone(),
        input_reservation,
        128,
    );
    registry
        .chat_stream(
            request.with_execution(
                LlmExecutionAttribution::new(&run.task, &run.run, "conversation"),
                observer,
            ),
            CancellationToken::new(),
        )
        .unwrap()
        .collect()
        .await
}

/// Wire-level fixture for the real HTTP adapter. It always returns an HTTP 429
/// without usage, so any accidental second network request remains observable.
struct Http429Fixture {
    base_url: String,
    requests: Arc<Mutex<Vec<serde_json::Value>>>,
    server: tokio::task::JoinHandle<()>,
}

impl Http429Fixture {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let body =
                    tokio::time::timeout(Duration::from_secs(5), read_http_request(&mut socket))
                        .await
                        .unwrap();
                captured.lock().unwrap().push(body);
                let body = r#"{"error":{"message":"fixture rate limit without usage"}}"#;
                socket.write_all(format!("HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: {}\r\nRetry-After: 0\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        });
        Self {
            base_url,
            requests,
            server,
        }
    }

    fn registry(&self) -> Arc<ProviderRegistry> {
        let provider = OpenAiCompatProvider::with_client(
            ProviderConfig::new(
                "deepseek",
                &self.base_url,
                ApiKey::new("local-fixture-only"),
                MODEL,
                vec![MODEL.into()],
            ),
            reqwest::Client::builder().no_proxy().build().unwrap(),
        );
        let mut registry =
            ProviderRegistry::new().with_retry_policy(zk_llm::RetryPolicy::immediate(2));
        registry.register("deepseek", Arc::new(provider), vec![MODEL.into()]);
        Arc::new(registry)
    }

    fn assert_single_request(&self, stream: bool) {
        let requests = self.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "the ledger must stop a second HTTP dispatch"
        );
        assert_eq!(requests[0]["model"], MODEL);
        assert_eq!(requests[0]["stream"], stream);
        if !stream {
            assert_eq!(requests[0]["max_tokens"], 8192);
            assert_eq!(requests[0]["reasoning_effort"], "max");
            assert!(requests[0].get("stream_options").is_none());
        }
    }
}

impl Drop for Http429Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) -> serde_json::Value {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0; 4096];
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&chunk[..count]);
        assert!(bytes.len() < 2 * 1024 * 1024);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
    assert!(headers.starts_with("POST /v1/chat/completions "));
    let length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    assert!(length < 2 * 1024 * 1024);
    while bytes.len() < header_end + length {
        let mut chunk = [0; 4096];
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&chunk[..count]);
    }
    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
}

fn assert_unknown_http_fee(run: &BudgetedRun) {
    let calls = run.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, "failed");
    assert_eq!(calls[0].error.as_deref(), Some("HTTP_429"));
    assert!(!calls[0].complete);
    assert_eq!(
        (calls[0].input, calls[0].output, calls[0].cost),
        (None, None, None)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_http_ordinary_429_stops_before_second_request_and_keeps_null_cost() {
    let run = BudgetedRun::new().await;
    let http = Http429Fixture::start().await;
    let events = ordinary_events_with_registry(&http.registry(), &run).await;
    assert!(
        matches!(events.last(), Some(ProviderEvent::Error { error: ProviderError::Config { message } }) if message.contains("BUDGET_USAGE_INCOMPLETE"))
    );
    http.assert_single_request(true);
    assert_unknown_http_fee(&run);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_http_summary_429_falls_back_locally_and_blocks_same_run_conversation() {
    let run = BudgetedRun::new().await;
    let http = Http429Fixture::start().await;
    let registry = http.registry();
    let summarizer = summarizer_with_registry(registry.clone());
    assert_local_fallback(&run, &summarizer);
    assert_eq!(summarizer.metrics().provider_failures, 1);
    http.assert_single_request(false);
    assert_unknown_http_fee(&run);
    let events = ordinary_events_with_registry(&registry, &run).await;
    assert!(
        matches!(events.last(), Some(ProviderEvent::Error { error: ProviderError::Config { message } }) if message.contains("BUDGET_USAGE_INCOMPLETE"))
    );
    http.assert_single_request(false);
    assert_unknown_http_fee(&run);
}

#[tokio::test]
async fn real_ledger_ordinary_unknown_429_refuses_retry_without_inventing_zero_cost() {
    let run = BudgetedRun::new().await;
    let fixture = ProtocolFixture::new(vec![limited(false), success()]);
    let events = ordinary_request_events(&fixture, &run).await;
    assert_eq!(fixture.calls(), 1);
    assert!(
        matches!(events.last(), Some(ProviderEvent::Error { error: ProviderError::Config { message } }) if message.contains("BUDGET_USAGE_INCOMPLETE"))
    );
    let calls = run.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, "failed");
    assert_eq!(calls[0].error.as_deref(), Some("HTTP_429"));
    assert!(!calls[0].complete);
    assert_eq!(
        (calls[0].input, calls[0].output, calls[0].cost),
        (None, None, None)
    );
    assert!(
        !run.db
            .read_task_budget(&run.task)
            .await
            .unwrap()
            .unwrap()
            .usage_complete
    );
}

#[tokio::test]
async fn real_ledger_ordinary_settled_failure_and_success_charge_both_requests() {
    let run = BudgetedRun::new().await;
    let fixture = ProtocolFixture::new(vec![limited(true), success()]);
    let events = ordinary_request_events(&fixture, &run).await;
    assert_eq!(fixture.calls(), 2);
    assert!(matches!(events.last(), Some(ProviderEvent::Finish { .. })));
    let calls = run.calls();
    assert_eq!(calls.len(), 2);
    assert!(
        calls
            .iter()
            .all(|call| call.complete && call.cost.is_some_and(|cost| cost > 0))
    );
    assert_eq!(
        calls.iter().filter(|call| call.status == "failed").count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.status == "completed")
            .count(),
        1
    );
    for call in &calls {
        assert_eq!((call.input, call.output), (Some(12), Some(4)));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(call.route.as_deref().unwrap()).unwrap()["kind"],
            "conversation"
        );
    }
    run.assert_live_charge_and_terminal_settlement(&calls).await;
}

#[tokio::test]
async fn real_ledger_ordinary_save_failures_never_publish_a_successful_finish() {
    for fail_start in [true, false] {
        let run = BudgetedRun::new().await;
        run.db.with_conn_blocking(move |conn| {
            conn.execute_batch(if fail_start {
                "CREATE TRIGGER ordinary_start_failure BEFORE INSERT ON llm_calls BEGIN SELECT RAISE(ABORT, 'fixture start failure'); END;"
            } else {
                "CREATE TRIGGER ordinary_finish_failure BEFORE UPDATE OF status ON llm_calls BEGIN SELECT RAISE(ABORT, 'fixture finish failure'); END;"
            })?;
            Ok(())
        }).unwrap();
        let fixture = ProtocolFixture::new(vec![success()]);
        let events = ordinary_request_events(&fixture, &run).await;
        assert_eq!(fixture.calls(), usize::from(!fail_start));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ProviderEvent::Finish { .. }))
        );
        assert!(
            matches!(events.last(), Some(ProviderEvent::Error { error: ProviderError::Config { message } }) if message.contains("LLM_LEDGER_"))
        );
        assert_eq!(run.calls().len(), usize::from(!fail_start));
        assert!(
            run.calls()
                .iter()
                .all(|call| call.status != "completed" && !call.complete)
        );
    }
}

#[tokio::test]
async fn real_ledger_unknown_429_refuses_a_second_physical_summary_request() {
    let run = BudgetedRun::new().await;
    let fixture = ProtocolFixture::new(vec![limited(false), success()]);
    let events = request_events(&registry(&fixture), &run.execution).await;
    assert_eq!(fixture.calls(), 1);
    assert!(
        matches!(events.last(), Some(ProviderEvent::Error { error: ProviderError::Config { message } }) if message.contains("BUDGET_USAGE_INCOMPLETE"))
    );
    let calls = run.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, "failed");
    assert_eq!(calls[0].error.as_deref(), Some("HTTP_429"));
    assert!(!calls[0].complete);
    assert_eq!(
        (calls[0].input, calls[0].output, calls[0].cost),
        (None, None, None)
    );
    assert!(
        !run.db
            .read_task_budget(&run.task)
            .await
            .unwrap()
            .unwrap()
            .usage_complete
    );
}

#[tokio::test]
async fn real_ledger_known_usage_allows_one_summary_retry_and_accounts_both_calls() {
    let run = BudgetedRun::new().await;
    let fixture = ProtocolFixture::new(vec![limited(true), success()]);
    let summary = LightModelSummarizer::summarize_scoped(
        &summarizer(&fixture),
        "Summarize",
        "local input",
        512,
        &run.execution,
    );
    assert!(
        summary
            .as_deref()
            .is_some_and(|text| text.contains("Durable summary"))
    );
    assert_eq!(fixture.calls(), 2);
    let calls = run.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls.iter().filter(|call| call.status == "failed").count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.status == "completed")
            .count(),
        1
    );
    for call in &calls {
        assert!(call.complete);
        assert_eq!((call.input, call.output), (Some(12), Some(4)));
        assert!(call.cost.is_some_and(|cost| cost > 0));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(call.route.as_deref().unwrap()).unwrap()["kind"],
            "summary"
        );
    }
    run.assert_live_charge_and_terminal_settlement(&calls).await;
}

#[tokio::test]
async fn unknown_pricing_summary_retry_retains_usage_without_inventing_cost() {
    for model in ["bailian/glm-5.3", "custom-summary-model"] {
        let run = BudgetedRun::new().await;
        let fixture = ProtocolFixture::for_model(model, vec![limited(true), success()]);
        let mut registry = ProviderRegistry::new();
        registry.register(fixture.provider_name(), fixture.clone(), vec![model.into()]);
        let summarizer =
            LlmSummarizer::with_timeout(Arc::new(registry), model, Duration::from_secs(3));
        let summary = LightModelSummarizer::summarize_scoped(
            &summarizer,
            "Summarize",
            "local input",
            512,
            &run.execution,
        );
        assert!(
            summary
                .as_deref()
                .is_some_and(|text| text.contains("Durable summary")),
            "model={model}"
        );
        assert_eq!(fixture.calls(), 2);
        let calls = run.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls.iter().filter(|call| call.status == "failed").count(),
            1
        );
        for call in &calls {
            assert!(call.complete);
            assert_eq!((call.input, call.output), (Some(12), Some(4)));
            assert_eq!(call.cost, None, "unknown price must remain unknown");
        }
        let live = run.db.find_run_by_id(&run.run).await.unwrap().unwrap();
        assert!(live.usage_complete);
        assert_eq!(live.total_tokens, 32);
        run.assert_terminal_settlement(&live.session_id, 32, 0)
            .await;
    }
}

#[tokio::test]
async fn unknown_pricing_cannot_bypass_already_exhausted_known_cost_budget() {
    let run = BudgetedRun::new().await;
    let run_id = run.run.clone();
    let limit = run.limits.cost_limit_nanos_usd.unwrap();
    run.db
        .with_conn_blocking(move |conn| {
            conn.execute(
                "UPDATE run_envelopes SET cost_nanos_usd=?1 WHERE id=?2",
                (limit, run_id),
            )?;
            Ok(())
        })
        .unwrap();
    let observer = DbLlmCallObserver::shared_budgeted(run.db.clone(), run.limits.clone(), 10, 10);
    let result = observer
        .call_started(zk_llm::LlmCallStarted {
            call_id: "unknown-at-exhausted-budget".into(),
            attribution: LlmExecutionAttribution::new(&run.task, &run.run, "summary"),
            provider: "local-fixture".into(),
            model: "custom-summary-model".into(),
            route: "{}".into(),
            provider_request_id: None,
        })
        .await;
    assert!(
        result
            .as_ref()
            .is_err_and(|error| error.contains("COST_BUDGET_EXHAUSTED")),
        "{result:?}"
    );
    assert!(run.calls().is_empty());
}

#[tokio::test]
async fn unknown_pricing_with_missing_usage_still_refuses_retry() {
    let run = BudgetedRun::new().await;
    let model = "custom-summary-model";
    let fixture = ProtocolFixture::for_model(model, vec![limited(false), success()]);
    let mut registry = ProviderRegistry::new();
    registry.register(fixture.provider_name(), fixture.clone(), vec![model.into()]);
    let summarizer = LlmSummarizer::with_timeout(Arc::new(registry), model, Duration::from_secs(3));
    assert!(
        LightModelSummarizer::summarize_scoped(
            &summarizer,
            "Summarize",
            "local input",
            512,
            &run.execution,
        )
        .is_none()
    );
    assert_eq!(fixture.calls(), 1);
    let calls = run.calls();
    assert_eq!(calls.len(), 1);
    assert!(!calls[0].complete);
    assert_eq!(calls[0].cost, None);
    assert!(
        !run.db
            .find_run_by_id(&run.run)
            .await
            .unwrap()
            .unwrap()
            .usage_complete
    );
}

#[tokio::test]
async fn real_ledger_summary_retries_at_most_once_even_when_both_failures_are_settled() {
    let run = BudgetedRun::new().await;
    let fixture = ProtocolFixture::new(vec![limited(true), limited(true), success()]);
    let result = LightModelSummarizer::summarize_scoped(
        &summarizer(&fixture),
        "Summarize",
        "local input",
        512,
        &run.execution,
    );
    assert!(result.is_none());
    assert_eq!(fixture.calls(), 2);
    assert_eq!(run.calls().len(), 2);
    assert!(
        run.calls()
            .iter()
            .all(|call| call.status == "failed" && call.complete)
    );
}

fn history() -> Vec<ChatMessage> {
    let mut messages = vec![
        ChatMessage::system("Respect saved permissions and preserve user text"),
        ChatMessage::user("Keep this exact original requirement: preserve working features."),
    ];
    messages.extend((0..8).map(|index| {
        ChatMessage::assistant(format!(
            "Intermediate note {index}: {}",
            "working notes ".repeat(600)
        ))
    }));
    messages.push(ChatMessage::user("Keep this final original question too."));
    messages.push(ChatMessage::assistant("The current task remains open."));
    messages
}

fn assert_local_fallback(run: &BudgetedRun, summarizer: &LlmSummarizer) {
    let original = history();
    let result = compact_messages_scoped(
        &original,
        MODEL,
        32_768,
        false,
        summarizer,
        Some(&run.execution),
    )
    .unwrap();
    assert_eq!(result.level, CompactLevel::KeyMessageSelection);
    assert!(result.tokens_saved() > 0);
    for user in original
        .iter()
        .filter(|message| message.role == zk_llm::Role::User)
    {
        assert!(result.messages.iter().any(|message| message == user));
    }
    assert!(result.messages.iter().any(|message| {
        message
            .content
            .contains("This is an omission notice, not evidence of completed work")
    }));
    assert!(
        !result
            .messages
            .iter()
            .any(|message| message.content.contains("Durable summary"))
    );
}

#[tokio::test]
async fn real_ledger_compaction_falls_back_locally_but_unknown_usage_still_blocks_payment() {
    let run = BudgetedRun::new().await;
    let fixture = ProtocolFixture::new(vec![limited(false), success()]);
    let summarizer = summarizer(&fixture);
    assert_local_fallback(&run, &summarizer);
    assert_eq!(fixture.calls(), 1);
    assert_eq!(summarizer.metrics().provider_failures, 1);
    assert!(!run.calls()[0].complete);
    let events = request_events(&registry(&fixture), &run.execution).await;
    assert!(
        matches!(events.last(), Some(ProviderEvent::Error { error: ProviderError::Config { message } }) if message.contains("BUDGET_USAGE_INCOMPLETE"))
    );
    assert_eq!(
        fixture.calls(),
        1,
        "local fallback never repairs unknown fees or authorizes a second request"
    );
}

#[tokio::test]
async fn real_ledger_finish_failure_cannot_be_presented_as_a_successful_summary() {
    let run = BudgetedRun::new().await;
    run.db.with_conn_blocking(|conn| {
        conn.execute_batch("CREATE TRIGGER reject_summary_finish BEFORE UPDATE OF status ON llm_calls WHEN NEW.status IN ('completed','failed','cancelled') BEGIN SELECT RAISE(ABORT, 'fixture ledger finish failure'); END;")?;
        Ok(())
    }).unwrap();
    let fixture = ProtocolFixture::new(vec![success()]);
    let summarizer = summarizer(&fixture);
    assert_local_fallback(&run, &summarizer);
    assert_eq!(fixture.calls(), 1);
    assert_eq!(summarizer.metrics().provider_failures, 1);
    let calls = run.calls();
    assert_eq!(calls.len(), 1);
    assert_ne!(calls[0].status, "completed");
    assert!(!calls[0].complete);
    assert_eq!(calls[0].cost, None);
}

#[tokio::test]
async fn real_ledger_start_failure_prevents_provider_dispatch_and_keeps_local_fallback() {
    let run = BudgetedRun::new().await;
    run.db.with_conn_blocking(|conn| {
        conn.execute_batch("CREATE TRIGGER reject_summary_start BEFORE INSERT ON llm_calls BEGIN SELECT RAISE(ABORT, 'fixture ledger start failure'); END;")?;
        Ok(())
    }).unwrap();
    let fixture = ProtocolFixture::new(vec![success()]);
    let summarizer = summarizer(&fixture);
    assert_local_fallback(&run, &summarizer);
    assert_eq!(fixture.calls(), 0);
    assert_eq!(summarizer.metrics().provider_failures, 1);
    assert!(run.calls().is_empty());
}
