//! zk-engine 引擎集成测试（零网络）：Phase 1 单轮回归 + 2.2 多轮工具循环
//! 与 interrupt。
//!
//! `MockChatProvider` 手写事件流喂入，`RecordingSink` 录制下行序列；
//! 消息 JSON 形状断言以 zk-protocol serde 输出为唯一权威。

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use zk_core::FeatureFlags;
use zk_db::model::{MessageRole, NewMessage, StoredBlock};
use zk_db::{CasOutcome, CreateTaskWithRun, Db, MemoryTarget, MemoryUpsert, TaskBudgetLimits};
use zk_engine::coordinator::build_coordinator_prompt;
use zk_engine::engine::{SubAgentRunConfig, SubAgentRunOutcome};
use zk_engine::{
    Admission, AdmissionRequest, AgentStatus, CLEARED_MESSAGE, ConversationRunOptions,
    ConversationService, CoordinatorService, Engine, MAX_TOKENS_RECOVERY_MESSAGE, MessageSink,
    RootTaskBudgetPolicy, ToolAdmission, calculate_token_warning_state,
};
use zk_llm::{
    ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent, ProviderRegistry, Role,
    VisionProviderView,
};
use zk_protocol::model::Usage;
use zk_protocol::{Attachment, ClientMessage, ServerMessage};
use zk_tools::{
    EVIDENCE_RECEIPT_SCHEMA_VERSION, EchoTool, EvidenceReceipt, EvidenceReceiptItem,
    EvidenceReceiptVerdict, SearchBackend, SearchBackendError, SearchRequest, SearchResult, Tool,
    ToolContext, ToolOutput, ToolRegistry, WebFetchError, WebFetchPort, WebFetchRequest,
    WebFetchResponse, WebFetchTool, WebSearchTool, WriteFileTool,
};

/// 下行录制桩（顺序即引擎推送顺序）。
#[derive(Default)]
struct RecordingSink {
    pushed: Mutex<Vec<(String, ServerMessage)>>,
}

impl MessageSink for RecordingSink {
    fn push<'a>(&'a self, session_id: &'a str, message: ServerMessage) -> BoxFuture<'a, ()> {
        self.pushed
            .lock()
            .expect("sink lock")
            .push((session_id.to_owned(), message));
        Box::pin(futures::future::ready(()))
    }
}

impl RecordingSink {
    fn kinds(&self) -> Vec<&'static str> {
        self.pushed
            .lock()
            .expect("sink lock")
            .iter()
            .map(|(_, message)| message.kind())
            .collect()
    }

    fn json_at(&self, index: usize) -> serde_json::Value {
        let pushed = self.pushed.lock().expect("sink lock");
        serde_json::to_value(&pushed[index].1).expect("serialize server message")
    }

    fn session_at(&self, index: usize) -> String {
        self.pushed.lock().expect("sink lock")[index].0.clone()
    }
}

/// 脚本化 Provider：每次 `chat_stream` 弹出下一个预置流并录制请求。
struct MockProvider {
    scripts: Mutex<VecDeque<Result<BoxStream<'static, ProviderEvent>, ProviderError>>>,
    requests: Mutex<Vec<ChatRequest>>,
}

impl MockProvider {
    fn new(scripts: Vec<Result<BoxStream<'static, ProviderEvent>, ProviderError>>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn request_count(&self) -> usize {
        self.requests.lock().expect("requests lock").len()
    }

    fn request_at(&self, index: usize) -> ChatRequest {
        self.requests.lock().expect("requests lock")[index].clone()
    }
}

impl ChatProvider for MockProvider {
    fn provider_name(&self) -> &'static str {
        "mock"
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        _cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        self.requests.lock().expect("requests lock").push(request);
        self.scripts
            .lock()
            .expect("scripts lock")
            .pop_front()
            .expect("unexpected extra chat_stream call")
    }
}

/// Test provider which corrupts only the owning Task's usage authority after
/// pre-call admission but before returning a tool-bearing response. This
/// distinguishes the post-turn three-layer integrity gate from the former
/// Run-only check.
struct PoisonOwningTaskUsageProvider {
    db: Db,
    calls: std::sync::atomic::AtomicUsize,
}

impl PoisonOwningTaskUsageProvider {
    fn new(db: Db) -> Self {
        Self {
            db,
            calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl ChatProvider for PoisonOwningTaskUsageProvider {
    fn provider_name(&self) -> &'static str {
        "poison-owning-task-usage"
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        _cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let task_id = request
            .execution
            .as_ref()
            .expect("engine supplies execution attribution")
            .task_id
            .clone();
        self.db
            .with_conn_blocking(move |connection| {
                let updated = connection
                    .execute("UPDATE tasks SET usage_complete=0 WHERE id=?1", [task_id])?;
                assert_eq!(updated, 1);
                Ok(())
            })
            .map_err(|error| ProviderError::Config {
                message: format!("test poison failed: {error}"),
            })?;
        events(vec![
            ProviderEvent::ToolUseStart {
                id: "poisoned-usage-tool".to_owned(),
                name: "Echo".to_owned(),
            },
            ProviderEvent::ToolInputDelta {
                id: "poisoned-usage-tool".to_owned(),
                delta: r#"{"text":"must-not-run"}"#.to_owned(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: Some(usage(3, 2)),
            },
        ])
    }
}

/// Deterministically holds the engine after the assistant/tool draft is
/// durable but before the guarded preparing -> running CAS.
struct BlockingToolAdmission {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl BlockingToolAdmission {
    fn new() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }
}

impl ToolAdmission for BlockingToolAdmission {
    fn admit<'a>(&'a self, request: AdmissionRequest<'a>) -> BoxFuture<'a, Admission> {
        let input = request.input.clone();
        Box::pin(async move {
            self.entered.notify_one();
            self.release.notified().await;
            Admission::Allow {
                execution_input: input,
            }
        })
    }
}

struct CountingTool {
    executions: Arc<std::sync::atomic::AtomicUsize>,
}

impl Tool for CountingTool {
    fn name(&self) -> &'static str {
        "Counted"
    }

    fn description(&self) -> &'static str {
        "test-only execution counter"
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {"value": {"type": "string"}},
            "required": ["value"]
        })
    }

    fn execute(&self, _input: serde_json::Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        self.executions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(futures::future::ready(ToolOutput::ok("executed")))
    }
}

/// Catalog-only stand-in used to prove request-level filtering reaches the
/// Coordinator prompt builder. It must never be executed in these tests.
struct CatalogAgentTool;

impl Tool for CatalogAgentTool {
    fn name(&self) -> &'static str {
        "Agent"
    }

    fn description(&self) -> &'static str {
        "test-only Agent catalog entry"
    }

    fn parameters(&self) -> serde_json::Value {
        json!({ "type": "object" })
    }

    fn execute(&self, _input: serde_json::Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        panic!("CatalogAgentTool must not execute")
    }
}

struct FixtureSearchBackend;

impl SearchBackend for FixtureSearchBackend {
    fn search(
        &self,
        _request: SearchRequest,
    ) -> BoxFuture<'_, Result<Vec<SearchResult>, SearchBackendError>> {
        Box::pin(futures::future::ready(Ok(vec![SearchResult {
            title: "Durable agents".to_owned(),
            url: "https://example.com/agents#overview".to_owned(),
            snippet: "A stable search finding.".to_owned(),
            source: "fixture-search".to_owned(),
            rank: 99,
        }])))
    }
}

struct FixtureFetchPort;

impl WebFetchPort for FixtureFetchPort {
    fn fetch(
        &self,
        _request: WebFetchRequest,
    ) -> BoxFuture<'_, Result<WebFetchResponse, WebFetchError>> {
        Box::pin(futures::future::ready(Ok(WebFetchResponse {
            final_url: "https://example.org/report".to_owned(),
            status: 200,
            content_type: "text/html; charset=utf-8".to_owned(),
            body: b"<html><head><title>Agent report</title></head><body><p>A fetched finding.</p></body></html>".to_vec(),
            truncated: false,
        })))
    }
}

#[derive(Clone, Copy)]
struct FixtureVerifyJourney;

impl Tool for FixtureVerifyJourney {
    fn name(&self) -> &'static str {
        "VerifyJourney"
    }

    fn description(&self) -> &'static str {
        "test-only verifier with a bounded evidence receipt"
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {"outcome": {"type": "string"}},
            "required": ["outcome"]
        })
    }

    fn execute(&self, input: serde_json::Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let outcome = input
                .get("outcome")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("invalid");
            if outcome == "invalid" {
                return ToolOutput::ok("completed without a receipt");
            }
            let failed = outcome == "failed";
            let receipt = EvidenceReceipt {
                schema_version: EVIDENCE_RECEIPT_SCHEMA_VERSION,
                kind: "browser_journey".into(),
                claim: Some("page is usable".into()),
                verdict: if failed {
                    EvidenceReceiptVerdict::Failed
                } else {
                    EvidenceReceiptVerdict::Verified
                },
                observed_at: "2026-09-09T00:00:00.000000Z".into(),
                items: vec![EvidenceReceiptItem {
                    item_type: "browser_journey_step".into(),
                    summary: Some(if failed {
                        "assert_text: failed".into()
                    } else {
                        "assert_text: passed".into()
                    }),
                    blob_sha256: None,
                    meta: Some(json!({"action":"assert_text","ok":!failed})),
                    sort_order: 0,
                }],
            };
            ToolOutput {
                content: if failed {
                    "Browser journey failed".into()
                } else {
                    "Browser journey passed".into()
                },
                is_error: failed,
                metadata: Some(json!({
                    "structuredResult": {
                        "passed": !failed,
                        "evidence": receipt,
                    }
                })),
            }
        })
    }
}

// Result 包装是刻意的：与 setup 的脚本槽位类型（Ok=流 / Err=建立期失败）对齐。
#[expect(clippy::unnecessary_wraps, reason = "匹配脚本槽位 Result 形态")]
fn events(seq: Vec<ProviderEvent>) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
    Ok(stream::iter(seq).boxed())
}

fn usage(input: i64, output: i64) -> Usage {
    Usage {
        input_tokens: input,
        output_tokens: output,
        ..Usage::default()
    }
}

/// 内存库 + 会话 + 引擎装配。
async fn setup(
    scripts: Vec<Result<BoxStream<'static, ProviderEvent>, ProviderError>>,
) -> (
    Arc<Engine>,
    Arc<MockProvider>,
    Arc<RecordingSink>,
    Db,
    String,
) {
    setup_with_model(scripts, "qwen3.8-max-0902").await
}

/// 内存库 + 指定模型的会话 + 引擎装配（输出预算/费率随模型能力表变化的
/// 用例专用）。
async fn setup_with_model(
    scripts: Vec<Result<BoxStream<'static, ProviderEvent>, ProviderError>>,
    model: &str,
) -> (
    Arc<Engine>,
    Arc<MockProvider>,
    Arc<RecordingSink>,
    Db,
    String,
) {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session(model, "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(scripts));
    let sink = Arc::new(RecordingSink::default());
    let engine = Arc::new(Engine::new(
        db.clone(),
        Arc::clone(&provider) as Arc<dyn ChatProvider>,
        Arc::clone(&sink) as Arc<dyn MessageSink>,
    ));
    (engine, provider, sink, db, session.id)
}

async fn run(engine: &Arc<Engine>, session_id: &str, text: &str) {
    Arc::clone(engine)
        .run_user_message(session_id.to_owned(), text.to_owned())
        .await;
}

fn trusted_url_attachment(index: usize) -> Attachment {
    Attachment {
        kind: "image".into(),
        path: None,
        media_type: Some("image/png".into()),
        base64_data: None,
        url: Some(format!("https://trusted.example.test/image-{index}.png")),
    }
}

async fn run_with_url_images(
    engine: &Arc<Engine>,
    sink: &RecordingSink,
    session_id: &str,
    image_count: usize,
    terminal_kind: &'static str,
) {
    engine.handle_client_message(
        session_id,
        ClientMessage::UserMessage {
            text: "inspect the image".into(),
            attachments: Some((0..image_count).map(trusted_url_attachment).collect()),
            references: None,
        },
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if sink.kinds().contains(&terminal_kind) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("engine should reach a terminal message");
}

fn registry_with_models(provider: Arc<MockProvider>, models: &[&str]) -> Arc<ProviderRegistry> {
    let mut registry = ProviderRegistry::new();
    registry.register(
        "zhipu",
        provider as Arc<dyn ChatProvider>,
        models.iter().map(|model| (*model).to_owned()).collect(),
    );
    Arc::new(registry)
}

fn engine_with_vision_registry(
    db: Db,
    provider: &Arc<MockProvider>,
    sink: &Arc<RecordingSink>,
    models: &[&str],
) -> Arc<Engine> {
    let registry = registry_with_models(Arc::clone(provider), models);
    Arc::new(
        Engine::new(
            db,
            Arc::clone(&registry) as Arc<dyn ChatProvider>,
            Arc::clone(sink) as Arc<dyn MessageSink>,
        )
        .with_trusted_image_url(Arc::new(|_| true))
        .with_vision_provider_view(registry as Arc<dyn VisionProviderView>),
    )
}

#[tokio::test]
async fn image_request_routes_glm_5_3_to_configured_glm_5_3_flash_for_this_run() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("glm-5.3", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(vec![events(vec![
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: None,
        },
    ])]));
    let sink = Arc::new(RecordingSink::default());
    let engine =
        engine_with_vision_registry(db.clone(), &provider, &sink, &["glm-5.3", "glm-5.3-flash"]);

    run_with_url_images(&engine, &sink, &session.id, 1, "session_list_updated").await;

    assert_eq!(provider.request_count(), 1);
    let request = provider.request_at(0);
    assert_eq!(request.model, "glm-5.3-flash");
    assert_eq!(
        request.messages.last().expect("user message").images.len(),
        1
    );
    assert!(
        request
            .system_text()
            .expect("system prompt")
            .contains("你由模型 glm-5.3-flash 驱动")
    );
    assert_eq!(sink.kinds().first(), Some(&"model_routed"));
    let routed = sink.json_at(0);
    assert_eq!(routed["originalModel"], "glm-5.3");
    assert_eq!(routed["routedModel"], "glm-5.3-flash");
    assert_eq!(routed["routedModelName"], "GLM-5.3-Flash");
    assert_eq!(
        routed["reason"],
        "当前模型不支持图片，已自动切换到 GLM-5.3-Flash"
    );
    let persisted = db
        .get_session(&session.id)
        .await
        .expect("get session")
        .expect("session remains");
    assert_eq!(
        persisted.model, "glm-5.3",
        "routing must not mutate session model"
    );
}

#[tokio::test]
async fn image_request_without_configured_vision_model_fails_before_provider() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("glm-5.3", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(Vec::new()));
    let sink = Arc::new(RecordingSink::default());
    let empty_registry = Arc::new(ProviderRegistry::new());
    let engine = Arc::new(
        Engine::new(
            db.clone(),
            Arc::clone(&provider) as Arc<dyn ChatProvider>,
            Arc::clone(&sink) as Arc<dyn MessageSink>,
        )
        .with_trusted_image_url(Arc::new(|_| true))
        .with_vision_provider_view(empty_registry as Arc<dyn VisionProviderView>),
    );

    run_with_url_images(&engine, &sink, &session.id, 1, "error").await;

    assert_eq!(provider.request_count(), 0);
    assert_eq!(sink.kinds(), vec!["error"]);
    assert_eq!(sink.json_at(0)["code"], "ATTACHMENT_MODEL_UNSUPPORTED");
    let messages = db
        .list_messages(&session.id, None, 10)
        .await
        .expect("list messages")
        .expect("session exists");
    assert!(
        messages.messages.is_empty(),
        "rejected input must not persist"
    );
}

#[tokio::test]
async fn native_vision_model_keeps_model_and_emits_no_route_event() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("glm-5.3-flash", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(vec![events(vec![
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: None,
        },
    ])]));
    let sink = Arc::new(RecordingSink::default());
    let engine = engine_with_vision_registry(db, &provider, &sink, &["glm-5.3-flash"]);

    run_with_url_images(&engine, &sink, &session.id, 1, "session_list_updated").await;

    assert_eq!(provider.request_at(0).model, "glm-5.3-flash");
    assert!(!sink.kinds().contains(&"model_routed"));
}

#[tokio::test]
async fn qwen_38_flash_accepts_the_frontend_twenty_image_limit() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-flash", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(vec![events(vec![
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: None,
        },
    ])]));
    let sink = Arc::new(RecordingSink::default());
    let engine = engine_with_vision_registry(db, &provider, &sink, &["qwen3.8-flash"]);

    run_with_url_images(&engine, &sink, &session.id, 20, "session_list_updated").await;

    assert_eq!(provider.request_count(), 1);
    assert_eq!(
        provider
            .request_at(0)
            .messages
            .last()
            .expect("user message")
            .images
            .len(),
        20
    );
}

#[tokio::test]
async fn qwen_38_flash_rejects_the_twenty_first_image_before_provider_call() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-flash", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(Vec::new()));
    let sink = Arc::new(RecordingSink::default());
    let engine = engine_with_vision_registry(db, &provider, &sink, &["qwen3.8-flash"]);

    run_with_url_images(&engine, &sink, &session.id, 21, "error").await;

    assert_eq!(provider.request_count(), 0);
    assert_eq!(sink.kinds(), vec!["error"]);
    assert_eq!(sink.json_at(0)["code"], "ATTACHMENT_COUNT_EXCEEDED");
    assert_eq!(
        sink.json_at(0)["message"],
        "Model qwen3.8-flash accepts at most 20 images"
    );
}

#[tokio::test]
async fn routed_model_image_limit_is_enforced_before_event_or_provider_call() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("glm-5.3", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(Vec::new()));
    let sink = Arc::new(RecordingSink::default());
    let engine = engine_with_vision_registry(db, &provider, &sink, &["glm-5.3", "glm-5.3-flash"]);

    // GLM-5.3 自身 max_images=0；路由后的 GLM-5.3-Flash max_images=50。
    run_with_url_images(&engine, &sink, &session.id, 51, "error").await;

    assert_eq!(provider.request_count(), 0);
    assert_eq!(sink.kinds(), vec!["error"]);
    assert_eq!(sink.json_at(0)["code"], "ATTACHMENT_COUNT_EXCEEDED");
    assert_eq!(
        sink.json_at(0)["message"],
        "Model glm-5.3-flash accepts at most 50 images"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn single_turn_streams_and_persists() {
    let (engine, provider, sink, db, sid) = setup(vec![events(vec![
        ProviderEvent::TextDelta {
            text: "Hello".into(),
        },
        ProviderEvent::TextDelta {
            text: " world".into(),
        },
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(12, 5)),
        },
    ])])
    .await;
    run(&engine, &sid, "hi").await;

    assert_eq!(
        sink.kinds(),
        vec![
            "stream_delta",
            "stream_delta",
            // Batch 0 Step 0-6：Finish 携带 usage → push_cost_update 落于此。
            "cost_update",
            "message_complete",
            "session_list_updated"
        ]
    );
    assert_eq!(sink.session_at(0), sid);
    assert_eq!(sink.json_at(0)["delta"], "Hello");
    // Batch 0 Step 0-6：cost_update 落于索引 2，message_complete 顺移至 3。
    let complete = sink.json_at(3);
    assert_eq!(complete["usage"]["inputTokens"], 12);
    assert_eq!(complete["usage"]["outputTokens"], 5);
    assert_eq!(complete["stopReason"], "end_turn");
    assert_eq!(complete["sessionId"], sid.as_str());
    assert!(complete["runId"].as_str().is_some_and(|id| id.len() == 36));
    // 首轮无历史：替换锚点缺省（字段跳过序列化）。
    assert!(complete.get("replaceAfterMessageId").is_none());
    let committed = complete["committedMessages"].as_array().expect("committed");
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0]["type"], "user");
    assert_eq!(committed[0]["content"][0]["text"], "hi");
    assert_eq!(committed[1]["type"], "assistant");
    assert_eq!(committed[1]["content"][0]["text"], "Hello world");
    assert_eq!(committed[1]["stopReason"], "end_turn");
    assert_eq!(committed[1]["usage"]["inputTokens"], 12);
    let run_id = complete["runId"].as_str().expect("run id");
    let workbench = db
        .find_workbench(run_id)
        .await
        .expect("workbench query")
        .expect("production workbench binding");
    assert_eq!(
        workbench.binding.request_message_id,
        committed[0]["uuid"].as_str().expect("request id")
    );
    assert_eq!(
        workbench.binding.result_message_id.as_deref(),
        committed[1]["uuid"].as_str()
    );
    assert_eq!(workbench.criteria.len(), 1);
    assert_eq!(workbench.criteria[0].source_text, "hi");
    assert_eq!(workbench.criteria[0].status, "not_verified");

    // 请求形状：system prompt = Batch 0 静态段 + 环境信息段、纯用户文本、
    // 会话模型。
    assert_eq!(provider.request_count(), 1);
    let request = provider.request_at(0);
    assert_eq!(request.model, "qwen3.8-max-0902");
    assert_eq!(request.thinking, zk_llm::ThinkingMode::Adaptive);
    // 拼装结果整体逐字互锁（任务 #54：6 个 P0 静态段 + env_info，段序与
    // 分隔符对照旧 `SystemPromptBuilder`）；环境信息段必须逐字告知会话工作
    // 目录与驱动模型（任务 #49：缺失时模型会按容器习惯发出
    // `/mnt/user-data/...` 绝对路径）。
    assert!(request.system_prompt.is_none());
    assert_eq!(request.system_segments.len(), 2);
    assert!(request.system_segments[0].cache_control);
    assert!(!request.system_segments[1].cache_control);
    assert!(!request.system_segments[0].text.contains("/tmp"));
    let system_prompt = request.system_text().expect("system prompt");
    assert!(system_prompt.starts_with("你是一个交互式 AI 编码助手"));
    assert!(system_prompt.contains("# 执行任务"));
    assert!(system_prompt.contains(" - 主工作目录：/tmp\n"));
    assert!(system_prompt.contains(" - 你由模型 qwen3.8-max-0902 驱动。\n"));
    assert!(system_prompt.contains("`/tmp/.zk/scratchpad`"));
    assert_eq!(request.messages.len(), 1);
    assert_eq!(request.messages[0].content, "hi");

    // 落库形状读回逐字段断言。
    let page = db
        .list_messages(&sid, None, 10)
        .await
        .expect("list")
        .expect("session exists");
    assert_eq!(page.messages.len(), 2);
    let user = &page.messages[0];
    assert_eq!(user.role, MessageRole::User);
    assert_eq!(user.seq_num, 1);
    assert_eq!(user.content, vec![StoredBlock::Text { text: "hi".into() }]);
    assert_eq!(user.stop_reason, None);
    let assistant = &page.messages[1];
    assert_eq!(assistant.role, MessageRole::Assistant);
    assert_eq!(assistant.seq_num, 2);
    assert_eq!(
        assistant.content,
        vec![StoredBlock::Text {
            text: "Hello world".into()
        }]
    );
    assert_eq!(assistant.stop_reason.as_deref(), Some("end_turn"));
    assert_eq!(assistant.input_tokens, 12);
    assert_eq!(assistant.output_tokens, 5);

    // Production wiring invariant: returning `message_complete` means the root
    // logical Task, physical Run and immutable result are already queryable and
    // the successful result is bound to the real persisted Assistant message.
    let task = db
        .find_runtime_task_by_id(run_id)
        .await
        .expect("root task query")
        .expect("root task must exist");
    assert_eq!(task.id, run_id);
    assert_eq!(task.current_run_id.as_deref(), Some(run_id));
    assert_eq!(task.status, zk_db::TaskStatus::Succeeded);
    let persisted_run = db
        .find_run_by_id(run_id)
        .await
        .expect("root run query")
        .expect("root run must exist");
    assert_eq!(persisted_run.status, "completed");
    let result = db
        .read_task_result(run_id, None, 0, zk_db::INLINE_RESULT_LIMIT)
        .await
        .expect("root result query")
        .expect("root result must exist");
    assert_eq!(result.result.status, zk_db::ResultStatus::Complete);
    assert_eq!(result.content, "Hello world");
    assert_eq!(
        result.result.final_message_id.as_deref(),
        Some(assistant.id.as_str())
    );
}

#[tokio::test]
async fn production_root_defaults_to_unlimited_spend_and_records_provider_usage() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(vec![events(vec![
        ProviderEvent::TextDelta {
            text: "budgeted response".into(),
        },
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(12, 5)),
        },
    ])]));
    let sink = Arc::new(RecordingSink::default());
    let registry = registry_with_models(Arc::clone(&provider), &["qwen3.8-max-0902"]);
    let engine = Arc::new(
        Engine::new(
            db.clone(),
            registry as Arc<dyn ChatProvider>,
            Arc::clone(&sink) as Arc<dyn MessageSink>,
        )
        .with_root_task_budget_policy(RootTaskBudgetPolicy::default()),
    );

    run(&engine, &session.id, "use a production budget").await;

    let events = sink
        .pushed
        .lock()
        .expect("sink lock")
        .iter()
        .map(|(_, event)| serde_json::to_value(event).expect("serialize event"))
        .collect::<Vec<_>>();
    assert_eq!(provider.request_count(), 1, "events: {events:?}");
    let complete_index = sink
        .kinds()
        .iter()
        .position(|kind| *kind == "message_complete")
        .expect("message_complete");
    let complete = sink.json_at(complete_index);
    let run_id = complete["runId"].as_str().expect("run id");
    let budget = db
        .read_task_budget(run_id)
        .await
        .expect("budget query")
        .expect("root budget");
    assert_eq!(budget.token_limit, None);
    assert_eq!(budget.cost_limit_nanos_usd, None);
    assert!(budget.deadline_at_ms.is_some());

    let call: (i64, String, i64, i64, i64) = db
        .with_conn_blocking({
            let run_id = run_id.to_owned();
            move |conn| {
                conn.query_row(
                    "SELECT COUNT(*),MIN(status),MIN(usage_complete),\
                            MIN(input_tokens),MIN(output_tokens)\
                     FROM llm_calls WHERE run_id=?1",
                    [run_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .map_err(Into::into)
            }
        })
        .expect("physical call ledger");
    assert_eq!(call, (1, "completed".into(), 1, 12, 5));
}

#[tokio::test]
async fn exhausted_root_cost_budget_rejects_before_provider_execution() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(Vec::new()));
    let sink = Arc::new(RecordingSink::default());
    let registry = registry_with_models(Arc::clone(&provider), &["qwen3.8-max-0902"]);
    let engine = Arc::new(
        Engine::new(
            db.clone(),
            registry as Arc<dyn ChatProvider>,
            Arc::clone(&sink) as Arc<dyn MessageSink>,
        )
        .with_root_task_budget_policy(RootTaskBudgetPolicy {
            token_limit: Some(1_000_000),
            cost_limit_nanos_usd: Some(1),
            deadline: Duration::from_mins(1),
        }),
    );

    run(&engine, &session.id, "must fail closed").await;

    assert_eq!(provider.request_count(), 0);
    assert!(sink.kinds().contains(&"error"));
    let complete_index = sink
        .kinds()
        .iter()
        .position(|kind| *kind == "message_complete")
        .expect("message_complete");
    let complete = sink.json_at(complete_index);
    assert_eq!(complete["stopReason"], "budget_exhausted");
    let run_id = complete["runId"].as_str().expect("run id");
    let result = db
        .read_task_result(run_id, None, 0, zk_db::INLINE_RESULT_LIMIT)
        .await
        .expect("result query")
        .expect("partial result");
    assert_eq!(result.result.status, zk_db::ResultStatus::Partial);
    assert_eq!(
        result.result.error_code.as_deref(),
        Some("COST_BUDGET_EXHAUSTED")
    );
    let physical_calls: i64 = db
        .with_conn_blocking({
            let run_id = run_id.to_owned();
            move |conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM llm_calls WHERE run_id=?1",
                    [run_id],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            }
        })
        .expect("physical call count");
    assert_eq!(physical_calls, 0);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn missing_post_turn_usage_fails_before_assistant_or_tool_side_effects() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(vec![events(vec![
        ProviderEvent::ToolUseStart {
            id: "usage-gap-call".into(),
            name: "Echo".into(),
        },
        ProviderEvent::ToolInputDelta {
            id: "usage-gap-call".into(),
            delta: "{\"text\":\"must-not-run\"}".into(),
        },
        ProviderEvent::Finish {
            finish_reason: FinishReason::ToolUse,
            usage: None,
        },
    ])]));
    let sink = Arc::new(RecordingSink::default());
    let provider_registry = registry_with_models(Arc::clone(&provider), &["qwen3.8-max-0902"]);
    let mut tools = ToolRegistry::new();
    tools.register(Arc::new(EchoTool));
    let engine = Arc::new(
        Engine::with_tools(
            db.clone(),
            provider_registry as Arc<dyn ChatProvider>,
            Arc::clone(&sink) as Arc<dyn MessageSink>,
            Arc::new(tools),
        )
        .with_root_task_budget_policy(RootTaskBudgetPolicy {
            token_limit: Some(1_000_000),
            cost_limit_nanos_usd: Some(4_000_000_000),
            deadline: Duration::from_mins(1),
        }),
    );

    run(
        &engine,
        &session.id,
        "do not execute without billable usage",
    )
    .await;

    assert_eq!(provider.request_count(), 1);
    let kinds = sink.kinds();
    assert!(kinds.contains(&"error"));
    assert!(kinds.contains(&"message_complete"));
    for forbidden in [
        "cost_update",
        "tool_use_start",
        "tool_use_input",
        "tool_result",
    ] {
        assert!(
            !kinds.contains(&forbidden),
            "unexpected {forbidden}: {kinds:?}"
        );
    }
    let error_index = kinds
        .iter()
        .position(|kind| *kind == "error")
        .expect("error event");
    assert_eq!(sink.json_at(error_index)["code"], "BUDGET_USAGE_INCOMPLETE");
    let complete_index = kinds
        .iter()
        .position(|kind| *kind == "message_complete")
        .expect("message_complete");
    let complete = sink.json_at(complete_index);
    assert_eq!(complete["stopReason"], "error");
    let run_id = complete["runId"].as_str().expect("run id");

    let page = db
        .list_messages(&session.id, None, 10)
        .await
        .expect("message query")
        .expect("session exists");
    assert_eq!(
        page.messages.len(),
        1,
        "assistant tool call must not persist"
    );
    assert_eq!(page.messages[0].role, MessageRole::User);
    let tool_invocations: i64 = db
        .with_conn_blocking(|conn| {
            conn.query_row("SELECT COUNT(*) FROM tool_invocations", [], |row| {
                row.get(0)
            })
            .map_err(Into::into)
        })
        .expect("tool invocation count");
    assert_eq!(tool_invocations, 0);

    let result = db
        .read_task_result(run_id, None, 0, zk_db::INLINE_RESULT_LIMIT)
        .await
        .expect("result query")
        .expect("failed result");
    assert_eq!(result.result.status, zk_db::ResultStatus::Error);
    assert_eq!(
        result.result.error_code.as_deref(),
        Some("BUDGET_USAGE_INCOMPLETE")
    );
    assert!(result.content.contains("BUDGET_USAGE_INCOMPLETE"));
    let persisted_run = db
        .find_run_by_id(run_id)
        .await
        .expect("run query")
        .expect("run exists");
    assert_eq!(persisted_run.status, "failed");
    assert_eq!(
        persisted_run.exit_reason.as_deref(),
        Some(zk_db::run::EXIT_INTERNAL_ERROR)
    );
    assert!(!persisted_run.usage_complete);
    assert!(
        persisted_run
            .error_summary
            .as_deref()
            .is_some_and(|summary| summary.contains("BUDGET_USAGE_INCOMPLETE"))
    );
}

#[tokio::test]
async fn owning_task_usage_poison_fails_before_assistant_or_tool_side_effects() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(PoisonOwningTaskUsageProvider::new(db.clone()));
    let sink = Arc::new(RecordingSink::default());
    let mut tools = ToolRegistry::new();
    tools.register(Arc::new(EchoTool));
    let engine = Arc::new(Engine::with_tools(
        db.clone(),
        provider.clone() as Arc<dyn ChatProvider>,
        Arc::clone(&sink) as Arc<dyn MessageSink>,
        Arc::new(tools),
    ));

    run(&engine, &session.id, "the poisoned Task must block Echo").await;

    assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let kinds = sink.kinds();
    assert!(kinds.contains(&"error"));
    assert!(!kinds.contains(&"tool_use_start"));
    assert!(!kinds.contains(&"tool_result"));
    let error_index = kinds
        .iter()
        .position(|kind| *kind == "error")
        .expect("usage error");
    assert_eq!(sink.json_at(error_index)["code"], "BUDGET_USAGE_INCOMPLETE");
    let complete_index = kinds
        .iter()
        .position(|kind| *kind == "message_complete")
        .expect("durable terminal completion");
    let complete = sink.json_at(complete_index);
    assert_eq!(complete["stopReason"], "error");
    let run_id = complete["runId"].as_str().expect("Run identity");

    let run = db
        .find_run_by_id(run_id)
        .await
        .expect("read Run")
        .expect("Run exists");
    assert!(
        run.usage_complete,
        "the provider poisoned only Task authority, not Run authority"
    );
    let task = db
        .find_runtime_task_by_id(&run.task_id)
        .await
        .expect("read Task")
        .expect("Task exists");
    assert!(!task.usage_complete);
    assert_eq!(task.status, zk_db::TaskStatus::Failed);
    let result = db
        .read_task_result(&task.id, None, 0, zk_db::INLINE_RESULT_LIMIT)
        .await
        .expect("read TaskResult")
        .expect("failed TaskResult exists");
    assert_eq!(result.result.status, zk_db::ResultStatus::Error);
    assert_eq!(
        result.result.error_code.as_deref(),
        Some("BUDGET_USAGE_INCOMPLETE")
    );
    let messages = db
        .list_messages(&session.id, None, 10)
        .await
        .expect("list transcript")
        .expect("session exists");
    assert_eq!(messages.messages.len(), 1);
    let invocations: i64 = db
        .with_conn_blocking(|connection| {
            connection
                .query_row("SELECT COUNT(*) FROM tool_invocations", [], |row| {
                    row.get(0)
                })
                .map_err(Into::into)
        })
        .expect("tool invocation count");
    assert_eq!(invocations, 0);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn root_usage_poison_after_assistant_commit_is_stopped_by_guarded_tool_start() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(vec![events(vec![
        ProviderEvent::ToolUseStart {
            id: "late-poison-tool".into(),
            name: "Counted".into(),
        },
        ProviderEvent::ToolInputDelta {
            id: "late-poison-tool".into(),
            delta: r#"{"value":"must-not-run"}"#.into(),
        },
        ProviderEvent::ToolUseStart {
            id: "late-poison-sibling-tool".into(),
            name: "Counted".into(),
        },
        ProviderEvent::ToolInputDelta {
            id: "late-poison-sibling-tool".into(),
            delta: r#"{"value":"must-also-not-run"}"#.into(),
        },
        ProviderEvent::Finish {
            finish_reason: FinishReason::ToolUse,
            usage: Some(usage(5, 2)),
        },
    ])]));
    let sink = Arc::new(RecordingSink::default());
    let admission = Arc::new(BlockingToolAdmission::new());
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(Arc::new(CountingTool {
        executions: Arc::clone(&executions),
    }));
    let engine = Arc::new(Engine::with_admission(
        db.clone(),
        provider as Arc<dyn ChatProvider>,
        Arc::clone(&sink) as Arc<dyn MessageSink>,
        Arc::new(tools),
        Arc::clone(&admission) as Arc<dyn ToolAdmission>,
    ));

    let handle = engine.spawn_user_message(&session.id, "exercise the late usage race".into());
    tokio::time::timeout(Duration::from_secs(2), admission.entered.notified())
        .await
        .expect("tool admission reached after assistant persistence");

    let before: (String, String, String, i64, i64) = db
        .with_conn_blocking(|connection| {
            connection
                .query_row(
                    "SELECT task.id,run.id,invocation.status,
                            task.usage_complete,run.usage_complete
                     FROM tasks task
                     JOIN run_envelopes run ON run.id=task.current_run_id
                     JOIN tool_invocations invocation ON invocation.run_id=run.id",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .map_err(Into::into)
        })
        .expect("durable assistant tool boundary");
    assert_eq!(before.2, "preparing");
    assert_eq!((before.3, before.4), (1, 1));
    let assistant_count: i64 = db
        .with_conn_blocking(|connection| {
            connection
                .query_row(
                    "SELECT COUNT(*) FROM messages WHERE role='assistant'",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
        })
        .expect("assistant count");
    assert_eq!(assistant_count, 1, "race begins after assistant commit");

    let root_task_id = before.0.clone();
    db.with_conn_blocking(move |connection| {
        assert_eq!(
            connection.execute(
                "UPDATE tasks SET usage_complete=0 WHERE id=?1",
                [root_task_id],
            )?,
            1
        );
        Ok(())
    })
    .expect("poison root after the advisory gate");
    admission.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("guarded CAS must terminate the run")
        .expect("engine task joins");

    assert_eq!(
        executions.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "physical tool execute must remain behind the guarded CAS"
    );
    let terminal: (String, String, i64, Option<String>, i64) = db
        .with_conn_blocking(|connection| {
            connection
                .query_row(
                    "SELECT task.status,run.status,
                            (SELECT COUNT(*) FROM tool_invocations
                             WHERE status='failed'
                               AND error_code='BUDGET_USAGE_INCOMPLETE'),
                            result.error_code,
                            (SELECT COUNT(*) FROM tool_invocations
                             WHERE status IN ('preparing','queued','running'))
                     FROM tasks task
                     JOIN run_envelopes run ON run.id=task.current_run_id
                     JOIN task_results result ON result.task_id=task.id",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .map_err(Into::into)
        })
        .expect("consistent terminal facts");
    assert_eq!(terminal.0, "failed");
    assert_eq!(terminal.1, "failed");
    assert_eq!(terminal.2, 2, "the entire unresolved batch must close");
    assert_eq!(terminal.3.as_deref(), Some("BUDGET_USAGE_INCOMPLETE"));
    assert_eq!(terminal.4, 0);

    let kinds = sink.kinds();
    let error_index = kinds
        .iter()
        .position(|kind| *kind == "error")
        .expect("stable error event");
    assert_eq!(sink.json_at(error_index)["code"], "BUDGET_USAGE_INCOMPLETE");
    let complete_index = kinds
        .iter()
        .position(|kind| *kind == "message_complete")
        .expect("terminal completion");
    assert_eq!(sink.json_at(complete_index)["stopReason"], "error");
}

#[tokio::test]
async fn production_root_deadline_stops_an_inflight_physical_call() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(vec![Ok(stream::pending().boxed())]));
    let sink = Arc::new(RecordingSink::default());
    let registry = registry_with_models(Arc::clone(&provider), &["qwen3.8-max-0902"]);
    let engine = Arc::new(
        Engine::new(
            db.clone(),
            registry as Arc<dyn ChatProvider>,
            Arc::clone(&sink) as Arc<dyn MessageSink>,
        )
        .with_root_task_budget_policy(RootTaskBudgetPolicy {
            token_limit: Some(1_000_000),
            cost_limit_nanos_usd: Some(4_000_000_000),
            // Leave enough room for the production three-layer SQLite usage
            // assertion so this test reaches the provider before exercising
            // in-flight deadline cancellation, even on a loaded CI worker.
            deadline: Duration::from_millis(500),
        }),
    );

    tokio::time::timeout(
        Duration::from_secs(3),
        run(&engine, &session.id, "wait forever"),
    )
    .await
    .expect("durable deadline must stop the physical call");

    assert_eq!(provider.request_count(), 1);
    let complete_index = sink
        .kinds()
        .iter()
        .position(|kind| *kind == "message_complete")
        .expect("message_complete");
    let complete = sink.json_at(complete_index);
    assert_eq!(complete["stopReason"], "timeout");
    let run_id = complete["runId"].as_str().expect("run id");
    let result = db
        .read_task_result(run_id, None, 0, zk_db::INLINE_RESULT_LIMIT)
        .await
        .expect("result query")
        .expect("timeout result");
    assert_eq!(result.result.status, zk_db::ResultStatus::Error);
    assert_eq!(result.result.error_code.as_deref(), Some("TIMEOUT"));

    let mut physical_status = None;
    for _ in 0..100 {
        let status = db
            .with_conn_blocking({
                let run_id = run_id.to_owned();
                move |conn| {
                    conn.query_row(
                        "SELECT status,usage_complete FROM llm_calls WHERE run_id=?1",
                        [run_id],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                    )
                    .map_err(Into::into)
                }
            })
            .expect("physical call status");
        if status.0 != "started" {
            physical_status = Some(status);
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(physical_status, Some(("cancelled".to_owned(), 0)));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn production_prompt_uses_bounded_sqlite_project_memory_and_ignores_legacy_files() {
    let workspace =
        std::env::temp_dir().join(format!("zk-engine-dynamic-prompt-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workspace).expect("workspace");
    let workspace = std::fs::canonicalize(workspace).expect("canonical workspace");
    std::fs::write(
        workspace.join("PROJECT.md"),
        "PROJECT_PROMPT_SENTINEL: keep responses concise",
    )
    .expect("project prompt");
    std::fs::write(
        workspace.join("zhikun.md"),
        "LEGACY_FILE_MEMORY_SENTINEL: must not be loaded",
    )
    .expect("legacy project memory fixture");

    let db = Db::open_in_memory().expect("db");
    db.create_memory(
        MemoryTarget::project(workspace.to_string_lossy().into_owned()).expect("project target"),
        MemoryUpsert {
            id: None,
            category: "quality".into(),
            title: "verification policy".into(),
            content: format!(
                "PROJECT_MEMORY_SENTINEL: verification is required {}",
                "large-memory-payload ".repeat(20_000)
            ),
            keywords: Some("verification,quality".into()),
            source: Some("USER".into()),
        },
    )
    .await
    .expect("SQLite project memory");
    db.create_memory(
        MemoryTarget::global(),
        MemoryUpsert {
            id: None,
            category: "global".into(),
            title: "explicit only".into(),
            content: "GLOBAL_MEMORY_SENTINEL: must not be injected by default".into(),
            keywords: None,
            source: Some("USER".into()),
        },
    )
    .await
    .expect("global memory fixture");
    db.put_config_value("user_config", r#"{"locale":"zh-CN"}"#)
        .await
        .expect("locale");
    let session = db
        .create_session(
            "qwen3.8-max-0902",
            workspace.to_str().expect("utf8 workspace"),
        )
        .await
        .expect("session");
    let provider = Arc::new(MockProvider::new(vec![events(vec![
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(1, 1)),
        },
    ])]));
    let sink = Arc::new(RecordingSink::default());
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(EchoTool));
    let engine = Arc::new(Engine::with_tools(
        db,
        Arc::clone(&provider) as Arc<dyn ChatProvider>,
        sink,
        Arc::new(registry),
    ));

    run(&engine, &session.id, "inspect the dynamic prompt").await;
    let request = provider.request_at(0);
    assert_eq!(request.system_segments.len(), 2);
    let static_prefix = &request.system_segments[0];
    let dynamic_suffix = &request.system_segments[1];
    assert!(static_prefix.cache_control);
    assert!(!dynamic_suffix.cache_control);
    assert!(!static_prefix.text.contains("PROJECT_PROMPT_SENTINEL"));
    assert!(!static_prefix.text.contains("PROJECT_MEMORY_SENTINEL"));
    assert!(
        !static_prefix
            .text
            .contains(workspace.to_str().expect("utf8"))
    );
    assert!(dynamic_suffix.text.contains("PROJECT_PROMPT_SENTINEL"));
    assert!(dynamic_suffix.text.contains("PROJECT_MEMORY_SENTINEL"));
    assert!(!dynamic_suffix.text.contains("LEGACY_FILE_MEMORY_SENTINEL"));
    assert!(!dynamic_suffix.text.contains("GLOBAL_MEMORY_SENTINEL"));
    assert!(
        dynamic_suffix
            .text
            .contains("[memory truncated to context budget]")
    );
    let memory_start = dynamic_suffix
        .text
        .find("<project_memory>")
        .expect("memory start");
    let memory_end = dynamic_suffix.text[memory_start..]
        .find("</project_memory>")
        .map(|offset| memory_start + offset + "</project_memory>".len())
        .expect("memory end");
    let memory_section = &dynamic_suffix.text[memory_start..memory_end];
    assert!(
        zk_engine::context::token_counter::count_text(memory_section, "qwen3.8-max-0902") <= 2_048
    );
    assert!(dynamic_suffix.text.contains("始终使用 zh-CN 回复"));
    assert!(
        dynamic_suffix
            .text
            .contains(workspace.to_str().expect("utf8"))
    );
    assert!(request.tools.iter().any(|tool| tool.name == "Echo"));

    std::fs::remove_dir_all(&workspace).expect("cleanup");
}

#[tokio::test]
async fn explicit_thinking_on_an_unsupported_model_downgrades_with_notification() {
    let (engine, provider, sink, db, sid) = setup_with_model(
        vec![events(vec![ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(1, 1)),
        }])],
        "unknown-non-thinking-model",
    )
    .await;
    let service = ConversationService::new(engine, db);
    service
        .execute_with_options(
            &sid,
            "think carefully".into(),
            ConversationRunOptions {
                thinking: Some(zk_llm::ThinkingMode::Enabled),
                ..ConversationRunOptions::default()
            },
        )
        .await;

    assert_eq!(
        provider.request_at(0).thinking,
        zk_llm::ThinkingMode::Disabled
    );
    assert_eq!(sink.kinds().first(), Some(&"notification"));
    assert_eq!(sink.json_at(0)["key"], "thinking_mode_downgraded");
    assert_eq!(sink.json_at(0)["level"], "warning");
}

#[tokio::test]
async fn thinking_mixed_stream_and_trailing_usage() {
    let (engine, _provider, sink, db, sid) = setup(vec![events(vec![
        ProviderEvent::ThinkingDelta {
            thinking: "pondering".into(),
        },
        ProviderEvent::TextDelta {
            text: "answer".into(),
        },
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: None,
        },
        // Finish 之后的 usage-only 尾块必须覆盖终态用量。
        ProviderEvent::UsageUpdate { usage: usage(3, 4) },
    ])])
    .await;
    run(&engine, &sid, "why").await;

    assert_eq!(
        sink.kinds(),
        vec![
            "thinking_delta",
            "stream_delta",
            // Batch 0 Step 0-6：Finish + trailing usage → push_cost_update。
            "cost_update",
            "message_complete",
            "session_list_updated"
        ]
    );
    // thinking_delta 线上字段名为 delta（zk-protocol 权威形状）。
    assert_eq!(sink.json_at(0)["delta"], "pondering");
    let complete = sink.json_at(3);
    assert_eq!(complete["usage"]["inputTokens"], 3);
    assert_eq!(complete["usage"]["outputTokens"], 4);

    let page = db
        .list_messages(&sid, None, 10)
        .await
        .expect("list")
        .expect("session exists");
    let assistant = &page.messages[1];
    assert_eq!(
        assistant.content,
        vec![
            StoredBlock::Thinking {
                thinking: "pondering".into()
            },
            StoredBlock::Text {
                text: "answer".into()
            },
        ]
    );
}

#[tokio::test]
async fn busy_rejects_second_run_and_releases_slot() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let gate_stream = Arc::clone(&gate);
    let first: BoxStream<'static, ProviderEvent> = stream::once(async move {
        gate_stream.notified().await;
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(1, 1)),
        }
    })
    .boxed();
    let (engine, provider, sink, db, sid) = setup(vec![
        Ok(first),
        events(vec![ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(2, 2)),
        }]),
    ])
    .await;

    let first_run = engine.spawn_user_message(&sid, "first".into());
    // 等第一 run 走到 provider 流（busy 槽位必然已占用）。
    while provider.request_count() == 0 {
        tokio::task::yield_now().await;
    }
    run(&engine, &sid, "second").await;
    // busy 拒绝：code/文案/retryable 对齐旧 L641；不产生任何落库。
    let busy = sink.json_at(sink.kinds().len() - 1);
    assert_eq!(busy["type"], "error");
    assert_eq!(busy["code"], "query_busy");
    assert_eq!(busy["message"], "当前会话正在处理中，请等待上一个请求完成");
    assert_eq!(busy["retryable"], false);

    gate.notify_one();
    first_run.await.expect("first run joins");
    // busy 消息不落库：仅第一 run 的 user+assistant 两条。
    let page = db
        .list_messages(&sid, None, 10)
        .await
        .expect("list")
        .expect("session exists");
    assert_eq!(page.messages.len(), 2);

    // 槽位释放后新 run 正常执行。
    run(&engine, &sid, "third").await;
    assert_eq!(provider.request_count(), 2);
    assert_eq!(sink.kinds().last(), Some(&"session_list_updated"));
}

#[tokio::test]
async fn fatal_stream_error_emits_complete_only_after_durable_error_result() {
    let (engine, _provider, sink, db, sid) = setup(vec![events(vec![ProviderEvent::Error {
        error: ProviderError::Network {
            message: "connection reset".into(),
        },
    }])])
    .await;
    run(&engine, &sid, "boom").await;

    assert_eq!(
        sink.kinds(),
        vec!["error", "message_complete", "session_list_updated"]
    );
    let error = sink.json_at(0);
    assert_eq!(error["code"], "query_error");
    assert_eq!(error["retryable"], true);
    // The completion is now attributed to the durably failed Run and includes
    // only records which were already committed before publication.
    let complete = sink.json_at(1);
    assert_eq!(complete["usage"]["inputTokens"], 0);
    assert_eq!(complete["stopReason"], "error");
    assert!(complete["runId"].as_str().is_some());
    assert_eq!(
        complete["committedMessages"].as_array().map(Vec::len),
        Some(1)
    );

    // 用户消息保留（provider 调用前已落库），助手消息不落库。
    let page = db
        .list_messages(&sid, None, 10)
        .await
        .expect("list")
        .expect("session exists");
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].role, MessageRole::User);
    let terminal: (String, String, i64) = db
        .with_conn_blocking(|conn| {
            conn.query_row(
                "SELECT t.status,r.status,(SELECT COUNT(*) FROM task_results tr WHERE tr.task_id=t.id)
                 FROM tasks t JOIN run_envelopes r ON r.id=t.current_run_id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(Into::into)
        })
        .expect("durable terminal result");
    assert_eq!(terminal, ("failed".to_owned(), "failed".to_owned(), 1));
}

#[tokio::test]
async fn setup_failure_from_chat_stream_is_fatal() {
    let (engine, _provider, sink, db, sid) = setup(vec![Err(ProviderError::Config {
        message: "provider 'mock' has empty api key".into(),
    })])
    .await;
    run(&engine, &sid, "hi").await;

    assert_eq!(
        sink.kinds(),
        vec!["error", "message_complete", "session_list_updated"]
    );
    let error = sink.json_at(0);
    assert_eq!(error["code"], "query_error");
    // 致命（Config 类）错误同样标记可重试：旧 catch/onError 恒发 true。
    assert_eq!(error["retryable"], true);
    let result_count: i64 = db
        .with_conn_blocking(|conn| {
            conn.query_row("SELECT COUNT(*) FROM task_results", [], |row| row.get(0))
                .map_err(Into::into)
        })
        .expect("result count");
    assert_eq!(result_count, 1);
}

#[tokio::test]
async fn root_establishment_budget_failure_preserves_the_exact_runtime_code() {
    let (engine, provider, sink, db, sid) = setup(vec![Err(ProviderError::Config {
        message: "observer rejected call: BUDGET_PRICE_UNKNOWN".into(),
    })])
    .await;

    run(&engine, &sid, "fail with the originating code").await;

    assert_eq!(provider.request_count(), 1);
    assert_eq!(
        sink.kinds(),
        vec!["error", "message_complete", "session_list_updated"]
    );
    assert_eq!(sink.json_at(0)["code"], "BUDGET_PRICE_UNKNOWN");
    assert_eq!(sink.json_at(0)["retryable"], false);
    let complete = sink.json_at(1);
    assert_eq!(complete["stopReason"], "error");
    let run_id = complete["runId"].as_str().expect("run id");
    let result = db
        .read_task_result(run_id, None, 0, zk_db::INLINE_RESULT_LIMIT)
        .await
        .expect("result query")
        .expect("failed result");
    assert_eq!(result.result.status, zk_db::ResultStatus::Error);
    assert_eq!(
        result.result.error_code.as_deref(),
        Some("BUDGET_PRICE_UNKNOWN")
    );
    assert!(result.content.contains("BUDGET_PRICE_UNKNOWN"));
}

#[allow(clippy::too_many_lines)]
async fn run_scripted_child(
    script: Result<BoxStream<'static, ProviderEvent>, ProviderError>,
) -> (Db, Arc<MockProvider>, String, String, SubAgentRunOutcome) {
    run_scripted_child_turns(vec![script], 1).await
}

#[allow(clippy::too_many_lines)]
async fn run_scripted_child_turns(
    scripts: Vec<Result<BoxStream<'static, ProviderEvent>, ProviderError>>,
    max_turns: u32,
) -> (Db, Arc<MockProvider>, String, String, SubAgentRunOutcome) {
    let db = Db::open_in_memory().expect("in-memory db");
    let root_session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("root session");
    let root_task_id = uuid::Uuid::new_v4().to_string();
    let root_run_id = uuid::Uuid::new_v4().to_string();
    let root = db
        .create_task_with_run(&CreateTaskWithRun {
            task_id: root_task_id.clone(),
            run_id: root_run_id.clone(),
            root_session_id: root_session.id.clone(),
            transcript_session_id: root_session.id.clone(),
            parent_task_id: None,
            parent_run_id: None,
            creator_tool_use_id: None,
            ordinal: 0,
            description: "root".into(),
            prompt: Some("root".into()),
            task_type: "agent".into(),
            model: "qwen3.8-max-0902".into(),
            working_dir: "/tmp".into(),
            execution_config_json: json!({
                "budget": {
                    "tokenLimit": 1_000_000,
                    "costLimitNanosUsd": 4_000_000_000_i64,
                    "deadlineAtMs": zk_db::time::now_millis() + 60_000,
                }
            })
            .to_string(),
            startup_epoch: 1,
        })
        .await
        .expect("root task");
    assert_eq!(
        db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
            .await
            .expect("claim root"),
        CasOutcome::Applied
    );

    let child = db
        .create_task_with_run(&CreateTaskWithRun {
            task_id: uuid::Uuid::new_v4().to_string(),
            run_id: uuid::Uuid::new_v4().to_string(),
            root_session_id: root_session.id,
            transcript_session_id: uuid::Uuid::new_v4().to_string(),
            parent_task_id: Some(root_task_id),
            parent_run_id: Some(root_run_id),
            creator_tool_use_id: Some("agent-call".into()),
            ordinal: 0,
            description: "child".into(),
            prompt: Some("child".into()),
            task_type: "agent".into(),
            model: "qwen3.8-max-0902".into(),
            working_dir: "/tmp".into(),
            execution_config_json: json!({"isolation": "readOnly"}).to_string(),
            startup_epoch: 1,
        })
        .await
        .expect("child task");
    assert_eq!(
        db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
            .await
            .expect("claim child"),
        CasOutcome::Applied
    );
    let budget = db
        .read_task_budget(&child.task.id)
        .await
        .expect("budget query")
        .expect("child budget");
    let child_task_id = child.task.id.clone();
    let child_run_id = child.run_id.clone();
    let child_session_id = child.transcript_session_id.clone();
    let provider = Arc::new(MockProvider::new(scripts));
    let engine = Engine::with_tools(
        db.clone(),
        Arc::clone(&provider) as Arc<dyn ChatProvider>,
        Arc::new(RecordingSink::default()),
        Arc::new(ToolRegistry::new()),
    )
    .with_coordinator(Arc::new(CoordinatorService::new(
        &FeatureFlags::with_defaults(),
        true,
    )));
    let (_mailbox_tx, mailbox) = tokio::sync::mpsc::unbounded_channel();
    let outcome = engine
        .run_sub_agent(
            SubAgentRunConfig {
                agent_id: child_task_id.clone(),
                session_id: child_session_id,
                run_id: child_run_id.clone(),
                model: "qwen3.8-max-0902".into(),
                system_prompt: "system".into(),
                user_prompt: "child".into(),
                work_dir: "/tmp".into(),
                max_turns,
                mailbox,
                budget: TaskBudgetLimits {
                    token_limit: budget.token_limit,
                    cost_limit_nanos_usd: budget.cost_limit_nanos_usd,
                    deadline_at_ms: budget.deadline_at_ms,
                },
                recovery_checkpoint: None,
            },
            CancellationToken::new(),
        )
        .await;
    (db, provider, child_task_id, child_run_id, outcome)
}

#[tokio::test]
async fn child_last_turn_synthesizes_partial_instead_of_returning_progress_note() {
    let (_db, provider, _, _, outcome) = run_scripted_child_turns(
        vec![
            events(vec![
                ProviderEvent::TextDelta {
                    text: "Still researching".into(),
                },
                ProviderEvent::ToolUseStart {
                    id: "call-search".into(),
                    name: "Unavailable".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "call-search".into(),
                    delta: "{}".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::ToolUse,
                    usage: Some(usage(4, 2)),
                },
            ]),
            events(vec![
                ProviderEvent::TextDelta {
                    text: "Partial report: search unavailable; no verified findings.".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(usage(4, 2)),
                },
            ]),
        ],
        2,
    )
    .await;
    assert_eq!(provider.request_count(), 2);
    assert_eq!(outcome.stop_reason.as_deref(), Some("max_turns"));
    let final_request = provider.request_at(1);
    assert!(final_request.tools.is_empty());
    assert!(
        final_request
            .messages
            .iter()
            .any(|message| message.content.contains("self-contained partial report"))
    );
    assert!(
        outcome
            .assistant_text
            .unwrap()
            .starts_with("Partial report:")
    );
    assert!(!outcome.has_error);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn child_usage_poison_after_assistant_commit_never_spawns_a_tool() {
    let db = Db::open_in_memory().expect("in-memory db");
    let root_session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("root session");
    let root = db
        .create_task_with_run(&CreateTaskWithRun {
            task_id: uuid::Uuid::new_v4().to_string(),
            run_id: uuid::Uuid::new_v4().to_string(),
            root_session_id: root_session.id.clone(),
            transcript_session_id: root_session.id.clone(),
            parent_task_id: None,
            parent_run_id: None,
            creator_tool_use_id: None,
            ordinal: 0,
            description: "root".into(),
            prompt: Some("root".into()),
            task_type: "agent".into(),
            model: "qwen3.8-max-0902".into(),
            working_dir: "/tmp".into(),
            execution_config_json: json!({
                "budget": {
                    "tokenLimit": 1_000_000,
                    "costLimitNanosUsd": 4_000_000_000_i64,
                    "deadlineAtMs": zk_db::time::now_millis() + 60_000,
                }
            })
            .to_string(),
            startup_epoch: 1,
        })
        .await
        .expect("root task");
    assert_eq!(
        db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
            .await
            .expect("claim root"),
        CasOutcome::Applied
    );
    let child = db
        .create_task_with_run(&CreateTaskWithRun {
            task_id: uuid::Uuid::new_v4().to_string(),
            run_id: uuid::Uuid::new_v4().to_string(),
            root_session_id: root_session.id,
            transcript_session_id: uuid::Uuid::new_v4().to_string(),
            parent_task_id: Some(root.task.id.clone()),
            parent_run_id: Some(root.run_id),
            creator_tool_use_id: Some("agent-call".into()),
            ordinal: 0,
            description: "child".into(),
            prompt: Some("child".into()),
            task_type: "agent".into(),
            model: "qwen3.8-max-0902".into(),
            working_dir: "/tmp".into(),
            execution_config_json: json!({"isolation": "readOnly"}).to_string(),
            startup_epoch: 1,
        })
        .await
        .expect("child task");
    assert_eq!(
        db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
            .await
            .expect("claim child"),
        CasOutcome::Applied
    );
    let budget = db
        .read_task_budget(&child.task.id)
        .await
        .expect("budget query")
        .expect("child budget");
    let provider = Arc::new(MockProvider::new(vec![events(vec![
        ProviderEvent::ToolUseStart {
            id: "child-late-poison".into(),
            name: "Counted".into(),
        },
        ProviderEvent::ToolInputDelta {
            id: "child-late-poison".into(),
            delta: r#"{"value":"must-not-run"}"#.into(),
        },
        ProviderEvent::Finish {
            finish_reason: FinishReason::ToolUse,
            usage: Some(usage(4, 2)),
        },
    ])]));
    let admission = Arc::new(BlockingToolAdmission::new());
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(Arc::new(CountingTool {
        executions: Arc::clone(&executions),
    }));
    let engine = Arc::new(Engine::with_admission(
        db.clone(),
        provider as Arc<dyn ChatProvider>,
        Arc::new(RecordingSink::default()),
        Arc::new(tools),
        Arc::clone(&admission) as Arc<dyn ToolAdmission>,
    ));
    let (_mailbox_tx, mailbox) = tokio::sync::mpsc::unbounded_channel();
    let child_task_id = child.task.id.clone();
    let child_run_id = child.run_id.clone();
    let child_session_id = child.transcript_session_id.clone();
    let before_run_id = child.run_id.clone();
    let terminal_run_id = child.run_id.clone();
    let handle = tokio::spawn({
        let engine = Arc::clone(&engine);
        async move {
            engine
                .run_sub_agent(
                    SubAgentRunConfig {
                        agent_id: child_task_id,
                        session_id: child_session_id,
                        run_id: child_run_id,
                        model: "qwen3.8-max-0902".into(),
                        system_prompt: "system".into(),
                        user_prompt: "child".into(),
                        work_dir: "/tmp".into(),
                        max_turns: 2,
                        mailbox,
                        budget: TaskBudgetLimits {
                            token_limit: budget.token_limit,
                            cost_limit_nanos_usd: budget.cost_limit_nanos_usd,
                            deadline_at_ms: budget.deadline_at_ms,
                        },
                        recovery_checkpoint: None,
                    },
                    CancellationToken::new(),
                )
                .await
        }
    });

    tokio::time::timeout(Duration::from_secs(2), admission.entered.notified())
        .await
        .expect("child admission reached");
    let child_session = child.transcript_session_id.clone();
    let before: (String, i64) = db
        .with_conn_blocking(move |connection| {
            connection
                .query_row(
                    "SELECT invocation.status,
                            (SELECT COUNT(*) FROM messages
                             WHERE session_id=?1 AND role='assistant')
                     FROM tool_invocations invocation WHERE invocation.run_id=?2",
                    [&child_session, &before_run_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(Into::into)
        })
        .expect("child assistant/preparing boundary");
    assert_eq!(before, ("preparing".to_owned(), 1));

    let root_task_id = root.task.id.clone();
    db.with_conn_blocking(move |connection| {
        assert_eq!(
            connection.execute(
                "UPDATE tasks SET usage_complete=0 WHERE id=?1",
                [root_task_id],
            )?,
            1
        );
        Ok(())
    })
    .expect("sibling poison root after child gate");
    admission.release.notify_one();
    let outcome = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("child exits at guarded boundary")
        .expect("child task joins");

    assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        outcome.stop_reason.as_deref(),
        Some("BUDGET_USAGE_INCOMPLETE")
    );
    assert!(outcome.has_error);
    assert_eq!(
        AgentStatus::classify(
            outcome.stop_reason.as_deref(),
            outcome.assistant_text.is_some(),
            outcome.has_error,
        ),
        AgentStatus::Failed
    );
    let invocation: (String, Option<String>, i64) = db
        .with_conn_blocking(move |connection| {
            connection
                .query_row(
                    "SELECT status,error_code,
                            (SELECT COUNT(*) FROM tool_invocations
                             WHERE status IN ('preparing','queued','running'))
                     FROM tool_invocations WHERE run_id=?1",
                    [terminal_run_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(Into::into)
        })
        .expect("closed child invocation");
    assert_eq!(invocation.0, "failed");
    assert_eq!(invocation.1.as_deref(), Some("BUDGET_USAGE_INCOMPLETE"));
    assert_eq!(invocation.2, 0);
}

#[tokio::test]
async fn stable_stream_runtime_failure_wins_over_later_finish_for_child() {
    for runtime_code in [
        "COST_BUDGET_EXHAUSTED",
        "BUDGET_USAGE_INCOMPLETE",
        "TASK_DEADLINE_EXCEEDED",
    ] {
        let (db, provider, child_task_id, child_run_id, outcome) =
            run_scripted_child(events(vec![
                ProviderEvent::Error {
                    error: ProviderError::Config {
                        message: runtime_code.to_owned(),
                    },
                },
                ProviderEvent::Error {
                    error: ProviderError::Parse {
                        message: "recoverable trailing chunk".to_owned(),
                    },
                },
                ProviderEvent::TextDelta {
                    text: "must not become a durable assistant".to_owned(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(usage(4, 2)),
                },
            ]))
            .await;

        assert_eq!(provider.request_count(), 1, "runtime_code={runtime_code}");
        let request = provider.request_at(0);
        assert_eq!(request.system_prompt.as_deref(), Some("system"));
        assert!(request.system_segments.is_empty());
        assert!(
            !request
                .system_text()
                .expect("child system prompt")
                .contains("# Coordinator 模式")
        );
        assert_eq!(outcome.stop_reason.as_deref(), Some(runtime_code));
        assert!(
            outcome.has_error,
            "every stable runtime failure must publish AgentFailed: runtime_code={runtime_code}"
        );
        assert_eq!(outcome.assistant_text.as_deref(), Some(runtime_code));

        let _child = db
            .find_runtime_task_by_id(&child_task_id)
            .await
            .expect("read child Task")
            .expect("child Task exists");
        let run = db
            .find_run_by_id(&child_run_id)
            .await
            .expect("read child Run")
            .expect("child Run exists");
        let transcript = db
            .list_messages(&run.session_id, None, 10)
            .await
            .expect("list child transcript")
            .expect("child transcript exists");
        assert_eq!(
            transcript.messages.len(),
            1,
            "post-error Finish must not persist a child assistant: runtime_code={runtime_code}"
        );
        assert_eq!(transcript.messages[0].role, MessageRole::User);
        assert_eq!(run.status, "running", "factory owns terminalization");
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn sub_agent_establishment_budget_failure_preserves_the_exact_runtime_code() {
    let db = Db::open_in_memory().expect("in-memory db");
    let root_session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("root session");
    let root_task_id = uuid::Uuid::new_v4().to_string();
    let root_run_id = uuid::Uuid::new_v4().to_string();
    let root = db
        .create_task_with_run(&CreateTaskWithRun {
            task_id: root_task_id.clone(),
            run_id: root_run_id.clone(),
            root_session_id: root_session.id.clone(),
            transcript_session_id: root_session.id.clone(),
            parent_task_id: None,
            parent_run_id: None,
            creator_tool_use_id: None,
            ordinal: 0,
            description: "root".into(),
            prompt: Some("root".into()),
            task_type: "agent".into(),
            model: "qwen3.8-max-0902".into(),
            working_dir: "/tmp".into(),
            execution_config_json: json!({
                "budget": {
                    "tokenLimit": 1_000_000,
                    "costLimitNanosUsd": 4_000_000_000_i64,
                    "deadlineAtMs": zk_db::time::now_millis() + 60_000,
                }
            })
            .to_string(),
            startup_epoch: 1,
        })
        .await
        .expect("root task");
    assert_eq!(
        db.claim_task_run_cas(&root.task.id, &root.run_id, root.task.version)
            .await
            .expect("claim root"),
        CasOutcome::Applied
    );

    let child_task_id = uuid::Uuid::new_v4().to_string();
    let child_run_id = uuid::Uuid::new_v4().to_string();
    let child_session_id = uuid::Uuid::new_v4().to_string();
    let child = db
        .create_task_with_run(&CreateTaskWithRun {
            task_id: child_task_id,
            run_id: child_run_id,
            root_session_id: root_session.id,
            transcript_session_id: child_session_id,
            parent_task_id: Some(root_task_id),
            parent_run_id: Some(root_run_id),
            creator_tool_use_id: Some("agent-call".into()),
            ordinal: 0,
            description: "child".into(),
            prompt: Some("child".into()),
            task_type: "agent".into(),
            model: "qwen3.8-max-0902".into(),
            working_dir: "/tmp".into(),
            execution_config_json: json!({"isolation": "readOnly"}).to_string(),
            startup_epoch: 1,
        })
        .await
        .expect("child task");
    assert_eq!(
        db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
            .await
            .expect("claim child"),
        CasOutcome::Applied
    );
    let budget = db
        .read_task_budget(&child.task.id)
        .await
        .expect("budget query")
        .expect("child budget");
    let provider = Arc::new(MockProvider::new(vec![Err(ProviderError::Config {
        message: "observer rejected call: BUDGET_USAGE_INCOMPLETE".into(),
    })]));
    let engine = Engine::with_tools(
        db,
        Arc::clone(&provider) as Arc<dyn ChatProvider>,
        Arc::new(RecordingSink::default()),
        Arc::new(ToolRegistry::new()),
    );
    let (_mailbox_tx, mailbox) = tokio::sync::mpsc::unbounded_channel();

    let outcome = engine
        .run_sub_agent(
            SubAgentRunConfig {
                agent_id: child.task.id,
                session_id: child.transcript_session_id,
                run_id: child.run_id,
                model: "qwen3.8-max-0902".into(),
                system_prompt: "system".into(),
                user_prompt: "child".into(),
                work_dir: "/tmp".into(),
                max_turns: 1,
                mailbox,
                budget: TaskBudgetLimits {
                    token_limit: budget.token_limit,
                    cost_limit_nanos_usd: budget.cost_limit_nanos_usd,
                    deadline_at_ms: budget.deadline_at_ms,
                },
                recovery_checkpoint: None,
            },
            CancellationToken::new(),
        )
        .await;

    assert_eq!(provider.request_count(), 1);
    assert_eq!(
        outcome.stop_reason.as_deref(),
        Some("BUDGET_USAGE_INCOMPLETE")
    );
    assert!(outcome.has_error);
    assert!(
        outcome
            .assistant_text
            .as_deref()
            .is_some_and(|text| text.contains("BUDGET_USAGE_INCOMPLETE"))
    );
}

#[tokio::test]
async fn terminal_result_storage_failure_never_publishes_message_complete() {
    let (engine, _provider, sink, db, sid) = setup(vec![events(vec![
        ProviderEvent::TextDelta {
            text: "must stay unreported".into(),
        },
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(2, 3)),
        },
    ])])
    .await;
    db.with_conn_blocking(|conn| {
        conn.execute_batch(
            "CREATE TRIGGER inject_result_commit_failure
             BEFORE INSERT ON task_results
             BEGIN SELECT RAISE(FAIL,'injected result commit failure'); END;",
        )?;
        Ok(())
    })
    .expect("install result failpoint");

    run(&engine, &sid, "finish only when durable").await;

    let kinds = sink.kinds();
    assert!(!kinds.contains(&"message_complete"));
    assert!(!kinds.contains(&"session_list_updated"));
    let durable: (String, String, i64, i64) = db
        .with_conn_blocking(|conn| {
            conn.query_row(
                "SELECT t.status,r.status,
                    (SELECT COUNT(*) FROM task_results),
                    (SELECT COUNT(*) FROM run_event_log WHERE event_type='task_needs_attention')
                 FROM tasks t JOIN run_envelopes r ON r.id=t.current_run_id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(Into::into)
        })
        .expect("quarantined runtime state");
    assert_eq!(
        durable,
        ("needsAttention".to_owned(), "interrupted".to_owned(), 0, 1)
    );
}

/// 会话不存在：旧 `IllegalStateException` 经 catch 归一为 `query_error` +
/// retryable=true，且只发 `error`（旧 finally 未执行，无兜底 complete）。
#[tokio::test]
async fn unknown_session_gets_query_error_only() {
    let (engine, provider, sink, db, _sid) = setup(vec![]).await;
    run(&engine, "no-such-session", "hi").await;

    assert_eq!(sink.kinds(), vec!["error"]);
    let error = sink.json_at(0);
    assert_eq!(error["code"], "query_error");
    assert_eq!(error["message"], "会话不存在: no-such-session");
    assert_eq!(error["retryable"], true);
    assert_eq!(provider.request_count(), 0);
    // 不存在的会话不产生任何写入。
    assert!(
        db.list_messages("no-such-session", None, 10)
            .await
            .expect("list")
            .is_none()
    );
}

#[tokio::test]
async fn parse_error_then_finish_still_succeeds() {
    let (engine, _provider, sink, _db, sid) = setup(vec![events(vec![
        ProviderEvent::Error {
            error: ProviderError::Parse {
                message: "bad chunk".into(),
            },
        },
        ProviderEvent::TextDelta { text: "ok".into() },
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(1, 1)),
        },
    ])])
    .await;
    run(&engine, &sid, "hi").await;

    // 单 chunk 解析错误的宽容行为（D-S6-3）：正常 Finish 即成功终态。
    // Batch 0 Step 0-6：Finish 携带 usage → push_cost_update 插入在
    // stream_delta 与 message_complete 之间。
    assert_eq!(
        sink.kinds(),
        vec![
            "stream_delta",
            "cost_update",
            "message_complete",
            "session_list_updated"
        ]
    );
}

#[tokio::test]
async fn stable_stream_runtime_failure_wins_over_later_finish_for_root() {
    for (runtime_code, stop_reason, result_status, result_code, run_status, exit_reason) in [
        (
            "TOKEN_BUDGET_EXHAUSTED",
            "budget_exhausted",
            zk_db::ResultStatus::Partial,
            "TOKEN_BUDGET_EXHAUSTED",
            "completed",
            zk_db::run::EXIT_BUDGET_EXHAUSTED,
        ),
        (
            "BUDGET_USAGE_INCOMPLETE",
            "error",
            zk_db::ResultStatus::Error,
            "BUDGET_USAGE_INCOMPLETE",
            "failed",
            zk_db::run::EXIT_INTERNAL_ERROR,
        ),
        (
            "TASK_DEADLINE_EXCEEDED",
            "timeout",
            zk_db::ResultStatus::Error,
            "TIMEOUT",
            "failed",
            zk_db::run::EXIT_TIMEOUT,
        ),
    ] {
        let (engine, provider, sink, db, sid) = setup(vec![events(vec![
            ProviderEvent::Error {
                error: ProviderError::Config {
                    message: runtime_code.to_owned(),
                },
            },
            // A later recoverable error must not overwrite the stable failure.
            ProviderEvent::Error {
                error: ProviderError::Parse {
                    message: "recoverable trailing chunk".to_owned(),
                },
            },
            ProviderEvent::TextDelta {
                text: "must not become a durable assistant".to_owned(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(usage(3, 2)),
            },
        ])])
        .await;

        run(&engine, &sid, "runtime failure must remain terminal").await;

        assert_eq!(provider.request_count(), 1, "runtime_code={runtime_code}");
        let kinds = sink.kinds();
        assert!(
            kinds.contains(&"stream_delta"),
            "runtime_code={runtime_code}"
        );
        assert!(kinds.contains(&"error"), "runtime_code={runtime_code}");
        assert!(
            !kinds.contains(&"cost_update"),
            "failed Finish usage must not be published: runtime_code={runtime_code}"
        );
        let error_index = kinds
            .iter()
            .position(|kind| *kind == "error")
            .expect("stable runtime error event");
        assert_eq!(sink.json_at(error_index)["code"], runtime_code);
        let complete_index = kinds
            .iter()
            .position(|kind| *kind == "message_complete")
            .expect("durable terminal completion");
        let complete = sink.json_at(complete_index);
        assert_eq!(complete["stopReason"], stop_reason);
        let run_id = complete["runId"].as_str().expect("terminal Run identity");

        let result = db
            .read_task_result(run_id, None, 0, zk_db::INLINE_RESULT_LIMIT)
            .await
            .expect("read TaskResult")
            .expect("runtime failure owns TaskResult");
        assert_eq!(result.result.status, result_status);
        assert_eq!(result.result.error_code.as_deref(), Some(result_code));
        let durable_run = db
            .find_run_by_id(run_id)
            .await
            .expect("read Run")
            .expect("Run exists");
        assert_eq!(durable_run.status, run_status);
        assert_eq!(durable_run.exit_reason.as_deref(), Some(exit_reason));

        let messages = db
            .list_messages(&sid, None, 10)
            .await
            .expect("list transcript")
            .expect("session exists");
        assert_eq!(
            messages.messages.len(),
            1,
            "post-error Finish must not persist an assistant: runtime_code={runtime_code}"
        );
        assert_eq!(messages.messages[0].role, MessageRole::User);
    }
}

#[tokio::test]
async fn second_turn_carries_history_and_replace_anchor() {
    let (engine, provider, sink, db, sid) = setup(vec![
        events(vec![
            ProviderEvent::TextDelta {
                text: "Hello world".into(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(usage(1, 1)),
            },
        ]),
        events(vec![
            ProviderEvent::TextDelta {
                text: "again!".into(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(usage(2, 2)),
            },
        ]),
    ])
    .await;
    run(&engine, &sid, "hi").await;
    run(&engine, &sid, "more").await;

    // 第二轮请求携带全量历史（user/assistant/user 纯文本序列）。
    let request = provider.request_at(1);
    let contents: Vec<&str> = request
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect();
    assert_eq!(contents, vec!["hi", "Hello world", "more"]);

    // 第二轮替换锚点 = 第一轮助手消息 uuid（历史末条）。
    let page = db
        .list_messages(&sid, None, 10)
        .await
        .expect("list")
        .expect("session exists");
    let first_assistant_id = page.messages[1].id.clone();
    let complete = sink.json_at(sink.kinds().len() - 2);
    assert_eq!(complete["type"], "message_complete");
    assert_eq!(
        complete["replaceAfterMessageId"],
        first_assistant_id.as_str()
    );
    assert_eq!(complete["stopReason"], "end_turn");
    assert_eq!(page.messages.len(), 4);
}

/// 内存库 + 会话 + 注入工具注册表的引擎装配（2.2 工具循环测试用）。
async fn setup_with_tools(
    scripts: Vec<Result<BoxStream<'static, ProviderEvent>, ProviderError>>,
    registry: ToolRegistry,
) -> (
    Arc<Engine>,
    Arc<MockProvider>,
    Arc<RecordingSink>,
    Db,
    String,
) {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(scripts));
    let sink = Arc::new(RecordingSink::default());
    let engine = Arc::new(Engine::with_tools(
        db.clone(),
        Arc::clone(&provider) as Arc<dyn ChatProvider>,
        Arc::clone(&sink) as Arc<dyn MessageSink>,
        Arc::new(registry),
    ));
    (engine, provider, sink, db, session.id)
}

/// 永不完成的工具桩（FIX-02 中断路径测试用；默认 120s 超时远大于断言窗口）。
struct NeverendingTool;

impl Tool for NeverendingTool {
    fn name(&self) -> &'static str {
        "Hang"
    }

    fn description(&self) -> &'static str {
        "never completes"
    }

    fn parameters(&self) -> serde_json::Value {
        json!({ "type": "object" })
    }

    fn execute(&self, _input: serde_json::Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(std::future::pending())
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "完整 WS 序列+落库+续轮请求逐字段断言"
)]
#[tokio::test]
async fn tool_call_loop_executes_and_continues() {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(EchoTool));
    let (engine, provider, sink, db, sid) = setup_with_tools(
        vec![
            events(vec![
                ProviderEvent::ToolUseStart {
                    id: "call-1".into(),
                    name: "Echo".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "call-1".into(),
                    delta: "{\"text\"".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "call-1".into(),
                    delta: ":\"hi\"}".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::ToolUse,
                    usage: Some(usage(3, 2)),
                },
            ]),
            events(vec![
                ProviderEvent::TextDelta {
                    text: "done".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(usage(5, 4)),
                },
            ]),
        ],
        registry,
    )
    .await;
    run(&engine, &sid, "echo hi").await;

    // 完整 WS 序列：provider usage → durable start（空 input 占位）→
    // input（flush 全参）→ result → 续轮文本流 → 终态。
    // Batch 0 Step 0-6：每次 Finish 携带 usage → push_cost_update 各落一次
    // （首轮 tool_use 结束时、续轮 end_turn 时）。
    assert_eq!(
        sink.kinds(),
        vec![
            "cost_update",
            "tool_use_start",
            "tool_use_input",
            "tool_result",
            "stream_delta",
            "cost_update",
            "message_complete",
            "session_list_updated",
        ]
    );
    let start = sink.json_at(1);
    assert_eq!(start["toolUseId"], "call-1");
    assert_eq!(start["toolName"], "Echo");
    assert_eq!(start["input"], json!({}));
    // Batch 0 Step 0-6：新增 cost_update 使后续索引整体后移。
    let input = sink.json_at(2);
    assert_eq!(input["input"]["text"], "hi");
    let result = sink.json_at(3);
    assert_eq!(result["toolUseId"], "call-1");
    assert_eq!(result["result"]["content"], "hi");
    assert_eq!(result["result"]["isError"], false);
    // 终态：usage 跨轮累计、stopReason=end_turn、committed 4 条完整链。
    let complete = sink.json_at(6);
    assert_eq!(complete["usage"]["inputTokens"], 8);
    assert_eq!(complete["usage"]["outputTokens"], 6);
    assert_eq!(complete["stopReason"], "end_turn");
    let committed = complete["committedMessages"].as_array().expect("committed");
    assert_eq!(committed.len(), 4);
    assert_eq!(committed[1]["type"], "assistant");
    assert_eq!(committed[1]["content"][0]["type"], "tool_use");
    assert_eq!(committed[1]["content"][0]["input"]["text"], "hi");
    assert_eq!(committed[1]["stopReason"], "tool_use");
    assert_eq!(committed[2]["type"], "user");
    assert_eq!(committed[2]["content"][0]["type"], "tool_result");
    assert_eq!(committed[3]["content"][0]["text"], "done");

    // 续轮请求回填：assistant(tool_calls) + 每结果一条 tool 消息；tools 恒下发。
    assert_eq!(provider.request_count(), 2);
    let request = provider.request_at(1);
    assert_eq!(request.tools.len(), 1);
    assert_eq!(request.messages.len(), 3);
    assert_eq!(request.messages[1].tool_calls.len(), 1);
    assert_eq!(request.messages[1].tool_calls[0].id, "call-1");
    assert_eq!(
        request.messages[1].tool_calls[0].arguments,
        "{\"text\":\"hi\"}"
    );
    assert_eq!(request.messages[2].tool_call_id.as_deref(), Some("call-1"));
    assert_eq!(request.messages[2].content, "hi");

    // 落库形状：user / assistant(tool_use) / user(tool_result) / assistant。
    let page = db
        .list_messages(&sid, None, 10)
        .await
        .expect("list")
        .expect("session exists");
    assert_eq!(page.messages.len(), 4);
    assert_eq!(page.messages[1].role, MessageRole::Assistant);
    assert_eq!(page.messages[1].stop_reason.as_deref(), Some("tool_use"));
    // 纯工具轮：无空 text 块，仅 tool_use 块。
    assert_eq!(
        page.messages[1].content,
        vec![StoredBlock::ToolUse {
            id: "call-1".into(),
            name: "Echo".into(),
            input: json!({ "text": "hi" }),
        }]
    );
    assert_eq!(page.messages[2].role, MessageRole::User);
    assert_eq!(
        page.messages[2].content,
        vec![StoredBlock::ToolResult {
            tool_use_id: "call-1".into(),
            content: "hi".into(),
            is_error: false,
            metadata: None,
        }]
    );
    let invocation: (String, String, String, String, i64) = db
        .with_conn_blocking(|conn| {
            conn.query_row(
                "SELECT status,input_json,output_ref,cleanup_status,version
                   FROM tool_invocations WHERE tool_use_id='call-1'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .map_err(Into::into)
        })
        .expect("durable tool invocation");
    assert_eq!(invocation.0, "succeeded");
    assert_eq!(invocation.1, r#"{"text":"hi"}"#);
    assert_eq!(
        invocation.2,
        format!("message:{}", page.messages[2].id),
        "the immutable ToolResult message is the invocation output reference"
    );
    // Echo is an in-process pure tool: no process, MCP connection, or other
    // supervised resource was allocated. `confirmed` is reserved for calls
    // that allocated at least one resource and positively released all of it.
    assert_eq!(invocation.3, "notRequired");
    assert_eq!(invocation.4, 2, "preparing -> running -> succeeded");
    let attribution: (String, String, String, String) = db
        .with_conn_blocking(|conn| {
            conn.query_row(
                "SELECT m.task_id,m.run_id,t.id,r.id
                   FROM messages m
                   JOIN run_envelopes r ON r.id=m.run_id
                   JOIN tasks t ON t.id=r.task_id
                  WHERE m.origin='tool_result' AND m.session_id=?1",
                [&sid],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(Into::into)
        })
        .expect("tool-result attribution");
    assert_eq!(attribution.0, attribution.2, "message task attribution");
    assert_eq!(attribution.1, attribution.3, "message Run attribution");
    let resource_count: i64 = db
        .with_conn_blocking(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM execution_resources er
                   JOIN tool_invocations ti ON ti.invocation_id=er.invocation_id
                  WHERE ti.tool_use_id='call-1'",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .expect("resource count");
    assert_eq!(
        resource_count, 0,
        "Echo must not invent a physical resource"
    );
}

#[tokio::test]
async fn successful_builtin_write_is_automatically_registered_as_sqlite_artifact() {
    // `setup_with_tools` deliberately gives this Session `/tmp` as its
    // authorized workspace. On macOS `std::env::temp_dir()` points at
    // `/var/folders/...`, which must (correctly) fail Artifact containment.
    let path = std::path::Path::new("/tmp").join(format!(
        "zk-engine-artifact-write-{}-{}.txt",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let _ = std::fs::remove_file(&path);
    let content = "durable artifact";
    let input = json!({
        "file_path": path.to_string_lossy(),
        "content": content,
    })
    .to_string();
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(WriteFileTool::new()));
    let (engine, _provider, _sink, db, sid) = setup_with_tools(
        vec![
            events(vec![
                ProviderEvent::ToolUseStart {
                    id: "write-artifact".into(),
                    name: "Write".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "write-artifact".into(),
                    delta: input,
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::ToolUse,
                    usage: Some(usage(3, 2)),
                },
            ]),
            events(vec![
                ProviderEvent::TextDelta {
                    text: "written".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(usage(2, 1)),
                },
            ]),
        ],
        registry,
    )
    .await;

    run(&engine, &sid, "write an artifact").await;
    let run_id = db
        .find_runs_by_session(&sid, 10)
        .await
        .expect("runs")
        .first()
        .expect("run")
        .id
        .clone();
    let manifest = db
        .find_artifact_manifest_by_run(&run_id)
        .await
        .expect("manifest query")
        .expect("automatic manifest");
    assert_eq!(manifest.state, "sealed");
    assert_eq!(manifest.entries.len(), 1);
    let entry = &manifest.entries[0];
    assert_eq!(entry.tool_use_id, "write-artifact");
    assert_eq!(entry.operation, "created");
    assert_eq!(
        entry.file_size,
        Some(i64::try_from(content.len()).expect("fixture size fits i64"))
    );
    assert_eq!(
        entry.sealed_hash,
        Some(zk_tools::sha256_hex(content.as_bytes()))
    );
    let invocation_id = entry
        .producer_invocation_id
        .as_deref()
        .expect("physical producer invocation");
    let invocation_status = db
        .with_conn_blocking(|conn| {
            conn.query_row(
                "SELECT status FROM tool_invocations WHERE invocation_id=?1",
                [invocation_id],
                |row| row.get::<_, String>(0),
            )
            .map_err(Into::into)
        })
        .expect("invocation status");
    assert_eq!(invocation_status, "succeeded");
    assert_eq!(
        std::fs::canonicalize(&path).expect("canonical artifact"),
        std::path::PathBuf::from(&entry.canonical_path)
    );
    std::fs::remove_file(&path).expect("cleanup artifact");
}

#[tokio::test]
async fn failed_builtin_write_does_not_create_an_artifact_manifest() {
    let path = std::path::Path::new("/tmp").join(format!(
        "zk-engine-artifact-failed-{}-{}.txt",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let _ = std::fs::remove_file(&path);
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(WriteFileTool::new()));
    let (engine, _provider, _sink, db, sid) = setup_with_tools(
        vec![
            events(vec![
                ProviderEvent::ToolUseStart {
                    id: "write-failed".into(),
                    name: "Write".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "write-failed".into(),
                    delta: json!({ "file_path": path.to_string_lossy() }).to_string(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::ToolUse,
                    usage: Some(usage(3, 2)),
                },
            ]),
            events(vec![ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(usage(2, 1)),
            }]),
        ],
        registry,
    )
    .await;

    run(&engine, &sid, "attempt an invalid write").await;
    let run_id = db
        .find_runs_by_session(&sid, 10)
        .await
        .expect("runs")
        .first()
        .expect("run")
        .id
        .clone();
    assert!(
        db.find_artifact_manifest_by_run(&run_id)
            .await
            .expect("manifest query")
            .is_none()
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn production_web_tools_register_only_successful_bounded_research_receipts() {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(WebSearchTool::new(Arc::new(FixtureSearchBackend))));
    registry.register(Arc::new(WebFetchTool::new(Arc::new(FixtureFetchPort))));
    let (engine, _provider, _sink, db, sid) = setup_with_tools(
        vec![
            events(vec![
                ProviderEvent::ToolUseStart {
                    id: "research-search".into(),
                    name: "WebSearch".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "research-search".into(),
                    delta: json!({"query": "durable agents", "limit": 1}).to_string(),
                },
                ProviderEvent::ToolUseStart {
                    id: "research-fetch".into(),
                    name: "WebFetch".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "research-fetch".into(),
                    delta: json!({"url": "https://example.org/report"}).to_string(),
                },
                ProviderEvent::ToolUseStart {
                    id: "research-failed".into(),
                    name: "WebSearch".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "research-failed".into(),
                    delta: json!({"query": " "}).to_string(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::ToolUse,
                    usage: Some(usage(5, 3)),
                },
            ]),
            events(vec![
                ProviderEvent::TextDelta {
                    text: "research complete".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(usage(2, 1)),
                },
            ]),
        ],
        registry,
    )
    .await;

    run(&engine, &sid, "research durable agents").await;
    let root = db
        .find_runs_by_session(&sid, 10)
        .await
        .expect("runs")
        .into_iter()
        .find(|run| run.parent_run_id.is_none())
        .expect("root run");
    let projection = db
        .find_research_projection_by_root_run(&root.id, &sid)
        .await
        .expect("research query")
        .expect("owned projection");
    assert_eq!(projection.captures.len(), 2);
    assert_eq!(projection.sources.len(), 2);
    assert_eq!(projection.findings.len(), 2);
    assert!(projection.sources.iter().any(|source| {
        source.source_kind == "searchResult"
            && source.url == "https://example.com/agents"
            && source.title.as_deref() == Some("Durable agents")
    }));
    assert!(projection.sources.iter().any(|source| {
        source.source_kind == "fetchedPage"
            && source.url == "https://example.org/report"
            && source.title.as_deref() == Some("Agent report")
    }));
    assert!(
        projection
            .findings
            .iter()
            .all(|finding| finding.excerpt.len() <= zk_db::MAX_RESEARCH_EXCERPT_BYTES)
    );
    let (failed_status, failed_capture_count): (String, i64) = db
        .with_conn_blocking(|conn| {
            conn.query_row(
                "SELECT invocation.status, \
                        (SELECT COUNT(*) FROM research_captures capture \
                         WHERE capture.producer_invocation_id=invocation.invocation_id) \
                 FROM tool_invocations invocation WHERE invocation.tool_use_id='research-failed'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(Into::into)
        })
        .expect("failed producer state");
    assert_eq!(failed_status, "failed");
    assert_eq!(failed_capture_count, 0);
}

async fn run_fixture_verifier(outcome: &str) -> (Arc<RecordingSink>, Db, String, String) {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(FixtureVerifyJourney));
    let tool_use_id = format!("verify-{outcome}");
    let (engine, _provider, sink, db, sid) = setup_with_tools(
        vec![
            events(vec![
                ProviderEvent::ToolUseStart {
                    id: tool_use_id.clone(),
                    name: "VerifyJourney".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: tool_use_id.clone(),
                    delta: json!({"outcome": outcome}).to_string(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::ToolUse,
                    usage: Some(usage(3, 2)),
                },
            ]),
            events(vec![
                ProviderEvent::TextDelta {
                    text: "verification handled".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(usage(2, 1)),
                },
            ]),
        ],
        registry,
    )
    .await;
    run(&engine, &sid, "verify the page").await;
    (sink, db, sid, tool_use_id)
}

#[tokio::test]
async fn verifier_receipt_commits_only_after_a_succeeded_invocation_for_pass_and_fail() {
    for (outcome, expected_verdict, expected_result_error) in
        [("passed", "verified", false), ("failed", "failed", true)]
    {
        let (sink, db, sid, tool_use_id) = run_fixture_verifier(outcome).await;
        let evidence = db
            .find_evidence_by_session(&sid)
            .await
            .expect("evidence query");
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].verdict, expected_verdict);
        let producer = evidence[0]
            .producer_invocation_id
            .as_deref()
            .expect("bundle producer");
        assert_eq!(
            evidence[0].items[0].producer_invocation_id.as_deref(),
            Some(producer)
        );
        let status: String = db
            .with_conn_blocking({
                let producer = producer.to_owned();
                move |conn| {
                    conn.query_row(
                        "SELECT status FROM tool_invocations WHERE invocation_id=?1",
                        [producer],
                        |row| row.get(0),
                    )
                    .map_err(Into::into)
                }
            })
            .expect("producer status");
        assert_eq!(status, "succeeded");
        let result = sink
            .pushed
            .lock()
            .expect("sink")
            .iter()
            .find_map(|(_, message)| match message {
                ServerMessage::ToolResult {
                    tool_use_id: id,
                    result,
                } if id == &tool_use_id => Some(result.clone()),
                _ => None,
            })
            .expect("published result after evidence commit");
        assert_eq!(result.is_error, expected_result_error);
    }
}

#[tokio::test]
async fn successful_verifier_without_a_valid_receipt_publishes_no_completion_or_evidence() {
    let (sink, db, sid, tool_use_id) = run_fixture_verifier("invalid").await;
    assert!(
        db.find_evidence_by_session(&sid)
            .await
            .expect("evidence query")
            .is_empty()
    );
    assert!(
        !sink
            .pushed
            .lock()
            .expect("sink")
            .iter()
            .any(|(_, message)| matches!(
                message,
                ServerMessage::ToolResult { tool_use_id: id, .. } if id == &tool_use_id
            )),
        "completion must not publish when authoritative evidence registration fails"
    );
}

#[tokio::test]
async fn disabled_coordinator_service_leaves_normal_root_prompt_unchanged() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(vec![events(vec![
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(2, 1)),
        },
    ])]));
    let sink = Arc::new(RecordingSink::default());
    let coordinator = Arc::new(CoordinatorService::new(
        &FeatureFlags::with_defaults(),
        false,
    ));
    let engine = Arc::new(
        Engine::new(
            db,
            Arc::clone(&provider) as Arc<dyn ChatProvider>,
            sink as Arc<dyn MessageSink>,
        )
        .with_coordinator(coordinator),
    );

    run(&engine, &session.id, "normal mode").await;

    let request = provider.request_at(0);
    assert!(request.system_prompt.is_none());
    assert_eq!(request.system_segments.len(), 2);
    assert!(
        !request
            .system_text()
            .expect("normal generated prompt")
            .contains("# Coordinator 模式")
    );
}

#[tokio::test]
async fn coordinator_prompt_is_root_dynamic_suffix_once_before_request_append() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(vec![events(vec![
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(2, 1)),
        },
    ])]));
    let sink = Arc::new(RecordingSink::default());
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(CatalogAgentTool));
    registry.register(Arc::new(EchoTool));
    let coordinator = Arc::new(CoordinatorService::new(
        &FeatureFlags::with_defaults(),
        true,
    ));
    let engine = Arc::new(
        Engine::with_tools(
            db.clone(),
            Arc::clone(&provider) as Arc<dyn ChatProvider>,
            Arc::clone(&sink) as Arc<dyn MessageSink>,
            Arc::new(registry),
        )
        .with_coordinator(coordinator),
    );
    let service = ConversationService::new(engine, db);
    service
        .execute_with_options(
            &session.id,
            "handle without delegation".into(),
            ConversationRunOptions {
                append_system_prompt: Some("REQUEST_APPEND_SENTINEL".into()),
                allowed_tools: Some(HashSet::from(["Echo".to_owned()])),
                ..ConversationRunOptions::default()
            },
        )
        .await;

    let request = provider.request_at(0);
    let expected = build_coordinator_prompt(&BTreeSet::from(["Echo".to_owned()]));
    assert!(request.system_prompt.is_none());
    assert_eq!(request.system_segments.len(), 2);
    assert!(!request.system_segments[0].text.contains(expected.as_str()));
    let dynamic = &request.system_segments[1].text;
    let base_position = dynamic.find("主工作目录：/tmp").expect("base dynamic");
    let coordinator_position = dynamic
        .find(expected.as_str())
        .expect("coordinator dynamic section");
    let append_position = dynamic
        .find("REQUEST_APPEND_SENTINEL")
        .expect("request append");
    assert!(base_position < coordinator_position);
    assert!(coordinator_position < append_position);
    assert_eq!(dynamic.matches(expected.as_str()).count(), 1);
    assert!(expected.contains("本轮不得尝试委派"));
}

#[tokio::test]
async fn explicit_system_override_excludes_enabled_coordinator_prompt() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("create session");
    let provider = Arc::new(MockProvider::new(vec![events(vec![
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(2, 1)),
        },
    ])]));
    let sink = Arc::new(RecordingSink::default());
    let coordinator = Arc::new(CoordinatorService::new(
        &FeatureFlags::with_defaults(),
        true,
    ));
    let engine = Arc::new(
        Engine::new(
            db.clone(),
            Arc::clone(&provider) as Arc<dyn ChatProvider>,
            sink as Arc<dyn MessageSink>,
        )
        .with_coordinator(coordinator),
    );
    let service = ConversationService::new(engine, db);
    service
        .execute_with_options(
            &session.id,
            "use explicit prompt".into(),
            ConversationRunOptions {
                system_prompt: Some("EXPLICIT_ROOT_PROMPT".into()),
                append_system_prompt: Some("REQUEST_APPEND_SENTINEL".into()),
                ..ConversationRunOptions::default()
            },
        )
        .await;

    let request = provider.request_at(0);
    assert_eq!(
        request.system_prompt.as_deref(),
        Some("EXPLICIT_ROOT_PROMPT\n\nREQUEST_APPEND_SENTINEL")
    );
    assert!(request.system_segments.is_empty());
    assert!(
        !request
            .system_text()
            .expect("explicit prompt")
            .contains("# Coordinator 模式")
    );
}

#[tokio::test]
async fn conversation_service_applies_prompt_tool_and_turn_limits() {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(EchoTool));
    let (engine, provider, sink, db, sid) = setup_with_tools(
        vec![events(vec![
            ProviderEvent::ToolUseStart {
                id: "call-limited".into(),
                name: "Echo".into(),
            },
            ProviderEvent::ToolInputDelta {
                id: "call-limited".into(),
                delta: r#"{"text":"hi"}"#.into(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: Some(usage(2, 1)),
            },
        ])],
        registry,
    )
    .await;
    let service = ConversationService::new(Arc::clone(&engine), db);
    let outcome = service
        .execute_with_options(
            &sid,
            "limited query".into(),
            ConversationRunOptions {
                max_turns: 1,
                system_prompt: Some("query system".into()),
                append_system_prompt: Some("query appendix".into()),
                allowed_tools: Some(HashSet::from(["Echo".to_owned()])),
                disallowed_tools: HashSet::new(),
                thinking: None,
                token_budget: None,
                cost_budget_nanos_usd: None,
                deadline: None,
            },
        )
        .await;

    assert_eq!(
        provider.request_count(),
        1,
        "maxTurns stops the second LLM turn"
    );
    let request = provider.request_at(0);
    assert_eq!(
        request.system_prompt.as_deref(),
        Some("query system\n\nquery appendix")
    );
    assert_eq!(
        request
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec!["Echo"]
    );
    assert_eq!(
        sink.kinds()
            .iter()
            .filter(|kind| **kind == "message_complete")
            .count(),
        1
    );
    assert_eq!(outcome.session_id, sid);
}

#[tokio::test]
async fn interrupt_mid_stream_terminates_within_deadline() {
    // 首流：一条增量后永不终止（interrupt 是唯一出路）。
    let first: BoxStream<'static, ProviderEvent> = stream::iter(vec![ProviderEvent::TextDelta {
        text: "partial".into(),
    }])
    .chain(stream::pending())
    .boxed();
    let (engine, provider, sink, db, sid) = setup(vec![
        Ok(first),
        events(vec![ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(1, 1)),
        }]),
    ])
    .await;
    let handle = engine.spawn_user_message(&sid, "go".into());
    while !sink.kinds().contains(&"stream_delta") {
        tokio::task::yield_now().await;
    }
    engine.handle_client_message(
        &sid,
        ClientMessage::Interrupt {
            is_submit_interrupt: None,
        },
    );
    // 交付判据：500ms 内流终止。
    tokio::time::timeout(Duration::from_millis(500), handle)
        .await
        .expect("run must terminate within 500ms")
        .expect("run task joins");

    let kinds = sink.kinds();
    assert!(kinds.contains(&"interrupt_ack"), "kinds: {kinds:?}");
    // 无 Finish 误发：取消路径不得走失败序列（无 error）。
    assert!(!kinds.contains(&"error"), "kinds: {kinds:?}");
    // 终态照常提交（end_turn）；流中中断的部分助手**不落库**。
    let complete_index = kinds
        .iter()
        .position(|kind| *kind == "message_complete")
        .expect("message_complete pushed");
    let complete = sink.json_at(complete_index);
    assert_eq!(complete["stopReason"], "end_turn");
    assert_eq!(
        complete["committedMessages"]
            .as_array()
            .expect("committed")
            .len(),
        1
    );
    let page = db
        .list_messages(&sid, None, 10)
        .await
        .expect("list")
        .expect("session exists");
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].role, MessageRole::User);

    // busy 槽已释放：新 run 正常完成。
    run(&engine, &sid, "again").await;
    assert_eq!(provider.request_count(), 2);
    assert_eq!(sink.kinds().last(), Some(&"session_list_updated"));
}

#[tokio::test]
async fn interrupt_during_tool_phase_synthesizes_fix02_results() {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(NeverendingTool));
    let (engine, _provider, sink, db, sid) = setup_with_tools(
        vec![events(vec![
            ProviderEvent::ToolUseStart {
                id: "call-1".into(),
                name: "Hang".into(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: Some(usage(2, 1)),
            },
        ])],
        registry,
    )
    .await;
    let handle = engine.spawn_user_message(&sid, "hang it".into());
    while !sink.kinds().contains(&"tool_use_input") {
        tokio::task::yield_now().await;
    }
    engine.handle_client_message(
        &sid,
        ClientMessage::Interrupt {
            is_submit_interrupt: None,
        },
    );
    tokio::time::timeout(Duration::from_millis(500), handle)
        .await
        .expect("run must terminate within 500ms")
        .expect("run task joins");

    let kinds = sink.kinds();
    // FIX-02：合成结果**落库不推送**——WS 序列无 tool_result。
    assert!(!kinds.contains(&"tool_result"), "kinds: {kinds:?}");
    assert!(kinds.contains(&"interrupt_ack"), "kinds: {kinds:?}");
    let complete_index = kinds
        .iter()
        .position(|kind| *kind == "message_complete")
        .expect("message_complete pushed");
    let complete = sink.json_at(complete_index);
    assert_eq!(complete["stopReason"], "end_turn");
    // committed：user + assistant(tool_use) + 合成结果 + USER_INTERRUPT 通知。
    let committed = complete["committedMessages"].as_array().expect("committed");
    assert_eq!(committed.len(), 4);
    let page = db
        .list_messages(&sid, None, 10)
        .await
        .expect("list")
        .expect("session exists");
    assert_eq!(page.messages.len(), 4);
    assert_eq!(
        page.messages[2].content,
        vec![StoredBlock::ToolResult {
            tool_use_id: "call-1".into(),
            content: "<tool_use_error>Interrupted by user</tool_use_error>".into(),
            is_error: true,
            metadata: None,
        }]
    );
    assert_eq!(
        page.messages[3].content,
        vec![StoredBlock::Text {
            text: "[User interrupted the assistant's response]".into(),
        }]
    );
    let invocation: (String, String, i64) = db
        .with_conn_blocking(|conn| {
            conn.query_row(
                "SELECT status,cleanup_status,version
                   FROM tool_invocations WHERE tool_use_id='call-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(Into::into)
        })
        .expect("cancelled invocation");
    assert_eq!(invocation.0, "cancelled");
    assert_eq!(invocation.1, "unconfirmed");
    assert_eq!(invocation.2, 2, "preparing -> running -> cancelled");
}

/// 工具入参 JSON 非法（flush 致命路径）：`query_error` + retryable=true
/// （旧 catch 分支恒发 true）→ durable error result → `message_complete`。
#[tokio::test]
async fn invalid_tool_arguments_json_is_retryable_query_error() {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(EchoTool));
    let (engine, _provider, sink, db, sid) = setup_with_tools(
        vec![events(vec![
            ProviderEvent::ToolUseStart {
                id: "call-1".into(),
                name: "Echo".into(),
            },
            ProviderEvent::ToolInputDelta {
                id: "call-1".into(),
                delta: "{not json".into(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: Some(usage(1, 1)),
            },
        ])],
        registry,
    )
    .await;
    run(&engine, &sid, "echo hi").await;

    // Finish usage is published, but invalid JSON never crossed the durable
    // invocation boundary and therefore must not create a ghost tool start.
    assert_eq!(
        sink.kinds(),
        vec![
            "cost_update",
            "error",
            "message_complete",
            "session_list_updated"
        ]
    );
    let error = sink.json_at(1);
    assert_eq!(error["code"], "query_error");
    assert_eq!(error["retryable"], true);
    assert!(
        error["message"]
            .as_str()
            .expect("message str")
            .contains("INVALID_TOOL_INPUT_JSON")
    );
    // 助手消息不落库（失败发生在落库前），用户消息保留。
    let page = db
        .list_messages(&sid, None, 10)
        .await
        .expect("list")
        .expect("session exists");
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].role, MessageRole::User);
    let invocations: i64 = db
        .with_conn_blocking(|connection| {
            connection
                .query_row("SELECT COUNT(*) FROM tool_invocations", [], |row| {
                    row.get(0)
                })
                .map_err(Into::into)
        })
        .expect("count invocations");
    assert_eq!(invocations, 0);
}

#[tokio::test]
async fn provider_failure_after_tool_draft_never_publishes_tool_start() {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(EchoTool));
    let (engine, _provider, sink, db, sid) = setup_with_tools(
        vec![events(vec![
            ProviderEvent::ToolUseStart {
                id: "draft-only".into(),
                name: "Echo".into(),
            },
            ProviderEvent::Error {
                error: ProviderError::Network {
                    message: "connection reset after draft".into(),
                },
            },
        ])],
        registry,
    )
    .await;

    run(&engine, &sid, "draft then fail").await;

    assert!(!sink.kinds().contains(&"tool_use_start"));
    let invocations: i64 = db
        .with_conn_blocking(|connection| {
            connection
                .query_row("SELECT COUNT(*) FROM tool_invocations", [], |row| {
                    row.get(0)
                })
                .map_err(Into::into)
        })
        .expect("count invocations");
    assert_eq!(invocations, 0);
}

#[tokio::test]
async fn tool_result_storage_failure_never_publishes_tool_or_run_completion() {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(EchoTool));
    let (engine, provider, sink, db, sid) = setup_with_tools(
        vec![events(vec![
            ProviderEvent::ToolUseStart {
                id: "durable-call".into(),
                name: "Echo".into(),
            },
            ProviderEvent::ToolInputDelta {
                id: "durable-call".into(),
                delta: r#"{"text":"hello"}"#.into(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: Some(usage(4, 2)),
            },
        ])],
        registry,
    )
    .await;
    db.with_conn_blocking(|conn| {
        conn.execute_batch(
            "CREATE TRIGGER inject_tool_result_message_failure
             BEFORE INSERT ON messages WHEN NEW.origin='tool_result'
             BEGIN SELECT RAISE(FAIL,'injected tool result message failure'); END;",
        )?;
        Ok(())
    })
    .expect("install tool-result failpoint");

    run(&engine, &sid, "echo once").await;

    assert_eq!(provider.request_count(), 1);
    let kinds = sink.kinds();
    assert!(!kinds.contains(&"tool_result"));
    assert!(!kinds.contains(&"message_complete"));
    let durable: (String, String, String, i64) = db
        .with_conn_blocking(|conn| {
            conn.query_row(
                "SELECT t.status,r.status,i.status,
                    (SELECT COUNT(*) FROM messages m WHERE m.origin='tool_result')
                 FROM tasks t
                 JOIN run_envelopes r ON r.id=t.current_run_id
                 JOIN tool_invocations i ON i.run_id=r.id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(Into::into)
        })
        .expect("quarantined tool state");
    assert_eq!(
        durable,
        (
            "needsAttention".to_owned(),
            "interrupted".to_owned(),
            "running".to_owned(),
            0,
        )
    );
}

#[tokio::test]
async fn unknown_tool_feeds_error_result_and_continues() {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(EchoTool));
    let (engine, provider, sink, db, sid) = setup_with_tools(
        vec![
            events(vec![
                ProviderEvent::ToolUseStart {
                    id: "call-9".into(),
                    name: "Nope".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::ToolUse,
                    usage: Some(usage(1, 1)),
                },
            ]),
            events(vec![
                ProviderEvent::TextDelta {
                    text: "recovered".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(usage(1, 1)),
                },
            ]),
        ],
        registry,
    )
    .await;
    run(&engine, &sid, "use nope").await;

    // Batch 0 Step 0-6：每次 Finish 携带 usage → push_cost_update 各落一次。
    assert_eq!(
        sink.kinds(),
        vec![
            "cost_update",
            "tool_use_start",
            "tool_result",
            "stream_delta",
            "cost_update",
            "message_complete",
            "session_list_updated",
        ]
    );
    // 未知工具不会通过准入，因此始终停留在 preparing，不能伪造 running input。
    // 未知工具：错误结果回喂模型（旧逐字文案，含可用工具清单）。
    let result = sink.json_at(2);
    assert_eq!(result["result"]["isError"], true);
    let content = result["result"]["content"].as_str().expect("content");
    assert!(content.contains("Tool 'Nope' does not exist"), "{content}");
    assert!(content.contains("[Echo]"), "{content}");
    let request = provider.request_at(1);
    assert_eq!(request.messages[1].tool_calls[0].arguments, "{}");
    assert!(request.messages[2].content.contains("does not exist"));
    let page = db
        .list_messages(&sid, None, 10)
        .await
        .expect("list")
        .expect("session exists");
    assert_eq!(page.messages.len(), 4);
}

/// 输出预算按会话模型的能力表取值（对照旧
/// `QueryConfig.getRecommendedMaxTokens` = `min(模型输出上限, 65536)`）：
/// kimi-k3（131072）夹紧为 65536，不再是 `ChatRequest::new` 的 8192 默认档。
#[tokio::test]
async fn max_tokens_budget_follows_model_capabilities() {
    let (engine, provider, _sink, _db, sid) = setup_with_model(
        vec![events(vec![ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(1, 1)),
        }])],
        "kimi-k3",
    )
    .await;
    run(&engine, &sid, "hi").await;
    assert_eq!(provider.request_at(0).max_tokens, 65536);

    // 输出上限低于 65536 的模型按原值下发（不抬高）。
    let (engine, provider, _sink, _db, sid) = setup_with_model(
        vec![events(vec![ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(1, 1)),
        }])],
        "moonshot-v1-128k",
    )
    .await;
    run(&engine, &sid, "hi").await;
    assert_eq!(provider.request_at(0).max_tokens, 8192);

    // 未知模型走能力表默认值 4096（旧 `ModelCapabilities.DEFAULT` 同值）。
    let (engine, provider, _sink, _db, sid) = setup_with_model(
        vec![events(vec![ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(1, 1)),
        }])],
        "nope-9000",
    )
    .await;
    run(&engine, &sid, "hi").await;
    assert_eq!(provider.request_at(0).max_tokens, 4096);
}

/// 截断毒化链修复主用例（对照旧 `QueryEngine` 步骤 6b + `flushTextBlock`）：
/// thinking 耗尽输出预算 → 空正文 **不写空 text 块**；首次截断升级预算重试；
/// 再截断则注入续写用户消息重试；恢复后正常收尾。
#[tokio::test]
async fn max_tokens_truncation_escalates_then_injects_recovery_message() {
    let (engine, provider, sink, db, sid) = setup_with_model(
        vec![
            events(vec![
                ProviderEvent::ThinkingDelta {
                    thinking: "long reasoning".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::MaxTokens,
                    usage: Some(usage(5, 8192)),
                },
            ]),
            events(vec![ProviderEvent::Finish {
                finish_reason: FinishReason::MaxTokens,
                usage: Some(usage(1, 65536)),
            }]),
            events(vec![
                ProviderEvent::TextDelta {
                    text: "final".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(usage(2, 3)),
                },
            ]),
        ],
        "moonshot-v1-128k",
    )
    .await;
    run(&engine, &sid, "hi").await;

    // 预算档位：首轮按模型能力（8192）→ 截断后升级为 ESCALATED（65536）。
    assert_eq!(provider.request_count(), 3);
    assert_eq!(provider.request_at(0).max_tokens, 8192);
    assert_eq!(provider.request_at(1).max_tokens, 65536);
    assert_eq!(provider.request_at(2).max_tokens, 65536);

    // 第 2 次请求仅升级预算、消息序列不变（空正文助手不入请求）。
    let second = provider.request_at(1);
    let second: Vec<&str> = second
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect();
    assert_eq!(second, vec!["hi"]);
    // 第 3 次请求追加续写提示（逐字为旧 MAX_TOKENS_RECOVERY_MESSAGE）。
    let third: Vec<String> = provider
        .request_at(2)
        .messages
        .iter()
        .map(|message| message.content.clone())
        .collect();
    assert_eq!(
        third,
        vec!["hi".to_owned(), MAX_TOKENS_RECOVERY_MESSAGE.to_owned()]
    );

    // 落库形状：空正文助手不含 text 块（旧 flushTextBlock 从不产出空块）。
    let page = db
        .list_messages(&sid, None, 10)
        .await
        .expect("list")
        .expect("session exists");
    assert_eq!(page.messages.len(), 5);
    assert_eq!(
        page.messages[1].content,
        vec![StoredBlock::Thinking {
            thinking: "long reasoning".into()
        }]
    );
    assert_eq!(page.messages[1].stop_reason.as_deref(), Some("max_tokens"));
    assert!(page.messages[2].content.is_empty());
    assert_eq!(page.messages[3].role, MessageRole::User);
    assert_eq!(
        page.messages[3].content,
        vec![StoredBlock::Text {
            text: MAX_TOKENS_RECOVERY_MESSAGE.to_owned()
        }]
    );
    assert_eq!(
        page.messages[4].content,
        vec![StoredBlock::Text {
            text: "final".into()
        }]
    );

    // 终态：恢复成功 → end_turn；用量跨轮累计。
    let complete = sink.json_at(sink.kinds().len() - 2);
    assert_eq!(complete["type"], "message_complete");
    assert_eq!(complete["stopReason"], "end_turn");
    assert_eq!(complete["usage"]["inputTokens"], 8);
    assert_eq!(complete["usage"]["outputTokens"], 8192 + 65536 + 3);
}

/// 恢复次数上限（旧 `QueryConfig.MAX_OUTPUT_TOKENS_RECOVERY_LIMIT` = 3）：
/// 升级 1 次 + 注入 3 次后仍截断 → 以原 stopReason 终止，不再无限重试。
#[tokio::test]
async fn max_tokens_recovery_stops_at_limit() {
    let truncated = || {
        events(vec![ProviderEvent::Finish {
            finish_reason: FinishReason::MaxTokens,
            usage: Some(usage(1, 1)),
        }])
    };
    let (engine, provider, sink, db, sid) = setup_with_model(
        vec![
            truncated(),
            truncated(),
            truncated(),
            truncated(),
            truncated(),
        ],
        "moonshot-v1-128k",
    )
    .await;
    run(&engine, &sid, "hi").await;

    // 1 次升级 + 3 次注入 = 5 次 provider 调用后终止。
    assert_eq!(provider.request_count(), 5);
    let complete = sink.json_at(sink.kinds().len() - 2);
    assert_eq!(complete["stopReason"], "max_tokens");

    // 注入的续写消息共 3 条（均落库，对照旧 state.addMessage 经监听器持久化）。
    let page = db
        .list_messages(&sid, None, 20)
        .await
        .expect("list")
        .expect("session exists");
    let recovery_count = page
        .messages
        .iter()
        .filter(|record| {
            record.role == MessageRole::User
                && record.content
                    == vec![StoredBlock::Text {
                        text: MAX_TOKENS_RECOVERY_MESSAGE.to_owned(),
                    }]
        })
        .count();
    assert_eq!(recovery_count, 3);
}

/// 已被毒化的历史会话可恢复（对照旧 `MessageNormalizer` Phase 3 /
/// `filterEmptyAssistantMessages`）：历史中空正文 assistant 回放时整条丢弃，
/// 不再以 `content: ""` 触发 provider 400。
#[tokio::test]
async fn poisoned_empty_assistant_history_is_filtered_on_replay() {
    let (engine, provider, _sink, db, sid) = setup(vec![events(vec![
        ProviderEvent::TextDelta { text: "ok".into() },
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(1, 1)),
        },
    ])])
    .await;
    // 旧毒化形状：空 text 块 + stop_reason=max_tokens。
    db.append_message(
        &sid,
        NewMessage {
            role: MessageRole::User,
            content: vec![StoredBlock::Text { text: "old".into() }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .expect("seed user");
    db.append_message(
        &sid,
        NewMessage {
            role: MessageRole::Assistant,
            content: vec![
                StoredBlock::Thinking {
                    thinking: "burned the budget".into(),
                },
                StoredBlock::Text {
                    text: String::new(),
                },
            ],
            stop_reason: Some("max_tokens".to_owned()),
            input_tokens: 1,
            output_tokens: 8192,
        },
    )
    .await
    .expect("seed poisoned assistant");
    // 全空白正文亦丢弃（旧 isBlank 语义）。
    db.append_message(
        &sid,
        NewMessage {
            role: MessageRole::Assistant,
            content: vec![StoredBlock::Text {
                text: "   \n".into(),
            }],
            stop_reason: Some("max_tokens".to_owned()),
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .expect("seed blank assistant");

    run(&engine, &sid, "again").await;

    let replayed = provider.request_at(0);
    let contents: Vec<&str> = replayed
        .messages
        .iter()
        .map(|message| message.content.as_str())
        .collect();
    assert_eq!(contents, vec!["old", "again"]);
    assert!(
        replayed
            .messages
            .iter()
            .all(|message| !message.content.is_empty() || !message.tool_calls.is_empty()),
        "空 content 消息不得进入 provider 请求"
    );
}

/// run 终态回写会话累计用量与成本（费用公式对照旧
/// `CostTrackerService.recordUsage`；旧实现无生产调用方，累计列恒 0）。
#[tokio::test]
async fn commit_run_writes_back_session_usage_totals() {
    let (engine, _provider, _sink, db, sid) = setup_with_model(
        vec![
            events(vec![ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(Usage {
                    input_tokens: 10_000,
                    output_tokens: 2_000,
                    cache_read_input_tokens: 4_000,
                    cache_creation_input_tokens: 7,
                }),
            }]),
            events(vec![ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(usage(1_000, 500)),
            }]),
        ],
        "kimi-k3",
    )
    .await;
    run(&engine, &sid, "hi").await;

    let detail = db
        .get_session(&sid)
        .await
        .expect("get session")
        .expect("session exists");
    assert_eq!(detail.total_usage.input_tokens, 10_000);
    assert_eq!(detail.total_usage.output_tokens, 2_000);
    assert_eq!(detail.total_usage.cache_read_input_tokens, 4_000);
    assert_eq!(detail.total_usage.cache_creation_input_tokens, 7);
    // 0.002/1k 输入 + 0.012/1k 输出 − 缓存读 9 折折让。
    let expected =
        10_000.0 * 0.002 / 1000.0 + 2_000.0 * 0.012 / 1000.0 - 4_000.0 * 0.002 * 0.9 / 1000.0;
    assert!(
        (detail.total_cost_usd - expected).abs() < 1e-9,
        "{} vs {expected}",
        detail.total_cost_usd
    );
    let runs = db
        .find_runs_by_session(&sid, 10)
        .await
        .expect("list first run");
    assert_eq!(runs.len(), 1);
    assert!(
        (runs[0].total_cost_usd - expected).abs() < 1e-9,
        "{} vs {expected}",
        runs[0].total_cost_usd
    );

    // 第二个 run 增量累加（对照旧 CostSummary.add 语义）。
    run(&engine, &sid, "more").await;
    let detail = db
        .get_session(&sid)
        .await
        .expect("get session")
        .expect("session exists");
    assert_eq!(detail.total_usage.input_tokens, 11_000);
    assert_eq!(detail.total_usage.output_tokens, 2_500);
    let expected = expected + 1_000.0 * 0.002 / 1000.0 + 500.0 * 0.012 / 1000.0;
    assert!(
        (detail.total_cost_usd - expected).abs() < 1e-9,
        "{} vs {expected}",
        detail.total_cost_usd
    );
    let runs = db
        .find_runs_by_session(&sid, 10)
        .await
        .expect("list both runs");
    assert_eq!(runs.len(), 2);
    let second_expected = 1_000.0 * 0.002 / 1000.0 + 500.0 * 0.012 / 1000.0;
    assert!(
        (runs[0].total_cost_usd - second_expected).abs() < 1e-9,
        "{} vs {second_expected}",
        runs[0].total_cost_usd
    );
}

/// 413 上下文超限三阶段恢复（2.x `ContextCascade` 接入）：首轮流内返回 413 →
/// 引擎经 `ContextRecovery`（Phase1 CollapseDrain）以更小上下文重试当前轮 →
/// 次轮正常完成。对照旧 `QueryEngine` 413 分支「恢复成功后 continue 主循环」。
///
/// 关键断言：发生了**恰好一次**恢复重试（两次 `chat_stream`），第二次上下文
/// 更小，且未走失败序列（无 `error`，最终 `session_list_updated`）。
#[tokio::test]
async fn context_limit_413_recovers_and_retries() {
    // 预置足量历史（≥ MIN_MESSAGES_FOR_COMPACT，含较大正文），使 413 恢复的
    // 压缩能真正减 token（否则 compact 返回 NoTokenSavings → 恢复耗尽）。
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.8-max-0902", "/tmp")
        .await
        .expect("create session");
    let sid = session.id.clone();
    for i in 0..6 {
        let role = if i % 2 == 0 {
            MessageRole::User
        } else {
            MessageRole::Assistant
        };
        db.append_message(
            &sid,
            NewMessage {
                role,
                content: vec![StoredBlock::Text {
                    text: format!("历史消息 {i} 内容 ").repeat(80),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .expect("seed history");
    }

    let provider = Arc::new(MockProvider::new(vec![
        events(vec![ProviderEvent::Error {
            error: ProviderError::http(
                413,
                "prompt is too long: 300000 tokens > 200000 maximum".into(),
                None,
            ),
        }]),
        events(vec![ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(3, 4)),
        }]),
    ]));
    let sink = Arc::new(RecordingSink::default());
    let engine = Arc::new(Engine::new(
        db.clone(),
        Arc::clone(&provider) as Arc<dyn ChatProvider>,
        Arc::clone(&sink) as Arc<dyn MessageSink>,
    ));

    run(&engine, &sid, "触发 413").await;

    // 恢复重试：无恢复时 413 → 直接失败仅一次调用；恢复成功 → 两次调用。
    assert_eq!(provider.request_count(), 2, "413 应触发恰好一次恢复重试");
    let first = provider.request_at(0);
    let second = provider.request_at(1);
    assert!(
        second.messages.len() < first.messages.len(),
        "恢复后应携带更小上下文：{} → {}",
        first.messages.len(),
        second.messages.len()
    );

    // 未走失败序列；Phase1 CollapseDrain 是内部恢复步骤，不投影可见压缩事件。
    let kinds = sink.kinds();
    assert!(
        !kinds.contains(&"error"),
        "恢复成功不应推送 error: {kinds:?}"
    );
    assert!(!kinds.contains(&"compact_start"), "{kinds:?}");
    assert!(!kinds.contains(&"compact_complete"), "{kinds:?}");
    assert!(!kinds.contains(&"compact_event"), "{kinds:?}");
    assert_eq!(kinds.last(), Some(&"session_list_updated"));
}

/// 白名单可微压缩工具桩：名称对齐 `MicroCompactService.COMPACTABLE_TOOLS` 的
/// `Read`，单条结果足以超过轻量压缩的 1024-token 净收益门槛，但不触发 L0
/// Snip（预算按上下文窗口 30% 计）或轮末摘要器截断（软上限 18000 字符）。
struct FakeReadTool;

impl Tool for FakeReadTool {
    fn name(&self) -> &'static str {
        "Read"
    }

    fn description(&self) -> &'static str {
        "stub read for micro-compact wiring test"
    }

    fn parameters(&self) -> serde_json::Value {
        json!({ "type": "object" })
    }

    fn execute(&self, _input: serde_json::Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async { ToolOutput::ok("file contents ".repeat(400)) })
    }
}

/// L1/L1.5 达到净收益门槛后仍在低压力上下文中落地，但只写 checkpoint，不能
/// 发送 `compact_start` / `compact_complete` / `compact_event` 污染根会话。
///
/// 以 L1 `MicroCompact` 为可观测探针：6 个工具轮后第 7 次请求的上下文共 13 条
/// （user + 6 ×（assistant `tool_calls` + tool）），保护尾部 10 →
/// `boundary = 3`，恰好首个工具结果（index 2）落入可清除区并贡献 >1024 token。
#[tokio::test]
async fn pre_api_lightweight_compaction_is_silent_below_auto_threshold() {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(FakeReadTool));

    let mut scripts = Vec::new();
    for i in 0..6 {
        scripts.push(events(vec![
            ProviderEvent::ToolUseStart {
                id: format!("call-{i}"),
                name: "Read".into(),
            },
            ProviderEvent::ToolInputDelta {
                id: format!("call-{i}"),
                delta: "{}".into(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: Some(usage(3, 2)),
            },
        ]));
    }
    scripts.push(events(vec![
        ProviderEvent::TextDelta {
            text: "read done".into(),
        },
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(usage(5, 4)),
        },
    ]));

    let (engine, provider, sink, _db, sid) = setup_with_tools(scripts, registry).await;
    run(&engine, &sid, "read six times").await;

    assert_eq!(provider.request_count(), 7, "6 工具轮 + 1 收尾轮");
    let last = provider.request_at(6);
    assert_eq!(last.messages.len(), 13, "user + 6 ×（assistant + tool）");

    // 前置事实：上下文连第一级警告线（阈值 70%）都未触及。
    let warning = calculate_token_warning_state(&last.messages, &last.model);
    assert!(
        !warning.above_warning_threshold,
        "探针上下文须远低于警告线，否则无法区分「无条件执行」与「阈值触发」：{warning:?}"
    );

    // 达到净收益门槛后，越过保护尾部边界的工具结果被清除。
    assert_eq!(last.messages[2].role, Role::Tool);
    assert_eq!(last.messages[2].content, CLEARED_MESSAGE);
    // 保护尾部内的工具结果原样保留（旧 `MICRO_COMPACT_PROTECTED_TAIL = 10`）。
    assert_eq!(last.messages[12].role, Role::Tool);
    assert_eq!(last.messages[12].content, "file contents ".repeat(400));
    assert!(
        last.messages[3..]
            .iter()
            .all(|message| message.content != CLEARED_MESSAGE),
        "仅越界的单条结果被清除，保护尾部内不受影响"
    );
    let kinds = sink.kinds();
    assert!(!kinds.contains(&"compact_start"), "{kinds:?}");
    assert!(!kinds.contains(&"compact_complete"), "{kinds:?}");
    assert!(!kinds.contains(&"compact_event"), "{kinds:?}");
}
