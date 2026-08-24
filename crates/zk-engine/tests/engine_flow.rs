//! zk-engine 引擎集成测试（零网络）：Phase 1 单轮回归 + 2.2 多轮工具循环
//! 与 interrupt。
//!
//! `MockChatProvider` 手写事件流喂入，`RecordingSink` 录制下行序列；
//! 消息 JSON 形状断言以 zk-protocol serde 输出为唯一权威。

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use zk_db::Db;
use zk_db::model::{MessageRole, NewMessage, StoredBlock};
use zk_engine::{
    CLEARED_MESSAGE, ConversationRunOptions, ConversationService, Engine,
    MAX_TOKENS_RECOVERY_MESSAGE, MessageSink, calculate_token_warning_state,
};
use zk_llm::{ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent, Role};
use zk_protocol::model::Usage;
use zk_protocol::{ClientMessage, ServerMessage};
use zk_tools::{EchoTool, Tool, ToolContext, ToolOutput, ToolRegistry};

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
    setup_with_model(scripts, "qwen3.7-max").await
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

#[tokio::test]
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
    assert_eq!(request.model, "qwen3.7-max");
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
    assert!(system_prompt.contains(" - 你由模型 qwen3.7-max 驱动。\n"));
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
}

#[tokio::test]
async fn production_prompt_includes_workspace_project_memory_language_and_tools() {
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
        "PROJECT_MEMORY_SENTINEL: verification is required",
    )
    .expect("project memory");

    let db = Db::open_in_memory().expect("db");
    db.put_config_value("user_config", r#"{"locale":"zh-CN"}"#)
        .await
        .expect("locale");
    let session = db
        .create_session("qwen3.7-max", workspace.to_str().expect("utf8 workspace"))
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
async fn fatal_stream_error_emits_error_then_fallback_complete() {
    let (engine, _provider, sink, db, sid) = setup(vec![events(vec![ProviderEvent::Error {
        error: ProviderError::Network {
            message: "connection reset".into(),
        },
    }])])
    .await;
    run(&engine, &sid, "boom").await;

    assert_eq!(sink.kinds(), vec!["error", "message_complete"]);
    let error = sink.json_at(0);
    assert_eq!(error["code"], "query_error");
    assert_eq!(error["retryable"], true);
    // 兜底完成信号对齐旧 finally：零用量 + stopReason=error，无 runId/committed。
    let complete = sink.json_at(1);
    assert_eq!(complete["usage"]["inputTokens"], 0);
    assert_eq!(complete["stopReason"], "error");
    assert!(complete.get("runId").is_none());
    assert!(complete.get("committedMessages").is_none());

    // 用户消息保留（provider 调用前已落库），助手消息不落库。
    let page = db
        .list_messages(&sid, None, 10)
        .await
        .expect("list")
        .expect("session exists");
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].role, MessageRole::User);
}

#[tokio::test]
async fn setup_failure_from_chat_stream_is_fatal() {
    let (engine, _provider, sink, _db, sid) = setup(vec![Err(ProviderError::Config {
        message: "provider 'mock' has empty api key".into(),
    })])
    .await;
    run(&engine, &sid, "hi").await;

    assert_eq!(sink.kinds(), vec!["error", "message_complete"]);
    let error = sink.json_at(0);
    assert_eq!(error["code"], "query_error");
    // 致命（Config 类）错误同样标记可重试：旧 catch/onError 恒发 true。
    assert_eq!(error["retryable"], true);
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
        .create_session("qwen3.7-max", "/tmp")
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

    // 完整 WS 序列：start（空 input 占位）→ input（flush 全参）→ result →
    // 续轮文本流 → 终态（对照旧推送点位时序）。
    // Batch 0 Step 0-6：每次 Finish 携带 usage → push_cost_update 各落一次
    // （首轮 tool_use 结束时、续轮 end_turn 时）。
    assert_eq!(
        sink.kinds(),
        vec![
            "tool_use_start",
            "cost_update",
            "tool_use_input",
            "tool_result",
            "stream_delta",
            "cost_update",
            "message_complete",
            "session_list_updated",
        ]
    );
    let start = sink.json_at(0);
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
}

/// 工具入参 JSON 非法（flush 致命路径）：`query_error` + retryable=true
/// （旧 catch 分支恒发 true）→ 兜底 `message_complete`。
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

    // Batch 0 Step 0-6：Finish 携带 usage → push_cost_update 先于 flush 失败。
    assert_eq!(
        sink.kinds(),
        vec!["tool_use_start", "cost_update", "error", "message_complete"]
    );
    let error = sink.json_at(2);
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
            "tool_use_start",
            "cost_update",
            "tool_use_input",
            "tool_result",
            "stream_delta",
            "cost_update",
            "message_complete",
            "session_list_updated",
        ]
    );
    // 空入参 flush 为空对象。
    assert_eq!(sink.json_at(2)["input"], json!({}));
    // 未知工具：错误结果回喂模型（旧逐字文案，含可用工具清单）。
    let result = sink.json_at(3);
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
        .create_session("qwen3.7-max", "/tmp")
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

    // 未走失败序列，且推送了 compact_complete，最终成功完成。
    let kinds = sink.kinds();
    assert!(
        !kinds.contains(&"error"),
        "恢复成功不应推送 error: {kinds:?}"
    );
    assert!(
        kinds.contains(&"compact_complete"),
        "应推送 compact_complete: {kinds:?}"
    );
    assert_eq!(kinds.last(), Some(&"session_list_updated"));
}

/// 白名单可微压缩工具桩：名称对齐 `MicroCompactService.COMPACTABLE_TOOLS` 的
/// `Read`，结果内容极短——既不触发 L0 Snip（预算按上下文窗口 30% 计），也不
/// 触发轮末摘要器截断（软上限 18000 字符），使 L1 成为唯一可观测的级联层。
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
        Box::pin(async { ToolOutput::ok("file contents") })
    }
}

/// Pre-API 级联 L0-L2 每轮无条件执行（对照旧 `QueryEngine` L647 无外层阈值
/// 守卫 + 旧 `ContextCascade` 类注释「Level 0-1 每次 API 调用前无条件执行」）。
///
/// 以 L1 `MicroCompact` 为可观测探针：6 个工具轮后第 7 次请求的上下文共 13 条
/// （user + 6 ×（assistant `tool_calls` + tool）），保护尾部 10 →
/// `boundary = 3`，恰好首个工具结果（index 2）落入可清除区。断言其被替换为
/// `CLEARED_MESSAGE`，而此时上下文远低于 token 警告线——证明 L1 的执行不受
/// 任何 token 阈值约束（回归前引擎侧的 `above_warning_threshold` 外层守卫会
/// 整体跳过级联，此断言必失败）。
#[tokio::test]
async fn pre_api_cascade_levels_run_every_turn_below_threshold() {
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

    let (engine, provider, _sink, _db, sid) = setup_with_tools(scripts, registry).await;
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

    // L1 每轮无条件执行的可观测证据：越过保护尾部边界的工具结果被清除。
    assert_eq!(last.messages[2].role, Role::Tool);
    assert_eq!(last.messages[2].content, CLEARED_MESSAGE);
    // 保护尾部内的工具结果原样保留（旧 `MICRO_COMPACT_PROTECTED_TAIL = 10`）。
    assert_eq!(last.messages[12].role, Role::Tool);
    assert_eq!(last.messages[12].content, "file contents");
    assert!(
        last.messages[3..]
            .iter()
            .all(|message| message.content != CLEARED_MESSAGE),
        "仅越界的单条结果被清除，保护尾部内不受影响"
    );
}
