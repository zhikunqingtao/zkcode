//! Query transport exercises the production Engine, `SQLite` ledger, hub and HTTP server.
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use zk_db::Db;
use zk_llm::{
    ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent, ProviderRegistry,
};
use zk_protocol::Usage;
use zk_server::{
    config::Config, engine_bridge::wire_engine, routes::build_router, state::AppState,
};

const MODEL: &str = "qwen3.8-max-0902";

#[derive(Default)]
struct Provider {
    release: Arc<Notify>,
    requests: Mutex<Vec<ChatRequest>>,
    calls: AtomicUsize,
    script: Mutex<Option<std::collections::VecDeque<Vec<ProviderEvent>>>>,
}

impl ChatProvider for Provider {
    fn provider_name(&self) -> &'static str {
        "query-fixture"
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request);
        if let Some(script) = self.script.lock().unwrap().as_mut() {
            return Ok(stream::iter(script.pop_front().expect("unexpected model request")).boxed());
        }
        let release = Arc::clone(&self.release);
        Ok(stream::iter([ProviderEvent::TextDelta { text: "first fragment".into() }]).chain(stream::once(async move {
            tokio::select! {
                () = release.notified() => ProviderEvent::Finish { finish_reason: FinishReason::EndTurn,
                    usage: Some(Usage { input_tokens: 23, output_tokens: 7, ..Usage::default() }) },
                () = cancel.cancelled() => ProviderEvent::Error { error: ProviderError::Cancelled },
            }
        })).boxed())
    }
}

struct Fixture {
    path: PathBuf,
    base: String,
    db: Db,
    session: String,
    provider: Arc<Provider>,
    interactions: Arc<zk_server::interaction::DurableInteractionService>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl Fixture {
    async fn new() -> Self {
        Self::with_provider(
            Arc::new(Provider::default()),
            zk_authz::model::PermissionMode::DontAsk,
        )
        .await
    }

    async fn with_provider(provider: Arc<Provider>, mode: zk_authz::model::PermissionMode) -> Self {
        let path = std::env::temp_dir().join(format!("zk-query-stream-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        let db = Db::open(path.join("query.sqlite")).unwrap();
        let mut config = Config::test_config();
        config.default_model = MODEL.into();
        config.workspace_default_root = path.to_string_lossy().into_owned();
        config.scratchpad_system_root = path.join("scratch");
        config.snapshot_dir = Some(path.join("snapshots"));
        config.mcp_registry_path = path.join("mcp.json");
        let mut registry = ProviderRegistry::new();
        registry.register("query-fixture", provider.clone(), vec![MODEL.into()]);
        let state =
            AppState::new(db.clone(), config).with_providers(registry.with_default_model(MODEL));
        state
            .set_startup_epoch(db.begin_runtime_startup_epoch().await.unwrap())
            .unwrap();
        let _engine = wire_engine(&state);
        let session = db
            .create_session(MODEL, path.to_str().unwrap())
            .await
            .unwrap()
            .id;
        state.authz.modes.set_mode(&session, mode).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let interactions = state.authz.interactions.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                build_router(state).into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        Self {
            path,
            base,
            db,
            session,
            provider,
            interactions,
            server,
        }
    }

    fn post(&self, path: &str) -> reqwest::RequestBuilder {
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("{}{path}", self.base))
            .header("origin", "http://127.0.0.1:5273")
    }

    fn body(&self, request: &str) -> Value {
        json!({"prompt":"hello", "sessionId":self.session, "requestId":request, "tools":[], "maxTurns":99, "timeoutSeconds":300})
    }
}

async fn receive_until<S>(stream: &mut S, text: &mut String, marker: &str)
where
    S: futures::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin,
{
    tokio::time::timeout(Duration::from_secs(10), async {
        while !text.contains(marker) {
            let chunk = stream
                .next()
                .await
                .expect("SSE remains open")
                .expect("HTTP frame");
            text.push_str(std::str::from_utf8(&chunk).unwrap());
        }
    })
    .await
    .expect("event must arrive before model completion");
}

fn sse_payload(text: &str, name: &str) -> Value {
    text.split("\n\n")
        .find_map(|frame| {
            frame
                .lines()
                .any(|line| line == format!("event: {name}"))
                .then(|| {
                    let data = frame
                        .lines()
                        .find_map(|line| line.strip_prefix("data: "))
                        .unwrap();
                    serde_json::from_str(data).unwrap()
                })
        })
        .expect("named SSE payload")
}

#[tokio::test]
async fn streaming_is_live_exclusive_and_ledger_backed() {
    let fixture = Fixture::new().await;
    let request = uuid::Uuid::new_v4().to_string();
    let response = fixture
        .post("/api/query/stream")
        .json(&fixture.body(&request))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut stream = response.bytes_stream();
    let mut text = String::new();
    receive_until(&mut stream, &mut text, "first fragment").await;
    assert!(
        !text.contains("event: complete"),
        "incremental text precedes provider finish"
    );

    let rejected = fixture
        .post("/api/query/stream")
        .json(&fixture.body(&uuid::Uuid::new_v4().to_string()))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        rejected.json::<Value>().await.unwrap()["code"],
        "QUERY_BUSY"
    );
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
    assert!(
        fixture.provider.requests.lock().unwrap()[0]
            .tools
            .is_empty()
    );

    fixture.provider.release.notify_one();
    receive_until(&mut stream, &mut text, "event: complete").await;
    assert_eq!(sse_payload(&text, "complete")["success"], true);
    let outcome = sse_payload(&text, "result");
    assert!(outcome["error"].is_null(), "{outcome}");
    assert_eq!(outcome["usage"]["inputTokens"], 23);
    assert_eq!(outcome["result"], "first fragment");
    let run = fixture
        .db
        .find_run_by_id(outcome["runId"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.input_tokens, 23);
    assert_eq!(
        outcome["costUsd"].as_f64().unwrap().to_bits(),
        (f64::from(u32::try_from(run.cost_nanos_usd).expect("bounded fixture cost"))
            / 1_000_000_000.0)
            .to_bits()
    );
    assert_eq!(text.matches("event: complete\n").count(), 1);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn query_deadline_is_timeout_in_rest_sse_and_sqlite() {
    for streaming in [false, true] {
        let fixture = Fixture::new().await;
        let request = uuid::Uuid::new_v4().to_string();
        let mut body = fixture.body(&request);
        body["timeoutSeconds"] = json!(1);
        let path = if streaming {
            "/api/query/stream"
        } else {
            "/api/query"
        };
        let response = fixture
            .post(path)
            .json(&body)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let outcome = if streaming {
            let text = response.text().await.unwrap();
            assert_eq!(text.matches("event: complete\n").count(), 1);
            assert_eq!(sse_payload(&text, "complete")["success"], false);
            sse_payload(&text, "result")
        } else {
            response.json::<Value>().await.unwrap()
        };
        assert_eq!(outcome["error"], "QUERY_TIMEOUT", "{outcome}");
        let run = fixture
            .db
            .find_run_by_id(outcome["runId"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.requested_exit_reason.as_deref(), Some("timeout"));
        assert_eq!(run.exit_reason.as_deref(), Some("timeout"));
        assert_ne!(run.abort_reason.as_deref(), Some("userCancelled"));
        let result = fixture
            .db
            .read_task_result(&run.task_id, None, 0, 1024)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.result.error_code.as_deref(), Some("TIMEOUT"));
        assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(run.cleanup_status, "confirmed");
    }
}

#[tokio::test]
async fn deadline_during_run_end_hook_projects_durable_timeout_everywhere() {
    let provider = Arc::new(Provider::default());
    *provider.script.lock().unwrap() = Some(std::collections::VecDeque::from([vec![
        ProviderEvent::TextDelta {
            text: "answer before the hook".into(),
        },
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(Usage {
                input_tokens: 23,
                output_tokens: 7,
                ..Usage::default()
            }),
        },
    ]]));
    let fixture =
        Fixture::with_provider(provider, zk_authz::model::PermissionMode::AutoApprove).await;
    std::fs::create_dir(fixture.path.join(".zk")).unwrap();
    std::fs::write(fixture.path.join(".zk/hooks.toml"),
        "[[hook]]\nname='slow-end'\nevent='RUN_END'\ncommand='printf started > run-end-started; sleep 10'\ntimeout_secs=15\n").unwrap();
    let mut body = fixture.body(&uuid::Uuid::new_v4().to_string());
    body["timeoutSeconds"] = json!(1);
    let text = fixture
        .post("/api/query/stream")
        .json(&body)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        fixture.path.join("run-end-started").exists(),
        "must expire after answer while the real Hook runs"
    );
    let outcome = sse_payload(&text, "result");
    assert_eq!(outcome["error"], "QUERY_TIMEOUT", "{text}");
    assert_eq!(outcome["stopReason"], "timeout", "{text}");
    assert_eq!(
        sse_payload(&text, "assistant_message")["stopReason"],
        "timeout",
        "{text}"
    );
    let run = fixture
        .db
        .find_run_by_id(outcome["runId"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.exit_reason.as_deref(), Some("timeout"));
    assert_eq!(run.cleanup_status, "confirmed");
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn completed_query_is_not_rewritten_by_a_late_deadline() {
    let provider = Arc::new(Provider::default());
    *provider.script.lock().unwrap() = Some(std::collections::VecDeque::from([vec![
        ProviderEvent::TextDelta {
            text: "complete before deadline".into(),
        },
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: Some(Usage {
                input_tokens: 23,
                output_tokens: 7,
                ..Usage::default()
            }),
        },
    ]]));
    let fixture = Fixture::with_provider(provider, zk_authz::model::PermissionMode::DontAsk).await;
    let mut body = fixture.body(&uuid::Uuid::new_v4().to_string());
    body["timeoutSeconds"] = json!(1);
    let outcome: Value = fixture
        .post("/api/query")
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(outcome["error"].is_null(), "{outcome}");
    let id = outcome["runId"].as_str().unwrap();
    let before = fixture.db.find_run_by_id(id).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let after = fixture.db.find_run_by_id(id).await.unwrap().unwrap();
    assert_eq!(before.status, "completed");
    assert_eq!(after.status, before.status);
    assert_eq!(after.exit_reason, before.exit_reason);
    assert!(after.requested_exit_reason.is_none());
}

#[tokio::test]
async fn explicit_request_stop_has_error_terminal_and_does_not_replay() {
    let fixture = Fixture::new().await;
    let request = uuid::Uuid::new_v4().to_string();
    let response = fixture
        .post("/api/query/stream")
        .json(&fixture.body(&request))
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    let mut text = String::new();
    receive_until(&mut stream, &mut text, "first fragment").await;
    let receipt: Value = fixture
        .post(&format!("/api/query/{request}/cancel"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(receipt["stopRequested"], true);
    assert_eq!(receipt["cleanupConfirmed"], false);
    receive_until(&mut stream, &mut text, "event: complete").await;
    assert_eq!(sse_payload(&text, "complete")["success"], false);
    let outcome = sse_payload(&text, "result");
    assert!(outcome["error"].is_string());
    let run = fixture
        .db
        .find_run_by_id(outcome["runId"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, "cancelled");
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);

    let next = uuid::Uuid::new_v4().to_string();
    let response = fixture
        .post("/api/query/stream")
        .json(&fixture.body(&next))
        .send()
        .await
        .unwrap();
    let mut next_stream = response.bytes_stream();
    let mut next_text = String::new();
    receive_until(&mut next_stream, &mut next_text, "first fragment").await;
    let old: Value = fixture
        .post(&format!("/api/query/{request}/cancel"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(old["stopRequested"], false);
    fixture.provider.release.notify_one();
    receive_until(&mut next_stream, &mut next_text, "event: complete").await;
    assert_eq!(sse_payload(&next_text, "complete")["success"], true);
}

#[tokio::test]
async fn stop_before_post_fences_late_admission() {
    let fixture = Fixture::new().await;
    let request = uuid::Uuid::new_v4().to_string();
    let receipt: Value = fixture
        .post(&format!("/api/query/{request}/cancel"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(receipt["admissionBlocked"], true);
    assert_eq!(receipt["cleanupConfirmed"], false);
    let response = fixture
        .post("/api/query")
        .json(&fixture.body(&request))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        response.json::<Value>().await.unwrap()["code"],
        "QUERY_REQUEST_CANCELLED"
    );
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn jsonl_messages_keep_identity_and_only_execute_once() {
    let fixture = Fixture::new().await;
    let request = uuid::Uuid::new_v4().to_string();
    let mut body = fixture.body(&request);
    body["prompt"] = json!("");
    body["messages"] = json!([{"role":"user","content":"first instruction"},{"role":"user","content":"second instruction"}]);
    body["allowedTools"] = json!([]);
    body.as_object_mut().unwrap().remove("tools");
    let response = fixture
        .post("/api/query/stream")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut stream = response.bytes_stream();
    let mut text = String::new();
    receive_until(&mut stream, &mut text, "first fragment").await;
    let users: Vec<String> = fixture.provider.requests.lock().unwrap()[0]
        .messages
        .iter()
        .filter(|message| message.role == zk_llm::Role::User)
        .map(|message| message.content.clone())
        .collect();
    assert_eq!(users, ["first instruction", "second instruction"]);
    assert!(
        fixture.provider.requests.lock().unwrap()[0]
            .tools
            .is_empty()
    );
    fixture.provider.release.notify_one();
    receive_until(&mut stream, &mut text, "event: complete").await;
    assert_eq!(sse_payload(&text, "complete")["success"], true);
    let session = fixture
        .db
        .get_session(&fixture.session)
        .await
        .unwrap()
        .unwrap();
    let saved: Vec<String> = session
        .messages
        .iter()
        .filter(|record| record.role == zk_db::MessageRole::User)
        .map(|record| {
            record
                .content
                .iter()
                .filter_map(|block| {
                    if let zk_db::StoredBlock::Text { text } = block {
                        Some(text.as_str())
                    } else {
                        None
                    }
                })
                .collect::<String>()
        })
        .collect();
    assert_eq!(saved, users);
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fork_query_executes_only_in_the_independent_permission_preserving_snapshot() {
    let fixture = Fixture::new().await;
    let original = fixture
        .db
        .append_message(
            &fixture.session,
            zk_db::NewMessage {
                meta: None,
                role: zk_db::MessageRole::User,
                content: vec![zk_db::StoredBlock::Text {
                    text: "Historical instruction only".into(),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .unwrap();
    let request = uuid::Uuid::new_v4().to_string();
    let mut body = fixture.body(&request);
    body["forkSession"] = json!(true);
    body["name"] = json!("Independent fork");
    let response = fixture
        .post("/api/query/stream")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut stream = response.bytes_stream();
    let mut text = String::new();
    receive_until(&mut stream, &mut text, "first fragment").await;
    let users: Vec<String> = fixture.provider.requests.lock().unwrap()[0]
        .messages
        .iter()
        .filter(|message| message.role == zk_llm::Role::User)
        .map(|message| message.content.clone())
        .collect();
    assert_eq!(users.len(), 2);
    assert!(users[0].starts_with("[Forked conversation history; reference only"));
    assert!(users[0].ends_with("Historical instruction only"));
    assert_eq!(users[1], "hello");
    fixture.provider.release.notify_one();
    receive_until(&mut stream, &mut text, "event: complete").await;
    assert_eq!(sse_payload(&text, "complete")["success"], true, "{text}");
    let result = sse_payload(&text, "result");
    let target = result["sessionId"].as_str().unwrap();
    assert_ne!(target, fixture.session);
    let source = fixture
        .db
        .get_session(&fixture.session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.messages.len(), 1);
    assert_eq!(source.messages[0].id, original.id);
    let fork = fixture.db.get_session(target).await.unwrap().unwrap();
    let ids = (target.to_owned(), fixture.session.clone());
    let modes: Vec<Option<String>> = fixture
        .db
        .with_reader(move |conn| {
            Ok(vec![
                conn.query_row(
                    "SELECT permission_mode FROM sessions WHERE id=?1",
                    [ids.0],
                    |row| row.get(0),
                )?,
                conn.query_row(
                    "SELECT permission_mode FROM sessions WHERE id=?1",
                    [ids.1],
                    |row| row.get(0),
                )?,
            ])
        })
        .await
        .unwrap();
    assert_eq!(modes, [Some("DONT_ASK".into()), Some("DONT_ASK".into())]);
    assert_ne!(fork.messages[0].id, original.id);
    assert_eq!(fork.messages[0].content, original.content);
    let replay = fixture
        .post("/api/query/stream")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn replayed_project_query_is_rejected_before_creating_another_session() {
    let fixture = Fixture::new().await;
    let project = fixture
        .db
        .create_project("query-project", fixture.path.to_str().unwrap())
        .await
        .unwrap();
    let request = uuid::Uuid::new_v4().to_string();
    let body = json!({"projectId":project.id,"requestId":request,"prompt":"only once","tools":[]});
    let response = fixture
        .post("/api/query/stream")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut stream = response.bytes_stream();
    let mut text = String::new();
    receive_until(&mut stream, &mut text, "first fragment").await;
    fixture.provider.release.notify_one();
    receive_until(&mut stream, &mut text, "event: complete").await;
    let count = fixture
        .db
        .list_sessions(None, 100)
        .await
        .unwrap()
        .sessions
        .len();
    let replay = fixture.post("/api/query").json(&body).send().await.unwrap();
    assert_eq!(replay.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        fixture
            .db
            .list_sessions(None, 100)
            .await
            .unwrap()
            .sessions
            .len(),
        count
    );
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
}

fn event_id(text: &str, name: &str) -> String {
    text.split("\n\n")
        .find_map(|frame| {
            frame
                .lines()
                .any(|line| line == format!("event: {name}"))
                .then(|| {
                    frame
                        .lines()
                        .find_map(|line| line.strip_prefix("id: "))
                        .unwrap()
                        .to_owned()
                })
        })
        .expect("event has recovery identity")
}

#[tokio::test]
async fn disconnected_stream_resumes_existing_execution_without_replaying_input() {
    let fixture = Fixture::new().await;
    let request = uuid::Uuid::new_v4().to_string();
    let response = fixture
        .post("/api/query/stream")
        .json(&fixture.body(&request))
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    let mut text = String::new();
    receive_until(&mut stream, &mut text, "first fragment").await;
    // Simulate an application that acknowledged only query_started before its
    // connection failed. The already-produced delta must be replayed once.
    let cursor = event_id(&text, "query_started");
    drop(stream);
    let client = reqwest::Client::new();
    let invalid = client
        .get(format!("{}/api/query/{request}/stream", fixture.base))
        .header("Last-Event-ID", format!("{}:1", uuid::Uuid::new_v4()))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid.json::<Value>().await.unwrap()["code"],
        "QUERY_CURSOR_INVALID"
    );
    let response = client
        .get(format!("{}/api/query/{request}/stream", fixture.base))
        .header("Last-Event-ID", cursor)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut resumed = response.bytes_stream();
    let mut replay = String::new();
    receive_until(&mut resumed, &mut replay, "first fragment").await;
    assert!(!replay.contains("event: query_started"));
    assert_eq!(replay.matches("event: text\n").count(), 1);
    let repeated = fixture
        .post("/api/query/stream")
        .json(&fixture.body(&request))
        .send()
        .await
        .unwrap();
    assert_eq!(repeated.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
    fixture.provider.release.notify_one();
    receive_until(&mut resumed, &mut replay, "event: complete").await;
    assert_eq!(sse_payload(&replay, "complete")["success"], true);
    assert_eq!(replay.matches("event: complete\n").count(), 1);
    let terminal_cursor = event_id(&replay, "complete");
    assert!(resumed.next().await.is_none());
    let gone = client
        .get(format!("{}/api/query/{request}/stream", fixture.base))
        .header("Last-Event-ID", terminal_cursor)
        .send()
        .await
        .unwrap();
    assert_eq!(gone.status(), reqwest::StatusCode::GONE);
    assert_eq!(
        gone.json::<Value>().await.unwrap()["code"],
        "QUERY_STREAM_UNAVAILABLE"
    );
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unreconnected_stream_stops_the_owned_execution_after_bounded_grace() {
    let fixture = Fixture::new().await;
    let request = uuid::Uuid::new_v4().to_string();
    let response = fixture
        .post("/api/query/stream")
        .json(&fixture.body(&request))
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    let mut text = String::new();
    receive_until(&mut stream, &mut text, "first fragment").await;
    drop(stream);
    // A peer close may first be detected on the next SSE keepalive (10s),
    // followed by the 15s reconnection grace and confirmed engine cleanup.
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let rows = fixture
                .db
                .with_reader(|conn| {
                    Ok(conn.query_row(
                        "SELECT COUNT(*) FROM run_envelopes WHERE status='cancelled'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )?)
                })
                .await
                .unwrap();
            if rows > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("disconnect grace must stop owned execution, not leave it charging indefinitely");
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn temporary_query_is_live_but_never_persists_input_or_hash_and_cannot_resume_after_end() {
    use sha2::{Digest, Sha256};
    let fixture = Fixture::new().await;
    let workspace = std::fs::canonicalize(&fixture.path).unwrap();
    let project = fixture
        .db
        .create_project("temporary-project", workspace.to_str().unwrap())
        .await
        .unwrap();
    let request = uuid::Uuid::new_v4().to_string();
    let secret = format!("PRIVATE_QUERY_BODY_{}", uuid::Uuid::new_v4());
    let hash = format!("{:x}", Sha256::digest(secret.as_bytes()));
    let body = json!({"requestId":request,"projectId":project.id,"prompt":secret,
        "noSession":true,"tools":[],"timeoutSeconds":300});
    let response = fixture
        .post("/api/query/stream")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut stream = response.bytes_stream();
    let mut text = String::new();
    receive_until(&mut stream, &mut text, "first fragment").await;
    let started = sse_payload(&text, "query_started");
    let temporary = started["sessionId"].as_str().unwrap();
    assert_eq!(
        fixture.db.session_retention(temporary).await.unwrap(),
        zk_db::content::ContentRetention::Ephemeral
    );
    assert!(
        fixture
            .db
            .get_session(temporary)
            .await
            .unwrap()
            .unwrap()
            .messages
            .iter()
            .any(|m| m
                .content
                .iter()
                .any(|b| matches!(b,zk_db::StoredBlock::Text{text} if text==&secret)))
    );
    assert!(
        fixture
            .db
            .list_sessions(None, 100)
            .await
            .unwrap()
            .sessions
            .iter()
            .all(|session| session.id != temporary)
    );
    fixture.provider.release.notify_one();
    receive_until(&mut stream, &mut text, "event: complete").await;
    assert_eq!(sse_payload(&text, "complete")["success"], true, "{text}");
    let outcome = sse_payload(&text, "result");
    assert_eq!(outcome["usage"]["inputTokens"], 23);
    assert_eq!(outcome["result"], "first fragment");
    let run = fixture
        .db
        .find_run_by_id(outcome["runId"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.input_tokens, 23);
    assert_eq!(run.status, "completed");
    assert!(
        fixture.db.get_session(temporary).await.is_err(),
        "the root lease expires after result materialization"
    );
    assert_eq!(fixture.db.memory_content_store().retained_bytes(), 0);
    let mut resumed = fixture.body(&uuid::Uuid::new_v4().to_string());
    resumed["sessionId"] = json!(temporary);
    let refused = fixture
        .post("/api/query")
        .json(&resumed)
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        refused.json::<Value>().await.unwrap()["code"],
        "EPHEMERAL_OPERATION_UNSUPPORTED"
    );
    scan(&fixture.path, &[&secret, &hash]);
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
}

fn scan(path: &std::path::Path, needles: &[&str]) {
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            scan(&path, needles);
        } else if path.is_file() {
            let bytes = std::fs::read(&path).unwrap();
            for needle in needles {
                assert!(
                    !bytes
                        .windows(needle.len())
                        .any(|piece| piece == needle.as_bytes()),
                    "temporary content leaked into {}",
                    path.display()
                );
            }
        }
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "One real HTTP session proves cross-query interpreter state, cross-session stop denial, merge/delete gating and confirmed cleanup"
)]
async fn ordinary_query_repl_state_survives_root_completion_and_blocks_delete_merge_until_stopped()
{
    let mut script = std::collections::VecDeque::new();
    for (id, code) in [
        ("first-repl", "answer=41\nanswer+1"),
        ("second-repl", "answer+=1\nanswer+1"),
    ] {
        script.push_back(vec![
            ProviderEvent::ToolUseStart {
                id: id.into(),
                name: "REPL".into(),
            },
            ProviderEvent::ToolInputDelta {
                id: id.into(),
                delta: json!({"language":"python","code":code,"sessionId":"console"}).to_string(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: Some(Usage {
                    input_tokens: 10,
                    output_tokens: 5,
                    ..Default::default()
                }),
            },
        ]);
        script.push_back(vec![
            ProviderEvent::TextDelta {
                text: "calculation complete".into(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(Usage {
                    input_tokens: 10,
                    output_tokens: 5,
                    ..Default::default()
                }),
            },
        ]);
    }
    let provider = Arc::new(Provider {
        script: Mutex::new(Some(script)),
        ..Default::default()
    });
    // Explicit fixture approval isolates native REPL ownership from interactive permission UI.
    let fixture =
        Fixture::with_provider(provider, zk_authz::model::PermissionMode::AutoApprove).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let status_url = format!(
        "{}/api/sessions/{}/repl-service",
        fixture.base, fixture.session
    );
    let mut service_run = None;
    for (prompt, expected) in [("set answer", "42"), ("continue answer", "43")] {
        let response = fixture.post("/api/query").json(&json!({
            "prompt":prompt,"sessionId":fixture.session,"requestId":uuid::Uuid::new_v4().to_string(),
            "tools":["REPL"],"maxTurns":4,"timeoutSeconds":30
        })).timeout(Duration::from_secs(40)).send().await.unwrap();
        let http_status = response.status();
        let outcome: Value = response.json().await.unwrap();
        assert!(http_status.is_success(), "{http_status}: {outcome}");
        assert!(outcome["error"].is_null(), "{outcome}");
        let calls = outcome["toolCalls"].as_array().unwrap();
        assert_eq!(calls.len(), 1, "{outcome}");
        assert_eq!(calls[0]["isError"], false, "{outcome}");
        assert_eq!(calls[0]["output"].as_str().unwrap().trim(), expected);
        let root = fixture
            .db
            .find_run_by_id(outcome["runId"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(root.status, "completed");
        let state: Value = client
            .get(&status_url)
            .header("x-session-id", &fixture.session)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(state["state"], "running", "{state}");
        let run = state["runId"].as_str().unwrap().to_owned();
        if let Some(original) = &service_run {
            assert_eq!(&run, original);
        } else {
            service_run = Some(run);
        }
    }
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 4);
    let delete = client
        .delete(format!("{}/api/sessions/{}", fixture.base, fixture.session))
        .header("origin", "http://127.0.0.1:5273")
        .send()
        .await
        .unwrap();
    assert_eq!(delete.status(), reqwest::StatusCode::CONFLICT);
    let other = fixture
        .db
        .create_session(MODEL, fixture.path.to_str().unwrap())
        .await
        .unwrap();
    let merge = fixture
        .post("/api/sessions/merge")
        .header("Idempotency-Key", uuid::Uuid::new_v4().to_string())
        .json(&json!({
            "sourceSessionIds":[fixture.session,other.id],"primarySessionId":fixture.session
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(merge.status(), reqwest::StatusCode::CONFLICT);
    let cross_session = client
        .delete(&status_url)
        .header("origin", "http://127.0.0.1:5273")
        .header("x-session-id", &other.id)
        .send()
        .await
        .unwrap();
    assert_eq!(cross_session.status(), reqwest::StatusCode::FORBIDDEN);
    let stopped = client
        .delete(&status_url)
        .header("origin", "http://127.0.0.1:5273")
        .header("x-session-id", &fixture.session)
        .send()
        .await
        .unwrap();
    assert_eq!(stopped.status(), reqwest::StatusCode::ACCEPTED);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state: Value = client
                .get(&status_url)
                .header("x-session-id", &fixture.session)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if state["state"] == "stopped" {
                assert_eq!(state["cleanupStatus"], "confirmed");
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    fixture
        .db
        .ensure_session_idle(&fixture.session)
        .await
        .unwrap();
    let run_id = service_run.unwrap();
    let resources = fixture
        .db
        .with_reader(move |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM execution_resources WHERE run_id=?1 AND status<>'released'",
                [run_id],
                |r| r.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(resources, 0);
    let delete = client
        .delete(format!("{}/api/sessions/{}", fixture.base, fixture.session))
        .header("origin", "http://127.0.0.1:5273")
        .send()
        .await
        .unwrap();
    assert_eq!(delete.status(), reqwest::StatusCode::OK);
}

/// Permission waiting must not allocate a persistent interpreter service or start its process.
#[tokio::test]
async fn default_permission_repl_waits_for_approval_and_cancel_never_starts_service() {
    let provider = Arc::new(Provider {
        script: Mutex::new(Some(std::collections::VecDeque::from([vec![
            ProviderEvent::ToolUseStart {
                id: "denied-repl".into(),
                name: "REPL".into(),
            },
            ProviderEvent::ToolInputDelta {
                id: "denied-repl".into(),
                delta: json!({
                    "language":"python", "code":"print(42)", "sessionId":"console"
                })
                .to_string(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: Some(Usage {
                    input_tokens: 10,
                    output_tokens: 5,
                    ..Usage::default()
                }),
            },
        ]]))),
        ..Provider::default()
    });
    let fixture = Fixture::with_provider(provider, zk_authz::model::PermissionMode::Default).await;
    let request = uuid::Uuid::new_v4().to_string();
    let mut body = fixture.body(&request);
    body["tools"] = json!(["REPL"]);
    let response = fixture
        .post("/api/query/stream")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut stream = response.bytes_stream();
    let mut text = String::new();
    // Without a bound interactive client, delivery deliberately remains pending;
    // the durable permission request still fences all physical service creation.
    let interaction = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let pending = fixture
                .interactions
                .pending_views(&fixture.session)
                .await
                .unwrap();
            if let Some(view) = pending.into_iter().find(|view| {
                view.interaction_type
                    .as_deref()
                    .is_some_and(|kind| kind.eq_ignore_ascii_case("permission"))
            }) {
                break serde_json::to_value(view).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a durable approval request must exist before any REPL process");
    assert_eq!(interaction["interactionType"], "permission");
    assert_eq!(interaction["prompt"]["toolName"], "REPL");
    assert_no_repl_resources(&fixture.db).await;
    let receipt: Value = fixture
        .post(&format!("/api/query/{request}/cancel"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(receipt["stopRequested"], true);
    receive_until(&mut stream, &mut text, "event: complete").await;
    assert_eq!(sse_payload(&text, "complete")["success"], false);
    assert_no_repl_resources(&fixture.db).await;
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 1);
    let result = sse_payload(&text, "result");
    let run = fixture
        .db
        .find_run_by_id(result["runId"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, "cancelled");
    assert_eq!(
        run.input_tokens, 10,
        "already consumed request stays in the ledger"
    );
}

async fn assert_no_repl_resources(db: &Db) {
    let (services, resources) = db
        .with_reader(|conn| {
            Ok((
                conn.query_row(
                    "SELECT COUNT(*) FROM tasks WHERE task_type='repl'",
                    [],
                    |row| row.get::<_, i64>(0),
                )?,
                conn.query_row("SELECT COUNT(*) FROM execution_resources", [], |row| {
                    row.get::<_, i64>(0)
                })?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(
        services, 0,
        "unapproved tool cannot create a session service task"
    );
    assert_eq!(
        resources, 0,
        "unapproved tool cannot start an interpreter process"
    );
}
