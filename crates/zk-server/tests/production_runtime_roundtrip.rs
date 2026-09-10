//! Production-composition regression: file `SQLite` + `wire_engine` + real Router/WS.
//!
//! This is deliberately not an `Engine` unit test. It proves that the same
//! composition root used by `zk-server` can accept a browser-shaped v4 frame,
//! execute a deterministic provider call, persist the root Task/Run/result and
//! Assistant message, and only then publish `message_complete`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::BoxStream;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{Barrier, Notify};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tokio_util::sync::CancellationToken;
use zk_db::model::{MessageRole, StoredBlock};
use zk_db::{BudgetReservationStatus, Db, InboxStatus, ResultStatus, RunStatus, TaskStatus};
use zk_engine::TaskOutputRequest;
use zk_llm::{
    ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent, ProviderRegistry, Role,
};
use zk_protocol::Usage;
use zk_server::config::Config;
use zk_server::engine_bridge::wire_engine;
use zk_server::routes::build_router;
use zk_server::state::AppState;
use zk_server::ws::WsConfig;
use zk_tools::ToolContext;

// Use a catalogued model identity so the production budget admission path can
// price the call. The provider implementation itself remains deterministic.
const MODEL: &str = "qwen3.8-max-0902";
const PROMPT: &str = "production wiring round trip";
const ANSWER: &str = "durable scripted answer";
const CHILD_ANSWER: &str = "attached child result";
const PARENT_ANSWER: &str = "parent consumed attached child";
const PARALLEL_ROOT_PROMPT: &str = "delegate four attached tasks in one model response";
const PARALLEL_CHILD_PREFIX: &str = "parallel-attached-child:";
const INCIDENT_CHILD_PREFIX: &str = "incident-child:";
const INCIDENT_ROOT_PREFIX: &str = "incident-root:";
const OVERRUN_CHILD_PROMPT: &str = "authoritative-cost-overrun-child";
const OVERRUN_ROOT_PROMPT: &str = "delegate one authoritative cost overrun child";
const OVERRUN_PARENT_ANSWER: &str = "parent synthesized the partial child receipt";

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Default)]
struct ScriptedProvider {
    requests: Mutex<Vec<ChatRequest>>,
}

impl ScriptedProvider {
    fn requests(&self) -> Vec<ChatRequest> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl ChatProvider for ScriptedProvider {
    fn provider_name(&self) -> &'static str {
        "scripted"
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        _cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request);
        Ok(Box::pin(futures::stream::iter(text_completion(
            ANSWER, 11, 3,
        ))))
    }
}

#[derive(Default)]
struct AttachedAgentProvider {
    calls: AtomicUsize,
    requests: Mutex<Vec<ChatRequest>>,
}

impl AttachedAgentProvider {
    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl ChatProvider for AttachedAgentProvider {
    fn provider_name(&self) -> &'static str {
        "scripted-attached-agent"
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        _cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request);
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let events = match call {
            0 => {
                let input = serde_json::json!({
                    "prompt": "produce the deterministic child result",
                    "description": "attached production fixture",
                    "waitMode": "terminal",
                    "isolation": "readOnly"
                })
                .to_string();
                vec![
                    ProviderEvent::ToolUseStart {
                        id: "agent-call-1".to_owned(),
                        name: "Agent".to_owned(),
                    },
                    ProviderEvent::ToolInputDelta {
                        id: "agent-call-1".to_owned(),
                        delta: input,
                    },
                    ProviderEvent::Finish {
                        finish_reason: FinishReason::ToolUse,
                        usage: None,
                    },
                    ProviderEvent::UsageUpdate {
                        usage: Usage {
                            input_tokens: 10,
                            output_tokens: 2,
                            cache_read_input_tokens: 0,
                            cache_creation_input_tokens: 0,
                        },
                    },
                ]
            }
            1 => text_completion(CHILD_ANSWER, 8, 3),
            2 => text_completion(PARENT_ANSWER, 14, 4),
            other => panic!("unexpected scripted provider call {other}"),
        };
        Ok(Box::pin(futures::stream::iter(events)))
    }
}

/// A single root response contains four Agent calls. The first provider-capacity
/// wave stays blocked until the test observes all four durable child Tasks,
/// reproducing the parent `waitingDependencies` transition between sibling
/// tool-start CAS operations.
struct ParallelAttachedAgentProvider {
    rendezvous: Arc<Barrier>,
    released: Arc<AtomicBool>,
    release_signal: Arc<Notify>,
    requests: Mutex<Vec<ChatRequest>>,
    root_calls: AtomicUsize,
    child_calls: AtomicUsize,
    resumed_calls: AtomicUsize,
}

impl ParallelAttachedAgentProvider {
    fn new(expected_running_children: usize) -> Self {
        Self {
            rendezvous: Arc::new(Barrier::new(expected_running_children + 1)),
            released: Arc::new(AtomicBool::new(false)),
            release_signal: Arc::new(Notify::new()),
            requests: Mutex::new(Vec::new()),
            root_calls: AtomicUsize::new(0),
            child_calls: AtomicUsize::new(0),
            resumed_calls: AtomicUsize::new(0),
        }
    }

    fn requests(&self) -> Vec<ChatRequest> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    async fn wait_for_provider_capacity_wave(&self) {
        self.rendezvous.wait().await;
    }

    fn release_children(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release_signal.notify_waiters();
    }
}

impl ChatProvider for ParallelAttachedAgentProvider {
    fn provider_name(&self) -> &'static str {
        "scripted-parallel-attached-agent"
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request.clone());
        let latest_user = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == Role::User)
            .map(|message| message.content.clone())
            .unwrap_or_default();

        if latest_user.starts_with(PARALLEL_CHILD_PREFIX) {
            self.child_calls.fetch_add(1, Ordering::SeqCst);
            let rendezvous = Arc::clone(&self.rendezvous);
            let released = Arc::clone(&self.released);
            let release_signal = Arc::clone(&self.release_signal);
            let answer = format!("completed {latest_user}");
            let gated = futures::stream::once(async move {
                if !released.load(Ordering::SeqCst) {
                    rendezvous.wait().await;
                }
                while !released.load(Ordering::SeqCst) {
                    tokio::select! {
                        () = release_signal.notified() => {}
                        () = cancel.cancelled() => return Vec::new(),
                    }
                }
                if cancel.is_cancelled() {
                    Vec::new()
                } else {
                    text_completion(&answer, 7, 2)
                }
            })
            .flat_map(futures::stream::iter);
            return Ok(Box::pin(gated));
        }

        if request
            .messages
            .iter()
            .any(|message| message.role == Role::Tool)
        {
            self.resumed_calls.fetch_add(1, Ordering::SeqCst);
            return Ok(Box::pin(futures::stream::iter(text_completion(
                "parent consumed four attached results",
                12,
                4,
            ))));
        }

        assert_eq!(latest_user, PARALLEL_ROOT_PROMPT);
        self.root_calls.fetch_add(1, Ordering::SeqCst);
        let mut events = Vec::with_capacity(10);
        for ordinal in 0..4 {
            let tool_use_id = format!("parallel-agent-{ordinal}");
            events.push(ProviderEvent::ToolUseStart {
                id: tool_use_id.clone(),
                name: "Agent".to_owned(),
            });
            events.push(ProviderEvent::ToolInputDelta {
                id: tool_use_id,
                delta: serde_json::json!({
                    "prompt": format!("{PARALLEL_CHILD_PREFIX}{ordinal}"),
                    "description": format!("parallel attached child {ordinal}"),
                    "waitMode": "terminal",
                    "isolation": "readOnly"
                })
                .to_string(),
            });
        }
        events.push(ProviderEvent::Finish {
            finish_reason: FinishReason::ToolUse,
            usage: None,
        });
        events.push(ProviderEvent::UsageUpdate {
            usage: Usage {
                input_tokens: 20,
                output_tokens: 8,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
        });
        Ok(Box::pin(futures::stream::iter(events)))
    }
}

#[derive(Default)]
struct MissingUsageAgentProvider {
    calls: AtomicUsize,
}

impl ChatProvider for MissingUsageAgentProvider {
    fn provider_name(&self) -> &'static str {
        "scripted-missing-usage-agent"
    }

    fn chat_stream(
        &self,
        _request: ChatRequest,
        _cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let input = serde_json::json!({
            "prompt": "this child must never be created",
            "description": "missing usage fail-closed fixture",
            "waitMode": "terminal",
            "isolation": "readOnly"
        })
        .to_string();
        Ok(Box::pin(futures::stream::iter([
            ProviderEvent::ToolUseStart {
                id: "forbidden-agent-call".to_owned(),
                name: "Agent".to_owned(),
            },
            ProviderEvent::ToolInputDelta {
                id: "forbidden-agent-call".to_owned(),
                delta: input,
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: None,
            },
        ])))
    }
}

#[derive(Default)]
struct ChildBudgetFailureProvider {
    calls: AtomicUsize,
}

/// Produces complete, authoritative usage that is slightly above the child's
/// 20% cost allocation. Unlike [`ChildBudgetFailureProvider`], this exercises
/// the post-response settlement path rather than a synthetic provider error.
#[derive(Default)]
struct AuthoritativeChildCostOverrunProvider {
    initial_root: AtomicUsize,
    child_executions: AtomicUsize,
    parent_resumptions: AtomicUsize,
}

impl ChatProvider for AuthoritativeChildCostOverrunProvider {
    fn provider_name(&self) -> &'static str {
        "scripted-authoritative-child-cost-overrun"
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        _cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        let latest_user = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == Role::User)
            .map(|message| message.content.as_str())
            .unwrap_or_default();

        if latest_user == OVERRUN_CHILD_PROMPT {
            self.child_executions.fetch_add(1, Ordering::SeqCst);
            // Qwen's configured input price is $0.009/1k tokens, so this
            // authoritative usage costs $0.810054: above the child's $0.80
            // allocation while remaining well inside the root's $4 budget.
            return Ok(Box::pin(futures::stream::iter(text_completion(
                "partial evidence gathered before the budget boundary",
                90_000,
                1,
            ))));
        }

        if request.messages.iter().any(|message| {
            message.role == Role::Tool || message.content.contains("<task-result taskId=\"")
        }) {
            self.parent_resumptions.fetch_add(1, Ordering::SeqCst);
            return Ok(Box::pin(futures::stream::iter(text_completion(
                OVERRUN_PARENT_ANSWER,
                12,
                3,
            ))));
        }

        assert_eq!(latest_user, OVERRUN_ROOT_PROMPT);
        self.initial_root.fetch_add(1, Ordering::SeqCst);
        let input = serde_json::json!({
            "prompt": OVERRUN_CHILD_PROMPT,
            "description": "authoritative child cost overrun fixture",
            "waitMode": "terminal",
            "isolation": "readOnly"
        })
        .to_string();
        Ok(Box::pin(futures::stream::iter([
            ProviderEvent::ToolUseStart {
                id: "authoritative-overrun-agent".to_owned(),
                name: "Agent".to_owned(),
            },
            ProviderEvent::ToolInputDelta {
                id: "authoritative-overrun-agent".to_owned(),
                delta: input,
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: None,
            },
            ProviderEvent::UsageUpdate {
                usage: Usage {
                    input_tokens: 10,
                    output_tokens: 2,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                },
            },
        ])))
    }
}

impl ChatProvider for ChildBudgetFailureProvider {
    fn provider_name(&self) -> &'static str {
        "scripted-child-budget-failure"
    }

    fn chat_stream(
        &self,
        _request: ChatRequest,
        _cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let events = match call {
            0 => {
                let input = serde_json::json!({
                    "prompt": "fail with the scripted child budget code",
                    "description": "child budget terminal consistency fixture",
                    "waitMode": "terminal",
                    "isolation": "readOnly"
                })
                .to_string();
                vec![
                    ProviderEvent::ToolUseStart {
                        id: "budget-agent-call".to_owned(),
                        name: "Agent".to_owned(),
                    },
                    ProviderEvent::ToolInputDelta {
                        id: "budget-agent-call".to_owned(),
                        delta: input,
                    },
                    ProviderEvent::Finish {
                        finish_reason: FinishReason::ToolUse,
                        usage: None,
                    },
                    ProviderEvent::UsageUpdate {
                        usage: Usage {
                            input_tokens: 10,
                            output_tokens: 2,
                            cache_read_input_tokens: 0,
                            cache_creation_input_tokens: 0,
                        },
                    },
                ]
            }
            1 => vec![
                ProviderEvent::Error {
                    error: ProviderError::Config {
                        message: "COST_BUDGET_EXHAUSTED".to_owned(),
                    },
                },
                // The registry treats a non-parse error as fatal. Keeping a
                // syntactic Finish behind it protects the production chain if
                // a provider ever leaks that invalid tail downstream.
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(Usage {
                        input_tokens: 3,
                        output_tokens: 1,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                    }),
                },
            ],
            2 => text_completion("parent observed child failure", 8, 2),
            other => panic!("unexpected child budget provider call {other}"),
        };
        Ok(Box::pin(futures::stream::iter(events)))
    }
}

/// Eight-way deterministic provider used to reproduce the original four-foreground /
/// four-background incident without a paid model.  Every child must reach the provider
/// before any of them may finish, so a serial implementation cannot accidentally pass.
struct IncidentProvider {
    rendezvous: Arc<Barrier>,
    released: Arc<AtomicBool>,
    release_signal: Arc<Notify>,
    root_calls: AtomicUsize,
    child_calls: AtomicUsize,
    resumed_calls: AtomicUsize,
}

impl IncidentProvider {
    fn new(expected_children: usize) -> Self {
        Self {
            rendezvous: Arc::new(Barrier::new(expected_children + 1)),
            released: Arc::new(AtomicBool::new(false)),
            release_signal: Arc::new(Notify::new()),
            root_calls: AtomicUsize::new(0),
            child_calls: AtomicUsize::new(0),
            resumed_calls: AtomicUsize::new(0),
        }
    }

    async fn wait_until_all_children_are_executing(&self) {
        self.rendezvous.wait().await;
    }

    fn release_children(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release_signal.notify_waiters();
    }
}

impl ChatProvider for IncidentProvider {
    fn provider_name(&self) -> &'static str {
        "scripted-production-incident"
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        let latest_user = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == Role::User)
            .map(|message| message.content.clone())
            .unwrap_or_default();

        if latest_user.starts_with(INCIDENT_CHILD_PREFIX) {
            self.child_calls.fetch_add(1, Ordering::SeqCst);
            let rendezvous = Arc::clone(&self.rendezvous);
            let released = Arc::clone(&self.released);
            let release_signal = Arc::clone(&self.release_signal);
            let child_answer = format!("completed {latest_user}");
            let gated = futures::stream::once(async move {
                // Hold the first provider-capacity wave so the test can inspect
                // all eight durable Tasks. Later waves skip the rendezvous after
                // the release flag is visible and therefore cannot deadlock on
                // a second barrier generation without the test-side participant.
                if !released.load(Ordering::SeqCst) {
                    rendezvous.wait().await;
                }
                while !released.load(Ordering::SeqCst) {
                    tokio::select! {
                        () = release_signal.notified() => {}
                        () = cancel.cancelled() => return Vec::new(),
                    }
                }
                if cancel.is_cancelled() {
                    Vec::new()
                } else {
                    text_completion(&child_answer, 7, 2)
                }
            })
            .flat_map(futures::stream::iter);
            return Ok(Box::pin(gated));
        }

        if request
            .messages
            .iter()
            .any(|message| message.role == Role::Tool)
        {
            self.resumed_calls.fetch_add(1, Ordering::SeqCst);
            return Ok(Box::pin(futures::stream::iter(text_completion(
                "parent consumed durable child result",
                9,
                3,
            ))));
        }

        let Some(suffix) = latest_user.strip_prefix(INCIDENT_ROOT_PREFIX) else {
            panic!("unexpected incident provider request: {latest_user}");
        };
        self.root_calls.fetch_add(1, Ordering::SeqCst);
        let (mode, ordinal) = suffix
            .split_once(':')
            .expect("incident root prompt contains mode and ordinal");
        let tool_use_id = format!("incident-{mode}-{ordinal}");
        let child_prompt = format!("{INCIDENT_CHILD_PREFIX}{mode}:{ordinal}");
        let (tool_name, input) = match mode {
            "terminal" => (
                "Agent",
                serde_json::json!({
                    "prompt": child_prompt,
                    "description": format!("terminal child {ordinal}"),
                    "waitMode": "terminal",
                    "isolation": "readOnly"
                }),
            ),
            "background" => (
                "TaskCreate",
                serde_json::json!({
                    "description": format!("background child {ordinal}"),
                    "prompt": child_prompt,
                    "taskType": "agent"
                }),
            ),
            other => panic!("unexpected incident mode {other}"),
        };
        Ok(Box::pin(futures::stream::iter([
            ProviderEvent::ToolUseStart {
                id: tool_use_id.clone(),
                name: tool_name.to_owned(),
            },
            ProviderEvent::ToolInputDelta {
                id: tool_use_id,
                delta: input.to_string(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: None,
            },
            ProviderEvent::UsageUpdate {
                usage: Usage {
                    input_tokens: 8,
                    output_tokens: 2,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                },
            },
        ])))
    }
}

fn text_completion(text: &str, input_tokens: i64, output_tokens: i64) -> Vec<ProviderEvent> {
    vec![
        ProviderEvent::TextDelta {
            text: text.to_owned(),
        },
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            usage: None,
        },
        ProviderEvent::UsageUpdate {
            usage: Usage {
                input_tokens,
                output_tokens,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
        },
    ]
}

#[derive(Default)]
struct TimeoutPartialProvider {
    root_calls: AtomicUsize,
    started: Notify,
}

impl ChatProvider for TimeoutPartialProvider {
    fn provider_name(&self) -> &'static str {
        "timeout-partial"
    }
    fn chat_stream(
        &self,
        request: ChatRequest,
        _: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        let prompt = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == Role::User)
            .map(|message| message.content.as_str())
            .unwrap_or_default();
        if prompt == "timeout-child" {
            self.started.notify_one();
            return Ok(Box::pin(
                futures::stream::iter(vec![ProviderEvent::TextDelta {
                    text: "recoverable timed-out finding".to_owned(),
                }])
                .chain(futures::stream::pending()),
            ));
        }
        if prompt == "successful-child" {
            return Ok(Box::pin(futures::stream::iter(text_completion(
                "successful finding",
                8,
                3,
            ))));
        }
        if self.root_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let mut events = Vec::new();
            for prompt in ["timeout-child", "successful-child"] {
                events.push(ProviderEvent::ToolUseStart {
                    id: prompt.to_owned(),
                    name: "Agent".to_owned(),
                });
                events.push(ProviderEvent::ToolInputDelta { id: prompt.to_owned(), delta: serde_json::json!({
                    "prompt": prompt, "description": prompt, "waitMode":"terminal", "isolation":"readOnly"
                }).to_string() });
            }
            events.push(ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: Some(Usage {
                    input_tokens: 10,
                    output_tokens: 3,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                }),
            });
            return Ok(Box::pin(futures::stream::iter(events)));
        }
        let receipts = request
            .messages
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            receipts.contains("recoverable timed-out finding"),
            "{receipts}"
        );
        assert!(receipts.contains("successful finding"));
        Ok(Box::pin(futures::stream::iter(text_completion(
            "combined partial and successful findings",
            20,
            5,
        ))))
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn production_timeout_preserves_partial_and_parent_finishes() {
    let isolated = IsolatedDir::create();
    let workspace = isolated.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let db = Db::open(isolated.path().join("timeout.sqlite3")).unwrap();
    let provider = Arc::new(TimeoutPartialProvider::default());
    let mut providers = ProviderRegistry::new();
    providers.register("scripted", provider.clone(), vec![MODEL.to_owned()]);
    let mut config = Config::test_config();
    config.default_model = MODEL.to_owned();
    config.db_path = isolated.path().join("timeout.sqlite3");
    config.workspace_default_root = workspace.to_string_lossy().into_owned();
    config.snapshot_dir = Some(isolated.path().join("snapshots"));
    config.scratchpad_system_root = isolated.path().join("scratchpad");
    config.mcp_registry_path = isolated.path().join("mcp-capabilities.json");
    config.agent_enabled = true;
    let state = AppState::new_with_ws(db.clone(), config, WsConfig::fast_for_tests())
        .with_providers(providers.with_default_model(MODEL));
    install_runtime_startup_epoch(&state, &db).await;
    let _engine = wire_engine(&state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = start_production_server(listener, build_router(state.clone()));
    let session = db
        .create_session(MODEL, workspace.to_string_lossy().as_ref())
        .await
        .unwrap();
    state
        .authz
        .modes
        .set_mode(&session.id, zk_authz::model::PermissionMode::AutoApprove)
        .await;
    let mut ws = connect(addr).await;
    send_json(
        &mut ws,
        serde_json::json!({"type":"bind_session", "sessionId":session.id,
        "bindRequestId":"timeout-bind", "bindingEpoch":1, "protocolVersion":4}),
    )
    .await;
    assert_eq!(next_json(&mut ws).await["type"], "session_restored");
    send_json(
        &mut ws,
        serde_json::json!({"type":"user_message", "text":"delegate two children"}),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(10), provider.started.notified())
        .await
        .unwrap();
    // Exercise the same durable timeout cause used by TaskRuntime's deadline,
    // without waiting thirty real minutes. Timer ownership has separate tests.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let tasks = db.find_task_tree_owned(&session.id).await.unwrap();
    let child = tasks
        .iter()
        .find(|t| t.description == "timeout-child")
        .unwrap();
    let root = tasks.iter().find(|t| t.parent_task_id.is_none()).unwrap();
    state
        .task_runtime()
        .cancel_run_with_cause(
            child.current_run_id.as_ref().unwrap(),
            "timeout",
            "test deadline",
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let task = db.find_runtime_task_by_id(&root.id).await.unwrap().unwrap();
            if task.status.is_terminal() || task.status == TaskStatus::NeedsAttention {
                assert_eq!(task.status, TaskStatus::Succeeded, "{task:?}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("parent completes after timeout cleanup");
    let result = db
        .read_task_result(&child.id, None, 0, 65536)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.result.status, ResultStatus::Partial);
    assert_eq!(
        result.result.error_code.as_deref(),
        Some("SUBAGENT_DEADLINE_EXCEEDED")
    );
    assert!(result.content.contains("recoverable timed-out finding"));
    let final_result = db
        .read_task_result(&root.id, None, 0, 65536)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        final_result.content,
        "combined partial and successful findings"
    );
    assert_eq!(provider.root_calls.load(Ordering::SeqCst), 2);
    let root_id = root.id.clone();
    let counts = db.with_reader(move |conn| {
        let started = conn.query_row("SELECT COUNT(*) FROM llm_calls c JOIN tasks t ON t.id=c.task_id WHERE t.root_task_id=?1 AND c.status='started'", [&root_id], |r| r.get::<_, i64>(0))?;
        let receipts = conn.query_row("SELECT COUNT(*),COUNT(DISTINCT producer_task_id) FROM task_result_receipts WHERE consumer_task_id=?1", [&root_id], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
        let active = conn.query_row("SELECT COUNT(*) FROM task_budget_reservations WHERE root_task_id=?1 AND status='active'", [&root_id], |r| r.get::<_, i64>(0))?;
        let unknown = conn.query_row("SELECT COUNT(*) FROM llm_calls c JOIN tasks t ON t.id=c.task_id WHERE t.root_task_id=?1 AND c.usage_complete=0 AND c.status='cancelled' AND c.error_code='STREAM_DROPPED'", [&root_id], |r| r.get::<_, i64>(0))?;
        Ok((started, receipts, active, unknown))
    }).await.unwrap();
    assert_eq!(counts, (0, (2, 2), 0, 1));
    drop(ws);
    graceful_shutdown_production_server(server, &state, &db).await;
}

struct IsolatedDir(PathBuf);

impl IsolatedDir {
    fn create() -> Self {
        let path = std::env::temp_dir().join(format!(
            "zkcode-production-runtime-e2e-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&path).expect("create isolated production fixture directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for IsolatedDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn install_runtime_startup_epoch(state: &AppState, db: &Db) {
    let epoch = db
        .begin_runtime_startup_epoch()
        .await
        .expect("allocate durable startup epoch");
    state
        .set_startup_epoch(epoch)
        .expect("install durable startup epoch before wiring execution");
}

struct ProductionServer {
    shutdown: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

fn start_production_server(listener: TcpListener, router: axum::Router) -> ProductionServer {
    let shutdown = CancellationToken::new();
    let serve_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move { serve_shutdown.cancelled().await })
        .await
        .expect("serve production-composed router");
    });
    ProductionServer { shutdown, handle }
}

async fn graceful_shutdown_production_server(server: ProductionServer, state: &AppState, db: &Db) {
    server.shutdown.cancel();
    let runtime = state.task_runtime();
    let runtime_shutdown = runtime
        .shutdown_with_supervisor(state.execution_supervisor.as_ref(), Duration::from_secs(10));
    let server_shutdown = tokio::time::timeout(Duration::from_secs(10), server.handle);
    let (runtime_result, server_result) = tokio::join!(runtime_shutdown, server_shutdown);
    server_result
        .expect("production server drains within ten seconds")
        .expect("production server task joins cleanly");
    let report = runtime_result.expect("unified runtime shutdown persists and drains");
    assert!(report.intake_closed);
    assert!(report.drained, "runtime shutdown report: {report:?}");
    assert_eq!(report.local_owners_timed_out, 0);
    assert_eq!(runtime.local_active_count(), 0);

    let (active_runs, active_resources): (i64, i64) = db
        .with_reader(|connection| {
            let active_runs = connection.query_row(
                "SELECT COUNT(*) FROM run_envelopes WHERE status IN \
                 ('queued','running','waitingDependencies','waitingInteraction','cancelling')",
                [],
                |row| row.get(0),
            )?;
            let active_resources = connection.query_row(
                "SELECT COUNT(*) FROM execution_resources WHERE status IN ('allocated','stopping')",
                [],
                |row| row.get(0),
            )?;
            Ok((active_runs, active_resources))
        })
        .await
        .expect("read post-shutdown runtime ownership");
    assert_eq!(
        active_runs, 0,
        "no durable Run remains active after shutdown"
    );
    assert_eq!(
        active_resources, 0,
        "no execution resource remains allocated/stopping after shutdown"
    );
}

async fn connect(addr: SocketAddr) -> WsStream {
    let mut request = format!("ws://{addr}/ws")
        .into_client_request()
        .expect("valid websocket request");
    request.headers_mut().insert(
        "Origin",
        "http://localhost:5273".parse().expect("trusted origin"),
    );
    connect_async(request)
        .await
        .expect("connect production WS")
        .0
}

async fn send_json(ws: &mut WsStream, value: serde_json::Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .expect("send websocket JSON");
}

async fn next_json(ws: &mut WsStream) -> serde_json::Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("production response within ten seconds")
            .expect("websocket remains open")
            .expect("read websocket frame");
        match frame {
            Message::Text(text) => {
                return serde_json::from_str(text.as_str()).expect("server frame is JSON");
            }
            Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
            Message::Close(frame) => panic!("production websocket closed early: {frame:?}"),
        }
    }
}

async fn run_prompt_to_completion(mut ws: WsStream, prompt: String) -> Vec<serde_json::Value> {
    send_json(
        &mut ws,
        serde_json::json!({
            "type": "user_message",
            "text": prompt,
            "attachments": [],
            "references": []
        }),
    )
    .await;
    let mut frames = Vec::new();
    loop {
        let frame = next_json(&mut ws).await;
        assert_ne!(frame["type"], "error", "unexpected engine error: {frame}");
        let complete = frame["type"] == "message_complete";
        frames.push(frame);
        if complete {
            return frames;
        }
    }
}

fn text_of(blocks: &[StoredBlock]) -> String {
    blocks
        .iter()
        .filter_map(|block| match block {
            StoredBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>()
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One ordered end-to-end transaction/publication proof.
async fn production_wiring_persists_root_completion_before_ws_publication() {
    let isolated = IsolatedDir::create();
    let workspace = isolated.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("create isolated workspace");
    let db =
        Db::open(isolated.path().join("runtime.sqlite3")).expect("file SQLite boots final schema");

    let provider = Arc::new(ScriptedProvider::default());
    let mut providers = ProviderRegistry::new();
    providers.register("scripted", provider.clone(), vec![MODEL.to_owned()]);

    let mut config = Config::test_config();
    config.default_model = MODEL.to_owned();
    config.db_path = isolated.path().join("runtime.sqlite3");
    config.workspace_default_root = workspace.to_string_lossy().into_owned();
    config.snapshot_dir = Some(isolated.path().join("snapshots"));
    config.scratchpad_system_root = isolated.path().join("scratchpad");
    config.mcp_registry_path = isolated.path().join("mcp-capabilities.json");

    let state = AppState::new_with_ws(db.clone(), config, WsConfig::fast_for_tests())
        .with_providers(providers.with_default_model(MODEL));
    install_runtime_startup_epoch(&state, &db).await;
    let _engine = wire_engine(&state);
    let router = build_router(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind isolated server port");
    let addr = listener.local_addr().expect("fixture server address");
    let server = start_production_server(listener, router);

    let session = db
        .create_session(MODEL, workspace.to_string_lossy().as_ref())
        .await
        .expect("create root session");
    let mut ws = connect(addr).await;
    send_json(
        &mut ws,
        serde_json::json!({
            "type": "bind_session",
            "sessionId": session.id,
            "bindRequestId": "production-bind",
            "bindingEpoch": 1,
            "protocolVersion": 4
        }),
    )
    .await;
    let restored = next_json(&mut ws).await;
    assert_eq!(restored["type"], "session_restored");

    send_json(
        &mut ws,
        serde_json::json!({
            "type": "user_message",
            "text": PROMPT,
            "attachments": [],
            "references": []
        }),
    )
    .await;

    let complete = loop {
        let frame = next_json(&mut ws).await;
        assert_ne!(frame["type"], "error", "unexpected engine error: {frame}");
        if frame["type"] == "message_complete" {
            break frame;
        }
    };

    // Publication is the observation boundary: every authoritative row must
    // already be readable from a separate SQLite reader at this exact point.
    let run_id = complete["runId"]
        .as_str()
        .expect("completed production frame owns a Run");
    let run = db
        .find_run_by_id(run_id)
        .await
        .expect("read completed Run")
        .expect("completed Run exists");
    assert_eq!(run.status, RunStatus::Completed.as_db());
    assert_eq!(run.exit_reason.as_deref(), Some("modelFinished"));

    let task = db
        .find_runtime_task_by_id(&run.task_id)
        .await
        .expect("read root Task")
        .expect("root Task exists");
    assert_eq!(task.status, TaskStatus::Succeeded);
    let result = db
        .read_task_result(&task.id, None, 0, 65_536)
        .await
        .expect("read immutable TaskResult")
        .expect("successful root Task owns a result");
    assert_eq!(result.result.status, ResultStatus::Complete);
    assert_eq!(result.content, ANSWER);

    let messages = db
        .list_messages(&session.id, None, 20)
        .await
        .expect("read committed transcript")
        .expect("session remains readable")
        .messages;
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].role, MessageRole::User);
    assert_eq!(text_of(&messages[0].content), PROMPT);
    assert_eq!(messages[1].role, MessageRole::Assistant);
    assert_eq!(text_of(&messages[1].content), ANSWER);
    assert_eq!(messages[1].stop_reason.as_deref(), Some("end_turn"));
    assert_eq!(
        complete["committedMessages"].as_array().map(Vec::len),
        Some(2)
    );

    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].model, MODEL);
    assert!(
        requests[0]
            .messages
            .iter()
            .any(|message| message.content == PROMPT),
        "the production provider request must contain the persisted user turn"
    );

    drop(ws);
    graceful_shutdown_production_server(server, &state, &db).await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One ordered parent/child/receipt production proof.
async fn production_wiring_executes_one_attached_agent_through_task_runtime() {
    let isolated = IsolatedDir::create();
    let workspace = isolated.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("create isolated workspace");
    let db =
        Db::open(isolated.path().join("attached.sqlite3")).expect("file SQLite boots final schema");

    let provider = Arc::new(AttachedAgentProvider::default());
    let mut providers = ProviderRegistry::new();
    providers.register("scripted", provider.clone(), vec![MODEL.to_owned()]);

    let mut config = Config::test_config();
    config.default_model = MODEL.to_owned();
    config.db_path = isolated.path().join("attached.sqlite3");
    config.workspace_default_root = workspace.to_string_lossy().into_owned();
    config.snapshot_dir = Some(isolated.path().join("snapshots"));
    config.scratchpad_system_root = isolated.path().join("scratchpad");
    config.mcp_registry_path = isolated.path().join("mcp-capabilities.json");
    config.agent_enabled = true;

    let state = AppState::new_with_ws(db.clone(), config, WsConfig::fast_for_tests())
        .with_providers(providers.with_default_model(MODEL));
    install_runtime_startup_epoch(&state, &db).await;
    let _engine = wire_engine(&state);
    let router = build_router(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind isolated server port");
    let addr = listener.local_addr().expect("fixture server address");
    let server = start_production_server(listener, router);

    let session = db
        .create_session(MODEL, workspace.to_string_lossy().as_ref())
        .await
        .expect("create root session");
    state
        .authz
        .modes
        .set_mode(&session.id, zk_authz::model::PermissionMode::AutoApprove)
        .await;
    let mut ws = connect(addr).await;
    send_json(
        &mut ws,
        serde_json::json!({
            "type": "bind_session",
            "sessionId": session.id,
            "bindRequestId": "attached-bind",
            "bindingEpoch": 1,
            "protocolVersion": 4
        }),
    )
    .await;
    assert_eq!(next_json(&mut ws).await["type"], "session_restored");

    send_json(
        &mut ws,
        serde_json::json!({
            "type": "user_message",
            "text": "delegate exactly one attached task",
            "attachments": [],
            "references": []
        }),
    )
    .await;

    let complete = loop {
        let frame = next_json(&mut ws).await;
        assert_ne!(frame["type"], "error", "unexpected engine error: {frame}");
        if frame["type"] == "message_complete" {
            break frame;
        }
    };
    let root_run_id = complete["runId"]
        .as_str()
        .expect("parent completion has Run identity");

    let runs = db
        .find_run_tree(root_run_id)
        .await
        .expect("read recursive Run tree");
    assert_eq!(runs.len(), 2, "one root Run and one attached child Run");
    assert_eq!(runs[0].status, RunStatus::Completed.as_db());
    assert_eq!(runs[1].status, RunStatus::Completed.as_db());
    assert!(
        runs.iter().all(|run| run.usage_complete),
        "every parent/child Run must have authoritative persisted usage"
    );
    assert_eq!(runs[1].parent_run_id.as_deref(), Some(root_run_id));

    let tasks = db
        .find_task_tree_owned(&session.id)
        .await
        .expect("read recursive Task tree");
    assert_eq!(tasks.len(), 2);
    let root_task = tasks
        .iter()
        .find(|task| task.parent_task_id.is_none())
        .expect("root Task");
    let child_task = tasks
        .iter()
        .find(|task| task.parent_task_id.as_deref() == Some(root_task.id.as_str()))
        .expect("attached child Task");
    assert_eq!(root_task.status, TaskStatus::Succeeded);
    assert_eq!(child_task.status, TaskStatus::Succeeded);
    assert!(root_task.usage_complete);
    assert!(child_task.usage_complete);
    assert_eq!(child_task.lifecycle_policy, "attached");
    assert_eq!(child_task.task_type, "agent");

    let child_result = db
        .read_task_result(&child_task.id, None, 0, 65_536)
        .await
        .expect("read child result")
        .expect("child result exists");
    assert_eq!(child_result.result.status, ResultStatus::Complete);
    assert_eq!(child_result.content, CHILD_ANSWER);
    let root_result = db
        .read_task_result(&root_task.id, None, 0, 65_536)
        .await
        .expect("read parent result")
        .expect("parent result exists");
    assert_eq!(root_result.content, PARENT_ANSWER);
    assert!(!root_result.content.trim().is_empty());

    let child_budget = db
        .read_task_budget(&child_task.id)
        .await
        .expect("read child budget")
        .expect("child budget exists");
    assert_eq!(
        child_budget
            .reservation
            .expect("attached child reservation")
            .status,
        BudgetReservationStatus::Settled
    );
    let run_ids = [root_run_id.to_owned(), runs[1].id.clone()];
    let persisted_usage = db
        .with_conn_blocking(move |conn| {
            conn.query_row(
                "SELECT COUNT(*),
                        COALESCE(SUM(CASE WHEN status='completed'
                                          AND usage_complete=1
                                          AND input_tokens IS NOT NULL
                                          AND output_tokens IS NOT NULL
                                          AND cost_nanos_usd IS NOT NULL
                                         THEN 1 ELSE 0 END),0)
                 FROM llm_calls WHERE run_id IN (?1,?2)",
                rusqlite::params![run_ids[0], run_ids[1]],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(Into::into)
        })
        .expect("read persisted parent/child LLM usage");
    assert_eq!(persisted_usage, (3, 3));

    let diagnostic = db
        .find_task_diagnostic(&root_task.id)
        .await
        .expect("read root diagnostic")
        .expect("root diagnostic exists");
    assert_eq!(diagnostic.receipts.len(), 1);
    assert_eq!(diagnostic.receipts[0].producer_task_id, child_task.id);
    assert_eq!(diagnostic.receipts[0].consumer_task_id, root_task.id);

    let visible_sessions = db
        .list_sessions(None, 20)
        .await
        .expect("list root sessions");
    assert_eq!(visible_sessions.sessions.len(), 1);
    assert_eq!(visible_sessions.sessions[0].id, session.id);
    assert_ne!(
        runs[1].session_id, session.id,
        "child transcript is internal"
    );
    assert_eq!(provider.call_count(), 3);

    drop(ws);
    graceful_shutdown_production_server(server, &state, &db).await;
}

/// A single assistant turn may emit several Agent calls. Creating the first
/// attached child moves the parent to `waitingDependencies`; every sibling
/// invocation prepared by that same turn must still cross the guarded start
/// boundary and execute exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn production_wiring_starts_four_attached_agents_from_one_response() {
    const CHILDREN: usize = 4;
    let isolated = IsolatedDir::create();
    let workspace = isolated.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("create isolated workspace");
    let db = Db::open(isolated.path().join("parallel-attached.sqlite3"))
        .expect("file SQLite boots final schema");

    let provider = Arc::new(ParallelAttachedAgentProvider::new(CHILDREN));
    let mut providers = ProviderRegistry::new();
    providers.register("dashscope", provider.clone(), vec![MODEL.to_owned()]);

    let mut config = Config::test_config();
    config.default_model = MODEL.to_owned();
    config.db_path = isolated.path().join("parallel-attached.sqlite3");
    config.workspace_default_root = workspace.to_string_lossy().into_owned();
    config.snapshot_dir = Some(isolated.path().join("snapshots"));
    config.scratchpad_system_root = isolated.path().join("scratchpad");
    config.mcp_registry_path = isolated.path().join("mcp-capabilities.json");
    config.agent_enabled = true;
    config.coordinator_mode_enabled = true;
    let state = AppState::new_with_ws(db.clone(), config, WsConfig::fast_for_tests())
        .with_providers(providers.with_default_model(MODEL));
    install_runtime_startup_epoch(&state, &db).await;
    let _engine = wire_engine(&state);
    let router = build_router(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind isolated server port");
    let addr = listener.local_addr().expect("fixture server address");
    let server = start_production_server(listener, router);

    let session = db
        .create_session(MODEL, workspace.to_string_lossy().as_ref())
        .await
        .expect("create root session");
    state
        .authz
        .modes
        .set_mode(&session.id, zk_authz::model::PermissionMode::AutoApprove)
        .await;
    let mut ws = connect(addr).await;
    send_json(
        &mut ws,
        serde_json::json!({
            "type": "bind_session",
            "sessionId": session.id,
            "bindRequestId": "parallel-attached-bind",
            "bindingEpoch": 1,
            "protocolVersion": 4
        }),
    )
    .await;
    assert_eq!(next_json(&mut ws).await["type"], "session_restored");
    let client = tokio::spawn(run_prompt_to_completion(
        ws,
        PARALLEL_ROOT_PROMPT.to_owned(),
    ));

    tokio::time::timeout(
        Duration::from_secs(20),
        provider.wait_for_provider_capacity_wave(),
    )
    .await
    .expect("all four sibling Agent invocations cross the guarded start boundary");

    let tasks = db
        .find_task_tree_owned(&session.id)
        .await
        .expect("read parallel Task tree before child completion");
    assert_eq!(tasks.len(), CHILDREN + 1);
    let root = tasks
        .iter()
        .find(|task| task.parent_task_id.is_none())
        .expect("root Task");
    assert_eq!(root.status, TaskStatus::WaitingDependencies);
    let root_task_id = root.id.clone();
    let root_run_id = root.current_run_id.clone().expect("root Run identity");
    let children = tasks
        .iter()
        .filter(|task| task.parent_task_id.as_deref() == Some(root.id.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(children.len(), CHILDREN);
    assert!(
        children
            .iter()
            .all(|child| child.status == TaskStatus::Running)
    );

    let running_root_run_id = root_run_id.clone();
    let running_invocations: (i64, i64) = db
        .with_conn_blocking(move |conn| {
            conn.query_row(
                "SELECT COUNT(*),COALESCE(SUM(status='running'),0)
                 FROM tool_invocations WHERE run_id=?1 AND tool_name='Agent'",
                rusqlite::params![running_root_run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(Into::into)
        })
        .expect("read running parallel Agent invocations");
    assert_eq!(running_invocations, (4, 4));

    provider.release_children();
    let frames = tokio::time::timeout(Duration::from_secs(30), client)
        .await
        .expect("parallel parent completes after child release")
        .expect("websocket reader task succeeds");
    let visible_starts = frames
        .iter()
        .filter(|frame| frame["type"] == "tool_use_input" && frame["toolName"] == "Agent")
        .count();
    let visible_results = frames
        .iter()
        .filter(|frame| {
            frame["type"] == "tool_result"
                && frame["toolUseId"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("parallel-agent-"))
        })
        .count();
    assert_eq!(visible_starts, CHILDREN);
    assert_eq!(visible_results, CHILDREN);
    assert!(frames.iter().all(|frame| frame["type"] != "error"));

    let completed_tasks = db
        .find_task_tree_owned(&session.id)
        .await
        .expect("read completed parallel Task tree");
    assert_eq!(completed_tasks.len(), CHILDREN + 1);
    assert!(
        completed_tasks
            .iter()
            .all(|task| task.status == TaskStatus::Succeeded && task.usage_complete)
    );
    let runs = db
        .find_run_tree(&root_run_id)
        .await
        .expect("read completed parallel Run tree");
    assert_eq!(runs.len(), CHILDREN + 1);
    assert!(
        runs.iter()
            .all(|run| run.status == RunStatus::Completed.as_db() && run.usage_complete)
    );

    let completed_root_run_id = root_run_id.clone();
    let completed_root_task_id = root_task_id.clone();
    let durable_counts: [i64; 15] = db
        .with_conn_blocking(move |conn| {
            let invocations = conn.query_row(
                "SELECT COUNT(*),COALESCE(SUM(status='succeeded'),0)
                 FROM tool_invocations WHERE run_id=?1 AND tool_name='Agent'",
                rusqlite::params![completed_root_run_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )?;
            let reservations = conn.query_row(
                "SELECT COUNT(*),COALESCE(SUM(status='settled' AND usage_complete=1),0)
                 FROM task_budget_reservations WHERE root_task_id=?1",
                rusqlite::params![completed_root_task_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )?;
            let calls = conn.query_row(
                "SELECT COUNT(*),COALESCE(SUM(status='completed' AND usage_complete=1),0)
                 FROM llm_calls WHERE run_id IN (
                     SELECT id FROM run_envelopes WHERE id=?1 OR parent_run_id=?1
                 )",
                rusqlite::params![completed_root_run_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )?;
            let task_results = conn.query_row(
                "SELECT COUNT(*) FROM task_results
                 WHERE task_id=?1 OR task_id IN (
                     SELECT child_task_id FROM task_dependencies WHERE parent_task_id=?1
                 )",
                rusqlite::params![completed_root_task_id],
                |row| row.get::<_, i64>(0),
            )?;
            let receipts = conn.query_row(
                "SELECT COUNT(*),COUNT(DISTINCT producer_task_id),COUNT(DISTINCT message_id)
                 FROM task_result_receipts WHERE consumer_task_id=?1",
                rusqlite::params![completed_root_task_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )?;
            let receipt_messages = conn.query_row(
                "SELECT COUNT(*) FROM messages WHERE task_id=?1 AND origin='task_result'",
                rusqlite::params![completed_root_task_id],
                |row| row.get::<_, i64>(0),
            )?;
            let dependencies = conn.query_row(
                "SELECT COUNT(*),COALESCE(SUM(consumed_result_version IS NOT NULL),0)
                 FROM task_dependencies WHERE parent_task_id=?1",
                rusqlite::params![completed_root_task_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )?;
            let wake_events = conn.query_row(
                "SELECT COUNT(*) FROM run_event_log
                 WHERE run_id=?1 AND event_type='task_dependencies_resolved'",
                rusqlite::params![completed_root_run_id],
                |row| row.get::<_, i64>(0),
            )?;
            let active_reservations = conn.query_row(
                "SELECT COUNT(*) FROM task_budget_reservations
                 WHERE root_task_id=?1 AND status='active'",
                rusqlite::params![completed_root_task_id],
                |row| row.get::<_, i64>(0),
            )?;
            Ok([
                invocations.0,
                invocations.1,
                reservations.0,
                reservations.1,
                calls.0,
                calls.1,
                task_results,
                receipts.0,
                receipts.1,
                receipts.2,
                receipt_messages,
                dependencies.0,
                dependencies.1,
                wake_events,
                active_reservations,
            ])
        })
        .expect("read completed parallel durable facts");
    assert_eq!(
        durable_counts,
        [4, 4, 4, 4, 6, 6, 5, 4, 4, 4, 4, 4, 4, 1, 0]
    );
    assert_eq!(provider.root_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.child_calls.load(Ordering::SeqCst), CHILDREN);
    assert_eq!(provider.resumed_calls.load(Ordering::SeqCst), 1);

    let requests = provider.requests();
    assert_eq!(requests.len(), CHILDREN + 2);
    let initial_root = requests
        .iter()
        .find(|request| {
            request
                .messages
                .iter()
                .rev()
                .find(|message| message.role == Role::User)
                .is_some_and(|message| message.content == PARALLEL_ROOT_PROMPT)
        })
        .expect("initial root provider request");
    assert_eq!(
        initial_root
            .system_text()
            .expect("generated root system prompt")
            .matches("# Coordinator 模式")
            .count(),
        1
    );
    let child_requests = requests
        .iter()
        .filter(|request| {
            request
                .messages
                .iter()
                .rev()
                .find(|message| message.role == Role::User)
                .is_some_and(|message| message.content.starts_with(PARALLEL_CHILD_PREFIX))
        })
        .collect::<Vec<_>>();
    assert_eq!(child_requests.len(), CHILDREN);
    assert!(child_requests.iter().all(|request| {
        !request
            .system_text()
            .unwrap_or_default()
            .contains("# Coordinator 模式")
    }));
    let resumed_root = requests
        .iter()
        .find(|request| {
            request.messages.iter().any(|message| {
                message.role == Role::User && message.content.contains("<task-result taskId=\"")
            })
        })
        .expect("root provider request after attached barrier");
    let receipt_task_ids = resumed_root
        .messages
        .iter()
        .filter(|message| message.role == Role::User)
        .filter_map(|message| {
            let marker = "<task-result taskId=\"";
            let suffix = message.content.split_once(marker)?.1;
            Some(suffix.split_once('"')?.0.to_owned())
        })
        .collect::<std::collections::BTreeSet<_>>();
    let child_task_ids = children
        .iter()
        .map(|task| task.id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(receipt_task_ids, child_task_ids);

    let root_result = db
        .read_task_result(&root_task_id, None, 0, 65_536)
        .await
        .expect("read parallel root result")
        .expect("parallel root result exists");
    assert_eq!(root_result.content, "parent consumed four attached results");

    let transcript = db
        .list_messages(&session.id, None, 100)
        .await
        .expect("read completed parallel transcript")
        .expect("parallel session exists")
        .messages;
    let final_assistant = transcript
        .iter()
        .rev()
        .find(|message| message.role == MessageRole::Assistant)
        .expect("parent final assistant message");
    assert_eq!(
        text_of(&final_assistant.content),
        "parent consumed four attached results"
    );
    assert!(!text_of(&final_assistant.content).trim().is_empty());

    graceful_shutdown_production_server(server, &state, &db).await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Full fail-closed boundary from provider tail to durable TaskResult.
async fn missing_usage_fails_before_agent_tool_side_effects() {
    let isolated = IsolatedDir::create();
    let workspace = isolated.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("create isolated workspace");
    let db = Db::open(isolated.path().join("missing-usage.sqlite3"))
        .expect("file SQLite boots final schema");

    let provider = Arc::new(MissingUsageAgentProvider::default());
    let mut providers = ProviderRegistry::new();
    providers.register("scripted", provider.clone(), vec![MODEL.to_owned()]);

    let mut config = Config::test_config();
    config.default_model = MODEL.to_owned();
    config.db_path = isolated.path().join("missing-usage.sqlite3");
    config.workspace_default_root = workspace.to_string_lossy().into_owned();
    config.snapshot_dir = Some(isolated.path().join("snapshots"));
    config.scratchpad_system_root = isolated.path().join("scratchpad");
    config.mcp_registry_path = isolated.path().join("mcp-capabilities.json");
    config.agent_enabled = true;

    let state = AppState::new_with_ws(db.clone(), config, WsConfig::fast_for_tests())
        .with_providers(providers.with_default_model(MODEL));
    install_runtime_startup_epoch(&state, &db).await;
    let _engine = wire_engine(&state);
    let router = build_router(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind isolated server port");
    let addr = listener.local_addr().expect("fixture server address");
    let server = start_production_server(listener, router);

    let session = db
        .create_session(MODEL, workspace.to_string_lossy().as_ref())
        .await
        .expect("create root session");
    let mut ws = connect(addr).await;
    send_json(
        &mut ws,
        serde_json::json!({
            "type": "bind_session",
            "sessionId": session.id,
            "bindRequestId": "missing-usage-bind",
            "bindingEpoch": 1,
            "protocolVersion": 4
        }),
    )
    .await;
    assert_eq!(next_json(&mut ws).await["type"], "session_restored");
    send_json(
        &mut ws,
        serde_json::json!({
            "type": "user_message",
            "text": "attempt the forbidden child call",
            "attachments": [],
            "references": []
        }),
    )
    .await;

    let mut runtime_error = None;
    let complete = loop {
        let frame = next_json(&mut ws).await;
        if frame["type"] == "error" {
            runtime_error = Some(frame);
            continue;
        }
        if frame["type"] == "message_complete" {
            break frame;
        }
    };
    let runtime_error = runtime_error.expect("missing usage emits an exact runtime error");
    assert_eq!(runtime_error["code"], "BUDGET_USAGE_INCOMPLETE");
    assert_eq!(complete["stopReason"], "error");
    let run_id = complete["runId"].as_str().expect("terminal Run identity");

    let run = db
        .find_run_by_id(run_id)
        .await
        .expect("read failed Run")
        .expect("failed Run exists");
    assert_eq!(run.status, RunStatus::Failed.as_db());
    assert!(!run.usage_complete);
    let task = db
        .find_runtime_task_by_id(&run.task_id)
        .await
        .expect("read failed Task")
        .expect("failed Task exists");
    assert_eq!(task.status, TaskStatus::Failed);
    assert!(!task.usage_complete);
    let result = db
        .read_task_result(&task.id, None, 0, 65_536)
        .await
        .expect("read failed TaskResult")
        .expect("failed TaskResult exists");
    assert_eq!(result.result.status, ResultStatus::Error);
    assert_eq!(
        result.result.error_code.as_deref(),
        Some("BUDGET_USAGE_INCOMPLETE")
    );

    let durable_counts = db
        .with_conn_blocking(|conn| {
            let tool_invocations =
                conn.query_row("SELECT COUNT(*) FROM tool_invocations", [], |row| {
                    row.get(0)
                })?;
            let tasks = conn.query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))?;
            let reservations =
                conn.query_row("SELECT COUNT(*) FROM task_budget_reservations", [], |row| {
                    row.get(0)
                })?;
            let llm_call = conn.query_row(
                "SELECT COUNT(*),
                        COALESCE(SUM(CASE WHEN status='completed'
                                          AND usage_complete=0
                                          AND input_tokens IS NULL
                                          AND output_tokens IS NULL
                                         THEN 1 ELSE 0 END),0)
                 FROM llm_calls",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )?;
            Ok((tool_invocations, tasks, reservations, llm_call))
        })
        .expect("read fail-closed durable facts");
    assert_eq!(durable_counts, (0_i64, 1_i64, 0_i64, (1_i64, 1_i64)));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

    drop(ws);
    graceful_shutdown_production_server(server, &state, &db).await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn child_budget_failure_emits_agent_failed_and_persists_partial_budget_terminal() {
    let isolated = IsolatedDir::create();
    let workspace = isolated.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("create isolated workspace");
    let db = Db::open(isolated.path().join("child-budget.sqlite3"))
        .expect("file SQLite boots final schema");

    let provider = Arc::new(ChildBudgetFailureProvider::default());
    let mut providers = ProviderRegistry::new();
    providers.register("scripted", provider.clone(), vec![MODEL.to_owned()]);

    let mut config = Config::test_config();
    config.default_model = MODEL.to_owned();
    config.db_path = isolated.path().join("child-budget.sqlite3");
    config.workspace_default_root = workspace.to_string_lossy().into_owned();
    config.snapshot_dir = Some(isolated.path().join("snapshots"));
    config.scratchpad_system_root = isolated.path().join("scratchpad");
    config.mcp_registry_path = isolated.path().join("mcp-capabilities.json");
    config.agent_enabled = true;

    let state = AppState::new_with_ws(db.clone(), config, WsConfig::fast_for_tests())
        .with_providers(providers.with_default_model(MODEL));
    install_runtime_startup_epoch(&state, &db).await;
    let _engine = wire_engine(&state);
    let router = build_router(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind isolated server port");
    let addr = listener.local_addr().expect("fixture server address");
    let server = start_production_server(listener, router);

    let session = db
        .create_session(MODEL, workspace.to_string_lossy().as_ref())
        .await
        .expect("create root session");
    state
        .authz
        .modes
        .set_mode(&session.id, zk_authz::model::PermissionMode::AutoApprove)
        .await;
    let mut ws = connect(addr).await;
    send_json(
        &mut ws,
        serde_json::json!({
            "type": "bind_session",
            "sessionId": session.id,
            "bindRequestId": "child-budget-bind",
            "bindingEpoch": 1,
            "protocolVersion": 4
        }),
    )
    .await;
    assert_eq!(next_json(&mut ws).await["type"], "session_restored");
    send_json(
        &mut ws,
        serde_json::json!({
            "type": "user_message",
            "text": "delegate the child budget failure fixture",
            "attachments": [],
            "references": []
        }),
    )
    .await;

    let mut frames = Vec::new();
    loop {
        let frame = next_json(&mut ws).await;
        let complete = frame["type"] == "message_complete";
        frames.push(frame);
        if complete {
            break;
        }
    }

    let tasks = db
        .find_task_tree_owned(&session.id)
        .await
        .expect("read root/child Task tree");
    assert_eq!(tasks.len(), 2);
    let root_task = tasks
        .iter()
        .find(|task| task.parent_task_id.is_none())
        .expect("root Task");
    let child_task = tasks
        .iter()
        .find(|task| task.parent_task_id.as_deref() == Some(root_task.id.as_str()))
        .expect("child Task");

    let failed = frames
        .iter()
        .find(|frame| frame["type"] == "agent_failed" && frame["agentId"] == child_task.id.as_str())
        .expect("budget rejection publishes AgentFailed");
    assert_eq!(failed["error"], "COST_BUDGET_EXHAUSTED");
    assert!(
        !frames.iter().any(|frame| {
            frame["type"] == "agent_completed" && frame["agentId"] == child_task.id.as_str()
        }),
        "the same child must never publish AgentCompleted"
    );

    assert_eq!(child_task.status, TaskStatus::Partial);
    let child_result = db
        .read_task_result(&child_task.id, None, 0, 65_536)
        .await
        .expect("read child TaskResult")
        .expect("child owns immutable TaskResult");
    assert_eq!(child_result.result.status, ResultStatus::Partial);
    assert_eq!(
        child_result.result.error_code.as_deref(),
        Some("COST_BUDGET_EXHAUSTED")
    );
    let child_run_id = child_task.current_run_id.as_deref().expect("child Run id");
    let child_run = db
        .find_run_by_id(child_run_id)
        .await
        .expect("read child Run")
        .expect("child Run exists");
    assert_eq!(child_run.status, RunStatus::Completed.as_db());
    assert_eq!(
        child_run.exit_reason.as_deref(),
        Some(zk_db::run::EXIT_BUDGET_EXHAUSTED)
    );
    let child_budget = db
        .read_task_budget(&child_task.id)
        .await
        .expect("read child budget")
        .expect("child budget exists");
    assert_eq!(
        child_budget
            .reservation
            .expect("attached child reservation")
            .status,
        BudgetReservationStatus::Incomplete,
        "the synthetic provider error has no authoritative usage to settle"
    );

    drop(ws);
    graceful_shutdown_production_server(server, &state, &db).await;
}

/// An authoritative provider response may overrun the child's fixed 20% cost
/// allocation even though its conservative preflight reservation was admitted.
/// That child is terminal `partial`, but its complete usage must not poison the
/// root ledger or prevent the parent from consuming the durable receipt.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn authoritative_child_cost_overrun_remains_complete_usage_and_parent_resumes() {
    let isolated = IsolatedDir::create();
    let workspace = isolated.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("create isolated workspace");
    let db = Db::open(isolated.path().join("authoritative-child-overrun.sqlite3"))
        .expect("file SQLite boots final schema");

    let provider = Arc::new(AuthoritativeChildCostOverrunProvider::default());
    let mut providers = ProviderRegistry::new();
    providers.register("dashscope", provider.clone(), vec![MODEL.to_owned()]);

    let mut config = Config::test_config();
    config.default_model = MODEL.to_owned();
    config.db_path = isolated.path().join("authoritative-child-overrun.sqlite3");
    config.workspace_default_root = workspace.to_string_lossy().into_owned();
    config.snapshot_dir = Some(isolated.path().join("snapshots"));
    config.scratchpad_system_root = isolated.path().join("scratchpad");
    config.mcp_registry_path = isolated.path().join("mcp-capabilities.json");
    config.agent_enabled = true;
    config.coordinator_mode_enabled = true;
    // Production defaults are spend-unlimited. This regression explicitly
    // enables the legacy $4 root ceiling to exercise optional overrun handling.
    config.root_task_budget_policy.cost_limit_nanos_usd = Some(4_000_000_000);

    let state = AppState::new_with_ws(db.clone(), config, WsConfig::fast_for_tests())
        .with_providers(providers.with_default_model(MODEL));
    install_runtime_startup_epoch(&state, &db).await;
    let _engine = wire_engine(&state);
    let router = build_router(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind isolated server port");
    let addr = listener.local_addr().expect("fixture server address");
    let server = start_production_server(listener, router);

    let session = db
        .create_session(MODEL, workspace.to_string_lossy().as_ref())
        .await
        .expect("create root session");
    state
        .authz
        .modes
        .set_mode(&session.id, zk_authz::model::PermissionMode::AutoApprove)
        .await;
    let mut ws = connect(addr).await;
    send_json(
        &mut ws,
        serde_json::json!({
            "type": "bind_session",
            "sessionId": session.id,
            "bindRequestId": "authoritative-child-overrun-bind",
            "bindingEpoch": 1,
            "protocolVersion": 4
        }),
    )
    .await;
    assert_eq!(next_json(&mut ws).await["type"], "session_restored");

    let frames = run_prompt_to_completion(ws, OVERRUN_ROOT_PROMPT.to_owned()).await;
    assert!(frames.iter().all(|frame| frame["type"] != "error"));
    let completion = frames
        .iter()
        .find(|frame| frame["type"] == "message_complete")
        .expect("root publishes completion");
    assert_ne!(completion["stopReason"], "error");

    let tasks = db
        .find_task_tree_owned(&session.id)
        .await
        .expect("read authoritative overrun Task tree");
    assert_eq!(tasks.len(), 2);
    let root_task = tasks
        .iter()
        .find(|task| task.parent_task_id.is_none())
        .expect("root Task");
    let child_task = tasks
        .iter()
        .find(|task| task.parent_task_id.as_deref() == Some(root_task.id.as_str()))
        .expect("child Task");
    assert_eq!(root_task.status, TaskStatus::Succeeded);
    assert!(root_task.usage_complete);
    assert_eq!(child_task.status, TaskStatus::Partial);
    assert!(child_task.usage_complete);

    let root_run_id = root_task.current_run_id.as_deref().expect("root Run id");
    let child_run_id = child_task.current_run_id.as_deref().expect("child Run id");
    let root_run = db
        .find_run_by_id(root_run_id)
        .await
        .expect("read root Run")
        .expect("root Run exists");
    let child_run = db
        .find_run_by_id(child_run_id)
        .await
        .expect("read child Run")
        .expect("child Run exists");
    assert_eq!(root_run.status, RunStatus::Completed.as_db());
    assert!(root_run.usage_complete);
    assert_eq!(child_run.status, RunStatus::Completed.as_db());
    assert!(child_run.usage_complete);
    assert_eq!(
        child_run.exit_reason.as_deref(),
        Some(zk_db::run::EXIT_BUDGET_EXHAUSTED)
    );

    let child_result = db
        .read_task_result(&child_task.id, None, 0, 65_536)
        .await
        .expect("read child TaskResult")
        .expect("partial child owns a result");
    assert_eq!(child_result.result.status, ResultStatus::Partial);
    assert_eq!(
        child_result.result.error_code.as_deref(),
        Some("COST_BUDGET_EXHAUSTED")
    );
    let root_result = db
        .read_task_result(&root_task.id, None, 0, 65_536)
        .await
        .expect("read root TaskResult")
        .expect("resumed root owns a result");
    assert_eq!(root_result.result.status, ResultStatus::Complete);
    assert_eq!(root_result.content, OVERRUN_PARENT_ANSWER);
    assert!(!root_result.content.trim().is_empty());

    let reservation = db
        .read_task_budget(&child_task.id)
        .await
        .expect("read child budget")
        .expect("child budget exists")
        .reservation
        .expect("child reservation exists");
    assert_eq!(reservation.status, BudgetReservationStatus::Incomplete);
    assert!(reservation.usage_complete);
    assert_eq!(reservation.used_tokens, Some(90_001));
    assert_eq!(reservation.used_cost_nanos_usd, Some(810_054_000));

    let root_task_id = root_task.id.clone();
    let root_run_id = root_run_id.to_owned();
    let child_run_id = child_run_id.to_owned();
    let durable_counts: (i64, i64, i64, i64, i64) = db
        .with_conn_blocking(move |conn| {
            let llm_calls = conn.query_row(
                "SELECT COUNT(*),COALESCE(SUM(status='completed' AND usage_complete=1),0)
                 FROM llm_calls WHERE run_id IN (?1,?2)",
                rusqlite::params![root_run_id, child_run_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )?;
            let receipts = conn.query_row(
                "SELECT COUNT(*) FROM task_result_receipts WHERE consumer_task_id=?1",
                rusqlite::params![root_task_id],
                |row| row.get::<_, i64>(0),
            )?;
            let active_reservations = conn.query_row(
                "SELECT COUNT(*) FROM task_budget_reservations
                 WHERE root_task_id=?1 AND status='active'",
                rusqlite::params![root_task_id],
                |row| row.get::<_, i64>(0),
            )?;
            let incomplete_usage = conn.query_row(
                "SELECT COUNT(*) FROM llm_calls
                 WHERE run_id IN (?1,?2) AND usage_complete=0",
                rusqlite::params![root_run_id, child_run_id],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((
                llm_calls.0,
                llm_calls.1,
                receipts,
                active_reservations,
                incomplete_usage,
            ))
        })
        .expect("read authoritative overrun durable facts");
    assert_eq!(durable_counts, (3, 3, 1, 0, 0));
    assert_eq!(provider.initial_root.load(Ordering::SeqCst), 1);
    assert_eq!(provider.child_executions.load(Ordering::SeqCst), 1);
    assert_eq!(provider.parent_resumptions.load(Ordering::SeqCst), 1);

    graceful_shutdown_production_server(server, &state, &db).await;
}

#[derive(Clone, Debug)]
struct IncidentTaskIdentity {
    session_id: String,
    mode: &'static str,
    ordinal: usize,
    root_task_id: String,
    root_run_id: String,
    child_task_id: String,
    child_run_id: String,
}

async fn wait_for_terminal_task(db: &Db, task_id: &str) -> zk_db::RuntimeTaskRecord {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let task = db
            .find_runtime_task_by_id(task_id)
            .await
            .expect("poll durable Task")
            .expect("polled Task exists");
        if task.status.is_terminal() || task.status == TaskStatus::NeedsAttention {
            return task;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Task {task_id} did not settle: {}",
            task.status.as_db()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Regression for the production incident that originally produced visible child Sessions,
/// missing Task IDs, `TASK_NOT_FOUND`, cross-Agent tool cards and ghost `running` state.
///
/// The test goes through the real websocket router, production `wire_engine`, the real Agent and
/// `TaskCreate` bridges, a file-backed `SQLite` database and eight concurrently blocked child engines.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn production_incident_four_terminal_and_four_background_agents_are_durable_and_partitioned()
{
    const CHILDREN: usize = 8;
    let isolated = IsolatedDir::create();
    let workspace = isolated.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("create isolated workspace");
    let db =
        Db::open(isolated.path().join("incident.sqlite3")).expect("file SQLite boots final schema");

    let provider = Arc::new(IncidentProvider::new(
        zk_llm::registry::VERIFIED_PROVIDER_CONCURRENCY,
    ));
    let mut providers = ProviderRegistry::new();
    providers.register("dashscope", provider.clone(), vec![MODEL.to_owned()]);

    let mut config = Config::test_config();
    config.default_model = MODEL.to_owned();
    config.db_path = isolated.path().join("incident.sqlite3");
    config.workspace_default_root = workspace.to_string_lossy().into_owned();
    config.snapshot_dir = Some(isolated.path().join("snapshots"));
    config.scratchpad_system_root = isolated.path().join("scratchpad");
    config.mcp_registry_path = isolated.path().join("mcp-capabilities.json");
    config.agent_enabled = true;

    let state = AppState::new_with_ws(db.clone(), config, WsConfig::fast_for_tests())
        .with_providers(providers.with_default_model(MODEL));
    install_runtime_startup_epoch(&state, &db).await;
    let _engine = wire_engine(&state);
    let router = build_router(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind isolated server port");
    let addr = listener.local_addr().expect("fixture server address");
    let server = start_production_server(listener, router);

    let mut session_ids = Vec::with_capacity(CHILDREN);
    let mut clients = Vec::with_capacity(CHILDREN);
    for ordinal in 0..CHILDREN {
        let mode = if ordinal < 4 {
            "terminal"
        } else {
            "background"
        };
        let session = db
            .create_session(MODEL, workspace.to_string_lossy().as_ref())
            .await
            .expect("create incident root session");
        state
            .authz
            .modes
            .set_mode(&session.id, zk_authz::model::PermissionMode::AutoApprove)
            .await;
        let mut ws = connect(addr).await;
        send_json(
            &mut ws,
            serde_json::json!({
                "type": "bind_session",
                "sessionId": session.id,
                "bindRequestId": format!("incident-bind-{ordinal}"),
                "bindingEpoch": 1,
                "protocolVersion": 4
            }),
        )
        .await;
        assert_eq!(next_json(&mut ws).await["type"], "session_restored");
        session_ids.push(session.id);
        clients.push(tokio::spawn(run_prompt_to_completion(
            ws,
            format!("{INCIDENT_ROOT_PREFIX}{mode}:{ordinal}"),
        )));
    }

    tokio::time::timeout(
        Duration::from_secs(20),
        provider.wait_until_all_children_are_executing(),
    )
    .await
    .expect("all eight children execute concurrently");

    // Submission is the acknowledgement boundary. While every provider stream remains blocked,
    // all Task/Run/internal-Session rows must already be visible and queryable.
    let runtime = state.task_runtime();
    let mut identities = Vec::with_capacity(CHILDREN);
    for (ordinal, session_id) in session_ids.iter().enumerate() {
        let mode = if ordinal < 4 {
            "terminal"
        } else {
            "background"
        };
        let tasks = db
            .find_task_tree_owned(session_id)
            .await
            .expect("query incident Task tree before completion");
        assert_eq!(tasks.len(), 2, "root and child are durable before result");
        let root = tasks
            .iter()
            .find(|task| task.parent_task_id.is_none())
            .expect("durable root Task");
        let child = tasks
            .iter()
            .find(|task| task.parent_task_id.as_deref() == Some(root.id.as_str()))
            .expect("durable attached child Task");
        assert_eq!(root.status, TaskStatus::WaitingDependencies);
        assert_eq!(child.status, TaskStatus::Running);
        let child_from_get = runtime
            .get_owned(session_id, &child.id)
            .await
            .expect("TaskGet storage succeeds")
            .expect("TaskGet immediately finds submitted child");
        assert_eq!(child_from_get.id, child.id);
        let root_run_id = root.current_run_id.clone().expect("root Run identity");
        let child_run_id = child.current_run_id.clone().expect("child Run identity");
        assert!(
            db.find_run_by_id(&child_run_id)
                .await
                .expect("query child Run")
                .is_some(),
            "submission never exposes a Task without its Run"
        );

        // Background handles are useful only if non-destructive queries and messaging work while
        // the child is still active. A wait expiry is an ordinary response, never a tool error.
        if mode == "background" {
            let output = runtime
                .read_output(TaskOutputRequest {
                    root_session_id: session_id.clone(),
                    task_id: child.id.clone(),
                    wait_ms: 20,
                    result_version: None,
                    cursor: 0,
                    max_bytes: 65_536,
                })
                .await
                .expect("TaskOutput wait expiry is not an error");
            assert!(output.wait_expired);
            assert!(output.result.is_none());

            let inbox = runtime
                .send_message(
                    session_id,
                    &child.id,
                    Some(&root.id),
                    &format!("durable follow-up {ordinal}"),
                )
                .await
                .expect("SendMessage persists while child is active");
            assert_eq!(inbox.status, InboxStatus::Queued);
            let persisted = runtime
                .read_inbox(session_id, &child.id, &[InboxStatus::Queued], 10)
                .await
                .expect("read durable child inbox");
            assert!(
                persisted
                    .iter()
                    .any(|message| message.message_id == inbox.message_id)
            );
        }

        identities.push(IncidentTaskIdentity {
            session_id: session_id.clone(),
            mode,
            ordinal,
            root_task_id: root.id.clone(),
            root_run_id,
            child_task_id: child.id.clone(),
            child_run_id,
        });
    }

    provider.release_children();
    let mut all_frames = Vec::with_capacity(CHILDREN);
    for client in clients {
        let frames = tokio::time::timeout(Duration::from_secs(30), client)
            .await
            .expect("incident root completes after child release")
            .expect("websocket reader task succeeds");
        all_frames.push(frames);
    }

    for (identity, frames) in identities.iter().zip(&all_frames) {
        let expected_tool_use_id = format!("incident-{}-{}", identity.mode, identity.ordinal);
        let starts = frames
            .iter()
            .filter(|frame| {
                frame["type"] == "tool_use_start" && frame["toolUseId"] == expected_tool_use_id
            })
            .collect::<Vec<_>>();
        let results = frames
            .iter()
            .filter(|frame| {
                frame["type"] == "tool_result" && frame["toolUseId"] == expected_tool_use_id
            })
            .collect::<Vec<_>>();
        assert_eq!(starts.len(), 1, "one visible start per invocation");
        assert_eq!(results.len(), 1, "one terminal result clears the tool card");
        for frame in [starts[0], results[0]] {
            assert_eq!(
                frame["eventContext"]["taskId"], identity.root_task_id,
                "tool UI ownership is the caller Task, not the returned child"
            );
            assert_eq!(
                frame["eventContext"]["runId"], identity.root_run_id,
                "start/result must stay in one caller Run partition"
            );
            assert_eq!(
                frame["eventContext"]["toolUseId"], expected_tool_use_id,
                "tool event retains its invocation identity"
            );
        }
        assert_ne!(
            starts[0]["eventContext"]["eventId"], results[0]["eventContext"]["eventId"],
            "durable WS events have unique replay IDs"
        );
        assert_eq!(
            results[0]["result"]["metadata"]["structuredResult"]["taskId"], identity.child_task_id,
            "business result still identifies the child"
        );

        let root = runtime
            .get_owned(&identity.session_id, &identity.root_task_id)
            .await
            .expect("query completed root")
            .expect("completed root exists");
        let child = runtime
            .get_owned(&identity.session_id, &identity.child_task_id)
            .await
            .expect("query completed child")
            .expect("completed child exists");
        assert_eq!(root.status, TaskStatus::Succeeded);
        assert_eq!(child.status, TaskStatus::Succeeded);
        assert!(root.usage_complete);
        assert!(child.usage_complete);
        let root_run = db
            .find_run_by_id(&identity.root_run_id)
            .await
            .expect("query completed root Run")
            .expect("root Run exists");
        let child_run = db
            .find_run_by_id(&identity.child_run_id)
            .await
            .expect("query completed child Run")
            .expect("child Run exists");
        assert_eq!(root_run.status, RunStatus::Completed.as_db());
        assert_eq!(child_run.status, RunStatus::Completed.as_db());
        assert!(root_run.usage_complete);
        assert!(child_run.usage_complete);
        let child_budget = db
            .read_task_budget(&identity.child_task_id)
            .await
            .expect("query completed child budget")
            .expect("child budget exists");
        assert_eq!(
            child_budget
                .reservation
                .expect("child reservation exists")
                .status,
            BudgetReservationStatus::Settled
        );
        assert!(
            db.read_task_result(&identity.child_task_id, None, 0, 65_536)
                .await
                .expect("read child TaskResult")
                .is_some()
        );
        let diagnostic = db
            .find_task_diagnostic(&identity.root_task_id)
            .await
            .expect("query root diagnostic")
            .expect("root diagnostic exists");
        assert_eq!(diagnostic.receipts.len(), 1);

        let terminal_message = runtime
            .send_message(
                &identity.session_id,
                &identity.child_task_id,
                Some(&identity.root_task_id),
                "late follow-up",
            )
            .await
            .expect_err("terminal child rejects a late message explicitly");
        assert_eq!(terminal_message.code, "TASK_TERMINAL");
    }

    let visible_sessions = db
        .list_sessions(None, 100)
        .await
        .expect("list root sessions only");
    assert_eq!(visible_sessions.sessions.len(), CHILDREN);
    assert_eq!(provider.root_calls.load(Ordering::SeqCst), CHILDREN);
    assert_eq!(provider.child_calls.load(Ordering::SeqCst), CHILDREN);
    assert_eq!(provider.resumed_calls.load(Ordering::SeqCst), CHILDREN);
    let usage_rows = db
        .with_conn_blocking(|conn| {
            conn.query_row(
                "SELECT COUNT(*),
                        COALESCE(SUM(CASE WHEN status='completed'
                                          AND usage_complete=1
                                          AND input_tokens IS NOT NULL
                                          AND output_tokens IS NOT NULL
                                          AND cost_nanos_usd IS NOT NULL
                                         THEN 1 ELSE 0 END),0)
                 FROM llm_calls",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(Into::into)
        })
        .expect("read incident LLM usage rows");
    let expected_calls = i64::try_from(CHILDREN * 3).expect("fixture count fits i64");
    assert_eq!(usage_rows, (expected_calls, expected_calls));

    graceful_shutdown_production_server(server, &state, &db).await;
}

/// `TaskStop` and parent cancellation share the production `TaskRuntime` cancellation tree.
/// Both children are held inside an actual provider request so the test exercises cancellation
/// during execution rather than the easier queued-state case.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn production_task_stop_and_parent_cancel_cascade_settle_without_late_wakeup() {
    const CASES: usize = 2;
    let isolated = IsolatedDir::create();
    let workspace = isolated.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("create isolated workspace");
    let db = Db::open(isolated.path().join("cancellation.sqlite3"))
        .expect("file SQLite boots final schema");
    let provider = Arc::new(IncidentProvider::new(CASES));
    let mut providers = ProviderRegistry::new();
    providers.register("dashscope", provider.clone(), vec![MODEL.to_owned()]);

    let mut config = Config::test_config();
    config.default_model = MODEL.to_owned();
    config.db_path = isolated.path().join("cancellation.sqlite3");
    config.workspace_default_root = workspace.to_string_lossy().into_owned();
    config.snapshot_dir = Some(isolated.path().join("snapshots"));
    config.scratchpad_system_root = isolated.path().join("scratchpad");
    config.mcp_registry_path = isolated.path().join("mcp-capabilities.json");
    config.agent_enabled = true;

    let state = AppState::new_with_ws(db.clone(), config, WsConfig::fast_for_tests())
        .with_providers(providers.with_default_model(MODEL));
    install_runtime_startup_epoch(&state, &db).await;
    let _engine = wire_engine(&state);
    let router = build_router(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind isolated server port");
    let addr = listener.local_addr().expect("fixture server address");
    let server = start_production_server(listener, router);

    let mut sessions = Vec::with_capacity(CASES);
    let mut sockets = Vec::with_capacity(CASES);
    for ordinal in 0..CASES {
        let session = db
            .create_session(MODEL, workspace.to_string_lossy().as_ref())
            .await
            .expect("create cancellation root session");
        state
            .authz
            .modes
            .set_mode(&session.id, zk_authz::model::PermissionMode::AutoApprove)
            .await;
        let mut ws = connect(addr).await;
        send_json(
            &mut ws,
            serde_json::json!({
                "type": "bind_session",
                "sessionId": session.id,
                "bindRequestId": format!("cancel-bind-{ordinal}"),
                "bindingEpoch": 1,
                "protocolVersion": 4
            }),
        )
        .await;
        assert_eq!(next_json(&mut ws).await["type"], "session_restored");
        send_json(
            &mut ws,
            serde_json::json!({
                "type": "user_message",
                "text": format!("{INCIDENT_ROOT_PREFIX}background:{ordinal}"),
                "attachments": [],
                "references": []
            }),
        )
        .await;
        sessions.push(session.id);
        sockets.push(ws);
    }

    tokio::time::timeout(
        Duration::from_secs(20),
        provider.wait_until_all_children_are_executing(),
    )
    .await
    .expect("both cancellation children enter provider execution");

    let mut task_pairs = Vec::with_capacity(CASES);
    for session_id in &sessions {
        let tasks = db
            .find_task_tree_owned(session_id)
            .await
            .expect("read cancellation Task tree");
        let root = tasks
            .iter()
            .find(|task| task.parent_task_id.is_none())
            .expect("root Task")
            .clone();
        let child = tasks
            .iter()
            .find(|task| task.parent_task_id.as_deref() == Some(root.id.as_str()))
            .expect("child Task")
            .clone();
        assert_eq!(root.status, TaskStatus::WaitingDependencies);
        assert_eq!(child.status, TaskStatus::Running);
        task_pairs.push((root, child));
    }

    // Exercise the actual production TaskStop tool and its RunTerminationCoordinator bridge.
    let stop_tool = state
        .tools()
        .get("TaskStop")
        .expect("TaskStop is installed");
    let (progress_tx, _progress_rx) = tokio::sync::mpsc::unbounded_channel();
    let stop_output = stop_tool
        .execute(
            serde_json::json!({"taskId": task_pairs[0].1.id}),
            ToolContext::new(CancellationToken::new(), progress_tx)
                .with_session_id(&sessions[0])
                .with_run_id(task_pairs[0].0.current_run_id.as_deref().expect("root Run"))
                .with_tool_use_id("production-task-stop")
                .with_working_dir(&workspace),
        )
        .await;
    assert!(!stop_output.is_error, "TaskStop failed: {stop_output:?}");
    let stop_result = &stop_output
        .metadata
        .as_ref()
        .expect("TaskStop structured metadata")["structuredResult"];
    assert_eq!(stop_result["taskId"], task_pairs[0].1.id);
    assert_eq!(stop_result["cancelRequested"], true);

    // Cancelling the parent is a single durable operation that cascades to its attached child
    // before signalling the parent execution token.
    let parent_cancel = state
        .task_runtime()
        .cancel_owned(
            &sessions[1],
            &task_pairs[1].0.id,
            "cancel parent incident fixture",
        )
        .await
        .expect("parent cancellation request persists");
    assert!(parent_cancel.cancel_requested);
    let cascaded_child = db
        .find_runtime_task_by_id(&task_pairs[1].1.id)
        .await
        .expect("query cascaded child")
        .expect("cascaded child exists");
    assert!(
        cascaded_child.status == TaskStatus::Cancelling || cascaded_child.status.is_terminal(),
        "child cancellation must be durable before parent cancellation returns"
    );

    let stopped_child = wait_for_terminal_task(&db, &task_pairs[0].1.id).await;
    let cascaded_child = wait_for_terminal_task(&db, &task_pairs[1].1.id).await;
    let cancelled_parent = wait_for_terminal_task(&db, &task_pairs[1].0.id).await;
    assert_eq!(stopped_child.status, TaskStatus::Cancelled);
    assert_eq!(cascaded_child.status, TaskStatus::Cancelled);
    assert_eq!(cancelled_parent.status, TaskStatus::Cancelled);

    let stopped_run = db
        .find_run_by_id(
            stopped_child
                .current_run_id
                .as_deref()
                .expect("stopped child Run"),
        )
        .await
        .expect("query stopped child Run")
        .expect("stopped child Run exists");
    let cascaded_run = db
        .find_run_by_id(
            cascaded_child
                .current_run_id
                .as_deref()
                .expect("cascaded child Run"),
        )
        .await
        .expect("query cascaded child Run")
        .expect("cascaded child Run exists");
    assert_eq!(stopped_run.exit_reason.as_deref(), Some("userCancelled"));
    assert_eq!(cascaded_run.exit_reason.as_deref(), Some("parentCancelled"));

    // A late child result cannot reactivate a cancelled parent or create a second attempt.
    provider.release_children();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let parent_after_release = db
        .find_runtime_task_by_id(&task_pairs[1].0.id)
        .await
        .expect("query parent after late release")
        .expect("parent remains present");
    assert_eq!(parent_after_release.status, TaskStatus::Cancelled);
    let parent_runs = db
        .find_runs_by_session(&sessions[1], 10)
        .await
        .expect("query root attempts");
    assert_eq!(
        parent_runs
            .iter()
            .filter(|run| run.task_id == task_pairs[1].0.id)
            .count(),
        1,
        "cancelled parent is never resumed"
    );

    // Keep the sockets alive until every durable assertion is complete, then let the
    // production-style shutdown path drain the listener and TaskRuntime owners.
    drop(sockets);
    graceful_shutdown_production_server(server, &state, &db).await;
}
