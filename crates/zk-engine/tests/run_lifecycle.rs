//! Run 生命周期集成测试：`run_envelopes` 必须在工具阶段之前落库，且 run 结束
//! 时必须落终态。
//!
//! 回归的 P0 缺陷：`prepare_run` 曾只生成 `run_id` 而从不写 `run_envelopes`，
//! 于是工具阶段的授权祖先链（`zk-authz` 的
//! `AuthorizationSubjectResolver::load_root`）`SELECT ... FROM run_envelopes
//! WHERE id=?` 返回 0 行 → `Run ancestry contains a missing parent`，**真实会话
//! 中所有工具调用全部失败**。
//!
//! 依赖铁律禁止 `zk-engine → zk-authz`，故本测试用 [`AncestryAdmission`]
//! 在准入端口上**逐字复刻** `load_root` 的上溯查询与四条失败消息
//! （`crates/zk-authz/src/subject.rs:138-185`）：引擎侧只要能让这份复刻解析成功，
//! 生产路径上真正的 `AuthorizationService` 也必然能解析。

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use zk_db::Db;
use zk_engine::admission::{Admission, AdmissionRequest, ToolAdmission};
use zk_engine::{Engine, MessageSink};
use zk_llm::{ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent};
use zk_protocol::model::Usage;
use zk_protocol::{ClientMessage, ServerMessage};
use zk_tools::{EchoTool, Tool, ToolContext, ToolOutput, ToolRegistry};

/// `zk-authz` 授权链最大上溯深度（旧 `AuthorizationSubjectResolver.MAX_DEPTH`）。
const MAX_DEPTH: usize = 32;

/// 下行录制桩。
#[derive(Default)]
struct RecordingSink {
    pushed: Mutex<Vec<ServerMessage>>,
}

impl MessageSink for RecordingSink {
    fn push<'a>(&'a self, _session_id: &'a str, message: ServerMessage) -> BoxFuture<'a, ()> {
        self.pushed.lock().expect("sink lock").push(message);
        Box::pin(futures::future::ready(()))
    }
}

impl RecordingSink {
    fn kinds(&self) -> Vec<&'static str> {
        self.pushed
            .lock()
            .expect("sink lock")
            .iter()
            .map(ServerMessage::kind)
            .collect()
    }
}

/// 脚本化 Provider。
struct MockProvider {
    scripts: Mutex<Vec<Result<BoxStream<'static, ProviderEvent>, ProviderError>>>,
}

impl MockProvider {
    fn new(mut scripts: Vec<Result<BoxStream<'static, ProviderEvent>, ProviderError>>) -> Self {
        scripts.reverse();
        Self {
            scripts: Mutex::new(scripts),
        }
    }
}

impl ChatProvider for MockProvider {
    fn provider_name(&self) -> &'static str {
        "mock"
    }

    fn chat_stream(
        &self,
        _request: ChatRequest,
        _cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        self.scripts
            .lock()
            .expect("scripts lock")
            .pop()
            .expect("unexpected extra chat_stream call")
    }
}

#[expect(clippy::unnecessary_wraps, reason = "匹配脚本槽位 Result 形态")]
fn events(seq: Vec<ProviderEvent>) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
    Ok(stream::iter(seq).boxed())
}

/// 逐字复刻 `zk-authz` 的 `load_root` 上溯：只查 `run_envelopes`，缺行即
/// `Run ancestry contains a missing parent`；抵达根 Run 后解析
/// `sessions.working_dir`。
fn resolve_ancestry(db: &Db, current_run_id: &str) -> Result<(String, String), String> {
    let run_id = current_run_id.to_owned();
    db.with_conn_blocking(move |conn| {
        let mut seen: HashSet<String> = HashSet::new();
        let mut cursor = run_id;
        for _ in 0..=MAX_DEPTH {
            if !seen.insert(cursor.clone()) {
                return Ok(Err("Run ancestry contains a cycle".to_owned()));
            }
            let row: Option<(String, String, Option<String>)> = conn
                .query_row(
                    "SELECT id,session_id,parent_run_id FROM run_envelopes WHERE id=?1",
                    rusqlite::params![cursor],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .ok();
            let Some((id, session_id, parent_run_id)) = row else {
                return Ok(Err("Run ancestry contains a missing parent".to_owned()));
            };
            let Some(parent) = parent_run_id else {
                let working_dir: Option<String> = conn
                    .query_row(
                        "SELECT working_dir FROM sessions WHERE id=?1",
                        rusqlite::params![session_id],
                        |row| row.get(0),
                    )
                    .ok();
                return Ok(working_dir.map_or_else(
                    || Err("Root session is missing or ambiguous".to_owned()),
                    |_| Ok((id, session_id)),
                ));
            };
            cursor = parent;
        }
        Ok(Err(format!("Run ancestry exceeds {MAX_DEPTH} levels")))
    })
    .expect("ancestry query")
}

/// 准入端口桩：解析祖先链，失败即 `Failed`（对照旧
/// `catch (AdmissionException)` → 不推下行、回喂失败结果）。
struct AncestryAdmission {
    db: Db,
    outcomes: Mutex<Vec<Result<String, String>>>,
}

impl AncestryAdmission {
    fn new(db: Db) -> Arc<Self> {
        Arc::new(Self {
            db,
            outcomes: Mutex::new(Vec::new()),
        })
    }

    fn outcomes(&self) -> Vec<Result<String, String>> {
        self.outcomes.lock().expect("outcomes lock").clone()
    }
}

impl ToolAdmission for AncestryAdmission {
    fn admit<'a>(&'a self, request: AdmissionRequest<'a>) -> BoxFuture<'a, Admission> {
        let resolved = resolve_ancestry(&self.db, request.run_id);
        let input = request.input.clone();
        let outcome = match &resolved {
            Ok((root_run_id, _)) => {
                self.outcomes
                    .lock()
                    .expect("outcomes lock")
                    .push(Ok(root_run_id.clone()));
                Admission::Allow {
                    execution_input: input,
                }
            }
            Err(message) => {
                self.outcomes
                    .lock()
                    .expect("outcomes lock")
                    .push(Err(message.clone()));
                Admission::Failed {
                    code: "AUTHORIZATION_ANCESTRY_INVALID".to_owned(),
                    message: message.clone(),
                }
            }
        };
        Box::pin(async move { outcome })
    }
}

/// 装配：内存库 + 会话 + 祖先链准入端口。
async fn setup(
    scripts: Vec<Result<BoxStream<'static, ProviderEvent>, ProviderError>>,
    registry: ToolRegistry,
) -> (
    Arc<Engine>,
    Arc<RecordingSink>,
    Arc<AncestryAdmission>,
    Db,
    String,
) {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.7-max", "/tmp")
        .await
        .expect("create session");
    let sink = Arc::new(RecordingSink::default());
    let admission = AncestryAdmission::new(db.clone());
    let engine = Arc::new(Engine::with_admission(
        db.clone(),
        Arc::new(MockProvider::new(scripts)) as Arc<dyn ChatProvider>,
        Arc::clone(&sink) as Arc<dyn MessageSink>,
        Arc::new(registry),
        Arc::clone(&admission) as Arc<dyn ToolAdmission>,
    ));
    (engine, sink, admission, db, session.id)
}

/// `run_envelopes` 单行读回（status / `exit_reason` / `error_summary` /
/// `abort_reason` / `total_tokens` / `turn_count` / `parent_run_id` /
/// `agent_type` / `terminal_at` 是否非空）。
type RunRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    i64,
    i64,
    Option<String>,
    Option<String>,
    bool,
);

fn read_run(db: &Db, session_id: &str) -> RunRow {
    let session_id = session_id.to_owned();
    db.with_conn_blocking(move |conn| {
        conn.query_row(
            "SELECT id,status,exit_reason,error_summary,abort_reason,total_tokens,turn_count,\
                    parent_run_id,agent_type,terminal_at \
             FROM run_envelopes WHERE session_id=?1",
            rusqlite::params![session_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get::<_, Option<String>>(9)?.is_some(),
                ))
            },
        )
        .map_err(Into::into)
    })
    .expect("exactly one run per session")
}

fn tool_events(db: &Db, run_id: &str) -> Vec<String> {
    let run_id = run_id.to_owned();
    db.with_conn_blocking(move |conn| {
        let mut stmt =
            conn.prepare("SELECT event_type FROM run_event_log WHERE run_id=?1 ORDER BY seq")?;
        let rows = stmt
            .query_map(rusqlite::params![run_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    })
    .expect("events readable")
}

/// **P0 主回归**：工具阶段的授权祖先链必须能查到根 Run，工具正常执行。
///
/// 修复前 `run_envelopes` 无行，本用例的 `outcomes` 会是
/// `Err("Run ancestry contains a missing parent")` 且工具不执行。
#[tokio::test]
async fn tool_phase_resolves_run_ancestry_and_executes() {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(EchoTool));
    let (engine, sink, admission, db, sid) = setup(
        vec![
            events(vec![
                ProviderEvent::ToolUseStart {
                    id: "call-1".into(),
                    name: "Echo".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "call-1".into(),
                    delta: r#"{"text":"hi"}"#.into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::ToolUse,
                    usage: Some(Usage {
                        input_tokens: 7,
                        output_tokens: 3,
                        ..Usage::default()
                    }),
                },
            ]),
            events(vec![
                ProviderEvent::TextDelta {
                    text: "done".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(Usage {
                        input_tokens: 5,
                        output_tokens: 2,
                        ..Usage::default()
                    }),
                },
            ]),
        ],
        registry,
    )
    .await;

    Arc::clone(&engine)
        .run_user_message(sid.clone(), "echo hi".to_owned())
        .await;

    // 祖先链解析成功，且解析到的根 Run 即引擎写入的那一行。
    let (
        run_id,
        status,
        exit_reason,
        error_summary,
        _abort,
        tokens,
        turns,
        parent,
        agent,
        terminal,
    ) = read_run(&db, &sid);
    assert_eq!(
        admission.outcomes(),
        vec![Ok(run_id.clone())],
        "工具阶段祖先链必须解析到根 Run，不得再报 missing parent"
    );

    // 工具真的执行了（tool_result 下行 + 续轮文本）。
    let kinds = sink.kinds();
    assert!(kinds.contains(&"tool_result"), "kinds: {kinds:?}");
    assert!(kinds.contains(&"message_complete"), "kinds: {kinds:?}");

    // Run 终态：completed / model_finished / 跨轮累计 tokens / 轮数 2。
    assert_eq!(status, "completed");
    assert_eq!(exit_reason.as_deref(), Some("model_finished"));
    assert_eq!(error_summary, None);
    assert_eq!(tokens, 17, "7+3+5+2 跨轮累计 totalTokens");
    assert_eq!(turns, 2);
    assert_eq!(parent, None, "本次只写根 Run（子代理传播属后续扩展点）");
    assert_eq!(agent.as_deref(), Some("query"));
    assert!(terminal, "终态必须落 terminal_at");
    assert_eq!(
        tool_events(&db, &run_id),
        vec!["run_started".to_owned(), "run_status_changed".to_owned()]
    );
}

/// 缺行护栏：`run_envelopes` 无对应行时，复刻的祖先链逐字返回旧源失败消息
/// ——即修复前生产路径上每次工具调用命中的分支。
#[tokio::test]
async fn missing_run_row_reproduces_missing_parent_message() {
    let db = Db::open_in_memory().expect("in-memory db");
    let session = db
        .create_session("qwen3.7-max", "/tmp")
        .await
        .expect("create session");
    assert_eq!(
        resolve_ancestry(&db, "never-persisted-run"),
        Err("Run ancestry contains a missing parent".to_owned())
    );
    // 同一 id 落库后立即可解析（Run 写入是解除阻塞的唯一必要条件）。
    db.start_run(
        "never-persisted-run",
        &session.id,
        None,
        Some("query"),
        "qwen3.7-max",
    )
    .await
    .expect("start run");
    assert_eq!(
        resolve_ancestry(&db, "never-persisted-run"),
        Ok(("never-persisted-run".to_owned(), session.id))
    );
}

/// 纯文本单轮（无工具）同样写 Run 并落 `completed`。
#[tokio::test]
async fn plain_turn_starts_and_completes_run() {
    let (engine, _sink, _admission, db, sid) = setup(
        vec![events(vec![
            ProviderEvent::TextDelta {
                text: "hello".into(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(Usage {
                    input_tokens: 4,
                    output_tokens: 6,
                    ..Usage::default()
                }),
            },
        ])],
        ToolRegistry::new(),
    )
    .await;
    Arc::clone(&engine)
        .run_user_message(sid.clone(), "hi".to_owned())
        .await;

    let (_id, status, exit_reason, _err, _abort, tokens, turns, _parent, _agent, terminal) =
        read_run(&db, &sid);
    assert_eq!(status, "completed");
    assert_eq!(exit_reason.as_deref(), Some("model_finished"));
    assert_eq!(tokens, 10);
    assert_eq!(turns, 1);
    assert!(terminal);
}

/// Provider 建立期失败 → `failed` / `internal_error` + `error_summary`
/// （旧 `RunTracker.failRun(e.getMessage())`）。
#[tokio::test]
async fn provider_failure_marks_run_failed_with_summary() {
    let (engine, _sink, _admission, db, sid) = setup(
        vec![Err(ProviderError::Config {
            message: "missing api key".to_owned(),
        })],
        ToolRegistry::new(),
    )
    .await;
    Arc::clone(&engine)
        .run_user_message(sid.clone(), "hi".to_owned())
        .await;

    let (_id, status, exit_reason, error_summary, _abort, _tokens, _turns, _p, _a, terminal) =
        read_run(&db, &sid);
    assert_eq!(status, "failed");
    assert_eq!(exit_reason.as_deref(), Some("internal_error"));
    assert!(
        error_summary
            .as_deref()
            .is_some_and(|text| text.contains("missing api key")),
        "summary: {error_summary:?}"
    );
    assert!(terminal);
}

/// 永不完成的工具桩（中断路径）。
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

/// 用户中断 → `cancelled` / `user_cancelled`（旧 `abortRun` → `cancelByUser`）。
#[tokio::test]
async fn user_interrupt_marks_run_cancelled() {
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(NeverendingTool));
    let (engine, sink, _admission, db, sid) = setup(
        vec![events(vec![
            ProviderEvent::ToolUseStart {
                id: "call-1".into(),
                name: "Hang".into(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: None,
            },
        ])],
        registry,
    )
    .await;
    let handle = engine.spawn_user_message(&sid, "hang it".to_owned());
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

    let (_id, status, exit_reason, _err, abort_reason, _tokens, _turns, _p, _a, terminal) =
        read_run(&db, &sid);
    assert_eq!(status, "cancelled");
    assert_eq!(exit_reason.as_deref(), Some("user_cancelled"));
    assert_eq!(abort_reason.as_deref(), Some("user_cancelled"));
    assert!(terminal);
}
