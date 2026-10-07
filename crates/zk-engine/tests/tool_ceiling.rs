//! A request ceiling constrains actual root, child and Shell dispatch, including without scope factories.
use futures::{
    StreamExt,
    future::BoxFuture,
    stream::{self, BoxStream},
};
use serde_json::json;
use std::{
    collections::{BTreeSet, HashSet, VecDeque},
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;
use zk_db::{Db, TaskBudgetLimits, tool_ceiling::ToolCeiling};
use zk_engine::{
    ChildTaskSubmission, ConversationRunOptions, ConversationService, Engine, MessageSink,
    TaskOutputRequest, TaskRuntime,
};
use zk_llm::{ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent};
use zk_protocol::{ServerMessage, Usage};
use zk_tools::{BashTool, ToolRegistry};
const MODEL: &str = "qwen3.8-max-0902";
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
        "ceiling-fixture"
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
                .expect("no extra model requests"),
        )
        .boxed())
    }
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
fn command() -> Vec<ProviderEvent> {
    vec![
        ProviderEvent::ToolUseStart {
            id: "attempt".into(),
            name: "Bash".into(),
        },
        ProviderEvent::ToolInputDelta {
            id: "attempt".into(),
            delta: json!({"command":"printf forbidden > side-effect"}).to_string(),
        },
        finish(FinishReason::ToolUse),
    ]
}
fn reply() -> Vec<ProviderEvent> {
    vec![
        ProviderEvent::TextDelta {
            text: "finished".into(),
        },
        finish(FinishReason::EndTurn),
    ]
}
fn engine(db: &Db, scripts: Vec<Vec<ProviderEvent>>) -> (Arc<Engine>, Arc<Provider>) {
    let provider = Arc::new(Provider {
        scripts: Mutex::new(scripts.into()),
        requests: Mutex::new(Vec::new()),
    });
    let mut tools = ToolRegistry::new();
    tools.register(Arc::new(BashTool));
    (
        Arc::new(Engine::with_admission(
            db.clone(),
            provider.clone(),
            Arc::new(Sink),
            Arc::new(tools),
            zk_engine::admission::allow_all(),
        )),
        provider,
    )
}
fn directory() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("zk-ceiling-execution-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path).unwrap();
    path.canonicalize().unwrap()
}
async fn parent(db: &Db, session: &str, denied: bool) -> String {
    let run = uuid::Uuid::new_v4().to_string();
    db.start_conversation_run_with_policy(
        &run,
        session,
        MODEL,
        Some(&TaskBudgetLimits {
            deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
            ..TaskBudgetLimits::default()
        }),
        1,
        &ToolCeiling {
            allowed: Some(BTreeSet::from(["Bash".into(), "TaskCreate".into()])),
            denied: if denied {
                BTreeSet::from(["Bash".into()])
            } else {
                BTreeSet::new()
            },
        },
    )
    .await
    .unwrap();
    run
}
async fn resource_count(db: &Db, run: &str) -> i64 {
    let run = run.to_owned();
    db.with_reader(move |conn| {
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM execution_resources WHERE run_id=?1 AND resource_kind='processGroup' ",
            [run],
            |row| row.get(0),
        )?)
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn root_query_deny_filters_catalog_and_rejects_provider_invented_call_before_process() {
    let dir = directory();
    let db = Db::open_in_memory().unwrap();
    let session = db
        .create_session(MODEL, dir.to_str().unwrap())
        .await
        .unwrap()
        .id;
    let (engine, provider) = engine(&db, vec![command(), reply()]);
    let outcome = ConversationService::new(engine, db.clone())
        .execute_with_options(
            &session,
            "do work".into(),
            ConversationRunOptions {
                disallowed_tools: HashSet::from(["Bash".into()]),
                ..ConversationRunOptions::default()
            },
        )
        .await;
    assert_eq!(outcome.error, None);
    assert!(
        provider
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.tools.is_empty())
    );
    assert!(!dir.join("side-effect").exists());
    let run = db
        .find_latest_root_run_by_session(&session)
        .await
        .unwrap()
        .unwrap();
    assert!(!db.run_tool_ceiling(&run.id).await.unwrap().allows("Bash"));
    assert_eq!(resource_count(&db, &run.id).await, 0);
    std::fs::remove_dir_all(dir).unwrap();
}
#[tokio::test]
async fn shell_child_intersection_blocks_real_process_and_preserves_normal_allowed_execution() {
    for (temporary, denied) in [(false, true), (true, true), (false, false), (true, false)] {
        let dir = directory();
        let db = Db::open_in_memory().unwrap();
        let (session, _lease) = if temporary {
            let (session, lease) = db
                .create_ephemeral_session(MODEL, dir.to_str().unwrap(), "DONT_ASK")
                .await
                .unwrap();
            (session, Some(lease))
        } else {
            (
                db.create_session(MODEL, dir.to_str().unwrap())
                    .await
                    .unwrap()
                    .id,
                None,
            )
        };
        let root = parent(&db, &session, denied).await;
        let runtime = TaskRuntime::new(db.clone(), Arc::new(Sink));
        let (engine, provider) = engine(&db, vec![]);
        let engine = Arc::new(
            Arc::try_unwrap(engine)
                .ok()
                .expect("new fixture engine")
                .with_run_tool_scopes(Arc::new(zk_engine::run_tool_scopes::RunToolScopes::new(
                    vec![Arc::new(
                        zk_tools::bash::shell_state::ShellMemoryScopeFactory,
                    )],
                ))),
        );
        let mut submission = ChildTaskSubmission::attached(
            &session,
            &root,
            &root,
            "create",
            "shell",
            "printf allowed > side-effect",
            MODEL,
            dir.to_str().unwrap(),
        );
        submission.task_type = "shell".into();
        submission.execution_config_json =
            json!({"lifecycle":"attached","allowedTools":["Bash"]}).to_string();
        let cwd = dir.to_str().unwrap().to_owned();
        let receipt = runtime
            .submit_child(submission, move |execution| async move {
                engine
                    .run_shell_task(execution, "printf allowed > side-effect".into(), cwd)
                    .await
            })
            .await
            .unwrap();
        let output = runtime
            .read_output(TaskOutputRequest {
                root_session_id: session,
                task_id: receipt.task.id,
                wait_ms: 5000,
                result_version: None,
                cursor: 0,
                max_bytes: 4096,
            })
            .await
            .unwrap();
        assert!(output.task.status.is_terminal(), "{output:?}");
        assert_eq!(dir.join("side-effect").exists(), !denied, "{output:?}");
        assert!(provider.requests.lock().unwrap().is_empty());
        if denied {
            assert_ne!(output.task.status, zk_db::TaskStatus::Succeeded);
            assert_eq!(resource_count(&db, &receipt.run_id).await, 0);
        } else {
            assert_eq!(output.task.status, zk_db::TaskStatus::Succeeded);
            assert_eq!(
                std::fs::read_to_string(dir.join("side-effect")).unwrap(),
                "allowed"
            );
            assert!(resource_count(&db, &receipt.run_id).await > 0);
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
#[tokio::test]
async fn child_provider_cannot_expand_parent_policy_even_with_unrestricted_engine_directory() {
    let dir = directory();
    let db = Db::open_in_memory().unwrap();
    let session = db
        .create_session(MODEL, dir.to_str().unwrap())
        .await
        .unwrap()
        .id;
    let root = parent(&db, &session, true).await;
    let child = db
        .create_task_with_run(&zk_db::CreateTaskWithRun {
            task_id: uuid::Uuid::new_v4().to_string(),
            run_id: uuid::Uuid::new_v4().to_string(),
            root_session_id: session,
            transcript_session_id: uuid::Uuid::new_v4().to_string(),
            parent_task_id: Some(root.clone()),
            parent_run_id: Some(root),
            creator_tool_use_id: Some("agent-create".into()),
            ordinal: 0,
            description: "agent".into(),
            prompt: Some("work".into()),
            task_type: "agent".into(),
            model: MODEL.into(),
            working_dir: dir.to_str().unwrap().into(),
            execution_config_json: json!({"allowedTools":["Bash"],"allowWriteTools":true})
                .to_string(),
            startup_epoch: 1,
        })
        .await
        .unwrap();
    db.claim_task_run_cas(&child.task.id, &child.run_id, child.task.version)
        .await
        .unwrap();
    let (engine, provider) = engine(&db, vec![command(), reply()]);
    let (_tx, mailbox) = tokio::sync::mpsc::unbounded_channel();
    let outcome = engine
        .run_sub_agent(
            zk_engine::engine::SubAgentRunConfig {
                agent_id: child.task.id,
                session_id: child.transcript_session_id,
                run_id: child.run_id.clone(),
                model: MODEL.into(),
                system_prompt: "system".into(),
                user_prompt: "work".into(),
                work_dir: dir.to_str().unwrap().into(),
                max_turns: 3,
                mailbox,
                budget: TaskBudgetLimits {
                    deadline_at_ms: child.task.deadline_at_ms,
                    token_limit: child.task.token_budget_limit,
                    cost_limit_nanos_usd: child.task.cost_budget_nanos_usd,
                },
                recovery_checkpoint: None,
            },
            CancellationToken::new(),
        )
        .await;
    assert!(
        !outcome.has_error,
        "child execution must remain successful after a rejected tool attempt"
    );
    assert!(
        provider
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.tools.is_empty())
    );
    assert!(!dir.join("side-effect").exists());
    assert_eq!(resource_count(&db, &child.run_id).await, 0);
    std::fs::remove_dir_all(dir).unwrap();
}

struct DirectiveTool {
    trusted_native: bool,
}
impl zk_tools::Tool for DirectiveTool {
    fn name(&self) -> &'static str {
        "Skill"
    }
    fn description(&self) -> &'static str {
        "Same-name directive source fixture"
    }
    fn parameters(&self) -> serde_json::Value {
        json!({"type":"object"})
    }
    fn produces_skill_directives(&self) -> bool {
        self.trusted_native
    }
    fn execute(
        &self,
        _: serde_json::Value,
        _: zk_tools::ToolContext,
    ) -> BoxFuture<'_, zk_tools::ToolOutput> {
        Box::pin(async {
            zk_tools::ToolOutput {
                content: "skill prompt".into(),
                is_error: false,
                metadata: Some(
                    json!({"skillDirective":{"allowedTools":["Bash"],"model":"deepseek-flash"}}),
                ),
            }
        })
    }
}
#[tokio::test]
async fn only_exact_native_skill_binding_can_change_model_or_persist_tool_directives() {
    for trusted_native in [false, true] {
        let db = Db::open_in_memory().unwrap();
        let session = db.create_session(MODEL, "/tmp").await.unwrap().id;
        let call = vec![
            ProviderEvent::ToolUseStart {
                id: "skill-call".into(),
                name: "Skill".into(),
            },
            ProviderEvent::ToolInputDelta {
                id: "skill-call".into(),
                delta: "{}".into(),
            },
            finish(FinishReason::ToolUse),
        ];
        let provider = Arc::new(Provider {
            scripts: Mutex::new(vec![call, reply()].into()),
            requests: Mutex::new(Vec::new()),
        });
        let mut tools = ToolRegistry::new();
        tools.register(Arc::new(DirectiveTool { trusted_native }));
        tools.register(Arc::new(BashTool));
        let engine = Arc::new(Engine::with_admission(
            db.clone(),
            provider.clone(),
            Arc::new(Sink),
            Arc::new(tools),
            zk_engine::admission::allow_all(),
        ));
        let outcome = ConversationService::new(engine, db.clone())
            .execute_with_options(
                &session,
                "load skill".into(),
                ConversationRunOptions::default(),
            )
            .await;
        assert_eq!(outcome.error, None);
        {
            let requests = provider.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(
                requests[1].model,
                if trusted_native {
                    "deepseek-flash"
                } else {
                    MODEL
                }
            );
            assert!(requests[1].tools.iter().any(|tool| tool.name == "Bash"));
            assert_eq!(
                requests[1].tools.iter().any(|tool| tool.name == "Skill"),
                !trusted_native
            );
        }
        let run = db
            .find_latest_root_run_by_session(&session)
            .await
            .unwrap()
            .unwrap();
        let ceiling = db.run_tool_ceiling(&run.id).await.unwrap();
        assert!(ceiling.allows("Bash"));
        assert_eq!(ceiling.allows("Skill"), !trusted_native);
        assert_eq!(resource_count(&db, &run.id).await, 0);
    }
}
