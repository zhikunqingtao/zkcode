//! Lifecycle Hooks share their real Root, total deadline and process cleanup authority.
#[path = "support/hook_admission.rs"]
mod hook_admission_fixture;
use futures::{
    StreamExt,
    future::BoxFuture,
    stream::{self, BoxStream},
};
use hook_admission_fixture::FixtureHookAdmission;
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use zk_db::{Db, MemoryTarget, MemoryUpsert};
use zk_engine::{
    ConversationRunOptions, ConversationService, Engine, ExecutionSupervisor, MessageSink,
    RootTaskBudgetPolicy,
    auxiliary_query::AuxiliaryQuery,
    hook::{HookRegistry, HookService},
    memory_retrieval::MemoryRetriever,
};
use zk_llm::{
    ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent, ProviderRegistry,
};
use zk_protocol::{ServerMessage, Usage};

const MODEL: &str = "qwen3.8-max-0902";
#[derive(Default)]
struct Provider(Mutex<Vec<String>>);
impl ChatProvider for Provider {
    fn provider_name(&self) -> &'static str {
        "hook-fixture"
    }
    fn chat_stream(
        &self,
        request: ChatRequest,
        _: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        let purpose = request
            .execution
            .as_ref()
            .map_or("unattributed", |value| value.kind.as_str());
        self.0.lock().unwrap().push(purpose.into());
        let text = if purpose == "memory_rerank" {
            let input: serde_json::Value =
                serde_json::from_str(&request.messages.last().unwrap().content).unwrap();
            json!({"revision":input["revision"],"ids":input["candidates"].as_array().unwrap().iter().map(|entry|entry["id"].clone()).collect::<Vec<_>>()}).to_string()
        } else {
            "actual answer".into()
        };
        Ok(stream::iter([
            ProviderEvent::TextDelta { text },
            ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(Usage {
                    input_tokens: 20,
                    output_tokens: 5,
                    ..Usage::default()
                }),
            },
        ])
        .boxed())
    }
}
struct Sink;
impl MessageSink for Sink {
    fn push<'a>(&'a self, _: &'a str, _: ServerMessage) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}
struct Fixture {
    root: PathBuf,
    db: Db,
    service: Arc<ConversationService>,
    provider: Arc<Provider>,
    session: String,
    admission: Arc<FixtureHookAdmission>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("zk-hook-owner-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join(".zk")).unwrap();
        let root = root.canonicalize().unwrap();
        let db = Db::open(root.join("runtime.sqlite")).unwrap();
        let session = db
            .create_session(MODEL, root.to_str().unwrap())
            .await
            .unwrap()
            .id;
        for id in ["one", "two"] {
            db.create_memory(
                MemoryTarget::project(root.to_string_lossy()).unwrap(),
                MemoryUpsert {
                    id: Some(id.into()),
                    category: "PROJECT".into(),
                    title: "alpha migration".into(),
                    content: "alpha migration relevant memory".into(),
                    keywords: None,
                    source: None,
                },
            )
            .await
            .unwrap();
        }
        let provider = Arc::new(Provider::default());
        let mut registry = ProviderRegistry::new();
        registry.register("hook-fixture", provider.clone(), vec![MODEL.into()]);
        let registry = Arc::new(registry);
        let supervisor = ExecutionSupervisor::new(db.clone());
        let hooks = HookRegistry::new();
        let admission = Arc::new(FixtureHookAdmission::for_registry(&hooks));
        let engine = Arc::new(
            Engine::new(db.clone(), registry.clone(), Arc::new(Sink))
                .with_execution_supervisor(&supervisor)
                .with_root_task_budget_policy(RootTaskBudgetPolicy {
                    deadline: Duration::from_secs(10),
                    ..RootTaskBudgetPolicy::default()
                })
                .with_memory_retriever(MemoryRetriever::with_reranker(Arc::new(
                    AuxiliaryQuery::new(registry, MODEL.into()),
                )))
                .with_hooks(Arc::new(
                    HookService::new(hooks).with_admission(admission.clone()),
                )),
        );
        Self {
            root,
            db: db.clone(),
            service: Arc::new(ConversationService::new(engine, db)),
            provider,
            session,
            admission,
        }
    }
    fn hook(&self, event: &str, command: &str, async_mode: bool, security: bool) {
        std::fs::write(self.root.join(".zk/hooks.toml"),format!("[[hook]]\nname = \"lifecycle\"\nevent = {event:?}\ncommand = {}\nasync = {async_mode}\nrole = {:?}\ntimeout_secs = 30\n",serde_json::to_string(command).unwrap(),if security {"security"} else {"notification"})).unwrap();
        self.admission
            .approve_registry(&HookRegistry::try_load_from_dir(&self.root).unwrap());
    }
    async fn wait_file(&self, name: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.root.join(name).exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn assert_released(&self, run: &str) {
        let run = run.to_owned();
        let rows:Vec<(String,Option<String>,String)>=self.db.with_reader(move|conn|{ let mut query=conn.prepare("SELECT resource_kind,external_id,status FROM execution_resources WHERE run_id=?1")?; Ok(query.query_map([run],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?.collect::<Result<Vec<_>,_>>()?) }).await.unwrap();
        assert!(!rows.is_empty(), "real Hooks require resource ownership");
        for (kind, pid, status) in rows {
            assert_eq!(status, "released");
            if kind == "processGroup" {
                let pid = pid.unwrap();
                assert!(
                    !std::process::Command::new("/bin/kill")
                        .args(["-0", &format!("-{pid}")])
                        .stderr(std::process::Stdio::null())
                        .status()
                        .unwrap()
                        .success(),
                    "Hook process group {pid} remains"
                );
            }
        }
    }
}

#[tokio::test]
async fn first_root_defers_session_start_but_resume_does_not_repeat_it() {
    let f = Fixture::new().await;
    f.hook("SESSION_START", "printf s >> starts", false, false);
    assert!(
        !f.root.join("starts").exists(),
        "creating a session cannot execute a process"
    );
    for _ in 0..2 {
        let result = f
            .service
            .execute(&f.session, "alpha migration".into())
            .await;
        assert_eq!(result.error, None, "{result:?}");
    }
    assert_eq!(std::fs::read_to_string(f.root.join("starts")).unwrap(), "s");
    let calls = f.provider.0.lock().unwrap().clone();
    assert_eq!(
        calls
            .iter()
            .filter(|value| value.as_str() == "memory_rerank")
            .count(),
        2,
        "fixture must exercise enabled semantic memory"
    );
    assert_eq!(
        calls
            .iter()
            .filter(|value| value.as_str() == "conversation")
            .count(),
        2
    );
    let fork =
        f.db.fork_session(
            "hook-fork",
            zk_db::SessionForkRequest {
                source_session_id: f.session.clone(),
                title: None,
            },
        )
        .await
        .unwrap();
    let result = f
        .service
        .execute(&fork.session_id, "alpha migration".into())
        .await;
    assert_eq!(result.error, None, "{result:?}");
    assert_eq!(
        std::fs::read_to_string(f.root.join("starts")).unwrap(),
        "ss",
        "a fork owns its first SessionStart independently"
    );
}

#[tokio::test]
async fn cancelling_blocking_session_start_stops_owned_group_before_any_model_call() {
    let f = Fixture::new().await;
    f.hook(
        "SESSION_START",
        "printf '%s' \"$$\" > entered; sleep 30; printf late > late",
        false,
        false,
    );
    let lease = f.service.reserve(&f.session).unwrap();
    let cancellation = f.service.cancellation(&lease);
    let service = f.service.clone();
    let execution = tokio::spawn(async move {
        service
            .execute_reserved(
                lease,
                "alpha migration".into(),
                ConversationRunOptions::default(),
            )
            .await
    });
    f.wait_file("entered").await;
    cancellation.cancel("USER_INTERRUPT");
    let outcome = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        outcome.error.as_deref(),
        Some("USER_CANCELLED"),
        "{outcome:?}"
    );
    assert!(
        f.provider.0.lock().unwrap().is_empty(),
        "both main and optional memory requests must remain absent"
    );
    assert!(!f.root.join("late").exists());
    f.assert_released(outcome.run_id.as_deref().unwrap()).await;
}

#[tokio::test]
async fn absolute_deadline_is_not_restarted_by_session_preparation_or_hook() {
    let f = Fixture::new().await;
    f.hook(
        "SESSION_START",
        "printf '%s' \"$$\" > entered; sleep 30; printf late > late",
        false,
        false,
    );
    let options = ConversationRunOptions {
        deadline: Some(Duration::from_secs(10)),
        deadline_at_ms: Some(zk_db::time::now_millis() + 1_200),
        notify_session_start: true,
        ..ConversationRunOptions::default()
    };
    tokio::time::sleep(Duration::from_millis(700)).await;
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(4),
        f.service
            .execute_with_options(&f.session, "alpha migration".into(), options),
    )
    .await
    .unwrap();
    assert_eq!(outcome.error.as_deref(), Some("TIMEOUT"), "{outcome:?}");
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(f.root.join("entered").exists());
    assert!(!f.root.join("late").exists());
    assert!(f.provider.0.lock().unwrap().is_empty());
    f.assert_released(outcome.run_id.as_deref().unwrap()).await;
}

#[tokio::test]
async fn asynchronous_end_notifications_drain_before_terminal() {
    for event in ["RUN_END", "MESSAGE_SENT"] {
        let f = Fixture::new().await;
        f.hook(event,"printf '%s' \"$$\" > ending; while [ ! -f release ]; do sleep 0.02; done; printf done > ended",true,false);
        let service = f.service.clone();
        let session = f.session.clone();
        let execution =
            tokio::spawn(async move { service.execute(&session, "alpha migration".into()).await });
        f.wait_file("ending").await;
        let run =
            f.db.find_latest_root_run_by_session(&f.session)
                .await
                .unwrap()
                .unwrap();
        assert!(run.finished_at.is_none());
        assert!(!execution.is_finished());
        std::fs::write(f.root.join("release"), "").unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(5), execution)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.error, None, "{outcome:?}");
        assert_eq!(
            std::fs::read_to_string(f.root.join("ended")).unwrap(),
            "done"
        );
        assert!(
            f.db.find_run_by_id(&run.id)
                .await
                .unwrap()
                .unwrap()
                .finished_at
                .is_some()
        );
        f.assert_released(&run.id).await;
    }
}
#[tokio::test]
async fn security_session_start_denial_prevents_optional_memory_and_main_requests() {
    let f = Fixture::new().await;
    f.hook("SESSION_START", "exit 7", false, true);
    let outcome = f
        .service
        .execute(&f.session, "alpha migration".into())
        .await;
    assert!(outcome.error.is_some(), "{outcome:?}");
    assert!(f.provider.0.lock().unwrap().is_empty());
    f.assert_released(outcome.run_id.as_deref().unwrap()).await;
}

#[tokio::test]
async fn security_run_end_failure_is_truthful_and_keeps_persisted_answer() {
    let f = Fixture::new().await;
    f.hook("RUN_END", "exit 7", false, true);
    let outcome = f
        .service
        .execute(&f.session, "alpha migration".into())
        .await;
    assert!(outcome.error.is_some(), "{outcome:?}");
    let messages =
        f.db.list_messages(&f.session, None, 100)
            .await
            .unwrap()
            .unwrap()
            .messages;
    assert!(
        messages
            .iter()
            .any(|message| message.content.iter().any(|block| {
                matches!(block, zk_db::StoredBlock::Text { text } if text == "actual answer")
            })),
        "end Hook failure must not rewrite already persisted model output"
    );
    f.assert_released(outcome.run_id.as_deref().unwrap()).await;
}
