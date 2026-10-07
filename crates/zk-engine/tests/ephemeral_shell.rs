//! Real Bash execution in root chat and attached Shell Tasks without disk transcripts.
use futures::{
    StreamExt,
    future::BoxFuture,
    stream::{self, BoxStream},
};
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use zk_db::{CasOutcome, CreateTaskWithRun, Db};
use zk_engine::{ChildTaskSubmission, Engine, MessageSink, TaskOutputRequest, TaskRuntime};
use zk_llm::{ChatProvider, ChatRequest, FinishReason, ProviderError, ProviderEvent};
use zk_protocol::{ServerMessage, model::Usage};
use zk_tools::{
    BashTool, ToolRegistry,
    bash::shell_state::{ShellMemoryScopeFactory, ShellStateManager},
};

struct Sink;
impl MessageSink for Sink {
    fn push<'a>(&'a self, _: &'a str, _: ServerMessage) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}
struct Script(Mutex<VecDeque<Vec<ProviderEvent>>>);
impl ChatProvider for Script {
    fn provider_name(&self) -> &'static str {
        "ephemeral-shell-test"
    }
    fn chat_stream(
        &self,
        _: ChatRequest,
        _: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        Ok(stream::iter(
            self.0
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected model request"),
        )
        .boxed())
    }
}
fn call(id: &str, command: &str) -> Vec<ProviderEvent> {
    vec![
        ProviderEvent::ToolUseStart {
            id: id.into(),
            name: "Bash".into(),
        },
        ProviderEvent::ToolInputDelta {
            id: id.into(),
            delta: json!({"command":command}).to_string(),
        },
        ProviderEvent::Finish {
            finish_reason: FinishReason::ToolUse,
            usage: Some(Usage {
                input_tokens: 1,
                output_tokens: 1,
                ..Usage::default()
            }),
        },
    ]
}
fn engine(db: &Db, scripts: Vec<Vec<ProviderEvent>>) -> Arc<Engine> {
    let mut tools = ToolRegistry::new();
    tools.register(Arc::new(BashTool));
    Arc::new(
        Engine::with_admission(
            db.clone(),
            Arc::new(Script(Mutex::new(scripts.into()))),
            Arc::new(Sink),
            Arc::new(tools),
            zk_engine::admission::allow_all(),
        )
        .with_run_tool_scopes(Arc::new(zk_engine::run_tool_scopes::RunToolScopes::new(
            vec![Arc::new(ShellMemoryScopeFactory)],
        ))),
    )
}
fn root() -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!("zk-temporary-shell-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join("sub")).unwrap();
    root.canonicalize().unwrap()
}
fn no_shell_files(session: &str) {
    assert!(!ShellStateManager::cwd_tracking_path(session).exists());
    if let Ok(entries) = std::fs::read_dir(ShellStateManager::state_directory()) {
        assert!(
            !entries
                .flatten()
                .any(|entry| entry.file_name().to_string_lossy().starts_with(session))
        );
    }
}

#[tokio::test]
async fn temporary_root_chat_runs_two_real_commands_and_keeps_cwd_only_in_live_scope() {
    let root = root();
    let db = Db::open_in_memory().unwrap();
    let (session, lease) = db
        .create_ephemeral_session("qwen3.8-max-0902", root.to_str().unwrap(), "DONT_ASK")
        .await
        .unwrap();
    let engine = engine(
        &db,
        vec![
            call("first", "cd sub; printf first"),
            call("second", "pwd -P; printf kept > artifact.txt"),
            vec![
                ProviderEvent::TextDelta {
                    text: "finished".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(Usage {
                        input_tokens: 1,
                        output_tokens: 1,
                        ..Usage::default()
                    }),
                },
            ],
        ],
    );
    tokio::time::timeout(
        Duration::from_secs(10),
        engine.run_user_message(session.clone(), "perform the two fixture commands".into()),
    )
    .await
    .unwrap();
    let messages = db.list_messages(&session, None, 100).await.unwrap();
    let body = serde_json::to_string(&messages).unwrap();
    assert!(body.contains("finished"), "{body}");
    assert!(body.contains(root.join("sub").to_str().unwrap()), "{body}");
    assert_eq!(
        std::fs::read_to_string(root.join("sub/artifact.txt")).unwrap(),
        "kept"
    );
    no_shell_files(&session);
    assert_eq!(
        ShellStateManager::resolve_working_directory(&session, root.to_str().unwrap()),
        root.to_str().unwrap(),
        "scope cleanup discards child cwd"
    );
    drop(lease);
    assert!(db.list_messages(&session, None, 100).await.is_err());
    assert!(root.join("sub/artifact.txt").is_file());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn attached_temporary_shell_task_uses_owned_scope_and_never_calls_a_model() {
    let root = root();
    let db = Db::open_in_memory().unwrap();
    let (session, lease) = db
        .create_ephemeral_session("qwen3.8-max-0902", root.to_str().unwrap(), "DONT_ASK")
        .await
        .unwrap();
    let parent_run = uuid::Uuid::new_v4().to_string();
    let parent = db.create_task_with_run(&CreateTaskWithRun {
        task_id: uuid::Uuid::new_v4().to_string(), run_id: parent_run.clone(), root_session_id: session.clone(), transcript_session_id: session.clone(),
        parent_task_id: None, parent_run_id: None, creator_tool_use_id: None, ordinal: 0,
        description: "root".into(), prompt: Some("private root".into()), task_type: "agent".into(), model: "qwen3.8-max-0902".into(),
        working_dir: root.to_str().unwrap().into(), execution_config_json: json!({"budget":{"tokenLimit":1_000_000,"costLimitNanosUsd":1_000_000_000_000_i64,"deadlineAtMs":zk_db::time::now_millis()+60_000}}).to_string(), startup_epoch: 1,
    }).await.unwrap();
    assert_eq!(
        db.claim_task_run_cas(&parent.task.id, &parent_run, parent.task.version)
            .await
            .unwrap(),
        CasOutcome::Applied
    );
    let runtime = TaskRuntime::new(db.clone(), Arc::new(Sink));
    let engine = engine(&db, vec![]);
    let command = "cd sub; printf child-kept > child.txt; printf shell-child-ok";
    let mut request = ChildTaskSubmission::attached(
        &session,
        &parent.task.id,
        &parent_run,
        "shell-create",
        "temporary shell",
        command,
        "qwen3.8-max-0902",
        root.to_str().unwrap(),
    );
    request.task_type = "shell".into();
    let cwd = root.to_str().unwrap().to_owned();
    let receipt = runtime
        .submit_child(request, move |execution| async move {
            engine.run_shell_task(execution, command.into(), cwd).await
        })
        .await
        .unwrap();
    let output = runtime
        .read_output(TaskOutputRequest {
            root_session_id: session.clone(),
            task_id: receipt.task.id,
            wait_ms: 5000,
            result_version: None,
            cursor: 0,
            max_bytes: 4096,
        })
        .await
        .unwrap();
    assert_eq!(
        output.task.status,
        zk_db::TaskStatus::Succeeded,
        "{output:?}"
    );
    assert!(output.result.unwrap().content.contains("shell-child-ok"));
    assert_eq!(
        db.session_retention(&receipt.transcript_session_id)
            .await
            .unwrap(),
        zk_db::content::ContentRetention::Ephemeral
    );
    no_shell_files(&session);
    no_shell_files(&receipt.transcript_session_id);
    assert_eq!(
        std::fs::read_to_string(root.join("sub/child.txt")).unwrap(),
        "child-kept"
    );
    drop(lease);
    assert!(
        db.list_messages(&receipt.transcript_session_id, None, 100)
            .await
            .is_err()
    );
    assert!(root.join("sub/child.txt").is_file());
    std::fs::remove_dir_all(root).unwrap();
}
