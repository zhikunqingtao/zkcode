//! Real host registry + `TaskRuntime` + Bash, with no model or external network.
use super::*;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use zk_db::{CasOutcome, CreateTaskWithRun};
use zk_tools::ToolContext;

async fn fixture(
    ephemeral: bool,
) -> (
    AppState,
    std::path::PathBuf,
    String,
    String,
    Option<zk_db::content::EphemeralContentLease>,
) {
    let root = std::env::temp_dir().join(format!("zk-background-bash-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let root = root.canonicalize().unwrap();
    let mut config = crate::config::Config::test_config();
    config.workspace_default_root = root.to_string_lossy().into_owned();
    assert!(!config.agent_enabled && !config.cron_enabled);
    let state = AppState::new(zk_db::Db::open_in_memory().unwrap(), config);
    state.set_startup_epoch(1).unwrap();
    state
        .db
        .create_project("background fixture", root.to_str().unwrap())
        .await
        .unwrap();
    let (session, lease) = if ephemeral {
        let (id, lease) = state
            .db
            .create_ephemeral_session("qwen3.8-max-0902", root.to_str().unwrap(), "AUTO_APPROVE")
            .await
            .unwrap();
        (id, Some(lease))
    } else {
        let id = uuid::Uuid::new_v4().to_string();
        state
            .db
            .create_session_with_permission(
                &id,
                "qwen3.8-max-0902",
                root.to_str().unwrap(),
                Some("AUTO_APPROVE"),
            )
            .await
            .unwrap();
        (id, None)
    };
    let run = uuid::Uuid::new_v4().to_string();
    let parent=state.db.create_task_with_run(&CreateTaskWithRun {task_id:uuid::Uuid::new_v4().to_string(),run_id:run.clone(),root_session_id:session.clone(),transcript_session_id:session.clone(),parent_task_id:None,parent_run_id:None,creator_tool_use_id:None,ordinal:0,description:"root".into(),prompt:Some("root".into()),task_type:"agent".into(),model:"qwen3.8-max-0902".into(),working_dir:root.to_string_lossy().into_owned(),execution_config_json:json!({"budget":{"tokenLimit":1_000_000,"costLimitNanosUsd":1_000_000_000_000_i64,"deadlineAtMs":zk_db::time::now_millis()+60_000}}).to_string(),startup_epoch:1}).await.unwrap();
    assert_eq!(
        state
            .db
            .claim_task_run_cas(&parent.task.id, &run, parent.task.version)
            .await
            .unwrap(),
        CasOutcome::Applied
    );
    (state, root, session, run, lease)
}
fn context(root: &std::path::Path, session: &str, run: &str, id: &str) -> ToolContext {
    let (tx, _) = tokio::sync::mpsc::unbounded_channel();
    ToolContext::new(CancellationToken::new(), tx)
        .with_session_id(session)
        .with_run_id(run)
        .with_tool_use_id(id)
        .with_working_dir(root)
}

#[tokio::test]
async fn ordinary_and_temporary_background_commands_are_real_owned_queryable_tasks_without_agent_flags()
 {
    for ephemeral in [false, true] {
        let (state, root, session, run, _lease) = fixture(ephemeral).await;
        let tools = build_tool_registry(&state);
        assert!(
            tools.get("Agent").is_none()
                && tools.get("TaskCreate").is_none()
                && tools.get("CronCreate").is_none()
        );
        let ctx = context(&root, &session, &run, "launch").with_ephemeral_content(ephemeral);
        let input = json!({"command":"printf owned > artifact.txt; printf background-output","is_background":true,"timeout":5000});
        let started = tools
            .get("Bash")
            .unwrap()
            .execute(input.clone(), ctx.clone())
            .await;
        assert!(!started.is_error, "{}", started.content);
        let task = started.metadata.unwrap()["structuredResult"]["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        let duplicate = tools.get("Bash").unwrap().execute(input, ctx.clone()).await;
        assert!(!duplicate.is_error, "{}", duplicate.content);
        assert_eq!(
            duplicate.metadata.unwrap()["structuredResult"]["taskId"],
            task
        );
        let output = tools
            .get("TaskOutput")
            .unwrap()
            .execute(json!({"taskId":task,"waitMs":5000}), ctx)
            .await;
        assert!(!output.is_error, "{}", output.content);
        let meta = output.metadata.unwrap()["structuredResult"].clone();
        assert_eq!(meta["status"], "succeeded", "{meta}");
        assert_eq!(meta["lifecycle"], "attached");
        assert_eq!(meta["taskType"], "shell");
        assert!(
            meta["content"]
                .as_str()
                .unwrap()
                .contains("background-output")
        );
        assert_eq!(
            std::fs::read_to_string(root.join("artifact.txt")).unwrap(),
            "owned"
        );
        let child_run = meta["runId"].as_str().unwrap();
        let child = state.db.find_run_by_id(child_run).await.unwrap().unwrap();
        assert_eq!(child.parent_run_id.as_deref(), Some(run.as_str()));
        if ephemeral {
            assert!(
                !zk_tools::bash::shell_state::ShellStateManager::cwd_tracking_path(
                    &child.session_id
                )
                .exists()
            );
        }
        let foreign = tools
            .get("TaskGet")
            .unwrap()
            .execute(
                json!({"taskId":task}),
                context(&root, "foreign", &run, "get"),
            )
            .await;
        assert!(foreign.is_error, "cross-session task must be inaccessible");
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn stopping_background_shell_terminates_its_owned_process_before_terminal_receipt() {
    let (state, root, session, run, _lease) = fixture(false).await;
    let tools = build_tool_registry(&state);
    let ctx = context(&root, &session, &run, "launch-stop");
    let started=tools.get("Bash").unwrap().execute(json!({"command":"printf started > started; sleep 60","is_background":true,"timeout":60000}),ctx.clone()).await;
    assert!(!started.is_error, "{}", started.content);
    let task = started.metadata.unwrap()["structuredResult"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !root.join("started").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let stop = tools
        .get("TaskStop")
        .unwrap()
        .execute(json!({"taskId":task}), ctx.clone())
        .await;
    assert!(!stop.is_error, "{}", stop.content);
    let output = tools
        .get("TaskOutput")
        .unwrap()
        .execute(json!({"taskId":task,"waitMs":20000}), ctx)
        .await;
    let meta = output.metadata.unwrap()["structuredResult"].clone();
    assert_eq!(meta["status"], "cancelled", "{meta}");
    assert_eq!(meta["cleanupStatus"], "confirmed", "{meta}");
    let child_run = meta["runId"].as_str().unwrap().to_owned();
    let (owned,unreleased):(i64,i64)=state.db.with_reader(move|conn|Ok(conn.query_row(
        "SELECT COUNT(*),COALESCE(SUM(status!='released'),0) FROM execution_resources WHERE run_id=?1 AND resource_kind='processGroup'",
        [child_run],|row|Ok((row.get(0)?,row.get(1)?))
    )?)).await.unwrap();
    assert!(
        owned > 0,
        "the stop fixture must have started a real owned process"
    );
    assert_eq!(
        unreleased, 0,
        "terminal receipt requires physical release confirmation"
    );
    std::fs::remove_dir_all(root).unwrap();
}

struct ArtifactScript(std::sync::Mutex<std::collections::VecDeque<Vec<zk_llm::ProviderEvent>>>);
impl zk_llm::ChatProvider for ArtifactScript {
    fn provider_name(&self) -> &'static str {
        "artifact-fixture"
    }
    fn chat_stream(
        &self,
        _: zk_llm::ChatRequest,
        _: CancellationToken,
    ) -> Result<futures::stream::BoxStream<'static, zk_llm::ProviderEvent>, zk_llm::ProviderError>
    {
        use futures::StreamExt;
        Ok(futures::stream::iter(
            self.0
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request"),
        )
        .boxed())
    }
}
struct ArtifactSink;
impl zk_engine::MessageSink for ArtifactSink {
    fn push<'a>(
        &'a self,
        _: &'a str,
        _: zk_protocol::ServerMessage,
    ) -> futures::future::BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "End-to-end declared-output authorization, shell execution, terminal observation and immutable-result assertions share one owner"
)]
async fn declared_bash_outputs_reach_the_production_terminal_integrity_observer() {
    use zk_llm::{FinishReason, ProviderEvent};
    let (state, root, _, _, _) = fixture(false).await;
    let session = state
        .db
        .create_session("qwen3.8-max-0902", root.to_str().unwrap())
        .await
        .unwrap();
    let input = json!({"command":"printf artifact-fact > declared.txt; printf incidental > incidental.txt","declared_outputs":[{"path":"declared.txt","operation":"created"}]});
    let usage = || {
        Some(zk_protocol::Usage {
            input_tokens: 1,
            output_tokens: 1,
            ..Default::default()
        })
    };
    let script = ArtifactScript(std::sync::Mutex::new(
        vec![
            vec![
                ProviderEvent::ToolUseStart {
                    id: "declared-bash".into(),
                    name: "Bash".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "declared-bash".into(),
                    delta: input.to_string(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::ToolUse,
                    usage: usage(),
                },
            ],
            vec![
                ProviderEvent::TextDelta {
                    text: "done".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: usage(),
                },
            ],
        ]
        .into(),
    ));
    let engine = Arc::new(
        zk_engine::Engine::with_admission(
            state.db.clone(),
            Arc::new(script),
            Arc::new(ArtifactSink),
            Arc::new(build_tool_registry(&state)),
            zk_engine::admission::allow_all(),
        )
        .with_task_runtime(Arc::clone(&state.task_runtime))
        .with_run_tool_scopes(Arc::clone(&state.run_tool_scopes)),
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        engine.run_user_message(session.id.clone(), "Create declared output".into()),
    )
    .await
    .unwrap();
    let run = state
        .db
        .find_latest_root_run_by_session(&session.id)
        .await
        .unwrap()
        .unwrap();
    let receipt = state
        .db
        .artifact_terminal_check(&run.id)
        .await
        .unwrap()
        .expect("production terminal observer must run");
    assert_eq!(receipt.status, "verified");
    let manifest = state
        .db
        .find_artifact_manifest_by_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.entries.len(), 1);
    let entry = &manifest.entries[0];
    let expected = zk_tools::atomic::sha256_hex(b"artifact-fact");
    assert_eq!(entry.sealed_hash.as_deref(), Some(expected.as_str()));
    assert_eq!(entry.actual_hash.as_deref(), Some(expected.as_str()));
    assert_eq!(
        entry.canonical_path,
        root.join("declared.txt").to_string_lossy()
    );
    assert!(root.join("incidental.txt").exists());
    crate::artifact_integrity::ArtifactIntegrityObserver(state.db.clone())
        .check(&run.id)
        .await
        .unwrap();
    assert_eq!(
        state
            .db
            .find_artifact_manifest_by_run(&run.id)
            .await
            .unwrap()
            .unwrap(),
        manifest
    );
    std::fs::remove_dir_all(root).unwrap();
}
