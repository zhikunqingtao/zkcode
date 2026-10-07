//! Production acceptance: scoped child authorization, real file tools, durable
//! usage/results, and retained worktree delivery without automatic Git commits.
use super::{AppState, ChatProvider};
use futures::{StreamExt, stream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use zk_llm::{ChatRequest, FinishReason, ProviderError, ProviderEvent, ProviderRegistry};
use zk_protocol::Usage;

struct WriteProvider {
    calls: AtomicUsize,
}
impl ChatProvider for WriteProvider {
    fn provider_name(&self) -> &'static str {
        "worktree-fixture"
    }
    fn chat_stream(
        &self,
        request: ChatRequest,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<futures::stream::BoxStream<'static, ProviderEvent>, ProviderError> {
        let ordinal = self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(request.tools.iter().any(|tool| tool.name == "Write"));
        assert!(!request.tools.iter().any(|tool| tool.name == "Agent"));
        let hints = request
            .messages
            .iter()
            .filter(|message| {
                message
                    .metadata
                    .as_ref()
                    .is_some_and(|meta| meta["runtimeRecoveryHint"] == true)
            })
            .count();
        assert_eq!(hints, usize::from(ordinal >= 3));
        let mut events = if ordinal < 4 {
            let (name, input) = if ordinal < 3 {
                ("Read", serde_json::json!({"file_path":"missing.txt"}))
            } else {
                (
                    "Write",
                    serde_json::json!({"file_path":"delivered.txt","content":"isolated delivery\n"}),
                )
            };
            vec![
                ProviderEvent::ToolUseStart {
                    id: format!("call-{ordinal}"),
                    name: name.into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: format!("call-{ordinal}"),
                    delta: input.to_string(),
                },
            ]
        } else {
            vec![ProviderEvent::TextDelta {
                text: "Created delivered.txt after recovering from missing input.".into(),
            }]
        };
        events.push(ProviderEvent::Finish {
            finish_reason: if ordinal < 4 {
                FinishReason::ToolUse
            } else {
                FinishReason::EndTurn
            },
            usage: Some(Usage {
                input_tokens: 10,
                output_tokens: 10,
                ..Usage::default()
            }),
        });
        Ok(stream::iter(events).boxed())
    }
}
fn git(root: &std::path::Path, args: &[&str]) -> String {
    let result = std::process::Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).unwrap().trim().to_owned()
}
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "One production acceptance checks scoped writes, durable accounting and the retained Git delivery together."
)]
async fn production_worktree_agent_writes_only_snapshot_and_retains_durable_delivery() {
    let root =
        std::env::temp_dir().join(format!("zk-production-worktree-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    git(&root, &["init", "-q"]);
    git(&root, &["config", "user.name", "Test"]);
    git(&root, &["config", "user.email", "test@example.invalid"]);
    git(&root, &["commit", "--allow-empty", "-qm", "base"]);
    let root = root.canonicalize().unwrap();
    let baseline = git(&root, &["rev-parse", "HEAD"]);
    let mut config = crate::config::Config::test_config();
    config.agent_enabled = true;
    config.agent_write_enabled = true;
    config.worktree_enabled = true;
    config.workspace_default_root = root.to_string_lossy().into_owned();
    let db = zk_db::Db::open_in_memory().unwrap();
    let state = AppState::new(db.clone(), config);
    let epoch = db.begin_runtime_startup_epoch().await.unwrap();
    state.set_startup_epoch(epoch).unwrap();
    let provider = Arc::new(WriteProvider {
        calls: AtomicUsize::new(0),
    });
    let mut providers = ProviderRegistry::new();
    providers.register("fixture", provider.clone(), vec!["qwen3.8-max-0902".into()]);
    state.providers.swap(providers);
    let tools = state.tools();
    let session = db
        .create_session("qwen3.8-max-0902", root.to_str().unwrap())
        .await
        .unwrap();
    state
        .authz
        .modes
        .set_mode(&session.id, zk_authz::model::PermissionMode::AcceptEdits)
        .await
        .unwrap();
    let run = uuid::Uuid::new_v4().to_string();
    db.start_root_run_with_budget_at_epoch(
        &run,
        &session.id,
        None,
        "qwen3.8-max-0902",
        &zk_db::TaskBudgetLimits {
            token_limit: Some(1_000_000),
            cost_limit_nanos_usd: Some(1_000_000_000_000),
            deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
        },
        epoch,
    )
    .await
    .unwrap();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let context = zk_tools::ToolContext::new(tokio_util::sync::CancellationToken::new(), tx)
        .with_session_id(session.id.clone())
        .with_run_id(run.clone())
        .with_tool_use_id("worktree-acceptance")
        .with_working_dir(&root)
        .with_tool_catalog(Arc::new(tools.specs()));
    let output=tokio::time::timeout(std::time::Duration::from_secs(40),tools.get("Agent").unwrap().execute(serde_json::json!({"prompt":"Create an isolated delivery, retain the worktree","isolation":"worktree","waitMode":"terminal"}),context)).await.unwrap();
    assert!(!output.is_error, "{} {:?}", output.content, output.metadata);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 5);
    assert!(!root.join("delivered.txt").exists());
    assert_eq!(git(&root, &["rev-parse", "HEAD"]), baseline);
    let record: String = db
        .with_reader(|conn| {
            conn.query_row("SELECT record_json FROM managed_worktrees", [], |row| {
                row.get(0)
            })
            .map_err(Into::into)
        })
        .await
        .unwrap();
    let record: serde_json::Value = serde_json::from_str(&record).unwrap();
    let path = std::path::Path::new(record["path"].as_str().unwrap());
    assert_eq!(
        std::fs::read_to_string(path.join("delivered.txt")).unwrap(),
        "isolated delivery\n"
    );
    assert_eq!(git(path, &["rev-parse", "HEAD"]), baseline);
    assert_eq!(record["worker_active"], false);
    let task = state
        .task_runtime
        .list_owned(&session.id, None)
        .await
        .unwrap()
        .into_iter()
        .find(|task| task.parent_task_id.is_some())
        .unwrap();
    assert_eq!(task.status, zk_db::TaskStatus::Succeeded);
    let result = db
        .read_task_result(&task.id, None, 0, zk_db::INLINE_RESULT_LIMIT)
        .await
        .unwrap()
        .unwrap();
    assert!(result.content.contains(path.to_str().unwrap()));
    let child_run = db
        .find_run_by_id(task.current_run_id.as_ref().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child_run.total_tokens, 100);
    assert!(child_run.usage_complete);
    assert_eq!(
        db.run_cleanup_status(&child_run.id).await.unwrap(),
        zk_db::CleanupStatus::Confirmed
    );
}
