//! Admission binds the executing Run's shell identity, including internal children.
use serde_json::json;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use zk_engine::{Admission, AdmissionRequest, ToolAdmission};
use zk_server::{authz::EngineAdmission, state::AppState};
use zk_tools::{
    BashTool, RunToolScopeFactory, Tool, ToolContext, ToolRegistry,
    bash::shell_state::ShellMemoryScopeFactory,
};

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "One real child execution checks admitted cwd identity, mutation rejection and unchanged workspace effects"
)]
async fn child_bash_admission_freezes_its_own_cwd_and_rejects_a_changed_identity() {
    let root = std::env::temp_dir().join(format!("zk-cwd-binding-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join("child")).unwrap();
    let root = root.canonicalize().unwrap();
    let state = AppState::for_tests();
    state
        .db
        .create_project("shell binding", root.to_str().unwrap())
        .await
        .unwrap();
    let (session, _lease) = state
        .db
        .create_ephemeral_session("qwen3.8-max-0902", root.to_str().unwrap(), "AUTO_APPROVE")
        .await
        .unwrap();
    let root_run = uuid::Uuid::new_v4().to_string();
    state
        .db
        .start_run(&root_run, &session, None, None, "qwen3.8-max-0902")
        .await
        .unwrap();
    let root_task = state
        .db
        .find_run_by_id(&root_run)
        .await
        .unwrap()
        .unwrap()
        .task_id;
    assert_eq!(
        state
            .db
            .configure_root_task_budget_cas(
                &root_task,
                0,
                &zk_db::TaskBudgetLimits {
                    token_limit: Some(10_000),
                    cost_limit_nanos_usd: Some(1_000_000_000),
                    deadline_at_ms: Some(zk_db::time::now_millis() + 60_000),
                }
            )
            .await
            .unwrap(),
        zk_db::CasOutcome::Applied
    );
    let child = uuid::Uuid::new_v4().to_string();
    let child_run = uuid::Uuid::new_v4().to_string();
    let created = state.db.create_task_with_run(&zk_db::CreateTaskWithRun {
        task_id: uuid::Uuid::new_v4().to_string(), run_id:child_run.clone(), root_session_id:session.clone(), transcript_session_id:child.clone(),
        parent_task_id:Some(root_task),parent_run_id:Some(root_run),creator_tool_use_id:Some("create-child".into()),ordinal:0,
        description:"child".into(),prompt:Some("fixture".into()),task_type:"shell".into(),model:"qwen3.8-max-0902".into(),working_dir:root.to_string_lossy().into_owned(),
        execution_config_json:json!({"lifecycle":"attached","isolation":"sharedWorkspace","allowedTools":["Bash"]}).to_string(),startup_epoch:1,
    }).await.unwrap();
    assert_eq!(
        state
            .db
            .claim_task_run_cas(&created.task.id, &child_run, created.task.version)
            .await
            .unwrap(),
        zk_db::CasOutcome::Applied
    );
    let (tx, _) = tokio::sync::mpsc::unbounded_channel();
    let context = ToolContext::new(CancellationToken::new(), tx)
        .with_session_id(&child)
        .with_run_id(&child_run)
        .with_working_dir(root.join("child"))
        .with_ephemeral_content(true);
    let mut directory = ToolRegistry::new();
    directory.register(Arc::new(BashTool));
    let directory = Arc::new(directory);
    let scope = ShellMemoryScopeFactory
        .prepare(context.clone(), directory.clone())
        .await
        .unwrap();
    // This fixture explicitly approves execution to isolate cwd identity checks.
    // Non-interactive DONT_ASK rejects guarded shell calls before this assertion.
    let admission = EngineAdmission::new(state.authz.clone(), directory);
    let input = json!({"command":"pwd -P", "authorized_shell_cwd":root});
    let admitted = admission
        .admit(AdmissionRequest {
            session_id: &session,
            run_id: &child_run,
            tool_use_id: "cwd-check",
            tool_name: "Bash",
            input: &input,
            working_directory: root.to_str(),
        })
        .await;
    let Admission::AllowWithShellCwd {
        execution_input,
        authorized_shell_cwd,
    } = admitted
    else {
        panic!("{admitted:?}");
    };
    assert_eq!(
        authorized_shell_cwd,
        root.join("child"),
        "authorization must use the child's RAM cwd, not root or client JSON"
    );
    zk_tools::bash::shell_state::ShellStateManager::reset_cwd(&child, root.to_str().unwrap());
    let denied = BashTool
        .execute(
            execution_input,
            context.with_authorized_shell_cwd(authorized_shell_cwd),
        )
        .await;
    assert!(
        denied.is_error && denied.content.contains("BASH_WORKING_DIRECTORY_CHANGED"),
        "{}",
        denied.content
    );
    scope.cleanup().await.unwrap();
    assert!(!zk_tools::bash::shell_state::ShellStateManager::cwd_tracking_path(&child).exists());
    std::fs::remove_dir_all(root).unwrap();
}
