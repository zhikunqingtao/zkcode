//! Real `DONT_ASK` admission and production `TaskUpdate` execution, including child ownership.
use crate::{
    authz::EngineAdmission, config::Config, engine_bridge::build_tool_registry, state::AppState,
};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zk_db::{CreateTaskWithRun, CreateTaskWithRunOutcome, Db};
use zk_engine::admission::{Admission, AdmissionRequest, ToolAdmission};
use zk_tools::ToolContext;

async fn task(
    db: &Db,
    session: &str,
    workspace: &str,
    parent: Option<&CreateTaskWithRunOutcome>,
    label: &str,
) -> CreateTaskWithRunOutcome {
    let result = db.create_task_with_run(&CreateTaskWithRun {
        task_id: uuid::Uuid::new_v4().to_string(), run_id: uuid::Uuid::new_v4().to_string(),
        root_session_id: session.into(), transcript_session_id: if parent.is_some() { uuid::Uuid::new_v4().to_string() } else { session.into() },
        parent_task_id: parent.map(|p|p.task.id.clone()), parent_run_id: parent.map(|p|p.run_id.clone()),
        creator_tool_use_id: parent.map(|_|label.to_owned()), ordinal: 0, description: label.into(), prompt: Some(label.into()),
        task_type: "agent".into(), model: "test-model".into(), working_dir: workspace.into(),
        execution_config_json: if parent.is_none() { json!({"budget":{"tokenLimit":1_000_000,"costLimitNanosUsd":1_000_000_000_i64,"deadlineAtMs":zk_db::time::now_millis()+60_000}}) } else { json!({"isolation":"readOnly"}) }.to_string(), startup_epoch:1,
    }).await.unwrap();
    assert_eq!(
        db.claim_task_run_cas(&result.task.id, &result.run_id, result.task.version)
            .await
            .unwrap(),
        zk_db::CasOutcome::Applied
    );
    result
}

#[cfg(test)]
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "One ownership scenario checks self, sibling and parent authority against the same live task tree."
)]
async fn dont_ask_admits_only_bound_child_output_and_backend_rechecks_the_same_identity() {
    let directory = std::env::temp_dir().join(format!("zk-task-display-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let directory = directory.canonicalize().unwrap();
    let workspace = directory.to_string_lossy();
    let mut config = Config::test_config();
    config.agent_enabled = true;
    let state = AppState::new(Db::open_in_memory().unwrap(), config);
    let session = state
        .db
        .create_session("test-model", &workspace)
        .await
        .unwrap();
    let root = task(&state.db, &session.id, &workspace, None, "root").await;
    let child = task(&state.db, &session.id, &workspace, Some(&root), "child").await;
    let sibling = task(&state.db, &session.id, &workspace, Some(&root), "sibling").await;
    let registry = Arc::new(build_tool_registry(&state));
    let admission = EngineAdmission::new_dont_ask(Arc::clone(&state.authz), Arc::clone(&registry));
    let child_registry =
        zk_engine::agent::executor::build_sub_agent_registry_with_policy(&registry, false);
    let tool = child_registry
        .get("TaskUpdate")
        .expect("self output must be in the effective catalog");
    assert!(tool.parameters()["properties"].get("description").is_none());
    for (ordinal, input) in [
        json!({"output":"current progress"}),
        json!({"taskId":child.task.id,"output":"own explicit progress"}),
    ]
    .into_iter()
    .enumerate()
    {
        let use_id = format!("display-allow-{ordinal}");
        let admitted = admission
            .admit(AdmissionRequest {
                session_id: &child.transcript_session_id,
                run_id: &child.run_id,
                tool_use_id: &use_id,
                tool_name: "TaskUpdate",
                input: &input,
                working_directory: Some(&workspace),
            })
            .await;
        let Admission::Allow { execution_input } = admitted else {
            panic!("own display note must not need interactive approval: {admitted:?}");
        };
        let (progress, _receiver) = mpsc::unbounded_channel();
        let result = tool
            .execute(
                execution_input,
                ToolContext::new(CancellationToken::new(), progress)
                    .with_session_id(&child.transcript_session_id)
                    .with_run_id(&child.run_id)
                    .with_tool_use_id(&use_id),
            )
            .await;
        assert!(!result.is_error, "{}", result.content);
        assert_eq!(
            state
                .db
                .find_runtime_task_by_id(&child.task.id)
                .await
                .unwrap()
                .unwrap()
                .display_output
                .as_deref(),
            input["output"].as_str()
        );
    }
    for (ordinal, input) in [
        json!({"output":"mixed", "description":"forged"}),
        json!({"output":"mixed", "plan":"{}"}),
        json!({"output":"mixed", "reportedProgress":1}),
        json!({"output":"mixed", "status":"succeeded"}),
        json!({"output":"forged sibling", "taskId":sibling.task.id}),
        json!({"output":"forged root", "taskId":root.task.id}),
    ]
    .into_iter()
    .enumerate()
    {
        let use_id = format!("display-deny-{ordinal}");
        let outcome = admission
            .admit(AdmissionRequest {
                session_id: &child.transcript_session_id,
                run_id: &child.run_id,
                tool_use_id: &use_id,
                tool_name: "TaskUpdate",
                input: &input,
                working_directory: Some(&workspace),
            })
            .await;
        assert!(
            matches!(outcome, Admission::Denied { .. }),
            "{input}: {outcome:?}"
        );
        let (progress, _receiver) = mpsc::unbounded_channel();
        assert!(
            tool.execute(
                input,
                ToolContext::new(CancellationToken::new(), progress)
                    .with_session_id(&child.transcript_session_id)
                    .with_run_id(&child.run_id)
            )
            .await
            .is_error,
            "backend must reject even a bypassed admission"
        );
    }
    let input = json!({"taskId":root.task.id,"output":"parent full control"});
    let parent = admission
        .admit(AdmissionRequest {
            session_id: &session.id,
            run_id: &root.run_id,
            tool_use_id: "parent-output",
            tool_name: "TaskUpdate",
            input: &input,
            working_directory: Some(&workspace),
        })
        .await;
    assert!(
        matches!(parent, Admission::Denied { .. }),
        "full parent TaskUpdate must keep CONTROL: {parent:?}"
    );
    for untouched in [&root.task.id, &sibling.task.id] {
        assert!(
            state
                .db
                .find_runtime_task_by_id(untouched)
                .await
                .unwrap()
                .unwrap()
                .display_output
                .is_none()
        );
    }
    let requests: i64 = state
        .db
        .with_reader(|conn| {
            Ok(
                conn.query_row("SELECT COUNT(*) FROM interaction_requests", [], |row| {
                    row.get(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(requests, 0, "DONT_ASK must never spawn an approval prompt");
    drop(state);
    std::fs::remove_dir_all(directory).unwrap();
}
