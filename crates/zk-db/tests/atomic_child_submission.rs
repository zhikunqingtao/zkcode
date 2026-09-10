//! Transactional regression coverage for attached child submission.

use zk_db::{CreateTaskWithRun, Db, TaskStatus};

fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn root_request(session_id: &str) -> CreateTaskWithRun {
    CreateTaskWithRun {
        task_id: id(),
        run_id: id(),
        root_session_id: session_id.to_owned(),
        transcript_session_id: session_id.to_owned(),
        parent_task_id: None,
        parent_run_id: None,
        creator_tool_use_id: None,
        ordinal: 0,
        description: "root".to_owned(),
        prompt: Some("root".to_owned()),
        task_type: "agent".to_owned(),
        model: "script".to_owned(),
        working_dir: "/tmp/zkcode-atomic-submit".to_owned(),
        execution_config_json: serde_json::json!({
            "budget": {
                "tokenLimit": 1_000_000,
                "costLimitNanosUsd": 1_000_000_000_000_i64,
                "deadlineAtMs": zk_db::time::now_millis() + 60_000,
            }
        })
        .to_string(),
        startup_epoch: 1,
    }
}

fn child_request(
    session_id: &str,
    parent_task_id: &str,
    parent_run_id: &str,
    transcript_session_id: String,
) -> CreateTaskWithRun {
    CreateTaskWithRun {
        task_id: id(),
        run_id: id(),
        root_session_id: session_id.to_owned(),
        transcript_session_id,
        parent_task_id: Some(parent_task_id.to_owned()),
        parent_run_id: Some(parent_run_id.to_owned()),
        creator_tool_use_id: Some(id()),
        ordinal: 0,
        description: "child".to_owned(),
        prompt: Some("child".to_owned()),
        task_type: "agent".to_owned(),
        model: "script".to_owned(),
        working_dir: "/tmp/zkcode-atomic-submit".to_owned(),
        execution_config_json: r#"{"isolation":"readOnly","lifecycle":"attached"}"#.to_owned(),
        startup_epoch: 1,
    }
}

#[tokio::test]
async fn attached_child_and_parent_wait_state_commit_together() {
    let db = Db::open_in_memory().expect("database");
    let session = db
        .create_session("script", "/tmp/zkcode-atomic-submit")
        .await
        .expect("session");
    let root = db
        .create_task_with_run(&root_request(&session.id))
        .await
        .expect("root");
    let child = db
        .create_task_with_run(&child_request(
            &session.id,
            &root.task.id,
            &root.run_id,
            id(),
        ))
        .await
        .expect("child");

    assert!(child.created);
    let parent = db
        .find_runtime_task_by_id(&root.task.id)
        .await
        .expect("parent query")
        .expect("parent");
    let parent_run = db
        .find_run_by_id(&root.run_id)
        .await
        .expect("parent run query")
        .expect("parent run");
    assert_eq!(parent.status, TaskStatus::WaitingDependencies);
    assert_eq!(parent_run.status, "waitingDependencies");
}

#[tokio::test]
async fn late_child_insert_failure_rolls_back_parent_wait_and_every_child_row() {
    let db = Db::open_in_memory().expect("database");
    let session = db
        .create_session("script", "/tmp/zkcode-atomic-submit")
        .await
        .expect("session");
    let occupied_transcript = db
        .create_session("script", "/tmp/zkcode-occupied-transcript")
        .await
        .expect("occupied transcript");
    let root = db
        .create_task_with_run(&root_request(&session.id))
        .await
        .expect("root");
    let request = child_request(
        &session.id,
        &root.task.id,
        &root.run_id,
        occupied_transcript.id,
    );

    db.create_task_with_run(&request)
        .await
        .expect_err("duplicate transcript must roll the transaction back");
    assert!(
        db.find_runtime_task_by_id(&request.task_id)
            .await
            .expect("child query")
            .is_none()
    );
    assert!(
        db.find_run_by_id(&request.run_id)
            .await
            .expect("run query")
            .is_none()
    );
    let parent = db
        .find_runtime_task_by_id(&root.task.id)
        .await
        .expect("parent query")
        .expect("parent");
    let parent_run = db
        .find_run_by_id(&root.run_id)
        .await
        .expect("parent run query")
        .expect("parent run");
    assert_eq!(parent.status, TaskStatus::Queued);
    assert_eq!(parent_run.status, "queued");
}
