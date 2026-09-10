//! Regression coverage for durable caller attribution of tool-result WS events.

use serde_json::json;
use zk_db::{CreateTaskWithRun, Db, NewToolInvocation};

fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn request(
    root_session_id: &str,
    task_id: String,
    run_id: String,
    transcript_session_id: String,
    parent: Option<(&str, &str, &str)>,
) -> CreateTaskWithRun {
    let execution_config_json = parent.map_or_else(
        || {
            serde_json::json!({
                "budget": {
                    "tokenLimit": 1_000_000,
                    "costLimitNanosUsd": 1_000_000_000_000_i64,
                    "deadlineAtMs": zk_db::time::now_millis() + 60_000,
                }
            })
            .to_string()
        },
        |_| "{}".to_owned(),
    );
    CreateTaskWithRun {
        task_id,
        run_id,
        root_session_id: root_session_id.to_owned(),
        transcript_session_id,
        parent_task_id: parent.map(|(task_id, _, _)| task_id.to_owned()),
        parent_run_id: parent.map(|(_, run_id, _)| run_id.to_owned()),
        creator_tool_use_id: parent.map(|(_, _, tool_use_id)| tool_use_id.to_owned()),
        ordinal: 0,
        description: "WS actor fixture".to_owned(),
        prompt: parent.map(|_| "child".to_owned()),
        task_type: "agent".to_owned(),
        model: "script".to_owned(),
        working_dir: "/tmp/zkcode-ws-actor".to_owned(),
        execution_config_json,
        startup_epoch: 1,
    }
}

#[tokio::test]
async fn tool_result_business_ids_cannot_replace_the_persisted_invocation_actor() {
    let db = Db::open_in_memory().expect("database");
    let root_session = db
        .create_session("script", "/tmp/zkcode-ws-actor")
        .await
        .expect("root session");
    let root = db
        .create_task_with_run(&request(
            &root_session.id,
            id(),
            id(),
            root_session.id.clone(),
            None,
        ))
        .await
        .expect("root task");
    let child_tool_use_id = "agent-tool-use";
    let child = db
        .create_task_with_run(&request(
            &root_session.id,
            id(),
            id(),
            id(),
            Some((&root.task.id, &root.run_id, child_tool_use_id)),
        ))
        .await
        .expect("child task");

    let preparing = db
        .append_ws_outbox_event(
            &root_session.id,
            &root_session.id,
            None,
            None,
            "ws_tool_use_start",
            Some(child_tool_use_id),
            &json!({
                "type": "tool_use_start",
                "toolUseId": child_tool_use_id,
                "input": {},
            }),
        )
        .await
        .expect("preparing outbox")
        .expect("preparing event resolves through the source Session");
    assert_eq!(preparing.source_task_id, root.task.id);
    assert_eq!(preparing.source_run_id, root.run_id);

    db.create_tool_invocation(&NewToolInvocation {
        invocation_id: id(),
        task_id: root.task.id.clone(),
        run_id: root.run_id.clone(),
        tool_use_id: child_tool_use_id.to_owned(),
        tool_name: "Agent".to_owned(),
        input_json: Some(r#"{"prompt":"child"}"#.to_owned()),
        side_effect_class: "none".to_owned(),
        directory_generation: None,
        connection_generation: None,
    })
    .await
    .expect("caller invocation");

    let payload = json!({
        "type": "tool_result",
        "toolUseId": child_tool_use_id,
        "result": {
            "content": "child submitted",
            "isError": false,
            "metadata": {"structuredResult": {
                "taskId": child.task.id.clone(),
                "runId": child.run_id.clone(),
                "sessionId": child.transcript_session_id.clone(),
            }}
        }
    });
    let event = db
        .append_ws_outbox_event(
            &root_session.id,
            &root_session.id,
            None,
            None,
            "ws_tool_result",
            Some(child_tool_use_id),
            &payload,
        )
        .await
        .expect("outbox")
        .expect("durable event");

    assert_eq!(event.source_task_id, root.task.id);
    assert_eq!(event.source_run_id, root.run_id);
    assert_ne!(event.source_task_id, child.task.id);
    assert_ne!(event.source_run_id, child.run_id);
}
