//! Production WS outbox identity and atomic Session restoration regressions.

use serde_json::json;
use zk_db::{CreateTaskWithRun, Db, NewToolInvocation};

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
        description: "WS runtime fixture".to_owned(),
        prompt: Some("test".to_owned()),
        task_type: "agent".to_owned(),
        model: "test-model".to_owned(),
        working_dir: "/tmp/zk-ws-runtime".to_owned(),
        execution_config_json: "{}".to_owned(),
        startup_epoch: 1,
    }
}

#[tokio::test]
async fn ws_outbox_rowid_and_restore_projection_share_one_durable_tree() {
    let db = Db::open_in_memory().expect("db");
    let session = db
        .create_session("test-model", "/tmp/zk-ws-runtime")
        .await
        .expect("session");
    let created = db
        .create_task_with_run(&root_request(&session.id))
        .await
        .expect("root task");
    db.create_tool_invocation(&NewToolInvocation {
        invocation_id: id(),
        task_id: created.task.id.clone(),
        run_id: created.run_id.clone(),
        tool_use_id: "tool-1".to_owned(),
        tool_name: "Read".to_owned(),
        input_json: Some(r#"{"path":"README.md"}"#.to_owned()),
        side_effect_class: "read".to_owned(),
        directory_generation: Some(1),
        connection_generation: None,
    })
    .await
    .expect("tool invocation");

    let event = db
        .append_ws_outbox_event(
            &session.id,
            &session.id,
            None,
            Some(&created.run_id),
            "ws_tool_use_start",
            Some("tool-1"),
            &json!({"type":"tool_use_start","toolUseId":"tool-1"}),
        )
        .await
        .expect("outbox append")
        .expect("run attributed event");
    assert!(event.id > 0);
    assert_eq!(event.source_run_id, created.run_id);
    assert_eq!(event.source_task_id, created.task.id);
    assert_eq!(event.root_session_id, session.id);

    let restore = db
        .get_session_runtime_restore(&event.root_session_id)
        .await
        .expect("restore query")
        .expect("session exists");
    assert_eq!(restore.snapshot_event_seq, event.id);
    assert_eq!(
        restore.run_snapshot.as_ref().map(|run| run.id.as_str()),
        Some(event.root_run_id.as_str())
    );
    assert_eq!(restore.active_tool_calls.len(), 1);
    assert_eq!(restore.task_tree.len(), 1);
    assert_eq!(restore.task_tree[0].id, created.task.id);
    assert_eq!(
        restore.task_tree[0].current_run_id.as_deref(),
        Some(created.run_id.as_str())
    );
    let tool = &restore.active_tool_calls[0];
    assert_eq!(tool.phase, "preparing");
    assert_eq!(tool.event_context["sourceRunId"], event.source_run_id);
    assert_eq!(tool.event_context["toolUseId"], "tool-1");
    assert!(restore.cost_summary.session_cost.abs() < f64::EPSILON);
    assert!(restore.cost_summary.usage_complete);

    // Replay rows keep the complete WS payload instead of the diagnostic
    // run-event 10 KiB preview, and use the global row ID as the cursor.
    let large_delta = "x".repeat(12 * 1024);
    let later = db
        .append_ws_outbox_event(
            &session.id,
            &session.id,
            None,
            Some(&created.run_id),
            "ws_stream_delta",
            None,
            &json!({"type":"stream_delta","delta":large_delta}),
        )
        .await
        .expect("later outbox append")
        .expect("later event");
    let replay = db
        .get_ws_outbox_events_after(&session.id, event.id)
        .await
        .expect("replay delta");
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].id, later.id);
    assert_eq!(
        replay[0].payload["delta"].as_str(),
        Some(large_delta.as_str())
    );
    assert!(
        db.get_ws_outbox_events_after(&session.id, later.id)
            .await
            .expect("empty replay")
            .is_empty()
    );
}

#[tokio::test]
async fn empty_restore_is_authoritative_and_outbox_route_mismatch_fails_closed() {
    let db = Db::open_in_memory().expect("db");
    let empty = db
        .create_session("test-model", "/tmp/empty")
        .await
        .expect("empty session");
    let restore = db
        .get_session_runtime_restore(&empty.id)
        .await
        .expect("restore")
        .expect("session");
    assert!(restore.run_snapshot.is_none());
    assert_eq!(restore.snapshot_event_seq, 0);
    assert!(restore.task_tree.is_empty());
    assert!(restore.active_tool_calls.is_empty());
    assert!(restore.cost_summary.total_cost.abs() < f64::EPSILON);

    let owner = db
        .create_session("test-model", "/tmp/zk-ws-runtime")
        .await
        .expect("owner");
    let created = db
        .create_task_with_run(&root_request(&owner.id))
        .await
        .expect("task");
    let error = db
        .append_ws_outbox_event(
            &empty.id,
            &owner.id,
            None,
            Some(&created.run_id),
            "ws_stream_delta",
            None,
            &json!({"type":"stream_delta","delta":"secret"}),
        )
        .await
        .expect_err("cross-session route must fail");
    assert!(error.to_string().contains("WS_OUTBOX_ROUTE_MISMATCH"));
}
