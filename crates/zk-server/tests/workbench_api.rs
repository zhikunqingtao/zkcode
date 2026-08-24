//! WP-06 workbench persistence, evidence binding and authorization tests.

mod common;

use axum::http::{Method, StatusCode};
use common::{call, json_body, local_with_headers};
use zk_db::{
    AcceptanceCriterionRecord, ArtifactEntryRecord, ArtifactManifestRecord, EvidenceBundleRecord,
    WorkbenchBindingRecord,
    model::{MessageRole, NewMessage, StoredBlock},
};

#[tokio::test]
#[allow(clippy::too_many_lines)] // full real Router/SQLite workbench round trip
async fn workbench_round_trip_requires_owned_run_and_owned_evidence() {
    let (mut app, db) = common::app_with_db();
    let session = db
        .create_session("test-model", "/tmp/workbench-api")
        .await
        .expect("session");
    let other = db
        .create_session("test-model", "/tmp/workbench-api")
        .await
        .expect("other session");
    let request_message = db
        .append_message(
            &session.id,
            NewMessage {
                role: MessageRole::User,
                content: vec![StoredBlock::Text {
                    text: "tests must pass".into(),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .expect("request message");
    db.start_run(
        "workbench-run",
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "test-model",
    )
    .await
    .expect("run");
    let now = "2026-08-22T00:00:00.000000Z".to_owned();
    db.initialize_workbench(
        &WorkbenchBindingRecord {
            root_run_id: "workbench-run".into(),
            request_message_id: request_message.id.clone(),
            result_message_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        &[AcceptanceCriterionRecord {
            criterion_id: "criterion-api".into(),
            root_run_id: "workbench-run".into(),
            ordinal: 0,
            criterion_type: "business".into(),
            source_text: "tests must pass".into(),
            status: "not_verified".into(),
            evidence_bundle_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        }],
    )
    .await
    .expect("workbench");
    db.save_evidence_bundle(&EvidenceBundleRecord {
        bundle_id: "evidence-api".into(),
        session_id: session.id.clone(),
        agent_id: None,
        kind: "test".into(),
        claim: Some("tests pass".into()),
        verdict: "verified".into(),
        created_at: now,
        run_id: Some("workbench-run".into()),
        items: Vec::new(),
    })
    .await
    .expect("evidence");

    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/workbench/workbench-run",
            Method::GET,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["binding"]["rootRunId"], "workbench-run");

    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            &format!("/api/sessions/{}/workbench/current", session.id),
            Method::GET,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let current = json_body(&body);
    assert_eq!(current["correlationMode"], "EXACT");
    assert_eq!(current["rootRun"]["id"], "workbench-run");
    assert_eq!(current["requestMessageId"], request_message.id);
    assert_eq!(
        current["verification"]["businessCriteria"][0]["status"],
        "NOT_VERIFIED"
    );

    let update = serde_json::json!({
        "criteria": [{
            "criterionId": "criterion-api",
            "status": "passed",
            "evidenceBundleId": "evidence-api"
        }]
    })
    .to_string();
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/workbench/workbench-run",
            Method::PUT,
            Some(update),
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let updated = json_body(&body);
    assert_eq!(updated["criteria"][0]["status"], "passed");
    assert_eq!(updated["criteria"][0]["evidenceBundleId"], "evidence-api");

    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/workbench/workbench-run",
            Method::GET,
            None,
            &[("x-session-id", &other.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_body(&body)["code"], "RUN_NOT_FOUND");
}

#[tokio::test]
async fn session_without_run_returns_an_empty_projection() {
    let (mut app, db) = common::app_with_db();
    let session = db
        .create_session("test-model", "/tmp/workbench-empty")
        .await
        .expect("session");
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            &format!("/api/sessions/{}/workbench/current", session.id),
            Method::GET,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let current = json_body(&body);
    assert!(current["rootRun"].is_null());
    assert_eq!(current["delivery"]["totalFiles"], 0);
    assert_eq!(current["pendingActionCount"], 0);
}

#[tokio::test]
async fn workbench_task_search_returns_global_reference_groups_without_session_header() {
    let (mut app, db) = common::app_with_db();
    let session = db
        .create_session("test-model", "/tmp/workbench-task-search")
        .await
        .expect("session");
    db.append_message(
        &session.id,
        NewMessage {
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: "verify release".into(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .expect("goal message");
    let unrelated = db
        .create_session("test-model", "/tmp/unrelated-folder")
        .await
        .expect("unrelated session");
    db.update_session_title(&unrelated.id, "different task")
        .await
        .expect("unrelated title");

    let (status, _, body) = call(
        &mut app,
        common::local_get("/api/workbench/tasks?query=release"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let response = json_body(&body);
    let groups = response["groups"].as_array().expect("groups");
    assert_eq!(groups.len(), 4);
    assert_eq!(groups[0]["status"], "ACTION_REQUIRED");
    assert_eq!(groups[0]["label"], "待我处理");
    assert_eq!(groups[1]["status"], "RUNNING");
    assert_eq!(groups[2]["status"], "REVIEWABLE");
    assert_eq!(groups[3]["status"], "OTHER");
    let tasks = groups[3]["tasks"].as_array().expect("other tasks");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0]["sessionId"], session.id);
    assert_eq!(tasks[0]["title"], "verify release");
    assert_eq!(tasks[0]["folderName"], "workbench-task-search");
    assert_eq!(tasks[0]["pendingCount"], 0);
    assert_eq!(tasks[0]["hint"], "尚未开始执行");
    assert!(tasks[0]["updatedAt"].as_str().is_some());
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // current/previous delivery fallback integration matrix
async fn failed_current_run_exposes_the_previous_completed_delivery() {
    let (mut app, db) = common::app_with_db();
    let session = db
        .create_session("test-model", "/tmp/workbench-previous-delivery")
        .await
        .expect("session");
    let previous_request = db
        .append_message(
            &session.id,
            NewMessage {
                role: MessageRole::User,
                content: vec![StoredBlock::Text {
                    text: "build the report".into(),
                }],
                stop_reason: None,
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .expect("request");
    let previous_result = db
        .append_message(
            &session.id,
            NewMessage {
                role: MessageRole::Assistant,
                content: vec![StoredBlock::Text {
                    text: "report delivered".into(),
                }],
                stop_reason: Some("end_turn".into()),
                input_tokens: 0,
                output_tokens: 0,
            },
        )
        .await
        .expect("result");
    db.start_run(
        "previous-root",
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "test-model",
    )
    .await
    .expect("previous run");
    db.complete_run("previous-root", 0, 0.0, 1)
        .await
        .expect("complete previous");
    let now = "2026-08-22T00:00:00.000000Z".to_owned();
    db.initialize_workbench(
        &WorkbenchBindingRecord {
            root_run_id: "previous-root".into(),
            request_message_id: previous_request.id,
            result_message_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        &[],
    )
    .await
    .expect("previous workbench");
    db.bind_workbench_result("previous-root", &previous_result.id)
        .await
        .expect("bind previous result");
    db.save_artifact_manifest(&ArtifactManifestRecord {
        manifest_id: "previous-manifest".into(),
        run_id: "previous-root".into(),
        session_id: session.id.clone(),
        workspace_root: "/tmp/workbench-previous-delivery".into(),
        state: "verified".into(),
        created_at: now.clone(),
        updated_at: now.clone(),
        entries: vec![ArtifactEntryRecord {
            artifact_id: "previous-artifact".into(),
            tool_use_id: "write-report".into(),
            canonical_path: "/tmp/workbench-previous-delivery/report.md".into(),
            operation: "created".into(),
            state: "integrity_verified".into(),
            sealed_hash: Some("abc".into()),
            actual_hash: Some("abc".into()),
            file_size: Some(12),
            required_validator_id: None,
            validator_result: None,
            failure_code: None,
            created_at: now.clone(),
            updated_at: now,
        }],
    })
    .await
    .expect("manifest");

    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    db.start_run(
        "failed-root",
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "test-model",
    )
    .await
    .expect("failed run");
    db.terminate_run(
        "failed-root",
        zk_db::run::EXIT_INTERNAL_ERROR,
        Some("provider unavailable"),
    )
    .await
    .expect("terminate current");

    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            &format!("/api/sessions/{}/workbench/current", session.id),
            Method::GET,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let current = json_body(&body);
    assert_eq!(current["rootRun"]["id"], "failed-root");
    assert_eq!(current["currentFailure"]["status"], "FAILED");
    assert_eq!(
        current["previousAvailableDelivery"]["rootRunId"],
        "previous-root"
    );
    assert_eq!(
        current["previousAvailableDelivery"]["delivery"]["totalFiles"],
        1
    );
    assert_eq!(
        current["previousAvailableDelivery"]["result"]["text"],
        "report delivered"
    );
}
