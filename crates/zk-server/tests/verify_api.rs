//! WP-06 engineering verification admission and durable evidence tests.

mod common;

use axum::http::{Method, StatusCode};
use common::{call, json_body, local_with_headers};

#[tokio::test]
async fn short_custom_check_runs_through_admission_and_persists_evidence() {
    let (mut app, db) = common::app_with_db();
    let workspace = std::env::temp_dir().join(format!("zkcode-verify-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workspace).expect("workspace");
    let session = db
        .create_session("test-model", workspace.to_str().expect("utf8 path"))
        .await
        .expect("session");
    db.create_project("verify-test", workspace.to_str().expect("utf8 path"))
        .await
        .expect("trusted project");
    db.start_run(
        "verify-run",
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "test-model",
    )
    .await
    .expect("run");
    let request = serde_json::json!({
        "runId": "verify-run",
        "claim": "short verification passes",
        "checks": [{
            "kind": "custom",
            "command": "pwd",
            "timeout_ms": 5000
        }]
    })
    .to_string();
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/verify/run-checks",
            Method::POST,
            Some(request),
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let response = json_body(&body);
    assert_eq!(response["report"]["status"], "pass", "response: {response}");
    assert_eq!(response["report"]["checks"][0]["status"], "pass");
    assert_eq!(response["evidence"]["verdict"], "verified");
    let bundle_id = response["evidence"]["bundleId"]
        .as_str()
        .expect("bundle id");
    let persisted = db
        .find_evidence_bundle(bundle_id)
        .await
        .expect("evidence query")
        .expect("evidence");
    assert_eq!(persisted.run_id.as_deref(), Some("verify-run"));
    assert_eq!(persisted.items.len(), 1);
    assert!(persisted.items[0].blob_sha256.is_some());

    let events = db
        .get_run_events("verify-run", 0, 20)
        .await
        .expect("events");
    assert!(
        events
            .iter()
            .any(|event| event.event_type == "tool_started")
    );
    assert!(
        events
            .iter()
            .any(|event| event.event_type == "tool_finished")
    );

    std::fs::remove_dir_all(&workspace).expect("remove isolated test workspace");
}

#[tokio::test]
async fn verify_rejects_caller_selected_working_directory() {
    let mut app = common::app();
    let request = serde_json::json!({
        "runId": "unused",
        "workingDirectory": "/tmp",
        "checks": ["compile"]
    })
    .to_string();
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/verify/run-checks",
            Method::POST,
            Some(request),
            &[("x-session-id", "unused")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(&body)["code"],
        "VERIFY_WORKING_DIRECTORY_FORBIDDEN"
    );
}
