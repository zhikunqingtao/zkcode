//! WP-06 engineering verification admission and durable evidence tests.

mod common;

use axum::http::{Method, StatusCode};
use common::{call, json_body, local_with_headers};
use rusqlite::params;

#[tokio::test]
#[allow(clippy::too_many_lines)] // End-to-end fixture keeps admission, execution, and evidence assertions together.
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
    let producer_invocation_id = response["evidence"]["producerInvocationId"]
        .as_str()
        .expect("single successful machine check has one producer invocation")
        .to_owned();
    let persisted = db
        .find_evidence_bundle(bundle_id)
        .await
        .expect("evidence query")
        .expect("evidence");
    assert_eq!(persisted.run_id.as_deref(), Some("verify-run"));
    assert_eq!(
        persisted.producer_invocation_id.as_deref(),
        Some(producer_invocation_id.as_str())
    );
    assert_eq!(persisted.items.len(), 1);
    assert!(persisted.items[0].blob_sha256.is_some());
    assert_eq!(
        persisted.items[0].producer_invocation_id.as_deref(),
        Some(producer_invocation_id.as_str())
    );
    assert!(
        persisted.items[0]
            .meta
            .as_ref()
            .is_none_or(|meta| meta.get("producerInvocationId").is_none()),
        "producer identity must not be accepted from item metadata"
    );

    let expected_invocation = producer_invocation_id.clone();
    let (task_id, run_id, status, cleanup_status, output_ref, released_resources) = db
        .with_conn_blocking(move |connection| {
            let invocation = connection.query_row(
                "SELECT task_id,run_id,status,cleanup_status,output_ref
                   FROM tool_invocations WHERE invocation_id=?1",
                params![expected_invocation],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )?;
            let released_resources = connection.query_row(
                "SELECT COUNT(*) FROM execution_resources
                  WHERE invocation_id=?1 AND status='released'",
                params![expected_invocation],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((
                invocation.0,
                invocation.1,
                invocation.2,
                invocation.3,
                invocation.4,
                released_resources,
            ))
        })
        .expect("invocation and resource projection");
    assert_eq!(task_id, "verify-run");
    assert_eq!(run_id, "verify-run");
    assert_eq!(status, "succeeded");
    assert_eq!(cleanup_status, "confirmed");
    assert!(output_ref.is_some_and(|value| value.starts_with("evidenceBlob:")));
    assert_eq!(released_resources, 1);

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
async fn failing_custom_check_closes_invocation_after_confirmed_process_cleanup() {
    let (mut app, db) = common::app_with_db();
    let workspace = std::env::temp_dir().join(format!("zkcode-verify-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workspace).expect("workspace");
    let session = db
        .create_session("test-model", workspace.to_str().expect("utf8 path"))
        .await
        .expect("session");
    db.create_project("verify-failure", workspace.to_str().expect("utf8 path"))
        .await
        .expect("trusted project");
    db.start_run(
        "verify-failure-run",
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "test-model",
    )
    .await
    .expect("run");
    let request = serde_json::json!({
        "runId": "verify-failure-run",
        "checks": [{
            "kind": "custom",
            "command": "false",
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
    assert_eq!(response["report"]["status"], "fail");
    let invocation_id = response["evidence"]["producerInvocationId"]
        .as_str()
        .expect("failed verdict is produced by a completed verifier invocation")
        .to_owned();
    assert_eq!(
        response["evidence"]["items"][0]["producerInvocationId"],
        invocation_id
    );
    assert!(response["evidence"]["items"][0]["meta"]["producerInvocationId"].is_null());

    let expected_invocation = invocation_id.clone();
    let (invocation_status, cleanup_status, error_code, released_resources) = db
        .with_conn_blocking(move |connection| {
            let invocation = connection.query_row(
                "SELECT status,cleanup_status,error_code FROM tool_invocations
                  WHERE invocation_id=?1 AND task_id='verify-failure-run'
                    AND run_id='verify-failure-run'",
                params![expected_invocation],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )?;
            let released_resources = connection.query_row(
                "SELECT COUNT(*) FROM execution_resources
                  WHERE invocation_id=?1 AND status='released'",
                params![expected_invocation],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((invocation.0, invocation.1, invocation.2, released_resources))
        })
        .expect("failed invocation and resource projection");
    assert_eq!(invocation_status, "succeeded");
    assert_eq!(cleanup_status, "confirmed");
    assert_eq!(error_code, None);
    assert_eq!(released_resources, 1);

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
