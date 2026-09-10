//! WP-06 artifact sealing, integrity verification and object authorization tests.

mod common;

use axum::http::{Method, StatusCode};
use common::{call, json_body, local_with_headers};
use zk_db::{
    AcceptanceCriterionRecord, CleanupStatus, EvidenceBundleRecord, EvidenceOrigin,
    NewToolInvocation, ToolInvocationStatus, VerificationStatus,
};

async fn seed_succeeded_invocation(db: &zk_db::Db, run_id: &str, tool_use_id: &str) -> String {
    let run = db
        .find_run_by_id(run_id)
        .await
        .expect("run query")
        .expect("run");
    let invocation_id = uuid::Uuid::new_v4().to_string();
    db.create_tool_invocation(&NewToolInvocation {
        invocation_id: invocation_id.clone(),
        task_id: run.task_id,
        run_id: run_id.to_owned(),
        tool_use_id: tool_use_id.to_owned(),
        tool_name: "Write".into(),
        input_json: Some("{}".into()),
        side_effect_class: "write".into(),
        directory_generation: Some(0),
        connection_generation: None,
    })
    .await
    .expect("create invocation");
    let transitioned = db
        .transition_tool_invocation_cas(
            &invocation_id,
            0,
            ToolInvocationStatus::Succeeded,
            Some("{}"),
            Some("toolResult:test"),
            None,
            CleanupStatus::Confirmed,
        )
        .await
        .expect("complete invocation");
    assert_eq!(transitioned, zk_db::CasOutcome::Applied);
    invocation_id
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One lifecycle fixture proves registration, mutation, stale evidence, and authorization.
async fn artifact_manifest_detects_mutation_and_authorizes_the_run() {
    let (mut app, db) = common::app_with_db();
    let workspace = std::env::temp_dir().join(format!("zkcode-artifact-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workspace).expect("workspace");
    std::fs::write(workspace.join("report.txt"), b"sealed content").expect("artifact");
    let session = db
        .create_session("test-model", workspace.to_str().expect("utf8 path"))
        .await
        .expect("session");
    let other = db
        .create_session("test-model", workspace.to_str().expect("utf8 path"))
        .await
        .expect("other session");
    db.start_run(
        "artifact-run",
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "test-model",
    )
    .await
    .expect("run");
    let producer_one = seed_succeeded_invocation(&db, "artifact-run", "tool-1").await;
    let producer_two = seed_succeeded_invocation(&db, "artifact-run", "tool-2").await;

    let request = serde_json::json!({
        "runId": "artifact-run",
        "entries": [
            {
                "toolUseId": "tool-1",
                "path": "report.txt",
                "operation": "created"
            },
            {
                "toolUseId": "tool-2",
                "path": "removed.txt",
                "operation": "deleted"
            }
        ]
    })
    .to_string();
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/artifacts/manifests",
            Method::POST,
            Some(request),
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let created = json_body(&body);
    let manifest_id = created["manifestId"].as_str().expect("manifest id");
    assert_eq!(created["state"], "sealed");
    assert_eq!(created["entries"].as_array().expect("entries").len(), 2);
    let producers = created["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .filter_map(|entry| entry["producerInvocationId"].as_str())
        .collect::<Vec<_>>();
    assert!(producers.contains(&producer_one.as_str()));
    assert!(producers.contains(&producer_two.as_str()));

    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/runs/artifact-run/manifest/verify",
            Method::POST,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["state"], "verified");

    db.with_conn_blocking(|conn| {
        conn.execute(
            "UPDATE run_envelopes SET verification_status='passed' WHERE id='artifact-run'",
            [],
        )?;
        conn.execute(
            "UPDATE tasks SET verification_status='passed' WHERE id='artifact-run'",
            [],
        )?;
        Ok(())
    })
    .expect("seed passing verification");
    db.save_evidence_bundle(&EvidenceBundleRecord {
        bundle_id: "artifact-evidence".into(),
        session_id: session.id.clone(),
        agent_id: None,
        kind: "verify".into(),
        claim: Some("artifact is intact".into()),
        origin: EvidenceOrigin::Human,
        producer_invocation_id: None,
        verdict: "verified".into(),
        created_at: "2026-09-08T00:00:00.000000Z".into(),
        run_id: Some("artifact-run".into()),
        items: Vec::new(),
    })
    .await
    .expect("evidence");
    db.replace_acceptance_criteria(
        "artifact-run",
        &[AcceptanceCriterionRecord {
            criterion_id: "artifact-criterion".into(),
            root_run_id: "artifact-run".into(),
            ordinal: 0,
            criterion_type: "business".into(),
            source_text: "report remains intact".into(),
            status: "passed".into(),
            evidence_bundle_id: Some("artifact-evidence".into()),
            created_at: "2026-09-08T00:00:00.000000Z".into(),
            updated_at: "2026-09-08T00:00:00.000000Z".into(),
        }],
    )
    .await
    .expect("criterion");

    // Same-size mutation proves that hash changes are detected independently of
    // the sealed byte count.
    std::fs::write(workspace.join("report.txt"), b"mutant content").expect("mutate");
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            &format!("/api/artifacts/manifests/{manifest_id}/verify"),
            Method::POST,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let verified = json_body(&body);
    assert_eq!(verified["state"], "unverified");
    assert!(
        verified["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .any(|entry| entry["failureCode"] == "ARTIFACT_HASH_MISMATCH")
    );
    assert_eq!(
        db.find_run_by_id("artifact-run")
            .await
            .expect("run query")
            .expect("run")
            .verification_status,
        "stale"
    );
    assert_eq!(
        db.find_runtime_task_by_id("artifact-run")
            .await
            .expect("task query")
            .expect("task")
            .verification_status,
        VerificationStatus::Stale
    );
    assert_eq!(
        db.find_evidence_bundle("artifact-evidence")
            .await
            .expect("evidence query")
            .expect("evidence")
            .verdict,
        "stale"
    );
    let criterion_status = db
        .with_conn_blocking(|conn| {
            conn.query_row(
                "SELECT status FROM run_acceptance_criteria \
                 WHERE criterion_id='artifact-criterion'",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(Into::into)
        })
        .expect("criterion status");
    assert_eq!(criterion_status, "not_verified");

    std::fs::write(workspace.join("report.txt"), b"sealed content").expect("restore");
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            &format!("/api/artifacts/manifests/{manifest_id}/verify"),
            Method::POST,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["state"], "verified");

    std::fs::write(
        workspace.join("report.txt"),
        b"sealed content with extra bytes",
    )
    .expect("resize");
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            &format!("/api/artifacts/manifests/{manifest_id}/verify"),
            Method::POST,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let size_changed = json_body(&body);
    assert_eq!(size_changed["state"], "unverified");
    assert!(
        size_changed["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .any(|entry| entry["failureCode"] == "ARTIFACT_SIZE_CHANGED")
    );

    std::fs::write(workspace.join("report.txt"), b"sealed content").expect("restore again");
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            &format!("/api/artifacts/manifests/{manifest_id}/verify"),
            Method::POST,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["state"], "verified");

    std::fs::remove_file(workspace.join("report.txt")).expect("remove artifact");
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            &format!("/api/artifacts/manifests/{manifest_id}/verify"),
            Method::POST,
            None,
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let missing = json_body(&body);
    assert_eq!(missing["state"], "unverified");
    assert!(
        missing["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .any(|entry| entry["failureCode"] == "ARTIFACT_FILE_MISSING")
    );

    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/runs/artifact-run/manifest",
            Method::GET,
            None,
            &[("x-session-id", &other.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_body(&body)["code"], "RUN_NOT_FOUND");

    std::fs::remove_dir_all(&workspace).expect("remove isolated test workspace");
}

#[tokio::test]
async fn artifact_manifest_rejects_an_unexecuted_tool_use_id() {
    let (mut app, db) = common::app_with_db();
    let workspace =
        std::env::temp_dir().join(format!("zkcode-artifact-owner-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workspace).expect("workspace");
    std::fs::write(workspace.join("report.txt"), b"content").expect("artifact");
    let session = db
        .create_session("test-model", workspace.to_str().expect("utf8 path"))
        .await
        .expect("session");
    db.start_run(
        "artifact-owner-run",
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "test-model",
    )
    .await
    .expect("run");
    let request = serde_json::json!({
        "runId": "artifact-owner-run",
        "entries": [{
            "toolUseId": "model-invented-id",
            "path": "report.txt",
            "operation": "created"
        }]
    })
    .to_string();
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/artifacts/manifests",
            Method::POST,
            Some(request),
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(&body)["code"],
        "ARTIFACT_PRODUCER_INVOCATION_NOT_FOUND"
    );
    std::fs::remove_dir_all(&workspace).expect("remove isolated test workspace");
}

#[cfg(unix)]
#[tokio::test]
async fn artifact_manifest_rejects_direct_symlinks() {
    use std::os::unix::fs::symlink;

    let (mut app, db) = common::app_with_db();
    let workspace =
        std::env::temp_dir().join(format!("zkcode-artifact-link-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workspace).expect("workspace");
    std::fs::write(workspace.join("target.txt"), b"content").expect("target");
    symlink(workspace.join("target.txt"), workspace.join("link.txt")).expect("symlink");
    let session = db
        .create_session("test-model", workspace.to_str().expect("utf8 path"))
        .await
        .expect("session");
    db.start_run(
        "artifact-link-run",
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "test-model",
    )
    .await
    .expect("run");
    seed_succeeded_invocation(&db, "artifact-link-run", "tool-link").await;
    let request = serde_json::json!({
        "runId": "artifact-link-run",
        "entries": [{
            "toolUseId": "tool-link",
            "path": "link.txt",
            "operation": "created"
        }]
    })
    .to_string();
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/artifacts/manifests",
            Method::POST,
            Some(request),
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&body)["code"], "ARTIFACT_SYMLINK_FORBIDDEN");

    std::fs::remove_dir_all(&workspace).expect("remove isolated test workspace");
}
