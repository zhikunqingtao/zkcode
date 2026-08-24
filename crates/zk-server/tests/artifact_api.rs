//! WP-06 artifact sealing, integrity verification and object authorization tests.

mod common;

use axum::http::{Method, StatusCode};
use common::{call, json_body, local_with_headers};

#[tokio::test]
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

    std::fs::write(workspace.join("report.txt"), b"tampered content").expect("mutate");
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
    assert_eq!(verified["state"], "failed");
    assert!(
        verified["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .any(|entry| entry["failureCode"] == "ARTIFACT_HASH_MISMATCH")
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
