//! WP-06 Evidence API persistence, authorization and blob safety tests.

mod common;

use axum::http::{Method, StatusCode};
use base64::Engine as _;
use common::{call, json_body, local_with_headers};

#[tokio::test]
async fn evidence_bundle_and_blob_round_trip_with_session_authorization() {
    let (mut app, db) = common::app_with_db();
    let workspace = std::env::temp_dir().join(format!("zkcode-evidence-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workspace).expect("workspace");
    let session = db
        .create_session("test-model", workspace.to_str().expect("utf8 path"))
        .await
        .expect("session");
    let other = db
        .create_session("test-model", workspace.to_str().expect("utf8 path"))
        .await
        .expect("other session");
    let encoded = base64::engine::general_purpose::STANDARD.encode(b"verification log");
    let body = serde_json::json!({
        "sessionId": session.id,
        "kind": "verify",
        "claim": "token=supersecretvalue tests pass",
        "items": [
            {
                "type": "log",
                "summary": "api_key=supersecretvalue",
                "blobBase64": encoded
            },
            {
                "type": "duplicate_log",
                "blobBase64": encoded
            }
        ]
    })
    .to_string();
    let (status, _, bytes) = call(
        &mut app,
        local_with_headers(
            "/api/evidence",
            Method::POST,
            Some(body),
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let created = json_body(&bytes);
    let bundle_id = created["bundleId"].as_str().expect("bundle id");
    let digest = created["items"][0]["blobSha256"].as_str().expect("digest");
    assert_eq!(created["items"][1]["blobSha256"], digest);
    assert_eq!(digest.len(), 64);
    assert!(
        !created["claim"]
            .as_str()
            .expect("claim")
            .contains("supersecretvalue")
    );

    let (status, _, bytes) = call(
        &mut app,
        local_with_headers(
            &format!("/api/evidence/{bundle_id}"),
            Method::GET,
            None,
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&bytes)["bundleId"], bundle_id);

    let (status, _, bytes) = call(
        &mut app,
        local_with_headers(
            &format!("/api/evidence/blob/{}", digest.to_ascii_uppercase()),
            Method::GET,
            None,
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&bytes[..], b"verification log");

    let (status, _, bytes) = call(
        &mut app,
        local_with_headers(
            &format!("/api/evidence/{bundle_id}"),
            Method::GET,
            None,
            &[("X-Session-Id", &other.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json_body(&bytes)["code"], "SESSION_ACCESS_DENIED");

    let (status, _, bytes) = call(
        &mut app,
        local_with_headers(
            "/api/evidence/blob/not-a-hash",
            Method::GET,
            None,
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&bytes)["code"], "EVIDENCE_BLOB_HASH_INVALID");

    std::fs::remove_dir_all(&workspace).expect("remove isolated test workspace");
}

#[cfg(unix)]
#[tokio::test]
async fn evidence_blob_store_rejects_symlink_escape() {
    use std::os::unix::fs::symlink;

    let (mut app, db) = common::app_with_db();
    let workspace =
        std::env::temp_dir().join(format!("zkcode-evidence-link-{}", uuid::Uuid::new_v4()));
    let outside =
        std::env::temp_dir().join(format!("zkcode-evidence-outside-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(workspace.join(".zk")).expect("workspace metadata");
    std::fs::create_dir_all(&outside).expect("outside");
    symlink(&outside, workspace.join(".zk/blobs")).expect("blob root symlink");
    let session = db
        .create_session("test-model", workspace.to_str().expect("utf8 path"))
        .await
        .expect("session");
    let encoded = base64::engine::general_purpose::STANDARD.encode(b"must stay inside");
    let body = serde_json::json!({
        "sessionId": session.id,
        "kind": "verify",
        "items": [{"type": "log", "blobBase64": encoded}]
    })
    .to_string();
    let (status, _, bytes) = call(
        &mut app,
        local_with_headers(
            "/api/evidence",
            Method::POST,
            Some(body),
            &[("x-session-id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&bytes)["code"], "EVIDENCE_BLOB_PATH_ESCAPE");
    assert!(
        std::fs::read_dir(&outside)
            .expect("outside readable")
            .next()
            .is_none()
    );

    std::fs::remove_dir_all(&workspace).expect("remove workspace");
    std::fs::remove_dir_all(&outside).expect("remove outside");
}
