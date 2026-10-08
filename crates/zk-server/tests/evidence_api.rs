//! WP-06 Evidence API persistence, authorization and blob safety tests.

mod common;

use axum::http::{Method, StatusCode};
use base64::Engine as _;
use common::{call, json_body, local_with_headers};

async fn create_blob_evidence(
    app: &mut axum::Router,
    session: &str,
    bytes: &[u8],
) -> zk_db::EvidenceBundleRecord {
    let body = serde_json::json!({
        "sessionId": session,
        "kind": "deletion-regression",
        "items": [{
            "type": "log",
            "blobBase64": base64::engine::general_purpose::STANDARD.encode(bytes)
        }]
    });
    let (status, _, response) = call(
        app,
        local_with_headers(
            "/api/evidence",
            Method::POST,
            Some(body.to_string()),
            &[("X-Session-Id", session)],
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&response)
    );
    serde_json::from_slice(&response).expect("created evidence")
}

async fn seed_succeeded_machine_evidence(
    db: &zk_db::Db,
    mut bundle: zk_db::EvidenceBundleRecord,
) -> zk_db::EvidenceBundleRecord {
    let run = uuid::Uuid::new_v4().to_string();
    db.start_run(&run, &bundle.session_id, None, Some("query"), "fixture")
        .await
        .expect("start run");
    let invocation = db
        .create_tool_invocation(&zk_db::NewToolInvocation {
            invocation_id: uuid::Uuid::new_v4().to_string(),
            task_id: run.clone(),
            run_id: run.clone(),
            tool_use_id: "verify-deletion".into(),
            tool_name: "VerifyJourney".into(),
            input_json: Some("{}".into()),
            side_effect_class: "read".into(),
            directory_generation: Some(1),
            connection_generation: None,
        })
        .await
        .expect("producer invocation");
    assert_eq!(
        db.transition_tool_invocation_cas(
            &invocation.invocation_id,
            invocation.version,
            zk_db::ToolInvocationStatus::Succeeded,
            Some("{\"ok\":true}"),
            Some("toolResult:verify-deletion"),
            None,
            zk_db::CleanupStatus::Confirmed,
        )
        .await
        .expect("succeed invocation"),
        zk_db::CasOutcome::Applied
    );
    bundle.bundle_id = uuid::Uuid::new_v4().to_string();
    bundle.origin = zk_db::EvidenceOrigin::Machine;
    bundle.verdict = "verified".into();
    bundle.run_id = Some(run.clone());
    bundle.producer_invocation_id = Some(invocation.invocation_id.clone());
    for item in &mut bundle.items {
        item.id = uuid::Uuid::new_v4().to_string();
        item.producer_invocation_id = Some(invocation.invocation_id.clone());
    }
    db.save_evidence_bundle(&bundle)
        .await
        .expect("save verified machine evidence");
    db.ensure_task_final_assistant(&run, &run, "verified")
        .await
        .expect("final assistant message");
    let task = db
        .find_runtime_task_by_id(&run)
        .await
        .expect("find task")
        .expect("task");
    assert!(matches!(
        db.commit_task_result(&zk_db::CommitTaskResult {
            task_id: task.id,
            run_id: run,
            expected_task_version: task.version,
            status: zk_db::ResultStatus::Complete,
            content: "verified".into(),
            media_type: "text/plain".into(),
            error_code: None,
            cleanup_status: zk_db::CleanupStatus::Confirmed,
            verification_status: zk_db::VerificationStatus::Passed,
        })
        .await
        .expect("finish task and run"),
        zk_db::CommitTaskResultOutcome::Committed { .. }
    ));
    bundle
}

#[allow(clippy::too_many_lines)] // Exercise deletion and all evidence endpoints on the same state.
async fn evidence_session_deletion_fixture(machine: bool) {
    let (mut app, db) = common::app_with_db();
    let workspace =
        std::env::temp_dir().join(format!("zkcode-evidence-deletion-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workspace).expect("workspace");
    let user_file = workspace.join("user-owned.txt");
    std::fs::write(&user_file, b"keep user files").expect("user file");
    let deleted = db
        .create_session("fixture", workspace.to_str().expect("utf8 path"))
        .await
        .expect("deleted session");
    let survivor = db
        .create_session("fixture", workspace.to_str().expect("utf8 path"))
        .await
        .expect("surviving session");
    let mut evidence =
        create_blob_evidence(&mut app, &deleted.id, b"deleted private evidence").await;
    if machine {
        evidence = seed_succeeded_machine_evidence(&db, evidence).await;
    }
    let other = create_blob_evidence(&mut app, &survivor.id, b"surviving evidence").await;
    let digest = evidence.items[0].blob_sha256.as_deref().expect("digest");
    let survivor_digest = other.items[0].blob_sha256.as_deref().expect("digest");
    let (status, _, response) = call(
        &mut app,
        local_with_headers(
            &format!("/api/evidence/{}", evidence.bundle_id),
            Method::GET,
            None,
            &[("X-Session-Id", &deleted.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&response)["bundleId"], evidence.bundle_id);

    for _ in 0..2 {
        let (status, _, response) = call(
            &mut app,
            common::local_delete(&format!("/api/sessions/{}", deleted.id)),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "deleting a completed evidence-owning session must be idempotent: {}",
            String::from_utf8_lossy(&response)
        );
        assert_eq!(json_body(&response)["success"], true);
    }
    let (status, _, _) = call(
        &mut app,
        common::local_get(&format!("/api/sessions/{}", deleted.id)),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    for asserted in [&deleted.id, &survivor.id] {
        for (path, method, body) in [
            (
                format!("/api/evidence/{}", evidence.bundle_id),
                Method::GET,
                None,
            ),
            (
                format!("/api/evidence/{}/verify", evidence.bundle_id),
                Method::POST,
                Some("{\"verdict\":\"verified\"}".to_owned()),
            ),
            (format!("/api/evidence/blob/{digest}"), Method::GET, None),
            (
                format!("/api/evidence/session/{}", deleted.id),
                Method::GET,
                None,
            ),
        ] {
            let (status, _, response) = call(
                &mut app,
                local_with_headers(&path, method, body, &[("X-Session-Id", asserted)]),
            )
            .await;
            assert!(
                matches!(status, StatusCode::FORBIDDEN | StatusCode::NOT_FOUND),
                "deleted evidence must be inaccessible without becoming a server error: {path}, {status}, {}",
                String::from_utf8_lossy(&response)
            );
        }
    }

    for path in [
        format!("/api/evidence/{}", other.bundle_id),
        format!("/api/evidence/session/{}", survivor.id),
    ] {
        let (status, _, response) = call(
            &mut app,
            local_with_headers(&path, Method::GET, None, &[("X-Session-Id", &survivor.id)]),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(String::from_utf8_lossy(&response).contains(&other.bundle_id));
    }
    let (status, _, response) = call(
        &mut app,
        local_with_headers(
            &format!("/api/evidence/blob/{survivor_digest}"),
            Method::GET,
            None,
            &[("X-Session-Id", &survivor.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response.as_ref(), b"surviving evidence");
    let (status, _, response) = call(
        &mut app,
        local_with_headers(
            &format!("/api/evidence/{}/verify", other.bundle_id),
            Method::POST,
            Some("{\"verdict\":\"verified\"}".into()),
            &[("X-Session-Id", &survivor.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&response)["verdict"], "verified");
    assert_eq!(
        std::fs::read(&user_file).expect("user file"),
        b"keep user files"
    );
    for (hash, expected) in [
        (digest, b"deleted private evidence".as_slice()),
        (survivor_digest, b"surviving evidence".as_slice()),
    ] {
        assert_eq!(
            std::fs::read(workspace.join(".zk/blobs").join(&hash[..2]).join(hash))
                .expect("session deletion preserves workspace blobs"),
            expected
        );
    }
    std::fs::remove_dir_all(workspace).expect("remove isolated workspace");
}

#[tokio::test]
async fn deleting_session_with_succeeded_machine_evidence_revokes_access_and_preserves_files() {
    evidence_session_deletion_fixture(true).await;
}

#[tokio::test]
async fn deleted_model_assertion_evidence_returns_access_error_instead_of_server_error() {
    evidence_session_deletion_fixture(false).await;
}

#[tokio::test]
async fn image_preview_requires_authorization_and_detects_blob_corruption() {
    let (mut app, db) = common::app_with_db();
    let workspace = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&workspace).unwrap();
    let session = db
        .create_session("test", workspace.to_str().unwrap())
        .await
        .unwrap();
    let bytes = base64::engine::general_purpose::STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==").unwrap();
    let body = serde_json::json!({"sessionId":session.id,"kind":"test","items":[{"type":"image","blobBase64":base64::engine::general_purpose::STANDARD.encode(&bytes)}]});
    let (status, _, response) = call(
        &mut app,
        local_with_headers(
            "/api/evidence",
            Method::POST,
            Some(body.to_string()),
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let evidence = json_body(&response);
    let hash = evidence["items"][0]["blobSha256"].as_str().unwrap();
    let path = format!("/api/evidence/blob/{hash}?preview=true");
    let (status, headers, response) = call(
        &mut app,
        local_with_headers(&path, Method::GET, None, &[("X-Session-Id", &session.id)]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "image/png");
    assert_eq!(headers["x-content-type-options"], "nosniff");
    assert_eq!(&response[..], bytes.as_slice());
    let (status, _, _) = call(&mut app, common::local_get(&path)).await;
    assert!(!status.is_success());
    std::fs::write(
        workspace.join(".zk/blobs").join(&hash[..2]).join(hash),
        b"corrupt",
    )
    .unwrap();
    let (status, _, response) = call(
        &mut app,
        local_with_headers(&path, Method::GET, None, &[("X-Session-Id", &session.id)]),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&response)["code"], "EVIDENCE_BLOB_CORRUPT");
    std::fs::remove_dir_all(workspace).unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One authorization fixture covers metadata, blobs, and isolation.
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
    assert_eq!(created["origin"], "modelAssertion");
    assert_eq!(created["verdict"], "pending");
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
            &format!("/api/evidence/{bundle_id}/verify"),
            Method::POST,
            Some(serde_json::json!({"verdict": "verified"}).to_string()),
            &[("X-Session-Id", &session.id)],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let reviewed = json_body(&bytes);
    assert_eq!(reviewed["origin"], "human");
    assert_eq!(reviewed["verdict"], "verified");

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

#[tokio::test]
async fn submitted_model_claim_cannot_forge_a_verified_verdict() {
    let (mut app, db) = common::app_with_db();
    let workspace =
        std::env::temp_dir().join(format!("zkcode-evidence-claim-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workspace).expect("workspace");
    let session = db
        .create_session("test-model", workspace.to_str().expect("utf8 path"))
        .await
        .expect("session");
    let body = serde_json::json!({
        "sessionId": session.id,
        "kind": "model_report",
        "claim": "I ran every check",
        "verdict": "verified"
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
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&bytes)["code"], "MODEL_ASSERTION_CANNOT_VERIFY");
    assert!(
        db.find_evidence_by_session(&session.id)
            .await
            .expect("query")
            .is_empty()
    );
    std::fs::remove_dir_all(&workspace).expect("remove workspace");
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

#[tokio::test]
async fn temporary_blob_preview_is_exactly_owned_and_never_creates_a_disk_blob() {
    use zk_server::{config::Config, routes::build_router, state::AppState};
    let root = std::env::temp_dir().join(format!(
        "zk-ephemeral-evidence-api-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root).unwrap();
    let db = zk_db::Db::open(root.join("evidence.sqlite")).unwrap();
    let (session, lease) = db
        .create_ephemeral_session("fixture", root.to_str().unwrap(), "DEFAULT")
        .await
        .unwrap();
    let other = db
        .create_session("fixture", root.to_str().unwrap())
        .await
        .unwrap();
    let marker = format!("temporary-evidence-body-{}", uuid::Uuid::new_v4());
    let encoded = base64::engine::general_purpose::STANDARD.encode(marker.as_bytes());
    let state = AppState::new(db.clone(), Config::test_config());
    let mut app = build_router(state);
    let request = serde_json::json!({"sessionId":session,"kind":"log","claim":marker,"items":[{"type":"log","blobBase64":encoded,"summary":marker,"meta":{"body":marker}}]});
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/api/evidence",
            Method::POST,
            Some(request.to_string()),
            &[("X-Session-Id", &session)],
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&body)
    );
    let bundle = json_body(&body);
    let digest = bundle["items"][0]["blobSha256"].as_str().unwrap();
    let path = format!("/api/evidence/blob/{digest}");
    let (status, _, body) = call(
        &mut app,
        local_with_headers(&path, Method::GET, None, &[("X-Session-Id", &session)]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), marker.as_bytes());
    let (status, _, _) = call(
        &mut app,
        local_with_headers(&path, Method::GET, None, &[("X-Session-Id", &other.id)]),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a shared workspace does not authorize the other session's blob"
    );
    assert!(!root.join(".zk").exists());
    for entry in std::fs::read_dir(&root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            let bytes = std::fs::read(&path).unwrap();
            for needle in [marker.as_str(), digest, encoded.as_str()] {
                assert!(
                    !bytes
                        .windows(needle.len())
                        .any(|part| part == needle.as_bytes()),
                    "temporary body/hash leaked to {}",
                    path.display()
                );
            }
        }
    }
    drop(lease);
    let (status, _, _) = call(
        &mut app,
        local_with_headers(&path, Method::GET, None, &[("X-Session-Id", &session)]),
    )
    .await;
    assert!(!status.is_success());
    drop(app);
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}
