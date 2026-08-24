//! Session snapshot REST persistence and atomic restore tests.

mod common;

use axum::http::StatusCode;
use common::{call, json_body, local_delete, local_get, local_post};
use zk_db::{MessageRole, NewMessage, StoredBlock};
use zk_server::config::Config;
use zk_server::routes::build_router;
use zk_server::state::AppState;

fn fixture(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "zkcode-snapshot-api-{tag}-{}",
        uuid::Uuid::new_v4()
    ));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).expect("create fixture workspace");
    (
        std::fs::canonicalize(workspace).expect("canonical workspace"),
        root.join("snapshots"),
    )
}

#[tokio::test]
async fn save_restart_resume_is_idempotent_and_delete_is_durable() {
    let (workspace, snapshot_dir) = fixture("roundtrip");
    let db = zk_db::Db::open_in_memory().expect("db");
    let session = db
        .create_session("model-before", &workspace.to_string_lossy())
        .await
        .expect("session");
    db.append_message(
        &session.id,
        NewMessage {
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: "saved message".to_owned(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        },
    )
    .await
    .expect("message");

    let mut config = Config::test_config();
    config.snapshot_dir = Some(snapshot_dir.clone());
    let mut router = build_router(AppState::new(db.clone(), config.clone()));
    let path = format!("/api/sessions/{}/snapshot", session.id);
    let (status, _, body) = call(&mut router, local_post(&path, None)).await;
    assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    assert_eq!(json_body(&body)["messageCount"], 1);

    db.update_session_model(&session.id, "model-after")
        .await
        .expect("update model");
    db.append_message(
        &session.id,
        NewMessage {
            role: MessageRole::Assistant,
            content: vec![StoredBlock::Text {
                text: "unsaved message".to_owned(),
            }],
            stop_reason: Some("end_turn".to_owned()),
            input_tokens: 1,
            output_tokens: 1,
        },
    )
    .await
    .expect("second message");

    // Rebuild AppState with the same DB and snapshot directory: no in-memory snapshot state.
    let mut router = build_router(AppState::new(db.clone(), config));
    let list_path = "/api/sessions/snapshots";
    let (status, _, body) = call(&mut router, local_get(list_path)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body).as_array().expect("list").len(), 1);

    let resume_path = format!("/api/sessions/{}/snapshot/resume", session.id);
    for _ in 0..2 {
        let (status, _, body) = call(&mut router, local_post(&resume_path, None)).await;
        assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    }
    let restored = db
        .get_session(&session.id)
        .await
        .expect("read")
        .expect("exists");
    assert_eq!(restored.model, "model-before");
    assert_eq!(
        restored.messages.len(),
        1,
        "resume must not duplicate messages"
    );

    let delete_path = format!("/api/sessions/snapshots/{}", session.id);
    let (status, _, body) = call(&mut router, local_delete(&delete_path)).await;
    assert_eq!(status, StatusCode::OK, "{}", json_body(&body));
    let (status, _, body) = call(&mut router, local_post(&resume_path, None)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_body(&body)["code"], "SNAPSHOT_NOT_FOUND");
}

#[tokio::test]
async fn invalid_snapshot_id_is_rejected_without_filesystem_traversal() {
    let mut router = build_router(AppState::for_tests());
    let (status, _, body) = call(
        &mut router,
        local_post("/api/sessions/%2E%2E%2Fsnapshot/resume", None),
    )
    .await;
    assert!(
        matches!(status, StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND),
        "{}",
        json_body(&body)
    );
}
