//! Browser replay is an explicit persistent-session surface, separate from temporary Run evidence.
mod common;
use axum::http::{Method, StatusCode};
use common::{call, json_body, local_get, local_with_headers};
use serde_json::json;
use zk_server::{config::Config, routes::build_router, state::AppState};

fn fixture() -> (std::path::PathBuf, AppState) {
    let root = std::env::temp_dir().join(format!("zk-replay-api-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let root = root.canonicalize().unwrap();
    let mut config = Config::test_config();
    config.workspace_default_root = root.to_str().unwrap().into();
    config.workspace_allowed_roots = vec![root.clone()];
    (
        root,
        AppState::new(zk_db::Db::open_in_memory().unwrap(), config),
    )
}

#[tokio::test]
async fn replay_requires_exact_session_and_only_acknowledges_real_deletion() {
    let (root, state) = fixture();
    let session = state
        .db
        .create_session("fixture", root.to_str().unwrap())
        .await
        .unwrap();
    let frames = json!([{"sessionId":session.id,"snapshotId":"one","title":"private-replay"}]);
    state.browser_replay.insert(&session.id, &frames).unwrap();
    let mut router = build_router(state.clone());
    let url = format!("/api/browser/replay/{}", session.id);
    assert_eq!(
        call(&mut router, local_get(&url)).await.0,
        StatusCode::BAD_REQUEST
    );
    for method in [Method::GET, Method::DELETE] {
        let (status, _, body) = call(
            &mut router,
            local_with_headers(&url, method, None, &[("X-Session-Id", "foreign")]),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(!String::from_utf8_lossy(&body).contains("private-replay"));
    }
    let (status, _, body) = call(
        &mut router,
        local_with_headers(&url, Method::GET, None, &[("X-Session-Id", &session.id)]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body), frames);
    let (status, _, body) = call(
        &mut router,
        local_with_headers(&url, Method::DELETE, None, &[("X-Session-Id", &session.id)]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_body(&body)["status"], "deleted");
    assert!(state.browser_replay.get(&session.id).unwrap().is_none());
    let (status, _, body) = call(
        &mut router,
        local_with_headers(&url, Method::DELETE, None, &[("X-Session-Id", &session.id)]),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_body(&body)["code"], "REPLAY_NOT_FOUND");
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn temporary_sessions_cannot_read_delete_or_create_disk_replay() {
    let (root, state) = fixture();
    let (session, _lease) = state
        .db
        .create_ephemeral_session("fixture", root.to_str().unwrap(), "DONT_ASK")
        .await
        .unwrap();
    let mut router = build_router(state.clone());
    let url = format!("/api/browser/replay/{session}");
    for method in [Method::GET, Method::DELETE] {
        let (status, _, body) = call(
            &mut router,
            local_with_headers(&url, method, None, &[("X-Session-Id", &session)]),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json_body(&body)["code"], "EPHEMERAL_OPERATION_UNSUPPORTED");
    }
    let registry = zk_server::command::CommandRegistry::with_builtin_commands();
    let context =
        zk_server::command::CommandContext::of(&session, root.to_str().unwrap(), "fixture", state);
    let result = registry
        .find_command("browser-snapshot")
        .unwrap()
        .execute("", &context)
        .await;
    match result {
        zk_server::command::CommandResult::Error(error) => {
            assert!(error.starts_with("EPHEMERAL_OPERATION_UNSUPPORTED"));
        }
        other => panic!("unexpected {other:?}"),
    }
    assert!(!root.join(".zk/browser-replay").exists());
    std::fs::remove_dir_all(root).unwrap();
}
