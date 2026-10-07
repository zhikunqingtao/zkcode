//! Real bound hook editor, CAS, validation and no-execution REST contracts.
mod common;
use axum::http::{Method, StatusCode};
use common::{call, json_body, local_with_headers};
use serde_json::json;

async fn fixture() -> (axum::Router, zk_db::Db, String, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("zk-hook-editor-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let root = root.canonicalize().unwrap();
    let db = zk_db::Db::open_in_memory().unwrap();
    let session = db
        .create_session("fixture", root.to_str().unwrap())
        .await
        .unwrap();
    let state =
        zk_server::state::AppState::new(db.clone(), zk_server::config::Config::test_config());
    (zk_server::routes::build_router(state), db, session.id, root)
}
async fn request(
    router: &mut axum::Router,
    id: &str,
    method: Method,
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let (status, _, bytes) = call(
        router,
        local_with_headers(
            &format!("/api/sessions/{id}/hooks"),
            method,
            body.map(|value| value.to_string()),
            &[("X-Session-Id", id)],
        ),
    )
    .await;
    (status, json_body(&bytes))
}

#[tokio::test]
async fn confirmed_hook_editor_uses_real_config_and_cas_without_running_commands() {
    let (mut router, _, id, root) = fixture().await;
    let (status, initial) = request(&mut router, &id, Method::GET, None).await;
    assert_eq!(status, StatusCode::OK, "{initial}");
    assert_eq!(initial["revision"], "absent");
    let content =
        "[[hook]]\nname = 'sentinel'\nevent = 'RUN_START'\ncommand = 'touch MUST_NOT_EXECUTE'\n";
    let body = json!({"content":content,"revision":"absent","confirmed":true});
    let (status, saved) = request(&mut router, &id, Method::PUT, Some(body.clone())).await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    assert_eq!(saved["hookCount"], 1);
    assert_ne!(saved["revision"], "absent");
    assert_eq!(
        std::fs::read_to_string(root.join(".zk/hooks.toml")).unwrap(),
        content
    );
    assert_eq!(
        zk_engine::hook::HookRegistry::try_load_from_dir(&root)
            .unwrap()
            .len(),
        1
    );
    assert!(!root.join("MUST_NOT_EXECUTE").exists());
    let (status, conflict) = request(&mut router, &id, Method::PUT, Some(body)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");
    assert_eq!(conflict["code"], "HOOK_CONFIG_CHANGED");
    let invalid = format!("{content}matcher = '['\n");
    let (status, _) = request(
        &mut router,
        &id,
        Method::PUT,
        Some(json!({"content":invalid,"revision":saved["revision"],"confirmed":true})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        std::fs::read_to_string(root.join(".zk/hooks.toml")).unwrap(),
        content
    );
    let (_, loaded) = request(&mut router, &id, Method::GET, None).await;
    assert_eq!(loaded, saved);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn hook_editor_requires_confirmation_scope_and_persistent_session() {
    let (mut router, db, id, root) = fixture().await;
    let (status, _) = request(
        &mut router,
        &id,
        Method::PUT,
        Some(json!({"content":"","revision":"absent","confirmed":false})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _, _) = call(
        &mut router,
        local_with_headers(
            &format!("/api/sessions/{id}/hooks"),
            Method::GET,
            None,
            &[("X-Session-Id", "other")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (temporary, _lease) = db
        .create_ephemeral_session("fixture", root.to_str().unwrap(), "DONT_ASK")
        .await
        .unwrap();
    let (status, body) = request(
        &mut router,
        &temporary,
        Method::PUT,
        Some(json!({"content":"","revision":"absent","confirmed":true})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "EPHEMERAL_OPERATION_UNSUPPORTED");
    assert!(!root.join(".zk").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn unsafe_or_malformed_existing_config_is_not_silently_replaced() {
    let (mut router, _, id, root) = fixture().await;
    std::fs::create_dir(root.join(".zk")).unwrap();
    let file = root.join(".zk/hooks.toml");
    std::fs::write(&file, "invalid TOML [").unwrap();
    let (status, loaded) = request(&mut router, &id, Method::GET, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(loaded["validationError"], "HOOK_CONFIG_INVALID");
    let (status, _) = request(
        &mut router,
        &id,
        Method::PUT,
        Some(json!({"content":"","revision":loaded["revision"],"confirmed":true})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    std::fs::remove_file(&file).unwrap();
    std::os::unix::fs::symlink(root.join("outside.toml"), &file).unwrap();
    let (status, _) = request(
        &mut router,
        &id,
        Method::PUT,
        Some(json!({"content":"","revision":"absent","confirmed":true})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(!root.join("outside.toml").exists());
    std::fs::remove_dir_all(root).unwrap();
}
