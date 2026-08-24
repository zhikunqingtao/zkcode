//! Object-authorized and idempotent Run cancellation tests.

mod common;

use axum::http::{Method, StatusCode};
use common::{call, json_body, local_with_headers};
use zk_server::routes::build_router;
use zk_server::state::AppState;

#[tokio::test]
async fn cancel_is_object_authorized_and_idempotent() {
    let state = AppState::for_tests();
    let db = state.db.clone();
    let session = db.create_session("model", "/tmp").await.expect("session");
    let other = db
        .create_session("model", "/tmp")
        .await
        .expect("other session");
    db.start_run(
        "run-cancel-api",
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "model",
    )
    .await
    .expect("run");
    let mut router = build_router(state);
    let path = "/api/runs/run-cancel-api/cancel";

    let unauthorized = local_with_headers(path, Method::POST, None, &[("x-session-id", &other.id)]);
    let (status, _, _) = call(&mut router, unauthorized).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let cancel = local_with_headers(path, Method::POST, None, &[("x-session-id", &session.id)]);
    let (status, _, body) = call(&mut router, cancel).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    assert_eq!(body["cancelled"], true);
    assert_eq!(body["alreadyTerminal"], false);

    let again = local_with_headers(path, Method::POST, None, &[("x-session-id", &session.id)]);
    let (status, _, body) = call(&mut router, again).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    assert_eq!(body["cancelled"], false);
    assert_eq!(body["alreadyTerminal"], true);
}
