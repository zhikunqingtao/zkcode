//! Object-authorized and idempotent Run cancellation tests.

mod common;

use axum::http::{Method, StatusCode};
use common::{call, json_body, local_with_headers};
use tokio_util::sync::CancellationToken;
use zk_db::{CleanupStatus, CommitTaskResult, ResultStatus, TaskStatus, VerificationStatus};
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
    let run_id = uuid::Uuid::new_v4().to_string();
    db.start_run(
        &run_id,
        &session.id,
        None,
        Some(zk_db::run::AGENT_TYPE_QUERY),
        "model",
    )
    .await
    .expect("run");
    let token = CancellationToken::new();
    let _execution = state
        .task_runtime()
        .attach_existing_execution(&session.id, &run_id, &run_id, token.clone())
        .await
        .expect("attach real execution token");
    let mut router = build_router(state);
    let path = format!("/api/runs/{run_id}/cancel");

    let unauthorized =
        local_with_headers(&path, Method::POST, None, &[("x-session-id", &other.id)]);
    let (status, _, _) = call(&mut router, unauthorized).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let cancel = local_with_headers(&path, Method::POST, None, &[("x-session-id", &session.id)]);
    let (status, _, body) = call(&mut router, cancel).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    assert_eq!(body["cancelRequested"], true);
    assert_eq!(body["cancelled"], false);
    assert_eq!(body["alreadyTerminal"], false);
    assert_eq!(body["runStatus"], "cancelling");
    assert_eq!(body["taskStatus"], "cancelling");
    assert_eq!(body["cleanupStatus"], "pending");
    assert!(
        token.is_cancelled(),
        "router cancellation reaches execution token"
    );

    let task = db
        .find_runtime_task_by_id(&run_id)
        .await
        .expect("task query")
        .expect("task");
    assert_eq!(task.status, TaskStatus::Cancelling);
    assert!(
        db.read_task_result(&run_id, None, 0, 65_536)
            .await
            .expect("result query")
            .is_none(),
        "transport may not manufacture a result before execution cleanup"
    );
    db.commit_task_result(&CommitTaskResult {
        task_id: run_id.clone(),
        run_id: run_id.clone(),
        expected_task_version: task.version,
        status: ResultStatus::Cancelled,
        content: "execution stopped cleanly".to_owned(),
        media_type: "text/markdown".to_owned(),
        error_code: Some("USER_CANCELLED".to_owned()),
        cleanup_status: CleanupStatus::NotRequired,
        verification_status: VerificationStatus::NotRequested,
    })
    .await
    .expect("terminal execution transaction");

    let again = local_with_headers(&path, Method::POST, None, &[("x-session-id", &session.id)]);
    let (status, _, body) = call(&mut router, again).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    assert_eq!(body["cancelRequested"], false);
    assert_eq!(body["cancelled"], true);
    assert_eq!(body["alreadyTerminal"], true);
    assert_eq!(body["runStatus"], "cancelled");
    assert_eq!(body["taskStatus"], "cancelled");
    assert_eq!(body["cleanupStatus"], "notRequired");
    let result = db
        .read_task_result(&run_id, None, 0, 65_536)
        .await
        .expect("result query")
        .expect("immutable result");
    assert_eq!(result.result.status, ResultStatus::Cancelled);
}
