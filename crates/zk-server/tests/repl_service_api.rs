//! The browser-visible service entry point preserves Session and mutation boundaries.
mod common;
use axum::http::{Method, StatusCode, header};
use common::{call, json_body, local_with_headers};
#[tokio::test]
async fn service_reads_never_start_an_interpreter_and_stop_requires_exact_session_and_origin() {
    let (mut app, db) = common::app_with_db();
    let session = db.create_session("fixture", "/tmp").await.unwrap();
    let path = format!("/api/sessions/{}/repl-service", session.id);
    for (method, expected) in [
        (Method::GET, StatusCode::OK),
        (Method::DELETE, StatusCode::ACCEPTED),
    ] {
        let response = call(
            &mut app,
            local_with_headers(&path, method, None, &[("x-session-id", &session.id)]),
        )
        .await;
        assert_eq!(
            response.0,
            expected,
            "{}",
            String::from_utf8_lossy(&response.2)
        );
        let body = json_body(&response.2);
        assert_eq!(body["state"], "absent");
        assert_eq!(body["cleanupStatus"], "notRequired");
    }
    let response = call(
        &mut app,
        local_with_headers(
            &path,
            Method::GET,
            None,
            &[("x-session-id", "another-session")],
        ),
    )
    .await;
    assert_eq!(response.0, StatusCode::FORBIDDEN);
    let mut request = local_with_headers(
        &path,
        Method::DELETE,
        None,
        &[("x-session-id", &session.id)],
    );
    request
        .headers_mut()
        .insert(header::ORIGIN, "https://untrusted.example".parse().unwrap());
    assert_eq!(call(&mut app, request).await.0, StatusCode::FORBIDDEN);
    let count = db
        .with_reader(|connection| {
            Ok(connection.query_row(
                "SELECT count(*) FROM tasks WHERE task_type='repl'",
                [],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(count, 0);
}
