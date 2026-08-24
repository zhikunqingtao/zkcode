//! Query API safety and shared-service contract tests.

mod common;

use axum::http::StatusCode;
use common::{call, json_body, local_post};

#[tokio::test]
async fn query_rejects_arbitrary_working_directory_before_execution() {
    let mut router = common::app();
    let (status, _, body) = call(
        &mut router,
        local_post(
            "/api/query",
            Some(r#"{"prompt":"read","workingDirectory":"/tmp"}"#.to_owned()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        json_body(&body)["code"],
        "QUERY_WORKING_DIRECTORY_FORBIDDEN"
    );
}

#[tokio::test]
async fn conversation_requires_an_existing_session() {
    let mut router = common::app();
    let (status, _, body) = call(
        &mut router,
        local_post(
            "/api/query/conversation",
            Some(r#"{"prompt":"hello"}"#.to_owned()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&body)["code"], "QUERY_SESSION_REQUIRED");
}

#[tokio::test]
async fn query_hard_limits_are_enforced() {
    let mut router = common::app();
    for (field, value, code) in [
        ("maxTurns", "5", "QUERY_MAX_TURNS_INVALID"),
        ("maxBudgetUsd", "1.01", "QUERY_BUDGET_INVALID"),
        ("timeoutSeconds", "91", "QUERY_TIMEOUT_INVALID"),
    ] {
        let body = format!(r#"{{"prompt":"hello","{field}":{value}}}"#);
        let (status, _, response) = call(&mut router, local_post("/api/query", Some(body))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{field}");
        assert_eq!(json_body(&response)["code"], code, "{field}");
    }
}

#[tokio::test]
async fn query_rejects_unknown_thinking_mode_before_scope_resolution() {
    let mut router = common::app();
    let (status, _, body) = call(
        &mut router,
        local_post(
            "/api/query",
            Some(r#"{"prompt":"hello","thinking":"maximum"}"#.to_owned()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&body)["code"], "QUERY_THINKING_MODE_INVALID");
}

#[tokio::test]
async fn valid_tool_filter_advances_to_scope_validation() {
    let mut router = common::app();
    let (status, _, body) = call(
        &mut router,
        local_post(
            "/api/query",
            Some(r#"{"prompt":"hello","allowedTools":["Read"]}"#.to_owned()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_body(&body)["code"], "QUERY_SCOPE_REQUIRED");
}
