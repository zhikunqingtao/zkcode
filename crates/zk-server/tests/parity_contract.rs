//! WP-00 safety-freeze integration gates against the real Axum router.

mod common;

use axum::http::StatusCode;
use common::{app, call, json_body, local_get, local_post};

#[tokio::test]
async fn unfinished_swarm_is_fail_closed() {
    let mut router = app();
    let (status, _headers, body) = call(
        &mut router,
        local_post(
            "/api/swarm",
            Some(r#"{"teamName":"parity-freeze","sessionId":"none"}"#.to_owned()),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let body = json_body(&body);
    assert_eq!(body["code"], "FEATURE_NOT_READY");
    assert!(body["message"].as_str().expect("message").contains("Swarm"));

    for path in [
        "/api/swarm",
        "/api/swarm/frozen/shutdown",
        "/api/swarm/frozen/force-stop",
        "/api/swarm/frozen/worker/worker-1/abort",
    ] {
        let request = if path == "/api/swarm" {
            local_get(path)
        } else {
            local_post(path, None)
        };
        let (status, _, body) = call(&mut router, request).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert_eq!(json_body(&body)["code"], "FEATURE_NOT_READY");
    }
}

#[tokio::test]
async fn unfinished_agent_and_worktree_tools_are_not_exposed() {
    let mut router = app();
    let (status, _headers, body) = call(&mut router, local_get("/api/tools")).await;
    assert_eq!(status, StatusCode::OK);

    let body = json_body(&body);
    let names: Vec<&str> = body["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .map(|item| item["name"].as_str().expect("tool name"))
        .collect();
    for frozen in [
        "Agent",
        "TaskCreate",
        "TaskGet",
        "TaskList",
        "TaskStop",
        "TaskUpdate",
        "Worktree",
    ] {
        assert!(!names.contains(&frozen), "{frozen} must be fail-closed");
    }
}
