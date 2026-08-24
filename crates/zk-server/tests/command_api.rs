//! REST command catalog contract tests.

mod common;

use axum::http::StatusCode;
use common::{app, call, json_body, local_get};

#[tokio::test]
async fn command_catalog_and_detail_share_the_runtime_registry() {
    let mut router = app();
    let (status, _, body) = call(&mut router, local_get("/api/commands")).await;
    assert_eq!(status, StatusCode::OK);
    let list = json_body(&body);
    let commands = list.as_array().expect("command list");
    assert!(!commands.is_empty());
    assert!(commands.iter().any(|item| item["name"] == "help"));

    let (status, _, body) = call(&mut router, local_get("/api/commands/help")).await;
    assert_eq!(status, StatusCode::OK);
    let detail = json_body(&body);
    assert_eq!(detail["name"], "help");
    assert!(
        detail["description"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert!(detail["commandType"].as_str().is_some());
}

#[tokio::test]
async fn unknown_command_has_stable_error_code() {
    let mut router = app();
    let (status, _, body) = call(&mut router, local_get("/api/commands/not-a-command")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_body(&body)["code"], "COMMAND_NOT_FOUND");
}
