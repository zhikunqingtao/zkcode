//! Reverse MCP JSON-RPC route wiring and transport boundary tests.

mod common;

use axum::http::{Method, StatusCode};
use common::{call, json_body, local_get, local_post, local_with_headers};

#[tokio::test]
async fn reverse_mcp_is_wired_through_the_real_router() {
    let mut app = common::app();

    let initialize = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {}
    });
    let (status, _, body) = call(&mut app, local_post("/mcp", Some(initialize.to_string()))).await;
    assert_eq!(status, StatusCode::OK);
    let body = json_body(&body);
    assert_eq!(body["result"]["serverInfo"]["name"], "zkcode");

    let list = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    });
    let (status, _, body) = call(&mut app, local_post("/mcp", Some(list.to_string()))).await;
    assert_eq!(status, StatusCode::OK);
    let tools = json_body(&body)["result"]["tools"]
        .as_array()
        .expect("tool list")
        .clone();
    assert!(tools.iter().any(|tool| tool["name"] == "ReadMcpResource"));
    assert!(tools.iter().any(|tool| tool["name"] == "Write"));
}

#[tokio::test]
async fn reverse_mcp_notifications_and_body_limit_are_enforced_by_router() {
    let mut app = common::app();
    let notification = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized",
        "params": {}
    });
    let (status, _, body) =
        call(&mut app, local_post("/mcp", Some(notification.to_string()))).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.is_empty());

    let oversized = "x".repeat(1024 * 1024 + 1);
    let (status, _, _) = call(&mut app, local_post("/mcp", Some(oversized))).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn reverse_mcp_real_read_uses_persistent_context_and_emits_metrics() {
    let root = std::env::temp_dir().join(format!("zk-reverse-mcp-real-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).expect("create temporary workspace");
    let workspace = root.canonicalize().expect("canonical workspace");
    let target = workspace.join("real-input.txt");
    std::fs::write(&target, "zkcode reverse MCP real read\n").expect("write real input");
    let workspace_text = workspace.to_string_lossy().into_owned();
    let mut config = zk_server::config::Config::test_config();
    config.workspace_default_root.clone_from(&workspace_text);
    let (mut app, db) = common::app_with_config(config);
    db.create_project("Reverse MCP Real Read", &workspace_text)
        .await
        .expect("create trusted project");
    let session = db
        .create_session("test-model", &workspace_text)
        .await
        .expect("create session");
    db.start_run(
        "reverse-mcp-real-run",
        &session.id,
        None,
        Some("query"),
        "test-model",
    )
    .await
    .expect("start run");

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "Read",
            "arguments": {"file_path": target}
        }
    });
    let (status, _, body) = call(
        &mut app,
        local_with_headers(
            "/mcp",
            Method::POST,
            Some(request.to_string()),
            &[
                ("x-session-id", &session.id),
                ("x-run-id", "reverse-mcp-real-run"),
            ],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let response = json_body(&body);
    assert!(response.get("error").is_none(), "{response}");
    assert_eq!(response["result"]["isError"], false, "{response}");
    assert!(
        response["result"]["content"][0]["text"]
            .as_str()
            .is_some_and(|text| text.contains("zkcode"))
    );

    let (status, _, body) = call(&mut app, local_get("/metrics")).await;
    assert_eq!(status, StatusCode::OK);
    let metrics = String::from_utf8(body.to_vec()).expect("metrics utf8");
    assert!(metrics.contains("zk_observability_accepted_total 2"));
    assert!(metrics.contains("zk_observability_dropped_total 0"));
    assert!(metrics.contains("zk_observability_write_failures_total 0"));
    std::fs::remove_dir_all(root).ok();
}
