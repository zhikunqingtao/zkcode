//! Actual mcp-stdio binary pipe against the authenticated local Rust router.
use serde_json::{Value, json};
use std::{net::SocketAddr, process::Stdio, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use zk_server::{config::Config, routes::build_router, state::AppState};

#[tokio::test]
async fn actual_stdio_process_reads_project_and_eof_waits_for_durable_cleanup() {
    exercise(false).await;
}
#[tokio::test]
async fn requested_stdio_capabilities_stay_closed_until_real_local_approval() {
    exercise(true).await;
}
#[allow(
    clippy::too_many_lines,
    reason = "One real STDIO process must retain its identity through initialization, approval and EOF cleanup."
)]
async fn exercise(request_write: bool) {
    let root = std::env::temp_dir().join(format!("zk-mcp-stdio-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let root = root.canonicalize().unwrap();
    std::fs::write(root.join("source.txt"), "stdio fixture body").unwrap();
    let db = zk_db::Db::open(root.join("runtime.sqlite")).unwrap();
    let mut config = Config::test_config();
    config.workspace_default_root = root.to_str().unwrap().into();
    config.mcp_registry_path = root.join("mcp.json");
    let state = AppState::new(db.clone(), config);
    state
        .set_startup_epoch(db.begin_runtime_startup_epoch().await.unwrap())
        .unwrap();
    let _engine = zk_server::engine_bridge::wire_engine(&state);
    let token_file = root.join("local-token");
    std::fs::write(&token_file, state.access_tokens.token()).unwrap();
    let project = db
        .create_project("STDIO fixture", root.to_str().unwrap())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            build_router(state).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    // The canonical subcommand must remain the first argument.
    let capability_args = if request_write {
        vec!["--request-capabilities", "write"]
    } else {
        vec![]
    };
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_zk-server"))
        .arg("mcp-stdio")
        .args(capability_args)
        .arg("--project-id")
        .arg(project.id)
        .arg("--server-url")
        .arg(format!("http://{address}"))
        .arg("--token-file")
        .arg(&token_file)
        .env_remove("ZK_PORT")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    for (id, method, params) in [
        (
            1,
            "initialize",
            json!({"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}),
        ),
        (2, "tools/list", json!({})),
        (
            3,
            "tools/call",
            json!({"name":"Read","arguments":{"file_path":root.join("source.txt")}}),
        ),
    ] {
        let bytes =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
                .unwrap();
        // A frame split across reads must survive an independently completed heartbeat.
        input.write_all(&bytes[..bytes.len() / 2]).await.unwrap();
        input.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        input.write_all(&bytes[bytes.len() / 2..]).await.unwrap();
        input.write_all(b"\n").await.unwrap();
        input.flush().await.unwrap();
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(35), output.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let message: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(message["id"], id);
        if request_write && id == 3 {
            // A durable candidate question pauses the same Run until answered.
            // Native commands cannot bypass that lifecycle via a second request.
            assert_eq!(message["error"]["code"], -32003, "{message}");
            assert!(
                message["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("CONTEXT_INACTIVE")
            );
            continue;
        }
        assert!(message["error"].is_null(), "{message}");
        if id == 2 {
            assert!(
                message["result"]["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|tool| tool["name"] != "Bash" && tool["name"] != "Write")
            );
        }
        if id == 3 {
            assert!(
                message.to_string().contains("stdio fixture body"),
                "{message}"
            );
        }
    }
    if request_write {
        let (id,session,version,prompt)=db.with_reader(|connection|Ok(connection.query_row("SELECT interaction_id,session_id,version,prompt_json FROM interaction_requests WHERE type='elicitation' AND status='pending'",[],|row|Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,i64>(2)?,row.get::<_,String>(3)?)))?)).await.unwrap();
        let prompt: Value = serde_json::from_str(&prompt).unwrap();
        let token = std::fs::read_to_string(&token_file).unwrap();
        let response=reqwest::Client::builder().no_proxy().build().unwrap().post(format!("http://{address}/api/interactions/{id}/decisions")).bearer_auth(token.trim()).header("x-session-id",session).json(&json!({"expectedVersion":version,"decision":"answer","response":prompt["options"][0]["value"]})).send().await.unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().await.unwrap()
        );
        input
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/list\"}\n")
            .await
            .unwrap();
        input.flush().await.unwrap();
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(10), output.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let message: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(message["id"], 4);
        let tools = message["result"]["tools"].as_array().unwrap();
        assert!(tools.iter().any(|tool| tool["name"] == "Write"));
        assert!(!tools.iter().any(|tool| tool["name"] == "Bash"));
        let read = json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"Read","arguments":{"file_path":root.join("source.txt")}}});
        input
            .write_all(format!("{read}\n").as_bytes())
            .await
            .unwrap();
        input.flush().await.unwrap();
        line.clear();
        tokio::time::timeout(Duration::from_secs(10), output.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let result: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(result["id"], 5);
        assert!(result["error"].is_null(), "{result}");
        assert!(
            result.to_string().contains("stdio fixture body"),
            "{result}"
        );
    }
    drop(input);
    let result = tokio::time::timeout(Duration::from_secs(35), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let (status, cleanup, results) = db
        .with_reader(|connection| {
            Ok((
                connection.query_row(
                    "SELECT status FROM run_envelopes ORDER BY created_at DESC LIMIT 1",
                    [],
                    |row| row.get::<_, String>(0),
                )?,
                connection.query_row(
                    "SELECT cleanup_status FROM run_envelopes ORDER BY created_at DESC LIMIT 1",
                    [],
                    |row| row.get::<_, String>(0),
                )?,
                connection.query_row("SELECT count(*) FROM task_results", [], |row| {
                    row.get::<_, i64>(0)
                })?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(status, "cancelled");
    assert_eq!(cleanup, "confirmed");
    assert_eq!(results, 1);
    server.abort();
    let _ = server.await;
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}
