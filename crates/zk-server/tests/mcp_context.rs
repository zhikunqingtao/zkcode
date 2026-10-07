//! Real durable external context, workspace ceiling, revocation and cleanup.
mod common;
use axum::http::{Method, StatusCode};
use common::{call, json_body, local_post, local_with_headers};
use serde_json::{Value, json};
use zk_server::{config::Config, routes::build_router, state::AppState};

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "One external context lifecycle must prove admission, exact scope, revocation and cleanup together."
)]
async fn external_context_has_dedicated_default_session_and_cannot_widen_or_forge_scope() {
    let root = std::env::temp_dir().join(format!("zk-external-mcp-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let root = root.canonicalize().unwrap();
    std::fs::write(root.join("hello.txt"), "workspace owned content").unwrap();
    let db = zk_db::Db::open(root.join("audit.sqlite")).unwrap();
    let mut config = Config::test_config();
    config.workspace_default_root = root.to_string_lossy().into_owned();
    config.mcp_registry_path = root.join("mcp.json");
    config.snapshot_dir = Some(root.join("snapshots"));
    let state = AppState::new(db.clone(), config);
    state
        .set_startup_epoch(db.begin_runtime_startup_epoch().await.unwrap())
        .unwrap();
    let _engine = zk_server::engine_bridge::wire_engine(&state);
    let project = db
        .create_project("fixture", root.to_str().unwrap())
        .await
        .unwrap();
    let mut app = build_router(state.clone());
    let response = call(
        &mut app,
        local_post(
            "/api/mcp/contexts",
            Some(json!({"projectId":project.id}).to_string()),
        ),
    )
    .await;
    assert_eq!(
        response.0,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&response.2)
    );
    let context: Value = json_body(&response.2);
    let run = context["runId"].as_str().unwrap();
    let session = context["sessionId"].as_str().unwrap();
    let token = context["contextToken"].as_str().unwrap();
    assert_eq!(context["permissionMode"], "DEFAULT");
    let session_owned = session.to_owned();
    let permission = db
        .with_reader(move |connection| {
            Ok(connection.query_row(
                "SELECT permission_mode FROM sessions WHERE id=?1",
                [session_owned],
                |row| row.get::<_, String>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(permission, "DEFAULT");
    assert_session_purpose(&mut app, session, "mcp").await;
    let ordinary = db
        .create_session("ordinary", root.to_str().unwrap())
        .await
        .unwrap();
    db.with_writer({
        let id = ordinary.id.clone();
        move |conn| {
            conn.execute(
                "UPDATE sessions SET title='MCP',metadata_json='{\"purpose\":\"mcp\"}' WHERE id=?1",
                [id],
            )?;
            Ok(())
        }
    })
    .await
    .unwrap();
    assert_session_purpose(&mut app, &ordinary.id, "chat").await;

    let headers = [
        ("x-session-id", session),
        ("x-run-id", run),
        ("x-mcp-context-token", token),
    ];
    let request = |method: &str, params: Value| {
        json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string()
    };
    let response = call(
        &mut app,
        local_with_headers(
            "/mcp",
            Method::POST,
            Some(request("tools/list", json!({}))),
            &headers,
        ),
    )
    .await;
    let tools: Value = json_body(&response.2);
    let names = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(names.contains(&"Read"));
    assert!(names.contains(&"LSP"));
    assert!(!names.contains(&"Bash"));
    assert!(!names.contains(&"Write"));
    let response = call(
        &mut app,
        local_with_headers(
            "/mcp",
            Method::POST,
            Some(request(
                "tools/call",
                json!({"name":"Read","arguments":{"file_path":root.join("hello.txt")}}),
            )),
            &headers,
        ),
    )
    .await;
    let output: Value = json_body(&response.2);
    assert!(output["error"].is_null(), "{output}");
    assert!(
        output.to_string().contains("workspace owned content"),
        "{output}"
    );
    for name in ["Bash", "TaskCreate", "Write"] {
        let response = call(
            &mut app,
            local_with_headers(
                "/mcp",
                Method::POST,
                Some(request("tools/call", json!({"name":name,"arguments":{}}))),
                &headers,
            ),
        )
        .await;
        assert!(json_body(&response.2)["error"].is_object());
    }
    let forged = [("x-session-id", session), ("x-run-id", run)];
    let response = call(
        &mut app,
        local_with_headers(
            "/mcp",
            Method::POST,
            Some(request(
                "tools/call",
                json!({"name":"Read","arguments":{"file_path":root.join("hello.txt")}}),
            )),
            &forged,
        ),
    )
    .await;
    assert!(json_body(&response.2)["error"].is_object());
    let response = call(
        &mut app,
        local_with_headers(
            &format!("/api/mcp/contexts/{run}"),
            Method::DELETE,
            None,
            &headers,
        ),
    )
    .await;
    if response.0 != StatusCode::ACCEPTED {
        let retry = state
            .task_runtime()
            .cancel_run_with_cause(
                run,
                zk_db::run::EXIT_USER_CANCELLED,
                "fixture diagnostic retry",
            )
            .await;
        panic!("close status {}; retry: {:?}", response.0, retry);
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let record = db.find_run_by_id(run).await.unwrap().unwrap();
            if record.status != "running" && record.status != "cancelling" {
                assert_eq!(record.cleanup_status, "confirmed");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let run_owned = run.to_owned();
    let count = db
        .with_reader(move |connection| {
            Ok(connection.query_row(
                "SELECT count(*) FROM task_results WHERE run_id=?1",
                [run_owned],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(count, 1);
    // Purpose survives shutdown; a service transcript must never become an ordinary
    // merge source or Query target merely because its connection ended.
    assert_session_purpose(&mut app, session, "mcp").await;
    assert!(db.require_conversation_session(session).await.is_err());
    let merged = call(&mut app, local_with_headers("/api/sessions/merge", Method::POST,
        Some(json!({"sourceSessionIds":[session,ordinary.id],"primarySessionId":session,"title":"must reject"}).to_string()),
        &[("Idempotency-Key", "mcp-purpose-merge")])).await;
    assert!(!merged.0.is_success());
    assert!(
        String::from_utf8_lossy(&merged.2).contains("MCP_SESSION_DEDICATED"),
        "{}",
        String::from_utf8_lossy(&merged.2)
    );

    let response = call(
        &mut app,
        local_with_headers(
            "/mcp",
            Method::POST,
            Some(request("tools/call", json!({"name":"Read","arguments":{}}))),
            &headers,
        ),
    )
    .await;
    assert_eq!(response.0, StatusCode::FORBIDDEN);
    drop(app);
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "One pending request exercises untrusted self-approval, local decision CAS, expiry and cleanup."
)]
async fn candidate_ceiling_requires_real_local_decision_and_cannot_be_self_approved() {
    let state = AppState::for_tests();
    let db = state.db.clone();
    let project = db
        .create_project(
            "MCP approval fixture",
            std::fs::canonicalize("/tmp").unwrap().to_str().unwrap(),
        )
        .await
        .unwrap();
    let mut app = build_router(state);
    let response = call(
        &mut app,
        local_post(
            "/api/mcp/contexts",
            Some(json!({"projectId":project.id,"write":true}).to_string()),
        ),
    )
    .await;
    assert!(
        !response.0.is_success(),
        "client JSON must never directly grant a ceiling"
    );
    let response = call(
        &mut app,
        local_post(
            "/api/mcp/contexts",
            Some(json!({"projectId":project.id}).to_string()),
        ),
    )
    .await;
    assert_eq!(
        response.0,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&response.2)
    );
    let context = json_body(&response.2);
    let run = context["runId"].as_str().unwrap();
    let session = context["sessionId"].as_str().unwrap();
    let token = context["contextToken"].as_str().unwrap();
    let external = [
        ("x-session-id", session),
        ("x-run-id", run),
        ("x-mcp-context-token", token),
    ];
    let local = [("x-session-id", session)];
    let path = format!("/api/mcp/contexts/{run}/capabilities");
    let request_path = format!("{path}/requests");
    let response = call(
        &mut app,
        local_with_headers(
            &request_path,
            Method::POST,
            Some(json!({"process":true}).to_string()),
            &external,
        ),
    )
    .await;
    assert_eq!(response.0, StatusCode::BAD_REQUEST);
    let requested = json!({"write":true});
    let response = call(
        &mut app,
        local_with_headers(
            &request_path,
            Method::POST,
            Some(requested.to_string()),
            &external,
        ),
    )
    .await;
    assert_eq!(
        response.0,
        StatusCode::ACCEPTED,
        "{}",
        String::from_utf8_lossy(&response.2)
    );
    let pending = json_body(&response.2);
    assert_eq!(pending["ceiling"]["write"], false);
    let request_id = pending["pendingRequestId"].as_str().unwrap().to_owned();
    let response = call(
        &mut app,
        local_with_headers(
            &request_path,
            Method::POST,
            Some(requested.to_string()),
            &external,
        ),
    )
    .await;
    assert_eq!(
        json_body(&response.2)["pendingRequestId"],
        request_id,
        "retry must reuse the same approval"
    );
    let read = |id: String| {
        move |connection: &mut rusqlite::Connection| -> Result<(i64, String), zk_db::DbError> {
            Ok(connection.query_row(
                "SELECT version,prompt_json FROM interaction_requests WHERE interaction_id=?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        }
    };
    let (version, prompt) = db.with_reader(read(request_id.clone())).await.unwrap();
    let prompt: Value = serde_json::from_str(&prompt).unwrap();
    let choice = prompt["options"][0]["value"].as_str().unwrap();
    let decision = format!("/api/interactions/{request_id}/decisions");
    let response = call(
        &mut app,
        local_with_headers(
            &decision,
            Method::POST,
            Some(
                json!({"expectedVersion":version,"decision":"answer","response":choice})
                    .to_string(),
            ),
            &external,
        ),
    )
    .await;
    assert_eq!(
        response.0,
        StatusCode::FORBIDDEN,
        "context access token cannot approve a management decision"
    );
    let response = call(
        &mut app,
        local_with_headers(
            &decision,
            Method::POST,
            Some(
                json!({"expectedVersion":version,"decision":"answer","response":"allow"})
                    .to_string(),
            ),
            &local,
        ),
    )
    .await;
    assert_eq!(response.0, StatusCode::OK);
    let response = call(
        &mut app,
        local_with_headers(&path, Method::GET, None, &external),
    )
    .await;
    assert_eq!(
        json_body(&response.2)["ceiling"]["write"],
        false,
        "free text must not grant candidate tools"
    );
    let response = call(
        &mut app,
        local_with_headers(
            &request_path,
            Method::POST,
            Some(requested.to_string()),
            &external,
        ),
    )
    .await;
    let request_id = json_body(&response.2)["pendingRequestId"]
        .as_str()
        .unwrap()
        .to_owned();
    let (version, prompt) = db.with_reader(read(request_id.clone())).await.unwrap();
    let prompt: Value = serde_json::from_str(&prompt).unwrap();
    let response=call(&mut app,local_with_headers(&format!("/api/interactions/{request_id}/decisions"),Method::POST,Some(json!({"expectedVersion":version,"decision":"answer","response":prompt["options"][0]["value"]}).to_string()),&local)).await;
    assert_eq!(
        response.0,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&response.2)
    );
    let response = call(
        &mut app,
        local_with_headers(&path, Method::GET, None, &external),
    )
    .await;
    let approved = json_body(&response.2);
    assert_eq!(
        approved["ceiling"],
        json!({"write":true,"process":false,"network":false})
    );
    assert_eq!(approved["epoch"], 1);
    let response = call(
        &mut app,
        local_with_headers(
            &format!("/api/mcp/contexts/{run}"),
            Method::DELETE,
            None,
            &external,
        ),
    )
    .await;
    assert_eq!(response.0, StatusCode::ACCEPTED);
}

async fn assert_session_purpose(app: &mut axum::Router, id: &str, expected: &str) {
    let response = call(
        app,
        local_with_headers(&format!("/api/sessions/{id}"), Method::GET, None, &[]),
    )
    .await;
    assert_eq!(response.0, StatusCode::OK);
    assert_eq!(json_body(&response.2)["purpose"], expected);
    let response = call(
        app,
        local_with_headers("/api/sessions?limit=100", Method::GET, None, &[]),
    )
    .await;
    let list = json_body(&response.2);
    let summary = list["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == id)
        .expect("MCP session stays visible for local approval");
    assert_eq!(summary["purpose"], expected);
}
