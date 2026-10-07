//! Native external execution uses the host Engine, real local decisions and immutable replay.
mod common;
use axum::{
    Router,
    http::{Method, StatusCode},
};
use common::{call, json_body, local_post, local_with_headers};
use serde_json::{Value, json};
use std::time::Duration;
use zk_server::{config::Config, routes::build_router, state::AppState};

struct Fixture {
    state: AppState,
    app: Router,
    root: std::path::PathBuf,
    session: String,
    run: String,
    token: String,
}
impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("zk-mcp-engine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let db = zk_db::Db::open(root.join("runtime.sqlite")).unwrap();
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
            .create_project("external native fixture", root.to_str().unwrap())
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
        let context = json_body(&response.2);
        Self {
            state,
            app,
            root,
            session: context["sessionId"].as_str().unwrap().into(),
            run: context["runId"].as_str().unwrap().into(),
            token: context["contextToken"].as_str().unwrap().into(),
        }
    }
    async fn rpc(&self, name: &str, input: Value, operation: &str) -> Value {
        let mut app = self.app.clone();
        let response=call(&mut app,local_with_headers("/mcp",Method::POST,Some(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":input,"_meta":{"operationId":operation}}}).to_string()),&[("x-session-id",&self.session),("x-run-id",&self.run),("x-mcp-context-token",&self.token)])).await;
        assert_eq!(response.0, StatusCode::OK);
        json_body(&response.2)
    }
    async fn approve_candidates(&self, requested: Value) {
        let mut app = self.app.clone();
        let headers = [
            ("x-session-id", self.session.as_str()),
            ("x-run-id", self.run.as_str()),
            ("x-mcp-context-token", self.token.as_str()),
        ];
        let path = format!("/api/mcp/contexts/{}/capabilities", self.run);
        let response = call(
            &mut app,
            local_with_headers(
                &format!("{path}/requests"),
                Method::POST,
                Some(requested.to_string()),
                &headers,
            ),
        )
        .await;
        assert_eq!(
            response.0,
            StatusCode::ACCEPTED,
            "{}",
            String::from_utf8_lossy(&response.2)
        );
        let id = json_body(&response.2)["pendingRequestId"]
            .as_str()
            .unwrap()
            .to_owned();
        let record = self
            .state
            .authz
            .interactions
            .find_by_id(&id)
            .await
            .unwrap()
            .unwrap();
        let prompt: Value = serde_json::from_str(&record.prompt_json).unwrap();
        let response=call(&mut app,local_with_headers(&format!("/api/interactions/{id}/decisions"),Method::POST,Some(json!({"expectedVersion":record.version,"decision":"answer","response":prompt["options"][0]["value"]}).to_string()),&[("x-session-id",&self.session)])).await;
        assert_eq!(
            response.0,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&response.2)
        );
        let response = call(
            &mut app,
            local_with_headers(&path, Method::GET, None, &headers),
        )
        .await;
        assert_eq!(response.0, StatusCode::OK);
        for flag in ["write", "process", "network"] {
            if requested[flag] == true {
                assert_eq!(json_body(&response.2)["ceiling"][flag], true);
            }
        }
    }
    async fn approve_operation(&self, record: zk_protocol::InteractionView) {
        // Simulate the actual delivery owner and matching browser ACK, as in the
        // interaction lifecycle suite. A raw pending view is not an approval token.
        let service = &self.state.authz.interactions;
        assert!(
            service
                .mark_dispatched(&record.interaction_id, "external-mcp-browser")
                .await
                .unwrap()
        );
        let dispatched = service
            .find_by_id(&record.interaction_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            service
                .acknowledge_received(
                    &record.interaction_id,
                    Some("external-mcp-browser"),
                    dispatched.delivery_generation
                )
                .await
                .unwrap()
        );
        let current = service
            .find_by_id(&record.interaction_id)
            .await
            .unwrap()
            .unwrap();
        let record =
            zk_server::interaction::service::DurableInteractionService::view(&current).unwrap();
        let mut app = self.app.clone();
        let response=call(&mut app,local_with_headers(&format!("/api/interactions/{}/decisions",record.interaction_id),Method::POST,Some(json!({"expectedVersion":record.version,"optionId":"allow_once","operationHash":record.operation_hash,"deliveryGeneration":record.delivery_generation}).to_string()),&[("x-session-id",&self.session)])).await;
        assert_eq!(
            response.0,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&response.2)
        );
    }
    async fn rpc_with_local_approval(&self, name: &str, input: Value, operation: &str) -> Value {
        let execution = self.rpc(name, input, operation);
        tokio::pin!(execution);
        let record = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    result = &mut execution => panic!("guarded operation bypassed local decision: {result}"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                let records = self.state.authz.interactions.pending_views(&self.session).await.unwrap();
                if let Some(record) = records.into_iter().find(|view|
                    view.interaction_type.as_deref().is_some_and(|kind|kind.eq_ignore_ascii_case("permission"))) {
                    break record;
                }
            }
        }).await.expect("guarded operation must request an actual local decision");
        self.approve_operation(record).await;
        tokio::time::timeout(Duration::from_secs(15), &mut execution)
            .await
            .unwrap()
    }
    async fn close(&self) {
        let mut app = self.app.clone();
        let response = call(
            &mut app,
            local_with_headers(
                &format!("/api/mcp/contexts/{}", self.run),
                Method::DELETE,
                None,
                &[
                    ("x-session-id", &self.session),
                    ("x-run-id", &self.run),
                    ("x-mcp-context-token", &self.token),
                ],
            ),
        )
        .await;
        assert_eq!(response.0, StatusCode::ACCEPTED);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let run = self
                    .state
                    .db
                    .find_run_by_id(&self.run)
                    .await
                    .unwrap()
                    .unwrap();
                if run.finished_at.is_some() {
                    assert!(
                        matches!(run.cleanup_status.as_str(), "confirmed" | "notRequired"),
                        "{run:?}"
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
#[tokio::test]
async fn approved_native_write_uses_snapshots_and_replay_never_rewrites() {
    let f = Fixture::new().await;
    let path = f.root.join("owned.txt");
    std::fs::write(&path, "original").unwrap();
    let denied = f
        .rpc(
            "Write",
            json!({"file_path":path,"content":"changed"}),
            "write-1",
        )
        .await;
    assert!(denied["error"].is_object());
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "original");
    f.approve_candidates(json!({"write":true})).await;
    let read = f.rpc("Read", json!({"file_path":path}), "read-1").await;
    assert!(read["error"].is_null(), "{read}");
    assert_eq!(read["result"]["isError"], false, "{read}");
    let first = f
        .rpc(
            "Write",
            json!({"file_path":path,"content":"changed"}),
            "write-1",
        )
        .await;
    assert!(first["error"].is_null(), "{first}");
    assert_eq!(first["result"]["isError"], false, "{first}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "changed");
    let snapshots = f.state.db.list_file_snapshots(&f.session).await.unwrap();
    assert_eq!(snapshots.len(), 1, "{snapshots:?}");
    assert_eq!(snapshots[0].content, "original");
    std::fs::write(&path, "user subsequently edited").unwrap();
    let replay = f
        .rpc(
            "Write",
            json!({"file_path":path,"content":"changed"}),
            "write-1",
        )
        .await;
    assert!(replay["error"].is_null(), "{replay}");
    assert_eq!(replay["result"]["_meta"]["replayed"], true);
    assert_eq!(
        replay["result"]["_meta"]["messageId"],
        first["result"]["_meta"]["messageId"]
    );
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "user subsequently edited"
    );
    let conflict = f
        .rpc(
            "Write",
            json!({"file_path":path,"content":"other"}),
            "write-1",
        )
        .await;
    assert!(conflict["error"].is_object(), "{conflict}");
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "user subsequently edited"
    );
    let snapshot = &snapshots[0];
    let restored = f
        .state
        .file_history
        .rewind_files(
            &f.session,
            snapshot.message_id.as_deref().unwrap(),
            Some(&[path.to_string_lossy().into_owned()]),
        )
        .await;
    assert!(restored.success, "{restored:?}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "original");
    f.close().await;
}
#[tokio::test]
async fn process_ceiling_still_requires_real_default_permission_and_replay_is_once() {
    let f = Fixture::new().await;
    let path = f.root.join("effect.txt");
    f.approve_candidates(json!({"write":true,"process":true,"network":true}))
        .await;
    // A later ordinary UI preference cannot widen this external coordinator.
    f.state
        .authz
        .modes
        .set_mode(&f.session, zk_authz::model::PermissionMode::AutoApprove)
        .await
        .unwrap();
    let input = json!({"command":"python3 -c 'open(\"effect.txt\", \"a\").write(\"once\\n\")'"});
    let execution = f.rpc("Bash", input.clone(), "process-1");
    tokio::pin!(execution);
    let record=tokio::time::timeout(Duration::from_secs(10),async {loop {
        tokio::select! {result=&mut execution=>panic!("Bash bypassed the expected DEFAULT permission: {result}"),()=tokio::time::sleep(Duration::from_millis(10))=>{}}
        let records=f.state.authz.interactions.pending_views(&f.session).await.unwrap();if let Some(view)=records.into_iter().find(|view|view.interaction_type.as_deref().is_some_and(|kind|kind.eq_ignore_ascii_case("permission"))) {break view;}
    }}).await.unwrap();
    assert!(
        !path.exists(),
        "no process effect before the actual permission answer"
    );
    f.approve_operation(record).await;
    let result = tokio::time::timeout(Duration::from_secs(15), &mut execution)
        .await
        .unwrap();
    assert_eq!(result["result"]["isError"], false, "{result}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "once\n");
    let replay = f.rpc("Bash", input, "process-1").await;
    assert_eq!(replay["result"]["_meta"]["replayed"], true, "{replay}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "once\n");
    f.close().await;
}

#[tokio::test]
async fn write_only_mcp_cannot_turn_workspace_hooks_into_process_or_http_authority() {
    let f = Fixture::new().await;
    f.approve_candidates(json!({"write":true})).await;
    std::fs::create_dir_all(f.root.join(".zk")).unwrap();
    std::fs::write(f.root.join("read.txt"), "safe read").unwrap();
    let marker = f.root.join("hook-side-effect.txt");
    let command = format!(
        "printf x >> '{}'; printf '%s' '{{\"decision\":\"continue\"}}'",
        marker.display()
    );
    let config = format!(
        "[[hook]]\nname = \"external-fixture\"\nevent = \"PRE_TOOL_USE\"\nrole = \"security\"\nmatcher = \"^Read$\"\ncommand = {}\n",
        serde_json::to_string(&command).unwrap()
    );
    let written = f
        .rpc_with_local_approval(
            "Write",
            json!({"file_path":f.root.join(".zk/hooks.toml"),"content":config}),
            "write-hook",
        )
        .await;
    assert_eq!(written["result"]["isError"], false, "{written}");
    let denied = f
        .rpc(
            "Read",
            json!({"file_path":f.root.join("read.txt")}),
            "read-hook-denied",
        )
        .await;
    assert_eq!(denied["result"]["isError"], true, "{denied}");
    assert!(
        denied
            .to_string()
            .contains("HOOK_EXTERNAL_CAPABILITY_DENIED"),
        "{denied}"
    );
    assert!(!marker.exists());
    // A subsequent local capability approval applies to a new operation only.
    f.approve_candidates(json!({"write":true,"process":true,"network":true}))
        .await;
    let accepted = f
        .rpc_with_local_approval(
            "Read",
            json!({"file_path":f.root.join("read.txt")}),
            "read-hook-accepted",
        )
        .await;
    assert_eq!(accepted["result"]["isError"], false, "{accepted}");
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");
    let replay = f
        .rpc(
            "Read",
            json!({"file_path":f.root.join("read.txt")}),
            "read-hook-accepted",
        )
        .await;
    assert_eq!(replay["result"]["_meta"]["replayed"], true, "{replay}");
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");
    f.close().await;
}

#[tokio::test]
async fn preconfigured_http_hook_requires_external_network_approval_before_dispatch() {
    let f = Fixture::new().await;
    std::fs::create_dir_all(f.root.join(".zk")).unwrap();
    std::fs::write(f.root.join("read.txt"), "safe read").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = format!(
        "[[hook]]\nname = \"http-fixture\"\nevent = \"PRE_TOOL_USE\"\nrole = \"notification\"\nurl = \"http://{}/\"\n",
        listener.local_addr().unwrap()
    );
    std::fs::write(f.root.join(".zk/hooks.toml"), config).unwrap();
    let denied = f
        .rpc(
            "Read",
            json!({"file_path":f.root.join("read.txt")}),
            "read-http-denied",
        )
        .await;
    assert_eq!(
        denied["result"]["isError"], false,
        "optional notification denial must preserve the authorized read: {denied}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
    f.close().await;
}

#[tokio::test]
async fn external_post_hooks_preserve_failed_tool_facts_with_or_without_capabilities() {
    let f = Fixture::new().await;
    std::fs::create_dir_all(f.root.join(".zk")).unwrap();
    let marker = f.root.join("post-side-effect.txt");
    let command = format!(
        "printf x >> '{}'; printf '%s' '{{\"presentation\":\"PRESENTATION_CANARY\",\"isError\":false}}'",
        marker.display()
    );
    let config = format!(
        "[[hook]]\nname = \"post-fixture\"\nevent = \"POST_TOOL_USE\"\nrole = \"presentation\"\ncommand = {}\n",
        serde_json::to_string(&command).unwrap()
    );
    std::fs::write(f.root.join(".zk/hooks.toml"), config).unwrap();
    let missing = json!({"file_path":f.root.join("missing.txt")});
    let first = f.rpc("Read", missing.clone(), "post-denied").await;
    assert_eq!(first["result"]["isError"], true, "{first}");
    assert!(!first.to_string().contains("PRESENTATION_CANARY"));
    assert!(!marker.exists());
    f.approve_candidates(json!({"write":true,"process":true,"network":true}))
        .await;
    let second = f
        .rpc_with_local_approval("Read", missing, "post-allowed")
        .await;
    assert_eq!(second["result"]["isError"], true, "{second}");
    assert_eq!(first["result"]["content"], second["result"]["content"]);
    assert!(!second.to_string().contains("PRESENTATION_CANARY"));
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");
    f.close().await;
}
