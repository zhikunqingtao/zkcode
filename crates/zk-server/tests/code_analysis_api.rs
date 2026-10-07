//! Actual Rust authorization and `camelCase/snake_case` adaptation over Unix HTTP.
mod common;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use common::{app_with_config, call, json_body, local_delete, local_get, local_post};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use zk_server::config::Config;

struct Fixture {
    app: Router,
    project: String,
    root: std::path::PathBuf,
    seen: Arc<Mutex<Vec<Value>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

async fn fixture() -> Fixture {
    // macOS TMPDIR can consume most of sockaddr_un::sun_path by itself.
    let root =
        std::path::PathBuf::from("/tmp").join(format!("zk-analysis-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let workspace = root.canonicalize().unwrap();
    std::fs::write(workspace.join("api.py"), "def entry(): pass\n").unwrap();
    let socket = workspace.join("a.sock");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sidecar = Router::new()
        .route("/api/analysis/generate-diagram", post(|State(seen): State<Arc<Mutex<Vec<Value>>>>, Json(body): Json<Value>| async move {
            let invalid = body["target"] == "invalid";
            seen.lock().unwrap().push(body);
            Json(if invalid { json!({"mermaid_syntax":"fake"}) } else { json!({"diagram_type":"flowchart", "mermaid_syntax":"flowchart TD\nA-->B", "confidence_score":0.9, "metadata":{"nodes_count":2,"edges_count":1,"languages_analyzed":["python"],"analysis_time_ms":1.2},"warnings":[]}) })
        }))
        .route("/api/analysis/api-endpoints", post(|State(seen): State<Arc<Mutex<Vec<Value>>>>, Json(body): Json<Value>| async move {
            seen.lock().unwrap().push(body);
            Json(json!({"success":true,"total":1,"endpoints":[{"http_method":"GET","path":"/users","handler_function":"entry","handler_class":"api","file_path":"api.py","line_number":1,"language":"python","parameters":[{"userID":"opaque"}]}]}))
        }))
        .route("/api/analysis/code-path", post(|State(seen): State<Arc<Mutex<Vec<Value>>>>, Json(body): Json<Value>| async move {
            seen.lock().unwrap().push(body);
            Json(json!({"success":true,"data":{"nodes":[],"edges":[],"layers":[],"entry_node":"api.entry","total_depth":0,"analysis_time_ms":1.0,"warnings":[]}}))
        }))
        .route("/api/analysis/change-impact", post(|State(seen): State<Arc<Mutex<Vec<Value>>>>, Json(body): Json<Value>| async move {
            seen.lock().unwrap().push(body.clone());
            Json(json!({"success":true,"elapsed_ms":1.5,"data":{"changed_file":body["file_path"],"changed_lines":body["changed_lines"],"impact_nodes":[{"id":"entry","type":"function","name":"entry","file_path":body["file_path"],"line_range":[1,1],"impact_level":"direct","confidence":"high","language":"python"}],"impact_edges":[],"summary":{"direct_count":1,"indirect_count":0,"potential_count":0,"affected_apis":[],"affected_tasks":[]},"truncated":false,"graph_stats":{"total_nodes":1,"total_edges":0},"analysis_kind":"advisory","is_verification_evidence":body["depth"]==5}}))
        }))
        .route("/api/code-quality/complexity", post(complexity_response))
        .route("/api/analysis/cancel", post(|State(seen): State<Arc<Mutex<Vec<Value>>>>, Json(body): Json<Value>| async move {
            seen.lock().unwrap().push(body);
            Json(json!({"cancellationRequested":true,"status":"cancelled"}))
        }))
        .route("/api/analysis/openapi/python", get(|| async { Json(json!({"openapi":"3.1.0","info":{"title":"Python","version":"test"},"paths":{"/api/analysis/api-endpoints":{"post":{"operationId":"scan","responses":{"200":{"description":"ok","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Result"}}}}}}}},"components":{"schemas":{"Result":{"type":"object"}}}})) }))
        .with_state(seen.clone());
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, sidecar).await.unwrap();
    });
    let mut config = Config::test_config();
    config.python_enabled = true;
    config.python_uds_path = socket;
    config.workspace_allowed_roots = vec![workspace.clone()];
    let (mut app, _) = app_with_config(config);
    let (status, _, bytes) = call(
        &mut app,
        local_post(
            "/api/projects",
            Some(json!({"name":"Analysis", "workspaceRoot":workspace}).to_string()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{bytes:?}");
    let project = json_body(&bytes)["id"].as_str().unwrap().to_owned();
    Fixture {
        app,
        project,
        root,
        seen,
        server,
    }
}

#[tokio::test]
async fn canonical_and_python_aliases_use_the_same_authorized_typed_adapter() {
    let mut f = fixture().await;
    for (path, camel) in [
        ("/api/code-diagrams/generate", true),
        ("/api/analysis/generate-diagram", false),
    ] {
        let (status, _, bytes) = call(&mut f.app, local_post(path, Some(json!({"projectId":f.project,"projectRoot":".","diagramType":"flowchart","target":"entry","depth":2}).to_string()))).await;
        assert_eq!(status, StatusCode::OK, "{bytes:?}");
        let result = json_body(&bytes);
        assert_eq!(
            result[if camel {
                "mermaidSyntax"
            } else {
                "mermaid_syntax"
            }],
            "flowchart TD\nA-->B"
        );
        let received = f.seen.lock().unwrap().last().unwrap().clone();
        assert_eq!(received["options"]["depth"], 2);
        assert_eq!(
            received["project_root"],
            f.root.canonicalize().unwrap().to_str().unwrap()
        );
        assert_eq!(received["analysis_owner"], format!("project:{}", f.project));
        assert!(received.get("projectRoot").is_none());
        assert!(uuid::Uuid::parse_str(received["request_id"].as_str().unwrap()).is_ok());
    }
    let (status, _, bytes) = call(
        &mut f.app,
        local_post(
            "/api/code-path/endpoints",
            Some(json!({"projectId":f.project}).to_string()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    assert_eq!(
        json_body(&bytes)["endpoints"][0]["handlerFunction"],
        "entry"
    );
    let (status, _, bytes) = call(&mut f.app, local_post("/api/code-path/trace", Some(json!({"projectId":f.project,"entryFile":"api.py","entryFunction":"entry","maxDepth":4}).to_string()))).await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    assert_eq!(json_body(&bytes)["entryNode"], "api.entry");
    assert_eq!(f.seen.lock().unwrap().last().unwrap()["max_depth"], 4);
}

#[tokio::test]
async fn invalid_scope_escape_revocation_and_malformed_sidecar_fail_closed() {
    let mut f = fixture().await;
    for (body, expected) in [
        (
            json!({"diagramType":"flowchart","target":"entry"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"projectId":f.project,"projectRoot":"/","diagramType":"flowchart","target":"entry"}),
            StatusCode::FORBIDDEN,
        ),
        (
            json!({"projectId":f.project,"diagramType":"flowchart","target":"entry","depth":0}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let (status, _, _) = call(
            &mut f.app,
            local_post("/api/code-diagrams/generate", Some(body.to_string())),
        )
        .await;
        assert_eq!(status, expected);
    }
    std::os::unix::fs::symlink("/etc/hosts", f.root.join("outside.py")).unwrap();
    let (status, _, _) = call(
        &mut f.app,
        local_post(
            "/api/code-path/trace",
            Some(
                json!({"projectId":f.project,"entryFile":"outside.py","entryFunction":"entry"})
                    .to_string(),
            ),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(f.seen.lock().unwrap().is_empty());
    let (status, _, _) = call(
        &mut f.app,
        local_post(
            "/api/code-diagrams/generate",
            Some(
                json!({"projectId":f.project,"diagramType":"flowchart","target":"invalid"})
                    .to_string(),
            ),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    call(
        &mut f.app,
        local_delete(&format!("/api/projects/{}", f.project)),
    )
    .await;
    let (status, _, _) = call(
        &mut f.app,
        local_post(
            "/api/code-path/endpoints",
            Some(json!({"projectId":f.project}).to_string()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(f.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cancellation_is_scoped_and_openapi_uses_native_rust_and_actual_uds() {
    let mut f = fixture().await;
    let id = uuid::Uuid::new_v4().to_string();
    let (status, _, _) = call(
        &mut f.app,
        local_post(
            "/api/code-analysis/cancel",
            Some(json!({"projectId":f.project,"requestId":id}).to_string()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(f.seen.lock().unwrap().last().unwrap()["request_id"], id);
    let (status, _, bytes) = call(&mut f.app, local_get("/api/analysis/openapi/merged")).await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    let merged = json_body(&bytes);
    assert!(merged["paths"]["/api/code-diagrams/generate"].is_object());
    assert_eq!(
        merged["paths"]["/api/analysis/api-endpoints"]["post"]["operationId"],
        "python_scan"
    );
    assert!(merged["components"]["schemas"]["Python__Result"].is_object());
    assert!(
        merged["components"]["schemas"]["DiagramResult"]["properties"]["mermaidSyntax"].is_object()
    );
    assert_eq!(
        merged["paths"]["/api/analysis/api-endpoints"]["post"]["x-zk-adapter"],
        "/api/code-path/endpoints"
    );
    let (status, _, bytes) = call(&mut f.app, local_get("/api/analysis/openapi/backend")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json_body(&bytes)["paths"]["/api/sessions"].is_object());
}

#[tokio::test]
async fn change_impact_is_scoped_validated_and_never_accepts_verification_claims() {
    let mut f = fixture().await;
    let (status, _, bytes) = call(
        &mut f.app,
        local_post(
            "/api/analysis/change-impact",
            Some(
                json!({"projectId":f.project,"filePath":"api.py","changedLines":[1,1],"depth":3})
                    .to_string(),
            ),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    assert_eq!(json_body(&bytes)["data"]["analysis_kind"], "advisory");
    assert_eq!(json_body(&bytes)["data"]["changed_lines"], json!([1]));
    let upstream = f.seen.lock().unwrap().last().unwrap().clone();
    assert_eq!(upstream["analysis_owner"], format!("project:{}", f.project));
    assert_eq!(
        upstream["project_root"],
        f.root.canonicalize().unwrap().to_str().unwrap()
    );
    for (body, status) in [
        (
            json!({"filePath":"api.py","changedLines":[1]}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"projectId":f.project,"filePath":"/etc/passwd","changedLines":[1]}),
            StatusCode::FORBIDDEN,
        ),
        (
            json!({"projectId":f.project,"filePath":"api.py","changedLines":[0]}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"projectId":f.project,"filePath":"api.py","changedLines":[1],"depth":5}),
            StatusCode::BAD_GATEWAY,
        ),
    ] {
        let (actual, _, bytes) = call(
            &mut f.app,
            local_post("/api/analysis/change-impact", Some(body.to_string())),
        )
        .await;
        assert_eq!(actual, status, "{bytes:?}");
    }
}

async fn complexity_response(
    State(seen): State<Arc<Mutex<Vec<Value>>>>,
    Json(body): Json<Value>,
) -> Json<Value> {
    seen.lock().unwrap().push(body.clone());
    let escaped = body["target_path"]
        .as_str()
        .is_some_and(|path| path.ends_with("invalid.py"));
    let file = if escaped {
        "/outside.py".to_owned()
    } else {
        format!("{}/api.py", body["project_root"].as_str().unwrap())
    };
    Json(json!({"success":true,"elapsed_ms":1,"data":{
        "analysis_kind":"heuristic","is_verification_evidence":false,"cached":false,"truncated":false,
        "stats":{"total_files":1,"avg_cc":2.0,"high_risk_count":0,"analysis_time_ms":1},
        "root":{"name":"project","type":"project","loc":4,"cc":2.0,"mi":90.0,"risk_level":"A", "children":[
            {"name":"api.py","type":"file","loc":4,"cc":2.0,"mi":90.0,"risk_level":"A","file_path":file}
        ]}
    }}))
}

#[tokio::test]
async fn complexity_uses_authorized_worker_contract_and_rejects_untrusted_paths() {
    let mut f = fixture().await;
    let request_id = uuid::Uuid::new_v4().to_string();
    let (status, _, bytes) = call(&mut f.app, local_post("/api/code-quality/complexity", Some(json!({
        "projectId":f.project,"requestId":request_id,"targetPath":"api.py","languages":["python"]
    }).to_string()))).await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    let result = json_body(&bytes);
    assert_eq!(result["data"]["stats"]["total_files"], 1);
    assert_eq!(result["data"]["analysis_kind"], "heuristic");
    assert_eq!(result["data"]["is_verification_evidence"], false);
    let payload = f.seen.lock().unwrap().last().unwrap().clone();
    assert_eq!(payload["request_id"], request_id);
    assert_eq!(payload["analysis_owner"], format!("project:{}", f.project));
    assert_eq!(
        payload["target_path"],
        f.root
            .canonicalize()
            .unwrap()
            .join("api.py")
            .to_str()
            .unwrap()
    );
    for body in [
        json!({"projectRoot":f.root}),
        json!({"projectId":f.project,"languages":["rust"]}),
        json!({"projectId":f.project,"targetPath":"../"}),
    ] {
        let (status, _, _) = call(
            &mut f.app,
            local_post("/api/code-quality/complexity", Some(body.to_string())),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    assert_eq!(
        f.seen.lock().unwrap().len(),
        1,
        "invalid requests never reach Python"
    );
    std::fs::write(f.root.join("invalid.py"), "x = 1\n").unwrap();
    let (status, _, _) = call(
        &mut f.app,
        local_post(
            "/api/code-quality/complexity",
            Some(json!({"projectId":f.project,"target_path":"invalid.py"}).to_string()),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "worker cannot return paths outside its bound root"
    );
}
