//! Native Rust -> Unix socket -> Python -> real Chromium / HTTP verification gate.
//! Run after `./dev sync`: `cargo test -p zk-server --test verify_journey_native -- --ignored`.
use serde_json::{Value, json};
use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zk_db::Db;
use zk_server::python::{BrowserVerifyJourneyTool, Correlation, PythonClient};
use zk_tools::{Tool, ToolContext};

struct Sidecar(tokio::process::Child, i32);
impl Drop for Sidecar {
    fn drop(&mut self) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(self.1),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}

#[derive(Default)]
struct ResourceEvents(
    std::sync::Mutex<
        Vec<(
            zk_tools::ExecutionResourceLease,
            zk_tools::ExecutionResourceTerminal,
        )>,
    >,
);
impl zk_tools::ExecutionResourceObserver for ResourceEvents {
    fn register(
        &self,
        _owner: zk_tools::ExecutionResourceOwner,
        allocation: zk_tools::ExecutionResourceAllocation,
    ) -> futures::future::BoxFuture<'static, Result<zk_tools::ExecutionResourceLease, String>> {
        Box::pin(std::future::ready(Ok(zk_tools::ExecutionResourceLease {
            resource_id: allocation.resource_id,
        })))
    }
    fn bind_external(
        &self,
        _lease: zk_tools::ExecutionResourceLease,
        _external_id: String,
    ) -> futures::future::BoxFuture<'static, Result<(), String>> {
        Box::pin(std::future::ready(Ok(())))
    }
    fn finish(
        &self,
        lease: zk_tools::ExecutionResourceLease,
        terminal: zk_tools::ExecutionResourceTerminal,
    ) -> futures::future::BoxFuture<'static, Result<(), String>> {
        self.0.lock().unwrap().push((lease, terminal));
        Box::pin(std::future::ready(Ok(())))
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One integration lifetime owns and checks all real subprocesses.
#[ignore = "requires the synchronized native Python/Chromium environment; runs in source-bootstrap CI"]
async fn real_browser_and_http_journeys_archive_evidence_and_release_owned_resources() {
    native_journeys(false).await;
}

#[tokio::test]
#[ignore = "requires the synchronized native Python/Chromium environment; runs in source-bootstrap CI"]
async fn temporary_browser_and_http_journeys_keep_evidence_in_ram_and_release_resources() {
    native_journeys(true).await;
}

#[allow(clippy::too_many_lines)]
async fn native_journeys(ephemeral: bool) {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_owned();
    let workspace =
        std::env::temp_dir().join(format!("zk-native-journey-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("index.html"), r#"<html><title>Native journey</title><button id="go" onclick="this.textContent='clicked once';this.disabled=true">click</button></html>"#).unwrap();
    let socket = PathBuf::from(format!("/tmp/zkj-{}.sock", uuid::Uuid::new_v4()));
    let log = std::fs::File::create(workspace.join("sidecar.log")).unwrap();
    let child = tokio::process::Command::new(root.join("python-service/.venv/bin/python"))
        .args([
            "-m",
            "uvicorn",
            "main:app",
            "--uds",
            socket.to_str().unwrap(),
            "--app-dir",
            "src",
            "--log-level",
            "warning",
        ])
        .current_dir(root.join("python-service"))
        .env("PLAYWRIGHT_BROWSERS_PATH", root.join(".runtime/playwright"))
        .env("BROWSER_HEADLESS", "true")
        .env("BROWSER_CHANNEL", "")
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .process_group(0)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = i32::try_from(child.id().unwrap()).unwrap();
    let mut sidecar = Sidecar(child, pid);
    let client = Arc::new(PythonClient::new(&socket));
    tokio::time::timeout(Duration::from_secs(30), async {
        while !client.is_healthy().await {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        client.refresh_capabilities().await;
    })
    .await
    .unwrap();
    let db = Db::open(workspace.join("evidence.sqlite")).unwrap();
    let (session, lease) = if ephemeral {
        let (id, lease) = db
            .create_ephemeral_session("native-test", workspace.to_str().unwrap(), "DEFAULT")
            .await
            .unwrap();
        (id, Some(lease))
    } else {
        (
            db.create_session("native-test", workspace.to_str().unwrap())
                .await
                .unwrap()
                .id,
            None,
        )
    };
    db.start_run(
        "native-journey",
        &session,
        None,
        Some("native test"),
        "native-test",
    )
    .await
    .unwrap();
    let (progress, _) = mpsc::unbounded_channel();
    let resources = Arc::new(ResourceEvents::default());
    let context = ToolContext::new(CancellationToken::new(), progress)
        .with_session_id(&session)
        .with_ephemeral_content(ephemeral)
        .with_run_id("native-journey")
        .with_working_dir(&workspace)
        .with_execution_resources(
            zk_tools::ExecutionResourceOwner {
                task_id: "native-task".into(),
                run_id: "native-journey".into(),
                invocation_id: "native-invocation".into(),
            },
            resources.clone(),
        );
    let tool = BrowserVerifyJourneyTool::new(Arc::clone(&client), db.clone());
    let result = tool
        .execute(
            json!({"journey":[
        {"action":"navigate","url":"/"},
        {"action":"click","selector":"#go"},
        {"action":"assert_text","selector":"#go","expected":"clicked once"}
    ],"verification_mode":"browser","record":!ephemeral}),
            context.clone(),
        )
        .await;
    assert!(
        !result.is_error,
        "{}; logs {}",
        result.content,
        workspace.display()
    );
    let response = &result.metadata.as_ref().unwrap()["structuredResult"];
    assert_eq!(response["step_results"][1]["ok"], true);
    for step in response["step_results"].as_array().unwrap() {
        let digest = step["screenshot_sha256"]
            .as_str()
            .expect("actual screenshot archived");
        let bytes = if ephemeral {
            assert!(!workspace.join(".zk").exists());
            for entry in std::fs::read_dir(&workspace).unwrap() {
                let file = entry.unwrap().path();
                if file.is_file()
                    && file
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with("evidence.sqlite")
                {
                    assert!(
                        !std::fs::read(file)
                            .unwrap()
                            .windows(digest.len())
                            .any(|bytes| bytes == digest.as_bytes())
                    );
                }
            }
            db.memory_content_store()
                .get_named_bytes(&session, &format!("evidence:{digest}"))
                .unwrap()
                .to_vec()
        } else {
            std::fs::read(workspace.join(".zk/blobs").join(&digest[..2]).join(digest)).unwrap()
        };
        assert!(bytes.starts_with(&[0xff, 0xd8, 0xff]));
        assert!(step.get("screenshot_base64").is_none());
    }
    let evidence = response["evidence"]["items"].as_array().unwrap();
    for kind in [
        "journey_trace_path",
        "journey_har_path",
        "journey_video_dir",
    ] {
        assert_eq!(
            evidence
                .iter()
                .any(|item| item["type"] == kind && item["blobSha256"].is_string()),
            !ephemeral,
            "recording policy {kind}: {evidence:?}"
        );
    }
    assert_eq!(resources.0.lock().unwrap().len(), 2);
    assert!(
        resources
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|(_, terminal)| *terminal == zk_tools::ExecutionResourceTerminal::Released)
    );
    let browser_id = response["session_id"].as_str().unwrap();
    let absent: Value = client
        .call_if_available(
            "BROWSER_AUTOMATION",
            "/api/browser/snapshot-semantic",
            &json!({"session_id":browser_id,"strict_session":true}),
            &Correlation::for_session(Some(&session)),
        )
        .await
        .unwrap();
    assert_eq!(absent["error_code"], "SESSION_NOT_FOUND", "{absent}");
    let url = reqwest::Url::parse(response["final_url"].as_str().unwrap()).unwrap();
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", url.port().unwrap()))
            .await
            .is_err(),
        "preview listener leaked"
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop_http = CancellationToken::new();
    let stopped = stop_http.clone();
    let http = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route(
                "/",
                axum::routing::get(|| async { axum::Json(json!({"ok":true})) }),
            ),
        )
        .with_graceful_shutdown(stopped.cancelled_owned())
        .await
        .unwrap();
    });
    let result = tool
        .execute(
            json!({"base_url":format!("http://{address}"),"journey":[
        {"action":"http_get","url":"/"},
        {"action":"assert_status","expected_code":200},
        {"action":"assert_json","path":"$.ok","expected":true}
    ],"verification_mode":"auto"}),
            context.clone(),
        )
        .await;
    assert!(!result.is_error, "{}", result.content);
    assert_eq!(
        result.metadata.unwrap()["structuredResult"]["evidence"]["kind"],
        "http_journey"
    );
    if ephemeral {
        use zk_tools::RunToolScopeFactory as _;
        let base = Arc::new(zk_tools::ToolRegistry::new());
        base.register_dynamic(Arc::new(zk_server::python::WebBrowserTool::new(
            client.clone(),
        )));
        let factory =
            zk_server::python::tools::BrowserRunScopeFactory::new(client.clone(), db.clone());
        let scope = factory.prepare(context.clone(), base).await.unwrap();
        let browser = scope.registry().get("WebBrowser").unwrap();
        let navigated = browser.execute(json!({"action":"navigate","url":format!("http://{address}"),"session_id":"same-alias"}),context.clone()).await;
        assert!(!navigated.is_error, "{}", navigated.content);
        let evaluated = browser.execute(json!({"action":"evaluate","script":"globalThis.privateCount=(globalThis.privateCount||0)+1","session_id":"same-alias"}),context.clone()).await;
        assert!(!evaluated.is_error, "{}", evaluated.content);
        let again = browser.execute(json!({"action":"evaluate","script":"globalThis.privateCount","session_id":"same-alias"}),context.clone()).await;
        assert!(!again.is_error, "{}", again.content);
        assert!(again.content.contains('1'));
        let capture = browser
            .execute(
                json!({"action":"screenshot","session_id":"same-alias"}),
                context.clone(),
            )
            .await;
        assert!(!capture.is_error, "{}", capture.content);
        let receipt = capture.evidence_receipt().unwrap();
        assert_eq!(
            receipt.verdict,
            zk_tools::EvidenceReceiptVerdict::Inconclusive
        );
        let digest = receipt.items[0].blob_sha256.as_ref().unwrap();
        assert!(
            db.memory_content_store()
                .get_named_bytes(&session, &format!("evidence:{digest}"))
                .unwrap()
                .starts_with(b"\x89PNG")
        );
        let wrong = browser
            .execute(
                json!({"action":"evaluate","script":"1"}),
                context.clone().with_run_id("other-run"),
            )
            .await;
        assert!(wrong.is_error);
        assert!(!workspace.join(".zk").exists());
        assert!(!workspace.join("screenshots").exists());
        scope.cleanup().await.unwrap();
        scope.cleanup().await.unwrap();
        assert!(
            !context.cancel.is_cancelled(),
            "scope cleanup must not stop the parent Run"
        );
        let stopped = browser
            .execute(
                json!({"action":"navigate","url":format!("http://{address}")}),
                context.clone(),
            )
            .await;
        assert!(stopped.is_error);
        assert!(
            resources
                .0
                .lock()
                .unwrap()
                .iter()
                .all(|(_, state)| *state == zk_tools::ExecutionResourceTerminal::Released)
        );
        drop(scope);
    }
    stop_http.cancel();
    http.await.unwrap();
    let _ = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGTERM,
    );
    tokio::time::timeout(Duration::from_secs(10), sidecar.0.wait())
        .await
        .unwrap()
        .unwrap();
    let _ = std::fs::remove_file(socket);
    drop(lease);
    if ephemeral {
        assert_eq!(db.memory_content_store().retained_bytes(), 0);
    }
    drop(tool);
    drop(db);
    std::fs::remove_dir_all(workspace).unwrap();
}
