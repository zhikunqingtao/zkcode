//! Native Rust -> Unix socket -> Python -> real Chromium / HTTP verification gate.
//! Run after `./dev sync`: `cargo test -p zk-server --test verify_journey_native -- --ignored`.
use serde_json::{Value, json};
use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zk_db::Db;
use zk_server::python::{BrowserVerifyJourneyTool, Correlation, PythonClient};
use zk_tools::{CallEnv, Tool, ToolContext, ToolEvent, ToolOutput};

const PASSWORD_CANARY: &str = "ZK_BROWSER_PASSWORD_CANARY_91378246";
const PASSWORD_HTML: &str = r#"<html><title>Password fixture</title><input id="secret" type="text" aria-label="Password" value="ZK_BROWSER_PASSWORD_CANARY_91378246"><input id="normal" value="ordinary-visible-value"><script>
const input = document.querySelector('#secret');
input.type = 'password';
const getter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').get;
window.passwordReads = 0;
Object.defineProperty(input,'value',{get(){ window.passwordReads++; return getter.call(this); }});
</script></html>"#;

struct Sidecar(tokio::process::Child, i32);
impl Drop for Sidecar {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_some() {
            return;
        }
        // Even a failed assertion must give this isolated application's browser
        // shutdown a chance; Chromium may own a child process group of its own.
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(self.1),
            nix::sys::signal::Signal::SIGTERM,
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if self.0.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(self.1),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}

async fn start_sidecar(
    root: &std::path::Path,
    workspace: &std::path::Path,
    socket: &std::path::Path,
) -> (Sidecar, Arc<PythonClient>) {
    // Test-owned wrapper counts dispatches; an opt-in marker drops one ACK reply
    // after the production handler has actually consumed its batch.
    std::fs::write(
        workspace.join("native_app.py"),
        r"import os
from main import app as production_app
async def app(scope, receive, send):
    if scope['type'] == 'http' and scope.get('path') == '/api/browser/journey/run':
        with open(os.environ['ZK_NATIVE_JOURNEY_REQUESTS'], 'a') as log:
            log.write('journey\n')
    marker = os.path.join(os.path.dirname(os.environ['ZK_NATIVE_JOURNEY_REQUESTS']), 'drop-ack-once')
    if scope['type'] == 'http' and scope.get('path') == '/api/browser/recordings/ack' and os.path.exists(marker):
        replies = []
        async def capture(message):
            replies.append(message)
        await production_app(scope, receive, capture)
        os.unlink(marker)
        await send({'type':'http.response.start','status':503,'headers':[]})
        await send({'type':'http.response.body','body':b'lost ACK reply'})
        return
    await production_app(scope, receive, send)
",
    )
    .unwrap();
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(workspace.join("sidecar.log"))
        .unwrap();
    let child = tokio::process::Command::new(root.join("python-service/.venv/bin/python"))
        .args([
            "-m",
            "uvicorn",
            "native_app:app",
            "--uds",
            socket.to_str().unwrap(),
            "--app-dir",
            workspace.to_str().unwrap(),
            "--log-level",
            "warning",
        ])
        .current_dir(root.join("python-service"))
        .env("PYTHONPATH", root.join("python-service/src"))
        .env(
            "ZK_NATIVE_JOURNEY_REQUESTS",
            workspace.join("journey-requests.log"),
        )
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
    let sidecar = Sidecar(child, pid);
    let client = Arc::new(PythonClient::new(socket));
    tokio::time::timeout(Duration::from_secs(30), async {
        while !client.is_healthy().await {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        client.refresh_capabilities().await;
    })
    .await
    .unwrap();
    (sidecar, client)
}

async fn stop_sidecar(sidecar: &mut Sidecar) {
    let _ = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(sidecar.1),
        nix::sys::signal::Signal::SIGTERM,
    );
    tokio::time::timeout(Duration::from_secs(10), sidecar.0.wait())
        .await
        .unwrap()
        .unwrap();
}

struct NativeProcess<'a> {
    root: &'a std::path::Path,
    workspace: &'a std::path::Path,
    socket: &'a std::path::Path,
    process: &'a mut Sidecar,
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
    if isolated_recording_process(
        "real_browser_and_http_journeys_archive_evidence_and_release_owned_resources",
    )
    .await
    {
        return;
    }
    native_journeys(false).await;
}

#[tokio::test]
#[ignore = "requires the synchronized native Python/Chromium environment; runs in source-bootstrap CI"]
async fn temporary_browser_and_http_journeys_keep_evidence_in_ram_and_release_resources() {
    if isolated_recording_process(
        "temporary_browser_and_http_journeys_keep_evidence_in_ram_and_release_resources",
    )
    .await
    {
        return;
    }
    native_journeys(true).await;
}

#[tokio::test]
#[ignore = "requires synchronized native Python/Chromium; isolated private fixture"]
#[allow(clippy::too_many_lines)] // Keep the isolated cancellation/deadline lifecycle assertions together.
async fn cancelled_and_deadline_recordings_converge_without_replaying_browser_actions() {
    use axum::{
        Router,
        response::Html,
        routing::{get, post},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    const TEST: &str =
        "cancelled_and_deadline_recordings_converge_without_replaying_browser_actions";
    if isolated_recording_process(TEST).await {
        return;
    }
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_owned();
    let workspace =
        std::env::temp_dir().join(format!("zk-native-recovery-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workspace).unwrap();
    let socket = PathBuf::from(format!("/tmp/zkj-{}.sock", uuid::Uuid::new_v4()));
    let (mut sidecar, mut client) = start_sidecar(&root, &workspace, &socket).await;
    let effects = Arc::new(AtomicUsize::new(0));
    let router=Router::new().route("/",get(||async {Html(r#"<button id="effect" onclick="fetch('/effect',{method:'POST'})">effect</button>"#)}))
        .route("/effect",post({let effects=effects.clone();move || {let effects=effects.clone();async move {effects.fetch_add(1,Ordering::SeqCst);"ok"}}}));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let db = Db::open(workspace.join("evidence.sqlite")).unwrap();
    for (index, deadline) in [false, true].into_iter().enumerate() {
        let session = db
            .create_session("native", workspace.to_str().unwrap())
            .await
            .unwrap()
            .id;
        let run_id = format!("stopped-{index}");
        db.start_root_run_with_budget(
            &run_id,
            &session,
            None,
            "native",
            &zk_db::TaskBudgetLimits {
                deadline_at_ms: Some(
                    zk_db::time::now_millis() + if deadline { 15_000 } else { 60_000 },
                ),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cancel = CancellationToken::new();
        let stop = {
            let effects = effects.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(20), async {
                    while effects.load(Ordering::SeqCst) <= index {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .unwrap();
                if !deadline {
                    cancel.cancel();
                }
            })
        };
        let input = json!({"base_url":format!("http://{address}"),"mode":"browser","record":{"har":true},"steps":[{"action":"navigate","url":"/"},{"action":"click","selector":"#effect"},{"action":"wait_for","selector":"#never","timeout":60000}]});
        let (output, invocation) = supervised_journey_controlled(
            &db,
            Arc::new(BrowserVerifyJourneyTool::new(client.clone(), db.clone())),
            &session,
            &workspace,
            input,
            &run_id,
            cancel,
        )
        .await;
        stop.await.unwrap();
        assert!(output.is_error, "{}", output.content);
        let outcome = if deadline {
            zk_db::ToolInvocationStatus::Failed
        } else {
            zk_db::ToolInvocationStatus::Cancelled
        };
        db.commit_tool_invocation_result(&zk_db::CommitToolInvocationResult {
            invocation_id: invocation.clone(),
            expected_version: 1,
            session_id: session.clone(),
            target: outcome,
            input_json: None,
            content: output.content.clone(),
            is_error: true,
            metadata: output.metadata.clone(),
            output_sha256: None,
            error_code: Some(if deadline { "TIMEOUT" } else { "CANCELLED" }.into()),
            cleanup_status: zk_db::CleanupStatus::Confirmed,
            postprocessing: None,
        })
        .await
        .unwrap();
        finish_native_run(&db, &run_id).await;
        let pending = db.pending_browser_recordings().await.unwrap();
        assert_eq!(pending.len(), 1);
        let batch = PathBuf::from(std::env::var_os("ZK_BROWSER_RECORDING_SPOOL").unwrap())
            .join(pending[0].state["identity"]["batch_id"].as_str().unwrap());
        assert!(db.delete_session(&session).await.is_err());
        stop_sidecar(&mut sidecar).await;
        assert_eq!(
            zk_server::python::tools::reconcile_browser_recordings(&db, &client)
                .await
                .unwrap(),
            0
        );
        let _ = std::fs::remove_file(&socket);
        let (restarted, reconnected) = start_sidecar(&root, &workspace, &socket).await;
        sidecar = restarted;
        client = reconnected;
        let reopened = Db::open(workspace.join("evidence.sqlite")).unwrap();
        assert_eq!(
            zk_server::python::tools::reconcile_browser_recordings(&reopened, &client)
                .await
                .unwrap(),
            1
        );
        assert!(batch.join("ack.json").exists());
        assert_eq!(
            effects.load(Ordering::SeqCst),
            index + 1,
            "recovery cannot replay the click"
        );
        let bundles = reopened.find_evidence_by_session(&session).await.unwrap();
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].verdict, "inconclusive");
        let source = invocation.clone();
        let persisted: String = reopened
            .with_conn_blocking(move |conn| {
                Ok(conn.query_row(
                    "SELECT status FROM tool_invocations WHERE invocation_id=?1",
                    [source],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(persisted, outcome.as_db());
        assert!(reopened.delete_session(&session).await.unwrap());
    }
    let dispatched = std::fs::read_to_string(workspace.join("journey-requests.log")).unwrap();
    assert_eq!(dispatched.lines().count(), 2);
    stop_sidecar(&mut sidecar).await;
    server.abort();
    let _ = std::fs::remove_file(socket);
    std::fs::remove_dir_all(workspace).unwrap();
}

// Configure the private spool before runtime creation, without mutating the
// process environment from concurrent Rust tests or touching a user's spool.
async fn isolated_recording_process(test: &str) -> bool {
    if std::env::var("ZK_NATIVE_JOURNEY_CHILD").as_deref() == Ok(test) {
        return false;
    }
    let root = std::env::temp_dir().join(format!("zk-native-spool-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let status = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args([test, "--exact", "--ignored", "--nocapture"])
        .env("ZK_NATIVE_JOURNEY_CHILD", test)
        .env("ZK_BROWSER_RECORDING_SPOOL", root.join("spool"))
        .kill_on_drop(true)
        .status()
        .await
        .unwrap();
    assert!(
        status.success(),
        "native fixture failed; private spool retained at {}",
        root.display()
    );
    std::fs::remove_dir_all(root).unwrap();
    true
}

async fn supervised_journey(
    db: &Db,
    tool: Arc<dyn Tool>,
    session: &str,
    workspace: &std::path::Path,
    input: Value,
    run_id: &str,
) -> (ToolOutput, String) {
    supervised_journey_controlled(
        db,
        tool,
        session,
        workspace,
        input,
        run_id,
        CancellationToken::new(),
    )
    .await
}

async fn supervised_journey_controlled(
    db: &Db,
    tool: Arc<dyn Tool>,
    session: &str,
    workspace: &std::path::Path,
    input: Value,
    run_id: &str,
    cancel: CancellationToken,
) -> (ToolOutput, String) {
    let run = db.find_run_by_id(run_id).await.unwrap().unwrap();
    let invocation = uuid::Uuid::new_v4().to_string();
    db.create_tool_invocation(&zk_db::NewToolInvocation {
        invocation_id: invocation.clone(),
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        tool_use_id: invocation.clone(),
        tool_name: tool.name().into(),
        input_json: Some(input.to_string()),
        side_effect_class: "read".into(),
        directory_generation: Some(1),
        connection_generation: None,
    })
    .await
    .unwrap();
    db.transition_tool_invocation_cas(
        &invocation,
        0,
        zk_db::ToolInvocationStatus::Running,
        Some(&input.to_string()),
        None,
        None,
        zk_db::CleanupStatus::Pending,
    )
    .await
    .unwrap();
    let supervisor = zk_engine::ExecutionSupervisor::new(db.clone());
    let mut events = supervisor.spawn_call_in(
        tool,
        invocation.clone(),
        input,
        &cancel,
        CallEnv::new()
            .with_session_id(session)
            .with_run_id(&run.id)
            .with_working_dir(workspace),
        zk_tools::ExecutionResourceOwner {
            task_id: run.task_id,
            run_id: run.id,
            invocation_id: invocation.clone(),
        },
    );
    while let Some(event) = events.recv().await {
        if let ToolEvent::Finished {
            output,
            cleanup_status,
            ..
        } = event
        {
            assert!(
                matches!(
                    cleanup_status,
                    zk_tools::ToolCleanupStatus::Confirmed
                        | zk_tools::ToolCleanupStatus::NotRequired
                ),
                "{}",
                output.content
            );
            return (output, invocation);
        }
    }
    if cancel.is_cancelled() {
        // The real supervisor deliberately closes its event stream on cancel;
        // reproduce the Engine's cancelled terminal commit rather than inventing
        // a successful verifier result.
        return (
            ToolOutput::error("VERIFY_CANCELLED: supervised execution stopped"),
            invocation,
        );
    }
    panic!("supervised invocation lost terminal result")
}

async fn finish_native_run(db: &Db, run_id: &str) {
    let run = db.find_run_by_id(run_id).await.unwrap().unwrap();
    let task = db
        .find_runtime_task_by_id(&run.task_id)
        .await
        .unwrap()
        .unwrap();
    let content = "Native local fixture completed";
    db.append_attributed_message(
        &run.session_id,
        zk_db::NewMessage {
            meta: None,
            role: zk_db::MessageRole::Assistant,
            content: vec![zk_db::StoredBlock::Text {
                text: content.into(),
            }],
            stop_reason: Some("end_turn".into()),
            input_tokens: 0,
            output_tokens: 0,
        },
        zk_db::MessageAttribution {
            task_id: Some(run.task_id.clone()),
            run_id: Some(run.id.clone()),
            origin: "conversation".into(),
            source_task_id: None,
        },
    )
    .await
    .unwrap();
    let result = db
        .commit_task_result(&zk_db::CommitTaskResult {
            task_id: run.task_id,
            run_id: run.id,
            expected_task_version: task.version,
            status: zk_db::ResultStatus::Complete,
            content: content.into(),
            media_type: "text/plain".into(),
            error_code: None,
            cleanup_status: zk_db::CleanupStatus::Confirmed,
            verification_status: zk_db::VerificationStatus::NotRequested,
        })
        .await
        .unwrap();
    assert!(
        matches!(result, zk_db::CommitTaskResultOutcome::Committed { .. }),
        "{result:?}"
    );
}

struct PersistentScopeProbe {
    db: Db,
    client: Arc<PythonClient>,
}
impl Tool for PersistentScopeProbe {
    fn name(&self) -> &'static str {
        "BrowserScopeProbe"
    }
    fn description(&self) -> &'static str {
        "Local native fixture for production browser Run scope"
    }
    fn parameters(&self) -> Value {
        json!({"type":"object"})
    }
    fn execute(
        &self,
        input: Value,
        context: ToolContext,
    ) -> futures::future::BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            use zk_tools::RunToolScopeFactory as _;
            let base = Arc::new(zk_tools::ToolRegistry::new());
            base.register_dynamic(Arc::new(zk_server::python::WebBrowserTool::new(
                self.client.clone(),
            )));
            let factory = zk_server::python::tools::BrowserRunScopeFactory::new(
                self.client.clone(),
                self.db.clone(),
            );
            let scope = factory
                .prepare(context.clone(), base)
                .await
                .expect("production Run scope");
            let browser = scope.registry().get("WebBrowser").unwrap();
            let mut last = ToolOutput::ok("");
            for action in input["actions"].as_array().unwrap() {
                last = browser.execute(action.clone(), context.clone()).await;
                if last.is_error {
                    break;
                }
            }
            if input["assert_password_getter_unread"] == true {
                let check = browser
                    .execute(
                        json!({"action":"evaluate","script":"window.passwordReads"}),
                        context.clone(),
                    )
                    .await;
                assert!(!check.is_error, "{}", check.content);
                assert_eq!(
                    serde_json::from_str::<Value>(&check.content).unwrap()["result"],
                    "0"
                );
            }
            scope.cleanup().await.expect("usage lease release");
            last
        })
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "One real evidence/ACK fault-recovery lifetime"
)]
async fn commit_native_recordings(
    db: &Db,
    client: &mut Arc<PythonClient>,
    session: &str,
    invocation: &str,
    output: &ToolOutput,
    process: &mut NativeProcess<'_>,
) {
    use zk_server::python::tools::reconcile_browser_recordings;
    let pending = db.pending_browser_recordings().await.unwrap();
    assert_eq!(
        pending.len(),
        1,
        "real supervisor must persist recording resource"
    );
    let batch = pending[0].state["identity"]["batch_id"].as_str().unwrap();
    let manifest = pending[0].state["manifest"].clone();
    let batch_path =
        PathBuf::from(std::env::var_os("ZK_BROWSER_RECORDING_SPOOL").unwrap()).join(batch);
    assert_eq!(
        reconcile_browser_recordings(db, client).await.unwrap(),
        0,
        "receipt alone cannot ACK"
    );
    assert!(!batch_path.join("ack.json").exists());
    db.commit_tool_invocation_result(&zk_db::CommitToolInvocationResult {
        invocation_id: invocation.into(),
        expected_version: 1,
        session_id: session.into(),
        target: zk_db::ToolInvocationStatus::Succeeded,
        input_json: Some("{}".into()),
        content: output.content.clone(),
        is_error: false,
        metadata: output.metadata.clone(),
        output_sha256: None,
        error_code: None,
        cleanup_status: zk_db::CleanupStatus::Confirmed,
        postprocessing: Some(json!({"evidence":true})),
    })
    .await
    .unwrap();
    let receipt = output
        .evidence_receipt()
        .expect("real verified evidence receipt");
    let bundle = uuid::Uuid::new_v4().to_string();
    db.save_evidence_bundle(&zk_db::EvidenceBundleRecord {
        bundle_id: bundle.clone(),
        session_id: session.into(),
        agent_id: None,
        kind: receipt.kind,
        claim: receipt.claim,
        origin: zk_db::EvidenceOrigin::Machine,
        producer_invocation_id: Some(invocation.into()),
        verdict: receipt.verdict.as_db().into(),
        created_at: receipt.observed_at,
        run_id: Some("native-journey".into()),
        items: receipt
            .items
            .into_iter()
            .map(|item| zk_db::EvidenceItemRecord {
                id: format!("{bundle}-{}", item.sort_order),
                producer_invocation_id: Some(invocation.into()),
                item_type: item.item_type,
                summary: item.summary,
                blob_sha256: item.blob_sha256,
                meta: item.meta,
                sort_order: i64::from(item.sort_order),
            })
            .collect(),
    })
    .await
    .unwrap();
    assert_eq!(
        reconcile_browser_recordings(db, client).await.unwrap(),
        0,
        "evidence alone cannot ACK before postprocessing"
    );
    db.complete_tool_result_postprocessing_cas(invocation, 0)
        .await
        .unwrap();
    finish_native_run(db, "native-journey").await;
    let dispatched_before =
        std::fs::read_to_string(process.workspace.join("journey-requests.log")).unwrap();
    assert_eq!(dispatched_before.lines().count(), 1);
    stop_sidecar(process.process).await;
    // An actual unavailable UDS causes a real ACK failure after eligibility was
    // durably established. No fake observer or simulated zero-cost receipt.
    assert_eq!(reconcile_browser_recordings(db, client).await.unwrap(), 0);
    let pending = db.pending_browser_recordings().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].state["phase"], "ackEligible");
    assert!(batch_path.join("manifest.json").exists());
    assert!(!batch_path.join("ack.json").exists());
    for file in manifest["files"].as_array().unwrap() {
        if file["status"] == "available" {
            assert!(batch_path.join(file["path"].as_str().unwrap()).exists());
        }
    }
    let denied = db
        .delete_session(session)
        .await
        .expect_err("pending recording blocks deletion");
    assert!(
        denied
            .to_string()
            .contains("SESSION_RECORDING_FINALIZATION_PENDING"),
        "{denied}"
    );
    // The next process has a new browser generation; it consumes persisted
    // manifest ownership without a browser action, new resource or new receipt.
    let _ = std::fs::remove_file(process.socket);
    let (restarted, reconnected) =
        start_sidecar(process.root, process.workspace, process.socket).await;
    *process.process = restarted;
    *client = reconnected;
    let reopened = Db::open(process.workspace.join("evidence.sqlite")).unwrap();
    assert_eq!(
        reopened.pending_browser_recordings().await.unwrap()[0].state["phase"],
        "ackEligible"
    );
    assert_eq!(
        reconcile_browser_recordings(&reopened, client)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        std::fs::read_to_string(process.workspace.join("journey-requests.log")).unwrap(),
        dispatched_before,
        "restart recovery cannot replay Journey"
    );
    let producer = invocation.to_owned();
    let evidence_count: i64 = reopened
        .with_reader(move |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM evidence_bundles WHERE producer_invocation_id=?1",
                [producer],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(evidence_count, 1, "no duplicate evidence on ACK recovery");
    assert_eq!(
        reconcile_browser_recordings(db, client).await.unwrap(),
        0,
        "ACK retry is idempotent"
    );
    assert!(batch_path.join("ack.json").exists());
    for file in manifest["files"].as_array().unwrap() {
        if let Some(path) = file["path"].as_str() {
            assert!(!batch_path.join(path).exists());
        }
    }
    assert!(db.pending_browser_recordings().await.unwrap().is_empty());
    let invocation = invocation.to_owned();
    let statuses: Vec<String> = db
        .with_reader(move |conn| {
            let mut statement =
                conn.prepare("SELECT status FROM execution_resources WHERE invocation_id=?1")?;
            Ok(statement
                .query_map([invocation], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?)
        })
        .await
        .unwrap();
    assert_eq!(statuses.len(), 2);
    assert!(statuses.iter().all(|status| status == "released"));
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
    let (mut sidecar, mut client) = start_sidecar(&root, &workspace, &socket).await;
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
    let mut tool = BrowserVerifyJourneyTool::new(Arc::clone(&client), db.clone());
    let input = json!({"journey":[
        {"action":"navigate","url":"/"},
        {"action":"click","selector":"#go"},
        {"action":"assert_text","selector":"#go","expected":"clicked once"}
    ],"verification_mode":"browser","record":!ephemeral});
    let (result, invocation) = if ephemeral {
        (tool.execute(input, context.clone()).await, None)
    } else {
        let (result, invocation) = supervised_journey(
            &db,
            Arc::new(BrowserVerifyJourneyTool::new(client.clone(), db.clone())),
            &session,
            &workspace,
            input,
            "native-journey",
        )
        .await;
        (result, Some(invocation))
    };
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
    for kind in ["trace", "har", "video"] {
        assert_eq!(
            evidence
                .iter()
                .any(|item| item["type"] == "journey_recording"
                    && item["meta"]["kind"] == kind
                    && item["blobSha256"].is_string()),
            !ephemeral,
            "recording policy {kind}: {evidence:?}"
        );
    }
    if let Some(invocation) = invocation {
        commit_native_recordings(
            &db,
            &mut client,
            &session,
            &invocation,
            &result,
            &mut NativeProcess {
                root: &root,
                workspace: &workspace,
                socket: &socket,
                process: &mut sidecar,
            },
        )
        .await;
        tool = BrowserVerifyJourneyTool::new(client.clone(), db.clone());
    } else {
        assert_eq!(resources.0.lock().unwrap().len(), 2);
    }
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
            axum::Router::new()
                .route(
                    "/",
                    axum::routing::get(|| async { axum::Json(json!({"ok":true})) }),
                )
                .route(
                    "/password",
                    axum::routing::get(|| async { axum::response::Html(PASSWORD_HTML) }),
                ),
        )
        .with_graceful_shutdown(stopped.cancelled_owned())
        .await
        .unwrap();
    });
    if !ephemeral {
        for (run_id, actions, expected) in [
            (
                "scope-round-one",
                json!([
                    {"action":"navigate","url":format!("http://{address}")},
                    {"action":"evaluate","script":"globalThis.crossRoundCount = 41"}
                ]),
                "41",
            ),
            (
                "scope-round-two",
                json!([
                    {"action":"evaluate","script":"globalThis.crossRoundCount += 1"}
                ]),
                "42",
            ),
            (
                "scope-password",
                json!([
                    {"action":"navigate","url":format!("http://{address}/password")},
                    {"action":"snapshot-semantic","include_screenshot":false}
                ]),
                "ordinary-visible-value",
            ),
        ] {
            db.start_root_run_with_budget(
                run_id,
                &session,
                None,
                "native-test",
                &zk_db::TaskBudgetLimits {
                    deadline_at_ms: Some(zk_db::time::now_millis() + 180_000),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let (result, invocation) = supervised_journey(
                &db,
                Arc::new(PersistentScopeProbe {
                    db: db.clone(),
                    client: client.clone(),
                }),
                &session,
                &workspace,
                json!({"actions":actions,"assert_password_getter_unread":run_id == "scope-password"}),
                run_id,
            )
            .await;
            assert!(!result.is_error, "{}", result.content);
            assert!(result.content.contains(expected), "{}", result.content);
            if run_id == "scope-password" {
                assert!(!result.content.contains(PASSWORD_CANARY));
                let data: Value = serde_json::from_str(&result.content).unwrap();
                assert_eq!(data["capture_status"], "complete");
                let mut config = zk_server::config::Config::test_config();
                config.workspace_default_root = workspace.to_string_lossy().into_owned();
                config.workspace_allowed_roots = vec![workspace.clone()];
                let replay_host = zk_server::state::AppState::new(db.clone(), config);
                replay_host
                    .browser_replay
                    .append_python_snapshot(&session, &data)
                    .unwrap();
                let retained = replay_host.browser_replay.get(&session).unwrap().unwrap();
                assert!(!retained.to_string().contains(PASSWORD_CANARY));
                assert!(retained.to_string().contains("ordinary-visible-value"));
            }
            db.commit_tool_invocation_result(&zk_db::CommitToolInvocationResult {
                invocation_id: invocation,
                expected_version: 1,
                session_id: session.clone(),
                target: zk_db::ToolInvocationStatus::Succeeded,
                input_json: Some("{}".into()),
                content: result.content,
                is_error: false,
                metadata: result.metadata,
                output_sha256: None,
                error_code: None,
                cleanup_status: zk_db::CleanupStatus::Confirmed,
                postprocessing: None,
            })
            .await
            .unwrap();
            finish_native_run(&db, run_id).await;
        }
        // Both Run scopes ended, yet ordinary page state was preserved. Explicit
        // Session cleanup releases it without guessing a model-controlled alias.
        zk_server::python::tools::close_session_browser_contexts(&db, &client, &session)
            .await
            .unwrap();
        let absent: Value = client.call_if_available("BROWSER_AUTOMATION","/api/browser/snapshot-semantic",&json!({"session_id":zk_server::python::tools::session_browser_id(&session,"default"),"strict_session":true}),&Correlation::for_session(Some(&session))).await.unwrap();
        assert_eq!(absent["error_code"], "SESSION_NOT_FOUND");
    }
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
        let password_page = browser.execute(json!({"action":"navigate","url":format!("http://{address}/password"),"session_id":"same-alias"}),context.clone()).await;
        assert!(!password_page.is_error, "{}", password_page.content);
        let snapshot = browser.execute(json!({"action":"snapshot-semantic","session_id":"same-alias","include_screenshot":false}),context.clone()).await;
        assert!(!snapshot.is_error, "{}", snapshot.content);
        assert!(!snapshot.content.contains(PASSWORD_CANARY));
        let evidence = snapshot.evidence_receipt().unwrap();
        assert_eq!(
            evidence.verdict,
            zk_tools::EvidenceReceiptVerdict::Inconclusive
        );
        let digest = evidence.items[0].blob_sha256.as_ref().unwrap();
        let bytes = db
            .memory_content_store()
            .get_named_bytes(&session, &format!("evidence:{digest}"))
            .unwrap();
        let snapshot_json = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(!snapshot_json.contains(PASSWORD_CANARY));
        assert!(snapshot_json.contains("ordinary-visible-value"));
        assert_eq!(
            serde_json::from_str::<Value>(&snapshot_json).unwrap()["capture_status"],
            "complete"
        );
        let reads = browser.execute(json!({"action":"evaluate","session_id":"same-alias","script":"window.passwordReads"}),context.clone()).await;
        assert!(!reads.is_error, "{}", reads.content);
        assert_eq!(
            serde_json::from_str::<Value>(&reads.content).unwrap()["result"],
            "0"
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
    for entry in std::fs::read_dir(&workspace).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy();
        if path.is_file() && (name.starts_with("evidence.sqlite") || name == "sidecar.log") {
            assert!(
                !std::fs::read(&path)
                    .unwrap()
                    .windows(PASSWORD_CANARY.len())
                    .any(|bytes| bytes == PASSWORD_CANARY.as_bytes()),
                "password leaked into {}",
                path.display()
            );
        }
    }
    stop_http.cancel();
    http.await.unwrap();
    stop_sidecar(&mut sidecar).await;
    let _ = std::fs::remove_file(socket);
    drop(lease);
    if ephemeral {
        assert_eq!(db.memory_content_store().retained_bytes(), 0);
    }
    drop(tool);
    drop(db);
    std::fs::remove_dir_all(workspace).unwrap();
}

struct RecordedJourneyProvider {
    input: Value,
    calls: std::sync::atomic::AtomicUsize,
}
impl zk_llm::ChatProvider for RecordedJourneyProvider {
    fn provider_name(&self) -> &'static str {
        "native-recording-fixture"
    }
    fn chat_stream(
        &self,
        _request: zk_llm::ChatRequest,
        _cancel: CancellationToken,
    ) -> Result<futures::stream::BoxStream<'static, zk_llm::ProviderEvent>, zk_llm::ProviderError>
    {
        use zk_llm::{FinishReason, ProviderEvent};
        let events = if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            vec![
                ProviderEvent::ToolUseStart {
                    id: "native-recorded-verifier".into(),
                    name: "VerifyJourney".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "native-recorded-verifier".into(),
                    delta: self.input.to_string(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::ToolUse,
                    usage: Some(zk_protocol::Usage::default()),
                },
            ]
        } else {
            vec![
                ProviderEvent::TextDelta {
                    text: "verification finished".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(zk_protocol::Usage::default()),
                },
            ]
        };
        Ok(Box::pin(futures::stream::iter(events)))
    }
}
#[derive(Default)]
struct NativeRecordingSink(std::sync::Mutex<Vec<zk_protocol::ServerMessage>>);
impl zk_engine::MessageSink for NativeRecordingSink {
    fn push<'a>(
        &'a self,
        _session: &'a str,
        message: zk_protocol::ServerMessage,
    ) -> futures::future::BoxFuture<'a, ()> {
        self.0.lock().unwrap().push(message);
        Box::pin(std::future::ready(()))
    }
}

async fn native_capacity_probe(
    root: &std::path::Path,
    workspace: &std::path::Path,
    after_ack: bool,
) {
    let status = tokio::process::Command::new(root.join("python-service/.venv/bin/python"))
        .args(["-c",r"import json,pathlib,shutil,sys,uuid
from services.browser_recordings import RecordingSpool
spool=RecordingSpool()
manifest=pathlib.Path(sys.argv[1]) / 'capacity-reservations.json'
def reserve():
    batch=str(uuid.uuid4())
    spool.reserve(dict(batch_id=batch,session_id='fixture',run_id='fixture',invocation_id='fixture'),1,{})
    return batch
if sys.argv[2]=='before':
    batches=[reserve() for _ in range(9)]
    manifest.write_text(json.dumps(batches))
    try: reserve()
    except ValueError as exc: assert str(exc)=='RECORDING_FINALIZATION_CAPACITY_REACHED'
    else: raise AssertionError('unacknowledged recording must occupy the tenth slot')
else:
    batches=json.loads(manifest.read_text())
    batches.append(reserve())
    for batch in batches: shutil.rmtree(spool.root / batch)
",workspace.to_str().unwrap(),if after_ack {"after"} else {"before"}])
        .env("PYTHONPATH",root.join("python-service/src"))
        .status().await.unwrap();
    assert!(
        status.success(),
        "actual Python reservation capacity must follow ACK"
    );
}

#[tokio::test]
#[ignore = "requires synchronized native Python/Chromium; isolated private fixture"]
#[allow(clippy::too_many_lines)]
async fn succeeded_recording_engine_faults_recover_after_restart_without_browser_replay() {
    use axum::{
        Router,
        response::Html,
        routing::{get, post},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    const TEST: &str =
        "succeeded_recording_engine_faults_recover_after_restart_without_browser_replay";
    if isolated_recording_process(TEST).await {
        return;
    }
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_owned();
    let workspace = std::env::temp_dir().join(format!(
        "zk-native-success-recovery-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&workspace).unwrap();
    let socket = PathBuf::from(format!("/tmp/zkj-{}.sock", uuid::Uuid::new_v4()));
    let (mut sidecar, client) = start_sidecar(&root, &workspace, &socket).await;
    let effects = Arc::new(AtomicUsize::new(0));
    let router=Router::new().route("/",get(||async {Html(r#"<button id="effect" onclick="fetch('/effect',{method:'POST'}).then(()=>this.textContent='done')">effect</button>"#)}))
        .route("/effect",post({let effects=effects.clone();move || {let effects=effects.clone();async move {effects.fetch_add(1,Ordering::SeqCst);"ok"}}}));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    for (index, fault) in ["evidence", "completion"].into_iter().enumerate() {
        let path = workspace.join(format!("{fault}.sqlite"));
        let db = Db::open(&path).unwrap();
        let session = db
            .create_session("qwen3.8-max-0902", workspace.to_str().unwrap())
            .await
            .unwrap()
            .id;
        db.with_conn_blocking(move |conn| {conn.execute_batch(if fault=="evidence" {
            "CREATE TEMP TRIGGER native_outage BEFORE INSERT ON evidence_bundles WHEN NEW.origin='machine' BEGIN SELECT RAISE(ABORT,'native evidence outage'); END"
        } else {
            "CREATE TEMP TRIGGER native_outage BEFORE UPDATE OF status ON tool_result_postprocessing WHEN NEW.status='completed' BEGIN SELECT RAISE(ABORT,'native journal outage'); END"
        })?;Ok(())}).unwrap();
        let input = json!({"base_url":format!("http://{address}"),"mode":"browser","record":{"har":true},"steps":[{"action":"navigate","url":"/"},{"action":"click","selector":"#effect"},{"action":"wait_for","selector":"#effect:has-text(\"done\")"},{"action":"assert_text","selector":"#effect","expected":"done"}]});
        let provider = Arc::new(RecordedJourneyProvider {
            input,
            calls: AtomicUsize::new(0),
        });
        let sink = Arc::new(NativeRecordingSink::default());
        let mut tools = zk_tools::ToolRegistry::new();
        tools.register(Arc::new(BrowserVerifyJourneyTool::new(
            client.clone(),
            db.clone(),
        )));
        let engine = Arc::new(zk_engine::Engine::with_tools(
            db.clone(),
            provider,
            sink.clone(),
            Arc::new(tools),
        ));
        tokio::time::timeout(
            Duration::from_secs(90),
            engine
                .clone()
                .run_user_message(session.clone(), "verify the local page once".into()),
        )
        .await
        .unwrap();
        assert_eq!(effects.load(Ordering::SeqCst), index + 1);
        let entry = db.pending_browser_recordings().await.unwrap().remove(0);
        assert_eq!(entry.physical_status, "released");
        assert_eq!(entry.state["phase"], "sealed");
        assert!(!entry.recoverable);
        let invocation = entry.state["identity"]["invocation_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let snapshot = db
            .recorded_journey_postprocessing(&invocation)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !snapshot.output_is_error,
            "the original browser Journey actually passed"
        );
        assert_eq!(snapshot.receipt["verdict"], "verified");
        assert!(
            db.find_evidence_by_session(&session)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(!sink.0.lock().unwrap().iter().any(|m|matches!(m,zk_protocol::ServerMessage::ToolResult{tool_use_id,..} if tool_use_id=="native-recorded-verifier")));
        let original = db
            .list_messages(&session, None, 100)
            .await
            .unwrap()
            .unwrap();
        assert!(db.delete_session(&session).await.is_err());
        native_capacity_probe(&root, &workspace, false).await;
        drop(engine);
        drop(db);
        // The temporary SQLite fault disappears with the actual writer process lifetime.
        let db = Db::open(&path).unwrap();
        db.reconcile_runtime_after_restart().await.unwrap();
        std::fs::write(
            workspace.join("drop-ack-once"),
            b"drop response after production consumes",
        )
        .unwrap();
        assert_eq!(
            zk_server::python::tools::reconcile_browser_recordings(&db, &client)
                .await
                .unwrap(),
            0
        );
        let entry = db.pending_browser_recordings().await.unwrap().remove(0);
        assert_eq!(entry.state["phase"], "ackEligible");
        let batch = PathBuf::from(std::env::var_os("ZK_BROWSER_RECORDING_SPOOL").unwrap())
            .join(entry.state["identity"]["batch_id"].as_str().unwrap());
        assert!(
            batch.join("ack.json").exists(),
            "production ACK committed even though its reply was lost"
        );
        assert_eq!(
            zk_server::python::tools::reconcile_browser_recordings(&db, &client)
                .await
                .unwrap(),
            1
        );
        assert!(db.pending_browser_recordings().await.unwrap().is_empty());
        native_capacity_probe(&root, &workspace, true).await;
        assert_eq!(effects.load(Ordering::SeqCst), index + 1);
        assert_eq!(
            std::fs::read_to_string(workspace.join("journey-requests.log"))
                .unwrap()
                .lines()
                .count(),
            index + 1
        );
        assert_eq!(
            db.find_evidence_by_session(&session).await.unwrap().len(),
            1
        );
        let after = db
            .list_messages(&session, None, 100)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(original).unwrap(),
            serde_json::to_value(after).unwrap()
        );
        assert!(
            !db.browser_recording_batch_protected(
                entry.state["identity"]["batch_id"].as_str().unwrap()
            )
            .await
            .unwrap()
        );
        let run = db
            .find_run_by_id(entry.state["identity"]["run_id"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.status, "interrupted");
        let task = db
            .find_runtime_task_by_id(&run.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.status, zk_db::TaskStatus::NeedsAttention);
        // Recording consumption is resolved. The independent Task quarantine
        // deliberately still requires attention; recovery does not invent a result.
        assert!(
            db.delete_session(&session)
                .await
                .unwrap_err()
                .to_string()
                .contains("active tasks")
        );
    }
    stop_sidecar(&mut sidecar).await;
    server.abort();
    let _ = std::fs::remove_file(socket);
    std::fs::remove_dir_all(workspace).unwrap();
}
