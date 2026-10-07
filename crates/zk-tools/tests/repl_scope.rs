//! Actual Python/Node/Ruby Run-owned interpreters and physical cleanup.
#![cfg(unix)]
use futures::future::BoxFuture;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use zk_tools::{
    ExecutionResourceAllocation, ExecutionResourceLease, ExecutionResourceObserver,
    ExecutionResourceOwner, ExecutionResourceTerminal, RunToolScopeFactory, ToolContext,
    ToolRegistry,
};
#[derive(Default)]
struct Ledger {
    resources: Mutex<HashMap<String, (Option<u32>, bool)>>,
    fail_bind: AtomicBool,
    fail_finish: AtomicBool,
}
impl ExecutionResourceObserver for Ledger {
    fn register(
        &self,
        _: ExecutionResourceOwner,
        allocation: ExecutionResourceAllocation,
    ) -> BoxFuture<'static, Result<ExecutionResourceLease, String>> {
        assert_eq!(allocation.resource_kind, "processGroup");
        let lease = ExecutionResourceLease {
            resource_id: allocation.resource_id,
        };
        self.resources
            .lock()
            .unwrap()
            .insert(lease.resource_id.clone(), (None, false));
        Box::pin(async { Ok(lease) })
    }
    fn bind_external(
        &self,
        lease: ExecutionResourceLease,
        id: String,
    ) -> BoxFuture<'static, Result<(), String>> {
        self.resources
            .lock()
            .unwrap()
            .get_mut(&lease.resource_id)
            .unwrap()
            .0 = Some(id.parse().unwrap());
        let failed = self.fail_bind.load(Ordering::SeqCst);
        Box::pin(async move {
            if failed {
                Err("injected ownership persistence failure".into())
            } else {
                Ok(())
            }
        })
    }
    fn finish(
        &self,
        lease: ExecutionResourceLease,
        status: ExecutionResourceTerminal,
    ) -> BoxFuture<'static, Result<(), String>> {
        let failed = self.fail_finish.load(Ordering::SeqCst);
        if !failed {
            self.resources
                .lock()
                .unwrap()
                .get_mut(&lease.resource_id)
                .unwrap()
                .1 = status == ExecutionResourceTerminal::Released;
        }
        Box::pin(async move {
            if failed {
                Err("injected finish failure".into())
            } else {
                Ok(())
            }
        })
    }
}
fn fixture(ledger: Arc<Ledger>, root: &std::path::Path) -> (ToolContext, Arc<ToolRegistry>) {
    let (tx, _) = tokio::sync::mpsc::unbounded_channel();
    let context = ToolContext::new(CancellationToken::new(), tx)
        .with_session_id("owned-session")
        .with_run_id("owned-run")
        .with_ephemeral_content(true)
        .with_working_dir(root)
        .with_execution_resources(
            ExecutionResourceOwner {
                task_id: "task".into(),
                run_id: "owned-run".into(),
                invocation_id: "real-setup".into(),
            },
            ledger,
        );
    let base = Arc::new(ToolRegistry::new());
    base.register_dynamic(Arc::new(zk_tools::REPLTool::new(Arc::new(
        zk_tools::ReplManager::new(),
    ))));
    (context, base)
}
fn root() -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!("zk-repl-owned-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    root
}
fn released(ledger: &Ledger) {
    let entries = ledger.resources.lock().unwrap();
    assert!(!entries.is_empty());
    for (pid, done) in entries.values() {
        assert!(*done);
        if let Some(pid) = pid {
            assert_eq!(
                nix::sys::signal::killpg(
                    nix::unistd::Pid::from_raw(i32::try_from(*pid).unwrap()),
                    None
                ),
                Err(nix::errno::Errno::ESRCH)
            );
        }
    }
}
#[tokio::test]
#[allow(clippy::too_many_lines)] // One native multi-language lifecycle scenario.
async fn three_languages_retain_local_state_report_errors_bound_output_and_disable_history() {
    let root = root();
    let ledger = Arc::new(Ledger::default());
    let (context, base) = fixture(ledger.clone(), &root);
    let scope = zk_tools::repl::ReplRunScopeFactory
        .prepare(context.clone(), base)
        .await
        .unwrap();
    let tool = scope.registry().get("REPL").unwrap();
    for (language, code, big, fail) in [
        (
            "python",
            "value=40\nvalue+2",
            "print('x'*200000)",
            "raise ValueError('expected failure')",
        ),
        (
            "node",
            "var value=40; value+2",
            "console.log('x'.repeat(200000))",
            "throw new Error('expected failure')",
        ),
        (
            "ruby",
            "value=40; value+2",
            "puts 'x'*200000",
            "raise 'expected failure'",
        ),
    ] {
        let first = tool
            .execute(
                json!({"language":language,"code":code,"sessionId":language}),
                context.clone(),
            )
            .await;
        assert!(!first.is_error, "{language}: {}", first.content);
        assert_eq!(first.content.trim(), "42");
        let second = tool
            .execute(
                json!({"language":language,"code":"value+3","session_id":language}),
                context.clone(),
            )
            .await;
        assert!(!second.is_error, "{language}: {}", second.content);
        assert_eq!(second.content.trim(), "43");
        let error = tool
            .execute(
                json!({"language":language,"code":fail,"sessionId":language}),
                context.clone(),
            )
            .await;
        assert!(error.is_error);
        assert!(error.content.contains("expected failure"));
        let output = tool
            .execute(
                json!({"language":language,"code":big,"sessionId":language}),
                context.clone(),
            )
            .await;
        assert!(!output.is_error, "{language}: {}", output.content);
        assert!(output.content.len() <= 102_400);
        assert_eq!(output.metadata.unwrap()["truncated"], true);
    }
    let awaited = tool
        .execute(
            json!({"language":"node","code":"await Promise.resolve(value+4)","sessionId":"node"}),
            context.clone(),
        )
        .await;
    assert!(!awaited.is_error, "{}", awaited.content);
    assert_eq!(awaited.content.trim(), "44");
    let mismatch = tool
        .execute(
            json!({"language":"ruby","code":"1","sessionId":"python"}),
            context.clone(),
        )
        .await;
    assert!(mismatch.is_error);
    let foreign = tool
        .execute(
            json!({"code":"1","sessionId":"python"}),
            context.clone().with_session_id("other-session"),
        )
        .await;
    assert!(foreign.is_error);
    let foreign = tool
        .execute(
            json!({"code":"1","sessionId":"python"}),
            context.clone().with_run_id("other-run"),
        )
        .await;
    assert!(foreign.is_error);
    let history=tool.execute(json!({"code":"import sys,os\nprint(sys.dont_write_bytecode,os.environ['PYTHONHISTFILE'])","sessionId":"python"}),context.clone()).await;
    assert!(history.content.contains("True /dev/null"));
    assert_eq!(
        std::fs::read_dir(&root).unwrap().count(),
        0,
        "REPL generated a source/history/output file"
    );
    let fourth = tool
        .execute(
            json!({"language":"ruby","code":"6*7","sessionId":"fourth"}),
            context.clone(),
        )
        .await;
    assert!(!fourth.is_error, "{}", fourth.content);
    assert_eq!(fourth.metadata.unwrap()["activeSessions"], 3);
    assert_eq!(ledger.resources.lock().unwrap().len(), 4);
    assert_eq!(
        ledger
            .resources
            .lock()
            .unwrap()
            .values()
            .filter(|(_, released)| *released)
            .count(),
        1,
        "LRU eviction must prove physical release"
    );
    scope.cleanup().await.unwrap();
    scope.cleanup().await.unwrap();
    assert!(!context.cancel.is_cancelled());
    released(&ledger);
    assert_eq!(ledger.resources.lock().unwrap().len(), 4);
    drop(scope);
    std::fs::remove_dir(root).unwrap();
}
#[tokio::test]
async fn failed_binding_never_runs_code_and_failed_cleanup_retries_without_restarting() {
    let root = root();
    let ledger = Arc::new(Ledger::default());
    ledger.fail_bind.store(true, Ordering::SeqCst);
    let (context, base) = fixture(ledger.clone(), &root);
    let scope = zk_tools::repl::ReplRunScopeFactory
        .prepare(context.clone(), base)
        .await
        .unwrap();
    let tool = scope.registry().get("REPL").unwrap();
    let result = tool
        .execute(
            json!({"code":"open('should-not-exist','w').write('bad')","sessionId":"one"}),
            context.clone(),
        )
        .await;
    assert!(result.is_error);
    assert!(!root.join("should-not-exist").exists());
    released(&ledger);
    ledger.fail_bind.store(false, Ordering::SeqCst);
    let result = tool
        .execute(json!({"code":"1+1","sessionId":"two"}), context.clone())
        .await;
    assert!(!result.is_error, "{}", result.content);
    ledger.fail_finish.store(true, Ordering::SeqCst);
    assert!(scope.cleanup().await.is_err());
    ledger.fail_finish.store(false, Ordering::SeqCst);
    scope.cleanup().await.unwrap();
    released(&ledger);
    assert!(!context.cancel.is_cancelled());
    std::fs::remove_dir(root).unwrap();
}
#[tokio::test]
async fn cancelling_an_inflight_repl_stops_its_group_and_does_not_replay() {
    let root = root();
    let ledger = Arc::new(Ledger::default());
    let (context, base) = fixture(ledger.clone(), &root);
    let scope = zk_tools::repl::ReplRunScopeFactory
        .prepare(context.clone(), base)
        .await
        .unwrap();
    let tool = scope.registry().get("REPL").unwrap();
    let ready = tool
        .execute(json!({"code":"1","sessionId":"one"}), context.clone())
        .await;
    assert!(!ready.is_error);
    let mut call = context.clone();
    call.cancel = context.cancel.child_token();
    let cancel = call.cancel.clone();
    let invocation = tokio::spawn(async move {
        tool.execute(
            json!({"code":"import time\ntime.sleep(60)","sessionId":"one"}),
            call,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_secs(10), invocation)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_error);
    assert!(result.content.contains("REPL_CANCELLED"));
    assert!(!context.cancel.is_cancelled());
    scope.cleanup().await.unwrap();
    released(&ledger);
    assert_eq!(ledger.resources.lock().unwrap().len(), 1);
    std::fs::remove_dir(root).unwrap();
}
