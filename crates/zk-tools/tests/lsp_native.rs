//! Explicit native acceptance of the installed five-language bundle.
use futures::future::BoxFuture;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use zk_tools::{
    ExecutionResourceAllocation, ExecutionResourceLease, ExecutionResourceObserver,
    ExecutionResourceOwner, ExecutionResourceTerminal, RunToolScopeFactory, ToolContext,
    ToolRegistry,
};

#[derive(Default)]
struct Ledger(Mutex<HashMap<String, (Option<String>, bool)>>);
impl ExecutionResourceObserver for Ledger {
    fn register(
        &self,
        _owner: ExecutionResourceOwner,
        allocation: ExecutionResourceAllocation,
    ) -> BoxFuture<'static, Result<ExecutionResourceLease, String>> {
        let lease = ExecutionResourceLease {
            resource_id: allocation.resource_id,
        };
        self.0
            .lock()
            .unwrap()
            .insert(lease.resource_id.clone(), (None, false));
        Box::pin(async move { Ok(lease) })
    }
    fn bind_external(
        &self,
        lease: ExecutionResourceLease,
        id: String,
    ) -> BoxFuture<'static, Result<(), String>> {
        self.0
            .lock()
            .unwrap()
            .get_mut(&lease.resource_id)
            .unwrap()
            .0 = Some(id);
        Box::pin(async { Ok(()) })
    }
    fn finish(
        &self,
        lease: ExecutionResourceLease,
        status: ExecutionResourceTerminal,
    ) -> BoxFuture<'static, Result<(), String>> {
        self.0
            .lock()
            .unwrap()
            .get_mut(&lease.resource_id)
            .unwrap()
            .1 = status == ExecutionResourceTerminal::Released;
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
#[ignore = "requires ./dev bootstrap private language-server bundle; run explicitly for release"]
#[allow(clippy::too_many_lines)] // One native multi-language lifecycle scenario.
async fn five_real_language_servers_find_symbols_and_release_process_groups() {
    let root =
        TempRoot(std::env::temp_dir().join(format!("zk-lsp-native-{}", uuid::Uuid::new_v4())));
    std::fs::create_dir(&root.0).unwrap();
    let manifest = zk_tools::lsp::default_manifest_path();
    assert!(
        manifest.is_file(),
        "language-server manifest missing: {}",
        manifest.display()
    );
    let run_id = format!("native-lsp-{}", uuid::Uuid::new_v4());
    let run_state = manifest.parent().unwrap().join("state").join(&run_id);
    let observer = Arc::new(Ledger::default());
    let (sender, _) = tokio::sync::mpsc::unbounded_channel();
    let context = ToolContext::new(CancellationToken::new(), sender)
        .with_working_dir(root.0.as_path())
        .with_session_id("native-lsp-session")
        .with_run_id(&run_id)
        .with_execution_resources(
            ExecutionResourceOwner {
                task_id: "native-task".into(),
                run_id: run_id.clone(),
                invocation_id: "native-setup".into(),
            },
            observer.clone(),
        );
    let scope = zk_tools::lsp::LspRunScopeFactory::new(manifest)
        .prepare(context.clone(), Arc::new(ToolRegistry::new()))
        .await
        .unwrap();
    let tool = scope.registry().get("LSP").unwrap();
    let cases = [
        (
            "typescript",
            "sample.ts",
            "export function nativeAnswer(value: number): number { return value + 1; }\nconst result = nativeAnswer(2);\n",
            None,
        ),
        (
            "python",
            "sample.py",
            "def nativeAnswer(value: int) -> int:\n    return value + 1\nresult = nativeAnswer(2)\n",
            None,
        ),
        (
            "rust",
            "src/lib.rs",
            "pub fn native_answer(value: i32) -> i32 { value + 1 }\n",
            Some((
                "Cargo.toml",
                "[package]\nname = \"native_lsp_fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
            )),
        ),
        (
            "go",
            "sample.go",
            "package sample\nfunc NativeAnswer(value int) int { return value + 1 }\n",
            Some(("go.mod", "module example.com/native\ngo 1.27.0\n")),
        ),
        (
            "java",
            "Sample.java",
            "public class Sample { public static int nativeAnswer(int value) { return value + 1; } }\n",
            None,
        ),
    ];
    for (language, file, source, config) in cases {
        let workspace = root.0.as_path().join(language);
        std::fs::create_dir_all(&workspace).unwrap();
        let path = workspace.join(file);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, source).unwrap();
        if let Some((file, content)) = config {
            std::fs::write(workspace.join(file), content).unwrap();
        }
        let started = std::time::Instant::now();
        let mut success = false;
        while started.elapsed() < Duration::from_mins(2) {
            let output = tool
                .execute(
                    json!({"action":"symbols","file_path":path,"language":language}),
                    context.clone().with_working_dir(&workspace),
                )
                .await;
            eprintln!("{language}: {}", output.content);
            if !output.is_error
                && (output.content.contains("nativeAnswer")
                    || output.content.contains("native_answer")
                    || output.content.contains("NativeAnswer"))
            {
                success = true;
                break;
            }
            if output.is_error {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if !success {
            scope.cleanup().await.unwrap();
            panic!("real {language} language server did not return the fixture symbol");
        }
        if language == "typescript" {
            for operation in [
                "hover",
                "goToDefinition",
                "findReferences",
                "prepareCallHierarchy",
                "incomingCalls",
                "outgoingCalls",
                "goToImplementation",
            ] {
                let output = tool
                    .execute(
                        json!({"operation":operation,"filePath":path,"line":1,"character":18}),
                        context.clone().with_working_dir(&workspace),
                    )
                    .await;
                assert!(!output.is_error, "{operation}: {}", output.content);
                let payload: serde_json::Value = serde_json::from_str(&output.content).unwrap();
                if matches!(
                    operation,
                    "hover" | "goToDefinition" | "findReferences" | "prepareCallHierarchy"
                ) {
                    assert!(
                        !payload["result"].is_null() && payload["result"] != json!([]),
                        "{operation}: {payload}"
                    );
                }
            }
            let output = tool.execute(json!({"operation":"workspaceSymbol","language":"typescript","query":"nativeAnswer"}), context.clone().with_working_dir(&workspace)).await;
            assert!(
                !output.is_error && output.content.contains("nativeAnswer"),
                "{}",
                output.content
            );
        }
    }
    scope.cleanup().await.unwrap();
    scope.cleanup().await.unwrap();
    assert!(!context.cancel.is_cancelled());
    assert!(!run_state.exists(), "owned LSP indexes were not cleaned");
    let records = observer.0.lock().unwrap();
    assert_eq!(records.len(), 5);
    for (pid, released) in records.values() {
        assert!(*released);
        let pid = pid.as_ref().unwrap().parse::<i32>().unwrap();
        assert!(matches!(
            nix::sys::signal::killpg(nix::unistd::Pid::from_raw(pid), None),
            Err(nix::errno::Errno::ESRCH)
        ));
    }
}

struct TempRoot(std::path::PathBuf);
impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
