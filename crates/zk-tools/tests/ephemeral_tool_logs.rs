//! Sink failures can embed full input bodies; diagnostic events must never serialize them.
use futures::future::BoxFuture;
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use zk_tools::{
    EditFileTool, NotebookEditTool, ReadFileTool, SnapshotRequest, SnapshotSink, Tool, ToolContext,
    WriteFileTool,
};

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<String>>);
struct Fields<'a>(&'a mut String);
impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = write!(self.0, "{}={value:?};", field.name());
    }
}
impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        attributes.record(&mut Fields(&mut self.0.lock().unwrap()));
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, values: &Record<'_>) {
        values.record(&mut Fields(&mut self.0.lock().unwrap()));
    }
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        event.record(&mut Fields(&mut self.0.lock().unwrap()));
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}
struct LeakySink(AtomicUsize);
impl SnapshotSink for LeakySink {
    fn capture(&self, request: SnapshotRequest) -> BoxFuture<'_, Result<(), String>> {
        self.0.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            Err(format!(
                "PRIVATE_ERROR {} {}",
                request.file_path, request.content
            ))
        })
    }
}
#[tokio::test(flavor = "current_thread")]
async fn real_file_tools_never_log_private_paths_bodies_or_sink_errors() {
    let capture = Capture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    let sink = Arc::new(LeakySink(AtomicUsize::new(0)));
    let root = std::env::temp_dir().join(format!("PRIVATE_PATH_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let root = root.canonicalize().unwrap();
    for ephemeral in [false, true] {
        let session = format!("log-test-{}", uuid::Uuid::new_v4());
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        let ctx = ToolContext::new(CancellationToken::new(), tx)
            .with_session_id(&session)
            .with_tool_use_id("test-tool")
            .with_working_dir(&root)
            .with_ephemeral_content(ephemeral);
        let write_path = root.join(format!("PRIVATE_WRITE_{ephemeral}.txt"));
        std::fs::write(&write_path, "PRIVATE_BODY original").unwrap();
        let read = ReadFileTool
            .execute(json!({"file_path":write_path}), ctx.clone())
            .await;
        assert!(!read.is_error, "{}", read.content);
        let output = WriteFileTool::with_snapshot_sink(sink.clone())
            .execute(
                json!({"file_path":write_path,"content":"authorized update"}),
                ctx.clone().with_authorized_write_path(&write_path),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        let edit_path = root.join(format!("PRIVATE_EDIT_{ephemeral}.txt"));
        std::fs::write(&edit_path, "PRIVATE_BODY original").unwrap();
        let read = ReadFileTool
            .execute(json!({"file_path":edit_path}), ctx.clone())
            .await;
        assert!(!read.is_error, "{}", read.content);
        let output=EditFileTool::with_snapshot_sink(sink.clone()).execute(json!({"file_path":edit_path,"old_string":"original","new_string":"authorized update"}),ctx.clone().with_authorized_write_path(&edit_path)).await;
        assert!(!output.is_error, "{}", output.content);
        let notebook = root.join(format!("PRIVATE_NOTEBOOK_{ephemeral}.ipynb"));
        let original=json!({"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[{"id":"one","cell_type":"code","source":["PRIVATE_BODY original"],"metadata":{},"outputs":[],"execution_count":null},{"id":"two","cell_type":"markdown","source":["second"],"metadata":{}}]}).to_string();
        std::fs::write(&notebook, &original).unwrap();
        let read = ReadFileTool
            .execute(json!({"file_path":notebook}), ctx.clone())
            .await;
        assert!(!read.is_error, "{}", read.content);
        let output = NotebookEditTool::with_snapshot_sink(sink.clone())
            .execute(
                json!({"path":notebook,"action":"delete_cell","index":1}),
                ctx.with_authorized_write_path(&notebook),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
    }
    assert_eq!(
        sink.0.load(Ordering::Acquire),
        6,
        "every actual tool must reach its erroring sink"
    );
    let log = capture.0.lock().unwrap().clone();
    assert_eq!(
        log.matches("HISTORY_SNAPSHOT_PERSIST_FAILED").count(),
        6,
        "all failures remain diagnosable: {log}"
    );
    for private in [
        "PRIVATE_PATH",
        "PRIVATE_BODY",
        "PRIVATE_ERROR",
        "PRIVATE_WRITE",
        "PRIVATE_EDIT",
        "PRIVATE_NOTEBOOK",
    ] {
        assert!(
            !log.contains(private),
            "private data leaked into diagnostic fields: {log}"
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}

struct LeakyResources;
impl zk_tools::ExecutionResourceObserver for LeakyResources {
    fn register(
        &self,
        _: zk_tools::ExecutionResourceOwner,
        allocation: zk_tools::ExecutionResourceAllocation,
    ) -> BoxFuture<'static, Result<zk_tools::ExecutionResourceLease, String>> {
        Box::pin(async move {
            Ok(zk_tools::ExecutionResourceLease {
                resource_id: allocation.resource_id,
            })
        })
    }
    fn bind_external(
        &self,
        _: zk_tools::ExecutionResourceLease,
        _: String,
    ) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
    fn finish(
        &self,
        _: zk_tools::ExecutionResourceLease,
        _: zk_tools::ExecutionResourceTerminal,
    ) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Err("PRIVATE_BODY PRIVATE_ERROR PRIVATE_PATH".into()) })
    }
}
#[tokio::test(flavor = "current_thread")]
async fn failed_resource_cleanup_keeps_unconfirmed_state_without_logging_arbitrary_observer_error()
{
    let capture = Capture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    let (tx, _) = tokio::sync::mpsc::unbounded_channel();
    let ctx = ToolContext::new(CancellationToken::new(), tx)
        .with_ephemeral_content(true)
        .with_execution_resources(
            zk_tools::ExecutionResourceOwner {
                task_id: "task".into(),
                run_id: "run".into(),
                invocation_id: "invocation".into(),
            },
            Arc::new(LeakyResources),
        );
    ctx.register_execution_resource("processGroup", None, json!({"private":"PRIVATE_BODY"}))
        .await
        .unwrap();
    ctx.force_unconfirmed_execution_resources().await;
    assert_eq!(
        ctx.execution_cleanup_status(),
        zk_tools::ToolCleanupStatus::Unconfirmed
    );
    let log = capture.0.lock().unwrap().clone();
    assert!(
        log.contains("EXECUTION_RESOURCE_UNCONFIRMED_PERSIST_FAILED"),
        "{log}"
    );
    assert!(!log.contains("PRIVATE_"), "{log}");
}
