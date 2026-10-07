//! Real Read → Edit/Write flows bind raw bytes, encoding and BOM without data loss.

use futures::future::BoxFuture;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
use zk_tools::file_edit::EditFileTool;
use zk_tools::file_read::ReadFileTool;
use zk_tools::file_write::WriteFileTool;
use zk_tools::text_encoding::{TextEncoding, TextFormat};
use zk_tools::{SnapshotRequest, SnapshotSink, Tool, ToolContext};

#[derive(Default)]
struct Capture(Mutex<Vec<SnapshotRequest>>);
impl SnapshotSink for Capture {
    fn capture(&self, request: SnapshotRequest) -> BoxFuture<'_, Result<(), String>> {
        self.0.lock().unwrap().push(request);
        Box::pin(async { Ok(()) })
    }
}
fn context(path: &std::path::Path, session: &str) -> ToolContext {
    let (tx, _) = tokio::sync::mpsc::unbounded_channel();
    ToolContext::new(CancellationToken::new(), tx)
        .with_session_id(session)
        .with_tool_use_id("edit-call")
        .with_authorized_write_path(path)
}
fn root() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("zk-encoded-tools-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&path).unwrap();
    path.canonicalize().unwrap()
}

#[tokio::test]
async fn edit_and_write_preserve_bom_encoding_mixed_newlines_and_snapshot_bytes() {
    let root = root();
    for (encoding, bom, explicit) in [
        (TextEncoding::Utf8, true, None),
        (TextEncoding::Utf16Le, true, None),
        (TextEncoding::Utf16Be, true, None),
        (TextEncoding::Gb18030, false, Some("GB18030")),
        (TextEncoding::Latin1, false, Some("ISO-8859-1")),
    ] {
        let format = TextFormat { encoding, bom };
        let session = uuid::Uuid::new_v4().to_string();
        let path = root.join(format!("{session}.txt"));
        let original = "café\r\nold\rlast\n";
        let bytes = format.encode(original).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        let mut input = json!({"file_path":path});
        if let Some(encoding) = explicit {
            input["encoding"] = json!(encoding);
        }
        let read = ReadFileTool
            .execute(input.clone(), context(&path, &session))
            .await;
        assert!(!read.is_error, "{}", read.content);
        assert_eq!(
            read.metadata.as_ref().unwrap()["structuredResult"]["overwriteEligible"],
            true
        );
        let capture = Arc::new(Capture::default());
        let edit = EditFileTool::with_snapshot_sink(capture.clone())
            .execute(
                json!({"file_path":path,"old_string":"old","new_string":"edited"}),
                context(&path, &session),
            )
            .await;
        assert!(!edit.is_error, "{}", edit.content);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            format.encode("café\r\nedited\rlast\n").unwrap()
        );
        let snapshot = capture.0.lock().unwrap()[0].clone();
        assert_eq!(snapshot.content, original);
        assert_eq!(snapshot.original_bytes, Some(bytes));
        let read = ReadFileTool.execute(input, context(&path, &session)).await;
        assert!(!read.is_error, "{}", read.content);
        let write = WriteFileTool::with_snapshot_sink(capture.clone())
            .execute(
                json!({"file_path":path,"content":"é\r\nreplaced\n"}),
                context(&path, &session),
            )
            .await;
        assert!(!write.is_error, "{}", write.content);
        let expected = format.encode("é\r\nreplaced\n").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        assert_eq!(
            write.metadata.unwrap()["structuredResult"]["bytesWritten"],
            expected.len()
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn fallback_is_preview_only_and_explicit_unrepresentable_writes_leave_bytes_unchanged() {
    let root = root();
    let path = root.join("latin1.txt");
    let session = uuid::Uuid::new_v4().to_string();
    let original = b"caf\xe9\r\n";
    std::fs::write(&path, original).unwrap();
    let read = ReadFileTool
        .execute(json!({"file_path":path}), context(&path, &session))
        .await;
    assert!(!read.is_error);
    assert!(read.content.contains("preview only"));
    assert_eq!(
        read.metadata.unwrap()["structuredResult"]["overwriteEligible"],
        false
    );
    let denied = WriteFileTool::default()
        .execute(
            json!({"file_path":path,"content":"replace"}),
            context(&path, &session),
        )
        .await;
    assert!(denied.is_error && denied.content.contains("FILE_READ_REQUIRED"));
    let explicit = ReadFileTool
        .execute(
            json!({"file_path":path,"encoding":"ISO-8859-1"}),
            context(&path, &session),
        )
        .await;
    assert!(!explicit.is_error);
    let denied = EditFileTool::default()
        .execute(
            json!({"file_path":path,"old_string":"café","new_string":"中文"}),
            context(&path, &session),
        )
        .await;
    assert!(denied.is_error && denied.content.contains("FILE_ENCODING_UNREPRESENTABLE"));
    assert_eq!(std::fs::read(&path).unwrap(), original);
    let foreign = context(&path, "different-session");
    assert!(
        WriteFileTool::default()
            .execute(json!({"file_path":path,"content":"replace"}), foreign)
            .await
            .is_error
    );
    std::fs::remove_dir_all(root).unwrap();
}
