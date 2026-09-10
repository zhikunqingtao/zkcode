//! `Write` 工具——带 Read-version CAS 的全量原子写文件。
//!
//! 对照旧 `tool/impl/FileWriteTool.java`（只读权威规格）：工具名 `Write`、
//! 入参 `file_path` / `content`、全量覆盖写、缺失父目录自动创建、原子落盘
//! （临时文件 + rename）、返回文本 `"create: <path>"` / `"update: <path>"`；
//! 写前经 `FileHistoryService.trackAppliedEdit(…, "write")` 落一条
//! `file_snapshots`（新建文件无旧内容 → 不产快照）。
//!
//! 已存在目标必须由同一 Session 完整 Read，并以该物理读取的 SHA-256
//! 作为 [`ExpectedOldState`]；新文件使用 `Absent`。快照落库经
//! [`SnapshotSink`] 反转依赖，未注入 sink 时静默跳过。

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::json;

use crate::atomic::{ExpectedOldState, WriteEffect, write_checked_authorized};
use crate::file_state::{self, session_key};
use crate::input::{failure, required_str, resolve_path};
use crate::snapshot::{MAX_SNAPSHOT_BYTES, SnapshotRequest, SnapshotSink};
use crate::tool::{FileArtifactReceipt, Tool, ToolContext, ToolOutput};

/// 快照 operation 列写入值（旧调用点逐字传 `"write"`）。
const SNAPSHOT_OPERATION: &str = "write";

/// 全量写文件工具（名 `Write`）。
#[derive(Clone, Default)]
pub struct WriteFileTool {
    /// 写前快照出口；`None` = 不落快照（单测 / 无会话上下文场景）。
    sink: Option<Arc<dyn SnapshotSink>>,
}

impl std::fmt::Debug for WriteFileTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WriteFileTool")
            .field("snapshot_sink", &self.sink.is_some())
            .finish()
    }
}

impl WriteFileTool {
    /// 装配（无快照出口）。
    #[must_use]
    pub fn new() -> Self {
        Self { sink: None }
    }

    /// 装配并注入写前快照出口（组合根提供 zk-db 实现）。
    #[must_use]
    pub fn with_snapshot_sink(sink: Arc<dyn SnapshotSink>) -> Self {
        Self { sink: Some(sink) }
    }

    /// 写前快照（旧 `trackAppliedEdit` 逐条对齐：无旧内容 / 超 10 MiB /
    /// 无 `session_id` → 跳过；落库失败仅告警）。
    async fn capture(&self, ctx: &ToolContext, path: &str, previous: Option<&str>) -> bool {
        let (Some(sink), Some(session_id), Some(content)) =
            (self.sink.as_ref(), ctx.session_id(), previous)
        else {
            return false;
        };
        if content.len() > MAX_SNAPSHOT_BYTES {
            tracing::warn!(path, bytes = content.len(), "snapshot skipped: too large");
            return false;
        }
        let request = SnapshotRequest {
            session_id: session_id.to_owned(),
            message_id: ctx.tool_use_id().map(str::to_owned),
            file_path: path.to_owned(),
            content: content.to_owned(),
            operation: SNAPSHOT_OPERATION.to_owned(),
        };
        match sink.capture(request).await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(path, %error, "snapshot persist failed");
                false
            }
        }
    }
}

impl Tool for WriteFileTool {
    fn name(&self) -> &'static str {
        "Write"
    }

    fn description(&self) -> &'static str {
        "Write a file to the local filesystem, overwriting it entirely. \
         Parent directories are created when missing. Existing files require a complete Read \
         in the same session and are replaced with a SHA-256 compare-and-swap."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Absolute path, or path relative to the session workspace."
                },
                "content": {
                    "type": "string",
                    "description": "Full content to write (the file is replaced entirely)."
                }
            },
            "required": ["file_path", "content"]
        })
    }

    /// 自报路径入参（旧 `FileWriteTool.java:101-103`：
    /// `input.has("file_path") ? input.getString("file_path") : null`）。
    fn path_of(&self, input: &serde_json::Value) -> Option<String> {
        input
            .get("file_path")
            .and_then(serde_json::Value::as_str)
            .map(std::borrow::ToOwned::to_owned)
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move { self.run(input, ctx).await })
    }
}

impl WriteFileTool {
    /// 执行主体（校验 → 读旧内容 → 快照 → 建目录 → 原子写）。
    async fn run(&self, input: serde_json::Value, ctx: ToolContext) -> ToolOutput {
        let raw_path = match required_str(&input, "file_path") {
            Ok(value) => value,
            Err(output) => return output,
        };
        let Some(content) = input.get("content").and_then(serde_json::Value::as_str) else {
            return failure(
                "MISSING_PARAMETER",
                "Required parameter 'content' is missing or not a string",
            );
        };
        let path = resolve_path(raw_path, &ctx);
        let display = path.display().to_string();
        let metadata = match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return failure(
                    "FILE_WRITE_SYMLINK_FORBIDDEN",
                    format!("refusing to overwrite symbolic link: {display}"),
                );
            }
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return failure("FILE_WRITE_IO_FAILED", format!("{display}: {error}"));
            }
        };
        if metadata.as_ref().is_some_and(std::fs::Metadata::is_dir) {
            return failure(
                "FILE_WRITE_IO_FAILED",
                format!("{display} is an existing directory"),
            );
        }
        let is_create = metadata.is_none();
        let session = session_key(ctx.session_id());
        let (previous, expected) = if is_create {
            (None, ExpectedOldState::Absent)
        } else {
            let store = file_state::global();
            let Some(expected_hash) = store.read_hash(session, &display) else {
                return failure(
                    "FILE_READ_REQUIRED",
                    "请先使用 Read 工具完整读取文件内容后再覆盖",
                );
            };
            if store.is_stale(session, &display) {
                return failure("FILE_READ_STATE_STALE", "文件已被外部修改，请重新 Read");
            }
            let previous = match tokio::fs::read(&path).await {
                Ok(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
                Err(error) => {
                    return failure("FILE_WRITE_IO_FAILED", format!("{display}: {error}"));
                }
            };
            (previous, ExpectedOldState::sha256(&expected_hash))
        };

        let outcome =
            write_checked_authorized(&path, content, &expected, ctx.authorized_write_path()).await;
        if !outcome.success {
            let reason = outcome.error.as_deref().unwrap_or("ATOMIC_WRITE_FAILED");
            let code = if reason.contains("CONFLICT") {
                "FILE_VERSION_CONFLICT"
            } else if outcome.effect == WriteEffect::Unknown {
                "FILE_WRITE_EFFECT_UNKNOWN"
            } else {
                "FILE_WRITE_IO_FAILED"
            };
            return failure(code, format!("{display}: {reason}"));
        }
        let snapshot = self.capture(&ctx, &display, previous.as_deref()).await;
        // 已读台账失效（对照旧 post-commit `cache.markModified(filePath)`，
        // 位于 `trackAppliedEdit` 之后、返回之前）。
        file_state::global().mark_modified(session_key(ctx.session_id()), &display);
        let kind = if is_create { "create" } else { "update" };
        let operation = if is_create { "created" } else { "modified" };
        let artifact = FileArtifactReceipt::capture(
            &path,
            operation,
            outcome.new_hash.as_deref(),
            content.len(),
        )
        .await;
        if artifact.is_none() {
            tracing::error!(path = %path.display(), "applied Write could not produce an artifact receipt");
        }
        let mut output = ToolOutput::ok(format!("{kind}: {display}"));
        output.metadata = Some(json!({
            "structuredResult": {
                "filePath": display,
                "type": kind,
                "bytesWritten": content.len(),
                "snapshot": snapshot,
                "sealedHash": outcome.new_hash,
                "artifact": artifact,
            }
        }));
        output
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::file_read::ReadFileTool;

    /// 记录型快照出口（断言写前快照的请求形状）。
    #[derive(Default)]
    struct RecordingSink {
        seen: Mutex<Vec<SnapshotRequest>>,
    }

    impl SnapshotSink for RecordingSink {
        fn capture(&self, request: SnapshotRequest) -> BoxFuture<'_, Result<(), String>> {
            self.seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request);
            Box::pin(futures::future::ready(Ok(())))
        }
    }

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
            .with_session_id("s1")
            .with_tool_use_id("call-1")
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-write-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::canonicalize(dir).expect("canonical temp dir")
    }

    #[tokio::test]
    async fn creates_file_and_missing_parents_without_snapshot_and_emits_artifact_receipt() {
        let sink = Arc::new(RecordingSink::default());
        let tool = WriteFileTool::with_snapshot_sink(Arc::clone(&sink) as Arc<dyn SnapshotSink>);
        let path = temp_dir("create").join("nested/deep/a.txt");
        let _ = std::fs::remove_file(&path);
        let output = tool
            .execute(
                json!({ "file_path": path.to_str().expect("utf8"), "content": "hello" }),
                ctx().with_authorized_write_path(&path),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.starts_with("create: "));
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "hello");
        let receipt = output.file_artifact_receipt().expect("artifact receipt");
        assert_eq!(receipt.operation, "created");
        assert_eq!(receipt.file_size, 5);
        assert_eq!(receipt.sealed_hash, crate::sha256_hex(b"hello"));
        assert_eq!(
            std::path::PathBuf::from(receipt.canonical_path),
            std::fs::canonicalize(&path).expect("canonical path")
        );
        assert!(
            sink.seen.lock().expect("lock").is_empty(),
            "new file must not produce a snapshot"
        );
    }

    #[tokio::test]
    async fn overwrites_existing_file_after_snapshot() {
        let sink = Arc::new(RecordingSink::default());
        let tool = WriteFileTool::with_snapshot_sink(Arc::clone(&sink) as Arc<dyn SnapshotSink>);
        let path = temp_dir("update").join("b.txt");
        std::fs::write(&path, "old body").expect("seed");
        let read = ReadFileTool
            .execute(json!({ "file_path": path.to_str().expect("utf8") }), ctx())
            .await;
        assert!(!read.is_error, "{}", read.content);
        let output = tool
            .execute(
                json!({ "file_path": path.to_str().expect("utf8"), "content": "new body" }),
                ctx().with_authorized_write_path(&path),
            )
            .await;
        assert!(output.content.starts_with("update: "));
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "new body");
        let seen = sink.seen.lock().expect("lock");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].session_id, "s1");
        assert_eq!(seen[0].message_id.as_deref(), Some("call-1"));
        assert_eq!(seen[0].content, "old body");
        assert_eq!(seen[0].operation, "write");
        let metadata = output.metadata.expect("metadata");
        assert_eq!(metadata["structuredResult"]["snapshot"], true);
    }

    #[tokio::test]
    async fn rejects_missing_parameters_and_directory_target() {
        let tool = WriteFileTool::new();
        let no_path = tool.execute(json!({ "content": "x" }), ctx()).await;
        assert!(no_path.is_error);
        assert!(no_path.content.starts_with("MISSING_PARAMETER: "));

        let no_content = tool
            .execute(json!({ "file_path": "/tmp/x.txt" }), ctx())
            .await;
        assert!(no_content.is_error);
        assert!(no_content.content.starts_with("MISSING_PARAMETER: "));

        let dir = temp_dir("dir-target");
        let on_dir = tool
            .execute(
                json!({ "file_path": dir.to_str().expect("utf8"), "content": "x" }),
                ctx(),
            )
            .await;
        assert!(on_dir.is_error);
        assert!(on_dir.content.starts_with("FILE_WRITE_IO_FAILED: "));
    }

    #[tokio::test]
    async fn skips_snapshot_without_session_id() {
        let sink = Arc::new(RecordingSink::default());
        let tool = WriteFileTool::with_snapshot_sink(Arc::clone(&sink) as Arc<dyn SnapshotSink>);
        let path = temp_dir("no-session").join("c.txt");
        std::fs::write(&path, "old").expect("seed");
        let (tx, _rx) = mpsc::unbounded_channel();
        let bare = ToolContext::new(CancellationToken::new(), tx).with_authorized_write_path(&path);
        let read = ReadFileTool
            .execute(json!({ "file_path": path.to_str().expect("utf8") }), bare)
            .await;
        assert!(!read.is_error, "{}", read.content);
        let (tx, _rx) = mpsc::unbounded_channel();
        let bare = ToolContext::new(CancellationToken::new(), tx).with_authorized_write_path(&path);
        let output = tool
            .execute(
                json!({ "file_path": path.to_str().expect("utf8"), "content": "new" }),
                bare,
            )
            .await;
        assert!(!output.is_error);
        assert!(sink.seen.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn existing_file_requires_complete_read_and_rejects_stale_hash() {
        let tool = WriteFileTool::new();
        let path = temp_dir("read-version").join("versioned.txt");
        std::fs::write(&path, "first\n").expect("seed");
        let input = json!({
            "file_path": path.to_str().expect("utf8"),
            "content": "replacement\n"
        });

        let unread = tool.execute(input.clone(), ctx()).await;
        assert!(unread.is_error);
        assert!(unread.content.starts_with("FILE_READ_REQUIRED:"));

        let partial = ReadFileTool
            .execute(
                json!({ "file_path": path.to_str().expect("utf8"), "offset": 2 }),
                ctx(),
            )
            .await;
        assert!(!partial.is_error);
        let partial_write = tool.execute(input.clone(), ctx()).await;
        assert!(partial_write.content.starts_with("FILE_READ_REQUIRED:"));

        let complete = ReadFileTool
            .execute(json!({ "file_path": path.to_str().expect("utf8") }), ctx())
            .await;
        assert!(!complete.is_error);
        std::fs::write(&path, "raced\n").expect("external mutation");
        let raced = tool
            .execute(input, ctx().with_authorized_write_path(&path))
            .await;
        assert!(raced.is_error);
        assert!(
            raced.content.starts_with("FILE_READ_STATE_STALE:")
                || raced.content.starts_with("FILE_VERSION_CONFLICT:"),
            "{}",
            raced.content
        );
        assert_eq!(std::fs::read_to_string(path).expect("read"), "raced\n");
    }
}
