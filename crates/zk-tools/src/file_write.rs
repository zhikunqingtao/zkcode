//! `Write` 工具——全量写文件（建必要目录 + 写前快照）。
//!
//! 对照旧 `tool/impl/FileWriteTool.java`（只读权威规格）：工具名 `Write`、
//! 入参 `file_path` / `content`、全量覆盖写、缺失父目录自动创建、原子落盘
//! （临时文件 + rename）、返回文本 `"create: <path>"` / `"update: <path>"`；
//! 写前经 `FileHistoryService.trackAppliedEdit(…, "write")` 落一条
//! `file_snapshots`（新建文件无旧内容 → 不产快照）。
//!
//! 差异（留痕 docs/compatibility.md §4）：旧实现另有「读后写」SHA-256
//! 冲突检测（依赖会话级 `FileFreshnessService` 读取台账），台账属 2.5
//! 权限/会话状态面，本阶段不实现；快照落库经 [`SnapshotSink`] 反转依赖，
//! 未注入 sink 或 [`ToolContext::session_id`] 缺失时静默跳过（旧亦为
//! best-effort，失败只告警不阻断写入）。

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::json;

use crate::file_state::{self, session_key};
use crate::input::{failure, required_str, resolve_path};
use crate::snapshot::{MAX_SNAPSHOT_BYTES, SnapshotRequest, SnapshotSink};
use crate::tool::{Tool, ToolContext, ToolOutput};

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
         Parent directories are created when missing."
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
        if path.is_dir() {
            return failure(
                "FILE_WRITE_IO_FAILED",
                format!("{display} is an existing directory"),
            );
        }
        let previous = tokio::fs::read(&path)
            .await
            .ok()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned());
        let is_create = previous.is_none();
        let snapshot = self.capture(&ctx, &display, previous.as_deref()).await;
        if let Some(parent) = path.parent()
            && let Err(error) = tokio::fs::create_dir_all(parent).await
        {
            return failure(
                "FILE_WRITE_IO_FAILED",
                format!("{}: {error}", parent.display()),
            );
        }
        if let Err(error) = atomic_write(&path, content).await {
            return failure("FILE_WRITE_IO_FAILED", format!("{display}: {error}"));
        }
        // 已读台账失效（对照旧 post-commit `cache.markModified(filePath)`，
        // 位于 `trackAppliedEdit` 之后、返回之前）。
        file_state::global().mark_modified(session_key(ctx.session_id()), &display);
        let kind = if is_create { "create" } else { "update" };
        let mut output = ToolOutput::ok(format!("{kind}: {display}"));
        output.metadata = Some(json!({
            "structuredResult": {
                "filePath": display,
                "type": kind,
                "bytesWritten": content.len(),
                "snapshot": snapshot,
            }
        }));
        output
    }
}

/// 原子落盘：同目录临时文件 + rename（对照旧 `Files.move(…,
/// ATOMIC_MOVE)`；rename 失败回落直写，避免跨设备场景失败）。
async fn atomic_write(path: &std::path::Path, content: &str) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return tokio::fs::write(path, content).await;
    };
    let name = path.file_name().map_or_else(
        || std::ffi::OsString::from("zk-write"),
        std::ffi::OsStr::to_os_string,
    );
    let mut temp_name = name;
    temp_name.push(format!(".zk-tmp-{}", std::process::id()));
    let temp = parent.join(temp_name);
    tokio::fs::write(&temp, content).await?;
    if tokio::fs::rename(&temp, path).await.is_ok() {
        return Ok(());
    }
    // rename 失败（跨设备 / 权限等）→ 回落为直写，并清理残留临时文件。
    let result = tokio::fs::write(path, content).await;
    let _ = tokio::fs::remove_file(&temp).await;
    result
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

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
        dir
    }

    #[tokio::test]
    async fn creates_file_and_missing_parents_without_snapshot() {
        let sink = Arc::new(RecordingSink::default());
        let tool = WriteFileTool::with_snapshot_sink(Arc::clone(&sink) as Arc<dyn SnapshotSink>);
        let path = temp_dir("create").join("nested/deep/a.txt");
        let _ = std::fs::remove_file(&path);
        let output = tool
            .execute(
                json!({ "file_path": path.to_str().expect("utf8"), "content": "hello" }),
                ctx(),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.starts_with("create: "));
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "hello");
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
        let output = tool
            .execute(
                json!({ "file_path": path.to_str().expect("utf8"), "content": "new body" }),
                ctx(),
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
        let bare = ToolContext::new(CancellationToken::new(), tx);
        let output = tool
            .execute(
                json!({ "file_path": path.to_str().expect("utf8"), "content": "new" }),
                bare,
            )
            .await;
        assert!(!output.is_error);
        assert!(sink.seen.lock().expect("lock").is_empty());
    }
}
