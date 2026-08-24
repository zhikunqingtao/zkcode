//! 2.3 写前快照落库集成测试——`WriteFileTool` × `DbSnapshotSink` × zk-db
//! `file_snapshots` 全链路（组合根侧真实接线，非测试替身）。
//!
//! 判据对照：写覆盖已有文件 → 旧内容成行落 `file_snapshots`；新建文件
//! 不产快照（旧 `FileHistoryService.trackEdit` 语义）。库文件为磁盘 sqlite，
//! 落库结果既经 zk-db reader 断言，也可用 `sqlite3` CLI 直查
//! （设 `ZK_KEEP_SNAPSHOT_DB=1` 保留库文件）。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::json;
use tokio_util::sync::CancellationToken;
use zk_db::Db;
use zk_server::snapshot_sink::DbSnapshotSink;
use zk_tools::{CallEnv, ToolEvent, ToolExecutor, ToolOutput, WriteFileTool};

/// 独占测试目录（库文件 + 工作区同根，便于单次清理）。
fn fixture(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("zk-snapshot-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("mkdir fixture");
    root
}

/// 清理（`ZK_KEEP_SNAPSHOT_DB=1` 时保留，供 `sqlite3` CLI 复核）。
fn cleanup(root: &Path) {
    if std::env::var_os("ZK_KEEP_SNAPSHOT_DB").is_none() {
        let _ = std::fs::remove_dir_all(root);
    } else {
        eprintln!("kept snapshot fixture at {}", root.display());
    }
}

/// 经执行器派发一次 `Write`，注入会话 ID / 工作目录，收敛为最终输出。
async fn write_via_executor(
    tool: Arc<WriteFileTool>,
    session_id: &str,
    working_dir: &Path,
    input: serde_json::Value,
) -> ToolOutput {
    let executor = ToolExecutor::new();
    let cancel = CancellationToken::new();
    let mut rx = executor.spawn_call_in(
        tool,
        "toolu_snap".to_owned(),
        input,
        &cancel,
        CallEnv::new()
            .with_session_id(session_id)
            .with_working_dir(working_dir),
    );
    while let Some(event) = rx.recv().await {
        if let ToolEvent::Finished { output, .. } = event {
            return output;
        }
    }
    panic!("executor closed without Finished");
}

/// 覆盖写：旧内容落 `file_snapshots`（路径/内容/operation/`message_id` 齐全）；
/// 新建写不产快照。
#[tokio::test]
async fn write_tool_persists_pre_write_snapshot() {
    let root = fixture("write");
    let db = Db::open(root.join("data.db")).expect("open file db");
    let session = db
        .create_session("gpt-4o", &root.to_string_lossy())
        .await
        .expect("session");
    let tool = Arc::new(WriteFileTool::with_snapshot_sink(Arc::new(
        DbSnapshotSink::new(db.clone()),
    )));

    // 1) 新建文件：无旧内容 → 无快照。
    let created = write_via_executor(
        Arc::clone(&tool),
        &session.id,
        &root,
        json!({ "file_path": "notes.md", "content": "v1\n" }),
    )
    .await;
    assert!(!created.is_error, "content: {}", created.content);
    assert_eq!(
        created.metadata.as_ref().expect("metadata")["structuredResult"]["snapshot"],
        false
    );
    assert!(
        db.list_file_snapshots(&session.id)
            .await
            .expect("list")
            .is_empty(),
        "new file must not snapshot"
    );

    // 2) 覆盖写：旧内容 `v1\n` 落库。
    let updated = write_via_executor(
        Arc::clone(&tool),
        &session.id,
        &root,
        json!({ "file_path": "notes.md", "content": "v2\n" }),
    )
    .await;
    assert!(!updated.is_error, "content: {}", updated.content);
    assert_eq!(
        updated.metadata.as_ref().expect("metadata")["structuredResult"]["snapshot"],
        true
    );

    let rows = db.list_file_snapshots(&session.id).await.expect("list");
    assert_eq!(rows.len(), 1, "exactly one pre-write snapshot");
    assert_eq!(rows[0].content, "v1\n", "snapshot holds pre-write bytes");
    assert_eq!(rows[0].operation, "write");
    assert_eq!(rows[0].message_id.as_deref(), Some("toolu_snap"));
    assert_eq!(rows[0].file_path, root.join("notes.md").to_string_lossy());
    assert_eq!(
        std::fs::read_to_string(root.join("notes.md")).expect("read"),
        "v2\n",
        "file itself holds post-write bytes"
    );

    drop(db);
    cleanup(&root);
}

/// 无会话 ID（如 REST 侧直调工具）时写入照常成功，仅不产快照——快照是
/// best-effort 旁路，不得影响文件效果。
#[tokio::test]
async fn write_without_session_still_succeeds() {
    let root = fixture("no-session");
    let db = Db::open(root.join("data.db")).expect("open file db");
    std::fs::write(root.join("a.txt"), "old").expect("seed");
    let tool = Arc::new(WriteFileTool::with_snapshot_sink(Arc::new(
        DbSnapshotSink::new(db.clone()),
    )));

    let executor = ToolExecutor::new();
    let cancel = CancellationToken::new();
    let mut rx = executor.spawn_call_in(
        tool,
        "toolu_nosession".to_owned(),
        json!({ "file_path": "a.txt", "content": "new" }),
        &cancel,
        CallEnv::new().with_working_dir(&root),
    );
    let mut output = None;
    while let Some(event) = rx.recv().await {
        if let ToolEvent::Finished { output: done, .. } = event {
            output = Some(done);
        }
    }
    let output = output.expect("Finished");
    assert!(!output.is_error, "content: {}", output.content);
    assert_eq!(
        output.metadata.as_ref().expect("metadata")["structuredResult"]["snapshot"],
        false
    );
    assert_eq!(
        std::fs::read_to_string(root.join("a.txt")).expect("read"),
        "new"
    );

    drop(db);
    cleanup(&root);
}
