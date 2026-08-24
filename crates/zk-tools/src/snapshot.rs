//! 写前快照窄接口——工具侧只声明「把旧内容交出去」，落库实现在组合根。
//!
//! 语义来源（旧仓库只读）：`FileHistoryService.trackAppliedEdit(filePath,
//! originalContent, sessionId, messageId, operation)` → `FileSnapshotRepository
//! .save(FileSnapshot)`（data.db `file_snapshots` 表）。要点逐条对齐：
//! - **写前**取旧内容，新建文件（无旧内容）不产快照（旧
//!   `HISTORY_SNAPSHOT_NOT_APPLICABLE`）；
//! - 旧内容 > 10 MiB 不产快照（旧同阈值判定）；
//! - `message_id` 列写 `tool_use_id`（旧调用点传 `context.toolUseId()`）；
//! - 失败**只告警不阻断**（旧 `HISTORY_SNAPSHOT_PERSIST_FAILED` best-effort）。
//!
//! 依赖方向：zk-tools 不依赖 zk-db（Phase 2 铁律），故以本 trait 反转依赖，
//! 由 zk-server 组合根提供 zk-db 实现（`snapshot_sink.rs`）。

use futures::future::BoxFuture;

/// 快照上限（旧 `trackAppliedEdit` 的 `10 * 1024 * 1024` 逐字对照）。
pub const MAX_SNAPSHOT_BYTES: usize = 10 * 1024 * 1024;

/// 一次写前快照请求（形状对齐旧 `FileSnapshot` 记录的可写列）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRequest {
    /// 会话 ID（`file_snapshots.session_id`，外键指向 sessions）。
    pub session_id: String,
    /// 关联工具调用 ID（写入 `message_id` 列；旧同为 `toolUseId`）。
    pub message_id: Option<String>,
    /// 目标文件绝对路径。
    pub file_path: String,
    /// **写前**内容（旧 `originalContent`）。
    pub content: String,
    /// 操作名（旧 `FileWriteTool` 传 `"write"`）。
    pub operation: String,
}

/// 快照落库出口（组合根实现；实现方须自担错误处理，仅回报成败）。
pub trait SnapshotSink: Send + Sync {
    /// 落一条写前快照；`Err` 携带可记日志的原因（调用方仅告警不阻断）。
    fn capture(&self, request: SnapshotRequest) -> BoxFuture<'_, Result<(), String>>;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// 记录型测试替身（供 `file_write` 单测与跨 crate 集成测试参照）。
    struct RecordingSink {
        seen: Arc<Mutex<Vec<SnapshotRequest>>>,
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

    #[tokio::test]
    async fn sink_is_object_safe_and_records() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink: Arc<dyn SnapshotSink> = Arc::new(RecordingSink {
            seen: Arc::clone(&seen),
        });
        let request = SnapshotRequest {
            session_id: "s1".to_owned(),
            message_id: Some("call-1".to_owned()),
            file_path: "/tmp/a.txt".to_owned(),
            content: "old".to_owned(),
            operation: "write".to_owned(),
        };
        sink.capture(request.clone()).await.expect("captured");
        let seen = seen.lock().expect("lock");
        assert_eq!(seen.as_slice(), [request]);
    }

    #[test]
    fn snapshot_cap_matches_legacy_ten_mib() {
        assert_eq!(MAX_SNAPSHOT_BYTES, 10 * 1024 * 1024);
    }
}
