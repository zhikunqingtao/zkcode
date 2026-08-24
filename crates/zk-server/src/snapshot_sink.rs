//! 写前快照落库实现——`zk-tools` 的 [`SnapshotSink`] 窄接口 × zk-db 的
//! `file_snapshots` 仓储（组合根侧适配，Phase 2 子阶段 2.3）。
//!
//! 依赖方向：zk-tools 不依赖 zk-db（Phase 2 铁律），工具侧只声明「把写前
//! 内容交出去」；本模块在组合根把它接到 [`Db::insert_file_snapshot`]。
//!
//! 语义来源（旧仓库只读）：`FileHistoryService.trackAppliedEdit` →
//! `FileSnapshotRepository.save`。best-effort 语义在工具侧兜底——本实现
//! 仅把 `DbError` 转成可记日志的字符串，不做重试、不吞错。

use zk_db::Db;
use zk_tools::{SnapshotRequest, SnapshotSink};

/// `file_snapshots` 表落库出口（克隆 `Db` 句柄即共享 writer 连接）。
pub struct DbSnapshotSink {
    /// 会话库句柄（写入走 zk-db 的单 writer 串行化）。
    db: Db,
}

impl DbSnapshotSink {
    /// 以 `Db` 句柄装配（组合根调用）。
    #[must_use]
    pub fn new(db: Db) -> Self {
        Self { db }
    }
}

impl SnapshotSink for DbSnapshotSink {
    fn capture(
        &self,
        request: SnapshotRequest,
    ) -> futures::future::BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.db
                .insert_file_snapshot(
                    &request.session_id,
                    request.message_id.as_deref(),
                    &request.file_path,
                    &request.content,
                    &request.operation,
                )
                .await
                .map(|_id| ())
                .map_err(|error| error.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 请求形状 → 表行：会话/路径/内容/操作/`message_id` 全字段落库。
    #[tokio::test]
    async fn capture_persists_request_fields() {
        let db = Db::open_in_memory().expect("boot");
        let session = db
            .create_session("gpt-4o", "/tmp/zk-sink")
            .await
            .expect("session");
        let sink = DbSnapshotSink::new(db.clone());
        sink.capture(SnapshotRequest {
            session_id: session.id.clone(),
            message_id: Some("toolu_1".to_owned()),
            file_path: "/tmp/zk-sink/a.txt".to_owned(),
            content: "before".to_owned(),
            operation: "write".to_owned(),
        })
        .await
        .expect("capture");

        let rows = db.list_file_snapshots(&session.id).await.expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].file_path, "/tmp/zk-sink/a.txt");
        assert_eq!(rows[0].content, "before");
        assert_eq!(rows[0].operation, "write");
        assert_eq!(rows[0].message_id.as_deref(), Some("toolu_1"));
    }

    /// 会话不存在 → `Err(可读原因)`（工具侧据此仅告警不阻断写入）。
    #[tokio::test]
    async fn capture_reports_error_for_unknown_session() {
        let db = Db::open_in_memory().expect("boot");
        let sink = DbSnapshotSink::new(db);
        let error = sink
            .capture(SnapshotRequest {
                session_id: "ghost".to_owned(),
                message_id: None,
                file_path: "/tmp/x.txt".to_owned(),
                content: "before".to_owned(),
                operation: "write".to_owned(),
            })
            .await
            .expect_err("unknown session must fail");
        assert!(error.contains("session not found"), "unexpected: {error}");
    }
}
