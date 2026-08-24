//! 文件快照域仓储——[`Db`](crate::Db) 的 `file_snapshots` 表写入与查询
//! （多文件 impl 之一，Phase 2 子阶段 2.3）。
//!
//! 语义来源（旧仓库只读，2026-08-16 冻结）：
//! - `FileSnapshotRepository.save`（七列全量 INSERT，主键 `UUIDv4`）；
//! - `FileSnapshotRepository.findBySessionId` / `findByMessageId` /
//!   `findLatestBySessionAndPath`（`ORDER BY created_at` 升序 / 升序 /
//!   降序 LIMIT 1）；
//! - 调用时机与跳过规则来自 `FileHistoryService.trackEdit`：**写前**快照、
//!   仅文件已存在时保存（新建不快照）、>10MB 跳过——该三条判定在工具层
//!   （`zk-tools::file_write`）完成，本层只负责落库。
//!
//! `operation` 取值沿用旧系统的自由文本约定（`"write"` / `"edit"` /
//! `"rewind"`），不在存储层做枚举收敛（旧 DDL 亦为 `TEXT DEFAULT 'edit'`）。

use rusqlite::{OptionalExtension, params};

use crate::error::{DbError, map_fk_violation};
use crate::time::{format_rfc3339_micros, now_millis};

/// 快照记录（`file_snapshots` 表整行；对齐旧 `FileSnapshot` record 的七元组）。
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSnapshotRecord {
    /// 主键（`UUIDv4`，插入时生成）。
    pub id: String,
    /// 所属会话（外键 → `sessions.id`，`ON DELETE CASCADE`）。
    pub session_id: String,
    /// 触发写入的消息（逻辑外键，无 DDL 约束；引擎暂未透传时为 `None`）。
    pub message_id: Option<String>,
    /// 被快照文件的绝对路径（原样存储，不做规范化）。
    pub file_path: String,
    /// 写前内容（UTF-8 文本；旧系统同样以字符串存入 BLOB 列）。
    pub content: String,
    /// 操作名（`"write"` / `"edit"` / `"rewind"`）。
    pub operation: String,
    /// 落库时刻（RFC 3339，恒 6 位微秒，见 `time` 模块）。
    pub created_at: String,
}

/// 快照行 SELECT（列清单与 [`map_snapshot_row`] 互锁）。
const SNAPSHOT_SELECT: &str = "SELECT id, session_id, message_id, file_path, content, \
                               operation, created_at FROM file_snapshots";

/// 整行映射（`content` 列为 BLOB，UTF-8 解码失败时回落有损转换——
/// 快照仅用于展示与回滚预览，不因个别坏字节整条失败）。
fn map_snapshot_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileSnapshotRecord> {
    let content: Option<Vec<u8>> = row.get("content")?;
    Ok(FileSnapshotRecord {
        id: row.get("id")?,
        session_id: row.get("session_id")?,
        message_id: row.get("message_id")?,
        file_path: row.get("file_path")?,
        content: content.map_or_else(String::new, |bytes| {
            String::from_utf8_lossy(&bytes).into_owned()
        }),
        operation: row.get("operation")?,
        created_at: row.get("created_at")?,
    })
}

impl crate::Db {
    /// 落一条写前文件快照（对齐旧 `FileSnapshotRepository.save`）。
    ///
    /// 返回新生成的主键。`created_at` 由本层取当前时刻（旧系统同样在
    /// `trackEdit` 内 `Instant.now()`，调用方不传时间）。
    ///
    /// # Errors
    ///
    /// 会话不存在（外键违例）时返回 [`DbError::SessionNotFound`]；
    /// 其余底层写入失败返回 [`DbError::Sqlite`]。
    pub async fn insert_file_snapshot(
        &self,
        session_id: &str,
        message_id: Option<&str>,
        file_path: &str,
        content: &str,
        operation: &str,
    ) -> Result<String, DbError> {
        let session_id = session_id.to_owned();
        let message_id = message_id.map(str::to_owned);
        let file_path = file_path.to_owned();
        let content = content.to_owned();
        let operation = operation.to_owned();
        self.with_writer(move |conn| {
            let id = uuid::Uuid::new_v4().to_string();
            let created_at = format_rfc3339_micros(now_millis());
            conn.execute(
                "INSERT INTO file_snapshots
                     (id, session_id, message_id, file_path, content, operation, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    id,
                    session_id,
                    message_id,
                    file_path,
                    content.as_bytes(),
                    operation,
                    created_at
                ],
            )
            .map_err(|err| map_fk_violation(&session_id, err))?;
            Ok(id)
        })
        .await
    }

    /// 列出某会话的全部快照（`created_at` 升序；对齐旧 `findBySessionId`）。
    ///
    /// 次序键补强（留痕）：旧实现仅 `ORDER BY created_at`，同毫秒内多条快照
    /// 次序不定；本层追加隐式 `rowid` 作次级键，保证同一时刻的多条按插入
    /// 顺序稳定返回（撤销/回滚依赖该次序）。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回 [`DbError::Sqlite`]。
    pub async fn list_file_snapshots(
        &self,
        session_id: &str,
    ) -> Result<Vec<FileSnapshotRecord>, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| {
            let sql = format!("{SNAPSHOT_SELECT} WHERE session_id = ?1 ORDER BY created_at, rowid");
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![session_id], map_snapshot_row)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await
    }

    /// 列出某会话某消息回合的全部快照（`created_at` 升序；对齐旧
    /// `findByMessageId`）。
    ///
    /// 供 Batch 5 的历史回退（`rewind`）与回合 diff 统计消费：一个
    /// `message_id` 对应一次助手回合内产生的全部写前快照。
    ///
    /// 会话维度收紧（留痕）：旧实现是 `WHERE message_id = ?` 单条件。
    /// `message_id` 由调用方从 URL 路径传入，跨会话撞同一 id 时旧实现会读到
    /// **别的会话**的快照内容并把它写回当前工作区。本层追加 `session_id`
    /// 谓词（端点本就带 `sessionId` 路径段，无需新增入参来源），杜绝该越界
    /// 面；正常调用下结果集与旧实现一致。
    ///
    /// 次序键补强同 [`Self::list_file_snapshots`]：追加隐式 `rowid` 作次级键，
    /// 保证同一时刻的多条按插入顺序稳定返回。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回 [`DbError::Sqlite`]。
    pub async fn list_by_message_id(
        &self,
        session_id: &str,
        message_id: &str,
    ) -> Result<Vec<FileSnapshotRecord>, DbError> {
        let session_id = session_id.to_owned();
        let message_id = message_id.to_owned();
        self.with_reader(move |conn| {
            let sql = format!(
                "{SNAPSHOT_SELECT} WHERE session_id = ?1 AND message_id = ?2 \
                 ORDER BY created_at, rowid"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![session_id, message_id], map_snapshot_row)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await
    }

    /// 取某会话某路径的最新快照（对齐旧 `findLatestBySessionAndPath`）。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回 [`DbError::Sqlite`]。
    pub async fn latest_file_snapshot(
        &self,
        session_id: &str,
        file_path: &str,
    ) -> Result<Option<FileSnapshotRecord>, DbError> {
        let session_id = session_id.to_owned();
        let file_path = file_path.to_owned();
        self.with_reader(move |conn| {
            let sql = format!(
                "{SNAPSHOT_SELECT} WHERE session_id = ?1 AND file_path = ?2 \
                 ORDER BY created_at DESC, rowid DESC LIMIT 1"
            );
            Ok(conn
                .query_row(&sql, params![session_id, file_path], map_snapshot_row)
                .optional()?)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::DbError;

    /// 建会话 → 落两条快照 → 升序列出 + 最新查询命中后写的那条。
    #[tokio::test]
    async fn insert_then_list_and_latest() {
        let db = crate::Db::open_in_memory().expect("boot");
        let session = db
            .create_session("gpt-4o", "/tmp/zk-snap")
            .await
            .expect("session");
        let first = db
            .insert_file_snapshot(&session.id, None, "/tmp/zk-snap/a.txt", "v1", "write")
            .await
            .expect("first");
        assert!(uuid::Uuid::parse_str(&first).is_ok(), "id must be uuid");
        db.insert_file_snapshot(
            &session.id,
            Some("msg-1"),
            "/tmp/zk-snap/a.txt",
            "v2",
            "edit",
        )
        .await
        .expect("second");

        let all = db.list_file_snapshots(&session.id).await.expect("list");
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].content, "v1");
        assert_eq!(all[0].operation, "write");
        assert_eq!(all[0].message_id, None);
        assert_eq!(all[0].created_at.len(), 27, "iso: {}", all[0].created_at);

        let latest = db
            .latest_file_snapshot(&session.id, "/tmp/zk-snap/a.txt")
            .await
            .expect("latest")
            .expect("some");
        assert_eq!(latest.content, "v2");
        assert_eq!(latest.message_id.as_deref(), Some("msg-1"));
    }

    /// 会话不存在 → 外键违例归一为 `SessionNotFound`（不是裸 `Sqlite`）。
    #[tokio::test]
    async fn insert_for_missing_session_is_not_found() {
        let db = crate::Db::open_in_memory().expect("boot");
        let err = db
            .insert_file_snapshot("ghost", None, "/tmp/x.txt", "body", "write")
            .await
            .expect_err("fk violation");
        assert!(
            matches!(err, DbError::SessionNotFound(ref id) if id == "ghost"),
            "unexpected: {err:?}"
        );
    }

    /// 空会话无快照；未知路径的最新查询返回 `None`（非错误）。
    #[tokio::test]
    async fn empty_queries_are_none_not_error() {
        let db = crate::Db::open_in_memory().expect("boot");
        let session = db
            .create_session("gpt-4o", "/tmp/zk-snap-empty")
            .await
            .expect("session");
        assert!(
            db.list_file_snapshots(&session.id)
                .await
                .expect("list")
                .is_empty()
        );
        assert_eq!(
            db.latest_file_snapshot(&session.id, "/tmp/none.txt")
                .await
                .expect("latest"),
            None
        );
    }

    /// 按 `message_id` 列出：只回本会话本回合，跨会话同名 id 不串。
    #[tokio::test]
    async fn list_by_message_id_scopes_to_session_and_turn() {
        let db = crate::Db::open_in_memory().expect("boot");
        let first = db
            .create_session("gpt-4o", "/tmp/zk-snap-msg-a")
            .await
            .expect("session a");
        let second = db
            .create_session("gpt-4o", "/tmp/zk-snap-msg-b")
            .await
            .expect("session b");
        db.insert_file_snapshot(&first.id, Some("turn-1"), "/tmp/a.txt", "a1", "edit")
            .await
            .expect("a1");
        db.insert_file_snapshot(&first.id, Some("turn-1"), "/tmp/b.txt", "b1", "edit")
            .await
            .expect("b1");
        db.insert_file_snapshot(&first.id, Some("turn-2"), "/tmp/a.txt", "a2", "edit")
            .await
            .expect("a2");
        // 同名 message_id 落在另一个会话：不得被读出。
        db.insert_file_snapshot(&second.id, Some("turn-1"), "/tmp/x.txt", "x1", "edit")
            .await
            .expect("x1");

        let turn = db
            .list_by_message_id(&first.id, "turn-1")
            .await
            .expect("list");
        assert_eq!(turn.len(), 2);
        assert_eq!(turn[0].file_path, "/tmp/a.txt");
        assert_eq!(turn[0].content, "a1");
        assert_eq!(turn[1].file_path, "/tmp/b.txt");
        assert!(
            db.list_by_message_id(&first.id, "turn-absent")
                .await
                .expect("list")
                .is_empty()
        );
    }

    /// 会话删除级联清空其快照（DDL `ON DELETE CASCADE` 生效验证）。
    #[tokio::test]
    async fn session_delete_cascades_snapshots() {
        let db = crate::Db::open_in_memory().expect("boot");
        let session = db
            .create_session("gpt-4o", "/tmp/zk-snap-cascade")
            .await
            .expect("session");
        db.insert_file_snapshot(&session.id, None, "/tmp/a.txt", "body", "write")
            .await
            .expect("snap");
        assert!(db.delete_session(&session.id).await.expect("delete"));
        assert!(
            db.list_file_snapshots(&session.id)
                .await
                .expect("list")
                .is_empty()
        );
    }
}
