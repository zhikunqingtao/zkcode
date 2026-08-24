//! zk-db 活动域——`activities` 表的只读分页查询（2.6，旧
//! `service/ActivityRepository.java`）。
//!
//! | 本模块 | 旧源 |
//! |---|---|
//! | [`Db::count_activities_by_session`] | `ActivityRepository.countBySessionId` |
//! | [`Db::find_activities_by_session_paged`] | `ActivityRepository.findBySessionIdPaged` |
//!
//! # 语义对照
//!
//! 旧 `findBySessionIdPaged` 走 `jdbcTemplate.queryForList` → `List<Map<String,
//! Object>>`，`ActivityController` 直接把该 Map 列表塞进响应，键为原始
//! `snake_case` 列名（Jackson 不做 `camelCase` 转换），JSON 文本列
//! （`tool_result_json` 等）作转义字符串输出，null 列被 `NON_NULL` 剥离。
//! 本模块复刻此形状：逐行按列名装配 `serde_json::Map`，null 值不写入键。
//!
//! # 偏离留痕
//!
//! - **A-01**：旧 `queryForList` 的 `LinkedCaseInsensitiveMap` 保留列序（插入
//!   序），而 `serde_json::Map` 默认 `BTreeMap`（未启用 `preserve_order`），
//!   故键序为字典序。消费方（前端「加载更多」）按键取值，不依赖键序（承接
//!   `run.rs` R-03 约定）。

use base64::Engine as _;
use rusqlite::types::ValueRef;
use rusqlite::{Row, params};
use serde_json::{Map, Value};

use crate::Db;
use crate::error::DbError;

/// Blob 列的编码引擎（防御性：`activities` 表无 BLOB 列，仅为覆盖
/// `queryForList` 的全类型分支——与 `cursor.rs` 同款 STANDARD）。
const BLOB_ENGINE: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// `SQLite` 动态列值 → JSON（旧 `queryForList` 的列值装箱）：`NULL` 返回
/// `None`（由调用方剥离键，对齐 `NON_NULL`）。
fn value_ref_to_json(value: ValueRef<'_>) -> Option<Value> {
    match value {
        ValueRef::Null => None,
        ValueRef::Integer(int) => Some(Value::from(int)),
        ValueRef::Real(real) => Some(Value::from(real)),
        ValueRef::Text(bytes) => Some(Value::from(String::from_utf8_lossy(bytes).into_owned())),
        ValueRef::Blob(bytes) => Some(Value::from(BLOB_ENGINE.encode(bytes))),
    }
}

/// 行 → JSON 对象（键 = `snake_case` 列名，null 列剥离）。
fn row_to_json_object(row: &Row<'_>, columns: &[String]) -> rusqlite::Result<Value> {
    let mut object = Map::with_capacity(columns.len());
    for (index, name) in columns.iter().enumerate() {
        if let Some(json) = value_ref_to_json(row.get_ref(index)?) {
            object.insert(name.clone(), json);
        }
    }
    Ok(Value::Object(object))
}

impl Db {
    /// 旧 `ActivityRepository.countBySessionId`（`SELECT COUNT(*) FROM
    /// activities WHERE session_id = ?`）：会话活动总数。只读路径。
    ///
    /// # Errors
    /// 底层查询失败时返回 [`DbError`]。
    pub async fn count_activities_by_session(&self, session_id: &str) -> Result<i64, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM activities WHERE session_id = ?1",
                params![session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(Into::into)
        })
        .await
    }

    /// 旧 `ActivityRepository.findBySessionIdPaged`（`SELECT * FROM activities
    /// WHERE session_id = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`）：
    /// 时间倒序分页。返回逐行 JSON 对象（旧 `queryForList` 形状）。只读路径。
    ///
    /// 参数绑定序与旧源一致：`session_id` / `limit` / `offset`（`LIMIT` 在
    /// `OFFSET` 之前）。
    ///
    /// # Errors
    /// 底层查询失败时返回 [`DbError`]。
    pub async fn find_activities_by_session_paged(
        &self,
        session_id: &str,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Value>, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT * FROM activities WHERE session_id = ?1 \
                 ORDER BY timestamp DESC LIMIT ?2 OFFSET ?3",
            )?;
            let columns: Vec<String> = stmt.column_names().into_iter().map(str::to_owned).collect();
            let rows = stmt
                .query_map(params![session_id, limit, offset], |row| {
                    row_to_json_object(row, &columns)
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::Db;

    /// 直连布数：建一条会话 + 若干活动行。
    async fn seeded() -> (Db, String) {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("claude-sonnet-4-5", "/tmp/zkcode-act-tests")
            .await
            .expect("session");
        let session_id = session.id;
        db.with_conn_blocking(|conn| {
            for idx in 0..3_i64 {
                conn.execute(
                    "INSERT INTO activities (id, session_id, operation_type, summary, \
                     status, timestamp, file_count, tool_result_json, created_at, updated_at) \
                     VALUES (?1, ?2, 'edit', 'did a thing', 'completed', ?3, 2, \
                     '{\"ok\":true}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
                    rusqlite::params![format!("act-{idx}"), session_id, idx * 1000],
                )?;
            }
            Ok(())
        })
        .expect("seed");
        (db, session_id)
    }

    #[tokio::test]
    async fn count_and_paged_shape_match_legacy_queryforlist() {
        let (db, session_id) = seeded().await;

        assert_eq!(
            db.count_activities_by_session(&session_id)
                .await
                .expect("count"),
            3
        );
        assert_eq!(
            db.count_activities_by_session("missing")
                .await
                .expect("count"),
            0
        );

        // timestamp DESC：act-2（ts=2000）在最前。
        let page = db
            .find_activities_by_session_paged(&session_id, 0, 2)
            .await
            .expect("paged");
        assert_eq!(page.len(), 2);
        let first = page[0].as_object().expect("object");
        assert_eq!(first["id"], "act-2");
        assert_eq!(first["session_id"], session_id.as_str());
        assert_eq!(first["operation_type"], "edit");
        assert_eq!(first["file_count"], 2);
        // JSON 列作转义字符串透传（非嵌套对象）。
        assert_eq!(first["tool_result_json"], "{\"ok\":true}");
        // NULL 列（duration / decision / changed_files_json / insight_json）被剥离。
        assert!(!first.contains_key("duration"));
        assert!(!first.contains_key("decision"));
        assert!(!first.contains_key("changed_files_json"));

        // OFFSET 生效：跳过 2 条只剩 1 条（act-0）。
        let tail = db
            .find_activities_by_session_paged(&session_id, 2, 2)
            .await
            .expect("paged tail");
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].as_object().expect("object")["id"], "act-0");
    }
}
