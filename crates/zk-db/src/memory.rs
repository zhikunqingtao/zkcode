//! 长期记忆域仓储——[`Db`](crate::Db) 的 `memories` 表 CRUD（Batch 5）。
//!
//! 语义来源（旧仓库只读）：`MemoryController` 的五个端点内联 SQL——旧系统
//! 没有 `MemoryRepository`，SQL 直写在 controller 里（`globalJdbcTemplate`）。
//! 本层把这批 SQL 下沉为唯一权威，zk-server 侧只做 HTTP 形状装配。
//!
//! # 归属库（D6 单库）
//!
//! 旧系统 `memories` 属**用户级** `global.db`（`@Qualifier("globalJdbcTemplate")`），
//! 与项目级 `data.db` 分离。zkcode 依 D6 裁定把 global.db 三表（`memories` /
//! `projects` / `auth_tokens`）并入单一 `.zk/data.db`，故本模块与会话域共用
//! 同一 [`Db`](crate::Db) 句柄；表 DDL 已在绿地基线
//! `migrations/V2__init_session_message.sql` 内联（含旧 `V002_AddMemorySource`
//! 补的 `source` 列），本批不新增迁移。
//!
//! # 逐字对照要点
//!
//! - 读投影恒为九列 `id, category, title, content, keywords, scope, source,
//!   created_at, updated_at`——`project_path` 列存在但旧端点从不读写（按范围
//!   过滤属 P3，本批不做），故新建记录该列留 `NULL`。
//! - `ORDER BY updated_at DESC`（`GET /api/memory` 与 `GET /api/memory/all`
//!   的 sqlite 分支在旧实现里是**逐字相同**的 SQL，见 [`Db::list_memories`]）。
//! - 写入时刻由本层取当前时刻（旧实现同样在 controller 内 `Instant.now()`，
//!   请求体里的 `createdAt` / `updatedAt` 被忽略）。
//! - 缺省值不对称（旧实现如此，非笔误）：INSERT 对 `scope` 兜 `"global"`、
//!   对 `source` 兜 `"USER"`；UPDATE 只对 `source` 兜 `"USER"`，`scope`
//!   原样写入——即 PUT 可把已有记录的 `scope` 置空。见 [`Db::update_memory`]。
//! - `category` / `title` / `content` 在 DDL 为 `NOT NULL`：请求体缺这三项时
//!   落库报约束违例（[`DbError::Sqlite`]），对齐旧实现的
//!   `DataIntegrityViolationException` → 500，不在本层提前拦。

use rusqlite::params;

use crate::error::DbError;
use crate::time::{format_rfc3339_micros, now_millis};

/// 记忆条目读投影（对齐旧 `MemoryController.MemoryEntry` 九元组）。
///
/// 非空性来自 DDL：`id` / `category` / `title` / `content` / `created_at` /
/// `updated_at` 为 `NOT NULL`，故取 `String`；`keywords` / `scope` / `source`
/// 可为 `NULL`，取 `Option<String>` 并序列化为 JSON `null`（对齐 Jackson
/// 默认含空字段的行为）。
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryRecord {
    /// 主键（调用方未给时由本层生成 `UUIDv4`）。
    pub id: String,
    /// 分类（旧系统自由文本，前端约定 `USER_PREFERENCE` / `PROJECT_CONVENTION` 等）。
    pub category: String,
    /// 标题。
    pub title: String,
    /// 正文。
    pub content: String,
    /// 检索关键词（旧系统为逗号分隔自由文本）。
    pub keywords: Option<String>,
    /// 作用域（`"global"` / 项目级；缺省 `"global"`）。
    pub scope: Option<String>,
    /// 来源（`"USER"` / `"AUTO"` / `"TOOL"`；缺省 `"USER"`）。
    pub source: Option<String>,
    /// 创建时刻（RFC 3339，恒 6 位微秒）。
    pub created_at: String,
    /// 最后更新时刻（RFC 3339，恒 6 位微秒；排序键）。
    pub updated_at: String,
}

/// 记忆条目写入形状（对齐旧 DTO 的「全字段可空」输入域）。
///
/// `id` 缺省时由本层生成；`created_at` / `updated_at` **不在本结构里**
/// ——旧实现忽略请求体里的时间字段、一律取服务端当前时刻。REST 层可直接
/// 用本结构反序列化请求体（`#[serde(default)]` 使缺字段落 `None`，多余的
/// `createdAt` / `updatedAt` 键被忽略，与 Jackson 宽容解析一致）。
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct MemoryUpsert {
    /// 主键；`None` 时生成 `UUIDv4`。
    pub id: Option<String>,
    /// 分类（DDL `NOT NULL`，缺失将落约束违例）。
    pub category: Option<String>,
    /// 标题（DDL `NOT NULL`）。
    pub title: Option<String>,
    /// 正文（DDL `NOT NULL`）。
    pub content: Option<String>,
    /// 检索关键词。
    pub keywords: Option<String>,
    /// 作用域；INSERT 缺省 `"global"`，UPDATE 原样写入（含 `NULL`）。
    pub scope: Option<String>,
    /// 来源；INSERT / UPDATE 均缺省 `"USER"`。
    pub source: Option<String>,
}

/// 读投影列清单（与 [`map_memory_row`] 互锁）。
const MEMORY_SELECT: &str = "SELECT id, category, title, content, keywords, scope, source, \
                             created_at, updated_at FROM memories";

/// 九列 INSERT（`create_memory` 与 `update_memory` 的兜底插入共用同一语句，
/// 保证两条路径写出的行形状严格一致）。
const MEMORY_INSERT: &str = "INSERT INTO memories \
                             (id, category, title, content, keywords, scope, source, \
                             created_at, updated_at) \
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)";

/// 整行映射。
fn map_memory_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MemoryRecord> {
    Ok(MemoryRecord {
        id: row.get("id")?,
        category: row.get("category")?,
        title: row.get("title")?,
        content: row.get("content")?,
        keywords: row.get("keywords")?,
        scope: row.get("scope")?,
        source: row.get("source")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
    })
}

/// 旧实现的 `scope` 兜底值（仅 INSERT 路径生效）。
const DEFAULT_SCOPE: &str = "global";
/// 旧实现的 `source` 兜底值（INSERT 与 UPDATE 路径均生效）。
const DEFAULT_SOURCE: &str = "USER";

impl crate::Db {
    /// 新建一条记忆（对齐旧 `POST /api/memory`）。
    ///
    /// 返回落库使用的主键（调用方未给 `id` 时为新生成的 `UUIDv4`），供
    /// 端点回 `{"success":true,"id":…}`。
    ///
    /// `created_at` 与 `updated_at` 同取当前时刻；`project_path` 留 `NULL`。
    ///
    /// # Errors
    ///
    /// 主键重复或 `NOT NULL` 约束违例（缺 `category` / `title` / `content`）时
    /// 返回 [`DbError::Sqlite`]——对齐旧实现「不预校验、由数据库拒绝」的行为。
    pub async fn create_memory(&self, entry: MemoryUpsert) -> Result<String, DbError> {
        self.with_writer(move |conn| {
            let id = entry.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let now = format_rfc3339_micros(now_millis());
            conn.execute(
                MEMORY_INSERT,
                params![
                    id,
                    entry.category,
                    entry.title,
                    entry.content,
                    entry.keywords,
                    entry.scope.unwrap_or_else(|| DEFAULT_SCOPE.to_owned()),
                    entry.source.unwrap_or_else(|| DEFAULT_SOURCE.to_owned()),
                    now,
                    now
                ],
            )?;
            Ok(id)
        })
        .await
    }

    /// 列出全部记忆（`updated_at` 降序；对齐旧 `GET /api/memory`）。
    ///
    /// 旧 `GET /api/memory/all` 的 `sqlite` 分支是**逐字相同**的 SQL
    /// （`MemoryController` L41 与 L107 完全一致），故不另立
    /// `list_all_memories`——同一查询保持单一权威，`/all` 端点复用本方法并
    /// 自行拼接 `memoryMd` 分支。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回 [`DbError::Sqlite`]。
    pub async fn list_memories(&self) -> Result<Vec<MemoryRecord>, DbError> {
        self.with_reader(move |conn| {
            let sql = format!("{MEMORY_SELECT} ORDER BY updated_at DESC");
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], map_memory_row)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok(out)
        })
        .await
    }

    /// 更新（不存在则插入）一条记忆（对齐旧 `PUT /api/memory` 的单条循环体）。
    ///
    /// 返回 `(id, inserted)`：`inserted` 为 true 表示走了兜底插入分支。
    ///
    /// 逐字对照的两处不对称（旧实现如此）：
    /// - UPDATE 不写 `created_at`（仅插入分支设置），故已有记录的创建时刻保持；
    /// - UPDATE 的 `scope` **不兜底**，`None` 会把该列置 `NULL`；`source` 兜
    ///   `"USER"`。插入分支两者都兜底。
    ///
    /// 批量语义留在端点层逐条调用（旧实现在 controller 内 for 循环、每条各自
    /// 自动提交；某条失败时前面的已生效——本方法保持该部分生效语义，不做
    /// 批事务包裹）。
    ///
    /// # Errors
    ///
    /// 底层写入失败（含插入分支的 `NOT NULL` 违例）时返回 [`DbError::Sqlite`]。
    pub async fn update_memory(&self, entry: MemoryUpsert) -> Result<(String, bool), DbError> {
        self.with_writer(move |conn| {
            let id = entry.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let now = format_rfc3339_micros(now_millis());
            let source = entry.source.unwrap_or_else(|| DEFAULT_SOURCE.to_owned());
            let updated = conn.execute(
                "UPDATE memories SET category = ?1, title = ?2, content = ?3, keywords = ?4, \
                 scope = ?5, source = ?6, updated_at = ?7 WHERE id = ?8",
                params![
                    entry.category,
                    entry.title,
                    entry.content,
                    entry.keywords,
                    entry.scope,
                    source,
                    now,
                    id
                ],
            )?;
            if updated > 0 {
                return Ok((id, false));
            }
            conn.execute(
                MEMORY_INSERT,
                params![
                    id,
                    entry.category,
                    entry.title,
                    entry.content,
                    entry.keywords,
                    entry.scope.unwrap_or_else(|| DEFAULT_SCOPE.to_owned()),
                    source,
                    now,
                    now
                ],
            )?;
            Ok((id, true))
        })
        .await
    }

    /// 删除一条记忆（对齐旧 `DELETE /api/memory/{memoryId}`）。
    ///
    /// 返回是否命中（旧实现 `deleted > 0` → 204，否则 404）。
    ///
    /// # Errors
    ///
    /// 底层写入失败时返回 [`DbError::Sqlite`]。
    pub async fn delete_memory(&self, id: &str) -> Result<bool, DbError> {
        let id = id.to_owned();
        self.with_writer(move |conn| {
            let deleted = conn.execute("DELETE FROM memories WHERE id = ?1", params![id])?;
            Ok(deleted > 0)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_SCOPE, DEFAULT_SOURCE, MemoryUpsert};

    /// 造一条最小合法写入形状（三个 `NOT NULL` 列齐备）。
    fn upsert(title: &str) -> MemoryUpsert {
        MemoryUpsert {
            category: Some("USER_PREFERENCE".to_owned()),
            title: Some(title.to_owned()),
            content: Some(format!("body of {title}")),
            ..MemoryUpsert::default()
        }
    }

    /// 新建 → 列出：主键为 UUID、缺省 scope/source 兜底、时间戳恒 27 字符。
    #[tokio::test]
    async fn create_applies_defaults_and_generates_id() {
        let db = crate::Db::open_in_memory().expect("boot");
        let id = db
            .create_memory(upsert("prefer rust"))
            .await
            .expect("create");
        assert!(uuid::Uuid::parse_str(&id).is_ok(), "id must be uuid: {id}");

        let all = db.list_memories().await.expect("list");
        assert_eq!(all.len(), 1);
        let entry = &all[0];
        assert_eq!(entry.id, id);
        assert_eq!(entry.category, "USER_PREFERENCE");
        assert_eq!(entry.title, "prefer rust");
        assert_eq!(entry.content, "body of prefer rust");
        assert_eq!(entry.keywords, None);
        assert_eq!(entry.scope.as_deref(), Some(DEFAULT_SCOPE));
        assert_eq!(entry.source.as_deref(), Some(DEFAULT_SOURCE));
        assert_eq!(entry.created_at.len(), 27, "iso: {}", entry.created_at);
        assert_eq!(entry.created_at, entry.updated_at);
    }

    /// 调用方自带 `id` 时原样落库（前端可指定主键，旧实现同）。
    #[tokio::test]
    async fn create_honours_caller_supplied_id() {
        let db = crate::Db::open_in_memory().expect("boot");
        let id = db
            .create_memory(MemoryUpsert {
                id: Some("mem-fixed".to_owned()),
                scope: Some("project".to_owned()),
                source: Some("AUTO".to_owned()),
                keywords: Some("rust,style".to_owned()),
                ..upsert("fixed")
            })
            .await
            .expect("create");
        assert_eq!(id, "mem-fixed");
        let entry = db.list_memories().await.expect("list").remove(0);
        assert_eq!(entry.scope.as_deref(), Some("project"));
        assert_eq!(entry.source.as_deref(), Some("AUTO"));
        assert_eq!(entry.keywords.as_deref(), Some("rust,style"));
    }

    /// 更新命中：`created_at` 不变、`updated_at` 前移；`scope` 不兜底可置空。
    #[tokio::test]
    async fn update_hits_existing_and_leaves_created_at() {
        let db = crate::Db::open_in_memory().expect("boot");
        db.create_memory(MemoryUpsert {
            id: Some("mem-1".to_owned()),
            ..upsert("v1")
        })
        .await
        .expect("create");
        // 确定性布数：把创建时刻推早，避免同毫秒内 updated_at 相等。
        db.with_conn_blocking(|conn| {
            conn.execute(
                "UPDATE memories SET created_at = ?1, updated_at = ?1 WHERE id = 'mem-1'",
                ["2020-01-01T00:00:00.000000Z"],
            )?;
            Ok(())
        })
        .expect("backdate");

        let (id, inserted) = db
            .update_memory(MemoryUpsert {
                id: Some("mem-1".to_owned()),
                category: Some("PROJECT_CONVENTION".to_owned()),
                title: Some("v2".to_owned()),
                content: Some("body v2".to_owned()),
                ..MemoryUpsert::default()
            })
            .await
            .expect("update");
        assert_eq!(id, "mem-1");
        assert!(!inserted, "existing row must take the UPDATE branch");

        let entry = db.list_memories().await.expect("list").remove(0);
        assert_eq!(entry.title, "v2");
        assert_eq!(entry.category, "PROJECT_CONVENTION");
        assert_eq!(entry.created_at, "2020-01-01T00:00:00.000000Z");
        assert!(entry.updated_at > entry.created_at);
        // 旧实现 UPDATE 的 scope 不兜底 → None 写成 NULL；source 仍兜 USER。
        assert_eq!(entry.scope, None);
        assert_eq!(entry.source.as_deref(), Some(DEFAULT_SOURCE));
    }

    /// 更新未命中 → 走兜底插入，此时 `scope` 兜 `"global"`。
    #[tokio::test]
    async fn update_falls_back_to_insert() {
        let db = crate::Db::open_in_memory().expect("boot");
        let (id, inserted) = db
            .update_memory(MemoryUpsert {
                id: Some("mem-ghost".to_owned()),
                ..upsert("brand new")
            })
            .await
            .expect("update");
        assert_eq!(id, "mem-ghost");
        assert!(inserted, "missing row must take the INSERT branch");
        let entry = db.list_memories().await.expect("list").remove(0);
        assert_eq!(entry.title, "brand new");
        assert_eq!(entry.scope.as_deref(), Some(DEFAULT_SCOPE));
        assert_eq!(entry.created_at, entry.updated_at);
    }

    /// 列表按 `updated_at` 降序（最近更新在前）。
    #[tokio::test]
    async fn list_orders_by_updated_at_desc() {
        let db = crate::Db::open_in_memory().expect("boot");
        for (id, stamp) in [
            ("old", "2020-01-01T00:00:00.000000Z"),
            ("mid", "2021-01-01T00:00:00.000000Z"),
            ("new", "2022-01-01T00:00:00.000000Z"),
        ] {
            db.create_memory(MemoryUpsert {
                id: Some(id.to_owned()),
                ..upsert(id)
            })
            .await
            .expect("create");
            let owned = id.to_owned();
            db.with_conn_blocking(move |conn| {
                conn.execute(
                    "UPDATE memories SET updated_at = ?1 WHERE id = ?2",
                    rusqlite::params![stamp, owned],
                )?;
                Ok(())
            })
            .expect("stamp");
        }
        let ids: Vec<String> = db
            .list_memories()
            .await
            .expect("list")
            .into_iter()
            .map(|entry| entry.id)
            .collect();
        assert_eq!(ids, vec!["new", "mid", "old"]);
    }

    /// 删除命中返回 true，重复删除返回 false（端点据此分 204 / 404）。
    #[tokio::test]
    async fn delete_reports_hit_then_miss() {
        let db = crate::Db::open_in_memory().expect("boot");
        db.create_memory(MemoryUpsert {
            id: Some("mem-del".to_owned()),
            ..upsert("doomed")
        })
        .await
        .expect("create");
        assert!(db.delete_memory("mem-del").await.expect("delete"));
        assert!(!db.delete_memory("mem-del").await.expect("delete again"));
        assert!(db.list_memories().await.expect("list").is_empty());
    }

    /// 缺 `NOT NULL` 列 → 落约束违例（旧实现同为 500，不在本层预校验）。
    #[tokio::test]
    async fn missing_not_null_columns_are_rejected_by_sqlite() {
        let db = crate::Db::open_in_memory().expect("boot");
        let err = db
            .create_memory(MemoryUpsert::default())
            .await
            .expect_err("not-null violation");
        assert!(
            matches!(err, crate::DbError::Sqlite(_)),
            "unexpected: {err:?}"
        );
    }
}
