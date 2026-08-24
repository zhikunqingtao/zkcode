//! 项目域仓储——[`Db`](crate::Db) 的 projects 表 CRUD 与 `project_config`
//! KV 表读写（多文件 impl 之一，Phase 2 子阶段 2.1）。
//!
//! 语义来源（旧仓库只读，2026-08-16 冻结）：
//! - `ProjectRepository.insert/findAll/findById/deleteById`（global.db 的
//!   projects 表，D6 并入单库；`workspace_root` UNIQUE 冲突由本层显式建模
//!   为 [`DbError::WorkspaceRootTaken`]，供 zk-server 映射 409）；
//! - `ConfigService.loadProjectConfigJson/saveProjectConfigJson`
//!   （`project_config` KV 表，与跨项目 `config` 表同构但相互隔离）。
//!
//! 路径合法性校验（绝对路径 / 存在性 / allowed roots）**不在本层**：旧系统
//! 同样由 `ProjectWorkspaceService` 在写库前完成，本层只存取已规范化的值。

use rusqlite::{OptionalExtension, params};

use crate::error::DbError;
use crate::time::{format_rfc3339_micros, now_millis};

/// 项目记录（projects 表整行；对齐旧 `Project.java` 的 Jackson 线上形状：
/// `{id,name,workspaceRoot,createdAt}`，`createdAt` 为 ISO 字符串原文）。
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectRecord {
    /// 主键（UUIDv4，插入时生成）。
    pub id: String,
    /// 项目显示名（1..=80 字符，由服务层校验后传入）。
    pub name: String,
    /// 工作区根目录绝对路径（UNIQUE；已由服务层 canonicalize）。
    pub workspace_root: String,
    /// 创建时刻（RFC 3339，恒 6 位微秒，见 `time` 模块）。
    pub created_at: String,
}

/// 将 `workspace_root` UNIQUE 违例归一为 [`DbError::WorkspaceRootTaken`]。
///
/// projects 表唯一的 UNIQUE 约束即 `workspace_root`；`SQLite` 唯一约束违例
/// （扩展码 2067，`SQLITE_CONSTRAINT_UNIQUE`）在此场景下即「工作区已被占用」，
/// 显式映射避免调用方 sniff 底层错误码（与 `error::map_fk_violation` 同套路）。
fn map_unique_violation(workspace_root: &str, err: rusqlite::Error) -> DbError {
    if matches!(
        err,
        rusqlite::Error::SqliteFailure(ref e, _) if e.extended_code == 2067
    ) {
        DbError::WorkspaceRootTaken(workspace_root.to_owned())
    } else {
        DbError::Sqlite(err)
    }
}

/// 整行映射（列序与 `PROJECT_SELECT` 一致）。
fn map_project_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProjectRecord> {
    Ok(ProjectRecord {
        id: row.get("id")?,
        name: row.get("name")?,
        workspace_root: row.get("workspace_root")?,
        created_at: row.get("created_at")?,
    })
}

/// 项目行 SELECT（列清单与 `map_project_row` 互锁）。
const PROJECT_SELECT: &str = "SELECT id, name, workspace_root, created_at FROM projects";

/// [`crate::Db::find_project_by_workspace_root`] 的当前写事务变体（沿
/// [`crate::run`] 系的 `_in_current_write` 命名）：复用调用方已持有的连接，
/// 供授权终局复检在 writer 锁内于**同一事务**重验工作区信任——对齐旧
/// `ProjectWorkspaceService#isTrustedFileScope` 在同线程 JDBC 事务上下文里
/// 复用连接的语义。
///
/// writer `Mutex` 不可重入：`with_writer` 闭包内绝不可改走
/// [`crate::Db::find_project_by_workspace_root_blocking`]（再锁同一 Mutex 即
/// 永久死锁），必须使用本函数。
///
/// # Errors
///
/// 底层 `SQLite` 查询失败时返回。
pub fn find_project_by_workspace_root_in_current_write(
    conn: &rusqlite::Connection,
    workspace_root: &str,
) -> Result<Option<ProjectRecord>, DbError> {
    let sql = format!("{PROJECT_SELECT} WHERE workspace_root = ?1");
    Ok(conn
        .query_row(&sql, params![workspace_root], map_project_row)
        .optional()?)
}

impl crate::Db {
    /// 创建项目（`POST /api/projects` 数据源；对齐 `ProjectRepository.insert`）。
    ///
    /// 生成 `UUIDv4` 主键与恒 6 位微秒 ISO `created_at`，返回完整记录。
    ///
    /// # Errors
    ///
    /// - `workspace_root` 已被其他项目占用（UNIQUE 违例）时返回
    ///   [`DbError::WorkspaceRootTaken`]（zk-server 映射 409）；
    /// - 其余底层 `SQLite` 写入失败时返回 [`DbError::Sqlite`]。
    pub async fn create_project(
        &self,
        name: &str,
        workspace_root: &str,
    ) -> Result<ProjectRecord, DbError> {
        let name = name.to_owned();
        let workspace_root = workspace_root.to_owned();
        self.with_writer(move |conn| {
            let id = uuid::Uuid::new_v4().to_string();
            let created_at = format_rfc3339_micros(now_millis());
            conn.execute(
                "INSERT INTO projects (id, name, workspace_root, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![id, name, workspace_root, created_at],
            )
            .map_err(|err| map_unique_violation(&workspace_root, err))?;
            Ok(ProjectRecord {
                id,
                name,
                workspace_root,
                created_at,
            })
        })
        .await
    }

    /// 项目列表（`GET /api/projects` 数据源；对齐 `ProjectRepository.findAll`
    /// 的 `ORDER BY created_at DESC` —— 追加 `id DESC` 使同刻插入次序稳定，
    /// 与 `idx_projects_created_at` 索引形状一致）。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回。
    pub async fn list_projects(&self) -> Result<Vec<ProjectRecord>, DbError> {
        self.with_reader(move |conn| {
            let sql = format!("{PROJECT_SELECT} ORDER BY created_at DESC, id DESC");
            let mut stmt = conn.prepare(&sql)?;
            Ok(stmt
                .query_map([], map_project_row)?
                .collect::<rusqlite::Result<_>>()?)
        })
        .await
    }

    /// 按 id 查项目（session 绑定解析与撤销前判定共用；对齐
    /// `ProjectRepository.findById`）。不存在时返回 `Ok(None)`。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回。
    pub async fn get_project(&self, id: &str) -> Result<Option<ProjectRecord>, DbError> {
        let id = id.to_owned();
        self.with_reader(move |conn| {
            let sql = format!("{PROJECT_SELECT} WHERE id = ?1");
            Ok(conn
                .query_row(&sql, params![id], map_project_row)
                .optional()?)
        })
        .await
    }

    /// 按 `workspace_root` 精确查项目（对齐 `ProjectRepository.findByWorkspaceRoot`）。
    ///
    /// `projects.workspace_root` 上有 UNIQUE 约束，故至多一行。不存在时
    /// `Ok(None)`。2.5 权限管线的工作区信任判定以本查询为权威。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回。
    pub async fn find_project_by_workspace_root(
        &self,
        workspace_root: &str,
    ) -> Result<Option<ProjectRecord>, DbError> {
        let workspace_root = workspace_root.to_owned();
        self.with_reader(move |conn| {
            let sql = format!("{PROJECT_SELECT} WHERE workspace_root = ?1");
            Ok(conn
                .query_row(&sql, params![workspace_root], map_project_row)
                .optional()?)
        })
        .await
    }

    /// [`Self::find_project_by_workspace_root`] 的同步出口。
    ///
    /// 2.5 的 `WorkspaceTrustProbe`（旧 `ProjectWorkspaceService#isTrustedFileScope`）
    /// 位于授权判定链的**同步**末端：旧源刻意把工作区信任查询放在全部廉价判定
    /// 之后，把它提前到 `async` 边界会改变查询时序。故此处提供同步出口——
    /// 单行主键级 UNIQUE 索引查询，持锁时间为微秒级。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回。
    pub fn find_project_by_workspace_root_blocking(
        &self,
        workspace_root: &str,
    ) -> Result<Option<ProjectRecord>, DbError> {
        let sql = format!("{PROJECT_SELECT} WHERE workspace_root = ?1");
        self.with_conn_blocking(|conn| {
            Ok(conn
                .query_row(&sql, params![workspace_root], map_project_row)
                .optional()?)
        })
    }

    /// 撤销项目（`DELETE /api/projects/{id}` 数据源；对齐
    /// `ProjectRepository.deleteById`）。返回是否真实删除了一行——
    /// 不存在时 `Ok(false)`（幂等，旧系统据此回 `revoked:false`）。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 写入失败时返回。
    pub async fn delete_project(&self, id: &str) -> Result<bool, DbError> {
        let id = id.to_owned();
        self.with_writer(move |conn| {
            let affected = conn.execute("DELETE FROM projects WHERE id = ?1", params![id])?;
            Ok(affected > 0)
        })
        .await
    }

    /// 读取项目级配置值（`GET /api/config/project` 数据源；对齐旧
    /// `loadProjectConfigJson`）。无对应行时返回 `Ok(None)`（调用方以
    /// 默认 `ProjectConfig` 兜底）。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 查询失败时返回。
    pub async fn get_project_config_value(&self, key: &str) -> Result<Option<String>, DbError> {
        let key = key.to_owned();
        self.with_reader(move |conn| {
            Ok(conn
                .query_row(
                    "SELECT value FROM project_config WHERE key = ?1",
                    params![key],
                    |row| row.get::<_, String>(0),
                )
                .optional()?)
        })
        .await
    }

    /// 写入（upsert）项目级配置值（`PUT /api/config/project` 落库；对齐旧
    /// `saveProjectConfigJson`，`ON CONFLICT` 等价其 UPDATE + INSERT 双步）。
    ///
    /// # Errors
    ///
    /// 底层 `SQLite` 写入失败时返回。
    pub async fn put_project_config_value(&self, key: &str, value: &str) -> Result<(), DbError> {
        let key = key.to_owned();
        let value = value.to_owned();
        self.with_writer(move |conn| {
            let updated_at = format_rfc3339_micros(now_millis());
            conn.execute(
                "INSERT INTO project_config (key, value, updated_at) VALUES (?1, ?2, ?3)
                 ON CONFLICT(key) DO UPDATE SET value = ?2, updated_at = ?3",
                params![key, value, updated_at],
            )?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::DbError;
    use rusqlite::params;

    /// 创建返回 `UUIDv4` 主键与恒 6 位微秒 ISO 时间戳，字段原样回显。
    #[tokio::test]
    async fn create_returns_uuid_and_micros_iso() {
        let db = crate::Db::open_in_memory().expect("boot");
        let rec = db
            .create_project("Demo", "/tmp/zk-demo")
            .await
            .expect("create");
        assert!(uuid::Uuid::parse_str(&rec.id).is_ok(), "id must be uuid");
        assert_eq!(rec.name, "Demo");
        assert_eq!(rec.workspace_root, "/tmp/zk-demo");
        // 恒 6 位微秒：....SS.ffffffZ（长度 27，秒后第 19 字节为小数点）。
        assert_eq!(rec.created_at.len(), 27, "iso: {}", rec.created_at);
        assert_eq!(rec.created_at.as_bytes()[19], b'.');
        assert!(rec.created_at.ends_with('Z'));
    }

    /// `workspace_root` UNIQUE 冲突归一为 `WorkspaceRootTaken`（409 语义源）。
    #[tokio::test]
    async fn create_duplicate_workspace_root_is_conflict() {
        let db = crate::Db::open_in_memory().expect("boot");
        db.create_project("A", "/tmp/zk-dup").await.expect("first");
        let err = db
            .create_project("B", "/tmp/zk-dup")
            .await
            .expect_err("duplicate root must fail");
        assert!(
            matches!(err, DbError::WorkspaceRootTaken(ref root) if root == "/tmp/zk-dup"),
            "unexpected: {err:?}"
        );
    }

    /// 同名不同 root 合法（旧 schema 仅对 `workspace_root` 设 UNIQUE）。
    #[tokio::test]
    async fn create_same_name_different_root_ok() {
        let db = crate::Db::open_in_memory().expect("boot");
        db.create_project("Same", "/tmp/zk-a").await.expect("a");
        db.create_project("Same", "/tmp/zk-b").await.expect("b");
        assert_eq!(db.list_projects().await.expect("list").len(), 2);
    }

    /// 空库列表为空数组（而非错误）。
    #[tokio::test]
    async fn list_empty_returns_empty_vec() {
        let db = crate::Db::open_in_memory().expect("boot");
        assert!(db.list_projects().await.expect("list").is_empty());
    }

    /// 列表按 `created_at DESC, id DESC` 排序（直连布不同时刻数据验证）。
    #[tokio::test]
    async fn list_orders_created_at_desc_then_id_desc() {
        let db = crate::Db::open_in_memory().expect("boot");
        for (id, created) in [
            ("id-b", "2026-08-01T00:00:00.000000Z"),
            ("id-a", "2026-08-01T00:00:00.000000Z"),
            ("id-c", "2026-08-02T00:00:00.000000Z"),
        ] {
            db.with_conn_blocking(|conn| {
                conn.execute(
                    "INSERT INTO projects (id, name, workspace_root, created_at)
                     VALUES (?1, ?1, ?1, ?2)",
                    params![id, created],
                )?;
                Ok(())
            })
            .expect("seed");
        }
        let ids: Vec<String> = db
            .list_projects()
            .await
            .expect("list")
            .into_iter()
            .map(|p| p.id)
            .collect();
        assert_eq!(ids, ["id-c", "id-b", "id-a"]);
    }

    /// get 命中返回与创建完全一致的整行。
    #[tokio::test]
    async fn get_roundtrips_created_record() {
        let db = crate::Db::open_in_memory().expect("boot");
        let created = db
            .create_project("Round", "/tmp/zk-round")
            .await
            .expect("create");
        let fetched = db
            .get_project(&created.id)
            .await
            .expect("get")
            .expect("must exist");
        assert_eq!(fetched, created);
    }

    /// get 未命中返回 `Ok(None)`（404 判定源）。
    #[tokio::test]
    async fn get_missing_returns_none() {
        let db = crate::Db::open_in_memory().expect("boot");
        assert_eq!(db.get_project("no-such-id").await.expect("get"), None);
    }

    /// `_in_current_write` 变体在 `with_writer` 闭包（持 writer 锁）内查询——
    /// 授权终局复检的连接复用形态；闭包内改走 `_blocking` 出口会重入同一
    /// writer Mutex 永久死锁。
    #[tokio::test]
    async fn find_by_workspace_root_in_current_write_reuses_writer_connection() {
        let db = crate::Db::open_in_memory().expect("boot");
        let created = db
            .create_project("Trusted", "/tmp/zk-in-write")
            .await
            .expect("create");
        let found = db
            .with_writer(move |conn| {
                let tx = conn.transaction()?;
                super::find_project_by_workspace_root_in_current_write(&tx, "/tmp/zk-in-write")
            })
            .await
            .expect("query inside writer lock");
        assert_eq!(found, Some(created));
    }

    /// delete 首次 true、再次 false（幂等，`revoked` 布尔语义源）。
    #[tokio::test]
    async fn delete_is_idempotent() {
        let db = crate::Db::open_in_memory().expect("boot");
        let rec = db
            .create_project("Gone", "/tmp/zk-gone")
            .await
            .expect("create");
        assert!(db.delete_project(&rec.id).await.expect("first delete"));
        assert!(!db.delete_project(&rec.id).await.expect("second delete"));
        assert_eq!(db.get_project(&rec.id).await.expect("get"), None);
    }

    /// 项目配置读缺省为 `None`（调用方兜底默认形状）。
    #[tokio::test]
    async fn project_config_default_none() {
        let db = crate::Db::open_in_memory().expect("boot");
        assert_eq!(
            db.get_project_config_value("project_config")
                .await
                .expect("get"),
            None
        );
    }

    /// 项目配置写入 → 读回 → 覆盖写（upsert 幂等）全链路。
    #[tokio::test]
    async fn project_config_roundtrip_and_upsert() {
        let db = crate::Db::open_in_memory().expect("boot");
        db.put_project_config_value("project_config", r#"{"lastCost":0.5}"#)
            .await
            .expect("insert");
        assert_eq!(
            db.get_project_config_value("project_config")
                .await
                .expect("get"),
            Some(r#"{"lastCost":0.5}"#.to_owned())
        );
        db.put_project_config_value("project_config", r#"{"lastCost":1.5}"#)
            .await
            .expect("upsert");
        assert_eq!(
            db.get_project_config_value("project_config")
                .await
                .expect("get"),
            Some(r#"{"lastCost":1.5}"#.to_owned())
        );
    }

    /// `project_config` 与跨项目 `config` 表相互隔离（同 key 互不可见）。
    #[tokio::test]
    async fn project_config_isolated_from_user_config_table() {
        let db = crate::Db::open_in_memory().expect("boot");
        db.put_config_value("shared_key", "user-side")
            .await
            .expect("user config write");
        assert_eq!(
            db.get_project_config_value("shared_key")
                .await
                .expect("get"),
            None
        );
        db.put_project_config_value("shared_key", "project-side")
            .await
            .expect("project config write");
        assert_eq!(
            db.get_config_value("shared_key").await.expect("get"),
            Some("user-side".to_owned())
        );
    }

    /// 文件库跨连接持久：项目与项目配置写入 → 重开同一文件仍可读回。
    #[tokio::test]
    async fn projects_survive_reopen() {
        let dir = std::env::temp_dir().join(format!("zk-db-project-{}", uuid::Uuid::new_v4()));
        let path = dir.join("data.db");
        let created = {
            let db = crate::Db::open(&path).expect("open file db");
            db.put_project_config_value("project_config", "{}")
                .await
                .expect("config write");
            db.create_project("Persist", "/tmp/zk-persist")
                .await
                .expect("create")
        };
        let db = crate::Db::open(&path).expect("reopen file db");
        assert_eq!(
            db.get_project(&created.id).await.expect("get"),
            Some(created)
        );
        assert_eq!(
            db.get_project_config_value("project_config")
                .await
                .expect("get"),
            Some("{}".to_owned())
        );
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
