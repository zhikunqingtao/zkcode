//! 任务持久层——`tasks` 表的 CRUD 方法。
//!
//! DDL 由 `migrations/V2__init_session_message.sql` 已建（`tasks` 表，
//! 含 `id` / `session_id` / `description` / `task_type` / `status` /
//! `output` / `error` / `progress` / `created_at` / `updated_at` /
//! `completed_at`）。本模块仅提供仓储方法，不重复建表。
//!
//! 有意差异：任务描述 DDL 简化版仅含 8 列，实际表含 11 列（已由迁移
//! 建表，本仓储适配实际 schema）。

use crate::Db;
use crate::error::DbError;
use crate::time::{format_rfc3339_micros, now_millis};

/// 任务记录（存储形状）。
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRecord {
    /// 任务 ID。
    pub id: String,
    /// 所属会话 ID。
    pub session_id: String,
    /// 任务类型（默认 `agent`）。
    pub task_type: String,
    /// 状态（默认 `PENDING`）。
    pub status: String,
    /// 描述。
    pub description: Option<String>,
    /// 输出。
    pub output: Option<String>,
    /// 失败原因。
    pub error: Option<String>,
    /// 进度（0.0..=1.0）。
    pub progress: f64,
    /// 创建时间（RFC 3339）。
    pub created_at: String,
    /// 更新时间（RFC 3339）。
    pub updated_at: String,
    /// 终态时间。
    pub completed_at: Option<String>,
}

impl TaskRecord {
    /// Creation time as epoch milliseconds for tool/WS projections.
    #[must_use]
    pub fn created_at_millis(&self) -> i64 {
        crate::time::parse_rfc3339_millis(&self.created_at).unwrap_or(0)
    }
}

impl Db {
    /// Count active tasks across all sessions.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` query fails or returns an invalid count.
    pub async fn count_active_tasks(&self) -> Result<usize, DbError> {
        self.with_reader(move |conn| {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM tasks WHERE status IN ('PENDING','RUNNING','IN_PROGRESS')",
                [],
                |row| row.get(0),
            )?;
            usize::try_from(count).map_err(|_| DbError::Invalid("negative task count".into()))
        })
        .await
    }

    /// 保存任务（INSERT OR REPLACE）。
    ///
    /// # Errors
    /// SQL 执行失败时返回。
    pub async fn save_task(&self, record: &TaskRecord) -> Result<(), DbError> {
        let record = record.clone();
        self.with_writer(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO tasks \
                 (id, session_id, description, task_type, status, output, error, progress, \
                  created_at, updated_at, completed_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![
                    &record.id,
                    &record.session_id,
                    &record.description,
                    &record.task_type,
                    &record.status,
                    &record.output,
                    &record.error,
                    record.progress,
                    &record.created_at,
                    &record.updated_at,
                    &record.completed_at,
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// 按会话 ID 查询任务列表。
    ///
    /// # Errors
    /// SQL 执行失败时返回。
    pub async fn find_tasks_by_session(
        &self,
        session_id: &str,
    ) -> Result<Vec<TaskRecord>, DbError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, session_id, task_type, status, description, output, error, progress, \
                 created_at, updated_at, completed_at FROM tasks WHERE session_id = ?1 \
                 ORDER BY created_at DESC",
            )?;
            let rows = stmt.query_map([&session_id], |row| {
                Ok(TaskRecord {
                    id: row.get(0)?,
                    session_id: row.get(1)?,
                    task_type: row.get(2)?,
                    status: row.get(3)?,
                    description: row.get(4)?,
                    output: row.get(5)?,
                    error: row.get(6)?,
                    progress: row.get(7)?,
                    created_at: row.get(8)?,
                    updated_at: row.get(9)?,
                    completed_at: row.get(10)?,
                })
            })?;
            let mut records = Vec::new();
            for row in rows {
                records.push(row?);
            }
            Ok(records)
        })
        .await
    }

    /// 按 ID 查询单个任务。
    ///
    /// # Errors
    /// SQL 执行失败时返回。
    pub async fn find_task_by_id(&self, id: &str) -> Result<Option<TaskRecord>, DbError> {
        let id = id.to_owned();
        self.with_reader(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, session_id, task_type, status, description, output, error, progress, \
                 created_at, updated_at, completed_at FROM tasks WHERE id = ?1",
            )?;
            let mut rows = stmt.query_map([&id], |row| {
                Ok(TaskRecord {
                    id: row.get(0)?,
                    session_id: row.get(1)?,
                    task_type: row.get(2)?,
                    status: row.get(3)?,
                    description: row.get(4)?,
                    output: row.get(5)?,
                    error: row.get(6)?,
                    progress: row.get(7)?,
                    created_at: row.get(8)?,
                    updated_at: row.get(9)?,
                    completed_at: row.get(10)?,
                })
            })?;
            match rows.next() {
                Some(Ok(record)) => Ok(Some(record)),
                Some(Err(e)) => Err(e.into()),
                None => Ok(None),
            }
        })
        .await
    }

    /// 更新任务状态。
    ///
    /// # Errors
    /// SQL 执行失败时返回。
    pub async fn update_task_status(&self, id: &str, status: &str) -> Result<(), DbError> {
        let id = id.to_owned();
        let status = status.to_owned();
        self.with_writer(move |conn| {
            let now = format_rfc3339_micros(now_millis());
            conn.execute(
                "UPDATE tasks SET status = ?1, updated_at = ?2, \
                 completed_at = CASE WHEN ?1 IN ('COMPLETED','FAILED','CANCELLED','KILLED') \
                                     THEN ?2 ELSE completed_at END WHERE id = ?3",
                rusqlite::params![&status, &now, &id],
            )?;
            Ok(())
        })
        .await
    }

    /// 更新任务输出。
    ///
    /// # Errors
    /// SQL 执行失败时返回。
    pub async fn update_task_output(&self, id: &str, output: Option<&str>) -> Result<(), DbError> {
        let id = id.to_owned();
        let output = output.map(String::from);
        self.with_writer(move |conn| {
            let now = format_rfc3339_micros(now_millis());
            conn.execute(
                "UPDATE tasks SET output = ?1, updated_at = ?2 WHERE id = ?3",
                rusqlite::params![&output, &now, &id],
            )?;
            Ok(())
        })
        .await
    }

    /// Atomically persist a task terminal/result projection.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` update fails.
    pub async fn update_task_result(
        &self,
        id: &str,
        status: &str,
        output: Option<&str>,
        error: Option<&str>,
        progress: f64,
    ) -> Result<(), DbError> {
        let id = id.to_owned();
        let status = status.to_owned();
        let output = output.map(String::from);
        let error = error.map(String::from);
        self.with_writer(move |conn| {
            let now = format_rfc3339_micros(now_millis());
            conn.execute(
                "UPDATE tasks SET status = ?1, output = ?2, error = ?3, progress = ?4, \
                 updated_at = ?5, completed_at = CASE \
                    WHEN ?1 IN ('COMPLETED','FAILED','CANCELLED','KILLED') THEN ?5 \
                    ELSE NULL END WHERE id = ?6",
                rusqlite::params![&status, &output, &error, progress, &now, &id],
            )?;
            Ok(())
        })
        .await
    }

    /// Mark tasks left active by a previous process as interrupted/killed.
    ///
    /// # Errors
    /// Returns [`DbError`] when the `SQLite` update fails.
    pub async fn interrupt_active_tasks(&self) -> Result<usize, DbError> {
        self.with_writer(move |conn| {
            let now = format_rfc3339_micros(now_millis());
            let changed = conn.execute(
                "UPDATE tasks SET status = 'KILLED', error = 'Task interrupted by process restart', \
                 updated_at = ?1, completed_at = ?1 \
                 WHERE status IN ('PENDING','RUNNING','IN_PROGRESS')",
                [&now],
            )?;
            Ok(changed)
        })
        .await
    }
}

/// 构造新任务的默认记录（当前时刻，`PENDING` 状态，`agent` 类型）。
#[must_use]
#[allow(dead_code)]
pub fn new_task_record(id: &str, session_id: &str, description: Option<&str>) -> TaskRecord {
    let now = format_rfc3339_micros(now_millis());
    TaskRecord {
        id: id.to_owned(),
        session_id: session_id.to_owned(),
        task_type: "agent".to_owned(),
        status: "PENDING".to_owned(),
        description: description.map(String::from),
        output: None,
        error: None,
        progress: 0.0,
        created_at: now.clone(),
        updated_at: now,
        completed_at: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn task_crud_lifecycle() {
        let db = Db::open_in_memory().expect("in-memory db");
        let session = db
            .create_session("claude-sonnet-4-5", "/tmp/zkcode-task-tests")
            .await
            .expect("session");
        let record = new_task_record("task-1", &session.id, Some("test task"));
        db.save_task(&record).await.expect("save");
        db.update_task_status("task-1", "RUNNING")
            .await
            .expect("update status");

        let found = db.find_task_by_id("task-1").await.expect("find");
        let found = found.expect("task exists");
        assert_eq!(found.status, "RUNNING");
        assert_eq!(found.description, Some("test task".into()));

        db.update_task_output("task-1", Some("done"))
            .await
            .expect("update output");
        db.update_task_result("task-1", "COMPLETED", Some("done"), None, 1.0)
            .await
            .expect("terminal result");
        let found = db
            .find_task_by_id("task-1")
            .await
            .expect("find")
            .expect("exists");
        assert_eq!(found.output, Some("done".into()));
        assert_eq!(found.status, "COMPLETED");
        assert!((found.progress - 1.0).abs() < f64::EPSILON);
        assert!(found.completed_at.is_some());

        let tasks = db.find_tasks_by_session(&session.id).await.expect("list");
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].id, "task-1");
    }

    #[tokio::test]
    async fn find_nonexistent_returns_none() {
        let db = Db::open_in_memory().expect("in-memory db");
        let result = db.find_task_by_id("nope").await.expect("query ok");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn restart_marks_only_active_tasks_killed() {
        let db = Db::open_in_memory().expect("in-memory db");
        let session = db
            .create_session("test-model", "/tmp/zkcode-task-restart")
            .await
            .expect("session");
        for (id, status) in [("active", "RUNNING"), ("done", "COMPLETED")] {
            let mut task = new_task_record(id, &session.id, Some(id));
            task.status = status.to_owned();
            db.save_task(&task).await.expect("save task");
        }
        assert_eq!(db.interrupt_active_tasks().await.expect("reconcile"), 1);
        let active = db
            .find_task_by_id("active")
            .await
            .expect("query")
            .expect("active task");
        assert_eq!(active.status, "KILLED");
        assert_eq!(
            active.error.as_deref(),
            Some("Task interrupted by process restart")
        );
        assert!(active.completed_at.is_some());
        let done = db
            .find_task_by_id("done")
            .await
            .expect("query")
            .expect("done task");
        assert_eq!(done.status, "COMPLETED");
    }
}
