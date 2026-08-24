//! 任务编排服务——对照旧 `TaskCoordinator.java`（197L）。

#![allow(clippy::doc_markdown)]
//!
//! 职责：管理后台任务生命周期（submit/cancel/query）+ tokio::spawn 执行
//! + 超时看门狗 + 三层取消（CancellationToken → JoinHandle abort →
//!   持久终态）+ 经 [`MessageSink`] 推送 `task_update`。SQLite 是状态、输出和
//!   错误的唯一权威；内存映射只保存当前进程的取消令牌与 JoinHandle。
//!
//! 有意差异：Java 用 `Virtual Thread` + `Future.cancel(true)` +
//! `ScheduledExecutorService` 看门狗；本实现取 `tokio::spawn` +
//! `CancellationToken` + `tokio::time::timeout` 三层取消。

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{error, info, warn};
use zk_db::{Db, TaskRecord};

use crate::sink::MessageSink;
use crate::task::state::TaskStatus;
use crate::{NoopObservabilityRecorder, ObservabilityEvent, ObservabilityRecorder};
use zk_protocol::ServerMessage;

/// 最大并发任务数（对照旧 `MAX_CONCURRENT_TASKS = 10`）。
pub const MAX_CONCURRENT_TASKS: usize = 10;

/// 单任务超时（对照旧 `PER_TASK_TIMEOUT = 30min`）。
pub const PER_TASK_TIMEOUT: Duration = Duration::from_mins(30);

/// TaskResult 最大大小（对照旧 `MAX_OUTPUT_SIZE = 1MB`）。
pub const MAX_OUTPUT_SIZE: usize = 1024 * 1024;

/// 任务编排服务。
pub struct TaskCoordinator {
    /// 当前进程活跃任务的取消状态；查询结果从 DB 读取。
    tasks: DashMap<String, tokio_util::sync::CancellationToken>,
    /// 当前进程任务句柄映射：task_id → JoinHandle。
    handles: DashMap<String, JoinHandle<()>>,
    /// Durable source of truth for task state and output.
    db: Db,
    /// 下行推送端口。
    sink: Arc<dyn MessageSink>,
    /// Best-effort operations telemetry, separate from durable task state.
    observability: Arc<dyn ObservabilityRecorder>,
}

impl TaskCoordinator {
    /// 构造协调器。
    #[must_use]
    pub fn new(db: Db, sink: Arc<dyn MessageSink>) -> Self {
        Self {
            tasks: DashMap::new(),
            handles: DashMap::new(),
            db,
            sink,
            observability: Arc::new(NoopObservabilityRecorder),
        }
    }

    /// Attach the process-wide observability recorder.
    #[must_use]
    pub fn with_observability(mut self, recorder: Arc<dyn ObservabilityRecorder>) -> Self {
        self.observability = recorder;
        self
    }

    /// Persist a task owned by another runtime tracker (for example SwarmService).
    ///
    /// # Errors
    /// Returns an error when capacity is exhausted or persistence fails.
    pub async fn register_external_task(
        &self,
        task_id: &str,
        session_id: &str,
        task_type: &str,
        description: &str,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<TaskRecord, String> {
        let active_count = self
            .db
            .count_active_tasks()
            .await
            .map_err(|error| format!("Task store unavailable: {error}"))?;
        if active_count >= MAX_CONCURRENT_TASKS {
            return Err(format!(
                "Max concurrent tasks reached: {MAX_CONCURRENT_TASKS}"
            ));
        }
        let mut record = zk_db::new_task_record(task_id, session_id, Some(description));
        record.task_type = task_type.to_owned();
        TaskStatus::Pending.as_str().clone_into(&mut record.status);
        self.db
            .save_task(&record)
            .await
            .map_err(|error| format!("Failed to persist task: {error}"))?;
        self.tasks.insert(task_id.to_owned(), cancel);
        record_task_event(
            &self.observability,
            session_id,
            task_id,
            "registered",
            "pending",
        );
        push_task_update_sync(&self.sink, session_id, task_id, TaskStatus::Pending, None);
        Ok(record)
    }

    /// Mark an externally tracked task running.
    ///
    /// # Errors
    /// Returns an error when persistence fails.
    pub async fn mark_external_task_running(
        &self,
        task_id: &str,
        session_id: &str,
    ) -> Result<(), String> {
        self.db
            .update_task_result(task_id, TaskStatus::Running.as_str(), None, None, 0.0)
            .await
            .map_err(|error| format!("Failed to persist running task: {error}"))?;
        record_task_event(
            &self.observability,
            session_id,
            task_id,
            "running",
            "running",
        );
        push_task_update(&self.sink, session_id, task_id, TaskStatus::Running, None).await;
        Ok(())
    }

    /// Persist an externally tracked task's terminal result.
    ///
    /// # Errors
    /// Returns an error when persistence fails.
    pub async fn finish_external_task(
        &self,
        task_id: &str,
        session_id: &str,
        result: &Result<String, String>,
    ) -> Result<(), String> {
        let (status, output, error, progress) = match result {
            Ok(output) => (
                TaskStatus::Completed,
                Some(truncate_output(output)),
                None,
                1.0,
            ),
            Err(error) => (TaskStatus::Failed, None, Some(error.clone()), 0.0),
        };
        self.db
            .update_task_result(
                task_id,
                status.as_str(),
                output.as_deref(),
                error.as_deref(),
                progress,
            )
            .await
            .map_err(|db_error| format!("Failed to persist terminal task: {db_error}"))?;
        self.tasks.remove(task_id);
        self.handles.remove(task_id);
        record_task_event(
            &self.observability,
            session_id,
            task_id,
            "complete",
            if status == TaskStatus::Completed {
                "ok"
            } else {
                "error"
            },
        );
        push_task_update(&self.sink, session_id, task_id, status, output).await;
        Ok(())
    }

    /// 提交任务——在独立 tokio task 中异步执行（对照旧 `submit`）。
    ///
    /// # Errors
    /// 并发任务数超过 `MAX_CONCURRENT_TASKS` 时返回错误。
    pub async fn submit(
        &self,
        task_id: impl Into<String>,
        session_id: impl Into<String>,
        description: impl Into<String>,
        f: impl std::future::Future<Output = Result<String, String>> + Send + 'static,
    ) -> Result<TaskRecord, String> {
        self.submit_with_cancel(task_id, session_id, description, move |_cancel| f)
            .await
    }

    /// Submit a task whose provider/tool execution observes the registry token.
    ///
    /// # Errors
    /// Returns a persistence or concurrency-limit error before spawning the task.
    #[allow(clippy::too_many_lines)] // persistence, spawn, timeout and terminal write are one lifecycle
    pub async fn submit_with_cancel<F, Fut>(
        &self,
        task_id: impl Into<String>,
        session_id: impl Into<String>,
        description: impl Into<String>,
        build: F,
    ) -> Result<TaskRecord, String>
    where
        F: FnOnce(tokio_util::sync::CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
    {
        let task_id = task_id.into();
        let session_id = session_id.into();
        let description = description.into();

        let active_count = self
            .db
            .count_active_tasks()
            .await
            .map_err(|error| format!("Task store unavailable: {error}"))?;
        if active_count >= MAX_CONCURRENT_TASKS {
            return Err(format!(
                "Max concurrent tasks reached: {MAX_CONCURRENT_TASKS}"
            ));
        }

        let mut record = zk_db::new_task_record(&task_id, &session_id, Some(&description));
        TaskStatus::Pending.as_str().clone_into(&mut record.status);
        self.db
            .save_task(&record)
            .await
            .map_err(|error| format!("Failed to persist task: {error}"))?;
        let cancel = tokio_util::sync::CancellationToken::new();
        self.tasks.insert(task_id.clone(), cancel.clone());
        record_task_event(
            &self.observability,
            &session_id,
            &task_id,
            "submitted",
            "pending",
        );
        let f = build(cancel);

        let session_for_task = session_id.clone();
        let task_id_for_task = task_id.clone();
        let sink = Arc::clone(&self.sink);
        let db = self.db.clone();
        let observability = Arc::clone(&self.observability);

        let handle = tokio::spawn(async move {
            if let Err(error) = db
                .update_task_result(
                    &task_id_for_task,
                    TaskStatus::Running.as_str(),
                    None,
                    None,
                    0.0,
                )
                .await
            {
                error!(task_id = %task_id_for_task, %error, "failed to persist running task");
                return;
            }
            push_task_update(
                &sink,
                &session_for_task,
                &task_id_for_task,
                TaskStatus::Running,
                None,
            )
            .await;
            record_task_event(
                &observability,
                &session_for_task,
                &task_id_for_task,
                "running",
                "running",
            );

            let result = timeout(PER_TASK_TIMEOUT, f).await;

            let (terminal_status, terminal_output, terminal_error) = match result {
                Ok(Ok(output)) => (TaskStatus::Completed, Some(truncate_output(&output)), None),
                Ok(Err(e)) => {
                    error!("Task {task_id_for_task} failed: {e}");
                    (TaskStatus::Failed, None, Some(e))
                }
                Err(_) => {
                    warn!("Task {task_id_for_task} timed out after {PER_TASK_TIMEOUT:?}");
                    (TaskStatus::Killed, None, Some("Task timed out".to_owned()))
                }
            };
            if let Err(error) = db
                .update_task_result(
                    &task_id_for_task,
                    terminal_status.as_str(),
                    terminal_output.as_deref(),
                    terminal_error.as_deref(),
                    if terminal_status == TaskStatus::Completed {
                        1.0
                    } else {
                        0.0
                    },
                )
                .await
            {
                error!(task_id = %task_id_for_task, %error, "failed to persist terminal task");
            }
            push_task_update(
                &sink,
                &session_for_task,
                &task_id_for_task,
                terminal_status,
                terminal_output,
            )
            .await;
            record_task_event(
                &observability,
                &session_for_task,
                &task_id_for_task,
                "complete",
                if terminal_status == TaskStatus::Completed {
                    "ok"
                } else if terminal_status == TaskStatus::Cancelled {
                    "cancelled"
                } else {
                    "error"
                },
            );
        });

        self.handles.insert(task_id.clone(), handle);
        push_task_update_sync(&self.sink, &session_id, &task_id, TaskStatus::Pending, None);
        Ok(record)
    }

    /// 取消任务——三层中断传播（对照旧 `cancelTask`）。
    pub async fn cancel_task(&self, task_id: &str) -> bool {
        let Ok(Some(record)) = self.db.find_task_by_id(task_id).await else {
            return false;
        };
        if TaskStatus::parse(&record.status).is_some_and(|status| status.is_terminal()) {
            return false;
        }

        // Layer 1: CancellationToken
        if let Some(task) = self.tasks.get(task_id) {
            task.cancel();
        }

        // Layer 2: abort JoinHandle
        if let Some(handle) = self.handles.get(task_id) {
            handle.abort();
        }

        // Layer 3: 递归取消子任务
        if let Err(error) = self
            .db
            .update_task_result(
                task_id,
                TaskStatus::Cancelled.as_str(),
                record.output.as_deref(),
                Some("Task cancelled"),
                record.progress,
            )
            .await
        {
            error!(task_id, %error, "failed to persist task cancellation");
            return false;
        }
        info!("Task {task_id} cancelled");
        record_task_event(
            &self.observability,
            &record.session_id,
            task_id,
            "cancelled",
            "cancelled",
        );
        true
    }

    /// Query a task from the durable source of truth.
    ///
    /// # Errors
    /// Returns an error when the `SQLite` task query is unavailable.
    pub async fn get_task(&self, task_id: &str) -> Result<Option<TaskRecord>, String> {
        self.db
            .find_task_by_id(task_id)
            .await
            .map_err(|error| format!("Task store unavailable: {error}"))
    }

    /// 按状态过滤任务列表（对照旧 `listTasks`）。
    ///
    /// # Errors
    /// Returns an error when the `SQLite` task query is unavailable.
    pub async fn list_tasks(
        &self,
        session_id: &str,
        filter_status: Option<TaskStatus>,
    ) -> Result<Vec<TaskRecord>, String> {
        let mut records = self
            .db
            .find_tasks_by_session(session_id)
            .await
            .map_err(|error| format!("Task store unavailable: {error}"))?;
        if let Some(status) = filter_status {
            records.retain(|record| TaskStatus::parse(&record.status) == Some(status));
        }
        Ok(records)
    }

    /// 获取活跃任务数。
    ///
    /// # Errors
    /// Returns an error when the `SQLite` count query is unavailable.
    pub async fn active_count(&self) -> Result<usize, String> {
        self.db
            .count_active_tasks()
            .await
            .map_err(|error| format!("Task store unavailable: {error}"))
    }
}

fn record_task_event(
    recorder: &Arc<dyn ObservabilityRecorder>,
    session_id: &str,
    task_id: &str,
    action: &str,
    outcome: &str,
) {
    let mut event = ObservabilityEvent::new("task", action, outcome);
    event.session_id = Some(session_id.to_owned());
    event.attributes.insert(
        "taskId".to_owned(),
        serde_json::Value::String(task_id.to_owned()),
    );
    recorder.record(event);
}

impl Drop for TaskCoordinator {
    fn drop(&mut self) {
        info!(
            "TaskCoordinator shutting down, cancelling {} tasks",
            self.tasks.len()
        );
        let ids: Vec<String> = self.tasks.iter().map(|e| e.key().clone()).collect();
        for id in ids {
            if let Some(task) = self.tasks.get(&id) {
                task.cancel();
            }
            if let Some(handle) = self.handles.get(&id) {
                handle.abort();
            }
        }
    }
}

/// 截断超大输出（对照旧 `truncateOutput`，1MB 限制）。
fn truncate_output(output: &str) -> String {
    if output.len() <= MAX_OUTPUT_SIZE {
        return output.to_owned();
    }
    let cut = output
        .char_indices()
        .nth(MAX_OUTPUT_SIZE)
        .map_or(MAX_OUTPUT_SIZE, |(idx, _)| idx);
    format!("{}\n[Output truncated at 1MB limit]", &output[..cut])
}

/// 推送任务状态变更（对照旧 `notifyTaskUpdate`）。
async fn push_task_update(
    sink: &Arc<dyn MessageSink>,
    session_id: &str,
    task_id: &str,
    status: TaskStatus,
    output: Option<String>,
) {
    let message = ServerMessage::TaskUpdate {
        task_id: task_id.to_owned(),
        status: status.as_str().to_owned(),
        progress: output.clone(),
        output,
    };
    sink.push(session_id, message).await;
}

/// 同步入口（spawn 阻塞推送，用于 submit 后的初始通知）。
fn push_task_update_sync(
    sink: &Arc<dyn MessageSink>,
    session_id: &str,
    task_id: &str,
    task_status: TaskStatus,
    progress: Option<String>,
) {
    let sink = Arc::clone(sink);
    let session_id = session_id.to_owned();
    let task_id = task_id.to_owned();
    let status = task_status.as_str().to_owned();
    tokio::spawn(async move {
        sink.push(
            &session_id,
            ServerMessage::TaskUpdate {
                task_id,
                status,
                progress: progress.clone(),
                output: progress,
            },
        )
        .await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopSink;
    impl MessageSink for NoopSink {
        fn push<'a>(
            &'a self,
            _session_id: &'a str,
            _message: ServerMessage,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn submit_and_complete() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/task-1")
            .await
            .expect("session");
        let coordinator = TaskCoordinator::new(db.clone(), Arc::new(NoopSink));
        let _record = coordinator
            .submit("t1", &session.id, "test", async { Ok("done".to_owned()) })
            .await
            .expect("submit");
        // Wait for task to complete
        tokio::time::sleep(Duration::from_millis(100)).await;
        let persisted = coordinator
            .get_task("t1")
            .await
            .expect("query")
            .expect("persisted");
        assert_eq!(persisted.status, "COMPLETED");
        assert_eq!(persisted.output.as_deref(), Some("done"));
    }

    #[tokio::test]
    async fn submit_and_fail() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/task-2")
            .await
            .expect("session");
        let coordinator = TaskCoordinator::new(db, Arc::new(NoopSink));
        let _record = coordinator
            .submit("t1", session.id, "test", async { Err("boom".to_owned()) })
            .await
            .expect("submit");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let persisted = coordinator
            .get_task("t1")
            .await
            .expect("query")
            .expect("persisted");
        assert_eq!(persisted.status, "FAILED");
        assert_eq!(persisted.error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn cancel_task() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/task-3")
            .await
            .expect("session");
        let coordinator = TaskCoordinator::new(db, Arc::new(NoopSink));
        let _record = coordinator
            .submit("t1", session.id, "test", async {
                tokio::time::sleep(Duration::from_mins(1)).await;
                Ok("done".to_owned())
            })
            .await
            .expect("submit");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(coordinator.cancel_task("t1").await);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let persisted = coordinator
            .get_task("t1")
            .await
            .expect("query")
            .expect("persisted");
        assert_eq!(persisted.status, "CANCELLED");
    }

    #[tokio::test]
    async fn task_stop_cancels_the_token_given_to_provider_builder() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/task-provider-cancel")
            .await
            .expect("session");
        let coordinator = TaskCoordinator::new(db, Arc::new(NoopSink));
        let observed = Arc::new(std::sync::Mutex::new(None));
        let observed_for_build = Arc::clone(&observed);
        coordinator
            .submit_with_cancel(
                "provider-task",
                session.id,
                "wait for cancellation",
                move |cancel| {
                    *observed_for_build.lock().expect("observed token") = Some(cancel.clone());
                    async move {
                        cancel.cancelled().await;
                        Err("cancelled".to_owned())
                    }
                },
            )
            .await
            .expect("submit");
        assert!(coordinator.cancel_task("provider-task").await);
        assert!(
            observed
                .lock()
                .expect("observed token")
                .as_ref()
                .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
        );
    }

    #[tokio::test]
    async fn external_task_stop_cancels_the_shared_worker_token() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/task-external")
            .await
            .expect("session");
        let coordinator = TaskCoordinator::new(db, Arc::new(NoopSink));
        let cancel = tokio_util::sync::CancellationToken::new();
        coordinator
            .register_external_task(
                "worker-1",
                &session.id,
                "swarm:test:worker",
                "read files",
                cancel.clone(),
            )
            .await
            .expect("register external task");
        assert!(coordinator.cancel_task("worker-1").await);
        assert!(cancel.is_cancelled());
        assert_eq!(
            coordinator
                .get_task("worker-1")
                .await
                .unwrap()
                .unwrap()
                .status,
            "CANCELLED"
        );
    }

    #[tokio::test]
    async fn max_concurrent_rejects() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("test-model", "/tmp/task-4")
            .await
            .expect("session");
        let coordinator = TaskCoordinator::new(db, Arc::new(NoopSink));
        for i in 0..MAX_CONCURRENT_TASKS {
            coordinator
                .submit(format!("t{i}"), &session.id, "x", async {
                    tokio::time::sleep(Duration::from_mins(1)).await;
                    Ok("ok".to_owned())
                })
                .await
                .expect("submit");
        }
        let result = coordinator
            .submit("overflow", &session.id, "x", async { Ok("ok".to_owned()) })
            .await;
        assert!(result.is_err());
        tokio::task::yield_now().await;
    }

    #[test]
    fn truncate_output_respects_limit() {
        let long = "x".repeat(MAX_OUTPUT_SIZE + 100);
        let truncated = truncate_output(&long);
        assert!(truncated.ends_with("[Output truncated at 1MB limit]"));
        assert!(truncated.len() <= MAX_OUTPUT_SIZE + 100);
    }
}
