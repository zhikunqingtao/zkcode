//! 任务状态记录——对照旧 `TaskState.java`（63L）+ `TaskStatus.java`（26L）。

#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_panics_doc)]
//!
//! 线程安全：`status` / `error` / `output` 经 `Mutex` 守护
//! （Java 用 `volatile`），`child_task_ids` 经 `Mutex<Vec>`
//! （Java 用 `CopyOnWriteArrayList`）。`cancel` 为
//! [`tokio_util::sync::CancellationToken`]——三层取消树的第三层。

use std::sync::Mutex;

use tokio_util::sync::CancellationToken;

/// 任务状态枚举（对照旧 `TaskStatus.java`）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskStatus {
    /// 等待执行。
    Pending,
    /// 正在执行。
    Running,
    /// 进行中（语义等价于 Running）。
    InProgress,
    /// 已完成（终态）。
    Completed,
    /// 已失败（终态）。
    Failed,
    /// 已取消（终态）。
    Cancelled,
    /// 已杀死（终态）。
    Killed,
}

impl TaskStatus {
    /// 是否为终态（对照旧 `isTerminal`）。
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Killed
        )
    }

    /// 是否为活跃态（对照旧 `isActive`，Running 和 InProgress 等价）。
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Running | Self::InProgress)
    }

    /// 序列化字符串（大写，对照旧 `TaskStatus.name()`）。
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::InProgress => "IN_PROGRESS",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::Killed => "KILLED",
        }
    }

    /// 从字符串解析（大小写不敏感，对照旧 `TaskStatus.valueOf`）。
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "PENDING" => Some(Self::Pending),
            "RUNNING" => Some(Self::Running),
            "IN_PROGRESS" => Some(Self::InProgress),
            "COMPLETED" => Some(Self::Completed),
            "FAILED" => Some(Self::Failed),
            "CANCELLED" => Some(Self::Cancelled),
            "KILLED" => Some(Self::Killed),
            _ => None,
        }
    }
}

/// 任务状态记录——持有后台任务的完整生命周期状态
/// （对照旧 `TaskState.java`）。
pub struct TaskState {
    /// 任务唯一 ID。
    pub task_id: String,
    /// 所属会话 ID。
    pub session_id: String,
    /// 任务描述。
    pub description: Option<String>,
    /// 创建时间（epoch 毫秒）。
    pub created_at: i64,
    /// 状态（`Mutex` 守护，对照旧 `volatile`）。
    status: Mutex<TaskStatus>,
    /// 错误信息。
    error: Mutex<Option<String>>,
    /// 输出内容。
    output: Mutex<Option<String>>,
    /// 子任务 ID 列表（对照旧 `CopyOnWriteArrayList`）。
    child_task_ids: Mutex<Vec<String>>,
    /// 取消令牌（三层取消树的第三层）。
    pub cancel: CancellationToken,
}

impl TaskState {
    /// 构造新任务状态（初始为 `Pending`）。
    #[must_use]
    pub fn new(task_id: impl Into<String>, session_id: impl Into<String>) -> Self {
        Self::with_status(task_id, session_id, TaskStatus::Pending)
    }

    /// 构造指定状态的任务状态。
    #[must_use]
    pub fn with_status(
        task_id: impl Into<String>,
        session_id: impl Into<String>,
        status: TaskStatus,
    ) -> Self {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0));
        Self {
            task_id: task_id.into(),
            session_id: session_id.into(),
            description: None,
            created_at,
            status: Mutex::new(status),
            error: Mutex::new(None),
            output: Mutex::new(None),
            child_task_ids: Mutex::new(Vec::new()),
            cancel: CancellationToken::new(),
        }
    }

    /// 设置描述。
    #[must_use]
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// 读取当前状态。
    pub fn status(&self) -> TaskStatus {
        *self.status.lock().expect("status mutex poisoned")
    }

    /// 设置状态。
    pub fn set_status(&self, status: TaskStatus) {
        *self.status.lock().expect("status mutex poisoned") = status;
    }

    /// 读取错误信息。
    pub fn error(&self) -> Option<String> {
        self.error.lock().expect("error mutex poisoned").clone()
    }

    /// 设置错误信息。
    pub fn set_error(&self, error: impl Into<String>) {
        *self.error.lock().expect("error mutex poisoned") = Some(error.into());
    }

    /// 读取输出内容。
    pub fn output(&self) -> Option<String> {
        self.output.lock().expect("output mutex poisoned").clone()
    }

    /// 设置输出内容。
    pub fn set_output(&self, output: impl Into<String>) {
        *self.output.lock().expect("output mutex poisoned") = Some(output.into());
    }

    /// 添加子任务 ID。
    pub fn add_child_task(&self, task_id: impl Into<String>) {
        self.child_task_ids
            .lock()
            .expect("child_task_ids mutex poisoned")
            .push(task_id.into());
    }

    /// 读取子任务 ID 列表。
    pub fn child_task_ids(&self) -> Vec<String> {
        self.child_task_ids
            .lock()
            .expect("child_task_ids mutex poisoned")
            .clone()
    }

    /// 是否为终态。
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.status().is_terminal()
    }

    /// 是否为活跃态。
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.status().is_active()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_status_terminal() {
        assert!(TaskStatus::Completed.is_terminal());
        assert!(TaskStatus::Failed.is_terminal());
        assert!(TaskStatus::Cancelled.is_terminal());
        assert!(TaskStatus::Killed.is_terminal());
        assert!(!TaskStatus::Pending.is_terminal());
        assert!(!TaskStatus::Running.is_terminal());
    }

    #[test]
    fn task_status_active() {
        assert!(TaskStatus::Running.is_active());
        assert!(TaskStatus::InProgress.is_active());
        assert!(!TaskStatus::Pending.is_active());
        assert!(!TaskStatus::Completed.is_active());
    }

    #[test]
    fn task_state_lifecycle() {
        let state = TaskState::new("t1", "s1").with_description("test task");
        assert_eq!(state.status(), TaskStatus::Pending);
        assert!(!state.is_terminal());
        assert!(!state.is_active());

        state.set_status(TaskStatus::Running);
        assert!(state.is_active());

        state.set_status(TaskStatus::Completed);
        assert!(state.is_terminal());

        state.set_output("done");
        assert_eq!(state.output(), Some("done".into()));

        state.set_error("err");
        assert_eq!(state.error(), Some("err".into()));

        state.add_child_task("child1");
        assert_eq!(state.child_task_ids(), vec!["child1".to_owned()]);
    }

    #[test]
    fn task_status_parse_roundtrip() {
        for status in [
            TaskStatus::Pending,
            TaskStatus::Running,
            TaskStatus::InProgress,
            TaskStatus::Completed,
            TaskStatus::Failed,
            TaskStatus::Cancelled,
            TaskStatus::Killed,
        ] {
            assert_eq!(TaskStatus::parse(status.as_str()), Some(status));
            assert_eq!(
                TaskStatus::parse(&status.as_str().to_lowercase()),
                Some(status)
            );
        }
        assert_eq!(TaskStatus::parse("unknown"), None);
    }
}
