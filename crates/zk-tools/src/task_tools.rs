//! 任务工具族（6 件）——`TaskCreate` / `TaskUpdate` / `TaskList` /
//! `TaskGet` / `TaskOutput` / `TaskStop`。
//!
//! # 依赖方向
//!
//! zk-tools 不依赖 zk-engine。经 [`TaskCoordinatorPort`] 端口反转注入：
//! 具体实现（桥接到 `zk_engine::task::TaskCoordinator`）落 zk-server 组合根。

use std::time::Duration;

use crate::input::{failure, optional_str, required_str};
use crate::tool::{Tool, ToolContext, ToolOutput};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::fmt::Write as _;
use std::path::PathBuf;

// ═══ 端口 ═══

/// 任务信息快照（zk-tools 自持，不依赖 zk-engine 的 `TaskState`）。
#[derive(Clone, Debug)]
pub struct TaskSnapshot {
    /// 任务 ID。
    pub task_id: String,
    /// 会话 ID。
    pub session_id: String,
    /// 状态字符串。
    pub status: String,
    /// 描述。
    pub description: Option<String>,
    /// 输出。
    pub output: Option<String>,
    /// 错误。
    pub error: Option<String>,
    /// 创建时间（epoch 毫秒）。
    pub created_at: i64,
    /// 子任务数。
    pub child_count: usize,
}

/// Complete parent identity for a durable background task invocation.
#[derive(Clone, Debug)]
pub struct TaskInvocation {
    /// Durable task identifier.
    pub task_id: String,
    /// Authoritative parent session.
    pub session_id: String,
    /// User-facing task description.
    pub description: String,
    /// Complete child prompt.
    pub prompt: String,
    /// Requested task/agent specialization.
    pub task_type: String,
    /// Parent run used for authorization ancestry.
    pub parent_run_id: String,
    /// Canonical workspace inherited from the session.
    pub working_directory: PathBuf,
    /// Tool-use identifier that created the task.
    pub tool_use_id: String,
}

/// 任务协调端口（zk-tools 不依赖 zk-engine）。
///
/// zk-server 组合根装配具体实现：桥接到
/// `zk_engine::task::TaskCoordinator`。
pub trait TaskCoordinatorPort: Send + Sync {
    /// 提交任务。
    ///
    /// # Errors
    /// 并发任务数超过上限或引擎工厂拒绝时返回错误字符串。
    fn submit_task(&self, invocation: TaskInvocation) -> BoxFuture<'_, Result<(), String>>;

    /// 取消任务。
    fn cancel_task(&self, task_id: String) -> BoxFuture<'_, bool>;

    /// 查询单个任务。
    fn get_task(&self, task_id: String) -> BoxFuture<'_, Option<TaskSnapshot>>;

    /// 列出任务。
    fn list_tasks(
        &self,
        session_id: String,
        filter_status: Option<String>,
    ) -> BoxFuture<'_, Vec<TaskSnapshot>>;

    /// 更新任务状态 / 输出。
    ///
    /// # Errors
    /// 任务不存在时返回错误字符串。
    fn update_task(
        &self,
        task_id: String,
        status: Option<String>,
        output: Option<String>,
    ) -> BoxFuture<'_, Result<(), String>>;
}

// ═══ TaskCreate ═══

/// 创建后台任务（对照旧 `TaskCreateTool`）。
pub struct TaskCreateTool {
    port: std::sync::Arc<dyn TaskCoordinatorPort>,
}

impl TaskCreateTool {
    /// 构造工具。
    #[must_use]
    pub fn new(port: std::sync::Arc<dyn TaskCoordinatorPort>) -> Self {
        Self { port }
    }
}

const TASK_TIMEOUT: Duration = Duration::from_mins(30);

impl Tool for TaskCreateTool {
    fn name(&self) -> &'static str {
        "TaskCreate"
    }

    fn description(&self) -> &'static str {
        "Create a background task to execute independently. \
         Tasks run in their own virtual thread. \
         Use this for parallel execution of independent subtasks."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "description": {"type": "string", "description": "Task description"},
                "prompt": {"type": "string", "description": "Task prompt / instructions"},
                "taskType": {
                    "type": "string",
                    "enum": ["shell", "agent", "remote_agent", "in_process_teammate", "local_workflow", "monitor_mcp", "dream"],
                    "description": "Type of task to create (default: agent)"
                }
            },
            "required": ["description", "prompt"]
        })
    }

    fn timeout(&self) -> Duration {
        TASK_TIMEOUT
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let port = std::sync::Arc::clone(&self.port);
        Box::pin(async move {
            let description = match required_str(&input, "description") {
                Ok(d) => d.to_owned(),
                Err(e) => return e,
            };
            let prompt = match required_str(&input, "prompt") {
                Ok(p) => p.to_owned(),
                Err(e) => return e,
            };
            let task_type = optional_str(&input, "taskType")
                .unwrap_or("agent")
                .to_owned();
            let session_id = ctx.session_id().unwrap_or("default").to_owned();
            let Some(parent_run_id) = ctx.run_id().map(str::to_owned) else {
                return failure("TASK_CONTEXT_INCOMPLETE", "Task requires parent run id");
            };
            let Some(tool_use_id) = ctx.tool_use_id().map(str::to_owned) else {
                return failure("TASK_CONTEXT_INCOMPLETE", "Task requires tool use id");
            };
            let task_id = uuid::Uuid::new_v4().to_string()[..8].to_owned();

            match port
                .submit_task(TaskInvocation {
                    task_id: task_id.clone(),
                    session_id,
                    description: description.clone(),
                    prompt,
                    task_type,
                    parent_run_id,
                    working_directory: ctx.working_dir().to_path_buf(),
                    tool_use_id,
                })
                .await
            {
                Ok(()) => ToolOutput::ok(format!(
                    "Task #{task_id} created successfully: {description}"
                )),
                Err(e) => failure("TASK_CREATE_FAILED", format!("Failed to create task: {e}")),
            }
        })
    }

    fn is_destructive(&self, _input: &Value) -> bool {
        false
    }
}

// ═══ TaskUpdate ═══

/// 更新任务状态 / 输出（对照旧 `TaskUpdateTool`）。
pub struct TaskUpdateTool {
    port: std::sync::Arc<dyn TaskCoordinatorPort>,
}

impl TaskUpdateTool {
    /// 构造工具。
    #[must_use]
    pub fn new(port: std::sync::Arc<dyn TaskCoordinatorPort>) -> Self {
        Self { port }
    }
}

impl Tool for TaskUpdateTool {
    fn name(&self) -> &'static str {
        "TaskUpdate"
    }

    fn description(&self) -> &'static str {
        "Update the status or output of an existing background task."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "taskId": {"type": "string", "description": "Task ID to update"},
                "status": {"type": "string", "description": "New status for the task"},
                "output": {"type": "string", "description": "Output content to set"}
            },
            "required": ["taskId"]
        })
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let port = std::sync::Arc::clone(&self.port);
        Box::pin(async move {
            let task_id = match required_str(&input, "taskId") {
                Ok(t) => t.to_owned(),
                Err(e) => return e,
            };
            let status = optional_str(&input, "status").map(String::from);
            let output = optional_str(&input, "output").map(String::from);

            if port.get_task(task_id.clone()).await.is_none() {
                return failure("TASK_NOT_FOUND", format!("Task not found: {task_id}"));
            }

            match port.update_task(task_id.clone(), status, output).await {
                Ok(()) => {
                    let snap = port.get_task(task_id.clone()).await;
                    let status_str = snap.map_or_else(|| "unknown".to_owned(), |s| s.status);
                    ToolOutput::ok(format!("Task {task_id} updated. Status: {status_str}"))
                }
                Err(e) => failure("TASK_UPDATE_FAILED", e),
            }
        })
    }
}

// ═══ TaskList ═══

/// 列出当前会话的后台任务（对照旧 `TaskListTool`）。
pub struct TaskListTool {
    port: std::sync::Arc<dyn TaskCoordinatorPort>,
}

impl TaskListTool {
    /// 构造工具。
    #[must_use]
    pub fn new(port: std::sync::Arc<dyn TaskCoordinatorPort>) -> Self {
        Self { port }
    }
}

impl Tool for TaskListTool {
    fn name(&self) -> &'static str {
        "TaskList"
    }

    fn description(&self) -> &'static str {
        "List background tasks in the current session, optionally filtered by status."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "status": {"type": "string", "description": "Filter by status (PENDING/RUNNING/COMPLETED/FAILED/CANCELLED)"}
            }
        })
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let port = std::sync::Arc::clone(&self.port);
        Box::pin(async move {
            let filter = optional_str(&input, "status");
            let session_id = ctx.session_id().unwrap_or("default").to_owned();

            let tasks = port.list_tasks(session_id, filter.map(str::to_owned)).await;

            if tasks.is_empty() {
                return ToolOutput::ok("No tasks found.");
            }

            let mut sb = String::new();
            writeln!(sb, "Tasks ({}):", tasks.len()).unwrap();
            for t in &tasks {
                writeln!(
                    sb,
                    "  [{}] {} — {}",
                    t.status,
                    t.task_id,
                    t.description.as_deref().unwrap_or("(no description)")
                )
                .unwrap();
                if let Some(out) = &t.output {
                    let preview = if out.len() > 100 {
                        format!("{}...", &out[..100])
                    } else {
                        out.clone()
                    };
                    writeln!(sb, "    Output: {preview}").unwrap();
                }
            }
            ToolOutput::ok(sb)
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }
}

// ═══ TaskGet ═══

/// 获取单个任务详情（对照旧 `TaskGetTool`）。
pub struct TaskGetTool {
    port: std::sync::Arc<dyn TaskCoordinatorPort>,
}

impl TaskGetTool {
    /// 构造工具。
    #[must_use]
    pub fn new(port: std::sync::Arc<dyn TaskCoordinatorPort>) -> Self {
        Self { port }
    }
}

impl Tool for TaskGetTool {
    fn name(&self) -> &'static str {
        "TaskGet"
    }

    fn description(&self) -> &'static str {
        "Get detailed information about a specific background task."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "taskId": {"type": "string", "description": "Task ID to query"}
            },
            "required": ["taskId"]
        })
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let port = std::sync::Arc::clone(&self.port);
        Box::pin(async move {
            let task_id = match required_str(&input, "taskId") {
                Ok(t) => t.to_owned(),
                Err(e) => return e,
            };

            match port.get_task(task_id.clone()).await {
                Some(t) => {
                    let mut sb = String::new();
                    writeln!(sb, "Task: {}", t.task_id).unwrap();
                    writeln!(sb, "Status: {}", t.status).unwrap();
                    writeln!(
                        sb,
                        "Description: {}",
                        t.description.as_deref().unwrap_or("(none)")
                    )
                    .unwrap();
                    writeln!(sb, "Created: {}", t.created_at).unwrap();
                    if let Some(out) = &t.output {
                        writeln!(sb, "Output:\n{out}").unwrap();
                    }
                    if let Some(err) = &t.error {
                        writeln!(sb, "Error: {err}").unwrap();
                    }
                    writeln!(sb, "Child tasks: {}", t.child_count).unwrap();
                    ToolOutput::ok(sb)
                }
                None => failure("TASK_NOT_FOUND", format!("Task not found: {task_id}")),
            }
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }
}

// ═══ TaskOutput ═══

/// Read or briefly wait for DB-authoritative task output without mutating state.
pub struct TaskOutputTool {
    port: std::sync::Arc<dyn TaskCoordinatorPort>,
}

impl TaskOutputTool {
    /// Construct the task output reader.
    #[must_use]
    pub fn new(port: std::sync::Arc<dyn TaskCoordinatorPort>) -> Self {
        Self { port }
    }
}

impl Tool for TaskOutputTool {
    fn name(&self) -> &'static str {
        "TaskOutput"
    }

    fn description(&self) -> &'static str {
        "Read a task's durable output, optionally waiting briefly for a terminal state."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "taskId": {"type": "string", "description": "Task ID to read"},
                "block": {"type": "boolean", "default": true},
                "timeout": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 30,
                    "default": 30,
                    "description": "Maximum wait in seconds; timeout never changes task state"
                }
            },
            "required": ["taskId"]
        })
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let port = std::sync::Arc::clone(&self.port);
        Box::pin(async move {
            let task_id = match required_str(&input, "taskId") {
                Ok(value) => value.to_owned(),
                Err(error) => return error,
            };
            let block = input.get("block").and_then(Value::as_bool).unwrap_or(true);
            let timeout_seconds = input
                .get("timeout")
                .and_then(Value::as_u64)
                .unwrap_or(30)
                .min(30);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds);
            loop {
                let Some(task) = port.get_task(task_id.clone()).await else {
                    return failure("TASK_NOT_FOUND", format!("Task not found: {task_id}"));
                };
                let terminal = matches!(
                    task.status.to_ascii_uppercase().as_str(),
                    "COMPLETED" | "FAILED" | "CANCELLED" | "KILLED"
                );
                if terminal || !block {
                    let mut output = format!("Task: {}\nStatus: {}", task.task_id, task.status);
                    if let Some(content) = task.output {
                        output.push_str("\nOutput:\n");
                        output.push_str(&content);
                    }
                    if let Some(error) = task.error {
                        output.push_str("\nError: ");
                        output.push_str(&error);
                    }
                    return ToolOutput::ok(output);
                }
                if tokio::time::Instant::now() >= deadline {
                    return failure(
                        "TASK_OUTPUT_TIMEOUT",
                        format!("Task {task_id} did not finish within {timeout_seconds}s"),
                    );
                }
                tokio::select! {
                    () = ctx.cancel.cancelled() => {
                        return failure("TASK_OUTPUT_CANCELLED", "Task output wait cancelled");
                    }
                    () = tokio::time::sleep(Duration::from_millis(50)) => {}
                }
            }
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }
}

// ═══ TaskStop ═══

/// 停止 / 取消正在执行的后台任务（对照旧 `TaskStopTool`）。
pub struct TaskStopTool {
    port: std::sync::Arc<dyn TaskCoordinatorPort>,
}

impl TaskStopTool {
    /// 构造工具。
    #[must_use]
    pub fn new(port: std::sync::Arc<dyn TaskCoordinatorPort>) -> Self {
        Self { port }
    }
}

impl Tool for TaskStopTool {
    fn name(&self) -> &'static str {
        "TaskStop"
    }

    fn description(&self) -> &'static str {
        "Stop/cancel a running background task. \
         Uses three-layer interrupt propagation to cleanly terminate the task."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "taskId": {"type": "string", "description": "Task ID to stop"},
                "reason": {"type": "string", "description": "Reason for cancellation"}
            },
            "required": ["taskId"]
        })
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let port = std::sync::Arc::clone(&self.port);
        Box::pin(async move {
            let task_id = match required_str(&input, "taskId") {
                Ok(t) => t.to_owned(),
                Err(e) => return e,
            };
            let reason = optional_str(&input, "reason").unwrap_or("User requested cancellation");

            match port.get_task(task_id.clone()).await {
                None => failure("TASK_NOT_FOUND", format!("Task not found: {task_id}")),
                Some(t) => {
                    let status_upper = t.status.to_uppercase();
                    if matches!(
                        status_upper.as_str(),
                        "COMPLETED" | "FAILED" | "CANCELLED" | "KILLED"
                    ) {
                        return failure(
                            "TASK_ALREADY_TERMINAL",
                            format!("Task already in terminal state: {}", t.status),
                        );
                    }
                    if port.cancel_task(task_id.clone()).await {
                        ToolOutput::ok(format!("Task {task_id} cancelled. Reason: {reason}"))
                    } else {
                        failure(
                            "TASK_CANCEL_FAILED",
                            format!("Failed to cancel task: {task_id}"),
                        )
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    struct StubPort;
    impl TaskCoordinatorPort for StubPort {
        fn submit_task(&self, _invocation: TaskInvocation) -> BoxFuture<'_, Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
        fn cancel_task(&self, _id: String) -> BoxFuture<'_, bool> {
            Box::pin(async { true })
        }
        fn get_task(&self, _id: String) -> BoxFuture<'_, Option<TaskSnapshot>> {
            Box::pin(async {
                Some(TaskSnapshot {
                    task_id: "t1".into(),
                    session_id: "s1".into(),
                    status: "RUNNING".into(),
                    description: Some("test".into()),
                    output: None,
                    error: None,
                    created_at: 0,
                    child_count: 0,
                })
            })
        }
        fn list_tasks(
            &self,
            _sid: String,
            _filter: Option<String>,
        ) -> BoxFuture<'_, Vec<TaskSnapshot>> {
            Box::pin(async {
                vec![TaskSnapshot {
                    task_id: "t1".into(),
                    session_id: "s1".into(),
                    status: "RUNNING".into(),
                    description: Some("test task".into()),
                    output: Some("partial".into()),
                    error: None,
                    created_at: 0,
                    child_count: 0,
                }]
            })
        }
        fn update_task(
            &self,
            _id: String,
            _status: Option<String>,
            _output: Option<String>,
        ) -> BoxFuture<'_, Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
            .with_session_id("s1")
            .with_run_id("run-1")
            .with_tool_use_id("tool-use-1")
            .with_working_dir("/tmp")
    }

    #[tokio::test]
    async fn task_create_success() {
        let tool = TaskCreateTool::new(std::sync::Arc::new(StubPort));
        let output = tool
            .execute(json!({"description": "test", "prompt": "do x"}), ctx())
            .await;
        assert!(!output.is_error);
        assert!(output.content.contains("created successfully"));
    }

    #[tokio::test]
    async fn task_create_missing_desc() {
        let tool = TaskCreateTool::new(std::sync::Arc::new(StubPort));
        let output = tool.execute(json!({"prompt": "x"}), ctx()).await;
        assert!(output.is_error);
    }

    #[tokio::test]
    async fn task_update_success() {
        let tool = TaskUpdateTool::new(std::sync::Arc::new(StubPort));
        let output = tool
            .execute(json!({"taskId": "t1", "status": "COMPLETED"}), ctx())
            .await;
        assert!(!output.is_error);
    }

    #[tokio::test]
    async fn task_list_returns_tasks() {
        let tool = TaskListTool::new(std::sync::Arc::new(StubPort));
        let output = tool.execute(json!({}), ctx()).await;
        assert!(!output.is_error);
        assert!(output.content.contains("Tasks (1)"));
    }

    #[tokio::test]
    async fn task_get_returns_details() {
        let tool = TaskGetTool::new(std::sync::Arc::new(StubPort));
        let output = tool.execute(json!({"taskId": "t1"}), ctx()).await;
        assert!(!output.is_error);
        assert!(output.content.contains("Task: t1"));
    }

    #[tokio::test]
    async fn task_output_nonblocking_reads_without_mutation() {
        let tool = TaskOutputTool::new(std::sync::Arc::new(StubPort));
        let output = tool
            .execute(json!({"taskId": "t1", "block": false}), ctx())
            .await;
        assert!(!output.is_error);
        assert!(output.content.contains("Status: RUNNING"));
    }

    #[tokio::test]
    async fn task_stop_success() {
        let tool = TaskStopTool::new(std::sync::Arc::new(StubPort));
        let output = tool.execute(json!({"taskId": "t1"}), ctx()).await;
        assert!(!output.is_error);
        assert!(output.content.contains("cancelled"));
    }

    #[tokio::test]
    async fn task_stop_not_found() {
        struct NotFoundPort;
        impl TaskCoordinatorPort for NotFoundPort {
            fn submit_task(&self, _: TaskInvocation) -> BoxFuture<'_, Result<(), String>> {
                Box::pin(async { Ok(()) })
            }
            fn cancel_task(&self, _: String) -> BoxFuture<'_, bool> {
                Box::pin(async { false })
            }
            fn get_task(&self, _: String) -> BoxFuture<'_, Option<TaskSnapshot>> {
                Box::pin(async { None })
            }
            fn list_tasks(&self, _: String, _: Option<String>) -> BoxFuture<'_, Vec<TaskSnapshot>> {
                Box::pin(async { vec![] })
            }
            fn update_task(
                &self,
                _: String,
                _: Option<String>,
                _: Option<String>,
            ) -> BoxFuture<'_, Result<(), String>> {
                Box::pin(async { Ok(()) })
            }
        }
        let tool = TaskStopTool::new(std::sync::Arc::new(NotFoundPort));
        let output = tool.execute(json!({"taskId": "nope"}), ctx()).await;
        assert!(output.is_error);
        assert!(output.content.starts_with("TASK_NOT_FOUND"));
    }
}
