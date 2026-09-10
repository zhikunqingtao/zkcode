//! 任务工具族（6 件）——`TaskCreate` / `TaskUpdate` / `TaskList` /
//! `TaskGet` / `TaskOutput` / `TaskStop`。
//!
//! # 依赖方向
//!
//! zk-tools 不依赖 zk-engine。经 [`TaskCoordinatorPort`] 端口反转注入：
//! 具体实现桥接到数据库权威的 `zk_engine::task::TaskRuntime`，落在 zk-server
//! 组合根；端口自身不持有第二套状态机。

use std::time::Duration;

use crate::task_runtime_v4_generated::{
    TASK_CREATE_ALLOWED_FIELDS, TASK_CREATE_LEGACY_FIELDS, TASK_CREATE_TASK_TYPE_VALUES,
    TASK_GET_ALLOWED_FIELDS, TASK_GET_LEGACY_FIELDS, TASK_LIST_ALLOWED_FIELDS,
    TASK_LIST_LEGACY_FIELDS, TASK_LIST_STATUS_VALUES, TASK_OUTPUT_ALLOWED_FIELDS,
    TASK_OUTPUT_LEGACY_FIELDS, TASK_OUTPUT_MAX_BYTES_DEFAULT, TASK_OUTPUT_MAX_BYTES_MAXIMUM,
    TASK_OUTPUT_MAX_BYTES_MINIMUM, TASK_OUTPUT_RESULT_VERSION_MINIMUM, TASK_OUTPUT_WAIT_MS_DEFAULT,
    TASK_OUTPUT_WAIT_MS_MAXIMUM, TASK_OUTPUT_WAIT_MS_MINIMUM, TASK_STOP_ALLOWED_FIELDS,
    TASK_STOP_LEGACY_FIELDS, TASK_UPDATE_ALLOWED_FIELDS, TASK_UPDATE_LEGACY_FIELDS,
    TASK_UPDATE_REPORTED_PROGRESS_MAXIMUM, TASK_UPDATE_REPORTED_PROGRESS_MINIMUM,
    task_create_input_schema, task_get_input_schema, task_list_input_schema,
    task_output_input_schema, task_runtime_error_response, task_stop_input_schema,
    task_update_input_schema,
};
use crate::tool::{Tool, ToolContext, ToolOutput};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

// ═══ 端口 ═══

/// 任务信息快照（zk-tools 自持，不依赖 zk-engine 的运行时实现）。
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSnapshot {
    /// 任务 ID。
    pub task_id: String,
    /// 会话 ID。
    pub session_id: String,
    /// Parent logical task for attached children.
    pub parent_task_id: Option<String>,
    /// Current execution attempt.
    pub run_id: Option<String>,
    /// 状态字符串。
    pub status: String,
    /// Stable terminal or waiting reason.
    pub reason: Option<String>,
    /// 描述。
    pub description: Option<String>,
    /// 输出。
    pub output: Option<String>,
    /// 错误。
    pub error: Option<String>,
    /// Latest immutable result version.
    pub result_version: Option<i64>,
    /// Whether the latest result is partial.
    pub partial: bool,
    /// Opaque durable result reference. This is never a filesystem path.
    pub result_ref: Option<String>,
    /// Resource cleanup projection.
    pub cleanup_status: String,
    /// Direct/subtree usage projection; incomplete usage remains explicit.
    pub usage_summary: Value,
    /// False for ordinary reads/submissions; `TaskOutput` sets this when its wait expires.
    pub wait_expired: bool,
    /// 创建时间（epoch 毫秒）。
    pub created_at: i64,
    /// 子任务数。
    pub child_count: usize,
}

impl TaskSnapshot {
    fn structured_result(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| {
            json!({
                "taskId": self.task_id,
                "status": self.status,
                "reason": "snapshotSerializationFailed"
            })
        })
    }

    fn render(&self) -> String {
        serde_json::to_string_pretty(&self.structured_result())
            .unwrap_or_else(|_| format!("Task {}: {}", self.task_id, self.status))
    }
}

/// Structured runtime error returned by the server-owned `TaskRuntime` port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskPortError {
    /// Stable machine-readable error code.
    pub code: String,
    /// Safe human-readable detail.
    pub message: String,
    /// Whether retrying without changing the request can succeed.
    pub retryable: bool,
}

impl TaskPortError {
    /// Construct a runtime error.
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
        }
    }

    pub(crate) fn into_output(self) -> ToolOutput {
        let structured = task_runtime_error_response(self.code, self.message, self.retryable);
        ToolOutput {
            content: serde_json::to_string_pretty(&structured)
                .unwrap_or_else(|_| "TASK_RUNTIME_ERROR".to_owned()),
            is_error: true,
            metadata: Some(json!({"structuredResult": structured})),
        }
    }
}

/// Complete parent identity for a durable background task invocation.
#[derive(Clone, Debug)]
pub struct TaskInvocation {
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
    /// Ordinal within the creating tool call. V1 creates exactly one child.
    pub ordinal: u32,
}

/// Non-destructive result read request.
#[derive(Clone, Debug)]
pub struct TaskOutputQuery {
    /// Opaque task identifier.
    pub task_id: String,
    /// Root session used for ownership validation.
    pub session_id: String,
    /// Maximum wait for state change/result availability.
    pub wait_ms: u64,
    /// Optional immutable result version.
    pub result_version: Option<i64>,
    /// Opaque pagination cursor.
    pub cursor: Option<String>,
    /// Maximum result bytes in this page.
    pub max_bytes: usize,
    /// Calling Run cancellation token.
    pub cancel: CancellationToken,
}

/// One page from an immutable `TaskResult`.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskOutputPage {
    /// Current task snapshot.
    #[serde(flatten)]
    pub task: TaskSnapshot,
    /// UTF-8 result content for this page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// Cursor for the next page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Keep the structured contract intact while avoiding duplicate report text in
/// model context. Only remove an exact alias; distinct output must be preserved.
fn task_output_model_text(structured: &Value) -> String {
    let mut model_view = structured.clone();
    if let Some(content) = structured.get("content").and_then(Value::as_str)
        && structured.get("output").and_then(Value::as_str) == Some(content)
        && let Some(object) = model_view.as_object_mut()
    {
        object.remove("output");
    }
    serde_json::to_string_pretty(&model_view).unwrap_or_else(|_| "{}".to_owned())
}

/// Idempotent cancellation acknowledgement returned by `TaskStop`.
///
/// The task snapshot is flattened so the V4 response keeps the common
/// `taskId`/`runId`/`status`/`cleanupStatus` fields at the top level while also
/// reporting whether this invocation actually requested cancellation.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStopReceipt {
    /// True only when this call won the transition into `cancelling`.
    pub cancel_requested: bool,
    /// Current durable task projection.
    #[serde(flatten)]
    pub task: TaskSnapshot,
}

impl TaskStopReceipt {
    fn structured_result(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| {
            json!({
                "taskId": self.task.task_id,
                "status": self.task.status,
                "cleanupStatus": self.task.cleanup_status,
                "cancelRequested": self.cancel_requested,
                "reason": "stopReceiptSerializationFailed"
            })
        })
    }
}

/// 任务协调端口（zk-tools 不依赖 zk-engine）。
///
/// zk-server 组合根装配具体实现，桥接到唯一 `TaskRuntime`。
pub trait TaskCoordinatorPort: Send + Sync {
    /// 提交任务。
    ///
    /// # Errors
    /// 并发任务数超过上限或引擎工厂拒绝时返回错误字符串。
    fn submit_task(
        &self,
        invocation: TaskInvocation,
    ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>>;

    /// 取消任务。
    fn cancel_task(
        &self,
        task_id: String,
        session_id: String,
        reason: String,
    ) -> BoxFuture<'_, Result<TaskStopReceipt, TaskPortError>>;

    /// 查询单个任务。
    fn get_task(
        &self,
        task_id: String,
        session_id: String,
    ) -> BoxFuture<'_, Result<Option<TaskSnapshot>, TaskPortError>>;

    /// 列出任务。
    fn list_tasks(
        &self,
        session_id: String,
        filter_status: Option<String>,
    ) -> BoxFuture<'_, Result<Vec<TaskSnapshot>, TaskPortError>>;

    /// 更新任务状态 / 输出。
    ///
    /// # Errors
    /// 任务不存在时返回错误字符串。
    fn update_task(
        &self,
        task_id: String,
        session_id: String,
        description: Option<String>,
        plan: Option<String>,
        reported_progress: Option<f64>,
    ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>>;

    /// Read or wait for one immutable result page.
    fn read_output(
        &self,
        query: TaskOutputQuery,
    ) -> BoxFuture<'_, Result<TaskOutputPage, TaskPortError>>;
}

fn snapshot_output(snapshot: &TaskSnapshot) -> ToolOutput {
    let structured = snapshot.structured_result();
    ToolOutput {
        content: snapshot.render(),
        is_error: false,
        metadata: Some(json!({"structuredResult": structured})),
    }
}

fn stop_output(receipt: &TaskStopReceipt) -> ToolOutput {
    let structured = receipt.structured_result();
    ToolOutput {
        content: serde_json::to_string_pretty(&structured).unwrap_or_else(|_| "{}".to_owned()),
        is_error: false,
        metadata: Some(json!({"structuredResult": structured})),
    }
}

pub(crate) fn v4_failure(code: &str, message: impl Into<String>) -> ToolOutput {
    TaskPortError::new(code, message, false).into_output()
}

/// Fail closed on malformed or stale tool arguments. JSON Schema is guidance for
/// providers, not an execution-boundary validator, so the runtime must repeat this
/// check before any durable mutation.
pub(crate) fn validate_v4_fields(
    input: &Value,
    allowed: &[&str],
    legacy: &[&str],
) -> Result<(), ToolOutput> {
    let Some(object) = input.as_object() else {
        return Err(v4_failure(
            "INVALID_REQUEST",
            "V4 tool input must be a JSON object",
        ));
    };
    if let Some(field) = legacy.iter().find(|field| object.contains_key(**field)) {
        return Err(v4_failure(
            "LEGACY_ARGUMENT_UNSUPPORTED",
            format!("legacy parameter '{field}' is not supported by the V4 contract"),
        ));
    }
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(v4_failure(
            "UNKNOWN_PARAMETER",
            format!("parameter '{field}' is not part of the V4 contract"),
        ));
    }
    Ok(())
}

pub(crate) fn required_v4_string<'a>(input: &'a Value, key: &str) -> Result<&'a str, ToolOutput> {
    input
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            v4_failure(
                "MISSING_PARAMETER",
                format!("Required parameter '{key}' is missing or not a non-empty string"),
            )
        })
}

pub(crate) fn optional_v4_string<'a>(
    input: &'a Value,
    key: &str,
) -> Result<Option<&'a str>, ToolOutput> {
    let Some(value) = input.get(key) else {
        return Ok(None);
    };
    value
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(Some)
        .ok_or_else(|| {
            v4_failure(
                "INVALID_PARAMETER",
                format!("Optional parameter '{key}' must be a non-empty string when supplied"),
            )
        })
}

pub(crate) fn required_v4_task_id(input: &Value) -> Result<String, ToolOutput> {
    let value = input
        .get("taskId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            v4_failure(
                "MISSING_PARAMETER",
                "Required parameter 'taskId' is missing or not a non-empty string",
            )
        })?;
    let parsed = uuid::Uuid::parse_str(&value).map_err(|_| {
        v4_failure(
            "INVALID_TASK_ID",
            "taskId must be a canonical UUID v4 string",
        )
    })?;
    if parsed.get_version_num() != 4 || parsed.to_string() != value {
        return Err(v4_failure(
            "INVALID_TASK_ID",
            "taskId must be a canonical UUID v4 string",
        ));
    }
    Ok(value)
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
        "Submit one attached child task to the durable TaskRuntime. The returned taskId is \
         immediately queryable and all terminal outcomes have a durable result."
    }

    fn parameters(&self) -> Value {
        task_create_input_schema()
    }

    fn timeout(&self) -> Duration {
        TASK_TIMEOUT
    }

    fn uses_execution_slot(&self) -> bool {
        false
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let port = std::sync::Arc::clone(&self.port);
        Box::pin(async move {
            if let Err(error) = validate_v4_fields(
                &input,
                TASK_CREATE_ALLOWED_FIELDS,
                TASK_CREATE_LEGACY_FIELDS,
            ) {
                return error;
            }
            let description = match required_v4_string(&input, "description") {
                Ok(d) => d.to_owned(),
                Err(e) => return e,
            };
            let prompt = match required_v4_string(&input, "prompt") {
                Ok(p) => p.to_owned(),
                Err(e) => return e,
            };
            let task_type = match required_v4_string(&input, "taskType") {
                Ok(task_type) => task_type.to_owned(),
                Err(error) => return error,
            };
            if !TASK_CREATE_TASK_TYPE_VALUES.contains(&task_type.as_str()) {
                return v4_failure(
                    "UNSUPPORTED_CAPABILITY",
                    format!("taskType '{task_type}' is not available in TaskRuntime v4"),
                );
            }
            let Some(session_id) = ctx.session_id().map(str::to_owned) else {
                return v4_failure("TASK_CONTEXT_INCOMPLETE", "Task requires session id");
            };
            let Some(parent_run_id) = ctx.run_id().map(str::to_owned) else {
                return v4_failure("TASK_CONTEXT_INCOMPLETE", "Task requires parent run id");
            };
            let Some(tool_use_id) = ctx.tool_use_id().map(str::to_owned) else {
                return v4_failure("TASK_CONTEXT_INCOMPLETE", "Task requires tool use id");
            };
            match port
                .submit_task(TaskInvocation {
                    session_id,
                    description: description.clone(),
                    prompt,
                    task_type,
                    parent_run_id,
                    working_directory: ctx.working_dir().to_path_buf(),
                    tool_use_id,
                    ordinal: 0,
                })
                .await
            {
                Ok(snapshot) => snapshot_output(&snapshot),
                Err(error) => error.into_output(),
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
        "Update advisory task description, plan, or reported progress. Execution terminal \
         state is owned exclusively by TaskRuntime."
    }

    fn parameters(&self) -> Value {
        task_update_input_schema()
    }

    fn uses_execution_slot(&self) -> bool {
        false
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let port = std::sync::Arc::clone(&self.port);
        Box::pin(async move {
            if let Err(error) = validate_v4_fields(
                &input,
                TASK_UPDATE_ALLOWED_FIELDS,
                TASK_UPDATE_LEGACY_FIELDS,
            ) {
                return error;
            }
            let task_id = match required_v4_task_id(&input) {
                Ok(task_id) => task_id,
                Err(error) => return error,
            };
            let Some(session_id) = ctx.session_id().map(str::to_owned) else {
                return v4_failure("TASK_CONTEXT_INCOMPLETE", "TaskUpdate requires session id");
            };
            let description = match optional_v4_string(&input, "description") {
                Ok(value) => value.map(String::from),
                Err(error) => return error,
            };
            let plan = match optional_v4_string(&input, "plan") {
                Ok(value) => value.map(String::from),
                Err(error) => return error,
            };
            let reported_progress = match input.get("reportedProgress") {
                None => None,
                Some(value) => match value.as_f64() {
                    Some(value) => Some(value),
                    None => {
                        return v4_failure(
                            "INVALID_REPORTED_PROGRESS",
                            "reportedProgress must be a number between 0 and 1",
                        );
                    }
                },
            };
            if reported_progress.is_some_and(|value| {
                !(TASK_UPDATE_REPORTED_PROGRESS_MINIMUM..=TASK_UPDATE_REPORTED_PROGRESS_MAXIMUM)
                    .contains(&value)
            }) {
                return v4_failure(
                    "INVALID_REPORTED_PROGRESS",
                    "reportedProgress must be between 0 and 1",
                );
            }

            match port
                .update_task(task_id, session_id, description, plan, reported_progress)
                .await
            {
                Ok(snapshot) => snapshot_output(&snapshot),
                Err(error) => error.into_output(),
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
        "List the current root session's durable task tree, optionally filtered by canonical status."
    }

    fn parameters(&self) -> Value {
        task_list_input_schema()
    }

    fn uses_execution_slot(&self) -> bool {
        false
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let port = std::sync::Arc::clone(&self.port);
        Box::pin(async move {
            if let Err(error) =
                validate_v4_fields(&input, TASK_LIST_ALLOWED_FIELDS, TASK_LIST_LEGACY_FIELDS)
            {
                return error;
            }
            let filter = match optional_v4_string(&input, "status") {
                Ok(status) => status,
                Err(error) => return error,
            };
            if filter.is_some_and(|status| !TASK_LIST_STATUS_VALUES.contains(&status)) {
                return v4_failure(
                    "INVALID_TASK_STATUS",
                    "status is not supported by the V4 contract",
                );
            }
            let Some(session_id) = ctx.session_id().map(str::to_owned) else {
                return v4_failure("TASK_CONTEXT_INCOMPLETE", "TaskList requires session id");
            };

            match port.list_tasks(session_id, filter.map(str::to_owned)).await {
                Ok(tasks) => {
                    let structured = json!({"tasks": tasks});
                    ToolOutput {
                        content: serde_json::to_string_pretty(&structured)
                            .unwrap_or_else(|_| "{\"tasks\":[]}".to_owned()),
                        is_error: false,
                        metadata: Some(json!({"structuredResult": structured})),
                    }
                }
                Err(error) => error.into_output(),
            }
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
        task_get_input_schema()
    }

    fn uses_execution_slot(&self) -> bool {
        false
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let port = std::sync::Arc::clone(&self.port);
        Box::pin(async move {
            if let Err(error) =
                validate_v4_fields(&input, TASK_GET_ALLOWED_FIELDS, TASK_GET_LEGACY_FIELDS)
            {
                return error;
            }
            let task_id = match required_v4_task_id(&input) {
                Ok(task_id) => task_id,
                Err(error) => return error,
            };
            let Some(session_id) = ctx.session_id().map(str::to_owned) else {
                return v4_failure("TASK_CONTEXT_INCOMPLETE", "TaskGet requires session id");
            };

            match port.get_task(task_id.clone(), session_id).await {
                Ok(Some(snapshot)) => snapshot_output(&snapshot),
                Ok(None) => v4_failure("TASK_NOT_FOUND", format!("Task not found: {task_id}")),
                Err(error) => error.into_output(),
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
        task_output_input_schema()
    }

    fn uses_execution_slot(&self) -> bool {
        false
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let port = std::sync::Arc::clone(&self.port);
        Box::pin(async move {
            if let Err(error) = validate_v4_fields(
                &input,
                TASK_OUTPUT_ALLOWED_FIELDS,
                TASK_OUTPUT_LEGACY_FIELDS,
            ) {
                return error;
            }
            let task_id = match required_v4_task_id(&input) {
                Ok(task_id) => task_id,
                Err(error) => return error,
            };
            let Some(session_id) = ctx.session_id().map(str::to_owned) else {
                return v4_failure("TASK_CONTEXT_INCOMPLETE", "TaskOutput requires session id");
            };
            let wait_ms = match input.get("waitMs") {
                None => u64::try_from(TASK_OUTPUT_WAIT_MS_DEFAULT)
                    .expect("generated waitMs default must be non-negative"),
                Some(value) => match value.as_u64() {
                    Some(value) => value,
                    None => return v4_failure("INVALID_WAIT_MS", "waitMs must be an integer"),
                },
            };
            let wait_ms_range = u64::try_from(TASK_OUTPUT_WAIT_MS_MINIMUM)
                .expect("generated waitMs minimum must be non-negative")
                ..=u64::try_from(TASK_OUTPUT_WAIT_MS_MAXIMUM)
                    .expect("generated waitMs maximum must be non-negative");
            if !wait_ms_range.contains(&wait_ms) {
                return v4_failure("INVALID_WAIT_MS", "waitMs must be between 0 and 30000");
            }
            let max_bytes = match input.get("maxBytes") {
                None => u64::try_from(TASK_OUTPUT_MAX_BYTES_DEFAULT)
                    .expect("generated maxBytes default must be non-negative"),
                Some(value) => match value.as_u64() {
                    Some(value) => value,
                    None => return v4_failure("INVALID_MAX_BYTES", "maxBytes must be an integer"),
                },
            };
            let max_bytes_range = u64::try_from(TASK_OUTPUT_MAX_BYTES_MINIMUM)
                .expect("generated maxBytes minimum must be non-negative")
                ..=u64::try_from(TASK_OUTPUT_MAX_BYTES_MAXIMUM)
                    .expect("generated maxBytes maximum must be non-negative");
            if !max_bytes_range.contains(&max_bytes) {
                return v4_failure("INVALID_MAX_BYTES", "maxBytes must be between 1 and 65536");
            }
            let result_version = match input.get("resultVersion") {
                None => None,
                Some(value) => match value.as_i64() {
                    Some(value) => Some(value),
                    None => {
                        return v4_failure(
                            "INVALID_RESULT_VERSION",
                            "resultVersion must be an integer",
                        );
                    }
                },
            };
            if result_version.is_some_and(|version| version < TASK_OUTPUT_RESULT_VERSION_MINIMUM) {
                return v4_failure("INVALID_RESULT_VERSION", "resultVersion must be positive");
            }
            let query = TaskOutputQuery {
                task_id,
                session_id,
                wait_ms,
                result_version,
                cursor: match optional_v4_string(&input, "cursor") {
                    Ok(cursor) => cursor.map(str::to_owned),
                    Err(error) => return error,
                },
                max_bytes: usize::try_from(max_bytes).unwrap_or_else(|_| {
                    usize::try_from(TASK_OUTPUT_MAX_BYTES_DEFAULT)
                        .expect("generated maxBytes default must fit usize")
                }),
                cancel: ctx.cancel.clone(),
            };
            match port.read_output(query).await {
                Ok(page) => {
                    let structured = serde_json::to_value(&page).unwrap_or_else(|_| {
                        json!({
                            "code": "TASK_OUTPUT_SERIALIZATION_FAILED"
                        })
                    });
                    ToolOutput {
                        content: task_output_model_text(&structured),
                        is_error: false,
                        metadata: Some(json!({"structuredResult": structured})),
                    }
                }
                Err(error) => error.into_output(),
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
        task_stop_input_schema()
    }

    fn uses_execution_slot(&self) -> bool {
        false
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let port = std::sync::Arc::clone(&self.port);
        Box::pin(async move {
            if let Err(error) =
                validate_v4_fields(&input, TASK_STOP_ALLOWED_FIELDS, TASK_STOP_LEGACY_FIELDS)
            {
                return error;
            }
            let task_id = match required_v4_task_id(&input) {
                Ok(task_id) => task_id,
                Err(error) => return error,
            };
            let Some(session_id) = ctx.session_id().map(str::to_owned) else {
                return v4_failure("TASK_CONTEXT_INCOMPLETE", "TaskStop requires session id");
            };

            match port
                .cancel_task(task_id, session_id, "userCancelled".to_owned())
                .await
            {
                Ok(receipt) => stop_output(&receipt),
                Err(error) => error.into_output(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    const TEST_TASK_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn snapshot(status: &str) -> TaskSnapshot {
        TaskSnapshot {
            task_id: TEST_TASK_ID.into(),
            session_id: "s1".into(),
            parent_task_id: Some("root-task".into()),
            run_id: Some("550e8400-e29b-41d4-a716-446655440001".into()),
            status: status.into(),
            reason: None,
            description: Some("test task".into()),
            output: None,
            error: None,
            result_version: None,
            partial: false,
            result_ref: None,
            cleanup_status: "notRequired".into(),
            usage_summary: json!({"complete": true}),
            wait_expired: false,
            created_at: 0,
            child_count: 0,
        }
    }

    struct StubPort;
    impl TaskCoordinatorPort for StubPort {
        fn submit_task(
            &self,
            _invocation: TaskInvocation,
        ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>> {
            Box::pin(async { Ok(snapshot("queued")) })
        }
        fn cancel_task(
            &self,
            _id: String,
            _session_id: String,
            _reason: String,
        ) -> BoxFuture<'_, Result<TaskStopReceipt, TaskPortError>> {
            Box::pin(async {
                Ok(TaskStopReceipt {
                    cancel_requested: true,
                    task: snapshot("cancelling"),
                })
            })
        }
        fn get_task(
            &self,
            _id: String,
            _session_id: String,
        ) -> BoxFuture<'_, Result<Option<TaskSnapshot>, TaskPortError>> {
            Box::pin(async { Ok(Some(snapshot("running"))) })
        }
        fn list_tasks(
            &self,
            _sid: String,
            _filter: Option<String>,
        ) -> BoxFuture<'_, Result<Vec<TaskSnapshot>, TaskPortError>> {
            Box::pin(async { Ok(vec![snapshot("running")]) })
        }
        fn update_task(
            &self,
            _id: String,
            _session_id: String,
            _description: Option<String>,
            _plan: Option<String>,
            _reported_progress: Option<f64>,
        ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>> {
            Box::pin(async { Ok(snapshot("running")) })
        }
        fn read_output(
            &self,
            _query: TaskOutputQuery,
        ) -> BoxFuture<'_, Result<TaskOutputPage, TaskPortError>> {
            Box::pin(async {
                Ok(TaskOutputPage {
                    task: TaskSnapshot {
                        wait_expired: true,
                        ..snapshot("running")
                    },
                    content: None,
                    next_cursor: None,
                })
            })
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
            .execute(
                json!({"description": "test", "prompt": "do x", "taskType": "agent"}),
                ctx(),
            )
            .await;
        assert!(!output.is_error);
        assert!(
            output
                .content
                .contains("550e8400-e29b-41d4-a716-446655440000")
        );
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
            .execute(
                json!({"taskId": TEST_TASK_ID, "reportedProgress": 0.5}),
                ctx(),
            )
            .await;
        assert!(!output.is_error);
    }

    #[tokio::test]
    async fn task_list_returns_tasks() {
        let tool = TaskListTool::new(std::sync::Arc::new(StubPort));
        let output = tool.execute(json!({}), ctx()).await;
        assert!(!output.is_error);
        assert!(output.content.contains("\"tasks\""));
    }

    #[tokio::test]
    async fn task_get_returns_details() {
        let tool = TaskGetTool::new(std::sync::Arc::new(StubPort));
        let output = tool.execute(json!({"taskId": TEST_TASK_ID}), ctx()).await;
        assert!(!output.is_error);
        assert!(
            output
                .content
                .contains("550e8400-e29b-41d4-a716-446655440000")
        );
        let response = &output.metadata.expect("structured response")["structuredResult"];
        for field in [
            "taskId",
            "runId",
            "parentTaskId",
            "status",
            "reason",
            "resultVersion",
            "partial",
            "resultRef",
            "usageSummary",
            "cleanupStatus",
            "waitExpired",
        ] {
            assert!(
                response.get(field).is_some(),
                "missing common field {field}"
            );
        }
    }

    #[test]
    fn task_output_model_view_deduplicates_only_identical_report_aliases() {
        let structured = json!({
            "content": "unique-report-body",
            "output": "unique-report-body",
            "nextCursor": "4096",
            "resultVersion": 2,
            "partial": true,
            "usageSummary": {"complete":true}
        });
        let text = task_output_model_text(&structured);
        assert_eq!(text.matches("unique-report-body").count(), 1);
        let view: Value = serde_json::from_str(&text).unwrap();
        assert!(view.get("output").is_none());
        for key in [
            "content",
            "nextCursor",
            "resultVersion",
            "partial",
            "usageSummary",
        ] {
            assert_eq!(view[key], structured[key]);
        }
        assert_eq!(structured["output"], "unique-report-body");
        for original in [
            json!({"content":"page 2", "output":"different summary"}),
            json!({"output":"snapshot only"}),
            json!({"content":null,"output":null,"waitExpired":true}),
        ] {
            let view: Value = serde_json::from_str(&task_output_model_text(&original)).unwrap();
            assert_eq!(view, original);
        }
    }

    #[tokio::test]
    async fn task_output_nonblocking_reads_without_mutation() {
        let tool = TaskOutputTool::new(std::sync::Arc::new(StubPort));
        let output = tool
            .execute(json!({"taskId": TEST_TASK_ID, "waitMs": 0}), ctx())
            .await;
        assert!(!output.is_error);
        assert!(output.content.contains("\"waitExpired\": true"));
        let response = &output.metadata.expect("structured response")["structuredResult"];
        assert_eq!(response["taskId"], TEST_TASK_ID);
        assert_eq!(response["waitExpired"], true);
        assert!(response.get("task").is_none(), "V4 response must be flat");
    }

    #[tokio::test]
    async fn v4_tools_reject_legacy_wait_and_stop_arguments() {
        let port = std::sync::Arc::new(StubPort);
        let output = TaskOutputTool::new(port.clone())
            .execute(json!({"taskId": TEST_TASK_ID, "block": true}), ctx())
            .await;
        assert!(output.is_error);
        assert_eq!(
            output.metadata.expect("structured error")["structuredResult"]["code"],
            "LEGACY_ARGUMENT_UNSUPPORTED"
        );

        let stopped = TaskStopTool::new(port)
            .execute(
                json!({"taskId": TEST_TASK_ID, "reason": "legacy free-form reason"}),
                ctx(),
            )
            .await;
        assert!(stopped.is_error);
        assert_eq!(
            stopped.metadata.expect("structured error")["structuredResult"]["code"],
            "LEGACY_ARGUMENT_UNSUPPORTED"
        );
    }

    #[tokio::test]
    async fn task_stop_success() {
        let tool = TaskStopTool::new(std::sync::Arc::new(StubPort));
        let output = tool.execute(json!({"taskId": TEST_TASK_ID}), ctx()).await;
        assert!(!output.is_error);
        let structured = &output.metadata.as_ref().expect("metadata")["structuredResult"];
        assert_eq!(structured["cancelRequested"], true);
        assert_eq!(structured["status"], "cancelling");
        assert_eq!(structured["cleanupStatus"], "notRequired");
        assert!(structured.get("cancel_requested").is_none());
    }

    #[tokio::test]
    async fn task_stop_not_found() {
        struct NotFoundPort;
        impl TaskCoordinatorPort for NotFoundPort {
            fn submit_task(
                &self,
                _: TaskInvocation,
            ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>> {
                Box::pin(async { Ok(snapshot("queued")) })
            }
            fn cancel_task(
                &self,
                _: String,
                _: String,
                _: String,
            ) -> BoxFuture<'_, Result<TaskStopReceipt, TaskPortError>> {
                Box::pin(async { Err(TaskPortError::new("TASK_NOT_FOUND", "not found", false)) })
            }
            fn get_task(
                &self,
                _: String,
                _: String,
            ) -> BoxFuture<'_, Result<Option<TaskSnapshot>, TaskPortError>> {
                Box::pin(async { Ok(None) })
            }
            fn list_tasks(
                &self,
                _: String,
                _: Option<String>,
            ) -> BoxFuture<'_, Result<Vec<TaskSnapshot>, TaskPortError>> {
                Box::pin(async { Ok(vec![]) })
            }
            fn update_task(
                &self,
                _: String,
                _: String,
                _: Option<String>,
                _: Option<String>,
                _: Option<f64>,
            ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>> {
                Box::pin(async { Ok(snapshot("running")) })
            }
            fn read_output(
                &self,
                _: TaskOutputQuery,
            ) -> BoxFuture<'_, Result<TaskOutputPage, TaskPortError>> {
                Box::pin(async { Err(TaskPortError::new("TASK_NOT_FOUND", "not found", false)) })
            }
        }
        let tool = TaskStopTool::new(std::sync::Arc::new(NotFoundPort));
        let output = tool
            .execute(
                json!({"taskId": "00000000-0000-4000-8000-000000000000"}),
                ctx(),
            )
            .await;
        assert!(output.is_error);
        assert!(output.content.contains("TASK_NOT_FOUND"));
    }

    #[tokio::test]
    async fn v4_task_read_and_stop_tools_reject_non_uuid_or_non_v4_ids() {
        let port = std::sync::Arc::new(StubPort);
        for tool in [
            std::sync::Arc::new(TaskGetTool::new(port.clone())) as std::sync::Arc<dyn Tool>,
            std::sync::Arc::new(TaskOutputTool::new(port.clone())) as std::sync::Arc<dyn Tool>,
            std::sync::Arc::new(TaskStopTool::new(port)) as std::sync::Arc<dyn Tool>,
        ] {
            for invalid in [
                "short-id",
                "550e8400-e29b-11d4-a716-446655440000",
                "550E8400-E29B-41D4-A716-446655440000",
            ] {
                let output = tool.execute(json!({"taskId": invalid}), ctx()).await;
                assert!(output.is_error);
                let structured = &output.metadata.expect("structured error")["structuredResult"];
                assert_eq!(structured["code"], "INVALID_TASK_ID");
                assert_eq!(structured["retryable"], false);
                assert!(structured["details"].is_object());
            }
        }
    }
}
