//! AgentTool——子代理系统核心工具，对照旧 `AgentTool.java`（271L）。
//!
//! 创建子代理执行独立任务。子代理继承父代理权限，但工具集受限
//! （禁止 Agent/Task 工具防止递归）。
//!
//! # 依赖方向
//!
//! zk-tools 不依赖 zk-engine。经 [`AgentToolBackend`] 端口反转注入：
//! 具体实现（桥接到 `SubAgentExecutor`）落 zk-server 组合根。

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use crate::input::optional_str;
use crate::task_runtime_v4_generated::{
    AGENT_ALLOWED_FIELDS, AGENT_ISOLATION_DEFAULT, AGENT_ISOLATION_VALUES, AGENT_LEGACY_FIELDS,
    AGENT_SUBAGENT_TYPE_VALUES, AGENT_WAIT_MODE_DEFAULT, AGENT_WAIT_MODE_VALUES,
    agent_input_schema, task_runtime_error_response,
};
use crate::task_tools::{
    TaskPortError, TaskSnapshot, optional_v4_string, required_v4_string, v4_failure,
    validate_v4_fields,
};
use crate::tool::{Tool, ToolContext, ToolOutput};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

/// Complete parent invocation identity passed across the zk-tools/engine boundary.
/// No field is inferred from process-global state by the production backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentInvocation {
    /// Complete task prompt.
    pub prompt: String,
    /// Short task description used in UI/background receipts.
    pub description: String,
    /// Requested child agent specialization.
    pub subagent_type: Option<String>,
    /// Optional model alias override.
    pub model_override: Option<String>,
    /// Isolation mode (`readOnly`, `worktree`, or gated `sharedWorkspace`).
    pub isolation: String,
    /// Whether the caller waits for a terminal result or receives a durable background handle.
    pub wait_mode: String,
    /// Authoritative parent session identifier.
    pub parent_session_id: String,
    /// Authoritative parent run identifier.
    pub parent_run_id: String,
    /// Authorized workspace inherited from the parent session.
    pub working_directory: PathBuf,
    /// Parent Agent tool-use identifier.
    pub tool_use_id: String,
    /// Optional caller policy that can only narrow the production child tool pool.
    pub allowed_tools: Option<BTreeSet<String>>,
}

/// 子代理执行后端端口（zk-tools 不依赖 zk-engine）。
///
/// zk-server 组合根装配具体实现：桥接到
/// `zk_engine::agent::SubAgentExecutor::execute_sync`。
pub trait AgentToolBackend: Send + Sync {
    /// 执行子代理。
    ///
    /// # 参数
    /// The invocation contains only the V4 contract: durable parent identity,
    /// `waitMode`, and fail-closed `isolation`. Legacy background flags are rejected
    /// before this port is called.
    ///
    /// # 返回
    /// A DB-authoritative task snapshot. Submission success always includes durable IDs.
    fn execute_agent(
        &self,
        invocation: AgentInvocation,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>>;
}

/// 子代理工具——创建子代理执行独立任务。
pub struct AgentTool {
    backend: std::sync::Arc<dyn AgentToolBackend>,
}

impl AgentTool {
    /// 构造工具（注入后端端口）。
    #[must_use]
    pub fn new(backend: std::sync::Arc<dyn AgentToolBackend>) -> Self {
        Self { backend }
    }
}

/// 子代理超时（30 分钟，对照旧 `getMaxExecutionTimeMs = 1_800_000L`）。
const AGENT_TOOL_TIMEOUT: Duration = Duration::from_mins(30);

impl Tool for AgentTool {
    fn name(&self) -> &'static str {
        "Agent"
    }

    fn description(&self) -> &'static str {
        "Submit one attached child Agent through the durable TaskRuntime. Both terminal and \
         background modes return immediately-queryable taskId/runId values; readOnly isolation \
         is the safe default. A timed-out child can return partial evidence: synthesize available \
         results with their limitations instead of automatically repeating the whole task."
    }

    fn parameters(&self) -> Value {
        agent_input_schema()
    }

    fn timeout(&self) -> Duration {
        AGENT_TOOL_TIMEOUT
    }

    fn timeout_policy(&self) -> crate::tool::ToolTimeoutPolicy {
        crate::tool::ToolTimeoutPolicy::TaskRuntime
    }

    fn uses_execution_slot(&self) -> bool {
        false
    }

    #[allow(clippy::too_many_lines)] // validates the complete public V4 Agent contract in one gate
    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let backend = std::sync::Arc::clone(&self.backend);
        Box::pin(async move {
            if let Err(error) =
                validate_v4_fields(&input, AGENT_ALLOWED_FIELDS, AGENT_LEGACY_FIELDS)
            {
                return error;
            }
            let prompt = match required_v4_string(&input, "prompt") {
                Ok(p) => p.to_owned(),
                Err(e) => return e,
            };
            let description = match optional_v4_string(&input, "description") {
                Ok(description) => description.unwrap_or("sub-agent task"),
                Err(error) => return error,
            };
            let subagent_type = match optional_v4_string(&input, "subagentType") {
                Ok(subagent_type) => subagent_type.map(String::from),
                Err(error) => return error,
            };
            if subagent_type
                .as_deref()
                .is_some_and(|agent_type| !AGENT_SUBAGENT_TYPE_VALUES.contains(&agent_type))
            {
                return v4_failure(
                    "INVALID_SUBAGENT_TYPE",
                    "subagentType is not supported by the V4 contract",
                );
            }
            let model = match optional_v4_string(&input, "model") {
                Ok(model) => model.map(String::from),
                Err(error) => return error,
            };
            let wait_mode = match optional_v4_string(&input, "waitMode") {
                Ok(wait_mode) => wait_mode.unwrap_or(AGENT_WAIT_MODE_DEFAULT).to_owned(),
                Err(error) => return error,
            };
            if !AGENT_WAIT_MODE_VALUES.contains(&wait_mode.as_str()) {
                return v4_failure("INVALID_WAIT_MODE", "expected terminal or background");
            }
            let isolation = match optional_v4_string(&input, "isolation") {
                Ok(isolation) => isolation.unwrap_or(AGENT_ISOLATION_DEFAULT).to_owned(),
                Err(error) => return error,
            };
            if !AGENT_ISOLATION_VALUES.contains(&isolation.as_str()) {
                return v4_failure(
                    "INVALID_ISOLATION",
                    "expected readOnly, worktree, or sharedWorkspace",
                );
            }
            let Some(parent_session_id) = ctx.session_id().map(str::to_owned) else {
                return v4_failure("AGENT_CONTEXT_INCOMPLETE", "parent session id is required");
            };
            let Some(parent_run_id) = ctx.run_id().map(str::to_owned) else {
                return v4_failure("AGENT_CONTEXT_INCOMPLETE", "parent run id is required");
            };
            let Some(tool_use_id) = ctx.tool_use_id().map(str::to_owned) else {
                return v4_failure("AGENT_CONTEXT_INCOMPLETE", "tool use id is required");
            };

            // The child may only receive capabilities that were visible in the
            // exact, already-authorized parent directory snapshot for this tool
            // invocation.  An absent catalog is fail-closed rather than meaning
            // "all child defaults": production always injects the snapshot, and
            // direct/test callers must opt capabilities in explicitly.
            let allowed_tools = ctx
                .tool_catalog()
                .unwrap_or_default()
                .iter()
                .map(|spec| spec.name.clone())
                .collect::<BTreeSet<_>>();

            let invocation = AgentInvocation {
                prompt: prompt.clone(),
                description: description.to_owned(),
                subagent_type,
                model_override: model,
                isolation,
                wait_mode,
                parent_session_id,
                parent_run_id,
                working_directory: ctx.working_dir().to_path_buf(),
                tool_use_id,
                allowed_tools: Some(allowed_tools),
            };

            match backend.execute_agent(invocation, ctx.cancel.clone()).await {
                Ok(snapshot) => {
                    let structured = serde_json::to_value(&snapshot).unwrap_or_else(|_| {
                        json!({
                            "taskId": snapshot.task_id,
                            "status": snapshot.status,
                        })
                    });
                    ToolOutput {
                        content: serde_json::to_string_pretty(&structured)
                            .unwrap_or_else(|_| "{}".to_owned()),
                        is_error: false,
                        metadata: Some(json!({"structuredResult": structured})),
                    }
                }
                Err(error) => {
                    let structured =
                        task_runtime_error_response(error.code, error.message, error.retryable);
                    ToolOutput {
                        content: serde_json::to_string_pretty(&structured)
                            .unwrap_or_else(|_| "AGENT_RUNTIME_ERROR".to_owned()),
                        is_error: true,
                        metadata: Some(json!({"structuredResult": structured})),
                    }
                }
            }
        })
    }

    fn is_destructive(&self, input: &Value) -> bool {
        optional_str(input, "isolation").unwrap_or(AGENT_ISOLATION_DEFAULT)
            != AGENT_ISOLATION_DEFAULT
    }

    fn is_read_only(&self, input: &Value) -> bool {
        !self.is_destructive(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    fn task_snapshot(status: &str) -> TaskSnapshot {
        TaskSnapshot {
            task_id: "550e8400-e29b-41d4-a716-446655440000".into(),
            session_id: "internal-session".into(),
            parent_task_id: Some("root-task".into()),
            run_id: Some("550e8400-e29b-41d4-a716-446655440001".into()),
            status: status.into(),
            reason: None,
            description: Some("agent test".into()),
            output: None,
            error: None,
            result_version: Some(1),
            partial: false,
            result_ref: Some("result:550e8400-e29b-41d4-a716-446655440000:1".into()),
            cleanup_status: "confirmed".into(),
            usage_summary: json!({"complete": true}),
            wait_expired: false,
            created_at: 0,
            child_count: 0,
        }
    }

    struct StubBackend;
    impl AgentToolBackend for StubBackend {
        fn execute_agent(
            &self,
            invocation: AgentInvocation,
            _cancel: CancellationToken,
        ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>> {
            let mut task = task_snapshot("succeeded");
            task.output = Some(format!("result for: {}", invocation.prompt));
            Box::pin(async move { Ok(task) })
        }
    }

    #[derive(Default)]
    struct RecordingBackend(Mutex<Option<AgentInvocation>>);

    impl AgentToolBackend for RecordingBackend {
        fn execute_agent(
            &self,
            invocation: AgentInvocation,
            _cancel: CancellationToken,
        ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>> {
            *self.0.lock().expect("invocation") = Some(invocation);
            Box::pin(async { Ok(task_snapshot("succeeded")) })
        }
    }

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
            .with_session_id("parent-session")
            .with_run_id("parent-run")
            .with_tool_use_id("tool-use-1")
            .with_working_dir("/tmp/zkcode-agent-tool")
    }

    #[tokio::test]
    async fn execute_success() {
        let tool = AgentTool::new(std::sync::Arc::new(StubBackend));
        let input = json!({"prompt": "do stuff"});
        let output = tool.execute(input, ctx()).await;
        assert!(!output.is_error);
        assert!(output.content.contains("result for: do stuff"));
    }

    #[tokio::test]
    async fn child_capabilities_are_exactly_the_parent_directory_snapshot() {
        let backend = std::sync::Arc::new(RecordingBackend::default());
        let tool = AgentTool::new(backend.clone());
        let catalog = vec![
            crate::tool::ToolSpec {
                name: "Agent".to_owned(),
                description: String::new(),
                parameters: json!({}),
            },
            crate::tool::ToolSpec {
                name: "Grep".to_owned(),
                description: String::new(),
                parameters: json!({}),
            },
        ];
        let output = tool
            .execute(
                json!({"prompt": "inspect"}),
                ctx().with_tool_catalog(std::sync::Arc::new(catalog)),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        let invocation = backend.0.lock().expect("invocation").clone().unwrap();
        assert_eq!(
            invocation.allowed_tools,
            Some(BTreeSet::from(["Agent".to_owned(), "Grep".to_owned()]))
        );

        let output = tool.execute(json!({"prompt": "inspect"}), ctx()).await;
        assert!(!output.is_error, "{}", output.content);
        assert_eq!(
            backend
                .0
                .lock()
                .expect("invocation")
                .as_ref()
                .unwrap()
                .allowed_tools,
            Some(BTreeSet::new()),
            "missing parent directory must fail closed"
        );
    }

    #[tokio::test]
    async fn execute_missing_prompt() {
        let tool = AgentTool::new(std::sync::Arc::new(StubBackend));
        let output = tool.execute(json!({}), ctx()).await;
        assert!(output.is_error);
        assert_eq!(
            output.metadata.expect("structured error")["structuredResult"]["code"],
            "MISSING_PARAMETER"
        );
    }

    #[tokio::test]
    async fn legacy_background_write_entry_is_rejected() {
        let tool = AgentTool::new(std::sync::Arc::new(StubBackend));
        for input in [
            json!({"prompt": "x", "run_in_background": true}),
            json!({"prompt": "x", "runInBackground": true}),
        ] {
            let output = tool.execute(input, ctx()).await;
            assert!(output.is_error);
            assert_eq!(
                output.metadata.expect("structured error")["structuredResult"]["code"],
                "LEGACY_ARGUMENT_UNSUPPORTED"
            );
        }
    }

    #[tokio::test]
    async fn execute_timeout() {
        struct TimeoutBackend;
        impl AgentToolBackend for TimeoutBackend {
            fn execute_agent(
                &self,
                _: AgentInvocation,
                _cancel: CancellationToken,
            ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>> {
                Box::pin(async { Err(TaskPortError::new("AGENT_TIMEOUT", "timed out", false)) })
            }
        }
        let tool = AgentTool::new(std::sync::Arc::new(TimeoutBackend));
        let output = tool.execute(json!({"prompt": "x"}), ctx()).await;
        assert!(output.is_error);
        assert!(output.content.contains("timed out"));
    }

    #[tokio::test]
    async fn incomplete_parent_context_fails_closed() {
        let tool = AgentTool::new(std::sync::Arc::new(StubBackend));
        let (tx, _rx) = mpsc::unbounded_channel();
        let output = tool
            .execute(
                json!({"prompt": "x"}),
                ToolContext::new(CancellationToken::new(), tx),
            )
            .await;
        assert!(output.is_error);
        assert_eq!(
            output.metadata.expect("structured error")["structuredResult"]["code"],
            "AGENT_CONTEXT_INCOMPLETE"
        );
    }

    #[test]
    fn name_and_description() {
        let tool = AgentTool::new(std::sync::Arc::new(StubBackend));
        assert_eq!(tool.name(), "Agent");
        assert!(!tool.description().is_empty());
    }

    #[test]
    fn parameters_has_prompt_required() {
        let tool = AgentTool::new(std::sync::Arc::new(StubBackend));
        let params = tool.parameters();
        assert_eq!(params["type"], "object");
        assert_eq!(params["additionalProperties"], false);
        let required = params["required"].as_array().expect("array");
        assert_eq!(required.len(), 1);
        assert_eq!(required[0], "prompt");
    }
}
