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

use crate::input::{bool_or, optional_str, required_str};
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
    pub agent_type: Option<String>,
    /// Optional model alias override.
    pub model_override: Option<String>,
    /// Isolation mode (`none` or `worktree`).
    pub isolation: String,
    /// Whether the caller requested background execution.
    pub run_in_background: bool,
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
    /// - `prompt`：任务提示
    /// - `agent_type`：代理类型（explore/verification/plan/general-purpose/guide）
    /// - `model`：模型覆盖
    /// - `isolation`：隔离模式（none/worktree）
    /// - `run_in_background`：是否后台运行
    /// - `session_id`：父会话 ID
    ///
    /// # 返回
    /// `(status_str, result_text, output_file)`
    fn execute_agent(
        &self,
        invocation: AgentInvocation,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, (String, Option<String>, Option<String>)>;
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
        "Launch a sub-agent to work on a specific task independently. \
         The sub-agent has its own conversation with the LLM and can use tools. \
         Use this when a task can be broken down into independent subtasks."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "prompt": {
                    "type": "string",
                    "description": "Complete task description for the sub-agent"
                },
                "description": {
                    "type": "string",
                    "description": "Short 3-5 word description of the task"
                },
                "subagent_type": {
                    "type": "string",
                    "enum": ["explore", "verification", "plan", "general-purpose", "guide"],
                    "description": "Type of sub-agent to use"
                },
                "model": {
                    "type": "string",
                    "description": "Model override for the sub-agent"
                },
                "run_in_background": {
                    "type": "boolean",
                    "description": "Run the agent in the background"
                },
                "isolation": {
                    "type": "string",
                    "enum": ["none", "worktree"],
                    "description": "Isolation mode for the sub-agent"
                }
            },
            "required": ["prompt"]
        })
    }

    fn timeout(&self) -> Duration {
        AGENT_TOOL_TIMEOUT
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let backend = std::sync::Arc::clone(&self.backend);
        Box::pin(async move {
            let prompt = match required_str(&input, "prompt") {
                Ok(p) => p.to_owned(),
                Err(e) => return e,
            };
            let description = optional_str(&input, "description").unwrap_or("sub-agent task");
            let agent_type = optional_str(&input, "subagent_type").map(String::from);
            let model = optional_str(&input, "model").map(String::from);
            let run_in_background = bool_or(&input, "run_in_background", false);
            let isolation = optional_str(&input, "isolation")
                .unwrap_or("none")
                .to_owned();
            let Some(parent_session_id) = ctx.session_id().map(str::to_owned) else {
                return ToolOutput::error(
                    "AGENT_CONTEXT_INCOMPLETE: parent session id is required",
                );
            };
            let Some(parent_run_id) = ctx.run_id().map(str::to_owned) else {
                return ToolOutput::error("AGENT_CONTEXT_INCOMPLETE: parent run id is required");
            };
            let Some(tool_use_id) = ctx.tool_use_id().map(str::to_owned) else {
                return ToolOutput::error("AGENT_CONTEXT_INCOMPLETE: tool use id is required");
            };

            let invocation = AgentInvocation {
                prompt: prompt.clone(),
                description: description.to_owned(),
                agent_type,
                model_override: model,
                isolation,
                run_in_background,
                parent_session_id,
                parent_run_id,
                working_directory: ctx.working_dir().to_path_buf(),
                tool_use_id,
                allowed_tools: None,
            };

            let (status, result_text, output_file) =
                backend.execute_agent(invocation, ctx.cancel.clone()).await;

            match status.as_str() {
                "completed" => ToolOutput::ok(
                    result_text
                        .unwrap_or_else(|| "Sub-agent completed without response.".to_owned()),
                ),
                "timeout" => ToolOutput::error(
                    result_text.unwrap_or_else(|| "Sub-agent timed out.".to_owned()),
                ),
                "async_launched" => {
                    let file = output_file.unwrap_or_default();
                    ToolOutput::ok(format!(
                        "Agent launched in background.\nDescription: {description}\nPrompt: {prompt}\nOutput file: {file}"
                    ))
                }
                "interrupted" => ToolOutput::error(
                    result_text.unwrap_or_else(|| "Sub-agent was interrupted.".to_owned()),
                ),
                "max_turns" => ToolOutput::error(result_text.unwrap_or_else(|| {
                    "Sub-agent reached max turns limit without completing.".to_owned()
                })),
                _ => ToolOutput::error(
                    result_text.unwrap_or_else(|| "Sub-agent execution failed.".to_owned()),
                ),
            }
        })
    }

    fn is_destructive(&self, _input: &Value) -> bool {
        true
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    struct StubBackend;
    impl AgentToolBackend for StubBackend {
        fn execute_agent(
            &self,
            invocation: AgentInvocation,
            _cancel: CancellationToken,
        ) -> BoxFuture<'_, (String, Option<String>, Option<String>)> {
            let prompt = invocation.prompt;
            Box::pin(async move {
                (
                    "completed".to_owned(),
                    Some(format!("result for: {prompt}")),
                    None,
                )
            })
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
    async fn execute_missing_prompt() {
        let tool = AgentTool::new(std::sync::Arc::new(StubBackend));
        let output = tool.execute(json!({}), ctx()).await;
        assert!(output.is_error);
        assert!(output.content.starts_with("MISSING_PARAMETER"));
    }

    #[tokio::test]
    async fn execute_timeout() {
        struct TimeoutBackend;
        impl AgentToolBackend for TimeoutBackend {
            fn execute_agent(
                &self,
                _: AgentInvocation,
                _cancel: CancellationToken,
            ) -> BoxFuture<'_, (String, Option<String>, Option<String>)> {
                Box::pin(async { ("timeout".to_owned(), Some("timed out".into()), None) })
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
        assert!(output.content.starts_with("AGENT_CONTEXT_INCOMPLETE:"));
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
        let required = params["required"].as_array().expect("array");
        assert_eq!(required.len(), 1);
        assert_eq!(required[0], "prompt");
    }
}
