//! Background Bash is an attached durable Shell Task, never a bare detached PID.
use super::BashTool;
use crate::input::{failure, required_str};
use crate::tool::{Tool, ToolContext, ToolOutput};
use crate::{TaskInvocation, TaskPortError, TaskSnapshot};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::Arc;

/// Host-owned bridge to the same task authority used by `TaskCreate`.
pub trait BackgroundShellPort: Send + Sync {
    /// Persist ownership before scheduling any command.
    fn submit(
        &self,
        invocation: TaskInvocation,
    ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>>;
}

/// Native Bash enriched with a host-injected background task adapter.
pub struct BackgroundBashTool {
    port: Arc<dyn BackgroundShellPort>,
}
impl BackgroundBashTool {
    pub(super) fn new(port: Arc<dyn BackgroundShellPort>) -> Self {
        Self { port }
    }
}
impl Tool for BackgroundBashTool {
    fn name(&self) -> &'static str {
        "Bash"
    }
    fn description(&self) -> &'static str {
        "Execute a shell command. is_background=true creates an attached managed Shell Task with captured output, a bounded deadline and TaskOutput/TaskStop controls. It never detaches from the owning run."
    }
    fn child_view(&self) -> Option<Arc<dyn Tool>> {
        Some(Arc::new(BashTool))
    }
    fn produces_declared_artifacts(&self) -> bool {
        true
    }
    fn parameters(&self) -> Value {
        BashTool.parameters()
    }
    fn timeout(&self) -> std::time::Duration {
        BashTool.timeout()
    }
    fn is_read_only(&self, input: &Value) -> bool {
        BashTool.is_read_only(input)
    }
    fn is_destructive(&self, input: &Value) -> bool {
        BashTool.is_destructive(input)
    }
    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            if input.get("is_background").and_then(Value::as_bool) != Some(true) {
                return BashTool.execute(input, ctx).await;
            }
            if input
                .get("declared_outputs")
                .is_some_and(|value| value.as_array().is_none_or(|items| !items.is_empty()))
            {
                return failure(
                    "BASH_BACKGROUND_OUTPUTS_UNSUPPORTED",
                    "Background commands cannot declare artifacts; use foreground Bash for sealed outputs",
                );
            }
            let command = match required_str(&input, "command") {
                Ok(value) => value.to_owned(),
                Err(error) => return error,
            };
            let (Some(session), Some(run), Some(tool_use)) =
                (ctx.session_id(), ctx.run_id(), ctx.tool_use_id())
            else {
                return failure(
                    "TASK_CONTEXT_INCOMPLETE",
                    "Background Bash requires session, run and tool-use ownership",
                );
            };
            if ctx.cancel.is_cancelled() {
                return failure("TASK_SUBMISSION_CANCELLED", "The owning run was cancelled");
            }
            let description = input
                .get("description")
                .and_then(Value::as_str)
                .filter(|text| !text.trim().is_empty())
                .unwrap_or("Background shell command")
                .to_owned();
            let timeout = super::resolve_timeout(&input, &command);
            let invocation = TaskInvocation {
                session_id: session.to_owned(),
                description,
                prompt: command.clone(),
                task_type: "shell".into(),
                lifecycle: "attached".into(),
                command: Some(command),
                timeout_ms: Some(timeout),
                authorized_shell_cwd: ctx.authorized_shell_cwd().map(std::path::Path::to_path_buf),
                parent_run_id: run.to_owned(),
                working_directory: ctx.working_dir().to_owned(),
                tool_use_id: tool_use.to_owned(),
                ordinal: 0,
            };
            match self.port.submit(invocation).await {
                Ok(snapshot) => ToolOutput {
                    content: snapshot.render(),
                    is_error: false,
                    metadata: Some(json!({"structuredResult":snapshot.structured_result()})),
                },
                Err(error) => error.into_output(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    struct RecordingPort(Mutex<Vec<TaskInvocation>>);
    impl BackgroundShellPort for RecordingPort {
        fn submit(
            &self,
            invocation: TaskInvocation,
        ) -> BoxFuture<'_, Result<TaskSnapshot, TaskPortError>> {
            self.0.lock().unwrap().push(invocation);
            Box::pin(async { Err(TaskPortError::new("TEST_SUBMITTED", "recorded", false)) })
        }
    }
    fn context() -> ToolContext {
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        ToolContext::new(tokio_util::sync::CancellationToken::new(), tx)
            .with_session_id("session")
            .with_run_id("run")
            .with_tool_use_id("tool-use")
            .with_working_dir("/tmp")
    }
    #[tokio::test]
    async fn background_uses_attached_task_identity_and_requested_deadline() {
        let port = Arc::new(RecordingPort(Mutex::new(Vec::new())));
        let tool = BackgroundBashTool::new(port.clone());
        let output = tool
            .execute(
                json!({"command":"printf 'literal $value'","is_background":true,"timeout":7000}),
                context(),
            )
            .await;
        assert!(output.content.contains("TEST_SUBMITTED"));
        let requests = port.0.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.lifecycle, "attached");
        assert_eq!(request.task_type, "shell");
        assert_eq!(request.session_id, "session");
        assert_eq!(request.parent_run_id, "run");
        assert_eq!(request.tool_use_id, "tool-use");
        assert_eq!(request.command.as_deref(), Some("printf 'literal $value'"));
        assert_eq!(request.timeout_ms, Some(7000));
    }
    #[tokio::test]
    async fn ambiguous_background_output_and_cancelled_owner_never_submit() {
        let port = Arc::new(RecordingPort(Mutex::new(Vec::new())));
        let tool = BackgroundBashTool::new(port.clone());
        let output=tool.execute(json!({"command":"touch dangerous","is_background":true,"declared_outputs":[{"path":"dangerous","operation":"created"}]}),context()).await;
        assert!(
            output
                .content
                .contains("BASH_BACKGROUND_OUTPUTS_UNSUPPORTED")
        );
        let ctx = context();
        ctx.cancel.cancel();
        let output = tool
            .execute(
                json!({"command":"touch dangerous","is_background":true}),
                ctx,
            )
            .await;
        assert!(output.content.contains("TASK_SUBMISSION_CANCELLED"));
        assert!(port.0.lock().unwrap().is_empty());
    }
}
