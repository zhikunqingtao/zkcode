//! Durable continuation delivery to an active child agent.

use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::input::required_str;
use crate::{Tool, ToolContext, ToolOutput};

/// Complete identity for one collaboration message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SendMessageInvocation {
    /// Active target agent identifier.
    pub target_agent_id: String,
    /// Bounded continuation text.
    pub message: String,
    /// Parent session authorized to address the child.
    pub parent_session_id: String,
    /// Parent Run that owns queued/consumed events.
    pub parent_run_id: String,
    /// Tool call responsible for the delivery.
    pub tool_use_id: String,
}

/// Server-owned persistence and delivery port.
pub trait SendMessageBackend: Send + Sync {
    /// Persist and route one message.
    fn send_message(&self, invocation: SendMessageInvocation) -> BoxFuture<'_, Result<(), String>>;
}

/// Model-callable child continuation tool.
pub struct SendMessageTool {
    backend: std::sync::Arc<dyn SendMessageBackend>,
}

impl SendMessageTool {
    /// Construct with the production delivery port.
    #[must_use]
    pub fn new(backend: std::sync::Arc<dyn SendMessageBackend>) -> Self {
        Self { backend }
    }
}

impl Tool for SendMessageTool {
    fn name(&self) -> &'static str {
        "SendMessage"
    }

    fn description(&self) -> &'static str {
        "Send a durable follow-up instruction to an active child agent. Terminal or unknown agents fail."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "to": {"type": "string", "description": "Active target agent ID"},
                "message": {"type": "string", "maxLength": 32768}
            },
            "required": ["to", "message"]
        })
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(5)
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let backend = std::sync::Arc::clone(&self.backend);
        Box::pin(async move {
            let target_agent_id = match required_str(&input, "to") {
                Ok(value) => value.trim().to_owned(),
                Err(error) => return error,
            };
            if target_agent_id.is_empty()
                || target_agent_id.len() > 128
                || !target_agent_id
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
            {
                return ToolOutput::error("INVALID_AGENT_ID: target must be path-safe");
            }
            let message = match required_str(&input, "message") {
                Ok(value) => value.to_owned(),
                Err(error) => return error,
            };
            if message.is_empty() || message.len() > 32 * 1024 {
                return ToolOutput::error("INVALID_MESSAGE: message must be 1..32768 bytes");
            }
            let (Some(parent_session_id), Some(parent_run_id), Some(tool_use_id)) =
                (ctx.session_id(), ctx.run_id(), ctx.tool_use_id())
            else {
                return ToolOutput::error(
                    "SEND_MESSAGE_CONTEXT_INCOMPLETE: session, run, and tool-use are required",
                );
            };
            let invocation = SendMessageInvocation {
                target_agent_id: target_agent_id.clone(),
                message,
                parent_session_id: parent_session_id.to_owned(),
                parent_run_id: parent_run_id.to_owned(),
                tool_use_id: tool_use_id.to_owned(),
            };
            match backend.send_message(invocation).await {
                Ok(()) => ToolOutput::ok(format!("Message queued for {target_agent_id}")),
                Err(error) => ToolOutput::error(error),
            }
        })
    }

    fn is_destructive(&self, _input: &Value) -> bool {
        false
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    #[derive(Default)]
    struct RecordingBackend(Mutex<Vec<SendMessageInvocation>>);

    impl SendMessageBackend for RecordingBackend {
        fn send_message(
            &self,
            invocation: SendMessageInvocation,
        ) -> BoxFuture<'_, Result<(), String>> {
            self.0.lock().expect("messages").push(invocation);
            Box::pin(async { Ok(()) })
        }
    }

    fn context() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
            .with_session_id("parent-session")
            .with_run_id("parent-run")
            .with_tool_use_id("tool-use")
    }

    #[tokio::test]
    async fn validates_context_and_routes_complete_identity() {
        let backend = std::sync::Arc::new(RecordingBackend::default());
        let tool = SendMessageTool::new(backend.clone());
        let output = tool
            .execute(json!({"to": "agent-1", "message": "continue"}), context())
            .await;
        assert!(!output.is_error);
        let messages = backend.0.lock().unwrap();
        assert_eq!(messages[0].parent_run_id, "parent-run");
        assert_eq!(messages[0].target_agent_id, "agent-1");
    }

    #[tokio::test]
    async fn rejects_unsafe_target_and_oversized_message() {
        let tool = SendMessageTool::new(std::sync::Arc::new(RecordingBackend::default()));
        assert!(
            tool.execute(json!({"to": "../agent", "message": "x"}), context())
                .await
                .is_error
        );
        assert!(
            tool.execute(
                json!({"to": "agent-1", "message": "x".repeat(32 * 1024 + 1)}),
                context(),
            )
            .await
            .is_error
        );
    }
}
