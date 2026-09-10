//! Durable continuation delivery to an active child agent.

use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::task_runtime_v4_generated::{
    SEND_MESSAGE_ALLOWED_FIELDS, SEND_MESSAGE_LEGACY_FIELDS, SEND_MESSAGE_MESSAGE_MAX_LENGTH,
    send_message_input_schema,
};
use crate::task_tools::{TaskPortError, required_v4_task_id, validate_v4_fields};
use crate::{Tool, ToolContext, ToolOutput};

/// Complete identity for one collaboration message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SendMessageInvocation {
    /// Durable target task identifier.
    pub target_task_id: String,
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
    fn send_message(
        &self,
        invocation: SendMessageInvocation,
    ) -> BoxFuture<'_, Result<SendMessageReceipt, TaskPortError>>;
}

/// Durable inbox write acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageReceipt {
    /// Persisted inbox message identifier. Terminal targets reject the write and
    /// therefore return `null` rather than inventing an ID.
    pub message_id: Option<String>,
    /// queued, delivered, or terminal.
    pub delivery_status: String,
    /// Current canonical target task status.
    pub status: String,
}

fn send_message_error(code: &str, message: impl Into<String>) -> ToolOutput {
    TaskPortError::new(code, message, false).into_output()
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
        "Persist a follow-up instruction in an attached child task inbox. Delivery occurs at the \
         target Run's next safe boundary; terminal tasks return an explicit terminal status."
    }

    fn parameters(&self) -> Value {
        send_message_input_schema()
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(5)
    }

    fn uses_execution_slot(&self) -> bool {
        false
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        let backend = std::sync::Arc::clone(&self.backend);
        Box::pin(async move {
            if let Err(error) = validate_v4_fields(
                &input,
                SEND_MESSAGE_ALLOWED_FIELDS,
                SEND_MESSAGE_LEGACY_FIELDS,
            ) {
                return error;
            }
            let target_task_id = match required_v4_task_id(&input) {
                Ok(task_id) => task_id,
                Err(error) => return error,
            };
            let Some(message) = input
                .get("message")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
            else {
                return send_message_error(
                    "MISSING_PARAMETER",
                    "Required parameter 'message' is missing or not a non-empty string",
                );
            };
            if message.len() > SEND_MESSAGE_MESSAGE_MAX_LENGTH {
                return send_message_error(
                    "INVALID_MESSAGE",
                    "message must be between 1 and 32768 bytes",
                );
            }
            let (Some(parent_session_id), Some(parent_run_id), Some(tool_use_id)) =
                (ctx.session_id(), ctx.run_id(), ctx.tool_use_id())
            else {
                return send_message_error(
                    "SEND_MESSAGE_CONTEXT_INCOMPLETE",
                    "session, run, and tool-use are required",
                );
            };
            let invocation = SendMessageInvocation {
                target_task_id,
                message,
                parent_session_id: parent_session_id.to_owned(),
                parent_run_id: parent_run_id.to_owned(),
                tool_use_id: tool_use_id.to_owned(),
            };
            match backend.send_message(invocation).await {
                Ok(receipt) => {
                    let structured = serde_json::to_value(&receipt).unwrap_or_else(|_| json!({}));
                    ToolOutput {
                        content: serde_json::to_string_pretty(&structured)
                            .unwrap_or_else(|_| "{}".to_owned()),
                        is_error: false,
                        metadata: Some(json!({"structuredResult": structured})),
                    }
                }
                Err(error) => error.into_output(),
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

    const TEST_TASK_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[derive(Default)]
    struct RecordingBackend(Mutex<Vec<SendMessageInvocation>>);

    impl SendMessageBackend for RecordingBackend {
        fn send_message(
            &self,
            invocation: SendMessageInvocation,
        ) -> BoxFuture<'_, Result<SendMessageReceipt, TaskPortError>> {
            self.0.lock().expect("messages").push(invocation);
            Box::pin(async {
                Ok(SendMessageReceipt {
                    message_id: Some("message-1".into()),
                    delivery_status: "queued".into(),
                    status: "running".into(),
                })
            })
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
            .execute(
                json!({"taskId": TEST_TASK_ID, "message": "continue"}),
                context(),
            )
            .await;
        assert!(!output.is_error);
        let messages = backend.0.lock().unwrap();
        assert_eq!(messages[0].parent_run_id, "parent-run");
        assert_eq!(messages[0].target_task_id, TEST_TASK_ID);
    }

    #[tokio::test]
    async fn rejects_unsafe_target_and_oversized_message() {
        let tool = SendMessageTool::new(std::sync::Arc::new(RecordingBackend::default()));
        assert!(
            tool.execute(json!({"taskId": "../task", "message": "x"}), context())
                .await
                .is_error
        );
        assert!(
            tool.execute(
                json!({"taskId": TEST_TASK_ID, "message": "x".repeat(32 * 1024 + 1)}),
                context(),
            )
            .await
            .is_error
        );
    }

    #[tokio::test]
    async fn requires_canonical_uuid_v4_task_id_and_emits_lower_camel_receipt() {
        let tool = SendMessageTool::new(std::sync::Arc::new(RecordingBackend::default()));
        let invalid = tool
            .execute(
                json!({
                    "taskId": "550e8400-e29b-11d4-a716-446655440000",
                    "message": "continue"
                }),
                context(),
            )
            .await;
        assert!(invalid.is_error);
        let error = &invalid.metadata.expect("structured error")["structuredResult"];
        assert_eq!(error["code"], "INVALID_TASK_ID");

        let output = tool
            .execute(
                json!({"taskId": TEST_TASK_ID, "message": "continue"}),
                context(),
            )
            .await;
        let receipt = &output.metadata.expect("structured receipt")["structuredResult"];
        assert_eq!(receipt["messageId"], "message-1");
        assert_eq!(receipt["deliveryStatus"], "queued");
        assert!(receipt.get("message_id").is_none());
        assert!(receipt.get("delivery_status").is_none());
    }

    #[tokio::test]
    async fn rejects_legacy_to_argument_instead_of_losing_the_message() {
        let tool = SendMessageTool::new(std::sync::Arc::new(RecordingBackend::default()));
        let output = tool
            .execute(
                json!({"to": TEST_TASK_ID, "message": "continue"}),
                context(),
            )
            .await;
        assert!(output.is_error);
        assert_eq!(
            output.metadata.expect("structured error")["structuredResult"]["code"],
            "LEGACY_ARGUMENT_UNSUPPORTED"
        );
        assert_eq!(tool.parameters()["additionalProperties"], false);
    }
}
