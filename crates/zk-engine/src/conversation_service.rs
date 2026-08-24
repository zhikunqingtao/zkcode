//! Transport-neutral conversation execution over the production [`crate::Engine`].

use std::sync::Arc;

use serde::Serialize;
use zk_db::{MessageRole, StoredBlock};
use zk_protocol::Usage;

use crate::engine::ConversationRunOptions;
use crate::{Engine, usage_cost_usd};

/// One tool call observed in the durable message sequence for a query.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationToolCall {
    /// Tool name.
    pub tool: String,
    /// Frozen tool input.
    pub input: serde_json::Value,
    /// Durable tool result, if the engine produced one.
    pub output: Option<String>,
    /// Whether the tool result was an error.
    pub is_error: bool,
}

/// Collected terminal projection shared by REST, SSE and CLI adapters.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationOutcome {
    /// Authorized session identifier.
    pub session_id: String,
    /// Concatenated terminal assistant text.
    pub result: String,
    /// Usage produced after this invocation began.
    pub usage: Usage,
    /// Estimated invocation cost.
    pub cost_usd: f64,
    /// Tool calls and their durable results.
    pub tool_calls: Vec<ConversationToolCall>,
    /// Last assistant stop reason.
    pub stop_reason: Option<String>,
    /// Stable execution error, if no terminal conversation could be loaded.
    pub error: Option<String>,
}

/// Shared business service; transports only select how the outcome is encoded.
pub struct ConversationService {
    engine: Arc<Engine>,
    db: zk_db::Db,
}

impl ConversationService {
    /// Bind the service to the same Engine and DB used by the WebSocket path.
    #[must_use]
    pub fn new(engine: Arc<Engine>, db: zk_db::Db) -> Self {
        Self { engine, db }
    }

    /// Execute a complete user turn and collect its durable result.
    pub async fn execute(&self, session_id: &str, prompt: String) -> ConversationOutcome {
        self.execute_with_options(session_id, prompt, ConversationRunOptions::default())
            .await
    }

    /// Execute with transport-scoped turn, prompt and tool limits.
    pub async fn execute_with_options(
        &self,
        session_id: &str,
        prompt: String,
        options: ConversationRunOptions,
    ) -> ConversationOutcome {
        let _options_guard = self
            .engine
            .install_conversation_options(session_id, options);
        let before_seq = self
            .db
            .get_session(session_id)
            .await
            .ok()
            .flatten()
            .and_then(|detail| detail.messages.last().map(|message| message.seq_num))
            .unwrap_or(0);
        Arc::clone(&self.engine)
            .run_user_message(session_id.to_owned(), prompt)
            .await;
        let Ok(Some(detail)) = self.db.get_session(session_id).await else {
            return ConversationOutcome {
                session_id: session_id.to_owned(),
                result: String::new(),
                usage: Usage::default(),
                cost_usd: 0.0,
                tool_calls: Vec::new(),
                stop_reason: None,
                error: Some("QUERY_RESULT_UNAVAILABLE".to_owned()),
            };
        };
        let messages: Vec<_> = detail
            .messages
            .iter()
            .filter(|message| message.seq_num > before_seq)
            .collect();
        let usage = messages.iter().fold(Usage::default(), |usage, message| {
            usage
                + Usage {
                    input_tokens: message.input_tokens,
                    output_tokens: message.output_tokens,
                    ..Usage::default()
                }
        });
        let mut result = String::new();
        let mut stop_reason = None;
        let mut tool_calls = Vec::new();
        for message in &messages {
            if message.role == MessageRole::Assistant {
                stop_reason.clone_from(&message.stop_reason);
                for block in &message.content {
                    match block {
                        StoredBlock::Text { text } => result.push_str(text),
                        StoredBlock::ToolUse { id, name, input } => {
                            let tool_result = messages
                                .iter()
                                .flat_map(|item| &item.content)
                                .find_map(|candidate| match candidate {
                                    StoredBlock::ToolResult {
                                        tool_use_id,
                                        content,
                                        is_error,
                                        ..
                                    } if tool_use_id == id => Some((content.clone(), *is_error)),
                                    _ => None,
                                });
                            tool_calls.push(ConversationToolCall {
                                tool: name.clone(),
                                input: input.clone(),
                                output: tool_result.as_ref().map(|(output, _)| output.clone()),
                                is_error: tool_result.is_some_and(|(_, is_error)| is_error),
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
        let cost_usd = usage_cost_usd(&detail.model, &usage);
        ConversationOutcome {
            session_id: session_id.to_owned(),
            result,
            usage,
            cost_usd,
            tool_calls,
            stop_reason,
            error: None,
        }
    }

    /// Cancel the active run for a transport-owned session.
    pub fn interrupt(&self, session_id: &str, reason: &'static str) {
        self.engine.interrupt(session_id, reason);
    }
}
