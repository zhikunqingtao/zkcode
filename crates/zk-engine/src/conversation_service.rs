//! Transport-neutral conversation execution over the production [`crate::Engine`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;

use serde::Serialize;
use zk_db::{MessageRole, StoredBlock};
use zk_protocol::Usage;

use crate::engine::ConversationRunOptions;
use crate::{ConversationCancellation, ConversationLease, Engine};

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
    /// Exact execution identity, absent only when admission did not create a Run.
    pub run_id: Option<String>,
    /// Concatenated terminal assistant text.
    pub result: String,
    /// Usage produced after this invocation began.
    pub usage: Usage,
    /// Direct physical-call cost from the authoritative Run ledger, including retries and summaries.
    pub cost_usd: f64,
    /// False means provider usage or price is incomplete, never a claim of a free request.
    pub usage_complete: bool,
    /// Tool calls and their durable results.
    pub tool_calls: Vec<ConversationToolCall>,
    /// Committed Run stop reason, or the last assistant reason after normal completion.
    pub stop_reason: Option<String>,
    /// Stable execution error, if no terminal conversation could be loaded.
    pub error: Option<String>,
}

/// Shared business service; transports only select how the outcome is encoded.
pub struct ConversationService {
    engine: Arc<Engine>,
    db: zk_db::Db,
    requests: Mutex<RequestIndex>,
}

#[derive(Default)]
struct RequestIndex {
    pending: HashSet<String>,
    active: HashMap<String, ConversationCancellation>,
    closed: HashMap<String, (&'static str, std::time::Instant)>,
}

impl RequestIndex {
    fn prune(&mut self) {
        self.closed
            .retain(|_, (_, at)| at.elapsed() < std::time::Duration::from_hours(24));
    }
}

impl ConversationService {
    /// Bind the service to the same Engine and DB used by the WebSocket path.
    #[must_use]
    pub fn new(engine: Arc<Engine>, db: zk_db::Db) -> Self {
        Self {
            engine,
            db,
            requests: Mutex::new(RequestIndex::default()),
        }
    }

    /// Execute an already authorized external MCP operation on this same Engine.
    /// The host retains the owned call/finalizer and supplies its normal admission
    /// wrapped by the current connection capability ceiling.
    ///
    /// # Errors
    /// Returns the unified engine's ownership, authorization or durability failure.
    pub async fn execute_external_bound_tool(
        &self,
        call: crate::ExternalToolCall,
        admission: Arc<dyn crate::admission::ToolAdmission>,
    ) -> Result<crate::ExternalToolResult, String> {
        self.engine
            .execute_external_bound_tool(call, admission)
            .await
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
        let Some(lease) = self.reserve(session_id) else {
            return Self::failed_outcome(session_id, "QUERY_BUSY".to_owned());
        };
        self.execute_reserved(lease, prompt, options).await
    }

    /// Hold exclusive ownership before a transport starts sending events.
    #[must_use]
    pub fn reserve(&self, session_id: &str) -> Option<ConversationLease> {
        self.engine.reserve_conversation(session_id)
    }

    /// Cancellation for this lease cannot interrupt a subsequent session query.
    #[must_use]
    pub fn cancellation(&self, lease: &ConversationLease) -> ConversationCancellation {
        lease.cancellation(Arc::clone(&self.engine))
    }

    /// Claim a content-free request identity before creating or forking a Session.
    ///
    /// # Errors
    /// Rejects reused/closed request identities and a full bounded request index.
    pub fn claim_request(&self, id: &str) -> Result<(), &'static str> {
        let mut requests = self
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        requests.prune();
        if requests.pending.contains(id) || requests.active.contains_key(id) {
            return Err("QUERY_REQUEST_ACTIVE");
        }
        if let Some((code, _)) = requests.closed.get(id) {
            return Err(code);
        }
        if requests.pending.len() + requests.active.len() + requests.closed.len() >= 16_384 {
            return Err("QUERY_REQUEST_INDEX_FULL");
        }
        requests.pending.insert(id.to_owned());
        Ok(())
    }

    /// Bind the claimed request to its exact execution cancellation capability.
    ///
    /// # Errors
    /// A cancelled, unclaimed or already closed identity cannot bind a new execution.
    pub fn register_request(
        &self,
        id: &str,
        lease: &ConversationLease,
    ) -> Result<(), &'static str> {
        let mut requests = self
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((code, _)) = requests.closed.get(id) {
            return Err(code);
        }
        if !requests.pending.remove(id) {
            return Err("QUERY_REQUEST_NOT_RESERVED");
        }
        requests
            .active
            .insert(id.to_owned(), self.cancellation(lease));
        Ok(())
    }

    /// Keep a bounded content-free tombstone, so neither a late stop nor a retry
    /// can replay an execution whose client has already disconnected.
    pub fn unregister_request(&self, id: &str) {
        let mut requests = self
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending = requests.pending.remove(id);
        if requests.active.remove(id).is_some() || pending {
            requests
                .closed
                .entry(id.to_owned())
                .or_insert(("QUERY_REQUEST_FINISHED", std::time::Instant::now()));
        }
    }

    /// Stop an exact request, fencing admission even if Ctrl+C arrived before POST.
    /// # Errors
    /// A full bounded index fails visibly instead of dropping the cancellation fence.
    pub fn cancel_request(&self, id: &str) -> Result<bool, &'static str> {
        let cancel = {
            let mut requests = self
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            requests.prune();
            let cancel = requests.active.get(id).cloned();
            if cancel.is_none()
                && !requests.closed.contains_key(id)
                && !requests.pending.contains(id)
                && requests.pending.len() + requests.active.len() + requests.closed.len() >= 16_384
            {
                return Err("QUERY_REQUEST_INDEX_FULL");
            }
            requests
                .closed
                .entry(id.to_owned())
                .or_insert(("QUERY_REQUEST_CANCELLED", std::time::Instant::now()));
            cancel
        };
        if let Some(cancel) = cancel {
            cancel.cancel("USER_INTERRUPT");
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Execute a previously admitted query using the same production engine.
    pub async fn execute_reserved(
        &self,
        lease: ConversationLease,
        prompt: String,
        options: ConversationRunOptions,
    ) -> ConversationOutcome {
        let session_id = lease.session_id().to_owned();
        let cancellation = self.cancellation(&lease);
        if options
            .thinking
            .is_some_and(zk_llm::ThinkingMode::requires_support)
        {
            match self.db.get_session(&session_id).await {
                Ok(Some(session)) if zk_llm::capabilities_for(&session.model).supports_thinking => {
                }
                Ok(Some(_)) => {
                    return Self::failed_outcome(&session_id, "QUERY_THINKING_UNSUPPORTED".into());
                }
                _ => return Self::failed_outcome(&session_id, "QUERY_SESSION_UNAVAILABLE".into()),
            }
        }
        if options.thinking == Some(zk_llm::ThinkingMode::Disabled)
            && options.reasoning_effort.is_some()
        {
            return Self::failed_outcome(&session_id, "REASONING_OPTIONS_CONFLICT".into());
        }
        let (_model, messages) = match Box::pin(
            self.engine
                .run_reserved_conversation(&lease, prompt, options),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => return Self::failed_outcome(&session_id, error),
        };
        let Some(run_id) = cancellation.run_id() else {
            return Self::failed_outcome(&session_id, "QUERY_NOT_STARTED".into());
        };
        let Ok(Some(run)) = self.db.find_run_by_id(&run_id).await else {
            return Self::failed_outcome(&session_id, "QUERY_LEDGER_UNAVAILABLE".into());
        };
        let usage = Usage {
            input_tokens: run.input_tokens,
            output_tokens: run.output_tokens,
            cache_read_input_tokens: run.cache_read_tokens,
            cache_creation_input_tokens: run.cache_create_tokens,
        };
        let error = match self.db.read_task_result(&run.task_id, None, 0, 1).await {
            Ok(Some(result)) if result.result.run_id == run_id => {
                if result.result.status == zk_db::ResultStatus::Complete {
                    None
                } else {
                    Some(result.result.error_code.unwrap_or_else(|| {
                        format!(
                            "QUERY_{}",
                            result.result.status.as_db().to_ascii_uppercase()
                        )
                    }))
                }
            }
            _ => Some("QUERY_RESULT_UNAVAILABLE".to_owned()),
        };
        let (result, stop_reason, tool_calls) = Self::project_messages(&messages);
        let stop_reason =
            crate::engine::committed_stop_reason(run.exit_reason.as_deref(), stop_reason);
        #[allow(clippy::cast_precision_loss)]
        let cost_usd = run.cost_nanos_usd as f64 / 1_000_000_000.0;
        ConversationOutcome {
            session_id,
            run_id: Some(run_id),
            result,
            usage,
            cost_usd,
            usage_complete: run.usage_complete,
            tool_calls,
            stop_reason,
            error,
        }
    }

    fn project_messages(
        messages: &[zk_db::model::MessageRecord],
    ) -> (String, Option<String>, Vec<ConversationToolCall>) {
        let mut result = String::new();
        let mut stop_reason = None;
        let mut tool_calls = Vec::new();
        for message in messages {
            if message.role == MessageRole::Assistant {
                stop_reason.clone_from(&message.stop_reason);
                // The result is the final assistant segment, not a concatenation
                // of intermediate narration and the final answer.
                result.clear();
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
        (result, stop_reason, tool_calls)
    }

    fn failed_outcome(session_id: &str, error: String) -> ConversationOutcome {
        ConversationOutcome {
            session_id: session_id.to_owned(),
            run_id: None,
            result: String::new(),
            usage: Usage::default(),
            cost_usd: 0.0,
            usage_complete: false,
            tool_calls: Vec::new(),
            stop_reason: None,
            error: Some(error),
        }
    }

    /// Reserve the query slot for a local file/history mutation.
    #[must_use]
    pub fn try_reserve_session_mutation(&self, session_id: &str) -> Option<impl Send + 'static> {
        self.engine.try_reserve_session_mutation(session_id)
    }

    /// Cancel the active run for a transport-owned session.
    pub fn interrupt(&self, session_id: &str, reason: &'static str) {
        self.engine.interrupt(session_id, reason);
    }
}
