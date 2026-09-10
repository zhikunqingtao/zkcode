//! Bounded, typed context checkpoints shared by root and child agent loops.
//!
//! Checkpoints are recovery evidence, not permission to resume. Inline image bytes
//! and likely credentials are deliberately excluded; the envelope records whether
//! it remains safe to reconstruct the request exactly.

use serde_json::{Value, json};
use zk_db::{Db, DbError};
use zk_llm::{ChatMessage, ChatRequest, Role, ToolCallRequest};

const CHECKPOINT_SCHEMA_VERSION: u32 = 1;
const MAX_CHECKPOINT_MESSAGES: usize = 64;
const MAX_MESSAGE_CHARS: usize = 32 * 1024;
const MAX_ARGUMENT_CHARS: usize = 16 * 1024;
const MAX_SYSTEM_CHARS: usize = 64 * 1024;

/// Why a durable checkpoint was taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckpointReason {
    RunStarted,
    ContextCompacted,
    ContextRecovered,
    ToolSubmitted,
    ParentWaiting,
    TurnCadence,
    ToolCadence,
    Terminal,
}

impl CheckpointReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RunStarted => "runStarted",
            Self::ContextCompacted => "contextCompacted",
            Self::ContextRecovered => "contextRecovered",
            Self::ToolSubmitted => "toolSubmitted",
            Self::ParentWaiting => "parentWaiting",
            Self::TurnCadence => "turnCadence",
            Self::ToolCadence => "toolCadence",
            Self::Terminal => "terminal",
        }
    }
}

/// Per-run checkpoint cursor and counters.
pub(crate) struct ContextCheckpointState {
    run_id: String,
    session_id: String,
    agent_id: String,
    next_seq: i64,
    working_dir: Option<String>,
    pub(crate) turn_count: u32,
    pub(crate) tool_call_count: u32,
    pub(crate) tokens_consumed: i64,
}

impl ContextCheckpointState {
    /// Continue after the newest committed sequence. A read failure is surfaced so
    /// callers can fail closed rather than overwrite an unknown checkpoint.
    pub(crate) async fn load(
        db: &Db,
        run_id: &str,
        session_id: &str,
        agent_id: &str,
        working_dir: Option<String>,
    ) -> Result<Self, DbError> {
        let latest = db.latest_agent_checkpoint(run_id).await?;
        if let Some(checkpoint) = latest.as_ref()
            && (checkpoint.session_id != session_id || checkpoint.agent_id != agent_id)
        {
            return Err(DbError::Invalid("CHECKPOINT_OWNER_MISMATCH".to_owned()));
        }
        let next_seq = latest
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.seq.saturating_add(1));
        Ok(Self {
            run_id: run_id.to_owned(),
            session_id: session_id.to_owned(),
            agent_id: agent_id.to_owned(),
            next_seq,
            working_dir,
            turn_count: latest.as_ref().map_or(0, |checkpoint| {
                u32::try_from(checkpoint.turn_count).unwrap_or(u32::MAX)
            }),
            tool_call_count: latest.as_ref().map_or(0, |checkpoint| {
                u32::try_from(checkpoint.tool_call_count).unwrap_or(u32::MAX)
            }),
            tokens_consumed: latest
                .as_ref()
                .map_or(0, |checkpoint| checkpoint.tokens_consumed.max(0)),
        })
    }

    pub(crate) fn note_turn(&mut self, usage_tokens: i64) {
        self.turn_count = self.turn_count.saturating_add(1);
        self.tokens_consumed = self.tokens_consumed.saturating_add(usage_tokens.max(0));
    }

    /// Returns true when this completion crossed a ten-tool checkpoint boundary.
    pub(crate) fn note_tools(&mut self, completed: usize) -> bool {
        let before = self.tool_call_count / 10;
        self.tool_call_count = self
            .tool_call_count
            .saturating_add(u32::try_from(completed).unwrap_or(u32::MAX));
        self.tool_call_count / 10 > before
    }

    pub(crate) const fn turn_cadence_due(&self) -> bool {
        self.turn_count > 0 && self.turn_count.is_multiple_of(5)
    }

    /// Persist a bounded checkpoint. Sequence advancement occurs only after the
    /// database write succeeds, so transient failures can be retried idempotently.
    pub(crate) async fn save(
        &mut self,
        db: &Db,
        request: &ChatRequest,
        reason: CheckpointReason,
        terminal_reason: Option<&str>,
    ) -> Result<(), DbError> {
        let (messages, mut restorable, mut truncated) = checkpoint_messages(&request.messages);
        let (system_prompt, system_truncated) =
            request.system_text().map_or((None, false), |text| {
                let (text, truncated) = redact_and_bound(&text, MAX_SYSTEM_CHARS);
                (Some(text), truncated)
            });
        truncated |= system_truncated;
        restorable &= !system_truncated;

        let payload = json!({
            "schemaVersion": CHECKPOINT_SCHEMA_VERSION,
            "kind": "contextCheckpoint",
            "reason": reason.as_str(),
            "restorable": restorable,
            "truncated": truncated,
            "model": request.model,
            "maxTokens": request.max_tokens,
            "thinking": format!("{:?}", request.thinking),
            "systemPrompt": system_prompt,
            "messages": messages,
            "terminalReason": terminal_reason,
        });
        let mut checkpoint = zk_db::new_agent_checkpoint(
            &self.run_id,
            &self.session_id,
            &self.agent_id,
            self.next_seq,
            payload,
        );
        checkpoint.turn_count = i64::from(self.turn_count);
        checkpoint.tool_call_count = i64::from(self.tool_call_count);
        checkpoint.tokens_consumed = self.tokens_consumed;
        checkpoint.working_dir.clone_from(&self.working_dir);
        db.save_agent_checkpoint(&checkpoint).await?;
        self.next_seq = self.next_seq.saturating_add(1);
        Ok(())
    }
}

fn checkpoint_messages(messages: &[ChatMessage]) -> (Vec<Value>, bool, bool) {
    let skipped = messages.len().saturating_sub(MAX_CHECKPOINT_MESSAGES);
    let selected = &messages[skipped..];
    let mut restorable = skipped == 0;
    let mut truncated = skipped > 0;
    let values = selected
        .iter()
        .map(|message| {
            let (content, content_truncated) =
                redact_and_bound(&message.content, MAX_MESSAGE_CHARS);
            let (thinking, thinking_truncated) =
                message
                    .thinking
                    .as_deref()
                    .map_or((None, false), |thinking| {
                        let (value, was_truncated) = redact_and_bound(thinking, MAX_MESSAGE_CHARS);
                        (Some(value), was_truncated)
                    });
            let has_images = !message.images.is_empty();
            restorable &= !content_truncated && !thinking_truncated && !has_images;
            truncated |= content_truncated || thinking_truncated;
            let tool_calls = message
                .tool_calls
                .iter()
                .map(|call| {
                    let (arguments, arguments_truncated) =
                        redact_and_bound(&call.arguments, MAX_ARGUMENT_CHARS);
                    restorable &= !arguments_truncated;
                    truncated |= arguments_truncated;
                    json!({
                        "id": call.id,
                        "name": call.name,
                        "arguments": arguments,
                    })
                })
                .collect::<Vec<_>>();
            let images = message
                .images
                .iter()
                .map(|image| {
                    json!({
                        "mediaType": image.media_type,
                        "hadInlineData": image.data.is_some(),
                        "hadUrl": image.url.is_some(),
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "role": message.role.as_str(),
                "content": content,
                "thinking": thinking,
                "toolCalls": tool_calls,
                "toolCallId": message.tool_call_id,
                "images": images,
            })
        })
        .collect();
    (values, restorable, truncated)
}

/// Reconstruct the provider-neutral conversation from a checkpoint that has
/// already passed the database recovery proof gate.
pub(crate) fn restore_checkpoint_messages(checkpoint: &Value) -> Result<Vec<ChatMessage>, String> {
    if checkpoint.get("kind").and_then(Value::as_str) != Some("contextCheckpoint")
        || checkpoint.get("restorable").and_then(Value::as_bool) != Some(true)
        || checkpoint.get("truncated").and_then(Value::as_bool) != Some(false)
    {
        return Err("RECOVERY_CHECKPOINT_NOT_RESTORABLE".to_owned());
    }
    let messages = checkpoint
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| "RECOVERY_CHECKPOINT_MESSAGES_MISSING".to_owned())?;
    if messages.is_empty() {
        return Err("RECOVERY_CHECKPOINT_MESSAGES_EMPTY".to_owned());
    }
    messages
        .iter()
        .map(|value| {
            let role = match value.get("role").and_then(Value::as_str) {
                Some("system") => Role::System,
                Some("user") => Role::User,
                Some("assistant") => Role::Assistant,
                Some("tool") => Role::Tool,
                _ => return Err("RECOVERY_CHECKPOINT_ROLE_INVALID".to_owned()),
            };
            let content = value
                .get("content")
                .and_then(Value::as_str)
                .ok_or_else(|| "RECOVERY_CHECKPOINT_CONTENT_INVALID".to_owned())?
                .to_owned();
            let images = value
                .get("images")
                .and_then(Value::as_array)
                .ok_or_else(|| "RECOVERY_CHECKPOINT_IMAGES_INVALID".to_owned())?;
            if !images.is_empty() {
                return Err("RECOVERY_CHECKPOINT_IMAGES_UNSUPPORTED".to_owned());
            }
            let tool_calls = value
                .get("toolCalls")
                .and_then(Value::as_array)
                .ok_or_else(|| "RECOVERY_CHECKPOINT_TOOL_CALLS_INVALID".to_owned())?
                .iter()
                .map(|call| {
                    Ok(ToolCallRequest {
                        id: call
                            .get("id")
                            .and_then(Value::as_str)
                            .ok_or_else(|| "RECOVERY_CHECKPOINT_TOOL_ID_INVALID".to_owned())?
                            .to_owned(),
                        name: call
                            .get("name")
                            .and_then(Value::as_str)
                            .ok_or_else(|| "RECOVERY_CHECKPOINT_TOOL_NAME_INVALID".to_owned())?
                            .to_owned(),
                        arguments: call
                            .get("arguments")
                            .and_then(Value::as_str)
                            .ok_or_else(|| "RECOVERY_CHECKPOINT_TOOL_ARGUMENTS_INVALID".to_owned())?
                            .to_owned(),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(ChatMessage {
                role,
                content,
                images: Vec::new(),
                thinking: value
                    .get("thinking")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                tool_calls,
                tool_call_id: value
                    .get("toolCallId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            })
        })
        .collect()
}

/// Remove likely secret-bearing lines and cap by Unicode scalar count (never split
/// UTF-8). Redaction itself makes a checkpoint non-exact and therefore truncated.
fn redact_and_bound(value: &str, max_chars: usize) -> (String, bool) {
    let mut redacted = false;
    let filtered = value
        .lines()
        .map(|line| {
            let lower = line.to_ascii_lowercase();
            if [
                "authorization:",
                "api_key",
                "apikey",
                "password",
                "secret",
                "cookie:",
                "access_token",
                "refresh_token",
            ]
            .iter()
            .any(|marker| lower.contains(marker))
            {
                redacted = true;
                "[REDACTED]"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let char_count = filtered.chars().count();
    if char_count <= max_chars {
        return (filtered, redacted);
    }
    let mut bounded = filtered.chars().take(max_chars).collect::<String>();
    bounded.push_str("\n[CHECKPOINT_TRUNCATED]");
    (bounded, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zk_llm::{ImageSource, ToolCallRequest};

    #[test]
    fn typed_checkpoint_redacts_secrets_and_marks_images_non_restorable() {
        let messages = vec![
            ChatMessage::user("hello\nAuthorization: Bearer private"),
            ChatMessage::user_with_images(
                "look",
                vec![ImageSource {
                    media_type: "image/png".to_owned(),
                    data: Some("private-base64".to_owned()),
                    url: None,
                }],
            ),
        ];
        let (value, restorable, truncated) = checkpoint_messages(&messages);
        assert!(!restorable);
        assert!(truncated, "redaction must prevent exact automatic restore");
        assert_eq!(value[0]["content"], "hello\n[REDACTED]");
        assert_eq!(value[1]["images"][0]["hadInlineData"], true);
        assert!(!value[1].to_string().contains("private-base64"));
    }

    #[test]
    fn oversized_content_and_arguments_are_bounded_without_splitting_utf8() {
        let message = ChatMessage::assistant_tool_calls(
            "中".repeat(MAX_MESSAGE_CHARS + 1),
            vec![ToolCallRequest {
                id: "call-1".to_owned(),
                name: "Read".to_owned(),
                arguments: "界".repeat(MAX_ARGUMENT_CHARS + 1),
            }],
        );
        let (value, restorable, truncated) = checkpoint_messages(&[message]);
        assert!(!restorable);
        assert!(truncated);
        assert!(
            value[0]["content"]
                .as_str()
                .unwrap()
                .ends_with("[CHECKPOINT_TRUNCATED]")
        );
        assert!(
            value[0]["toolCalls"][0]["arguments"]
                .as_str()
                .unwrap()
                .ends_with("[CHECKPOINT_TRUNCATED]")
        );
    }

    #[tokio::test]
    async fn successful_writes_advance_sequence_and_capture_counters() {
        let db = Db::open_in_memory().expect("db");
        let session = db
            .create_session("kimi-k3", "/tmp/project")
            .await
            .expect("session");
        let task_id = uuid::Uuid::new_v4().to_string();
        let run_id = uuid::Uuid::new_v4().to_string();
        let submitted = db
            .create_task_with_run(&zk_db::CreateTaskWithRun {
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                root_session_id: session.id.clone(),
                transcript_session_id: session.id.clone(),
                parent_task_id: None,
                parent_run_id: None,
                creator_tool_use_id: None,
                ordinal: 0,
                description: "checkpoint test".to_owned(),
                prompt: Some("hello".to_owned()),
                task_type: "agent".to_owned(),
                model: "kimi-k3".to_owned(),
                working_dir: "/tmp/project".to_owned(),
                execution_config_json: "{}".to_owned(),
                startup_epoch: 1,
            })
            .await
            .expect("task and run");
        assert_eq!(
            db.claim_task_run_cas(&task_id, &run_id, submitted.task.version)
                .await
                .expect("claim"),
            zk_db::CasOutcome::Applied
        );
        let mut state = ContextCheckpointState::load(
            &db,
            &run_id,
            &session.id,
            "agent-context",
            Some("/tmp/project".to_owned()),
        )
        .await
        .expect("state");
        state.note_turn(7);
        assert!(state.note_tools(10));
        let request = ChatRequest::new("kimi-k3").with_message(ChatMessage::user("hello"));
        state
            .save(&db, &request, CheckpointReason::TurnCadence, None)
            .await
            .expect("first");
        state
            .save(&db, &request, CheckpointReason::Terminal, Some("end_turn"))
            .await
            .expect("second");
        let stored = db
            .latest_agent_checkpoint(&run_id)
            .await
            .expect("load")
            .expect("checkpoint");
        assert_eq!(stored.seq, 1);
        assert_eq!(stored.turn_count, 1);
        assert_eq!(stored.tool_call_count, 10);
        assert_eq!(stored.tokens_consumed, 7);
        assert_eq!(stored.messages["reason"], "terminal");
    }
}
