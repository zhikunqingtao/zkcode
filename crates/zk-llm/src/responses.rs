//! Stateless Responses transport for `ZenMux` OpenAI/Gemini routes.
//! Function calls are released only after a complete, validated terminal payload.
use crate::openai_compat::LineSplitter;
use crate::{ChatRequest, FinishReason, ProviderError, ProviderEvent, ProviderResponseState, Role};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde_json::{Value, json};
use std::collections::{HashSet, VecDeque};
use tokio_util::sync::CancellationToken;
use zk_protocol::model::Usage;

/// Whether this provider/model requires Responses rather than Chat Completions.
#[must_use]
pub fn uses_responses(provider: &str, model: &str) -> bool {
    provider == "zenmux"
        && matches!(
            model,
            "openai/gpt-5.6-sol"
                | "openai/gpt-6-astra"
                | "google/gemini-3.8-flash"
                | "x-ai/grok-4.6"
        )
}

/// Build a stateless request, replaying opaque state only to its exact origin.
#[must_use]
pub fn build_request(request: &ChatRequest, provider: &str) -> Value {
    let mut input = Vec::new();
    for message in &request.messages {
        if message.role == Role::Assistant
            && let Some(state) = &message.provider_state
            && state.provider == provider
            && state.model == request.model
            && !state.output.is_empty()
        {
            input.extend(state.output.clone());
            continue;
        }
        if message.role == Role::Tool {
            input.push(json!({"type":"function_call_output", "call_id":message.tool_call_id, "output":message.content}));
            continue;
        }
        let mut content = Vec::new();
        if !message.content.is_empty() {
            content.push(json!({"type":if message.role == Role::Assistant {"output_text"} else {"input_text"}, "text":message.content}));
        }
        if message.role == Role::User {
            for image in &message.images {
                if let Some(url) = crate::openai_compat::resolve_image_url(image) {
                    content.push(json!({"type":"input_image", "image_url":url, "detail":"auto"}));
                }
            }
        }
        if !content.is_empty() {
            input.push(json!({"type":"message", "role":message.role.as_str(), "content":content}));
        }
        for call in &message.tool_calls {
            input.push(json!({"type":"function_call", "call_id":call.id, "name":call.name, "arguments":call.arguments}));
        }
    }
    let mut body = json!({"model":request.model,"stream":true,"store":false,"parallel_tool_calls":true,
        "max_output_tokens":request.max_tokens,"include":["reasoning.encrypted_content"],"input":input,
        "reasoning": if request.thinking.requires_support() {json!({"effort":if request.model.starts_with("openai/") {"xhigh"} else {"high"},"summary":"auto"})} else {json!({"effort":"none"})}});
    if let Some(effort) = request.reasoning_effort {
        body["reasoning"]["effort"] = json!(effort.as_str());
    }
    if let Some(system) = request.system_text() {
        body["instructions"] = json!(system);
    }
    if !request.tools.is_empty() {
        body["tools"] = json!(
            request
                .tools
                .iter()
                .map(|tool| json!({"type":"function", "name":tool.name,
            "description":tool.description,"parameters":tool.parameters,"strict":false}))
                .collect::<Vec<_>>()
        );
    }
    body
}

/// Parse Responses SSE, rejecting a disconnected/incomplete tool batch.
#[expect(
    clippy::too_many_lines,
    reason = "Protocol stream finalization validates the complete tool batch before emitting any execution event."
)]
pub fn event_stream<S>(
    source: S,
    cancel: CancellationToken,
    provider: String,
    model: String,
) -> impl Stream<Item = ProviderEvent> + Send + 'static
where
    S: Stream<Item = Result<Bytes, ProviderError>> + Send + 'static,
{
    struct State<S> {
        source: std::pin::Pin<Box<S>>,
        splitter: LineSplitter,
        pending: VecDeque<ProviderEvent>,
        cancel: CancellationToken,
        terminal: bool,
        text: String,
        thinking: String,
        provider: String,
        model: String,
    }
    futures::stream::unfold(
        State {
            source: Box::pin(source),
            splitter: LineSplitter::default(),
            pending: VecDeque::new(),
            cancel,
            terminal: false,
            text: String::new(),
            thinking: String::new(),
            provider,
            model,
        },
        |mut state| async move {
            loop {
                if state.cancel.is_cancelled() {
                    return None;
                }
                if let Some(event) = state.pending.pop_front() {
                    return Some((event, state));
                }
                if state.terminal {
                    return None;
                }
                let chunk = tokio::select! { biased; ()=state.cancel.cancelled()=>return None, chunk=state.source.next()=>chunk };
                let mut framing_error = None;
                let lines = match chunk {
                    Some(Ok(bytes)) => match state.splitter.feed(&bytes) {
                        Ok(lines) => lines,
                        Err(failure) => {
                            framing_error = Some(failure.error);
                            failure.lines
                        }
                    },
                    Some(Err(error)) => {
                        state.terminal = true;
                        state.pending.push_back(ProviderEvent::Error { error });
                        continue;
                    }
                    None => {
                        state.terminal = true;
                        state.splitter.flush().into_iter().collect()
                    }
                };
                let ended = state.terminal;
                let mut completed = false;
                for line in lines {
                    let Some(payload) = line.strip_prefix("data:").map(str::trim) else {
                        continue;
                    };
                    if payload == "[DONE]" {
                        continue;
                    }
                    let parsed =
                        serde_json::from_str::<Value>(payload).map_err(|_| ProviderError::Parse {
                            message: "INVALID_RESPONSES_EVENT".into(),
                        });
                    let event = match parsed {
                        Ok(event) => event,
                        Err(error) => {
                            state.pending.push_back(ProviderEvent::Error { error });
                            completed = true;
                            break;
                        }
                    };
                    match event["type"].as_str().unwrap_or("") {
                        "response.output_text.delta" => {
                            if let Some(text) = event["delta"].as_str() {
                                state.text.push_str(text);
                                state
                                    .pending
                                    .push_back(ProviderEvent::TextDelta { text: text.into() });
                            }
                        }
                        "response.reasoning_summary_text.delta" => {
                            if let Some(text) = event["delta"].as_str() {
                                state.thinking.push_str(text);
                                state.pending.push_back(ProviderEvent::ThinkingDelta {
                                    thinking: text.into(),
                                });
                            }
                        }
                        "response.completed" | "response.incomplete" => {
                            let incomplete = event["type"] == "response.incomplete";
                            match finish(
                                &event["response"],
                                incomplete,
                                &state.provider,
                                &state.model,
                                &state.text,
                                &state.thinking,
                            ) {
                                Ok(events) => state.pending.extend(events),
                                Err(error) => {
                                    state.pending.push_back(ProviderEvent::Error { error });
                                }
                            }
                            completed = true;
                            break;
                        }
                        "response.failed" | "error" => {
                            let error = if event["type"] == "response.failed" {
                                &event["response"]["error"]
                            } else {
                                &event
                            };
                            let code = error["code"].as_str().unwrap_or("responses_error");
                            state.pending.push_back(ProviderEvent::Error {
                                error: ProviderError::Stream {
                                    code: code.into(),
                                    message: error["message"].as_str().unwrap_or(code).into(),
                                },
                            });
                            completed = true;
                            break;
                        }
                        _ => {}
                    }
                }
                if completed {
                    state.terminal = true;
                } else if let Some(error) = framing_error {
                    state.pending.push_back(ProviderEvent::Error { error });
                    state.terminal = true;
                } else if ended {
                    state.pending.push_back(ProviderEvent::Error {
                        error: ProviderError::Network {
                            message: "INCOMPLETE_RESPONSES_STREAM: terminal response missing"
                                .into(),
                        },
                    });
                }
            }
        },
    )
}

fn finish(
    response: &Value,
    incomplete: bool,
    provider: &str,
    model: &str,
    emitted: &str,
    thinking: &str,
) -> Result<Vec<ProviderEvent>, ProviderError> {
    let invalid = || ProviderError::Parse {
        message: "INVALID_RESPONSES_OUTPUT".into(),
    };
    let output = response["output"].as_array().ok_or_else(invalid)?;
    let calls: Vec<&Value> = output
        .iter()
        .filter(|item| item["type"] == "function_call")
        .collect();
    if incomplete
        && (response["incomplete_details"]["reason"] != "max_output_tokens" || !calls.is_empty())
    {
        return Err(invalid());
    }
    let mut ids = HashSet::new();
    for call in &calls {
        let id = call["call_id"]
            .as_str()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(invalid)?;
        call["name"]
            .as_str()
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(invalid)?;
        let args = call["arguments"].as_str().ok_or_else(invalid)?;
        if !ids.insert(id)
            || !serde_json::from_str::<Value>(args).is_ok_and(|value| value.is_object())
        {
            return Err(invalid());
        }
    }
    let text: String = output
        .iter()
        .filter(|item| item["type"] == "message")
        .flat_map(|item| item["content"].as_array().into_iter().flatten())
        .filter(|part| part["type"] == "output_text")
        .filter_map(|part| part["text"].as_str())
        .collect();
    let mut events = Vec::new();
    if !text.starts_with(emitted) {
        return Err(invalid());
    }
    if text.len() > emitted.len() {
        events.push(ProviderEvent::TextDelta {
            text: text[emitted.len()..].into(),
        });
    }
    if thinking.is_empty() {
        let summary: String = output
            .iter()
            .filter(|item| item["type"] == "reasoning")
            .flat_map(|item| item["summary"].as_array().into_iter().flatten())
            .filter_map(|part| part["text"].as_str())
            .collect();
        if !summary.is_empty() {
            events.push(ProviderEvent::ThinkingDelta { thinking: summary });
        }
    }
    events.push(ProviderEvent::ResponseState {
        state: ProviderResponseState {
            provider: provider.into(),
            model: model.into(),
            output: output.clone(),
        },
    });
    for call in &calls {
        events.push(ProviderEvent::ToolUseStart {
            id: call["call_id"].as_str().unwrap_or_default().into(),
            name: call["name"].as_str().unwrap_or_default().into(),
        });
        events.push(ProviderEvent::ToolInputDelta {
            id: call["call_id"].as_str().unwrap_or_default().into(),
            delta: call["arguments"].as_str().unwrap_or_default().into(),
        });
    }
    let usage = response
        .get("usage")
        .filter(|value| value.is_object())
        .and_then(|value| {
            Some(Usage {
                input_tokens: value["input_tokens"].as_i64().filter(|n| *n >= 0)?,
                output_tokens: value["output_tokens"].as_i64().filter(|n| *n >= 0)?,
                cache_read_input_tokens: value["input_tokens_details"]["cached_tokens"]
                    .as_i64()
                    .unwrap_or(0),
                cache_creation_input_tokens: 0,
            })
        });
    events.push(ProviderEvent::Finish {
        finish_reason: FinishReason::from_openai(if incomplete {
            "length"
        } else if calls.is_empty() {
            "stop"
        } else {
            "tool_calls"
        }),
        usage,
    });
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChatMessage, ToolCallRequest};

    #[tokio::test]
    async fn provider_stream_error_keeps_exact_code_without_inventing_http_status() {
        for (code, retryable) in [
            ("server_error", true),
            ("rate_limit_exceeded", true),
            ("not_a_rate_limit", false),
            ("internal_server_error", false),
            ("invalid_prompt", false),
        ] {
            for kind in ["response.failed", "error"] {
                let detail = json!({"code":code,"message":"provider failure"});
                let event = if kind == "response.failed" {
                    json!({"type":kind,"response":{"error":detail}})
                } else {
                    json!({"type":kind,"code":code,"message":"provider failure"})
                };
                let source = futures::stream::iter([Ok(Bytes::from(format!("data: {event}\n\n")))]);
                let events: Vec<_> = event_stream(
                    source,
                    CancellationToken::new(),
                    "fixture".into(),
                    "model".into(),
                )
                .collect()
                .await;
                assert!(
                    matches!(events.as_slice(), [ProviderEvent::Error { error: ProviderError::Stream { code: actual, .. } }] if actual == code)
                );
                let ProviderEvent::Error { error } = &events[0] else {
                    unreachable!()
                };
                assert_eq!(error.is_retryable(), retryable);
                assert!(!error.to_string().contains("http"));
            }
        }
    }
    #[test]
    fn assistant_history_uses_output_text_and_provider_state_is_origin_scoped() {
        let state = ProviderResponseState {
            provider: "zenmux".into(),
            model: "openai/gpt-6-astra".into(),
            output: vec![json!({"type":"reasoning","encrypted_content":"signed"})],
        };
        let request = ChatRequest::new("openai/gpt-6-astra")
            .with_message(ChatMessage::assistant("prior").with_provider_state(Some(state)));
        assert_eq!(
            build_request(&request, "zenmux")["input"][0]["encrypted_content"],
            "signed"
        );
        let other = build_request(&request, "other");
        assert_eq!(other["input"][0]["content"][0]["type"], "output_text");
        assert!(!other.to_string().contains("signed"));
    }
    #[test]
    fn tool_history_is_flattened_and_call_id_is_preserved() {
        let request = ChatRequest::new("openai/gpt-6-astra")
            .with_message(ChatMessage::assistant_tool_calls(
                "",
                vec![ToolCallRequest {
                    id: "c1".into(),
                    name: "Read".into(),
                    arguments: "{}".into(),
                }],
            ))
            .with_message(ChatMessage::tool("c1", "contents"));
        let body = build_request(&request, "zenmux");
        assert_eq!(body["input"][0]["type"], "function_call");
        assert_eq!(body["input"][1]["call_id"], "c1");
        assert_eq!(body["input"][1]["output"], "contents");
        assert_eq!(body["store"], false);
    }
    #[tokio::test]
    async fn invalid_second_tool_prevents_the_whole_batch() {
        let response = json!({"type":"response.completed","response":{"output":[
            {"type":"function_call","call_id":"one","name":"Write","arguments":"{}"},
            {"type":"function_call","call_id":"two","name":"Write","arguments":"{"}],"usage":{"input_tokens":13,"output_tokens":2}}});
        let source = futures::stream::iter([Ok(Bytes::from(format!("data: {response}\n\n")))]);
        let events: Vec<_> = event_stream(
            source,
            CancellationToken::new(),
            "zenmux".into(),
            "openai/gpt-6-astra".into(),
        )
        .collect()
        .await;
        assert!(matches!(events.as_slice(), [ProviderEvent::Error { .. }]));
    }
    #[tokio::test]
    async fn disconnected_response_never_finishes_successfully() {
        let source = futures::stream::iter([Ok(Bytes::from_static(
            b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
        ))]);
        let events: Vec<_> = event_stream(
            source,
            CancellationToken::new(),
            "zenmux".into(),
            "openai/gpt-6-astra".into(),
        )
        .collect()
        .await;
        assert!(matches!(events.last(),Some(ProviderEvent::Error{error}) if error.is_retryable()));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ProviderEvent::Finish { .. }))
        );
    }
    #[tokio::test]
    async fn terminal_only_response_emits_text_usage_and_opaque_state() {
        let payload = json!({"type":"response.completed","response":{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}],"usage":{"input_tokens":20,"output_tokens":4,"input_tokens_details":{"cached_tokens":9}}}});
        let source = futures::stream::iter([Ok(Bytes::from(format!("data: {payload}\n")))]);
        let events: Vec<_> = event_stream(
            source,
            CancellationToken::new(),
            "zenmux".into(),
            "openai/gpt-6-astra".into(),
        )
        .collect()
        .await;
        assert!(matches!(&events[0],ProviderEvent::TextDelta{text} if text=="answer"));
        assert!(
            matches!(&events[1],ProviderEvent::ResponseState{state} if state.model=="openai/gpt-6-astra")
        );
        assert!(
            matches!(events.last(),Some(ProviderEvent::Finish{usage:Some(usage),..}) if usage.cache_read_input_tokens==9)
        );
    }
}
