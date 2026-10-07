//! Provider-specific routing uses the actual HTTP adapter with synthetic local data.
//! These checks exercise wire contracts; they do not claim remote API availability.
use std::time::Duration;

use futures::StreamExt as _;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use zk_llm::secret::KeySelectionStrategy;
use zk_llm::{
    ApiKey, ApiKeyRing, ChatMessage, ChatProvider, ChatRequest, FinishReason, OpenAiCompatProvider,
    ProviderConfig, ProviderEvent, ProviderResponseState, ThinkingMode, ToolCallRequest,
};

struct RecordedRequest {
    headers: String,
    body: Value,
}

async fn read_request(socket: &mut TcpStream) -> RecordedRequest {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut part = [0; 4096];
        let size = socket.read(&mut part).await.unwrap();
        assert_ne!(size, 0, "request ended before its headers");
        bytes.extend_from_slice(&part[..size]);
        assert!(bytes.len() <= 1024 * 1024, "synthetic request is bounded");
        if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    assert!(length <= 1024 * 1024);
    while bytes.len() < header_end + length {
        let mut part = [0; 4096];
        let size = socket.read(&mut part).await.unwrap();
        assert_ne!(size, 0, "request ended before its body");
        bytes.extend_from_slice(&part[..size]);
    }
    RecordedRequest {
        headers,
        body: serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap(),
    }
}

async fn server(
    responses: Vec<(u16, String)>,
) -> (String, tokio::task::JoinHandle<Vec<RecordedRequest>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, body) in responses {
            let (mut socket, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
                .await
                .unwrap()
                .unwrap();
            requests.push(read_request(&mut socket).await);
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 {status} Test\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
        requests
    });
    (url, task)
}

fn completed() -> String {
    "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"Checked\",\"content\":\"5\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n".into()
}

fn provider(name: &str, url: &str, model: &str) -> OpenAiCompatProvider {
    OpenAiCompatProvider::with_client(
        ProviderConfig::new(
            name,
            url,
            ApiKey::new("synthetic-test-key"),
            model,
            vec![model.into()],
        ),
        // Loopback wire fixtures must not be intercepted by host HTTP proxies.
        reqwest::Client::builder().no_proxy().build().unwrap(),
    )
}

fn continuation(model: &str, thinking: ThinkingMode) -> ChatRequest {
    let mut request = ChatRequest::new(model)
        .with_message(ChatMessage::user("Add 2 and 3"))
        .with_message(
            ChatMessage::assistant_tool_calls(
                "",
                vec![ToolCallRequest {
                    id: "call-add".into(),
                    name: "add".into(),
                    arguments: r#"{"a":2,"b":3}"#.into(),
                }],
            )
            .with_thinking(Some("Use the addition result".into())),
        )
        .with_message(ChatMessage::tool("call-add", "5"));
    request.max_tokens = 131_072;
    request.thinking = thinking;
    request
}

async fn run(provider: &OpenAiCompatProvider, request: ChatRequest) -> Vec<ProviderEvent> {
    tokio::time::timeout(
        Duration::from_secs(10),
        provider
            .chat_stream(request, CancellationToken::new())
            .unwrap()
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap()
}

fn assert_completed(events: &[ProviderEvent]) {
    assert!(
        events
            .iter()
            .all(|event| !matches!(event, ProviderEvent::Error { .. })),
        "unexpected provider events: {events:?}"
    );
    assert!(events.iter().any(|event| matches!(
        event,
        ProviderEvent::Finish {
            finish_reason: FinishReason::EndTurn,
            ..
        }
    )));
}

#[tokio::test]
async fn kimi_subscription_and_moonshot_respect_thinking_without_losing_tool_history() {
    let (url, captured) = server(vec![(200, completed()); 5]).await;
    for model in ["k3", "kimi-for-coding"] {
        for thinking in [ThinkingMode::Disabled, ThinkingMode::Adaptive] {
            assert_completed(
                &run(
                    &provider("kimi-code", &url, model),
                    continuation(model, thinking),
                )
                .await,
            );
        }
    }
    let mut direct = continuation("kimi-k3", ThinkingMode::Disabled);
    direct.max_tokens = 512;
    assert_completed(&run(&provider("moonshot", &url, "kimi-k3"), direct).await);
    let requests = captured.await.unwrap();
    for (index, request) in requests[..4].iter().enumerate() {
        assert!(request.headers.starts_with("POST /v1/chat/completions "));
        assert!(
            request
                .headers
                .to_ascii_lowercase()
                .contains("authorization: bearer synthetic-test-key")
        );
        let body = &request.body;
        assert_eq!(body["max_tokens"], 131_072);
        assert!(body.get("max_completion_tokens").is_none());
        if index % 2 == 0 {
            assert_eq!(body["thinking"]["type"], "disabled");
            assert!(body.get("reasoning_effort").is_none());
        } else {
            assert_eq!(body["thinking"]["type"], "enabled");
            assert_eq!(body["reasoning_effort"], "max");
        }
        assert_eq!(body["stream_options"]["include_usage"], true);
        assert_eq!(
            body["messages"][1]["reasoning_content"],
            "Use the addition result"
        );
        assert_eq!(
            body["messages"][1]["tool_calls"][0]["function"]["arguments"],
            r#"{"a":2,"b":3}"#
        );
        assert_eq!(body["messages"][2]["tool_call_id"], "call-add");
    }
    let direct = &requests[4].body;
    assert_eq!(direct["model"], "kimi-k3");
    assert_eq!(direct["max_completion_tokens"], 512);
    assert!(direct.get("max_tokens").is_none());
    assert_eq!(direct["thinking"]["type"], "disabled");
    assert!(direct.get("reasoning_effort").is_none());
}

#[tokio::test]
async fn bailian_glm_uses_local_route_identity_and_its_distinct_wire_parameters() {
    let (url, captured) = server(vec![(200, completed()); 2]).await;
    let mut sub = continuation("bailian/glm-5.3", ThinkingMode::Adaptive);
    sub.max_tokens = 65_536;
    assert_completed(
        &run(
            &provider("dashscope-token-plan", &url, "bailian/glm-5.3"),
            sub,
        )
        .await,
    );
    let mut direct = continuation("glm-5.3", ThinkingMode::Adaptive);
    direct.max_tokens = 512;
    assert_completed(&run(&provider("zhipu", &url, "glm-5.3"), direct).await);
    let requests = captured.await.unwrap();
    let body = &requests[0].body;
    assert_eq!(body["model"], "glm-5.3");
    assert_eq!(body["max_tokens"], 65_536);
    assert_eq!(body["enable_thinking"], true);
    assert_eq!(body["reasoning_effort"], "max");
    assert_eq!(body["clear_thinking"], false);
    assert!(body.get("thinking").is_none());
    assert!(body.get("tool_stream").is_none());
    assert_eq!(body["messages"][2]["content"], "5");
    let direct = &requests[1].body;
    assert_eq!(direct["model"], "glm-5.3");
    assert_eq!(direct["thinking"]["type"], "enabled");
    assert!(direct.get("enable_thinking").is_none());
}

#[tokio::test]
async fn openrouter_replays_opaque_state_only_to_the_original_provider_and_model() {
    const MODEL: &str = "openrouter/openai/gpt-6-astra";
    let (url, captured) = server(vec![(200, completed()); 3]).await;
    for (name, model) in [
        ("openrouter", MODEL),
        ("openrouter", "openrouter/anthropic/claude-fable-5.1"),
        ("other-provider", MODEL),
    ] {
        let mut request = continuation(model, ThinkingMode::Disabled);
        request.messages[1].provider_state = Some(ProviderResponseState {
            provider: "openrouter".into(),
            model: MODEL.into(),
            output: vec![json!({"type":"openrouter_reasoning","reasoning_details":[{
                "type":"reasoning.encrypted","id":"reasoning-1","data":"sealed-only-original"
            }]})],
        });
        assert_completed(&run(&provider(name, &url, model), request).await);
    }
    let requests = captured.await.unwrap();
    assert_eq!(requests[0].body["model"], "openai/gpt-6-astra");
    assert_eq!(requests[0].body["reasoning"], json!({"enabled": false}));
    assert_eq!(requests[0].body["provider"]["require_parameters"], true);
    assert_eq!(
        requests[0].body["messages"][1]["reasoning_details"][0]["data"],
        "sealed-only-original"
    );
    for request in &requests[1..] {
        assert!(!request.body.to_string().contains("sealed-only-original"));
    }
}

#[tokio::test]
async fn zenmux_quota_failure_cools_the_used_credential_for_later_requests() {
    for (status, kind) in [(402, "quote_exceeded"), (404, "model_not_available")] {
        let error = json!({"error":{"type":kind,"message":"subscription exhausted"}}).to_string();
        let finished = "data: {\"type\":\"response.completed\",\"response\":{\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n".to_owned();
        let (url, captured) = server(vec![
            (status, error),
            (200, finished.clone()),
            (200, finished),
        ])
        .await;
        // This explicitly constructed ring represents the paid-failover opt-in.
        // Environment opt-in filtering is tested separately in config.rs.
        let keys = ApiKeyRing::with_strategy(
            vec![ApiKey::new("sk-ss-v1-sub"), ApiKey::new("sk-ai-v1-paid")],
            KeySelectionStrategy::PriorityFailover,
        );
        let model = "openai/gpt-5.6-sol";
        let provider = OpenAiCompatProvider::with_client(
            ProviderConfig::with_keys("zenmux", url, keys, model, vec![model.into()]),
            reqwest::Client::builder().no_proxy().build().unwrap(),
        );
        let first = run(&provider, ChatRequest::new(model)).await;
        assert!(
            first
                .iter()
                .any(|event| matches!(event, ProviderEvent::Error { .. }))
        );
        for _ in 0..2 {
            assert_completed(&run(&provider, ChatRequest::new(model)).await);
        }
        let requests = captured.await.unwrap();
        for (request, credential) in
            requests
                .iter()
                .zip(["sk-ss-v1-sub", "sk-ai-v1-paid", "sk-ai-v1-paid"])
        {
            assert!(request.headers.starts_with("POST /v1/responses "));
            assert_eq!(request.body["reasoning"], json!({"effort":"none"}));
            assert!(
                request
                    .headers
                    .to_ascii_lowercase()
                    .contains(&format!("authorization: bearer {credential}"))
            );
        }
    }
}

#[tokio::test]
async fn deepseek_wire_honors_disabled_and_preserves_adaptive_explicit_effort() {
    use zk_llm::ReasoningEffort::{High, Low, Max};
    let options = [
        (ThinkingMode::Disabled, None),
        (ThinkingMode::Adaptive, None),
        (ThinkingMode::Enabled, Some(Low)),
        (ThinkingMode::Adaptive, Some(High)),
        (ThinkingMode::Enabled, Some(Max)),
    ];
    let models = ["deepseek-flash", "deepseek-v4-flash", "deepseek-v4-pro"];
    let (url, captured) = server(vec![(200, completed()); options.len() * models.len()]).await;
    for model in models {
        for (thinking, effort) in options {
            let mut request = ChatRequest::new(model)
                .with_message(ChatMessage::user("public test"))
                .with_thinking(thinking);
            request.reasoning_effort = effort;
            assert_completed(&run(&provider("deepseek", &url, model), request).await);
        }
    }
    let requests = captured.await.unwrap();
    for batch in requests.chunks_exact(options.len()) {
        assert_eq!(batch[0].body["thinking"]["type"], "disabled");
        assert!(batch[0].body.get("reasoning_effort").is_none());
        for (index, effort) in ["max", "low", "high", "max"].into_iter().enumerate() {
            let body = &batch[index + 1].body;
            assert_eq!(body["thinking"]["type"], "enabled");
            assert_eq!(body["reasoning_effort"], effort);
            assert_eq!(body["stream_options"]["include_usage"], true);
        }
    }
}

#[tokio::test]
async fn independent_summary_wire_retains_its_own_reasoning_override() {
    use zk_llm::SummaryThinkingMode::{Low, Max, Off};
    let response = json!({"choices":[{"message":{"content":"summary"},"finish_reason":"stop"}],
        "usage":{"prompt_tokens":11,"completion_tokens":2}})
    .to_string();
    let (url, captured) = server(vec![(200, response); 3]).await;
    for (thinking, summary) in [
        (ThinkingMode::Enabled, Off),
        (ThinkingMode::Disabled, Low),
        (ThinkingMode::Disabled, Max),
    ] {
        let mut request = ChatRequest::new("deepseek-flash")
            .with_message(ChatMessage::user("public summary test"))
            .with_thinking(thinking);
        request.summary_thinking = Some(summary);
        assert_completed(&run(&provider("deepseek", &url, "deepseek-flash"), request).await);
    }
    let requests = captured.await.unwrap();
    assert_eq!(requests[0].body["thinking"]["type"], "disabled");
    assert!(requests[0].body.get("reasoning_effort").is_none());
    for (index, effort) in ["low", "max"].into_iter().enumerate() {
        assert_eq!(requests[index + 1].body["thinking"]["type"], "enabled");
        assert_eq!(requests[index + 1].body["reasoning_effort"], effort);
    }
    for request in requests {
        assert_eq!(request.body["stream"], false);
        assert!(request.body.get("stream_options").is_none());
    }
}

#[tokio::test]
async fn qwen_disabled_is_explicit_and_glm_disabled_never_opens_http() {
    let models = [
        "qwen3.6-max",
        "qwen3.7-plus",
        "qwen3.8-max-0902",
        "qwen3.8-flash",
    ];
    let (url, captured) = server(vec![(200, completed()); models.len()]).await;
    for model in models {
        assert_completed(
            &run(
                &provider("dashscope", &url, model),
                ChatRequest::new(model)
                    .with_max_tokens(512)
                    .with_message(ChatMessage::user("public test"))
                    .with_thinking(ThinkingMode::Disabled),
            )
            .await,
        );
    }
    for request in captured.await.unwrap() {
        assert_eq!(request.body["enable_thinking"], false);
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    for (name, model) in [
        ("zhipu", "glm-5.3"),
        ("zhipu", "glm-5.3-flash"),
        ("dashscope-token-plan", "bailian/glm-5.3"),
    ] {
        for summary in [None, Some(zk_llm::SummaryThinkingMode::Off)] {
            let mut request = ChatRequest::new(model).with_thinking(ThinkingMode::Disabled);
            request.summary_thinking = summary;
            let result = provider(name, &url, model).chat_stream(request, CancellationToken::new());
            assert!(
                matches!(result, Err(zk_llm::ProviderError::Preflight { message })
                if message.starts_with("UNSUPPORTED_THINKING_DISABLED:"))
            );
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err(),
        "unsupported thinking mode must fail before opening any HTTP connection"
    );
}

#[tokio::test]
async fn openrouter_thinking_switch_survives_provider_specific_request_mapping() {
    let models = [
        "openrouter/openai/gpt-6-astra",
        "openrouter/anthropic/claude-fable-5.1",
    ];
    let (url, captured) = server(vec![(200, completed()); 6]).await;
    for model in models {
        for thinking in [
            ThinkingMode::Disabled,
            ThinkingMode::Adaptive,
            ThinkingMode::Enabled,
        ] {
            let mut request = ChatRequest::new(model)
                .with_message(ChatMessage::user("public test"))
                .with_thinking(thinking);
            if thinking == ThinkingMode::Enabled {
                request.reasoning_effort = Some(zk_llm::ReasoningEffort::Max);
            }
            assert_completed(&run(&provider("openrouter", &url, model), request).await);
        }
    }
    for batch in captured.await.unwrap().chunks_exact(3) {
        assert_eq!(batch[0].body["reasoning"], json!({"enabled":false}));
        for request in &batch[1..] {
            assert_eq!(
                request.body["reasoning"],
                json!({"effort":"max","exclude":false})
            );
            assert!(request.body.get("reasoning_effort").is_none());
        }
        for request in batch {
            assert_eq!(request.body["provider"]["require_parameters"], true);
        }
    }
}

#[tokio::test]
async fn direct_openai_respects_supported_disabled_and_rejects_forced_reasoning() {
    let (url, captured) = server(vec![(200, completed()); 3]).await;
    for (model, thinking) in [
        ("gpt-5.6-sol", ThinkingMode::Disabled),
        ("gpt-5.6-sol", ThinkingMode::Adaptive),
        ("gpt-6-astra", ThinkingMode::Adaptive),
    ] {
        assert_completed(
            &run(
                &provider("openai", &url, model),
                ChatRequest::new(model)
                    .with_max_tokens(512)
                    .with_message(ChatMessage::user("public test"))
                    .with_thinking(thinking),
            )
            .await,
        );
    }
    let requests = captured.await.unwrap();
    assert_eq!(requests[0].body["reasoning_effort"], "none");
    for request in &requests[1..] {
        assert_eq!(request.body["reasoning_effort"], "max");
    }
    for request in &requests {
        assert!(request.body.get("reasoning").is_none());
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let direct = provider("openai", &url, "gpt-6-astra");
    let request = ChatRequest::new("gpt-6-astra").with_thinking(ThinkingMode::Disabled);
    assert!(matches!(
        direct.chat_stream(request, CancellationToken::new()),
        Err(zk_llm::ProviderError::Preflight { message })
            if message.starts_with("UNSUPPORTED_THINKING_DISABLED:")
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err(),
        "known forced reasoning must fail before opening any HTTP connection"
    );
}
