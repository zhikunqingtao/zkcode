//! Independent compaction transport against an actual local HTTP server.
use futures::StreamExt as _;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use zk_llm::{
    ApiKeyRing, ChatMessage, ChatProvider, ChatRequest, OpenAiCompatProvider, ProviderConfig,
    ProviderEvent, ProviderRegistry, SummaryThinkingMode,
};

async fn request(socket: &mut tokio::net::TcpStream) -> (String, Value) {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0; 4096];
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
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
    while bytes.len() < header_end + length {
        let mut chunk = [0; 4096];
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&chunk[..count]);
    }
    (
        headers,
        serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap(),
    )
}

#[tokio::test]
async fn summary_http_is_nonstreaming_retries_once_with_same_key_and_keeps_partial_usage_unknown() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut seen = Vec::new();
        for attempt in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            seen.push(request(&mut socket).await);
            let (status, body) = if attempt == 0 {
                (
                    "429 Too Many Requests",
                    json!({"error":{"message":"retry once"}}),
                )
            } else {
                (
                    "200 OK",
                    json!({"choices":[{"message":{"content":"<summary>keep this</summary>"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12}}),
                )
            };
            let body = body.to_string();
            socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nRetry-After: 0\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        }
        seen
    });
    // Loopback fixtures must not pass through an ambient HTTP proxy.
    let provider = OpenAiCompatProvider::with_client(
        ProviderConfig::with_keys(
            "deepseek",
            url,
            ApiKeyRing::from_csv("summary-first,chat-second"),
            "deepseek-flash",
            vec!["deepseek-flash".into()],
        ),
        reqwest::Client::builder().no_proxy().build().unwrap(),
    );
    let mut registry = ProviderRegistry::new();
    registry.register(
        "deepseek",
        Arc::new(provider),
        vec!["deepseek-flash".into()],
    );
    let mut request =
        ChatRequest::new("deepseek-flash").with_message(ChatMessage::user("summarize"));
    request.summary_thinking = Some(SummaryThinkingMode::Low);
    request.max_tokens = 8192;
    let events = registry
        .chat_stream(request, CancellationToken::new())
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    let seen = server.await.unwrap();
    assert_eq!(seen.len(), 2);
    for (headers, body) in seen {
        assert!(headers.starts_with("POST /v1/chat/completions "));
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("authorization: bearer summary-first")
        );
        assert_eq!(body["stream"], false);
        assert_eq!(body["max_tokens"], 8192);
        assert_eq!(body["reasoning_effort"], "low");
        assert!(body.get("stream_options").is_none());
    }
    assert!(events.iter().any(|event| matches!(event, ProviderEvent::TextDelta { text } if text == "<summary>keep this</summary>")));
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ProviderEvent::Finish { usage: None, .. }))
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ProviderEvent::UsageUpdate { .. }))
    );
}
