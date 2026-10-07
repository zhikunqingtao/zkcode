//! Real local HTTP transport exercises final image projection, without provider credentials.
use base64::Engine as _;
use futures::StreamExt as _;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use zk_llm::{
    AnthropicProvider, ApiKey, ChatMessage, ChatProvider, ChatRequest, ImageSource,
    OpenAiCompatProvider, ProviderConfig, ProviderEvent,
};

fn image_bytes() -> Vec<u8> {
    let mut state = 19u32;
    let image = image::RgbImage::from_fn(640, 640, |_, _| {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        let bytes = state.to_be_bytes();
        image::Rgb([bytes[1], bytes[2], bytes[3]])
    });
    let mut output = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut output, image::ImageFormat::Png)
        .unwrap();
    assert!(output.get_ref().len() > 1_125_000);
    output.into_inner()
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<(String, Vec<u8>)> {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0; 8192];
        let count = socket.read(&mut chunk).await.ok()?;
        if count == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let header = String::from_utf8_lossy(&bytes[..header_end]);
    let path = header.split_whitespace().nth(1)?.to_owned();
    let content_length = header
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let mut chunk = [0; 8192];
        let count = socket.read(&mut chunk).await.ok()?;
        if count == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    Some((path, bytes[header_end..].to_vec()))
}

async fn fixture(
    png: Vec<u8>,
) -> (
    String,
    Arc<Mutex<Vec<(String, Value)>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let captured = Arc::new(Mutex::new(Vec::new()));
    let observations = captured.clone();
    let server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let Some((path, body)) = read_request(&mut socket).await else {
                continue;
            };
            observations.lock().unwrap().push((
                path.clone(),
                serde_json::from_slice(&body).unwrap_or(Value::Null),
            ));
            let payload = match path.as_str() {
                "/image" => png.clone(),
                "/v1/messages" => b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".to_vec(),
                "/v1/responses" => b"data: {\"type\":\"response.completed\",\"response\":{\"output\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n".to_vec(),
                _ => b"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n".to_vec(),
            };
            let mime = if path == "/image" {
                "image/png"
            } else {
                "text/event-stream"
            };
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                payload.len()
            );
            socket.write_all(header.as_bytes()).await.unwrap();
            socket.write_all(&payload).await.unwrap();
        }
    });
    (base, captured, server)
}

#[tokio::test]
async fn real_chat_anthropic_and_responses_preserve_image_bytes_without_internal_identity() {
    let png = image_bytes();
    let (base, observed, server) = fixture(png.clone()).await;
    for (name, model, expected_path) in [
        ("kimi-code", "kimi-k3", "/v1/chat/completions"),
        ("anthropic", "claude-sonnet-4-6", "/v1/messages"),
        ("zenmux", "openai/gpt-6-astra", "/v1/responses"),
    ] {
        let config = ProviderConfig::new(
            name,
            if name == "anthropic" {
                base.clone()
            } else {
                format!("{base}/v1")
            },
            ApiKey::new("local-fixture"),
            model,
            vec![model.into()],
        );
        let provider: Box<dyn ChatProvider> = if name == "anthropic" {
            Box::new(AnthropicProvider::new(config).unwrap())
        } else {
            Box::new(OpenAiCompatProvider::new(config).unwrap())
        };
        let mut request = ChatRequest::new(model);
        request.current_user_message_id = Some("immutable-current-identity".into());
        request.messages.push(ChatMessage::user_with_images("inspect", vec![ImageSource {
            media_type: "image/png".into(),
            data: if name == "kimi-code" { None } else { Some(base64::engine::general_purpose::STANDARD.encode(&png)) },
            url: if name == "kimi-code" { Some(format!("{base}/image")) } else { None },
        }]).with_metadata(Some(json!({"sourceMessageId":"immutable-current-identity","imageSourceDigests":["private-source-digest"],"transientImages":false}))));
        let original = request.clone();
        let events: Vec<_> = provider
            .chat_stream(request, CancellationToken::new())
            .unwrap()
            .collect()
            .await;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ProviderEvent::Finish { .. })),
            "{name}: {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ProviderEvent::Error { .. })),
            "{name}: {events:?}"
        );
        let capture = observed.lock().unwrap();
        let (path, body) = capture.last().unwrap();
        assert_eq!(path, expected_path);
        assert_image_body(name, body, &png, &base);
        if name == "kimi-code" {
            assert!(original.messages[0].images[0].data.is_none());
        }
    }
    assert_eq!(
        observed
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path == "/image")
            .count(),
        1
    );
    server.abort();
}

#[tokio::test]
async fn cancellation_drops_active_image_download_before_any_provider_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (path, _) = read_request(&mut socket).await.unwrap();
        assert_eq!(path, "/slow-image");
        started_tx.send(()).unwrap();
        let mut byte = [0];
        tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
            .await
            .unwrap()
            .unwrap()
    });
    let config = ProviderConfig::new(
        "kimi-code",
        format!("{base}/v1"),
        ApiKey::new("local-fixture"),
        "kimi-k3",
        vec!["kimi-k3".into()],
    );
    let provider = OpenAiCompatProvider::new(config).unwrap();
    let mut request = ChatRequest::new("kimi-k3");
    request.current_user_message_id = Some("cancel-image-owner".into());
    request.messages.push(ChatMessage::user_with_images(
        "inspect",
        vec![ImageSource {
            media_type: "image/png".into(),
            data: None,
            url: Some(format!("{base}/slow-image")),
        }],
    ));
    let cancel = CancellationToken::new();
    let stream = provider.chat_stream(request, cancel.clone()).unwrap();
    let driver = tokio::spawn(async move { stream.collect::<Vec<_>>().await });
    started_rx.await.unwrap();
    cancel.cancel();
    let events = tokio::time::timeout(Duration::from_secs(1), driver)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ProviderEvent::Finish { .. }))
    );
    assert_eq!(
        server.await.unwrap(),
        0,
        "cancelled download must close its actual socket"
    );
}

#[tokio::test]
async fn chunked_image_body_over_limit_closes_download_before_any_provider_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        assert_eq!(
            read_request(&mut socket).await.unwrap().0,
            "/oversize-image"
        );
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
        let chunk = vec![0; 64 * 1024];
        for _ in 0..160 {
            socket.write_all(b"10000\r\n").await.unwrap();
            socket.write_all(&chunk).await.unwrap();
            socket.write_all(b"\r\n").await.unwrap();
        }
        // Exactly 10 MiB has already streamed; one more byte must fail without
        // waiting for the chunked terminator or a declared Content-Length.
        socket.write_all(b"1\r\nx\r\n").await.unwrap();
        let mut byte = [0];
        match tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
            .await
            .unwrap()
        {
            Ok(0) => {}
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
            other => panic!("oversized download socket remained open: {other:?}"),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "invalid current image must not trigger a provider request"
        );
    });
    let provider = OpenAiCompatProvider::new(ProviderConfig::new(
        "kimi-code",
        format!("{base}/v1"),
        ApiKey::new("local-fixture"),
        "kimi-k3",
        vec!["kimi-k3".into()],
    ))
    .unwrap();
    let mut request = ChatRequest::new("kimi-k3");
    request.current_user_message_id = Some("chunked-oversize-owner".into());
    request.messages.push(ChatMessage::user_with_images(
        "inspect",
        vec![ImageSource {
            media_type: "image/png".into(),
            data: None,
            url: Some(format!("{base}/oversize-image")),
        }],
    ));
    let events = tokio::time::timeout(
        Duration::from_secs(10),
        provider
            .chat_stream(request, CancellationToken::new())
            .unwrap()
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ProviderEvent::Error { error }
        if error.to_string().contains("IMAGE_SIZE_EXCEEDED"))),
        "{events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ProviderEvent::Finish { .. }))
    );
    server.await.unwrap();
}

fn assert_image_body(name: &str, body: &Value, png: &[u8], base: &str) {
    let encoded = match name {
        "anthropic" => body["messages"][0]["content"][1]["source"]["data"]
            .as_str()
            .unwrap(),
        "zenmux" => {
            body["input"][0]["content"][1]["image_url"]
                .as_str()
                .unwrap()
                .split_once(',')
                .unwrap()
                .1
        }
        _ => {
            body["messages"][0]["content"][1]["image_url"]["url"]
                .as_str()
                .unwrap()
                .split_once(',')
                .unwrap()
                .1
        }
    };
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .unwrap(),
        png
    );
    let body = body.to_string();
    for private in [
        "immutable-current-identity",
        "private-source-digest",
        "imageSourceDigests",
        "sourceMessageId",
        "transientImages",
        &format!("{base}/image"),
    ] {
        assert!(!body.contains(private), "{name}: internal identity leaked");
    }
}
