//! Actual Read → engine projection → local provider wire, without credentials.
#![cfg(feature = "image-budget")]

use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zk_db::Db;
use zk_engine::{Engine, MessageSink};
use zk_llm::{ApiKey, OpenAiCompatProvider, ProviderConfig};
use zk_protocol::ServerMessage;

#[derive(Default)]
struct Sink(Mutex<Vec<ServerMessage>>);
impl MessageSink for Sink {
    fn push<'a>(&'a self, _: &'a str, event: ServerMessage) -> BoxFuture<'a, ()> {
        self.0.lock().unwrap().push(event);
        Box::pin(async {})
    }
}

async fn read_body(socket: &mut tokio::net::TcpStream) -> Value {
    let mut bytes = Vec::new();
    let end = loop {
        let mut buffer = [0; 8192];
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let len = String::from_utf8_lossy(&bytes[..end])
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        })
        .unwrap();
    while bytes.len() < end + len {
        let mut buffer = [0; 8192];
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&buffer[..count]);
    }
    serde_json::from_slice(&bytes[end..end + len]).unwrap()
}

fn tool_call(index: usize, name: &str, args: &Value) -> Value {
    json!({"index":index,"id":format!("call-{index}"),"type":"function","function":{"name":name,"arguments":args.to_string()}})
}

fn image_urls(request: &Value) -> Vec<String> {
    request["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|message| message["content"].as_array())
        .flatten()
        .filter_map(|part| part["image_url"]["url"].as_str().map(str::to_owned))
        .collect()
}

fn image_sources(root: &std::path::Path) -> [std::path::PathBuf; 3] {
    let image = image::RgbImage::from_pixel(2, 2, image::Rgb([20, 50, 90]));
    let mut encoded = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut encoded, image::ImageFormat::Bmp)
        .unwrap();
    let mut bytes = encoded.into_inner();
    let first = root.join("first.bmp");
    let second = root.join("second.bmp");
    std::fs::write(&first, &bytes).unwrap();
    // A reserved header byte changes source identity, without changing pixels.
    bytes[6] = 1;
    std::fs::write(&second, &bytes).unwrap();
    let webp = root.join("third.webp");
    image
        .save_with_format(&webp, image::ImageFormat::WebP)
        .unwrap();
    [first, second, webp]
}

fn response_for(turn: usize, paths: &[std::path::PathBuf; 3]) -> String {
    let (delta, finish) = match turn {
        0 => (
            json!({"tool_calls":[tool_call(0,"Read",&json!({"file_path":paths[0]})),tool_call(1,"Read",&json!({"file_path":paths[1]})),tool_call(2,"Read",&json!({"file_path":paths[2]}))]}),
            "tool_calls",
        ),
        1 => (
            json!({"tool_calls":[tool_call(3,"Echo",&json!({"text":"continue"}))]}),
            "tool_calls",
        ),
        _ => (json!({"content":"Images inspected"}), "stop"),
    };
    let chunk = json!({"choices":[{"delta":delta,"finish_reason":finish}],"usage":{"prompt_tokens":100,"completion_tokens":10}});
    let body = format!("data: {chunk}\n\ndata: [DONE]\n\n");
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

#[tokio::test]
async fn actual_read_confirms_all_source_identities_only_after_successful_delivery() {
    let directory = std::env::temp_dir().join(format!("zk-image-wire-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let directory = directory.canonicalize().unwrap();
    let paths = image_sources(&directory);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let captured = Arc::new(Mutex::new(Vec::new()));
    let request_log = captured.clone();
    let worker = tokio::spawn(async move {
        for turn in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_body(&mut socket).await;
            request_log.lock().unwrap().push(body);
            let response = response_for(turn, &paths);
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    // Loopback fixtures must not pass through an ambient HTTP proxy.
    let provider = OpenAiCompatProvider::with_client(
        ProviderConfig::new(
            "bailian",
            base,
            ApiKey::new("local-fixture"),
            "qwen3.8-max-0902",
            vec!["qwen3.8-max-0902".into()],
        ),
        reqwest::Client::builder().no_proxy().build().unwrap(),
    );
    let db = Db::open_in_memory().unwrap();
    let session = db
        .create_session("qwen3.8-max-0902", directory.to_str().unwrap())
        .await
        .unwrap();
    let mut tools = zk_tools::ToolRegistry::new();
    tools.register(Arc::new(zk_tools::ReadFileTool));
    tools.register(Arc::new(zk_tools::EchoTool));
    let sink = Arc::new(Sink::default());
    let engine = Arc::new(Engine::with_tools(
        db.clone(),
        Arc::new(provider),
        sink.clone(),
        Arc::new(tools),
    ));
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        engine.run_user_message(session.id.clone(), "Inspect both images".into()),
    )
    .await
    .unwrap();
    worker.await.unwrap();
    {
        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(image_urls(&requests[0]).is_empty());
        let delivered = image_urls(&requests[1]);
        assert_eq!(delivered.len(), 3);
        assert!(delivered[2].starts_with("data:image/webp;base64,"));
        assert_eq!(delivered[0], delivered[1]);
        assert!(
            image_urls(&requests[2]).is_empty(),
            "both original sources must be confirmed despite equal PNG payload"
        );
        assert!(
            !serde_json::to_string(&*requests)
                .unwrap()
                .contains("__zkTrustedImageProducer")
        );
    }

    let records = db.get_session(&session.id).await.unwrap().unwrap().messages;
    let replay = zk_engine::engine::history_to_chat_messages(&records);
    let retained: Vec<_> = replay
        .iter()
        .filter_map(|message| message.metadata.as_ref())
        .filter_map(|meta| meta["imageSourceDigests"].as_array())
        .flatten()
        .collect();
    assert_eq!(
        retained.len(),
        3,
        "delivery dedup must not erase durable original identities"
    );
    assert_ne!(retained[0], retained[1]);
    assert!(
        !serde_json::to_string(&*sink.0.lock().unwrap())
            .unwrap()
            .contains("__zkTrustedImageProducer")
    );
    std::fs::remove_dir_all(directory).unwrap();
}
