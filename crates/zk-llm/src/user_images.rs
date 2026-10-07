//! Request-copy image preparation: preserve the current turn, degrade only history.
use crate::{ChatRequest, ImageSource, ProviderError, Role, payload_guard};
use base64::Engine as _;
use http_body_util::BodyExt as _;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const MAX_BYTES: usize = 10 * 1024 * 1024;
const MAX_IMAGES: usize = 8;
const CACHE_BYTES: usize = 32 * 1024 * 1024;
const CACHE_TTL: Duration = Duration::from_mins(30);

struct CachedImage {
    owner: String,
    url: String,
    image: ImageSource,
    saved: Instant,
    weight: usize,
}

#[derive(Default)]
struct ImageCache {
    entries: VecDeque<CachedImage>,
    weight: usize,
}

impl ImageCache {
    fn expire(&mut self) {
        self.entries
            .retain(|entry| entry.saved.elapsed() < CACHE_TTL);
        self.weight = self.entries.iter().map(|entry| entry.weight).sum();
    }

    fn get(&mut self, owner: &str, url: &str) -> Option<ImageSource> {
        self.expire();
        let index = self
            .entries
            .iter()
            .position(|entry| entry.owner == owner && entry.url == url)?;
        let entry = self.entries.remove(index)?;
        let image = entry.image.clone();
        self.entries.push_back(entry);
        Some(image)
    }

    fn put(&mut self, owner: &str, url: &str, image: &ImageSource) {
        self.expire();
        let weight = owner.len() + url.len() + image.data.as_ref().map_or(0, String::len);
        if weight > CACHE_BYTES {
            return;
        }
        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.owner == owner && entry.url == url)
            && let Some(entry) = self.entries.remove(index)
        {
            self.weight -= entry.weight;
        }
        while self.weight.saturating_add(weight) > CACHE_BYTES {
            let Some(entry) = self.entries.pop_front() else {
                break;
            };
            self.weight -= entry.weight;
        }
        self.weight += weight;
        self.entries.push_back(CachedImage {
            owner: owner.into(),
            url: url.into(),
            image: image.clone(),
            saved: Instant::now(),
            weight,
        });
    }
}

fn cache() -> &'static Mutex<ImageCache> {
    static CACHE: OnceLock<Mutex<ImageCache>> = OnceLock::new();
    CACHE.get_or_init(Mutex::default)
}

fn invalid(message: &str) -> ProviderError {
    ProviderError::Preflight {
        message: message.into(),
    }
}

fn mandatory(request: &ChatRequest, message: usize) -> bool {
    let message = &request.messages[message];
    let transient = message
        .metadata
        .as_ref()
        .is_some_and(|meta| meta["syntheticToolImages"] == true);
    if transient {
        return false;
    }
    let Some(current) = request.current_user_message_id.as_deref() else {
        // Direct provider callers without an Engine boundary must not silently
        // lose any input. Runtime requests supply the immutable Run-entry ID.
        return true;
    };
    if message.role != Role::User {
        return false;
    }
    message
        .metadata
        .as_ref()
        .and_then(|meta| meta["sourceMessageId"].as_str())
        .is_none_or(|id| id == current)
}

/// Record only identities still represented by images in the final request copy.
/// This is attempt-local evidence, not confirmation that the provider accepted it.
pub(crate) fn record_dispatched_image_sources(request: &ChatRequest) {
    let sources = request
        .messages
        .iter()
        .filter_map(|message| {
            let metadata = message.metadata.as_ref()?;
            (metadata["syntheticToolImages"] == true).then_some((message, metadata))
        })
        .flat_map(|(message, metadata)| {
            metadata["imageSourceDigests"]
                .as_array()
                .into_iter()
                .flatten()
                .take(message.images.len())
                .filter_map(|value| value.as_str().map(str::to_owned))
        })
        .collect();
    // Replacing prevents a failed physical attempt from leaking identities into
    // a successful fallback whose final image projection is different.
    *request
        .delivered_image_sources
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = sources;
}

pub(crate) async fn prepare_inline_images(
    request: ChatRequest,
    provider: &str,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<ChatRequest, ProviderError> {
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(ProviderError::Cancelled),
        result = prepare_inline_images_uncancelled(request, provider) => result,
    }
}

async fn prepare_inline_images_uncancelled(
    mut request: ChatRequest,
    provider: &str,
) -> Result<ChatRequest, ProviderError> {
    request
        .image_notices
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
    let capabilities = crate::capabilities_for(&request.model);
    if provider != "kimi-code"
        && request.model != "kimi-k3"
        && capabilities.image_input_mode != crate::models::ImageInputMode::Base64Only
    {
        return Ok(request);
    }
    let mut candidates = Vec::new();
    for (mi, message) in request.messages.iter().enumerate().rev() {
        for ii in 0..message.images.len() {
            candidates.push((mi, ii, mandatory(&request, mi)));
        }
    }
    if candidates.is_empty() {
        return Ok(request);
    }
    // Stable sort preserves newest-first ordering among historical images.
    candidates.sort_by_key(|(_, _, required)| !required);
    let max_images = MAX_IMAGES.min(capabilities.max_images as usize);
    if candidates
        .iter()
        .filter(|(_, _, required)| *required)
        .count()
        > max_images
    {
        return Err(invalid(
            "IMAGE_COUNT_EXCEEDED: current request images exceed model limit; originals preserved",
        ));
    }
    let available = available_tokens(&request);
    let mut used_tokens = text_tokens(&request);
    let mut used_images = 0;
    let mut omitted = Vec::new();
    // A Run's current message identity is globally unique. Caching only within
    // that boundary cannot resurrect a URL across sessions or later permission
    // changes. Unattributed direct callers do not cache at all.
    let owner = request.current_user_message_id.clone();
    for (mi, ii, required) in candidates {
        let prepared = if used_images >= max_images {
            Err(invalid("IMAGE_COUNT_EXCEEDED"))
        } else {
            prepare_image(&request.messages[mi].images[ii], owner.as_deref()).await
        };
        let prepared = prepared.and_then(|image| {
            let tokens =
                payload_guard::inline_image_tokens(image.data.as_deref().unwrap_or_default())?
                    .saturating_add(16);
            if used_tokens.saturating_add(tokens) > available {
                return Err(invalid("IMAGE_CONTEXT_BUDGET_EXCEEDED"));
            }
            used_tokens = used_tokens.saturating_add(tokens);
            Ok(image)
        });
        match prepared {
            Ok(image) => {
                request.messages[mi].images[ii] = image;
                used_images += 1;
            }
            Err(error) if required => {
                return Err(invalid(&format!(
                    "IMAGE_INPUT_INVALID: {error}; current originals preserved"
                )));
            }
            Err(error) => {
                let reason = omission_reason(&error);
                let _ = write!(request.messages[mi].content, "\n[历史图片已省略：{reason}]");
                let notice = format!("历史图片本轮已省略：{reason}。原附件仍保留在会话中。");
                let mut notices = request
                    .image_notices
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !notices.contains(&notice) {
                    notices.push(notice);
                }
                omitted.push((mi, ii));
            }
        }
    }
    omitted.sort_unstable();
    for (mi, ii) in omitted.into_iter().rev() {
        request.messages[mi].images.remove(ii);
        if let Some(digests) = request.messages[mi]
            .metadata
            .as_mut()
            .filter(|metadata| metadata["syntheticToolImages"] == true)
            .and_then(|metadata| metadata.get_mut("imageSourceDigests"))
            .and_then(serde_json::Value::as_array_mut)
            && ii < digests.len()
        {
            digests.remove(ii);
        }
    }
    ensure_inline_image_budget(&request)?;
    Ok(request)
}

fn omission_reason(error: &ProviderError) -> &'static str {
    // Never echo URLs, query strings, network diagnostics or untrusted errors.
    let text = error.to_string();
    if text.contains("COUNT") {
        "超出本次图片数量上限"
    } else if text.contains("BUDGET") {
        "超出本次图片上下文预算"
    } else {
        "图片不可用或格式无效"
    }
}

async fn prepare_image(
    image: &ImageSource,
    owner: Option<&str>,
) -> Result<ImageSource, ProviderError> {
    let bytes = if let Some(encoded) = &image.data {
        if encoded.len() > MAX_BYTES.div_ceil(3) * 4 {
            return Err(invalid("IMAGE_SIZE_EXCEEDED"));
        }
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| invalid("IMAGE_BASE64_INVALID"))?
    } else {
        let url = image
            .url
            .as_deref()
            .ok_or_else(|| invalid("IMAGE_SOURCE_MISSING"))?;
        let parsed = reqwest::Url::parse(url).map_err(|_| invalid("IMAGE_URL_INVALID"))?;
        if !matches!(parsed.scheme(), "http" | "https")
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return Err(invalid("IMAGE_URL_INVALID"));
        }
        // The Engine has already admitted this URL under its origin policy.
        // Redirects are disabled, and no arbitrary auth headers are forwarded.
        if let Some(owner) = owner
            && let Ok(mut cache) = cache().lock()
            && let Some(image) = cache.get(owner, url)
        {
            return Ok(image);
        }
        download_image(&parsed).await?
    };
    let media_type = payload_guard::image_media_type(&bytes)?;
    let prepared = ImageSource {
        media_type: media_type.into(),
        data: Some(base64::engine::general_purpose::STANDARD.encode(bytes)),
        url: None,
    };
    if image.data.is_none()
        && let (Some(owner), Some(url)) = (owner, image.url.as_deref())
        && let Ok(mut cache) = cache().lock()
    {
        cache.put(owner, url, &prepared);
    }
    Ok(prepared)
}

struct ImageConnection(tokio::task::JoinHandle<()>);
impl Drop for ImageConnection {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn download_image(url: &reqwest::Url) -> Result<Vec<u8>, ProviderError> {
    // Own the actual connection driver. Dropping a pooled reqwest response
    // future can leave its background connection waiting for response headers;
    // cancellation here aborts the driver and closes the socket immediately.
    tokio::time::timeout(Duration::from_secs(30), async {
        let host = url.host_str().ok_or_else(|| invalid("IMAGE_URL_INVALID"))?;
        let hostname = host.trim_start_matches('[').trim_end_matches(']');
        let port = url
            .port_or_known_default()
            .ok_or_else(|| invalid("IMAGE_URL_INVALID"))?;
        let stream = tokio::net::TcpStream::connect((hostname, port))
            .await
            .map_err(|_| invalid("IMAGE_FETCH_FAILED"))?;
        if url.scheme() == "https" {
            let connector = native_tls::TlsConnector::new()
                .map_err(|_| invalid("IMAGE_FETCH_CLIENT_UNAVAILABLE"))?;
            let stream = tokio_native_tls::TlsConnector::from(connector)
                .connect(hostname, stream)
                .await
                .map_err(|_| invalid("IMAGE_FETCH_FAILED"))?;
            exchange_image(stream, url).await
        } else {
            exchange_image(stream, url).await
        }
    })
    .await
    .map_err(|_| invalid("IMAGE_FETCH_DEADLINE_EXCEEDED"))?
}

async fn exchange_image<T>(stream: T, url: &reqwest::Url) -> Result<Vec<u8>, ProviderError>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .map_err(|_| invalid("IMAGE_FETCH_FAILED"))?;
    let _driver = ImageConnection(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let host = url.host_str().ok_or_else(|| invalid("IMAGE_URL_INVALID"))?;
    let authority = url
        .port()
        .map_or_else(|| host.to_owned(), |port| format!("{host}:{port}"));
    let target = url.query().map_or_else(
        || url.path().to_owned(),
        |query| format!("{}?{query}", url.path()),
    );
    let request = hyper::Request::builder()
        .uri(target)
        .header(hyper::header::HOST, authority)
        .header(hyper::header::CONNECTION, "close")
        .body(http_body_util::Empty::<bytes::Bytes>::new())
        .map_err(|_| invalid("IMAGE_URL_INVALID"))?;
    let response = sender
        .send_request(request)
        .await
        .map_err(|_| invalid("IMAGE_FETCH_FAILED"))?;
    if !response.status().is_success()
        || response
            .headers()
            .get(hyper::header::CONTENT_LENGTH)
            .and_then(|n| n.to_str().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .is_some_and(|n| n > MAX_BYTES as u64)
    {
        return Err(invalid("IMAGE_FETCH_INVALID_RESPONSE"));
    }
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| invalid("IMAGE_FETCH_FAILED"))?;
        if let Some(chunk) = frame.data_ref() {
            if bytes.len().saturating_add(chunk.len()) > MAX_BYTES {
                return Err(invalid("IMAGE_SIZE_EXCEEDED"));
            }
            bytes.extend_from_slice(chunk);
        }
    }
    Ok(bytes)
}

fn available_tokens(request: &ChatRequest) -> u64 {
    let capacity = u64::from(crate::capabilities_for(&request.model).context_window);
    capacity
        .saturating_sub(u64::from(request.max_tokens))
        .saturating_sub(capacity.div_ceil(20).max(2048))
}

fn text_tokens(request: &ChatRequest) -> u64 {
    let mut tokens = request
        .system_text()
        .map_or(0, |text| text.len() as u64)
        .div_ceil(3);
    for message in &request.messages {
        tokens = tokens.saturating_add((message.content.len() as u64).div_ceil(3));
        for call in &message.tool_calls {
            tokens = tokens.saturating_add((call.arguments.len() as u64).div_ceil(3));
        }
    }
    for tool in &request.tools {
        tokens = tokens.saturating_add(
            (tool.parameters.to_string().len() as u64 + tool.description.len() as u64).div_ceil(3),
        );
    }
    tokens
}

pub(crate) fn ensure_inline_image_budget(request: &ChatRequest) -> Result<(), ProviderError> {
    let mut estimated = text_tokens(request);
    for image in request.messages.iter().flat_map(|message| &message.images) {
        if let Some(data) = &image.data {
            estimated = estimated.saturating_add(payload_guard::inline_image_tokens(data)?);
        }
    }
    if estimated > available_tokens(request) {
        return Err(invalid(
            "IMAGE_BUDGET_EXCEEDED: converted inline images exceed the model input budget",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::prepare_inline_images_uncancelled as prepare_inline_images;
    use super::*;
    use crate::ChatMessage;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn png() -> Vec<u8> {
        let image = image::DynamicImage::new_rgb8(128, 128);
        let mut cursor = std::io::Cursor::new(Vec::new());
        image
            .write_to(&mut cursor, image::ImageFormat::Png)
            .unwrap();
        cursor.into_inner()
    }

    fn inline(data: &[u8]) -> ImageSource {
        ImageSource {
            media_type: "image/png".into(),
            data: Some(base64::engine::general_purpose::STANDARD.encode(data)),
            url: None,
        }
    }

    fn remote(url: &str) -> ImageSource {
        ImageSource {
            media_type: "image/png".into(),
            data: None,
            url: Some(url.into()),
        }
    }

    fn message(id: &str, images: Vec<ImageSource>) -> ChatMessage {
        ChatMessage::user_with_images("inspect", images)
            .with_metadata(Some(json!({"sourceMessageId": id})))
    }

    fn request(id: &str) -> ChatRequest {
        let mut request = ChatRequest::new("kimi-k3");
        request.current_user_message_id = Some(id.into());
        request
    }

    #[tokio::test]
    async fn dispatch_evidence_tracks_retained_indices_and_replaces_failed_attempt_sources() {
        let image = inline(&png());
        let mut input = request("current");
        input.messages.push(
            ChatMessage::user_with_images(
                "tool",
                vec![inline(b"invalid old image"), image.clone(), image.clone()],
            )
            .with_metadata(Some(
                json!({"syntheticToolImages":true,"imageSourceDigests":["bad","kept-a","kept-b"]}),
            )),
        );
        input
            .messages
            .push(message("current", vec![image.clone(); 6]));
        let prepared = prepare_inline_images(input.clone(), "kimi-code")
            .await
            .unwrap();
        assert_eq!(prepared.messages[0].images.len(), 2);
        assert_eq!(
            prepared.messages[0].metadata.as_ref().unwrap()["imageSourceDigests"],
            json!(["kept-a", "kept-b"])
        );
        assert_eq!(input.image_notices.lock().unwrap().len(), 1);
        assert!(input.image_notices.lock().unwrap()[0].contains("图片不可用或格式无效"));
        record_dispatched_image_sources(&prepared);
        assert_eq!(
            *input.delivered_image_sources.lock().unwrap(),
            std::collections::HashSet::from(["kept-a".into(), "kept-b".into()])
        );
        let mut fallback = input.clone();
        fallback.messages = vec![message("current", vec![image])];
        let fallback = prepare_inline_images(fallback, "kimi-code").await.unwrap();
        assert!(
            input.image_notices.lock().unwrap().is_empty(),
            "failed attempt notices do not leak into fallback"
        );
        record_dispatched_image_sources(&fallback);
        assert!(input.delivered_image_sources.lock().unwrap().is_empty());
        assert_eq!(
            input.messages[0].images.len(),
            3,
            "durable input was not mutated"
        );
    }

    #[tokio::test]
    async fn current_images_take_priority_and_originals_remain_unchanged() {
        let mut input = request("current");
        input
            .messages
            .push(message("old", vec![inline(b"invalid history")]));
        input
            .messages
            .push(message("current", vec![inline(&png()); 8]));
        input.messages.push(
            ChatMessage::user_with_images("tool", vec![inline(b"invalid tool image")])
                .with_metadata(Some(json!({"syntheticToolImages": true}))),
        );
        let original = input.clone();
        let prepared = prepare_inline_images(input, "kimi-code").await.unwrap();
        assert_eq!(prepared.messages[1].images.len(), 8);
        assert!(prepared.messages[0].images.is_empty());
        assert!(prepared.messages[2].images.is_empty());
        assert!(prepared.messages[0].content.contains("图片数量上限"));
        assert_eq!(original.messages[0].images.len(), 1);
        assert_eq!(
            original.messages[1].images[0].data,
            prepared.messages[1].images[0].data
        );
    }

    #[tokio::test]
    async fn current_invalid_or_excess_images_fail_without_degrading_current() {
        let mut input = request("current");
        input
            .messages
            .push(message("current", vec![inline(b"invalid")]));
        assert!(
            prepare_inline_images(input, "kimi-code")
                .await
                .unwrap_err()
                .to_string()
                .contains("IMAGE_INPUT_INVALID")
        );
        let mut input = request("current");
        input
            .messages
            .push(message("current", vec![inline(&png()); 9]));
        assert!(
            prepare_inline_images(input, "kimi-code")
                .await
                .unwrap_err()
                .to_string()
                .contains("IMAGE_COUNT_EXCEEDED")
        );
        let mut direct = ChatRequest::new("kimi-k3");
        direct
            .messages
            .push(message("old", vec![inline(b"invalid")]));
        direct.messages.push(message("new", vec![inline(&png())]));
        assert!(
            prepare_inline_images(direct, "kimi-code").await.is_err(),
            "missing immutable boundary must fail closed"
        );
    }

    #[tokio::test]
    async fn history_budget_omits_oldest_first_and_keeps_current() {
        let mut input = request("current");
        let capacity = crate::capabilities_for("kimi-k3").context_window;
        input.max_tokens = capacity - capacity.div_ceil(20).max(2048) - 2500;
        input.messages.push(message("oldest", vec![inline(&png())]));
        input.messages.push(message("recent", vec![inline(&png())]));
        input
            .messages
            .push(message("current", vec![inline(&png())]));
        let prepared = prepare_inline_images(input, "kimi-code").await.unwrap();
        assert!(prepared.messages[0].images.is_empty());
        assert!(prepared.messages[0].content.contains("图片上下文预算"));
        assert_eq!(prepared.messages[1].images.len(), 1);
        assert_eq!(prepared.messages[2].images.len(), 1);
    }

    async fn server() -> (String, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let paths = Arc::new(Mutex::new(Vec::new()));
        let captured = paths.clone();
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = vec![0; 4096];
                let n = socket.read(&mut request).await.unwrap();
                if n == 0 {
                    continue;
                }
                let request = String::from_utf8_lossy(&request[..n]);
                let path = request.split_whitespace().nth(1).unwrap().to_owned();
                captured.lock().unwrap().push(path.clone());
                let (status, extra, body) = match path.as_str() {
                    "/valid" => ("200 OK", "", png()),
                    "/redirect" => ("302 Found", "Location: /valid\r\n", Vec::new()),
                    "/large" => {
                        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10485761\r\nConnection: close\r\n\r\n").await.unwrap();
                        continue;
                    }
                    _ => ("404 Not Found", "", Vec::new()),
                };
                let header = format!(
                    "HTTP/1.1 {status}\r\n{extra}Content-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
        });
        (url, paths, task)
    }

    #[tokio::test]
    async fn real_http_history_failure_continues_and_cache_is_run_scoped() {
        let (base, paths, server) = server().await;
        let mut input = request("http-owner-a");
        input.messages.push(message(
            "old",
            vec![remote(&format!("{base}/missing?secret=hidden"))],
        ));
        input.messages.push(message(
            "http-owner-a",
            vec![remote(&format!("{base}/valid"))],
        ));
        let original = input.clone();
        let prepared = prepare_inline_images(input.clone(), "kimi-code")
            .await
            .unwrap();
        assert!(prepared.messages[0].images.is_empty());
        assert!(!prepared.messages[0].content.contains("secret"));
        assert_eq!(
            paths.lock().unwrap().as_slice(),
            ["/valid", "/missing?secret=hidden"]
        );
        assert!(original.messages[1].images[0].data.is_none());
        assert!(prepared.messages[1].images[0].url.is_none());
        prepare_inline_images(input, "kimi-code").await.unwrap();
        assert_eq!(
            paths
                .lock()
                .unwrap()
                .iter()
                .filter(|p| *p == "/valid")
                .count(),
            1
        );
        let mut other = request("http-owner-b");
        other.messages.push(message(
            "http-owner-b",
            vec![remote(&format!("{base}/valid"))],
        ));
        prepare_inline_images(other, "kimi-code").await.unwrap();
        assert_eq!(
            paths
                .lock()
                .unwrap()
                .iter()
                .filter(|p| *p == "/valid")
                .count(),
            2
        );
        // Failures are not cached as successes: the missing historical URL was tried twice.
        assert_eq!(
            paths
                .lock()
                .unwrap()
                .iter()
                .filter(|p| p.contains("missing"))
                .count(),
            2
        );
        server.abort();
    }

    #[tokio::test]
    async fn redirects_and_oversized_downloads_are_rejected_before_followup() {
        let (base, paths, server) = server().await;
        for route in ["redirect", "large"] {
            let mut input = request("refusal-owner");
            input.messages.push(message(
                "refusal-owner",
                vec![remote(&format!("{base}/{route}"))],
            ));
            assert!(
                prepare_inline_images(input, "kimi-code")
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("IMAGE_FETCH_INVALID_RESPONSE")
            );
        }
        assert_eq!(paths.lock().unwrap().as_slice(), ["/redirect", "/large"]);
        server.abort();
    }

    #[test]
    fn cache_weight_expiry_and_owner_isolation_are_bounded() {
        let mut cache = ImageCache::default();
        let image = ImageSource {
            media_type: "image/png".into(),
            data: Some("A".repeat(12 * 1024 * 1024)),
            url: None,
        };
        cache.put("a", "first", &image);
        cache.put("a", "second", &image);
        assert!(cache.get("b", "second").is_none());
        cache.put("a", "third", &image);
        assert!(cache.weight <= CACHE_BYTES);
        assert!(cache.get("a", "first").is_none());
        cache.entries.front_mut().unwrap().saved = Instant::now().checked_sub(CACHE_TTL).unwrap();
        assert!(cache.get("a", "second").is_none());
        assert!(cache.get("a", "third").is_some());
    }

    #[tokio::test]
    async fn url_capable_provider_keeps_native_image_representation() {
        let mut input = ChatRequest::new("qwen3.8-max-0902");
        input.messages.push(message(
            "original",
            vec![remote("https://trusted.invalid/original")],
        ));
        let prepared = prepare_inline_images(input.clone(), "dashscope")
            .await
            .unwrap();
        assert_eq!(prepared.messages, input.messages);
    }
}
