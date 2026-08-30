//! `DashScope` speech integration shared by the ASR and TTS REST handlers.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use futures::{StreamExt as _, future::BoxFuture};
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use url::Url;
use zk_llm::{ApiKeyRing, config::DASHSCOPE_BASE_URL};
use zk_mcp::McpCredentialResolver;

/// `DashScope` ASR accepts at most 10 MiB of Base64 data. Raw input is capped at
/// three quarters of that size to account for Base64 expansion.
pub(crate) const MAX_AUDIO_BYTES: usize = 10 * 1024 * 1024 * 3 / 4;
/// Transport guard for the complete multipart request, including boundaries
/// and part headers.
pub(crate) const MAX_MULTIPART_BYTES: usize = 9 * 1024 * 1024;

const ASR_MODEL: &str = "qwen3-asr-flash";
const TTS_MODEL: &str = "qwen3-tts-flash";
const TTS_VOICE: &str = "Cherry";
const TTS_LANGUAGE: &str = "Auto";
const TTS_ENDPOINT: &str =
    "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const REQUEST_TIMEOUT: Duration = Duration::from_mins(1);
const MAX_TTS_UTF16_UNITS: usize = 500;
const MAX_UPSTREAM_RESPONSE_BYTES: usize = 1024 * 1024;

/// Audio bytes accepted by the speech service after multipart extraction.
pub(crate) struct SpeechAudio {
    pub(crate) bytes: bytes::Bytes,
    pub(crate) mime_type: String,
}

/// Stable failure classes mapped by the REST layer without exposing provider
/// response bodies or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SpeechError {
    #[error("invalid speech input")]
    InvalidInput,
    #[error("speech payload is too large")]
    PayloadTooLarge,
    #[error("standard DashScope credentials are unavailable")]
    Unavailable,
    #[error("DashScope rate limit reached")]
    RateLimited,
    #[error("DashScope request timed out")]
    Timeout,
    #[error("DashScope request failed")]
    Upstream,
}

/// Unified ASR/TTS service port. The narrow boxed-future surface keeps handler
/// tests network-free without adding an async-trait dependency.
pub(crate) trait SpeechService: Send + Sync {
    fn is_available(&self) -> bool;

    fn recognize(&self, audio: SpeechAudio) -> BoxFuture<'_, Result<String, SpeechError>>;

    fn synthesize(&self, text: String) -> BoxFuture<'_, Result<String, SpeechError>>;
}

struct CachedKeyRing {
    source_fingerprint: [u8; 32],
    ring: ApiKeyRing,
}

/// Production `DashScope` client. Credentials are resolved for every operation;
/// the cached ring is rebuilt whenever a DB/environment CSV value changes.
pub(crate) struct DashScopeSpeechService {
    client: reqwest::Client,
    credentials: Arc<dyn McpCredentialResolver>,
    key_ring: Mutex<Option<CachedKeyRing>>,
    request_timeout: Duration,
}

impl DashScopeSpeechService {
    #[must_use]
    pub(crate) fn new(credentials: Arc<dyn McpCredentialResolver>) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("static DashScope HTTP client configuration is valid");
        Self {
            client,
            credentials,
            key_ring: Mutex::new(None),
            request_timeout: REQUEST_TIMEOUT,
        }
    }

    fn current_key_ring(&self) -> Option<ApiKeyRing> {
        let source = self
            .credentials
            .resolve(zk_mcp::security::DASHSCOPE_PROVIDER);
        let mut cached = match self.key_ring.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(source) = source else {
            *cached = None;
            return None;
        };
        let source_fingerprint: [u8; 32] = Sha256::digest(source.as_bytes()).into();
        if cached
            .as_ref()
            .is_none_or(|existing| existing.source_fingerprint != source_fingerprint)
        {
            let ring = ApiKeyRing::from_csv(&source);
            *cached = Some(CachedKeyRing {
                source_fingerprint,
                ring,
            });
        }
        cached
            .as_ref()
            .map(|entry| entry.ring.clone())
            .filter(|ring| !ring.is_empty())
    }

    async fn post_json(&self, endpoint: &str, body: &Value) -> Result<Value, SpeechError> {
        let ring = self.current_key_ring().ok_or(SpeechError::Unavailable)?;
        let operation = async {
            for _ in 0..ring.len() {
                let key = ring.next_key().ok_or(SpeechError::RateLimited)?;
                let response = self
                    .client
                    .post(endpoint)
                    .bearer_auth(key.expose())
                    .json(body)
                    .send()
                    .await
                    .map_err(|error| map_reqwest_error(&error))?;
                if response.status() == StatusCode::TOO_MANY_REQUESTS {
                    // Match the LLM provider's CSV key-ring behavior: cool the
                    // limited key and try the next member, if any.
                    ring.mark_rate_limited(&key);
                    continue;
                }
                if !response.status().is_success() {
                    return Err(SpeechError::Upstream);
                }
                if response
                    .content_length()
                    .is_some_and(|length| length > MAX_UPSTREAM_RESPONSE_BYTES as u64)
                {
                    return Err(SpeechError::Upstream);
                }
                let mut stream = response.bytes_stream();
                let mut bytes = Vec::new();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|error| map_reqwest_error(&error))?;
                    if bytes.len().saturating_add(chunk.len()) > MAX_UPSTREAM_RESPONSE_BYTES {
                        return Err(SpeechError::Upstream);
                    }
                    bytes.extend_from_slice(&chunk);
                }
                return serde_json::from_slice(&bytes).map_err(|_| SpeechError::Upstream);
            }
            Err(SpeechError::RateLimited)
        };
        tokio::time::timeout(self.request_timeout, operation)
            .await
            .map_err(|_| SpeechError::Timeout)?
    }
}

impl SpeechService for DashScopeSpeechService {
    fn is_available(&self) -> bool {
        self.current_key_ring().is_some()
    }

    fn recognize(&self, audio: SpeechAudio) -> BoxFuture<'_, Result<String, SpeechError>> {
        Box::pin(async move {
            validate_audio(&audio)?;
            let body = asr_request_body(&audio);
            let endpoint = format!(
                "{}/chat/completions",
                DASHSCOPE_BASE_URL.trim_end_matches('/')
            );
            let response = self.post_json(&endpoint, &body).await?;
            response
                .pointer("/choices/0/message/content")
                .and_then(Value::as_str)
                .filter(|text| !text.trim().is_empty())
                .map(str::to_owned)
                .ok_or(SpeechError::Upstream)
        })
    }

    fn synthesize(&self, text: String) -> BoxFuture<'_, Result<String, SpeechError>> {
        Box::pin(async move {
            if text.trim().is_empty() {
                return Err(SpeechError::InvalidInput);
            }
            let text = truncate_utf16(&text, MAX_TTS_UTF16_UNITS);
            let body = tts_request_body(&text);
            let response = self.post_json(TTS_ENDPOINT, &body).await?;
            let raw_url = response
                .pointer("/output/audio/url")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or(SpeechError::Upstream)?;
            validate_audio_url(raw_url)
        })
    }
}

fn validate_audio(audio: &SpeechAudio) -> Result<(), SpeechError> {
    if audio.bytes.is_empty() || !audio.mime_type.starts_with("audio/") {
        return Err(SpeechError::InvalidInput);
    }
    if audio.bytes.len() > MAX_AUDIO_BYTES {
        return Err(SpeechError::PayloadTooLarge);
    }
    Ok(())
}

fn asr_request_body(audio: &SpeechAudio) -> Value {
    let encoded = base64::engine::general_purpose::STANDARD.encode(&audio.bytes);
    let data_uri = format!("data:{};base64,{encoded}", audio.mime_type);
    json!({
        "model": ASR_MODEL,
        "messages": [{
            "role": "user",
            "content": [{
                "type": "input_audio",
                "input_audio": { "data": data_uri }
            }]
        }],
        "stream": false,
        "asr_options": { "enable_itn": true }
    })
}

fn tts_request_body(text: &str) -> Value {
    json!({
        "model": TTS_MODEL,
        "input": {
            "text": text,
            "voice": TTS_VOICE,
            "language_type": TTS_LANGUAGE
        }
    })
}

fn truncate_utf16(text: &str, max_units: usize) -> String {
    let boundary = text
        .char_indices()
        .scan(0usize, |used, (index, character)| {
            let next = *used + character.len_utf16();
            if next > max_units {
                None
            } else {
                *used = next;
                Some(index + character.len_utf8())
            }
        })
        .last()
        .unwrap_or(0);
    text[..boundary].to_owned()
}

fn validate_audio_url(raw: &str) -> Result<String, SpeechError> {
    let mut url = Url::parse(raw).map_err(|_| SpeechError::Upstream)?;
    let host = url.domain().ok_or(SpeechError::Upstream)?;
    let trusted_host = host == "aliyuncs.com" || host.ends_with(".aliyuncs.com");
    if !trusted_host || !url.username().is_empty() || url.password().is_some() {
        return Err(SpeechError::Upstream);
    }

    // DashScope currently returns a signed OSS URL with an `http` scheme.
    // Never hand that insecure URL to the browser: upgrade only the already
    // pinned default-port aliyuncs.com destination, then enforce HTTPS below.
    if url.scheme() == "http" && url.port_or_known_default() == Some(80) {
        url.set_scheme("https")
            .map_err(|()| SpeechError::Upstream)?;
    }
    if url.scheme() != "https" || url.port_or_known_default() != Some(443) {
        return Err(SpeechError::Upstream);
    }
    Ok(url.to_string())
}

fn map_reqwest_error(error: &reqwest::Error) -> SpeechError {
    if error.is_timeout() {
        SpeechError::Timeout
    } else {
        SpeechError::Upstream
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asr_body_uses_audio_data_uri_and_itn() {
        let body = asr_request_body(&SpeechAudio {
            bytes: bytes::Bytes::from_static(b"abc"),
            mime_type: "audio/webm".to_owned(),
        });
        assert_eq!(body["model"], ASR_MODEL);
        assert_eq!(body["stream"], false);
        assert_eq!(body["asr_options"]["enable_itn"], true);
        assert_eq!(
            body["messages"][0]["content"][0]["input_audio"]["data"],
            "data:audio/webm;base64,YWJj"
        );
    }

    #[test]
    fn tts_text_is_bounded_by_utf16_units_without_splitting_unicode() {
        let text = format!("{}😀tail", "a".repeat(499));
        let truncated = truncate_utf16(&text, MAX_TTS_UTF16_UNITS);
        assert_eq!(truncated.encode_utf16().count(), 499);
        assert!(!truncated.contains('😀'));
        let body = tts_request_body(&truncated);
        assert_eq!(body["model"], TTS_MODEL);
        assert_eq!(body["input"]["voice"], TTS_VOICE);
        assert_eq!(body["input"]["language_type"], TTS_LANGUAGE);
    }

    #[test]
    fn tts_audio_url_is_https_and_pinned_to_aliyuncs_domain() {
        let valid = "https://dashscope-result.oss-cn-beijing.aliyuncs.com/a.wav?sig=x";
        assert_eq!(validate_audio_url(valid).unwrap(), valid);
        assert_eq!(
            validate_audio_url(
                "http://dashscope-result-bj.oss-cn-beijing.aliyuncs.com/a.wav?sig=x"
            )
            .unwrap(),
            "https://dashscope-result-bj.oss-cn-beijing.aliyuncs.com/a.wav?sig=x"
        );
        for invalid in [
            "https://dashscope.aliyuncs.com:444/a.wav",
            "http://dashscope.aliyuncs.com:444/a.wav",
            "https://aliyuncs.com.evil.example/a.wav",
            "https://user@dashscope.aliyuncs.com/a.wav",
            "not-a-url",
        ] {
            assert_eq!(validate_audio_url(invalid), Err(SpeechError::Upstream));
        }
    }

    #[test]
    fn credential_lookup_tracks_csv_hot_updates() {
        let current = Arc::new(std::sync::RwLock::new(None::<String>));
        let resolver: Arc<dyn McpCredentialResolver> = {
            let current = Arc::clone(&current);
            Arc::new(move |provider: &str| {
                if provider != zk_mcp::security::DASHSCOPE_PROVIDER {
                    return None;
                }
                current.read().expect("credential lock").clone()
            })
        };
        let service = DashScopeSpeechService::new(resolver);
        assert!(!service.is_available());
        *current.write().expect("credential lock") = Some("key-a,key-b".to_owned());
        let ring = service.current_key_ring().expect("hot key ring");
        assert_eq!(ring.len(), 2);
        *current.write().expect("credential lock") = None;
        assert!(!service.is_available());
    }
}
