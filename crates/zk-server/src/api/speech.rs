//! ASR/TTS REST handlers backed by the shared `DashScope` speech service.

use axum::Json;
use axum::extract::multipart::MultipartRejection;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::ApiError;
use crate::speech::{MAX_AUDIO_BYTES, SpeechAudio, SpeechError};
use crate::state::AppState;

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct SpeechStatusResponse {
    available: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct AsrResponse {
    text: String,
}

#[derive(Debug, ToSchema)]
#[allow(dead_code)] // OpenAPI-only multipart schema; Axum extracts the field dynamically.
struct AsrMultipartRequest {
    #[schema(value_type = String, format = Binary)]
    audio: Vec<u8>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct TtsRequest {
    /// Visible assistant text; safely truncated to 500 UTF-16 code units.
    text: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TtsResponse {
    audio_url: String,
}

/// `GET /api/asr/status` — availability of the standard `DashScope` ASR model.
#[utoipa::path(
    get,
    path = "/api/asr/status",
    tag = "speech",
    responses((status = 200, body = SpeechStatusResponse))
)]
pub(crate) async fn asr_status(State(state): State<AppState>) -> Json<SpeechStatusResponse> {
    Json(SpeechStatusResponse {
        available: state.speech().is_available(),
    })
}

/// `POST /api/asr/recognize` — recognize one multipart `audio` field.
#[utoipa::path(
    post,
    path = "/api/asr/recognize",
    tag = "speech",
    request_body(content = inline(AsrMultipartRequest), content_type = "multipart/form-data"),
    responses(
        (status = 200, body = AsrResponse),
        (status = 400, description = "Missing/empty/non-audio multipart field"),
        (status = 403, description = "Trusted local Origin or Bearer token required"),
        (status = 413, description = "Audio payload exceeds 7.5 MiB"),
        (status = 429, description = "All configured DashScope keys are rate limited"),
        (status = 502, description = "DashScope returned an invalid/error response"),
        (status = 503, description = "Standard DashScope key is unavailable"),
        (status = 504, description = "DashScope request timed out")
    )
)]
pub(crate) async fn recognize(
    State(state): State<AppState>,
    multipart: Result<Multipart, MultipartRejection>,
) -> Result<Json<AsrResponse>, ApiError> {
    let mut multipart = multipart.map_err(|error| map_multipart_rejection(&error))?;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| map_multipart_error(&error))?
    {
        if field.name() != Some("audio") {
            continue;
        }
        let mime_type = field.content_type().map(ToString::to_string);
        let bytes = field
            .bytes()
            .await
            .map_err(|error| map_multipart_error(&error))?;
        if bytes.is_empty() {
            return Err(invalid_input("audio file must not be empty"));
        }
        if bytes.len() > MAX_AUDIO_BYTES {
            return Err(map_speech_error(SpeechError::PayloadTooLarge));
        }
        let mime_type = mime_type
            .filter(|value| value.starts_with("audio/"))
            .ok_or_else(|| invalid_input("audio field must use an audio/* content type"))?;
        let text = state
            .speech()
            .recognize(SpeechAudio { bytes, mime_type })
            .await
            .map_err(map_speech_error)?;
        return Ok(Json(AsrResponse { text }));
    }
    Err(invalid_input("audio multipart field is required"))
}

/// `GET /api/tts/status` — availability of the standard `DashScope` TTS model.
#[utoipa::path(
    get,
    path = "/api/tts/status",
    tag = "speech",
    responses((status = 200, body = SpeechStatusResponse))
)]
pub(crate) async fn tts_status(State(state): State<AppState>) -> Json<SpeechStatusResponse> {
    Json(SpeechStatusResponse {
        available: state.speech().is_available(),
    })
}

/// `POST /api/tts/synthesize` — synthesize text and return a validated signed
/// `aliyuncs.com` HTTPS media URL.
#[utoipa::path(
    post,
    path = "/api/tts/synthesize",
    tag = "speech",
    request_body = TtsRequest,
    responses(
        (status = 200, body = TtsResponse),
        (status = 400, description = "Missing, malformed, or blank text"),
        (status = 403, description = "Trusted local Origin or Bearer token required"),
        (status = 413, description = "JSON request body is too large"),
        (status = 429, description = "All configured DashScope keys are rate limited"),
        (status = 502, description = "DashScope returned an invalid/error response"),
        (status = 503, description = "Standard DashScope key is unavailable"),
        (status = 504, description = "DashScope request timed out"),
        (status = 415, description = "Content-Type must be application/json")
    )
)]
pub(crate) async fn synthesize(
    State(state): State<AppState>,
    body: Result<Json<TtsRequest>, JsonRejection>,
) -> Result<Json<TtsResponse>, ApiError> {
    let Json(body) = body.map_err(|error| {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            payload_too_large("TTS request payload is too large")
        } else {
            invalid_input("JSON body with text is required")
        }
    })?;
    if body.text.trim().is_empty() {
        return Err(invalid_input("text must not be blank"));
    }
    let audio_url = state
        .speech()
        .synthesize(body.text)
        .await
        .map_err(map_speech_error)?;
    Ok(Json(TtsResponse { audio_url }))
}

fn map_multipart_error(error: &axum::extract::multipart::MultipartError) -> ApiError {
    if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
        payload_too_large("Audio payload exceeds the 7.5 MiB limit")
    } else {
        invalid_input("malformed multipart audio payload")
    }
}

fn map_multipart_rejection(error: &MultipartRejection) -> ApiError {
    if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
        payload_too_large("Audio payload exceeds the 7.5 MiB limit")
    } else {
        invalid_input("multipart/form-data is required")
    }
}

fn invalid_input(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "INVALID_REQUEST".to_owned(),
        message: message.to_owned(),
    }
}

fn payload_too_large(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        code: "PAYLOAD_TOO_LARGE".to_owned(),
        message: message.to_owned(),
    }
}

fn map_speech_error(error: SpeechError) -> ApiError {
    match error {
        SpeechError::InvalidInput => invalid_input("invalid speech input"),
        SpeechError::PayloadTooLarge => {
            payload_too_large("Audio payload exceeds the 7.5 MiB limit")
        }
        SpeechError::Unavailable => ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "SPEECH_SERVICE_UNAVAILABLE".to_owned(),
            message: "A standard DashScope API key is required".to_owned(),
        },
        SpeechError::RateLimited => ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: "SPEECH_RATE_LIMITED".to_owned(),
            message: "DashScope speech service is rate limited".to_owned(),
        },
        SpeechError::Timeout => ApiError {
            status: StatusCode::GATEWAY_TIMEOUT,
            code: "SPEECH_UPSTREAM_TIMEOUT".to_owned(),
            message: "DashScope speech service timed out".to_owned(),
        },
        SpeechError::Upstream => ApiError {
            status: StatusCode::BAD_GATEWAY,
            code: "SPEECH_UPSTREAM_ERROR".to_owned(),
            message: "DashScope speech service returned an invalid response".to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::extract::ConnectInfo;
    use axum::http::{Method, Request, header};
    use futures::future::BoxFuture;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;
    use crate::routes::build_router;
    use crate::speech::{SpeechAudio, SpeechService};

    struct FakeSpeech;

    impl SpeechService for FakeSpeech {
        fn is_available(&self) -> bool {
            true
        }

        fn recognize(&self, _audio: SpeechAudio) -> BoxFuture<'_, Result<String, SpeechError>> {
            Box::pin(async { Ok("recognized text".to_owned()) })
        }

        fn synthesize(&self, _text: String) -> BoxFuture<'_, Result<String, SpeechError>> {
            Box::pin(async { Ok("https://result.oss-cn-beijing.aliyuncs.com/a.wav".to_owned()) })
        }
    }

    fn app() -> Router {
        let state = AppState::for_tests().with_speech_service(Arc::new(FakeSpeech));
        build_router(state)
    }

    fn request(method: Method, uri: &str, content_type: &str, body: Body) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .extension(ConnectInfo(
                "127.0.0.1:51717"
                    .parse::<SocketAddr>()
                    .expect("loopback address"),
            ))
            .header(header::ORIGIN, "http://127.0.0.1:5273")
            .header(header::CONTENT_TYPE, content_type)
            .body(body)
            .expect("request")
    }

    async fn json_response(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
        let response = router.clone().oneshot(request).await.expect("response");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let value = serde_json::from_slice(&body).expect("JSON response");
        (status, value)
    }

    #[tokio::test]
    async fn success_contracts_cover_status_asr_and_tts() {
        let router = app();
        let status_request = Request::builder()
            .uri("/api/asr/status")
            .extension(ConnectInfo(
                "127.0.0.1:51717"
                    .parse::<SocketAddr>()
                    .expect("loopback address"),
            ))
            .header("sec-fetch-site", "same-origin")
            .body(Body::empty())
            .expect("request");
        let (status, value) = json_response(&router, status_request).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value, json!({"available": true}));

        let boundary = "speech-boundary";
        let multipart = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"audio\"; filename=\"a.webm\"\r\nContent-Type: audio/webm\r\n\r\nabc\r\n--{boundary}--\r\n"
        );
        let asr = request(
            Method::POST,
            "/api/asr/recognize",
            &format!("multipart/form-data; boundary={boundary}"),
            Body::from(multipart),
        );
        let (status, value) = json_response(&router, asr).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value, json!({"text": "recognized text"}));

        let tts = request(
            Method::POST,
            "/api/tts/synthesize",
            "application/json",
            Body::from(r#"{"text":"hello"}"#),
        );
        let (status, value) = json_response(&router, tts).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            value,
            json!({"audioUrl":"https://result.oss-cn-beijing.aliyuncs.com/a.wav"})
        );
    }

    #[tokio::test]
    async fn speech_posts_require_trusted_origin() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/tts/synthesize")
            .extension(ConnectInfo(
                "127.0.0.1:51717"
                    .parse::<SocketAddr>()
                    .expect("loopback address"),
            ))
            .header(header::ORIGIN, "https://attacker.example")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"text":"hello"}"#))
            .expect("request");
        let (status, value) = json_response(&app(), request).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(value["code"], "SPEECH_REQUEST_ORIGIN_DENIED");
    }

    #[tokio::test]
    async fn multipart_transport_limit_maps_to_json_413() {
        let boundary = "oversized-speech";
        let mut body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"audio\"; filename=\"a.webm\"\r\nContent-Type: audio/webm\r\n\r\n"
        )
        .into_bytes();
        body.resize(crate::speech::MAX_MULTIPART_BYTES + 1, b'a');
        let request = request(
            Method::POST,
            "/api/asr/recognize",
            &format!("multipart/form-data; boundary={boundary}"),
            Body::from(body),
        );
        let (status, value) = json_response(&app(), request).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(value["code"], "PAYLOAD_TOO_LARGE");
    }

    #[test]
    fn service_failures_map_to_required_http_statuses() {
        for (error, expected) in [
            (SpeechError::InvalidInput, StatusCode::BAD_REQUEST),
            (SpeechError::PayloadTooLarge, StatusCode::PAYLOAD_TOO_LARGE),
            (SpeechError::RateLimited, StatusCode::TOO_MANY_REQUESTS),
            (SpeechError::Upstream, StatusCode::BAD_GATEWAY),
            (SpeechError::Unavailable, StatusCode::SERVICE_UNAVAILABLE),
            (SpeechError::Timeout, StatusCode::GATEWAY_TIMEOUT),
        ] {
            assert_eq!(map_speech_error(error).status, expected);
        }
    }
}
