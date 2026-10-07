//! Independent summary wire transport; accounting remains in the normal registry.
use crate::ProviderError;
use futures::StreamExt;
use serde_json::{Value, json};

pub(crate) fn prepare(body: &mut Value, base_url: &str) {
    body["stream"] = json!(false);
    if let Some(object) = body.as_object_mut() {
        object.remove("stream_options");
        if base_url.trim_end_matches('/')
            == "https://token-plan.cn-beijing.maas.aliyuncs.com/compatible-mode/v1"
            && let Some(limit) = object.remove("max_tokens")
        {
            object.insert("max_completion_tokens".into(), limit);
        }
    }
}

/// Preserve missing usage fields; the common parser must not turn them into zeros.
fn response_frame(body: &[u8]) -> Result<bytes::Bytes, ProviderError> {
    let root: Value =
        serde_json::from_slice(body).map_err(|_| invalid("SUMMARY_INVALID_RESPONSE"))?;
    let choice = root
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .ok_or_else(|| invalid("SUMMARY_EMPTY_RESPONSE"))?;
    let mut frame = json!({"choices":[{"index":0,"delta":choice["message"],"finish_reason":choice["finish_reason"]}]});
    if let Some(usage) = root.get("usage") {
        frame["usage"] = usage.clone();
    }
    Ok(format!("data: {frame}\n\ndata: [DONE]\n\n").into())
}

pub(crate) async fn response_bytes(
    response: reqwest::Response,
) -> Result<bytes::Bytes, ProviderError> {
    const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| invalid("SUMMARY_TRANSPORT_FAILED"))?;
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(invalid("SUMMARY_RESPONSE_TOO_LARGE"));
        }
        body.extend_from_slice(&chunk);
    }
    response_frame(&body)
}

fn invalid(message: &str) -> ProviderError {
    ProviderError::Config {
        message: message.into(),
    }
}

pub(crate) fn retry_delay_ms(value: Option<&str>) -> u64 {
    let Some(value) = value else {
        return 1000;
    };
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return seconds.saturating_mul(1000);
    }
    chrono::DateTime::parse_from_rfc2822(value.trim())
        .ok()
        .map_or(1000, |when| {
            u64::try_from(
                when.timestamp_millis()
                    .saturating_sub(chrono::Utc::now().timestamp_millis())
                    .max(0),
            )
            .unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn summary_wire_preserves_usage_and_uses_endpoint_output_limit() {
        let mut body =
            json!({"max_tokens":8192,"stream":true,"stream_options":{"include_usage":true}});
        prepare(
            &mut body,
            "https://token-plan.cn-beijing.maas.aliyuncs.com/compatible-mode/v1",
        );
        assert_eq!(body["max_completion_tokens"], 8192);
        assert!(body.get("max_tokens").is_none());
        assert_eq!(body["stream"], false);
        assert!(body.get("stream_options").is_none());
        let mut direct = json!({"max_tokens":8192});
        prepare(&mut direct, "https://api.deepseek.com/v1");
        assert_eq!(direct["max_tokens"], 8192);
        let frame=response_frame(br#"{"choices":[{"message":{"content":"summary"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7}}"#).unwrap();
        let frame = std::str::from_utf8(&frame).unwrap();
        assert!(frame.contains("prompt_tokens"));
        assert!(!frame.contains("completion_tokens"));
        assert_eq!(retry_delay_ms(Some("3")), 3000);
        assert_eq!(retry_delay_ms(Some("-1")), 1000);
        assert_eq!(retry_delay_ms(Some("Wed, 21 Oct 2015 07:28:00 GMT")), 0);
    }
}
