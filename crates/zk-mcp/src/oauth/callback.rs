use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use url::Url;

use super::OAuthError;

pub(super) async fn wait_code(
    listener: TcpListener,
    redirect: &str,
    state: &str,
    issuer: &str,
) -> Result<String, OAuthError> {
    let redirect = Url::parse(redirect).map_err(|_| OAuthError::InvalidMetadata)?;
    loop {
        let (mut stream, address) = listener.accept().await.map_err(|_| OAuthError::Network)?;
        if !address.ip().is_loopback() {
            continue;
        }
        let request = tokio::time::timeout(Duration::from_secs(5), read_request(&mut stream)).await;
        let outcome = match request {
            Ok(Ok(request)) => parse_callback(&request, &redirect, state, issuer),
            _ => Err(OAuthError::InvalidMetadata),
        };
        let valid_state = outcome.is_ok() || outcome == Err(OAuthError::RemoteRejected);
        let (status, text) = if valid_state {
            (
                "200 OK",
                "Authorization response received. Return to zkcode to check the result.",
            )
        } else {
            ("400 Bad Request", "Invalid authorization response.")
        };
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nCache-Control: no-store\r\nContent-Security-Policy: default-src 'none'\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{text}",
            text.len()
        );
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            stream.write_all(response.as_bytes()),
        )
        .await;
        if valid_state {
            return outcome;
        }
    }
}

async fn read_request(stream: &mut TcpStream) -> Result<String, OAuthError> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 1024];
    while bytes.len() <= 8192 {
        let count = stream
            .read(&mut chunk)
            .await
            .map_err(|_| OAuthError::Network)?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            if bytes.len() > 8192 {
                break;
            }
            return String::from_utf8(bytes).map_err(|_| OAuthError::InvalidMetadata);
        }
    }
    Err(OAuthError::InvalidMetadata)
}

fn parse_callback(
    request: &str,
    redirect: &Url,
    state: &str,
    issuer: &str,
) -> Result<String, OAuthError> {
    let mut lines = request.split("\r\n");
    let mut first = lines
        .next()
        .ok_or(OAuthError::InvalidMetadata)?
        .split_whitespace();
    if first.next() != Some("GET") {
        return Err(OAuthError::InvalidMetadata);
    }
    let target = first.next().ok_or(OAuthError::InvalidMetadata)?;
    if first.next() != Some("HTTP/1.1") || first.next().is_some() {
        return Err(OAuthError::InvalidMetadata);
    }
    let expected_host = format!(
        "127.0.0.1:{}",
        redirect.port().ok_or(OAuthError::InvalidMetadata)?
    );
    let hosts: Vec<_> = lines
        .filter_map(|line| line.split_once(':'))
        .filter(|(key, _)| key.eq_ignore_ascii_case("host"))
        .map(|(_, value)| value.trim())
        .collect();
    if hosts != [expected_host] || !target.starts_with('/') || target.starts_with("//") {
        return Err(OAuthError::InvalidMetadata);
    }
    let url = redirect
        .join(target)
        .map_err(|_| OAuthError::InvalidMetadata)?;
    if url.path() != redirect.path() {
        return Err(OAuthError::InvalidMetadata);
    }
    let pairs: Vec<_> = url.query_pairs().collect();
    let get = |name: &str| -> Result<Option<String>, OAuthError> {
        let found: Vec<_> = pairs
            .iter()
            .filter(|(key, _)| key == name)
            .map(|(_, value)| value.to_string())
            .collect();
        if found.len() > 1 {
            return Err(OAuthError::InvalidMetadata);
        }
        Ok(found.into_iter().next())
    };
    let returned_state = get("state")?.ok_or(OAuthError::BindingMismatch)?;
    if returned_state.len() != state.len()
        || returned_state
            .bytes()
            .zip(state.bytes())
            .fold(0, |difference, (left, right)| difference | (left ^ right))
            != 0
    {
        return Err(OAuthError::BindingMismatch);
    }
    if get("iss")?.is_some_and(|value| value != issuer) {
        return Err(OAuthError::BindingMismatch);
    }
    if get("error")?.is_some() {
        return Err(OAuthError::RemoteRejected);
    }
    get("code")?
        .filter(|code| !code.is_empty())
        .ok_or(OAuthError::InvalidMetadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn callback_checks_host_state_path_issuer_and_duplicate_parameters() {
        let redirect = Url::parse("http://127.0.0.1:1234/mcp/oauth/test").unwrap();
        let request = |query: &str| {
            format!("GET /mcp/oauth/test?{query} HTTP/1.1\r\nHost: 127.0.0.1:1234\r\n\r\n")
        };
        assert_eq!(
            parse_callback(
                &request("state=abc&code=one"),
                &redirect,
                "abc",
                "https://issuer.test"
            ),
            Ok("one".into())
        );
        for query in [
            "state=wrong&code=one",
            "state=abc&state=abc&code=one",
            "state=abc&code=one&iss=https%3A%2F%2Fother.test",
        ] {
            assert!(
                parse_callback(&request(query), &redirect, "abc", "https://issuer.test").is_err()
            );
        }
        assert!(
            parse_callback(
                &request("state=abc&code=one").replace("Host: 127.0.0.1", "Host: evil.test"),
                &redirect,
                "abc",
                "https://issuer.test"
            )
            .is_err()
        );
    }
}
