use super::*;
use futures::future::BoxFuture;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

#[derive(Default)]
struct Bindings {
    values: Mutex<HashMap<String, OAuthBinding>>,
    fail_save: AtomicBool,
}
impl OAuthBindingStore for Bindings {
    fn load<'a>(
        &'a self,
        name: &'a str,
    ) -> BoxFuture<'a, Result<Option<OAuthBinding>, OAuthError>> {
        Box::pin(async move { Ok(lock(&self.values).get(name).cloned()) })
    }
    fn save<'a>(
        &'a self,
        name: &'a str,
        binding: Option<OAuthBinding>,
    ) -> BoxFuture<'a, Result<(), OAuthError>> {
        Box::pin(async move {
            if self.fail_save.load(Ordering::Acquire) {
                return Err(OAuthError::BindingStorage);
            }
            let mut values = lock(&self.values);
            if let Some(binding) = binding {
                values.insert(name.into(), binding);
            } else {
                values.remove(name);
            }
            Ok(())
        })
    }
}
#[derive(Default)]
struct Secrets {
    values: Mutex<HashMap<String, OAuthSecrets>>,
    fail_save: AtomicBool,
    fail_load: AtomicBool,
    fail_delete: AtomicBool,
}
impl OAuthSecretStore for Secrets {
    fn load<'a>(
        &'a self,
        reference: &'a str,
    ) -> BoxFuture<'a, Result<Option<OAuthSecrets>, OAuthError>> {
        Box::pin(async move {
            if self.fail_load.load(Ordering::Acquire) {
                return Err(OAuthError::SecretStorage);
            }
            Ok(lock(&self.values).get(reference).cloned())
        })
    }
    fn save<'a>(
        &'a self,
        reference: &'a str,
        secrets: OAuthSecrets,
    ) -> BoxFuture<'a, Result<(), OAuthError>> {
        Box::pin(async move {
            if self.fail_save.load(Ordering::Acquire) {
                return Err(OAuthError::SecretStorage);
            }
            lock(&self.values).insert(reference.into(), secrets);
            Ok(())
        })
    }
    fn delete<'a>(&'a self, reference: &'a str) -> BoxFuture<'a, Result<(), OAuthError>> {
        Box::pin(async move {
            if self.fail_delete.load(Ordering::Acquire) {
                return Err(OAuthError::SecretStorage);
            }
            lock(&self.values).remove(reference);
            Ok(())
        })
    }
}

#[tokio::test]
async fn logout_failure_is_durably_inactive_across_restart_and_keeps_cleanup_reference() {
    for fail_load in [true, false] {
        let server = Server::start().await;
        let bindings = Arc::new(Bindings::default());
        let secrets = Arc::new(Secrets::default());
        let oauth = coordinator(bindings.clone(), secrets.clone());
        let resource = format!("{}/mcp", server.base);
        let (start, completed) = oauth
            .begin(
                "srv",
                &resource,
                OAuthOptions::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let state = server.capture(&start);
        assert_eq!(callback(&start, &state).await, reqwest::StatusCode::OK);
        assert_eq!(completed.await.unwrap(), Ok(()));
        let original = bindings.load("srv").await.unwrap().unwrap();
        secrets.fail_load.store(fail_load, Ordering::Release);
        secrets.fail_delete.store(!fail_load, Ordering::Release);
        assert_eq!(oauth.logout("srv").await, Err(OAuthError::SecretStorage));
        let restarted = coordinator(bindings.clone(), secrets.clone());
        assert_ne!(restarted.status("srv").await.unwrap().state, "authorized");
        assert_eq!(
            bindings.load("srv").await.unwrap().unwrap().credential_ref,
            original.credential_ref
        );
        assert_eq!(
            restarted.authorization_header("srv", &resource).await,
            Err(OAuthError::AuthorizationRequired)
        );
        secrets.fail_load.store(false, Ordering::Release);
        secrets.fail_delete.store(false, Ordering::Release);
        restarted.logout("srv").await.unwrap();
        assert!(lock(&secrets.values).is_empty());
        assert_eq!(
            restarted.authorization_header("srv", &resource).await,
            Err(OAuthError::AuthorizationRequired)
        );
    }
}

struct Server {
    base: String,
    handle: tokio::task::JoinHandle<()>,
    challenge: Arc<Mutex<Option<String>>>,
    codes: Arc<AtomicUsize>,
    refreshes: Arc<AtomicUsize>,
    revokes: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
impl Server {
    #[allow(clippy::too_many_lines)] // One loopback OAuth fixture serves discovery, registration, tokens and revocation.
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let served_base = base.clone();
        let challenge: Arc<Mutex<Option<String>>> = Arc::default();
        let expected = challenge.clone();
        let codes = Arc::new(AtomicUsize::new(0));
        let observed_codes = codes.clone();
        let refreshes = Arc::new(AtomicUsize::new(0));
        let observed_refresh = refreshes.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();
        let revokes = Arc::new(AtomicUsize::new(0));
        let observed_revoke = revokes.clone();
        let handle = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut chunk = [0u8; 4096];
                let header_end = loop {
                    let count = socket.read(&mut chunk).await.unwrap();
                    if count == 0 {
                        break None;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break Some(end + 4);
                    }
                    assert!(bytes.len() < 32768);
                };
                let Some(header_end) = header_end else {
                    continue;
                };
                let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
                let length: usize = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map_or(0, |(_, value)| value.trim().parse().unwrap());
                while bytes.len() < header_end + length {
                    let count = socket.read(&mut chunk).await.unwrap();
                    if count == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                }
                let target = headers
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap();
                let mut status = "200 OK";
                let mut extra = String::new();
                let body = match target {
                    "/mcp" if headers.starts_with("POST ") => {
                        let message: Value = serde_json::from_slice(&bytes[header_end..]).unwrap();
                        assert!(
                            headers
                                .to_ascii_lowercase()
                                .contains("authorization: bearer ")
                        );
                        if message["method"] == "tools/call" {
                            observed_calls.fetch_add(1, Ordering::AcqRel);
                            assert!(headers.contains("rotated-token"));
                        }
                        json!({"jsonrpc":"2.0", "id":message["id"], "result":{"protocolVersion":"2025-03-26", "capabilities":{}, "content":[{"type":"text", "text":"once"}]}})
                    }
                    "/mcp" => {
                        status = "401 Unauthorized";
                        extra = format!(
                            "WWW-Authenticate: Bearer resource_metadata=\"{served_base}/meta\", scope=\"read\"\r\n"
                        );
                        json!({})
                    }
                    "/meta" => {
                        json!({"resource":format!("{served_base}/mcp"),"authorization_servers":[served_base]})
                    }
                    "/.well-known/oauth-authorization-server" => {
                        json!({"issuer":served_base,"authorization_endpoint":format!("{served_base}/authorize"),"token_endpoint":format!("{served_base}/token"),"registration_endpoint":format!("{served_base}/register"),"revocation_endpoint":format!("{served_base}/revoke"),"code_challenge_methods_supported":["S256"],"token_endpoint_auth_methods_supported":["none"]})
                    }
                    "/register" => {
                        let value: Value = serde_json::from_slice(&bytes[header_end..]).unwrap();
                        assert_eq!(value["token_endpoint_auth_method"], "none");
                        json!({"client_id":"issued-client", "token_endpoint_auth_method":"none"})
                    }
                    "/token" => {
                        let fields: HashMap<_, _> =
                            url::form_urlencoded::parse(&bytes[header_end..])
                                .into_owned()
                                .collect();
                        assert_eq!(
                            fields.get("resource").unwrap(),
                            &format!("{served_base}/mcp")
                        );
                        if fields.get("grant_type").unwrap() == "authorization_code" {
                            observed_codes.fetch_add(1, Ordering::AcqRel);
                            assert_eq!(fields.get("code").unwrap(), "valid-code");
                            assert_eq!(
                                Some(URL_SAFE_NO_PAD.encode(Sha256::digest(
                                    fields.get("code_verifier").unwrap().as_bytes()
                                ))),
                                *lock(&expected)
                            );
                            json!({"access_token":"initial-token","refresh_token":"initial-refresh","token_type":"Bearer","expires_in":3600})
                        } else {
                            observed_refresh.fetch_add(1, Ordering::AcqRel);
                            assert_eq!(fields.get("refresh_token").unwrap(), "initial-refresh");
                            json!({"access_token":"rotated-token","refresh_token":"rotated-refresh","token_type":"Bearer","expires_in":3600})
                        }
                    }
                    "/revoke" => {
                        observed_revoke.fetch_add(1, Ordering::AcqRel);
                        Value::Null
                    }
                    _ => {
                        status = "404 Not Found";
                        json!({})
                    }
                };
                let body = if target == "/revoke" {
                    String::new()
                } else {
                    body.to_string()
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\n{extra}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        Self {
            base,
            handle,
            challenge,
            codes,
            refreshes,
            revokes,
            calls,
        }
    }
    fn capture(&self, start: &OAuthStart) -> String {
        let url = Url::parse(&start.authorization_url).unwrap();
        let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query.get("code_challenge_method").unwrap(), "S256");
        assert_eq!(
            query.get("resource").unwrap(),
            &format!("{}/mcp", self.base)
        );
        assert_eq!(query.get("client_id").unwrap(), "issued-client");
        *lock(&self.challenge) = query.get("code_challenge").cloned();
        query.get("state").unwrap().clone()
    }
}

fn coordinator(bindings: Arc<Bindings>, secrets: Arc<Secrets>) -> Arc<OAuthCoordinator> {
    Arc::new(OAuthCoordinator {
        bindings,
        secrets,
        http: http::OAuthHttp {
            allow_loopback: true,
        },
        locks: Mutex::default(),
        pending: Mutex::default(),
    })
}

async fn callback(start: &OAuthStart, state: &str) -> reqwest::StatusCode {
    let mut url = Url::parse(&start.redirect_uri).unwrap();
    url.query_pairs_mut()
        .extend_pairs([("state", state), ("code", "valid-code")]);
    reqwest::get(url).await.unwrap().status()
}

#[tokio::test]
async fn real_http_pkce_callback_rotation_singleflight_and_empty_body_revocation() {
    let server = Server::start().await;
    let bindings = Arc::new(Bindings::default());
    let secrets = Arc::new(Secrets::default());
    let oauth = coordinator(bindings.clone(), secrets.clone());
    let resource = format!("{}/mcp", server.base);
    let (start, completed) = oauth
        .begin(
            "srv",
            &resource,
            OAuthOptions::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let state = server.capture(&start);
    assert_eq!(
        callback(&start, "wrong-state").await,
        reqwest::StatusCode::BAD_REQUEST
    );
    assert_eq!(callback(&start, &state).await, reqwest::StatusCode::OK);
    assert_eq!(completed.await.unwrap(), Ok(()));
    assert_eq!(server.codes.load(Ordering::Acquire), 1);
    assert_eq!(
        oauth
            .authorization_header("srv", &resource)
            .await
            .unwrap()
            .as_deref(),
        Some("Bearer initial-token")
    );
    assert_eq!(
        oauth
            .authorization_header("srv", "http://127.0.0.1:9/other")
            .await,
        Err(OAuthError::BindingMismatch)
    );
    lock(&bindings.values).get_mut("srv").unwrap().expires_at = Some(0);
    let (first, second) = tokio::join!(
        oauth.authorization_header("srv", &resource),
        oauth.authorization_header("srv", &resource)
    );
    assert_eq!(first.unwrap(), Some("Bearer rotated-token".into()));
    assert_eq!(second.unwrap(), Some("Bearer rotated-token".into()));
    assert_eq!(server.refreshes.load(Ordering::Acquire), 1);
    assert!(oauth.logout("srv").await.unwrap());
    assert_eq!(server.revokes.load(Ordering::Acquire), 1);
    assert!(lock(&secrets.values).is_empty());
    assert_eq!(
        bindings.load("srv").await.unwrap().unwrap().state,
        OAuthBindingState::Inactive
    );
    assert_eq!(oauth.status("srv").await.unwrap().state, "idle");
}

#[tokio::test]
async fn keychain_failure_never_publishes_binding_and_metadata_failure_rolls_back_secret() {
    for secret_failure in [true, false] {
        let server = Server::start().await;
        let bindings = Arc::new(Bindings::default());
        let secrets = Arc::new(Secrets::default());
        secrets.fail_save.store(secret_failure, Ordering::Release);
        bindings.fail_save.store(!secret_failure, Ordering::Release);
        let oauth = coordinator(bindings.clone(), secrets.clone());
        let (start, completed) = oauth
            .begin(
                "srv",
                &format!("{}/mcp", server.base),
                OAuthOptions::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let state = server.capture(&start);
        callback(&start, &state).await;
        assert_eq!(
            completed.await.unwrap(),
            Err(if secret_failure {
                OAuthError::SecretStorage
            } else {
                OAuthError::BindingStorage
            })
        );
        assert!(lock(&bindings.values).is_empty());
        assert!(lock(&secrets.values).is_empty());
        assert_eq!(oauth.status("srv").await.unwrap().state, "error");
    }
}

#[tokio::test]
async fn disabling_service_invalidates_callback_before_any_code_exchange() {
    let server = Server::start().await;
    let bindings = Arc::new(Bindings::default());
    let secrets = Arc::new(Secrets::default());
    let oauth = coordinator(bindings.clone(), secrets.clone());
    let cancel = CancellationToken::new();
    let (_, completed) = oauth
        .begin(
            "srv",
            &format!("{}/mcp", server.base),
            OAuthOptions::default(),
            cancel.clone(),
        )
        .await
        .unwrap();
    cancel.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), completed)
            .await
            .unwrap()
            .unwrap(),
        Err(OAuthError::Cancelled)
    );
    assert_eq!(server.codes.load(Ordering::Acquire), 0);
    assert!(lock(&bindings.values).is_empty());
    assert!(lock(&secrets.values).is_empty());
}

#[tokio::test]
async fn production_http_rejects_loopback_non_tls_and_embedded_credentials() {
    let http = http::OAuthHttp::production();
    for url in [
        "http://public.example/mcp",
        "https://secret@public.example/mcp",
        "https://127.0.0.1/mcp",
        "https://[::1]/mcp",
    ] {
        assert!(http.get_json(url).await.is_err());
    }
}

#[tokio::test]
#[ignore = "Requires an unlocked macOS login Keychain; run explicitly on the supported local host"]
async fn native_keychain_round_trip() {
    let store = storage::KeychainSecretStore;
    let key = format!("zkcode-test-{}", uuid::Uuid::new_v4());
    let payload = OAuthSecrets {
        access_token: "local-test-token".into(),
        refresh_token: Some("local-test-refresh".into()),
        client_secret: None,
    };
    store.save(&key, payload).await.unwrap();
    let loaded = store.load(&key).await;
    let deleted = store.delete(&key).await;
    assert_eq!(loaded.unwrap().unwrap().access_token, "local-test-token");
    deleted.unwrap();
    assert!(store.load(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn live_http_transport_refreshes_before_dispatch_without_replaying_calls() {
    use crate::transport::McpTransport;
    let server = Server::start().await;
    let bindings = Arc::new(Bindings::default());
    let secrets = Arc::new(Secrets::default());
    let oauth = coordinator(bindings.clone(), secrets);
    let resource = format!("{}/mcp", server.base);
    let (start, completed) = oauth
        .begin(
            "srv",
            &resource,
            OAuthOptions::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let state = server.capture(&start);
    callback(&start, &state).await;
    completed.await.unwrap().unwrap();
    let cancel = CancellationToken::new();
    let transport =
        crate::StreamableHttpTransport::new(&resource, std::collections::BTreeMap::default())
            .unwrap();
    transport.set_request_authorizer(oauth.request_authorizer(
        "srv".into(),
        resource.clone(),
        cancel.clone(),
    ));
    transport.connect().await.unwrap();
    lock(&bindings.values).get_mut("srv").unwrap().expires_at = Some(0);
    let result = transport
        .send_request(
            transport.next_request_id(),
            "tools/call",
            Some(json!({"name":"echo","arguments":{}})),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(result.unwrap()["content"][0]["text"], "once");
    assert_eq!(server.calls.load(Ordering::Acquire), 1);
    assert_eq!(server.refreshes.load(Ordering::Acquire), 1);
    cancel.cancel();
    assert!(
        transport
            .send_request(
                transport.next_request_id(),
                "tools/call",
                Some(json!({"name":"echo"})),
                Duration::from_secs(3)
            )
            .await
            .is_err()
    );
    assert_eq!(server.calls.load(Ordering::Acquire), 1);
    transport.close().await;
}
