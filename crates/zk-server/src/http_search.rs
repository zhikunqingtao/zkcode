//! Optional `SearXNG`-compatible `WebSearch` backend over the safe `WebFetch` port.

use std::sync::Arc;

use futures::future::BoxFuture;
use reqwest::Url;
use serde_json::Value;
use zk_tools::{
    MAX_FETCH_BYTES, MAX_REDIRECTS, SearchBackend, SearchBackendError, SearchRequest, SearchResult,
    WebFetchPort, WebFetchRequest,
};

/// HTTP search provider using `SearXNG`'s `format=json` response shape.
pub struct SearxngSearchBackend {
    endpoint: Url,
    fetch: Arc<dyn WebFetchPort>,
}

impl std::fmt::Debug for SearxngSearchBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SearxngSearchBackend")
            .field(
                "endpoint_origin",
                &self.endpoint.origin().ascii_serialization(),
            )
            .finish_non_exhaustive()
    }
}

impl SearxngSearchBackend {
    /// Parses a configured public HTTP(S) endpoint. Credentials in URLs are rejected.
    ///
    /// # Errors
    /// Returns [`SearchBackendError`] when the endpoint is invalid, non-HTTP(S), lacks a
    /// host, or embeds credentials.
    pub fn new(endpoint: &str, fetch: Arc<dyn WebFetchPort>) -> Result<Self, SearchBackendError> {
        let endpoint = Url::parse(endpoint).map_err(|_| {
            SearchBackendError::new(
                "WEB_SEARCH_CONFIG_INVALID",
                "configured search endpoint is not a valid URL",
            )
        })?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            return Err(SearchBackendError::new(
                "WEB_SEARCH_CONFIG_INVALID",
                "configured search endpoint must be an absolute HTTP(S) URL without credentials",
            ));
        }
        Ok(Self { endpoint, fetch })
    }
}

impl SearchBackend for SearxngSearchBackend {
    fn search(
        &self,
        request: SearchRequest,
    ) -> BoxFuture<'_, Result<Vec<SearchResult>, SearchBackendError>> {
        Box::pin(async move {
            let mut url = self.endpoint.clone();
            {
                let mut query = url.query_pairs_mut();
                query.append_pair("q", &request.query);
                query.append_pair("format", "json");
                query.append_pair("safesearch", "1");
            }
            let response = self
                .fetch
                .fetch(WebFetchRequest {
                    url: url.to_string(),
                    max_bytes: MAX_FETCH_BYTES,
                    max_redirects: MAX_REDIRECTS,
                    connect_timeout: std::time::Duration::from_secs(5),
                    total_timeout: std::time::Duration::from_secs(20),
                })
                .await
                .map_err(|error| SearchBackendError::new(error.code, error.message))?;
            if response.status >= 400 {
                return Err(SearchBackendError::new(
                    "WEB_SEARCH_BACKEND_ERROR",
                    format!("search provider returned HTTP {}", response.status),
                ));
            }
            let media_type = response
                .content_type
                .split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if media_type != "application/json" {
                return Err(SearchBackendError::new(
                    "WEB_SEARCH_BACKEND_ERROR",
                    "search provider did not return JSON",
                ));
            }
            let payload: Value = serde_json::from_slice(&response.body).map_err(|_| {
                SearchBackendError::new(
                    "WEB_SEARCH_BACKEND_ERROR",
                    "search provider returned invalid JSON",
                )
            })?;
            let rows = payload
                .get("results")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    SearchBackendError::new(
                        "WEB_SEARCH_BACKEND_ERROR",
                        "search provider response has no results array",
                    )
                })?;
            let source_host = self.endpoint.host_str().unwrap_or("http-search");
            let results = rows
                .iter()
                .take(request.limit)
                .enumerate()
                .filter_map(|(index, row)| {
                    let title = row.get("title")?.as_str()?.to_owned();
                    let url = row.get("url")?.as_str()?.to_owned();
                    let snippet = row
                        .get("content")
                        .or_else(|| row.get("snippet"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    let source = row
                        .get("engine")
                        .and_then(Value::as_str)
                        .unwrap_or(source_host)
                        .to_owned();
                    Some(SearchResult {
                        title,
                        url,
                        snippet,
                        source,
                        rank: index + 1,
                    })
                })
                .collect();
            Ok(results)
        })
    }
}

#[cfg(test)]
mod tests {
    use zk_tools::{WebFetchError, WebFetchResponse};

    use super::*;

    struct FakeFetch;

    impl WebFetchPort for FakeFetch {
        fn fetch(
            &self,
            request: WebFetchRequest,
        ) -> BoxFuture<'_, Result<WebFetchResponse, WebFetchError>> {
            assert!(request.url.contains("q=rust+security"));
            assert!(request.url.contains("format=json"));
            Box::pin(futures::future::ready(Ok(WebFetchResponse {
                final_url: request.url,
                status: 200,
                content_type: "application/json; charset=utf-8".to_owned(),
                body: br#"{"results":[{"title":"Rust","url":"https://www.rust-lang.org/","content":"Language","engine":"docs"}]}"#.to_vec(),
                truncated: false,
            })))
        }
    }

    #[tokio::test]
    async fn maps_searxng_json_without_credentials_or_network() {
        let backend =
            SearxngSearchBackend::new("https://search.example.com/search", Arc::new(FakeFetch))
                .expect("backend");
        let results = backend
            .search(SearchRequest {
                query: "rust security".to_owned(),
                limit: 3,
            })
            .await
            .expect("results");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Rust");
        assert_eq!(results[0].source, "docs");
    }

    #[test]
    fn rejects_non_http_and_embedded_credentials_in_config() {
        for endpoint in [
            "file:///tmp/search",
            "https://user:secret@example.com/search",
        ] {
            assert!(
                SearxngSearchBackend::new(endpoint, Arc::new(FakeFetch)).is_err(),
                "{endpoint}"
            );
        }
    }
}
