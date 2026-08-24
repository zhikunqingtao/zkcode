//! `WebSearch` tool with an injected backend and a stable provider-neutral result schema.

use std::sync::Arc;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

use crate::input::{failure, optional_usize, required_str};
use crate::tool::{Tool, ToolContext, ToolOutput};
use crate::web_fetch::is_public_web_ip;

/// Maximum Unicode scalar count accepted for a search query.
pub const MAX_SEARCH_QUERY_CHARS: usize = 1_000;
/// Maximum results exposed to the model per call.
pub const MAX_SEARCH_RESULTS: usize = 10;

/// Provider-neutral search request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchRequest {
    /// User query after whitespace trimming.
    pub query: String,
    /// Requested result cap.
    pub limit: usize,
}

/// Fixed result shape shared by MCP and HTTP search backends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    /// Result title.
    pub title: String,
    /// Canonical public HTTP(S) URL.
    pub url: String,
    /// Short provider excerpt.
    pub snippet: String,
    /// Backend/server identity without credentials.
    pub source: String,
    /// One-based rank in the final normalized result set.
    pub rank: usize,
}

/// Stable backend failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchBackendError {
    /// Machine-readable error code.
    pub code: &'static str,
    /// Redacted user-facing reason.
    pub message: String,
}

impl SearchBackendError {
    /// Constructs a backend error.
    #[must_use]
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Search provider port. Implementations must not include keys in errors or results.
pub trait SearchBackend: Send + Sync {
    /// Executes one bounded query.
    fn search(
        &self,
        request: SearchRequest,
    ) -> BoxFuture<'_, Result<Vec<SearchResult>, SearchBackendError>>;
}

/// Explicit production fallback when neither MCP search nor an HTTP provider exists.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnavailableSearchBackend;

impl SearchBackend for UnavailableSearchBackend {
    fn search(
        &self,
        _request: SearchRequest,
    ) -> BoxFuture<'_, Result<Vec<SearchResult>, SearchBackendError>> {
        Box::pin(futures::future::ready(Err(SearchBackendError::new(
            "WEB_SEARCH_UNAVAILABLE",
            "no MCP or HTTP web search provider is configured",
        ))))
    }
}

/// Model-callable `WebSearch` tool.
#[derive(Clone)]
pub struct WebSearchTool {
    backend: Arc<dyn SearchBackend>,
}

impl std::fmt::Debug for WebSearchTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebSearchTool")
            .finish_non_exhaustive()
    }
}

impl WebSearchTool {
    /// Constructs `WebSearch` around the selected production backend.
    #[must_use]
    pub fn new(backend: Arc<dyn SearchBackend>) -> Self {
        Self { backend }
    }
}

impl Tool for WebSearchTool {
    fn name(&self) -> &'static str {
        "WebSearch"
    }

    fn description(&self) -> &'static str {
        "Search the public web through a configured MCP or HTTP provider."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["query"],
            "properties": {
                "query": { "type": "string", "minLength": 1, "maxLength": MAX_SEARCH_QUERY_CHARS },
                "limit": { "type": "integer", "minimum": 1, "maximum": MAX_SEARCH_RESULTS }
            }
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let Ok(query) = required_str(&input, "query") else {
                return failure("WEB_SEARCH_QUERY_INVALID", "query is required");
            };
            let query = query.trim();
            if query.is_empty() || query.chars().count() > MAX_SEARCH_QUERY_CHARS {
                return failure(
                    "WEB_SEARCH_QUERY_INVALID",
                    format!("query must contain 1 to {MAX_SEARCH_QUERY_CHARS} characters"),
                );
            }
            let limit = optional_usize(&input, "limit").unwrap_or(5);
            if !(1..=MAX_SEARCH_RESULTS).contains(&limit) {
                return failure(
                    "WEB_SEARCH_LIMIT_INVALID",
                    format!("limit must be between 1 and {MAX_SEARCH_RESULTS}"),
                );
            }
            let raw_results = match self
                .backend
                .search(SearchRequest {
                    query: query.to_owned(),
                    limit,
                })
                .await
            {
                Ok(results) => results,
                Err(error) => return failure(error.code, error.message),
            };
            let mut results = Vec::with_capacity(limit.min(raw_results.len()));
            for mut result in raw_results {
                if results.len() == limit {
                    break;
                }
                let Some(url) = normalize_result_url(&result.url) else {
                    continue;
                };
                result.url = url;
                result.rank = results.len() + 1;
                result.title = truncate(&result.title, 500);
                result.snippet = truncate(&result.snippet, 2_000);
                result.source = truncate(&result.source, 200);
                results.push(result);
            }
            let content =
                serde_json::to_string_pretty(&results).unwrap_or_else(|_| "[]".to_owned());
            ToolOutput {
                content,
                is_error: false,
                metadata: Some(json!({ "results": results, "count": results.len() })),
            }
        })
    }
}

fn normalize_result_url(raw: &str) -> Option<String> {
    let mut url = Url::parse(raw).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    if let Some(host) = url.host_str() {
        let host = host.trim_matches(['[', ']']);
        if host.eq_ignore_ascii_case("localhost")
            || host.ends_with(".localhost")
            || host.parse().is_ok_and(|ip| !is_public_web_ip(ip))
        {
            return None;
        }
    }
    url.set_fragment(None);
    Some(url.to_string())
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        value.to_owned()
    } else {
        value.chars().take(max_chars).collect()
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    struct FakeBackend(Vec<SearchResult>);

    impl SearchBackend for FakeBackend {
        fn search(
            &self,
            _request: SearchRequest,
        ) -> BoxFuture<'_, Result<Vec<SearchResult>, SearchBackendError>> {
            Box::pin(futures::future::ready(Ok(self.0.clone())))
        }
    }

    fn context() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    #[tokio::test]
    async fn unavailable_backend_returns_stable_error() {
        let tool = WebSearchTool::new(Arc::new(UnavailableSearchBackend));
        let output = tool.execute(json!({ "query": "rust" }), context()).await;
        assert!(output.is_error);
        assert_eq!(
            output.content,
            "WEB_SEARCH_UNAVAILABLE: no MCP or HTTP web search provider is configured"
        );
    }

    #[tokio::test]
    async fn normalizes_ranks_and_drops_unsafe_result_urls() {
        let backend = FakeBackend(vec![
            SearchResult {
                title: "private".to_owned(),
                url: "http://127.0.0.1/admin".to_owned(),
                snippet: "no".to_owned(),
                source: "fake".to_owned(),
                rank: 99,
            },
            SearchResult {
                title: "public".to_owned(),
                url: "HTTPS://Example.COM:443/page#fragment".to_owned(),
                snippet: "ok".to_owned(),
                source: "fake".to_owned(),
                rank: 42,
            },
        ]);
        let tool = WebSearchTool::new(Arc::new(backend));
        let output = tool
            .execute(json!({ "query": "rust", "limit": 2 }), context())
            .await;
        assert!(!output.is_error, "{}", output.content);
        let results = output.metadata.expect("metadata")["results"]
            .as_array()
            .expect("results")
            .clone();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["rank"], 1);
        assert_eq!(results[0]["url"], "https://example.com/page");
    }

    #[tokio::test]
    async fn invalid_query_and_limit_are_rejected_before_backend() {
        let tool = WebSearchTool::new(Arc::new(FakeBackend(Vec::new())));
        for input in [
            json!({ "query": " " }),
            json!({ "query": "x", "limit": 11 }),
        ] {
            let output = tool.execute(input, context()).await;
            assert!(output.is_error);
        }
    }
}
