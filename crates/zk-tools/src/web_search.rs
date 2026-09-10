//! `WebSearch` tool with an injected backend and a stable provider-neutral result schema.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{SecondsFormat, Utc};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

use crate::input::{failure, optional_usize, required_str};
use crate::research::{
    MAX_RESEARCH_PROVIDER_BYTES, MAX_RESEARCH_URL_BYTES, RESEARCH_RECEIPT_SCHEMA_VERSION,
    ResearchReceipt, ResearchReceiptEntry, ResearchReceiptKind, truncate_utf8_bytes,
};
use crate::tool::{Tool, ToolContext, ToolOutput};
use crate::web_fetch::is_public_web_ip;

/// Maximum Unicode scalar count accepted for a search query.
pub const MAX_SEARCH_QUERY_CHARS: usize = 1_000;
/// Maximum results exposed to the model per call.
pub const MAX_SEARCH_RESULTS: usize = 10;
/// Empty responses tolerated before the current Run stops calling the backend.
const MAX_CONSECUTIVE_EMPTY_SEARCHES: u8 = 3;
/// Bound process memory when callers abandon Runs without another search.
const MAX_TRACKED_SEARCH_RUNS: usize = 4_096;

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
    /// Canonical public HTTP(S) URL, or empty for an unverified provider lead.
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
    empty_searches_by_run: Arc<Mutex<HashMap<String, u8>>>,
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
        Self {
            backend,
            empty_searches_by_run: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn empty_search_count(&self, run_id: Option<&str>) -> u8 {
        let Some(run_id) = run_id else {
            return 0;
        };
        *self
            .empty_searches_by_run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(run_id)
            .unwrap_or(&0)
    }

    fn record_empty_search(&self, run_id: Option<&str>) -> u8 {
        let Some(run_id) = run_id else {
            return 1;
        };
        let mut counts = self
            .empty_searches_by_run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !counts.contains_key(run_id)
            && counts.len() >= MAX_TRACKED_SEARCH_RUNS
            && let Some(expired) = counts.keys().next().cloned()
        {
            counts.remove(&expired);
        }
        let count = counts.entry(run_id.to_owned()).or_default();
        *count = count.saturating_add(1);
        *count
    }

    fn clear_empty_searches(&self, run_id: Option<&str>) {
        let Some(run_id) = run_id else {
            return;
        };
        self.empty_searches_by_run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(run_id);
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

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
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
            let run_id = ctx.run_id().map(str::to_owned);
            if self.empty_search_count(run_id.as_deref()) >= MAX_CONSECUTIVE_EMPTY_SEARCHES {
                return failure(
                    "WEB_SEARCH_EMPTY_CIRCUIT_OPEN",
                    "WebSearch returned no usable results three times in this run; switch to another configured search provider or finish with the available evidence",
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
            let results = normalize_results(raw_results, limit);
            if results.is_empty() {
                let empty_count = self.record_empty_search(run_id.as_deref());
                let (code, guidance) = if empty_count >= MAX_CONSECUTIVE_EMPTY_SEARCHES {
                    (
                        "WEB_SEARCH_EMPTY_CIRCUIT_OPEN",
                        "the empty-result circuit is now open; do not retry WebSearch in this run",
                    )
                } else {
                    (
                        "WEB_SEARCH_NO_RESULTS",
                        "try a materially different query or another configured search provider",
                    )
                };
                return failure(
                    code,
                    format!(
                        "WebSearch returned no usable results ({empty_count}/{MAX_CONSECUTIVE_EMPTY_SEARCHES}); {guidance}"
                    ),
                );
            }
            self.clear_empty_searches(run_id.as_deref());
            let mut content =
                serde_json::to_string_pretty(&results).unwrap_or_else(|_| "[]".to_owned());
            if results.iter().any(|result| result.url.is_empty()) {
                content.insert_str(0, "UNVERIFIED_SEARCH_LEADS: Entries with an empty URL are provider summaries, not verifiable sources. Never invent URLs, proxy paths or signatures. Use these only as search leads; label unsupported claims as unverified.\n");
            }
            let receipt = ResearchReceipt {
                schema_version: RESEARCH_RECEIPT_SCHEMA_VERSION,
                kind: ResearchReceiptKind::WebSearch,
                query: Some(query.to_owned()),
                fetched_at: Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true),
                entries: results
                    .iter()
                    .filter(|result| !result.url.is_empty())
                    .enumerate()
                    .map(|(index, result)| ResearchReceiptEntry {
                        url: result.url.clone(),
                        title: (!result.title.is_empty()).then(|| result.title.clone()),
                        provider: (!result.source.is_empty()).then(|| {
                            truncate_utf8_bytes(&result.source, MAX_RESEARCH_PROVIDER_BYTES)
                        }),
                        excerpt: (!result.snippet.is_empty()).then(|| result.snippet.clone()),
                        rank: u32::try_from(index + 1).ok(),
                        http_status: None,
                        content_type: None,
                        truncated: false,
                    })
                    .collect(),
            };
            ToolOutput {
                content,
                is_error: false,
                metadata: Some(json!({
                    "results": results,
                    "count": results.len(),
                    "structuredResult": {
                        "results": results,
                        "count": results.len(),
                        "research": receipt,
                    }
                })),
            }
        })
    }
}

fn normalize_results(raw_results: Vec<SearchResult>, limit: usize) -> Vec<SearchResult> {
    let mut results = Vec::with_capacity(limit.min(raw_results.len()));
    for mut result in raw_results {
        if results.len() == limit {
            break;
        }
        let url = if result.url.trim().is_empty() && !result.snippet.trim().is_empty() {
            String::new()
        } else {
            let Some(url) = normalize_result_url(&result.url) else {
                continue;
            };
            url
        };
        if url.len() > MAX_RESEARCH_URL_BYTES {
            continue;
        }
        result.url = url;
        result.rank = results.len() + 1;
        result.title = truncate(&result.title, 500);
        result.snippet = truncate(&result.snippet, 2_000);
        result.source = truncate(&result.source, 200);
        results.push(result);
    }
    results
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    #[derive(Default)]
    struct CountingEmptyBackend(AtomicUsize);

    impl SearchBackend for CountingEmptyBackend {
        fn search(
            &self,
            _request: SearchRequest,
        ) -> BoxFuture<'_, Result<Vec<SearchResult>, SearchBackendError>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Box::pin(futures::future::ready(Ok(Vec::new())))
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
        let receipt = output.research_receipt().expect("research receipt");
        assert_eq!(receipt.kind, ResearchReceiptKind::WebSearch);
        assert_eq!(receipt.query.as_deref(), Some("rust"));
        assert_eq!(receipt.entries.len(), 1);
        let results = output.metadata.expect("metadata")["results"]
            .as_array()
            .expect("results")
            .clone();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["rank"], 1);
        assert_eq!(results[0]["url"], "https://example.com/page");
        assert_eq!(receipt.entries[0].url, "https://example.com/page");
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

    #[tokio::test]
    async fn unlinked_summaries_are_leads_not_source_receipts_or_empty_results() {
        let tool = WebSearchTool::new(Arc::new(FakeBackend(vec![SearchResult {
            title: "Provider summary".to_owned(),
            url: String::new(),
            snippet: "An unverified claim".to_owned(),
            source: "provider".to_owned(),
            rank: 1,
        }])));
        for _ in 0..4 {
            let output = tool.execute(json!({"query":"example"}), context()).await;
            assert!(!output.is_error);
            assert!(output.content.starts_with("UNVERIFIED_SEARCH_LEADS:"));
            assert!(output.content.contains("An unverified claim"));
            assert!(output.research_receipt().unwrap().entries.is_empty());
        }
    }

    #[tokio::test]
    async fn repeated_empty_results_open_a_per_run_circuit() {
        let backend = Arc::new(CountingEmptyBackend::default());
        let tool = WebSearchTool::new(backend.clone());

        for attempt in 1..=4 {
            let output = tool
                .execute(
                    json!({ "query": format!("different query {attempt}") }),
                    context().with_run_id("run-empty"),
                )
                .await;
            assert!(output.is_error);
            if attempt < 3 {
                assert!(output.content.starts_with("WEB_SEARCH_NO_RESULTS:"));
            } else {
                assert!(output.content.starts_with("WEB_SEARCH_EMPTY_CIRCUIT_OPEN:"));
            }
        }

        assert_eq!(backend.0.load(Ordering::Relaxed), 3);
    }
}
