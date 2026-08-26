//! Production `WebSearch` backend over the process-wide MCP client manager.
//!
//! The model-facing built-in tool keeps its stable `WebSearch` name while this
//! adapter invokes the exact `DashScope` marketplace server/tool pair used by the
//! tracked capability registry: `zhipu-websearch` / `webSearchPro`.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::{Value, json};
use zk_mcp::{McpClientManager, McpConnectionStatus, McpServerConnection};
use zk_tools::{SearchBackend, SearchBackendError, SearchRequest, SearchResult};

/// Capability-registry server key derived from the tracked `DashScope` endpoint.
pub const ZHIPU_WEB_SEARCH_SERVER: &str = "zhipu-websearch";
/// Exact remote tool exposed by the Zhipu web-search MCP server.
pub const ZHIPU_WEB_SEARCH_TOOL: &str = "webSearchPro";
/// Hard wall-clock limit for one built-in search request.
pub const MCP_SEARCH_TIMEOUT: Duration = Duration::from_secs(30);

/// MCP implementation of the built-in provider-neutral [`SearchBackend`].
#[derive(Clone)]
pub struct McpSearchBackend {
    manager: Arc<OnceLock<Arc<McpClientManager>>>,
}

impl std::fmt::Debug for McpSearchBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpSearchBackend")
            .field("manager_initialized", &self.manager.get().is_some())
            .finish_non_exhaustive()
    }
}

impl McpSearchBackend {
    /// Binds the same lazy manager slot owned by [`crate::state::AppState`].
    #[must_use]
    pub fn new(manager: Arc<OnceLock<Arc<McpClientManager>>>) -> Self {
        Self { manager }
    }
}

impl SearchBackend for McpSearchBackend {
    fn search(
        &self,
        request: SearchRequest,
    ) -> BoxFuture<'_, Result<Vec<SearchResult>, SearchBackendError>> {
        Box::pin(async move {
            let manager = self.manager.get().cloned().ok_or_else(|| {
                SearchBackendError::new(
                    "WEB_SEARCH_UNAVAILABLE",
                    "MCP web search is not initialized",
                )
            })?;
            let connection = manager
                .get_connection(ZHIPU_WEB_SEARCH_SERVER)
                .ok_or_else(|| {
                    SearchBackendError::new(
                        "WEB_SEARCH_UNAVAILABLE",
                        "MCP server 'zhipu-websearch' is not configured",
                    )
                })?;
            if connection.status() != McpConnectionStatus::Connected {
                return Err(SearchBackendError::new(
                    "WEB_SEARCH_UNAVAILABLE",
                    "MCP server 'zhipu-websearch' is not connected",
                ));
            }
            search_connected(&connection, request).await
        })
    }
}

async fn search_connected(
    connection: &McpServerConnection,
    request: SearchRequest,
) -> Result<Vec<SearchResult>, SearchBackendError> {
    let limit = request.limit;
    let call = connection.call_tool(
        ZHIPU_WEB_SEARCH_TOOL,
        Some(json!({
            "search_query": request.query,
            "count": limit,
        })),
        MCP_SEARCH_TIMEOUT,
        None,
    );
    let response = tokio::time::timeout(MCP_SEARCH_TIMEOUT, call)
        .await
        .map_err(|_| {
            SearchBackendError::new(
                "WEB_SEARCH_TIMEOUT",
                "MCP web search exceeded the 30 second deadline",
            )
        })?
        .map_err(|error| {
            SearchBackendError::new(
                "WEB_SEARCH_BACKEND_ERROR",
                format!("MCP web search failed ({})", error.code()),
            )
        })?;
    parse_search_response(response.as_ref(), limit)
}

fn parse_search_response(
    response: Option<&Value>,
    limit: usize,
) -> Result<Vec<SearchResult>, SearchBackendError> {
    let response = response.ok_or_else(|| invalid_response("MCP web search returned no result"))?;
    if response.get("isError").and_then(Value::as_bool) == Some(true) {
        return Err(SearchBackendError::new(
            "WEB_SEARCH_BACKEND_ERROR",
            "MCP web search reported a tool error",
        ));
    }
    let rows = extract_result_rows(response)
        .ok_or_else(|| invalid_response("MCP web search response contains no results array"))?;
    Ok(rows
        .into_iter()
        .take(limit)
        .enumerate()
        .filter_map(|(index, row)| map_result(&row, index + 1))
        .collect())
}

fn invalid_response(message: &'static str) -> SearchBackendError {
    SearchBackendError::new("WEB_SEARCH_BACKEND_ERROR", message)
}

fn extract_result_rows(response: &Value) -> Option<Vec<Value>> {
    if let Some(rows) = rows_in_value(response) {
        return Some(rows.to_vec());
    }
    let content = response.get("content")?.as_array()?;
    for item in content {
        if item.get("type").and_then(Value::as_str) != Some("text") {
            continue;
        }
        let text = item.get("text").and_then(Value::as_str)?;
        let parsed = parse_text_payload(text)?;
        if let Some(rows) = rows_in_value(&parsed) {
            return Some(rows.to_vec());
        }
    }
    None
}

fn rows_in_value(value: &Value) -> Option<&[Value]> {
    value
        .get("results")
        .and_then(Value::as_array)
        .or_else(|| {
            value
                .get("structuredContent")
                .and_then(|structured| structured.get("results"))
                .and_then(Value::as_array)
        })
        .or_else(|| {
            value
                .get("data")
                .and_then(|data| data.get("results"))
                .and_then(Value::as_array)
        })
        .map(Vec::as_slice)
        .or_else(|| value.as_array().map(Vec::as_slice))
}

fn parse_text_payload(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return decode_nested_json(value);
    }
    let fenced = trimmed
        .strip_prefix("```json\n")
        .or_else(|| trimmed.strip_prefix("```\n"))?;
    let fenced = fenced.strip_suffix("```")?.trim();
    serde_json::from_str::<Value>(fenced)
        .ok()
        .and_then(decode_nested_json)
}

fn decode_nested_json(value: Value) -> Option<Value> {
    if let Some(encoded) = value.as_str() {
        serde_json::from_str(encoded).ok()
    } else {
        Some(value)
    }
}

fn map_result(row: &Value, rank: usize) -> Option<SearchResult> {
    let title = row.get("title")?.as_str()?.to_owned();
    let url = row
        .get("url")
        .or_else(|| row.get("link"))?
        .as_str()?
        .to_owned();
    let snippet = row
        .get("content")
        .or_else(|| row.get("snippet"))
        .or_else(|| row.get("description"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let source = row
        .get("site_name")
        .or_else(|| row.get("source"))
        .and_then(Value::as_str)
        .unwrap_or(ZHIPU_WEB_SEARCH_SERVER)
        .to_owned();
    Some(SearchResult {
        title,
        url,
        snippet,
        source,
        rank,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use zk_mcp::{McpServerConfig, McpTransportType};

    #[test]
    fn parses_real_mcp_text_and_structured_content_shapes() {
        let text = json!({
            "content": [{
                "type": "text",
                "text": "{\"results\":[{\"title\":\"Rust\",\"url\":\"https://www.rust-lang.org/\",\"content\":\"Language\",\"site_name\":\"Rust\"}]}"
            }]
        });
        let parsed = parse_search_response(Some(&text), 5).expect("text results");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].snippet, "Language");
        assert_eq!(parsed[0].source, "Rust");

        let structured = json!({
            "structuredContent": {
                "results": [{
                    "title": "Docs",
                    "url": "https://example.com/docs",
                    "snippet": "Reference"
                }]
            }
        });
        let parsed = parse_search_response(Some(&structured), 1).expect("structured results");
        assert_eq!(parsed[0].source, ZHIPU_WEB_SEARCH_SERVER);
    }

    #[tokio::test]
    async fn real_stdio_mcp_path_calls_exact_tool_and_arguments() {
        let connection = McpServerConnection::new(McpServerConfig::stdio(
            ZHIPU_WEB_SEARCH_SERVER,
            "/bin/sh",
            vec!["-c".to_owned(), exact_search_server_script()],
        ));
        connection.connect().await;
        assert_eq!(connection.status(), McpConnectionStatus::Connected);

        let results = search_connected(
            &connection,
            SearchRequest {
                query: "rust security".to_owned(),
                limit: 2,
            },
        )
        .await
        .expect("exact MCP call succeeds");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Rust Security");
        assert_eq!(results[0].source, "Example");
        connection.close().await;
    }

    #[tokio::test]
    #[ignore = "requires a real DashScope key and makes one bounded network search"]
    async fn dashscope_web_search_live() {
        let key = [
            "LLM_PROVIDER_DASHSCOPE_API_KEY",
            "LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY",
            "DASHSCOPE_API_KEY",
        ]
        .into_iter()
        .find_map(|name| std::env::var(name).ok())
        .filter(|value| !value.trim().is_empty())
        .expect("a DashScope API key environment variable is required");
        let mut config = McpServerConfig::sse(
            ZHIPU_WEB_SEARCH_SERVER,
            "https://dashscope.aliyuncs.com/api/v1/mcps/zhipu-websearch/sse",
        );
        config.headers = BTreeMap::from([("Authorization".to_owned(), format!("Bearer {key}"))]);
        let results = tokio::time::timeout(Duration::from_secs(45), async move {
            let connection = McpServerConnection::new(config);
            connection.connect().await;
            assert_eq!(connection.status(), McpConnectionStatus::Connected);
            let results = search_connected(
                &connection,
                SearchRequest {
                    query: "Rust programming language official website".to_owned(),
                    limit: 1,
                },
            )
            .await
            .expect("bounded live search");
            connection.close().await;
            results
        })
        .await
        .expect("live MCP smoke exceeded its 45 second total deadline");
        assert!(!results.is_empty());
        assert!(results[0].url.starts_with("http"));
    }

    fn exact_search_server_script() -> String {
        r#"while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"serverInfo":{"name":"search-test"}}}\n' "$id" ;;
    *'"method":"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"webSearchPro","description":"search","inputSchema":{"type":"object"}}]}}\n' "$id" ;;
    *'"method":"tools/call"'*)
      valid=true
      case "$line" in *'"name":"webSearchPro"'*) ;; *) valid=false ;; esac
      case "$line" in *'"count":2'*) ;; *) valid=false ;; esac
      case "$line" in *'"search_query":"rust security"'*) ;; *) valid=false ;; esac
      if [ "$valid" = true ]; then
        printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"{\\"results\\":[{\\"title\\":\\"Rust Security\\",\\"url\\":\\"https://example.com/rust\\",\\"content\\":\\"Safe systems\\",\\"site_name\\":\\"Example\\"}]}"}]}}\n' "$id"
      else
        printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32602,"message":"wrong tool or arguments"}}\n' "$id"
      fi ;;
  esac
done"#
            .to_owned()
    }

    #[test]
    fn timeout_and_transport_constants_are_frozen() {
        assert_eq!(MCP_SEARCH_TIMEOUT, Duration::from_secs(30));
        assert_eq!(ZHIPU_WEB_SEARCH_SERVER, "zhipu-websearch");
        assert_eq!(ZHIPU_WEB_SEARCH_TOOL, "webSearchPro");
        assert_eq!(
            McpTransportType::Sse.as_str(),
            "SSE",
            "tracked capability uses SSE"
        );
    }
}
