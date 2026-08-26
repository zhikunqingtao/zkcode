//! Model-callable MCP resource tools sharing the process-wide client manager.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::{Value, json};
use zk_mcp::{McpClientManager, McpConnectionStatus, ResourceDefinition};
use zk_tools::{Tool, ToolContext, ToolOutput};

const MAX_LISTED_RESOURCES: usize = 256;
pub(crate) const MAX_RESOURCE_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_URI_BYTES: usize = 4096;
const ALLOWED_SCHEMES: [&str; 8] = [
    "file", "http", "https", "mcp", "resource", "postgres", "sqlite", "memory",
];

/// Shared policy result for every MCP resource entry point (model tool, REST,
/// and reverse MCP server).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResourcePolicyError {
    UriRejected,
    NotDeclared,
    MimeRejected,
}

/// Require an exact resources/list declaration before resources/read, and
/// validate its URI scheme and declared MIME type.
pub(crate) fn validate_declared_resource<'a>(
    uri: &str,
    declared: &'a [ResourceDefinition],
) -> Result<&'a ResourceDefinition, ResourcePolicyError> {
    if uri.len() > MAX_URI_BYTES || !allowed_uri(uri) {
        return Err(ResourcePolicyError::UriRejected);
    }
    let resource = declared
        .iter()
        .find(|resource| resource.uri == uri)
        .ok_or(ResourcePolicyError::NotDeclared)?;
    if !allowed_mime(resource.mime_type.as_deref()) {
        return Err(ResourcePolicyError::MimeRejected);
    }
    Ok(resource)
}

fn manager(slot: &OnceLock<Arc<McpClientManager>>) -> Result<Arc<McpClientManager>, ToolOutput> {
    slot.get()
        .cloned()
        .ok_or_else(|| ToolOutput::error("MCP_MANAGER_UNAVAILABLE: MCP is not initialized"))
}

/// List resources from all connected MCP servers.
pub struct ListMcpResourcesTool {
    manager: Arc<OnceLock<Arc<McpClientManager>>>,
}

impl ListMcpResourcesTool {
    /// Bind the process-wide lazy MCP manager.
    #[must_use]
    pub fn new(manager: Arc<OnceLock<Arc<McpClientManager>>>) -> Self {
        Self { manager }
    }
}

impl Tool for ListMcpResourcesTool {
    fn name(&self) -> &'static str {
        "ListMcpResources"
    }

    fn description(&self) -> &'static str {
        "List bounded MCP resources from connected servers. Returns server identity, URI, MIME type, name, and description."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "server": {"type": "string", "description": "Optional exact MCP server name"}
            },
            "additionalProperties": false
        })
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(30)
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let manager = match manager(&self.manager) {
                Ok(manager) => manager,
                Err(error) => return error,
            };
            let requested = input.get("server").and_then(Value::as_str);
            let mut output = Vec::new();
            for connection in manager.connected_servers() {
                if requested.is_some_and(|server| server != connection.name()) {
                    continue;
                }
                for resource in connection.discover_resources().await {
                    if output.len() >= MAX_LISTED_RESOURCES {
                        break;
                    }
                    output.push(json!({
                        "server": connection.name(),
                        "uri": resource.uri,
                        "name": resource.name,
                        "mimeType": resource.mime_type,
                        "description": resource.description,
                    }));
                }
            }
            let truncated = output.len() == MAX_LISTED_RESOURCES;
            ToolOutput::ok(
                serde_json::to_string(&json!({
                    "resources": output,
                    "truncated": truncated,
                }))
                .unwrap_or_else(|_| "{\"resources\":[]}".to_owned()),
            )
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }
}

/// Read one declared MCP resource with server, URI, MIME, and size enforcement.
pub struct ReadMcpResourceTool {
    manager: Arc<OnceLock<Arc<McpClientManager>>>,
}

impl ReadMcpResourceTool {
    /// Bind the process-wide lazy MCP manager.
    #[must_use]
    pub fn new(manager: Arc<OnceLock<Arc<McpClientManager>>>) -> Self {
        Self { manager }
    }
}

impl Tool for ReadMcpResourceTool {
    fn name(&self) -> &'static str {
        "ReadMcpResource"
    }

    fn description(&self) -> &'static str {
        "Read one resource declared by an exact connected MCP server. URI scheme, MIME type, and response size are restricted."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "server": {"type": "string", "minLength": 1},
                "uri": {"type": "string", "minLength": 1, "maxLength": MAX_URI_BYTES}
            },
            "required": ["server", "uri"],
            "additionalProperties": false
        })
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(30)
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let Some(server) = input.get("server").and_then(Value::as_str) else {
                return ToolOutput::error("MCP_RESOURCE_SERVER_REQUIRED");
            };
            let Some(uri) = input.get("uri").and_then(Value::as_str) else {
                return ToolOutput::error("MCP_RESOURCE_URI_REQUIRED");
            };
            let manager = match manager(&self.manager) {
                Ok(manager) => manager,
                Err(error) => return error,
            };
            let Some(connection) = manager.get_connection(server) else {
                return ToolOutput::error("MCP_RESOURCE_SERVER_NOT_FOUND");
            };
            if connection.status() != McpConnectionStatus::Connected {
                return ToolOutput::error("MCP_RESOURCE_SERVER_NOT_CONNECTED");
            }
            let resources = connection.discover_resources().await;
            let declared = match validate_declared_resource(uri, &resources) {
                Ok(declared) => declared,
                Err(ResourcePolicyError::UriRejected) => {
                    return ToolOutput::error("MCP_RESOURCE_URI_REJECTED");
                }
                Err(ResourcePolicyError::NotDeclared) => {
                    return ToolOutput::error("MCP_RESOURCE_NOT_DECLARED");
                }
                Err(ResourcePolicyError::MimeRejected) => {
                    return ToolOutput::error("MCP_RESOURCE_MIME_REJECTED");
                }
            };
            match connection.read_resource(uri).await {
                Ok(content) if content.len() <= MAX_RESOURCE_BYTES => ToolOutput::ok(
                    serde_json::to_string(&json!({
                        "server": server,
                        "uri": uri,
                        "mimeType": declared.mime_type,
                        "content": content,
                    }))
                    .unwrap_or_else(|_| "{}".to_owned()),
                ),
                Ok(_) => ToolOutput::error("MCP_RESOURCE_TOO_LARGE"),
                Err(error) => ToolOutput::error(format!("MCP_RESOURCE_READ_FAILED: {error}")),
            }
        })
    }

    fn is_read_only(&self, _input: &Value) -> bool {
        true
    }
}

pub(crate) fn allowed_uri(uri: &str) -> bool {
    let Some((scheme, rest)) = uri.split_once(':') else {
        return false;
    };
    !rest.is_empty()
        && scheme.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.')
        })
        && ALLOWED_SCHEMES.contains(&scheme.to_ascii_lowercase().as_str())
}

pub(crate) fn allowed_mime(mime: Option<&str>) -> bool {
    let Some(mime) = mime else {
        return true;
    };
    let mime = mime.to_ascii_lowercase();
    mime.starts_with("text/")
        || matches!(
            mime.split(';').next().unwrap_or_default().trim(),
            "application/json"
                | "application/xml"
                | "application/yaml"
                | "application/x-yaml"
                | "application/octet-stream"
        )
        || mime
            .split(';')
            .next()
            .is_some_and(|value| value.trim().ends_with("+json"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_scheme_and_mime_limits_fail_closed() {
        assert!(allowed_uri("file:///tmp/a"));
        assert!(allowed_uri("mcp://server/resource"));
        assert!(!allowed_uri("javascript:alert(1)"));
        assert!(!allowed_uri("data:text/plain,secret"));
        assert!(!allowed_uri("relative/path"));
        assert!(allowed_mime(Some("application/problem+json")));
        assert!(allowed_mime(Some("text/plain; charset=utf-8")));
        assert!(!allowed_mime(Some("image/png")));
    }

    #[test]
    fn resource_must_be_exactly_declared_with_safe_mime() {
        let declared = vec![ResourceDefinition {
            uri: "mcp://weather/today".to_owned(),
            name: "today".to_owned(),
            mime_type: Some("application/json".to_owned()),
            description: None,
        }];
        assert_eq!(
            validate_declared_resource("mcp://weather/today", &declared)
                .expect("declared safe resource")
                .name,
            "today"
        );
        assert_eq!(
            validate_declared_resource("mcp://weather/tomorrow", &declared),
            Err(ResourcePolicyError::NotDeclared)
        );
        assert_eq!(
            validate_declared_resource("javascript:alert(1)", &declared),
            Err(ResourcePolicyError::UriRejected)
        );

        let unsafe_mime = vec![ResourceDefinition {
            uri: "mcp://weather/image".to_owned(),
            name: "image".to_owned(),
            mime_type: Some("image/png".to_owned()),
            description: None,
        }];
        assert_eq!(
            validate_declared_resource("mcp://weather/image", &unsafe_mime),
            Err(ResourcePolicyError::MimeRejected)
        );
    }
}
