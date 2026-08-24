//! MCP prompt template adapter exposed as a model-callable tool.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde_json::{Map, Value, json};
use zk_tools::{McpToolIdentity, Tool, ToolContext, ToolOutput};

use crate::connection::McpServerConnection;
use crate::protocol::PromptDefinition;
use crate::tool_adapter::{MCP_TOOL_NAME_PREFIX, mcp_config_hash};

const MAX_ARGUMENT_BYTES: usize = 16 * 1024;

/// One remote prompt registered under the owning server prefix.
pub struct McpPromptAdapter {
    name: String,
    description: String,
    parameters: Value,
    connection: Arc<McpServerConnection>,
    definition: PromptDefinition,
    identity: McpToolIdentity,
}

impl McpPromptAdapter {
    /// Construct a server-scoped prompt tool.
    #[must_use]
    pub fn new(connection: Arc<McpServerConnection>, definition: PromptDefinition) -> Self {
        let server = connection.name().to_owned();
        let name = format!(
            "{MCP_TOOL_NAME_PREFIX}{server}__prompt__{}",
            definition.name
        );
        let mut properties = Map::new();
        let mut required = Vec::new();
        for argument in &definition.arguments {
            properties.insert(
                argument.name.clone(),
                json!({
                    "type": "string",
                    "description": argument.description,
                    "maxLength": MAX_ARGUMENT_BYTES,
                }),
            );
            if argument.required {
                required.push(Value::String(argument.name.clone()));
            }
        }
        let parameters = json!({
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false,
        });
        let identity = McpToolIdentity {
            server_id: server.clone(),
            server_name: server.clone(),
            tool_name: format!("prompts/get:{}", definition.name),
            capability_id: Some(format!("prompt:{}", definition.name)),
            resource_scope: Some(format!("mcp://{server}/prompts/{}", definition.name)),
            domain_scope: Some("prompts".to_owned()),
            config_hash: mcp_config_hash(connection.config()),
        };
        Self {
            name,
            description: if definition.description.trim().is_empty() {
                format!(
                    "Render MCP prompt '{}' from server '{server}'",
                    definition.name
                )
            } else {
                definition.description.clone()
            },
            parameters,
            connection,
            definition,
            identity,
        }
    }

    fn arguments(&self, input: &Value) -> Result<BTreeMap<String, String>, ToolOutput> {
        let Some(object) = input.as_object() else {
            return Err(ToolOutput::error(
                "MCP_PROMPT_ARGUMENTS_INVALID: expected object",
            ));
        };
        let mut values = BTreeMap::new();
        for argument in &self.definition.arguments {
            match object.get(&argument.name) {
                Some(Value::String(value)) if value.len() <= MAX_ARGUMENT_BYTES => {
                    if argument.required && value.trim().is_empty() {
                        return Err(ToolOutput::error(format!(
                            "MCP_PROMPT_ARGUMENT_REQUIRED: {}",
                            argument.name
                        )));
                    }
                    values.insert(argument.name.clone(), value.clone());
                }
                Some(Value::String(_)) => {
                    return Err(ToolOutput::error(format!(
                        "MCP_PROMPT_ARGUMENT_TOO_LARGE: {}",
                        argument.name
                    )));
                }
                Some(_) => {
                    return Err(ToolOutput::error(format!(
                        "MCP_PROMPT_ARGUMENT_INVALID: {}",
                        argument.name
                    )));
                }
                None if argument.required => {
                    return Err(ToolOutput::error(format!(
                        "MCP_PROMPT_ARGUMENT_REQUIRED: {}",
                        argument.name
                    )));
                }
                None => {}
            }
        }
        Ok(values)
    }
}

impl Tool for McpPromptAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters(&self) -> Value {
        self.parameters.clone()
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(30)
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let arguments = match self.arguments(&input) {
                Ok(arguments) => arguments,
                Err(error) => return error,
            };
            match self
                .connection
                .get_prompt(&self.definition.name, &arguments)
                .await
            {
                Ok(messages) => ToolOutput::ok(
                    serde_json::to_string(&messages).unwrap_or_else(|_| "[]".to_owned()),
                ),
                Err(error) => ToolOutput::error(format!("MCP_PROMPT_FAILED: {error}")),
            }
        })
    }

    fn mcp_identity(&self) -> Option<&McpToolIdentity> {
        Some(&self.identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{McpServerConfig, PromptArgument};

    #[test]
    fn schema_and_identity_are_server_scoped() {
        let connection =
            McpServerConnection::new(McpServerConfig::sse("alpha", "http://127.0.0.1:9"));
        let adapter = McpPromptAdapter::new(
            connection,
            PromptDefinition {
                name: "review".to_owned(),
                description: "Review code".to_owned(),
                arguments: vec![PromptArgument {
                    name: "path".to_owned(),
                    description: String::new(),
                    required: true,
                }],
            },
        );
        assert_eq!(adapter.name(), "mcp__alpha__prompt__review");
        assert_eq!(adapter.parameters()["required"], json!(["path"]));
        let identity = adapter.mcp_identity().unwrap();
        assert_eq!(identity.server_id, "alpha");
        assert_eq!(identity.capability_id.as_deref(), Some("prompt:review"));
    }
}
