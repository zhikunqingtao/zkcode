//! Persistent-memory tool backed through a narrow storage port.
//!
//! Project scope is selected from [`ToolContext::working_dir`] unless the
//! model explicitly sends `"scope":"global"`. The tool itself never reads or
//! writes `MEMORY.md`; the composition root supplies the `SQLite` adapter.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::input::{failure, optional_str, required_str_allow_empty};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// Memory scope understood by the tool-side storage port.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryScope {
    /// Memory isolated to the invocation's project directory.
    Project,
    /// User-wide memory, available only when explicitly requested.
    Global,
}

/// Validated target passed from the tool to its storage adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryTarget {
    scope: MemoryScope,
    project_path: Option<PathBuf>,
}

impl MemoryTarget {
    /// Select project memory for an invocation working directory.
    #[must_use]
    pub fn project(project_path: impl Into<PathBuf>) -> Self {
        Self {
            scope: MemoryScope::Project,
            project_path: Some(project_path.into()),
        }
    }

    /// Select global memory explicitly.
    #[must_use]
    pub const fn global() -> Self {
        Self {
            scope: MemoryScope::Global,
            project_path: None,
        }
    }

    /// Selected scope.
    #[must_use]
    pub const fn scope(&self) -> MemoryScope {
        self.scope
    }

    /// Project path, present only for project targets.
    #[must_use]
    pub fn project_path(&self) -> Option<&Path> {
        self.project_path.as_deref()
    }
}

/// SQLite-agnostic memory storage port.
pub trait MemoryStore: Send + Sync {
    /// Render memories from exactly one target for the model.
    fn read_memories(&self, target: MemoryTarget) -> BoxFuture<'_, Result<String, String>>;

    /// Store semantic memory with source `TOOL` in exactly one target.
    fn write_tool_memory(
        &self,
        target: MemoryTarget,
        content: String,
    ) -> BoxFuture<'_, Result<(), String>>;

    /// Delete literal content matches from exactly one target.
    fn delete_memory(
        &self,
        target: MemoryTarget,
        pattern: String,
    ) -> BoxFuture<'_, Result<bool, String>>;
}

/// LLM-facing `Memory` tool.
#[derive(Clone)]
pub struct MemoryTool {
    store: Arc<dyn MemoryStore>,
}

impl std::fmt::Debug for MemoryTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("MemoryTool").finish_non_exhaustive()
    }
}

impl MemoryTool {
    /// Construct with the authoritative storage adapter.
    #[must_use]
    pub fn with_store(store: Arc<dyn MemoryStore>) -> Self {
        Self { store }
    }
}

impl Tool for MemoryTool {
    fn name(&self) -> &'static str {
        "Memory"
    }

    fn description(&self) -> &'static str {
        "Read or write persistent memories. Memory is project-scoped by default; \
         set scope to global only for user-wide preferences."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["read", "write", "delete"],
                    "description": "The action to perform"
                },
                "content": {
                    "type": "string",
                    "description": "Content to write or literal content pattern to delete"
                },
                "scope": {
                    "type": "string",
                    "enum": ["project", "global"],
                    "default": "project",
                    "description": "Defaults to the current project. Global must be explicit."
                }
            },
            "required": ["action"]
        })
    }

    fn is_read_only(&self, input: &Value) -> bool {
        action_or_default(input) == "read"
    }

    fn execute(&self, input: Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let action = match required_str_allow_empty(&input, "action") {
                Ok(action) => action,
                Err(rejected) => return rejected,
            };
            let target = match target_from_input(&input, ctx.working_dir()) {
                Ok(target) => target,
                Err(output) => return output,
            };
            match action {
                "read" => match self.store.read_memories(target).await {
                    Ok(memories) if memories.is_empty() => {
                        ToolOutput::ok("No memories stored yet.")
                    }
                    Ok(memories) => ToolOutput::ok(memories),
                    Err(reason) => failure("MEMORY_READ_FAILED", reason),
                },
                "write" => {
                    let Some(content) = optional_str(&input, "content") else {
                        return failure(
                            "MEMORY_CONTENT_REQUIRED",
                            "Content is required for write action.",
                        );
                    };
                    if content.trim().is_empty() {
                        return failure(
                            "MEMORY_CONTENT_REQUIRED",
                            "Content is required for write action.",
                        );
                    }
                    match self
                        .store
                        .write_tool_memory(target, content.to_owned())
                        .await
                    {
                        Ok(()) => ToolOutput::ok("Memory saved."),
                        Err(reason) => failure("MEMORY_WRITE_FAILED", reason),
                    }
                }
                "delete" => {
                    let Some(pattern) = optional_str(&input, "content") else {
                        return failure(
                            "MEMORY_PATTERN_REQUIRED",
                            "Content (search pattern) is required for delete action.",
                        );
                    };
                    if pattern.trim().is_empty() {
                        return failure(
                            "MEMORY_PATTERN_REQUIRED",
                            "Content (search pattern) is required for delete action.",
                        );
                    }
                    match self.store.delete_memory(target, pattern.to_owned()).await {
                        Ok(true) => ToolOutput::ok("Memory deleted."),
                        Ok(false) => failure("MEMORY_NOT_FOUND", "No matching memory found."),
                        Err(reason) => failure("MEMORY_DELETE_FAILED", reason),
                    }
                }
                other => failure("MEMORY_ACTION_INVALID", format!("Unknown action: {other}")),
            }
        })
    }
}

fn target_from_input(input: &Value, working_dir: &Path) -> Result<MemoryTarget, ToolOutput> {
    match input.get("scope") {
        None => Ok(MemoryTarget::project(working_dir)),
        Some(Value::String(value)) if value == "project" => Ok(MemoryTarget::project(working_dir)),
        Some(Value::String(value)) if value == "global" => Ok(MemoryTarget::global()),
        _ => Err(failure(
            "MEMORY_SCOPE_INVALID",
            "Scope must be either project or global.",
        )),
    }
}

fn action_or_default(input: &Value) -> &str {
    input
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("read")
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    #[derive(Default)]
    struct StubStore {
        calls: Mutex<Vec<(String, MemoryTarget)>>,
    }

    impl MemoryStore for StubStore {
        fn read_memories(&self, target: MemoryTarget) -> BoxFuture<'_, Result<String, String>> {
            self.calls.lock().unwrap().push(("read".to_owned(), target));
            Box::pin(async { Ok(String::new()) })
        }

        fn write_tool_memory(
            &self,
            target: MemoryTarget,
            content: String,
        ) -> BoxFuture<'_, Result<(), String>> {
            self.calls
                .lock()
                .unwrap()
                .push((format!("write:{content}"), target));
            Box::pin(async { Ok(()) })
        }

        fn delete_memory(
            &self,
            target: MemoryTarget,
            pattern: String,
        ) -> BoxFuture<'_, Result<bool, String>> {
            self.calls
                .lock()
                .unwrap()
                .push((format!("delete:{pattern}"), target));
            Box::pin(async { Ok(true) })
        }
    }

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx).with_working_dir("/work/project-a")
    }

    #[tokio::test]
    async fn project_is_default_and_uses_invocation_working_dir() {
        let store = Arc::new(StubStore::default());
        let output = MemoryTool::with_store(store.clone())
            .execute(json!({"action": "read"}), ctx())
            .await;
        assert!(!output.is_error);
        let calls = store.calls.lock().unwrap();
        assert_eq!(calls[0].1.scope(), MemoryScope::Project);
        assert_eq!(
            calls[0].1.project_path(),
            Some(Path::new("/work/project-a"))
        );
    }

    #[tokio::test]
    async fn global_requires_explicit_scope() {
        let store = Arc::new(StubStore::default());
        let tool = MemoryTool::with_store(store.clone());
        let output = tool
            .execute(
                json!({"action": "write", "scope": "global", "content": "preference"}),
                ctx(),
            )
            .await;
        assert!(!output.is_error);
        assert_eq!(store.calls.lock().unwrap()[0].1, MemoryTarget::global());

        let rejected = tool
            .execute(json!({"action": "read", "scope": "GLOBAL"}), ctx())
            .await;
        assert!(rejected.is_error);
        assert!(rejected.content.contains("MEMORY_SCOPE_INVALID"));
    }

    #[test]
    fn schema_advertises_project_default() {
        let schema = MemoryTool::with_store(Arc::new(StubStore::default())).parameters();
        assert_eq!(schema["properties"]["scope"]["default"], "project");
        assert_eq!(
            schema["properties"]["scope"]["enum"],
            json!(["project", "global"])
        );
    }
}
