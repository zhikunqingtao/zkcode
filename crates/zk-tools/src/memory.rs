//! `Memory` 工具——LLM 主动读写跨会话长期记忆（Batch 5 Step 4）。
//!
//! 逐字对照旧 `memdir/MemoryTool.java`（只读权威规格）：工具名 `Memory`、
//! 三动作 `read` / `write` / `delete`、四条错误码
//! （`MEMORY_CONTENT_REQUIRED` / `MEMORY_PATTERN_REQUIRED` / `MEMORY_NOT_FOUND`
//! / `MEMORY_ACTION_INVALID`）与四段成功文案（`"No memories stored yet."` /
//! 记忆全文 / `"Memory saved."` / `"Memory deleted."`）。
//!
//! # 依赖方向
//!
//! 旧 `MemoryTool` 构造注入 `MemdirService`；该服务的移植体
//! `zk_engine::MemdirStore` 落在 zk-engine，而依赖方向铁律禁止
//! `zk-tools → zk-engine`。故此处只定义窄端口 [`MemoryStore`]（恰三个方法，
//! 与旧工具实际调用的 `readMemories` / `writeMemory(content, TOOL)` /
//! `deleteMemory(pattern)` 一一对应），实现落 zk-engine，装配落 zk-server
//! 组合根——范式与 [`crate::snapshot::SnapshotSink`] /
//! [`crate::elicitation::ElicitationSink`] / [`crate::config_tool::ModelCatalog`]
//! 一致。
//!
//! 端口方法名 `write_tool_memory` 把旧调用点写死的
//! `MemorySource.TOOL` 固化在**端口契约**上（而非藏进实现），因此本工具无法
//! 越权以其他来源写入。
//!
//! # 差异（留痕 docs/compatibility.md §9）
//!
//! - 旧 `call` 首行 `input.getString("action")` 在 `action` 缺失时得 `null`，
//!   随即 `switch (null)` 抛 `NullPointerException`（由执行管线兜成错误结果）。
//!   本实现按框架约定返回 `MISSING_PARAMETER`（与 [`crate::config_tool`] 同
//!   处置）；`action` **存在但为空白串**仍走旧的 `MEMORY_ACTION_INVALID`
//!   分支，故不能用拒空白的 `required_str`。
//! - 旧 `writeMemory` 失败抛 `MemdirException`（RuntimeException）穿出 `call`，
//!   由执行管线兜成通用失败。Rust 无异常，端口以 `Result` 显式回报，映射为
//!   `MEMORY_WRITE_FAILED` 错误码。
//! - 旧 `isConcurrencySafe(input)` 无 Rust 对应面（并发安全由执行器统一的
//!   信号量 + 取消树承担），只移植 `isReadOnly(input) = action == "read"`。
//! - 旧 `getPermissionRequirement() = NONE` 与 `getGroup() = "general"` 在本
//!   框架无对应面（权限判定归 2.5 授权链的 `is_read_only` / `is_destructive`
//!   事实面；工具分组无消费方），未移植。

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::{Value, json};

use crate::input::{failure, optional_str, required_str_allow_empty};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 长期记忆存储端口（旧 `MemdirService` 的三个生产调用面）。
///
/// 生产实现为 `zk_engine::MemdirStore`（用户级 `~/.zk/MEMORY.md`）。
pub trait MemoryStore: Send + Sync {
    /// 读取全部记忆原文（旧 `readMemories()`；文件缺失 / 读失败返回空串）。
    fn read_memories(&self) -> BoxFuture<'_, String>;

    /// 以 `TOOL` 来源追加一条记忆（旧
    /// `writeMemory(content, MemorySource.TOOL)`）。
    ///
    /// `Err` 携带可直接呈给模型的失败原因（旧 `MemdirException` 的消息）。
    fn write_tool_memory(&self, content: String) -> BoxFuture<'_, Result<(), String>>;

    /// 删除内容包含 `pattern` 的记忆条目，返回是否有命中
    /// （旧 `deleteMemory(pattern)`）。
    fn delete_memory(&self, pattern: String) -> BoxFuture<'_, bool>;
}

/// 长期记忆工具（名 `Memory`）。
///
/// 必须注入 [`MemoryStore`] 才能构造——没有存储的 `Memory` 工具无任何可用
/// 语义，故不提供 `Default` / 无参 `new`。
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
    /// 装配并注入记忆存储（组合根提供 zk-engine 实现）。
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
        "Read or write persistent memories that carry across sessions. \
         Use this to remember user preferences, project conventions, \
         and other important context."
    }

    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["read", "write", "delete"],
                    "description": "The action to perform: read, write, or delete"
                },
                "content": {
                    "type": "string",
                    "description": "Content to write or search pattern to delete"
                }
            },
            "required": ["action"]
        })
    }

    /// 只读判定（旧 `isReadOnly(input) = "read".equals(getString("action",
    /// "read"))`，缺省 `read`）。
    fn is_read_only(&self, input: &Value) -> bool {
        action_or_default(input) == "read"
    }

    fn execute(&self, input: Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let action = match required_str_allow_empty(&input, "action") {
                Ok(action) => action,
                Err(rejected) => return rejected,
            };
            match action {
                "read" => {
                    let memories = self.store.read_memories().await;
                    if memories.is_empty() {
                        ToolOutput::ok("No memories stored yet.")
                    } else {
                        ToolOutput::ok(memories)
                    }
                }
                "write" => {
                    let Some(content) = optional_str(&input, "content") else {
                        return failure(
                            "MEMORY_CONTENT_REQUIRED",
                            "Content is required for write action.",
                        );
                    };
                    match self.store.write_tool_memory(content.to_owned()).await {
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
                    if self.store.delete_memory(pattern.to_owned()).await {
                        ToolOutput::ok("Memory deleted.")
                    } else {
                        failure("MEMORY_NOT_FOUND", "No matching memory found.")
                    }
                }
                other => failure("MEMORY_ACTION_INVALID", format!("Unknown action: {other}")),
            }
        })
    }
}

/// `action` 取值（缺失 / 非串 → `"read"`，旧 `getString("action", "read")`）。
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

    /// 记录型存储替身：可编排 read 返回值、write 成败、delete 命中与否。
    struct StubStore {
        memories: String,
        write_result: Result<(), String>,
        delete_hit: bool,
        seen: Mutex<Vec<String>>,
    }

    impl StubStore {
        fn new() -> Self {
            Self {
                memories: String::new(),
                write_result: Ok(()),
                delete_hit: true,
                seen: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn record(&self, entry: String) {
            self.seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(entry);
        }
    }

    impl MemoryStore for StubStore {
        fn read_memories(&self) -> BoxFuture<'_, String> {
            self.record("read".to_owned());
            Box::pin(async move { self.memories.clone() })
        }

        fn write_tool_memory(&self, content: String) -> BoxFuture<'_, Result<(), String>> {
            self.record(format!("write:{content}"));
            Box::pin(async move { self.write_result.clone() })
        }

        fn delete_memory(&self, pattern: String) -> BoxFuture<'_, bool> {
            self.record(format!("delete:{pattern}"));
            Box::pin(async move { self.delete_hit })
        }
    }

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    fn tool(store: Arc<StubStore>) -> MemoryTool {
        MemoryTool::with_store(store)
    }

    #[test]
    fn spec_matches_legacy_shape() {
        let tool = tool(Arc::new(StubStore::new()));
        assert_eq!(tool.name(), "Memory");
        assert_eq!(
            tool.description(),
            "Read or write persistent memories that carry across sessions. \
             Use this to remember user preferences, project conventions, \
             and other important context."
        );
        let schema = tool.parameters();
        assert_eq!(schema["type"], "object");
        assert_eq!(
            schema["properties"]["action"]["enum"],
            json!(["read", "write", "delete"])
        );
        assert_eq!(schema["required"], json!(["action"]));
        assert_eq!(schema["properties"]["content"]["type"], "string");
    }

    #[tokio::test]
    async fn read_reports_placeholder_when_store_is_empty() {
        let store = Arc::new(StubStore::new());
        let output = tool(Arc::clone(&store))
            .execute(json!({ "action": "read" }), ctx())
            .await;
        assert!(!output.is_error);
        assert_eq!(output.content, "No memories stored yet.");
        assert_eq!(store.calls(), ["read"]);
    }

    #[tokio::test]
    async fn read_returns_raw_memories() {
        let store = Arc::new(StubStore {
            memories: "<!-- source:TOOL -->\nremember me".to_owned(),
            ..StubStore::new()
        });
        let output = tool(store)
            .execute(json!({ "action": "read" }), ctx())
            .await;
        assert!(!output.is_error);
        assert_eq!(output.content, "<!-- source:TOOL -->\nremember me");
    }

    #[tokio::test]
    async fn write_persists_content_and_confirms() {
        let store = Arc::new(StubStore::new());
        let output = tool(Arc::clone(&store))
            .execute(json!({ "action": "write", "content": "likes Rust" }), ctx())
            .await;
        assert!(!output.is_error);
        assert_eq!(output.content, "Memory saved.");
        assert_eq!(store.calls(), ["write:likes Rust"]);
    }

    #[tokio::test]
    async fn write_rejects_missing_or_blank_content() {
        for input in [
            json!({ "action": "write" }),
            json!({ "action": "write", "content": "   " }),
            json!({ "action": "write", "content": 7 }),
        ] {
            let store = Arc::new(StubStore::new());
            let output = tool(Arc::clone(&store)).execute(input, ctx()).await;
            assert!(output.is_error);
            assert_eq!(
                output.content,
                "MEMORY_CONTENT_REQUIRED: Content is required for write action."
            );
            assert!(store.calls().is_empty(), "store must not be touched");
        }
    }

    #[tokio::test]
    async fn write_failure_surfaces_as_error_code() {
        let store = Arc::new(StubStore {
            write_result: Err("Failed to write memory: disk full".to_owned()),
            ..StubStore::new()
        });
        let output = tool(store)
            .execute(json!({ "action": "write", "content": "x" }), ctx())
            .await;
        assert!(output.is_error);
        assert_eq!(
            output.content,
            "MEMORY_WRITE_FAILED: Failed to write memory: disk full"
        );
    }

    #[tokio::test]
    async fn delete_confirms_on_hit() {
        let store = Arc::new(StubStore::new());
        let output = tool(Arc::clone(&store))
            .execute(json!({ "action": "delete", "content": "Rust" }), ctx())
            .await;
        assert!(!output.is_error);
        assert_eq!(output.content, "Memory deleted.");
        assert_eq!(store.calls(), ["delete:Rust"]);
    }

    #[tokio::test]
    async fn delete_reports_not_found_on_miss() {
        let store = Arc::new(StubStore {
            delete_hit: false,
            ..StubStore::new()
        });
        let output = tool(store)
            .execute(json!({ "action": "delete", "content": "Rust" }), ctx())
            .await;
        assert!(output.is_error);
        assert_eq!(
            output.content,
            "MEMORY_NOT_FOUND: No matching memory found."
        );
    }

    #[tokio::test]
    async fn delete_rejects_missing_pattern() {
        let output = tool(Arc::new(StubStore::new()))
            .execute(json!({ "action": "delete" }), ctx())
            .await;
        assert!(output.is_error);
        assert_eq!(
            output.content,
            "MEMORY_PATTERN_REQUIRED: Content (search pattern) is required for delete action."
        );
    }

    /// 空白 action 走旧的 `MEMORY_ACTION_INVALID`（不是 `MISSING_PARAMETER`）。
    #[tokio::test]
    async fn unknown_action_reports_legacy_code() {
        for (input, expected) in [
            (
                json!({ "action": "purge" }),
                "MEMORY_ACTION_INVALID: Unknown action: purge",
            ),
            (
                json!({ "action": "" }),
                "MEMORY_ACTION_INVALID: Unknown action: ",
            ),
            (
                json!({ "action": " " }),
                "MEMORY_ACTION_INVALID: Unknown action:  ",
            ),
            (
                json!({ "action": "READ" }),
                "MEMORY_ACTION_INVALID: Unknown action: READ",
            ),
        ] {
            let output = tool(Arc::new(StubStore::new())).execute(input, ctx()).await;
            assert!(output.is_error);
            assert_eq!(output.content, expected);
        }
    }

    #[tokio::test]
    async fn missing_action_reports_framework_code() {
        for input in [json!({}), json!({ "action": 1 })] {
            let output = tool(Arc::new(StubStore::new())).execute(input, ctx()).await;
            assert!(output.is_error);
            assert!(output.content.starts_with("MISSING_PARAMETER: "));
        }
    }

    #[test]
    fn read_only_only_for_read_action() {
        let tool = tool(Arc::new(StubStore::new()));
        assert!(tool.is_read_only(&json!({ "action": "read" })));
        // 缺省即 read（旧 `getString("action", "read")`）。
        assert!(tool.is_read_only(&json!({})));
        assert!(!tool.is_read_only(&json!({ "action": "write" })));
        assert!(!tool.is_read_only(&json!({ "action": "delete" })));
        assert!(!tool.is_destructive(&json!({ "action": "delete" })));
    }
}
