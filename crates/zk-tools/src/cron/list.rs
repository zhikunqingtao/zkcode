//! `CronList` 工具——列出全部定时任务。
//!
//! 对照旧 `tool/impl/CronListTool.java`（95L，只读权威规格）：名 `CronList`、
//! 空入参 schema、`isEnabled() → featureFlags.isEnabled("AGENT_TRIGGERS")`、
//! `isReadOnly = true`、`isConcurrencySafe = true`；空表回**逐字**
//! `"No scheduled tasks."`，否则回 JSON `{total, tasks:[…]}`，每项键序
//! `id` / `cron` / `prompt`（60 字符截断）/ `recurring` / `durable` /
//! `created_at` / `expires_at`；异常回 `CRON_LIST_FAILED`。

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::json;

use super::service::{CronTaskService, clip};
use crate::input::failure;
use crate::tool::{Tool, ToolContext, ToolOutput};

/// `prompt` 在列表里的截断长度（旧 `length() > 60 ? substring(0, 60) + "..."`）。
pub const LIST_PROMPT_CLIP: usize = 60;

/// 空表回执（**逐字**取自旧 `CronListTool.java` L67）。
pub const NO_TASKS: &str = "No scheduled tasks.";

/// 定时任务列表工具（旧 `CronListTool`）。
#[derive(Debug)]
pub struct CronListTool {
    /// 共享任务台账。
    service: Arc<CronTaskService>,
}

impl CronListTool {
    /// 绑定任务台账。
    #[must_use]
    pub fn new(service: Arc<CronTaskService>) -> Self {
        Self { service }
    }
}

impl Tool for CronListTool {
    fn name(&self) -> &'static str {
        "CronList"
    }

    fn description(&self) -> &'static str {
        // 逐字取自旧 `getDescription()`。
        "List all scheduled cron tasks with their IDs, schedules, and status."
    }

    fn parameters(&self) -> serde_json::Value {
        // 旧 `Map.of("type", "object", "properties", Map.of())`。
        json!({ "type": "object", "properties": {} })
    }

    /// 旧 `isReadOnly(input) → true`。
    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, _input: serde_json::Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let tasks = self.service.list_all();
            if tasks.is_empty() {
                return ToolOutput::ok(NO_TASKS);
            }
            let items: Vec<serde_json::Value> = tasks
                .iter()
                .map(|task| {
                    json!({
                        "id": task.id,
                        "cron": task.cron,
                        "prompt": clip(&task.prompt, LIST_PROMPT_CLIP),
                        "recurring": task.recurring,
                        "durable": task.durable,
                        "created_at": task.created_at_iso(),
                        "expires_at": task.expires_at_iso(),
                    })
                })
                .collect();
            let body = json!({ "total": tasks.len(), "tasks": items });
            match serde_json::to_string(&body) {
                Ok(text) => ToolOutput::ok(text),
                Err(error) => failure(
                    "CRON_LIST_FAILED",
                    format!("Failed to list cron tasks: {error}"),
                ),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx)
    }

    fn tool(tag: &str) -> CronListTool {
        let cwd = std::env::temp_dir().join(format!("zk-cron-list-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cwd);
        std::fs::create_dir_all(&cwd).expect("mkdir");
        CronListTool::new(Arc::new(CronTaskService::new(&cwd)))
    }

    /// 规格：名 / 空 properties / 只读标记对齐旧实现。
    #[test]
    fn spec_matches_legacy_shape() {
        let tool = tool("spec");
        let spec = tool.spec();
        assert_eq!(spec.name, "CronList");
        assert_eq!(
            spec.parameters,
            json!({ "type": "object", "properties": {} })
        );
        assert!(tool.is_read_only(&json!({})));
        assert!(!tool.is_destructive(&json!({})));
    }

    /// 空表回逐字旧文案，且**不是**错误。
    #[tokio::test]
    async fn empty_table_returns_legacy_sentence() {
        let output = tool("empty").execute(json!({}), ctx()).await;
        assert!(!output.is_error);
        assert_eq!(output.content, "No scheduled tasks.");
    }

    /// 非空表回 `{total, tasks:[…]}`，每项键齐全，prompt 截到 60 字符。
    #[tokio::test]
    async fn lists_tasks_with_legacy_item_shape() {
        let tool = tool("items");
        tool.service
            .add_task("*/5 * * * *", &"q".repeat(100), true, false, Some("s1"))
            .expect("added");
        tool.service
            .add_task("0 3 * * *", "nightly", false, true, None)
            .expect("added");

        let output = tool.execute(json!({}), ctx()).await;
        assert!(!output.is_error, "{}", output.content);
        let body: serde_json::Value = serde_json::from_str(&output.content).expect("json");
        assert_eq!(body["total"], 2);

        let items = body["tasks"].as_array().expect("array");
        assert_eq!(items.len(), 2);
        for item in items {
            for key in [
                "id",
                "cron",
                "prompt",
                "recurring",
                "durable",
                "created_at",
                "expires_at",
            ] {
                assert!(!item[key].is_null(), "{key} missing in {item}");
            }
        }
        // 两条任务可能落在同一毫秒（定序退化为 id 比较），故按 cron 定位而非下标。
        let clipped = items
            .iter()
            .find(|item| item["cron"] == "*/5 * * * *")
            .expect("clipped item");
        assert_eq!(
            clipped["prompt"],
            format!("{}...", "q".repeat(LIST_PROMPT_CLIP))
        );
        assert_eq!(clipped["durable"], false);

        let nightly = items
            .iter()
            .find(|item| item["cron"] == "0 3 * * *")
            .expect("nightly item");
        assert_eq!(nightly["prompt"], "nightly");
        assert_eq!(nightly["durable"], true);
        assert_eq!(nightly["recurring"], false);
    }
}
