//! `CronDelete` 工具——按 ID 删除定时任务。
//!
//! 对照旧 `tool/impl/CronDeleteTool.java`（88L，只读权威规格）：名 `CronDelete`、
//! 入参 `id`（必填）、`isEnabled() → featureFlags.isEnabled("AGENT_TRIGGERS")`、
//! `shouldDefer() → true`；校验期 `MISSING_ID` / `NOT_FOUND`，
//! 执行期 `CRON_TASK_NOT_FOUND`（校验与执行之间被并发删掉的窄窗）。
//!
//! 成功文案**逐字**对齐旧 L81-83：
//! `"Deleted scheduled task: id=<id>, cron='<cron>', remaining=<n>"`。

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::json;

use super::service::CronTaskService;
use crate::input::{failure, optional_str};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// 定时任务删除工具（旧 `CronDeleteTool`）。
#[derive(Debug)]
pub struct CronDeleteTool {
    /// 共享任务台账。
    service: Arc<CronTaskService>,
}

impl CronDeleteTool {
    /// 绑定任务台账。
    #[must_use]
    pub fn new(service: Arc<CronTaskService>) -> Self {
        Self { service }
    }
}

impl Tool for CronDeleteTool {
    fn name(&self) -> &'static str {
        "CronDelete"
    }

    fn description(&self) -> &'static str {
        // 逐字取自旧 `getDescription()`。
        "Delete a scheduled cron task by its ID."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "The ID of the cron task to delete"
                }
            }
        })
    }

    /// 删任务改台账（旧无显式 `isDestructive`，默认 false；但本框架据此
    /// 决定是否走权限询问，故如实标记为破坏性）。
    fn is_destructive(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, input: serde_json::Value, _ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            // ── 校验期（旧 validateInput 两条错误码同序） ──
            let Some(id) = optional_str(&input, "id") else {
                return failure("MISSING_ID", "Task id is required");
            };
            if self.service.get_task(id).is_none() {
                return failure(
                    "NOT_FOUND",
                    format!("No scheduled task found with id: {id}"),
                );
            }

            // ── 执行期（旧 call） ──
            match self.service.remove(id) {
                Some(task) => ToolOutput::ok(format!(
                    "Deleted scheduled task: id={}, cron='{}', remaining={}",
                    task.id,
                    task.cron,
                    self.service.task_count()
                )),
                None => failure(
                    "CRON_TASK_NOT_FOUND",
                    format!("No scheduled task found with id: {id}"),
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

    fn tool(tag: &str) -> CronDeleteTool {
        let cwd = std::env::temp_dir().join(format!("zk-cron-del-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cwd);
        std::fs::create_dir_all(&cwd).expect("mkdir");
        CronDeleteTool::new(Arc::new(CronTaskService::new(&cwd)))
    }

    /// 规格：名 / 必填 `id` / 破坏性标记。
    #[test]
    fn spec_matches_legacy_shape() {
        let tool = tool("spec");
        let spec = tool.spec();
        assert_eq!(spec.name, "CronDelete");
        assert_eq!(spec.parameters["required"], json!(["id"]));
        assert!(tool.is_destructive(&json!({})));
        assert!(!tool.is_read_only(&json!({})));
    }

    /// 缺 `id` → `MISSING_ID`；未知 `id` → `NOT_FOUND`（旧同名错误码）。
    #[tokio::test]
    async fn validation_errors_match_legacy_codes() {
        let tool = tool("validate");
        for (input, code) in [
            (json!({}), "MISSING_ID"),
            (json!({ "id": "   " }), "MISSING_ID"),
            (json!({ "id": "deadbeef" }), "NOT_FOUND"),
        ] {
            let output = tool.execute(input.clone(), ctx()).await;
            assert!(output.is_error, "{input} unexpectedly succeeded");
            assert!(
                output.content.starts_with(&format!("{code}: ")),
                "{} did not start with {code}",
                output.content
            );
        }
    }

    /// 成功文案逐字对齐旧实现，`remaining` 反映删后计数。
    #[tokio::test]
    async fn deletes_and_reports_legacy_sentence() {
        let tool = tool("ok");
        let first = tool
            .service
            .add_task("*/5 * * * *", "one", true, false, None)
            .expect("added");
        tool.service
            .add_task("0 3 * * *", "two", true, false, None)
            .expect("added");

        let output = tool.execute(json!({ "id": first.id }), ctx()).await;
        assert!(!output.is_error, "{}", output.content);
        assert_eq!(
            output.content,
            format!(
                "Deleted scheduled task: id={}, cron='*/5 * * * *', remaining=1",
                first.id
            )
        );
        assert_eq!(tool.service.task_count(), 1);
        assert!(tool.service.get_task(&first.id).is_none());
    }
}
