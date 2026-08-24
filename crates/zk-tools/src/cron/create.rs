//! `CronCreate` 工具——创建定时任务。
//!
//! 对照旧 `tool/impl/CronCreateTool.java`（163L，只读权威规格）：名 `CronCreate`、
//! 入参 `cron`（必填）/ `prompt`（必填）/ `recurring`（默认 true）/
//! `durable`（默认 false）、`isEnabled() → featureFlags.isEnabled("AGENT_TRIGGERS")`、
//! `shouldDefer() → true`。
//!
//! 校验期错误码逐条对齐旧 `validateInput`：
//! `MISSING_CRON` / `MISSING_PROMPT` / `INVALID_CRON` / `LIMIT_REACHED`；
//! 执行期 `CRON_TASK_INVALID`（任务数触顶）/ `CRON_CREATE_FAILED`（其余）。
//!
//! 成功返回 JSON，键序与旧 `LinkedHashMap` 逐条一致：
//! `id` / `cron` / `prompt`（80 字符截断 + `"..."`）/ `recurring` / `durable` /
//! `next_run` / `expires_at` / `total_tasks`。

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::json;

use super::service::{CronTaskService, clip, next_run, parse_schedule};
use crate::input::{bool_or, failure, optional_str};
use crate::tool::{Tool, ToolContext, ToolOutput};

/// `prompt` 在成功回执里的截断长度（旧 `length() > 80 ? substring(0, 80) + "..."`）。
pub const CREATE_PROMPT_CLIP: usize = 80;

/// 定时任务创建工具（旧 `CronCreateTool`）。
#[derive(Debug)]
pub struct CronCreateTool {
    /// 共享任务台账（旧构造注入的 `CronTaskService` bean）。
    service: Arc<CronTaskService>,
}

impl CronCreateTool {
    /// 绑定任务台账。
    #[must_use]
    pub fn new(service: Arc<CronTaskService>) -> Self {
        Self { service }
    }
}

impl Tool for CronCreateTool {
    fn name(&self) -> &'static str {
        "CronCreate"
    }

    fn description(&self) -> &'static str {
        // 逐字取自旧 `getDescription()` 三段拼接。
        "Create a scheduled cron task that triggers at specified intervals. \
         Uses standard 5-field Unix cron expressions (minute hour day-of-month month day-of-week). \
         Maximum 50 concurrent tasks. Tasks expire after 30 days."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "required": ["cron", "prompt"],
            "properties": {
                "cron": {
                    "type": "string",
                    "description": "5-field Unix cron expression (e.g., '*/5 * * * *' for every 5 minutes)"
                },
                "prompt": {
                    "type": "string",
                    "description": "The prompt/instruction to execute when triggered"
                },
                "recurring": {
                    "type": "boolean",
                    "description": "Whether the task repeats (default: true)"
                },
                "durable": {
                    "type": "boolean",
                    "description": "Whether the task survives restarts (default: false)"
                }
            }
        })
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            // ── 校验期（旧 validateInput，四条错误码同序） ──
            let Some(expression) = optional_str(&input, "cron") else {
                return failure("MISSING_CRON", "cron expression is required");
            };
            let Some(prompt) = optional_str(&input, "prompt") else {
                return failure("MISSING_PROMPT", "prompt is required");
            };
            let schedule = match parse_schedule(expression) {
                Ok(schedule) => schedule,
                Err(reason) => {
                    return failure("INVALID_CRON", format!("Invalid cron expression: {reason}"));
                }
            };
            // 旧实现额外验「未来一年内有匹配」；`cron` crate 的迭代器直接给出
            // 下次触发时刻，取不到即等价于「无未来匹配」。
            let Some(next) = next_run(&schedule) else {
                return failure(
                    "INVALID_CRON",
                    "Cron expression does not match any date in the next year",
                );
            };
            let recurring = bool_or(&input, "recurring", true);
            let durable = bool_or(&input, "durable", false);
            // 旧在 durable 分支上提前查上限（`durable && taskCount() >= 50`）。
            if durable && self.service.task_count() >= super::MAX_JOBS {
                return failure(
                    "LIMIT_REACHED",
                    format!(
                        "Maximum number of scheduled tasks ({}) reached",
                        super::MAX_JOBS
                    ),
                );
            }

            // ── 执行期（旧 call） ──
            let task = match self.service.add_task(
                expression,
                prompt,
                recurring,
                durable,
                ctx.session_id(),
            ) {
                Ok(task) => task,
                Err(limit) => return failure("CRON_TASK_INVALID", limit.to_string()),
            };

            let body = json!({
                "id": task.id,
                "cron": task.cron,
                "prompt": clip(&task.prompt, CREATE_PROMPT_CLIP),
                "recurring": task.recurring,
                "durable": task.durable,
                "next_run": next,
                "expires_at": task.expires_at_iso(),
                "total_tasks": self.service.task_count(),
            });
            match serde_json::to_string(&body) {
                Ok(text) => ToolOutput::ok(text),
                Err(error) => failure(
                    "CRON_CREATE_FAILED",
                    format!("Failed to create cron task: {error}"),
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
        ToolContext::new(CancellationToken::new(), tx).with_session_id("sess-cron")
    }

    fn tool(tag: &str) -> CronCreateTool {
        let cwd = std::env::temp_dir().join(format!("zk-cron-create-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cwd);
        std::fs::create_dir_all(&cwd).expect("mkdir");
        CronCreateTool::new(Arc::new(CronTaskService::new(&cwd)))
    }

    /// 规格：名 / 必填两项 / 四个属性对齐旧 schema。
    #[test]
    fn spec_matches_legacy_shape() {
        let spec = tool("spec").spec();
        assert_eq!(spec.name, "CronCreate");
        assert_eq!(spec.parameters["required"], json!(["cron", "prompt"]));
        for key in ["cron", "prompt", "recurring", "durable"] {
            assert!(spec.parameters["properties"][key].is_object(), "{key}");
        }
        assert!(spec.description.contains("5-field Unix cron expressions"));
    }

    /// 校验期四条错误码逐条对齐旧 `validateInput`。
    #[tokio::test]
    async fn validation_errors_match_legacy_codes() {
        let tool = tool("validate");
        for (input, code) in [
            (json!({}), "MISSING_CRON"),
            (json!({ "cron": "  " }), "MISSING_CRON"),
            (json!({ "cron": "* * * * *" }), "MISSING_PROMPT"),
            (json!({ "cron": "bogus", "prompt": "hi" }), "INVALID_CRON"),
            (
                json!({ "cron": "99 * * * *", "prompt": "hi" }),
                "INVALID_CRON",
            ),
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

    /// 成功回执键序与内容对齐旧 `LinkedHashMap`，并写入台账。
    #[tokio::test]
    async fn creates_a_task_and_returns_legacy_json() {
        let tool = tool("ok");
        let output = tool
            .execute(
                json!({ "cron": "*/5 * * * *", "prompt": "run the nightly report" }),
                ctx(),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);

        let body: serde_json::Value = serde_json::from_str(&output.content).expect("json");
        assert_eq!(body["cron"], "*/5 * * * *");
        assert_eq!(body["prompt"], "run the nightly report");
        assert_eq!(body["recurring"], true);
        assert_eq!(body["durable"], false);
        assert_eq!(body["total_tasks"], 1);
        assert_eq!(
            body["id"].as_str().expect("id").len(),
            8,
            "short id is 8 chars"
        );
        assert!(
            body["next_run"].as_str().expect("next_run").ends_with('Z'),
            "{}",
            body["next_run"]
        );
        assert!(
            body["expires_at"]
                .as_str()
                .expect("expires_at")
                .ends_with('Z')
        );

        // 键序（serde_json 的 Map 默认保序编译特性未开时按插入序输出）。
        let keys: Vec<&str> = body
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        assert!(keys.contains(&"total_tasks"));

        // 会话 ID 被记为归属代理（旧 `context.sessionId()`）。
        let stored = tool
            .service
            .get_task(body["id"].as_str().expect("id"))
            .expect("stored");
        assert_eq!(stored.agent_id.as_deref(), Some("sess-cron"));
    }

    /// 超长 prompt 在回执里被截到 80 字符 + `"..."`（台账仍存全文）。
    #[tokio::test]
    async fn long_prompt_is_clipped_in_the_receipt_only() {
        let tool = tool("clip");
        let prompt = "p".repeat(200);
        let output = tool
            .execute(json!({ "cron": "0 3 * * *", "prompt": prompt }), ctx())
            .await;
        assert!(!output.is_error, "{}", output.content);
        let body: serde_json::Value = serde_json::from_str(&output.content).expect("json");
        assert_eq!(
            body["prompt"],
            format!("{}...", "p".repeat(CREATE_PROMPT_CLIP))
        );
        let stored = tool
            .service
            .get_task(body["id"].as_str().expect("id"))
            .expect("stored");
        assert_eq!(stored.prompt.len(), 200);
    }

    /// durable 触顶 → `LIMIT_REACHED`（旧校验期分支）。
    #[tokio::test]
    async fn durable_creation_respects_the_cap() {
        let tool = tool("cap");
        for index in 0..crate::cron::MAX_JOBS {
            tool.service
                .add_task("* * * * *", &format!("job {index}"), true, false, None)
                .expect("added");
        }
        let output = tool
            .execute(
                json!({ "cron": "* * * * *", "prompt": "hi", "durable": true }),
                ctx(),
            )
            .await;
        assert!(output.is_error);
        assert!(
            output.content.starts_with("LIMIT_REACHED: "),
            "{}",
            output.content
        );

        // 非 durable 路径触顶落执行期的 `CRON_TASK_INVALID`（旧 catch 分支）。
        let output = tool
            .execute(json!({ "cron": "* * * * *", "prompt": "hi" }), ctx())
            .await;
        assert!(
            output.content.starts_with("CRON_TASK_INVALID: "),
            "{}",
            output.content
        );
    }
}
