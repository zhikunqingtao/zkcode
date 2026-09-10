//! `CronCreate` tool backed exclusively by the server-provided durable port.
#![allow(missing_docs)]

use std::sync::Arc;

use chrono::Utc;
use futures::future::BoxFuture;
use serde_json::json;

use super::service::{
    CronCreateRequest, CronTaskPort, DEFAULT_MISSED_POLICY, DEFAULT_OVERLAP_POLICY,
    DEFAULT_TIMEZONE, clip, next_run_after_ms, parse_timezone,
};
use crate::input::{bool_or, failure, optional_str};
use crate::tool::{Tool, ToolContext, ToolOutput};

pub const CREATE_PROMPT_CLIP: usize = 80;

pub struct CronCreateTool {
    service: Arc<dyn CronTaskPort>,
}

impl std::fmt::Debug for CronCreateTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CronCreateTool")
            .finish_non_exhaustive()
    }
}

impl CronCreateTool {
    #[must_use]
    pub fn new(service: Arc<dyn CronTaskPort>) -> Self {
        Self { service }
    }
}

impl Tool for CronCreateTool {
    fn name(&self) -> &'static str {
        "CronCreate"
    }

    fn description(&self) -> &'static str {
        "Create a persistent scheduled task with an explicit IANA timezone. Missed and overlapping occurrences are recorded and skipped by default."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "required": ["cron", "prompt"],
            "properties": {
                "cron": {
                    "type": "string",
                    "description": "5-field Unix cron expression (for example, '*/5 * * * *')"
                },
                "prompt": {
                    "type": "string",
                    "description": "Instruction executed for each claimed occurrence"
                },
                "timezone": {
                    "type": "string",
                    "description": "IANA timezone name (default: UTC)"
                },
                "recurring": {
                    "type": "boolean",
                    "description": "Whether the job remains active after its first occurrence (default: true)"
                },
                "overlapPolicy": {
                    "type": "string",
                    "enum": ["skip"],
                    "description": "Behavior while an earlier occurrence is active (v1: skip)"
                },
                "missedPolicy": {
                    "type": "string",
                    "enum": ["skip"],
                    "description": "Behavior for occurrences missed during downtime (v1: skip)"
                }
            }
        })
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let Some(expression) = optional_str(&input, "cron") else {
                return failure("MISSING_CRON", "cron expression is required");
            };
            let Some(prompt) = optional_str(&input, "prompt") else {
                return failure("MISSING_PROMPT", "prompt is required");
            };
            let timezone = optional_str(&input, "timezone").unwrap_or(DEFAULT_TIMEZONE);
            let canonical_timezone = match parse_timezone(timezone) {
                Ok(timezone) => timezone.to_string(),
                Err(reason) => return failure("INVALID_TIMEZONE", reason),
            };
            if let Err(reason) = next_run_after_ms(
                expression,
                &canonical_timezone,
                Utc::now().timestamp_millis(),
            ) {
                return failure("INVALID_CRON", format!("Invalid cron expression: {reason}"));
            }
            let overlap_policy =
                optional_str(&input, "overlapPolicy").unwrap_or(DEFAULT_OVERLAP_POLICY);
            if overlap_policy != DEFAULT_OVERLAP_POLICY {
                return failure(
                    "UNSUPPORTED_OVERLAP_POLICY",
                    "v1 supports only overlapPolicy='skip'",
                );
            }
            let missed_policy =
                optional_str(&input, "missedPolicy").unwrap_or(DEFAULT_MISSED_POLICY);
            if missed_policy != DEFAULT_MISSED_POLICY {
                return failure(
                    "UNSUPPORTED_MISSED_POLICY",
                    "v1 supports only missedPolicy='skip'",
                );
            }
            let Some(owner_session_id) = ctx.session_id().map(str::to_owned) else {
                return failure(
                    "CRON_CONTEXT_REQUIRED",
                    "CronCreate requires an authorized root session",
                );
            };
            let task = match self
                .service
                .create(CronCreateRequest {
                    owner_session_id,
                    cron_expression: expression.to_owned(),
                    timezone: canonical_timezone,
                    prompt: prompt.to_owned(),
                    recurring: bool_or(&input, "recurring", true),
                    overlap_policy: overlap_policy.to_owned(),
                    missed_policy: missed_policy.to_owned(),
                })
                .await
            {
                Ok(task) => task,
                Err(error) => return failure(&error.code, error.message),
            };

            let body = json!({
                "jobId": task.job_id,
                "cron": task.cron,
                "timezone": task.timezone,
                "prompt": clip(&task.prompt, CREATE_PROMPT_CLIP),
                "recurring": task.recurring,
                "overlapPolicy": task.overlap_policy,
                "missedPolicy": task.missed_policy,
                "status": task.status,
                "nextScheduledAt": task.next_scheduled_at,
                "createdAt": task.created_at,
            });
            serde_json::to_string(&body).map_or_else(
                |error| {
                    failure(
                        "CRON_CREATE_FAILED",
                        format!("Failed to encode cron job: {error}"),
                    )
                },
                ToolOutput::ok,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::cron::{CronDeleteReceipt, CronPortError, CronTask};

    #[derive(Default)]
    struct FakePort {
        created: Mutex<Vec<CronCreateRequest>>,
    }

    impl CronTaskPort for FakePort {
        fn create(
            &self,
            request: CronCreateRequest,
        ) -> BoxFuture<'_, Result<CronTask, CronPortError>> {
            self.created
                .lock()
                .expect("created lock")
                .push(request.clone());
            Box::pin(async move {
                Ok(CronTask {
                    job_id: uuid::Uuid::new_v4().to_string(),
                    cron: request.cron_expression,
                    timezone: request.timezone,
                    prompt: request.prompt,
                    recurring: request.recurring,
                    overlap_policy: request.overlap_policy,
                    missed_policy: request.missed_policy,
                    status: "active".to_owned(),
                    next_scheduled_at: Some("2026-09-09T01:00:00.000Z".to_owned()),
                    created_at: "2026-09-09T00:00:00.000Z".to_owned(),
                    updated_at: "2026-09-09T00:00:00.000Z".to_owned(),
                })
            })
        }

        fn list(
            &self,
            _owner_session_id: String,
        ) -> BoxFuture<'_, Result<Vec<CronTask>, CronPortError>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn delete(
            &self,
            _owner_session_id: String,
            _job_id: String,
        ) -> BoxFuture<'_, Result<Option<CronDeleteReceipt>, CronPortError>> {
            Box::pin(async { Ok(None) })
        }
    }

    fn ctx() -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx).with_session_id("sess-cron")
    }

    #[test]
    fn schema_is_lower_camel_and_exposes_policies() {
        let tool = CronCreateTool::new(Arc::new(FakePort::default()));
        let spec = tool.spec();
        assert_eq!(spec.parameters["required"], json!(["cron", "prompt"]));
        for key in [
            "cron",
            "prompt",
            "timezone",
            "recurring",
            "overlapPolicy",
            "missedPolicy",
        ] {
            assert!(spec.parameters["properties"][key].is_object(), "{key}");
        }
        assert!(spec.parameters["properties"].get("durable").is_none());
    }

    #[tokio::test]
    async fn defaults_to_utc_and_skip_policies() {
        let port = Arc::new(FakePort::default());
        let tool = CronCreateTool::new(port.clone());
        let output = tool
            .execute(json!({"cron": "*/5 * * * *", "prompt": "status"}), ctx())
            .await;
        assert!(!output.is_error, "{}", output.content);
        let body: serde_json::Value = serde_json::from_str(&output.content).expect("json");
        assert!(body["jobId"].as_str().is_some());
        assert_eq!(body["timezone"], "UTC");
        assert_eq!(body["overlapPolicy"], "skip");
        assert_eq!(body["missedPolicy"], "skip");
        assert!(body.get("nextScheduledAt").is_some());
        let request = port.created.lock().expect("created lock")[0].clone();
        assert_eq!(request.owner_session_id, "sess-cron");
    }

    #[tokio::test]
    async fn rejects_non_iana_zone_and_unimplemented_policies() {
        let tool = CronCreateTool::new(Arc::new(FakePort::default()));
        for (input, code) in [
            (
                json!({"cron": "* * * * *", "prompt": "x", "timezone": "Mars/Olympus"}),
                "INVALID_TIMEZONE",
            ),
            (
                json!({"cron": "* * * * *", "prompt": "x", "overlapPolicy": "parallel"}),
                "UNSUPPORTED_OVERLAP_POLICY",
            ),
            (
                json!({"cron": "* * * * *", "prompt": "x", "missedPolicy": "catchUp"}),
                "UNSUPPORTED_MISSED_POLICY",
            ),
        ] {
            let output = tool.execute(input, ctx()).await;
            assert!(output.is_error);
            assert!(output.content.starts_with(code), "{}", output.content);
        }
    }
}
