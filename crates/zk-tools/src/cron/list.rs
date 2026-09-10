//! `CronList` tool backed by the server-owned `SQLite` port.
#![allow(missing_docs)]

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::json;

use super::service::{CronTaskPort, clip};
use crate::input::failure;
use crate::tool::{Tool, ToolContext, ToolOutput};

pub const LIST_PROMPT_CLIP: usize = 60;
pub const NO_TASKS: &str = "No scheduled tasks.";

pub struct CronListTool {
    service: Arc<dyn CronTaskPort>,
}

impl std::fmt::Debug for CronListTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CronListTool")
            .finish_non_exhaustive()
    }
}

impl CronListTool {
    #[must_use]
    pub fn new(service: Arc<dyn CronTaskPort>) -> Self {
        Self { service }
    }
}

impl Tool for CronListTool {
    fn name(&self) -> &'static str {
        "CronList"
    }

    fn description(&self) -> &'static str {
        "List persistent scheduled tasks owned by the current root session."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({ "type": "object", "properties": {} })
    }

    fn is_read_only(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, _input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let Some(owner_session_id) = ctx.session_id().map(str::to_owned) else {
                return failure(
                    "CRON_CONTEXT_REQUIRED",
                    "CronList requires an authorized root session",
                );
            };
            let tasks = match self.service.list(owner_session_id).await {
                Ok(tasks) => tasks,
                Err(error) => return failure(&error.code, error.message),
            };
            if tasks.is_empty() {
                return ToolOutput::ok(NO_TASKS);
            }
            let tasks = tasks
                .into_iter()
                .map(|task| {
                    json!({
                        "jobId": task.job_id,
                        "cron": task.cron,
                        "timezone": task.timezone,
                        "prompt": clip(&task.prompt, LIST_PROMPT_CLIP),
                        "recurring": task.recurring,
                        "overlapPolicy": task.overlap_policy,
                        "missedPolicy": task.missed_policy,
                        "status": task.status,
                        "nextScheduledAt": task.next_scheduled_at,
                        "createdAt": task.created_at,
                        "updatedAt": task.updated_at,
                    })
                })
                .collect::<Vec<_>>();
            serde_json::to_string(&json!({ "total": tasks.len(), "tasks": tasks })).map_or_else(
                |error| {
                    failure(
                        "CRON_LIST_FAILED",
                        format!("Failed to encode cron jobs: {error}"),
                    )
                },
                ToolOutput::ok,
            )
        })
    }
}
