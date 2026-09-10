//! `CronDelete` tool backed by the server-owned `SQLite` port.
#![allow(missing_docs)]

use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::json;

use super::service::CronTaskPort;
use crate::input::{failure, optional_str};
use crate::tool::{Tool, ToolContext, ToolOutput};

pub struct CronDeleteTool {
    service: Arc<dyn CronTaskPort>,
}

impl std::fmt::Debug for CronDeleteTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CronDeleteTool")
            .finish_non_exhaustive()
    }
}

impl CronDeleteTool {
    #[must_use]
    pub fn new(service: Arc<dyn CronTaskPort>) -> Self {
        Self { service }
    }
}

impl Tool for CronDeleteTool {
    fn name(&self) -> &'static str {
        "CronDelete"
    }

    fn description(&self) -> &'static str {
        "Disable a persistent scheduled task owned by the current root session."
    }

    fn parameters(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "required": ["jobId"],
            "properties": {
                "jobId": {
                    "type": "string",
                    "description": "Full UUID v4 of the scheduled job"
                }
            }
        })
    }

    fn is_destructive(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn execute(&self, input: serde_json::Value, ctx: ToolContext) -> BoxFuture<'_, ToolOutput> {
        Box::pin(async move {
            let Some(job_id) = optional_str(&input, "jobId") else {
                return failure("MISSING_JOB_ID", "jobId is required");
            };
            let Ok(parsed) = uuid::Uuid::parse_str(job_id) else {
                return failure("INVALID_JOB_ID", "jobId must be a full UUID v4");
            };
            if parsed.get_version() != Some(uuid::Version::Random)
                || parsed.hyphenated().to_string() != job_id
            {
                return failure("INVALID_JOB_ID", "jobId must be a full UUID v4");
            }
            let Some(owner_session_id) = ctx.session_id().map(str::to_owned) else {
                return failure(
                    "CRON_CONTEXT_REQUIRED",
                    "CronDelete requires an authorized root session",
                );
            };
            match self
                .service
                .delete(owner_session_id, job_id.to_owned())
                .await
            {
                Ok(Some(receipt)) => ToolOutput::ok(
                    serde_json::json!({
                        "jobId": receipt.task.job_id,
                        "status": "deleted",
                        "remaining": receipt.remaining,
                    })
                    .to_string(),
                ),
                Ok(None) => failure(
                    "CRON_JOB_NOT_FOUND",
                    format!("No scheduled job found with jobId: {job_id}"),
                ),
                Err(error) => failure(&error.code, error.message),
            }
        })
    }
}
