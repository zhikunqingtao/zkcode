//! Read-only, redacted `TaskRuntime` diagnostics.
//!
//! The projection is deliberately defined separately from the operational records:
//! sensitive payload columns are not selected at all, rather than being loaded and
//! redacted after the fact.  One reader transaction supplies the complete snapshot.
#![allow(missing_docs, clippy::missing_errors_doc, clippy::too_many_lines)]

use std::collections::HashMap;

use rusqlite::{OptionalExtension, Transaction, params};
use serde::Serialize;

use crate::time::now_millis;
use crate::{Db, DbError};

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDiagnostic {
    pub task: TaskDiagnosticTask,
    pub attempts: Vec<TaskDiagnosticRun>,
    pub tool_invocations: Vec<TaskDiagnosticToolInvocation>,
    pub execution_resources: Vec<TaskDiagnosticExecutionResource>,
    pub results: Vec<TaskDiagnosticResult>,
    pub receipts: Vec<TaskDiagnosticReceipt>,
    pub llm_calls: Vec<TaskDiagnosticLlmCall>,
    pub checkpoints: Vec<TaskDiagnosticCheckpoint>,
    pub resume_eligibility: Vec<TaskResumeEligibility>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDiagnosticTask {
    pub task_id: String,
    pub session_id: String,
    pub root_session_id: String,
    pub parent_task_id: Option<String>,
    pub root_task_id: String,
    pub current_run_id: Option<String>,
    pub creator_run_id: Option<String>,
    pub creator_tool_use_id: Option<String>,
    pub ordinal: i64,
    pub task_type: String,
    pub status: String,
    pub lifecycle_policy: String,
    pub cleanup_status: String,
    pub verification_status: String,
    pub token_budget_limit: Option<i64>,
    pub cost_budget_nanos_usd: Option<i64>,
    pub deadline_at_ms: Option<i64>,
    pub budget_reserved_tokens: i64,
    pub budget_reserved_cost_nanos_usd: i64,
    pub budget_consumed_tokens: i64,
    pub budget_consumed_cost_nanos_usd: i64,
    pub usage_complete: bool,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
    pub terminal_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDiagnosticRun {
    pub run_id: String,
    pub session_id: String,
    pub task_id: String,
    pub attempt: i64,
    pub startup_epoch: i64,
    pub checkpoint_id: Option<String>,
    pub parent_run_id: Option<String>,
    pub status: String,
    pub version: i64,
    pub agent_type: Option<String>,
    pub model: String,
    pub prompt_hash: Option<String>,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub terminal_at: Option<String>,
    pub exit_reason: Option<String>,
    pub requested_exit_reason: Option<String>,
    pub verification_status: String,
    pub cleanup_status: String,
    pub abort_reason: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_create_tokens: i64,
    pub cost_nanos_usd: i64,
    pub usage_complete: bool,
    pub tool_call_count: i64,
    pub turn_count: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDiagnosticToolInvocation {
    pub invocation_id: String,
    pub task_id: String,
    pub run_id: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub status: String,
    pub error_code: Option<String>,
    pub side_effect_class: String,
    pub cleanup_status: String,
    pub directory_generation: Option<i64>,
    pub connection_generation: Option<i64>,
    pub version: i64,
    pub started_at: Option<String>,
    pub terminal_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDiagnosticExecutionResource {
    pub resource_id: String,
    pub task_id: String,
    pub run_id: String,
    pub invocation_id: Option<String>,
    pub resource_kind: String,
    pub status: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
    pub released_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDiagnosticResult {
    pub result_id: String,
    pub task_id: String,
    pub run_id: String,
    pub result_version: i64,
    pub status: String,
    pub blob_sha256: Option<String>,
    pub byte_len: i64,
    pub content_sha256: String,
    pub media_type: String,
    pub error_code: Option<String>,
    pub final_message_id: Option<String>,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDiagnosticReceipt {
    pub receipt_id: String,
    pub consumer_task_id: String,
    pub producer_task_id: String,
    pub result_version: i64,
    pub message_id: String,
    pub result_sha256: String,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDiagnosticLlmCall {
    pub call_id: String,
    pub task_id: String,
    pub run_id: String,
    pub provider: String,
    pub model: String,
    pub route: Option<String>,
    pub status: String,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_create_tokens: Option<i64>,
    pub cost_nanos_usd: Option<i64>,
    pub reserved_input_tokens: i64,
    pub reserved_output_tokens: i64,
    pub reserved_cost_nanos_usd: i64,
    pub usage_complete: bool,
    pub error_code: Option<String>,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDiagnosticCheckpoint {
    pub checkpoint_id: String,
    pub run_id: String,
    pub session_id: String,
    pub agent_id: String,
    pub sequence: i64,
    pub tool_call_count: i64,
    pub turn_count: i64,
    pub tokens_consumed: i64,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskResumeEligibility {
    pub run_id: String,
    pub attempt: i64,
    /// Durable-storage eligibility only. The runtime must still revalidate the
    /// workspace, permissions, tool directory, provider and current budget.
    pub eligible: bool,
    pub runtime_revalidation_required: bool,
    pub reasons: Vec<TaskResumeIneligibilityReason>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskResumeIneligibilityReason {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<i64>,
}

impl TaskResumeIneligibilityReason {
    fn one(code: &str) -> Self {
        Self {
            code: code.to_owned(),
            count: None,
        }
    }

    fn counted(code: &str, count: usize) -> Self {
        Self {
            code: code.to_owned(),
            count: i64::try_from(count).ok(),
        }
    }
}

impl Db {
    /// Return one coherent, payload-free diagnostic snapshot for a logical Task.
    pub async fn find_task_diagnostic(
        &self,
        task_id: &str,
    ) -> Result<Option<TaskDiagnostic>, DbError> {
        let task_id = task_id.to_owned();
        self.with_reader(move |connection| {
            let transaction = connection.transaction()?;
            let Some(task) = load_task(&transaction, &task_id)? else {
                transaction.commit()?;
                return Ok(None);
            };
            let attempts = load_runs(&transaction, &task_id)?;
            let tool_invocations = load_invocations(&transaction, &task_id)?;
            let execution_resources = load_resources(&transaction, &task_id)?;
            let results = load_results(&transaction, &task_id)?;
            let receipts = load_receipts(&transaction, &task_id)?;
            let llm_calls = load_llm_calls(&transaction, &task_id)?;
            let checkpoints = load_checkpoints(&transaction, &task_id)?;
            let resume_eligibility = assess_resume_eligibility(
                &task,
                &attempts,
                &tool_invocations,
                &execution_resources,
                &results,
                &llm_calls,
                &checkpoints,
            );
            transaction.commit()?;
            Ok(Some(TaskDiagnostic {
                task,
                attempts,
                tool_invocations,
                execution_resources,
                results,
                receipts,
                llm_calls,
                checkpoints,
                resume_eligibility,
            }))
        })
        .await
    }
}

fn load_task(
    transaction: &Transaction<'_>,
    task_id: &str,
) -> Result<Option<TaskDiagnosticTask>, DbError> {
    transaction
        .query_row(
            "SELECT t.id,t.session_id,root.session_id,t.parent_task_id,t.root_task_id,
                    t.current_run_id,t.creator_run_id,t.creator_tool_use_id,t.ordinal,
                    t.task_type,t.status,t.lifecycle_policy,t.cleanup_status,
                    t.verification_status,t.token_budget_limit,t.cost_budget_nanos_usd,
                    t.deadline_at_ms,t.budget_reserved_tokens,
                    t.budget_reserved_cost_nanos_usd,t.budget_consumed_tokens,
                    t.budget_consumed_cost_nanos_usd,t.usage_complete,t.version,
                    t.created_at,t.updated_at,t.terminal_at
               FROM tasks t JOIN tasks root ON root.id=t.root_task_id
              WHERE t.id=?1",
            params![task_id],
            |row| {
                Ok(TaskDiagnosticTask {
                    task_id: row.get(0)?,
                    session_id: row.get(1)?,
                    root_session_id: row.get(2)?,
                    parent_task_id: row.get(3)?,
                    root_task_id: row.get(4)?,
                    current_run_id: row.get(5)?,
                    creator_run_id: row.get(6)?,
                    creator_tool_use_id: row.get(7)?,
                    ordinal: row.get(8)?,
                    task_type: row.get(9)?,
                    status: row.get(10)?,
                    lifecycle_policy: row.get(11)?,
                    cleanup_status: row.get(12)?,
                    verification_status: row.get(13)?,
                    token_budget_limit: row.get(14)?,
                    cost_budget_nanos_usd: row.get(15)?,
                    deadline_at_ms: row.get(16)?,
                    budget_reserved_tokens: row.get(17)?,
                    budget_reserved_cost_nanos_usd: row.get(18)?,
                    budget_consumed_tokens: row.get(19)?,
                    budget_consumed_cost_nanos_usd: row.get(20)?,
                    usage_complete: row.get::<_, i64>(21)? != 0,
                    version: row.get(22)?,
                    created_at: row.get(23)?,
                    updated_at: row.get(24)?,
                    terminal_at: row.get(25)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn load_runs(
    transaction: &Transaction<'_>,
    task_id: &str,
) -> Result<Vec<TaskDiagnosticRun>, DbError> {
    let mut statement = transaction.prepare(
        "SELECT id,session_id,task_id,attempt,startup_epoch,checkpoint_id,parent_run_id,
                status,version,agent_type,model,prompt_hash,started_at,finished_at,terminal_at,
                exit_reason,requested_exit_reason,verification_status,cleanup_status,abort_reason,
                input_tokens,output_tokens,cache_read_tokens,cache_create_tokens,cost_nanos_usd,
                usage_complete,tool_call_count,turn_count,created_at,updated_at
           FROM run_envelopes WHERE task_id=?1 ORDER BY attempt,id",
    )?;
    let rows = statement.query_map(params![task_id], |row| {
        Ok(TaskDiagnosticRun {
            run_id: row.get(0)?,
            session_id: row.get(1)?,
            task_id: row.get(2)?,
            attempt: row.get(3)?,
            startup_epoch: row.get(4)?,
            checkpoint_id: row.get(5)?,
            parent_run_id: row.get(6)?,
            status: row.get(7)?,
            version: row.get(8)?,
            agent_type: row.get(9)?,
            model: row.get(10)?,
            prompt_hash: row.get(11)?,
            started_at: row.get(12)?,
            finished_at: row.get(13)?,
            terminal_at: row.get(14)?,
            exit_reason: row.get(15)?,
            requested_exit_reason: row.get(16)?,
            verification_status: row.get(17)?,
            cleanup_status: row.get(18)?,
            abort_reason: row.get(19)?,
            input_tokens: row.get(20)?,
            output_tokens: row.get(21)?,
            cache_read_tokens: row.get(22)?,
            cache_create_tokens: row.get(23)?,
            cost_nanos_usd: row.get(24)?,
            usage_complete: row.get::<_, i64>(25)? != 0,
            tool_call_count: row.get(26)?,
            turn_count: row.get(27)?,
            created_at: row.get(28)?,
            updated_at: row.get(29)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn load_invocations(
    transaction: &Transaction<'_>,
    task_id: &str,
) -> Result<Vec<TaskDiagnosticToolInvocation>, DbError> {
    let mut statement = transaction.prepare(
        "SELECT invocation_id,task_id,run_id,tool_use_id,tool_name,status,error_code,
                side_effect_class,cleanup_status,directory_generation,connection_generation,
                version,started_at,terminal_at,created_at,updated_at
           FROM tool_invocations WHERE task_id=?1 ORDER BY created_at,invocation_id",
    )?;
    let rows = statement.query_map(params![task_id], |row| {
        Ok(TaskDiagnosticToolInvocation {
            invocation_id: row.get(0)?,
            task_id: row.get(1)?,
            run_id: row.get(2)?,
            tool_use_id: row.get(3)?,
            tool_name: row.get(4)?,
            status: row.get(5)?,
            error_code: row.get(6)?,
            side_effect_class: row.get(7)?,
            cleanup_status: row.get(8)?,
            directory_generation: row.get(9)?,
            connection_generation: row.get(10)?,
            version: row.get(11)?,
            started_at: row.get(12)?,
            terminal_at: row.get(13)?,
            created_at: row.get(14)?,
            updated_at: row.get(15)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn load_resources(
    transaction: &Transaction<'_>,
    task_id: &str,
) -> Result<Vec<TaskDiagnosticExecutionResource>, DbError> {
    let mut statement = transaction.prepare(
        "SELECT resource_id,task_id,run_id,invocation_id,resource_kind,status,version,
                created_at,updated_at,released_at
           FROM execution_resources WHERE task_id=?1 ORDER BY created_at,resource_id",
    )?;
    let rows = statement.query_map(params![task_id], |row| {
        Ok(TaskDiagnosticExecutionResource {
            resource_id: row.get(0)?,
            task_id: row.get(1)?,
            run_id: row.get(2)?,
            invocation_id: row.get(3)?,
            resource_kind: row.get(4)?,
            status: row.get(5)?,
            version: row.get(6)?,
            created_at: row.get(7)?,
            updated_at: row.get(8)?,
            released_at: row.get(9)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn load_results(
    transaction: &Transaction<'_>,
    task_id: &str,
) -> Result<Vec<TaskDiagnosticResult>, DbError> {
    let mut statement = transaction.prepare(
        "SELECT result_id,task_id,run_id,result_version,status,blob_sha256,byte_len,
                content_sha256,media_type,error_code,final_message_id,created_at
           FROM task_results WHERE task_id=?1 ORDER BY result_version,result_id",
    )?;
    let rows = statement.query_map(params![task_id], |row| {
        Ok(TaskDiagnosticResult {
            result_id: row.get(0)?,
            task_id: row.get(1)?,
            run_id: row.get(2)?,
            result_version: row.get(3)?,
            status: row.get(4)?,
            blob_sha256: row.get(5)?,
            byte_len: row.get(6)?,
            content_sha256: row.get(7)?,
            media_type: row.get(8)?,
            error_code: row.get(9)?,
            final_message_id: row.get(10)?,
            created_at: row.get(11)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn load_receipts(
    transaction: &Transaction<'_>,
    task_id: &str,
) -> Result<Vec<TaskDiagnosticReceipt>, DbError> {
    let mut statement = transaction.prepare(
        "SELECT receipt_id,consumer_task_id,producer_task_id,result_version,message_id,
                result_sha256,created_at FROM task_result_receipts
          WHERE consumer_task_id=?1 OR producer_task_id=?1 ORDER BY created_at,receipt_id",
    )?;
    let rows = statement.query_map(params![task_id], |row| {
        Ok(TaskDiagnosticReceipt {
            receipt_id: row.get(0)?,
            consumer_task_id: row.get(1)?,
            producer_task_id: row.get(2)?,
            result_version: row.get(3)?,
            message_id: row.get(4)?,
            result_sha256: row.get(5)?,
            created_at: row.get(6)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn load_llm_calls(
    transaction: &Transaction<'_>,
    task_id: &str,
) -> Result<Vec<TaskDiagnosticLlmCall>, DbError> {
    let mut statement = transaction.prepare(
        "SELECT call_id,task_id,run_id,provider,model,route,status,input_tokens,output_tokens,
                cache_read_tokens,cache_create_tokens,cost_nanos_usd,reserved_input_tokens,
                reserved_output_tokens,reserved_cost_nanos_usd,usage_complete,error_code,
                started_at,finished_at,created_at,updated_at
           FROM llm_calls WHERE task_id=?1 ORDER BY started_at,call_id",
    )?;
    let rows = statement.query_map(params![task_id], |row| {
        Ok(TaskDiagnosticLlmCall {
            call_id: row.get(0)?,
            task_id: row.get(1)?,
            run_id: row.get(2)?,
            provider: row.get(3)?,
            model: row.get(4)?,
            route: row.get(5)?,
            status: row.get(6)?,
            input_tokens: row.get(7)?,
            output_tokens: row.get(8)?,
            cache_read_tokens: row.get(9)?,
            cache_create_tokens: row.get(10)?,
            cost_nanos_usd: row.get(11)?,
            reserved_input_tokens: row.get(12)?,
            reserved_output_tokens: row.get(13)?,
            reserved_cost_nanos_usd: row.get(14)?,
            usage_complete: row.get::<_, i64>(15)? != 0,
            error_code: row.get(16)?,
            started_at: row.get(17)?,
            finished_at: row.get(18)?,
            created_at: row.get(19)?,
            updated_at: row.get(20)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn load_checkpoints(
    transaction: &Transaction<'_>,
    task_id: &str,
) -> Result<Vec<TaskDiagnosticCheckpoint>, DbError> {
    let mut statement = transaction.prepare(
        "SELECT c.id,c.run_id,c.session_id,c.agent_id,c.seq,c.tool_call_count,c.turn_count,
                c.tokens_consumed,c.created_at FROM agent_checkpoints c
                JOIN run_envelopes r ON r.id=c.run_id
          WHERE r.task_id=?1 ORDER BY r.attempt,c.seq,c.id",
    )?;
    let rows = statement.query_map(params![task_id], |row| {
        Ok(TaskDiagnosticCheckpoint {
            checkpoint_id: row.get(0)?,
            run_id: row.get(1)?,
            session_id: row.get(2)?,
            agent_id: row.get(3)?,
            sequence: row.get(4)?,
            tool_call_count: row.get(5)?,
            turn_count: row.get(6)?,
            tokens_consumed: row.get(7)?,
            created_at: row.get(8)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
fn assess_resume_eligibility(
    task: &TaskDiagnosticTask,
    runs: &[TaskDiagnosticRun],
    invocations: &[TaskDiagnosticToolInvocation],
    resources: &[TaskDiagnosticExecutionResource],
    results: &[TaskDiagnosticResult],
    llm_calls: &[TaskDiagnosticLlmCall],
    checkpoints: &[TaskDiagnosticCheckpoint],
) -> Vec<TaskResumeEligibility> {
    let checkpoint_ids: HashMap<&str, &str> = checkpoints
        .iter()
        .map(|checkpoint| {
            (
                checkpoint.run_id.as_str(),
                checkpoint.checkpoint_id.as_str(),
            )
        })
        .collect();
    runs.iter()
        .map(|run| {
            let mut reasons = Vec::new();
            if run.status != "interrupted" {
                reasons.push(TaskResumeIneligibilityReason::one("RUN_NOT_INTERRUPTED"));
            }
            if run.exit_reason.as_deref() != Some("serviceRestart") {
                reasons.push(TaskResumeIneligibilityReason::one(
                    "EXIT_REASON_NOT_SERVICE_RESTART",
                ));
            }
            if task.status != "needsAttention" {
                reasons.push(TaskResumeIneligibilityReason::one(
                    "TASK_NOT_NEEDS_ATTENTION",
                ));
            }
            if task.current_run_id.as_deref() != Some(run.run_id.as_str()) {
                reasons.push(TaskResumeIneligibilityReason::one("NOT_CURRENT_ATTEMPT"));
            }
            match (
                run.checkpoint_id.as_deref(),
                checkpoint_ids.get(run.run_id.as_str()),
            ) {
                (None, _) | (_, None) => {
                    reasons.push(TaskResumeIneligibilityReason::one("CHECKPOINT_MISSING"));
                }
                (Some(pointer), Some(latest)) if pointer != *latest => reasons.push(
                    TaskResumeIneligibilityReason::one("CHECKPOINT_POINTER_STALE"),
                ),
                _ => {}
            }
            let active_tools = invocations
                .iter()
                .filter(|item| {
                    item.run_id == run.run_id
                        && matches!(item.status.as_str(), "preparing" | "queued" | "running")
                })
                .count();
            if active_tools > 0 {
                reasons.push(TaskResumeIneligibilityReason::counted(
                    "ACTIVE_TOOL_INVOCATIONS",
                    active_tools,
                ));
            }
            let unknown_side_effects = invocations
                .iter()
                .filter(|item| item.run_id == run.run_id && item.side_effect_class == "unknown")
                .count();
            if unknown_side_effects > 0 {
                reasons.push(TaskResumeIneligibilityReason::counted(
                    "UNKNOWN_TOOL_SIDE_EFFECTS",
                    unknown_side_effects,
                ));
            }
            let unsafe_writes = invocations
                .iter()
                .filter(|item| {
                    item.run_id == run.run_id
                        && item.side_effect_class == "write"
                        && item.status != "succeeded"
                })
                .count();
            if unsafe_writes > 0 {
                reasons.push(TaskResumeIneligibilityReason::counted(
                    "UNRESOLVED_WRITE_SIDE_EFFECTS",
                    unsafe_writes,
                ));
            }
            let active_resources = resources
                .iter()
                .filter(|item| {
                    item.run_id == run.run_id
                        && matches!(item.status.as_str(), "allocated" | "stopping")
                })
                .count();
            if active_resources > 0 {
                reasons.push(TaskResumeIneligibilityReason::counted(
                    "ACTIVE_EXECUTION_RESOURCES",
                    active_resources,
                ));
            }
            let unconfirmed_resources = resources
                .iter()
                .filter(|item| item.run_id == run.run_id && item.status == "unconfirmed")
                .count();
            if unconfirmed_resources > 0 {
                reasons.push(TaskResumeIneligibilityReason::counted(
                    "UNCONFIRMED_EXECUTION_RESOURCES",
                    unconfirmed_resources,
                ));
            }
            if matches!(task.cleanup_status.as_str(), "pending" | "unconfirmed")
                || matches!(run.cleanup_status.as_str(), "pending" | "unconfirmed")
            {
                reasons.push(TaskResumeIneligibilityReason::one("CLEANUP_NOT_CONFIRMED"));
            }
            let incomplete_calls = llm_calls
                .iter()
                .filter(|call| call.run_id == run.run_id && !call.usage_complete)
                .count();
            if !task.usage_complete || !run.usage_complete || incomplete_calls > 0 {
                reasons.push(TaskResumeIneligibilityReason::counted(
                    "USAGE_INCOMPLETE",
                    incomplete_calls,
                ));
            }
            let active_calls = llm_calls
                .iter()
                .filter(|call| call.run_id == run.run_id && call.status == "started")
                .count();
            if active_calls > 0 {
                reasons.push(TaskResumeIneligibilityReason::counted(
                    "ACTIVE_LLM_CALLS",
                    active_calls,
                ));
            }
            let committed_results = results
                .iter()
                .filter(|result| result.run_id == run.run_id)
                .count();
            if committed_results > 0 {
                reasons.push(TaskResumeIneligibilityReason::counted(
                    "RESULT_ALREADY_COMMITTED",
                    committed_results,
                ));
            }
            if task
                .deadline_at_ms
                .is_some_and(|deadline| deadline <= now_millis())
            {
                reasons.push(TaskResumeIneligibilityReason::one("DEADLINE_EXPIRED"));
            }
            if task.token_budget_limit.is_some_and(|limit| {
                task.budget_reserved_tokens
                    .saturating_add(task.budget_consumed_tokens)
                    >= limit
            }) {
                reasons.push(TaskResumeIneligibilityReason::one("TOKEN_BUDGET_EXHAUSTED"));
            }
            if task.cost_budget_nanos_usd.is_some_and(|limit| {
                task.budget_reserved_cost_nanos_usd
                    .saturating_add(task.budget_consumed_cost_nanos_usd)
                    >= limit
            }) {
                reasons.push(TaskResumeIneligibilityReason::one("COST_BUDGET_EXHAUSTED"));
            }
            let eligible = reasons.is_empty();
            TaskResumeEligibility {
                run_id: run.run_id.clone(),
                attempt: run.attempt,
                eligible,
                runtime_revalidation_required: eligible,
                reasons,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn missing_task_returns_none() {
        let db = Db::open_in_memory().expect("database");
        assert!(
            db.find_task_diagnostic("missing")
                .await
                .expect("query")
                .is_none()
        );
    }

    #[tokio::test]
    async fn aggregates_every_ledger_without_sensitive_payloads() {
        let db = Db::open_in_memory().expect("database");
        let session = db
            .create_session("model", "/secret/workspace")
            .await
            .expect("session");
        db.start_run("diagnostic-task", &session.id, None, Some("query"), "model")
            .await
            .expect("run");
        db.start_run("consumer-task", &session.id, None, Some("query"), "model")
            .await
            .expect("consumer");
        db.with_conn_blocking(|connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "UPDATE tasks SET prompt='secret-task-prompt',description='secret-description',
                 reason='secret-reason',plan_json='secret-plan',execution_config_json='secret-config',
                 status='needsAttention',cleanup_status='confirmed' WHERE id='diagnostic-task'",
                [],
            )?;
            transaction.execute(
                "UPDATE run_envelopes SET status='interrupted',exit_reason='serviceRestart',
                 terminal_at='2026-01-01T00:00:00Z',finished_at='2026-01-01T00:00:00Z',
                 error_summary=?1,waiting_reason=?2 WHERE id='diagnostic-task'",
                params!["secret-run-error", "secret-wait"],
            )?;
            transaction.execute(
                "INSERT INTO agent_checkpoints(id,run_id,session_id,agent_id,seq,messages_json,
                 file_state_json,tool_call_count,turn_count,tokens_consumed,working_dir,created_at)
                 VALUES('checkpoint-1','diagnostic-task',?1,'agent-1',1,'[\"secret-message\"]',
                 '{\"secret-file\":true}',1,2,3,'/secret/workspace','2026-01-01T00:00:00Z')",
                params![session.id],
            )?;
            transaction.execute(
                "UPDATE run_envelopes SET checkpoint_id='checkpoint-1' WHERE id='diagnostic-task'",
                [],
            )?;
            transaction.execute(
                "INSERT INTO tool_invocations(invocation_id,task_id,run_id,tool_use_id,tool_name,
                 status,input_json,output_ref,side_effect_class,cleanup_status,version,terminal_at,
                 created_at,updated_at) VALUES('invocation-1','diagnostic-task','diagnostic-task',
                 'tool-use-1','Read','succeeded','{\"token\":\"secret-input\"}',
                 'secret-output','read','confirmed',1,'2026-01-01T00:00:00Z',
                 '2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
                [],
            )?;
            transaction.execute(
                "INSERT INTO execution_resources(resource_id,task_id,run_id,invocation_id,
                 resource_kind,external_id,status,metadata_json,version,created_at,updated_at,released_at)
                 VALUES('resource-1','diagnostic-task','diagnostic-task','invocation-1','process',
                 'secret-pid','released','{\"credential\":\"secret-metadata\"}',1,
                 '2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
                [],
            )?;
            let hash = "a".repeat(64);
            transaction.execute(
                "INSERT INTO task_results(result_id,task_id,run_id,result_version,status,inline_text,
                 byte_len,content_sha256,media_type,created_at) VALUES('result-1','diagnostic-task',
                 'diagnostic-task',1,'partial','secret-result-body',18,?1,'text/plain',
                 '2026-01-01T00:00:00Z')",
                params![hash],
            )?;
            transaction.execute(
                "INSERT INTO messages(id,session_id,role,content_json,origin,created_at,seq_num)
                 VALUES('receipt-message',?1,'assistant','[{\"text\":\"secret-receipt-body\"}]',
                 'task_result','2026-01-01T00:00:00Z',99)",
                params![session.id],
            )?;
            transaction.execute(
                "INSERT INTO task_result_receipts(receipt_id,consumer_task_id,producer_task_id,
                 result_version,message_id,result_sha256,created_at) VALUES('receipt-1','consumer-task',
                 'diagnostic-task',1,'receipt-message',?1,'2026-01-01T00:00:00Z')",
                params![hash],
            )?;
            transaction.execute(
                "INSERT INTO llm_calls(call_id,task_id,run_id,provider,model,route,
                 provider_request_id,status,input_tokens,output_tokens,cache_read_tokens,
                 cache_create_tokens,cost_nanos_usd,usage_complete,started_at,finished_at,
                 created_at,updated_at) VALUES('call-1','diagnostic-task','diagnostic-task',
                 'provider','model','primary','secret-provider-request','completed',10,5,0,0,42,1,
                 '2026-01-01T00:00:00Z','2026-01-01T00:00:01Z',
                 '2026-01-01T00:00:00Z','2026-01-01T00:00:01Z')",
                [],
            )?;
            transaction.commit()?;
            Ok(())
        }).expect("seed ledgers");

        let diagnostic = db
            .find_task_diagnostic("diagnostic-task")
            .await
            .expect("query")
            .expect("task");
        assert_eq!(diagnostic.attempts.len(), 1);
        assert_eq!(diagnostic.tool_invocations.len(), 1);
        assert_eq!(diagnostic.execution_resources.len(), 1);
        assert_eq!(diagnostic.results.len(), 1);
        assert_eq!(diagnostic.receipts.len(), 1);
        assert_eq!(diagnostic.llm_calls.len(), 1);
        assert_eq!(diagnostic.checkpoints.len(), 1);
        assert_eq!(diagnostic.resume_eligibility.len(), 1);
        assert!(
            !diagnostic.resume_eligibility[0].eligible,
            "committed result must make retry unsafe"
        );

        let json = serde_json::to_string(&diagnostic).expect("serialize diagnostic");
        for secret in [
            "secret-task-prompt",
            "secret-description",
            "secret-reason",
            "secret-plan",
            "secret-config",
            "secret-run-error",
            "secret-wait",
            "secret-message",
            "secret-file",
            "secret-input",
            "secret-output",
            "secret-pid",
            "secret-metadata",
            "secret-result-body",
            "secret-receipt-body",
            "secret-provider-request",
            "/secret/workspace",
        ] {
            assert!(!json.contains(secret), "diagnostic leaked {secret}");
        }
    }
}
