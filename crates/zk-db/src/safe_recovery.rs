//! Fail-closed eligibility and atomic attempt creation for restart recovery.
//!
//! Startup reconciliation always interrupts the previous process' active Runs
//! first. This module may then create a new queued attempt only when every
//! persisted fact needed to prove a read-only replay is still true.

#![allow(clippy::missing_errors_doc, clippy::too_many_lines, missing_docs)]

use std::fmt::Write as _;

use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::time::{format_rfc3339_micros, now_millis};
use crate::{AgentCheckpointRecord, Db, DbError};

const STARTUP_EPOCH_KEY: &str = "runtime.startupEpoch";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryEligibility {
    pub eligible: bool,
    pub code: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SafeRecoveryCandidate {
    pub task_id: String,
    pub previous_run_id: String,
    pub transcript_session_id: String,
    pub parent_task_id: Option<String>,
    pub parent_run_id: Option<String>,
    pub attempt: i64,
    pub task_version: i64,
    pub model: String,
    pub prompt: String,
    pub working_dir: String,
    pub execution_config: Value,
    pub execution_config_sha256: String,
    pub permission_fingerprint: String,
    pub workspace_binding_sha256: String,
    pub checkpoint_id: String,
    pub checkpoint: Value,
    pub eligibility: RecoveryEligibility,
}

#[derive(Clone, Debug)]
pub struct CreateSafeRecoveryAttempt {
    pub task_id: String,
    pub previous_run_id: String,
    pub expected_task_version: i64,
    pub startup_epoch: i64,
    pub execution_config_sha256: String,
    pub permission_fingerprint: String,
    pub workspace_binding_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SafeRecoveryAttempt {
    pub task_id: String,
    pub run_id: String,
    pub transcript_session_id: String,
    pub parent_task_id: Option<String>,
    pub parent_run_id: Option<String>,
    pub attempt: i64,
    pub startup_epoch: i64,
    pub model: String,
    pub prompt: String,
    pub working_dir: String,
    pub execution_config: Value,
    pub checkpoint: Value,
}

#[derive(Debug)]
struct RecoveryOwner {
    task_id: String,
    parent_task_id: Option<String>,
    task_status: String,
    task_cleanup_status: String,
    task_type: String,
    execution_config_json: String,
    prompt: Option<String>,
    task_version: i64,
    task_usage_complete: bool,
    token_limit: Option<i64>,
    cost_limit: Option<i64>,
    deadline_at_ms: Option<i64>,
    consumed_tokens: i64,
    consumed_cost: i64,
    root_usage_complete: bool,
    run_id: String,
    attempt: i64,
    run_startup_epoch: i64,
    run_status: String,
    exit_reason: Option<String>,
    run_cleanup_status: String,
    run_usage_complete: bool,
    run_tokens: i64,
    run_cost: i64,
    transcript_session_id: String,
    model: String,
    parent_run_id: Option<String>,
    prompt_hash: Option<String>,
    checkpoint_id: Option<String>,
    working_dir: String,
}

impl Db {
    /// Persist and return the monotonically increasing process-start epoch.
    /// The data-directory OS lease makes this a single-owner sequence, while
    /// the transaction keeps it durable across a crash before reconciliation.
    pub async fn begin_runtime_startup_epoch(&self) -> Result<i64, DbError> {
        self.with_writer(|connection| {
            let tx = connection.transaction()?;
            let previous = tx
                .query_row(
                    "SELECT value FROM config WHERE key=?1",
                    params![STARTUP_EPOCH_KEY],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map_or(Ok(0_i64), |value| {
                    value.parse::<i64>().map_err(|_| {
                        DbError::Invalid("STARTUP_EPOCH_CORRUPT".to_owned())
                    })
                })?;
            let next = previous
                .checked_add(1)
                .ok_or_else(|| DbError::Invalid("STARTUP_EPOCH_EXHAUSTED".to_owned()))?;
            let now = format_rfc3339_micros(now_millis());
            tx.execute(
                "INSERT INTO config(key,value,updated_at) VALUES(?1,?2,?3)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                params![STARTUP_EPOCH_KEY, next.to_string(), now],
            )?;
            tx.commit()?;
            Ok(next)
        })
        .await
    }

    /// Inspect all quarantined tasks against the current startup epoch. The
    /// result includes rejected candidates so operators can see the exact
    /// fail-closed reason without changing durable state.
    pub async fn inspect_safe_recovery_candidates(
        &self,
        startup_epoch: i64,
    ) -> Result<Vec<SafeRecoveryCandidate>, DbError> {
        if startup_epoch <= 0 {
            return Err(DbError::Invalid("STARTUP_EPOCH_INVALID".to_owned()));
        }
        self.with_reader(move |connection| {
            let task_ids = {
                let mut statement = connection.prepare(
                    "SELECT id FROM tasks WHERE status='needsAttention' ORDER BY created_at,id",
                )?;
                statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?
            };
            task_ids
                .iter()
                .map(|task_id| assess_candidate(connection, task_id, startup_epoch))
                .collect()
        })
        .await
    }

    /// Re-check eligibility and atomically create `attempt + 1`, copy the
    /// restorable checkpoint onto the new Run, carry prior-attempt usage into
    /// the Task budget, and make the queued Run current.
    pub async fn create_safe_recovery_attempt(
        &self,
        request: &CreateSafeRecoveryAttempt,
    ) -> Result<SafeRecoveryAttempt, DbError> {
        if request.startup_epoch <= 0 {
            return Err(DbError::Invalid("STARTUP_EPOCH_INVALID".to_owned()));
        }
        let request = request.clone();
        self.with_writer(move |connection| {
            let tx = connection.transaction()?;
            let candidate = assess_candidate(&tx, &request.task_id, request.startup_epoch)?;
            if !candidate.eligibility.eligible {
                return Err(DbError::Invalid(format!(
                    "SAFE_RECOVERY_INELIGIBLE:{}",
                    candidate.eligibility.code
                )));
            }
            if candidate.previous_run_id != request.previous_run_id
                || candidate.task_version != request.expected_task_version
                || candidate.execution_config_sha256 != request.execution_config_sha256
                || candidate.permission_fingerprint != request.permission_fingerprint
                || candidate.workspace_binding_sha256 != request.workspace_binding_sha256
            {
                return Err(DbError::Invalid("SAFE_RECOVERY_PROOF_CHANGED".to_owned()));
            }

            let now_ms = now_millis();
            let now = format_rfc3339_micros(now_ms);
            let new_run_id = uuid::Uuid::new_v4().to_string();
            let new_checkpoint_id = uuid::Uuid::new_v4().to_string();
            let next_attempt = candidate
                .attempt
                .checked_add(1)
                .ok_or_else(|| DbError::Invalid("RUN_ATTEMPT_EXHAUSTED".to_owned()))?;

            let owner = load_owner(&tx, &candidate.task_id)?;
            let carried_tokens = owner.consumed_tokens.saturating_add(owner.run_tokens);
            let carried_cost = owner.consumed_cost.saturating_add(owner.run_cost);
            if owner
                .token_limit
                .is_some_and(|limit| carried_tokens >= limit)
            {
                return Err(DbError::Invalid(
                    "SAFE_RECOVERY_TOKEN_BUDGET_EXHAUSTED".to_owned(),
                ));
            }
            if owner.cost_limit.is_some_and(|limit| carried_cost >= limit) {
                return Err(DbError::Invalid(
                    "SAFE_RECOVERY_COST_BUDGET_EXHAUSTED".to_owned(),
                ));
            }

            let mut checkpoint = candidate.checkpoint.clone();
            if let Some(object) = checkpoint.as_object_mut() {
                object.insert(
                    "reason".to_owned(),
                    Value::String("contextRecovered".to_owned()),
                );
                object.insert("terminalReason".to_owned(), Value::Null);
                if let Some(proof) = object
                    .get_mut("recoveryProof")
                    .and_then(Value::as_object_mut)
                {
                    proof.insert("runId".to_owned(), Value::String(new_run_id.clone()));
                    proof.insert("startupEpoch".to_owned(), json!(request.startup_epoch));
                    proof.insert(
                        "recoveredFromRunId".to_owned(),
                        Value::String(candidate.previous_run_id.clone()),
                    );
                    proof.insert(
                        "budget".to_owned(),
                        json!({
                            "tokenLimit": owner.token_limit,
                            "costLimitNanosUsd": owner.cost_limit,
                            "deadlineAtMs": owner.deadline_at_ms,
                            "consumedTokens": carried_tokens,
                            "consumedCostNanosUsd": carried_cost,
                            "usageComplete": true,
                        }),
                    );
                }
            }
            let checkpoint_json = serde_json::to_string(&checkpoint)?;

            tx.execute(
                "INSERT INTO run_envelopes
                    (id,session_id,task_id,attempt,startup_epoch,checkpoint_id,parent_run_id,
                     status,version,agent_type,model,prompt_hash,started_at,
                     verification_status,cleanup_status,total_tokens,total_cost_usd,
                     input_tokens,output_tokens,cache_read_tokens,cache_create_tokens,
                     cost_nanos_usd,usage_complete,tool_call_count,turn_count,created_at,updated_at)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,'queued',0,'subagent',?8,?9,?10,
                        'notRequested','notRequired',0,0.0,0,0,0,0,0,1,0,0,?10,?10)",
                params![
                    new_run_id,
                    candidate.transcript_session_id,
                    candidate.task_id,
                    next_attempt,
                    request.startup_epoch,
                    new_checkpoint_id,
                    candidate.parent_run_id,
                    candidate.model,
                    owner.prompt_hash,
                    now,
                ],
            )?;
            tx.execute(
                "INSERT INTO agent_checkpoints
                    (id,run_id,session_id,agent_id,seq,messages_json,file_state_json,
                     tool_call_count,turn_count,tokens_consumed,working_dir,created_at)
                 SELECT ?1,?2,session_id,agent_id,0,?3,file_state_json,
                        tool_call_count,turn_count,tokens_consumed,working_dir,?4
                 FROM agent_checkpoints WHERE id=?5 AND run_id=?6",
                params![
                    new_checkpoint_id,
                    new_run_id,
                    checkpoint_json,
                    now,
                    candidate.checkpoint_id,
                    candidate.previous_run_id,
                ],
            )?;
            let task_updated = tx.execute(
                "UPDATE tasks SET current_run_id=?1,status='queued',reason='safeRecoveryQueued',
                    cleanup_status='notRequired',verification_status='notRequested',
                    budget_consumed_tokens=?2,budget_consumed_cost_nanos_usd=?3,
                    budget_version=budget_version+1,updated_at=?4,version=version+1
                 WHERE id=?5 AND current_run_id=?6 AND version=?7 AND status='needsAttention'",
                params![
                    new_run_id,
                    carried_tokens,
                    carried_cost,
                    now,
                    candidate.task_id,
                    candidate.previous_run_id,
                    request.expected_task_version,
                ],
            )?;
            if task_updated != 1 {
                return Err(DbError::Invalid(
                    "SAFE_RECOVERY_TASK_VERSION_CONFLICT".to_owned(),
                ));
            }
            crate::run::append_event_in_current_write(
                &tx,
                &new_run_id,
                "safe_recovery_queued",
                None,
                &json!({
                    "protocolVersion": 4,
                    "taskId": candidate.task_id,
                    "runId": new_run_id,
                    "previousRunId": candidate.previous_run_id,
                    "attempt": next_attempt,
                    "startupEpoch": request.startup_epoch,
                    "checkpointId": new_checkpoint_id,
                }),
            )?;
            tx.commit()?;
            Ok(SafeRecoveryAttempt {
                task_id: candidate.task_id,
                run_id: new_run_id,
                transcript_session_id: candidate.transcript_session_id,
                parent_task_id: candidate.parent_task_id,
                parent_run_id: candidate.parent_run_id,
                attempt: next_attempt,
                startup_epoch: request.startup_epoch,
                model: candidate.model,
                prompt: candidate.prompt,
                working_dir: candidate.working_dir,
                execution_config: candidate.execution_config,
                checkpoint,
            })
        })
        .await
    }
}

fn load_owner(connection: &Connection, task_id: &str) -> Result<RecoveryOwner, DbError> {
    connection
        .query_row(
            "SELECT task.id,task.parent_task_id,task.status,task.cleanup_status,task.task_type,
                    task.execution_config_json,task.prompt,task.version,task.usage_complete,
                    task.token_budget_limit,task.cost_budget_nanos_usd,task.deadline_at_ms,
                    task.budget_consumed_tokens,task.budget_consumed_cost_nanos_usd,
                    root.usage_complete,
                    run.id,run.attempt,run.startup_epoch,run.status,run.exit_reason,
                    run.cleanup_status,run.usage_complete,run.total_tokens,run.cost_nanos_usd,
                    run.session_id,run.model,run.parent_run_id,run.prompt_hash,run.checkpoint_id,
                    session.working_dir
             FROM tasks task
             JOIN tasks root ON root.id=task.root_task_id
             JOIN run_envelopes run ON run.id=task.current_run_id AND run.task_id=task.id
             JOIN sessions session ON session.id=run.session_id
             WHERE task.id=?1",
            params![task_id],
            |row| {
                Ok(RecoveryOwner {
                    task_id: row.get(0)?,
                    parent_task_id: row.get(1)?,
                    task_status: row.get(2)?,
                    task_cleanup_status: row.get(3)?,
                    task_type: row.get(4)?,
                    execution_config_json: row.get(5)?,
                    prompt: row.get(6)?,
                    task_version: row.get(7)?,
                    task_usage_complete: row.get::<_, i64>(8)? != 0,
                    token_limit: row.get(9)?,
                    cost_limit: row.get(10)?,
                    deadline_at_ms: row.get(11)?,
                    consumed_tokens: row.get(12)?,
                    consumed_cost: row.get(13)?,
                    root_usage_complete: row.get::<_, i64>(14)? != 0,
                    run_id: row.get(15)?,
                    attempt: row.get(16)?,
                    run_startup_epoch: row.get(17)?,
                    run_status: row.get(18)?,
                    exit_reason: row.get(19)?,
                    run_cleanup_status: row.get(20)?,
                    run_usage_complete: row.get::<_, i64>(21)? != 0,
                    run_tokens: row.get(22)?,
                    run_cost: row.get(23)?,
                    transcript_session_id: row.get(24)?,
                    model: row.get(25)?,
                    parent_run_id: row.get(26)?,
                    prompt_hash: row.get(27)?,
                    checkpoint_id: row.get(28)?,
                    working_dir: row.get(29)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| DbError::Invalid("SAFE_RECOVERY_TASK_NOT_FOUND".to_owned()))
}

fn assess_candidate(
    connection: &Connection,
    task_id: &str,
    startup_epoch: i64,
) -> Result<SafeRecoveryCandidate, DbError> {
    let owner = load_owner(connection, task_id)?;
    let execution_config: Value = serde_json::from_str(&owner.execution_config_json)?;
    let execution_config_sha256 = sha256_hex(owner.execution_config_json.as_bytes());
    let permission_policy = json!({
        "version": execution_config.get("permissionPolicyVersion"),
        "isolation": execution_config.get("isolation"),
        "allowWriteTools": execution_config.get("allowWriteTools"),
        "allowedTools": execution_config.get("allowedTools"),
    });
    let permission_fingerprint = sha256_hex(&serde_json::to_vec(&permission_policy)?);
    let workspace_binding_sha256 = sha256_hex(owner.working_dir.as_bytes());
    let checkpoint_id = owner.checkpoint_id.clone().unwrap_or_default();
    let checkpoint_record = if checkpoint_id.is_empty() {
        None
    } else {
        load_checkpoint(connection, &checkpoint_id)?
    };
    let checkpoint = checkpoint_record
        .as_ref()
        .map_or(Value::Null, |record| record.messages.clone());
    let prompt = owner.prompt.clone().unwrap_or_default();
    let mut candidate = SafeRecoveryCandidate {
        task_id: owner.task_id.clone(),
        previous_run_id: owner.run_id.clone(),
        transcript_session_id: owner.transcript_session_id.clone(),
        parent_task_id: owner.parent_task_id.clone(),
        parent_run_id: owner.parent_run_id.clone(),
        attempt: owner.attempt,
        task_version: owner.task_version,
        model: owner.model.clone(),
        prompt,
        working_dir: owner.working_dir.clone(),
        execution_config,
        execution_config_sha256: execution_config_sha256.clone(),
        permission_fingerprint: permission_fingerprint.clone(),
        workspace_binding_sha256: workspace_binding_sha256.clone(),
        checkpoint_id,
        checkpoint,
        eligibility: eligible(),
    };

    macro_rules! reject {
        ($code:literal, $reason:literal) => {{
            candidate.eligibility = rejected($code, $reason);
            return Ok(candidate);
        }};
    }

    if owner.task_status != "needsAttention" {
        reject!(
            "TASK_NOT_QUARANTINED",
            "task is not awaiting restart review"
        );
    }
    if owner.task_type != "agent" || owner.parent_task_id.is_none() {
        reject!(
            "RECOVERY_KIND_UNSUPPORTED",
            "only attached child Agent tasks have a production recovery executor"
        );
    }
    if owner.run_status != "interrupted" || owner.exit_reason.as_deref() != Some("serviceRestart") {
        reject!(
            "RUN_NOT_SERVICE_INTERRUPTED",
            "current run was not factually interrupted by service restart"
        );
    }
    if owner.run_startup_epoch >= startup_epoch {
        reject!(
            "RUN_EPOCH_NOT_OLDER",
            "run belongs to the current or a future startup epoch"
        );
    }
    if !matches!(
        owner.task_cleanup_status.as_str(),
        "notRequired" | "confirmed"
    ) || !matches!(
        owner.run_cleanup_status.as_str(),
        "notRequired" | "confirmed"
    ) {
        reject!(
            "CLEANUP_NOT_CONFIRMED",
            "task or run cleanup is pending or unconfirmed"
        );
    }
    if !owner.task_usage_complete || !owner.root_usage_complete || !owner.run_usage_complete {
        reject!(
            "USAGE_INCOMPLETE",
            "task, root budget, or interrupted run has incomplete usage"
        );
    }
    if owner
        .deadline_at_ms
        .is_none_or(|deadline| deadline <= now_millis())
    {
        reject!(
            "DEADLINE_EXPIRED",
            "the persisted finite task deadline is absent or expired"
        );
    }
    let carried_tokens = owner.consumed_tokens.saturating_add(owner.run_tokens);
    let carried_cost = owner.consumed_cost.saturating_add(owner.run_cost);
    if owner
        .token_limit
        .is_none_or(|limit| carried_tokens >= limit)
        || owner.cost_limit.is_none_or(|limit| carried_cost >= limit)
    {
        reject!(
            "BUDGET_EXHAUSTED",
            "no provable finite token or cost budget remains"
        );
    }

    let reservation_ok: i64 = connection.query_row(
        "SELECT COUNT(*) FROM task_budget_reservations
         WHERE child_task_id=?1 AND status='active' AND settled_at IS NULL",
        params![owner.task_id],
        |row| row.get(0),
    )?;
    if reservation_ok != 1 {
        reject!(
            "BUDGET_RESERVATION_NOT_ACTIVE",
            "child allocation is missing, settled, or incomplete"
        );
    }
    let active_or_unclean: i64 = connection.query_row(
        "SELECT
            (SELECT COUNT(*) FROM tool_invocations
              WHERE run_id=?1 AND (status IN ('preparing','queued','running')
                   OR cleanup_status IN ('pending','unconfirmed')))
          + (SELECT COUNT(*) FROM execution_resources
              WHERE run_id=?1 AND status<>'released')",
        params![owner.run_id],
        |row| row.get(0),
    )?;
    if active_or_unclean != 0 {
        reject!(
            "ACTIVE_OR_UNCLEAN_RESOURCE",
            "a tool or execution resource is still active or not confirmed released"
        );
    }
    let unsafe_side_effects: i64 = connection.query_row(
        "SELECT COUNT(*) FROM tool_invocations
         WHERE run_id=?1 AND side_effect_class IN ('write','unknown')",
        params![owner.run_id],
        |row| row.get(0),
    )?;
    if unsafe_side_effects != 0 {
        reject!(
            "SIDE_EFFECT_UNKNOWN",
            "write or unknown tool side effects forbid automatic replay"
        );
    }
    let started_calls: i64 = connection.query_row(
        "SELECT COUNT(*) FROM llm_calls
         WHERE run_id=?1 AND (status='started' OR usage_complete=0)",
        params![owner.run_id],
        |row| row.get(0),
    )?;
    if started_calls != 0 {
        reject!(
            "LLM_USAGE_UNKNOWN",
            "a physical model request is unfinished or lacks authoritative usage"
        );
    }
    let pending_interactions: i64 = connection.query_row(
        "SELECT COUNT(*) FROM interaction_requests WHERE run_id=?1 AND status='pending'",
        params![owner.run_id],
        |row| row.get(0),
    )?;
    if pending_interactions != 0 {
        reject!(
            "INTERACTION_PENDING",
            "a user or permission interaction is still unresolved"
        );
    }
    let pending_inbox: i64 = connection.query_row(
        "SELECT COUNT(*) FROM task_inbox_messages
         WHERE task_id=?1 AND status IN ('queued','delivered')",
        params![owner.task_id],
        |row| row.get(0),
    )?;
    if pending_inbox != 0 {
        reject!(
            "INBOX_PENDING",
            "a collaboration message has not reached a safe consumption boundary"
        );
    }
    let child_dependencies: i64 = connection.query_row(
        "SELECT COUNT(*) FROM task_dependencies WHERE parent_task_id=?1",
        params![owner.task_id],
        |row| row.get(0),
    )?;
    if child_dependencies != 0 {
        reject!(
            "DEPENDENCIES_UNSUPPORTED",
            "recursive or dependency-owning tasks are not automatically replayed"
        );
    }
    let existing_results: i64 = connection.query_row(
        "SELECT COUNT(*) FROM task_results WHERE task_id=?1",
        params![owner.task_id],
        |row| row.get(0),
    )?;
    if existing_results != 0 {
        reject!(
            "RESULT_ALREADY_COMMITTED",
            "an immutable result already exists for this task"
        );
    }

    let Some(checkpoint_record) = checkpoint_record else {
        reject!(
            "CHECKPOINT_MISSING",
            "the interrupted run has no current typed checkpoint"
        );
    };
    if checkpoint_record.run_id != owner.run_id
        || checkpoint_record.session_id != owner.transcript_session_id
        || checkpoint_record.agent_id != owner.task_id
        || checkpoint_record.working_dir.as_deref() != Some(owner.working_dir.as_str())
    {
        reject!(
            "CHECKPOINT_OWNER_MISMATCH",
            "checkpoint task, run, session, agent, or workspace binding differs"
        );
    }
    if checkpoint_record
        .messages
        .get("kind")
        .and_then(Value::as_str)
        != Some("contextCheckpoint")
        || checkpoint_record
            .messages
            .get("restorable")
            .and_then(Value::as_bool)
            != Some(true)
        || checkpoint_record
            .messages
            .get("truncated")
            .and_then(Value::as_bool)
            != Some(false)
        || checkpoint_record
            .messages
            .get("model")
            .and_then(Value::as_str)
            != Some(owner.model.as_str())
        || !checkpoint_messages_are_replayable(&checkpoint_record.messages)
    {
        reject!(
            "CHECKPOINT_NOT_RESTORABLE",
            "checkpoint is truncated, image-bearing, untyped, or otherwise incomplete"
        );
    }
    let Some(proof) = checkpoint_record
        .messages
        .get("recoveryProof")
        .and_then(Value::as_object)
    else {
        reject!(
            "RECOVERY_PROOF_MISSING",
            "checkpoint predates the fail-closed recovery proof contract"
        );
    };
    if proof.get("proofVersion").and_then(Value::as_u64) != Some(1)
        || proof.get("supported").and_then(Value::as_bool) != Some(true)
        || proof.get("taskId").and_then(Value::as_str) != Some(owner.task_id.as_str())
        || proof.get("runId").and_then(Value::as_str) != Some(owner.run_id.as_str())
        || proof.get("sessionId").and_then(Value::as_str)
            != Some(owner.transcript_session_id.as_str())
        || proof.get("executionConfigSha256").and_then(Value::as_str)
            != Some(execution_config_sha256.as_str())
        || proof.get("permissionFingerprint").and_then(Value::as_str)
            != Some(permission_fingerprint.as_str())
        || proof.get("workspaceBindingSha256").and_then(Value::as_str)
            != Some(workspace_binding_sha256.as_str())
    {
        reject!(
            "RECOVERY_PROOF_MISMATCH",
            "checkpoint execution, permission, or workspace proof no longer matches"
        );
    }
    let proof_budget = proof.get("budget").cloned().unwrap_or(Value::Null);
    let expected_budget = json!({
        "tokenLimit": owner.token_limit,
        "costLimitNanosUsd": owner.cost_limit,
        "deadlineAtMs": owner.deadline_at_ms,
        "consumedTokens": owner.consumed_tokens,
        "consumedCostNanosUsd": owner.consumed_cost,
        "usageComplete": owner.task_usage_complete,
    });
    if proof_budget != expected_budget {
        reject!(
            "BUDGET_PROOF_MISMATCH",
            "checkpoint budget snapshot differs from the durable task account"
        );
    }
    candidate.eligibility = eligible();
    Ok(candidate)
}

fn checkpoint_messages_are_replayable(checkpoint: &Value) -> bool {
    let Some(messages) = checkpoint.get("messages").and_then(Value::as_array) else {
        return false;
    };
    !messages.is_empty()
        && messages.iter().all(|message| {
            matches!(
                message.get("role").and_then(Value::as_str),
                Some("system" | "user" | "assistant" | "tool")
            ) && message.get("content").is_some_and(Value::is_string)
                && message
                    .get("images")
                    .and_then(Value::as_array)
                    .is_some_and(Vec::is_empty)
                && message
                    .get("toolCalls")
                    .and_then(Value::as_array)
                    .is_some_and(|calls| {
                        calls.iter().all(|call| {
                            call.get("id").is_some_and(Value::is_string)
                                && call.get("name").is_some_and(Value::is_string)
                                && call.get("arguments").is_some_and(Value::is_string)
                        })
                    })
        })
}

fn load_checkpoint(
    connection: &Connection,
    checkpoint_id: &str,
) -> Result<Option<AgentCheckpointRecord>, DbError> {
    connection
        .query_row(
            "SELECT id,run_id,session_id,agent_id,seq,messages_json,file_state_json,
                    tool_call_count,turn_count,tokens_consumed,working_dir,created_at
             FROM agent_checkpoints WHERE id=?1",
            params![checkpoint_id],
            |row| {
                let messages_json: String = row.get(5)?;
                let file_state_json: Option<String> = row.get(6)?;
                Ok(AgentCheckpointRecord {
                    id: row.get(0)?,
                    run_id: row.get(1)?,
                    session_id: row.get(2)?,
                    agent_id: row.get(3)?,
                    seq: row.get(4)?,
                    messages: serde_json::from_str(&messages_json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                    file_state: file_state_json
                        .as_deref()
                        .map(serde_json::from_str)
                        .transpose()
                        .map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                6,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })?,
                    tool_call_count: row.get(7)?,
                    turn_count: row.get(8)?,
                    tokens_consumed: row.get(9)?,
                    working_dir: row.get(10)?,
                    created_at: row.get(11)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn eligible() -> RecoveryEligibility {
    RecoveryEligibility {
        eligible: true,
        code: "ELIGIBLE".to_owned(),
        reason: "all durable recovery proofs match".to_owned(),
    }
}

fn rejected(code: &str, reason: &str) -> RecoveryEligibility {
    RecoveryEligibility {
        eligible: false,
        code: code.to_owned(),
        reason: reason.to_owned(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CasOutcome, CreateTaskWithRun, TaskBudgetLimits, new_agent_checkpoint};

    struct Fixture {
        db: Db,
        child_task_id: String,
        child_run_id: String,
    }

    async fn fixture() -> Fixture {
        let db = Db::open_in_memory().expect("db");
        let working_dir = "/tmp/zk-safe-recovery";
        let session = db
            .create_session("model", working_dir)
            .await
            .expect("session");
        let root_run_id = uuid::Uuid::new_v4().to_string();
        db.start_root_run_with_budget(
            &root_run_id,
            &session.id,
            Some(crate::run::AGENT_TYPE_QUERY),
            "model",
            &TaskBudgetLimits {
                token_limit: Some(10_000),
                cost_limit_nanos_usd: Some(10_000_000),
                deadline_at_ms: Some(now_millis() + 3_600_000),
            },
        )
        .await
        .expect("root");
        let child_task_id = uuid::Uuid::new_v4().to_string();
        let child_run_id = uuid::Uuid::new_v4().to_string();
        let child_session_id = uuid::Uuid::new_v4().to_string();
        let created = db
            .create_task_with_run(&CreateTaskWithRun {
                task_id: child_task_id.clone(),
                run_id: child_run_id.clone(),
                root_session_id: session.id.clone(),
                transcript_session_id: child_session_id.clone(),
                parent_task_id: Some(root_run_id.clone()),
                parent_run_id: Some(root_run_id),
                creator_tool_use_id: Some(uuid::Uuid::new_v4().to_string()),
                ordinal: 0,
                description: "recoverable child".to_owned(),
                prompt: Some("continue the read-only investigation".to_owned()),
                task_type: "agent".to_owned(),
                model: "model".to_owned(),
                working_dir: working_dir.to_owned(),
                execution_config_json: json!({
                    "isolation": "readOnly",
                    "lifecycle": "attached",
                    "allowWriteTools": false,
                    "allowedTools": ["Read", "Grep"],
                    "permissionPolicyVersion": 1,
                })
                .to_string(),
                startup_epoch: 0,
            })
            .await
            .expect("child");
        assert_eq!(
            db.claim_task_run_cas(&child_task_id, &child_run_id, created.task.version)
                .await
                .expect("claim"),
            CasOutcome::Applied
        );
        let mut checkpoint = new_agent_checkpoint(
            &child_run_id,
            &child_session_id,
            &child_task_id,
            0,
            json!({
                "schemaVersion": 1,
                "kind": "contextCheckpoint",
                "reason": "turnCadence",
                "restorable": true,
                "truncated": false,
                "model": "model",
                "systemPrompt": "You are a read-only child.",
                "messages": [{
                    "role": "user",
                    "content": "read only",
                    "thinking": null,
                    "toolCalls": [],
                    "toolCallId": null,
                    "images": [],
                }],
            }),
        );
        checkpoint.working_dir = Some(working_dir.to_owned());
        db.save_agent_checkpoint(&checkpoint)
            .await
            .expect("checkpoint");
        Fixture {
            db,
            child_task_id,
            child_run_id,
        }
    }

    fn request(candidate: &SafeRecoveryCandidate, startup_epoch: i64) -> CreateSafeRecoveryAttempt {
        CreateSafeRecoveryAttempt {
            task_id: candidate.task_id.clone(),
            previous_run_id: candidate.previous_run_id.clone(),
            expected_task_version: candidate.task_version,
            startup_epoch,
            execution_config_sha256: candidate.execution_config_sha256.clone(),
            permission_fingerprint: candidate.permission_fingerprint.clone(),
            workspace_binding_sha256: candidate.workspace_binding_sha256.clone(),
        }
    }

    async fn eligible_child(fixture: &Fixture, epoch: i64) -> SafeRecoveryCandidate {
        fixture
            .db
            .inspect_safe_recovery_candidates(epoch)
            .await
            .expect("inspect")
            .into_iter()
            .find(|candidate| candidate.task_id == fixture.child_task_id)
            .expect("child candidate")
    }

    #[tokio::test]
    async fn recovery_attempt_is_atomic_and_survives_another_pre_dispatch_crash() {
        let fixture = fixture().await;
        let first_epoch = fixture
            .db
            .begin_runtime_startup_epoch()
            .await
            .expect("epoch 1");
        fixture
            .db
            .reconcile_runtime_after_restart()
            .await
            .expect("reconcile 1");
        let candidate = eligible_child(&fixture, first_epoch).await;
        assert!(
            candidate.eligibility.eligible,
            "{:?}",
            candidate.eligibility
        );
        let attempt = fixture
            .db
            .create_safe_recovery_attempt(&request(&candidate, first_epoch))
            .await
            .expect("attempt 2");
        assert_eq!(attempt.attempt, 2);
        assert_eq!(attempt.startup_epoch, first_epoch);
        assert_ne!(attempt.run_id, fixture.child_run_id);
        let current = fixture
            .db
            .find_runtime_task_by_id(&fixture.child_task_id)
            .await
            .expect("task")
            .expect("task exists");
        assert_eq!(current.status.as_db(), "queued");
        assert_eq!(
            current.current_run_id.as_deref(),
            Some(attempt.run_id.as_str())
        );

        // Crash before the dispatcher claims attempt 2. Startup 2 first
        // interrupts it and then proves the copied checkpoint safe again.
        let second_epoch = fixture
            .db
            .begin_runtime_startup_epoch()
            .await
            .expect("epoch 2");
        assert_eq!(second_epoch, first_epoch + 1);
        fixture
            .db
            .reconcile_runtime_after_restart()
            .await
            .expect("reconcile 2");
        let candidate = eligible_child(&fixture, second_epoch).await;
        assert!(
            candidate.eligibility.eligible,
            "{:?}",
            candidate.eligibility
        );
        assert_eq!(candidate.attempt, 2);
        let third = fixture
            .db
            .create_safe_recovery_attempt(&request(&candidate, second_epoch))
            .await
            .expect("attempt 3");
        assert_eq!(third.attempt, 3);

        let attempts: i64 = fixture
            .db
            .with_reader({
                let task_id = fixture.child_task_id.clone();
                move |connection| {
                    connection
                        .query_row(
                            "SELECT COUNT(*) FROM run_envelopes WHERE task_id=?1",
                            params![task_id],
                            |row| row.get(0),
                        )
                        .map_err(Into::into)
                }
            })
            .await
            .expect("attempt count");
        assert_eq!(attempts, 3);
    }

    #[tokio::test]
    async fn unknown_side_effect_stays_needs_attention() {
        let fixture = fixture().await;
        fixture
            .db
            .with_writer({
                let task_id = fixture.child_task_id.clone();
                let run_id = fixture.child_run_id.clone();
                move |connection| {
                    let now = format_rfc3339_micros(now_millis());
                    connection.execute(
                        "INSERT INTO tool_invocations
                            (invocation_id,task_id,run_id,tool_use_id,tool_name,status,input_json,
                             side_effect_class,cleanup_status,version,terminal_at,created_at,updated_at)
                         VALUES(?1,?2,?3,?4,'UnknownTool','succeeded','{}','unknown',
                                'notRequired',0,?5,?5,?5)",
                        params![
                            uuid::Uuid::new_v4().to_string(),
                            task_id,
                            run_id,
                            uuid::Uuid::new_v4().to_string(),
                            now,
                        ],
                    )?;
                    Ok(())
                }
            })
            .await
            .expect("unsafe invocation");
        let epoch = fixture
            .db
            .begin_runtime_startup_epoch()
            .await
            .expect("epoch");
        fixture
            .db
            .reconcile_runtime_after_restart()
            .await
            .expect("reconcile");
        let candidate = eligible_child(&fixture, epoch).await;
        assert!(!candidate.eligibility.eligible);
        assert_eq!(candidate.eligibility.code, "SIDE_EFFECT_UNKNOWN");
        assert_eq!(
            fixture
                .db
                .find_runtime_task_by_id(&fixture.child_task_id)
                .await
                .expect("task")
                .expect("task exists")
                .status
                .as_db(),
            "needsAttention"
        );
    }

    #[tokio::test]
    async fn checkpoint_copy_failure_rolls_back_the_whole_attempt() {
        let fixture = fixture().await;
        let epoch = fixture
            .db
            .begin_runtime_startup_epoch()
            .await
            .expect("epoch");
        fixture
            .db
            .reconcile_runtime_after_restart()
            .await
            .expect("reconcile");
        let candidate = eligible_child(&fixture, epoch).await;
        assert!(candidate.eligibility.eligible);
        fixture
            .db
            .with_writer(|connection| {
                connection.execute_batch(
                    "CREATE TRIGGER fail_recovery_checkpoint
                     BEFORE INSERT ON agent_checkpoints
                     WHEN EXISTS(
                         SELECT 1 FROM run_envelopes run
                         WHERE run.id=NEW.run_id AND run.attempt>1
                     )
                     BEGIN
                       SELECT RAISE(ABORT,'injected recovery checkpoint failure');
                     END;",
                )?;
                Ok(())
            })
            .await
            .expect("failpoint");
        assert!(
            fixture
                .db
                .create_safe_recovery_attempt(&request(&candidate, epoch))
                .await
                .is_err()
        );
        let task = fixture
            .db
            .find_runtime_task_by_id(&fixture.child_task_id)
            .await
            .expect("task")
            .expect("task exists");
        assert_eq!(task.status.as_db(), "needsAttention");
        assert_eq!(
            task.current_run_id.as_deref(),
            Some(fixture.child_run_id.as_str())
        );
    }
}
