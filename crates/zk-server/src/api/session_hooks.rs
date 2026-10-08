//! Session lifecycle Hooks are host operations owned by the existing `TaskRuntime`.
use std::{path::Path, time::Duration};

use tokio_util::sync::CancellationToken;
use zk_engine::hook::{HookContext, HookEvent};
use zk_engine::{ExternalRootSubmission, TaskExecutionResult, TaskOutputRequest};

use super::{dto::CompactResponse, mapping};
use crate::{error::ApiError, state::AppState};

#[derive(Clone, Copy)]
pub(super) enum Operation {
    Delete,
    Compact,
}
pub(super) enum Response {
    Deleted,
    Compacted(CompactResponse),
}

fn failure(code: &str, message: &str) -> ApiError {
    ApiError {
        status: axum::http::StatusCode::CONFLICT,
        code: code.into(),
        message: message.into(),
    }
}
fn hook_error(code: String) -> ApiError {
    ApiError {
        status: axum::http::StatusCode::CONFLICT,
        code,
        message: "The required session lifecycle Hook did not authorize this operation".into(),
    }
}
struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) async fn execute(
    state: AppState,
    session: String,
    operation: Operation,
) -> Result<Response, ApiError> {
    let reservation = state
        .conversation()
        .as_ref()
        .map(|conversation| {
            conversation
                .try_reserve_session_mutation(&session)
                .map(|guard| Box::new(guard) as Box<dyn Send>)
                .ok_or_else(|| {
                    failure(
                        "SESSION_BUSY",
                        "Wait for the current session operation to finish",
                    )
                })
        })
        .transpose()?;
    state.db.ensure_session_idle(&session).await?;
    let Some(detail) = state.db.get_session(&session).await? else {
        return if matches!(operation, Operation::Delete) {
            Ok(Response::Deleted)
        } else {
            Err(ApiError::session_not_found(&session))
        };
    };
    if state.db.is_merge_billing_session(&session).await? {
        return Err(ApiError::session_not_found(&session));
    }
    let ephemeral =
        state.db.session_retention(&session).await? == zk_db::content::ContentRetention::Ephemeral;
    let context = HookContext::new()
        .with_session(&session)
        .with_working_dir(&detail.working_dir)
        .with_ephemeral_content(ephemeral);
    let events: &[HookEvent] = match operation {
        Operation::Delete => &[HookEvent::SessionEnd],
        Operation::Compact => &[HookEvent::PreCompact, HookEvent::PostCompact],
    };
    if !state.hooks.has_lifecycle_hooks(events, &context) || ephemeral {
        // Required security Hooks still reject incompatible ephemeral operations;
        // optional Hooks cannot export bodies. No empty Task is allocated.
        if ephemeral {
            state
                .hooks
                .fire_lifecycle(events[0], &context)
                .await
                .map_err(hook_error)?;
        }
        let result = match operation {
            Operation::Delete => {
                crate::python::tools::close_session_browser_contexts(
                    &state.db,
                    &state.python,
                    &session,
                )
                .await
                .map_err(|code| failure(&code, "Browser session cleanup is not confirmed"))?;
                state.db.delete_session(&session).await?;
                Response::Deleted
            }
            Operation::Compact => Response::Compacted(save_compaction(&state, &detail).await?.0),
        };
        drop(reservation);
        return Ok(result);
    }
    let cancelled = CancellationToken::new();
    let _request = CancelOnDrop(cancelled.clone());
    let (tx, rx) = tokio::sync::oneshot::channel();
    let supervisor = state.execution_supervisor.clone();
    supervisor
        .spawn_owned_finalizer(
            cancelled.clone(),
            Box::pin(async move {
                // The reservation survives request loss through terminal/resource reconciliation.
                let _reservation = reservation;
                let result = run_owned(&state, detail, operation, cancelled).await;
                let _ = tx.send(result);
            }),
        )
        .map_err(|_| {
            failure(
                "HOOK_SUPERVISOR_UNAVAILABLE",
                "Session lifecycle supervision is unavailable",
            )
        })?;
    rx.await.map_err(|_| {
        failure(
            "HOOK_CLEANUP_UNCONFIRMED",
            "Session lifecycle outcome is unconfirmed",
        )
    })?
}

async fn save_compaction(
    state: &AppState,
    detail: &zk_db::SessionDetail,
) -> Result<(CompactResponse, bool), ApiError> {
    let outcome = mapping::compact_deterministic(&detail.messages);
    if let Some(summary) = &outcome.summary
        && !state
            .db
            .update_session_summary(&detail.session_id, summary)
            .await?
    {
        return Err(ApiError::session_not_found(&detail.session_id));
    }
    Ok((
        CompactResponse {
            success: true,
            tokens_before: outcome.tokens_before,
            tokens_after: outcome.tokens_after,
        },
        outcome.summary.is_some(),
    ))
}

async fn run_owned(
    state: &AppState,
    detail: zk_db::SessionDetail,
    operation: Operation,
    cancelled: CancellationToken,
) -> Result<Response, ApiError> {
    if cancelled.is_cancelled() {
        return Err(failure(
            "HOOK_REQUEST_CANCELLED",
            "Session operation was cancelled before admission",
        ));
    }
    let session = detail.session_id.clone();
    let worker_state = state.clone();
    let worker_request = cancelled.clone();
    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
    let receipt = state
        .task_runtime
        .submit_hook_operation(
            ExternalRootSubmission {
                session_id: session.clone(),
                startup_epoch: state.startup_epoch(),
                timeout: state
                    .config
                    .root_task_budget_policy
                    .deadline
                    .min(zk_engine::DEFAULT_TASK_TIMEOUT),
                budget: zk_db::TaskBudgetLimits::default(),
            },
            move |owner| async move {
                let work =
                    execute_phase(&worker_state, &detail, operation, &worker_request, &owner).await;
                worker_state.hooks.drain_run(&owner.run_id).await;
                let terminal = match &work {
                    _ if owner.cancel.is_cancelled() => TaskExecutionResult::Cancelled {
                        message: "Session lifecycle owner stopped before completion".into(),
                    },
                    Ok(_) => {
                        TaskExecutionResult::complete("Local session lifecycle operation completed")
                    }
                    Err(error) => TaskExecutionResult::Failed {
                        code: error.code.clone(),
                        message: error.message.clone(),
                    },
                };
                let _ = outcome_tx.send(work);
                terminal
            },
        )
        .await
        .map_err(|error| failure(&error.code, "Session lifecycle admission failed"))?;
    let output = wait_for_cleanup(state, &session, &receipt, &cancelled).await?;
    if output
        .result
        .as_ref()
        .and_then(|result| result.result.error_code.as_deref())
        == Some("TIMEOUT")
    {
        return Err(failure(
            "TIMEOUT",
            "Session lifecycle exceeded its configured deadline; resources were cleaned up",
        ));
    }
    let outcome = outcome_rx.await.map_err(|_| {
        failure(
            "HOOK_OUTCOME_UNAVAILABLE",
            "Session lifecycle response is unavailable",
        )
    })??;
    if output.task.status != zk_db::TaskStatus::Succeeded {
        return Err(failure(
            "HOOK_OPERATION_FAILED",
            "Session lifecycle did not complete successfully",
        ));
    }
    if matches!(operation, Operation::Delete) {
        state.db.ensure_session_idle(&session).await?;
        crate::python::tools::close_session_browser_contexts(&state.db, &state.python, &session)
            .await
            .map_err(|code| failure(&code, "Browser session cleanup is not confirmed"))?;
        state.db.delete_session(&session).await?;
    }
    Ok(outcome)
}

async fn wait_for_cleanup(
    state: &AppState,
    session: &str,
    receipt: &zk_engine::TaskSubmissionReceipt,
    cancelled: &CancellationToken,
) -> Result<zk_engine::TaskOutputResponse, ApiError> {
    let mut cancellation_requested = false;
    let mut cancellation_error = None;
    loop {
        let output = state.task_runtime.read_output(TaskOutputRequest {
            root_session_id: session.to_owned(),
            task_id: receipt.task.id.clone(),
            wait_ms: 1000,
            result_version: None,
            cursor: 0,
            max_bytes: 1024,
        });
        let output = tokio::select! {
            biased;
            () = cancelled.cancelled(), if !cancellation_requested => {
                cancellation_requested = true;
                if let Err(error) = state.task_runtime.cancel_owned(session, &receipt.task.id, "HOOK_HTTP_CLIENT_DISCONNECTED").await {
                    cancellation_error = Some(failure(&error.code, "Cancellation persistence failed; lifecycle cleanup remains owned"));
                }
                continue;
            }
            output = output => match output {
                Ok(output) => output,
                Err(error) => {
                    // A read failure cannot relinquish a still-live owner or its Session lease.
                    // Stop physical work first, then retain this finalizer until SQLite can
                    // confirm the terminal result and resource reconciliation again.
                    cancellation_error.get_or_insert_with(|| failure(&error.code, "Session lifecycle reconciliation failed; cleanup remains owned"));
                    cancellation_requested = true;
                    cancelled.cancel();
                    if let Err(stop) = state.task_runtime.cancel_run_with_cause(
                        &receipt.run_id,
                        zk_db::run::EXIT_INTERNAL_ERROR,
                        "HOOK_RECONCILIATION_FAILED",
                    ).await {
                        tracing::warn!(code=%stop.code, "lifecycle cancellation persistence failed; retrying owned reconciliation");
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        if output.task.status == zk_db::TaskStatus::NeedsAttention {
            return Err(failure(
                "HOOK_CLEANUP_UNCONFIRMED",
                "Session lifecycle requires cleanup or terminal reconciliation",
            ));
        }
        if !output.task.status.is_terminal() {
            continue;
        }
        if !matches!(
            output.task.cleanup_status,
            zk_db::CleanupStatus::Confirmed | zk_db::CleanupStatus::NotRequired
        ) {
            return Err(failure(
                "HOOK_CLEANUP_UNCONFIRMED",
                "Session lifecycle resource cleanup is unconfirmed",
            ));
        }
        if let Some(error) = cancellation_error {
            return Err(error);
        }
        if cancelled.is_cancelled() {
            return Err(failure(
                "HOOK_REQUEST_CANCELLED",
                "Session lifecycle was cancelled after cleanup",
            ));
        }
        return Ok(output);
    }
}

async fn execute_phase(
    state: &AppState,
    detail: &zk_db::SessionDetail,
    operation: Operation,
    request_cancel: &CancellationToken,
    owner: &zk_engine::TaskExecutionContext,
) -> Result<Response, ApiError> {
    let context = state.execution_supervisor.hook_context(
        &owner.task_id,
        &owner.run_id,
        &detail.session_id,
        Path::new(&detail.working_dir),
        owner.cancel.clone(),
    );
    if request_cancel.is_cancelled() || owner.cancel.is_cancelled() {
        return Err(failure(
            "HOOK_REQUEST_CANCELLED",
            "Session operation was cancelled",
        ));
    }
    match operation {
        Operation::Delete => {
            // SESSION_END is a deletion-request gate. The Session still exists
            // here so permission history is authoritative; deletion follows cleanup.
            let context = context
                .clone()
                .with_result_preview("Session deletion requested; deletion has not committed");
            state
                .hooks
                .fire_lifecycle(HookEvent::SessionEnd, &context)
                .await
                .map_err(hook_error)?;
            Ok(Response::Deleted)
        }
        Operation::Compact => {
            state
                .hooks
                .fire_lifecycle(HookEvent::PreCompact, &context)
                .await
                .map_err(hook_error)?;
            if request_cancel.is_cancelled() || owner.cancel.is_cancelled() {
                return Err(failure(
                    "HOOK_REQUEST_CANCELLED",
                    "Session operation was cancelled",
                ));
            }
            let (result, saved) = save_compaction(state, detail).await?;
            if saved {
                state.hooks.fire(HookEvent::PostCompact, &context).await;
            }
            Ok(Response::Compacted(result))
        }
    }
}
