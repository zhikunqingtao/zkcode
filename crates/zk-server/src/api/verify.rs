//! Engineering verification REST entry routed through Bash admission and durable Evidence.

use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use zk_authz::model::PermissionMode;
use zk_authz::sensitive::SensitiveDataFilter;
use zk_db::{
    CasOutcome, CleanupStatus, EvidenceBundleRecord, EvidenceItemRecord, EvidenceOrigin,
    NewToolInvocation, ToolInvocationRecord, ToolInvocationStatus,
};
use zk_engine::ObservabilityEvent;
use zk_engine::admission::{Admission, AdmissionRequest, ToolAdmission};
use zk_tools::verify_journey::default_command;
use zk_tools::{
    CallEnv, CheckResult, CheckStatus, ExecutionResourceOwner, JourneyReport, ProjectKind,
    ToolCleanupStatus, ToolEvent, parse_verify_request,
};

use crate::authz::{AuthzStack, EngineAdmission};
use crate::error::ApiError;
use crate::session_access::{accessible_run, require_session_header};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RunChecksRequest {
    run_id: String,
    checks: Vec<Value>,
    #[serde(default = "default_fail_fast")]
    fail_fast: bool,
    claim: Option<String>,
    working_directory: Option<String>,
}

fn default_fail_fast() -> bool {
    true
}

struct AdmittedChecks {
    report: JourneyReport,
    invocation_ids: Vec<Option<String>>,
    blob_sha256: Vec<Option<String>>,
}

/// Execute bounded engineering checks through the production Bash authorization pipeline.
pub(crate) async fn run_checks(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RunChecksRequest>,
) -> Result<Json<Value>, ApiError> {
    if request.working_directory.is_some() {
        return Err(ApiError::validation_with_code(
            "VERIFY_WORKING_DIRECTORY_FORBIDDEN",
            "workingDirectory is derived from the authorized session",
        ));
    }
    let asserted = require_session_header(&headers)?;
    let run = accessible_run(&state, &request.run_id, &asserted)
        .await?
        .ok_or_else(|| ApiError::not_found("RUN_NOT_FOUND", "Run not found"))?;
    let session = state
        .db
        .get_session(&run.session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&run.session_id))?;
    let workspace = std::fs::canonicalize(&session.working_dir).map_err(|_| {
        ApiError::validation_with_code("WORKSPACE_UNAVAILABLE", "Workspace is unavailable")
    })?;
    let normalized = json!({
        "checks": request.checks,
        "fail_fast": request.fail_fast,
    });
    let parsed = parse_verify_request(&normalized, &workspace)
        .map_err(|error| ApiError::validation_with_code(&error.code, &error.message))?;
    let executed =
        run_admitted_checks(&state, &asserted, &run.task_id, &request.run_id, parsed).await?;
    let evidence = evidence_for_report(
        &state,
        &session.session_id,
        &request.run_id,
        &workspace,
        request.claim,
        &executed,
    )
    .await?;
    Ok(Json(json!({
        "report": executed.report.to_json(),
        "evidence": evidence,
    })))
}

#[allow(clippy::too_many_lines)] // ordered verification, Admission, evidence telemetry boundary
async fn run_admitted_checks(
    state: &AppState,
    session_id: &str,
    task_id: &str,
    run_id: &str,
    request: zk_tools::verify_journey::JourneyRequest,
) -> Result<AdmittedChecks, ApiError> {
    let started = Instant::now();
    let mut telemetry = ObservabilityEvent::new("verify", "run_checks", "running");
    telemetry.session_id = Some(session_id.to_owned());
    telemetry.run_id = Some(run_id.to_owned());
    state.observability.record(telemetry);
    let project = ProjectKind::detect(&request.working_dir);
    let registry = state.tools();
    // The REST call is itself an explicit request to execute these checks. Use an isolated
    // authorization stack so AUTO_APPROVE cannot leak into a concurrent conversation in the
    // same session; command analysis, absolute-deny rules and gateway rechecks still all run.
    let verify_authz = Arc::new(AuthzStack::build(
        &state.db,
        &state.config,
        None,
        &state.task_runtime,
    ));
    verify_authz
        .modes
        .set_mode(session_id, PermissionMode::AutoApprove)
        .await;
    let admission = EngineAdmission::new(verify_authz, Arc::clone(&registry));
    let mut results = Vec::with_capacity(request.plans.len());
    let mut invocation_ids = Vec::with_capacity(request.plans.len());
    let mut blob_sha256 = Vec::with_capacity(request.plans.len());
    let mut skip_remaining = false;
    for plan in request.plans {
        if skip_remaining {
            results.push(CheckResult::skipped(plan.kind, "skipped by fail_fast"));
            invocation_ids.push(None);
            blob_sha256.push(None);
            continue;
        }
        let Some(command) = plan
            .command
            .clone()
            .or_else(|| default_command(plan.kind, project).map(str::to_owned))
        else {
            results.push(CheckResult::skipped(
                plan.kind,
                format!(
                    "no default command for kind '{}' on '{}' project",
                    plan.kind.as_str(),
                    project.as_str()
                ),
            ));
            invocation_ids.push(None);
            blob_sha256.push(None);
            continue;
        };
        let tool_use_id = format!("verify-{}", uuid::Uuid::new_v4());
        let timeout_ms = u64::try_from(plan.timeout.as_millis()).unwrap_or(u64::MAX);
        let input = json!({
            "command": command,
            "timeout": timeout_ms,
            "description": format!("{} verification", plan.kind.as_str()),
        });
        let binding = registry
            .resolve("Bash")
            .ok_or_else(|| ApiError::feature_not_ready("verify", "Bash tool registration"))?;
        let bash = binding.tool();
        let input_json = serde_json::to_string(&input).map_err(|error| {
            tracing::error!(%error, "failed to serialize verification invocation input");
            ApiError::internal()
        })?;
        let invocation_id = uuid::Uuid::new_v4().to_string();
        let mut invocation = state
            .db
            .create_tool_invocation(&NewToolInvocation {
                invocation_id: invocation_id.clone(),
                task_id: task_id.to_owned(),
                run_id: run_id.to_owned(),
                tool_use_id: tool_use_id.clone(),
                tool_name: "Bash".to_owned(),
                input_json: Some(input_json),
                side_effect_class: if bash.is_read_only(&input) {
                    "read"
                } else {
                    "write"
                }
                .to_owned(),
                directory_generation: Some(
                    i64::try_from(binding.directory_generation()).unwrap_or(i64::MAX),
                ),
                connection_generation: binding
                    .connection_generation()
                    .map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
            })
            .await?;
        let step_started = Instant::now();
        let admitted = admission
            .admit(AdmissionRequest {
                session_id,
                run_id,
                tool_use_id: &tool_use_id,
                tool_name: "Bash",
                input: &input,
                working_directory: request.working_dir.to_str(),
            })
            .await;
        let result = match admitted {
            Admission::Allow { execution_input } => {
                if !registry.is_binding_current(&binding) {
                    transition_invocation(
                        state,
                        &mut invocation,
                        ToolInvocationStatus::Failed,
                        &execution_input,
                        None,
                        Some("TOOL_CAPABILITY_REVOKED"),
                        CleanupStatus::NotRequired,
                    )
                    .await?;
                    invocation_ids.push(None);
                    blob_sha256.push(None);
                    let result = CheckResult {
                        kind: plan.kind,
                        status: CheckStatus::Fail,
                        command: Some(command),
                        output:
                            "TOOL_CAPABILITY_REVOKED: Bash registration changed before execution"
                                .to_owned(),
                        exit_code: None,
                        duration_ms: elapsed_millis(step_started),
                        timed_out: false,
                        truncated: false,
                        skip_reason: None,
                    };
                    if request.fail_fast {
                        skip_remaining = true;
                    }
                    results.push(result);
                    continue;
                }
                transition_invocation(
                    state,
                    &mut invocation,
                    ToolInvocationStatus::Running,
                    &execution_input,
                    None,
                    None,
                    CleanupStatus::Pending,
                )
                .await?;
                // The durable Running write is an await boundary. A directory
                // generation revoked there must fail closed before process spawn.
                if !registry.is_binding_current(&binding) {
                    transition_invocation(
                        state,
                        &mut invocation,
                        ToolInvocationStatus::Failed,
                        &execution_input,
                        None,
                        Some("TOOL_CAPABILITY_REVOKED"),
                        CleanupStatus::NotRequired,
                    )
                    .await?;
                    invocation_ids.push(None);
                    blob_sha256.push(None);
                    let result = CheckResult {
                        kind: plan.kind,
                        status: CheckStatus::Fail,
                        command: Some(command),
                        output:
                            "TOOL_CAPABILITY_REVOKED: Bash registration changed before execution"
                                .to_owned(),
                        exit_code: None,
                        duration_ms: elapsed_millis(step_started),
                        timed_out: false,
                        truncated: false,
                        skip_reason: None,
                    };
                    if request.fail_fast {
                        skip_remaining = true;
                    }
                    results.push(result);
                    continue;
                }
                let cancel = CancellationToken::new();
                let env = CallEnv::new()
                    .with_session_id(session_id)
                    .with_run_id(run_id)
                    .with_working_dir(&request.working_dir)
                    .with_tool_catalog(registry.specs());
                let owner = ExecutionResourceOwner {
                    task_id: task_id.to_owned(),
                    run_id: run_id.to_owned(),
                    invocation_id: invocation_id.clone(),
                };
                let mut events = state.execution_supervisor.spawn_call_in(
                    bash,
                    tool_use_id.clone(),
                    execution_input.clone(),
                    &cancel,
                    env,
                    owner,
                );
                let finished = loop {
                    match events.recv().await {
                        Some(ToolEvent::Progress { .. }) => {}
                        Some(ToolEvent::Finished {
                            output,
                            cleanup_status,
                            ..
                        }) => break Some((output, cleanup_status)),
                        None => break None,
                    }
                };
                let Some((output, cleanup_status)) = finished else {
                    transition_invocation(
                        state,
                        &mut invocation,
                        ToolInvocationStatus::Interrupted,
                        &execution_input,
                        None,
                        Some("TOOL_EXECUTION_INTERRUPTED"),
                        CleanupStatus::Unconfirmed,
                    )
                    .await?;
                    state
                        .db
                        .append_run_event(
                            run_id,
                            "tool_finished",
                            Some(&tool_use_id),
                            &json!({
                                "name": "Bash",
                                "ok": false,
                                "invocationId": invocation_id,
                                "cleanupStatus": "unconfirmed",
                                "source": "verify_run_checks",
                            }),
                        )
                        .await?;
                    invocation_ids.push(None);
                    blob_sha256.push(None);
                    let result = CheckResult {
                        kind: plan.kind,
                        status: CheckStatus::Fail,
                        command: Some(command),
                        output: "TOOL_EXECUTION_INTERRUPTED".to_owned(),
                        exit_code: None,
                        duration_ms: elapsed_millis(step_started),
                        timed_out: false,
                        truncated: false,
                        skip_reason: None,
                    };
                    if request.fail_fast {
                        skip_remaining = true;
                    }
                    results.push(result);
                    continue;
                };
                let structured = output
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.get("structuredResult"));
                let exit_code = structured
                    .and_then(|value| value.get("exitCode"))
                    .and_then(Value::as_i64)
                    .and_then(|value| i32::try_from(value).ok());
                let timed_out = structured
                    .and_then(|value| value.get("timedOut"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let truncated = structured
                    .and_then(|value| value.get("truncated"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let observation_completed = structured
                    .and_then(|value| value.get("exitCode"))
                    .and_then(Value::as_i64)
                    .is_some()
                    && !structured
                        .and_then(|value| value.get("cancelled"))
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                let safe_output = SensitiveDataFilter::filter(&output.content);
                let output_blob = if safe_output.is_empty() {
                    None
                } else {
                    match super::evidence::store_blob(
                        request.working_dir.clone(),
                        safe_output.as_bytes().to_vec(),
                    )
                    .await
                    {
                        Ok(sha256) => Some(sha256),
                        Err(error) => {
                            transition_invocation(
                                state,
                                &mut invocation,
                                ToolInvocationStatus::Failed,
                                &execution_input,
                                None,
                                Some("VERIFY_OUTPUT_PERSIST_FAILED"),
                                durable_cleanup_status(cleanup_status),
                            )
                            .await?;
                            return Err(error);
                        }
                    }
                };
                let output_ref = output_blob
                    .as_ref()
                    .map(|sha256| format!("evidenceBlob:{sha256}"));
                let terminal = if observation_completed {
                    ToolInvocationStatus::Succeeded
                } else {
                    ToolInvocationStatus::Failed
                };
                let error_code =
                    (!observation_completed).then_some("VERIFY_OBSERVATION_INCOMPLETE");
                transition_invocation(
                    state,
                    &mut invocation,
                    terminal,
                    &execution_input,
                    output_ref.as_deref(),
                    error_code,
                    durable_cleanup_status(cleanup_status),
                )
                .await?;
                state
                    .db
                    .append_run_event(
                        run_id,
                        "tool_finished",
                        Some(&tool_use_id),
                        &json!({
                            "name": "Bash",
                            "ok": !output.is_error,
                            "invocationId": invocation_id,
                            "cleanupStatus": durable_cleanup_status(cleanup_status).as_db(),
                            "source": "verify_run_checks",
                        }),
                    )
                    .await?;
                invocation_ids.push(observation_completed.then_some(invocation_id));
                blob_sha256.push(output_blob);
                CheckResult {
                    kind: plan.kind,
                    status: if output.is_error {
                        CheckStatus::Fail
                    } else {
                        CheckStatus::Pass
                    },
                    command: Some(command),
                    output: output.content,
                    exit_code,
                    duration_ms: elapsed_millis(step_started),
                    timed_out,
                    truncated,
                    skip_reason: None,
                }
            }
            Admission::Denied { code, message } | Admission::Failed { code, message } => {
                transition_invocation(
                    state,
                    &mut invocation,
                    ToolInvocationStatus::Failed,
                    &input,
                    None,
                    Some(&code),
                    CleanupStatus::NotRequired,
                )
                .await?;
                invocation_ids.push(None);
                blob_sha256.push(None);
                CheckResult {
                    kind: plan.kind,
                    status: CheckStatus::Fail,
                    command: Some(command),
                    output: format!("{code}: {message}"),
                    exit_code: None,
                    duration_ms: elapsed_millis(step_started),
                    timed_out: false,
                    truncated: false,
                    skip_reason: None,
                }
            }
        };
        if result.status == CheckStatus::Fail && request.fail_fast {
            skip_remaining = true;
        }
        results.push(result);
    }
    let report = JourneyReport {
        working_dir: request.working_dir.to_string_lossy().into_owned(),
        project,
        fail_fast: request.fail_fast,
        results,
        total_duration_ms: elapsed_millis(started),
    };
    let outcome = if report
        .results
        .iter()
        .any(|result| result.status == CheckStatus::Fail)
    {
        "error"
    } else {
        "ok"
    };
    let mut telemetry = ObservabilityEvent::new("verify", "run_checks", outcome);
    telemetry.session_id = Some(session_id.to_owned());
    telemetry.run_id = Some(run_id.to_owned());
    telemetry.duration_ms = Some(report.total_duration_ms);
    telemetry
        .attributes
        .insert("checkCount".to_owned(), json!(report.results.len()));
    state.observability.record(telemetry);
    Ok(AdmittedChecks {
        report,
        invocation_ids,
        blob_sha256,
    })
}

#[allow(clippy::too_many_arguments)]
async fn transition_invocation(
    state: &AppState,
    invocation: &mut ToolInvocationRecord,
    target: ToolInvocationStatus,
    input: &Value,
    output_ref: Option<&str>,
    error_code: Option<&str>,
    cleanup_status: CleanupStatus,
) -> Result<(), ApiError> {
    let input_json = serde_json::to_string(input).map_err(|error| {
        tracing::error!(%error, "failed to serialize admitted verification input");
        ApiError::internal()
    })?;
    let outcome = state
        .db
        .transition_tool_invocation_cas(
            &invocation.invocation_id,
            invocation.version,
            target,
            Some(&input_json),
            output_ref,
            error_code,
            cleanup_status,
        )
        .await?;
    if outcome != CasOutcome::Applied {
        tracing::error!(
            invocation_id = %invocation.invocation_id,
            ?target,
            ?outcome,
            "verification invocation transition was not applied"
        );
        return Err(ApiError::internal());
    }
    invocation.version = invocation.version.saturating_add(1);
    Ok(())
}

const fn durable_cleanup_status(status: ToolCleanupStatus) -> CleanupStatus {
    match status {
        ToolCleanupStatus::NotRequired => CleanupStatus::NotRequired,
        ToolCleanupStatus::Pending => CleanupStatus::Pending,
        ToolCleanupStatus::Confirmed => CleanupStatus::Confirmed,
        ToolCleanupStatus::Unconfirmed => CleanupStatus::Unconfirmed,
    }
}

async fn evidence_for_report(
    state: &AppState,
    session_id: &str,
    run_id: &str,
    workspace: &std::path::Path,
    claim: Option<String>,
    executed: &AdmittedChecks,
) -> Result<EvidenceBundleRecord, ApiError> {
    let report = &executed.report;
    let verdict = match report.status() {
        CheckStatus::Pass => "verified",
        CheckStatus::Fail => "failed",
        CheckStatus::Skip => "unavailable",
    };
    let terminal_machine_verdict = matches!(verdict, "verified" | "failed");
    let mut items = Vec::with_capacity(report.results.len());
    for (sort_order, result) in report.results.iter().enumerate() {
        let producer_invocation_id = executed.invocation_ids.get(sort_order).cloned().flatten();
        // Skipped checks are useful in the report, but are not machine
        // observations supporting a terminal bundle verdict.
        if terminal_machine_verdict
            && result.status == CheckStatus::Skip
            && producer_invocation_id.is_none()
        {
            continue;
        }
        if terminal_machine_verdict && producer_invocation_id.is_none() {
            tracing::error!(
                run_id,
                sort_order,
                "engineering evidence item has no succeeded invocation"
            );
            return Err(ApiError::internal());
        }
        let safe_output = SensitiveDataFilter::filter(&result.output);
        let blob_sha256 = match executed.blob_sha256.get(sort_order).cloned().flatten() {
            Some(sha256) => Some(sha256),
            None if safe_output.is_empty() => None,
            None => Some(
                super::evidence::store_blob(
                    workspace.to_path_buf(),
                    safe_output.as_bytes().to_vec(),
                )
                .await?,
            ),
        };
        items.push(EvidenceItemRecord {
            id: uuid::Uuid::new_v4().to_string(),
            producer_invocation_id,
            item_type: "engineering_check".into(),
            summary: Some(format!(
                "{}: {}",
                result.kind.as_str(),
                result.status.as_str()
            )),
            blob_sha256,
            meta: Some(result.to_json()),
            sort_order: i64::try_from(sort_order).unwrap_or(i64::MAX),
        });
    }
    // The first completed check is the bundle commit witness. Every item keeps
    // its own typed producer as well, so multi-check provenance is never
    // inferred from caller-controlled metadata.
    let producer_invocation_id = executed.invocation_ids.iter().flatten().next().cloned();
    if terminal_machine_verdict && producer_invocation_id.is_none() {
        tracing::error!(
            run_id,
            "engineering evidence bundle has no succeeded invocation"
        );
        return Err(ApiError::internal());
    }
    let bundle = EvidenceBundleRecord {
        bundle_id: uuid::Uuid::new_v4().to_string(),
        session_id: session_id.to_owned(),
        agent_id: None,
        kind: "engineering_verification".into(),
        claim: Some(SensitiveDataFilter::filter(
            claim
                .as_deref()
                .unwrap_or("Engineering verification checks"),
        )),
        origin: EvidenceOrigin::Machine,
        producer_invocation_id,
        verdict: verdict.into(),
        created_at: crate::iso::format_rfc3339_micros(crate::iso::now_millis()),
        run_id: Some(run_id.to_owned()),
        items,
    };
    state.db.save_evidence_bundle(&bundle).await?;
    Ok(bundle)
}

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
