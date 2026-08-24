//! Engineering verification REST entry routed through Bash admission and durable Evidence.

use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zk_authz::model::PermissionMode;
use zk_authz::sensitive::SensitiveDataFilter;
use zk_db::{EvidenceBundleRecord, EvidenceItemRecord};
use zk_engine::ObservabilityEvent;
use zk_engine::admission::{Admission, AdmissionRequest, ToolAdmission};
use zk_tools::verify_journey::default_command;
use zk_tools::{
    CheckResult, CheckStatus, JourneyReport, ProjectKind, ToolContext, parse_verify_request,
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
    let report = run_admitted_checks(&state, &asserted, &request.run_id, parsed).await?;
    let evidence = evidence_for_report(
        &state,
        &session.session_id,
        &request.run_id,
        &workspace,
        request.claim,
        &report,
    )
    .await?;
    Ok(Json(json!({
        "report": report.to_json(),
        "evidence": evidence,
    })))
}

#[allow(clippy::too_many_lines)] // ordered verification, Admission, evidence telemetry boundary
async fn run_admitted_checks(
    state: &AppState,
    session_id: &str,
    run_id: &str,
    request: zk_tools::verify_journey::JourneyRequest,
) -> Result<JourneyReport, ApiError> {
    let started = Instant::now();
    let mut telemetry = ObservabilityEvent::new("verify", "run_checks", "running");
    telemetry.session_id = Some(session_id.to_owned());
    telemetry.run_id = Some(run_id.to_owned());
    state.observability.record(telemetry);
    let project = ProjectKind::detect(&request.working_dir);
    let registry = state.tools();
    let bash = registry
        .get("Bash")
        .ok_or_else(|| ApiError::feature_not_ready("verify", "Bash tool registration"))?;
    // The REST call is itself an explicit request to execute these checks. Use an isolated
    // authorization stack so AUTO_APPROVE cannot leak into a concurrent conversation in the
    // same session; command analysis, absolute-deny rules and gateway rechecks still all run.
    let verify_authz = Arc::new(AuthzStack::build(&state.db, &state.config, None));
    verify_authz
        .modes
        .set_mode(session_id, PermissionMode::AutoApprove)
        .await;
    let admission = EngineAdmission::new(verify_authz, Arc::clone(&registry));
    let mut results = Vec::with_capacity(request.plans.len());
    let mut skip_remaining = false;
    for plan in request.plans {
        if skip_remaining {
            results.push(CheckResult::skipped(plan.kind, "skipped by fail_fast"));
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
            continue;
        };
        let tool_use_id = format!("verify-{}", uuid::Uuid::new_v4());
        let timeout_ms = u64::try_from(plan.timeout.as_millis()).unwrap_or(u64::MAX);
        let input = json!({
            "command": command,
            "timeout": timeout_ms,
            "description": format!("{} verification", plan.kind.as_str()),
        });
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
                let (progress, _receiver) = mpsc::unbounded_channel();
                let context = ToolContext::new(CancellationToken::new(), progress)
                    .with_session_id(session_id)
                    .with_run_id(run_id)
                    .with_tool_use_id(&tool_use_id)
                    .with_working_dir(&request.working_dir);
                let output = bash.execute(execution_input, context).await;
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
                state
                    .db
                    .append_run_event(
                        run_id,
                        "tool_finished",
                        Some(&tool_use_id),
                        &json!({
                            "name": "Bash",
                            "ok": !output.is_error,
                            "source": "verify_run_checks",
                        }),
                    )
                    .await?;
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
    Ok(report)
}

async fn evidence_for_report(
    state: &AppState,
    session_id: &str,
    run_id: &str,
    workspace: &std::path::Path,
    claim: Option<String>,
    report: &JourneyReport,
) -> Result<EvidenceBundleRecord, ApiError> {
    let mut items = Vec::with_capacity(report.results.len());
    for (sort_order, result) in report.results.iter().enumerate() {
        let safe_output = SensitiveDataFilter::filter(&result.output);
        let blob_sha256 = if safe_output.is_empty() {
            None
        } else {
            Some(
                super::evidence::store_blob(
                    workspace.to_path_buf(),
                    safe_output.as_bytes().to_vec(),
                )
                .await?,
            )
        };
        items.push(EvidenceItemRecord {
            id: uuid::Uuid::new_v4().to_string(),
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
    let verdict = match report.status() {
        CheckStatus::Pass => "verified",
        CheckStatus::Fail => "failed",
        CheckStatus::Skip => "unavailable",
    };
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
