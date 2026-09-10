//! Durable request/result workbench and acceptance-criteria endpoints.

use std::path::Path as FsPath;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use zk_db::model::{MessageRecord, StoredBlock};
use zk_db::{
    ArtifactManifestRecord, PreviousWorkbenchDelivery, ResearchProjection, SessionSummary,
    WorkbenchRecord,
};

use crate::error::ApiError;
use crate::session_access::{accessible_run, can_access_session, require_session_header};
use crate::state::AppState;

/// Read a root-run workbench after run object authorization.
pub(crate) async fn get_workbench(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let asserted = require_session_header(&headers)?;
    let run = accessible_run(&state, &run_id, &asserted)
        .await?
        .ok_or_else(|| ApiError::not_found("RUN_NOT_FOUND", "Run not found"))?;
    let workbench = state
        .db
        .find_workbench(&run_id)
        .await?
        .ok_or_else(|| ApiError::not_found("WORKBENCH_NOT_FOUND", "Run workbench not found"))?;
    let research = state
        .db
        .find_research_projection_by_root_run(&run_id, &asserted)
        .await?
        .unwrap_or_else(|| ResearchProjection {
            root_task_id: run.task_id,
            ..ResearchProjection::default()
        });
    Ok(Json(json!({
        "binding": workbench.binding,
        "criteria": workbench.criteria,
        "research": research,
    })))
}

/// Read the latest root-run workbench for an authorized session.
pub(crate) async fn get_current_workbench(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let asserted = require_session_header(&headers)?;
    if !can_access_session(&state, &session_id, &asserted).await? {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            code: "SESSION_ACCESS_DENIED".into(),
            message: "Session access denied".into(),
        });
    }
    let projection = state
        .db
        .find_current_workbench_projection(&session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&session_id))?;
    let Some(root) = projection.root_run.as_ref() else {
        return Ok(Json(empty_projection()));
    };
    let persisted = projection.workbench.as_ref();
    let correlation = if persisted.is_some() {
        "EXACT"
    } else {
        "UNBOUND"
    };
    let request = projection.request_message.as_ref().map(message_view);
    let result = projection.result_message.as_ref().map(message_view);
    let delivery = delivery_view(&projection.manifests);
    let verification =
        verification_view(persisted, &projection.manifests, &projection.evidence, root);
    let failure = if root.is_terminal() && root.status != "completed" {
        Some(json!({
            "status": root.status,
            "reason": root.error_summary.as_deref()
                .or(root.abort_reason.as_deref())
                .or(root.waiting_reason.as_deref())
                .or(root.exit_reason.as_deref()),
        }))
    } else {
        None
    };
    let previous_delivery = projection
        .previous_delivery
        .as_ref()
        .map(previous_delivery_view);
    let request_id = projection
        .request_message
        .as_ref()
        .map(|message| message.id.as_str());
    let result_id = projection
        .result_message
        .as_ref()
        .map(|message| message.id.as_str());
    let result_text = result.as_ref().and_then(|value| value["text"].as_str());
    Ok(Json(json!({
        "correlationMode": correlation,
        "requestMessageId": request_id,
        "resultMessageId": result_id,
        "rootTask": projection.root_task,
        "taskTree": projection.task_tree,
        "rootRun": root,
        "runTree": projection.run_tree,
        "usage": projection.usage,
        "eventHighWater": projection.event_high_water,
        "activeTools": projection.active_tools,
        "request": request,
        "result": result,
        "structuredSummary": structured_summary(result_text),
        "delivery": delivery,
        "verification": verification,
        "pendingActionCount": projection.pending_actions.len(),
        "pendingActions": projection.pending_actions,
        "activities": projection.activities,
        "research": projection.research,
        "previousAvailableDelivery": previous_delivery,
        "currentFailure": failure,
    })))
}

fn previous_delivery_view(previous: &PreviousWorkbenchDelivery) -> Value {
    json!({
        "rootRunId": previous.root_run.id,
        "finishedAt": previous.root_run.finished_at,
        "result": previous.result_message.as_ref().map(message_view),
        "delivery": delivery_view(&previous.manifests),
    })
}

fn empty_projection() -> Value {
    json!({
        "correlationMode": "EMPTY",
        "requestMessageId": null,
        "resultMessageId": null,
        "rootTask": null,
        "taskTree": [],
        "rootRun": null,
        "runTree": [],
        "usage": {
            "inputTokens": 0, "outputTokens": 0, "cacheReadTokens": 0,
            "cacheCreateTokens": 0, "costNanosUsd": 0, "complete": true
        },
        "eventHighWater": 0,
        "activeTools": [],
        "request": null,
        "result": null,
        "structuredSummary": structured_summary(None),
        "delivery": {"manifests": [], "files": [], "totalFiles": 0, "primaryArtifactPath": null},
        "verification": {
            "businessCriteria": [], "technicalChecks": [], "evidence": [],
            "overallStatus": "NOT_VERIFIED"
        },
        "pendingActionCount": 0,
        "pendingActions": [],
        "activities": [],
        "research": {
            "rootTaskId": "", "truncated": false, "captures": [], "sources": [], "findings": [],
            "conflicts": [], "openQuestions": [], "requirementCoverage": []
        },
        "previousAvailableDelivery": null,
        "currentFailure": null,
    })
}

fn message_view(message: &MessageRecord) -> Value {
    let text = message
        .content
        .iter()
        .filter_map(|block| match block {
            StoredBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    json!({
        "messageId": message.id,
        "text": text,
        "timestamp": crate::iso::format_rfc3339_micros(message.created_at),
    })
}

fn structured_summary(result: Option<&str>) -> Value {
    json!({
        "conclusion": result.filter(|text| !text.trim().is_empty()),
        "completed": [],
        "issues": [],
        "nextSteps": [],
    })
}

fn delivery_view(manifests: &[ArtifactManifestRecord]) -> Value {
    let mut files = Vec::new();
    for manifest in manifests {
        for entry in &manifest.entries {
            if entry.operation == "deleted" {
                continue;
            }
            let relative = std::path::Path::new(&entry.canonical_path)
                .strip_prefix(&manifest.workspace_root)
                .unwrap_or_else(|_| std::path::Path::new(&entry.canonical_path))
                .to_string_lossy()
                .into_owned();
            files.push(json!({
                "manifestId": manifest.manifest_id,
                "workspaceRoot": manifest.workspace_root,
                "id": entry.artifact_id,
                "filePath": entry.canonical_path,
                "relativePath": relative,
                "operation": entry.operation,
                "state": entry.state,
                "fileSize": entry.file_size,
                "verified": entry.state == "integrity_verified",
                "mismatchDetail": entry.failure_code,
                "primary": false,
            }));
        }
    }
    files.sort_by(|left, right| left["filePath"].as_str().cmp(&right["filePath"].as_str()));
    if let Some(first) = files.first_mut() {
        first["primary"] = Value::Bool(true);
    }
    let primary = files.first().and_then(|file| file["filePath"].as_str());
    let manifest_views = manifests
        .iter()
        .map(|manifest| json!({
            "id": manifest.manifest_id,
            "runId": manifest.run_id,
            "sessionId": manifest.session_id,
            "workspaceRoot": manifest.workspace_root,
            "status": manifest.state,
            "createdAt": manifest.created_at,
            "updatedAt": manifest.updated_at,
            "totalFiles": manifest.entries.len(),
            "verifiedFiles": manifest.entries.iter().filter(|entry| entry.state == "integrity_verified").count(),
            "failedFiles": manifest.entries.iter().filter(|entry| entry.state == "failed").count(),
            "entries": manifest.entries.iter().map(|entry| json!({
                "id": entry.artifact_id,
                "filePath": entry.canonical_path,
                "operation": entry.operation,
                "state": entry.state,
                "fileSize": entry.file_size,
                "verified": entry.state == "integrity_verified",
                "mismatchDetail": entry.failure_code,
            })).collect::<Vec<_>>(),
        }))
        .collect::<Vec<_>>();
    json!({
        "manifests": manifest_views,
        "totalFiles": files.len(),
        "primaryArtifactPath": primary,
        "files": files,
    })
}

fn verification_view(
    workbench: Option<&WorkbenchRecord>,
    manifests: &[ArtifactManifestRecord],
    evidence: &[zk_db::EvidenceBundleRecord],
    root: &zk_db::run::RunEnvelopeView,
) -> Value {
    let business = workbench.map_or_else(Vec::new, |workbench| {
        workbench.criteria.iter().map(|criterion| json!({
            "id": criterion.criterion_id,
            "type": criterion.criterion_type,
            "text": criterion.source_text,
            "status": criterion.status.to_uppercase(),
            "detail": if criterion.evidence_bundle_id.is_some() { "已绑定确定性证据" } else { "尚无明确关联的确定性证据" },
            "evidenceBundleId": criterion.evidence_bundle_id,
        })).collect::<Vec<_>>()
    });
    let manifest_status = if manifests.is_empty() {
        "NOT_VERIFIED"
    } else if manifests.iter().any(|manifest| manifest.state == "failed") {
        "FAILED"
    } else if manifests
        .iter()
        .any(|manifest| manifest.state == "unverified")
    {
        "STALE"
    } else if manifests
        .iter()
        .all(|manifest| manifest.state == "verified")
    {
        "PASSED"
    } else {
        "PARTIAL"
    };
    let verifying_evidence = evidence
        .iter()
        .filter(|bundle| bundle.origin.can_verify())
        .collect::<Vec<_>>();
    let runtime_status = if verifying_evidence.is_empty() {
        "NOT_VERIFIED"
    } else if verifying_evidence
        .iter()
        .any(|bundle| bundle.verdict == "failed")
    {
        "FAILED"
    } else if verifying_evidence
        .iter()
        .any(|bundle| bundle.verdict == "stale")
    {
        "STALE"
    } else if verifying_evidence
        .iter()
        .all(|bundle| bundle.verdict == "verified")
    {
        "PASSED"
    } else {
        "PARTIAL"
    };
    let technical = vec![
        json!({"id":"technical-manifest-integrity","type":"technical","text":"交付文件与Manifest一致","status":manifest_status,"detail":"只统计当前 Root Run 子树的 Manifest","evidenceBundleId":null}),
        json!({"id":"technical-runtime-verification","type":"technical","text":"页面或程序完成运行时检查","status":runtime_status,"detail":"仅使用明确绑定到当前 Run 树的证据","evidenceBundleId":null}),
        json!({"id":"technical-no-failure-evidence","type":"technical","text":"本轮交付没有明确失败结论","status":if root.is_terminal() && root.status != "completed" {"FAILED"} else if root.is_terminal() {"PASSED"} else {"NOT_VERIFIED"},"detail":root.error_summary,"evidenceBundleId":null}),
    ];
    let statuses = business
        .iter()
        .chain(&technical)
        .filter_map(|criterion| criterion["status"].as_str())
        .collect::<Vec<_>>();
    let overall = if statuses.contains(&"FAILED") {
        "FAILED"
    } else if statuses.contains(&"STALE") {
        "STALE"
    } else if statuses.contains(&"PARTIAL") {
        "PARTIAL"
    } else if !statuses.is_empty() && statuses.iter().all(|status| *status == "PASSED") {
        "PASSED"
    } else {
        "NOT_VERIFIED"
    };
    json!({
        "businessCriteria": business,
        "technicalChecks": technical,
        "evidence": evidence,
        "overallStatus": overall,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UpdateWorkbenchRequest {
    criteria: Vec<CriterionDecision>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CriterionDecision {
    criterion_id: String,
    status: String,
    evidence_bundle_id: String,
}

/// Bind explicit evidence-backed acceptance decisions.
pub(crate) async fn update_workbench(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(request): Json<UpdateWorkbenchRequest>,
) -> Result<Json<WorkbenchRecord>, ApiError> {
    let asserted = require_session_header(&headers)?;
    let run = accessible_run(&state, &run_id, &asserted)
        .await?
        .ok_or_else(|| ApiError::not_found("RUN_NOT_FOUND", "Run not found"))?;
    let workbench = state
        .db
        .find_workbench(&run_id)
        .await?
        .ok_or_else(|| ApiError::not_found("WORKBENCH_NOT_FOUND", "Run workbench not found"))?;
    let run_tree = state.db.find_run_tree(&run_id).await?;
    for decision in request.criteria {
        if !matches!(
            decision.status.as_str(),
            "passed" | "failed" | "partial" | "not_verified"
        ) {
            return Err(ApiError::validation_with_code(
                "ACCEPTANCE_STATUS_INVALID",
                "Acceptance status is invalid",
            ));
        }
        if !workbench
            .criteria
            .iter()
            .any(|criterion| criterion.criterion_id == decision.criterion_id)
        {
            return Err(ApiError::not_found(
                "ACCEPTANCE_CRITERION_NOT_FOUND",
                "Acceptance criterion not found",
            ));
        }
        let evidence = state
            .db
            .find_evidence_bundle(&decision.evidence_bundle_id)
            .await?
            .ok_or_else(|| {
                ApiError::not_found("EVIDENCE_NOT_FOUND", "Evidence bundle not found")
            })?;
        if evidence.session_id != run.session_id {
            return Err(ApiError {
                status: StatusCode::FORBIDDEN,
                code: "EVIDENCE_ACCESS_DENIED".into(),
                message: "Evidence does not belong to this session".into(),
            });
        }
        if !evidence.run_id.as_deref().is_some_and(|evidence_run_id| {
            run_tree
                .iter()
                .any(|tree_run| tree_run.id == evidence_run_id)
        }) {
            return Err(ApiError::validation_with_code(
                "EVIDENCE_RUN_MISMATCH",
                "Evidence must be produced by the current root run or one of its children",
            ));
        }
        let verdict_matches = match decision.status.as_str() {
            "passed" => evidence.origin.can_verify() && evidence.verdict == "verified",
            "failed" => evidence.origin.can_verify() && evidence.verdict == "failed",
            "partial" => {
                evidence.origin.can_verify()
                    && matches!(evidence.verdict.as_str(), "inconclusive" | "unavailable")
            }
            "not_verified" => true,
            _ => false,
        };
        if !verdict_matches {
            return Err(ApiError::validation_with_code(
                "EVIDENCE_VERDICT_MISMATCH",
                "Acceptance status must match deterministic machine or human evidence",
            ));
        }
        state
            .db
            .bind_criterion_evidence(
                &decision.criterion_id,
                &decision.evidence_bundle_id,
                &decision.status,
            )
            .await?;
    }
    Ok(Json(
        state
            .db
            .find_workbench(&run_id)
            .await?
            .expect("workbench exists after criteria update"),
    ))
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct TaskSearchQuery {
    query: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum WorkbenchTaskGroup {
    ActionRequired,
    Running,
    Reviewable,
    Other,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkbenchTaskItem {
    session_id: String,
    title: String,
    folder_name: String,
    status: WorkbenchTaskGroup,
    updated_at: String,
    pending_count: usize,
    hint: String,
}

#[derive(Debug, Serialize)]
struct WorkbenchTaskGroupView {
    status: WorkbenchTaskGroup,
    label: &'static str,
    tasks: Vec<WorkbenchTaskItem>,
}

#[derive(Debug, Serialize)]
pub(crate) struct WorkbenchTaskListView {
    groups: Vec<WorkbenchTaskGroupView>,
}

/// Search and group the global durable Session task history.
///
/// This endpoint intentionally has no `X-Session-Id`: it is the navigation
/// source used before a session is selected. Object-level authorization still
/// applies when the user opens one of the returned sessions.
pub(crate) async fn search_tasks(
    State(state): State<AppState>,
    Query(query): Query<TaskSearchQuery>,
) -> Result<Json<WorkbenchTaskListView>, ApiError> {
    let needle = query
        .query
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_lowercase();
    let sessions = state.db.list_sessions(None, 500).await?.sessions;
    let mut action_required = Vec::new();
    let mut running = Vec::new();
    let mut reviewable = Vec::new();
    let mut other = Vec::new();

    for session in sessions {
        let title = task_title(&session);
        let folder = task_folder(&session.working_directory);
        let haystack = format!(
            "{title} {folder} {}",
            session.goal_preview.as_deref().unwrap_or_default()
        )
        .to_lowercase();
        if !needle.is_empty() && !haystack.contains(&needle) {
            continue;
        }

        let pending_count = state
            .authz
            .interactions
            .pending(&session.id)
            .await
            .map_err(|error| {
                tracing::error!(session_id = %session.id, %error, "task-list pending query failed");
                ApiError::internal()
            })?
            .len();
        let run = state
            .db
            .find_latest_root_run_by_session(&session.id)
            .await?;
        let group = task_group(&state, &session.id, run.as_ref(), pending_count).await?;
        let item = WorkbenchTaskItem {
            session_id: session.id,
            title,
            folder_name: folder,
            status: group,
            updated_at: zk_db::time::format_rfc3339_micros(session.updated_at),
            pending_count,
            hint: task_hint(group, pending_count),
        };
        match group {
            WorkbenchTaskGroup::ActionRequired => action_required.push(item),
            WorkbenchTaskGroup::Running => running.push(item),
            WorkbenchTaskGroup::Reviewable => reviewable.push(item),
            WorkbenchTaskGroup::Other => other.push(item),
        }
    }

    Ok(Json(WorkbenchTaskListView {
        groups: vec![
            WorkbenchTaskGroupView {
                status: WorkbenchTaskGroup::ActionRequired,
                label: "待我处理",
                tasks: action_required,
            },
            WorkbenchTaskGroupView {
                status: WorkbenchTaskGroup::Running,
                label: "进行中",
                tasks: running,
            },
            WorkbenchTaskGroupView {
                status: WorkbenchTaskGroup::Reviewable,
                label: "可查看结果",
                tasks: reviewable,
            },
            WorkbenchTaskGroupView {
                status: WorkbenchTaskGroup::Other,
                label: "其他任务",
                tasks: other,
            },
        ],
    }))
}

async fn task_group(
    state: &AppState,
    session_id: &str,
    run: Option<&zk_db::RunEnvelopeView>,
    pending_count: usize,
) -> Result<WorkbenchTaskGroup, ApiError> {
    if pending_count > 0 {
        return Ok(WorkbenchTaskGroup::ActionRequired);
    }
    let Some(run) = run else {
        return Ok(WorkbenchTaskGroup::Other);
    };
    if !run.is_terminal() {
        return Ok(WorkbenchTaskGroup::Running);
    }
    if run.status != "completed" {
        return Ok(WorkbenchTaskGroup::Reviewable);
    }
    let upper_bound = run.finished_at.as_deref().map_or_else(
        || zk_db::time::format_rfc3339_micros(zk_db::time::now_millis()),
        |finished| {
            let millis = zk_db::time::parse_rfc3339_millis(finished)
                .unwrap_or_else(zk_db::time::now_millis)
                .saturating_add(2_000);
            zk_db::time::format_rfc3339_micros(millis)
        },
    );
    if state
        .db
        .has_reviewable_run_result(session_id, &run.id, &run.started_at, &upper_bound)
        .await?
    {
        Ok(WorkbenchTaskGroup::Reviewable)
    } else {
        Ok(WorkbenchTaskGroup::Other)
    }
}

fn task_hint(group: WorkbenchTaskGroup, pending_count: usize) -> String {
    match group {
        WorkbenchTaskGroup::ActionRequired => format!("{pending_count} 项需要处理"),
        WorkbenchTaskGroup::Running => "正在执行".to_owned(),
        WorkbenchTaskGroup::Reviewable => "结果可查看".to_owned(),
        WorkbenchTaskGroup::Other => "尚未开始执行".to_owned(),
    }
}

fn task_title(session: &SessionSummary) -> String {
    if let Some(title) = session
        .title
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        return title.clone();
    }
    if let Some(goal) = session
        .goal_preview
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        return goal.clone();
    }
    let folder = task_folder(&session.working_directory);
    if folder.trim().is_empty() {
        "未命名任务".to_owned()
    } else {
        format!("在 {folder} 中的新任务")
    }
}

fn task_folder(working_directory: &str) -> String {
    if working_directory.trim().is_empty() {
        return "未选择文件夹".to_owned();
    }
    FsPath::new(working_directory).file_name().map_or_else(
        || working_directory.to_owned(),
        |name| name.to_string_lossy().into_owned(),
    )
}
