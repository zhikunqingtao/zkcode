//! Durable request/result workbench and acceptance-criteria endpoints.

use std::path::Path as FsPath;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use zk_db::model::{MessageRecord, MessageRole, StoredBlock};
use zk_db::{ArtifactManifestRecord, SessionSummary, WorkbenchRecord};

use crate::error::ApiError;
use crate::session_access::{accessible_run, can_access_session, require_session_header};
use crate::state::AppState;

/// Read a root-run workbench after run object authorization.
pub(crate) async fn get_workbench(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<WorkbenchRecord>, ApiError> {
    let asserted = require_session_header(&headers)?;
    accessible_run(&state, &run_id, &asserted)
        .await?
        .ok_or_else(|| ApiError::not_found("RUN_NOT_FOUND", "Run not found"))?;
    let workbench = state
        .db
        .find_workbench(&run_id)
        .await?
        .ok_or_else(|| ApiError::not_found("WORKBENCH_NOT_FOUND", "Run workbench not found"))?;
    Ok(Json(workbench))
}

/// Read the latest root-run workbench for an authorized session.
#[allow(clippy::too_many_lines)] // one correlation projection with durable and legacy branches
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
    let session = state
        .db
        .get_session(&session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&session_id))?;
    let runs = state.db.find_runs_by_session(&session_id, 200).await?;
    let Some(root) = runs.iter().find(|run| run.parent_run_id.is_none()) else {
        let request = session
            .messages
            .iter()
            .rev()
            .find(|message| message.role == MessageRole::User)
            .map(message_view);
        let result = session
            .messages
            .iter()
            .rev()
            .find(|message| message.role == MessageRole::Assistant)
            .map(message_view);
        return Ok(Json(empty_projection(request.as_ref(), result.as_ref())));
    };
    let tree_ids = run_tree_ids(root, &runs);
    let persisted = state.db.find_workbench(&root.id).await?;
    let correlation = if persisted.is_some() {
        "EXACT"
    } else {
        "LEGACY_FALLBACK"
    };
    let request = persisted
        .as_ref()
        .and_then(|workbench| {
            message_by_id(&session.messages, &workbench.binding.request_message_id)
        })
        .or_else(|| {
            session
                .messages
                .iter()
                .find(|message| message.role == MessageRole::User)
                .map(message_view)
        });
    let result = persisted
        .as_ref()
        .and_then(|workbench| workbench.binding.result_message_id.as_deref())
        .and_then(|id| message_by_id(&session.messages, id));
    let mut manifests = Vec::new();
    let mut evidence = Vec::new();
    for run_id in &tree_ids {
        if let Some(manifest) = state.db.find_artifact_manifest_by_run(run_id).await? {
            manifests.push(manifest);
        }
        evidence.extend(state.db.find_evidence_by_run(run_id).await?);
    }
    let delivery = delivery_view(&manifests);
    let verification = verification_view(persisted.as_ref(), &manifests, &evidence, root);
    let pending_actions = state
        .authz
        .interactions
        .pending_views(&session_id)
        .await
        .map_err(|_| ApiError::internal())?
        .into_iter()
        .filter(|interaction| {
            interaction
                .run_id
                .as_ref()
                .is_some_and(|run_id| tree_ids.contains(run_id))
        })
        .collect::<Vec<_>>();
    let activities = state
        .db
        .find_activities_by_session_paged(&session_id, 0, 200)
        .await?
        .into_iter()
        .filter(|activity| {
            activity
                .get("run_id")
                .and_then(Value::as_str)
                .is_some_and(|run_id| tree_ids.contains(run_id))
        })
        .collect::<Vec<_>>();
    let failure = if root.is_terminal() && root.status != "COMPLETED" {
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
    let previous_delivery = if failure.is_some() {
        previous_delivery_view(&state, root, &runs, &session.messages).await?
    } else {
        None
    };
    let request_id = request
        .as_ref()
        .and_then(|value| value["messageId"].as_str());
    let result_id = result
        .as_ref()
        .and_then(|value| value["messageId"].as_str());
    let result_text = result.as_ref().and_then(|value| value["text"].as_str());
    Ok(Json(json!({
        "correlationMode": correlation,
        "requestMessageId": request_id,
        "resultMessageId": result_id,
        "rootRun": root,
        "request": request,
        "result": result,
        "structuredSummary": structured_summary(result_text),
        "delivery": delivery,
        "verification": verification,
        "pendingActionCount": pending_actions.len(),
        "pendingActions": pending_actions,
        "activities": activities,
        "previousAvailableDelivery": previous_delivery,
        "currentFailure": failure,
    })))
}

async fn previous_delivery_view(
    state: &AppState,
    current: &zk_db::run::RunEnvelopeView,
    runs: &[zk_db::run::RunEnvelopeView],
    messages: &[MessageRecord],
) -> Result<Option<Value>, ApiError> {
    for candidate in runs.iter().filter(|run| {
        run.id != current.id && run.parent_run_id.is_none() && run.status == "COMPLETED"
    }) {
        let tree_ids = run_tree_ids(candidate, runs);
        let mut manifests = Vec::new();
        for run_id in tree_ids {
            if let Some(manifest) = state.db.find_artifact_manifest_by_run(&run_id).await? {
                manifests.push(manifest);
            }
        }
        let delivery = delivery_view(&manifests);
        if delivery["totalFiles"].as_u64().unwrap_or(0) == 0 {
            continue;
        }
        let result = state
            .db
            .find_workbench(&candidate.id)
            .await?
            .and_then(|workbench| workbench.binding.result_message_id)
            .and_then(|id| message_by_id(messages, &id));
        return Ok(Some(json!({
            "rootRunId": candidate.id,
            "finishedAt": candidate.finished_at,
            "result": result,
            "delivery": delivery,
        })));
    }
    Ok(None)
}

fn empty_projection(request: Option<&Value>, result: Option<&Value>) -> Value {
    let request_id = request.and_then(|value| value["messageId"].as_str());
    let result_id = result.and_then(|value| value["messageId"].as_str());
    let result_text = result.and_then(|value| value["text"].as_str());
    json!({
        "correlationMode": "LEGACY_FALLBACK",
        "requestMessageId": request_id,
        "resultMessageId": result_id,
        "rootRun": null,
        "request": request,
        "result": result,
        "structuredSummary": structured_summary(result_text),
        "delivery": {"manifests": [], "files": [], "totalFiles": 0, "primaryArtifactPath": null},
        "verification": {
            "businessCriteria": [], "technicalChecks": [], "evidence": [],
            "overallStatus": "NOT_VERIFIED"
        },
        "pendingActionCount": 0,
        "pendingActions": [],
        "activities": [],
        "previousAvailableDelivery": null,
        "currentFailure": null,
    })
}

fn run_tree_ids(
    root: &zk_db::run::RunEnvelopeView,
    runs: &[zk_db::run::RunEnvelopeView],
) -> std::collections::HashSet<String> {
    let mut ids = std::collections::HashSet::from([root.id.clone()]);
    loop {
        let before = ids.len();
        for run in runs {
            if run
                .parent_run_id
                .as_ref()
                .is_some_and(|parent| ids.contains(parent))
            {
                ids.insert(run.id.clone());
            }
        }
        if ids.len() == before {
            return ids;
        }
    }
}

fn message_by_id(messages: &[MessageRecord], id: &str) -> Option<Value> {
    messages
        .iter()
        .find(|message| message.id == id)
        .map(message_view)
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
        .all(|manifest| manifest.state == "verified")
    {
        "PASSED"
    } else {
        "PARTIAL"
    };
    let runtime_status = if evidence.is_empty() {
        "NOT_VERIFIED"
    } else if evidence.iter().any(|bundle| bundle.verdict == "failed") {
        "FAILED"
    } else if evidence.iter().all(|bundle| bundle.verdict == "verified") {
        "PASSED"
    } else {
        "PARTIAL"
    };
    let technical = vec![
        json!({"id":"technical-manifest-integrity","type":"technical","text":"交付文件与Manifest一致","status":manifest_status,"detail":"只统计当前 Root Run 子树的 Manifest","evidenceBundleId":null}),
        json!({"id":"technical-runtime-verification","type":"technical","text":"页面或程序完成运行时检查","status":runtime_status,"detail":"仅使用明确绑定到当前 Run 树的证据","evidenceBundleId":null}),
        json!({"id":"technical-no-failure-evidence","type":"technical","text":"本轮交付没有明确失败结论","status":if root.is_terminal() && root.status != "COMPLETED" {"FAILED"} else if root.is_terminal() {"PASSED"} else {"NOT_VERIFIED"},"detail":root.error_summary,"evidenceBundleId":null}),
    ];
    let statuses = business
        .iter()
        .chain(&technical)
        .filter_map(|criterion| criterion["status"].as_str())
        .collect::<Vec<_>>();
    let overall = if statuses.contains(&"FAILED") {
        "FAILED"
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
    if run.status != "COMPLETED" {
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
