//! 技能域端点——`GET /api/skills` 与 `GET /api/skills/{name}`（3B.7）。
//!
//! 语义来源（旧仓库只读，`581d407b`）：
//! `backend/src/main/java/com/aicodeassistant/controller/SkillController.java`
//! 逐字段复刻：
//!
//! - 列表端点 → `List<Map>`，每项三键 `name` / `description` / `source`
//!   （`name` 取 `effectiveName()`，`description` 取 `effectiveDescription()`，
//!   `source` 取枚举名大写）；
//! - 详情端点 → 五键 `name` / `description` / `source` / `content` /
//!   `filePath`（`filePath` 为内置技能时 `null`），未命中抛
//!   `ResourceNotFoundException("SKILL_NOT_FOUND", "Skill not found: " + name)`
//!   → 信封 404。
//!
//! 前端契约（旧仓库 `frontend/src/App.tsx:89` 与
//! `components/skills/SkillDetailModal.tsx`）依赖上述两个形状，不可增删键。

use axum::Json;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::HeaderMap;
use serde::Serialize;

use crate::error::ApiError;
use crate::skill::SkillDefinition;
use crate::state::AppState;

/// 列表项（旧 `SkillController.listSkills` 的 `Map.of` 三键）。
#[derive(Debug, Serialize)]
pub(crate) struct SkillListItem {
    /// 展示名（`frontmatter.name` 优先，回落文件名）。
    name: String,
    /// 展示描述（`frontmatter.description` 优先，回落 `Skill: <name>`）。
    description: String,
    /// 加载来源（`BUNDLED` / `USER` / `PROJECT` / …）。
    source: &'static str,
}

impl From<&SkillDefinition> for SkillListItem {
    fn from(skill: &SkillDefinition) -> Self {
        Self {
            name: skill.effective_name().to_owned(),
            description: skill.effective_description(),
            source: skill.source.as_str(),
        }
    }
}

/// 详情体（旧 `SkillController.getSkill` 的 `HashMap` 五键——`filePath`
/// 可为 null 故不能用 `Map.of`，旧源同样如此）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SkillDetail {
    /// 展示名。
    name: String,
    /// 展示描述。
    description: String,
    /// 加载来源。
    source: &'static str,
    /// Markdown 正文（模板原文，未做参数替换）。
    content: String,
    /// 文件绝对路径（内置技能为 `null`）。
    file_path: Option<String>,
}

/// `GET /api/skills`——全部已注册技能（按 `effectiveName` 升序）。
#[utoipa::path(
    get,
    path = "/api/skills",
    tag = "skills",
    responses(
        (status = 200, description = "[{name, description, source}]（按展示名升序）")
    )
)]
pub(crate) async fn list_skills(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SkillScopeQuery>,
) -> Result<Json<Vec<SkillListItem>>, ApiError> {
    let view = scope(&state, &headers, query.project_id.as_deref()).await?;
    Ok(Json(
        view.all_skills().iter().map(SkillListItem::from).collect(),
    ))
}

/// `GET /api/skills/{name}`——单个技能详情（旧 `resolve` 的 `/` 前缀剥离与
/// 大小写不敏感匹配同样生效）。
#[utoipa::path(
    get,
    path = "/api/skills/{name}",
    tag = "skills",
    responses(
        (status = 200, description = "{name, description, source, content, filePath}"),
        (status = 403, description = "SKILL_SOURCE_UNAUTHORIZED：来源不再属于已授权目录"),
        (status = 503, description = "SKILL_SCAN_FAILED / SKILL_READ_FAILED：来源刷新失败"),
        (status = 404, description = "SKILL_NOT_FOUND 信封")
    )
)]
pub(crate) async fn get_skill(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<SkillScopeQuery>,
) -> Result<Json<SkillDetail>, ApiError> {
    let view = scope(&state, &headers, query.project_id.as_deref()).await?;
    let skill = view
        .resolve(&name)
        .ok_or_else(|| missing_skill(&view, &name))?;
    Ok(Json(SkillDetail {
        name: skill.effective_name().to_owned(),
        description: skill.effective_description(),
        source: skill.source.as_str(),
        content: skill.content,
        file_path: skill.file_path,
    }))
}

fn missing_skill(view: &crate::skill::catalog::SkillView, name: &str) -> ApiError {
    match view.state_error().as_deref() {
        Some("SKILL_SOURCE_UNAUTHORIZED") => ApiError {
            status: axum::http::StatusCode::FORBIDDEN,
            code: "SKILL_SOURCE_UNAUTHORIZED".into(),
            message: "Skill source is outside its authorized root or the root identity changed"
                .into(),
        },
        Some(code @ ("SKILL_SCAN_FAILED" | "SKILL_READ_FAILED")) => ApiError {
            status: axum::http::StatusCode::SERVICE_UNAVAILABLE,
            code: code.into(),
            message: "Skill source could not be refreshed; the last validated snapshot is retained"
                .into(),
        },
        _ => ApiError::not_found("SKILL_NOT_FOUND", &format!("Skill not found: {name}")),
    }
}

fn management_item(
    state: &AppState,
    view: &crate::skill::catalog::SkillView,
    skill: &SkillDefinition,
) -> serde_json::Value {
    serde_json::json!({
        "id": skill.name, "name": skill.effective_name(),
        "description": skill.effective_description(), "source": skill.source.as_str(),
        "enabled": state.skills.is_enabled(&skill.name), "scope": "global",
        "stateError": view.state_error(), "content": skill.content,
        "filePath": skill.file_path, "userInvocable": skill.is_user_invocable(),
    })
}

/// Optional selected project; explicit session scope comes from X-Session-Id.
#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SkillScopeQuery {
    project_id: Option<String>,
}

async fn scope(
    state: &AppState,
    headers: &HeaderMap,
    project: Option<&str>,
) -> Result<crate::skill::catalog::SkillView, ApiError> {
    let session = headers
        .get("X-Session-Id")
        .map(|header| header.to_str())
        .transpose()
        .map_err(|_| ApiError::validation("Invalid Skill session scope"))?;
    if session.is_some_and(|id| id.trim().is_empty())
        || project.is_some_and(|id| id.trim().is_empty())
    {
        return Err(ApiError::validation("Empty Skill scope"));
    }
    state
        .skill_catalog
        .view(session, project)
        .await
        .map_err(Into::into)
}

pub(crate) async fn manage_skills(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SkillScopeQuery>,
) -> Result<Json<Vec<serde_json::Value>>, ApiError> {
    let view = scope(&state, &headers, query.project_id.as_deref()).await?;
    Ok(Json(
        view.manage_skills()
            .iter()
            .map(|skill| management_item(&state, &view, skill))
            .collect(),
    ))
}

pub(crate) async fn manage_skill(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<SkillScopeQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let view = scope(&state, &headers, query.project_id.as_deref()).await?;
    let skill = view
        .resolve_including_disabled(&id)
        .filter(|skill| skill.name.eq_ignore_ascii_case(&id))
        .ok_or_else(|| missing_skill(&view, &id))?;
    Ok(Json(management_item(&state, &view, &skill)))
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToggleSkillQuery {
    enabled: bool,
    project_id: Option<String>,
}

pub(crate) async fn toggle_skill(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<ToggleSkillQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let view = scope(&state, &headers, query.project_id.as_deref()).await?;
    let skill = view
        .resolve_including_disabled(&id)
        .filter(|skill| skill.name.eq_ignore_ascii_case(&id))
        .ok_or_else(|| missing_skill(&view, &id))?;
    if !state.skills.state_available() {
        return Err(ApiError { status:axum::http::StatusCode::SERVICE_UNAVAILABLE, code:"SKILL_STATE_UNAVAILABLE".into(), message:"Skill preferences are unavailable; repair the database and restart before changing them".into() });
    }
    view.set_enabled(&skill.name, query.enabled)
        .await
        .map_err(|_| ApiError {
            status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            code: "SKILL_SETTINGS_SAVE_FAILED".into(),
            message: "Skill preference could not be saved; the last valid state is retained".into(),
        })?;
    Ok(Json(management_item(&state, &view, &skill)))
}
