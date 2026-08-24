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
use axum::extract::{Path as AxumPath, State};
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
pub(crate) async fn list_skills(State(state): State<AppState>) -> Json<Vec<SkillListItem>> {
    let items = state
        .skills
        .all_skills()
        .iter()
        .map(SkillListItem::from)
        .collect();
    Json(items)
}

/// `GET /api/skills/{name}`——单个技能详情（旧 `resolve` 的 `/` 前缀剥离与
/// 大小写不敏感匹配同样生效）。
#[utoipa::path(
    get,
    path = "/api/skills/{name}",
    tag = "skills",
    responses(
        (status = 200, description = "{name, description, source, content, filePath}"),
        (status = 404, description = "SKILL_NOT_FOUND 信封")
    )
)]
pub(crate) async fn get_skill(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<SkillDetail>, ApiError> {
    let skill = state.skills.resolve(&name).ok_or_else(|| {
        ApiError::not_found("SKILL_NOT_FOUND", &format!("Skill not found: {name}"))
    })?;
    Ok(Json(SkillDetail {
        name: skill.effective_name().to_owned(),
        description: skill.effective_description(),
        source: skill.source.as_str(),
        content: skill.content,
        file_path: skill.file_path,
    }))
}
