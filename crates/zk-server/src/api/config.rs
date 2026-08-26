//! 配置域端点——`GET/PUT /api/config`（S7b）与 `GET/PUT /api/config/project`
//! （2.1）。
//!
//! 语义来源（旧仓库只读）：`ConfigController.getUserConfig` /
//! `updateUserConfig` / `getProjectConfig` / `updateProjectConfig`（PATCH
//! 语义：顶层键合并后整体校验、落库、回显）与 `ConfigService` 的代码内置
//! 默认值。响应形状权威：`GET_api-config.json` / `PUT_api-config.json` /
//! `GET_api-config-project.json` 样例逐键对齐（null 可选键经 `NON_NULL`
//! 剥离，样例不含）。
//!
//! 存储裁定：用户配置存 zk-db `config` 表（key=`user_config`）；项目配置存
//! `project_config` 表（key=`project_config`，2.1 补齐 Phase 1 决策 §12 降级项，
//! 旧系统为 `.ai-code-assistant/config.json` 文件——存储介质差异留痕
//! `docs/compatibility.md` §2）。

use std::collections::BTreeMap;

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ApiError;
use crate::state::AppState;

/// config 表内的用户全局配置行键（旧 `ConfigService` 的 `user_config`）。
const USER_CONFIG_KEY: &str = "user_config";

/// `project_config` 表内的项目级配置行键（旧 `ConfigService` 的
/// `project_config`）。
const PROJECT_CONFIG_KEY: &str = "project_config";

/// 用户全局配置（旧 `UserConfig` record 的线上/存储双形状）。
///
/// 键集对齐 `GET_api-config.json` 样例 12 键；`api_key` / `oauth_token`
/// 为旧 record 组件，null 时剥离（`NON_NULL`），样例中不出现。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct UserConfig {
    /// 认证类型（Phase 1 恒 `localhost`）。
    pub auth_type: String,
    /// API 密钥（旧 record 组件；null 剥离，样例不含）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// OAuth 令牌（旧 record 组件；null 剥离，样例不含）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth_token: Option<Value>,
    /// 默认模型。
    pub default_model: String,
    /// 模型别名映射。
    pub model_aliases: BTreeMap<String, String>,
    /// 主题（旧默认 `dark`）。
    pub theme: String,
    /// 语言（旧默认 `en`）。
    pub locale: String,
    /// 默认权限模式（旧枚举 `PermissionMode`，Phase 1 以字符串承载）。
    pub default_permission_mode: String,
    /// 全局 always-allow 规则（Phase 1 无权限域，元素以 JSON 承载）。
    pub global_always_allow_rules: Vec<Value>,
    /// 全局 always-deny 规则。
    pub global_always_deny_rules: Vec<Value>,
    /// MCP 服务器配置（Phase 1 无 MCP 域，值以 JSON 承载）。
    pub mcp_servers: BTreeMap<String, Value>,
    /// 是否上报分析数据。
    pub analytics_enabled: bool,
    /// 自动压缩开关。
    pub auto_compact_enabled: bool,
    /// 自动压缩阈值（百分比）。
    pub auto_compact_threshold: i64,
}

impl Default for UserConfig {
    /// 代码内置默认值（旧 `defaultUserConfig` 逐字段照抄；`default_model`
    /// 在 handler 侧以运行配置覆盖，见 [`UserConfig::default_with_model`]）。
    fn default() -> Self {
        Self {
            auth_type: "localhost".into(),
            api_key: None,
            oauth_token: None,
            default_model: "qwen3.8-max".into(),
            model_aliases: BTreeMap::new(),
            theme: "dark".into(),
            locale: "en".into(),
            // Keep the public configuration response aligned with the actual
            // new-session behavior in `api::session::create_session`.
            default_permission_mode: "AUTO_APPROVE".into(),
            global_always_allow_rules: Vec::new(),
            global_always_deny_rules: Vec::new(),
            mcp_servers: BTreeMap::new(),
            analytics_enabled: false,
            auto_compact_enabled: true,
            auto_compact_threshold: 80,
        }
    }
}

impl UserConfig {
    /// 默认配置 + 运行配置的默认模型（旧 `@Value("${app.model.default}")`
    /// 注入语义：默认模型来源与创建会话共用同一配置源）。
    fn default_with_model(default_model: &str) -> Self {
        Self {
            default_model: default_model.to_owned(),
            ..Self::default()
        }
    }
}

/// `PUT /api/config` 200 响应（旧 `Map.of("success", true, "config", updated)`）。
#[derive(Debug, Serialize)]
pub(crate) struct UpdateConfigResponse {
    /// 恒 `true`（失败在错误信封层）。
    pub success: bool,
    /// 合并后的完整配置回显。
    pub config: UserConfig,
}

/// 项目级配置（旧 `ProjectConfig` record 6 字段的线上/存储双形状）。
///
/// 键集对齐 `GET_api-config-project.json` 样例 4 键；`last_session_id` /
/// `last_model` 为旧 record 组件，null 时剥离（`NON_NULL`），样例中不出现。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct ProjectConfig {
    /// 最近会话 id（null 剥离，样例不含）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_session_id: Option<String>,
    /// 最近使用模型（null 剥离，样例不含）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_model: Option<String>,
    /// 最近会话成本（旧 `double`，默认 0.0）。
    pub last_cost: f64,
    /// 项目级 always-allow 规则（元素以 JSON 承载，同用户配置裁定）。
    pub project_always_allow_rules: Vec<Value>,
    /// 项目级 MCP 服务器配置（值以 JSON 承载）。
    pub project_mcp_servers: BTreeMap<String, Value>,
    /// 自定义设置（自由形状）。
    pub custom_settings: BTreeMap<String, Value>,
}

/// `PUT /api/config/project` 200 响应（旧 `Map.of("success", true,
/// "config", updated)`）。
#[derive(Debug, Serialize)]
pub(crate) struct UpdateProjectConfigResponse {
    /// 恒 `true`（失败在错误信封层）。
    pub success: bool,
    /// 合并后的完整配置回显。
    pub config: ProjectConfig,
}

/// `GET /api/config`——用户全局配置（无存量行时返回代码内置默认值）。
#[utoipa::path(
    get,
    path = "/api/config",
    tag = "config",
    responses((status = 200, description = "用户全局配置（键集对齐 GET_api-config.json 样例）"))
)]
pub(crate) async fn get_config(
    State(state): State<AppState>,
) -> Result<Json<UserConfig>, ApiError> {
    Ok(Json(load_user_config(&state).await?))
}

/// `PUT /api/config`——PATCH 语义部分更新（顶层键合并 → 校验 → 落库 → 回显）。
#[utoipa::path(
    put,
    path = "/api/config",
    tag = "config",
    responses(
        (status = 200, description = "更新成功，回显合并后完整配置（PUT_api-config.json 样例形状）"),
        (status = 400, description = "请求体非 JSON 对象（INVALID_REQUEST_BODY）"),
        (status = 500, description = "字段类型非法（旧 ConfigService 反序列化失败 → RuntimeException → INTERNAL_ERROR）")
    )
)]
pub(crate) async fn put_config(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<UpdateConfigResponse>, ApiError> {
    let Ok(Value::Object(updates)) = serde_json::from_slice::<Value>(&body) else {
        // 旧 `@RequestBody Map<String,Object>`：缺失/非法 JSON/非对象体 → 400。
        return Err(ApiError::invalid_request_body());
    };
    let current = load_user_config(&state).await?;
    // 旧 `updateUserConfig` 的 putAll 合并：当前配置序列化为 JSON 对象后
    // 顶层键覆盖（未知键随合并进入但不落形状——serde 忽略未知字段）。
    let mut merged = serde_json::to_value(&current).map_err(|err| {
        tracing::error!(error = %err, "user config serialization failed");
        ApiError::internal()
    })?;
    if let Value::Object(map) = &mut merged {
        map.extend(updates);
    }
    // 类型非法（如 autoCompactThreshold 传字符串）→ 500：旧
    // `ConfigService.updateUserConfig` 的 Jackson 反序列化失败包装为
    // `RuntimeException`，经 `GlobalExceptionHandler.handleGeneric` 落
    // 500 `INTERNAL_ERROR`（细节只进日志，不透出响应体）。
    let next: UserConfig = serde_json::from_value(merged).map_err(|err| {
        tracing::error!(error = %err, "user config update rejected by shape");
        ApiError::internal()
    })?;
    let json = serde_json::to_string(&next).map_err(|err| {
        tracing::error!(error = %err, "user config serialization failed");
        ApiError::internal()
    })?;
    state.db.put_config_value(USER_CONFIG_KEY, &json).await?;
    Ok(Json(UpdateConfigResponse {
        success: true,
        config: next,
    }))
}

/// 存量行加载 + 默认值兜底（GET / PUT 与 `/config` 斜杠命令共用；对齐旧 `getUserConfig` 的
/// 「解析失败回默认」宽容语义——存量损坏不阻断服务）。
pub(crate) async fn load_user_config(state: &AppState) -> Result<UserConfig, ApiError> {
    let stored = state.db.get_config_value(USER_CONFIG_KEY).await?;
    Ok(stored
        .and_then(|json| {
            serde_json::from_str::<UserConfig>(&json)
                .map_err(|err| {
                    tracing::warn!(error = %err, "stored user config unparseable, using defaults");
                })
                .ok()
        })
        .unwrap_or_else(|| UserConfig::default_with_model(&state.config.default_model)))
}

/// `GET /api/config/project`——项目级配置（无存量行时返回默认值）。
#[utoipa::path(
    get,
    path = "/api/config/project",
    tag = "config",
    responses((status = 200, description = "项目级配置（键集对齐 GET_api-config-project.json 样例）"))
)]
pub(crate) async fn get_project_config(
    State(state): State<AppState>,
) -> Result<Json<ProjectConfig>, ApiError> {
    Ok(Json(load_project_config(&state).await?))
}

/// `PUT /api/config/project`——PATCH 语义部分更新（同 `PUT /api/config`：
/// 顶层键合并 → 校验 → 落库 → 回显）。
#[utoipa::path(
    put,
    path = "/api/config/project",
    tag = "config",
    responses(
        (status = 200, description = "更新成功，回显合并后完整配置（旧 {success,config} 形状）"),
        (status = 400, description = "请求体非 JSON 对象（INVALID_REQUEST_BODY）"),
        (status = 500, description = "字段类型非法（旧 ConfigService 反序列化失败 → RuntimeException → INTERNAL_ERROR）")
    )
)]
pub(crate) async fn put_project_config(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<UpdateProjectConfigResponse>, ApiError> {
    let Ok(Value::Object(updates)) = serde_json::from_slice::<Value>(&body) else {
        return Err(ApiError::invalid_request_body());
    };
    let current = load_project_config(&state).await?;
    let mut merged = serde_json::to_value(&current).map_err(|err| {
        tracing::error!(error = %err, "project config serialization failed");
        ApiError::internal()
    })?;
    if let Value::Object(map) = &mut merged {
        map.extend(updates);
    }
    // 类型非法 → 500（同 `put_config`：旧 `updateProjectConfig` 抛
    // `RuntimeException`，经 `handleGeneric` 归一为 `INTERNAL_ERROR`）。
    let next: ProjectConfig = serde_json::from_value(merged).map_err(|err| {
        tracing::error!(error = %err, "project config update rejected by shape");
        ApiError::internal()
    })?;
    let json = serde_json::to_string(&next).map_err(|err| {
        tracing::error!(error = %err, "project config serialization failed");
        ApiError::internal()
    })?;
    state
        .db
        .put_project_config_value(PROJECT_CONFIG_KEY, &json)
        .await?;
    Ok(Json(UpdateProjectConfigResponse {
        success: true,
        config: next,
    }))
}

/// 项目级配置加载 + 默认值兜底（同 `load_user_config` 的宽容语义）。
async fn load_project_config(state: &AppState) -> Result<ProjectConfig, ApiError> {
    let stored = state
        .db
        .get_project_config_value(PROJECT_CONFIG_KEY)
        .await?;
    Ok(stored
        .and_then(|json| {
            serde_json::from_str::<ProjectConfig>(&json)
                .map_err(|err| {
                    tracing::warn!(error = %err, "stored project config unparseable, using defaults");
                })
                .ok()
        })
        .unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 默认配置序列化键集与样例 12 键一致（null 可选键剥离）。
    #[test]
    fn default_config_serializes_sample_key_set() {
        let value = serde_json::to_value(UserConfig::default_with_model("m1")).expect("json");
        let mut keys: Vec<&str> = value
            .as_object()
            .expect("obj")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "analyticsEnabled",
                "authType",
                "autoCompactEnabled",
                "autoCompactThreshold",
                "defaultModel",
                "defaultPermissionMode",
                "globalAlwaysAllowRules",
                "globalAlwaysDenyRules",
                "locale",
                "mcpServers",
                "modelAliases",
                "theme"
            ]
        );
        assert_eq!(value["defaultModel"], "m1");
        assert_eq!(value["defaultPermissionMode"], "AUTO_APPROVE");
        assert_eq!(value["theme"], "dark");
        assert_eq!(value["autoCompactThreshold"], 80);
    }

    /// 默认项目配置序列化键集与 `GET_api-config-project.json` 样例 4 键
    /// 一致（null 可选键剥离）。
    #[test]
    fn default_project_config_serializes_sample_key_set() {
        let value = serde_json::to_value(ProjectConfig::default()).expect("json");
        let mut keys: Vec<&str> = value
            .as_object()
            .expect("obj")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "customSettings",
                "lastCost",
                "projectAlwaysAllowRules",
                "projectMcpServers"
            ]
        );
        assert_eq!(value["lastCost"], 0.0);
    }
}
