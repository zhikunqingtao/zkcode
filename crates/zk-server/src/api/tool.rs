//! 工具域端点——`GET /api/tools`、`GET /api/tools/{toolName}`、
//! `PATCH /api/tools/{toolName}`（Batch 1 Step 1-5，P0-9）。
//!
//! 语义来源（旧仓库只读，逐字对照）：
//! `backend/src/main/java/com/aicodeassistant/controller/ToolController.java`
//! （97 行）+ `tool/ToolRegistry.java` + `tool/ToolSessionState.java`。
//!
//! # 端点逐条对照
//!
//! | 旧方法 | HTTP | 响应 | 错误 |
//! |---|---|---|---|
//! | `listTools` L30-57 | `GET /api/tools?sessionId=&toolName=` | `{"tools":[{name,description,category,permissionLevel,enabled}]}` | `toolName` 非空且未注册 → 404 `TOOL_NOT_FOUND` |
//! | `getToolDetail` L60-69 | `GET /api/tools/{toolName}` | `{name,description,category,permissionLevel,inputSchema}` | 未注册 → `ToolRegistry.findByName` 抛 `IllegalArgumentException("Unknown tool: …")` → `GlobalExceptionHandler:107` **400 `INVALID_REQUEST`** |
//! | `toggleTool` L72-87 | `PATCH /api/tools/{toolName}` | `{"tool":name,"enabled":bool}` | 未注册 → 404 `TOOL_NOT_FOUND`；体缺失/非法 → 400 `INVALID_REQUEST_BODY` |
//!
//! # 三处必须照抄的旧行为怪癖
//!
//! 1. **`listTools` 的 `toolName` 只做存在性校验，不过滤列表**（旧 L34-39
//!    先 `findByNameOptional(...).orElseThrow(...)`，随后仍 `getAllTools()`
//!    全量返回）。本移植保持同样语义，不"顺手修正"为筛选。
//! 2. **详情端点未命中是 400 而非 404**：旧 `getToolDetail` 走
//!    `findByName`（抛 `IllegalArgumentException`），而列表/PATCH 走
//!    `findByNameOptional` + `ResourceNotFoundException`。两条路径的状态码在
//!    旧端本就不一致，这是旧行为的一部分（迁移任务书写的「404」与旧源不符，
//!    按准则①「逐字对照 Java」取旧源为准，见交付报告偏差项）。
//! 3. **`sessionId` / `toolName` 的空白串等价于缺省**（旧 `isBlank()` 判定，
//!    非 `isEmpty()`——全空格同样视为未传）。
//!
//! # `enabled` 的两级语义
//!
//! 旧 `enabled = tool.isEnabled()`，再被会话级覆盖顶掉。Rust 侧 feature flag
//! 门控发生在**注册期**（`build_tool_registry`：flag 关 → 工具不注册 → 目录
//! 不可见），故凡出现在注册表里的工具全局启用位恒 `true`；会话级覆盖语义与
//! 旧端完全一致。

use std::collections::HashMap;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::state::AppState;
use crate::tool_catalog::{group_of, permission_level_of};

/// 工具全局启用位——Rust 侧「注册即启用」（见模块文档 `enabled` 两级语义）。
const REGISTERED_TOOL_ENABLED: bool = true;

/// 列表响应体（旧 `record ToolListResponse(List<ToolInfo> tools)`）。
#[derive(Debug, Serialize)]
pub(crate) struct ToolListResponse {
    /// 工具清单（注册表 `BTreeMap` 故恒为工具名字典序）。
    tools: Vec<ToolInfo>,
}

/// 列表项（旧 `record ToolInfo(name, description, category, permissionLevel,
/// enabled)`）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolInfo {
    /// 工具名（LLM function 名 / 注册表键）。
    name: String,
    /// 工具描述（供 LLM 决策的原文）。
    description: String,
    /// 分组（旧 `Tool.getGroup()`）。
    category: &'static str,
    /// 权限需求枚举名（旧 `PermissionRequirement.name()`）。
    permission_level: &'static str,
    /// 启用状态（会话级覆盖优先）。
    enabled: bool,
}

/// 详情体（旧 `record ToolDetail(name, description, category, permissionLevel,
/// inputSchema)`）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ToolDetail {
    /// 工具名。
    name: String,
    /// 工具描述。
    description: String,
    /// 分组。
    category: &'static str,
    /// 权限需求枚举名。
    permission_level: &'static str,
    /// 入参 JSON Schema（旧 `Tool.getInputSchema()` → zk-tools
    /// `Tool::parameters()`）。
    input_schema: serde_json::Value,
}

/// PATCH 请求体（旧 `record ToggleToolRequest(String sessionId, boolean
/// enabled)`——Jackson 对缺省 `enabled` 填 `false`、缺省 `sessionId` 填
/// `null`，故此处两字段皆 `#[serde(default)]`）。
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToggleToolRequest {
    /// 目标会话（空/空白 → 只做存在性校验，不落状态；旧 L82 `isBlank`）。
    #[serde(default)]
    session_id: Option<String>,
    /// 目标启用位。
    #[serde(default)]
    enabled: bool,
}

/// PATCH 响应体（旧 `Map.of("tool", toolName, "enabled", request.enabled())`）。
#[derive(Debug, Serialize)]
pub(crate) struct ToggleToolResponse {
    /// 被改写的工具名（路径参数原值）。
    tool: String,
    /// 改写后的启用位（请求值原样回显——旧实现不回读状态表）。
    enabled: bool,
}

/// 空白串归一（旧 `s != null && !s.isBlank()` 判定的等价物）。
fn non_blank(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|text| !text.is_empty())
}

/// 未注册工具的 404（旧 `ResourceNotFoundException("TOOL_NOT_FOUND",
/// "Tool not found: " + toolName)` 文案逐字）。
fn tool_not_found(tool_name: &str) -> ApiError {
    ApiError::not_found("TOOL_NOT_FOUND", &format!("Tool not found: {tool_name}"))
}

/// `GET /api/tools`——全部已注册工具（含会话级启用位覆盖）。
///
/// `toolName` 参数只做存在性校验、**不筛选**返回列表（旧 L34-39 语义，见
/// 模块文档怪癖 ①）。
#[utoipa::path(
    get,
    path = "/api/tools",
    tag = "tools",
    params(
        ("sessionId" = Option<String>, Query, description = "会话级启用位覆盖的归属会话（空白等价缺省）"),
        ("toolName" = Option<String>, Query, description = "仅校验该工具是否存在（不筛选列表）；未注册 → 404")
    ),
    responses(
        (status = 200, description = "{tools:[{name,description,category,permissionLevel,enabled}]}"),
        (status = 404, description = "toolName 未注册（TOOL_NOT_FOUND 信封）")
    )
)]
pub(crate) async fn list_tools(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<ToolListResponse>, ApiError> {
    let registry = state.tools();
    // 旧 L34-39：单个工具查询时先验证存在性（校验完毕仍返回全量列表）。
    if let Some(tool_name) = non_blank(query.get("toolName").map(String::as_str))
        && registry.get(tool_name).is_none()
    {
        return Err(tool_not_found(tool_name));
    }
    let session_id = non_blank(query.get("sessionId").map(String::as_str));
    let tools = registry
        .specs()
        .into_iter()
        .map(|spec| {
            // 旧 L42-47：全局启用位 → 会话级覆盖顶掉。
            let enabled = session_id
                .and_then(|session| state.tool_session_state.tool_state(session, &spec.name))
                .unwrap_or(REGISTERED_TOOL_ENABLED);
            ToolInfo {
                category: group_of(&spec.name),
                permission_level: permission_level_of(&spec.name),
                name: spec.name,
                description: spec.description,
                enabled,
            }
        })
        .collect();
    Ok(Json(ToolListResponse { tools }))
}

/// `GET /api/tools/{toolName}`——工具详情（含入参 Schema）。
///
/// 未命中 → 400 `INVALID_REQUEST` + `Unknown tool: {name}`（旧
/// `findByName` 的 `IllegalArgumentException` 经 `GlobalExceptionHandler:107`
/// 落 400，见模块文档怪癖 ②）。
#[utoipa::path(
    get,
    path = "/api/tools/{toolName}",
    tag = "tools",
    params(("toolName" = String, Path, description = "工具名")),
    responses(
        (status = 200, description = "{name,description,category,permissionLevel,inputSchema}"),
        (status = 400, description = "未注册工具（INVALID_REQUEST + `Unknown tool: …`，旧 findByName 语义）")
    )
)]
pub(crate) async fn get_tool_detail(
    State(state): State<AppState>,
    AxumPath(tool_name): AxumPath<String>,
) -> Result<Json<ToolDetail>, ApiError> {
    let tool = state
        .tools()
        .get(&tool_name)
        .ok_or_else(|| ApiError::validation(format!("Unknown tool: {tool_name}")))?;
    let spec = tool.spec();
    Ok(Json(ToolDetail {
        category: group_of(&spec.name),
        permission_level: permission_level_of(&spec.name),
        name: spec.name,
        description: spec.description,
        input_schema: spec.parameters,
    }))
}

/// `PATCH /api/tools/{toolName}`——启用/禁用工具（会话级）。
///
/// 存在性校验走 `findByNameOptional` → 404 `TOOL_NOT_FOUND`；`sessionId`
/// 空白时**不落状态**但仍回 200（旧 L82-84 语义）。
#[utoipa::path(
    patch,
    path = "/api/tools/{toolName}",
    tag = "tools",
    params(("toolName" = String, Path, description = "工具名")),
    responses(
        (status = 200, description = "{tool,enabled}"),
        (status = 400, description = "体缺失或非法 JSON（INVALID_REQUEST_BODY）"),
        (status = 404, description = "工具未注册（TOOL_NOT_FOUND 信封）")
    )
)]
pub(crate) async fn toggle_tool(
    State(state): State<AppState>,
    AxumPath(tool_name): AxumPath<String>,
    body: Bytes,
) -> Result<Json<ToggleToolResponse>, ApiError> {
    // 旧 `@RequestBody`（required 默认 true）：空体 → `HttpMessageNotReadable`
    // → 400 `INVALID_REQUEST_BODY`。
    if body.is_empty() {
        return Err(ApiError::invalid_request_body());
    }
    let request = serde_json::from_slice::<ToggleToolRequest>(&body)
        .map_err(|_| ApiError::invalid_request_body())?;
    // 旧 L76-79：先验证工具存在性（先于状态写入）。
    if state.tools().get(&tool_name).is_none() {
        return Err(tool_not_found(&tool_name));
    }
    // 旧 L80-84：仅在 sessionId 非空白时持久化会话级状态。
    if let Some(session_id) = non_blank(request.session_id.as_deref()) {
        state
            .tool_session_state
            .set_tool_state(session_id, &tool_name, request.enabled);
    }
    Ok(Json(ToggleToolResponse {
        tool: tool_name,
        enabled: request.enabled,
    }))
}

#[cfg(test)]
mod tests {
    use super::{ToggleToolRequest, non_blank};

    /// 空白归一：`None` / 空串 / 全空格皆视为缺省（旧 `isBlank`）。
    #[test]
    fn blank_query_values_are_treated_as_absent() {
        assert_eq!(non_blank(None), None);
        assert_eq!(non_blank(Some("")), None);
        assert_eq!(non_blank(Some("   ")), None);
        assert_eq!(non_blank(Some(" s1 ")), Some("s1"));
    }

    /// 缺省字段对齐 Jackson：`enabled` → `false`、`sessionId` → `null`。
    #[test]
    fn toggle_request_defaults_match_jackson() {
        let request: ToggleToolRequest = serde_json::from_str("{}").expect("empty object parses");
        assert!(!request.enabled);
        assert_eq!(request.session_id, None);

        let request: ToggleToolRequest =
            serde_json::from_str(r#"{"sessionId":"s1","enabled":true}"#).expect("full body parses");
        assert!(request.enabled);
        assert_eq!(request.session_id.as_deref(), Some("s1"));
    }
}
