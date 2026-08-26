//! MCP 能力注册表端点——对照旧 `McpCapabilityController`（237 行，基路径
//! `/api/mcp/capabilities`）。
//!
//! # 端点逐条对照
//!
//! | 方法 + 路径 | 旧方法（行号） | 成功 | 失败 |
//! | --- | --- | --- | --- |
//! | `GET /api/mcp/capabilities` | `listCapabilities` L34-50 | 200 `{capabilities,total,enabledCount}` | `enabled` 不可识别 → 500 |
//! | `GET /api/mcp/capabilities/{id}` | `getCapability` L52-57 | 200 裸定义 | 404 空体 |
//! | `PUT /api/mcp/capabilities/{id}` | `updateCapability` L59-67 | 200 裸定义 | 404 空体 / 400 体不可解析 |
//! | `PATCH /api/mcp/capabilities/{id}/toggle` | `toggleCapability` L69-93 | 200 `{id,enabled,status}` | 404 空体 / 400 缺 `enabled` / 500 |
//! | `POST /api/mcp/capabilities` | `addCapability` L95-103 | 201 裸定义 | 400 空体（id 重复）/ 400 体不可解析 |
//! | `DELETE /api/mcp/capabilities/{id}` | `deleteCapability` L105-110 | 200 `{success:true}` | 404 空体 |
//! | `GET /api/mcp/capabilities/domains` | `listDomains` L112-115 | 200 `{domains}` | — |
//! | `GET /api/mcp/capabilities/{id}/server-tools` | `listServerTools` L117-147 | 200 三形态 | 404 空体 |
//! | `POST /api/mcp/capabilities/{id}/test` | `testCapability` L149-170 | 200 `{id,status,serverKey}` | 404 空体 |
//! | `POST /api/mcp/capabilities/{id}/invoke` | legacy `invokeCapability` | — | 403；必须改走带 session/run 上下文的 `/mcp tools/call` |
//!
//! # 必须照抄的旧行为怪癖
//!
//! 1. **失败体是空体**——旧端用 `ResponseEntity.notFound().build()` /
//!    `.badRequest().build()`，既不带全局错误信封也不带任何正文；前端
//!    `mcpCapabilityStore.ts` 只判 `resp.ok`，故照抄空体。
//! 2. `server-tools` / `test` 把**一切**下游异常吞成 200 +
//!    `{"status":"error"}`；只有「能力 id 不在注册表」才是 404。
//! 3. `server-tools` 的错误分支**不带** `serverKey`（旧 L143-144），而成功分支
//!    与 `not_connected` 分支带。
//! 4. `toggle` 关闭时有「同服务器多工具保护」：仅当**无**其他启用能力共享同一
//!    `serverKey` 才摘除服务器（旧 L80-86）。
//! 5. `total` / `enabledCount` 取自**整表**，与 `capabilities` 的过滤结果无关
//!    （旧 L46-49）。
//! 6. `initialize` 握手补偿的异常被**静默忽略**（旧 L133）——已初始化的服务器
//!    会拒绝重复握手，属预期。
//!
//! # 偏离
//!
//! - `B4B-08`：`initialize.clientInfo` 取
//!   [`zk_mcp::protocol::CLIENT_NAME`]（`"zkcode"`）而非旧硬编码
//!   `"zhikuncode"`——整仓身份串改名，已在 `zk-mcp` 侧记录，非行为差异。
//! - `B4B-09`：`PUT /{id}` 体缺 `id` 或 `id: null` 时以 URL 的 `id` 补齐
//!   （旧 Jackson 允许 `null` id 并原样入表，[`McpCapabilityDefinition::id`]
//!   为必填 `String`）。入表键两侧同为 URL 的 `id`，行为等价。
//! - `B4B-10`：`POST` 体缺 `id` → 400 `INVALID_REQUEST_BODY`（旧
//!   `LinkedHashMap` 接受 `null` 键并 201 回 `id:null`，是纯缺陷）。
//! - `B4B-11`：`test` 的 `status:"error"` 分支不可达——
//!   [`McpServerConnection::connect`] 自吞错误不返回 `Result`，连接失败体现为
//!   `is_alive() == false` → `"unreachable"`（旧同样绝大多数失败落
//!   `"unreachable"`，仅构造期异常才 `"error"`）。
//! - `B4B-12`：`server-tools` 的 `toolName` 为 `null` 时以 `""` 调用。
//! - `B4B-15`：`server-tools` 的下游 `result` 缺失时回 `null`
//!   （旧 `Map.of("tools", null)` 抛 NPE → 200 `{"status":"error"}`，是纯缺陷）。
//! - `B4B-16`：请求体不可解析 → 400 `INVALID_REQUEST_BODY`（旧
//!   `HttpMessageNotReadableException` 无专门 handler → 500）；与
//!   [`crate::api::mcp`] 的 `add_server` 取同一约定。
//! - **安全边界**：能力定义是本地 HTTP 可变输入，新增/更新会拒绝非 TLS、
//!   loopback/私网、URL userinfo、任意 `apiKeyConfig` 与非 `DashScope` 凭据
//!   主机。legacy `invoke` 缺少权威 session/run 上下文，无法复用生产
//!   Hook/Admission/ToolRegistry，故失败关闭。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use zk_mcp::capability_registry::RegistryError;
use zk_mcp::protocol::{
    CLIENT_NAME, METHOD_INITIALIZE, METHOD_INITIALIZED, METHOD_TOOLS_LIST, PROTOCOL_VERSION,
    client_info,
};
use zk_mcp::{
    McpCapabilityDefinition, McpConnectionStatus, McpServerConnection,
    validate_capability_definition, validate_capability_destination,
};

use crate::api::http_params::{optional_spring_bool, require_spring_bool};
use crate::api::mcp::manager_error;
use crate::error::ApiError;
use crate::state::AppState;

/// `initialize` / `tools/list` 的请求超时（旧两处硬编码 `15000`）。
const MCP_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

// ===== 视图与共用助手 =====

/// 能力定义 → 线上 JSON。
///
/// 旧 record 带 `@JsonInclude(NON_NULL)`，[`McpCapabilityDefinition`] 各
/// `Option` 字段的 `skip_serializing_if = "Option::is_none"` 与之逐字等价，
/// 故直接复用其 `Serialize`（**不**铺开 `null`）。
fn capability_view(definition: &McpCapabilityDefinition) -> Value {
    serde_json::to_value(definition).expect("McpCapabilityDefinition 序列化不会失败")
}

/// 404 空体（旧 `ResponseEntity.notFound().build()`）。
fn not_found() -> Response {
    StatusCode::NOT_FOUND.into_response()
}

/// 400 空体（旧 `ResponseEntity.badRequest().build()`）。
fn bad_request_empty() -> Response {
    StatusCode::BAD_REQUEST.into_response()
}

/// 200 + 裸 JSON。
fn ok_json(body: Value) -> Response {
    Json(body).into_response()
}

/// `{"id":…,"status":"error","error":…}`——三个自吞异常端点的统一失败形状。
fn error_view(id: &str, message: &str) -> Value {
    json!({ "id": id, "status": "error", "error": message })
}

fn capability_security_error(error: impl std::fmt::Display) -> ApiError {
    ApiError::validation_with_code("MCP_CAPABILITY_REJECTED", &error.to_string())
}

fn direct_invoke_error(code: &str, message: &str) -> ApiError {
    ApiError {
        status: StatusCode::FORBIDDEN,
        code: code.to_owned(),
        message: message.to_owned(),
    }
}

/// 解析 `PUT` 的能力定义体，`id` 缺失 / 为 `null` 时以 URL 的 `id` 补齐
/// （见模块偏离 `B4B-09`）。
fn parse_definition(body: &Bytes, id: &str) -> Result<McpCapabilityDefinition, ApiError> {
    if body.is_empty() {
        return Err(ApiError::invalid_request_body());
    }
    let mut value: Value =
        serde_json::from_slice(body).map_err(|_| ApiError::invalid_request_body())?;
    if let Some(object) = value.as_object_mut()
        && object.get("id").is_none_or(Value::is_null)
    {
        object.insert("id".to_owned(), Value::String(id.to_owned()));
    }
    serde_json::from_value(value).map_err(|_| ApiError::invalid_request_body())
}

/// 握手补偿（旧 L126-133 / L191-197）：异常一律忽略，随后无条件补发
/// `notifications/initialized`。
async fn ensure_initialized(connection: &Arc<McpServerConnection>) {
    let params = json!({
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": client_info(CLIENT_NAME),
    });
    if let Err(error) = connection
        .request(METHOD_INITIALIZE, Some(params), MCP_REQUEST_TIMEOUT)
        .await
    {
        tracing::debug!(server = connection.name(), %error, "MCP re-initialize ignored");
    }
    connection.send_notification(METHOD_INITIALIZED, None).await;
}

// ===== CRUD 端点 =====

/// `GET /api/mcp/capabilities`——按 `domain` / `enabled` 过滤（旧
/// `listCapabilities` L34-50）。
#[utoipa::path(
    get,
    path = "/api/mcp/capabilities",
    tag = "mcp",
    params(
        ("domain" = Option<String>, Query, description = "按功能域过滤（空串视为未指定）"),
        ("enabled" = Option<bool>, Query, description = "仅在 domain 未指定且取 true 时生效")
    ),
    responses(
        (status = 200, description = "{capabilities:[…],total,enabledCount}（后两者取自整表）"),
        (status = 500, description = "enabled 不可识别（INTERNAL_ERROR，Spring 类型转换失败语义）")
    )
)]
#[allow(clippy::unused_async)] // axum handler 必须返回 Future
pub(crate) async fn list_capabilities(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    // 旧端的 `Boolean enabled` 绑定发生在方法体之前——不可识别值先于 domain 分支报错。
    let enabled = optional_spring_bool(&query, "enabled")?;
    let registry = &state.mcp_capabilities;
    let result = match query.get("domain").filter(|domain| !domain.is_empty()) {
        Some(domain) => registry.list_by_domain(domain),
        None if enabled == Some(true) => registry.list_enabled(),
        None => registry.list_all(),
    };
    let capabilities: Vec<Value> = result.iter().map(capability_view).collect();
    Ok(Json(json!({
        "capabilities": capabilities,
        "total": registry.size(),
        "enabledCount": registry.enabled_count(),
    })))
}

/// `GET /api/mcp/capabilities/domains`——功能域清单（旧 `listDomains`
/// L112-115）。
#[utoipa::path(
    get,
    path = "/api/mcp/capabilities/domains",
    tag = "mcp",
    responses((status = 200, description = "{domains:[…]}"))
)]
#[allow(clippy::unused_async)] // axum handler 必须返回 Future
pub(crate) async fn list_domains(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "domains": state.mcp_capabilities.list_domains() }))
}

/// `GET /api/mcp/capabilities/{id}`——单条定义（旧 `getCapability` L52-57）。
#[utoipa::path(
    get,
    path = "/api/mcp/capabilities/{id}",
    tag = "mcp",
    params(("id" = String, Path, description = "能力 id")),
    responses(
        (status = 200, description = "裸能力定义（null 字段省略）"),
        (status = 404, description = "id 不在注册表（空体）")
    )
)]
#[allow(clippy::unused_async)] // axum handler 必须返回 Future
pub(crate) async fn get_capability(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Response {
    match state.mcp_capabilities.find_by_id(&id) {
        Some(definition) => ok_json(capability_view(&definition)),
        None => not_found(),
    }
}

/// `PUT /api/mcp/capabilities/{id}`——整条替换（旧 `updateCapability`
/// L59-67）。
#[utoipa::path(
    put,
    path = "/api/mcp/capabilities/{id}",
    tag = "mcp",
    params(("id" = String, Path, description = "能力 id（同时作为入表键）")),
    responses(
        (status = 200, description = "替换后的裸定义"),
        (status = 400, description = "体缺失或不可解析（INVALID_REQUEST_BODY，见偏离 B4B-16）"),
        (status = 404, description = "id 不在注册表（空体）")
    )
)]
pub(crate) async fn update_capability(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let updated = parse_definition(&body, &id)?;
    validate_capability_definition(&updated).map_err(capability_security_error)?;
    match state.mcp_capabilities.update_capability(&id, updated) {
        Ok(definition) => Ok(ok_json(capability_view(&definition))),
        // 旧端 `catch (IllegalArgumentException)` 覆盖整段 → 一律 404 空体。
        Err(RegistryError::NotFound(_) | RegistryError::AlreadyExists(_)) => Ok(not_found()),
    }
}

/// `POST /api/mcp/capabilities`——新增（旧 `addCapability` L95-103）。
#[utoipa::path(
    post,
    path = "/api/mcp/capabilities",
    tag = "mcp",
    responses(
        (status = 201, description = "新增后的裸定义"),
        (status = 400, description = "id 重复（空体）/ 体缺失或不可解析（INVALID_REQUEST_BODY）")
    )
)]
pub(crate) async fn add_capability(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Response, ApiError> {
    if body.is_empty() {
        return Err(ApiError::invalid_request_body());
    }
    let definition: McpCapabilityDefinition =
        serde_json::from_slice(&body).map_err(|_| ApiError::invalid_request_body())?;
    validate_capability_definition(&definition).map_err(capability_security_error)?;
    match state.mcp_capabilities.add_capability(definition) {
        Ok(created) => Ok((StatusCode::CREATED, Json(capability_view(&created))).into_response()),
        Err(RegistryError::NotFound(_) | RegistryError::AlreadyExists(_)) => {
            Ok(bad_request_empty())
        }
    }
}

/// `DELETE /api/mcp/capabilities/{id}`——删除（旧 `deleteCapability`
/// L105-110）。
#[utoipa::path(
    delete,
    path = "/api/mcp/capabilities/{id}",
    tag = "mcp",
    params(("id" = String, Path, description = "能力 id")),
    responses(
        (status = 200, description = "{success:true}"),
        (status = 404, description = "id 不在注册表（空体）")
    )
)]
#[allow(clippy::unused_async)] // axum handler 必须返回 Future
pub(crate) async fn delete_capability(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Response {
    if state.mcp_capabilities.delete_capability(&id) {
        ok_json(json!({ "success": true }))
    } else {
        not_found()
    }
}

/// `PATCH /api/mcp/capabilities/{id}/toggle`——启停并联动服务器（旧
/// `toggleCapability` L69-93）。
#[utoipa::path(
    patch,
    path = "/api/mcp/capabilities/{id}/toggle",
    tag = "mcp",
    params(
        ("id" = String, Path, description = "能力 id"),
        ("enabled" = bool, Query, description = "必填；缺省 400，空串/不可识别 500")
    ),
    responses(
        (status = 200, description = "{id,enabled,status}（status = connected/disabled/…）"),
        (status = 400, description = "缺 enabled（MISSING_PARAMETER）"),
        (status = 404, description = "id 不在注册表（空体）"),
        (status = 500, description = "enabled 不可识别 / MCP 管理器未运行（INTERNAL_ERROR）")
    )
)]
pub(crate) async fn toggle_capability(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let enabled = require_spring_bool(&query, "enabled")?;
    let registry = Arc::clone(&state.mcp_capabilities);
    if enabled {
        let Some(definition) = registry.find_by_id(&id) else {
            return Ok(not_found());
        };
        tokio::time::timeout(
            Duration::from_secs(5),
            validate_capability_destination(&definition),
        )
        .await
        .map_err(|_| capability_security_error("MCP capability DNS validation timed out"))?
        .map_err(capability_security_error)?;
    }
    let Ok(definition) = registry.toggle_enabled(&id, enabled) else {
        return Ok(not_found());
    };
    let manager = state.mcp();
    let status = if enabled {
        let connection = manager
            .enable_from_registry(&definition)
            .await
            .map_err(|error| manager_error(&error))?;
        if connection.status() == McpConnectionStatus::Connected {
            "connected".to_owned()
        } else {
            connection.status().as_str().to_ascii_lowercase()
        }
    } else {
        // 同服务器多工具保护——仅当无其他启用能力共享该 serverKey 才摘除（怪癖 5）。
        let server_key = definition.extract_server_key();
        let other_enabled = registry
            .list_enabled()
            .iter()
            .any(|other| other.id != id && other.extract_server_key() == server_key);
        if !other_enabled {
            let _removed = manager.remove_server(&server_key).await;
        }
        "disabled".to_owned()
    };
    Ok(ok_json(
        json!({ "id": id, "enabled": enabled, "status": status }),
    ))
}

// ===== 诊断与调用端点 =====

/// `GET /api/mcp/capabilities/{id}/server-tools`——服务器实际工具清单（旧
/// `listServerTools` L117-147）。
#[utoipa::path(
    get,
    path = "/api/mcp/capabilities/{id}/server-tools",
    tag = "mcp",
    params(("id" = String, Path, description = "能力 id")),
    responses(
        (status = 200, description = "{id,serverKey,tools} / {id,serverKey,status:\"not_connected\"} / {id,status:\"error\",error}"),
        (status = 404, description = "id 不在注册表（空体）")
    )
)]
pub(crate) async fn list_server_tools(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Response {
    let Some(definition) = state.mcp_capabilities.find_by_id(&id) else {
        return not_found();
    };
    let server_key = definition.extract_server_key();
    let manager = state.mcp();
    let Some(connection) = manager
        .get_connection(&server_key)
        .filter(|connection| connection.is_alive())
    else {
        return ok_json(json!({
            "id": id,
            "serverKey": server_key,
            "status": "not_connected",
        }));
    };
    ensure_initialized(&connection).await;
    match connection
        .request(METHOD_TOOLS_LIST, Some(json!({})), MCP_REQUEST_TIMEOUT)
        .await
    {
        Ok(tools) => ok_json(json!({ "id": id, "serverKey": server_key, "tools": tools })),
        // 怪癖 3：错误分支不带 serverKey。
        Err(error) => ok_json(error_view(&id, &error.to_string())),
    }
}

/// `POST /api/mcp/capabilities/{id}/test`——临时连接连通性测试（旧
/// `testCapability` L149-170）。
#[utoipa::path(
    post,
    path = "/api/mcp/capabilities/{id}/test",
    tag = "mcp",
    params(("id" = String, Path, description = "能力 id")),
    responses(
        (status = 200, description = "{id,status:\"reachable\"|\"unreachable\",serverKey}"),
        (status = 404, description = "id 不在注册表（空体）")
    )
)]
pub(crate) async fn test_capability(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Response, ApiError> {
    let Some(definition) = state.mcp_capabilities.find_by_id(&id) else {
        return Ok(not_found());
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        validate_capability_destination(&definition),
    )
    .await
    .map_err(|_| capability_security_error("MCP capability DNS validation timed out"))?
    .map_err(capability_security_error)?;
    let config = state.mcp().build_resolved_config_from_registry(&definition);
    let connection = McpServerConnection::new(config);
    connection.connect().await;
    let alive = connection.is_alive();
    connection.close().await;
    let status = if alive { "reachable" } else { "unreachable" };
    Ok(ok_json(json!({
        "id": id,
        "status": status,
        "serverKey": definition.extract_server_key(),
    })))
}

/// `POST /api/mcp/capabilities/{id}/invoke`——legacy 直调入口失败关闭。
#[utoipa::path(
    post,
    path = "/api/mcp/capabilities/{id}/invoke",
    tag = "mcp",
    params(("id" = String, Path, description = "能力 id")),
    responses(
        (status = 403, description = "能力已关闭，或 legacy 直调被禁用；使用 /mcp tools/call"),
        (status = 400, description = "体缺失或不可解析（INVALID_REQUEST_BODY，见偏离 B4B-16）"),
        (status = 404, description = "id 不在注册表（空体）")
    )
)]
pub(crate) async fn invoke_capability(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    if body.is_empty() {
        return Err(ApiError::invalid_request_body());
    }
    // 旧端 `@RequestBody` 的反序列化先于方法体——体不可解析时不进 404 分支。
    let _payload: Value =
        serde_json::from_slice(&body).map_err(|_| ApiError::invalid_request_body())?;
    let Some(definition) = state.mcp_capabilities.find_by_id(&id) else {
        return Ok(not_found());
    };
    if !definition.enabled {
        return Err(direct_invoke_error(
            "MCP_CAPABILITY_DISABLED",
            "MCP capability is disabled",
        ));
    }
    // This legacy endpoint carries no authoritative session/run context and
    // therefore cannot enter Hook -> Admission -> ToolRegistry safely.  Fail
    // closed and direct callers to the reverse MCP endpoint, whose tools/call
    // path requires both identifiers and uses the production Admission stack.
    Err(direct_invoke_error(
        "MCP_DIRECT_INVOKE_DISABLED",
        "Direct capability invocation is disabled; use /mcp tools/call with session and run context",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition(id: &str) -> McpCapabilityDefinition {
        McpCapabilityDefinition::new(id)
    }

    #[test]
    fn capability_view_omits_null_fields_like_json_include_non_null() {
        let view = capability_view(&definition("mcp_weather"));
        let object = view.as_object().expect("object");
        assert_eq!(object.get("id"), Some(&json!("mcp_weather")));
        // 旧 record 带 @JsonInclude(NON_NULL)：未设置的字符串字段整键消失。
        assert!(!object.contains_key("name"), "{view}");
        assert!(!object.contains_key("sseUrl"), "{view}");
        // 基元字段不受 NON_NULL 影响，恒出现。
        assert_eq!(object.get("timeoutMs"), Some(&json!(0)));
        assert_eq!(object.get("enabled"), Some(&json!(false)));
        assert_eq!(object.get("videoCallEnabled"), Some(&json!(false)));
    }

    #[test]
    fn parse_definition_injects_url_id_when_body_omits_or_nulls_it() {
        let omitted = Bytes::from_static(br#"{"name":"Weather"}"#);
        let parsed = parse_definition(&omitted, "mcp_weather").expect("omitted id");
        assert_eq!(parsed.id, "mcp_weather");
        assert_eq!(parsed.name.as_deref(), Some("Weather"));

        let nulled = Bytes::from_static(br#"{"id":null,"enabled":true}"#);
        let parsed = parse_definition(&nulled, "mcp_maps").expect("null id");
        assert_eq!(parsed.id, "mcp_maps");
        assert!(parsed.enabled);

        // 体内 id 与 URL id 不一致时按体内值构造——入表键仍是 URL id（旧同）。
        let mismatched = Bytes::from_static(br#"{"id":"other"}"#);
        let parsed = parse_definition(&mismatched, "mcp_weather").expect("explicit id");
        assert_eq!(parsed.id, "other");
    }

    #[test]
    fn parse_definition_rejects_empty_and_malformed_bodies() {
        assert_eq!(
            parse_definition(&Bytes::new(), "x")
                .expect_err("empty")
                .status,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            parse_definition(&Bytes::from_static(b"not json"), "x")
                .expect_err("malformed")
                .status,
            StatusCode::BAD_REQUEST
        );
        // 非对象体：id 无从补齐 → 反序列化失败。
        assert_eq!(
            parse_definition(&Bytes::from_static(b"[]"), "x")
                .expect_err("array")
                .status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn error_view_carries_no_server_key() {
        let view = error_view("mcp_weather", "boom");
        assert_eq!(
            view,
            json!({ "id": "mcp_weather", "status": "error", "error": "boom" })
        );
    }

    #[test]
    fn disabled_status_uses_lowercased_java_enum_name() {
        // toggle 的 status 计算：非 CONNECTED 时取枚举名小写。
        assert_eq!(
            McpConnectionStatus::NeedsAuth.as_str().to_ascii_lowercase(),
            "needs_auth"
        );
        assert_eq!(
            McpConnectionStatus::Connected.as_str().to_ascii_lowercase(),
            "connected"
        );
    }
}
