//! MCP 服务器域端点——服务器管理 5 + 资源发现 2 + Prompt 2 + 重连 1（Batch 4B
//! Step 9）。
//!
//! 语义来源（旧仓库只读，逐字对照）：
//! `backend/src/main/java/com/aicodeassistant/controller/McpController.java`
//! （349 行）+ `mcp/McpClientManager.java`（`addServer` / `removeServer` /
//! `restartServer` / `getServerLogs`）+ `mcp/McpPromptAdapter.java`（194 行，
//! 参数校验与执行语义在本模块内联，见下）。
//!
//! # 端点逐条对照
//!
//! | 旧方法 | HTTP | 成功响应 | 失败 |
//! |---|---|---|---|
//! | `listServers` L32-35 | `GET /api/mcp/servers` | 200 `{"servers":[连接视图]}` | 无 |
//! | `addServer` L38-44 | `POST /api/mcp/servers` | **201** `{"name","status":"connecting"}` | 体缺失/非法 → 400 `INVALID_REQUEST_BODY`；管理器未运行 → 500 |
//! | `deleteServer` L47-51 | `DELETE /api/mcp/servers/{name}` | 200 `{"success":true}` | 无（不存在同样回 `true`） |
//! | `restartServer` L54-58 | `POST /api/mcp/servers/{name}/restart` | 200 `{"status":"connecting"}` | 不存在 → 400 `INVALID_REQUEST` `MCP server not found: …` |
//! | `getServerLogs` L61-66 | `GET /api/mcp/servers/{name}/logs?lines=100` | 200 `{"logs":[…]}` | `lines` 非法 → 500（`parse_spring_int` 语义） |
//! | `listResources` L78-124 | `GET /api/mcp/resources[?server=]` | 200 `{"resources":{server:[…]},"totalCount":n}` | — |
//! | `readResource` L133-164 | `GET /api/mcp/resources/read?uri=&server=` | 200 `{uri,serverName,content}` | 缺参 → 400 `MISSING_PARAMETER`；其余失败使用统一 `{code,message,requestId}` |
//! | `listPrompts` L176-231 | `GET /api/mcp/prompts[?server=]` | 200 `{"prompts":{server:[…]},"totalCount":n}` | — |
//! | `executePrompt` L239-310 | `POST /api/mcp/prompts/execute` | 200 `{success,serverName,promptName,messages}` | 名缺失/服务器缺失/未连接/校验失败 → 400；prompt 未找到 → **404** |
//! | `reconnectServer` L320-347 | `POST /api/mcp/reconnect?server=` | 200 `{serverName,status,success}` | 服务器缺失 → 400；重启失败 → 500 |
//!
//! # 兼容边界
//!
//! 1. 旧 `McpController` 的自持 `try/catch` 会返回多种裸 map；功能对齐契约明确
//!    覆盖该怪癖，所有非 2xx REST 响应统一走 [`ApiError`]。
//! 2. **`deleteServer` 恒回 `{"success": true}`**：旧实现丢弃
//!    `removeServer` 的 `boolean` 返回值，删不存在的服务器也报成功。
//! 3. **`getServerLogs` 的 `lines` 参数被无条件忽略**：旧
//!    `McpClientManager.getServerLogs(name, lines)` 返回固定 4 行状态快照，
//!    `lines` 不参与裁剪；服务器不存在时返回单元素列表
//!    `["MCP server not found: …"]` 而非报错。
//! 4. **`server` 查询参数用 `isEmpty()` 而非 `isBlank()` 判定**（旧 L83 /
//!    L181）：`?server=%20` 是「指定了名为一个空格的服务器」→ 查不到 → 空
//!    分组，而非回落到「全部已连接服务器」。
//! 5. **`readResource` / `listResources` 的字段缺省值不同**：`listResources`
//!    把 `description` 缺省成 `""`、`mimeType` 缺省成 `"unknown"`（旧 L101-102），
//!    而 `listServers` 回显的 `resources` 直接透传 `null`（Jackson 对 record
//!    的默认序列化）。两处形状不同是旧行为的一部分。
//!
//! # 偏离
//!
//! - **`B4B-02`**：分组对象（`resources` / `prompts` 的外层 map）键序为**字典
//!   序**而非旧 `LinkedHashMap` 的连接注册序——`serde_json` 未启用
//!   `preserve_order`，`Map` 内部即 `BTreeMap`。两个消费方（`mcpStore.ts` 的
//!   `fetchResources` / `fetchPrompts`）都是 `Object.entries` 灌进 `Map`，与
//!   键序无关。
//! - **`B4B-03`**：`POST /api/mcp/servers` 的体校验早于业务。旧 Jackson 对缺失
//!   `name` / `type` 填 `null`，随后在 `addServer` → `connect` 的 switch 上
//!   NPE → 500；Rust 侧两字段为必填，缺失即 400 `INVALID_REQUEST_BODY`。缺失
//!   `scope` 的语义**逐字保留**：旧 `config.scope() != null` 为假 → 不自动信任
//!   → 连接落 `NEEDS_AUTH`，Rust 侧缺省填 [`McpConfigScope::Dynamic`]，
//!   `McpClientManager::add_server` 内 `scope != Dynamic` 判定与之等价。
//! - **`B4B-04`**：`readResource` 旧有两条 catch（`McpProtocolException` →
//!   `"Failed to read resource: …"`；`Exception` → `"Unexpected error: …"`）。
//!   Rust 侧 `read_resource` 的错误类型只有 `McpProtocolError`，故仅第一条可达，
//!   第二条不移植（无可能触发的入口）。
//! - **`B4B-05`**：`listResources` / `listPrompts` 的顶层 catch（→ 500
//!   `"Failed to list …"`）与逐服务器 catch 在 Rust 侧均不可达：
//!   `discover_resources` / `list_prompts` 自身吞掉协议错误并返回空列表（与旧
//!   逐服务器 catch 的 `List.of()` 同效），无 `Result` 外泄。
//! - **`B4B-06`**：prompt 参数长度上限按 **UTF-16 码元**计（Java
//!   `String.length()` 语义，`encode_utf16().count()`），非字节数也非字符数。
//! - **`B4B-07`**：`executePrompt` 的 `arguments` 只接受字符串值，非字符串值
//!   （数字 / 布尔 / 嵌套）被**丢弃**。旧代码把 `Map<String,Object>` 无检查
//!   强转 `Map<String,String>`，非字符串值会在 `value.isBlank()` 处
//!   `ClassCastException` → 落外层 catch → 500 `"Failed to execute prompt: …"`。
//!   丢弃后必填参数会命中「missing or empty」校验（400），比 500 更贴合契约。

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value, json};
use zk_mcp::{
    ManagerError, McpClientManager, McpConfigScope, McpConnectionStatus, McpServerConfig,
    McpServerConnection, McpTransportType, PromptDefinition, PromptMessage, ResourceDefinition,
    ToolDefinition,
};

use crate::api::http_params::{parse_spring_int, require_param};
use crate::error::ApiError;
use crate::state::AppState;

/// 日志行数缺省（旧 `@RequestParam(defaultValue = "100") int lines`）。
const DEFAULT_LOG_LINES: i32 = 100;

/// prompt 参数值长度上限（旧 `McpPromptAdapter.MAX_ARGUMENT_VALUE_LENGTH`）。
const MAX_ARGUMENT_VALUE_LENGTH: usize = 10_000;

/// `POST /api/mcp/servers` 的请求体（旧 `@RequestBody McpServerConfig`）。
///
/// 字段与 [`McpServerConfig`] 一一对应，差别只在**缺省填充**：旧 Jackson 对
/// record 的缺失字段填 `null` / 保留 `null` 集合，此处以 `serde` 默认值复刻
/// （`scope` 的 `null` ⇔ [`McpConfigScope::Dynamic`]，见模块偏离 `B4B-03`）。
#[derive(Debug, Deserialize)]
struct AddServerRequest {
    /// 服务器名（唯一标识）。
    name: String,
    /// 传输类型（线上键名 `type`）。
    #[serde(rename = "type")]
    transport: McpTransportType,
    /// stdio 启动命令。
    #[serde(default)]
    command: Option<String>,
    /// stdio 命令参数。
    #[serde(default)]
    args: Vec<String>,
    /// 子进程环境变量。
    #[serde(default)]
    env: BTreeMap<String, String>,
    /// SSE / HTTP 服务 URL。
    #[serde(default)]
    url: Option<String>,
    /// HTTP 头。
    #[serde(default)]
    headers: BTreeMap<String, String>,
    /// 配置作用域（缺省 = 运行时动态注册，等价旧 `null`）。
    #[serde(default = "default_scope")]
    scope: McpConfigScope,
}

/// `scope` 缺省值——旧 `null` 的等价物（见模块偏离 `B4B-03`）。
const fn default_scope() -> McpConfigScope {
    McpConfigScope::Dynamic
}

impl AddServerRequest {
    /// 转为管理器可消费的配置。
    fn into_config(self) -> McpServerConfig {
        McpServerConfig {
            name: self.name,
            transport: self.transport,
            command: self.command,
            args: self.args,
            env: self.env,
            url: self.url,
            headers: self.headers,
            scope: self.scope,
        }
    }
}

/// `ManagerError` → REST 错误信封。
///
/// 旧五个服务器管理端点无 `try/catch`，异常经 `GlobalExceptionHandler`：
/// `IllegalArgumentException("MCP server not found: …")` → 400
/// `INVALID_REQUEST`（`GlobalExceptionHandler:107`）；三类
/// `IllegalStateException`（生命周期）无专门 handler → 落
/// `@ExceptionHandler(Exception.class)` → 500 `INTERNAL_ERROR`。
pub(crate) fn manager_error(error: &ManagerError) -> ApiError {
    match error {
        ManagerError::ServerNotFound(_) => ApiError::validation(error.to_string()),
        ManagerError::NotRunning
        | ManagerError::CannotRestartAfterShutdown
        | ManagerError::LifecycleChanged => {
            tracing::error!(%error, "MCP manager rejected REST request");
            ApiError::internal()
        }
    }
}

/// MCP REST 400 统一进入公共错误契约。
fn bad_request(message: &str) -> ApiError {
    ApiError::validation_with_code("MCP_REQUEST_FAILED", message)
}

/// MCP REST 500 统一进入公共错误契约，响应不暴露内部协议细节。
fn server_error(message: &str) -> ApiError {
    tracing::error!(%message, "MCP REST backend failure");
    ApiError::internal()
}

/// prompt 域同样使用统一 REST 错误结构。
fn bad_request_flagged(message: &str) -> ApiError {
    ApiError::validation_with_code("MCP_PROMPT_INVALID", message)
}

/// `McpServerConfig` 的线上回显。
///
/// 旧 Jackson 对 record 的默认序列化把缺省的 `command` / `url` 写成 `null`；
/// [`McpServerConfig`] 自身的 `Serialize` 带 `skip_serializing_if`（服务于
/// `mcp.json` 落盘），故此处手工铺开以保住旧形状。
fn config_view(config: &McpServerConfig) -> Value {
    const REDACTED: &str = "[REDACTED]";
    let env = config
        .env
        .keys()
        .map(|key| (key.clone(), REDACTED))
        .collect::<BTreeMap<_, _>>();
    let headers = config
        .headers
        .keys()
        .map(|key| (key.clone(), REDACTED))
        .collect::<BTreeMap<_, _>>();
    json!({
        "name": config.name,
        "type": config.transport.as_str(),
        "command": config.command,
        "args": config.args,
        "env": env,
        "url": config.url,
        "headers": headers,
        "scope": config.scope.as_str(),
    })
}

/// 工具定义回显（旧 `record McpToolDefinition(name, description, inputSchema)`）。
fn tool_view(tool: &ToolDefinition) -> Value {
    json!({
        "name": tool.name,
        "description": tool.description,
        "inputSchema": tool.input_schema,
    })
}

/// 资源定义回显（旧 `record McpResourceDefinition(uri, name, mimeType,
/// description)`——`null` 原样透传，与 `listResources` 的缺省填充不同）。
fn resource_view(resource: &ResourceDefinition) -> Value {
    json!({
        "uri": resource.uri,
        "name": resource.name,
        "mimeType": resource.mime_type,
        "description": resource.description,
    })
}

/// 连接视图——旧 `McpServerConnection` 被 Jackson 按 getter 序列化出的 7 个
/// 属性（`getConfig` / `getName` / `getStatus` / `getTools` / `getResources` /
/// `getReconnectAttempts` / `isAlive`；`getTransport` 无 getter 故不在内）。
fn connection_view(connection: &McpServerConnection) -> Value {
    json!({
        "name": connection.name(),
        "status": connection.status().as_str(),
        "config": config_view(connection.config()),
        "tools": connection.tools().iter().map(tool_view).collect::<Vec<_>>(),
        "resources": connection
            .resources()
            .iter()
            .map(resource_view)
            .collect::<Vec<_>>(),
        "reconnectAttempts": connection.reconnect_attempts(),
        "alive": connection.is_alive(),
    })
}

/// 待遍历的连接集合（旧 L82-88 / L180-186 完全同构的两段）。
///
/// `server` 非 `null` 且**非空串**（`isEmpty`，见怪癖 4）→ 精确取该连接（缺失
/// 则空集）；否则取全部**已连接**服务器。
fn selected_connections(
    manager: &Arc<McpClientManager>,
    server: Option<&str>,
) -> Vec<Arc<McpServerConnection>> {
    match server.filter(|name| !name.is_empty()) {
        Some(name) => manager
            .get_connection(name)
            .map(|connection| vec![connection])
            .unwrap_or_default(),
        None => manager.connected_servers(),
    }
}

/// `arguments` 抽取——只保留字符串值（见模块偏离 `B4B-07`）。
fn string_arguments(node: Option<&Value>) -> BTreeMap<String, String> {
    node.and_then(Value::as_object)
        .map(|object| {
            object
                .iter()
                .filter_map(|(key, value)| {
                    value.as_str().map(|text| (key.clone(), text.to_owned()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// prompt 参数校验（旧 `McpPromptAdapter.validateArguments` L70-87 逐字移植）。
///
/// 必填缺失/空白优先报错并 `continue`（同一参数不再报长度）；长度上限按 UTF-16
/// 码元计（见模块偏离 `B4B-06`）。
fn validate_arguments(
    definition: &PromptDefinition,
    arguments: &BTreeMap<String, String>,
) -> Vec<String> {
    let mut errors = Vec::new();
    for argument in &definition.arguments {
        let value = arguments.get(&argument.name);
        if argument.required && value.is_none_or(|text| text.trim().is_empty()) {
            errors.push(format!(
                "Required argument '{}' is missing or empty",
                argument.name
            ));
            continue;
        }
        if value.is_some_and(|text| text.encode_utf16().count() > MAX_ARGUMENT_VALUE_LENGTH) {
            errors.push(format!(
                "Argument '{}' exceeds maximum length of {MAX_ARGUMENT_VALUE_LENGTH}",
                argument.name
            ));
        }
    }
    errors
}

/// prompt 消息回显（旧 `Map.of("role", …, "content", …)`）。
fn prompt_messages(messages: &[PromptMessage]) -> Vec<Value> {
    messages
        .iter()
        .map(|message| json!({ "role": message.role, "content": message.content }))
        .collect()
}

// ===== 服务器管理端点 =====

/// `GET /api/mcp/servers`——列出全部连接（旧 `listServers` L32-35）。
#[utoipa::path(
    get,
    path = "/api/mcp/servers",
    tag = "mcp",
    responses(
        (status = 200, description = "{servers:[{name,status,config,tools,resources,reconnectAttempts,alive}]}")
    )
)]
#[allow(clippy::unused_async)] // axum handler 必须返回 Future
pub(crate) async fn list_servers(State(state): State<AppState>) -> Json<Value> {
    let servers: Vec<Value> = state
        .mcp()
        .list_connections()
        .iter()
        .map(|connection| connection_view(connection))
        .collect();
    Json(json!({ "servers": servers }))
}

/// `POST /api/mcp/servers`——注册并连接新服务器（旧 `addServer` L38-44）。
#[utoipa::path(
    post,
    path = "/api/mcp/servers",
    tag = "mcp",
    responses(
        (status = 201, description = "{name,status:\"connecting\"}"),
        (status = 400, description = "体缺失或缺 name/type（INVALID_REQUEST_BODY）"),
        (status = 500, description = "管理器未运行 / 生命周期已变更（INTERNAL_ERROR）")
    )
)]
pub(crate) async fn add_server(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if body.is_empty() {
        return Err(ApiError::invalid_request_body());
    }
    let request: AddServerRequest =
        serde_json::from_slice(&body).map_err(|_| ApiError::invalid_request_body())?;
    let config = request.into_config();
    let name = config.name.clone();
    state
        .mcp()
        .add_server(config)
        .await
        .map_err(|error| manager_error(&error))?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "name": name, "status": "connecting" })),
    ))
}

/// `DELETE /api/mcp/servers/{name}`——摘除服务器（旧 `deleteServer` L47-51；
/// 返回值被丢弃，见怪癖 2）。
#[utoipa::path(
    delete,
    path = "/api/mcp/servers/{name}",
    tag = "mcp",
    params(("name" = String, Path, description = "MCP 服务器名")),
    responses((status = 200, description = "{success:true}（服务器不存在同样为 true）"))
)]
pub(crate) async fn delete_server(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Json<Value> {
    let _removed = state.mcp().remove_server(&name).await;
    Json(json!({ "success": true }))
}

/// `POST /api/mcp/servers/{name}/restart`——重启服务器（旧 `restartServer`
/// L54-58）。
#[utoipa::path(
    post,
    path = "/api/mcp/servers/{name}/restart",
    tag = "mcp",
    params(("name" = String, Path, description = "MCP 服务器名")),
    responses(
        (status = 200, description = "{status:\"connecting\"}"),
        (status = 400, description = "服务器不存在（INVALID_REQUEST + `MCP server not found: …`）"),
        (status = 500, description = "管理器未运行 / 已关停（INTERNAL_ERROR）")
    )
)]
pub(crate) async fn restart_server(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    state
        .mcp()
        .restart_server(&name)
        .await
        .map_err(|error| manager_error(&error))?;
    Ok(Json(json!({ "status": "connecting" })))
}

/// `GET /api/mcp/servers/{name}/logs`——状态快照（旧 `getServerLogs` L61-66；
/// `lines` 被忽略，见怪癖 3）。
#[utoipa::path(
    get,
    path = "/api/mcp/servers/{name}/logs",
    tag = "mcp",
    params(
        ("name" = String, Path, description = "MCP 服务器名"),
        ("lines" = Option<i32>, Query, description = "旧端接收但不使用；缺省 100")
    ),
    responses(
        (status = 200, description = "{logs:[…]}（服务器不存在 → 单元素说明行）"),
        (status = 500, description = "lines 非法（INTERNAL_ERROR，Spring 类型转换失败语义）")
    )
)]
#[allow(clippy::unused_async)] // axum handler 必须返回 Future
pub(crate) async fn server_logs(
    State(state): State<AppState>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let lines = parse_spring_int(query.get("lines").map(String::as_str), DEFAULT_LOG_LINES)?;
    let logs = state
        .mcp()
        .server_logs(&name, usize::try_from(lines).unwrap_or(0));
    Ok(Json(json!({ "logs": logs })))
}

// ===== 资源发现端点 =====

/// `GET /api/mcp/resources`——按服务器分组的资源清单（旧 `listResources`
/// L78-124）。
#[utoipa::path(
    get,
    path = "/api/mcp/resources",
    tag = "mcp",
    params(("server" = Option<String>, Query, description = "仅列出该服务器（空串视为已指定，见怪癖 4）")),
    responses(
        (status = 200, description = "{resources:{server:[{uri,name,description,mimeType,serverName}]},totalCount}")
    )
)]
pub(crate) async fn list_resources(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let manager = state.mcp();
    let connections = selected_connections(&manager, query.get("server").map(String::as_str));
    let mut grouped = JsonMap::new();
    let mut total_count = 0usize;
    for connection in connections {
        let resources = connection.discover_resources().await;
        let entries: Vec<Value> = resources
            .iter()
            .map(|resource| {
                json!({
                    "uri": resource.uri,
                    "name": resource.name,
                    "description": resource.description.clone().unwrap_or_default(),
                    "mimeType": resource
                        .mime_type
                        .clone()
                        .unwrap_or_else(|| "unknown".to_owned()),
                    "serverName": connection.name(),
                })
            })
            .collect();
        total_count += entries.len();
        grouped.insert(connection.name().to_owned(), Value::Array(entries));
    }
    Json(json!({ "resources": grouped, "totalCount": total_count }))
}

/// `GET /api/mcp/resources/read`——读取单个资源（旧 `readResource` L133-164）。
#[utoipa::path(
    get,
    path = "/api/mcp/resources/read",
    tag = "mcp",
    params(
        ("uri" = String, Query, description = "资源 URI（必填）"),
        ("server" = String, Query, description = "MCP 服务器名（必填）")
    ),
    responses(
        (status = 200, description = "{uri,serverName,content}"),
        (status = 400, description = "统一错误响应：缺参或服务器缺失/未连接"),
        (status = 500, description = "统一错误响应：协议失败")
    )
)]
pub(crate) async fn read_resource(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let uri = require_param(&query, "uri")?.to_owned();
    let server = require_param(&query, "server")?.to_owned();
    let manager = state.mcp();
    let Some(connection) = manager.get_connection(&server) else {
        return Err(bad_request(&format!("MCP server not found: {server}")));
    };
    let status = connection.status();
    if status != McpConnectionStatus::Connected {
        return Err(bad_request(&format!(
            "MCP server '{server}' not connected (status: {status})"
        )));
    }
    match connection.read_resource(&uri).await {
        Ok(content) => Ok((
            StatusCode::OK,
            Json(json!({ "uri": uri, "serverName": server, "content": content })),
        )),
        Err(error) => {
            tracing::error!(%uri, %server, %error, "Failed to read MCP resource");
            Err(server_error(&format!("Failed to read resource: {error}")))
        }
    }
}

// ===== Prompt 端点 =====

/// `GET /api/mcp/prompts`——按服务器分组的 prompt 清单（旧 `listPrompts`
/// L176-231）。
#[utoipa::path(
    get,
    path = "/api/mcp/prompts",
    tag = "mcp",
    params(("server" = Option<String>, Query, description = "仅列出该服务器（空串视为已指定，见怪癖 4）")),
    responses(
        (status = 200, description = "{prompts:{server:[{name,description,serverName,arguments:[{name,description,required}]}]},totalCount}")
    )
)]
pub(crate) async fn list_prompts(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let manager = state.mcp();
    let connections = selected_connections(&manager, query.get("server").map(String::as_str));
    let mut grouped = JsonMap::new();
    let mut total_count = 0usize;
    for connection in connections {
        let prompts = connection.list_prompts().await;
        let entries: Vec<Value> = prompts
            .iter()
            .map(|prompt| {
                json!({
                    "name": prompt.name,
                    "description": prompt.description,
                    "serverName": connection.name(),
                    "arguments": prompt
                        .arguments
                        .iter()
                        .map(|argument| json!({
                            "name": argument.name,
                            "description": argument.description,
                            "required": argument.required,
                        }))
                        .collect::<Vec<_>>(),
                })
            })
            .collect();
        total_count += entries.len();
        grouped.insert(connection.name().to_owned(), Value::Array(entries));
    }
    Json(json!({ "prompts": grouped, "totalCount": total_count }))
}

/// `POST /api/mcp/prompts/execute`——渲染 prompt 模板（旧 `executePrompt`
/// L239-310 + `McpPromptAdapter.execute` L96-159）。
///
/// 执行期的协议失败**不**升级为 HTTP 错误：旧 adapter 把 `McpProtocolException`
/// 转成单条 `system` 消息后照常返回，controller 仍回 200 `success: true`
/// （旧 L296 的返回值直接进 `messages`）。
#[utoipa::path(
    post,
    path = "/api/mcp/prompts/execute",
    tag = "mcp",
    responses(
        (status = 200, description = "{success:true,serverName,promptName,messages:[{role,content}]}"),
        (status = 400, description = "统一错误响应：名缺失 / 服务器缺失或未连接 / 参数校验失败"),
        (status = 404, description = "统一错误响应：服务器上无该 prompt")
    )
)]
pub(crate) async fn execute_prompt(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if body.is_empty() {
        return Err(ApiError::invalid_request_body());
    }
    let payload: Value =
        serde_json::from_slice(&body).map_err(|_| ApiError::invalid_request_body())?;
    let prompt_name = payload
        .get("promptName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let server_name = payload
        .get("server")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    if prompt_name.trim().is_empty() {
        return Err(bad_request_flagged("Prompt name is required"));
    }
    if server_name.trim().is_empty() {
        return Err(bad_request_flagged("Server name is required"));
    }
    let manager = state.mcp();
    let Some(connection) = manager.get_connection(&server_name) else {
        return Err(bad_request_flagged(&format!(
            "MCP server not found: {server_name}"
        )));
    };
    if connection.status() != McpConnectionStatus::Connected {
        return Err(bad_request_flagged(&format!(
            "MCP server '{server_name}' not connected"
        )));
    }
    let Some(definition) = connection
        .list_prompts()
        .await
        .into_iter()
        .find(|prompt| prompt.name == prompt_name)
    else {
        return Err(ApiError::not_found(
            "MCP_PROMPT_NOT_FOUND",
            &format!("Prompt '{prompt_name}' not found on server '{server_name}'"),
        ));
    };
    let arguments = string_arguments(payload.get("arguments"));
    let validation_errors = validate_arguments(&definition, &arguments);
    if !validation_errors.is_empty() {
        return Err(ApiError::validation_with_code(
            "MCP_PARAMETER_VALIDATION_FAILED",
            &format!(
                "Parameter validation failed: {}",
                validation_errors.join("; ")
            ),
        ));
    }
    let messages = match connection.get_prompt(&prompt_name, &arguments).await {
        Ok(messages) => messages,
        Err(error) => {
            tracing::error!(
                prompt = %prompt_name,
                server = %server_name,
                %error,
                "Failed to execute MCP prompt"
            );
            vec![PromptMessage {
                role: "system".to_owned(),
                content: format!("Error executing prompt '{prompt_name}': {error}"),
            }]
        }
    };
    Ok((
        StatusCode::OK,
        Json(json!({
            "success": true,
            "serverName": server_name,
            "promptName": prompt_name,
            "messages": prompt_messages(&messages),
        })),
    ))
}

// ===== 重连端点 =====

/// `POST /api/mcp/reconnect`——重连并回读状态（旧 `reconnectServer` L320-347）。
///
/// `success` 取**重启后**的状态是否为 `CONNECTED`——非阻塞重连下多为 `PENDING`，
/// 故前端 `mcpStore.reconnectServer` 常收到 `false`，这是旧行为。
#[utoipa::path(
    post,
    path = "/api/mcp/reconnect",
    tag = "mcp",
    params(("server" = String, Query, description = "MCP 服务器名（必填）")),
    responses(
        (status = 200, description = "{serverName,status,success}"),
        (status = 400, description = "统一错误响应：缺参 / 服务器缺失"),
        (status = 500, description = "统一错误响应：重启失败")
    )
)]
pub(crate) async fn reconnect_server(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let server = require_param(&query, "server")?.to_owned();
    let manager = state.mcp();
    if manager.get_connection(&server).is_none() {
        return Err(bad_request(&format!("MCP server not found: {server}")));
    }
    tracing::info!(%server, "REST API: reconnecting MCP server");
    if let Err(error) = manager.restart_server(&server).await {
        tracing::error!(%server, %error, "Failed to reconnect MCP server");
        return Err(server_error(&format!("Failed to reconnect: {error}")));
    }
    let status = manager
        .get_connection(&server)
        .map_or(McpConnectionStatus::Failed, |connection| {
            connection.status()
        });
    Ok((
        StatusCode::OK,
        Json(json!({
            "serverName": server,
            "status": status.as_str(),
            "success": status == McpConnectionStatus::Connected,
        })),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `arguments` 只取字符串值（见模块偏离 `B4B-07`）。
    #[test]
    fn string_arguments_drops_non_string_values() {
        let payload = json!({ "a": "x", "b": 1, "c": null, "d": ["y"] });
        let arguments = string_arguments(Some(&payload));
        assert_eq!(arguments.len(), 1);
        assert_eq!(arguments.get("a").map(String::as_str), Some("x"));
        assert!(string_arguments(None).is_empty());
        assert!(string_arguments(Some(&json!("not an object"))).is_empty());
    }

    /// 必填缺失优先报错并跳过长度检查（旧 `continue` 语义）。
    #[test]
    fn validation_reports_missing_before_length() {
        let definition = PromptDefinition {
            name: "p".to_owned(),
            description: String::new(),
            arguments: vec![
                zk_mcp::PromptArgument {
                    name: "required".to_owned(),
                    description: String::new(),
                    required: true,
                },
                zk_mcp::PromptArgument {
                    name: "optional".to_owned(),
                    description: String::new(),
                    required: false,
                },
            ],
        };
        let mut arguments = BTreeMap::new();
        // 全空白等价缺失（旧 `isBlank()`）。
        arguments.insert("required".to_owned(), "   ".to_owned());
        let errors = validate_arguments(&definition, &arguments);
        assert_eq!(
            errors,
            vec!["Required argument 'required' is missing or empty".to_owned()]
        );
    }

    /// 长度上限按 UTF-16 码元计（见模块偏离 `B4B-06`）。
    #[test]
    fn length_limit_counts_utf16_units() {
        let definition = PromptDefinition {
            name: "p".to_owned(),
            description: String::new(),
            arguments: vec![zk_mcp::PromptArgument {
                name: "text".to_owned(),
                description: String::new(),
                required: false,
            }],
        };
        // 每个 emoji 占 2 个 UTF-16 码元 → 5000 个恰好命中上限（不超）。
        let mut arguments = BTreeMap::new();
        arguments.insert(
            "text".to_owned(),
            "😀".repeat(MAX_ARGUMENT_VALUE_LENGTH / 2),
        );
        assert!(validate_arguments(&definition, &arguments).is_empty());
        arguments.insert(
            "text".to_owned(),
            "😀".repeat(MAX_ARGUMENT_VALUE_LENGTH / 2 + 1),
        );
        assert_eq!(
            validate_arguments(&definition, &arguments),
            vec!["Argument 'text' exceeds maximum length of 10000".to_owned()]
        );
    }

    /// 配置回显保住旧 Jackson 的 `null` 字段（不 skip）。
    #[test]
    fn config_view_keeps_null_command_and_url() {
        let config = McpServerConfig::stdio("srv", "node", vec!["a.js".to_owned()]);
        let view = config_view(&config);
        assert_eq!(view["type"], json!("STDIO"));
        assert_eq!(view["scope"], json!("LOCAL"));
        assert_eq!(view["url"], Value::Null);
        assert!(view.get("url").is_some());

        let sse = McpServerConfig::sse("remote", "https://example.test/sse");
        let view = config_view(&sse);
        assert_eq!(view["command"], Value::Null);
        assert_eq!(view["type"], json!("SSE"));
        assert_eq!(view["scope"], json!("USER"));
    }

    /// 管理端点可以回显已配置的键名，但绝不能泄漏环境变量或请求头的值。
    #[test]
    fn config_view_redacts_environment_and_header_values() {
        let mut config = McpServerConfig::sse("remote", "https://example.test/sse");
        config
            .env
            .insert("MCP_API_KEY".to_owned(), "env-secret-value".to_owned());
        config.headers.insert(
            "Authorization".to_owned(),
            "Bearer header-secret-value".to_owned(),
        );

        let view = config_view(&config);
        assert_eq!(view["env"]["MCP_API_KEY"], json!("[REDACTED]"));
        assert_eq!(view["headers"]["Authorization"], json!("[REDACTED]"));
        let serialized = view.to_string();
        assert!(!serialized.contains("env-secret-value"));
        assert!(!serialized.contains("header-secret-value"));
    }

    /// 缺省 `scope` 落 `DYNAMIC`（旧 `null` 的等价物，见模块偏离 `B4B-03`）。
    #[test]
    fn add_server_request_defaults_scope_to_dynamic() {
        let request: AddServerRequest =
            serde_json::from_str(r#"{"name":"s","type":"STDIO","command":"node"}"#).unwrap();
        let config = request.into_config();
        assert_eq!(config.scope, McpConfigScope::Dynamic);
        assert_eq!(config.transport, McpTransportType::Stdio);
        assert!(config.args.is_empty());
        assert!(config.url.is_none());
    }

    /// 缺 `type` → 反序列化失败（Rust 侧 400，见模块偏离 `B4B-03`）。
    #[test]
    fn add_server_request_requires_name_and_type() {
        assert!(serde_json::from_str::<AddServerRequest>(r#"{"name":"s"}"#).is_err());
        assert!(serde_json::from_str::<AddServerRequest>(r#"{"type":"STDIO"}"#).is_err());
    }

    /// `ManagerError` 的状态码分档（400 只给 `ServerNotFound`）。
    #[test]
    fn manager_error_maps_not_found_to_bad_request() {
        let error = manager_error(&ManagerError::ServerNotFound("srv".to_owned()));
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            manager_error(&ManagerError::NotRunning).status,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
