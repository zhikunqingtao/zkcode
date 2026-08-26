//! 上行分发——壳解析（错误三分类）、bind 握手、应用层心跳与 S9 挂点路由。
//!
//! 解析分两步（`envelope` 模块「备选回退」策略的落地）：
//! 1. 解 `serde_json::Value` 取顶层 `type`；
//! 2. 已知 type 才二次解 [`ClientEnvelope`]——错误因此可三分类：
//!    - 非 JSON / 缺 `type` 字段 → `MALFORMED_MESSAGE`；
//!    - `type` 不在 16 variant → `UNKNOWN_MESSAGE_TYPE:<原 type>`（含原串，
//!      任务规格 §5）；
//!    - 已知 type 但字段坏 → `INVALID_MESSAGE`。
//!
//! 三者均回 `protocol_error` **直发**（旧 `pushBindError` / `pushToPrincipal`
//! 语义，不带路由字段）。
//!
//! # bind 握手校验链（对齐旧 `handleBindSession` L1571-1726 顺序）
//!
//! `UPGRADE_REQUIRED`（协议版本过旧）→ `BIND_REQUEST_ID_REQUIRED` →
//! `BINDING_EPOCH_REQUIRED`（<1）→ `SESSION_NOT_FOUND` / `BIND_RECOVERY_FAILED`
//! （DB 读失败）→ `STALE_BINDING_EPOCH`（hub 单调校验）→ 成功：
//! `session_restored` 直发 + pending 按序重放。
//!
//! # `slash_command` 分发（Batch 3）
//!
//! `/skill` 保持 3B.7 的独立 PROMPT 注入路径（见 [`handle_skill_command`]）；
//! 其余命令交 [`crate::command::CommandRegistry`]（见
//! [`handle_slash_command`]）。
//!
//! Batch 8A 起注册表内亦有 `PROMPT` 命令（`/git-review`、`/init`、`/retry`），
//! 由 [`handle_slash_command`] 的 PROMPT 分支处理：先推
//! `command_result{resultType:"prompt"}`（正文不下行），再把提示词按
//! `user_message` 注入引擎——与 `/skill` 同一范式。

//! # `mcp_operation` 分发（Batch 4B Step 10）
//!
//! 四操作 connect / disconnect / refresh / list 对照旧
//! `WebSocketController.handleMcpOperation`（L1449-1503）；未知操作回
//! `INVALID_MCP_OP`，下游异常回 `MCP_ERROR`（见 [`handle_mcp_operation`]）。

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use zk_authz::interaction::{InteractionStatus, InteractionType};
use zk_authz::model::PermissionMode;
use zk_protocol::{ClientEnvelope, ClientMessage, McpToolInfo, ServerMessage};

use super::metrics as ws_metrics;
use super::restore;
use super::{BindError, WS_PROTOCOL_VERSION};
use crate::command::{CommandContext, CommandResult, CommandType};
use crate::interaction::DurableInteractionService;
use crate::iso::now_millis;
use crate::state::AppState;

/// 15 个已知上行 type（两步解析的壳判定集；与 `ClientMessage::kind()` 输出域
/// 一致，单测互锁防漂移）。
const KNOWN_CLIENT_TYPES: [&str; 15] = [
    "user_message",
    "run_input",
    "permission_response",
    "interrupt",
    "set_model",
    "set_permission_mode",
    "slash_command",
    "mcp_operation",
    "rewind_files",
    "elicitation_response",
    "ping",
    "bind_session",
    "interaction_ack",
    "activity_save",
    "activity_update",
];

/// 读循环文本帧入口：两步解析 + 分发。
pub(crate) async fn handle_text(state: &AppState, conn_id: &str, text: &str) {
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(err) => {
            tracing::debug!(conn_id, error = %err, "inbound frame is not json");
            reply_protocol_error(state, conn_id, "MALFORMED_MESSAGE".to_owned(), None, None);
            return;
        }
    };
    let Some(kind) = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
    else {
        tracing::debug!(conn_id, "inbound frame has no type field");
        reply_protocol_error(state, conn_id, "MALFORMED_MESSAGE".to_owned(), None, None);
        return;
    };
    if !KNOWN_CLIENT_TYPES.contains(&kind.as_str()) {
        tracing::debug!(conn_id, kind = %kind, "unknown inbound type");
        reply_protocol_error(
            state,
            conn_id,
            format!("UNKNOWN_MESSAGE_TYPE:{kind}"),
            None,
            None,
        );
        return;
    }
    let envelope: ClientEnvelope = match serde_json::from_value(value) {
        Ok(envelope) => envelope,
        Err(err) => {
            tracing::debug!(conn_id, kind = %kind, error = %err, "invalid inbound payload");
            reply_protocol_error(state, conn_id, "INVALID_MESSAGE".to_owned(), None, None);
            return;
        }
    };
    ws_metrics::count_message("in", envelope.kind());
    dispatch(state, conn_id, envelope.msg).await;
}

/// 分发：本地处理 `ping` / `bind_session` / `interaction_ack` / `set_model` /
/// `permission_response` / `set_permission_mode` 六类 + `slash_command`
///（`/skill` 走 3B.7 的 PROMPT 注入，其余走 Batch 3 的命令注册表），
/// 其余已绑定转发 `EngineHook`（S9 挂点），未绑定 debug 丢弃。
async fn dispatch(state: &AppState, conn_id: &str, msg: ClientMessage) {
    match msg {
        ClientMessage::Ping => handle_ping(state, conn_id).await,
        ClientMessage::BindSession {
            session_id,
            bind_request_id,
            binding_epoch,
            protocol_version,
        } => {
            handle_bind(
                state,
                conn_id,
                &session_id,
                &bind_request_id,
                binding_epoch,
                protocol_version,
            )
            .await;
        }
        ClientMessage::InteractionAck {
            interaction_id,
            delivery_generation,
        } => {
            handle_interaction_ack(state, conn_id, &interaction_id, delivery_generation).await;
        }
        ClientMessage::SetModel { model } => handle_set_model(state, conn_id, &model).await,
        ClientMessage::PermissionResponse { .. } => {
            handle_permission_response(state, conn_id).await;
        }
        ClientMessage::SetPermissionMode { mode } => {
            handle_set_permission_mode(state, conn_id, &mode).await;
        }
        // 3B.7：`/skill` 在服务端本地渲染为提示词后注入对话（旧 PROMPT 命令
        // 路径）。
        ClientMessage::SlashCommand { command, args } if command == SKILL_COMMAND => {
            handle_skill_command(state, conn_id, &args).await;
        }
        // Batch 3：其余 slash 命令由命令注册表在服务端本地执行（旧
        // `handleSlashCommand` 的非 PROMPT 路径），不再转发引擎。
        ClientMessage::SlashCommand { command, args } => {
            handle_slash_command(state, conn_id, &command, &args).await;
        }
        // Batch 4B：MCP 服务器操作在服务端本地执行（旧 `handleMcpOperation`）。
        ClientMessage::McpOperation {
            operation,
            server_id,
            config: _,
        } => {
            handle_mcp_operation(state, conn_id, &operation, &server_id).await;
        }
        other => match state.hub.bound_session(conn_id) {
            Some((session_id, _epoch)) => state.hub.dispatch_to_engine(&session_id, other),
            None => {
                tracing::debug!(
                    conn_id,
                    kind = other.kind(),
                    "non-channel inbound dropped (session not bound)"
                );
            }
        },
    }
}

/// `/skill` 命令名（旧 `SkillCommand.getName()`）。
const SKILL_COMMAND: &str = "skill";

/// MCP 操作通知的自动消失毫秒数（旧三处 `"timeout", 3000`）。
const MCP_NOTIFICATION_TIMEOUT_MS: i64 = 3000;

/// `/skill <name> [args]`——解析技能、渲染模板并注入对话（3B.7）。
///
/// 语义来源（旧仓库只读，`581d407b`）：`command/impl/SkillCommand.execute`
///（用法校验 → `split("\\s+", 2)` 切名与参数 → `resolve` → `parseArgs` +
/// `renderTemplate`）+ `WebSocketController.handleSlashCommand` 的 PROMPT
/// 分支（L1360-1402：先推 `command_result{resultType:"prompt"}` 简洁通知，
/// 再把渲染结果当用户文本注入 LLM）。
///
/// 与旧实现的刻意差异（3B.7 范围外，留痕）：
/// - `allowedTools` / `model` 覆盖不生效——旧 `executePromptCommand` 用它们
///   过滤工具池与覆盖模型，属引擎侧能力；本任务不改 zk-engine，故 frontmatter
///   的 `allowed-tools` / `model` 仅解析入模型，暂不参与派发；
/// - 并发保护不在此处做：注入走 `UserMessage`，由 zk-engine 既有 CAS 门控
///   （`query_busy`，retryable=false）承担，与旧 `sessionQueryRunning` 同语义;
/// - `context: fork` 不分叉子会话（任务明确「先 inline」）。
async fn handle_skill_command(state: &AppState, conn_id: &str, args: &str) {
    let Some((session_id, _epoch)) = state.hub.bound_session(conn_id) else {
        tracing::debug!(conn_id, "slash_command /skill dropped (session not bound)");
        return;
    };
    // 旧 `SkillCommand.execute` L38-41：空参数即用法提示。
    let args = args.trim();
    if args.is_empty() {
        push_command_error(state, &session_id, "用法: /skill <技能名称> [参数]").await;
        return;
    }
    // 旧 `args.trim().split("\s+", 2)`：首个空白串整体作分隔（故 rest 需
    // `trim_start` 吃掉剩余空白，与 Java `\s+` 的贪婪消费一致）。
    let (name, skill_args) = match args.find(char::is_whitespace) {
        Some(idx) => (&args[..idx], args[idx..].trim_start()),
        None => (args, ""),
    };
    let Some(skill) = state.skills.resolve(name) else {
        // 旧 L50-56 文案逐字（含「可用技能:」清单与空清单的「无」）。
        let available = state
            .skills
            .all_skills()
            .iter()
            .map(|skill| skill.effective_name().to_owned())
            .collect::<Vec<_>>();
        let available = if available.is_empty() {
            "无".to_owned()
        } else {
            available.join(", ")
        };
        push_command_error(
            state,
            &session_id,
            &format!("技能未找到: {name}。可用技能: {available}"),
        )
        .await;
        return;
    };
    let params = skill.parse_args(skill_args);
    let rendered = skill.render_template(&params);
    tracing::info!(
        session_id = %session_id,
        skill = skill.effective_name(),
        source = skill.source.as_str(),
        rendered_len = rendered.len(),
        "slash_command /skill rendered"
    );
    // 旧 L1364-1367：只推命令名与结果类型，完整提示词不下行。
    state
        .hub
        .push(
            &session_id,
            ServerMessage::CommandResult {
                command: SKILL_COMMAND.to_owned(),
                result_type: "prompt".to_owned(),
                output: None,
                data: None,
            },
        )
        .await;
    // 旧 L1388-1392 → `executePromptCommand` → `executeQueryInternal`：渲染
    // 结果即 `userText`，与普通用户消息同一执行路径（含持久化与并发门控）。
    state.hub.dispatch_to_engine(
        &session_id,
        ClientMessage::UserMessage {
            text: rendered,
            attachments: None,
            references: None,
        },
    );
}

/// slash 命令本地执行与下行映射（旧 `handleSlashCommand` L1340-1443）。
///
/// 顺序逐字对照旧实现：
/// 1. 会话未绑定 → 丢弃（旧 `sessionId == null` 直接 return + warn）；
/// 2. `commandRegistry.getCommand` 未命中 → `COMMAND_NOT_FOUND`；
/// 3. `CommandContext.of(sessionId, requireSessionWorkingDirectory(sessionId), …)`；
/// 4. `execute` → 按结果类型下行；
/// 5. 成功（`isSuccess()`，含 `SKIP`）后追推 `session_list_updated`
///    ——旧注释「斜杠命令可能变更会话列表（如 `/clear` 创建新会话）」。
///
/// 结果类型映射（旧 switch L1348-1427）：
///
/// | 结果 | 下行 |
/// |---|---|
/// | `TEXT` | `command_result{command, resultType:"text", output}` |
/// | `JSX` | `command_result{command, resultType:"jsx", data}` |
/// | `COMPACT` | `compact_complete`（载荷差异见下） |
/// | `SKIP` | 无动作 |
/// | `ERROR` | `error{code:"COMMAND_ERROR", retryable:false}` |
///
/// # 与旧实现的刻意差异（留痕）
///
/// 1. **`COMMAND_NOT_FOUND` 的文案**：旧 catch 块丢弃异常携带的模糊建议，
///    只推 `"Unknown command: /<name>"`；此处推
///    [`crate::command::CommandRegistry::get_command`] 的完整消息（含
///    `Did you mean: …?`）——旧 `suggestCommands` 的计算结果在旧 WS 路径上被
///    白算，是旧侧缺陷。
/// 2. **`COMPACT` 的载荷键**：旧推 `{displayText, compactionData}`；zkcode 的
///    `compact_complete` 是**冻结协议**（S4 契约冻结点），载荷为
///    `{summary, tokensSaved}`。故 `displayText` → `summary`，`tokensSaved`
///    取压缩元数据的 `savedTokens` 键（同一数值，键名随协议）。
/// 3. **PROMPT 分支的并发门控与工具/模型覆盖**：Batch 8A 起本域有 `PROMPT`
///    命令，`TEXT` 分支按命令类型二分（旧 L1359-1409 同构）。差异只在注入方式：
///    旧侧先用 `sessionQueryRunning` CAS 抢锁再起虚拟线程跑
///    `executePromptCommand(value, allowedTools, modelOverride)`；zkcode 把提示词
///    按 `UserMessage` 交 zk-engine，并发门控由引擎既有 CAS（`query_busy`，
///    retryable=false）承担，与 `/skill`（3B.7）同一范式。`allowedTools` /
///    `model` 覆盖属引擎侧能力且本域三个 PROMPT 命令均未声明覆盖（旧
///    `GitReviewCommand` 实现 `Command` 而非 `PromptCommand`；`InitCommand` /
///    `RetryCommand` 取默认 `null`），故无行为差。
/// 4. **上下文解析失败**：旧 `requireSessionWorkingDirectory` 抛
///    `IllegalStateException`，该异常不在旧 catch 覆盖内 → 逃逸到 STOMP 框架
///    （前端无任何下行）。此处改推 `COMMAND_ERROR` 携同源文案——静默失败是旧
///    侧缺陷。
async fn handle_slash_command(state: &AppState, conn_id: &str, command: &str, args: &str) {
    let Some((session_id, _epoch)) = state.hub.bound_session(conn_id) else {
        tracing::warn!(
            conn_id,
            command,
            "slash_command dropped (session not bound)"
        );
        return;
    };
    tracing::info!(
        session_id = %session_id,
        command,
        args_length = args.len(),
        "WS slash_command"
    );
    // 旧 L1351：注册表未命中即失败关闭（文案差异见上文差异 1）。
    let handler = match state.commands.get_command(command) {
        Ok(handler) => handler,
        Err(not_found) => {
            tracing::warn!(session_id = %session_id, command, "slash command not found");
            state
                .hub
                .push(
                    &session_id,
                    ServerMessage::Error {
                        code: "COMMAND_NOT_FOUND".to_owned(),
                        message: not_found.message,
                        retryable: false,
                    },
                )
                .await;
            return;
        }
    };
    // 旧 L1352-1355 的上下文构造。
    let ctx = match CommandContext::resolve(state, &session_id).await {
        Ok(ctx) => ctx,
        Err(message) => {
            push_command_error(state, &session_id, &message).await;
            return;
        }
    };
    match handler.execute(args, &ctx).await {
        // 旧 L1361-1403：TEXT + `PROMPT` 类型 → 通知 + 注入对话（见差异 3）。
        CommandResult::Text(prompt) if handler.command_type() == CommandType::Prompt => {
            inject_prompt_command(state, &session_id, command, prompt).await;
        }
        // 旧 L1404-1409 的非 PROMPT（`LOCAL` 等）TEXT 分支。
        CommandResult::Text(output) => {
            state
                .hub
                .push(
                    &session_id,
                    ServerMessage::CommandResult {
                        command: command.to_owned(),
                        result_type: "text".to_owned(),
                        output: Some(output),
                        data: None,
                    },
                )
                .await;
        }
        // 旧 L1411-1417：`value` 为 null，`data` 承载结构化数据。
        CommandResult::Jsx(data) => {
            state
                .hub
                .push(
                    &session_id,
                    ServerMessage::CommandResult {
                        command: command.to_owned(),
                        result_type: "jsx".to_owned(),
                        output: None,
                        data: Some(data),
                    },
                )
                .await;
        }
        // 旧 L1418-1423（载荷键差异见上文差异 2）。
        CommandResult::Compact { display_text, data } => {
            let tokens_saved = data
                .get("savedTokens")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            state
                .hub
                .push(
                    &session_id,
                    ServerMessage::CompactComplete {
                        summary: display_text,
                        tokens_saved,
                    },
                )
                .await;
        }
        // 旧 L1424：`case SKIP -> { /* 无操作 */ }`（仍算成功，故照样追推列表刷新）。
        CommandResult::Skip => {}
        // 旧 L1433-1437：失败分支不追推 `session_list_updated`。
        CommandResult::Error(message) => {
            push_command_error(state, &session_id, &message).await;
            return;
        }
    }
    // 旧 L1428-1431：命令可能改动会话列表（如 `/clear` 新建会话）。
    state
        .hub
        .push(&session_id, ServerMessage::SessionListUpdated)
        .await;
}

/// `PROMPT` 命令的下行 + 注入（旧 `handleSlashCommand` L1361-1403 的
/// PROMPT 分支；并发门控与工具/模型覆盖的差异见 [`handle_slash_command`] 差异 3）。
async fn inject_prompt_command(state: &AppState, session_id: &str, command: &str, prompt: String) {
    // 旧 L1364-1367：只推命令名与结果类型，完整提示词不下行。
    state
        .hub
        .push(
            session_id,
            ServerMessage::CommandResult {
                command: command.to_owned(),
                result_type: "prompt".to_owned(),
                output: None,
                data: None,
            },
        )
        .await;
    tracing::info!(
        session_id,
        command,
        prompt_length = prompt.len(),
        "slash command prompt injected"
    );
    // 旧 L1388-1392 → `executePromptCommand` → `executeQueryInternal`：提示词即
    // `userText`，与普通用户消息同一执行路径（含持久化与并发门控）。
    state.hub.dispatch_to_engine(
        session_id,
        ClientMessage::UserMessage {
            text: prompt,
            attachments: None,
            references: None,
        },
    );
}

/// slash 命令失败下行（旧 `handleSlashCommand` L1433-1437：`COMMAND_ERROR`
/// + `retryable:false`）。
async fn push_command_error(state: &AppState, session_id: &str, message: &str) {
    tracing::warn!(session_id = %session_id, "slash command rejected");
    state
        .hub
        .push(
            session_id,
            ServerMessage::Error {
                code: "COMMAND_ERROR".to_owned(),
                message: message.to_owned(),
                retryable: false,
            },
        )
        .await;
}

/// `mcp_operation` 本地执行（旧 `handleMcpOperation` L1449-1503）。
///
/// 顺序逐字对照旧实现：会话未绑定 → warn 后丢弃 → `info` 日志 → 四操作
/// switch → 未知操作 `INVALID_MCP_OP` → 兜底异常 `MCP_ERROR`。
///
/// | 操作 | 旧行为 | 本端行为 |
/// |---|---|---|
/// | `connect` | 已存在连接 → `notification{key:"mcp-connect"}`；否则 `error{MCP_NOT_FOUND}` | 同 |
/// | `disconnect` | `removeServer` 后 `notification{key:"mcp-disconnect"}`（返回值丢弃） | 同 |
/// | `refresh` | `restartServer` 后 `notification{key:"mcp-refresh"}`；异常 → `MCP_ERROR` | 同 |
/// | `list` | `mcp_status{servers}` | 逐连接 `mcp_tool_update` + `mcp_health_status`（见差异 1） |
/// | 其他 | `error{INVALID_MCP_OP}` | 同 |
///
/// # 与旧实现的刻意差异（留痕）
///
/// 1. **`list` 的下行类型**：旧推 `mcp_status`——该 type 既不在 zkcode 冻结的
///    `ServerMessage` 域内，也不在前端 `stompClient.ts` 的可派发白名单里（旧前端
///    同样丢弃，是旧侧死流量）。故改推前端真实消费的两类：每个连接一条
///    `mcp_tool_update{serverId,tools}`（喂 `mcpStore.mcpTools`）+ 一条
///    `mcp_health_status{serverName,status,…}`（喂 `mcpStore.mcpHealthStates`），
///    信息量与旧 `mcp_status{servers}` 的 name/status/tools 三元等价。
/// 2. **`connect` 不建连**：旧注释自陈「McpClientManager uses addServer /
///    name-based lookup」——该分支只做存在性探测，不会真的连接未知服务器（建连
///    走 `POST /api/mcp/servers`）。照抄。
/// 3. **`config` 字段被忽略**：旧 `McpOperationPayload.config()` 在 handler 内
///    从未被读取。照抄（`dispatch` 处显式 `config: _`）。
async fn handle_mcp_operation(state: &AppState, conn_id: &str, operation: &str, server_id: &str) {
    let Some((session_id, _epoch)) = state.hub.bound_session(conn_id) else {
        tracing::warn!(
            conn_id,
            operation,
            "mcp_operation dropped (session not bound)"
        );
        return;
    };
    tracing::info!(
        session_id = %session_id,
        operation,
        server_id,
        "WS mcp_operation"
    );
    let manager = state.mcp();
    match operation {
        // 旧 L1461-1474：仅存在性探测，不建连（见差异 2）。
        "connect" => {
            if manager.get_connection(server_id).is_some() {
                push_mcp_notification(
                    state,
                    &session_id,
                    "mcp-connect",
                    format!("MCP server already connected: {server_id}"),
                )
                .await;
            } else {
                push_mcp_error(
                    state,
                    &session_id,
                    "MCP_NOT_FOUND",
                    format!("MCP server not found: {server_id}"),
                )
                .await;
            }
        }
        // 旧 L1475-1481：返回值丢弃，恒推成功通知。
        "disconnect" => {
            let _removed = manager.remove_server(server_id).await;
            push_mcp_notification(
                state,
                &session_id,
                "mcp-disconnect",
                format!("MCP server disconnected: {server_id}"),
            )
            .await;
        }
        // 旧 L1482-1488：`restartServer` 抛异常时落 catch → `MCP_ERROR`。
        "refresh" => match manager.restart_server(server_id).await {
            Ok(()) => {
                push_mcp_notification(
                    state,
                    &session_id,
                    "mcp-refresh",
                    format!("MCP server refreshed: {server_id}"),
                )
                .await;
            }
            Err(error) => {
                tracing::error!(session_id = %session_id, server_id, %error, "MCP operation failed");
                push_mcp_error(state, &session_id, "MCP_ERROR", error.to_string()).await;
            }
        },
        // 旧 L1489-1490（下行类型差异见差异 1）。
        "list" => push_mcp_snapshot(state, &session_id, &manager).await,
        // 旧 L1491-1494。
        unknown => {
            push_mcp_error(
                state,
                &session_id,
                "INVALID_MCP_OP",
                format!("Unknown MCP operation: {unknown}"),
            )
            .await;
        }
    }
}

/// `list` 操作的连接快照下推（旧 `mcp_status{servers}` 的等价物，见
/// [`handle_mcp_operation`] 差异 1）：每个连接一条 `mcp_tool_update` +
/// 一条 `mcp_health_status`。
async fn push_mcp_snapshot(
    state: &AppState,
    session_id: &str,
    manager: &Arc<zk_mcp::McpClientManager>,
) {
    for connection in manager.list_connections() {
        let tools = connection
            .tools()
            .into_iter()
            .map(|tool| McpToolInfo {
                name: tool.name,
                description: tool.description,
                input_schema: tool.input_schema,
            })
            .collect();
        let name = connection.name().to_owned();
        state
            .hub
            .push(
                session_id,
                ServerMessage::McpToolUpdate {
                    server_id: name.clone(),
                    tools,
                },
            )
            .await;
        state
            .hub
            .push(
                session_id,
                ServerMessage::McpHealthStatus {
                    status: connection.status().as_str().to_owned(),
                    consecutive_failures: i64::from(manager.consecutive_failures(&name)),
                    last_successful_ping: manager
                        .last_successful_ping(&name)
                        .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
                        .and_then(|since| i64::try_from(since.as_millis()).ok()),
                    server_name: name,
                },
            )
            .await;
    }
}

/// MCP 操作成功通知（旧三处 `push(sessionId, "notification", …)`：`level` 恒
/// `info`、`timeout` 恒 3000）。
async fn push_mcp_notification(state: &AppState, session_id: &str, key: &str, message: String) {
    state
        .hub
        .push(
            session_id,
            ServerMessage::Notification {
                key: key.to_owned(),
                level: "info".to_owned(),
                message,
                timeout: MCP_NOTIFICATION_TIMEOUT_MS,
            },
        )
        .await;
}

/// MCP 操作失败下行（旧三处 `push(sessionId, "error", …)`：`retryable` 恒
/// `false`）。
async fn push_mcp_error(state: &AppState, session_id: &str, code: &str, message: String) {
    state
        .hub
        .push(
            session_id,
            ServerMessage::Error {
                code: code.to_owned(),
                message,
                retryable: false,
            },
        )
        .await;
}

/// 应用层心跳（旧 `handlePing` L1550-1566 双路径）。
///
/// - 已绑定：`pong` 走会话推送（带 `_sessionId` / `_bindingEpoch` 与 seq）；
/// - 未绑定：`pong` 直发 + `bindRequired:true` + `serverNow`（提示前端
///   先完成 bind-session 握手）。
async fn handle_ping(state: &AppState, conn_id: &str) {
    match state.hub.bound_session(conn_id) {
        Some((session_id, _epoch)) => {
            state
                .hub
                .push(
                    &session_id,
                    ServerMessage::Pong {
                        bind_required: None,
                        server_now: None,
                    },
                )
                .await;
        }
        None => {
            state.hub.push_direct(
                conn_id,
                ServerMessage::Pong {
                    bind_required: Some(true),
                    server_now: Some(now_millis()),
                },
            );
        }
    }
}

/// `bind_session` 握手（校验链见模块文档；成功路径 restore + replay）。
async fn handle_bind(
    state: &AppState,
    conn_id: &str,
    session_id: &str,
    bind_request_id: &str,
    binding_epoch: i64,
    protocol_version: i64,
) {
    // 1. 协议版本过旧（旧 `< WS_PROTOCOL_VERSION` 判定；更高版本照常处理）。
    if protocol_version < WS_PROTOCOL_VERSION {
        reply_protocol_error(
            state,
            conn_id,
            "UPGRADE_REQUIRED".to_owned(),
            Some(bind_request_id.to_owned()),
            None,
        );
        return;
    }
    // 2. bindRequestId 缺失。
    if bind_request_id.trim().is_empty() {
        reply_protocol_error(
            state,
            conn_id,
            "BIND_REQUEST_ID_REQUIRED".to_owned(),
            None,
            None,
        );
        return;
    }
    // 3. bindingEpoch 必须 >= 1。
    if binding_epoch < 1 {
        reply_protocol_error(
            state,
            conn_id,
            "BINDING_EPOCH_REQUIRED".to_owned(),
            Some(bind_request_id.to_owned()),
            None,
        );
        return;
    }
    let client_epoch = u64::try_from(binding_epoch).unwrap_or(1);
    // 4. 会话存在性（DB 读失败 → BIND_RECOVERY_FAILED，对齐旧 catch 路径）。
    let detail = match state.db.get_session(session_id).await {
        Ok(Some(detail)) => detail,
        Ok(None) => {
            reply_protocol_error(
                state,
                conn_id,
                "SESSION_NOT_FOUND".to_owned(),
                Some(bind_request_id.to_owned()),
                None,
            );
            return;
        }
        Err(err) => {
            tracing::error!(conn_id, session_id, error = %err, "bind recovery db failure");
            reply_protocol_error(
                state,
                conn_id,
                "BIND_RECOVERY_FAILED".to_owned(),
                Some(bind_request_id.to_owned()),
                None,
            );
            return;
        }
    };
    // 5. epoch 单调校验（旧 STALE_BINDING_EPOCH；回显当前 epoch 供前端递增重试）。
    let epoch = match state.hub.bind(conn_id, session_id, client_epoch) {
        Ok(epoch) => epoch,
        Err(BindError::StaleEpoch) => {
            let current = state
                .hub
                .bound_session(conn_id)
                .map_or(0, |(_, epoch)| i64::try_from(epoch).unwrap_or(i64::MAX));
            reply_protocol_error(
                state,
                conn_id,
                "STALE_BINDING_EPOCH".to_owned(),
                Some(bind_request_id.to_owned()),
                Some(current),
            );
            return;
        }
        Err(BindError::NotRegistered) => {
            // 升级注册与 disconnect 的竞态残留：连接已不在，无处可答。
            tracing::debug!(conn_id, "bind on unregistered connection");
            return;
        }
    };
    // 6. 成功：session_restored 直发（pushToPrincipal 语义，不带路由字段）
    //    + pending critical 按序重放（对齐旧 getPrincipalsForSession 语义）。
    let restored = restore::build_session_restored(
        detail,
        Some(bind_request_id.to_owned()),
        epoch,
        state.authz.modes.get_mode(session_id),
    );
    state.hub.push_direct(conn_id, restored);
    let replayed = state.hub.replay_pending(session_id).await;
    if replayed > 0 {
        tracing::info!(
            conn_id,
            session_id,
            replayed,
            "ws pending replayed after bind"
        );
    }
    // 7. 重放全部 pending interaction（旧 L1703-1718）。`session_restored` 已带
    //    权威权限模式，故此处**不**补发 `permission_mode_changed`（旧源同注释：
    //    会触发虚假「已切换」通知）。
    replay_pending_interactions(state, conn_id, session_id).await;
}

/// bind 成功后的 pending interaction 恢复重投（旧 L1703-1718 +
/// `deliverInteraction(request, RECOVERY)`，L288-307）。
///
/// RECOVERY 投递语义：`prepare_recovery_delivery` 自增 `delivery_generation` 与
/// `dispatch_attempts` 并重置 ACK 窗口——旧连接的迟到 ACK 因代次落后被丢弃；
/// 回查后状态仍为 `pending` 才算 claim 成功，随后推**完整**视图且事件类型是
/// `interaction_created`（旧源 L306 硬编码，不是 `interaction_updated`）。
async fn replay_pending_interactions(state: &AppState, conn_id: &str, session_id: &str) {
    let pending = match state.authz.interactions.pending(session_id).await {
        Ok(pending) => pending,
        Err(error) => {
            tracing::warn!(session_id, error = %error, "failed to replay pending permissions");
            return;
        }
    };
    if pending.is_empty() {
        return;
    }
    let total = pending.len();
    let mut permission_count = 0_usize;
    for record in pending {
        if record.kind == InteractionType::Permission {
            permission_count += 1;
        }
        let claimed = match state
            .authz
            .interactions
            .prepare_recovery_delivery(&record.interaction_id, conn_id)
            .await
        {
            // 旧 L299-300：`claimed = request.status() == PENDING`。
            Ok(Some(refreshed)) => {
                (refreshed.status == InteractionStatus::Pending).then_some(refreshed)
            }
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(
                    interaction_id = %record.interaction_id,
                    error = %error,
                    "recovery delivery claim failed"
                );
                None
            }
        };
        let Some(refreshed) = claimed else { continue };
        match DurableInteractionService::view(&refreshed) {
            Ok(view) => {
                state
                    .hub
                    .push(session_id, ServerMessage::InteractionCreated { view })
                    .await;
            }
            Err(error) => tracing::warn!(
                interaction_id = %refreshed.interaction_id,
                code = %error.code,
                "pending interaction is not publishable"
            ),
        }
    }
    tracing::info!(
        session_id,
        total,
        permission_count,
        "replayed pending interactions"
    );
}

/// `interaction_ack` 确认——逐字对齐旧 `handleInteractionReceived`
/// （`WebSocketController.java:1182-1216`）。
///
/// 旧源校验链：`transportId == null` → return；`interactionId` 空白 → 只 debug；
/// 会话未绑定 → warn + return；交互不属于本会话或 transport 不在本会话通道集合
/// → warn 拒绝；`deliveryGeneration` 非 Number → `-1`（服务端据此丢弃过期 ACK）。
/// ACK 成功后回查交互，推 `interaction_updated` 四字段子集（L1207-1211）。
///
/// zkcode 侧 `bound_session(conn_id)` 已同时给出「本连接归属哪个会话」，因此旧源
/// 的 `getTransportIdsForSession(sessionId).contains(transportId)` 与
/// `sessionId.equals(requested.sessionId())` 两项合并为一次会话归属比对。
/// 旧源整段包在 `try { ... } catch (Exception e) { log.debug(...) }` 里——
/// 任何异常（含交互不存在）只 debug 不回错误，此处以 `Result`/`Option` 早退等价。
async fn handle_interaction_ack(
    state: &AppState,
    conn_id: &str,
    interaction_id: &str,
    delivery_generation: i64,
) {
    if interaction_id.trim().is_empty() {
        tracing::debug!(
            conn_id,
            "ignoring legacy interaction ACK without interactionId"
        );
        return;
    }
    let Some((session_id, _epoch)) = state.hub.bound_session(conn_id) else {
        tracing::debug!(conn_id, "interaction_ack dropped (session not bound)");
        return;
    };
    let requested = match state.authz.interactions.find_by_id(interaction_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            tracing::debug!(
                conn_id,
                interaction_id,
                "interaction ACK ignored: not found"
            );
            return;
        }
        Err(error) => {
            tracing::debug!(interaction_id, error = %error, "interaction ACK ignored");
            return;
        }
    };
    if requested.session_id != session_id {
        tracing::warn!(
            interaction_id,
            session = %session_id,
            transport = conn_id,
            "interaction ACK rejected"
        );
        return;
    }
    let acknowledged = state
        .authz
        .interactions
        .acknowledge_received(interaction_id, Some(conn_id), delivery_generation)
        .await;
    match acknowledged {
        Ok(true) => {}
        Ok(false) => return,
        Err(error) => {
            tracing::debug!(interaction_id, error = %error, "interaction ACK ignored");
            return;
        }
    }
    // 旧 L1205-1211：ACK 成功后**回查**交互再推，载荷取回查后的 deadline/version。
    let Ok(Some(record)) = state.authz.interactions.find_by_id(interaction_id).await else {
        return;
    };
    let view = DurableInteractionService::ack_view(&record);
    state
        .hub
        .push(
            &record.session_id,
            ServerMessage::InteractionUpdated { view },
        )
        .await;
}

/// `permission_response` 上行——逐字对齐旧 `handlePermissionResponse`
/// （`WebSocketController.java:1171-1180`）。
///
/// **协议 v2 起该上行不再承载决策**：旧源无条件回 `interaction_rest_required`
/// 错误，把决策引导到 `POST /api/interactions/{id}/decisions`（REST 才有
/// `expectedVersion` 乐观锁与 11 段校验链）。zkcode 逐字保留此行为。
async fn handle_permission_response(state: &AppState, conn_id: &str) {
    let Some((session_id, _epoch)) = state.hub.bound_session(conn_id) else {
        tracing::debug!(conn_id, "permission_response dropped (session not bound)");
        return;
    };
    state
        .hub
        .push(
            &session_id,
            ServerMessage::Error {
                code: "interaction_rest_required".to_owned(),
                message: "协议 v2 的交互决定必须提交到 /api/interactions/{id}/decisions".to_owned(),
                retryable: false,
            },
        )
        .await;
}

/// `set_permission_mode` 上行——逐字对齐旧 `handleSetPermissionMode`
/// （`WebSocketController.java:1310-1335`）。
///
/// 空白或非法枚举名 → `INVALID_PERMISSION_MODE` 错误下行（旧源以
/// `IllegalArgumentException` 统一捕获，且只记录长度与指纹、不记录原值）；
/// 合法 → `info` 日志 + `PermissionModeManager.setMode`（真实变化才推
/// `permission_mode_changed`）。
async fn handle_set_permission_mode(state: &AppState, conn_id: &str, mode: &str) {
    let Some((session_id, _epoch)) = state.hub.bound_session(conn_id) else {
        tracing::debug!(conn_id, "set_permission_mode dropped (session not bound)");
        return;
    };
    // 旧 L1320-1324：空白视同非法；`valueOf` 只认精确枚举名（大写 SNAKE_CASE）。
    let parsed = if mode.trim().is_empty() {
        None
    } else {
        PermissionMode::parse(mode)
    };
    let Some(parsed) = parsed else {
        tracing::warn!(length = mode.chars().count(), "invalid permission mode");
        state
            .hub
            .push(
                &session_id,
                ServerMessage::Error {
                    code: "INVALID_PERMISSION_MODE".to_owned(),
                    message: "Invalid permission mode".to_owned(),
                    retryable: false,
                },
            )
            .await;
        return;
    };
    tracing::info!(session_id = %session_id, mode = parsed.as_str(), "WS set_permission_mode");
    state.authz.modes.set_mode(&session_id, parsed).await;
}

/// `set_model` 上行处理（旧 `handleSetModel` L1285-1304 语义 + 2.7 多提供商校验）。
///
/// - 未绑定会话：debug 丢弃（旧 `principalSession==null` 直接 return）。
/// - 模型校验：`model` 空白 → `INVALID_MODEL`；`ProviderRegistry` 已注册模型清单
///   非空且不含该模型 → `INVALID_MODEL`；注册表为空（Phase 1 单 provider 回退）
///   放行任意模型（旧行为不校验能力表外的模型）。
/// - 有效：落库 `sessions.model`（失败仅告警不阻断，对齐旧「更新失败仍确认」的
///   宽松路径）+ 推送下行 `model_changed`。
async fn handle_set_model(state: &AppState, conn_id: &str, model: &str) {
    let Some((session_id, _epoch)) = state.hub.bound_session(conn_id) else {
        tracing::debug!(conn_id, "set_model dropped (session not bound)");
        return;
    };
    let trimmed = model.trim();
    let providers = state.providers.load();
    let known = providers.models();
    let supported = !trimmed.is_empty() && (known.is_empty() || known.iter().any(|m| m == trimmed));
    if !supported {
        state
            .hub
            .push(
                &session_id,
                ServerMessage::Error {
                    code: "INVALID_MODEL".to_owned(),
                    message: format!("Unsupported model: {model}"),
                    retryable: false,
                },
            )
            .await;
        return;
    }
    if let Err(err) = state.db.update_session_model(&session_id, trimmed).await {
        tracing::warn!(session_id, error = %err, "persist session model failed");
    }
    state
        .hub
        .push(
            &session_id,
            ServerMessage::ModelChanged {
                model: trimmed.to_owned(),
            },
        )
        .await;
}

/// `protocol_error` 直发（握手错误的统一出口；计出向 kind 一次）。
fn reply_protocol_error(
    state: &AppState,
    conn_id: &str,
    code: String,
    bind_request_id: Option<String>,
    binding_epoch: Option<i64>,
) {
    state.hub.push_direct(
        conn_id,
        ServerMessage::ProtocolError {
            code,
            supported_version: WS_PROTOCOL_VERSION,
            bind_request_id,
            binding_epoch,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `KNOWN_CLIENT_TYPES` 与 `ClientMessage` 16 variant 的 kind 输出域互锁
    ///（新增 variant 忘记登记时此处失败，避免已知消息被误判未知）。
    #[test]
    fn known_client_types_stay_in_sync() {
        use zk_protocol::ClientMessage as M;
        let empty = String::new;
        let mut kinds = vec![
            M::UserMessage {
                text: empty(),
                attachments: None,
                references: None,
            }
            .kind(),
            M::RunInput {
                request_id: empty(),
                text: empty(),
            }
            .kind(),
            M::PermissionResponse {
                tool_use_id: empty(),
                decision: empty(),
                remember: false,
                scope: empty(),
            }
            .kind(),
            M::Interrupt {
                is_submit_interrupt: None,
            }
            .kind(),
            M::SetModel { model: empty() }.kind(),
            M::SetPermissionMode { mode: empty() }.kind(),
            M::SlashCommand {
                command: empty(),
                args: empty(),
            }
            .kind(),
            M::McpOperation {
                operation: empty(),
                server_id: empty(),
                config: None,
            }
            .kind(),
            M::RewindFiles {
                message_id: empty(),
                file_paths: Vec::new(),
            }
            .kind(),
            M::ElicitationResponse {
                request_id: empty(),
                answer: serde_json::Value::Null,
            }
            .kind(),
            M::Ping.kind(),
            M::BindSession {
                session_id: empty(),
                bind_request_id: empty(),
                binding_epoch: 1,
                protocol_version: 3,
            }
            .kind(),
            M::InteractionAck {
                interaction_id: empty(),
                delivery_generation: 1,
            }
            .kind(),
            M::ActivitySave {
                id: empty(),
                operation_type: empty(),
                summary: None,
                status: None,
                timestamp: None,
                duration: None,
                file_count: None,
                decision: None,
                tool_result: None,
                changed_files: None,
                insight: None,
            }
            .kind(),
            M::ActivityUpdate {
                id: empty(),
                decision: None,
                insight: None,
            }
            .kind(),
        ];
        kinds.sort_unstable();
        let mut known = KNOWN_CLIENT_TYPES.to_vec();
        known.sort_unstable();
        assert_eq!(kinds, known, "KNOWN_CLIENT_TYPES must track ClientMessage");
    }
}
