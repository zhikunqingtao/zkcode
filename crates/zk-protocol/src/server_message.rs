//! 下行消息（Server → Client）全量建模：**57 个 variant = 37 个顶层 record +
//! 17 个字符串直推类型，重名已收敛**（`cost_update` / `pong` 既是 record 又有
//! 直推调用点，各收敛为单一 variant；5 个嵌套 record 建为独立 struct 而非 variant）。
//!
//! - 权威 type 字符串清单：旧前端 `stompClient.ts` `VALID_MESSAGE_TYPES` 白名单
//!   （57 条，逐字提取）；variant 名 `PascalCase` 经 `rename_all = "snake_case"`
//!   自动派生后与白名单**逐字一致**（核对结论见 `tests/roundtrip.rs`）。
//! - variant 字段一律 `rename_all = "camelCase"`（旧线上 JSON 即 camelCase）。
//! - 嵌套 record（`ToolResultContent` / `ElicitationOption` / `McpToolInfo` /
//!   `SessionMetadata` / `WorkerSnapshot` / `InteractionView`）为共享 struct。
//!
//! # 已知线上形状差异（建模取舍留痕）
//!
//! 1. **`swarm_state_update` / `worker_progress` 的嵌套 payload**：旧系统
//!    `pushToUser` 对非 Map payload 会降级嵌套（`message.put("payload", payload)`，
//!    `WebSocketController` L197-201），`SwarmService` 传 record 即走此路径。本
//!    建模按 U1 扁平信封**统一平铺**——旧嵌套路径本身是注释中自认的「降级」
//!    缺陷，新系统不复制；两类型均 `Phase 2+ 建模未激活`，对照调试期如需解析旧
//!    嵌套流量由 ws 层适配（Phase 2+ 待办）。
//! 2. **直推超集字段**：`session_restored` / `rewind_complete` / `pong` 的实际
//!    直推字段多于同名 record（活动列表、恢复文件明细、bindRequired 等），按
//!    直推超集建模（record 子集是其真子集，解析兼容）。
//! 3. **`tool_permission_denied`**：§11.1b 表述为 `interactionId/operationHash`，
//!    后端实际直推（L413-414）为 `toolUseId/toolName`，**以代码为准**。

use crate::model::{FlexEpoch, Message, Usage};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 下行消息枚举——57 个 type 全量冻结。
///
/// 每个 variant 的文档注明旧 record / 直推调用点、一句话语义与激活状态
/// （`Phase 1 激活` / `Phase 2+ 建模未激活`）。激活判定依据
/// `docs/architecture.md` D3/D5/D8 与任务规格：Phase 1 最小引擎只做
/// 「收 prompt → 单轮补全 → 流式 delta」，工具 / 交互 / Swarm 子系统 Phase 2+。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)] // 序列化主导；Phase 1 无高频 clone 路径，不引入 Box
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    // ═══════════ messageStore 链路（record #1-5 + 直推 tool_use_input） ═══════════
    /// 旧 record `StreamDelta` / 直推点 `sendStreamDelta`（L335）：文本流增量。
    ///
    /// **Phase 1 激活**（D3 最小引擎的核心输出）。
    #[serde(rename_all = "camelCase")]
    StreamDelta {
        /// 增量文本。
        delta: String,
    },

    /// 旧 record `ThinkingDelta` / 直推点 `sendThinkingDelta`（L340）：思考流增量。
    ///
    /// **Phase 1 激活**（D8：reasoning delta 前端已有渲染管线）。
    #[serde(rename_all = "camelCase")]
    ThinkingDelta {
        /// 增量思考文本。
        delta: String,
    },

    /// 旧 record `ToolUseStart` / 直推点 `sendToolUseStart`（L345）：工具调用开始。
    ///
    /// **Phase 2+ 建模未激活**（Phase 1 引擎无工具调用）。
    #[serde(rename_all = "camelCase")]
    ToolUseStart {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 工具名。
        tool_name: String,
        /// 工具入参（开始时通常为空对象，完整入参随后由
        /// [`ServerMessage::ToolUseInput`] 补齐）。
        input: serde_json::Value,
    },

    /// 旧 record `ToolUseProgress` / 直推点 `sendToolUseProgress`（L351）：工具执行
    /// 进度（stdout 增量）。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    ToolUseProgress {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 进度文本。
        progress: String,
    },

    /// 旧 record `ToolResult` / 直推点 `sendToolResult`（L369）：工具结果返回。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    ToolResult {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 结果内容体（嵌套对象，线上形状 `result: {content, isError, metadata?}`）。
        result: ToolResultContent,
    },

    /// 直推点 `WsMessageHandler.onToolUseComplete`（L1103）：工具完整入参预览
    ///（流式收齐后补发）。无对应 record。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    ToolUseInput {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 工具名。
        tool_name: String,
        /// 完整工具入参。
        input: serde_json::Value,
    },

    /// 直推点 `sendToolPermissionDenied`（L413）：用户拒绝授权通知（前端清理
    /// changedFiles）。无对应 record；字段以代码为准（见模块文档差异 3）。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    ToolPermissionDenied {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 工具名。
        tool_name: String,
    },

    // ═══════════ permissionStore + sessionStore（record #6-8） ═══════════
    /// 旧 record `PermissionRequest` / 直推点 `sendPermissionRequest`（L394）与
    /// `sendPermissionRequestFromChild`（L407）：权限确认请求。字段取两直推点
    /// 与前端 `PermissionRequestPayload` 消费形状的并集；生产环境权限改走
    /// `interaction_created`（持久化交互），本 variant 为兼容保留。
    ///
    /// **Phase 2+ 建模未激活**（Phase 1 鉴权无工具权限弹窗链路）。
    #[serde(rename_all = "camelCase")]
    PermissionRequest {
        /// 交互 ID（interaction 通道融合后可选携带）。
        #[serde(skip_serializing_if = "Option::is_none")]
        interaction_id: Option<String>,
        /// 工具调用 ID。
        tool_use_id: String,
        /// 工具名。
        tool_name: String,
        /// 工具入参。
        input: serde_json::Value,
        /// 风险等级（low/medium/high）。
        risk_level: String,
        /// 请求原因说明。
        reason: String,
        /// 来源（子代理转发时为 `subagent`）。
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        /// 子代理会话 ID（父会话转发场景）。
        #[serde(skip_serializing_if = "Option::is_none")]
        child_session_id: Option<String>,
        /// 决策截止时间。
        #[serde(skip_serializing_if = "Option::is_none")]
        decision_deadline_at: Option<FlexEpoch>,
        /// 可选授权范围（Bash 类工具仅 session）。
        #[serde(skip_serializing_if = "Option::is_none")]
        scope_options: Option<Vec<String>>,
    },

    /// 旧 record `MessageComplete` / 直推点 `sendMessageComplete`（L442）：助手
    /// 回合完成（含权威落库消息尾部）。
    ///
    /// **Phase 1 激活**。注意：并发保护拒绝（`query_busy`）复用
    /// [`ServerMessage::Error`]，code=`query_busy`。
    #[serde(rename_all = "camelCase")]
    MessageComplete {
        /// Token 用量。
        usage: Usage,
        /// 停止原因（`end_turn` 等；可为 null）。
        #[serde(skip_serializing_if = "Option::is_none")]
        stop_reason: Option<String>,
        /// 会话 ID（携带 committedMessages 时存在）。
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        /// Run ID。
        #[serde(skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
        /// 权威尾部替换锚点（其后的本地乐观消息被回滚）。
        #[serde(skip_serializing_if = "Option::is_none")]
        replace_after_message_id: Option<String>,
        /// 权威落库消息尾部。
        #[serde(skip_serializing_if = "Option::is_none")]
        committed_messages: Option<Vec<Message>>,
    },

    /// 旧 record `Error` / 直推点 `sendError`（L449）等多处：API 错误 / 过载 /
    /// 业务拒绝（`query_busy` / `INVALID_MODEL` / `interaction_rest_required` 等
    /// code 复用本类型）。
    ///
    /// **Phase 1 激活**。
    #[serde(rename_all = "camelCase")]
    Error {
        /// 错误码（机器可读）。
        code: String,
        /// 人类可读消息。
        message: String,
        /// 是否可重试。
        retryable: bool,
    },

    // ═══════════ sessionStore / appUiStore / taskStore / costStore（record #9-22） ═══════════
    /// 旧 record `CompactStart` / 直推点 `sendCompactStart`（L457）：上下文压缩开始。
    ///
    /// **Phase 2+ 建模未激活**（Phase 1 无 compact 执行链路）。
    CompactStart,

    /// 旧 record `CompactComplete` / 直推点 `sendCompactComplete`（L462）：压缩完成。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    CompactComplete {
        /// 压缩摘要。
        summary: String,
        /// 节省 token 数。
        tokens_saved: i64,
    },

    /// 旧 record `Elicitation` / 直推点 `sendElicitation`（L475）：AI 反向提问。
    ///
    /// **Phase 2+ 建模未激活**（elicitation 走 interaction 通道，Phase 2+）。
    #[serde(rename_all = "camelCase")]
    Elicitation {
        /// 请求 ID。
        request_id: String,
        /// 问题文本。
        question: String,
        /// 选项列表。
        options: Vec<ElicitationOption>,
    },

    /// 旧 record `AgentSpawn` / 直推点 `sendAgentSpawn`（L483）：子代理启动。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    AgentSpawn {
        /// 任务 ID。
        task_id: String,
        /// 代理显示名。
        agent_name: String,
        /// 代理类型。
        agent_type: String,
    },

    /// 旧 record `AgentUpdate` / 直推点 `sendAgentUpdate`（L489）：子代理进度。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    AgentUpdate {
        /// 任务 ID。
        task_id: String,
        /// 进度文本。
        progress: String,
    },

    /// 旧 record `AgentComplete` / 直推点 `sendAgentComplete`（L494）：子代理完成。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    AgentComplete {
        /// 任务 ID。
        task_id: String,
        /// 结果文本。
        result: String,
    },

    /// Batch 6 子代理启动通知。
    #[serde(rename_all = "camelCase")]
    AgentStarted {
        /// 代理 ID。
        agent_id: String,
        /// 任务提示。
        prompt: String,
    },

    /// Batch 6 子代理完成通知。
    #[serde(rename_all = "camelCase")]
    AgentCompleted {
        /// 代理 ID。
        agent_id: String,
        /// 结果文本。
        result: String,
    },

    /// Batch 6 子代理失败通知。
    #[serde(rename_all = "camelCase")]
    AgentFailed {
        /// 代理 ID。
        agent_id: String,
        /// 错误信息。
        error: String,
    },

    /// 旧 record `CostUpdate` + 直推点 `sendCostUpdate`（L508）——**重名收敛为单一
    /// variant**：费用 / Token 周期性更新。
    ///
    /// **Phase 1 激活**（用量核算属于单轮引擎闭环）。
    #[serde(rename_all = "camelCase")]
    CostUpdate {
        /// 会话累计费用。
        session_cost: f64,
        /// 全局累计费用。
        total_cost: f64,
        /// 本次用量。
        usage: Usage,
    },

    /// 旧 record `RateLimit` / 直推点 `sendRateLimit`（L516）：限流通知。
    ///
    /// **Phase 2+ 建模未激活**（Phase 1 单用户 localhost 限流不触发）。
    #[serde(rename_all = "camelCase")]
    RateLimit {
        /// 建议重试等待毫秒数。
        retry_after_ms: i64,
        /// 限流类型。
        limit_type: String,
    },

    /// 旧 record `Notification` / 直推点 `sendNotification`（L525）：系统通知推送。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    Notification {
        /// 通知去重键。
        key: String,
        /// 级别（info/warning/error）。
        level: String,
        /// 通知文本。
        message: String,
        /// 自动消失毫秒数（0 表示不消失）。
        timeout: i64,
    },

    /// 旧 record `TaskUpdate` / 直推点 `sendTaskUpdate`（L538）：后台任务状态。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    TaskUpdate {
        /// 任务 ID。
        task_id: String,
        /// 状态。
        status: String,
        /// 进度文本（可缺省）。
        #[serde(skip_serializing_if = "Option::is_none")]
        progress: Option<String>,
        /// 输出内容（可缺省，Batch 6 追加）。
        #[serde(skip_serializing_if = "Option::is_none")]
        output: Option<String>,
    },

    /// 旧 record `PromptSuggestion` / 直推点 `sendPromptSuggestion`（L545）：提示
    /// 建议推送。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    PromptSuggestion {
        /// 建议列表。
        suggestions: Vec<String>,
    },

    /// 旧 record `BridgeStatus` / 直推点 `sendBridgeStatus`（L552）：IDE 桥接连接
    /// 状态。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    BridgeStatus {
        /// 连接状态。
        status: String,
        /// 桥接 URL。
        url: String,
    },

    /// 旧 record `TeammateMessage` / 直推点 `sendTeammateMessage`（L559）：Swarm
    /// 队友消息。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    TeammateMessage {
        /// 发送者 ID。
        from_id: String,
        /// 消息内容。
        content: String,
    },

    /// 旧 record `SpeculationResult` / 直推点 `sendSpeculationResult`（L566）：推测
    /// 执行结果。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    SpeculationResult {
        /// 推测 ID。
        id: String,
        /// 是否被采纳。
        accepted: bool,
    },

    // ═══════════ mcpStore / 断线重连 / 心跳（record #23-25） ═══════════
    /// 旧 record `McpToolUpdate` / 直推点 `sendMcpToolUpdate`（L573）：MCP 工具
    /// 列表变更。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    McpToolUpdate {
        /// MCP 服务器 ID。
        server_id: String,
        /// 工具元数据列表。
        tools: Vec<McpToolInfo>,
    },

    /// 旧 record `SessionRestored` + 直推点 `handleBindSession`（L1691，走
    /// `pushToPrincipal`）——**重名收敛**；字段按直推超集建模（record 仅
    /// `messages` + `metadata`，直推还携带活动列表、恢复投影与费用摘要）。
    ///
    /// **Phase 1 激活**（bind-session 握手的成功响应，U1 恢复门语义）。
    #[serde(rename_all = "camelCase")]
    SessionRestored {
        /// 历史消息（权威 `Message` 列表）。
        messages: Vec<Message>,
        /// 恢复会话元信息。
        metadata: SessionMetadata,
        /// 活动列表（最近 50 条；活动行结构 Phase 2+ 强类型化，暂以 JSON 透传）。
        #[serde(skip_serializing_if = "Option::is_none")]
        activities: Option<Vec<serde_json::Value>>,
        /// 活动总数。
        #[serde(skip_serializing_if = "Option::is_none")]
        total_activity_count: Option<i64>,
        /// 活动是否还有更多。
        #[serde(skip_serializing_if = "Option::is_none")]
        has_more: Option<bool>,
        /// WS 协议版本（当前 3）。
        protocol_version: i64,
        /// 绑定请求 ID（与上行 `bind_session.bindRequestId` 对应）。
        bind_request_id: Option<String>,
        /// 连接绑定纪元。
        binding_epoch: Option<i64>,
        /// 服务端当前时间（epoch 毫秒）。
        #[serde(skip_serializing_if = "Option::is_none")]
        server_now: Option<i64>,
        /// Run 恢复快照（Phase 2+ run 投影；透传）。
        #[serde(skip_serializing_if = "Option::is_none")]
        run_snapshot: Option<serde_json::Value>,
        /// 快照事件序号。
        #[serde(skip_serializing_if = "Option::is_none")]
        snapshot_event_seq: Option<i64>,
        /// 活跃工具调用（Phase 2+；透传）。
        #[serde(skip_serializing_if = "Option::is_none")]
        active_tool_calls: Option<serde_json::Value>,
        /// 会话费用摘要（透传）。
        #[serde(skip_serializing_if = "Option::is_none")]
        cost_summary: Option<serde_json::Value>,
    },

    /// 旧 record `Pong` + 直推点 `sendPong`（L580，空 Map）与 `handlePing`
    /// 未绑定路径（L1560，携带 bindRequired/serverNow）——**重名收敛**：心跳响应。
    ///
    /// **Phase 1 激活**（D1 应用层心跳 pong）。
    #[serde(rename_all = "camelCase")]
    Pong {
        /// 连接未绑定会话时为 true（提示前端需要 bind-session）。
        #[serde(skip_serializing_if = "Option::is_none")]
        bind_required: Option<bool>,
        /// 服务端当前时间（epoch 毫秒，供 RTT 测量）。
        #[serde(skip_serializing_if = "Option::is_none")]
        server_now: Option<i64>,
    },

    // ═══════════ 新增消息类型（record #26-32） ═══════════
    /// 旧 record `CompactEvent`：压缩进度事件。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    CompactEvent {
        /// 阶段（warning 等）。
        phase: String,
        /// 使用百分比。
        usage_percent: i64,
        /// 当前 token 数。
        current_tokens: i64,
    },

    /// 旧 record `TokenWarning` / 直推点 `pushTokenWarning`（L610）：Token 用量
    /// 警告。注意直推的 `usagePercent` 为 Java double（JSON `90.0`），故取 f64。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    TokenWarning {
        /// 当前 token 数。
        current_tokens: i64,
        /// 有效窗口大小。
        max_tokens: i64,
        /// 使用百分比（0-100，小数合法）。
        usage_percent: f64,
        /// 告警级别（`normal` / `warning` / `critical` / `trigger_compact`；线上亦见 `red`）。
        warning_level: String,
    },

    /// 旧 record `InterruptAck` / 直推点 `handleInterrupt`（L1243）与
    /// `RemoteControlController`（L102）：中断确认。
    ///
    /// **Phase 2+ 建模未激活**（Phase 1 引擎单轮即时结束，无中断恢复复杂度）。
    #[serde(rename_all = "camelCase")]
    InterruptAck {
        /// 中断原因（`USER_INTERRUPT` / `SUBMIT_INTERRUPT` / `REMOTE` 等 `AbortReason`）。
        reason: String,
    },

    /// 旧 record `ModelChanged` / 直推点 `handleSetModel`（L1297）：模型切换确认。
    ///
    /// **Phase 1 激活**。
    #[serde(rename_all = "camelCase")]
    ModelChanged {
        /// 切换后的模型标识。
        model: String,
    },

    /// 旧 record `PermissionModeChanged` / 直推点 `PermissionModeManager`（L63）：
    /// 权限模式切换确认。
    ///
    /// **Phase 1 激活**。
    #[serde(rename_all = "camelCase")]
    PermissionModeChanged {
        /// 新模式（`DEFAULT` / `AUTO_APPROVE` 等）。
        mode: String,
        /// 前一模式（个别调用点可缺省）。
        #[serde(skip_serializing_if = "Option::is_none")]
        previous: Option<String>,
    },

    /// 旧 record `CommandResult` / 直推点 slash 命令处理器（L1365 等）：命令执行
    /// 结果。
    ///
    /// **3B.7 激活**（`/skill` 走 `resultType="prompt"` 分支）。字段按旧
    /// `WebSocketController.handleSlashCommand` 的三个直推点与前端契约
    /// （`frontend/src/types/index.ts` `CommandResultPayload`）取超集：
    /// `prompt` 分支只推 `command` + `resultType`（正文不下行，前端仅显示
    /// 「AI 正在处理 /x 命令…」），`text` 分支带 `output`，`jsx` 分支带 `data`。
    #[serde(rename_all = "camelCase")]
    CommandResult {
        /// 命令名。
        command: String,
        /// 结果类型（`text` / `jsx` / `prompt`）。
        result_type: String,
        /// 输出文本（`text` 分支）。
        #[serde(skip_serializing_if = "Option::is_none")]
        output: Option<String>,
        /// 结构化数据（`jsx` 分支）。
        #[serde(skip_serializing_if = "Option::is_none")]
        data: Option<serde_json::Value>,
    },

    /// 旧 record `RewindComplete` + 直推点 `handleRewindFiles`（L1523）——字段按
    /// 直推超集建模（record 仅 `messageId` + `files`）：文件回退完成。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    RewindComplete {
        /// 锚点消息 ID。
        message_id: String,
        /// 是否成功。
        success: bool,
        /// 已恢复文件列表。
        restored_files: Vec<String>,
        /// 跳过文件列表。
        skipped_files: Vec<String>,
        /// 错误列表。
        errors: Vec<String>,
        /// 请求回退的文件列表。
        files: Vec<String>,
    },

    /// 旧 record `McpHealthStatus`：MCP 服务器连接健康状态变更。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    McpHealthStatus {
        /// 服务器名。
        server_name: String,
        /// 健康状态。
        status: String,
        /// 连续失败次数。
        consecutive_failures: i64,
        /// 最近一次成功 ping（可空）。
        #[serde(skip_serializing_if = "Option::is_none")]
        last_successful_ping: Option<i64>,
    },

    /// 旧 record `TokenBudgetNudge` / 直推点 `onTokenBudgetNudge`（L1162）：Token
    /// 预算续写提示。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    TokenBudgetNudge {
        /// 已消耗百分比。
        pct: i64,
        /// 当前 token 数。
        current_tokens: i64,
        /// 预算 token 数。
        budget_tokens: i64,
    },

    // ═══════════ swarmStore / coordinatorStore（record #38-41） ═══════════
    /// 旧 record `SwarmStateUpdate` / 直推点 `SwarmService`（L442）：Swarm 状态
    /// 变更通知。注意：旧 `pushToUser` record 路径产生嵌套 `payload` 键，本建模
    /// 按 U1 统一平铺（见模块文档差异 1）。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    SwarmStateUpdate {
        /// Swarm ID。
        swarm_id: String,
        /// 阶段（`INITIALIZING` / `RUNNING` / `IDLE` / `SHUTTING_DOWN` / `TERMINATED`）。
        phase: String,
        /// 活跃 worker 数。
        active_workers: i64,
        /// 总 worker 数。
        total_workers: i64,
        /// 已完成任务数。
        completed_tasks: i64,
        /// 总任务数。
        total_tasks: i64,
        /// Worker 快照（workerId → 快照）。
        workers: BTreeMap<String, WorkerSnapshot>,
    },

    /// 旧 record `WorkerProgress` / 直推点 `SwarmService`（L463）：Worker 实时进度
    ///（含 Phase 2 扩展字段，全部可缺省）。嵌套 payload 差异同上。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    WorkerProgress {
        /// Swarm ID。
        swarm_id: String,
        /// Worker ID。
        worker_id: String,
        /// 状态（STARTING/WORKING/IDLE/TERMINATED）。
        status: String,
        /// 当前任务描述（可空）。
        #[serde(skip_serializing_if = "Option::is_none")]
        current_task: Option<String>,
        /// 工具调用次数。
        tool_call_count: i64,
        /// 消耗 token 数。
        token_consumed: i64,
        /// 最近 5 个工具调用名。
        #[serde(skip_serializing_if = "Option::is_none")]
        recent_tool_calls: Option<Vec<String>>,
        /// 进度百分比（Phase 2 扩展）。
        #[serde(skip_serializing_if = "Option::is_none")]
        progress_percent: Option<i64>,
        /// 总步骤数（Phase 2 扩展）。
        #[serde(skip_serializing_if = "Option::is_none")]
        total_steps: Option<i64>,
        /// 已完成步骤数（Phase 2 扩展）。
        #[serde(skip_serializing_if = "Option::is_none")]
        completed_steps: Option<i64>,
        /// 错误消息（Phase 2 扩展）。
        #[serde(skip_serializing_if = "Option::is_none")]
        error_message: Option<String>,
        /// 当前步骤描述（Phase 2 扩展）。
        #[serde(skip_serializing_if = "Option::is_none")]
        current_step_description: Option<String>,
        /// 终止原因（completed/error/aborted；Phase 2 扩展）。
        #[serde(skip_serializing_if = "Option::is_none")]
        termination_reason: Option<String>,
    },

    /// 旧 record `WorkflowPhaseUpdate` / 直推点 `CoordinatorWorkflowEngine`
    ///（L389/L411）：Coordinator 四阶段工作流阶段变更。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    WorkflowPhaseUpdate {
        /// 工作流 ID。
        workflow_id: String,
        /// 阶段名（Research/Synthesis/Implementation/Verification；完成为空串）。
        phase_name: String,
        /// 状态（`NOT_STARTED` / `RUNNING` / `COMPLETED` / `FAILED` / `CANCELLED`）。
        status: String,
        /// 阶段序号（0-3；完成为 -1）。
        phase_index: i64,
        /// 总阶段数（4）。
        total_phases: i64,
        /// 阶段引导提示词。
        phase_prompt: String,
        /// 工作流目标。
        objective: String,
    },

    // ═══════════ 字符串直推类型（无 record，§11.1b 补录） ═══════════
    /// 直推点 `handleUserMessage` 视觉模型自动路由（L673）：模型自动切换通知。
    /// 当前由 `zk-engine` 在附件校验通过、构造请求前推送；只覆盖本次 Run，
    /// 不修改持久 Session 模型。
    #[serde(rename_all = "camelCase")]
    ModelRouted {
        /// 原模型标识。
        original_model: String,
        /// 路由后模型标识。
        routed_model: String,
        /// 路由后模型显示名。
        routed_model_name: String,
        /// 路由原因（人类可读）。
        reason: String,
    },

    /// 直推点 `sendPlanUpdate`（L587，PlanCommand 构造）：Plan Mode 状态更新。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    PlanUpdate {
        /// 是否处于 Plan Mode。
        is_plan_mode: bool,
        /// 计划名（关闭时可缺省）。
        #[serde(skip_serializing_if = "Option::is_none")]
        plan_name: Option<String>,
        /// 计划概览。
        #[serde(skip_serializing_if = "Option::is_none")]
        plan_overview: Option<String>,
    },

    /// 直推点 `sendSessionListUpdated`（L594）：会话列表变更通知（前端按需刷新）。
    ///
    /// **Phase 1 激活**（会话生命周期属于 U2 REST + WS 通知闭环）。
    SessionListUpdated,

    /// 直推点 `VisualizationPayloadBuilder.publish`（L66，自行组装信封）：结构化
    /// 输出自动可视化消息。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    Visualization {
        /// 可视化条目 UUID。
        uuid: String,
        /// 视图类型（mermaid/schema/timeline/json/text）。
        view_type: String,
        /// 视图属性。
        props: serde_json::Value,
    },

    /// 直推点 `VerifyCheckService.pushVerificationResult`（L405）：验证子系统最终
    /// 结论。
    ///
    /// **Phase 2+ 建模未激活**（APOS）。
    #[serde(rename_all = "camelCase")]
    VerificationResult {
        /// 信号（`auto_approve` / `review_recommended` / `manual_required` / `blocked`）。
        signal: String,
        /// 信号原因。
        signal_reason: String,
        /// 总体状态（pass/partial/fail）。
        overall_status: String,
        /// 验证耗时（毫秒）。
        duration: i64,
        /// 检查文件数。
        file_count: i64,
        /// 时间戳（原样字符串）。
        timestamp: String,
    },

    /// 直推点 `VerifyCheckService.pushVerifyProgress`（L387）：验证子系统进度。
    /// `result` 为 `FileCheckResult`（含 CheckDetail/TestCheckDetail 嵌套，Phase 2+
    /// 强类型化，暂透传）。
    ///
    /// **Phase 2+ 建模未激活**（APOS）。
    #[serde(rename_all = "camelCase")]
    VerifyProgress {
        /// 检查中的文件路径。
        file_path: String,
        /// 已完成数。
        completed: i64,
        /// 总数。
        total: i64,
        /// 单文件检查结果（透传）。
        result: serde_json::Value,
    },

    /// 直推点 `NotificationService`（RV-4，直接 `convertAndSendToUser`，payload 自
    /// 带 type 字段）：证据包待审批通知。注意：旧路径**不携带顶层 `ts`**（见
    /// envelope 文档的差异记录）。
    ///
    /// **Phase 2+ 建模未激活**（RV-4）。
    #[serde(rename_all = "camelCase")]
    VerifyAttention {
        /// 会话 ID。
        session_id: String,
        /// 证据包 ID。
        bundle_id: String,
        /// 判定（failed/inconclusive）。
        verdict: String,
        /// Agent 声称完成的内容。
        #[serde(skip_serializing_if = "Option::is_none")]
        claim: Option<String>,
        /// 简要描述。
        #[serde(skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
        /// 是否需要用户批准。
        requires_approval: bool,
        /// ISO-8601 时间戳（原样字符串）。
        timestamp: String,
    },

    /// 直推点 `pushBindError`（L1734）：bind-session 握手失败（协议版本 / 绑定
    /// 参数 / 会话缺失等工作区级错误）。
    ///
    /// **Phase 1 激活**（bind-session 错误路径与 `session_restored` 成对，缺此则
    /// 握手失败无消息可发）。
    #[serde(rename_all = "camelCase")]
    ProtocolError {
        /// 错误码（`UPGRADE_REQUIRED` / `SESSION_NOT_FOUND` / `BIND_FAILED` 等）。
        code: String,
        /// 服务端支持的协议版本。
        supported_version: i64,
        /// 绑定请求 ID（可用时回显）。
        #[serde(skip_serializing_if = "Option::is_none")]
        bind_request_id: Option<String>,
        /// 连接绑定纪元（>=0 时回显）。
        #[serde(skip_serializing_if = "Option::is_none")]
        binding_epoch: Option<i64>,
    },

    /// 直推点 `pushInteractionView(request, "interaction_created")`（L307）：交互
    /// 创建（权限请求 / 澄清问题的持久化生命周期入口）。
    ///
    /// **Phase 1 激活**（权限与澄清是 Phase 1 鉴权链路的一部分；激活但 Phase 1
    /// 后端仅在需要时发送）。
    #[serde(rename_all = "camelCase")]
    InteractionCreated {
        /// 交互视图（全字段建模，多数可缺省）。
        #[serde(flatten)]
        view: InteractionView,
    },

    /// 直推点 `pushInteractionView(request, "interaction_terminal")`（L279）：交互
    /// 终结（已决策 / 超时 / 撤销）。
    ///
    /// **Phase 1 激活**（与 `interaction_created` 成对闭环）。
    #[serde(rename_all = "camelCase")]
    InteractionTerminal {
        /// 交互视图。
        #[serde(flatten)]
        view: InteractionView,
    },

    /// 直推点两条：`pushInteractionView`（L314，完整视图）与 ACK 路径
    /// （L1207，仅 `interactionId`/`decisionDeadlineAt`/`serverNow`/`version` 四
    /// 字段）——按并集建模，全 Option 字段兼容两种形状：交互更新。
    ///
    /// **Phase 1 激活**（ACK 闭环属于 ws 层基础语义）。
    #[serde(rename_all = "camelCase")]
    InteractionUpdated {
        /// 交互视图。
        #[serde(flatten)]
        view: InteractionView,
    },

    /// 直推点 `handleRunInput` QUEUED/APPLYING（L716）：追加指令已排队 / 应用中。
    ///
    /// **Phase 2+ 建模未激活**（run 输入队列随引擎多轮化引入）。
    #[serde(rename_all = "camelCase")]
    RunInputQueued {
        /// 请求 ID。
        request_id: String,
        /// 提交时间（epoch 毫秒）。
        submitted_at: i64,
    },

    /// 直推点 `handleRunInput` APPLIED（L720）与 `onStreamEvent`（L1133）：追加
    /// 指令已应用到运行中的 Run。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    RunInputApplied {
        /// 请求 ID。
        request_id: String,
        /// 追加的指令文本。
        text: String,
        /// 应用时间（epoch 毫秒）。
        applied_at: i64,
    },

    /// 直推点 `pushRunInputRejected`（L734）：追加指令被拒绝。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    RunInputRejected {
        /// 请求 ID。
        request_id: String,
        /// 拒绝码。
        code: String,
        /// 拒绝原因。
        message: String,
        /// 拒绝时间（epoch 毫秒）。
        rejected_at: i64,
    },

    /// 直推点 `McpProgressTracker`（L82，自行组装信封）：MCP 长操作进度。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    McpToolProgress {
        /// 进度令牌。
        progress_token: String,
        /// 服务器名。
        server_name: String,
        /// 工具名。
        tool_name: String,
        /// 当前进度。
        progress: f64,
        /// 总量。
        total: f64,
        /// 进度消息。
        message: String,
    },
}

/// 工具结果内容体——旧嵌套 record `ServerMessage.ToolResult.ToolResultContent`。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolResultContent {
    /// 结果文本。
    pub content: String,
    /// 是否出错。
    pub is_error: bool,
    /// 结构化结果元数据（仅 `structuredResult` 键；可缺省）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// 反向提问选项——旧嵌套 record `Elicitation.ElicitationOption`。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElicitationOption {
    /// 选项值。
    pub value: String,
    /// 选项显示标签。
    pub label: String,
}

/// MCP 工具元数据——旧嵌套 record `McpToolUpdate.McpToolInfo`。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpToolInfo {
    /// 工具名。
    pub name: String,
    /// 工具描述。
    pub description: String,
    /// JSON Schema 入参定义。
    pub input_schema: serde_json::Value,
}

/// 恢复会话元信息——旧嵌套 record `SessionRestored.SessionMetadata`。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionMetadata {
    /// 会话 ID。
    pub session_id: String,
    /// 模型标识。
    pub model: String,
    /// 权限模式（`DEFAULT` / `AUTO_APPROVE` 等）。
    pub permission_mode: String,
    /// 会话状态（idle 等）。
    pub status: String,
}

/// Worker 快照——旧嵌套 record `SwarmStateUpdate.WorkerSnapshot`。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerSnapshot {
    /// Worker ID。
    pub worker_id: String,
    /// 状态。
    pub status: String,
    /// 当前任务（可空）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_task: Option<String>,
    /// 工具调用次数。
    pub tool_call_count: i64,
    /// 消耗 token 数。
    pub token_consumed: i64,
}

/// 交互视图——旧 `interaction/InteractionView.java`（27 字段），`interaction_created`
/// / `interaction_updated` / `interaction_terminal` 共用载荷（经
/// `objectMapper.convertValue(view, Map)` 平铺进信封）。
///
/// 时间字段为 [`FlexEpoch`]：旧 Jackson 默认输出 ISO-8601 字符串，而 ACK 路径显式
/// `toEpochMilli()`，双格式均需可解（见 `model::FlexEpoch` 文档）。除标识字段外
/// 全部可缺省，以兼容 ACK 路径的 4 字段子集形状。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InteractionView {
    /// 交互 ID。
    pub interaction_id: String,
    /// 协议版本（permission=3，elicitation=2）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<i64>,
    /// 关联键（权限场景为 toolUseId）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_key: Option<String>,
    /// 会话 ID。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Run ID。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// 交互类型（`permission` / `elicitation` / `plan_approval`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interaction_type: Option<String>,
    /// 状态（pending/received/decided/expired/...）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// 提示载荷（toolUseId/toolName/inputSummary/riskLevel/reason 或问题结构）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<serde_json::Value>,
    /// 允许的决策列表（allow/deny/...）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_decisions: Option<Vec<String>>,
    /// 可选授权范围。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_options: Option<Vec<String>>,
    /// 已提交的决策响应。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<serde_json::Value>,
    /// 来源（subagent 等）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// 子代理会话 ID。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_session_id: Option<String>,
    /// 发起 Run ID。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor_run_id: Option<String>,
    /// 发起者类型。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor_type: Option<String>,
    /// 投递代际（ACK 幂等键）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_generation: Option<i64>,
    /// 已投递次数。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dispatch_attempts: Option<i64>,
    /// 创建时间。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<FlexEpoch>,
    /// 前端确认接收时间。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_at: Option<FlexEpoch>,
    /// 决策截止时间。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_deadline_at: Option<FlexEpoch>,
    /// 投递窗口截止时间。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_window_ends_at: Option<FlexEpoch>,
    /// 决策时间。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decided_at: Option<FlexEpoch>,
    /// 终结原因。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<String>,
    /// 乐观并发版本号。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i64>,
    /// 服务端当前时间（epoch 毫秒）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_now: Option<i64>,
    /// 操作哈希（权限幂等去重）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_hash: Option<String>,
    /// 澄清问题选项（elicitation）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<serde_json::Value>>,
}

impl ServerMessage {
    /// 返回该消息的 `type` 标签字符串（与旧白名单逐字一致的 `snake_case` 名）。
    ///
    /// 以序列化实际产物为准（而非手写 match），保证「kind 输出 == 线上 type」
    /// 永不漂移：任一 variant 序列化后的 `type` 字段即本方法返回值。
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            ServerMessage::StreamDelta { .. } => "stream_delta",
            ServerMessage::ThinkingDelta { .. } => "thinking_delta",
            ServerMessage::ToolUseStart { .. } => "tool_use_start",
            ServerMessage::ToolUseProgress { .. } => "tool_use_progress",
            ServerMessage::ToolResult { .. } => "tool_result",
            ServerMessage::ToolUseInput { .. } => "tool_use_input",
            ServerMessage::ToolPermissionDenied { .. } => "tool_permission_denied",
            ServerMessage::PermissionRequest { .. } => "permission_request",
            ServerMessage::MessageComplete { .. } => "message_complete",
            ServerMessage::Error { .. } => "error",
            ServerMessage::CompactStart => "compact_start",
            ServerMessage::CompactComplete { .. } => "compact_complete",
            ServerMessage::Elicitation { .. } => "elicitation",
            ServerMessage::AgentSpawn { .. } => "agent_spawn",
            ServerMessage::AgentUpdate { .. } => "agent_update",
            ServerMessage::AgentComplete { .. } => "agent_complete",
            ServerMessage::AgentStarted { .. } => "agent_started",
            ServerMessage::AgentCompleted { .. } => "agent_completed",
            ServerMessage::AgentFailed { .. } => "agent_failed",
            ServerMessage::CostUpdate { .. } => "cost_update",
            ServerMessage::RateLimit { .. } => "rate_limit",
            ServerMessage::Notification { .. } => "notification",
            ServerMessage::TaskUpdate { .. } => "task_update",
            ServerMessage::PromptSuggestion { .. } => "prompt_suggestion",
            ServerMessage::BridgeStatus { .. } => "bridge_status",
            ServerMessage::TeammateMessage { .. } => "teammate_message",
            ServerMessage::SpeculationResult { .. } => "speculation_result",
            ServerMessage::McpToolUpdate { .. } => "mcp_tool_update",
            ServerMessage::SessionRestored { .. } => "session_restored",
            ServerMessage::Pong { .. } => "pong",
            ServerMessage::CompactEvent { .. } => "compact_event",
            ServerMessage::TokenWarning { .. } => "token_warning",
            ServerMessage::InterruptAck { .. } => "interrupt_ack",
            ServerMessage::ModelChanged { .. } => "model_changed",
            ServerMessage::PermissionModeChanged { .. } => "permission_mode_changed",
            ServerMessage::CommandResult { .. } => "command_result",
            ServerMessage::RewindComplete { .. } => "rewind_complete",
            ServerMessage::McpHealthStatus { .. } => "mcp_health_status",
            ServerMessage::TokenBudgetNudge { .. } => "token_budget_nudge",
            ServerMessage::SwarmStateUpdate { .. } => "swarm_state_update",
            ServerMessage::WorkerProgress { .. } => "worker_progress",
            ServerMessage::WorkflowPhaseUpdate { .. } => "workflow_phase_update",
            ServerMessage::ModelRouted { .. } => "model_routed",
            ServerMessage::PlanUpdate { .. } => "plan_update",
            ServerMessage::SessionListUpdated => "session_list_updated",
            ServerMessage::Visualization { .. } => "visualization",
            ServerMessage::VerificationResult { .. } => "verification_result",
            ServerMessage::VerifyProgress { .. } => "verify_progress",
            ServerMessage::VerifyAttention { .. } => "verify_attention",
            ServerMessage::ProtocolError { .. } => "protocol_error",
            ServerMessage::InteractionCreated { .. } => "interaction_created",
            ServerMessage::InteractionTerminal { .. } => "interaction_terminal",
            ServerMessage::InteractionUpdated { .. } => "interaction_updated",
            ServerMessage::RunInputQueued { .. } => "run_input_queued",
            ServerMessage::RunInputApplied { .. } => "run_input_applied",
            ServerMessage::RunInputRejected { .. } => "run_input_rejected",
            ServerMessage::McpToolProgress { .. } => "mcp_tool_progress",
        }
    }

    /// 该消息是否属于 Phase 1 激活子集（与各 variant 文档标注一致）。
    ///
    /// 激活集：`stream_delta` / `thinking_delta` / `message_complete` / `error`（含
    /// `query_busy` code）/ `pong` / `session_restored` / `session_list_updated` /
    /// `model_changed` / `permission_mode_changed` / `protocol_error` /
    /// `interaction_created` / `interaction_terminal` / `interaction_updated`。
    #[must_use]
    pub fn is_phase1_active(&self) -> bool {
        matches!(
            self,
            ServerMessage::StreamDelta { .. }
                | ServerMessage::ThinkingDelta { .. }
                | ServerMessage::MessageComplete { .. }
                | ServerMessage::Error { .. }
                | ServerMessage::Pong { .. }
                | ServerMessage::SessionRestored { .. }
                | ServerMessage::SessionListUpdated
                | ServerMessage::ModelChanged { .. }
                | ServerMessage::PermissionModeChanged { .. }
                | ServerMessage::ProtocolError { .. }
                | ServerMessage::InteractionCreated { .. }
                | ServerMessage::InteractionTerminal { .. }
                | ServerMessage::InteractionUpdated { .. }
        )
    }
}
