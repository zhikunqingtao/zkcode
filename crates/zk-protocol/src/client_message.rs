//! 上行消息（Client → Server）全量建模：**15 个 variant = 旧 11 个顶层 payload
//! record（+2 嵌套）+ 4 个 Map-payload 入口**。
//!
//! - 旧 15 个 `@MessageMapping` 入口中 11 个有 `ClientMessage` record，其余
//!   四个（`/app/bind-session`、`/app/interaction-received`、
//!   `/app/activity-save` 与 `/app/activity-update`）以 `Map<String,Object>`
//!   接收，按 handler 实际读取的键建模。
//! - 旧前端的 `evidence_decision` 没有后端消费者，已从 WS 契约移除；证据审批
//!   统一走 REST Evidence API。
//! - 上行 type 命名对齐改造方案 §11.2（`user_message` / `run_input` /
//!   `permission_response` 等）；Map 入口按 destination 语义命名。
//! - 字段 camelCase；attachment / reference 嵌套结构照抄 Java record。

use serde::{Deserialize, Serialize};

/// 上行消息枚举——15 个 type 全量冻结。
///
/// 旧 STOMP 上行 body **不含** type 字段（靠 destination 路由）；新原生 WS 协议
/// 上行携带顶层 `type`（信封见 [`crate::ClientEnvelope`]），由服务端按 type 路由，
/// 替代 STOMP destination 语义（改造方案 §4.2：上行固定走 `main` 通道）。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// 旧 record `UserMessagePayload`（/app/chat）：用户消息。
    ///
    /// **Phase 1 激活**。
    #[serde(rename_all = "camelCase")]
    UserMessage {
        /// 消息文本。
        text: String,
        /// 附件列表（图片等）。
        #[serde(skip_serializing_if = "Option::is_none")]
        attachments: Option<Vec<Attachment>>,
        /// 文件引用区间列表。
        #[serde(skip_serializing_if = "Option::is_none")]
        references: Option<Vec<Reference>>,
    },

    /// 旧 record `RunInputPayload`（/app/run-input）：向运行中的根 Run 追加指令。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    RunInput {
        /// 请求 ID。
        request_id: String,
        /// 追加指令文本。
        text: String,
    },

    /// 旧 record `PermissionResponsePayload`（/app/permission）：权限决策响应。
    ///
    /// 注意：协议 v2 下服务端回复 `error(code=interaction_rest_required)`，决策
    /// 须走 REST `/api/interactions/{id}/decisions`；类型仍冻结进契约（迁移期
    /// 前端零改动需要）。
    ///
    /// **Phase 2+ 建模未激活**（Phase 1 权限走 interaction 通道 + REST）。
    #[serde(rename_all = "camelCase")]
    PermissionResponse {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 决策（`allow` / `deny` / `allow_always`）。
        decision: String,
        /// 是否记忆决策。
        remember: bool,
        /// 记忆范围（`session` / `project` / `global`）。
        scope: String,
    },

    /// 旧 record `InterruptPayload`（/app/interrupt）：中断当前回合。旧 handler
    /// 以可选 Map 接收并读取 `isSubmitInterrupt`，纳入建模。
    ///
    /// **Phase 1 激活**（用户中断属于基础交互）。
    #[serde(rename_all = "camelCase")]
    Interrupt {
        /// 是否为「提交输入引发的中断」（`SUBMIT_INTERRUPT`）。
        #[serde(skip_serializing_if = "Option::is_none")]
        is_submit_interrupt: Option<bool>,
    },

    /// 旧 record `SetModelPayload`（/app/model）：切换 LLM 模型。
    ///
    /// **Phase 1 激活**。
    #[serde(rename_all = "camelCase")]
    SetModel {
        /// 目标模型标识。
        model: String,
    },

    /// 旧 record `SetPermissionModePayload`（/app/permission-mode）：切换权限模式。
    ///
    /// **Phase 1 激活**。
    #[serde(rename_all = "camelCase")]
    SetPermissionMode {
        /// 目标模式（`DEFAULT` / `AUTO_APPROVE` 等）。
        mode: String,
    },

    /// 旧 record `SlashCommandPayload`（/app/command）：Slash 命令。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    SlashCommand {
        /// 命令名（不含 `/`）。
        command: String,
        /// 参数（可为空串）。
        args: String,
    },

    /// 旧 record `McpOperationPayload`（/app/mcp）：MCP 服务器操作。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    McpOperation {
        /// 操作（`connect` / `disconnect` / `restart` / `list_tools` / `approve` / `list`）。
        operation: String,
        /// 服务器 ID。
        server_id: String,
        /// 附加配置（connect 场景）。
        #[serde(skip_serializing_if = "Option::is_none")]
        config: Option<serde_json::Value>,
    },

    /// 旧 record `RewindFilesPayload`（/app/rewind）：回退文件。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    RewindFiles {
        /// 锚点消息 ID。
        message_id: String,
        /// 待回退文件路径列表。
        file_paths: Vec<String>,
    },

    /// 旧 record `ElicitationResponsePayload`（/app/elicitation）：反向提问响应。
    /// 协议 v2 下服务端回复 `interaction_rest_required`（同 `PermissionResponse`）。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    ElicitationResponse {
        /// 请求 ID。
        request_id: String,
        /// 答案（选项值或自由文本，任意 JSON）。
        answer: serde_json::Value,
    },

    /// 旧 record `PingPayload`（/app/ping）：应用层心跳探测。
    ///
    /// **Phase 1 激活**（D1 双轨心跳的应用层一轨）。
    Ping,

    /// 旧 Map 入口 `/app/bind-session`（handler 读取 sessionId / bindRequestId /
    /// bindingEpoch / protocolVersion）：绑定会话到 WS 连接。
    ///
    /// **Phase 1 激活**（握手前置，`session_restored` 的触发方）。
    #[serde(rename_all = "camelCase")]
    BindSession {
        /// 应用层会话 ID。
        session_id: String,
        /// 绑定请求 ID（防回放，服务端在响应中回显）。
        bind_request_id: String,
        /// 连接绑定纪元（>=1，防跨连接错投）。
        binding_epoch: i64,
        /// WS 协议版本（当前 3）。
        protocol_version: i64,
    },

    /// 旧 Map 入口 `/app/interaction-received`（handler 读取 interactionId /
    /// deliveryGeneration）：交互接收确认（ACK，幂等去重的客户端半边）。
    ///
    /// **Phase 1 激活**（与 `interaction_updated` ACK 响应成对）。
    #[serde(rename_all = "camelCase")]
    InteractionAck {
        /// 交互 ID。
        interaction_id: String,
        /// 投递代际（服务端以此判重）。
        delivery_generation: i64,
    },

    /// 旧 Map 入口 `/app/activity-save`（handler 读取 id / operationType /
    /// summary / status / timestamp / duration / fileCount / decision /
    /// toolResult / changedFiles / insight）：保存 Activity。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    ActivitySave {
        /// 活动 ID。
        id: String,
        /// 操作类型。
        operation_type: String,
        /// 摘要。
        #[serde(skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
        /// 状态。
        #[serde(skip_serializing_if = "Option::is_none")]
        status: Option<String>,
        /// 时间戳（epoch 毫秒；缺省由服务端补当前时间）。
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<i64>,
        /// 耗时毫秒。
        #[serde(skip_serializing_if = "Option::is_none")]
        duration: Option<i64>,
        /// 涉及文件数。
        #[serde(skip_serializing_if = "Option::is_none")]
        file_count: Option<i64>,
        /// 决策。
        #[serde(skip_serializing_if = "Option::is_none")]
        decision: Option<String>,
        /// 工具结果（JSON，服务端 10KB 有界截断）。
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_result: Option<serde_json::Value>,
        /// 变更文件列表（JSON）。
        #[serde(skip_serializing_if = "Option::is_none")]
        changed_files: Option<serde_json::Value>,
        /// 洞察（JSON）。
        #[serde(skip_serializing_if = "Option::is_none")]
        insight: Option<serde_json::Value>,
    },

    /// 旧 Map 入口 `/app/activity-update`（handler 读取 id / decision / insight）：
    /// 更新 Activity 的决策与洞察字段。
    ///
    /// **Phase 2+ 建模未激活**。
    #[serde(rename_all = "camelCase")]
    ActivityUpdate {
        /// 活动 ID。
        id: String,
        /// 决策。
        #[serde(skip_serializing_if = "Option::is_none")]
        decision: Option<String>,
        /// 洞察（JSON）。
        #[serde(skip_serializing_if = "Option::is_none")]
        insight: Option<serde_json::Value>,
    },
}

/// 消息附件——旧嵌套 record `UserMessagePayload.Attachment`。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Attachment {
    /// 附件类型（image 等）。
    #[serde(rename = "type")]
    pub kind: String,
    /// 文件路径（本地引用场景）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// MIME 类型（image/png 等）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// base64 数据（内联场景）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base64_data: Option<String>,
    /// 远程 URL（链接场景）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// 文件引用区间——旧嵌套 record `UserMessagePayload.Reference`。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reference {
    /// 引用类型。
    #[serde(rename = "type")]
    pub kind: String,
    /// 文件路径。
    pub path: String,
    /// 起始行（1-based）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_line: Option<i64>,
    /// 结束行（含）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_line: Option<i64>,
}

impl ClientMessage {
    /// 返回该消息的 `type` 标签字符串（新协议上行路由键）。
    ///
    /// 与 [`ServerMessage::kind`] 同理：以序列化实际产物为准，保证与契约
    /// type 逐字一致。
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            ClientMessage::UserMessage { .. } => "user_message",
            ClientMessage::RunInput { .. } => "run_input",
            ClientMessage::PermissionResponse { .. } => "permission_response",
            ClientMessage::Interrupt { .. } => "interrupt",
            ClientMessage::SetModel { .. } => "set_model",
            ClientMessage::SetPermissionMode { .. } => "set_permission_mode",
            ClientMessage::SlashCommand { .. } => "slash_command",
            ClientMessage::McpOperation { .. } => "mcp_operation",
            ClientMessage::RewindFiles { .. } => "rewind_files",
            ClientMessage::ElicitationResponse { .. } => "elicitation_response",
            ClientMessage::Ping => "ping",
            ClientMessage::BindSession { .. } => "bind_session",
            ClientMessage::InteractionAck { .. } => "interaction_ack",
            ClientMessage::ActivitySave { .. } => "activity_save",
            ClientMessage::ActivityUpdate { .. } => "activity_update",
        }
    }
}
