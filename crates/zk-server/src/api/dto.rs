//! REST 线上 DTO——旧 Controller DTO records 与 `Message`/`ContentBlock`
//! 的 REST 通道序列化形状（S5 源码核实 + S2 实采样例锁定）。
//!
//! # 三套消息形状的边界（勿混淆）
//!
//! 旧系统同一内容块在三个通道形状互异，zk-server 负责 REST 出口映射：
//! - **存储**（zk-db `StoredBlock`，`content_json`）：`snake_case`，
//!   image 为 `source:{type,media_type,data}` 嵌套；
//! - **WS**（zk-protocol `ContentBlock`，`convertContentBlocksForWs`）：
//!   camelCase（`toolUseId` / `mediaType` / `base64Data`，丢 `width/height`）；
//! - **REST**（本模块 [`ApiBlock`]，`Message.java` 直接 Jackson 序列化）：
//!   `tool_use`/`tool_result` 蛇形字段 + image 平铺 `mediaType`/`base64Data`/
//!   `width`/`height`（`ContentBlock.java` 的 `@JsonProperty` 实查）。
//!
//! # `NON_NULL` 语义
//!
//! 旧全局 Jackson `default-property-inclusion: non_null`：null 字段剥离。
//! 本模块以 `skip_serializing_if = "Option::is_none"` 表达；无法用 serde
//! 属性表达的动态剥离（详情端点的 `title`/`summary`）在 `mapping` 中以
//! 递归 `strip_nulls` 完成。

use serde::Serialize;
use zk_protocol::Usage;

/// `POST /api/sessions` 请求体（对齐旧 `CreateSessionRequest`，全可选）。
#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateSessionRequest {
    /// 项目 ID（2.1 消费：非空时解析项目 workspace 为会话工作目录，
    /// 未知 id → 404 `PROJECT_NOT_FOUND`；缺省保持 Phase 1 行为）。
    pub project_id: Option<String>,
    /// 旧系统设计上拒绝的字段（非空 → 400 `SESSION_WORKING_DIRECTORY_UNSUPPORTED`）。
    pub working_directory: Option<String>,
    /// 模型（空 → 默认模型）。
    pub model: Option<String>,
    /// 权限模式（空 → `DEFAULT`）。
    pub permission_mode: Option<String>,
    /// resume 语义占位（Phase 1 不消费；resume 走独立端点）。
    #[allow(dead_code)]
    pub resume_session_id: Option<String>,
}

/// `POST /api/sessions` 201 响应（样例 POST_api-sessions.json 逐键权威）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateSessionResponse {
    /// 会话 ID（UUIDv4）。
    pub session_id: String,
    /// WebSocket 通道路径（`/ws/session/{id}`；Phase 1 REST 骨架先行返回，
    /// WS 端点在后续步骤接线）。
    pub web_socket_url: String,
    /// 实际生效的模型。
    pub model: String,
    /// 权限模式（`DEFAULT`…）。
    pub permission_mode: String,
    /// 创建时刻（RFC 3339）。
    pub created_at: String,
}

/// `DELETE /api/sessions/{id}` 200 响应（样例：`{"success":true}`）。
#[derive(Debug, Serialize)]
pub(crate) struct DeleteSessionResponse {
    /// 恒 `true`（旧语义：删除幂等，不存在亦成功）。
    pub success: bool,
}

/// `POST /api/sessions/{id}/resume` 200 响应（样例逐键权威）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResumeSessionResponse {
    /// 会话 ID。
    pub session_id: String,
    /// WebSocket 通道路径。
    pub web_socket_url: String,
    /// 完整历史消息（REST 线上形状）。
    pub messages: Vec<ApiMessage>,
}

/// `POST /api/sessions/{id}/compact` 200 响应（样例逐键权威）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompactResponse {
    /// 恒 `true`（旧端点语义：尽力压缩，失败在异常层）。
    pub success: bool,
    /// 压缩前估算 token。
    pub tokens_before: i64,
    /// 压缩后估算 token。
    pub tokens_after: i64,
}

/// `GET /api/sessions/{id}/messages` 200 响应（样例键集 `{messages,hasMore}`，
/// `nextCursor` 有值才出现——P0 游标）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessageListResponse {
    /// 本页消息（REST 线上形状）。
    pub messages: Vec<ApiMessage>,
    /// 是否还有后续。
    pub has_more: bool,
    /// 下一页游标（`Base64(十进制索引)`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// REST 通道消息（旧 `Message.java` `@JsonTypeInfo(property="type")` 多态形状）。
///
/// `system` 变体省略旧 record 的 `SystemMessageType type` 组件——它与多态
/// 判别字段 `"type"` 在 Jackson 侧冲突（旧源码歧义点，zk-db 亦不存储该值；
/// 裁定见 S7 报告）。`user` 的 `toolUseResult`/`sourceToolAssistantUUID`
/// 为存储层不持有的可选字段，保留形状占位（`None` 时剥离，对齐 `NON_NULL`）。
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all_fields = "camelCase")]
pub(crate) enum ApiMessage {
    /// 用户消息（`type:"user"`）。
    #[serde(rename = "user")]
    User {
        /// 消息 ID（旧 `uuid`）。
        uuid: String,
        /// 创建时刻（RFC 3339，旧 `timestamp`）。
        timestamp: String,
        /// 内容块。
        content: Vec<ApiBlock>,
        /// 旧版工具结果文本（存储层不持有，恒 `None` → 剥离）。
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_use_result: Option<String>,
        /// 来源助手 UUID（存储层不持有，恒 `None` → 剥离）。
        #[serde(
            rename = "sourceToolAssistantUUID",
            skip_serializing_if = "Option::is_none"
        )]
        source_tool_assistant_uuid: Option<String>,
    },
    /// 助手消息（`type:"assistant"`）。
    #[serde(rename = "assistant")]
    Assistant {
        /// 消息 ID。
        uuid: String,
        /// 创建时刻（RFC 3339）。
        timestamp: String,
        /// 内容块。
        content: Vec<ApiBlock>,
        /// 停止原因（库内 null → `"end_turn"` 兜底，对齐 `mapRowToMessage`）。
        stop_reason: String,
        /// 用量（`Usage(in,out,0,0)`，恒非 null）。
        usage: Usage,
    },
    /// 系统消息（`type:"system"`）。
    #[serde(rename = "system")]
    System {
        /// 消息 ID。
        uuid: String,
        /// 创建时刻（RFC 3339）。
        timestamp: String,
        /// 纯文本内容（text 块 `\n` 连接）。
        content: String,
    },
}

/// REST 通道内容块（旧 `ContentBlock.java` REST 序列化形状）。
///
/// 与存储形状的**唯一结构性分歧**是 image：存储为 `source` 嵌套，
/// REST 为平铺 `mediaType`/`base64Data` + 恒输出的 `width`/`height`
///（旧 Java `int` 原始类型，`NON_NULL` 不剥离，缺省 0）。`tool_result`
/// 的 `is_error`/`metadata` 同理恒输出（旧 `boolean`/紧凑构造器保证非 null）。
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub(crate) enum ApiBlock {
    /// 文本块。
    #[serde(rename = "text")]
    Text {
        /// 文本。
        text: String,
    },
    /// 工具调用块（REST 通道即存储通道形状：`id`/`name`/`input`）。
    #[serde(rename = "tool_use")]
    ToolUse {
        /// 调用 ID。
        id: String,
        /// 工具名。
        name: String,
        /// 工具入参。
        input: serde_json::Value,
    },
    /// 工具结果块。
    #[serde(rename = "tool_result")]
    ToolResult {
        /// 调用 ID。
        tool_use_id: String,
        /// 结果文本。
        content: String,
        /// 是否出错（恒输出）。
        is_error: bool,
        /// 结构化元数据（恒输出，缺省 `{}`）。
        metadata: serde_json::Value,
    },
    /// 思考块。
    #[serde(rename = "thinking")]
    Thinking {
        /// 思考文本。
        thinking: String,
    },
    /// 图片块（平铺形状，REST 通道唯一的 camelCase 块字段）。
    #[serde(rename = "image")]
    Image {
        /// MIME 类型。
        #[serde(rename = "mediaType")]
        media_type: String,
        /// base64 数据。
        #[serde(rename = "base64Data")]
        base64_data: String,
        /// 宽（恒输出，缺省 0）。
        width: i64,
        /// 高（恒输出，缺省 0）。
        height: i64,
    },
    /// 脱敏思考块。
    #[serde(rename = "redacted_thinking")]
    RedactedThinking {
        /// 不透明数据。
        data: String,
    },
}
