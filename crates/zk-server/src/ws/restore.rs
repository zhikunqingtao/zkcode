//! bind 恢复载荷组装——zk-db 存储形状 → WS 线上形状（U1 扁平信封的消息体侧）。
//!
//! 存储 → WS 消息映射本体已上收 `zk_db::convert`（S9 回改 D-S9-2：引擎组装
//! `committedMessages` 复用同一映射，禁止双实现漂移），本模块只负责
//! `session_restored` 载荷的组装与元信息。映射语义对齐旧
//! `WebSocketController.convertMessagesForWs` /
//! `convertContentBlocksForWs`（L1905-1988）：
//!
//! - `StoredBlock`（存储 `snake_case`）→ `ContentBlock`（WS `camelCase`）：
//!   `tool_use.id/name` → `toolUseId/toolName`；`image.source.{media_type,data}`
//!   拍平为 `mediaType/base64Data` 并**丢弃 `width/height`**；
//!   `redacted_thinking.data` 丢弃（WS 形状无字段）；
//! - `MessageRecord` → `Message`：`uuid = 行 id`、`timestamp = created_at`
//!   （epoch 毫秒）；assistant 的 `usage` 仅在 `input+output > 0` 时携带
//!   （其余字段缓存计数 Phase 1 无来源，恒 0）；system 消息内容取全部 text
//!   块拼接（存储层裸文本回退产物即单 text 块）；
//! - `session_restored` 元信息：`permission_mode` 取 `PermissionModeManager`
//!   的会话权威值（旧 `WebSocketController.java:1657`
//!   `permissionModeManager.getMode(sessionId).name()`；未显式设置回退
//!   `DEFAULT`）、`status` 取库内小写状态。

use zk_authz::model::PermissionMode;
use zk_db::convert::record_to_ws_message;
use zk_db::model::SessionDetail;
use zk_protocol::model::Message as WsMessage;
use zk_protocol::{ServerMessage, SessionMetadata};

use super::WS_PROTOCOL_VERSION;
use crate::iso::now_millis;

/// 组装 `session_restored`（bind 成功路径的响应体；直发不带路由字段）。
#[must_use]
pub(crate) fn build_session_restored(
    detail: SessionDetail,
    bind_request_id: Option<String>,
    binding_epoch: u64,
    permission_mode: PermissionMode,
) -> ServerMessage {
    let messages: Vec<WsMessage> = detail
        .messages
        .into_iter()
        .map(record_to_ws_message)
        .collect();
    ServerMessage::SessionRestored {
        messages,
        metadata: SessionMetadata {
            session_id: detail.session_id,
            model: detail.model,
            permission_mode: permission_mode.as_str().to_owned(),
            status: detail.status,
        },
        // 活动列表 / run 快照 / 工具调用投影：Phase 2+（S5 双表无对应存储）。
        activities: None,
        total_activity_count: None,
        has_more: None,
        protocol_version: WS_PROTOCOL_VERSION,
        bind_request_id,
        binding_epoch: Some(i64::try_from(binding_epoch).unwrap_or(i64::MAX)),
        server_now: Some(now_millis()),
        run_snapshot: None,
        snapshot_event_seq: None,
        active_tool_calls: None,
        cost_summary: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zk_db::model::{MessageRecord, MessageRole, StoredBlock};
    use zk_protocol::model::Usage;

    /// 最小 `SessionDetail` 构造（消息与状态可注入）。
    fn detail(messages: Vec<MessageRecord>, status: &str) -> SessionDetail {
        SessionDetail {
            session_id: "s-1".to_owned(),
            model: "qwen3.7-max".to_owned(),
            working_dir: "/tmp".to_owned(),
            title: None,
            status: status.to_owned(),
            messages,
            config: serde_json::Map::new(),
            total_usage: Usage::default(),
            total_cost_usd: 0.0,
            summary: None,
            created_at: 1_000,
            updated_at: 1_000,
        }
    }

    #[test]
    fn restored_carries_bind_fields_and_default_permission_mode() {
        let restored = build_session_restored(
            detail(Vec::new(), "active"),
            Some("br-1".into()),
            4,
            PermissionMode::Default,
        );
        let value = serde_json::to_value(&restored).expect("json");
        assert_eq!(value["type"], "session_restored");
        assert_eq!(value["protocolVersion"], 3);
        assert_eq!(value["bindRequestId"], "br-1");
        assert_eq!(value["bindingEpoch"], 4);
        assert_eq!(value["metadata"]["permissionMode"], "DEFAULT");
        assert_eq!(value["metadata"]["status"], "active");
        assert_eq!(value["metadata"]["model"], "qwen3.7-max");
        assert!(value["serverNow"].as_i64().is_some());
        assert!(value.get("activities").is_none(), "phase2 fields absent");
        assert!(value.get("runSnapshot").is_none());
    }

    #[test]
    fn storage_blocks_map_to_ws_shape() {
        let record = MessageRecord {
            id: "m-1".to_owned(),
            session_id: "s-1".to_owned(),
            role: MessageRole::Assistant,
            content: vec![
                StoredBlock::Text {
                    text: "hi".to_owned(),
                },
                StoredBlock::ToolUse {
                    id: "toolu_9".to_owned(),
                    name: "Read".to_owned(),
                    input: serde_json::json!({"path": "/a"}),
                },
                StoredBlock::ToolResult {
                    tool_use_id: "toolu_9".to_owned(),
                    content: "ok".to_owned(),
                    is_error: false,
                    metadata: None,
                },
                StoredBlock::Image {
                    source: zk_db::model::ImageSource {
                        kind: "base64".to_owned(),
                        media_type: "image/png".to_owned(),
                        data: "AAAA".to_owned(),
                    },
                    width: Some(640),
                    height: None,
                },
                StoredBlock::RedactedThinking {
                    data: "blob".to_owned(),
                },
            ],
            stop_reason: Some("end_turn".to_owned()),
            input_tokens: 10,
            output_tokens: 5,
            seq_num: 1,
            created_at: 1_234,
        };
        let restored = build_session_restored(
            detail(vec![record], "active"),
            None,
            1,
            PermissionMode::Default,
        );
        let value = serde_json::to_value(&restored).expect("json");
        let msg = &value["messages"][0];
        assert_eq!(msg["type"], "assistant");
        assert_eq!(msg["uuid"], "m-1");
        assert_eq!(msg["timestamp"], 1_234);
        assert_eq!(msg["stopReason"], "end_turn");
        assert_eq!(msg["usage"]["inputTokens"], 10);
        assert_eq!(msg["usage"]["outputTokens"], 5);
        // camelCase 重映射 + image 拍平（width 丢弃）+ redacted 无 data。
        let content = msg["content"].as_array().expect("content array");
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[1]["toolUseId"], "toolu_9");
        assert_eq!(content[1]["toolName"], "Read");
        assert_eq!(content[2]["toolUseId"], "toolu_9");
        assert_eq!(content[3]["mediaType"], "image/png");
        assert_eq!(content[3]["base64Data"], "AAAA");
        assert!(content[3].get("width").is_none());
        assert_eq!(content[4]["type"], "redacted_thinking");
        assert!(content[4].get("data").is_none());
    }

    #[test]
    fn assistant_without_tokens_omits_usage_and_system_joins_text() {
        let assistant = MessageRecord {
            id: "m-2".to_owned(),
            session_id: "s-1".to_owned(),
            role: MessageRole::Assistant,
            content: vec![StoredBlock::Text {
                text: "plain".to_owned(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            seq_num: 2,
            created_at: 2_000,
        };
        let system = MessageRecord {
            id: "m-3".to_owned(),
            session_id: "s-1".to_owned(),
            role: MessageRole::System,
            content: vec![
                StoredBlock::Text {
                    text: "a".to_owned(),
                },
                StoredBlock::Text {
                    text: "b".to_owned(),
                },
            ],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            seq_num: 3,
            created_at: 3_000,
        };
        let restored = build_session_restored(
            detail(vec![assistant, system], "closed"),
            None,
            1,
            PermissionMode::Default,
        );
        let value = serde_json::to_value(&restored).expect("json");
        assert!(value["messages"][0].get("usage").is_none());
        assert!(value["messages"][0].get("stopReason").is_none());
        assert_eq!(value["messages"][1]["type"], "system");
        assert_eq!(value["messages"][1]["content"], "ab");
    }
}
