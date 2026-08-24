//! 存储形状 → WS 线上形状的唯一权威转换（S9 回改引入）。
//!
//! 语义逐条对齐旧 `WebSocketController.convertMessagesForWs` /
//! `convertContentBlocksForWs`（L1905-1988）；原实现位于 zk-server
//! `ws/restore.rs`（S8 交付，crate 私有）。S9 引擎组装 `message_complete.
//! committedMessages` 需要同一映射，而 zk-engine 依赖方向禁止引用
//! zk-server——本 crate 同时知晓 [`StoredBlock`]（自有）与
//! `zk_protocol::ContentBlock`（既有依赖），是该映射的唯一合法单点，
//! 故上收至此，restore.rs 改为委托（决策 D-S9-2，防双实现漂移）。
//!
//! 映射偏离清单（可接受丢弃，均对齐旧转换层）：
//! - `image.source.{media_type,data}` 拍平为 `mediaType/base64Data`，
//!   **丢弃 `width/height`**；
//! - `redacted_thinking.data` 丢弃（WS 形状无字段）；
//! - assistant `usage` 仅 `input+output > 0` 时携带（其余计数 Phase 1 无来源）；
//! - system 消息内容取全部 text 块拼接。

use zk_protocol::model::{ContentBlock, Message as WsMessage, Usage};

use crate::model::{MessageRecord, MessageRole, StoredBlock};

/// `MessageRecord` → 协议 `Message`（角色三分支）。
#[must_use]
pub fn record_to_ws_message(record: MessageRecord) -> WsMessage {
    match record.role {
        MessageRole::User => WsMessage::User {
            uuid: record.id,
            timestamp: record.created_at,
            content: blocks_to_ws(record.content),
        },
        MessageRole::Assistant => {
            let usage = (record.input_tokens + record.output_tokens > 0).then(|| Usage {
                input_tokens: record.input_tokens,
                output_tokens: record.output_tokens,
                ..Usage::default()
            });
            WsMessage::Assistant {
                uuid: record.id,
                timestamp: record.created_at,
                content: blocks_to_ws(record.content),
                stop_reason: record.stop_reason,
                usage,
            }
        }
        MessageRole::System => {
            let content = record
                .content
                .iter()
                .filter_map(|block| match block {
                    StoredBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("");
            WsMessage::System {
                uuid: record.id,
                timestamp: record.created_at,
                content,
            }
        }
    }
}

/// `StoredBlock`（存储 `snake_case`）→ `ContentBlock`（WS `camelCase`）。
#[must_use]
pub fn blocks_to_ws(blocks: Vec<StoredBlock>) -> Vec<ContentBlock> {
    blocks
        .into_iter()
        .map(|block| match block {
            StoredBlock::Text { text } => ContentBlock::Text { text },
            StoredBlock::ToolUse { id, name, input } => ContentBlock::ToolUse {
                tool_use_id: id,
                tool_name: name,
                input,
            },
            StoredBlock::ToolResult {
                tool_use_id,
                content,
                is_error,
                metadata,
            } => ContentBlock::ToolResult {
                tool_use_id,
                content,
                is_error,
                metadata,
            },
            StoredBlock::Thinking { thinking } => ContentBlock::Thinking { thinking },
            // WS 转换层丢弃 width/height（zk-protocol model 模块文档丢弃清单）。
            StoredBlock::Image { source, .. } => ContentBlock::Image {
                media_type: source.media_type,
                base64_data: source.data,
            },
            // WS 形状无字段（data 被转换层丢弃）。
            StoredBlock::RedactedThinking { .. } => ContentBlock::RedactedThinking,
        })
        .collect()
}
