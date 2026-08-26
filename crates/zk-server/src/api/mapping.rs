//! 存储形状 → REST 线上形状映射，及 compact 确定性压缩、markdown 导出、
//! token 估算（对齐旧 `TokenCounter` / `CompactService` / `exportAsMarkdown`）。
//!
//! # 三个序列化出口（对应旧系统三套 Jackson 配置）
//!
//! | 出口 | 旧配置 | 本模块 |
//! |---|---|---|
//! | 详情端点 | 全局 mapper：`NON_NULL` + ISO 字符串 | [`detail_to_value`]（null 剥离） |
//! | messages/resume 端点 | 同上（DTO 内已 `skip_serializing_if`） | [`api_messages`] |
//! | export 端点 | 独立 `new ObjectMapper()`：null 保留 + epoch 浮点秒 | [`export_to_value`] |
//!
//! # token 估算的整数化改写
//!
//! 旧 `estimateTokens = ceil(chars / 3.5 + n * 4)`（`double` 运算）。此处以
//! 整数 `ceil((2·chars + 28·n) / 7)` 等价改写（`chars` 计数基准同为字符数，
//! 仅 astral 平面字符与 Java UTF-16 单位有理论偏差，估算用途可忽略），
//! 避免 `f64 → i64` 截断 lints 与浮点误差。

use serde_json::{Map, Value};
use zk_db::model::{MessageRecord, MessageRole, SessionDetail, StoredBlock};
use zk_protocol::Usage;

use crate::api::dto::{ApiBlock, ApiMessage};
use crate::iso::{format_rfc3339_micros, now_millis};

/// `DEFAULT_CHARS_PER_TOKEN = 3.5` 的整数分子/分母（`7 / 2`）。
const CHARS_PER_TOKEN_NUM: i64 = 7;
const CHARS_PER_TOKEN_DEN: i64 = 2;
/// 每条消息的边界开销 token（旧实现 `messages.size() * 4`）。
const PER_MESSAGE_TOKENS: i64 = 4;
/// 压缩触发的最低消息数（旧 `MIN_MESSAGES_FOR_COMPACT = 5`）。
const MIN_MESSAGES_FOR_COMPACT: usize = 5;
/// 常规压缩保留的最近 user 轮次（旧 `PRESERVED_RECENT_TURNS = 3`）。
const PRESERVED_USER_TURNS: usize = 3;
/// 确定性摘要里每条消息的截断长度（字符计）。
const SUMMARY_EXCERPT_MAX_CHARS: usize = 500;
/// 无尺寸信息图片的兜底 token（旧 `estimateImageTokens` 回退值 85）。
const IMAGE_FALLBACK_TOKENS: i64 = 85;
/// 图片 token 系数分母（旧 `width * height / 750`）。
const IMAGE_PIXELS_PER_TOKEN: i64 = 750;

/// 存储内容块列表 → REST 线上内容块列表。
#[must_use]
pub(crate) fn api_blocks(blocks: &[StoredBlock]) -> Vec<ApiBlock> {
    blocks.iter().map(api_block).collect()
}

fn api_block(block: &StoredBlock) -> ApiBlock {
    match block {
        StoredBlock::Text { text } => ApiBlock::Text { text: text.clone() },
        StoredBlock::ToolUse { id, name, input } => ApiBlock::ToolUse {
            id: id.clone(),
            name: name.clone(),
            input: input.clone(),
        },
        StoredBlock::ToolResult {
            tool_use_id,
            content,
            is_error,
            metadata,
        } => ApiBlock::ToolResult {
            tool_use_id: tool_use_id.clone(),
            content: content.clone(),
            is_error: *is_error,
            // 旧紧凑构造器保证 metadata 恒非 null（缺省 `Map.of()`）。
            metadata: metadata
                .clone()
                .unwrap_or_else(|| Value::Object(Map::new())),
        },
        StoredBlock::Thinking { thinking } => ApiBlock::Thinking {
            thinking: thinking.clone(),
        },
        StoredBlock::Image {
            source,
            width,
            height,
        } => {
            // url 图片只带 url（对齐 WS 出站：url 非空白只写 url 否则 base64Data）。
            let url = source.remote_url().map(str::to_owned);
            ApiBlock::Image {
                media_type: source.media_type_or_default().to_owned(),
                base64_data: if url.is_some() {
                    None
                } else {
                    source.data.clone()
                },
                url,
                width: width.unwrap_or(0),
                height: height.unwrap_or(0),
            }
        }
        StoredBlock::RedactedThinking { data } => ApiBlock::RedactedThinking { data: data.clone() },
    }
}

/// 存储消息 → REST 线上消息（对齐 `mapRowToMessage` 的兜底语义）。
#[must_use]
pub(crate) fn api_message(record: &MessageRecord) -> ApiMessage {
    let timestamp = format_rfc3339_micros(record.created_at);
    match record.role {
        MessageRole::User => ApiMessage::User {
            uuid: record.id.clone(),
            timestamp,
            content: api_blocks(&record.content),
            tool_use_result: None,
            source_tool_assistant_uuid: None,
        },
        MessageRole::Assistant => ApiMessage::Assistant {
            uuid: record.id.clone(),
            timestamp,
            content: api_blocks(&record.content),
            // 旧 `mapRowToMessage`：stopReason null → "end_turn"。
            stop_reason: record
                .stop_reason
                .clone()
                .unwrap_or_else(|| "end_turn".to_owned()),
            usage: Usage {
                input_tokens: record.input_tokens,
                output_tokens: record.output_tokens,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
        },
        MessageRole::System => ApiMessage::System {
            uuid: record.id.clone(),
            timestamp,
            content: system_text(&record.content),
        },
    }
}

/// 存储消息列表 → REST 线上消息列表。
#[must_use]
pub(crate) fn api_messages(records: &[MessageRecord]) -> Vec<ApiMessage> {
    records.iter().map(api_message).collect()
}

/// system 消息纯文本（旧侧以 text 块 `\n` 连接存储，此处对称还原）。
fn system_text(blocks: &[StoredBlock]) -> String {
    blocks
        .iter()
        .filter_map(|block| {
            if let StoredBlock::Text { text } = block {
                Some(text.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ===== token 估算（旧 TokenCounter 整数化） =====

/// 消息列表 token 估算：`ceil(chars / 3.5 + n * 4)`。
#[must_use]
pub(crate) fn estimate_tokens(messages: &[ApiMessage]) -> i64 {
    if messages.is_empty() {
        return 0;
    }
    let total_chars: i64 = messages.iter().map(message_chars).sum();
    let message_count = i64::try_from(messages.len()).unwrap_or(i64::MAX);
    let numerator = total_chars
        .saturating_mul(CHARS_PER_TOKEN_DEN)
        .saturating_add(
            CHARS_PER_TOKEN_NUM
                .saturating_mul(PER_MESSAGE_TOKENS)
                .saturating_mul(message_count),
        );
    ceil_div(numerator, CHARS_PER_TOKEN_NUM)
}

/// 单条消息字符量（system 消息按其文本内容计，对齐 `viewOf` 包装语义）。
fn message_chars(message: &ApiMessage) -> i64 {
    match message {
        ApiMessage::User { content, .. } | ApiMessage::Assistant { content, .. } => {
            content.iter().map(block_chars).sum()
        }
        ApiMessage::System { content, .. } => char_count(content),
    }
}

/// 单内容块字符量（对齐 `estimateBlockChars` 的逐分支开销）。
fn block_chars(block: &ApiBlock) -> i64 {
    match block {
        ApiBlock::Text { text } => char_count(text),
        ApiBlock::ToolUse { name, input, .. } => {
            // name + input 序列化长度 + 20（JSON 结构开销）。
            char_count(name)
                .saturating_add(char_count(&input.to_string()))
                .saturating_add(20)
        }
        ApiBlock::ToolResult { content, .. } => char_count(content).saturating_add(10),
        ApiBlock::Thinking { thinking } => char_count(thinking),
        ApiBlock::Image { width, height, .. } => image_chars(*width, *height),
        ApiBlock::RedactedThinking { .. } => 10,
    }
}

/// 图片块等效字符量：`tokens = ceil(w·h / 750)`（无尺寸回退 85），
/// `chars = ceil(tokens · 3.5)`。
fn image_chars(width: i64, height: i64) -> i64 {
    let tokens = if width <= 0 || height <= 0 {
        IMAGE_FALLBACK_TOKENS
    } else {
        ceil_div(width.saturating_mul(height), IMAGE_PIXELS_PER_TOKEN)
    };
    ceil_div(
        tokens.saturating_mul(CHARS_PER_TOKEN_NUM),
        CHARS_PER_TOKEN_DEN,
    )
}

/// `ceil(a / b)`（b > 0），饱和安全。
fn ceil_div(numerator: i64, denominator: i64) -> i64 {
    numerator.div_euclid(denominator) + i64::from(numerator.rem_euclid(denominator) != 0)
}

/// 字符数（Java `String.length()` 的 UTF-16 单位在 BMP 内与码点数一致）。
fn char_count(text: &str) -> i64 {
    i64::try_from(text.chars().count()).unwrap_or(i64::MAX)
}

// ===== 详情 / export 出口 =====

/// 详情端点响应（全局 mapper 语义：null 字段剥离）。
#[must_use]
pub(crate) fn detail_to_value(detail: &SessionDetail) -> Value {
    strip_nulls(Value::Object(session_root(
        detail,
        &rest_messages,
        &iso_time,
    )))
}

/// REST 通道消息列表的 `Value` 包装（`session_root` 注入用）。
fn rest_messages(records: &[MessageRecord]) -> Value {
    serde_json::to_value(api_messages(records)).unwrap_or(Value::Array(Vec::new()))
}

/// export 端点响应（独立 mapper 语义：null 保留 + epoch 浮点秒 + null 保留
/// 的消息形状）。
#[must_use]
pub(crate) fn export_to_value(detail: &SessionDetail) -> Value {
    Value::Object(session_root(detail, &export_messages, &epoch_time))
}

/// 会话根对象骨架（两出口共享键集；消息与时间字段由调用方注入）。
fn session_root(
    detail: &SessionDetail,
    messages: &dyn Fn(&[MessageRecord]) -> Value,
    times: &dyn Fn(i64) -> Value,
) -> Map<String, Value> {
    let mut root = Map::new();
    root.insert("sessionId".into(), Value::String(detail.session_id.clone()));
    root.insert("model".into(), Value::String(detail.model.clone()));
    root.insert(
        "workingDir".into(),
        Value::String(detail.working_dir.clone()),
    );
    root.insert("title".into(), opt_string(detail.title.as_deref()));
    root.insert("status".into(), Value::String(detail.status.clone()));
    root.insert("messages".into(), messages(&detail.messages));
    root.insert("config".into(), Value::Object(detail.config.clone()));
    root.insert("totalUsage".into(), usage_json(&detail.total_usage));
    root.insert(
        "totalCostUsd".into(),
        Value::Number(
            serde_json::Number::from_f64(detail.total_cost_usd)
                .unwrap_or_else(|| serde_json::Number::from(0)),
        ),
    );
    root.insert("summary".into(), opt_string(detail.summary.as_deref()));
    root.insert("createdAt".into(), times(detail.created_at));
    root.insert("updatedAt".into(), times(detail.updated_at));
    root
}

fn iso_time(millis: i64) -> Value {
    Value::String(format_rfc3339_micros(millis))
}

fn epoch_time(millis: i64) -> Value {
    match serde_json::Number::from_f64(millis_to_epoch_secs(millis)) {
        Some(number) => Value::Number(number),
        // 恒有限值，兜底不可达。
        None => Value::Null,
    }
}

/// epoch 毫秒 → 浮点秒（`i32 → f64` 精确换算，避免 int→float 截断 lint；
/// epoch 秒在 2038 年前恒落 i32 正域）。
fn millis_to_epoch_secs(millis: i64) -> f64 {
    let secs = millis.div_euclid(1000);
    let sub_second = millis.rem_euclid(1000);
    f64::from(i32::try_from(secs).unwrap_or(i32::MAX))
        + f64::from(i32::try_from(sub_second).unwrap_or(0)) / 1000.0
}

fn opt_string(value: Option<&str>) -> Value {
    match value {
        Some(text) => Value::String(text.to_owned()),
        None => Value::Null,
    }
}

fn usage_json(usage: &Usage) -> Value {
    serde_json::json!({
        "inputTokens": usage.input_tokens,
        "outputTokens": usage.output_tokens,
        "cacheReadInputTokens": usage.cache_read_input_tokens,
        "cacheCreationInputTokens": usage.cache_creation_input_tokens,
    })
}

/// export 通道消息列表（null 保留：`toolUseResult` / `sourceToolAssistantUUID`）。
fn export_messages(records: &[MessageRecord]) -> Value {
    Value::Array(records.iter().map(export_message).collect())
}

fn export_message(record: &MessageRecord) -> Value {
    let timestamp = epoch_time(record.created_at);
    match record.role {
        MessageRole::User => serde_json::json!({
            "type": "user",
            "uuid": record.id,
            "timestamp": timestamp,
            "content": api_blocks(&record.content),
            "toolUseResult": Value::Null,
            "sourceToolAssistantUUID": Value::Null,
        }),
        MessageRole::Assistant => serde_json::json!({
            "type": "assistant",
            "uuid": record.id,
            "timestamp": timestamp,
            "content": api_blocks(&record.content),
            "stopReason": record.stop_reason.clone().unwrap_or_else(|| "end_turn".to_owned()),
            "usage": Usage {
                input_tokens: record.input_tokens,
                output_tokens: record.output_tokens,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
        }),
        MessageRole::System => serde_json::json!({
            "type": "system",
            "uuid": record.id,
            "timestamp": timestamp,
            "content": system_text(&record.content),
        }),
    }
}

/// 递归剥离 `null` 值键（复刻旧全局 Jackson `NON_NULL` 对动态字段的语义）。
fn strip_nulls(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(_, value)| !value.is_null())
                .map(|(key, value)| (key, strip_nulls(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(strip_nulls).collect()),
        other => other,
    }
}

// ===== markdown 导出（旧 `exportAsMarkdown` 逐行对齐） =====

/// 会话 markdown 转录。
#[must_use]
pub(crate) fn export_markdown(detail: &SessionDetail) -> String {
    use std::fmt::Write as _;
    let title = detail.title.as_deref().unwrap_or(&detail.session_id);
    let mut out = String::new();
    let _ = writeln!(out, "# Session: {title}");
    let _ = writeln!(out);
    let _ = writeln!(out, "- **Model**: {}", detail.model);
    let _ = writeln!(
        out,
        "- **Created**: {}",
        format_rfc3339_micros(detail.created_at)
    );
    let _ = writeln!(out, "- **Messages**: {}", detail.messages.len());
    let _ = writeln!(out);
    let _ = writeln!(out, "---");
    let _ = writeln!(out);
    for record in &detail.messages {
        match record.role {
            MessageRole::User => {
                let _ = writeln!(out, "## User");
                let _ = writeln!(out);
                for block in &record.content {
                    match block {
                        StoredBlock::Text { text } => {
                            let _ = writeln!(out, "{text}");
                            let _ = writeln!(out);
                        }
                        StoredBlock::ToolResult { content, .. } => {
                            let _ = writeln!(out, "{content}");
                            let _ = writeln!(out);
                        }
                        _ => {}
                    }
                }
            }
            MessageRole::Assistant => {
                let _ = writeln!(out, "## Assistant");
                let _ = writeln!(out);
                for block in &record.content {
                    if let StoredBlock::Text { text } = block {
                        let _ = writeln!(out, "{text}");
                        let _ = writeln!(out);
                    }
                }
            }
            MessageRole::System => {
                let _ = writeln!(out, "## System");
                let _ = writeln!(out);
                let _ = writeln!(out, "{}", system_text(&record.content));
                let _ = writeln!(out);
            }
        }
    }
    out
}

// ===== 确定性压缩 =====

/// compact 结果（`CompactOutcome`）。
pub(crate) struct CompactOutcome {
    /// 压缩前估算 token（`estimateTokens(messages)`）。
    pub tokens_before: i64,
    /// 压缩后估算 token（摘要 system 消息 + 保留区）。
    pub tokens_after: i64,
    /// 需落库的 summary（notNeeded / 未实际缩小时为 `None`）。
    pub summary: Option<String>,
}

/// Phase 1 确定性压缩（旧 `CompactService.compact` 的无 LLM 替身）。
///
/// 语义对齐点：低于最低消息数 / 保留区外无消息 → `notNeeded(0, 0)`
///（样例 POST compact 空会话即此分支）；正常路径保留最近 3 个 user 轮次，
/// 前缀消息压缩为 `[User]/[Assistant]` 摘要 system 消息并落 `summary` 列。
/// LLM 摘要（旧 `COMPACT_SYSTEM_PROMPT` 三级降级）在 Phase 2 接 zk-llm。
#[must_use]
pub(crate) fn compact_deterministic(messages: &[MessageRecord]) -> CompactOutcome {
    let not_needed = CompactOutcome {
        tokens_before: 0,
        tokens_after: 0,
        summary: None,
    };
    if messages.len() < MIN_MESSAGES_FOR_COMPACT {
        return not_needed;
    }
    let user_positions: Vec<usize> = messages
        .iter()
        .enumerate()
        .filter(|(_, record)| record.role == MessageRole::User)
        .map(|(index, _)| index)
        .collect();
    if user_positions.len() <= PRESERVED_USER_TURNS {
        return not_needed;
    }
    let boundary = user_positions[user_positions.len() - PRESERVED_USER_TURNS];
    if boundary == 0 {
        return not_needed;
    }
    let tokens_before = estimate_tokens(&api_messages(messages));
    let summary = summarize_prefix(&messages[..boundary]);
    let mut compacted = vec![ApiMessage::System {
        uuid: uuid::Uuid::new_v4().to_string(),
        timestamp: format_rfc3339_micros(now_millis()),
        content: summary.clone(),
    }];
    compacted.extend(api_messages(&messages[boundary..]));
    let tokens_after = estimate_tokens(&compacted);
    // 与旧 success 判定一致：仅实际缩小时落 summary。
    let summary = (tokens_after < tokens_before).then_some(summary);
    CompactOutcome {
        tokens_before,
        tokens_after,
        summary,
    }
}

/// 前缀消息的确定性摘要：`[角色] 首个文本块（截 500 字符）` 逐行拼接。
fn summarize_prefix(messages: &[MessageRecord]) -> String {
    let mut lines = Vec::with_capacity(messages.len());
    for record in messages {
        let label = match record.role {
            MessageRole::User => "[User]",
            MessageRole::Assistant => "[Assistant]",
            MessageRole::System => "[System]",
        };
        let mut line = format!("{label} {}", first_text_of(record));
        if line.chars().count() > SUMMARY_EXCERPT_MAX_CHARS {
            let truncated: String = line.chars().take(SUMMARY_EXCERPT_MAX_CHARS).collect();
            line = format!("{truncated}…");
        }
        lines.push(line);
    }
    lines.join("\n")
}

/// 消息首个非空文本（`text` 块优先，`tool_result` 次之）。
fn first_text_of(record: &MessageRecord) -> String {
    for block in &record.content {
        match block {
            StoredBlock::Text { text } if !text.trim().is_empty() => return text.clone(),
            StoredBlock::ToolResult { content, .. } if !content.trim().is_empty() => {
                return content.clone();
            }
            _ => {}
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user_msg(text: &str) -> MessageRecord {
        MessageRecord {
            id: uuid::Uuid::new_v4().to_string(),
            session_id: "s1".into(),
            role: MessageRole::User,
            content: vec![StoredBlock::Text { text: text.into() }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            seq_num: 0,
            created_at: 0,
        }
    }

    fn assistant_msg(text: &str, stop: Option<&str>) -> MessageRecord {
        MessageRecord {
            id: uuid::Uuid::new_v4().to_string(),
            session_id: "s1".into(),
            role: MessageRole::Assistant,
            content: vec![StoredBlock::Text { text: text.into() }],
            stop_reason: stop.map(str::to_owned),
            input_tokens: 3,
            output_tokens: 5,
            seq_num: 0,
            created_at: 0,
        }
    }

    /// 旧算法黄金值互锁：`ceil(5 / 3.5 + 4) = ceil(5.428…) = 6`。
    #[test]
    fn estimate_tokens_matches_legacy_formula() {
        let messages = vec![api_message(&user_msg("hello"))];
        assert_eq!(estimate_tokens(&messages), 6);
    }

    /// 图片黄金值：`ceil(100·100 / 750) = 14` token → `ceil(14·3.5) = 49` 字符。
    #[test]
    fn image_chars_matches_legacy_formula() {
        assert_eq!(image_chars(100, 100), 49);
        assert_eq!(image_chars(0, 0), ceil_div(IMAGE_FALLBACK_TOKENS * 7, 2));
    }

    /// url 型图片块：REST 出站携带 url、无 base64Data；无尺寸 → 85 token 兜底。
    #[test]
    fn url_image_maps_to_api_block_with_fallback_budget() {
        let block = StoredBlock::Image {
            source: zk_db::model::ImageSource {
                kind: "url".into(),
                media_type: None,
                data: None,
                url: Some(
                    "https://bkt.oss.example.com/zhikuncode-artifacts/clipboard/a.png".into(),
                ),
            },
            width: None,
            height: None,
        };
        let api = api_blocks(std::slice::from_ref(&block));
        let ApiBlock::Image {
            media_type,
            base64_data,
            url,
            width,
            height,
        } = &api[0]
        else {
            panic!("expected image block");
        };
        assert_eq!(media_type, "image/png");
        assert_eq!(base64_data.as_deref(), None);
        assert_eq!(
            url.as_deref(),
            Some("https://bkt.oss.example.com/zhikuncode-artifacts/clipboard/a.png")
        );
        assert_eq!((*width, *height), (0, 0));
        // 无尺寸兜底：85 token → ceil(85·3.5) = 298 等效字符。
        assert_eq!(image_chars(*width, *height), 298);
    }

    /// 空会话与低消息数 → notNeeded(0,0)（对齐样例）。
    #[test]
    fn compact_not_needed_below_threshold() {
        let empty = CompactOutcome {
            tokens_before: 0,
            tokens_after: 0,
            summary: None,
        };
        assert_eq!(
            compact_deterministic(&[]).tokens_before,
            empty.tokens_before
        );
        let few: Vec<MessageRecord> = (0..4).map(|_| user_msg("x")).collect();
        let outcome = compact_deterministic(&few);
        assert_eq!((outcome.tokens_before, outcome.tokens_after), (0, 0));
        assert!(outcome.summary.is_none());
    }

    /// 足量消息 → 保留 3 轮 user + 摘要落库位。
    #[test]
    fn compact_produces_summary_and_shrinks() {
        let mut messages: Vec<MessageRecord> = Vec::new();
        for round in 0..5 {
            messages.push(user_msg(&format!(
                "user turn {round} with a fairly long body"
            )));
            messages.push(assistant_msg(&"assistant reply ".repeat(20), None));
        }
        let outcome = compact_deterministic(&messages);
        assert!(outcome.tokens_before > outcome.tokens_after);
        let summary = outcome.summary.expect("summary persisted");
        assert!(summary.contains("[User] user turn 0"));
        assert!(summary.contains("[Assistant]"));
    }

    /// assistant `stopReason` 为 null 兜底 `end_turn`；`usage` 恒非 null。
    #[test]
    fn assistant_fallbacks() {
        let value =
            serde_json::to_value(api_message(&assistant_msg("hi", None))).expect("serializable");
        assert_eq!(value["stopReason"], "end_turn");
        assert_eq!(value["usage"]["inputTokens"], 3);
        assert_eq!(value["usage"]["outputTokens"], 5);
        let kept = serde_json::to_value(api_message(&assistant_msg("hi", Some("tool_use"))))
            .expect("serializable");
        assert_eq!(kept["stopReason"], "tool_use");
    }

    /// 详情出口剥离 null（title/summary），export 出口保留并以浮点秒表示时间。
    #[test]
    fn detail_and_export_null_semantics() {
        let detail = SessionDetail {
            session_id: "s1".into(),
            model: "m".into(),
            working_dir: "/w".into(),
            title: None,
            status: "active".into(),
            messages: vec![user_msg("hello")],
            config: Map::new(),
            total_usage: Usage::default(),
            total_cost_usd: 0.0,
            summary: None,
            created_at: 1_786_792_040_564,
            updated_at: 1_786_792_040_564,
        };
        let detail_value = detail_to_value(&detail);
        assert!(detail_value.get("title").is_none());
        assert!(detail_value.get("summary").is_none());
        assert_eq!(detail_value["createdAt"], "2026-08-15T11:07:20.564000Z");
        let export_value = export_to_value(&detail);
        assert!(export_value["title"].is_null());
        assert!(export_value["summary"].is_null());
        // 浮点秒形状（值因毫秒截断与样例尾数不同，形状与类型一致）。
        assert_eq!(export_value["createdAt"].as_f64(), Some(1_786_792_040.564));
        assert!(export_value["messages"][0]["toolUseResult"].is_null());
        assert_eq!(export_value["messages"][0]["type"], "user");
    }

    /// markdown 导出结构（旧 `exportAsMarkdown` 头部骨架）。
    #[test]
    fn markdown_export_skeleton() {
        let detail = SessionDetail {
            session_id: "s1".into(),
            model: "qwen3.7-max".into(),
            working_dir: "/w".into(),
            title: None,
            status: "active".into(),
            messages: vec![user_msg("question"), assistant_msg("answer", None)],
            config: Map::new(),
            total_usage: Usage::default(),
            total_cost_usd: 0.0,
            summary: None,
            created_at: 0,
            updated_at: 0,
        };
        let markdown = export_markdown(&detail);
        assert!(markdown.starts_with("# Session: s1\n\n"));
        assert!(markdown.contains("- **Model**: qwen3.7-max\n"));
        assert!(markdown.contains("- **Messages**: 2\n"));
        assert!(markdown.contains("## User\n\nquestion\n\n"));
        assert!(markdown.contains("## Assistant\n\nanswer\n\n"));
    }
}
