//! 领域模型：会话消息（`Message`）、内容块（`ContentBlock`）与 Token 用量（`Usage`）。
//!
//! 建模来源（旧仓库只读规格，2026-08-15 冻结）：
//! - `backend/src/main/java/com/aicodeassistant/model/Message.java`
//! - `backend/src/main/java/com/aicodeassistant/model/ContentBlock.java`
//! - `backend/src/main/java/com/aicodeassistant/model/Usage.java`
//! - `backend/src/main/java/com/aicodeassistant/model/SystemMessageType.java`
//! - WS 线上真实形状：`WebSocketController.convertMessagesForWs` /
//!   `convertContentBlocksForWs`（L1905-1988）——`committed_messages` 与
//!   `session_restored.messages` 的唯一序列化出口。
//!
//! # 与 Java record 的形状取舍（重要）
//!
//! Java record 上的 `@JsonProperty("tool_use_id")` / `("is_error")` 等 `snake_case`
//! 注解只作用于 REST / 存储通道；**WS 通道**经 `convertContentBlocksForWs` 显式
//! 重映射为前端 camelCase（`toolUseId` / `isError`），且丢弃
//! `SystemMessage.type`（`SystemMessageType`）、`ImageBlock.width/height`、
//! `UserMessage.toolUseResult/sourceToolAssistantUUID`。本 crate 是 WS 协议契约层，
//! 因此**按 WS 线上形状建模**（camelCase 字段、丢弃字段不建模），丢弃清单在此留痕。
//!
//! # Message 的 tag 字段名
//!
//! Java `@JsonTypeInfo(property = "type")` + 前端 `dispatch.test.ts` 真实样例
//! （`{ type: 'user', uuid, timestamp, content }`）一致确认：多态 tag 是顶层
//! `type` 字段（值 `user` / `assistant` / `system`），**不是 `role`**。

use serde::{Deserialize, Serialize};

/// 会话消息——对齐 `model/Message.java` 的 sealed interface 三子类。
///
/// `timestamp` 为 epoch 毫秒（Java `Instant` 经 `toEpochMilli()` 落 WS）。
/// `Message::User` 不含 Java record 的 `toolUseResult` / `sourceToolAssistantUUID`
/// （WS 转换层丢弃，见模块文档）；`Message::System` 不含 `SystemMessageType`
/// （同上，前端以 `subtype` 自行合成，不属下行协议）。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    /// 用户消息（Java `Message.UserMessage`）。
    #[serde(rename_all = "camelCase")]
    User {
        /// 消息 UUID。
        uuid: String,
        /// epoch 毫秒时间戳。
        timestamp: i64,
        /// 内容块列表。
        content: Vec<ContentBlock>,
    },
    /// 助手消息（Java `Message.AssistantMessage`）。
    #[serde(rename_all = "camelCase")]
    Assistant {
        /// 消息 UUID。
        uuid: String,
        /// epoch 毫秒时间戳。
        timestamp: i64,
        /// 内容块列表。
        content: Vec<ContentBlock>,
        /// 停止原因（`end_turn` 等；Java 允许 null → Option）。
        #[serde(skip_serializing_if = "Option::is_none")]
        stop_reason: Option<String>,
        /// Token 用量（Java 允许 null → Option）。
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<Usage>,
    },
    /// 系统消息（Java `Message.SystemMessage`；WS 形状仅保留 content）。
    #[serde(rename_all = "camelCase")]
    System {
        /// 消息 UUID。
        uuid: String,
        /// epoch 毫秒时间戳。
        timestamp: i64,
        /// 系统消息文本。
        content: String,
    },
}

/// 内容块——对齐 `ContentBlock.java` 六子类（WS 线上 camelCase 形状）。
///
/// Phase 1 只消费 `Text` / `Image`（D8：thinking 走独立 delta 通道、工具走
/// Phase 2+ 引擎），但六种全量冻结进契约。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    /// 文本块（Java `ContentBlock.TextBlock`）。
    Text {
        /// 文本内容。
        text: String,
    },
    /// 工具调用块（Java `ContentBlock.ToolUseBlock`；WS 层将 `id`/`name` 重映射为
    /// `toolUseId`/`toolName`）。`input` 为任意 JSON 工具参数。
    #[serde(rename_all = "camelCase")]
    ToolUse {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 工具名。
        tool_name: String,
        /// 工具入参（原样 JSON）。
        input: serde_json::Value,
    },
    /// 工具结果块（Java `ContentBlock.ToolResultBlock`；WS 层将 `tool_use_id`/
    /// `is_error` 重映射为 camelCase）。`metadata` 仅承载 `structuredResult`。
    #[serde(rename_all = "camelCase")]
    ToolResult {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 结果文本。
        content: String,
        /// 是否出错。
        is_error: bool,
        /// 结构化结果元数据（可缺省）。
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
    },
    /// 思考块（Java `ContentBlock.ThinkingBlock`）。
    Thinking {
        /// 思考文本。
        thinking: String,
    },
    /// 图片块（Java `ContentBlock.ImageBlock`；`width`/`height` 被 WS 层丢弃）。
    ///
    /// `base64Data` 与 `url` 二选一：可信 OSS 剪贴板图片出站只写 `url`
    /// （旧 `WebSocketController` L1988 出站分支），其余仍为 base64；缺省侧
    /// 键被省略（对齐旧 Jackson `NON_NULL`）。
    #[serde(rename_all = "camelCase")]
    Image {
        /// MIME 类型（如 `image/png`；旧出站恒写 `mediaType`）。
        media_type: String,
        /// base64 编码数据（url 场景缺省）。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base64_data: Option<String>,
        /// 可信远程 URL（base64 场景缺省）。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
    },
    /// 脱敏思考块（Java `ContentBlock.RedactedThinkingBlock`；WS 形状无字段）。
    RedactedThinking,
}

/// Token 用量统计——字段名逐字对齐 `Usage.java` / 后端 `usageMap` 构造
/// （`WebSocketController` L427-431、L1925-1929 双处一致）：
/// `inputTokens` / `outputTokens` / `cacheReadInputTokens` /
/// `cacheCreationInputTokens`。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Usage {
    /// 输入 token 数。
    pub input_tokens: i64,
    /// 输出 token 数。
    pub output_tokens: i64,
    /// 缓存读 token 数（字段名与 `Usage.cacheReadInputTokens` 逐字一致）。
    pub cache_read_input_tokens: i64,
    /// 缓存创建 token 数（字段名与 `Usage.cacheCreationInputTokens` 逐字一致）。
    pub cache_creation_input_tokens: i64,
}

impl Usage {
    /// 输入 + 输出合计（对齐 Java `Usage.totalTokens()`）。
    #[must_use]
    pub fn total_tokens(self) -> i64 {
        self.input_tokens + self.output_tokens
    }
}

impl std::ops::Add for Usage {
    type Output = Usage;
    /// 两份用量逐字段相加（对齐 Java `Usage.add`；以运算符形态提供）。
    fn add(self, other: Usage) -> Usage {
        Usage {
            input_tokens: self.input_tokens + other.input_tokens,
            output_tokens: self.output_tokens + other.output_tokens,
            cache_read_input_tokens: self.cache_read_input_tokens + other.cache_read_input_tokens,
            cache_creation_input_tokens: self.cache_creation_input_tokens
                + other.cache_creation_input_tokens,
        }
    }
}

/// 时间戳双格式标量：反序列化接受 **epoch 毫秒数字** 或 **RFC 3339 字符串**，
/// 序列化恒为 epoch 毫秒数字。
///
/// 存在原因：旧系统 Jackson 默认把 `java.time.Instant` 序列化为 ISO-8601 字符串
/// （如 `InteractionView.createdAt` 走 `objectMapper.convertValue` 路径），而另一
/// 些调用点显式 `toEpochMilli()`（如 `interaction_updated` ACK 路径的
/// `decisionDeadlineAt`）；前端 `dispatch.ts:109 clientDeadline` 对两种格式做容错。
/// zkcode 新实现统一发 epoch 毫秒，但契约层必须能解析旧线上两种流量（对照调试期
/// 8082/8080 并行，见 U3）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlexEpoch(i64);

impl FlexEpoch {
    /// 以 epoch 毫秒构造。
    #[must_use]
    pub fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// 取 epoch 毫秒值。
    #[must_use]
    pub fn as_millis(self) -> i64 {
        self.0
    }
}

impl Serialize for FlexEpoch {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for FlexEpoch {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = FlexEpoch;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("epoch milliseconds number or RFC 3339 string")
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<FlexEpoch, E> {
                Ok(FlexEpoch(v))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<FlexEpoch, E> {
                i64::try_from(v)
                    .map(FlexEpoch)
                    .map_err(|_| E::custom("epoch milliseconds out of i64 range"))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<FlexEpoch, E> {
                if v.is_finite() && v.abs() < 9.0e18 {
                    // 允许截断：JSON number 整数值场景，f64 53-bit 精度覆盖
                    // epoch 毫秒量级（~2^51），此转换无损；仅畸形小数被截断。
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        Ok(FlexEpoch(v as i64))
                    }
                } else {
                    Err(E::custom("epoch milliseconds out of range"))
                }
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<FlexEpoch, E> {
                parse_rfc3339_millis(v)
                    .map(FlexEpoch)
                    .ok_or_else(|| E::custom(format!("invalid RFC 3339 timestamp: {v}")))
            }
        }
        deserializer.deserialize_any(V)
    }
}

/// 解析 RFC 3339（`YYYY-MM-DDTHH:MM:SS[.fff+](Z|±HH:MM|±HHMM)`）为 epoch 毫秒。
///
/// 手写实现（civil-days 算法，Howard Hinnant `days_from_civil`），避免为协议层
/// 引入 chrono 依赖；仅覆盖 Jackson `Instant.toString` 的实际输出形状。
fn parse_rfc3339_millis(input: &str) -> Option<i64> {
    let b = input.as_bytes();
    let digits = |range: std::ops::Range<usize>| -> Option<i64> {
        let mut n: i64 = 0;
        for &c in b.get(range)? {
            n = n * 10 + i64::from((c as char).to_digit(10)?);
        }
        Some(n)
    };
    if b.len() < 20 {
        return None;
    }
    let year = digits(0..4)?;
    let month = digits(5..7)?;
    let day = digits(8..10)?;
    if b[4] != b'-' || b[7] != b'-' || (b[10] != b'T' && b[10] != b't') {
        return None;
    }
    let hour = digits(11..13)?;
    let minute = digits(14..16)?;
    let second = digits(17..19)?;
    if b[13] != b':' || b[16] != b':' {
        return None;
    }
    // 可选小数秒：取毫秒（截断到 3 位，余下忽略）。
    let mut millis = 0_i64;
    let mut idx = 19;
    if b.get(idx) == Some(&b'.') {
        idx += 1;
        let start = idx;
        while idx < b.len() && b[idx].is_ascii_digit() {
            if idx - start < 3 {
                millis = millis * 10 + i64::from(b[idx] - b'0');
            }
            idx += 1;
        }
        for _ in (idx - start)..3 {
            millis *= 10;
        }
        if idx == start {
            return None;
        }
    }
    // 时区：Z / ±HH:MM / ±HHMM。
    let offset_secs: i64 = match b.get(idx) {
        Some(&b'Z' | &b'z') => {
            idx += 1;
            0
        }
        Some(&sign @ (b'+' | b'-')) => {
            idx += 1;
            let oh = digits(idx..idx + 2)?;
            let om = if b.get(idx + 2) == Some(&b':') {
                let m = digits(idx + 3..idx + 5)?;
                idx += 5;
                m
            } else {
                let m = digits(idx + 2..idx + 4)?;
                idx += 4;
                m
            };
            let mag = oh * 3600 + om * 60;
            if sign == b'+' { mag } else { -mag }
        }
        _ => return None,
    };
    if idx != b.len() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // days_from_civil（正负年份通用，shift 到 0/3/2 闰年锚点）。
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = month;
    let d = day;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + hour * 3600 + minute * 60 + second - offset_secs;
    Some(secs * 1000 + millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flex_epoch_accepts_both_shapes() {
        // 1_755_000_960_000 ms == 2025-08-12T14:40:00Z（UTC 基准，三形状互证）。
        let as_num: FlexEpoch = serde_json::from_str("1755009600000").unwrap();
        let as_zulu: FlexEpoch = serde_json::from_str("\"2025-08-12T14:40:00Z\"").unwrap();
        assert_eq!(as_num, as_zulu);
        let with_offset: FlexEpoch =
            serde_json::from_str("\"2025-08-12T22:40:00.250+08:00\"").unwrap();
        assert_eq!(with_offset.as_millis(), 1_755_009_600_000 + 250);
        assert_eq!(serde_json::to_string(&as_num).unwrap(), "1755009600000");
        // 小数秒位数边界：1 位 / 2 位 / 5 位（截断到毫秒）。
        let one_digit: FlexEpoch = serde_json::from_str("\"2025-08-12T14:40:00.5Z\"").unwrap();
        assert_eq!(one_digit.as_millis(), 1_755_009_600_000 + 500);
        let two_digit: FlexEpoch = serde_json::from_str("\"2025-08-12T14:40:00.25Z\"").unwrap();
        assert_eq!(two_digit.as_millis(), 1_755_009_600_000 + 250);
        let five_digit: FlexEpoch = serde_json::from_str("\"2025-08-12T14:40:00.25012Z\"").unwrap();
        assert_eq!(five_digit.as_millis(), 1_755_009_600_000 + 250);
    }

    #[test]
    fn message_tag_is_type_not_role() {
        let raw =
            r#"{"type":"user","uuid":"1","timestamp":1,"content":[{"type":"text","text":"hi"}]}"#;
        let msg: Message = serde_json::from_str(raw).unwrap();
        assert!(matches!(msg, Message::User { .. }));
        assert_eq!(serde_json::from_str::<Message>(raw).unwrap(), msg);
    }

    #[test]
    fn usage_field_names_match_java() {
        let u = Usage::default();
        let json = serde_json::to_value(u).unwrap();
        let keys: std::collections::BTreeSet<String> =
            json.as_object().unwrap().keys().cloned().collect();
        assert_eq!(
            keys,
            [
                "cacheCreationInputTokens",
                "cacheReadInputTokens",
                "inputTokens",
                "outputTokens"
            ]
            .into_iter()
            .map(String::from)
            .collect::<std::collections::BTreeSet<_>>()
        );
        assert_eq!(u.total_tokens(), 0);
        assert_eq!(Usage::default() + Usage::default(), Usage::default());
    }
}
