//! zk-db 领域模型——会话/消息的持久化形状与 8 端点语义的数据载体。
//!
//! # 建模来源（旧仓库只读规格，2026-08-15 冻结）
//!
//! - `model/SessionSummary.java` / `session/SessionData.java`（领域 record）
//! - `session/SessionManager.mapRowToMessage` / `parseContentBlocks`
//!   （`content_json` 存储形状的唯一权威）
//! - S2 实采样例 `docs/baseline/samples/GET_api-sessions*.json`（JSON 字段名）
//!
//! # 存储形状 vs WS 形状（为何 blocks 自持而不复用 zk-protocol）
//!
//! 消息 `content_json` 的**存储形状**是 `snake_case`（`tool_use_id` / `is_error` /
//! `image.source.{media_type,data}`），而 zk-protocol `ContentBlock` 按旧系统
//! WS 转换层（`convertContentBlocksForWs`）的 **camelCase 线上形状**建模
//!（`toolUseId` / `mediaType`…，见其 model.rs 模块文档）。两形状在 image 与
//! `tool_use` 块上结构不同，复用任一方都会在另一通道丢字段。故 [`StoredBlock`]
//! 由本 crate 自持（忠实存储通道），WS/REST 出口映射是 zk-server 的职责；
//! [`SessionDetail::total_usage`] 复用 `zk_protocol::Usage`（其字段名与旧
//! `Usage.java` record 序列化逐字一致，无形状分歧，复用零成本）。
//!
//! # serde 形状
//!
//! 字段命名 `camelCase` 对齐实采样例（`sessionId` / `workingDir` /
//! `messageCount` / `totalCostUsd`…）；时间字段为 epoch 毫秒 `i64`，序列化
//! 为 RFC 3339 字符串（`serde_iso_ms`，见 `time.rs`）。

use zk_protocol::model::Usage;

use crate::time::serde_iso_ms;

/// 消息角色（`messages.role` 列域）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    /// 用户消息。
    User,
    /// 助手消息。
    Assistant,
    /// 系统消息。
    System,
}

impl MessageRole {
    /// DB 存储字符串（`messages.role`）。
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::System => "system",
        }
    }

    /// 从 DB 存储字符串解析；未知值返回 `None`（对齐旧系统
    /// `mapRowToMessage` 对未知 role 抛错→跳过整条消息的语义）。
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "user" => Some(Self::User),
            "assistant" => Some(Self::Assistant),
            "system" => Some(Self::System),
            _ => None,
        }
    }
}

/// 图片块内层 `source` 对象（存储形状：`{type:"base64", media_type, data}`
/// 或 `{type:"url", url}`——旧 `SessionManager.contentBlockToMap` url 分支
/// 不写 `media_type`）。
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ImageSource {
    /// 来源类型，`"base64"` 或 `"url"`（保留字段以忠实存储形状）。
    #[serde(rename = "type")]
    pub kind: String,
    /// MIME 类型（如 `image/png`；url 形状缺省，读取侧兜底见
    /// [`Self::media_type_or_default`]）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// base64 编码数据（url 形状缺省）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    /// 可信远程图片地址（base64 形状缺省）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

impl ImageSource {
    /// MIME 类型，缺省 `image/png`（旧 `parseContentBlocks` 缺省值）。
    #[must_use]
    pub fn media_type_or_default(&self) -> &str {
        self.media_type.as_deref().unwrap_or("image/png")
    }

    /// url 型来源的非空白远程地址（旧 parse `"url".equals(type) && url != null`
    /// 判定；空白地址下游必然跳过，此处一并收紧）。
    #[must_use]
    pub fn remote_url(&self) -> Option<&str> {
        if self.kind != "url" {
            return None;
        }
        self.url.as_deref().filter(|url| !url.trim().is_empty())
    }
}

/// 消息内容块——`content_json` 的**存储形状**（`snake_case`）。
///
/// 与旧系统 `SessionManager.contentBlockToMap`（写入）与
/// `parseContentBlocks`（读取）逐字段对齐；`is_error` 仅在 `true` 时写出
///（旧写入侧 `if (result.isError()) map.put("is_error", true)`）。
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StoredBlock {
    /// 文本块。
    Text {
        /// 文本内容。
        text: String,
    },
    /// 工具调用块（`{type:"tool_use", id, name, input}`）。
    ToolUse {
        /// 工具调用 ID。
        id: String,
        /// 工具名。
        name: String,
        /// 工具入参（原样 JSON）。
        input: serde_json::Value,
    },
    /// 工具结果块（`{type:"tool_result", tool_use_id, content, is_error?, metadata?}`）。
    ToolResult {
        /// 工具调用 ID。
        tool_use_id: String,
        /// 结果文本。
        content: String,
        /// 是否出错（缺省 false；序列化仅在 true 时写出）。
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        is_error: bool,
        /// 结构化结果元数据（可缺省）。
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
    },
    /// 思考块。
    Thinking {
        /// 思考文本。
        thinking: String,
    },
    /// 图片块（`{type:"image", source:{...}, width?, height?}`）。
    Image {
        /// 来源描述（base64 或 url）。
        source: ImageSource,
        /// 原始宽度（旧系统仅在 >0 时写出）。
        #[serde(skip_serializing_if = "Option::is_none")]
        width: Option<i64>,
        /// 原始高度（旧系统仅在 >0 时写出）。
        #[serde(skip_serializing_if = "Option::is_none")]
        height: Option<i64>,
    },
    /// 脱敏思考块（`{type:"redacted_thinking", data}`）。
    RedactedThinking {
        /// 不透明数据。
        data: String,
    },
}

/// 宽容解析 `content_json` → 内容块列表（对齐 `parseContentBlocks`）。
///
/// - 非数组开头或整体非法 JSON → 回退为单条 [`StoredBlock::Text`]（裸文本）；
/// - 数组内**逐块**解析，未知 `type` / 缺字段 / 单块损坏 → 跳过该块
///   （对齐旧系统逐块 try-catch `log.warn` 后继续）。
#[must_use]
pub fn parse_blocks(content_json: &str) -> Vec<StoredBlock> {
    let trimmed = content_json.trim();
    if !trimmed.starts_with('[') {
        return vec![StoredBlock::Text {
            text: content_json.to_owned(),
        }];
    }
    match serde_json::from_str::<Vec<serde_json::Value>>(content_json) {
        Ok(nodes) => nodes
            .into_iter()
            .filter_map(|node| serde_json::from_value::<StoredBlock>(node).ok())
            .filter(|block| match block {
                // url / data 皆无的图片块整块丢弃（旧 parse else-if 语义）。
                StoredBlock::Image { source, .. } => {
                    source.remote_url().is_some() || source.data.is_some()
                }
                _ => true,
            })
            .collect(),
        Err(_) => vec![StoredBlock::Text {
            text: content_json.to_owned(),
        }],
    }
}

/// 从首条 user 消息 `content_json` 提取目标预览（对齐 `extractGoalPreview`）：
/// 取第一个非空 text 块，折叠连续空白为单空格，超 80 字符（codepoint 计）
/// 截断加 `…`。
#[must_use]
pub fn goal_preview(content_json: Option<&str>) -> Option<String> {
    let json = content_json?;
    if json.trim().is_empty() {
        return None;
    }
    let nodes = serde_json::from_str::<Vec<serde_json::Value>>(json).ok()?;
    for node in nodes {
        if node.get("type").and_then(serde_json::Value::as_str) != Some("text") {
            continue;
        }
        let raw = node
            .get("text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
        if collapsed.is_empty() {
            continue;
        }
        if collapsed.chars().count() <= 80 {
            return Some(collapsed);
        }
        let prefix = collapsed.chars().take(80).collect::<String>();
        return Some(format!("{prefix}…"));
    }
    None
}

/// 会话摘要（对齐 `SessionSummary` record；列表端点数据载体）。
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSummary {
    /// 会话 ID（UUIDv4）。
    pub id: String,
    /// 标题（可空）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// 首条 user 消息目标预览（可空）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_preview: Option<String>,
    /// 模型标识。
    pub model: String,
    /// 工作目录绝对路径。
    pub working_directory: String,
    /// 消息总数（子查询 COUNT）。
    pub message_count: i64,
    /// 累计成本（USD）。
    pub cost_usd: f64,
    /// 创建时间（epoch 毫秒；serde 为 RFC 3339 字符串）。
    #[serde(with = "serde_iso_ms")]
    pub created_at: i64,
    /// 最后更新时间（epoch 毫秒；serde 为 RFC 3339 字符串）。
    #[serde(with = "serde_iso_ms")]
    pub updated_at: i64,
}

/// 完整会话（对齐 `SessionData` record；详情/resume/export 数据源）。
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionDetail {
    /// 会话 ID。
    pub session_id: String,
    /// 模型标识。
    pub model: String,
    /// 工作目录。
    pub working_dir: String,
    /// 标题（可空；`None` 序列化为 `null`，对齐 export 样例）。
    pub title: Option<String>,
    /// 会话状态（`active` / `closed`…，小写存储）。
    pub status: String,
    /// 按序完整消息历史。
    pub messages: Vec<MessageRecord>,
    /// `metadata_json` 解析结果（损坏时为空 map，对齐 `parseJsonMap`）。
    pub config: serde_json::Map<String, serde_json::Value>,
    /// 累计 token 用量（复用 `zk_protocol::Usage`）。
    pub total_usage: Usage,
    /// 累计成本（USD）。
    pub total_cost_usd: f64,
    /// 上下文压缩摘要（可空）。
    pub summary: Option<String>,
    /// 创建时间（epoch 毫秒）。
    #[serde(with = "serde_iso_ms")]
    pub created_at: i64,
    /// 最后更新时间（epoch 毫秒）。
    #[serde(with = "serde_iso_ms")]
    pub updated_at: i64,
}

/// 消息行（忠实 `messages` 表列 + 已解析内容块）。
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageRecord {
    /// 消息 ID（UUIDv4）。
    pub id: String,
    /// 所属会话 ID。
    pub session_id: String,
    /// 角色。
    pub role: MessageRole,
    /// 内容块（存储形状，宽容解析产物）。
    pub content: Vec<StoredBlock>,
    /// 停止原因（`end_turn` 等；仅 assistant 使用）。
    pub stop_reason: Option<String>,
    /// 输入 token 数（仅 assistant 使用）。
    pub input_tokens: i64,
    /// 输出 token 数（仅 assistant 使用）。
    pub output_tokens: i64,
    /// 会话内单调序号（`UNIQUE(session_id, seq_num)`）。
    pub seq_num: i64,
    /// 创建时间（epoch 毫秒）。
    #[serde(with = "serde_iso_ms")]
    pub created_at: i64,
}

/// 追加消息的写入参数（id / `created_at` / `seq_num` 由仓储分配）。
#[derive(Clone, Debug, PartialEq)]
pub struct NewMessage {
    /// 角色。
    pub role: MessageRole,
    /// 内容块（将序列化为存储形状 `content_json`）。
    pub content: Vec<StoredBlock>,
    /// 停止原因（可空）。
    pub stop_reason: Option<String>,
    /// 输入 token 数。
    pub input_tokens: i64,
    /// 输出 token 数。
    pub output_tokens: i64,
}

/// 会话列表分页结果（`GET /api/sessions` 载体；nextCursor 已编码）。
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionPage {
    /// 本页会话摘要（`updated_at` 降序）。
    pub sessions: Vec<SessionSummary>,
    /// 是否还有更早的会话。
    pub has_more: bool,
    /// 下一页游标（`Base64("updated_at|id")`；尾页为 `None`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// 消息列表分页结果（`GET /api/sessions/{id}/messages` 载体）。
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagePage {
    /// 本页消息（`seq_num` 升序）。
    pub messages: Vec<MessageRecord>,
    /// 是否还有后续消息。
    pub has_more: bool,
    /// 下一页游标（`Base64(十进制索引)`；尾页为 `None`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_roundtrip() {
        for role in [
            MessageRole::User,
            MessageRole::Assistant,
            MessageRole::System,
        ] {
            assert_eq!(MessageRole::parse(role.as_str()), Some(role));
        }
        assert_eq!(MessageRole::parse("tool"), None);
        let json = serde_json::to_string(&MessageRole::User).unwrap();
        assert_eq!(json, r#""user""#);
    }

    #[test]
    fn stored_block_storage_shape_is_snake_case() {
        let blocks = vec![
            StoredBlock::Text { text: "hi".into() },
            StoredBlock::ToolUse {
                id: "t1".into(),
                name: "Bash".into(),
                input: serde_json::json!({"cmd": "ls"}),
            },
            StoredBlock::ToolResult {
                tool_use_id: "t1".into(),
                content: "ok".into(),
                is_error: false,
                metadata: None,
            },
            StoredBlock::Image {
                source: ImageSource {
                    kind: "base64".into(),
                    media_type: Some("image/png".into()),
                    data: Some("AAAA".into()),
                    url: None,
                },
                width: None,
                height: None,
            },
            StoredBlock::RedactedThinking {
                data: "blob".into(),
            },
        ];
        let json = serde_json::to_string(&blocks).unwrap();
        // 存储形状逐键断言（snake_case + image 嵌套 source）。
        assert!(json.contains(r#""type":"text""#));
        assert!(json.contains(r#""type":"tool_use","id":"t1","name":"Bash""#));
        assert!(
            json.contains(r#""type":"tool_result","tool_use_id":"t1","content":"ok""#)
                && !json.contains("is_error")
        );
        assert!(
            json.contains(r#""type":"image","source":{"type":"base64""#)
                && json.contains(r#""media_type":"image/png","data":"AAAA""#)
        );
        assert!(json.contains(r#""type":"redacted_thinking","data":"blob""#));
        // roundtrip。
        assert_eq!(parse_blocks(&json), blocks);
    }

    #[test]
    fn image_url_storage_shape_omits_media_type_and_data() {
        // url 存储形状只有 `{type:"url", url}`（旧 url 分支不写 media_type/data）。
        let block = StoredBlock::Image {
            source: ImageSource {
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
        let json = serde_json::to_string(&vec![block.clone()]).unwrap();
        assert!(json.contains(r#""source":{"type":"url","url":"https://"#));
        assert!(!json.contains("media_type") && !json.contains(r#""data""#));
        // roundtrip + 读取侧缺省 media_type 兜底 image/png（旧 parse 缺省值）。
        let parsed = parse_blocks(&json);
        assert_eq!(parsed, vec![block]);
        let StoredBlock::Image { source, .. } = &parsed[0] else {
            panic!("expected image block");
        };
        assert_eq!(source.media_type_or_default(), "image/png");
        assert_eq!(
            source.remote_url(),
            Some("https://bkt.oss.example.com/zhikuncode-artifacts/clipboard/a.png")
        );
        // url / data 皆无的图片块被整块丢弃（旧 parse else-if 语义）。
        let empty = r#"[{"type":"image","source":{"type":"base64"}},{"type":"text","text":"k"}]"#;
        assert_eq!(
            parse_blocks(empty),
            vec![StoredBlock::Text { text: "k".into() }]
        );
    }

    #[test]
    fn tool_result_error_flag_only_written_when_true() {
        let block = StoredBlock::ToolResult {
            tool_use_id: "t".into(),
            content: "boom".into(),
            is_error: true,
            metadata: None,
        };
        // 存储契约是块数组；单块序列化后经数组通道 roundtrip。
        let json = serde_json::to_string(&vec![block.clone()]).unwrap();
        assert!(json.contains(r#""is_error":true"#));
        assert_eq!(parse_blocks(&json), vec![block]);
    }

    #[test]
    fn parse_blocks_tolerates_legacy_payloads() {
        // 裸文本回退（非数组）。
        assert_eq!(
            parse_blocks("plain text"),
            vec![StoredBlock::Text {
                text: "plain text".into()
            }]
        );
        // 损坏 JSON 回退（数组开头但截断）。
        assert_eq!(
            parse_blocks(r#"[{"type":"text""#),
            vec![StoredBlock::Text {
                text: r#"[{"type":"text""#.into()
            }]
        );
        // 未知块跳过、已知块保留（对齐逐块 try-catch）。
        let mixed = r#"[{"type":"future_block","x":1},{"type":"text","text":"ok"}]"#;
        assert_eq!(
            parse_blocks(mixed),
            vec![StoredBlock::Text { text: "ok".into() }]
        );
        // 缺必需字段的 tool_use 被跳过。
        let broken = r#"[{"type":"tool_use","id":"1"},{"type":"text","text":"a"}]"#;
        assert_eq!(
            parse_blocks(broken),
            vec![StoredBlock::Text { text: "a".into() }]
        );
    }

    #[test]
    fn goal_preview_truncates_at_80_codepoints() {
        let short = r#"[{"type":"text","text":"  hello   world "}]"#;
        assert_eq!(goal_preview(Some(short)).as_deref(), Some("hello world"));
        // 非 text 块跳过；空白折叠；81 字符截断 + "…"。
        let long_text = "字".repeat(81);
        let long = serde_json::json!([
            {"type": "image", "x": 1},
            {"type": "text", "text": long_text},
        ])
        .to_string();
        let preview = goal_preview(Some(&long)).unwrap();
        assert_eq!(preview.chars().count(), 81);
        assert!(preview.ends_with('…'));
        // null / 空 / 非 JSON / 无 text 块 → None。
        assert_eq!(goal_preview(None), None);
        assert_eq!(goal_preview(Some("   ")), None);
        assert_eq!(goal_preview(Some("not json")), None);
        assert_eq!(
            goal_preview(Some(r#"[{"type":"image","source":{}}]"#)),
            None
        );
    }

    #[test]
    fn summary_serialization_matches_sample_shape() {
        // 字段名集合与实采样例 GET_api-sessions.json 逐字对齐。
        let summary = SessionSummary {
            id: "s1".into(),
            title: None,
            goal_preview: None,
            model: "qwen3.7-max".into(),
            working_directory: "/tmp".into(),
            message_count: 0,
            cost_usd: 0.0,
            created_at: 1_786_792_040_564,
            updated_at: 1_786_792_040_564,
        };
        let value = serde_json::to_value(&summary).unwrap();
        // serde_json Map 为字母序（非 preserve_order）：按键集合比较。
        let mut keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "costUsd",
                "createdAt",
                "id",
                "messageCount",
                "model",
                "updatedAt",
                "workingDirectory"
            ]
        );
        // 毫秒领域值序列化为恒 6 位微秒（样例微秒尾 532 在此精度下为 000）。
        assert_eq!(
            value["createdAt"].as_str(),
            Some("2026-08-15T11:07:20.564000Z")
        );
        // 反序列化接受毫秒数字（FlexEpoch 输入域）。
        let mut numeric = serde_json::to_value(&summary).unwrap();
        numeric["createdAt"] = serde_json::json!(1_786_792_040_564_i64);
        let back: SessionSummary = serde_json::from_value(numeric).unwrap();
        assert_eq!(back.created_at, summary.created_at);
    }

    #[test]
    fn detail_serialization_matches_sample_shape() {
        let detail = SessionDetail {
            session_id: "s1".into(),
            model: "m".into(),
            working_dir: "/w".into(),
            title: None,
            status: "active".into(),
            messages: vec![],
            config: serde_json::Map::new(),
            total_usage: Usage::default(),
            total_cost_usd: 0.0,
            summary: None,
            created_at: 1_786_792_040_564,
            updated_at: 1_786_792_040_564,
        };
        let value = serde_json::to_value(&detail).unwrap();
        // 对齐 GET_api-sessions-id / export 样例键集（title/summary 为 null 保留）。
        let mut keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        let mut expected = vec![
            "sessionId",
            "model",
            "workingDir",
            "title",
            "status",
            "messages",
            "config",
            "totalUsage",
            "totalCostUsd",
            "summary",
            "createdAt",
            "updatedAt",
        ];
        expected.sort_unstable();
        assert_eq!(keys, expected);
        assert!(value["title"].is_null());
    }
}
