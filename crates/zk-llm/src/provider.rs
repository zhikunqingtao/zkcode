//! `ChatProvider` 抽象：请求模型、统一流事件与停止原因归一化。
//!
//! 建模来源（旧仓库只读规格）：
//! - `llm/LlmProvider.java`（接口形状：provider 名 / 流式调用 / 取消上下文）
//! - `llm/LlmStreamEvent.java`（统一事件：`TextDelta` / `ThinkingDelta` /
//!   `MessageDelta`(usage, stopReason) / `ToolUseStart` / `ToolInputDelta` /
//!   Error）
//! - `llm/ThinkingConfig.java`（思考三态：Adaptive / Enabled / Disabled）
//! - `llm/ToolDefinition.java`（工具三元组：name / description / input schema）
//!
//! # trait 形态裁决（D-S6-1）
//!
//! [`ChatProvider`] 采用 **object-safe 的同步方法 + 返回 `BoxStream`** 形态，
//! 不使用 `async fn` in trait：
//!
//! 1. 方法返回惰性流（首个事件 poll 时才发出 HTTP 请求），「发起请求」本身
//!    被包进流内部，方法无需 async——天然对象安全，注册表可直接持有
//!    `Arc<dyn ChatProvider>`（双提供商注册、Phase 2+ 多 Provider 全量接入
//!    均是 dyn 场景）；
//! 2. 避免 `async-trait` 的 `Box<dyn Future>` 双层打包与生命周期体操；
//! 3. 流式消费方（zk-engine）本就逐项 poll，`BoxStream` 是最自然的交接物。
//!
//! # 与旧回调形态的映射
//!
//! 旧 `StreamChatCallback`（onEvent / onComplete / onError）→ 流语义：
//! - `onEvent(TextDelta|ThinkingDelta)` → [`ProviderEvent::TextDelta`] /
//!   [`ProviderEvent::ThinkingDelta`]；
//! - usage-only chunk 的 `onEvent(MessageDelta(usage, null))` →
//!   [`ProviderEvent::UsageUpdate`]；
//! - 含 `finish_reason` 的 `onEvent(MessageDelta(usage, reason))` →
//!   [`ProviderEvent::Finish`]（`usage: Option`——旧 Java 无 usage 时发零值
//!   哨兵，新实现以 `None` 显式表达「本 chunk 未携带」，见 D-S6-7）；
//! - 旧 `ToolCallAccumulator` 的 `ContentBlockStart(ToolUseBlock)` /
//!   `InputJsonDelta` → [`ProviderEvent::ToolUseStart`] /
//!   [`ProviderEvent::ToolInputDelta`]（Phase 2.2 契约冻结；旧 finish 时
//!   补发的 `BlockStop` 不建模——引擎在 Finish 时统一 flush 草稿，见
//!   docs/compatibility.md §3）；
//! - `onComplete`（`[DONE]` / 流耗尽）→ 流自然终止（无事件）；
//! - `onError` → [`ProviderEvent::Error`]（流内错误，见 D-S6-3）。
#![allow(clippy::module_name_repetitions)] // ProviderEvent 等命名对齐旧领域术语，不按模块前缀裁剪

use std::borrow::Cow;

use futures::stream::BoxStream;
use tokio_util::sync::CancellationToken;
use zk_protocol::model::Usage;

use crate::cache::{SystemPrompt, SystemPromptSegment, ToolOrigin, tool_cache_breakpoint};
use crate::error::ProviderError;

/// Chat 消息角色（`OpenAI` Chat Completions 语义子集）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// 系统指令。
    System,
    /// 用户输入。
    User,
    /// 助手历史回复。
    Assistant,
    /// 工具结果回传（Phase 2.2 激活；对照旧请求构造的
    /// `{role:"tool", tool_call_id, content}` 形状）。
    Tool,
}

impl Role {
    /// `OpenAI` wire 格式角色名。
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
        }
    }
}

/// 助手消息携带的一次工具调用请求（`OpenAI` wire 形状的强类型载体）。
///
/// 对照旧请求构造：assistant 历史含 `tool_use` 块时序列化为
/// `{id, type:"function", function:{name, arguments}}`，`arguments`
/// 为 JSON **字符串**（非对象，`OpenAI` 协议要求）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolCallRequest {
    /// 工具调用 ID（`tool_call_id` 回填锚点）。
    pub id: String,
    /// 工具名。
    pub name: String,
    /// 入参 JSON 字符串。
    pub arguments: String,
}

/// 一条 chat 消息（role + 文本、多模态图片、thinking 与工具调用载体）。
///
/// OpenAI-compatible providers receive user images as content arrays and assistant
/// thinking as `reasoning_content`. Anthropic receives image source blocks and omits
/// reconstructed thinking when the protocol would require an unavailable signature.
/// 工具语义（Phase 2.2 激活）：`tool_calls` 仅 assistant 消息使用；
/// `tool_call_id` 仅 [`Role::Tool`] 消息使用（旧每 `tool_result` 块一条
/// `{role:"tool"}` 消息）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatMessage {
    /// 消息角色。
    pub role: Role,
    /// 消息文本。
    pub content: String,
    /// Validated inline images attached to a user message.
    pub images: Vec<ImageSource>,
    /// Assistant reasoning reconstructed from durable history. Providers may omit it
    /// when their wire protocol requires an unavailable signature.
    pub thinking: Option<String>,
    /// 助手工具调用请求（非 assistant / 无调用时为空）。
    pub tool_calls: Vec<ToolCallRequest>,
    /// 工具结果对应的调用 ID（仅 [`Role::Tool`] 消息）。
    pub tool_call_id: Option<String>,
}

/// Provider-neutral validated image input.
///
/// Exactly one of `data` / `url` is expected to be present; providers skip
/// the image when both are absent (Java `appendImageUrlPart` return
/// semantics).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageSource {
    /// Whitelisted media type (`image/png`, `image/jpeg`, `image/gif`, `image/webp`).
    pub media_type: String,
    /// Standard padded base64 payload without a data-URI prefix (`None` for
    /// URL-referenced images).
    pub data: Option<String>,
    /// Trusted remote image URL, forwarded verbatim to the provider when present.
    pub url: Option<String>,
}

impl ChatMessage {
    fn plain(role: Role, content: impl Into<String>) -> Self {
        Self {
            role,
            content: content.into(),
            images: Vec::new(),
            thinking: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    /// 构造 system 消息。
    #[must_use]
    pub fn system(content: impl Into<String>) -> Self {
        Self::plain(Role::System, content)
    }

    /// 构造 user 消息。
    #[must_use]
    pub fn user(content: impl Into<String>) -> Self {
        Self::plain(Role::User, content)
    }

    /// Construct a user turn with validated image inputs.
    #[must_use]
    pub fn user_with_images(content: impl Into<String>, images: Vec<ImageSource>) -> Self {
        Self {
            images,
            ..Self::plain(Role::User, content)
        }
    }

    /// 构造 assistant 消息。
    #[must_use]
    pub fn assistant(content: impl Into<String>) -> Self {
        Self::plain(Role::Assistant, content)
    }

    /// Attach durable assistant reasoning to a reconstructed provider message.
    #[must_use]
    pub fn with_thinking(mut self, thinking: Option<String>) -> Self {
        self.thinking = thinking.filter(|value| !value.is_empty());
        self
    }

    /// 构造携带工具调用的 assistant 消息（多轮回填历史）。
    #[must_use]
    pub fn assistant_tool_calls(
        content: impl Into<String>,
        tool_calls: Vec<ToolCallRequest>,
    ) -> Self {
        Self {
            role: Role::Assistant,
            content: content.into(),
            images: Vec::new(),
            thinking: None,
            tool_calls,
            tool_call_id: None,
        }
    }

    /// 构造 tool 结果消息（对照旧 `{role:"tool", tool_call_id, content}`）。
    #[must_use]
    pub fn tool(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: Role::Tool,
            content: content.into(),
            images: Vec::new(),
            thinking: None,
            tool_calls: Vec::new(),
            tool_call_id: Some(tool_call_id.into()),
        }
    }
}

/// 工具定义三元组（对齐旧 `ToolDefinition`：name / description / JSON Schema）。
///
/// 序列化为 `OpenAI` function calling 格式由 [`crate::openai_compat`] 的请求
/// 构建层**总是**执行（修复旧强类型桥接经 `toAnthropicFormat` 透传
/// `Anthropic` 形状给 `OpenAI` 兼容端点的缺陷，见 D-S6-8）。
#[derive(Clone, Debug, PartialEq)]
pub struct ToolSpec {
    /// 工具名。
    pub name: String,
    /// 工具描述。
    pub description: String,
    /// JSON Schema 入参定义。
    pub parameters: serde_json::Value,
}

/// 思考模式三态（对齐旧 `ThinkingConfig` sealed interface）。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ThinkingMode {
    /// 自适应——模型 / 服务端自行决定是否及多少思考。
    Adaptive,
    /// 显式启用。
    Enabled,
    /// 禁用（默认，对齐 Phase 1 最小引擎）。
    #[default]
    Disabled,
}

impl ThinkingMode {
    /// 是否要求 provider 具备思考能力（对齐旧 `requiresThinkingSupport`：
    /// Adaptive / Enabled 为 `true`，Disabled 为 `false`）。
    #[must_use]
    pub fn requires_support(self) -> bool {
        !matches!(self, Self::Disabled)
    }
}

/// 一次流式补全请求。
///
/// 生成参数按旧 Java 实际下发字段收敛：`max_tokens`（kimi 系列在 wire 层
/// 自动换名 `max_completion_tokens`）+ 思考参数（按模型族判定）；旧实现
/// **不发送** temperature / `top_p` / stop，本实现保持一致（不虚构字段）。
///
/// prompt caching（`Anthropic` 专属）：[`Self::system_segments`] 与
/// [`Self::tool_cache_breakpoint`] 承载两个**相互独立**的缓存断点，规则、门控与
/// 旧仓库对照见 [`crate::cache`]；非 `Anthropic` provider 忽略这两个字段。
#[derive(Clone, Debug)]
pub struct ChatRequest {
    /// 目标模型标识。
    pub model: String,
    /// 对话消息（不含 system prompt——后者独立字段）。
    pub messages: Vec<ChatMessage>,
    /// 系统提示（空白视为无，对齐旧 `isBlank()` 判定）。
    ///
    /// 与 [`Self::system_segments`] 二者取一——读取端一律用
    /// [`Self::system_text`]（分段视图优先拍平）。
    pub system_prompt: Option<String>,
    /// 系统提示分段视图（prompt caching 断点 #1，对照旧 `llm/SystemPrompt.java`）。
    ///
    /// 空 = 无分段视图：`Anthropic` 路径按 [`Self::system_prompt`] 渲染**单块**
    /// 并对整段打断点（逐字段等价旧 `AnthropicProvider` L158-163）。非空时
    /// `Anthropic` 路径逐段成块，`cache_control=true` 的段末尾落断点（静态前缀
    /// 与动态后缀的分界）；`OpenAI` 兼容路径只取拍平纯文本、永不打断点。
    pub system_segments: Vec<SystemPromptSegment>,
    /// 工具定义（空 = 不下发 tools 字段）。
    pub tools: Vec<ToolSpec>,
    /// 工具定义缓存断点下标（prompt caching 断点 #2，对照旧
    /// `ToolRegistry.getToolDefinitionsWithCacheBreakpoint`）。
    ///
    /// `Some(i)` 时 `Anthropic` 路径在第 `i` 个工具定义上追加 `cache_control`
    /// （越界下标 = 不注入）；取值由 [`tool_cache_breakpoint`] 依工具来源折算，
    /// 与 [`Self::system_segments`] 相互独立。
    pub tool_cache_breakpoint: Option<usize>,
    /// 最大输出 token 数。
    pub max_tokens: u32,
    /// 思考模式。
    pub thinking: ThinkingMode,
}

impl ChatRequest {
    /// 以目标模型构造空请求（默认：无消息、无工具、`max_tokens = 8192`、
    /// 思考关闭）。
    #[must_use]
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            messages: Vec::new(),
            system_prompt: None,
            system_segments: Vec::new(),
            tools: Vec::new(),
            tool_cache_breakpoint: None,
            max_tokens: 8192,
            thinking: ThinkingMode::Disabled,
        }
    }

    /// 追加一条消息（链式）。
    #[must_use]
    pub fn with_message(mut self, message: ChatMessage) -> Self {
        self.messages.push(message);
        self
    }

    /// 设置系统提示（链式）。
    #[must_use]
    pub fn with_system_prompt(mut self, prompt: Option<String>) -> Self {
        self.system_prompt = prompt;
        self
    }

    /// 设置分段系统提示（链式，prompt caching 断点 #1）。
    ///
    /// 分段是 wire 层的权威视图（`Anthropic` 逐段成块）；纯文本读取请一律用
    /// [`Self::system_text`]（对照旧强类型桥接的 `toPlainText()`）。
    #[must_use]
    pub fn with_system(mut self, system: SystemPrompt) -> Self {
        self.system_segments = system.into_segments();
        self
    }

    /// 设置工具定义缓存断点下标（链式，prompt caching 断点 #2）。
    #[must_use]
    pub fn with_tool_cache_breakpoint(mut self, breakpoint: Option<usize>) -> Self {
        self.tool_cache_breakpoint = breakpoint;
        self
    }

    /// 依工具来源序列折算并设置工具定义缓存断点（链式）。
    ///
    /// `origins` 必须与 [`Self::tools`] 同序；落点规则见
    /// [`tool_cache_breakpoint`]（最后一个内建工具，且存在 MCP 工具时才成立）。
    #[must_use]
    pub fn with_tool_origins(self, origins: &[ToolOrigin]) -> Self {
        let breakpoint = tool_cache_breakpoint(origins);
        self.with_tool_cache_breakpoint(breakpoint)
    }

    /// 系统提示纯文本（分段视图优先拍平，段间 `"\n"`——对照旧
    /// `SystemPrompt.toPlainText()`；空白视为无，对齐旧 `isBlank()` 判定）。
    #[must_use]
    pub fn system_text(&self) -> Option<Cow<'_, str>> {
        if self.system_segments.is_empty() {
            return self
                .system_prompt
                .as_deref()
                .filter(|text| !text.trim().is_empty())
                .map(Cow::Borrowed);
        }
        let joined = self
            .system_segments
            .iter()
            .map(|segment| segment.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        if joined.trim().is_empty() {
            None
        } else {
            Some(Cow::Owned(joined))
        }
    }

    /// 设置工具定义（链式）。
    #[must_use]
    pub fn with_tools(mut self, tools: Vec<ToolSpec>) -> Self {
        self.tools = tools;
        self
    }

    /// 设置最大输出 token（链式）。
    #[must_use]
    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = max_tokens;
        self
    }

    /// 设置思考模式（链式）。
    #[must_use]
    pub fn with_thinking(mut self, thinking: ThinkingMode) -> Self {
        self.thinking = thinking;
        self
    }
}

/// 停止原因（旧内部统一格式）。
///
/// 归一化对齐旧 `normalizeFinishReason`：`stop` → `end_turn`、
/// `tool_calls` → `tool_use`、`length` → `max_tokens`、其余（含
/// `content_filter` 与未知值）原样透传。`as_str` 的输出域即
/// zk-protocol `MessageComplete.stop_reason` 的取值域。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FinishReason {
    /// `stop`——自然结束（内部名 `end_turn`）。
    EndTurn,
    /// `length`——达到 token 上限（内部名 `max_tokens`）。
    MaxTokens,
    /// `tool_calls`——模型请求工具（内部名 `tool_use`）。
    ToolUse,
    /// `content_filter`——内容过滤（原样透传）。
    ContentFilter,
    /// 未知名（原样保留，不丢弃诊断信息）。
    Other(String),
}

impl FinishReason {
    /// 从 `OpenAI` 兼容 `finish_reason` 原值归一化。
    #[must_use]
    pub fn from_openai(raw: &str) -> Self {
        match raw {
            "stop" => Self::EndTurn,
            "length" => Self::MaxTokens,
            "tool_calls" => Self::ToolUse,
            "content_filter" => Self::ContentFilter,
            other => Self::Other(other.to_owned()),
        }
    }

    /// 内部统一格式名（对齐旧归一化输出的字符串域）。
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::EndTurn => "end_turn",
            Self::MaxTokens => "max_tokens",
            Self::ToolUse => "tool_use",
            Self::ContentFilter => "content_filter",
            Self::Other(raw) => raw,
        }
    }
}

/// 统一 LLM 流事件（对齐旧 `LlmStreamEvent`，Phase 2.2 含工具增量族）。
///
/// 事件顺序保证：单 chunk 内先 [`Self::ThinkingDelta`] 后
/// [`Self::TextDelta`] 再工具增量（对齐旧 `processChunk` 的字段处理顺序）；
/// [`Self::Finish`] 之后仍可能出现 [`Self::UsageUpdate`]（`OpenAI`
/// `stream_options.include_usage` 的 usage-only 尾块）。
///
/// 工具增量契约（对照旧 `ToolCallAccumulator`，`OpenAiCompatibleProvider`
/// L648-694）：同一调用先 [`Self::ToolUseStart`]（id+name 首次齐备时恰
/// 一次）再零到多次 [`Self::ToolInputDelta`]（arguments 分片，空串也发）；
/// 消费方在 [`Self::Finish`]（`finish_reason=tool_calls`）时以累积的
/// arguments 组装完整入参（旧 `BlockStop` 补发不建模，留痕 §3）。
#[derive(Clone, Debug, PartialEq)]
pub enum ProviderEvent {
    /// 文本增量（`choices[].delta.content`，空串不产出）。
    TextDelta {
        /// 增量文本。
        text: String,
    },
    /// 思考增量（`choices[].delta.reasoning_content` 映射，空串不产出）。
    ThinkingDelta {
        /// 增量思考文本。
        thinking: String,
    },
    /// 工具调用开始（`tool_calls` delta 的 id+name 首次齐备，每调用恰一次）。
    ToolUseStart {
        /// 工具调用 ID。
        id: String,
        /// 工具名。
        name: String,
    },
    /// 工具入参分片（`function.arguments` 增量；空串也产出，对齐旧行为）。
    ToolInputDelta {
        /// 所属工具调用 ID。
        id: String,
        /// 入参 JSON 字符串分片。
        delta: String,
    },
    /// usage-only 尾块（`choices` 缺失 / 空数组 + 携带 usage）。
    UsageUpdate {
        /// 服务端权威 token 用量。
        usage: Usage,
    },
    /// 流终结：`finish_reason` 归一化 + 本 chunk 携带的 usage。
    ///
    /// `usage` 为 `None` 表示该 chunk 未携带（`stream_options.include_usage`
    /// 模式下权威用量随后的 [`Self::UsageUpdate`] 到达）；取消路径**从不**
    /// 产出本事件。
    Finish {
        /// 归一化停止原因。
        finish_reason: FinishReason,
        /// 本 chunk 携带的用量（可能 `None`）。
        usage: Option<Usage>,
    },
    /// 流中错误（网络 / HTTP 状态 / 解析）。
    ///
    /// 产出后流是否终止取决于错误来源：HTTP 状态错误与网络错误终止流
    ///（对齐旧 `handleErrorResponse` / `IOException` 的 return 语义）；单
    /// chunk JSON 解析失败对齐旧 `processChunk` 的宽容行为——产出本事件
    /// 后继续消费后续 chunk（D-S6-3）；`tool_calls` 增量流身份缺失
    ///（对齐旧 `INVALID_TOOL_CALL_STREAM`，非重试）产出本事件后**终止**流。
    Error {
        /// 错误分类与详情。
        error: ProviderError,
    },
}

/// LLM Provider 抽象（object-safe，见模块文档 trait 形态裁决）。
pub trait ChatProvider: Send + Sync {
    /// 供应商标识（注册表键，如 `dashscope` / `openai`）。
    fn provider_name(&self) -> &str;

    /// 发起流式补全，返回惰性事件流。
    ///
    /// 返回的 `Err` 仅覆盖**建立期**失败（请求体构建、配置非法）；
    /// HTTP 状态错误、网络错误与解析错误作为流内
    /// [`ProviderEvent::Error`] 产出（D-S6-3）。`cancel` 触发后流静默
    /// 终止——不产出 [`ProviderEvent::Finish`]（杜绝半条消息的假成功）
    /// 与 [`ProviderEvent::Error`]，且丢弃一切未投递的积压事件。
    ///
    /// # Errors
    ///
    /// 请求体无法序列化或提供商配置非法（[`ProviderError::Config`]）时
    /// 返回错误；网络与 HTTP 失败不在此报告（走流内 `Error` 事件）。
    fn chat_stream(
        &self,
        request: ChatRequest,
        cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finish_reason_normalization_matches_legacy() {
        // 旧 normalizeFinishReason：stop→end_turn、tool_calls→tool_use、
        // length→max_tokens、default→原样。
        for (raw, normalized) in [
            ("stop", "end_turn"),
            ("tool_calls", "tool_use"),
            ("length", "max_tokens"),
            ("content_filter", "content_filter"),
            ("function_call", "function_call"),
            ("weird_reason", "weird_reason"),
        ] {
            assert_eq!(FinishReason::from_openai(raw).as_str(), normalized);
        }
        assert_eq!(
            FinishReason::from_openai("stop"),
            FinishReason::from_openai("stop")
        );
        assert_ne!(
            FinishReason::from_openai("stop"),
            FinishReason::from_openai("length")
        );
    }

    #[test]
    fn thinking_mode_requires_support_matches_legacy() {
        assert!(ThinkingMode::Adaptive.requires_support());
        assert!(ThinkingMode::Enabled.requires_support());
        assert!(!ThinkingMode::Disabled.requires_support());
    }

    #[test]
    fn chat_message_constructors_set_roles() {
        assert_eq!(ChatMessage::system("s").role, Role::System);
        assert_eq!(ChatMessage::user("u").role, Role::User);
        assert_eq!(ChatMessage::assistant("a").role, Role::Assistant);
        assert_eq!(Role::Assistant.as_str(), "assistant");
        assert_eq!(Role::Tool.as_str(), "tool");
        // 纯文本构造器不携带工具载体。
        let plain = ChatMessage::user("u");
        assert!(plain.images.is_empty());
        assert!(plain.tool_calls.is_empty());
        assert!(plain.tool_call_id.is_none());

        let image = ChatMessage::user_with_images(
            "inspect",
            vec![ImageSource {
                media_type: "image/png".into(),
                data: Some("aGVsbG8=".into()),
                url: None,
            }],
        );
        assert_eq!(image.images.len(), 1);
        assert_eq!(image.images[0].media_type, "image/png");
    }

    #[test]
    fn chat_message_tool_constructors() {
        let tool = ChatMessage::tool("call-1", "result text");
        assert_eq!(tool.role, Role::Tool);
        assert_eq!(tool.tool_call_id.as_deref(), Some("call-1"));
        assert_eq!(tool.content, "result text");
        assert!(tool.tool_calls.is_empty());

        let assistant = ChatMessage::assistant_tool_calls(
            "",
            vec![ToolCallRequest {
                id: "call-1".into(),
                name: "Echo".into(),
                arguments: r#"{"text":"hi"}"#.into(),
            }],
        );
        assert_eq!(assistant.role, Role::Assistant);
        assert_eq!(assistant.tool_calls.len(), 1);
        assert_eq!(assistant.tool_calls[0].name, "Echo");
        assert!(assistant.tool_call_id.is_none());
    }

    #[test]
    fn chat_request_builder_defaults() {
        let req = ChatRequest::new("qwen3.7-max")
            .with_message(ChatMessage::user("hi"))
            .with_system_prompt(Some("be brief".into()))
            .with_max_tokens(1024)
            .with_thinking(ThinkingMode::Enabled);
        assert_eq!(req.model, "qwen3.7-max");
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.system_prompt.as_deref(), Some("be brief"));
        assert_eq!(req.max_tokens, 1024);
        assert_eq!(req.thinking, ThinkingMode::Enabled);
        assert!(req.tools.is_empty());
        // 默认值独立验证。
        let bare = ChatRequest::new("m");
        assert_eq!(bare.max_tokens, 8192);
        assert_eq!(bare.thinking, ThinkingMode::Disabled);
    }

    /// prompt caching 两个断点默认全关（= 旧单段 / 无工具标记行为）。
    #[test]
    fn chat_request_cache_breakpoints_default_off() {
        let bare = ChatRequest::new("m");
        assert!(bare.system_segments.is_empty(), "默认无分段视图");
        assert_eq!(bare.tool_cache_breakpoint, None);
        assert_eq!(bare.system_text(), None);
    }

    #[test]
    fn chat_request_system_text_prefers_segments() {
        let plain = ChatRequest::new("m").with_system_prompt(Some("solo".into()));
        assert_eq!(plain.system_text().as_deref(), Some("solo"));
        // 空白提示视为无（旧 isBlank 判定）。
        assert_eq!(
            ChatRequest::new("m")
                .with_system_prompt(Some("  \n ".into()))
                .system_text(),
            None
        );
        // 分段视图优先，段间以 "\n" 连接（旧 toPlainText）。
        let segmented = ChatRequest::new("m")
            .with_system_prompt(Some("superseded".into()))
            .with_system(SystemPrompt::segmented("static", "dynamic"));
        assert_eq!(segmented.system_text().as_deref(), Some("static\ndynamic"));
        assert_eq!(segmented.system_segments.len(), 2);
    }

    #[test]
    fn chat_request_tool_origins_compute_breakpoint() {
        // 最后一个内建工具（存在 MCP 工具）→ 下标 1。
        let mixed = ChatRequest::new("m").with_tool_origins(&[
            ToolOrigin::BuiltIn,
            ToolOrigin::BuiltIn,
            ToolOrigin::Mcp,
        ]);
        assert_eq!(mixed.tool_cache_breakpoint, Some(1));
        // 无 MCP 工具 → 无断点。
        let built_in_only =
            ChatRequest::new("m").with_tool_origins(&[ToolOrigin::BuiltIn, ToolOrigin::BuiltIn]);
        assert_eq!(built_in_only.tool_cache_breakpoint, None);
        // 显式设置与分段视图互不影响（两断点独立可控）。
        let explicit = ChatRequest::new("m").with_tool_cache_breakpoint(Some(3));
        assert_eq!(explicit.tool_cache_breakpoint, Some(3));
        assert!(explicit.system_segments.is_empty());
    }
}
