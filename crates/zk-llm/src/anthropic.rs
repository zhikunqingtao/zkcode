//! `Anthropic` 原生 Provider：Messages API 请求构建与六类 SSE 事件解析。
//!
//! 逐字段对照旧 `llm/impl/AnthropicProvider.java`（338 行，只读权威规格）：
//!
//! - 请求（`buildRequestBody` L145-186 / `executeStreamRequest` L189-200）：
//!   `POST {base_url}/v1/messages`，头 `x-api-key` + `anthropic-version:
//!   2023-06-01` + `content-type: application/json`；体字段顺序 `model` /
//!   `max_tokens` / `stream:true` / `system`（独立字段 + `cache_control`
//!   ephemeral，空白不发）/ `messages` / thinking 三分支 / `tools`（非空才发）。
//! - prompt caching 双断点（[`crate::cache`]）：`system` 有分段视图时逐段成块、
//!   `cache_control=true` 的段末尾注入 `{"type":"ephemeral"}`（断点 #1——静态
//!   前缀 / 动态后缀分界）；`tools` 在内建/MCP 分界（最后一个内建工具定义）注入
//!   同一标记（断点 #2，旧 `ToolRegistry.getToolDefinitionsWithCacheBreakpoint`
//!   L131-165）。两断点相互独立，且注入点**只在本 provider**——旧
//!   `AnthropicProvider` 是 `@ConditionalOnProperty(llm.provider=anthropic)` 的
//!   独立类，旧 `OpenAiCompatibleProvider.buildOpenAiRequest` 全程无该字段。
//! - thinking 三分支（旧 `ThinkingConfig` sealed interface）：`Enabled` →
//!   `{type:"enabled", budget_tokens: min(max_tokens - 1, 10000)}`（预算缺省
//!   取旧 `ThinkingConfig.DEFAULT_BUDGET_TOKENS`）；`Adaptive` →
//!   `{type:"adaptive"}`；`Disabled` → 不发 thinking，发 `temperature: 1.0`。
//! - HTTP 错误（L196-199）：`retryable = code == 429 || code == 529 ||
//!   code == 503`——比 `OpenAI` 兼容路径**更窄**（500 / 502 在旧实现里致命）。
//! - SSE（`parseAnthropicSSE` L221-337）：`event: <type>` 行记录待定事件类型，
//!   空行清空，`data: {json}` 行携带载荷。
//!
//! # SSE 事件映射表（旧 `LlmStreamEvent` → [`ProviderEvent`]）
//!
//! | `Anthropic` 事件 | 旧回调事件 | 本实现产出 |
//! |---|---|---|
//! | `message_start` | `MessageStart(id)` + usage 暂存 | 无事件（usage 暂存至 `message_delta`） |
//! | `content_block_start` / `text` | `TextStart(index)` | 无事件（块起始不建模） |
//! | `content_block_start` / `thinking` | `ThinkingStart(index)` | 无事件 |
//! | `content_block_start` / `tool_use` | `ToolUseStart(id, name)` | [`ProviderEvent::ToolUseStart`] |
//! | `content_block_delta` / `text_delta` | `TextDelta(text)` | [`ProviderEvent::TextDelta`] |
//! | `content_block_delta` / `thinking_delta` | `ThinkingDelta(thinking)` | [`ProviderEvent::ThinkingDelta`] |
//! | `content_block_delta` / `input_json_delta` | `ToolInputDelta(null, partial_json)` | [`ProviderEvent::ToolInputDelta`]（id 由块索引补齐） |
//! | `content_block_stop` | `BlockStop(index)` | 无事件（引擎在 Finish 时统一 flush，§3） |
//! | `message_delta`（`stop_reason` 非空） | `MessageDelta(usage, reason)` | [`ProviderEvent::Finish`] |
//! | `message_delta`（`stop_reason` 空） | `MessageDelta(usage, null)` | [`ProviderEvent::UsageUpdate`] |
//! | `message_stop` | 无（caller `onComplete`） | 流终止 |
//! | `event: error` | `throw LlmApiException(msg, retryable, 0)` | [`ProviderEvent::Error`] + 终止 |
//!
//! # 与旧实现的差异（留痕 docs/compatibility.md §5）
//!
//! 1. `ToolInputDelta.id`：旧实现传 `null`（消费侧靠「当前块」隐式关联），本
//!    实现以 `content_block_start` 的 `index` → `tool_use.id` 映射补齐——该
//!    字段在 2.2 冻结面里非 Option，且显式关联杜绝并发块错配。
//! 2. `event: error` 的可重试性：[`ProviderError`] 无「流内自定义 retryable」
//!    变体（2.2 冻结面，不新增变体），故以**合成状态码**复用既有分类：
//!    `overloaded_error` → 529、`rate_limit_error` → 429（`is_retryable()`
//!    为真），其余 → 400（致命）——与旧 `retryable` 判定逐值等价。
//! 3. 块起始 / 块结束事件（`TextStart` / `ThinkingStart` / `BlockStop`）不
//!    建模（2.2 已裁定引擎在 `Finish` 时统一 flush 工具草稿）。
//! 4. `message_stop` 终止流（旧实现空操作、依赖字节源耗尽）——避免 keep-alive
//!    连接上无限等待；语义等价，行为更收敛。
//! 5. 消息转换：旧 `messages` 由上游 `MessageParamConverter` 直接给出
//!    `Anthropic` 形状；本 crate 的 [`ChatRequest`] 是统一形状，故在此转换
//!    （连续 tool 结果合并进**一条** user 消息——`Anthropic` 协议要求同轮
//!    全部 `tool_result` 在一条 user turn 内）。

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use zk_protocol::model::Usage;

use crate::cache::ephemeral_cache_control;
use crate::config::ProviderConfig;
use crate::error::ProviderError;
use crate::openai_compat::{LineSplitter, shared_http_client};
use crate::provider::{ChatProvider, ChatRequest, FinishReason, ProviderEvent, Role, ThinkingMode};
use crate::secret::ApiKey;

/// `Anthropic` API 版本头值（旧 `anthropic-version: 2023-06-01`）。
pub const ANTHROPIC_VERSION: &str = "2023-06-01";

/// extended thinking 缺省预算（旧 `ThinkingConfig.DEFAULT_BUDGET_TOKENS`）。
pub const DEFAULT_THINKING_BUDGET_TOKENS: u32 = 10_000;

/// `Anthropic` 原生 Messages API Provider。
#[derive(Debug)]
pub struct AnthropicProvider {
    client: reqwest::Client,
    config: ProviderConfig,
}

impl AnthropicProvider {
    /// 以配置 + 注入的共享 client 构造。
    #[must_use]
    pub fn with_client(config: ProviderConfig, client: reqwest::Client) -> Self {
        Self { client, config }
    }

    /// 以配置构造（内部建独立 client；注册表场景请用
    /// [`crate::registry::ProviderRegistry::from_configs`] 共享单例）。
    ///
    /// # Errors
    ///
    /// client 构建失败返回 [`ProviderError::Config`]。
    pub fn new(config: ProviderConfig) -> Result<Self, ProviderError> {
        Ok(Self::with_client(config, shared_http_client()?))
    }
}

/// Messages 端点 URL（`base_url` 去尾斜杠 + `/v1/messages`，对齐旧构造器）。
#[must_use]
pub fn messages_url(base_url: &str) -> String {
    format!("{}/v1/messages", base_url.trim_end_matches('/'))
}

/// 构建已签名请求（密钥在此唯一一次进入 `x-api-key` 头，不入日志 / 错误链）。
fn build_signed_request(
    client: &reqwest::Client,
    url: &str,
    key: &ApiKey,
    body: &Value,
) -> Result<reqwest::Request, ProviderError> {
    client
        .post(url)
        .header("x-api-key", key.expose())
        .header("anthropic-version", ANTHROPIC_VERSION)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .json(body)
        .build()
        .map_err(|e| ProviderError::Config {
            message: format!("anthropic request build failed: {e}"),
        })
}

impl ChatProvider for AnthropicProvider {
    fn provider_name(&self) -> &str {
        &self.config.name
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        cancel: CancellationToken,
    ) -> Result<futures::stream::BoxStream<'static, ProviderEvent>, ProviderError> {
        let body = build_request_body(&request);
        let url = messages_url(&self.config.base_url);
        if self.config.api_keys.is_empty() {
            return Err(ProviderError::Config {
                message: format!("provider '{}' has empty api key", self.config.name),
            });
        }
        let keys = self.config.api_keys.clone();
        let provider = self.config.name.clone();
        let client = self.client.clone();

        let byte_source = futures::stream::once(async move {
            let key = keys.next_key().ok_or_else(|| ProviderError::Config {
                message: format!("provider '{provider}' has empty api key"),
            })?;
            let signed = build_signed_request(&client, &url, &key, &body)?;
            let response = client
                .execute(signed)
                .await
                .map_err(|e| ProviderError::Network {
                    message: e.to_string(),
                })?;
            let status = response.status();
            if !status.is_success() {
                if status.as_u16() == 429 {
                    keys.mark_rate_limited(&key);
                }
                let retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
                let body_text = response.text().await.unwrap_or_default();
                return Err(map_anthropic_http(
                    status.as_u16(),
                    &body_text,
                    retry_after.as_deref(),
                ));
            }
            Ok(response)
        })
        .flat_map(
            |result: Result<reqwest::Response, ProviderError>| match result {
                Ok(response) => response
                    .bytes_stream()
                    .map(|r| {
                        r.map_err(|e| ProviderError::Network {
                            message: e.to_string(),
                        })
                    })
                    .left_stream(),
                Err(error) => {
                    futures::stream::once(futures::future::ready(Err(error))).right_stream()
                }
            },
        );

        Ok(Box::pin(anthropic_event_stream(byte_source, cancel)))
    }
}

/// `Anthropic` HTTP 错误映射：可重试域为 429 / 529 / 503（旧 L198 逐值等价）。
///
/// 消息提取沿用 `OpenAI` 兼容路径的 `error.message` 约定（`Anthropic` 错误体
/// 同为 `{"type":"error","error":{"type":..,"message":..}}`）。
fn map_anthropic_http(status: u16, body_text: &str, retry_after: Option<&str>) -> ProviderError {
    let message = serde_json::from_str::<Value>(body_text)
        .ok()
        .and_then(|v| {
            v.get("error")
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| format!("HTTP {status}"));
    let retry_after_ms = if status == 429 {
        retry_after
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .map(|seconds| seconds.saturating_mul(1000))
    } else {
        None
    };
    ProviderError::Http {
        status,
        message,
        retry_after_ms,
        retryable: matches!(status, 429 | 503 | 529),
    }
}

/// 构建 `Anthropic` Messages 请求体（字段顺序对齐旧 `buildRequestBody`）。
fn build_request_body(request: &ChatRequest) -> Value {
    let mut root = serde_json::Map::new();
    root.insert("model".into(), json!(request.model));
    root.insert("max_tokens".into(), json!(request.max_tokens));
    root.insert("stream".into(), json!(true));

    // system 独立字段 + cache_control ephemeral（费用节省 50-90%，旧 L158-163）；
    // 有分段视图时逐段成块并在可缓存段末尾落断点（prompt caching 断点 #1）。
    let system_blocks = build_system_blocks(request);
    if !system_blocks.is_empty() {
        root.insert("system".into(), Value::Array(system_blocks));
    }

    root.insert(
        "messages".into(),
        Value::Array(convert_messages(&request.messages)),
    );

    // thinking 三分支（旧 L168-178）。
    match request.thinking {
        ThinkingMode::Enabled => {
            let budget = request
                .max_tokens
                .saturating_sub(1)
                .min(DEFAULT_THINKING_BUDGET_TOKENS);
            root.insert(
                "thinking".into(),
                json!({ "type": "enabled", "budget_tokens": budget }),
            );
        }
        ThinkingMode::Adaptive => {
            root.insert("thinking".into(), json!({ "type": "adaptive" }));
        }
        // thinking 关闭时才发 temperature（启用时 Anthropic 要求不带该参数）。
        ThinkingMode::Disabled => {
            root.insert("temperature".into(), json!(1.0));
        }
    }

    // 工具定义（`Anthropic` 形状：name / description / input_schema）+ 内建/MCP
    // 分界处的 cache_control 断点（prompt caching 断点 #2，旧 `ToolRegistry`
    // L153-163：仅最后一个内建工具、且存在 MCP 工具时才追加）。
    if !request.tools.is_empty() {
        let tools: Vec<Value> = request
            .tools
            .iter()
            .enumerate()
            .map(|(index, t)| {
                let mut def = serde_json::Map::new();
                def.insert("name".into(), json!(t.name));
                def.insert("description".into(), json!(t.description));
                def.insert("input_schema".into(), t.parameters.clone());
                if request.tool_cache_breakpoint == Some(index) {
                    def.insert("cache_control".into(), ephemeral_cache_control());
                }
                Value::Object(def)
            })
            .collect();
        root.insert("tools".into(), Value::Array(tools));
    }
    Value::Object(root)
}

/// `system` 字段的 content block 数组（空 = 不发 `system` 字段）。
///
/// 分段视图（[`ChatRequest::system_segments`]）非空时逐段成块——
/// `cache_control=true` 的段末尾注入 `{"type":"ephemeral"}`（prompt caching
/// 断点 #1，静态前缀与动态后缀的分界）；空白段跳过（旧
/// `buildSystemPromptWithCacheControl` L306-311 已在装配侧过滤，此处等价兜底）。
/// 无分段视图时回落旧单块行为：整段一块 + `cache_control`（旧 L158-163；键集
/// 恒为 `type` / `text` / `cache_control` 三键——JSON 对象键序无协议语义，本
/// crate 的 `serde_json` 未启用 `preserve_order`，序列化即字典序）。
fn build_system_blocks(request: &ChatRequest) -> Vec<Value> {
    if !request.system_segments.is_empty() {
        return request
            .system_segments
            .iter()
            .filter(|segment| !segment.text.trim().is_empty())
            .map(|segment| {
                let mut block = serde_json::Map::new();
                block.insert("type".into(), json!("text"));
                block.insert("text".into(), json!(segment.text));
                if segment.cache_control {
                    block.insert("cache_control".into(), ephemeral_cache_control());
                }
                Value::Object(block)
            })
            .collect();
    }
    request
        .system_prompt
        .as_deref()
        .filter(|prompt| !prompt.trim().is_empty())
        .map(|prompt| {
            vec![json!({
                "type": "text",
                "text": prompt,
                "cache_control": ephemeral_cache_control(),
            })]
        })
        .unwrap_or_default()
}

/// 统一消息 → `Anthropic` messages 数组。
///
/// 规则（对照旧 `MessageParamConverter` 的 `Anthropic` 输出形状）：
/// - `System` 角色消息合并为 user 文本（system prompt 走独立字段，历史里的
///   system 消息不能出现在 `messages` 内——`Anthropic` 协议限制）；
/// - `User` → `{role:"user", content:"..."}`；
/// - `Assistant` 无工具调用 → `{role:"assistant", content:"..."}`；
/// - `Assistant` 携带 `tool_calls` → content 数组 `[text?, tool_use...]`
///   （`input` 为解析后的对象，解析失败退化为 `{}`）；
/// - 连续 `Tool` 结果 → **合并**为一条 `{role:"user", content:[tool_result...]}`。
fn convert_messages(messages: &[crate::provider::ChatMessage]) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::with_capacity(messages.len());
    let mut tool_results: Vec<Value> = Vec::new();
    for message in messages {
        if message.role == Role::Tool {
            tool_results.push(json!({
                "type": "tool_result",
                "tool_use_id": message.tool_call_id.clone().unwrap_or_default(),
                "content": message.content,
            }));
            continue;
        }
        // 非 tool 消息前先冲刷已积累的 tool_result（合并成一条 user turn）。
        flush_tool_results(&mut tool_results, &mut out);
        match message.role {
            Role::Assistant if !message.tool_calls.is_empty() => {
                let mut blocks: Vec<Value> = Vec::with_capacity(message.tool_calls.len() + 1);
                if !message.content.is_empty() {
                    blocks.push(json!({ "type": "text", "text": message.content }));
                }
                for call in &message.tool_calls {
                    let input = serde_json::from_str::<Value>(&call.arguments)
                        .ok()
                        .filter(Value::is_object)
                        .unwrap_or_else(|| json!({}));
                    blocks.push(json!({
                        "type": "tool_use",
                        "id": call.id,
                        "name": call.name,
                        "input": input,
                    }));
                }
                out.push(json!({ "role": "assistant", "content": blocks }));
            }
            Role::Assistant => {
                out.push(json!({ "role": "assistant", "content": message.content }));
            }
            Role::User if !message.images.is_empty() => {
                let mut blocks = Vec::with_capacity(message.images.len() + 1);
                if !message.content.is_empty() {
                    blocks.push(json!({"type":"text", "text":message.content}));
                }
                blocks.extend(message.images.iter().filter_map(|image| {
                    // URL 图片直发 Anthropic `url` source；否则 base64 source；
                    // 两者皆无跳过（与 OpenAI 侧 `appendImageUrlPart` 同语义）。
                    if let Some(url) = image.url.as_deref().filter(|url| !url.trim().is_empty()) {
                        return Some(json!({
                            "type":"image",
                            "source": {"type":"url", "url": url}
                        }));
                    }
                    image.data.as_deref().map(|data| {
                        json!({
                            "type":"image",
                            "source": {"type":"base64", "media_type":image.media_type, "data":data}
                        })
                    })
                }));
                out.push(json!({"role":"user", "content":blocks}));
            }
            // system 历史与 user 同列（协议不允许 messages 内 system 角色）。
            Role::System | Role::User | Role::Tool => {
                out.push(json!({ "role": "user", "content": message.content }));
            }
        }
    }
    flush_tool_results(&mut tool_results, &mut out);
    out
}

/// 冲刷积累的 `tool_result` 块为一条 user 消息（空则无操作）。
fn flush_tool_results(tool_results: &mut Vec<Value>, out: &mut Vec<Value>) {
    if tool_results.is_empty() {
        return;
    }
    out.push(json!({ "role": "user", "content": std::mem::take(tool_results) }));
}

/// `Anthropic` `stop_reason` → [`FinishReason`]。
///
/// `Anthropic` 的取值域（`end_turn` / `max_tokens` / `tool_use` /
/// `stop_sequence`）本就是旧实现的**内部统一名**，故直接对位映射（不能复用
/// [`FinishReason::from_openai`]——后者会把 `end_turn` 落到 `Other`）。
fn finish_reason_from_anthropic(raw: &str) -> FinishReason {
    match raw {
        "end_turn" => FinishReason::EndTurn,
        "max_tokens" => FinishReason::MaxTokens,
        "tool_use" => FinishReason::ToolUse,
        other => FinishReason::Other(other.to_owned()),
    }
}

/// 将 `Anthropic` 原生 SSE 字节流转换为统一事件流（惰性求值）。
///
/// 终止语义：
/// - `message_stop` → 终止（积压事件先吐完，见差异 §4）；
/// - 字节源耗尽 → 正常终止；
/// - 字节源 `Err`（网络 / HTTP 状态）→ 产出 [`ProviderEvent::Error`] 后终止；
/// - `event: error` → 产出 [`ProviderEvent::Error`]（合成状态码）后终止
///   （对齐旧 `throw LlmApiException` 的中断语义）；
/// - `data:` 载荷 JSON 解析失败 → 产出 [`ProviderEvent::Error`] 后终止
///   （旧 `readTree` 抛 `IOException` 走 `onError`，与 `OpenAI` 兼容路径的
///   宽容策略不同——此处对齐 `Anthropic` 侧旧行为）；
/// - `cancel` 触发 → 静默终止（不产出任何事件，D-S6-5）。
pub fn anthropic_event_stream<S>(
    byte_source: S,
    cancel: CancellationToken,
) -> impl Stream<Item = ProviderEvent> + Send + 'static
where
    S: Stream<Item = Result<Bytes, ProviderError>> + Send + 'static,
{
    struct AnthropicState<S> {
        source: Pin<Box<S>>,
        splitter: LineSplitter,
        pending: VecDeque<ProviderEvent>,
        parser: SseParser,
        cancel: CancellationToken,
        done: bool,
    }
    futures::stream::unfold(
        AnthropicState {
            source: Box::pin(byte_source),
            splitter: LineSplitter::default(),
            pending: VecDeque::new(),
            parser: SseParser::default(),
            cancel,
            done: false,
        },
        |mut st| async move {
            loop {
                if st.cancel.is_cancelled() {
                    st.done = true;
                    st.pending.clear();
                    return None;
                }
                if let Some(event) = st.pending.pop_front() {
                    return Some((event, st));
                }
                if st.done {
                    return None;
                }
                match st.source.as_mut().next().await {
                    None => {
                        if let Some(line) = st.splitter.flush() {
                            st.parser.feed_line(&line, &mut st.pending, &mut st.done);
                        }
                        st.done = true;
                    }
                    Some(Ok(bytes)) => {
                        for line in st.splitter.feed(&bytes) {
                            st.parser.feed_line(&line, &mut st.pending, &mut st.done);
                            if st.done {
                                break;
                            }
                        }
                    }
                    Some(Err(error)) => {
                        st.pending.push_back(ProviderEvent::Error { error });
                        st.done = true;
                    }
                }
            }
        },
    )
}

/// `Anthropic` SSE 逐行状态机（对照旧 `parseAnthropicSSE` 的局部变量集）。
///
/// 持有：待定事件类型（`event:` 行）、块索引 → `tool_use` id 映射（补齐
/// `ToolInputDelta.id`，见差异 §1）、usage 四计数（`message_start` 暂存的
/// input / cache 读写 + `message_delta` 的 output）。
#[derive(Debug, Default)]
struct SseParser {
    pending_event_type: Option<String>,
    tool_ids: BTreeMap<u64, String>,
    input_tokens: i64,
    output_tokens: i64,
    cache_read_input_tokens: i64,
    cache_creation_input_tokens: i64,
}

impl SseParser {
    /// 处理一行 SSE 文本。
    fn feed_line(&mut self, line: &str, pending: &mut VecDeque<ProviderEvent>, done: &mut bool) {
        // 空行：一个 SSE 事件结束，清空待定类型（旧 L233-236）。
        if line.is_empty() {
            self.pending_event_type = None;
            return;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            self.pending_event_type = Some(rest.trim().to_owned());
            return;
        }
        let Some(data) = line.strip_prefix("data:") else {
            return;
        };
        let data = data.strip_prefix(' ').unwrap_or(data);
        // error 事件：合成状态码复用可重试分类后终止（旧 L247-256）。
        if self.pending_event_type.as_deref() == Some("error") {
            pending.push_back(ProviderEvent::Error {
                error: parse_error_event(data),
            });
            *done = true;
            return;
        }
        let event: Value = match serde_json::from_str(data) {
            Ok(value) => value,
            Err(err) => {
                pending.push_back(ProviderEvent::Error {
                    error: ProviderError::Parse {
                        message: format!("anthropic sse chunk parse failed: {err}"),
                    },
                });
                *done = true;
                return;
            }
        };
        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| self.pending_event_type.clone())
            .unwrap_or_else(|| "unknown".to_owned());
        match event_type.as_str() {
            "message_start" => self.on_message_start(&event),
            "content_block_start" => self.on_block_start(&event, pending),
            "content_block_delta" => self.on_block_delta(&event, pending, done),
            "message_delta" => self.on_message_delta(&event, pending),
            // content_block_stop / message_stop / 未知事件：无事件产出。
            "message_stop" => *done = true,
            _ => {}
        }
    }

    /// `message_start`：暂存输入侧 usage（旧 L262-271）。
    fn on_message_start(&mut self, event: &Value) {
        let usage = event.pointer("/message/usage");
        if let Some(usage) = usage {
            self.input_tokens = json_i64(usage, "input_tokens");
            self.cache_read_input_tokens = json_i64(usage, "cache_read_input_tokens");
            self.cache_creation_input_tokens = json_i64(usage, "cache_creation_input_tokens");
        }
    }

    /// `content_block_start`：仅 `tool_use` 块产出事件并登记 id（旧 L272-285）。
    fn on_block_start(&mut self, event: &Value, pending: &mut VecDeque<ProviderEvent>) {
        let index = event.get("index").and_then(Value::as_u64).unwrap_or(0);
        let Some(block) = event.get("content_block") else {
            return;
        };
        if block.get("type").and_then(Value::as_str) != Some("tool_use") {
            return;
        }
        let id = block
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let name = block
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        self.tool_ids.insert(index, id.clone());
        pending.push_back(ProviderEvent::ToolUseStart { id, name });
    }

    /// `content_block_delta`：三类 delta 分派（旧 L286-300）。
    fn on_block_delta(
        &mut self,
        event: &Value,
        pending: &mut VecDeque<ProviderEvent>,
        done: &mut bool,
    ) {
        let index = event.get("index").and_then(Value::as_u64).unwrap_or(0);
        let Some(delta) = event.get("delta") else {
            return;
        };
        match delta.get("type").and_then(Value::as_str) {
            Some("text_delta") => pending.push_back(ProviderEvent::TextDelta {
                text: json_str(delta, "text"),
            }),
            Some("thinking_delta") => pending.push_back(ProviderEvent::ThinkingDelta {
                thinking: json_str(delta, "thinking"),
            }),
            Some("input_json_delta") => {
                // id 由块索引补齐（差异 §1）；无对应 tool_use 块 = 流身份缺失，
                // 与 OpenAI 兼容路径的 INVALID_TOOL_CALL_STREAM 同为致命。
                let Some(id) = self.tool_ids.get(&index).cloned() else {
                    pending.push_back(ProviderEvent::Error {
                        error: ProviderError::Parse {
                            message: format!(
                                "INVALID_TOOL_CALL_STREAM: input_json_delta for unknown block {index}"
                            ),
                        },
                    });
                    *done = true;
                    return;
                };
                pending.push_back(ProviderEvent::ToolInputDelta {
                    id,
                    delta: json_str(delta, "partial_json"),
                });
            }
            _ => {}
        }
    }

    /// `message_delta`：`stop_reason` 有值 → Finish，否则 usage-only（旧 L305-318）。
    fn on_message_delta(&mut self, event: &Value, pending: &mut VecDeque<ProviderEvent>) {
        if let Some(usage) = event.get("usage") {
            self.output_tokens = json_i64(usage, "output_tokens");
        }
        let usage = Usage {
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_read_input_tokens: self.cache_read_input_tokens,
            cache_creation_input_tokens: self.cache_creation_input_tokens,
        };
        let stop_reason = event
            .pointer("/delta/stop_reason")
            .filter(|value| !value.is_null())
            .and_then(Value::as_str);
        match stop_reason {
            Some(raw) => pending.push_back(ProviderEvent::Finish {
                finish_reason: finish_reason_from_anthropic(raw),
                usage: Some(usage),
            }),
            None => pending.push_back(ProviderEvent::UsageUpdate { usage }),
        }
    }
}

/// `event: error` 载荷 → [`ProviderError`]（合成状态码，见差异 §2）。
fn parse_error_event(data: &str) -> ProviderError {
    let parsed: Value = serde_json::from_str(data).unwrap_or(Value::Null);
    let error_type = parsed
        .pointer("/error/type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let message = parsed
        .pointer("/error/message")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let status = match error_type {
        "overloaded_error" => 529,
        "rate_limit_error" => 429,
        _ => 400,
    };
    ProviderError::Http {
        status,
        message,
        retry_after_ms: None,
        retryable: matches!(status, 429 | 529),
    }
}

/// 取整数字段（缺失 / 类型不符为 0，对齐 `asInt(0)`）。
fn json_i64(node: &Value, key: &str) -> i64 {
    node.get(key).and_then(Value::as_i64).unwrap_or(0)
}

/// 取字符串字段（缺失为空串，对齐 `asText()`）。
fn json_str(node: &Value, key: &str) -> String {
    node.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{SystemPrompt, SystemPromptSegment, ToolOrigin};
    use crate::provider::{ChatMessage, ToolCallRequest, ToolSpec};
    use crate::secret::ApiKey;

    /// 最小工具定义（断点测试只关心定义序与标记落点）。
    fn tool_spec(name: &str) -> ToolSpec {
        ToolSpec {
            name: name.to_owned(),
            description: format!("{name} description"),
            parameters: json!({ "type": "object" }),
        }
    }

    fn config() -> ProviderConfig {
        ProviderConfig::new(
            "anthropic",
            "https://api.anthropic.com/",
            ApiKey::new("sk-ant-secret"),
            "claude-sonnet-4-6",
            vec!["claude-sonnet-4-6".into()],
        )
        .with_protocol(crate::config::ProviderProtocol::AnthropicNative)
    }

    /// 逐行喂入 fixture（模拟真实 SSE：`event:` + `data:` 两行 + 空行）。
    fn parse_lines(fixture: &str) -> (Vec<ProviderEvent>, bool) {
        let mut parser = SseParser::default();
        let mut pending = VecDeque::new();
        let mut done = false;
        for line in fixture.split('\n') {
            parser.feed_line(line, &mut pending, &mut done);
            if done {
                break;
            }
        }
        (pending.into_iter().collect(), done)
    }

    #[test]
    fn messages_url_trims_trailing_slash() {
        assert_eq!(
            messages_url("https://api.anthropic.com/"),
            "https://api.anthropic.com/v1/messages"
        );
        assert_eq!(
            messages_url("https://gw.internal"),
            "https://gw.internal/v1/messages"
        );
    }

    /// 认证头形态：`x-api-key` + `anthropic-version`（不是 `Bearer`）。
    #[test]
    fn signed_request_carries_anthropic_headers() {
        let client = shared_http_client().unwrap();
        let key = ApiKey::new("sk-ant-secret");
        let request = build_signed_request(
            &client,
            &messages_url("https://api.anthropic.com"),
            &key,
            &json!({"model": "claude-sonnet-4-6"}),
        )
        .expect("request builds");
        assert_eq!(
            request.url().as_str(),
            "https://api.anthropic.com/v1/messages"
        );
        assert_eq!(
            request.headers().get("anthropic-version").unwrap(),
            ANTHROPIC_VERSION
        );
        assert_eq!(request.headers().get("x-api-key").unwrap(), "sk-ant-secret");
        assert!(
            request
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .is_none(),
            "anthropic native uses x-api-key, never Bearer"
        );
    }

    #[test]
    fn request_body_matches_legacy_shape() {
        let request = ChatRequest::new("claude-sonnet-4-6")
            .with_system_prompt(Some("be terse".into()))
            .with_message(ChatMessage::user("hello"))
            .with_max_tokens(4096);
        let body = build_request_body(&request);
        assert_eq!(body["model"], "claude-sonnet-4-6");
        assert_eq!(body["max_tokens"], 4096);
        assert_eq!(body["stream"], true);
        // system 独立字段 + cache_control ephemeral。
        assert_eq!(body["system"][0]["type"], "text");
        assert_eq!(body["system"][0]["text"], "be terse");
        assert_eq!(body["system"][0]["cache_control"]["type"], "ephemeral");
        assert_eq!(body["messages"][0]["role"], "user");
        assert_eq!(body["messages"][0]["content"], "hello");
        // thinking 关闭 → temperature 1.0，且不带 thinking。
        assert_eq!(body["temperature"], 1.0);
        assert!(body.get("thinking").is_none());
        assert!(body.get("tools").is_none(), "empty tools are not sent");
    }

    #[test]
    fn thinking_modes_map_to_legacy_branches() {
        let enabled = build_request_body(
            &ChatRequest::new("claude-sonnet-4-6")
                .with_max_tokens(8192)
                .with_thinking(ThinkingMode::Enabled),
        );
        assert_eq!(enabled["thinking"]["type"], "enabled");
        // budget = min(max_tokens - 1, 10000)。
        assert_eq!(enabled["thinking"]["budget_tokens"], 8191);
        assert!(enabled.get("temperature").is_none());

        let big = build_request_body(
            &ChatRequest::new("claude-sonnet-4-6")
                .with_max_tokens(64_000)
                .with_thinking(ThinkingMode::Enabled),
        );
        assert_eq!(big["thinking"]["budget_tokens"], 10_000);

        let adaptive = build_request_body(
            &ChatRequest::new("claude-sonnet-4-6").with_thinking(ThinkingMode::Adaptive),
        );
        assert_eq!(adaptive["thinking"]["type"], "adaptive");
        assert!(adaptive.get("temperature").is_none());
    }

    #[test]
    fn tools_use_input_schema_key() {
        let body =
            build_request_body(
                &ChatRequest::new("claude-sonnet-4-6").with_tools(vec![ToolSpec {
                    name: "read_file".into(),
                    description: "read a file".into(),
                    parameters: json!({"type": "object"}),
                }]),
            );
        assert_eq!(body["tools"][0]["name"], "read_file");
        assert_eq!(body["tools"][0]["description"], "read a file");
        assert_eq!(body["tools"][0]["input_schema"]["type"], "object");
        assert!(
            body["tools"][0].get("parameters").is_none(),
            "anthropic uses input_schema, not parameters"
        );
    }

    /// 断点 #1：分段视图逐段成块，`cache_control` 只落在静态前缀段末尾。
    #[test]
    fn system_segments_place_cache_control_at_static_boundary() {
        let request = ChatRequest::new("claude-sonnet-4-6")
            .with_system(SystemPrompt::segmented("static core", "session state"));
        let body = build_request_body(&request);
        let blocks = body["system"].as_array().expect("system is a block array");
        assert_eq!(blocks.len(), 2, "静态前缀 + 动态后缀各一块");
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[0]["text"], "static core");
        assert_eq!(blocks[0]["cache_control"]["type"], "ephemeral");
        assert_eq!(blocks[1]["type"], "text");
        assert_eq!(blocks[1]["text"], "session state");
        assert!(
            blocks[1].get("cache_control").is_none(),
            "动态后缀不得打断点"
        );
    }

    /// 无分段视图 → 逐字段等价旧单块行为（整段一块 + `cache_control` 三键）。
    #[test]
    fn plain_system_prompt_keeps_legacy_single_block() {
        let body = build_request_body(
            &ChatRequest::new("claude-sonnet-4-6").with_system_prompt(Some("be terse".into())),
        );
        let blocks = body["system"].as_array().expect("system is a block array");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["cache_control"]["type"], "ephemeral");
        // 键集恒为三键（键序无协议语义，见 build_system_blocks 文档）。
        let block = blocks[0].as_object().expect("block is an object");
        assert_eq!(block.len(), 3);
        assert_eq!(block["type"], "text");
        assert_eq!(block["text"], "be terse");
    }

    /// 空白段不成块；全空白分段 → 不发 `system` 字段（旧 isBlank 判定）。
    #[test]
    fn blank_system_segments_are_dropped() {
        let body = build_request_body(&ChatRequest::new("claude-sonnet-4-6").with_system(
            SystemPrompt::new(vec![
                SystemPromptSegment::cached("static core"),
                SystemPromptSegment::dynamic("  \n "),
            ]),
        ));
        let blocks = body["system"].as_array().expect("system is a block array");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["cache_control"]["type"], "ephemeral");

        let empty = build_request_body(
            &ChatRequest::new("claude-sonnet-4-6")
                .with_system(SystemPrompt::new(vec![SystemPromptSegment::dynamic("   ")])),
        );
        assert!(empty.get("system").is_none(), "全空白分段不发 system 字段");
    }

    /// 断点 #2：`cache_control` 落在最后一个内建工具定义上（存在 MCP 工具时）。
    #[test]
    fn tool_cache_breakpoint_marks_last_builtin_definition() {
        let request = ChatRequest::new("claude-sonnet-4-6")
            .with_tools(vec![
                tool_spec("read_file"),
                tool_spec("write_file"),
                tool_spec("mcp__srv__query"),
            ])
            .with_tool_origins(&[ToolOrigin::BuiltIn, ToolOrigin::BuiltIn, ToolOrigin::Mcp]);
        let body = build_request_body(&request);
        let tools = body["tools"].as_array().expect("tools array");
        assert_eq!(tools.len(), 3);
        assert!(tools[0].get("cache_control").is_none());
        assert_eq!(tools[1]["cache_control"]["type"], "ephemeral");
        assert!(
            tools[2].get("cache_control").is_none(),
            "MCP 工具位于断点之后"
        );
        // 断点只**追加**键，旧 toToolDefinition 的三键原样保留。
        let marked = tools[1].as_object().expect("definition is an object");
        assert_eq!(marked.len(), 4);
        assert_eq!(marked["name"], "write_file");
        assert_eq!(marked["description"], "write_file description");
        assert_eq!(marked["input_schema"]["type"], "object");

        // 无 MCP 工具 → 全程无断点（旧 hasMcpTools 条件）。
        let built_in_only = build_request_body(
            &ChatRequest::new("claude-sonnet-4-6")
                .with_tools(vec![tool_spec("read_file")])
                .with_tool_origins(&[ToolOrigin::BuiltIn]),
        );
        assert!(built_in_only["tools"][0].get("cache_control").is_none());

        // 越界下标不注入（防御：折算后工具集被裁剪的场景）。
        let out_of_range = build_request_body(
            &ChatRequest::new("claude-sonnet-4-6")
                .with_tools(vec![tool_spec("read_file")])
                .with_tool_cache_breakpoint(Some(7)),
        );
        assert!(out_of_range["tools"][0].get("cache_control").is_none());
    }

    /// 双断点相互独立：任一侧开启不影响另一侧。
    #[test]
    fn cache_breakpoints_are_independently_controllable() {
        // 仅系统提示断点（工具无标记）。
        let system_only = build_request_body(
            &ChatRequest::new("claude-sonnet-4-6")
                .with_system(SystemPrompt::segmented("static core", "session state"))
                .with_tools(vec![tool_spec("read_file")]),
        );
        assert_eq!(
            system_only["system"][0]["cache_control"]["type"],
            "ephemeral"
        );
        assert!(system_only["tools"][0].get("cache_control").is_none());

        // 仅工具断点（系统提示走单块旧行为）。
        let tools_only = build_request_body(
            &ChatRequest::new("claude-sonnet-4-6")
                .with_system_prompt(Some("be terse".into()))
                .with_tools(vec![tool_spec("read_file"), tool_spec("mcp__srv__query")])
                .with_tool_origins(&[ToolOrigin::BuiltIn, ToolOrigin::Mcp]),
        );
        assert_eq!(tools_only["system"].as_array().expect("system").len(), 1);
        assert_eq!(tools_only["tools"][0]["cache_control"]["type"], "ephemeral");
        assert!(tools_only["tools"][1].get("cache_control").is_none());

        // 双断点齐发。
        let both = build_request_body(
            &ChatRequest::new("claude-sonnet-4-6")
                .with_system(SystemPrompt::segmented("static core", "session state"))
                .with_tools(vec![tool_spec("read_file"), tool_spec("mcp__srv__query")])
                .with_tool_origins(&[ToolOrigin::BuiltIn, ToolOrigin::Mcp]),
        );
        assert_eq!(both["system"][0]["cache_control"]["type"], "ephemeral");
        assert!(both["system"][1].get("cache_control").is_none());
        assert_eq!(both["tools"][0]["cache_control"]["type"], "ephemeral");
        assert!(both["tools"][1].get("cache_control").is_none());

        // 皆不开启：单块 system（旧行为）+ 工具无标记。
        let neither = build_request_body(
            &ChatRequest::new("claude-sonnet-4-6")
                .with_system_prompt(Some("be terse".into()))
                .with_tools(vec![tool_spec("read_file")]),
        );
        assert_eq!(neither["system"].as_array().expect("system").len(), 1);
        assert!(neither["tools"][0].get("cache_control").is_none());
    }

    /// 多轮工具历史：assistant `tool_use` 块 + 连续 `tool_result` 合并为一条 user。
    #[test]
    fn tool_history_converts_to_anthropic_blocks() {
        let request = ChatRequest::new("claude-sonnet-4-6")
            .with_message(ChatMessage::user("list files"))
            .with_message(ChatMessage::assistant_tool_calls(
                "working",
                vec![
                    ToolCallRequest {
                        id: "call_1".into(),
                        name: "ls".into(),
                        arguments: r#"{"path":"."}"#.into(),
                    },
                    ToolCallRequest {
                        id: "call_2".into(),
                        name: "pwd".into(),
                        arguments: "not-json".into(),
                    },
                ],
            ))
            .with_message(ChatMessage::tool("call_1", "a.txt"))
            .with_message(ChatMessage::tool("call_2", "/tmp"));
        let body = build_request_body(&request);
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 3, "user / assistant / merged tool results");
        assert_eq!(messages[1]["role"], "assistant");
        assert_eq!(messages[1]["content"][0]["type"], "text");
        assert_eq!(messages[1]["content"][1]["type"], "tool_use");
        assert_eq!(messages[1]["content"][1]["id"], "call_1");
        assert_eq!(messages[1]["content"][1]["input"]["path"], ".");
        // 非法 arguments 退化为空对象（不丢弃该 tool_use 块）。
        assert_eq!(messages[1]["content"][2]["input"], json!({}));
        // 连续 tool 结果合并成一条 user turn（Anthropic 协议要求）。
        assert_eq!(messages[2]["role"], "user");
        let results = messages[2]["content"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["type"], "tool_result");
        assert_eq!(results[0]["tool_use_id"], "call_1");
        assert_eq!(results[1]["tool_use_id"], "call_2");
    }

    /// 判据主测：mock SSE 流覆盖 thinking + text + `tool_use` 三种 block 类型。
    #[test]
    fn sse_stream_covers_thinking_text_and_tool_use_blocks() {
        let fixture = concat!(
            "event: message_start\n",
            r#"data: {"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":42,"cache_read_input_tokens":7,"cache_creation_input_tokens":3}}}"#,
            "\n\n",
            "event: content_block_start\n",
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking"}}"#,
            "\n\n",
            "event: content_block_delta\n",
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"weighing"}}"#,
            "\n\n",
            "event: content_block_stop\n",
            r#"data: {"type":"content_block_stop","index":0}"#,
            "\n\n",
            "event: content_block_start\n",
            r#"data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}"#,
            "\n\n",
            "event: content_block_delta\n",
            r#"data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Hello"}}"#,
            "\n\n",
            "event: content_block_start\n",
            r#"data: {"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_9","name":"read_file"}}"#,
            "\n\n",
            "event: content_block_delta\n",
            r#"data: {"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"path\""}}"#,
            "\n\n",
            "event: content_block_delta\n",
            r#"data: {"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":":\"a.txt\"}"}}"#,
            "\n\n",
            "event: message_delta\n",
            r#"data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":25}}"#,
            "\n\n",
            "event: message_stop\n",
            r#"data: {"type":"message_stop"}"#,
            "\n\n",
        );
        let (events, done) = parse_lines(fixture);
        assert!(done, "message_stop terminates the stream");
        assert_eq!(
            events,
            vec![
                ProviderEvent::ThinkingDelta {
                    thinking: "weighing".into()
                },
                ProviderEvent::TextDelta {
                    text: "Hello".into()
                },
                ProviderEvent::ToolUseStart {
                    id: "toolu_9".into(),
                    name: "read_file".into()
                },
                ProviderEvent::ToolInputDelta {
                    id: "toolu_9".into(),
                    delta: "{\"path\"".into()
                },
                ProviderEvent::ToolInputDelta {
                    id: "toolu_9".into(),
                    delta: ":\"a.txt\"}".into()
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::ToolUse,
                    usage: Some(Usage {
                        input_tokens: 42,
                        output_tokens: 25,
                        cache_read_input_tokens: 7,
                        cache_creation_input_tokens: 3,
                    }),
                },
            ]
        );
    }

    /// `message_delta` 无 `stop_reason` → usage-only（旧 `MessageDelta(usage, null)`）。
    #[test]
    fn message_delta_without_stop_reason_is_usage_only() {
        let fixture = concat!(
            "event: message_delta\n",
            r#"data: {"type":"message_delta","delta":{"stop_reason":null},"usage":{"output_tokens":11}}"#,
            "\n\n",
        );
        let (events, done) = parse_lines(fixture);
        assert!(!done);
        assert_eq!(
            events,
            vec![ProviderEvent::UsageUpdate {
                usage: Usage {
                    input_tokens: 0,
                    output_tokens: 11,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                }
            }]
        );
    }

    /// `stop_reason` 全域映射（`Anthropic` 值即内部统一名）。
    #[test]
    fn stop_reason_maps_to_unified_domain() {
        for (raw, expected) in [
            ("end_turn", FinishReason::EndTurn),
            ("max_tokens", FinishReason::MaxTokens),
            ("tool_use", FinishReason::ToolUse),
            ("stop_sequence", FinishReason::Other("stop_sequence".into())),
        ] {
            assert_eq!(finish_reason_from_anthropic(raw), expected);
        }
    }

    /// `event: error` → 合成状态码 + 可重试分类（旧 `retryable` 判定逐值等价）。
    #[test]
    fn error_event_maps_retryable_types_and_terminates() {
        for (error_type, status, retryable) in [
            ("overloaded_error", 529_u16, true),
            ("rate_limit_error", 429, true),
            ("invalid_request_error", 400, false),
        ] {
            let fixture = format!(
                "event: error\ndata: {{\"type\":\"error\",\"error\":{{\"type\":\"{error_type}\",\"message\":\"boom\"}}}}\n\n"
            );
            let (events, done) = parse_lines(&fixture);
            assert!(done, "{error_type} must terminate the stream");
            match events.as_slice() {
                [ProviderEvent::Error { error }] => {
                    assert_eq!(
                        *error,
                        ProviderError::Http {
                            status,
                            message: "boom".into(),
                            retry_after_ms: None,
                            retryable,
                        }
                    );
                    assert_eq!(error.is_retryable(), retryable);
                }
                other => panic!("expected single error event, got {other:?}"),
            }
        }
    }

    /// 载荷 JSON 非法 → Error 后终止（对齐旧 `readTree` 抛 `IOException` 路径）。
    #[test]
    fn malformed_data_payload_terminates_with_parse_error() {
        let (events, done) = parse_lines("event: content_block_delta\ndata: {not json\n\n");
        assert!(done);
        assert!(matches!(
            events.as_slice(),
            [ProviderEvent::Error {
                error: ProviderError::Parse { .. }
            }]
        ));
    }

    /// `input_json_delta` 找不到 `tool_use` 块 → 身份缺失致命终止。
    #[test]
    fn input_json_delta_without_tool_block_is_fatal() {
        let fixture = concat!(
            "event: content_block_delta\n",
            r#"data: {"type":"content_block_delta","index":3,"delta":{"type":"input_json_delta","partial_json":"{}"}}"#,
            "\n\n",
        );
        let (events, done) = parse_lines(fixture);
        assert!(done);
        match events.as_slice() {
            [
                ProviderEvent::Error {
                    error: ProviderError::Parse { message },
                },
            ] => assert!(message.contains("INVALID_TOOL_CALL_STREAM")),
            other => panic!("expected fatal parse error, got {other:?}"),
        }
    }

    /// HTTP 错误可重试域：429 / 503 / 529 可重试，500 / 502 / 4xx 致命（旧 L198）。
    #[test]
    fn http_error_retryable_domain_is_narrower_than_openai() {
        for status in [429_u16, 503, 529] {
            assert!(
                map_anthropic_http(status, "", None).is_retryable(),
                "status {status} must be retryable"
            );
        }
        for status in [400_u16, 401, 404, 500, 502] {
            assert!(
                !map_anthropic_http(status, "", None).is_retryable(),
                "status {status} must be fatal"
            );
        }
        // 消息取响应体 error.message；429 的 Retry-After 数字秒转毫秒。
        let err = map_anthropic_http(
            429,
            r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#,
            Some("2"),
        );
        assert_eq!(
            err,
            ProviderError::Http {
                status: 429,
                message: "slow down".into(),
                retry_after_ms: Some(2000),
                retryable: true,
            }
        );
    }

    /// 未配密钥 → 建立期 `Config` 错误（不发网络）；provider 名取配置名。
    #[test]
    fn missing_api_key_fails_at_setup() {
        let provider = AnthropicProvider::new(config()).unwrap();
        assert_eq!(provider.provider_name(), "anthropic");
        let keyless = ProviderConfig::new(
            "anthropic",
            "https://api.anthropic.com",
            ApiKey::new(""),
            "claude-sonnet-4-6",
            vec![],
        );
        let provider = AnthropicProvider::new(keyless).unwrap();
        let outcome = provider.chat_stream(
            ChatRequest::new("claude-sonnet-4-6"),
            CancellationToken::new(),
        );
        let Err(error) = outcome else {
            panic!("keyless provider must fail at setup");
        };
        assert!(matches!(error, ProviderError::Config { .. }));
    }

    /// 密钥不进入 Debug / 错误消息（脱敏不变量）。
    #[test]
    fn provider_debug_never_leaks_api_key() {
        let provider = AnthropicProvider::new(config()).unwrap();
        assert!(!format!("{provider:?}").contains("sk-ant-secret"));
    }

    /// 端到端流装配：mock 字节源 → 事件序列（惰性 + 取消静默终止）。
    #[tokio::test]
    async fn event_stream_consumes_chunked_bytes_and_respects_cancel() {
        let chunks = vec![
            Ok(Bytes::from_static(b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\n")),
            Ok(Bytes::from_static(b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n")),
            Ok(Bytes::from_static(b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n")),
        ];
        let source = futures::stream::iter(chunks);
        let events: Vec<ProviderEvent> = anthropic_event_stream(source, CancellationToken::new())
            .collect()
            .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta { text: "hi".into() },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(Usage {
                        input_tokens: 0,
                        output_tokens: 2,
                        cache_read_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                    }),
                },
            ]
        );

        // 已取消 token：静默终止，绝不产出 Finish。
        let cancel = CancellationToken::new();
        cancel.cancel();
        let source = futures::stream::iter(vec![Ok(Bytes::from_static(
            b"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
        ))]);
        let events: Vec<ProviderEvent> = anthropic_event_stream(source, cancel).collect().await;
        assert!(events.is_empty(), "cancelled stream must be silent");
    }

    #[test]
    fn anthropic_messages_serialize_validated_user_images_as_base64_blocks() {
        let messages = convert_messages(&[ChatMessage::user_with_images(
            "inspect",
            vec![crate::ImageSource {
                media_type: "image/png".into(),
                data: Some("aGVsbG8=".into()),
                url: None,
            }],
        )]);
        assert_eq!(messages[0]["content"][0]["type"], "text");
        assert_eq!(messages[0]["content"][1]["type"], "image");
        assert_eq!(messages[0]["content"][1]["source"]["type"], "base64");
        assert_eq!(
            messages[0]["content"][1]["source"]["media_type"],
            "image/png"
        );
    }

    #[test]
    fn anthropic_messages_serialize_url_images_as_url_source() {
        let messages = convert_messages(&[ChatMessage::user_with_images(
            "inspect",
            vec![crate::ImageSource {
                media_type: "image/png".into(),
                data: None,
                url: Some(
                    "https://bkt.oss.example.com/zhikuncode-artifacts/clipboard/a.png".into(),
                ),
            }],
        )]);
        assert_eq!(messages[0]["content"][1]["type"], "image");
        assert_eq!(messages[0]["content"][1]["source"]["type"], "url");
        assert_eq!(
            messages[0]["content"][1]["source"]["url"],
            "https://bkt.oss.example.com/zhikuncode-artifacts/clipboard/a.png"
        );
        assert!(messages[0]["content"][1]["source"].get("data").is_none());
    }
}
