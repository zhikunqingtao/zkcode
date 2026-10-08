//! `OpenAI` 兼容 Provider：请求构建、SSE 增量解析与流式事件组装。
//!
//! 逐字段对照旧 `OpenAiCompatibleProvider.java`（788 行，只读权威规格）：
//! - 请求构建（L245-498 `buildOpenAiRequest` / `buildBaseRequest`）：
//!   `model`、`max_tokens`（kimi- 系列换名 `max_completion_tokens`）、
//!   `messages`（system 前置；tool 结果 → `{role:"tool", tool_call_id,
//!   content}`；assistant 携带 `tool_calls` 时 content 空 → `null`）、
//!   `stream: true`、`stream_options: {include_usage: true}`、思考参数
//!   （deepseek-v4-* / kimi-k3 / GLM-5.3 / qwen3.8-* / qwen3.7-* / qwen3.6-*
//!   按模型族判定）、`tools`。
//! - SSE 解析（L198-222 / L607-723）：`data:` 行、`[DONE]` 终止、
//!   `choices[].delta.reasoning_content` → `ThinkingDelta`、
//!   `choices[].delta.content` → `TextDelta`、`choices[].delta.tool_calls`
//!   → per-index [`ToolCallAccumulator`]（id+name 齐备 → `ToolUseStart`
//!   恰一次、arguments 分片 → `ToolInputDelta`、身份缺失 →
//!   `INVALID_TOOL_CALL_STREAM` 致命终止，对照旧 L648-694）、
//!   `choices[].finish_reason` 归一化（`tool_calls` → `tool_use`）、
//!   usage-only 尾块、流耗尽无 `[DONE]` 仍正常结束。
//! - 错误映射（L736-765 `handleErrorResponse`）：429 / 5xx 可重试，
//!   `error.message` 提取，`Retry-After` 数字秒。
//!
//! # SSE 解析器选型（D-S6-2）
//!
//! 方案文档建议 `eventsource-stream`；本实现选择**手写增量行解析器**
//! [`LineSplitter`]：权威规格（旧 Java）即手写逐行（okio
//! `readUtf8LineStrict` + `data: ` 前缀匹配），手写可逐字段对齐该语义
//!（含 `\r` / `\r\n` / `\n` 三种行尾与「每行独立处理」——非 SSE 规范的
//! 多行 data 聚合），且粘包 / 半行测试完全可控，省去一个依赖。
use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use zk_protocol::model::Usage;

use crate::error::ProviderError;
use crate::provider::{ChatProvider, ChatRequest, FinishReason, ProviderEvent, Role, ThinkingMode};

/// 连接超时（规格：10s）。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// SSE 读超时（规格：10min，对齐旧 `OkHttp` `readTimeout(10min)` 防连接泄漏）。
const READ_TIMEOUT: Duration = Duration::from_mins(10);
/// 每 host 最大空闲连接（规格：5）。
const POOL_MAX_IDLE_PER_HOST: usize = 5;
/// 空闲连接保活（规格：30s）。
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// 构建共享 HTTP client（连接池复用，不每次新建）。
///
/// 全部 provider 共享同一 client 单例（由 [`crate::config`] 注入）；
/// reqwest 默认 native-tls（macOS SecurityFramework，CI license 面兼容）。
///
/// # Errors
///
/// client 构建失败（如 TLS 初始化异常）返回 [`ProviderError::Config`]。
pub fn shared_http_client() -> Result<reqwest::Client, ProviderError> {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .build()
        .map_err(|e| ProviderError::Config {
            message: format!("http client init failed: {e}"),
        })
}

/// `OpenAI` 兼容流式补全 Provider。
///
/// 通过 `base_url` 可配置性支持一切 `OpenAI` Chat Completions 兼容服务
///（`DashScope` 兼容模式 / `OpenAI` 官方 / `DeepSeek` / `Moonshot` / `Ollama` 等），
/// 对齐旧 `OpenAiCompatibleProvider` 的单实现多后端定位。
#[derive(Debug)]
pub struct OpenAiCompatProvider {
    client: reqwest::Client,
    config: crate::config::ProviderConfig,
}

impl OpenAiCompatProvider {
    /// 以配置 + 注入的共享 client 构造。
    #[must_use]
    pub fn with_client(config: crate::config::ProviderConfig, client: reqwest::Client) -> Self {
        Self { client, config }
    }

    /// 以配置构造（内部构建独立 client；注册表场景请用
    /// [`crate::registry::ProviderRegistry::from_configs`] 共享单例）。
    ///
    /// # Errors
    ///
    /// client 构建失败返回 [`ProviderError::Config`]。
    pub fn new(config: crate::config::ProviderConfig) -> Result<Self, ProviderError> {
        Ok(Self::with_client(config, shared_http_client()?))
    }
}

/// Chat Completions 端点 URL（`base_url` 去尾斜杠 + `/chat/completions`，
/// 对齐旧 `OpenAiCompatibleProvider` 构造器的 trim + 固定路径拼接）。
#[must_use]
pub fn chat_completions_url(base_url: &str) -> String {
    format!("{}/chat/completions", base_url.trim_end_matches('/'))
}

impl ChatProvider for OpenAiCompatProvider {
    fn provider_name(&self) -> &str {
        &self.config.name
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        cancel: CancellationToken,
    ) -> Result<futures::stream::BoxStream<'static, ProviderEvent>, ProviderError> {
        crate::validate_request_options(&request, &self.config.name)?;
        let responses = crate::responses::uses_responses(&self.config.name, &request.model);
        let url = if responses {
            format!("{}/responses", self.config.base_url.trim_end_matches('/'))
        } else {
            chat_completions_url(&self.config.base_url)
        };
        let summary = request.summary_thinking.is_some();
        let base_url = self.config.base_url.clone();
        let model = request.model.clone();
        let origin = self.config.name.clone();
        if self.config.api_keys.is_empty() {
            return Err(ProviderError::Config {
                message: format!("provider '{}' has empty api key", self.config.name),
            });
        }
        // 密钥环整体移入流内部：取值发生在请求发起时刻（round-robin 因此按
        // 实际调用序推进，而非流构建序），429 回作限流冷却标记（2.7）。
        let keys = self.config.api_keys.clone();
        let provider = self.config.name.clone();
        let client = self.client.clone();

        // 字节源：先发请求（HTTP 状态错误 → Err 项），成功则转响应字节流。
        let image_cancel = cancel.clone();
        let byte_source = futures::stream::once(async move {
            let request =
                crate::user_images::prepare_inline_images(request, &provider, image_cancel).await?;
            let mut body = if responses {
                crate::responses::build_request(&request, &provider)
            } else {
                build_provider_request(&request, &provider)
            };
            if summary {
                crate::summary_transport::prepare(&mut body, &base_url);
            }
            crate::payload_guard::validate(&body, &request.model, request.max_tokens)?;
            crate::user_images::record_dispatched_image_sources(&request);
            let key = if summary {
                keys.summary_key()
            } else {
                keys.next_key()
            }
            .ok_or_else(|| ProviderError::Config {
                message: format!("provider '{provider}' has empty api key"),
            })?;
            // 密钥在此唯一一次离开 ApiKey——进入 Authorization 头，不进入日志/错误链。
            let bearer = format!("Bearer {}", key.expose());
            let response = client
                .post(&url)
                .header(reqwest::header::AUTHORIZATION, bearer)
                .json(&body)
                .send()
                .await
                .map_err(ProviderError::from_transport)?;
            if !response.status().is_success() {
                return Err(read_response_error(response, &provider, summary, &keys, &key).await);
            }
            Ok(response)
        })
        .flat_map(
            move |result: Result<reqwest::Response, ProviderError>| match result {
                Ok(response) if summary => {
                    futures::stream::once(crate::summary_transport::response_bytes(response))
                        .boxed()
                }
                Ok(response) => response
                    .bytes_stream()
                    .map(|r| r.map_err(ProviderError::from_transport))
                    .boxed(),
                Err(error) => futures::stream::once(futures::future::ready(Err(error))).boxed(),
            },
        );

        if responses {
            Ok(Box::pin(crate::responses::event_stream(
                byte_source,
                cancel,
                origin,
                model,
            )))
        } else {
            Ok(Box::pin(sse_event_stream(byte_source, cancel).map(
                move |event| match event {
                    ProviderEvent::ResponseState { mut state } => {
                        state.provider.clone_from(&origin);
                        state.model.clone_from(&model);
                        ProviderEvent::ResponseState { state }
                    }
                    other => other,
                },
            )))
        }
    }
}

async fn read_response_error(
    response: reqwest::Response,
    provider: &str,
    summary: bool,
    keys: &crate::ApiKeyRing,
    key: &crate::ApiKey,
) -> ProviderError {
    let status = response.status().as_u16();
    if status == 429 && !summary {
        keys.mark_rate_limited(key);
    }
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let (body, diagnostic) = read_bounded_error_body(response).await;
    let mut error = response_error(
        provider,
        status,
        &body,
        retry_after.as_deref(),
        summary,
        keys,
        key,
    );
    attach_body_diagnostic(&mut error, diagnostic);
    error
}

const ERROR_BODY_LIMIT: usize = 256 * 1024;

pub(crate) async fn read_bounded_error_body(
    mut response: reqwest::Response,
) -> (String, Option<&'static str>) {
    let mut bytes = Vec::new();
    let diagnostic = loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = ERROR_BODY_LIMIT - bytes.len();
                bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                if chunk.len() > remaining {
                    break Some("ERROR_BODY_TRUNCATED");
                }
            }
            Ok(None) => break None,
            Err(_) => break Some("ERROR_BODY_READ_FAILED"),
        }
    };
    (String::from_utf8_lossy(&bytes).into_owned(), diagnostic)
}

pub(crate) fn attach_body_diagnostic(error: &mut ProviderError, diagnostic: Option<&str>) {
    if let ProviderError::Http { message, .. } = error {
        let suffix = diagnostic.map(|value| format!(" [{value}]"));
        let limit = ERROR_BODY_LIMIT - suffix.as_ref().map_or(0, String::len);
        let mut end = message.len().min(limit);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
        if let Some(suffix) = suffix {
            message.push_str(&suffix);
        }
    }
}

fn response_error(
    provider: &str,
    status: u16,
    body: &str,
    retry_after: Option<&str>,
    summary: bool,
    keys: &crate::ApiKeyRing,
    key: &crate::ApiKey,
) -> ProviderError {
    let mut error = map_http_response(status, body, retry_after);
    if summary {
        if let ProviderError::Http {
            retryable,
            retry_after_ms,
            ..
        } = &mut error
        {
            *retryable = status == 429;
            *retry_after_ms =
                (status == 429).then(|| crate::summary_transport::retry_delay_ms(retry_after));
        }
        return error;
    }
    let quota_failure = provider == "zenmux"
        && ((status == 402
            && (body.contains("quota_exceeded") || body.contains("quote_exceeded")))
            || (status == 404 && body.contains("model_not_available")));
    if quota_failure {
        keys.mark_rate_limited_at(key, crate::clock::mono_millis(), 15 * 60 * 1000);
        if keys.len() > 1
            && let ProviderError::Http { retryable, .. } = &mut error
        {
            *retryable = true;
        }
    }
    error
}

struct SseState<S> {
    source: Pin<Box<S>>,
    splitter: LineSplitter,
    pending: VecDeque<ProviderEvent>,
    accumulators: BTreeMap<u64, ToolCallAccumulator>,
    cancel: CancellationToken,
    done: bool,
    saw_finish: bool,
}

impl<S> SseState<S> {
    fn require_finish_or_error(&mut self) {
        if !self.saw_finish
            && !self.pending.iter().any(|event| {
                matches!(
                    event,
                    ProviderEvent::Finish { .. } | ProviderEvent::Error { .. }
                )
            })
        {
            self.pending.push_back(ProviderEvent::Error {
                error: ProviderError::Network {
                    message: "INCOMPLETE_CHAT_STREAM: finish_reason missing".into(),
                },
            });
        }
    }
}

/// 将 `OpenAI` 兼容 SSE 字节流转换为统一事件流。
///
/// 公开供 fixture 测试注入字节流（生产路径由 [`OpenAiCompatProvider`]
/// 内部接入 reqwest `bytes_stream`）；流为惰性求值。
///
/// 终止语义（对齐旧 Java）：
/// - `data: [DONE]` → 终止（此前积压事件先吐完）；
/// - 字节源耗尽（无 `[DONE]`）→ 仍正常终止（旧 L221-222 容错）；
/// - 字节源 `Err`（网络 / HTTP 状态）→ 产出 [`ProviderEvent::Error`]
///   后终止；
/// - `cancel` 触发 → **静默**终止：丢弃积压事件（含未投递的
///   [`ProviderEvent::Finish`]），不产出任何后续事件（D-S6-5）；
/// - 单 chunk JSON 解析失败 → 产出 [`ProviderEvent::Error`] 后**继续**
///   消费（对齐旧 `processChunk` L717-722 的宽容行为，D-S6-3）；
/// - `tool_calls` 增量流身份缺失（`INVALID_TOOL_CALL_STREAM`）→ 产出
///   [`ProviderEvent::Error`] 后**终止**（对照旧致命语义，留痕 §3）。
pub fn sse_event_stream<S>(
    byte_source: S,
    cancel: CancellationToken,
) -> impl Stream<Item = ProviderEvent> + Send + 'static
where
    S: Stream<Item = Result<Bytes, ProviderError>> + Send + 'static,
{
    futures::stream::unfold(
        SseState {
            source: Box::pin(byte_source),
            splitter: LineSplitter::default(),
            pending: VecDeque::new(),
            accumulators: BTreeMap::new(),
            cancel,
            done: false,
            saw_finish: false,
        },
        |mut st| async move {
            loop {
                // 取消优先于积压事件投递：Finish 绝不在取消后误发。
                if st.cancel.is_cancelled() {
                    st.done = true;
                    st.pending.clear();
                    return None;
                }
                // 积压事件先于终止判定投递：[DONE] / 流耗尽 / Error 之前
                // 产出的事件必须完整吐出（积压丢弃 = 半条消息假失败）。
                if let Some(event) = st.pending.pop_front() {
                    if matches!(event, ProviderEvent::Finish { .. }) {
                        st.saw_finish = true;
                    }
                    return Some((event, st));
                }
                if st.done {
                    return None;
                }
                let mut source = st.source.as_mut();
                let next = tokio::select! {
                    biased;
                    () = st.cancel.cancelled() => return None,
                    next = source.next() => next,
                };
                match next {
                    None => {
                        // 流耗尽：冲刷残留半行（末行无换行的宽容处理）后结束。
                        if let Some(line) = st.splitter.flush() {
                            process_line(
                                &line,
                                &mut st.pending,
                                &mut st.done,
                                &mut st.accumulators,
                            );
                        }
                        st.done = true;
                        st.require_finish_or_error();
                    }
                    Some(Ok(bytes)) => {
                        let (lines, failure) = match st.splitter.feed(&bytes) {
                            Ok(lines) => (lines, None),
                            Err(failure) => (failure.lines, Some(failure.error)),
                        };
                        for line in lines {
                            process_line(
                                &line,
                                &mut st.pending,
                                &mut st.done,
                                &mut st.accumulators,
                            );
                            if st.done {
                                st.require_finish_or_error();
                                break;
                            }
                        }
                        if !st.done
                            && let Some(error) = failure
                        {
                            st.pending.push_back(ProviderEvent::Error { error });
                            st.done = true;
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

/// 处理一行 SSE 文本（对齐旧 streamChat L209-219）。
///
/// 空行与 `data:` 之外的行（注释 `:` 行、`event:` 行等）忽略；
/// `data: [DONE]` 置 `done`；其余 `data:` 载荷交 [`parse_chunk`]——
/// 失败产出 [`ProviderEvent::Error`]，致命失败（`INVALID_TOOL_CALL_STREAM`
/// 族）额外置 `done` 终止整条流。
fn process_line(
    line: &str,
    pending: &mut VecDeque<ProviderEvent>,
    done: &mut bool,
    accumulators: &mut BTreeMap<u64, ToolCallAccumulator>,
) {
    if line.is_empty() {
        return;
    }
    let Some(data) = line.strip_prefix("data:") else {
        return;
    };
    // 宽容处理 'data: x' 与 'data:x'（SSE 规范冒号后空格可选；旧 Java
    // 严格匹配 'data: '，OpenAI 兼容服务恒发带空格形态，此处取规范宽容）。
    let data = data.strip_prefix(' ').unwrap_or(data);
    if data == "[DONE]" {
        *done = true;
        return;
    }
    let (events, failure) = parse_chunk(data, accumulators);
    pending.extend(events);
    if let Some(failure) = failure {
        pending.push_back(ProviderEvent::Error {
            error: failure.error,
        });
        if failure.fatal {
            *done = true;
        }
    }
}

/// per-index 工具调用累积器（对照旧 `ToolCallAccumulator`，L648-694）。
///
/// `OpenAI` `tool_calls` delta 按 `index` 分片到达：首片携带 `id` +
/// `function.name`，后续片仅携带 `function.arguments` 增量。id+name 首次
/// 齐备时发恰一次 [`ProviderEvent::ToolUseStart`]；arguments 分片透传为
/// [`ProviderEvent::ToolInputDelta`]（空串也发，对齐旧行为）。arguments
/// 全量累积由消费方（zk-engine 草稿）完成——本层不持有 write-only 副本
///（偏离留痕 docs/compatibility.md §3）。
#[derive(Debug, Default)]
struct ToolCallAccumulator {
    id: Option<String>,
    name: Option<String>,
    start_emitted: bool,
}

/// 单 chunk 解析失败载体：错误 + 是否致命（终止整条流）。
struct ChunkFailure {
    error: ProviderError,
    fatal: bool,
}

impl ChunkFailure {
    /// 可恢复失败（JSON 解析失败，对齐旧 `processChunk` 宽容行为 D-S6-3）。
    fn recoverable(error: ProviderError) -> Self {
        Self {
            error,
            fatal: false,
        }
    }

    /// 致命失败（`INVALID_TOOL_CALL_STREAM` 族，对齐旧致命终止语义）。
    fn fatal(error: ProviderError) -> Self {
        Self { error, fatal: true }
    }
}

/// 解析单个 `OpenAI` 兼容 chunk JSON → （事件列表, 可选失败）。
///
/// 事件顺序（对齐旧 `processChunk`）：`ThinkingDelta` → `TextDelta` →
/// 工具增量（`ToolUseStart` / `ToolInputDelta`）→ `Finish`。`choices`
/// 缺失 / 空数组时按 usage-only 尾块处理。失败前已解析出的事件仍然
/// 返回（半 chunk 有效增量不丢弃）。
#[expect(
    clippy::too_many_lines,
    reason = "One SSE chunk projects ordered reasoning, text, tools and finish events atomically."
)]
fn parse_chunk(
    payload: &str,
    accumulators: &mut BTreeMap<u64, ToolCallAccumulator>,
) -> (Vec<ProviderEvent>, Option<ChunkFailure>) {
    let chunk: Value = match serde_json::from_str(payload) {
        Ok(value) => value,
        Err(e) => {
            return (
                Vec::new(),
                Some(ChunkFailure::recoverable(ProviderError::Parse {
                    message: format!("failed to parse OpenAI chunk: {e}"),
                })),
            );
        }
    };
    if let Some(error) = chunk.get("error").filter(|error| !error.is_null()) {
        let status = error["code"]
            .as_u64()
            .and_then(|code| u16::try_from(code).ok())
            .unwrap_or(502);
        return (
            Vec::new(),
            Some(ChunkFailure::fatal(ProviderError::http(
                status,
                error["message"]
                    .as_str()
                    .unwrap_or("Provider stream error")
                    .into(),
                None,
            ))),
        );
    }
    let choice = chunk
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|arr| arr.first());
    let Some(choice) = choice else {
        // usage-only chunk（对齐旧 L614-621：choices 空且带 usage → 只补 usage）。
        if let Some(usage) = chunk.get("usage").and_then(parse_usage) {
            return (vec![ProviderEvent::UsageUpdate { usage }], None);
        }
        return (Vec::new(), None);
    };

    let mut events = Vec::new();
    if let Some(delta) = choice.get("delta").filter(|d| !d.is_null()) {
        // 1. DeepSeek reasoning_content 思考增量（非空才发，对齐旧 L631-637）。
        if let Some(details) = delta.get("reasoning_details").and_then(Value::as_array)
            && !details.is_empty()
        {
            events.push(ProviderEvent::ResponseState {
                state: crate::ProviderResponseState {
                    provider: "openrouter".into(),
                    model: String::new(),
                    output: vec![
                        json!({"type":"openrouter_reasoning","reasoning_details":details}),
                    ],
                },
            });
        }
        let direct_reasoning = delta
            .get("reasoning")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .or_else(|| {
                delta
                    .get("reasoning_content")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
            });
        let thinking = direct_reasoning.map_or_else(
            || {
                delta
                    .get("reasoning_details")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|item| {
                        item.get("text")
                            .or_else(|| item.get("summary"))
                            .and_then(Value::as_str)
                    })
                    .collect::<Vec<_>>()
                    .join("")
            },
            str::to_owned,
        );
        if !thinking.is_empty() {
            events.push(ProviderEvent::ThinkingDelta { thinking });
        }
        // 2. 文本增量（非空才发，对齐旧 L639-645）。
        if let Some(text) = delta.get("content").and_then(Value::as_str)
            && !text.is_empty()
        {
            events.push(ProviderEvent::TextDelta {
                text: text.to_owned(),
            });
        }
        // 3. tool_calls 增量累积（对照旧 L647-695 ToolCallAccumulator）。
        if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                if let Some(failure) = accumulate_tool_call(call, accumulators, &mut events) {
                    return (events, Some(failure));
                }
            }
        }
    }
    // 4. finish_reason（非 null）→ Finish（对齐旧 L698-715）。
    if let Some(raw) = choice.get("finish_reason").and_then(Value::as_str) {
        let usage = chunk
            .get("usage")
            .filter(|u| !u.is_null())
            .and_then(parse_usage);
        events.push(ProviderEvent::Finish {
            finish_reason: FinishReason::from_openai(raw),
            usage,
        });
    }
    (events, None)
}

/// 累积单个 `tool_calls` delta 项 → 工具增量事件（对照旧 L648-694）。
///
/// - `index` 缺失按 0 处理（对齐旧 `getOrDefault(0)`）；
/// - `id` / `function.name` 非空白 → 更新累积器；**空白且尚未累积** →
///   致命 `INVALID_TOOL_CALL_STREAM`（身份不可恢复）；
/// - id+name 首次齐备 → 发恰一次 [`ProviderEvent::ToolUseStart`]；
/// - `function.arguments` 到达但身份未齐备 → 致命（旧同名错误码）；
///   否则 → [`ProviderEvent::ToolInputDelta`]（空串也发）。
fn accumulate_tool_call(
    call: &Value,
    accumulators: &mut BTreeMap<u64, ToolCallAccumulator>,
    events: &mut Vec<ProviderEvent>,
) -> Option<ChunkFailure> {
    let index = call.get("index").and_then(Value::as_u64).unwrap_or(0);
    let function = call.get("function");
    let acc = accumulators.entry(index).or_default();
    if let Some(failure) = update_identity(call.get("id"), &mut acc.id, "id", index) {
        return Some(failure);
    }
    let name_node = function.and_then(|f| f.get("name"));
    if let Some(failure) = update_identity(name_node, &mut acc.name, "function.name", index) {
        return Some(failure);
    }
    if !acc.start_emitted
        && let (Some(id), Some(name)) = (acc.id.as_ref(), acc.name.as_ref())
    {
        acc.start_emitted = true;
        events.push(ProviderEvent::ToolUseStart {
            id: id.clone(),
            name: name.clone(),
        });
    }
    if let Some(arguments) = function
        .and_then(|f| f.get("arguments"))
        .and_then(Value::as_str)
    {
        if !acc.start_emitted {
            return Some(ChunkFailure::fatal(ProviderError::Parse {
                message: format!(
                    "INVALID_TOOL_CALL_STREAM: arguments arrived before id/name at index {index}"
                ),
            }));
        }
        events.push(ProviderEvent::ToolInputDelta {
            id: acc.id.clone().unwrap_or_default(),
            delta: arguments.to_owned(),
        });
    }
    None
}

/// 更新累积器身份字段（`id` / `function.name`）。
///
/// 字段缺失 → 无操作；非空白 → 覆盖累积；空白且累积器尚无该身份 →
/// 致命 `INVALID_TOOL_CALL_STREAM`（对照旧空白校验语义）。
fn update_identity(
    node: Option<&Value>,
    slot: &mut Option<String>,
    field: &str,
    index: u64,
) -> Option<ChunkFailure> {
    let value = node.and_then(Value::as_str)?;
    if value.trim().is_empty() {
        if slot.is_none() {
            return Some(ChunkFailure::fatal(ProviderError::Parse {
                message: format!(
                    "INVALID_TOOL_CALL_STREAM: blank {field} at index {index} with no accumulated identity"
                ),
            }));
        }
        return None;
    }
    if slot.as_ref().is_some_and(|existing| existing != value) {
        return Some(ChunkFailure::fatal(ProviderError::Parse {
            message: format!("INVALID_TOOL_CALL_STREAM: {field} changed at index {index}"),
        }));
    }
    *slot = Some(value.to_owned());
    None
}

/// 解析 usage 节点（对齐旧 parseUsage L771-778）。
///
/// `prompt_tokens` / `completion_tokens` → input / output；cache 两字段
/// 恒 0（旧注释：`OpenAI` 标准 API 无此字段；qwen 的 cache 字段旧实现
/// 亦不读，保持一致）。
fn parse_usage(node: &Value) -> Option<Usage> {
    let input_tokens = node.get("prompt_tokens")?.as_i64().filter(|n| *n >= 0)?;
    let output_tokens = node
        .get("completion_tokens")?
        .as_i64()
        .filter(|n| *n >= 0)?;
    Some(Usage {
        input_tokens,
        output_tokens,
        cache_read_input_tokens: node["prompt_tokens_details"]["cached_tokens"]
            .as_i64()
            .or_else(|| node["prompt_cache_hit_tokens"].as_i64())
            .unwrap_or(0)
            .max(0),
        cache_creation_input_tokens: node["cache_creation_input_tokens"]
            .as_i64()
            .unwrap_or(0)
            .max(0),
    })
}

/// HTTP 错误响应 → [`ProviderError`]（对齐旧 handleErrorResponse L736-765）。
///
/// 消息优先取响应体 `error.message`；体不可解析或无该字段时回退
/// `HTTP {status}`。`Retry-After` 仅 429 且为数字秒时转毫秒。
pub(crate) fn map_http_response(
    status: u16,
    body_text: &str,
    retry_after: Option<&str>,
) -> ProviderError {
    let message = serde_json::from_str::<Value>(body_text)
        .ok()
        .and_then(|v| {
            v.get("error")
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| format!("HTTP {status}"));
    let retry_after_ms = if matches!(status, 429 | 529) {
        parse_retry_after_ms(retry_after)
    } else {
        None
    };
    ProviderError::http(status, message, retry_after_ms)
}

/// `Retry-After` 数字秒 → 毫秒（HTTP-date 形式不解析，对齐旧实现）。
fn parse_retry_after_ms(value: Option<&str>) -> Option<u64> {
    let seconds: u64 = value?.trim().parse().ok()?;
    Some(seconds.saturating_mul(1000))
}

/// 构建请求体（对齐旧 buildOpenAiRequest / buildBaseRequest，字段逐项见模块文档）。
fn build_request_body(request: &ChatRequest) -> Value {
    let mut root = serde_json::Map::new();
    root.insert("model".into(), json!(request.model));
    // kimi- 系列官方要求 max_completion_tokens 参数名（旧 L300-302）。
    let max_tokens_key = if request.model.starts_with("kimi-") {
        "max_completion_tokens"
    } else {
        "max_tokens"
    };
    root.insert(max_tokens_key.into(), json!(request.max_tokens));

    let mut messages = Vec::with_capacity(request.messages.len() + 1);
    // 1. 系统提示 → 前置 system 消息（空白视为无，对齐旧 isBlank 判定）。
    //    分段视图在此拍平为纯文本（对照旧强类型桥接 LlmProvider.streamChat 的
    //    `systemPrompt.toPlainText()`）——OpenAI 兼容协议无 prompt caching 断点，
    //    故本路径永不产出 cache_control（与旧 buildOpenAiRequest 同构）。
    if let Some(prompt) = request.system_text() {
        messages.push(json!({ "role": "system", "content": prompt }));
    }
    // 2. 对话消息：tool 结果 / assistant 携带 tool_calls / 纯文本三分支
    //   （对照旧请求构造的 MessageParamConverter 工具分支；多模态 content
    //    数组仍为 Phase 2+ 待办）。
    for message in &request.messages {
        match message.role {
            // tool 结果 → {role:"tool", tool_call_id, content}（旧每
            // tool_result 块一条独立 tool 消息）。
            Role::Tool => messages.push(json!({
                "role": "tool",
                "tool_call_id": message.tool_call_id,
                "content": message.content,
            })),
            // assistant 携带 tool_calls：content 空串 → null（OpenAI 协议
            // 要求）；arguments 为 JSON 字符串原样透传。
            Role::Assistant if !message.tool_calls.is_empty() => {
                let content = if message.content.is_empty() {
                    Value::Null
                } else {
                    Value::String(message.content.clone())
                };
                let tool_calls: Vec<Value> = message
                    .tool_calls
                    .iter()
                    .map(|tc| {
                        json!({
                            "id": tc.id,
                            "type": "function",
                            "function": { "name": tc.name, "arguments": tc.arguments },
                        })
                    })
                    .collect();
                let mut wire = json!({
                    "role": "assistant",
                    "content": content,
                    "tool_calls": tool_calls,
                });
                if let Some(thinking) = message.thinking.as_deref() {
                    wire["reasoning_content"] = Value::String(thinking.to_owned());
                }
                messages.push(wire);
            }
            Role::User if !message.images.is_empty() => {
                let mut content = Vec::with_capacity(message.images.len() + 1);
                if !message.content.is_empty() {
                    content.push(json!({"type":"text", "text":message.content}));
                }
                content.extend(message.images.iter().filter_map(|image| {
                    resolve_image_url(image)
                        .map(|url| json!({"type":"image_url", "image_url": {"url": url}}))
                }));
                messages.push(json!({"role":"user", "content":content}));
            }
            Role::Assistant if message.thinking.is_some() => {
                messages.push(json!({
                    "role":"assistant",
                    "content":message.content,
                    "reasoning_content":message.thinking,
                }));
            }
            _ => {
                messages.push(json!({ "role": message.role.as_str(), "content": message.content }));
            }
        }
    }
    root.insert("messages".into(), Value::Array(messages));

    // 3. 流式与 usage 尾块（旧 L254-256）。
    root.insert("stream".into(), json!(true));
    root.insert("stream_options".into(), json!({ "include_usage": true }));

    // 4. 思考参数（模型族判定，vision 兜底单列，见 insert_thinking_params）。
    insert_thinking_params(&mut root, request);

    // 5. 工具定义（总是转 OpenAI function 格式，见 D-S6-8）。
    if !request.tools.is_empty() {
        let tools: Vec<Value> = request
            .tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    }
                })
            })
            .collect();
        root.insert("tools".into(), Value::Array(tools));
    }
    Value::Object(root)
}

#[cfg(test)]
use crate::user_images::ensure_inline_image_budget;

fn build_provider_request(request: &ChatRequest, provider: &str) -> Value {
    let mut body = build_request_body(request);
    if !request.stop_sequences.is_empty() {
        body["stop"] = json!(request.stop_sequences);
    }
    if provider == "dashscope-token-plan" && request.model == "bailian/glm-5.3" {
        body["model"] = json!("glm-5.3");
        body["enable_thinking"] = json!(true);
        body["reasoning_effort"] = json!("max");
        body["clear_thinking"] = json!(false);
    }
    if provider == "kimi-code" {
        if let Some(body) = body.as_object_mut() {
            body.remove("max_completion_tokens");
        }
        body["max_tokens"] = json!(request.max_tokens);
        body["thinking"] =
            json!({"type": if request.thinking.requires_support() {"enabled"} else {"disabled"}});
        if request.thinking.requires_support() {
            body["reasoning_effort"] = json!("max");
        }
    }
    if provider == "openrouter" {
        if let Some(model) = request.model.strip_prefix("openrouter/") {
            body["model"] = json!(model);
            body["reasoning"] = if request.thinking.requires_support() {
                json!({"effort":"max","exclude":false})
            } else {
                json!({"enabled":false})
            };
            body["provider"] = json!({"require_parameters":true});
        }
        let offset = usize::from(request.system_text().is_some());
        for (index, message) in request.messages.iter().enumerate() {
            if let Some(state) = &message.provider_state
                && state.provider == provider
                && state.model == request.model
            {
                let details = merge_openrouter_reasoning(&state.output);
                if !details.is_empty() {
                    body["messages"][index + offset]["reasoning_details"] = json!(details);
                }
            }
        }
    }
    if let Some(effort) = request.reasoning_effort {
        if provider == "openrouter" {
            body["reasoning"] = json!({"effort":effort.as_str(),"exclude":false});
            if let Some(body) = body.as_object_mut() {
                body.remove("reasoning_effort");
            }
        } else {
            body["reasoning_effort"] = json!(effort.as_str());
        }
    }
    if let Some(mode) = request.summary_thinking {
        use crate::SummaryThinkingMode;
        if request.model.starts_with("deepseek-") {
            body["thinking"] =
                json!({"type":if mode==SummaryThinkingMode::Off {"disabled"} else {"enabled"}});
            if mode == SummaryThinkingMode::Off {
                if let Some(body) = body.as_object_mut() {
                    body.remove("reasoning_effort");
                }
            } else {
                body["reasoning_effort"] = json!(if mode == SummaryThinkingMode::Low {
                    "low"
                } else {
                    "max"
                });
            }
        } else if request.model.starts_with("qwen") {
            body["enable_thinking"] = json!(mode != SummaryThinkingMode::Off);
            if mode != SummaryThinkingMode::Off {
                body["reasoning_effort"] = json!("max");
            }
        }
    }
    body
}

/// Reassemble streamed reasoning details by stable index/id. Provider identity
/// and the model are checked before this opaque state can reach the wire.
fn merge_openrouter_reasoning(output: &[Value]) -> Vec<Value> {
    let mut merged: Vec<Value> = Vec::new();
    for fragment in output
        .iter()
        .filter(|item| item["type"] == "openrouter_reasoning")
        .flat_map(|item| item["reasoning_details"].as_array().into_iter().flatten())
    {
        let found = merged.iter_mut().find(|item| {
            item["type"] == fragment["type"]
                && !(item.get("id").is_some()
                    && fragment.get("id").is_some()
                    && item.get("id") != fragment.get("id"))
                && (fragment.get("index").is_some() && item.get("index") == fragment.get("index")
                    || fragment.get("id").is_some() && item.get("id") == fragment.get("id"))
        });
        if let Some(existing) = found {
            if let (Some(existing), Some(fragment)) =
                (existing.as_object_mut(), fragment.as_object())
            {
                for (key, value) in fragment {
                    if matches!(key.as_str(), "text" | "data" | "summary") && value.is_string() {
                        if let Some(current) =
                            existing.get(key).and_then(Value::as_str).map(str::to_owned)
                        {
                            existing.insert(
                                key.clone(),
                                Value::String(current + value.as_str().unwrap_or_default()),
                            );
                        } else {
                            existing.insert(key.clone(), value.clone());
                        }
                    } else {
                        existing.insert(key.clone(), value.clone());
                    }
                }
            }
        } else {
            merged.push(fragment.clone());
        }
    }
    merged
}

/// 思考参数（DeepSeek/Kimi/Qwen 尊重开关；GLM 强制思考由预检拒绝关闭）。
///
/// vision 兜底模型单列——实测视觉请求在非思考模式下才稳定返回完整正文
///（思考模式可能在较短 `max_tokens` 下只消耗推理预算而没有 content），
/// 故 thinking 固定 disabled 且不发 `reasoning_effort`。
fn insert_thinking_params(root: &mut serde_json::Map<String, Value>, request: &ChatRequest) {
    if is_deepseek_vision_model(&request.model) {
        root.insert("thinking".into(), json!({ "type": "disabled" }));
    } else if request.model.starts_with("deepseek-v4-")
        || matches!(
            request.model.as_str(),
            "deepseek-flash" | "deepseek-v4.1-flash" | "kimi-k3"
        )
    {
        insert_switchable_thinking(root, request.thinking);
    } else if is_glm_forced_thinking_model(&request.model) {
        root.insert(
            "thinking".into(),
            json!({ "type": "enabled", "clear_thinking": false }),
        );
        root.insert("reasoning_effort".into(), json!("max"));
        root.insert("tool_stream".into(), json!(true));
    } else if is_openai_max_reasoning_model(&request.model) {
        if request.thinking.requires_support() {
            root.insert("reasoning_effort".into(), json!("max"));
        } else if request.model.starts_with("openai/") {
            root.insert("reasoning".into(), json!({ "enabled": false }));
        } else {
            // Chat Completions must receive the explicit switch. Known models
            // that cannot disable reasoning are rejected before HTTP dispatch.
            root.insert("reasoning_effort".into(), json!("none"));
        }
    } else if is_high_reasoning_model(&request.model) {
        if request.thinking.requires_support() {
            root.insert("reasoning_effort".into(), json!("high"));
        } else {
            root.insert("reasoning".into(), json!({ "enabled": false }));
        }
    } else if request.model.starts_with("qwen3.8-")
        || request.model.starts_with("qwen3.7-")
        || request.model.starts_with("qwen3.6-")
    {
        root.insert(
            "enable_thinking".into(),
            json!(request.thinking.requires_support()),
        );
    }
}

fn insert_switchable_thinking(root: &mut serde_json::Map<String, Value>, mode: ThinkingMode) {
    root.insert(
        "thinking".into(),
        json!({"type": if mode.requires_support() {"enabled"} else {"disabled"}}),
    );
    if mode.requires_support() {
        root.insert("reasoning_effort".into(), json!("max"));
    }
}

/// GLM-5.3 系列的官方接口不接受关闭思考；仅这两个内置 ID 强制下发参数，
/// 不将规则泛化到用户自定义的其他 `glm-*` 模型。
fn is_glm_forced_thinking_model(model: &str) -> bool {
    matches!(model, "glm-5.3" | "glm-5.3-flash")
}

/// `OpenAI` Sol / Astra 在直连与 `ZenMux` 路由下启用思考时使用最高推理档位。
fn is_openai_max_reasoning_model(model: &str) -> bool {
    matches!(
        model,
        "gpt-5.6-sol" | "openai/gpt-5.6-sol" | "gpt-6-astra" | "openai/gpt-6-astra"
    )
}

/// Gemini 3.8 Flash / Grok 4.6 启用思考时的最高合法档位是 `high`（不是 `max`）。
fn is_high_reasoning_model(model: &str) -> bool {
    matches!(model, "google/gemini-3.8-flash" | "x-ai/grok-4.6")
}

/// 判断思考模式**启用**参数是否会被下发（供请求构建单测与文档自证）。
///
/// `DeepSeek` 视觉兜底模型固定下发 `thinking: disabled`，不属于启用范畴，
/// 返回 `false`（对照旧 `isDeepSeekV4Model` 排除 vision 的语义）。
#[must_use]
pub fn thinking_params_for(model: &str, mode: ThinkingMode) -> bool {
    (((model.starts_with("deepseek-v4-") && !is_deepseek_vision_model(model))
        || matches!(model, "deepseek-flash" | "deepseek-v4.1-flash" | "kimi-k3"))
        && mode.requires_support())
        || is_glm_forced_thinking_model(model)
        || ((is_openai_max_reasoning_model(model) || is_high_reasoning_model(model))
            && mode.requires_support())
        || ((model.starts_with("qwen3.8-")
            || model.starts_with("qwen3.7-")
            || model.starts_with("qwen3.6-"))
            && mode.requires_support())
}

/// `DeepSeek` 图片理解兜底模型使用独立、稳定的非思考请求参数
///（对照旧 `OpenAiCompatibleProvider.isDeepSeekVisionModel`）。
#[must_use]
pub fn is_deepseek_vision_model(model: &str) -> bool {
    model == "deepseek-v4-flash-vision-exp"
}

/// 增量 SSE 行切分器。
///
/// 行尾语义对齐 okio `readUtf8LineStrict`：`\n`、`\r\n`、单独 `\r` 均切行；
/// 半行缓冲等待后续字节；[`LineSplitter::flush`] 在 EOF 时回收无行尾的
/// 残留尾行（部分简易服务端末行不带换行）。
pub(crate) const MAX_SSE_EVENT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Default)]
pub(crate) struct LineSplitter {
    buf: Vec<u8>,
    skip_lf: bool,
    event_bytes: usize,
    failed: bool,
}

/// A terminal framing error must not discard complete earlier lines from the
/// same network chunk. Consumers process these lines before emitting the error.
#[derive(Debug)]
pub(crate) struct LineSplitFailure {
    pub(crate) lines: Vec<String>,
    pub(crate) error: ProviderError,
}

impl LineSplitter {
    /// Scan only incoming bytes; neither a line nor an SSE event may grow unbounded.
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Result<Vec<String>, LineSplitFailure> {
        if self.failed {
            return Err(LineSplitFailure {
                lines: Vec::new(),
                error: Self::limit_error(),
            });
        }
        let mut lines = Vec::new();
        let mut start = 0;
        for (index, byte) in chunk.iter().copied().enumerate() {
            if self.skip_lf {
                self.skip_lf = false;
                if byte == b'\n' {
                    start = index + 1;
                    continue;
                }
            }
            if byte == b'\r' || byte == b'\n' {
                if let Err(error) = self.append(&chunk[start..index]) {
                    return Err(LineSplitFailure { lines, error });
                }
                if self.buf.is_empty() {
                    self.event_bytes = 0;
                }
                lines.push(String::from_utf8_lossy(&std::mem::take(&mut self.buf)).into_owned());
                self.skip_lf = byte == b'\r';
                start = index + 1;
            }
        }
        if let Err(error) = self.append(&chunk[start..]) {
            return Err(LineSplitFailure { lines, error });
        }
        Ok(lines)
    }

    fn append(&mut self, bytes: &[u8]) -> Result<(), ProviderError> {
        if bytes.len() > MAX_SSE_EVENT_BYTES.saturating_sub(self.buf.len())
            || bytes.len() > MAX_SSE_EVENT_BYTES.saturating_sub(self.event_bytes)
        {
            self.failed = true;
            self.buf.clear();
            return Err(Self::limit_error());
        }
        self.buf.extend_from_slice(bytes);
        self.event_bytes += bytes.len();
        Ok(())
    }

    fn limit_error() -> ProviderError {
        ProviderError::Parse {
            message: "PROVIDER_SSE_EVENT_TOO_LARGE: 16 MiB limit".into(),
        }
    }

    pub(crate) fn flush(&mut self) -> Option<String> {
        if self.failed || self.buf.is_empty() {
            None
        } else {
            Some(String::from_utf8_lossy(&std::mem::take(&mut self.buf)).into_owned())
        }
    }
}

/// Resolve the `OpenAI` `image_url.url` value for one image (Java
/// `appendImageUrlPart`): a non-blank trusted remote URL is forwarded
/// verbatim, otherwise the base64 payload becomes a data URI, and images
/// carrying neither are skipped.
pub(crate) fn resolve_image_url(image: &crate::ImageSource) -> Option<String> {
    if let Some(url) = image.url.as_deref()
        && !url.trim().is_empty()
    {
        return Some(url.to_owned());
    }
    image
        .data
        .as_deref()
        .map(|data| format!("data:{};base64,{}", image.media_type, data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{SystemPrompt, ToolOrigin};
    use crate::provider::{ChatMessage, ToolCallRequest, ToolSpec};

    #[tokio::test]
    async fn http_error_body_is_bounded_without_losing_status_or_retry_after() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let _ = stream.read(&mut request).await;
            let body = serde_json::json!({"error":{"message":"x".repeat(300 * 1024)}}).to_string();
            let header = format!(
                "HTTP/1.1 429 Too Many Requests\r\nContent-Length: {}\r\nRetry-After: 2\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(header.as_bytes()).await;
            let _ = stream.write_all(body.as_bytes()).await;
        });
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(format!("http://{address}"))
            .send()
            .await
            .unwrap();
        let key = crate::ApiKey::new("fixture");
        let keys = crate::ApiKeyRing::new(vec![key.clone()]);
        let error = read_response_error(response, "fixture", true, &keys, &key).await;
        server.await.unwrap();
        let ProviderError::Http {
            status,
            message,
            retry_after_ms,
            ..
        } = error
        else {
            panic!("HTTP status must be preserved");
        };
        assert_eq!(status, 429);
        assert_eq!(retry_after_ms, Some(2000));
        assert!(message.len() <= 256 * 1024 && message.contains("TRUNCATED"));
    }

    #[test]
    fn unterminated_sse_line_cannot_accumulate_unbounded_bytes() {
        let mut splitter = LineSplitter::default();
        let block = vec![b'x'; 1024 * 1024];
        for _ in 0..17 {
            let _ = splitter.feed(&block);
        }
        assert!(splitter.buf.len() <= 16 * 1024 * 1024);
    }

    #[test]
    fn split_crlf_does_not_create_a_second_empty_event_boundary() {
        let mut splitter = LineSplitter::default();
        let _ = splitter.feed(b"data: first\r");
        assert_eq!(
            splitter.feed(b"\ndata: second\n").unwrap(),
            vec!["data: second"]
        );
    }

    #[test]
    fn sse_bounds_whole_multiline_event_but_resets_after_event_separator() {
        let line = format!("data: {}\n", "x".repeat(1024));
        let mut splitter = LineSplitter::default();
        for _ in 0..(MAX_SSE_EVENT_BYTES / (line.len() - 1)) {
            splitter.feed(line.as_bytes()).unwrap();
        }
        assert!(splitter.feed(line.as_bytes()).is_err());
        assert!(splitter.flush().is_none());
        let mut splitter = LineSplitter::default();
        let event = format!("{line}\n");
        for _ in 0..(MAX_SSE_EVENT_BYTES / line.len() + 100) {
            assert_eq!(splitter.feed(event.as_bytes()).unwrap().len(), 2);
        }
    }

    #[tokio::test]
    async fn oversized_sse_stream_ends_with_one_error_and_no_success() {
        let source = futures::stream::iter([
            Ok(bytes::Bytes::from(vec![b'x'; MAX_SSE_EVENT_BYTES + 1])),
            Ok(bytes::Bytes::from_static(b"data: [DONE]\n\n")),
        ]);
        let events: Vec<_> = sse_event_stream(source, CancellationToken::new())
            .collect()
            .await;
        assert!(matches!(
            events.as_slice(),
            [ProviderEvent::Error {
                error: ProviderError::Parse { .. }
            }]
        ));
    }

    #[tokio::test]
    async fn complete_usage_before_oversized_event_is_independent_of_chunk_boundaries() {
        let prefix =
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":13,\"completion_tokens\":2}}\n\n";
        let oversized = vec![b'x'; MAX_SSE_EVENT_BYTES + 1];
        let split_source = futures::stream::iter([
            Ok(Bytes::copy_from_slice(prefix)),
            Ok(Bytes::copy_from_slice(&oversized)),
        ]);
        let expected: Vec<_> = sse_event_stream(split_source, CancellationToken::new())
            .collect()
            .await;
        assert!(matches!(
            expected.as_slice(),
            [
                ProviderEvent::UsageUpdate { .. },
                ProviderEvent::Error { .. }
            ]
        ));
        let mut combined = prefix.to_vec();
        combined.extend_from_slice(&oversized);
        let combined_source = futures::stream::iter([Ok(Bytes::from(combined))]);
        let actual: Vec<_> = sse_event_stream(combined_source, CancellationToken::new())
            .collect()
            .await;
        assert_eq!(
            actual, expected,
            "valid usage must precede the oversized-event error for either network chunking"
        );
    }

    #[tokio::test]
    async fn terminal_events_before_oversized_tail_are_independent_of_chunk_boundaries() {
        let cases = [
            (
                "chat",
                concat!(
                    "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":13,\"completion_tokens\":2}}\n\n",
                    "data: [DONE]\n\n",
                ),
            ),
            (
                "anthropic",
                concat!(
                    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":13,\"output_tokens\":0}}}\n\n",
                    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
                    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
                ),
            ),
            (
                "responses",
                "data: {\"type\":\"response.completed\",\"response\":{\"output\":[],\"usage\":{\"input_tokens\":13,\"output_tokens\":2}}}\n\n",
            ),
        ];
        for (provider, prefix) in cases {
            let oversized = Bytes::from(vec![b'x'; MAX_SSE_EVENT_BYTES + 1]);
            let split = vec![Bytes::copy_from_slice(prefix.as_bytes()), oversized.clone()];
            let mut combined = prefix.as_bytes().to_vec();
            combined.extend_from_slice(&oversized);
            let expected = chunked_events(provider, split).await;
            assert!(expected.iter().any(|event| matches!(event, ProviderEvent::Finish { usage: Some(usage), .. } if usage.input_tokens == 13 && usage.output_tokens == 2)), "{provider}: {expected:?}");
            assert!(
                !expected
                    .iter()
                    .any(|event| matches!(event, ProviderEvent::Error { .. }))
            );
            let actual = chunked_events(provider, vec![Bytes::from(combined)]).await;
            assert_eq!(
                actual, expected,
                "{provider}: an earlier terminal event must keep its usage and terminal result"
            );
        }
    }

    async fn chunked_events(provider: &str, chunks: Vec<Bytes>) -> Vec<ProviderEvent> {
        let source = futures::stream::iter(chunks.into_iter().map(Ok));
        match provider {
            "chat" => {
                sse_event_stream(source, CancellationToken::new())
                    .collect()
                    .await
            }
            "anthropic" => {
                crate::anthropic::anthropic_event_stream(source, CancellationToken::new())
                    .collect()
                    .await
            }
            "responses" => {
                crate::responses::event_stream(
                    source,
                    CancellationToken::new(),
                    "fixture".into(),
                    "fixture".into(),
                )
                .collect()
                .await
            }
            _ => unreachable!("fixed fixture providers"),
        }
    }

    fn qwen_request() -> ChatRequest {
        ChatRequest::new("qwen3.8-max-0902")
            .with_system_prompt(Some("be terse".into()))
            .with_message(ChatMessage::user("hello"))
            .with_message(ChatMessage::assistant("hi"))
            .with_max_tokens(2048)
    }

    /// 独立累积器解析且断言无失败（多数纯文本 fixture 的便捷入口）。
    fn parse_ok(payload: &str) -> Vec<ProviderEvent> {
        parse_ok_with(payload, &mut BTreeMap::new())
    }

    /// 共享累积器解析（跨 chunk `tool_calls` fixture）且断言无失败。
    fn parse_ok_with(
        payload: &str,
        accumulators: &mut BTreeMap<u64, ToolCallAccumulator>,
    ) -> Vec<ProviderEvent> {
        let (events, failure) = parse_chunk(payload, accumulators);
        assert!(failure.is_none(), "unexpected chunk failure");
        events
    }

    #[test]
    fn incomplete_usage_is_unknown_and_explicit_zero_is_known() {
        for usage in [
            json!({}),
            json!({"prompt_tokens":4}),
            json!({"completion_tokens":0}),
            json!({"prompt_tokens":-1,"completion_tokens":2}),
        ] {
            assert!(super::parse_usage(&usage).is_none());
            let payload = json!({"choices":[{"finish_reason":"stop"}],"usage":usage});
            let (events, error) =
                super::parse_chunk(&payload.to_string(), &mut std::collections::BTreeMap::new());
            assert!(error.is_none());
            assert!(matches!(
                events.last(),
                Some(ProviderEvent::Finish { usage: None, .. })
            ));
        }
        assert_eq!(
            super::parse_usage(&json!({"prompt_tokens":0,"completion_tokens":0})),
            Some(Usage::default())
        );
    }

    #[test]
    fn summary_wire_override_supports_low_off_without_changing_chat() {
        use crate::SummaryThinkingMode;
        let mut request = ChatRequest::new("deepseek-flash").with_thinking(ThinkingMode::Adaptive);
        assert_eq!(
            super::build_provider_request(&request, "deepseek")["reasoning_effort"],
            "max"
        );
        request.summary_thinking = Some(SummaryThinkingMode::Low);
        assert_eq!(
            super::build_provider_request(&request, "deepseek")["reasoning_effort"],
            "low"
        );
        request.summary_thinking = Some(SummaryThinkingMode::Off);
        let body = super::build_provider_request(&request, "deepseek");
        assert_eq!(body["thinking"]["type"], "disabled");
        assert!(body.get("reasoning_effort").is_none());
        request.model = "qwen3.7-plus".into();
        assert_eq!(
            super::build_provider_request(&request, "dashscope")["enable_thinking"],
            false
        );
    }

    #[test]
    fn request_body_matches_legacy_shape() {
        let body = build_request_body(&qwen_request());
        assert_eq!(body["model"], "qwen3.8-max-0902");
        assert_eq!(body["max_tokens"], 2048);
        assert_eq!(body["stream"], true);
        assert_eq!(body["stream_options"]["include_usage"], true);
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(messages[0]["content"], "be terse");
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[2]["role"], "assistant");
        // qwen + Disabled must override the provider's thinking-on default.
        assert_eq!(body["enable_thinking"], false);
        assert!(body.get("tools").is_none());
    }

    #[test]
    fn request_body_thinking_params_per_model_family() {
        // The normal engine's Adaptive default retains enabled + max.
        let deepseek = build_request_body(
            &ChatRequest::new("deepseek-v4-flash").with_thinking(ThinkingMode::Adaptive),
        );
        assert_eq!(deepseek["thinking"]["type"], "enabled");
        assert_eq!(deepseek["reasoning_effort"], "max");
        // deepseek 视觉兜底模型：thinking 固定 disabled 且不发 reasoning_effort。
        let vision = build_request_body(&ChatRequest::new("deepseek-v4-flash-vision-exp"));
        assert_eq!(vision["thinking"]["type"], "disabled");
        assert!(vision.get("reasoning_effort").is_none());
        // kimi-k3：reasoning_effort=max；且 max_tokens 换名。
        let kimi =
            build_request_body(&ChatRequest::new("kimi-k3").with_thinking(ThinkingMode::Adaptive));
        assert_eq!(kimi["reasoning_effort"], "max");
        assert!(kimi.get("max_tokens").is_none());
        assert_eq!(kimi["max_completion_tokens"], 8192);
        // GLM-5.3 requires thinking; disabled is rejected before this renderer.
        for model in ["glm-5.3", "glm-5.3-flash"] {
            let glm =
                build_request_body(&ChatRequest::new(model).with_thinking(ThinkingMode::Adaptive));
            assert_eq!(glm["thinking"]["type"], "enabled");
            assert_eq!(glm["thinking"]["clear_thinking"], false);
            assert_eq!(glm["reasoning_effort"], "max");
            assert_eq!(glm["tool_stream"], true);
        }
        // qwen3.7-*：仅 thinking 需要支持时下发 enable_thinking。
        let qwen_off = build_request_body(&qwen_request());
        assert_eq!(qwen_off["enable_thinking"], false);
        let qwen_on = build_request_body(
            &ChatRequest::new("qwen3.7-plus").with_thinking(ThinkingMode::Enabled),
        );
        assert_eq!(qwen_on["enable_thinking"], true);
        // qwen3.6-* 同族。
        let qwen36 = build_request_body(
            &ChatRequest::new("qwen3.6-max").with_thinking(ThinkingMode::Adaptive),
        );
        assert_eq!(qwen36["enable_thinking"], true);
        let qwen38 = build_request_body(
            &ChatRequest::new("qwen3.8-flash").with_thinking(ThinkingMode::Enabled),
        );
        assert_eq!(qwen38["enable_thinking"], true);
        let qwen38_off = build_request_body(&ChatRequest::new("qwen3.8-max"));
        assert_eq!(qwen38_off["enable_thinking"], false);
        // OpenAI Sol / Astra：ZenMux ID 显式关闭；启用时直连与 ZenMux 均使用 max。
        for model in [
            "gpt-5.6-sol",
            "openai/gpt-5.6-sol",
            "gpt-6-astra",
            "openai/gpt-6-astra",
        ] {
            let disabled = build_request_body(&ChatRequest::new(model));
            if model.starts_with("openai/") {
                assert!(disabled.get("reasoning_effort").is_none());
                assert_eq!(disabled["reasoning"]["enabled"], false);
            } else {
                assert_eq!(disabled["reasoning_effort"], "none");
                assert!(disabled.get("reasoning").is_none());
            }
            let enabled =
                build_request_body(&ChatRequest::new(model).with_thinking(ThinkingMode::Enabled));
            assert_eq!(enabled["reasoning_effort"], "max");
        }
        // Gemini / Grok：ZenMux 下显式关闭，启用时使用最高合法档位 high。
        let gemini = build_request_body(
            &ChatRequest::new("google/gemini-3.8-flash").with_thinking(ThinkingMode::Enabled),
        );
        assert_eq!(gemini["max_tokens"], 8192);
        assert_eq!(gemini["stream"], true);
        assert_eq!(gemini["stream_options"]["include_usage"], true);
        assert_eq!(gemini["reasoning_effort"], "high");
        let grok = build_request_body(
            &ChatRequest::new("x-ai/grok-4.6").with_thinking(ThinkingMode::Enabled),
        );
        assert_eq!(grok["max_tokens"], 8192);
        assert_eq!(grok["stream"], true);
        assert_eq!(grok["stream_options"]["include_usage"], true);
        assert_eq!(grok["reasoning_effort"], "high");
        for model in ["google/gemini-3.8-flash", "x-ai/grok-4.6"] {
            let disabled = build_request_body(&ChatRequest::new(model));
            assert!(disabled.get("reasoning_effort").is_none());
            assert_eq!(disabled["reasoning"]["enabled"], false);
        }
    }

    #[test]
    fn request_body_blank_system_prompt_is_dropped() {
        let req = ChatRequest::new("m").with_system_prompt(Some("   \n\t ".into()));
        let body = build_request_body(&req);
        assert_eq!(body["messages"].as_array().unwrap().len(), 0);
    }

    /// prompt caching 断点是 `Anthropic` 专属：本路径把分段拍平为纯文本，且请求
    /// 体任何层级都不得出现 `cache_control`（旧 buildOpenAiRequest 无该字段）。
    #[test]
    fn cache_breakpoints_never_reach_openai_request() {
        let req = ChatRequest::new("qwen3.8-max-0902")
            .with_system(SystemPrompt::segmented("static core", "session state"))
            .with_tools(vec![
                ToolSpec {
                    name: "read_file".into(),
                    description: "read a file".into(),
                    parameters: json!({ "type": "object" }),
                },
                ToolSpec {
                    name: "mcp__srv__query".into(),
                    description: "mcp tool".into(),
                    parameters: json!({ "type": "object" }),
                },
            ])
            .with_tool_origins(&[ToolOrigin::BuiltIn, ToolOrigin::Mcp]);
        assert_eq!(
            req.tool_cache_breakpoint,
            Some(0),
            "断点已折算，但本路径必须忽略"
        );
        let body = build_request_body(&req);
        // 分段拍平为单条 system 消息（段间 "\n"，旧 toPlainText）。
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][0]["content"], "static core\nsession state");
        assert!(
            !serde_json::to_string(&body)
                .expect("body serializes")
                .contains("cache_control"),
            "openai 兼容路径不得出现 cache_control"
        );
    }

    #[test]
    fn request_body_tools_use_openai_function_format() {
        let req = ChatRequest::new("qwen3.8-max-0902").with_tools(vec![ToolSpec {
            name: "get_weather".into(),
            description: "query weather".into(),
            parameters: json!({ "type": "object", "properties": {} }),
        }]);
        let body = build_request_body(&req);
        let tools = body["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["function"]["name"], "get_weather");
        assert_eq!(tools[0]["function"]["parameters"]["type"], "object");
    }

    #[test]
    fn request_body_tool_and_assistant_tool_call_shapes() {
        // 多轮回填历史：assistant(tool_calls) → tool 结果 → assistant 纯文本。
        let req = ChatRequest::new("qwen3.8-max-0902")
            .with_message(ChatMessage::user("check"))
            .with_message(ChatMessage::assistant_tool_calls(
                "",
                vec![ToolCallRequest {
                    id: "call-1".into(),
                    name: "Echo".into(),
                    arguments: r#"{"text":"hi"}"#.into(),
                }],
            ))
            .with_message(ChatMessage::tool("call-1", "hi"))
            .with_message(ChatMessage::assistant_tool_calls(
                "and text",
                vec![ToolCallRequest {
                    id: "call-2".into(),
                    name: "Echo".into(),
                    arguments: "{}".into(),
                }],
            ));
        let body = build_request_body(&req);
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 4);
        // assistant 携带 tool_calls：content 空串 → null。
        assert_eq!(messages[1]["role"], "assistant");
        assert!(messages[1]["content"].is_null());
        assert_eq!(messages[1]["tool_calls"][0]["id"], "call-1");
        assert_eq!(messages[1]["tool_calls"][0]["type"], "function");
        assert_eq!(messages[1]["tool_calls"][0]["function"]["name"], "Echo");
        assert_eq!(
            messages[1]["tool_calls"][0]["function"]["arguments"],
            r#"{"text":"hi"}"#
        );
        // tool 结果消息形状。
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[2]["tool_call_id"], "call-1");
        assert_eq!(messages[2]["content"], "hi");
        assert!(messages[2].get("tool_calls").is_none());
        // content 非空的 assistant tool_calls：文本保留。
        assert_eq!(messages[3]["content"], "and text");
    }

    #[test]
    fn parse_chunk_extracts_deltas_and_finish() {
        let events = parse_ok(
            r#"{"choices":[{"delta":{"reasoning_content":"think","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":2}}"#,
        );
        assert_eq!(
            events,
            vec![
                ProviderEvent::ThinkingDelta {
                    thinking: "think".into()
                },
                ProviderEvent::TextDelta { text: "hi".into() },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(Usage {
                        input_tokens: 7,
                        output_tokens: 2,
                        ..Usage::default()
                    }),
                },
            ]
        );
    }

    #[test]
    fn parse_chunk_empty_strings_emit_nothing() {
        // 对齐旧 Java：空串 content / reasoning_content 不发事件。
        let events = parse_ok(r#"{"choices":[{"delta":{"content":"","reasoning_content":""}}]}"#);
        assert!(events.is_empty());
    }

    #[test]
    fn parse_chunk_usage_only_and_null_usage() {
        let events =
            parse_ok(r#"{"choices":[],"usage":{"prompt_tokens":12,"completion_tokens":5}}"#);
        assert_eq!(
            events,
            vec![ProviderEvent::UsageUpdate {
                usage: Usage {
                    input_tokens: 12,
                    output_tokens: 5,
                    ..Usage::default()
                },
            }]
        );
        // choices 缺失 + usage null → 无事件。
        assert!(parse_ok(r#"{"usage":null}"#).is_empty());
        // 完整 chunk 无 usage → Finish.usage == None。
        let events = parse_ok(r#"{"choices":[{"delta":{},"finish_reason":"length"}]}"#);
        assert_eq!(
            events,
            vec![ProviderEvent::Finish {
                finish_reason: FinishReason::MaxTokens,
                usage: None,
            }]
        );
    }

    #[test]
    fn parse_chunk_tool_calls_accumulate_across_chunks() {
        let mut accumulators = BTreeMap::new();
        // 首片：id + name + 空 arguments → ToolUseStart 恰一次 + 空 delta。
        let events = parse_ok_with(
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"Echo","arguments":""}}]}}]}"#,
            &mut accumulators,
        );
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolUseStart {
                    id: "call-1".into(),
                    name: "Echo".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "call-1".into(),
                    delta: String::new(),
                },
            ]
        );
        // 后续片：仅 index + arguments 分片 → 不再重发 ToolUseStart。
        let events = parse_ok_with(
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"text\""}}]}}]}"#,
            &mut accumulators,
        );
        assert_eq!(
            events,
            vec![ProviderEvent::ToolInputDelta {
                id: "call-1".into(),
                delta: "{\"text\"".into(),
            }]
        );
        // 并行第二调用（index=1）独立累积；文本与工具增量同 chunk 时序保序。
        let events = parse_ok_with(
            r#"{"choices":[{"delta":{"content":"and","tool_calls":[{"index":1,"id":"call-2","function":{"name":"Echo"}},{"index":0,"function":{"arguments":":\"hi\"}"}}]}}]}"#,
            &mut accumulators,
        );
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta { text: "and".into() },
                ProviderEvent::ToolUseStart {
                    id: "call-2".into(),
                    name: "Echo".into(),
                },
                ProviderEvent::ToolInputDelta {
                    id: "call-1".into(),
                    delta: ":\"hi\"}".into(),
                },
            ]
        );
        // 终结片：finish_reason=tool_calls → ToolUse 归一化。
        let events = parse_ok_with(
            r#"{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}"#,
            &mut accumulators,
        );
        assert_eq!(
            events,
            vec![ProviderEvent::Finish {
                finish_reason: FinishReason::ToolUse,
                usage: None,
            }]
        );
    }

    #[test]
    fn parse_chunk_blank_identity_is_fatal() {
        // id 空白且累积器无身份 → INVALID_TOOL_CALL_STREAM 致命。
        let (events, failure) = parse_chunk(
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"  "}]}}]}"#,
            &mut BTreeMap::new(),
        );
        assert!(events.is_empty());
        let failure = failure.expect("blank id must fail");
        assert!(failure.fatal);
        assert!(
            failure
                .error
                .to_string()
                .contains("INVALID_TOOL_CALL_STREAM")
        );
        // 已累积身份后出现空白 id → 忽略（不覆盖、不失败）。
        let mut accumulators = BTreeMap::new();
        parse_ok_with(
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","function":{"name":"Echo"}}]}}]}"#,
            &mut accumulators,
        );
        let events = parse_ok_with(
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"","function":{"arguments":"{}"}}]}}]}"#,
            &mut accumulators,
        );
        assert_eq!(
            events,
            vec![ProviderEvent::ToolInputDelta {
                id: "call-1".into(),
                delta: "{}".into(),
            }]
        );
    }

    #[test]
    fn parse_chunk_arguments_before_identity_is_fatal() {
        let (events, failure) = parse_chunk(
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]}}]}"#,
            &mut BTreeMap::new(),
        );
        assert!(events.is_empty());
        let failure = failure.expect("arguments before identity must fail");
        assert!(failure.fatal);
        assert!(
            failure
                .error
                .to_string()
                .contains("INVALID_TOOL_CALL_STREAM")
        );
    }

    #[test]
    fn parse_chunk_bad_json_is_recoverable_parse_error() {
        let (events, failure) = parse_chunk("{not json", &mut BTreeMap::new());
        assert!(events.is_empty());
        let failure = failure.expect("bad json must fail");
        assert!(
            !failure.fatal,
            "json parse failure stays recoverable (D-S6-3)"
        );
        assert!(!failure.error.is_retryable());
        assert!(matches!(failure.error, ProviderError::Parse { .. }));
    }

    #[test]
    fn process_line_fatal_failure_sets_done() {
        let mut pending = VecDeque::new();
        let mut done = false;
        let mut accumulators = BTreeMap::new();
        process_line(
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]}}]}"#,
            &mut pending,
            &mut done,
            &mut accumulators,
        );
        assert!(done, "fatal tool_calls failure must terminate the stream");
        assert_eq!(pending.len(), 1);
        assert!(matches!(
            pending.pop_front(),
            Some(ProviderEvent::Error { .. })
        ));
        // 对照：坏 JSON 仅产出 Error，不终止（宽容恢复）。
        let mut done = false;
        process_line("data: {broken", &mut pending, &mut done, &mut accumulators);
        assert!(!done);
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn map_http_response_extracts_error_message() {
        let err = map_http_response(
            401,
            r#"{"error":{"message":"Invalid API key","type":"invalid_request_error"}}"#,
            None,
        );
        assert!(err.to_string().contains("Invalid API key"));
        assert!(!err.is_retryable());

        let err = map_http_response(500, "internal", None);
        assert!(err.is_retryable());
        assert!(err.to_string().contains("HTTP 500")); // 体非 JSON → 回退

        let err = map_http_response(429, r#"{"error":{"message":"rate limited"}}"#, Some("2"));
        assert!(err.is_retryable());
        assert!(matches!(
            err,
            ProviderError::Http {
                retry_after_ms: Some(2000),
                ..
            }
        ));
        // Retry-After 非 429 不解析；HTTP-date 形式不解析。
        let err = map_http_response(500, "", Some("3"));
        assert!(matches!(
            err,
            ProviderError::Http {
                retry_after_ms: None,
                ..
            }
        ));
        let err = map_http_response(429, "", Some("Wed, 21 Oct 2015 07:28:00 GMT"));
        assert!(matches!(
            err,
            ProviderError::Http {
                retry_after_ms: None,
                ..
            }
        ));
    }

    #[test]
    fn line_splitter_handles_all_line_endings_and_partial_chunks() {
        let mut splitter = LineSplitter::default();
        // \n / \r\n / 单独 \r 三种行尾 + 跨块 CRLF。
        assert_eq!(
            splitter.feed(b"data: a\ndata: b\r\ndata: c\r").unwrap(),
            vec!["data: a", "data: b", "data: c"]
        );
        // \r 在上块末尾、\n 在下块开头 → 只切一行。
        let mut splitter = LineSplitter::default();
        assert_eq!(splitter.feed(b"data: x\r").unwrap().len(), 1);
        assert!(!splitter.feed(b"\ndata: y\n").unwrap().is_empty());
        // 半行缓冲。
        let mut splitter = LineSplitter::default();
        assert!(splitter.feed(b"da").unwrap().is_empty());
        assert!(splitter.feed(b"ta: z").unwrap().is_empty());
        assert_eq!(splitter.feed(b"\n").unwrap(), vec!["data: z"]);
        // EOF 残留尾行。
        let mut splitter = LineSplitter::default();
        assert!(splitter.feed(b"tail").unwrap().is_empty());
        assert_eq!(splitter.flush().as_deref(), Some("tail"));
        assert_eq!(splitter.flush(), None);
        // 多字节 UTF-8 跨块不损坏（`你` 的 3 字节跨块边界）。
        let mut splitter = LineSplitter::default();
        assert!(
            splitter
                .feed(&"data: 你\n".as_bytes()[..6])
                .unwrap()
                .is_empty()
        );
        let lines = splitter.feed(&"data: 你\n".as_bytes()[6..]).unwrap();
        assert_eq!(lines, vec!["data: 你"]);
    }

    #[test]
    fn thinking_params_helper_mirrors_request_body() {
        assert!(!thinking_params_for(
            "deepseek-v4-pro",
            ThinkingMode::Disabled
        ));
        assert!(thinking_params_for(
            "deepseek-v4-pro",
            ThinkingMode::Adaptive
        ));
        // 视觉兜底模型下发的是 disabled，不算思考启用参数。
        assert!(!thinking_params_for(
            "deepseek-v4-flash-vision-exp",
            ThinkingMode::Enabled
        ));
        assert!(is_deepseek_vision_model("deepseek-v4-flash-vision-exp"));
        assert!(!is_deepseek_vision_model("deepseek-v4-flash"));
        assert!(!thinking_params_for("kimi-k3", ThinkingMode::Disabled));
        assert!(thinking_params_for("kimi-k3", ThinkingMode::Adaptive));
        assert!(thinking_params_for("glm-5.3", ThinkingMode::Disabled));
        assert!(thinking_params_for("glm-5.3-flash", ThinkingMode::Disabled));
        assert!(!thinking_params_for(
            "glm-5.3-flash-preview",
            ThinkingMode::Enabled
        ));
        assert!(thinking_params_for("qwen3.8-flash", ThinkingMode::Enabled));
        assert!(!thinking_params_for("qwen3.8-max", ThinkingMode::Disabled));
        assert!(thinking_params_for(
            "qwen3.8-max-0902",
            ThinkingMode::Enabled
        ));
        assert!(!thinking_params_for(
            "qwen3.8-max-0902",
            ThinkingMode::Disabled
        ));
        for model in [
            "gpt-5.6-sol",
            "openai/gpt-5.6-sol",
            "gpt-6-astra",
            "openai/gpt-6-astra",
        ] {
            assert!(!thinking_params_for(model, ThinkingMode::Disabled));
            assert!(thinking_params_for(model, ThinkingMode::Enabled));
        }
        assert!(!thinking_params_for(
            "google/gemini-3.8-flash",
            ThinkingMode::Disabled
        ));
        assert!(thinking_params_for(
            "google/gemini-3.8-flash",
            ThinkingMode::Adaptive
        ));
        assert!(!thinking_params_for(
            "x-ai/grok-4.6",
            ThinkingMode::Disabled
        ));
        assert!(thinking_params_for("x-ai/grok-4.6", ThinkingMode::Enabled));
    }

    #[test]
    fn openai_request_serializes_validated_user_images_as_content_parts() {
        let request = ChatRequest::new("vision-model").with_message(ChatMessage::user_with_images(
            "inspect",
            vec![crate::ImageSource {
                media_type: "image/png".into(),
                data: Some("aGVsbG8=".into()),
                url: None,
            }],
        ));
        let body = build_request_body(&request);
        assert_eq!(body["messages"][0]["content"][0]["type"], "text");
        assert_eq!(body["messages"][0]["content"][1]["type"], "image_url");
        assert_eq!(
            body["messages"][0]["content"][1]["image_url"]["url"],
            "data:image/png;base64,aGVsbG8="
        );
    }

    #[test]
    fn openai_request_sends_trusted_remote_image_url_verbatim() {
        // url 图片直发远程地址（旧 `appendImageUrlPart` url 分支）。
        let request = ChatRequest::new("vision-model").with_message(ChatMessage::user_with_images(
            "inspect",
            vec![crate::ImageSource {
                media_type: "image/png".into(),
                data: None,
                url: Some(
                    "https://bkt.oss.example.com/zhikuncode-artifacts/clipboard/a.png".into(),
                ),
            }],
        ));
        let body = build_request_body(&request);
        assert_eq!(body["messages"][0]["content"][1]["type"], "image_url");
        assert_eq!(
            body["messages"][0]["content"][1]["image_url"]["url"],
            "https://bkt.oss.example.com/zhikuncode-artifacts/clipboard/a.png"
        );
        // 载荷与 url 皆无的图片按旧 return 语义整块跳过（只剩 text part）。
        let empty = ChatRequest::new("vision-model").with_message(ChatMessage::user_with_images(
            "inspect",
            vec![crate::ImageSource {
                media_type: "image/png".into(),
                data: None,
                url: None,
            }],
        ));
        let body = build_request_body(&empty);
        assert_eq!(
            body["messages"][0]["content"].as_array().map(Vec::len),
            Some(1)
        );
    }
    #[test]
    fn openrouter_streamed_reasoning_reassembles_only_same_identity() {
        let fragments = vec![
            json!({"type":"openrouter_reasoning","reasoning_details":[{"type":"reasoning.text","index":0,"id":"a","text":"first "}]}),
            json!({"type":"openrouter_reasoning","reasoning_details":[{"type":"reasoning.text","index":0,"id":"a","text":"second"},{"type":"reasoning.encrypted","id":"sealed","data":"a"}]}),
            json!({"type":"openrouter_reasoning","reasoning_details":[{"type":"reasoning.text","index":0,"id":"b","text":"distinct"},{"type":"reasoning.encrypted","id":"sealed","data":"b"}]}),
        ];
        let merged = super::merge_openrouter_reasoning(&fragments);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0]["text"], "first second");
        assert_eq!(merged[1]["data"], "ab");
        assert_eq!(merged[2]["text"], "distinct");
    }

    #[test]
    fn kimi_inline_payload_is_rechecked_after_url_conversion() {
        let mut request = ChatRequest::new("kimi-k3");
        let capacity = crate::models::capabilities_for("kimi-k3").context_window as usize;
        request.messages.push(crate::ChatMessage::user_with_images(
            "inspect",
            vec![crate::ImageSource {
                media_type: "image/png".into(),
                data: Some("A".repeat(capacity)),
                url: None,
            }],
        ));
        assert!(
            super::ensure_inline_image_budget(&request)
                .unwrap_err()
                .to_string()
                .contains("IMAGE_BUDGET_EXCEEDED")
        );
        assert_eq!(
            request.messages[0].images[0].data.as_ref().unwrap().len(),
            capacity
        );
        request.messages[0].images[0].data = Some("small".into());
        assert!(super::ensure_inline_image_budget(&request).is_ok());
    }
    #[test]
    fn explicit_effort_and_stop_override_only_the_requested_wire_options() {
        let mut request = ChatRequest::new("deepseek-flash").with_thinking(ThinkingMode::Enabled);
        let default = build_provider_request(&request, "deepseek");
        assert_eq!(default["reasoning_effort"], "max");
        assert!(default.get("stop").is_none());
        request.reasoning_effort = Some(crate::ReasoningEffort::Low);
        request.stop_sequences = vec![" END ".into(), "完成".into()];
        crate::validate_request_options(&request, "deepseek").unwrap();
        let body = build_provider_request(&request, "deepseek");
        assert_eq!(body["reasoning_effort"], "low");
        assert_eq!(body["thinking"]["type"], "enabled");
        assert_eq!(body["stop"], json!([" END ", "完成"]));
        assert_eq!(body["max_tokens"], default["max_tokens"]);
        request.model = "openrouter/openai/gpt-6-astra".into();
        request.reasoning_effort = Some(crate::ReasoningEffort::Max);
        crate::validate_request_options(&request, "openrouter").unwrap();
        let body = build_provider_request(&request, "openrouter");
        assert_eq!(body["reasoning"]["effort"], "max");
        assert!(body.get("reasoning_effort").is_none());
    }
}
