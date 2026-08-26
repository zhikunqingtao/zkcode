//! zk-llm：zkcode LLM 接入层——`ChatProvider` 抽象与 `OpenAI` 兼容 SSE 流式解析。
//!
//! 9-crate 拓扑定位：位于 zk-protocol 之上的适配 crate，被 zk-engine 依赖
//!（消费 `ChatRequest`、产出 `ProviderEvent` 流式增量）。遵循 D2：dashscope
//! 主通道 + `OpenAI` 兼容回退，配置驱动 endpoint / apiKey / model 三元组。
//!
//! # S6 + 2.2 交付范围
//!
//! - [`ChatProvider`]：object-safe trait（同步方法返回 `BoxStream`，见
//!   [`provider`] 模块文档的形态裁决 D-S6-1）；
//! - [`openai_compat`]：`OpenAI` 兼容实现——请求构建（逐字段对照旧
//!   `OpenAiCompatibleProvider.buildOpenAiRequest`，含 tools / `tool_calls` /
//!   tool 角色三分支）、增量 SSE 解析（`data:` 行 / `[DONE]` /
//!   `reasoning_content` / `tool_calls` delta 分片累积 / `finish_reason`
//!   归一化 / usage 尾块）、协作取消（`CancellationToken`）；
//! - `tool_calls` 增量事件族（Phase 2.2 契约冻结）：[`ProviderEvent::ToolUseStart`]
//!   / [`ProviderEvent::ToolInputDelta`]（对照旧 `ToolCallAccumulator` 语义，
//!   引擎在 Finish 时统一 flush，见 docs/compatibility.md §3）；
//! - [`config`]：双提供商注册（dashscope + openai），共享 reqwest client
//!   单例（connect 10s / read 10min / pool 5×idle 30s）；
//! - [`secret`]：`ApiKey` 脱敏 newtype（Debug 恒为占位符，无 Display）；
//! - [`error`]：可重试 / 致命二分（429 / 5xx / 网络 → 可重试；401 / 403 /
//!   400 / 解析 / 配置 → 致命）。
//!
//! # Phase 2+ 待办（本 crate 留白）
//!
//! - 多模态 `content` 数组与图片块转换（旧 `MessageParamConverter` 的图片
//!   分支）；assistant 历史 `reasoning_content` 回传；
//! - `Anthropic` 原生协议 Provider（方案 D4：独立实现）与 8 家配置面全量；
//! - API 密钥轮换 / 熔断 / 模型降级链（旧 `ApiKeyRotationManager` 等）。
//!
//! # 模块导航
//!
//! - [`provider`]：`ChatProvider` trait + `ChatRequest` / `ProviderEvent` /
//!   `FinishReason`（归一化）。
//! - [`openai_compat`]：`OpenAiCompatProvider` + `sse_event_stream` +
//!   共享 HTTP client。
//! - [`config`]：`ProviderConfig` / `PROVIDER_CATALOG`（8+1 家声明目录 + 环境
//!   变量扫描装配）。
//! - [`registry`]：`ProviderRegistry`（model → provider 路由 + 熔断 + 降级链，
//!   自身即 `ChatProvider`）。
//! - [`anthropic`]：`AnthropicProvider`（原生 Messages API + 六类 SSE 事件）。
//! - [`cache`]：prompt caching 缓存断点（`SystemPrompt` 分段 + 工具定义内建/MCP
//!   分界下标折算；注入面为 `Anthropic` 专属）。
//! - [`breaker`]：`CircuitBreaker`（连续 3 次 5xx → 30s degraded）。
//! - [`retry`]：`RetryPolicy` / `RetryState` / `wait_for_retry`（同 provider
//!   指数退避 + 25% jitter + `Retry-After`，对照旧 `ApiRetryService`）。
//! - [`models`]：内置模型能力表（`ModelCapabilities` / `capabilities_for` /
//!   `max_output_tokens_for`，对照旧 `ModelRegistry.BUILTIN_MODELS`）。
//! - [`vision_router`]：视觉模型路由（图片消息 + 当前模型不支持图片时的
//!   单请求级模型切换，对照旧 `VisionModelRouter`）。
//! - [`secret`]：`ApiKey`。
//! - [`error`]：`ProviderError`。

pub mod anthropic;
pub mod breaker;
pub mod cache;
mod clock;
pub mod config;
pub mod error;
pub mod models;
pub mod openai_compat;
pub mod provider;
pub mod registry;
pub mod retry;
pub mod secret;
pub mod swappable;
pub mod vision_router;

pub use anthropic::AnthropicProvider;
pub use breaker::{BreakerState, CircuitBreaker, DEGRADE_WINDOW_MS, FAILURE_THRESHOLD};
pub use cache::{
    EPHEMERAL, SystemPrompt, SystemPromptSegment, ToolOrigin, ephemeral_cache_control,
    first_mcp_index, tool_cache_breakpoint,
};
pub use config::{
    ANTHROPIC_BASE_URL, DASHSCOPE_BASE_URL, DEFAULT_MODEL, FALLBACK_CHAIN_ENV, OPENAI_BASE_URL,
    PROVIDER_CATALOG, PROVIDER_ENV_PREFIX, ProviderConfig, ProviderProtocol, declared_models,
    has_provider_env, provider_configs_from_env,
};
pub use error::ProviderError;
pub use models::{
    BUILTIN_MODELS, DEFAULT_CAPABILITIES, ModelCapabilities, capabilities_for, is_known_model,
    max_output_tokens_for,
};
pub use openai_compat::{OpenAiCompatProvider, shared_http_client, sse_event_stream};
pub use provider::{
    ChatMessage, ChatProvider, ChatRequest, FinishReason, ImageSource, ProviderEvent, Role,
    ThinkingMode, ToolCallRequest, ToolSpec,
};
pub use registry::{MAX_FALLBACK_DEPTH, ProviderRegistry};
pub use retry::{
    BASE_DELAY_MS, CAPACITY_LIMIT_STATUS, DEFAULT_MAX_RETRIES, FOREGROUND_529_RETRY_SOURCES,
    FOREGROUND_QUERY_SOURCE, MAX_529_RETRIES, MAX_DELAY_MS, ModelRetryConfig, RetryPolicy,
    RetryState, retry_config_for, wait_for_retry,
};
pub use secret::{ApiKey, ApiKeyRing, KEY_COOLDOWN_MS};
pub use swappable::SwappableProvider;
pub use vision_router::{
    DEEPSEEK_VISION_MODEL, FALLBACK_VISION_MODEL, VisionProviderView, resolve_vision_model,
};

/// 骨架连续性测试：验证 crate 编译、workspace lints 接线与 test harness 加载。
#[test]
fn crate_boots() {
    // 模块树完整性：核心类型均可命名（编译期即验证，此处保底运行期引用）。
    let _ = Role::User.as_str();
    let request = ChatRequest::new("qwen3.7-max");
    assert_eq!(request.model, "qwen3.7-max");
    assert_eq!(
        FinishReason::from_openai("stop").as_str(),
        "end_turn",
        "finish_reason normalization must stay wired"
    );
}
