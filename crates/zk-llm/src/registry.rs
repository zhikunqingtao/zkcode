//! 多提供商注册表——model → provider 路由 + 熔断 + 模型降级链（2.7）。
//!
//! # 形态裁决（D-P2-3 延伸）
//!
//! [`ProviderRegistry`] **自身实现 [`ChatProvider`]**：引擎侧注入类型仍是
//! `Arc<dyn ChatProvider>`，zk-engine 零改动即获得多提供商路由、熔断与降级
//! 能力（`ChatRequest.model` 就是路由键）。
//!
//! # 路由规则
//!
//! 1. `model` 命中 [`ProviderRegistry::model_owner`] → 该 provider；
//! 2. 未命中（未知模型 / Phase 1 单 provider 回退）→ 默认模型归属 provider，
//!    再退化为首个注册 provider（[`ProviderRegistry::resolve_provider`]）——
//!    保住「仅配 `ZK_LLM_BASE_URL` + `ZK_LLM_API_KEY`」时的 S9 行为不变；
//! 3. 熔断 `Degraded` 的候选在存在其他候选时被跳过；**全部降级则 fail-open**
//!    （宁可打一次可能失败的请求，也不凭空拒绝用户对话）。
//!
//! # 模型降级链（`ZK_MODEL_FALLBACK_CHAIN`）
//!
//! 链是**模型**序列（如 `kimi-k3:qwen3.8-max-0902:deepseek-chat`）。候选序 =
//! 请求模型 + 链中其后继（请求模型不在链中时接整条链），去重后截断到
//! [`MAX_FALLBACK_DEPTH`]。降级触发条件（三者同时成立）：
//!
//! - 错误可重试（[`crate::error::ProviderError::is_retryable`]：429 / 5xx /
//!   网络层）；
//! - 该次尝试**尚未产出任何内容事件**（text / thinking / 工具增量）——已经
//!   吐字后切换模型会拼出两个模型的混合回答；
//! - 仍有后继候选。
//!
//! 降级发生时**不向下游产出**触发错误（对上层表现为一次成功对话）；无可用
//! 后继时才把挂起的错误作为 [`ProviderEvent::Error`] 吐出。
//!
//! # 同 provider 重试（对照旧 `ApiRetryService`）
//!
//! 切换候选**之前**先在同一候选上重试：错误可重试、该次尝试尚未吐字、
//! 重试预算未尽且该 provider 未熔断时，按 [`crate::retry::RetryPolicy`]
//!（500ms × 2^n + 25% jitter，429 遵循 `Retry-After`，上限 30s）等待后
//! 重新发起同一 provider 的流。三条协作语义（均对齐旧实现）：
//!
//! - **取消优先**：退避等待用 `biased select!`，取消令牌先于定时器——取消
//!   即静默终止流，且**不**计熔断失败（取消是控制流，不是 provider 故障）；
//! - **不重试已熔断的 provider**：重试前查 `is_available()`，`false` 则直接
//!   走降级/上抛，不把退避时间浪费在已降级的提供商上；
//! - **熔断失败每次逻辑调用只记一次**：内层重试不重复消耗熔断配额，只在
//!   放弃重试的终态分支 `record_error`（旧 `recordFailure` 同）。
//!
//! # 密钥安全
//!
//! 注册表只持有 [`crate::config::ProviderConfig`] 构造出的 provider 实例，
//! 密钥留在各 provider 的 [`crate::secret::ApiKeyRing`] 内（Debug 脱敏）；
//! 注册表自身的 `Debug` 只输出 provider 名与模型面。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, OnceLock};

use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::anthropic::AnthropicProvider;
use crate::breaker::{BreakerState, CircuitBreaker};
use crate::config::{
    DEFAULT_MODEL, ProviderConfig, ProviderProtocol, fallback_chain_from_env,
    provider_configs_from_env,
};
use crate::error::ProviderError;
use crate::ledger::{LlmCallFinished, LlmCallObserver, LlmCallStarted, LlmCallStatus};
use crate::openai_compat::{OpenAiCompatProvider, shared_http_client};
use crate::provider::{ChatProvider, ChatRequest, ProviderEvent};
use crate::retry::{RetryPolicy, RetryState, wait_for_retry};

/// 降级链最大深度（含首选，含首选共 3 次尝试——对齐 2.7 规格示例长度）。
pub const MAX_FALLBACK_DEPTH: usize = 3;
/// Default cap for providers in the built-in, production-verified catalog.
pub const VERIFIED_PROVIDER_CONCURRENCY: usize = 4;
/// Conservative cap for custom or otherwise unverified provider adapters.
pub const UNKNOWN_PROVIDER_CONCURRENCY: usize = 2;

/// A completion callback may momentarily lose a race with `SQLite` checkpointing
/// or another process releasing a database lock.  Every observer is required to
/// be idempotent by `call_id`, so retrying the exact same immutable completion is
/// safe.  Keep this deliberately short and bounded: graceful shutdown performs
/// a final durable reconciliation of any call which is still `started`.
const COMPLETION_PERSIST_ATTEMPTS: usize = 4;
const COMPLETION_RETRY_DELAYS_MS: [u64; COMPLETION_PERSIST_ATTEMPTS - 1] = [10, 25, 50];

/// Observer persistence must outlive short-lived caller runtimes (notably the
/// current-thread runtime used by summarization). A single process-lifetime
/// executor owns every admission, completion, and Drop caretaker task; this
/// avoids both per-call OS threads and caller-runtime teardown races.
static LIFECYCLE_EXECUTOR: OnceLock<Result<Handle, String>> = OnceLock::new();

fn start_lifecycle_executor() -> Result<Handle, String> {
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("zk-llm-lifecycle".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ =
                        ready_tx.send(Err(format!("LLM_LIFECYCLE_RUNTIME_BUILD_FAILED: {error}")));
                    return;
                }
            };
            let handle = runtime.handle().clone();
            if ready_tx.send(Ok(handle)).is_err() {
                return;
            }
            runtime.block_on(std::future::pending::<()>());
        })
        .map_err(|error| format!("LLM_LIFECYCLE_THREAD_START_FAILED: {error}"))?;
    ready_rx
        .recv()
        .map_err(|error| format!("LLM_LIFECYCLE_THREAD_DIED: {error}"))?
}

fn lifecycle_executor() -> Result<Handle, String> {
    LIFECYCLE_EXECUTOR
        .get_or_init(start_lifecycle_executor)
        .clone()
}

async fn persist_completion_with_retry(
    observer: &Arc<dyn LlmCallObserver>,
    completion: &LlmCallFinished,
) -> Result<(), String> {
    let mut last_error = None;
    for attempt in 0..COMPLETION_PERSIST_ATTEMPTS {
        match observer.call_finished(completion.clone()).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                last_error = Some(error);
                if let Some(delay_ms) = COMPLETION_RETRY_DELAYS_MS.get(attempt) {
                    tracing::warn!(
                        call_id = %completion.call_id,
                        attempt = attempt + 1,
                        "physical LLM completion persistence failed; retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(*delay_ms)).await;
                }
            }
        }
    }
    Err(last_error.unwrap_or_else(|| "LLM_COMPLETION_PERSIST_FAILED".to_owned()))
}

fn provider_concurrency_limit(name: &str) -> usize {
    if crate::config::catalog_entry(name).is_some() {
        VERIFIED_PROVIDER_CONCURRENCY
    } else {
        UNKNOWN_PROVIDER_CONCURRENCY
    }
}

/// 注册表内的单 provider 槽位（实例 + 独立熔断器）。
#[derive(Clone)]
struct ProviderSlot {
    provider: Arc<dyn ChatProvider>,
    breaker: Arc<CircuitBreaker>,
    concurrency: Arc<Semaphore>,
}

/// 多提供商注册表（自身即 [`ChatProvider`]）。
#[derive(Default)]
pub struct ProviderRegistry {
    providers: HashMap<String, ProviderSlot>,
    order: Vec<String>,
    model_index: HashMap<String, String>,
    model_order: Vec<String>,
    default_model: String,
    fallback_chain: Vec<String>,
    retry_policy: RetryPolicy,
}

impl std::fmt::Debug for ProviderRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderRegistry")
            .field("providers", &self.order)
            .field("models", &self.model_order)
            .field("model_index", &self.model_index)
            .field("default_model", &self.default_model)
            .field("fallback_chain", &self.fallback_chain)
            .field("retry_policy", &self.retry_policy)
            .finish()
    }
}

impl ProviderRegistry {
    /// 空注册表（默认模型取 [`DEFAULT_MODEL`]）。
    #[must_use]
    pub fn new() -> Self {
        Self {
            default_model: DEFAULT_MODEL.to_owned(),
            // WebSocket 对话即旧 `repl_main_thread` 前台源（529 可重试）。
            retry_policy: RetryPolicy::foreground(),
            ..Self::default()
        }
    }

    /// 按配置列表装配（共享 HTTP client 单例）。
    ///
    /// 逐条按 [`ProviderProtocol`] 分派实现：`OpenAiCompat` →
    /// [`OpenAiCompatProvider`]，`AnthropicNative` → [`AnthropicProvider`]。
    /// 默认模型取**首条**配置的 `default_model`（配置为空时 [`DEFAULT_MODEL`]）。
    ///
    /// # Errors
    ///
    /// 共享 client 构建失败（TLS 初始化异常等）返回 [`ProviderError::Config`]。
    pub fn from_configs(configs: Vec<ProviderConfig>) -> Result<Self, ProviderError> {
        let client = shared_http_client()?;
        Ok(Self::from_configs_with_client(configs, &client))
    }

    /// 按配置列表装配（注入 client——单测与共享单例复用入口）。
    #[must_use]
    pub fn from_configs_with_client(
        configs: Vec<ProviderConfig>,
        client: &reqwest::Client,
    ) -> Self {
        let mut registry = Self::new();
        let mut default_seen = false;
        for config in configs {
            let name = config.name.clone();
            let models = config.models.clone();
            let default_model = config.default_model.clone();
            let provider: Arc<dyn ChatProvider> = match config.protocol {
                ProviderProtocol::OpenAiCompat => {
                    Arc::new(OpenAiCompatProvider::with_client(config, client.clone()))
                }
                ProviderProtocol::AnthropicNative => {
                    Arc::new(AnthropicProvider::with_client(config, client.clone()))
                }
            };
            registry.register(name, provider, models);
            if !default_seen && !default_model.is_empty() {
                registry.default_model = default_model;
                default_seen = true;
            }
        }
        registry
    }

    /// 环境变量装配（`LLM_PROVIDER_*` 扫描 + `ZK_MODEL_FALLBACK_CHAIN`）。
    ///
    /// 未配置任何 `LLM_PROVIDER_<NAME>_API_KEY` 时返回**空注册表**——调用方
    /// 据此走 Phase 1 单 provider 回退（见 [`crate::config`] 模块文档）。
    ///
    /// # Errors
    ///
    /// 共享 client 构建失败返回 [`ProviderError::Config`]。
    pub fn from_env() -> Result<Self, ProviderError> {
        let configs = provider_configs_from_env();
        Ok(Self::from_configs(configs)?.with_fallback_chain(fallback_chain_from_env()))
    }

    /// 注册一个 provider 实例及其模型归属（注册序即展示序；模型首个归属者胜）。
    pub fn register(
        &mut self,
        name: impl Into<String>,
        provider: Arc<dyn ChatProvider>,
        models: Vec<String>,
    ) {
        let name = name.into();
        if !self.providers.contains_key(&name) {
            self.order.push(name.clone());
        }
        self.providers.insert(
            name.clone(),
            ProviderSlot {
                provider,
                breaker: Arc::new(CircuitBreaker::new()),
                concurrency: Arc::new(Semaphore::new(provider_concurrency_limit(&name))),
            },
        );
        for model in models {
            if model.trim().is_empty() {
                continue;
            }
            if !self.model_index.contains_key(&model) {
                self.model_order.push(model.clone());
                self.model_index.insert(model, name.clone());
            }
        }
    }

    /// 设置默认模型（链式；空串忽略）。
    #[must_use]
    pub fn with_default_model(mut self, model: impl Into<String>) -> Self {
        let model = model.into();
        if !model.trim().is_empty() {
            self.default_model = model;
        }
        self
    }

    /// 设置模型降级链（链式）。
    #[must_use]
    pub fn with_fallback_chain(mut self, chain: Vec<String>) -> Self {
        self.fallback_chain = chain;
        self
    }

    /// 设置同 provider 重试策略（链式；默认为旧 `ApiRetryService` 常量面）。
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// 当前同 provider 重试策略。
    #[must_use]
    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry_policy
    }

    /// 当前实际可用的默认模型。
    ///
    /// 保持既有调用点也不会取得已下线的配置值；新代码可使用语义更明确的
    /// [`Self::effective_default_model`]。
    #[must_use]
    pub fn default_model(&self) -> &str {
        self.effective_default_model()
    }

    /// 模型是否可被当前注册表接受。
    ///
    /// 一旦 provider 声明了模型清单便严格按清单校验；模型清单为空时保留
    /// Phase 1 单 provider 的自定义模型兼容（仅拒绝空白模型）。
    #[must_use]
    pub fn supports_model(&self, model: &str) -> bool {
        !model.trim().is_empty()
            && (self.model_order.is_empty() || self.model_index.contains_key(model))
    }

    /// 当前实际可用的默认模型。
    ///
    /// 配置默认值仍在已注册清单中时原样返回；配置已过期/下线时回退到注册序
    /// 首个模型。模型清单为空时保留 Phase 1 自定义默认模型。
    #[must_use]
    pub fn effective_default_model(&self) -> &str {
        if self.model_order.is_empty() && self.default_model.trim().is_empty() {
            DEFAULT_MODEL
        } else if self.model_order.is_empty() || self.model_index.contains_key(&self.default_model)
        {
            &self.default_model
        } else {
            self.model_order
                .first()
                .map_or(DEFAULT_MODEL, String::as_str)
        }
    }

    /// 模型降级链（配置原序）。
    #[must_use]
    pub fn fallback_chain(&self) -> &[String] {
        &self.fallback_chain
    }

    /// 已注册 provider 名（注册序）。
    #[must_use]
    pub fn names(&self) -> &[String] {
        &self.order
    }

    /// 已注册 provider 数。
    #[must_use]
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    /// 是否无任何 provider（→ 调用方走 Phase 1 回退）。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    /// 按名取 provider 实例。
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Arc<dyn ChatProvider>> {
        self.providers.get(name).map(|slot| slot.provider.clone())
    }

    /// 聚合模型清单（provider 注册序 × 各自 models 序，保序去重）。
    ///
    /// `GET /api/models` 的动态数据源；未配密钥的 provider 不在其中。
    #[must_use]
    pub fn models(&self) -> &[String] {
        &self.model_order
    }

    /// 模型的**严格**归属 provider（未注册该模型时 `None`）。
    #[must_use]
    pub fn model_owner(&self, model: &str) -> Option<&str> {
        self.model_index.get(model).map(String::as_str)
    }

    /// 模型的路由归属（严格命中 → 默认模型归属 → 首个注册 provider）。
    #[must_use]
    pub fn resolve_provider(&self, model: &str) -> Option<&str> {
        if let Some(name) = self.model_owner(model) {
            return Some(name);
        }
        self.model_owner(self.effective_default_model())
            .or_else(|| self.order.first().map(String::as_str))
    }

    /// 某 provider 的熔断器（诊断 / 单测入口）。
    #[must_use]
    pub fn breaker(&self, name: &str) -> Option<Arc<CircuitBreaker>> {
        self.providers.get(name).map(|slot| slot.breaker.clone())
    }

    /// 某 provider 的熔断状态。
    #[must_use]
    pub fn breaker_state(&self, name: &str) -> Option<BreakerState> {
        self.providers.get(name).map(|slot| slot.breaker.state())
    }

    /// 候选模型序（请求模型 + 降级链后继，去重截断到 [`MAX_FALLBACK_DEPTH`]）。
    #[must_use]
    pub fn candidate_models(&self, model: &str) -> Vec<String> {
        let mut models = vec![model.to_owned()];
        let start = self
            .fallback_chain
            .iter()
            .position(|candidate| candidate == model)
            .map_or(0, |index| index + 1);
        for next in self.fallback_chain.iter().skip(start) {
            if models.len() >= MAX_FALLBACK_DEPTH {
                break;
            }
            if !models.contains(next) {
                models.push(next.clone());
            }
        }
        models
    }

    /// 候选槽位序（路由 + 熔断过滤；全部降级时 fail-open 返回未过滤序列）。
    fn candidates_for(&self, model: &str) -> Vec<Candidate> {
        let mut candidates: Vec<Candidate> = Vec::new();
        for model in self.candidate_models(model) {
            let Some(name) = self.resolve_provider(&model) else {
                continue;
            };
            let Some(slot) = self.providers.get(name) else {
                continue;
            };
            if candidates
                .iter()
                .any(|existing| existing.model == model && existing.provider_name == name)
            {
                continue;
            }
            candidates.push(Candidate {
                model,
                provider_name: name.to_owned(),
                provider: slot.provider.clone(),
                breaker: slot.breaker.clone(),
                concurrency: Arc::clone(&slot.concurrency),
                retry_policy: self.retry_policy.clone(),
            });
        }
        let available: Vec<Candidate> = candidates
            .iter()
            .filter(|candidate| candidate.breaker.is_available())
            .cloned()
            .collect();
        if available.is_empty() {
            candidates
        } else {
            available
        }
    }
}

/// 单次尝试的候选（模型 + provider 实例 + 其熔断器）。
#[derive(Clone)]
struct Candidate {
    model: String,
    provider_name: String,
    provider: Arc<dyn ChatProvider>,
    breaker: Arc<CircuitBreaker>,
    concurrency: Arc<Semaphore>,
    retry_policy: RetryPolicy,
}

impl Candidate {
    fn physical_request(&self, request: &ChatRequest) -> Result<ChatRequest, ProviderError> {
        let mut physical = request.clone();
        physical.model.clone_from(&self.model);

        // The root model was validated before the registry was called, but a
        // fallback is a different execution target. Re-run every capability
        // check that affects the wire request; otherwise tool/vision/thinking
        // support here used to be inherited incorrectly from the failed model.
        if self.model != request.model {
            let capabilities = crate::capabilities_for(&self.model);
            let image_count = request
                .messages
                .iter()
                .try_fold(0_u32, |total, message| {
                    let count = u32::try_from(message.images.len()).ok()?;
                    total.checked_add(count)
                })
                .ok_or_else(|| ProviderError::Config {
                    message: "FALLBACK_IMAGE_COUNT_OVERFLOW".to_owned(),
                })?;
            if image_count > 0
                && (!capabilities.supports_images || image_count > capabilities.max_images)
            {
                return Err(ProviderError::Config {
                    message: format!(
                        "FALLBACK_CAPABILITY_MISMATCH: model '{}' cannot accept {image_count} images",
                        self.model
                    ),
                });
            }
            if !request.tools.is_empty() && !capabilities.supports_tool_use {
                return Err(ProviderError::Config {
                    message: format!(
                        "FALLBACK_CAPABILITY_MISMATCH: model '{}' does not support tools",
                        self.model
                    ),
                });
            }
            if request.thinking.requires_support() && !capabilities.supports_thinking {
                return Err(ProviderError::Config {
                    message: format!(
                        "FALLBACK_CAPABILITY_MISMATCH: model '{}' does not support thinking",
                        self.model
                    ),
                });
            }
            // Output limits belong to the physical model, not the originally
            // requested model. Budget gates may lower this further through the
            // physical-call observer before network execution.
            physical.max_tokens = physical.max_tokens.min(capabilities.max_output_tokens);
        }
        Ok(physical)
    }

    /// 以候选模型改写请求并发起流（建立期失败原样上抛）。
    fn start(
        &self,
        request: &ChatRequest,
        cancel: &CancellationToken,
        physical_attempt: u32,
        reason: &'static str,
    ) -> Result<Attempt, ProviderError> {
        let requested_model = request.model.clone();
        let observer = request.call_observer.clone();
        let pending_start = request.execution.clone().zip(observer.as_ref()).map(
            |(attribution, _)| LlmCallStarted {
                call_id: uuid::Uuid::new_v4().to_string(),
                attribution,
                provider: self.provider_name.clone(),
                model: self.model.clone(),
                route: serde_json::json!({
                    "kind": request.execution.as_ref().map_or("unknown", |value| value.kind.as_str()),
                    "requestedModel": requested_model,
                    "reason": reason,
                    "physicalAttempt": physical_attempt,
                })
                .to_string(),
                provider_request_id: None,
            },
        );
        let call_id = pending_start.as_ref().map(|call| call.call_id.clone());
        let physical_request = self.physical_request(request)?;
        // An observed request is admitted durably before the concrete stream is
        // constructed. Providers are allowed to start network I/O eagerly from
        // `chat_stream`, so delaying only the first stream poll is not sufficient.
        let (stream, pending_provider_start) = if pending_start.is_some() {
            (
                None,
                Some(PendingProviderStart {
                    provider: Arc::clone(&self.provider),
                    request: physical_request,
                    cancel: cancel.clone(),
                }),
            )
        } else {
            (
                Some(
                    self.provider
                        .chat_stream(physical_request, cancel.clone())?,
                ),
                None,
            )
        };
        Ok(Attempt {
            stream,
            pending_provider_start,
            breaker: self.breaker.clone(),
            produced_content: false,
            candidate: self.clone(),
            retry: RetryState::new(self.retry_policy.clone(), self.model.clone()),
            observer,
            pending_start,
            start_task: None,
            terminal_task: None,
            lifecycle_runtime: None,
            call_id,
            usage: None,
            last_error: None,
            lookahead_event: None,
            deferred_terminal_events: VecDeque::new(),
            concurrency: Arc::clone(&self.concurrency),
            permit: None,
            started: false,
            finished: false,
            model: self.model.clone(),
        })
    }
}

/// 进行中的一次尝试（含重启所需的候选与跨重试累计的重试账本）。
struct Attempt {
    stream: Option<BoxStream<'static, ProviderEvent>>,
    pending_provider_start: Option<PendingProviderStart>,
    breaker: Arc<CircuitBreaker>,
    produced_content: bool,
    candidate: Candidate,
    retry: RetryState,
    observer: Option<Arc<dyn LlmCallObserver>>,
    pending_start: Option<LlmCallStarted>,
    /// Independently-owned admission write. Awaiting the handle is cancel-safe:
    /// dropping the registry stream transfers it to [`Drop`] instead of aborting
    /// a database write which may already have crossed its commit point.
    start_task: Option<JoinHandle<Result<(), String>>>,
    /// The one immutable terminal write selected at the lifecycle linearization
    /// point. Once present, neither cancellation nor [`Drop`] may replace its
    /// payload with a different status.
    terminal_task: Option<JoinHandle<Result<(), String>>>,
    lifecycle_runtime: Option<Handle>,
    call_id: Option<String>,
    usage: Option<zk_protocol::Usage>,
    /// Last provider error already exposed downstream without switching the
    /// physical attempt. EOF turns it into Failed; a later Finish supersedes it.
    last_error: Option<ProviderError>,
    /// A single provider event read ahead to distinguish a recoverable stream
    /// error from an error which is immediately followed by physical EOF.
    lookahead_event: Option<ProviderEvent>,
    /// `Finish` is a model-generation boundary, not necessarily the physical
    /// response boundary. OpenAI-compatible providers may append one or more
    /// usage-only chunks before EOF, so terminal events stay hidden until the
    /// observer has durably recorded the final (last-wins) usage snapshot.
    deferred_terminal_events: VecDeque<ProviderEvent>,
    concurrency: Arc<Semaphore>,
    permit: Option<OwnedSemaphorePermit>,
    started: bool,
    finished: bool,
    model: String,
}

struct PendingProviderStart {
    provider: Arc<dyn ChatProvider>,
    request: ChatRequest,
    cancel: CancellationToken,
}

impl Attempt {
    async fn ensure_started(&mut self, cancel: &CancellationToken) -> Result<bool, String> {
        if self.permit.is_none() {
            let permit = tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(false),
                permit = Arc::clone(&self.concurrency).acquire_owned() => permit
                    .map_err(|_| "PROVIDER_CONCURRENCY_CLOSED".to_owned())?,
            };
            self.permit = Some(permit);
        }
        if let Some(started) = self.pending_start.take() {
            let Some(observer) = self.observer.clone() else {
                return Err("LLM_LEDGER_START_OBSERVER_MISSING".to_owned());
            };
            let runtime = lifecycle_executor()
                .map_err(|error| format!("LLM_LEDGER_START_EXECUTOR_UNAVAILABLE: {error}"))?;
            self.lifecycle_runtime = Some(runtime.clone());
            self.start_task =
                Some(runtime.spawn(async move { observer.call_started(started).await }));
        }
        if self.start_task.is_some() {
            let joined = {
                let task = self
                    .start_task
                    .as_mut()
                    .expect("start task checked immediately above");
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return Ok(false),
                    result = task => result,
                }
            };
            self.start_task.take();
            match joined {
                Ok(Ok(())) => self.started = true,
                Ok(Err(error)) => return Err(error),
                Err(error) => return Err(format!("LLM_LEDGER_START_TASK_FAILED: {error}")),
            }
        }
        if let Some(pending) = self.pending_provider_start.take() {
            self.stream = Some(
                match pending
                    .provider
                    .chat_stream(pending.request, pending.cancel)
                {
                    Ok(stream) => stream,
                    Err(error) => {
                        futures::stream::once(async move { ProviderEvent::Error { error } }).boxed()
                    }
                },
            );
        }
        Ok(true)
    }

    async fn finish(
        &mut self,
        status: LlmCallStatus,
        error_code: Option<String>,
    ) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        if self.terminal_task.is_none() {
            let Some(observer) = self.observer.clone() else {
                self.finished = true;
                self.permit.take();
                return Ok(());
            };
            let Some(call_id) = self.call_id.clone() else {
                self.finished = true;
                self.permit.take();
                return Ok(());
            };
            let completion = LlmCallFinished {
                call_id,
                model: self.model.clone(),
                status,
                usage: self.usage,
                error_code,
            };
            let runtime = match self.lifecycle_runtime.clone() {
                Some(runtime) => runtime,
                None => lifecycle_executor()
                    .map_err(|error| format!("LLM_LEDGER_FINISH_EXECUTOR_UNAVAILABLE: {error}"))?,
            };
            self.lifecycle_runtime = Some(runtime.clone());
            // Linearization point: from this assignment onward the immutable
            // completion above is the sole terminal payload for this call. Drop
            // may await this task, but must never manufacture a replacement.
            self.terminal_task =
                Some(runtime.spawn(async move {
                    persist_completion_with_retry(&observer, &completion).await
                }));
        }
        let joined = self
            .terminal_task
            .as_mut()
            .expect("terminal task initialized above")
            .await;
        self.terminal_task.take();
        self.permit.take();
        // This attempt has exhausted its single bounded persistence duty even
        // when the observer remains unavailable.  Do not start a second retry
        // batch from `Drop`; the execution fails closed and startup/shutdown
        // reconciliation converges the still-started durable row.
        self.finished = true;
        match joined {
            Ok(result) => result,
            Err(error) => Err(format!("LLM_LEDGER_FINISH_TASK_FAILED: {error}")),
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        let runtime = self
            .lifecycle_runtime
            .clone()
            .or_else(|| lifecycle_executor().ok());
        let permit = self.permit.take();

        if let Some(terminal_task) = self.terminal_task.take() {
            let call_id = self.call_id.clone().unwrap_or_else(|| "unknown".to_owned());
            let Some(runtime) = runtime else {
                // Dropping a Tokio JoinHandle detaches rather than aborts the
                // immutable terminal write, so ownership remains single even
                // though its result cannot be observed during runtime teardown.
                tracing::error!(%call_id, "terminal LLM write detached outside its Tokio runtime");
                drop(permit);
                drop(terminal_task);
                return;
            };
            runtime.spawn(async move {
                let _permit = permit;
                match terminal_task.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::error!(%call_id, %error, "failed to persist physical LLM completion after stream drop");
                    }
                    Err(error) => {
                        tracing::error!(%call_id, %error, "physical LLM completion task failed after stream drop");
                    }
                }
            });
            return;
        }

        let observer = self.observer.take();
        let call_id = self.call_id.take();
        let model = self.model.clone();
        let usage = self.usage;
        if let Some(start_task) = self.start_task.take() {
            let Some(runtime) = runtime else {
                tracing::error!("in-flight LLM start lost its caretaker during runtime teardown");
                drop(permit);
                drop(start_task);
                return;
            };
            runtime.spawn(async move {
                let _permit = permit;
                match start_task.await {
                    Ok(Ok(())) => {
                        let (Some(observer), Some(call_id)) = (observer, call_id) else {
                            tracing::error!("successful LLM start had no completion identity");
                            return;
                        };
                        let completion = LlmCallFinished {
                            call_id,
                            model,
                            status: LlmCallStatus::Cancelled,
                            usage,
                            error_code: Some("STREAM_DROPPED".to_owned()),
                        };
                        if let Err(error) =
                            persist_completion_with_retry(&observer, &completion).await
                        {
                            tracing::error!(%error, "failed to close dropped in-flight LLM start");
                        }
                    }
                    Ok(Err(_)) => {}
                    Err(error) => {
                        tracing::error!(%error, "physical LLM start task failed after stream drop");
                    }
                }
            });
            return;
        }

        if !self.started {
            return;
        }
        let (Some(runtime), Some(observer), Some(call_id)) = (runtime, observer, call_id) else {
            tracing::error!("started physical LLM call lost its drop completion identity");
            return;
        };
        let completion = LlmCallFinished {
            call_id,
            model,
            status: LlmCallStatus::Cancelled,
            usage,
            error_code: Some("STREAM_DROPPED".to_owned()),
        };
        runtime.spawn(async move {
            let _permit = permit;
            if let Err(error) = persist_completion_with_retry(&observer, &completion).await {
                tracing::error!(%error, "failed to persist dropped physical LLM call completion");
            }
        });
    }
}

/// 降级链流状态机的内部状态。
struct FallbackState {
    request: ChatRequest,
    cancel: CancellationToken,
    queue: VecDeque<Candidate>,
    current: Option<Attempt>,
    pending_error: Option<ProviderError>,
    /// Events released only after the physical completion observer commits.
    ready_events: VecDeque<ProviderEvent>,
    next_physical_attempt: u32,
}

impl ChatProvider for ProviderRegistry {
    // trait 签名固定为 `-> &str`（object-safe），此处返回定值不改签名。
    #[allow(clippy::unnecessary_literal_bound)]
    fn provider_name(&self) -> &str {
        "registry"
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        let candidates = self.candidates_for(&request.model);
        if candidates.is_empty() {
            return Err(ProviderError::Config {
                message: format!("no provider registered for model '{}'", request.model),
            });
        }
        let mut queue: VecDeque<Candidate> = candidates.into();
        let mut setup_error: Option<ProviderError> = None;
        let mut current: Option<Attempt> = None;
        // 首选的建立期失败按候选序继续尝试；全部失败才把最后一个错误上抛
        //（trait 契约：Err 仅覆盖建立期）。
        while let Some(candidate) = queue.pop_front() {
            let reason = if candidate.model == request.model {
                "primary"
            } else {
                "fallback"
            };
            match candidate.start(&request, &cancel, 1, reason) {
                Ok(attempt) => {
                    current = Some(attempt);
                    break;
                }
                Err(err) => setup_error = Some(err),
            }
        }
        let Some(current) = current else {
            return Err(setup_error.unwrap_or_else(|| ProviderError::Config {
                message: format!("no provider could serve model '{}'", request.model),
            }));
        };
        Ok(fallback_stream(FallbackState {
            request,
            cancel,
            queue,
            current: Some(current),
            pending_error: None,
            ready_events: VecDeque::new(),
            next_physical_attempt: 2,
        })
        .boxed())
    }
}

/// 降级链事件流：透传当前尝试的事件，必要时切换到后继候选。
// Retry, fallback, breaker accounting, durable call completion, and stream
// emission form one ordered state transition. Keeping them adjacent makes it
// possible to audit that every physical attempt is finished exactly once.
#[allow(clippy::too_many_lines)]
fn fallback_stream(state: FallbackState) -> impl Stream<Item = ProviderEvent> + Send + 'static {
    futures::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.ready_events.pop_front() {
                return Some((event, state));
            }
            if state.current.is_none() {
                // 无可用后继：把挂起的触发错误吐给下游，随后终止。
                let error = state.pending_error.take()?;
                return Some((ProviderEvent::Error { error }, state));
            }

            // Once Finish or a recoverable error is pending, cancellation still
            // wins until the physical stream reaches EOF and terminal state is
            // durable. Never release buffered success after cancellation.
            let (finish_pending, exposed_error_pending) =
                state.current.as_ref().map_or((false, false), |attempt| {
                    (
                        !attempt.deferred_terminal_events.is_empty(),
                        attempt.last_error.is_some(),
                    )
                });
            let cancellation_sensitive = finish_pending || exposed_error_pending;
            if cancellation_sensitive && state.cancel.is_cancelled() {
                let error = ProviderError::Cancelled;
                let persisted = state
                    .current
                    .as_mut()
                    .expect("current attempt checked above")
                    .finish(LlmCallStatus::Cancelled, Some(provider_error_code(&error)))
                    .await;
                state.current = None;
                return Some((
                    match persisted {
                        Ok(()) => ProviderEvent::Error { error },
                        Err(error) => ProviderEvent::Error {
                            error: ProviderError::Config {
                                message: format!("LLM_LEDGER_FINISH_FAILED: {error}"),
                            },
                        },
                    },
                    state,
                ));
            }

            let attempt = state
                .current
                .as_mut()
                .expect("current attempt checked above");
            match attempt.ensure_started(&state.cancel).await {
                Ok(true) => {}
                Ok(false) => return None,
                Err(error) => {
                    state.current = None;
                    return Some((
                        ProviderEvent::Error {
                            error: ProviderError::Config {
                                message: format!("LLM_LEDGER_START_FAILED: {error}"),
                            },
                        },
                        state,
                    ));
                }
            }
            let lookahead = attempt.lookahead_event.take();
            let next = if let Some(event) = lookahead {
                Some(event)
            } else if cancellation_sensitive {
                // Do not rely on a concrete provider waking its stream when the
                // shared token is cancelled. Once Finish or an exposed recoverable
                // error is pending, the registry owns the terminal race and must be
                // able to close the call even if the provider tail never wakes.
                let polled = tokio::select! {
                    biased;
                    () = state.cancel.cancelled() => None,
                    event = attempt
                        .stream
                        .as_mut()
                        .expect("provider stream initialized after admission")
                        .next() => Some(event),
                };
                let Some(event) = polled else {
                    let error = ProviderError::Cancelled;
                    let persisted = attempt
                        .finish(LlmCallStatus::Cancelled, Some(provider_error_code(&error)))
                        .await;
                    state.current = None;
                    return Some((
                        match persisted {
                            Ok(()) => ProviderEvent::Error { error },
                            Err(error) => ProviderEvent::Error {
                                error: ProviderError::Config {
                                    message: format!("LLM_LEDGER_FINISH_FAILED: {error}"),
                                },
                            },
                        },
                        state,
                    ));
                };
                event
            } else {
                attempt
                    .stream
                    .as_mut()
                    .expect("provider stream initialized after admission")
                    .next()
                    .await
            };
            match next {
                None => {
                    let finish_pending = state
                        .current
                        .as_ref()
                        .is_some_and(|attempt| !attempt.deferred_terminal_events.is_empty());
                    if finish_pending {
                        if state.cancel.is_cancelled() {
                            let error = ProviderError::Cancelled;
                            let persisted = state
                                .current
                                .as_mut()
                                .expect("current attempt checked above")
                                .finish(LlmCallStatus::Cancelled, Some(provider_error_code(&error)))
                                .await;
                            state.current = None;
                            return Some((
                                match persisted {
                                    Ok(()) => ProviderEvent::Error { error },
                                    Err(error) => ProviderEvent::Error {
                                        error: ProviderError::Config {
                                            message: format!("LLM_LEDGER_FINISH_FAILED: {error}"),
                                        },
                                    },
                                },
                                state,
                            ));
                        }
                        let (requires_usage, usage_missing) = {
                            let attempt = state
                                .current
                                .as_ref()
                                .expect("current attempt checked above");
                            (attempt.observer.is_some(), attempt.usage.is_none())
                        };
                        if let Err(error) = state
                            .current
                            .as_mut()
                            .expect("current attempt checked above")
                            .finish(LlmCallStatus::Completed, None)
                            .await
                        {
                            state.current = None;
                            return Some((
                                ProviderEvent::Error {
                                    error: ProviderError::Config {
                                        message: format!("LLM_LEDGER_FINISH_FAILED: {error}"),
                                    },
                                },
                                state,
                            ));
                        }

                        let mut completed =
                            state.current.take().expect("current attempt checked above");
                        // Completion persistence is the terminal linearization
                        // point. Cancellation which lost that race cannot replace
                        // the immutable Completed row, but it still suppresses the
                        // buffered success boundary from downstream consumers.
                        if state.cancel.is_cancelled() {
                            if !usage_missing {
                                completed.breaker.record_success();
                            }
                            return Some((
                                ProviderEvent::Error {
                                    error: ProviderError::Cancelled,
                                },
                                state,
                            ));
                        }
                        // Durable/budgeted execution may never expose a successful
                        // model boundary when authoritative usage never arrived. The
                        // observer has already persisted the incomplete physical call,
                        // so the next admission can also fail closed deterministically.
                        if requires_usage && usage_missing {
                            return Some((
                                ProviderEvent::Error {
                                    error: ProviderError::Config {
                                        message: "BUDGET_USAGE_INCOMPLETE".to_owned(),
                                    },
                                },
                                state,
                            ));
                        }

                        completed.breaker.record_success();
                        state
                            .ready_events
                            .append(&mut completed.deferred_terminal_events);
                        continue;
                    }

                    let (status, error_code, terminal_error) = if state.cancel.is_cancelled() {
                        (
                            LlmCallStatus::Cancelled,
                            Some(provider_error_code(&ProviderError::Cancelled)),
                            None,
                        )
                    } else if let Some(error) = state
                        .current
                        .as_ref()
                        .expect("current attempt checked above")
                        .last_error
                        .clone()
                    {
                        (
                            LlmCallStatus::Failed,
                            Some(provider_error_code(&error)),
                            Some(error),
                        )
                    } else {
                        (LlmCallStatus::Completed, None, None)
                    };
                    if let Err(error) = state
                        .current
                        .as_mut()
                        .expect("current attempt checked above")
                        .finish(status, error_code)
                        .await
                    {
                        state.current = None;
                        return Some((
                            ProviderEvent::Error {
                                error: ProviderError::Config {
                                    message: format!("LLM_LEDGER_FINISH_FAILED: {error}"),
                                },
                            },
                            state,
                        ));
                    }
                    if let Some(error) = terminal_error {
                        state
                            .current
                            .as_ref()
                            .expect("current attempt checked above")
                            .breaker
                            .record_error(&error);
                    }
                    return None;
                }
                Some(ProviderEvent::Error { error }) => {
                    let finish_pending = state
                        .current
                        .as_ref()
                        .is_some_and(|attempt| !attempt.deferred_terminal_events.is_empty());
                    let (produced, error_already_exposed, breaker) = {
                        let attempt = state
                            .current
                            .as_ref()
                            .expect("current attempt checked above");
                        (
                            attempt.produced_content,
                            attempt.last_error.is_some(),
                            attempt.breaker.clone(),
                        )
                    };
                    if finish_pending || matches!(error, ProviderError::Cancelled) {
                        // A transport/protocol failure after Finish invalidates the
                        // buffered success. It must not retry or fall back because the
                        // model response has already produced a terminal boundary.
                        let call_status = if matches!(error, ProviderError::Cancelled) {
                            LlmCallStatus::Cancelled
                        } else {
                            LlmCallStatus::Failed
                        };
                        if let Err(ledger_error) = state
                            .current
                            .as_mut()
                            .expect("current attempt checked above")
                            .finish(call_status, Some(provider_error_code(&error)))
                            .await
                        {
                            state.current = None;
                            return Some((
                                ProviderEvent::Error {
                                    error: ProviderError::Config {
                                        message: format!(
                                            "LLM_LEDGER_FINISH_FAILED: {ledger_error}"
                                        ),
                                    },
                                },
                                state,
                            ));
                        }
                        if !matches!(error, ProviderError::Cancelled) {
                            breaker.record_error(&error);
                        }
                        state.current = None;
                        return Some((ProviderEvent::Error { error }, state));
                    }

                    if matches!(error, ProviderError::Parse { .. }) {
                        // A parse error can describe one malformed optional stream
                        // chunk rather than a terminal transport failure. Read one
                        // event ahead before exposing it: a following event proves
                        // recovery, while EOF lets us persist Failed first so a
                        // consumer which stops at Error cannot race Drop into a
                        // Cancelled replacement. All other error variants retain
                        // their terminal/retry contract and must not be erased by a
                        // later provider event.
                        let polled = {
                            let attempt = state
                                .current
                                .as_mut()
                                .expect("current attempt checked above");
                            tokio::select! {
                                biased;
                                () = state.cancel.cancelled() => None,
                                event = attempt
                                    .stream
                                    .as_mut()
                                    .expect("provider stream initialized after admission")
                                    .next() => Some(event),
                            }
                        };
                        let Some(following) = polled else {
                            let cancelled = ProviderError::Cancelled;
                            let persisted = state
                                .current
                                .as_mut()
                                .expect("current attempt checked above")
                                .finish(
                                    LlmCallStatus::Cancelled,
                                    Some(provider_error_code(&cancelled)),
                                )
                                .await;
                            state.current = None;
                            return Some((
                                match persisted {
                                    Ok(()) => ProviderEvent::Error { error: cancelled },
                                    Err(error) => ProviderEvent::Error {
                                        error: ProviderError::Config {
                                            message: format!("LLM_LEDGER_FINISH_FAILED: {error}"),
                                        },
                                    },
                                },
                                state,
                            ));
                        };
                        if let Some(following) = following {
                            let attempt = state
                                .current
                                .as_mut()
                                .expect("current attempt checked above");
                            attempt.last_error = Some(error.clone());
                            attempt.lookahead_event = Some(following);
                            return Some((ProviderEvent::Error { error }, state));
                        }
                    }

                    // ① 同 provider 重试（旧 ApiRetryService.executeWithRetry 内层
                    //    循环）：已吐字的尝试不透明重试（状态已污染，旧
                    //    collector.hasReceivedEvents() 同）。
                    let may_switch = error.is_retryable() && !produced && !error_already_exposed;
                    let mut finalized_for_switch = false;
                    if may_switch {
                        let decision = state
                            .current
                            .as_mut()
                            .expect("current attempt checked above")
                            .retry
                            .on_error(&error);
                        // 已熔断的 provider 不值得再等退避。
                        if let Some(delay_ms) = decision.filter(|_| breaker.is_available()) {
                            if let Err(ledger_error) = state
                                .current
                                .as_mut()
                                .expect("current attempt checked above")
                                .finish(LlmCallStatus::Failed, Some(provider_error_code(&error)))
                                .await
                            {
                                state.current = None;
                                return Some((
                                    ProviderEvent::Error {
                                        error: ProviderError::Config {
                                            message: format!(
                                                "LLM_LEDGER_FINISH_FAILED: {ledger_error}"
                                            ),
                                        },
                                    },
                                    state,
                                ));
                            }
                            finalized_for_switch = true;
                            if !wait_for_retry(delay_ms, &state.cancel).await {
                                // 退避期间被取消 → 静默终止，不计熔断失败。
                                return None;
                            }
                            let (candidate, retry) = {
                                let attempt = state
                                    .current
                                    .as_ref()
                                    .expect("current attempt checked above");
                                (attempt.candidate.clone(), attempt.retry.clone())
                            };
                            let physical_attempt = state.next_physical_attempt;
                            state.next_physical_attempt =
                                state.next_physical_attempt.saturating_add(1);
                            if let Ok(mut retried) = candidate.start(
                                &state.request,
                                &state.cancel,
                                physical_attempt,
                                "retry",
                            ) {
                                // 重试账本跨尝试累计（额度不因重启而回满）。
                                retried.retry = retry;
                                state.current = Some(retried);
                                continue;
                            }
                        }
                    }

                    if may_switch && !state.queue.is_empty() {
                        if !finalized_for_switch
                            && let Err(ledger_error) = state
                                .current
                                .as_mut()
                                .expect("current attempt checked above")
                                .finish(LlmCallStatus::Failed, Some(provider_error_code(&error)))
                                .await
                        {
                            state.current = None;
                            return Some((
                                ProviderEvent::Error {
                                    error: ProviderError::Config {
                                        message: format!(
                                            "LLM_LEDGER_FINISH_FAILED: {ledger_error}"
                                        ),
                                    },
                                },
                                state,
                            ));
                        }
                        // ② 放弃重试 → 记一次 provider 失败：5xx 计入熔断（429 归密钥
                        //    冷却，见 breaker 模块文档）；一次逻辑调用只记一次。
                        breaker.record_error(&error);
                        state.current = None;
                        state.pending_error = Some(error);
                        while let Some(candidate) = state.queue.pop_front() {
                            let physical_attempt = state.next_physical_attempt;
                            state.next_physical_attempt =
                                state.next_physical_attempt.saturating_add(1);
                            if let Ok(attempt) = candidate.start(
                                &state.request,
                                &state.cancel,
                                physical_attempt,
                                "fallback",
                            ) {
                                state.current = Some(attempt);
                                state.pending_error = None;
                                break;
                            }
                        }
                        continue;
                    }

                    if finalized_for_switch {
                        // A retry was selected and the old physical attempt was
                        // already finalized, but constructing the replacement failed.
                        // The old stream can no longer recover after that boundary.
                        breaker.record_error(&error);
                        state.current = None;
                        return Some((ProviderEvent::Error { error }, state));
                    }

                    // Parse lookahead proved physical EOF, or a non-retryable
                    // terminal error arrived. Persist Failed before exposing the
                    // Error so downstream consumers may stop immediately without
                    // racing Drop into a Cancelled replacement.
                    if let Err(ledger_error) = state
                        .current
                        .as_mut()
                        .expect("current attempt checked above")
                        .finish(LlmCallStatus::Failed, Some(provider_error_code(&error)))
                        .await
                    {
                        state.current = None;
                        return Some((
                            ProviderEvent::Error {
                                error: ProviderError::Config {
                                    message: format!("LLM_LEDGER_FINISH_FAILED: {ledger_error}"),
                                },
                            },
                            state,
                        ));
                    }
                    breaker.record_error(&error);
                    state.current = None;
                    return Some((ProviderEvent::Error { error }, state));
                }
                Some(event) => {
                    let attempt = state
                        .current
                        .as_mut()
                        .expect("current attempt checked above");
                    if !attempt.deferred_terminal_events.is_empty() {
                        if let ProviderEvent::UsageUpdate { usage } = &event {
                            // Multiple usage-only trailers are legal; the final one
                            // is authoritative for the durable physical-call ledger.
                            attempt.usage = Some(*usage);
                            attempt.deferred_terminal_events.push_back(event);
                            continue;
                        }

                        let kind = match event {
                            ProviderEvent::TextDelta { .. } => "text_delta",
                            ProviderEvent::ThinkingDelta { .. } => "thinking_delta",
                            ProviderEvent::ToolUseStart { .. } => "tool_use_start",
                            ProviderEvent::ToolInputDelta { .. } => "tool_input_delta",
                            ProviderEvent::Finish { .. } => "finish",
                            ProviderEvent::UsageUpdate { .. } | ProviderEvent::Error { .. } => {
                                unreachable!("handled by preceding stream branches")
                            }
                        };
                        let error = ProviderError::Parse {
                            message: format!("INVALID_EVENT_AFTER_FINISH: {kind}"),
                        };
                        let persisted = attempt
                            .finish(LlmCallStatus::Failed, Some(provider_error_code(&error)))
                            .await;
                        attempt.breaker.record_error(&error);
                        state.current = None;
                        return Some((
                            match persisted {
                                Ok(()) => ProviderEvent::Error { error },
                                Err(error) => ProviderEvent::Error {
                                    error: ProviderError::Config {
                                        message: format!("LLM_LEDGER_FINISH_FAILED: {error}"),
                                    },
                                },
                            },
                            state,
                        ));
                    }

                    match &event {
                        ProviderEvent::TextDelta { .. }
                        | ProviderEvent::ThinkingDelta { .. }
                        | ProviderEvent::ToolUseStart { .. }
                        | ProviderEvent::ToolInputDelta { .. } => {
                            attempt.produced_content = true;
                            return Some((event, state));
                        }
                        ProviderEvent::UsageUpdate { usage } => {
                            attempt.usage = Some(*usage);
                            return Some((event, state));
                        }
                        ProviderEvent::Finish {
                            usage: Some(usage), ..
                        } => {
                            attempt.usage = Some(*usage);
                            attempt.deferred_terminal_events.push_back(event);
                        }
                        ProviderEvent::Finish { usage: None, .. } => {
                            attempt.deferred_terminal_events.push_back(event);
                        }
                        ProviderEvent::Error { .. } => {
                            unreachable!("handled by preceding stream branch")
                        }
                    }
                }
            }
        }
    })
}

fn provider_error_code(error: &ProviderError) -> String {
    match error {
        ProviderError::Http { status, .. } => format!("HTTP_{status}"),
        ProviderError::Network { .. } => "PROVIDER_NETWORK".to_owned(),
        ProviderError::Cancelled => "PROVIDER_CANCELLED".to_owned(),
        ProviderError::Parse { .. } => "PROVIDER_PARSE".to_owned(),
        ProviderError::Config { .. } => "PROVIDER_CONFIG".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::config::{ANTHROPIC_BASE_URL, MOONSHOT_BASE_URL, PROVIDER_CATALOG, catalog_entry};
    use crate::ledger::LlmExecutionAttribution;
    use crate::openai_compat::chat_completions_url;
    use crate::provider::FinishReason;
    use crate::secret::{ApiKey, ApiKeyRing};
    use tokio::sync::Notify;

    /// 脚本化 provider：按固定事件序回放，并记录每次请求的模型。
    struct ScriptedProvider {
        name: String,
        events: Vec<ProviderEvent>,
        seen_models: Arc<Mutex<Vec<String>>>,
        seen_requests: Arc<Mutex<Vec<ChatRequest>>>,
    }

    impl ScriptedProvider {
        fn new(name: &str, events: Vec<ProviderEvent>) -> Self {
            Self {
                name: name.to_owned(),
                events,
                seen_models: Arc::new(Mutex::new(Vec::new())),
                seen_requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl ChatProvider for ScriptedProvider {
        fn provider_name(&self) -> &str {
            &self.name
        }

        fn chat_stream(
            &self,
            request: ChatRequest,
            _cancel: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            self.seen_models
                .lock()
                .expect("lock")
                .push(request.model.clone());
            self.seen_requests
                .lock()
                .expect("lock")
                .push(request.clone());
            Ok(futures::stream::iter(self.events.clone()).boxed())
        }
    }

    /// 逐次回放不同脚本的 provider：第 n 次调用取第 n 个脚本（超出后重复末条），
    /// 用于验证「同 provider 重试」的多次尝试语义。
    struct SequencedProvider {
        name: String,
        scripts: Vec<Vec<ProviderEvent>>,
        seen_models: Arc<Mutex<Vec<String>>>,
    }

    struct ConcurrencyProbeProvider {
        name: String,
        current: Arc<std::sync::atomic::AtomicUsize>,
        max_seen: Arc<std::sync::atomic::AtomicUsize>,
        release: Arc<Semaphore>,
    }

    /// Emits Finish, then waits at the physical tail before yielding usage-only
    /// events. Tests use the gate to prove the registry exposes neither Finish
    /// nor observer completion before EOF.
    struct GatedTrailingUsageProvider {
        reached_tail: Arc<Notify>,
        release_tail: Arc<Notify>,
        finish_usage: Option<zk_protocol::Usage>,
        trailing_usage: Arc<Vec<zk_protocol::Usage>>,
    }

    impl ChatProvider for GatedTrailingUsageProvider {
        fn provider_name(&self) -> &'static str {
            "gated-tail"
        }

        fn chat_stream(
            &self,
            _request: ChatRequest,
            _cancel: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            let reached_tail = Arc::clone(&self.reached_tail);
            let release_tail = Arc::clone(&self.release_tail);
            let trailing_usage = Arc::clone(&self.trailing_usage);
            let finish_usage = self.finish_usage;
            Ok(futures::stream::unfold(0_usize, move |state| {
                let reached_tail = Arc::clone(&reached_tail);
                let release_tail = Arc::clone(&release_tail);
                let trailing_usage = Arc::clone(&trailing_usage);
                async move {
                    if state == 0 {
                        return Some((
                            ProviderEvent::Finish {
                                finish_reason: FinishReason::EndTurn,
                                usage: finish_usage,
                            },
                            1,
                        ));
                    }
                    if state == 1 {
                        reached_tail.notify_one();
                        // Deliberately ignore the provider cancellation token. This
                        // makes the registry cancellation test prove that its own
                        // post-Finish select can interrupt a permanently pending tail.
                        release_tail.notified().await;
                    }
                    trailing_usage
                        .get(state - 1)
                        .copied()
                        .map(|usage| (ProviderEvent::UsageUpdate { usage }, state + 1))
                }
            })
            .boxed())
        }
    }

    impl ChatProvider for ConcurrencyProbeProvider {
        fn provider_name(&self) -> &str {
            &self.name
        }

        fn chat_stream(
            &self,
            _request: ChatRequest,
            _cancel: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            let current = Arc::clone(&self.current);
            let max_seen = Arc::clone(&self.max_seen);
            let release = Arc::clone(&self.release);
            Ok(futures::stream::once(async move {
                use std::sync::atomic::Ordering;

                let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(active, Ordering::SeqCst);
                let permit = release.acquire().await.expect("probe semaphore open");
                permit.forget();
                current.fetch_sub(1, Ordering::SeqCst);
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: None,
                }
            })
            .boxed())
        }
    }

    /// In-memory observer used to assert the one-start/one-finish contract at
    /// the physical provider-attempt boundary.
    #[derive(Debug, Default)]
    struct RecordingCallObserver {
        started: Mutex<Vec<LlmCallStarted>>,
        finished: Mutex<Vec<LlmCallFinished>>,
    }

    impl LlmCallObserver for RecordingCallObserver {
        fn call_started(
            &self,
            call: LlmCallStarted,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            self.started.lock().expect("lock").push(call);
            Box::pin(async { Ok(()) })
        }

        fn call_finished(
            &self,
            call: LlmCallFinished,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            self.finished.lock().expect("lock").push(call);
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Debug, Default)]
    struct RejectFinishObserver {
        started: Mutex<usize>,
        finished: Mutex<usize>,
    }

    #[derive(Debug, Default)]
    struct FailOnceFinishObserver {
        started: Mutex<Vec<LlmCallStarted>>,
        attempts: Mutex<usize>,
        finished: Mutex<Vec<LlmCallFinished>>,
    }

    #[derive(Debug, Default)]
    struct BlockingStartObserver {
        start_entered: Arc<Notify>,
        release_start: Arc<Notify>,
        started: Arc<Mutex<Vec<LlmCallStarted>>>,
        finished: Arc<Mutex<Vec<LlmCallFinished>>>,
    }

    #[derive(Debug, Default)]
    struct BlockingFinishObserver {
        finish_entered: Arc<Notify>,
        release_finish: Arc<Notify>,
        started: Arc<Mutex<Vec<LlmCallStarted>>>,
        finished: Arc<Mutex<Vec<LlmCallFinished>>>,
    }

    #[derive(Debug)]
    struct RejectStartObserver;

    impl LlmCallObserver for RejectStartObserver {
        fn call_started(
            &self,
            _call: LlmCallStarted,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            Box::pin(async { Err("BUDGET_EXHAUSTED".to_owned()) })
        }

        fn call_finished(
            &self,
            _call: LlmCallFinished,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            Box::pin(async { panic!("a rejected start cannot have a completion") })
        }
    }

    impl LlmCallObserver for RejectFinishObserver {
        fn call_started(
            &self,
            _call: LlmCallStarted,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            *self.started.lock().expect("lock") += 1;
            Box::pin(async { Ok(()) })
        }

        fn call_finished(
            &self,
            _call: LlmCallFinished,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            *self.finished.lock().expect("lock") += 1;
            Box::pin(async { Err("disk full".to_owned()) })
        }
    }

    impl LlmCallObserver for FailOnceFinishObserver {
        fn call_started(
            &self,
            call: LlmCallStarted,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            self.started.lock().expect("lock").push(call);
            Box::pin(async { Ok(()) })
        }

        fn call_finished(
            &self,
            call: LlmCallFinished,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            let mut attempts = self.attempts.lock().expect("lock");
            *attempts += 1;
            if *attempts == 1 {
                return Box::pin(async { Err("sqlite transient busy".to_owned()) });
            }
            drop(attempts);
            self.finished.lock().expect("lock").push(call);
            Box::pin(async { Ok(()) })
        }
    }

    impl LlmCallObserver for BlockingStartObserver {
        fn call_started(
            &self,
            call: LlmCallStarted,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            let start_entered = Arc::clone(&self.start_entered);
            let release_start = Arc::clone(&self.release_start);
            let started = Arc::clone(&self.started);
            Box::pin(async move {
                start_entered.notify_one();
                release_start.notified().await;
                started.lock().expect("lock").push(call);
                Ok(())
            })
        }

        fn call_finished(
            &self,
            call: LlmCallFinished,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            self.finished.lock().expect("lock").push(call);
            Box::pin(async { Ok(()) })
        }
    }

    impl LlmCallObserver for BlockingFinishObserver {
        fn call_started(
            &self,
            call: LlmCallStarted,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            self.started.lock().expect("lock").push(call);
            Box::pin(async { Ok(()) })
        }

        fn call_finished(
            &self,
            call: LlmCallFinished,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            let finish_entered = Arc::clone(&self.finish_entered);
            let release_finish = Arc::clone(&self.release_finish);
            let finished = Arc::clone(&self.finished);
            Box::pin(async move {
                finish_entered.notify_one();
                release_finish.notified().await;
                finished.lock().expect("lock").push(call);
                Ok(())
            })
        }
    }

    impl SequencedProvider {
        fn new(name: &str, scripts: Vec<Vec<ProviderEvent>>) -> Self {
            assert!(!scripts.is_empty(), "at least one script required");
            Self {
                name: name.to_owned(),
                scripts,
                seen_models: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl ChatProvider for SequencedProvider {
        fn provider_name(&self) -> &str {
            &self.name
        }

        fn chat_stream(
            &self,
            request: ChatRequest,
            _cancel: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            let index = {
                let mut seen = self.seen_models.lock().expect("lock");
                seen.push(request.model.clone());
                seen.len() - 1
            };
            let script = self
                .scripts
                .get(index)
                .unwrap_or_else(|| self.scripts.last().expect("non-empty scripts"));
            Ok(futures::stream::iter(script.clone()).boxed())
        }
    }

    /// 建立期即失败的 provider（验证候选序跳过语义）。
    struct SetupFailProvider {
        name: String,
    }

    impl ChatProvider for SetupFailProvider {
        fn provider_name(&self) -> &str {
            &self.name
        }

        fn chat_stream(
            &self,
            _request: ChatRequest,
            _cancel: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            Err(ProviderError::Config {
                message: format!("provider '{}' has empty api key", self.name),
            })
        }
    }

    fn text_then_finish(text: &str) -> Vec<ProviderEvent> {
        vec![
            ProviderEvent::TextDelta {
                text: text.to_owned(),
            },
            ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: None,
            },
        ]
    }

    fn error_event(status: u16) -> ProviderEvent {
        ProviderEvent::Error {
            error: ProviderError::http(status, format!("HTTP {status}"), None),
        }
    }

    async fn drain(
        registry: &ProviderRegistry,
        model: &str,
    ) -> Result<Vec<ProviderEvent>, ProviderError> {
        let request = ChatRequest::new(model);
        let stream = registry.chat_stream(request, CancellationToken::new())?;
        Ok(stream.collect::<Vec<_>>().await)
    }

    async fn drain_with_cancel(
        registry: &ProviderRegistry,
        model: &str,
        cancel: CancellationToken,
    ) -> Result<Vec<ProviderEvent>, ProviderError> {
        let stream = registry.chat_stream(ChatRequest::new(model), cancel)?;
        Ok(stream.collect::<Vec<_>>().await)
    }

    fn wait_for_sync_completion(finished: &Mutex<Vec<LlmCallFinished>>) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while finished.lock().expect("lock").is_empty() {
            assert!(
                Instant::now() < deadline,
                "lifecycle completion did not outlive the caller runtime"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn register_keeps_provider_and_model_order_with_first_owner_wins() {
        let mut registry = ProviderRegistry::new();
        registry.register(
            "moonshot",
            Arc::new(ScriptedProvider::new("moonshot", Vec::new())),
            vec!["kimi-k3".into(), "moonshot-v1-128k".into()],
        );
        registry.register(
            "dashscope",
            Arc::new(ScriptedProvider::new("dashscope", Vec::new())),
            // kimi-k3 重复声明——首个归属者（moonshot）胜。
            vec!["qwen3.8-max-0902".into(), "kimi-k3".into(), "  ".into()],
        );
        assert_eq!(registry.names(), ["moonshot", "dashscope"]);
        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());
        assert_eq!(
            registry.models(),
            ["kimi-k3", "moonshot-v1-128k", "qwen3.8-max-0902"]
        );
        assert_eq!(registry.model_owner("kimi-k3"), Some("moonshot"));
        assert_eq!(registry.model_owner("qwen3.8-max-0902"), Some("dashscope"));
        assert_eq!(registry.model_owner("unknown-model"), None);
        assert!(registry.get("moonshot").is_some());
        assert!(registry.get("zhipu").is_none());
    }

    #[test]
    fn model_support_is_strict_once_a_catalog_exists_and_stale_default_falls_back() {
        let mut registry = ProviderRegistry::new();
        registry.register(
            "moonshot",
            Arc::new(ScriptedProvider::new("moonshot", Vec::new())),
            vec!["kimi-k3".into(), "moonshot-v1-128k".into()],
        );
        let registry = registry.with_default_model("retired-model");

        assert!(registry.supports_model("kimi-k3"));
        assert!(!registry.supports_model("retired-model"));
        assert!(!registry.supports_model("  "));
        assert_eq!(registry.effective_default_model(), "kimi-k3");
    }

    #[test]
    fn empty_model_catalog_preserves_phase_one_custom_models() {
        let mut registry = ProviderRegistry::new();
        registry.register(
            "openai-compat",
            Arc::new(ScriptedProvider::new("openai-compat", Vec::new())),
            Vec::new(),
        );
        let registry = registry.with_default_model("company/custom-model");

        assert!(registry.supports_model("another/custom-model"));
        assert!(!registry.supports_model(""));
        assert_eq!(registry.effective_default_model(), "company/custom-model");
        assert_eq!(
            ProviderRegistry::default().effective_default_model(),
            DEFAULT_MODEL
        );
    }

    #[test]
    fn resolve_provider_falls_back_to_default_model_owner_then_first() {
        let mut registry = ProviderRegistry::new();
        registry.register(
            "moonshot",
            Arc::new(ScriptedProvider::new("moonshot", Vec::new())),
            vec!["kimi-k3".into()],
        );
        registry.register(
            "dashscope",
            Arc::new(ScriptedProvider::new("dashscope", Vec::new())),
            vec!["qwen3.8-max-0902".into()],
        );
        // 默认模型归属者接管未知模型。
        let registry = registry.with_default_model("qwen3.8-max-0902");
        assert_eq!(registry.resolve_provider("nope"), Some("dashscope"));
        // 默认模型也未注册时退到首个注册 provider。
        let registry = registry.with_default_model("not-registered");
        assert_eq!(registry.resolve_provider("nope"), Some("moonshot"));
        // 空注册表无从路由。
        assert_eq!(ProviderRegistry::new().resolve_provider("any"), None);
    }

    #[test]
    fn candidate_models_follow_chain_suffix_and_depth_cap() {
        let registry = ProviderRegistry::new().with_fallback_chain(vec![
            "kimi-k3".into(),
            "qwen3.8-max-0902".into(),
            "deepseek-chat".into(),
            "glm-5.3".into(),
        ]);
        // 请求模型在链中 → 取其后继（含自身共 MAX_FALLBACK_DEPTH 个）。
        assert_eq!(
            registry.candidate_models("kimi-k3"),
            ["kimi-k3", "qwen3.8-max-0902", "deepseek-chat"]
        );
        assert_eq!(
            registry.candidate_models("deepseek-chat"),
            ["deepseek-chat", "glm-5.3"]
        );
        // 请求模型不在链中 → 接整条链（同样截断）。
        assert_eq!(
            registry.candidate_models("MiniMax-M3"),
            ["MiniMax-M3", "kimi-k3", "qwen3.8-max-0902"]
        );
        // 无链配置 → 只有首选。
        assert_eq!(
            ProviderRegistry::new().candidate_models("kimi-k3"),
            ["kimi-k3"]
        );
    }

    #[tokio::test]
    async fn single_provider_serves_unknown_model_phase_one_fallback() {
        // Phase 1 回退形态：仅一个 provider、models 清单为空（S9 的 ProviderConfig
        // 构造即如此）→ 任何模型都路由给它，且请求模型原样透传。
        let provider = ScriptedProvider::new("openai-compat", text_then_finish("hi"));
        let seen = provider.seen_models.clone();
        let mut registry = ProviderRegistry::new();
        registry.register("openai-compat", Arc::new(provider), Vec::new());
        let events = drain(&registry, "qwen3.8-max-0902").await.expect("stream");
        assert_eq!(events.len(), 2);
        assert_eq!(seen.lock().expect("lock").as_slice(), ["qwen3.8-max-0902"]);
        assert!(registry.models().is_empty(), "no declared models to expose");
    }

    #[tokio::test]
    async fn retryable_error_before_content_degrades_to_next_chain_model() {
        let primary = ScriptedProvider::new("moonshot", vec![error_event(503)]);
        let secondary = ScriptedProvider::new("dashscope", text_then_finish("from fallback"));
        let primary_seen = primary.seen_models.clone();
        let secondary_seen = secondary.seen_models.clone();
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(primary), vec!["kimi-k3".into()]);
        registry.register(
            "dashscope",
            Arc::new(secondary),
            vec!["qwen3.8-max-0902".into()],
        );
        let registry = registry
            .with_fallback_chain(vec!["kimi-k3".into(), "qwen3.8-max-0902".into()])
            .with_default_model("kimi-k3")
            // 本例只验证降级转移语义，关掉同 provider 重试。
            .with_retry_policy(RetryPolicy::none());

        let events = drain(&registry, "kimi-k3").await.expect("stream");
        // 触发降级的 503 不外泄；次选完整完成对话。
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta {
                    text: "from fallback".into()
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: None,
                },
            ]
        );
        assert_eq!(primary_seen.lock().expect("lock").as_slice(), ["kimi-k3"]);
        assert_eq!(
            secondary_seen.lock().expect("lock").as_slice(),
            ["qwen3.8-max-0902"],
            "fallback request must carry the fallback model"
        );
    }

    #[tokio::test]
    async fn fallback_rechecks_capabilities_before_starting_physical_provider() {
        use crate::provider::{ImageSource, ToolSpec};

        let primary = ScriptedProvider::new("moonshot", vec![error_event(503)]);
        let secondary = ScriptedProvider::new("ollama", text_then_finish("must not run"));
        let secondary_seen = secondary.seen_models.clone();
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(primary), vec!["kimi-k3".into()]);
        registry.register(
            "ollama",
            Arc::new(secondary),
            vec!["ollama/llama3.1:8b".into()],
        );
        let registry = registry
            .with_fallback_chain(vec!["kimi-k3".into(), "ollama/llama3.1:8b".into()])
            .with_retry_policy(RetryPolicy::none());
        let mut request = ChatRequest::new("kimi-k3").with_tools(vec![ToolSpec {
            name: "Read".into(),
            description: "read".into(),
            parameters: serde_json::json!({"type":"object"}),
        }]);
        request
            .messages
            .push(crate::provider::ChatMessage::user_with_images(
                "inspect",
                vec![ImageSource {
                    media_type: "image/png".into(),
                    data: Some("aGVsbG8=".into()),
                    url: None,
                }],
            ));

        let stream = registry
            .chat_stream(request, CancellationToken::new())
            .expect("primary stream");
        let events = stream.collect::<Vec<_>>().await;
        assert_eq!(events, vec![error_event(503)]);
        assert!(
            secondary_seen.lock().expect("lock").is_empty(),
            "an incompatible fallback must be rejected before network execution"
        );
    }

    #[tokio::test]
    async fn fallback_clamps_output_limit_to_the_physical_model() {
        let primary = ScriptedProvider::new("moonshot", vec![error_event(503)]);
        let secondary = ScriptedProvider::new("dashscope", text_then_finish("fallback"));
        let secondary_requests = secondary.seen_requests.clone();
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(primary), vec!["kimi-k3".into()]);
        registry.register("dashscope", Arc::new(secondary), vec!["qwen-turbo".into()]);
        let registry = registry
            .with_fallback_chain(vec!["kimi-k3".into(), "qwen-turbo".into()])
            .with_retry_policy(RetryPolicy::none());
        let request = ChatRequest::new("kimi-k3").with_max_tokens(100_000);

        let events = registry
            .chat_stream(request, CancellationToken::new())
            .expect("primary stream")
            .collect::<Vec<_>>()
            .await;
        assert_eq!(events, text_then_finish("fallback"));
        let seen = secondary_requests.lock().expect("lock");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].model, "qwen-turbo");
        assert_eq!(seen[0].max_tokens, 8192);
    }

    #[tokio::test]
    async fn same_provider_retry_recovers_without_touching_fallback() {
        // 首次 503 → 同 provider 重试 → 第二次成功；降级链全程不介入。
        let primary = SequencedProvider::new(
            "moonshot",
            vec![vec![error_event(503)], text_then_finish("recovered")],
        );
        let secondary = ScriptedProvider::new("dashscope", text_then_finish("never"));
        let primary_seen = primary.seen_models.clone();
        let secondary_seen = secondary.seen_models.clone();
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(primary), vec!["kimi-k3".into()]);
        registry.register(
            "dashscope",
            Arc::new(secondary),
            vec!["qwen3.8-max-0902".into()],
        );
        let registry = registry
            .with_fallback_chain(vec!["kimi-k3".into(), "qwen3.8-max-0902".into()])
            .with_retry_policy(RetryPolicy::immediate(5));

        let events = drain(&registry, "kimi-k3").await.expect("stream");
        // 触发重试的 503 不外泄；重试后的成功流原样透出。
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta {
                    text: "recovered".into()
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: None,
                },
            ]
        );
        assert_eq!(
            primary_seen.lock().expect("lock").as_slice(),
            ["kimi-k3", "kimi-k3"],
            "retry must re-issue the same model on the same provider"
        );
        assert!(
            secondary_seen.lock().expect("lock").is_empty(),
            "fallback must stay untouched while retry budget remains"
        );
    }

    #[tokio::test]
    async fn retry_budget_exhaustion_then_degrades_to_fallback() {
        // 恒 503：耗尽 max_retries 次同 provider 尝试后才降级到后继候选。
        let primary = SequencedProvider::new("moonshot", vec![vec![error_event(503)]]);
        let secondary = ScriptedProvider::new("dashscope", text_then_finish("from fallback"));
        let primary_seen = primary.seen_models.clone();
        let secondary_seen = secondary.seen_models.clone();
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(primary), vec!["kimi-k3".into()]);
        registry.register(
            "dashscope",
            Arc::new(secondary),
            vec!["qwen3.8-max-0902".into()],
        );
        let registry = registry
            .with_fallback_chain(vec!["kimi-k3".into(), "qwen3.8-max-0902".into()])
            .with_retry_policy(RetryPolicy::immediate(3));

        let events = drain(&registry, "kimi-k3").await.expect("stream");
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta {
                    text: "from fallback".into()
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: None,
                },
            ]
        );
        assert_eq!(
            primary_seen.lock().expect("lock").len(),
            3,
            "max_retries counts failures: 3rd failure gives up (旧 attempt >= maxRetries)"
        );
        assert_eq!(
            secondary_seen.lock().expect("lock").as_slice(),
            ["qwen3.8-max-0902"]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // one retry/fallback ledger sequence and its exact pairing assertions
    async fn physical_call_observer_tracks_every_retry_and_fallback_once() {
        let primary = SequencedProvider::new("moonshot", vec![vec![error_event(503)]]);
        let secondary = ScriptedProvider::new(
            "dashscope",
            vec![
                ProviderEvent::TextDelta {
                    text: "from fallback".into(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(zk_protocol::Usage {
                        input_tokens: 11,
                        output_tokens: 7,
                        cache_read_input_tokens: 3,
                        cache_creation_input_tokens: 2,
                    }),
                },
            ],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(primary), vec!["kimi-k3".into()]);
        registry.register(
            "dashscope",
            Arc::new(secondary),
            vec!["qwen3.8-max-0902".into()],
        );
        let registry = registry
            .with_fallback_chain(vec!["kimi-k3".into(), "qwen3.8-max-0902".into()])
            // First primary + one retry, then fallback.
            .with_retry_policy(RetryPolicy::immediate(2));
        let observer = Arc::new(RecordingCallObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-1", "run-1", "conversation"),
            observer.clone(),
        );

        let events = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream")
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(events.last(), Some(ProviderEvent::Finish { .. })));

        let started = observer.started.lock().expect("lock").clone();
        let finished = observer.finished.lock().expect("lock").clone();
        assert_eq!(
            started.len(),
            3,
            "primary, retry, and fallback must all start"
        );
        assert_eq!(
            finished.len(),
            3,
            "every started attempt must terminate once"
        );

        let routes = started
            .iter()
            .map(|call| {
                let route: serde_json::Value =
                    serde_json::from_str(&call.route).expect("route json");
                (
                    call.provider.clone(),
                    call.model.clone(),
                    route["reason"].as_str().expect("reason").to_owned(),
                    route["physicalAttempt"].as_u64().expect("attempt"),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            routes,
            vec![
                (
                    "moonshot".to_owned(),
                    "kimi-k3".to_owned(),
                    "primary".to_owned(),
                    1,
                ),
                (
                    "moonshot".to_owned(),
                    "kimi-k3".to_owned(),
                    "retry".to_owned(),
                    2,
                ),
                (
                    "dashscope".to_owned(),
                    "qwen3.8-max-0902".to_owned(),
                    "fallback".to_owned(),
                    3,
                ),
            ]
        );

        for start in &started {
            uuid::Uuid::parse_str(&start.call_id).expect("full UUID call id");
            assert_eq!(start.attribution.task_id, "task-1");
            assert_eq!(start.attribution.run_id, "run-1");
            assert_eq!(
                finished
                    .iter()
                    .filter(|finish| finish.call_id == start.call_id)
                    .count(),
                1,
                "each start must have exactly one matching finish"
            );
        }
        assert_eq!(finished[0].status, LlmCallStatus::Failed);
        assert_eq!(finished[0].error_code.as_deref(), Some("HTTP_503"));
        assert_eq!(finished[1].status, LlmCallStatus::Failed);
        assert_eq!(finished[1].error_code.as_deref(), Some("HTTP_503"));
        assert_eq!(finished[2].status, LlmCallStatus::Completed);
        assert_eq!(finished[2].usage.map(|usage| usage.input_tokens), Some(11));
    }

    #[tokio::test]
    async fn observed_finish_waits_for_eof_and_last_trailing_usage_wins() {
        let reached_tail = Arc::new(Notify::new());
        let release_tail = Arc::new(Notify::new());
        let first_usage = zk_protocol::Usage {
            input_tokens: 10,
            output_tokens: 2,
            cache_read_input_tokens: 1,
            cache_creation_input_tokens: 0,
        };
        let authoritative_usage = zk_protocol::Usage {
            input_tokens: 12,
            output_tokens: 4,
            cache_read_input_tokens: 3,
            cache_creation_input_tokens: 1,
        };
        let provider = GatedTrailingUsageProvider {
            reached_tail: Arc::clone(&reached_tail),
            release_tail: Arc::clone(&release_tail),
            finish_usage: None,
            trailing_usage: Arc::new(vec![first_usage, authoritative_usage]),
        };
        let mut registry = ProviderRegistry::new();
        registry.register("gated-tail", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RecordingCallObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-tail", "run-tail", "conversation"),
            observer.clone(),
        );
        let mut stream = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream");

        let first = stream.next();
        tokio::pin!(first);
        tokio::select! {
            () = reached_tail.notified() => {}
            leaked = &mut first => panic!("Finish leaked before physical EOF: {leaked:?}"),
        }
        assert!(
            observer.finished.lock().expect("lock").is_empty(),
            "observer completion must also wait for the usage tail"
        );

        release_tail.notify_one();
        assert_eq!(
            first.await,
            Some(ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: None,
            })
        );
        assert_eq!(
            stream.next().await,
            Some(ProviderEvent::UsageUpdate { usage: first_usage })
        );
        assert_eq!(
            stream.next().await,
            Some(ProviderEvent::UsageUpdate {
                usage: authoritative_usage,
            })
        );
        assert!(stream.next().await.is_none());

        let finished = observer.finished.lock().expect("lock");
        assert_eq!(finished.len(), 1, "one physical terminal write");
        assert_eq!(finished[0].status, LlmCallStatus::Completed);
        assert_eq!(finished[0].usage, Some(authoritative_usage));
        assert_eq!(finished[0].error_code, None);
    }

    #[tokio::test]
    async fn finish_with_inline_usage_is_committed_before_it_is_exposed() {
        let usage = zk_protocol::Usage {
            input_tokens: 21,
            output_tokens: 8,
            cache_read_input_tokens: 5,
            cache_creation_input_tokens: 2,
        };
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(usage),
            }],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RecordingCallObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-inline", "run-inline", "conversation"),
            observer.clone(),
        );

        let events = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream")
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            events,
            vec![ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(usage),
            }]
        );
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].status, LlmCallStatus::Completed);
        assert_eq!(finished[0].usage, Some(usage));
    }

    #[tokio::test]
    async fn cancellation_during_completed_write_hides_finish_without_replacing_payload() {
        let usage = zk_protocol::Usage {
            input_tokens: 31,
            output_tokens: 9,
            ..zk_protocol::Usage::default()
        };
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(usage),
            }],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(BlockingFinishObserver::default());
        let cancel = CancellationToken::new();
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-finish-race", "run-finish-race", "conversation"),
            observer.clone(),
        );
        let mut stream = registry
            .chat_stream(request, cancel.clone())
            .expect("stream");

        let first = stream.next();
        tokio::pin!(first);
        tokio::select! {
            () = observer.finish_entered.notified() => {}
            leaked = &mut first => panic!("Finish leaked before observer commit: {leaked:?}"),
        }
        cancel.cancel();
        observer.release_finish.notify_one();
        assert!(matches!(
            first.await,
            Some(ProviderEvent::Error {
                error: ProviderError::Cancelled
            })
        ));
        assert!(stream.next().await.is_none());

        let finished = observer.finished.lock().expect("lock");
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].status, LlmCallStatus::Completed);
        assert_eq!(finished[0].usage, Some(usage));
        assert_eq!(finished[0].error_code, None);
    }

    #[tokio::test]
    async fn drop_during_completed_write_awaits_the_same_terminal_payload() {
        let usage = zk_protocol::Usage {
            input_tokens: 29,
            output_tokens: 7,
            ..zk_protocol::Usage::default()
        };
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(usage),
            }],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(BlockingFinishObserver::default());
        let cancel = CancellationToken::new();
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-finish-drop", "run-finish-drop", "conversation"),
            observer.clone(),
        );
        let mut stream = registry
            .chat_stream(request, cancel.clone())
            .expect("stream");

        {
            let first = stream.next();
            tokio::pin!(first);
            tokio::select! {
                () = observer.finish_entered.notified() => {}
                leaked = &mut first => panic!("Finish leaked before blocked write: {leaked:?}"),
            }
        }
        cancel.cancel();
        drop(stream);
        observer.release_finish.notify_one();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !observer.finished.lock().expect("lock").is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drop caretaker must await the in-flight Completed write");
        tokio::task::yield_now().await;

        let finished = observer.finished.lock().expect("lock");
        assert_eq!(
            finished.len(),
            1,
            "only the selected payload may be written"
        );
        assert_eq!(finished[0].status, LlmCallStatus::Completed);
        assert_eq!(finished[0].usage, Some(usage));
        assert_eq!(finished[0].error_code, None);
    }

    #[test]
    fn in_flight_start_and_drop_survive_caller_runtime_teardown() {
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(zk_protocol::Usage {
                    input_tokens: 5,
                    output_tokens: 1,
                    ..zk_protocol::Usage::default()
                }),
            }],
        );
        let seen_requests = Arc::clone(&provider.seen_requests);
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(BlockingStartObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new(
                "task-runtime-start-drop",
                "run-runtime-start-drop",
                "conversation",
            ),
            observer.clone(),
        );
        let caller_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("caller runtime");

        caller_runtime.block_on(async {
            let mut stream = registry
                .chat_stream(request, CancellationToken::new())
                .expect("stream");
            {
                let first = stream.next();
                tokio::pin!(first);
                tokio::select! {
                    () = observer.start_entered.notified() => {}
                    leaked = &mut first => panic!("provider ran before start admission: {leaked:?}"),
                }
            }
            drop(stream);
        });
        drop(caller_runtime);

        observer.release_start.notify_one();
        wait_for_sync_completion(&observer.finished);

        let started = observer.started.lock().expect("lock");
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(started.len(), 1);
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].call_id, started[0].call_id);
        assert_eq!(finished[0].status, LlmCallStatus::Cancelled);
        assert_eq!(finished[0].error_code.as_deref(), Some("STREAM_DROPPED"));
        assert!(finished[0].usage.is_none());
        assert!(
            seen_requests.lock().expect("lock").is_empty(),
            "caller teardown must not start a provider after its stream was dropped"
        );
    }

    #[test]
    fn in_flight_terminal_write_survives_caller_runtime_teardown() {
        let usage = zk_protocol::Usage {
            input_tokens: 37,
            output_tokens: 11,
            ..zk_protocol::Usage::default()
        };
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(usage),
            }],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(BlockingFinishObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new(
                "task-runtime-finish-drop",
                "run-runtime-finish-drop",
                "conversation",
            ),
            observer.clone(),
        );
        let caller_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("caller runtime");

        caller_runtime.block_on(async {
            let mut stream = registry
                .chat_stream(request, CancellationToken::new())
                .expect("stream");
            {
                let first = stream.next();
                tokio::pin!(first);
                tokio::select! {
                    () = observer.finish_entered.notified() => {}
                    leaked = &mut first => panic!("Finish leaked before observer commit: {leaked:?}"),
                }
            }
            drop(stream);
        });
        drop(caller_runtime);

        observer.release_finish.notify_one();
        wait_for_sync_completion(&observer.finished);

        let started = observer.started.lock().expect("lock");
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(started.len(), 1);
        assert_eq!(
            finished.len(),
            1,
            "the immutable terminal payload is unique"
        );
        assert_eq!(finished[0].call_id, started[0].call_id);
        assert_eq!(finished[0].status, LlmCallStatus::Completed);
        assert_eq!(finished[0].usage, Some(usage));
        assert_eq!(finished[0].error_code, None);
    }

    #[tokio::test]
    async fn observed_finish_without_any_usage_fails_closed() {
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: None,
            }],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RecordingCallObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-missing", "run-missing", "conversation"),
            observer.clone(),
        );

        let events = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream")
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(
            events.as_slice(),
            [ProviderEvent::Error {
                error: ProviderError::Config { message }
            }] if message == "BUDGET_USAGE_INCOMPLETE"
        ));
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(finished.len(), 1, "incomplete usage is still durable");
        assert_eq!(finished[0].status, LlmCallStatus::Completed);
        assert_eq!(finished[0].usage, None);
    }

    #[tokio::test]
    async fn cancellation_after_finish_discards_buffered_success() {
        let reached_tail = Arc::new(Notify::new());
        let release_tail = Arc::new(Notify::new());
        let inline_usage = zk_protocol::Usage {
            input_tokens: 7,
            output_tokens: 3,
            ..zk_protocol::Usage::default()
        };
        let provider = GatedTrailingUsageProvider {
            reached_tail: Arc::clone(&reached_tail),
            release_tail,
            finish_usage: Some(inline_usage),
            trailing_usage: Arc::new(Vec::new()),
        };
        let mut registry = ProviderRegistry::new();
        registry.register("gated-tail", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RecordingCallObserver::default());
        let cancel = CancellationToken::new();
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-cancel", "run-cancel", "conversation"),
            observer.clone(),
        );
        let mut stream = registry
            .chat_stream(request, cancel.clone())
            .expect("stream");

        let first = stream.next();
        tokio::pin!(first);
        tokio::select! {
            () = reached_tail.notified() => {}
            leaked = &mut first => panic!("Finish leaked before cancellation: {leaked:?}"),
        }
        cancel.cancel();
        assert!(matches!(
            first.await,
            Some(ProviderEvent::Error {
                error: ProviderError::Cancelled
            })
        ));
        assert!(stream.next().await.is_none());

        let finished = observer.finished.lock().expect("lock");
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].status, LlmCallStatus::Cancelled);
        assert_eq!(
            finished[0].error_code.as_deref(),
            Some("PROVIDER_CANCELLED")
        );
        assert_eq!(finished[0].usage, Some(inline_usage));
    }

    #[tokio::test]
    async fn non_usage_event_after_finish_discards_buffered_success() {
        let inline_usage = zk_protocol::Usage {
            input_tokens: 5,
            output_tokens: 2,
            ..zk_protocol::Usage::default()
        };
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(inline_usage),
                },
                ProviderEvent::TextDelta {
                    text: "illegal tail".to_owned(),
                },
            ],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RecordingCallObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-invalid", "run-invalid", "conversation"),
            observer.clone(),
        );

        let events = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream")
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(
            events.as_slice(),
            [ProviderEvent::Error {
                error: ProviderError::Parse { message }
            }] if message == "INVALID_EVENT_AFTER_FINISH: text_delta"
        ));
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].status, LlmCallStatus::Failed);
        assert_eq!(finished[0].error_code.as_deref(), Some("PROVIDER_PARSE"));
        assert_eq!(finished[0].usage, Some(inline_usage));
    }

    #[tokio::test]
    async fn provider_cancellation_is_a_cancelled_physical_call() {
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![ProviderEvent::Error {
                error: ProviderError::Cancelled,
            }],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RecordingCallObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-1", "run-1", "conversation"),
            observer.clone(),
        );

        let events = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream")
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(
            events.as_slice(),
            [ProviderEvent::Error {
                error: ProviderError::Cancelled
            }]
        ));
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].status, LlmCallStatus::Cancelled);
        assert_eq!(
            finished[0].error_code.as_deref(),
            Some("PROVIDER_CANCELLED")
        );
        assert!(finished[0].usage.is_none());
    }

    #[tokio::test]
    async fn terminal_ledger_failure_is_fail_closed_before_finish_is_exposed() {
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![
                ProviderEvent::TextDelta {
                    text: "answer".to_owned(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(zk_protocol::Usage {
                        input_tokens: 9,
                        output_tokens: 3,
                        ..zk_protocol::Usage::default()
                    }),
                },
            ],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RejectFinishObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-1", "run-1", "conversation"),
            observer.clone(),
        );

        let events = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream")
            .collect::<Vec<_>>()
            .await;
        assert_eq!(*observer.started.lock().expect("lock"), 1);
        assert_eq!(
            *observer.finished.lock().expect("lock"),
            COMPLETION_PERSIST_ATTEMPTS,
            "terminal persistence retries are bounded"
        );
        assert_eq!(events.len(), 2, "text may stream, model finish may not");
        assert!(matches!(
            &events[0],
            ProviderEvent::TextDelta { text } if text == "answer"
        ));
        assert!(matches!(
            &events[1],
            ProviderEvent::Error {
                error: ProviderError::Config { message }
            } if message == "LLM_LEDGER_FINISH_FAILED: disk full"
        ));
    }

    #[tokio::test]
    async fn rejected_admission_never_constructs_the_concrete_provider_stream() {
        let provider = ScriptedProvider::new("moonshot", text_then_finish("must not run"));
        let seen_requests = Arc::clone(&provider.seen_requests);
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-1", "run-1", "subAgent"),
            Arc::new(RejectStartObserver),
        );

        let events = registry
            .chat_stream(request, CancellationToken::new())
            .expect("registry stream")
            .collect::<Vec<_>>()
            .await;

        assert!(seen_requests.lock().expect("lock").is_empty());
        assert!(matches!(
            events.as_slice(),
            [ProviderEvent::Error {
                error: ProviderError::Config { message }
            }] if message.contains("LLM_LEDGER_START_FAILED: BUDGET_EXHAUSTED")
        ));
    }

    #[tokio::test]
    async fn drop_during_start_write_closes_the_call_after_start_commits() {
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![ProviderEvent::Finish {
                finish_reason: FinishReason::EndTurn,
                usage: Some(zk_protocol::Usage {
                    input_tokens: 5,
                    output_tokens: 1,
                    ..zk_protocol::Usage::default()
                }),
            }],
        );
        let seen_requests = Arc::clone(&provider.seen_requests);
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(BlockingStartObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-start-drop", "run-start-drop", "conversation"),
            observer.clone(),
        );
        let mut stream = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream");

        {
            let first = stream.next();
            tokio::pin!(first);
            tokio::select! {
                () = observer.start_entered.notified() => {}
                leaked = &mut first => panic!("provider ran before start admission: {leaked:?}"),
            }
        }
        assert!(observer.started.lock().expect("lock").is_empty());
        assert!(seen_requests.lock().expect("lock").is_empty());
        drop(stream);
        observer.release_start.notify_one();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !observer.finished.lock().expect("lock").is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("successful in-flight start must receive a drop completion");
        tokio::task::yield_now().await;

        let started = observer.started.lock().expect("lock");
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(started.len(), 1);
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].call_id, started[0].call_id);
        assert_eq!(finished[0].status, LlmCallStatus::Cancelled);
        assert_eq!(finished[0].error_code.as_deref(), Some("STREAM_DROPPED"));
        assert!(finished[0].usage.is_none());
        assert!(
            seen_requests.lock().expect("lock").is_empty(),
            "a dropped admission must never construct the provider stream"
        );
    }

    #[tokio::test]
    async fn dropping_a_started_stream_closes_the_physical_call() {
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![
                ProviderEvent::TextDelta {
                    text: "partial".to_owned(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: None,
                },
            ],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RecordingCallObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-drop", "run-drop", "conversation"),
            observer.clone(),
        );

        let mut stream = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream");
        assert!(matches!(
            stream.next().await,
            Some(ProviderEvent::TextDelta { .. })
        ));
        drop(stream);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !observer.finished.lock().expect("lock").is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drop completion must not be orphaned");
        let started = observer.started.lock().expect("lock");
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(started.len(), 1);
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].call_id, started[0].call_id);
        assert_eq!(finished[0].status, LlmCallStatus::Cancelled);
        assert_eq!(finished[0].error_code.as_deref(), Some("STREAM_DROPPED"));
        assert!(finished[0].usage.is_none());
    }

    #[tokio::test]
    async fn dropping_with_finish_buffered_closes_once_without_exposing_success() {
        let reached_tail = Arc::new(Notify::new());
        let release_tail = Arc::new(Notify::new());
        let inline_usage = zk_protocol::Usage {
            input_tokens: 17,
            output_tokens: 6,
            cache_read_input_tokens: 2,
            cache_creation_input_tokens: 1,
        };
        let provider = GatedTrailingUsageProvider {
            reached_tail: Arc::clone(&reached_tail),
            release_tail,
            finish_usage: Some(inline_usage),
            trailing_usage: Arc::new(Vec::new()),
        };
        let mut registry = ProviderRegistry::new();
        registry.register("gated-tail", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RecordingCallObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-tail-drop", "run-tail-drop", "conversation"),
            observer.clone(),
        );
        let mut stream = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream");

        {
            let first = stream.next();
            tokio::pin!(first);
            tokio::select! {
                () = reached_tail.notified() => {}
                leaked = &mut first => panic!("buffered Finish leaked before stream drop: {leaked:?}"),
            }
        }
        assert!(observer.finished.lock().expect("lock").is_empty());
        drop(stream);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !observer.finished.lock().expect("lock").is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("buffered Finish drop must not orphan completion");
        tokio::task::yield_now().await;

        let started = observer.started.lock().expect("lock");
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(started.len(), 1);
        assert_eq!(
            finished.len(),
            1,
            "drop must persist exactly one terminal row"
        );
        assert_eq!(finished[0].call_id, started[0].call_id);
        assert_eq!(finished[0].status, LlmCallStatus::Cancelled);
        assert_eq!(finished[0].error_code.as_deref(), Some("STREAM_DROPPED"));
        assert_eq!(finished[0].usage, Some(inline_usage));
    }

    #[tokio::test]
    async fn dropped_stream_retries_one_transient_completion_failure() {
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![
                ProviderEvent::TextDelta {
                    text: "partial".to_owned(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: None,
                },
            ],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(FailOnceFinishObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-drop-retry", "run-drop-retry", "conversation"),
            observer.clone(),
        );

        let mut stream = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream");
        assert!(matches!(
            stream.next().await,
            Some(ProviderEvent::TextDelta { .. })
        ));
        drop(stream);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !observer.finished.lock().expect("lock").is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("one transient failure must not orphan the completion");

        assert_eq!(*observer.attempts.lock().expect("lock"), 2);
        let started = observer.started.lock().expect("lock");
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(started.len(), 1);
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].call_id, started[0].call_id);
        assert_eq!(finished[0].status, LlmCallStatus::Cancelled);
        assert_eq!(finished[0].error_code.as_deref(), Some("STREAM_DROPPED"));
        assert!(finished[0].usage.is_none());
    }

    #[tokio::test]
    async fn provider_concurrency_uses_verified_and_conservative_caps() {
        async fn assert_cap(name: &str, expected: usize) {
            use std::sync::atomic::{AtomicUsize, Ordering};

            let current = Arc::new(AtomicUsize::new(0));
            let max_seen = Arc::new(AtomicUsize::new(0));
            let release = Arc::new(Semaphore::new(0));
            let provider = ConcurrencyProbeProvider {
                name: name.to_owned(),
                current: Arc::clone(&current),
                max_seen: Arc::clone(&max_seen),
                release: Arc::clone(&release),
            };
            let mut registry = ProviderRegistry::new();
            registry.register(name, Arc::new(provider), vec!["probe-model".to_owned()]);
            let mut handles = Vec::new();
            for _ in 0..6 {
                let stream = registry
                    .chat_stream(ChatRequest::new("probe-model"), CancellationToken::new())
                    .expect("stream");
                handles.push(tokio::spawn(stream.collect::<Vec<_>>()));
            }

            tokio::time::timeout(Duration::from_secs(1), async {
                while current.load(Ordering::SeqCst) != expected {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("expected provider calls must reach the concurrency gate");
            assert_eq!(max_seen.load(Ordering::SeqCst), expected);
            release.add_permits(6);
            for handle in handles {
                let events = handle.await.expect("join");
                assert!(matches!(events.as_slice(), [ProviderEvent::Finish { .. }]));
            }
            assert_eq!(current.load(Ordering::SeqCst), 0);
            assert_eq!(max_seen.load(Ordering::SeqCst), expected);
        }

        assert_cap("moonshot", VERIFIED_PROVIDER_CONCURRENCY).await;
        assert_cap("custom-provider", UNKNOWN_PROVIDER_CONCURRENCY).await;
    }

    #[tokio::test]
    async fn cancellation_during_retry_backoff_terminates_silently() {
        // 取消是控制流：退避等待被取消 → 流静默终止，不外泄错误、不降级。
        let primary = SequencedProvider::new("moonshot", vec![vec![error_event(503)]]);
        let secondary = ScriptedProvider::new("dashscope", text_then_finish("never"));
        let primary_seen = primary.seen_models.clone();
        let secondary_seen = secondary.seen_models.clone();
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(primary), vec!["kimi-k3".into()]);
        registry.register(
            "dashscope",
            Arc::new(secondary),
            vec!["qwen3.8-max-0902".into()],
        );
        let registry = registry
            .with_fallback_chain(vec!["kimi-k3".into(), "qwen3.8-max-0902".into()])
            // 30s 退避 + 已取消令牌：biased select 必走取消分支，无真实等待。
            .with_retry_policy(RetryPolicy {
                delay_override_ms: Some(crate::retry::MAX_DELAY_MS),
                ..RetryPolicy::foreground()
            });

        let cancel = CancellationToken::new();
        cancel.cancel();
        let events = drain_with_cancel(&registry, "kimi-k3", cancel)
            .await
            .expect("stream");
        assert!(
            events.is_empty(),
            "cancelled backoff must not surface the trigger error"
        );
        assert_eq!(primary_seen.lock().expect("lock").len(), 1);
        assert!(secondary_seen.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn retries_spare_breaker_quota_and_skip_degraded_providers() {
        // 两条协作语义合验（均对齐旧 `ApiRetryService`）：
        // ① 熔断失败每次逻辑调用只记一次——内层重试不重复消耗熔断配额；
        // ② 已熔断的 provider 不做退避重试——退避时间不花在已降级的提供商上。
        let only = ScriptedProvider::new("moonshot", vec![error_event(503)]);
        let seen = only.seen_models.clone();
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(only), vec!["kimi-k3".into()]);
        let registry = registry.with_retry_policy(RetryPolicy::immediate(3));
        let breaker = registry.breaker("moonshot").expect("slot");

        // ① 单候选 + 预算 3：3 次尝试，熔断只记 1 次失败（阈值 3 未触发）。
        let events = drain(&registry, "kimi-k3").await.expect("stream");
        assert_eq!(seen.lock().expect("lock").len(), 3, "首发 + 2 次重试");
        assert_eq!(
            breaker.consecutive_failures(),
            1,
            "一次逻辑调用只记一次熔断失败（旧 recordFailure 只在终态分支）"
        );
        assert_eq!(
            registry.breaker_state("moonshot"),
            Some(BreakerState::Closed)
        );
        assert!(
            matches!(events.last(), Some(ProviderEvent::Error { .. })),
            "预算耗尽且无后继 → 错误上抛"
        );

        // ② 进入 Degraded 后：fail-open 仍发一次请求，但不再退避重试。
        for _ in 0..3 {
            breaker.record_status(503);
        }
        assert_eq!(
            registry.breaker_state("moonshot"),
            Some(BreakerState::Degraded)
        );
        seen.lock().expect("lock").clear();
        let events = drain(&registry, "kimi-k3").await.expect("stream");
        assert_eq!(
            seen.lock().expect("lock").len(),
            1,
            "已熔断的 provider 不做退避重试"
        );
        assert!(matches!(events.last(), Some(ProviderEvent::Error { .. })));
    }

    #[tokio::test]
    async fn recoverable_parse_error_then_finish_keeps_events_and_ledger_consistent() {
        let parse_error = ProviderError::Parse {
            message: "malformed optional SSE chunk".to_owned(),
        };
        let usage = zk_protocol::Usage {
            input_tokens: 19,
            output_tokens: 5,
            ..zk_protocol::Usage::default()
        };
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![
                ProviderEvent::Error {
                    error: parse_error.clone(),
                },
                ProviderEvent::TextDelta {
                    text: "recovered".to_owned(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(usage),
                },
            ],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RecordingCallObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new(
                "task-parse-recovery",
                "run-parse-recovery",
                "conversation",
            ),
            observer.clone(),
        );

        let events = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream")
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Error { error: parse_error },
                ProviderEvent::TextDelta {
                    text: "recovered".to_owned(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(usage),
                },
            ]
        );
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].status, LlmCallStatus::Completed);
        assert_eq!(finished[0].usage, Some(usage));
        assert_eq!(finished[0].error_code, None);
    }

    #[tokio::test]
    async fn config_error_followed_by_finish_stays_terminal_failed() {
        let config_error = ProviderError::Config {
            message: "BUDGET_USAGE_INCOMPLETE".to_owned(),
        };
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![
                ProviderEvent::Error {
                    error: config_error.clone(),
                },
                ProviderEvent::Finish {
                    finish_reason: FinishReason::EndTurn,
                    usage: Some(zk_protocol::Usage {
                        input_tokens: 23,
                        output_tokens: 8,
                        ..zk_protocol::Usage::default()
                    }),
                },
            ],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RecordingCallObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new(
                "task-config-terminal",
                "run-config-terminal",
                "conversation",
            ),
            observer.clone(),
        );
        let mut stream = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream");

        assert_eq!(
            stream.next().await,
            Some(ProviderEvent::Error {
                error: config_error
            })
        );
        assert_eq!(
            observer.finished.lock().expect("lock").len(),
            1,
            "Config Error is exposed only after Failed is durable"
        );
        assert!(
            stream.next().await.is_none(),
            "a later provider Finish cannot erase a terminal Config failure"
        );

        let started = observer.started.lock().expect("lock");
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(started.len(), 1);
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].call_id, started[0].call_id);
        assert_eq!(finished[0].status, LlmCallStatus::Failed);
        assert_eq!(finished[0].error_code.as_deref(), Some("PROVIDER_CONFIG"));
        assert_eq!(finished[0].usage, None);
    }

    #[tokio::test]
    async fn terminating_parse_error_fails_the_physical_call_at_eof() {
        let parse_error = ProviderError::Parse {
            message: "fatal SSE framing error".to_owned(),
        };
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![ProviderEvent::Error {
                error: parse_error.clone(),
            }],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RecordingCallObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-parse-fatal", "run-parse-fatal", "conversation"),
            observer.clone(),
        );

        let events = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream")
            .collect::<Vec<_>>()
            .await;
        assert_eq!(events, vec![ProviderEvent::Error { error: parse_error }]);
        let finished = observer.finished.lock().expect("lock");
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].status, LlmCallStatus::Failed);
        assert_eq!(finished[0].error_code.as_deref(), Some("PROVIDER_PARSE"));
        assert_eq!(finished[0].usage, None);
    }

    #[tokio::test]
    async fn dropping_after_terminal_error_keeps_the_durable_failed_payload() {
        let parse_error = ProviderError::Parse {
            message: "terminal malformed chunk".to_owned(),
        };
        let provider = ScriptedProvider::new(
            "moonshot",
            vec![ProviderEvent::Error {
                error: parse_error.clone(),
            }],
        );
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(provider), vec!["kimi-k3".into()]);
        let observer = Arc::new(RecordingCallObserver::default());
        let request = ChatRequest::new("kimi-k3").with_execution(
            LlmExecutionAttribution::new("task-error-drop", "run-error-drop", "summary"),
            observer.clone(),
        );
        let mut stream = registry
            .chat_stream(request, CancellationToken::new())
            .expect("stream");

        assert_eq!(
            stream.next().await,
            Some(ProviderEvent::Error { error: parse_error })
        );
        assert_eq!(
            observer.finished.lock().expect("lock").len(),
            1,
            "terminal Error is exposed only after Failed is durable"
        );
        drop(stream);
        tokio::task::yield_now().await;

        let finished = observer.finished.lock().expect("lock");
        assert_eq!(finished.len(), 1, "Drop must not add a Cancelled payload");
        assert_eq!(finished[0].status, LlmCallStatus::Failed);
        assert_eq!(finished[0].error_code.as_deref(), Some("PROVIDER_PARSE"));
    }

    #[tokio::test]
    async fn error_after_content_is_passed_through_without_fallback() {
        let primary = ScriptedProvider::new(
            "moonshot",
            vec![
                ProviderEvent::TextDelta {
                    text: "partial".into(),
                },
                error_event(503),
            ],
        );
        let secondary = ScriptedProvider::new("dashscope", text_then_finish("never"));
        let secondary_seen = secondary.seen_models.clone();
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(primary), vec!["kimi-k3".into()]);
        registry.register(
            "dashscope",
            Arc::new(secondary),
            vec!["qwen3.8-max-0902".into()],
        );
        let registry =
            registry.with_fallback_chain(vec!["kimi-k3".into(), "qwen3.8-max-0902".into()]);

        let events = drain(&registry, "kimi-k3").await.expect("stream");
        assert_eq!(events.len(), 2);
        assert!(matches!(events[1], ProviderEvent::Error { .. }));
        assert!(
            secondary_seen.lock().expect("lock").is_empty(),
            "already streamed content must not be mixed with another model"
        );
    }

    #[tokio::test]
    async fn fatal_error_is_never_degraded() {
        let primary = ScriptedProvider::new(
            "moonshot",
            vec![ProviderEvent::Error {
                error: ProviderError::http(401, "Unauthorized".into(), None),
            }],
        );
        let secondary = ScriptedProvider::new("dashscope", text_then_finish("never"));
        let secondary_seen = secondary.seen_models.clone();
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(primary), vec!["kimi-k3".into()]);
        registry.register(
            "dashscope",
            Arc::new(secondary),
            vec!["qwen3.8-max-0902".into()],
        );
        let registry =
            registry.with_fallback_chain(vec!["kimi-k3".into(), "qwen3.8-max-0902".into()]);

        let events = drain(&registry, "kimi-k3").await.expect("stream");
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ProviderEvent::Error { .. }));
        assert!(secondary_seen.lock().expect("lock").is_empty());
        // 401 不是 5xx → 熔断计数不动。
        assert_eq!(
            registry.breaker_state("moonshot"),
            Some(BreakerState::Closed)
        );
    }

    #[tokio::test]
    async fn setup_failure_skips_to_next_candidate() {
        let secondary = ScriptedProvider::new("dashscope", text_then_finish("ok"));
        let secondary_seen = secondary.seen_models.clone();
        let mut registry = ProviderRegistry::new();
        registry.register(
            "moonshot",
            Arc::new(SetupFailProvider {
                name: "moonshot".into(),
            }),
            vec!["kimi-k3".into()],
        );
        registry.register(
            "dashscope",
            Arc::new(secondary),
            vec!["qwen3.8-max-0902".into()],
        );
        let registry =
            registry.with_fallback_chain(vec!["kimi-k3".into(), "qwen3.8-max-0902".into()]);

        let events = drain(&registry, "kimi-k3").await.expect("stream");
        assert_eq!(events.len(), 2);
        assert_eq!(
            secondary_seen.lock().expect("lock").as_slice(),
            ["qwen3.8-max-0902"]
        );
    }

    #[test]
    fn all_candidates_failing_setup_surfaces_error() {
        let mut registry = ProviderRegistry::new();
        registry.register(
            "moonshot",
            Arc::new(SetupFailProvider {
                name: "moonshot".into(),
            }),
            vec!["kimi-k3".into()],
        );
        let outcome = registry.chat_stream(ChatRequest::new("kimi-k3"), CancellationToken::new());
        let Err(err) = outcome else {
            panic!("setup failure expected");
        };
        assert!(matches!(err, ProviderError::Config { .. }));
        assert!(!err.to_string().contains("sk-"));
    }

    #[test]
    fn empty_registry_reports_config_error_for_any_model() {
        let registry = ProviderRegistry::new();
        assert!(registry.is_empty());
        let outcome = registry.chat_stream(ChatRequest::new("kimi-k3"), CancellationToken::new());
        let Err(err) = outcome else {
            panic!("empty registry must not serve any model");
        };
        match err {
            ProviderError::Config { message } => assert!(message.contains("kimi-k3")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn breaker_walks_closed_degraded_halfopen_and_back() {
        // 熔断三态转换（注册表持有的实例，时钟注入 → 无 sleep）。
        let mut registry = ProviderRegistry::new();
        registry.register(
            "moonshot",
            Arc::new(ScriptedProvider::new("moonshot", Vec::new())),
            vec!["kimi-k3".into()],
        );
        let breaker = registry.breaker("moonshot").expect("slot");
        assert_eq!(breaker.state_at(1_000), BreakerState::Closed);
        for _ in 0..3 {
            breaker.record_status_at(500, 1_000);
        }
        assert_eq!(breaker.state_at(1_000), BreakerState::Degraded);
        assert!(!breaker.is_available_at(1_000));
        // 窗口结束 → 半开（放行探测）。
        let after = 1_000 + crate::breaker::DEGRADE_WINDOW_MS;
        assert_eq!(breaker.state_at(after), BreakerState::HalfOpen);
        assert!(breaker.is_available_at(after));
        // 探测成功 → 回到正常。
        breaker.record_success();
        assert_eq!(breaker.state_at(after), BreakerState::Closed);
        assert_eq!(breaker.consecutive_failures(), 0);
    }

    #[tokio::test]
    async fn degraded_provider_is_skipped_when_alternative_exists() {
        let primary = ScriptedProvider::new("moonshot", text_then_finish("never"));
        let secondary = ScriptedProvider::new("dashscope", text_then_finish("served"));
        let primary_seen = primary.seen_models.clone();
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(primary), vec!["kimi-k3".into()]);
        registry.register(
            "dashscope",
            Arc::new(secondary),
            vec!["qwen3.8-max-0902".into()],
        );
        let registry =
            registry.with_fallback_chain(vec!["kimi-k3".into(), "qwen3.8-max-0902".into()]);
        let breaker = registry.breaker("moonshot").expect("slot");
        for _ in 0..3 {
            breaker.record_status(503);
        }
        assert_eq!(
            registry.breaker_state("moonshot"),
            Some(BreakerState::Degraded)
        );

        let events = drain(&registry, "kimi-k3").await.expect("stream");
        assert_eq!(
            events[0],
            ProviderEvent::TextDelta {
                text: "served".into()
            }
        );
        assert!(
            primary_seen.lock().expect("lock").is_empty(),
            "degraded provider must not receive new requests"
        );
    }

    #[tokio::test]
    async fn all_candidates_degraded_fails_open() {
        let only = ScriptedProvider::new("moonshot", text_then_finish("still served"));
        let seen = only.seen_models.clone();
        let mut registry = ProviderRegistry::new();
        registry.register("moonshot", Arc::new(only), vec!["kimi-k3".into()]);
        let breaker = registry.breaker("moonshot").expect("slot");
        for _ in 0..3 {
            breaker.record_status(500);
        }
        assert_eq!(
            registry.breaker_state("moonshot"),
            Some(BreakerState::Degraded)
        );

        // 无其他候选 → fail-open（不凭空拒绝对话）。
        let events = drain(&registry, "kimi-k3").await.expect("stream");
        assert_eq!(events.len(), 2);
        assert_eq!(seen.lock().expect("lock").as_slice(), ["kimi-k3"]);
    }

    #[test]
    fn from_configs_dispatches_by_protocol_and_indexes_models() {
        let client = reqwest::Client::new();
        let moonshot = ProviderConfig::with_keys(
            "moonshot",
            MOONSHOT_BASE_URL,
            ApiKeyRing::from_csv("sk-moonshot-secret-a, sk-moonshot-secret-b"),
            "kimi-k3",
            vec!["kimi-k3".into(), "moonshot-v1-128k".into()],
        );
        let anthropic = ProviderConfig::with_keys(
            "anthropic",
            ANTHROPIC_BASE_URL,
            ApiKeyRing::single(ApiKey::new("sk-ant-secret")),
            "claude-sonnet-4-6",
            vec!["claude-sonnet-4-6".into()],
        )
        .with_protocol(ProviderProtocol::AnthropicNative);
        let registry =
            ProviderRegistry::from_configs_with_client(vec![moonshot, anthropic], &client);

        assert_eq!(registry.names(), ["moonshot", "anthropic"]);
        assert_eq!(
            registry.models(),
            ["kimi-k3", "moonshot-v1-128k", "claude-sonnet-4-6"]
        );
        // 默认模型取首条配置。
        assert_eq!(registry.default_model(), "kimi-k3");
        assert_eq!(
            registry
                .get("anthropic")
                .expect("registered")
                .provider_name(),
            "anthropic"
        );
        assert_eq!(registry.model_owner("claude-sonnet-4-6"), Some("anthropic"));
        // 密钥不出现在任何调试输出中。
        let rendered = format!("{registry:?}");
        assert!(!rendered.contains("sk-"), "registry debug leaked a key");
        assert!(rendered.contains("moonshot"));
    }

    #[test]
    fn catalog_endpoints_match_protocol_paths() {
        // 8+1 家：`OpenAI` 兼容走 /chat/completions，`Anthropic` 原生走 /v1/messages。
        for entry in PROVIDER_CATALOG {
            let url = match entry.protocol {
                ProviderProtocol::OpenAiCompat => chat_completions_url(entry.base_url),
                ProviderProtocol::AnthropicNative => crate::anthropic::messages_url(entry.base_url),
            };
            assert!(
                url.starts_with(entry.base_url.trim_end_matches('/')),
                "{}: endpoint must extend its base_url",
                entry.name
            );
            let suffix = match entry.protocol {
                ProviderProtocol::OpenAiCompat => "/chat/completions",
                ProviderProtocol::AnthropicNative => "/v1/messages",
            };
            assert!(
                url.ends_with(suffix),
                "{}: wrong endpoint {url}",
                entry.name
            );
            assert!(
                !url.contains("//v1"),
                "{}: double slash in {url}",
                entry.name
            );
        }
        // 逐值抽检两条（对照方案 §13-3）。
        assert_eq!(
            chat_completions_url(catalog_entry("moonshot").expect("moonshot").base_url),
            "https://api.moonshot.cn/v1/chat/completions"
        );
        assert_eq!(
            crate::anthropic::messages_url(catalog_entry("anthropic").expect("anthropic").base_url),
            "https://api.anthropic.com/v1/messages"
        );
    }
}
