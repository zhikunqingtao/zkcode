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
//! 链是**模型**序列（如 `kimi-k3:qwen3.7-max:deepseek-chat`）。候选序 =
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
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::anthropic::AnthropicProvider;
use crate::breaker::{BreakerState, CircuitBreaker};
use crate::config::{
    DEFAULT_MODEL, ProviderConfig, ProviderProtocol, fallback_chain_from_env,
    provider_configs_from_env,
};
use crate::error::ProviderError;
use crate::openai_compat::{OpenAiCompatProvider, shared_http_client};
use crate::provider::{ChatProvider, ChatRequest, ProviderEvent};
use crate::retry::{RetryPolicy, RetryState, wait_for_retry};

/// 降级链最大深度（含首选，含首选共 3 次尝试——对齐 2.7 规格示例长度）。
pub const MAX_FALLBACK_DEPTH: usize = 3;

/// 注册表内的单 provider 槽位（实例 + 独立熔断器）。
#[derive(Clone)]
struct ProviderSlot {
    provider: Arc<dyn ChatProvider>,
    breaker: Arc<CircuitBreaker>,
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

    /// 默认模型。
    #[must_use]
    pub fn default_model(&self) -> &str {
        &self.default_model
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
        self.model_owner(&self.default_model)
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
    retry_policy: RetryPolicy,
}

impl Candidate {
    /// 以候选模型改写请求并发起流（建立期失败原样上抛）。
    fn start(
        &self,
        request: &ChatRequest,
        cancel: &CancellationToken,
    ) -> Result<Attempt, ProviderError> {
        let mut request = request.clone();
        request.model.clone_from(&self.model);
        let stream = self.provider.chat_stream(request, cancel.clone())?;
        Ok(Attempt {
            stream,
            breaker: self.breaker.clone(),
            produced_content: false,
            candidate: self.clone(),
            retry: RetryState::new(self.retry_policy.clone(), self.model.clone()),
        })
    }
}

/// 进行中的一次尝试（含重启所需的候选与跨重试累计的重试账本）。
struct Attempt {
    stream: BoxStream<'static, ProviderEvent>,
    breaker: Arc<CircuitBreaker>,
    produced_content: bool,
    candidate: Candidate,
    retry: RetryState,
}

/// 降级链流状态机的内部状态。
struct FallbackState {
    request: ChatRequest,
    cancel: CancellationToken,
    queue: VecDeque<Candidate>,
    current: Option<Attempt>,
    pending_error: Option<ProviderError>,
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
            match candidate.start(&request, &cancel) {
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
        })
        .boxed())
    }
}

/// 降级链事件流：透传当前尝试的事件，必要时切换到后继候选。
fn fallback_stream(state: FallbackState) -> impl Stream<Item = ProviderEvent> + Send + 'static {
    futures::stream::unfold(state, |mut state| async move {
        loop {
            if state.current.is_none() {
                // 无可用后继：把挂起的触发错误吐给下游，随后终止。
                let error = state.pending_error.take()?;
                return Some((ProviderEvent::Error { error }, state));
            }
            let next = state
                .current
                .as_mut()
                .expect("current attempt checked above")
                .stream
                .next()
                .await;
            match next {
                None => return None,
                Some(ProviderEvent::Error { error }) => {
                    let (produced, breaker) = {
                        let attempt = state
                            .current
                            .as_ref()
                            .expect("current attempt checked above");
                        (attempt.produced_content, attempt.breaker.clone())
                    };
                    // ① 同 provider 重试（旧 ApiRetryService.executeWithRetry 内层
                    //    循环）：已吐字的尝试不透明重试（状态已污染，旧
                    //    collector.hasReceivedEvents() 同）。
                    if !produced {
                        let decision = state
                            .current
                            .as_mut()
                            .expect("current attempt checked above")
                            .retry
                            .on_error(&error);
                        // 已熔断的 provider 不值得再等退避。
                        if let Some(delay_ms) = decision.filter(|_| breaker.is_available()) {
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
                            if let Ok(mut retried) = candidate.start(&state.request, &state.cancel)
                            {
                                // 重试账本跨尝试累计（额度不因重启而回满）。
                                retried.retry = retry;
                                state.current = Some(retried);
                                continue;
                            }
                        }
                    }
                    // ② 放弃重试 → 记一次 provider 失败：5xx 计入熔断（429 归密钥
                    //    冷却，见 breaker 模块文档）；一次逻辑调用只记一次。
                    breaker.record_error(&error);
                    if !error.is_retryable() || produced || state.queue.is_empty() {
                        return Some((ProviderEvent::Error { error }, state));
                    }
                    state.current = None;
                    state.pending_error = Some(error);
                    while let Some(candidate) = state.queue.pop_front() {
                        if let Ok(attempt) = candidate.start(&state.request, &state.cancel) {
                            state.current = Some(attempt);
                            state.pending_error = None;
                            break;
                        }
                    }
                }
                Some(event) => {
                    let attempt = state
                        .current
                        .as_mut()
                        .expect("current attempt checked above");
                    if matches!(
                        event,
                        ProviderEvent::TextDelta { .. }
                            | ProviderEvent::ThinkingDelta { .. }
                            | ProviderEvent::ToolUseStart { .. }
                            | ProviderEvent::ToolInputDelta { .. }
                    ) {
                        attempt.produced_content = true;
                    }
                    if matches!(event, ProviderEvent::Finish { .. }) {
                        attempt.breaker.record_success();
                    }
                    return Some((event, state));
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::config::{ANTHROPIC_BASE_URL, MOONSHOT_BASE_URL, PROVIDER_CATALOG, catalog_entry};
    use crate::openai_compat::chat_completions_url;
    use crate::provider::FinishReason;
    use crate::secret::{ApiKey, ApiKeyRing};

    /// 脚本化 provider：按固定事件序回放，并记录每次请求的模型。
    struct ScriptedProvider {
        name: String,
        events: Vec<ProviderEvent>,
        seen_models: Arc<Mutex<Vec<String>>>,
    }

    impl ScriptedProvider {
        fn new(name: &str, events: Vec<ProviderEvent>) -> Self {
            Self {
                name: name.to_owned(),
                events,
                seen_models: Arc::new(Mutex::new(Vec::new())),
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
            vec!["qwen3.7-max".into(), "kimi-k3".into(), "  ".into()],
        );
        assert_eq!(registry.names(), ["moonshot", "dashscope"]);
        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());
        assert_eq!(
            registry.models(),
            ["kimi-k3", "moonshot-v1-128k", "qwen3.7-max"]
        );
        assert_eq!(registry.model_owner("kimi-k3"), Some("moonshot"));
        assert_eq!(registry.model_owner("qwen3.7-max"), Some("dashscope"));
        assert_eq!(registry.model_owner("unknown-model"), None);
        assert!(registry.get("moonshot").is_some());
        assert!(registry.get("zhipu").is_none());
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
            vec!["qwen3.7-max".into()],
        );
        // 默认模型归属者接管未知模型。
        let registry = registry.with_default_model("qwen3.7-max");
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
            "qwen3.7-max".into(),
            "deepseek-chat".into(),
            "glm-5.3".into(),
        ]);
        // 请求模型在链中 → 取其后继（含自身共 MAX_FALLBACK_DEPTH 个）。
        assert_eq!(
            registry.candidate_models("kimi-k3"),
            ["kimi-k3", "qwen3.7-max", "deepseek-chat"]
        );
        assert_eq!(
            registry.candidate_models("deepseek-chat"),
            ["deepseek-chat", "glm-5.3"]
        );
        // 请求模型不在链中 → 接整条链（同样截断）。
        assert_eq!(
            registry.candidate_models("MiniMax-M3"),
            ["MiniMax-M3", "kimi-k3", "qwen3.7-max"]
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
        let events = drain(&registry, "qwen3.7-max").await.expect("stream");
        assert_eq!(events.len(), 2);
        assert_eq!(seen.lock().expect("lock").as_slice(), ["qwen3.7-max"]);
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
        registry.register("dashscope", Arc::new(secondary), vec!["qwen3.7-max".into()]);
        let registry = registry
            .with_fallback_chain(vec!["kimi-k3".into(), "qwen3.7-max".into()])
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
            ["qwen3.7-max"],
            "fallback request must carry the fallback model"
        );
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
        registry.register("dashscope", Arc::new(secondary), vec!["qwen3.7-max".into()]);
        let registry = registry
            .with_fallback_chain(vec!["kimi-k3".into(), "qwen3.7-max".into()])
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
        registry.register("dashscope", Arc::new(secondary), vec!["qwen3.7-max".into()]);
        let registry = registry
            .with_fallback_chain(vec!["kimi-k3".into(), "qwen3.7-max".into()])
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
            ["qwen3.7-max"]
        );
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
        registry.register("dashscope", Arc::new(secondary), vec!["qwen3.7-max".into()]);
        let registry = registry
            .with_fallback_chain(vec!["kimi-k3".into(), "qwen3.7-max".into()])
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
        registry.register("dashscope", Arc::new(secondary), vec!["qwen3.7-max".into()]);
        let registry = registry.with_fallback_chain(vec!["kimi-k3".into(), "qwen3.7-max".into()]);

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
        registry.register("dashscope", Arc::new(secondary), vec!["qwen3.7-max".into()]);
        let registry = registry.with_fallback_chain(vec!["kimi-k3".into(), "qwen3.7-max".into()]);

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
        registry.register("dashscope", Arc::new(secondary), vec!["qwen3.7-max".into()]);
        let registry = registry.with_fallback_chain(vec!["kimi-k3".into(), "qwen3.7-max".into()]);

        let events = drain(&registry, "kimi-k3").await.expect("stream");
        assert_eq!(events.len(), 2);
        assert_eq!(
            secondary_seen.lock().expect("lock").as_slice(),
            ["qwen3.7-max"]
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
        registry.register("dashscope", Arc::new(secondary), vec!["qwen3.7-max".into()]);
        let registry = registry.with_fallback_chain(vec!["kimi-k3".into(), "qwen3.7-max".into()]);
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
