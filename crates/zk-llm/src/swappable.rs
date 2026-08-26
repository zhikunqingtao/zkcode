//! 可热替换的 Provider 代理层。
//!
//! 包裹 `ArcSwap<ProviderRegistry>`，自身实现 `ChatProvider` trait，
//! 使持有者无需感知 registry 被替换。

use std::sync::Arc;

use arc_swap::ArcSwap;
use futures::stream::BoxStream;
use tokio_util::sync::CancellationToken;

use crate::vision_router::VisionProviderView;
use crate::{ChatProvider, ChatRequest, ProviderError, ProviderEvent, ProviderRegistry};

/// 可原子替换的 Provider 代理。
///
/// Engine / Sub-agent / Summarizer 持有此结构体的 `Arc`，每次
/// `chat_stream` 调用内部 `load()` 获取最新 `ProviderRegistry` 并委托。
pub struct SwappableProvider {
    inner: ArcSwap<ProviderRegistry>,
}

impl SwappableProvider {
    /// 以初始 registry 构建。
    #[must_use]
    pub fn new(registry: ProviderRegistry) -> Self {
        Self {
            inner: ArcSwap::from_pointee(registry),
        }
    }

    /// 获取当前 registry 的快照引用。
    pub fn load(&self) -> arc_swap::Guard<Arc<ProviderRegistry>> {
        self.inner.load()
    }

    /// 原子替换为新的 registry（后续请求立即生效）。
    pub fn swap(&self, new: ProviderRegistry) {
        self.inner.store(Arc::new(new));
    }
}

impl ChatProvider for SwappableProvider {
    #[allow(clippy::unnecessary_literal_bound)] // trait 签名固定为 &str，不能收窄成 &'static str
    fn provider_name(&self) -> &str {
        "swappable"
    }

    fn chat_stream(
        &self,
        request: ChatRequest,
        cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        self.inner.load().chat_stream(request, cancel)
    }
}

impl VisionProviderView for SwappableProvider {
    fn model_owner(&self, model: &str) -> Option<String> {
        self.inner.load().model_owner(model).map(str::to_owned)
    }

    fn provider_models(&self, provider: &str) -> Vec<String> {
        let registry = self.inner.load();
        registry
            .models()
            .iter()
            .filter(|model| registry.model_owner(model) == Some(provider))
            .cloned()
            .collect()
    }

    fn resolve_vision_model(&self, current_model: &str) -> Option<String> {
        let registry = self.inner.load();
        crate::vision_router::resolve_vision_model(registry.as_ref(), current_model)
    }
}
