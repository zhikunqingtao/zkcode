//! Explicit auxiliary model routing, refreshed on every call after key reload.

use std::sync::Arc;

use futures::stream::BoxStream;
use tokio_util::sync::CancellationToken;
use zk_engine::{auxiliary_query::AuxiliaryQuery, memory_retrieval::MemoryRetriever};
use zk_llm::{ChatProvider, ChatRequest, ProviderError, ProviderEvent, SwappableProvider};

pub(crate) fn memory_retriever(providers: Arc<SwappableProvider>) -> MemoryRetriever {
    let configured = std::env::var("ZK_MEMORY_RERANK_MODEL").ok();
    configured_memory_retriever(providers, configured.as_deref())
}

fn configured_memory_retriever(
    providers: Arc<SwappableProvider>,
    model: Option<&str>,
) -> MemoryRetriever {
    let Some(model) = model.map(str::trim).filter(|value| !value.is_empty()) else {
        return MemoryRetriever::default();
    };
    let query = AuxiliaryQuery::new(
        Arc::new(StrictAuxiliaryProvider { providers }),
        model.to_owned(),
    );
    MemoryRetriever::with_reranker(Arc::new(query))
}

pub(crate) fn visualization_router(
    providers: Arc<SwappableProvider>,
) -> Arc<zk_engine::auto_visualization::VisualizationIntentRouter> {
    let enabled = std::env::var("ZK_VISUALIZATION_AUTO_ROUTING_ENABLED")
        .is_ok_and(|value| matches!(value.trim(), "1" | "true"));
    let model = std::env::var("ZK_VISUALIZATION_MODEL")
        .ok()
        .or_else(|| std::env::var("ZK_FAST_MODEL").ok())
        .or_else(|| std::env::var("LLM_FAST_MODEL").ok());
    configured_visualization_router(providers, enabled, model.as_deref())
}

fn configured_visualization_router(
    providers: Arc<SwappableProvider>,
    enabled: bool,
    model: Option<&str>,
) -> Arc<zk_engine::auto_visualization::VisualizationIntentRouter> {
    let Some(model) = model
        .map(str::trim)
        .filter(|value| enabled && !value.is_empty())
    else {
        return Arc::default();
    };
    Arc::new(
        zk_engine::auto_visualization::VisualizationIntentRouter::with_query(Arc::new(
            AuxiliaryQuery::new(
                Arc::new(StrictAuxiliaryProvider { providers }),
                model.to_owned(),
            ),
        )),
    )
}

struct StrictAuxiliaryProvider {
    providers: Arc<SwappableProvider>,
}

impl ChatProvider for StrictAuxiliaryProvider {
    fn provider_name(&self) -> &'static str {
        "configured_auxiliary"
    }

    fn chat_stream(
        &self,
        mut request: ChatRequest,
        cancel: CancellationToken,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        let registry = self.providers.load();
        let provider = registry
            .model_owner(&request.model)
            .and_then(|owner| registry.isolated_provider(owner, &request.model))
            .ok_or_else(|| ProviderError::Config {
                message: "AUXILIARY_MODEL_NOT_CONFIGURED".into(),
            })?;
        // A missing/revoked model must not resolve to another configured model or
        // introduce a new paid candidate. Registry retries still retain accounting.
        request.fallback_models = Some(Vec::new());
        provider.chat_stream(request, cancel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zk_llm::ProviderRegistry;

    #[test]
    fn unknown_explicit_auxiliary_model_does_not_use_the_default_route() {
        let provider = StrictAuxiliaryProvider {
            providers: Arc::new(SwappableProvider::new(ProviderRegistry::new())),
        };
        assert!(matches!(
            provider.chat_stream(ChatRequest::new("missing"), CancellationToken::new()),
            Err(ProviderError::Config { .. })
        ));
    }
}
