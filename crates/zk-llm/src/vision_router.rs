//! 视觉模型路由器——当用户附带图片但当前模型不支持图片时，自动选择一个
//! 支持视觉输入的目标模型用于本次请求（对照旧 `VisionModelRouter.java`）。
//!
//! 路由策略（按优先级）：
//!
//! 1. 当前模型本身已支持图片：返回 `None`（无需路由）；
//! 2. `DeepSeek` 系列优先使用官方 `DeepSeek` 视觉模型
//!    [`DEEPSEEK_VISION_MODEL`]（保持模型家族一致）；
//! 3. 同 provider 下查找首个支持图片的模型；
//! 4. 全局兜底视觉模型 [`FALLBACK_VISION_MODEL`]。兜底同样以 provider
//!    实际配置为准——无任何可用视觉模型时返回 `None`（比旧实现的无条件
//!    兜底更严格，避免路由到不可调用模型）。
//!
//! 路由是单次请求级别行为，不修改会话级模型选择。

use crate::models::capabilities_for;
use crate::registry::ProviderRegistry;

/// 全局兜底视觉模型。
pub const FALLBACK_VISION_MODEL: &str = "qwen3.7-plus";

/// `DeepSeek` 系列专属图片理解兜底模型。
pub const DEEPSEEK_VISION_MODEL: &str = "deepseek-v4-flash-vision-exp";

/// 视觉路由所需的最小 provider 视图（窄 trait）。
///
/// 把「模型 → 已配置 provider」与「provider → 模型清单」两问从
/// [`ProviderRegistry`] 抽出，便于单测以内存替身覆盖各路由分支
///（对照旧 `LlmProviderRegistry.getProvider` / `getSupportedModels`）。
pub trait VisionProviderView: Send + Sync {
    /// 模型的严格归属 provider（未注册到任何已配置 provider 时 `None`）。
    fn model_owner(&self, model: &str) -> Option<String>;

    /// 某 provider 注册的模型清单（注册序）。
    fn provider_models(&self, provider: &str) -> Vec<String>;

    /// 基于一个逻辑视图解析本次请求的视觉模型。
    ///
    /// 热替换实现可覆盖此方法，在同一 registry 快照上完成全部查询，避免密钥
    /// 热更新恰好发生在两次查询之间时得到混合视图。
    fn resolve_vision_model(&self, current_model: &str) -> Option<String> {
        resolve_vision_model(self, current_model)
    }
}

impl VisionProviderView for ProviderRegistry {
    fn model_owner(&self, model: &str) -> Option<String> {
        ProviderRegistry::model_owner(self, model).map(str::to_owned)
    }

    fn provider_models(&self, provider: &str) -> Vec<String> {
        self.models()
            .iter()
            .filter(|model| ProviderRegistry::model_owner(self, model) == Some(provider))
            .cloned()
            .collect()
    }
}

/// 为不支持图片的模型查找视觉路由目标（对照旧 `resolveVisionModel`）。
///
/// 返回 `None` 表示保持当前模型：当前模型已支持图片（无需路由），或
/// 没有任何已配置的视觉模型可用。
#[must_use]
pub fn resolve_vision_model<V>(view: &V, current_model: &str) -> Option<String>
where
    V: VisionProviderView + ?Sized,
{
    if capabilities_for(current_model).supports_images {
        return None; // 当前模型已支持图片，无需路由。
    }

    // 1. DeepSeek 系列优先保持模型家族一致。可用性必须以 provider 实际配置
    //    为准，避免只有内置能力声明、但没有配置 DeepSeek API Key 时路由到
    //    不可调用模型。
    if current_model.starts_with("deepseek-")
        && is_available_vision_model(view, DEEPSEEK_VISION_MODEL)
    {
        tracing::info!(
            "Vision route: {current_model} -> {DEEPSEEK_VISION_MODEL} (DeepSeek family fallback)"
        );
        return Some(DEEPSEEK_VISION_MODEL.to_owned());
    }

    // 2. 查找同 provider 下首个支持图片的模型。
    if let Some(provider) = view.model_owner(current_model) {
        for candidate in view.provider_models(&provider) {
            if candidate == current_model {
                continue;
            }
            if capabilities_for(&candidate).supports_images {
                tracing::info!("Vision route: {current_model} -> {candidate} (same provider)");
                return Some(candidate);
            }
        }
    }

    // 3. 同 provider 无视觉模型，使用全局兜底（同样要求已配置可调用）。
    if is_available_vision_model(view, FALLBACK_VISION_MODEL) {
        tracing::info!("Vision route: {current_model} -> {FALLBACK_VISION_MODEL} (fallback)");
        return Some(FALLBACK_VISION_MODEL.to_owned());
    }
    tracing::info!("Vision route: no configured vision model available for {current_model}");
    None
}

/// 视觉模型可用 = 内置能力声明支持图片 **且** 已注册到某个配置了密钥的
/// provider（对照旧 `isAvailableVisionModel`）。
fn is_available_vision_model<V>(view: &V, model: &str) -> bool
where
    V: VisionProviderView + ?Sized,
{
    capabilities_for(model).supports_images && view.model_owner(model).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// 内存替身：provider → 模型清单（`BTreeMap` 保证遍历有序、断言稳定）。
    struct FakeView {
        providers: BTreeMap<&'static str, Vec<&'static str>>,
    }

    impl FakeView {
        fn new(providers: &[(&'static str, &[&'static str])]) -> Self {
            Self {
                providers: providers
                    .iter()
                    .map(|(name, models)| (*name, models.to_vec()))
                    .collect(),
            }
        }
    }

    impl VisionProviderView for FakeView {
        fn model_owner(&self, model: &str) -> Option<String> {
            self.providers
                .iter()
                .find(|(_, models)| models.contains(&model))
                .map(|(name, _)| (*name).to_owned())
        }

        fn provider_models(&self, provider: &str) -> Vec<String> {
            self.providers
                .get(provider)
                .map(|models| models.iter().map(|m| (*m).to_owned()).collect())
                .unwrap_or_default()
        }
    }

    #[test]
    fn deepseek_routes_to_family_vision_model() {
        // DeepSeek provider 已配置且注册了 vision-exp → 家族内路由。
        let view = FakeView::new(&[(
            "deepseek",
            &["deepseek-v4-flash", "deepseek-v4-flash-vision-exp"],
        )]);
        assert_eq!(
            resolve_vision_model(&view, "deepseek-v4-flash").as_deref(),
            Some(DEEPSEEK_VISION_MODEL)
        );
        // 家族优先级高于同 provider 顺序扫描之外的任何候选。
        assert_eq!(
            resolve_vision_model(&view, "deepseek-v4-pro-0813").as_deref(),
            Some(DEEPSEEK_VISION_MODEL)
        );
    }

    #[test]
    fn image_capable_model_needs_no_route() {
        // 当前模型已支持图片 → None（即使替身内一个 provider 都没有）。
        let view = FakeView::new(&[]);
        assert!(resolve_vision_model(&view, "deepseek-v4-flash-vision-exp").is_none());
        assert!(resolve_vision_model(&view, "qwen3.7-plus").is_none());
        assert!(resolve_vision_model(&view, "kimi-k3").is_none());
    }

    #[test]
    fn same_provider_vision_model_preferred_over_global_fallback() {
        // dashscope 下 qwen3.8-max 支持图片 → 同 provider 命中，不落全局兜底。
        let view = FakeView::new(&[("dashscope", &["qwen3.7-max", "qwen3.8-max"])]);
        assert_eq!(
            resolve_vision_model(&view, "qwen3.7-max").as_deref(),
            Some("qwen3.8-max")
        );
    }

    #[test]
    fn deepseek_vision_unconfigured_falls_back_to_global() {
        // DeepSeek provider 没注册 vision-exp（如未配 Key），同 provider 也无
        // 视觉模型 → 落到全局兜底 qwen3.7-plus（dashscope 已配置）。
        let view = FakeView::new(&[
            ("dashscope", &["qwen3.7-max", "qwen3.7-plus"]),
            ("deepseek", &["deepseek-v4-flash"]),
        ]);
        assert_eq!(
            resolve_vision_model(&view, "deepseek-v4-flash").as_deref(),
            Some(FALLBACK_VISION_MODEL)
        );
    }

    #[test]
    fn no_available_vision_model_returns_none() {
        // 全部 provider 均无视觉模型且兜底未配置 → None（不路由到不可调用模型）。
        let view = FakeView::new(&[("deepseek", &["deepseek-v4-flash"])]);
        assert!(resolve_vision_model(&view, "deepseek-v4-flash").is_none());
        // 未知模型 + 空注册表同样返回 None。
        assert!(resolve_vision_model(&FakeView::new(&[]), "mystery-model").is_none());
    }

    /// [`ProviderRegistry`] 适配层：`model_owner` / `provider_models` 语义
    /// 与注册表一致（严格归属 + 注册序清单）。
    #[test]
    fn registry_view_exposes_owner_and_models() {
        struct StubProvider;
        impl crate::ChatProvider for StubProvider {
            #[allow(clippy::unnecessary_literal_bound)] // trait 签名固定为 &str。
            fn provider_name(&self) -> &str {
                "stub"
            }
            fn chat_stream(
                &self,
                _request: crate::ChatRequest,
                _cancel: tokio_util::sync::CancellationToken,
            ) -> Result<
                futures::stream::BoxStream<'static, crate::ProviderEvent>,
                crate::ProviderError,
            > {
                Ok(Box::pin(futures::stream::empty()))
            }
        }

        let mut registry = ProviderRegistry::new();
        registry.register(
            "deepseek",
            Arc::new(StubProvider),
            vec![
                "deepseek-v4-flash".into(),
                "deepseek-v4-flash-vision-exp".into(),
            ],
        );
        let view: &dyn VisionProviderView = &registry;
        assert_eq!(
            view.model_owner("deepseek-v4-flash").as_deref(),
            Some("deepseek")
        );
        assert!(view.model_owner("qwen3.7-plus").is_none());
        assert_eq!(
            view.provider_models("deepseek"),
            vec!["deepseek-v4-flash", "deepseek-v4-flash-vision-exp"]
        );
        // 端到端：注册表直接作为路由视图 → DeepSeek 家族命中。
        assert_eq!(
            resolve_vision_model(view, "deepseek-v4-flash").as_deref(),
            Some(DEEPSEEK_VISION_MODEL)
        );
    }
}
