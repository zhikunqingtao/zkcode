//! 模型域端点——`GET /api/models`（S7b）。
//!
//! 语义来源（旧仓库只读）：`ModelController.listModels`（Provider 注册模型
//! 经 `ModelRegistry` 补齐能力信息 + `?modelId=` 存在性校验，未知模型 400
//! `INVALID_REQUEST`）。响应形状权威：`GET_api-models.json` 样例逐键对齐。
//!
//! 2.7 裁定：模型清单从 [`AppState::providers`]（[`zk_llm::ProviderRegistry`]）
//! 动态聚合——注册表非空时按其 model → provider 索引序生成条目，注册表为空
//!（Phase 1 单 provider 回退 / 未配任何 `LLM_PROVIDER_*` key）时使用
//! [`zk_llm::declared_models`] 的声明式基线。已知模型的元数据统一取
//! [`zk_llm::capabilities_for`]，未收录的新模型走 [`dynamic_info`] 兜底。
//! `defaultModel` 取注册表的有效默认：配置模型已下线时回退到首个已注册模型；
//! Phase 1 自定义默认若不在静态目录中，会以动态能力条目追加，保证默认值始终
//! 可由同一响应的 `models` 选择。响应形状（models 数组 + defaultModel，每条
//! 11 键 camelCase）不变。

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Query, State};
use serde::Serialize;

use crate::error::ApiError;
use crate::state::AppState;

/// 模型能力条目（旧 `ModelController.ModelInfo` record 的线上形状——
/// 能力开关即线上布尔键，不可折叠）。
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ModelInfo {
    /// 模型 ID。
    pub id: String,
    /// 展示名。
    pub display_name: String,
    /// 最大输出 token。
    pub max_output_tokens: i64,
    /// 上下文窗口。
    pub context_window: i64,
    /// 是否支持流式。
    pub supports_streaming: bool,
    /// 是否支持思考块。
    pub supports_thinking: bool,
    /// 是否支持图片输入。
    pub supports_images: bool,
    /// 最大图片数。
    pub max_images: i64,
    /// 是否支持工具调用。
    pub supports_tool_use: bool,
    /// 每千 token 输入成本（USD）。
    pub cost_per_1k_input: f64,
    /// 每千 token 输出成本（USD）。
    pub cost_per_1k_output: f64,
}

/// `GET /api/models` 200 响应（旧 `ModelListResponse` record）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ModelListResponse {
    /// 可用模型目录。
    pub models: Vec<ModelInfo>,
    /// 默认模型 ID。
    pub default_model: String,
}

/// 将 `zk-llm` 的内部能力记录投影为公开 API 形状；内部专用的字符比率和缓存
/// 能力不进入既有 wire contract。
fn info_from_capabilities(id: &str, capabilities: &zk_llm::ModelCapabilities) -> ModelInfo {
    ModelInfo {
        id: id.to_owned(),
        display_name: capabilities.display_name.to_string(),
        max_output_tokens: i64::from(capabilities.max_output_tokens),
        context_window: i64::from(capabilities.context_window),
        supports_streaming: capabilities.supports_streaming,
        supports_thinking: capabilities.supports_thinking,
        supports_images: capabilities.supports_images,
        max_images: i64::from(capabilities.max_images),
        supports_tool_use: capabilities.supports_tool_use,
        cost_per_1k_input: capabilities.cost_per_1k_input,
        cost_per_1k_output: capabilities.cost_per_1k_output,
    }
}

/// 未知模型的兜底能力（对齐旧 `ModelRegistry` 未注册模型的缺省 capabilities：
/// 8192 输出 / 200k 上下文 / 流式 + 图片 + 工具，无 thinking）——动态聚合遇到
/// 能力表未收录的新模型时使用，`id` / `displayName` 取模型标识本身。
fn dynamic_info(id: &str) -> ModelInfo {
    ModelInfo {
        id: id.to_owned(),
        display_name: id.to_owned(),
        max_output_tokens: 8192,
        context_window: 200_000,
        supports_streaming: true,
        supports_thinking: false,
        supports_images: true,
        max_images: 10,
        supports_tool_use: true,
        cost_per_1k_input: 0.003,
        cost_per_1k_output: 0.015,
    }
}

/// 单模型元数据：已知模型直接复用 `zk-llm` 能力表，否则保留动态兜底语义。
fn info_for(id: &str) -> ModelInfo {
    let capabilities = zk_llm::capabilities_for(id);
    if zk_llm::is_known_model(id) {
        info_from_capabilities(id, capabilities)
    } else {
        dynamic_info(id)
    }
}

/// 默认公开目录的成员和顺序取 provider 声明，元数据统一取能力表。
fn catalog() -> Vec<ModelInfo> {
    zk_llm::declared_models()
        .into_iter()
        .map(|id| info_for(&id))
        .collect()
}

/// `GET /api/models` 的有效模型清单（2.7）：注册表非空则按其聚合模型序动态
/// 生成条目；注册表为空（Phase 1 单 provider 回退 / 未配任何 provider key）则
/// 退化为 provider 声明式目录，保住既有响应契约。
#[cfg(test)]
fn effective_models(state: &AppState) -> Vec<ModelInfo> {
    let registry = state.providers.load();
    effective_models_for(&registry)
}

fn effective_models_for(registry: &zk_llm::ProviderRegistry) -> Vec<ModelInfo> {
    let registry_models = registry.models();
    let effective_default = registry.effective_default_model();
    let mut models = if registry_models.is_empty() {
        catalog()
    } else {
        registry_models.iter().map(|id| info_for(id)).collect()
    };
    if !models.iter().any(|model| model.id == effective_default) {
        models.push(info_for(effective_default));
    }
    for model in &mut models {
        let capabilities = zk_llm::capabilities_for(&model.id);
        model.supports_images = capabilities.supports_images;
        model.max_images = if capabilities.supports_images {
            i64::from(capabilities.max_images)
        } else {
            zk_llm::resolve_vision_model(registry, &model.id).map_or(0, |routed| {
                i64::from(zk_llm::capabilities_for(&routed).max_images)
            })
        };
    }
    models
}

/// `GET /api/models`——动态聚合注册表模型 + 默认模型；`?modelId=` 非空时校验存在性
/// （未知模型 400 `INVALID_REQUEST`，通过则仍返回全量目录，旧端点语义）。
#[utoipa::path(
    get,
    path = "/api/models",
    tag = "models",
    params(("modelId" = Option<String>, Query, description = "可选：校验指定模型是否存在（未知 → 400）")),
    responses(
        (status = 200, description = "模型目录 + defaultModel（键集对齐 GET_api-models.json 样例）"),
        (status = 400, description = "modelId 未知（INVALID_REQUEST）")
    )
)]
pub(crate) async fn list_models(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<ModelListResponse>, ApiError> {
    let registry = state.providers.load();
    let models = effective_models_for(&registry);
    let default_model = registry.effective_default_model().to_owned();
    if let Some(model_id) = query
        .get("modelId")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        // 旧 `IllegalArgumentException("Invalid model: ...")` → 400 信封。
        if !models.iter().any(|model| model.id == model_id) {
            return Err(ApiError::validation(format!("Invalid model: {model_id}")));
        }
    }
    Ok(Json(ModelListResponse {
        models,
        default_model,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use futures::stream::BoxStream;
    use tokio_util::sync::CancellationToken;
    use zk_llm::{ChatProvider, ChatRequest, ProviderError, ProviderEvent, ProviderRegistry};

    struct StubProvider;

    impl ChatProvider for StubProvider {
        fn provider_name(&self) -> &'static str {
            "stub"
        }

        fn chat_stream(
            &self,
            _request: ChatRequest,
            _cancel: CancellationToken,
        ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    /// 目录 21 条、ID 唯一、序列化键形状（camelCase `costPer1kInput`）。
    #[test]
    fn catalog_size_and_wire_shape() {
        let models = catalog();
        assert_eq!(models.len(), 21);
        let ids: std::collections::HashSet<&str> = models.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids.len(), 21);
        assert_eq!(
            models
                .iter()
                .map(|model| model.id.clone())
                .collect::<Vec<_>>(),
            zk_llm::declared_models()
        );
        for model in &models {
            let capabilities = zk_llm::capabilities_for(&model.id);
            assert_eq!(model.display_name, capabilities.display_name);
            assert_eq!(
                model.max_output_tokens,
                i64::from(capabilities.max_output_tokens)
            );
            assert_eq!(model.context_window, i64::from(capabilities.context_window));
            assert_eq!(model.supports_streaming, capabilities.supports_streaming);
            assert_eq!(model.supports_thinking, capabilities.supports_thinking);
            assert_eq!(model.supports_images, capabilities.supports_images);
            assert_eq!(model.max_images, i64::from(capabilities.max_images));
            assert_eq!(model.supports_tool_use, capabilities.supports_tool_use);
            assert!(
                (model.cost_per_1k_input - capabilities.cost_per_1k_input).abs() < f64::EPSILON
            );
            assert!(
                (model.cost_per_1k_output - capabilities.cost_per_1k_output).abs() < f64::EPSILON
            );
        }
        let astra = models
            .iter()
            .find(|model| model.id == "openai/gpt-6-astra")
            .expect("gpt-6-astra catalog entry");
        assert_eq!(astra.context_window, 1_050_000);
        assert_eq!(astra.max_output_tokens, 128_000);
        assert!(astra.supports_thinking);
        let gemini = models
            .iter()
            .find(|model| model.id == "google/gemini-3.8-flash")
            .expect("gemini-3.8-flash catalog entry");
        assert_eq!(gemini.context_window, 1_048_576);
        assert_eq!(gemini.max_output_tokens, 65_536);
        assert!(gemini.supports_streaming);
        assert!(gemini.supports_thinking);
        let grok = models
            .iter()
            .find(|model| model.id == "x-ai/grok-4.6")
            .expect("grok-4.6 catalog entry");
        assert_eq!(grok.context_window, 500_000);
        assert_eq!(grok.max_output_tokens, 65_536);
        assert!(grok.supports_streaming);
        assert!(grok.supports_thinking);
        assert!(
            !models
                .iter()
                .any(|model| model.id == "google/gemini-3.5-flash")
        );
        assert_eq!(
            models
                .iter()
                .find(|model| model.id == "qwen3.8-flash")
                .expect("qwen3.8-flash catalog entry")
                .max_images,
            20
        );
        let first = serde_json::to_value(&models[0]).expect("json");
        let mut keys: Vec<&str> = first
            .as_object()
            .expect("obj")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "contextWindow",
                "costPer1kInput",
                "costPer1kOutput",
                "displayName",
                "id",
                "maxImages",
                "maxOutputTokens",
                "supportsImages",
                "supportsStreaming",
                "supportsThinking",
                "supportsToolUse"
            ]
        );
    }

    #[test]
    fn effective_image_limits_match_registered_capabilities() {
        let mut providers = ProviderRegistry::new();
        providers.register(
            "dashscope",
            Arc::new(StubProvider),
            vec!["qwen3.8-max-0902".into(), "qwen3.7-plus".into()],
        );
        let state = AppState::for_tests().with_providers(providers);
        let models = effective_models(&state);
        let max_images = |id: &str| {
            models
                .iter()
                .find(|model| model.id == id)
                .expect("model entry")
                .max_images
        };

        assert_eq!(max_images("qwen3.8-max-0902"), 4);
        assert_eq!(max_images("qwen3.7-plus"), 4);
    }

    #[tokio::test]
    async fn response_uses_registered_fallback_when_configured_default_is_stale() {
        let mut providers = ProviderRegistry::new();
        providers.register(
            "stub",
            Arc::new(StubProvider),
            vec!["current-model".into(), "other-model".into()],
        );
        let state =
            AppState::for_tests().with_providers(providers.with_default_model("retired-model"));

        let Json(response) = list_models(State(state), Query(HashMap::new()))
            .await
            .expect("models response");
        assert_eq!(response.default_model, "current-model");
        assert!(
            response
                .models
                .iter()
                .any(|model| model.id == response.default_model)
        );
    }

    #[tokio::test]
    async fn phase_one_custom_default_is_appended_to_static_catalog() {
        let mut providers = ProviderRegistry::new();
        providers.register("stub", Arc::new(StubProvider), Vec::new());
        let state = AppState::for_tests()
            .with_providers(providers.with_default_model("company/custom-model"));

        let Json(response) = list_models(State(state), Query(HashMap::new()))
            .await
            .expect("models response");
        assert_eq!(response.default_model, "company/custom-model");
        assert!(
            response
                .models
                .iter()
                .any(|model| model.id == "company/custom-model")
        );
    }
}
