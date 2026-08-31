//! 模型域端点——`GET /api/models`（S7b）。
//!
//! 语义来源（旧仓库只读）：`ModelController.listModels`（Provider 注册模型
//! 经 `ModelRegistry` 补齐能力信息 + `?modelId=` 存在性校验，未知模型 400
//! `INVALID_REQUEST`）。响应形状权威：`GET_api-models.json` 样例逐键对齐。
//!
//! 2.7 裁定：模型清单从 [`AppState::providers`]（[`zk_llm::ProviderRegistry`]）
//! 动态聚合——注册表非空时按其 model → provider 索引序生成条目（命中静态目录
//! 取权威能力值，未收录的新模型走 [`dynamic_info`] 兜底元数据）；注册表为空
//!（Phase 1 单 provider 回退 / 未配任何 `LLM_PROVIDER_*` key）时退化为声明式
//! 静态目录（样例 16 模型逐条照抄，能力值与 `ModelRegistry.BUILTIN_MODELS`
//! 交叉核实一致），保住既有响应契约。`defaultModel` 恒取运行配置
//! `ZK_DEFAULT_MODEL`（与创建会话共用同一配置源，对齐旧
//! `providerRegistry.getDefaultModel()` 语义）。响应形状（models 数组 +
//! defaultModel，每条 11 键 camelCase）不变。

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

/// 目录条目便捷构造（旧 `ModelRegistry.caps` 的参数序照抄）。
#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
fn caps(
    id: &str,
    display_name: &str,
    max_output_tokens: i64,
    context_window: i64,
    supports_streaming: bool,
    supports_thinking: bool,
    supports_images: bool,
    max_images: i64,
    supports_tool_use: bool,
    cost_per_1k_input: f64,
    cost_per_1k_output: f64,
) -> ModelInfo {
    ModelInfo {
        id: id.to_owned(),
        display_name: display_name.to_owned(),
        max_output_tokens,
        context_window,
        supports_streaming,
        supports_thinking,
        supports_images,
        max_images,
        supports_tool_use,
        cost_per_1k_input,
        cost_per_1k_output,
    }
}

/// Phase 1 静态模型目录（`GET_api-models.json` 样例 16 条逐值照抄，顺序同；
/// 数据表函数，行数上限豁免）。
#[allow(clippy::too_many_lines)]
fn catalog() -> Vec<ModelInfo> {
    vec![
        caps(
            "qwen3.7-max",
            "Qwen 3.7 Max",
            65536,
            1_000_000,
            true,
            true,
            false,
            0,
            true,
            0.009,
            0.054,
        ),
        caps(
            "qwen3.7-plus",
            "Qwen 3.7 Plus",
            8192,
            1_000_000,
            true,
            true,
            true,
            4,
            true,
            0.0008,
            0.002,
        ),
        caps(
            "qwen3.8-max",
            "Qwen 3.8 Max (百炼订阅)",
            65536,
            1_000_000,
            true,
            true,
            true,
            4,
            true,
            0.009,
            0.054,
        ),
        caps(
            "qwen3.8-flash",
            "Qwen 3.8 Flash（百炼）",
            131_072,
            1_000_000,
            true,
            true,
            true,
            20,
            true,
            0.0,
            0.0,
        ),
        caps(
            "deepseek-v4-pro",
            "DeepSeek V4 Pro",
            384_000,
            1_000_000,
            true,
            true,
            false,
            0,
            true,
            0.001,
            0.004,
        ),
        caps(
            "deepseek-v4-flash",
            "DeepSeek V4 Flash",
            384_000,
            1_000_000,
            true,
            true,
            false,
            0,
            true,
            0.0005,
            0.002,
        ),
        caps(
            "kimi-k3", "Kimi K3", 131_072, 1_000_000, true, true, true, 8, true, 0.002, 0.012,
        ),
        caps(
            "kimi-k2.7-code",
            "Kimi K2.7 Code",
            16384,
            256_000,
            true,
            true,
            true,
            8,
            true,
            0.002,
            0.012,
        ),
        caps(
            "moonshot-v1-128k",
            "Moonshot V1 128K",
            8192,
            128_000,
            true,
            false,
            false,
            0,
            true,
            0.001,
            0.002,
        ),
        caps(
            "glm-5.3", "GLM-5.3", 131_072, 1_048_576, true, true, false, 0, true, 0.001, 0.001,
        ),
        caps(
            "glm-5.3-flash",
            "GLM-5.3-Flash",
            131_072,
            1_048_576,
            true,
            true,
            true,
            50,
            true,
            0.00015,
            0.0005,
        ),
        caps(
            "MiniMax-M3",
            "MiniMax M3",
            16384,
            1_000_000,
            true,
            true,
            true,
            4,
            true,
            0.001,
            0.004,
        ),
        caps(
            "anthropic/claude-opus-4.8",
            "Claude Opus 4.8",
            64000,
            1_000_000,
            true,
            false,
            true,
            5,
            true,
            0.005,
            0.025,
        ),
        caps(
            "anthropic/claude-fable-5",
            "Claude Fable 5",
            64000,
            1_000_000,
            true,
            false,
            true,
            5,
            true,
            0.010,
            0.050,
        ),
        caps(
            "openai/gpt-5.6-sol",
            "OpenAI GPT-5.6 Sol",
            128_000,
            1_050_000,
            true,
            false,
            true,
            4,
            true,
            0.030,
            0.180,
        ),
        caps(
            "google/gemini-3.5-flash",
            "Google Gemini 3.5 Flash",
            65530,
            1_050_000,
            false,
            false,
            true,
            4,
            true,
            0.0015,
            0.009,
        ),
    ]
}

/// 未知模型的兜底能力（对齐旧 `ModelRegistry` 未注册模型的缺省 capabilities：
/// 8192 输出 / 200k 上下文 / 流式 + 图片 + 工具，无 thinking）——动态聚合遇到
/// 目录未收录的新模型时使用，`id` / `displayName` 取模型标识本身。
fn dynamic_info(id: &str) -> ModelInfo {
    caps(
        id, id, 8192, 200_000, true, false, true, 10, true, 0.003, 0.015,
    )
}

/// 单模型元数据：命中静态目录取其权威能力值，否则动态生成兜底元数据。
fn info_for(id: &str) -> ModelInfo {
    catalog()
        .into_iter()
        .find(|model| model.id == id)
        .unwrap_or_else(|| dynamic_info(id))
}

/// `GET /api/models` 的有效模型清单（2.7）：注册表非空则按其聚合模型序动态
/// 生成条目；注册表为空（Phase 1 单 provider 回退 / 未配任何 provider key）则
/// 退化为声明式静态目录（16 条基线），保住既有响应契约。
fn effective_models(state: &AppState) -> Vec<ModelInfo> {
    let registry = state.providers.load();
    let registry_models = registry.models();
    let mut models = if registry_models.is_empty() {
        catalog()
    } else {
        registry_models.iter().map(|id| info_for(id)).collect()
    };
    for model in &mut models {
        let capabilities = zk_llm::capabilities_for(&model.id);
        model.supports_images = capabilities.supports_images;
        model.max_images = if capabilities.supports_images {
            i64::from(capabilities.max_images)
        } else {
            zk_llm::resolve_vision_model(registry.as_ref(), &model.id).map_or(0, |routed| {
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
    let models = effective_models(&state);
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
        default_model: state.config.default_model.clone(),
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

    /// 目录 16 条、ID 唯一、序列化键形状（camelCase `costPer1kInput`）。
    #[test]
    fn catalog_size_and_wire_shape() {
        let models = catalog();
        assert_eq!(models.len(), 16);
        let ids: std::collections::HashSet<&str> = models.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids.len(), 16);
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
    fn effective_image_limits_match_the_configured_vision_route() {
        let mut providers = ProviderRegistry::new();
        providers.register(
            "dashscope",
            Arc::new(StubProvider),
            vec![
                "qwen3.7-max".into(),
                "qwen3.8-max".into(),
                "qwen3.8-flash".into(),
            ],
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

        // The non-vision model routes to the first configured vision model,
        // so the advertised input limit must match the engine's chosen target.
        assert_eq!(max_images("qwen3.7-max"), 4);
        assert_eq!(max_images("qwen3.8-max"), 4);
        assert_eq!(max_images("qwen3.8-flash"), 20);
    }
}
