//! Provider 配置与声明式目录——配置驱动的 endpoint / apiKey / model 三元组（D2），
//! 2.7 起扩展为 8+1 家声明目录 + 环境变量扫描装配（D-P2-3）。
//!
//! # 声明目录（对照方案 §13-3 与旧 application.yml `llm.providers`）
//!
//! [`PROVIDER_CATALOG`] 逐条对齐旧 `LlmProvidersProperties` 的 per-provider
//! 段：`base-url` / `default-model` / `models`，外加两个新增维度——
//! [`CatalogEntry::protocol`]（`OpenAI` 兼容 vs `Anthropic` 原生，D-P2-3）与
//! [`CatalogEntry::in_default_catalog`]（是否进入 `GET /api/models` 的声明式
//! 聚合基线；`openai` 回退通道与 `anthropic` 原生通道均不进——两者的模型面
//! 由环境变量给出，进目录会破坏 19 条基线契约）。
//!
//! # 环境变量约定（2.7 冻结）
//!
//! | 变量 | 语义 |
//! |---|---|
//! | `LLM_PROVIDER_<NAME>_API_KEY` | 密钥；**存在且非空**才注册该 provider（可逗号分隔多把 → 轮换环） |
//! | `LLM_PROVIDER_<NAME>_MODELS` | 模型清单（逗号分隔）；覆盖目录声明 |
//! | `LLM_PROVIDER_<NAME>_BASE_URL` | 覆盖目录 `base_url`（联调 / 私有网关） |
//! | `LLM_PROVIDER_<NAME>_DEFAULT_MODEL` | 覆盖目录默认模型 |
//! | `ZK_MODEL_FALLBACK_CHAIN` | 模型降级链（冒号分隔，如 `kimi-k3:qwen3.7-max`） |
//!
//! `<NAME>` = provider 名大写、`-` 换 `_`（`dashscope-token-plan` →
//! `DASHSCOPE_TOKEN_PLAN`，与旧 `.env` 完全一致）。
//!
//! Phase 1 回退：不存在任何 `LLM_PROVIDER_*_API_KEY` 时
//! [`provider_configs_from_env`] 返回空列表，调用方（zk-server main）改以
//! `ZK_LLM_BASE_URL` + `ZK_LLM_API_KEY` 构建单 provider——行为与 S9 一致。

use crate::secret::{ApiKey, ApiKeyRing};

/// `DashScope` `OpenAI` 兼容端点（application.yml `llm.providers.dashscope`）。
pub const DASHSCOPE_BASE_URL: &str = "https://dashscope.aliyuncs.com/compatible-mode/v1";
/// `DashScope` Token 套餐端点（`llm.providers.dashscope-token-plan`）。
pub const DASHSCOPE_TOKEN_PLAN_BASE_URL: &str =
    "https://token-plan.cn-beijing.maas.aliyuncs.com/compatible-mode/v1";
/// `DeepSeek` 官方端点。
pub const DEEPSEEK_BASE_URL: &str = "https://api.deepseek.com/v1";
/// `Moonshot`（Kimi）官方端点。
pub const MOONSHOT_BASE_URL: &str = "https://api.moonshot.cn/v1";
/// 智谱（`GLM`）开放平台端点。
pub const ZHIPU_BASE_URL: &str = "https://open.bigmodel.cn/api/paas/v4";
/// `MiniMax` 官方端点。
pub const MINIMAX_BASE_URL: &str = "https://api.minimax.chat/v1";
/// `ZenMux` 聚合网关端点（`Anthropic` / `OpenAI` / `Gemini` 模型的 `OpenAI` 兼容出口）。
pub const ZENMUX_BASE_URL: &str = "https://zenmux.ai/api/v1";
/// `Anthropic` 原生 Messages API 端点（路径由 provider 自行补 `/v1/messages`）。
pub const ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com";
/// `OpenAI` 官方 API 端点（`openai` 直连 `provider`）。
pub const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";

/// provider 环境变量前缀（`LLM_PROVIDER_<NAME>_*`）。
pub const PROVIDER_ENV_PREFIX: &str = "LLM_PROVIDER_";
/// 模型降级链环境变量（冒号分隔，对照旧 `ModelDegradationChain` 的配置面）。
pub const FALLBACK_CHAIN_ENV: &str = "ZK_MODEL_FALLBACK_CHAIN";

/// provider 线协议（D-P2-3：`Anthropic` 原生与 `OpenAI` 兼容同期交付）。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProviderProtocol {
    /// `OpenAI` Chat Completions 兼容（`/chat/completions` + `Bearer` 认证）。
    #[default]
    OpenAiCompat,
    /// `Anthropic` 原生 Messages API（`/v1/messages` + `x-api-key` 认证）。
    AnthropicNative,
}

/// 单提供商配置（provider name → `base_url` / 密钥环 / model 三元组 + 协议）。
///
/// 形状对齐旧 `LlmProvidersProperties` 的 per-provider 段与 §13-3 配置矩阵；
/// 密钥以 [`ApiKeyRing`]（内含脱敏 [`ApiKey`]）持有，支持同 provider 多把
/// 密钥 round-robin（旧 `ApiKeyRotationManager` 语义）。
#[derive(Clone, Debug)]
pub struct ProviderConfig {
    /// 供应商标识（注册表键：`dashscope` / `openai` / `deepseek` 等）。
    pub name: String,
    /// base URL（尾斜杠 tolerated，请求时统一去除）。
    pub base_url: String,
    /// 脱敏密钥轮换环（环境变量注入，不落盘）。
    pub api_keys: ApiKeyRing,
    /// 默认模型。
    pub default_model: String,
    /// 可用模型列表（展示与路由索引用；请求侧以 `ChatRequest.model` 为准）。
    pub models: Vec<String>,
    /// 线协议（决定实例化哪种 Provider 实现）。
    pub protocol: ProviderProtocol,
}

impl ProviderConfig {
    /// 任意 `OpenAI` 兼容提供商配置（单密钥；签名与 S9 保持不变）。
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        base_url: impl Into<String>,
        api_key: ApiKey,
        default_model: impl Into<String>,
        models: Vec<String>,
    ) -> Self {
        Self::with_keys(
            name,
            base_url,
            ApiKeyRing::single(api_key),
            default_model,
            models,
        )
    }

    /// 以密钥环构造（多密钥轮换形态）。
    #[must_use]
    pub fn with_keys(
        name: impl Into<String>,
        base_url: impl Into<String>,
        api_keys: ApiKeyRing,
        default_model: impl Into<String>,
        models: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            base_url: base_url.into(),
            api_keys,
            default_model: default_model.into(),
            models,
            protocol: ProviderProtocol::OpenAiCompat,
        }
    }

    /// 设置线协议（链式）。
    #[must_use]
    pub fn with_protocol(mut self, protocol: ProviderProtocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// `DashScope` 主通道（对齐 application.yml：qwen3.7-max 默认 +
    /// qwen3.7-max/qwen3.7-plus 模型注册）。
    #[must_use]
    pub fn dashscope(api_key: ApiKey) -> Self {
        Self::new(
            "dashscope",
            DASHSCOPE_BASE_URL,
            api_key,
            "qwen3.7-max",
            vec!["qwen3.7-max".into(), "qwen3.7-plus".into()],
        )
    }

    /// `OpenAI` 兼容回退通道（env `LLM_BASE_URL` / `LLM_API_KEY` /
    /// `LLM_DEFAULT_MODEL` / `LLM_MODELS` 的承接形态，§13-3 末行）。
    #[must_use]
    pub fn openai_fallback(
        base_url: impl Into<String>,
        api_key: ApiKey,
        default_model: impl Into<String>,
        models: Vec<String>,
    ) -> Self {
        Self::new("openai", base_url, api_key, default_model, models)
    }

    /// 取下一把可用密钥（round-robin + 跳过限流冷却）；未配置时 `None`。
    #[must_use]
    pub fn next_api_key(&self) -> Option<ApiKey> {
        self.api_keys.next_key()
    }

    /// 是否已配置至少一把密钥。
    #[must_use]
    pub fn has_api_key(&self) -> bool {
        !self.api_keys.is_empty()
    }
}

/// 声明目录条目（`'static` 常量表——不读环境、可在单测中逐条自证）。
#[derive(Clone, Copy, Debug)]
pub struct CatalogEntry {
    /// provider 名（注册表键 / 环境变量名词根）。
    pub name: &'static str,
    /// 目录声明的 base URL（可被 `LLM_PROVIDER_<NAME>_BASE_URL` 覆盖）。
    pub base_url: &'static str,
    /// 目录声明的默认模型。
    pub default_model: &'static str,
    /// 目录声明的模型清单（可被 `LLM_PROVIDER_<NAME>_MODELS` 覆盖）。
    pub models: &'static [&'static str],
    /// 线协议。
    pub protocol: ProviderProtocol,
    /// 是否进入声明式模型聚合基线（`GET /api/models` 的 19 条契约来源）。
    pub in_default_catalog: bool,
}

/// 8+1 家 provider 声明目录（顺序 = 方案 §13-3 表格顺序 = 模型聚合顺序）。
///
/// 前 7 家 `in_default_catalog = true`，其 `models` 并集恰为
/// `GET /api/models` 的 19 条基线（顺序一致）；`openai`（Phase 1 回退通道）
/// 与 `anthropic`（原生协议通道）不进基线目录。
pub const PROVIDER_CATALOG: &[CatalogEntry] = &[
    CatalogEntry {
        name: "dashscope",
        base_url: DASHSCOPE_BASE_URL,
        default_model: "qwen3.7-max",
        models: &["qwen3.7-max", "qwen3.7-plus"],
        protocol: ProviderProtocol::OpenAiCompat,
        in_default_catalog: true,
    },
    CatalogEntry {
        name: "dashscope-token-plan",
        base_url: DASHSCOPE_TOKEN_PLAN_BASE_URL,
        default_model: "qwen3.8-max",
        models: &[
            "qwen3.8-max",
            "qwen3.8-flash",
            "deepseek-v4-pro-0813",
            "deepseek-v4-flash-0731",
        ],
        protocol: ProviderProtocol::OpenAiCompat,
        in_default_catalog: true,
    },
    CatalogEntry {
        name: "deepseek",
        base_url: DEEPSEEK_BASE_URL,
        default_model: "deepseek-v4-pro",
        models: &[
            "deepseek-v4-pro",
            "deepseek-v4-flash",
            "deepseek-v4-flash-vision-exp",
        ],
        protocol: ProviderProtocol::OpenAiCompat,
        in_default_catalog: true,
    },
    CatalogEntry {
        name: "moonshot",
        base_url: MOONSHOT_BASE_URL,
        default_model: "kimi-k3",
        models: &["kimi-k3", "kimi-k2.7-code", "moonshot-v1-128k"],
        protocol: ProviderProtocol::OpenAiCompat,
        in_default_catalog: true,
    },
    CatalogEntry {
        name: "zhipu",
        base_url: ZHIPU_BASE_URL,
        default_model: "glm-5.3",
        models: &["glm-5.3", "glm-5.3-flash"],
        protocol: ProviderProtocol::OpenAiCompat,
        in_default_catalog: true,
    },
    CatalogEntry {
        name: "minimax",
        base_url: MINIMAX_BASE_URL,
        default_model: "MiniMax-M3",
        models: &["MiniMax-M3"],
        protocol: ProviderProtocol::OpenAiCompat,
        in_default_catalog: true,
    },
    CatalogEntry {
        name: "zenmux",
        base_url: ZENMUX_BASE_URL,
        default_model: "anthropic/claude-opus-4.8",
        models: &[
            "anthropic/claude-opus-4.8",
            "anthropic/claude-fable-5",
            "openai/gpt-5.6-sol",
            "google/gemini-3.5-flash",
        ],
        protocol: ProviderProtocol::OpenAiCompat,
        in_default_catalog: true,
    },
    CatalogEntry {
        name: "anthropic",
        base_url: ANTHROPIC_BASE_URL,
        default_model: "claude-sonnet-4-6",
        models: &[
            "claude-sonnet-4-6",
            "claude-3-7-sonnet-20250219",
            "claude-3-5-sonnet-20241022",
        ],
        protocol: ProviderProtocol::AnthropicNative,
        in_default_catalog: false,
    },
    CatalogEntry {
        name: "openai",
        base_url: OPENAI_BASE_URL,
        default_model: "gpt-4o",
        models: &["gpt-4o", "gpt-4o-mini"],
        protocol: ProviderProtocol::OpenAiCompat,
        in_default_catalog: false,
    },
];

/// 声明目录默认模型（旧 `LlmProviderRegistry.getDefaultModel` 兜底值）。
pub const DEFAULT_MODEL: &str = "qwen3.8-max";

/// 按名查目录条目。
#[must_use]
pub fn catalog_entry(name: &str) -> Option<&'static CatalogEntry> {
    PROVIDER_CATALOG.iter().find(|entry| entry.name == name)
}

/// provider 名 → 环境变量名词根（大写、`-` 换 `_`）。
#[must_use]
pub fn env_name_root(provider: &str) -> String {
    provider.to_ascii_uppercase().replace(['-', '.'], "_")
}

/// 声明式模型基线（`in_default_catalog` 条目的 `models` 并集，保序去重）。
#[must_use]
pub fn declared_models() -> Vec<String> {
    let mut models: Vec<String> = Vec::new();
    for entry in PROVIDER_CATALOG.iter().filter(|e| e.in_default_catalog) {
        for model in entry.models {
            let model = (*model).to_owned();
            if !models.contains(&model) {
                models.push(model);
            }
        }
    }
    models
}

/// 是否存在任一 `LLM_PROVIDER_*_API_KEY`（决定走多 provider 装配还是
/// Phase 1 单 provider 回退）。
#[must_use]
pub fn has_provider_env() -> bool {
    PROVIDER_CATALOG
        .iter()
        .any(|entry| read_env(&api_key_env(entry.name)).is_some())
}

/// 扫描环境变量装配 provider 配置（顺序 = [`PROVIDER_CATALOG`] 顺序）。
///
/// 仅注册 `LLM_PROVIDER_<NAME>_API_KEY` 存在且非空的条目——未配密钥的
/// provider 既不实例化也不出现在模型清单里。
#[must_use]
pub fn provider_configs_from_env() -> Vec<ProviderConfig> {
    PROVIDER_CATALOG
        .iter()
        .filter_map(|entry| config_from_env_with(entry, read_env))
        .collect()
}

/// 单条目的环境装配（`lookup` 注入以便单测不污染进程环境）。
///
/// 返回 `None` 表示未配密钥（或密钥全为空白）→ 不注册。
#[must_use]
pub fn config_from_env_with(
    entry: &CatalogEntry,
    lookup: impl Fn(&str) -> Option<String>,
) -> Option<ProviderConfig> {
    let raw_keys = lookup(&api_key_env(entry.name))?;
    let api_keys = ApiKeyRing::from_csv(&raw_keys);
    if api_keys.is_empty() {
        return None;
    }
    let root = env_name_root(entry.name);
    let base_url = lookup(&format!("{PROVIDER_ENV_PREFIX}{root}_BASE_URL"))
        .unwrap_or_else(|| entry.base_url.to_owned());
    let models = lookup(&format!("{PROVIDER_ENV_PREFIX}{root}_MODELS"))
        .map(|raw| split_csv(&raw))
        .filter(|models: &Vec<String>| !models.is_empty())
        .unwrap_or_else(|| entry.models.iter().map(|m| (*m).to_owned()).collect());
    let default_model = lookup(&format!("{PROVIDER_ENV_PREFIX}{root}_DEFAULT_MODEL"))
        .or_else(|| models.first().cloned())
        .unwrap_or_else(|| entry.default_model.to_owned());
    Some(
        ProviderConfig::with_keys(entry.name, base_url, api_keys, default_model, models)
            .with_protocol(entry.protocol),
    )
}

/// 模型降级链（`ZK_MODEL_FALLBACK_CHAIN`，冒号分隔；未配置为空）。
#[must_use]
pub fn fallback_chain_from_env() -> Vec<String> {
    read_env(FALLBACK_CHAIN_ENV).map_or_else(Vec::new, |raw| {
        raw.split(':')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(str::to_owned)
            .collect()
    })
}

/// 某 provider 的密钥环境变量名。
#[must_use]
pub fn api_key_env(provider: &str) -> String {
    format!("{PROVIDER_ENV_PREFIX}{}_API_KEY", env_name_root(provider))
}

/// 读环境变量，空白视为未配置。
fn read_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// 逗号分隔切分（去空白项）。
fn split_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dashscope_preset_matches_application_yml() {
        let cfg = ProviderConfig::dashscope(ApiKey::new("sk-x"));
        assert_eq!(cfg.name, "dashscope");
        assert_eq!(cfg.base_url, DASHSCOPE_BASE_URL);
        assert_eq!(cfg.default_model, "qwen3.7-max");
        assert_eq!(cfg.models, vec!["qwen3.7-max", "qwen3.7-plus"]);
        assert_eq!(cfg.protocol, ProviderProtocol::OpenAiCompat);
        assert_eq!(cfg.api_keys.len(), 1);
    }

    #[test]
    fn config_debug_never_leaks_api_key() {
        let cfg = ProviderConfig::dashscope(ApiKey::new("sk-plaintext"));
        assert!(!format!("{cfg:?}").contains("sk-plaintext"));
        let multi = ProviderConfig::with_keys(
            "x",
            "https://e",
            ApiKeyRing::from_csv("sk-one,sk-two"),
            "m",
            vec![],
        );
        let rendered = format!("{multi:?}");
        assert!(!rendered.contains("sk-one") && !rendered.contains("sk-two"));
    }

    /// 目录逐条对照方案 §13-3 / 旧 application.yml（base-url + default-model）。
    #[test]
    fn catalog_matches_plan_matrix() {
        let expected: [(&str, &str, &str, usize); 9] = [
            ("dashscope", DASHSCOPE_BASE_URL, "qwen3.7-max", 2),
            (
                "dashscope-token-plan",
                DASHSCOPE_TOKEN_PLAN_BASE_URL,
                "qwen3.8-max",
                4,
            ),
            ("deepseek", DEEPSEEK_BASE_URL, "deepseek-v4-pro", 3),
            ("moonshot", MOONSHOT_BASE_URL, "kimi-k3", 3),
            ("zhipu", ZHIPU_BASE_URL, "glm-5.3", 2),
            ("minimax", MINIMAX_BASE_URL, "MiniMax-M3", 1),
            ("zenmux", ZENMUX_BASE_URL, "anthropic/claude-opus-4.8", 4),
            ("anthropic", ANTHROPIC_BASE_URL, "claude-sonnet-4-6", 3),
            ("openai", OPENAI_BASE_URL, "gpt-4o", 2),
        ];
        assert_eq!(PROVIDER_CATALOG.len(), expected.len());
        for (entry, (name, base_url, default_model, model_count)) in
            PROVIDER_CATALOG.iter().zip(expected)
        {
            assert_eq!(entry.name, name);
            assert_eq!(entry.base_url, base_url, "{name} base_url");
            assert_eq!(entry.default_model, default_model, "{name} default_model");
            assert_eq!(entry.models.len(), model_count, "{name} models");
        }
        // 协议归属：仅 anthropic 走原生协议。
        for entry in PROVIDER_CATALOG {
            let expected = if entry.name == "anthropic" {
                ProviderProtocol::AnthropicNative
            } else {
                ProviderProtocol::OpenAiCompat
            };
            assert_eq!(entry.protocol, expected, "{} protocol", entry.name);
        }
    }

    /// 声明式基线 = 前 7 家并集 = `GET /api/models` 的 19 条（顺序一致）。
    #[test]
    fn declared_models_match_nineteen_model_baseline() {
        assert_eq!(
            declared_models(),
            vec![
                "qwen3.7-max",
                "qwen3.7-plus",
                "qwen3.8-max",
                "qwen3.8-flash",
                "deepseek-v4-pro-0813",
                "deepseek-v4-flash-0731",
                "deepseek-v4-pro",
                "deepseek-v4-flash",
                "deepseek-v4-flash-vision-exp",
                "kimi-k3",
                "kimi-k2.7-code",
                "moonshot-v1-128k",
                "glm-5.3",
                "glm-5.3-flash",
                "MiniMax-M3",
                "anthropic/claude-opus-4.8",
                "anthropic/claude-fable-5",
                "openai/gpt-5.6-sol",
                "google/gemini-3.5-flash",
            ]
        );
    }

    /// 环境变量名词根对齐旧 `.env`（`dashscope-token-plan` → `DASHSCOPE_TOKEN_PLAN`）。
    #[test]
    fn env_names_match_legacy_dotenv() {
        assert_eq!(
            env_name_root("dashscope-token-plan"),
            "DASHSCOPE_TOKEN_PLAN"
        );
        assert_eq!(
            api_key_env("dashscope-token-plan"),
            "LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY"
        );
        assert_eq!(api_key_env("moonshot"), "LLM_PROVIDER_MOONSHOT_API_KEY");
    }

    #[test]
    fn env_scan_registers_only_keyed_providers() {
        let env = |key: &str| match key {
            "LLM_PROVIDER_MOONSHOT_API_KEY" => Some("sk-a,sk-b".to_owned()),
            "LLM_PROVIDER_ZHIPU_API_KEY" => Some("   ".to_owned()),
            _ => None,
        };
        let moonshot = config_from_env_with(catalog_entry("moonshot").unwrap(), env)
            .expect("keyed provider registers");
        assert_eq!(moonshot.name, "moonshot");
        assert_eq!(moonshot.base_url, MOONSHOT_BASE_URL);
        assert_eq!(moonshot.api_keys.len(), 2, "csv keys build a rotation ring");
        assert_eq!(moonshot.models.len(), 3);
        assert_eq!(moonshot.default_model, "kimi-k3");
        // 空白密钥 = 未配置；无密钥变量 = 未配置。
        assert!(config_from_env_with(catalog_entry("zhipu").unwrap(), env).is_none());
        assert!(config_from_env_with(catalog_entry("deepseek").unwrap(), env).is_none());
    }

    #[test]
    fn env_overrides_base_url_models_and_default_model() {
        let env = |key: &str| match key {
            "LLM_PROVIDER_DEEPSEEK_API_KEY" => Some("sk-x".to_owned()),
            "LLM_PROVIDER_DEEPSEEK_BASE_URL" => Some("https://gw.internal/v1".to_owned()),
            "LLM_PROVIDER_DEEPSEEK_MODELS" => Some("deepseek-chat, deepseek-r2 ,".to_owned()),
            _ => None,
        };
        let cfg = config_from_env_with(catalog_entry("deepseek").unwrap(), env).expect("registers");
        assert_eq!(cfg.base_url, "https://gw.internal/v1");
        assert_eq!(cfg.models, vec!["deepseek-chat", "deepseek-r2"]);
        // 未显式给默认模型 → 取清单首项（对齐旧 provider 首模型语义）。
        assert_eq!(cfg.default_model, "deepseek-chat");
    }
}
