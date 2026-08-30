//! 模型能力表——内置模型映射表 + 精确/前缀查表 + 保守默认值。
//!
//! 建模来源（旧仓库只读权威规格，`main@581d407b`）：
//! - `llm/ModelCapabilities.java`：能力记录字段序与保守默认值
//!   （`DEFAULT` = unknown / 4096 输出 / 8192 窗口 / `tokenCharRatio` 3.5）；
//! - `llm/ModelRegistry.java`：`BUILTIN_MODELS` 内置表（26 条，逐条数值对照）
//!   与 `getCapabilities` 的查表顺序。
//!
//! - `llm/ModelCapabilitiesProperties.java`：`model.capabilities.<id>.*` 配置绑定
//!   （7 个可覆盖维度，`tokenCharRatio` 必填）与 `ModelRegistry.applyConfiguredOverrides`
//!   的合并规则（Batch 0 P0-21 补齐）。
//!
//! # 与旧 `getCapabilities` 四级优先级的对应
//!
//! 旧实现为 4 级：①用户自定义覆盖（`ModelCapabilitiesProperties` YAML）→
//! ②Provider 动态查询（`LlmProvider.getModelCapabilities`）→ ③内置表
//! （精确匹配 + `ollama/*` 前缀通配）→ ④保守默认值。本实现还原 ① + ③ + ④：
//! ① 见 [`ModelRegistry`]（配置文件形态见下节），② [`ChatProvider`](crate::ChatProvider)
//! trait 不暴露能力查询，仍为留白项——非行为偏离（旧 ② 在 Provider 未实现能力
//! 查询时同样落到 ③ / ④）。
//!
//! # `model.capabilities` 配置面（Batch 0 P0-21）
//!
//! 旧仓库的能力覆盖写在 `backend/src/main/resources/application.yml` 的
//! `model.capabilities` 节（L295+，8 个模型），由 Spring `@ConfigurationProperties`
//! 绑定。zkcode 侧无 YAML 装配层，故取**同名同义的 JSON 配置文件**——键名逐字
//! 沿用旧 YAML 的 camelCase（`contextWindow` / `tokenCharRatio` /
//! `outputMaxTokens` / `supportsCache` / `supportsToolUse` / `supportsVision` /
//! `supportsStreaming`），可从 `application.yml` 该节一对一转写：
//!
//! ```json
//! { "capabilities": { "qwen3.7-max": { "contextWindow": 1000000,
//!   "tokenCharRatio": 2.5, "outputMaxTokens": 65536, "supportsToolUse": true } } }
//! ```
//!
//! 文件路径由 [`MODEL_CAPABILITIES_PATH_ENV`] 给出（zk-server 启动期亦可经
//! [`install_registry`] 显式装配，路径由其经 `zk_core::paths` 解析——本 crate
//! 不持有目录名字面量）。**比率与窗口不再是代码内常量**：内置表只是
//! 「配置缺省」，配置一经声明即以配置为准。
//!
//! # 消费方
//!
//! - zk-engine 的 `recommended_max_tokens`（旧 `QueryConfig.getRecommendedMaxTokens`
//!   = `min(maxOutputTokens, 65536)`）；
//! - zk-engine 的会话成本累计（旧 `CostTrackerService.recordUsage` 的
//!   `costPer1kInput` / `costPer1kOutput` 费率）。

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::Deserialize;

/// `model.capabilities` 配置文件路径的环境变量。
///
/// 旧实现该配置随 `application.yml` 装配，无独立路径；zkcode 侧配置文件位于
/// 用户/项目 `.zk/` 目录下，而目录名字面量的唯一事实源在 `zk_core::paths`
/// （本 crate 不依赖 zk-core，故不在此重复该字面量）——因此路径由外部给出：
/// zk-server 启动期经 [`install_registry`] 显式装配（失败即拒启，对齐旧
/// `applyConfiguredOverrides` 的 fail-fast），未装配时本 crate 退化为读该环境
/// 变量的惰性装配（见 [`registry`]）。
pub const MODEL_CAPABILITIES_PATH_ENV: &str = "ZK_MODEL_CAPABILITIES_PATH";

/// `model.capabilities` 配置文件的约定文件名（供 zk-server 拼 `.zk/` 路径）。
pub const MODEL_CAPABILITIES_FILE_NAME: &str = "model-capabilities.json";

/// 单模型能力（字段序逐项对照旧 `ModelCapabilities` record）。
///
/// 不变量（旧 record 紧凑构造器校验，本实现由 `builtin_table_holds_legacy_invariants`
/// 单测守卫）：`0 < max_output_tokens < context_window`，
/// `0.5 < token_char_ratio <= 16.0`。
/// 两个字符串字段为 [`Cow`]：内置表项恒为 `Borrowed`（const 可构造、零分配），
/// 配置来源的覆盖项为 `Owned`——由此让「静态表项」与「配置项」共用同一记录
/// 类型，无需把配置字符串 leak 成 `&'static`（热重载重复装配即内存泄漏）。
#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "字段集逐项对照旧 ModelCapabilities record，不重构为状态机/枚举"
)]
pub struct ModelCapabilities {
    /// 模型 ID（表键；`ollama/*` 为前缀通配键）。
    pub model_id: Cow<'static, str>,
    /// 展示名。
    pub display_name: Cow<'static, str>,
    /// 最大输出 token。
    pub max_output_tokens: u32,
    /// 上下文窗口。
    pub context_window: u32,
    /// 是否支持流式。
    pub supports_streaming: bool,
    /// 是否支持思考模式。
    pub supports_thinking: bool,
    /// 是否支持图片输入。
    pub supports_images: bool,
    /// 单请求图片数量上限。
    pub max_images: u32,
    /// 是否支持工具调用。
    pub supports_tool_use: bool,
    /// 每 1k 输入 token 成本（USD）。
    pub cost_per_1k_input: f64,
    /// 每 1k 输出 token 成本（USD）。
    pub cost_per_1k_output: f64,
    /// token/字符比（旧 `caps()` 助手恒 3.5）。
    pub token_char_ratio: f64,
    /// 是否支持提示缓存（旧 `caps()` 助手恒 false）。
    pub supports_cache: bool,
}

impl ModelCapabilities {
    /// 内置表构造助手（逐字对照旧 `ModelRegistry.caps(...)`：
    /// `tokenCharRatio` = 3.5、`supportsCache` = false）。
    #[expect(
        clippy::too_many_arguments,
        clippy::fn_params_excessive_bools,
        reason = "参数序逐项对照旧 caps(...) 助手，不重排不聚合"
    )]
    const fn caps(
        model_id: &'static str,
        display_name: &'static str,
        max_output_tokens: u32,
        context_window: u32,
        supports_streaming: bool,
        supports_thinking: bool,
        supports_images: bool,
        max_images: u32,
        supports_tool_use: bool,
        cost_per_1k_input: f64,
        cost_per_1k_output: f64,
    ) -> Self {
        Self {
            model_id: Cow::Borrowed(model_id),
            display_name: Cow::Borrowed(display_name),
            max_output_tokens,
            context_window,
            supports_streaming,
            supports_thinking,
            supports_images,
            max_images,
            supports_tool_use,
            cost_per_1k_input,
            cost_per_1k_output,
            token_char_ratio: 3.5,
            supports_cache: false,
        }
    }
}

/// 未知模型的保守默认能力（逐字对照旧 `ModelCapabilities.DEFAULT`）。
pub const DEFAULT_CAPABILITIES: ModelCapabilities = ModelCapabilities {
    model_id: Cow::Borrowed("unknown"),
    display_name: Cow::Borrowed("Unknown Model"),
    max_output_tokens: 4096,
    context_window: 8192,
    supports_streaming: true,
    supports_thinking: false,
    supports_images: false,
    max_images: 0,
    supports_tool_use: false,
    cost_per_1k_input: 0.0,
    cost_per_1k_output: 0.0,
    token_char_ratio: 3.5,
    supports_cache: false,
};

/// 内置模型表（条目顺序与数值逐条对照旧 `ModelRegistry.BUILTIN_MODELS`）。
pub static BUILTIN_MODELS: &[ModelCapabilities] = &[
    // OpenAI
    ModelCapabilities::caps(
        "gpt-5.6-sol",
        "GPT-5.6-Sol",
        128_000,
        1_050_000,
        true,
        true,
        true,
        10,
        true,
        0.005,
        0.03,
    ),
    ModelCapabilities::caps(
        "gpt-5.4-mini",
        "GPT-5.4 Mini",
        128_000,
        400_000,
        true,
        true,
        true,
        10,
        true,
        0.00075,
        0.0045,
    ),
    // Anthropic
    ModelCapabilities::caps(
        "claude-sonnet-4-6",
        "Claude Sonnet 4.6",
        16384,
        200_000,
        true,
        true,
        true,
        10,
        true,
        0.003,
        0.015,
    ),
    ModelCapabilities::caps(
        "claude-opus-4-8",
        "Claude Opus 4.8",
        16384,
        200_000,
        true,
        true,
        true,
        10,
        true,
        0.015,
        0.075,
    ),
    ModelCapabilities::caps(
        "claude-haiku-4-5",
        "Claude Haiku 4.5",
        8192,
        200_000,
        true,
        false,
        true,
        10,
        true,
        0.0008,
        0.004,
    ),
    // Anthropic via ZenMux（anthropic/ 前缀 = zenmux 中转，保守设 64K 输出）
    ModelCapabilities::caps(
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
    ModelCapabilities::caps(
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
    // OpenAI via ZenMux（openai/ 前缀 = zenmux 中转）
    ModelCapabilities::caps(
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
    // Google via ZenMux（google/ 前缀 = zenmux 中转）
    ModelCapabilities::caps(
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
    // 国产大模型
    ModelCapabilities::caps(
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
    ModelCapabilities::caps(
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
    ModelCapabilities::caps(
        "deepseek-v4-flash-vision-exp",
        "DeepSeek V4 Flash Vision Exp",
        384_000,
        1_000_000,
        true,
        true,
        true,
        5,
        true,
        0.0005,
        0.002,
    ),
    ModelCapabilities::caps(
        "deepseek-v4-pro-0813",
        "DeepSeek V4 Pro 0813（百炼）",
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
    ModelCapabilities::caps(
        "deepseek-v4-flash-0731",
        "DeepSeek V4 Flash 0731（百炼）",
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
    // Moonshot
    ModelCapabilities::caps(
        "kimi-k3", "Kimi K3", 131_072, 1_000_000, true, true, true, 8, true, 0.002, 0.012,
    ),
    ModelCapabilities::caps(
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
    ModelCapabilities::caps(
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
    ModelCapabilities::caps(
        "qwen-turbo",
        "Qwen Turbo",
        8192,
        1_000_000,
        true,
        false,
        false,
        0,
        true,
        0.0003,
        0.0006,
    ),
    ModelCapabilities::caps(
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
    ModelCapabilities::caps(
        "qwen3.8-max",
        "Qwen 3.8 Max（百炼）",
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
    ModelCapabilities::caps(
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
    ModelCapabilities::caps(
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
    ModelCapabilities::caps(
        "glm-5.3", "GLM-5.3", 131_072, 1_048_576, true, true, false, 0, true, 0.001, 0.001,
    ),
    ModelCapabilities::caps(
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
    // MiniMax
    ModelCapabilities::caps(
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
    // Ollama 本地（`/*` 前缀通配键）
    ModelCapabilities::caps(
        "ollama/*",
        "Ollama Local",
        4096,
        8192,
        true,
        false,
        false,
        0,
        false,
        0.0,
        0.0,
    ),
];

/// 单模型的配置覆盖项（字段序逐项对照旧 `ModelCapabilitiesProperties.Override`）。
///
/// 七个字段一律为 `Option`：旧实现以 `null` 表「未配置 → 取 base 值」
/// （`ModelRegistry.value(configured, fallback)`），故此处不可用 serde 缺省值
/// 代偿。键名沿用旧 `application.yml` 的 `model.capabilities.<id>.*` camelCase
/// 拼写，可从该节一对一转写。
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CapabilityOverride {
    /// 上下文窗口覆盖（缺省取 base）。
    pub context_window: Option<u32>,
    /// token/字符比覆盖（旧实现**必填**，缺失即装配失败）。
    pub token_char_ratio: Option<f64>,
    /// 最大输出 token 覆盖（缺省取 base）。
    pub output_max_tokens: Option<u32>,
    /// 提示缓存支持覆盖（缺省取 base）。
    pub supports_cache: Option<bool>,
    /// 工具调用支持覆盖（缺省取 base）。
    pub supports_tool_use: Option<bool>,
    /// 图片输入支持覆盖（旧键名 `supportsVision`，落到 `supports_images`）。
    pub supports_vision: Option<bool>,
    /// 流式支持覆盖（缺省取 base）。
    pub supports_streaming: Option<bool>,
}

/// `model.capabilities` 配置文件（对照旧 `@ConfigurationProperties(prefix = "model")`）。
///
/// 旧实现的 `capabilities` 缺省为空 map（`setCapabilities(null)` 亦归一为空），
/// 故此处 `#[serde(default)]`：`{}` 与缺字段等价，均表「无覆盖」。
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapabilitiesConfig {
    /// `<模型 ID> → 覆盖项`（旧 `Map<String, Override> capabilities`）。
    #[serde(default)]
    pub capabilities: BTreeMap<String, CapabilityOverride>,
}

impl ModelCapabilitiesConfig {
    /// 解析配置 JSON 文本。
    ///
    /// # Errors
    ///
    /// 文本非合法 JSON 或字段类型不匹配时返回 [`CapabilitiesConfigError::Parse`]。
    pub fn from_json_str(json: &str) -> Result<Self, CapabilitiesConfigError> {
        serde_json::from_str(json).map_err(CapabilitiesConfigError::Parse)
    }

    /// 读取配置文件；文件不存在返回 `Ok(None)`（对照旧 `application.yml` 无
    /// `model.capabilities` 节即空 map，不视为错误）。
    ///
    /// # Errors
    ///
    /// 文件存在但读取失败返回 [`CapabilitiesConfigError::Read`]；内容非法返回
    /// [`CapabilitiesConfigError::Parse`]。
    pub fn load(path: &Path) -> Result<Option<Self>, CapabilitiesConfigError> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::from_json_str(&text).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(CapabilitiesConfigError::Read {
                path: path.to_path_buf(),
                source: error,
            }),
        }
    }
}

/// `model.capabilities` 装配失败。
///
/// 后三个变体的文案逐字对照旧实现抛出的异常消息：
/// `applyConfiguredOverrides` 的 `IllegalStateException`，以及
/// `ModelCapabilities` 紧凑构造器的两条 `IllegalArgumentException`。
#[derive(Debug, thiserror::Error)]
pub enum CapabilitiesConfigError {
    /// 配置文件存在但读取失败。
    #[error("failed to read model capabilities config {path}: {source}")]
    Read {
        /// 配置文件路径。
        path: PathBuf,
        /// 底层 IO 错误。
        source: std::io::Error,
    },
    /// 配置内容非合法 JSON（旧实现对应 Spring 绑定失败）。
    #[error("failed to parse model capabilities config: {0}")]
    Parse(#[source] serde_json::Error),
    /// `tokenCharRatio` 缺失（旧 `IllegalStateException`）。
    #[error("model.capabilities.{model_id}.tokenCharRatio is required")]
    MissingTokenCharRatio {
        /// 模型 ID。
        model_id: String,
    },
    /// 合并后 `0 < maxOutputTokens < contextWindow` 不成立。
    #[error("Invalid model token limits for {model_id}")]
    InvalidTokenLimits {
        /// 模型 ID。
        model_id: String,
    },
    /// 合并后比率非有限或越出 `(0.5, 16.0]`。
    #[error("Invalid tokenCharRatio for {model_id}")]
    InvalidTokenCharRatio {
        /// 模型 ID。
        model_id: String,
    },
}

/// 模型注册表（对照旧 `ModelRegistry`）：配置覆盖 + 内置表 + 保守默认值。
///
/// 旧实现是 Spring 单例，构造期跑一次 `applyConfiguredOverrides()` 把 YAML
/// 覆盖并入 `customModels`（任一条不合法即拒启）；本实现同构——
/// [`ModelRegistry::from_config`] 在装配期完成全部合并与校验，其后查表为纯读。
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ModelRegistry {
    /// 旧 `customModels`：配置覆盖，精确键，查表 Level 1。
    custom: BTreeMap<String, ModelCapabilities>,
}

impl ModelRegistry {
    /// 无配置覆盖的注册表（只走内置表 + 保守默认值）。
    #[must_use]
    pub fn builtin_only() -> Self {
        Self::default()
    }

    /// 按配置装配（逐条对照旧 `applyConfiguredOverrides` 的合并规则与校验）。
    ///
    /// # Errors
    ///
    /// 任一模型缺 `tokenCharRatio`、或合并后触犯窗口/比率不变量即返回错误
    /// （对照旧实现构造期 fail-fast，绝不静默降级为部分生效）。
    pub fn from_config(config: &ModelCapabilitiesConfig) -> Result<Self, CapabilitiesConfigError> {
        let mut custom = BTreeMap::new();
        for (model_id, over) in &config.capabilities {
            custom.insert(model_id.clone(), merge_override(model_id, over)?);
        }
        Ok(Self { custom })
    }

    /// 由配置 JSON 文本装配。
    ///
    /// # Errors
    ///
    /// 见 [`ModelCapabilitiesConfig::from_json_str`] 与 [`Self::from_config`]。
    pub fn from_json_str(json: &str) -> Result<Self, CapabilitiesConfigError> {
        Self::from_config(&ModelCapabilitiesConfig::from_json_str(json)?)
    }

    /// 由配置文件装配；文件缺失退化为 [`Self::builtin_only`]。
    ///
    /// # Errors
    ///
    /// 见 [`ModelCapabilitiesConfig::load`] 与 [`Self::from_config`]。
    pub fn from_path(path: &Path) -> Result<Self, CapabilitiesConfigError> {
        match ModelCapabilitiesConfig::load(path)? {
            Some(config) => Self::from_config(&config),
            None => Ok(Self::builtin_only()),
        }
    }

    /// 由 [`MODEL_CAPABILITIES_PATH_ENV`] 指向的配置文件装配；变量未设或为空白
    /// 时退化为 [`Self::builtin_only`]。
    ///
    /// # Errors
    ///
    /// 见 [`Self::from_path`]。
    pub fn from_env() -> Result<Self, CapabilitiesConfigError> {
        match std::env::var(MODEL_CAPABILITIES_PATH_ENV) {
            Ok(path) if !path.trim().is_empty() => Self::from_path(Path::new(path.trim())),
            _ => Ok(Self::builtin_only()),
        }
    }

    /// 查模型能力（对照旧 `getCapabilities`）：空白名直接取默认值；否则
    /// Level 1 配置覆盖 → Level 3 内置表（精确 + `<prefix>/*` 通配）→
    /// Level 4 [`DEFAULT_CAPABILITIES`]。
    #[must_use]
    pub fn capabilities(&self, model_id: &str) -> &ModelCapabilities {
        if model_id.trim().is_empty() {
            return &DEFAULT_CAPABILITIES;
        }
        if let Some(custom) = self.custom.get(model_id) {
            return custom;
        }
        builtin_capabilities(model_id).unwrap_or(&DEFAULT_CAPABILITIES)
    }

    /// 模型是否已知（对照旧 `isKnownModel`：配置覆盖键 → 内置精确键 →
    /// 内置 `<prefix>/*` 通配键；Provider 动态查询分支见模块文档留白说明）。
    #[must_use]
    pub fn is_known(&self, model_id: &str) -> bool {
        !model_id.is_empty()
            && (self.custom.contains_key(model_id) || builtin_capabilities(model_id).is_some())
    }

    /// 配置覆盖生效的模型 ID（升序；用于启动期日志与自检）。
    #[must_use]
    pub fn configured_model_ids(&self) -> Vec<&str> {
        self.custom.keys().map(String::as_str).collect()
    }
}

/// 合并单条配置覆盖（逐行对照旧 `applyConfiguredOverrides` 的循环体）。
///
/// 旧规则：`tokenCharRatio` 必填；`contextWindow` / `outputMaxTokens` 缺省取
/// base；`displayName` 在 base 为默认值（`"unknown"`）时取模型 ID 本身，否则
/// 取 base 展示名；`supportsStreaming` / `supportsVision` / `supportsToolUse` /
/// `supportsCache` 可覆盖；`supportsThinking` / `maxImages` / 两项费率恒取 base。
fn merge_override(
    model_id: &str,
    over: &CapabilityOverride,
) -> Result<ModelCapabilities, CapabilitiesConfigError> {
    let Some(token_char_ratio) = over.token_char_ratio else {
        return Err(CapabilitiesConfigError::MissingTokenCharRatio {
            model_id: model_id.to_owned(),
        });
    };
    let base = resolve_base(model_id);
    let display_name = if base.model_id == DEFAULT_CAPABILITIES.model_id {
        Cow::Owned(model_id.to_owned())
    } else {
        base.display_name.clone()
    };
    let merged = ModelCapabilities {
        model_id: Cow::Owned(model_id.to_owned()),
        display_name,
        max_output_tokens: over.output_max_tokens.unwrap_or(base.max_output_tokens),
        context_window: over.context_window.unwrap_or(base.context_window),
        supports_streaming: over.supports_streaming.unwrap_or(base.supports_streaming),
        supports_thinking: base.supports_thinking,
        supports_images: over.supports_vision.unwrap_or(base.supports_images),
        max_images: base.max_images,
        supports_tool_use: over.supports_tool_use.unwrap_or(base.supports_tool_use),
        cost_per_1k_input: base.cost_per_1k_input,
        cost_per_1k_output: base.cost_per_1k_output,
        token_char_ratio,
        supports_cache: over.supports_cache.unwrap_or(base.supports_cache),
    };
    validate_capabilities(&merged)?;
    Ok(merged)
}

/// 覆盖项的 base（对照旧 `resolveBase`）：内置表**精确**匹配（不走 `/*` 通配）
/// → [`DEFAULT_CAPABILITIES`]。Provider 动态查询分支见模块文档留白说明。
fn resolve_base(model_id: &str) -> &'static ModelCapabilities {
    BUILTIN_MODELS
        .iter()
        .find(|caps| caps.model_id == model_id)
        .unwrap_or(&DEFAULT_CAPABILITIES)
}

/// 内置表查询（精确匹配 → `<prefix>/*` 通配键；旧 Level 3 的两步）。
fn builtin_capabilities(model_id: &str) -> Option<&'static ModelCapabilities> {
    if let Some(exact) = BUILTIN_MODELS.iter().find(|caps| caps.model_id == model_id) {
        return Some(exact);
    }
    // 前缀匹配（旧实现：键以 `/*` 结尾时按去掉末位 `*` 的前缀比对）。
    BUILTIN_MODELS.iter().find(|caps| {
        caps.model_id.ends_with("/*")
            && caps
                .model_id
                .strip_suffix('*')
                .is_some_and(|prefix| model_id.starts_with(prefix))
    })
}

/// 能力不变量校验（逐字对照旧 `ModelCapabilities` 紧凑构造器的两条判定）。
fn validate_capabilities(caps: &ModelCapabilities) -> Result<(), CapabilitiesConfigError> {
    if caps.max_output_tokens == 0
        || caps.context_window == 0
        || caps.max_output_tokens >= caps.context_window
    {
        return Err(CapabilitiesConfigError::InvalidTokenLimits {
            model_id: caps.model_id.to_string(),
        });
    }
    if !caps.token_char_ratio.is_finite()
        || caps.token_char_ratio <= 0.5
        || caps.token_char_ratio > 16.0
    {
        return Err(CapabilitiesConfigError::InvalidTokenCharRatio {
            model_id: caps.model_id.to_string(),
        });
    }
    Ok(())
}

/// 全局注册表（对照旧 Spring 单例 `ModelRegistry` bean）。
static GLOBAL_REGISTRY: OnceLock<ModelRegistry> = OnceLock::new();

/// 装配全局注册表（zk-server 启动期调用一次：先 [`ModelRegistry::from_path`]
/// 拿 `Result` 做 fail-fast，再装配——由此保住旧实现「配置非法即拒启」语义）。
///
/// # Errors
///
/// 已装配（含已被 [`registry`] 惰性初始化）时把入参交回 `Err`，语义同
/// `OnceLock::set`。
pub fn install_registry(registry: ModelRegistry) -> Result<(), ModelRegistry> {
    GLOBAL_REGISTRY.set(registry)
}

/// 全局注册表。未经 [`install_registry`] 显式装配时，惰性读
/// [`MODEL_CAPABILITIES_PATH_ENV`] 装配一次。
///
/// **差异留痕**：惰性路径无处报错，故配置非法时记 `error!` 后退回内置表，而非
/// 旧实现的拒启。该降级仅在「未显式装配」时可达；zk-server 走
/// [`install_registry`] + fail-fast，行为与旧实现一致。
#[must_use]
pub fn registry() -> &'static ModelRegistry {
    GLOBAL_REGISTRY.get_or_init(|| {
        ModelRegistry::from_env().unwrap_or_else(|error| {
            tracing::error!(
                error = %error,
                "model capabilities config rejected; falling back to built-in table"
            );
            ModelRegistry::builtin_only()
        })
    })
}

/// 查模型能力（对照旧 `ModelRegistry.getCapabilities`，走全局注册表）。
#[must_use]
pub fn capabilities_for(model_id: &str) -> &'static ModelCapabilities {
    registry().capabilities(model_id)
}

/// 模型的最大输出 token（对照旧 `ModelRegistry.getMaxOutputTokensForModel`）。
#[must_use]
pub fn max_output_tokens_for(model_id: &str) -> u32 {
    capabilities_for(model_id).max_output_tokens
}

/// 模型是否已知（对照旧 `ModelRegistry.isKnownModel`，走全局注册表）。
#[must_use]
pub fn is_known_model(model_id: &str) -> bool {
    registry().is_known(model_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kimi_k3_matches_legacy_builtin_entry() {
        // 旧 BUILTIN_MODELS：kimi-k3 = 131072 输出 / 1000000 窗口 / 0.002 · 0.012。
        let caps = capabilities_for("kimi-k3");
        assert_eq!(caps.max_output_tokens, 131_072);
        assert_eq!(caps.context_window, 1_000_000);
        assert!((caps.cost_per_1k_input - 0.002).abs() < f64::EPSILON);
        assert!((caps.cost_per_1k_output - 0.012).abs() < f64::EPSILON);
        assert!(caps.supports_thinking);
        assert_eq!(caps.max_images, 8);
    }

    #[test]
    fn unknown_and_blank_models_fall_back_to_default() {
        for model in ["", "   ", "totally-unknown-model"] {
            let caps = capabilities_for(model);
            assert_eq!(caps.model_id, "unknown");
            assert_eq!(caps.max_output_tokens, 4096);
            assert_eq!(caps.context_window, 8192);
        }
        assert!(!is_known_model("totally-unknown-model"));
        assert!(is_known_model("kimi-k3"));
    }

    #[test]
    fn wildcard_prefix_matches_ollama_family() {
        // 旧前缀匹配：键 `ollama/*` 命中一切 `ollama/` 前缀模型。
        let caps = capabilities_for("ollama/llama3.1:8b");
        assert_eq!(caps.model_id, "ollama/*");
        assert_eq!(caps.max_output_tokens, 4096);
        assert!(!caps.supports_tool_use);
        assert!(is_known_model("ollama/qwen2"));
        // 非 `/` 分隔前缀不误命中。
        assert_eq!(capabilities_for("ollamaX").model_id, "unknown");
    }

    #[test]
    fn builtin_table_holds_legacy_invariants() {
        // 旧 record 紧凑构造器不变量：0 < maxOut < ctx；0.5 < ratio <= 16。
        for caps in BUILTIN_MODELS {
            assert!(caps.max_output_tokens > 0, "{}", caps.model_id);
            assert!(
                caps.max_output_tokens < caps.context_window,
                "{}",
                caps.model_id
            );
            assert!(
                caps.token_char_ratio > 0.5 && caps.token_char_ratio <= 16.0,
                "{}",
                caps.model_id
            );
        }
        // 8/28 模型迁移后：新增 qwen3.8-flash，GLM 视觉模型原位替换。
        assert_eq!(BUILTIN_MODELS.len(), 26);
        // 键唯一。
        let mut ids: Vec<&str> = BUILTIN_MODELS
            .iter()
            .map(|caps| caps.model_id.as_ref())
            .collect();
        ids.sort_unstable();
        let total = ids.len();
        ids.dedup();
        assert_eq!(ids.len(), total);
    }

    #[test]
    fn max_output_tokens_accessor_matches_capabilities() {
        assert_eq!(max_output_tokens_for("kimi-k3"), 131_072);
        assert_eq!(max_output_tokens_for("qwen3.7-max"), 65536);
        assert_eq!(max_output_tokens_for("deepseek-v4-pro"), 384_000);
        assert_eq!(
            max_output_tokens_for("deepseek-v4-flash-vision-exp"),
            384_000
        );
        assert_eq!(max_output_tokens_for("nope"), 4096);
    }

    #[test]
    fn new_deepseek_entries_match_legacy_registry_diff() {
        // 旧 ModelRegistry.java 新增三条：vision-exp 开图片输入（5 张上限）；
        // 0813 / 0731 为百炼渠道别名，参数与 pro / flash 本体逐字段一致。
        let vision = capabilities_for("deepseek-v4-flash-vision-exp");
        assert_eq!(vision.display_name, "DeepSeek V4 Flash Vision Exp");
        assert_eq!(vision.max_output_tokens, 384_000);
        assert_eq!(vision.context_window, 1_000_000);
        assert!(vision.supports_streaming);
        assert!(vision.supports_thinking);
        assert!(vision.supports_images);
        assert_eq!(vision.max_images, 5);
        assert!(vision.supports_tool_use);
        assert!((vision.cost_per_1k_input - 0.0005).abs() < f64::EPSILON);
        assert!((vision.cost_per_1k_output - 0.002).abs() < f64::EPSILON);

        let pro = capabilities_for("deepseek-v4-pro-0813");
        assert_eq!(pro.display_name, "DeepSeek V4 Pro 0813（百炼）");
        assert_eq!(pro.max_output_tokens, 384_000);
        assert_eq!(pro.context_window, 1_000_000);
        assert!(pro.supports_thinking);
        assert!(!pro.supports_images);
        assert_eq!(pro.max_images, 0);
        assert!((pro.cost_per_1k_input - 0.001).abs() < f64::EPSILON);
        assert!((pro.cost_per_1k_output - 0.004).abs() < f64::EPSILON);

        let flash = capabilities_for("deepseek-v4-flash-0731");
        assert_eq!(flash.display_name, "DeepSeek V4 Flash 0731（百炼）");
        assert_eq!(flash.max_output_tokens, 384_000);
        assert_eq!(flash.context_window, 1_000_000);
        assert!(flash.supports_thinking);
        assert!(!flash.supports_images);
        assert!((flash.cost_per_1k_input - 0.0005).abs() < f64::EPSILON);
        assert!((flash.cost_per_1k_output - 0.002).abs() < f64::EPSILON);
    }

    #[test]
    fn glm_53_family_matches_migrated_registry_entries() {
        let caps = capabilities_for("glm-5.3");
        assert_eq!(caps.display_name, "GLM-5.3");
        assert_eq!(caps.max_output_tokens, 131_072);
        assert_eq!(caps.context_window, 1_048_576);
        assert!(!caps.supports_images);
        assert!((caps.cost_per_1k_input - 0.001).abs() < f64::EPSILON);
        assert!((caps.cost_per_1k_output - 0.001).abs() < f64::EPSILON);
        assert!(is_known_model("glm-5.3"));

        let flash = capabilities_for("glm-5.3-flash");
        assert_eq!(flash.display_name, "GLM-5.3-Flash");
        assert_eq!(flash.max_output_tokens, 131_072);
        assert_eq!(flash.context_window, 1_048_576);
        assert!(flash.supports_streaming);
        assert!(flash.supports_thinking);
        assert!(flash.supports_images);
        assert_eq!(flash.max_images, 50);
        assert!(flash.supports_tool_use);
        assert!((flash.cost_per_1k_input - 0.00015).abs() < f64::EPSILON);
        assert!((flash.cost_per_1k_output - 0.0005).abs() < f64::EPSILON);
        assert!(is_known_model("glm-5.3-flash"));

        assert_eq!(
            BUILTIN_MODELS
                .iter()
                .filter(|caps| caps.model_id.starts_with("glm-5."))
                .count(),
            2
        );
    }

    #[test]
    fn qwen_38_flash_has_confirmed_zero_cost_capabilities() {
        let caps = capabilities_for("qwen3.8-flash");
        assert_eq!(caps.display_name, "Qwen 3.8 Flash（百炼）");
        assert_eq!(caps.max_output_tokens, 131_072);
        assert_eq!(caps.context_window, 1_000_000);
        assert!(caps.supports_streaming);
        assert!(caps.supports_thinking);
        assert!(caps.supports_images);
        assert_eq!(caps.max_images, 20);
        assert!(caps.supports_tool_use);
        assert!(caps.cost_per_1k_input.abs() < f64::EPSILON);
        assert!(caps.cost_per_1k_output.abs() < f64::EPSILON);
        assert!(is_known_model("qwen3.8-flash"));
    }

    /// 旧 `application.yml` 的 `model.capabilities` 节（L295+，8 个模型）一对一
    /// 转写为 JSON——用于证明配置面可从旧配置逐字迁移，且合并结果与旧
    /// `applyConfiguredOverrides` 一致。
    const LEGACY_YAML_AS_JSON: &str = r#"{
      "capabilities": {
        "claude-sonnet-4-6": { "contextWindow": 200000, "tokenCharRatio": 3.5, "outputMaxTokens": 64000,
          "supportsCache": true, "supportsToolUse": true, "supportsVision": true, "supportsStreaming": true },
        "qwen3.7-max":  { "contextWindow": 1000000, "tokenCharRatio": 2.5, "outputMaxTokens": 65536, "supportsToolUse": true },
        "qwen3.7-plus": { "contextWindow": 1000000, "tokenCharRatio": 2.5, "outputMaxTokens": 8192,  "supportsToolUse": true },
        "deepseek-coder": { "contextWindow": 65536, "tokenCharRatio": 2.8, "outputMaxTokens": 8192 },
        "anthropic/claude-opus-4.8": { "contextWindow": 1000000, "tokenCharRatio": 3.5, "outputMaxTokens": 64000,
          "supportsCache": true, "supportsToolUse": true, "supportsVision": true, "supportsStreaming": true },
        "anthropic/claude-fable-5": { "contextWindow": 1000000, "tokenCharRatio": 3.5, "outputMaxTokens": 64000,
          "supportsCache": true, "supportsToolUse": true, "supportsVision": true, "supportsStreaming": true },
        "openai/gpt-5.6-sol": { "contextWindow": 1050000, "tokenCharRatio": 3.5, "outputMaxTokens": 128000 },
        "google/gemini-3.5-flash": { "contextWindow": 1050000, "tokenCharRatio": 3.5, "outputMaxTokens": 65530 }
      }
    }"#;

    #[test]
    fn config_overrides_win_over_builtin_table() {
        let registry = ModelRegistry::from_json_str(LEGACY_YAML_AS_JSON).expect("legacy config");
        assert_eq!(registry.configured_model_ids().len(), 8);

        // 覆盖生效：比率不再是内置表的 3.5（旧 caps() 助手恒 3.5），而是配置的 2.5。
        let qwen = registry.capabilities("qwen3.7-max");
        assert!((qwen.token_char_ratio - 2.5).abs() < f64::EPSILON);
        assert_eq!(qwen.context_window, 1_000_000);
        assert_eq!(qwen.max_output_tokens, 65536);
        assert!(qwen.supports_tool_use);
        // 未覆盖维度恒取 base（内置表 qwen3.7-max）。
        assert_eq!(qwen.display_name, "Qwen 3.7 Max");
        assert!(qwen.supports_thinking);
        assert_eq!(qwen.max_images, 0);
        assert!((qwen.cost_per_1k_input - 0.009).abs() < f64::EPSILON);

        // claude-sonnet-4-6：内置表输出上限 16384 → 配置抬到 64000；supportsCache 覆盖为 true。
        let sonnet = registry.capabilities("claude-sonnet-4-6");
        assert_eq!(sonnet.max_output_tokens, 64000);
        assert_eq!(sonnet.context_window, 200_000);
        assert!(sonnet.supports_cache);
        assert_eq!(sonnet.display_name, "Claude Sonnet 4.6");

        // 未配置模型不受影响，仍取内置表原值。
        assert!((registry.capabilities("kimi-k3").token_char_ratio - 3.5).abs() < f64::EPSILON);
    }

    #[test]
    fn override_of_unlisted_model_takes_default_base_and_id_as_display_name() {
        // deepseek-coder 不在内置表 → 旧 resolveBase 落到 DEFAULT，
        // displayName 取模型 ID 本身（`"unknown".equals(base.modelId()) ? id : ...`）。
        let registry = ModelRegistry::from_json_str(LEGACY_YAML_AS_JSON).expect("legacy config");
        let caps = registry.capabilities("deepseek-coder");
        assert_eq!(caps.model_id, "deepseek-coder");
        assert_eq!(caps.display_name, "deepseek-coder");
        assert_eq!(caps.context_window, 65536);
        assert_eq!(caps.max_output_tokens, 8192);
        assert!((caps.token_char_ratio - 2.8).abs() < f64::EPSILON);
        // DEFAULT 的未覆盖维度：streaming true / thinking false / images false / 费率 0。
        assert!(caps.supports_streaming);
        assert!(!caps.supports_thinking);
        assert!(!caps.supports_images);
        assert!(!caps.supports_tool_use);
        assert!((caps.cost_per_1k_input - 0.0).abs() < f64::EPSILON);
        // 配置声明即「已知模型」（旧 isKnownModel 的 customModels 分支）。
        assert!(registry.is_known("deepseek-coder"));
        assert!(!ModelRegistry::builtin_only().is_known("deepseek-coder"));
    }

    #[test]
    fn config_rejects_missing_ratio_and_broken_invariants() {
        // 旧 applyConfiguredOverrides：tokenCharRatio 缺失 → IllegalStateException。
        let error =
            ModelRegistry::from_json_str(r#"{"capabilities":{"foo":{"contextWindow":1000}}}"#)
                .expect_err("missing ratio must fail");
        assert_eq!(
            error.to_string(),
            "model.capabilities.foo.tokenCharRatio is required"
        );

        // 旧紧凑构造器：maxOutputTokens >= contextWindow → Invalid model token limits。
        let error = ModelRegistry::from_json_str(
            r#"{"capabilities":{"foo":{"tokenCharRatio":3.5,"contextWindow":8192,"outputMaxTokens":8192}}}"#,
        )
        .expect_err("output >= context must fail");
        assert_eq!(error.to_string(), "Invalid model token limits for foo");

        // 旧紧凑构造器：ratio 越界 (0.5, 16.0] → Invalid tokenCharRatio。
        for ratio in ["0.5", "16.5"] {
            let json = format!(r#"{{"capabilities":{{"foo":{{"tokenCharRatio":{ratio}}}}}}}"#);
            let error = ModelRegistry::from_json_str(&json).expect_err("ratio out of range");
            assert_eq!(error.to_string(), "Invalid tokenCharRatio for foo");
        }
        // 边界内取值可通过（16.0 含、0.5 不含）。
        assert!(
            ModelRegistry::from_json_str(r#"{"capabilities":{"foo":{"tokenCharRatio":16.0}}}"#)
                .is_ok()
        );
    }

    #[test]
    fn empty_and_absent_config_degrade_to_builtin_table() {
        // 空对象、空 capabilities 均等价「无覆盖」（旧 setCapabilities(null) 归一为空 map）。
        for json in ["{}", r#"{"capabilities":{}}"#] {
            let registry = ModelRegistry::from_json_str(json).expect("empty config");
            assert_eq!(registry, ModelRegistry::builtin_only());
            assert_eq!(registry.capabilities("kimi-k3").max_output_tokens, 131_072);
        }
        // 文件缺失不是错误（旧 YAML 无该节即空 map）。
        let missing = std::env::temp_dir().join("zk-model-capabilities-absent-9c1f.json");
        let _ = std::fs::remove_file(&missing);
        assert_eq!(
            ModelRegistry::from_path(&missing).expect("absent file"),
            ModelRegistry::builtin_only()
        );
    }

    #[test]
    fn capabilities_read_from_config_file_on_disk() {
        // 「从配置文件读取而非硬编码」的端到端断言：落盘 → from_path → 查表。
        let path = std::env::temp_dir().join(format!(
            "zk-{}-{}.json",
            MODEL_CAPABILITIES_FILE_NAME.trim_end_matches(".json"),
            std::process::id()
        ));
        std::fs::write(
            &path,
            r#"{"capabilities":{"kimi-k3":{"tokenCharRatio":2.75,"contextWindow":262144,"outputMaxTokens":32768}}}"#,
        )
        .expect("write config");
        let registry = ModelRegistry::from_path(&path).expect("load config");
        let caps = registry.capabilities("kimi-k3");
        assert!((caps.token_char_ratio - 2.75).abs() < f64::EPSILON);
        assert_eq!(caps.context_window, 262_144);
        assert_eq!(caps.max_output_tokens, 32768);
        // 展示名/费率仍来自内置表 base。
        assert_eq!(caps.display_name, "Kimi K3");
        assert!((caps.cost_per_1k_output - 0.012).abs() < f64::EPSILON);
        std::fs::remove_file(&path).ok();

        // 非法 JSON → Parse 错误（不静默降级）。
        std::fs::write(&path, "{ not json").expect("write bad config");
        let error = ModelRegistry::from_path(&path).expect_err("bad json");
        assert!(
            error
                .to_string()
                .starts_with("failed to parse model capabilities config"),
            "{error}"
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn global_registry_is_readable_and_defaults_to_builtin_table() {
        // 未显式装配时惰性装配（测试进程未设 ZK_MODEL_CAPABILITIES_PATH → 内置表）。
        assert_eq!(
            registry().capabilities("kimi-k3").max_output_tokens,
            131_072
        );
        // 已初始化后再装配即拒绝（OnceLock::set 语义），入参原样交回。
        let rejected = install_registry(ModelRegistry::builtin_only());
        assert!(rejected.is_err());
    }
}
