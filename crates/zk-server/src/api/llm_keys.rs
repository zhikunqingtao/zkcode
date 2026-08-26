//! LLM 密钥管理端点——`GET/PUT /api/llm-keys`（Task 4 Step 5-6）。
//!
//! GET 返回所有声明目录 provider 的密钥状态（DB 密钥 + 环境变量密钥合并
//! 判定，密钥脱敏显示）。PUT 接收密钥更新，验证后落库并触发 provider
//! 注册表热替换（`SwappableProvider::swap`）。
//!
//! 密钥优先级：DB 保存的密钥 > 环境变量密钥。PUT 空值表示删除该 provider
//! 的 DB 密钥；包含 `****` 的值拒绝写入（防止把脱敏回显当明文存储）。
//!
//! 共享合并函数 [`build_merged_provider_configs`] 同时被 PUT handler 与
//! 启动时 [`crate::state::AppState::merge_db_llm_keys`] 调用。

use std::collections::BTreeMap;

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::error::ApiError;
use crate::state::AppState;

/// config 表内 LLM provider 密钥行的键名。
const DB_KEY: &str = "llm_provider_keys";

// ── DTO ──

/// 单个 provider 的密钥状态（GET 响应元素）。
#[derive(Debug, Serialize)]
pub(crate) struct LlmKeyProvider {
    /// provider 名（注册表键 / 环境变量词根）。
    pub name: String,
    /// 人类可读标签。
    pub label: String,
    /// 是否已配置密钥（DB 或环境变量）。
    pub has_key: bool,
    /// 脱敏密钥预览（未配置时为 `null`）。
    pub masked_key: Option<String>,
}

/// GET / PUT 共用响应形状。
#[derive(Debug, Serialize)]
pub(crate) struct LlmKeysResponse {
    pub providers: Vec<LlmKeyProvider>,
}

/// PUT 请求体。
#[derive(Debug, Deserialize)]
pub(crate) struct LlmKeysUpdate {
    pub keys: BTreeMap<String, String>,
}

// ── 标签 ──

/// provider 名 → 人类可读标签（与 `PROVIDER_CATALOG` 顺序对齐）。
fn provider_label(name: &str) -> &'static str {
    match name {
        "dashscope" => "DashScope (通义千问)",
        "dashscope-token-plan" => "DashScope (百炼订阅)",
        "deepseek" => "DeepSeek",
        "moonshot" => "Moonshot (Kimi)",
        "zhipu" => "智谱 (GLM)",
        "minimax" => "MiniMax",
        "zenmux" => "ZenMux",
        "anthropic" => "Anthropic (Claude)",
        "openai" => "OpenAI",
        _ => "Unknown",
    }
}

// ── 脱敏 ──

/// 密钥脱敏：保留前 5 + `****` + 末 4；不足 12 字符全 `****`。
fn mask_key(key: &str) -> String {
    let chars: Vec<char> = key.chars().collect();
    if chars.len() < 12 {
        return "****".to_owned();
    }
    let prefix: String = chars[..5].iter().collect();
    let suffix: String = chars[chars.len() - 4..].iter().collect();
    format!("{prefix}****{suffix}")
}

// ── DB 读写 ──

/// 从 DB 读取 `llm_provider_keys`（解析失败回空表，不阻断服务）。
async fn load_db_keys(state: &AppState) -> Result<BTreeMap<String, String>, ApiError> {
    let stored = state.db.get_config_value(DB_KEY).await?;
    Ok(stored
        .and_then(|json| {
            serde_json::from_str::<BTreeMap<String, String>>(&json)
                .map_err(|err| {
                    tracing::warn!(
                        error = %err,
                        "stored llm provider keys unparseable, treating as empty"
                    );
                })
                .ok()
        })
        .unwrap_or_default())
}

// ── 密钥状态列表构建 ──

/// 构建所有声明目录 provider 的密钥状态列表。
///
/// `has_key` = DB 密钥存在 **或** 环境变量密钥存在。
/// `masked_key` 取 DB 密钥（优先）或环境变量密钥的脱敏预览。
fn build_provider_list(db_keys: &BTreeMap<String, String>) -> Vec<LlmKeyProvider> {
    zk_llm::PROVIDER_CATALOG
        .iter()
        .map(|entry| {
            let env_key = std::env::var(zk_llm::config::api_key_env(entry.name))
                .ok()
                .filter(|v| !v.trim().is_empty());
            // DB 密钥优先于环境变量密钥。
            let effective_key: Option<&str> = db_keys
                .get(entry.name)
                .map(String::as_str)
                .or(env_key.as_deref());
            LlmKeyProvider {
                name: entry.name.to_owned(),
                label: provider_label(entry.name).to_owned(),
                has_key: effective_key.is_some(),
                masked_key: effective_key.map(mask_key),
            }
        })
        .collect()
}

// ── 合并配置构建（PUT handler + 启动时共用） ──

/// 合并 DB 密钥与环境变量密钥，构建 provider 配置列表。
///
/// 密钥优先级：DB > 环境变量。对每个 [`zk_llm::config::PROVIDER_CATALOG`]
/// 条目，若 DB 或环境变量提供了密钥，则通过
/// [`zk_llm::config::config_from_env_with`] 装配完整配置（含 `base_url` /
/// `models` / `default_model` / protocol 的环境变量覆盖语义不变）。返回的
/// 配置列表可直接传入 [`zk_llm::ProviderRegistry::from_configs`]。
///
/// 此函数同时被 PUT handler（[`put_llm_keys`]）与启动时
/// [`crate::state::AppState::merge_db_llm_keys`] 调用——共享合并逻辑。
pub(crate) fn build_merged_provider_configs(
    db_keys: &BTreeMap<String, String>,
) -> Vec<zk_llm::config::ProviderConfig> {
    use zk_llm::config::{PROVIDER_CATALOG, api_key_env, config_from_env_with};

    PROVIDER_CATALOG
        .iter()
        .filter_map(|entry| {
            let key_var = api_key_env(entry.name);
            // DB 密钥优先；缺失时回退到环境变量。
            let effective_key = db_keys.get(entry.name).cloned().or_else(|| {
                std::env::var(&key_var)
                    .ok()
                    .filter(|v| !v.trim().is_empty())
            });
            // 无任何密钥 → 不注册该 provider（与 provider_configs_from_env 一致）。
            let effective_key = effective_key?;
            // 注入式 lookup：API_KEY 返回合并后的密钥，其余变量直读环境。
            let lookup = |var: &str| {
                if var == key_var {
                    Some(effective_key.clone())
                } else {
                    std::env::var(var).ok().filter(|v| !v.trim().is_empty())
                }
            };
            config_from_env_with(entry, lookup)
        })
        .collect()
}

/// Build the complete production registry for the current credentials.
///
/// Named providers always use their own catalog endpoint, so an `openai` key
/// cannot accidentally be sent to `DashScope`.  The legacy `ZK_LLM_*` provider
/// is considered only when no named provider is configured and its key is
/// explicitly non-empty.
pub(crate) fn build_provider_registry(
    config: &Config,
    db_keys: &BTreeMap<String, String>,
) -> Result<zk_llm::ProviderRegistry, zk_llm::ProviderError> {
    let configs = build_merged_provider_configs(db_keys);
    let registry = if !configs.is_empty() {
        zk_llm::ProviderRegistry::from_configs(configs)?
    } else if !config.llm_api_key.is_empty() {
        zk_llm::ProviderRegistry::from_configs(vec![zk_llm::ProviderConfig::new(
            "openai-compat",
            config.llm_base_url.clone(),
            config.llm_api_key.clone(),
            config.default_model.clone(),
            Vec::new(),
        )])?
    } else {
        zk_llm::ProviderRegistry::new()
    };
    Ok(registry
        .with_default_model(config.default_model.clone())
        .with_fallback_chain(zk_llm::config::fallback_chain_from_env()))
}

// ── Handlers ──

/// `GET /api/llm-keys`——返回所有声明目录 provider 的密钥状态。
pub(crate) async fn get_llm_keys(
    State(state): State<AppState>,
) -> Result<Json<LlmKeysResponse>, ApiError> {
    let db_keys = load_db_keys(&state).await?;
    Ok(Json(LlmKeysResponse {
        providers: build_provider_list(&db_keys),
    }))
}

/// `PUT /api/llm-keys`——保存密钥并触发 provider 注册表热替换。
///
/// 1. 验证：包含 `****` 的值拒绝写入（返回 400）。
/// 2. 空字符串表示删除该 provider 的 DB 密钥。
/// 3. 合并现有 DB 密钥，落库。
/// 4. 热替换：env configs + DB keys overlay → new registry → `swap`。
/// 5. 回显更新后的密钥状态列表（同 GET 格式）。
pub(crate) async fn put_llm_keys(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<LlmKeysResponse>, ApiError> {
    if body.len() > 64 * 1024 {
        return Err(ApiError::validation("provider key request is too large"));
    }
    let update: LlmKeysUpdate =
        serde_json::from_slice(&body).map_err(|_| ApiError::invalid_request_body())?;

    if update.keys.len() > zk_llm::PROVIDER_CATALOG.len() {
        return Err(ApiError::validation("too many provider keys"));
    }

    // Only declared provider identities are accepted.  Apart from closing a
    // configuration typo, this prevents an attacker from turning a provider
    // name into an arbitrary environment-variable lookup in MCP resolution.
    for (name, value) in &update.keys {
        if !zk_llm::PROVIDER_CATALOG
            .iter()
            .any(|entry| entry.name == name)
        {
            return Err(ApiError::validation(format!(
                "unknown LLM provider '{name}'"
            )));
        }
        if value.contains("****") {
            return Err(ApiError::validation(format!(
                "refusing to write masked key for provider '{name}'"
            )));
        }
        if value.len() > 512 || value.chars().any(char::is_control) {
            return Err(ApiError::validation(format!(
                "invalid API key for provider '{name}'"
            )));
        }
    }

    // 加载现有 DB 密钥，合并更新（空值 = 删除）。
    let mut current = load_db_keys(&state).await?;
    for (name, value) in update.keys {
        if value.trim().is_empty() {
            current.remove(&name);
        } else {
            current.insert(name, value.trim().to_owned());
        }
    }

    // Construct the replacement before committing it so a TLS/client setup
    // failure cannot leave durable credentials and the live registry split.
    let registry = build_provider_registry(&state.config, &current).map_err(|err| {
        tracing::error!(error = %err, "failed to rebuild provider registry");
        ApiError::internal()
    })?;

    // 落库。
    let json = serde_json::to_string(&current).map_err(|err| {
        tracing::error!(error = %err, "failed to serialize llm provider keys");
        ApiError::internal()
    })?;
    state.db.put_config_value(DB_KEY, &json).await?;

    state.replace_llm_provider_keys(current.clone());
    state.providers.swap(registry);
    // MCP transports own immutable Authorization headers.  Refresh them in a
    // coalesced background pass so this local settings request never waits on
    // DNS/network handshakes and a removed key cannot remain active in memory.
    state.mcp().schedule_registry_credential_refresh();

    tracing::info!(
        db_key_count = current.len(),
        "llm provider keys updated and registry hot-reloaded"
    );

    // 回显更新后的密钥状态列表。
    Ok(Json(LlmKeysResponse {
        providers: build_provider_list(&current),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;

    #[test]
    fn mask_key_preserves_prefix_and_suffix() {
        // 13 字符：前 5 + **** + 末 4。
        assert_eq!(mask_key("sk-abcd567890"), "sk-ab****7890");
        // 26 字符：同规则。
        assert_eq!(mask_key("sk-sp-abcdefghijklmnopcCMA"), "sk-sp****cCMA");
    }

    #[test]
    fn mask_key_short_value_is_all_stars() {
        // 不足 12 字符全 ****。
        assert_eq!(mask_key("short"), "****");
        assert_eq!(mask_key("only11chars"), "****");
        // 恰好 12 字符：边界值，可以脱敏。
        assert_eq!(mask_key("abcdefghijkl"), "abcde****ijkl");
    }

    #[test]
    fn build_merged_configs_registers_db_keyed_provider() {
        let mut db_keys = BTreeMap::new();
        db_keys.insert("moonshot".to_owned(), "sk-db-moonshot-key-12345".to_owned());
        let configs = build_merged_provider_configs(&db_keys);
        let moonshot = configs
            .iter()
            .find(|c| c.name == "moonshot")
            .expect("moonshot registered from DB key");
        assert_eq!(moonshot.api_keys.len(), 1);
        assert_eq!(
            moonshot.next_api_key().expect("key").expose(),
            "sk-db-moonshot-key-12345"
        );
    }

    #[test]
    fn openai_db_key_uses_the_official_openai_endpoint() {
        let mut db_keys = BTreeMap::new();
        db_keys.insert("openai".to_owned(), "sk-openai-test-only-12345".to_owned());
        let configs = build_merged_provider_configs(&db_keys);
        let openai = configs
            .iter()
            .find(|config| config.name == "openai")
            .expect("OpenAI provider is registered from its DB key");
        assert_eq!(openai.base_url, zk_llm::OPENAI_BASE_URL);
        assert_eq!(openai.default_model, "gpt-4o");
        assert!(openai.models.iter().all(|model| !model.starts_with("qwen")));
    }

    #[test]
    fn build_provider_list_shows_db_key_status() {
        // 验证 build_provider_list 正确反映 DB 密钥状态。
        let mut db_keys = BTreeMap::new();
        db_keys.insert("moonshot".to_owned(), "sk-moonshot-abcdef123456".to_owned());
        let list = build_provider_list(&db_keys);
        let moonshot = list
            .iter()
            .find(|p| p.name == "moonshot")
            .expect("moonshot in list");
        assert!(moonshot.has_key, "DB key present → has_key true");
        assert_eq!(moonshot.masked_key.as_deref(), Some("sk-mo****3456"));
    }

    #[test]
    fn build_merged_configs_empty_db_returns_env_only() {
        // 无 DB 密钥时，结果取决于环境变量（测试环境通常无 LLM_PROVIDER_* 配置）。
        let db_keys = BTreeMap::new();
        let configs = build_merged_provider_configs(&db_keys);
        // 所有返回的配置都不应来自 DB（因为 DB 密钥为空）。
        for config in &configs {
            assert!(
                !db_keys.contains_key(&config.name),
                "provider should not be registered from empty DB keys"
            );
        }
    }

    #[tokio::test]
    async fn production_handler_persists_and_hot_loads_a_provider_key() {
        let state = AppState::for_tests();
        let body =
            Bytes::from_static(br#"{"keys":{"moonshot":"sk-moonshot-handler-test-123456"}}"#);
        let response = put_llm_keys(State(state.clone()), body)
            .await
            .expect("valid key update succeeds");

        let moonshot = response
            .0
            .providers
            .iter()
            .find(|provider| provider.name == "moonshot")
            .expect("Moonshot remains in the declared provider list");
        assert!(moonshot.has_key);
        let provider_keys = state.llm_provider_keys_handle();
        let stored_key = match provider_keys.read() {
            Ok(keys) => keys.get("moonshot").cloned(),
            Err(poisoned) => poisoned.into_inner().get("moonshot").cloned(),
        };
        assert_eq!(
            stored_key.as_deref(),
            Some("sk-moonshot-handler-test-123456")
        );
        assert_eq!(
            state.providers.load().resolve_provider("kimi-k3"),
            Some("moonshot")
        );
        let stored = state
            .db
            .get_config_value(DB_KEY)
            .await
            .expect("runtime DB read succeeds")
            .expect("key map was persisted");
        let stored: BTreeMap<String, String> =
            serde_json::from_str(&stored).expect("stored key map is valid JSON");
        assert!(stored.contains_key("moonshot"));
    }

    #[tokio::test]
    async fn production_handler_rejects_unknown_provider_identity() {
        let state = AppState::for_tests();
        let body = Bytes::from_static(br#"{"keys":{"arbitrary-env-name":"not-a-key"}}"#);
        assert!(put_llm_keys(State(state.clone()), body).await.is_err());
        assert!(
            state
                .db
                .get_config_value(DB_KEY)
                .await
                .expect("runtime DB read succeeds")
                .is_none(),
            "invalid provider names must not be persisted"
        );
    }
}
