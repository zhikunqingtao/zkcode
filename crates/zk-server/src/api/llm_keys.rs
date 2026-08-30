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

/// Load `llm_provider_keys`; invalid durable state fails closed so a PUT cannot
/// silently replace an unparseable credential map.
pub(crate) async fn load_db_keys(state: &AppState) -> Result<BTreeMap<String, String>, ApiError> {
    let stored = state
        .db
        .get_config_value(crate::demo_credentials::KEYS_DB_KEY)
        .await?;
    match stored {
        Some(json) => serde_json::from_str(&json).map_err(|error| {
            tracing::error!(error = %error, "stored LLM provider keys are invalid");
            ApiError::internal()
        }),
        None => Ok(BTreeMap::new()),
    }
}

/// Load the private public-demo provenance companion row.
pub(crate) async fn load_db_provenance(
    state: &AppState,
) -> Result<crate::demo_credentials::ProvenanceMap, ApiError> {
    let stored = state
        .db
        .get_config_value(crate::demo_credentials::PROVENANCE_DB_KEY)
        .await?;
    match stored {
        Some(json) => serde_json::from_str(&json).map_err(|error| {
            tracing::error!(error = %error, "stored LLM key provenance is invalid");
            ApiError::internal()
        }),
        None => Ok(crate::demo_credentials::ProvenanceMap::new()),
    }
}

pub(crate) async fn persist_db_key_state(
    state: &AppState,
    keys: &BTreeMap<String, String>,
    provenance: &crate::demo_credentials::ProvenanceMap,
) -> Result<(), ApiError> {
    let keys_json = serde_json::to_string(keys).map_err(|error| {
        tracing::error!(error = %error, "failed to serialize LLM provider keys");
        ApiError::internal()
    })?;
    let provenance_json = serde_json::to_string(provenance).map_err(|error| {
        tracing::error!(error = %error, "failed to serialize LLM key provenance");
        ApiError::internal()
    })?;
    state
        .db
        .put_config_values_atomically(&[
            (crate::demo_credentials::KEYS_DB_KEY, &keys_json),
            (crate::demo_credentials::PROVENANCE_DB_KEY, &provenance_json),
        ])
        .await?;
    Ok(())
}

fn load_demo_catalog(state: &AppState) -> Result<crate::demo_credentials::Catalog, ApiError> {
    crate::demo_credentials::load_catalog(&state.config.demo_credentials_path).map_err(|error| {
        tracing::error!(error = %error, "public demo credential catalog is unavailable");
        ApiError::internal()
    })
}

// ── 密钥状态列表构建 ──

/// 构建所有声明目录 provider 的密钥状态列表。
///
/// `has_key` and `masked_key` reflect the same effective credentials used by
/// the live registry, including public-demo filtering when opt-out is active.
fn build_provider_list(
    config: &Config,
    demo_catalog: &crate::demo_credentials::Catalog,
    db_keys: &BTreeMap<String, String>,
) -> Vec<LlmKeyProvider> {
    build_provider_list_with_lookup(config, demo_catalog, db_keys, |var| {
        std::env::var(var)
            .ok()
            .filter(|value| !value.trim().is_empty())
    })
}

fn build_provider_list_with_lookup(
    config: &Config,
    demo_catalog: &crate::demo_credentials::Catalog,
    db_keys: &BTreeMap<String, String>,
    lookup: impl Fn(&str) -> Option<String>,
) -> Vec<LlmKeyProvider> {
    zk_llm::PROVIDER_CATALOG
        .iter()
        .map(|entry| {
            let effective_key = effective_named_provider_key(
                config,
                demo_catalog,
                db_keys,
                entry.name,
                lookup(&zk_llm::config::api_key_env(entry.name)),
            );
            LlmKeyProvider {
                name: entry.name.to_owned(),
                label: provider_label(entry.name).to_owned(),
                has_key: effective_key.is_some(),
                masked_key: effective_key.as_deref().map(mask_key),
            }
        })
        .collect()
}

fn effective_named_provider_key(
    config: &Config,
    demo_catalog: &crate::demo_credentials::Catalog,
    db_keys: &BTreeMap<String, String>,
    provider: &str,
    environment_key: Option<String>,
) -> Option<String> {
    db_keys
        .get(provider)
        .cloned()
        .and_then(|raw| apply_named_demo_policy(config, demo_catalog, raw))
        .or_else(|| {
            environment_key.and_then(|raw| apply_named_demo_policy(config, demo_catalog, raw))
        })
}

fn apply_named_demo_policy(
    config: &Config,
    demo_catalog: &crate::demo_credentials::Catalog,
    raw: String,
) -> Option<String> {
    let has_effective_member = raw.split(',').any(|part| !part.trim().is_empty());
    if !has_effective_member {
        None
    } else if config.demo_credential_allowed {
        Some(raw)
    } else {
        // Runtime opt-out is value-bound, not label-bound: a known public key
        // must not become usable merely by storing it under another provider.
        demo_catalog.retain_values_not_public_for_any_provider(&raw)
    }
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
    config: &Config,
    demo_catalog: &crate::demo_credentials::Catalog,
    db_keys: &BTreeMap<String, String>,
) -> Vec<zk_llm::config::ProviderConfig> {
    build_merged_provider_configs_with_lookup(config, demo_catalog, db_keys, |var| {
        std::env::var(var)
            .ok()
            .filter(|value| !value.trim().is_empty())
    })
}

fn build_merged_provider_configs_with_lookup(
    config: &Config,
    demo_catalog: &crate::demo_credentials::Catalog,
    db_keys: &BTreeMap<String, String>,
    lookup: impl Fn(&str) -> Option<String>,
) -> Vec<zk_llm::config::ProviderConfig> {
    use zk_llm::config::{PROVIDER_CATALOG, api_key_env, config_from_env_with};

    PROVIDER_CATALOG
        .iter()
        .filter_map(|entry| {
            let key_var = api_key_env(entry.name);
            // DB 密钥优先；若禁用策略过滤掉其全部 public-demo 成员，则回退到
            // 同 provider 的安全环境变量密钥，而不是让 public DB 值遮蔽它。
            let effective_key = effective_named_provider_key(
                config,
                demo_catalog,
                db_keys,
                entry.name,
                lookup(&key_var),
            );
            // 无任何密钥 → 不注册该 provider（与 provider_configs_from_env 一致）。
            let effective_key = effective_key?;
            // 注入式 lookup：API_KEY 返回合并后的密钥，其余变量直读环境。
            let lookup = |var: &str| {
                if var == key_var {
                    Some(effective_key.clone())
                } else {
                    lookup(var).filter(|value| !value.trim().is_empty())
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
    demo_catalog: &crate::demo_credentials::Catalog,
    db_keys: &BTreeMap<String, String>,
) -> Result<zk_llm::ProviderRegistry, zk_llm::ProviderError> {
    let configs = build_merged_provider_configs(config, demo_catalog, db_keys);
    build_provider_registry_from_configs(config, demo_catalog, configs)
}

#[cfg(test)]
fn build_provider_registry_with_lookup(
    config: &Config,
    demo_catalog: &crate::demo_credentials::Catalog,
    db_keys: &BTreeMap<String, String>,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<zk_llm::ProviderRegistry, zk_llm::ProviderError> {
    let configs = build_merged_provider_configs_with_lookup(config, demo_catalog, db_keys, lookup);
    build_provider_registry_from_configs(config, demo_catalog, configs)
}

fn build_provider_registry_from_configs(
    config: &Config,
    demo_catalog: &crate::demo_credentials::Catalog,
    configs: Vec<zk_llm::config::ProviderConfig>,
) -> Result<zk_llm::ProviderRegistry, zk_llm::ProviderError> {
    let registry = if configs.is_empty() {
        let legacy_keys = if config.demo_credential_allowed {
            (!config.llm_api_key.is_empty()).then(|| config.llm_api_key.expose().to_owned())
        } else {
            demo_catalog.retain_values_not_public_for_any_provider(config.llm_api_key.expose())
        };
        match legacy_keys {
            Some(legacy_keys) => {
                zk_llm::ProviderRegistry::from_configs(vec![zk_llm::ProviderConfig::with_keys(
                    "openai-compat",
                    config.llm_base_url.clone(),
                    zk_llm::ApiKeyRing::from_csv(&legacy_keys),
                    config.default_model.clone(),
                    Vec::new(),
                )])?
            }
            None => zk_llm::ProviderRegistry::new(),
        }
    } else {
        zk_llm::ProviderRegistry::from_configs(configs)?
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
    let demo_catalog = load_demo_catalog(&state)?;
    let db_keys = load_db_keys(&state).await?;
    Ok(Json(LlmKeysResponse {
        providers: build_provider_list(&state.config, &demo_catalog, &db_keys),
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

    let demo_catalog = load_demo_catalog(&state)?;

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
        if !state.config.demo_credential_allowed
            && !value.trim().is_empty()
            && demo_catalog.contains_public_value_for_any_provider(value)
        {
            return Err(ApiError::validation(
                "the public demo credential is disabled for this source checkout",
            ));
        }
    }

    // Serialize the complete read-modify-write and live replacement sequence.
    // Atomic pair persistence alone does not prevent concurrent handlers from
    // reading the same old pair and then overwriting one another.
    let _credential_update_guard = state.llm_credential_update_lock.lock().await;

    // 加载现有 DB 密钥，合并更新（空值 = 删除）。
    let mut current = load_db_keys(&state).await?;
    let mut provenance = load_db_provenance(&state).await?;
    for (name, value) in update.keys {
        // Every explicit user update revokes any prior public-demo proof. User
        // credentials never receive a persisted fingerprint.
        provenance.remove(&name);
        if value.trim().is_empty() {
            current.remove(&name);
        } else {
            current.insert(name, value.trim().to_owned());
        }
    }

    // Construct the replacement before committing it so a TLS/client setup
    // failure cannot leave durable credentials and the live registry split.
    let registry =
        build_provider_registry(&state.config, &demo_catalog, &current).map_err(|err| {
            tracing::error!(error = %err, "failed to rebuild provider registry");
            ApiError::internal()
        })?;

    // Persist the unchanged key-map shape and provenance companion atomically.
    persist_db_key_state(&state, &current, &provenance).await?;

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
        providers: build_provider_list(&state.config, &demo_catalog, &current),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;

    fn build_test_merged_provider_configs(
        db_keys: &BTreeMap<String, String>,
    ) -> Vec<zk_llm::config::ProviderConfig> {
        let config = crate::config::Config::test_config();
        let catalog = crate::demo_credentials::load_catalog(&config.demo_credentials_path)
            .expect("tracked public demo catalog");
        build_merged_provider_configs_with_lookup(&config, &catalog, db_keys, |_| None)
    }

    fn build_test_provider_list(db_keys: &BTreeMap<String, String>) -> Vec<LlmKeyProvider> {
        let config = crate::config::Config::test_config();
        let catalog = crate::demo_credentials::load_catalog(&config.demo_credentials_path)
            .expect("tracked public demo catalog");
        build_provider_list_with_lookup(&config, &catalog, db_keys, |_| None)
    }

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
        let configs = build_test_merged_provider_configs(&db_keys);
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
        let configs = build_test_merged_provider_configs(&db_keys);
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
        let list = build_test_provider_list(&db_keys);
        let moonshot = list
            .iter()
            .find(|p| p.name == "moonshot")
            .expect("moonshot in list");
        assert!(moonshot.has_key, "DB key present → has_key true");
        assert_eq!(moonshot.masked_key.as_deref(), Some("sk-mo****3456"));
    }

    #[test]
    fn provider_list_hides_public_only_environment_key_when_opted_out() {
        let config = crate::config::Config::test_config();
        let catalog = crate::demo_credentials::load_catalog(&config.demo_credentials_path)
            .expect("tracked public demo catalog");
        let public_key = catalog
            .credentials()
            .get("dashscope-token-plan")
            .expect("public key")
            .clone();
        let list = build_provider_list_with_lookup(&config, &catalog, &BTreeMap::new(), |var| {
            (var == "LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY")
                .then(|| format!(", {public_key}, {public_key},"))
        });
        let provider = list
            .iter()
            .find(|provider| provider.name == "dashscope-token-plan")
            .expect("declared provider remains listed");
        assert!(!provider.has_key);
        assert!(provider.masked_key.is_none());
    }

    #[test]
    fn provider_list_masks_only_retained_user_environment_members_when_opted_out() {
        let config = crate::config::Config::test_config();
        let catalog = crate::demo_credentials::load_catalog(&config.demo_credentials_path)
            .expect("tracked public demo catalog");
        let public_key = catalog
            .credentials()
            .get("dashscope-token-plan")
            .expect("public key")
            .clone();
        let list = build_provider_list_with_lookup(&config, &catalog, &BTreeMap::new(), |var| {
            (var == "LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY")
                .then(|| format!("{public_key},,sk-user-environment-key-123456,{public_key}"))
        });
        let provider = list
            .iter()
            .find(|provider| provider.name == "dashscope-token-plan")
            .expect("declared provider remains listed");
        assert!(provider.has_key);
        assert_eq!(provider.masked_key.as_deref(), Some("sk-us****3456"));
    }

    #[test]
    fn build_merged_configs_empty_db_and_injected_environment_returns_empty() {
        let db_keys = BTreeMap::new();
        let configs = build_test_merged_provider_configs(&db_keys);
        assert!(configs.is_empty());
    }

    #[test]
    fn opted_out_named_environment_ring_filters_public_members_and_keeps_user_members() {
        let config = crate::config::Config::test_config();
        let catalog = crate::demo_credentials::load_catalog(&config.demo_credentials_path)
            .expect("tracked public demo catalog");
        let public_key = catalog
            .credentials()
            .get("dashscope-token-plan")
            .expect("public key")
            .clone();
        let configs =
            build_merged_provider_configs_with_lookup(&config, &catalog, &BTreeMap::new(), |var| {
                (var == "LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY")
                    .then(|| format!(" {public_key},,user-env-key,{public_key},"))
            });
        let provider = configs
            .iter()
            .find(|provider| provider.name == "dashscope-token-plan")
            .expect("safe environment member keeps provider registered");
        assert_eq!(provider.api_keys.len(), 1);
        assert_eq!(
            provider.next_api_key().expect("retained key").expose(),
            "user-env-key"
        );
    }

    #[test]
    fn opted_out_named_environment_ring_drops_provider_when_only_public_members_remain() {
        let config = crate::config::Config::test_config();
        let catalog = crate::demo_credentials::load_catalog(&config.demo_credentials_path)
            .expect("tracked public demo catalog");
        let public_key = catalog
            .credentials()
            .get("dashscope-token-plan")
            .expect("public key")
            .clone();
        let configs =
            build_merged_provider_configs_with_lookup(&config, &catalog, &BTreeMap::new(), |var| {
                (var == "LLM_PROVIDER_DASHSCOPE_TOKEN_PLAN_API_KEY")
                    .then(|| format!(",{public_key}, {public_key},"))
            });
        assert!(
            configs
                .iter()
                .all(|provider| provider.name != "dashscope-token-plan")
        );
    }

    #[test]
    fn opted_out_named_environment_rejects_a_public_value_under_another_provider() {
        let config = crate::config::Config::test_config();
        let catalog = crate::demo_credentials::load_catalog(&config.demo_credentials_path)
            .expect("tracked public demo catalog");
        let public_key = catalog
            .credentials()
            .get("dashscope-token-plan")
            .expect("public key")
            .clone();
        let configs =
            build_merged_provider_configs_with_lookup(&config, &catalog, &BTreeMap::new(), |var| {
                (var == "LLM_PROVIDER_DASHSCOPE_API_KEY").then(|| public_key.clone())
            });
        assert!(configs.iter().all(|provider| provider.name != "dashscope"));
    }

    #[test]
    fn opted_out_legacy_ring_filters_public_members_across_provider_names() {
        let mut config = crate::config::Config::test_config();
        let catalog = crate::demo_credentials::load_catalog(&config.demo_credentials_path)
            .expect("tracked public demo catalog");
        let public_key = catalog
            .credentials()
            .get("dashscope-token-plan")
            .expect("public key");
        config.llm_api_key =
            zk_llm::ApiKey::new(format!("user-legacy-key,,{public_key},user-legacy-key-two"));
        let retained = catalog
            .retain_values_not_public_for_any_provider(config.llm_api_key.expose())
            .expect("user legacy members remain");
        assert_eq!(retained, "user-legacy-key,user-legacy-key-two");
        let registry =
            build_provider_registry_with_lookup(&config, &catalog, &BTreeMap::new(), |_| None)
                .expect("filtered legacy registry builds");
        assert_eq!(registry.len(), 1);

        config.llm_api_key = zk_llm::ApiKey::new(format!(",{public_key},{public_key},"));
        let registry =
            build_provider_registry_with_lookup(&config, &catalog, &BTreeMap::new(), |_| None)
                .expect("public-only legacy registry builds empty");
        assert!(registry.is_empty());
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
            .get_config_value(crate::demo_credentials::KEYS_DB_KEY)
            .await
            .expect("runtime DB read succeeds")
            .expect("key map was persisted");
        let stored: BTreeMap<String, String> =
            serde_json::from_str(&stored).expect("stored key map is valid JSON");
        assert!(stored.contains_key("moonshot"));
        let provenance = state
            .db
            .get_config_value(crate::demo_credentials::PROVENANCE_DB_KEY)
            .await
            .expect("runtime DB read succeeds")
            .expect("companion row was persisted");
        let provenance: crate::demo_credentials::ProvenanceMap =
            serde_json::from_str(&provenance).expect("provenance map is valid JSON");
        assert!(
            provenance.is_empty(),
            "user credentials must not receive persisted fingerprints"
        );
    }

    #[tokio::test]
    async fn concurrent_provider_updates_do_not_lose_keys_or_diverge_from_live_registry() {
        let state = AppState::for_tests();
        let moonshot = put_llm_keys(
            State(state.clone()),
            Bytes::from_static(br#"{"keys":{"moonshot":"user-moonshot-key"}}"#),
        );
        let zhipu = put_llm_keys(
            State(state.clone()),
            Bytes::from_static(br#"{"keys":{"zhipu":"user-zhipu-key"}}"#),
        );

        let (moonshot_result, zhipu_result) = tokio::join!(moonshot, zhipu);
        let _ = moonshot_result.expect("concurrent Moonshot update succeeds");
        let _ = zhipu_result.expect("concurrent Zhipu update succeeds");

        let durable = load_db_keys(&state).await.expect("durable keys load");
        assert_eq!(
            durable.get("moonshot").map(String::as_str),
            Some("user-moonshot-key")
        );
        assert_eq!(
            durable.get("zhipu").map(String::as_str),
            Some("user-zhipu-key")
        );
        let mirror_handle = state.llm_provider_keys_handle();
        let mirrored = match mirror_handle.read() {
            Ok(keys) => keys.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        assert_eq!(mirrored, durable);
        let registry = state.providers.load();
        assert_eq!(registry.model_owner("kimi-k3"), Some("moonshot"));
        assert_eq!(registry.model_owner("glm-5.3"), Some("zhipu"));
    }

    #[tokio::test]
    async fn production_handler_rejects_unknown_provider_identity() {
        let state = AppState::for_tests();
        let body = Bytes::from_static(br#"{"keys":{"arbitrary-env-name":"not-a-key"}}"#);
        assert!(put_llm_keys(State(state.clone()), body).await.is_err());
        assert!(
            state
                .db
                .get_config_value(crate::demo_credentials::KEYS_DB_KEY)
                .await
                .expect("runtime DB read succeeds")
                .is_none(),
            "invalid provider names must not be persisted"
        );
    }

    #[tokio::test]
    async fn production_handler_rejects_public_seed_in_any_key_ring_member_when_opted_out() {
        let state = AppState::for_tests();
        let catalog = crate::demo_credentials::load_catalog(&state.config.demo_credentials_path)
            .expect("tracked catalog");
        let public_key = catalog
            .credentials()
            .get("dashscope-token-plan")
            .expect("public key")
            .clone();
        let cases = [
            public_key.clone(),
            format!("{public_key},"),
            format!("{public_key}, {public_key}"),
            format!("user-owned-key, , {public_key}"),
            format!(" , {public_key},   "),
        ];
        for value in cases {
            let body = Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "keys": {"dashscope-token-plan": value}
                }))
                .expect("request JSON"),
            );

            let error = put_llm_keys(State(state.clone()), body)
                .await
                .expect_err("opted-out source checkout rejects every public key-ring member");
            assert_eq!(error.status, axum::http::StatusCode::BAD_REQUEST);
            assert_eq!(error.code, "INVALID_REQUEST");
        }
        let relabelled = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "keys": {"dashscope": public_key}
            }))
            .expect("request JSON"),
        );
        let error = put_llm_keys(State(state.clone()), relabelled)
            .await
            .expect_err("a public key cannot bypass opt-out under another provider label");
        assert_eq!(error.status, axum::http::StatusCode::BAD_REQUEST);
        assert!(
            state
                .db
                .get_config_value(crate::demo_credentials::KEYS_DB_KEY)
                .await
                .expect("runtime DB read succeeds")
                .is_none()
        );
    }

    #[tokio::test]
    async fn explicit_user_update_clears_public_demo_provenance() {
        let mut config = crate::config::Config::test_config();
        config.demo_credential_allowed = true;
        let state = AppState::new(
            zk_db::Db::open_in_memory().expect("runtime SQLite opens"),
            config,
        );
        let catalog = crate::demo_credentials::load_catalog(&state.config.demo_credentials_path)
            .expect("tracked catalog");
        let public_key = catalog
            .credentials()
            .get("dashscope-token-plan")
            .expect("public key")
            .clone();
        let marker = catalog
            .marker_for("dashscope-token-plan", &public_key)
            .expect("public marker");
        let keys = BTreeMap::from([("dashscope-token-plan".to_owned(), public_key)]);
        let provenance = crate::demo_credentials::ProvenanceMap::from([(
            "dashscope-token-plan".to_owned(),
            marker,
        )]);
        persist_db_key_state(&state, &keys, &provenance)
            .await
            .expect("seed pair persists");

        let _ = put_llm_keys(
            State(state.clone()),
            Bytes::from_static(
                br#"{"keys":{"dashscope-token-plan":"user-replacement-key-123456"}}"#,
            ),
        )
        .await
        .expect("user replacement succeeds while demo opt-in is enabled");

        let stored_provenance = load_db_provenance(&state)
            .await
            .expect("provenance read succeeds");
        assert!(stored_provenance.is_empty());
        let stored_keys = load_db_keys(&state).await.expect("keys read succeeds");
        assert_eq!(
            stored_keys.get("dashscope-token-plan").map(String::as_str),
            Some("user-replacement-key-123456")
        );
    }

    #[tokio::test]
    async fn deleting_a_key_also_clears_its_public_demo_provenance() {
        let mut config = crate::config::Config::test_config();
        config.demo_credential_allowed = true;
        let state = AppState::new(
            zk_db::Db::open_in_memory().expect("runtime SQLite opens"),
            config,
        );
        let catalog = crate::demo_credentials::load_catalog(&state.config.demo_credentials_path)
            .expect("tracked catalog");
        let public_key = catalog
            .credentials()
            .get("dashscope-token-plan")
            .expect("public key")
            .clone();
        let marker = catalog
            .marker_for("dashscope-token-plan", &public_key)
            .expect("public marker");
        persist_db_key_state(
            &state,
            &BTreeMap::from([("dashscope-token-plan".to_owned(), public_key)]),
            &crate::demo_credentials::ProvenanceMap::from([(
                "dashscope-token-plan".to_owned(),
                marker,
            )]),
        )
        .await
        .expect("seed pair persists");

        let _ = put_llm_keys(
            State(state.clone()),
            Bytes::from_static(br#"{"keys":{"dashscope-token-plan":""}}"#),
        )
        .await
        .expect("deletion succeeds");

        assert!(
            load_db_keys(&state)
                .await
                .expect("keys read succeeds")
                .is_empty()
        );
        assert!(
            load_db_provenance(&state)
                .await
                .expect("provenance read succeeds")
                .is_empty()
        );
    }
}
