//! MCP 能力注册表 — `mcp_capability_registry.json` 的加载、查询、修改与持久化。
//!
//! 逐字对照 Java 基线 `mcp/McpCapabilityRegistryService.java`（184 行）与
//! `mcp/McpCapabilityDefinition.java`（58 行）：
//! - 启动加载：文件不存在 → warn 后留空；`mcp_tools` 非数组 → warn 后留空；
//!   逐条 `treeToValue`，单条解析失败仅 warn（不影响其余条目），`id` 为空跳过；
//! - 查询：`listAll` / `listByDomain` / `listEnabled` / `findById` /
//!   `findByToolName(serverKey, toolName)` / `listDomains`（去重 + 升序）/
//!   `size` / `enabledCount`；
//! - 修改：`toggleEnabled` / `updateCapability` / `addCapability` /
//!   `deleteCapability`，前三者「不存在 / 已存在」抛
//!   `IllegalArgumentException`（此处映射为 [`RegistryError`]）；
//! - 持久化：每次修改触发 500ms 防抖（新任务取消旧任务），落盘时**保留**原文件
//!   除 `mcp_tools` / `lastUpdated` 外的所有字段（`description`、
//!   `usage_guide`、`_schema_version` 等），条目按 `id` 升序，缩进输出；
//! - `extractServerKey()`：取 `sseUrl` path 的倒数第二段，异常/缺失回落 `id`。
//!
//! 形态差异（Rust 侧必然）：
//! - Jackson `@JsonInclude(NON_NULL)` → 全部可选字段 `Option<_>` +
//!   `skip_serializing_if`；`@JsonIgnoreProperties(ignoreUnknown = true)` 是
//!   serde 默认行为；
//! - Java 的 `ConcurrentHashMap` 遍历顺序不定（落盘前显式按 `id` 排序）；此处
//!   直接用 `BTreeMap<String, _>`，查询与落盘天然按 `id` 升序；
//! - `ScheduledExecutorService` + `ScheduledFuture.cancel` → 单槽
//!   [`CancellationToken`] + `tokio::spawn`；无 tokio 运行时（同步上下文/单元
//!   测试）时退化为同步落盘（Java 恒有虚拟线程调度器可用）；
//! - `ReentrantLock saveLock` → [`Mutex`]（仅保护「读原文件 → 合并 → 写回」这
//!   一临界区）。
//!
//! 行为偏离（在 `docs/compatibility.md` 留痕）：
//! 1. **原子落盘**：Java `objectMapper.writeValue(path.toFile(), root)` 直接截断
//!    覆写，进程在写入中途被杀会留下半截 JSON（下次启动整表丢失）。此处写临时
//!    文件 + `rename` 原子替换（与 zkcode 既有「写入原子性」审查结论一致）。
//! 2. **`lastUpdated` 取 UTC 日期**：Java `LocalDate.now()` 是 JVM 默认时区；
//!    Rust 标准库无时区数据库（引 chrono 仅为一个日期串不值当），故写 UTC
//!    `YYYY-MM-DD`。该字段仅为人读元信息，不参与任何逻辑。
//! 3. **`apiKeyConfig` 解析走环境变量**：Java 经 Spring `Environment` 读
//!    `application.yml` 属性（如 `dashscope.api-key`）；zkcode 无 Spring 属性
//!    体系，故按 Spring relaxed-binding 规则把属性名转为环境变量名
//!    （`dashscope.api-key` → `DASHSCOPE_API_KEY`）后读 `std::env::var`，
//!    并对 `apiKeyDefault` 中的 `${VAR}` 占位做一次环境展开（Java 侧该展开由
//!    Spring 的属性占位符解析器完成，Jackson 读出的原文若不展开会把
//!    `${LLM_API_KEY}` 字面量当作 API Key 发出去）。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use crate::sse::{lock, read_lock, write_lock};

/// 能力注册表默认路径（对照 Java `capability-registry-path` 的默认值
/// `configuration/mcp/mcp_capability_registry.json`）。
pub const DEFAULT_REGISTRY_PATH: &str = "configuration/mcp/mcp_capability_registry.json";

/// 注册表路径的环境变量覆盖（对照 Java `${MCP_REGISTRY_PATH:...}`）。
pub const ENV_VAR_REGISTRY_PATH: &str = "MCP_REGISTRY_PATH";

/// 落盘防抖窗口（对照 Java `saveScheduler.schedule(this::saveToFile, 500, MS)`）。
const SAVE_DEBOUNCE: Duration = Duration::from_millis(500);

/// 新建文件时写入的 schema 版本（对照 Java `root.put("_schema_version", "1.0")`）。
const SCHEMA_VERSION: &str = "1.0";

/// 注册表修改类操作的错误 — 对照 Java 抛出的 `IllegalArgumentException`。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    /// 目标能力不存在（`toggleEnabled` / `updateCapability`）。
    #[error("MCP capability not found: {0}")]
    NotFound(String),
    /// 能力 id 已存在（`addCapability`）。
    #[error("Already exists: {0}")]
    AlreadyExists(String),
}

/// MCP 能力定义 — `mcp_capability_registry.json` 中 `mcp_tools[]` 的单条。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpCapabilityDefinition {
    /// 能力唯一标识（建议 `mcp_` 前缀）。
    pub id: String,
    /// 显示名称。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// MCP 服务器上的原始工具名。
    #[serde(default, rename = "toolName", skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    /// MCP 服务器的 SSE 端点 URL。
    #[serde(default, rename = "sseUrl", skip_serializing_if = "Option::is_none")]
    pub sse_url: Option<String>,
    /// API Key 的配置属性名。
    #[serde(
        default,
        rename = "apiKeyConfig",
        skip_serializing_if = "Option::is_none"
    )]
    pub api_key_config: Option<String>,
    /// API Key 的默认值（配置属性缺失时使用）。
    #[serde(
        default,
        rename = "apiKeyDefault",
        skip_serializing_if = "Option::is_none"
    )]
    pub api_key_default: Option<String>,
    /// 功能域分类。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    /// 分类（固定 `MCP_TOOL`）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    /// 简要描述（供 AI 任务匹配）。
    #[serde(
        default,
        rename = "briefDescription",
        skip_serializing_if = "Option::is_none"
    )]
    pub brief_description: Option<String>,
    /// 精简描述（视频通话等场景的意图识别）。
    #[serde(
        default,
        rename = "videoCallSummary",
        skip_serializing_if = "Option::is_none"
    )]
    pub video_call_summary: Option<String>,
    /// 完整描述（覆盖 MCP 服务器自报的工具描述）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 输入参数 Schema。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
    /// 输出参数 Schema。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// 调用超时（毫秒），`0` 表示沿用默认。
    #[serde(default, rename = "timeoutMs")]
    pub timeout_ms: i64,
    /// 是否启用。
    #[serde(default)]
    pub enabled: bool,
    /// 视频通话场景是否启用。
    #[serde(default, rename = "videoCallEnabled")]
    pub video_call_enabled: bool,
}

impl McpCapabilityDefinition {
    /// 最小构造（仅 id，其余留空）— 供测试与增量构建使用。
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: None,
            tool_name: None,
            sse_url: None,
            api_key_config: None,
            api_key_default: None,
            domain: None,
            category: None,
            brief_description: None,
            video_call_summary: None,
            description: None,
            input: None,
            output: None,
            timeout_ms: 0,
            enabled: false,
            video_call_enabled: false,
        }
    }

    /// 快捷构造：切换 `enabled` 状态（对照 Java `withEnabled`）。
    #[must_use]
    pub fn with_enabled(&self, enabled: bool) -> Self {
        Self {
            enabled,
            ..self.clone()
        }
    }

    /// 从 `sseUrl` 提取服务器 key（URL path 倒数第二段），缺失/异常回落 `id`。
    ///
    /// 对照 Java `extractServerKey()`：`URI.create(sseUrl).getPath()` 按 `/`
    /// 切分、丢空段，段数 ≥2 取倒数第二段。此处不引 URL 解析库——Java 取的仅是
    /// path，故先剥离 `scheme://authority`（首个 `://` 之后的第一个 `/` 起）与
    /// 尾部 `?query` / `#fragment`，再按同规则切分。
    #[must_use]
    pub fn extract_server_key(&self) -> String {
        let Some(url) = self.sse_url.as_deref() else {
            return self.id.clone();
        };
        let path = url_path(url);
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if segments.len() >= 2 {
            segments[segments.len() - 2].to_owned()
        } else {
            self.id.clone()
        }
    }

    /// 解析该能力的 API Key（模块级偏离 3）。
    ///
    /// 顺序对照 Java `buildConfigFromRegistry`：先取 `apiKeyConfig` 指向的配置
    /// 值，为空则回落 `apiKeyDefault`。
    #[must_use]
    pub fn resolve_api_key(&self) -> Option<String> {
        let from_config = self
            .api_key_config
            .as_deref()
            .and_then(property_from_env)
            .filter(|value| !value.is_empty());
        if from_config.is_some() {
            return from_config;
        }
        self.api_key_default
            .as_deref()
            .map(expand_env_placeholders)
            .filter(|value| !value.is_empty())
    }
}

/// 剥离 URL 的 scheme/authority 与 query/fragment，返回 path 部分。
fn url_path(url: &str) -> &str {
    let after_authority = match url.find("://") {
        Some(index) => {
            let rest = &url[index + 3..];
            match rest.find('/') {
                Some(slash) => &rest[slash..],
                None => "",
            }
        }
        None => url,
    };
    let end = after_authority
        .find(['?', '#'])
        .unwrap_or(after_authority.len());
    &after_authority[..end]
}

/// Spring 属性名 → 环境变量名（relaxed binding：`.`/`-` → `_`，大写）。
fn property_from_env(property: &str) -> Option<String> {
    let key: String = property
        .chars()
        .map(|c| match c {
            '.' | '-' => '_',
            other => other.to_ascii_uppercase(),
        })
        .collect();
    std::env::var(key).ok()
}

/// 展开 `${VAR}` 占位符（未定义的变量原样保留，与 Spring 无默认值时的行为一致）。
fn expand_env_placeholders(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let tail = &rest[start + 2..];
        let Some(end) = tail.find('}') else {
            out.push_str(&rest[start..]);
            return out;
        };
        let name = &tail[..end];
        if let Ok(value) = std::env::var(name) {
            out.push_str(&value);
        } else {
            out.push_str("${");
            out.push_str(name);
            out.push('}');
        }
        rest = &tail[end + 1..];
    }
    out.push_str(rest);
    out
}

/// MCP 能力注册表 — 内存表 + 防抖落盘。
#[derive(Debug)]
pub struct McpCapabilityRegistry {
    path: PathBuf,
    capabilities: RwLock<BTreeMap<String, McpCapabilityDefinition>>,
    save_lock: Mutex<()>,
    pending_save: Mutex<Option<CancellationToken>>,
}

impl McpCapabilityRegistry {
    /// 按显式路径创建注册表（不加载文件）。
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            capabilities: RwLock::new(BTreeMap::new()),
            save_lock: Mutex::new(()),
            pending_save: Mutex::new(None),
        }
    }

    /// 按默认路径规则创建并加载（`MCP_REGISTRY_PATH` → 默认相对路径）。
    ///
    /// 对照 Java `@PostConstruct loadRegistry()`：文件缺失仅 warn，服务照常起。
    #[must_use]
    pub fn load_default() -> Self {
        let path = std::env::var(ENV_VAR_REGISTRY_PATH)
            .map_or_else(|_| PathBuf::from(DEFAULT_REGISTRY_PATH), PathBuf::from);
        let registry = Self::new(path);
        registry.load();
        registry
    }

    /// 注册表文件路径。
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 加载注册表文件（对照 Java `loadRegistry()`）。
    pub fn load(&self) {
        let path = &self.path;
        if !path.exists() {
            tracing::warn!(path = %path.display(), "MCP capability registry not found");
            return;
        }
        let root: Value = match std::fs::read_to_string(path)
            .map_err(|error| error.to_string())
            .and_then(|text| serde_json::from_str(&text).map_err(|error| error.to_string()))
        {
            Ok(root) => root,
            Err(error) => {
                tracing::error!(
                    path = %path.display(),
                    %error,
                    "Failed to load MCP capability registry"
                );
                return;
            }
        };
        let Some(entries) = root.get("mcp_tools").and_then(Value::as_array) else {
            tracing::warn!("MCP capability registry has no 'mcp_tools' array");
            return;
        };
        let mut loaded = BTreeMap::new();
        for node in entries {
            match serde_json::from_value::<McpCapabilityDefinition>(node.clone()) {
                Ok(definition) if !definition.id.is_empty() => {
                    loaded.insert(definition.id.clone(), definition);
                }
                // Java: `def.id() != null` 才入表（id 缺失时静默跳过）。
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%error, "Failed to parse MCP capability");
                }
            }
        }
        let count = loaded.len();
        *write_lock(&self.capabilities) = loaded;
        tracing::info!(
            count,
            path = %path.display(),
            "MCP capability registry loaded"
        );
    }

    // ===== 查询接口 =====

    /// 全部能力（按 id 升序）。
    #[must_use]
    pub fn list_all(&self) -> Vec<McpCapabilityDefinition> {
        read_lock(&self.capabilities).values().cloned().collect()
    }

    /// 按功能域筛选。
    #[must_use]
    pub fn list_by_domain(&self, domain: &str) -> Vec<McpCapabilityDefinition> {
        read_lock(&self.capabilities)
            .values()
            .filter(|definition| definition.domain.as_deref() == Some(domain))
            .cloned()
            .collect()
    }

    /// 已启用的能力。
    #[must_use]
    pub fn list_enabled(&self) -> Vec<McpCapabilityDefinition> {
        read_lock(&self.capabilities)
            .values()
            .filter(|definition| definition.enabled)
            .cloned()
            .collect()
    }

    /// 按 id 查找。
    #[must_use]
    pub fn find_by_id(&self, id: &str) -> Option<McpCapabilityDefinition> {
        read_lock(&self.capabilities).get(id).cloned()
    }

    /// 按「服务器 key + 原始工具名」查找（对照 Java `findByToolName`）。
    #[must_use]
    pub fn find_by_tool_name(
        &self,
        server_key: &str,
        tool_name: &str,
    ) -> Option<McpCapabilityDefinition> {
        read_lock(&self.capabilities)
            .values()
            .find(|definition| {
                definition.tool_name.as_deref() == Some(tool_name)
                    && definition.extract_server_key() == server_key
            })
            .cloned()
    }

    /// 全部功能域（去重 + 升序）。
    #[must_use]
    pub fn list_domains(&self) -> Vec<String> {
        let mut domains: Vec<String> = read_lock(&self.capabilities)
            .values()
            .filter_map(|definition| definition.domain.clone())
            .collect();
        domains.sort_unstable();
        domains.dedup();
        domains
    }

    /// 条目总数。
    #[must_use]
    pub fn size(&self) -> usize {
        read_lock(&self.capabilities).len()
    }

    /// 已启用条目数。
    #[must_use]
    pub fn enabled_count(&self) -> usize {
        read_lock(&self.capabilities)
            .values()
            .filter(|definition| definition.enabled)
            .count()
    }

    // ===== 修改接口 =====

    /// 切换启用状态（对照 Java `toggleEnabled`）。
    ///
    /// # Errors
    /// [`RegistryError::NotFound`] — `id` 不在注册表中（对照 Java 抛
    /// `IllegalArgumentException("MCP capability not found: ...")`）。
    pub fn toggle_enabled(
        &self,
        id: &str,
        enabled: bool,
    ) -> Result<McpCapabilityDefinition, RegistryError> {
        let updated = {
            let mut guard = write_lock(&self.capabilities);
            let existing = guard
                .get(id)
                .ok_or_else(|| RegistryError::NotFound(id.to_owned()))?;
            let updated = existing.with_enabled(enabled);
            guard.insert(id.to_owned(), updated.clone());
            updated
        };
        self.save_async();
        tracing::info!(id, enabled, "MCP capability toggled");
        Ok(updated)
    }

    /// 整条替换（对照 Java `updateCapability`；沿用 URL 中的 id 作为键）。
    ///
    /// # Errors
    /// [`RegistryError::NotFound`] — `id` 不在注册表中。
    pub fn update_capability(
        &self,
        id: &str,
        updated: McpCapabilityDefinition,
    ) -> Result<McpCapabilityDefinition, RegistryError> {
        {
            let mut guard = write_lock(&self.capabilities);
            if !guard.contains_key(id) {
                return Err(RegistryError::NotFound(id.to_owned()));
            }
            guard.insert(id.to_owned(), updated.clone());
        }
        self.save_async();
        Ok(updated)
    }

    /// 新增能力（对照 Java `addCapability`）。
    ///
    /// # Errors
    /// [`RegistryError::AlreadyExists`] — 同名 id 已存在。
    pub fn add_capability(
        &self,
        definition: McpCapabilityDefinition,
    ) -> Result<McpCapabilityDefinition, RegistryError> {
        {
            let mut guard = write_lock(&self.capabilities);
            if guard.contains_key(&definition.id) {
                return Err(RegistryError::AlreadyExists(definition.id.clone()));
            }
            guard.insert(definition.id.clone(), definition.clone());
        }
        self.save_async();
        Ok(definition)
    }

    /// 删除能力（对照 Java `deleteCapability`，不存在返回 `false`）。
    pub fn delete_capability(&self, id: &str) -> bool {
        let removed = write_lock(&self.capabilities).remove(id).is_some();
        if removed {
            self.save_async();
        }
        removed
    }

    // ===== 持久化（防抖 + 互斥）=====

    /// 触发防抖落盘（500ms 内的重复修改合并为一次写盘）。
    ///
    /// 无 tokio 运行时时同步落盘（见模块文档「形态差异」）。
    pub fn save_async(&self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.save();
            return;
        };
        let token = CancellationToken::new();
        let previous = lock(&self.pending_save).replace(token.clone());
        if let Some(previous) = previous {
            previous.cancel();
        }
        let snapshot = self.snapshot_for_save();
        let path = self.path.clone();
        handle.spawn(async move {
            tokio::select! {
                () = token.cancelled() => {}
                () = tokio::time::sleep(SAVE_DEBOUNCE) => {
                    if let Err(error) = write_registry(&path, &snapshot) {
                        tracing::error!(%error, "Failed to save MCP capability registry");
                    } else {
                        tracing::debug!(path = %path.display(), "MCP capability registry saved");
                    }
                }
            }
        });
    }

    /// 立即落盘（对照 Java `saveToFile()`）。
    pub fn save(&self) {
        let snapshot = self.snapshot_for_save();
        let _guard = lock(&self.save_lock);
        if let Err(error) = write_registry(&self.path, &snapshot) {
            tracing::error!(%error, "Failed to save MCP capability registry");
        } else {
            tracing::debug!(path = %self.path.display(), "MCP capability registry saved");
        }
    }

    fn snapshot_for_save(&self) -> Vec<McpCapabilityDefinition> {
        read_lock(&self.capabilities).values().cloned().collect()
    }
}

/// 合并写回：保留原文件其余字段，仅替换 `mcp_tools` 与 `lastUpdated`。
fn write_registry(path: &Path, definitions: &[McpCapabilityDefinition]) -> std::io::Result<()> {
    let mut root = std::fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
        .and_then(|value| match value {
            Value::Object(map) => Some(map),
            _ => None,
        })
        .unwrap_or_else(|| {
            let mut fresh = Map::new();
            fresh.insert("_schema_version".to_owned(), Value::from(SCHEMA_VERSION));
            fresh
        });
    let tools = definitions
        .iter()
        .map(|definition| serde_json::to_value(definition).unwrap_or(Value::Null))
        .collect::<Vec<_>>();
    root.insert("mcp_tools".to_owned(), Value::Array(tools));
    root.insert("lastUpdated".to_owned(), Value::from(today_utc()));
    let body = serde_json::to_string_pretty(&Value::Object(root))
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    // 模块级偏离 1：临时文件 + rename，避免半截 JSON 毁表。
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, body)?;
    std::fs::rename(&temporary, path)
}

/// 当天 UTC 日期 `YYYY-MM-DD`（模块级偏离 2）。
fn today_utc() -> String {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)
        });
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    format!("{year:04}-{month:02}-{day:02}")
}

/// 自 1970-01-01 的天数 → civil 日期（Howard Hinnant `civil_from_days`）。
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// 中毒降级取锁的策略与 crate::sse 一致（lock / read_lock / write_lock 均复用
// 该模块的实现，避免同一降级语义在本 crate 内出现两份）。
#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> McpCapabilityDefinition {
        McpCapabilityDefinition {
            tool_name: Some("modelstudio_image_edit_wan25".to_owned()),
            sse_url: Some("https://dashscope.aliyuncs.com/api/v1/mcps/Wan25Media/sse".to_owned()),
            domain: Some("image_processing".to_owned()),
            timeout_ms: 120_000,
            enabled: true,
            ..McpCapabilityDefinition::new("mcp_wan25_image_edit")
        }
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let unique = uuid::Uuid::new_v4();
        let dir = std::env::temp_dir().join(format!("zk-mcp-registry-{tag}-{unique}"));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn extracts_server_key_from_second_last_path_segment() {
        assert_eq!(sample().extract_server_key(), "Wan25Media");
    }

    #[test]
    fn falls_back_to_id_when_path_too_short() {
        let short = McpCapabilityDefinition {
            sse_url: Some("https://example.com/sse".to_owned()),
            ..McpCapabilityDefinition::new("cap-1")
        };
        assert_eq!(short.extract_server_key(), "cap-1");
        let missing = McpCapabilityDefinition::new("cap-2");
        assert_eq!(missing.extract_server_key(), "cap-2");
    }

    #[test]
    fn ignores_query_and_fragment_when_extracting_key() {
        let definition = McpCapabilityDefinition {
            sse_url: Some("https://host/api/mcps/Search/sse?key=1#frag".to_owned()),
            ..McpCapabilityDefinition::new("cap-3")
        };
        assert_eq!(definition.extract_server_key(), "Search");
    }

    #[test]
    fn loads_registry_and_skips_unparsable_entries() {
        let dir = temp_dir("load");
        let path = dir.join("registry.json");
        std::fs::write(
            &path,
            r#"{
              "_schema_version": "1.0",
              "description": "keep me",
              "mcp_tools": [
                {"id": "a", "toolName": "t1", "enabled": true, "domain": "d1"},
                {"id": "b", "toolName": "t2", "enabled": false, "domain": "d2"},
                {"toolName": "no-id"},
                {"id": "c", "timeoutMs": "not-a-number"}
              ]
            }"#,
        )
        .expect("write");

        let registry = McpCapabilityRegistry::new(&path);
        registry.load();

        assert_eq!(registry.size(), 2);
        assert_eq!(registry.enabled_count(), 1);
        assert_eq!(
            registry.list_domains(),
            vec!["d1".to_owned(), "d2".to_owned()]
        );
        assert_eq!(registry.list_by_domain("d1").len(), 1);
        assert!(registry.find_by_id("a").is_some());
        assert!(registry.find_by_id("c").is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_file_leaves_registry_empty() {
        let registry = McpCapabilityRegistry::new("/nonexistent/zk-mcp/registry.json");
        registry.load();
        assert_eq!(registry.size(), 0);
    }

    #[test]
    fn registry_without_tools_array_is_tolerated() {
        let dir = temp_dir("no-array");
        let path = dir.join("registry.json");
        std::fs::write(&path, r#"{"mcp_tools": {"not": "array"}}"#).expect("write");
        let registry = McpCapabilityRegistry::new(&path);
        registry.load();
        assert_eq!(registry.size(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn find_by_tool_name_matches_server_key_and_tool() {
        let dir = temp_dir("find-by-tool-name");
        let registry = McpCapabilityRegistry::new(dir.join("registry.json"));
        registry.add_capability(sample()).expect("add");
        assert!(
            registry
                .find_by_tool_name("Wan25Media", "modelstudio_image_edit_wan25")
                .is_some()
        );
        assert!(
            registry
                .find_by_tool_name("other", "modelstudio_image_edit_wan25")
                .is_none()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn crud_rejects_conflicting_operations() {
        let dir = temp_dir("crud");
        let path = dir.join("registry.json");
        let registry = McpCapabilityRegistry::new(&path);

        registry.add_capability(sample()).expect("add");
        assert_eq!(
            registry.add_capability(sample()),
            Err(RegistryError::AlreadyExists(
                "mcp_wan25_image_edit".to_owned()
            ))
        );
        assert_eq!(
            registry.toggle_enabled("missing", true),
            Err(RegistryError::NotFound("missing".to_owned()))
        );
        assert_eq!(
            registry.update_capability("missing", sample()),
            Err(RegistryError::NotFound("missing".to_owned()))
        );

        let toggled = registry
            .toggle_enabled("mcp_wan25_image_edit", false)
            .expect("toggle");
        assert!(!toggled.enabled);
        assert_eq!(registry.enabled_count(), 0);

        assert!(registry.delete_capability("mcp_wan25_image_edit"));
        assert!(!registry.delete_capability("mcp_wan25_image_edit"));
        assert_eq!(registry.size(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn save_preserves_unknown_root_fields_and_sorts_entries() {
        let dir = temp_dir("save");
        let path = dir.join("registry.json");
        std::fs::write(
            &path,
            r#"{"_schema_version":"1.0","usage_guide":{"id":"doc"},"mcp_tools":[]}"#,
        )
        .expect("write");

        let registry = McpCapabilityRegistry::new(&path);
        registry
            .add_capability(McpCapabilityDefinition::new("zzz"))
            .expect("add");
        registry
            .add_capability(McpCapabilityDefinition::new("aaa"))
            .expect("add");

        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
        assert_eq!(written["usage_guide"]["id"], Value::from("doc"));
        assert_eq!(written["_schema_version"], Value::from("1.0"));
        let ids: Vec<&str> = written["mcp_tools"]
            .as_array()
            .expect("array")
            .iter()
            .map(|node| node["id"].as_str().unwrap_or_default())
            .collect();
        assert_eq!(ids, vec!["aaa", "zzz"]);
        assert_eq!(
            written["lastUpdated"].as_str().map(str::len),
            Some("2026-01-01".len())
        );
        // NON_NULL：未设置的可选字段不写出。
        assert!(written["mcp_tools"][0].get("sseUrl").is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn save_creates_file_with_schema_version_when_absent() {
        let dir = temp_dir("fresh");
        let path = dir.join("nested").join("registry.json");
        let registry = McpCapabilityRegistry::new(&path);
        registry
            .add_capability(McpCapabilityDefinition::new("only"))
            .expect("add");
        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
        assert_eq!(written["_schema_version"], Value::from("1.0"));
        assert_eq!(written["mcp_tools"][0]["id"], Value::from("only"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn async_saves_are_debounced_into_single_write() {
        let dir = temp_dir("debounce");
        let path = dir.join("registry.json");
        let registry = McpCapabilityRegistry::new(&path);

        registry
            .add_capability(McpCapabilityDefinition::new("a"))
            .expect("add");
        registry
            .add_capability(McpCapabilityDefinition::new("b"))
            .expect("add");
        // 防抖窗口内尚未落盘。
        assert!(!path.exists());

        tokio::time::sleep(SAVE_DEBOUNCE + Duration::from_millis(200)).await;
        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
        assert_eq!(written["mcp_tools"].as_array().map(Vec::len), Some(2));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn api_key_prefers_property_then_expands_default_placeholder() {
        // 属性名 → 环境变量名（relaxed binding）。
        let unique = uuid::Uuid::new_v4().simple().to_string();
        let property = format!("zk.mcp-{unique}");
        let env_name = property
            .chars()
            .map(|c| match c {
                '.' | '-' => '_',
                other => other.to_ascii_uppercase(),
            })
            .collect::<String>();
        let definition = McpCapabilityDefinition {
            api_key_config: Some(property),
            api_key_default: Some(format!("${{ZK_MCP_FALLBACK_{unique}}}")),
            ..McpCapabilityDefinition::new("cap")
        };

        // 两者都未定义：占位符原样保留（Java 同样把原文发出去）。
        assert_eq!(
            definition.resolve_api_key(),
            Some(format!("${{ZK_MCP_FALLBACK_{unique}}}"))
        );

        // 仅 default 的环境变量存在 → 展开。
        // SAFETY-free：`std::env::set_var` 在 2024 edition 为 unsafe，故此处不改
        // 进程环境，仅验证展开纯函数。
        assert_eq!(
            expand_env_placeholders("a${__ZK_MCP_UNSET__}b"),
            "a${__ZK_MCP_UNSET__}b"
        );
        assert_eq!(expand_env_placeholders("plain"), "plain");
        assert_eq!(expand_env_placeholders("${unterminated"), "${unterminated");
        assert!(std::env::var(&env_name).is_err());
    }

    #[test]
    fn civil_from_days_matches_golden_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // 与 zk-server `iso.rs` / zk-db `time.rs` 的黄金值互锁：
        // epoch 秒 1_786_792_040 → 2026-08-15（即 20_680 天）。
        assert_eq!(civil_from_days(1_786_792_040 / 86_400), (2026, 8, 15));
    }
}
