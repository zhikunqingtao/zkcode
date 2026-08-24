//! MCP 服务器配置模型与多来源配置解析。
//!
//! 对照 Java 基线：
//! - [`McpServerConfig`] ← `mcp/McpServerConfig.java`（44 行 record + 三个工厂）；
//! - [`McpTransportType`] ← `mcp/McpTransportType.java`（8 变体）；
//! - [`McpConfigScope`] ← `mcp/McpConfigScope.java`（7 变体）；
//! - [`McpConfigurationResolver`] ← `mcp/McpConfigurationResolver.java`（248 行）。
//!
//! 偏离（均已在任务判据内）：
//! - 配置文件路径改走 zk-core 单一事实源：USER = `~/.zk/mcp.json`、
//!   LOCAL = `{workingDir}/.zk/mcp.json`（Java 为
//!   `~/.config/ai-code-assistant/mcp.json` 与 `{workingDir}/.ai-code-assistant/
//!   mcp.json`）——整仓配置目录已统一为 `.zk/`（Step 0-1）；
//! - ENTERPRISE（`/etc/ai-code-assistant/mcp.json`）来源不实现（任务不做项）；
//!   `ENTERPRISE` 枚举变体仍完整保留，供未来接入与 registry 回显；
//! - `loadFromZhikunAI` / `loadFromManaged` 是 Java 侧的空壳方法（恒返回空
//!   列表、`@SuppressWarnings("unused")` 无调用点），不移植；
//! - ENV 来源的取值路径为 `std::env::var("MCP_SERVERS")`（Java 先查 Spring
//!   `Environment` 再回落 `System.getenv`，Rust 侧无 Spring 属性体系）；
//! - `env` / `headers` 以 `BTreeMap` 承载（Java `LinkedHashMap` 保文件序）：
//!   进程环境与 HTTP 头均与顺序无关，键序确定反而让快照测试稳定。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// MCP 配置文件名（位于 `.zk/` 下）。
pub const MCP_CONFIG_FILE: &str = "mcp.json";

/// 环境变量来源的键名（JSON 字符串，结构同 `mcp.json`）。
pub const ENV_VAR_MCP_SERVERS: &str = "MCP_SERVERS";

/// MCP 传输类型 — 8 种，本 crate 实现 STDIO / SSE / HTTP / WS 四种。
///
/// `SSE_IDE` 复用 SSE，`WS_IDE` 复用 WS；`SDK` / `ZHIKUN_AI_PROXY` 无对应
/// 传输实现，连接时按容错分支处理。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum McpTransportType {
    /// 标准进程 I/O。
    Stdio,
    /// Server-Sent Events。
    Sse,
    /// IDE 内置 SSE。
    SseIde,
    /// Streamable HTTP。
    Http,
    /// WebSocket。
    Ws,
    /// IDE 内置 WebSocket。
    WsIde,
    /// SDK 内嵌。
    Sdk,
    /// AI 平台代理。
    ZhikunAiProxy,
}

impl McpTransportType {
    /// Java 枚举常量名（日志 / 注册表 / REST 回显用）。
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Stdio => "STDIO",
            Self::Sse => "SSE",
            Self::SseIde => "SSE_IDE",
            Self::Http => "HTTP",
            Self::Ws => "WS",
            Self::WsIde => "WS_IDE",
            Self::Sdk => "SDK",
            Self::ZhikunAiProxy => "ZHIKUN_AI_PROXY",
        }
    }
}

impl std::fmt::Display for McpTransportType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// MCP 配置作用域 — 7 级（注释中的「优先级」逐字取自 Java）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum McpConfigScope {
    /// 项目本地（`.zk/mcp.json`）— 优先级 1（最高）。
    Local,
    /// 用户级（`~/.zk/mcp.json`）— 优先级 2。
    User,
    /// 项目配置 — 优先级 3。
    Project,
    /// 运行时动态注册 — 优先级 7（最低）。
    Dynamic,
    /// 企业配置（MDM 推送）— 优先级 4。
    Enterprise,
    /// AI 平台托管 — 优先级 5。
    ZhikunAi,
    /// 受管理配置（Marketplace）— 优先级 6。
    Managed,
}

impl McpConfigScope {
    /// Java 枚举常量名（授权来源串 `"{scope}_RUNTIME"` 等处使用）。
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Local => "LOCAL",
            Self::User => "USER",
            Self::Project => "PROJECT",
            Self::Dynamic => "DYNAMIC",
            Self::Enterprise => "ENTERPRISE",
            Self::ZhikunAi => "ZHIKUN_AI",
            Self::Managed => "MANAGED",
        }
    }
}

impl std::fmt::Display for McpConfigScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// MCP 服务器配置 — 定义一个 MCP 服务器的连接参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpServerConfig {
    /// 服务器名称（唯一标识）。
    pub name: String,
    /// 传输类型（Java 字段名 `type`，Rust 关键字冲突故改名，序列化仍为 `type`）。
    #[serde(rename = "type")]
    pub transport: McpTransportType,
    /// stdio：启动命令。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// stdio：命令参数。
    #[serde(default)]
    pub args: Vec<String>,
    /// 环境变量（stdio 子进程）。
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// SSE / HTTP / WS：服务 URL。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// HTTP 头（SSE / HTTP 传输）。
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// 配置作用域。
    pub scope: McpConfigScope,
}

impl McpServerConfig {
    /// stdio 类型构建（scope = `LOCAL`，对照 Java `McpServerConfig.stdio`）。
    #[must_use]
    pub fn stdio(name: impl Into<String>, command: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            name: name.into(),
            transport: McpTransportType::Stdio,
            command: Some(command.into()),
            args,
            env: BTreeMap::new(),
            url: None,
            headers: BTreeMap::new(),
            scope: McpConfigScope::Local,
        }
    }

    /// SSE 类型构建（scope = `USER`，对照 Java `McpServerConfig.sse`）。
    #[must_use]
    pub fn sse(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            transport: McpTransportType::Sse,
            command: None,
            args: Vec::new(),
            env: BTreeMap::new(),
            url: Some(url.into()),
            headers: BTreeMap::new(),
            scope: McpConfigScope::User,
        }
    }

    /// Streamable HTTP 类型构建（scope = `USER`，对照 Java
    /// `McpServerConfig.http`）。
    #[must_use]
    pub fn http(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            transport: McpTransportType::Http,
            command: None,
            args: Vec::new(),
            env: BTreeMap::new(),
            url: Some(url.into()),
            headers: BTreeMap::new(),
            scope: McpConfigScope::User,
        }
    }
}

/// MCP 配置解析器 — 多来源配置的统一加载与合并。
///
/// 优先级从低到高：ENV（`MCP_SERVERS`，scope = `DYNAMIC`）→ USER
/// （`~/.zk/mcp.json`）→ LOCAL（`{workingDir}/.zk/mcp.json`，最高）。
/// 后加载的同名服务器覆盖先加载的，且**保持首次出现的位置**（对照 Java
/// `LinkedHashMap.put` 的语义）。
#[derive(Debug, Clone, Default)]
pub struct McpConfigurationResolver {
    working_dir: Option<PathBuf>,
}

impl McpConfigurationResolver {
    /// 以工作目录（项目根）构造；`None` 表示无项目上下文，跳过 LOCAL 来源
    /// （对照 Java `app.working-dir` 为空时 `getProjectMcpPath()` 返回 null）。
    #[must_use]
    pub fn new(working_dir: Option<PathBuf>) -> Self {
        Self { working_dir }
    }

    /// 用户级配置文件路径（`~/.zk/mcp.json`）。
    #[must_use]
    pub fn user_mcp_path() -> PathBuf {
        zk_core::paths::user_config_dir().join(MCP_CONFIG_FILE)
    }

    /// 项目级配置文件路径（`{workingDir}/.zk/mcp.json`）。
    #[must_use]
    pub fn project_mcp_path(&self) -> Option<PathBuf> {
        self.working_dir
            .as_deref()
            .map(|cwd| zk_core::paths::project_config_dir(cwd).join(MCP_CONFIG_FILE))
    }

    /// 解析并合并所有来源的 MCP 配置。
    #[must_use]
    pub fn resolve_all(&self) -> Vec<McpServerConfig> {
        let mut merged: Vec<McpServerConfig> = Vec::new();
        merge(&mut merged, Self::load_from_environment());
        merge(
            &mut merged,
            load_from_file(McpConfigScope::User, &Self::user_mcp_path()),
        );
        if let Some(path) = self.project_mcp_path() {
            merge(&mut merged, load_from_file(McpConfigScope::Local, &path));
        }
        let sources = {
            let mut scopes: Vec<McpConfigScope> = merged.iter().map(|c| c.scope).collect();
            scopes.sort_by_key(McpConfigScope::as_str);
            scopes.dedup();
            scopes.len()
        };
        tracing::info!(
            servers = merged.len(),
            sources,
            "Resolved MCP server configurations"
        );
        merged
    }

    /// 从环境变量 `MCP_SERVERS` 加载（scope = `DYNAMIC`）。
    #[must_use]
    pub fn load_from_environment() -> Vec<McpServerConfig> {
        let Ok(raw) = std::env::var(ENV_VAR_MCP_SERVERS) else {
            return Vec::new();
        };
        if raw.trim().is_empty() {
            return Vec::new();
        }
        match serde_json::from_str::<Value>(&raw) {
            Ok(root) => {
                let configs = parse_servers(&root, McpConfigScope::Dynamic);
                tracing::debug!(
                    count = configs.len(),
                    "Loaded MCP configs from env var {ENV_VAR_MCP_SERVERS}"
                );
                configs
            }
            Err(error) => {
                tracing::warn!(%error, "Failed to parse {ENV_VAR_MCP_SERVERS} env var");
                Vec::new()
            }
        }
    }
}

/// 同名覆盖但保持首次位置（对照 Java `LinkedHashMap.put`）。
fn merge(merged: &mut Vec<McpServerConfig>, incoming: Vec<McpServerConfig>) {
    for config in incoming {
        if let Some(slot) = merged.iter_mut().find(|c| c.name == config.name) {
            *slot = config;
        } else {
            merged.push(config);
        }
    }
}

/// 从配置文件加载（文件不存在 / 解析失败 → 空列表 + 日志，绝不上抛）。
///
/// 支持两种 JSON 结构：`{"mcpServers": {"name": {...}}}` 与 `{"name": {...}}`
/// （对照 Java `root.has("mcpServers") ? root.get("mcpServers") : root`）。
#[must_use]
pub fn load_from_file(scope: McpConfigScope, path: &Path) -> Vec<McpServerConfig> {
    if !path.exists() {
        tracing::debug!(path = %path.display(), "MCP config file not found");
        return Vec::new();
    }
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "Failed to read MCP config file");
            return Vec::new();
        }
    };
    let root: Value = match serde_json::from_str(&raw) {
        Ok(root) => root,
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "Failed to read MCP config file");
            return Vec::new();
        }
    };
    let servers = root.get("mcpServers").unwrap_or(&root);
    let configs = parse_servers(servers, scope);
    tracing::debug!(
        count = configs.len(),
        path = %path.display(),
        %scope,
        "Loaded MCP configs from file"
    );
    configs
}

/// 解析「服务器名 → 配置」对象；单项失败仅 warn 跳过（对照 Java 逐项
/// try/catch）。
#[must_use]
pub fn parse_servers(servers: &Value, scope: McpConfigScope) -> Vec<McpServerConfig> {
    let Some(map) = servers.as_object() else {
        return Vec::new();
    };
    map.iter()
        .filter_map(|(name, node)| parse_server_node(name, node, scope))
        .collect()
}

/// 解析单个 MCP 服务器 JSON 节点。
///
/// 逐字对照 Java `parseServerNode`：有 `command` → STDIO（读 `args`）；否则有
/// `url` → 按 `type` 字段大写映射 `HTTP` / `WS`，其余（含缺省）→ SSE；两者皆无
/// → warn 并跳过。
#[must_use]
pub fn parse_server_node(
    name: &str,
    node: &Value,
    scope: McpConfigScope,
) -> Option<McpServerConfig> {
    let mut command = None;
    let mut args = Vec::new();
    let mut url = None;
    let transport;

    if let Some(command_node) = node.get("command") {
        transport = McpTransportType::Stdio;
        command = Some(as_text(command_node));
        if let Some(list) = node.get("args").and_then(Value::as_array) {
            args = list.iter().map(as_text).collect();
        }
    } else if let Some(url_node) = node.get("url") {
        let type_str = node
            .get("type")
            .map_or_else(|| "SSE".to_owned(), |n| as_text(n).to_uppercase());
        transport = match type_str.as_str() {
            "HTTP" => McpTransportType::Http,
            "WS" => McpTransportType::Ws,
            _ => McpTransportType::Sse,
        };
        url = Some(as_text(url_node));
    } else {
        tracing::warn!(
            server = name,
            "MCP config has neither 'command' nor 'url', skipping"
        );
        return None;
    }

    Some(McpServerConfig {
        name: name.to_owned(),
        transport,
        command,
        args,
        env: string_map(node.get("env")),
        url,
        headers: string_map(node.get("headers")),
        scope,
    })
}

/// 取 JSON 节点的文本形态（对照 Jackson `asText()`：字符串取原值，其余取
/// JSON 字面量文本，`null` → `"null"`）。
fn as_text(node: &Value) -> String {
    match node {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// 把 `{k: v}` 对象读为字符串映射（值经 [`as_text`] 归一）。
fn string_map(node: Option<&Value>) -> BTreeMap<String, String> {
    node.and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .map(|(key, value)| (key.clone(), as_text(value)))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn factories_match_java_defaults() {
        let stdio = McpServerConfig::stdio("fs", "npx", vec!["-y".to_owned()]);
        assert_eq!(stdio.transport, McpTransportType::Stdio);
        assert_eq!(stdio.scope, McpConfigScope::Local);
        assert_eq!(McpServerConfig::sse("s", "u").scope, McpConfigScope::User);
        assert_eq!(
            McpServerConfig::http("h", "u").transport,
            McpTransportType::Http
        );
    }

    #[test]
    fn transport_and_scope_names_match_java_enum_constants() {
        assert_eq!(McpTransportType::SseIde.as_str(), "SSE_IDE");
        assert_eq!(McpTransportType::ZhikunAiProxy.as_str(), "ZHIKUN_AI_PROXY");
        assert_eq!(McpConfigScope::ZhikunAi.as_str(), "ZHIKUN_AI");
        assert_eq!(
            serde_json::to_value(McpTransportType::Stdio).unwrap(),
            json!("STDIO")
        );
        assert_eq!(
            serde_json::from_value::<McpConfigScope>(json!("DYNAMIC")).unwrap(),
            McpConfigScope::Dynamic
        );
    }

    #[test]
    fn config_serializes_type_field_as_java_name() {
        let json = serde_json::to_value(McpServerConfig::sse("s", "http://x")).unwrap();
        assert_eq!(json["type"], json!("SSE"));
        assert_eq!(json["scope"], json!("USER"));
        assert!(json.get("command").is_none());
    }

    #[test]
    fn parse_stdio_node_reads_command_args_env() {
        let node = json!({
            "command": "npx",
            "args": ["-y", "@modelcontextprotocol/server-filesystem"],
            "env": {"TOKEN": "t", "PORT": 8080}
        });
        let config = parse_server_node("fs", &node, McpConfigScope::Local).expect("parsed");
        assert_eq!(config.transport, McpTransportType::Stdio);
        assert_eq!(config.command.as_deref(), Some("npx"));
        assert_eq!(config.args.len(), 2);
        assert_eq!(config.env.get("TOKEN").map(String::as_str), Some("t"));
        // 非字符串值经 asText 归一为 JSON 字面量文本（对照 Jackson）。
        assert_eq!(config.env.get("PORT").map(String::as_str), Some("8080"));
        assert!(config.url.is_none());
    }

    #[test]
    fn parse_url_node_maps_type_case_insensitively() {
        for (raw, expected) in [
            ("http", McpTransportType::Http),
            ("HTTP", McpTransportType::Http),
            ("ws", McpTransportType::Ws),
            ("sse", McpTransportType::Sse),
            ("bogus", McpTransportType::Sse),
        ] {
            let node = json!({"url": "http://localhost:3000", "type": raw});
            let config = parse_server_node("x", &node, McpConfigScope::User).expect("parsed");
            assert_eq!(config.transport, expected, "type={raw}");
        }
        // 缺省 type → SSE
        let node =
            json!({"url": "http://localhost:3000", "headers": {"Authorization": "Bearer k"}});
        let config = parse_server_node("x", &node, McpConfigScope::User).expect("parsed");
        assert_eq!(config.transport, McpTransportType::Sse);
        assert_eq!(
            config.headers.get("Authorization").map(String::as_str),
            Some("Bearer k")
        );
    }

    #[test]
    fn node_without_command_or_url_is_skipped() {
        assert!(parse_server_node("x", &json!({"foo": 1}), McpConfigScope::Local).is_none());
    }

    #[test]
    fn parse_servers_accepts_both_file_shapes() {
        let wrapped = json!({"mcpServers": {"a": {"command": "c"}}});
        let servers = wrapped.get("mcpServers").unwrap();
        assert_eq!(parse_servers(servers, McpConfigScope::Local).len(), 1);
        let flat = json!({"a": {"command": "c"}, "b": {"url": "u"}, "c": {}});
        assert_eq!(parse_servers(&flat, McpConfigScope::Local).len(), 2);
    }

    #[test]
    fn merge_overrides_by_name_but_keeps_first_position() {
        let mut merged = vec![
            McpServerConfig::sse("a", "u1"),
            McpServerConfig::sse("b", "u2"),
        ];
        merge(
            &mut merged,
            vec![
                McpServerConfig::stdio("a", "cmd", Vec::new()),
                McpServerConfig::sse("c", "u3"),
            ],
        );
        let names: Vec<&str> = merged.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
        assert_eq!(merged[0].transport, McpTransportType::Stdio);
    }

    #[test]
    fn load_from_file_is_tolerant_to_missing_and_invalid_files() {
        let dir = std::env::temp_dir().join(format!("zk-mcp-cfg-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let missing = dir.join("nope.json");
        assert!(load_from_file(McpConfigScope::User, &missing).is_empty());

        let broken = dir.join("broken.json");
        std::fs::write(&broken, "{not json").expect("write");
        assert!(load_from_file(McpConfigScope::User, &broken).is_empty());

        let good = dir.join("mcp.json");
        std::fs::write(&good, r#"{"mcpServers":{"fs":{"command":"npx"}}}"#).expect("write");
        let configs = load_from_file(McpConfigScope::User, &good);
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].scope, McpConfigScope::User);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn project_path_is_none_without_working_dir() {
        assert!(
            McpConfigurationResolver::new(None)
                .project_mcp_path()
                .is_none()
        );
        let resolver = McpConfigurationResolver::new(Some(PathBuf::from("/tmp/proj")));
        assert_eq!(
            resolver.project_mcp_path().unwrap(),
            PathBuf::from("/tmp/proj/.zk/mcp.json")
        );
        assert!(
            McpConfigurationResolver::user_mcp_path()
                .to_string_lossy()
                .ends_with("/.zk/mcp.json")
        );
    }
}
