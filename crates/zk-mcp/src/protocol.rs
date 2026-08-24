//! MCP 协议信封：方法常量、协议版本、客户端能力声明、四类定义结构，
//! 以及 Roots 边界与进度追踪端口。
//!
//! 对照 Java 基线：
//! - 方法名字符串与握手参数散落在 `McpServerConnection.java`（L121-167、
//!   L250-320）与 `McpStreamableHttpTransport.java`（L80-120）——Rust 侧集中为
//!   常量，消除字符串字面量重复；
//! - [`ToolDefinition`] / [`ResourceDefinition`] / [`PromptDefinition`] /
//!   [`PromptArgument`] 对照 `McpServerConnection` 的四个内部 record
//!   （L561-588）；
//! - [`RootDescriptor`] / [`RootsProvider`] 对照 `mcp/roots/RootDescriptor.java`
//!   （26 行）+ `mcp/roots/McpRootsProvider.java`（50 行）；
//! - [`ProgressTracker`] 对照 `mcp/progress/McpProgressTracker.java` 的对外
//!   契约——具体实现（推 WS 到前端）属 zk-server 职责，本 crate 只留端口
//!   （依赖倒置，Batch 4B 注入）。

use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// MCP 基础协议版本（STDIO / SSE / WS 传输的 `initialize` 声明值）。
pub const PROTOCOL_VERSION: &str = "2024-11-05";
/// MCP Streamable HTTP 规范版本（HTTP 传输自身握手的声明值）。
pub const PROTOCOL_VERSION_STREAMABLE_HTTP: &str = "2025-03-26";

/// 客户端标识名（`initialize.clientInfo.name`）。
///
/// 偏离：Java 为 `"zhikuncode"`——本仓库身份串统一为 `"zkcode"`（与 zk-server
/// 既有 User-Agent / serverInfo 一致），属整仓改名而非行为差异。
pub const CLIENT_NAME: &str = "zkcode";
/// Streamable HTTP 传输自报的客户端标识名（Java 为 `"zhikun-mcp-client"`）。
pub const HTTP_CLIENT_NAME: &str = "zkcode-mcp-client";
/// 客户端版本（`initialize.clientInfo.version`）。
pub const CLIENT_VERSION: &str = "1.0.0";

/// `initialize` — 协议握手请求。
pub const METHOD_INITIALIZE: &str = "initialize";
/// `notifications/initialized` — 握手完成通知。
pub const METHOD_INITIALIZED: &str = "notifications/initialized";
/// `tools/list` — 工具发现。
pub const METHOD_TOOLS_LIST: &str = "tools/list";
/// `tools/call` — 工具调用。
pub const METHOD_TOOLS_CALL: &str = "tools/call";
/// `resources/list` — 资源发现。
pub const METHOD_RESOURCES_LIST: &str = "resources/list";
/// `resources/read` — 资源读取。
pub const METHOD_RESOURCES_READ: &str = "resources/read";
/// `prompts/list` — prompt 模板发现。
pub const METHOD_PROMPTS_LIST: &str = "prompts/list";
/// `prompts/get` — prompt 模板渲染。
pub const METHOD_PROMPTS_GET: &str = "prompts/get";
/// `notifications/progress` — 服务端进度通知（入站）。
pub const METHOD_PROGRESS: &str = "notifications/progress";
/// `notifications/cancelled` — 取消通知（出站）。
pub const METHOD_CANCELLED: &str = "notifications/cancelled";
/// `notifications/ping` — 心跳通知（出站，SSE 健康探测）。
pub const METHOD_PING: &str = "notifications/ping";
/// `roots/list` — 服务端→客户端反向请求（入站，需回响应）。
pub const METHOD_ROOTS_LIST: &str = "roots/list";
/// `notifications/roots/list_changed` — roots 变更通知（出站）。
pub const METHOD_ROOTS_LIST_CHANGED: &str = "notifications/roots/list_changed";

/// 客户端能力声明（`initialize.capabilities`）。
///
/// 对照 Java `performProtocolHandshake` L130-135：`tools` / `prompts` /
/// `resources` 均为空对象（仅声明支持），`roots` 声明 `listChanged: true`
/// （M3 安全边界：工程切换时客户端会主动发 `notifications/roots/list_changed`）。
#[must_use]
pub fn client_capabilities() -> Value {
    json!({
        "tools": {},
        "prompts": {},
        "resources": {},
        "roots": { "listChanged": true }
    })
}

/// 客户端信息（`initialize.clientInfo`）。
#[must_use]
pub fn client_info(name: &str) -> Value {
    json!({ "name": name, "version": CLIENT_VERSION })
}

/// MCP 工具定义（`tools/list` 单项）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDefinition {
    /// 工具原始名（未加 `mcp__<server>__` 前缀）。
    pub name: String,
    /// 工具描述（服务端缺省时为空串，对照 Java `asText("")`）。
    pub description: String,
    /// JSON Schema 入参定义（服务端缺省时为空对象）。
    pub input_schema: Value,
}

/// MCP 资源定义（`resources/list` 单项）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDefinition {
    /// 资源 URI。
    pub uri: String,
    /// 可读名（缺省空串）。
    pub name: String,
    /// MIME 类型（缺省 `None`）。
    pub mime_type: Option<String>,
    /// 描述（缺省 `None`）。
    pub description: Option<String>,
}

/// MCP prompt 模板定义（`prompts/list` 单项）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptDefinition {
    /// 模板名。
    pub name: String,
    /// 描述（缺省空串）。
    pub description: String,
    /// 参数列表。
    pub arguments: Vec<PromptArgument>,
}

/// MCP prompt 模板参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptArgument {
    /// 参数名（缺省空串）。
    pub name: String,
    /// 参数描述（缺省空串）。
    pub description: String,
    /// 是否必填（缺省 false）。
    pub required: bool,
}

/// `prompts/get` 返回的单条消息（role + 扁平化后的文本内容）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptMessage {
    /// 角色（服务端缺省时为 `"user"`，对照 Java `asText("user")`）。
    pub role: String,
    /// 文本内容（对象形态取 `text` 字段，缺省空串）。
    pub content: String,
}

/// MCP Roots 描述符 — 允许 MCP 服务器访问的文件系统根。
///
/// 对照 `RootDescriptor.java`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootDescriptor {
    /// 文件系统 URI，形如 `file:///Users/user/project`。
    pub uri: String,
    /// 可读名（通常为项目名）。
    pub name: String,
}

impl RootDescriptor {
    /// 由绝对路径构造。
    ///
    /// 逐字对照 Java `fromPath`：以 `/` 开头 → `"file://" + path`；否则
    /// （Windows 盘符路径）→ `"file:///" + path.replace('\\', '/')`。
    #[must_use]
    pub fn from_path(absolute_path: &str, project_name: impl Into<String>) -> Self {
        let uri = if absolute_path.starts_with('/') {
            format!("file://{absolute_path}")
        } else {
            format!("file:///{}", absolute_path.replace('\\', "/"))
        };
        Self {
            uri,
            name: project_name.into(),
        }
    }
}

/// 集中管理当前 MCP 客户端声明的 Roots 范围。
///
/// 对照 `McpRootsProvider.java`：Java 用 `CopyOnWriteArrayList` + 返回
/// `List.copyOf` 不可变快照；Rust 侧用 `RwLock<Vec<_>>` + 返回克隆快照
/// （同为「读多写极少 + 快照隔离」语义）。
#[derive(Debug, Default)]
pub struct RootsProvider {
    current_roots: RwLock<Vec<RootDescriptor>>,
}

impl RootsProvider {
    /// 创建空的 roots 提供者。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 获取当前声明的所有 roots（快照）。
    ///
    /// 锁中毒时返回空列表并 warn——roots 是安全边界，宁可让服务端看到「无可
    /// 访问根」也不 panic 掉调用方（对齐本 crate 其余锁降级策略）。
    #[must_use]
    pub fn current_roots(&self) -> Vec<RootDescriptor> {
        match self.current_roots.read() {
            Ok(guard) => guard.clone(),
            Err(error) => {
                tracing::warn!(%error, "MCP roots lock poisoned, reporting empty roots");
                Vec::new()
            }
        }
    }

    /// 替换当前 roots 集合 — 工程切换时调用。
    ///
    /// 对照 Java `updateRoots`：先清空，路径非空白才追加单项。
    pub fn update_roots(&self, workspace_path: &str, project_name: &str) {
        if let Ok(mut guard) = self.current_roots.write() {
            guard.clear();
            if !workspace_path.trim().is_empty() {
                guard.push(RootDescriptor::from_path(workspace_path, project_name));
            }
        }
    }

    /// 追加额外 root — 多工程场景。
    pub fn add_root(&self, workspace_path: &str, project_name: &str) {
        if workspace_path.trim().is_empty() {
            return;
        }
        if let Ok(mut guard) = self.current_roots.write() {
            guard.push(RootDescriptor::from_path(workspace_path, project_name));
        }
    }

    /// 清空所有 roots（连接关闭或测试场景）。
    pub fn clear(&self) {
        if let Ok(mut guard) = self.current_roots.write() {
            guard.clear();
        }
    }
}

/// MCP 长操作进度追踪端口。
///
/// 对照 `McpProgressTracker.java` 的对外契约：`tools/call` 前注册
/// progressToken → 服务端 `notifications/progress` 入站时按 token 查会话并推送
/// → 调用结束注销。zk-mcp 只定义端口，前端推送由 zk-server 实现注入
/// （Batch 4B）；未注入时连接层静默丢弃进度通知（对照 Java `tracker == null`
/// 的 debug 分支）。
pub trait ProgressTracker: Send + Sync {
    /// 注册 progressToken 与会话/服务器/工具的关联（token 为空则忽略）。
    fn register_progress(&self, token: &str, session_id: &str, server_name: &str, tool_name: &str);

    /// 注销 progressToken（调用结束，无论成败）。
    fn unregister_progress(&self, token: &str);

    /// 处理服务端 `notifications/progress` 通知（未知 token 静默忽略）。
    fn handle_progress_notification(&self, notification: &Value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_match_java_handshake_shape() {
        let capabilities = client_capabilities();
        assert_eq!(capabilities["tools"], json!({}));
        assert_eq!(capabilities["prompts"], json!({}));
        assert_eq!(capabilities["resources"], json!({}));
        assert_eq!(capabilities["roots"]["listChanged"], json!(true));
    }

    #[test]
    fn client_info_carries_name_and_version() {
        assert_eq!(
            client_info(CLIENT_NAME),
            json!({"name": "zkcode", "version": "1.0.0"})
        );
    }

    #[test]
    fn root_descriptor_from_unix_and_windows_paths() {
        assert_eq!(
            RootDescriptor::from_path("/Users/foo/project", "project").uri,
            "file:///Users/foo/project"
        );
        assert_eq!(
            RootDescriptor::from_path("C:\\work\\project", "project").uri,
            "file:///C:/work/project"
        );
        assert_eq!(RootDescriptor::from_path("", "n").uri, "file:///");
    }

    #[test]
    fn roots_provider_update_replaces_and_ignores_blank() {
        let provider = RootsProvider::new();
        provider.update_roots("/a", "a");
        provider.add_root("/b", "b");
        assert_eq!(provider.current_roots().len(), 2);

        provider.update_roots("/c", "c");
        let roots = provider.current_roots();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].name, "c");

        provider.update_roots("   ", "blank");
        assert!(provider.current_roots().is_empty());

        provider.add_root("/d", "d");
        provider.clear();
        assert!(provider.current_roots().is_empty());
    }
}
