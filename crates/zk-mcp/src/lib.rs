//! MCP（Model Context Protocol）客户端引擎。
//!
//! 迁移自 Java 基线 `com.aicodeassistant.mcp` 包（35 文件 / 6172 行）的核心
//! 引擎部分：JSON-RPC 协议层、四种传输（STDIO / SSE / Streamable HTTP /
//! WebSocket）、服务器
//! 连接（握手、工具/资源/prompt 发现、调用）、客户端管理器（批量启动、健康检查、
//! 退避重连、代际保护）、多来源配置解析与能力注册表，以及把 MCP 工具适配为
//! `zk_tools::Tool` 的适配器。
//!
//! 依赖倒置：本 crate 不依赖 zk-server / zk-engine。凡需宿主能力（工具注册、
//! 授权判定、健康广播、进度推送）之处均以 trait 端口表达，由组合根注入。
//!
//! 未移植（对照任务判据的「不做项」）：
//! - `SDK` / `ZHIKUN_AI_PROXY` 传输实现（枚举变体保留）；
//! - `ENTERPRISE` / `MANAGED` / `ZHIKUN_AI` 配置来源（Java 侧为空壳）；
//! - `McpAuthTool` 的 OAuth 流程与 `SchemaCompressor`（均 P2）。

pub mod capability_registry;
pub mod config;
pub mod connection;
pub mod error;
pub mod jsonrpc;
pub mod manager;
pub mod prompt_adapter;
pub mod protocol;
pub mod sse;
pub mod stdio;
pub mod streamable_http;
pub mod tool_adapter;
pub mod transport;
pub mod websocket;

pub use capability_registry::{McpCapabilityDefinition, McpCapabilityRegistry};
pub use config::{McpConfigScope, McpConfigurationResolver, McpServerConfig, McpTransportType};
pub use connection::{McpConnectionStatus, McpServerConnection};
pub use error::{JsonRpcError, McpProtocolError};
pub use jsonrpc::{JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, RequestId};
pub use manager::{
    ApprovalPort, ManagerError, McpClientManager, McpClientManagerBuilder, McpHealthObserver,
    McpToolSink,
};
pub use prompt_adapter::McpPromptAdapter;
pub use protocol::{
    ProgressTracker, PromptArgument, PromptDefinition, PromptMessage, ResourceDefinition,
    RootDescriptor, RootsProvider, ToolDefinition,
};
pub use sse::SseTransport;
pub use stdio::StdioTransport;
pub use streamable_http::StreamableHttpTransport;
pub use tool_adapter::McpToolAdapter;
pub use transport::{DisconnectCallback, McpTransport, NotificationHandler};
pub use websocket::WebSocketTransport;
