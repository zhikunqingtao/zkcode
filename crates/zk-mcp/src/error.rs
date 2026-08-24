//! JSON-RPC 2.0 错误对象与 MCP 协议异常。
//!
//! 对照 Java 基线 `mcp/JsonRpcError.java`（68 行）+ `mcp/McpProtocolException.java`
//! （23 行）逐字移植：错误码常量、五个带前缀的工厂方法（`"Parse error: "` 等
//! 前缀逐字保留，因其会随 `tools/call` 失败结果回流到模型可见文本），以及
//! 「异常持 `JsonRpcError` + 独立 display 消息」的双字段形态——Java 的
//! `McpProtocolException(String, Throwable)` 构造把 `getMessage()` 置为原始
//! 消息、而 `rpcError.message()` 置为 `"Internal error: " + message`，二者不等，
//! 上层（`McpToolAdapter`）读的是 `getMessage()`、判分支读的是 `getCode()`。

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 解析错误：收到无效 JSON。
pub const PARSE_ERROR: i32 = -32700;
/// 无效请求：JSON 有效但不符合 JSON-RPC 规范。
pub const INVALID_REQUEST: i32 = -32600;
/// 方法未找到。
pub const METHOD_NOT_FOUND: i32 = -32601;
/// 无效参数。
pub const INVALID_PARAMS: i32 = -32602;
/// 内部错误。
pub const INTERNAL_ERROR: i32 = -32603;
/// MCP 自定义（-32000 ~ -32099）：服务器初始化失败 / 未初始化。
pub const SERVER_NOT_INITIALIZED: i32 = -32002;
/// MCP 自定义（-32000 ~ -32099）：请求超时。
pub const REQUEST_TIMEOUT: i32 = -32001;

/// JSON-RPC 2.0 错误对象（`{code, message, data?}`）。
///
/// `data` 为 `None` 时不序列化（对照 Java `@JsonInclude(NON_NULL)`）；反序列化
/// 忽略未知字段（对照 `@JsonIgnoreProperties(ignoreUnknown = true)`，serde 默认
/// 行为即忽略）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonRpcError {
    /// 错误码（预定义或服务端自定义）。
    pub code: i32,
    /// 错误描述。
    pub message: String,
    /// 附加错误数据（可选）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcError {
    /// 便捷构造：无附加数据（对照 Java `JsonRpcError(int, String)`）。
    #[must_use]
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// 带附加数据构造。
    #[must_use]
    pub fn with_data(code: i32, message: impl Into<String>, data: Value) -> Self {
        Self {
            code,
            message: message.into(),
            data: Some(data),
        }
    }

    /// `-32700`，消息前缀 `"Parse error: "`。
    #[must_use]
    pub fn parse_error(detail: &str) -> Self {
        Self::new(PARSE_ERROR, format!("Parse error: {detail}"))
    }

    /// `-32600`，消息前缀 `"Invalid request: "`。
    #[must_use]
    pub fn invalid_request(detail: &str) -> Self {
        Self::new(INVALID_REQUEST, format!("Invalid request: {detail}"))
    }

    /// `-32601`，消息前缀 `"Method not found: "`。
    #[must_use]
    pub fn method_not_found(method: &str) -> Self {
        Self::new(METHOD_NOT_FOUND, format!("Method not found: {method}"))
    }

    /// `-32602`，消息前缀 `"Invalid params: "`。
    #[must_use]
    pub fn invalid_params(detail: &str) -> Self {
        Self::new(INVALID_PARAMS, format!("Invalid params: {detail}"))
    }

    /// `-32603`，消息前缀 `"Internal error: "`。
    #[must_use]
    pub fn internal_error(detail: &str) -> Self {
        Self::new(INTERNAL_ERROR, format!("Internal error: {detail}"))
    }
}

/// MCP 协议异常 — JSON-RPC 错误响应转化为 Rust 错误。
///
/// 对照 Java `McpProtocolException`：持 [`JsonRpcError`] 供 `code()` 判分支，
/// 另持 display 消息供上层拼装模型可见文本。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct McpProtocolError {
    error: JsonRpcError,
    message: String,
}

impl McpProtocolError {
    /// 由 JSON-RPC 错误对象构造（display 消息 = `error.message`）。
    ///
    /// 对照 Java `McpProtocolException(JsonRpcError)`。
    #[must_use]
    pub fn from_rpc(error: JsonRpcError) -> Self {
        let message = error.message.clone();
        Self { error, message }
    }

    /// 由本地失败原因构造：display 消息为原始 `message`，而内嵌错误对象为
    /// `INTERNAL_ERROR` + `"Internal error: " + message`。
    ///
    /// 对照 Java `McpProtocolException(String message, Throwable cause)`——Rust
    /// 侧不持 cause（调用点已把 source 文本拼进 `message`）。
    #[must_use]
    pub fn wrapped(message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            error: JsonRpcError::internal_error(&message),
            message,
        }
    }

    /// `code, message` 直构（不加前缀），对应 Java
    /// `new McpProtocolException(new JsonRpcError(code, message))`。
    #[must_use]
    pub fn of(code: i32, message: impl Into<String>) -> Self {
        Self::from_rpc(JsonRpcError::new(code, message))
    }

    /// `SERVER_NOT_INITIALIZED`（`-32002`）：传输未连接 / 服务器未就绪。
    #[must_use]
    pub fn not_initialized(message: impl Into<String>) -> Self {
        Self::of(SERVER_NOT_INITIALIZED, message)
    }

    /// `REQUEST_TIMEOUT`（`-32001`）：请求超时。
    #[must_use]
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::of(REQUEST_TIMEOUT, message)
    }

    /// `INTERNAL_ERROR`（`-32603`）：不加前缀的内部错误（如 `"HTTP 500: ..."`）。
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::of(INTERNAL_ERROR, message)
    }

    /// JSON-RPC 错误码（对照 Java `getCode()`）。
    #[must_use]
    pub fn code(&self) -> i32 {
        self.error.code
    }

    /// display 消息（对照 Java `getMessage()`）。
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// 内嵌 JSON-RPC 错误对象（对照 Java `getRpcError()`）。
    #[must_use]
    pub fn rpc_error(&self) -> &JsonRpcError {
        &self.error
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factories_keep_java_message_prefixes() {
        assert_eq!(JsonRpcError::parse_error("bad").message, "Parse error: bad");
        assert_eq!(
            JsonRpcError::invalid_request("x").message,
            "Invalid request: x"
        );
        assert_eq!(
            JsonRpcError::method_not_found("tools/list").message,
            "Method not found: tools/list"
        );
        assert_eq!(
            JsonRpcError::invalid_params("y").message,
            "Invalid params: y"
        );
        assert_eq!(
            JsonRpcError::internal_error("z").message,
            "Internal error: z"
        );
        assert_eq!(JsonRpcError::parse_error("bad").code, PARSE_ERROR);
    }

    #[test]
    fn data_absent_is_not_serialized() {
        let json = serde_json::to_string(&JsonRpcError::new(INTERNAL_ERROR, "boom")).unwrap();
        assert_eq!(json, r#"{"code":-32603,"message":"boom"}"#);
    }

    #[test]
    fn deserialize_ignores_unknown_fields() {
        let error: JsonRpcError =
            serde_json::from_str(r#"{"code":-32601,"message":"m","extra":1}"#).unwrap();
        assert_eq!(error.code, METHOD_NOT_FOUND);
        assert!(error.data.is_none());
    }

    #[test]
    fn wrapped_keeps_raw_display_but_prefixed_rpc_message() {
        let error = McpProtocolError::wrapped("connect failed: refused");
        assert_eq!(error.message(), "connect failed: refused");
        assert_eq!(error.code(), INTERNAL_ERROR);
        assert_eq!(
            error.rpc_error().message,
            "Internal error: connect failed: refused"
        );
        assert_eq!(error.to_string(), "connect failed: refused");
    }

    #[test]
    fn typed_constructors_carry_expected_codes() {
        assert_eq!(
            McpProtocolError::not_initialized("STDIO not connected").code(),
            SERVER_NOT_INITIALIZED
        );
        assert_eq!(
            McpProtocolError::timeout("Request timeout: tools/list").code(),
            REQUEST_TIMEOUT
        );
        assert_eq!(
            McpProtocolError::internal("HTTP 500: x").code(),
            INTERNAL_ERROR
        );
    }
}
