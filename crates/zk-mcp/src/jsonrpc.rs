//! JSON-RPC 2.0 报文类型 — MCP 协议基础通信单元。
//!
//! 对照 Java 基线 `mcp/JsonRpcMessage.java`（89 行）：`Request` / `Response` /
//! `Notification` 三种类型 + `JSONRPC_VERSION` 常量。
//!
//! 偏离：Java 的第四种 `BatchRequest` 未移植——它在旧仓库全域仅有声明、无任何
//! 构造或消费点（MCP 客户端侧从不发批量请求），移植等于引入死代码。
//!
//! 补充：新增 [`IncomingMessage`] 作为「未知形态入站报文」的统一解析目标。
//! Java 侧传输层直接在 `JsonNode` 上走 `path("id")` / `path("method")` 弱类型
//! 探测（响应与服务端→客户端反向请求共用一条解码路径），Rust 无 `JsonNode`
//! 等价物，故以「全字段 Option」结构承载同一语义。

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::JsonRpcError;

/// JSON-RPC 协议版本串（`"2.0"`）。
pub const JSONRPC_VERSION: &str = "2.0";

/// `"2.0"` 的 serde 默认值提供者。
fn default_version() -> String {
    JSONRPC_VERSION.to_owned()
}

/// JSON-RPC 请求 id — 规范允许 String 或 Number。
///
/// 以 `#[serde(untagged)]` 承载联合类型：数字优先（本客户端自发的 id 恒为自增
/// 数字），字符串兜底（服务端反向请求的 id 可能为字符串）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    /// 数字 id。
    Number(i64),
    /// 字符串 id。
    Text(String),
}

impl RequestId {
    /// pending 请求表的匹配键。
    ///
    /// 对照 Java 传输层一律以 `idNode.asText()` 作 `pendingRequests` 的键——
    /// 数字 `7` 与字符串 `"7"` 归一为同一键，保证服务端回显类型漂移时仍能匹配。
    #[must_use]
    pub fn as_key(&self) -> String {
        match self {
            Self::Number(n) => n.to_string(),
            Self::Text(s) => s.clone(),
        }
    }
}

impl From<i64> for RequestId {
    fn from(value: i64) -> Self {
        Self::Number(value)
    }
}

impl From<String> for RequestId {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Number(n) => write!(f, "{n}"),
            Self::Text(s) => write!(f, "{s}"),
        }
    }
}

/// JSON-RPC 请求 — 需要响应。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    /// 协议版本，恒为 `"2.0"`。
    #[serde(default = "default_version")]
    pub jsonrpc: String,
    /// 请求 id。
    pub id: RequestId,
    /// 方法名。
    pub method: String,
    /// 参数（`None` 时不序列化，对照 `@JsonInclude(NON_NULL)`）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl JsonRpcRequest {
    /// 构造请求（版本自动填 `"2.0"`）。
    #[must_use]
    pub fn new(id: RequestId, method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: default_version(),
            id,
            method: method.into(),
            params,
        }
    }
}

/// JSON-RPC 响应 — 对请求的回复。`result` 与 `error` 互斥。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    /// 协议版本，恒为 `"2.0"`。
    #[serde(default = "default_version")]
    pub jsonrpc: String,
    /// 对应请求的 id（协议错误时可能为 null）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
    /// 成功结果。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// 失败错误对象。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    /// 成功响应（对照 Java `Response.success`）。
    #[must_use]
    pub fn success(id: RequestId, result: Value) -> Self {
        Self {
            jsonrpc: default_version(),
            id: Some(id),
            result: Some(result),
            error: None,
        }
    }

    /// 错误响应（对照 Java `Response.error`）。
    #[must_use]
    pub fn error(id: Option<RequestId>, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: default_version(),
            id,
            result: None,
            error: Some(error),
        }
    }

    /// 是否为错误响应（对照 Java `isError()`）。
    #[must_use]
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }
}

/// JSON-RPC 通知 — 无需响应（无 `id` 字段）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    /// 协议版本，恒为 `"2.0"`。
    #[serde(default = "default_version")]
    pub jsonrpc: String,
    /// 方法名。
    pub method: String,
    /// 参数（`None` 时不序列化）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl JsonRpcNotification {
    /// 构造通知（版本自动填 `"2.0"`）。
    #[must_use]
    pub fn new(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: default_version(),
            method: method.into(),
            params,
        }
    }
}

/// 未知形态的入站报文 — 响应 / 通知 / 服务端反向请求三者的统一解析目标。
///
/// 全字段可选：`id` 有值且 `method` 为空 → 响应；`id` 空且 `method` 有值 →
/// 通知；两者皆有 → 服务端→客户端反向请求（如 `roots/list`）。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct IncomingMessage {
    /// 请求 id（`null` 与缺失等价，对照 Java `!idNode.isNull()` 判定）。
    #[serde(default)]
    pub id: Option<RequestId>,
    /// 方法名（通知 / 反向请求才有）。
    #[serde(default)]
    pub method: Option<String>,
    /// 参数。
    #[serde(default)]
    pub params: Option<Value>,
    /// 成功结果。
    #[serde(default)]
    pub result: Option<Value>,
    /// 错误对象。
    #[serde(default)]
    pub error: Option<JsonRpcError>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_serializes_with_version_and_omits_null_params() {
        let request = JsonRpcRequest::new(RequestId::Number(1), "tools/list", None);
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        );
    }

    #[test]
    fn request_id_union_round_trips_both_shapes() {
        let numeric: RequestId = serde_json::from_str("7").unwrap();
        let textual: RequestId = serde_json::from_str("\"7\"").unwrap();
        assert_eq!(numeric, RequestId::Number(7));
        assert_eq!(textual, RequestId::Text("7".to_owned()));
        // pending 表键归一：数字与字符串同键（对照 Java `idNode.asText()`）。
        assert_eq!(numeric.as_key(), textual.as_key());
        assert_eq!(serde_json::to_string(&numeric).unwrap(), "7");
        assert_eq!(serde_json::to_string(&textual).unwrap(), "\"7\"");
    }

    #[test]
    fn response_success_and_error_are_mutually_exclusive() {
        let ok = JsonRpcResponse::success(RequestId::Number(2), json!({"tools": []}));
        assert!(!ok.is_error());
        assert_eq!(
            serde_json::to_value(&ok).unwrap(),
            json!({"jsonrpc": "2.0", "id": 2, "result": {"tools": []}})
        );

        let bad = JsonRpcResponse::error(
            Some(RequestId::Text("abc".to_owned())),
            JsonRpcError::method_not_found("nope"),
        );
        assert!(bad.is_error());
        assert_eq!(
            serde_json::to_value(&bad).unwrap(),
            json!({
                "jsonrpc": "2.0",
                "id": "abc",
                "error": {"code": -32601, "message": "Method not found: nope"}
            })
        );
    }

    #[test]
    fn notification_has_no_id_field() {
        let notification = JsonRpcNotification::new("notifications/initialized", None);
        let json = serde_json::to_value(&notification).unwrap();
        assert!(json.get("id").is_none());
        assert_eq!(json["method"], "notifications/initialized");
    }

    #[test]
    fn incoming_message_discriminates_three_shapes() {
        let response: IncomingMessage =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":3,"result":{"ok":true}}"#).unwrap();
        assert!(response.id.is_some() && response.method.is_none());

        let notification: IncomingMessage =
            serde_json::from_str(r#"{"jsonrpc":"2.0","method":"notifications/progress"}"#).unwrap();
        assert!(notification.id.is_none() && notification.method.is_some());

        let reverse: IncomingMessage =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":"r1","method":"roots/list"}"#).unwrap();
        assert!(reverse.id.is_some() && reverse.method.is_some());

        // 显式 null id 与缺失 id 等价。
        let explicit_null: IncomingMessage =
            serde_json::from_str(r#"{"id":null,"method":"x"}"#).unwrap();
        assert!(explicit_null.id.is_none());
    }
}
