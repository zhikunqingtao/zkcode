//! zk-protocol 错误类型。

/// 协议层错误——反序列化失败 / 未知消息类型。
///
/// # 未知 `type` 的处理约定（契约冻结）
///
/// zk-protocol **不提供** lossy（宽容）解析：下行 JSON 携带本枚举未收录的
/// `type` 字符串时，`ServerMessage` / [`crate::ServerEnvelope`] 反序列化直接
/// 返回 [`ProtocolError::Serde`]（serde unknown-variant 错误）。
///
/// 该语义对齐旧前端白名单行为（`stompClient.ts` `VALID_MESSAGE_TYPES` 之外的
/// 类型 `console.debug` 打日志后跳过）：**由调用方（ws 层）捕获本错误，
/// 记 WARN 日志并丢弃该帧**，不得中断连接、不得向上抛 panic。错误信息保留
/// 原始 type 字符串以便排障（见 `tests/roundtrip.rs::unknown_type_is_error`）。
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// JSON 反序列化失败（含未知 `type` variant：serde 报 unknown variant
    /// 错误，错误信息内嵌原始 type 字符串）。
    #[error("protocol deserialization failure: {0}")]
    Serde(#[from] serde_json::Error),
}
