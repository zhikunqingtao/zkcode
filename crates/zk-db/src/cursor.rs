//! 列表游标原语——格式与旧系统线上行为逐字对齐（S2 实采样例黄金锁定）。
//!
//! 两类游标（**编码格式不同**，勿混淆）：
//! - **会话列表**（`GET /api/sessions`）：`Base64("updated_at|session_id")`，
//!   `updated_at` 为库内 RFC 3339 字符串，`session_id` 为锚点会话 ID；
//! - **消息列表**（`GET /api/sessions/{id}/messages`）：`Base64(十进制索引)`，
//!   旧系统 P0 简单截取实现（源码注释「后续 Round 实现完整游标分页」）。
//!
//! Base64 alphabet 为标准带 padding（`java.util.Base64.getEncoder()` 等价）。
//! 旧系统对无效游标（非 Base64 / 缺 `|` / 非数字）仅 `log.warn` 后回退为
//! 「从最新开始」；本 crate 以 `Option` 表达同一语义，不产生错误。

use base64::Engine as _;

/// 标准 Base64 引擎（带 padding，对齐 `java.util.Base64.getEncoder()`）。
const ENGINE: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// 编码会话列表游标：`Base64("updated_at|session_id")`。
pub(crate) fn encode_session_cursor(updated_at: &str, session_id: &str) -> String {
    ENGINE.encode(format!("{updated_at}|{session_id}"))
}

/// 解码会话列表游标 → 锚点会话 ID（`split("\\|", 2)` 的 `parts[1]`）。
///
/// 无效（非 Base64 / 非 UTF-8 / 无 `|`）返回 `None`，调用方回退为
/// 「从最新开始」（对齐旧系统 `SessionController.listSessions`）。
pub(crate) fn decode_session_cursor(cursor: &str) -> Option<String> {
    let decoded = ENGINE.decode(cursor).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    decoded.split_once('|').map(|(_, id)| id.to_owned())
}

/// 编码消息列表游标：`Base64(十进制索引)`。
pub(crate) fn encode_message_cursor(index: u64) -> String {
    ENGINE.encode(index.to_string())
}

/// 解码消息列表游标 → 起始索引。无效返回 `None`（调用方回退索引 0）。
pub(crate) fn decode_message_cursor(cursor: &str) -> Option<u64> {
    let decoded = ENGINE.decode(cursor).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    decoded.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 黄金样例：S2 实采 `GET /api/sessions?limit=5` 的 nextCursor 原文，
    /// 解码应为 `2026-08-08T10:38:15.413839Z|32f9a9fb-cf5e-4104-9335-e2e0dc170061`。
    const GOLDEN_CURSOR: &str =
        "MjAyNi0wOC0wOFQxMDozODoxNS40MTM4MzlafDMyZjlhOWZiLWNmNWUtNDEwNC05MzM1LWUyZTBkYzE3MDA2MQ==";
    const GOLDEN_UPDATED_AT: &str = "2026-08-08T10:38:15.413839Z";
    const GOLDEN_SESSION_ID: &str = "32f9a9fb-cf5e-4104-9335-e2e0dc170061";

    #[test]
    fn session_cursor_matches_live_sample() {
        assert_eq!(
            decode_session_cursor(GOLDEN_CURSOR).as_deref(),
            Some(GOLDEN_SESSION_ID)
        );
        assert_eq!(
            encode_session_cursor(GOLDEN_UPDATED_AT, GOLDEN_SESSION_ID),
            GOLDEN_CURSOR
        );
    }

    #[test]
    fn session_cursor_invalid_inputs_fall_to_none() {
        assert_eq!(decode_session_cursor("!!!not-base64!!!"), None);
        assert_eq!(decode_session_cursor("aGVsbG8"), None); // "hello"，无 '|'
        // "aGVsbG8=" = "hello"（合法 Base64、无 '|'）→ None
        assert_eq!(decode_session_cursor("aGVsbG8="), None);
    }

    #[test]
    fn message_cursor_roundtrip() {
        assert_eq!(encode_message_cursor(0), "MA==");
        assert_eq!(encode_message_cursor(50), "NTA=");
        assert_eq!(decode_message_cursor("NTA="), Some(50));
        assert_eq!(decode_message_cursor("###"), None);
        assert_eq!(decode_message_cursor("bm90LWEtbnVtYmVy"), None); // "not-a-number"
    }

    /// `split("\\|", 2)` 语义：`updated_at` 含 `|` 时只取**首个**之后全部——
    /// 此时取出的「id」已受污染（`timestamp|abc`），锚点查询 miss → 回退最新，
    /// 与旧系统逐字一致（时间戳含 `|` 属畸形数据，不额外防御）。
    #[test]
    fn session_cursor_splits_on_first_pipe_only() {
        let encoded = encode_session_cursor("weird|timestamp", "abc");
        assert_eq!(
            decode_session_cursor(&encoded).as_deref(),
            Some("timestamp|abc")
        );
    }
}
