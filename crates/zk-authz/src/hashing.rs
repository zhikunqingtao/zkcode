//! 授权哈希与规范 JSON 的唯一权威实现。
//!
//! 逐字对照 `backend/src/main/java/com/aicodeassistant/authorization/OperationHashing.java`
//! （L14-36）与 `FrozenToolInputFactory.java`（L64-65、L90-141）。四个哈希域各自带
//! 独立字符串前缀（含 `\0` 分隔），互不串扰：
//!
//! | 哈希 | 前缀 | 旧源 |
//! |---|---|---|
//! | `operation_hash` | `authz-operation-v1\0` | `OperationHashing.java:21` |
//! | `input_hash` | `authz-input-v1\0{tool}\0{schema}\0` | `FrozenToolInputFactory.java:64-65` |
//! | `workspace_key` | `workspace-v2\0{identityPath}` | `WorkspaceIdentityService.java:53` |
//! | `capability_hash` | `{kind}\0{constraintsJson}\0{schema}` | `PermissionGrantRepository.java:129` |
//!
//! 全部为无 salt 的裸 SHA-256，输出小写 hex 64 字符（`HexFormat.of().formatHex`）。

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

/// 冻结输入的 schema 版本。
///
/// 对照 `FrozenToolInputFactory.java:25` `INPUT_SCHEMA_VERSION = 1`。
pub const INPUT_SCHEMA_VERSION: i64 = 1;

/// 规范 JSON 单次输入的字节上限。
///
/// 对照 `FrozenToolInputFactory.java:32` 默认 `authorization.max-canonical-input-bytes:10485760`。
pub const MAX_CANONICAL_INPUT_BYTES: usize = 10 * 1024 * 1024;

/// 小写 hex 编码，对照 `java.util.HexFormat.of().formatHex`。
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // 写入 String 永不失败；`expect` 仅为满足 `must_use`。
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// 裸 SHA-256 hex（无前缀），对照
/// `WorkspaceIdentityService.java:220-227` 与 `PermissionGrantRepository.java:392-396`。
#[must_use]
pub fn sha256_hex(value: &str) -> String {
    hex(&Sha256::digest(value.as_bytes()))
}

/// 递归按 key 排序：对象转 `TreeMap` 顺序，数组保序后逐元素递归。
///
/// 对照 `OperationHashing.java:25-36`。数组顺序**不排序**——旧实现只对 object 的
/// field 排序，元素次序由 [`crate::canonicalizer`] 在事实构造阶段固定。
#[must_use]
pub fn sort_json(node: &Value) -> Value {
    match node {
        Value::Object(fields) => {
            let mut keys: Vec<&String> = fields.keys().collect();
            // Java `TreeMap<String, _>` 用 String 自然序（UTF-16 code unit）；此处为
            // UTF-8 字节序。授权事实的 key 全为 ASCII 标识符，两者完全一致。
            keys.sort_unstable();
            let mut sorted = Map::new();
            for key in keys {
                sorted.insert(key.clone(), sort_json(&fields[key]));
            }
            Value::Object(sorted)
        }
        Value::Array(items) => Value::Array(items.iter().map(sort_json).collect()),
        other => other.clone(),
    }
}

/// 规范 JSON 字节：递归 key 排序后紧凑序列化（无空格、无换行）。
///
/// 对照 `OperationHashing.java:18-19`（`mapper.writeValueAsBytes(sort(...))`）与
/// `FrozenToolInputFactory.java:90-134`（`writeCanonical` 逐层排序后由 `JsonGenerator`
/// 紧凑输出）。两条旧路径产出的字节完全同构，故本 crate 只保留一份实现。
#[must_use]
pub fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    // `serde_json::to_vec` 的紧凑形态与 Jackson 默认 `writeValueAsBytes` 一致：
    // 无缩进、`:`/`,` 无空格、仅转义 `"` `\` 与 <0x20 控制字符。
    serde_json::to_vec(&sort_json(value)).unwrap_or_else(|_| b"null".to_vec())
}

/// `operation_hash`：`SHA256("authz-operation-v1\0" ‖ canonicalJson)`。
///
/// 对照 `OperationHashing.java:16-24`。注意前缀 `update` 在 `digest(bytes)` **之前**，
/// 即前缀参与摘要但不出现在 JSON 里。
#[must_use]
pub fn operation_hash(facts: &Value) -> String {
    let mut digest = Sha256::new();
    digest.update(b"authz-operation-v1\0");
    digest.update(canonical_json_bytes(facts));
    hex(&digest.finalize())
}

/// `input_hash`：`SHA256("authz-input-v1\0{tool}\0{schema}\0" ‖ canonicalBytes)`。
///
/// 对照 `FrozenToolInputFactory.java:64-65` 与 `sha256(prefix, bytes, length)`（L136-141）。
#[must_use]
pub fn input_hash(tool_name: &str, canonical: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(format!("authz-input-v1\0{tool_name}\0{INPUT_SCHEMA_VERSION}\0").as_bytes());
    digest.update(canonical);
    hex(&digest.finalize())
}

/// `workspace_key`：`SHA256("workspace-v2\0" + identityPath)`。
///
/// 对照 `WorkspaceIdentityService.java:53`。
#[must_use]
pub fn workspace_key(identity_path: &str) -> String {
    sha256_hex(&format!("workspace-v2\0{identity_path}"))
}

/// `capability_hash`：`SHA256(kind + "\0" + constraintsJson + "\0" + schemaVersion)`。
///
/// 对照 `PermissionGrantRepository.java:128-129`。仅 capability 类 grant 计算该值，
/// `EXACT_GUARDED` / `TOOL_GUARDED` 落库时为 NULL。
#[must_use]
pub fn capability_hash(kind: &str, constraints_json: &str, schema_version: i64) -> String {
    sha256_hex(&format!("{kind}\0{constraints_json}\0{schema_version}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 黄金向量：由 `python3 -c 'hashlib.sha256(b"authz-operation-v1\x00{}").hexdigest()'`
    /// 独立算出，与本实现互锁（旧源 `OperationHashing.java:16-24`）。
    #[test]
    fn operation_hash_matches_independent_golden_vector() {
        assert_eq!(
            operation_hash(&json!({})),
            "b416a1c20bd21da720fa371fe5b12a7b7c766b8f8f2a7b26e96859a720d4292e"
        );
    }

    /// 旧源 `OperationHashing.java:26-31`：对象 key 递归排序，输出与构造顺序无关。
    #[test]
    fn object_keys_are_sorted_recursively() {
        let a = json!({"b": {"y": 1, "x": 2}, "a": 3});
        let b = json!({"a": 3, "b": {"x": 2, "y": 1}});
        assert_eq!(canonical_json_bytes(&a), canonical_json_bytes(&b));
        assert_eq!(
            String::from_utf8(canonical_json_bytes(&a)).unwrap(),
            r#"{"a":3,"b":{"x":2,"y":1}}"#
        );
    }

    /// 旧源 `OperationHashing.java:32-34`：数组保序，不参与排序。
    #[test]
    fn array_order_is_preserved() {
        assert_ne!(
            canonical_json_bytes(&json!({"e": ["b", "a"]})),
            canonical_json_bytes(&json!({"e": ["a", "b"]}))
        );
    }

    /// 旧源 `FrozenToolInputFactory.java:64-65`：input 域前缀含工具名，跨工具隔离。
    #[test]
    fn input_hash_is_domain_separated_per_tool() {
        assert_ne!(input_hash("Read", b"{}"), input_hash("Write", b"{}"));
        assert_eq!(input_hash("Read", b"{}").len(), 64);
    }

    /// 旧源 `WorkspaceIdentityService.java:53`：workspaceKey 为无 salt 裸 SHA-256。
    #[test]
    fn workspace_key_is_prefixed_plain_sha256() {
        assert_eq!(workspace_key("/tmp/x"), sha256_hex("workspace-v2\0/tmp/x"));
        assert_eq!(workspace_key("/tmp/x").len(), 64);
    }

    /// 旧源 `PermissionGrantRepository.java:128-129`：capabilityHash 三段拼接。
    #[test]
    fn capability_hash_concatenates_kind_constraints_and_schema() {
        assert_eq!(
            capability_hash("READ_CAPABILITY", "{}", 1),
            sha256_hex("READ_CAPABILITY\u{0}{}\u{0}1")
        );
    }
}
