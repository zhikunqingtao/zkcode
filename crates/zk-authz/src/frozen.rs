//! 冻结工具输入：规范字节是授权与后续实际执行共同使用的唯一输入。
//!
//! 逐字对照 `authorization/FrozenToolInput.java`（L12-55）与
//! `authorization/FrozenToolInputFactory.java`（L24-141）。
//!
//! 移植裁定：旧实现用 `AtomicLong inflightBytes` + `LimitedOutputStream` 做 JVM 堆配额
//! 会计（`AUTHORIZATION_INPUT_CAPACITY_EXCEEDED`）。Rust 侧不存在同类堆压力风险面
//! （无 GC、无 8KiB→2× 扩容拷贝语义），故只保留**单次输入字节上限**这一安全语义
//! （`TOOL_INPUT_TOO_LARGE`）；配额会计记为 DEFERRED，详见 `docs/compatibility.md` §8。

use serde_json::Value;

use crate::hashing::{
    INPUT_SCHEMA_VERSION, MAX_CANONICAL_INPUT_BYTES, canonical_json_bytes, input_hash,
};
use crate::model::{AuthzError, AuthzResult};

/// 规范化并冻结后的工具输入快照。
///
/// 对照 `FrozenToolInput.java:12-40`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenToolInput {
    tool_name: String,
    schema_version: i64,
    canonical_json: Vec<u8>,
    input_hash: String,
}

impl FrozenToolInput {
    /// 工具稳定名称。对照 `FrozenToolInput.java:33`。
    #[must_use]
    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }

    /// 输入 schema 版本。对照 `FrozenToolInput.java:34`。
    #[must_use]
    pub const fn schema_version(&self) -> i64 {
        self.schema_version
    }

    /// 规范 JSON 字节。对照 `FrozenToolInput.java:35`（旧实现返回防御性副本）。
    #[must_use]
    pub fn canonical_json_bytes(&self) -> &[u8] {
        &self.canonical_json
    }

    /// 规范 JSON 文本。对照 `FrozenToolInput.java:36-37`。
    #[must_use]
    pub fn canonical_json(&self) -> String {
        String::from_utf8_lossy(&self.canonical_json).into_owned()
    }

    /// 完整性哈希。对照 `FrozenToolInput.java:38`。
    #[must_use]
    pub fn input_hash(&self) -> &str {
        &self.input_hash
    }

    /// 规范字节长度。对照 `FrozenToolInput.java:39`。
    #[must_use]
    pub fn byte_size(&self) -> usize {
        self.canonical_json.len()
    }

    /// 由规范字节重建执行输入。对照 `FrozenToolInput.java:42-50`。
    ///
    /// # Errors
    ///
    /// 规范字节无法解析回 JSON 对象时返回 `TOOL_INPUT_INVALID`。
    pub fn to_tool_input(&self) -> AuthzResult<Value> {
        serde_json::from_slice(&self.canonical_json).map_err(|_| {
            AuthzError::new("TOOL_INPUT_INVALID", "frozen input cannot be reconstructed")
        })
    }
}

/// 有大小上限的规范 JSON 构建器。
///
/// 对照 `FrozenToolInputFactory.java:23-75`。
#[derive(Debug, Clone, Copy)]
pub struct FrozenToolInputFactory {
    max_bytes: usize,
}

impl Default for FrozenToolInputFactory {
    fn default() -> Self {
        Self {
            max_bytes: MAX_CANONICAL_INPUT_BYTES,
        }
    }
}

impl FrozenToolInputFactory {
    /// 以显式字节上限构造。
    ///
    /// # Panics
    ///
    /// `max_bytes == 0` 时 panic——对照 `FrozenToolInputFactory.java:34-36`
    /// 的 `IllegalArgumentException("Invalid authorization canonical input limits")`。
    #[must_use]
    pub fn with_max_bytes(max_bytes: usize) -> Self {
        assert!(
            max_bytes >= 1,
            "Invalid authorization canonical input limits"
        );
        Self { max_bytes }
    }

    /// 将工具输入规范化并冻结。
    ///
    /// 对照 `FrozenToolInputFactory.java:49-75`。
    ///
    /// # Errors
    ///
    /// - `TOOL_INPUT_TOO_LARGE`：规范字节超过上限（`FrozenToolInputFactory.java:68-69`）
    /// - `TOOL_INPUT_INVALID`：输入含非法 JSON 值（`FrozenToolInputFactory.java:70-71`）
    pub fn freeze(&self, tool_name: &str, input: &Value) -> AuthzResult<FrozenToolInput> {
        reject_non_canonical(input)?;
        let bytes = canonical_json_bytes(input);
        if bytes.len() > self.max_bytes {
            return Err(AuthzError::new(
                "TOOL_INPUT_TOO_LARGE",
                format!(
                    "Canonical tool input exceeds {} UTF-8 bytes",
                    self.max_bytes
                ),
            ));
        }
        let hash = input_hash(tool_name, &bytes);
        Ok(FrozenToolInput {
            tool_name: tool_name.to_string(),
            schema_version: INPUT_SCHEMA_VERSION,
            canonical_json: bytes,
            input_hash: hash,
        })
    }
}

/// 规范 JSON 值域校验：拒绝非有限数值。
///
/// 对照 `FrozenToolInputFactory.java:123-133`（`Non-finite JSON number` /
/// `Unsupported canonical JSON value`）。`serde_json::Value` 的类型域天然排除了
/// 旧实现中需要显式列举的 Java 类型（`BigDecimal` 等），且 `Value::Number` 不可能
/// 承载 NaN/Inf，因此本函数只在对象 key 与嵌套结构上做递归遍历以保持行为对齐。
fn reject_non_canonical(value: &Value) -> AuthzResult<()> {
    match value {
        Value::Object(fields) => {
            for nested in fields.values() {
                reject_non_canonical(nested)?;
            }
            Ok(())
        }
        Value::Array(items) => {
            for nested in items {
                reject_non_canonical(nested)?;
            }
            Ok(())
        }
        Value::Number(number) => {
            if number.as_f64().is_some_and(f64::is_finite) || number.is_i64() || number.is_u64() {
                Ok(())
            } else {
                Err(AuthzError::new(
                    "TOOL_INPUT_INVALID",
                    "Non-finite JSON number",
                ))
            }
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 旧源 `FrozenToolInputFactoryTest.java:19-28`：同一输入的不同 key 顺序哈希一致。
    #[test]
    fn key_order_does_not_change_the_input_hash() {
        let factory = FrozenToolInputFactory::default();
        let left = factory.freeze("Read", &json!({"b": 1, "a": 2})).unwrap();
        let right = factory.freeze("Read", &json!({"a": 2, "b": 1})).unwrap();
        assert_eq!(left.input_hash(), right.input_hash());
        assert_eq!(left.canonical_json(), r#"{"a":2,"b":1}"#);
    }

    /// 旧源 `FrozenToolInputFactory.java:64-65`：哈希域按工具名隔离。
    #[test]
    fn different_tools_never_share_an_input_hash() {
        let factory = FrozenToolInputFactory::default();
        let read = factory.freeze("Read", &json!({"a": 1})).unwrap();
        let write = factory.freeze("Write", &json!({"a": 1})).unwrap();
        assert_ne!(read.input_hash(), write.input_hash());
    }

    /// 旧源 `FrozenToolInputFactory.java:68-69`：超限输入抛 `TOOL_INPUT_TOO_LARGE`。
    #[test]
    fn oversized_canonical_input_is_rejected() {
        let factory = FrozenToolInputFactory::with_max_bytes(8);
        let error = factory
            .freeze("Read", &json!({"file_path": "/very/long/path"}))
            .unwrap_err();
        assert_eq!(error.code, "TOOL_INPUT_TOO_LARGE");
    }

    /// 旧源 `FrozenToolInput.java:42-50`：规范字节可无损重建为执行输入。
    #[test]
    fn frozen_bytes_round_trip_to_the_execution_input() {
        let factory = FrozenToolInputFactory::default();
        let frozen = factory
            .freeze("Read", &json!({"file_path": "a.txt"}))
            .unwrap();
        assert_eq!(
            frozen.to_tool_input().unwrap(),
            json!({"file_path": "a.txt"})
        );
        assert_eq!(frozen.schema_version(), 1);
        assert_eq!(frozen.tool_name(), "Read");
        assert_eq!(frozen.byte_size(), frozen.canonical_json_bytes().len());
    }
}
