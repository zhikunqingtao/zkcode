//! 入参提取与路径解析共用件（文件族 / 搜索族 / Bash / Git 共享）。
//!
//! 语义来源（旧仓库只读）：`ToolInput.getString/getInt/getBoolean` 的
//! 「缺失即默认值、类型不符即校验失败」取值风格，与
//! `ToolResult.validationError(code, message)` 的错误码前缀文案。
//! 旧 `ToolResult` 有 failureType / retryability / effectState 三维分类；
//! 2.2 冻结的 [`ToolOutput`] 只有 `is_error` 一维，故错误码以
//! `"CODE: message"` 形式落在文本首段（差异留痕 docs/compatibility.md §4）。

use std::path::PathBuf;

use crate::tool::{ToolContext, ToolOutput};

/// 构造带错误码的失败结果（`"CODE: message"`，对照旧
/// `ToolResult.validationError` 的错误码 + 文案二元组）。
pub(crate) fn failure(code: &str, message: impl AsRef<str>) -> ToolOutput {
    ToolOutput::error(format!("{code}: {}", message.as_ref()))
}

/// 必需字符串入参（缺失 / 类型不符 / 空白 → `MISSING_PARAMETER`）。
pub(crate) fn required_str<'a>(
    input: &'a serde_json::Value,
    key: &str,
) -> Result<&'a str, ToolOutput> {
    match input.get(key).and_then(serde_json::Value::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(failure(
            "MISSING_PARAMETER",
            format!("Required parameter '{key}' is missing or not a non-empty string"),
        )),
    }
}

/// 必需字符串入参，**允许空串**（缺失 / 类型不符 → `MISSING_PARAMETER`）。
///
/// 旧 `ToolInput.getString(key)` 不拒空串；`Edit` 的 `old_string` 以空串
/// 表达「新建文件」、`new_string` 以空串表达「删除该片段」，故不能复用
/// [`required_str`] 的非空白口径。
pub(crate) fn required_str_allow_empty<'a>(
    input: &'a serde_json::Value,
    key: &str,
) -> Result<&'a str, ToolOutput> {
    input
        .get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            failure(
                "MISSING_PARAMETER",
                format!("Required parameter '{key}' is missing or not a string"),
            )
        })
}

/// 可选字符串入参（缺失 / 空白 → `None`）。
pub(crate) fn optional_str<'a>(input: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    input
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

/// 可选非负整数入参（负数 / 类型不符 → `None`，对照旧 `getOptionalInt`
/// 的宽容取值 + 调用点 `Math.max(0, …)` 钳制）。
pub(crate) fn optional_usize(input: &serde_json::Value, key: &str) -> Option<usize> {
    input
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
}

/// 可选布尔入参（缺失 / 类型不符 → 默认值）。
pub(crate) fn bool_or(input: &serde_json::Value, key: &str, default: bool) -> bool {
    input
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(default)
}

/// 解析入参路径：绝对路径原样、相对路径以 [`ToolContext::working_dir`] 为
/// 基准（对照旧 `file_path may be absolute or relative to the current
/// Session workspace`）。
///
/// **不做**工作区边界 / 敏感文件校验——旧 `PathSecurityService` /
/// `ManagedWorkspacePathResolver` 的权限面归子阶段 2.5，本阶段留痕不实现。
pub(crate) fn resolve_path(raw: &str, ctx: &ToolContext) -> PathBuf {
    let candidate = PathBuf::from(raw);
    if candidate.is_absolute() {
        candidate
    } else {
        ctx.working_dir().join(candidate)
    }
}

/// 按字符边界截断到 `max_chars`，返回（文本, 是否截断）。
pub(crate) fn truncate_chars(text: String, max_chars: usize) -> (String, bool) {
    if text.chars().count() <= max_chars {
        return (text, false);
    }
    let kept: String = text.chars().take(max_chars).collect();
    (kept, true)
}

/// 截断标记（对照旧 `"\n[Results truncated]"` 逐字文案）。
pub(crate) const RESULTS_TRUNCATED: &str = "\n[Results truncated]";

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ctx(working_dir: &str) -> ToolContext {
        let (tx, _rx) = mpsc::unbounded_channel();
        ToolContext::new(CancellationToken::new(), tx).with_working_dir(working_dir)
    }

    #[test]
    fn required_str_rejects_missing_and_blank() {
        let input = json!({ "a": "x", "b": "   ", "c": 7 });
        assert_eq!(required_str(&input, "a").expect("present"), "x");
        for key in ["b", "c", "missing"] {
            let err = required_str(&input, key).expect_err("rejected");
            assert!(err.is_error);
            assert!(err.content.starts_with("MISSING_PARAMETER: "));
        }
    }

    #[test]
    fn required_str_allow_empty_accepts_empty_string() {
        let input = json!({ "empty": "", "num": 1 });
        assert_eq!(
            required_str_allow_empty(&input, "empty").expect("present"),
            ""
        );
        for key in ["num", "missing"] {
            let err = required_str_allow_empty(&input, key).expect_err("rejected");
            assert!(err.content.starts_with("MISSING_PARAMETER: "));
        }
    }

    #[test]
    fn optional_getters_are_lenient() {
        let input = json!({ "s": "v", "blank": " ", "n": 12, "neg": -3, "b": true });
        assert_eq!(optional_str(&input, "s"), Some("v"));
        assert_eq!(optional_str(&input, "blank"), None);
        assert_eq!(optional_usize(&input, "n"), Some(12));
        assert_eq!(optional_usize(&input, "neg"), None);
        assert!(bool_or(&input, "b", false));
        assert!(bool_or(&input, "missing", true));
    }

    #[test]
    fn resolve_path_joins_relative_only() {
        let ctx = ctx("/tmp/zk-base");
        assert_eq!(
            resolve_path("sub/file.txt", &ctx),
            PathBuf::from("/tmp/zk-base/sub/file.txt")
        );
        assert_eq!(
            resolve_path("/abs/file.txt", &ctx),
            PathBuf::from("/abs/file.txt")
        );
    }

    #[test]
    fn truncate_chars_respects_char_count() {
        let (kept, truncated) = truncate_chars("你好世界".to_owned(), 2);
        assert!(truncated);
        assert_eq!(kept, "你好");
        let (kept, truncated) = truncate_chars("abc".to_owned(), 8);
        assert!(!truncated);
        assert_eq!(kept, "abc");
    }
}
