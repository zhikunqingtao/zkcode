//! Python 桥接工具族——`WebBrowser` / `CodeIntel` / `Git`（2.6）。
//!
//! 三件工具逐字对照旧 `tool/impl/{WebBrowserTool,CodeIntelTool,GitTool}.java`：
//! 工具名、description / prompt 文案、JSON Schema 字段、校验分支与错误码、
//! 降级文案全部原样搬运，仅把 Java 的 `PythonCapabilityAwareClient` 换成
//! [`super::PythonClient`]（HTTP over UDS）。
//!
//! # 两处结构性差异（详见 `docs/compatibility.md` §6 偏离表）
//!
//! 1. **`isEnabled()` 双门控拆两处**：旧 `Tool.isEnabled()` 由注册表在每次
//!    列举工具时动态调用（feature flag ∧ Python 能力域）。Rust 组合根在每次
//!    能力刷新后动态增删三件工具并重建 `ToolSearch` 快照；execute 期仍保留
//!    能力门，覆盖刷新与执行之间的竞态窗口。
//! 2. **`ToolResult` 三维 → `ToolOutput` 一维**：旧 `failureType` /
//!    `retryability` / `effectState` 在 2.2 冻结的 [`ToolOutput`] 中无承载
//!    位，沿用 zk-tools §4 既定约定——错误码以 `"CODE: message"` 落文本首段。

mod code_intel;
mod git_enhanced;
mod verify_journey;
mod web_browser;

pub use code_intel::CodeIntelTool;
pub use git_enhanced::GitEnhancedTool;
pub use verify_journey::BrowserVerifyJourneyTool;
pub use web_browser::WebBrowserTool;

use zk_tools::ToolOutput;

/// 浏览器自动化能力域（旧 `WebBrowserTool.CAPABILITY`）。
pub(crate) const BROWSER_AUTOMATION: &str = "BROWSER_AUTOMATION";

/// 代码智能能力域（旧 `CodeIntelTool.call` 传入的 `"CODE_INTEL"`）。
pub(crate) const CODE_INTEL: &str = "CODE_INTEL";

/// Git 增强能力域（旧 `GitTool.CAPABILITY`）。
pub(crate) const GIT_ENHANCED: &str = "GIT_ENHANCED";

/// 带错误码的失败结果（`"CODE: message"`——与 zk-tools `input::failure`
/// 同形；该函数为 zk-tools crate 私有，此处自持等价副本）。
pub(crate) fn failure(code: &str, message: impl AsRef<str>) -> ToolOutput {
    ToolOutput::error(format!("{code}: {}", message.as_ref()))
}

/// 可选字符串入参（缺失 / 类型不符 → `None`；**不**过滤空白，对照旧
/// `ToolInput.getString(key, null)` 的原样取值——空白判定由各校验分支
/// 自行 `isBlank()`）。
pub(crate) fn opt_str<'a>(input: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    input.get(key).and_then(serde_json::Value::as_str)
}

/// 字符串是否 `null` 或全空白（旧 `value == null || value.isBlank()`）。
pub(crate) fn is_blank(value: Option<&str>) -> bool {
    value.is_none_or(|text| text.trim().is_empty())
}

/// 可选整数入参 + 默认值（旧 `ToolInput.getInt(key, default)`）。
pub(crate) fn int_or(input: &serde_json::Value, key: &str, default: i64) -> i64 {
    input
        .get(key)
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(default)
}

/// 排序后的允许值清单文案（旧 `Set.toString()` 顺序不稳定，此处取字典序
/// 以获得确定性文案：`[a, b, c]`）。
pub(crate) fn allowed_list(actions: &[&str]) -> String {
    let mut sorted: Vec<&str> = actions.to_vec();
    sorted.sort_unstable();
    format!("[{}]", sorted.join(", "))
}

/// Python 统一响应信封（`python-service` 的 `*Response` Pydantic 模型：
/// `{success, data, error_code, error_message}`，`snake_case`）。
#[derive(Debug, serde::Deserialize)]
pub(crate) struct PythonEnvelope {
    /// 业务是否成功。
    #[serde(default)]
    pub(crate) success: bool,
    /// 成功载荷（失败时通常为 `null`）。
    #[serde(default)]
    pub(crate) data: Option<serde_json::Value>,
    /// 业务错误码。
    #[serde(default)]
    pub(crate) error_code: Option<String>,
    /// 业务错误文案。
    #[serde(default)]
    pub(crate) error_message: Option<String>,
}

impl PythonEnvelope {
    /// 错误码（缺失时 `"UNKNOWN"`，对照旧 `GitTool` :170 的兜底）。
    pub(crate) fn code(&self) -> &str {
        self.error_code.as_deref().unwrap_or("UNKNOWN")
    }

    /// 错误文案（缺失时 `"Unknown error"`，对照旧 `GitTool` :171 的兜底）。
    pub(crate) fn message(&self) -> &str {
        self.error_message.as_deref().unwrap_or("Unknown error")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn failure_prefixes_error_code() {
        let out = failure("SOME_CODE", "boom");
        assert!(out.is_error);
        assert_eq!(out.content, "SOME_CODE: boom");
    }

    #[test]
    fn blank_detection_matches_java_is_blank() {
        assert!(is_blank(None));
        assert!(is_blank(Some("")));
        assert!(is_blank(Some("   \t\n")));
        assert!(!is_blank(Some(" x ")));
    }

    #[test]
    fn scalar_extraction_falls_back_to_defaults() {
        let input = json!({ "a": "x", "n": 7, "wrong": true });
        assert_eq!(opt_str(&input, "a"), Some("x"));
        assert_eq!(opt_str(&input, "missing"), None);
        assert_eq!(opt_str(&input, "wrong"), None);
        assert_eq!(int_or(&input, "n", 30_000), 7);
        assert_eq!(int_or(&input, "missing", 30_000), 30_000);
        assert_eq!(int_or(&input, "wrong", 30_000), 30_000);
    }

    #[test]
    fn allowed_list_is_deterministic() {
        assert_eq!(
            allowed_list(&["log", "diff", "blame"]),
            "[blame, diff, log]"
        );
    }

    #[test]
    fn envelope_defaults_cover_missing_fields() {
        let envelope: PythonEnvelope = serde_json::from_str("{}").expect("empty object parses");
        assert!(!envelope.success);
        assert!(envelope.data.is_none());
        assert_eq!(envelope.code(), "UNKNOWN");
        assert_eq!(envelope.message(), "Unknown error");

        let envelope: PythonEnvelope = serde_json::from_value(json!({
            "success": false,
            "data": null,
            "error_code": "GIT_REPO_INVALID",
            "error_message": "not a repo"
        }))
        .expect("envelope parses");
        assert_eq!(envelope.code(), "GIT_REPO_INVALID");
        assert_eq!(envelope.message(), "not a repo");
    }
}
