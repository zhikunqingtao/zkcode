//! 命令契约——[`Command`] / [`CommandType`] / [`CommandResult`]。
//!
//! 语义来源（旧仓库只读，逐字对照）：
//! - `backend/src/main/java/com/aicodeassistant/command/Command.java`
//! - `command/CommandType.java`（三值枚举）
//! - `command/CommandResult.java`（五型 record + `isSuccess`）
//!
//! # 异步方法的承载方式
//!
//! 旧 `Command.execute` 是同步方法（Spring 侧仓储亦同步）；zkcode 的会话仓储
//! 是 `async`，故 `execute` 必须可等待。本 crate 无 `async_trait` 依赖，沿用
//! 既有先例 `zk_engine::MessageSink::push`——返回 `BoxFuture<'a, _>`，trait
//! 因此仍可 `dyn`（对象安全）。
//!
//! # `dyn Command` 的边界
//!
//! [`CommandRegistry`](super::CommandRegistry) 以 `Arc<dyn Command>` 持有实例
//! 并跨 `.await` 传递，故 trait 显式要求 `Send + Sync + 'static`（旧侧是
//! Spring 单例 Bean，等价约束）。

use futures::future::BoxFuture;

use super::context::CommandContext;

/// 命令类型（旧 `CommandType`：三值，无其它成员）。
///
/// 语义（旧枚举注释）：
/// - `Local`——服务端本地执行，结果直接回显，不进模型；
/// - `Prompt`——展开为提示词后注入 LLM 对话；
/// - `LocalJsx`——本地执行且结果由前端结构化组件渲染。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum CommandType {
    /// 旧 `LOCAL`。
    Local,
    /// 旧 `PROMPT`。
    Prompt,
    /// 旧 `LOCAL_JSX`。
    LocalJsx,
}

impl CommandType {
    /// 枚举字面量（旧 Java `enum.toString()` 的输出，`/help` 详情逐字复用）。
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "LOCAL",
            Self::Prompt => "PROMPT",
            Self::LocalJsx => "LOCAL_JSX",
        }
    }
}

impl std::fmt::Display for CommandType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 命令执行结果（旧 `CommandResult` record 的五个 `ResultType` 分支）。
///
/// 旧 record 用 `(type, value, data, error)` 四字段承载所有分支，非法组合靠
/// 静态工厂约束；此处改为 enum——每个分支只带该分支实际用到的负载，非法组合
/// 在类型层不可构造（观察等价，构造面更窄）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandResult {
    /// 旧 `TEXT`：`value` 为回显文本（PROMPT 命令的 `value` 即待注入提示词）。
    Text(String),
    /// 旧 `COMPACT`：`value` = `displayText`，`data` = 压缩元数据。
    Compact {
        /// 旧 `value`（人读摘要行）。
        display_text: String,
        /// 旧 `data`（`compactionData`）。
        data: serde_json::Value,
    },
    /// 旧 `SKIP`：无下行动作。
    Skip,
    /// 旧 `JSX`：`value` 为 null，`data` 为结构化渲染数据。
    Jsx(serde_json::Value),
    /// 旧 `ERROR`：`error` 为失败文案。
    Error(String),
}

impl CommandResult {
    /// 旧 `CommandResult.text(String)`。
    #[must_use]
    pub fn text(value: impl Into<String>) -> Self {
        Self::Text(value.into())
    }

    /// 旧 `CommandResult.compact(String displayText, Map data)`。
    #[must_use]
    pub fn compact(display_text: impl Into<String>, data: serde_json::Value) -> Self {
        Self::Compact {
            display_text: display_text.into(),
            data,
        }
    }

    /// 旧 `CommandResult.skip()`。
    #[must_use]
    pub fn skip() -> Self {
        Self::Skip
    }

    /// 旧 `CommandResult.jsx(Map data)`。
    #[must_use]
    pub fn jsx(data: serde_json::Value) -> Self {
        Self::Jsx(data)
    }

    /// 旧 `CommandResult.error(String)`。
    #[must_use]
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error(message.into())
    }

    /// 旧 `isSuccess()`：`type != ERROR`。
    #[must_use]
    pub fn is_success(&self) -> bool {
        !matches!(self, Self::Error(_))
    }
}

/// 斜杠命令（旧 `Command` 接口）。
///
/// 默认方法与旧接口逐一对应；旧接口的 `isMcp()` / `getLoadedFrom()` /
/// `isSensitive()` 三个默认方法本批无消费方（MCP 命令族归后续 Batch），故未
/// 建模——留痕见 [`super`] 模块文档的偏离表。
pub trait Command: Send + Sync + 'static {
    /// 旧 `getName()`：命令名（不含前导 `/`，小写）。
    fn name(&self) -> &'static str;

    /// 旧 `getAliases()`：别名清单（缺省空，旧 `List.of()`）。
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    /// 旧 `getDescription()`：一行说明（`/help` 列表与详情复用）。
    fn description(&self) -> &'static str;

    /// 旧 `getType()`。
    fn command_type(&self) -> CommandType;

    /// 旧 `execute(String args, CommandContext context)`。
    ///
    /// `args` 为原始参数串（**未** trim；各命令自行按旧实现处理空白，旧侧传的
    /// 也是原始 payload）。
    fn execute<'a>(
        &'a self,
        args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult>;

    /// 旧 `isHidden()`：隐藏命令不出现在 `/help` 列表（缺省 false）。
    fn is_hidden(&self) -> bool {
        false
    }

    /// 旧 `isImmediate()`：前端可在补全时立即执行（缺省 false）。
    fn is_immediate(&self) -> bool {
        false
    }

    /// 旧 `getVersion()`：缺省 `"1.0"`（`/help` 详情输出该值）。
    fn version(&self) -> &'static str {
        "1.0"
    }

    /// 旧 `supportsNonInteractive()`：可在非交互（批处理）模式下执行。
    fn supports_non_interactive(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{CommandResult, CommandType};

    /// 类型字面量即旧 Java 枚举名（`/help` 详情的 `Type:` 行逐字依赖）。
    #[test]
    fn command_type_renders_java_enum_names() {
        assert_eq!(CommandType::Local.as_str(), "LOCAL");
        assert_eq!(CommandType::Prompt.as_str(), "PROMPT");
        assert_eq!(CommandType::LocalJsx.as_str(), "LOCAL_JSX");
        assert_eq!(CommandType::LocalJsx.to_string(), "LOCAL_JSX");
    }

    /// 旧 `isSuccess()`：仅 `ERROR` 为失败，`SKIP` 亦算成功。
    #[test]
    fn only_the_error_variant_is_unsuccessful() {
        assert!(CommandResult::text("x").is_success());
        assert!(CommandResult::jsx(serde_json::json!({})).is_success());
        assert!(CommandResult::compact("x", serde_json::json!({})).is_success());
        assert!(CommandResult::skip().is_success());
        assert!(!CommandResult::error("boom").is_success());
    }
}
