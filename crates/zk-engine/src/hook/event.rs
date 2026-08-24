//! Hook 事件类型与配置数据模型（Batch 8B Step 1）。
//!
//! 对照旧 `hook/HookEvent.java`（76L）。语义偏离留痕：
//!
//! - **H-01 事件集**：旧枚举 12 成员（`PRE_TOOL_USE` / `POST_TOOL_USE` /
//!   `USER_PROMPT_SUBMIT` / `NOTIFICATION` / `STOP` / `SESSION_START` /
//!   `SESSION_END` / `TASK_COMPLETED` / `TEAMMATE_IDLE` / `STOP_HOOKS` /
//!   `PRE_COMPACT` / `POST_COMPACT`），且旧 hook 是**进程内函数**——可改写
//!   工具输入/输出、拒绝调用（`HookResult.proceed=false`），参与准入裁决。
//!   本端 hook 是**外部副作用通知**（本地命令 / HTTP POST），仅告知事件发生，
//!   **绝不**参与主流程裁决（准入唯一权威仍是 [`crate::admission::ToolAdmission`]）。
//!   故事件集按 Batch 8B 规格重定为 8 个观测点：工具执行前后、会话起止、
//!   run 起止、消息发送、错误发生。
//! - **H-01b 解析容错**：保留旧 `fromString` 的多写法归一（`UPPER_SNAKE` /
//!   `kebab-case` / `PascalCase` 皆可），供 `.zk/hooks.toml` 的 `event` 键手写。

use std::fmt;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize, Serializer};

/// Hook 观测事件（外部副作用通知的触发点；见模块文档 H-01）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookEvent {
    /// 工具执行前（派发执行器之前，准入已放行）。
    PreToolExecution,
    /// 工具执行后（结果落库之后）。
    PostToolExecution,
    /// 会话开始。
    SessionStart,
    /// 会话结束。
    SessionEnd,
    /// Run 开始（用户消息落库、请求构建完成，`run_id` 已知）。
    RunStart,
    /// Run 结束（终态提交之后）。
    RunEnd,
    /// 消息发送（下行推送后的观测点）。
    MessageSent,
    /// 错误发生。
    ErrorOccurred,
}

impl HookEvent {
    /// 全部事件（`.zk/hooks.toml` 校验与索引初始化用）。
    pub const ALL: [Self; 8] = [
        Self::PreToolExecution,
        Self::PostToolExecution,
        Self::SessionStart,
        Self::SessionEnd,
        Self::RunStart,
        Self::RunEnd,
        Self::MessageSent,
        Self::ErrorOccurred,
    ];

    /// 规范字面量（`UPPER_SNAKE`，出线 / 环境变量注入用）。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreToolExecution => "PRE_TOOL_EXECUTION",
            Self::PostToolExecution => "POST_TOOL_EXECUTION",
            Self::SessionStart => "SESSION_START",
            Self::SessionEnd => "SESSION_END",
            Self::RunStart => "RUN_START",
            Self::RunEnd => "RUN_END",
            Self::MessageSent => "MESSAGE_SENT",
            Self::ErrorOccurred => "ERROR_OCCURRED",
        }
    }

    /// 从字符串解析（旧 `fromString` 的多写法归一：剥 `-`/`_` 后小写比对）。
    ///
    /// 空白 / 未知字面量返回 `None`（旧源抛 `IllegalArgumentException`；配置
    /// 加载侧据此 warn 并跳过该条 hook，不 fail-fast——外部通知非核心链路）。
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        let normalized = normalize(value);
        if normalized.is_empty() {
            return None;
        }
        Self::ALL
            .into_iter()
            .find(|event| normalize(event.as_str()) == normalized)
    }
}

/// 归一化：剥去 `-` / `_` 后转小写（`PRE_TOOL_EXECUTION` /
/// `pre-tool-execution` / `PreToolExecution` 皆折叠为 `pretoolexecution`）。
fn normalize(value: &str) -> String {
    value
        .chars()
        .filter(|c| *c != '-' && *c != '_')
        .flat_map(char::to_lowercase)
        .collect()
}

impl fmt::Display for HookEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for HookEvent {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for HookEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EventVisitor;
        impl Visitor<'_> for EventVisitor {
            type Value = HookEvent;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a hook event name (UPPER_SNAKE / kebab-case / PascalCase)")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<HookEvent, E> {
                HookEvent::parse(value)
                    .ok_or_else(|| E::custom(format!("unknown hook event: {value:?}")))
            }
        }
        deserializer.deserialize_str(EventVisitor)
    }
}

/// 单条 hook 声明（`.zk/hooks.toml` 的 `[[hook]]` 表项；见 Step 1 数据模型）。
///
/// `command` 与 `url` 二选一：`url` 存在 → HTTP POST 通知（经
/// [`crate::hook::HttpHookExecutor`] 走 SSRF 防护）；否则 `command` 存在 →
/// 本地命令通知（`sh -c`）。两者皆空的条目在加载期被丢弃并 warn。
#[derive(Debug, Clone, Deserialize)]
pub struct HookConfig {
    /// hook 名（日志定位 / 注销键）。
    pub name: String,
    /// 触发事件。
    pub event: HookEvent,
    /// Functional role. Security hooks fail closed; notification hooks remain
    /// isolated and cannot weaken admission.
    #[serde(default)]
    pub role: HookRole,
    /// Optional regular expression matched against the tool name.
    #[serde(default)]
    pub matcher: Option<String>,
    /// Lower values execute first.
    #[serde(default)]
    pub priority: i32,
    /// 本地命令（`sh -c` 执行；与 `url` 二选一）。
    #[serde(default)]
    pub command: Option<String>,
    /// HTTP 目标 URL（POST JSON；与 `command` 二选一）。
    #[serde(default)]
    pub url: Option<String>,
    /// 异步模式：`true` = `tokio::spawn` 不等待；`false` = 等待完成（见
    /// [`default_async_mode`]，缺省同步）。
    #[serde(default = "default_async_mode", rename = "async")]
    pub async_mode: bool,
    /// 本地命令等待上限秒数（HTTP 通道恒 10s，见 `HttpHookExecutor`）。
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

/// Hook participation role.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HookRole {
    /// Best-effort external notification; output is ignored.
    #[default]
    Notification,
    /// May return a modified input or a stable denial.
    Transform,
    /// May modify/deny and fails closed on execution or protocol errors.
    Security,
    /// Post-execution presentation notification.
    Presentation,
}

/// `async_mode` 缺省值（同步等待，与旧 hook 的默认阻塞语义一致）。
#[must_use]
pub const fn default_async_mode() -> bool {
    false
}

/// `timeout_secs` 缺省值（本地命令最多等 30s，见 Step 3）。
#[must_use]
pub const fn default_timeout_secs() -> u64 {
    30
}

impl HookConfig {
    /// Whether this hook applies to the supplied tool name.
    #[must_use]
    pub fn matches_tool(&self, tool_name: &str) -> bool {
        self.matcher.as_ref().is_none_or(|pattern| {
            regex::Regex::new(pattern).is_ok_and(|matcher| matcher.is_match(tool_name))
        })
    }

    /// Whether an execution or protocol failure must deny the operation.
    #[must_use]
    pub fn fails_closed(&self) -> bool {
        self.role == HookRole::Security
    }

    /// 是否为 HTTP 通道（`url` 优先于 `command`）。
    #[must_use]
    pub fn is_http(&self) -> bool {
        self.url.as_ref().is_some_and(|url| !url.trim().is_empty())
    }

    /// 是否为本地命令通道。
    #[must_use]
    pub fn is_command(&self) -> bool {
        !self.is_http()
            && self
                .command
                .as_ref()
                .is_some_and(|command| !command.trim().is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_upper_snake_kebab_and_pascal() {
        for literal in [
            "PRE_TOOL_EXECUTION",
            "pre-tool-execution",
            "PreToolExecution",
            "pretoolexecution",
        ] {
            assert_eq!(HookEvent::parse(literal), Some(HookEvent::PreToolExecution));
        }
        assert_eq!(HookEvent::parse("RUN_END"), Some(HookEvent::RunEnd));
        assert_eq!(
            HookEvent::parse("error-occurred"),
            Some(HookEvent::ErrorOccurred)
        );
    }

    #[test]
    fn parse_rejects_blank_and_unknown() {
        assert_eq!(HookEvent::parse(""), None);
        assert_eq!(HookEvent::parse("   "), None);
        assert_eq!(HookEvent::parse("__"), None);
        assert_eq!(HookEvent::parse("NOTIFICATION"), None);
    }

    #[test]
    fn all_events_have_unique_literals() {
        let mut seen = std::collections::HashSet::new();
        for event in HookEvent::ALL {
            assert!(seen.insert(event.as_str()), "duplicate literal {event}");
            assert_eq!(HookEvent::parse(event.as_str()), Some(event));
        }
        assert_eq!(seen.len(), 8);
    }

    #[test]
    fn serde_roundtrips_through_canonical_literal() {
        for event in HookEvent::ALL {
            let json = serde_json::to_string(&event).expect("serialize");
            assert_eq!(json, format!("\"{}\"", event.as_str()));
            let back: HookEvent = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, event);
        }
    }

    #[test]
    fn config_channel_classification() {
        let http = HookConfig {
            name: "h".to_owned(),
            event: HookEvent::RunStart,
            role: HookRole::Notification,
            matcher: None,
            priority: 0,
            command: Some("echo".to_owned()),
            url: Some("http://example.com".to_owned()),
            async_mode: false,
            timeout_secs: 30,
        };
        // url 优先：HTTP 通道判定为真，command 通道判定为假。
        assert!(http.is_http());
        assert!(!http.is_command());

        let command = HookConfig {
            name: "h".to_owned(),
            event: HookEvent::RunStart,
            role: HookRole::Notification,
            matcher: None,
            priority: 0,
            command: Some("echo hi".to_owned()),
            url: None,
            async_mode: true,
            timeout_secs: 5,
        };
        assert!(command.is_command());
        assert!(!command.is_http());
    }
}
