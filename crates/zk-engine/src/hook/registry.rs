//! Hook 注册表：按事件类型索引 hook 声明（Batch 8B Step 2）。
//!
//! 对照旧 `hook/HookRegistry.java`（178L）。语义偏离留痕：
//!
//! - **H-03 索引简化**：旧注册表值为 `HookRegistration`（含 role / matcher 正则 /
//!   priority / source），且对 `PRE_TOOL_USE` 禁 `PRESENTATION`、`POST_TOOL_USE`
//!   必须 `PRESENTATION`，并按 priority 升序返回——那是**进程内函数 hook** 参与
//!   准入与展示裁决的必要元数据（见 [`crate::hook::event`] H-01）。本端 hook 是
//!   外部副作用通知，无准入/展示语义，故索引值直接是 [`HookConfig`]，注册即入表、
//!   保持声明顺序（`.zk/hooks.toml` 内自上而下），无 priority / matcher。
//! - **配置源**：旧 hook 由代码注册（`register`）；本端由 `.zk/hooks.toml` 声明式
//!   加载（[`HookRegistry::load_from_dir`]），`register` 仍保留供编程式注入 / 测试。

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use super::event::{HookConfig, HookEvent, HookRole};

/// `.zk` 目录下的 hook 配置文件名。
pub const HOOKS_FILE_REL: &str = ".zk/hooks.toml";

/// `.zk/hooks.toml` 的顶层结构：`[[hook]]` 数组表。
#[derive(Debug, Default, Deserialize)]
struct HooksFile {
    /// 全部 hook 声明（`[[hook]]` 表项，缺省空）。
    #[serde(default)]
    hook: Vec<HookConfig>,
}

/// Hook 注册表：`HashMap<HookEvent, Vec<HookConfig>>`，按事件类型索引。
#[derive(Debug, Default, Clone)]
pub struct HookRegistry {
    by_event: HashMap<HookEvent, Vec<HookConfig>>,
    invalid_security_config: bool,
}

impl HookRegistry {
    /// 空注册表。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 从工作根目录加载 `.zk/hooks.toml`（不存在 → 空注册表）。
    ///
    /// 解析失败或单条 hook 无效（既无 `command` 又无 `url`）时 `warn!` 并跳过，
    /// **不** fail-fast——hook 是外部通知，配置错误不应拖垮服务启动。
    #[must_use]
    pub fn load_from_dir(root: &Path) -> Self {
        let path = root.join(HOOKS_FILE_REL);
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Self::new(),
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "hooks config metadata unavailable; skipping");
                return Self::new();
            }
        };
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            tracing::warn!(path = %path.display(), "hooks config must be a regular non-symlink file; skipping");
            return Self::new();
        }
        if metadata.len() > 256 * 1024 {
            tracing::warn!(path = %path.display(), "hooks config exceeds 256KiB; skipping");
            return Self::new();
        }
        let raw = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "hooks config unreadable; skipping");
                return Self::new();
            }
        };
        let file: HooksFile = match toml::from_str(&raw) {
            Ok(file) => file,
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "hooks config parse failed; skipping");
                return Self::new();
            }
        };
        let mut registry = Self::new();
        for config in file.hook {
            registry.register(config);
        }
        tracing::info!(
            path = %path.display(),
            count = registry.len(),
            "hooks loaded"
        );
        registry
    }

    /// 注册一条 hook（既无 `command` 又无 `url` 的无效声明被 `warn!` 丢弃）。
    pub fn register(&mut self, config: HookConfig) {
        if !config.is_http() && !config.is_command() {
            tracing::warn!(
                name = %config.name,
                event = %config.event,
                "hook has neither command nor url; skipping"
            );
            return;
        }
        if let Some(matcher) = config.matcher.as_deref()
            && let Err(error) = regex::Regex::new(matcher)
        {
            tracing::warn!(name = %config.name, %error, "hook matcher is invalid; skipping");
            if config.role == HookRole::Security {
                self.invalid_security_config = true;
            }
            return;
        }
        let hooks = self.by_event.entry(config.event).or_default();
        hooks.push(config);
        hooks.sort_by_key(|hook| hook.priority);
    }

    /// 注销指定名的全部 hook（跨所有事件），返回移除条数。
    pub fn unregister_by_name(&mut self, name: &str) -> usize {
        let mut removed = 0;
        for configs in self.by_event.values_mut() {
            let before = configs.len();
            configs.retain(|config| config.name != name);
            removed += before - configs.len();
        }
        removed
    }

    /// 某事件下的 hook 列表（声明顺序；无则空切片）。
    #[must_use]
    pub fn hooks_for(&self, event: HookEvent) -> &[HookConfig] {
        self.by_event.get(&event).map_or(&[], Vec::as_slice)
    }

    /// 注册的 hook 总数（跨所有事件）。
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_event.values().map(Vec::len).sum()
    }

    /// 是否无任何 hook。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Invalid security configuration must fail closed instead of silently
    /// disabling a declared protection.
    #[must_use]
    pub fn has_invalid_security_config(&self) -> bool {
        self.invalid_security_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_indexes_by_event_and_skips_invalid() {
        let mut registry = HookRegistry::new();
        registry.register(HookConfig {
            name: "cmd".to_owned(),
            event: HookEvent::PreToolExecution,
            role: HookRole::Notification,
            matcher: None,
            priority: 0,
            command: Some("echo hi".to_owned()),
            url: None,
            async_mode: false,
            timeout_secs: 30,
        });
        registry.register(HookConfig {
            name: "http".to_owned(),
            event: HookEvent::PreToolExecution,
            role: HookRole::Notification,
            matcher: None,
            priority: 0,
            command: None,
            url: Some("https://example.com/hook".to_owned()),
            async_mode: true,
            timeout_secs: 30,
        });
        // 既无 command 又无 url → 丢弃。
        registry.register(HookConfig {
            name: "empty".to_owned(),
            event: HookEvent::RunEnd,
            role: HookRole::Notification,
            matcher: None,
            priority: 0,
            command: None,
            url: None,
            async_mode: false,
            timeout_secs: 30,
        });

        assert_eq!(registry.len(), 2);
        assert_eq!(registry.hooks_for(HookEvent::PreToolExecution).len(), 2);
        assert!(registry.hooks_for(HookEvent::RunEnd).is_empty());
        assert!(!registry.is_empty());
    }

    #[test]
    fn unregister_by_name_removes_across_events() {
        let mut registry = HookRegistry::new();
        for event in [HookEvent::RunStart, HookEvent::RunEnd] {
            registry.register(HookConfig {
                name: "shared".to_owned(),
                event,
                role: HookRole::Notification,
                matcher: None,
                priority: 0,
                command: Some("echo".to_owned()),
                url: None,
                async_mode: false,
                timeout_secs: 30,
            });
        }
        assert_eq!(registry.unregister_by_name("shared"), 2);
        assert!(registry.is_empty());
    }

    #[test]
    fn matcher_and_priority_are_enforced_and_invalid_security_fails_closed() {
        let mut registry = HookRegistry::new();
        for (name, priority) in [("late", 20), ("early", -10)] {
            registry.register(HookConfig {
                name: name.to_owned(),
                event: HookEvent::PreToolExecution,
                role: HookRole::Transform,
                matcher: Some("^Read$".to_owned()),
                priority,
                command: Some("echo".to_owned()),
                url: None,
                async_mode: false,
                timeout_secs: 5,
            });
        }
        assert_eq!(
            registry
                .hooks_for(HookEvent::PreToolExecution)
                .iter()
                .map(|hook| hook.name.as_str())
                .collect::<Vec<_>>(),
            ["early", "late"]
        );
        assert!(registry.hooks_for(HookEvent::PreToolExecution)[0].matches_tool("Read"));
        assert!(!registry.hooks_for(HookEvent::PreToolExecution)[0].matches_tool("Bash"));

        registry.register(HookConfig {
            name: "broken-security".to_owned(),
            event: HookEvent::PreToolExecution,
            role: HookRole::Security,
            matcher: Some("[".to_owned()),
            priority: 0,
            command: Some("echo".to_owned()),
            url: None,
            async_mode: false,
            timeout_secs: 5,
        });
        assert!(registry.has_invalid_security_config());
    }

    #[test]
    fn load_from_dir_missing_file_is_empty() {
        let dir = std::env::temp_dir().join("zkcode-hooks-missing-XXXX");
        let registry = HookRegistry::load_from_dir(&dir);
        assert!(registry.is_empty());
    }

    #[test]
    fn load_from_dir_parses_hooks_toml() {
        let base = std::env::temp_dir().join(format!("zkcode-hooks-{}", std::process::id()));
        let zk = base.join(".zk");
        std::fs::create_dir_all(&zk).expect("mkdir");
        std::fs::write(
            zk.join("hooks.toml"),
            r#"
[[hook]]
name = "pre"
event = "pre-tool-execution"
command = "echo pre"

[[hook]]
name = "post"
event = "POST_TOOL_EXECUTION"
url = "https://example.com/hook"
async = true
timeout_secs = 5
"#,
        )
        .expect("write");
        let registry = HookRegistry::load_from_dir(&base);
        assert_eq!(registry.len(), 2);
        assert_eq!(registry.hooks_for(HookEvent::PreToolExecution).len(), 1);
        let post = &registry.hooks_for(HookEvent::PostToolExecution)[0];
        assert!(post.is_http());
        assert!(post.async_mode);
        assert_eq!(post.timeout_secs, 5);
        std::fs::remove_dir_all(&base).ok();
    }
}
