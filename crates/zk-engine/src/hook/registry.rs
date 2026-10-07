//! Bounded, descriptor-anchored loading of `.zk/hooks.toml`.
//!
//! Complete valid replacements are loaded atomically by `HookService`. Malformed
//! replacements cannot silently erase security rules. Event role constraints,
//! regex matchers and priorities are validated before any external execution.
//!
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
    /// Invalid configuration keeps the security gate closed. `HookService` hot
    /// reload retains the last fully valid replacement instead of resetting rules.
    #[must_use]
    pub fn load_from_dir(root: &Path) -> Self {
        if let Ok(registry) = Self::try_load_from_dir(root) {
            registry
        } else {
            tracing::error!(
                error_code = "HOOK_CONFIG_INVALID",
                "hooks configuration unavailable; security gate remains closed"
            );
            Self {
                invalid_security_config: true,
                ..Self::new()
            }
        }
    }

    /// Read a complete bounded configuration. Malformed replacements are not
    /// interpreted as an empty configuration by hot-reload callers.
    ///
    /// # Errors
    /// Unreadable, oversized, invalid or unsafe configurations return a diagnostic.
    pub fn try_load_from_dir(root: &Path) -> Result<Self, String> {
        use std::io::Read;
        let root = match root.canonicalize() {
            Ok(root) => root,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Self::new()),
            Err(error) => return Err(format!("HOOK_ROOT_UNAVAILABLE: {error}")),
        };
        let path = root.join(HOOKS_FILE_REL);
        let file = match zk_tools::safe_file::open_bound_regular(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Self::new()),
            Err(error) => return Err(format!("HOOK_CONFIG_READ_FAILED: {error}")),
        };
        let mut bytes = Vec::new();
        file.take(256 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        if bytes.len() > 256 * 1024 {
            return Err("HOOK_CONFIG_TOO_LARGE".into());
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| "HOOK_CONFIG_INVALID_UTF8")?;
        Self::try_parse(text)
    }

    /// Validate a bounded replacement before saving or executing any hook.
    ///
    /// # Errors
    /// Invalid roles, matchers, declarations or oversized text are rejected.
    pub fn try_parse(text: &str) -> Result<Self, String> {
        if text.len() > 256 * 1024 {
            return Err("HOOK_CONFIG_TOO_LARGE".into());
        }
        let file: HooksFile =
            toml::from_str(text).map_err(|error| format!("HOOK_CONFIG_INVALID: {error}"))?;
        if file.hook.len() > 128 {
            return Err("HOOK_CONFIG_TOO_MANY".into());
        }
        let mut registry = Self::new();
        let expected = file.hook.len();
        for config in file.hook {
            registry.register(config);
        }
        if registry.invalid_security_config || registry.len() != expected {
            return Err("HOOK_CONFIG_INVALID_ROLE_OR_COMMAND".into());
        }
        Ok(registry)
    }

    /// Register a validated declaration. Invalid entries mark the configuration
    /// unusable for security decisions instead of silently disappearing.
    pub fn register(&mut self, config: HookConfig) {
        let invalid_role = (config.event == HookEvent::PreToolExecution
            && config.role == HookRole::Presentation)
            || (config.event == HookEvent::PostToolExecution
                && matches!(config.role, HookRole::Security | HookRole::Transform))
            || (config.async_mode
                && matches!(
                    config.role,
                    HookRole::Security | HookRole::Transform | HookRole::Presentation
                ));
        if invalid_role
            || config.timeout_secs == 0
            || config.timeout_secs > 300
            || config.name.trim().is_empty()
        {
            self.invalid_security_config = true;
            tracing::error!(name=%config.name, "invalid hook role, timeout or name");
            return;
        }
        if !config.is_http() && !config.is_command() {
            self.invalid_security_config = true;
            tracing::error!(name=%config.name, event=%config.event, "hook has neither command nor url");
            return;
        }
        if let Some(matcher) = config.matcher.as_deref()
            && regex::Regex::new(matcher).is_err()
        {
            tracing::warn!(
                code = "HOOK_MATCHER_INVALID",
                "hook matcher is invalid; skipping"
            );
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

    /// Matching event declarations ordered by priority, with stable declaration ties.
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
