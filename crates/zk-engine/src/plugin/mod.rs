//! Plugin 系统（Batch 8G）：从 `.zk/plugins/` 扫描 `manifest.toml` 加载插件。
//!
//! 对照旧 `com.aicodeassistant.plugin` 包（15 文件，1405L）。
//!
//! # 与旧实现的刻意差异（留痕）
//!
//! - **PG-01 加载模型**：旧端用 `URLClassLoader` + SPI（`PluginExtension`）加载 JAR；
//!   本端改为 **manifest-only** 模型——每个插件目录含 `manifest.toml`，无字节码
//!   加载（Rust 无类 JVM 的热加载 `ClassLoader` 概念）。插件的「行为」通过声明
//!   hooks（接入已有 [`crate::hook::HookService`]）和 MCP servers 实现。
//! - **PG-02 简化范围**：旧 `PluginExtension` 可提供 commands / tools / hooks /
//!   MCP servers / LSP 五种组件；本端仅建模 hooks + MCP servers 声明（commands /
//!   tools 注册属后续 Phase，与旧 Java 的 SPI 动态发现语义等价但载体不同）。

pub mod loader;
pub mod manager;

use std::path::PathBuf;

use crate::hook::HookEvent;

/// 插件清单（旧 `PluginManifest` record 的 Rust 等价物）。
///
/// 字段集对齐旧 record：`name` / `version` / `description` / `hooks` / `source` /
/// `isBuiltin`。`author` 为新增字段（TOML 生态习惯）；`repository` 未移植
/// （旧端字段但本端无 marketplace 消费方）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginManifest {
    /// 插件名称（唯一标识，旧 `name()`）。
    pub name: String,
    /// 语义版本号（旧 `version()`）。
    pub version: String,
    /// 作者（TOML 生态新增；旧端无对应字段）。
    pub author: String,
    /// 一行说明（旧 `description()`）。
    pub description: String,
    /// 声明的 hook 事件列表（旧 `hooks` Map 的键集）。
    pub hooks: Vec<HookEvent>,
    /// 是否启用（旧 `enabled`）。
    pub enabled: bool,
}

impl PluginManifest {
    /// 最小清单（旧 `PluginManifest.of(name, version, description)`）。
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            author: String::new(),
            description: description.into(),
            hooks: Vec::new(),
            enabled: true,
        }
    }

    /// 名称非空且 ≤ 64 字符，仅含字母/数字/下划线/中划线。
    #[must_use]
    pub fn is_valid_name(&self) -> bool {
        !self.name.is_empty()
            && self.name.len() <= 64
            && self
                .name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    }

    /// 是否声明了指定 hook 事件。
    #[must_use]
    pub fn has_hook(&self, event: HookEvent) -> bool {
        self.hooks.contains(&event)
    }
}

/// 已加载插件（旧 `LoadedPlugin` record 的 Rust 等价物）。
///
/// 字段集简化：旧 record 含 `extension` / `commands` / `tools` / `hooks` /
/// `mcpServers` 五个组件集合；本端仅保留 manifest + path + source + enabled
/// （组件注册由 [`manager::PluginManager`] 在 install 阶段驱动）。
#[derive(Clone, Debug)]
pub struct Plugin {
    /// 插件名称（与 manifest.name 同值，索引键）。
    pub name: String,
    /// 插件清单。
    pub manifest: PluginManifest,
    /// 插件目录路径（旧 `path`；内置插件为 `None`）。
    pub path: Option<PathBuf>,
    /// 来源标识（旧 `source`：`"local"` / `"builtin"`）。
    pub source: String,
    /// 是否内置（旧 `isBuiltin`）。
    pub is_builtin: bool,
    /// 是否启用（旧 `enabled`）。
    pub enabled: bool,
}

impl Plugin {
    /// 从清单创建本地插件（旧 `LoadedPlugin.local(name, path, ext, enabled)`）。
    #[must_use]
    pub fn from_manifest(manifest: PluginManifest, path: PathBuf) -> Self {
        let enabled = manifest.enabled;
        Self {
            name: manifest.name.clone(),
            manifest,
            path: Some(path),
            source: "local".into(),
            is_builtin: false,
            enabled,
        }
    }

    /// 插件信息摘要（供 REST 列表端点序列化）。
    #[must_use]
    pub fn to_info(&self) -> PluginInfo {
        PluginInfo {
            name: self.name.clone(),
            version: self.manifest.version.clone(),
            author: self.manifest.author.clone(),
            description: self.manifest.description.clone(),
            source: self.source.clone(),
            is_builtin: self.is_builtin,
            enabled: self.enabled,
            hooks: self
                .manifest
                .hooks
                .iter()
                .map(|e| e.as_str().to_owned())
                .collect(),
        }
    }
}

/// 插件信息 DTO（REST `/api/plugins` 列表元素）。
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PluginInfo {
    /// 插件名称。
    pub name: String,
    /// 版本号。
    pub version: String,
    /// 作者。
    pub author: String,
    /// 说明。
    pub description: String,
    /// 来源（`local` / `builtin`）。
    pub source: String,
    /// 是否内置。
    pub is_builtin: bool,
    /// 是否启用。
    pub enabled: bool,
    /// 声明的 hook 事件名列表。
    pub hooks: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::PluginManifest;
    use crate::hook::HookEvent;

    #[test]
    fn manifest_new_defaults_to_enabled_with_no_hooks() {
        let m = PluginManifest::new("test", "1.0.0", "A test plugin");
        assert_eq!(m.name, "test");
        assert_eq!(m.version, "1.0.0");
        assert!(m.enabled);
        assert!(m.hooks.is_empty());
        assert!(m.is_valid_name());
    }

    #[test]
    fn invalid_names_rejected() {
        let mut m = PluginManifest::new("", "1.0", "x");
        assert!(!m.is_valid_name(), "empty name");
        m.name = "has space".into();
        assert!(!m.is_valid_name(), "space in name");
        m.name = "a".repeat(65);
        assert!(!m.is_valid_name(), "too long");
    }

    #[test]
    fn has_hook_checks_declared_events() {
        let mut m = PluginManifest::new("h", "1.0", "hooks");
        m.hooks = vec![HookEvent::SessionStart, HookEvent::RunEnd];
        assert!(m.has_hook(HookEvent::SessionStart));
        assert!(!m.has_hook(HookEvent::ErrorOccurred));
    }
}
