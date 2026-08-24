//! 插件加载器（Batch 8G Step 1）：扫描 `.zk/plugins/` 加载 `manifest.toml`。
//!
//! 对照旧 `PluginLoader.java`（332L）。
//!
//! # 与旧实现的刻意差异（留痕）
//!
//! - **PG-03 来源简化**：旧端三类来源（classpath SPI / 本地 JAR / marketplace），
//!   本端仅支持本地目录扫描（`.zk/plugins/{name}/manifest.toml`）。JAR 加载
//!   在 Rust 生态无对应概念；marketplace 属后续 Phase。
//! - **PG-04 校验简化**：旧端 `validateJar` 检查文件大小 / SPI 注册 / Manifest
//!   属性；本端仅检查 TOML 可解析性与 `name` 字段存在性。

use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::hook::HookEvent;

use super::{Plugin, PluginManifest};

/// 插件目录名（旧 `~/.zhikun/plugins/`，本端统一用 `.zk/plugins/`）。
const PLUGINS_DIR: &str = "plugins";

/// manifest 文件名（旧端 JAR 内 `META-INF` 约定；本端为目录内平铺文件）。
const MANIFEST_FILE: &str = "manifest.toml";

/// TOML manifest 的原始反序列化结构（所有字段可选，宽容解析）。
#[derive(Deserialize)]
struct RawManifest {
    name: Option<String>,
    #[serde(default = "default_version")]
    version: String,
    #[serde(default)]
    author: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    hooks: Vec<String>,
    #[serde(default = "default_true")]
    enabled: bool,
}

fn default_version() -> String {
    "0.1.0".into()
}

fn default_true() -> bool {
    true
}

/// 从基础目录加载插件（旧 `PluginLoader.loadPlugins()`）。
///
/// `base_dir` 通常是项目根目录或 `~/.zk`；函数在其下查找 `plugins/`
/// 子目录并逐个扫描。
///
/// # Errors
/// 仅返回 I/O 级别的严重错误（如 `plugins/` 存在但不可读）。单个插件
/// 目录的解析失败仅 `warn` 不中断，与旧端 `catch (Exception e)` 行为一致。
pub fn load_plugins(base_dir: &Path) -> Result<Vec<Plugin>, std::io::Error> {
    let plugins_dir = base_dir.join(PLUGINS_DIR);
    if !plugins_dir.exists() {
        return Ok(Vec::new());
    }
    if !plugins_dir.is_dir() {
        tracing::warn!(
            path = %plugins_dir.display(),
            "plugins path exists but is not a directory"
        );
        return Ok(Vec::new());
    }

    let mut plugins = Vec::new();
    for entry in fs::read_dir(&plugins_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let manifest_path = path.join(MANIFEST_FILE);
        if !manifest_path.is_file() {
            tracing::debug!(
                dir = %path.display(),
                "skipping directory without manifest.toml"
            );
            continue;
        }
        match load_single_plugin(&manifest_path, &path) {
            Ok(plugin) => plugins.push(plugin),
            Err(err) => {
                tracing::warn!(
                    path = %manifest_path.display(),
                    error = %err,
                    "failed to load plugin manifest"
                );
            }
        }
    }

    tracing::info!(count = plugins.len(), "loaded plugins from disk");
    Ok(plugins)
}

/// 加载单个插件清单（旧 `loadLocalPlugin`）。
fn load_single_plugin(manifest_path: &Path, plugin_dir: &Path) -> Result<Plugin, String> {
    let content = fs::read_to_string(manifest_path).map_err(|e| format!("read manifest: {e}"))?;
    parse_manifest(&content)
        .map(|manifest| Plugin::from_manifest(manifest, plugin_dir.to_path_buf()))
}

/// 解析 TOML manifest 文本为 [`PluginManifest`]。
///
/// # Errors
/// TOML 解析失败或缺失 `name` 字段时返回错误字符串。
pub fn parse_manifest(content: &str) -> Result<PluginManifest, String> {
    let raw: RawManifest =
        toml::from_str(content).map_err(|e| format!("parse manifest TOML: {e}"))?;

    let name = raw
        .name
        .ok_or_else(|| "manifest missing required 'name' field".to_owned())?;

    let hooks: Vec<HookEvent> = raw
        .hooks
        .iter()
        .filter_map(|s| {
            if let Some(event) = HookEvent::parse(s) {
                Some(event)
            } else {
                tracing::warn!(hook = %s, plugin = %name, "unknown hook event, skipping");
                None
            }
        })
        .collect();

    Ok(PluginManifest {
        name,
        version: raw.version,
        author: raw.author,
        description: raw.description,
        hooks,
        enabled: raw.enabled,
    })
}

/// 解析 manifest 文件路径（从基础目录 + 插件名定位）。
///
/// 供 `PluginManager.install(path)` 调用：外部传入插件目录路径，此函数
/// 在其下寻找 `manifest.toml` 并解析。
/// # Errors
///
/// Returns `Err(String)` if the `manifest.toml` is missing, malformed,
/// or fails validation (invalid name, duplicate hook, etc.).
pub fn load_from_path(plugin_dir: &Path) -> Result<Plugin, String> {
    let manifest_path = plugin_dir.join(MANIFEST_FILE);
    if !manifest_path.is_file() {
        return Err(format!(
            "no {} found in {}",
            MANIFEST_FILE,
            plugin_dir.display()
        ));
    }
    load_single_plugin(&manifest_path, plugin_dir)
}

#[cfg(test)]
mod tests {
    use super::parse_manifest;
    use crate::hook::HookEvent;

    #[test]
    fn parse_full_manifest() {
        let toml = r#"
name = "hello-plugin"
version = "1.0.0"
author = "test"
description = "A hello world plugin"
hooks = ["session_start", "run_end"]
enabled = true
"#;
        let manifest = parse_manifest(toml).expect("valid");
        assert_eq!(manifest.name, "hello-plugin");
        assert_eq!(manifest.version, "1.0.0");
        assert_eq!(manifest.author, "test");
        assert_eq!(manifest.hooks.len(), 2);
        assert_eq!(manifest.hooks[0], HookEvent::SessionStart);
        assert_eq!(manifest.hooks[1], HookEvent::RunEnd);
        assert!(manifest.enabled);
    }

    #[test]
    fn parse_minimal_manifest() {
        let toml = r#"name = "minimal""#;
        let manifest = parse_manifest(toml).expect("valid");
        assert_eq!(manifest.name, "minimal");
        assert_eq!(manifest.version, "0.1.0");
        assert!(manifest.enabled);
        assert!(manifest.hooks.is_empty());
    }

    #[test]
    fn missing_name_returns_error() {
        let toml = r#"version = "1.0""#;
        let err = parse_manifest(toml).unwrap_err();
        assert!(err.contains("name"), "unexpected error: {err}");
    }

    #[test]
    fn unknown_hooks_are_skipped() {
        let toml = r#"
name = "test"
hooks = ["session_start", "bogus_event", "run_end"]
"#;
        let manifest = parse_manifest(toml).expect("valid");
        assert_eq!(manifest.hooks.len(), 2, "bogus_event skipped");
    }

    #[test]
    fn invalid_toml_returns_error() {
        let err = parse_manifest("not valid toml {{{").unwrap_err();
        assert!(err.contains("parse"), "unexpected error: {err}");
    }
}
