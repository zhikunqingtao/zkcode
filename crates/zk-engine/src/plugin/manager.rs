//! 插件管理器（Batch 8G Step 2）：install / uninstall / reload / list。
//!
//! 对照旧 `PluginManager.java`（361L）。
//!
//! # 与旧实现的刻意差异（留痕）
//!
//! - **PG-05 并发模型**：旧端用 `ReentrantReadWriteLock` + `ConcurrentHashMap`；
//!   本端用 [`dashmap::DashMap`]（已在 workspace 依赖中）——读写并发更优，
//!   无需显式锁粒度管理。
//! - **PG-06 组件注册简化**：旧端 install 阶段将 plugin 的 commands / tools /
//!   hooks / MCP servers 逐一注册到四个全局注册表；本端仅记录插件元数据，
//!   hook 集成由调用方（`AppState`）按需驱动。
//! - **PG-07 生命周期**：旧端 `@PreDestroy` 关闭 `hookExecutor`；本端无需
//!   独立线程池（Rust 无 JVM 虚拟线程 executor 概念），`Drop` 即可。

use std::path::{Path, PathBuf};

use dashmap::DashMap;

use super::loader;
use super::{Plugin, PluginInfo};

/// 插件管理器（旧 `PluginManager` 的 Rust 等价物）。
///
/// 进程内单例（由 `AppState` 持有 `Arc<PluginManager>`），管理全部已加载
/// 插件的生命周期。线程安全：内部 [`DashMap`] 提供细粒度并发读写。
pub struct PluginManager {
    /// 插件名 → 已加载插件（旧 `loadedPlugins: ConcurrentHashMap`）。
    plugins: DashMap<String, Plugin>,
    /// 插件扫描基础目录（旧 `~/.zhikun`）。
    base_dir: PathBuf,
}

impl PluginManager {
    /// 创建管理器并指定基础目录。
    ///
    /// `base_dir` 是 `.zk` 目录的父目录（如项目根或 `~`），加载器在其下
    /// 查找 `plugins/` 子目录。
    #[must_use]
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            plugins: DashMap::new(),
            base_dir,
        }
    }

    /// 从磁盘扫描并加载全部插件（旧 `initialize()`）。
    ///
    /// 应在启动期调用一次。重复调用会叠加（不先清空），如需全量重载
    /// 请先调用 [`reload`](Self::reload)。
    pub fn initialize(&self) {
        match loader::load_plugins(&self.base_dir) {
            Ok(plugins) => {
                for plugin in plugins {
                    self.plugins.insert(plugin.name.clone(), plugin);
                }
                tracing::info!(count = self.plugins.len(), "plugin manager initialized");
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    base_dir = %self.base_dir.display(),
                    "failed to scan plugins directory"
                );
            }
        }
    }

    /// 安装插件（旧 `installPlugin(path)`）。
    ///
    /// 从指定路径加载 manifest 并注册到管理器。同名插件会被覆盖（旧行为：
    /// `loadedPlugins.put(name, plugin)`）。
    ///
    /// # Errors
    /// manifest 解析失败或路径不存在时返回错误字符串。
    pub fn install(&self, path: &Path) -> Result<PluginInfo, String> {
        let plugin = loader::load_from_path(path)?;
        let info = plugin.to_info();
        tracing::info!(name = %plugin.name, "plugin installed");
        self.plugins.insert(plugin.name.clone(), plugin);
        Ok(info)
    }

    /// 卸载插件（旧 `unloadPlugin(name)`）。
    ///
    /// 返回是否成功移除（旧 `loadedPlugins.remove(name) != null`）。
    pub fn uninstall(&self, name: &str) -> bool {
        let removed = self.plugins.remove(name).is_some();
        if removed {
            tracing::info!(name = %name, "plugin uninstalled");
        }
        removed
    }

    /// 全量重载（旧 `reloadPlugins()`）。
    ///
    /// 清空当前注册表后重新扫描磁盘。
    pub fn reload(&self) {
        self.plugins.clear();
        self.initialize();
        tracing::info!(count = self.plugins.len(), "plugins reloaded");
    }

    /// 列出全部已加载插件信息（旧 `getLoadedPlugins()`）。
    ///
    /// 返回按名称排序的列表（[`DashMap::iter`] 无序，此处显式排序以保证
    /// REST 端点的确定性输出）。
    #[must_use]
    pub fn list(&self) -> Vec<PluginInfo> {
        let mut infos: Vec<PluginInfo> = self
            .plugins
            .iter()
            .map(|entry| entry.value().to_info())
            .collect();
        infos.sort_by(|a, b| a.name.cmp(&b.name));
        infos
    }

    /// 按名称获取插件信息。
    #[must_use]
    pub fn get(&self, name: &str) -> Option<PluginInfo> {
        self.plugins.get(name).map(|entry| entry.value().to_info())
    }

    /// 已加载插件数量。
    #[must_use]
    pub fn count(&self) -> usize {
        self.plugins.len()
    }

    /// 是否已加载指定插件。
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.plugins.contains_key(name)
    }
}

impl Default for PluginManager {
    fn default() -> Self {
        Self::new(PathBuf::from(".zk"))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::PluginManager;

    /// 在 OS temp 目录下创建唯一子目录（用 test name 区分避免并行冲突）。
    fn tmp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join("zk-plugin-tests")
            .join(format!("{label}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create tmp dir");
        dir
    }

    fn setup_plugin_dir(base: &std::path::Path, name: &str, toml: &str) {
        let plugin_dir = base.join("plugins").join(name);
        fs::create_dir_all(&plugin_dir).expect("create dir");
        fs::write(plugin_dir.join("manifest.toml"), toml).expect("write manifest");
    }

    #[test]
    fn new_manager_is_empty() {
        let pm = PluginManager::default();
        assert_eq!(pm.count(), 0);
        assert!(pm.list().is_empty());
    }

    #[test]
    fn initialize_loads_plugins_from_disk() {
        let tmp = tmp_dir("init");
        setup_plugin_dir(
            &tmp,
            "hello",
            "name = \"hello\"\nversion = \"1.0.0\"\ndescription = \"Hi\"",
        );
        setup_plugin_dir(&tmp, "world", "name = \"world\"\nversion = \"2.0.0\"");

        let pm = PluginManager::new(tmp);
        pm.initialize();
        assert_eq!(pm.count(), 2);
        assert!(pm.contains("hello"));
        assert!(pm.contains("world"));
    }

    #[test]
    fn install_and_uninstall() {
        let tmp = tmp_dir("install");
        let plugin_dir = tmp.join("my-plugin");
        fs::create_dir_all(&plugin_dir).expect("create dir");
        fs::write(
            plugin_dir.join("manifest.toml"),
            "name = \"my-plugin\"\nversion = \"0.1.0\"",
        )
        .expect("write manifest");

        let pm = PluginManager::default();
        let info = pm.install(&plugin_dir).expect("install");
        assert_eq!(info.name, "my-plugin");
        assert_eq!(pm.count(), 1);

        assert!(pm.uninstall("my-plugin"));
        assert_eq!(pm.count(), 0);
        assert!(!pm.uninstall("my-plugin"), "double uninstall");
    }

    #[test]
    fn reload_clears_and_rescans() {
        let tmp = tmp_dir("reload");
        setup_plugin_dir(&tmp, "alpha", "name = \"alpha\"\nversion = \"1.0.0\"");

        let pm = PluginManager::new(tmp.clone());
        pm.initialize();
        assert_eq!(pm.count(), 1);

        setup_plugin_dir(&tmp, "beta", "name = \"beta\"\nversion = \"1.0.0\"");
        pm.reload();
        assert_eq!(pm.count(), 2);
    }

    #[test]
    fn list_returns_sorted_infos() {
        let tmp = tmp_dir("sorted");
        setup_plugin_dir(&tmp, "zebra", "name = \"zebra\"");
        setup_plugin_dir(&tmp, "alpha", "name = \"alpha\"");

        let pm = PluginManager::new(tmp);
        pm.initialize();
        let list = pm.list();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].name, "alpha");
        assert_eq!(list[1].name, "zebra");
    }

    #[test]
    fn install_missing_path_returns_error() {
        let pm = PluginManager::default();
        let err = pm
            .install(std::path::Path::new("/nonexistent/dir"))
            .unwrap_err();
        assert!(err.contains("manifest"), "unexpected: {err}");
    }
}
