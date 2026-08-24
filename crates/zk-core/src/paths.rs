//! 路径解析——用户态/项目态配置目录与暂存区的唯一事实源。
//!
//! 全 workspace 的目录名字面量只在本模块声明一次，上层 crate 一律经函数或
//! 常量消费，禁止自行 `join(".zk")`。缓存策略：`$HOME` 在进程生命周期内不变，
//! 故 [`user_config_dir`] 用 [`OnceLock`] 只解析一次；项目态函数取 `cwd`
//! 入参（同一进程可服务多个工作区），故不缓存。

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// 当前配置目录名——用户态 `~/.zk/` 与项目态 `{cwd}/.zk/` 共用。
pub const CONFIG_DIR_NAME: &str = ".zk";

/// 旧版配置目录名。
///
/// **这是全 workspace 唯一允许出现该字面量的位置**：迁移器（[`crate::migrate`]）
/// 需要它定位迁移源，授权门禁需要它把旧目录继续留在保护名单里——迁移是拷贝
/// 而非移动，旧目录在用户机器上依然存在，删除保护面等于安全回归。
pub const LEGACY_CONFIG_DIR_NAME: &str = ".zhikun";

/// 项目级配置文件名（危险文件名单的当前保护面）。
pub const CONFIG_FILE_NAME: &str = ".zk.json";

/// 旧版项目级配置文件名（危险文件名单的遗留保护面，理由同
/// [`LEGACY_CONFIG_DIR_NAME`]）。
pub const LEGACY_CONFIG_FILE_NAME: &str = ".zhikun.json";

/// 暂存区子目录名（相对配置目录）。
pub const SCRATCHPAD_DIR_NAME: &str = "scratchpad";

/// `HOME` 不可用时的回落基址。
///
/// 与 `zk-server` 侧车 UDS 默认路径同一约定（`HOME` 缺失回落 `/tmp`），保证
/// 无 home 的运行环境（容器 / launchd 早期阶段）下路径依旧绝对且可写。
const HOME_FALLBACK: &str = "/tmp";

/// 进程级缓存的用户配置目录。
static USER_CONFIG_DIR: OnceLock<PathBuf> = OnceLock::new();

/// 用户全局配置目录：`$HOME/.zk/`。
///
/// 首次调用解析 `$HOME` 并缓存，后续调用直接克隆缓存值——因此进程启动后修改
/// `HOME` 不会改变返回值（有意为之：避免同一进程内路径身份漂移）。
#[must_use]
pub fn user_config_dir() -> PathBuf {
    USER_CONFIG_DIR
        .get_or_init(|| home_dir().join(CONFIG_DIR_NAME))
        .clone()
}

/// 旧版用户全局配置目录：`$HOME/.zhikun/`（仅迁移源，不做缓存）。
#[must_use]
pub fn legacy_user_config_dir() -> PathBuf {
    home_dir().join(LEGACY_CONFIG_DIR_NAME)
}

/// 项目配置目录：`{cwd}/.zk/`。
#[must_use]
pub fn project_config_dir(cwd: &Path) -> PathBuf {
    cwd.join(CONFIG_DIR_NAME)
}

/// 工作区暂存区：`{cwd}/.zk/scratchpad/`。
///
/// 与 `zk-authz` `SystemScratchpadPathPolicy` 默认根、`zk-server`
/// `ZK_SCRATCHPAD_SYSTEM_ROOT` 缺省、系统提示的「临时目录」段三处同源。
#[must_use]
pub fn scratchpad_dir(cwd: &Path) -> PathBuf {
    project_config_dir(cwd).join(SCRATCHPAD_DIR_NAME)
}

/// 解析 `$HOME`：空白值与缺失一律回落 [`HOME_FALLBACK`]。
fn home_dir() -> PathBuf {
    std::env::var("HOME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map_or_else(|| PathBuf::from(HOME_FALLBACK), PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dir_names_are_stable_wire_level_constants() {
        assert_eq!(CONFIG_DIR_NAME, ".zk");
        assert_eq!(SCRATCHPAD_DIR_NAME, "scratchpad");
        assert_eq!(CONFIG_FILE_NAME, ".zk.json");
        // 旧名不得被「顺手改掉」：迁移源与遗留保护面都锚在它上面。
        assert_eq!(LEGACY_CONFIG_DIR_NAME, ".zhikun");
        assert_eq!(LEGACY_CONFIG_FILE_NAME, ".zhikun.json");
        assert_ne!(CONFIG_DIR_NAME, LEGACY_CONFIG_DIR_NAME);
    }

    #[test]
    fn project_config_dir_appends_config_dir_name() {
        let dir = project_config_dir(Path::new("/Users/dev/project"));
        assert_eq!(dir, PathBuf::from("/Users/dev/project/.zk"));
    }

    #[test]
    fn project_config_dir_keeps_relative_input_relative() {
        // 相对 cwd（如 `ZK_DB_PATH` 的相对语义）不得被隐式绝对化。
        assert_eq!(project_config_dir(Path::new("")), PathBuf::from(".zk"));
        assert_eq!(
            project_config_dir(Path::new("workspace")),
            PathBuf::from("workspace/.zk")
        );
    }

    #[test]
    fn scratchpad_dir_nests_under_project_config_dir() {
        let cwd = Path::new("/Users/dev/project");
        assert_eq!(
            scratchpad_dir(cwd),
            PathBuf::from("/Users/dev/project/.zk/scratchpad")
        );
        assert!(scratchpad_dir(cwd).starts_with(project_config_dir(cwd)));
    }

    #[test]
    fn user_config_dir_is_absolute_and_cached() {
        let first = user_config_dir();
        let second = user_config_dir();
        assert_eq!(first, second, "OnceLock 必须返回同一路径");
        assert!(first.is_absolute(), "{first:?} 必须绝对（HOME 回落亦绝对）");
        assert_eq!(
            first.file_name().map(std::ffi::OsStr::to_string_lossy),
            Some(CONFIG_DIR_NAME.into())
        );
    }

    #[test]
    fn user_and_legacy_user_dirs_share_home_but_differ_in_leaf() {
        let current = user_config_dir();
        let legacy = legacy_user_config_dir();
        assert_eq!(current.parent(), legacy.parent(), "同一 $HOME 基址");
        assert_ne!(current, legacy);
        assert_eq!(
            legacy.file_name().map(std::ffi::OsStr::to_string_lossy),
            Some(LEGACY_CONFIG_DIR_NAME.into())
        );
    }

    #[test]
    fn home_dir_matches_env_or_falls_back() {
        let resolved = home_dir();
        match std::env::var("HOME") {
            Ok(home) if !home.trim().is_empty() => assert_eq!(resolved, PathBuf::from(home)),
            _ => assert_eq!(resolved, PathBuf::from(HOME_FALLBACK)),
        }
    }
}
