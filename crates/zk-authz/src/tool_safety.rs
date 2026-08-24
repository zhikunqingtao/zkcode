//! 工具参数安全守卫（§9.3）——与授权系统**正交**的第二道纵深防护。
//!
//! 逐字对照 `backend/src/main/java/com/aicodeassistant/service/ToolSafetyGuard.java`
//! （225 行）。Java 侧 Javadoc 明确它与权限系统正交：权限系统回答「用户允不
//! 允许」，本守卫回答「**调用参数本身是否安全**」。旧类三层分工如下，本模块
//! 只承接 Rust 侧此前**缺失**的那一层，其余两层已由既有模块以更严格的实现覆盖：
//!
//! | 旧 `ToolSafetyGuard` 层 | 旧源 | Rust 权威实现 | 本模块 |
//! |---|---|---|---|
//! | ① 路径安全（realpath / 符号链接 / 沙箱边界 / 黑名单 / 挂载点） | L34-158 | [`crate::path_security`]（Layer 1-8 + 符号链接 + 边界 + 敏感文件） | 不重复 |
//! | ② 命令安全（危险命令正则模式匹配） | L160-195 | zk-tools `bash::security`（AST 解析，远严于正则）+ [`crate::path_security::PathSecurityService::check_dangerous_removal`] | 不重复 |
//! | ③ **环境安全（子进程敏感环境变量清理）** | L197-223 | —（迁移前 grep `ToolSafety` / `SENSITIVE_ENV_VARS` 零命中） | ✅ 本模块补齐 |
//!
//! 另配套 scratchpad 写入边界：旧 `security/SystemScratchpadPathPolicy.java`
//! 已由 [`crate::path_security::SystemScratchpadPathPolicy`] 逐字移植。本模块只
//! 提供一层**薄装配**——经 [`zk_core::paths::scratchpad_dir`] 把工作区根解析为
//! `.zk/scratchpad/` 后委托该策略，不重复实现任何路径规范化逻辑。
//!
//! # 正交性铁律
//!
//! 「权限已放行 ≠ 参数安全」是 Java 侧的显式分层设计。环境清理与权限判定互不
//! 替代：即便某次 `Bash` 调用已获授权，子进程仍不得继承 `AWS_SECRET_ACCESS_KEY`
//! 等敏感变量。本模块不做任何路径/命令判定（那是 `path_security` 与
//! `bash::security` 的职责），只治理「参数（环境 / scratchpad 目标）本身」。
//!
//! # 权威源约束
//!
//! [`SENSITIVE_ENV_VARS`] 是**清单的唯一权威源**。子进程 env 的物理执行点在
//! `zk-tools::process` 的 `Command` 构造处，那里内联同一 10 项做无条件
//! `env_remove`；zk-server 装配根级漂移锁测试断言两处集合恒等，永不分叉
//! （见 `crates/zk-server/tests/tool_safety_env_baseline.rs`）。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::path_security::SystemScratchpadPathPolicy;

/// 需要从子进程环境中清除的敏感变量——逐字对照 `ToolSafetyGuard.java:200-206`
/// 的 `SENSITIVE_ENV_VARS`（`Set.of(...)`，键唯一、无序；本 slice 保持同样
/// 10 项，顺序不影响 `contains` / 逐项 `remove` 语义）。
///
/// 旧源以**黑名单移除**语义使用（`sanitizeProcessEnvironment` 逐项 `env.remove`），
/// 而非白名单保留：未列入本表的变量（`PATH` / `HOME` 等）原样保留。
pub const SENSITIVE_ENV_VARS: &[&str] = &[
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "NPM_TOKEN",
    "DOCKER_PASSWORD",
    "DATABASE_PASSWORD",
    "DB_PASSWORD",
    "PRIVATE_KEY",
    "SECRET_KEY",
];

/// 工具参数安全守卫——环境安全层（旧 `ToolSafetyGuard` 的 §环境安全）。
///
/// 无状态：敏感变量清单为编译期常量。以零尺寸类型承载，供执行链在构造子进程
/// 环境前调用（见 [`Self::sensitive_env_vars`] 与 [`Self::sanitize_environment`]）。
#[derive(Debug, Clone, Copy, Default)]
pub struct ToolSafetyGuard;

impl ToolSafetyGuard {
    /// 构造守卫（无依赖）。
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// 获取需要从子进程环境中清除的敏感变量集合——对照
    /// `ToolSafetyGuard.java:208-213`（`getSensitiveEnvVars`）。
    ///
    /// 返回 `BTreeSet`（确定性字典序）而非旧源无序 `Set`：集合语义等价，
    /// 顺序在结果上不可观测，但便于测试断言与日志稳定输出。
    #[must_use]
    pub fn sensitive_env_vars(&self) -> BTreeSet<&'static str> {
        SENSITIVE_ENV_VARS.iter().copied().collect()
    }

    /// 判定某环境变量名是否属敏感黑名单。
    ///
    /// **大小写敏感**——逐字对齐旧源 `Set<String>.contains` 语义（`env.remove`
    /// 亦大小写敏感）：进程环境变量名在 POSIX 下大小写有别，故不做归一化。
    #[must_use]
    pub fn is_sensitive_env_var(&self, name: &str) -> bool {
        SENSITIVE_ENV_VARS.contains(&name)
    }

    /// 就地清理一份环境映射——对照 `ToolSafetyGuard.java:215-223`
    /// （`sanitizeProcessEnvironment`：`SENSITIVE_ENV_VARS.forEach(env::remove)`）。
    ///
    /// 旧源直接作用于 `ProcessBuilder.environment()` 这一 `Map<String,String>`。
    /// zk-authz 是纯策略 crate，不得依赖 `tokio::process` / `std::process`（会
    /// 引入执行面耦合），故本方法作用于调用方提供的映射；实际子进程 `Command`
    /// 的清理由 zk-tools 侧在 spawn 处对同一 10 项调 `Command::env_remove` 完成
    /// （同源常量 + 漂移锁测试保证语义永不分叉）。
    pub fn sanitize_environment(&self, env: &mut BTreeMap<String, String>) {
        for var in SENSITIVE_ENV_VARS {
            env.remove(*var);
        }
    }
}

/// scratchpad 写入边界——仅允许写入工作区 `.zk/scratchpad/`。
///
/// 对应旧 `SystemScratchpadPathPolicy` 的可写边界语义（工具/协调者只应把临时
/// 产物写入暂存区）。本类型**不重复**任何路径规范化：它经
/// [`zk_core::paths::scratchpad_dir`] 解析工作区暂存区根，再委托
/// [`SystemScratchpadPathPolicy`]（已含 canonical 包含判定、缺失祖先解析、
/// 符号链接改绑失败关闭）。与 `path_security` 的其余层正交——那些层治理
/// 「路径是否越出项目 / 命中敏感黑名单」，本边界只治理「目标是否落在 scratchpad」。
#[derive(Debug, Clone)]
pub struct ScratchpadWriteBoundary {
    policy: SystemScratchpadPathPolicy,
}

impl ScratchpadWriteBoundary {
    /// 以工作区根装配：暂存区根 = `{workspace_root}/.zk/scratchpad/`
    /// （经 [`zk_core::paths::scratchpad_dir`]，与 zk-server 缺省、系统提示同源）。
    #[must_use]
    pub fn for_workspace(workspace_root: &Path) -> Self {
        Self {
            policy: SystemScratchpadPathPolicy::new(&zk_core::paths::scratchpad_dir(
                workspace_root,
            )),
        }
    }

    /// 以显式 scratchpad 根装配（服务端自有根 / 单元测试用）。
    #[must_use]
    pub fn with_root(scratchpad_root: &Path) -> Self {
        Self {
            policy: SystemScratchpadPathPolicy::new(scratchpad_root),
        }
    }

    /// 目标是否落在 scratchpad 边界内（委托 [`SystemScratchpadPathPolicy::contains`]，
    /// 根不稳定/身份漂移时失败关闭返回 `false`）。
    #[must_use]
    pub fn permits(&self, target: &Path) -> bool {
        self.policy.contains(target)
    }

    /// 边界校验：目标越出 `.zk/scratchpad/` 时返回拒绝原因。
    ///
    /// # Errors
    ///
    /// 目标不在 scratchpad 边界内（例如 `/etc/passwd`、项目内非暂存区路径）时
    /// 返回可展示的拒绝文案。
    pub fn check_write(&self, target: &Path) -> Result<(), String> {
        if self.permits(target) {
            Ok(())
        } else {
            Err(format!(
                "scratchpad boundary violation: {} is outside the scratchpad root",
                target.display()
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 敏感变量被清理、非敏感（白名单）变量原样保留——对照旧
    /// `sanitizeProcessEnvironment` 的黑名单移除语义。
    #[test]
    fn sanitize_environment_removes_sensitive_keeps_safe() {
        let mut env: BTreeMap<String, String> = BTreeMap::new();
        // 敏感项（全部 10 项）。
        for var in SENSITIVE_ENV_VARS {
            env.insert((*var).to_owned(), "secret".to_owned());
        }
        // 非敏感 / 白名单项（PATH/HOME 等属 bash SAFE_ENV_VARS，此处代表安全变量）。
        env.insert("PATH".to_owned(), "/usr/bin".to_owned());
        env.insert("HOME".to_owned(), "/home/dev".to_owned());
        env.insert("LANG".to_owned(), "en_US.UTF-8".to_owned());

        let guard = ToolSafetyGuard::new();
        guard.sanitize_environment(&mut env);

        // 敏感项全部被移除。
        for var in SENSITIVE_ENV_VARS {
            assert!(!env.contains_key(*var), "sensitive var {var} not removed");
        }
        // 安全项保留。
        assert_eq!(env.get("PATH").map(String::as_str), Some("/usr/bin"));
        assert_eq!(env.get("HOME").map(String::as_str), Some("/home/dev"));
        assert_eq!(env.get("LANG").map(String::as_str), Some("en_US.UTF-8"));
        assert_eq!(env.len(), 3);
    }

    /// 清单与 Java `SENSITIVE_ENV_VARS` 逐条对齐（10 项，全部命中）。
    #[test]
    fn sensitive_env_vars_matches_java_baseline() {
        let guard = ToolSafetyGuard::new();
        let set = guard.sensitive_env_vars();
        assert_eq!(set.len(), 10);
        for var in [
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "GITHUB_TOKEN",
            "GH_TOKEN",
            "NPM_TOKEN",
            "DOCKER_PASSWORD",
            "DATABASE_PASSWORD",
            "DB_PASSWORD",
            "PRIVATE_KEY",
            "SECRET_KEY",
        ] {
            assert!(set.contains(var), "missing sensitive var {var}");
            assert!(guard.is_sensitive_env_var(var));
        }
    }

    /// 大小写敏感：`github_token` 不视为敏感（对齐 Java `Set.contains` / `env.remove`）。
    #[test]
    fn is_sensitive_env_var_is_case_sensitive() {
        let guard = ToolSafetyGuard::new();
        assert!(guard.is_sensitive_env_var("GITHUB_TOKEN"));
        assert!(!guard.is_sensitive_env_var("github_token"));
        assert!(!guard.is_sensitive_env_var("PATH"));
    }

    /// scratchpad 合法路径通过、`/etc/` 等边界外路径被拒。
    #[test]
    fn scratchpad_boundary_allows_inside_denies_outside() {
        let temp = std::env::temp_dir().join(format!("zk-tsg-{}", std::process::id()));
        let scratchpad = zk_core::paths::scratchpad_dir(&temp);
        std::fs::create_dir_all(&scratchpad).expect("create scratchpad");

        let boundary = ScratchpadWriteBoundary::for_workspace(&temp);

        // scratchpad 内合法目标通过。
        let inside = scratchpad.join("session/note.md");
        assert!(boundary.permits(&inside));
        assert!(boundary.check_write(&inside).is_ok());

        // /etc 被拒。
        let etc = Path::new("/etc/passwd");
        assert!(!boundary.permits(etc));
        let err = boundary.check_write(etc).expect_err("etc must be denied");
        assert!(err.contains("scratchpad boundary violation"));

        // 项目内但非 scratchpad 的路径被拒（边界严格于项目边界）。
        let non_scratchpad = temp.join("src/main.rs");
        assert!(!boundary.permits(&non_scratchpad));

        let _ = std::fs::remove_dir_all(&temp);
    }

    /// `with_root` 显式根等价装配。
    #[test]
    fn scratchpad_boundary_with_explicit_root() {
        let temp = std::env::temp_dir().join(format!("zk-tsg-root-{}", std::process::id()));
        std::fs::create_dir_all(&temp).expect("create root");

        let boundary = ScratchpadWriteBoundary::with_root(&temp);
        assert!(boundary.permits(&temp.join("child.txt")));
        assert!(!boundary.permits(Path::new("/etc/shadow")));

        let _ = std::fs::remove_dir_all(&temp);
    }
}
