//! 内建命令族（旧 `command/impl/*.java`）。
//!
//! 旧侧每个命令是一个 `@Component`，由 Spring 收集进
//! `CommandRegistry(List<Command>)` 构造器；zkcode 无容器扫描，故在
//! [`register_builtin_commands`] 显式枚举——清单即事实源，单测互锁防漏注册。
//!
//! # 当前 21 个命令
//!
//! | 命令 | 旧实现类 | 类型 |
//! |---|---|---|
//! | `/clear` | `ClearCommand` | `LOCAL` |
//! | `/compact` | `CompactCommand` | `LOCAL` |
//! | `/config` | `ConfigCommand` | `LOCAL_JSX` |
//! | `/cost` | `CostCommand` | `LOCAL` |
//! | `/diff` | `DiffCommand` | `LOCAL` |
//! | `/doctor` | `DoctorCommand` | `LOCAL_JSX` |
//! | `/exit` | `ExitCommand` | `LOCAL` |
//! | `/git-commit` | `GitCommitCommand`（旧名 `commit`，注册为别名） | `LOCAL` |
//! | `/git-review` | `GitReviewCommand`（旧名 `review`，注册为别名） | `PROMPT` |
//! | `/help` | `HelpCommand` | `LOCAL` |
//! | `/init` | `InitCommand` | `PROMPT` |
//! | `/mcp` | `McpCommands` 三个 `@Bean` 合一 | `LOCAL` |
//! | `/memory` | `MemoryCommand` | `LOCAL_JSX` |
//! | `/model` | `ModelCommand` | `LOCAL_JSX` |
//! | `/permissions` | `PermissionsCommand` | `LOCAL_JSX` |
//! | `/plan` | `PlanCommand` | `LOCAL` |
//! | `/resume` | `ResumeCommand` | `LOCAL_JSX` |
//! | `/retry` | `RetryCommand` | `PROMPT` |
//! | `/session` | `SessionCommand` | `LOCAL_JSX` |
//! | `/status` | `StatusCommand` | `LOCAL` |
//! | `/undo` | `UndoCommand` | `LOCAL` |
//!
//! # 尚未移植的旧命令（留痕）
//!
//! | 旧实现类 | 原因 |
//! |---|---|
//! | `SkillCommand` | 3B.7 已在 [`crate::ws::inbound`] 独立落地（PROMPT 注入路径），本域分发对 `/skill` 显式让位，不重复注册 |
//! | `BrowserSnapshotCommand` / `FeedbackCommand` / `StatsCommand` / `UsageCommand` / `VersionCommand` | 依赖后续 Batch 的域能力（浏览器、遥测、版本元数据），本批不实现 |

mod clear;
mod compact;
mod config;
mod diff;
mod doctor;
mod exit;
mod git_commit;
mod git_review;
mod help;
mod info;
mod init;
mod mcp_cmd;
mod memory;
mod model;
mod plan;
mod resume;
mod retry;
mod undo;
mod visualize;
// ── Batch 8G：长尾命令（login / logout / browser-snapshot / allowed-tools /
//    account / environment）──
mod account;
mod allowed_tools;
mod browser_snapshot;
mod environment;
mod login;
mod logout;

use std::sync::Arc;

use super::registry::CommandRegistry;

/// 注册全部内建命令（旧 Spring 构造器注入 `List<Command>` 的等价物）。
///
/// 注册序不影响可观察行为（[`CommandRegistry`] 底层 `BTreeMap` 按名排序），
/// 此处按命令名字典序枚举以便与下方清单单测逐行对读。
pub(super) fn register_builtin_commands(registry: &CommandRegistry) {
    registry.register(Arc::new(clear::ClearCommand));
    registry.register(Arc::new(compact::CompactCommand));
    registry.register(Arc::new(config::ConfigCommand));
    registry.register(Arc::new(info::CostCommand));
    registry.register(Arc::new(diff::DiffCommand));
    registry.register(Arc::new(doctor::DoctorCommand));
    registry.register(Arc::new(exit::ExitCommand));
    registry.register(Arc::new(git_commit::GitCommitCommand));
    registry.register(Arc::new(git_review::GitReviewCommand));
    registry.register(Arc::new(help::HelpCommand));
    registry.register(Arc::new(init::InitCommand));
    registry.register(Arc::new(mcp_cmd::McpCommand));
    registry.register(Arc::new(memory::MemoryCommand));
    registry.register(Arc::new(model::ModelCommand));
    registry.register(Arc::new(plan::PlanCommand));
    registry.register(Arc::new(config::PermissionsCommand));
    registry.register(Arc::new(resume::ResumeCommand));
    registry.register(Arc::new(retry::RetryCommand));
    registry.register(Arc::new(info::SessionCommand));
    registry.register(Arc::new(info::StatusCommand));
    registry.register(Arc::new(undo::UndoCommand));
    // ── Batch 8G：长尾命令 ──
    registry.register(Arc::new(account::ExtraUsageCommand));
    registry.register(Arc::new(account::RateLimitOptionsCommand));
    registry.register(Arc::new(account::UpgradeCommand));
    registry.register(Arc::new(account::UsageCommand));
    registry.register(Arc::new(account::VersionCommand));
    registry.register(Arc::new(browser_snapshot::BrowserSnapshotCommand));
    registry.register(Arc::new(environment::EnvVarsCommand));
    registry.register(Arc::new(environment::SetEnvCommand));
    registry.register(Arc::new(login::LoginCommand));
    registry.register(Arc::new(logout::LogoutCommand));
    registry.register(Arc::new(visualize::VisualizeCommand::default()));
}

#[cfg(test)]
mod tests {
    use crate::command::CommandRegistry;
    use crate::command::traits::CommandType;

    /// 内建清单互锁：32 个命令名 + 类型 + 别名与旧实现逐条对齐。
    ///
    /// 新增/漏注册命令时此处失败——`/help` 的 `total` 与前端补全清单都以它为源。
    #[test]
    fn builtin_catalog_matches_the_legacy_component_set() {
        let registry = CommandRegistry::with_builtin_commands();
        assert_eq!(registry.len(), 32);

        let local: Vec<&str> = registry
            .commands_by_type(CommandType::Local)
            .iter()
            .map(|command| command.name())
            .collect();
        assert_eq!(
            local,
            [
                "browser-snapshot",
                "clear",
                "compact",
                "cost",
                "diff",
                "env-vars",
                "exit",
                "extra-usage",
                "git-commit",
                "help",
                "logout",
                "mcp",
                "plan",
                "rate-limit-options",
                "set-env",
                "status",
                "undo",
                "upgrade",
                "usage",
                "version",
            ]
        );

        let jsx: Vec<&str> = registry
            .commands_by_type(CommandType::LocalJsx)
            .iter()
            .map(|command| command.name())
            .collect();
        assert_eq!(
            jsx,
            [
                "config",
                "doctor",
                "login",
                "memory",
                "model",
                "permissions",
                "resume",
                "session",
                "visualize",
            ]
        );

        // Batch 8A 起本域有 `PROMPT` 成员——分发侧（[`crate::ws::inbound`]）必须
        // 走「先推 prompt 通知，再把正文注入引擎」的旧 PROMPT 分支。
        let prompt: Vec<&str> = registry
            .commands_by_type(CommandType::Prompt)
            .iter()
            .map(|command| command.name())
            .collect();
        assert_eq!(prompt, ["git-review", "init", "retry"]);

        // 别名索引（旧各 `getAliases()` 逐字；`review` / `commit` 是旧主名，
        // zkcode 主名加 `git-` 前缀，故旧名降级为别名）。
        for (alias, primary) in [
            ("reset", "clear"),
            ("new", "clear"),
            ("settings", "config"),
            ("commit", "git-commit"),
            ("review", "git-review"),
            ("allowed-tools", "permissions"),
            ("continue", "resume"),
            ("info", "status"),
            ("env", "env-vars"),
            ("snapshot", "browser-snapshot"),
            ("viz", "visualize"),
        ] {
            let resolved = registry
                .find_command(alias)
                .unwrap_or_else(|| panic!("alias /{alias} must resolve"));
            assert_eq!(resolved.name(), primary, "alias /{alias}");
        }

        // 本批命令无一隐藏（旧侧同样均未覆写 `isHidden`）。
        assert_eq!(registry.visible_commands().len(), 32);

        // 旧 `supportsNonInteractive()` 覆写者恰为 `/compact` 与 `/cost`。
        let non_interactive: Vec<&str> = registry
            .visible_commands()
            .iter()
            .filter(|command| command.supports_non_interactive())
            .map(|command| command.name())
            .collect();
        assert_eq!(non_interactive, ["compact", "cost"]);
    }

    /// `/skill` 不进本注册表——分发侧对它保持 3B.7 的独立拦截路径。
    #[test]
    fn skill_is_not_registered_in_this_domain() {
        let registry = CommandRegistry::with_builtin_commands();
        assert!(registry.find_command("skill").is_none());
    }

    /// 旧 `mcp-servers` / `mcp-tools` / `mcp-resources` 不注册为别名——别名不
    /// 携带子命令，会被当成 `/mcp` 无参而输出服务器清单（错答比未注册更坏）。
    #[test]
    fn legacy_mcp_bean_names_are_not_aliases() {
        let registry = CommandRegistry::with_builtin_commands();
        for legacy in ["mcp-servers", "mcp-tools", "mcp-resources"] {
            assert!(
                registry.find_command(legacy).is_none(),
                "/{legacy} must not resolve"
            );
        }
    }
}
