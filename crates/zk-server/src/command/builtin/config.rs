//! 配置族——`/config`（别名 `/settings`）与 `/permissions`（别名
//! `/allowed-tools`），旧 `command/impl/ConfigCommand.java` /
//! `PermissionsCommand.java`。
//!
//! # `/config` 三分支（旧 L44-57 逐字）
//!
//! | 入参 | 行为 |
//! |---|---|
//! | 空白 | `showAllConfig`：五行清单 + 用法尾行 |
//! | 单 token | `showConfigValue`：敏感 key 掩码 / 白名单取值 / 其余报未知 key |
//! | 两 token（`split("\\s+", 2)`） | `setConfigValue`：回 `jsx({action:"setConfig",…})` |
//!
//! `setConfigValue` 在旧实现里**不落库**——只回结构化指令，由前端自行持久化
//! （前端 `setConfig` 动作会打 `PUT /api/config`）。本移植保持同一分工：
//! 命令域不旁路 [`crate::api::config`] 的校验/合并管线。
//!
//! # 事实源映射
//!
//! | 旧读取点 | 本实现 |
//! |---|---|
//! | `state.session().currentModel()` | [`CommandContext::current_model`] |
//! | `state.session().workingDirectory()` | `CommandContext::working_dir` |
//! | `state.permissions().permissionMode()` | [`crate::authz::PermissionModeRegistry::get_mode`] |
//! | `state.ui().theme()` | `UserConfig::theme`（落库用户配置） |
//!
//! # 与旧实现的刻意差异（留痕）
//!
//! 1. **`autoCompact` 行**：旧实现硬编码字面量 `"true"`（L66），与真实开关
//!    无关；zkcode 有真实事实源 `UserConfig::auto_compact_enabled`（
//!    `GET/PUT /api/config` 的键），故渲染真实值——旧行为在开关关闭时输出
//!    误导信息，属旧侧缺陷，不复刻。
//! 2. **配置读取失败**：旧实现读进程内 `AppStateStore` 无失败路径；zkcode 的
//!    `theme` / `autoCompact` 在库里，读库失败时回 `error`（而非渲染半真值）。
//! 3. **`/permissions` 的 allow 规则**：旧 `PermissionState.alwaysAllowRules`
//!    的写入方法 `withAlwaysAllowRules` 在整个旧仓库**无调用点**，该 map 恒空
//!    → 旧输出恒为 `(none)`。zkcode 的等价事实源是 `permission_grants` 表的
//!    活跃授权（[`zk_authz::GrantStore::list_active_for_session`]），故渲染真实
//!    授权行 `    <scope> → <toolName>`。
//! 4. **`/permissions` 的 deny 规则**：同理旧 map 恒空；zkcode 侧亦无「常驻
//!    拒绝」存储（授权表只存放行记录，拒绝是一次性判定），故该段恒 `(none)`
//!    ——与旧可观察输出一致。
//! 5. **`Bypass:` 行**：旧取 `PermissionState.isSkipAllPrompts`，该字段在旧
//!    仓库**从未被置 true**（`empty()` 填 false，无 with-er）。zkcode 的等价
//!    语义是「无条件放行模式」，即 [`PermissionMode::AutoApprove`]（
//!    `zk_authz::AuthorizationService` 中唯一的无条件放行分支），故按模式派生。

use std::fmt::Write as _;

use futures::future::BoxFuture;
use zk_authz::model::PermissionMode;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// 旧 `SENSITIVE_KEYS`（L29）：查看时一律掩码，写入日志时同样掩码。
const SENSITIVE_KEYS: [&str; 2] = ["apiKey", "llm.apiKey"];

/// 旧 `/permissions` 的 allow 段读取上限（旧无上限——map 恒空；此处取仓储
/// `list_active_for_session` 的合法区间上界，避免会话授权极多时的巨幅输出）。
const GRANT_LIST_LIMIT: i64 = 200;

/// `/config`（别名 `/settings`）——查看/设置配置（旧 `ConfigCommand`）。
pub(super) struct ConfigCommand;

impl Command for ConfigCommand {
    fn name(&self) -> &'static str {
        "config"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["settings"]
    }

    fn description(&self) -> &'static str {
        "View or set configuration"
    }

    fn command_type(&self) -> CommandType {
        CommandType::LocalJsx
    }

    fn execute<'a>(
        &'a self,
        args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            // 旧 L44-46：`args == null || args.isBlank()`。
            let trimmed = args.trim();
            if trimmed.is_empty() {
                return show_all_config(ctx).await;
            }
            // 旧 L48-56：`split("\\s+", 2)`——第二段保留内部空白（含尾随空白）。
            match trimmed.split_once(char::is_whitespace) {
                None => show_config_value(trimmed, ctx).await,
                Some((key, rest)) => {
                    // Java 的 `\\s+` 限长切分把分隔符处的连续空白整体吃掉，
                    // 故 value 为 rest 去掉前导空白后的原文。
                    let value = rest.trim_start();
                    set_config_value(key, value)
                }
            }
        })
    }
}

/// 旧 `showAllConfig`（L59-71）：五行 + 用法尾行。
///
/// 对齐空格逐字复刻旧 `StringBuilder`——注意 `permissionMode:` 行只跟 1 个
/// 空格（旧实现该行未与其余四行对齐，属旧输出既有形状）。
async fn show_all_config(ctx: &CommandContext) -> CommandResult {
    let config = match crate::api::config::load_user_config(&ctx.state).await {
        Ok(config) => config,
        Err(err) => {
            return CommandResult::error(format!(
                "Failed to load configuration: {message}",
                message = err.message
            ));
        }
    };
    let mode = ctx.state.authz.modes.get_mode(&ctx.session_id);
    let mut text = String::from("Current Configuration:\n\n");
    let _ = writeln!(text, "  model:         {}", ctx.current_model);
    let _ = writeln!(text, "  workingDir:    {}", ctx.working_dir);
    let _ = writeln!(text, "  permissionMode: {}", mode.as_str());
    let _ = writeln!(text, "  autoCompact:   {}", config.auto_compact_enabled);
    let _ = writeln!(text, "  theme:         {}", config.theme);
    text.push_str("\nUse /config <key> <value> to update a setting.");
    CommandResult::text(text)
}

/// 旧 `showConfigValue`（L73-92）：敏感掩码 → 白名单取值 → 未知 key 报错。
///
/// 白名单四键逐字（旧 switch 无 `autoCompact` 分支——该键只在全量清单里出现，
/// 单独查询时按未知 key 处理，此为旧行为，不补齐）。
async fn show_config_value(key: &str, ctx: &CommandContext) -> CommandResult {
    if SENSITIVE_KEYS.contains(&key) {
        return CommandResult::text(format!("{key} = ***"));
    }
    let value = match key {
        "model" => Some(ctx.current_model.clone()),
        "workingDir" => Some(ctx.working_dir.clone()),
        "permissionMode" => Some(
            ctx.state
                .authz
                .modes
                .get_mode(&ctx.session_id)
                .as_str()
                .to_owned(),
        ),
        "theme" => match crate::api::config::load_user_config(&ctx.state).await {
            Ok(config) => Some(config.theme),
            Err(err) => {
                return CommandResult::error(format!(
                    "Failed to load configuration: {message}",
                    message = err.message
                ));
            }
        },
        _ => None,
    };
    // 旧 L87-89：白名单外的 key。
    let Some(value) = value else {
        return CommandResult::error(format!("Unknown config key: {key}"));
    };
    CommandResult::text(format!("{key} = {value}"))
}

/// 旧 `setConfigValue`（L94-103）：只回 JSX 指令，敏感值在日志里掩码。
fn set_config_value(key: &str, value: &str) -> CommandResult {
    let logged = if SENSITIVE_KEYS.contains(&key) {
        "***"
    } else {
        value
    };
    tracing::info!(key, value = logged, "Setting config");
    CommandResult::jsx(serde_json::json!({
        "action": "setConfig",
        "key": key,
        "value": value,
    }))
}

/// `/permissions`（别名 `/allowed-tools`）——权限模式与规则清单
/// （旧 `PermissionsCommand`）。
pub(super) struct PermissionsCommand;

impl Command for PermissionsCommand {
    fn name(&self) -> &'static str {
        "permissions"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["allowed-tools"]
    }

    fn description(&self) -> &'static str {
        "View and manage tool permissions"
    }

    fn command_type(&self) -> CommandType {
        CommandType::LocalJsx
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            let mode = ctx.state.authz.modes.get_mode(&ctx.session_id);
            // 旧 `isSkipAllPrompts` 的等价语义（见模块文档差异 5）。
            let bypass = mode == PermissionMode::AutoApprove;
            let grants = match ctx
                .state
                .authz
                .grants
                .list_active_for_session(&ctx.session_id, GRANT_LIST_LIMIT)
                .await
            {
                Ok(grants) => grants,
                Err(err) => {
                    tracing::warn!(session_id = %ctx.session_id, error = %err, "grant list failed");
                    return CommandResult::error(format!("Failed to read permissions: {err}"));
                }
            };

            let mut text = String::from("Permission Settings:\n\n");
            let _ = writeln!(text, "  Mode: {}", mode.as_str());
            let _ = writeln!(text, "  Bypass: {bypass}\n");
            // 旧 L40-51：allow 段（空 → `(none)`）。
            text.push_str("  Always Allow Rules:\n");
            if grants.is_empty() {
                text.push_str("    (none)\n");
            } else {
                for grant in &grants {
                    let _ = writeln!(
                        text,
                        "    {scope} → {tool}",
                        scope = grant.scope,
                        tool = grant.tool_name
                    );
                }
            }
            // 旧 L54-65：deny 段（zkcode 无常驻拒绝存储 → 恒 `(none)`，见差异 4）。
            text.push_str("\n  Always Deny Rules:\n");
            text.push_str("    (none)\n");
            CommandResult::text(text)
        })
    }
}

#[cfg(test)]
mod tests {
    use zk_authz::model::PermissionMode;

    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(name: &str, args: &str, session_id: &str, state: AppState) -> CommandResult {
        let ctx = CommandContext::of(session_id, "/tmp/zk-config", "kimi-k3", state);
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry.find_command(name).expect("registered");
        command.execute(args, &ctx).await
    }

    /// `/config` 无参 → 五行清单 + 用法尾行（`permissionMode:` 行 1 空格）。
    #[tokio::test]
    async fn config_without_args_lists_every_setting() {
        let CommandResult::Text(text) = run("config", "", "s-1", AppState::for_tests()).await
        else {
            panic!("/config must be text");
        };
        assert_eq!(
            text,
            "Current Configuration:\n\n\
             \x20 model:         kimi-k3\n\
             \x20 workingDir:    /tmp/zk-config\n\
             \x20 permissionMode: DEFAULT\n\
             \x20 autoCompact:   true\n\
             \x20 theme:         dark\n\
             \nUse /config <key> <value> to update a setting."
        );
    }

    /// `/config` 单 key：白名单取值、敏感掩码、未知 key 报错（旧三分支）。
    #[tokio::test]
    async fn config_single_key_covers_the_three_lookup_branches() {
        let state = AppState::for_tests();
        assert_eq!(
            run("config", "model", "s-1", state.clone()).await,
            CommandResult::text("model = kimi-k3")
        );
        assert_eq!(
            run("config", "theme", "s-1", state.clone()).await,
            CommandResult::text("theme = dark")
        );
        assert_eq!(
            run("config", "apiKey", "s-1", state.clone()).await,
            CommandResult::text("apiKey = ***")
        );
        assert_eq!(
            run("config", "llm.apiKey", "s-1", state.clone()).await,
            CommandResult::text("llm.apiKey = ***")
        );
        // 旧 switch 无 `autoCompact` 分支 → 落 default（未知 key）。
        assert_eq!(
            run("config", "autoCompact", "s-1", state).await,
            CommandResult::error("Unknown config key: autoCompact")
        );
    }

    /// `/config <key> <value>` → JSX 指令（不落库），value 保留内部空白。
    #[tokio::test]
    async fn config_with_value_returns_the_set_config_action() {
        let result = run(
            "config",
            "  theme   solarized dark  ",
            "s-1",
            AppState::for_tests(),
        )
        .await;
        assert_eq!(
            result,
            CommandResult::jsx(serde_json::json!({
                "action": "setConfig",
                "key": "theme",
                "value": "solarized dark",
            }))
        );
    }

    /// `/permissions` 空授权态：Mode/Bypass + 两段 `(none)`（旧输出逐字）。
    #[tokio::test]
    async fn permissions_renders_the_empty_rule_sections() {
        let CommandResult::Text(text) = run("permissions", "", "s-1", AppState::for_tests()).await
        else {
            panic!("/permissions must be text");
        };
        assert_eq!(
            text,
            "Permission Settings:\n\n\
             \x20 Mode: DEFAULT\n\
             \x20 Bypass: false\n\n\
             \x20 Always Allow Rules:\n\
             \x20   (none)\n\
             \n  Always Deny Rules:\n\
             \x20   (none)\n"
        );
    }

    /// `Bypass` 行由「无条件放行模式」派生（旧死字段的等价语义）。
    #[tokio::test]
    async fn permissions_bypass_tracks_the_auto_approve_mode() {
        let state = AppState::for_tests();
        state
            .authz
            .modes
            .set_mode("s-1", PermissionMode::AutoApprove)
            .await;
        let CommandResult::Text(text) = run("permissions", "", "s-1", state).await else {
            panic!("/permissions must be text");
        };
        assert!(
            text.contains("  Mode: AUTO_APPROVE\n"),
            "unexpected: {text}"
        );
        assert!(text.contains("  Bypass: true\n"), "unexpected: {text}");
    }
}
