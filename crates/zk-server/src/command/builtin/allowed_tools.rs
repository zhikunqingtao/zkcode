//! `/allowed-tools`——显示当前可用工具列表（旧 `AllowedToolsCommand.java`）。
//!
//! 已作为 `/permissions` 的别名注册（旧 `getAliases()` 返回 `["allowed-tools"]`）。
//! 此文件保留为独立命令实现，以便后续扩展为专用工具权限视图。
//!
//! 旧实现读 `toolRegistry.getEnabledTools()` 列出全部启用工具。本端等价：
//! 读 `AppState.tools()` 的注册清单。

use std::fmt::Write as _;

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/allowed-tools` 命令（独立入口；别名指向 `/permissions`）。
///
/// 注意：实际注册为 `/permissions` 的别名，此结构体保留供后续扩展。
#[allow(dead_code)]
pub(super) struct AllowedToolsCommand;

impl Command for AllowedToolsCommand {
    fn name(&self) -> &'static str {
        "allowed-tools"
    }

    fn description(&self) -> &'static str {
        "显示当前可用工具列表"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Local
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            let tools = ctx.state.tools();
            let names = tools.names();
            let mut sb = String::new();
            let _ = writeln!(sb, "## 可用工具 ({})", names.len());
            sb.push('\n');
            for name in &names {
                let _ = writeln!(sb, "- **{name}**");
            }
            CommandResult::text(sb)
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::command::CommandRegistry;

    #[tokio::test]
    async fn allowed_tools_alias_resolves_to_permissions() {
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry.find_command("allowed-tools").expect("registered");
        // `allowed-tools` 是 `/permissions` 的别名，故解析到 `permissions` 命令。
        assert_eq!(command.name(), "permissions");
    }
}
