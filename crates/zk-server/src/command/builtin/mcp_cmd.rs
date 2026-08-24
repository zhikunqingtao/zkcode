//! `/mcp [list|tools|resources|status|restart <name>]`——MCP 服务器族（Batch 8A）。
//!
//! 语义来源（旧仓库只读）：`McpCommands.java`（72L，`@Configuration` 里三个
//! `@Bean` 匿名命令 `mcp-servers` / `mcp-tools` / `mcp-resources`）+
//! `McpClientManager.restartServer`（旧 WS `handleMcpOperation` 的 `refresh`
//! 分支同源）。
//!
//! # 有意差异
//!
//! - **三命令合一**：旧侧是三个独立命令，此处收敛为 `/mcp` 一个命令 + 子命令
//!   （`list` / `tools` / `resources`）。补全列表里三个 `mcp-*` 顶层项挤占视野，
//!   且旧 `mcp-servers` 与本命令的 `list` 输出逐字相同。旧命令名**未**注册为
//!   别名：别名不携带子命令（`execute` 只收 args，看不到调用名），`/mcp-tools`
//!   会被当成 `/mcp` 无参而输出服务器清单——错答比未注册更坏。
//! - **新增 `status` 子命令**：旧三命令只输出 `✅/❌` 二值；连接实际有六态
//!   （[`McpConnectionStatus`]），`status` 把状态字面量与工具/资源计数摊开，
//!   排障时不必再去翻日志。
//! - **新增 `restart <name>` 子命令**：能力与旧 WS `mcp_operation{refresh}`
//!   等价，只是从斜杠命令入口也能触发。

use std::fmt::Write as _;

use futures::future::BoxFuture;
use zk_mcp::{McpClientManager, McpConnectionStatus};

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// 用法提示（未知子命令时下行）。
const USAGE: &str = "用法: /mcp [list|tools|resources|status|restart <服务器名>]";

/// `/mcp` 命令。
pub(super) struct McpCommand;

impl Command for McpCommand {
    fn name(&self) -> &'static str {
        "mcp"
    }

    fn description(&self) -> &'static str {
        "查看与管理 MCP 服务器"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Local
    }

    fn execute<'a>(
        &'a self,
        args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            let args = args.trim();
            // 子命令与其余参数（旧 `split("\\s+", 2)` 的同一切法）。
            let (subcommand, rest) = match args.find(char::is_whitespace) {
                Some(idx) => (&args[..idx], args[idx..].trim()),
                None => (args, ""),
            };
            let manager = ctx.state.mcp();
            match subcommand {
                // 无参即 `list`（旧 `mcp-servers` 的输出逐字）。
                "" | "list" | "servers" => CommandResult::text(server_listing(&manager)),
                "tools" => CommandResult::text(tool_listing(&manager)),
                "resources" => CommandResult::text(resource_listing(&manager)),
                "status" => CommandResult::text(status_listing(&manager)),
                "restart" => restart(&manager, rest).await,
                _ => CommandResult::error(USAGE),
            }
        })
    }
}

/// 旧 `mcpServersCommand`：`## MCP 服务器 (N)` + 每行 `✅/❌` 与工具数。
fn server_listing(manager: &McpClientManager) -> String {
    let connections = manager.list_connections();
    let mut text = format!("## MCP 服务器 ({})\n\n", connections.len());
    for connection in connections {
        let mark = if connection.status() == McpConnectionStatus::Connected {
            "✅"
        } else {
            "❌"
        };
        let _ = writeln!(
            text,
            "- **{name}**: {mark} (工具: {tools})",
            name = connection.name(),
            tools = connection.tools().len()
        );
    }
    text
}

/// 旧 `mcpToolsCommand`：`## MCP 工具 (N)` + 每行工具名与描述。
fn tool_listing(manager: &McpClientManager) -> String {
    let tools = manager.discover_and_wrap_tools();
    let mut text = format!("## MCP 工具 ({})\n\n", tools.len());
    for tool in tools {
        let _ = writeln!(
            text,
            "- **{name}**: {description}",
            name = tool.name(),
            description = tool.description()
        );
    }
    text
}

/// 旧 `mcpResourcesCommand`：`## MCP 资源 (N)` + 每行资源名与 URI（只取已连接
/// 服务器，旧 `getConnectedServers()`）。
fn resource_listing(manager: &McpClientManager) -> String {
    let resources: Vec<_> = manager
        .connected_servers()
        .iter()
        .flat_map(|connection| connection.resources())
        .collect();
    let mut text = format!("## MCP 资源 ({})\n\n", resources.len());
    for resource in resources {
        let _ = writeln!(
            text,
            "- **{name}**: {uri}",
            name = resource.name,
            uri = resource.uri
        );
    }
    text
}

/// 连接状态明细（本端新增；见模块文档的有意差异）。
fn status_listing(manager: &McpClientManager) -> String {
    let connections = manager.list_connections();
    let mut text = format!("## MCP 连接状态 ({})\n\n", connections.len());
    for connection in connections {
        let _ = writeln!(
            text,
            "- **{name}**: {status} (工具: {tools}, 资源: {resources})",
            name = connection.name(),
            status = connection.status(),
            tools = connection.tools().len(),
            resources = connection.resources().len()
        );
    }
    text
}

/// `restart <name>`：与旧 WS `mcp_operation{refresh}` 同一管理器调用。
async fn restart(manager: &std::sync::Arc<McpClientManager>, name: &str) -> CommandResult {
    if name.is_empty() {
        return CommandResult::error("用法: /mcp restart <服务器名>");
    }
    match manager.restart_server(name).await {
        Ok(()) => CommandResult::text(format!("✅ MCP 服务器已重连: {name}")),
        Err(error) => {
            tracing::warn!(server = name, %error, "slash command /mcp restart failed");
            CommandResult::error(format!("MCP 服务器重连失败: {name} — {error}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command::traits::{CommandResult, CommandType};
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    /// 跑一次 `/mcp <args>`（未启动管理器：清单皆空、`restart` 必失败）。
    async fn run(args: &str) -> CommandResult {
        let ctx = CommandContext::of("s-mcp", "/tmp", "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry.find_command("mcp").expect("registered");
        assert_eq!(command.command_type(), CommandType::Local);
        command.execute(args, &ctx).await
    }

    /// 取 `Text` 正文（其余变体即失败）。
    fn text_of(result: CommandResult) -> String {
        match result {
            CommandResult::Text(text) => text,
            other => panic!("expected text, got {other:?}"),
        }
    }

    /// 无参与 `list` / `servers` 同解，标题带计数。
    #[tokio::test]
    async fn blank_args_lists_servers() {
        for args in ["", "  ", "list", "servers"] {
            let text = text_of(run(args).await);
            assert!(
                text.starts_with("## MCP 服务器 (0)"),
                "unexpected output for {args:?}: {text}"
            );
        }
    }

    /// `tools` / `resources` / `status` 各自的标题（旧三命令逐字 + 本端新增项）。
    #[tokio::test]
    async fn subcommands_render_their_own_headings() {
        for (args, heading) in [
            ("tools", "## MCP 工具 (0)"),
            ("resources", "## MCP 资源 (0)"),
            ("status", "## MCP 连接状态 (0)"),
        ] {
            let text = text_of(run(args).await);
            assert!(
                text.starts_with(heading),
                "unexpected output for /mcp {args}: {text}"
            );
        }
    }

    /// 未知子命令 → 用法错误（不静默当成 `list`）。
    #[tokio::test]
    async fn unknown_subcommand_reports_usage() {
        match run("delete everything").await {
            CommandResult::Error(message) => assert_eq!(message, super::USAGE),
            other => panic!("expected error, got {other:?}"),
        }
    }

    /// `restart` 缺服务器名 → 专用用法错误。
    #[tokio::test]
    async fn restart_without_name_reports_usage() {
        match run("restart").await {
            CommandResult::Error(message) => {
                assert_eq!(message, "用法: /mcp restart <服务器名>");
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    /// 管理器未启动时 `restart <name>` → 失败文案携带底层原因。
    #[tokio::test]
    async fn restart_surfaces_manager_error() {
        match run("restart nope").await {
            CommandResult::Error(message) => assert_eq!(
                message,
                "MCP 服务器重连失败: nope — MCP_CLIENT_MANAGER_NOT_RUNNING"
            ),
            other => panic!("expected error, got {other:?}"),
        }
    }
}
