//! `/memory [show|init]`——管理项目记忆文件（Batch 7）。
//!
//! 语义来源（旧仓库只读）：`MemoryCommand.java`（72L）。
//!
//! 用法：
//! - `/memory show`（或无参数）— 显示当前项目记忆文件内容；
//! - `/memory init` — 创建 `zhikun.md` 模板文件。

use std::path::Path;

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/memory` 命令。
pub(super) struct MemoryCommand;

impl Command for MemoryCommand {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn description(&self) -> &'static str {
        "Manage project memory (zhikun.md)"
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
            if ctx.working_dir.trim().is_empty() {
                return CommandResult::error("工作目录未设置，无法加载记忆文件");
            }
            let working_dir = Path::new(&ctx.working_dir);

            if args.trim().is_empty() || args.trim() == "show" {
                let memory_path = working_dir.join("zhikun.md");
                let content = tokio::fs::read_to_string(&memory_path)
                    .await
                    .unwrap_or_default();
                let has_memory = !content.trim().is_empty();
                CommandResult::jsx(serde_json::json!({
                    "action": "showMemoryFiles",
                    "workingDir": ctx.working_dir,
                    "hasMemory": has_memory,
                    "content": if has_memory { content } else { String::new() },
                    "files": ["zhikun.md", "zhikun.local.md"]
                }))
            } else if args.trim() == "init" {
                let template = "# 项目记忆\n\n## 技术栈\n<!-- 在此填写项目技术栈信息 -->\n\n## 编码规范\n<!-- 在此填写编码规范 -->\n\n## 注意事项\n<!-- 在此填写项目注意事项 -->\n";
                let memory_path = working_dir.join("zhikun.md");
                match tokio::fs::write(&memory_path, template).await {
                    Ok(()) => CommandResult::jsx(serde_json::json!({
                        "action": "memoryCreated",
                        "fileName": "zhikun.md",
                        "workingDir": ctx.working_dir
                    })),
                    Err(e) => CommandResult::error(format!("创建失败: {e}")),
                }
            } else {
                CommandResult::error("未知子命令。用法: /memory [show|init]")
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("zk-memory-cmd-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    async fn run(args: &str, working_dir: &str) -> CommandResult {
        let ctx = CommandContext::of("s-1", working_dir, "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let cmd = registry.find_command("memory").expect("registered");
        cmd.execute(args, &ctx).await
    }

    #[tokio::test]
    async fn show_without_file_returns_empty() {
        let dir = temp_dir("empty");
        let result = run("show", dir.to_str().unwrap()).await;
        match result {
            CommandResult::Jsx(data) => {
                assert_eq!(data["action"], "showMemoryFiles");
                assert_eq!(data["hasMemory"], false);
            }
            _ => panic!("expected jsx"),
        }
    }

    #[tokio::test]
    async fn init_creates_template() {
        let dir = temp_dir("init");
        let result = run("init", dir.to_str().unwrap()).await;
        match result {
            CommandResult::Jsx(data) => {
                assert_eq!(data["action"], "memoryCreated");
                assert_eq!(data["fileName"], "zhikun.md");
            }
            _ => panic!("expected jsx"),
        }
        assert!(dir.join("zhikun.md").exists());
    }

    #[tokio::test]
    async fn unknown_subcommand_returns_error() {
        let dir = temp_dir("unknown");
        let result = run("delete", dir.to_str().unwrap()).await;
        assert!(matches!(result, CommandResult::Error(_)));
    }
}
