//! `/memory [show|init]` opens the transactional `SQLite` memory editor.
//! Markdown files are never silently overwritten by a command alias.
use crate::command::{
    CommandContext,
    traits::{Command, CommandResult, CommandType},
};
use futures::future::BoxFuture;

pub(super) struct MemoryCommand;
impl Command for MemoryCommand {
    fn name(&self) -> &'static str {
        "memory"
    }
    fn description(&self) -> &'static str {
        "查看与编辑项目记忆（SQLite 唯一内容来源）"
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
            if !matches!(args.trim(), "" | "show" | "init") {
                return CommandResult::error("用法：/memory [show|init]；编辑后显式保存");
            }
            CommandResult::jsx(
                serde_json::json!({"component":"MemoryManager","sessionId":ctx.session_id}),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn show_and_legacy_init_open_the_cas_editor_without_overwriting_files() {
        let root = std::env::temp_dir().join(format!("zk-memory-command-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let file = root.join("zhikun.md");
        std::fs::write(&file, "user-owned memory file").unwrap();
        let ctx = CommandContext::of(
            "session",
            root.to_string_lossy(),
            "fixture",
            crate::state::AppState::for_tests(),
        );
        for args in ["", "show", "init"] {
            assert_eq!(
                MemoryCommand.execute(args, &ctx).await,
                CommandResult::jsx(
                    serde_json::json!({"component":"MemoryManager","sessionId":"session"})
                )
            );
            assert_eq!(
                std::fs::read_to_string(&file).unwrap(),
                "user-owned memory file"
            );
        }
        assert!(matches!(
            MemoryCommand.execute("delete", &ctx).await,
            CommandResult::Error(_)
        ));
        std::fs::remove_dir_all(root).unwrap();
    }
}
