//! `/clear`（别名 `/reset` `/new`）——以新会话替代当前会话
//! （旧 `command/impl/ClearCommand.java`）。
//!
//! 旧实现不删旧会话，而是 `sessionManager.createSession(currentModel,
//! workingDir)` 建新会话「替代」；前端随后收到 `session_list_updated`
//! （分发侧统一推送，见 [`crate::ws::inbound`]）并自行重绑。zkcode 同构：
//! 建新会话 + 回 `Conversation cleared.`，旧会话记录保留可经 `/resume` 找回。

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/clear` 命令。
pub(super) struct ClearCommand;

impl Command for ClearCommand {
    fn name(&self) -> &'static str {
        "clear"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["reset", "new"]
    }

    fn description(&self) -> &'static str {
        "Clear conversation history"
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
            // 旧 L40-43：`sessionId == null || isBlank()` → 固定错误文案。
            if ctx.session_id.trim().is_empty() {
                return CommandResult::error("No active session to clear.");
            }
            // 旧 L46-47：新会话沿用当前模型与工作目录。
            match ctx
                .state
                .db
                .create_session(&ctx.current_model, &ctx.working_dir)
                .await
            {
                Ok(created) => {
                    tracing::info!(
                        old_session = %ctx.session_id,
                        new_session = %created.id,
                        "Conversation cleared"
                    );
                    CommandResult::text("Conversation cleared.")
                }
                // 旧 L53-55 的 catch：文案前缀逐字，尾随异常消息。
                Err(err) => {
                    tracing::error!(session_id = %ctx.session_id, error = %err, "Failed to clear conversation");
                    CommandResult::error(format!("Failed to clear conversation: {err}"))
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(session_id: &str, state: AppState) -> CommandResult {
        let ctx = CommandContext::of(session_id, "/tmp/zk-clear", "kimi-k3", state);
        let registry = CommandRegistry::with_builtin_commands();
        let clear = registry.find_command("clear").expect("registered");
        clear.execute("", &ctx).await
    }

    /// 成功路径：库内多出一条同模型/同目录的新会话，回旧文案。
    #[tokio::test]
    async fn creates_a_replacement_session_and_returns_the_legacy_text() {
        let state = AppState::for_tests();
        let before = state
            .db
            .list_sessions(None, 50)
            .await
            .expect("list")
            .sessions
            .len();
        let result = run("s-1", state.clone()).await;
        assert_eq!(result, CommandResult::text("Conversation cleared."));

        let page = state.db.list_sessions(None, 50).await.expect("list");
        assert_eq!(page.sessions.len(), before + 1);
        let created = page.sessions.first().expect("new session is newest");
        assert_eq!(created.model, "kimi-k3");
        assert_eq!(created.working_directory, "/tmp/zk-clear");
    }

    /// 空白 `sessionId` → 旧固定错误文案，且不建会话。
    #[tokio::test]
    async fn blank_session_id_is_rejected_without_touching_the_repository() {
        let state = AppState::for_tests();
        let result = run("   ", state.clone()).await;
        assert_eq!(result, CommandResult::error("No active session to clear."));
        assert!(
            state
                .db
                .list_sessions(None, 50)
                .await
                .expect("list")
                .sessions
                .is_empty()
        );
    }
}
