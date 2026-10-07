//! Factual context statistics. No model call or guessed window occupancy.
use crate::command::{
    CommandContext,
    traits::{Command, CommandResult, CommandType},
};
use futures::future::BoxFuture;
use zk_tools::ContextInfoPort;

pub(super) struct ContextCommand;
impl Command for ContextCommand {
    fn name(&self) -> &'static str {
        "context"
    }
    fn description(&self) -> &'static str {
        "显示真实会话消息数和累计 Token 用量"
    }
    fn command_type(&self) -> CommandType {
        CommandType::Local
    }
    fn supports_non_interactive(&self) -> bool {
        true
    }
    fn execute<'a>(
        &'a self,
        args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            if !args.trim().is_empty() {
                return CommandResult::error("Usage: /context");
            }
            match crate::context_info::DbContextInfo(ctx.state.db.clone())
                .get_context_info(&ctx.session_id, None)
                .await
            {
                Ok(info) => CommandResult::text(format!(
                    "Context statistics:\n  Model: {}\n  Session: {}\n  Stored messages: {}\n  Cumulative input tokens: {}\n  Cumulative output tokens: {}\nCumulative usage is not the active context-window occupancy; system instructions, tool schemas, and compaction also affect each request.",
                    ctx.current_model,
                    info.session_id,
                    info.message_count,
                    info.total_input_tokens,
                    info.total_output_tokens
                )),
                Err(code) => CommandResult::error(code),
            }
        })
    }
}
