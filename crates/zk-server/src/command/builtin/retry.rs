//! `/retry`——重试上一次查询（Batch 8A）。
//!
//! 语义来源（旧仓库只读）：`RetryCommand.java`（48L，`PromptCommand`）——从会话
//! 倒序找最后一条**非空**用户消息，把其文本块 `join("\n")` 后当提示词重发，
//! 空会话 / 找不到用户消息各有一条固定错误文案。
//!
//! # 有意差异
//!
//! - **数据源**：旧侧走 `SessionManager.loadSession(...).messages()`；本端等价物
//!   是 [`zk_db::Db::get_session`] 的 `detail.messages`（同一张
//!   `messages` 表的全量读，不走游标分页——分页首页未必含最后一条）。
//! - **读库失败**：旧 `loadSession` 返回 `Optional.empty()` 时与空会话同文案；
//!   本端把 `Err` 一并归入该分支（另记 `warn` 日志），行为对齐。

use futures::future::BoxFuture;
use zk_db::{MessageRole, StoredBlock};

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/retry` 命令。
pub(super) struct RetryCommand;

impl Command for RetryCommand {
    fn name(&self) -> &'static str {
        "retry"
    }

    fn description(&self) -> &'static str {
        "重试上一次失败的查询"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Prompt
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            let detail = match ctx.state.db.get_session(&ctx.session_id).await {
                Ok(Some(detail)) => detail,
                Ok(None) => return CommandResult::error("当前会话无消息，无法重试"),
                Err(error) => {
                    tracing::warn!(session_id = %ctx.session_id, %error, "/retry session load failed");
                    return CommandResult::error("当前会话无消息，无法重试");
                }
            };
            if detail.messages.is_empty() {
                return CommandResult::error("当前会话无消息，无法重试");
            }

            // 旧循环：倒序扫描，遇到首条正文非空的用户消息即停。
            let last_user_message = detail
                .messages
                .iter()
                .rev()
                .filter(|message| message.role == MessageRole::User)
                .map(|message| user_text(&message.content))
                .find(|text| !text.trim().is_empty());

            match last_user_message {
                Some(text) => CommandResult::text(text),
                None => CommandResult::error("未找到用户消息，无法重试"),
            }
        })
    }
}

/// 用户消息的文本块拼接（旧 `filter(TextBlock).map(text).joining("\n")`）。
fn user_text(content: &[StoredBlock]) -> String {
    content
        .iter()
        .filter_map(|block| match block {
            StoredBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use zk_db::{MessageRole, NewMessage, StoredBlock};

    use crate::command::traits::{CommandResult, CommandType};
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    /// 追加一条消息（`content` 逐块给出）。
    async fn append(
        state: &AppState,
        session_id: &str,
        role: MessageRole,
        blocks: Vec<StoredBlock>,
    ) {
        state
            .db
            .append_message(
                session_id,
                NewMessage {
                    role,
                    content: blocks,
                    stop_reason: None,
                    input_tokens: 0,
                    output_tokens: 0,
                },
            )
            .await
            .expect("append message");
    }

    /// 文本块速写。
    fn text_block(text: &str) -> StoredBlock {
        StoredBlock::Text {
            text: text.to_owned(),
        }
    }

    /// 跑一次 `/retry`。
    async fn run(state: &AppState, session_id: &str) -> CommandResult {
        let ctx = CommandContext::of(session_id, "/tmp", "kimi-k3", state.clone());
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry.find_command("retry").expect("registered");
        assert_eq!(command.command_type(), CommandType::Prompt);
        command.execute("", &ctx).await
    }

    /// 建一条空会话，返回 (state, `session_id`) 二元组。
    async fn seed_session() -> (AppState, String) {
        let state = AppState::for_tests();
        let id = state
            .db
            .create_session("kimi-k3", "/tmp")
            .await
            .expect("session created")
            .id;
        (state, id)
    }

    /// 会话不存在 → 旧「当前会话无消息」文案。
    #[tokio::test]
    async fn missing_session_reports_no_messages() {
        let state = AppState::for_tests();
        match run(&state, "no-such-session").await {
            CommandResult::Error(message) => {
                assert_eq!(message, "当前会话无消息，无法重试");
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    /// 空会话（有会话无消息）→ 同一文案。
    #[tokio::test]
    async fn empty_session_reports_no_messages() {
        let (state, id) = seed_session().await;
        match run(&state, &id).await {
            CommandResult::Error(message) => {
                assert_eq!(message, "当前会话无消息，无法重试");
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    /// 只有助手消息 → 旧「未找到用户消息」文案。
    #[tokio::test]
    async fn assistant_only_history_reports_no_user_message() {
        let (state, id) = seed_session().await;
        append(
            &state,
            &id,
            MessageRole::Assistant,
            vec![text_block("我先答一句")],
        )
        .await;
        match run(&state, &id).await {
            CommandResult::Error(message) => {
                assert_eq!(message, "未找到用户消息，无法重试");
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    /// 倒序取最后一条用户消息，多文本块以 `\n` 拼接（旧 `joining("\n")`）。
    #[tokio::test]
    async fn returns_last_user_message_joined_by_newline() {
        let (state, id) = seed_session().await;
        append(&state, &id, MessageRole::User, vec![text_block("第一问")]).await;
        append(&state, &id, MessageRole::Assistant, vec![text_block("答")]).await;
        append(
            &state,
            &id,
            MessageRole::User,
            vec![text_block("第二问"), text_block("补充")],
        )
        .await;
        append(
            &state,
            &id,
            MessageRole::Assistant,
            vec![text_block("再答")],
        )
        .await;

        match run(&state, &id).await {
            CommandResult::Text(text) => assert_eq!(text, "第二问\n补充"),
            other => panic!("expected text, got {other:?}"),
        }
    }

    /// 末条用户消息无文本块（如仅工具结果）→ 继续往前找非空的那条。
    #[tokio::test]
    async fn skips_user_messages_without_text_blocks() {
        let (state, id) = seed_session().await;
        append(
            &state,
            &id,
            MessageRole::User,
            vec![text_block("真正的问题")],
        )
        .await;
        append(
            &state,
            &id,
            MessageRole::User,
            vec![StoredBlock::ToolResult {
                tool_use_id: "call-1".to_owned(),
                content: "ok".to_owned(),
                is_error: false,
                metadata: None,
            }],
        )
        .await;

        match run(&state, &id).await {
            CommandResult::Text(text) => assert_eq!(text, "真正的问题"),
            other => panic!("expected text, got {other:?}"),
        }
    }
}
