//! `/compact [custom_instruction]`——手动触发上下文压缩
//! （旧 `command/impl/CompactCommand.java`）。
//!
//! 旧实现四步：`sessionManager.loadSession` 取消息 → `ModelRegistry`
//! 取上下文窗口 → `TokenCounter` 估算压缩前 token → `CompactService.compact(
//! messages, contextWindow, false)`，再按 `skipReason` 分流。zkcode 的对应件
//! 已在 3A.1 落地（[`zk_engine::context`]），本命令只做编排与文案。
//!
//! # 逐字对照表
//!
//! | 旧行为 | 本实现 |
//! |---|---|
//! | `sessionManager.loadSession(sessionId)` | [`zk_db::Db::get_session`] |
//! | `sessionData.get().messages()`（LLM 请求消息序列） | `history_to_chat_messages(&detail.messages)` |
//! | `modelRegistry.getContextWindowForModel(model)` | [`zk_engine::context::context_window_for`] |
//! | `tokenCounter.estimateTokens(messages)` | [`zk_engine::context::estimate_tokens`] |
//! | `compactService.compact(msgs, cw, false)` | [`zk_engine::context::compact_messages`]（`is_reactive=false`） |
//! | `result.skipReason()` 三取值 | [`CompactSkip`] 三变体（见 [`skip_reason`]） |
//! | `result.summary()` | [`legacy_summary`]（旧 `String.format` 逐字） |
//!
//! # 与旧实现的刻意差异（留痕）
//!
//! 1. **`compactedMessageCount` 的语义**：旧 record 字段名易误读，其值是
//!    `compacted.size()`（**压缩产物**的条数，见
//!    `engine/CompactService.java` `CompactResult.success`）；Rust
//!    [`zk_engine::context::CompactResult::compacted_count`] 才是「被压缩掉的
//!    条数」，两者不同义。本实现取 `result.messages.len()` 对齐旧输出。
//! 2. **压缩结果不落库**：旧实现同样只回结果、不改会话（写回由自动压缩链路
//!    负责），此处保持一致——`/compact` 是「压缩预览 + 统计」。

use std::fmt::Write as _;

use futures::future::BoxFuture;
use zk_engine::LlmSummarizer;
use zk_engine::context::compact::CompactSkip;
use zk_engine::context::{compact_messages, context_window_for, estimate_tokens};
use zk_engine::engine::history_to_chat_messages;
use zk_llm::ChatProvider;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// 旧 `context.currentModel() != null ? currentModel : "default"` 的兜底值。
const DEFAULT_MODEL: &str = "default";

/// `/compact` 命令。
pub(super) struct CompactCommand;

impl Command for CompactCommand {
    fn name(&self) -> &'static str {
        "compact"
    }

    fn description(&self) -> &'static str {
        "Compact conversation context"
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
            // 旧 L57-59：会话缺失是 `text`（非 `error`），文案含 sessionId。
            let detail = match ctx.state.db.get_session(&ctx.session_id).await {
                Ok(Some(detail)) => detail,
                Ok(None) => {
                    return CommandResult::text(format!("Session not found: {}", ctx.session_id));
                }
                // 旧 catch-all（L89-91）：读库失败前缀逐字。
                Err(err) => {
                    tracing::error!(session_id = %ctx.session_id, error = %err, "Compact failed");
                    return CommandResult::error(format!("Failed to compact conversation: {err}"));
                }
            };
            // 旧 L62-64：`messages == null || isEmpty()`（破折号为全角 em dash）。
            if detail.messages.is_empty() {
                return CommandResult::text("Nothing to compact — conversation is empty.");
            }
            let messages = history_to_chat_messages(&detail.messages);
            let model = if ctx.current_model.trim().is_empty() {
                DEFAULT_MODEL
            } else {
                ctx.current_model.as_str()
            };
            let context_window = context_window_for(model);
            // 旧 L70：命令侧独立估算一次（与 `compact_messages` 内部的
            // `before_tokens` 同一算法同一入参，故两值恒等；此处保留旧的两次
            // 计算结构，metadata 的 `beforeTokens` 取本值）。
            let before_tokens = estimate_tokens(&messages, model);
            let lightweight_model = std::env::var("ZK_LIGHTWEIGHT_MODEL")
                .ok()
                .filter(|model| !model.trim().is_empty())
                .unwrap_or_else(|| ctx.state.providers.default_model().to_owned());
            let provider: std::sync::Arc<dyn ChatProvider> = ctx.state.providers.clone();
            let summarizer = LlmSummarizer::new(provider, lightweight_model);
            let result =
                match compact_messages(&messages, model, context_window, false, &summarizer) {
                    Ok(result) => result,
                    // 旧 L73-75：`skipReason != null` → `text`（非 `error`）。
                    Err(skip) => {
                        return CommandResult::text(format!(
                            "Compact skipped: {reason}",
                            reason = skip_reason(skip)
                        ));
                    }
                };

            let instruction = args.trim();
            let mut display_text = format!(
                "Conversation compacted. {summary}",
                summary = legacy_summary(&result)
            );
            // 旧 L78-80：自定义指令追加块。
            if !instruction.is_empty() {
                let _ = write!(display_text, " (Instruction: {instruction})");
            }
            // 旧 L83-91 的七键 metadata（键名逐字）。
            CommandResult::compact(
                display_text,
                serde_json::json!({
                    "originalMessageCount": messages.len(),
                    "compactedMessageCount": result.messages.len(),
                    "beforeTokens": before_tokens,
                    "afterTokens": result.after_tokens,
                    "savedTokens": result.tokens_saved(),
                    "compressionRatio": result.ratio,
                    "instruction": instruction,
                }),
            )
        })
    }
}

/// 旧 `CompactResult.skipReason()` 的三个取值。
///
/// 旧静态工厂 `notNeeded()` / `skipped("no_token_savings")` / `failed(n)` 分别
/// 写入 `"not_needed"` / `"no_token_savings"` / `"compact_failed"`，与
/// [`CompactSkip`] 三变体一一对应（后者的文档已标注同一映射）。
fn skip_reason(skip: CompactSkip) -> &'static str {
    match skip {
        CompactSkip::NotNeeded => "not_needed",
        CompactSkip::NoTokenSavings => "no_token_savings",
        CompactSkip::AlreadyAttempted => "compact_failed",
    }
}

/// 旧 `CompactResult.summary()` 逐字：
/// `"压缩 %d 条消息: %d → %d tokens (%.1f%% 压缩率)"`。
///
/// 首个占位取旧 `compactedMessageCount`，即压缩产物条数（见模块文档差异 2）。
fn legacy_summary(result: &zk_engine::context::CompactResult) -> String {
    format!(
        "压缩 {count} 条消息: {before} → {after} tokens ({ratio:.1}% 压缩率)",
        count = result.messages.len(),
        before = result.before_tokens,
        after = result.after_tokens,
        ratio = result.ratio * 100.0
    )
}

#[cfg(test)]
mod tests {
    use zk_db::{MessageRole, NewMessage, StoredBlock};
    use zk_engine::context::compact::CompactSkip;

    use super::skip_reason;
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    fn user_text(text: &str) -> NewMessage {
        NewMessage {
            role: MessageRole::User,
            content: vec![StoredBlock::Text {
                text: text.to_owned(),
            }],
            stop_reason: None,
            input_tokens: 0,
            output_tokens: 0,
        }
    }

    async fn run(args: &str, session_id: &str, state: AppState) -> CommandResult {
        let ctx = CommandContext::of(session_id, "/tmp/zk-compact", "kimi-k3", state);
        let registry = CommandRegistry::with_builtin_commands();
        let compact = registry.find_command("compact").expect("registered");
        compact.execute(args, &ctx).await
    }

    /// 会话缺失 → `text`（旧非 `error` 分支），文案含 sessionId。
    #[tokio::test]
    async fn missing_session_is_reported_as_text() {
        let result = run("", "ghost", AppState::for_tests()).await;
        assert_eq!(result, CommandResult::text("Session not found: ghost"));
    }

    /// 空会话 → 旧固定文案（含全角破折号）。
    #[tokio::test]
    async fn empty_conversation_reports_nothing_to_compact() {
        let state = AppState::for_tests();
        let session = state
            .db
            .create_session("kimi-k3", "/tmp/zk-compact")
            .await
            .expect("session created");
        let result = run("", &session.id, state).await;
        assert_eq!(
            result,
            CommandResult::text("Nothing to compact — conversation is empty.")
        );
    }

    /// 消息不足触发压缩 → 旧 `Compact skipped: <reason>` 文案。
    #[tokio::test]
    async fn short_conversation_skips_with_the_legacy_reason() {
        let state = AppState::for_tests();
        let session = state
            .db
            .create_session("kimi-k3", "/tmp/zk-compact")
            .await
            .expect("session created");
        state
            .db
            .append_message(&session.id, user_text("hello"))
            .await
            .expect("message appended");
        let CommandResult::Text(text) = run("", &session.id, state).await else {
            panic!("skip path must be text");
        };
        assert!(
            text.starts_with("Compact skipped: "),
            "unexpected output: {text}"
        );
    }

    /// 三个 skip 原因的字面量与旧 `skipReason()` 取值逐字一致。
    #[test]
    fn skip_reasons_match_the_legacy_literals() {
        assert_eq!(skip_reason(CompactSkip::NotNeeded), "not_needed");
        assert_eq!(skip_reason(CompactSkip::NoTokenSavings), "no_token_savings");
        assert_eq!(skip_reason(CompactSkip::AlreadyAttempted), "compact_failed");
    }
}
