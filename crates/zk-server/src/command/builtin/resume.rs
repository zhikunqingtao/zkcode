//! `/resume [session_id]`（别名 `/continue`）——历史会话恢复
//! （旧 `command/impl/ResumeCommand.java`）。
//!
//! # 两分支（旧 L36-63 逐字）
//!
//! | 入参 | 行为 |
//! |---|---|
//! | 非空白 | `sessionManager.resumeSession(id)`：存在 → `text("Session resumed: <id>")`；不存在 → `error("Session not found: <id>")` |
//! | 空白 | `sessionManager.listSessions(20)`：空 → `text("No previous sessions found.")`；否则清单块 + 用法尾行 |
//!
//! 清单行形状（旧 L56-59）：
//! `"  " + id[0..8] + "  " + %-30s(title|"(untitled)") + "  " + n + " msgs" + "  " + updatedAt`
//!
//! # 与旧实现的刻意差异（留痕）
//!
//! 1. **有参分支只做存在性校验**：旧 `resumeSession` 除读取会话外还把它装进
//!    进程级 `AppStateStore`（「当前会话」单例）。zkcode 的会话是**连接级**
//!    概念——WS 每帧自带 `sessionId`，服务端无「当前会话」可切，切换由前端改
//!    自己的活跃会话完成（`/api/sessions/{id}` 已提供读取面）。故本命令回与旧
//!    一致的两条文案，但不写任何服务端状态。
//! 2. **`updatedAt` 的渲染**：旧 `SessionSummary.updatedAt()` 是 `Instant`，
//!    `StringBuilder.append` 走 `Instant.toString()`（ISO-8601，秒级精度按需
//!    截断）。zkcode 落库为 epoch 毫秒，此处用 REST 出口的同一格式化器
//!    [`crate::iso::format_rfc3339_micros`]（恒 6 位微秒）——同为 ISO-8601 UTC
//!    形状，小数位数固定而非按需截断。
//! 3. **`id[0..8]` 的截断**：旧 `String.substring(0, 8)` 对短于 8 字符的 ID 会
//!    抛 `StringIndexOutOfBoundsException`（会话 ID 是 UUID，旧侧不会触发）。
//!    本实现用 `chars().take(8)`——短 ID 原样输出，不 panic。

use std::fmt::Write as _;

use futures::future::BoxFuture;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};
use crate::iso::format_rfc3339_micros;

/// 旧 `listSessions(20)` 的条数上限（L49）。
const RECENT_SESSION_LIMIT: u32 = 20;

/// 旧 `String.format("%-30s", title)` 的字段宽度（L57）。
const TITLE_WIDTH: usize = 30;

/// 旧 `title() != null ? title() : "(untitled)"`（L57）。
const UNTITLED: &str = "(untitled)";

/// `/resume` 命令。
pub(super) struct ResumeCommand;

impl Command for ResumeCommand {
    fn name(&self) -> &'static str {
        "resume"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["continue"]
    }

    fn description(&self) -> &'static str {
        "Resume a previous session"
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
            // 旧 L36-46：有参分支。
            let requested = args.trim();
            if !requested.is_empty() {
                return match ctx.state.db.get_session(requested).await {
                    Ok(Some(_)) => {
                        tracing::info!(session_id = requested, "Session resumed");
                        CommandResult::text(format!("Session resumed: {requested}"))
                    }
                    Ok(None) => CommandResult::error(format!("Session not found: {requested}")),
                    Err(err) => {
                        tracing::warn!(session_id = requested, error = %err, "resume lookup failed");
                        // 读库失败不可断言「不存在」，故不复用旧的 not-found 文案。
                        CommandResult::error(format!("Failed to resume session: {err}"))
                    }
                };
            }

            // 旧 L49-52：无参分支。
            let page = match ctx.state.db.list_sessions(None, RECENT_SESSION_LIMIT).await {
                Ok(page) => page,
                Err(err) => {
                    tracing::warn!(error = %err, "session list failed");
                    return CommandResult::error(format!("Failed to list sessions: {err}"));
                }
            };
            if page.sessions.is_empty() {
                return CommandResult::text("No previous sessions found.");
            }
            // 旧 L54-62：清单块 + 用法尾行。
            let mut text = String::from("Recent Sessions:\n\n");
            for session in &page.sessions {
                let short_id: String = session.id.chars().take(8).collect();
                let title = session.title.as_deref().unwrap_or(UNTITLED);
                let _ = writeln!(
                    text,
                    "  {short_id}  {title:<TITLE_WIDTH$}  {count} msgs  {updated}",
                    count = session.message_count,
                    updated = format_rfc3339_micros(session.updated_at)
                );
            }
            text.push_str("\nUsage: /resume <session_id>");
            CommandResult::text(text)
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(args: &str, state: AppState) -> CommandResult {
        let ctx = CommandContext::of("s-1", "/tmp/zk-resume", "kimi-k3", state);
        let registry = CommandRegistry::with_builtin_commands();
        let resume = registry.find_command("resume").expect("registered");
        resume.execute(args, &ctx).await
    }

    /// 无会话 → 旧固定文案。
    #[tokio::test]
    async fn empty_repository_reports_no_previous_sessions() {
        assert_eq!(
            run("", AppState::for_tests()).await,
            CommandResult::text("No previous sessions found.")
        );
    }

    /// 清单行形状：8 字前缀 + 30 宽标题 + `n msgs` + ISO 时刻 + 用法尾行。
    #[tokio::test]
    async fn listing_renders_the_legacy_row_layout() {
        let state = AppState::for_tests();
        let session = state
            .db
            .create_session("kimi-k3", "/tmp/zk-resume")
            .await
            .expect("session created");
        let CommandResult::Text(text) = run("", state).await else {
            panic!("/resume must be text");
        };
        assert!(
            text.starts_with("Recent Sessions:\n\n"),
            "unexpected: {text}"
        );
        assert!(text.ends_with("\nUsage: /resume <session_id>"), "{text}");
        let row = text.lines().nth(2).expect("one session row").to_owned();
        let short_id: String = session.id.chars().take(8).collect();
        // 新建会话无标题 → `(untitled)` 左对齐补白到 30 列。
        assert_eq!(
            row,
            format!(
                "  {short_id}  (untitled)                      0 msgs  {updated}",
                updated = crate::iso::format_rfc3339_micros(session.updated_at)
            )
        );
    }

    /// 有参且会话存在 → 旧成功文案（不改任何服务端状态）。
    #[tokio::test]
    async fn resuming_an_existing_session_returns_the_legacy_text() {
        let state = AppState::for_tests();
        let session = state
            .db
            .create_session("kimi-k3", "/tmp/zk-resume")
            .await
            .expect("session created");
        assert_eq!(
            run(&format!("  {}  ", session.id), state).await,
            CommandResult::text(format!("Session resumed: {}", session.id))
        );
    }

    /// 有参但会话不存在 → 旧 `error` 文案。
    #[tokio::test]
    async fn resuming_a_missing_session_is_an_error() {
        assert_eq!(
            run("ghost", AppState::for_tests()).await,
            CommandResult::error("Session not found: ghost")
        );
    }
}
