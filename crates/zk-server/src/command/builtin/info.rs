//! 会话/系统信息族——`/cost`、`/session`、`/status`
//! （旧 `command/impl/CostCommand.java` / `SessionCommand.java` /
//! `StatusCommand.java`）。
//!
//! 三条命令在旧实现里都读同一个进程级 `AppStateStore`（`cost()` / `session()`）
//! 或 `SessionManager`；zkcode 无进程级会话单例，事实源改为：
//!
//! | 旧读取点 | 本实现 |
//! |---|---|
//! | `state.cost().totalCostUsd()` | [`crate::cost::AtomicCostTracker::global_cost`]（进程内全局累计） |
//! | `state.cost().sessionCostUsd()` | `AtomicCostTracker::session_cost(sessionId)` |
//! | `state.cost().totalUsage()` | [`zk_db::SessionDetail::total_usage`]（落库累计） |
//! | `state.session().messages().size()` | `SessionDetail::messages.len()` |
//! | `state.session().turnCount()` | `SessionDetail::messages` 中 user 角色条数（见 [`SessionCommand`]） |
//! | `sessionManager.loadSession(id)` | [`zk_db::Db::get_session`] |
//!
//! # 与旧实现的刻意差异（留痕）
//!
//! 两条成本行取**进程内**累加器（与旧 `CostState` 同一语义：进程重启归零，
//! 故恒满足 `Total ≥ Session`）；四条 token 行取**落库**累计（zkcode 无进程内
//! usage 累加器，且落库值才是会话的完整事实）。重启后同一会话会出现「token 非
//! 零而成本为 0」——旧实现重启后两者同时归零，此为唯一可观察差异，持久化的
//! 会话成本可经 `GET /api/sessions` 查看。

use std::fmt::Write as _;

use futures::future::BoxFuture;
use zk_db::{MessageRole, SessionDetail};
use zk_engine::CostTracker;
use zk_protocol::Usage;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

/// `/cost`——会话成本与 token 用量（旧 `CostCommand`）。
pub(super) struct CostCommand;

impl Command for CostCommand {
    fn name(&self) -> &'static str {
        "cost"
    }

    fn description(&self) -> &'static str {
        "Show session cost and token usage"
    }

    fn command_type(&self) -> CommandType {
        CommandType::Local
    }

    fn supports_non_interactive(&self) -> bool {
        true
    }

    fn execute<'a>(
        &'a self,
        _args: &'a str,
        ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async move {
            let usage = load_session(ctx)
                .await
                .map_or_else(Usage::default, |detail| detail.total_usage);
            let total_cost = ctx.state.costs.global_cost();
            let session_cost = ctx.state.costs.session_cost(&ctx.session_id);
            // 旧 L32-39 的六行（对齐空格与 `$` 前缀逐字，`%.4f` 四位小数）。
            let mut text = String::from("Session Cost Summary:\n\n");
            let _ = writeln!(text, "  Total Cost:     ${total_cost:.4}");
            let _ = writeln!(text, "  Session Cost:   ${session_cost:.4}");
            let _ = writeln!(text, "  Input Tokens:   {}", usage.input_tokens);
            let _ = writeln!(text, "  Output Tokens:  {}", usage.output_tokens);
            let _ = writeln!(text, "  Cache Read:     {}", usage.cache_read_input_tokens);
            let _ = writeln!(
                text,
                "  Cache Create:   {}",
                usage.cache_creation_input_tokens
            );
            CommandResult::text(text)
        })
    }
}

/// `/session`——当前会话信息（旧 `SessionCommand`）。
///
/// `Turns:` 行在旧实现取 `AppState.session().turnCount()`（每次
/// `executeQuery` 自增的进程内计数器）。落库模型无该列，此处以 **user 角色
/// 消息条数**派生——一轮对话恰有一条用户消息，与旧计数器的增长点一一对应。
pub(super) struct SessionCommand;

impl Command for SessionCommand {
    fn name(&self) -> &'static str {
        "session"
    }

    fn description(&self) -> &'static str {
        "Show session information"
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
            // 旧实现读全局 AppState，会话缺失时两个计数均为 0（空状态）。
            let detail = load_session(ctx).await;
            let (turns, messages) = detail.as_ref().map_or((0, 0), |detail| {
                let turns = detail
                    .messages
                    .iter()
                    .filter(|record| record.role == MessageRole::User)
                    .count();
                (turns, detail.messages.len())
            });
            // 旧 L31-36 的五行（对齐空格逐字）。
            let mut text = String::from("Session Information:\n\n");
            let _ = writeln!(text, "  ID:        {}", ctx.session_id);
            let _ = writeln!(text, "  Model:     {}", ctx.current_model);
            let _ = writeln!(text, "  Directory: {}", ctx.working_dir);
            let _ = writeln!(text, "  Turns:     {turns}");
            let _ = writeln!(text, "  Messages:  {messages}");
            CommandResult::text(text)
        })
    }
}

/// `/status`（别名 `/info`）——会话与系统状态（旧 `StatusCommand`）。
///
/// 旧输出为中文 Markdown 片段，且**不含** uptime / 活跃会话数（那两项在旧
/// 实现里属 `/api/health`）；本移植按旧输出逐字，不自行扩项。
pub(super) struct StatusCommand;

impl Command for StatusCommand {
    fn name(&self) -> &'static str {
        "status"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["info"]
    }

    fn description(&self) -> &'static str {
        "显示当前会话和系统状态"
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
            let mut text = String::from("## 系统状态\n\n");
            let _ = writeln!(text, "- 会话 ID: {}", ctx.session_id);
            // 旧 L29-37：会话存在列四项，否则单行「未找到」。
            match load_session(ctx).await {
                Some(detail) => {
                    let _ = writeln!(text, "- 消息数: {}", detail.messages.len());
                    let _ = writeln!(text, "- Token 用量: {}", render_usage(&detail.total_usage));
                    let _ = writeln!(text, "- 工作目录: {}", detail.working_dir);
                    let _ = writeln!(text, "- 当前模型: {}", detail.model);
                }
                None => text.push_str("- 会话状态: 未找到\n"),
            }
            CommandResult::text(text)
        })
    }
}

/// 会话读取（读库失败与「不存在」都退化为 `None`——旧实现读内存态无失败路径，
/// 三条命令的旧行为在会话缺失时均为「按空状态渲染」，故合流到同一分支）。
async fn load_session(ctx: &CommandContext) -> Option<SessionDetail> {
    match ctx.state.db.get_session(&ctx.session_id).await {
        Ok(detail) => detail,
        Err(err) => {
            tracing::warn!(session_id = %ctx.session_id, error = %err, "session load failed for info command");
            None
        }
    }
}

/// 旧 `Usage` record 的 `toString()` 形状——`/status` 的「Token 用量」行直接
/// 拼接 record，Java 编译器生成的形状为
/// `Usage[c1=v1, c2=v2, ...]`（组件序即 record 声明序）。
fn render_usage(usage: &Usage) -> String {
    format!(
        "Usage[inputTokens={input}, outputTokens={output}, \
         cacheReadInputTokens={cache_read}, cacheCreationInputTokens={cache_create}]",
        input = usage.input_tokens,
        output = usage.output_tokens,
        cache_read = usage.cache_read_input_tokens,
        cache_create = usage.cache_creation_input_tokens
    )
}

#[cfg(test)]
mod tests {
    use zk_engine::CostTracker;
    use zk_protocol::Usage;

    use super::render_usage;
    use crate::command::traits::CommandResult;
    use crate::command::{CommandContext, CommandRegistry};
    use crate::state::AppState;

    async fn run(name: &str, session_id: &str, state: AppState) -> CommandResult {
        let ctx = CommandContext::of(session_id, "/tmp/zk-info", "kimi-k3", state);
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry.find_command(name).expect("registered");
        command.execute("", &ctx).await
    }

    /// `/cost` 全零态的六行逐字（`$0.0000` 四位小数）。
    #[tokio::test]
    async fn cost_renders_the_legacy_summary_block() {
        let CommandResult::Text(text) = run("cost", "s-1", AppState::for_tests()).await else {
            panic!("/cost must be text");
        };
        assert_eq!(
            text,
            "Session Cost Summary:\n\n\
             \x20 Total Cost:     $0.0000\n\
             \x20 Session Cost:   $0.0000\n\
             \x20 Input Tokens:   0\n\
             \x20 Output Tokens:  0\n\
             \x20 Cache Read:     0\n\
             \x20 Cache Create:   0\n"
        );
    }

    /// `/cost` 的两条成本行读 `AppState` 的累加器实例（引擎与命令同源）。
    #[tokio::test]
    async fn cost_reads_the_shared_process_wide_tracker() {
        let state = AppState::for_tests();
        // kimi-k3 输入费率 0.002/1k → 1.5M token 恰好 $3.0000。
        state.costs.add_usage(
            "s-1",
            "kimi-k3",
            &Usage {
                input_tokens: 1_500_000,
                output_tokens: 0,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
        );
        let CommandResult::Text(text) = run("cost", "s-1", state).await else {
            panic!("/cost must be text");
        };
        assert!(
            text.contains("  Session Cost:   $3.0000\n"),
            "unexpected output: {text}"
        );
        assert!(
            text.contains("  Total Cost:     $3.0000\n"),
            "unexpected output: {text}"
        );
    }

    /// `/session` 五行逐字；`Turns` 取 user 消息条数，会话缺失时两计数为 0。
    #[tokio::test]
    async fn session_renders_the_legacy_information_block() {
        let CommandResult::Text(text) = run("session", "s-1", AppState::for_tests()).await else {
            panic!("/session must be text");
        };
        assert_eq!(
            text,
            "Session Information:\n\n\
             \x20 ID:        s-1\n\
             \x20 Model:     kimi-k3\n\
             \x20 Directory: /tmp/zk-info\n\
             \x20 Turns:     0\n\
             \x20 Messages:  0\n"
        );
    }

    /// `/status` 会话缺失分支：单行「未找到」（旧 else 分支）。
    #[tokio::test]
    async fn status_reports_a_missing_session() {
        let CommandResult::Text(text) = run("status", "ghost", AppState::for_tests()).await else {
            panic!("/status must be text");
        };
        assert_eq!(
            text,
            "## 系统状态\n\n- 会话 ID: ghost\n- 会话状态: 未找到\n"
        );
    }

    /// `/status` 会话存在分支：四项取落库事实（目录/模型为库内原值）。
    #[tokio::test]
    async fn status_reports_persisted_session_facts() {
        let state = AppState::for_tests();
        let session = state
            .db
            .create_session("kimi-k9", "/tmp/zk-status")
            .await
            .expect("session created");
        let CommandResult::Text(text) = run("status", &session.id, state).await else {
            panic!("/status must be text");
        };
        assert!(text.contains("- 消息数: 0\n"), "unexpected: {text}");
        assert!(
            text.contains(
                "- Token 用量: Usage[inputTokens=0, outputTokens=0, cacheReadInputTokens=0, cacheCreationInputTokens=0]\n"
            ),
            "unexpected: {text}"
        );
        assert!(
            text.contains("- 工作目录: /tmp/zk-status\n"),
            "unexpected: {text}"
        );
        assert!(text.contains("- 当前模型: kimi-k9\n"), "unexpected: {text}");
    }

    /// `Usage` 渲染形状即 Java record `toString()`（组件名与声明序逐字）。
    #[test]
    fn usage_rendering_matches_the_java_record_to_string() {
        let usage = Usage {
            input_tokens: 1,
            output_tokens: 2,
            cache_read_input_tokens: 3,
            cache_creation_input_tokens: 4,
        };
        assert_eq!(
            render_usage(&usage),
            "Usage[inputTokens=1, outputTokens=2, cacheReadInputTokens=3, cacheCreationInputTokens=4]"
        );
    }
}
