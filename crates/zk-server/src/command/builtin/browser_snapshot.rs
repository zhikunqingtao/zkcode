//! `/browser-snapshot`——浏览器语义快照（旧 `BrowserSnapshotCommand.java`）。
//!
//! 类型 `LOCAL`：通过 Python 侧车截取当前浏览器页面，并追加到持久化
//! Browser Replay 时间线。

use futures::future::BoxFuture;
use serde_json::json;

use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};
use crate::python::client::Correlation;
use crate::python::tools::{BROWSER_AUTOMATION, PythonEnvelope};

/// `/browser-snapshot` 命令。
pub(super) struct BrowserSnapshotCommand;

impl Command for BrowserSnapshotCommand {
    fn name(&self) -> &'static str {
        "browser-snapshot"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["snapshot"]
    }

    fn description(&self) -> &'static str {
        "Capture browser semantic snapshot"
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
            let selector = if args.trim().is_empty() {
                None
            } else {
                Some(args.trim())
            };
            let response: Option<PythonEnvelope> = ctx
                .state
                .python
                .call_if_available(
                    BROWSER_AUTOMATION,
                    "/api/browser/snapshot-semantic",
                    &json!({
                        "session_id": ctx.session_id,
                        "selector": selector,
                        "interesting_only": true,
                        "include_screenshot": false,
                        "strict_session": true,
                    }),
                    &Correlation::for_session(Some(&ctx.session_id)),
                )
                .await;
            let Some(response) = response else {
                return CommandResult::error(
                    "BROWSER_AUTOMATION_UNAVAILABLE: Browser semantic snapshot unavailable",
                );
            };
            if !response.success {
                return CommandResult::error(format!(
                    "{}: {}",
                    response.code(),
                    response.message()
                ));
            }
            let data = response.data.unwrap_or(serde_json::Value::Null);
            match ctx
                .state
                .browser_replay
                .append_python_snapshot(&ctx.session_id, &data)
            {
                Ok(snapshot) => CommandResult::text(format!(
                    "Browser semantic snapshot captured: {} interactive elements, {} nodes",
                    snapshot["interactive"].as_array().map_or(0, Vec::len),
                    snapshot["nodeCount"].as_u64().unwrap_or(0)
                )),
                Err(error) => {
                    tracing::error!(
                        error_type = ?error.kind(),
                        "browser snapshot command persistence failed"
                    );
                    CommandResult::error(
                        "BROWSER_REPLAY_PERSIST_FAILED: Browser snapshot could not be persisted",
                    )
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

    #[tokio::test]
    async fn returns_unavailable_message() {
        let ctx = CommandContext::of("s-1", "/tmp", "kimi-k3", AppState::for_tests());
        let registry = CommandRegistry::with_builtin_commands();
        let command = registry
            .find_command("browser-snapshot")
            .expect("registered");
        let result = command.execute("", &ctx).await;
        match result {
            CommandResult::Error(text) => {
                assert!(text.contains("unavailable"), "unexpected: {text}");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn alias_snapshot_resolves() {
        let registry = CommandRegistry::with_builtin_commands();
        let aliased = registry.find_command("snapshot").expect("alias registered");
        assert_eq!(aliased.name(), "browser-snapshot");
    }
}
