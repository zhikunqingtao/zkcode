//! Real local editor commands. Preferences are durable, keystrokes never leave React.
use axum::{Json, body::Bytes, extract::State};
use futures::future::BoxFuture;
use serde_json::json;

use crate::api::config::{load_user_config, put_config};
use crate::command::context::CommandContext;
use crate::command::traits::{Command, CommandResult, CommandType};

pub(super) struct VimCommand;
impl Command for VimCommand {
    fn name(&self) -> &'static str {
        "vim"
    }
    fn description(&self) -> &'static str {
        "Toggle chat-input Vim mode, or /vim on|off|status; global local-editor preference"
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
            let config = match load_user_config(&ctx.state).await {
                Ok(value) => value,
                Err(error) => return CommandResult::error(error.message),
            };
            let mut preferences = config.editor_preferences.unwrap_or_default();
            let argument = args.trim();
            preferences.vim_enabled = match argument {
                "" => !preferences.vim_enabled,
                "on" => true,
                "off" => false,
                "status" => {
                    return CommandResult::text(format!(
                        "Chat-input Vim: {} (all projects on this device)",
                        if preferences.vim_enabled { "on" } else { "off" }
                    ));
                }
                _ => return CommandResult::error("Usage: /vim [on|off|status]"),
            };
            let body = Bytes::from(json!({"editorPreferences": preferences}).to_string());
            match put_config(State(ctx.state.clone()), body).await {
                Ok(Json(saved)) => CommandResult::jsx(json!({
                    "component": "EditorPreferences",
                    "preferences": saved.config.editor_preferences,
                    "message": "Chat-input Vim settings saved for all projects on this device.",
                })),
                Err(error) => CommandResult::error(error.message),
            }
        })
    }
}

pub(super) struct KeybindingsCommand;
impl Command for KeybindingsCommand {
    fn name(&self) -> &'static str {
        "keybindings"
    }
    fn description(&self) -> &'static str {
        "Open the real local keyboard/Vim settings editor"
    }
    fn command_type(&self) -> CommandType {
        CommandType::LocalJsx
    }
    fn execute<'a>(
        &'a self,
        _args: &'a str,
        _ctx: &'a CommandContext,
    ) -> BoxFuture<'a, CommandResult> {
        Box::pin(async { CommandResult::jsx(json!({"component": "KeybindingsEditor"})) })
    }
}

pub(super) struct FastCommand;
pub(super) struct EffortCommand;
macro_rules! execution_command {
    ($command:ident, $name:literal, $description:literal, $fast:literal) => {
        impl Command for $command {
            fn name(&self) -> &'static str {
                $name
            }
            fn description(&self) -> &'static str {
                $description
            }
            fn command_type(&self) -> CommandType {
                CommandType::LocalJsx
            }
            fn execute<'a>(
                &'a self,
                args: &'a str,
                ctx: &'a CommandContext,
            ) -> BoxFuture<'a, CommandResult> {
                Box::pin(execution_preference(ctx, args, $fast))
            }
        }
    };
}
execution_command!(
    FastCommand,
    "fast",
    "Current-session fast route: /fast [on|off|status]; requires a configured fast model",
    true
);
execution_command!(
    EffortCommand,
    "effort",
    "Current-session reasoning strength: /effort [auto|low|medium|high|xhigh|max]",
    false
);

async fn execution_preference(ctx: &CommandContext, args: &str, fast: bool) -> CommandResult {
    use crate::api::execution_preferences::{
        ExecutionPreferencesPatch, load_execution_preferences, update_execution_preferences,
    };
    let current = match load_execution_preferences(&ctx.state, &ctx.session_id).await {
        Ok(current) => current,
        Err(error) => return CommandResult::error(error.message),
    };
    let argument = args.trim();
    if argument == "status" || (!fast && argument.is_empty()) {
        return CommandResult::text(format!(
            "Current session: fast={}, effort={}, effective model={}. Applies to the next turn; new-session defaults are unchanged.",
            current.fast, current.effort, current.effective_model
        ));
    }
    let mut patch = ExecutionPreferencesPatch {
        revision: current.revision,
        effort: None,
        fast: None,
    };
    if fast {
        patch.fast = Some(match argument {
            "" => !current.fast,
            "on" => true,
            "off" => false,
            _ => return CommandResult::error("Usage: /fast [on|off|status]"),
        });
    } else {
        if !["auto", "low", "medium", "high", "xhigh", "max"].contains(&argument) {
            return CommandResult::error("Usage: /effort [auto|low|medium|high|xhigh|max]");
        }
        patch.effort = Some(argument.to_owned());
    }
    match update_execution_preferences(&ctx.state, &ctx.session_id, patch).await {
        Ok(preferences) => CommandResult::jsx(json!({
            "component": "SessionExecutionPreferences",
            "sessionId": ctx.session_id,
            "message": format!("Current session saved: fast={}, effort={}, effective model={}. Next turn only; new-session defaults unchanged.", preferences.fast, preferences.effort, preferences.effective_model),
        })),
        Err(error) => CommandResult::error(error.message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AppState;

    #[tokio::test]
    async fn vim_command_persists_settings_and_invalid_arguments_do_not_mutate_them() {
        let state = AppState::for_tests();
        let ctx = CommandContext::of("s-1", "/tmp", "kimi-k3", state.clone());
        assert!(matches!(
            VimCommand.execute("on", &ctx).await,
            CommandResult::Jsx(_)
        ));
        assert!(
            load_user_config(&state)
                .await
                .unwrap()
                .editor_preferences
                .unwrap()
                .vim_enabled
        );
        assert!(matches!(
            VimCommand.execute("invalid", &ctx).await,
            CommandResult::Error(_)
        ));
        assert!(
            load_user_config(&state)
                .await
                .unwrap()
                .editor_preferences
                .unwrap()
                .vim_enabled
        );
        assert!(matches!(
            VimCommand.execute("", &ctx).await,
            CommandResult::Jsx(_)
        ));
        assert!(
            !load_user_config(&state)
                .await
                .unwrap()
                .editor_preferences
                .unwrap()
                .vim_enabled
        );
    }

    #[tokio::test]
    async fn execution_commands_use_session_preferences_and_reject_unknown_effort() {
        let state = AppState::for_tests();
        state
            .db
            .create_session_with_id("s-1", "kimi-k3", "/tmp")
            .await
            .unwrap();
        let ctx = CommandContext::of("s-1", "/tmp", "kimi-k3", state.clone());
        assert!(matches!(
            EffortCommand.execute("auto", &ctx).await,
            CommandResult::Jsx(_)
        ));
        let initial = state.db.session_execution_preferences("s-1").await.unwrap();
        assert!(matches!(
            EffortCommand.execute("unknown", &ctx).await,
            CommandResult::Error(_)
        ));
        assert_eq!(
            state
                .db
                .session_execution_preferences("s-1")
                .await
                .unwrap()
                .revision,
            initial.revision
        );
        assert!(matches!(
            FastCommand.execute("off", &ctx).await,
            CommandResult::Jsx(_)
        ));
        assert!(
            !state
                .db
                .session_execution_preferences("s-1")
                .await
                .unwrap()
                .fast
        );
    }
}
