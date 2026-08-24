//! Read-only REST projection of the shared slash-command registry.

use axum::Json;
use axum::extract::{Path, State};
use serde::Serialize;

use crate::error::ApiError;
use crate::state::AppState;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommandView {
    name: String,
    aliases: Vec<String>,
    description: String,
    command_type: String,
    immediate: bool,
    version: String,
    supports_non_interactive: bool,
}

fn view(command: &dyn crate::command::Command) -> CommandView {
    CommandView {
        name: command.name().to_owned(),
        aliases: command
            .aliases()
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        description: command.description().to_owned(),
        command_type: command.command_type().as_str().to_owned(),
        immediate: command.is_immediate(),
        version: command.version().to_owned(),
        supports_non_interactive: command.supports_non_interactive(),
    }
}

/// `GET /api/commands` — visible commands in stable registry order.
pub(crate) async fn list(State(state): State<AppState>) -> Json<Vec<CommandView>> {
    Json(
        state
            .commands
            .visible_commands()
            .iter()
            .map(|command| view(command.as_ref()))
            .collect(),
    )
}

/// `GET /api/commands/{name}` — name and aliases resolve identically to WS.
pub(crate) async fn get(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<CommandView>, ApiError> {
    let command = state.commands.find_command(&name).ok_or_else(|| {
        ApiError::not_found("COMMAND_NOT_FOUND", &format!("Command not found: {name}"))
    })?;
    Ok(Json(view(command.as_ref())))
}
