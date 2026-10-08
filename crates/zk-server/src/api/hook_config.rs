//! Explicit local hook configuration editing, with content CAS and no execution.
use crate::{error::ApiError, state::AppState};
use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::{Path as FsPath, PathBuf};
use zk_tools::atomic::{ExpectedOldState, sha256_hex, write_checked_bytes_authorized};

const MAX_BYTES: usize = 256 * 1024;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SaveHooks {
    revision: String,
    content: String,
    confirmed: bool,
}

async fn bound_root(state: &AppState, id: &str, headers: &HeaderMap) -> Result<PathBuf, ApiError> {
    if crate::session_access::require_session_header(headers)? != id {
        return Err(ApiError::session_not_found(id));
    }
    let session = state
        .db
        .get_session(id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(id))?;
    crate::workspace::require_current_binding(&state.config, &session.working_dir)
}

fn read_config(root: &FsPath) -> Result<(String, ExpectedOldState), ApiError> {
    use std::io::Read;
    let path = root.join(zk_engine::hook::registry::HOOKS_FILE_REL);
    let file = match zk_tools::safe_file::open_bound_regular(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((String::new(), ExpectedOldState::Absent));
        }
        Err(_) => {
            return Err(ApiError::validation_with_code(
                "HOOK_CONFIG_UNAVAILABLE",
                "Hook configuration is not a safe readable file",
            ));
        }
    };
    let mut bytes = Vec::new();
    file.take(u64::try_from(MAX_BYTES + 1).map_err(|_| ApiError::internal())?)
        .read_to_end(&mut bytes)
        .map_err(|_| ApiError::internal())?;
    if bytes.len() > MAX_BYTES {
        return Err(ApiError::validation_with_code(
            "HOOK_CONFIG_TOO_LARGE",
            "Hook configuration exceeds 256 KiB",
        ));
    }
    let revision = ExpectedOldState::sha256(&sha256_hex(&bytes));
    let content = String::from_utf8(bytes).map_err(|_| {
        ApiError::validation_with_code(
            "HOOK_CONFIG_INVALID_UTF8",
            "Hook configuration must be UTF-8",
        )
    })?;
    Ok((content, revision))
}
fn version(state: &ExpectedOldState) -> &str {
    match state {
        ExpectedOldState::Absent => "absent",
        ExpectedOldState::Sha256(hash) => hash,
    }
}
fn projection(content: &str, revision: &ExpectedOldState) -> Value {
    let parsed = zk_engine::hook::HookRegistry::try_parse(content);
    let diagnostic = parsed
        .as_ref()
        .err()
        .map(|error| zk_authz::analyzer::redact_command(error));
    json!({"content":content,"revision":version(revision),"path":".zk/hooks.toml","hookCount":parsed.as_ref().ok().map(zk_engine::hook::HookRegistry::len),"validationError":parsed.err().map(|_|"HOOK_CONFIG_INVALID"),"validationMessage":diagnostic,"events":zk_engine::hook::HookEvent::ALL.map(zk_engine::hook::HookEvent::as_str)})
}

#[utoipa::path(get,path="/api/sessions/{id}/hooks",tag="hooks",params(("id"=String,Path),("X-Session-Id"=String,Header)),responses((status=200,description="Bound hooks text, content revision and validation state"),(status=404,description="Session not accessible")))]
pub(crate) async fn get(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let root = bound_root(&state, &id, &headers).await?;
    let (content, revision) = tokio::task::spawn_blocking(move || read_config(&root))
        .await
        .map_err(|_| ApiError::internal())??;
    Ok(Json(projection(&content, &revision)))
}

#[utoipa::path(put,path="/api/sessions/{id}/hooks",tag="hooks",params(("id"=String,Path),("X-Session-Id"=String,Header)),responses((status=200,description="Validated replacement saved; no hooks executed"),(status=400,description="Invalid or unconfirmed replacement"),(status=409,description="Content revision changed")))]
pub(crate) async fn put(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(input): Json<SaveHooks>,
) -> Result<Json<Value>, ApiError> {
    let root = bound_root(&state, &id, &headers).await?;
    if !input.confirmed {
        return Err(ApiError::validation_with_code(
            "HOOK_CONFIRMATION_REQUIRED",
            "Confirm the persistent hook configuration change",
        ));
    }
    if state.db.session_retention(&id).await? != zk_db::content::ContentRetention::Persistent {
        return Err(ApiError::validation_with_code(
            "EPHEMERAL_OPERATION_UNSUPPORTED",
            "Temporary conversations cannot save hook configuration",
        ));
    }
    zk_engine::hook::HookRegistry::try_parse(&input.content).map_err(|error| {
        ApiError::validation_with_code(
            "HOOK_CONFIG_INVALID",
            &format!(
                "{}; no changes were saved",
                zk_authz::analyzer::redact_command(&error)
            ),
        )
    })?;
    let conversation = state.conversation();
    let _reservation = conversation
        .as_ref()
        .map(|engine| {
            engine.try_reserve_session_mutation(&id).ok_or_else(|| {
                crate::workspace::failure(
                    axum::http::StatusCode::CONFLICT,
                    "SESSION_BUSY",
                    "Wait for the current task before editing hooks",
                )
            })
        })
        .transpose()?;
    state.db.ensure_session_idle(&id).await?;
    let root_for_read = root.clone();
    let (_, current) = tokio::task::spawn_blocking(move || read_config(&root_for_read))
        .await
        .map_err(|_| ApiError::internal())??;
    if version(&current) != input.revision {
        return Err(crate::workspace::failure(
            axum::http::StatusCode::CONFLICT,
            "HOOK_CONFIG_CHANGED",
            "Hooks changed; reload before saving",
        ));
    }
    let directory = root.join(".zk");
    let root_for_create = root.clone();
    tokio::task::spawn_blocking(move || ensure_config_directory(&root_for_create))
        .await
        .map_err(|_| ApiError::internal())??;
    if tokio::fs::symlink_metadata(&directory)
        .await
        .map_err(|_| ApiError::internal())?
        .file_type()
        .is_symlink()
        || tokio::fs::canonicalize(&directory)
            .await
            .map_err(|_| ApiError::internal())?
            != directory
    {
        return Err(ApiError::validation_with_code(
            "HOOK_CONFIG_UNAVAILABLE",
            "Hook directory identity changed",
        ));
    }
    let target = directory.join("hooks.toml");
    let outcome =
        write_checked_bytes_authorized(&target, input.content.as_bytes(), &current, Some(&target))
            .await;
    if !outcome.success {
        return Err(crate::workspace::failure(
            axum::http::StatusCode::CONFLICT,
            "HOOK_CONFIG_SAVE_UNCONFIRMED",
            "Hooks changed or saving could not be confirmed; reload before retrying",
        ));
    }
    let revision = ExpectedOldState::sha256(&sha256_hex(input.content.as_bytes()));
    Ok(Json(projection(&input.content, &revision)))
}

fn ensure_config_directory(root: &FsPath) -> Result<(), ApiError> {
    use nix::{
        fcntl::{OFlag, open, openat},
        sys::stat::{Mode, mkdirat},
    };
    use std::path::Component;
    let fail = |_| {
        ApiError::validation_with_code("HOOK_CONFIG_UNAVAILABLE", "Hook directory identity changed")
    };
    let flags = OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_DIRECTORY;
    let mut fd = open("/", flags, Mode::empty()).map_err(fail)?;
    for part in root.components() {
        match part {
            Component::RootDir => {}
            Component::Normal(name) => {
                fd = openat(&fd, name, flags, Mode::empty()).map_err(fail)?;
            }
            _ => return Err(ApiError::validation("Invalid bound workspace")),
        }
    }
    match mkdirat(&fd, ".zk", Mode::from_bits_truncate(0o700)) {
        Ok(()) | Err(nix::errno::Errno::EEXIST) => {}
        Err(error) => return Err(fail(error)),
    }
    let _directory = openat(&fd, ".zk", flags, Mode::empty()).map_err(fail)?;
    Ok(())
}
