//! SQLite-authoritative memory HTTP API.
//!
//! Every endpoint defaults to the configured project. Global memory is
//! reachable only through an explicit `scope=global` query or body field.
//! `MEMORY.md` and `MemdirStore` are not read or written by this API.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{Value, json};
use zk_db::{MemoryScope, MemoryTarget, MemoryUpsert};

use crate::error::ApiError;
use crate::state::AppState;

/// Scope selector shared by read/delete query strings and write bodies.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct MemorySelector {
    /// Omitted means project; global must be explicit.
    scope: Option<MemoryScope>,
    /// Optional project override; omitted uses `workspace_default_root`.
    project_path: Option<String>,
}

/// One write row plus its explicit-or-default selector.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScopedMemoryUpsert {
    #[serde(default)]
    scope: Option<MemoryScope>,
    #[serde(default)]
    project_path: Option<String>,
    #[serde(flatten)]
    entry: MemoryUpsert,
}

impl ScopedMemoryUpsert {
    fn into_parts(self, state: &AppState) -> Result<(MemoryTarget, MemoryUpsert), ApiError> {
        let target = target(
            state,
            MemorySelector {
                scope: self.scope,
                project_path: self.project_path,
            },
        )?;
        Ok((target, self.entry))
    }
}

/// Batch upsert request.
#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct UpdateMemoriesRequest {
    pub entries: Vec<ScopedMemoryUpsert>,
}

fn target(state: &AppState, selector: MemorySelector) -> Result<MemoryTarget, ApiError> {
    match selector.scope.unwrap_or(MemoryScope::Project) {
        MemoryScope::Project => MemoryTarget::project(
            selector
                .project_path
                .unwrap_or_else(|| state.config.workspace_default_root.clone()),
        )
        .map_err(|_| ApiError::validation("projectPath must not be blank")),
        MemoryScope::Global => {
            if selector.project_path.is_some() {
                return Err(ApiError::validation(
                    "projectPath cannot be supplied for global memory",
                ));
            }
            Ok(MemoryTarget::global())
        }
    }
}

/// List memory in one scope; project is the default.
#[utoipa::path(
    get,
    path = "/api/memory",
    tag = "memory",
    responses((status = 200, description = "Scoped SQLite memory rows"))
)]
pub(crate) async fn get_memories(
    State(state): State<AppState>,
    Query(selector): Query<MemorySelector>,
) -> Result<Json<Value>, ApiError> {
    let entries = state.db.list_memories(target(&state, selector)?).await?;
    Ok(Json(json!({ "entries": entries })))
}

/// Upsert memory rows. Each omitted scope targets the configured project.
#[utoipa::path(
    put,
    path = "/api/memory",
    tag = "memory",
    responses(
        (status = 200, description = "{\"success\":true}"),
        (status = 400, description = "Malformed body or invalid scope")
    )
)]
pub(crate) async fn update_memories(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let request = serde_json::from_slice::<UpdateMemoriesRequest>(&body)
        .map_err(|_| ApiError::invalid_request_body())?;
    let total = request.entries.len();
    for scoped in request.entries {
        let (target, entry) = scoped.into_parts(&state)?;
        state.db.update_memory(target, entry).await?;
    }
    tracing::info!(total, "Updated scoped SQLite memory entries");
    Ok(Json(json!({ "success": true })))
}

/// Create one memory row; omitted scope targets the configured project.
#[utoipa::path(
    post,
    path = "/api/memory",
    tag = "memory",
    responses(
        (status = 201, description = "{\"success\":true,\"id\":\"…\"}"),
        (status = 400, description = "Malformed body or invalid scope")
    )
)]
pub(crate) async fn create_memory(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let scoped = serde_json::from_slice::<ScopedMemoryUpsert>(&body)
        .map_err(|_| ApiError::invalid_request_body())?;
    let (target, entry) = scoped.into_parts(&state)?;
    let source = entry.source.clone().unwrap_or_else(|| "USER".to_owned());
    let id = state.db.create_memory(target, entry).await?;
    tracing::info!(id = %id, source = %source, "Created scoped SQLite memory entry");
    Ok((
        StatusCode::CREATED,
        Json(json!({ "success": true, "id": id })),
    ))
}

/// Alias for the scoped `SQLite` list. It intentionally has no `memoryMd`
/// branch; `SQLite` is the only authority.
#[utoipa::path(
    get,
    path = "/api/memory/all",
    tag = "memory",
    responses((status = 200, description = "Scoped SQLite memory rows"))
)]
pub(crate) async fn get_all_memories(
    State(state): State<AppState>,
    Query(selector): Query<MemorySelector>,
) -> Result<Json<Value>, ApiError> {
    let entries = state.db.list_memories(target(&state, selector)?).await?;
    Ok(Json(json!({ "entries": entries })))
}

/// Delete one id from the selected scope only.
#[utoipa::path(
    delete,
    path = "/api/memory/{memoryId}",
    tag = "memory",
    params(("memoryId" = String, Path, description = "Memory id")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "No row in the selected scope")
    )
)]
pub(crate) async fn delete_memory(
    State(state): State<AppState>,
    AxumPath(memory_id): AxumPath<String>,
    Query(selector): Query<MemorySelector>,
) -> Result<Response, ApiError> {
    if state
        .db
        .delete_memory(target(&state, selector)?, &memory_id)
        .await?
    {
        tracing::info!(id = %memory_id, "Deleted scoped SQLite memory entry");
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    Ok(StatusCode::NOT_FOUND.into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_scope_deserializes_as_project_default() {
        let entry = serde_json::from_str::<ScopedMemoryUpsert>(
            r#"{"category":"SEMANTIC","title":"t","content":"c"}"#,
        )
        .expect("valid entry");
        assert_eq!(entry.scope, None);
    }

    #[test]
    fn missing_required_memory_fields_is_rejected() {
        assert!(serde_json::from_str::<ScopedMemoryUpsert>("{}").is_err());
    }
}
