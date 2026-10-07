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
use base64::Engine as _;
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

const DOCUMENT_MAX_SIZE: usize = 1_048_576;
const ENTRY_PREFIX: &str = "<!-- zk-memory:v1 ";
const ENTRY_END: &str = "\n<!-- /zk-memory -->\n";

fn render_document(entries: &[zk_db::MemoryRecord]) -> Result<String, ApiError> {
    let mut document = String::new();
    for row in entries {
        let meta = MemoryUpsert {
            id: Some(row.id.clone()),
            category: row.category.clone(),
            title: row.title.clone(),
            content: String::new(),
            keywords: row.keywords.clone(),
            source: Some(row.source.clone()),
        };
        let json = serde_json::to_vec(&meta).map_err(|_| ApiError::internal())?;
        document.push_str(ENTRY_PREFIX);
        document.push_str(&base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json));
        document.push_str(" -->\n");
        document.push_str(
            &row.content
                .split('\n')
                .map(|line| {
                    if line.starts_with('\\')
                        || line.starts_with("<!-- zk-memory:")
                        || line.starts_with("<!-- /zk-memory")
                    {
                        format!("\\{line}")
                    } else {
                        line.to_owned()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n"),
        );
        document.push_str(ENTRY_END);
    }
    Ok(document)
}

fn parse_document(content: &str) -> Result<Vec<MemoryUpsert>, ApiError> {
    if content.len() > DOCUMENT_MAX_SIZE {
        return Err(ApiError::validation("memory document exceeds maxSize"));
    }
    if content.is_empty() {
        return Ok(Vec::new());
    }
    if !content.starts_with(ENTRY_PREFIX) {
        if content.contains("<!-- zk-memory:") || content.contains("<!-- /zk-memory") {
            return Err(ApiError::validation("malformed memory entry marker"));
        }
        let title = content
            .lines()
            .find(|l| !l.trim().is_empty())
            .unwrap_or("记忆")
            .trim_start_matches('#')
            .trim()
            .chars()
            .take(120)
            .collect();
        return Ok(vec![MemoryUpsert {
            id: None,
            category: "SEMANTIC".into(),
            title,
            content: content.into(),
            keywords: None,
            source: Some("USER".into()),
        }]);
    }
    let mut remaining = content;
    let mut entries = Vec::new();
    while !remaining.is_empty() {
        let rest = remaining
            .strip_prefix(ENTRY_PREFIX)
            .ok_or_else(|| ApiError::validation("malformed memory entry marker"))?;
        let (encoded, rest) = rest
            .split_once(" -->\n")
            .ok_or_else(|| ApiError::validation("malformed memory entry header"))?;
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| ApiError::validation("invalid memory entry metadata"))?;
        let mut entry: MemoryUpsert = serde_json::from_slice(&raw)
            .map_err(|_| ApiError::validation("invalid memory entry metadata"))?;
        let (body, rest) = rest
            .split_once(ENTRY_END)
            .ok_or_else(|| ApiError::validation("missing memory entry terminator"))?;
        entry.content = body
            .split('\n')
            .map(|line| line.strip_prefix('\\').unwrap_or(line))
            .collect::<Vec<_>>()
            .join("\n");
        entries.push(entry);
        remaining = rest;
    }
    Ok(entries)
}

fn document_response(snapshot: &zk_db::MemorySnapshot) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        json!({"content": render_document(&snapshot.entries)?, "entries": snapshot.entries, "revision": snapshot.revision, "updatedAt": snapshot.updated_at, "maxSize": DOCUMENT_MAX_SIZE}),
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SaveDocumentRequest {
    #[serde(flatten)]
    selector: MemorySelector,
    expected_revision: i64,
    content: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SaveEntriesRequest {
    #[serde(flatten)]
    selector: MemorySelector,
    expected_revision: i64,
    entries: Vec<MemoryUpsert>,
}

/// Reversible Markdown view; the database entries remain authoritative.
pub(crate) async fn get_document(
    State(state): State<AppState>,
    Query(selector): Query<MemorySelector>,
) -> Result<Json<Value>, ApiError> {
    document_response(&state.db.memory_snapshot(target(&state, selector)?).await?)
}

/// Replace a whole document using the scope revision supplied by the reader.
pub(crate) async fn save_document(
    State(state): State<AppState>,
    Json(request): Json<SaveDocumentRequest>,
) -> Result<Json<Value>, ApiError> {
    let entries = parse_document(&request.content)?;
    document_response(
        &state
            .db
            .replace_memory_scope(
                target(&state, request.selector)?,
                request.expected_revision,
                entries,
            )
            .await?,
    )
}

/// The card editor uses the identical transaction and concurrency token.
pub(crate) async fn save_document_entries(
    State(state): State<AppState>,
    Json(request): Json<SaveEntriesRequest>,
) -> Result<Json<Value>, ApiError> {
    if request
        .entries
        .iter()
        .map(|e| e.content.len())
        .sum::<usize>()
        > DOCUMENT_MAX_SIZE
    {
        return Err(ApiError::validation("memory document exceeds maxSize"));
    }
    document_response(
        &state
            .db
            .replace_memory_scope(
                target(&state, request.selector)?,
                request.expected_revision,
                request.entries,
            )
            .await?,
    )
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

    #[tokio::test]
    async fn markdown_view_roundtrips_metadata_content_and_reserved_markers() {
        let db = zk_db::Db::open_in_memory().unwrap();
        let row = MemoryUpsert {
            id: Some("stable".into()),
            category: "custom".into(),
            title: "含 --> 符号".into(),
            content: "# 正文\n\\path\n<!-- zk-memory:v1 literal -->\n<!-- /zk-memory -->\n\n"
                .into(),
            keywords: Some("a,b".into()),
            source: Some("TOOL".into()),
        };
        db.create_memory(MemoryTarget::global(), row.clone())
            .await
            .unwrap();
        let snapshot = db.memory_snapshot(MemoryTarget::global()).await.unwrap();
        let markdown = render_document(&snapshot.entries).unwrap();
        assert_eq!(parse_document(&markdown).unwrap(), vec![row]);
        assert!(parse_document("<!-- zk-memory:v1 invalid -->\nbody").is_err());
    }

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
