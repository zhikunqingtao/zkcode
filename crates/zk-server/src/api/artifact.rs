//! Local artifact manifest declaration, sealing and integrity verification.

use std::io::Read;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use zk_db::{ArtifactEntryRecord, ArtifactManifestRecord};

use crate::error::ApiError;
use crate::session_access::{accessible_run, require_session_header};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateManifestRequest {
    run_id: String,
    entries: Vec<CreateArtifactEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateArtifactEntry {
    tool_use_id: String,
    path: String,
    operation: String,
    required_validator_id: Option<String>,
}

/// Create and seal a local manifest from current workspace state.
pub(crate) async fn create_manifest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateManifestRequest>,
) -> Result<(StatusCode, Json<ArtifactManifestRecord>), ApiError> {
    let asserted = require_session_header(&headers)?;
    let run = accessible_run(&state, &request.run_id, &asserted)
        .await?
        .ok_or_else(|| ApiError::not_found("RUN_NOT_FOUND", "Run not found"))?;
    let session = state
        .db
        .get_session(&run.session_id)
        .await?
        .ok_or_else(|| ApiError::session_not_found(&run.session_id))?;
    let workspace = std::fs::canonicalize(&session.working_dir).map_err(|_| {
        ApiError::validation_with_code("WORKSPACE_UNAVAILABLE", "Workspace is unavailable")
    })?;
    let entries = tokio::task::spawn_blocking(move || seal_entries(&workspace, request.entries))
        .await
        .map_err(|error| {
            tracing::error!(%error, "artifact sealing task panicked");
            ApiError::internal()
        })??;
    let now = crate::iso::format_rfc3339_micros(crate::iso::now_millis());
    let manifest = ArtifactManifestRecord {
        manifest_id: uuid::Uuid::new_v4().to_string(),
        run_id: run.id,
        session_id: run.session_id,
        workspace_root: session.working_dir,
        state: "sealed".into(),
        created_at: now.clone(),
        updated_at: now,
        entries,
    };
    state.db.save_artifact_manifest(&manifest).await?;
    Ok((StatusCode::CREATED, Json(manifest)))
}

/// Get the manifest associated with an authorized run.
pub(crate) async fn get_run_manifest(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(run_id): AxumPath<String>,
) -> Result<Json<ArtifactManifestRecord>, ApiError> {
    let asserted = require_session_header(&headers)?;
    accessible_run(&state, &run_id, &asserted)
        .await?
        .ok_or_else(|| ApiError::not_found("RUN_NOT_FOUND", "Run not found"))?;
    let manifest = state
        .db
        .find_artifact_manifest_by_run(&run_id)
        .await?
        .ok_or_else(|| {
            ApiError::not_found("ARTIFACT_MANIFEST_NOT_FOUND", "Artifact manifest not found")
        })?;
    Ok(Json(manifest))
}

/// Verify by manifest id (machine contract route).
pub(crate) async fn verify_manifest(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(manifest_id): AxumPath<String>,
) -> Result<Json<ArtifactManifestRecord>, ApiError> {
    verify_by_id(&state, &headers, &manifest_id).await.map(Json)
}

/// Verify the manifest associated with a run (guide compatibility route).
pub(crate) async fn verify_run_manifest(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(run_id): AxumPath<String>,
) -> Result<Json<ArtifactManifestRecord>, ApiError> {
    let asserted = require_session_header(&headers)?;
    accessible_run(&state, &run_id, &asserted)
        .await?
        .ok_or_else(|| ApiError::not_found("RUN_NOT_FOUND", "Run not found"))?;
    let manifest = state
        .db
        .find_artifact_manifest_by_run(&run_id)
        .await?
        .ok_or_else(|| {
            ApiError::not_found("ARTIFACT_MANIFEST_NOT_FOUND", "Artifact manifest not found")
        })?;
    verify_by_id(&state, &headers, &manifest.manifest_id)
        .await
        .map(Json)
}

async fn verify_by_id(
    state: &AppState,
    headers: &HeaderMap,
    manifest_id: &str,
) -> Result<ArtifactManifestRecord, ApiError> {
    let asserted = require_session_header(headers)?;
    let mut manifest = state
        .db
        .find_artifact_manifest(manifest_id)
        .await?
        .ok_or_else(|| {
            ApiError::not_found("ARTIFACT_MANIFEST_NOT_FOUND", "Artifact manifest not found")
        })?;
    accessible_run(state, &manifest.run_id, &asserted)
        .await?
        .ok_or_else(|| ApiError::not_found("RUN_NOT_FOUND", "Run not found"))?;
    let workspace = std::fs::canonicalize(&manifest.workspace_root).map_err(|_| {
        ApiError::validation_with_code("WORKSPACE_UNAVAILABLE", "Workspace is unavailable")
    })?;
    let entries = std::mem::take(&mut manifest.entries);
    manifest.entries = tokio::task::spawn_blocking(move || verify_entries(&workspace, entries))
        .await
        .map_err(|error| {
            tracing::error!(%error, "artifact verification task panicked");
            ApiError::internal()
        })?;
    manifest.state = if manifest
        .entries
        .iter()
        .all(|entry| entry.state == "integrity_verified")
    {
        "verified".into()
    } else {
        "failed".into()
    };
    manifest.updated_at = crate::iso::format_rfc3339_micros(crate::iso::now_millis());
    state.db.save_artifact_manifest(&manifest).await?;
    Ok(manifest)
}

fn seal_entries(
    workspace: &Path,
    entries: Vec<CreateArtifactEntry>,
) -> Result<Vec<ArtifactEntryRecord>, ApiError> {
    let now = crate::iso::format_rfc3339_micros(crate::iso::now_millis());
    entries
        .into_iter()
        .map(|entry| {
            if !matches!(entry.operation.as_str(), "created" | "modified" | "deleted") {
                return Err(ApiError::validation_with_code(
                    "ARTIFACT_OPERATION_INVALID",
                    "Artifact operation must be created, modified or deleted",
                ));
            }
            let path = resolve_artifact_path(workspace, &entry.path, entry.operation == "deleted")?;
            let (sealed_hash, size) = if entry.operation == "deleted" {
                (None, None)
            } else {
                let metadata = reject_special_or_symlink(&path)?;
                let (hash, size) = hash_file(&path)?;
                debug_assert_eq!(size, i64::try_from(metadata.len()).unwrap_or(i64::MAX));
                (Some(hash), Some(size))
            };
            Ok(ArtifactEntryRecord {
                artifact_id: uuid::Uuid::new_v4().to_string(),
                tool_use_id: entry.tool_use_id,
                canonical_path: path.to_string_lossy().into_owned(),
                operation: entry.operation,
                state: "sealed".into(),
                sealed_hash,
                actual_hash: None,
                file_size: size,
                required_validator_id: entry.required_validator_id,
                validator_result: None,
                failure_code: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            })
        })
        .collect()
}

fn verify_entries(workspace: &Path, entries: Vec<ArtifactEntryRecord>) -> Vec<ArtifactEntryRecord> {
    entries
        .into_iter()
        .map(|mut entry| {
            entry.updated_at = crate::iso::format_rfc3339_micros(crate::iso::now_millis());
            if entry.operation == "deleted" {
                if Path::new(&entry.canonical_path).exists() {
                    entry.state = "failed".into();
                    entry.failure_code = Some("ARTIFACT_DELETED_PATH_EXISTS".into());
                } else {
                    entry.state = "integrity_verified".into();
                    entry.failure_code = None;
                }
                return entry;
            }
            let result = resolve_artifact_path(workspace, &entry.canonical_path, false)
                .and_then(|path| reject_special_or_symlink(&path).map(|_| path))
                .and_then(|path| hash_file(&path));
            match result {
                Ok((actual, size)) => {
                    entry.actual_hash = Some(actual.clone());
                    entry.file_size = Some(size);
                    if entry.sealed_hash.as_deref() == Some(actual.as_str()) {
                        entry.state = "integrity_verified".into();
                        entry.failure_code = None;
                    } else {
                        entry.state = "failed".into();
                        entry.failure_code = Some("ARTIFACT_HASH_MISMATCH".into());
                    }
                }
                Err(error) => {
                    entry.state = "failed".into();
                    entry.failure_code = Some(error.code);
                }
            }
            entry
        })
        .collect()
}

fn resolve_artifact_path(
    workspace: &Path,
    value: &str,
    allow_missing: bool,
) -> Result<PathBuf, ApiError> {
    let candidate = PathBuf::from(value);
    let candidate = if candidate.is_absolute() {
        candidate
    } else {
        workspace.join(candidate)
    };
    if candidate.exists()
        && std::fs::symlink_metadata(&candidate)
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
    {
        return Err(ApiError::validation_with_code(
            "ARTIFACT_SYMLINK_FORBIDDEN",
            "Artifact path must not be a symlink",
        ));
    }
    let resolved = if allow_missing && !candidate.exists() {
        let parent = candidate.parent().ok_or_else(|| {
            ApiError::validation_with_code("ARTIFACT_PATH_INVALID", "Artifact path is invalid")
        })?;
        let parent = std::fs::canonicalize(parent).map_err(|_| {
            ApiError::validation_with_code("ARTIFACT_PATH_INVALID", "Artifact parent is missing")
        })?;
        parent.join(candidate.file_name().ok_or_else(|| {
            ApiError::validation_with_code("ARTIFACT_PATH_INVALID", "Artifact path is invalid")
        })?)
    } else {
        std::fs::canonicalize(&candidate).map_err(|_| {
            ApiError::validation_with_code("ARTIFACT_FILE_MISSING", "Artifact file is missing")
        })?
    };
    if !resolved.starts_with(workspace) {
        return Err(ApiError::validation_with_code(
            "ARTIFACT_PATH_ESCAPE",
            "Artifact path escapes workspace",
        ));
    }
    Ok(resolved)
}

fn reject_special_or_symlink(path: &Path) -> Result<std::fs::Metadata, ApiError> {
    let link_metadata = std::fs::symlink_metadata(path).map_err(|_| {
        ApiError::validation_with_code("ARTIFACT_FILE_MISSING", "Artifact file is missing")
    })?;
    if link_metadata.file_type().is_symlink() {
        return Err(ApiError::validation_with_code(
            "ARTIFACT_SYMLINK_FORBIDDEN",
            "Artifact path must not be a symlink",
        ));
    }
    if !link_metadata.is_file() {
        return Err(ApiError::validation_with_code(
            "ARTIFACT_SPECIAL_FILE_FORBIDDEN",
            "Artifact must be a regular file",
        ));
    }
    Ok(link_metadata)
}

fn hash_file(path: &Path) -> Result<(String, i64), ApiError> {
    let mut file = std::fs::File::open(path).map_err(|_| ApiError::internal())?;
    let mut hasher = Sha256::new();
    let mut size = 0_i64;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer).map_err(|_| ApiError::internal())?;
        if read == 0 {
            break;
        }
        size = size.saturating_add(i64::try_from(read).unwrap_or(i64::MAX));
        hasher.update(&buffer[..read]);
    }
    Ok((format!("{:x}", hasher.finalize()), size))
}
