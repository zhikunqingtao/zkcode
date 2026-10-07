//! Local artifact manifest declaration, sealing and integrity verification.

use std::collections::HashMap;
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
    let mut producer_invocations = HashMap::new();
    for entry in &request.entries {
        let invocation_id = state
            .db
            .find_artifact_producer_invocation(&run.id, &entry.tool_use_id)
            .await?
            .ok_or_else(|| {
                ApiError::validation_with_code(
                    "ARTIFACT_PRODUCER_INVOCATION_NOT_FOUND",
                    "Artifact must reference a succeeded tool invocation from the owning run",
                )
            })?;
        producer_invocations.insert(entry.tool_use_id.clone(), invocation_id);
    }
    let entries = tokio::task::spawn_blocking(move || {
        seal_entries(&workspace, request.entries, &producer_invocations)
    })
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
    let expected = manifest.clone();
    manifest = tokio::task::spawn_blocking(move || check_manifest_snapshot(manifest))
        .await
        .map_err(|_| ApiError::internal())?;
    state
        .db
        .save_artifact_verification_cas(&expected, &manifest)
        .await?;
    Ok(manifest)
}

fn seal_entries(
    workspace: &Path,
    entries: Vec<CreateArtifactEntry>,
    producer_invocations: &HashMap<String, String>,
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
                producer_invocation_id: producer_invocations.get(&entry.tool_use_id).cloned(),
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

/// Read-only integrity projection shared by explicit requests and terminal observers.
/// This deliberately does not execute validators, commands, Hooks or browser journeys.
pub(crate) fn check_manifest_snapshot(
    mut manifest: ArtifactManifestRecord,
) -> ArtifactManifestRecord {
    let was_verified = manifest.state == "verified";
    let workspace = std::fs::canonicalize(&manifest.workspace_root)
        .unwrap_or_else(|_| PathBuf::from(&manifest.workspace_root));
    manifest.entries = verify_entries(&workspace, manifest.entries, was_verified);
    manifest.state = if !manifest.entries.is_empty()
        && manifest
            .entries
            .iter()
            .all(|entry| entry.state == "integrity_verified")
    {
        "verified"
    } else if was_verified {
        "unverified"
    } else {
        "failed"
    }
    .into();
    manifest.updated_at = crate::iso::format_rfc3339_micros(crate::iso::now_millis());
    manifest
}

fn verify_entries(
    workspace: &Path,
    entries: Vec<ArtifactEntryRecord>,
    invalidation_check: bool,
) -> Vec<ArtifactEntryRecord> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    let mut remaining = 1024_u64 * 1024 * 1024;
    entries
        .into_iter()
        .map(|mut entry| {
            entry.updated_at = crate::iso::format_rfc3339_micros(crate::iso::now_millis());
            if std::time::Instant::now() >= deadline {
                entry.state = failed_integrity_state(invalidation_check).into();
                entry.failure_code = Some("ARTIFACT_CHECK_TIMEOUT".into());
                entry.actual_hash = None;
                return entry;
            }
            if entry.operation == "deleted" {
                // `Path::exists` follows links and therefore misses a dangling
                // symlink recreated at a path that was sealed as deleted.
                match deleted_bound_path_integrity(workspace, Path::new(&entry.canonical_path)) {
                    Ok(()) => {
                        entry.state = "integrity_verified".into();
                        entry.failure_code = None;
                    }
                    Err(code) => {
                        entry.state = failed_integrity_state(invalidation_check).into();
                        entry.failure_code = Some(code.into());
                        entry.validator_result = None;
                    }
                }
                return entry;
            }
            let sealed_size = entry.file_size;
            let path = Path::new(&entry.canonical_path);
            let result = if !path.is_absolute() || !path.starts_with(workspace) {
                Err(ApiError::validation_with_code(
                    "ARTIFACT_PATH_ESCAPE",
                    "Artifact path escapes workspace",
                ))
            } else {
                reject_special_or_symlink(path)
                    .and_then(|_| hash_file_bounded(path, deadline, &mut remaining))
            };
            match result {
                Ok((actual, size)) => {
                    entry.actual_hash = Some(actual.clone());
                    if sealed_size != Some(size) {
                        entry.state = failed_integrity_state(invalidation_check).into();
                        entry.failure_code = Some("ARTIFACT_SIZE_CHANGED".into());
                        entry.validator_result = None;
                    } else if entry.sealed_hash.as_deref() == Some(actual.as_str()) {
                        entry.state = "integrity_verified".into();
                        entry.failure_code = None;
                    } else {
                        entry.state = failed_integrity_state(invalidation_check).into();
                        entry.failure_code = Some("ARTIFACT_HASH_MISMATCH".into());
                        entry.validator_result = None;
                    }
                }
                Err(error) => {
                    entry.state = failed_integrity_state(invalidation_check).into();
                    entry.failure_code = Some(error.code);
                    entry.actual_hash = None;
                    entry.validator_result = None;
                }
            }
            entry
        })
        .collect()
}

#[cfg(test)]
fn deleted_path_integrity(path: &Path) -> Result<(), &'static str> {
    classify_deleted_path(std::fs::symlink_metadata(path))
}

#[cfg(test)]
fn classify_deleted_path(result: std::io::Result<std::fs::Metadata>) -> Result<(), &'static str> {
    match result {
        Ok(_) => Err("ARTIFACT_DELETED_PATH_EXISTS"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            Err("ARTIFACT_ACCESS_DENIED")
        }
        Err(_) => Err("ARTIFACT_IO_FAILED"),
    }
}

fn failed_integrity_state(invalidation_check: bool) -> &'static str {
    if invalidation_check {
        "unverified"
    } else {
        "failed"
    }
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
    let mut remaining = 1024_u64 * 1024 * 1024;
    hash_file_bounded(
        path,
        std::time::Instant::now() + std::time::Duration::from_secs(8),
        &mut remaining,
    )
}

fn io_error(error: &std::io::Error) -> ApiError {
    let (code, message) = match error.kind() {
        std::io::ErrorKind::NotFound => ("ARTIFACT_FILE_MISSING", "Artifact file is missing"),
        std::io::ErrorKind::PermissionDenied => {
            ("ARTIFACT_ACCESS_DENIED", "Artifact access is denied")
        }
        _ => ("ARTIFACT_IO_FAILED", "Artifact file inspection failed"),
    };
    ApiError::validation_with_code(code, message)
}

fn hash_file_bounded(
    path: &Path,
    deadline: std::time::Instant,
    remaining: &mut u64,
) -> Result<(String, i64), ApiError> {
    let mut file =
        zk_tools::safe_file::open_bound_regular(path).map_err(|error| io_error(&error))?;
    let before = file.metadata().map_err(|error| io_error(&error))?;
    if before.len() > *remaining {
        return Err(ApiError::validation_with_code(
            "ARTIFACT_CHECK_SIZE_LIMIT",
            "Artifact integrity byte budget exhausted",
        ));
    }
    let mut hasher = Sha256::new();
    let mut size = 0_i64;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        if std::time::Instant::now() >= deadline {
            return Err(ApiError::validation_with_code(
                "ARTIFACT_CHECK_TIMEOUT",
                "Artifact integrity time budget exhausted",
            ));
        }
        let read = file.read(&mut buffer).map_err(|error| io_error(&error))?;
        if read == 0 {
            break;
        }
        let count = u64::try_from(read).unwrap_or(u64::MAX);
        if count > *remaining {
            return Err(ApiError::validation_with_code(
                "ARTIFACT_CHECK_SIZE_LIMIT",
                "Artifact integrity byte budget exhausted",
            ));
        }
        *remaining -= count;
        size = size.saturating_add(i64::try_from(read).unwrap_or(i64::MAX));
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata().map_err(|error| io_error(&error))?;
    if before.len() != after.len()
        || before.modified().ok() != after.modified().ok()
        || u64::try_from(size).ok() != Some(after.len())
    {
        return Err(ApiError::validation_with_code(
            "ARTIFACT_CHANGED_DURING_CHECK",
            "Artifact changed while being inspected",
        ));
    }
    // A new file at the same path cannot inherit the descriptor's passing result.
    let path_meta = std::fs::symlink_metadata(path).map_err(|error| io_error(&error))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if path_meta.dev() != after.dev()
            || path_meta.ino() != after.ino()
            || path_meta.ctime() != after.ctime()
            || path_meta.ctime_nsec() != after.ctime_nsec()
        {
            return Err(ApiError::validation_with_code(
                "ARTIFACT_CHANGED_DURING_CHECK",
                "Artifact identity changed while being inspected",
            ));
        }
    }
    Ok((format!("{:x}", hasher.finalize()), size))
}

fn deleted_bound_path_integrity(workspace: &Path, path: &Path) -> Result<(), &'static str> {
    use nix::fcntl::AtFlags;
    use nix::fcntl::{OFlag, open, openat};
    use nix::sys::stat::{Mode, fstatat};
    use std::path::Component;
    if !path.is_absolute() || !path.starts_with(workspace) {
        return Err("ARTIFACT_PATH_ESCAPE");
    }
    let flags = OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_DIRECTORY;
    let mut directory = open("/", flags, Mode::empty()).map_err(|_| "ARTIFACT_ACCESS_DENIED")?;
    let mut parts = path.components().peekable();
    if parts.next() != Some(Component::RootDir) {
        return Err("ARTIFACT_PATH_INVALID");
    }
    while let Some(part) = parts.next() {
        let Component::Normal(name) = part else {
            return Err("ARTIFACT_PATH_INVALID");
        };
        let result = if parts.peek().is_none() {
            return match fstatat(&directory, name, AtFlags::AT_SYMLINK_NOFOLLOW) {
                Ok(_) => Err("ARTIFACT_DELETED_PATH_EXISTS"),
                Err(nix::errno::Errno::ENOENT) => Ok(()),
                Err(nix::errno::Errno::EACCES | nix::errno::Errno::EPERM) => {
                    Err("ARTIFACT_ACCESS_DENIED")
                }
                Err(_) => Err("ARTIFACT_IO_FAILED"),
            };
        } else {
            openat(&directory, name, flags, Mode::empty())
        };
        match result {
            Ok(next) => directory = next,
            Err(nix::errno::Errno::ENOENT) => return Ok(()),
            Err(nix::errno::Errno::EACCES | nix::errno::Errno::EPERM) => {
                return Err("ARTIFACT_ACCESS_DENIED");
            }
            Err(_) => return Err("ARTIFACT_IO_FAILED"),
        }
    }
    Err("ARTIFACT_PATH_INVALID")
}

#[cfg(test)]
mod deletion_integrity_tests {
    use super::{classify_deleted_path, deleted_path_integrity};

    #[test]
    fn failed_inspection_does_not_prove_deletion() {
        assert_eq!(
            classify_deleted_path(Err(std::io::ErrorKind::NotFound.into())),
            Ok(())
        );
        assert_eq!(
            classify_deleted_path(Err(std::io::ErrorKind::PermissionDenied.into())),
            Err("ARTIFACT_ACCESS_DENIED")
        );
        assert_eq!(
            classify_deleted_path(Err(std::io::ErrorKind::Other.into())),
            Err("ARTIFACT_IO_FAILED")
        );
        assert_eq!(
            classify_deleted_path(Err(std::io::ErrorKind::NotADirectory.into())),
            Err("ARTIFACT_IO_FAILED")
        );
    }

    #[cfg(unix)]
    #[test]
    fn dangling_symlink_is_still_an_existing_deleted_artifact() {
        let root =
            std::env::temp_dir().join(format!("zk-artifact-deletion-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("deleted");
        assert_eq!(deleted_path_integrity(&path), Ok(()));
        std::os::unix::fs::symlink(root.join("missing-target"), &path).unwrap();
        assert_eq!(
            deleted_path_integrity(&path),
            Err("ARTIFACT_DELETED_PATH_EXISTS")
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
