//! Evidence REST service with durable bundles and content-addressed workspace blobs.

use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use zk_authz::sensitive::SensitiveDataFilter;
use zk_db::{EvidenceBundleRecord, EvidenceItemRecord, EvidenceOrigin};

use crate::error::ApiError;
use crate::session_access::{accessible_run, can_access_session, require_session_header};
use crate::state::AppState;

const MAX_BLOB_BYTES: usize = 10 * 1024 * 1024;

#[derive(Debug, Default, Deserialize)]
pub(crate) struct BlobQuery {
    #[serde(default)]
    preview: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateEvidenceRequest {
    session_id: String,
    run_id: Option<String>,
    agent_id: Option<String>,
    kind: String,
    claim: Option<String>,
    #[serde(default = "pending_verdict")]
    verdict: String,
    #[serde(default)]
    items: Vec<CreateEvidenceItem>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateEvidenceItem {
    #[serde(rename = "type")]
    item_type: String,
    summary: Option<String>,
    blob_base64: Option<String>,
    meta: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct VerifyEvidenceRequest {
    verdict: String,
}

fn pending_verdict() -> String {
    "pending".to_owned()
}

/// Create one durable evidence bundle.
pub(crate) async fn create_evidence(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateEvidenceRequest>,
) -> Result<(StatusCode, Json<EvidenceBundleRecord>), ApiError> {
    let asserted = require_session_header(&headers)?;
    let session = require_session(&state, &request.session_id, &asserted).await?;
    if let Some(run_id) = request.run_id.as_deref()
        && accessible_run(&state, run_id, &asserted).await?.is_none()
    {
        return Err(ApiError::not_found("RUN_NOT_FOUND", "Run not found"));
    }
    if request.kind.trim().is_empty() {
        return Err(ApiError::validation_with_code(
            "EVIDENCE_KIND_REQUIRED",
            "Evidence kind must not be blank",
        ));
    }
    if !matches!(request.verdict.as_str(), "pending" | "inconclusive") {
        return Err(ApiError::validation_with_code(
            "MODEL_ASSERTION_CANNOT_VERIFY",
            "Submitted claims require a machine check or explicit human review",
        ));
    }
    let workspace = PathBuf::from(session.working_dir);
    let mut items = Vec::with_capacity(request.items.len());
    for (sort_order, item) in request.items.into_iter().enumerate() {
        let blob_sha256 = match item.blob_base64 {
            Some(encoded) => {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|_| {
                        ApiError::validation_with_code(
                            "EVIDENCE_BLOB_INVALID",
                            "blobBase64 is not valid base64",
                        )
                    })?;
                if bytes.len() > MAX_BLOB_BYTES {
                    return Err(ApiError::validation_with_code(
                        "EVIDENCE_BLOB_TOO_LARGE",
                        "Evidence blob exceeds 10 MiB",
                    ));
                }
                Some(store_blob(&state.db, &request.session_id, workspace.clone(), bytes).await?)
            }
            None => None,
        };
        items.push(EvidenceItemRecord {
            id: uuid::Uuid::new_v4().to_string(),
            producer_invocation_id: None,
            item_type: item.item_type,
            summary: item.summary.map(|text| SensitiveDataFilter::filter(&text)),
            blob_sha256,
            meta: item.meta,
            sort_order: i64::try_from(sort_order).unwrap_or(i64::MAX),
        });
    }
    let bundle = EvidenceBundleRecord {
        bundle_id: uuid::Uuid::new_v4().to_string(),
        session_id: request.session_id,
        agent_id: request.agent_id,
        kind: request.kind,
        claim: request.claim.map(|text| SensitiveDataFilter::filter(&text)),
        origin: EvidenceOrigin::ModelAssertion,
        producer_invocation_id: None,
        verdict: request.verdict,
        created_at: crate::iso::format_rfc3339_micros(crate::iso::now_millis()),
        run_id: request.run_id,
        items,
    };
    state.db.save_evidence_bundle(&bundle).await?;
    Ok((StatusCode::CREATED, Json(bundle)))
}

/// Read one bundle with session object authorization.
pub(crate) async fn get_evidence(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(bundle_id): AxumPath<String>,
) -> Result<Json<EvidenceBundleRecord>, ApiError> {
    let asserted = require_session_header(&headers)?;
    let bundle = state
        .db
        .find_evidence_bundle(&bundle_id)
        .await?
        .ok_or_else(|| ApiError::not_found("EVIDENCE_NOT_FOUND", "Evidence bundle not found"))?;
    require_session(&state, &bundle.session_id, &asserted).await?;
    Ok(Json(bundle))
}

/// List bundles for one authorized session.
pub(crate) async fn list_session_evidence(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(session_id): AxumPath<String>,
) -> Result<Json<Vec<EvidenceBundleRecord>>, ApiError> {
    let asserted = require_session_header(&headers)?;
    require_session(&state, &session_id, &asserted).await?;
    Ok(Json(state.db.find_evidence_by_session(&session_id).await?))
}

/// Bind an explicit verification verdict.
pub(crate) async fn verify_evidence(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(bundle_id): AxumPath<String>,
    Json(request): Json<VerifyEvidenceRequest>,
) -> Result<Json<EvidenceBundleRecord>, ApiError> {
    let asserted = require_session_header(&headers)?;
    let bundle = state
        .db
        .find_evidence_bundle(&bundle_id)
        .await?
        .ok_or_else(|| ApiError::not_found("EVIDENCE_NOT_FOUND", "Evidence bundle not found"))?;
    require_session(&state, &bundle.session_id, &asserted).await?;
    if !matches!(
        request.verdict.as_str(),
        "verified" | "failed" | "inconclusive"
    ) {
        return Err(ApiError::validation_with_code(
            "EVIDENCE_REVIEW_VERDICT_INVALID",
            "Human review verdict must be verified, failed or inconclusive",
        ));
    }
    state
        .db
        .update_evidence_verdict(&bundle_id, &request.verdict)
        .await?;
    Ok(Json(
        state
            .db
            .find_evidence_bundle(&bundle_id)
            .await?
            .expect("bundle exists after verdict update"),
    ))
}

/// Read a content-addressed blob from the asserted session workspace.
pub(crate) async fn get_blob(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(sha256): AxumPath<String>,
    Query(query): Query<BlobQuery>,
) -> Result<Response, ApiError> {
    let asserted = require_session_header(&headers)?;
    let session = require_session(&state, &asserted, &asserted).await?;
    let digest = normalize_digest(&sha256)?;
    let workspace = PathBuf::from(session.working_dir);
    if !state.db.evidence_owns_blob(&asserted, &digest).await? {
        return Err(ApiError::not_found(
            "EVIDENCE_BLOB_NOT_FOUND",
            "Evidence blob not found in this session",
        ));
    }
    let bytes = if state.db.session_retention(&asserted).await?
        == zk_db::content::ContentRetention::Ephemeral
    {
        let bytes = state
            .db
            .memory_content_store()
            .get_named_bytes(&asserted, &format!("evidence:{digest}"))?;
        if format!("{:x}", Sha256::digest(&bytes)) != digest {
            return Err(ApiError::internal());
        }
        bytes.to_vec()
    } else {
        read_blob(workspace, digest.clone()).await?
    };
    let mime = if query.preview {
        image_mime(&bytes).ok_or_else(|| {
            ApiError::validation_with_code(
                "EVIDENCE_PREVIEW_UNSUPPORTED",
                "Only PNG and JPEG evidence can be previewed",
            )
        })?
    } else {
        "application/octet-stream"
    };
    let disposition = super::attachment::content_disposition(
        if query.preview {
            "inline"
        } else {
            "attachment"
        },
        &digest,
    );
    Ok((
        [
            (header::CONTENT_TYPE, mime),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::CACHE_CONTROL, "private, no-store"),
            (header::CONTENT_DISPOSITION, disposition.as_str()),
        ],
        Body::from(bytes),
    )
        .into_response())
}

pub(crate) fn image_mime(bytes: &[u8]) -> Option<&'static str> {
    // Use bounded header inspection; optional PNG text/profile metadata is never inflated.
    match zk_llm::payload_guard::image_media_type(bytes).ok()? {
        "image/png" if bytes.ends_with(b"\0\0\0\0IEND\xae\x42\x60\x82") => Some("image/png"),
        "image/jpeg" if bytes.ends_with(b"\xff\xd9") => Some("image/jpeg"),
        _ => None,
    }
}

async fn require_session(
    state: &AppState,
    requested: &str,
    asserted: &str,
) -> Result<zk_db::SessionDetail, ApiError> {
    if !can_access_session(state, requested, asserted).await? {
        return Err(ApiError {
            status: StatusCode::FORBIDDEN,
            code: "SESSION_ACCESS_DENIED".into(),
            message: "Session access denied".into(),
        });
    }
    state
        .db
        .get_session(requested)
        .await?
        .ok_or_else(|| ApiError::session_not_found(requested))
}

fn normalize_digest(value: &str) -> Result<String, ApiError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError::validation_with_code(
            "EVIDENCE_BLOB_HASH_INVALID",
            "sha256 must be 64 hexadecimal characters",
        ));
    }
    Ok(value.to_ascii_lowercase())
}

pub(crate) async fn store_blob(
    db: &zk_db::Db,
    session_id: &str,
    workspace: PathBuf,
    bytes: Vec<u8>,
) -> Result<String, ApiError> {
    if bytes.len() > MAX_BLOB_BYTES {
        return Err(ApiError::validation_with_code(
            "EVIDENCE_BLOB_TOO_LARGE",
            "Evidence blob exceeds 10 MiB",
        ));
    }
    if db.session_retention(session_id).await? == zk_db::content::ContentRetention::Ephemeral {
        let digest = format!("{:x}", Sha256::digest(&bytes));
        db.memory_content_store().put_named_bytes(
            session_id,
            &format!("evidence:{digest}"),
            &bytes,
        )?;
        return Ok(digest);
    }
    tokio::task::spawn_blocking(move || store_blob_blocking(&workspace, &bytes))
        .await
        .map_err(|error| {
            tracing::error!(%error, "evidence blob writer panicked");
            ApiError::internal()
        })?
}

fn store_blob_blocking(workspace: &Path, bytes: &[u8]) -> Result<String, ApiError> {
    if bytes.len() > MAX_BLOB_BYTES {
        return Err(ApiError::validation_with_code(
            "EVIDENCE_BLOB_TOO_LARGE",
            "Evidence blob exceeds 10 MiB",
        ));
    }
    let workspace = std::fs::canonicalize(workspace).map_err(|_| {
        ApiError::validation_with_code("WORKSPACE_UNAVAILABLE", "Workspace is unavailable")
    })?;
    let digest = format!("{:x}", Sha256::digest(bytes));
    let root = workspace.join(".zk/blobs");
    std::fs::create_dir_all(&root).map_err(|_| ApiError::internal())?;
    let canonical_root = std::fs::canonicalize(&root).map_err(|_| ApiError::internal())?;
    if !canonical_root.starts_with(&workspace) {
        return Err(ApiError::validation_with_code(
            "EVIDENCE_BLOB_PATH_ESCAPE",
            "Evidence blob root escapes workspace",
        ));
    }
    let parent = canonical_root.join(&digest[..2]);
    std::fs::create_dir_all(&parent).map_err(|_| ApiError::internal())?;
    let canonical_parent = std::fs::canonicalize(&parent).map_err(|_| ApiError::internal())?;
    if !canonical_parent.starts_with(&canonical_root) {
        return Err(ApiError::validation_with_code(
            "EVIDENCE_BLOB_PATH_ESCAPE",
            "Evidence blob path escapes workspace",
        ));
    }
    let target = canonical_parent.join(&digest);
    if std::fs::symlink_metadata(&target).is_ok() {
        validate_existing_blob(&target, bytes)?;
        return Ok(digest);
    }
    let temp = canonical_parent.join(format!(".{digest}.{}.tmp", uuid::Uuid::new_v4()));
    let write_result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temp)
            .map_err(|_| ApiError::internal())?;
        file.write_all(bytes).map_err(|_| ApiError::internal())?;
        file.sync_all().map_err(|_| ApiError::internal())?;
        match std::fs::hard_link(&temp, &target) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                validate_existing_blob(&target, bytes)
            }
            Err(_) => Err(ApiError::internal()),
        }
    })();
    finish_blob_publication(write_result, std::fs::remove_file(&temp))?;
    Ok(digest)
}

fn finish_blob_publication(
    publication: Result<(), ApiError>,
    cleanup: std::io::Result<()>,
) -> Result<(), ApiError> {
    if let Err(cleanup_error) = cleanup {
        tracing::error!(%cleanup_error, "evidence temporary blob cleanup failed");
        // Preserve a primary publication failure. Cleanup failure after a
        // successful atomic link must not be silently reported as success.
        return publication.and(Err(ApiError {
            status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            code: "EVIDENCE_BLOB_CLEANUP_FAILED".into(),
            message: "Evidence temporary blob cleanup failed".into(),
        }));
    }
    publication
}

fn validate_existing_blob(target: &Path, bytes: &[u8]) -> Result<(), ApiError> {
    let corrupt = || {
        ApiError::validation_with_code(
            "EVIDENCE_BLOB_CORRUPT",
            "Stored blob does not match its digest",
        )
    };
    let metadata = std::fs::symlink_metadata(target).map_err(|_| corrupt())?;
    if !metadata.file_type().is_file() || metadata.len() != bytes.len() as u64 {
        return Err(corrupt());
    }
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(target)
        .map_err(|_| corrupt())?;
    let mut existing = Vec::new();
    std::io::Read::read_to_end(
        &mut std::io::Read::take(&mut file, MAX_BLOB_BYTES as u64 + 1),
        &mut existing,
    )
    .map_err(|_| corrupt())?;
    if existing != bytes {
        return Err(corrupt());
    }
    Ok(())
}

async fn read_blob(workspace: PathBuf, digest: String) -> Result<Vec<u8>, ApiError> {
    tokio::task::spawn_blocking(move || {
        let workspace = std::fs::canonicalize(workspace).map_err(|_| {
            ApiError::validation_with_code("WORKSPACE_UNAVAILABLE", "Workspace is unavailable")
        })?;
        let root = std::fs::canonicalize(workspace.join(".zk/blobs"))
            .map_err(|_| ApiError::not_found("EVIDENCE_BLOB_NOT_FOUND", "Blob not found"))?;
        if !root.starts_with(&workspace) {
            return Err(ApiError::validation_with_code(
                "EVIDENCE_BLOB_PATH_ESCAPE",
                "Evidence blob root escapes workspace",
            ));
        }
        let path = root.join(&digest[..2]).join(&digest);
        let canonical = std::fs::canonicalize(&path)
            .map_err(|_| ApiError::not_found("EVIDENCE_BLOB_NOT_FOUND", "Blob not found"))?;
        if !canonical.starts_with(&root) || !canonical.is_file() {
            return Err(ApiError::not_found(
                "EVIDENCE_BLOB_NOT_FOUND",
                "Blob not found",
            ));
        }
        let metadata = std::fs::metadata(&canonical).map_err(|_| ApiError::internal())?;
        if metadata.len() > MAX_BLOB_BYTES as u64 {
            return Err(ApiError::validation_with_code(
                "EVIDENCE_BLOB_TOO_LARGE",
                "Evidence blob exceeds 10 MiB",
            ));
        }
        let bytes = std::fs::read(canonical).map_err(|_| ApiError::internal())?;
        if format!("{:x}", Sha256::digest(&bytes)) != digest {
            return Err(ApiError::validation_with_code(
                "EVIDENCE_BLOB_CORRUPT",
                "Stored blob does not match its digest",
            ));
        }
        Ok(bytes)
    })
    .await
    .map_err(|error| {
        tracing::error!(%error, "evidence blob reader panicked");
        ApiError::internal()
    })?
}

#[cfg(test)]
mod screenshot_format_tests {
    use super::image_mime;
    use base64::Engine as _;

    #[test]
    fn cleanup_failure_never_turns_publication_into_success_or_masks_primary_failure() {
        let denied = || std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        let cleanup_only = super::finish_blob_publication(Ok(()), Err(denied())).unwrap_err();
        assert_eq!(cleanup_only.code, "EVIDENCE_BLOB_CLEANUP_FAILED");
        let primary = crate::error::ApiError::validation_with_code(
            "EVIDENCE_BLOB_CORRUPT",
            "existing blob mismatch",
        );
        let both = super::finish_blob_publication(Err(primary), Err(denied())).unwrap_err();
        assert_eq!(both.code, "EVIDENCE_BLOB_CORRUPT");
    }

    #[test]
    fn screenshot_headers_require_metadata_and_complete_trailers_without_inflating_png_text() {
        let png = base64::engine::general_purpose::STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==").unwrap();
        assert_eq!(image_mime(&png), Some("image/png"));
        assert_eq!(image_mime(&png[..png.len() - 1]), None);
        let mut ancillary = png[..33].to_vec();
        ancillary.extend_from_slice(&6u32.to_be_bytes());
        ancillary.extend_from_slice(b"zTXtk\0\0\x01\x02\x03");
        ancillary.extend_from_slice(&[0; 4]);
        ancillary.extend_from_slice(&png[33..]);
        assert_eq!(image_mime(&ancillary), Some("image/png"));
        let mut fake = b"\x89PNG\r\n\x1a\n".to_vec();
        fake.extend_from_slice(b"\0\0\0\0IEND\xae\x42\x60\x82");
        for bytes in [&fake[..], b"\xff\xd8\xff\xd9", b"<svg/>", b"GIF89a", b""] {
            assert_eq!(image_mime(bytes), None);
        }
    }
}
