//! Evidence REST service with durable bundles and content-addressed workspace blobs.

use std::io::Write;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use zk_authz::sensitive::SensitiveDataFilter;
use zk_db::{EvidenceBundleRecord, EvidenceItemRecord};

use crate::error::ApiError;
use crate::session_access::{accessible_run, can_access_session, require_session_header};
use crate::state::AppState;

const MAX_BLOB_BYTES: usize = 10 * 1024 * 1024;

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
                Some(store_blob(workspace.clone(), bytes).await?)
            }
            None => None,
        };
        items.push(EvidenceItemRecord {
            id: uuid::Uuid::new_v4().to_string(),
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
    if request.verdict.trim().is_empty() {
        return Err(ApiError::validation("Evidence verdict must not be blank"));
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
) -> Result<Response, ApiError> {
    let asserted = require_session_header(&headers)?;
    let session = require_session(&state, &asserted, &asserted).await?;
    let digest = normalize_digest(&sha256)?;
    let workspace = PathBuf::from(session.working_dir);
    let bytes = read_blob(workspace, digest).await?;
    Ok((
        [(header::CONTENT_TYPE, "application/octet-stream")],
        Body::from(bytes),
    )
        .into_response())
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

pub(crate) async fn store_blob(workspace: PathBuf, bytes: Vec<u8>) -> Result<String, ApiError> {
    tokio::task::spawn_blocking(move || store_blob_blocking(&workspace, &bytes))
        .await
        .map_err(|error| {
            tracing::error!(%error, "evidence blob writer panicked");
            ApiError::internal()
        })?
}

fn store_blob_blocking(workspace: &Path, bytes: &[u8]) -> Result<String, ApiError> {
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
    if target.exists() {
        return Ok(digest);
    }
    let temp = canonical_parent.join(format!(".{digest}.{}.tmp", uuid::Uuid::new_v4()));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)
        .map_err(|_| ApiError::internal())?;
    file.write_all(bytes).map_err(|_| ApiError::internal())?;
    file.sync_all().map_err(|_| ApiError::internal())?;
    match std::fs::rename(&temp, &target) {
        Ok(()) => {}
        Err(_) if target.exists() => {
            let _ = std::fs::remove_file(&temp);
        }
        Err(_) => {
            let _ = std::fs::remove_file(&temp);
            return Err(ApiError::internal());
        }
    }
    Ok(digest)
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
        std::fs::read(canonical).map_err(|_| ApiError::internal())
    })
    .await
    .map_err(|error| {
        tracing::error!(%error, "evidence blob reader panicked");
        ApiError::internal()
    })?
}
