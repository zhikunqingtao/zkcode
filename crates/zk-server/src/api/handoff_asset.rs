//! Authenticated download of immutable, merged source assets.
use crate::{error::ApiError, session_access::require_session_header, state::AppState};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, header},
    response::Response,
};

pub(crate) async fn download(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, reference)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let session = require_session_header(&headers)?;
    let bytes = state
        .db
        .handoff_asset(&session, Some(id), &reference, 1024 * 1024 * 1024)
        .await
        .map_err(|_| {
            ApiError::not_found(
                "HANDOFF_ASSET_NOT_FOUND",
                "Asset not found for this merged session",
            )
        })?;
    let name = reference
        .strip_prefix("asset:")
        .filter(|value| value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| ApiError::validation("invalid asset reference"))?;
    Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"handoff-{name}.bin\""),
        )
        .header("X-Content-Type-Options", "nosniff")
        .header(header::CACHE_CONTROL, "private, no-store")
        .body(Body::from(bytes))
        .map_err(|_| ApiError::internal())
}
