//! Project/Session-bound complexity adapter over the existing killable Python workers.
use super::code_analysis::{AnalysisScope, bind, forward, invalid_response, recheck, with_binding};
use crate::{error::ApiError, state::AppState};
use axum::{Json, extract::State, http::HeaderMap};
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::{Component, Path};
use utoipa::ToSchema;

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ComplexityRequest {
    #[serde(flatten)]
    scope: AnalysisScope,
    #[serde(default, alias = "target_path")]
    target_path: Option<String>,
    #[serde(default)]
    languages: Option<Vec<String>>,
}

#[utoipa::path(post, path="/api/code-quality/complexity",tag="analysis",request_body=ComplexityRequest,responses((status=200,description="Bounded heuristic metrics, not verification evidence")))]
pub(crate) async fn complexity(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ComplexityRequest>,
) -> Result<Json<Value>, ApiError> {
    let binding = bind(&state, &headers, &request.scope).await?;
    if request.languages.as_ref().is_some_and(|languages| {
        languages.is_empty()
            || languages.len() > 4
            || languages.iter().any(|lang| {
                !["python", "java", "typescript", "javascript"].contains(&lang.as_str())
            })
    }) {
        return Err(ApiError::validation_with_code(
            "ANALYSIS_LANGUAGE_UNSUPPORTED",
            "Select Python, Java, TypeScript or JavaScript",
        ));
    }
    let target = request
        .target_path
        .as_deref()
        .filter(|path| !path.trim().is_empty())
        .map(|path| {
            let candidate = Path::new(path);
            if candidate
                .components()
                .any(|part| matches!(part, Component::ParentDir))
            {
                return Err(ApiError::validation("Target must stay inside the project"));
            }
            let absolute = if candidate.is_absolute() {
                candidate.to_path_buf()
            } else {
                binding.root.join(candidate)
            };
            let resolved = absolute
                .canonicalize()
                .map_err(|_| ApiError::validation("Complexity target is unavailable"))?;
            if !resolved.starts_with(&binding.root) {
                return Err(ApiError::validation("Target must stay inside the project"));
            }
            Ok(resolved)
        })
        .transpose()?;
    let payload = with_binding(
        json!({"target_path":target,"languages":request.languages}),
        &binding,
    )?;
    let result = forward(&state, "/api/code-quality/complexity", payload, &binding).await?;
    validate_result(&result, &binding.root)?;
    recheck(&state, &headers, &request.scope, &binding).await?;
    Ok(Json(result))
}

fn validate_result(result: &Value, root: &Path) -> Result<(), ApiError> {
    let data = &result["data"];
    if result["success"] != true
        || data["analysis_kind"] != "heuristic"
        || data["is_verification_evidence"] != false
        || !data["cached"].is_boolean()
        || !data["truncated"].is_boolean()
        || !data["stats"]["total_files"].is_u64()
        || !data["stats"]["avg_cc"].is_number()
        || !data["stats"]["high_risk_count"].is_u64()
    {
        return Err(invalid_response());
    }
    let mut stack = vec![(&data["root"], 0usize)];
    let mut nodes = 0usize;
    while let Some((node, depth)) = stack.pop() {
        nodes += 1;
        if nodes > 20_000
            || depth > 64
            || !node["name"].is_string()
            || !["project", "directory", "file", "class", "method"]
                .contains(&node["type"].as_str().unwrap_or_default())
            || !node["loc"].is_u64()
            || node["cc"]
                .as_f64()
                .is_none_or(|n| !n.is_finite() || n < 0.0)
            || node["mi"]
                .as_f64()
                .is_none_or(|n| !n.is_finite() || !(0.0..=100.0).contains(&n))
            || !["A", "B", "C", "D", "E"].contains(&node["risk_level"].as_str().unwrap_or_default())
        {
            return Err(invalid_response());
        }
        if let Some(path) = node.get("file_path") {
            let path = Path::new(path.as_str().ok_or_else(invalid_response)?);
            if !path.starts_with(root)
                || path.components().any(|p| matches!(p, Component::ParentDir))
            {
                return Err(invalid_response());
            }
        }
        if let Some(children) = node.get("children") {
            for child in children.as_array().ok_or_else(invalid_response)? {
                stack.push((child, depth + 1));
            }
        }
    }
    Ok(())
}
