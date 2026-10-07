//! Runtime API documentation: Rust is authoritative; Python is queried over UDS.

use axum::{
    Json,
    extract::State,
    http::{Method, StatusCode},
};
use serde_json::{Map, Value, json};
use utoipa::OpenApi;

use super::openapi::ApiDoc;
use crate::{error::ApiError, python::Correlation, state::AppState, workspace::failure};

fn native_spec() -> Result<Value, ApiError> {
    serde_json::to_value(ApiDoc::openapi()).map_err(|_| ApiError::internal())
}

async fn python_spec(state: &AppState) -> Result<Value, ApiError> {
    if !state.config.python_enabled {
        return Err(failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "PYTHON_SERVICE_DISABLED",
            "Python service is disabled",
        ));
    }
    let response = state
        .python
        .forward(
            Method::GET,
            "/api/analysis/openapi/python",
            None,
            &Correlation::default(),
        )
        .await
        .map_err(|_| {
            failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "PYTHON_SERVICE_UNAVAILABLE",
                "Python documentation is unavailable",
            )
        })?;
    if !response.status.is_success() {
        return Err(failure(
            StatusCode::BAD_GATEWAY,
            "PYTHON_OPENAPI_UNAVAILABLE",
            "Python documentation is unavailable",
        ));
    }
    let mut spec: Value = serde_json::from_str(&response.body).map_err(|_| invalid_spec())?;
    if !spec.get("paths").is_some_and(Value::is_object)
        || !spec
            .get("openapi")
            .and_then(Value::as_str)
            .is_some_and(|version| version.starts_with("3."))
    {
        return Err(invalid_spec());
    }
    mark_python_paths(&mut spec);
    Ok(spec)
}

fn invalid_spec() -> ApiError {
    failure(
        StatusCode::BAD_GATEWAY,
        "PYTHON_OPENAPI_INVALID",
        "Python returned an invalid OpenAPI document",
    )
}

const METHODS: [&str; 8] = [
    "get", "post", "put", "patch", "delete", "head", "options", "trace",
];

fn exposed_python_path(path: &str, method: &str) -> bool {
    matches!(method, "get" | "post")
        && !path.starts_with("/api/analysis/openapi/")
        && (path == "/api/files/tree"
            || path == "/api/code-quality/health"
            || ["/api/tokenizer/", "/api/files/analysis/", "/api/analysis/"]
                .iter()
                .any(|prefix| path.starts_with(prefix)))
}

fn mark_python_paths(spec: &mut Value) {
    if let Some(paths) = spec.get_mut("paths").and_then(Value::as_object_mut) {
        for (path, item) in paths {
            if let Some(operations) = item.as_object_mut() {
                for method in METHODS {
                    if let Some(operation) =
                        operations.get_mut(method).and_then(Value::as_object_mut)
                    {
                        operation.insert("x-zk-source".into(), json!("python"));
                        operation.insert("x-zk-transport".into(), json!("unix-domain-socket"));
                        operation.insert(
                            "x-zk-public-gateway".into(),
                            json!(exposed_python_path(path, method)),
                        );
                        if !exposed_python_path(path, method) {
                            let description = operation
                                .get("description")
                                .and_then(Value::as_str)
                                .unwrap_or_default();
                            operation.insert("description".into(), json!(format!("Python internal UDS endpoint; not exposed by the public Rust HTTP gateway. {description}")));
                        }
                    }
                }
            }
        }
    }
}

/// Namespace every component category and its references, including security
/// requirement keys. Python may not overwrite a Rust schema with the same name.
fn namespace_python(value: &mut Value) {
    match value {
        Value::String(reference) if reference.starts_with("#/components/") => {
            let tail = &reference["#/components/".len()..];
            if let Some((category, name)) = tail.split_once('/') {
                *reference = format!("#/components/{category}/Python__{name}");
            }
        }
        Value::Array(values) => values.iter_mut().for_each(namespace_python),
        Value::Object(object) => {
            for child in object.values_mut() {
                namespace_python(child);
            }
            if let Some(Value::Array(requirements)) = object.get_mut("security") {
                for requirement in requirements {
                    if let Some(schemes) = requirement.as_object_mut() {
                        let original = std::mem::take(schemes);
                        *schemes = original
                            .into_iter()
                            .map(|(key, value)| (format!("Python__{key}"), value))
                            .collect();
                    }
                }
            }
            if let Some(Value::String(id)) = object.get_mut("operationId") {
                *id = format!("python_{id}");
            }
        }
        _ => {}
    }
}

fn merge(mut native: Value, mut python: Value) -> Result<Value, ApiError> {
    namespace_python(&mut python);
    // These public URLs are typed Rust adapters, not transparent Python routes.
    // Advertise the actual Project/Session request contract while retaining the
    // aliases' Python response shape. Apply after namespacing so native schema
    // references remain native.
    for (alias, canonical) in [
        (
            "/api/analysis/generate-diagram",
            "/api/code-diagrams/generate",
        ),
        ("/api/analysis/api-endpoints", "/api/code-path/endpoints"),
        ("/api/analysis/code-path", "/api/code-path/trace"),
        ("/api/analysis/cancel", "/api/code-analysis/cancel"),
    ] {
        if let Some(body) = native["paths"][canonical]["post"]
            .get("requestBody")
            .cloned()
            && let Some(operation) = python["paths"][alias]["post"].as_object_mut()
        {
            operation.insert("requestBody".into(), body);
            operation.insert("x-zk-adapter".into(), json!(canonical));
            operation.insert("description".into(), json!("Rust-authorized Project/Session request; snake_case aliases are accepted. A project_root path alone never grants access. Responses retain Python snake_case fields."));
        }
    }
    let mut warnings = Vec::new();
    let python_security = python.get("security").cloned();
    let native_paths = native
        .get_mut("paths")
        .and_then(Value::as_object_mut)
        .ok_or_else(ApiError::internal)?;
    for (path, mut item) in python
        .get("paths")
        .and_then(Value::as_object)
        .ok_or_else(invalid_spec)?
        .clone()
    {
        // Documentation helper URLs are implemented by the Rust aggregator;
        // their Python-only/410 variants do not describe the public gateway.
        if path.starts_with("/api/analysis/openapi/") {
            continue;
        }
        if let Some(operations) = item.as_object_mut() {
            for method in METHODS {
                if let Some(operation) = operations.get_mut(method).and_then(Value::as_object_mut)
                    && let Some(security) = &python_security
                {
                    operation
                        .entry("security")
                        .or_insert_with(|| security.clone());
                }
            }
        }
        if let Some(existing) = native_paths.get_mut(&path).and_then(Value::as_object_mut) {
            for (method, operation) in item.as_object().ok_or_else(invalid_spec)? {
                if existing.contains_key(method) {
                    if METHODS.contains(&method.as_str()) {
                        warnings.push(format!("Rust owns {method} {path}; the Python service variant is available in the Python tab."));
                    }
                } else {
                    existing.insert(method.clone(), operation.clone());
                }
            }
        } else {
            native_paths.insert(path, item);
        }
    }
    let native_components = native
        .as_object_mut()
        .ok_or_else(ApiError::internal)?
        .entry("components")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(ApiError::internal)?;
    if let Some(components) = python.get("components").and_then(Value::as_object) {
        for (category, definitions) in components {
            let destination = native_components
                .entry(category.clone())
                .or_insert_with(|| json!({}))
                .as_object_mut()
                .ok_or_else(invalid_spec)?;
            for (name, definition) in definitions.as_object().ok_or_else(invalid_spec)? {
                if destination
                    .insert(format!("Python__{name}"), definition.clone())
                    .is_some()
                {
                    return Err(invalid_spec());
                }
            }
        }
    }
    let mut tags = Map::new();
    for spec in [&native, &python] {
        if let Some(values) = spec.get("tags").and_then(Value::as_array) {
            for tag in values {
                if let Some(name) = tag.get("name").and_then(Value::as_str) {
                    tags.entry(name).or_insert_with(|| tag.clone());
                }
            }
        }
    }
    native["tags"] = Value::Array(tags.into_values().collect());
    native["warnings"] = json!(warnings);
    native["info"]["title"] = json!("zkcode Rust and Python API");
    native["x-zk-sources"] = json!(["rust", "python"]);
    Ok(native)
}

pub(crate) async fn backend() -> Result<Json<Value>, ApiError> {
    Ok(Json(native_spec()?))
}
pub(crate) async fn python(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(python_spec(&state).await?))
}
pub(crate) async fn merged(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let mut native = native_spec()?;
    match python_spec(&state).await {
        Ok(python) => Ok(Json(merge(native, python)?)),
        Err(error) => {
            native["warnings"] = json!([format!(
                "{}: {}; showing Rust documentation only",
                error.code, error.message
            )]);
            native["x-zk-sources"] = json!(["rust"]);
            Ok(Json(native))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn merge_preserves_native_operations_and_namespaces_components_and_security() {
        let native = json!({"openapi":"3.1.0","info":{},"paths":{"/shared":{"get":{"operationId":"native"}}},"components":{"schemas":{"Result":{"type":"string"}}}});
        let python = json!({"openapi":"3.1.0","paths":{"/shared":{"get":{"operationId":"python"},"post":{"responses":{"200":{"content":{"application/json":{"schema":{"$ref":"#/components/schemas/Result"}}}}}}}},"components":{"schemas":{"Result":{"type":"integer"}},"securitySchemes":{"auth":{"type":"http","scheme":"bearer"}}},"security":[{"auth":[]}]});
        let result = merge(native, python).unwrap();
        assert_eq!(result["paths"]["/shared"]["get"]["operationId"], "native");
        assert_eq!(result["components"]["schemas"]["Result"]["type"], "string");
        assert_eq!(
            result["components"]["schemas"]["Python__Result"]["type"],
            "integer"
        );
        assert_eq!(
            result["paths"]["/shared"]["post"]["responses"]["200"]["content"]["application/json"]["schema"]
                ["$ref"],
            "#/components/schemas/Python__Result"
        );
        assert!(
            result["paths"]["/shared"]["post"]["security"][0]
                .get("Python__auth")
                .is_some()
        );
        assert_eq!(result["warnings"].as_array().unwrap().len(), 1);
    }
}
