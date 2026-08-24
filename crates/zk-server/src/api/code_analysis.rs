//! 代码分析端点（Batch 8G Step 5，旧 `CodeDiagramController` /
//! `CodePathController`）。
//!
//! | Method | Path | Handler |
//! |--------|------|---------|
//! | POST | /api/code-diagrams/generate | `generate_diagram` |
//! | POST | /api/code-path/endpoints | `analyze_endpoints` |
//! | POST | /api/code-path/trace | `trace_path` |
//!
//! 实现策略：本 Phase 返回结构化占位响应（旧端同样以 P1 占位居多），
//! 后续可转调 Python 侧车实现真实静态分析。

use axum::Json;
use axum::extract::State;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::ApiError;
use crate::state::AppState;

/// `POST /api/code-diagrams/generate` 请求体。
#[derive(Debug, Deserialize)]
pub(crate) struct DiagramRequest {
    /// 代码片段。
    #[serde(default)]
    pub code: String,
    /// 目标语言（如 `rust` / `java` / `python`）。
    #[serde(default)]
    pub language: Option<String>,
    /// 图表类型提示（如 `flowchart` / `sequence`）。
    #[serde(default)]
    pub diagram_type: Option<String>,
}

/// `POST /api/code-path/endpoints` 请求体。
#[derive(Debug, Deserialize)]
pub(crate) struct EndpointsRequest {
    /// 项目根目录。
    #[serde(default)]
    pub project_path: String,
    /// 语言过滤。
    #[serde(default)]
    pub language: Option<String>,
}

/// `POST /api/code-path/trace` 请求体。
#[derive(Debug, Deserialize)]
pub(crate) struct TraceRequest {
    /// 起始函数/方法名。
    pub entry_point: String,
    /// 项目根目录。
    #[serde(default)]
    pub project_path: String,
    /// 最大追踪深度。
    #[serde(default = "default_depth")]
    pub max_depth: usize,
}

fn default_depth() -> usize {
    10
}

/// `POST /api/code-diagrams/generate`——生成 Mermaid 图表。
///
/// Phase 1：返回基于代码片段的结构化占位响应。后续可转调 Python 侧车
/// 的 `analysis.py` 实现真实 AST 分析。
pub(crate) async fn generate_diagram(
    State(_state): State<AppState>,
    Json(body): Json<DiagramRequest>,
) -> Result<Json<Value>, ApiError> {
    if body.code.is_empty() {
        return Err(ApiError::validation("code field is required"));
    }

    let diagram_type = body.diagram_type.as_deref().unwrap_or("flowchart");
    let language = body.language.as_deref().unwrap_or("auto");
    let line_count = body.code.lines().count();

    Ok(Json(json!({
        "diagram": format!("```mermaid\n{diagram_type} TD\n    A[Entry] --> B[Process]\n    B --> C[Exit]\n```"),
        "diagramType": diagram_type,
        "language": language,
        "sourceLines": line_count,
        "status": "generated"
    })))
}

/// `POST /api/code-path/endpoints`——分析项目 API 端点。
pub(crate) async fn analyze_endpoints(
    State(_state): State<AppState>,
    Json(body): Json<EndpointsRequest>,
) -> Result<Json<Value>, ApiError> {
    if body.project_path.is_empty() {
        return Err(ApiError::validation("project_path is required"));
    }

    Ok(Json(json!({
        "endpoints": [],
        "projectPath": body.project_path,
        "language": body.language,
        "status": "analyzed",
        "note": "Phase 1 placeholder — integrate with Python sidecar for real analysis"
    })))
}

/// `POST /api/code-path/trace`——追踪调用路径。
pub(crate) async fn trace_path(
    State(_state): State<AppState>,
    Json(body): Json<TraceRequest>,
) -> Result<Json<Value>, ApiError> {
    if body.entry_point.is_empty() {
        return Err(ApiError::validation("entry_point is required"));
    }

    Ok(Json(json!({
        "entryPoint": body.entry_point,
        "projectPath": body.project_path,
        "maxDepth": body.max_depth,
        "callGraph": {
            "nodes": [],
            "edges": []
        },
        "status": "traced",
        "note": "Phase 1 placeholder — integrate with Python sidecar for real trace"
    })))
}

#[cfg(test)]
mod tests {
    use super::{DiagramRequest, EndpointsRequest, TraceRequest};

    #[test]
    fn diagram_request_deserializes() {
        let json = r#"{"code": "fn main() {}", "language": "rust"}"#;
        let req: DiagramRequest = serde_json::from_str(json).expect("valid");
        assert_eq!(req.code, "fn main() {}");
        assert_eq!(req.language.as_deref(), Some("rust"));
    }

    #[test]
    fn endpoints_request_deserializes() {
        let json = r#"{"project_path": "/tmp/proj"}"#;
        let req: EndpointsRequest = serde_json::from_str(json).expect("valid");
        assert_eq!(req.project_path, "/tmp/proj");
    }

    #[test]
    fn trace_request_default_depth() {
        let json = r#"{"entry_point": "main", "project_path": "/tmp"}"#;
        let req: TraceRequest = serde_json::from_str(json).expect("valid");
        assert_eq!(req.max_depth, 10);
    }
}
