"""Analysis routes (F25: API Contract, F33: Change Impact, F35: Diagram Generation, F40: Code Path Tracing)."""

import asyncio
import logging
import time
import uuid
from typing import Any, List, Literal, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from workspace_paths import WorkspacePathError, resolve_workspace_path

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Analysis"])

@router.get("/health")
async def health():
    return {"status": "ok", "service": "analysis"}


@router.get("/openapi/python")
async def get_python_openapi(request: Request):
    """The running service's actual schema, including available capability routes."""
    return request.app.openapi()


@router.get("/openapi/merged")
async def get_merged_openapi(request: Request, refresh: bool = False):
    """Standalone UDS callers get an explicitly partial spec; Rust owns aggregation."""
    spec = dict(request.app.openapi())
    spec["warnings"] = ["Python service only; use the Rust gateway for merged documentation"]
    return spec


@router.get("/openapi/java", deprecated=True)
@router.get("/openapi/backend")
async def get_backend_openapi():
    raise HTTPException(410, "Backend documentation is provided by the Rust gateway")


# ═══════════════════════════════════════════════════════════════
# F35: Diagram Generation (时序图 / 流程图)
# ═══════════════════════════════════════════════════════════════


class DiagramOptions(BaseModel):
    depth: int = Field(default=3, ge=1, le=5, description="追踪深度")
    include_tests: bool = Field(default=False, description="是否包含测试文件")
    format: str = Field(default="mermaid", description="输出格式")


class AnalysisControl(BaseModel):
    request_id: Optional[str] = None
    analysis_owner: str = "standalone"

    def job_payload(self) -> dict:
        payload = self.model_dump()
        payload["request_id"] = self.request_id or str(uuid.uuid4())
        return payload


class DiagramRequest(AnalysisControl):
    diagram_type: Literal["sequence", "flowchart"]
    target: str  # API路径或方法签名
    project_root: str  # 项目根目录绝对路径
    options: DiagramOptions = DiagramOptions()


class DiagramMetadataResponse(BaseModel):
    nodes_count: int
    edges_count: int
    languages_analyzed: List[str]
    analysis_time_ms: float


class DiagramGenerationResult(BaseModel):
    diagram_type: str
    mermaid_syntax: str
    confidence_score: float  # 0-1
    metadata: DiagramMetadataResponse
    warnings: List[str] = []


_DIAGRAM_TIMEOUT_SECONDS = 30


@router.post("/generate-diagram", response_model=DiagramGenerationResult)
async def generate_diagram(request: DiagramRequest, http_request: Request = None):
    """生成代码图表（时序图/流程图）— F35"""
    try:
        project_root = resolve_workspace_path(
            request.project_root, require_directory=True)
    except WorkspacePathError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    request = request.model_copy(update={"project_root": str(project_root)})

    if not request.target.strip():
        raise HTTPException(status_code=400, detail="target must not be empty")

    from analysis_jobs import run
    return await run("diagram", request.job_payload(), http_request, _DIAGRAM_TIMEOUT_SECONDS)


def _generate_diagram_sync(request: DiagramRequest) -> DiagramGenerationResult:
    """同步执行图表生成逻辑"""
    if request.diagram_type == "sequence":
        from analyzers.sequence_diagram_generator import SequenceDiagramGenerator
        generator = SequenceDiagramGenerator(project_root=request.project_root)
        diagram_result = generator.generate(
            target=request.target,
            depth=request.options.depth,
            include_tests=request.options.include_tests,
        )
    else:
        from analyzers.flow_chart_generator import FlowChartGenerator
        generator = FlowChartGenerator(project_root=request.project_root)
        diagram_result = generator.generate(
            target=request.target,
            depth=request.options.depth,
        )

    # 如果 confidence_score 为 0 且有 "未找到" 警告，视为 target 未找到
    if diagram_result.confidence_score == 0.0 and any(
        "未找到" in w for w in diagram_result.warnings
    ):
        raise FileNotFoundError(f"Target '{request.target}' not found in project")

    return DiagramGenerationResult(
        diagram_type=diagram_result.diagram_type,
        mermaid_syntax=diagram_result.mermaid_syntax,
        confidence_score=diagram_result.confidence_score,
        metadata=DiagramMetadataResponse(
            nodes_count=diagram_result.metadata.nodes_count,
            edges_count=diagram_result.metadata.edges_count,
            languages_analyzed=diagram_result.metadata.languages_analyzed,
            analysis_time_ms=diagram_result.metadata.analysis_time_ms,
        ),
        warnings=diagram_result.warnings,
    )


# ═══════════════════════════════════════════════════════════════
# F33: Change Impact Analysis
# ═══════════════════════════════════════════════════════════════

class ChangeImpactRequest(AnalysisControl):
    file_path: str = Field(..., description="被修改的文件路径")
    changed_lines: List[int] = Field(..., min_length=1, max_length=20000, description="修改的新版本行号列表")
    project_root: str = Field(..., description="项目根目录")
    depth: int = Field(3, ge=1, le=5, description="BFS 最大深度 (1|3|5)")


class AnalysisError(BaseModel):
    code: str
    message: str
    retryable: bool = False


class ChangeImpactResponse(BaseModel):
    success: bool
    data: Optional[dict[str, Any]] = None
    error: Optional[AnalysisError] = None
    elapsed_ms: float


_change_impact_analyzer = None


def _get_change_impact_analyzer():
    global _change_impact_analyzer
    if _change_impact_analyzer is None:
        from analyzers.change_impact_analyzer import ChangeImpactAnalyzer
        _change_impact_analyzer = ChangeImpactAnalyzer()
    return _change_impact_analyzer


def _change_impact_error(status: int, code: str, message: str, start_ms: float) -> JSONResponse:
    envelope = ChangeImpactResponse(
        success=False,
        error=AnalysisError(code=code, message=message, retryable=status >= 500),
        elapsed_ms=round(time.time() * 1000 - start_ms, 1),
    )
    return JSONResponse(status_code=status, content=envelope.model_dump())


@router.post("/change-impact", response_model=ChangeImpactResponse)
async def analyze_change_impact(request: ChangeImpactRequest, http_request: Request = None):
    """分析代码变更的影响链路 (F33)"""
    start_ms = time.time() * 1000

    try:
        project_root = resolve_workspace_path(
            request.project_root, require_directory=True)
        file_path = resolve_workspace_path(
            request.file_path, base=project_root, require_file=True)
    except WorkspacePathError:
        return _change_impact_error(
            400, "FILE_OUTSIDE_PROJECT",
            "project_root and file_path must resolve inside the workspace",
            start_ms)

    if any(line < 1 for line in request.changed_lines):
        return _change_impact_error(400, "CHANGED_LINES_INVALID", "Changed lines must be positive", start_ms)
    if file_path.suffix.lower() not in {".py", ".java", ".ts", ".tsx", ".js", ".jsx"}:
        return _change_impact_error(400, "ANALYSIS_LANGUAGE_UNSUPPORTED", "Semantic impact is available for Python, Java and TypeScript/JavaScript; use LSP for Rust", start_ms)
    request=request.model_copy(update={"project_root":str(project_root),"file_path":str(file_path),"changed_lines":sorted(set(request.changed_lines))})
    from analysis_jobs import run
    result=await run("impact",request.job_payload(),http_request)
    return ChangeImpactResponse(success=True,data=result,error=None,elapsed_ms=round(time.time()*1000-start_ms,1))


def _change_impact_sync(request: ChangeImpactRequest) -> dict:
    from analyzers.change_impact_analyzer import ChangeImpactAnalyzer
    result=ChangeImpactAnalyzer()._analyze_sync(request.file_path,request.changed_lines,request.project_root,request.depth).model_dump()
    result.update(analysis_kind="advisory",is_verification_evidence=False)
    return result


# ═══════════════════════════════════════════════════════════════
# F40: Code Path Tracing (代码路径追踪)
# ═══════════════════════════════════════════════════════════════


class APIEndpointRequest(AnalysisControl):
    project_root: str = Field(..., description="项目根目录路径")
    languages: Optional[List[str]] = Field(None, description="指定语言过滤")


class CodePathRequest(AnalysisControl):
    project_root: str = Field(..., description="项目根目录路径")
    entry_file: str = Field(..., description="入口方法所在文件路径")
    entry_function: str = Field(..., description="入口方法名")
    max_depth: int = Field(10, ge=1, le=20, description="最大追踪深度")


def _scan_endpoints_sync(request: APIEndpointRequest) -> dict:
    """同步执行 API 端点扫描"""
    from analyzers.code_path_tracer import CodePathTracer
    tracer = CodePathTracer(project_root=request.project_root)
    endpoints = tracer.scan_api_endpoints(languages=request.languages)
    return {
        "success": True,
        "endpoints": [ep.model_dump() for ep in endpoints],
        "total": len(endpoints),
    }


def _trace_code_path_sync(request: CodePathRequest) -> dict:
    """同步执行代码路径追踪"""
    from analyzers.code_path_tracer import CodePathTracer
    tracer = CodePathTracer(project_root=request.project_root)
    result = tracer.trace_code_path(
        entry_file=request.entry_file,
        entry_function=request.entry_function,
        max_depth=request.max_depth,
    )
    if result.entry_node is None:
        raise FileNotFoundError("Entry function not found")
    return {
        "success": True,
        "data": result.model_dump(),
    }


@router.post("/api-endpoints")
async def scan_api_endpoints(request: APIEndpointRequest, http_request: Request = None):
    """扫描项目所有 API 端点 (F40)"""
    try:
        project_root = resolve_workspace_path(
            request.project_root, require_directory=True)
    except WorkspacePathError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    request = request.model_copy(update={"project_root": str(project_root)})

    from analysis_jobs import run
    return await run("endpoints", request.job_payload(), http_request)


@router.post("/code-path")
async def trace_code_path(request: CodePathRequest, http_request: Request = None):
    """追踪指定 API 的完整代码路径 (F40)"""
    try:
        project_root = resolve_workspace_path(
            request.project_root, require_directory=True)
        entry_file = resolve_workspace_path(
            request.entry_file, base=project_root, require_file=True)
    except WorkspacePathError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    request = request.model_copy(update={
        "project_root": str(project_root),
        "entry_file": str(entry_file),
    })

    from analysis_jobs import run
    return await run("trace", request.job_payload(), http_request)


class AnalysisCancelRequest(AnalysisControl):
    project_root: str
    request_id: str


@router.post("/cancel")
async def cancel_analysis(request: AnalysisCancelRequest):
    from analysis_jobs import cancel
    try:
        uuid.UUID(request.request_id)
        root = resolve_workspace_path(request.project_root, require_directory=True)
    except (ValueError, WorkspacePathError) as error:
        raise HTTPException(400, "Invalid analysis cancellation scope") from error
    payload = request.model_dump()
    payload["project_root"] = str(root)
    return cancel(payload)
