"""Scoped, killable code-complexity analysis; metrics are advisory, not verification."""
import time
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import Field
from routers.analysis import AnalysisControl
from workspace_paths import WorkspacePathError, resolve_workspace_path

router = APIRouter(tags=["Code Quality"])


class ComplexityRequest(AnalysisControl):
    project_root: str
    target_path: Optional[str] = None
    languages: Optional[list[Literal["python", "java", "typescript", "javascript"]]] = Field(None, max_length=4)


@router.get("/health")
async def health():
    return {"status": "ok", "service": "code-quality"}


@router.post("/complexity")
async def analyze_complexity(request: ComplexityRequest, http_request: Request = None):
    try:
        root = resolve_workspace_path(request.project_root, require_directory=True)
        target = resolve_workspace_path(request.target_path, base=root) if request.target_path else None
    except WorkspacePathError as error:
        raise HTTPException(400, "Complexity target is outside the authorized project or unavailable") from error
    request = request.model_copy(update={"project_root": str(root), "target_path": str(target) if target else None})
    from analysis_jobs import run
    return await run("complexity", request.job_payload(), http_request, 30)


def analyze_complexity_sync(payload: dict) -> dict:
    from services.complexity_analyzer import ComplexityAnalyzer
    start = time.monotonic()
    result = ComplexityAnalyzer(payload.get("languages"))._analyze_sync(payload["project_root"], payload.get("target_path"))
    elapsed = int((time.monotonic() - start) * 1000)
    result.stats.analysis_time_ms = elapsed
    return {"success": True, "data": {**result.model_dump(exclude_none=True), "analysis_kind": "heuristic", "is_verification_evidence": False}, "elapsed_ms": elapsed}
