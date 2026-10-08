"""
Journey Run 端点 Pydantic 模型 — 用户旅程验证 DSL 请求/响应定义
"""

from pydantic import BaseModel, Field, model_validator
from typing import List, Dict, Any, Optional


class JourneyRunRequest(BaseModel):
    session_id: Optional[str] = Field(default=None, pattern=r"^[A-Za-z0-9_-]{1,64}$")
    ephemeral_content: bool = False
    base_url: str
    steps: List[Dict[str, Any]] = Field(min_length=1)
    record: Dict[str, bool] = {}
    recording: Optional[Dict[str, str]] = None
    viewport: Dict[str, int] = {"width": 1280, "height": 800}
    mode: str = "browser"  # "browser" 或 "http_api"
    # Optional for existing callers. Java/Python normally share the host clock;
    # an absolute deadline also rejects requests delayed beyond the caller budget.
    deadline_epoch_ms: Optional[int] = None

    @model_validator(mode="after")
    def reject_ephemeral_recording(self):
        if any(key not in {"trace", "har", "video"} for key in self.record):
            raise ValueError("RECORDING_OPTION_UNSUPPORTED")
        if self.ephemeral_content and any(self.record.values()):
            raise ValueError("EPHEMERAL_RECORDING_UNSUPPORTED")
        return self


class StepResultModel(BaseModel):
    index: int
    action: str
    ok: bool
    duration_ms: int = 0
    screenshot_base64: Optional[str] = None
    error: Optional[str] = None
    console_errors: List[str] = []
    method: Optional[str] = None
    warning: Optional[str] = None
    screenshot_error: Optional[str] = None


class JourneyRunResponse(BaseModel):
    passed: bool
    step_results: List[StepResultModel]
    session_id: str
    final_url: str
    artifacts: Dict[str, str] = {}
