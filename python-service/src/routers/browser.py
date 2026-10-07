"""
浏览器自动化 FastAPI 路由 — §10.4 B3

前缀 /api/browser，13 个端点对应 WebBrowserTool.java 的 13 个 action。
每个端点：接收 Pydantic 模型 → 委托 BrowserService → 统一 BrowserResponse。

生命周期：
  browser_service 的 startup/shutdown 通过 main.py lifespan 管理，
  或通过 startup_browser/shutdown_browser 回调注入。
"""

import logging

from fastapi import APIRouter
from services.content_privacy import ephemeral_request, require_body_free_browser_logging

from services.browser_service import BrowserNavigationRejected, BrowserService
from services.browser_models import (
    NavigateRequest,
    ClickRequest,
    TypeRequest,
    EvaluateRequest,
    ScreenshotRequest,
    ExtractRequest,
    WaitForRequest,
    SelectOptionRequest,
    DialogRequest,
    CookieRequest,
    SetCookieRequest,
    CloseSessionRequest,
    JsErrorsRequest,
    BrowserResponse,
)
from services.browser_models import SemanticSnapshotRequest

logger = logging.getLogger(__name__)

router = APIRouter()
browser_service = BrowserService()


# ═══ 生命周期回调（由 main.py lifespan 调用）═══

async def startup_browser():
    await browser_service.startup()


async def shutdown_browser():
    await browser_service.shutdown()


@router.post("/recover_failed_cleanup")
async def recover_failed_cleanup() -> BrowserResponse:
    """Explicit maintenance action; refuses to interrupt healthy/active sessions."""
    try:
        if not await browser_service.recover_failed_cleanup():
            return BrowserResponse(success=False, error_code="BROWSER_RECOVERY_NOT_READY",
                                   error_message="Active sessions/creation exist or no failed cleanup remains")
        return BrowserResponse(success=True, data={"recovered": True})
    except Exception:
        logger.warning("Browser recovery could not confirm resource release or restart")
        return BrowserResponse(success=False, error_code="BROWSER_RECOVERY_FAILED",
                               error_message="Browser recovery incomplete; resource ownership retained")


# ═══ 端点 ═══

@router.post("/navigate")
@ephemeral_request
async def navigate(req: NavigateRequest) -> BrowserResponse:
    try:
        data = await browser_service.navigate(
            req.session_id, req.url, req.wait_until, req.timeout
        )
        return BrowserResponse(success=True, data=data)
    except BrowserNavigationRejected as e:
        return BrowserResponse(
            success=False, error_code=e.code, error_message=str(e)
        )
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/screenshot")
@ephemeral_request
async def screenshot(req: ScreenshotRequest) -> BrowserResponse:
    try:
        data = await browser_service.screenshot(
            req.session_id, req.full_page, req.selector,
            strict_session=req.strict_session,
        )
        if not data.get("success", True) is True and "error_code" in data:
            return BrowserResponse(success=False, error_code=data["error_code"], error_message=data["error_message"])
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/click")
@ephemeral_request
async def click(req: ClickRequest) -> BrowserResponse:
    try:
        data = await browser_service.click(
            req.session_id, req.selector, req.timeout,
            strict_session=req.strict_session,
            no_wait_after=req.no_wait_after,
            force=req.force,
        )
        if data.get("success") is False or data.get("clicked") is False:
            return BrowserResponse(
                success=False,
                data=data,
                error_code=data.get("error_code") or "BROWSER_CLICK_FAILED",
                error_message=data.get("error_message") or data.get("error") or "Browser click failed",
            )
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/type")
@ephemeral_request
async def type_text(req: TypeRequest) -> BrowserResponse:
    try:
        data = await browser_service.type_text(
            req.session_id, req.selector, req.text, req.timeout,
            strict_session=req.strict_session,
        )
        if data.get("success") is False:
            return BrowserResponse(
                success=False,
                data=data,
                error_code=data.get("error_code") or "BROWSER_TYPE_FAILED",
                error_message=data.get("error_message") or data.get("error") or "Browser type failed",
            )
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/evaluate")
@ephemeral_request
async def evaluate(req: EvaluateRequest) -> BrowserResponse:
    try:
        data = await browser_service.evaluate(
            req.session_id, req.script,
            strict_session=req.strict_session,
        )
        # evaluate 内部已做了错误捕获，检查返回结构
        if data.get("success") is False and "error_code" in data:
            return BrowserResponse(success=False, error_code=data["error_code"], error_message=data["error_message"])
        if data.get("success") is False:
            return BrowserResponse(success=False, data=data, error_code="JS_EVALUATION_ERROR", error_message=data.get("error_message", "JavaScript evaluation failed"))
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/extract_text")
@ephemeral_request
async def extract_text(req: ExtractRequest) -> BrowserResponse:
    try:
        data = await browser_service.extract_text(
            req.session_id, req.selector,
            strict_session=req.strict_session,
        )
        if not data.get("success", True) is True and "error_code" in data:
            return BrowserResponse(success=False, error_code=data["error_code"], error_message=data["error_message"])
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/extract_html")
@ephemeral_request
async def extract_html(req: ExtractRequest) -> BrowserResponse:
    try:
        data = await browser_service.extract_html(
            req.session_id, req.selector,
            strict_session=req.strict_session,
        )
        if not data.get("success", True) is True and "error_code" in data:
            return BrowserResponse(success=False, error_code=data["error_code"], error_message=data["error_message"])
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/wait_for")
@ephemeral_request
async def wait_for(req: WaitForRequest) -> BrowserResponse:
    try:
        data = await browser_service.wait_for(
            req.session_id,
            selector=req.selector,
            state=req.state,
            timeout=req.timeout,
            wait_until=req.wait_until,
            text_contains=req.text_contains,
            strict_session=req.strict_session,
        )
        if not data.get("success", True) is True and "error_code" in data:
            return BrowserResponse(success=False, error_code=data["error_code"], error_message=data["error_message"])
        if data.get("error"):
            return BrowserResponse(success=False, error_code="INVALID_PARAMS", error_message=data["error"])
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/select_option")
@ephemeral_request
async def select_option(req: SelectOptionRequest) -> BrowserResponse:
    try:
        data = await browser_service.select_option(
            req.session_id, req.selector, req.values,
            strict_session=req.strict_session,
        )
        if not data.get("success", True) is True and "error_code" in data:
            return BrowserResponse(success=False, error_code=data["error_code"], error_message=data["error_message"])
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/handle_dialog")
@ephemeral_request
async def handle_dialog(req: DialogRequest) -> BrowserResponse:
    try:
        data = await browser_service.handle_dialog(
            req.session_id, req.accept, req.text,
            strict_session=req.strict_session,
        )
        if not data.get("success", True) is True and "error_code" in data:
            return BrowserResponse(success=False, error_code=data["error_code"], error_message=data["error_message"])
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/get_cookies")
@ephemeral_request
async def get_cookies(req: CookieRequest) -> BrowserResponse:
    try:
        data = await browser_service.get_cookies(
            req.session_id,
            strict_session=req.strict_session,
        )
        if not data.get("success", True) is True and "error_code" in data:
            return BrowserResponse(success=False, error_code=data["error_code"], error_message=data["error_message"])
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/set_cookie")
@ephemeral_request
async def set_cookie(req: SetCookieRequest) -> BrowserResponse:
    try:
        data = await browser_service.set_cookie(
            req.session_id, req.cookie,
            strict_session=req.strict_session,
        )
        if not data.get("success", True) is True and "error_code" in data:
            return BrowserResponse(success=False, error_code=data["error_code"], error_message=data["error_message"])
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/close_session")
@ephemeral_request
async def close_session(req: CloseSessionRequest) -> BrowserResponse:
    try:
        closed = await browser_service.close_session(req.session_id)
        return BrowserResponse(success=True, data={"closed": closed})
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.delete("/session/{session_id}")
async def delete_session(session_id: str) -> BrowserResponse:
    """RESTful DELETE endpoint to explicitly close and cleanup a browser session."""
    try:
        closed = await browser_service.close_session(session_id)
        return BrowserResponse(success=True, data={"closed": closed})
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/get_js_errors")
@ephemeral_request
async def get_js_errors(req: JsErrorsRequest) -> BrowserResponse:
    """Return collected JS errors for a given session."""
    try:
        errors = await browser_service.get_js_errors(req.session_id)
        return BrowserResponse(success=True, data={"js_errors": errors, "count": len(errors)})
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


@router.post("/snapshot-semantic")
@ephemeral_request
async def snapshot_semantic(req: SemanticSnapshotRequest) -> BrowserResponse:
    """语义快照 — zkcode v1.5 升级项 A MVP。

    基于 Playwright accessibility.snapshot() 返回页面的语义 DOM 树，
    仅包含可交互与有语义的节点 (role/name/value)，相比原始 HTML 体积小 10-100 倍。
    可选同步返回缩略图用于前端 Replay 时间线渲染。
    """
    try:
        data = await browser_service.snapshot_semantic(
            req.session_id,
            selector=req.selector,
            interesting_only=req.interesting_only,
            include_screenshot=req.include_screenshot,
            strict_session=req.strict_session,
        )
        if data.get("success") is False and "error_code" in data:
            return BrowserResponse(
                success=False,
                error_code=data["error_code"],
                error_message=data["error_message"],
            )
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=type(e).__name__, error_message=str(e)
        )


# Run-owned temporary contexts use only host-generated opaque IDs. Model-provided
# browser aliases are resolved in Rust and never become sidecar context names.
from typing import Any, Literal
from pydantic import BaseModel, Field, ValidationError


class OwnedBrowserCreate(BaseModel):
    session_id: str = Field(pattern=r"^owned-[0-9a-f-]{36}$")
    ephemeral_content: Literal[True] = True


class OwnedBrowserAction(OwnedBrowserCreate):
    action: str
    parameters: dict[str, Any] = Field(default_factory=dict)


@router.post("/owned/create")
@ephemeral_request
async def create_owned(req: OwnedBrowserCreate) -> BrowserResponse:
    require_body_free_browser_logging()
    try:
        await browser_service.create_owned_ephemeral_session(req.session_id)
        return BrowserResponse(success=True, data={"created": True})
    except Exception as exc:
        return BrowserResponse(success=False, error_code=type(exc).__name__,
                               error_message="Temporary browser context creation failed")


@router.post("/owned/action")
@ephemeral_request
async def owned_action(req: OwnedBrowserAction) -> BrowserResponse:
    require_body_free_browser_logging()
    actions = {
        "navigate": (NavigateRequest, navigate), "screenshot": (ScreenshotRequest, screenshot),
        "click": (ClickRequest, click), "type": (TypeRequest, type_text),
        "evaluate": (EvaluateRequest, evaluate), "extract_text": (ExtractRequest, extract_text),
        "extract_html": (ExtractRequest, extract_html), "wait_for": (WaitForRequest, wait_for),
        "select_option": (SelectOptionRequest, select_option), "handle_dialog": (DialogRequest, handle_dialog),
        "get_cookies": (CookieRequest, get_cookies), "set_cookie": (SetCookieRequest, set_cookie),
        "get_js_errors": (JsErrorsRequest, get_js_errors),
        "snapshot-semantic": (SemanticSnapshotRequest, snapshot_semantic),
    }
    action = actions.get(req.action)
    if action is None:
        return BrowserResponse(success=False, error_code="INVALID_ACTION", error_message="Unsupported browser action")
    try:
        # Exact existing temporary lookup; ordinary session creation cannot run
        # inside this ContextVar scope, including after a concurrent close.
        await browser_service.get_or_create_session(req.session_id)
        body = {**req.parameters, "session_id": req.session_id,
                "strict_session": True, "ephemeral_content": True}
        model, handler = action
        return await handler(model.model_validate(body))
    except ValidationError:
        return BrowserResponse(success=False, error_code="INVALID_PARAMS", error_message="Invalid browser action parameters")
    except Exception as exc:
        return BrowserResponse(success=False, error_code=type(exc).__name__,
                               error_message="Temporary browser action failed")
