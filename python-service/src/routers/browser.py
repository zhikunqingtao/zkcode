"""
浏览器自动化 FastAPI 路由 — §10.4 B3

前缀 /api/browser，13 个端点对应 WebBrowserTool.java 的 13 个 action。
每个端点：接收 Pydantic 模型 → 委托 BrowserService → 统一 BrowserResponse。

生命周期：
  browser_service 的 startup/shutdown 通过 main.py lifespan 管理，
  或通过 startup_browser/shutdown_browser 回调注入。
"""

import logging
import asyncio
import time
from functools import wraps

from fastapi import APIRouter
from services.content_privacy import ephemeral_request, require_body_free_browser_logging

from services.browser_service import BrowserNavigationRejected, BrowserAdmissionError, BrowserService, creation_deadline, creation_owner
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
    BrowserResponse, BrowserLeaseRequest, RecordingAckRequest, BrowserOwnerRequest, RecordingPruneRequest,
)
from services.browser_models import SemanticSnapshotRequest

logger = logging.getLogger(__name__)

router = APIRouter()
browser_service = BrowserService()


def managed_action(handler):
    """Validate host Run leases and strict lookup before any driver action."""
    @wraps(handler)
    async def wrapped(req):
        lease = getattr(req, "managed_lease", None)
        if lease is None and not getattr(req, "strict_session", False):
            return await handler(req)
        try:
            async with browser_service.action_scope(
                req.session_id, lease, getattr(req, "deadline_epoch_ms", None)
            ):
                return await handler(req)
        except BrowserAdmissionError as error:
            return BrowserResponse(success=False, error_code=error.code, error_message=str(error))
    return wrapped


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
@managed_action
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
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/screenshot")
@ephemeral_request
@managed_action
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
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/click")
@ephemeral_request
@managed_action
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
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/type")
@ephemeral_request
@managed_action
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
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/evaluate")
@ephemeral_request
@managed_action
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
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/extract_text")
@ephemeral_request
@managed_action
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
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/extract_html")
@ephemeral_request
@managed_action
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
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/wait_for")
@ephemeral_request
@managed_action
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
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/select_option")
@ephemeral_request
@managed_action
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
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/handle_dialog")
@ephemeral_request
@managed_action
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
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/get_cookies")
@ephemeral_request
@managed_action
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
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/set_cookie")
@ephemeral_request
@managed_action
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
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/close_session")
@ephemeral_request
@managed_action
async def close_session(req: CloseSessionRequest) -> BrowserResponse:
    try:
        identity = req.recording
        if identity is not None and browser_service._recording_sessions.get(req.session_id) != identity:
            async with browser_service._lock:
                active = req.session_id in browser_service._sessions or req.session_id in browser_service._creating or req.session_id in browser_service._unclosed_contexts.values()
            if active:
                raise BrowserAdmissionError("RECORDING_OWNER_MISMATCH", "Recording does not belong to this browser", 409)
        closed = await browser_service.close_session(req.session_id)
        async with browser_service._lock:
            absent = req.session_id not in browser_service._sessions and req.session_id not in browser_service._creating and req.session_id not in browser_service._unclosed_contexts.values()
        confirmed = closed or absent
        data = {"closed": confirmed}
        if confirmed and identity is not None:
            try:
                async with browser_service._lock:
                    absent = req.session_id not in browser_service._sessions and req.session_id not in browser_service._creating and req.session_id not in browser_service._unclosed_contexts.values()
                    batch_absent = absent and await asyncio.to_thread(browser_service.recordings.absent, identity["batch_id"])
                if batch_absent:
                    data["recording_finalization"] = {"phase": "not_created", "identity": identity}
                elif not absent:
                    data["recording_error_code"] = "RECORDING_CLEANUP_UNCONFIRMED"
                else:
                    # Physical context and retained preparations are gone. Old
                    # generation manifests remain valid after sidecar restart.
                    data["recording_manifest"] = await asyncio.to_thread(browser_service.recordings.seal, identity["batch_id"], identity, None)
            except (ValueError, OSError) as error:
                data["recording_error_code"] = str(error) if isinstance(error, ValueError) else "RECORDING_SEAL_FAILED"
        return BrowserResponse(success=confirmed, data=data, error_code=None if confirmed else "BROWSER_CLEANUP_UNCONFIRMED")
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.delete("/session/{session_id}")
async def delete_session(session_id: str) -> BrowserResponse:
    """RESTful DELETE endpoint to explicitly close and cleanup a browser session."""
    try:
        closed = await browser_service.close_session(session_id)
        return BrowserResponse(success=True, data={"closed": closed})
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/get_js_errors")
@ephemeral_request
@managed_action
async def get_js_errors(req: JsErrorsRequest) -> BrowserResponse:
    """Return collected JS errors for a given session."""
    try:
        errors = await browser_service.get_js_errors(req.session_id)
        return BrowserResponse(success=True, data={"js_errors": errors, "count": len(errors)})
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


@router.post("/snapshot-semantic")
@ephemeral_request
@managed_action
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
                data=data,
                error_code=data["error_code"],
                error_message=data["error_message"],
            )
        return BrowserResponse(success=True, data=data)
    except Exception as e:
        return BrowserResponse(
            success=False, error_code=e.code if isinstance(e, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED", error_message=str(e) if isinstance(e, BrowserAdmissionError) else "Browser operation failed; inspect diagnostics"
        )


# Run-owned temporary contexts use only host-generated opaque IDs. Model-provided
# browser aliases are resolved in Rust and never become sidecar context names.
from typing import Any, Literal
from pydantic import BaseModel, Field, ValidationError


class OwnedBrowserCreate(BaseModel):
    session_id: str = Field(pattern=r"^owned-[0-9a-f-]{36}$")
    ephemeral_content: Literal[True] = True
    deadline_epoch_ms: int | None = None


class OwnedBrowserAction(OwnedBrowserCreate):
    action: str
    parameters: dict[str, Any] = Field(default_factory=dict)


@router.post("/owned/create")
@ephemeral_request
async def create_owned(req: OwnedBrowserCreate) -> BrowserResponse:
    require_body_free_browser_logging()
    token = creation_deadline.set(time.monotonic() + max(0, req.deadline_epoch_ms / 1000 - time.time()) if req.deadline_epoch_ms else None)
    try:
        await browser_service.create_owned_ephemeral_session(req.session_id)
        return BrowserResponse(success=True, data={"created": True})
    except Exception as exc:
        return BrowserResponse(success=False, error_code=exc.code if isinstance(exc, BrowserAdmissionError) else "BROWSER_CREATE_FAILED",
                               error_message="Temporary browser context creation failed")
    finally:
        creation_deadline.reset(token)


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
                "strict_session": True, "ephemeral_content": True, "managed_lease": None}
        model, handler = action
        return await handler(model.model_validate(body))
    except ValidationError:
        return BrowserResponse(success=False, error_code="INVALID_PARAMS", error_message="Invalid browser action parameters")
    except Exception as exc:
        return BrowserResponse(success=False, error_code=exc.code if isinstance(exc, BrowserAdmissionError) else "BROWSER_OPERATION_FAILED",
                               error_message="Temporary browser action failed")


@router.post("/lease/acquire")
async def acquire_run_lease(req: BrowserLeaseRequest) -> BrowserResponse:
    token = creation_deadline.set(time.monotonic() + max(0, req.deadline_epoch_ms / 1000 - time.time()) if req.deadline_epoch_ms else None)
    owner_token = creation_owner.set((req.owner_session_id, browser_service.owner_epochs.get(req.owner_session_id, 0), req.host_epoch, req.run_id))
    try:
        if req.deadline_epoch_ms is not None and req.deadline_epoch_ms <= time.time() * 1000:
            raise BrowserAdmissionError("BROWSER_LEASE_EXPIRED", "Run deadline has passed", 409)
        if req.generation is not None:
            async with browser_service._lock:
                session = browser_service._sessions.get(req.session_id)
                if req.generation != browser_service.generation or session is None or session.closing:
                    raise BrowserAdmissionError("BROWSER_LEASE_EXPIRED", "Browser usage lease cannot recreate a context", 409)
        else:
            session = await browser_service.get_or_create_session(req.session_id)
        if session.owner_session_id not in (None, req.owner_session_id):
            raise BrowserAdmissionError("BROWSER_OWNER_MISMATCH", "Browser belongs to another session", 409)
        session.owner_session_id = req.owner_session_id
        generation = req.generation or browser_service.generation
        await browser_service.renew_run_lease(req.session_id, req.run_id, req.host_epoch, generation, req.ttl)
        return BrowserResponse(data={"generation": generation})
    except BrowserAdmissionError as error:
        return BrowserResponse(success=False, error_code=error.code, error_message=str(error))
    finally:
        creation_deadline.reset(token)
        creation_owner.reset(owner_token)


@router.post("/lease/release")
async def release_run_lease(req: BrowserLeaseRequest) -> BrowserResponse:
    try:
        async with browser_service._lock:
            browser_service.retired_usage.add((req.session_id, req.host_epoch, req.run_id))
            reservation = browser_service._creating.get(req.session_id)
            if reservation is not None and reservation.owner_identity is not None and reservation.owner_identity[0] == req.owner_session_id and reservation.owner_identity[2:] == (req.host_epoch, req.run_id):
                reservation.cancelled = True
                reservation.owner.cancel()
        await browser_service.release_run_lease(req.session_id, req.run_id, req.host_epoch, req.generation or browser_service.generation)
        return BrowserResponse(data={"released": True})
    except BrowserAdmissionError as error:
        return BrowserResponse(success=False, error_code=error.code, error_message=str(error))


@router.post("/recordings/ack")
async def acknowledge_recordings(req: RecordingAckRequest) -> BrowserResponse:
    try:
        acknowledged = await asyncio.to_thread(browser_service.recordings.ack, req.identity["batch_id"], req.identity, req.manifest_sha256)
        return BrowserResponse(data={"acknowledged": acknowledged})
    except (ValueError, OSError, KeyError) as error:
        return BrowserResponse(success=False, error_code=str(error) if isinstance(error, ValueError) else "RECORDING_ACK_FAILED", error_message="Recording consumption is not confirmed")


@router.post("/owner/close")
async def close_owner_contexts(req: BrowserOwnerRequest) -> BrowserResponse:
    # The caller seals TaskRuntime admission before invoking this host endpoint.
    # Mark under the same lock as renew/acquire so an active lease cannot be lost.
    async with browser_service._lock:
        owned = [(sid, session) for sid, session in browser_service._sessions.items()
                 if session.owner_session_id == req.owner_session_id]
        if any(any(deadline > time.monotonic() for deadline in session.run_leases.values())
               or session.owner_task is not None for _, session in owned):
            return BrowserResponse(success=False, error_code="BROWSER_OWNER_BUSY", error_message="Session still has active browser usage")
        browser_service.owner_epochs[req.owner_session_id] = browser_service.owner_epochs.get(req.owner_session_id, 0) + 1
        pending = [sid for sid, reservation in browser_service._creating.items()
                   if reservation.owner_identity is not None and reservation.owner_identity[0] == req.owner_session_id]
        for _, session in owned:
            session.closing = True
    confirmed = True
    for sid, session in owned:
        try:
            confirmed = await browser_service.close_session(sid, expected_session=session) and confirmed
        except Exception:
            confirmed = False
    for sid in pending:
        try:
            confirmed = await browser_service.close_session(sid) and confirmed
        except Exception:
            confirmed = False
    return BrowserResponse(success=confirmed, data={"closed": confirmed, "count": len(owned)},
                           error_code=None if confirmed else "BROWSER_CLEANUP_UNCONFIRMED")


@router.post("/recordings/orphans")
async def recording_orphan_candidates() -> BrowserResponse:
    try:
        candidates = await asyncio.to_thread(browser_service.recordings.orphan_candidates)
        return BrowserResponse(data={"candidates": candidates})
    except (ValueError, OSError):
        return BrowserResponse(success=False, error_code="RECORDING_SCAN_FAILED", error_message="Recording metadata retained")


@router.post("/recordings/prune")
async def prune_recording(req: RecordingPruneRequest) -> BrowserResponse:
    active = {getattr(session, "_recording_identity", {}).get("batch_id") for session in browser_service._sessions.values() if getattr(session, "_recording_identity", None)}
    if req.batch_id in active or browser_service._creating:
        return BrowserResponse(success=False, error_code="RECORDING_RETENTION_PROTECTED", error_message="Recording may still have an active owner")
    try:
        removed = await asyncio.to_thread(browser_service.recordings.prune_unreferenced, req.batch_id)
        return BrowserResponse(data={"removed": removed})
    except (ValueError, OSError):
        return BrowserResponse(success=False, error_code="RECORDING_PRUNE_UNCONFIRMED", error_message="Recording cleanup is unconfirmed")
