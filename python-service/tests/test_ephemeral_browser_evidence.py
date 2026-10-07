"""Temporary browser evidence has no recording or screenshot-file fallback."""
import base64
import logging
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError
from routers import browser, journey
from services.browser_service import BrowserAdmissionError, BrowserService, BrowserSession
from services.journey_models import JourneyRunRequest


@pytest.mark.parametrize("kind", ["video", "trace", "har"])
def test_ephemeral_recording_rejected_by_request_model(kind):
    with pytest.raises(ValidationError, match="EPHEMERAL_RECORDING_UNSUPPORTED"):
        JourneyRunRequest(base_url="http://localhost", steps=[{"action": "screenshot"}],
                          ephemeral_content=True, record={kind: True})


@pytest.mark.asyncio
async def test_service_refuses_recording_before_context_creation(monkeypatch):
    service = BrowserService()
    create = AsyncMock()
    monkeypatch.setattr(service, "_create_session", create)
    with pytest.raises(BrowserAdmissionError, match="Temporary browser recordings"):
        await service._create_context_for_journey("temporary", {"trace": True}, {}, ephemeral_content=True)
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_ephemeral_screenshot_never_opens_file(monkeypatch):
    service = BrowserService()
    raw = b"binary screenshot sentinel"
    page = SimpleNamespace(screenshot=AsyncMock(return_value=raw))
    session = BrowserSession(SimpleNamespace(), page, datetime.now())
    session.ephemeral_content = True
    monkeypatch.setattr(service, "get_or_create_session", AsyncMock(return_value=session))
    def forbidden(*args, **kwargs):
        raise AssertionError("temporary screenshot attempted a disk write")
    monkeypatch.setattr("builtins.open", forbidden)
    response = await service.screenshot("temporary")
    assert base64.b64decode(response["screenshot_base64"]) == raw
    assert response["ephemeral"] is True
    assert "screenshot_path" not in response


@pytest.mark.asyncio
async def test_ephemeral_journey_propagates_policy_and_logs_no_page_body(monkeypatch, caplog):
    marker = "private-browser-error-body"
    page = SimpleNamespace(url="http://localhost", screenshot=AsyncMock(side_effect=RuntimeError(marker)))
    session = SimpleNamespace(page=page, _record_opts={}, _context_kwargs={})
    service = SimpleNamespace(default_timeout=1000,
        _create_context_for_journey=AsyncMock(return_value=session), release_session=AsyncMock(),
        get_js_errors=AsyncMock(return_value=[]))
    monkeypatch.setattr(browser, "browser_service", service)
    with caplog.at_level(logging.DEBUG):
        response = await journey.journey_run(JourneyRunRequest(
            session_id="temporary", base_url="http://localhost", steps=[{"action":"screenshot"}],
            ephemeral_content=True, record={}))
    assert marker in response.step_results[0].screenshot_error
    assert marker not in caplog.text
    assert response.artifacts == {}
    assert service._create_context_for_journey.call_args.kwargs == {"ephemeral_content":True}


@pytest.mark.asyncio
async def test_request_policy_suppresses_library_messages_and_tracebacks(caplog):
    from services.content_privacy import ephemeral_request
    @ephemeral_request
    async def handler(request):
        try:
            raise RuntimeError("private-exception-body")
        except RuntimeError:
            logging.getLogger("httpx").exception("HTTP request private-url-body")
    with caplog.at_level(logging.DEBUG):
        await handler(SimpleNamespace(ephemeral_content=True))
    assert "private-" not in caplog.text
    assert "Temporary operation diagnostic suppressed" in caplog.text
    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        await handler(SimpleNamespace(ephemeral_content=False))
    assert "private-url-body" in caplog.text


@pytest.mark.asyncio
async def test_driver_debug_logging_rejects_before_browser_side_effect(monkeypatch):
    from fastapi import HTTPException
    service=SimpleNamespace(_create_context_for_journey=AsyncMock())
    monkeypatch.setattr(browser,"browser_service",service)
    monkeypatch.setenv("DEBUG","pw:api")
    with pytest.raises(HTTPException) as failure:
        await journey.journey_run(JourneyRunRequest(base_url="http://localhost",steps=[{"action":"screenshot"}],ephemeral_content=True))
    assert failure.value.detail == "EPHEMERAL_BROWSER_DEBUG_LOGGING_UNSUPPORTED"
    service._create_context_for_journey.assert_not_awaited()


@pytest.mark.asyncio
async def test_owned_browser_never_recreates_after_close_or_uses_other_context(monkeypatch):
    from services.content_privacy import ephemeral_request
    from routers.browser import OwnedBrowserCreate
    service = BrowserService()
    create = AsyncMock()
    monkeypatch.setattr(service, "_create_session", create)
    @ephemeral_request
    async def lookup(req):
        return await service.get_or_create_session(req.session_id)
    request = OwnedBrowserCreate(session_id="owned-00000000-0000-4000-8000-000000000000")
    with pytest.raises(BrowserAdmissionError, match="Temporary browser context unavailable"):
        await lookup(request)
    session = BrowserSession(SimpleNamespace(), SimpleNamespace(), datetime.now())
    service._sessions[request.session_id] = session
    with pytest.raises(BrowserAdmissionError, match="Temporary browser context unavailable"):
        await lookup(request)
    session.ephemeral_content = True
    assert await lookup(request) is session
    session.closing = True
    with pytest.raises(BrowserAdmissionError, match="Temporary browser context unavailable"):
        await lookup(request)
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_owned_action_forces_private_identity_and_existing_session(monkeypatch):
    request = browser.OwnedBrowserAction(session_id="owned-00000000-0000-4000-8000-000000000000",
        action="evaluate", parameters={"session_id":"victim", "strict_session":False,
                                        "ephemeral_content":False,"script":"1+1"})
    service=SimpleNamespace(get_or_create_session=AsyncMock(), evaluate=AsyncMock(return_value={"result":2}))
    monkeypatch.setattr(browser, "browser_service", service)
    response=await browser.owned_action(request)
    assert response.success
    assert service.evaluate.call_args.args[0] == request.session_id
    assert service.evaluate.call_args.kwargs["strict_session"] is True
    service.get_or_create_session.assert_awaited_once_with(request.session_id)
