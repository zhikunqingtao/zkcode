"""Managed browser actions must never recreate or borrow an unowned context."""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from routers import browser
from services.browser_models import BrowserLeaseRequest, NavigateRequest
from services.browser_recordings import RecordingSpool
from services.browser_service import BrowserService


@pytest.fixture
def service(tmp_path, monkeypatch):
    service = BrowserService()
    service.recordings = RecordingSpool(tmp_path / "recordings")
    service.cleanup_timeout = .05
    page = SimpleNamespace(
        set_default_timeout=Mock(), add_init_script=AsyncMock(), on=Mock(),
        goto=AsyncMock(return_value=SimpleNamespace(status=200)),
        title=AsyncMock(return_value="fixture"), url="https://fixture.invalid",
    )
    context = Mock(new_page=AsyncMock(return_value=page), close=AsyncMock(),
                   add_init_script=AsyncMock(), tracing=SimpleNamespace(start=AsyncMock()))
    service._browser = SimpleNamespace(new_context=AsyncMock(return_value=context))
    monkeypatch.setattr(browser, "browser_service", service)
    return service


async def acquire(service):
    identity = dict(owner_session_id="owner", run_id="run", host_epoch="epoch")
    reply = await browser.acquire_run_lease(BrowserLeaseRequest(session_id="managed", **identity))
    assert reply.success
    return {**identity, "generation": reply.data["generation"]}


def navigate(lease=None, **extra):
    return NavigateRequest(session_id="managed", strict_session=True,
                           managed_lease=lease, url="https://fixture.invalid", **extra)


@pytest.mark.asyncio
async def test_failed_acquire_cannot_fall_through_to_strict_navigation(service):
    service.max_sessions = 1
    await service.get_or_create_session("other")
    failed = await browser.acquire_run_lease(BrowserLeaseRequest(
        session_id="managed", owner_session_id="owner", run_id="run", host_epoch="epoch"))
    assert not failed.success and failed.error_code == "BROWSER_CAPACITY_REACHED"
    await service.close_session("other")
    result = await browser.navigate(navigate())
    assert not result.success
    assert "managed" not in service._sessions
    assert service._browser.new_context.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["generation", "owner_session_id", "run_id", "host_epoch", "expired", "released"])
async def test_managed_navigation_rejects_stale_or_wrong_usage(service, change):
    identity = await acquire(service)
    if change == "expired":
        service._sessions["managed"].run_leases[("epoch", "run")] = time.monotonic() - 1
    elif change == "released":
        await service.release_run_lease("managed", "run", "epoch", identity["generation"])
    else:
        identity[change] = "wrong"
    result = await browser.navigate(navigate(identity))
    assert not result.success
    service._sessions["managed"].page.goto.assert_not_awaited()


@pytest.mark.asyncio
async def test_valid_managed_navigation_preserves_session_and_lease(service):
    identity = await acquire(service)
    session = service._sessions["managed"]
    result = await browser.navigate(navigate(identity))
    assert result.success
    assert service._sessions["managed"] is session
    assert session.owner_session_id == "owner"
    assert ("epoch", "run") in session.run_leases
    assert service._browser.new_context.await_count == 1


@pytest.mark.asyncio
async def test_managed_action_deadline_is_checked_before_driver_io(service):
    identity = await acquire(service)
    result = await browser.navigate(navigate(identity, deadline_epoch_ms=int(time.time()*1000)-1))
    assert not result.success
    service._sessions["managed"].page.goto.assert_not_awaited()


@pytest.mark.asyncio
async def test_context_replaced_after_action_admission_is_not_used(service, monkeypatch):
    identity = await acquire(service)
    entered, resume = asyncio.Event(), asyncio.Event()
    original = service.navigate

    async def delayed(*args, **kwargs):
        entered.set()
        await resume.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(service, "navigate", delayed)
    action = asyncio.create_task(browser.navigate(navigate(identity)))
    await entered.wait()
    await service.close_session("managed")
    await acquire(service)
    resume.set()
    result = await action
    assert not result.success
    service._sessions["managed"].page.goto.assert_not_awaited()


@pytest.mark.asyncio
async def test_sidecar_generation_change_rejects_old_managed_action(service):
    identity = await acquire(service)
    service.generation = "restarted"
    result = await browser.navigate(navigate(identity))
    assert not result.success
    service._sessions["managed"].page.goto.assert_not_awaited()
