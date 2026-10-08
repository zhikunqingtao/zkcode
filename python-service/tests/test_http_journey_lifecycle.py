"""HTTP journeys stop their owned request on deadline or caller disconnect."""
import asyncio
import time
from unittest.mock import AsyncMock
import pytest
from fastapi import HTTPException
from routers import http_api
from services.journey_models import JourneyRunRequest

@pytest.mark.asyncio
async def test_expired_http_journey_does_not_execute(monkeypatch):
    execute = AsyncMock()
    monkeypatch.setattr(http_api, '_http_journey_run', execute)
    with pytest.raises(HTTPException) as error:
        await http_api.http_journey_run(JourneyRunRequest(base_url='http://localhost', steps=[{"action":"request","method":"GET","url":"/"}], deadline_epoch_ms=int(time.time()*1000)-1))
    assert error.value.status_code == 504
    execute.assert_not_awaited()

@pytest.mark.asyncio
async def test_http_disconnect_cancels_owned_io(monkeypatch):
    stopped = asyncio.Event()
    async def execute(_):
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()
    monkeypatch.setattr(http_api, '_http_journey_run', execute)
    request = AsyncMock()
    request.is_disconnected.return_value = True
    with pytest.raises(HTTPException) as error:
        await http_api.http_journey_run(JourneyRunRequest(base_url='http://localhost', steps=[{"action":"request","method":"GET","url":"/"}]), request)
    assert error.value.status_code == 499
    assert stopped.is_set()
