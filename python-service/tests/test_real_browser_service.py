"""End-to-end Playwright checks against a real loopback HTTP server."""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from services.browser_service import (
    BrowserNavigationRejected,
    BrowserService,
    BrowserSession,
)
from services.journey_models import JourneyRunRequest


_PAGE = b"""<!doctype html>
<html><head><title>zkcode browser fixture</title></head>
<body>
  <main id="app">
    <h1>Ready</h1>
    <label>Name <input id="name" value=""></label>
    <select id="choice"><option value="a">Alpha</option><option value="b">Beta</option></select>
    <button id="go" onclick="document.querySelector('h1').textContent='Clicked'">Go</button>
    <button id="dialog" onclick="alert('hello')">Dialog</button>
    <div id="hidden" style="display:none">Hidden target</div>
  </main>
</body></html>"""


class _FixtureHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - stdlib handler contract
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(_PAGE)))
        self.end_headers()
        self.wfile.write(_PAGE)

    def log_message(self, *_args):
        return


@pytest.fixture
def local_page_url():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FixtureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/fixture"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.asyncio
@pytest.mark.timeout(45)
async def test_real_chromium_complete_browser_lifecycle(local_page_url):
    service = BrowserService()
    service.default_timeout = 5_000
    await service.startup()
    screenshot_path: Path | None = None
    try:
        missing = await service.extract_text("missing", strict_session=True)
        assert missing["error_code"] == "SESSION_NOT_FOUND"

        navigation = await service.navigate("real-browser", local_page_url)
        assert navigation["status"] == 200
        assert navigation["title"] == "zkcode browser fixture"
        assert await service.validate_session("real-browser") is True
        first = await service.get_or_create_session("real-browser")
        assert first is await service.get_or_create_session("real-browser")

        body = await service.extract_text("real-browser")
        assert "Ready" in body["text"]
        fragment = await service.extract_html("real-browser", "#app")
        assert 'id="go"' in fragment["html"]

        assert (await service.wait_for("real-browser", wait_until="load"))["success"]
        assert (await service.wait_for("real-browser", wait_until="domcontentloaded"))["success"]
        assert (await service.wait_for("real-browser", selector="#go"))["success"]
        text_wait = await service.wait_for(
            "real-browser", selector="h1", text_contains="Ready"
        )
        assert text_wait["waited_for"] == "text_contains"
        assert "error" in await service.wait_for("real-browser")
        assert (await service.wait_for_selector("real-browser", "#name"))["success"]

        typed = await service.type_text("real-browser", "#name", "Grace Hopper")
        assert typed["success"] and typed["method"] == "playwright"
        value = await service.evaluate("real-browser", "document.querySelector('#name').value")
        assert value["result"] == "Grace Hopper"

        selected = await service.select_option("real-browser", "#choice", ["b"])
        assert selected["selected"] == ["b"]
        clicked = await service.click("real-browser", "#go")
        assert clicked["method"] == "playwright"
        assert (await service.extract_text("real-browser", "h1"))["text"] == "Clicked"

        dialog = await service.handle_dialog("real-browser", accept=True)
        assert dialog["accept"] is True
        await service.click("real-browser", "#dialog")

        cookie = await service.set_cookie(
            "real-browser",
            {"name": "zk", "value": "ok", "url": local_page_url},
        )
        assert cookie["cookie_set"] == "zk"
        cookies = await service.get_cookies("real-browser")
        assert any(item["name"] == "zk" for item in cookies["cookies"])

        invalid_js = await service.evaluate("real-browser", "(() => {")
        assert invalid_js["success"] is False
        await service.evaluate(
            "real-browser",
            "setTimeout(() => { throw new Error('captured-real-error') }, 0)",
        )
        await asyncio.sleep(0.1)
        errors = await service.get_js_errors("real-browser")
        assert any("captured-real-error" in item["message"] for item in errors)

        semantic = await service.snapshot_semantic(
            "real-browser", selector="#app", include_screenshot=True
        )
        assert semantic["node_count"] > 0
        assert len(semantic["interactive"]) >= 4
        assert semantic["screenshot_base64"]
        with pytest.raises(ValueError, match="Element not found"):
            await service.snapshot_semantic("real-browser", selector="#absent")

        shot = await service.screenshot("real-browser", selector="#app")
        assert shot["size"] > 100
        screenshot_path = Path(shot["screenshot_path"])
        assert screenshot_path.is_file()

        assert await service.close_session("real-browser") is True
        assert await service.close_session("real-browser") is False
        assert await service.validate_session("real-browser") is False
    finally:
        await service.shutdown()
        if screenshot_path is not None and screenshot_path.is_file():
            screenshot_path.unlink()


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_real_chromium_session_eviction(local_page_url):
    service = BrowserService()
    service.max_sessions = 1
    await service.startup()
    try:
        await service.navigate("old", local_page_url)
        await service.navigate("new", local_page_url)
        assert await service.validate_session("old") is False
        assert await service.validate_session("new") is True
    finally:
        await service.shutdown()


def test_browser_session_expiration_uses_real_clock_values():
    # No browser I/O is needed for this small state invariant.
    session = object.__new__(BrowserSession)
    session.created_at = datetime.now() - timedelta(minutes=10)
    session.last_activity = datetime.now() - timedelta(minutes=6)
    assert session.is_expired(timedelta(minutes=5)) is True
    session.touch()
    assert session.is_expired(timedelta(minutes=5)) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "data:text/plain,local-data",
        "javascript:document.body.innerText",
        "about:blank",
        "/relative/path",
    ],
)
async def test_navigation_rejects_non_web_urls_before_browser_start(url):
    service = BrowserService()
    with pytest.raises(BrowserNavigationRejected, match="absolute http"):
        await service.navigate("rejected-navigation", url)
    assert await service.validate_session("rejected-navigation") is False


@pytest.mark.asyncio
@pytest.mark.timeout(45)
async def test_real_journey_dsl_runs_in_chromium(local_page_url):
    from routers.browser import browser_service
    from routers.journey import journey_run

    await browser_service.startup()
    session_ids: list[str] = []
    try:
        passed = await journey_run(
            JourneyRunRequest(
                session_id="journey-real-pass",
                base_url=local_page_url,
                steps=[
                    {"action": "navigate", "url": local_page_url},
                    {"action": "wait_for", "selector": "#name"},
                    {"action": "type", "selector": "#name", "text": "Lin"},
                    {"action": "click", "selector": "#go"},
                    {"action": "assert_text", "selector": "h1", "expected": "Clicked"},
                    {"action": "assert_url", "contains": "/fixture"},
                    {"action": "assert_no_console_error"},
                    {"action": "screenshot"},
                ],
            )
        )
        session_ids.append(passed.session_id)
        assert passed.passed is True
        assert len(passed.step_results) == 8
        assert all(step.ok for step in passed.step_results)
        assert all(step.screenshot_base64 for step in passed.step_results)

        failed = await journey_run(
            JourneyRunRequest(
                session_id="journey-real-fail",
                base_url=local_page_url,
                steps=[
                    {"action": "navigate", "url": local_page_url},
                    {"action": "assert_text", "selector": "h1", "expected": "Absent"},
                    {"action": "screenshot"},
                ],
            )
        )
        session_ids.append(failed.session_id)
        assert failed.passed is False
        assert len(failed.step_results) == 2
        assert failed.step_results[-1].error.startswith("assert_text failed")

        unknown = await journey_run(
            JourneyRunRequest(
                session_id="journey-real-unknown",
                base_url=local_page_url,
                steps=[{"action": "unknown"}],
            )
        )
        session_ids.append(unknown.session_id)
        assert unknown.passed is False
        assert unknown.step_results[0].error == "Unknown action: unknown"
    finally:
        for session_id in session_ids:
            await browser_service.close_session(session_id)
        await browser_service.shutdown()
