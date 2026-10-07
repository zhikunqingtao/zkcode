"""Production parsers in isolated workers, plus deterministic process cleanup."""
import asyncio
import multiprocessing
import time
import threading
import uuid

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import analysis_jobs as jobs
from analyzers.code_path_tracer import CodePathTracer
from routers import analysis


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKSPACE_ROOT", str(tmp_path))
    (tmp_path / "api.py").write_text('''from fastapi import APIRouter
router = APIRouter()
@router.get("/users")
def get_user():
    return fetch()
def fetch():
    return {"id": 1}
''')
    return tmp_path


def payload(project, **extra):
    return {"project_root": str(project), "request_id": str(uuid.uuid4()), "analysis_owner": "session:test", **extra}


def _slow_worker(*_args):
    time.sleep(60)


def test_actual_routes_execute_real_parsers_in_workers(project):
    app = FastAPI()
    app.include_router(analysis.router, prefix="/api/analysis")
    with TestClient(app) as client:
        response = client.post("/api/analysis/generate-diagram", json=payload(project, diagram_type="flowchart", target="get_user", options={"depth": 3}))
        assert response.status_code == 200, response.text
        assert response.json()["mermaid_syntax"].startswith("flowchart")
        assert response.json()["metadata"]["nodes_count"] > 0
        endpoints = client.post("/api/analysis/api-endpoints", json=payload(project, languages=["python"]))
        assert endpoints.status_code == 200, endpoints.text
        endpoint = endpoints.json()["endpoints"][0]
        assert endpoint["http_method"] == "GET"
        traced = client.post("/api/analysis/code-path", json=payload(project, entry_file="api.py", entry_function="get_user", max_depth=5))
        assert traced.status_code == 200, traced.text
        assert traced.json()["data"]["entry_node"]
        assert any(node["name"] == "fetch" for node in traced.json()["data"]["nodes"])
        missing = client.post("/api/analysis/code-path", json=payload(project, entry_file="api.py", entry_function="absent", max_depth=5))
        assert missing.status_code == 404


def test_cache_identity_includes_language_and_actual_bytes(project):
    assert CodePathTracer(str(project)).scan_api_endpoints(["python"])
    assert CodePathTracer(str(project)).scan_api_endpoints(["java"]) == []
    source = project / "api.py"
    source.write_text(source.read_text().replace('/users', '/teams'))
    endpoints = CodePathTracer(str(project)).scan_api_endpoints(["python"])
    assert endpoints and endpoints[0].path == "/teams"


def test_python_endpoints_preserve_router_prefixes_and_multiple_methods(project):
    (project / "routes.py").write_text('''from fastapi import APIRouter
users = APIRouter(prefix="/v1")
@users.api_route("/users", methods=["GET", "POST"])
def users_handler():
    return []
@cache.get("/not-an-http-route")
def cached():
    return []
''')
    endpoints = CodePathTracer(str(project)).scan_api_endpoints(["python"])
    assert {endpoint.http_method for endpoint in endpoints if endpoint.path == "/v1/users"} == {"GET", "POST"}
    assert all(endpoint.handler_function != "cached" for endpoint in endpoints)


@pytest.mark.asyncio
async def test_http_worker_cache_is_content_addressed_and_owner_scoped(project):
    first = await jobs.run("endpoints", payload(project, languages=["python"]), None)
    keys = set(jobs._cache)
    repeated = await jobs.run("endpoints", payload(project, languages=["python"]), None)
    assert first == repeated
    assert set(jobs._cache) == keys
    await jobs.run("endpoints", {**payload(project, languages=["python"]), "analysis_owner": "session:other"}, None)
    assert len(set(jobs._cache) - keys) == 1
    source = project / "api.py"
    source.write_text(source.read_text().replace('/users', '/teams'))
    changed = await jobs.run("endpoints", payload(project, languages=["python"]), None)
    assert changed["endpoints"][0]["path"] == "/teams"
    assert len(set(jobs._cache) - keys) == 2


@pytest.mark.asyncio
async def test_cancellation_is_owner_scoped_and_reaps_worker(project, monkeypatch):
    monkeypatch.setattr(jobs, "_worker", _slow_worker)
    request = payload(project)
    before = {child.pid for child in multiprocessing.active_children()}
    pending = asyncio.create_task(jobs.run("diagram", request, None))
    await asyncio.sleep(0.15)
    jobs.cancel({**request, "analysis_owner": "session:other"})
    assert not pending.done()
    receipt = jobs.cancel(request)
    assert receipt["status"] == "cancelling"
    with pytest.raises(HTTPException) as error:
        await pending
    assert error.value.status_code == 409
    assert not {child.pid for child in multiprocessing.active_children()} - before
    with pytest.raises(HTTPException) as duplicate:
        await jobs.run("diagram", request, None)
    assert duplicate.value.status_code == 409


@pytest.mark.asyncio
async def test_timeout_reaps_worker_and_releases_admission(project, monkeypatch):
    monkeypatch.setattr(jobs, "_worker", _slow_worker)
    request = payload(project)
    before = {child.pid for child in multiprocessing.active_children()}
    with pytest.raises(HTTPException) as error:
        await jobs.run("diagram", request, None, timeout=0.05)
    assert error.value.status_code == 408
    assert not {child.pid for child in multiprocessing.active_children()} - before
    assert not jobs._jobs[jobs._key(request)].active


def test_python_documentation_never_calls_a_java_backend():
    app = FastAPI(version="test-version")
    app.include_router(analysis.router, prefix="/api/analysis")
    with TestClient(app) as client:
        spec = client.get("/api/analysis/openapi/python").json()
        assert spec["info"]["version"] == "test-version"
        partial = client.get("/api/analysis/openapi/merged").json()
        assert partial["warnings"] and partial["paths"]
        assert client.get("/api/analysis/openapi/java").status_code == 410


@pytest.mark.asyncio
async def test_repeated_http_cancellation_waits_for_owned_worker_cleanup(project, monkeypatch):
    monkeypatch.setattr(jobs, "_worker", _slow_worker)
    cleanup_started = threading.Event()
    continue_cleanup = threading.Event()
    cleanup_worker = jobs._cleanup_worker

    def delayed_cleanup(*args):
        cleanup_started.set()
        assert continue_cleanup.wait(2)
        cleanup_worker(*args)

    monkeypatch.setattr(jobs, "_cleanup_worker", delayed_cleanup)
    request = payload(project)
    before = {child.pid for child in multiprocessing.active_children()}
    pending = asyncio.create_task(jobs.run("diagram", request, None))
    await asyncio.sleep(0.15)
    pending.cancel()
    assert await asyncio.to_thread(cleanup_started.wait, 2)
    pending.cancel()  # A second disconnect/shutdown arrives during join.
    await asyncio.sleep(0)
    assert not pending.done()
    assert jobs._jobs[jobs._key(request)].active
    continue_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert not jobs._jobs[jobs._key(request)].active
    assert not {child.pid for child in multiprocessing.active_children()} - before


def test_change_impact_uses_actual_lines_and_never_claims_verification(project):
    app=FastAPI()
    app.include_router(analysis.router,prefix="/api/analysis")
    with TestClient(app) as client:
        response=client.post("/api/analysis/change-impact",json=payload(project,file_path="api.py",changed_lines=[7],depth=3))
        assert response.status_code==200,response.text
        data=response.json()["data"]
        assert data["analysis_kind"]=="advisory"
        assert data["is_verification_evidence"] is False
        assert {"fetch","get_user"} <= {node["name"] for node in data["impact_nodes"]}
        assert data["changed_lines"]==[7]
        assert not data["truncated"]
        assert client.post("/api/analysis/change-impact",json=payload(project,file_path="api.py",changed_lines=[0],depth=3)).status_code==400
        (project/"lib.rs").write_text("fn main() {}")
        refused=client.post("/api/analysis/change-impact",json=payload(project,file_path="lib.rs",changed_lines=[1],depth=3))
        assert refused.status_code==400
        assert refused.json()["error"]["code"]=="ANALYSIS_LANGUAGE_UNSUPPORTED"


@pytest.mark.asyncio
async def test_change_impact_cache_rechecks_nonmax_mtime_source_and_timeout_reaps(project,monkeypatch):
    first=await jobs.run("impact",payload(project,file_path=str(project/"api.py"),changed_lines=[7],depth=3),None)
    assert any(node["name"]=="get_user" for node in first["impact_nodes"])
    source=project/"api.py"
    stat=source.stat()
    source.write_text(source.read_text().replace("get_user","get_team"))
    import os
    os.utime(source,ns=(stat.st_atime_ns,stat.st_mtime_ns))
    second=await jobs.run("impact",payload(project,file_path=str(source),changed_lines=[7],depth=3),None)
    names={node["name"] for node in second["impact_nodes"]}
    assert "get_team" in names and "get_user" not in names
    monkeypatch.setattr(jobs,"_worker",_slow_worker)
    before={child.pid for child in multiprocessing.active_children()}
    with pytest.raises(HTTPException) as error:
        await jobs.run("impact",payload(project,file_path=str(source),changed_lines=[7],depth=1),None,timeout=0.05)
    assert error.value.status_code==408
    assert not {child.pid for child in multiprocessing.active_children()}-before


def test_unconfirmed_cleanup_keeps_owner_and_releases_admission_only_after_exit():
    exited=threading.Event()
    closed=threading.Event()
    class Process:
        def is_alive(self): return not exited.is_set()
        def terminate(self): pass
        def kill(self): pass
        def join(self,_): time.sleep(0.005)
        def close(self):
            assert exited.is_set()
            closed.set()
    class Pipe:
        def close(self): pass
    job=jobs.Job(active=True)
    with pytest.raises(HTTPException) as error:
        jobs._cleanup_worker(Process(),Pipe(),Pipe(),True,job)
    assert error.value.status_code==503
    assert job.active and not closed.is_set()
    exited.set()
    assert closed.wait(2)
    deadline=time.monotonic()+1
    while job.active and time.monotonic()<deadline: time.sleep(0.005)
    assert not job.active
