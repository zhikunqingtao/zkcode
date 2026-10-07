"""Real complexity routes share owner-bound workers, cache and cancellation."""
import asyncio
import json
import os
from pathlib import Path
import multiprocessing
import uuid

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
import analysis_jobs as jobs
from routers.code_quality import router
from services.complexity_analyzer import ComplexityAnalyzer


def payload(root, **extra):
    return {"project_root": str(root), "request_id": str(uuid.uuid4()), "analysis_owner": "session:complexity", **extra}


def test_real_worker_metrics_cache_and_changed_bytes(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKSPACE_ROOT", str(tmp_path))
    source = tmp_path / "计算.py"
    source.write_text('def calculate(value):\n    if value:\n        return 1\n    return 0\n')
    app = FastAPI(); app.include_router(router, prefix="/api/code-quality")
    with TestClient(app) as client:
        first = client.post('/api/code-quality/complexity', json=payload(tmp_path, languages=['python']))
        assert first.status_code == 200, first.text
        data = first.json()['data']
        assert data['root']['children'][0]['cc'] == 2
        assert data['stats']['total_files'] == 1
        assert data['analysis_kind'] == 'heuristic' and data['is_verification_evidence'] is False
        assert data['truncated'] is False
        second = client.post('/api/code-quality/complexity', json=payload(tmp_path, languages=['python']))
        assert second.status_code == 200
        assert second.json()["data"]["cached"] is True
        source.write_text(source.read_text().replace('    if value:', '    if value and value > 2:'))
        changed = client.post('/api/code-quality/complexity', json=payload(tmp_path, languages=['python']))
        assert changed.json()['data']['root']['children'][0]['cc'] == 3
        invalid = client.post('/api/code-quality/complexity', json=payload(tmp_path, languages=['rust']))
        assert invalid.status_code == 422
        source.write_bytes(b'\xff\xfe\x01')
        failed = client.post('/api/code-quality/complexity', json=payload(tmp_path, languages=['python']))
        assert failed.status_code == 500
        assert 'success' not in failed.json()


def test_global_file_limit_is_truthful_and_does_not_reset_in_each_directory(tmp_path, monkeypatch):
    monkeypatch.setattr('services.complexity_analyzer._MAX_FILES', 2)
    for name in ('one', 'two', 'three'):
        directory = tmp_path / name; directory.mkdir(); (directory / 'a.py').write_text('a = 1\n')
    result = ComplexityAnalyzer(['python'])._analyze_sync(str(tmp_path))
    assert result.stats.total_files == 2 and result.truncated is True


@pytest.mark.asyncio
async def test_complexity_cancelled_before_dispatch_never_starts_a_worker(tmp_path, monkeypatch):
    monkeypatch.setenv('WORKSPACE_ROOT', str(tmp_path))
    request = payload(tmp_path, languages=['python'])
    before = {process.pid for process in multiprocessing.active_children()}
    jobs.cancel(request)
    with pytest.raises(HTTPException) as error:
        await jobs.run('complexity', request, None)
    assert error.value.status_code == 409
    assert {process.pid for process in multiprocessing.active_children()} == before


@pytest.mark.parametrize("extension", ["js", "jsx"])
@pytest.mark.parametrize("languages", [None, ["javascript"]], ids=["default", "explicit"])
def test_javascript_content_changes_invalidate_real_worker_cache(tmp_path, monkeypatch, extension, languages):
    monkeypatch.setenv("WORKSPACE_ROOT", str(tmp_path))
    source = tmp_path / f"entry.{extension}"
    original = "function count(value) { if (value) return 1; return 0; }".ljust(100) + "\n"
    changed = "function count(value) { if (value) return 1; if (ok) return 2; return 0; }".ljust(100) + "\n"
    assert len(original) == len(changed)
    source.write_text(original)
    stamp = source.stat()
    app = FastAPI(); app.include_router(router, prefix="/api/code-quality")
    options = {} if languages is None else {"languages": languages}
    with TestClient(app) as client:
        first = client.post("/api/code-quality/complexity", json=payload(tmp_path, **options))
        assert first.status_code == 200, first.text
        assert first.json()["data"]["root"]["children"][0]["cc"] == 2
        hit = client.post("/api/code-quality/complexity", json=payload(tmp_path, **options))
        assert hit.status_code == 200 and hit.json()["data"]["cached"] is True
        source.write_text(changed)
        os.utime(source, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
        fresh = client.post("/api/code-quality/complexity", json=payload(tmp_path, **options))
        assert fresh.status_code == 200, fresh.text
        assert fresh.json()["data"]["cached"] is False
        assert fresh.json()["data"]["root"]["children"][0]["cc"] == 3
        other_owner = client.post("/api/code-quality/complexity", json=payload(tmp_path, analysis_owner="session:other", **options))
        assert other_owner.status_code == 200
        assert other_owner.json()["data"]["cached"] is False


def test_real_worker_cache_tracks_target_add_delete_and_rename(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKSPACE_ROOT", str(tmp_path))
    selected = tmp_path / "selected"
    selected.mkdir()
    other = tmp_path / "other"
    other.mkdir()
    source = selected / "entry.js"
    source.write_text("function f(x) { if (x) return 1; return 0; }\n")
    outside = other / "outside.js"
    outside.write_text("function g(x) { return x; }\n")
    app = FastAPI()
    app.include_router(router, prefix="/api/code-quality")

    with TestClient(app) as client:
        def analyze(target):
            response = client.post(
                "/api/code-quality/complexity",
                json=payload(tmp_path, target_path=str(target), languages=["javascript"]),
            )
            assert response.status_code == 200, response.text
            return response.json()["data"]

        first = analyze(selected)
        assert first["cached"] is False
        assert first["root"]["children"][0]["name"] == "entry.js"
        assert first["stats"]["total_files"] == 1
        assert analyze(selected)["cached"] is True

        outside.write_text("function g(x) { if (x) return 1; if (x > 1) return 2; return 0; }\n")
        assert analyze(selected)["cached"] is True
        other_result = analyze(other)
        assert other_result["cached"] is False
        assert other_result["root"]["children"][0]["name"] == "outside.js"
        assert other_result["root"]["children"][0]["cc"] == 3

        renamed = selected / "renamed.js"
        source.rename(renamed)
        moved = analyze(selected)
        assert moved["cached"] is False
        assert moved["stats"]["total_files"] == 1
        assert moved["root"]["children"][0]["name"] == "renamed.js"

        (selected / "added.jsx").write_text("function h(x) { return x; }\n")
        added = analyze(selected)
        assert added["cached"] is False
        assert added["stats"]["total_files"] == 2
        assert {node["name"] for node in added["root"]["children"]} == {"renamed.js", "added.jsx"}

        renamed.unlink()
        deleted = analyze(selected)
        assert deleted["cached"] is False
        assert deleted["stats"]["total_files"] == 1
        assert deleted["root"]["children"][0]["name"] == "added.jsx"
        assert analyze(other)["cached"] is True


def test_complexity_selection_and_hash_share_languages_target_filters_and_limit(tmp_path, monkeypatch):
    from services.complexity_analyzer import complexity_files, complexity_fingerprint
    root = tmp_path / "project"; root.mkdir()
    selected = root / "chosen"; selected.mkdir()
    for name in ("a.js", "b.jsx", "c.py", "d.ts", "e.tsx", "f.java"):
        (selected / name).write_text("// selected source\n")
    for directory in ("node_modules", ".hidden", "dist"):
        (selected / directory).mkdir()
        (selected / directory / "skip.js").write_text("private ignored code")
    outside = tmp_path / "outside.js"; outside.write_text("outside")
    (selected / "linked.js").symlink_to(outside)
    assert len(complexity_files(str(root), str(selected)).files) == 6
    js = complexity_files(str(root), str(selected), ["javascript"])
    assert [Path(path).name for path in js.files] == ["a.js", "b.jsx"]
    before = complexity_fingerprint(str(root), str(selected), ["javascript"])
    (root / "not-target.js").write_text("changed outside target")
    (selected / "c.py").write_text("changed other language")
    outside.write_text("changed linked source")
    assert complexity_fingerprint(str(root), str(selected), ["javascript"]) == before
    # Existing explicit-file analysis remains independent of directory language filters.
    explicit = complexity_files(str(root), str(selected / "a.js"), ["python"])
    assert explicit.files == (str(selected / "a.js"),)
    monkeypatch.setattr('services.complexity_analyzer._MAX_FILES', 1)
    limited = complexity_files(str(root), str(selected), ["javascript"])
    assert limited.files == (str(selected / "a.js"),) and limited.truncated
    first = complexity_fingerprint(str(root), str(selected), ["javascript"])
    (selected / "b.jsx").write_text("changed after file limit")
    assert complexity_fingerprint(str(root), str(selected), ["javascript"]) == first
    (selected / "b.jsx").unlink()
    assert complexity_fingerprint(str(root), str(selected), ["javascript"]) != first


def _complexity_worker_mutating_after_analysis(request, output, parent_pid):
    """Mutate deterministically inside a real spawned worker after actual analysis."""
    from routers import code_quality
    original = code_quality.analyze_complexity_sync

    def mutate(current):
        result = original(current)
        (Path(current["project_root"]) / "entry.js").write_text("function f(x) { if (x) return 1; return 0; }\n")
        return result

    code_quality.analyze_complexity_sync = mutate
    jobs._worker("complexity", request, output, parent_pid, 30)


def test_complexity_worker_rejects_mutation_during_analysis_without_caching(tmp_path):
    source = tmp_path / "entry.js"
    source.write_text("function f(x) { return x; }\n")
    context = multiprocessing.get_context("spawn")
    reader, writer = context.Pipe(duplex=True)
    process = context.Process(
        target=_complexity_worker_mutating_after_analysis,
        args=(payload(tmp_path), writer, os.getpid()),
        daemon=True,
    )
    process.start()
    writer.close()
    try:
        assert reader.poll(10), "worker did not fingerprint its selected files"
        probe = json.loads(reader.recv_bytes(jobs.MAX_RESULT_BYTES))
        assert "cache_probe" in probe
        reader.send_bytes(b"run")
        assert reader.poll(10), "worker did not finish actual complexity analysis"
        result = json.loads(reader.recv_bytes(jobs.MAX_RESULT_BYTES))
        assert result == {"status": 409, "error": "Project changed during analysis; retry"}
        assert "result" not in result
        assert jobs._cached(probe["cache_probe"]) is None
        assert "if (x)" in source.read_text()
        process.join(5)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join(5)
        reader.close()
        writer.close()
