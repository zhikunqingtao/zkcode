"""Bounded, cancellable static-analysis workers; cancellation reaps the process.

No executor thread is abandoned after an HTTP timeout. Results cross a bounded
pipe, and an owner-scoped request ID prevents cancellation of another session.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import multiprocessing
import os
import threading
import time
from dataclasses import dataclass, field
from collections import OrderedDict
from typing import Any

from fastapi import HTTPException, Request

MAX_PROCESSES = 2
MAX_RESULT_BYTES = 8 * 1024 * 1024
REQUEST_TTL_SECONDS = 120
MAX_REQUESTS = 256
MAX_CACHE_BYTES = 32 * 1024 * 1024
CACHE_TTL_SECONDS = 300
_cache: OrderedDict[str, tuple[float, bytes]] = OrderedDict()


@dataclass
class Job:
    cancelled: threading.Event = field(default_factory=threading.Event)
    active: bool = False
    expires: float = 0


_jobs: dict[tuple[str, str, str], Job] = {}
_lock = threading.Lock()


def _key(payload: dict) -> tuple[str, str, str]:
    return (payload["analysis_owner"], payload["project_root"], payload["request_id"])


def _prune() -> None:
    now = time.monotonic()
    for key in list(_jobs):
        if not _jobs[key].active and _jobs[key].expires <= now:
            del _jobs[key]
    for key in list(_cache):
        if _cache[key][0] <= now:
            del _cache[key]


def _cached(key: str) -> bytes | None:
    with _lock:
        _prune()
        entry = _cache.get(key)
        if entry:
            _cache.move_to_end(key)
            return entry[1]
    return None


def _remember(key: str, raw: bytes) -> None:
    with _lock:
        _prune()
        _cache[key] = (time.monotonic() + CACHE_TTL_SECONDS, raw)
        _cache.move_to_end(key)
        while len(_cache) > 32 or sum(len(entry[1]) for entry in _cache.values()) > MAX_CACHE_BYTES:
            _cache.popitem(last=False)


def cancel(payload: dict) -> dict:
    with _lock:
        _prune()
        key = _key(payload)
        job = _jobs.get(key)
        if job is None:
            if len(_jobs) >= MAX_REQUESTS:
                raise HTTPException(429, "Analysis request registry is full")
            job = Job(expires=time.monotonic() + REQUEST_TTL_SECONDS)
            _jobs[key] = job
        job.cancelled.set()
        status = "cancelling" if job.active else "cancelled"
    return {"cancellationRequested": True, "status": status, "requestId": payload["request_id"]}


def _worker(kind: str, payload: dict, output: Any, parent_pid: int, timeout: float) -> None:
    def watchdog() -> None:
        deadline = time.monotonic() + timeout + 1
        while time.monotonic() < deadline and os.getppid() == parent_pid:
            time.sleep(0.2)
        os._exit(124)

    threading.Thread(target=watchdog, daemon=True).start()
    try:
        from analyzers.code_path_tracer import _graph_fingerprint
        languages = payload.get("languages") if kind == "endpoints" else None
        skip_tests = not payload.get("options", {}).get("include_tests", False)

        def current_fingerprint():
            if kind == "complexity":
                from services.complexity_analyzer import complexity_fingerprint
                return complexity_fingerprint(payload["project_root"], payload.get("target_path"), payload.get("languages"))
            return _graph_fingerprint(payload["project_root"], languages, skip_tests)

        fingerprint = current_fingerprint()
        # Fingerprinting also runs in the killable process: large projects cannot
        # leave an abandoned scanner thread behind on cancellation. The cache is
        # parent-owned, bounded, memory-only and separated by authorized owner.
        options = {key: value for key, value in payload.items() if key != "request_id"}
        cache_key = hashlib.sha256(json.dumps([kind, options, fingerprint], sort_keys=True).encode()).hexdigest()
        output.send_bytes(json.dumps({"cache_probe": cache_key}).encode())
        if output.recv_bytes(16) != b"run":
            return
        from routers.analysis import (
            APIEndpointRequest, CodePathRequest, DiagramRequest, ChangeImpactRequest,
            _generate_diagram_sync, _scan_endpoints_sync, _trace_code_path_sync, _change_impact_sync,
        )
        if kind == "diagram":
            result = _generate_diagram_sync(DiagramRequest(**payload)).model_dump()
        elif kind == "endpoints":
            result = _scan_endpoints_sync(APIEndpointRequest(**payload))
        elif kind == "trace":
            result = _trace_code_path_sync(CodePathRequest(**payload))
        elif kind == "impact":
            result = _change_impact_sync(ChangeImpactRequest(**payload))
        elif kind == "complexity":
            from routers.code_quality import analyze_complexity_sync
            result = analyze_complexity_sync(payload)
        else:
            raise ValueError("Unknown analysis job")
        if fingerprint != current_fingerprint():
            output.send_bytes(b'{"status":409,"error":"Project changed during analysis; retry"}')
            return
        encoded = json.dumps({"result": result}, ensure_ascii=False).encode("utf-8")
        if len(encoded) > MAX_RESULT_BYTES:
            encoded = b'{"status":413,"error":"Analysis result exceeds size limit"}'
    except FileNotFoundError:
        encoded = b'{"status":404,"error":"Analysis target not found in project"}'
    except Exception:
        # Parser exceptions can contain source code; never copy them into logs.
        encoded = b'{"status":500,"error":"Static analysis failed"}'
    try:
        output.send_bytes(encoded)
    finally:
        output.close()


async def run(kind: str, payload: dict, request: Request | None, timeout: float = 30) -> dict:
    key = _key(payload)
    with _lock:
        _prune()
        if key in _jobs:
            raise HTTPException(409, "Analysis request was already submitted or cancelled")
        if len(_jobs) >= MAX_REQUESTS or sum(job.active for job in _jobs.values()) >= MAX_PROCESSES:
            raise HTTPException(429, "Analysis workers are busy; retry with a new request")
        job = Job(active=True)
        _jobs[key] = job
    context = multiprocessing.get_context("spawn")
    reader, writer = context.Pipe(duplex=True)
    process = context.Process(target=_worker, args=(kind, payload, writer, os.getpid(), timeout), daemon=True)
    started = False
    cache_key = None
    try:
        process.start()
        started = True
        writer.close()
        deadline = time.monotonic() + timeout
        while True:
            if job.cancelled.is_set() or (request is not None and await request.is_disconnected()):
                raise HTTPException(409, "Analysis request cancelled")
            if time.monotonic() >= deadline:
                raise HTTPException(408, "Analysis exceeded its execution deadline")
            if reader.poll():
                try:
                    raw = await asyncio.to_thread(reader.recv_bytes, MAX_RESULT_BYTES)
                    envelope = json.loads(raw)
                except (EOFError, OSError, ValueError) as error:
                    raise HTTPException(502, "Analysis worker returned an invalid result") from error
                if job.cancelled.is_set():
                    raise HTTPException(409, "Analysis request cancelled")
                if "cache_probe" in envelope:
                    cache_key = envelope["cache_probe"]
                    cached = _cached(cache_key)
                    if cached is not None:
                        result = json.loads(cached)["result"]
                        if kind == "complexity":
                            result["data"]["cached"] = True
                        return result
                    reader.send_bytes(b"run")
                    continue
                if "error" in envelope:
                    raise HTTPException(envelope["status"], envelope["error"])
                if cache_key:
                    _remember(cache_key, raw)
                return envelope["result"]
            if not process.is_alive():
                raise HTTPException(502, "Analysis worker stopped without a result")
            await asyncio.sleep(0.025)
    finally:
        # HTTP disconnect, explicit cancellation and shutdown may each cancel
        # this task. A second cancellation must not interrupt join/kill or mark
        # admission released before the owned worker has actually disappeared.
        cleanup = asyncio.create_task(asyncio.to_thread(
            _cleanup_worker, process, reader, writer, started, job,
        ))
        cancelled_during_cleanup = False
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                cancelled_during_cleanup = True
        cleanup.result()
        if cancelled_during_cleanup:
            raise asyncio.CancelledError


def _release_job(process: Any, job: Job) -> None:
    process.close()
    with _lock:
        job.active = False
        job.expires = time.monotonic() + REQUEST_TTL_SECONDS


def _reap_unconfirmed_worker(process: Any, job: Job) -> None:
    """Retain the exact child owner after an explicitly unconfirmed HTTP result."""
    while True:
        try:
            if not process.is_alive():
                _release_job(process, job)
                return
            process.kill()
            process.join(1)
        except (OSError, ValueError):
            # Do not free admission based on an exception. The child's watchdog
            # also exits when the parent dies, without executing project code.
            time.sleep(0.1)


def _cleanup_worker(process: Any, reader: Any, writer: Any, started: bool, job: Job) -> None:
    writer.close()
    try:
        if started:
            if process.is_alive():
                process.terminate()
            process.join(1)
            if process.is_alive():
                process.kill()
                process.join(3)
            if process.is_alive():
                # A separate owner retains both the process and admission. A
                # failed immediate join must not leak a permanently busy slot.
                threading.Thread(target=_reap_unconfirmed_worker, args=(process, job), daemon=True).start()
                raise HTTPException(503, "Analysis worker cleanup could not be confirmed")
            _release_job(process, job)
        else:
            with _lock:
                job.active = False
                job.expires = time.monotonic() + REQUEST_TTL_SECONDS
    finally:
        reader.close()
