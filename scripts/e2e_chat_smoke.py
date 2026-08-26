#!/usr/bin/env python3
"""S9 真实模型 e2e 冒烟：chat 上行 → LLM 流式 → WS 下行 → 落库读回。

前置：zk-server 真实进程已启动（ZK_LLM_BASE_URL / ZK_LLM_API_KEY 已配置）。
用法：ZK_E2E_BASE=http://127.0.0.1:<port> ZK_E2E_MODEL=kimi-k3 python3 scripts/e2e_chat_smoke.py

断言链：POST /api/sessions（model 指定）→ /ws bind_session → session_restored
→ user_message 上行 → stream_delta*（可含 thinking_delta*）→ message_complete
（usage/stopReason/runId/committedMessages）→ session_list_updated →
GET messages 读回 user+assistant 两条。密钥不经过本脚本，帧内容截断打印。
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import urllib.request

import websockets

BASE = os.environ.get("ZK_E2E_BASE", "http://127.0.0.1:8082")
MODEL = os.environ.get("ZK_E2E_MODEL", "kimi-k3")
WS_URL = BASE.replace("http://", "ws://") + "/ws"
SCENARIO_TIMEOUT_SECONDS = 90


def http_json(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def trunc(s: str, n: int = 160) -> str:
    return s if len(s) <= n else s[:n] + f"…(+{len(s) - n})"


async def main() -> int:
    session = http_json("POST", "/api/sessions", {"model": MODEL})
    sid = session["sessionId"]
    print(f"[1] session created: id={sid} model={session.get('model')}")

    kinds: list[str] = []
    complete: dict | None = None
    text_parts: list[str] = []
    thinking_chars = 0

    async with websockets.connect(WS_URL, max_size=8 * 1024 * 1024) as ws:
        await ws.send(json.dumps({
            "type": "bind_session", "sessionId": sid,
            "bindRequestId": "e2e-bind-1", "bindingEpoch": 1,
            "protocolVersion": 3,
        }))
        restored = json.loads(await asyncio.wait_for(ws.recv(), 15))
        assert restored["type"] == "session_restored", restored
        print(f"[2] bound: session_restored bindRequestId={restored.get('bindRequestId')}")

        await ws.send(json.dumps({
            "type": "user_message",
            "text": "请用一句话（不超过30字）介绍你自己。",
        }))
        print("[3] user_message sent, streaming:")
        while True:
            frame = json.loads(await asyncio.wait_for(ws.recv(), 85))
            kind = frame.get("type")
            kinds.append(kind)
            if kind == "stream_delta":
                text_parts.append(frame.get("delta", ""))
            elif kind == "thinking_delta":
                thinking_chars += len(frame.get("delta", ""))
            elif kind == "message_complete":
                complete = frame
            elif kind == "error":
                print("    ERROR frame:", json.dumps(frame, ensure_ascii=False))
            elif kind == "session_list_updated":
                break

    def squash(seq: list[str]) -> str:
        out, i = [], 0
        while i < len(seq):
            j = i
            while j < len(seq) and seq[j] == seq[i]:
                j += 1
            out.append(seq[i] if j - i == 1 else f"{seq[i]}×{j - i}")
            i = j
        return " → ".join(out)

    print(f"    frame sequence: {squash(kinds)}")
    full_text = "".join(text_parts)
    print(f"    assistant text: {trunc(full_text)}")
    if thinking_chars:
        print(f"    thinking chars: {thinking_chars}")
    assert complete is not None, "no message_complete received"
    assert "stream_delta" in kinds, "no stream_delta received"
    usage = complete.get("usage") or {}
    committed = complete.get("committedMessages") or []
    print(f"[4] message_complete: stopReason={complete.get('stopReason')} "
          f"runId={complete.get('runId')} usage={json.dumps(usage)} "
          f"committed={len(committed)}")
    assert complete.get("stopReason") == "end_turn", complete.get("stopReason")
    assert complete.get("runId"), "runId missing"
    assert usage.get("outputTokens", 0) > 0, usage
    assert len(committed) == 2, committed

    page = http_json("GET", f"/api/sessions/{sid}/messages?limit=10")
    msgs = page["messages"]
    roles = [m["type"] for m in msgs]
    print(f"[5] persisted messages: roles={roles} "
          f"assistant.stopReason={msgs[-1].get('stopReason')}")
    assert roles == ["user", "assistant"], roles
    stored_text = "".join(
        b.get("text", "") for b in msgs[-1]["content"] if b.get("type") == "text")
    assert stored_text == full_text, "persisted text != streamed text"
    print("[6] persisted assistant text matches streamed concatenation ✓")
    print("E2E SMOKE PASSED")
    return 0


async def bounded_main() -> int:
    try:
        return await asyncio.wait_for(main(), timeout=SCENARIO_TIMEOUT_SECONDS)
    except TimeoutError:
        print(
            f"E2E SMOKE FAILED: exceeded {SCENARIO_TIMEOUT_SECONDS}s hard limit",
            file=sys.stderr,
        )
        return 124


if __name__ == "__main__":
    sys.exit(asyncio.run(bounded_main()))
