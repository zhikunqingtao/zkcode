"""
zkcode HTTP 客户端 — §4.21.8

httpx 同步/异步 HTTP + SSE 客户端封装。
所有请求路由到 Rust REST + SSE 端点。
"""

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

import httpx

MAX_SSE_EVENT_BYTES = 16 * 1024 * 1024


class StreamProtocolError(ValueError):
    """A malformed or incomplete stream must never be reported as success."""


class StreamInterruptedError(StreamProtocolError):
    """A clean EOF without a terminal event can resume a still-live execution."""

# Token 默认路径
DEFAULT_TOKEN_PATH = Path.home() / ".zk" / "access-token"
LEGACY_TOKEN_PATH = Path.home() / ".config" / "zkcode" / "access-token"

# 环境变量映射
ENV_SERVER = "ZK_SERVER"
ENV_TOKEN = "ZK_TOKEN"
ENV_MODEL = "ZK_MODEL"


@dataclass
class StreamEvent:
    """SSE 流式事件"""
    type: str       # thinking | text | tool_use | tool_result | error | message_complete
    data: dict


class ZkcodeClient:
    """zkcode CLI HTTP 客户端 — httpx 封装"""

    def __init__(
        self,
        server: str = "http://127.0.0.1:8082",
        token: Optional[str] = None,
        timeout: int = 300,
    ) -> None:
        self.server = server.rstrip("/")
        self.token = token or self._load_token()
        self.timeout = timeout

    def _load_token(self) -> Optional[str]:
        """加载认证 Token: 环境变量 > 文件"""
        import os
        env_token = os.environ.get(ENV_TOKEN) or os.environ.get("AICA_TOKEN")
        if env_token:
            return env_token
        for path in (DEFAULT_TOKEN_PATH, LEGACY_TOKEN_PATH):
            if path.exists():
                return path.read_text(encoding="utf-8").strip()
        return None

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def sync_query(self, body: dict) -> dict:
        """同步查询 — POST /api/query"""
        url = f"{self.server}/api/query"
        response = httpx.post(
            url,
            json=body,
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def list_projects(self) -> list[dict]:
        """列出服务端已注册的 Project。"""
        url = f"{self.server}/api/projects"
        response = httpx.get(
            url,
            headers=self._headers(),
            timeout=min(self.timeout, 30),
        )
        response.raise_for_status()
        return response.json()

    def create_project(
        self,
        name: str,
        workspace_root: str,
    ) -> dict:
        """注册一个服务端可访问的 Project 工作区。"""
        url = f"{self.server}/api/projects"
        response = httpx.post(
            url,
            json={
                "name": name,
                "workspaceRoot": workspace_root,
            },
            headers=self._headers(),
            timeout=min(self.timeout, 30),
        )
        response.raise_for_status()
        return response.json()

    def stream_query(self, body: dict) -> Iterator[dict]:
        """SSE 流式查询 — POST /api/query/stream

        使用 httpx 的 stream 功能读取 SSE 事件流，
        每个事件解析为 JSON dict 并 yield。
        """
        url = f"{self.server}/api/query/stream"
        headers = self._headers()
        headers["Accept"] = "text/event-stream, application/json"

        request_id = body.get("requestId")
        cursor = None
        method = "POST"
        retries = 0
        deadline = time.monotonic() + self.timeout
        with httpx.Client(timeout=self.timeout) as client:
            while True:
                try:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise StreamProtocolError("Query stream deadline exceeded")
                    arguments = {"json": body} if method == "POST" else {}
                    with client.stream(method, url, headers=headers, timeout=remaining, **arguments) as response:
                        if response.is_error:
                            response.read()  # Keep REST errors available after closing.
                        response.raise_for_status()
                        for event in self._decode_sse(response.iter_bytes()):
                            if event.get("event") == "query_started":
                                received = event.get("requestId")
                                if request_id is not None and received != request_id:
                                    raise StreamProtocolError("Query request identity changed")
                                request_id = received
                            event_id = event.get("eventId")
                            if event_id is not None:
                                prefix, separator, sequence = event_id.rpartition(":")
                                if not separator or prefix != request_id or not sequence.isascii() or not sequence.isdigit():
                                    raise StreamProtocolError("Query cursor identity is invalid")
                                cursor = event_id
                            yield event
                    return
                except (httpx.TransportError, StreamInterruptedError):
                    # Only GET resumes are allowed. Even if the first POST lost
                    # its response, never send the user request or tools again.
                    if retries >= 2 or not request_id or time.monotonic() >= deadline:
                        raise
                    retries += 1
                    method = "GET"
                    url = f"{self.server}/api/query/{request_id}/stream"
                    headers["Last-Event-ID"] = cursor or f"{request_id}:0"

    @classmethod
    def _decode_sse(cls, chunks) -> Iterator[dict]:
        """Bound the buffer before decoding, including a peer that never sends LF."""
        line = bytearray()
        lines = []
        frame_size = 0
        terminal = False
        previous_cr = False
        for chunk in chunks:
            # Byte-wise framing accepts CR, LF, and CRLF without splitting UTF-8.
            for byte in chunk:
                if previous_cr and byte == 10:
                    previous_cr = False
                    continue
                previous_cr = byte == 13
                if byte not in (10, 13):
                    if frame_size + len(line) + 1 > MAX_SSE_EVENT_BYTES:
                        raise StreamProtocolError("SSE event exceeds the size limit")
                    line.append(byte)
                    continue
                try:
                    decoded = line.decode("utf-8")
                except UnicodeDecodeError as error:
                    raise StreamProtocolError("SSE contains invalid UTF-8") from error
                frame_size += len(line) + 1
                line.clear()
                if decoded:
                    lines.append(decoded)
                    continue
                parsed = cls._parse_sse_event("\n".join(lines))
                lines, frame_size = [], 0
                if parsed is not None:
                    if terminal:
                        raise StreamProtocolError("SSE contains data after its terminal event")
                    terminal = parsed.get("event", parsed.get("type")) == "complete"
                    yield parsed
        if line or lines or not terminal:
            raise StreamInterruptedError("SSE ended before a complete terminal event")

    def cancel_query(self, request_id: str) -> dict:
        """Request cancellation of one exact invocation, including sync queries."""
        response = httpx.post(
            f"{self.server}/api/query/{request_id}/cancel",
            headers=self._headers(), timeout=3,
        )
        response.raise_for_status()
        return response.json()

    def conversation_query(self, body: dict) -> dict:
        """多轮会话查询 — POST /api/query/conversation"""
        url = f"{self.server}/api/query/conversation"
        response = httpx.post(
            url,
            json=body,
            headers=self._headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def health_check(self) -> dict:
        """健康检查 — GET /api/health"""
        url = f"{self.server}/api/health"
        response = httpx.get(url, headers=self._headers(), timeout=5)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _parse_sse_event(event_str: str) -> Optional[dict]:
        """解析 SSE 事件字符串为 dict"""
        data_lines = []
        event_name = None
        event_id = None
        for line in event_str.splitlines():
            if line.startswith(":"):
                continue
            field, separator, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if field == "data":
                data_lines.append(value if separator else "")
            elif field == "event":
                event_name = value
            elif field == "id" and "\0" not in value:
                event_id = value
        if data_lines:
            raw = "\n".join(data_lines)
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as error:
                raise StreamProtocolError("SSE contains invalid JSON") from error
            if not isinstance(data, dict):
                raise StreamProtocolError("SSE data must be a JSON object")
            if event_name:
                data["event"] = event_name
                data.setdefault("type", event_name)
            if event_id is not None:
                data["eventId"] = event_id
            return data
        return None
