"""Request-local suppression of body-bearing Python/library diagnostics.

ContextVars follow asyncio child tasks, including cleanup retained after disconnect.
The factory retains severity and source identity; the message, arguments and traceback
are suppressed before any existing handler (including rotating file handlers) sees them.
"""
from contextvars import ContextVar
from functools import wraps
import logging
import os

_ephemeral = ContextVar("zk_ephemeral_content", default=False)
_previous_factory = logging.getLogRecordFactory()


def _record_factory(*args, _previous=_previous_factory, **kwargs):
    record = _previous(*args, **kwargs)
    if _ephemeral.get():
        record.msg = "Temporary operation diagnostic suppressed"
        record.args = ()
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
    return record


logging.setLogRecordFactory(_record_factory)


def ephemeral_request(handler):
    """Propagate the host's body-retention policy across owned request tasks."""
    @wraps(handler)
    async def wrapped(*args, **kwargs):
        request = kwargs.get("request", kwargs.get("req"))
        if request is None and args:
            request = args[0]
        temporary = _ephemeral.get() or bool(getattr(request, "ephemeral_content", False))
        token = _ephemeral.set(temporary)
        try:
            return await handler(*args, **kwargs)
        finally:
            _ephemeral.reset(token)
    return wrapped


def require_body_free_browser_logging():
    # Playwright's driver emits DEBUG output outside Python logging and cannot
    # change its logging policy safely for one context of a shared process.
    if os.environ.get("DEBUG") or os.environ.get("PWDEBUG"):
        from fastapi import HTTPException
        raise HTTPException(status_code=409, detail="EPHEMERAL_BROWSER_DEBUG_LOGGING_UNSUPPORTED")


def is_ephemeral_request():
    """Whether the current operation must never create a durable fallback context."""
    return _ephemeral.get()
