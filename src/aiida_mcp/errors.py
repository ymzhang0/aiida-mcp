"""Surface worker error details to MCP clients.

Without this, a tool call that fails in the worker reaches the model as the
bare JSON-RPC message ("Invalid params"), with the reason left behind in
``WorkerRuntimeError.detail``. A model cannot correct a call it cannot diagnose.
"""
from __future__ import annotations

import json
from typing import Any

from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import Middleware

from .runtime import WorkerRuntimeError

_OMITTED_DETAIL_KEYS = frozenset({"status_code", "available_methods"})


def describe_worker_error(exc: WorkerRuntimeError) -> str:
    """Return the worker message followed by its structured detail as JSON."""
    message = str(exc)
    detail: dict[str, Any] = {key: value for key, value in exc.detail.items() if key not in _OMITTED_DETAIL_KEYS and value not in (None, "", [], {})}
    if detail.get("error") == message:
        detail.pop("error")
    if not detail:
        return message
    reason = detail.pop("reason", None)
    # The worker pool already appends the reason to its message; do not repeat it.
    if isinstance(reason, str) and reason and not message.endswith(reason):
        message = f"{message}: {reason}"
    if not detail:
        return message
    return f"{message}\n{json.dumps(detail, ensure_ascii=False, default=str)}"


def _worker_error(exc: BaseException) -> WorkerRuntimeError | None:
    """Find the worker error behind ``exc``; FastMCP wraps tool exceptions in ``ToolError``."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, WorkerRuntimeError):
            return current
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return None


class WorkerErrorMiddleware(Middleware):
    """Re-raise tool failures caused by a ``WorkerRuntimeError`` with its detail attached."""

    async def on_call_tool(self, context, call_next):
        try:
            return await call_next(context)
        except Exception as exc:
            worker_error = _worker_error(exc)
            if worker_error is None:
                raise
            raise ToolError(describe_worker_error(worker_error)) from worker_error
