from __future__ import annotations

from typing import Any


_STATUS_BY_KIND = {
    "invalid_argument": 400,
    "permission_denied": 403,
    "not_found": 404,
    "conflict": 409,
    "runtime_unavailable": 503,
    "internal_error": 500,
}


class WorkerDomainError(Exception):
    """Transport-neutral application error returned by every worker adapter."""

    def __init__(self, kind: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.status_code = _STATUS_BY_KIND.get(kind, 500)
        self.data = {"kind": kind, "error": message, **details}
        self.detail = self.data
