"""Small native AiiDA archive inspection."""
from __future__ import annotations

from pathlib import Path

from aiida import orm, profile_context
from aiida.storage.sqlite_zip.backend import SqliteZipBackend

from aiida_mcp.worker.protocol.errors import WorkerDomainError


def inspect_archive(path: str, *, workspace_path: str | None) -> dict[str, object]:
    if not workspace_path:
        raise WorkerDomainError("permission_denied", "archive.inspect requires a configured workspace")
    workspace = Path(workspace_path).resolve()
    requested = Path(path)
    if requested.is_absolute():
        raise WorkerDomainError("permission_denied", "archive path must be relative to the configured workspace")
    archive = (workspace / requested).resolve()
    try:
        archive.relative_to(workspace)
    except ValueError as exc:
        raise WorkerDomainError("permission_denied", "archive path escapes the configured workspace") from exc
    if not archive.is_file():
        raise WorkerDomainError("not_found", f"AiiDA archive not found: {path}")
    with profile_context(SqliteZipBackend.create_profile(archive), allow_switch=True):
        return {"path": str(archive.relative_to(workspace)), "nodes": orm.QueryBuilder().append(orm.Node).count(), "groups": orm.QueryBuilder().append(orm.Group).count()}
