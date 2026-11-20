"""Small native AiiDA archive inspection."""
from __future__ import annotations

from pathlib import Path

from aiida import orm, profile_context
from aiida.storage.sqlite_zip.backend import SqliteZipBackend

from aiida_mcp.worker.protocol.errors import WorkerDomainError


def inspect_archive(path: str, *, workspace_path: str | None) -> dict[str, object]:
    from aiida_mcp.worker.application.export_service import workspace_target

    workspace, archive = workspace_target(path, workspace_path)
    if not archive.is_file():
        raise WorkerDomainError("not_found", f"AiiDA archive not found: {path}")
    with profile_context(SqliteZipBackend.create_profile(archive), allow_switch=True):
        return {"path": str(archive.relative_to(workspace)), "nodes": orm.QueryBuilder().append(orm.Node).count(), "groups": orm.QueryBuilder().append(orm.Group).count()}
