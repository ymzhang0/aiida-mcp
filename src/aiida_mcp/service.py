"""Generic AiiDA operations for one explicitly configured runtime."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from .runtime import Runtime, WorkerRuntimePool


@dataclass(frozen=True)
class RuntimeConfig:
    python_interpreter_path: str
    aiida_profile: str
    workspace_path: str | None = None


class AiiDAService:
    """No project, campaign, or Group-scope policy belongs here."""

    def __init__(self, runtime: RuntimeConfig, workers: WorkerRuntimePool) -> None:
        self.runtime = runtime
        self.workers = workers

    async def status(self) -> dict[str, Any]: return await self._call("runtime.status")
    async def nodes(self, *, limit: int = 50, node_type: str | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": max(1, min(int(limit), 200)), "root_only": False}
        if node_type: params["node_type"] = node_type
        return await self._call("node.recent", params)
    async def node_get(self, pk: int) -> dict[str, Any]: return await self._call("node.summary", {"pk": int(pk)})
    async def node_links(self, identifier: str) -> dict[str, Any]: return await self._call("node.links", {"identifier": str(identifier)})
    async def node_compare(self, left: str, right: str, *, depth: int = 2, max_differences: int = 100) -> dict[str, Any]:
        return await self._call("node.compare", {"left": str(left), "right": str(right), "max_depth": max(0, min(int(depth), 6)), "max_differences": max(1, min(int(max_differences), 500))})
    async def process_get(self, identifier: str) -> dict[str, Any]: return await self._call("process.detail", {"identifier": str(identifier)})
    async def process_workgraph(self, identifier: str) -> dict[str, Any]: return await self._call("process.workgraph", {"identifier": str(identifier)})
    async def process_logs(self, identifier: str) -> dict[str, Any]: return await self._call("process.logs", {"identifier": str(identifier)})
    async def group_get(self, identifier: str) -> dict[str, Any]:
        value = str(identifier).strip()
        try:
            UUID(value)
            return await self._call("group.inspect_uuid", {"group_uuid": value, "limit": 1})
        except ValueError:
            return await self._call("group.inspect", {"label": value, "limit": 1})
    async def group_list(self) -> dict[str, Any]: return await self._call("group.list")
    async def provenance_walk(self, root_uuid: str, *, direction: str, depth: int = 2) -> dict[str, Any]: return await self._call("provenance.walk", {"root_uuid": root_uuid, "direction": direction, "depth": max(0, min(int(depth), 6))})
    async def provenance_roots(self, group_uuid: str, *, limit: int = 100) -> dict[str, Any]: return await self._call("provenance.roots", {"group_uuid": group_uuid, "limit": max(1, min(int(limit), 200))})
    async def provenance_subgraph(self, group_uuid: str, root_uuid: str, *, depth: int = 2) -> dict[str, Any]: return await self._call("provenance.subgraph", {"group_uuid": group_uuid, "root_uuid": root_uuid, "depth": max(0, min(int(depth), 6))})
    async def provenance_node_detail(self, group_uuid: str, uuid: str) -> dict[str, Any]: return await self._call("provenance.node_detail", {"group_uuid": group_uuid, "uuid": uuid})
    async def archive_inspect(self, path: str) -> dict[str, Any]: return await self._call("archive.inspect", {"path": str(path)})

    async def _call(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        runtime = Runtime("configured-runtime", self.runtime.python_interpreter_path, self.runtime.aiida_profile, self.runtime.workspace_path)
        return await self.workers.request(runtime, method, params)
