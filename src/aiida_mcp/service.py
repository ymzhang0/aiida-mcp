"""Generic AiiDA operations for one explicitly configured runtime."""
from __future__ import annotations

import asyncio
import time
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

    # Processes
    async def process_list(self, *, limit: int = 20, process_state: str | None = None, label: str | None = None, root_only: bool = True, days: int | None = None, group_uuid: str | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": max(1, min(int(limit), 200)), "node_type": "ProcessNode", "root_only": bool(root_only)}
        for key, value in {"process_state": process_state, "label": label, "days": days, "group_uuid": group_uuid}.items():
            if value not in (None, ""): params[key] = value
        return await self._call("node.recent", params)
    async def process_control(self, action: str, identifiers: list[str], *, force: bool = False) -> dict[str, Any]:
        return await self._call("process.control", {"action": action, "identifiers": [str(item) for item in identifiers], "force": bool(force)})

    # Workflows and submission
    async def workflow_catalog(self) -> dict[str, Any]: return await self._call("workflow.catalog")
    async def workflow_spec(self, entry_point: str) -> dict[str, Any]: return await self._call("submission.spec", {"entry_point": str(entry_point)})
    async def workflow_input_candidates(self, entry_point: str, port_path: str, *, limit: int = 50) -> dict[str, Any]:
        return await self._call("workflow.input_candidates", {"entry_point": str(entry_point), "port_path": str(port_path), "limit": max(1, min(int(limit), 200))})
    async def code_list(self, computer_label: str | None = None) -> dict[str, Any]:
        return await self._call("code.list", {"computer_label": computer_label} if computer_label else {})
    async def submission_draft(self, draft: dict[str, Any]) -> dict[str, Any]: return await self._call("submission.builder_draft", dict(draft))
    async def submission_submit(self, draft: dict[str, Any]) -> dict[str, Any]: return await self._call("submission.submit", {"draft": dict(draft)})

    # Computers and codes
    async def ssh_hosts(self) -> dict[str, Any]: return await self._call("infrastructure.ssh_config")
    async def computer_list(self) -> dict[str, Any]: return await self._call("computer.list")
    async def infrastructure_capabilities(self) -> dict[str, Any]: return await self._call("infrastructure.capabilities")
    async def computer_create(self, params: dict[str, Any]) -> dict[str, Any]: return await self._call("computer.create", params)
    async def code_check_executable(self, computer_label: str, executable: str, prepend_text: str | None = None) -> dict[str, Any]:
        return await self._call("code.check_executable", {"computer_label": computer_label, "executable": executable, "prepend_text": prepend_text})
    async def code_create(self, params: dict[str, Any]) -> dict[str, Any]: return await self._call("code.create", params)

    # Structures
    async def create_structure(self, params: dict[str, Any]) -> dict[str, Any]: return await self._call("structure.create", params)

    # Waiting without burning agent turns
    async def wait_for_process(self, identifier: str, *, timeout_seconds: float = 300, poll_seconds: float = 5) -> dict[str, Any]:
        deadline = time.monotonic() + max(0.0, min(float(timeout_seconds), 1800.0))
        while True:
            state = await self._call("process.state", {"identifier": str(identifier)})
            remaining = deadline - time.monotonic()
            if state.get("terminated") or remaining <= 0:
                return {**state, "timed_out": not state.get("terminated")}
            await asyncio.sleep(min(max(1.0, float(poll_seconds)), remaining))

    # Plugins (loaded inside the worker from the `aiida_mcp.tools` entry-point group)
    async def plugin_manifest(self) -> dict[str, Any]: return await self._call("plugins.manifest")
    async def plugin_call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]: return await self._call("plugins.call", {"tool": tool, "arguments": dict(arguments)})

    async def _call(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        runtime = Runtime("configured-runtime", self.runtime.python_interpreter_path, self.runtime.aiida_profile, self.runtime.workspace_path)
        return await self.workers.request(runtime, method, params)
