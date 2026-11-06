"""Generic MCP tools for one explicitly configured AiiDA runtime."""
from __future__ import annotations
from typing import Any
from fastmcp import FastMCP
from fastmcp.tools import Tool
from fastmcp.tools.base import ToolResult
from mcp.types import ToolAnnotations
from .service import AiiDAService

_READ_ONLY = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False}
_WRITE = {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": False}
_DESTRUCTIVE = {"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": False}

_INSTRUCTIONS = """AiiDA tools for the single Python environment and profile selected when this server started.
Checking work: aiida_process_list (filter by process_state such as waiting/running/finished/excepted/killed)
-> aiida_process_get / aiida_process_logs / aiida_provenance_descendants for one process.
Running work: aiida_workflow_list -> aiida_workflow_spec -> aiida_code_list / aiida_workflow_input_candidates
-> aiida_submission_draft (validates, stores nothing) -> aiida_submit -> poll aiida_process_get.
Input values: plain JSON for base types (2 -> Int, {"a": 1} -> Dict); reference stored nodes as {"pk": N} or
{"uuid": "..."}; codes as "label@computer". Tools prefixed with a plugin name come from installed plugins."""


class PluginTool(Tool):
    """Proxy one worker-side plugin function; its schema comes from the worker manifest."""

    service: Any = None

    async def run(self, arguments: dict[str, Any]) -> ToolResult:
        result = await self.service.plugin_call(self.name, arguments)
        return ToolResult(structured_content=result)


def build_mcp_server(service: AiiDAService, *, allow_write: bool = False, plugin_tools: list[dict[str, Any]] | None = None) -> FastMCP:
    server = FastMCP("aiida", instructions=_INSTRUCTIONS if allow_write else _INSTRUCTIONS + "\nThis server is read-only: submission and process control are disabled.")

    # Runtime, nodes, groups, provenance
    @server.tool(name="aiida_runtime_status", description="Read the configured AiiDA runtime status: profile, versions, daemon, installed workflow plugins.", annotations=_READ_ONLY)
    async def runtime_status() -> dict[str, Any]: return await service.status()
    @server.tool(name="aiida_node_search", description="List recent nodes in the configured runtime.", annotations=_READ_ONLY)
    async def node_search(limit: int = 50, node_type: str | None = None) -> dict[str, Any]: return await service.nodes(limit=limit, node_type=node_type)
    @server.tool(name="aiida_node_get", description="Read a node by PK.", annotations=_READ_ONLY)
    async def node_get(pk: int) -> dict[str, Any]: return await service.node_get(pk)
    @server.tool(name="aiida_node_links", description="Read direct AiiDA links for a node UUID or PK.", annotations=_READ_ONLY)
    async def node_links(identifier: str) -> dict[str, Any]: return await service.node_links(identifier)
    @server.tool(name="aiida_node_compare", description="Compare two AiiDA nodes recursively by attributes, ports, links, arrays, and repository content.", annotations=_READ_ONLY)
    async def node_compare(left: str, right: str, depth: int = 2, max_differences: int = 100) -> dict[str, Any]: return await service.node_compare(left, right, depth=depth, max_differences=max_differences)
    @server.tool(name="aiida_group_get", description="Read a Group by UUID or label.", annotations=_READ_ONLY)
    async def group_get(identifier: str) -> dict[str, Any]: return await service.group_get(identifier)
    @server.tool(name="aiida_group_list", description="List Groups in the configured runtime.", annotations=_READ_ONLY)
    async def group_list() -> dict[str, Any]: return await service.group_list()
    @server.tool(name="aiida_provenance_ancestors", description="Read bounded upstream provenance for a node UUID.", annotations=_READ_ONLY)
    async def provenance_ancestors(root_uuid: str, depth: int = 2) -> dict[str, Any]: return await service.provenance_walk(root_uuid, direction="ancestors", depth=depth)
    @server.tool(name="aiida_provenance_descendants", description="Read bounded downstream provenance for a node UUID.", annotations=_READ_ONLY)
    async def provenance_descendants(root_uuid: str, depth: int = 2) -> dict[str, Any]: return await service.provenance_walk(root_uuid, direction="descendants", depth=depth)
    @server.tool(name="aiida_provenance_roots", description="List top-level process roots for a supplied Group UUID.", annotations=_READ_ONLY)
    async def provenance_roots(group_uuid: str, limit: int = 100) -> dict[str, Any]: return await service.provenance_roots(group_uuid, limit=limit)
    @server.tool(name="aiida_provenance_subgraph", description="Read a bounded real AiiDA provenance graph.", annotations=_READ_ONLY)
    async def provenance_subgraph(group_uuid: str, root_uuid: str, depth: int = 2) -> dict[str, Any]: return await service.provenance_subgraph(group_uuid, root_uuid, depth=depth)
    @server.tool(name="aiida_archive_inspect", description="Read a workspace-relative AiiDA archive summary without importing it.", annotations=_READ_ONLY)
    async def archive_inspect(path: str) -> dict[str, Any]: return await service.archive_inspect(path)

    # Processes
    @server.tool(name="aiida_process_list", description="List processes newest first. Filter by process_state (created, waiting, running, finished, excepted, killed), label/process-label substring or PK, age in days, or Group UUID. root_only=False includes sub-processes.", annotations=_READ_ONLY)
    async def process_list(limit: int = 20, process_state: str | None = None, label: str | None = None, root_only: bool = True, days: int | None = None, group_uuid: str | None = None) -> dict[str, Any]:
        return await service.process_list(limit=limit, process_state=process_state, label=label, root_only=root_only, days=days, group_uuid=group_uuid)
    @server.tool(name="aiida_process_get", description="Read a process by PK or UUID: state, exit status, inputs, outputs, and called sub-processes.", annotations=_READ_ONLY)
    async def process_get(identifier: str) -> dict[str, Any]: return await service.process_get(identifier)
    @server.tool(name="aiida_process_workgraph", description="Read the call graph of an aiida-workgraph WorkGraph process.", annotations=_READ_ONLY)
    async def process_workgraph(identifier: str) -> dict[str, Any]: return await service.process_workgraph(identifier)
    @server.tool(name="aiida_process_logs", description="Read process reports and scheduler stderr excerpt by PK or UUID.", annotations=_READ_ONLY)
    async def process_logs(identifier: str) -> dict[str, Any]: return await service.process_logs(identifier)

    # Workflows, codes, submission drafting
    @server.tool(name="aiida_workflow_list", description="List installed workflow entry points with their description, protocols, and required inputs.", annotations=_READ_ONLY)
    async def workflow_list() -> dict[str, Any]: return await service.workflow_catalog()
    @server.tool(name="aiida_workflow_spec", description="Read the full input port specification of a workflow or calculation entry point (e.g. 'quantumespresso.pw.base' or 'core.arithmetic.add').", annotations=_READ_ONLY)
    async def workflow_spec(entry_point: str) -> dict[str, Any]: return await service.workflow_spec(entry_point)
    @server.tool(name="aiida_workflow_input_candidates", description="List stored nodes compatible with one input port, e.g. port_path='pw.structure'.", annotations=_READ_ONLY)
    async def workflow_input_candidates(entry_point: str, port_path: str, limit: int = 50) -> dict[str, Any]: return await service.workflow_input_candidates(entry_point, port_path, limit=limit)
    @server.tool(name="aiida_code_list", description="List configured codes, optionally only those on one computer. Use full_label ('label@computer') as a code input.", annotations=_READ_ONLY)
    async def code_list(computer_label: str | None = None) -> dict[str, Any]: return await service.code_list(computer_label)
    @server.tool(name="aiida_submission_draft", description="Build and validate a process builder without storing or submitting anything. Give explicit `inputs`, or omit them and give `protocol` plus `protocol_arguments` (and optional `overrides`) for get_builder_from_protocol.", annotations=_READ_ONLY)
    async def submission_draft(entry_point: str, inputs: dict[str, Any] | None = None, protocol: str | None = None, protocol_arguments: dict[str, Any] | None = None, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.submission_draft(_draft(entry_point, inputs, protocol, protocol_arguments, overrides))

    if allow_write:
        @server.tool(name="aiida_submit", description="Submit a workflow or calculation to the AiiDA daemon. Takes the same arguments as aiida_submission_draft; run that first. Returns the new process PK.", annotations=_WRITE)
        async def submit(entry_point: str, inputs: dict[str, Any] | None = None, protocol: str | None = None, protocol_arguments: dict[str, Any] | None = None, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
            return await service.submission_submit(_draft(entry_point, inputs, protocol, protocol_arguments, overrides))
        @server.tool(name="aiida_process_kill", description="Kill running or waiting processes (PKs or UUIDs). Irreversible.", annotations=_DESTRUCTIVE)
        async def process_kill(identifiers: list[str], force: bool = False) -> dict[str, Any]: return await service.process_control("kill", identifiers, force=force)
        @server.tool(name="aiida_process_pause", description="Pause running or waiting processes (PKs or UUIDs).", annotations=_WRITE)
        async def process_pause(identifiers: list[str]) -> dict[str, Any]: return await service.process_control("pause", identifiers)
        @server.tool(name="aiida_process_play", description="Resume paused processes (PKs or UUIDs).", annotations=_WRITE)
        async def process_play(identifiers: list[str]) -> dict[str, Any]: return await service.process_control("play", identifiers)

    for item in plugin_tools or []:
        if not item.get("read_only") and not allow_write:
            continue
        server.add_tool(PluginTool(
            name=str(item["name"]),
            description=str(item.get("description") or item["name"]),
            parameters=dict(item.get("input_schema") or {"type": "object", "properties": {}}),
            annotations=ToolAnnotations(**(_READ_ONLY if item.get("read_only") else _DESTRUCTIVE)),
            tags={"plugin", str(item.get("plugin") or "")},
            service=service,
        ))
    return server


def _draft(entry_point: str, inputs: dict[str, Any] | None, protocol: str | None, protocol_arguments: dict[str, Any] | None, overrides: dict[str, Any] | None) -> dict[str, Any]:
    draft: dict[str, Any] = {"entry_point": entry_point, "protocol": protocol, "intent_data": dict(protocol_arguments or {}), "overrides": dict(overrides or {})}
    if inputs is not None:
        draft["inputs"] = dict(inputs)
    return draft
