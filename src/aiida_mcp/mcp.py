"""Read-only generic MCP tools for one explicitly configured AiiDA runtime."""
from __future__ import annotations
from typing import Any
from fastmcp import FastMCP
from .service import AiiDAService

_READ_ONLY = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False}

def build_mcp_server(service: AiiDAService) -> FastMCP:
    server = FastMCP("aiida", instructions="Read-only generic AiiDA tools for the runtime selected when this server started. These tools do not know projects, campaigns, or scope Groups.")
    @server.tool(name="aiida_runtime_status", description="Read the configured AiiDA runtime status.", annotations=_READ_ONLY)
    async def runtime_status() -> dict[str, Any]: return await service.status()
    @server.tool(name="aiida_node_search", description="List recent nodes in the configured runtime.", annotations=_READ_ONLY)
    async def node_search(limit: int = 50, node_type: str | None = None) -> dict[str, Any]: return await service.nodes(limit=limit, node_type=node_type)
    @server.tool(name="aiida_node_get", description="Read a node by PK.", annotations=_READ_ONLY)
    async def node_get(pk: int) -> dict[str, Any]: return await service.node_get(pk)
    @server.tool(name="aiida_node_links", description="Read direct AiiDA links for a node UUID or PK.", annotations=_READ_ONLY)
    async def node_links(identifier: str) -> dict[str, Any]: return await service.node_links(identifier)
    @server.tool(name="aiida_node_compare", description="Compare two AiiDA nodes recursively by attributes, ports, links, arrays, and repository content.", annotations=_READ_ONLY)
    async def node_compare(left: str, right: str, depth: int = 2, max_differences: int = 100) -> dict[str, Any]: return await service.node_compare(left, right, depth=depth, max_differences=max_differences)
    @server.tool(name="aiida_process_get", description="Read a process by identifier.", annotations=_READ_ONLY)
    async def process_get(identifier: str) -> dict[str, Any]: return await service.process_get(identifier)
    @server.tool(name="aiida_process_workgraph", description="Read a process call graph. This is a graph, not a simplified tree.", annotations=_READ_ONLY)
    async def process_workgraph(identifier: str) -> dict[str, Any]: return await service.process_workgraph(identifier)
    @server.tool(name="aiida_process_logs", description="Read process logs by identifier.", annotations=_READ_ONLY)
    async def process_logs(identifier: str) -> dict[str, Any]: return await service.process_logs(identifier)
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
    return server
