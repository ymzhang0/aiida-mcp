from __future__ import annotations
import pytest
from aiida_mcp.mcp import build_mcp_server
class _Service:
    async def status(self): return {"status": "online"}
    async def nodes(self, **_): return {"items": []}
    async def node_get(self, pk): return {"pk": pk}
    async def node_links(self, identifier): return {"identifier": identifier, "links": []}
    async def node_compare(self, left, right, **_): return {"left": left, "right": right}
    async def process_get(self, identifier): return {"identifier": identifier}
    async def process_workgraph(self, identifier): return {"identifier": identifier, "nodes": [], "links": []}
    async def process_logs(self, identifier): return {"identifier": identifier}
    async def group_get(self, identifier): return {"identifier": identifier}
    async def group_list(self): return {"items": []}
    async def provenance_walk(self, root_uuid, **_): return {"root_uuid": root_uuid, "nodes": [], "links": []}
    async def provenance_roots(self, group_uuid, **_): return {"group_uuid": group_uuid}
    async def provenance_subgraph(self, group_uuid, root_uuid, **_): return {"group_uuid": group_uuid, "root_uuid": root_uuid}
    async def archive_inspect(self, path): return {"path": path}
@pytest.mark.anyio
async def test_mcp_tools_do_not_accept_project_ref() -> None:
    server = build_mcp_server(_Service())
    tools = {tool.name: tool for tool in await server.list_tools()}
    assert "project_ref" not in tools["list_nodes"].parameters["properties"]
    assert tools["list_nodes"].annotations.readOnlyHint is True
    assert {"get_node_links", "compare_nodes", "get_workgraph", "get_ancestors", "get_descendants", "inspect_archive"} <= set(tools)
    assert "get_process_tree" not in tools


def test_internal_worker_has_no_project_binding_methods() -> None:
    from aiida_mcp.worker.service import AiiDAWorkerService

    methods = set(AiiDAWorkerService().methods)
    assert {"node.links", "node.compare", "process.workgraph", "provenance.walk", "archive.inspect"} <= methods
    assert "process.tree" not in methods
    assert "group.ensure_project" not in methods
