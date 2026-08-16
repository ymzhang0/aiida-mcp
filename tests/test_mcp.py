from __future__ import annotations

import pytest

from aiida_mcp.mcp import build_mcp_server


class _Service:
    def list_projects(self):
        return [{"project_ref": "sp_test", "name": "Test project"}]

    async def status(self, project_ref):
        return {"project_ref": project_ref, "status": "online"}

    async def recent_processes(self, project_ref, *, limit=50):
        return {"items": [{"pk": 7}], "project_ref": project_ref, "limit": limit}

    async def recent_nodes(self, project_ref, *, limit=50, node_type=None):
        return {"items": [], "project_ref": project_ref, "node_type": node_type}

    async def inspect_process(self, project_ref, identifier):
        return {"pk": int(identifier), "project_ref": project_ref}

    async def resources(self, project_ref):
        return {"project_ref": project_ref, "computers": []}

    async def workflow_catalog(self, project_ref):
        return {"project_ref": project_ref, "workflows": []}

    async def input_candidates(self, project_ref, *, workchain, port_path, limit=50):
        return {"project_ref": project_ref, "entry_point": workchain, "port_path": port_path}

    async def submission_spec(self, project_ref, *, workchain):
        return {"project_ref": project_ref, "entry_point": workchain, "inputs": {}}

    async def build_submission_preview(self, project_ref, request):
        return {"project_ref": project_ref, "request": request}

    async def validate_submission_preview(self, project_ref, draft):
        return {"project_ref": project_ref, "valid": True}


@pytest.mark.anyio
async def test_mcp_requires_project_ref_for_scoped_tools() -> None:
    server = build_mcp_server(_Service())
    tools = {tool.name: tool for tool in await server.list_tools()}

    assert "aiida_submit" not in tools
    assert "project_ref" in tools["aiida_recent_processes"].parameters["properties"]
    assert tools["aiida_recent_processes"].annotations.readOnlyHint is True

    result = await server.call_tool("aiida_recent_processes", {"project_ref": "sp_test"})
    assert result.structured_content["project_ref"] == "sp_test"
