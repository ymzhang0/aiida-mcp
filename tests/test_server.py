from __future__ import annotations

from fastapi.testclient import TestClient

from aiida_mcp.registry import ProjectRegistry
from aiida_mcp.server import create_app


class _Workers:
    async def stop(self):
        return None


def test_mcp_route_requires_configured_bearer_token(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AIIDA_MCP_TOKEN", "test-token")
    app = create_app(registry=ProjectRegistry(tmp_path / "projects.json"), workers=_Workers())

    with TestClient(app) as client:
        response = client.get("/mcp")

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid MCP token"}
