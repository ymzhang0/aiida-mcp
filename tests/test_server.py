from __future__ import annotations
from fastapi.testclient import TestClient
from aiida_mcp.server import create_app
from aiida_mcp.service import RuntimeConfig
class _Workers:
    async def stop(self): return None

def test_generic_api_has_no_manager_page() -> None:
    app = create_app(runtime=RuntimeConfig("/bin/python", "test"), workers=_Workers())
    with TestClient(app) as client:
        assert client.get("/").status_code == 404
        assert client.get("/api/ready").json() == {"status": "ready"}


def test_mcp_endpoint_accepts_a_session() -> None:
    """The mounted MCP app needs its lifespan; without it every request is a 500."""
    app = create_app(runtime=RuntimeConfig("/bin/python", "test"), workers=_Workers())
    request = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "test", "version": "0"}}}
    with TestClient(app) as client:
        response = client.post("/mcp/", json=request, headers={"accept": "application/json, text/event-stream"})
    assert response.status_code == 200
    assert "aiida" in response.text
