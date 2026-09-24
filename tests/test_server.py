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
