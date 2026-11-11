from __future__ import annotations
from fastapi.testclient import TestClient
import pytest
from aiida_mcp.server import access_error, create_app, require_token_for_host
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
    with TestClient(app, base_url="http://127.0.0.1:8043") as client:
        response = client.post("/mcp/", json=request, headers={"accept": "application/json, text/event-stream"})
    assert response.status_code == 200
    assert "aiida" in response.text


def test_without_a_token_only_loopback_hosts_are_served(monkeypatch) -> None:
    monkeypatch.delenv("AIIDA_MCP_TOKEN", raising=False)
    app = create_app(runtime=RuntimeConfig("/bin/python", "test"), workers=_Workers())
    with TestClient(app, base_url="http://my-tunnel.example.com") as client:
        assert client.get("/api/ready").status_code == 200
        assert client.get("/api/groups").status_code == 403
        assert client.post("/mcp/", json={}).status_code == 403


def test_token_protects_api_as_well_as_mcp(monkeypatch) -> None:
    monkeypatch.setenv("AIIDA_MCP_TOKEN", "secret")
    app = create_app(runtime=RuntimeConfig("/bin/python", "test"), workers=_Workers())
    with TestClient(app, base_url="http://127.0.0.1:8043") as client:
        assert client.get("/api/ready").status_code == 200
        assert client.get("/api/groups").status_code == 401
        assert client.post("/mcp/", json={}, headers={"authorization": "Bearer wrong"}).status_code == 401


def test_access_rules() -> None:
    allowed = {"token": "", "path": "/api/groups", "authorization": ""}
    assert access_error(host_header="localhost:8043", **allowed) is None
    assert access_error(host_header="[::1]:8043", **allowed) is None
    assert access_error(host_header="evil.example.com", **allowed) == (403, "Set AIIDA_MCP_TOKEN to reach this server through anything but localhost")
    assert access_error(token="secret", path="/mcp/", host_header="tunnel.example.com", authorization="Bearer secret") is None
    assert access_error(token="secret", path="/mcp/", host_header="127.0.0.1", authorization="")[0] == 401


def test_public_bind_needs_a_token(monkeypatch) -> None:
    monkeypatch.delenv("AIIDA_MCP_TOKEN", raising=False)
    require_token_for_host("127.0.0.1")
    with pytest.raises(SystemExit):
        require_token_for_host("0.0.0.0")
    monkeypatch.setenv("AIIDA_MCP_TOKEN", "secret")
    require_token_for_host("0.0.0.0")
