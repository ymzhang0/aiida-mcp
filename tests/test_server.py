from __future__ import annotations

from fastapi.testclient import TestClient

from aiida_mcp.registry import ProjectRegistry
from aiida_mcp import server
from aiida_mcp.server import create_app, default_worker_source


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


def test_default_worker_source_resolves_the_bundled_workspace_worker(monkeypatch) -> None:
    monkeypatch.delenv("AIIDA_MCP_WORKER_SOURCE", raising=False)
    assert default_worker_source().name == "aiida-worker"
    assert (default_worker_source() / "src" / "aris_aiida_worker").is_dir()

def test_manager_root_serves_the_chatgpt_style_desktop_layout(tmp_path) -> None:
    app = create_app(registry=ProjectRegistry(tmp_path / "projects.json"), workers=_Workers())

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'class="sidebar"' in response.text
    assert 'id="inspector"' in response.text
    assert 'Recent activity' in response.text

def test_stdio_transport_runs_the_plugin_server(monkeypatch) -> None:
    calls: list[str] = []

    class _StdioMcp:
        def run(self, *, transport: str) -> None:
            calls.append(transport)

    monkeypatch.setenv("AIIDA_MCP_TRANSPORT", "stdio")
    monkeypatch.setattr(server, "ProjectRegistry", lambda: object())
    monkeypatch.setattr(server, "WorkerRuntimePool", lambda **_: object())
    monkeypatch.setattr(server, "build_mcp_server", lambda _: _StdioMcp())

    server.main()

    assert calls == ["stdio"]