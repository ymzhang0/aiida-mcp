from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from aiida_mcp.api import build_manager_router
from aiida_mcp.registry import ProjectRegistry


class _Service:
    def __init__(self, registry):
        self.registry = registry

    def list_projects(self):
        return [project.public_dict() for project in self.registry.list_projects()]

    async def status(self, project_ref):
        return {"project_ref": project_ref, "status": "online"}

    async def recent_processes(self, project_ref, *, limit=50):
        return {"project_ref": project_ref, "items": []}


def test_manager_api_creates_a_project_and_returns_only_public_fields(tmp_path) -> None:
    registry = ProjectRegistry(tmp_path / "projects.json")
    app = FastAPI()
    app.include_router(build_manager_router(registry, _Service(registry)))

    with TestClient(app) as client:
        response = client.post(
            "/api/projects",
            json={
                "name": "Vibe research",
                "aiida_profile": "research",
                "python_interpreter_path": "/opt/research/bin/python",
                "group_uuid": "67b39b4d-9684-4d4e-ae85-123456789abc",
                "group_label": "vibe-research",
            },
        )

    assert response.status_code == 200
    assert response.json()["project_ref"].startswith("sp_")
    assert "python_interpreter_path" not in response.json()
