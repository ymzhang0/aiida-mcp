"""ARIS's fixed-project MCP HTTP entry point."""
from __future__ import annotations

import argparse
import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI
from fastmcp import FastMCP

from .runtime import Runtime, WorkerRuntimePool
from .mcp import build_mcp_server as build_generic_mcp_server
from .service import AiiDAService, RuntimeConfig
from .worker.service import AiiDAWorkerService


_READ_ONLY = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False}

_WRITE_CAPABLE = {"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": False}
_WORKER_METHODS = frozenset(AiiDAWorkerService().methods)

@dataclass(frozen=True)
class Project:
    project_id: str
    name: str
    python_interpreter_path: str
    aiida_profile: str
    workspace_path: str | None

    def public(self) -> dict[str, str | None]:
        return {"project_id": self.project_id, "name": self.name, "python_interpreter_path": self.python_interpreter_path, "aiida_profile": self.aiida_profile, "workspace_path": self.workspace_path}


def _projects_file(path: str | None = None) -> Path:
    return Path(path or os.environ.get("ARIS_AIIDA_PROJECTS_FILE") or Path.home() / ".aiida" / "aiida-mcp-projects.json").expanduser()


def load_projects(path: str | None = None) -> list[Project]:
    try:
        payload = json.loads(_projects_file(path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError("ARIS project registry is unavailable") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("ARIS project registry is invalid") from exc
    entries = payload.get("projects", []) if isinstance(payload, dict) else []
    projects = []
    for item in entries:
        if not isinstance(item, dict) or item.get("archived"):
            continue
        project_id = str(item.get("id") or "").strip()
        interpreter = str(item.get("python_interpreter_path") or "").strip()
        profile = str(item.get("aiida_profile") or "").strip()
        if project_id and interpreter and profile:
            workspace = str(item.get("workspace_path") or "").strip() or None
            projects.append(Project(project_id, str(item.get("name") or project_id), interpreter, profile, workspace))
    return sorted(projects, key=lambda project: (project.name.lower(), project.project_id))


def select_project(project_id: str, projects: list[Project]) -> Project:
    selected = str(project_id or "").strip()
    for project in projects:
        if project.project_id == selected:
            interpreter = Path(project.python_interpreter_path).expanduser()
            if not interpreter.is_file() or not os.access(interpreter, os.X_OK):
                raise ValueError("Selected project's Python interpreter is unavailable")
            return project
    raise ValueError("Selected ARIS project is unavailable")


class ArisAiiDAService:
    def __init__(self, project: Project, workers: WorkerRuntimePool) -> None:
        self.project = project
        self.workers = workers

    def runtime(self) -> Runtime:
        return Runtime(f"aris-project:{self.project.project_id}", self.project.python_interpreter_path, self.project.aiida_profile, self.project.workspace_path)

    async def recent_processes(self, limit: int = 15) -> dict[str, Any]:
        return await self.workers.request(self.runtime(), "process.recent", {"limit": max(1, min(int(limit), 200)), "root_only": False})

    async def worker_call(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        selected_method = str(method or "").strip()
        if selected_method not in _WORKER_METHODS:
            raise ValueError("Unknown worker RPC method")
        return await self.workers.request(self.runtime(), selected_method, dict(params or {}))


def build_mcp_server(projects: list[Project], selected: Project, service: ArisAiiDAService) -> FastMCP:
    server = FastMCP("aris-aiida", instructions="AiiDA access for the single ARIS project selected at server startup. Generic read-only tools and every registered worker RPC are available through this MCP server.")
    server.mount(build_generic_mcp_server(AiiDAService(RuntimeConfig(selected.python_interpreter_path, selected.aiida_profile, selected.workspace_path), service.workers)))

    @server.tool(name="aiida_list_projects", description="List the local ARIS projects available for read-only query.", annotations=_READ_ONLY)
    async def list_projects() -> dict[str, list[dict[str, str | None]]]:
        return {"items": [project.public() for project in projects]}

    @server.tool(name="aiida_select_project", description="Confirm the single ARIS project configured for this read-only server.", annotations=_READ_ONLY)
    async def confirm_project(project_id: str) -> dict[str, str | None]:
        if project_id != selected.project_id:
            raise ValueError("This read-only server is fixed to its configured ARIS project")
        return selected.public()

    @server.tool(name="aiida_recent_processes", description="Read recent AiiDA processes from the configured ARIS project.", annotations=_READ_ONLY)
    async def recent_processes(limit: int = 15) -> dict[str, Any]:
        return await service.recent_processes(limit)

    @server.tool(name="aiida_worker_methods", description="List every registered AiiDA worker JSON-RPC method available to this project.", annotations=_READ_ONLY)
    async def worker_methods() -> dict[str, list[str]]:
        return {"methods": sorted(_WORKER_METHODS)}

    @server.tool(name="aiida_worker_call", description="Call any registered AiiDA worker JSON-RPC method for the configured project, including write, submission, and execution operations.", annotations=_WRITE_CAPABLE)
    async def worker_call(method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.worker_call(method, params)

    return server


def create_app(*, project_id: str, projects_file: str | None = None) -> FastAPI:
    projects = load_projects(projects_file)
    selected = select_project(project_id, projects)
    workers = WorkerRuntimePool()
    service = ArisAiiDAService(selected, workers)
    mcp = build_mcp_server(projects, selected, service)
    mcp_app = mcp.http_app(path="/", transport="streamable-http")

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            await workers.stop()

    app = FastAPI(title="ARIS AiiDA MCP", version="0.1.0", lifespan=mcp_app.lifespan)
    app.mount("/mcp", mcp_app)
    return app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="aris-aiida-mcp-http")
    parser.add_argument("serve", nargs="?")
    parser.add_argument("--project-id", default=os.environ.get("ARIS_AIIDA_PROJECT_ID"))
    parser.add_argument("--projects-file", default=os.environ.get("ARIS_AIIDA_PROJECTS_FILE"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("ARIS_AIIDA_MCP_PORT", "8000")))
    args = parser.parse_args(argv)
    if not args.project_id:
        parser.error("--project-id or ARIS_AIIDA_PROJECT_ID is required")
    uvicorn.run(create_app(project_id=args.project_id, projects_file=args.projects_file), host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
