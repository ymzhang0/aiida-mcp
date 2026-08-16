"""Local control-plane API consumed by the AiiDA Manager."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .registry import ProjectRegistry
from .service import AiiDAService, ProjectScopeError
from .runtime import WorkerRuntimeError


class ProjectCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    aiida_profile: str = Field(min_length=1, max_length=120)
    python_interpreter_path: str = Field(min_length=1, max_length=500)
    group_uuid: str = Field(min_length=1, max_length=120)
    group_label: str = Field(default="", max_length=240)
    workspace_path: str | None = Field(default=None, max_length=1000)
    database_fingerprint: str | None = Field(default=None, max_length=240)
    chatgpt_external_project_id: str | None = Field(default=None, max_length=240)


def _error(error: Exception) -> HTTPException:
    if isinstance(error, KeyError):
        return HTTPException(status_code=404, detail="AiiDA project not found")
    if isinstance(error, ProjectScopeError):
        return HTTPException(status_code=404, detail=str(error))
    if isinstance(error, WorkerRuntimeError):
        return HTTPException(status_code=error.status_code, detail={"message": str(error), **error.detail})
    if isinstance(error, ValueError):
        return HTTPException(status_code=422, detail=str(error))
    return HTTPException(status_code=500, detail=str(error))


def build_manager_router(registry: ProjectRegistry, service: AiiDAService) -> APIRouter:
    router = APIRouter(prefix="/api", tags=["aiida-manager"])

    @router.get("/projects")
    async def list_projects() -> dict[str, Any]:
        return {"items": service.list_projects()}

    @router.post("/projects")
    async def create_project(payload: ProjectCreateRequest) -> dict[str, Any]:
        try:
            project = registry.create(**payload.model_dump())
            return project.public_dict()
        except Exception as exc:  # noqa: BLE001
            raise _error(exc) from exc

    @router.delete("/projects/{project_ref}")
    async def archive_project(project_ref: str) -> dict[str, Any]:
        try:
            return registry.archive(project_ref).public_dict()
        except Exception as exc:  # noqa: BLE001
            raise _error(exc) from exc

    @router.get("/projects/{project_ref}/status")
    async def project_status(project_ref: str) -> dict[str, Any]:
        try:
            return await service.status(project_ref)
        except Exception as exc:  # noqa: BLE001
            raise _error(exc) from exc

    @router.get("/projects/{project_ref}/processes")
    async def project_processes(project_ref: str, limit: int = 50) -> dict[str, Any]:
        try:
            return await service.recent_processes(project_ref, limit=limit)
        except Exception as exc:  # noqa: BLE001
            raise _error(exc) from exc

    @router.get("/projects/{project_ref}/nodes")
    async def project_nodes(project_ref: str, limit: int = 50, node_type: str | None = None) -> dict[str, Any]:
        try:
            return await service.recent_nodes(project_ref, limit=limit, node_type=node_type)
        except Exception as exc:  # noqa: BLE001
            raise _error(exc) from exc

    @router.get("/projects/{project_ref}/resources")
    async def project_resources(project_ref: str) -> dict[str, Any]:
        try:
            return await service.resources(project_ref)
        except Exception as exc:  # noqa: BLE001
            raise _error(exc) from exc

    @router.get("/projects/{project_ref}/processes/{identifier}")
    async def project_process(project_ref: str, identifier: str) -> dict[str, Any]:
        try:
            return await service.inspect_process(project_ref, identifier)
        except Exception as exc:  # noqa: BLE001
            raise _error(exc) from exc

    return router


__all__ = ["ProjectCreateRequest", "build_manager_router"]
