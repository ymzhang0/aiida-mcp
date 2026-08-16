"""Project-scoped AiiDA operations shared by the MCP plugin and Manager."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .registry import AiiDAProject, ProjectRegistry
from .runtime import ProjectRuntime, WorkerRuntimePool


class ProjectScopeError(ValueError):
    pass


class AiiDAService:
    """Resolve a project ref once, then call only its AiiDA runtime."""

    def __init__(self, registry: ProjectRegistry, workers: WorkerRuntimePool) -> None:
        self.registry = registry
        self.workers = workers

    def list_projects(self) -> list[dict[str, Any]]:
        return [project.public_dict() for project in self.registry.list_projects()]

    def project(self, project_ref: str) -> AiiDAProject:
        project = self.registry.get(project_ref)
        if project is None or project.archived:
            raise ProjectScopeError("Unknown or archived AiiDA project")
        return project

    async def status(self, project_ref: str) -> dict[str, Any]:
        project = self.project(project_ref)
        result = await self._call(project, "runtime.status")
        return {**result, "project": project.public_dict()}

    async def profiles(self, project_ref: str) -> dict[str, Any]:
        return await self._call(self.project(project_ref), "profile.list")

    async def resources(self, project_ref: str) -> dict[str, Any]:
        return await self._call(self.project(project_ref), "resource.summary")

    async def system_info(self, project_ref: str) -> dict[str, Any]:
        return await self._call(self.project(project_ref), "system.info")

    async def database_summary(self, project_ref: str) -> dict[str, Any]:
        return await self._call(self.project(project_ref), "system.database_summary")

    async def group_inspect(self, project_ref: str, *, limit: int = 500) -> dict[str, Any]:
        project = self.project(project_ref)
        return await self._call(
            project, "group.inspect_uuid", {"group_uuid": project.group_uuid, "limit": max(1, min(int(limit), 500))}
        )

    async def node_detail(self, project_ref: str, identifier: str) -> dict[str, Any]:
        project = self.project(project_ref)
        await self._assert_group_member(project, identifier)
        return await self._call(project, "node.summary", {"pk": int(identifier)})

    async def process_logs(self, project_ref: str, identifier: str) -> dict[str, Any]:
        project = self.project(project_ref)
        await self._assert_group_member(project, identifier)
        return await self._call(project, "process.logs", {"identifier": str(identifier)})

    async def process_workgraph(self, project_ref: str, identifier: str) -> dict[str, Any]:
        project = self.project(project_ref)
        await self._assert_group_member(project, identifier)
        return await self._call(project, "process.workgraph", {"identifier": str(identifier)})
    async def recent_nodes(
        self,
        project_ref: str,
        *,
        limit: int = 50,
        node_type: str | None = None,
    ) -> dict[str, Any]:
        project = self.project(project_ref)
        payload: dict[str, Any] = {
            "limit": max(1, min(int(limit), 200)),
            "group_uuid": project.group_uuid,
            "root_only": False,
        }
        if node_type:
            payload["node_type"] = str(node_type)
        return await self._call(project, "node.recent", payload)

    async def recent_processes(self, project_ref: str, *, limit: int = 50) -> dict[str, Any]:
        return await self.recent_nodes(project_ref, limit=limit, node_type="ProcessNode")

    async def inspect_process(self, project_ref: str, identifier: str) -> dict[str, Any]:
        project = self.project(project_ref)
        cleaned = str(identifier or "").strip()
        if not cleaned:
            raise ValueError("Process identifier is required")
        await self._assert_group_member(project, cleaned)
        return await self._call(project, "process.detail", {"identifier": cleaned})

    async def workflow_catalog(self, project_ref: str) -> dict[str, Any]:
        return await self._call(self.project(project_ref), "workflow.catalog")

    async def input_candidates(
        self, project_ref: str, *, workchain: str, port_path: str, limit: int = 50
    ) -> dict[str, Any]:
        return await self._call(
            self.project(project_ref),
            "workflow.input_candidates",
            {"entry_point": workchain, "port_path": port_path, "limit": max(1, min(int(limit), 200))},
        )

    async def submission_spec(self, project_ref: str, *, workchain: str) -> dict[str, Any]:
        return await self._call(self.project(project_ref), "submission.spec", {"entry_point": workchain})

    async def build_submission_preview(self, project_ref: str, request: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(request, dict) or not request:
            raise ValueError("Structured submission request is required")
        return await self._call(self.project(project_ref), "submission.builder_draft", request)

    async def validate_submission_preview(self, project_ref: str, draft: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(draft, dict) or not draft:
            raise ValueError("Submission draft is required")
        return await self._call(self.project(project_ref), "submission.validate", {"draft": draft})

    async def _assert_group_member(self, project: AiiDAProject, identifier: str) -> None:
        members = await self._call(project, "group.inspect_uuid", {"group_uuid": project.group_uuid, "limit": 500})
        nodes = members.get("nodes") if isinstance(members, dict) else []
        allowed = {
            str(item.get(field))
            for item in nodes if isinstance(item, dict)
            for field in ("pk", "uuid") if item.get(field) is not None
        }
        if str(identifier) not in allowed:
            raise ProjectScopeError("Record is not a member of this AiiDA project")
    async def _call(self, project: AiiDAProject, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        runtime = ProjectRuntime(
            project_id=project.id,
            python_interpreter_path=project.python_interpreter_path,
            aiida_profile=project.aiida_profile,
            workspace_path=project.workspace_path,
        )
        return await self.workers.request(runtime, method, params)


__all__ = ["AiiDAService", "ProjectScopeError"]
