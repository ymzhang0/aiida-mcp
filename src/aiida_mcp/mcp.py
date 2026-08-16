"""Safe ChatGPT plugin tool surface for a project-scoped AiiDA service."""

from __future__ import annotations

from typing import Any, Literal

from fastmcp import FastMCP
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .service import AiiDAService


_READ_ONLY = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": False,
}
_PREVIEW = {**_READ_ONLY, "readOnlyHint": False}


class SubmissionPreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["single", "batch"]
    builder_strategy: Literal["protocol", "explicit_inputs"] = "protocol"
    workchain: str = Field(min_length=1)
    code: str | None = Field(default=None, min_length=1)
    protocol: str = Field(default="moderate", min_length=1)
    structure_pk: int | None = Field(default=None, gt=0)
    structure_pks: list[int] = Field(default_factory=list)
    overrides: dict[str, Any] = Field(default_factory=dict)
    protocol_kwargs: dict[str, Any] = Field(default_factory=dict)
    parameter_grid: dict[str, Any] = Field(default_factory=dict)
    matrix_mode: Literal["product", "zip"] = "product"
    inputs: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_topology(self) -> "SubmissionPreviewRequest":
        if self.builder_strategy == "explicit_inputs":
            if not self.inputs:
                raise ValueError("explicit_inputs strategy requires inputs")
            return self
        if not self.code:
            raise ValueError("protocol strategy requires code")
        if self.mode == "single" and self.structure_pk is None:
            raise ValueError("single mode requires structure_pk")
        if self.mode == "batch" and not self.structure_pks:
            raise ValueError("batch mode requires structure_pks")
        return self


def build_mcp_server(service: AiiDAService) -> FastMCP:
    server = FastMCP(
        "aiida",
        instructions=(
            "AiiDA scientific research plugin. Always use an explicit project_ref. "
            "Read project data first, then create and validate structured submission previews. "
            "This plugin never launches calculations."
        ),
    )

    @server.tool(name="aiida_list_projects", description="List AiiDA projects available to this plugin.", annotations=_READ_ONLY)
    async def list_projects() -> dict[str, Any]:
        return {"projects": service.list_projects()}

    @server.tool(name="aiida_project_status", description="Read the selected AiiDA project's worker and profile status.", annotations=_READ_ONLY)
    async def project_status(project_ref: str) -> dict[str, Any]:
        return await service.status(project_ref)

    @server.tool(name="aiida_recent_processes", description="List recent AiiDA processes belonging to one project.", annotations=_READ_ONLY)
    async def recent_processes(project_ref: str, limit: int = 50) -> dict[str, Any]:
        return await service.recent_processes(project_ref, limit=limit)

    @server.tool(name="aiida_recent_nodes", description="List recent AiiDA nodes belonging to one project.", annotations=_READ_ONLY)
    async def recent_nodes(project_ref: str, limit: int = 50, node_type: str | None = None) -> dict[str, Any]:
        return await service.recent_nodes(project_ref, limit=limit, node_type=node_type)

    @server.tool(name="aiida_inspect_process", description="Inspect a process that belongs to the explicit AiiDA project.", annotations=_READ_ONLY)
    async def inspect_process(project_ref: str, identifier: str) -> dict[str, Any]:
        return await service.inspect_process(project_ref, identifier)

    @server.tool(name="aiida_resources", description="Read configured AiiDA computers and codes.", annotations=_READ_ONLY)
    async def resources(project_ref: str) -> dict[str, Any]:
        return await service.resources(project_ref)

    @server.tool(name="aiida_workflow_catalog", description="List WorkChains installed for the project runtime.", annotations=_READ_ONLY)
    async def workflow_catalog(project_ref: str) -> dict[str, Any]:
        return await service.workflow_catalog(project_ref)

    @server.tool(name="aiida_input_candidates", description="Find stored inputs compatible with a WorkChain port.", annotations=_READ_ONLY)
    async def input_candidates(project_ref: str, workchain: str, port_path: str, limit: int = 50) -> dict[str, Any]:
        return await service.input_candidates(project_ref, workchain=workchain, port_path=port_path, limit=limit)

    @server.tool(name="aiida_submission_spec", description="Read structured inputs for one WorkChain.", annotations=_READ_ONLY)
    async def submission_spec(project_ref: str, workchain: str) -> dict[str, Any]:
        return await service.submission_spec(project_ref, workchain=workchain)

    @server.tool(name="aiida_build_submission_preview", description="Build but never submit a structured AiiDA calculation preview.", annotations=_PREVIEW)
    async def build_submission_preview(project_ref: str, request: SubmissionPreviewRequest) -> dict[str, Any]:
        return await service.build_submission_preview(project_ref, request.model_dump(mode="json"))

    @server.tool(name="aiida_validate_submission_preview", description="Validate a preview without launching a calculation.", annotations=_PREVIEW)
    async def validate_submission_preview(project_ref: str, draft: dict[str, Any]) -> dict[str, Any]:
        return await service.validate_submission_preview(project_ref, draft)

    return server


__all__ = ["SubmissionPreviewRequest", "build_mcp_server"]
