from __future__ import annotations

import pytest

from aiida_mcp.registry import ProjectRegistry
from aiida_mcp.service import AiiDAService, ProjectScopeError


class _Workers:
    def __init__(self, *, member: bool):
        self.member = member
        self.calls = []

    async def request(self, runtime, method, params=None):
        self.calls.append((runtime, method, params or {}))
        if method == "group.contains_node":
            return {"member": self.member}
        if method == "process.detail":
            return {"pk": int((params or {})["identifier"])}
        raise AssertionError(method)


def _service(tmp_path, *, member: bool) -> tuple[AiiDAService, _Workers, str]:
    registry = ProjectRegistry(tmp_path / "projects.json")
    project = registry.create(
        name="Large project",
        aiida_profile="research",
        python_interpreter_path="/opt/research/bin/python",
        group_uuid="67b39b4d-9684-4d4e-ae85-123456789abc",
    )
    workers = _Workers(member=member)
    return AiiDAService(registry, workers), workers, project.project_ref


@pytest.mark.anyio
async def test_process_membership_uses_exact_worker_query(tmp_path) -> None:
    service, workers, project_ref = _service(tmp_path, member=True)

    result = await service.inspect_process(project_ref, "1625")

    assert result == {"pk": 1625}
    assert workers.calls[0][1:] == (
        "group.contains_node",
        {
            "group_uuid": "67b39b4d-9684-4d4e-ae85-123456789abc",
            "identifier": "1625",
        },
    )


@pytest.mark.anyio
async def test_process_outside_project_is_rejected(tmp_path) -> None:
    service, _, project_ref = _service(tmp_path, member=False)

    with pytest.raises(ProjectScopeError):
        await service.inspect_process(project_ref, "1625")
