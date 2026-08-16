from __future__ import annotations

from aiida_mcp.registry import ProjectRegistry


def test_registry_creates_opaque_ref_and_hides_runtime_details(tmp_path) -> None:
    registry = ProjectRegistry(tmp_path / "projects.json")

    project = registry.create(
        name="Silicon study",
        aiida_profile="research",
        python_interpreter_path="/opt/research/bin/python",
        group_uuid="9d2ef8db-875b-440e-b9d1-123456789abc",
        group_label="silicon-study",
    )

    assert project.project_ref.startswith("sp_")
    assert registry.get(project.project_ref) == project
    public = registry.list_projects()[0].public_dict()
    assert public["group_uuid"] == project.group_uuid
    assert "python_interpreter_path" not in public
    assert "workspace_path" not in public


def test_registry_rejects_duplicate_profile_group_binding(tmp_path) -> None:
    registry = ProjectRegistry(tmp_path / "projects.json")
    kwargs = {
        "name": "Study",
        "aiida_profile": "research",
        "python_interpreter_path": "/usr/bin/python",
        "group_uuid": "group-uuid",
    }
    registry.create(**kwargs)

    try:
        registry.create(**kwargs)
    except ValueError as exc:
        assert "already bound" in str(exc)
    else:
        raise AssertionError("duplicate Group binding was accepted")
