from __future__ import annotations

import json

import pytest

from aiida_mcp.aris import load_projects, select_project


def test_registry_exposes_only_complete_active_projects(tmp_path):
    python = tmp_path / "python"
    python.write_text("", encoding="utf-8")
    python.chmod(0o755)
    registry = tmp_path / "projects.json"
    registry.write_text(json.dumps({"projects": [
        {"id": "active", "name": "Active", "python_interpreter_path": str(python), "aiida_profile": "hh"},
        {"id": "archived", "python_interpreter_path": str(python), "aiida_profile": "old", "archived": True},
        {"id": "incomplete", "aiida_profile": "missing"},
    ]}), encoding="utf-8")

    projects = load_projects(str(registry))

    assert [project.project_id for project in projects] == ["active"]
    assert select_project("active", projects).aiida_profile == "hh"
    with pytest.raises(ValueError):
        select_project("other", projects)
