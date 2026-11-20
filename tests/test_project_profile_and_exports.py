from __future__ import annotations

import json

import pytest

from aiida_mcp.mcp import build_mcp_server
from aiida_mcp.server import runtime_config
from aiida_mcp.service import EXPORT_TIMEOUT, AiiDAService, RuntimeConfig


def _project(root, profile="hh", python="/project/bin/python"):
    root.mkdir(parents=True, exist_ok=True)
    (root / "project.yaml").write_text(f"project:\n  id: demo\nruntime:\n  aiida_profile: {profile}\n  python_interpreter: {python}\n")


@pytest.fixture(autouse=True)
def _clean_environment(monkeypatch, tmp_path):
    for name in ("AIIDA_MCP_PROFILE", "AIIDA_MCP_PYTHON", "AIIDA_MCP_WORKSPACE", "AIIDA_MCP_AIIDA_PATH", "AIIDA_PATH"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))


def test_profile_comes_from_the_project_the_server_starts_in(monkeypatch, tmp_path) -> None:
    project = tmp_path / "hh"
    _project(project)
    (project / "notebooks" / "deep").mkdir(parents=True)
    monkeypatch.chdir(project / "notebooks" / "deep")
    monkeypatch.setenv("AIIDA_MCP_PROFILE", "test")  # a preset profile must not beat the project
    monkeypatch.setenv("AIIDA_MCP_PYTHON", "/env/bin/python")
    runtime = runtime_config()
    assert (runtime.aiida_profile, runtime.python_interpreter_path) == ("hh", "/project/bin/python")
    assert runtime.workspace_path == str(project.resolve())
    assert runtime.source == str(project.resolve() / "project.yaml")
    assert runtime_config(profile="other").aiida_profile == "other"


def test_outside_a_project_the_environment_then_the_default_profile_is_used(monkeypatch, tmp_path) -> None:
    (tmp_path / "plain").mkdir()
    (tmp_path / "plain" / "project.yaml").write_text("name: not an aiida project\n")
    monkeypatch.chdir(tmp_path / "plain")
    monkeypatch.setenv("AIIDA_MCP_PYTHON", "/env/bin/python")
    with pytest.raises(ValueError, match="No AiiDA profile"):
        runtime_config()
    config = tmp_path / "home" / ".aiida"
    config.mkdir(parents=True)
    (config / "config.json").write_text(json.dumps({"default_profile": "main"}))
    runtime = runtime_config()
    assert runtime.aiida_profile == "main" and runtime.source.startswith("default profile")
    assert runtime.workspace_path == str((tmp_path / "plain").resolve())
    monkeypatch.setenv("AIIDA_MCP_PROFILE", "preset")
    assert runtime_config().aiida_profile == "preset"


def test_workspace_option_selects_the_project(monkeypatch, tmp_path) -> None:
    _project(tmp_path / "a", profile="alpha")
    monkeypatch.chdir(tmp_path)
    assert runtime_config(workspace=str(tmp_path / "a")).aiida_profile == "alpha"


class _Workers:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def request(self, runtime, method, params=None, **kwargs):
        self.calls.append((method, dict(params or {}), kwargs))
        return {"profile": runtime.aiida_profile}


@pytest.mark.anyio
async def test_export_tools_exist_on_read_only_servers_and_get_a_long_timeout() -> None:
    workers = _Workers()
    service = AiiDAService(RuntimeConfig("/bin/python", "hh", "/work", "/work/project.yaml"), workers)
    server = build_mcp_server(service, allow_write=False)
    tools = {tool.name: tool for tool in await server.list_tools()}
    assert {"export_archive", "dump_files"} <= set(tools)
    assert tools["dump_files"].annotations.readOnlyHint is False
    await service.archive_export({"path": "out.aiida", "groups": ["g"]})
    await service.files_dump({"path": "dump", "group": "g"})
    assert [(call[0], call[2]) for call in workers.calls] == [("archive.export", {"timeout": EXPORT_TIMEOUT}), ("files.dump", {"timeout": EXPORT_TIMEOUT})]
    assert await service.status() == {"profile": "hh", "profile_source": "/work/project.yaml", "workspace": "/work"}


def test_export_paths_stay_inside_the_workspace(tmp_path) -> None:
    pytest.importorskip("aiida")
    from aiida_mcp.worker.application.export_service import workspace_target
    from aiida_mcp.worker.protocol.errors import WorkerDomainError

    assert workspace_target("out/a.aiida", str(tmp_path))[1] == tmp_path.resolve() / "out" / "a.aiida"
    outside = tmp_path.parent / f"{tmp_path.name}-other-disk"
    outside.mkdir()
    (tmp_path / "data").symlink_to(outside)  # a link the user put in the workspace is followed
    assert workspace_target("data/conv/x", str(tmp_path))[1] == tmp_path.resolve() / "data" / "conv" / "x"
    for bad in ("../a.aiida", "/etc/a.aiida", "", "data/../../a.aiida"):
        with pytest.raises(WorkerDomainError):
            workspace_target(bad, str(tmp_path))
    with pytest.raises(WorkerDomainError):
        workspace_target("a.aiida", None)
