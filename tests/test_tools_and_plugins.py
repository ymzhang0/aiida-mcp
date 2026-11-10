from __future__ import annotations

from importlib.metadata import EntryPoint

import pytest

from aiida_mcp.mcp import build_mcp_server
from aiida_mcp.worker.application import plugin_service

_WRITE_TOOLS = {"submit", "restart_process", "kill_processes", "pause_processes", "resume_processes"}
_MANIFEST = [
    {"name": "demo_report", "plugin": "demo", "description": "Report.", "read_only": True,
     "input_schema": {"type": "object", "properties": {"identifier": {"type": "string"}}, "required": ["identifier"]}},
    {"name": "demo_cleanup", "plugin": "demo", "description": "Cleanup.", "read_only": False,
     "input_schema": {"type": "object", "properties": {}}},
]


class _Service:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def plugin_call(self, tool, arguments):
        self.calls.append((tool, arguments))
        return {"tool": tool, "arguments": arguments}

    async def process_restart(self, params):
        self.calls.append(("restart", params))
        return {"success": True, "status": "dry_run" if params.get("dry_run") else "submitted"}

    async def submission_submit(self, draft):
        self.calls.append(("submit", draft))
        return {"pk": 1}


async def _tools(server):
    return {tool.name: tool for tool in await server.list_tools()}


@pytest.mark.anyio
async def test_write_tools_are_registered_only_when_allowed() -> None:
    writable = await _tools(build_mcp_server(_Service(), allow_write=True, plugin_tools=_MANIFEST))
    read_only = await _tools(build_mcp_server(_Service(), allow_write=False, plugin_tools=_MANIFEST))
    assert _WRITE_TOOLS <= set(writable)
    assert not _WRITE_TOOLS & set(read_only)
    assert writable["kill_processes"].annotations.destructiveHint is True
    assert {"list_processes", "list_workflows", "get_workflow_spec", "list_codes", "validate_submission"} <= set(read_only)
    assert "demo_report" in read_only and "demo_cleanup" not in read_only
    assert "demo_cleanup" in writable


@pytest.mark.anyio
async def test_plugin_tool_uses_manifest_schema_and_proxies_to_worker() -> None:
    service = _Service()
    server = build_mcp_server(service, allow_write=False, plugin_tools=_MANIFEST)
    tool = (await _tools(server))["demo_report"]
    assert tool.parameters["required"] == ["identifier"]
    assert tool.annotations.readOnlyHint is True
    result = await server.call_tool("demo_report", {"identifier": "5"})
    assert result.structured_content == {"tool": "demo_report", "arguments": {"identifier": "5"}}


@pytest.mark.anyio
async def test_submit_maps_protocol_arguments_to_builder_intent() -> None:
    service = _Service()
    server = build_mcp_server(service, allow_write=True)
    await server.call_tool("submit", {"entry_point": "x.y", "protocol": "fast", "protocol_arguments": {"code": "pw@cluster"}})
    assert service.calls == [("submit", {"entry_point": "x.y", "protocol": "fast", "intent_data": {"code": "pw@cluster"}, "overrides": {}})]


@pytest.mark.anyio
async def test_restart_process_forwards_only_given_arguments() -> None:
    service = _Service()
    server = build_mcp_server(service, allow_write=True)
    result = await server.call_tool("restart_process", {"identifier": "1462", "builder_updates": {"clean_workdir": False}, "dry_run": True})
    assert service.calls == [("restart", {"identifier": "1462", "builder_updates": {"clean_workdir": False}, "dry_run": True})]
    assert result.structured_content["status"] == "dry_run"


def report(identifier: str, include_outputs: bool = False) -> dict:
    """Diagnose a process."""
    return {"identifier": identifier, "include_outputs": include_outputs}


report.read_only = True


def cleanup() -> list:
    return [1]


def test_worker_discovers_entry_point_tools(monkeypatch) -> None:
    entry_point = EntryPoint(name="demo", value=f"{__name__}:_TOOLS", group=plugin_service.ENTRY_POINT_GROUP)
    broken = EntryPoint(name="broken", value="does_not_exist:tools", group=plugin_service.ENTRY_POINT_GROUP)
    monkeypatch.setattr(plugin_service, "entry_points", lambda group: [entry_point, broken])
    registry = plugin_service.PluginRegistry()
    manifest = registry.manifest()
    tools = {item["name"]: item for item in manifest["tools"]}
    assert set(tools) == {"report", "cleanup"}
    assert tools["report"]["read_only"] is True
    assert tools["report"]["description"] == "Diagnose a process."
    assert tools["report"]["input_schema"]["required"] == ["identifier"]
    assert tools["cleanup"]["read_only"] is False
    assert [error["plugin"] for error in manifest["errors"]] == ["broken"]

    monkeypatch.setattr("aiida_mcp.core.engine.ensure_profile_loaded", lambda: None)
    assert registry.call("report", {"identifier": "7"}) == {"identifier": "7", "include_outputs": False}
    assert registry.call("cleanup", {}) == {"result": [1]}
    with pytest.raises(Exception):
        registry.call("report", {"identifier": "7", "unexpected": 1})


def test_duplicate_plugin_tool_names_are_reported(monkeypatch) -> None:
    first = EntryPoint(name="first", value=f"{__name__}:_TOOLS", group=plugin_service.ENTRY_POINT_GROUP)
    second = EntryPoint(name="second", value=f"{__name__}:_TOOLS", group=plugin_service.ENTRY_POINT_GROUP)
    monkeypatch.setattr(plugin_service, "entry_points", lambda group: [first, second])
    manifest = plugin_service.PluginRegistry().manifest()
    assert {item["plugin"] for item in manifest["tools"]} == {"first"}
    assert {(error["plugin"], error["tool"]) for error in manifest["errors"]} == {("second", "report"), ("second", "cleanup")}


@pytest.mark.anyio
async def test_plugin_tool_cannot_shadow_a_builtin_tool() -> None:
    service = _Service()
    shadow = [{**_MANIFEST[0], "name": "submit"}]
    server = build_mcp_server(service, allow_write=True, plugin_tools=shadow)
    await server.call_tool("submit", {"entry_point": "x.y"})
    # The built-in tool records ("submit", draft); the plugin proxy would record ("submit", arguments) with no intent_data.
    assert service.calls == [("submit", {"entry_point": "x.y", "protocol": None, "intent_data": {}, "overrides": {}})]


_TOOLS = [report, cleanup]


def test_compact_process_keeps_values_and_drops_duplicates() -> None:
    from aiida_mcp.mcp import compact_process

    params = {"pk": 9, "type": "Dict", "payload": {"energy": -310.7, "symmetries": list(range(48)), "SYSTEM": {"ecutrho": 360}},
              "preview_info": {"summary": "x" * 5000, "data": {"energy": -310.7}}, "link_label": "output_parameters"}
    detail = {"summary": {"pk": 1, "process_label": "PwBaseWorkChain", "state": "finished", "exit_status": 0, "full_type": "long"},
              "inputs": {"max_iterations": {"pk": 2, "type": "Int", "payload": 5, "label": "5"}},
              "outputs": {"output_parameters": params, "iteration_01": {"pk": 3, "process_label": "PwCalculation", "state": "finished", "exit_status": 0}},
              "direct_outputs": {"output_parameters": params}, "workchain": {"provenance_tree": "y" * 80000},
              "logs": {"reports": [f"line {i}" for i in range(30)]}}
    compact = compact_process(detail)
    assert compact["outputs"]["output_parameters"]["value"] == {"energy": -310.7, "symmetries": "<list of 48>", "SYSTEM": {"ecutrho": 360}}
    assert compact["inputs"]["max_iterations"] == {"pk": 2, "type": "Int", "value": 5}
    assert compact["called"]["iteration_01"]["type"] == "PwCalculation"
    assert len(compact["reports"]) == 20 and "full_type" not in compact["summary"]
    assert "workchain" not in compact and "direct_outputs" not in compact


def test_worker_keeps_stdout_for_json_rpc_when_handlers_print() -> None:
    import json
    import subprocess
    import sys
    from pathlib import Path

    import aiida_mcp

    code = (
        "import sys, asyncio\n"
        "from aiida_mcp.worker import stdio\n"
        "from aiida_mcp.worker.service import AiiDAWorkerService\n"
        "class Noisy(AiiDAWorkerService):\n"
        "    def __init__(self):\n"
        "        super().__init__()\n"
        "        self._handlers['noisy'] = lambda params: (print('chatty plugin output'), {'ok': True})[1]\n"
        "stdio.AiiDAWorkerService = Noisy\n"
        "stdio.main()\n"
    )
    src = str(Path(aiida_mcp.__file__).resolve().parents[1])
    done = subprocess.run([sys.executable, "-c", code], input='{"jsonrpc": "2.0", "id": 1, "method": "noisy", "params": {}}\n',
                          capture_output=True, text=True, timeout=60, env={"PYTHONPATH": src, "PATH": "/usr/bin:/bin"})
    lines = done.stdout.splitlines()
    assert len(lines) == 1 and json.loads(lines[0])["result"] == {"ok": True}
    assert "chatty plugin output" in done.stderr
