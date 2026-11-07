import pytest

pytest.importorskip("aiida")
pytest_plugins = ["aiida.tools.pytest_fixtures"]

from aiida import orm  # noqa: E402

from aiida_mcp.mcp import build_mcp_server  # noqa: E402
from aiida_mcp.worker.application.agent_service import create_structure, process_state  # noqa: E402
from aiida_mcp.worker.protocol.errors import WorkerDomainError  # noqa: E402


@pytest.mark.usefixtures("aiida_profile_clean")
def test_create_structure_from_bulk_cell_and_workspace_file(tmp_path) -> None:
    si = create_structure({"bulk": {"name": "Si", "crystalstructure": "diamond", "a": 5.43}}, workspace_path=None)
    assert si["formula"] == "Si2" and si["n_sites"] == 2
    assert abs(si["cell_volume"] - 5.43**3 / 4) < 1e-6
    assert orm.load_node(si["pk"]).is_stored

    nacl = create_structure({"cell": [[5.6, 0, 0], [0, 5.6, 0], [0, 0, 5.6]], "sites": [{"symbol": "Na", "position": [0, 0, 0]}, {"symbol": "Cl", "position": [2.8, 2.8, 2.8]}], "supercell": [2, 1, 1]}, workspace_path=None)
    assert nacl["formula"] == "Cl2Na2"

    (tmp_path / "si.xyz").write_text("1\n\nSi 0 0 0\n")
    from_file = create_structure({"file": "si.xyz", "pbc": None, "label": "from file"}, workspace_path=str(tmp_path))
    assert from_file["label"] == "from file"
    with pytest.raises(WorkerDomainError):
        create_structure({"file": "../outside.cif"}, workspace_path=str(tmp_path))
    with pytest.raises(ValueError, match="exactly one"):
        create_structure({"bulk": {"name": "Si"}, "file": "si.xyz"}, workspace_path=str(tmp_path))
    with pytest.raises(ValueError, match="Unknown bulk keys"):
        create_structure({"bulk": {"name": "Si", "lattice": 5.43}}, workspace_path=None)


@pytest.mark.usefixtures("aiida_profile_clean")
def test_process_state_reports_kill_reason() -> None:
    node = orm.CalcFunctionNode()
    node.set_process_state("killed")
    node.set_process_status("Killed through `verdi process kill`")
    node.store()
    state = process_state(str(node.pk))
    assert state["terminated"] is True and state["state"] == "killed"
    assert state["process_status"] == "Killed through `verdi process kill`"


class _Waiting:
    def __init__(self) -> None:
        self.polls = 0

    async def _call(self, method, params=None):
        self.polls += 1
        return {"pk": 1, "terminated": self.polls >= 3, "state": "finished" if self.polls >= 3 else "running"}


@pytest.mark.anyio
async def test_wait_for_process_polls_until_terminated_and_times_out() -> None:
    from aiida_mcp.service import AiiDAService

    service = AiiDAService.__new__(AiiDAService)
    waiting = _Waiting()
    service._call = waiting._call
    done = await service.wait_for_process("1", timeout_seconds=30, poll_seconds=0.01)
    assert done["state"] == "finished" and done["timed_out"] is False and waiting.polls == 3

    waiting.polls = -10_000
    timed_out = await service.wait_for_process("1", timeout_seconds=0)
    assert timed_out["timed_out"] is True


@pytest.mark.anyio
async def test_create_structure_is_a_write_tool() -> None:
    read_only = {tool.name for tool in await build_mcp_server(object(), allow_write=False).list_tools()}
    writable = {tool.name for tool in await build_mcp_server(object(), allow_write=True).list_tools()}
    assert "wait_for_process" in read_only and "create_structure" not in read_only and "create_structure" in writable
