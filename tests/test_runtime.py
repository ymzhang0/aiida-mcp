from __future__ import annotations

import asyncio
import sys

import pytest

from aiida_mcp import runtime as runtime_module
from aiida_mcp.runtime import Runtime, WorkerRuntimeError, WorkerRuntimePool, _status_code

_FAKE_WORKER = """
import json, os, sys, time
for line in sys.stdin:
    request = json.loads(line)
    method = request["method"]
    if method == "die":
        sys.exit(1)
    if method == "slow":
        time.sleep(0.5)
    if method == "bad_status":
        response = {"error": {"message": "Invalid params", "data": {"status_code": "oops"}}}
    elif method == "not_an_object":
        print(json.dumps([1, 2]), flush=True)
        continue
    else:
        response = {"result": {"profile": "test", "pid": os.getpid(), "method": method}}
    print(json.dumps({"jsonrpc": "2.0", "id": request["id"], **response}), flush=True)
"""


@pytest.fixture
def pool(monkeypatch, tmp_path):
    script = tmp_path / "fake_worker.py"
    script.write_text(_FAKE_WORKER, encoding="utf-8")
    real = runtime_module._WorkerProcess

    def fake_worker(*, command, **kwargs):
        return real(command=[sys.executable, "-u", str(script)], **kwargs)

    monkeypatch.setattr(runtime_module, "_WorkerProcess", fake_worker)
    return WorkerRuntimePool(request_timeout=5)


def _runtime(workspace=None) -> Runtime:
    return Runtime("r", sys.executable, "test", workspace)


def test_crashed_worker_is_replaced_on_the_next_request(pool) -> None:
    async def scenario():
        try:
            first = await pool.request(_runtime(), "ping")
            with pytest.raises(WorkerRuntimeError, match="stopped unexpectedly") as caught:
                await pool.request(_runtime(), "die")
            assert "stderr" not in caught.value.detail
            second = await pool.request(_runtime(), "ping")
            return first["pid"], second["pid"]
        finally:
            await pool.stop()

    first_pid, second_pid = asyncio.run(scenario())
    assert first_pid != second_pid


def test_cancelled_request_does_not_poison_the_next_one(pool) -> None:
    async def scenario():
        try:
            await pool.request(_runtime(), "ping")
            task = asyncio.create_task(pool.request(_runtime(), "slow"))
            await asyncio.sleep(0.1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            return await pool.request(_runtime(), "ping")
        finally:
            await pool.stop()

    assert asyncio.run(scenario())["method"] == "ping"


def test_malformed_worker_replies_become_worker_errors(pool) -> None:
    async def scenario():
        try:
            with pytest.raises(WorkerRuntimeError) as bad_status:
                await pool.request(_runtime(), "bad_status")
            with pytest.raises(WorkerRuntimeError, match="invalid response"):
                await pool.request(_runtime(), "not_an_object")
            return bad_status.value.status_code
        finally:
            await pool.stop()

    assert asyncio.run(scenario()) == 422


def test_missing_workspace_is_reported_without_the_path(pool, tmp_path) -> None:
    missing = tmp_path / "gone"

    async def scenario():
        with pytest.raises(WorkerRuntimeError) as caught:
            await pool.request(_runtime(str(missing)), "ping")
        return caught.value

    error = asyncio.run(scenario())
    assert error.status_code == 404
    assert str(missing) not in str(error)


def test_status_code_falls_back_for_unusable_values() -> None:
    assert _status_code(404) == 404
    assert _status_code("409") == 409
    assert _status_code(None) == _status_code("oops") == _status_code(200) == 422
