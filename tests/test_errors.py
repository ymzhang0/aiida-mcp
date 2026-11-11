from __future__ import annotations

import asyncio

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from aiida_mcp.errors import WorkerErrorMiddleware, describe_worker_error
from aiida_mcp.runtime import WorkerRuntimeError


def test_reason_is_appended_to_the_message() -> None:
    exc = WorkerRuntimeError("Invalid params", status_code=422, detail={"method": "node.summary", "reason": "pk parameter is required", "status_code": 422})
    text = describe_worker_error(exc)
    assert text.startswith("Invalid params: pk parameter is required")
    assert '"method": "node.summary"' in text
    assert "status_code" not in text


def test_reason_already_in_the_message_is_not_repeated() -> None:
    exc = WorkerRuntimeError("Node not found: no Node with ID<9>", detail={"reason": "no Node with ID<9>"})
    assert describe_worker_error(exc) == "Node not found: no Node with ID<9>"


def test_message_without_detail_is_unchanged() -> None:
    assert describe_worker_error(WorkerRuntimeError("AiiDA worker is offline")) == "AiiDA worker is offline"


def test_tool_failure_reaches_the_client_with_the_reason() -> None:
    server = FastMCP("test")
    server.add_middleware(WorkerErrorMiddleware())

    @server.tool
    async def failing() -> dict:
        raise WorkerRuntimeError("Invalid params", detail={"reason": "computer_label: Field required"})

    @server.tool
    async def unrelated() -> dict:
        raise RuntimeError("something else")

    async def call(name: str) -> str:
        async with Client(server) as client:
            with pytest.raises(ToolError) as caught:
                await client.call_tool(name)
            return str(caught.value)

    assert "computer_label: Field required" in asyncio.run(call("failing"))
    assert "something else" in asyncio.run(call("unrelated"))
