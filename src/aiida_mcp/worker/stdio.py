from __future__ import annotations

import asyncio
import json
import sys
from typing import Any, TextIO

from aiida_mcp.worker.protocol import JsonRpcError, JsonRpcResponse
from aiida_mcp.worker.service import AiiDAWorkerService


def _write_response(stream: TextIO, response: JsonRpcResponse) -> None:
    stream.write(response.model_dump_json(exclude_none=True) + "\n")
    stream.flush()


async def serve_stdio(
    *,
    input_stream: TextIO = sys.stdin,
    output_stream: TextIO = sys.stdout,
    service: AiiDAWorkerService | None = None,
) -> int:
    """Serve newline-delimited JSON-RPC until EOF or runtime.shutdown."""

    runtime_service = service or AiiDAWorkerService()
    while True:
        line = await asyncio.to_thread(input_stream.readline)
        if line == "":
            return 0
        cleaned = line.strip()
        if not cleaned:
            continue

        try:
            payload: Any = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            _write_response(
                output_stream,
                JsonRpcResponse(
                    id=None,
                    error=JsonRpcError(
                        code=-32700,
                        message="Parse error",
                        data={"reason": str(exc)},
                    ),
                ),
            )
            continue

        response = await runtime_service.dispatch(payload)
        _write_response(output_stream, response)
        if isinstance(payload, dict) and payload.get("method") == "runtime.shutdown" and response.error is None:
            return 0


def main() -> None:
    raise SystemExit(asyncio.run(serve_stdio()))
