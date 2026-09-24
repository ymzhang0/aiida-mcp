from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class WorkerRequestContext(BaseModel):
    """Execution context carried separately from method-specific parameters."""

    model_config = ConfigDict(extra="forbid")

    session_id: str | None = None
    workspace_path: str | None = None
    python_interpreter_path: str | None = None
    profile_name: str | None = None


class JsonRpcRequest(BaseModel):
    """One JSON-RPC 2.0 request sent to an isolated AiiDA runtime."""

    model_config = ConfigDict(extra="forbid")

    jsonrpc: str = Field(default="2.0", pattern=r"^2\.0$")
    id: str | int
    method: str = Field(..., min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    context: WorkerRequestContext | None = None


class JsonRpcError(BaseModel):
    code: int
    message: str
    data: dict[str, Any] = Field(default_factory=dict)


class JsonRpcResponse(BaseModel):
    """One JSON-RPC 2.0 response emitted by the isolated runtime."""

    model_config = ConfigDict(extra="forbid")

    jsonrpc: str = Field(default="2.0", pattern=r"^2\.0$")
    id: str | int | None
    result: Any | None = None
    error: JsonRpcError | None = None
