"""One local generic AiiDA MCP server for an explicitly selected runtime."""
from __future__ import annotations
import argparse
import os
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from .api import build_router
from .mcp import build_mcp_server
from .runtime import WorkerRuntimePool
from .service import AiiDAService, RuntimeConfig
def runtime_config(*, python_interpreter: str | None = None, profile: str | None = None, workspace: str | None = None) -> RuntimeConfig:
    python = python_interpreter or os.environ.get("AIIDA_MCP_PYTHON")
    selected_profile = profile or os.environ.get("AIIDA_MCP_PROFILE")
    if not python or not selected_profile: raise ValueError("AIIDA_MCP_PYTHON and AIIDA_MCP_PROFILE are required")
    return RuntimeConfig(python, selected_profile, workspace or os.environ.get("AIIDA_MCP_WORKSPACE"))

def create_app(*, runtime: RuntimeConfig | None = None, workers: WorkerRuntimePool | None = None) -> FastAPI:
    configured_runtime = runtime or runtime_config()
    pool = workers or WorkerRuntimePool()
    service = AiiDAService(configured_runtime, pool)
    mcp = build_mcp_server(service)
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try: yield
        finally: await pool.stop()
    app = FastAPI(title="AiiDA MCP", version="0.1.0", lifespan=lifespan)
    app.include_router(build_router(service))
    token = str(os.environ.get("AIIDA_MCP_TOKEN") or "").strip()
    @app.middleware("http")
    async def require_plugin_token(request: Request, call_next):
        if token and request.url.path.startswith("/mcp") and request.headers.get("authorization") != f"Bearer {token}": return JSONResponse(status_code=401, content={"detail": "Invalid MCP token"})
        return await call_next(request)
    app.mount("/mcp", mcp.http_app(path="/", transport="streamable-http"))
    return app

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="aiida-mcp")
    parser.add_argument("serve", nargs="?")
    parser.add_argument("--python", dest="python_interpreter")
    parser.add_argument("--profile")
    parser.add_argument("--workspace")
    args = parser.parse_args(argv)
    runtime = runtime_config(python_interpreter=args.python_interpreter, profile=args.profile, workspace=args.workspace)
    if os.environ.get("AIIDA_MCP_TRANSPORT", "").lower() == "stdio":
        build_mcp_server(AiiDAService(runtime, WorkerRuntimePool())).run(transport="stdio")
        return
    uvicorn.run(create_app(runtime=runtime), host=os.environ.get("AIIDA_MCP_HOST", "127.0.0.1"), port=int(os.environ.get("AIIDA_MCP_PORT", "8043")))
