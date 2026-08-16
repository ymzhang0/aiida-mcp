"""One local daemon serving the AiiDA Manager API and ChatGPT MCP plugin."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse

from .api import build_manager_router
from .mcp import build_mcp_server
from .registry import ProjectRegistry
from .runtime import WorkerRuntimePool
from .service import AiiDAService


def default_worker_source() -> Path:
    configured = os.environ.get("AIIDA_MCP_WORKER_SOURCE")
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parents[4] / "aiida-worker"


def create_app(
    *,
    registry: ProjectRegistry | None = None,
    workers: WorkerRuntimePool | None = None,
) -> FastAPI:
    project_registry = registry or ProjectRegistry()
    runtime_pool = workers or WorkerRuntimePool(worker_source=default_worker_source())
    service = AiiDAService(project_registry, runtime_pool)
    mcp = build_mcp_server(service)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            await runtime_pool.stop()

    app = FastAPI(title="AiiDA MCP", version="0.1.0", lifespan=lifespan)
    app.state.project_registry = project_registry
    app.state.aiida_service = service
    app.include_router(build_manager_router(project_registry, service))

    @app.get("/", include_in_schema=False)
    async def console() -> FileResponse:
        return FileResponse(Path(__file__).with_name("static") / "index.html")

    token = str(os.environ.get("AIIDA_MCP_TOKEN") or "").strip()

    @app.middleware("http")
    async def require_plugin_token(request: Request, call_next):
        if token and request.url.path.startswith("/mcp"):
            expected = f"Bearer {token}"
            if request.headers.get("authorization") != expected:
                return JSONResponse(status_code=401, content={"detail": "Invalid MCP token"})
        return await call_next(request)

    # The Manager API remains local by default.  Only /mcp should be exposed
    # through a secure tunnel or reverse proxy for the ChatGPT plugin.
    app.mount("/mcp", mcp.http_app(path="/", transport="streamable-http"))
    return app


def main() -> None:
    uvicorn.run(
        create_app(),
        host=os.environ.get("AIIDA_MCP_HOST", "127.0.0.1"),
        port=int(os.environ.get("AIIDA_MCP_PORT", "8043")),
        log_level=os.environ.get("AIIDA_MCP_LOG_LEVEL", "info"),
    )


if __name__ == "__main__":
    main()
