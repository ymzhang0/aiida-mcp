"""One local generic AiiDA MCP server for an explicitly selected runtime."""
from __future__ import annotations
import argparse
import json
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from .api import build_router
from .mcp import build_mcp_server
from .plugins import discover_plugin_tools
from .runtime import WorkerRuntimePool
from .service import AiiDAService, RuntimeConfig
def find_project(start: Path) -> tuple[Path, dict] | None:
    """The nearest project.yaml at or above `start` that names an AiiDA profile (runtime.aiida_profile)."""
    for directory in [start, *start.parents]:
        candidate = directory / "project.yaml"
        if not candidate.is_file(): continue
        try: data = yaml.safe_load(candidate.read_text())
        except (OSError, yaml.YAMLError): continue
        declared = data.get("runtime") if isinstance(data, dict) else None
        if isinstance(declared, dict) and str(declared.get("aiida_profile") or "").strip(): return directory, declared
    return None

def default_profile() -> str | None:
    """The default profile of the AiiDA config dir the workers will use."""
    bases = [item for item in [os.environ.get("AIIDA_MCP_AIIDA_PATH"), *str(os.environ.get("AIIDA_PATH") or "").split(os.pathsep)] if item]
    for base in [*bases, str(Path.home())]:
        try: name = json.loads((Path(base).expanduser() / ".aiida" / "config.json").read_text()).get("default_profile")
        except (OSError, ValueError, AttributeError): continue
        if name: return str(name)
    return None

def runtime_config(*, python_interpreter: str | None = None, profile: str | None = None, workspace: str | None = None) -> RuntimeConfig:
    """Which Python, profile and workspace this server uses.

    The profile is the one of the project being worked on: the nearest project.yaml at or above the
    workspace (default: the directory the server was started in) names it as runtime.aiida_profile,
    and may name runtime.python_interpreter. Outside a project, AIIDA_MCP_PROFILE and then the
    default profile are used. --profile overrides everything.
    """
    workspace = workspace or os.environ.get("AIIDA_MCP_WORKSPACE")
    start = Path(workspace or os.getcwd()).expanduser().resolve()
    root, declared = find_project(start) or (None, {})
    if profile: source = "--profile"
    elif declared: profile, source = str(declared["aiida_profile"]).strip(), str(root / "project.yaml")
    elif os.environ.get("AIIDA_MCP_PROFILE"): profile, source = os.environ["AIIDA_MCP_PROFILE"], "AIIDA_MCP_PROFILE"
    else: profile, source = default_profile(), "default profile (no project.yaml above the start directory)"
    python = python_interpreter or declared.get("python_interpreter") or os.environ.get("AIIDA_MCP_PYTHON")
    if not python: raise ValueError("No Python interpreter: set AIIDA_MCP_PYTHON or runtime.python_interpreter in project.yaml")
    if not profile: raise ValueError("No AiiDA profile: start the server inside a project (project.yaml with runtime.aiida_profile), or set AIIDA_MCP_PROFILE")
    return RuntimeConfig(str(python), profile, workspace or str(root or start), source)

def allow_write_default() -> bool:
    return os.environ.get("AIIDA_MCP_READ_ONLY", "").strip().lower() not in {"1", "true", "yes"}

_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})

def _hostname(host_header: str) -> str:
    """Host header without its port: 'localhost:8043' -> 'localhost', '[::1]:8043' -> '::1'."""
    value = host_header.strip().lower()
    if value.startswith("["): return value[1:].split("]", 1)[0]
    return value.rsplit(":", 1)[0] if value.count(":") == 1 else value

def access_error(*, token: str, path: str, host_header: str, authorization: str) -> tuple[int, str] | None:
    """Why a request to /mcp or /api is refused, or None.

    With a token, every request needs it. Without one the server only answers requests
    addressed to a loopback name: that stops DNS rebinding from a web page and keeps a
    tunnel that forwards the whole port from exposing the runtime unauthenticated.
    """
    if path == "/api/ready" or not (path.startswith("/mcp") or path.startswith("/api")): return None
    if token:
        if secrets.compare_digest(authorization.encode(), f"Bearer {token}".encode()): return None
        return 401, "Invalid MCP token"
    if _hostname(host_header) in _LOOPBACK_HOSTS: return None
    return 403, "Set AIIDA_MCP_TOKEN to reach this server through anything but localhost"

def require_token_for_host(host: str) -> None:
    if host.strip().lower() not in _LOOPBACK_HOSTS and not str(os.environ.get("AIIDA_MCP_TOKEN") or "").strip():
        raise SystemExit(f"Refusing to listen on {host} without AIIDA_MCP_TOKEN: anyone who can reach the port could use this AiiDA runtime.")

def create_app(*, runtime: RuntimeConfig | None = None, workers: WorkerRuntimePool | None = None, allow_write: bool | None = None, plugin_tools: list[dict] | None = None) -> FastAPI:
    configured_runtime = runtime or runtime_config()
    pool = workers or WorkerRuntimePool()
    service = AiiDAService(configured_runtime, pool)
    mcp = build_mcp_server(service, allow_write=allow_write_default() if allow_write is None else allow_write, plugin_tools=plugin_tools)
    mcp_app = mcp.http_app(path="/", transport="streamable-http")
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # The mounted MCP app's session manager only starts inside its own lifespan.
        try:
            async with mcp_app.lifespan(app): yield
        finally: await pool.stop()
    app = FastAPI(title="AiiDA MCP", version="0.1.0", lifespan=lifespan)
    app.include_router(build_router(service))
    token = str(os.environ.get("AIIDA_MCP_TOKEN") or "").strip()
    @app.middleware("http")
    async def require_plugin_token(request: Request, call_next):
        refused = access_error(token=token, path=request.url.path, host_header=request.headers.get("host", ""), authorization=request.headers.get("authorization", ""))
        if refused: return JSONResponse(status_code=refused[0], content={"detail": refused[1]})
        return await call_next(request)
    app.mount("/mcp", mcp_app)
    return app

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="aiida-mcp")
    parser.add_argument("serve", nargs="?")
    parser.add_argument("--python", dest="python_interpreter")
    parser.add_argument("--profile")
    parser.add_argument("--workspace")
    parser.add_argument("--read-only", action="store_true", help="Disable submission, process control, and write plugin tools (or set AIIDA_MCP_READ_ONLY=1).")
    parser.add_argument("--no-plugins", action="store_true", help="Do not load tools from the aiida_mcp.tools entry-point group.")
    args = parser.parse_args(argv)
    runtime = runtime_config(python_interpreter=args.python_interpreter, profile=args.profile, workspace=args.workspace)
    allow_write = allow_write_default() and not args.read_only
    plugin_tools = [] if args.no_plugins else discover_plugin_tools(runtime)
    if os.environ.get("AIIDA_MCP_TRANSPORT", "").lower() == "stdio":
        build_mcp_server(AiiDAService(runtime, WorkerRuntimePool()), allow_write=allow_write, plugin_tools=plugin_tools).run(transport="stdio")
        return
    host = os.environ.get("AIIDA_MCP_HOST", "127.0.0.1")
    require_token_for_host(host)
    uvicorn.run(create_app(runtime=runtime, allow_write=allow_write, plugin_tools=plugin_tools), host=host, port=int(os.environ.get("AIIDA_MCP_PORT", "8043")))
