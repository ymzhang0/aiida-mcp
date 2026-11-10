"""Fetch the plugin tool manifest from the selected runtime at server start.

Plugins live in the project Python, not in the server's environment, so the
server asks a short-lived worker for the manifest before it registers tools.
That worker imports AiiDA and every plugin, which on a cold network file system
can take longer than a client waits for the server to answer `initialize`. The
last manifest is therefore cached per runtime: a start with a cache registers
the cached tools at once and refreshes the cache in the background, so a newly
installed or removed plugin tool shows up from the next start.
"""
from __future__ import annotations

import json
import logging
import os
import hashlib
import subprocess
import threading
from pathlib import Path
from typing import Any

from .runtime import worker_environment
from .service import RuntimeConfig

log = logging.getLogger(__name__)


def _cache_path(runtime: RuntimeConfig) -> Path:
    key = "|".join([runtime.python_interpreter_path, runtime.aiida_profile, os.environ.get("AIIDA_MCP_AIIDA_PATH", "")])
    root = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "aiida-mcp"
    return root / f"plugin-manifest-{hashlib.sha256(key.encode()).hexdigest()[:16]}.json"


def _read_cache(path: Path) -> list[dict[str, Any]] | None:
    try:
        tools = json.loads(path.read_text())["tools"]
        return tools if isinstance(tools, list) else None
    except (OSError, ValueError, KeyError, TypeError):
        return None


def _write_cache(path: Path, tools: list[dict[str, Any]]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(f".{os.getpid()}.tmp")
        tmp.write_text(json.dumps({"tools": tools}))
        tmp.replace(path)
    except OSError as exc:
        log.warning("aiida-mcp could not cache the plugin manifest: %s", exc)


def discover_plugin_tools(runtime: RuntimeConfig, *, timeout: float = 120.0, use_cache: bool = True) -> list[dict[str, Any]]:
    """Plugin tools for ``runtime``: the cached manifest when there is one (refreshed in the background), else a fresh one."""
    if not use_cache:
        return _query_manifest(runtime, timeout=timeout) or []
    path = _cache_path(runtime)
    cached = _read_cache(path)
    if cached is None:
        tools = _query_manifest(runtime, timeout=timeout)
        if tools is not None:
            _write_cache(path, tools)
        return tools or []

    def refresh() -> None:
        tools = _query_manifest(runtime, timeout=timeout)
        if tools is not None and tools != cached:
            _write_cache(path, tools)
            log.info("aiida-mcp plugin tools changed; restart the server to load them")

    threading.Thread(target=refresh, name="aiida-mcp-manifest-refresh", daemon=True).start()
    return cached


def _query_manifest(runtime: RuntimeConfig, *, timeout: float) -> list[dict[str, Any]] | None:
    """Ask a short-lived worker for the manifest; None when the worker could not answer (never cached)."""
    bootstrap = Path(__file__).with_name("worker_bootstrap.py")
    requests = [
        {"jsonrpc": "2.0", "id": 1, "method": "plugins.manifest", "params": {}},
        {"jsonrpc": "2.0", "id": 2, "method": "runtime.shutdown", "params": {}},
    ]
    try:
        completed = subprocess.run(
            [runtime.python_interpreter_path, "-u", str(bootstrap)],
            input="".join(json.dumps(item) + "\n" for item in requests),
            capture_output=True, text=True, timeout=timeout, cwd=runtime.workspace_path,
            env={**os.environ, **worker_environment(runtime.aiida_profile)},
        )
        response = json.loads(completed.stdout.splitlines()[0])
    except (OSError, subprocess.SubprocessError, IndexError, json.JSONDecodeError) as exc:
        log.warning("aiida-mcp plugin discovery failed; continuing without plugins: %s", exc)
        return None
    if "error" in response:
        log.warning("aiida-mcp plugin discovery failed; continuing without plugins: %s", response["error"])
        return None
    for error in response["result"].get("errors", []):
        log.warning("aiida-mcp plugin failed to load: %s", error)
    return list(response["result"].get("tools", []))
