"""Fetch the plugin tool manifest from the selected runtime at server start.

Plugins live in the project Python, not in the server's environment, so the
server asks a short-lived worker for the manifest before it registers tools.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Any

from .runtime import worker_environment
from .service import RuntimeConfig

log = logging.getLogger(__name__)


def discover_plugin_tools(runtime: RuntimeConfig, *, timeout: float = 120.0) -> list[dict[str, Any]]:
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
        return []
    if "error" in response:
        log.warning("aiida-mcp plugin discovery failed; continuing without plugins: %s", response["error"])
        return []
    for error in response["result"].get("errors", []):
        log.warning("aiida-mcp plugin failed to load: %s", error)
    return list(response["result"].get("tools", []))
