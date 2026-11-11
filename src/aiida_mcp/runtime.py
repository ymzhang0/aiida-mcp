"""One stdio JSON-RPC AiiDA worker per explicit runtime identity."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import deque
from dataclasses import dataclass
from itertools import count
from pathlib import Path
from typing import Any, Mapping


RPC_LINE_LIMIT = 16 * 1024 * 1024

log = logging.getLogger(__name__)


class WorkerRuntimeError(RuntimeError):
    def __init__(self, message: str, *, status_code: int = 503, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.status_code = int(status_code)
        self.detail = dict(detail or {})


@dataclass(frozen=True)
class Runtime:
    runtime_id: str
    python_interpreter_path: str
    aiida_profile: str
    workspace_path: str | None = None

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.runtime_id, self.python_interpreter_path, self.aiida_profile)

    def context(self) -> dict[str, str]:
        value = {
            "python_interpreter_path": self.python_interpreter_path,
            "profile_name": self.aiida_profile,
        }
        if self.workspace_path:
            value["workspace_path"] = self.workspace_path
        return value


def worker_environment(profile: str) -> dict[str, str]:
    """Environment overrides for a worker: the profile, plus AIIDA_PATH when the server was given one."""
    env = {"AIIDA_PROFILE": profile}
    aiida_path = str(os.environ.get("AIIDA_MCP_AIIDA_PATH") or "").strip()
    if aiida_path:
        env["AIIDA_PATH"] = aiida_path
    return env


class WorkerRuntimePool:
    """Own private stdio workers; no worker HTTP service exists."""

    def __init__(self, *, request_timeout: float = 60.0) -> None:
        self.request_timeout = max(0.2, float(request_timeout))
        self._workers: dict[tuple[str, str, str], _WorkerProcess] = {}
        self._locks: dict[tuple[str, str, str], asyncio.Lock] = {}

    async def request(
        self,
        runtime: Runtime,
        method: str,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        worker = await self._worker_for(runtime)
        return await worker.request(method, dict(params or {}), context=runtime.context())

    async def stop(self) -> None:
        workers = list(self._workers.values())
        self._workers.clear()
        await asyncio.gather(*(worker.stop() for worker in workers), return_exceptions=True)

    async def _worker_for(self, runtime: Runtime) -> "_WorkerProcess":
        # One lock per runtime: a slow worker start must not block requests to other runtimes.
        async with self._locks.setdefault(runtime.key, asyncio.Lock()):
            worker = self._workers.get(runtime.key)
            if worker is not None:
                if worker.alive:
                    return worker
                # A worker that crashed, timed out, or was invalidated is replaced, not kept offline forever.
                self._workers.pop(runtime.key, None)
                await worker.stop()
            interpreter = Path(runtime.python_interpreter_path).expanduser()
            bootstrap = Path(__file__).with_name("worker_bootstrap.py")
            if not interpreter.is_file() or not os.access(interpreter, os.X_OK):
                raise WorkerRuntimeError("Project Python interpreter is unavailable", status_code=404)
            if not bootstrap.is_file():
                raise WorkerRuntimeError("Bundled worker bootstrap is unavailable")
            if runtime.workspace_path and not Path(runtime.workspace_path).expanduser().is_dir():
                raise WorkerRuntimeError("Configured workspace directory is unavailable", status_code=404)
            worker = _WorkerProcess(
                command=[str(interpreter), "-u", str(bootstrap)],
                cwd=runtime.workspace_path,
                env=worker_environment(runtime.aiida_profile),
                request_timeout=self.request_timeout,
            )
            try:
                await worker.start()
            except OSError as exc:
                log.warning("Could not start the AiiDA worker: %s", exc)
                raise WorkerRuntimeError("AiiDA worker could not be started") from exc
            try:
                status = await worker.request("runtime.status", {}, context=runtime.context())
            except BaseException:
                await worker.stop()
                raise
            if str(status.get("profile") or "") != runtime.aiida_profile:
                await worker.stop()
                raise WorkerRuntimeError(
                    f"AiiDA profile '{runtime.aiida_profile}' could not be loaded in {runtime.python_interpreter_path} "
                    f"(config dir: {status.get('config_dir') or 'unknown'}; loaded: {status.get('profile') or 'none'}). "
                    f"{status.get('profile_error') or ''} If the profile lives in another config dir, set AIIDA_PATH for the server.".strip(),
                    detail={"config_dir": status.get("config_dir"), "profile_error": status.get("profile_error")},
                )
            self._workers[runtime.key] = worker
            return worker


def _status_code(value: Any, default: int = 422) -> int:
    """A worker-supplied HTTP status, or ``default`` when it is missing or not a usable error status."""
    try:
        code = int(value)
    except (TypeError, ValueError):
        return default
    return code if 400 <= code <= 599 else default


class _WorkerProcess:
    def __init__(self, *, command: list[str], cwd: str | None, env: Mapping[str, str], request_timeout: float) -> None:
        self.command = command
        self.cwd = cwd
        self.env = dict(env)
        self.request_timeout = request_timeout
        self.process: asyncio.subprocess.Process | None = None
        self._lock = asyncio.Lock()
        self._request_ids = count(1)
        self._stderr: deque[str] = deque(maxlen=50)
        self._stderr_task: asyncio.Task[None] | None = None

    @property
    def alive(self) -> bool:
        return self.process is not None and self.process.returncode is None

    async def start(self) -> None:
        async with self._lock:
            if self.process is not None and self.process.returncode is None:
                return
            environment = os.environ.copy()
            environment.update(self.env)
            self.process = await asyncio.create_subprocess_exec(
                *self.command,
                cwd=self.cwd,
                env=environment,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=RPC_LINE_LIMIT,
            )
            self._stderr_task = asyncio.create_task(self._collect_stderr(self.process))

    async def request(self, method: str, params: dict[str, Any], *, context: dict[str, str]) -> dict[str, Any]:
        async with self._lock:
            process = self.process
            if process is None or process.returncode is not None or process.stdin is None or process.stdout is None:
                raise WorkerRuntimeError("AiiDA worker is offline")
            request_id = next(self._request_ids)
            payload = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params, "context": context}
            try:
                process.stdin.write((json.dumps(payload) + "\n").encode())
                await process.stdin.drain()
                raw = await asyncio.wait_for(process.stdout.readline(), timeout=self.request_timeout)
            except (BrokenPipeError, ConnectionError, asyncio.TimeoutError) as exc:
                await self._invalidate()
                raise WorkerRuntimeError(f"AiiDA worker request failed: {exc or 'timeout'}") from exc
            except asyncio.CancelledError:
                # The response to this request is still on its way; left in the pipe it would be
                # read as the answer to the next request. Drop the worker; the pool starts a new one.
                await asyncio.shield(self._invalidate())
                raise
            if not raw:
                # stderr stays in the server log: it can hold paths and tracebacks the client has no use for.
                log.warning("AiiDA worker stopped unexpectedly; last stderr lines:\n%s", "\n".join(self._stderr))
                await self._invalidate()
                raise WorkerRuntimeError("AiiDA worker stopped unexpectedly")
            try:
                response = json.loads(raw.decode())
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                await self._invalidate()
                raise WorkerRuntimeError("AiiDA worker returned invalid JSON") from exc
            if not isinstance(response, dict):
                await self._invalidate()
                raise WorkerRuntimeError("AiiDA worker returned an invalid response")
            if response.get("id") != request_id:
                await self._invalidate()
                raise WorkerRuntimeError("AiiDA worker returned a mismatched response")
            error = response.get("error")
            if isinstance(error, dict):
                detail = error.get("data") if isinstance(error.get("data"), dict) else {}
                message = str(error.get("message") or "AiiDA worker request failed")
                if detail.get("reason"):
                    message = f"{message}: {detail['reason']}"
                raise WorkerRuntimeError(message, status_code=_status_code(detail.get("status_code")), detail=detail)
            result = response.get("result")
            if not isinstance(result, dict):
                raise WorkerRuntimeError("AiiDA worker returned an invalid result")
            return result

    async def stop(self) -> None:
        async with self._lock:
            await self._invalidate()

    async def _invalidate(self) -> None:
        process, self.process = self.process, None
        if process is not None and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=3)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        task, self._stderr_task = self._stderr_task, None
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _collect_stderr(self, process: asyncio.subprocess.Process) -> None:
        if process.stderr is None:
            return
        while line := await process.stderr.readline():
            self._stderr.append(line.decode(errors="replace").rstrip())
