"""One stdio JSON-RPC AiiDA worker per explicit project runtime identity."""

from __future__ import annotations

import asyncio
import json
import os
from collections import deque
from dataclasses import dataclass
from itertools import count
from pathlib import Path
from typing import Any, Mapping


RPC_LINE_LIMIT = 16 * 1024 * 1024


class WorkerRuntimeError(RuntimeError):
    def __init__(self, message: str, *, status_code: int = 503, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.status_code = int(status_code)
        self.detail = dict(detail or {})


@dataclass(frozen=True)
class ProjectRuntime:
    project_id: str
    python_interpreter_path: str
    aiida_profile: str
    workspace_path: str | None = None

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.project_id, self.python_interpreter_path, self.aiida_profile)

    def context(self) -> dict[str, str]:
        value = {
            "project_id": self.project_id,
            "python_interpreter_path": self.python_interpreter_path,
            "profile_name": self.aiida_profile,
        }
        if self.workspace_path:
            value["workspace_path"] = self.workspace_path
        return value


class WorkerRuntimePool:
    """Shared owner of project workers; no HTTP is ever added to aiida-worker."""

    def __init__(self, *, worker_source: Path | str, request_timeout: float = 60.0) -> None:
        self.worker_source = Path(worker_source).expanduser().resolve()
        self.request_timeout = max(0.2, float(request_timeout))
        self._workers: dict[tuple[str, str, str], _WorkerProcess] = {}
        self._lock = asyncio.Lock()

    async def request(
        self,
        runtime: ProjectRuntime,
        method: str,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        worker = await self._worker_for(runtime)
        return await worker.request(method, dict(params or {}), context=runtime.context())

    async def stop(self) -> None:
        workers = list(self._workers.values())
        self._workers.clear()
        await asyncio.gather(*(worker.stop() for worker in workers), return_exceptions=True)

    async def _worker_for(self, runtime: ProjectRuntime) -> "_WorkerProcess":
        async with self._lock:
            worker = self._workers.get(runtime.key)
            if worker is not None:
                return worker
            interpreter = Path(runtime.python_interpreter_path).expanduser()
            worker_package = self.worker_source / "src" / "aris_aiida_worker"
            bootstrap = Path(__file__).with_name("worker_bootstrap.py")
            if not interpreter.is_file() or not os.access(interpreter, os.X_OK):
                raise WorkerRuntimeError("Project Python interpreter is unavailable", status_code=404)
            if not worker_package.is_dir() or not bootstrap.is_file():
                raise WorkerRuntimeError("Bundled aiida-worker source is unavailable")
            worker = _WorkerProcess(
                command=[str(interpreter), "-u", str(bootstrap), str(self.worker_source)],
                cwd=runtime.workspace_path,
                env={"AIIDA_PROFILE": runtime.aiida_profile},
                request_timeout=self.request_timeout,
            )
            await worker.start()
            status = await worker.request("runtime.status", {}, context=runtime.context())
            if str(status.get("profile") or "") != runtime.aiida_profile:
                await worker.stop()
                raise WorkerRuntimeError("Worker started with an unexpected AiiDA profile")
            self._workers[runtime.key] = worker
            return worker


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
            if not raw:
                detail = {"stderr": "\n".join(self._stderr)}
                await self._invalidate()
                raise WorkerRuntimeError("AiiDA worker stopped unexpectedly", detail=detail)
            try:
                response = json.loads(raw.decode())
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                await self._invalidate()
                raise WorkerRuntimeError("AiiDA worker returned invalid JSON") from exc
            if response.get("id") != request_id:
                await self._invalidate()
                raise WorkerRuntimeError("AiiDA worker returned a mismatched response")
            error = response.get("error")
            if isinstance(error, dict):
                detail = error.get("data") if isinstance(error.get("data"), dict) else {}
                raise WorkerRuntimeError(str(error.get("message") or "AiiDA worker request failed"), status_code=int(detail.get("status_code", 422)), detail=detail)
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
