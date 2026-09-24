from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
import traceback
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import count
from pathlib import Path
from typing import Any, Awaitable, Callable
from uuid import uuid4

from aiida_mcp.worker.protocol.errors import WorkerDomainError

RESULT_MARKER = "__ARIS_EXECUTION_RESULT__:"
METADATA_MARKER = "__ARIS_ENV_METADATA__:"
ACTIVE_WORKSPACE_ENV_VAR = "ARIS_ACTIVE_WORKSPACE_PATH"

_RUNNER_BOOTSTRAP = """
import asyncio
import inspect
import json
import os
from pathlib import Path
import sys
import traceback

RESULT_MARKER = "__ARIS_EXECUTION_RESULT__:"
ACTIVE_WORKSPACE_ENV_VAR = "ARIS_ACTIVE_WORKSPACE_PATH"

def _normalize(value):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_normalize(item) for item in value]
    return str(value)

def save_artifact(relative_path, content, *, encoding="utf-8"):
    workspace = os.environ.get(ACTIVE_WORKSPACE_ENV_VAR)
    if not workspace:
        raise RuntimeError("No workspace path is active")
    root = Path(workspace).resolve()
    target = (root / str(relative_path)).resolve()
    target.relative_to(root)
    target.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, (bytes, bytearray)):
        target.write_bytes(bytes(content))
    elif isinstance(content, (dict, list)):
        target.write_text(json.dumps(_normalize(content), ensure_ascii=True, indent=2) + "\\n", encoding=encoding)
    else:
        target.write_text(str(content), encoding=encoding)
    return str(target)

payload_path = Path(sys.argv[1])
payload = json.loads(payload_path.read_text(encoding="utf-8"))
profile_name = payload.get("profile_name")
if isinstance(profile_name, str):
    profile_name = profile_name.strip() or None
else:
    profile_name = None
workspace_path = payload.get("workspace_path")
if isinstance(workspace_path, str):
    workspace_path = workspace_path.strip() or None
else:
    workspace_path = None

if workspace_path:
    workspace = str(Path(workspace_path).expanduser().resolve())
    Path(workspace).mkdir(parents=True, exist_ok=True)
    os.chdir(workspace)
    os.environ[ACTIVE_WORKSPACE_ENV_VAR] = workspace
else:
    workspace = None
    os.environ.pop(ACTIVE_WORKSPACE_ENV_VAR, None)

namespace = {
    "__name__": "__main__",
    "__file__": str(payload.get("source_name") or "<aris-execution>"),
    "ACTIVE_WORKSPACE_PATH": workspace,
    "save_artifact": save_artifact,
}
script_content = str(payload.get("script_content") or "")
mode = str(payload.get("mode") or "exec")
params = payload.get("params") if isinstance(payload.get("params"), dict) else {}

try:
    if profile_name:
        try:
            from aiida import load_profile
        except ModuleNotFoundError:
            pass
        else:
            load_profile(profile_name)
    exec(compile(script_content, namespace["__file__"], "exec"), namespace)
    result = None
    if mode == "call_main":
        entrypoint = namespace.get("main")
        if not callable(entrypoint):
            raise RuntimeError("Script must expose callable main(params)")
        if inspect.iscoroutinefunction(entrypoint):
            result = asyncio.run(entrypoint(params))
        else:
            maybe_result = entrypoint(params)
            if inspect.isawaitable(maybe_result):
                result = asyncio.run(maybe_result)
            else:
                result = maybe_result
    print(RESULT_MARKER + json.dumps({"success": True, "result": _normalize(result)}, ensure_ascii=True))
except Exception as exc:
    print(RESULT_MARKER + json.dumps({"success": False, "error": traceback.format_exc(), "reason": str(exc)}, ensure_ascii=True))
    raise
"""

_METADATA_BOOTSTRAP = """
import json
import sys
import traceback

payload = {
    "success": True,
    "python_interpreter_path": sys.executable,
    "python_version": sys.version.split()[0],
    "aiida_core_version": None,
    "profile": None,
    "plugins": {"calculations": [], "workflows": [], "data": []},
    "codes": [],
    "computers": [],
    "errors": [],
}

try:
    from aiida import __version__ as aiida_core_version, load_profile, orm
    from aiida.plugins.entry_point import get_entry_point_names

    payload["aiida_core_version"] = aiida_core_version

    for group, key in (
        ("aiida.calculations", "calculations"),
        ("aiida.workflows", "workflows"),
        ("aiida.data", "data"),
    ):
        try:
            payload["plugins"][key] = sorted(str(name) for name in get_entry_point_names(group))
        except Exception as exc:
            payload["success"] = False
            payload["errors"].append({"phase": "entry_points", "group": group, "reason": str(exc)})

    try:
        profile = load_profile()
        payload["profile"] = str(getattr(profile, "name", "") or "") or None
    except Exception as exc:
        payload["success"] = False
        payload["errors"].append({"phase": "load_profile", "reason": str(exc)})
        print("__ARIS_ENV_METADATA__:" + json.dumps(payload, ensure_ascii=True))
        raise SystemExit(0)

    try:
        computers = sorted(orm.Computer.collection.all(), key=lambda item: str(item.label).lower())
        payload["computers"] = [
            {
                "label": str(item.label),
                "hostname": str(getattr(item, "hostname", "") or ""),
                "description": str(getattr(item, "description", "") or "") or None,
                "scheduler_type": str(getattr(item, "scheduler_type", "") or "") or None,
                "transport_type": str(getattr(item, "transport_type", "") or "") or None,
                "is_enabled": bool(
                    getattr(item, "is_configured", False)()
                    if callable(getattr(item, "is_configured", None))
                    else getattr(item, "is_configured", False)
                ),
            }
            for item in computers
        ]
    except Exception as exc:
        payload["success"] = False
        payload["errors"].append({"phase": "computers", "reason": str(exc)})

    try:
        codes = sorted(orm.Code.collection.all(), key=lambda item: str(item.label).lower())
        serialized_codes = []
        for item in codes:
            computer_label = None
            try:
                computer = item.computer
                if computer is not None:
                    computer_label = str(computer.label)
            except Exception:
                computer_label = None
            serialized_codes.append(
                {
                    "label": str(item.label),
                    "default_plugin": str(getattr(item, "default_calc_job_plugin", "") or "") or None,
                    "computer_label": computer_label,
                }
            )
        payload["codes"] = serialized_codes
    except Exception as exc:
        payload["success"] = False
        payload["errors"].append({"phase": "codes", "reason": str(exc)})
except Exception as exc:
    payload["success"] = False
    payload["errors"].append({"phase": "bootstrap", "reason": str(exc), "traceback": traceback.format_exc()})

print("__ARIS_ENV_METADATA__:" + json.dumps(payload, ensure_ascii=True))
"""

OutputCallback = Callable[[dict[str, Any]], Awaitable[None] | None]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _domain_error(status_code: int, error: str, **extra: Any) -> WorkerDomainError:
    kinds = {
        400: "invalid_argument",
        403: "permission_denied",
        404: "not_found",
        409: "conflict",
        422: "invalid_argument",
        503: "runtime_unavailable",
    }
    return WorkerDomainError(kinds.get(status_code, "internal_error"), error, **extra)


async def _maybe_await(value: Any) -> None:
    if asyncio.iscoroutine(value) or isinstance(value, asyncio.Future):
        await value


def _normalize_workspace_path(raw_path: str | None) -> str | None:
    cleaned = str(raw_path or "").strip()
    if not cleaned:
        return None
    return str(Path(cleaned).expanduser().resolve())


def _resolve_execution_workspace_path(raw_path: str | None) -> str | None:
    normalized = _normalize_workspace_path(raw_path)
    if normalized is None:
        return None

    candidate = Path(normalized)
    if candidate.name in {"codes", "data"} and candidate.parent.exists():
        return str(candidate.parent)
    return normalized


def _absolute_path_preserving_symlink(path: Path) -> Path:
    return Path(os.path.abspath(str(path.expanduser())))


def resolve_worker_default_python_interpreter() -> Path:
    """Return the interpreter that owns this project-scoped worker process."""
    return validate_python_interpreter_path(sys.executable)


def validate_python_interpreter_path(raw_path: str) -> Path:
    cleaned = str(raw_path or "").strip()
    if not cleaned:
        raise _domain_error(400, "python_interpreter_path is required")

    target = _absolute_path_preserving_symlink(Path(cleaned))
    if not target.exists():
        raise _domain_error(404, "Python interpreter not found", python_interpreter_path=cleaned)
    if not target.is_file():
        raise _domain_error(400, "python_interpreter_path must point to a file", python_interpreter_path=cleaned)
    if os.name != "nt" and not os.access(target, os.X_OK):
        raise _domain_error(400, "Python interpreter is not executable", python_interpreter_path=cleaned)
    return target


def _load_marker_payload(line: str, marker: str) -> dict[str, Any] | None:
    if not line.startswith(marker):
        return None
    raw_payload = line[len(marker) :].strip()
    if not raw_payload:
        return None
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


@dataclass(slots=True)
class ExecutionResult:
    execution_id: str
    success: bool
    python_interpreter_path: str
    workspace_path: str | None
    returncode: int
    output: str
    stdout: str
    stderr: str
    result: Any = None
    error: str | None = None
    reason: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "execution_id": self.execution_id,
            "success": self.success,
            "python_interpreter_path": self.python_interpreter_path,
            "workspace_path": self.workspace_path,
            "returncode": self.returncode,
            "output": self.output,
            "stdout": self.stdout,
            "stderr": self.stderr,
        }
        if self.result is not None:
            payload["result"] = self.result
        if self.error:
            payload["error"] = self.error
        if self.reason:
            payload["reason"] = self.reason
        return payload


@dataclass(slots=True)
class ActiveExecution:
    execution_id: str
    process: asyncio.subprocess.Process
    python_interpreter_path: str
    workspace_path: str | None
    started_at: str
    description: str
    process_group_id: int | None = None


class DynamicExecutionRuntime:
    def __init__(self) -> None:
        self._active_executions: dict[str, ActiveExecution] = {}
        self._metadata_cache: dict[str, dict[str, Any]] = {}
        self._metadata_inflight: dict[str, asyncio.Task[dict[str, Any]]] = {}

    def status_payload(self) -> dict[str, Any]:
        items = sorted(
            (
                {
                    "execution_id": item.execution_id,
                    "python_interpreter_path": item.python_interpreter_path,
                    "workspace_path": item.workspace_path,
                    "started_at": item.started_at,
                    "description": item.description,
                    "pid": item.process.pid,
                }
                for item in self._active_executions.values()
            ),
            key=lambda item: item["started_at"],
        )
        return {
            "status": "online",
            "mode": "core-injected-executor",
            "active_executions": len(items),
            "cached_environments": len(self._metadata_cache),
            "items": items,
        }


    async def shutdown(self) -> None:
        active = list(self._active_executions.values())
        if not active:
            return
        await asyncio.gather(*(self._terminate_execution(item) for item in active), return_exceptions=True)

    async def execute_inline_script(
        self,
        *,
        script_content: str,
        python_interpreter_path: str,
        workspace_path: str | None = None,
        profile_name: str | None = None,
        on_output: OutputCallback | None = None,
    ) -> ExecutionResult:
        cleaned_script = str(script_content or "")
        if not cleaned_script.strip():
            raise _domain_error(400, "script_content is required")
        return await self._execute_script_payload(
            script_payload={
                "mode": "exec",
                "script_content": cleaned_script,
                "source_name": "<aris-inline-script>",
                "profile_name": str(profile_name or "").strip() or None,
            },
            python_interpreter_path=python_interpreter_path,
            workspace_path=workspace_path,
            description="inline-script",
            on_output=on_output,
        )

    async def execute_registered_script(
        self,
        *,
        script_name: str,
        script_content: str,
        python_interpreter_path: str,
        params: dict[str, Any] | None = None,
        workspace_path: str | None = None,
        profile_name: str | None = None,
        on_output: OutputCallback | None = None,
    ) -> ExecutionResult:
        return await self._execute_script_payload(
            script_payload={
                "mode": "call_main",
                "script_content": str(script_content or ""),
                "source_name": f"{script_name}.py",
                "params": dict(params or {}),
                "profile_name": str(profile_name or "").strip() or None,
            },
            python_interpreter_path=python_interpreter_path,
            workspace_path=workspace_path,
            description=f"registered-script:{script_name}",
            on_output=on_output,
        )

    async def inspect_environment(
        self,
        *,
        python_interpreter_path: str,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        interpreter = validate_python_interpreter_path(python_interpreter_path)
        cache_key = str(interpreter)

        if not force_refresh:
            cached = self._metadata_cache.get(cache_key)
            if cached is not None:
                return {**cached, "cached": True}

        in_flight = self._metadata_inflight.get(cache_key)
        if in_flight is not None and not force_refresh:
            payload = await in_flight
            return {**payload, "cached": False}

        task = asyncio.create_task(self._probe_environment_metadata_uncached(interpreter))
        self._metadata_inflight[cache_key] = task
        try:
            payload = await task
        finally:
            self._metadata_inflight.pop(cache_key, None)

        payload = {**payload, "cached_at": _utc_now_iso()}
        self._metadata_cache[cache_key] = payload
        return {**payload, "cached": False}

    async def _execute_script_payload(
        self,
        *,
        script_payload: dict[str, Any],
        python_interpreter_path: str,
        workspace_path: str | None,
        description: str,
        on_output: OutputCallback | None = None,
    ) -> ExecutionResult:
        interpreter = validate_python_interpreter_path(python_interpreter_path)
        normalized_workspace_path = _resolve_execution_workspace_path(workspace_path)
        execution_id = uuid4().hex
        payload_path = self._write_json_payload({**script_payload, "workspace_path": normalized_workspace_path})

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        if normalized_workspace_path:
            env[ACTIVE_WORKSPACE_ENV_VAR] = normalized_workspace_path
        else:
            env.pop(ACTIVE_WORKSPACE_ENV_VAR, None)

        process_kwargs: dict[str, Any] = {
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
            # Run from the project root so scripts under codes/ can resolve relative paths
            # such as codes/helper.py or data/results.json predictably.
            "cwd": normalized_workspace_path or None,
            "env": env,
        }
        if os.name == "nt":
            process_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            process_kwargs["start_new_session"] = True

        try:
            process = await asyncio.create_subprocess_exec(
                str(interpreter),
                "-u",
                "-c",
                _RUNNER_BOOTSTRAP,
                payload_path,
                **process_kwargs,
            )
        except FileNotFoundError as exc:
            raise _domain_error(404, "Python interpreter not found", python_interpreter_path=str(interpreter)) from exc
        except OSError as exc:
            raise _domain_error(
                500,
                "Failed to launch python interpreter",
                python_interpreter_path=str(interpreter),
                reason=str(exc),
            ) from exc

        active = ActiveExecution(
            execution_id=execution_id,
            process=process,
            python_interpreter_path=str(interpreter),
            workspace_path=normalized_workspace_path,
            started_at=_utc_now_iso(),
            description=description,
        )
        if os.name != "nt":
            with suppress(ProcessLookupError):
                active.process_group_id = os.getpgid(process.pid)
        self._active_executions[execution_id] = active

        try:
            return await self._collect_execution_result(active, on_output=on_output)
        except asyncio.CancelledError:
            await self._terminate_execution(active)
            raise
        finally:
            self._active_executions.pop(execution_id, None)
            with suppress(FileNotFoundError):
                Path(payload_path).unlink()

    async def _collect_execution_result(
        self,
        active: ActiveExecution,
        *,
        on_output: OutputCallback | None = None,
    ) -> ExecutionResult:
        if active.process.stdout is None or active.process.stderr is None:
            raise _domain_error(500, "Failed to attach subprocess streams", execution_id=active.execution_id)

        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        ordered_chunks: list[tuple[int, str]] = []
        structured_payload: dict[str, Any] | None = None
        chunk_index = count()

        async def _consume(stream: asyncio.StreamReader, stream_name: str) -> None:
            nonlocal structured_payload

            while True:
                raw_line = await stream.readline()
                if not raw_line:
                    break
                text = raw_line.decode("utf-8", errors="replace")
                marker_payload = _load_marker_payload(text.strip(), RESULT_MARKER)
                if marker_payload is not None:
                    structured_payload = marker_payload
                    continue

                ordered_chunks.append((next(chunk_index), text))
                if stream_name == "stdout":
                    stdout_chunks.append(text)
                else:
                    stderr_chunks.append(text)

                if on_output is not None:
                    await _maybe_await(
                        on_output(
                            {
                                "stream": stream_name,
                                "text": text,
                                "execution_id": active.execution_id,
                            }
                        )
                    )

        await asyncio.gather(
            _consume(active.process.stdout, "stdout"),
            _consume(active.process.stderr, "stderr"),
        )
        returncode = await active.process.wait()

        output = "".join(chunk for _, chunk in sorted(ordered_chunks, key=lambda item: item[0]))
        stdout = "".join(stdout_chunks)
        stderr = "".join(stderr_chunks)
        structured_success = bool(structured_payload.get("success")) if isinstance(structured_payload, dict) else True

        result = structured_payload.get("result") if isinstance(structured_payload, dict) else None
        error = structured_payload.get("error") if isinstance(structured_payload, dict) else None
        reason = structured_payload.get("reason") if isinstance(structured_payload, dict) else None

        success = returncode == 0 and structured_success
        if not success and not error:
            error = stderr or f"Interpreter exited with code {returncode}"

        return ExecutionResult(
            execution_id=active.execution_id,
            success=success,
            python_interpreter_path=active.python_interpreter_path,
            workspace_path=active.workspace_path,
            returncode=returncode,
            output=output,
            stdout=stdout,
            stderr=stderr,
            result=result,
            error=str(error) if error else None,
            reason=str(reason) if reason else None,
        )

    async def _probe_environment_metadata_uncached(self, interpreter: Path) -> dict[str, Any]:
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        process_kwargs: dict[str, Any] = {
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
            "env": env,
        }
        if os.name == "nt":
            process_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            process_kwargs["start_new_session"] = True

        try:
            process = await asyncio.create_subprocess_exec(
                str(interpreter),
                "-u",
                "-c",
                _METADATA_BOOTSTRAP,
                **process_kwargs,
            )
        except FileNotFoundError as exc:
            raise _domain_error(404, "Python interpreter not found", python_interpreter_path=str(interpreter)) from exc
        except OSError as exc:
            raise _domain_error(
                500,
                "Failed to launch python interpreter",
                python_interpreter_path=str(interpreter),
                reason=str(exc),
            ) from exc

        stdout_bytes, stderr_bytes = await process.communicate()
        stdout_text = stdout_bytes.decode("utf-8", errors="replace")
        stderr_text = stderr_bytes.decode("utf-8", errors="replace")

        metadata_payload: dict[str, Any] | None = None
        for line in stdout_text.splitlines():
            maybe_payload = _load_marker_payload(line.strip(), METADATA_MARKER)
            if maybe_payload is not None:
                metadata_payload = maybe_payload
                break

        if metadata_payload is None:
            raise _domain_error(
                500,
                "Environment metadata probe did not return a payload",
                python_interpreter_path=str(interpreter),
                returncode=process.returncode,
                stderr=stderr_text,
            )

        if stderr_text:
            errors = metadata_payload.setdefault("errors", [])
            if isinstance(errors, list):
                errors.append({"phase": "stderr", "reason": stderr_text})

        metadata_payload["python_interpreter_path"] = str(interpreter)
        return metadata_payload

    async def _terminate_execution(self, active: ActiveExecution) -> None:
        process = active.process
        if process.returncode is not None:
            return

        try:
            if os.name == "nt":
                process.terminate()
            elif active.process_group_id is not None:
                os.killpg(active.process_group_id, signal.SIGTERM)
            else:
                process.terminate()
            await asyncio.wait_for(process.wait(), timeout=5.0)
            return
        except (asyncio.TimeoutError, ProcessLookupError):
            pass

        try:
            if os.name == "nt":
                process.kill()
            elif active.process_group_id is not None:
                os.killpg(active.process_group_id, signal.SIGKILL)
            else:
                process.kill()
            await asyncio.wait_for(process.wait(), timeout=2.0)
        except (asyncio.TimeoutError, ProcessLookupError):
            pass

    @staticmethod
    def _write_json_payload(payload: dict[str, Any]) -> str:
        handle, path = tempfile.mkstemp(prefix="aris-exec-", suffix=".json")
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, ensure_ascii=True)
        except Exception:
            with suppress(FileNotFoundError):
                Path(path).unlink()
            raise
        return path


_EXECUTION_RUNTIME = DynamicExecutionRuntime()


def get_execution_runtime() -> DynamicExecutionRuntime:
    return _EXECUTION_RUNTIME


def worker_status_payload() -> dict[str, Any]:
    return _EXECUTION_RUNTIME.status_payload()




async def shutdown_execution_runtime() -> None:
    await _EXECUTION_RUNTIME.shutdown()
