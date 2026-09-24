from __future__ import annotations

import importlib.util
import io
import sys
import traceback
import uuid
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from inspect import iscoroutinefunction
from typing import Any, Mapping

from aiida_mcp.worker.protocol.errors import WorkerDomainError
from aiida_mcp.core.engine import (
    ensure_script_registry_dir,
    http_error,
    normalize_script_name,
    script_meta_path,
    script_path,
)
from aiida_mcp.core.utils import to_jsonable
from repository.analysis.common_utils import (
    activate_workspace_path,
    capture_saved_artifacts,
    persist_plot_artifacts,
    save_artifact,
)


def load_script_metadata(script_name: str) -> dict[str, Any]:
    metadata_path = script_meta_path(script_name)
    if not metadata_path.exists():
        return {}
    try:
        import json

        raw = metadata_path.read_text(encoding="utf-8")
        payload = json.loads(raw)
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def persist_script_metadata(script_name: str, payload: Mapping[str, Any]) -> None:
    metadata_path = script_meta_path(script_name)
    safe_payload = to_jsonable(dict(payload))
    try:
        import json

        metadata_path.write_text(json.dumps(safe_payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    except OSError as exc:
        raise http_error(500, "Failed to write script metadata", script_name=script_name, reason=str(exc)) from exc


def register_script(
    script_name: str,
    script: str,
    *,
    description: str | None = None,
    overwrite: bool = True,
) -> dict[str, Any]:
    safe_name = normalize_script_name(script_name)
    target = script_path(safe_name)
    script_text = str(script or "")
    if not script_text.strip():
        raise http_error(400, "Script content is required")

    if target.exists() and not overwrite:
        raise http_error(409, "Script already exists", script_name=safe_name)

    previous_metadata = load_script_metadata(safe_name)
    now_iso = datetime.utcnow().isoformat() + "Z"
    metadata = {
        "name": safe_name,
        "description": str(description).strip() if isinstance(description, str) and description.strip() else None,
        "created_at": previous_metadata.get("created_at") or now_iso,
        "updated_at": now_iso,
        "entrypoint": "main(params)",
    }
    if metadata["description"] is None and isinstance(previous_metadata.get("description"), str):
        old_desc = str(previous_metadata.get("description")).strip()
        if old_desc:
            metadata["description"] = old_desc

    try:
        target.write_text(script_text, encoding="utf-8")
    except OSError as exc:
        raise http_error(500, "Failed to write script", script_name=safe_name, reason=str(exc)) from exc

    persist_script_metadata(safe_name, metadata)
    return {
        "status": "registered",
        "script_name": safe_name,
        "path": str(target),
        "metadata": metadata,
    }


def list_registered_scripts() -> dict[str, Any]:
    directory = ensure_script_registry_dir()
    items: list[dict[str, Any]] = []
    for item_path in sorted(directory.glob("*.py"), key=lambda path: path.name.lower()):
        script_name = item_path.stem
        metadata = load_script_metadata(script_name)
        stat = item_path.stat()
        items.append(
            {
                "name": script_name,
                "path": str(item_path),
                "size": int(stat.st_size),
                "updated_at": metadata.get("updated_at") or datetime.utcfromtimestamp(stat.st_mtime).isoformat() + "Z",
                "description": metadata.get("description"),
                "entrypoint": metadata.get("entrypoint") or "main(params)",
            }
        )

    return {
        "count": len(items),
        "items": items,
    }


def get_registered_script(script_name: str, *, include_content: bool = True) -> dict[str, Any]:
    safe_name = normalize_script_name(script_name)
    target = script_path(safe_name)
    if not target.exists():
        raise http_error(404, "Script not found", script_name=safe_name)

    metadata = load_script_metadata(safe_name)
    payload: dict[str, Any] = {
        "name": safe_name,
        "path": str(target),
        "metadata": metadata,
    }

    if include_content:
        try:
            payload["content"] = target.read_text(encoding="utf-8")
        except OSError as exc:
            raise http_error(500, "Failed to read script content", script_name=safe_name, reason=str(exc)) from exc

    return payload


async def execute_registered_script(
    script_name: str,
    params: Mapping[str, Any] | None = None,
    *,
    workspace_path: str | None = None,
) -> dict[str, Any]:
    safe_name = normalize_script_name(script_name)
    target = script_path(safe_name)
    if not target.exists():
        raise http_error(404, "Script not found", script_name=safe_name)

    module_name = f"worker_script_{safe_name}_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, target)
    if spec is None or spec.loader is None:
        raise http_error(500, "Failed to load script module", script_name=safe_name)

    module = importlib.util.module_from_spec(spec)
    module.save_artifact = save_artifact
    module.ACTIVE_WORKSPACE_PATH = workspace_path
    output_buffer = io.StringIO()
    parsed_params = dict(params or {})

    try:
        sys.modules[module_name] = module
        with (
            activate_workspace_path(workspace_path),
            capture_saved_artifacts() as saved_artifacts,
            redirect_stdout(output_buffer),
            redirect_stderr(output_buffer),
        ):
            spec.loader.exec_module(module)
            entrypoint = getattr(module, "main", None)
            if not callable(entrypoint):
                raise http_error(400, "Script must expose callable main(params)", script_name=safe_name)

            if iscoroutinefunction(entrypoint):
                result = await entrypoint(parsed_params)
            else:
                result = entrypoint(parsed_params)
            if not saved_artifacts:
                namespace = dict(module.__dict__)
                namespace["__result__"] = result
                persist_plot_artifacts(namespace, prefix=f"{safe_name}-auto")
    except WorkerDomainError:
        raise
    except Exception as exc:  # noqa: BLE001
        return {
            "success": False,
            "script_name": safe_name,
            "output": output_buffer.getvalue(),
            "workspace_path": workspace_path,
            "error": traceback.format_exc(),
            "reason": str(exc),
        }
    finally:
        sys.modules.pop(module_name, None)

    return {
        "success": True,
        "script_name": safe_name,
        "result": to_jsonable(result),
        "output": output_buffer.getvalue() or "",
        "workspace_path": workspace_path,
        "artifacts": saved_artifacts,
    }
