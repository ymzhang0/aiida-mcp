from __future__ import annotations

from copy import deepcopy
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
import threading
import time
from typing import Any

from aiida import orm
from aiida.orm import Group, Node, ProcessNode, QueryBuilder
from aiida.common.exceptions import NotExistent, MissingEntryPointError
from aiida.plugins import DataFactory
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from aiida_mcp.worker.application.group_service import _list_groups
from aiida_mcp.worker.application.registry_service import _data_entry_point_aliases
from aiida_mcp.worker.protocol.errors import WorkerDomainError
from aiida_mcp.core.engine import (
    active_profile_name,
    current_mounted_archive,
    db_access_guard,
    http_error,
    load_archive_profile,
    switch_profile,
)
from aiida_mcp.core.node_utils import get_node_summary
from aiida_mcp.core.utils import to_jsonable

_NODE_CLASS_MAP: dict[str, type[Node]] = {
    "ProcessNode": orm.ProcessNode,
    "WorkChainNode": orm.WorkChainNode,
    "StructureData": orm.StructureData,
}


# Short: agents poll process state. Stale entries still serve as a fallback when the DB is locked.
_RECENT_NODES_CACHE_TTL_SECONDS = 5
_RECENT_NODES_CACHE_MAX_ITEMS = 100
_RECENT_NODES_CACHE_LOCK = threading.RLock()
_RECENT_NODES_CACHE: dict[tuple[int, str | None, str | None, str | None, str | None, bool, float | None], tuple[float, list[dict[str, Any]]]] = {}
_SOFT_DELETED_EXTRA_KEY = "aris_soft_deleted"
_SOFT_DELETED_AT_EXTRA_KEY = "aris_soft_deleted_at"

def _resolve_node_class(node_type: str | None) -> type[Node]:
    if not node_type or not str(node_type).strip():
        return Node

    normalized = str(node_type).strip()
    if normalized in _NODE_CLASS_MAP:
        return _NODE_CLASS_MAP[normalized]

    for known_name, node_class in _NODE_CLASS_MAP.items():
        if normalized.lower() == known_name.lower():
            return node_class

    dynamic_node_class = getattr(orm, normalized, None)
    if isinstance(dynamic_node_class, type) and issubclass(dynamic_node_class, Node):
        return dynamic_node_class

    normalized_lower = normalized.lower()
    for attr_name in dir(orm):
        if attr_name.lower() != normalized_lower:
            continue
        candidate = getattr(orm, attr_name, None)
        if isinstance(candidate, type) and issubclass(candidate, Node):
            return candidate

    entry_point_candidates: list[str] = []
    aliased = _data_entry_point_aliases().get(normalized)
    if aliased:
        entry_point_candidates.append(aliased)

    if normalized_lower.endswith("data"):
        base = normalized_lower[:-4]
        if base:
            entry_point_candidates.extend([normalized_lower, f"core.{base}"])

    for entry_point in entry_point_candidates:
        with suppress(MissingEntryPointError, ValueError, TypeError, RuntimeError):
            factory_cls = DataFactory(entry_point)
            if isinstance(factory_cls, type) and issubclass(factory_cls, Node):
                return factory_cls

    supported = ", ".join(_NODE_CLASS_MAP.keys())
    raise WorkerDomainError("invalid_argument", f"Unsupported node_type '{normalized}'", supported=supported)



def _get_recent_processes(limit: int = 15, *, root_only: bool = True) -> list[dict[str, Any]]:
    qb = QueryBuilder()
    qb.append(
        ProcessNode,
        project=[
            "*",
        ],
        tag="process",
        subclassing=True,
    )
    qb.order_by({"process": {"ctime": "desc"}})
    qb.limit(max(1, int(limit)) * 6)

    try:
        with db_access_guard("recent-processes"):
            rows = qb.all()
    except WorkerDomainError:
        raise
    except SQLAlchemyTimeoutError as exc:
        raise WorkerDomainError("runtime_unavailable", "Database temporarily unavailable", endpoint="recent-processes", reason=str(exc)) from exc

    results: list[dict[str, Any]] = []

    if root_only and rows:
        node_pks = [node.pk for (node,) in rows]
        qb_caller = QueryBuilder()
        qb_caller.append(ProcessNode, filters={"id": {"in": node_pks}}, tag="child", project=["id"])
        qb_caller.append(ProcessNode, with_outgoing="child")
        has_caller_pks = {row[0] for row in qb_caller.all()}
    else:
        has_caller_pks = set()

    for (node,) in rows:
        if root_only and node.pk in has_caller_pks:
            continue
        state = getattr(node, "process_state", None)
        state_value = state.value if hasattr(state, "value") else str(state or "unknown")
        results.append(
            {
                "pk": int(node.pk),
                "label": str(getattr(node, "process_label", None) or node.label or "Unknown Task"),
                "process_label": str(getattr(node, "process_label", None) or node.label or "Unknown Task"),
                "state": state_value,
                "exit_status": getattr(node, "exit_status", None),
                "ctime": node.ctime.strftime("%Y-%m-%d %H:%M:%S") if getattr(node, "ctime", None) else None,
            }
        )
        if len(results) >= max(1, int(limit)):
            break
    return results



def _clone_recent_nodes_payload(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return deepcopy(items)



def _get_recent_nodes_cached(
    key: tuple[int, str | None, str | None, str | None, str | None, bool, float | None],
    *,
    allow_stale: bool = False,
) -> list[dict[str, Any]] | None:
    with _RECENT_NODES_CACHE_LOCK:
        record = _RECENT_NODES_CACHE.get(key)
    if record is None:
        return None
    cached_at, payload = record
    is_fresh = (time.monotonic() - cached_at) <= _RECENT_NODES_CACHE_TTL_SECONDS
    if not is_fresh and not allow_stale:
        return None
    return _clone_recent_nodes_payload(payload)



def _set_recent_nodes_cached(
    key: tuple[int, str | None, str | None, str | None, str | None, bool, float | None],
    payload: list[dict[str, Any]],
) -> None:
    with _RECENT_NODES_CACHE_LOCK:
        if len(_RECENT_NODES_CACHE) >= _RECENT_NODES_CACHE_MAX_ITEMS:
            oldest_key = min(_RECENT_NODES_CACHE.items(), key=lambda item: item[1][0])[0]
            _RECENT_NODES_CACHE.pop(oldest_key, None)
        _RECENT_NODES_CACHE[key] = (time.monotonic(), _clone_recent_nodes_payload(payload))



def _clear_recent_nodes_cache() -> None:
    with _RECENT_NODES_CACHE_LOCK:
        _RECENT_NODES_CACHE.clear()


def _normalize_recent_days(days: float | int | None) -> float | None:
    if days is None:
        return None
    try:
        normalized = float(days)
    except (TypeError, ValueError) as exc:
        raise WorkerDomainError("invalid_argument", "days must be a positive number") from exc
    if normalized <= 0:
        raise WorkerDomainError("invalid_argument", "days must be greater than zero")
    return normalized


def _serialize_ctime(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value is not None else None


def _get_node_groups(node_pks: list[int]) -> dict[int, list[dict[str, Any]]]:
    """Return every visible AiiDA group for each requested node."""
    if not node_pks:
        return {}

    try:
        with db_access_guard("recent-nodes-groups"):
            qb = QueryBuilder()
            qb.append(Node, filters={"id": {"in": node_pks}}, project="id", tag="node")
            qb.append(
                Group,
                with_node="node",
                project=["id", "uuid", "label", "type_string"],
                tag="group",
            )
            rows = qb.all()
    except Exception:
        return {}

    groups_by_node: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            continue
        node_pk, group_pk, group_uuid, group_label, group_type = row[:5]
        try:
            node_pk = int(node_pk)
            group_pk = int(group_pk)
        except (TypeError, ValueError):
            continue
        groups_by_node.setdefault(node_pk, []).append(
            {
                "pk": group_pk,
                "uuid": str(group_uuid),
                "label": str(group_label),
                "type_string": str(group_type) if group_type else None,
            }
        )
    return groups_by_node



def _is_soft_deleted(node: Node) -> bool:
    try:
        return bool(node.base.extras.get(_SOFT_DELETED_EXTRA_KEY, False))
    except Exception:
        return False
    except Exception:  # noqa: BLE001
        return False






def _soft_delete_node(pk: int, *, deleted: bool = True) -> dict[str, Any]:
    try:
        node = orm.load_node(pk=int(pk))
    except (NotExistent, ValueError, TypeError) as exc:
        raise WorkerDomainError("not_found", "Node not found", pk=int(pk)) from exc

    if deleted:
        deleted_at = int(time.time())
        node.base.extras.set(_SOFT_DELETED_EXTRA_KEY, True)
        node.base.extras.set(_SOFT_DELETED_AT_EXTRA_KEY, deleted_at)
    else:
        with suppress(Exception):
            node.base.extras.delete(_SOFT_DELETED_EXTRA_KEY)
        with suppress(Exception):
            node.base.extras.delete(_SOFT_DELETED_AT_EXTRA_KEY)

    _clear_recent_nodes_cache()
    return {"pk": int(node.pk), "soft_deleted": bool(deleted)}



def _get_recent_nodes(
    limit: int = 15,
    group_uuid: str | None = None,
    node_type: str | None = None,
    label: str | None = None,
    process_state: str | None = None,
    root_only: bool = True,
    days: float | int | None = None,
) -> list[dict[str, Any]]:
    normalized_limit = max(1, int(limit))
    normalized_group_uuid = str(group_uuid).strip() if group_uuid and str(group_uuid).strip() else None
    normalized_node_type = str(node_type).strip() if node_type and str(node_type).strip() else None
    normalized_label = str(label).strip() if label and str(label).strip() else None
    normalized_process_state = str(process_state).strip() if process_state and str(process_state).strip() else None
    normalized_days = _normalize_recent_days(days)

    cache_key = (
        normalized_limit,
        normalized_group_uuid,
        normalized_node_type,
        normalized_label,
        normalized_process_state,
        bool(root_only),
        normalized_days,
    )

    cached_payload = _get_recent_nodes_cached(cache_key)
    if cached_payload is not None:
        return cached_payload

    node_class = Node if normalized_node_type == "ProcessNode" else _resolve_node_class(normalized_node_type)
    qb = QueryBuilder()
    raw_projection = [
        "id",
        "node_type",
        "label",
        "process_type",
        "attributes.process_state",
        "attributes.process_label",
        "attributes.exit_status",
        "extras.aris_soft_deleted",
        "ctime",
        "attributes.process_status",
        "attributes.exit_message",
    ]

    if normalized_group_uuid:
        qb.append(Group, filters={"uuid": normalized_group_uuid}, tag="group")
        qb.append(node_class, with_group="group", project=raw_projection, tag="node", subclassing=True)
    else:
        qb.append(node_class, project=raw_projection, tag="node", subclassing=True)

    node_filters: dict[str, Any] = {}
    if normalized_node_type == "ProcessNode":
        node_filters["node_type"] = {"like": "process.%"}
    if normalized_label:
        if normalized_label.isdigit():
            label_filters = [
                {"id": int(normalized_label)},
                {"label": {"ilike": f"%{normalized_label}%"}},
            ]
        else:
            label_filters = [{"label": {"ilike": f"%{normalized_label}%"}}]
        if "and" not in node_filters:
            node_filters["and"] = []
        node_filters["and"].append({"or": label_filters})
    if normalized_process_state:
        node_filters["attributes.process_state"] = normalized_process_state
    if normalized_days is not None:
        node_filters["ctime"] = {">=": datetime.now(timezone.utc) - timedelta(days=normalized_days)}
    if node_filters:
        qb.add_filter("node", node_filters)

    qb.order_by({"node": {"ctime": "desc"}})
    qb.limit(max(normalized_limit * 4, normalized_limit))

    try:
        with db_access_guard("recent-nodes"):
            rows = qb.all()
    except WorkerDomainError as exc:
        stale_payload = _get_recent_nodes_cached(cache_key, allow_stale=True)
        if stale_payload is not None:
            return stale_payload
        raise exc
    except SQLAlchemyTimeoutError as exc:
        stale_payload = _get_recent_nodes_cached(cache_key, allow_stale=True)
        if stale_payload is not None:
            return stale_payload
        raise WorkerDomainError("runtime_unavailable", "Database temporarily unavailable", endpoint="recent-nodes", reason=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        stale_payload = _get_recent_nodes_cached(cache_key, allow_stale=True)
        if stale_payload is not None:
            return stale_payload
        raise WorkerDomainError("internal_error", "Failed to query recent nodes", endpoint="recent-nodes", reason=str(exc)) from exc

    def _node_type_name_from_string(raw_node_type: str | None) -> str:
        cleaned = str(raw_node_type or "").strip()
        if not cleaned:
            return "Node"
        parts = [part for part in cleaned.split(".") if part]
        return parts[-1] if parts else cleaned

    pks_list = [row[0] for row in rows if isinstance(row, (list, tuple)) and len(row) >= 1]
    has_caller_pks = set()
    if root_only and pks_list:
        try:
            with db_access_guard("recent-nodes-callers"):
                qb_caller = QueryBuilder()
                qb_caller.append(Node, filters={"id": {"in": pks_list}}, project="id", tag="called")
                qb_caller.append(Node, with_outgoing="called", edge_filters={"type": {"like": "call%"}}, project="id")
                for (called_pk, caller_pk) in qb_caller.all():
                    has_caller_pks.add(called_pk)
        except Exception:
            pass

    non_process_pks = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 8:
            continue
        pk, raw_node_type, _, _, _, _, _, raw_soft_deleted = row[:8]
        if raw_soft_deleted or (root_only and pk in has_caller_pks):
            continue
        is_process = raw_node_type and str(raw_node_type).startswith("process.")
        if not is_process:
            non_process_pks.append(int(pk))

    results: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 8:
            continue
        pk, raw_node_type, raw_label, _raw_process_type, raw_process_state, raw_process_label, raw_exit_status, raw_soft_deleted = row[:8]
        raw_ctime = row[8] if len(row) > 8 else None
        raw_process_status = row[9] if len(row) > 9 else None
        raw_exit_message = row[10] if len(row) > 10 else None
        if raw_soft_deleted:
            continue
        if root_only and pk in has_caller_pks:
            continue

        process_state_value: str | None = str(raw_process_state).strip() if raw_process_state is not None else None
        if process_state_value == "":
            process_state_value = None

        resolved_node_type = _node_type_name_from_string(raw_node_type)
        process_label = str(raw_process_label or "").strip()
        label = process_label or str(raw_label or "").strip() or resolved_node_type

        results.append(
            {
                "pk": int(pk),
                "state": process_state_value or "unknown",
                "label": str(label),
                "node_type": str(resolved_node_type),
                "process_state": process_state_value,
                "process_label": process_label or None,
                "exit_status": raw_exit_status,
                "process_status": raw_process_status or None,
                "exit_message": raw_exit_message or None,
                "ctime": _serialize_ctime(raw_ctime),
                "groups": [],
                "formula": None,
                "preview_info": None,
            }
        )
        if len(results) >= normalized_limit:
            break

    pks_to_load = [r["pk"] for r in results]
    if pks_to_load:
        from aiida import orm
        from aiida_mcp.core.node_utils import get_structure_formula, build_node_preview
        try:
            with db_access_guard("recent-nodes-bulk-load"):
                loaded_nodes = {node.pk: node for node in orm.load_nodes(pks_to_load)}
                for result in results:
                    node = loaded_nodes.get(result["pk"])
                    if node:
                        result["formula"] = get_structure_formula(node)
                        result["preview_info"] = build_node_preview(node)
        except Exception:
            pass

    groups_by_node = _get_node_groups(pks_to_load)
    for result in results:
        result["groups"] = groups_by_node.get(result["pk"], [])

    _set_recent_nodes_cached(cache_key, results)
    return results



def _get_unified_source_map(target: str | None = None) -> dict[str, Any]:
    if target and str(target).strip():
        cleaned = str(target).strip()
        candidate = Path(cleaned).expanduser()
        if candidate.exists() and candidate.is_file() and candidate.suffix.lower() in {".aiida", ".zip"}:
            load_archive_profile(str(candidate))
        else:
            switch_profile(cleaned)

    groups = _list_groups()
    mounted_archive = current_mounted_archive()
    source_name = mounted_archive if mounted_archive else active_profile_name()
    source_type = "archive" if mounted_archive else "profile"

    return {
        "name": str(Path(source_name).name if source_type == "archive" else source_name),
        "type": source_type,
        "current_profile": active_profile_name(),
        "groups": [{"label": item["label"], "pk": item["pk"]} for item in groups],
    }



def _get_bands_plot_data(pk: int) -> dict[str, Any]:
    try:
        node = orm.load_node(pk)
    except Exception as exc:  # noqa: BLE001
        raise WorkerDomainError("not_found", "Node not found", pk=pk, reason=str(exc)) from exc

    if not isinstance(node, orm.BandsData) and not hasattr(node, "_matplotlib_get_dict"):
        raise WorkerDomainError("invalid_argument", "Node is not a compatible BandsData type", pk=pk)

    try:
        if hasattr(node, "_matplotlib_get_dict"):
            return {
                "pk": int(node.pk),
                "data": to_jsonable(node._matplotlib_get_dict()),
            }
    except Exception as exc:  # noqa: BLE001
        raise WorkerDomainError("internal_error", "Failed to extract bands data", pk=pk, reason=str(exc)) from exc

    raise WorkerDomainError("internal_error", "Bands data extraction failed", pk=pk)



def _list_remote_files(pk: int | str) -> dict[str, Any]:
    try:
        node = orm.load_node(pk)
    except Exception as exc:  # noqa: BLE001
        raise WorkerDomainError("not_found", "Node not found", pk=str(pk), reason=str(exc)) from exc

    if not isinstance(node, orm.RemoteData):
        raise WorkerDomainError("invalid_argument", "Node is not RemoteData", pk=str(pk), type=node.__class__.__name__)

    try:
        files = node.listdir()
    except Exception as exc:  # noqa: BLE001
        raise WorkerDomainError("internal_error", "Failed to list remote files", pk=str(pk), reason=str(exc)) from exc

    return {
        "pk": int(node.pk),
        "files": to_jsonable(files),
    }



def _get_remote_file_content(pk: int | str, filename: str) -> dict[str, Any]:
    try:
        node = orm.load_node(pk)
    except Exception as exc:  # noqa: BLE001
        raise WorkerDomainError("not_found", "Node not found", pk=str(pk), reason=str(exc)) from exc

    if not isinstance(node, orm.RemoteData):
        raise WorkerDomainError("invalid_argument", "Node is not RemoteData", pk=str(pk), type=node.__class__.__name__)

    target = str(filename or "").strip()
    if not target:
        raise WorkerDomainError("invalid_argument", "Filename is required")

    try:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmpdir:
            destination = Path(tmpdir) / target
            destination.parent.mkdir(parents=True, exist_ok=True)
            node.getfile(target, str(destination))
            content = destination.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        raise WorkerDomainError("internal_error", "Failed to read remote file", pk=str(pk), filename=target, reason=str(exc)) from exc

    return {
        "pk": int(node.pk),
        "filename": target,
        "content": content,
    }



def _list_repository_files(pk: int | str, source: str = "folder") -> dict[str, Any]:
    try:
        node = orm.load_node(pk)
    except Exception as exc:  # noqa: BLE001
        raise WorkerDomainError("not_found", "Node not found", pk=str(pk), reason=str(exc)) from exc

    mode = str(source or "folder").strip().lower()

    try:
        if mode in {"repository", "virtual.repository"}:
            files = node.base.repository.list_object_names()
        else:
            files = node.list_object_names()
    except Exception as exc:  # noqa: BLE001
        raise http_error(
            500,
            "Failed to list node files",
            pk=str(pk),
            source=source,
            reason=str(exc),
        ) from exc

    return {
        "pk": int(node.pk),
        "source": mode,
        "files": to_jsonable(sorted([str(entry) for entry in files])),
    }



def _get_node_file_content(pk: int | str, filename: str, source: str = "folder") -> dict[str, Any]:
    try:
        node = orm.load_node(pk)
    except Exception as exc:  # noqa: BLE001
        raise WorkerDomainError("not_found", "Node not found", pk=str(pk), reason=str(exc)) from exc

    target = str(filename or "").strip()
    if not target:
        raise WorkerDomainError("invalid_argument", "Filename is required")

    mode = str(source or "folder").strip().lower()

    try:
        if mode in {"repository", "virtual.repository"}:
            raw = node.base.repository.get_object_content(target)
        else:
            raw = node.get_object_content(target)
    except Exception as exc:  # noqa: BLE001
        raise http_error(
            500,
            "Failed to read node file",
            pk=str(pk),
            filename=target,
            source=source,
            reason=str(exc),
        ) from exc

    if isinstance(raw, bytes):
        content = raw.decode("utf-8", errors="replace")
    else:
        content = str(raw)

    return {
        "pk": int(node.pk),
        "filename": target,
        "source": source,
        "content": content,
    }
