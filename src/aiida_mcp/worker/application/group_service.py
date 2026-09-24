from __future__ import annotations

import base64
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any
from uuid import uuid4

from aiida import orm
from aiida.orm import Group, Node, QueryBuilder
from aiida.common.exceptions import NotExistent
from aiida.tools.archive import create_archive
from aiida_mcp.core.engine import db_access_guard, get_profile_default_user
from aiida_mcp.core.node_utils import build_node_preview, node_type_name
from aiida_mcp.core.utils import to_jsonable
from aiida_mcp.worker.protocol.errors import WorkerDomainError

def _clear_recent_nodes_cache():
    from aiida_mcp.worker.application.node_service import _clear_recent_nodes_cache as clear_cache
    clear_cache()

def _list_group_labels(search_string: str | None = None) -> list[str]:
    qb = QueryBuilder()
    filters = {"label": {"like": f"%{search_string}%"}} if search_string else {}
    qb.append(Group, project=["label", "*"], filters=filters)

    with db_access_guard("groups-labels"):
        rows = qb.all()

    labels: list[str] = []
    for label, group in rows:
        if getattr(group, "type_string", "") == "core.import":
            continue
        labels.append(str(label))

    return sorted(set(labels), key=str.lower)



def _list_groups(search: str | None = None) -> list[dict[str, Any]]:
    qb = QueryBuilder()
    filters = {"label": {"like": f"%{search}%"}} if search else {}
    qb.append(Group, project=["label", "id", "*"], filters=filters)

    with db_access_guard("groups"):
        rows = qb.all()

        group_counts = {}
        try:
            from aiida.manage.manager import get_manager
            from sqlalchemy import text
            session = get_manager().get_profile_storage().get_session()
            group_pks = [pk for label, pk, group in rows if getattr(group, "type_string", "") != "core.import"]
            if group_pks:
                placeholders = ",".join(str(int(pk)) for pk in group_pks)
                query = text(f"SELECT dbgroup_id, count(dbnode_id) FROM db_dbgroup_dbnodes WHERE dbgroup_id IN ({placeholders}) GROUP BY dbgroup_id")
                for gid, count in session.execute(query).fetchall():
                    group_counts[gid] = count
        except Exception:
            pass

    items: list[dict[str, Any]] = []
    for label, pk, group in rows:
        if getattr(group, "type_string", "") == "core.import":
            continue
        count = group_counts.get(pk)
        if count is None:
            try:
                count = int(group.count())
            except Exception:
                count = 0

        items.append(
            {
                "label": str(label),
                "pk": int(pk),
                "uuid": str(group.uuid),
                "count": count,
            }
        )

    return sorted(items, key=lambda item: item["label"].lower())



def _inspect_group(group_uuid: str, limit: int = 20) -> dict[str, Any]:
    """Inspect one group by its immutable AiiDA UUID."""
    cleaned_uuid = str(group_uuid or "").strip()
    if not cleaned_uuid:
        raise WorkerDomainError("invalid_argument", "group_uuid is required")

    try:
        group = orm.load_group(uuid=cleaned_uuid)
    except (NotExistent, ValueError, TypeError) as exc:
        raise WorkerDomainError("not_found", "Group not found", group_uuid=cleaned_uuid) from exc
    nodes = list(group.nodes)[: max(1, int(limit))]

    serialized_nodes: list[dict[str, Any]] = []
    for node in nodes:
        process_state = getattr(node, "process_state", None)
        state_value = process_state.value if hasattr(process_state, "value") else str(process_state or "N/A")
        preview = build_node_preview(node)
        serialized_nodes.append(
            {
                "pk": int(node.pk),
                "uuid": str(node.uuid),
                "label": str(node.label or node_type_name(node)),
                "type": node_type_name(node),
                "full_type": str(node.node_type),
                "process_label": str(getattr(node, "process_label", "N/A") or "N/A"),
                "process_state": state_value,
                "exit_status": getattr(node, "exit_status", None),
                "ctime": node.ctime.strftime("%Y-%m-%d %H:%M:%S") if getattr(node, "ctime", None) else None,
                "mtime": node.mtime.strftime("%Y-%m-%d %H:%M:%S") if getattr(node, "mtime", None) else None,
                "attributes": to_jsonable(node.base.attributes.all),
                "extras": to_jsonable(node.base.extras.all),
                "preview_info": preview,
            }
        )

    return {
        "group": {
            "label": str(group.label),
            "pk": int(group.pk),
            "uuid": str(group.uuid),
            "size": int(len(group.nodes)),
            "type_string": str(getattr(group, "type_string", "")),
        },
        "nodes": serialized_nodes,
    }


def _group_contains_node(group_uuid: str, identifier: str | int) -> dict[str, Any]:
    """Return exact membership without materializing or truncating the Group."""
    cleaned_uuid = str(group_uuid or "").strip()
    cleaned_identifier = str(identifier or "").strip()
    if not cleaned_uuid or not cleaned_identifier:
        raise WorkerDomainError(
            "invalid_argument", "group_uuid and identifier are required"
        )

    try:
        group = orm.load_group(uuid=cleaned_uuid)
    except (NotExistent, ValueError, TypeError) as exc:
        raise WorkerDomainError(
            "not_found", "Group not found", group_uuid=cleaned_uuid
        ) from exc
    try:
        node = orm.load_node(int(cleaned_identifier) if cleaned_identifier.isdigit() else cleaned_identifier)
    except (NotExistent, ValueError, TypeError) as exc:
        raise WorkerDomainError(
            "not_found", "Node not found", identifier=cleaned_identifier
        ) from exc

    qb = QueryBuilder()
    qb.append(Group, filters={"id": int(group.pk)}, tag="group")
    qb.append(Node, with_group="group", filters={"id": int(node.pk)}, project="id")
    with db_access_guard("group-contains-node"):
        member = qb.first() is not None

    return {
        "group_uuid": str(group.uuid),
        "node_pk": int(node.pk),
        "node_uuid": str(node.uuid),
        "member": member,
    }



def _serialize_group_item(group: Group) -> dict[str, Any]:
    return {
        "label": str(group.label),
        "pk": int(group.pk),
        "uuid": str(group.uuid),
        "count": int(len(group.nodes)),
        "type_string": str(getattr(group, "type_string", "")) or None,
    }



def _load_group_by_pk(pk: int) -> Group:
    try:
        return orm.load_group(pk=int(pk))
    except (NotExistent, ValueError, TypeError) as exc:
        raise WorkerDomainError("not_found", "Group not found", pk=int(pk)) from exc



def _create_group(label: str) -> dict[str, Any]:
    cleaned = str(label or "").strip()
    if not cleaned:
        raise WorkerDomainError("invalid_argument", "Group label is required")

    existing = Group.collection.find(filters={"label": cleaned})
    if existing:
        raise WorkerDomainError("conflict", "Group already exists", label=cleaned)

    default_user = get_profile_default_user()
    group = Group(label=cleaned, user=default_user).store() if default_user is not None else Group(label=cleaned).store()
    _clear_recent_nodes_cache()
    return _serialize_group_item(group)



def _rename_group(pk: int, label: str) -> dict[str, Any]:
    group = _load_group_by_pk(pk)
    cleaned = str(label or "").strip()
    if not cleaned:
        raise WorkerDomainError("invalid_argument", "Group label is required")

    if cleaned != str(group.label):
        existing = Group.collection.find(filters={"label": cleaned})
        if any(int(candidate.pk) != int(group.pk) for candidate in existing):
            raise WorkerDomainError("conflict", "Group already exists", label=cleaned)
        group.label = cleaned
        _clear_recent_nodes_cache()

    return _serialize_group_item(group)



def _delete_group(pk: int) -> dict[str, Any]:
    group = _load_group_by_pk(pk)
    payload = _serialize_group_item(group)
    Group.collection.delete(int(group.pk))
    _clear_recent_nodes_cache()
    return {"status": "deleted", **payload}



def _add_nodes_to_group(pk: int, node_pks: list[int]) -> dict[str, Any]:
    group = _load_group_by_pk(pk)
    normalized_ids: list[int] = []
    seen: set[int] = set()
    for raw_pk in node_pks:
        try:
            parsed = int(raw_pk)
        except (TypeError, ValueError):
            continue
        if parsed <= 0 or parsed in seen:
            continue
        seen.add(parsed)
        normalized_ids.append(parsed)

    if not normalized_ids:
        return {"group": _serialize_group_item(group), "added": [], "missing": []}

    existing_node_ids = {int(node.pk) for node in group.nodes}
    nodes_to_add: list[Node] = []
    added_ids: list[int] = []
    missing_ids: list[int] = []

    for node_pk in normalized_ids:
        if node_pk in existing_node_ids:
            continue
        try:
            node = orm.load_node(node_pk)
        except (NotExistent, ValueError, TypeError):
            missing_ids.append(node_pk)
            continue
        nodes_to_add.append(node)
        added_ids.append(node_pk)

    if nodes_to_add:
        group.add_nodes(nodes_to_add)
        _clear_recent_nodes_cache()

    return {
        "group": _serialize_group_item(group),
        "added": added_ids,
        "missing": missing_ids,
    }



def _remove_node_from_group(pk: int, node_pk: int) -> dict[str, Any]:
    group = _load_group_by_pk(pk)
    try:
        node = orm.load_node(int(node_pk))
    except (NotExistent, ValueError, TypeError) as exc:
        raise WorkerDomainError("not_found", "Node not found", pk=int(node_pk)) from exc

    existing_node_ids = {int(n.pk) for n in group.nodes}
    if int(node.pk) not in existing_node_ids:
        return {"group": _serialize_group_item(group), "removed": False}

    group.remove_nodes([node])
    _clear_recent_nodes_cache()
    return {"group": _serialize_group_item(group), "removed": True}





def _sanitize_archive_name(filename: str) -> str:
    safe = Path(filename).name.replace(" ", "_")
    cleaned = "".join(ch for ch in safe if ch.isalnum() or ch in {"-", "_", "."})
    if not cleaned:
        cleaned = "group-export"
    if not cleaned.endswith(".aiida"):
        cleaned = f"{cleaned}.aiida"
    return cleaned


def _export_group_archive(pk: int) -> dict[str, Any]:
    group = _load_group_by_pk(pk)
    archive_name = _sanitize_archive_name(group.label or f"group-{pk}")
    archive_path = Path(tempfile.gettempdir()) / f"aris-{uuid4().hex}-{archive_name}"
    try:
        create_archive(
            entities=[group, *list(group.nodes)],
            filename=archive_path,
            overwrite=True,
        )
        content = archive_path.read_bytes()
    except Exception as exc:  # noqa: BLE001
        raise WorkerDomainError(
            "internal_error",
            "Failed to export group archive",
            pk=int(pk),
            reason=str(exc),
        ) from exc
    finally:
        with suppress(FileNotFoundError):
            archive_path.unlink()

    return {
        "filename": archive_name,
        "media_type": "application/octet-stream",
        "content_base64": base64.b64encode(content).decode("ascii"),
    }
