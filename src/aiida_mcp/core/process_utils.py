from __future__ import annotations

from collections import defaultdict
from contextlib import suppress
import re
from typing import Any, Mapping

from aiida import orm
from aiida.common.exceptions import MissingEntryPointError
from aiida.orm import QueryBuilder

from aiida_mcp.core.engine import http_error
from aiida_mcp.core.node_utils import get_structure_formula, node_type_name, serialize_node
from aiida_mcp.core.submission_utils import _load_workflow, _prepare_and_validate
from aiida_mcp.core.utils import to_jsonable


_PARALLEL_SETTING_KEYS = {
    "num_machines",
    "num_mpiprocs_per_machine",
    "tot_num_mpiprocs",
    "num_cores_per_machine",
    "num_cores_per_mpiproc",
    "npool",
    "nk",
    "ntg",
    "ndiag",
    "withmpi",
    "queue_name",
    "account",
    "qos",
    "max_wallclock_seconds",
}

_AIIDA_NODE_UUID_PATTERN = re.compile(r"!aiida_node\s+['\"]([^'\"]+)['\"]")


def _merge_nested_dicts(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[str(key)] = _merge_nested_dicts(existing, value)
        else:
            merged[str(key)] = value
    return merged


def _is_nested_inputs_manager(value: Any) -> bool:
    if hasattr(value, "_get_keys") and hasattr(value, "_get_node_by_link_label"):
        return True
    return hasattr(value, "keys") and not isinstance(value, Mapping)


def _iterate_inputs_manager(manager: Any) -> list[tuple[str, Any]]:
    entries: list[tuple[str, Any]] = []
    if hasattr(manager, "_get_keys") and hasattr(manager, "_get_node_by_link_label"):
        try:
            for key in manager._get_keys():
                entries.append((str(key), manager._get_node_by_link_label(key)))
            return entries
        except Exception:
            entries = []

    try:
        keys = manager.keys()
    except Exception:
        return entries

    for key in keys:
        key_text = str(key).strip()
        if not key_text:
            continue
        try:
            entries.append((key_text, manager[key]))
        except Exception:
            continue
    return entries


def _append_pk_map_entry(
    entries: list[dict[str, Any]],
    seen: set[tuple[int, str]],
    *,
    pk: int,
    path: str,
    label: str,
) -> None:
    dedupe_key = (pk, path)
    if dedupe_key in seen:
        return
    seen.add(dedupe_key)
    entries.append({"pk": int(pk), "path": path, "label": label})


def _append_structure_metadata_entry(
    entries: list[dict[str, Any]],
    seen: set[tuple[int, str]],
    *,
    node: orm.StructureData,
    path: str,
) -> None:
    dedupe_key = (int(node.pk), path)
    if dedupe_key in seen:
        return
    seen.add(dedupe_key)

    atom_count: int | None = None
    with suppress(Exception):
        atom_count = len(node.sites)

    symmetry_label: str | None = None
    with suppress(Exception):
        from pymatgen.symmetry.analyzer import SpacegroupAnalyzer

        analyzer = SpacegroupAnalyzer(node.get_pymatgen_structure())
        symbol = str(analyzer.get_space_group_symbol())
        number = int(analyzer.get_space_group_number())
        symmetry_label = f"{symbol} ({number})"

    entries.append(
        {
            "pk": int(node.pk),
            "path": path,
            "label": str(node.label or get_structure_formula(node) or f"Structure #{node.pk}"),
            "formula": get_structure_formula(node),
            "symmetry": symmetry_label,
            "num_atoms": atom_count,
        }
    )


def _serialize_code_input(code: Any) -> str:
    for attr_name in ("full_label",):
        value = getattr(code, attr_name, None)
        if isinstance(value, str) and value.strip():
            return value.strip()

    label = str(getattr(code, "label", "") or "").strip()
    computer = getattr(code, "computer", None)
    computer_label = str(getattr(computer, "label", "") or "").strip() if computer is not None else ""
    if label and computer_label:
        return f"{label}@{computer_label}"
    if label:
        return label
    return str(getattr(code, "pk", "") or "").strip()


def _serialize_submission_node_value(
    node: orm.Node,
    *,
    path: str,
    pk_map_entries: list[dict[str, Any]],
    pk_map_seen: set[tuple[int, str]],
    structure_metadata: list[dict[str, Any]],
    structure_seen: set[tuple[int, str]],
    state: dict[str, Any],
) -> Any:
    if isinstance(node, orm.Dict):
        return to_jsonable(node.get_dict())
    if isinstance(node, orm.List):
        return to_jsonable(node.get_list())
    if isinstance(node, orm.Int):
        return int(node.value)
    if isinstance(node, orm.Float):
        return float(node.value)
    if isinstance(node, orm.Bool):
        return bool(node.value)
    if isinstance(node, orm.Str):
        return str(node.value)

    abstract_code = getattr(orm, "AbstractCode", None)
    if isinstance(abstract_code, type) and isinstance(node, abstract_code):
        computer = getattr(node, "computer", None)
        computer_label = str(getattr(computer, "label", "") or "").strip() if computer is not None else ""
        if computer_label and not state.get("target_computer"):
            state["target_computer"] = computer_label
        return _serialize_code_input(node)
    if isinstance(node, orm.Code):
        computer = getattr(node, "computer", None)
        computer_label = str(getattr(computer, "label", "") or "").strip() if computer is not None else ""
        if computer_label and not state.get("target_computer"):
            state["target_computer"] = computer_label
        return _serialize_code_input(node)

    _append_pk_map_entry(
        pk_map_entries,
        pk_map_seen,
        pk=int(node.pk),
        path=path,
        label=path.rsplit(".", 1)[-1] if path else "pk",
    )

    if isinstance(node, orm.StructureData):
        _append_structure_metadata_entry(
            structure_metadata,
            structure_seen,
            node=node,
            path=path,
        )

    return int(node.pk)


def _serialize_process_inputs(
    manager: Any,
    *,
    prefix: str = "",
    pk_map_entries: list[dict[str, Any]],
    pk_map_seen: set[tuple[int, str]],
    structure_metadata: list[dict[str, Any]],
    structure_seen: set[tuple[int, str]],
    state: dict[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in _iterate_inputs_manager(manager):
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, orm.Node):
            payload[key] = _serialize_submission_node_value(
                value,
                path=path,
                pk_map_entries=pk_map_entries,
                pk_map_seen=pk_map_seen,
                structure_metadata=structure_metadata,
                structure_seen=structure_seen,
                state=state,
            )
            continue
        if _is_nested_inputs_manager(value):
            payload[key] = _serialize_process_inputs(
                value,
                prefix=path,
                pk_map_entries=pk_map_entries,
                pk_map_seen=pk_map_seen,
                structure_metadata=structure_metadata,
                structure_seen=structure_seen,
                state=state,
            )
            continue
        payload[key] = to_jsonable(value)
    return payload


def _collect_parallel_settings(payload: Any) -> dict[str, Any]:
    settings: dict[str, Any] = {}

    def walk(node: Any) -> None:
        if isinstance(node, Mapping):
            for key, value in node.items():
                lowered = str(key).strip().lower()
                if lowered in _PARALLEL_SETTING_KEYS and lowered not in settings:
                    settings[lowered] = to_jsonable(value)
                if isinstance(value, Mapping):
                    walk(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, (Mapping, list)):
                            walk(item)
        elif isinstance(node, list):
            for item in node:
                if isinstance(item, (Mapping, list)):
                    walk(item)

    walk(payload)
    return settings


def _build_clone_validation_payload(entry_point: str, inputs: Mapping[str, Any]) -> dict[str, Any]:
    try:
        workflow = _load_workflow(entry_point)
        _, validation_error = _prepare_and_validate(workflow, inputs)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "VALIDATION_FAILED",
            "is_valid": False,
            "errors": [str(exc)],
            "warnings": [],
            "source": "process.clone_draft",
            "entry_point": entry_point,
        }

    if validation_error is None:
        return {
            "status": "VALIDATION_OK",
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "source": "process.clone_draft",
            "entry_point": entry_point,
        }

    message = str(getattr(validation_error, "message", "") or str(validation_error)).strip()
    return {
        "status": "VALIDATION_FAILED",
        "is_valid": False,
        "errors": [message] if message else [str(validation_error)],
        "warnings": [],
        "source": "process.clone_draft",
        "entry_point": entry_point,
        "port": str(getattr(validation_error, "port", "") or ""),
        "full_error": str(validation_error),
    }


def _get_process_summary(node: orm.ProcessNode) -> dict[str, Any]:
    process_state = getattr(node, "process_state", None)
    return {
        "pk": int(node.pk),
        "uuid": str(node.uuid),
        "type": node_type_name(node),
        "process_label": str(getattr(node, "process_label", None) or "N/A"),
        "state": process_state.value if hasattr(process_state, "value") else str(process_state or "unknown"),
        "exit_status": getattr(node, "exit_status", None),
    }


def _get_links_dict(node: orm.Node, direction: str) -> dict[str, Any]:
    links_dict: dict[str, Any] = {}
    counts: defaultdict[str, int] = defaultdict(int)
    link_manager = node.base.links.get_incoming() if direction == "incoming" else node.base.links.get_outgoing()

    for link in link_manager.all():
        label = str(link.link_label)
        unique_label = f"{label}_{counts[label]}" if counts[label] > 0 else label
        counts[label] += 1

        serialized = serialize_node(link.node)
        serialized["link_label"] = label
        serialized["pk"] = int(serialized["pk"])
        links_dict[unique_label] = serialized

    return links_dict


def _flatten_node_manager(manager: Any, prefix: str = "") -> dict[str, orm.Node]:
    result: dict[str, orm.Node] = {}

    # Handle AiiDA's NodeLinksManager (used by node.inputs/node.outputs)
    if hasattr(manager, "_get_keys") and hasattr(manager, "_get_node_by_link_label"):
        try:
            for key in manager._get_keys():
                val = manager._get_node_by_link_label(key)
                if isinstance(val, orm.Node):
                    result[f"{prefix}{key}"] = val
                elif hasattr(val, "_get_keys"): # Nested NodeLinksManager
                    result.update(_flatten_node_manager(val, prefix=f"{prefix}{key}__"))
            return result
        except Exception:
            pass

    # Fallback/Recursive for standard dict-like managers or nested namespaces
    try:
        keys = manager.keys()
    except AttributeError:
        return result

    for key in keys:
        try:
            val = manager[key]
            if isinstance(val, orm.Node):
                result[f"{prefix}{key}"] = val
            elif hasattr(val, "keys") or hasattr(val, "_get_keys"):
                result.update(_flatten_node_manager(val, prefix=f"{prefix}{key}__"))
        except Exception:
            pass
    return result


def _get_direct_links_dict(node: orm.Node, direction: str) -> dict[str, Any]:
    manager = getattr(node, "inputs" if direction == "incoming" else "outputs", None)
    if manager is None:
        return {}

    links_dict: dict[str, Any] = {}
    counts: defaultdict[str, int] = defaultdict(int)
    flat = _flatten_node_manager(manager)
    for label, port_node in flat.items():
        unique_label = f"{label}_{counts[label]}" if counts[label] > 0 else label
        counts[label] += 1

        serialized = serialize_node(port_node)
        serialized["link_label"] = label
        serialized["pk"] = int(serialized["pk"])
        links_dict[unique_label] = serialized

    return links_dict


def _safe_get_links_dict(node: orm.Node, direction: str) -> dict[str, Any]:
    try:
        return _get_links_dict(node, direction)
    except MissingEntryPointError:
        return {}


def _safe_get_direct_links_dict(node: orm.Node, direction: str) -> dict[str, Any]:
    try:
        return _get_direct_links_dict(node, direction)
    except MissingEntryPointError:
        return {}

class ProcessTree:
    """Recursively build and serialize process provenance trees."""

    def __init__(self, node: orm.ProcessNode):
        self.node = node
        self.children: dict[str, ProcessTree] = {}

        if isinstance(node, orm.WorkChainNode):
            subprocesses = sorted(node.called, key=lambda process: process.ctime)
            counts: defaultdict[str, int] = defaultdict(int)
            for sub in subprocesses:
                raw_label: Any = None
                metadata_inputs = sub.base.attributes.all.get("metadata_inputs", {})
                if isinstance(metadata_inputs, Mapping):
                    metadata = metadata_inputs.get("metadata", {})
                    if isinstance(metadata, Mapping):
                        raw_label = metadata.get("call_link_label")
                if not raw_label:
                    raw_label = getattr(sub, "process_label", "process")
                label = str(raw_label)
                unique_label = f"{label}_{counts[label]}" if counts[label] > 0 else label
                counts[label] += 1
                self.children[unique_label] = ProcessTree(sub)

    def to_dict(self) -> dict[str, Any]:
        summary = _get_process_summary(self.node)
        direct_inputs = _safe_get_direct_links_dict(self.node, "incoming")
        direct_outputs = _safe_get_direct_links_dict(self.node, "outgoing")
        summary["inputs"] = direct_inputs
        summary["outputs"] = direct_outputs
        summary["direct_inputs"] = direct_inputs
        summary["direct_outputs"] = direct_outputs
        summary["children"] = {label: child.to_dict() for label, child in self.children.items()}
        return summary


def _load_raw_node_record(identifier: int | str) -> dict[str, Any] | None:
    text_identifier = str(identifier).strip()
    if not text_identifier:
        return None

    filters: dict[str, Any]
    if text_identifier.isdigit():
        filters = {"id": int(text_identifier)}
    else:
        filters = {"uuid": text_identifier}

    qb = QueryBuilder().append(
        orm.Node,
        filters=filters,
        project=[
            "id",
            "uuid",
            "label",
            "node_type",
            "process_type",
            "ctime",
            "mtime",
            "attributes",
            "extras",
        ],
    )
    row = qb.first()
    if row is None:
        return None

    return {
        "id": int(row[0]),
        "uuid": str(row[1]),
        "label": str(row[2] or ""),
        "node_type": str(row[3] or ""),
        "process_type": str(row[4] or ""),
        "ctime": row[5],
        "mtime": row[6],
        "attributes": row[7] if isinstance(row[7], Mapping) else {},
        "extras": row[8] if isinstance(row[8], Mapping) else {},
    }


def _is_workgraph_record(record: Mapping[str, Any] | None) -> bool:
    if not isinstance(record, Mapping):
        return False
    node_type = str(record.get("node_type") or "").lower()
    process_type = str(record.get("process_type") or "").lower()
    return "workgraph" in node_type or process_type == "aiida.workflows:workgraph.engine"


def _serialize_raw_node_summary(record: Mapping[str, Any]) -> dict[str, Any]:
    attributes = record.get("attributes") if isinstance(record.get("attributes"), Mapping) else {}
    process_state = str(attributes.get("process_state") or "unknown")
    process_label = str(attributes.get("process_label") or record.get("label") or "Process")
    exit_status_value = attributes.get("exit_status")
    exit_status = int(exit_status_value) if isinstance(exit_status_value, int) else None

    return {
        "pk": int(record.get("id", 0) or 0),
        "uuid": str(record.get("uuid") or ""),
        "type": process_label,
        "node_type": str(record.get("node_type") or ""),
        "full_type": str(record.get("node_type") or ""),
        "label": str(record.get("label") or process_label or ""),
        "state": process_state,
        "process_label": process_label,
        "exit_status": exit_status,
        "ctime": record.get("ctime").isoformat() if hasattr(record.get("ctime"), "isoformat") else None,
        "mtime": record.get("mtime").isoformat() if hasattr(record.get("mtime"), "isoformat") else None,
        "preview_info": None,
    }


def _extract_aiida_node_uuid(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    match = _AIIDA_NODE_UUID_PATTERN.search(value)
    if not match:
        return None
    return str(match.group(1)).strip() or None


def _load_raw_node_records_by_uuid(uuids: list[str]) -> dict[str, dict[str, Any]]:
    normalized = sorted({str(uuid).strip() for uuid in uuids if str(uuid).strip()})
    if not normalized:
        return {}

    qb = QueryBuilder().append(
        orm.Node,
        filters={"uuid": {"in": normalized}},
        project=[
            "uuid",
            "id",
            "label",
            "node_type",
            "process_type",
            "attributes.process_label",
            "attributes.process_state",
            "attributes.exit_status",
        ],
    )

    records: dict[str, dict[str, Any]] = {}
    for uuid, pk, label, node_type, process_type, process_label, process_state, exit_status in qb.all():
        records[str(uuid)] = {
            "uuid": str(uuid),
            "pk": int(pk),
            "label": str(label or ""),
            "node_type": str(node_type or ""),
            "process_type": str(process_type or ""),
            "process_label": str(process_label or label or ""),
            "process_state": str(process_state or "unknown"),
            "exit_status": int(exit_status) if isinstance(exit_status, int) else None,
        }
    return records


def _build_workgraph_layout(node_ids: list[str], edges: list[dict[str, Any]]) -> tuple[list[str], list[str], list[list[str]]]:
    adjacency: dict[str, set[str]] = {node_id: set() for node_id in node_ids}
    indegree: dict[str, int] = {node_id: 0 for node_id in node_ids}

    for edge in edges:
        source = str(edge.get("source") or "").strip()
        target = str(edge.get("target") or "").strip()
        if not source or not target or source not in adjacency or target not in indegree:
            continue
        if target in adjacency[source]:
            continue
        adjacency[source].add(target)
        indegree[target] += 1

    roots = [node_id for node_id in node_ids if indegree.get(node_id, 0) == 0]
    queue = list(roots)
    order: list[str] = []
    level_map: dict[str, int] = {node_id: 0 for node_id in roots}

    while queue:
        source = queue.pop(0)
        order.append(source)
        source_level = level_map.get(source, 0)
        for target in sorted(adjacency.get(source, set())):
            indegree[target] -= 1
            level_map[target] = max(level_map.get(target, 0), source_level + 1)
            if indegree[target] == 0:
                queue.append(target)

    remaining = [node_id for node_id in node_ids if node_id not in order]
    for index, node_id in enumerate(remaining):
        order.append(node_id)
        level_map.setdefault(node_id, 0 if not roots else max(level_map.values(), default=0) + index)

    max_level = max(level_map.values(), default=0)
    levels: list[list[str]] = []
    for level in range(max_level + 1):
        level_nodes = [node_id for node_id in order if level_map.get(node_id, 0) == level]
        if level_nodes:
            levels.append(level_nodes)

    return roots, order, levels


def inspect_workgraph_payload(identifier: int | str, *, raw_record: Mapping[str, Any] | None = None) -> dict[str, Any]:
    record = dict(raw_record) if isinstance(raw_record, Mapping) else _load_raw_node_record(identifier)
    if record is None:
        raise http_error(404, "WorkGraph node not found", identifier=str(identifier))
    if not _is_workgraph_record(record):
        raise http_error(422, "Node is not a WorkGraph node", identifier=str(identifier), node_type=str(record.get("node_type") or ""))

    attributes = record.get("attributes") if isinstance(record.get("attributes"), Mapping) else {}
    workgraph_short = attributes.get("workgraph_data_short") if isinstance(attributes.get("workgraph_data_short"), Mapping) else {}
    short_nodes = workgraph_short.get("nodes") if isinstance(workgraph_short.get("nodes"), Mapping) else {}
    short_links = workgraph_short.get("links") if isinstance(workgraph_short.get("links"), list) else []
    task_states = attributes.get("task_states") if isinstance(attributes.get("task_states"), Mapping) else {}
    task_processes = attributes.get("task_processes") if isinstance(attributes.get("task_processes"), Mapping) else {}

    process_uuid_map: dict[str, str] = {}
    for task_id, value in task_processes.items():
        uuid = _extract_aiida_node_uuid(value)
        if uuid:
            process_uuid_map[str(task_id)] = uuid
    process_records = _load_raw_node_records_by_uuid(list(process_uuid_map.values()))

    edges: list[dict[str, Any]] = []
    for index, raw_edge in enumerate(short_links):
        if not isinstance(raw_edge, Mapping):
            continue
        source = str(raw_edge.get("from_node") or "").strip()
        target = str(raw_edge.get("to_node") or "").strip()
        if not source or not target:
            continue
        source_socket = str(raw_edge.get("from_socket") or "").strip() or None
        target_socket = str(raw_edge.get("to_socket") or "").strip() or None
        edge_label = " -> ".join(part for part in (source_socket, target_socket) if part)
        edges.append(
            {
                "id": f"{source}:{source_socket or 'out'}->{target}:{target_socket or 'in'}:{index}",
                "source": source,
                "target": target,
                "source_socket": source_socket,
                "target_socket": target_socket,
                "label": edge_label or None,
            }
        )

    node_ids = [str(task_id) for task_id in short_nodes.keys()]
    roots, topological_order, levels = _build_workgraph_layout(node_ids, edges)
    edges_by_source: dict[str, list[str]] = defaultdict(list)
    edges_by_target: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        edges_by_source[str(edge["source"])].append(str(edge["target"]))
        edges_by_target[str(edge["target"])].append(str(edge["source"]))

    nodes: list[dict[str, Any]] = []
    for task_id, raw_node in short_nodes.items():
        task_key = str(task_id)
        node_payload = raw_node if isinstance(raw_node, Mapping) else {}
        task_state = str(task_states.get(task_key) or node_payload.get("state") or "unknown")
        process_uuid = process_uuid_map.get(task_key)
        process_record = process_records.get(process_uuid or "")
        task_type = str(node_payload.get("node_type") or "NORMAL")
        label = str(node_payload.get("label") or task_key)
        position_value = node_payload.get("position")
        position = position_value if isinstance(position_value, list) else None

        nodes.append(
            {
                "id": task_key,
                "label": label,
                "identifier": str(node_payload.get("identifier") or task_key),
                "task_type": task_type,
                "state": task_state,
                "position": position,
                "children": [str(child) for child in node_payload.get("children", []) if str(child).strip()],
                "inputs": [str(item.get("name") or "") for item in node_payload.get("inputs", []) if isinstance(item, Mapping)],
                "outputs": [str(item.get("name") or "") for item in node_payload.get("outputs", []) if isinstance(item, Mapping)],
                "incoming": edges_by_target.get(task_key, []),
                "outgoing": edges_by_source.get(task_key, []),
                "process_pk": int(process_record["pk"]) if isinstance(process_record, Mapping) else None,
                "process_uuid": process_uuid,
                "process_label": str(process_record.get("process_label") or process_record.get("label") or label) if isinstance(process_record, Mapping) else None,
                "process_node_type": str(process_record.get("node_type") or "") if isinstance(process_record, Mapping) else None,
                "process_state": str(process_record.get("process_state") or task_state) if isinstance(process_record, Mapping) else None,
                "process_exit_status": process_record.get("exit_status") if isinstance(process_record, Mapping) else None,
            }
        )

    nodes.sort(key=lambda entry: topological_order.index(str(entry["id"])) if str(entry["id"]) in topological_order else len(topological_order))

    return {
        "pk": int(record.get("id", 0) or 0),
        "label": str(record.get("label") or workgraph_short.get("name") or "WorkGraph"),
        "state": str(attributes.get("process_state") or "unknown"),
        "name": str(workgraph_short.get("name") or record.get("label") or "WorkGraph"),
        "roots": roots,
        "topological_order": topological_order,
        "levels": levels,
        "nodes": nodes,
        "edges": edges,
    }


def get_process_log_payload(identifier_or_node: int | str | orm.ProcessNode) -> dict[str, Any]:
    if isinstance(identifier_or_node, orm.ProcessNode):
        node = identifier_or_node
    else:
        try:
            node = orm.load_node(identifier_or_node)
        except MissingEntryPointError:
            raw_record = _load_raw_node_record(identifier_or_node)
            if _is_workgraph_record(raw_record):
                summary = _serialize_raw_node_summary(raw_record or {})
                summary_state = str(summary.get("state") or "unknown")
                return {
                    "pk": int(summary.get("pk", 0) or 0),
                    "lines": [],
                    "reports": [],
                    "stderr_excerpt": None,
                    "text": f"No execution log stream is available for this WorkGraph node yet. Current state: {summary_state}.",
                }
            raise http_error(404, "Process node not found", identifier=str(identifier_or_node))
        except Exception as exc:  # noqa: BLE001
            raise http_error(404, "Process node not found", identifier=str(identifier_or_node), reason=str(exc)) from exc

    if not isinstance(node, orm.ProcessNode):
        raise http_error(400, "Node is not a ProcessNode", identifier=str(getattr(node, "pk", identifier_or_node)))

    lines: list[str] = []
    reports: list[str] = []

    report_rows: list[tuple[str, Any]] = []
    for log_filter_key in ("dbnode_id", "objpk"):
        try:
            qb = QueryBuilder().append(orm.Log, filters={log_filter_key: int(node.pk)}, project=["message", "time"])
            if qb.count() > 0:
                report_rows = list(qb.order_by({orm.Log: {"time": "asc"}}).all())
                break
        except Exception:  # noqa: BLE001
            continue

    if report_rows:
        lines.append("--- Reports ---")
        for message, timestamp in report_rows:
            formatted = f"[{timestamp.strftime('%H:%M:%S')}] {message}"
            reports.append(formatted)
            lines.append(formatted)

    stderr_excerpt = None
    if isinstance(node, orm.CalcJobNode) and "retrieved" in node.outputs:
        with suppress(Exception):
            stderr = node.get_scheduler_stderr()
            if stderr:
                stderr_excerpt = str(stderr)[-2000:]
                lines.append("--- Stderr ---")
                lines.append(stderr_excerpt)

    return {
        "pk": int(node.pk),
        "lines": lines,
        "reports": reports,
        "stderr_excerpt": stderr_excerpt,
        "text": "\n".join(lines) if lines else "No logs found.",
    }


def inspect_calculation_node(node: orm.ProcessNode) -> dict[str, Any]:
    if not isinstance(node, (orm.CalcJobNode, orm.CalcFunctionNode)):
        return {"error": f"Node {node.pk} is not a calculation node."}

    payload: dict[str, Any] = {
        "summary": serialize_node(node),
        "inputs": _safe_get_links_dict(node, "incoming"),
        "outputs": _safe_get_links_dict(node, "outgoing"),
        "direct_inputs": _safe_get_direct_links_dict(node, "incoming"),
        "direct_outputs": _safe_get_direct_links_dict(node, "outgoing"),
        "repository_files": [],
    }

    with suppress(Exception):
        files = node.base.repository.list_object_names()
        payload["repository_files"] = [entry for entry in files if not str(entry).startswith(".aiida")]

    if isinstance(node, orm.CalcJobNode):
        payload["scheduler_info"] = {
            "remote_workdir": node.get_remote_workdir(),
            "stdout_name": node.get_option("output_filename"),
            "stderr_name": node.get_option("error_filename"),
            "has_retrieved": "retrieved" in node.outputs,
        }

    return payload


def inspect_workchain_node(node: orm.WorkChainNode) -> dict[str, Any]:
    tree = ProcessTree(node)
    return {"provenance_tree": tree.to_dict()}


def build_process_clone_payload(identifier_or_node: int | str | orm.ProcessNode) -> dict[str, Any]:
    if isinstance(identifier_or_node, orm.ProcessNode):
        node = identifier_or_node
    else:
        try:
            node = orm.load_node(identifier_or_node)
        except Exception as exc:  # noqa: BLE001
            raise http_error(404, "Process node not found", identifier=str(identifier_or_node), reason=str(exc)) from exc

    if not isinstance(node, orm.ProcessNode):
        raise http_error(400, "Node is not a ProcessNode", identifier=str(getattr(node, "pk", identifier_or_node)))
    if not isinstance(node, orm.WorkflowNode):
        raise http_error(
            422,
            "Clone & Edit is only supported for workflow processes",
            identifier=str(getattr(node, "pk", identifier_or_node)),
            node_type=node.__class__.__name__,
        )

    entry_point = str(getattr(node, "process_type", "") or "").strip()
    if not entry_point:
        raise http_error(422, "Workflow entry point is unavailable for this process", identifier=str(node.pk))

    pk_map_entries: list[dict[str, Any]] = []
    pk_map_seen: set[tuple[int, str]] = set()
    structure_metadata: list[dict[str, Any]] = []
    structure_seen: set[tuple[int, str]] = set()
    state: dict[str, Any] = {"target_computer": None}

    inputs = _serialize_process_inputs(
        node.inputs,
        pk_map_entries=pk_map_entries,
        pk_map_seen=pk_map_seen,
        structure_metadata=structure_metadata,
        structure_seen=structure_seen,
        state=state,
    )

    metadata_inputs = node.base.attributes.all.get("metadata_inputs")
    if isinstance(metadata_inputs, Mapping):
        inputs = _merge_nested_dicts(inputs, to_jsonable(dict(metadata_inputs)))

    validation = _build_clone_validation_payload(entry_point, inputs)
    process_label = str(getattr(node, "process_label", "") or entry_point or "AiiDA Workflow").strip()
    parallel_settings = _collect_parallel_settings(inputs)

    return {
        "process_label": process_label or "AiiDA Workflow",
        "entry_point": entry_point,
        "inputs": inputs,
        "recommended_inputs": {},
        "advanced_settings": {},
        "meta": {
            "draft": {
                "entry_point": entry_point,
                "inputs": inputs,
            },
            "entry_point": entry_point,
            "workchain": entry_point,
            "workchain_entry_point": entry_point,
            "pk_map": pk_map_entries,
            "target_computer": state.get("target_computer"),
            "parallel_settings": parallel_settings,
            "structure_metadata": structure_metadata,
            "validation": validation,
            "source_process_pk": int(node.pk),
            "source_process_uuid": str(node.uuid),
        },
    }


def inspect_process_payload(identifier: str | int) -> dict[str, Any]:
    try:
        node = orm.load_node(identifier)
    except MissingEntryPointError:
        raw_record = _load_raw_node_record(identifier)
        if raw_record is None:
            raise http_error(404, "Node not found", identifier=str(identifier))
        if not _is_workgraph_record(raw_record):
            raise http_error(
                422,
                "Node entry point is unavailable and cannot be inspected",
                identifier=str(identifier),
                node_type=str(raw_record.get("node_type") or ""),
            )

        summary = _serialize_raw_node_summary(raw_record)
        return {
            "summary": summary,
            "inputs": {},
            "outputs": {},
            "direct_inputs": {},
            "direct_outputs": {},
            "logs": get_process_log_payload(str(identifier)),
        }
    except Exception as exc:  # noqa: BLE001
        raise http_error(404, "Node not found", identifier=str(identifier), reason=str(exc)) from exc

    # Use serialize_node for a consistent summary
    summary = serialize_node(node)

    payload: dict[str, Any] = {
        "summary": summary,
        "inputs": _safe_get_links_dict(node, "incoming"),
        "outputs": _safe_get_links_dict(node, "outgoing"),
        "direct_inputs": _safe_get_direct_links_dict(node, "incoming"),
        "direct_outputs": _safe_get_direct_links_dict(node, "outgoing"),
    }

    if isinstance(node, orm.ProcessNode):
        payload["logs"] = get_process_log_payload(node)

        if isinstance(node, (orm.CalcJobNode, orm.CalcFunctionNode)):
            payload["calculation"] = inspect_calculation_node(node)

        if isinstance(node, orm.WorkflowNode):
            payload["workchain"] = inspect_workchain_node(node)
    else:
        # Non-process nodes don't have logs or complex execution info
        payload["logs"] = {"pk": int(node.pk), "text": "No logs for data nodes.", "lines": [], "reports": []}

    return payload
