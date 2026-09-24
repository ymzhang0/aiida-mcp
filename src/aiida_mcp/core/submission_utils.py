from __future__ import annotations

import copy
import inspect
from dataclasses import dataclass, field
from itertools import product
from typing import Any, Mapping, Sequence, get_args, get_origin

from aiida import orm
from aiida.common.exceptions import MissingEntryPointError
from aiida.engine import submit
from aiida.engine.processes.ports import InputPort, PortNamespace
from aiida.plugins import WorkflowFactory

from aiida_mcp.core.engine import (
    cleanup_storage_session,
    http_error,
    prime_storage_user_context,
    reset_storage_backend_caches,
)
from aiida_mcp.core.utils import serialize_spec, to_jsonable, type_to_string
from aiida_mcp.worker.protocol.errors import WorkerDomainError


_MISSING = object()
_BATCH_ASSIGNMENT_ROOTS = ("inputs", "intent_data", "overrides")
_DEFAULT_BATCH_MAX_JOBS = 1000
def _serialize_builder_code_input(code: Any) -> str:
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


def _serialize_builder_node_value(node: orm.Node) -> Any:
    if isinstance(node, orm.Dict):
        try:
            return to_jsonable(node.get_dict())
        except Exception:  # noqa: BLE001
            return {}
    if isinstance(node, orm.List):
        try:
            return to_jsonable(node.get_list())
        except Exception:  # noqa: BLE001
            return []
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
        return _serialize_builder_code_input(node)
    if isinstance(node, orm.Code):
        return _serialize_builder_code_input(node)

    raw_pk = getattr(node, "pk", None)
    pk_value = int(raw_pk) if isinstance(raw_pk, int) else None
    label = str(getattr(node, "label", "") or "").strip()
    return {
        "pk": pk_value,
        "uuid": str(getattr(node, "uuid", "") or ""),
        "type": node.__class__.__name__,
        "label": label,
    }


def _serialize_builder_inputs_payload(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, orm.Node):
        return _serialize_builder_node_value(value)

    if value.__class__.__name__.startswith("ProcessBuilderNamespace"):
        return {str(key): _serialize_builder_inputs_payload(item) for key, item in value.items()}

    if isinstance(value, Mapping):
        return {str(key): _serialize_builder_inputs_payload(item) for key, item in value.items()}

    if isinstance(value, (list, tuple, set, frozenset)):
        return [_serialize_builder_inputs_payload(item) for item in value]

    return to_jsonable(value)


@dataclass(slots=True)
class BatchContext:
    items: list[dict[str, Any]] = field(default_factory=list)
    structures: list[Any] = field(default_factory=list)
    structure_path: str | None = None
    parameter_grid: dict[str, list[Any]] = field(default_factory=dict)
    matrix_mode: str = "product"
    max_jobs: int = _DEFAULT_BATCH_MAX_JOBS
    source_group: dict[str, Any] | None = None

    def summary(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "matrix_mode": self.matrix_mode,
            "structure_path": self.structure_path,
            "structure_count": len(self.structures),
            "parameter_paths": list(self.parameter_grid.keys()),
            "item_count": len(self.items),
            "max_jobs": self.max_jobs,
        }
        if self.source_group:
            payload["source_group"] = dict(self.source_group)
        return payload


def _coerce_positive_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _serialize_batch_payload(payload: Any) -> Any:
    return _serialize_builder_inputs_payload(payload)


def _normalize_batch_assignment_path(path: Any, *, default_root: str | None) -> str:
    cleaned = str(path or "").strip()
    if not cleaned:
        raise http_error(400, "Batch assignment path is required")

    root = cleaned.split(".", 1)[0]
    if root in _BATCH_ASSIGNMENT_ROOTS or default_root is None:
        return cleaned
    return f"{default_root}.{cleaned}"


def _set_dotted_mapping_value(payload: dict[str, Any], path: str, value: Any) -> None:
    segments = [segment for segment in str(path).split(".") if segment]
    if not segments:
        raise http_error(400, "Batch assignment path is required")

    cursor = payload
    for segment in segments[:-1]:
        existing = cursor.get(segment)
        if not isinstance(existing, dict):
            existing = {}
            cursor[segment] = existing
        cursor = existing
    cursor[segments[-1]] = copy.deepcopy(value)


def _deep_merge_mappings(target: dict[str, Any], patch: Mapping[str, Any]) -> dict[str, Any]:
    for key, value in patch.items():
        key_text = str(key)
        if isinstance(value, Mapping) and isinstance(target.get(key_text), dict):
            _deep_merge_mappings(target[key_text], value)
            continue
        target[key_text] = copy.deepcopy(value)
    return target


def _apply_batch_fragment(
    payload: dict[str, Any],
    fragment: Mapping[str, Any],
    *,
    default_root: str | None,
) -> dict[str, Any]:
    direct_top_level_keys = {
        "entry_point",
        "protocol",
        "draft",
        "batch",
        "batch_context",
    }
    for raw_key, raw_value in fragment.items():
        key = str(raw_key).strip()
        if not key:
            continue

        if key in _BATCH_ASSIGNMENT_ROOTS and isinstance(raw_value, Mapping):
            current = payload.get(key)
            if not isinstance(current, dict):
                current = {}
                payload[key] = current
            _deep_merge_mappings(current, raw_value)
            continue

        if key in direct_top_level_keys:
            payload[key] = copy.deepcopy(raw_value)
            continue

        if "." in key or key not in _BATCH_ASSIGNMENT_ROOTS:
            assignment_path = _normalize_batch_assignment_path(key, default_root=default_root)
            _set_dotted_mapping_value(payload, assignment_path, raw_value)
            continue

        payload[key] = copy.deepcopy(raw_value)

    return payload


def _load_group_for_batch(reference: Any) -> orm.Group:
    if isinstance(reference, orm.Group):
        return reference

    if isinstance(reference, Mapping):
        for key in ("pk", "group_pk", "id"):
            parsed = _coerce_positive_int(reference.get(key))
            if parsed is not None:
                try:
                    return orm.load_group(parsed)
                except Exception as exc:  # noqa: BLE001
                    raise http_error(404, "Batch group not found", group=parsed, reason=str(exc)) from exc

        for key in ("uuid", "group_uuid", "label", "group_label", "group"):
            value = reference.get(key)
            if value in (None, ""):
                continue
            try:
                return orm.load_group(value)
            except Exception:
                try:
                    return orm.Group.collection.get(label=str(value).strip())
                except Exception as exc:  # noqa: BLE001
                    raise http_error(404, "Batch group not found", group=value, reason=str(exc)) from exc

    if isinstance(reference, (int, str)):
        try:
            return orm.load_group(reference)
        except Exception:
            try:
                return orm.Group.collection.get(label=str(reference).strip())
            except Exception as exc:  # noqa: BLE001
                raise http_error(404, "Batch group not found", group=reference, reason=str(exc)) from exc

    raise http_error(400, "Batch structures must be a node list or a valid group reference")


def _resolve_batch_structures(raw_structures: Any) -> tuple[list[Any], dict[str, Any] | None]:
    if raw_structures is None:
        return [], None

    if isinstance(raw_structures, Mapping) and isinstance(raw_structures.get("nodes"), Sequence) and not isinstance(raw_structures.get("nodes"), (str, bytes, bytearray)):
        nodes = list(raw_structures.get("nodes") or [])
        return nodes, None

    if isinstance(raw_structures, Sequence) and not isinstance(raw_structures, (str, bytes, bytearray, Mapping)):
        return list(raw_structures), None

    group = _load_group_for_batch(raw_structures)
    nodes = sorted(
        list(getattr(group, "nodes", [])),
        key=lambda node: _coerce_positive_int(getattr(node, "pk", None)) or 0,
    )
    if not nodes:
        raise http_error(400, "Batch group contains no nodes", group=getattr(group, "label", None) or getattr(group, "pk", None))
    return nodes, {
        "pk": _coerce_positive_int(getattr(group, "pk", None)),
        "label": str(getattr(group, "label", "") or "").strip() or None,
        "count": len(nodes),
    }


def _normalize_batch_axis_values(name: str, value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        values = list(value)
    else:
        raise http_error(400, f"Batch field '{name}' must be an array")

    if not values:
        raise http_error(400, f"Batch field '{name}' cannot be empty")
    return values


def _parse_batch_context(
    batch_data: Mapping[str, Any],
    *,
    default_root: str | None,
    default_structure_path: str | None,
) -> BatchContext:
    raw_items = batch_data.get("items") or []
    if raw_items and (not isinstance(raw_items, Sequence) or isinstance(raw_items, (str, bytes, bytearray))):
        raise http_error(400, "Batch field 'items' must be an array of objects")

    items: list[dict[str, Any]] = []
    for index, item in enumerate(raw_items):
        if not isinstance(item, Mapping):
            raise http_error(400, "Batch item must be an object", index=index)
        items.append(dict(item))

    structures, source_group = _resolve_batch_structures(batch_data.get("structures"))
    structure_path = batch_data.get("structure_path") or batch_data.get("structure_field") or default_structure_path
    if structures:
        structure_path = _normalize_batch_assignment_path(structure_path, default_root=default_root)

    raw_parameter_grid = batch_data.get("parameter_grid") or batch_data.get("parameters") or {}
    if raw_parameter_grid and not isinstance(raw_parameter_grid, Mapping):
        raise http_error(400, "Batch field 'parameter_grid' must be an object")

    parameter_grid: dict[str, list[Any]] = {}
    for raw_key, raw_value in dict(raw_parameter_grid).items():
        parameter_grid[_normalize_batch_assignment_path(raw_key, default_root=default_root)] = _normalize_batch_axis_values(
            str(raw_key),
            raw_value,
        )

    matrix_mode = str(batch_data.get("matrix_mode") or "product").strip().lower()
    if matrix_mode not in {"product", "zip"}:
        raise http_error(400, "Batch field 'matrix_mode' must be 'product' or 'zip'")

    max_jobs = _coerce_positive_int(batch_data.get("max_jobs")) or _DEFAULT_BATCH_MAX_JOBS

    context = BatchContext(
        items=items,
        structures=structures,
        structure_path=structure_path,
        parameter_grid=parameter_grid,
        matrix_mode=matrix_mode,
        max_jobs=max_jobs,
        source_group=source_group,
    )
    if not context.items and not context.structures and not context.parameter_grid:
        raise http_error(400, "Batch payload must include 'items', 'structures', or 'parameter_grid'")
    return context


def _build_batch_axis_assignments(batch_context: BatchContext) -> list[list[tuple[str, Any]]]:
    axes: list[tuple[str, list[Any]]] = []
    if batch_context.structures:
        if not batch_context.structure_path:
            raise http_error(400, "Batch structures require 'structure_path' or 'structure_field'")
        axes.append((batch_context.structure_path, list(batch_context.structures)))

    for path, values in batch_context.parameter_grid.items():
        axes.append((path, list(values)))

    if not axes:
        return [[]]

    lengths = {len(values) for _, values in axes}
    if any(length == 0 for length in lengths):
        raise http_error(400, "Batch axes cannot be empty")

    if batch_context.matrix_mode == "zip":
        if len(lengths) > 1:
            raise http_error(400, "Batch zip mode requires all axes to have the same length")
        shared_length = next(iter(lengths))
        return [[(path, values[index]) for path, values in axes] for index in range(shared_length)]

    axis_paths = [path for path, _values in axes]
    axis_values = [values for _path, values in axes]
    return [list(zip(axis_paths, values)) for values in product(*axis_values)]


def _expand_batch_requests(
    base_payload: Mapping[str, Any],
    batch_context: BatchContext,
    *,
    default_root: str | None,
) -> list[dict[str, Any]]:
    item_fragments = batch_context.items or [{}]
    axis_assignments = _build_batch_axis_assignments(batch_context)
    total_jobs = len(item_fragments) * len(axis_assignments)
    if total_jobs > batch_context.max_jobs:
        raise http_error(
            400,
            "Batch expansion exceeds max_jobs",
            total_jobs=total_jobs,
            max_jobs=batch_context.max_jobs,
        )

    expanded: list[dict[str, Any]] = []
    for item_fragment in item_fragments:
        for assignments in axis_assignments:
            job_payload = copy.deepcopy(dict(base_payload))
            if item_fragment:
                _apply_batch_fragment(job_payload, item_fragment, default_root=default_root)
            for path, value in assignments:
                _set_dotted_mapping_value(job_payload, path, value)
            expanded.append(job_payload)
    return expanded


def _extract_submission_pk(payload: Any) -> int | None:
    if not isinstance(payload, Mapping):
        return None
    for key in ("pk", "submitted_pk", "process_pk", "workflow_pk"):
        parsed = _coerce_positive_int(payload.get(key))
        if parsed is not None:
            return parsed
    submitted = payload.get("submitted_pks")
    if isinstance(submitted, Sequence) and not isinstance(submitted, (str, bytes, bytearray)):
        for item in submitted:
            parsed = _coerce_positive_int(item)
            if parsed is not None:
                return parsed
    return None


def batch_submit(
    submitter: Any,
    *,
    base_payload: Mapping[str, Any] | None = None,
    batch_data: Mapping[str, Any] | None = None,
    requests: Sequence[Mapping[str, Any]] | None = None,
    default_root: str | None = None,
    default_structure_path: str | None = None,
) -> dict[str, Any]:
    expanded_requests: list[dict[str, Any]]
    batch_context_summary: dict[str, Any] = {}

    if requests is not None:
        expanded_requests = []
        for index, item in enumerate(requests):
            if not isinstance(item, Mapping):
                raise http_error(400, "Batch request item must be an object", index=index)
            expanded_requests.append(dict(item))
        batch_context_summary = {
            "request_count": len(expanded_requests),
            "mode": "explicit_requests",
        }
    else:
        if base_payload is None or batch_data is None:
            raise http_error(400, "Batch submission requires either 'requests' or a base payload with batch context")
        batch_context = _parse_batch_context(
            batch_data,
            default_root=default_root,
            default_structure_path=default_structure_path,
        )
        expanded_requests = _expand_batch_requests(
            base_payload,
            batch_context,
            default_root=default_root,
        )
        batch_context_summary = batch_context.summary()

    if not expanded_requests:
        raise http_error(400, "Batch submission produced no requests")

    submitted_pks: list[int] = []
    responses: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for index, request_payload in enumerate(expanded_requests):
        serialized_request = _serialize_batch_payload(request_payload)
        try:
            response = submitter(request_payload)
            normalized_response = _serialize_batch_payload(response)
            submitted_pk = _extract_submission_pk(response)
            if submitted_pk is not None:
                submitted_pks.append(submitted_pk)
            responses.append(
                {
                    "index": index,
                    "request": serialized_request,
                    "response": normalized_response,
                }
            )
        except WorkerDomainError as exc:
            detail = exc.detail if isinstance(exc.detail, Mapping) else {"error": _failure_message_from_domain_error(exc)}
            failures.append(
                {
                    "index": index,
                    "request": serialized_request,
                    "status_code": int(exc.status_code),
                    "detail": to_jsonable(detail),
                }
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {
                    "index": index,
                    "request": serialized_request,
                    "status_code": 500,
                    "detail": {"error": str(exc)},
                }
            )
        finally:
            cleanup_storage_session()
            reset_storage_backend_caches()
            prime_storage_user_context()

    return {
        "status": "SUBMITTED_BATCH",
        "total": len(expanded_requests),
        "submitted_count": len(responses),
        "failed_count": len(failures),
        "submitted_pks": submitted_pks,
        "process_pks": list(submitted_pks),
        "responses": responses,
        "failures": failures,
        "batch_context": _serialize_batch_payload(batch_context_summary),
    }


def _extract_valid_types(port: InputPort | PortNamespace) -> tuple[type[Any], ...]:
    valid_type = getattr(port, "valid_type", None)
    if valid_type is None:
        return ()
    if isinstance(valid_type, tuple):
        return tuple(entry for entry in valid_type if isinstance(entry, type))
    if isinstance(valid_type, type):
        return (valid_type,)
    return ()


def _expects_node(port: InputPort | PortNamespace) -> bool:
    for port_type in _extract_valid_types(port):
        if issubclass(port_type, orm.Node):
            return True
    return False


def _is_node_pk_candidate(
    port: InputPort | PortNamespace,
    value: Any,
    *,
    allow_scalar_pk_resolution: bool = True,
) -> bool:
    if not allow_scalar_pk_resolution:
        return False
    if not isinstance(value, (int, str)):
        return False
    # If the value is an empty string, it's not a valid candidate for PK UUID resolution
    if isinstance(value, str) and not value.strip():
        return False
    if not _expects_node(port):
        return False
    if isinstance(value, int) and value <= 0:
        return False
    return True


def _resolve_node_reference(
    port: InputPort | PortNamespace,
    value: Any,
    path: Sequence[str],
    *,
    allow_scalar_pk_resolution: bool = True,
) -> Any:
    # Handle dictionary representation of a Node payload (having 'pk' or 'uuid')
    if isinstance(value, Mapping):
        if _expects_node(port) and ("pk" in value or "uuid" in value) and set(value.keys()).issubset({"pk", "uuid", "type", "label", "node_type"}):
            raw_pk = value.get("pk") or value.get("uuid")
            if raw_pk:
                try:
                    return orm.load_node(raw_pk)
                except Exception as exc:  # noqa: BLE001
                    joined = ".".join(path)
                    raise ValueError(f"Could not load node for '{joined}' with id={raw_pk}: {exc}") from exc
        return value

    # Automatically map scalar ints/strings into AiiDA nodes if the port strictly expects a Node
    if _is_node_pk_candidate(port, value, allow_scalar_pk_resolution=allow_scalar_pk_resolution):
        try:
            # Code nodes are often referenced by "code@computer" labels, which need
            # load_code handling instead of plain load_node resolution.
            valid_types = _extract_valid_types(port)
            expects_code = any(issubclass(t, orm.AbstractCode) for t in valid_types if isinstance(t, type))
            expects_group = any(issubclass(t, orm.Group) for t in valid_types if isinstance(t, type))

            if expects_code and isinstance(value, str):
                try:
                    loaded_node = orm.load_code(value)
                except Exception:
                    loaded_node = orm.load_node(value)
            elif expects_group and isinstance(value, str):
                try:
                    loaded_node = orm.load_group(value)
                except Exception:
                    try:
                        loaded_node = orm.Group.collection.get(label=value)
                    except Exception:
                        loaded_node = orm.load_node(value)
            else:
                loaded_node = orm.load_node(value)
        except Exception as exc:  # noqa: BLE001
            # If resolution fails, and the port actually supports scalar DB types (like Int/Str/List)
            # return the raw scalar cleanly instead of crashing as it might just be a standard python value
            scalar_node_types = (orm.Int, orm.Float, orm.Bool, orm.Str, orm.Dict, orm.List)
            if any(port_type in scalar_node_types for port_type in _extract_valid_types(port)):
                return value
            joined = ".".join(path)
            raise ValueError(f"Could not load node for '{joined}' with pk={value}: {exc}") from exc

        valid_types = _extract_valid_types(port)
        if valid_types and not isinstance(loaded_node, valid_types):
            joined = ".".join(path)
            expected = type_to_string(valid_types)
            raise ValueError(
                f"Loaded node pk={value} for '{joined}' has type {loaded_node.__class__.__name__}, expected {expected}"
            )
        return loaded_node

    return value


def resolve_generic_inputs(
    namespace: PortNamespace,
    raw_inputs: Mapping[str, Any],
    path: Sequence[str] = ("inputs",),
    *,
    allow_scalar_pk_resolution: bool = True,
) -> dict[str, Any]:
    """
    Recursively traverse raw workflow inputs. Automatically load AiiDA nodes via UUID or PK
    if the target port is determined to expect an AiiDA Node instead of primitive maps.
    """
    resolved: dict[str, Any] = {}

    for key, value in raw_inputs.items():
        port = namespace.get(key)
        child_path = (*path, key)

        if isinstance(port, PortNamespace) and isinstance(value, Mapping):
            resolved[key] = resolve_generic_inputs(
                port,
                value,
                child_path,
                allow_scalar_pk_resolution=allow_scalar_pk_resolution,
            )
            continue

        if port is not None:
            resolved[key] = _resolve_node_reference(
                port,
                value,
                child_path,
                allow_scalar_pk_resolution=allow_scalar_pk_resolution,
            )
        else:
            resolved[key] = value

    return resolved


def _load_workflow(entry_point: str) -> Any:
    cleaned = str(entry_point or "").strip()
    if not cleaned:
        raise http_error(400, "Workflow entry point is required")

    try:
        return WorkflowFactory(cleaned)
    except MissingEntryPointError as exc:
        raise http_error(404, "Workflow entry point not found", entry_point=cleaned, reason=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise http_error(400, "Failed to load workflow entry point", entry_point=cleaned, reason=str(exc)) from exc



def _prepare_and_validate(process: Any, raw_inputs: Mapping[str, Any]) -> tuple[dict[str, Any], Any]:
    spec_inputs = process.spec().inputs
    resolved_inputs = resolve_generic_inputs(spec_inputs, raw_inputs)

    try:
        processed_inputs = spec_inputs.pre_process(dict(resolved_inputs))
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Failed to pre-process inputs: {exc}") from exc

    spec = process.spec()
    validation_error = None
    used_process_validate = False
    validate_method = getattr(spec, "validate", None)
    if callable(validate_method):
        try:
            validation_error = validate_method(dict(processed_inputs))
            used_process_validate = True
        except Exception:
            used_process_validate = False
    if not used_process_validate:
        validation_error = spec_inputs.validate(processed_inputs)
    return dict(processed_inputs), validation_error


def _merge_nested_inputs(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[str(key)] = _merge_nested_inputs(existing, value)
        else:
            merged[str(key)] = value
    return merged


def _safe_pk_value(entity: Any) -> int | None:
    raw = getattr(entity, "pk", None)
    if isinstance(raw, int):
        return int(raw)
    return None


def _serialize_computer_summary(computer: orm.Computer) -> dict[str, Any]:
    return {
        "pk": _safe_pk_value(computer),
        "uuid": str(getattr(computer, "uuid", "") or "") or None,
        "label": str(getattr(computer, "label", "") or "") or None,
        "hostname": str(getattr(computer, "hostname", "") or "") or None,
    }


def _serialize_code_summary(code: orm.AbstractCode) -> dict[str, Any]:
    computer_payload: dict[str, Any] | None = None
    try:
        computer = getattr(code, "computer", None)
        if isinstance(computer, orm.Computer):
            computer_payload = _serialize_computer_summary(computer)
    except Exception:  # noqa: BLE001
        computer_payload = None

    return {
        "pk": _safe_pk_value(code),
        "uuid": str(getattr(code, "uuid", "") or "") or None,
        "label": str(getattr(code, "label", "") or "") or None,
        "type": code.__class__.__name__,
        "computer": computer_payload,
    }


def _collect_job_resources(
    value: Any,
    *,
    codes_by_key: dict[str, dict[str, Any]],
    computers_by_key: dict[str, dict[str, Any]],
) -> None:
    if isinstance(value, Mapping):
        for nested in value.values():
            _collect_job_resources(nested, codes_by_key=codes_by_key, computers_by_key=computers_by_key)
        return

    if isinstance(value, (list, tuple, set, frozenset)):
        for nested in value:
            _collect_job_resources(nested, codes_by_key=codes_by_key, computers_by_key=computers_by_key)
        return

    if isinstance(value, orm.AbstractCode):
        code_payload = _serialize_code_summary(value)
        code_key = str(code_payload.get("uuid") or code_payload.get("pk") or id(value))
        codes_by_key[code_key] = code_payload

        computer_payload = code_payload.get("computer")
        if isinstance(computer_payload, Mapping):
            computer_key = str(computer_payload.get("uuid") or computer_payload.get("pk") or f"code:{code_key}")
            computers_by_key[computer_key] = dict(computer_payload)
        return

    if isinstance(value, orm.Computer):
        computer_payload = _serialize_computer_summary(value)
        computer_key = str(computer_payload.get("uuid") or computer_payload.get("pk") or id(value))
        computers_by_key[computer_key] = computer_payload


def _build_job_validation_summary(inputs: Mapping[str, Any]) -> dict[str, Any]:
    codes_by_key: dict[str, dict[str, Any]] = {}
    computers_by_key: dict[str, dict[str, Any]] = {}
    _collect_job_resources(inputs, codes_by_key=codes_by_key, computers_by_key=computers_by_key)

    codes = list(codes_by_key.values())
    computers = list(computers_by_key.values())
    primary_code = codes[0] if codes else None
    primary_computer = None

    if isinstance(primary_code, Mapping):
        maybe_primary_computer = primary_code.get("computer")
        if isinstance(maybe_primary_computer, Mapping):
            primary_computer = dict(maybe_primary_computer)
    if primary_computer is None and computers:
        primary_computer = computers[0]

    return {
        "inputs": to_jsonable(inputs),
        "code": primary_code,
        "computer": primary_computer,
        "codes": codes,
        "computers": computers,
    }




# -----------------------------------------------------------------------------
# Dynamic protocol builder inspection and assembly
# -----------------------------------------------------------------------------

def _normalize_lookup_key(value: str) -> str:
    return "".join(char for char in str(value).lower() if char.isalnum())


def _find_intent_value(name: str, intent_data: Mapping[str, Any]) -> Any:
    if name in intent_data:
        direct = intent_data[name]
        if direct is not None:
            return direct

    normalized_target = _normalize_lookup_key(name)
    for key, value in intent_data.items():
        if _normalize_lookup_key(key) == normalized_target and value is not None:
            return value

    suffixes = ("pk", "label", "uuid", "node", "id")
    for key, value in intent_data.items():
        normalized_key = _normalize_lookup_key(key)
        for suffix in suffixes:
            if (
                value is not None
                and (
                    normalized_key == f"{normalized_target}{suffix}"
                    or normalized_target == f"{normalized_key}{suffix}"
                )
            ):
                return value

    return _MISSING


def _is_required_parameter(parameter: inspect.Parameter) -> bool:
    if parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
        return False
    return parameter.default is inspect._empty


def _resolve_code_reference(value: Any, *, argument_name: str) -> orm.AbstractCode:
    if isinstance(value, orm.AbstractCode):
        return value
    try:
        return orm.load_code(value)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Failed to resolve protocol argument '{argument_name}' as AiiDA Code: {exc}") from exc


def _resolve_node_reference_value(
    value: Any,
    *,
    argument_name: str,
    expected_types: tuple[type[Any], ...] | None = None,
) -> orm.Node:
    loaded_node: orm.Node
    if isinstance(value, orm.Node):
        loaded_node = value
    elif isinstance(value, Mapping):
        if "pk" in value:
            loaded_node = orm.load_node(value.get("pk"))
        elif "uuid" in value:
            loaded_node = orm.load_node(value.get("uuid"))
        else:
            raise ValueError(
                f"Protocol argument '{argument_name}' expects node-like reference mapping with 'pk' or 'uuid'."
            )
    else:
        loaded_node = orm.load_node(value)

    if expected_types:
        if not isinstance(loaded_node, expected_types):
            expected = type_to_string(expected_types)
            raise ValueError(
                f"Protocol argument '{argument_name}' resolved node type {loaded_node.__class__.__name__}, expected {expected}."
            )
    return loaded_node


def _extract_annotation_types(annotation: Any) -> tuple[type[Any], ...]:
    if annotation is inspect._empty or isinstance(annotation, str):
        return ()
    if isinstance(annotation, type):
        return (annotation,)

    origin = get_origin(annotation)
    if origin is None:
        return ()

    collected: list[type[Any]] = []
    for arg in get_args(annotation):
        collected.extend(_extract_annotation_types(arg))
    return tuple(collected)


def _annotation_includes_subclass(annotation: Any, target: type[Any]) -> bool:
    for candidate in _extract_annotation_types(annotation):
        try:
            if issubclass(candidate, target):
                return True
        except Exception:  # noqa: BLE001
            continue
    return False



def _sanitize_overrides_for_spec(
    namespace: PortNamespace,
    overrides: Mapping[str, Any],
    path: Sequence[str] = ("overrides",),
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    sanitized: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []

    for raw_key, value in overrides.items():
        key = str(raw_key)
        child_path = (*path, key)
        port = namespace.get(key)

        if port is None:
            errors.append(
                {
                    "stage": "invalid_override",
                    "port": ".".join(child_path),
                    "message": "Override key does not exist in workchain input spec",
                }
            )
            continue

        if isinstance(port, PortNamespace):
            if not isinstance(value, Mapping):
                errors.append(
                    {
                        "stage": "invalid_override_type",
                        "port": ".".join(child_path),
                        "message": "Expected object value for PortNamespace override",
                    }
                )
                continue
            nested, nested_errors = _sanitize_overrides_for_spec(port, value, child_path)
            sanitized[key] = nested
            errors.extend(nested_errors)
            continue

        sanitized[key] = value

    return sanitized, errors


def _collect_required_port_paths(
    namespace: PortNamespace,
    payload: Mapping[str, Any],
    prefix: Sequence[str] = ()
) -> list[tuple[str, ...]]:
    required: list[tuple[str, ...]] = []

    for key, port in namespace.items():
        path = (*prefix, str(key))
        if path and path[0] == "metadata":
            continue

        port_present = key in payload and payload[key] is not None

        if isinstance(port, PortNamespace):
            # If the namespace is required OR it is present in payload, check its children
            is_required = bool(getattr(port, "required", False))
            if is_required or port_present:
                sub_payload = payload.get(key)
                if not isinstance(sub_payload, Mapping):
                    sub_payload = {}
                required.extend(_collect_required_port_paths(port, sub_payload, path))
        else:
            # Atomic port
            is_required = bool(getattr(port, "required", False))
            if is_required:
                required.append(path)

    return required


def _path_exists(payload: Mapping[str, Any], path: Sequence[str]) -> bool:
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping) or key not in current:
            return False
        current = current[key]
    return current is not None


def _list_missing_required_ports(namespace: PortNamespace, payload: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    required_paths = _collect_required_port_paths(namespace, payload)

    for path in required_paths:
        if not _path_exists(payload, path):
            missing.append(".".join(path))
    return missing


def _infer_resource_domain(*values: Any) -> str | None:
    joined = " ".join(str(value or "") for value in values).strip().lower()
    if not joined:
        return None
    if "entry point" in joined or "workflowfactory" in joined:
        return "entry_point"
    if "computer" in joined or "scheduler" in joined:
        return "computer"
    if "code" in joined:
        return "code"
    if "group" in joined:
        return "group"
    if "pseudo" in joined or "upf" in joined:
        return "pseudo"
    if "structure" in joined:
        return "structure"
    if "node" in joined or "pk=" in joined or "uuid" in joined:
        return "node"
    return None


def _build_recovery_plan(
    *,
    entry_point: str,
    errors: Sequence[Mapping[str, Any]],
    missing_ports: Sequence[str],
    required_ports: Sequence[str],
) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    issue_keys: set[tuple[str, str, str]] = set()
    resource_domains: set[str] = set()

    def append_issue(
        issue_type: str,
        *,
        message: str,
        port: str | None = None,
        stage: str | None = None,
        resource_domain: str | None = None,
    ) -> None:
        normalized_message = str(message or "").strip()
        if not normalized_message:
            return
        key = (issue_type, str(port or "").strip(), normalized_message)
        if key in issue_keys:
            return
        issue_keys.add(key)
        issue: dict[str, Any] = {
            "type": issue_type,
            "message": normalized_message,
        }
        if port:
            issue["port"] = port
        if stage:
            issue["stage"] = stage
        if resource_domain:
            issue["resource_domain"] = resource_domain
            resource_domains.add(resource_domain)
        issues.append(issue)

    if missing_ports:
        preview = ", ".join(str(port) for port in missing_ports[:6])
        if len(missing_ports) > 6:
            preview += ", ..."
        append_issue(
            "missing_required_inputs",
            message=f"Required inputs are still missing after builder construction: {preview}",
            stage="missing_required_ports",
        )

    for error in errors:
        stage = str(error.get("stage") or "").strip()
        port = str(error.get("port") or "").strip() or None
        message = str(error.get("message") or error.get("reason") or error).strip()
        lowered = message.lower()
        resource_domain = _infer_resource_domain(port, message)

        if stage == "missing_protocol_argument":
            append_issue("missing_protocol_argument", message=message, port=port, stage=stage, resource_domain=resource_domain)
            continue
        if stage in {"resolve_protocol_argument", "resolve_overrides"}:
            issue_type = "resource_reference_unresolved" if "could not load" in lowered or "not found" in lowered else "input_resolution_error"
            append_issue(issue_type, message=message, port=port, stage=stage, resource_domain=resource_domain)
            continue
        if stage == "unsupported_overrides":
            append_issue("unsupported_override", message=message, port=port, stage=stage)
            continue
        if "missing entry point" in lowered or resource_domain == "entry_point":
            append_issue("entry_point_unavailable", message=message, port=port, stage=stage or "load_workflow", resource_domain="entry_point")
            continue
        if "could not load" in lowered or "not found" in lowered or "does not exist" in lowered or "no such" in lowered:
            append_issue("resource_reference_unresolved", message=message, port=port, stage=stage, resource_domain=resource_domain)
            continue
        if stage == "validate":
            append_issue("validation_error", message=message, port=port, stage=stage, resource_domain=resource_domain)
            continue
        append_issue("builder_construction_error", message=message, port=port, stage=stage, resource_domain=resource_domain)

    recommended_actions: list[dict[str, Any]] = []
    seen_actions: set[str] = set()

    def append_action(action: str, reason: str) -> None:
        normalized_action = str(action or "").strip()
        normalized_reason = str(reason or "").strip()
        if not normalized_action or normalized_action in seen_actions:
            return
        seen_actions.add(normalized_action)
        recommended_actions.append({"action": normalized_action, "reason": normalized_reason})

    issue_types = {issue["type"] for issue in issues}
    if issue_types & {
        "missing_required_inputs",
        "missing_protocol_argument",
        "input_resolution_error",
        "unsupported_override",
        "validation_error",
    }:
        append_action(
            "inspect_spec",
            "Review the WorkChain spec and builder signature before changing inputs.",
        )
    if "entry_point_unavailable" in issue_types:
        append_action(
            "inspect_available_workchains",
            "Confirm that the requested WorkChain entry point is installed in the current worker.",
        )
    if resource_domains & {"code", "computer"}:
        append_action(
            "inspect_resources",
            "Check the active AiiDA profile for matching computers and codes before retrying.",
        )
    if resource_domains & {"group", "pseudo", "structure", "node"} or missing_ports:
        append_action(
            "inspect_database_inputs",
            "Check whether the required nodes, groups, or data objects exist in the current profile.",
        )
    if issues:
        append_action(
            "ask_user",
            "Do not substitute missing inputs or resources silently; confirm the user's preferred fix.",
        )
        append_action(
            "stop_if_unresolved",
            "If the required input or resource cannot be found, stop the submission path and report the blocker.",
        )

    summary_parts: list[str] = []
    if missing_ports:
        summary_parts.append(f"Missing required inputs: {', '.join(str(port) for port in missing_ports[:5])}")
    elif "entry_point_unavailable" in issue_types:
        summary_parts.append("The requested WorkChain is not available in the current worker.")
    elif resource_domains:
        summary_parts.append("One or more referenced AiiDA resources could not be resolved.")
    elif issues:
        summary_parts.append(str(issues[0].get("message") or "Builder validation failed."))
    else:
        summary_parts.append("Review builder diagnostics before retrying.")

    if required_ports:
        summary_parts.append(f"Required spec ports: {', '.join(str(port) for port in required_ports[:6])}")

    return {
        "status": "blocked" if issues else "ready",
        "entry_point": entry_point,
        "summary": " ".join(summary_parts),
        "issues": issues,
        "missing_ports": list(missing_ports),
        "required_ports": list(required_ports),
        "resource_domains": sorted(resource_domains),
        "recommended_actions": recommended_actions,
        "user_decision_required": bool(issues),
    }


def _failure_message_from_domain_error(exc: WorkerDomainError) -> str:
    detail = exc.detail
    if isinstance(detail, Mapping):
        return str(detail.get("reason") or detail.get("error") or detail.get("message") or exc)
    return str(detail or exc)


def _build_failed_protocol_builder_result(
    *,
    entry_point: str,
    protocol: str | None,
    intent_data: Mapping[str, Any],
    overrides: Mapping[str, Any],
    errors: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    normalized_errors = [dict(item) for item in errors]
    recovery_plan = _build_recovery_plan(
        entry_point=entry_point,
        errors=normalized_errors,
        missing_ports=[],
        required_ports=[],
    )
    return {
        "success": False,
        "entry_point": entry_point,
        "protocol": protocol,
        "intent_data": to_jsonable(intent_data),
        "overrides": to_jsonable(overrides),
        "protocol_kwargs": {},
        "signature": [],
        "pseudo_expectations": [],
        "required_ports": [],
        "missing_ports": [],
        "builder": None,
        "builder_inputs": {},
        "errors": normalized_errors,
        "recovery_plan": recovery_plan,
    }


def _coerce_builder_argument(
    name: str,
    raw_value: Any,
    parameter: inspect.Parameter,
    spec_inputs: PortNamespace,
) -> Any:
    normalized_name = name.lower()
    port = spec_inputs.get(name)

    expects_code = _annotation_includes_subclass(parameter.annotation, orm.AbstractCode)
    if not expects_code:
        expects_code = normalized_name == "code" or normalized_name.endswith("_code")

    if expects_code:
        return _resolve_code_reference(raw_value, argument_name=name)

    expected_node_types: tuple[type[Any], ...] = ()
    if _annotation_includes_subclass(parameter.annotation, orm.Node):
        expected_node_types = tuple(
            candidate
            for candidate in _extract_annotation_types(parameter.annotation)
            if isinstance(candidate, type) and issubclass(candidate, orm.Node)
        )
    elif port is not None:
        valid_types = _extract_valid_types(port)
        if any(issubclass(t, orm.Node) for t in valid_types if isinstance(t, type)):
            expected_node_types = tuple(t for t in valid_types if isinstance(t, type))

    if expected_node_types:
        return _resolve_node_reference_value(raw_value, argument_name=name, expected_types=expected_node_types)

    return raw_value


def _extract_builder_request(draft_data: Mapping[str, Any]) -> tuple[str, str | None, dict[str, Any], dict[str, Any]]:
    entry_point = str(draft_data.get("entry_point") or "").strip()
    if not entry_point:
        raise http_error(400, "Builder payload requires 'entry_point'")

    protocol_raw = draft_data.get("protocol", "moderate")
    protocol = str(protocol_raw) if protocol_raw is not None else None

    intent_data: dict[str, Any] = {}
    provided_intent = draft_data.get("intent_data")
    if isinstance(provided_intent, Mapping):
        intent_data.update(dict(provided_intent))

    raw_overrides = draft_data.get("overrides") or {}
    if not isinstance(raw_overrides, Mapping):
        raise http_error(400, "Builder payload field 'overrides' must be an object")

    return entry_point, protocol, intent_data, dict(raw_overrides)


def _inspect_builder_protocol_signature(workchain: Any) -> dict[str, inspect.Parameter]:
    if not hasattr(workchain, "get_builder_from_protocol"):
        raise http_error(400, "WorkChain does not define get_builder_from_protocol")

    try:
        signature = inspect.signature(workchain.get_builder_from_protocol)
    except Exception as exc:  # noqa: BLE001
        raise http_error(500, "Failed to inspect get_builder_from_protocol signature", reason=str(exc)) from exc

    parameters: dict[str, inspect.Parameter] = {}
    for name, parameter in signature.parameters.items():
        if name in {"self", "cls"}:
            continue
        parameters[name] = parameter
    return parameters


def _build_dynamic_protocol_builder(draft_data: Mapping[str, Any]) -> dict[str, Any]:
    reset_storage_backend_caches()
    prime_storage_user_context()
    entry_point, protocol, intent_data, overrides = _extract_builder_request(draft_data)
    try:
        workchain = _load_workflow(entry_point)
    except WorkerDomainError as exc:
        return _build_failed_protocol_builder_result(
            entry_point=entry_point,
            protocol=protocol,
            intent_data=intent_data,
            overrides=overrides,
            errors=[
                {
                    "stage": "load_workflow",
                    "port": entry_point,
                    "message": _failure_message_from_domain_error(exc),
                }
            ],
        )

    try:
        parameters = _inspect_builder_protocol_signature(workchain)
    except WorkerDomainError as exc:
        return _build_failed_protocol_builder_result(
            entry_point=entry_point,
            protocol=protocol,
            intent_data=intent_data,
            overrides=overrides,
            errors=[
                {
                    "stage": "inspect_protocol_signature",
                    "port": entry_point,
                    "message": _failure_message_from_domain_error(exc),
                }
            ],
        )

    spec_inputs = workchain.spec().inputs
    errors: list[dict[str, Any]] = []
    required_ports = _list_missing_required_ports(spec_inputs, {})

    sanitized_overrides, override_errors = _sanitize_overrides_for_spec(spec_inputs, overrides)
    errors.extend(override_errors)

    try:
        resolved_overrides = resolve_generic_inputs(
            spec_inputs,
            sanitized_overrides,
            allow_scalar_pk_resolution=True,
        )
    except ValueError as exc:
        errors.append(
            {
                "stage": "resolve_overrides",
                "message": str(exc),
            }
        )
        resolved_overrides = sanitized_overrides

    kwargs: dict[str, Any] = {}

    for name, parameter in parameters.items():
        if parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            continue

        if name == "protocol":
            if protocol is not None:
                kwargs[name] = protocol
            continue

        if name == "overrides":
            kwargs[name] = resolved_overrides
            continue

        raw_value = _find_intent_value(name, intent_data)
        if raw_value is _MISSING:
            if _is_required_parameter(parameter):
                errors.append(
                    {
                        "stage": "missing_protocol_argument",
                        "port": name,
                        "message": f"Missing required protocol argument '{name}'",
                    }
                )
            continue

        try:
            kwargs[name] = _coerce_builder_argument(name, raw_value, parameter, spec_inputs)
        except ValueError as exc:
            errors.append(
                {
                    "stage": "resolve_protocol_argument",
                    "port": name,
                    "message": str(exc),
                }
            )

    if "overrides" not in parameters and resolved_overrides:
        errors.append(
            {
                "stage": "unsupported_overrides",
                "port": "overrides",
                "message": "This WorkChain does not accept 'overrides' in get_builder_from_protocol",
            }
        )

    builder = None
    builder_inputs: dict[str, Any] = {}
    missing_ports: list[str] = []

    if not errors:
        try:
            builder = workchain.get_builder_from_protocol(**kwargs)
            builder_inputs = builder._inputs(prune=True)
        except Exception as exc:  # noqa: BLE001
            errors.append(
                {
                    "stage": "get_builder_from_protocol",
                    "message": str(exc),
                }
            )

    if not builder_inputs:
        try:
            fallback_builder = workchain.get_builder()
            builder_inputs = fallback_builder._inputs(prune=True)
        except Exception:
            pass

    if builder is not None:
        missing_ports = _list_missing_required_ports(spec_inputs, builder_inputs)
        for missing in missing_ports:
            errors.append(
                {
                    "stage": "missing_required_ports",
                    "port": missing,
                    "message": f"Required input '{missing}' is missing after builder construction",
                }
            )

        if not missing_ports:
            try:
                _processed_inputs, validation_error = _prepare_and_validate(workchain, builder_inputs)
                if validation_error is not None:
                    errors.append(
                        {
                            "stage": "validate",
                            "port": str(getattr(validation_error, "port", "") or ""),
                            "message": str(getattr(validation_error, "message", "") or str(validation_error)),
                            "full_error": str(validation_error),
                        }
                    )
            except ValueError as exc:
                errors.append(
                    {
                        "stage": "validate",
                        "message": str(exc),
                    }
                )

    signature_payload: list[dict[str, Any]] = []
    for name, parameter in parameters.items():
        signature_payload.append(
            {
                "name": name,
                "kind": str(parameter.kind),
                "required": _is_required_parameter(parameter),
                "default": None if parameter.default is inspect._empty else to_jsonable(parameter.default),
                "annotation": None
                if parameter.annotation is inspect._empty
                else str(getattr(parameter.annotation, "__name__", parameter.annotation)),
            }
        )

    recovery_plan = _build_recovery_plan(
        entry_point=entry_point,
        errors=errors,
        missing_ports=missing_ports,
        required_ports=required_ports,
    )

    return {
        "success": len(errors) == 0,
        "entry_point": entry_point,
        "protocol": protocol,
        "intent_data": to_jsonable(intent_data),
        "overrides": to_jsonable(resolved_overrides),
        "protocol_kwargs": to_jsonable(kwargs),
        "signature": signature_payload,
        "pseudo_expectations": [],  # Generic WorkChains don't have QE specific hardcoding
        "required_ports": required_ports,
        "missing_ports": missing_ports,
        "builder": builder,
        "builder_inputs": _serialize_builder_inputs_payload(builder_inputs),
        "errors": errors,
        "recovery_plan": recovery_plan,
    }




def _submit_workchain_builder(draft_data: Mapping[str, Any]) -> dict[str, Any]:
    result = _build_dynamic_protocol_builder(draft_data)
    if not result["success"]:
        raise http_error(
            422,
            "Builder validation failed",
            errors=result["errors"],
            missing_ports=result["missing_ports"],
            entry_point=result["entry_point"],
            recovery_plan=result["recovery_plan"],
        )

    builder = result.get("builder")
    if builder is None:
        raise http_error(500, "Builder submission failed", reason="Validated builder is unavailable")

    try:
        reset_storage_backend_caches()
        prime_storage_user_context()
        node = submit(builder)
    except Exception as exc:  # noqa: BLE001
        raise http_error(
            500,
            "Builder submission failed",
            entry_point=result.get("entry_point"),
            reason=str(exc),
        ) from exc

    process_state = getattr(node, "process_state", None)
    state = process_state.value if hasattr(process_state, "value") else str(process_state or "created")
    return {
        "status": "submitted",
        "pk": int(node.pk),
        "uuid": str(node.uuid),
        "state": state,
    }




def _submit_validated_workflow(entry_point: str, inputs: Mapping[str, Any] | None = None) -> dict[str, Any]:
    process = _load_workflow(entry_point)

    try:
        processed_inputs, validation_error = _prepare_and_validate(process, inputs or {})
    except ValueError as exc:
        raise http_error(400, "Failed to normalize or pre-process inputs", reason=str(exc)) from exc

    if validation_error is not None:
        raise http_error(
            422,
            "Validation failed",
            port=str(getattr(validation_error, "port", "") or ""),
            reason=str(getattr(validation_error, "message", "") or str(validation_error)),
            full_error=str(validation_error),
        )

    try:
        node = submit(process, **processed_inputs)
    except Exception as exc:  # noqa: BLE001
        raise http_error(500, "Submission failed", reason=str(exc)) from exc

    process_state = getattr(node, "process_state", None)
    state = process_state.value if hasattr(process_state, "value") else str(process_state or "created")
    return {"pk": int(node.pk), "uuid": str(node.uuid), "state": state}
