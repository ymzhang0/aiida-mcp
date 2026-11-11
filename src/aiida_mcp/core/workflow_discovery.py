from __future__ import annotations

import inspect
from importlib import metadata
from typing import Any, Iterable, Mapping

from aiida import orm
from aiida.engine.processes.ports import PortNamespace
from aiida.plugins import CalculationFactory, WorkflowFactory
from aiida.plugins.entry_point import get_entry_point_names

from aiida_mcp.core.engine import ensure_profile_loaded
from aiida_mcp.core.submission import load_process_class
from aiida_mcp.core.utils import serialize_spec, to_jsonable, type_to_string


def _distribution_for_entry_point(group: str, name: str) -> tuple[str | None, str | None]:
    for entry_point in metadata.entry_points().select(group=group, name=name):
        distribution = getattr(entry_point, "dist", None)
        package = getattr(distribution, "name", None)
        version = getattr(distribution, "version", None)
        return (
            str(package) if package else None,
            str(version) if version else None,
        )
    return None, None


def _supported_protocols(process_class: type[Any]) -> list[str]:
    getter = getattr(process_class, "get_available_protocols", None)
    if not callable(getter):
        return []
    try:
        protocols = getter()
    except Exception:  # noqa: BLE001 - third-party plugin inspection must be isolated
        return []
    if isinstance(protocols, Mapping):
        return sorted(str(name) for name in protocols)
    if isinstance(protocols, Iterable) and not isinstance(protocols, (str, bytes)):
        return sorted(str(name) for name in protocols)
    return []


_CATALOG_GROUPS = {"workflow": ("aiida.workflows", WorkflowFactory), "calculation": ("aiida.calculations", CalculationFactory)}


def workflow_catalog(kind: str = "all") -> dict[str, Any]:
    """Return deterministic metadata for every registered workflow and calculation entry point.

    ``kind`` is 'workflow', 'calculation' or 'all'. Both kinds are submitted the same way.
    """
    ensure_profile_loaded()
    kinds = list(_CATALOG_GROUPS) if kind in (None, "", "all") else [str(kind)]
    workflows: list[dict[str, Any]] = []
    for selected in kinds:
        if selected not in _CATALOG_GROUPS:
            raise ValueError(f"kind must be one of: all, {', '.join(_CATALOG_GROUPS)}")
        workflows.extend(_catalog_for(selected, *_CATALOG_GROUPS[selected]))
    return {"workflows": workflows, "count": len(workflows)}


def _catalog_for(kind: str, group: str, factory: Any) -> list[dict[str, Any]]:
    workflows: list[dict[str, Any]] = []
    for entry_point in sorted(get_entry_point_names(group)):
        package, version = _distribution_for_entry_point(group, entry_point)
        descriptor: dict[str, Any] = {
            "entry_point": entry_point,
            "kind": kind,
            "plugin_package": package,
            "plugin_version": version,
            "loadable": False,
            "process_class": None,
            "description": None,
            "builder_strategy": "explicit_inputs",
            "protocols": [],
            "required_inputs": [],
        }
        try:
            process_class = factory(entry_point)
            descriptor.update(
                {
                    "loadable": True,
                    "process_class": f"{process_class.__module__}.{process_class.__qualname__}",
                    "description": inspect.getdoc(process_class),
                    "builder_strategy": (
                        "get_builder_from_protocol"
                        if callable(getattr(process_class, "get_builder_from_protocol", None))
                        else "explicit_inputs"
                    ),
                    "protocols": _supported_protocols(process_class),
                }
            )
            spec = serialize_spec(process_class.spec().inputs)
            descriptor["required_inputs"] = _required_paths(spec)
        except Exception as exc:  # noqa: BLE001 - one broken plugin must not hide the catalog
            # A class whose spec() raises cannot be submitted either: do not advertise it as loadable.
            descriptor["loadable"] = False
            descriptor["load_error"] = str(exc)
        workflows.append(descriptor)
    return workflows


def _required_paths(spec: Mapping[str, Any], prefix: str = "") -> list[str]:
    required: list[str] = []
    for name, child in (spec.get("ports") or {}).items():
        path = f"{prefix}.{name}" if prefix else str(name)
        if child.get("type") == "PortNamespace":
            required.extend(_required_paths(child, path))
        elif child.get("required"):
            required.append(path)
    return required


def _resolve_port(process_class: type[Any], port_path: str) -> Any:
    current: Any = process_class.spec().inputs
    for segment in [part for part in port_path.split(".") if part]:
        if not isinstance(current, PortNamespace) or segment not in current:
            raise ValueError(f"Unknown input port: {port_path}")
        current = current[segment]
    return current


def _candidate_classes(valid_type: Any) -> list[type[orm.Entity]]:
    types = valid_type if isinstance(valid_type, tuple) else (valid_type,)
    result: list[type[orm.Entity]] = []
    for candidate in types:
        if isinstance(candidate, type) and issubclass(candidate, (orm.Node, orm.Group)):
            result.append(candidate)
    return result


def _entity_payload(entity: orm.Entity) -> dict[str, Any]:
    payload = {
        "pk": int(entity.pk),
        "uuid": str(entity.uuid),
        "label": str(getattr(entity, "label", "") or ""),
        "type": entity.__class__.__name__,
    }
    if isinstance(entity, orm.AbstractCode):
        payload["full_label"] = str(getattr(entity, "full_label", "") or payload["label"])
        payload["default_calc_job_plugin"] = getattr(entity, "default_calc_job_plugin", None)
    return to_jsonable(payload)


def input_port_candidates(
    *, entry_point: str, port_path: str, limit: int = 50
) -> dict[str, Any]:
    """Return stored AiiDA entities compatible with one concrete input port."""
    ensure_profile_loaded()
    process_class = load_process_class(entry_point)
    port = _resolve_port(process_class, port_path)
    valid_type = getattr(port, "valid_type", None)
    classes = _candidate_classes(valid_type)
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entity_class in classes:
        for entity in entity_class.collection.find(limit=max(1, min(int(limit), 200))):
            uuid = str(entity.uuid)
            if uuid in seen:
                continue
            seen.add(uuid)
            candidates.append(_entity_payload(entity))
            if len(candidates) >= limit:
                break
        if len(candidates) >= limit:
            break
    return {
        "entry_point": entry_point,
        "port_path": port_path,
        "port_type": type_to_string(valid_type),
        "candidates": candidates,
        "candidate_count": len(candidates),
        "requires_literal_value": not classes,
    }
