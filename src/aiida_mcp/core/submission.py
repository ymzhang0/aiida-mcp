from __future__ import annotations

import inspect
from typing import Any, Mapping

from aiida import orm
from aiida.engine import submit
from aiida.engine.processes.ports import PortNamespace
from aiida.plugins import CalculationFactory, WorkflowFactory

from aiida_mcp.core.engine import ensure_profile_loaded

_MISSING = object()


def load_process_class(entry_point: str) -> Any:
    """Resolve a submittable process class from an ``aiida.workflows`` or ``aiida.calculations`` entry point."""
    try:
        return WorkflowFactory(entry_point)
    except Exception as workflow_error:
        try:
            return CalculationFactory(entry_point)
        except Exception:
            raise workflow_error from None


def _normalize(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_normalize(item) for item in value]
    return str(value)


def _normalize_key(value: str) -> str:
    return "".join(char for char in str(value).lower() if char.isalnum())


def _find_intent_value(name: str, intent_data: Mapping[str, Any]) -> Any:
    if name in intent_data:
        return intent_data[name]

    target = _normalize_key(name)
    for key, value in intent_data.items():
        if _normalize_key(key) == target:
            return value

    for key, value in intent_data.items():
        normalized = _normalize_key(key)
        for suffix in ("pk", "label", "uuid", "node", "id"):
            if normalized == f"{target}{suffix}" or target == f"{normalized}{suffix}":
                return value

    return _MISSING


def _safe_issubclass(candidate: Any, parent: Any) -> bool:
    try:
        return isinstance(candidate, type) and isinstance(parent, type) and issubclass(candidate, parent)
    except Exception:
        return False


def _extract_valid_types(port: Any) -> list[type]:
    valid_type = getattr(port, "valid_type", None)
    if valid_type is None:
        return []
    if isinstance(valid_type, tuple):
        return [item for item in valid_type if isinstance(item, type)]
    return [valid_type] if isinstance(valid_type, type) else []


def _is_required_parameter(parameter: inspect.Parameter) -> bool:
    if parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
        return False
    return parameter.default is inspect._empty


def _load_node_like(value: Any) -> Any:
    if isinstance(value, Mapping):
        if value.get("pk") not in (None, ""):
            return orm.load_node(value["pk"])
        if value.get("uuid") not in (None, ""):
            return orm.load_node(value["uuid"])
        return value
    if isinstance(value, int) and value > 0:
        return orm.load_node(value)
    return value


def _load_group_like(value: Any) -> Any:
    if isinstance(value, Mapping):
        if value.get("pk") not in (None, ""):
            return orm.load_group(value["pk"])
        if value.get("uuid") not in (None, ""):
            return orm.load_group(value["uuid"])
        if value.get("label") not in (None, ""):
            return orm.Group.collection.get(label=str(value["label"]).strip())
        return value
    if isinstance(value, int) and value > 0:
        return orm.load_group(value)
    if isinstance(value, str) and value.strip():
        try:
            return orm.load_group(value)
        except Exception:
            return orm.Group.collection.get(label=value.strip())
    return value


def _load_code_like(value: Any) -> Any:
    if isinstance(value, Mapping):
        if value.get("pk") not in (None, ""):
            return orm.load_code(value["pk"])
        if value.get("uuid") not in (None, ""):
            return orm.load_code(value["uuid"])
        if value.get("label") not in (None, ""):
            return orm.load_code(str(value["label"]).strip())
        return value
    if isinstance(value, str) and value.strip():
        try:
            return orm.load_code(value)
        except Exception:
            return orm.load_node(value)
    return _load_node_like(value)


def _coerce_argument(name: str, value: Any, port: Any = None) -> Any:
    if value is None:
        return None

    valid_types = _extract_valid_types(port)
    abstract_code = getattr(orm, "AbstractCode", None)
    group_type = getattr(orm, "Group", None)
    node_type = getattr(orm, "Node", None)

    if name == "code" or str(name).lower().endswith("_code"):
        return _load_code_like(value)

    normalized_name = str(name).lower()
    if normalized_name == "structure" or normalized_name.endswith("_structure"):
        return _load_node_like(value)

    if abstract_code is not None and any(_safe_issubclass(item, abstract_code) for item in valid_types):
        return _load_code_like(value)

    if group_type is not None and any(_safe_issubclass(item, group_type) for item in valid_types):
        return _load_group_like(value)

    if node_type is not None and any(_safe_issubclass(item, node_type) for item in valid_types):
        if not _is_node_reference(value):
            wrapped = _wrap_base_value(value, valid_types)
            if wrapped is not _MISSING:
                return wrapped
        return _load_node_like(value)

    return value


def _is_node_reference(value: Any) -> bool:
    return isinstance(value, Mapping) and set(value) <= {"pk", "uuid"} and any(value.get(key) not in (None, "") for key in ("pk", "uuid"))


def _wrap_base_value(value: Any, valid_types: list[type]) -> Any:
    """Wrap a raw JSON value into the base Data type a port accepts, e.g. ``2`` -> ``Int(2)``.

    Without this, a bare integer for an ``Int`` port is misread as a node PK.
    """
    if isinstance(value, orm.Node):
        return _MISSING
    is_number = isinstance(value, (int, float)) and not isinstance(value, bool)
    compatible = (
        (orm.Bool, isinstance(value, bool)),
        (orm.Int, isinstance(value, int) and not isinstance(value, bool)),
        (orm.Float, is_number),
        (orm.Str, isinstance(value, str)),
        (orm.Dict, isinstance(value, Mapping)),
        (orm.List, isinstance(value, (list, tuple))),
    )
    for data_type, matches in compatible:
        if matches and any(item is data_type for item in valid_types):
            return data_type(dict(value)) if data_type is orm.Dict else data_type(list(value)) if data_type is orm.List else data_type(value)
    return _MISSING


def _serialize_node_value(node: orm.Node) -> Any:
    if isinstance(node, orm.Dict):
        try:
            return _normalize(node.get_dict())
        except Exception:
            return {}
    if isinstance(node, orm.List):
        try:
            return _normalize(node.get_list())
        except Exception:
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
        full_label = getattr(node, "full_label", None)
        if isinstance(full_label, str) and full_label.strip():
            return full_label.strip()
        label = str(getattr(node, "label", "") or "").strip()
        computer = getattr(node, "computer", None)
        computer_label = str(getattr(computer, "label", "") or "").strip() if computer is not None else ""
        if label and computer_label:
            return f"{label}@{computer_label}"
        if label:
            return label

    return {
        "pk": int(node.pk) if getattr(node, "pk", None) is not None else None,
        "uuid": str(getattr(node, "uuid", "") or ""),
        "type": node.__class__.__name__,
        "label": str(getattr(node, "label", "") or "").strip() or None,
    }


def _serialize_builder_inputs(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, orm.Node):
        return _serialize_node_value(value)
    if value.__class__.__name__.startswith("ProcessBuilderNamespace"):
        return {str(key): _serialize_builder_inputs(item) for key, item in value.items()}
    if isinstance(value, Mapping):
        return {str(key): _serialize_builder_inputs(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_serialize_builder_inputs(item) for item in value]
    return _normalize(value)


def _coerce_explicit_inputs(namespace: Any, provided: Mapping[str, Any]) -> dict[str, Any]:
    coerced: dict[str, Any] = {}
    for name, value in provided.items():
        port = namespace.get(name) if hasattr(namespace, "get") else None
        if isinstance(port, PortNamespace) and isinstance(value, Mapping):
            coerced[name] = _coerce_explicit_inputs(port, value)
        else:
            coerced[name] = _coerce_argument(str(name), value, port)
    return coerced


def _list_missing_required_ports(namespace: Any, provided: Any, prefix: str = "") -> list[str]:
    missing: list[str] = []
    if not hasattr(namespace, "items"):
        return missing

    provided_mapping = provided if isinstance(provided, Mapping) else {}
    for name, port in namespace.items():
        child_path = f"{prefix}.{name}" if prefix else str(name)
        present = name in provided_mapping

        if isinstance(port, PortNamespace):
            nested = provided_mapping.get(name) if present else {}
            if getattr(port, "required", False) and not present and not getattr(port, "dynamic", False):
                missing.append(child_path)
                continue
            missing.extend(_list_missing_required_ports(port, nested, child_path))
            continue

        if getattr(port, "required", False) and not present:
            missing.append(child_path)

    return missing


def _serialize_signature(signature: inspect.Signature) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for name, parameter in signature.parameters.items():
        if name in {"self", "cls"}:
            continue
        payload.append(
            {
                "name": name,
                "kind": str(parameter.kind),
                "required": _is_required_parameter(parameter),
                "default": None if parameter.default is inspect._empty else _normalize(parameter.default),
                "annotation": None
                if parameter.annotation is inspect._empty
                else str(getattr(parameter.annotation, "__name__", parameter.annotation)),
            }
        )
    return payload


def _validate_builder_inputs(workchain: Any, builder_inputs: Mapping[str, Any]) -> dict[str, Any] | None:
    spec_inputs = workchain.spec().inputs
    try:
        processed = spec_inputs.pre_process(dict(builder_inputs))
    except Exception as exc:
        return {
            "stage": "validate",
            "message": str(exc),
        }

    validate_method = getattr(spec_inputs, "validate", None)
    if callable(validate_method):
        try:
            validation_error = validate_method(processed)
        except Exception as exc:
            return {
                "stage": "validate",
                "message": str(exc),
            }
        if validation_error is not None:
            return {
                "stage": "validate",
                "port": str(getattr(validation_error, "port", "") or ""),
                "message": str(getattr(validation_error, "message", "") or str(validation_error)),
                "full_error": str(validation_error),
            }

    return None


def _serialize_port(port: Any) -> dict[str, Any]:
    payload = {
        "required": bool(getattr(port, "required", False)),
        "dynamic": bool(getattr(port, "dynamic", False)),
        "help": str(getattr(port, "help", "") or "").strip() or None,
        "valid_type": [str(getattr(item, "__name__", item)) for item in _extract_valid_types(port)],
    }
    if isinstance(port, PortNamespace):
        payload["type"] = "namespace"
        payload["ports"] = {str(name): _serialize_port(child) for name, child in port.items()}
    else:
        payload["type"] = port.__class__.__name__
        try:
            default = getattr(port, "default", None)
        except Exception:
            default = None
        if default is not None:
            payload["default"] = _normalize(default)
    return payload


def _build_builder_result(params: Mapping[str, Any]) -> dict[str, Any]:
    entry_point = str(params.get("entry_point") or "").strip()
    protocol = params.get("protocol")
    intent_data = params.get("intent_data") if isinstance(params.get("intent_data"), Mapping) else {}
    overrides = params.get("overrides") if isinstance(params.get("overrides"), Mapping) else {}
    input_bindings = params.get("inputs") if isinstance(params.get("inputs"), Mapping) else None

    workchain = load_process_class(entry_point)
    spec_inputs = workchain.spec().inputs
    errors: list[dict[str, Any]] = []
    signature = None
    builder = None
    builder_inputs = {}
    builder_strategy = "explicit_inputs" if input_bindings is not None else "get_builder_from_protocol"
    if input_bindings is not None:
        try:
            builder = workchain.get_builder()
            builder._update(_coerce_explicit_inputs(spec_inputs, input_bindings))
            builder_inputs = builder._inputs(prune=True)
        except Exception as exc:
            errors.append(
                {
                    "stage": "bind_explicit_inputs",
                    "message": str(exc),
                }
            )
    else:
        get_builder_from_protocol = getattr(workchain, "get_builder_from_protocol", None)
        if not callable(get_builder_from_protocol):
            errors.append(
                {
                    "stage": "builder_strategy",
                    "message": "This WorkChain requires explicit 'inputs'; it has no get_builder_from_protocol",
                }
            )
        else:
            signature = inspect.signature(get_builder_from_protocol)
            kwargs = {}
            for name, parameter in signature.parameters.items():
                if name in {"self", "cls"} or parameter.kind in {
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                }:
                    continue
                if name == "protocol":
                    if protocol is not None:
                        kwargs[name] = protocol
                    continue
                if name == "overrides":
                    kwargs[name] = dict(overrides)
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
                port = spec_inputs.get(name)
                try:
                    kwargs[name] = _coerce_argument(name, raw_value, port)
                except Exception as exc:
                    errors.append(
                        {
                            "stage": "resolve_protocol_argument",
                            "port": name,
                            "message": str(exc),
                        }
                    )
            if signature is not None and "overrides" not in signature.parameters and overrides:
                errors.append(
                    {
                        "stage": "unsupported_overrides",
                        "port": "overrides",
                        "message": "This WorkChain does not accept 'overrides' in get_builder_from_protocol",
                    }
                )
            if not errors:
                try:
                    builder = get_builder_from_protocol(**kwargs)
                    builder_inputs = builder._inputs(prune=True)
                except Exception as exc:
                    errors.append({"stage": "get_builder_from_protocol", "message": str(exc)})

    missing_ports = _list_missing_required_ports(spec_inputs, builder_inputs)
    for missing in missing_ports:
        errors.append(
            {
                "stage": "missing_required_ports",
                "port": missing,
                "message": f"Required input '{missing}' is missing after builder construction",
            }
        )

    if not errors and builder is not None:
        validation_error = _validate_builder_inputs(workchain, builder_inputs)
        if validation_error is not None:
            errors.append(validation_error)

    return {
        "success": len(errors) == 0,
        "entry_point": entry_point,
        "protocol": protocol,
        "intent_data": _normalize(dict(intent_data)),
        "overrides": _normalize(dict(overrides)),
        "inputs": _normalize(dict(input_bindings or {})),
        "builder_strategy": builder_strategy,
        "signature": _serialize_signature(signature) if signature is not None else [],
        "pseudo_expectations": [],
        "builder_inputs": _serialize_builder_inputs(builder_inputs),
        "missing_ports": missing_ports,
        "errors": errors,
        "builder": builder,
    }


def _execute(mode: str, params: Mapping[str, Any]) -> dict[str, Any]:
    ensure_profile_loaded()
    mode = str(mode or "draft").strip().lower()
    entry_point = str(params.get("entry_point") or "").strip()
    if not entry_point:
        raise ValueError("entry_point is required")

    if mode == "spec":
        workchain = load_process_class(entry_point)
        return {
            "entry_point": entry_point,
            "inputs": _serialize_port(workchain.spec().inputs).get("ports", {}),
        }

    result = _build_builder_result(params)
    if mode == "draft":
        if not result["success"]:
            return {
                "success": False,
                "status": "DRAFT_INVALID",
                "entry_point": result["entry_point"],
                "protocol": result["protocol"],
                "intent_data": result["intent_data"],
                "overrides": result["overrides"],
                "inputs": result["inputs"],
                "builder_strategy": result["builder_strategy"],
                "signature": result["signature"],
                "pseudo_expectations": result["pseudo_expectations"],
                "builder_inputs": result["builder_inputs"],
                "errors": result["errors"],
                "missing_ports": result["missing_ports"],
                "recovery_plan": {},
            }
        return {
            "success": True,
            "status": "DRAFT_READY",
            "entry_point": result["entry_point"],
            "protocol": result["protocol"],
            "intent_data": result["intent_data"],
            "overrides": result["overrides"],
            "inputs": result["inputs"],
            "builder_strategy": result["builder_strategy"],
            "signature": result["signature"],
            "pseudo_expectations": result["pseudo_expectations"],
            "builder_inputs": result["builder_inputs"],
            "errors": [],
            "missing_ports": [],
            "recovery_plan": {},
            "preview": f"Ready to submit {result['entry_point']} using protocol '{result['protocol']}'.",
        }

    if mode == "validate":
        if not result["success"]:
            return {
                "success": False,
                "message": "Builder validation failed",
                "errors": result["errors"],
                "missing_ports": result["missing_ports"],
                "entry_point": result["entry_point"],
                "signature": result["signature"],
                "pseudo_expectations": result["pseudo_expectations"],
                "builder_inputs": result["builder_inputs"],
                "recovery_plan": {},
            }
        return {
            "success": True,
            "message": "Builder draft validated",
            "errors": [],
            "entry_point": result["entry_point"],
            "signature": result["signature"],
            "pseudo_expectations": result["pseudo_expectations"],
            "builder_inputs": result["builder_inputs"],
            "recovery_plan": {},
        }

    if mode == "submit":
        if not result["success"] or result.get("builder") is None:
            return {
                "success": False,
                "status": "DRAFT_INVALID",
                "entry_point": result["entry_point"],
                "errors": result["errors"],
                "missing_ports": result["missing_ports"],
            }
        node = submit(result["builder"])
        process_state = getattr(node, "process_state", None)
        state = process_state.value if hasattr(process_state, "value") else str(process_state or "created")
        return {
            "success": True,
            "status": "submitted",
            "pk": int(node.pk),
            "uuid": str(node.uuid),
            "state": state,
        }

    raise ValueError(f"Unsupported submission mode: {mode}")


def _normalize_builder_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    return {
        "entry_point": normalized.get("entry_point"),
        "protocol": normalized.get("protocol"),
        "intent_data": dict(normalized.get("intent_data") or {}),
        "overrides": dict(normalized.get("overrides") or {}),
        "inputs": dict(normalized.get("inputs") or {}) if isinstance(normalized.get("inputs"), Mapping) else None,
    }


def execute_builder_action(*, mode: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    params = {
        "mode": str(mode).strip().lower(),
        **_normalize_builder_payload(payload),
    }
    return _execute(str(mode).strip().lower(), params)


def get_submission_spec(*, entry_point: str) -> dict[str, Any]:
    return _execute("spec", {"mode": "spec", "entry_point": entry_point})
