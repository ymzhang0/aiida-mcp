from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from aiida import orm
from aiida.engine.processes.ports import InputPort, PortNamespace


def type_to_string(value: Any) -> str:
    if value is None:
        return "Any"
    if isinstance(value, tuple):
        return " | ".join(type_to_string(entry) for entry in value)
    if isinstance(value, type):
        return value.__name__
    return str(value)


def to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, orm.Node):
        if isinstance(value, orm.Dict):
            try:
                return to_jsonable(value.get_dict())
            except Exception:  # noqa: BLE001
                pass

        raw_pk = getattr(value, "pk", None)
        pk_value = int(raw_pk) if isinstance(raw_pk, int) else None
        return {
            "pk": pk_value,
            "uuid": str(value.uuid),
            "type": value.__class__.__name__,
        }

    if value.__class__.__name__.startswith("ProcessBuilderNamespace"):
        return {str(key): to_jsonable(item) for key, item in value.items()}

    if isinstance(value, Mapping):
        return {str(key): to_jsonable(item) for key, item in value.items()}

    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_jsonable(item) for item in value]

    if is_dataclass(value):
        return to_jsonable(asdict(value))

    if callable(value):
        module = getattr(value, "__module__", "")
        qualname = getattr(value, "__qualname__", repr(value))
        return f"<callable {module}.{qualname}>"

    return str(value)


def _extract_default(port: InputPort | PortNamespace) -> Any:
    has_default = False
    has_default_method = getattr(port, "has_default", None)
    if callable(has_default_method):
        try:
            has_default = bool(has_default_method())
        except Exception:  # noqa: BLE001
            has_default = False

    if not has_default:
        return None

    try:
        return to_jsonable(port.default)
    except Exception:  # noqa: BLE001
        return None


def _extract_help(port: InputPort | PortNamespace) -> str | None:
    try:
        text = port.help
    except Exception:  # noqa: BLE001
        return None
    return str(text) if text else None


def _extract_required(port: InputPort | PortNamespace) -> bool:
    try:
        return bool(port.required)
    except Exception:  # noqa: BLE001
        return False


def serialize_spec(port_or_namespace: InputPort | PortNamespace) -> dict[str, Any]:
    """
    Recursively serialize an AiiDA input port/namespace into a JSON-safe structure.
    Port names are represented by keys in the parent ``ports`` mapping.
    """
    payload: dict[str, Any] = {
        "required": _extract_required(port_or_namespace),
        "default": _extract_default(port_or_namespace),
        "help": _extract_help(port_or_namespace),
    }

    if isinstance(port_or_namespace, PortNamespace):
        payload["type"] = "PortNamespace"
        payload["dynamic"] = bool(getattr(port_or_namespace, "dynamic", False))
        payload["ports"] = {
            child_name: serialize_spec(child_port)
            for child_name, child_port in port_or_namespace.items()
        }
    else:
        payload["type"] = type_to_string(getattr(port_or_namespace, "valid_type", None))
        payload["non_db"] = bool(getattr(port_or_namespace, "non_db", False))

    return payload
