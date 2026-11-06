"""Load third-party MCP tools inside the worker (the selected project Python).

A package exposes tools through the ``aiida_mcp.tools`` entry-point group::

    [project.entry-points."aiida_mcp.tools"]
    analyser = "aiida_analyser.mcp:tools"

The entry point resolves to an iterable of plain functions, or to a callable
returning one. Each function becomes the MCP tool ``<entry point>_<function>``:
its docstring is the description and its type-hinted signature is the input
schema. Mark a function ``fn.read_only = True`` when it never mutates the
profile; anything else is treated as a write tool and is hidden from
``--read-only`` servers. Plugins never need to import ``aiida_mcp``.
"""

from __future__ import annotations

import inspect
import json
import re
from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Any, Callable

from pydantic import BaseModel, ConfigDict, create_model

ENTRY_POINT_GROUP = "aiida_mcp.tools"
_NAME = re.compile(r"[^A-Za-z0-9_]+")


@dataclass(frozen=True)
class PluginTool:
    name: str
    plugin: str
    function: Callable[..., Any]
    model: type[BaseModel]
    description: str
    read_only: bool

    def manifest(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "plugin": self.plugin,
            "description": self.description,
            "input_schema": self.model.model_json_schema(),
            "read_only": self.read_only,
        }


def _arguments_model(name: str, function: Callable[..., Any]) -> type[BaseModel]:
    fields: dict[str, Any] = {}
    for parameter in inspect.signature(function).parameters.values():
        if parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            continue
        annotation = Any if parameter.annotation is inspect.Parameter.empty else parameter.annotation
        default = ... if parameter.default is inspect.Parameter.empty else parameter.default
        fields[parameter.name] = (annotation, default)
    return create_model(f"{name}_arguments", __config__=ConfigDict(extra="forbid"), **fields)


def _iter_functions(loaded: Any) -> list[Callable[..., Any]]:
    if not isinstance(loaded, (list, tuple)):
        loaded = loaded()
    return [item for item in loaded if callable(item)]


def discover() -> tuple[dict[str, PluginTool], list[dict[str, str]]]:
    tools: dict[str, PluginTool] = {}
    errors: list[dict[str, str]] = []
    for entry_point in sorted(entry_points(group=ENTRY_POINT_GROUP), key=lambda item: item.name):
        plugin = _NAME.sub("_", entry_point.name).strip("_")
        try:
            functions = _iter_functions(entry_point.load())
        except Exception as exc:  # noqa: BLE001 - one broken plugin must not hide the others
            errors.append({"plugin": plugin, "error": f"{type(exc).__name__}: {exc}"})
            continue
        for function in functions:
            name = f"{plugin}_{_NAME.sub('_', function.__name__).strip('_')}"
            try:
                model = _arguments_model(name, function)
            except Exception as exc:  # noqa: BLE001
                errors.append({"plugin": plugin, "tool": name, "error": f"{type(exc).__name__}: {exc}"})
                continue
            tools[name] = PluginTool(
                name=name,
                plugin=plugin,
                function=function,
                model=model,
                description=inspect.getdoc(function) or name,
                read_only=getattr(function, "read_only", False) is True,
            )
    return tools, errors


class PluginRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, PluginTool] | None = None
        self._errors: list[dict[str, str]] = []

    def tools(self) -> dict[str, PluginTool]:
        if self._tools is None:
            self._tools, self._errors = discover()
        return self._tools

    def manifest(self) -> dict[str, Any]:
        tools = self.tools()
        return {"tools": [tool.manifest() for tool in tools.values()], "errors": list(self._errors)}

    def call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        tool = self.tools().get(name)
        if tool is None:
            raise ValueError(f"Unknown plugin tool: {name}")
        from aiida_mcp.core.engine import ensure_profile_loaded

        ensure_profile_loaded()
        validated = tool.model.model_validate(arguments or {})
        result = tool.function(**{key: getattr(validated, key) for key in type(validated).model_fields})
        # Results cross a JSON-RPC pipe: coerce anything exotic to its string form.
        payload = json.loads(json.dumps(result, default=str))
        return payload if isinstance(payload, dict) else {"result": payload}
