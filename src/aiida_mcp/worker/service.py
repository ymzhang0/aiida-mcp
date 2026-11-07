from __future__ import annotations

import inspect
import os
import sys
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from typing import Any

from pydantic import ValidationError

from aiida_mcp.core.dynamic_execution import (
    get_execution_runtime,
    shutdown_execution_runtime,
    worker_status_payload,
)

from aiida_mcp.worker.protocol import JsonRpcError, JsonRpcRequest, JsonRpcResponse
from aiida_mcp.worker.protocol.errors import WorkerDomainError

JsonRpcHandler = Callable[[dict[str, Any]], Mapping[str, Any] | Awaitable[Mapping[str, Any]]]
_request_context: ContextVar[dict[str, Any]] = ContextVar("worker_request_context", default={})


def _context_value(params: Mapping[str, Any], field_name: str, default: Any = None) -> Any:
    value = params.get(field_name)
    if value is not None and value != "":
        return value
    return _request_context.get().get(field_name, default)


class AiiDAWorkerService:
    """Dispatch AiiDA runtime capabilities without depending on HTTP."""

    def __init__(self) -> None:
        self._handlers: dict[str, JsonRpcHandler] = {
            "runtime.status": self._runtime_status,
            "runtime.shutdown": self._runtime_shutdown,
            "environment.inspect": self._environment_inspect,
            "environment.default": self._environment_default,
            "profile.list": self._profile_list,
            "profile.current_user": self._profile_current_user,
            "profile.setup": self._profile_setup,
            "profile.switch": self._profile_switch,
            "profile.load_archive": self._profile_load_archive,
            "archive.list": self._archive_list,
            "system.info": self._system_info,
            "system.statistics": self._system_statistics,
            "system.database_summary": self._system_database_summary,
            "infrastructure.ssh_config": self._infrastructure_ssh_config,
            "resource.summary": self._resource_summary,
            "resource.plugins": self._resource_plugins,
            "workflow.catalog": self._workflow_catalog,
            "workflow.input_candidates": self._workflow_input_candidates,
            "infrastructure.capabilities": self._infrastructure_capabilities,
            "infrastructure.computer_codes": self._infrastructure_computer_codes,
            "infrastructure.inspect_v2": self._infrastructure_inspect_v2,
            "infrastructure.setup": self._infrastructure_setup,
            "infrastructure.setup_code": self._infrastructure_setup_code,
            "computer.install": self._computer_install,
            "code.install": self._code_install,
            "infrastructure.test_connection": self._infrastructure_test_connection,
            "infrastructure.export_computer": self._infrastructure_export_computer,
            "infrastructure.export_code": self._infrastructure_export_code,
            "submission.spec": self._submission_spec,
            "submission.validate": self._submission_validate,
            "submission.builder_draft": self._submission_builder_draft,
            "submission.submit": self._submission_submit,
            "submission.workgraph.submit": self._submission_workgraph_submit,
            "process.detail": self._process_detail,
            "process.workgraph": self._process_workgraph,
            "process.logs": self._process_logs,
            "process.clone_draft": self._process_clone_draft,
            "group.list": self._group_list,
            "group.labels": self._group_labels,
            "group.inspect": self._group_inspect,
            "group.inspect_uuid": self._group_inspect_uuid,
            "group.contains_node": self._group_contains_node,
            "node.links": self._node_links,
            "node.compare": self._node_compare,
            "provenance.walk": self._provenance_walk,
            "provenance.roots": self._provenance_roots,
            "provenance.subgraph": self._provenance_subgraph,
            "provenance.node_detail": self._provenance_node_detail,
            "archive.inspect": self._archive_inspect,
            "group.create": self._group_create,
            "group.delete": self._group_delete,
            "group.rename": self._group_rename,
            "group.add_nodes": self._group_add_nodes,
            "group.remove_node": self._group_remove_node,
            "group.export_archive": self._group_export_archive,
            "node.recent": self._node_recent,
            "process.recent": self._process_recent,
            "node.summary": self._node_summary,
            "node.context": self._node_context,
            "node.soft_delete": self._node_soft_delete,
            "node.script": self._node_script,
            "source_map": self._source_map,
            "data.bands": self._data_bands,
            "data.remote_files": self._data_remote_files,
            "data.remote_file": self._data_remote_file,
            "data.repository_files": self._data_repository_files,
            "data.repository_file": self._data_repository_file,
            "data.import": self._data_import,
            "structure.import": self._structure_import,
            "execution.run_python": self._execution_run_python,
            "registry.list": self._registry_list,
            "registry.register": self._registry_register,
            "registry.execute": self._registry_execute,
            "code.list": self._code_list,
            "process.control": self._process_control,
            "plugins.manifest": self._plugins_manifest,
            "plugins.call": self._plugins_call,
            "computer.list": self._computer_list,
            "computer.create": self._computer_create,
            "code.check_executable": self._code_check_executable,
            "code.create": self._code_create,
            "structure.create": self._structure_create,
            "process.state": self._process_state,
        }
        from aiida_mcp.worker.application.plugin_service import PluginRegistry

        self._plugins = PluginRegistry()

    @property
    def methods(self) -> tuple[str, ...]:
        return tuple(sorted(self._handlers))

    async def dispatch(self, payload: Any) -> JsonRpcResponse:
        request_id = payload.get("id") if isinstance(payload, Mapping) else None
        try:
            request = JsonRpcRequest.model_validate(payload)
        except ValidationError as exc:
            return JsonRpcResponse(
                id=request_id if isinstance(request_id, (str, int)) else None,
                error=JsonRpcError(
                    code=-32600,
                    message="Invalid Request",
                    data={"validation_errors": exc.errors(include_url=False)},
                ),
            )

        handler = self._handlers.get(request.method)
        if handler is None:
            return JsonRpcResponse(
                id=request.id,
                error=JsonRpcError(
                    code=-32601,
                    message="Method not found",
                    data={"method": request.method, "available_methods": list(self.methods)},
                ),
            )

        context_token = _request_context.set(
            request.context.model_dump(exclude_none=True) if request.context is not None else {}
        )
        try:
            result = handler(dict(request.params))
            if inspect.isawaitable(result):
                result = await result
            return JsonRpcResponse(id=request.id, result=dict(result))
        except WorkerDomainError as exc:
            return JsonRpcResponse(
                id=request.id,
                error=JsonRpcError(
                    code=-32000,
                    message=exc.message,
                    data={
                        **exc.data,
                        "status_code": exc.status_code,
                        "method": request.method,
                    },
                ),
            )
        except (TypeError, ValueError) as exc:
            return JsonRpcResponse(
                id=request.id,
                error=JsonRpcError(
                    code=-32602,
                    message="Invalid params",
                    data={"method": request.method, "reason": str(exc)},
                ),
            )
        except Exception as exc:  # noqa: BLE001
            return JsonRpcResponse(
                id=request.id,
                error=JsonRpcError(
                    code=-32603,
                    message="Internal error",
                    data={"method": request.method, "reason": str(exc)},
                ),
            )
        finally:
            _request_context.reset(context_token)
            try:
                from aiida_mcp.core.engine import cleanup_storage_session
                cleanup_storage_session()
            except Exception:  # noqa: BLE001
                pass

    @staticmethod
    def _runtime_status(_: dict[str, Any]) -> Mapping[str, Any]:
        payload = {
            **worker_status_payload(),
            "status": "online",
            "environment": "Managed worker subprocess",
            "transport": "stdio-jsonrpc",
            "protocol_version": "2.0",
            "python_interpreter_path": sys.executable,
            "python_version": sys.version.split()[0],
        }
        try:
            from aiida import __version__ as aiida_version
            from aiida.manage.manager import get_manager
            from aiida.plugins.entry_point import get_entry_point_names
            from aiida_mcp.core.engine import (
                _is_daemon_running,
                active_profile_name,
                ensure_profile_loaded,
                serialize_codes,
                serialize_computers,
            )
            ensure_profile_loaded()
            payload["profile"] = active_profile_name()
            profile = get_manager().get_profile()
            payload["aiida_version"] = str(aiida_version)
            payload["database_backend"] = str(getattr(profile, "storage_backend", "unknown"))
            num_computers = len(serialize_computers())
            num_codes = len(serialize_codes())
            plugins_list = sorted(get_entry_point_names("aiida.workflows"))
            payload["resources"] = {
                "computers": num_computers,
                "codes": num_codes,
                "workchains": len(plugins_list),
            }
            payload["plugins"] = plugins_list
            payload["daemon_status"] = _is_daemon_running()
            from aiida_mcp.worker.application.agent_service import daemon_outdated_packages
            outdated = daemon_outdated_packages()
            if outdated:
                payload["daemon_outdated_packages"] = outdated
                payload["daemon_warning"] = "Packages changed since the daemon started; it will not see new or updated plugins until `verdi daemon restart`."
        except Exception as exc:  # noqa: BLE001
            payload["profile_error"] = f"{type(exc).__name__}: {exc}"
            payload.setdefault("profile", "unknown")
            payload.setdefault("daemon_status", False)
            payload.setdefault("aiida_version", "unknown")
            payload.setdefault("database_backend", "unknown")
            payload.setdefault("resources", {"computers": 0, "codes": 0, "workchains": 0})
            payload.setdefault("plugins", [])
        try:
            from aiida.manage.configuration.settings import AiiDAConfigDir
            payload["config_dir"] = str(AiiDAConfigDir.get())
        except Exception:  # noqa: BLE001
            payload["config_dir"] = os.environ.get("AIIDA_PATH")
        return payload

    @staticmethod
    async def _runtime_shutdown(_: dict[str, Any]) -> Mapping[str, Any]:
        await shutdown_execution_runtime()
        return {"status": "shutting_down"}

    @staticmethod
    async def _environment_inspect(params: dict[str, Any]) -> Mapping[str, Any]:
        unsupported = set(params) - {"force_refresh", "python_interpreter_path"}
        if unsupported:
            raise ValueError(f"Unsupported params: {', '.join(sorted(unsupported))}")
        force_refresh = params.get("force_refresh", False)
        if not isinstance(force_refresh, bool):
            raise TypeError("force_refresh must be a boolean")
        requested_python = str(
            _context_value(params, "python_interpreter_path", sys.executable)
        ).strip()
        try:
            same_interpreter = os.path.samefile(requested_python, sys.executable)
        except (FileNotFoundError, OSError):
            same_interpreter = False
        if not same_interpreter:
            raise ValueError(
                "environment.inspect cannot switch Python inside a worker; ARIS must start the project-scoped worker with that interpreter"
            )
        return await get_execution_runtime().inspect_environment(
            python_interpreter_path=sys.executable,
            force_refresh=force_refresh,
        )

    @staticmethod
    async def _environment_default(params: dict[str, Any]) -> Mapping[str, Any]:
        force_refresh = params.get("force_refresh", False)
        if not isinstance(force_refresh, bool):
            raise TypeError("force_refresh must be a boolean")
        from aiida_mcp.core.dynamic_execution import resolve_worker_default_python_interpreter
        return await get_execution_runtime().inspect_environment(
            python_interpreter_path=str(resolve_worker_default_python_interpreter()),
            force_refresh=force_refresh,
        )

    @staticmethod
    def _profile_list(_: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import list_profiles_payload
        return list_profiles_payload()

    @staticmethod
    def _profile_current_user(_: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded, get_profile
        ensure_profile_loaded()
        try:
            profile = get_profile()
            default_email = profile.default_user_email
            from aiida import orm
            user = orm.User.collection.get(email=default_email)
            return {
                "email": str(getattr(user, "email", "") or ""),
                "first_name": str(getattr(user, "first_name", "") or ""),
                "last_name": str(getattr(user, "last_name", "") or ""),
                "institution": str(getattr(user, "institution", "") or ""),
            }
        except Exception:  # noqa: BLE001
            return {
                "email": "aiida@localhost",
                "first_name": "AiiDA",
                "last_name": "User",
                "institution": "Local Workstation",
            }

    @staticmethod
    def _profile_setup(params: dict[str, Any]) -> Mapping[str, Any]:
        profile_name = params.get("profile_name")
        if not profile_name or not isinstance(profile_name, str):
            raise ValueError("profile_name parameter is required")
        from pathlib import Path
        from aiida.manage.configuration import create_profile, get_config
        email = str(params.get("email") or "aiida@localhost")
        first_name = str(params.get("first_name") or "AiiDA")
        last_name = str(params.get("last_name") or "User")
        institution = str(params.get("institution") or "Local Workstation")
        filepath = str(
            Path(str(params.get("filepath") or f"~/.aiida/repository/{profile_name}"))
            .expanduser()
            .resolve()
        )
        config = get_config()
        profile = create_profile(
            config,
            storage_backend=str(params.get("backend") or "core.sqlite_dos"),
            storage_config={"filepath": filepath},
            broker_backend="core.rabbitmq",
            broker_config={"host": "localhost", "port": 5672},
            name=profile_name,
            email=email,
            first_name=first_name,
            last_name=last_name,
            institution=institution,
            is_test_profile=False,
        )
        if bool(params.get("set_as_default", True)):
            config.set_default_profile(profile.name, overwrite=True)
        config.store()
        return {"status": "success", "profile_name": profile_name}

    @staticmethod
    def _profile_switch(params: dict[str, Any]) -> Mapping[str, Any]:
        profile_name = params.get("profile")
        if not profile_name or not isinstance(profile_name, str):
            raise ValueError("profile parameter is required")
        from aiida_mcp.core.engine import switch_profile
        current = switch_profile(profile_name)
        return {"status": "switched", "current_profile": current}

    @staticmethod
    def _profile_load_archive(params: dict[str, Any]) -> Mapping[str, Any]:
        archive_path = str(params.get("path") or "").strip()
        if not archive_path:
            raise ValueError("path parameter is required")
        from pathlib import Path
        from aiida_mcp.core.engine import ensure_profile_loaded, load_archive_profile
        ensure_profile_loaded()
        current = load_archive_profile(archive_path)
        return {
            "status": "loaded",
            "current_profile": current,
            "source": str(Path(archive_path).expanduser()),
        }

    @staticmethod
    def _archive_list(params: dict[str, Any]) -> Mapping[str, Any]:
        from pathlib import Path
        from aiida_mcp.core.engine import ensure_profile_loaded
        ensure_profile_loaded()
        root = Path(str(params.get("path") or ".")).expanduser()
        if not root.exists() or not root.is_dir():
            raise WorkerDomainError("invalid_argument", "Archive directory does not exist", path=str(root))
        items = [
            str(entry)
            for entry in sorted(root.iterdir())
            if entry.is_file() and entry.suffix.lower() in {".aiida", ".zip"}
        ]
        return {"path": str(root.resolve()), "archives": items}

    @staticmethod
    def _system_info(_: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded, get_system_info_payload
        ensure_profile_loaded()
        return get_system_info_payload()

    @staticmethod
    def _system_statistics(_: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.system_service import _get_statistics_payload
        ensure_profile_loaded()
        return _get_statistics_payload()

    @staticmethod
    def _system_database_summary(_: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.system_service import _get_database_summary_payload
        ensure_profile_loaded()
        return _get_database_summary_payload()

    @staticmethod
    def _infrastructure_ssh_config(_: dict[str, Any]) -> Mapping[str, Any]:
        import os
        import paramiko

        ssh_config_path = os.path.expanduser("~/.ssh/config")
        if not os.path.exists(ssh_config_path):
            return {"hosts": []}

        config = paramiko.config.SSHConfig()
        try:
            with open(ssh_config_path, "r") as f:
                config.parse(f)
        except Exception:  # noqa: BLE001
            return {"hosts": []}

        hosts = []
        for alias in config.get_hostnames():
            if alias == "*":
                continue
            host_dict = config.lookup(alias)
            port = host_dict.get("port")
            if port:
                try:
                    port = int(port)
                except ValueError:
                    port = None
            hosts.append({
                "alias": alias,
                "hostname": host_dict.get("hostname"),
                "user": host_dict.get("user"),
                "port": port,
                "proxyjump": host_dict.get("proxyjump"),
                "identityfile": host_dict.get("identityfile"),
            })
        return {"hosts": hosts}

    @staticmethod
    def _resource_summary(_: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded, serialize_codes, serialize_computers
        ensure_profile_loaded()
        return {
            "computers": serialize_computers(),
            "codes": serialize_codes(),
        }

    @staticmethod
    def _resource_plugins(_: dict[str, Any]) -> Mapping[str, Any]:
        from aiida.plugins.entry_point import get_entry_point_names
        from aiida_mcp.core.engine import ensure_profile_loaded
        ensure_profile_loaded()
        plugins = list(sorted(get_entry_point_names("aiida.workflows")))
        return {"plugins": plugins}

    @staticmethod
    def _workflow_catalog(_: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.workflow_discovery import workflow_catalog

        return workflow_catalog()

    @staticmethod
    def _workflow_input_candidates(params: dict[str, Any]) -> Mapping[str, Any]:
        entry_point = str(params.get("entry_point") or "").strip()
        port_path = str(params.get("port_path") or "").strip()
        if not entry_point:
            raise ValueError("entry_point parameter is required")
        if not port_path:
            raise ValueError("port_path parameter is required")
        from aiida_mcp.core.workflow_discovery import input_port_candidates

        return input_port_candidates(
            entry_point=entry_point,
            port_path=port_path,
            limit=max(1, min(int(params.get("limit") or 50), 200)),
        )

    @staticmethod
    def _infrastructure_capabilities(_: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.infra_service import _get_infrastructure_capabilities_payload
        ensure_profile_loaded()
        return _get_infrastructure_capabilities_payload()

    @staticmethod
    def _infrastructure_computer_codes(params: dict[str, Any]) -> Mapping[str, Any]:
        computer_label = params.get("computer_label")
        if not computer_label or not isinstance(computer_label, str):
            raise ValueError("computer_label parameter is required")
        return AiiDAWorkerService._code_list({"computer_label": computer_label})

    @staticmethod
    def _code_list(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded, orm
        ensure_profile_loaded()
        query = orm.QueryBuilder()
        computer_label = params.get("computer_label")
        computer_filters = {"label": str(computer_label)} if computer_label else {}
        query.append(orm.Computer, filters=computer_filters, tag="computer", project="label")
        # QueryBuilder widens AbstractCode to every `data.core.%` node (RemoteData too):
        # narrow in SQL, then keep only real code classes.
        query.append(orm.Data, with_computer="computer", filters={"node_type": {"like": "%code%"}}, project="*")
        result = []
        for computer, code in query.all():
            if not isinstance(code, orm.AbstractCode):
                continue
            result.append({
                "pk": int(code.pk),
                "label": str(code.label),
                "full_label": str(getattr(code, "full_label", "") or f"{code.label}@{computer}"),
                "computer": computer,
                "description": str(code.description) if code.description else None,
                "default_calc_job_plugin": str(getattr(code, "default_calc_job_plugin", "") or ""),
                "remote_abspath": str(getattr(code, "filepath_executable", "") or ""),
                "prepend_text": str(code.prepend_text) if hasattr(code, "prepend_text") else None,
                "append_text": str(code.append_text) if hasattr(code, "append_text") else None,
                "with_mpi": bool(getattr(code, "with_mpi", False)),
                "use_double_quotes": bool(getattr(code, "use_double_quotes", False)),
                "hidden": bool(code.is_hidden) if hasattr(code, "is_hidden") else False,
            })
        return {"codes": sorted(result, key=lambda item: item["pk"])}

    @staticmethod
    def _process_control(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida.engine.processes import control
        from aiida_mcp.core.engine import ensure_profile_loaded, orm
        ensure_profile_loaded()
        action = str(params.get("action") or "").strip().lower()
        actions = {"kill": control.kill_processes, "pause": control.pause_processes, "play": control.play_processes}
        if action not in actions:
            raise ValueError("action must be one of: kill, pause, play")
        identifiers = params.get("identifiers") or []
        if not isinstance(identifiers, list) or not identifiers:
            raise ValueError("identifiers must be a non-empty list of process PKs or UUIDs")
        nodes = []
        for identifier in identifiers:
            node = orm.load_node(int(identifier) if str(identifier).isdigit() else str(identifier))
            if not isinstance(node, orm.ProcessNode):
                raise ValueError(f"Node {identifier} is not a process")
            nodes.append(node)
        kwargs: dict[str, Any] = {"timeout": float(params.get("timeout", 5.0))}
        if action == "kill":
            kwargs["force"] = bool(params.get("force", False))
        actions[action](nodes, **kwargs)
        return {
            "action": action,
            "processes": [
                {"pk": int(node.pk), "process_state": getattr(node.process_state, "value", None), "paused": bool(node.paused)}
                for node in nodes
            ],
        }

    @staticmethod
    def _infrastructure_inspect_v2(_: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded, serialize_codes, serialize_computers
        ensure_profile_loaded()
        computers = serialize_computers()
        codes = serialize_codes()
        codes_by_computer: dict[str, list[dict[str, Any]]] = {}
        for code in codes:
            comp = code.get("computer_label") or "localhost"
            codes_by_computer.setdefault(comp, []).append(code)
        items = []
        for comp in computers:
            label = comp["label"]
            items.append({
                **comp,
                "codes": codes_by_computer.get(label, []),
            })
        return {"infrastructure": items}

    @staticmethod
    def _infrastructure_setup(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.protocol.infrastructure import InfrastructureSetupRequest
        from aiida_mcp.worker.application.infra_service import setup_infrastructure
        ensure_profile_loaded()
        req = InfrastructureSetupRequest.model_validate(params)
        res = setup_infrastructure(req)
        return res if isinstance(res, dict) else {"status": "success"}

    @staticmethod
    def _infrastructure_setup_code(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.protocol.infrastructure import CodeSetupRequest
        from aiida_mcp.worker.application.infra_service import setup_code
        ensure_profile_loaded()
        req = CodeSetupRequest.model_validate(params)
        res = setup_code(req)
        return res if isinstance(res, dict) else {"status": "success"}

    @staticmethod
    def _computer_install(params: dict[str, Any]) -> Mapping[str, Any]:
        """Create or update a configured AiiDA ``Computer`` and its AuthInfo."""
        return AiiDAWorkerService._infrastructure_setup(params)

    @staticmethod
    def _code_install(params: dict[str, Any]) -> Mapping[str, Any]:
        """Create an AiiDA ``InstalledCode`` on an existing ``Computer``."""
        return AiiDAWorkerService._infrastructure_setup_code(params)

    @staticmethod
    def _infrastructure_test_connection(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.protocol.infrastructure import InfrastructureSetupRequest
        from aiida_mcp.worker.application.infra_service import test_infrastructure_connection
        ensure_profile_loaded()
        result = test_infrastructure_connection(InfrastructureSetupRequest.model_validate(params))
        return result if isinstance(result, dict) else {"status": "success"}

    @staticmethod
    def _infrastructure_export_computer(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        if pk is None:
            raise ValueError("pk parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.infra_service import export_computer_config_by_pk
        ensure_profile_loaded()
        result = export_computer_config_by_pk(int(pk))
        return result.model_dump() if hasattr(result, "model_dump") else dict(result)

    @staticmethod
    def _infrastructure_export_code(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        if pk is None:
            raise ValueError("pk parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.infra_service import export_code_config
        ensure_profile_loaded()
        result = export_code_config(int(pk))
        return result.model_dump() if hasattr(result, "model_dump") else dict(result)

    @staticmethod
    def _submission_spec(params: dict[str, Any]) -> Mapping[str, Any]:
        entry_point = params.get("entry_point")
        if not entry_point or not isinstance(entry_point, str):
            raise ValueError("entry_point parameter is required")
        import inspect as _inspect

        from aiida_mcp.core.submission import get_submission_spec, load_process_class
        from aiida_mcp.core.workflow_discovery import _supported_protocols
        res = get_submission_spec(entry_point=entry_point)
        payload: dict[str, Any] = {"entry_point": entry_point, "inputs": dict(res.get("inputs") or {})}
        process_class = load_process_class(entry_point)
        payload["protocols"] = _supported_protocols(process_class)
        getter = getattr(process_class, "get_builder_from_protocol", None)
        if callable(getter):
            arguments = {}
            for name, parameter in _inspect.signature(getter).parameters.items():
                if name in {"cls", "self"} or parameter.kind in {_inspect.Parameter.VAR_POSITIONAL, _inspect.Parameter.VAR_KEYWORD}:
                    continue
                arguments[name] = "required" if parameter.default is _inspect.Parameter.empty else f"default={parameter.default!r}"[:80]
            payload["protocol_arguments"] = arguments
        return payload

    @staticmethod
    def _submission_validate(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.submission import execute_builder_action
        draft = params.get("draft")
        payload = dict(draft) if isinstance(draft, Mapping) else params
        return execute_builder_action(
            mode="validate",
            payload=payload,
        )

    @staticmethod
    def _submission_builder_draft(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.submission import execute_builder_action
        return execute_builder_action(
            mode="draft",
            payload=params,
        )

    @staticmethod
    def _submission_submit(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.submission import execute_builder_action
        draft = params.get("draft")
        payload = dict(draft) if isinstance(draft, Mapping) else params
        return execute_builder_action(
            mode="submit",
            payload=payload,
        )

    @staticmethod
    def _submission_workgraph_submit(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.submission import execute_builder_action
        return execute_builder_action(
            mode="workgraph_submit",
            payload=params,
        )

    @staticmethod
    def _computer_list(_: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.computer_service import list_computers
        ensure_profile_loaded()
        return list_computers()

    @staticmethod
    def _computer_create(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.computer_service import create_computer
        ensure_profile_loaded()
        return create_computer(params)

    @staticmethod
    def _code_check_executable(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.computer_service import check_executable
        ensure_profile_loaded()
        return check_executable(params)

    @staticmethod
    def _code_create(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.computer_service import create_code
        ensure_profile_loaded()
        return create_code(params)

    @staticmethod
    def _structure_create(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.agent_service import create_structure
        ensure_profile_loaded()
        return create_structure(params, workspace_path=_context_value(params, "workspace_path"))

    @staticmethod
    def _process_state(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.agent_service import process_state
        ensure_profile_loaded()
        return process_state(str(params.get("identifier") or ""))

    def _plugins_manifest(self, _: dict[str, Any]) -> Mapping[str, Any]:
        return self._plugins.manifest()

    def _plugins_call(self, params: dict[str, Any]) -> Mapping[str, Any]:
        arguments = params.get("arguments")
        return self._plugins.call(str(params.get("tool") or ""), dict(arguments) if isinstance(arguments, Mapping) else {})

    @staticmethod
    def _process_detail(params: dict[str, Any]) -> Mapping[str, Any]:
        identifier = params.get("identifier")
        if not identifier or not isinstance(identifier, str):
            raise ValueError("identifier parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.core.process_utils import inspect_process_payload
        ensure_profile_loaded()
        return inspect_process_payload(identifier)

    @staticmethod
    def _process_workgraph(params: dict[str, Any]) -> Mapping[str, Any]:
        identifier = params.get("identifier")
        if not identifier or not isinstance(identifier, str):
            raise ValueError("identifier parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.core.process_utils import inspect_workgraph_payload
        ensure_profile_loaded()
        return inspect_workgraph_payload(identifier)

    @staticmethod
    def _process_logs(params: dict[str, Any]) -> Mapping[str, Any]:
        identifier = params.get("identifier")
        if not identifier or not isinstance(identifier, str):
            raise ValueError("identifier parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.core.process_utils import get_process_log_payload
        ensure_profile_loaded()
        return get_process_log_payload(identifier)

    @staticmethod
    def _process_clone_draft(params: dict[str, Any]) -> Mapping[str, Any]:
        identifier = params.get("identifier")
        if not identifier or not isinstance(identifier, str):
            raise ValueError("identifier parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.core.process_utils import build_process_clone_payload
        ensure_profile_loaded()
        return build_process_clone_payload(identifier)

    @staticmethod
    def _group_list(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.group_service import _list_groups
        ensure_profile_loaded()
        search = params.get("search")
        return {"items": _list_groups(str(search) if search else None)}

    @staticmethod
    def _group_labels(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.group_service import _list_group_labels
        ensure_profile_loaded()
        search = params.get("search_string") or params.get("search")
        return {"items": _list_group_labels(str(search) if search else None)}

    @staticmethod
    def _group_inspect(params: dict[str, Any]) -> Mapping[str, Any]:
        group_name = params.get("group_name") or params.get("label")
        if not group_name or not isinstance(group_name, str):
            raise ValueError("group_name parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida.orm import Group
        ensure_profile_loaded()
        matches = Group.collection.find(filters={"label": group_name})
        if not matches:
            raise WorkerDomainError("not_found", "Group not found", label=group_name)
        from aiida_mcp.worker.application.group_service import _inspect_group
        return _inspect_group(str(matches[0].uuid), limit=int(params.get("limit", 20)))

    @staticmethod
    def _group_inspect_uuid(params: dict[str, Any]) -> Mapping[str, Any]:
        group_uuid = params.get("group_uuid")
        if not group_uuid or not isinstance(group_uuid, str):
            raise ValueError("group_uuid parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.group_service import _inspect_group
        ensure_profile_loaded()
        return _inspect_group(group_uuid, limit=int(params.get("limit", 20)))

    @staticmethod
    def _group_contains_node(params: dict[str, Any]) -> Mapping[str, Any]:
        group_uuid = params.get("group_uuid")
        identifier = params.get("identifier")
        if not group_uuid or identifier is None:
            raise ValueError("group_uuid and identifier parameters are required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.group_service import _group_contains_node
        ensure_profile_loaded()
        return _group_contains_node(str(group_uuid), identifier)

    @staticmethod
    def _group_create(params: dict[str, Any]) -> Mapping[str, Any]:
        label = params.get("label")
        if not label or not isinstance(label, str):
            raise ValueError("label parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.group_service import _create_group
        ensure_profile_loaded()
        return _create_group(label)

    @staticmethod
    def _group_delete(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        if pk is None:
            raise ValueError("pk parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.group_service import _delete_group
        ensure_profile_loaded()
        return _delete_group(int(pk))

    @staticmethod
    def _group_rename(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        label = params.get("label")
        if pk is None or not label:
            raise ValueError("pk and label parameters are required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.group_service import _rename_group
        ensure_profile_loaded()
        return _rename_group(int(pk), str(label))

    @staticmethod
    def _group_add_nodes(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        node_pks = params.get("node_pks") or params.get("ids") or []
        if pk is None or not isinstance(node_pks, list):
            raise ValueError("pk and node_pks parameters are required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.group_service import _add_nodes_to_group
        ensure_profile_loaded()
        return _add_nodes_to_group(int(pk), [int(item) for item in node_pks])

    @staticmethod
    def _group_remove_node(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        node_pk = params.get("node_pk")
        if pk is None or node_pk is None:
            raise ValueError("pk and node_pk parameters are required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.group_service import _remove_node_from_group
        ensure_profile_loaded()
        return _remove_node_from_group(int(pk), int(node_pk))

    @staticmethod
    def _group_export_archive(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        if pk is None:
            raise ValueError("pk parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.group_service import _export_group_archive
        ensure_profile_loaded()
        return _export_group_archive(int(pk))

    @staticmethod
    def _provenance_roots(params: dict[str, Any]) -> Mapping[str, Any]:
        group_uuid = str(params.get("group_uuid") or "").strip()
        if not group_uuid:
            raise ValueError("group_uuid parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.provenance_service import roots
        ensure_profile_loaded()
        return roots(group_uuid, int(params.get("limit", 100)))

    @staticmethod
    def _provenance_subgraph(params: dict[str, Any]) -> Mapping[str, Any]:
        group_uuid = str(params.get("group_uuid") or "").strip()
        root_uuid = str(params.get("root_uuid") or "").strip()
        if not group_uuid or not root_uuid:
            raise ValueError("group_uuid and root_uuid parameters are required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.provenance_service import subgraph
        ensure_profile_loaded()
        return subgraph(group_uuid, root_uuid, int(params.get("depth", 2)))

    @staticmethod
    def _provenance_node_detail(params: dict[str, Any]) -> Mapping[str, Any]:
        group_uuid = str(params.get("group_uuid") or "").strip()
        uuid = str(params.get("uuid") or "").strip()
        if not group_uuid or not uuid:
            raise ValueError("group_uuid and uuid parameters are required")

        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.provenance_service import node_detail

        ensure_profile_loaded()
        return node_detail(group_uuid, uuid)

    @staticmethod
    def _node_links(params: dict[str, Any]) -> Mapping[str, Any]:
        identifier = str(params.get("identifier") or "").strip()
        if not identifier:
            raise ValueError("identifier parameter is required")

        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.generic_provenance import links

        ensure_profile_loaded()
        return links(identifier)

    @staticmethod
    def _node_compare(params: dict[str, Any]) -> Mapping[str, Any]:
        left = str(params.get("left") or "").strip()
        right = str(params.get("right") or "").strip()
        if not left or not right:
            raise ValueError("left and right parameters are required")

        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.node_compare import compare_nodes

        ensure_profile_loaded()
        return compare_nodes(left, right, max_depth=int(params.get("max_depth", 2)), max_differences=int(params.get("max_differences", 100)))

    @staticmethod
    def _archive_inspect(params: dict[str, Any]) -> Mapping[str, Any]:
        path = str(params.get("path") or "").strip()
        if not path:
            raise ValueError("path parameter is required")

        from aiida_mcp.worker.application.archive_service import inspect_archive

        return inspect_archive(path, workspace_path=_context_value(params, "workspace_path"))

    @staticmethod
    def _provenance_walk(params: dict[str, Any]) -> Mapping[str, Any]:
        root_uuid = str(params.get("root_uuid") or "").strip()
        if not root_uuid:
            raise ValueError("root_uuid parameter is required")

        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.generic_provenance import walk

        ensure_profile_loaded()
        return walk(root_uuid, str(params.get("direction") or ""), int(params.get("depth", 2)))

    @staticmethod
    def _node_recent(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.node_service import _get_recent_nodes
        ensure_profile_loaded()
        limit = int(params.get("limit", 15))
        group_uuid = params.get("group_uuid")
        node_type = params.get("node_type")
        label = params.get("label")
        process_state = params.get("process_state")
        root_only = bool(params.get("root_only", True))
        days = params.get("days")
        items = _get_recent_nodes(
            limit=limit,
            group_uuid=str(group_uuid) if group_uuid else None,
            node_type=str(node_type) if node_type else None,
            label=str(label) if label else None,
            process_state=str(process_state) if process_state else None,
            root_only=root_only,
            days=days,
        )
        return {"items": items}

    @staticmethod
    def _process_recent(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.node_service import _get_recent_processes
        ensure_profile_loaded()
        limit = int(params.get("limit", 15))
        root_only = bool(params.get("root_only", True))
        items = _get_recent_processes(limit=limit, root_only=root_only)
        return {"items": items}

    @staticmethod
    def _node_summary(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        if pk is None:
            raise ValueError("pk parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.core.node_utils import get_node_summary
        ensure_profile_loaded()
        return get_node_summary(int(pk))

    @staticmethod
    def _node_context(params: dict[str, Any]) -> Mapping[str, Any]:
        ids = params.get("ids") or []
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.core.node_utils import get_node_summary
        ensure_profile_loaded()
        items = []
        for raw_pk in ids[:30]:
            try:
                items.append(get_node_summary(int(raw_pk)))
            except Exception as exc:  # noqa: BLE001
                items.append({"pk": int(raw_pk), "error": str(exc)})
        return {"items": items}

    @staticmethod
    def _node_soft_delete(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        if pk is None:
            raise ValueError("pk parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.node_service import _soft_delete_node
        ensure_profile_loaded()
        return _soft_delete_node(int(pk), deleted=bool(params.get("deleted", True)))

    @staticmethod
    def _node_script(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        if pk is None:
            raise ValueError("pk parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.core.node_utils import get_node_script_payload
        ensure_profile_loaded()
        return get_node_script_payload(int(pk))

    @staticmethod
    def _source_map(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.node_service import _get_unified_source_map
        ensure_profile_loaded()
        target = params.get("target")
        return _get_unified_source_map(str(target) if target else None)

    @staticmethod
    def _data_bands(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        if pk is None:
            raise ValueError("pk parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.node_service import _get_bands_plot_data
        ensure_profile_loaded()
        return _get_bands_plot_data(int(pk))

    @staticmethod
    def _data_remote_files(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        if pk is None:
            raise ValueError("pk parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.node_service import _list_remote_files
        ensure_profile_loaded()
        return _list_remote_files(pk)

    @staticmethod
    def _data_remote_file(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        filename = str(params.get("filename") or "").strip()
        if pk is None or not filename:
            raise ValueError("pk and filename parameters are required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.node_service import _get_remote_file_content
        ensure_profile_loaded()
        return _get_remote_file_content(pk, filename)

    @staticmethod
    def _data_repository_files(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        if pk is None:
            raise ValueError("pk parameter is required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.node_service import _list_repository_files
        ensure_profile_loaded()
        return _list_repository_files(pk, source=str(params.get("source") or "folder"))

    @staticmethod
    def _data_repository_file(params: dict[str, Any]) -> Mapping[str, Any]:
        pk = params.get("pk")
        filename = str(params.get("filename") or "").strip()
        if pk is None or not filename:
            raise ValueError("pk and filename parameters are required")
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.worker.application.node_service import _get_node_file_content
        ensure_profile_loaded()
        return _get_node_file_content(
            pk,
            filename,
            source=str(params.get("source") or "folder"),
        )

    @staticmethod
    def _data_import(params: dict[str, Any]) -> Mapping[str, Any]:
        import base64
        from aiida_mcp.core.data_importers import create_node_from_file
        from aiida_mcp.core.engine import ensure_profile_loaded
        ensure_profile_loaded()
        data_type = str(params.get("data_type") or "").strip()
        source_type = str(params.get("source_type") or "file").strip()
        if not data_type:
            raise ValueError("data_type parameter is required")
        if source_type == "raw_text":
            content: bytes | str = str(params.get("raw_text") or "")
            filename = None
        else:
            encoded = str(params.get("content_base64") or "")
            if not encoded:
                raise ValueError("content_base64 parameter is required")
            try:
                content = base64.b64decode(encoded, validate=True)
            except ValueError as exc:
                raise ValueError("content_base64 must contain valid base64") from exc
            filename = str(params.get("filename") or "uploaded_file")
        result = create_node_from_file(
            data_type=data_type,
            file_content=content,
            filename=filename,
            label=params.get("label"),
            description=params.get("description"),
            source_type=source_type,
        )
        if isinstance(result, dict):
            return result
        return {
            "status": "success",
            "pk": int(result.pk),
            "uuid": str(result.uuid),
            "type": result.__class__.__name__,
        }

    @staticmethod
    def _structure_import(params: dict[str, Any]) -> Mapping[str, Any]:
        from pydantic import ValidationError

        from aiida_mcp.worker.application.structure_service import import_structure
        from aiida_mcp.worker.protocol.structures import StructureImportRequest
        from aiida_mcp.core.engine import ensure_profile_loaded

        try:
            request = StructureImportRequest.model_validate(params)
        except ValidationError as exc:
            raise ValueError(
                f"Invalid structure import payload: {exc.errors(include_url=False)}"
            ) from exc
        ensure_profile_loaded()
        return import_structure(request).model_dump(mode="json")

    @staticmethod
    async def _execution_run_python(params: dict[str, Any]) -> Mapping[str, Any]:
        script_content = str(params.get("script_content") or "")
        if not script_content.strip():
            raise ValueError("script_content parameter is required")
        from aiida_mcp.core.dynamic_execution import resolve_worker_default_python_interpreter
        from aiida_mcp.core.engine import active_profile_name, ensure_profile_loaded
        ensure_profile_loaded()
        python_path = str(resolve_worker_default_python_interpreter())
        result = await get_execution_runtime().execute_inline_script(
            script_content=script_content,
            python_interpreter_path=python_path,
            workspace_path=str(_context_value(params, "workspace_path") or "").strip() or None,
            profile_name=active_profile_name(),
        )
        return result.to_payload()

    @staticmethod
    def _registry_list(_: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.core.scripts import list_registered_scripts
        ensure_profile_loaded()
        return list_registered_scripts()

    @staticmethod
    def _registry_register(params: dict[str, Any]) -> Mapping[str, Any]:
        from aiida_mcp.core.engine import ensure_profile_loaded
        from aiida_mcp.core.scripts import register_script
        ensure_profile_loaded()
        return register_script(
            script_name=str(params.get("script_name") or ""),
            script=str(params.get("script") or ""),
            description=params.get("description"),
            overwrite=bool(params.get("overwrite", True)),
        )

    @staticmethod
    async def _registry_execute(params: dict[str, Any]) -> Mapping[str, Any]:
        script_name = str(params.get("script_name") or "").strip()
        if not script_name:
            raise ValueError("script_name parameter is required")
        from aiida_mcp.core.dynamic_execution import resolve_worker_default_python_interpreter
        from aiida_mcp.core.engine import active_profile_name, ensure_profile_loaded
        from aiida_mcp.core.scripts import get_registered_script
        ensure_profile_loaded()
        registered = get_registered_script(script_name, include_content=True)
        script_content = str(registered.get("script") or registered.get("content") or "")
        if not script_content:
            raise WorkerDomainError("not_found", "Script content not found", script_name=script_name)
        python_path = str(resolve_worker_default_python_interpreter())
        result = await get_execution_runtime().execute_registered_script(
            script_name=script_name,
            script_content=script_content,
            python_interpreter_path=python_path,
            params=dict(params.get("params") or {}),
            workspace_path=str(_context_value(params, "workspace_path") or "").strip() or None,
            profile_name=active_profile_name(),
        )
        payload = result.to_payload()
        payload["script_name"] = script_name
        return payload
