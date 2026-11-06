"""Agent-facing Computer and Code setup: create-only, connection-tested, YAML-compatible with verdi."""

from __future__ import annotations

import shlex
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Mapping, TypeVar

import yaml
from aiida import orm

from aiida_mcp.core.engine import http_error
from aiida_mcp.worker.application.infra_service import (
    _resolve_auth_user,
    setup_code,
    setup_infrastructure,
    test_infrastructure_connection,
)
from aiida_mcp.worker.protocol.infrastructure import CodeSetupRequest, InfrastructureSetupRequest

T = TypeVar("T")

# `verdi computer setup --config` / `verdi computer configure <transport> --config` keys -> request fields.
_COMPUTER_YAML_KEYS = {
    "label": "computer_label", "hostname": "hostname", "description": "computer_description",
    "transport": "transport_type", "scheduler": "scheduler_type", "shebang": "shebang", "work_dir": "work_dir",
    "mpirun_command": "mpirun_command", "mpiprocs_per_machine": "mpiprocs_per_machine",
    "default_memory_per_machine": "default_memory_per_machine", "use_double_quotes": "use_double_quotes",
    "prepend_text": "prepend_text", "append_text": "append_text",
}
_CONFIGURE_YAML_KEYS = {
    "username": "username", "port": "port", "look_for_keys": "look_for_keys", "key_filename": "key_filename",
    "timeout": "timeout", "allow_agent": "allow_agent", "proxy_jump": "proxy_jump", "proxy_command": "proxy_command",
    "compress": "compress", "gss_auth": "gss_auth", "gss_kex": "gss_kex", "gss_deleg_creds": "gss_deleg_creds",
    "gss_host": "gss_host", "load_system_host_keys": "load_system_host_keys", "key_policy": "key_policy",
    "use_login_shell": "use_login_shell", "safe_interval": "safe_interval", "host": "host",
    "max_io_allowed": "max_io_allowed", "script_before": "authentication_script", "backend": "backend",
}
# `verdi code create core.code.installed --config` keys -> request fields.
_CODE_YAML_KEYS = {
    "label": "label", "description": "description", "default_calc_job_plugin": "default_calc_job_plugin",
    "computer": "computer_label", "filepath_executable": "remote_abspath", "prepend_text": "prepend_text",
    "append_text": "append_text", "use_double_quotes": "use_double_quotes", "with_mpi": "with_mpi",
}
_FOUND_MARKER = "AIIDA_MCP_FOUND:"


def _in_thread(function: Callable[[], T]) -> T:
    """Run transport I/O off the worker's event-loop thread (async transports start their own loop)."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(function).result()


def _from_yaml(text: str | None, mapping: Mapping[str, str], what: str) -> dict[str, Any]:
    if not text:
        return {}
    data = yaml.safe_load(text)
    if not isinstance(data, Mapping):
        raise ValueError(f"{what} YAML must be a mapping")
    unknown = sorted(set(data) - set(mapping))
    if unknown:
        raise ValueError(f"Unknown {what} YAML keys: {', '.join(unknown)}")
    return {mapping[key]: value for key, value in data.items() if value is not None}


def _known(fields: Mapping[str, Any], model: type, what: str) -> dict[str, Any]:
    unknown = sorted(set(fields) - set(model.model_fields))
    if unknown:
        raise ValueError(f"Unknown {what} fields: {', '.join(unknown)}")
    return dict(fields)


def list_computers() -> dict[str, Any]:
    user = _resolve_auth_user(None)
    items = []
    for computer in orm.Computer.collection.all():
        try:
            computer.get_authinfo(user)
            configured = True
        except Exception:  # noqa: BLE001
            configured = False
        items.append({
            "pk": int(computer.pk), "label": computer.label, "hostname": computer.hostname,
            "description": computer.description or None, "transport": computer.transport_type,
            "scheduler": computer.scheduler_type, "work_dir": computer.get_workdir(),
            "mpirun_command": " ".join(computer.get_mpirun_command()),
            "mpiprocs_per_machine": computer.get_default_mpiprocs_per_machine(),
            "prepend_text": computer.get_prepend_text() or None, "configured": configured,
        })
    return {"computers": sorted(items, key=lambda item: item["pk"])}


def create_computer(params: Mapping[str, Any]) -> dict[str, Any]:
    fields = {
        **_from_yaml(params.get("setup_yaml"), _COMPUTER_YAML_KEYS, "computer setup"),
        **_from_yaml(params.get("configure_yaml"), _CONFIGURE_YAML_KEYS, "computer configure"),
        **{key: value for key, value in dict(params.get("fields") or {}).items() if value is not None},
    }
    fields.pop("allow_update", None)
    request = InfrastructureSetupRequest.model_validate(_known(fields, InfrastructureSetupRequest, "computer"))
    if orm.Computer.collection.find(filters={"label": request.computer_label}):
        raise http_error(409, "Computer already exists", reason=f"Computer '{request.computer_label}' already exists; choose another label or change it with verdi.")
    test = _in_thread(lambda: test_infrastructure_connection(request))
    if test.get("connection_status") != "success" and not params.get("store_without_connection"):
        return {"status": "not_created", "connection_status": "failed", "connection_error": test.get("connection_error"),
                "hint": "Nothing was stored. Fix the settings (hostname, username, proxy_jump, key) and retry."}
    result = _in_thread(lambda: setup_infrastructure(request))
    return {**result, "label": request.computer_label}


def check_executable(params: Mapping[str, Any]) -> dict[str, Any]:
    computer_label = str(params.get("computer_label") or "").strip()
    target = str(params.get("executable") or "").strip()
    if not computer_label or not target:
        raise ValueError("computer_label and executable are required")
    computer = orm.Computer.collection.get(label=computer_label)
    user = _resolve_auth_user(None)
    script = "\n".join(filter(None, [
        computer.get_prepend_text(),
        str(params.get("prepend_text") or ""),
        f'p=$(command -v {shlex.quote(target)}) && [ -x "$p" ] && echo "{_FOUND_MARKER}$p"',
        "true",
    ]))

    def run() -> tuple[int, str, str]:
        transport = computer.get_transport(user)
        with transport:
            return transport.exec_command_wait(script)

    retval, stdout, stderr = _in_thread(run)
    found = next((line[len(_FOUND_MARKER):].strip() for line in str(stdout).splitlines() if line.startswith(_FOUND_MARKER)), None)
    return {"computer": computer_label, "executable": target, "found": found is not None, "path": found,
            "stderr": str(stderr).strip()[-2000:] or None, "exit_status": retval}


def create_code(params: Mapping[str, Any]) -> dict[str, Any]:
    fields = {
        **_from_yaml(params.get("yaml"), _CODE_YAML_KEYS, "code"),
        **{key: value for key, value in dict(params.get("fields") or {}).items() if value is not None},
    }
    request = CodeSetupRequest.model_validate(_known(fields, CodeSetupRequest, "code"))
    check = None
    if not params.get("skip_check"):
        check = check_executable({"computer_label": request.computer_label, "executable": request.remote_abspath, "prepend_text": request.prepend_text})
        if not check["found"]:
            return {"status": "not_created", "check": check,
                    "hint": "The executable was not found on the computer with this prepend_text (e.g. module loads). Nothing was stored."}
        if check["path"] != request.remote_abspath:
            request = request.model_copy(update={"remote_abspath": check["path"]})
    result = setup_code(request)
    code = orm.load_code(int(result["pk"]))
    return {**result, "full_label": code.full_label, "filepath_executable": str(code.filepath_executable), "check": check}
