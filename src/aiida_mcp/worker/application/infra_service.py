from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any
from uuid import uuid4

import yaml
from aiida import __version__ as AIIDA_CORE_VERSION
from aiida import orm
from aiida.manage import get_manager
from aiida.manage.configuration import get_profile
from aiida.plugins import TransportFactory
from aiida.plugins.entry_point import get_entry_point_names
from packaging.version import InvalidVersion, Version

from aiida_mcp.worker.protocol.errors import WorkerDomainError
from aiida_mcp.core.engine import get_profile_default_user, http_error
from aiida_mcp.worker.protocol.infrastructure import (
    CodeSetupRequest,
    InfrastructureCapabilitiesResponse,
    InfrastructureExportResponse,
    InfrastructureSetupRequest,
)

_TRANSPORT_ALIASES = {
    "local": "core.local",
    "core.local": "core.local",
    "ssh": "core.ssh",
    "core.ssh": "core.ssh",
    "ssh_async": "core.ssh_async",
    "core.ssh_async": "core.ssh_async",
    "asyncssh": "core.ssh_async",
}
_ASYNC_SSH_TRANSPORT = "core.ssh_async"
_SYNC_SSH_TRANSPORT = "core.ssh"
_ASYNC_ONLY_PAYLOAD_FIELDS = ("host", "max_io_allowed", "authentication_script", "backend")
_SYNC_ONLY_PAYLOAD_FIELDS = (
    "username",
    "port",
    "look_for_keys",
    "key_filename",
    "timeout",
    "connection_timeout",
    "allow_agent",
    "proxy_jump",
    "proxy_command",
    "compress",
    "gss_auth",
    "gss_kex",
    "gss_deleg_creds",
    "gss_host",
    "load_system_host_keys",
    "key_policy",
)
_TRANSPORT_AUTH_FIELD_ALIASES = {"script_before": "authentication_script"}

def _get_supported_transport_names() -> list[str]:
    available = sorted({str(name) for name in get_entry_point_names("aiida.transports")})
    preferred = ["core.local", "core.ssh", "core.ssh_async"]
    ordered = [name for name in preferred if name in available]
    ordered.extend(name for name in available if name not in ordered)
    return ordered



def _supports_async_ssh_transport() -> bool:
    try:
        return Version(str(AIIDA_CORE_VERSION)) >= Version("2.7.0") and _ASYNC_SSH_TRANSPORT in _get_supported_transport_names()
    except InvalidVersion:
        return _ASYNC_SSH_TRANSPORT in _get_supported_transport_names()



def _normalize_transport_type(transport_type: str | None) -> str:
    normalized = str(transport_type or "").strip().lower()
    if not normalized:
        return _ASYNC_SSH_TRANSPORT if _supports_async_ssh_transport() else _SYNC_SSH_TRANSPORT
    return _TRANSPORT_ALIASES.get(normalized, str(transport_type or "").strip())



def _get_transport_auth_fields(transport_type: str) -> list[str]:
    try:
        transport_cls = TransportFactory(transport_type)
        return [
            _TRANSPORT_AUTH_FIELD_ALIASES.get(str(field), str(field))
            for field in transport_cls.get_valid_auth_params()
        ]
    except Exception:
        return []



def _get_infrastructure_capabilities_payload() -> dict[str, Any]:
    transports = _get_supported_transport_names()
    supports_async_ssh = _supports_async_ssh_transport()
    recommended_transport = (
        _ASYNC_SSH_TRANSPORT
        if supports_async_ssh
        else (_SYNC_SSH_TRANSPORT if _SYNC_SSH_TRANSPORT in transports else transports[0] if transports else "core.local")
    )
    return {
        "aiida_core_version": str(AIIDA_CORE_VERSION),
        "available_transports": transports,
        "recommended_transport": recommended_transport,
        "supports_async_ssh": supports_async_ssh,
        "transport_auth_fields": {
            transport_type: _get_transport_auth_fields(transport_type)
            for transport_type in transports
        },
    }


def get_infrastructure_capabilities() -> InfrastructureCapabilitiesResponse:
    return InfrastructureCapabilitiesResponse(**_get_infrastructure_capabilities_payload())



def _resolve_transport_capabilities(transport_type: str | None) -> tuple[str, dict[str, Any], set[str]]:
    capabilities = _get_infrastructure_capabilities_payload()
    normalized = _normalize_transport_type(transport_type)
    available = set(capabilities["available_transports"])
    if normalized not in available:
        raise http_error(
            422,
            "Unsupported transport for the installed AiiDA version",
            transport_type=normalized,
            aiida_core_version=capabilities["aiida_core_version"],
            available_transports=capabilities["available_transports"],
        )
    auth_fields = set(capabilities["transport_auth_fields"].get(normalized, []))
    return normalized, capabilities, auth_fields



def _has_configured_value(value: Any) -> bool:
    return value is not None and value != ""



def _build_transport_auth_params(payload: InfrastructureSetupRequest, transport_type: str, auth_fields: set[str]) -> dict[str, Any]:
    auth_params: dict[str, Any] = {}

    if "use_login_shell" in auth_fields and payload.use_login_shell is not None:
        auth_params["use_login_shell"] = payload.use_login_shell
    if "safe_interval" in auth_fields and payload.safe_interval is not None:
        auth_params["safe_interval"] = payload.safe_interval

    if transport_type == _ASYNC_SSH_TRANSPORT:
        unsupported = [field for field in _SYNC_ONLY_PAYLOAD_FIELDS if _has_configured_value(getattr(payload, field, None))]
        if unsupported:
            raise http_error(
                422,
                "core.ssh_async uses SSH config style authentication parameters",
                transport_type=transport_type,
                unsupported_fields=unsupported,
                expected_fields=["host", "max_io_allowed", "authentication_script", "backend", "use_login_shell", "safe_interval"],
            )
        if "host" in auth_fields:
            auth_params["host"] = str(payload.host or payload.hostname or "").strip()
        if "max_io_allowed" in auth_fields and payload.max_io_allowed is not None:
            auth_params["max_io_allowed"] = int(payload.max_io_allowed)
        if "authentication_script" in auth_fields and payload.authentication_script:
            auth_params["script_before"] = payload.authentication_script
        if "backend" in auth_fields and payload.backend:
            auth_params["backend"] = payload.backend
        return auth_params

    if "username" in auth_fields and payload.username:
        auth_params["username"] = payload.username
    if "port" in auth_fields and payload.port is not None:
        auth_params["port"] = int(payload.port)
    if "look_for_keys" in auth_fields and payload.look_for_keys is not None:
        auth_params["look_for_keys"] = bool(payload.look_for_keys)
    if "key_filename" in auth_fields and payload.key_filename:
        auth_params["key_filename"] = payload.key_filename
    timeout = payload.timeout if payload.timeout is not None else payload.connection_timeout
    if "timeout" in auth_fields and timeout is not None:
        auth_params["timeout"] = int(timeout)
    if "allow_agent" in auth_fields and payload.allow_agent is not None:
        auth_params["allow_agent"] = bool(payload.allow_agent)
    if "proxy_command" in auth_fields and payload.proxy_command:
        auth_params["proxy_command"] = payload.proxy_command
    if "proxy_jump" in auth_fields and payload.proxy_jump:
        auth_params["proxy_jump"] = payload.proxy_jump
    if "compress" in auth_fields and payload.compress is not None:
        auth_params["compress"] = bool(payload.compress)
    if "gss_auth" in auth_fields and payload.gss_auth is not None:
        auth_params["gss_auth"] = bool(payload.gss_auth)
    if "gss_kex" in auth_fields and payload.gss_kex is not None:
        auth_params["gss_kex"] = bool(payload.gss_kex)
    if "gss_deleg_creds" in auth_fields and payload.gss_deleg_creds is not None:
        auth_params["gss_deleg_creds"] = bool(payload.gss_deleg_creds)
    if "gss_host" in auth_fields and payload.gss_host:
        auth_params["gss_host"] = payload.gss_host
    if "load_system_host_keys" in auth_fields and payload.load_system_host_keys is not None:
        auth_params["load_system_host_keys"] = bool(payload.load_system_host_keys)
    if "key_policy" in auth_fields and payload.key_policy:
        auth_params["key_policy"] = payload.key_policy

    return auth_params



def _export_auth_payload(computer: Any, auth_params: dict[str, Any]) -> dict[str, Any] | None:
    transport_type = str(getattr(computer, "transport_type", "") or "")

    if transport_type == _ASYNC_SSH_TRANSPORT:
        exported = {
            "host": auth_params.get("host"),
            "max_io_allowed": auth_params.get("max_io_allowed"),
            "authentication_script": auth_params.get("script_before"),
            "backend": auth_params.get("backend"),
            "use_login_shell": auth_params.get("use_login_shell"),
            "safe_interval": auth_params.get("safe_interval"),
        }
    else:
        exported = {
            "username": auth_params.get("username"),
            "port": auth_params.get("port"),
            "look_for_keys": auth_params.get("look_for_keys"),
            "key_filename": auth_params.get("key_filename"),
            "timeout": auth_params.get("timeout", auth_params.get("connection_timeout")),
            "allow_agent": auth_params.get("allow_agent"),
            "proxy_command": auth_params.get("proxy_command"),
            "proxy_jump": auth_params.get("proxy_jump"),
            "compress": auth_params.get("compress"),
            "gss_auth": auth_params.get("gss_auth"),
            "gss_kex": auth_params.get("gss_kex"),
            "gss_deleg_creds": auth_params.get("gss_deleg_creds"),
            "gss_host": auth_params.get("gss_host"),
            "load_system_host_keys": auth_params.get("load_system_host_keys"),
            "key_policy": auth_params.get("key_policy"),
            "use_login_shell": auth_params.get("use_login_shell"),
            "safe_interval": auth_params.get("safe_interval"),
        }

    trimmed = {key: value for key, value in exported.items() if value not in (None, "", [], {})}
    return trimmed or None



def _resolve_auth_user(user_email: str | None) -> Any:
    cleaned = str(user_email or "").strip()
    if cleaned:
        try:
            return orm.User.collection.get(email=cleaned)
        except Exception as exc:
            raise WorkerDomainError("internal_error", "AiiDA user not found", user=cleaned) from exc

    default_user = get_profile_default_user()
    if default_user is not None:
        return default_user

    try:
        user = orm.User.collection.get_default()
    except Exception as exc:
        raise WorkerDomainError("internal_error", "No default AiiDA user configured for the active profile") from exc

    if user is None:
        raise WorkerDomainError("internal_error", "No default AiiDA user configured for the active profile")
    return user



def _prepare_unstored_node_for_store(node: Any, *, computer_pk: int | None = None) -> None:
    """Rewrite unstored node relationships to raw foreign keys before store().

    AiiDA's SQLAlchemy backend caches the default user object and can construct unstored nodes
    with relationship objects attached to a different session than the one used in the later
    `store()` call. Clearing the relationship objects and keeping only the FK ids avoids
    `Object '<DbUser ...>' is already attached to session ...` errors.
    """
    backend_entity = getattr(node, "backend_entity", None)
    bare_model = getattr(backend_entity, "bare_model", None)
    if bare_model is None:
        return

    user_pk: int | None = None
    session_user = None
    try:
        default_email = str(getattr(get_profile(), "default_user_email", "") or "").strip()
        storage = get_manager().get_profile_storage()
        backend = getattr(storage, "_backend", storage)
        backend_users = getattr(backend, "users", None)
        entity_cls = getattr(backend_users, "ENTITY_CLASS", None)
        model_cls = getattr(entity_cls, "MODEL_CLASS", None)
        get_session = getattr(backend, "get_session", None)
        if default_email and callable(get_session) and model_cls is not None:
            session = get_session()
            dbmodel = session.query(model_cls).filter(model_cls.email == default_email).one_or_none()
            if dbmodel is not None and getattr(dbmodel, "id", None) is not None:
                user_pk = int(dbmodel.id)
                session_user = dbmodel
    except Exception:
        user_pk = None

    if user_pk is None:
        default_user = get_profile_default_user()
        if default_user is not None and getattr(default_user, "pk", None) is not None:
            user_pk = int(default_user.pk)
        else:
            try:
                fallback_user = orm.User.collection.get_default()
            except Exception:
                fallback_user = None
            if fallback_user is not None and getattr(fallback_user, "pk", None) is not None:
                user_pk = int(fallback_user.pk)

    if session_user is not None and hasattr(bare_model, "user"):
        bare_model.user = session_user
    elif hasattr(bare_model, "user"):
        with suppress(Exception):
            bare_model.user = None
    if user_pk is not None and hasattr(bare_model, "user_id"):
        bare_model.user_id = user_pk

    # Keep the computer relationship intact because AiiDA validates `InstalledCode.computer`
    # before store() and expects the ORM relationship to still be present at that point.
    if computer_pk is not None and hasattr(bare_model, "dbcomputer_id") and getattr(bare_model, "dbcomputer_id", None) is None:
        bare_model.dbcomputer_id = int(computer_pk)



def _test_transport_connection(computer: Any, user: Any) -> None:
    transport = computer.get_transport(user)

    if str(getattr(computer, "transport_type", "") or "") == _ASYNC_SSH_TRANSPORT:
        loop = asyncio.new_event_loop()
        previous_loop: asyncio.AbstractEventLoop | None = None
        try:
            with suppress(RuntimeError):
                previous_loop = asyncio.get_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(transport.open_async())
        finally:
            if getattr(transport, "is_open", False):
                with suppress(Exception):
                    loop.run_until_complete(transport.close_async())
            asyncio.set_event_loop(previous_loop)
            loop.close()
        return

    with transport:
        if not transport.is_open:
            transport.open()



def _trim_export_value(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, nested_value in value.items():
            trimmed = _trim_export_value(nested_value)
            if trimmed is None:
                continue
            if isinstance(trimmed, str) and not trimmed:
                continue
            if isinstance(trimmed, dict) and not trimmed:
                continue
            cleaned[str(key)] = trimmed
        return cleaned
    if isinstance(value, list):
        return [_trim_export_value(item) for item in value]
    if value is None:
        return None
    return value



def _dump_export_yaml(payload: dict[str, Any]) -> str:
    return yaml.safe_dump(
        _trim_export_value(payload),
        sort_keys=False,
        allow_unicode=False,
        default_flow_style=False,
    )



def _build_computer_export_response(computer: Any) -> InfrastructureExportResponse:
    exported: dict[str, Any] = {
        "label": str(computer.label),
        "hostname": str(computer.hostname),
        "description": str(computer.description) if computer.description else None,
        "transport": str(computer.transport_type),
        "scheduler": str(computer.scheduler_type),
        "shebang": str(getattr(computer, "get_shebang", lambda: "")() or ""),
        "work_dir": str(getattr(computer, "get_workdir", lambda: "")() or ""),
        "mpiprocs_per_machine": getattr(computer, "get_default_mpiprocs_per_machine", lambda: None)(),
        "mpirun_command": " ".join(getattr(computer, "get_mpirun_command", lambda: [])() or []),
        "default_memory_per_machine": getattr(computer, "get_default_memory_per_machine", lambda: None)(),
        "prepend_text": str(computer.get_prepend_text()) if hasattr(computer, "get_prepend_text") else None,
        "append_text": str(computer.get_append_text()) if hasattr(computer, "get_append_text") else None,
        "use_double_quotes": bool(getattr(computer, "get_use_double_quotes", lambda: False)()),
    }

    with suppress(Exception):
        user = _resolve_auth_user(None)
        authinfo = computer.get_authinfo(user)
        auth_params = authinfo.get_auth_params() if hasattr(authinfo, "get_auth_params") else {}
        if isinstance(auth_params, dict) and auth_params:
            exported_auth = _export_auth_payload(computer, auth_params)
            if exported_auth:
                exported_auth["user"] = str(getattr(getattr(authinfo, "user", None), "email", "") or "") or None
                exported_auth = {key: value for key, value in exported_auth.items() if value not in (None, "", [], {})}
                exported["auth"] = exported_auth

    return InfrastructureExportResponse(
        kind="computer",
        label=str(computer.label),
        filename=f"{computer.label}-setup.yaml",
        content=_dump_export_yaml(exported),
    )


def setup_code(payload: CodeSetupRequest) -> dict[str, Any]:
    try:
        computer = orm.Computer.collection.get(label=payload.computer_label)
        existing = (
            orm.QueryBuilder()
            .append(orm.Computer, filters={"uuid": computer.uuid}, tag="computer")
            .append(orm.Data, with_computer="computer", filters={"label": payload.label, "node_type": {"like": "%code%"}})
            .all(flat=True)
        )
        existing = [node for node in existing if isinstance(node, orm.AbstractCode)]
        if existing:
            raise http_error(
                409,
                "Code already exists",
                reason=(
                    f"Code '{payload.label}' already exists on computer "
                    f"'{payload.computer_label}'. Choose a different label."
                ),
                pk=int(existing[0].pk),
            )
        code = orm.InstalledCode(
            label=payload.label,
            description=payload.description or "",
            default_calc_job_plugin=payload.default_calc_job_plugin,
            computer=computer,
            filepath_executable=payload.remote_abspath,
        )
        if payload.prepend_text:
            code.set_prepend_text(payload.prepend_text)
        if payload.append_text:
            code.set_append_text(payload.append_text)
        if payload.with_mpi is not None:
            code.with_mpi = payload.with_mpi
        code.use_double_quotes = payload.use_double_quotes
        _prepare_unstored_node_for_store(code, computer_pk=int(computer.pk))
        code.store()
        return {"status": "success", "pk": int(code.pk), "label": str(code.label)}
    except WorkerDomainError:
        raise
    except Exception as exc:
        raise http_error(500, "Failed to setup code", reason=str(exc)) from exc


def export_computer_config_by_pk(computer_pk: int) -> InfrastructureExportResponse:
    try:
        computer = orm.Computer.collection.get(pk=int(computer_pk))
        return _build_computer_export_response(computer)
    except Exception as exc:
        raise http_error(500, "Failed to export computer config", reason=str(exc)) from exc


def export_code_config(code_pk: int) -> InfrastructureExportResponse:
    try:
        code = orm.load_code(int(code_pk))
        computer = getattr(code, "computer", None)
        computer_label = str(computer.label) if computer else None
        exported = {
            "label": str(code.label),
            "computer": computer_label,
            "description": str(code.description) if code.description else None,
            "default_calc_job_plugin": str(getattr(code, "default_calc_job_plugin", "") or ""),
            "filepath_executable": str(getattr(code, "filepath_executable", "") or ""),
            "prepend_text": str(code.get_prepend_text()) if hasattr(code, "get_prepend_text") else None,
            "append_text": str(code.get_append_text()) if hasattr(code, "get_append_text") else None,
            "with_mpi": bool(getattr(code, "with_mpi", False)),
            "use_double_quotes": bool(getattr(code, "use_double_quotes", False)),
        }
        filename_label = f"{code.label}@{computer_label}" if computer_label else str(code.label)
        return InfrastructureExportResponse(
            kind="code",
            label=str(code.label),
            filename=f"{filename_label}.yaml",
            content=_dump_export_yaml(exported),
        )
    except Exception as exc:
        raise http_error(500, "Failed to export code config", reason=str(exc)) from exc


def _configure_computer(computer: Any, payload: InfrastructureSetupRequest) -> None:
    computer.set_default_mpiprocs_per_machine(payload.mpiprocs_per_machine)
    if payload.mpirun_command:
        computer.set_mpirun_command(payload.mpirun_command.split())
    if payload.shebang:
        computer.set_shebang(payload.shebang)
    if payload.default_memory_per_machine is not None:
        computer.set_default_memory_per_machine(payload.default_memory_per_machine)
    computer.set_use_double_quotes(payload.use_double_quotes)
    if payload.prepend_text:
        computer.set_prepend_text(payload.prepend_text)
    if payload.append_text:
        computer.set_append_text(payload.append_text)


def setup_infrastructure(payload: InfrastructureSetupRequest) -> dict[str, Any]:
    try:
        transport_type, _, auth_fields = _resolve_transport_capabilities(payload.transport_type)
        try:
            computer = orm.Computer.collection.get(label=payload.computer_label)
            if not payload.allow_update:
                raise http_error(
                    409,
                    "Computer already exists",
                    reason=f"Computer '{payload.computer_label}' already exists; choose another label or update it with verdi.",
                    computer=payload.computer_label,
                    pk=int(computer.pk),
                )
            if str(computer.transport_type) != transport_type:
                raise http_error(
                    409,
                    "Computer already exists with a different transport type",
                    computer=payload.computer_label,
                    existing_transport=str(computer.transport_type),
                    requested_transport=transport_type,
                )
        except WorkerDomainError:
            raise
        except Exception:
            computer = orm.Computer(
                label=payload.computer_label,
                hostname=payload.hostname,
                description=payload.computer_description or "",
                transport_type=transport_type,
                scheduler_type=payload.scheduler_type,
                workdir=payload.work_dir,
            )
        _configure_computer(computer, payload)
        computer.store()

        user = _resolve_auth_user(payload.user)
        try:
            authinfo = computer.get_authinfo(user)
        except Exception:
            authinfo = orm.AuthInfo(computer=computer, user=user)
        authinfo.set_auth_params(_build_transport_auth_params(payload, transport_type, auth_fields))
        authinfo.store()

        connection_status = "success"
        connection_error = None
        try:
            _test_transport_connection(computer, user)
        except Exception as exc:
            connection_status = "failed"
            connection_error = str(exc)

        code_pk = None
        if payload.code_label and payload.default_calc_job_plugin and payload.remote_abspath:
            code_result = setup_code(
                CodeSetupRequest(
                    computer_label=payload.computer_label,
                    label=payload.code_label,
                    description=payload.code_description,
                    default_calc_job_plugin=payload.default_calc_job_plugin,
                    remote_abspath=payload.remote_abspath,
                    prepend_text=payload.code_prepend_text,
                    append_text=payload.code_append_text,
                )
            )
            code_pk = int(code_result["pk"])
        return {
            "status": "success",
            "computer_pk": int(computer.pk),
            "code_pk": code_pk,
            "connection_status": connection_status,
            "connection_error": connection_error,
        }
    except WorkerDomainError:
        raise
    except Exception as exc:
        raise http_error(500, "Failed to setup infrastructure", reason=str(exc)) from exc


def test_infrastructure_connection(payload: InfrastructureSetupRequest) -> dict[str, Any]:
    temporary_pk: int | None = None
    try:
        transport_type, _, auth_fields = _resolve_transport_capabilities(payload.transport_type)
        computer = orm.Computer(
            label=f"__aris_test__{payload.computer_label or 'computer'}__{uuid4().hex[:8]}",
            hostname=payload.hostname,
            description=payload.computer_description or "",
            transport_type=transport_type,
            scheduler_type=payload.scheduler_type,
            workdir=payload.work_dir,
        )
        _configure_computer(computer, payload)
        computer.store()
        temporary_pk = int(computer.pk)
        user = _resolve_auth_user(payload.user)
        authinfo = orm.AuthInfo(computer=computer, user=user)
        authinfo.set_auth_params(_build_transport_auth_params(payload, transport_type, auth_fields))
        authinfo.store()
        try:
            _test_transport_connection(computer, user)
            return {"status": "success", "connection_status": "success", "connection_error": None}
        except Exception as exc:
            return {"status": "error", "connection_status": "failed", "connection_error": str(exc)}
    except WorkerDomainError:
        raise
    except Exception as exc:
        raise http_error(500, "Failed to test infrastructure connection", reason=str(exc)) from exc
    finally:
        if temporary_pk is not None:
            with suppress(Exception):
                orm.Computer.collection.delete(temporary_pk)
