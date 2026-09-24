from __future__ import annotations

import os
import re
import threading
from contextlib import contextmanager, suppress
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from aiida import __version__ as AIIDA_CORE_VERSION, load_profile, orm
from aiida.manage.configuration import get_config
from aiida.manage.manager import get_manager
from aiida.storage.sqlite_zip.backend import SqliteZipBackend
from aiida.engine.daemon.client import get_daemon_client
from aiida.orm import Group, Node, ProcessNode, QueryBuilder
from aiida.plugins.entry_point import get_entry_point_names
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

PROFILE_NAME = os.getenv("AIIDA_PROFILE", "sandbox")
_PROFILE_LOADED = False
_ACTIVE_PROFILE_NAME = ""
_CURRENT_MOUNTED_ARCHIVE: str | None = None

_DB_ACCESS_CONCURRENCY = max(1, int(os.getenv("AIIDA_DB_ACCESS_CONCURRENCY", "4")))
_DB_ACCESS_ACQUIRE_TIMEOUT_SECONDS = max(0.1, float(os.getenv("AIIDA_DB_ACCESS_ACQUIRE_TIMEOUT_SECONDS", "1.5")))
_DB_ACCESS_SEMAPHORE = threading.BoundedSemaphore(_DB_ACCESS_CONCURRENCY)

_DB_POOL_SIZE = 20
_DB_MAX_OVERFLOW = 30
_DB_POOL_PRE_PING = True

SCRIPT_REPOSITORY_ROOT = Path(
    os.getenv(
        "AIIDA_SCRIPT_REPOSITORY_DIR",
        os.getenv("AIIDA_SKILL_REPOSITORY_DIR", Path(__file__).resolve().parents[1] / "repository"),
    )
).expanduser()
SCRIPT_REGISTRY_DIR = SCRIPT_REPOSITORY_ROOT / "scripts"
SCRIPT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")

from aiida_mcp.worker.protocol.errors import WorkerDomainError

def http_error(status_code: int, error: str, **extra: Any) -> WorkerDomainError:
    if status_code == 404:
        kind = "not_found"
    elif status_code in {400, 422}:
        kind = "invalid_argument"
    elif status_code == 409:
        kind = "conflict"
    elif status_code == 503:
        kind = "runtime_unavailable"
    elif status_code == 403:
        kind = "permission_denied"
    else:
        kind = "internal_error"

    payload: dict[str, Any] = dict(extra)
    if _ACTIVE_PROFILE_NAME and "profile" not in payload:
        payload["profile"] = _ACTIVE_PROFILE_NAME
    return WorkerDomainError(kind=kind, message=error, **payload)


@contextmanager
def db_access_guard(endpoint: str) -> Any:
    acquired = _DB_ACCESS_SEMAPHORE.acquire(timeout=_DB_ACCESS_ACQUIRE_TIMEOUT_SECONDS)
    if not acquired:
        raise http_error(
            503,
            "Database busy",
            endpoint=endpoint,
            reason="Request throttled while waiting for a database slot",
        )
    try:
        yield
    finally:
        _DB_ACCESS_SEMAPHORE.release()


def ensure_profile_loaded() -> None:
    """Load configured AiiDA profile once with robust fallback behavior."""
    global _ACTIVE_PROFILE_NAME, _PROFILE_LOADED

    if _PROFILE_LOADED:
        return

    configured_profile = PROFILE_NAME.strip()
    candidates: list[str | None] = [configured_profile or "sandbox"]
    if "sandbox" not in candidates:
        candidates.append("sandbox")
    candidates.append(None)  # fallback to AiiDA configured default

    errors: list[dict[str, str]] = []
    for candidate in candidates:
        label = candidate or "<aiida-default>"
        try:
            profile = load_profile(candidate)
            _ACTIVE_PROFILE_NAME = str(getattr(profile, "name", candidate or "default"))
            _PROFILE_LOADED = True
            _configure_storage_engine_pool()
            return
        except Exception as exc:  # noqa: BLE001
            errors.append({"profile": label, "error": str(exc)})

    raise http_error(500, "Failed to load any AiiDA profile", errors=errors)


def switch_profile(profile_name: str) -> str:
    global _ACTIVE_PROFILE_NAME, _PROFILE_LOADED, _CURRENT_MOUNTED_ARCHIVE

    cleaned = str(profile_name or "").strip()
    if not cleaned:
        raise http_error(400, "Profile name is required")

    try:
        profile = load_profile(cleaned, allow_switch=True)
    except Exception as exc:  # noqa: BLE001
        raise http_error(400, "Failed to switch profile", target=cleaned, reason=str(exc)) from exc

    _ACTIVE_PROFILE_NAME = str(getattr(profile, "name", cleaned))
    _PROFILE_LOADED = True
    _CURRENT_MOUNTED_ARCHIVE = None
    _configure_storage_engine_pool()
    return _ACTIVE_PROFILE_NAME


def load_archive_profile(filepath: str) -> str:
    global _ACTIVE_PROFILE_NAME, _PROFILE_LOADED, _CURRENT_MOUNTED_ARCHIVE

    source = str(filepath or "").strip()
    if not source:
        raise http_error(400, "Archive path is required")

    archive = Path(source).expanduser()
    if not archive.exists() or not archive.is_file():
        raise http_error(404, "Archive file not found", path=str(archive))

    if archive.suffix.lower() not in {".aiida", ".zip"}:
        raise http_error(400, "Unsupported archive format", path=str(archive))

    try:
        archive_profile = SqliteZipBackend.create_profile(filepath=str(archive))
        profile = load_profile(archive_profile, allow_switch=True)
    except Exception as exc:  # noqa: BLE001
        raise http_error(400, "Failed to load archive profile", path=str(archive), reason=str(exc)) from exc

    _ACTIVE_PROFILE_NAME = str(getattr(profile, "name", archive.stem))
    _PROFILE_LOADED = True
    _CURRENT_MOUNTED_ARCHIVE = str(archive)
    _configure_storage_engine_pool()
    return _ACTIVE_PROFILE_NAME


def active_profile_name() -> str:
    if _ACTIVE_PROFILE_NAME:
        return _ACTIVE_PROFILE_NAME
    try:
        manager = get_manager()
        profile = manager.get_profile()
        if profile is not None:
            return str(profile.name)
    except Exception:  # noqa: BLE001
        pass
    return PROFILE_NAME.strip() or "unknown"


def current_mounted_archive() -> str | None:
    return _CURRENT_MOUNTED_ARCHIVE


def _configure_storage_engine_pool() -> None:
    """Configure SQLAlchemy engine pooling for the active storage backend."""
    try:
        manager = get_manager()
        storage = manager.get_profile_storage()
    except Exception:  # noqa: BLE001
        return

    backend = getattr(storage, "_backend", storage)
    session_factory = getattr(backend, "_session_factory", None)
    bind = getattr(session_factory, "bind", None)
    if bind is None:
        return

    pool = getattr(bind, "pool", None)
    with suppress(Exception):
        current_size = int(pool.size()) if pool is not None and callable(getattr(pool, "size", None)) else None
        current_overflow = int(getattr(pool, "_max_overflow", -1))
        current_pre_ping = bool(getattr(pool, "_pre_ping", False))
        if (
            current_size == _DB_POOL_SIZE
            and current_overflow == _DB_MAX_OVERFLOW
            and current_pre_ping == _DB_POOL_PRE_PING
        ):
            return

    engine = None

    filepath_database = getattr(backend, "filepath_database", None)
    if filepath_database is not None:
        with suppress(Exception):
            from aiida.storage.sqlite_zip.utils import create_sqla_engine

            engine = create_sqla_engine(
                filepath_database,
                pool_size=_DB_POOL_SIZE,
                max_overflow=_DB_MAX_OVERFLOW,
                pool_pre_ping=_DB_POOL_PRE_PING,
            )

    if engine is None and str(getattr(getattr(bind, "url", None), "drivername", "")).startswith("sqlite"):
        database_path = getattr(getattr(bind, "url", None), "database", None)
        if database_path:
            with suppress(Exception):
                from aiida.storage.sqlite_zip.utils import create_sqla_engine

                engine = create_sqla_engine(
                    database_path,
                    pool_size=_DB_POOL_SIZE,
                    max_overflow=_DB_MAX_OVERFLOW,
                    pool_pre_ping=_DB_POOL_PRE_PING,
                )

    if engine is None:
        profile = getattr(backend, "_profile", None)
        storage_config = deepcopy(getattr(profile, "storage_config", {}) if profile is not None else {})
        if isinstance(storage_config, Mapping):
            with suppress(Exception):
                from aiida.storage.psql_dos.utils import create_sqlalchemy_engine

                engine_kwargs = dict(storage_config.get("engine_kwargs", {}) or {})
                engine_kwargs.update(
                    {
                        "pool_size": _DB_POOL_SIZE,
                        "max_overflow": _DB_MAX_OVERFLOW,
                        "pool_pre_ping": _DB_POOL_PRE_PING,
                    }
                )
                storage_config["engine_kwargs"] = engine_kwargs
                engine = create_sqlalchemy_engine(storage_config)

    if engine is None:
        return

    remove = getattr(session_factory, "remove", None)
    if callable(remove):
        with suppress(Exception):
            remove()

    with suppress(Exception):
        session_factory.configure(bind=engine)

    with suppress(Exception):
        bind.dispose()


def reset_storage_backend_caches() -> None:
    """Drop process-wide backend caches that can leak ORM state across requests/threads."""
    try:
        manager = get_manager()
        storage = manager.get_profile_storage()
        if hasattr(storage, "is_closed") and storage.is_closed():
            return
        if hasattr(storage, "_backend") and getattr(storage._backend, "is_closed", lambda: False)():
            return
    except Exception:  # noqa: BLE001
        return

    for candidate in {storage, getattr(storage, "_backend", None)}:
        if candidate is None:
            continue
        if hasattr(candidate, "_default_user"):
            with suppress(Exception):
                setattr(candidate, "_default_user", None)


def cleanup_storage_session() -> None:
    """Release thread-local SQLAlchemy sessions for the active profile backend."""
    try:
        manager = get_manager()
        storage = manager.get_profile_storage()
        # Check if storage is actually open/available
        if hasattr(storage, "is_closed") and storage.is_closed():
            return
        if hasattr(storage, "_backend") and getattr(storage._backend, "is_closed", lambda: False)():
            return
    except Exception:  # noqa: BLE001
        return

    backend = getattr(storage, "_backend", storage)
    session_factory = getattr(backend, "_session_factory", None)
    session = None

    if session_factory is not None:
        registry = getattr(session_factory, "registry", None)
        has = getattr(registry, "has", None)
        has_session = False
        if callable(has):
            with suppress(Exception):
                has_session = bool(has())
        if has_session:
            with suppress(Exception):
                session = session_factory()
    else:
        get_session = getattr(backend, "get_session", None)
        if callable(get_session):
            with suppress(Exception):
                session = get_session()

    if session is not None:
        with suppress(Exception):
            session.rollback()
        with suppress(Exception):
            session.close()

    if session_factory is not None:
        remove = getattr(session_factory, "remove", None)
        if callable(remove):
            with suppress(Exception):
                remove()

    reset_storage_backend_caches()


def list_profile_users() -> list[orm.User]:
    collection = orm.User.collection
    list_all = getattr(collection, "all", None)
    if callable(list_all):
        users = list(list_all())
        if users:
            return users

    find_users = getattr(collection, "find", None)
    if callable(find_users):
        users = list(find_users())
        if users:
            return users

    qb = QueryBuilder()
    qb.append(orm.User, project=["*"])
    return [row[0] for row in qb.all()]


def get_profile_default_user() -> orm.User | None:
    from aiida.manage.configuration import get_profile
    from aiida.orm.entities import from_backend_entity

    default_email = str(getattr(get_profile(), "default_user_email", "") or "").strip()
    try:
        manager = get_manager()
        storage = manager.get_profile_storage()
        backend = getattr(storage, "_backend", storage)
    except Exception:  # noqa: BLE001
        storage = None
        backend = None

    backend_users = getattr(backend, "users", None) if backend is not None else None
    entity_cls = getattr(backend_users, "ENTITY_CLASS", None)
    model_cls = getattr(entity_cls, "MODEL_CLASS", None)
    get_session = getattr(backend, "get_session", None)

    if default_email and callable(get_session) and entity_cls is not None and model_cls is not None:
        try:
            session = get_session()
            dbmodel = session.query(model_cls).filter(model_cls.email == default_email).one_or_none()
            if dbmodel is not None:
                backend_user = entity_cls.from_dbmodel(dbmodel, backend)
                return from_backend_entity(orm.User, backend_user)
        except Exception:  # noqa: BLE001
            pass

    if default_email:
        try:
            collection = orm.User.get_collection(storage if storage is not None else get_manager().get_profile_storage())
            return collection.get(email=default_email)
        except Exception:  # noqa: BLE001
            pass

    get_default = getattr(orm.User.collection, "get_default", None)
    if callable(get_default):
        try:
            return get_default()
        except Exception:  # noqa: BLE001
            pass

    users = list_profile_users()
    return users[0] if users else None


def prime_storage_user_context() -> orm.User | None:
    """Reset per-request storage state without materializing ORM users eagerly."""
    try:
        manager = get_manager()
        storage = manager.get_profile_storage()
        if hasattr(storage, "is_closed") and storage.is_closed():
            return None
        if hasattr(storage, "_backend") and getattr(storage._backend, "is_closed", lambda: False)():
            return None
    except Exception:  # noqa: BLE001
        return None

    reset_storage_backend_caches()

    # Do not materialize the default user here. Creating an ORM user wrapper at request start
    # can bind the underlying DbUser to one SQLAlchemy session, while later store() calls may
    # use another session in the same request and trigger "already attached to session" errors.
    #
    # Callers that truly need a user object should request one explicitly closer to the point of
    # use via get_profile_default_user() / orm.User.collection.get_default().
    return None




def list_profiles_payload() -> dict[str, Any]:
    ensure_profile_loaded()
    try:
        config = get_config()
        default_profile = str(config.default_profile_name or "") or None
        raw_profiles = config.profiles
        if isinstance(raw_profiles, Mapping):
            profile_names = sorted([str(name) for name in raw_profiles.keys()])
        else:
            profile_names = sorted([str(getattr(profile, "name", profile)) for profile in raw_profiles])
    except Exception as exc:  # noqa: BLE001
        raise http_error(500, "Failed to list configured profiles", reason=str(exc)) from exc

    current_profile = active_profile_name()
    profiles = [
        {
            "name": name,
            "is_default": bool(default_profile and name == default_profile),
            "is_active": bool(name == current_profile),
        }
        for name in profile_names
    ]
    return {
        "current_profile": current_profile,
        "default_profile": default_profile,
        "profiles": profiles,
        "mounted_archive": _CURRENT_MOUNTED_ARCHIVE,
    }


def ensure_script_registry_dir() -> Path:
    try:
        SCRIPT_REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise http_error(500, "Failed to initialize script registry directory", reason=str(exc)) from exc
    return SCRIPT_REGISTRY_DIR


def normalize_script_name(raw_name: str) -> str:
    cleaned = str(raw_name or "").strip()
    if not cleaned:
        raise http_error(400, "Script name is required")
    if not SCRIPT_NAME_PATTERN.fullmatch(cleaned):
        raise http_error(
            400,
            "Invalid script name",
            reason="Use 1-128 chars: letters, digits, underscore, hyphen. Must start with letter/digit.",
        )
    return cleaned


def script_path(script_name: str) -> Path:
    safe_name = normalize_script_name(script_name)
    return ensure_script_registry_dir() / f"{safe_name}.py"


def script_meta_path(script_name: str) -> Path:
    safe_name = normalize_script_name(script_name)
    return ensure_script_registry_dir() / f"{safe_name}.json"


def _is_daemon_running() -> bool:
    try:
        daemon_client = get_daemon_client()
        return bool(getattr(daemon_client, "is_daemon_running", False))
    except Exception:  # noqa: BLE001
        return False


def _query_count(cls: type[Any], *, filters: dict[str, Any] | None = None) -> int:
    qb = QueryBuilder()
    qb.append(cls, filters=filters or {})
    with db_access_guard(f"count:{cls.__name__}"):
        return int(qb.count())


def _collect_system_counts() -> dict[str, int]:
    try:
        groups_count = _query_count(Group)
    except SQLAlchemyTimeoutError:
        groups_count = 0
    except Exception:  # noqa: BLE001
        groups_count = 0

    try:
        nodes_count = _query_count(Node)
    except SQLAlchemyTimeoutError:
        nodes_count = 0
    except Exception:  # noqa: BLE001
        nodes_count = 0

    try:
        processes_count = _query_count(ProcessNode)
    except SQLAlchemyTimeoutError:
        processes_count = 0
    except Exception:  # noqa: BLE001
        processes_count = 0

    try:
        failed_count = _query_count(ProcessNode, filters={"exit_status": {"!==": 0}})
    except SQLAlchemyTimeoutError:
        failed_count = 0
    except Exception:  # noqa: BLE001
        failed_count = 0

    try:
        computers_count = _query_count(orm.Computer)
    except SQLAlchemyTimeoutError:
        computers_count = 0
    except Exception:  # noqa: BLE001
        computers_count = 0

    try:
        codes_count = _query_count(orm.Code)
    except SQLAlchemyTimeoutError:
        codes_count = 0
    except Exception:  # noqa: BLE001
        codes_count = 0

    try:
        workchains_count = len(get_entry_point_names("aiida.workflows"))
    except Exception:  # noqa: BLE001
        workchains_count = 0

    return {
        "computers": computers_count,
        "codes": codes_count,
        "workchains": workchains_count,
        "groups": groups_count,
        "nodes": nodes_count,
        "processes": processes_count,
        "failed_processes": failed_count,
    }


def get_system_info_payload() -> dict[str, Any]:
    counts = _collect_system_counts()
    return {
        "profile": active_profile_name(),
        "counts": {
            "computers": counts["computers"],
            "codes": counts["codes"],
            "workchains": counts["workchains"],
        },
        "daemon_status": _is_daemon_running(),
        "aiida_core_version": str(AIIDA_CORE_VERSION),
    }
def serialize_computers() -> list[dict[str, Any]]:
    """Serialize all computers for API responses."""
    computers = sorted(orm.Computer.collection.all(), key=lambda computer: computer.label.lower())
    return [
        {
            "label": str(computer.label),
            "hostname": str(computer.hostname),
            "description": str(computer.description) if computer.description else None,
        }
        for computer in computers
    ]


def serialize_codes() -> list[dict[str, Any]]:
    """Serialize all codes for API responses."""
    codes = sorted(orm.Code.collection.all(), key=lambda code: code.label.lower())
    payload: list[dict[str, Any]] = []
    for code in codes:
        computer_label = None
        try:
            computer = code.computer
            computer_label = str(computer.label) if computer is not None else None
        except Exception:  # noqa: BLE001
            computer_label = None

        payload.append(
            {
                "label": str(code.label),
                "default_plugin": str(getattr(code, "default_calc_job_plugin", "") or "") or None,
                "computer_label": computer_label,
            }
        )
    return payload
