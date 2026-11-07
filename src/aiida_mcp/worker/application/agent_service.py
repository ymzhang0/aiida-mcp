"""Small capabilities an agent needs that have no verdi-free equivalent elsewhere in the worker."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from aiida import orm

from aiida_mcp.worker.protocol.errors import WorkerDomainError

_BULK_KEYS = {"name", "crystalstructure", "a", "b", "c", "alpha", "covera", "u", "orthorhombic", "cubic"}


def _workspace_file(path: str, workspace_path: str | None) -> Path:
    if not workspace_path:
        raise WorkerDomainError("permission_denied", "Reading a structure file requires a configured workspace")
    workspace = Path(workspace_path).resolve()
    requested = Path(path)
    if requested.is_absolute():
        raise WorkerDomainError("permission_denied", "Structure file path must be relative to the workspace")
    resolved = (workspace / requested).resolve()
    try:
        resolved.relative_to(workspace)
    except ValueError as exc:
        raise WorkerDomainError("permission_denied", "Structure file path escapes the workspace") from exc
    if not resolved.is_file():
        raise WorkerDomainError("not_found", f"Structure file not found: {path}")
    return resolved


def create_structure(params: Mapping[str, Any], *, workspace_path: str | None) -> dict[str, Any]:
    """Store a StructureData built from exactly one of: ASE bulk parameters, a workspace file, or cell + sites."""
    sources = [key for key in ("bulk", "file", "cell") if params.get(key)]
    if len(sources) != 1:
        raise ValueError("Give exactly one of `bulk`, `file`, or `cell` (with `sites`)")
    source = sources[0]
    if source == "bulk":
        from ase.build import bulk

        spec = dict(params["bulk"])
        unknown = sorted(set(spec) - _BULK_KEYS)
        if unknown:
            raise ValueError(f"Unknown bulk keys: {', '.join(unknown)}; allowed: {', '.join(sorted(_BULK_KEYS))}")
        structure = orm.StructureData(ase=bulk(**spec))
    elif source == "file":
        from ase.io import read

        structure = orm.StructureData(ase=read(_workspace_file(str(params["file"]), workspace_path), index=params.get("index", -1)))
    else:
        sites = params.get("sites") or []
        if not sites:
            raise ValueError("`cell` needs a non-empty `sites` list of {symbol, position}")
        structure = orm.StructureData(cell=params["cell"], pbc=tuple(params.get("pbc") or (True, True, True)))
        for site in sites:
            kwargs = {"name": site["name"]} if site.get("name") else {}
            structure.append_atom(symbols=site["symbol"], position=tuple(site["position"]), **kwargs)
    if params.get("supercell"):
        structure = orm.StructureData(ase=structure.get_ase().repeat(tuple(params["supercell"])))
    structure.label = str(params.get("label") or structure.get_formula())
    structure.description = str(params.get("description") or "")
    structure.store()
    return {
        "pk": int(structure.pk),
        "uuid": str(structure.uuid),
        "label": structure.label,
        "formula": structure.get_formula(),
        "n_sites": len(structure.sites),
        "cell": structure.cell,
        "cell_volume": structure.get_cell_volume(),
        "pbc": list(structure.pbc),
    }


def process_state(identifier: str) -> dict[str, Any]:
    """The few fields needed to decide whether a process is done, cheap enough to poll."""
    value = str(identifier).strip()
    node = orm.load_node(int(value) if value.isdigit() else value)
    if not isinstance(node, orm.ProcessNode):
        raise ValueError(f"Node {identifier} is not a process")
    state = getattr(node.process_state, "value", None)
    return {
        "pk": int(node.pk),
        "process_label": node.process_label,
        "state": state,
        "terminated": bool(node.is_terminated),
        "exit_status": node.exit_status,
        "exit_message": node.exit_message,
        "process_status": node.process_status,
        "paused": bool(node.paused),
        "outputs": sorted(node.outputs) if node.is_terminated else [],
    }


def daemon_outdated_packages() -> list[dict[str, Any]] | None:
    """Packages whose installed version differs from when the daemon started; None if unknown."""
    try:
        from aiida.engine.daemon.client import get_daemon_client

        client = get_daemon_client()
        if not client.is_daemon_running:
            return None
        started = client._get_daemon_env_info()  # noqa: SLF001 - same check `verdi status` performs
        if started is None:
            return None
        current = client._get_package_version_snapshot()  # noqa: SLF001
        before = dict(started.packages)
        return [
            {"package": name, "daemon": before.get(name), "installed": current.get(name)}
            for name in sorted(set(before) | set(current))
            if before.get(name) != current.get(name)
        ]
    except Exception:  # noqa: BLE001 - informational only
        return None
