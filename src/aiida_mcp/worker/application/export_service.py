"""Take data out of the profile: an AiiDA archive, or the plain files of processes.

Both write under the configured workspace only and never change the profile.
"""
from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from typing import Any, Iterable

from aiida import orm
from aiida.common.exceptions import MultipleObjectsError, NotExistent
from aiida.common.links import LinkType

from aiida_mcp.worker.protocol.errors import WorkerDomainError

_UNSAFE = re.compile(r"[^A-Za-z0-9._+-]+")
_LISTED = 50


def workspace_target(path: str, workspace_path: str | None) -> tuple[Path, Path]:
    """Resolve a workspace-relative path; return (workspace, target).

    The check is on the path as written: absolute paths and `..` that climbs out are refused, but a
    symbolic link inside the workspace (e.g. `data` -> another disk) is the user's own layout and is followed.
    """
    if not workspace_path:
        raise WorkerDomainError("permission_denied", "This tool reads or writes files and needs a configured workspace")
    if not str(path).strip():
        raise WorkerDomainError("invalid_argument", "path is required")
    workspace = Path(workspace_path).expanduser().resolve()
    requested = Path(path)
    if requested.is_absolute():
        raise WorkerDomainError("permission_denied", "path must be relative to the workspace", workspace=str(workspace))
    target = Path(os.path.normpath(workspace / requested))
    try:
        target.relative_to(workspace)
    except ValueError as exc:
        raise WorkerDomainError("permission_denied", "path escapes the workspace", workspace=str(workspace)) from exc
    return workspace, target


def _identifier(value: Any) -> int | str:
    text = str(value).strip()
    return int(text) if text.isdigit() else text


def _load(loader: Any, kind: str, value: Any) -> Any:
    try:
        return loader(_identifier(value))
    except (NotExistent, MultipleObjectsError, ValueError) as exc:
        raise WorkerDomainError("not_found", f"{kind} not found: {value}", reason=str(exc)) from exc


def export_archive(path: str, *, groups: Iterable[Any], nodes: Iterable[Any], overwrite: bool, workspace_path: str | None) -> dict[str, Any]:
    from aiida.tools.archive import create_archive

    workspace, target = workspace_target(path, workspace_path)
    if target.suffix != ".aiida":
        target = target.with_name(target.name + ".aiida")
    if target.exists() and not overwrite:
        raise WorkerDomainError("conflict", f"{target.relative_to(workspace)} already exists; pass overwrite=true to replace it")
    loaded_groups = [_load(orm.load_group, "Group", item) for item in groups]
    loaded_nodes = [_load(orm.load_node, "Node", item) for item in nodes]
    if not loaded_groups and not loaded_nodes:
        raise WorkerDomainError("invalid_argument", "Give at least one Group or node to export")
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        create_archive(entities=[*loaded_groups, *loaded_nodes], filename=target, overwrite=bool(overwrite))
    except Exception as exc:  # noqa: BLE001 - report AiiDA's own reason
        raise WorkerDomainError("internal_error", "Archive export failed", reason=f"{type(exc).__name__}: {exc}") from exc
    return {
        "path": str(target.relative_to(workspace)),
        "absolute_path": str(target.resolve()),
        "size_bytes": target.stat().st_size,
        "groups": [{"label": group.label, "pk": group.pk, "nodes": group.count()} for group in loaded_groups],
        "nodes": [node.pk for node in loaded_nodes],
    }


def _safe(name: str) -> str:
    return _UNSAFE.sub("_", name).strip("._") or "unnamed"


def _node_label(node: orm.Node) -> str:
    return _safe(f"{getattr(node, 'process_label', None) or type(node).__name__}-{node.pk}")


class _Dump:
    def __init__(self, max_file_bytes: int) -> None:
        self.max_file_bytes = max_file_bytes
        self.files = 0
        self.bytes = 0
        self.calculations = 0
        self.skipped: list[dict[str, Any]] = []
        self.errors: list[dict[str, Any]] = []

    def copy_repository(self, node: orm.Node, directory: Path) -> None:
        repository = node.base.repository
        for root, _, filenames in repository.walk():
            for filename in filenames:
                relative = root / filename
                with repository.open(str(relative), mode="rb") as source:
                    source.seek(0, 2)
                    size = source.tell()
                    source.seek(0)
                    if size > self.max_file_bytes:
                        self.skipped.append({"pk": node.pk, "file": str(relative), "size_bytes": size})
                        continue
                    destination = directory / relative
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    with destination.open("wb") as handle:
                        shutil.copyfileobj(source, handle)
                self.files += 1
                self.bytes += size

    def process(self, node: orm.ProcessNode, directory: Path) -> None:
        if isinstance(node, orm.CalcJobNode):
            self.calculations += 1
            directory.mkdir(parents=True, exist_ok=True)
            self.copy_repository(node, directory)
            retrieved = node.base.links.get_outgoing(link_type=LinkType.CREATE, link_label_filter="retrieved").all_nodes()
            if retrieved:
                self.copy_repository(retrieved[0], directory)
            return
        called = node.base.links.get_outgoing(link_type=(LinkType.CALL_CALC, LinkType.CALL_WORK)).all()
        used: set[str] = set()
        for triple in sorted(called, key=lambda item: item.node.ctime):
            name = _safe(triple.link_label) if triple.link_label and triple.link_label != "CALL" else _node_label(triple.node)
            if name in used:
                name = f"{name}-{triple.node.pk}"
            used.add(name)
            try:
                self.process(triple.node, directory / name)
            except Exception as exc:  # noqa: BLE001 - one unreadable branch must not lose the rest
                self.errors.append({"pk": triple.node.pk, "error": f"{type(exc).__name__}: {exc}"})


def dump_files(path: str, *, nodes: Iterable[Any], group: Any, max_file_mb: float, workspace_path: str | None) -> dict[str, Any]:
    workspace, target = workspace_target(path, workspace_path)
    if target.exists() and not target.is_dir():
        raise WorkerDomainError("conflict", f"{target.relative_to(workspace)} exists and is not a directory")
    roots: dict[int, orm.Node] = {}
    for item in nodes:
        node = _load(orm.load_node, "Node", item)
        roots[node.pk] = node
    group_summary = None
    if group not in (None, ""):
        loaded = _load(orm.load_group, "Group", group)
        members = [node for node in loaded.nodes if isinstance(node, orm.ProcessNode)]
        group_summary = {"label": loaded.label, "pk": loaded.pk, "processes": len(members)}
        roots.update({node.pk: node for node in members})
    if not roots:
        raise WorkerDomainError("invalid_argument", "Nothing to dump: give `nodes` and/or a `group` that holds processes")

    dump = _Dump(int(max(0.0, float(max_file_mb)) * 1024 * 1024))
    directories: list[str] = []
    for node in roots.values():
        directory = target / _node_label(node)
        before = dump.files
        try:
            if isinstance(node, orm.ProcessNode):
                dump.process(node, directory)
            else:
                dump.copy_repository(node, directory)
        except Exception as exc:  # noqa: BLE001
            dump.errors.append({"pk": node.pk, "error": f"{type(exc).__name__}: {exc}"})
        if dump.files > before:
            directories.append(str(directory.relative_to(workspace)))
    result: dict[str, Any] = {
        "path": str(target.relative_to(workspace)),
        "absolute_path": str(target.resolve()),
        "roots": len(roots),
        "calculations": dump.calculations,
        "files": dump.files,
        "size_bytes": dump.bytes,
        "directories": directories[:_LISTED],
    }
    if len(directories) > _LISTED:
        result["directories_omitted"] = len(directories) - _LISTED
    if group_summary:
        result["group"] = group_summary
    if dump.skipped:
        result["skipped_large_files"] = dump.skipped[:_LISTED]
        result["skipped_large_files_total"] = len(dump.skipped)
    if dump.errors:
        result["errors"] = dump.errors[:_LISTED]
    return result
