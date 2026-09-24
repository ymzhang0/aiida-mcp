from __future__ import annotations

from collections import defaultdict
from contextlib import suppress
from datetime import datetime
from pprint import pformat
from typing import Any, Mapping

from aiida import orm

from aiida_mcp.core.engine import http_error
from aiida_mcp.core.utils import to_jsonable


def node_type_name(node: orm.Node) -> str:
    node_type = str(getattr(node, "node_type", node.__class__.__name__))
    if "." in node_type:
        parts = node_type.split(".")
        if len(parts) >= 2:
            return parts[-2]
    return str(node.__class__.__name__)


def extract_process_state_value(node: orm.Node, *, default: str = "unknown") -> str:
    process_state = getattr(node, "process_state", None)
    return process_state.value if hasattr(process_state, "value") else str(process_state or default)


def compute_process_execution_time_seconds(node: orm.ProcessNode, state: str | None = None) -> float | None:
    ctime = getattr(node, "ctime", None)
    if not isinstance(ctime, datetime):
        return None

    normalized_state = str(state or extract_process_state_value(node, default="unknown")).strip().lower()
    if normalized_state in {"created", "running", "waiting"}:
        end_time = datetime.now(tz=ctime.tzinfo)
    else:
        mtime = getattr(node, "mtime", None)
        end_time = mtime if isinstance(mtime, datetime) else None

    if not isinstance(end_time, datetime):
        return None

    elapsed = (end_time - ctime).total_seconds()
    return round(max(0.0, float(elapsed)), 3)


def get_structure_formula(node: orm.StructureData) -> str | None:
    for method_name in ("get_formula", "get_chemical_formula"):
        method = getattr(node, method_name, None)
        if not callable(method):
            continue
        try:
            formula = method()
        except TypeError:
            formula = method(mode="hill")
        except Exception:
            continue
        if formula:
            return str(formula)
    return None


def shorten_path_for_preview(path: str | None, *, depth: int = 2) -> str | None:
    cleaned = str(path or "").strip()
    if not cleaned:
        return None
    segments = [segment for segment in cleaned.rstrip("/").split("/") if segment]
    if not segments:
        return cleaned
    return f".../{'/'.join(segments[-max(1, depth):])}"


from typing import Protocol

class NodeSerializerStrategy(Protocol):
    def build_preview(self, node: Any) -> dict[str, Any] | None: ...
    def extract_payload(self, node: Any) -> Any: ...


def _format_python_literal(value: Any) -> str:
    return pformat(to_jsonable(value), width=100, sort_dicts=False)


def _extract_structure_script_parts(node: orm.StructureData) -> tuple[list[str], list[list[float]], list[list[float]], list[bool]]:
    symbols: list[str] = []
    positions: list[list[float]] = []
    cell: list[list[float]] = []
    pbc: list[bool] = [True, True, True]

    with suppress(Exception):
        atoms = node.get_ase()
        symbols = [str(symbol) for symbol in atoms.get_chemical_symbols()]
        positions = [[float(coord) for coord in row] for row in atoms.get_positions().tolist()]
        cell = [[float(coord) for coord in row] for row in atoms.cell.tolist()]
        pbc = [bool(flag) for flag in atoms.get_pbc().tolist()]
        return symbols, positions, cell, pbc

    kind_symbol_map: dict[str, str] = {}
    with suppress(Exception):
        for kind in node.kinds:
            symbols_attr = getattr(kind, "symbols", None)
            if isinstance(symbols_attr, (list, tuple)) and len(symbols_attr) == 1:
                kind_symbol_map[str(kind.name)] = str(symbols_attr[0])
            else:
                kind_symbol_map[str(kind.name)] = str(getattr(kind, "symbol", None) or kind.name)

    with suppress(Exception):
        for site in node.sites:
            kind_name = str(site.kind_name)
            symbols.append(kind_symbol_map.get(kind_name, kind_name))
            positions.append([float(coord) for coord in site.position])

    with suppress(Exception):
        cell = [[float(coord) for coord in row] for row in node.cell]

    return symbols, positions, cell, pbc


def _render_structure_script(node: orm.StructureData) -> str:
    formula = get_structure_formula(node) or f"StructureData #{node.pk}"
    symbols, positions, cell, pbc = _extract_structure_script_parts(node)
    lines = [
        f"# StructureData PK {int(node.pk)}: {formula}",
        "from ase import Atoms",
        "",
        f"symbols = {_format_python_literal(symbols)}",
        f"positions = {_format_python_literal(positions)}",
        f"cell = {_format_python_literal(cell)}",
        f"pbc = {_format_python_literal(pbc)}",
        "",
        "atoms = Atoms(",
        "    symbols=symbols,",
        "    positions=positions,",
        "    cell=cell,",
        "    pbc=pbc,",
        ")",
        "",
        "# Optional: wrap back into AiiDA",
        "# from aiida.orm import StructureData",
        "# structure = StructureData(ase=atoms)",
    ]
    return "\n".join(lines)


def _render_dict_script(node: orm.Dict) -> str:
    payload = node.get_dict()
    lines = [
        f"# Dict PK {int(node.pk)}",
        f"data = {_format_python_literal(payload)}",
        "",
        "# Optional: wrap back into AiiDA",
        "# from aiida.orm import Dict",
        "# data_node = Dict(dict=data)",
    ]
    return "\n".join(lines)


def get_node_script_payload(node_pk: int) -> dict[str, Any]:
    try:
        node = orm.load_node(node_pk)
    except Exception as exc:  # noqa: BLE001
        raise http_error(404, "Node not found", pk=int(node_pk), reason=str(exc)) from exc

    if isinstance(node, orm.StructureData):
        script = _render_structure_script(node)
    elif isinstance(node, orm.Dict):
        script = _render_dict_script(node)
    else:
        raise http_error(
            422,
            "Copy as Script is only supported for StructureData and Dict nodes",
            pk=int(node.pk),
            node_type=node_type_name(node),
        )

    return {
        "pk": int(node.pk),
        "node_type": node_type_name(node),
        "language": "python",
        "script": script,
    }


def _build_structure_position_preview(node: orm.StructureData, *, limit: int = 50) -> list[dict[str, Any]] | None:
    preview_sites: list[Any] = []
    with suppress(Exception):
        preview_sites = list(node.sites[:limit])

    if not preview_sites:
        return None

    fractional_positions: list[list[float]] | None = None

    with suppress(Exception):
        atoms = node.get_ase()
        scaled_positions = atoms.get_scaled_positions(wrap=False).tolist()
        if len(scaled_positions) >= len(preview_sites):
            fractional_positions = [[float(coord) for coord in row] for row in scaled_positions[:len(preview_sites)]]

    if fractional_positions is None:
        with suppress(Exception):
            struct = node.get_pymatgen_structure()
            scaled_positions = struct.frac_coords.tolist()
            if len(scaled_positions) >= len(preview_sites):
                fractional_positions = [[float(coord) for coord in row] for row in scaled_positions[:len(preview_sites)]]

    positions: list[dict[str, Any]] = []
    for index, site in enumerate(preview_sites):
        entry: dict[str, Any] = {
            "kind": str(site.kind_name),
            "position": [float(coord) for coord in site.position],
        }
        if fractional_positions is not None and index < len(fractional_positions):
            entry["fractional_position"] = fractional_positions[index]
        positions.append(entry)

    return positions


class StructureDataSerializer:
    def build_preview(self, node: orm.StructureData) -> dict[str, Any] | None:
        atom_count: int | None = None
        cell_volume: float | None = None
        positions: list[dict[str, Any]] | None = None
        symmetry: dict[str, Any] | None = None
        lattice: dict[str, Any] | None = None

        with suppress(Exception):
            atom_count = len(node.sites)
        with suppress(Exception):
            cell_volume = node.get_cell_volume()
        with suppress(Exception):
            positions = _build_structure_position_preview(node, limit=50)

        # Extra info using pymatgen
        with suppress(Exception):
            from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
            struct = node.get_pymatgen_structure()
            sga = SpacegroupAnalyzer(struct)
            symmetry = {
                "number": int(sga.get_space_group_number()),
                "symbol": str(sga.get_space_group_symbol()),
                "crystal_system": str(sga.get_crystal_system()),
            }
            lat = struct.lattice
            lattice = {
                "a": float(lat.a),
                "b": float(lat.b),
                "c": float(lat.c),
                "alpha": float(lat.alpha),
                "beta": float(lat.beta),
                "gamma": float(lat.gamma),
            }

        return {
            "formula": get_structure_formula(node),
            "atom_count": atom_count,
            "cell_volume": round(float(cell_volume), 4) if cell_volume is not None else None,
            "positions": positions,
            "symmetry": symmetry,
            "lattice": lattice
        }
    def extract_payload(self, node: orm.StructureData) -> Any:
        return node.get_formula()


class BandsDataSerializer:
    def build_preview(self, node: orm.BandsData) -> dict[str, Any] | None:
        num_bands: int | None = None
        num_kpoints: int | None = None
        with suppress(Exception):
            bands = node.get_bands()
            shape = getattr(bands, "shape", None)
            if shape:
                if len(shape) >= 2:
                    num_kpoints = int(shape[-2])
                    num_bands = int(shape[-1])
                elif len(shape) == 1:
                    num_bands = int(shape[0])
        if num_kpoints is None:
            with suppress(Exception):
                kpoints = node.get_kpoints()
                num_kpoints = len(kpoints)
        return {"num_bands": num_bands, "num_kpoints": num_kpoints}
    def extract_payload(self, node: orm.BandsData) -> Any:
        return "BandsStructure"


class ArrayDataSerializer:
    def build_preview(self, node: orm.ArrayData) -> dict[str, Any] | None:
        array_info: list[dict[str, Any]] = []
        with suppress(Exception):
            for name in node.get_arraynames():
                info: dict[str, Any] = {"name": str(name)}
                with suppress(Exception):
                    info["shape"] = [int(dimension) for dimension in node.get_shape(name)]

                # Get a sample of the array data (first 5 elements)
                with suppress(Exception):
                    array = node.get_array(name)
                    # We use to_jsonable for numpy arrays
                    info["data"] = to_jsonable(array.flatten()[:5].tolist())

                array_info.append(info)
        return {"arrays": array_info}
    def extract_payload(self, node: orm.ArrayData) -> Any:
        return None


class RemoteDataSerializer:
    def build_preview(self, node: orm.RemoteData) -> dict[str, Any] | None:
        computer_label: str | None = None
        with suppress(Exception):
            computer = getattr(node, "computer", None)
            if computer is not None:
                computer_label = str(getattr(computer, "label", None) or getattr(computer, "name", None) or "")
        remote_path: str | None = None
        with suppress(Exception):
            remote_path = str(node.get_remote_path())
        return {
            "computer": computer_label or None,
            "path": shorten_path_for_preview(remote_path),
            "remote_path": remote_path,
        }
    def extract_payload(self, node: orm.RemoteData) -> Any:
        return None


class FolderDataSerializer:
    def build_preview(self, node: orm.FolderData) -> dict[str, Any] | None:
        file_names: list[str] = []
        with suppress(Exception):
            file_names = sorted([str(name) for name in node.list_object_names()])
        return {"file_count": len(file_names), "files": file_names[:3]}
    def extract_payload(self, node: orm.FolderData) -> Any:
        return node.list_object_names()


class ProcessNodeSerializer:
    def build_preview(self, node: orm.ProcessNode) -> dict[str, Any] | None:
        state = extract_process_state_value(node)
        return {"state": state, "execution_time_seconds": compute_process_execution_time_seconds(node, state=state)}
    def extract_payload(self, node: orm.ProcessNode) -> Any:
        return None


class DictSerializer:
    def build_preview(self, node: orm.Dict) -> dict[str, Any] | None:
        d = node.get_dict()
        keys = list(d.keys())
        # For large dicts, we still want to return a significant chunk but maybe not megalobytes
        # but 1MB of JSON is usually fine for a single node preview
        serialized = str(d)
        is_truncated = len(serialized) > 20000
        summary = serialized[:20000] + ("..." if is_truncated else "")
        return {
            "keys": keys[:10],
            "count": len(keys),
            "summary": summary,
            "data": d if not is_truncated else None # Only send full data if it's small
        }
    def extract_payload(self, node: orm.Dict) -> Any:
        return node.get_dict()


class ListSerializer:
    def build_preview(self, node: orm.List) -> dict[str, Any] | None:
        with suppress(Exception):
            l = node.get_list()
            return {"count": len(l), "summary": str(l)[:100] + ("..." if len(str(l)) > 100 else "")}
        return None
    def extract_payload(self, node: orm.List) -> Any:
        return node.get_list()


class ScalarSerializer:
    def build_preview(self, node: Any) -> dict[str, Any] | None:
        return {"value": str(getattr(node, "value", ""))}
    def extract_payload(self, node: Any) -> Any:
        return getattr(node, "value", None)


class CodeSerializer:
    def build_preview(self, node: orm.Code) -> dict[str, Any] | None:
        return None
    def extract_payload(self, node: orm.Code) -> Any:
        return node.full_label


class XyDataSerializer:
    def build_preview(self, node: orm.XyData) -> dict[str, Any] | None:
        preview: dict[str, Any] = {}
        with suppress(Exception):
            x_name, x_array, x_units = node.get_x()
            y_data = node.get_y()
            preview["x_label"] = str(x_name)
            preview["y_labels"] = [str(item[0]) for item in y_data]
            preview["x_sample"] = to_jsonable(x_array.tolist()[:5])
            if y_data:
                preview["y_sample"] = to_jsonable(y_data[0][1].tolist()[:5])
        return preview
    def extract_payload(self, node: orm.XyData) -> Any:
        return None


class KpointsDataSerializer:
    def build_preview(self, node: orm.KpointsData) -> dict[str, Any] | None:
        with suppress(Exception):
            mesh, offset = node.get_kpoints_mesh()
            return {
                "mode": "mesh",
                "mesh": to_jsonable(mesh),
                "offset": to_jsonable(offset),
            }

        with suppress(Exception):
            kpoints = node.get_kpoints()
            labels = getattr(node, "labels", None)
            return {
                "mode": "list",
                "num_points": int(len(kpoints)),
                "has_labels": bool(labels),
            }

        return None
    def extract_payload(self, node: orm.KpointsData) -> Any:
        try:
            mesh, offset = node.get_kpoints_mesh()
            return {"mode": "mesh", "mesh": to_jsonable(mesh), "offset": to_jsonable(offset)}
        except Exception:
            with suppress(Exception):
                kpoints = node.get_kpoints()
                return {"mode": "list", "num_points": len(kpoints), "points": to_jsonable(kpoints.tolist())}
        return None


_SCALAR_SERIALIZER = ScalarSerializer()

NODE_SERIALIZERS: dict[type[orm.Node], NodeSerializerStrategy] = {
    orm.StructureData: StructureDataSerializer(),
    orm.BandsData: BandsDataSerializer(),
    orm.ArrayData: ArrayDataSerializer(),
    orm.RemoteData: RemoteDataSerializer(),
    orm.FolderData: FolderDataSerializer(),
    orm.ProcessNode: ProcessNodeSerializer(),
    orm.Dict: DictSerializer(),
    orm.List: ListSerializer(),
    orm.XyData: XyDataSerializer(),
    orm.Int: _SCALAR_SERIALIZER,
    orm.Float: _SCALAR_SERIALIZER,
    orm.Str: _SCALAR_SERIALIZER,
    orm.Bool: _SCALAR_SERIALIZER,
    orm.Code: CodeSerializer(),
    orm.KpointsData: KpointsDataSerializer(),
}


def _get_serializer(node: orm.Node) -> NodeSerializerStrategy | None:
    # Exact match first
    node_type = type(node)
    if node_type in NODE_SERIALIZERS:
        return NODE_SERIALIZERS[node_type]

    # Subclass match
    for base_type, serializer in NODE_SERIALIZERS.items():
        if isinstance(node, base_type):
            return serializer

    return None


def build_node_preview(node: orm.Node) -> dict[str, Any] | None:
    serializer = _get_serializer(node)
    if serializer:
        return serializer.build_preview(node)
    return None


def _iterate_node_manager(manager: Any) -> list[tuple[str, Any]]:
    entries: list[tuple[str, Any]] = []
    if hasattr(manager, "_get_keys") and hasattr(manager, "_get_node_by_link_label"):
        try:
            for key in manager._get_keys():
                entries.append((str(key), manager._get_node_by_link_label(key)))
            return entries
        except Exception:
            entries = []

    try:
        keys = manager.keys()
    except Exception:
        return entries

    for key in keys:
        key_text = str(key).strip()
        if not key_text:
            continue
        try:
            entries.append((key_text, manager[key]))
        except Exception:
            continue
    return entries


def _flatten_node_manager(manager: Any, prefix: str = "") -> dict[str, orm.Node]:
    result: dict[str, orm.Node] = {}
    for key, value in _iterate_node_manager(manager):
        if isinstance(value, orm.Node):
            result[f"{prefix}{key}"] = value
            continue
        if hasattr(value, "_get_keys") or (
            hasattr(value, "keys") and not isinstance(value, Mapping)
        ):
            result.update(_flatten_node_manager(value, prefix=f"{prefix}{key}__"))
    return result


def serialize_direct_links(node: orm.Node, mode: str = "incoming") -> dict[str, Any]:
    qb = orm.QueryBuilder()
    qb.append(orm.Node, filters={"id": node.pk}, tag="center")

    project = [
        "id",
        "node_type",
        "label",
        "attributes.process_state",
        "attributes.exit_status",
    ]

    if mode == "incoming":
        qb.append(orm.Node, with_outgoing="center", project=project, edge_project=["label"])
    else:
        qb.append(orm.Node, with_incoming="center", project=project, edge_project=["label"])

    links: dict[str, Any] = {}
    counts: defaultdict[str, int] = defaultdict(int)
    for pk, raw_node_type, label, p_state, e_status, link_label in qb.all():
        unique_label = f"{link_label}_{counts[link_label]}" if counts[link_label] > 0 else link_label
        counts[link_label] += 1

        cleaned_type = str(raw_node_type or "").strip()
        parts = [p for p in cleaned_type.split(".") if p]
        resolved_type = parts[-2] if len(parts) >= 2 else cleaned_type

        links[unique_label] = {
            "pk": int(pk),
            "node_type": resolved_type,
            "label": str(label or resolved_type),
            "link_label": link_label,
            "state": str(p_state or "unknown"),
            "exit_status": e_status,
        }

    return links




def extract_node_payload(node: orm.Node) -> Any:
    serializer = _get_serializer(node)
    payload = None
    if serializer:
        try:
            payload = serializer.extract_payload(node)
        except Exception:  # noqa: BLE001
            payload = "Error loading content"
    return to_jsonable(payload)


def serialize_node(node: orm.Node) -> dict[str, Any]:
    type_name = node_type_name(node)
    info: dict[str, Any] = {
        "pk": int(node.pk),
        "uuid": str(node.uuid),
        "type": type_name,
        "node_type": type_name,
        "full_type": str(getattr(node, "node_type", node.__class__.__name__)),
        "label": str(getattr(node, "label", None) or ""),
        "ctime": node.ctime.strftime("%Y-%m-%d %H:%M:%S") if getattr(node, "ctime", None) else None,
    }

    if not info["label"]:
        if isinstance(node, (orm.Int, orm.Float, orm.Str, orm.Bool)):
            info["label"] = str(node.value)
        elif isinstance(node, orm.Code):
            info["label"] = node.label
        else:
            info["label"] = type_name

    payload = extract_node_payload(node)
    if payload is not None:
        info["payload"] = payload

    preview = build_node_preview(node)
    if preview is not None:
        info["preview_info"] = preview

    if isinstance(node, orm.ProcessNode):
        info["state"] = extract_process_state_value(node)
        info["exit_status"] = getattr(node, "exit_status", None)
        info["process_label"] = str(getattr(node, "process_label", None) or "N/A")

    return info


def get_node_summary(node_pk: int) -> dict[str, Any]:
    try:
        node = orm.load_node(node_pk)
    except Exception as exc:  # noqa: BLE001
        raise http_error(404, "Node not found", pk=node_pk, reason=str(exc)) from exc

    info = serialize_node(node)
    direct_inputs = serialize_direct_links(node, mode="incoming")
    direct_outputs = serialize_direct_links(node, mode="outgoing")

    # ProcessNode specific fields might already be populated by serialize_node,
    # but get_node_summary explicitly returns them even if empty or defaults
    if "state" not in info:
        info["state"] = "N/A"
    if "process_label" not in info:
        info["process_label"] = "N/A"
    if "exit_status" not in info:
        info["exit_status"] = "N/A"

    info["incoming"] = len(direct_inputs)
    info["outgoing"] = len(direct_outputs)
    info["attributes"] = to_jsonable(node.base.attributes.all)

    # Inspector links should reflect only the node's direct input/output port mappings.
    info["inputs"] = direct_inputs
    info["outputs"] = direct_outputs
    info["direct_inputs"] = direct_inputs
    info["direct_outputs"] = direct_outputs

    # Remove payload from summary for lightweight response
    info.pop("payload", None)

    return info
