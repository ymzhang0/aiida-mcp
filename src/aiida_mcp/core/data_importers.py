from __future__ import annotations

import json
import io
from typing import Any

import pandas as pd
import yaml
from aiida import orm
from aiida.plugins import DataFactory
from aiida.tools.archive import import_archive as aiida_import_archive

def import_archive(file_content: bytes, filename: str) -> dict[str, Any]:
    """Import an AiiDA archive from bytes."""
    import tempfile
    import os

    with tempfile.NamedTemporaryFile(suffix=".aiida", delete=False) as tmp:
        tmp.write(file_content)
        tmp_path = tmp.name

    try:
        # aiida_import_archive returns information about the imported entities
        aiida_import_archive(tmp_path)
        return {"status": "success", "message": f"Archive {filename} imported successfully"}
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

def import_structure(file_content: bytes, filename: str, format: str | None = None) -> orm.StructureData:
    """Import a structure from file content using ASE."""
    import ase.io

    # Map common extensions to ASE formats if not provided
    if not format:
        ext = filename.split(".")[-1].lower()
        format_map = {
            "cif": "cif",
            "xyz": "xyz",
            "poscar": "vasp",
            "vasp": "vasp",
            "res": "res",
        }
        format = format_map.get(ext)

    # Use a temporary stream for ASE
    stream = io.BytesIO(file_content)
    # Note: ASE might need a text stream for some formats, but BytesIO usually works or can be wrapped
    try:
        # Try reading as text if bytes fails for some reason
        text_content = file_content.decode("utf-8")
        text_stream = io.StringIO(text_content)
        atoms = ase.io.read(text_stream, format=format)
    except Exception:
        stream.seek(0)
        atoms = ase.io.read(stream, format=format)

    structure = orm.StructureData(ase=atoms)
    return structure

def import_dict(file_content: bytes, filename: str, format: str | None = None) -> orm.Dict:
    """Import a dictionary from JSON or YAML."""
    text_content = file_content.decode("utf-8")

    if not format:
        ext = filename.split(".")[-1].lower()
        if ext in ("yaml", "yml"):
            format = "yaml"
        else:
            format = "json"

    if format == "yaml":
        data = yaml.safe_load(text_content)
    else:
        data = json.loads(text_content)

    if not isinstance(data, dict):
        raise ValueError(f"Imported data must be a dictionary, got {type(data)}")

    return orm.Dict(dict=data)

def import_array(file_content: bytes, filename: str) -> orm.ArrayData:
    """Import an array from CSV using Pandas."""
    stream = io.BytesIO(file_content)
    df = pd.read_csv(stream)

    array = orm.ArrayData()
    for column in df.columns:
        array.set_array(column, df[column].values)

    return array

def import_kpoints(file_content: bytes, filename: str) -> orm.KpointsData:
    """Import k-points from a simple list of coordinates (JSON or CSV)."""
    text_content = file_content.decode("utf-8")
    ext = filename.split(".")[-1].lower()

    kpoints = orm.KpointsData()

    if ext == "json":
        data = json.loads(text_content)
        if isinstance(data, list):
            kpoints.set_kpoints(data)
        elif isinstance(data, dict) and "points" in data:
            kpoints.set_kpoints(data["points"])
        else:
            raise ValueError("Invalid JSON format for KpointsData. Expected a list of coordinates.")
    else:
        # Assume CSV
        stream = io.StringIO(text_content)
        df = pd.read_csv(stream, header=None)
        kpoints.set_kpoints(df.values)

    return kpoints

def create_node_from_file(
    data_type: str,
    file_content: bytes | str,
    filename: str | None = None,
    label: str | None = None,
    description: str | None = None,
    source_type: str = "file"
) -> orm.Node | dict[str, Any]:
    """Main entry point to create and store a node from uploaded file content or raw text."""

    # Special case for AiiDA archives
    if data_type == "Archive" or (filename and filename.endswith(".aiida")):
        if isinstance(file_content, str):
             raise ValueError("Archive import only supports binary file uploads.")
        return import_archive(file_content, filename or "archive.aiida")

    importers = {
        "StructureData": import_structure,
        "Dict": import_dict,
        "ArrayData": import_array,
        "KpointsData": import_kpoints,
    }

    # Normalize data_type
    normalized_type = data_type.split(".")[-1]
    for key in importers:
        if key.lower() == normalized_type.lower():
            normalized_type = key
            break

    if normalized_type not in importers:
        raise ValueError(f"Unsupported data type for import: {data_type}")

    importer = importers[normalized_type]

    # Convert string to bytes if needed for importers that expect bytes
    if isinstance(file_content, str):
        content_bytes = file_content.encode("utf-8")
    else:
        content_bytes = file_content

    node = importer(content_bytes, filename or f"data.{normalized_type.lower()}")

    if label:
        node.label = label
    if description:
        node.description = description

    node.store()
    return node
