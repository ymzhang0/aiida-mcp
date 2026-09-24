from __future__ import annotations

from aiida import orm

from aiida_mcp.worker.protocol.structures import (
    StructureImportReceipt,
    StructureImportRequest,
)

_SOURCE_KEY_EXTRA = "aris_source_key"
_SOURCE_EXTRA = "aris_structure_source"


def _find_existing_structure(source_key: str):
    query = orm.QueryBuilder()
    query.append(
        orm.StructureData,
        filters={f"extras.{_SOURCE_KEY_EXTRA}": {"==": source_key}},
        project=["*"],
    )
    result = query.first()
    if not result:
        return None
    return result[0]


def import_structure(request: StructureImportRequest) -> StructureImportReceipt:
    artifact = request.artifact
    source_key = artifact.source.source_key
    if request.deduplicate:
        existing = _find_existing_structure(source_key)
        if existing is not None:
            return StructureImportReceipt(
                pk=existing.pk,
                uuid=str(existing.uuid),
                formula=existing.get_formula(),
                source_key=source_key,
                deduplicated=True,
            )

    structure = orm.StructureData(
        cell=[list(vector) for vector in artifact.lattice_vectors],
        pbc=tuple(artifact.periodic_boundary_conditions),
    )
    for site in artifact.sites:
        symbols = [component.symbol for component in site.species]
        weights = [component.concentration for component in site.species]
        atom_kwargs = {
            "position": tuple(site.position),
            "symbols": symbols[0] if len(symbols) == 1 else symbols,
            "name": site.name or None,
        }
        if len(symbols) > 1 or weights[0] != 1.0:
            atom_kwargs["weights"] = weights[0] if len(weights) == 1 else weights
        structure.append_atom(
            **atom_kwargs,
        )

    structure.label = (
        str(request.label or "").strip()
        or artifact.formula
        or f"{artifact.source.provider}:{artifact.source.entry_id}"
    )
    structure.description = str(request.description or "").strip()
    structure.store()
    structure.base.extras.set(_SOURCE_KEY_EXTRA, source_key)
    structure.base.extras.set(
        _SOURCE_EXTRA,
        artifact.source.model_dump(mode="json"),
    )

    return StructureImportReceipt(
        pk=structure.pk,
        uuid=str(structure.uuid),
        formula=structure.get_formula(),
        source_key=source_key,
        deduplicated=False,
    )


__all__ = ["import_structure"]
