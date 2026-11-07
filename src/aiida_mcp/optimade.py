"""Search OPTIMADE databases (MC3D, MC2D, Materials Project) from the server process.

Only HTTP lives here; storing a structure goes through the worker's `structure.import`, which records the
source and deduplicates by it. Results are deliberately compact: one short row per candidate, no arrays.
"""
from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from functools import reduce
from typing import Any

import httpx

PROVIDERS: dict[str, dict[str, str]] = {
    "mc3d": {"name": "Materials Cloud MC3D (PBE)", "base": "https://optimade.materialscloud.org/main/mc3d-pbe-v1/v1"},
    "mc2d": {"name": "Materials Cloud MC2D", "base": "https://optimade.materialscloud.org/main/mc2d/v1"},
    "mp": {"name": "Materials Project", "base": "https://optimade.materialsproject.org/v1"},
}
_FIELDS = "id,chemical_formula_reduced,nsites,nelements,lattice_vectors,dimension_types,last_modified,immutable_id"
_EXTRA_FIELDS = {
    "mc3d": ",_mcloud_mc3d_id,_mcloud_source_db,_mcloud_source_db_id,_mcloud_total_energy,_mcloud_cell_volume,_mcloud_total_magnetization",
    "mc2d": ",_mcloud_band_gap",
    "mp": ",_mp_stability",
}
_TOKEN = re.compile(r"([A-Z][a-z]?)(\d*)")


def reduced_formula(formula: str) -> str:
    """'SiO2' -> 'O2Si', 'Si2' -> 'Si': OPTIMADE's alphabetical, reduced form."""
    counts: dict[str, int] = {}
    cleaned = formula.replace(" ", "")
    if not cleaned or "".join(m.group(0) for m in _TOKEN.finditer(cleaned)) != cleaned:
        raise ValueError(f"Cannot parse formula {formula!r}; use element symbols and integers, e.g. 'SiO2'")
    for symbol, count in _TOKEN.findall(cleaned):
        counts[symbol] = counts.get(symbol, 0) + int(count or 1)
    divisor = reduce(math.gcd, counts.values())
    return "".join(f"{symbol}{'' if n // divisor == 1 else n // divisor}" for symbol, n in sorted(counts.items()))


def _provider(key: str) -> dict[str, str]:
    if key not in PROVIDERS:
        raise ValueError(f"Unknown provider {key!r}; choose one of {', '.join(PROVIDERS)}")
    return PROVIDERS[key]


def build_filter(*, formula: str | None, elements: list[str] | None, elements_only: bool, max_sites: int | None) -> str:
    clauses = []
    if formula:
        clauses.append(f'chemical_formula_reduced="{reduced_formula(formula)}"')
    if elements:
        symbols = sorted({str(e).strip() for e in elements})
        clauses.append("elements HAS ALL " + ",".join(f'"{s}"' for s in symbols))
        if elements_only:
            clauses.append(f"nelements={len(symbols)}")
    if max_sites:
        clauses.append(f"nsites<={int(max_sites)}")
    if not clauses:
        raise ValueError("Give a formula and/or elements")
    return " AND ".join(clauses)


def _lengths(vectors: list[list[float]] | None) -> list[float] | None:
    if not vectors:
        return None
    return [round(math.sqrt(sum(c * c for c in v)), 4) for v in vectors]


def _summary(provider: str, entry: dict[str, Any]) -> dict[str, Any]:
    a = entry["attributes"]
    row: dict[str, Any] = {
        "id": entry["id"], "formula": a.get("chemical_formula_reduced"), "nsites": a.get("nsites"),
        "lattice_lengths": _lengths(a.get("lattice_vectors")), "periodic": a.get("dimension_types"),
    }
    if provider == "mc3d":
        row.update({"mc3d_id": a.get("_mcloud_mc3d_id"), "source": f"{a.get('_mcloud_source_db')}:{a.get('_mcloud_source_db_id')}",
                    "total_energy_eV": a.get("_mcloud_total_energy"), "volume_A3": a.get("_mcloud_cell_volume"),
                    "magnetization": a.get("_mcloud_total_magnetization")})
        if a.get("_mcloud_total_energy") is not None and a.get("nsites"):
            row["energy_per_atom_eV"] = round(a["_mcloud_total_energy"] / a["nsites"], 4)
    elif provider == "mc2d":
        row["band_gap_eV"] = a.get("_mcloud_band_gap")
    elif provider == "mp":
        stability = (a.get("_mp_stability") or {}).get("gga_gga+u") or {}
        row["e_above_hull_eV_per_atom"] = stability.get("energy_above_hull")
    return {key: value for key, value in row.items() if value is not None}


async def search_structures(provider: str, *, formula: str | None = None, elements: list[str] | None = None,
                            elements_only: bool = False, max_sites: int | None = None, limit: int = 10) -> dict[str, Any]:
    spec = _provider(provider)
    query = build_filter(formula=formula, elements=elements, elements_only=elements_only, max_sites=max_sites)
    params = {"filter": query, "page_limit": max(1, min(int(limit), 50)), "response_fields": _FIELDS + _EXTRA_FIELDS[provider]}
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        response = await client.get(f"{spec['base']}/structures", params=params)
        response.raise_for_status()
        payload = response.json()
    rows = [_summary(provider, entry) for entry in payload.get("data", [])]
    if provider == "mc3d":
        rows.sort(key=lambda row: row.get("energy_per_atom_eV", math.inf))
    elif provider == "mp":
        rows.sort(key=lambda row: row.get("e_above_hull_eV_per_atom", math.inf))
    return {"provider": spec["name"], "filter": query, "total_matches": payload.get("meta", {}).get("data_returned"),
            "returned": len(rows), "results": rows,
            "hint": "Store one with import_structure(provider, id); it returns a PK for {\"pk\": N}."}


def to_artifact(provider: str, entry: dict[str, Any]) -> dict[str, Any]:
    """Map one OPTIMADE structure entry to the worker's StructureArtifact payload."""
    spec, a = _provider(provider), entry["attributes"]
    species = {item["name"]: item for item in a.get("species") or []}
    sites = []
    for name, position in zip(a["species_at_sites"], a["cartesian_site_positions"]):
        item = species.get(name) or {"chemical_symbols": [name], "concentration": [1.0]}
        components = [{"symbol": s, "concentration": c} for s, c in zip(item["chemical_symbols"], item["concentration"]) if s != "vacancy"]
        if not components:
            raise ValueError(f"Site {name!r} is a pure vacancy; AiiDA cannot store it")
        sites.append({"position": position, "species": components, "name": name})
    provider_fields = {key: value for key, value in a.items() if key.startswith("_") and value is not None}
    if a.get("last_modified"):
        provider_fields["last_modified"] = a["last_modified"]
    return {
        "formula": a.get("chemical_formula_reduced"),
        "lattice_vectors": a["lattice_vectors"],
        "periodic_boundary_conditions": [bool(flag) for flag in (a.get("dimension_types") or [1, 1, 1])],
        "sites": sites,
        "source": {
            "provider": "optimade", "database": provider, "entry_id": str(entry["id"]),
            "immutable_id": a.get("immutable_id"), "url": f"{spec['base']}/structures/{entry['id']}",
            "retrieved_at": datetime.now(timezone.utc).isoformat(), "provider_fields": provider_fields,
        },
    }


async def fetch_entry(provider: str, entry_id: str) -> dict[str, Any]:
    spec = _provider(provider)
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        response = await client.get(f"{spec['base']}/structures/{entry_id}")
        if response.status_code == 404:
            raise ValueError(f"No {provider} structure with id {entry_id!r}")
        response.raise_for_status()
        return response.json()["data"]
