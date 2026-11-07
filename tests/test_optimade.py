import pytest

from aiida_mcp.optimade import _summary, build_filter, reduced_formula, to_artifact
from aiida_mcp.worker.protocol.structures import StructureImportRequest


@pytest.mark.parametrize(("raw", "expected"), [("SiO2", "O2Si"), ("Si2", "Si"), ("MoS2", "MoS2"), ("Fe2O3", "Fe2O3"), ("O4Si2", "O2Si")])
def test_reduced_formula_is_alphabetical_and_reduced(raw, expected) -> None:
    assert reduced_formula(raw) == expected


def test_bad_formula_and_empty_filter_are_rejected() -> None:
    with pytest.raises(ValueError):
        reduced_formula("si-o2")
    with pytest.raises(ValueError):
        build_filter(formula=None, elements=None, elements_only=False, max_sites=None)
    assert build_filter(formula=None, elements=["Si", "O"], elements_only=True, max_sites=6) == 'elements HAS ALL "O","Si" AND nelements=2 AND nsites<=6'


MC2D_ENTRY = {"id": "abc", "attributes": {
    "chemical_formula_reduced": "MoS2", "nsites": 3, "dimension_types": [1, 1, 0], "immutable_id": "abc",
    "lattice_vectors": [[3.19, 0, 0], [-1.59, 2.76, 0], [0, 0, 23.2]],
    "species": [{"name": "Mo", "chemical_symbols": ["Mo"], "concentration": [1.0]}, {"name": "S", "chemical_symbols": ["S"], "concentration": [1.0]}],
    "species_at_sites": ["Mo", "S", "S"], "cartesian_site_positions": [[0, 0, 11.6], [1.59, 0.92, 13.2], [1.59, 0.92, 10.0]],
    "_mcloud_band_gap": 1.62}}


def test_mc2d_entry_maps_to_a_valid_import_request_with_2d_pbc() -> None:
    artifact = to_artifact("mc2d", MC2D_ENTRY)
    assert artifact["periodic_boundary_conditions"] == [True, True, False]
    assert artifact["source"]["provider_fields"]["_mcloud_band_gap"] == 1.62
    request = StructureImportRequest.model_validate({"artifact": artifact})
    assert request.artifact.source.source_key == "optimade:mc2d:abc:abc"
    row = _summary("mc2d", MC2D_ENTRY)
    assert row["band_gap_eV"] == 1.62 and row["lattice_lengths"][2] == 23.2 and "lattice_vectors" not in row
