from __future__ import annotations

import math
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_ELEMENT_PATTERN = re.compile(r"^[A-Z][a-z]?$|^X$")


class SpeciesComponent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    concentration: float = Field(gt=0, le=1)

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        symbol = str(value or "").strip()
        if not _ELEMENT_PATTERN.fullmatch(symbol):
            raise ValueError(f"Invalid chemical element symbol: {symbol!r}")
        return symbol


class StructureSite(BaseModel):
    model_config = ConfigDict(extra="forbid")

    position: tuple[float, float, float]
    species: list[SpeciesComponent] = Field(min_length=1)
    name: str | None = None

    @field_validator("position")
    @classmethod
    def validate_position(cls, value: tuple[float, float, float]):
        if not all(math.isfinite(component) for component in value):
            raise ValueError("Site positions must be finite")
        return value

    @model_validator(mode="after")
    def validate_concentrations(self) -> "StructureSite":
        if sum(item.concentration for item in self.species) > 1.0 + 1e-6:
            raise ValueError("Species concentrations at each site cannot exceed one")
        return self


class StructureSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str = Field(min_length=1)
    database: str = Field(min_length=1)
    entry_id: str = Field(min_length=1)
    immutable_id: str | None = None
    url: str = Field(min_length=1)
    retrieved_at: str
    database_version: str | None = None
    license: str | None = None
    citations: list[str] = Field(default_factory=list)
    provider_fields: dict[str, Any] = Field(default_factory=dict)

    @property
    def source_key(self) -> str:
        version = (
            self.immutable_id
            or self.database_version
            or self.provider_fields.get("last_modified")
            or "current"
        )
        return f"{self.provider}:{self.database}:{self.entry_id}:{version}"


class StructureArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    formula: str | None = None
    lattice_vectors: tuple[
        tuple[float, float, float],
        tuple[float, float, float],
        tuple[float, float, float],
    ]
    periodic_boundary_conditions: tuple[bool, bool, bool] = (True, True, True)
    sites: list[StructureSite] = Field(min_length=1)
    source: StructureSource

    @field_validator("lattice_vectors")
    @classmethod
    def validate_lattice(cls, value):
        if not all(math.isfinite(component) for vector in value for component in vector):
            raise ValueError("Lattice vectors must be finite")
        return value


class StructureImportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifact: StructureArtifact
    label: str | None = None
    description: str | None = None
    deduplicate: bool = True


class StructureImportReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str = "success"
    pk: int = Field(gt=0)
    uuid: str
    formula: str | None = None
    source_key: str
    deduplicated: bool = False


__all__ = [
    "SpeciesComponent",
    "StructureArtifact",
    "StructureImportReceipt",
    "StructureImportRequest",
    "StructureSite",
    "StructureSource",
]
