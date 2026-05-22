from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class DuckDBConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    path: Path = Path("./target/docbt.duckdb")
    schema_name: str = Field(default="docbt", alias="schema")


class ExtractionDefaults(BaseModel):
    default_backend: str = "json"


class ProjectConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True, protected_namespaces=())

    name: str
    version: str = "0.1.0"
    profile: str | None = None
    duckdb: DuckDBConfig = Field(default_factory=DuckDBConfig)
    extraction: ExtractionDefaults = Field(default_factory=ExtractionDefaults)

    source_paths: list[Path] = Field(
        default_factory=lambda: [Path("sources")], alias="source-paths"
    )
    model_paths: list[Path] = Field(
        default_factory=lambda: [Path("models")], alias="model-paths"
    )
    transform_paths: list[Path] = Field(
        default_factory=lambda: [Path("transforms")], alias="transform-paths"
    )
    target_path: Path = Field(default=Path("target"), alias="target-path")
