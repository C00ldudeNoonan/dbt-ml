from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ExtractionConfig(BaseModel):
    backend: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)


class TransformConfig(BaseModel):
    type: str
    module: str | None = None


class FieldConfig(BaseModel):
    name: str
    description: str | None = None


class ModelConfig(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    name: str
    description: str | None = None
    source: str | None = None
    depends_on: list[str] | None = None
    extraction: ExtractionConfig | None = None
    transform: TransformConfig | None = None
    fields: list[FieldConfig] = Field(default_factory=list)
    materialization: Literal["full", "incremental"] = "full"
    tests: list[Any] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)


class ModelFile(BaseModel):
    version: int = 2
    models: list[ModelConfig]
