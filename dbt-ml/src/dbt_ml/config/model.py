from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class ExtractionConfig(BaseModel):
    backend: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)


class TransformConfig(BaseModel):
    type: str
    module: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)


class MLArtifactConfig(BaseModel):
    path: Path | None = None
    include_metrics: bool = True

    @field_serializer("path")
    def _serialize_path(self, path: Path | None) -> str | None:
        return path.as_posix() if path is not None else None


class MLConfig(BaseModel):
    task: Literal[
        "features",
        "classifier",
        "regressor",
        "cluster",
        "topic_model",
        "nlp",
    ]
    mode: Literal["fit_transform", "fit", "predict", "load_pretrained"] = "fit_transform"
    provider: str | None = None
    text_field: str | None = None
    label_field: str | None = None
    artifact: MLArtifactConfig = Field(default_factory=MLArtifactConfig)
    metrics: list[str] = Field(default_factory=list)
    options: dict[str, Any] = Field(default_factory=dict)


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
    ml: MLConfig | None = None
    fields: list[FieldConfig] = Field(default_factory=list)
    materialization: Literal["full", "incremental"] = "full"
    tests: list[Any] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)


class ModelFile(BaseModel):
    version: int = 2
    models: list[ModelConfig]
