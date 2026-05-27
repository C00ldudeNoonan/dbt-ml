from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class WarehouseConfig(BaseModel):
    """Where docbt writes materialized tables. Adapter chosen by `type:`.

    v0.2: `duckdb` (current default), `lancedb` (incoming).
    v0.3+: `postgres`, `snowflake`, `bigquery`, `databricks`, `redshift`.
    """

    model_config = ConfigDict(populate_by_name=True)

    type: str = "duckdb"
    path: Path
    schema_name: str = Field(default="docbt", alias="schema")


class LLMConfig(BaseModel):
    """Defaults for the LLM extraction backend and LLM-using transforms."""

    provider: Literal["anthropic"] = "anthropic"
    model: str = "claude-haiku-4-5"
    api_key_env: str = "ANTHROPIC_API_KEY"
    cache_path: Path | None = None
    system_prompt: str | None = None


class TargetConfig(BaseModel):
    warehouse: WarehouseConfig
    llm: LLMConfig | None = None


class ProfileConfig(BaseModel):
    """A named profile: one or more targets (e.g. dev/prod), plus default target."""

    target: str = "dev"
    outputs: dict[str, TargetConfig]
