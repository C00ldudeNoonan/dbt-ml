from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class DurationSpec(BaseModel):
    count: int
    period: Literal["minute", "hour", "day", "week"]

    def to_seconds(self) -> int:
        per = {"minute": 60, "hour": 3600, "day": 86400, "week": 604800}
        return self.count * per[self.period]


class FreshnessConfig(BaseModel):
    warn_after: DurationSpec | None = None
    error_after: DurationSpec | None = None


class SourceConfig(BaseModel):
    name: str
    description: str | None = None
    path: str
    file_pattern: str = "*.json"
    recursive: bool = True
    tags: list[str] = Field(default_factory=list)
    freshness: FreshnessConfig | None = None


class SourceFile(BaseModel):
    version: int = 2
    sources: list[SourceConfig]
