from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ExtractionResult:
    """Output of a single document extraction.

    `fields` holds the projected field values. `warnings` collects
    non-fatal issues surfaced by the backend.
    """

    fields: dict[str, Any]
    warnings: list[str] = field(default_factory=list)


class BaseBackend(ABC):
    """Contract every extraction backend implements."""

    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def supported_formats(self) -> list[str]: ...

    @abstractmethod
    def extract(self, path: Path, options: dict[str, Any]) -> ExtractionResult: ...

    def validate(self) -> None:
        """Raise if the backend's runtime deps are missing. Default: no-op."""
        return None
