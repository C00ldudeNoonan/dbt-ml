from __future__ import annotations

from pathlib import Path

from ..config.profile import WarehouseConfig
from .base import AdapterError, WarehouseAdapter


class UnknownAdapterError(AdapterError):
    pass


_REGISTRY: dict[str, type[WarehouseAdapter]] = {}


def register(cls: type[WarehouseAdapter]) -> type[WarehouseAdapter]:
    _REGISTRY[cls.adapter_type()] = cls
    return cls


def create_adapter(
    config: WarehouseConfig, *, project_dir: Path | None = None
) -> WarehouseAdapter:
    cls = _REGISTRY.get(config.type)
    if cls is None:
        raise UnknownAdapterError(
            f"No adapter registered for warehouse.type='{config.type}'. "
            f"Known: {sorted(_REGISTRY)}"
        )
    return cls(config, project_dir=project_dir)


def list_adapter_types() -> list[str]:
    return sorted(_REGISTRY)
