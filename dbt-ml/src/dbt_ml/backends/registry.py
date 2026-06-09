from __future__ import annotations

from .base import BaseBackend


class BackendNotFoundError(Exception):
    pass


_REGISTRY: dict[str, type[BaseBackend]] = {}


def register(backend_cls: type[BaseBackend]) -> type[BaseBackend]:
    """Decorator: register a backend class by its `name()`."""
    instance = backend_cls()
    _REGISTRY[instance.name()] = backend_cls
    return backend_cls


def get_backend(name: str) -> BaseBackend:
    cls = _REGISTRY.get(name)
    if cls is None:
        raise BackendNotFoundError(
            f"Backend '{name}' is not registered. Available: {sorted(_REGISTRY)}"
        )
    return cls()


def list_backends() -> list[str]:
    return sorted(_REGISTRY)
