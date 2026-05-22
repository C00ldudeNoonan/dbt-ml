from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import polars as pl

from ..config.profile import LLMConfig, WarehouseConfig
from ..versioning import resolve_module_file


@dataclass(frozen=True)
class TransformContext:
    """Passed to transforms that declare a second arg.

    Lets transforms reach the resolved profile (e.g. LLM config) without
    hard-coding cache paths or model ids in the transform module.
    """

    project_dir: Path
    profile_name: str
    target_name: str
    warehouse: WarehouseConfig
    llm: LLMConfig | None


class TransformFn(Protocol):
    def __call__(self, deps: dict[str, pl.DataFrame]) -> pl.DataFrame: ...


def load_transform(module_path: str, project_dir: Path) -> TransformFn:
    """Load a user-defined transform module from disk and return its `run` callable."""
    file_path = resolve_module_file(module_path, project_dir)
    if not file_path.exists():
        raise FileNotFoundError(f"Transform module not found: {file_path}")

    spec = importlib.util.spec_from_file_location(module_path, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load transform module: {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_path] = module
    spec.loader.exec_module(module)

    run_fn = getattr(module, "run", None)
    if run_fn is None or not callable(run_fn):
        raise AttributeError(
            f"Transform '{module_path}' must define a top-level "
            f"`run(deps: dict[str, polars.DataFrame]) -> polars.DataFrame`"
        )
    return run_fn  # type: ignore[no-any-return]
