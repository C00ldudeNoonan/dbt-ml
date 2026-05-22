"""Custom Python tests: user-supplied modules that return None (pass) or a
failure message string (fail)."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import duckdb

from ..versioning import resolve_module_file


class CustomTestError(Exception):
    pass


def run_python_test(
    module_path: str,
    project_dir: Path,
    con: duckdb.DuckDBPyConnection,
    table_ref: str,
) -> str | None:
    """Load `module_path` and call its `run(con, table_ref)`.

    Returns None for pass, a string message for fail.
    """
    file_path = resolve_module_file(module_path, project_dir)
    if not file_path.exists():
        raise CustomTestError(f"Custom test module not found: {file_path}")

    spec = importlib.util.spec_from_file_location(module_path, file_path)
    if spec is None or spec.loader is None:
        raise CustomTestError(f"Could not load test module: {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_path] = module
    spec.loader.exec_module(module)

    run_fn: Any = getattr(module, "run", None)
    if run_fn is None or not callable(run_fn):
        raise CustomTestError(
            f"Custom test '{module_path}' must define `run(con, table_ref) -> str | None`"
        )

    result = run_fn(con, table_ref)
    if result is None:
        return None
    if not isinstance(result, str):
        return str(result)
    return result
