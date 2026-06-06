"""Translate a docbt project into a dbt-compatible sources.yml.

The idea: a dbt-duckdb project pointed at the same DuckDB file can consume
docbt-materialized tables via `{{ source('docbt_<project>', '<model>') }}`.
This module emits the sources.yml declaration that makes that work.

The emitted YAML is validated against the strict dbt Fusion engine in CI
(`.github/workflows/dbt-fusion.yml`). Fusion fails the parse — rather than
warning — on undeclared macro tests, so anything we emit here that depends on
a dbt package (e.g. dbt_utils) is paired with a generated `packages.yml`.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .config import load_project
from .config.model import ModelConfig
from .config.profile import WarehouseConfig
from .dag import ProjectDAG
from .profile import resolve_profile

DEFAULT_OUTPUT_FILENAME = "sources.yml"
DEFAULT_PACKAGES_FILENAME = "packages.yml"

# dbt_utils version range that ships the macro tests we emit. Pinned to the 1.x
# line, which both dbt-core (>=1.3) and the Fusion engine resolve.
DBT_UTILS_PACKAGE = "dbt-labs/dbt_utils"
DBT_UTILS_VERSION = [">=1.1.0", "<2.0.0"]


def build_dbt_sources(
    project_dir: Path,
    *,
    source_name: str | None = None,
    select: str | None = None,
    exclude: str | None = None,
    target: str | None = None,
    profiles_dir: Path | None = None,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    """Build a dbt v2 `sources.yml` payload for docbt's materialized tables.

    `warnings`, when provided, is populated with human-readable notes about test
    specs that have no faithful dbt source-test equivalent (so the caller can
    surface them instead of dropping them silently).
    """
    project, sources_cfg, models = load_project(project_dir)
    resolved = resolve_profile(
        project, project_dir, target=target, profiles_dir=profiles_dir
    )
    dag = ProjectDAG(sources_cfg, models)
    selected_names = set(dag.select_models(select=select, exclude=exclude))
    selected_models = [m for m in models if m.name in selected_names]

    name = source_name or f"docbt_{project.name}"
    catalog = _derive_catalog(resolved.warehouse)

    return {
        "version": 2,
        "sources": [
            {
                "name": name,
                "description": (
                    f"Tables materialized by docbt project '{project.name}'."
                ),
                "database": catalog,
                "schema": resolved.warehouse.schema_name,
                "tables": [
                    _table_for_model(m, warnings) for m in selected_models
                ],
            }
        ],
    }


def requires_dbt_utils(payload: dict[str, Any]) -> bool:
    """Whether the emitted sources reference a dbt_utils macro test.

    Fusion rejects undeclared macros at parse time, so when this is true the
    consuming project needs a `packages.yml` declaring dbt_utils. Use
    `build_dbt_packages` / `write_dbt_packages` to generate one.
    """
    for source in payload.get("sources", []):
        for table in source.get("tables", []):
            for spec in table.get("tests", []):
                if isinstance(spec, dict) and any(
                    str(k).startswith("dbt_utils.") for k in spec
                ):
                    return True
    return False


def build_dbt_packages() -> dict[str, Any]:
    """A `packages.yml` payload declaring the packages our macro tests need."""
    return {
        "packages": [
            {"package": DBT_UTILS_PACKAGE, "version": list(DBT_UTILS_VERSION)}
        ]
    }


def write_dbt_packages(output: Path) -> Path:
    """Write a `packages.yml` next to the emitted sources file."""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        yaml.safe_dump(build_dbt_packages(), sort_keys=False, default_flow_style=False)
    )
    return output


def write_dbt_sources(
    project_dir: Path,
    *,
    source_name: str | None = None,
    select: str | None = None,
    exclude: str | None = None,
    output: Path | None = None,
    target: str | None = None,
    profiles_dir: Path | None = None,
    emit_packages: bool = False,
    warnings: list[str] | None = None,
) -> Path:
    project, _, _ = load_project(project_dir)
    payload = build_dbt_sources(
        project_dir,
        source_name=source_name,
        select=select,
        exclude=exclude,
        target=target,
        profiles_dir=profiles_dir,
        warnings=warnings,
    )

    if output is None:
        target_dir = (project_dir / project.target_path).resolve()
        target_dir.mkdir(parents=True, exist_ok=True)
        output = target_dir / DEFAULT_OUTPUT_FILENAME
    else:
        output.parent.mkdir(parents=True, exist_ok=True)

    output.write_text(yaml.safe_dump(payload, sort_keys=False, default_flow_style=False))

    if requires_dbt_utils(payload):
        if emit_packages:
            write_dbt_packages(output.parent / DEFAULT_PACKAGES_FILENAME)
        elif warnings is not None:
            warnings.append(
                "emitted a dbt_utils macro test but no packages.yml — dbt Fusion "
                "will fail to parse until dbt_utils is declared. Re-run with "
                "--emit-packages or add dbt_utils to the project's packages.yml."
            )

    return output


def _derive_catalog(warehouse: WarehouseConfig) -> str:
    """DuckDB catalog name == basename of the database file without extension."""
    return Path(warehouse.path).stem


def _table_for_model(
    model: ModelConfig, warnings: list[str] | None = None
) -> dict[str, Any]:
    columns_by_name: dict[str, dict[str, Any]] = {}

    for field in model.fields:
        columns_by_name[field.name] = {"name": field.name}
        if field.description:
            columns_by_name[field.name]["description"] = field.description

    table_tests: list[Any] = []
    for spec in model.tests:
        _apply_test_spec(spec, columns_by_name, table_tests, model.name, warnings)

    table: dict[str, Any] = {"name": model.name}
    if model.description:
        table["description"] = model.description
    if model.tags:
        table["tags"] = model.tags

    if columns_by_name:
        table["columns"] = list(columns_by_name.values())
    if table_tests:
        table["tests"] = table_tests
    return table


def _apply_test_spec(
    spec: Any,
    columns_by_name: dict[str, dict[str, Any]],
    table_tests: list[Any],
    model_name: str,
    warnings: list[str] | None,
) -> None:
    if isinstance(spec, str):
        _attach_table_test(spec, None, table_tests)
        return
    if not isinstance(spec, dict) or len(spec) != 1:
        return
    ((name, arg),) = spec.items()

    if name == "not_null":
        cols = arg if isinstance(arg, list) else [arg]
        for col in cols:
            _ensure_col(columns_by_name, col).setdefault("tests", []).append("not_null")
        return

    if name == "unique":
        cols = arg if isinstance(arg, list) else [arg]
        if len(cols) == 1:
            _ensure_col(columns_by_name, cols[0]).setdefault("tests", []).append(
                "unique"
            )
        else:
            # dbt has no native composite-unique on a source table; emit the
            # dbt_utils macro test. See write_dbt_sources / --emit-packages:
            # Fusion rejects this unless dbt_utils is declared in packages.yml.
            table_tests.append(
                {
                    "dbt_utils.unique_combination_of_columns": {
                        "combination_of_columns": list(cols)
                    }
                }
            )
        return

    # min_rows / not_empty / has_text have no faithful dbt source-test
    # equivalent. Surface them so the user can re-express them dbt-side rather
    # than discovering the silent gap later.
    if warnings is not None:
        warnings.append(
            f"{model_name}: test '{name}' has no dbt source-test equivalent and "
            f"was not emitted; re-express it as a dbt-side data test if needed."
        )


def _ensure_col(
    columns_by_name: dict[str, dict[str, Any]], name: str
) -> dict[str, Any]:
    if name not in columns_by_name:
        columns_by_name[name] = {"name": name}
    return columns_by_name[name]


def _attach_table_test(name: str, _arg: Any, table_tests: list[Any]) -> None:
    # Reserved for future named string tests; nothing to emit for v1's bare strings.
    return
