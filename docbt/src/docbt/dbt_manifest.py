"""Emit dbt-schema `manifest.json` / `run_results.json` for a docbt project.

docbt already writes its *own* artifacts (`manifest.py`, schema v1) which its
docs site consumes. This module emits a second, **dbt-conformant** pair so dbt
tooling — catalog/lineage viewers, the dbt-artifacts ecosystem, state-aware
selection — can read docbt's DAG directly.

Mapping: docbt does the unstructured "E"; dbt does the SQL "T". From dbt's point
of view every docbt-materialized table is a **source** (sources-only), grouped
under one source named `docbt_<project>`. docbt's internal lineage and
`code_version` are preserved under each node's `meta.docbt` block so
manifest-diffing tools can still see what changed.

Artifacts land under `<target>/dbt/` so they never clobber docbt's native ones.
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import load_project
from .config.model import ModelConfig
from .dag import ProjectDAG, parse_ref
from .dbt_export import _derive_catalog
from .profile import resolve_profile
from .runner import ModelRunResult
from .versioning import compute_code_version

# Schema targets. Pinned so we validate against a fixed contract; bump
# deliberately when we move to a newer dbt artifact schema.
DBT_MANIFEST_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"
DBT_RUN_RESULTS_SCHEMA = "https://schemas.getdbt.com/dbt/run-results/v6.json"
DBT_VERSION = "1.9.0"

DBT_ARTIFACT_DIR = "dbt"
DBT_MANIFEST_FILENAME = "manifest.json"
DBT_RUN_RESULTS_FILENAME = "run_results.json"

_SOURCE_PATH = "models/sources/docbt_sources.yml"


def build_dbt_manifest(
    project_dir: Path,
    *,
    source_name: str | None = None,
    select: str | None = None,
    exclude: str | None = None,
    target: str | None = None,
    profiles_dir: Path | None = None,
) -> dict[str, Any]:
    project, sources_cfg, models = load_project(project_dir)
    resolved = resolve_profile(
        project, project_dir, target=target, profiles_dir=profiles_dir
    )
    dag = ProjectDAG(sources_cfg, models)
    selected = set(dag.select_models(select=select, exclude=exclude))
    selected_models = [m for m in models if m.name in selected]

    src_name = source_name or f"docbt_{project.name}"
    catalog = _derive_catalog(resolved.warehouse)
    schema = resolved.warehouse.schema_name

    nodes_by_uid: dict[str, dict[str, Any]] = {}
    for model in selected_models:
        node = _source_node(
            model,
            project_name=project.name,
            source_name=src_name,
            catalog=catalog,
            schema=schema,
            project_dir=project_dir,
        )
        nodes_by_uid[node["unique_id"]] = node

    return {
        "metadata": {
            "dbt_schema_version": DBT_MANIFEST_SCHEMA,
            "dbt_version": DBT_VERSION,
            "generated_at": _now(),
            "invocation_id": str(uuid.uuid4()),
            "env": {},
            "project_name": project.name,
            "project_id": _project_id(project.name),
            "user_id": None,
            "send_anonymous_usage_stats": False,
            "adapter_type": resolved.warehouse.type,
        },
        "nodes": {},
        "sources": nodes_by_uid,
        "macros": {},
        "docs": {},
        "exposures": {},
        "metrics": {},
        "groups": {},
        "selectors": {},
        "disabled": {},
        # docbt tables are sources: roots with no dbt-side parents/children
        # until a downstream dbt model refs them.
        "parent_map": {uid: [] for uid in nodes_by_uid},
        "child_map": {uid: [] for uid in nodes_by_uid},
        "group_map": {},
        "saved_queries": {},
        "semantic_models": {},
        "unit_tests": {},
    }


def write_dbt_manifest(
    project_dir: Path,
    *,
    source_name: str | None = None,
    select: str | None = None,
    exclude: str | None = None,
    output: Path | None = None,
    target: str | None = None,
    profiles_dir: Path | None = None,
) -> Path:
    project, _, _ = load_project(project_dir)
    payload = build_dbt_manifest(
        project_dir,
        source_name=source_name,
        select=select,
        exclude=exclude,
        target=target,
        profiles_dir=profiles_dir,
    )
    out = output or _artifact_path(project_dir, project.target_path, DBT_MANIFEST_FILENAME)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2))
    return out


def build_dbt_run_results(
    project_dir: Path,
    results: list[ModelRunResult],
    *,
    source_name: str | None = None,
    target: str | None = None,
    profiles_dir: Path | None = None,
    elapsed_time: float | None = None,
) -> dict[str, Any]:
    project, _, _ = load_project(project_dir)
    src_name = source_name or f"docbt_{project.name}"

    entries = [_result_entry(r, project.name, src_name) for r in results]
    total = (
        elapsed_time
        if elapsed_time is not None
        else sum(r.duration_seconds for r in results)
    )

    return {
        "metadata": {
            "dbt_schema_version": DBT_RUN_RESULTS_SCHEMA,
            "dbt_version": DBT_VERSION,
            "generated_at": _now(),
            "invocation_id": str(uuid.uuid4()),
            "env": {},
        },
        "results": entries,
        "elapsed_time": total,
        "args": {},
    }


def write_dbt_run_results(
    project_dir: Path,
    results: list[ModelRunResult],
    *,
    source_name: str | None = None,
    output: Path | None = None,
    target: str | None = None,
    profiles_dir: Path | None = None,
    elapsed_time: float | None = None,
) -> Path:
    project, _, _ = load_project(project_dir)
    payload = build_dbt_run_results(
        project_dir,
        results,
        source_name=source_name,
        target=target,
        profiles_dir=profiles_dir,
        elapsed_time=elapsed_time,
    )
    out = output or _artifact_path(
        project_dir, project.target_path, DBT_RUN_RESULTS_FILENAME
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2))
    return out


def source_unique_id(project_name: str, source_name: str, table: str) -> str:
    return f"source.{project_name}.{source_name}.{table}"


def _source_node(
    model: ModelConfig,
    *,
    project_name: str,
    source_name: str,
    catalog: str,
    schema: str,
    project_dir: Path,
) -> dict[str, Any]:
    uid = source_unique_id(project_name, source_name, model.name)
    kind = _model_kind(model)
    code_version = compute_code_version(
        extraction=model.extraction,
        transform=model.transform,
        project_dir=project_dir,
    )
    depends_on = [parse_ref(d) for d in (model.depends_on or [])]
    if model.source:
        depends_on.insert(0, parse_ref(model.source))

    return {
        "database": catalog,
        "schema": schema,
        "name": model.name,
        "resource_type": "source",
        "package_name": project_name,
        "path": _SOURCE_PATH,
        "original_file_path": _SOURCE_PATH,
        "unique_id": uid,
        "fqn": [project_name, source_name, model.name],
        "source_name": source_name,
        "source_description": (
            f"Tables materialized by docbt project '{project_name}'."
        ),
        "loader": "docbt",
        "identifier": model.name,
        "quoting": {
            "database": None,
            "schema": None,
            "identifier": None,
            "column": None,
        },
        "loaded_at_field": None,
        "freshness": None,
        "external": None,
        "description": model.description or "",
        "columns": {f.name: _column(f.name, f.description) for f in model.fields},
        "meta": {
            "docbt": {
                "kind": kind,
                "materialization": model.materialization,
                "code_version": code_version,
                "depends_on": depends_on,
            }
        },
        "source_meta": {},
        "tags": list(model.tags),
        "config": {"enabled": True},
        "patch_path": None,
        "unrendered_config": {},
        "relation_name": f'"{catalog}"."{schema}"."{model.name}"',
        "created_at": time.time(),
    }


def _column(name: str, description: str | None) -> dict[str, Any]:
    return {
        "name": name,
        "description": description or "",
        "meta": {},
        "data_type": None,
        "constraints": [],
        "quote": None,
        "tags": [],
    }


def _result_entry(
    result: ModelRunResult, project_name: str, source_name: str
) -> dict[str, Any]:
    completed = datetime.now(UTC)
    started = datetime.fromtimestamp(
        completed.timestamp() - result.duration_seconds, tz=UTC
    )
    errored = bool(result.errors)
    return {
        "status": "error" if errored else "success",
        "timing": [
            {
                "name": "execute",
                "started_at": started.isoformat(),
                "completed_at": completed.isoformat(),
            }
        ],
        "thread_id": "main",
        "execution_time": result.duration_seconds,
        "adapter_response": {
            "_message": f"OK {result.rows_written}",
            "rows_affected": result.rows_written,
        },
        "message": "; ".join(result.errors) if errored else None,
        "failures": len(result.errors) if errored else None,
        "unique_id": source_unique_id(project_name, source_name, result.model_name),
    }


def _model_kind(model: ModelConfig) -> str:
    if model.extraction is not None:
        return "extraction"
    if model.transform is not None:
        return "transform"
    return "unknown"


def _artifact_path(project_dir: Path, target_path: Path, filename: str) -> Path:
    return (project_dir / target_path / DBT_ARTIFACT_DIR / filename).resolve()


def _project_id(name: str) -> str:
    return hashlib.blake2b(name.encode(), digest_size=16).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat()
