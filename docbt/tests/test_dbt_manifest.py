"""Tests for the dbt-schema manifest.json / run_results.json emitter."""
from __future__ import annotations

import json
import re
from pathlib import Path

from docbt.dbt_manifest import (
    DBT_MANIFEST_SCHEMA,
    DBT_RUN_RESULTS_SCHEMA,
    build_dbt_manifest,
    build_dbt_run_results,
    source_unique_id,
    write_dbt_manifest,
    write_dbt_run_results,
)
from docbt.runner import ModelRunResult

_CODE_VERSION = re.compile(r"^[0-9a-f]{16}$")


def test_manifest_top_level_shape(example_project_dir: Path) -> None:
    m = build_dbt_manifest(example_project_dir)
    assert m["metadata"]["dbt_schema_version"] == DBT_MANIFEST_SCHEMA
    assert m["metadata"]["project_name"] == "invoice_pipeline"
    assert m["metadata"]["adapter_type"] == "duckdb"
    # docbt tables are sources, not nodes.
    assert m["nodes"] == {}
    assert set(m["sources"]) == set(m["parent_map"]) == set(m["child_map"])
    for required in ("macros", "docs", "exposures", "disabled", "semantic_models"):
        assert required in m


def test_source_nodes_are_well_formed(example_project_dir: Path) -> None:
    sources = build_dbt_manifest(example_project_dir)["sources"]
    expected = {
        source_unique_id("invoice_pipeline", "docbt_invoice_pipeline", name)
        for name in ("raw_invoices", "invoice_summary", "monthly_totals")
    }
    assert set(sources) == expected

    for uid, node in sources.items():
        assert node["unique_id"] == uid
        assert node["resource_type"] == "source"
        assert node["database"] and node["schema"] and node["identifier"]
        assert node["relation_name"].count('"') == 6  # "db"."schema"."tbl"
        assert _CODE_VERSION.match(node["meta"]["docbt"]["code_version"])


def test_depends_on_refs_are_unwrapped(example_project_dir: Path) -> None:
    sources = build_dbt_manifest(example_project_dir)["sources"]
    summary = sources[
        source_unique_id("invoice_pipeline", "docbt_invoice_pipeline", "invoice_summary")
    ]
    deps = summary["meta"]["docbt"]["depends_on"]
    assert "raw_invoices" in deps
    assert all("ref(" not in d for d in deps)


def test_columns_mapped_from_fields(example_project_dir: Path) -> None:
    sources = build_dbt_manifest(example_project_dir)["sources"]
    raw = sources[
        source_unique_id("invoice_pipeline", "docbt_invoice_pipeline", "raw_invoices")
    ]
    assert "invoice_id" in raw["columns"]
    assert raw["columns"]["invoice_id"]["name"] == "invoice_id"


def test_select_filters_sources(example_project_dir: Path) -> None:
    sources = build_dbt_manifest(example_project_dir, select="raw_invoices")["sources"]
    assert list(sources) == [
        source_unique_id("invoice_pipeline", "docbt_invoice_pipeline", "raw_invoices")
    ]


def test_custom_source_name(example_project_dir: Path) -> None:
    sources = build_dbt_manifest(example_project_dir, source_name="docbt_x")["sources"]
    assert all(uid.startswith("source.invoice_pipeline.docbt_x.") for uid in sources)


def test_no_nulls_in_required_string_fields(example_project_dir: Path) -> None:
    for node in build_dbt_manifest(example_project_dir)["sources"].values():
        for key in ("database", "schema", "name", "identifier", "source_name"):
            assert isinstance(node[key], str) and node[key]


def test_write_manifest_lands_under_target_dbt(
    tmp_path: Path, example_project_dir: Path
) -> None:
    import shutil

    dst = tmp_path / "p"
    shutil.copytree(
        example_project_dir,
        dst,
        ignore=shutil.ignore_patterns("data", "target", "__pycache__"),
    )
    out = write_dbt_manifest(dst)
    assert out.parent.name == "dbt"
    assert out.name == "manifest.json"
    parsed = json.loads(out.read_text())
    assert parsed["metadata"]["dbt_schema_version"] == DBT_MANIFEST_SCHEMA


def test_run_results_shape_and_unique_ids(example_project_dir: Path) -> None:
    results = [
        ModelRunResult(
            model_name="raw_invoices",
            materialization="full",
            kind="extraction",
            backend="json",
            rows_written=12,
            duration_seconds=0.5,
        ),
        ModelRunResult(
            model_name="invoice_summary",
            materialization="full",
            kind="transform",
            rows_written=3,
            duration_seconds=0.2,
            errors=["boom"],
        ),
    ]
    rr = build_dbt_run_results(example_project_dir, results)
    assert rr["metadata"]["dbt_schema_version"] == DBT_RUN_RESULTS_SCHEMA
    assert rr["elapsed_time"] == 0.7

    by_uid = {r["unique_id"]: r for r in rr["results"]}
    raw_uid = source_unique_id(
        "invoice_pipeline", "docbt_invoice_pipeline", "raw_invoices"
    )
    summ_uid = source_unique_id(
        "invoice_pipeline", "docbt_invoice_pipeline", "invoice_summary"
    )
    assert by_uid[raw_uid]["status"] == "success"
    assert by_uid[raw_uid]["adapter_response"]["rows_affected"] == 12
    assert by_uid[summ_uid]["status"] == "error"
    assert by_uid[summ_uid]["message"] == "boom"
    assert by_uid[summ_uid]["failures"] == 1


def test_run_results_unique_ids_match_manifest(example_project_dir: Path) -> None:
    manifest = build_dbt_manifest(example_project_dir)
    results = [
        ModelRunResult(
            model_name="raw_invoices", materialization="full", kind="extraction"
        )
    ]
    rr = build_dbt_run_results(example_project_dir, results)
    assert rr["results"][0]["unique_id"] in manifest["sources"]


def test_write_run_results_lands_under_target_dbt(
    tmp_path: Path, example_project_dir: Path
) -> None:
    import shutil

    dst = tmp_path / "p"
    shutil.copytree(
        example_project_dir,
        dst,
        ignore=shutil.ignore_patterns("data", "target", "__pycache__"),
    )
    out = write_dbt_run_results(
        dst,
        [ModelRunResult(model_name="raw_invoices", materialization="full", kind="x")],
    )
    assert out.parent.name == "dbt"
    assert out.name == "run_results.json"
    assert json.loads(out.read_text())["results"]
