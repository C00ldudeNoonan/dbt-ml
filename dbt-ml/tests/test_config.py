from __future__ import annotations

from pathlib import Path

import pytest

from dbt_ml.config import ConfigError, load_project


def test_load_example_project(example_project_dir: Path) -> None:
    project, sources, models = load_project(example_project_dir)
    assert project.name == "invoice_pipeline"
    assert project.duckdb.schema_name == "dbt_ml"
    assert project.extraction.default_backend == "json"
    assert {s.name for s in sources} == {"vendor_invoices"}
    assert {m.name for m in models} == {"raw_invoices", "invoice_summary", "monthly_totals"}


def test_missing_project_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"dbt_ml_project\.yml"):
        load_project(tmp_path)


def test_invalid_yaml_reports_path(tmp_path: Path) -> None:
    (tmp_path / "dbt_ml_project.yml").write_text("name: x\n")
    (tmp_path / "sources").mkdir()
    (tmp_path / "sources" / "bad.yml").write_text(
        "version: 2\nsources:\n  - description: 'missing required name'\n"
    )
    with pytest.raises(ConfigError, match=r"bad\.yml"):
        load_project(tmp_path)


def test_raw_invoices_is_incremental(example_project_dir: Path) -> None:
    _, _, models = load_project(example_project_dir)
    raw = next(m for m in models if m.name == "raw_invoices")
    assert raw.materialization == "incremental"
    assert raw.extraction is not None
    assert raw.extraction.backend == "json"
    assert raw.extraction.options["fields"] == [
        "invoice_id",
        "vendor",
        "issue_date",
        "line_items",
        "total",
        "currency",
    ]


def test_invoice_summary_depends_on_raw(example_project_dir: Path) -> None:
    _, _, models = load_project(example_project_dir)
    summary = next(m for m in models if m.name == "invoice_summary")
    assert summary.materialization == "full"
    assert summary.depends_on == ["ref('raw_invoices')"]
    assert summary.transform is not None
    assert summary.transform.module == "transforms.summarize"
