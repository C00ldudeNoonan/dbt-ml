from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from docbt.manifest import (
    MANIFEST_FILENAME,
    RUN_RESULTS_FILENAME,
    build_manifest,
    write_manifest,
    write_run_results,
)
from docbt.runner import run_project
from docbt.synth import generate_invoices


@pytest.fixture
def fresh_project(tmp_path: Path, example_project_dir: Path) -> Path:
    dst = tmp_path / "project"
    shutil.copytree(
        example_project_dir,
        dst,
        ignore=shutil.ignore_patterns("data", "target", "__pycache__"),
    )
    return dst


def test_manifest_shape(fresh_project: Path) -> None:
    m = build_manifest(fresh_project)
    assert m["manifest_version"] == 1
    assert m["project"]["name"] == "invoice_pipeline"
    assert {s["name"] for s in m["sources"]} == {"vendor_invoices"}
    assert {x["name"] for x in m["models"]} == {
        "raw_invoices",
        "invoice_summary",
        "monthly_totals",
    }
    assert m["dag"]["execution_order"][0] == "raw_invoices"
    assert ["vendor_invoices", "raw_invoices"] in m["dag"]["edges"]


def test_manifest_has_code_versions(fresh_project: Path) -> None:
    m = build_manifest(fresh_project)
    versions = {x["name"]: x["code_version"] for x in m["models"]}
    assert all(isinstance(v, str) and len(v) == 16 for v in versions.values())


def test_write_manifest_creates_file(fresh_project: Path) -> None:
    path = write_manifest(fresh_project)
    assert path.exists()
    assert path.name == MANIFEST_FILENAME
    payload = json.loads(path.read_text())
    assert payload["project"]["name"] == "invoice_pipeline"


def test_run_writes_run_results(fresh_project: Path) -> None:
    generate_invoices(3, fresh_project / "data" / "invoices", seed=1)
    results = run_project(fresh_project)
    path = write_run_results(fresh_project, results)
    assert path.exists()
    assert path.name == RUN_RESULTS_FILENAME
    payload = json.loads(path.read_text())
    assert len(payload["results"]) == len(results)
    assert {r["model_name"] for r in payload["results"]} == {r.model_name for r in results}
