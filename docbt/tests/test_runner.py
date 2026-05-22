from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
import pytest

from docbt.runner import clean_project, run_project
from docbt.synth import generate_invoices


@pytest.fixture
def fresh_project(tmp_path: Path, example_project_dir: Path) -> Path:
    """Copy the example project into a tmp dir so each test gets a clean slate."""
    dst = tmp_path / "project"
    shutil.copytree(
        example_project_dir,
        dst,
        ignore=shutil.ignore_patterns("data", "target", "__pycache__"),
    )
    return dst


def _query(db_path: Path, sql: str) -> list[tuple]:
    con = duckdb.connect(str(db_path))
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_end_to_end_run(fresh_project: Path) -> None:
    invoices_dir = fresh_project / "data" / "invoices"
    generate_invoices(10, invoices_dir, seed=1)

    results = run_project(fresh_project)
    by_name = {r.model_name: r for r in results}
    assert by_name["raw_invoices"].documents_processed == 10
    assert by_name["raw_invoices"].documents_skipped == 0
    assert by_name["raw_invoices"].rows_written == 10
    assert by_name["invoice_summary"].kind == "transform"

    db = fresh_project / "target" / "docbt.duckdb"
    assert db.exists()
    rows = _query(db, 'SELECT COUNT(*) FROM "docbt".docbt.raw_invoices')
    assert rows[0][0] == 10


def test_second_run_is_incremental(fresh_project: Path) -> None:
    invoices_dir = fresh_project / "data" / "invoices"
    generate_invoices(5, invoices_dir, seed=1)
    run_project(fresh_project)

    results = run_project(fresh_project)
    raw = next(r for r in results if r.model_name == "raw_invoices")
    assert raw.documents_processed == 0
    assert raw.documents_skipped == 5


def test_changed_doc_is_reprocessed(fresh_project: Path) -> None:
    invoices_dir = fresh_project / "data" / "invoices"
    generate_invoices(5, invoices_dir, seed=1)
    run_project(fresh_project)

    # Mutate one doc's content
    target = invoices_dir / "invoice_00002.json"
    data = json.loads(target.read_text())
    data["vendor"] = "MUTATED_VENDOR"
    target.write_text(json.dumps(data))

    results = run_project(fresh_project)
    raw = next(r for r in results if r.model_name == "raw_invoices")
    assert raw.documents_processed == 1
    assert raw.documents_skipped == 4

    db = fresh_project / "target" / "docbt.duckdb"
    rows = _query(
        db,
        'SELECT vendor FROM "docbt".docbt.raw_invoices '
        "WHERE source_path = 'invoice_00002.json'",
    )
    assert rows[0][0] == "MUTATED_VENDOR"


def test_full_refresh_reprocesses_all(fresh_project: Path) -> None:
    invoices_dir = fresh_project / "data" / "invoices"
    generate_invoices(5, invoices_dir, seed=1)
    run_project(fresh_project)

    results = run_project(fresh_project, full_refresh=True)
    raw = next(r for r in results if r.model_name == "raw_invoices")
    assert raw.documents_processed == 5
    assert raw.documents_skipped == 0


def test_transform_aggregates_dependency(fresh_project: Path) -> None:
    invoices_dir = fresh_project / "data" / "invoices"
    generate_invoices(20, invoices_dir, seed=1)
    run_project(fresh_project)

    db = fresh_project / "target" / "docbt.duckdb"
    rows = _query(
        db,
        'SELECT SUM(invoice_count), SUM(total_spend) FROM "docbt".docbt.invoice_summary',
    )
    raw_rows = _query(
        db, 'SELECT COUNT(*), SUM(total) FROM "docbt".docbt.raw_invoices'
    )
    assert rows[0][0] == raw_rows[0][0]
    assert rows[0][1] == pytest.approx(raw_rows[0][1])


def test_run_with_select(fresh_project: Path) -> None:
    generate_invoices(5, fresh_project / "data" / "invoices", seed=2)
    results = run_project(fresh_project, select="raw_invoices")
    assert [r.model_name for r in results] == ["raw_invoices"]


def test_run_with_select_descendants(fresh_project: Path) -> None:
    generate_invoices(5, fresh_project / "data" / "invoices", seed=2)
    results = run_project(fresh_project, select="raw_invoices+")
    assert {r.model_name for r in results} == {
        "raw_invoices",
        "invoice_summary",
        "monthly_totals",
    }


def test_run_with_exclude(fresh_project: Path) -> None:
    generate_invoices(5, fresh_project / "data" / "invoices", seed=2)
    results = run_project(fresh_project, exclude="invoice_summary")
    assert "invoice_summary" not in {r.model_name for r in results}
    assert {r.model_name for r in results} == {"raw_invoices", "monthly_totals"}


def test_run_with_threads_produces_same_results(fresh_project: Path) -> None:
    """Parallel extraction must yield the same rows as serial."""
    generate_invoices(20, fresh_project / "data" / "invoices", seed=4)

    results_serial = run_project(fresh_project)
    raw_serial = next(r for r in results_serial if r.model_name == "raw_invoices")
    assert raw_serial.rows_written == 20

    # Clean and re-run with 4 threads
    from docbt.runner import clean_project

    clean_project(fresh_project)
    results_parallel = run_project(fresh_project, threads=4)
    raw_parallel = next(r for r in results_parallel if r.model_name == "raw_invoices")
    assert raw_parallel.rows_written == 20

    db = fresh_project / "target" / "docbt.duckdb"
    rows = _query(db, 'SELECT COUNT(*) FROM "docbt".docbt.raw_invoices')
    assert rows[0][0] == 20


def test_clean_removes_duckdb(fresh_project: Path) -> None:
    invoices_dir = fresh_project / "data" / "invoices"
    generate_invoices(2, invoices_dir, seed=1)
    run_project(fresh_project)
    db = fresh_project / "target" / "docbt.duckdb"
    assert db.exists()

    clean_project(fresh_project)
    assert not db.exists()
