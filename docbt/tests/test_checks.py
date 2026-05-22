from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest

from docbt.checks import run_project_tests
from docbt.checks.schema import TestResult, UnknownTestError, evaluate_test_spec
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


@pytest.fixture
def populated_db(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    db = tmp_path / "t.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE items (id INTEGER, vendor VARCHAR, total DOUBLE)")
    con.execute(
        "INSERT INTO items VALUES "
        "(1, 'A', 10.0), (2, 'B', 20.0), (3, 'C', NULL), (3, 'D', 30.0)"
    )
    yield con
    con.close()


def _by(results: list[TestResult], **filters: str | None) -> TestResult:
    return next(
        r
        for r in results
        if all(getattr(r, k) == v for k, v in filters.items())
    )


def test_not_null_pass_and_fail(populated_db: duckdb.DuckDBPyConnection) -> None:
    results = evaluate_test_spec(
        {"not_null": ["id", "total"]},
        model_name="items",
        table_ref="items",
        con=populated_db,
    )
    assert _by(results, column="id").passed
    assert not _by(results, column="total").passed
    assert "1 rows" in _by(results, column="total").message


def test_unique_detects_dups(populated_db: duckdb.DuckDBPyConnection) -> None:
    results = evaluate_test_spec(
        {"unique": "id"},
        model_name="items",
        table_ref="items",
        con=populated_db,
    )
    assert len(results) == 1
    assert not results[0].passed


def test_unique_composite(populated_db: duckdb.DuckDBPyConnection) -> None:
    results = evaluate_test_spec(
        {"unique": ["id", "vendor"]},
        model_name="items",
        table_ref="items",
        con=populated_db,
    )
    assert results[0].passed


def test_min_rows(populated_db: duckdb.DuckDBPyConnection) -> None:
    ok = evaluate_test_spec(
        {"min_rows": 1}, model_name="items", table_ref="items", con=populated_db
    )
    assert ok[0].passed
    bad = evaluate_test_spec(
        {"min_rows": 100}, model_name="items", table_ref="items", con=populated_db
    )
    assert not bad[0].passed


def test_not_empty_bare_string(populated_db: duckdb.DuckDBPyConnection) -> None:
    results = evaluate_test_spec(
        "not_empty", model_name="items", table_ref="items", con=populated_db
    )
    assert results[0].test_name == "not_empty"
    assert results[0].passed


def test_unknown_test_raises(populated_db: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(UnknownTestError):
        evaluate_test_spec(
            {"nonsense": "foo"},
            model_name="items",
            table_ref="items",
            con=populated_db,
        )


def test_severity_warn_downgrades_failure(
    populated_db: duckdb.DuckDBPyConnection,
) -> None:
    # `total` has a NULL → would be a "fail" by default
    results = evaluate_test_spec(
        {"not_null": "total", "severity": "warn"},
        model_name="items",
        table_ref="items",
        con=populated_db,
    )
    assert results[0].status == "warn"
    assert results[0].severity == "warn"
    assert not results[0].passed
    assert not results[0].is_hard_failure


def test_severity_error_is_default(populated_db: duckdb.DuckDBPyConnection) -> None:
    results = evaluate_test_spec(
        {"not_null": "total"},
        model_name="items",
        table_ref="items",
        con=populated_db,
    )
    assert results[0].status == "fail"
    assert results[0].severity == "error"
    assert results[0].is_hard_failure


def test_severity_warn_keeps_passing_results(
    populated_db: duckdb.DuckDBPyConnection,
) -> None:
    # Passing test should still report "pass" even with severity: warn
    results = evaluate_test_spec(
        {"not_null": "id", "severity": "warn"},
        model_name="items",
        table_ref="items",
        con=populated_db,
    )
    assert results[0].status == "pass"


def test_unknown_severity_raises(populated_db: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(UnknownTestError, match="Unknown severity"):
        evaluate_test_spec(
            {"not_null": "id", "severity": "loud"},
            model_name="items",
            table_ref="items",
            con=populated_db,
        )


def test_end_to_end_passes(fresh_project: Path) -> None:
    generate_invoices(15, fresh_project / "data" / "invoices", seed=99)
    run_project(fresh_project)
    results = run_project_tests(fresh_project)
    assert len(results) > 0
    failed = [r for r in results if not r.passed]
    assert failed == []


def test_end_to_end_fail_when_unique_violated(fresh_project: Path) -> None:
    """Two files with identical invoice_id violate raw_invoices' unique test."""
    invoices_dir = fresh_project / "data" / "invoices"
    invoices_dir.mkdir(parents=True)
    body = (
        '{"invoice_id":"DUP","vendor":"V","issue_date":"2025-01-01",'
        '"currency":"USD","line_items":[],"total":1.0}'
    )
    (invoices_dir / "a.json").write_text(body)
    (invoices_dir / "b.json").write_text(body)
    run_project(fresh_project)
    results = run_project_tests(fresh_project)
    fails = [r for r in results if not r.passed]
    assert any(r.test_name == "unique" for r in fails)
