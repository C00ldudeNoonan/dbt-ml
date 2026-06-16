from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
import pytest

from dbt_ml.manifest import write_run_results
from dbt_ml.runner import clean_project, run_project
from dbt_ml.synth import generate_invoices, generate_support_tickets


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


def _write_ticket(path: Path, ticket_id: str, summary: str) -> None:
    path.write_text(
        json.dumps(
            {
                "ticket_id": ticket_id,
                "summary": summary,
                "priority": "medium",
            }
        )
    )


def test_end_to_end_run(fresh_project: Path) -> None:
    invoices_dir = fresh_project / "data" / "invoices"
    generate_invoices(10, invoices_dir, seed=1)

    results = run_project(fresh_project)
    by_name = {r.model_name: r for r in results}
    assert by_name["raw_invoices"].documents_processed == 10
    assert by_name["raw_invoices"].documents_skipped == 0
    assert by_name["raw_invoices"].rows_written == 10
    assert by_name["invoice_summary"].kind == "transform"

    db = fresh_project / "target" / "dbt_ml.duckdb"
    assert db.exists()
    rows = _query(db, 'SELECT COUNT(*) FROM "dbt_ml".dbt_ml.raw_invoices')
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

    db = fresh_project / "target" / "dbt_ml.duckdb"
    rows = _query(
        db,
        'SELECT vendor FROM "dbt_ml".dbt_ml.raw_invoices '
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

    db = fresh_project / "target" / "dbt_ml.duckdb"
    rows = _query(
        db,
        'SELECT SUM(invoice_count), SUM(total_spend) FROM "dbt_ml".dbt_ml.invoice_summary',
    )
    raw_rows = _query(
        db, 'SELECT COUNT(*), SUM(total) FROM "dbt_ml".dbt_ml.raw_invoices'
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
    from dbt_ml.runner import clean_project

    clean_project(fresh_project)
    results_parallel = run_project(fresh_project, threads=4)
    raw_parallel = next(r for r in results_parallel if r.model_name == "raw_invoices")
    assert raw_parallel.rows_written == 20

    db = fresh_project / "target" / "dbt_ml.duckdb"
    rows = _query(db, 'SELECT COUNT(*) FROM "dbt_ml".dbt_ml.raw_invoices')
    assert rows[0][0] == 20


def test_clean_removes_duckdb(fresh_project: Path) -> None:
    invoices_dir = fresh_project / "data" / "invoices"
    generate_invoices(2, invoices_dir, seed=1)
    run_project(fresh_project)
    db = fresh_project / "target" / "dbt_ml.duckdb"
    assert db.exists()

    clean_project(fresh_project)
    assert not db.exists()


def test_classic_ml_tfidf_end_to_end(tmp_path: Path) -> None:
    src = Path(__file__).resolve().parents[1] / "examples" / "classic_text_ml"
    project = tmp_path / "classic_text_ml"
    shutil.copytree(src, project, ignore=shutil.ignore_patterns("data", "target"))
    generate_support_tickets(8, project / "data" / "tickets", seed=7)

    results = run_project(project)
    by_name = {r.model_name: r for r in results}
    ml_result = by_name["ticket_tfidf"]
    assert ml_result.kind == "ml"
    assert ml_result.rows_written > 0
    assert ml_result.artifact_version is not None
    assert ml_result.training_input is not None
    assert ml_result.training_input["refs"] == ["raw_tickets"]
    assert ml_result.training_input["row_count"] == 8
    assert ml_result.metrics["vocabulary_size"] > 0
    assert ml_result.metrics["feature_rows"] == ml_result.rows_written

    artifact = project / "target" / "artifacts" / "ticket_tfidf"
    assert (artifact / "metadata.json").exists()
    assert (artifact / "vocabulary.json").exists()

    db = project / "target" / "dbt_ml.duckdb"
    rows = _query(
        db,
        'SELECT COUNT(*), COUNT(DISTINCT row_id) FROM '
        '"dbt_ml".classic_text_ml.ticket_tfidf',
    )
    assert rows[0][0] == ml_result.rows_written
    assert rows[0][1] == 8

    run_results_path = write_run_results(project, results)
    payload = json.loads(run_results_path.read_text())
    emitted = next(r for r in payload["results"] if r["model_name"] == "ticket_tfidf")
    assert emitted["artifact_version"] == ml_result.artifact_version
    assert emitted["training_input"]["row_count"] == 8
    assert emitted["metrics"]["vocabulary_size"] == ml_result.metrics["vocabulary_size"]


def test_classic_ml_tfidf_fit_then_predict(tmp_path: Path) -> None:
    src = Path(__file__).resolve().parents[1] / "examples" / "classic_text_ml"
    project = tmp_path / "classic_text_ml"
    shutil.copytree(src, project, ignore=shutil.ignore_patterns("data", "target"))
    (project / "models" / "ticket_tfidf.yml").write_text(
        "\n".join(
            [
                "version: 2",
                "models:",
                "  - name: ticket_tfidf_fit",
                "    depends_on: [ref('raw_tickets')]",
                "    ml:",
                "      task: features",
                "      mode: fit",
                "      provider: builtin.tfidf",
                "      text_field: summary",
                "      artifact:",
                "        path: target/artifacts/ticket_tfidf",
                "      options:",
                "        min_df: 1",
                "  - name: ticket_tfidf_predict",
                "    depends_on: [ref('raw_tickets'), ref('ticket_tfidf_fit')]",
                "    ml:",
                "      task: features",
                "      mode: predict",
                "      provider: builtin.tfidf",
                "      text_field: summary",
                "      artifact:",
                "        path: target/artifacts/ticket_tfidf",
            ]
        )
    )
    generate_support_tickets(5, project / "data" / "tickets", seed=11)

    results = run_project(project)
    by_name = {r.model_name: r for r in results}
    fit = by_name["ticket_tfidf_fit"]
    predict = by_name["ticket_tfidf_predict"]
    assert fit.rows_written == 1
    assert predict.rows_written > 0
    assert fit.artifact_version == predict.artifact_version
    assert fit.training_input == predict.training_input


def test_classic_ml_count_vectorizer_options(tmp_path: Path) -> None:
    src = Path(__file__).resolve().parents[1] / "examples" / "classic_text_ml"
    project = tmp_path / "classic_text_ml"
    shutil.copytree(src, project, ignore=shutil.ignore_patterns("data", "target"))
    tickets = project / "data" / "tickets"
    tickets.mkdir(parents=True)
    _write_ticket(tickets / "ticket_1.json", "T-1", "alpha alpha beta the")
    _write_ticket(tickets / "ticket_2.json", "T-2", "beta gamma the")
    (project / "models" / "ticket_tfidf.yml").write_text(
        "\n".join(
            [
                "version: 2",
                "models:",
                "  - name: ticket_count",
                "    depends_on: [ref('raw_tickets')]",
                "    ml:",
                "      task: features",
                "      mode: fit_transform",
                "      provider: builtin.count",
                "      text_field: summary",
                "      options:",
                "        binary: true",
                "        stop_words: [the]",
                "    materialization: full",
            ]
        )
    )

    results = run_project(project)
    count = next(r for r in results if r.model_name == "ticket_count")
    assert count.rows_written == 4
    assert count.metrics["vocabulary_size"] == 3

    db = project / "target" / "dbt_ml.duckdb"
    rows = _query(
        db,
        'SELECT term, SUM(count), SUM(value) FROM "dbt_ml".classic_text_ml.ticket_count '
        "GROUP BY term ORDER BY term",
    )
    assert rows == [
        ("alpha", 1, 1.0),
        ("beta", 2, 2.0),
        ("gamma", 1, 1.0),
    ]
    vocab_path = project / "target" / "artifacts" / "ticket_count" / "vocabulary.json"
    vocab = json.loads(vocab_path.read_text())
    assert vocab["terms"] == ["alpha", "beta", "gamma"]


def test_classic_ml_hashing_vectorizer(tmp_path: Path) -> None:
    src = Path(__file__).resolve().parents[1] / "examples" / "classic_text_ml"
    project = tmp_path / "classic_text_ml"
    shutil.copytree(src, project, ignore=shutil.ignore_patterns("data", "target"))
    tickets = project / "data" / "tickets"
    tickets.mkdir(parents=True)
    _write_ticket(tickets / "ticket_1.json", "T-1", "alpha beta")
    _write_ticket(tickets / "ticket_2.json", "T-2", "alpha")
    (project / "models" / "ticket_tfidf.yml").write_text(
        "\n".join(
            [
                "version: 2",
                "models:",
                "  - name: ticket_hashing",
                "    depends_on: [ref('raw_tickets')]",
                "    ml:",
                "      task: features",
                "      mode: fit_transform",
                "      provider: builtin.hashing",
                "      text_field: summary",
                "      options:",
                "        n_features: 8",
                "        alternate_sign: false",
                "    materialization: full",
            ]
        )
    )

    results = run_project(project)
    hashing = next(r for r in results if r.model_name == "ticket_hashing")
    assert hashing.rows_written > 0
    assert hashing.metrics["hash_buckets"] == 8
    assert hashing.metrics["vocabulary_size"] == 0

    db = project / "target" / "dbt_ml.duckdb"
    rows = _query(
        db,
        'SELECT MIN(hash_bucket), MAX(hash_bucket), COUNT(DISTINCT term) '
        'FROM "dbt_ml".classic_text_ml.ticket_hashing',
    )
    assert rows[0][0] >= 0
    assert rows[0][1] < 8
    assert rows[0][2] > 0
    metadata = json.loads(
        (project / "target" / "artifacts" / "ticket_hashing" / "metadata.json").read_text()
    )
    assert metadata["files"] == ["metadata.json"]


def test_classic_ml_tfidf_character_ngrams(tmp_path: Path) -> None:
    src = Path(__file__).resolve().parents[1] / "examples" / "classic_text_ml"
    project = tmp_path / "classic_text_ml"
    shutil.copytree(src, project, ignore=shutil.ignore_patterns("data", "target"))
    tickets = project / "data" / "tickets"
    tickets.mkdir(parents=True)
    _write_ticket(tickets / "ticket_1.json", "T-1", "abc abc")
    _write_ticket(tickets / "ticket_2.json", "T-2", "abd")
    (project / "models" / "ticket_tfidf.yml").write_text(
        "\n".join(
            [
                "version: 2",
                "models:",
                "  - name: ticket_char_tfidf",
                "    depends_on: [ref('raw_tickets')]",
                "    ml:",
                "      task: features",
                "      mode: fit_transform",
                "      provider: builtin.tfidf",
                "      text_field: summary",
                "      options:",
                "        analyzer: char",
                "        ngram_range: [3, 3]",
                "        min_df: 1",
                "    materialization: full",
            ]
        )
    )

    run_project(project)

    db = project / "target" / "dbt_ml.duckdb"
    rows = _query(
        db,
        'SELECT term, COUNT(*) FROM "dbt_ml".classic_text_ml.ticket_char_tfidf '
        "WHERE term = 'abc' GROUP BY term",
    )
    assert rows == [("abc", 1)]
