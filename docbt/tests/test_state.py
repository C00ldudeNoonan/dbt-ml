from __future__ import annotations

from pathlib import Path

from docbt.state import State


def test_state_creates_schema_and_table(tmp_path: Path) -> None:
    db = tmp_path / "test.duckdb"
    with State(db, schema="testns") as state:
        result = state.connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'testns' AND table_name = 'docbt_state'"
        ).fetchone()
        assert result is not None and result[0] == 1


def test_upsert_and_query(tmp_path: Path) -> None:
    db = tmp_path / "test.duckdb"
    with State(db, schema="testns") as state:
        state.upsert_processed(
            "m1",
            [
                ("doc-1", "hash-a", "v1"),
                ("doc-2", "hash-b", "v1"),
            ],
        )
        processed = state.get_processed("m1")
        assert processed == {"doc-1": ("hash-a", "v1"), "doc-2": ("hash-b", "v1")}

        # Updating doc-1's hash should overwrite, not duplicate
        state.upsert_processed("m1", [("doc-1", "hash-a2", "v2")])
        processed = state.get_processed("m1")
        assert processed["doc-1"] == ("hash-a2", "v2")
        assert len(processed) == 2


def test_state_persists_across_sessions(tmp_path: Path) -> None:
    db = tmp_path / "test.duckdb"
    with State(db, schema="testns") as state:
        state.upsert_processed("m1", [("doc-1", "h", "v")])

    with State(db, schema="testns") as state:
        assert state.get_processed("m1") == {"doc-1": ("h", "v")}


def test_clear_model(tmp_path: Path) -> None:
    db = tmp_path / "test.duckdb"
    with State(db, schema="testns") as state:
        state.upsert_processed("m1", [("doc-1", "h", "v")])
        state.upsert_processed("m2", [("doc-1", "h", "v")])
        state.clear_model("m1")
        assert state.get_processed("m1") == {}
        assert state.get_processed("m2") == {"doc-1": ("h", "v")}


def test_qualified_schema_avoids_catalog_collision(tmp_path: Path) -> None:
    # Filename matches schema name on purpose — used to fail before catalog qualification.
    db = tmp_path / "docbt.duckdb"
    with State(db, schema="docbt") as state:
        state.upsert_processed("m1", [("doc-1", "h", "v")])
        assert state.get_processed("m1") == {"doc-1": ("h", "v")}
