from __future__ import annotations

from pathlib import Path
from types import TracebackType
from typing import Self

import duckdb


class State:
    """DuckDB-backed incremental state tracker.

    The `docbt_state` table records, for each (model, document) pair, the
    content hash and code version observed at the last successful run. The
    runner consults this to decide what to reprocess.
    """

    def __init__(self, db_path: Path, schema: str) -> None:
        self.db_path = db_path
        self.schema = schema
        self.con: duckdb.DuckDBPyConnection | None = None
        self._schema_ref: str = ""

    def __enter__(self) -> Self:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        catalog = self.con.execute("SELECT current_database()").fetchone()
        catalog_name = catalog[0] if catalog else "memory"
        self._schema_ref = f'"{catalog_name}"."{self.schema}"'
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema_ref}")
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema_ref}.docbt_state (
                model_name VARCHAR NOT NULL,
                document_id VARCHAR NOT NULL,
                content_hash VARCHAR NOT NULL,
                code_version VARCHAR NOT NULL,
                last_run_at TIMESTAMP NOT NULL,
                PRIMARY KEY (model_name, document_id)
            )
            """
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self.con is not None:
            self.con.close()
            self.con = None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        if self.con is None:
            raise RuntimeError("State must be used as a context manager")
        return self.con

    @property
    def schema_ref(self) -> str:
        """Fully qualified `"catalog"."schema"` for use in SQL."""
        if not self._schema_ref:
            raise RuntimeError("State must be used as a context manager")
        return self._schema_ref

    def get_processed(self, model_name: str) -> dict[str, tuple[str, str]]:
        """Return {document_id: (content_hash, code_version)} for this model."""
        rows = self.connection.execute(
            f"SELECT document_id, content_hash, code_version "
            f"FROM {self.schema_ref}.docbt_state WHERE model_name = ?",
            [model_name],
        ).fetchall()
        return {r[0]: (r[1], r[2]) for r in rows}

    def upsert_processed(
        self, model_name: str, records: list[tuple[str, str, str]]
    ) -> None:
        if not records:
            return
        self.connection.executemany(
            f"""
            INSERT INTO {self.schema_ref}.docbt_state
                (model_name, document_id, content_hash, code_version, last_run_at)
            VALUES (?, ?, ?, ?, current_timestamp)
            ON CONFLICT (model_name, document_id) DO UPDATE SET
                content_hash = excluded.content_hash,
                code_version = excluded.code_version,
                last_run_at  = excluded.last_run_at
            """,
            [[model_name, doc_id, ch, cv] for doc_id, ch, cv in records],
        )

    def clear_model(self, model_name: str) -> None:
        self.connection.execute(
            f"DELETE FROM {self.schema_ref}.docbt_state WHERE model_name = ?",
            [model_name],
        )
