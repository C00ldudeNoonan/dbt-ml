# CLAUDE.md — docbt v1 PoC

## What this is

docbt is "dbt for unstructured data." This workspace contains the v1 proof of
concept: pure Python, DuckDB as both metadata + output store, synthetic JSON
"documents" instead of real PDFs. The full Rust+Python plan lives in
`docbt-core-implementation-plan.md` and is deferred to v2.

The approved v1 plan is at
`~/.claude/plans/system-instruction-you-are-working-lovely-chipmunk.md`.

## Scope discipline

- No Rust, no PyO3, no PDF/OCR backends in v1. If a change pulls those in, push back.
- DuckDB is the only data store. Don't add Parquet/JSON output writers unless asked.
- Metaxy is deferred — incremental state is hand-rolled in DuckDB (`state.py`).

## Build & run

```
uv sync                                # install deps
uv run docbt seed --count 50           # generate synthetic invoices
uv run docbt compile                   # parse YAML + validate DAG
uv run docbt run                       # extract + materialize to DuckDB
uv run docbt test                      # run schema tests
uv run pytest                          # test the docbt package itself
```

## Conventions

- Python 3.12+, type hints everywhere, ruff + mypy clean.
- Pydantic v2 for config models.
- Click for CLI.
- No comments explaining what code does — only why, and only when non-obvious.

## Key files (once built)

- `docbt/src/docbt/config/` — Pydantic models for project/source/model YAML
- `docbt/src/docbt/dag.py` — graphlib-based DAG + Mermaid render
- `docbt/src/docbt/state.py` — DuckDB-backed incremental state
- `docbt/src/docbt/runner.py` — extract → materialize orchestration
- `docbt/src/docbt/backends/` — extraction backends (json_backend.py only in v1)
- `docbt/examples/invoice_pipeline/` — runnable example

## Brand note

"dbt" is always lowercase, even at the start of a sentence. The project name
here is "docbt" (lowercase). dbt Labs is the company (capital L).
