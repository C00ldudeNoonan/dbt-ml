# CLAUDE.md — docbt

## What this is

docbt is "dbt for unstructured data." v0.1 (merged) is the pure-Python PoC:
DuckDB warehouse, 6 backends (json/markdown/pdf/html/email/llm), profiles,
manifest artifacts, dbt-shaped selectors, schema tests, incremental
materialization. The full Rust+Python design lives in
`docbt-core-implementation-plan.md` and is deferred to a later v2.

v0.2 is being scoped: see the GitHub issue tagged `roadmap` for the live plan.
The headline shifts are RAG support (chunking, embeddings, vector storage)
and broadening warehouse support to match the dbt-core adapter set.

## Scope discipline

- **No Rust, no PyO3.** Still pure Python through v0.2.
- **Match dbt-core warehouses over time.** v0.2 starts with DuckDB + LanceDB
  (lakehouse-style vector store); subsequent versions add Postgres, then
  Snowflake / BigQuery / Databricks / Redshift via a warehouse adapter
  pattern. Avoid changes that hard-code DuckDB; route through the adapter.
- **Metaxy is deferred** — incremental state lives in the warehouse (today
  `state.py` with DuckDB). When we move to a real adapter pattern, state
  needs to follow the adapter too.

## Build & run

```
uv sync                                # install deps
uv run docbt init my_project           # scaffold (templates: json/pdf/markdown/html)
uv run docbt seed --count 20           # synthetic data
uv run docbt run                       # extract + materialize
uv run docbt test                      # schema + custom python tests
uv run docbt docs generate             # static HTML site from manifest.json
uv run pytest                          # docbt package's own tests
```

## Conventions

- Python 3.12+, type hints everywhere, ruff clean.
- Pydantic v2 for config models.
- Click for CLI.
- No comments explaining what code does — only why, and only when non-obvious.

## Key files

- `docbt/src/docbt/config/` — Pydantic models for project/source/model/profile YAML
- `docbt/src/docbt/dag.py` — graphlib-based DAG + selectors + Mermaid render
- `docbt/src/docbt/state.py` — DuckDB-backed incremental state (per-adapter in v0.2+)
- `docbt/src/docbt/runner.py` — extract → materialize orchestration
- `docbt/src/docbt/backends/` — extraction backends
- `docbt/src/docbt/profile.py` — profile discovery + resolution
- `docbt/src/docbt/manifest.py` / `docs.py` / `dbt_export.py` — artifacts
- `docbt/examples/*/` — runnable example projects

## Private working notes

Anything under `docs/research/`, `docs/private/`, or `_scratch/` is gitignored.
Internal industry research and design sketches live there; never commit them.

## Brand note

"dbt" is always lowercase, even at the start of a sentence. The project name
here is "docbt" (lowercase). dbt Labs is the company (capital L).
