# Changelog

## Unreleased

### dbt-schema artifacts

- New `emit-dbt-manifest` command writes a dbt-conformant `manifest.json`
  (schema v12) describing docbt's materialized tables. Each docbt table is
  emitted as a dbt source under `docbt_<project>`, with docbt's lineage and
  `code_version` preserved under each node's `meta.docbt` so dbt catalog/lineage
  tooling and manifest-diffing can read docbt's DAG directly.
- `run --emit-dbt-artifacts` additionally writes a dbt-schema `manifest.json` +
  `run_results.json` (schema v6) to `<target>/dbt/`, alongside docbt's native
  artifacts (which are unchanged).

### dbt Fusion compatibility

- `emit-dbt-sources` now surfaces a warning (instead of silently dropping)
  for test specs with no faithful dbt source-test equivalent
  (`min_rows`, `not_empty`, `has_text`).
- `emit-dbt-sources --emit-packages` writes a `packages.yml` declaring
  `dbt_utils` whenever a composite-unique macro test is emitted, so the output
  parses under the strict dbt Fusion engine (which fails, rather than warns, on
  undeclared macros).
- New `dbt-compat` GitHub Actions workflow validates the emitted sources against
  both dbt-core (dbt-duckdb, hard gate) and the dbt Fusion engine
  (informational while its DuckDB support matures) via the `dbt_consumer`
  example.

## v0.1.0 (unreleased)

Initial public preview.

### Backends
- `json` — project keys from JSON objects (deterministic, no API)
- `markdown` — frontmatter + body + word count
- `pdf` — text extraction via pypdf, with empty-text warnings for scanned PDFs
- `html` — body text, CSS selectors, OpenGraph, meta tags via BeautifulSoup
- `llm` — Claude-backed structured extraction with response caching

### Pipeline mechanics
- Declarative YAML: project, sources, extraction models, transform models
- DAG via `graphlib`, `ref()` syntax, cycle detection
- Incremental materialization keyed on content + code version
- `full` / `incremental` materialization
- `target/manifest.json` and `target/run_results.json` artifacts on every run

### CLI
- `init` (with `--template {json,pdf,markdown,html}`)
- `seed`, `compile`, `graph`, `run` (with `--full-refresh`), `test`, `show`, `clean`
- `source freshness` — mtime-vs-threshold check
- `emit-dbt-sources` — write dbt-compatible `sources.yml`

### Selection + filtering
- `--select` / `--exclude` with dbt-shaped syntax: name, `name+`, `+name`, `+name+`
- `tag:` prefix for tag-based selection
- `tags:` on models and sources

### Testing
- Built-in: `not_null`, `unique`, `min_rows`, `not_empty`
- Severity: `severity: warn` downgrades fail → warn (exit 0)
- Custom Python tests: drop `tests/<module>.py` with `run(con, table_ref) -> str | None`

### Profiles
- dbt-shaped `profiles.yml` with per-target warehouse + llm config
- Lookup: `--profiles-dir` → `$DOCBT_PROFILES_DIR` → `<project>/profiles.yml` → `~/.docbt/profiles.yml`
- `--target` flag selects within active profile
- LLM cache and model id come from profile, with per-model overrides

### Composition
- `docbt emit-dbt-sources` writes dbt-compatible `sources.yml` so a
  `dbt-duckdb` project can `{{ source(...) }}` docbt-materialized tables in the same DuckDB file
- Worked example in `examples/dbt_consumer/` (verified end-to-end with `dbt build`)
