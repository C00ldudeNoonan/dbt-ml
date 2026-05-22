# dbt_consumer — dbt-duckdb on top of docbt

This is the round-trip smoke test for `docbt emit-dbt-sources`. It's a
dbt-duckdb project pointed at the **same** DuckDB file that
`examples/invoice_pipeline` materializes into, with two downstream models that
read from docbt sources via `{{ source('docbt_invoice_pipeline', 'raw_invoices') }}`.

## Run it

From the repo root (`docbt/`):

```bash
# 1. Materialize docbt
uv run docbt --project-dir examples/invoice_pipeline seed --count 50
uv run docbt --project-dir examples/invoice_pipeline run

# 2. Emit dbt sources.yml into this project's models/sources/
uv run docbt --project-dir examples/invoice_pipeline emit-dbt-sources \
  --output examples/dbt_consumer/models/sources/_docbt_sources.yml

# 3. Run dbt
cd examples/dbt_consumer
uv sync
uv run dbt build --profiles-dir .
```

`dbt build` will:
- parse the generated `_docbt_sources.yml` and verify the source tables exist,
- materialize `invoice_facts` and `vendor_overview` as new tables in the same DuckDB file (schema `dbt_marts`),
- run the column tests defined in `models/marts/schema.yml`.

After it succeeds, both the docbt tables and dbt tables live in one DuckDB file:

```bash
duckdb ../invoice_pipeline/target/docbt.duckdb -c "SHOW ALL TABLES"
```

## What this proves

- docbt's emitted `sources.yml` is dbt-parseable.
- Column tests (`not_null`, `unique`) translate cleanly.
- A dbt model can `{{ source(...) }}` directly into a docbt-materialized table without any glue.
- docbt-produced and dbt-produced tables can coexist in one DuckDB file.
