# docbt

**dbt for unstructured data.** Declarative YAML pipelines that turn folders of
documents — PDFs, markdown, HTML, JSON, email, free-form text — into warehouse
tables. Incremental processing, schema tests, dbt-style selectors, profiles,
and a manifest artifact you can wire into other tools.

This is the v0.1 PoC: pure Python, DuckDB warehouse. **v0.2 is in scope** —
adding RAG support (chunking, embeddings, vector storage via LanceDB) and a
warehouse adapter pattern aimed at the dbt-core set (Postgres, Snowflake,
BigQuery, Databricks, …). The full Rust+Python design lives in
`docbt-core-implementation-plan.md` and is deferred to a later v2.

## Where docbt fits

The 2026 landscape for unstructured document pipelines has two stable poles:

- **Managed RAG-as-a-Service** (Vectara, Bedrock Knowledge Bases, Vertex AI
  Search, Snowflake Cortex Search, Glean) — best when time-to-value matters
  and the team can't dedicate ML engineers.
- **Compose best-of-breed Python components** (LlamaParse → contextual
  chunking → Voyage embeddings → Qdrant → Cohere Rerank → Ragas) — best when
  retrieval quality, multi-tenant isolation, or unusual document types
  matter and you have ≥2 ML engineers.

docbt is the **opinionated, declarative path through the second lane**.
Where LlamaIndex is imperative Python, docbt is YAML + a manifest + tests +
lineage. Where Snowflake Cortex Search hides everything, docbt makes every
stage inspectable and reproducible. It's *dbt-shaped*: the same DAG +
selectors + tests + artifacts pattern, applied to unstructured data.

---

## You have a folder of files. Get them into your warehouse.

```bash
# Install (once it's published; today: clone and `uv sync`)
uv add git+https://github.com/<your-org>/docbt    # or local: uv pip install -e .

# 1. Scaffold a project for whatever shape your data is
uv run docbt init my_project --template pdf      # or json, markdown, html

# 2. Drop your files into ./my_project/data/pdfs/  (or wherever the source points)

# 3. Run it
cd my_project
uv run docbt run

# 4. Query the result
duckdb target/docbt.duckdb -c "SELECT * FROM my_project.raw_pdf_text LIMIT 5"
```

That's the whole loop. Everything else (selectors, profiles, tests, LLM
extraction, dbt handoff) is opt-in on top.

## What docbt actually does

| Concept            | What it means                                                                  |
|--------------------|--------------------------------------------------------------------------------|
| **Source**         | A glob over a folder. `*.pdf`, `*.json`, `*.html`, `*.md` — your choice.        |
| **Extraction model** | One row per source file, produced by a backend (pdf, json, markdown, html, llm). |
| **Transform model**  | A Python module returning a Polars DataFrame, depends on other models via `ref()`. |
| **Materialization**  | `full` (always replace) or `incremental` (skip unchanged input on re-runs).      |
| **Tests**          | `not_null`, `unique`, `min_rows`, custom Python — with `severity: warn` if you want.|
| **Profile**        | Warehouse + LLM config, swappable per `--target dev|prod`. No credentials in models. |
| **Artifacts**      | `target/manifest.json`, `target/run_results.json`, `target/sources.yml` (for dbt). |

## Backends

| Backend    | Reads             | Notes                                                                                     |
|------------|-------------------|-------------------------------------------------------------------------------------------|
| `json`     | `*.json`          | Projects keys per `options.fields`. Deterministic, no API.                                |
| `markdown` | `*.md`            | YAML frontmatter + `body` + optional `word_count`. Deterministic, no API.                 |
| `pdf`      | `*.pdf`           | Per-page text via pypdf. Warns on empty extracts (likely scanned). Deterministic, no API. |
| `html`     | `*.html`/`*.htm`  | Body text + CSS selectors + OpenGraph/meta via BeautifulSoup. Deterministic, no API.      |
| `email`    | `*.eml`           | from/to/subject/date/body via stdlib `email`. Deterministic, no API.                      |
| `llm`      | `*.txt`/`*.md`    | Claude tool-use → structured fields. Responses cached. Requires `ANTHROPIC_API_KEY`.      |

Add a new backend = drop a file under `src/docbt/backends/`, inherit from
`BaseBackend`, decorate with `@register`. No plugin system needed for v1.

## The CLI

```
docbt init <name> [--template {json,pdf,markdown,html}]   # scaffold a fresh project
docbt seed [--count N] [--type {invoices,posts,...,tickets,emails}]
docbt compile                                             # parse YAML, validate DAG, write manifest.json
docbt graph                                               # Mermaid DAG to stdout
docbt run [--select EXPR] [--exclude EXPR] [--full-refresh] [--threads N] [--watch]
docbt test [--select EXPR] [--exclude EXPR]
docbt show <model> [--limit N]                            # peek at a materialized table
docbt source freshness                                    # mtime vs warn_after/error_after
docbt docs generate [--output DIR]                        # static HTML site from manifest.json
docbt docs serve [--port N]                               # local http.server over target/docs/
docbt emit-dbt-sources [--output PATH]                    # write dbt-compatible sources.yml
docbt clean                                               # delete the project's DuckDB

# Global flags (work on every command):
docbt --project-dir <dir> --profiles-dir <dir> --target <name> <command>
```

### Useful flags

- `--watch` on `run` listens to source paths and re-runs on file changes
  (debounced 500ms). Ctrl-C to stop.
- `--threads N` parallelizes per-document extraction within an extraction
  model. Most useful for PDF / LLM / HTML (I/O- or API-bound). The LLM cache
  is lock-serialized so threading is safe.

## Selectors

dbt-shaped. Whitespace-separated tokens, optional `+` modifiers, `tag:` prefix.

```bash
docbt run --select raw_pdf_text       # one model
docbt run --select 'raw_pdf_text+'    # plus all downstream
docbt run --select '+invoice_summary' # plus all upstream
docbt run --select 'tag:raw+'         # all models tagged "raw" + their downstream
docbt run --exclude tag:expensive
```

## Profiles

Warehouse and LLM config live in `profiles.yml`, *not* in `docbt_project.yml`.
Project YAML says `profile: my_project`; profile says where to write and which
LLM to call. Swap `--target prod` to switch environments.

```yaml
# profiles.yml — sits next to docbt_project.yml, or in ~/.docbt/profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      warehouse:
        type: duckdb
        path: ./target/docbt.duckdb
        schema: my_project
      llm:
        provider: anthropic
        model: claude-haiku-4-5
        api_key_env: ANTHROPIC_API_KEY
        cache_path: ./target/llm_cache.duckdb
    prod:
      warehouse:
        type: duckdb
        path: /data/prod/docbt.duckdb
        schema: my_project_prod
      llm:
        model: claude-sonnet-4-6
        cache_path: /data/prod/llm_cache.duckdb
```

Lookup order: `--profiles-dir` flag → `$DOCBT_PROFILES_DIR` →
`<project>/profiles.yml` → `~/.docbt/profiles.yml`.

## Built-in text preprocessing

Reference any of these as a Python transform module — no project-local code
needed. Users can override by writing their own `transforms/<name>.py`
(project-local files win over installed packages).

```yaml
- name: post_text_stats
  depends_on: [ref('raw_posts')]
  transform:
    type: python
    module: docbt.text.transforms.text_stats   # built-in, ships with docbt
    options:
      text_field: body
      emit: [word_count, sentence_count]
```

| Module                                    | What it does                                                                   |
|-------------------------------------------|--------------------------------------------------------------------------------|
| `docbt.text.transforms.text_stats`        | Adds `word_count` / `char_count` / `sentence_count` / `paragraph_count`         |
| `docbt.text.transforms.clean_encoding`    | Fixes mojibake (UTF-8-as-Latin-1 confusion) via ftfy                            |
| `docbt.text.transforms.detect_language`   | Adds a 2-letter ISO language code per row via langdetect                        |
| `docbt.text.transforms.count_tokens`      | Adds `token_count` for an OpenAI / Claude-style tokenizer (tiktoken)            |
| `docbt.text.transforms.find_duplicates`   | Flags near-duplicate rows via MinHash + LSH (Jaccard threshold configurable)    |
| `docbt.text.transforms.redact_pii`        | Detects + redacts PII via Microsoft Presidio (requires `en_core_web_sm` spaCy model) |

All are pure functions importable via `from docbt.text import …` if you'd
rather wire them into your own transforms.

**PII setup** — `redact_pii` uses spaCy under the hood. First-time install:

```bash
python -m spacy download en_core_web_sm
```

Without the model, calls into `redact_pii` raise a clear `PIIError` pointing
at this command.

## Tests

```yaml
tests:
  - not_null: [vendor, total]            # column-level, fails the run
  - unique: invoice_id                   # single-column
  - unique: [a, b]                       # composite (compiled to dbt_utils on emit)
  - min_rows: 100
  - not_empty                            # bare-string form of min_rows: 1
  - not_null: total, severity: warn      # warn doesn't fail the run
  - python: tests.my_check               # custom: tests/my_check.py defines run(con, table_ref) -> str | None
```

## Examples in this repo

| Path                                | What it shows                                                          |
|-------------------------------------|------------------------------------------------------------------------|
| `examples/invoice_pipeline/`        | JSON extraction → per-vendor + monthly aggregations                    |
| `examples/blog_pipeline/`           | Markdown frontmatter → per-author word counts                          |
| `examples/pdf_invoice_pipeline/`    | PDFs → text via pypdf → LLM-extracted structured fields                |
| `examples/llm_invoice_pipeline/`    | Free-form invoice text → LLM extraction (no PDF stage)                 |
| `examples/support_tickets_pipeline/`| JSON tickets → open queue + SLA breaches + per-team workload (no LLM)  |
| `examples/dbt_consumer/`            | dbt-duckdb project consuming docbt-materialized tables                 |

Each example is runnable end-to-end with `uv run docbt --project-dir examples/<name> ...`.

## Composing with dbt (dbt-duckdb)

docbt and dbt can share a DuckDB file: docbt does the unstructured→structured
"E", dbt does the SQL "T". The bridge:

```bash
uv run docbt --project-dir examples/invoice_pipeline run
uv run docbt --project-dir examples/invoice_pipeline emit-dbt-sources \
  --output examples/dbt_consumer/models/sources/_docbt_sources.yml

cd examples/dbt_consumer && uv sync && uv run dbt build --profiles-dir .
```

`emit-dbt-sources` translates docbt tables into a dbt-compatible `sources.yml`.
Column tests carry over (`not_null`, single-column `unique`); composite unique
becomes a `dbt_utils.unique_combination_of_columns` macro test.

## Artifacts

Every `docbt compile` / `docbt run` writes to `target/`:

- **`manifest.json`** — project, sources, models, refs, tags, `code_version` per
  model, DAG nodes+edges+execution order. Re-generated each run.
- **`run_results.json`** — per-model documents processed/skipped, rows written,
  duration, errors.
- **`sources.yml`** — only when you call `emit-dbt-sources`. dbt-shaped.
- **`docs/`** — static HTML site (`docbt docs generate`) with project overview,
  Mermaid DAG, per-model pages. Serve locally with `docbt docs serve`.

External tools (lineage viewers, CI dashboards, the dbt-consumer above)
consume these.

## Benchmarks

```bash
uv run python scripts/benchmark.py --count 5000
```

5000-doc benchmark on the JSON backend:

```
seed 5000 invoices                          0.8s    →   6.3k docs/sec
first run (cold)                            4.8s    →   1.0k docs/sec
second run (all skipped)                    0.3s    →  19.9k docs/sec
third run (1 changed)                       0.3s    →  18.2k docs/sec
full-refresh                                4.3s    →   1.2k docs/sec
```

Linear through 5k. Bottleneck is single-threaded extraction; parallelism is a v2 item.

## Layout

```
src/docbt/
├── cli.py                 # click: init/seed/compile/graph/run/test/show/clean/source freshness/emit-dbt-sources
├── config/                # pydantic models for project/source/model/profile + loader
├── profile.py             # profile discovery + resolution (warehouse + llm)
├── dag.py                 # graphlib-based DAG, selectors (+ name +, tag:foo), Mermaid render
├── state.py               # DuckDB-backed incremental state
├── runner.py              # extract → materialize orchestration
├── manifest.py            # target/manifest.json + run_results.json
├── dbt_export.py          # target/sources.yml (dbt-shaped)
├── freshness.py           # source mtime check
├── backends/              # json, markdown, pdf, html, llm
├── transforms/runner.py   # loads user Python transform modules + TransformContext
├── checks/                # schema tests + custom Python tests + severity
├── synth/                 # synthetic data generators per shape
└── templates/             # init scaffolds for {json,pdf,markdown,html}
```

## Roadmap

**v0.2 — RAG + warehouse adapter pattern.** Tracked in GitHub issues
tagged `roadmap`. The four headline pieces:

1. **Warehouse adapter pattern** matching dbt-core's set. v0.2 starts with
   DuckDB (current) + LanceDB (lakehouse-style vector store); subsequent
   versions add Postgres, then Snowflake / BigQuery / Databricks / Redshift.
2. **Chunking primitives** as a first-class model kind: recursive (default),
   token-aware, layout-aware, optional Anthropic Contextual Retrieval
   (49–67% retrieval failure reduction per published numbers).
3. **Embedding primitives** as a first-class model kind: Voyage, Cohere,
   OpenAI, and local sentence-transformers providers. Same cache mechanic
   as today's LLM backend so re-runs are free.
4. **Layout-aware OSS parsers** as additional backends: Docling (privacy +
   table quality), Marker (best OSS layout fidelity).

**Deferred beyond v0.2:**

- Rust CLI + PyO3 bridge.
- Metaxy integration (replace `state.py` with `MetadataStore`).
- Field-level lineage (`version_from: [ref('x').field_a]`).
- Parallel *model* execution (today's `--threads` parallelizes within a model).
- Managed parser backends (Reducto, Mistral OCR 3, LlamaParse) — generic
  remote-parser adapter pattern when there's a real ask.
- Reranker hooks (Cohere Rerank, Voyage Rerank).
- Multi-LLM-provider adapters (Bedrock, Vertex, OpenAI structured output).
- PII detection / redaction (Microsoft Presidio).
- Ragas integration (`docbt eval`).
