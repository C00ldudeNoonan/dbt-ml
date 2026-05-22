# Contributing to docbt

docbt is a small, focused Python project. Contributions welcome.

## Setup

```bash
git clone https://github.com/<your-org>/docbt
cd docbt
uv sync
uv run pytest -q
```

## Local checks (required before opening a PR)

```bash
uv run ruff check         # lint
uv run pytest -q          # tests
```

CI runs both on every push and PR (see `.github/workflows/ci.yml`).

## Adding a new backend

1. New file in `src/docbt/backends/` inheriting from `BaseBackend`.
2. Decorate the class with `@register`.
3. Implement `name()`, `supported_formats()`, `extract(path, options)`.
4. Register the import in `src/docbt/backends/__init__.py` (the side-effect
   import is what triggers `@register`).
5. Add a synth generator under `src/docbt/synth/` if you want to support
   `docbt seed --type <name>`.
6. Add tests under `tests/test_<backend>_backend.py`.
7. Add an init template under `src/docbt/templates/<backend>/` if a fresh-
   project starter makes sense.

## Adding a new schema test

1. Add the name to `SUPPORTED_TESTS` in `src/docbt/checks/schema.py`.
2. Implement a helper function returning `TestResult`(s).
3. Wire into `_run_named_test`.
4. Add tests under `tests/test_checks.py`.

## Adding a new CLI command

1. Add the click subcommand in `src/docbt/cli.py`.
2. Wire through `ctx.obj["project_dir"]` / `profiles_dir` / `target` like the
   existing commands do.
3. Raise `click.ClickException` from any `*Error` exception you catch.
4. Update README's CLI section.

## Scope discipline

docbt v1 is intentionally limited:

- DuckDB-only. No Snowflake/BigQuery adapters in v1.
- Pure Python. No Rust until the model is proven.
- LLM provider is Anthropic-only. Bedrock/Vertex/OpenAI are v2.
- Schema tests are the four listed in README, plus custom Python. No dbt-style
  generic test machinery in v1.

If a change pulls in any of the above, push back or open a discussion first.

## Commit style

Conventional commits not required, but tight subject lines please:

```
add html backend
fix: pdf backend silent failure on scanned PDFs
test: cover profile lookup with $DOCBT_PROFILES_DIR
docs: rewrite README quickstart for end users
```

## Releasing

Bump `version` in `pyproject.toml`, update `CHANGELOG.md`, tag the commit
`v0.X.Y`. PyPI publishing is manual today (`uv build && uv publish`).
