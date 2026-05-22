from __future__ import annotations

import concurrent.futures
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from .backends import ExtractionResult, get_backend
from .config import load_project
from .config.model import ModelConfig
from .config.project import ProjectConfig
from .config.source import SourceConfig
from .dag import ProjectDAG, parse_ref
from .profile import ResolvedProfile, resolve_llm_options, resolve_profile
from .state import State
from .transforms import load_transform
from .versioning import (
    compute_code_version,
    compute_content_hash,
    compute_document_id,
)


class RunError(Exception):
    pass


@dataclass
class DocumentRef:
    source_name: str
    path: Path
    relative_path: str
    document_id: str
    content_hash: str


@dataclass
class ModelRunResult:
    model_name: str
    materialization: str
    kind: str  # "extraction" | "transform"
    backend: str | None = None
    documents_processed: int = 0
    documents_skipped: int = 0
    rows_written: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)


def run_project(
    project_dir: Path,
    *,
    full_refresh: bool = False,
    select: str | None = None,
    exclude: str | None = None,
    target: str | None = None,
    profiles_dir: Path | None = None,
    threads: int = 1,
) -> list[ModelRunResult]:
    project, sources, models = load_project(project_dir)
    resolved = resolve_profile(
        project, project_dir, target=target, profiles_dir=profiles_dir
    )
    dag = ProjectDAG(sources, models)
    selected = dag.select_models(select=select, exclude=exclude)

    source_docs: dict[str, list[DocumentRef]] = {
        s.name: _discover_source(s, project_dir) for s in sources
    }

    db_path = (project_dir / resolved.warehouse.path).resolve()
    results: list[ModelRunResult] = []

    with State(db_path, schema=resolved.warehouse.schema_name) as state:
        for name in selected:
            model = next(m for m in models if m.name == name)
            result = _run_model(
                model=model,
                project=project,
                project_dir=project_dir,
                source_docs=source_docs,
                state=state,
                resolved=resolved,
                full_refresh=full_refresh,
                threads=threads,
            )
            results.append(result)

    return results


def _discover_source(source: SourceConfig, project_dir: Path) -> list[DocumentRef]:
    source_dir = (project_dir / source.path).resolve()
    if not source_dir.exists():
        return []
    pattern = f"**/{source.file_pattern}" if source.recursive else source.file_pattern
    files = sorted(p for p in source_dir.glob(pattern) if p.is_file())
    refs: list[DocumentRef] = []
    for p in files:
        relative_path = str(p.relative_to(source_dir))
        refs.append(
            DocumentRef(
                source_name=source.name,
                path=p,
                relative_path=relative_path,
                document_id=compute_document_id(source.name, relative_path),
                content_hash=compute_content_hash(p),
            )
        )
    return refs


def _run_model(
    *,
    model: ModelConfig,
    project: ProjectConfig,
    project_dir: Path,
    source_docs: dict[str, list[DocumentRef]],
    state: State,
    resolved: ResolvedProfile,
    full_refresh: bool,
    threads: int = 1,
) -> ModelRunResult:
    start = time.monotonic()
    if model.extraction is not None:
        result = _run_extraction_model(
            model=model,
            project=project,
            project_dir=project_dir,
            source_docs=source_docs,
            state=state,
            resolved=resolved,
            full_refresh=full_refresh,
            threads=threads,
        )
    elif model.transform is not None:
        result = _run_transform_model(
            model=model,
            project_dir=project_dir,
            state=state,
            resolved=resolved,
        )
    else:
        raise RunError(
            f"Model '{model.name}' has neither extraction nor transform configured"
        )
    result.duration_seconds = round(time.monotonic() - start, 3)
    return result


def _run_extraction_model(
    *,
    model: ModelConfig,
    project: ProjectConfig,
    project_dir: Path,
    source_docs: dict[str, list[DocumentRef]],
    state: State,
    resolved: ResolvedProfile,
    full_refresh: bool,
    threads: int = 1,
) -> ModelRunResult:
    assert model.extraction is not None
    backend_name = model.extraction.backend or project.extraction.default_backend
    backend = get_backend(backend_name)
    options = model.extraction.options
    if backend_name == "llm":
        options = resolve_llm_options(options, resolved)

    if not model.source:
        raise RunError(f"Extraction model '{model.name}' must declare a `source:`")
    source_name = parse_ref(model.source)
    docs = source_docs.get(source_name)
    if docs is None:
        raise RunError(
            f"Model '{model.name}' references unknown source '{source_name}'"
        )

    code_version = compute_code_version(
        extraction=model.extraction,
        transform=None,
        project_dir=project_dir,
    )

    is_incremental = model.materialization == "incremental" and not full_refresh
    processed_state = state.get_processed(model.name) if is_incremental else {}

    docs_to_process: list[DocumentRef] = []
    for doc in docs:
        if is_incremental:
            prior = processed_state.get(doc.document_id)
            if prior == (doc.content_hash, code_version):
                continue
        docs_to_process.append(doc)

    skipped = len(docs) - len(docs_to_process)
    errors: list[str] = []
    rows: list[dict[str, Any]] = []
    state_records: list[tuple[str, str, str]] = []

    def _one(doc: DocumentRef) -> tuple[DocumentRef, ExtractionResult | None, str | None]:
        try:
            return doc, backend.extract(doc.path, options), None
        except Exception as e:
            return doc, None, str(e)

    if threads > 1 and len(docs_to_process) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as ex:
            extracted = list(ex.map(_one, docs_to_process))
    else:
        extracted = [_one(d) for d in docs_to_process]

    for doc, result, err in extracted:
        if err is not None or result is None:
            errors.append(f"{doc.relative_path}: {err}")
            continue
        rows.append(_row_for_extraction(doc, code_version, result))
        state_records.append((doc.document_id, doc.content_hash, code_version))

    rows_written = 0
    if rows or full_refresh or model.materialization == "full":
        rows_written = _materialize_extraction(
            con=state.connection,
            schema_ref=state.schema_ref,
            table=model.name,
            rows=rows,
            materialization=model.materialization,
            full_refresh=full_refresh,
        )

    if full_refresh:
        state.clear_model(model.name)
    state.upsert_processed(model.name, state_records)

    return ModelRunResult(
        model_name=model.name,
        materialization=model.materialization,
        kind="extraction",
        backend=backend_name,
        documents_processed=len(docs_to_process),
        documents_skipped=skipped,
        rows_written=rows_written,
        errors=errors,
    )


def _row_for_extraction(
    doc: DocumentRef, code_version: str, result: ExtractionResult
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "document_id": doc.document_id,
        "source_path": doc.relative_path,
        "content_hash": doc.content_hash,
        "code_version": code_version,
    }
    for key, value in result.fields.items():
        row[key] = _scalarize(value)
    return row


def _scalarize(value: Any) -> Any:
    """Serialize nested types as JSON strings so DuckDB gets a flat schema."""
    if isinstance(value, dict | list):
        return json.dumps(value, default=str)
    return value


def _materialize_extraction(
    *,
    con: duckdb.DuckDBPyConnection,
    schema_ref: str,
    table: str,
    rows: list[dict[str, Any]],
    materialization: str,
    full_refresh: bool,
) -> int:
    full_name = f"{schema_ref}.{table}"

    if not rows and materialization == "incremental" and not full_refresh:
        return 0

    df = pl.DataFrame(rows) if rows else pl.DataFrame()
    con.register("docbt_staging", df)
    try:
        if materialization == "full" or full_refresh:
            con.execute(f"CREATE OR REPLACE TABLE {full_name} AS SELECT * FROM docbt_staging")
        else:
            con.execute(
                f"CREATE TABLE IF NOT EXISTS {full_name} AS "
                f"SELECT * FROM docbt_staging LIMIT 0"
            )
            ids = df["document_id"].to_list() if "document_id" in df.columns else []
            if ids:
                placeholders = ",".join(["?"] * len(ids))
                con.execute(
                    f"DELETE FROM {full_name} WHERE document_id IN ({placeholders})",
                    ids,
                )
            con.execute(f"INSERT INTO {full_name} SELECT * FROM docbt_staging")
    finally:
        con.unregister("docbt_staging")

    return df.height


def _run_transform_model(
    *,
    model: ModelConfig,
    project_dir: Path,
    state: State,
    resolved: ResolvedProfile,
) -> ModelRunResult:
    assert model.transform is not None
    if model.transform.type != "python":
        raise RunError(
            f"Model '{model.name}': only `type: python` transforms are supported in v1"
        )
    if not model.transform.module:
        raise RunError(f"Model '{model.name}': transform requires a `module:`")
    if not model.depends_on:
        raise RunError(
            f"Transform model '{model.name}' must declare `depends_on:` for v1"
        )

    import inspect

    from .transforms import TransformContext

    transform_fn = load_transform(model.transform.module, project_dir)
    deps: dict[str, pl.DataFrame] = {}
    for dep_ref in model.depends_on:
        dep_name = parse_ref(dep_ref)
        deps[dep_name] = state.connection.execute(
            f"SELECT * FROM {state.schema_ref}.{dep_name}"
        ).pl()

    sig = inspect.signature(transform_fn)
    if len(sig.parameters) >= 2:
        ctx = TransformContext(
            project_dir=project_dir,
            profile_name=resolved.profile_name,
            target_name=resolved.target_name,
            warehouse=resolved.warehouse,
            llm=resolved.llm,
        )
        output = transform_fn(deps, ctx)
    else:
        output = transform_fn(deps)

    if not isinstance(output, pl.DataFrame):
        raise RunError(
            f"Transform '{model.transform.module}' must return a polars.DataFrame"
        )

    full_name = f"{state.schema_ref}.{model.name}"
    state.connection.register("docbt_staging", output)
    try:
        state.connection.execute(
            f"CREATE OR REPLACE TABLE {full_name} AS SELECT * FROM docbt_staging"
        )
    finally:
        state.connection.unregister("docbt_staging")

    return ModelRunResult(
        model_name=model.name,
        materialization=model.materialization,
        kind="transform",
        rows_written=output.height,
    )


def clean_project(
    project_dir: Path,
    *,
    target: str | None = None,
    profiles_dir: Path | None = None,
) -> Path:
    """Delete the DuckDB output file. Returns the path that was removed."""
    project, _, _ = load_project(project_dir)
    resolved = resolve_profile(
        project, project_dir, target=target, profiles_dir=profiles_dir
    )
    db_path = (project_dir / resolved.warehouse.path).resolve()
    if db_path.exists():
        db_path.unlink()
    return db_path
