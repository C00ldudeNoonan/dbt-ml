from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from .adapters import WarehouseAdapter
from .config.model import MLConfig, ModelConfig
from .config.project import ProjectConfig
from .dag import parse_ref

_TOKEN_RE = re.compile(r"\w+")


@dataclass
class ClassicMLRun:
    df: pl.DataFrame
    artifact_path: Path
    artifact_version: str
    training_input: dict[str, Any]
    metrics: dict[str, Any]


def run_classic_ml_model(
    *,
    model: ModelConfig,
    project: ProjectConfig,
    project_dir: Path,
    adapter: WarehouseAdapter,
) -> ClassicMLRun:
    assert model.ml is not None
    if model.ml.task != "features":
        raise NotImplementedError(
            f"ML task '{model.ml.task}' is not executable yet; first supported task is 'features'."
        )
    provider = model.ml.provider or "builtin.tfidf"
    if provider != "builtin.tfidf":
        raise NotImplementedError(
            f"ML provider '{provider}' is not executable yet; "
            "first supported provider is 'builtin.tfidf'."
        )
    return _run_tfidf(
        model=model,
        ml=model.ml,
        project=project,
        project_dir=project_dir,
        adapter=adapter,
    )


def _run_tfidf(
    *,
    model: ModelConfig,
    ml: MLConfig,
    project: ProjectConfig,
    project_dir: Path,
    adapter: WarehouseAdapter,
) -> ClassicMLRun:
    if not model.depends_on:
        raise ValueError(f"ML model '{model.name}' must declare depends_on.")
    if not ml.text_field:
        raise ValueError(f"ML model '{model.name}' requires ml.text_field.")

    source_name = parse_ref(model.depends_on[0])
    source_df = adapter.query_df(f"SELECT * FROM {adapter.table_ref(source_name)}")
    if ml.text_field not in source_df.columns:
        raise ValueError(
            f"ML model '{model.name}' text_field '{ml.text_field}' "
            f"is not present in '{source_name}'."
        )

    artifact_path = _artifact_path(ml, model, project, project_dir)
    rows = _source_rows(source_df, ml.text_field)
    training_input = _training_input(model.depends_on, rows)

    if ml.mode in {"fit_transform", "fit"}:
        vocab, idf_by_term, doc_tokens = _fit_tfidf(rows, ml.options)
        metadata = _metadata(
            model=model,
            ml=ml,
            training_input=training_input,
            vocabulary=vocab,
            idf_by_term=idf_by_term,
        )
        _write_artifact(artifact_path, metadata, vocab, idf_by_term)
    elif ml.mode in {"predict", "load_pretrained"}:
        metadata, vocab, idf_by_term = _read_artifact(artifact_path)
        doc_tokens = [_tokenize_with_options(row["text"], ml.options) for row in rows]
    else:
        raise ValueError(f"Unsupported ML mode: {ml.mode}")

    features = _feature_rows(rows, doc_tokens, vocab, idf_by_term, source_name)
    metrics = {
        "row_count": len(rows),
        "vocabulary_size": len(vocab),
        "feature_rows": len(features),
    }
    if ml.mode == "fit":
        df = pl.DataFrame(
            [
                {
                    "artifact_version": metadata["artifact_version"],
                    "row_count": len(rows),
                    "vocabulary_size": len(vocab),
                }
            ]
        )
    else:
        df = pl.DataFrame(features) if features else _empty_feature_df()

    return ClassicMLRun(
        df=df,
        artifact_path=artifact_path,
        artifact_version=str(metadata["artifact_version"]),
        training_input=metadata.get("training_input", training_input),
        metrics=metrics,
    )


def _artifact_path(
    ml: MLConfig,
    model: ModelConfig,
    project: ProjectConfig,
    project_dir: Path,
) -> Path:
    if ml.artifact.path is not None:
        path = ml.artifact.path
        return path if path.is_absolute() else project_dir / path
    return project_dir / project.target_path / "artifacts" / model.name


def _source_rows(df: pl.DataFrame, text_field: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(df.iter_rows(named=True)):
        text = "" if row[text_field] is None else str(row[text_field])
        row_id = str(row.get("document_id") or row.get("id") or index)
        payload: dict[str, Any] = {"row_index": index, "row_id": row_id, "text": text}
        if "document_id" in row:
            payload["document_id"] = row["document_id"]
        if "source_path" in row:
            payload["source_path"] = row["source_path"]
        rows.append(payload)
    return rows


def _training_input(depends_on: list[str], rows: list[dict[str, Any]]) -> dict[str, Any]:
    content = [
        {"row_id": row["row_id"], "text": row["text"]}
        for row in rows
    ]
    raw = json.dumps(content, sort_keys=True, separators=(",", ":"))
    return {
        "refs": [parse_ref(ref) for ref in depends_on],
        "row_count": len(rows),
        "content_hash": hashlib.blake2b(raw.encode(), digest_size=8).hexdigest(),
    }


def _fit_tfidf(
    rows: list[dict[str, Any]], options: dict[str, Any]
) -> tuple[list[str], dict[str, float], list[list[str]]]:
    doc_tokens = [_tokenize_with_options(row["text"], options) for row in rows]
    min_df = int(options.get("min_df", 1))
    max_features = options.get("max_features")
    doc_freq: Counter[str] = Counter()
    for tokens in doc_tokens:
        doc_freq.update(set(tokens))

    terms = [
        term for term, count in doc_freq.items()
        if count >= min_df
    ]
    terms.sort(key=lambda t: (-doc_freq[t], t))
    if max_features is not None:
        terms = terms[: int(max_features)]
    terms.sort()

    n_docs = max(1, len(rows))
    idf_by_term = {
        term: math.log((1 + n_docs) / (1 + doc_freq[term])) + 1
        for term in terms
    }
    return terms, idf_by_term, doc_tokens


def _tokenize_with_options(text: str, options: dict[str, Any]) -> list[str]:
    tokens = _TOKEN_RE.findall(text.lower())
    ngram_range = options.get("ngram_range", [1, 1])
    min_n = int(ngram_range[0])
    max_n = int(ngram_range[1])
    out: list[str] = []
    for n in range(min_n, max_n + 1):
        if n <= 0 or len(tokens) < n:
            continue
        out.extend(" ".join(tokens[i : i + n]) for i in range(len(tokens) - n + 1))
    return out


def _feature_rows(
    rows: list[dict[str, Any]],
    doc_tokens: list[list[str]],
    vocab: list[str],
    idf_by_term: dict[str, float],
    source_name: str,
) -> list[dict[str, Any]]:
    term_index = {term: i for i, term in enumerate(vocab)}
    vocab_set = set(vocab)
    features: list[dict[str, Any]] = []
    for row, tokens in zip(rows, doc_tokens, strict=True):
        counts = Counter(t for t in tokens if t in vocab_set)
        total = sum(counts.values()) or 1
        for term in sorted(counts):
            tf = counts[term] / total
            feature_row: dict[str, Any] = {
                "source_model": source_name,
                "row_index": row["row_index"],
                "row_id": row["row_id"],
                "term": term,
                "term_index": term_index[term],
                "tf": tf,
                "idf": idf_by_term[term],
                "tfidf": tf * idf_by_term[term],
            }
            if "document_id" in row:
                feature_row["document_id"] = row["document_id"]
            if "source_path" in row:
                feature_row["source_path"] = row["source_path"]
            features.append(feature_row)
    return features


def _metadata(
    *,
    model: ModelConfig,
    ml: MLConfig,
    training_input: dict[str, Any],
    vocabulary: list[str],
    idf_by_term: dict[str, float],
) -> dict[str, Any]:
    base = {
        "model_name": model.name,
        "task": ml.task,
        "provider": ml.provider or "builtin.tfidf",
        "mode": ml.mode,
        "training_input": training_input,
        "metrics": {
            "row_count": training_input["row_count"],
            "vocabulary_size": len(vocabulary),
        },
        "files": ["metadata.json", "vocabulary.json"],
        "vocabulary_hash": _hash_json(vocabulary),
        "idf_hash": _hash_json(idf_by_term),
    }
    base["artifact_version"] = _hash_json(base)
    return base


def _write_artifact(
    path: Path,
    metadata: dict[str, Any],
    vocabulary: list[str],
    idf_by_term: dict[str, float],
) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True))
    (path / "vocabulary.json").write_text(
        json.dumps(
            {
                "terms": vocabulary,
                "idf": idf_by_term,
            },
            indent=2,
            sort_keys=True,
        )
    )


def _read_artifact(path: Path) -> tuple[dict[str, Any], list[str], dict[str, float]]:
    metadata_path = path / "metadata.json"
    vocab_path = path / "vocabulary.json"
    if not metadata_path.exists() or not vocab_path.exists():
        raise FileNotFoundError(f"Missing classic ML artifact at {path}")
    metadata = json.loads(metadata_path.read_text())
    vocab_payload = json.loads(vocab_path.read_text())
    return (
        metadata,
        [str(t) for t in vocab_payload["terms"]],
        {str(k): float(v) for k, v in vocab_payload["idf"].items()},
    )


def _empty_feature_df() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "source_model": pl.String,
            "row_index": pl.Int64,
            "row_id": pl.String,
            "term": pl.String,
            "term_index": pl.Int64,
            "tf": pl.Float64,
            "idf": pl.Float64,
            "tfidf": pl.Float64,
        }
    )


def _hash_json(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.blake2b(raw.encode(), digest_size=8).hexdigest()
