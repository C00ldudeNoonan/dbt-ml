from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .config.model import ExtractionConfig, MLConfig, TransformConfig


def compute_content_hash(path: Path) -> str:
    return hashlib.blake2b(path.read_bytes(), digest_size=8).hexdigest()


def compute_document_id(scope: str, relative_path: str) -> str:
    return hashlib.blake2b(f"{scope}:{relative_path}".encode(), digest_size=8).hexdigest()


def compute_code_version(
    *,
    extraction: ExtractionConfig | None,
    transform: TransformConfig | None,
    ml: MLConfig | None = None,
    project_dir: Path,
) -> str:
    payload: dict[str, Any] = {
        "extraction": extraction.model_dump() if extraction else None,
        "transform": transform.model_dump() if transform else None,
        "ml": ml.model_dump(mode="json") if ml else None,
    }
    if transform and transform.module:
        module_file = resolve_module_file(transform.module, project_dir)
        if module_file.exists():
            payload["transform_code_hash"] = hashlib.blake2b(
                module_file.read_bytes(), digest_size=8
            ).hexdigest()
        else:
            payload["transform_code_hash"] = "missing"

    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.blake2b(canonical.encode(), digest_size=8).hexdigest()


def resolve_module_file(module: str, project_dir: Path) -> Path:
    """Resolve a dotted module path (e.g. 'transforms.summarize') to a .py file
    relative to the project directory."""
    parts = module.split(".")
    return project_dir / Path(*parts).with_suffix(".py")
