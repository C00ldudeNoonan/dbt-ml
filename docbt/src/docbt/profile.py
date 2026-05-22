"""Discovery + resolution of profiles.yml.

Lookup order for the profiles file:
  1. The directory passed via `--profiles-dir` (CLI flag).
  2. The directory named by the `DOCBT_PROFILES_DIR` env var.
  3. `<project_dir>/profiles.yml` (project-local; docbt addition for portability).
  4. `~/.docbt/profiles.yml` (dbt-style user-global location).

First hit wins.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from .config.profile import LLMConfig, ProfileConfig, WarehouseConfig
from .config.project import DuckDBConfig, ProjectConfig

PROFILES_FILENAME = "profiles.yml"


class ProfileError(Exception):
    pass


@dataclass(frozen=True)
class ResolvedProfile:
    """The single source of truth for warehouse + LLM config during a run."""

    profile_name: str
    target_name: str
    warehouse: WarehouseConfig
    llm: LLMConfig | None
    profiles_path: Path | None  # None when using inline-legacy fallback


def resolve_profile(
    project: ProjectConfig,
    project_dir: Path,
    *,
    target: str | None = None,
    profiles_dir: Path | None = None,
) -> ResolvedProfile:
    """Resolve the active profile + target for this invocation.

    Falls back to the legacy inline `duckdb:` block when no `profile:` is set
    in the project file. Raises `ProfileError` on any structured problem.
    """
    if not project.profile:
        return _legacy_resolved(project)

    profiles_path = _discover_profiles_file(project_dir, profiles_dir)
    if profiles_path is None:
        raise ProfileError(
            f"Project '{project.name}' references profile '{project.profile}' "
            f"but no profiles.yml was found. Looked in: "
            f"--profiles-dir, $DOCBT_PROFILES_DIR, {project_dir}/profiles.yml, "
            f"~/.docbt/profiles.yml."
        )

    profiles = _load_profiles_file(profiles_path)
    if project.profile not in profiles:
        raise ProfileError(
            f"Profile '{project.profile}' not in {profiles_path}. "
            f"Available: {sorted(profiles)}"
        )

    profile = profiles[project.profile]
    target_name = target or profile.target
    if target_name not in profile.outputs:
        raise ProfileError(
            f"Target '{target_name}' not in profile '{project.profile}' "
            f"({profiles_path}). Available: {sorted(profile.outputs)}"
        )

    selected = profile.outputs[target_name]
    return ResolvedProfile(
        profile_name=project.profile,
        target_name=target_name,
        warehouse=_absolutize_warehouse(selected.warehouse, project_dir),
        llm=_absolutize_llm(selected.llm, project_dir),
        profiles_path=profiles_path,
    )


def _absolutize_warehouse(wh: WarehouseConfig, project_dir: Path) -> WarehouseConfig:
    return WarehouseConfig(
        type=wh.type,
        path=(project_dir / wh.path).resolve(),
        schema=wh.schema_name,
    )


def _absolutize_llm(llm: LLMConfig | None, project_dir: Path) -> LLMConfig | None:
    if llm is None:
        return None
    cache_path: Path | None = None
    if llm.cache_path is not None:
        cache_path = (project_dir / llm.cache_path).resolve()
    return LLMConfig(
        provider=llm.provider,
        model=llm.model,
        api_key_env=llm.api_key_env,
        cache_path=cache_path,
        system_prompt=llm.system_prompt,
    )


def _legacy_resolved(project: ProjectConfig) -> ResolvedProfile:
    duckdb: DuckDBConfig = project.duckdb
    warehouse = WarehouseConfig(
        type="duckdb",
        path=duckdb.path,
        schema=duckdb.schema_name,
    )
    return ResolvedProfile(
        profile_name="<inline>",
        target_name="<inline>",
        warehouse=warehouse,
        llm=None,
        profiles_path=None,
    )


def _discover_profiles_file(
    project_dir: Path, profiles_dir: Path | None
) -> Path | None:
    candidates: list[Path] = []
    if profiles_dir is not None:
        candidates.append(profiles_dir / PROFILES_FILENAME)
    env_dir = os.environ.get("DOCBT_PROFILES_DIR")
    if env_dir:
        candidates.append(Path(env_dir) / PROFILES_FILENAME)
    candidates.append(project_dir / PROFILES_FILENAME)
    candidates.append(Path.home() / ".docbt" / PROFILES_FILENAME)

    for p in candidates:
        if p.exists():
            return p
    return None


def _load_profiles_file(path: Path) -> dict[str, ProfileConfig]:
    with path.open() as f:
        data: Any = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ProfileError(f"{path}: top-level must be a mapping of profile names")

    out: dict[str, ProfileConfig] = {}
    for name, body in data.items():
        try:
            out[name] = ProfileConfig.model_validate(body)
        except ValidationError as e:
            raise ProfileError(f"{path}: profile '{name}' invalid:\n{e}") from e
    return out


def resolve_llm_options(
    options: dict[str, Any], resolved: ResolvedProfile
) -> dict[str, Any]:
    """Merge profile.llm defaults into model-level extraction options.

    Model-level options always win; profile.llm fills in what's missing.
    """
    if resolved.llm is None:
        return options

    merged = dict(options)
    merged.setdefault("model", resolved.llm.model)
    if resolved.llm.cache_path is not None:
        merged.setdefault("cache_path", str(resolved.llm.cache_path))
    if resolved.llm.system_prompt is not None:
        merged.setdefault("system_prompt", resolved.llm.system_prompt)
    return merged
