from __future__ import annotations

from pathlib import Path

from ..config import load_project
from ..config.model import ModelConfig
from ..dag import ProjectDAG
from ..profile import resolve_profile
from ..state import State
from .schema import TestResult, UnknownTestError, evaluate_test_spec


def run_project_tests(
    project_dir: Path,
    *,
    select: str | None = None,
    exclude: str | None = None,
    target: str | None = None,
    profiles_dir: Path | None = None,
) -> list[TestResult]:
    project, sources, models = load_project(project_dir)
    resolved = resolve_profile(
        project, project_dir, target=target, profiles_dir=profiles_dir
    )
    dag = ProjectDAG(sources, models)
    selected_names = set(dag.select_models(select=select, exclude=exclude))

    db_path = (project_dir / resolved.warehouse.path).resolve()
    results: list[TestResult] = []
    with State(db_path, schema=resolved.warehouse.schema_name) as state:
        for model in models:
            if model.name not in selected_names:
                continue
            if not model.tests:
                continue
            results.extend(run_model_tests(model, state, project_dir=project_dir))
    return results


def run_model_tests(
    model: ModelConfig, state: State, *, project_dir: Path | None = None
) -> list[TestResult]:
    table_ref = f"{state.schema_ref}.{model.name}"
    out: list[TestResult] = []
    for spec in model.tests:
        try:
            out.extend(
                evaluate_test_spec(
                    spec,
                    model_name=model.name,
                    table_ref=table_ref,
                    con=state.connection,
                    project_dir=project_dir,
                )
            )
        except UnknownTestError as e:
            out.append(
                TestResult(
                    test_name=str(spec),
                    model_name=model.name,
                    column=None,
                    status="fail",
                    message=str(e),
                )
            )
    return out
