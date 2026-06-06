"""Fusion-compatibility checks for the emitted dbt sources.yml.

dbt Fusion parses far more strictly than dbt-core — it fails (not warns) on
undeclared macros and malformed properties. These tests encode the rules we
must hold to so the artifact stays Fusion-parseable, plus the packages.yml /
warning machinery that keeps macro tests from breaking that parse.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from docbt.dbt_export import (
    DBT_UTILS_PACKAGE,
    build_dbt_packages,
    build_dbt_sources,
    requires_dbt_utils,
    write_dbt_sources,
)

# Source-test references the emitter is allowed to produce. Anything else would
# be an undeclared macro under Fusion.
_ALLOWED_TEST_NAMES = {"not_null", "unique"}
_ALLOWED_MACRO_PREFIX = "dbt_utils."


def _write_project(tmp_path: Path, *, tests_block: str) -> Path:
    project_dir = tmp_path / "p"
    project_dir.mkdir()
    (project_dir / "docbt_project.yml").write_text(
        "name: p\nduckdb:\n  path: ./target/p.duckdb\n  schema: p\n"
    )
    (project_dir / "models").mkdir()
    (project_dir / "models" / "x.yml").write_text(
        "version: 2\nmodels:\n"
        "  - name: x\n"
        "    extraction:\n      backend: json\n"
        "    source: ref('s')\n"
        "    fields:\n      - name: a\n      - name: b\n"
        f"{tests_block}"
    )
    (project_dir / "sources").mkdir()
    (project_dir / "sources" / "s.yml").write_text(
        "version: 2\nsources:\n  - name: s\n    path: ./data/\n"
    )
    return project_dir


def _all_test_refs(payload: dict) -> list[str]:
    refs: list[str] = []
    for source in payload["sources"]:
        for table in source["tables"]:
            for col in table.get("columns", []):
                refs.extend(col.get("tests", []))
            for spec in table.get("tests", []):
                refs.append(next(iter(spec)) if isinstance(spec, dict) else spec)
    return refs


def test_only_known_test_names_are_emitted(example_project_dir: Path) -> None:
    payload = build_dbt_sources(example_project_dir)
    for ref in _all_test_refs(payload):
        assert ref in _ALLOWED_TEST_NAMES or ref.startswith(_ALLOWED_MACRO_PREFIX), (
            f"unexpected test ref {ref!r} would fail dbt Fusion parse"
        )


def test_source_has_required_string_fields(example_project_dir: Path) -> None:
    src = build_dbt_sources(example_project_dir)["sources"][0]
    for key in ("name", "database", "schema"):
        assert isinstance(src[key], str) and src[key], f"{key} must be a non-empty str"


def test_no_null_values_anywhere(example_project_dir: Path) -> None:
    # Fusion rejects explicit nulls in properties; yaml.safe_dump of a None
    # would surface as `null`. Round-trip and assert none leaked in.
    dumped = yaml.safe_dump(build_dbt_sources(example_project_dir))
    assert ": null" not in dumped and "- null" not in dumped


def test_untranslatable_tests_warn_instead_of_silent_drop(tmp_path: Path) -> None:
    project_dir = _write_project(
        tmp_path, tests_block="    tests:\n      - min_rows: 5\n      - not_empty: a\n"
    )
    warnings: list[str] = []
    payload = build_dbt_sources(project_dir, warnings=warnings)

    assert "min_rows" not in yaml.safe_dump(payload)
    assert any("min_rows" in w for w in warnings)
    assert any("not_empty" in w for w in warnings)


def test_composite_unique_requires_dbt_utils(tmp_path: Path) -> None:
    project_dir = _write_project(
        tmp_path, tests_block="    tests:\n      - unique: [a, b]\n"
    )
    payload = build_dbt_sources(project_dir)
    assert requires_dbt_utils(payload) is True


def test_single_column_tests_do_not_require_dbt_utils(
    example_project_dir: Path,
) -> None:
    payload = build_dbt_sources(example_project_dir)
    assert requires_dbt_utils(payload) is False


def test_emit_packages_writes_packages_yml(tmp_path: Path) -> None:
    project_dir = _write_project(
        tmp_path, tests_block="    tests:\n      - unique: [a, b]\n"
    )
    out = tmp_path / "out" / "_docbt_sources.yml"
    write_dbt_sources(project_dir, output=out, emit_packages=True)

    packages = out.parent / "packages.yml"
    assert packages.exists()
    parsed = yaml.safe_load(packages.read_text())
    assert parsed["packages"][0]["package"] == DBT_UTILS_PACKAGE


def test_missing_packages_warns_when_macro_test_emitted(tmp_path: Path) -> None:
    project_dir = _write_project(
        tmp_path, tests_block="    tests:\n      - unique: [a, b]\n"
    )
    out = tmp_path / "out" / "_docbt_sources.yml"
    warnings: list[str] = []
    write_dbt_sources(project_dir, output=out, emit_packages=False, warnings=warnings)

    assert not (out.parent / "packages.yml").exists()
    assert any("packages.yml" in w for w in warnings)


def test_build_dbt_packages_shape() -> None:
    payload = build_dbt_packages()
    pkg = payload["packages"][0]
    assert pkg["package"] == DBT_UTILS_PACKAGE
    assert isinstance(pkg["version"], list) and pkg["version"]
