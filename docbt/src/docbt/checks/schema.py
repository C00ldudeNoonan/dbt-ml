from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from .python import CustomTestError, run_python_test

SUPPORTED_TESTS = {"not_null", "unique", "min_rows", "not_empty", "python"}
SUPPORTED_SEVERITIES = {"error", "warn"}


class UnknownTestError(Exception):
    pass


@dataclass
class TestResult:
    __test__ = False  # tell pytest not to collect this dataclass as a test class

    test_name: str
    model_name: str
    column: str | None
    status: str  # "pass" | "warn" | "fail"
    message: str = ""
    severity: str = "error"

    @property
    def passed(self) -> bool:
        return self.status == "pass"

    @property
    def is_hard_failure(self) -> bool:
        """True when this result should cause the run to exit non-zero."""
        return self.status == "fail"


def evaluate_test_spec(
    spec: Any,
    *,
    model_name: str,
    table_ref: str,
    con: duckdb.DuckDBPyConnection,
    project_dir: Path | None = None,
) -> list[TestResult]:
    """Parse one test spec and run it.

    Accepted forms:
        "not_empty"                                  -> bare test name
        {not_null: [a, b]}                           -> single-key mapping
        {not_null: [a, b], severity: warn}           -> with severity sibling key
        {python: my.module.path}                     -> custom Python test
    """
    if isinstance(spec, str):
        return _apply_severity(
            _run_named_test(spec, None, model_name, table_ref, con, project_dir),
            "error",
        )
    if isinstance(spec, dict):
        body = dict(spec)
        severity = body.pop("severity", "error")
        if severity not in SUPPORTED_SEVERITIES:
            raise UnknownTestError(
                f"Unknown severity '{severity}'. Allowed: {sorted(SUPPORTED_SEVERITIES)}"
            )
        if len(body) != 1:
            raise UnknownTestError(
                f"Test spec must have exactly one test key (plus optional severity), got: {spec!r}"
            )
        ((test_name, arg),) = body.items()
        return _apply_severity(
            _run_named_test(test_name, arg, model_name, table_ref, con, project_dir),
            severity,
        )
    raise UnknownTestError(f"Unsupported test spec: {spec!r}")


def _apply_severity(results: list[TestResult], severity: str) -> list[TestResult]:
    out: list[TestResult] = []
    for r in results:
        new_status = r.status
        if r.status == "fail" and severity == "warn":
            new_status = "warn"
        out.append(
            TestResult(
                test_name=r.test_name,
                model_name=r.model_name,
                column=r.column,
                status=new_status,
                message=r.message,
                severity=severity,
            )
        )
    return out


def _run_named_test(
    test_name: str,
    arg: Any,
    model_name: str,
    table_ref: str,
    con: duckdb.DuckDBPyConnection,
    project_dir: Path | None,
) -> list[TestResult]:
    if test_name == "not_null":
        return _not_null(model_name, table_ref, con, arg)
    if test_name == "unique":
        return [_unique(model_name, table_ref, con, arg)]
    if test_name == "min_rows":
        return [_min_rows(model_name, table_ref, con, int(arg))]
    if test_name == "not_empty":
        return [_min_rows(model_name, table_ref, con, 1, display_as="not_empty")]
    if test_name == "python":
        if project_dir is None:
            raise UnknownTestError(
                "Custom python test requires the test runner to know project_dir; "
                "this usually means you're calling evaluate_test_spec directly without it."
            )
        return [_python(model_name, table_ref, con, str(arg), project_dir)]
    raise UnknownTestError(
        f"Unknown test '{test_name}'. Supported: {sorted(SUPPORTED_TESTS)}"
    )


def _not_null(
    model_name: str,
    table_ref: str,
    con: duckdb.DuckDBPyConnection,
    arg: Any,
) -> list[TestResult]:
    cols = arg if isinstance(arg, list) else [arg]
    results: list[TestResult] = []
    for col in cols:
        row = con.execute(
            f'SELECT COUNT(*) FROM {table_ref} WHERE "{col}" IS NULL'
        ).fetchone()
        count = row[0] if row else 0
        results.append(
            TestResult(
                test_name="not_null",
                model_name=model_name,
                column=col,
                status="pass" if count == 0 else "fail",
                message="" if count == 0 else f"{count} rows have NULL {col}",
            )
        )
    return results


def _unique(
    model_name: str,
    table_ref: str,
    con: duckdb.DuckDBPyConnection,
    arg: Any,
) -> TestResult:
    cols = arg if isinstance(arg, list) else [arg]
    col_list = ", ".join(f'"{c}"' for c in cols)
    row = con.execute(
        f"SELECT COUNT(*) FROM ("
        f"  SELECT {col_list} FROM {table_ref}"
        f"  GROUP BY {col_list} HAVING COUNT(*) > 1"
        f")"
    ).fetchone()
    count = row[0] if row else 0
    return TestResult(
        test_name="unique",
        model_name=model_name,
        column=",".join(cols),
        status="pass" if count == 0 else "fail",
        message="" if count == 0 else f"{count} duplicate groups on {cols}",
    )


def _python(
    model_name: str,
    table_ref: str,
    con: duckdb.DuckDBPyConnection,
    module_path: str,
    project_dir: Path,
) -> TestResult:
    try:
        message = run_python_test(module_path, project_dir, con, table_ref)
    except CustomTestError as e:
        return TestResult(
            test_name=f"python:{module_path}",
            model_name=model_name,
            column=None,
            status="fail",
            message=str(e),
        )
    return TestResult(
        test_name=f"python:{module_path}",
        model_name=model_name,
        column=None,
        status="pass" if message is None else "fail",
        message=message or "",
    )


def _min_rows(
    model_name: str,
    table_ref: str,
    con: duckdb.DuckDBPyConnection,
    n: int,
    *,
    display_as: str = "min_rows",
) -> TestResult:
    row = con.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()
    actual = row[0] if row else 0
    return TestResult(
        test_name=display_as,
        model_name=model_name,
        column=None,
        status="pass" if actual >= n else "fail",
        message=f"actual={actual}, required>={n}",
    )
