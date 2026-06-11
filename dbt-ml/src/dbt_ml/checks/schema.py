from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..adapters import WarehouseAdapter
from .python import CustomTestError, run_python_test

SUPPORTED_TESTS = {
    "not_null",
    "unique",
    "min_rows",
    "not_empty",
    "python",
    # deterministic ML/statistical quality checks (issue #10, Tier 1 + grounding)
    "matches_regex",
    "accepted_values",
    "accepted_range",
    "null_rate",
    "grounded_in",
}
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
    adapter: WarehouseAdapter,
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
            _run_named_test(spec, None, model_name, table_ref, adapter, project_dir),
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
            _run_named_test(test_name, arg, model_name, table_ref, adapter, project_dir),
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
    adapter: WarehouseAdapter,
    project_dir: Path | None,
) -> list[TestResult]:
    if test_name == "not_null":
        return _not_null(model_name, table_ref, adapter, arg)
    if test_name == "unique":
        return [_unique(model_name, table_ref, adapter, arg)]
    if test_name == "min_rows":
        return [_min_rows(model_name, table_ref, adapter, int(arg))]
    if test_name == "not_empty":
        return [_min_rows(model_name, table_ref, adapter, 1, display_as="not_empty")]
    if test_name == "python":
        if project_dir is None:
            raise UnknownTestError(
                "Custom python test requires the test runner to know project_dir; "
                "this usually means you're calling evaluate_test_spec directly without it."
            )
        return [_python(model_name, table_ref, adapter, str(arg), project_dir)]
    if test_name == "matches_regex":
        return [_matches_regex(model_name, table_ref, adapter, arg)]
    if test_name == "accepted_values":
        return [_accepted_values(model_name, table_ref, adapter, arg)]
    if test_name == "accepted_range":
        return [_accepted_range(model_name, table_ref, adapter, arg)]
    if test_name == "null_rate":
        return [_null_rate(model_name, table_ref, adapter, arg)]
    if test_name == "grounded_in":
        return [_grounded_in(model_name, table_ref, adapter, arg)]
    raise UnknownTestError(
        f"Unknown test '{test_name}'. Supported: {sorted(SUPPORTED_TESTS)}"
    )


def _not_null(
    model_name: str,
    table_ref: str,
    adapter: WarehouseAdapter,
    arg: Any,
) -> list[TestResult]:
    cols = arg if isinstance(arg, list) else [arg]
    results: list[TestResult] = []
    for col in cols:
        count = adapter.scalar(
            f'SELECT COUNT(*) FROM {table_ref} WHERE "{col}" IS NULL'
        ) or 0
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
    adapter: WarehouseAdapter,
    arg: Any,
) -> TestResult:
    cols = arg if isinstance(arg, list) else [arg]
    col_list = ", ".join(f'"{c}"' for c in cols)
    count = adapter.scalar(
        f"SELECT COUNT(*) FROM ("
        f"  SELECT {col_list} FROM {table_ref}"
        f"  GROUP BY {col_list} HAVING COUNT(*) > 1"
        f")"
    ) or 0
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
    adapter: WarehouseAdapter,
    module_path: str,
    project_dir: Path,
) -> TestResult:
    try:
        message = run_python_test(module_path, project_dir, adapter, table_ref)
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
    adapter: WarehouseAdapter,
    n: int,
    *,
    display_as: str = "min_rows",
) -> TestResult:
    actual = adapter.scalar(f"SELECT COUNT(*) FROM {table_ref}") or 0
    return TestResult(
        test_name=display_as,
        model_name=model_name,
        column=None,
        status="pass" if actual >= n else "fail",
        message=f"actual={actual}, required>={n}",
    )


# ─── deterministic ML / statistical quality checks (issue #10) ─────────────


def _require_dict(test_name: str, arg: Any) -> dict[str, Any]:
    if not isinstance(arg, dict):
        raise UnknownTestError(
            f"Test '{test_name}' expects a mapping of options, got: {arg!r}"
        )
    return arg


def _matches_regex(
    model_name: str, table_ref: str, adapter: WarehouseAdapter, arg: Any
) -> TestResult:
    """Every non-null value of `column` matches `pattern`. Deterministic."""
    import re

    opts = _require_dict("matches_regex", arg)
    column = opts["column"]
    pattern = re.compile(opts["pattern"])

    df = adapter.query_df(f'SELECT "{column}" AS v FROM {table_ref}')
    values = [v for v in df["v"].to_list() if v is not None]
    misses = [str(v) for v in values if not pattern.search(str(v))]
    n = len(misses)
    sample = ", ".join(repr(m) for m in misses[:3])
    return TestResult(
        test_name="matches_regex",
        model_name=model_name,
        column=column,
        status="pass" if n == 0 else "fail",
        message="" if n == 0 else f"{n} values don't match {opts['pattern']!r} (e.g. {sample})",
    )


def _accepted_values(
    model_name: str, table_ref: str, adapter: WarehouseAdapter, arg: Any
) -> TestResult:
    """Every non-null value of `column` is in `values`. Deterministic, SQL aggregate."""
    opts = _require_dict("accepted_values", arg)
    column = opts["column"]
    allowed = opts["values"]
    placeholders = ", ".join(["?"] * len(allowed))
    bad = (
        adapter.scalar(
            f'SELECT COUNT(*) FROM {table_ref} '
            f'WHERE "{column}" IS NOT NULL AND "{column}" NOT IN ({placeholders})',
            list(allowed),
        )
        or 0
    )
    return TestResult(
        test_name="accepted_values",
        model_name=model_name,
        column=column,
        status="pass" if bad == 0 else "fail",
        message="" if bad == 0 else f"{bad} values outside {allowed}",
    )


def _accepted_range(
    model_name: str, table_ref: str, adapter: WarehouseAdapter, arg: Any
) -> TestResult:
    """Numeric `column` within [min, max] (either bound optional). SQL aggregate."""
    opts = _require_dict("accepted_range", arg)
    column = opts["column"]
    conds = []
    params: list[Any] = []
    if "min" in opts:
        conds.append(f'"{column}" < ?')
        params.append(opts["min"])
    if "max" in opts:
        conds.append(f'"{column}" > ?')
        params.append(opts["max"])
    if not conds:
        raise UnknownTestError("accepted_range requires at least one of: min, max")
    where = " OR ".join(conds)
    bad = (
        adapter.scalar(
            f'SELECT COUNT(*) FROM {table_ref} '
            f'WHERE "{column}" IS NOT NULL AND ({where})',
            params,
        )
        or 0
    )
    bounds = f"[{opts.get('min', '-inf')}, {opts.get('max', 'inf')}]"
    return TestResult(
        test_name="accepted_range",
        model_name=model_name,
        column=column,
        status="pass" if bad == 0 else "fail",
        message="" if bad == 0 else f"{bad} values outside {bounds}",
    )


def _null_rate(
    model_name: str, table_ref: str, adapter: WarehouseAdapter, arg: Any
) -> TestResult:
    """Fraction of NULLs in `column` is <= `max`. The #1 silent extraction failure."""
    opts = _require_dict("null_rate", arg)
    column = opts["column"]
    max_rate = float(opts.get("max", 0.0))
    total = adapter.scalar(f"SELECT COUNT(*) FROM {table_ref}") or 0
    if total == 0:
        return TestResult(
            test_name="null_rate", model_name=model_name, column=column,
            status="pass", message="empty table",
        )
    nulls = adapter.scalar(
        f'SELECT COUNT(*) FROM {table_ref} WHERE "{column}" IS NULL'
    ) or 0
    rate = nulls / total
    return TestResult(
        test_name="null_rate",
        model_name=model_name,
        column=column,
        status="pass" if rate <= max_rate else "fail",
        message=f"null_rate={rate:.3f} (max {max_rate:.3f}, {nulls}/{total})",
    )


def _grounded_in(
    model_name: str, table_ref: str, adapter: WarehouseAdapter, arg: Any
) -> TestResult:
    """Deterministic faithfulness proxy: the `value` column's text must appear in
    (or fuzzy-match) the `source` column's text — catching hallucinated values
    with zero LLM calls.

    options: value, source, method ("exact" | "fuzzy"), min_score (fuzzy, default 0.8).
    """
    opts = _require_dict("grounded_in", arg)
    value_col = opts["value"]
    source_col = opts["source"]
    method = opts.get("method", "exact")
    min_score = float(opts.get("min_score", 0.8))

    df = adapter.query_df(
        f'SELECT "{value_col}" AS val, "{source_col}" AS src FROM {table_ref}'
    )
    ungrounded = 0
    checked = 0
    for row in df.iter_rows(named=True):
        val, src = row["val"], row["src"]
        if val is None or src is None or str(val).strip() == "":
            continue
        checked += 1
        v = str(val).lower().strip()
        s = str(src).lower()
        if method == "exact":
            grounded = v in s
        else:  # fuzzy: best partial ratio of val against any window is approximated
            grounded = v in s or _fuzzy_contains(v, s, min_score)
        if not grounded:
            ungrounded += 1

    return TestResult(
        test_name="grounded_in",
        model_name=model_name,
        column=value_col,
        status="pass" if ungrounded == 0 else "fail",
        message=(
            ""
            if ungrounded == 0
            else (
                f"{ungrounded}/{checked} '{value_col}' values not grounded "
                f"in '{source_col}' ({method})"
            )
        ),
    )


def _fuzzy_contains(needle: str, haystack: str, min_score: float) -> bool:
    """True if `needle` approximately appears in `haystack` (stdlib difflib).

    Slides difflib's ratio over haystack windows the size of needle; cheap enough
    for demo/real use, upgradeable to rapidfuzz later.
    """
    import difflib

    if not needle:
        return True
    window = len(needle)
    step = max(1, window // 4)
    best = 0.0
    for i in range(0, max(1, len(haystack) - window + 1), step):
        chunk = haystack[i : i + window]
        score = difflib.SequenceMatcher(None, needle, chunk).ratio()
        if score >= min_score:
            return True
        best = max(best, score)
    return best >= min_score
