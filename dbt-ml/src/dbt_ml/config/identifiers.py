"""Node-name validation shared by model and source configs.

Names become warehouse identifiers (table names, document_id scopes, dbt
export names), so they are restricted to a conservative charset up front.
SQL-side safety is handled separately by adapter quoting; this exists so a
typo'd or hostile name fails at config load with a clear message instead of
surfacing as a warehouse error mid-run.
"""
from __future__ import annotations

import re

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Tables dbt-ml manages internally (state, staging views, test-failure tables)
# all live under this prefix; user models must stay out of it.
RESERVED_PREFIX = "dbt_ml_"


def validate_node_name(name: str, *, kind: str, reserve_internal: bool = False) -> str:
    if not _IDENT_RE.match(name):
        raise ValueError(
            f"{kind} name {name!r} is invalid: names must start with a letter or "
            "underscore and contain only letters, digits, and underscores"
        )
    if reserve_internal and name.lower().startswith(RESERVED_PREFIX):
        raise ValueError(
            f"{kind} name {name!r} is invalid: the '{RESERVED_PREFIX}' prefix is "
            "reserved for dbt-ml internal tables"
        )
    return name
