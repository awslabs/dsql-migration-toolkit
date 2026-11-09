# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL's (and Aurora DSQL's) identifier rules: how an UNQUOTED name folds, and how
the catalog stores a name (cut to NAMEDATALEN-1 bytes).

Standard library only, so any module -- the models, the converters, the assessor, CDC,
Schema Apply -- imports it at its top level. :mod:`dsql_migrator.core.target_introspector`
re-exports every name (the E2E harness and ``scripts/`` import them from there).
"""

from __future__ import annotations


# PostgreSQL (and Aurora DSQL) stores an identifier cut to NAMEDATALEN-1 bytes.
_PG_NAME_MAX_BYTES = 63


def pg_fold_unquoted(name: str) -> str:
    """``name`` as PostgreSQL folds an UNQUOTED identifier: ASCII letters to lower case."""
    return "".join(ch.lower() if "A" <= ch <= "Z" else ch for ch in name)


def pg_stored_name(name: str) -> str:
    """``name`` as the catalog stores it: UTF-8 cut to 63 bytes on a character boundary
    (PostgreSQL's pg_mbcliplen). A DDL or DML naming the long form reaches this column."""
    raw = name.encode("utf-8")
    if len(raw) <= _PG_NAME_MAX_BYTES:
        return name
    return raw[:_PG_NAME_MAX_BYTES].decode("utf-8", errors="ignore")


__all__ = ["pg_fold_unquoted", "pg_stored_name"]
