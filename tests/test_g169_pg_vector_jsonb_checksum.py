# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G169 (K32): a pgvector column remodelled to jsonb no longer CHECKSUM-mismatches.

PG_UNSUPPORTED_TYPE advises jsonb or text for pgvector's vector / halfvec. Remodelled to
jsonb, Full Load stores the array correctly, but Validation rendered both ends with a plain
``::text`` -- '[1,2,3]' on the source, '[1, 2, 3]' on the jsonb target -- so every non-NULL
row mismatched and the cut-over gate held a correct table. Now such a column renders as
``(CAST(CAST(col AS text) AS jsonb))::text``, the identity on the jsonb target, so one
expression serves both ends.

Live on PostgreSQL 17.11 with pgvector 0.8.7 (throwaway local container): a vector(3) and a
halfvec(3) source table (values incl. 1e-05, 3.4028235e+38, -0 and NULL) against the jsonb
table Full Load writes, through ``build_pg_checksum_sql``: 1757384959684357715 vs
1735470228666420266 (MISMATCH) on the previous commit, 1735470228666420266 on both now.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.models import ColumnDef
from dsql_migrator.core.validation_sql import _checksum_kind, _pg_checksum_expr


@pytest.mark.parametrize("declared", ["vector(3)", "halfvec(3)", "ext.vector(1536)", '"My Ext".halfvec'])
def test_a_vector_remodelled_to_jsonb_is_compared_as_json(declared: str) -> None:
    column = ColumnDef(name="e", mysql_type=declared, target_type="jsonb")
    assert _checksum_kind(column, source_is_postgres=True) == "vector_json"
    rendered = _pg_checksum_expr(column, source_is_postgres=True).as_string(None)
    assert rendered == '(CAST(CAST("e" AS text) AS jsonb))::text'


@pytest.mark.parametrize(
    ("declared", "applied", "postgres"),
    [
        ("vector(3)", "text", True),        # remodelled to text: compared as text, as before
        ("sparsevec(5)", "jsonb", True),    # its text is no JSON array
        ("vector(3)", None, True),          # no applied type known
        ("text", "jsonb", True),
    ],
)
def test_everything_else_is_rendered_as_before(declared, applied, postgres) -> None:  # noqa: ANN001
    column = ColumnDef(name="e", mysql_type=declared, target_type=applied)
    assert _checksum_kind(column, source_is_postgres=postgres) != "vector_json"
