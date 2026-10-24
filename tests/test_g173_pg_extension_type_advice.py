# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G173: PG_UNSUPPORTED_TYPE advises pgvector's types by name, whatever their schema.

``format_type`` qualifies a type whose schema is not on the search path, so a vector column
of an extension installed in schema ``ext`` reads ``ext.vector(3)``, and the remodel advice
(jsonb or text) was looked up by the qualified name: it fell to the advice for a
user-defined type ("Aurora DSQL has no CREATE TYPE ..."), while CDC's grading already
ignored the schema (cdc_drops_extension_type). halfvec and sparsevec, pgvector's other
types, had no advice of their own either. Now the three are advised by name -- halfvec as
vector, sparsevec as text (its text is no JSON array) -- unless the column is of a
user-defined kind that happens to share the name.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.converter_postgres import unsupported_dsql_reason

_JSONB_OR_TEXT = "Store it as jsonb or text (pgvector is an extension Aurora DSQL does not provide)"


@pytest.mark.parametrize("declared", ["vector(3)", "ext.vector(3)", '"My Ext".vector(1536)',
                                      "halfvec(3)", "public.halfvec(8)"])
def test_vector_and_halfvec_get_the_jsonb_or_text_advice(declared: str) -> None:
    assert _JSONB_OR_TEXT in unsupported_dsql_reason(declared)


def test_sparsevec_gets_text_only() -> None:
    reason = unsupported_dsql_reason("ext.sparsevec(5)")
    assert "Store it as text (pgvector is an extension" in reason and "jsonb" not in reason


def test_a_user_defined_kind_of_that_name_keeps_its_own_advice() -> None:
    assert _JSONB_OR_TEXT not in unsupported_dsql_reason("app.vector", type_kind="composite")
    # A user's type named like a built-in one is not taken for it.
    assert "If it is a user-defined type" in unsupported_dsql_reason("app.inet")
