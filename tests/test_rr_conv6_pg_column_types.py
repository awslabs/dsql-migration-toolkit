# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CONV-6 (a): a PostgreSQL source table's column-type notes come from one converter helper.

``SchemaConverter.convert_table`` ran a ~150-line loop over a PostgreSQL table's columns
inline (a converted ENUM, a substituted type, a type Aurora DSQL does not support, a
character or numeric type clamped to DSQL's limits, a bare numeric). It is now the
module-level ``converter._pg_column_type_warnings``, which ``convert_table`` extends its
notes with at the same point. It stays in the converter: converter_postgres holds the rules
it asks, and moving it there would make that module depend back on the converter's notes.

Pinned here: the notes ``convert_table`` gives for one table that reaches every branch (a
golden captured before the extraction, and run green on that code), that they are exactly
the helper's, the identity key's numeric skip, and the structure.

Pure: no source, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import ast
import hashlib
import inspect
import pathlib
import textwrap

from dsql_migrator.core import converter
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import ColumnDef, SourceType, TableDef

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"

_TABLE = TableDef(
    name="app.t",
    columns=[
        ColumnDef(name="id", mysql_type="numeric(1200,0)", nullable=False),
        ColumnDef(
            name="mood", mysql_type="app.mood", type_kind="enum",
            enum_labels=["sad", "ok"], default="'ok'::app.mood",
        ),
        ColumnDef(name="tags", mysql_type="text[]"),
        ColumnDef(name="addr", mysql_type="app.addr", type_kind="composite"),
        ColumnDef(name="vec", mysql_type="vector(3)"),
        ColumnDef(name="big", mysql_type="character varying(70000)"),
        ColumnDef(name="amt", mysql_type="numeric(1200,2)"),
        ColumnDef(name="bare", mysql_type="numeric"),
        ColumnDef(name="plain", mysql_type="integer"),
    ],
    primary_key=["id"],
)

# (column, source type, target type, classification, kind, sha256(message)[:16]), in order.
_GOLDEN = [
    ("id", "numeric(1200,0)", "numeric(1000,0)", "MANUAL", "LOSS", "f64cd376832ea1d5"),
    ("mood", "app.mood", "text", "MANUAL", "LOSS", "35899211bafc1435"),
    ("tags", "text[]", "jsonb", "MANUAL", "LOSS", "63079fc40029cea2"),
    ("addr", "app.addr", "app.addr", "UNSUPPORTED", "LOSS", "3089dcc745be433b"),
    ("vec", "vector(3)", "vector(3)", "UNSUPPORTED", "LOSS", "5f2f2e69f377eebc"),
    ("big", "character varying(70000)", "text", "MANUAL", "LOSS", "b2cbb47cbde93545"),
    ("amt", "numeric(1200,2)", "numeric(1000,2)", "MANUAL", "LOSS", "8685100dfa11632b"),
    ("bare", "numeric", "numeric(18,6)", "MANUAL", "LOSS", "d62bd2fce906b64b"),
]


def _shape(warnings) -> list:
    return [
        (
            w.column_name, w.source_type, w.target_type, w.classification.name,
            w.kind.name if w.kind else None,
            hashlib.sha256(w.message.encode()).hexdigest()[:16],
        )
        for w in warnings
    ]


def _converted():
    return SchemaConverter(source_type=SourceType.POSTGRES).convert_table(_TABLE)


def test_convert_tables_notes_are_unchanged() -> None:
    assert _shape(_converted().warnings) == _GOLDEN


def test_they_are_the_helpers_notes() -> None:
    notes = converter._pg_column_type_warnings(_TABLE, identity_key=None)
    assert [w.model_dump() for w in notes] == [w.model_dump() for w in _converted().warnings]


def test_the_identity_key_gets_no_numeric_note() -> None:
    notes = converter._pg_column_type_warnings(_TABLE, identity_key="id")
    assert _shape(notes) == _GOLDEN[1:]


def test_convert_table_extends_its_notes_with_the_helper() -> None:
    source = textwrap.dedent(inspect.getsource(SchemaConverter.convert_table))
    calls = [
        ast.unparse(node)
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call) and "_pg_column_type_warnings" in ast.unparse(node.func)
    ]
    assert calls == ["_pg_column_type_warnings(table, identity_key=identity_key)"]
    assert "unsupported_dsql_reason" not in source and "clamp_pg_character" not in source


def test_the_helper_stays_in_the_converter() -> None:
    assert converter._pg_column_type_warnings.__module__ == "dsql_migrator.core.converter"
    text = (_SRC / "core" / "converter_postgres.py").read_text(encoding="utf-8")
    assert "_pg_column_type_warnings" not in text
