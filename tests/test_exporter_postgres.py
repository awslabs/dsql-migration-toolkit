# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL-source Full Load value conversion (``PostgresValueConverter``).

PG->DSQL is psycopg-native on both ends, so value conversion is pure pass-through;
json/jsonb/interval fidelity is handled upstream on the read (select_column_sql text
cast), not per value.
"""

import datetime
import uuid
from decimal import Decimal

from dsql_migrator.core.exporter_postgres import PostgresValueConverter
from dsql_migrator.core.models import ColumnDef, TableDef


def _table(*names: str) -> TableDef:
    return TableDef(
        name="t",
        columns=[ColumnDef(name=n, mysql_type="text") for n in names] or
        [ColumnDef(name="id", mysql_type="integer")],
        primary_key=["id"] if not names else [names[0]],
    )


def test_convert_row_passes_native_pg_values_through_unchanged() -> None:
    # Every psycopg-native Python type binds straight back to the same PG type on the
    # DSQL target, so the converter must not alter them.
    conv = PostgresValueConverter(
        _table("id", "amount", "flag", "blob", "uid", "ts", "tags", "doc")
    )
    row = {
        "id": 42,
        "amount": Decimal("12.34"),
        "flag": True,
        "blob": b"\x00\x01\x02",
        "uid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "ts": datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        "tags": ["a", "b"],  # array column -> Python list, binds to a PG array as-is
        "doc": '{"a": 1}',  # json/jsonb read as text (select_column_sql) -> passes through
    }
    out = conv.convert_row(row)
    assert out == row
    # A fresh dict (not the same object) so downstream mutation can't corrupt the source row.
    assert out is not row


def test_convert_value_is_identity_including_none() -> None:
    conv = PostgresValueConverter(_table("id"))
    assert conv.convert_value("id", 7) == 7
    assert conv.convert_value("id", None) is None
    assert conv.convert_value("anything", "x") == "x"


def test_convert_row_passes_unknown_columns_through() -> None:
    conv = PostgresValueConverter(_table("id"))
    assert conv.convert_row({"id": 1, "surprise": "kept"}) == {"id": 1, "surprise": "kept"}


def test_target_types_is_accepted_but_does_not_change_passthrough() -> None:
    # target_types is accepted for interface parity with the MySQL converter but no PG
    # type needs a per-value transform, so it must not alter the pass-through behavior.
    conv = PostgresValueConverter(
        _table("id", "doc"), target_types={"doc": "jsonb"}
    )
    assert conv.convert_row({"id": 1, "doc": '{"k": 1}'}) == {"id": 1, "doc": '{"k": 1}'}
