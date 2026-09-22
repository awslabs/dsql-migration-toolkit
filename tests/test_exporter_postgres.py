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


def test_a_domain_column_is_read_through_its_base_type_not_the_domain_name() -> None:
    """A DOMAIN over jsonb/interval must still take the text-cast read path.

    ``format_type`` reports a domain as the DOMAIN's own name, so the text-cast rule --
    which matches ``json``/``jsonb``/``interval`` -- found nothing and the column was read
    NATIVELY. PostgreSQL still describes the result with the BASE type's OID, so psycopg
    applied the base loader and produced exactly the loss the cast exists to prevent.
    Live-confirmed on PostgreSQL 16: a domain over jsonb turned the JSON literal ``null``
    into Python ``None`` (SQL NULL on the target), and a domain over interval turned
    ``2 years 3 mons`` into ``820 days`` -- silently, with the table reported DONE.
    """
    from dsql_migrator.core.models import ColumnDef
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    dialect = PostgresSourceDialect()

    # A domain: the declared type says nothing, the BASE type decides.
    for base in ("jsonb", "json", "interval", "interval day to second"):
        column = ColumnDef(
            name="v", mysql_type="app.mydomain", nullable=True, base_type=base
        )
        assert dialect.select_column_sql(column) == 'CAST("v" AS text) AS "v"', base

    # A domain over a type that needs no cast stays native.
    native = ColumnDef(name="v", mysql_type="app.shortstr", nullable=True, base_type="text")
    assert dialect.select_column_sql(native) == '"v"'

    # Unchanged for a plain column (base_type is None).
    assert dialect.select_column_sql(
        ColumnDef(name="v", mysql_type="jsonb", nullable=True)
    ) == 'CAST("v" AS text) AS "v"'
    assert dialect.select_column_sql(
        ColumnDef(name="v", mysql_type="text", nullable=True)
    ) == '"v"'


def test_the_read_follows_the_applied_target_type_for_a_remodelled_column() -> None:
    """The tool's own remodel advice must be implementable on the data path.

    ``unsupported_dsql_reason`` tells the operator exactly what to remodel a
    DSQL-unsupported PostgreSQL type to, Schema Conversion lets them do it, and the loader
    even parses the applied DDL's types -- but the PostgreSQL value converter DISCARDED
    them, so every value still arrived in the SOURCE type. Live-confirmed on PostgreSQL 16:
    money -> numeric was rejected 22P02 (psycopg returns ``'$12.34'``), text[] -> jsonb was
    rejected 22P02, and bit -> bytea silently stored the ASCII digits of ``'10101010'``.
    """
    from dsql_migrator.core.models import ColumnDef
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    dialect = PostgresSourceDialect()

    def read(source: str, target: str | None) -> str:
        return dialect.select_column_sql(
            ColumnDef(name="v", mysql_type=source, nullable=True), target_type=target
        )

    # A textual target takes the value's canonical source text.
    for source in ("inet", "cidr", "xml", "tsvector", "point", "bit(8)", "macaddr"):
        assert read(source, "text") == 'CAST("v" AS text) AS "v"', source
    assert read("inet", "character varying(64)") == 'CAST("v" AS text) AS "v"'

    # money -> numeric must cast on the SOURCE: its psycopg text is locale-formatted.
    assert read("money", "numeric") == 'CAST("v" AS numeric) AS "v"'
    assert read("money", "numeric(12,2)") == 'CAST("v" AS numeric) AS "v"'

    # An array -> jsonb needs real JSON, not the array's own '{a,b}' text.
    assert read("text[]", "jsonb") == 'CAST(to_jsonb("v") AS text) AS "v"'
    assert read("integer[]", "jsonb") == 'CAST(to_jsonb("v") AS text) AS "v"'

    # Unchanged when the target matches the source, or when no target is known.
    assert read("text", "text") == '"v"'
    assert read("inet", None) == '"v"'
    # ...and the source-driven json/interval rule still applies underneath.
    assert read("jsonb", "jsonb") == 'CAST("v" AS text) AS "v"'


def test_bit_is_no_longer_advised_to_bytea_because_the_loader_cannot_produce_it() -> None:
    """Advice the data path cannot honour is worse than no advice.

    A bit string is read as its text, so binding it to bytea stored the ASCII bytes of
    ``"10101010"`` rather than the byte ``0xAA`` -- silent corruption behind a green DONE,
    catchable only by a CHECKSUM (not ROW_COUNT) validation.
    """
    from dsql_migrator.core.converter_postgres import _PG_UNSUPPORTED_REMODEL

    for source in ("bit", "bit varying", "varbit"):
        advice = _PG_UNSUPPORTED_REMODEL[source]
        assert "bytea" not in advice, f"{source} still advises an unproducible bytea"
        assert "text" in advice


def test_the_exporter_hands_the_applied_target_types_to_the_read_expression() -> None:
    """Pins the WIRING, not just the rule.

    The per-column rule and the exporter that feeds it are separate failures: asserting
    only on ``select_column_sql`` left the plumbing untested, so deleting the exporter's
    ``target_type=`` argument reintroduced the whole defect with the suite still green.
    Drives the real ``keyset_stream`` against a recording connection and reads the SELECT
    list it actually built.
    """
    from dsql_migrator.core.exporter import keyset_stream
    from dsql_migrator.core.models import ColumnDef, SourceType, TableDef
    from dsql_migrator.core.source_dialect import dialect_for

    table = TableDef(
        name="app.t",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="amount", mysql_type="money", nullable=True),
            ColumnDef(name="tags", mysql_type="text[]", nullable=True),
            ColumnDef(name="addr", mysql_type="inet", nullable=True),
        ],
        primary_key=["id"],
        indexes=[],
    )

    statements: list[str] = []

    class _Result:
        def mappings(self):
            return iter(())   # an empty first page ends the keyset loop at once

    class _Connection:
        def execute(self, statement, *args, **kwargs):
            statements.append(str(statement))
            return _Result()

    list(
        keyset_stream(
            _Connection(), table,
            dialect=dialect_for(SourceType.POSTGRES),
            target_types={"amount": "numeric", "tags": "jsonb", "addr": "text"},
        )
    )
    assert statements, "keyset_stream issued no statement"
    select_sql = statements[0]
    assert 'CAST("amount" AS numeric)' in select_sql, select_sql
    assert 'CAST(to_jsonb("tags") AS text)' in select_sql, select_sql
    assert 'CAST("addr" AS text)' in select_sql, select_sql


def test_the_live_streaming_path_threads_the_target_types_into_keyset_stream() -> None:
    """The production read path must pass them on, not just ``keyset_stream``'s API.

    ``stream_converted_rows`` is what Full Load actually calls. Removing its
    ``target_types=`` argument left the whole remodel fix dead in production while a test
    that calls ``keyset_stream`` directly stayed green -- so pin the hand-off itself.
    """
    import inspect

    from dsql_migrator.core.exporter import TableExporter

    body = inspect.getsource(TableExporter.stream_converted_rows)
    # Scoped to the keyset_stream CALL: the same function also passes target_types to the
    # value converter, so a whole-body search matches that and survives the hand-off being
    # deleted -- the exact false pass this test exists to prevent.
    call_start = body.index("keyset_stream(")
    call = body[call_start : body.index(")", body.index("dialect=dialect", call_start))]
    assert "target_types=target_types" in call, (
        "stream_converted_rows no longer hands the applied target types to keyset_stream, "
        f"so a remodelled column is read in its SOURCE type again:\n{call}"
    )
