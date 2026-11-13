# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the source-connector value-conversion-failure parser.

The failure these cover is SILENT by construction: the connector logs the column and
delivers NULL, the row lands, and nothing is dead-lettered -- so the parser must NAME the
column, must carry no free text, and must stay strictly disjoint from the dead-letter
parser (a conversion failure read as a quarantined record would inflate the one number a
cut-over decision turns on).
"""

from __future__ import annotations

from dsql_migrator.core.cdc_dlq import parse_dlq_log_message
from dsql_migrator.core.cdc_value_conversion import (
    VALUE_CONVERSION_FILTER_PATTERN,
    CdcValueConversionFailure,
    parse_value_conversion_failure,
)

# The line as the shipped debezium-core-2.7.4.Final.jar emits it (javap of
# io/debezium/relational/TableSchemaBuilder: "Failed to properly convert data value for
# '{}.{}' of type {}" via Loggings.logErrorAndTraceRecord), plus MSK Connect's
# AWS-managed "(logger:line)" tail. Captured live on Aurora PostgreSQL 17.7.
_PG_NAN = (
    "[2026-09-27 03:14:15,926] ERROR Failed to properly convert data value for "
    "'v45arr.arr_ok.vt_nan' of type numeric "
    "(io.debezium.relational.TableSchemaBuilder:279)"
)


def test_parses_the_named_column_and_its_type() -> None:
    failure = parse_value_conversion_failure(_PG_NAN)
    assert failure == CdcValueConversionFailure(
        table="v45arr.arr_ok", column="vt_nan", type_name="numeric"
    )
    # The column is NAMED end to end: it is the whole recovery signal (which table to
    # reload, and which column Validation will flag).
    assert failure.qualified_column == "v45arr.arr_ok.vt_nan"


def test_parses_an_array_type_and_a_multi_word_type() -> None:
    assert (
        parse_value_conversion_failure(
            "ERROR Failed to properly convert data value for 'v45arr.arr_ok.p_bit4' of "
            "type _bit (io.debezium.relational.TableSchemaBuilder:279)"
        ).type_name
        == "_bit"
    )
    # debezium-core is byte-identical in BOTH bundled plugins (md5
    # ac4ad9dd15a7a41b2262e624bb4fb752), so a MySQL source logs the same line -- with a
    # type name that contains spaces. Reporting only "BIGINT" would misname the column's
    # type to the operator.
    assert (
        parse_value_conversion_failure(
            "ERROR Failed to properly convert data value for 'shop.orders.qty' of type "
            "BIGINT UNSIGNED (io.debezium.relational.TableSchemaBuilder:279)"
        ).type_name
        == "BIGINT UNSIGNED"
    )
    assert (
        parse_value_conversion_failure(
            "ERROR Failed to properly convert data value for 'public.t.c' of type "
            "timestamp with time zone (io.debezium.relational.TableSchemaBuilder:279)"
        ).type_name
        == "timestamp with time zone"
    )


def test_the_type_never_swallows_a_following_package_name() -> None:
    # Defensive: if a future worker layout renders the converter exception on the SAME
    # line, the type must still be the type -- not "numeric org".
    failure = parse_value_conversion_failure(
        "ERROR Failed to properly convert data value for 'public.t.c' of type numeric "
        "org.postgresql.util.PSQLException: bad value"
    )
    assert failure.type_name == "numeric"


def test_a_three_part_identifier_keeps_the_db_qualified_table() -> None:
    # A PostgreSQL id can be catalog.schema.table; the table key must stay schema.table
    # so it reads consistently with every other per-table CDC surface.
    failure = parse_value_conversion_failure(
        "ERROR Failed to properly convert data value for 'app.public.orders.total' of "
        "type numeric (io.debezium.relational.TableSchemaBuilder:279)"
    )
    assert (failure.table, failure.column) == ("public.orders", "total")


def test_ignores_unrelated_lines() -> None:
    assert parse_value_conversion_failure(None) is None
    assert parse_value_conversion_failure("") is None
    assert parse_value_conversion_failure("[2026] INFO routine worker line") is None


def test_the_two_connector_log_parsers_are_disjoint() -> None:
    """Neither line class can be read as the other.

    This is the load-bearing property: these rows WERE applied, so if the dead-letter
    parser accepted the line the "Quarantined" count -- the number a cut-over decision
    turns on -- would grow for rows that are sitting on the target.
    """
    dlq_line = (
        "Quarantined record to DLQ (topic=dsqlcdc.shop.orders, partition=0, "
        "offset=42): apply failed (sqlstate=42804)"
    )
    assert parse_dlq_log_message(_PG_NAN) is None
    assert parse_value_conversion_failure(dlq_line) is None


def test_no_free_text_from_the_line_is_carried() -> None:
    """Only identifiers are kept (Property 7).

    The connector's ERROR line is followed by the converter exception, whose rendering can
    quote the offending VALUE. The record has no free-text field, so nothing but the
    table / column / type can reach the UI or the durable activity log.
    """
    failure = parse_value_conversion_failure(
        _PG_NAN + "\njava.lang.NumberFormatException: Character N is neither a digit"
    )
    assert set(failure.model_dump()) == {"table", "column", "type_name"}
    assert "NumberFormatException" not in "".join(failure.model_dump().values())


def test_the_filter_pattern_matches_the_shipped_wording() -> None:
    # The CloudWatch selector and the regex must pick the SAME lines; pinning the phrase
    # here is what stops one drifting from the other.
    assert VALUE_CONVERSION_FILTER_PATTERN == '"Failed to properly convert data value"'
    assert VALUE_CONVERSION_FILTER_PATTERN.strip('"') in _PG_NAN


# ---------------------------------------------------------------------------
# A '.' inside a table name: the streamed names say where the table ends
# ---------------------------------------------------------------------------


def _line(ident: str, type_name: str = "numeric") -> str:
    return (
        f"ERROR Failed to properly convert data value for '{ident}' of type {type_name} "
        "(io.debezium.relational.TableSchemaBuilder:279)"
    )


def _split(ident: str, tables) -> tuple[str, str]:
    failure = parse_value_conversion_failure(_line(ident), tables=tables)
    assert failure is not None
    return failure.table, failure.column


def test_a_mysql_table_name_holding_a_dot_is_named_from_the_streamed_tables() -> None:
    # FAILS WITHOUT THE FIX: MySQL `shop`.`orders.v2` logs shop.orders.v2.qty, which the
    # last-segments rule read as table orders.v2 (a database orders) -- the notice and the
    # activity log then named a table this migration does not have.
    assert _split("shop.orders.v2.qty", {"shop.orders.v2", "shop.items"}) == (
        "shop.orders.v2",
        "qty",
    )


def test_a_postgresql_table_name_holding_a_dot_is_named_with_or_without_a_catalog() -> None:
    # FAILS WITHOUT THE FIX (both). The schema.table id is the shape captured live on
    # PostgreSQL 17.7 (_PG_NAN); an id that also carries the database as its catalog is
    # read after its first '.'.
    assert _split("app.orders.v2.total", ["app.orders.v2"]) == ("app.orders.v2", "total")
    assert _split("testdb.app.orders.v2.col", ["app.orders.v2"]) == ("app.orders.v2", "col")


def test_the_longest_streamed_name_wins_and_the_column_is_the_rest() -> None:
    # FAILS WITHOUT THE FIX: both shop.orders and shop.orders.v2 are streamed, and the line
    # is the dotted table's -- the shorter name would report column "v2.qty" of shop.orders.
    tables = frozenset({"shop.orders", "shop.orders.v2"})
    assert _split("shop.orders.v2.qty", tables) == ("shop.orders.v2", "qty")
    assert _split("shop.orders.qty", tables) == ("shop.orders", "qty")
    # FAILS WITHOUT THE FIX: a '.' inside the COLUMN name stays in the column once the
    # table is known (the last-segments rule read table orders.unit, column price).
    assert _split("shop.orders.unit.price", tables) == ("shop.orders", "unit.price")


def test_a_tie_between_the_two_forms_takes_the_plain_one() -> None:
    # Pins the documented, deterministic choice for an identifier two streamed names of
    # the same length both read: a.b (column c.d) over b.c read after a catalog (column d).
    assert _split("a.b.c.d", {"a.b", "b.c"}) == ("a.b", "c.d")


def test_an_unknown_table_keeps_the_legacy_split() -> None:
    # With no streamed name that reads the line -- none given, or the table is not one of
    # them -- the result is the last-segments rule's, exactly as before ``tables`` existed.
    legacy = parse_value_conversion_failure(_PG_NAN)
    assert legacy == CdcValueConversionFailure(
        table="v45arr.arr_ok", column="vt_nan", type_name="numeric"
    )
    assert parse_value_conversion_failure(_PG_NAN, tables=()) == legacy
    # A name is read only up to a '.': v45arr.arr is a prefix of the text, not of the table.
    assert parse_value_conversion_failure(_PG_NAN, tables=["other.t", "v45arr.arr"]) == legacy
    # A name the identifier only equals (no column after it) does not read it either.
    assert parse_value_conversion_failure(_PG_NAN, tables={"v45arr.arr_ok.vt_nan"}) == legacy
    assert _split("app.public.orders.total", ["public.orders.x"]) == ("public.orders", "total")
