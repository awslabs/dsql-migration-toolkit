# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G141: follow-ups to G139 / G140.

- A one-column key's page boundary comes back as text and is bound back untyped, so the
  server parses it in the column's own type. Bound natively, a ``real`` key (a MySQL FLOAT)
  went back as psycopg's float of its shortest decimal -- a float8 below the stored float4
  for some values -- and the next page read that row again: one row per such boundary,
  which an equal number of missing target rows hid (a false ROW_COUNT MATCH). Aurora DSQL's
  count, checksum and orphan pages, a PostgreSQL source's through the shim and the cut-over
  orphan count; a source's one-column checksum pages are held to its live count.
  Reconciliation reads such a key's text as the integer the source holds: a boolean key (a
  MySQL TINYINT(1) on Aurora DSQL) as 0 / 1.
- A MySQL TIME / BIT key is paged again, with the exact cursor values (G140 checksummed it
  in one statement, with no page beat or Stop poll); only a single-precision FLOAT key keeps
  the one statement, which now beats before and after it.
- A table not matched ONLY because its orphan check could not run is its own case: no
  backfill runbook, and readiness and the Result badge say "orphan check could not run",
  never a data mismatch; its notice names the checksums only when they ran.
- A session the server ends (57P01 / 57P02 / 57P03) is a lost connection on the target read
  path (the cut-over count's cases are in test_g139_resumable_orphan_count.py).

Doubles only: no database, no AWS.
"""

from __future__ import annotations

import struct
from typing import Any

import pytest
from psycopg import sql

import dsql_migrator.core.validator as v
import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.models import (
    ForeignKeyDef,
    ReconcileResult,
    TableValidationResult,
    ValidationMode,
    ValidationReport,
)
from dsql_migrator.core.validator import PagedReadError, ValidationCancelled, render_text_report
from dsql_migrator.ui.data_migration._models import MigrationType
from tests.test_g138_composite_validation import (
    _MY,
    _PG,
    _PG_SOURCE,
    _SOURCE,
    _TARGET,
    MySqlDouble,
    PgDouble,
    _order_items,
    _PgSqla,
    _Result,
    _SqlState,
    _Table,
    _tok,
    _validator,
    _with_fk,
)
from tests.test_g140_validation_followups import (
    _TYPE_CLASH,
    _DriverDouble,
    _bits,
    _failed_orphan_report,
    _one_column,
    _poll_limit,
    _times,
    _whole_table_sums,
)

# ---------------------------------------------------------------------------
# A real (float4) key, as PostgreSQL / Aurora DSQL and psycopg read and compare it
# ---------------------------------------------------------------------------


def _f32(value: float) -> float:
    """``value`` rounded to the nearest float4 (a Python float holding it exactly)."""
    return struct.unpack("<f", struct.pack("<f", value))[0]


def _f32_text(value: float) -> str:
    """A float4 as PostgreSQL prints it (``extra_float_digits`` > 0): its shortest decimal."""
    for digits in range(1, 10):
        text = f"{value:.{digits}g}"
        if _f32(float(text)) == value:
            return text
    raise AssertionError(value)


class _RealPg(PgDouble):
    """A PostgreSQL / Aurora DSQL side whose ``real`` key column is read and compared as the
    server and psycopg do: returned as ``::text`` it is its shortest decimal, returned
    natively the float psycopg parses from that decimal; a cursor bound untyped (text) is
    parsed into a float4, one bound as a Python float is a float8, to which the stored
    float4 widens exactly -- so a decimal below it sorts before it."""

    def _parse(self, table: _Table, column: str, text: object) -> object:
        if table.types.get(column) != "real":
            return super()._parse(table, column, text)
        return text if isinstance(text, float) else _f32(float(text))  # type: ignore[arg-type]

    def _out(self, table: _Table, column: str, value: object, as_text: bool) -> object:
        if table.types.get(column) != "real":
            return super()._out(table, column, value, as_text)
        text = _f32_text(value)  # type: ignore[arg-type]
        return text if as_text else float(text)


def _reals(n: int = 1000, name: str = "app.prices", *, kind: str = "real") -> _Table:
    """``n`` price-like float4 keys (0.01 + 0.03 k): at a page of 100, three boundaries hold
    a value whose shortest decimal is below the stored float4."""
    rows = [{"f": _f32(0.01 + 0.03 * k), "v": k} for k in range(n)]
    return _Table(name, ("f",), rows, types={"f": kind, "v": "int"})


def _native_pk_page(table, pk_column, page_size, nonnull_columns=(), *, first):
    """The one-column count page as it was before G141: the keys come back natively."""
    return sql.SQL(
        "SELECT {pk} AS pk FROM {table}" + ("" if first else " WHERE {pk} > {last}")
        + " ORDER BY {pk} LIMIT {limit}"
    ).format(pk=sql.Identifier(pk_column), table=v._pg_table_identifier(table.name),
             last=sql.Placeholder("last"), limit=sql.Literal(page_size))


def _bind_natively(monkeypatch) -> None:
    """Put back the pre-G141 count pages (the boundary bound as psycopg's float)."""
    monkeypatch.setattr(v, "build_pg_pk_first_page_sql",
                        lambda t, pk, size, cols=(): _native_pk_page(t, pk, size, first=True))
    monkeypatch.setattr(v, "build_pg_pk_next_page_sql",
                        lambda t, pk, size, cols=(): _native_pk_page(t, pk, size, first=False))


def test_every_one_column_page_returns_its_last_key_as_text() -> None:
    table = _reals(3).tabledef()
    fk = ForeignKeyDef(name="fk", columns=["v"], referenced_table="app.p",
                       referenced_columns=["id"])
    count = v.build_pg_pk_next_page_sql(table, "f", 5).as_string(None)
    assert count == ('SELECT "f"::text AS pk FROM "app"."prices" WHERE "f" > %(last)s '
                     'ORDER BY "app"."prices"."f" LIMIT 5')
    for page in (v.build_pg_page_checksum_next_sql(table, "f", 5),
                 v.build_pg_orphan_page_next_sql("app.prices", fk, "f", 5)):
        text = page.as_string(None)
        assert "(array_agg(page_pk::text ORDER BY page_pk))[COUNT(*)]" in text
        assert '"f" > %(last)s' in text


def test_a_key_column_named_pk_still_sorts_as_itself() -> None:
    # The window's alias ``pk`` is text now; an unqualified ORDER BY "pk" would sort by it.
    table = _Table("app.t", ("pk",), [{"pk": 1}]).tabledef()
    text = v.build_pg_pk_first_page_sql(table, "pk", 5).as_string(None)
    assert text == 'SELECT "pk"::text AS pk FROM "app"."t" ORDER BY "app"."t"."pk" LIMIT 5'
    assert v._from_only(v.build_pg_pk_first_page_sql(table, "pk", 5),
                        v._pg_table_identifier("app.t")).as_string(None) == (
        'SELECT "pk"::text AS pk FROM ONLY "app"."t" ORDER BY "app"."t"."pk" LIMIT 5')


@pytest.mark.parametrize("page", [2, 7, 100, 997])
def test_a_real_keys_pages_count_and_checksum_every_row_once(page, monkeypatch) -> None:
    table = _reals()
    tabledef = table.tabledef()
    assert v._target_count_keyset(_RealPg(table), tabledef, "f", page) == 1000
    assert v._target_checksum_keyset(_RealPg(table), tabledef, "f", page) == str(
        sum(_tok(r) for r in table.rows))
    # Bound as psycopg's float (before G141), each boundary below its float4 was read again.
    _bind_natively(monkeypatch)
    native = v._target_count_keyset(_RealPg(table), tabledef, "f", page)
    assert native == {2: 1314, 7: 1077, 100: 1003, 997: 1000}[page]


@pytest.mark.parametrize("mode", [ValidationMode.ROW_COUNT, ValidationMode.CHECKSUM])
def test_three_missing_target_rows_are_no_longer_hidden_by_three_rereads(
    mode, monkeypatch
) -> None:
    source = _reals(kind="float")  # MySQL FLOAT: COUNT(*), and one checksum statement
    target = _Table(source.name, ("f",), source.rows[:-3], types={"f": "real", "v": "int"})

    def _run():
        return _validator(MySqlDouble(source), _RealPg(target), reconcile_page_size=100
                          ).validate(_SOURCE, _TARGET, [source.tabledef()], mode).items[0]

    item = _run()
    assert (item.source_row_count, item.target_row_count) == (1000, 997)
    assert item.row_count_match is False and item.matched is False
    if mode is ValidationMode.ROW_COUNT:
        # The verifier's false MATCH: three re-read boundary rows made up the three missing.
        _bind_natively(monkeypatch)
        before = _run()
        assert (before.target_row_count, before.matched) == (1000, True)


def test_a_postgresql_source_real_key_is_counted_and_checksummed_exactly() -> None:
    table = _reals(300)
    target = _Table(table.name, ("f",), table.rows, types=table.types)
    for mode in (ValidationMode.ROW_COUNT, ValidationMode.CHECKSUM):
        item = _validator(_PgSqla(_RealPg(table)), _RealPg(target), reconcile_page_size=7
                          ).validate(_PG_SOURCE, _TARGET, [table.tabledef()], mode).items[0]
        assert (item.source_row_count, item.target_row_count, item.matched) == (300, 300, True)
        assert item.error is None


def _real_child() -> tuple[_Table, _Table, ForeignKeyDef]:
    """300 children keyed like :func:`_reals`, every one but each tenth an orphan (270): a
    boundary row read twice is an orphan counted twice."""
    rows = [{"f": _f32(0.01 + 0.03 * k), "v": 0 if k % 10 == 0 else 9000 + k}
            for k in range(300)]
    child = _Table("app.prices", ("f",), rows, types={"f": "real", "v": "int"})
    parent = _Table("app.p", ("id",), [{"id": i} for i in range(10)])
    fk = ForeignKeyDef(name="fk_p", columns=["v"], referenced_table="app.p",
                       referenced_columns=["id"])
    return child, parent, fk


@pytest.mark.parametrize("page", [2, 7, 100])
def test_a_real_keyed_childs_orphans_are_counted_once(page) -> None:
    child, parent, fk = _real_child()
    assert v._target_orphan_count_keyset(_RealPg(child, parent), "app.prices", fk, "f",
                                         page) == 270
    # The cut-over pre-check counts the same way, on its own reconnecting connection.
    assert engine._count_orphans(lambda: _RealPg(child, parent), "app.prices", fk,
                                 page_size=page, sleep=lambda _s: None) == 270


# ---------------------------------------------------------------------------
# A boolean key (a MySQL TINYINT(1) on Aurora DSQL) is reconciled as MySQL's 0 / 1
# ---------------------------------------------------------------------------


class _BoolPg(PgDouble):
    """Aurora DSQL with a boolean key, as the server and psycopg read it: ``::text`` is
    'false' / 'true', natively a bool, and a cursor bound untyped is parsed as a boolean.
    The rows hold the source's 0 / 1, so the doubles' row tokens agree, as the checksum
    renders both ends 'false' / 'true'."""

    def _parse(self, table: _Table, column: str, text: object) -> object:
        if table.types.get(column) != "boolean":
            return super()._parse(table, column, text)
        return {"false": 0, "true": 1}[text]  # type: ignore[index]

    def _out(self, table: _Table, column: str, value: object, as_text: bool) -> object:
        if table.types.get(column) != "boolean":
            return super()._out(table, column, value, as_text)
        return ("true" if value else "false") if as_text else bool(value)


def _flags(keys=(0, 1), *, kind: str = "tinyint(1)") -> _Table:
    """A one-row-per-flag table keyed by a MySQL TINYINT(1) (``kind="boolean"``: as it is
    held on Aurora DSQL)."""
    return _Table("app.flags", ("id",), [{"id": k, "v": 10 + k} for k in keys],
                  types={"id": kind, "v": "int"})


@pytest.mark.parametrize("page, cursors", [(1, ["false", "true"]), (2, ["true"]), (5, [])])
def test_a_boolean_key_is_streamed_as_the_0_and_1_the_source_holds(page, cursors) -> None:
    target = _BoolPg(_flags(kind="boolean"))
    assert list(v._iter_target_pks(target, _flags().tabledef(), "id", page)) == [0, 1]
    # The cursor is still bound back as the text it came back as.
    assert [params["last"] for _, params in target.executed if "last" in params] == cursors


@pytest.mark.parametrize("mode", [ValidationMode.ROW_COUNT, ValidationMode.CHECKSUM])
def test_a_tinyint1_key_on_a_boolean_target_is_reconciled(mode) -> None:
    # The verifier's regression: int('false') errored the table whenever reconcile ran.
    source = _flags()
    item = _validator(MySqlDouble(source), _BoolPg(_flags(kind="boolean")), reconcile_page_size=1
                      ).validate(_SOURCE, _TARGET, [source.tabledef()], mode,
                                 reconcile=True).items[0]
    assert item.error is None and item.matched is True
    assert item.reconcile == ReconcileResult(pk_column="id", source_count=2, target_count=2)


def test_a_boolean_keys_divergence_is_still_named_exactly() -> None:
    # Equal row counts (1 / 1), different keys: only the reconciliation sees it.
    source = _flags((1,))
    item = _validator(MySqlDouble(source), _BoolPg(_flags((0,), kind="boolean")),
                      reconcile_page_size=1).validate(
        _SOURCE, _TARGET, [source.tabledef()], ValidationMode.ROW_COUNT, reconcile=True).items[0]
    assert item.error is None and item.row_count_match is True and item.matched is False
    assert (item.reconcile.missing_on_target, item.reconcile.missing_sample) == (1, ["1"])
    assert (item.reconcile.extra_on_target, item.reconcile.extra_sample) == (1, ["0"])


# ---------------------------------------------------------------------------
# A MySQL TIME / BIT key is paged with exact cursors; a FLOAT key keeps one statement
# ---------------------------------------------------------------------------


def _next_page_cursors(double: MySqlDouble) -> list:
    return [params["last"] for statement, params in double.executed
            if "SUM(page_tok)" in statement and "last" in params]


@pytest.mark.parametrize("kind, bound_type", [("time", str), ("bit", int)])
@pytest.mark.parametrize("page", [1, 3, 7])
def test_a_one_column_time_or_bit_key_pages_with_the_value_mysql_compares(
    kind, bound_type, page
) -> None:
    table = _one_column(kind)
    double = _DriverDouble(table)
    total = v._source_checksum(double, table.tabledef(), page, dialect=_MY,
                               expected_rows=len(table.rows))
    assert total == str(sum(_tok(r) for r in table.rows))
    cursors = _next_page_cursors(double)
    assert cursors and all(type(c) is bound_type for c in cursors)
    assert len(cursors) == len(table.rows) // page
    assert _whole_table_sums(double) == 0


def test_a_negative_time_cursor_is_its_duration_text() -> None:
    table = _one_column("time")  # 0, -00:01:30, -00:03:00, ...
    double = _DriverDouble(table)
    v._source_checksum(double, table.tabledef(), 2, dialect=_MY)
    # Descending durations, so each page ends on the larger (closer to zero) value.
    assert _next_page_cursors(double)[:3] == ["-00:27:00", "-00:24:00", "-00:21:00"]


@pytest.mark.parametrize("make", [_times, _bits])
def test_a_composite_time_or_bit_key_pages_through_the_readers_cursor(make) -> None:
    table = make()
    double = _DriverDouble(table)
    total = v._source_checksum(double, table.tabledef(), 5, dialect=_MY,
                               expected_rows=len(table.rows))
    assert total == str(sum(_tok(r) for r in table.rows))
    assert _whole_table_sums(double) == 0
    assert sum("SUM(ck_tok)" in s for s, _ in double.executed) == len(table.rows) // 5 + 1


@pytest.mark.parametrize("kind", ["time", "bit"])
def test_the_drivers_own_binding_of_a_one_column_key_falls_back_in_bounded_pages(
    kind, monkeypatch, caplog
) -> None:
    monkeypatch.setattr(v, "_cursor_value", lambda _type, value: value)
    table = _one_column(kind)
    double = _DriverDouble(table)
    expected = len(table.rows)
    with caplog.at_level("WARNING", logger="dsql_migrator.core.validator"):
        total = v._source_checksum(double, table.tabledef(), 3, _poll_beat(table),
                                   dialect=_MY, expected_rows=expected)
    assert total == str(sum(_tok(r) for r in table.rows))  # the one exact statement
    assert _whole_table_sums(double) == 1
    assert sum("SUM(page_tok)" in s for s, _ in double.executed) <= expected // 3 + 1
    assert "read in one statement instead" in caplog.text


def _poll_beat(table: _Table):
    stop = _poll_limit(table)

    def _beat() -> None:
        assert not stop(), "the pages never ended"

    return _beat


class _MyScripted:
    """A MySQL source answering a one-column checksum page with the next scripted row, and the
    whole-table checksum with ``whole``."""

    invalidated = False

    def __init__(self, pages: list, whole: int = 77) -> None:
        self.pages = list(pages)
        self.whole = whole
        self.executed: list = []

    def execute(self, statement: object, parameters: object = None) -> _Result:
        text = str(statement)
        self.executed.append((text, dict(parameters or {})))
        if "SUM(page_tok)" in text:
            return _Result([self.pages.pop(0)])
        return _Result([(self.whole,)])


def test_a_one_column_page_that_does_not_move_falls_back_even_without_a_count(caplog) -> None:
    table = _one_column("time").tabledef()
    source = _MyScripted([(5, "00:01:00", 3), (5, "00:01:00", 3), (5, "00:01:00", 3)])
    with caplog.at_level("WARNING", logger="dsql_migrator.core.validator"):
        assert v._source_checksum(source, table, 3, dialect=_MY) == "77"
    assert sum("SUM(page_tok)" in s for s, _ in source.executed) == 2
    assert "did not move past the key it was read after" in caplog.text


@pytest.mark.parametrize("counted, why", [(4, "had read 6 rows, more than the 4"),
                                          (9, "the pages read 7 rows, COUNT(*) counts 9")])
def test_one_column_pages_are_held_to_the_live_count(counted, why, caplog) -> None:
    table = _one_column("bit").tabledef()
    source = _MyScripted([(1, 3, 3), (2, 6, 3), (3, 9, 1)])
    with caplog.at_level("WARNING", logger="dsql_migrator.core.validator"):
        assert v._source_checksum(source, table, 3, dialect=_MY, expected_rows=counted) == "77"
    assert why in caplog.text
    # Without a live count, the same pages are trusted.
    assert v._source_checksum(_MyScripted([(1, 3, 3), (2, 6, 3), (3, 9, 1)]), table, 3,
                              dialect=_MY) == "6"


def test_a_float_key_keeps_one_statement_which_beats_before_and_after() -> None:
    table = _one_column("float")
    double = _DriverDouble(table)
    beats: list = []
    total = v._source_checksum(double, table.tabledef(), 3, lambda: beats.append(
        len(double.executed)), dialect=_MY, expected_rows=len(table.rows))
    assert total == str(sum(_tok(r) for r in table.rows))
    assert _whole_table_sums(double) == 1
    statement = next(i for i, (s, _) in enumerate(double.executed)
                     if s.startswith("SELECT COALESCE(SUM("))
    assert beats == [statement, statement + 1]  # one beat right before it, one right after


def test_a_stop_before_the_one_statement_skips_it() -> None:
    table = _one_column("float")
    double = _DriverDouble(table)

    def _stopped() -> None:
        raise ValidationCancelled("stopped")

    with pytest.raises(ValidationCancelled):
        v._source_checksum(double, table.tabledef(), 3, _stopped, dialect=_MY)
    assert _whole_table_sums(double) == 0


def test_the_fallback_statement_beats_too(monkeypatch) -> None:
    monkeypatch.setattr(v, "_cursor_value", lambda _type, value: value)
    table = _one_column("time")
    double = _DriverDouble(table)
    beats: list = []
    v._source_checksum(double, table.tabledef(), 3, lambda: beats.append(len(double.executed)),
                       dialect=_MY, expected_rows=len(table.rows))
    statement = next(i for i, (s, _) in enumerate(double.executed)
                     if s.startswith("SELECT COALESCE(SUM(") and "page_tok" not in s)
    assert beats[-2:] == [statement, statement + 1]


def test_a_postgresql_source_one_column_checksum_is_held_to_its_count() -> None:
    table = _reals(30)
    assert v._source_checksum_for(_PG, _PgSqla(_RealPg(table)), table.tabledef(), 4,
                                  expected_rows=30) == str(sum(_tok(r) for r in table.rows))
    with pytest.raises(PagedReadError, match="had read 28 rows, more than the 27"):
        v._source_checksum_for(_PG, _PgSqla(_RealPg(table)), table.tabledef(), 4,
                               expected_rows=27)
    with pytest.raises(PagedReadError, match="read 30 rows, but COUNT"):
        v._source_checksum_for(_PG, _PgSqla(_RealPg(table)), table.tabledef(), 4,
                               expected_rows=31)


def test_a_postgresql_source_whose_count_and_checksum_disagree_is_an_error() -> None:
    table = _reals(30)

    class _CountsOneLess(_RealPg):
        """The count's last (short) page misses a row: the count reads 29 of 30."""

        def resolve(self, statement: Any, params: Any) -> Any:
            rows = super().resolve(statement, params)
            text = statement if isinstance(statement, str) else statement.as_string(None)
            short = " AS pk" in text and len(rows) < int(text.rsplit("LIMIT ", 1)[1])
            return rows[:-1] if short else rows

    target = _Table(table.name, ("f",), table.rows, types=table.types)
    item = _validator(_PgSqla(_CountsOneLess(table)), _RealPg(target), reconcile_page_size=7
                      ).validate(_PG_SOURCE, _TARGET, [table.tabledef()],
                                 ValidationMode.CHECKSUM).items[0]
    assert item.matched is False
    assert "had read 30 rows, more than the 29" in (item.error or "")


# ---------------------------------------------------------------------------
# A table not matched ONLY because its orphan check could not run
# ---------------------------------------------------------------------------


def _item(**kw: Any) -> TableValidationResult:
    base = dict(table="app.t", source_row_count=5, target_row_count=5, row_count_match=True,
                checksum_match=True, matched=False, orphan_check_error="foreign key fk (boom)")
    base.update(kw)
    return TableValidationResult(**base)


@pytest.mark.parametrize("change, only", [
    ({}, True),
    ({"checksum_match": None}, True),
    ({"orphan_check_error": None}, False),
    ({"matched": True, "orphan_check_error": None}, False),
    ({"row_count_match": False, "target_row_count": 4}, False),
    ({"checksum_match": False}, False),
    ({"excluded_values_block": True}, False),
    ({"reconcile": ReconcileResult(pk_column="id", source_count=5, target_count=5,
                                   missing_on_target=1, extra_on_target=1, consistent=False)},
     False),
    ({"error": "QueryCanceled: boom"}, False),
])
def test_orphan_check_only_is_the_orphan_check_and_nothing_else(change, only) -> None:
    assert _item(**change).orphan_check_only is only


def _render(report: ValidationReport, **kw: Any):
    from tests.test_ui_validation import _g1_render

    return _g1_render(report, None, **kw)


@pytest.mark.parametrize("migration_type", [MigrationType.FULL_LOAD_ONLY,
                                            MigrationType.FULL_LOAD_AND_CDC,
                                            MigrationType.CDC_ONLY])
@pytest.mark.parametrize("streamed", [False, True])
def test_an_orphan_check_only_table_gets_no_backfill_runbook(migration_type, streamed) -> None:
    ui = _render(_failed_orphan_report(missing_row=False), migration_type=migration_type,
                 cdc_stack_streamed=streamed)
    body = ui.body()
    assert "The orphan check could not run — there is nothing to backfill" in body
    assert ("app.order_items: its row counts and checksums match, and it is not matched only "
            "because the orphan check could not run (see Orphan records)") in body
    for runbook in ("backfill the gap", "Steps to recover", "Delete CDC infrastructure",
                    "Drop & reload"):
        assert runbook not in body, runbook


def test_the_runbook_stays_for_a_real_gap_beside_an_orphan_check_only_table() -> None:
    report = _failed_orphan_report(missing_row=False)
    short = TableValidationResult(table="app.lines", source_row_count=5, target_row_count=4,
                                  row_count_match=False, checksum_match=False, matched=False)
    report = ValidationReport.build(mode=report.mode, items=[*report.items, short],
                                    orphan_check_performed=True)
    body = _render(report, migration_type=MigrationType.FULL_LOAD_ONLY).body()
    assert "The orphan check could not run — there is nothing to backfill" in body
    assert "Re-run Full Load to backfill the gap" in body and "Steps to recover" in body


def test_known_dropped_rows_beside_an_orphan_check_only_table_are_no_backfill_either() -> None:
    report = _failed_orphan_report(missing_row=False)
    dropped = TableValidationResult(table="app.blobs", source_row_count=5, target_row_count=4,
                                    row_count_match=False, matched=False, rows_quarantined=1)
    report = ValidationReport.build(mode=report.mode, items=[*report.items, dropped],
                                    orphan_check_performed=True)
    body = _render(report).body()
    assert "These rows can't be stored as-is" in body
    assert "The orphan check could not run — there is nothing to backfill" in body
    assert "backfill the gap" not in body


def test_a_fast_sweeps_orphan_only_notice_names_only_the_row_counts() -> None:
    from dsql_migrator.ui.validation import summarize_validation

    # CHECKSUM mode with Fast sweep on: the counts agree, so no checksum ran.
    child = _order_items(23)
    orders = _Table("app.orders", ("id",), [{"id": i} for i in range(1000, 1006)])
    target = PgDouble(_order_items(23), orders, fail={"WITH pg AS MATERIALIZED": _TYPE_CLASH})
    report = _validator(MySqlDouble(child, orders), target, reconcile_page_size=5).validate(
        _SOURCE, _TARGET, [_with_fk(child), orders.tabledef()], ValidationMode.CHECKSUM,
        check_orphans=True, deep_only_on_count_mismatch=True)
    item = {i.table: i for i in report.items}["app.order_items"]
    assert (item.checksum_match, item.deep_checks_skipped, item.orphan_check_only) == (
        None, True, True)
    assert summarize_validation(report).orphan_check_only_checksummed is False
    body = _render(report).body()
    assert ("app.order_items: its row counts match, and it is not matched only because the "
            "orphan check could not run (see Orphan records)") in body
    assert "checksums match" not in body
    # Without the fast sweep the checksum ran, and the notice says so.
    assert summarize_validation(
        _failed_orphan_report(missing_row=False)).orphan_check_only_checksummed is True


@pytest.mark.parametrize("checksums, named", [
    ((True, True), True), ((True, None), False), ((None, None), False)])
def test_the_notice_names_the_checksums_only_when_each_tables_ran(checksums, named) -> None:
    from dsql_migrator.ui.validation import summarize_validation

    report = ValidationReport.build(
        mode=ValidationMode.CHECKSUM, orphan_check_performed=True,
        items=[_item(table=f"app.t{n}", checksum_match=c) for n, c in enumerate(checksums)])
    summary = summarize_validation(report)
    assert summary.orphan_check_only_tables == ("app.t0", "app.t1")
    assert summary.orphan_check_only_checksummed is named


def _check_status(ui, label: str) -> tuple[str, str]:
    """``(status, detail)`` of the readiness check row ``label``."""
    at = ui.texts.index(label)
    return ui.texts[at + 1], ui.texts[at + 2]


def test_readiness_says_the_orphan_check_could_not_run_not_that_data_mismatched() -> None:
    ui = _render(_failed_orphan_report(missing_row=False))
    status, detail = _check_status(ui, "Data identical")
    assert status == "Passed"
    assert detail.startswith("1/2 tables matched (mode: CHECKSUM). The comparison of 1 more "
                             "table matched too: it is not matched only because the orphan "
                             "check could not run (below).")
    assert "mismatched" not in detail
    status, detail = _check_status(ui, "Orphan check ran")
    assert status == "Failed"
    assert detail.startswith("The orphan check could not run for 1 table(s): app.order_items.")
    # The verdict still holds cut over (fail closed).
    assert "Not ready for cut-over" in ui.body()


def test_a_real_mismatch_with_a_failed_orphan_check_still_reads_as_a_mismatch() -> None:
    ui = _render(_failed_orphan_report(missing_row=True))
    status, detail = _check_status(ui, "Data identical")
    assert status == "Failed" and "1 mismatched" in detail and "The comparison of" not in detail
    assert _check_status(ui, "Orphan check ran")[0] == "Failed"


def test_the_ai_diagnosis_is_told_there_is_nothing_to_backfill() -> None:
    from dsql_migrator.ui.validation import (
        _validation_run_facts,
        format_drift,
        summarize_validation,
    )

    report = _failed_orphan_report(missing_row=False)
    facts = _validation_run_facts(summarize_validation(report), format_drift(report))
    assert ("Not matched only because their orphan check could not run (their comparison "
            "matched; nothing to backfill): app.order_items") in facts
    mixed = _failed_orphan_report(missing_row=True)
    assert "nothing to backfill" not in _validation_run_facts(
        summarize_validation(mixed), format_drift(mixed))


def test_no_orphan_check_row_when_every_check_ran() -> None:
    report = ValidationReport.build(mode=ValidationMode.ROW_COUNT, orphan_check_performed=True,
                                    items=[_item(orphan_check_error=None, matched=True)])
    assert "Orphan check ran" not in _render(report).texts


def test_the_result_badge_says_the_orphan_check_could_not_run() -> None:
    from dsql_migrator.ui.validation import _table_row

    row = _table_row(_item())
    assert row["result"] == {"text": "orphan check could not run", "color": "red-6"}
    assert row["result_sort"] == 0  # still among the failures, first
    assert _table_row(_item(checksum_match=False))["result"]["text"] == "mismatch"


def test_the_text_report_does_not_call_it_a_mismatch() -> None:
    text = render_text_report(_failed_orphan_report(missing_row=False))
    assert ("- app.order_items: source=23 target=23 (row count match), checksum match, "
            "orphan check could not run, [NOT MATCHED]") in text
    assert ("- Data identical: yes (1/2 tables matched; the comparison of 1 more matched, but "
            "the orphan check could not run)") in text
    # A real mismatch keeps its MISMATCH (G140's pin) and its NO.
    mixed = render_text_report(_failed_orphan_report(missing_row=True))
    assert "orphan check could not run, [MISMATCH]" in mixed
    assert "- Data identical: NO (1/2 tables matched)" in mixed
    assert "Overall: MISMATCH" in text


def test_an_orphan_check_only_table_still_blocks_cut_over() -> None:
    from dsql_migrator.ui.validation import cutover_release_state, summarize_validation

    summary = summarize_validation(_failed_orphan_report(missing_row=False))
    assert summary.orphan_check_only_tables == ("app.order_items",)
    assert summary.orphan_check_failed_tables == ("app.order_items",)
    assert cutover_release_state(summary) == "blocked"
    mixed = summarize_validation(_failed_orphan_report(missing_row=True))
    assert mixed.orphan_check_only_tables == ()
    assert mixed.orphan_check_failed_tables == ("app.order_items",)


# ---------------------------------------------------------------------------
# A session the server ends is a lost connection on the target read path
# ---------------------------------------------------------------------------


class _EndsOnce(_RealPg):
    """A target connection the server ends once, at statement ``at`` (``sqlstate``)."""

    def __init__(self, *tables: _Table, at: int, sqlstate: str) -> None:
        super().__init__(*tables)
        self.at, self.sqlstate, self.statements = at, sqlstate, 0

    def resolve(self, statement: Any, params: Any) -> Any:
        self.statements += 1
        if self.statements == self.at:
            raise _SqlState(self.sqlstate, "terminating connection due to administrator command")
        return super().resolve(statement, params)


@pytest.mark.parametrize("sqlstate", ["57P01", "57P02", "57P03"])
def test_a_validation_read_survives_a_session_the_server_ends(sqlstate) -> None:
    table = _reals(40)
    target = _EndsOnce(_Table(table.name, ("f",), table.rows, types=table.types), at=4,
                       sqlstate=sqlstate)
    opened: list = []

    def _factory(_cfg):
        opened.append(1)
        return target

    validator = v.Validator(source_engine_factory=lambda _cfg: MySqlDouble(table),
                            target_connection_factory=_factory, reconcile_page_size=7)
    item = validator.validate(_SOURCE, _TARGET, [table.tabledef()]).items[0]
    assert item.error is None and item.matched is True
    assert len(opened) == 2  # the ended session was replaced once


def test_a_query_cancel_is_still_an_error_not_a_reconnect() -> None:
    # 57014 (a statement timeout or a cancel) is no ended session: never replayed.
    table = _reals(40)
    target = _EndsOnce(_Table(table.name, ("f",), table.rows, types=table.types), at=4,
                       sqlstate="57014")
    opened: list = []
    validator = v.Validator(source_engine_factory=lambda _cfg: MySqlDouble(table),
                            target_connection_factory=lambda _cfg: opened.append(1) or target,
                            reconcile_page_size=7)
    item = validator.validate(_SOURCE, _TARGET, [table.tabledef()]).items[0]
    assert item.matched is False
    assert item.error == "_SqlState: terminating connection due to administrator command"
    assert len(opened) == 1


def test_a_session_the_server_ends_never_costs_the_null_proof() -> None:
    proof = v._NullProofAccumulator(columns=("c",), legacy=frozenset())
    runs: list = []

    def _run(acc):
        runs.append(acc)
        if acc is not None:
            acc.failed = _SqlState("57P01", "terminating connection")
            raise acc.failed
        return 5

    with pytest.raises(_SqlState):
        v.Validator._with_proof_fallback(_run, proof)
    # Once more WITH the proof (a lost connection is not the proof's fault), then the error.
    assert runs == [proof, proof] and proof.reason is None



# ---------------------------------------------------------------------------
# The manual
# ---------------------------------------------------------------------------


def _flat(path) -> str:
    """The page with its wrapping and blockquote markers folded into single spaces."""
    import re
    from pathlib import Path

    text = (Path(__file__).resolve().parents[1] / path).read_text(encoding="utf-8")
    return re.sub(r"\s+", " ", re.sub(r"\n>\s?", "\n", text))


@pytest.mark.parametrize("lang, needles", [
    ("en", ("never reads a row at a page boundary twice", "held to the source's row count",
            "a single-precision `FLOAT` part, a one-column key too",
            "15-minute no-progress limit", "*orphan check could not run*",
            "**Orphan check ran**", "says there is nothing to backfill",
            "A `TINYINT(1)` key, a boolean on Aurora DSQL, is reconciled too")),
    ("ko", ("페이지 경계의 행을 두 번 읽지 않습니다", "소스의 행 수에 맞춥니다",
            "단정밀도 `FLOAT` 부분이 있는 키도 단일 컬럼 키까지", "15분 무진행 한도",
            "*orphan check could not run*", "**Orphan check ran**", "채워 넣을 것이 없다고",
            "boolean이 되는 `TINYINT(1)` 키도 대조되며")),
    ("ja", ("ページ境界の行を二度読むことはありません", "ソースの行数と照合します",
            "単精度 `FLOAT` の部分を持つキーも", "15 分の無進捗上限",
            "*orphan check could not run*", "**Orphan check ran**", "埋め戻すものがないと",
            "boolean になる `TINYINT(1)` キーも reconcile され")),
])
def test_the_manual_says_what_g141_changed(lang, needles) -> None:
    validation = _flat(f"docs/manual/{lang}/05-validation.md")
    for needle in needles:
        assert needle in validation, (lang, needle)
    # G140's one-statement rule for TIME / BIT keys is gone.
    for stale in ("`FLOAT`, `TIME` or `BIT` part", "`FLOAT`·`TIME`·`BIT` 부분",
                  "`FLOAT`・`TIME`・`BIT` の部分"):
        assert stale not in validation, (lang, stale)
    assert "`57P01`–`57P03`" in _flat(f"docs/manual/{lang}/02-evaluation-and-schema-conversion.md")
    testing = _flat(f"docs/manual/{lang}/08-testing-and-verification.md")
    assert "`real`" in testing and "`FLOAT` / `TIME` / `BIT` part is checksummed" not in testing
