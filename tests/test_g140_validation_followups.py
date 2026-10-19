# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G140: follow-ups to G138.

- A MySQL TIME / BIT key part: the cursor binds it as MySQL compares it (the duration text,
  the integer its bits hold) -- the driver's own binding started the next page before its
  cursor -- and Validation checksums such a key (one-column keys too) in one exact statement.
- The source's keyset pages stop as soon as they read more than the snapshot's COUNT(*)
  (read before them), so a cursor that goes back and forth ends in bounded time; a MySQL
  source then falls back to the one exact statement (and logs why), a PostgreSQL source's
  table is an error.
- A failed orphan check is kept apart from the comparison's error: the table is not
  matched, but its counts and data verdicts stay visible, Orphan records lists the failed
  foreign key, and the dead-letter "could not finish comparing" exit leaves it out.
- The Full Load reader refuses a single-precision FLOAT key column only where another key
  column follows it.

The MySQL double below answers the pagers' statements the way MySQL reads what PyMySQL
sends: each bound parameter is rendered with PyMySQL's own escaping, then read back with
MySQL's rules for the column it is compared with -- no database is reached.
"""

from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from pymysql.converters import escape_bytes, escape_item

import dsql_migrator.core.source_dialect.mysql as mysql_dialect
import dsql_migrator.core.validator as v
from dsql_migrator.core.exporter import ExportError, keyset_stream
from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    TableDef,
    TableValidationResult,
    ValidationMode,
    ValidationReport,
)
from dsql_migrator.core.validator import PagedReadError, render_text_report
from tests.test_g138_composite_validation import (
    _MY,
    _PG,
    _SOURCE,
    _TARGET,
    MySqlDouble,
    PgDouble,
    _order_items,
    _PgSqla,
    _Scripted,
    _SqlState,
    _Table,
    _tok,
    _validator,
    _with_fk,
)


# ---------------------------------------------------------------------------
# A MySQL double that reads bound parameters as MySQL reads PyMySQL's literals
# ---------------------------------------------------------------------------


def _micros(value: timedelta) -> int:
    return value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds


def _time_literal(text: str) -> int:
    """Microseconds of a MySQL TIME string literal ``[-]H..H:MM:SS[.ffffff]``."""
    sign = -1 if text.startswith("-") else 1
    hms, _, fraction = text.lstrip("-").partition(".")
    hours, minutes, seconds = (int(part) for part in hms.split(":"))
    return sign * ((hours * 3600 + minutes * 60 + seconds) * 1_000_000
                   + int((fraction + "000000")[:6]))


def _leading_number(text: str) -> int:
    """MySQL's string-to-number conversion of a string literal: its leading digits, or 0."""
    match = re.match(r"\s*(\d+)", text)
    return int(match[1]) if match else 0


def _kind(column_type: object) -> str:
    """A recorded MySQL column type's base type (``"time"`` for ``time(6)``)."""
    return str(column_type or "").strip().lower().split("(")[0].split(" ")[0]


class _DriverDouble(MySqlDouble):
    """``MySqlDouble`` whose cursor terms compare a TIME / BIT key column as MySQL does with
    the literal PyMySQL sends for the bound value (``escape_item`` / ``escape_bytes``)."""

    def _stored(self, table: _Table, column: str, value: object) -> object:
        kind = _kind(table.types.get(column))
        if kind == "time":
            return _micros(value)  # type: ignore[arg-type]
        if kind == "bit":
            return int.from_bytes(value, "big")  # type: ignore[arg-type]
        return value

    def _bound(self, table: _Table, column: str, value: object) -> object:
        literal = (escape_bytes(bytes(value)) if isinstance(value, (bytes, bytearray))
                   else escape_item(value, "utf8mb4"))
        kind = _kind(table.types.get(column))
        if literal.startswith("'"):
            text = literal[1:-1]
            if kind == "time":
                return _time_literal(text)
            if kind == "bit":
                return _leading_number(text)
            return text
        return int(literal) if re.fullmatch(r"-?\d+", literal) else float(literal)

    def _filter(self, table: _Table, where: str, params: dict) -> list[dict]:
        rows = table.rows
        if where:
            expr = re.sub(r"`([^`]+)` > :(\w+)", lambda m: f"_gt(row, {m[1]!r}, {m[2]!r})", where)
            expr = re.sub(r"`([^`]+)` = :(\w+)", lambda m: f"_eq(row, {m[1]!r}, {m[2]!r})", expr)
            expr = expr.replace(" AND ", " and ").replace(" OR ", " or ").replace("1 = 0", "False")
            def _pair(row: dict, c: str, n: str) -> tuple:
                return self._stored(table, c, row[c]), self._bound(table, c, params[n])

            env = {
                "_gt": lambda row, c, n: (lambda s, b: s > b)(*_pair(row, c, n)),
                "_eq": lambda row, c, n: (lambda s, b: s == b)(*_pair(row, c, n)),
            }
            rows = [row for row in rows if eval(expr, env, {"row": row})]  # noqa: S307
        return sorted(rows, key=lambda r: tuple(self._stored(table, c, r[c]) for c in table.key))


def _times(name: str = "app.shifts", *, negative_half_hours: bool = False) -> _Table:
    """(t TIME, n INT): every half hour from -03:00 to +02:30, three rows each; with
    ``negative_half_hours`` only -00:30, -01:30, ... -11:30 -- every page boundary then a
    value PyMySQL writes an hour early."""
    minutes = ([-(30 + 60 * k) for k in range(12)] if negative_half_hours
               else [30 * k for k in range(-6, 6)])
    rows = [{"t": timedelta(minutes=m), "n": n, "v": f"v{m}{n}"} for m in minutes for n in range(3)]
    return _Table(name, ("t", "n"), rows, types={"t": "time", "n": "int", "v": "varchar(9)"})


def _bits(name: str = "app.flags") -> _Table:
    """(id INT, flag BIT(8)), the flags including the byte '1' (0x31) and 0x00."""
    rows = [{"id": i // 4, "flag": bytes([b]), "v": f"v{i}"}
            for i, b in enumerate([0, 1, 0x31, 0xFF] * 6)]
    return _Table(name, ("id", "flag"), rows,
                  types={"id": "int", "flag": "bit(8)", "v": "varchar(9)"})


def _one_column(kind: str) -> _Table:
    if kind == "time":
        rows = [{"t": timedelta(seconds=-90 * k), "v": k} for k in range(20)]
        return _Table("app.t1", ("t",), rows, types={"t": "time(6)", "v": "int"})
    if kind == "bit":
        rows = [{"b": (k * 7).to_bytes(2, "big"), "v": k} for k in range(20)]
        return _Table("app.b1", ("b",), rows, types={"b": "bit(16)", "v": "int"})
    rows = [{"f": k / 4, "v": k} for k in range(20)]
    return _Table("app.f1", ("f",), rows, types={"f": "float", "v": "int"})


def _keys(table: _Table, rows) -> list[tuple]:
    return [tuple(r[c] for c in table.key) for r in rows]


def _bounded(rows, table: _Table) -> list:
    """Every row of a read -- failing, never hanging, on a read that does not end (these
    reads went back and forth forever before G140)."""
    out = []
    for row in rows:
        out.append(row)
        assert len(out) <= 5 * len(table.rows), "the read never ended"
    return out


def _poll_limit(table: _Table):
    """A Stop pressed long after the table could have been read (the run then raises
    ValidationCancelled instead of hanging)."""
    polls = {"n": 0}

    def _stop() -> bool:
        polls["n"] += 1
        return polls["n"] > 20 * len(table.rows)

    return _stop


class _TimedPg(PgDouble):
    """The Aurora DSQL side of a TIME key: the text-bound last key parses back into the
    column's own value, as the server parses it."""

    def _parse(self, table: _Table, column: str, text: object) -> object:
        if _kind(table.types.get(column)) == "time":
            return next(r[column] for r in table.rows if str(r[column]) == text)
        return super()._parse(table, column, text)


# ---------------------------------------------------------------------------
# The cursor's binding (Full Load reader and the Validation pager alike)
# ---------------------------------------------------------------------------


def _key_table(*columns: tuple[str, str]) -> TableDef:
    return TableDef(
        name="app.k",
        columns=[ColumnDef(name=n, mysql_type=t) for n, t in columns] + [
            ColumnDef(name="payload", mysql_type="varchar(9)")],
        primary_key=[n for n, _ in columns],
    )


def test_a_time_key_part_is_bound_as_its_duration_text() -> None:
    table = _key_table(("t", "time(6)"), ("n", "int"))
    for value, text in (
        (timedelta(minutes=-30), "-00:30:00"),             # PyMySQL writes '-1:30:00'
        (timedelta(microseconds=-500_000), "-00:00:00.5"),  # PyMySQL: '-1:59:59.500000'
        (timedelta(hours=30, minutes=5), "30:05:00"),
        (timedelta(hours=-838, minutes=-59, seconds=-59), "-838:59:59"),
    ):
        where, params = _MY.keyset_after_sql(table, (value, 2), {})
        assert where == "((`t` > :last_0) OR (`t` = :last_0 AND `n` > :last_1))"
        assert params == {"last_0": text, "last_1": 2}
    single = _key_table(("t", "time"))
    assert _MY.keyset_after_sql(single, (timedelta(minutes=-30),), {}) == (
        "`t` > :last", {"last": "-00:30:00"})


def test_a_bit_key_part_is_bound_as_the_integer_its_bits_hold() -> None:
    table = _key_table(("id", "int"), ("flag", "bit(1)"))
    assert _MY.keyset_after_sql(table, (7, b"\x01"), {})[1] == {"last_0": 7, "last_1": 1}
    wide = _key_table(("b", "BIT(64)"))
    assert _MY.keyset_after_sql(wide, (b"\xff" * 8,), {})[1] == {"last": 2**64 - 1}
    # A BINARY key part keeps its bytes: MySQL compares those as bytes.
    binary = _key_table(("id", "int"), ("h", "binary(1)"))
    assert _MY.keyset_after_sql(binary, (7, b"\x01"), {})[1] == {"last_0": 7, "last_1": b"\x01"}


@pytest.mark.parametrize("make", [_times, _bits, lambda: _one_column("time"),
                                  lambda: _one_column("bit")])
@pytest.mark.parametrize("batch", [1, 2, 3, 5, 7])
def test_the_reader_pages_a_time_or_bit_key_exactly(make, batch) -> None:
    table = make()
    read = _keys(table, _bounded(keyset_stream(_DriverDouble(table), table.tabledef(),
                                               batch_size=batch, dialect=_MY), table))
    assert read == _keys(table, _DriverDouble(table)._filter(table, "", {}))  # once each, in order


@pytest.mark.parametrize("make", [_times, _bits])
def test_the_drivers_own_binding_read_rows_again(make, monkeypatch) -> None:
    # Why the binding matters: bound as PyMySQL returns it, the cursor starts before the
    # page it continues, so the read meets rows it already passed.
    monkeypatch.setattr(mysql_dialect, "_cursor_value", lambda _type, value: value)
    table = make()
    stream = keyset_stream(_DriverDouble(table), table.tabledef(), batch_size=5, dialect=_MY)
    with pytest.raises(ExportError, match="returned the row the page was read after"):
        for _ in range(5 * len(table.rows)):
            next(stream)


# ---------------------------------------------------------------------------
# A single-precision FLOAT key column: refused only where another key column follows
# ---------------------------------------------------------------------------


def test_a_float_key_column_is_refused_only_when_another_key_column_follows() -> None:
    with pytest.raises(ExportError, match="single-precision FLOAT followed by another key"):
        _MY.keyset_after_sql(_key_table(("f", "float"), ("n", "int")), (0.7, 3), {})
    for table, last, bound in (
        (_key_table(("n", "int"), ("f", "float")), (3, 0.7), {"last_0": 3, "last_1": 0.7}),
        (_key_table(("f", "float unsigned")), (0.7,), {"last": 0.7}),  # the only key column
    ):
        assert _MY.keyset_after_sql(table, last, {})[1] == bound  # bound as before
    members = {"s": ("enum", ("", "a", "b"))}
    walked = _key_table(("s", "enum('a','b')"), ("f", "float"), ("n", "int"))
    with pytest.raises(ExportError, match="FLOAT followed by another key"):
        _MY.keyset_after_sql(walked, ("a", 0.7, 1), members)
    last_after_walk = _key_table(("s", "enum('a','b')"), ("f", "float"))
    assert _MY.keyset_after_sql(last_after_walk, ("a", 0.7), members)[0] == (
        "`s` = :last_0 AND ((`f` > :last_1))")


@pytest.mark.parametrize("key, rows", [
    (("f",), [{"f": k / 4, "v": k} for k in range(1, 21)]),
    (("n", "f"), [{"n": k // 3, "f": (1.0, 2.0, 2.5)[k % 3], "v": k} for k in range(30)]),
])
def test_a_last_or_only_float_key_column_loads_again(key, rows) -> None:
    table = _Table("app.fl", key, rows, types={"f": "float", "n": "int", "v": "int"})
    read = _bounded(keyset_stream(MySqlDouble(table), table.tabledef(), batch_size=4,
                                  dialect=_MY), table)
    assert len(read) == len(rows)


# ---------------------------------------------------------------------------
# Validation: a TIME / BIT / FLOAT key part keeps one exact statement on the source
# ---------------------------------------------------------------------------


def _whole_table_sums(double: MySqlDouble) -> int:
    return sum(sql.startswith("SELECT COALESCE(SUM(") and "ck_tok" not in sql
               and "page_tok" not in sql for sql, _ in double.executed)


@pytest.mark.parametrize("make", [_times, _bits, lambda: _one_column("time"),
                                  lambda: _one_column("bit"), lambda: _one_column("float")])
def test_a_time_bit_or_float_key_part_is_checksummed_in_one_statement(make) -> None:
    table = make()
    double = _DriverDouble(table)
    stop = _poll_limit(table)

    def _beat() -> None:
        assert not stop(), "the pages never ended"

    total = v._source_checksum(double, table.tabledef(), 3, _beat, dialect=_MY,
                               expected_rows=len(table.rows))
    assert total == str(sum(_tok(r) for r in table.rows))
    assert _whole_table_sums(double) == 1
    assert not any("ck_tok" in sql or "page_tok" in sql or "STATISTICS" in sql
                   for sql, _ in double.executed)


@pytest.mark.parametrize("mode", [ValidationMode.ROW_COUNT, ValidationMode.CHECKSUM])
def test_a_negative_time_key_matches_end_to_end(mode) -> None:
    table = _times()
    target = _Table(table.name, table.key, table.rows, types=table.types)
    report = _validator(_DriverDouble(table), _TimedPg(target), reconcile_page_size=4).validate(
        _SOURCE, _TARGET, [table.tabledef()], mode, should_cancel=_poll_limit(table))
    assert report.is_match is True, report.items[0].error


# ---------------------------------------------------------------------------
# Bounded pages: stop at the first page past the snapshot's COUNT(*)
# ---------------------------------------------------------------------------


def _rereading(monkeypatch) -> None:
    """Put back the driver's binding and let the pager page a TIME / BIT key, as G138 did."""
    monkeypatch.setattr(mysql_dialect, "_cursor_value", lambda _type, value: value)
    monkeypatch.setattr(v, "_MYSQL_UNPAGED_KEY_TYPES", frozenset({"float"}))


@pytest.mark.parametrize("make", [lambda: _times(negative_half_hours=True), _bits])
@pytest.mark.parametrize("page", [2, 5, 7])
def test_mysql_pages_that_read_rows_again_end_in_bounded_time_and_fall_back(
    make, page, monkeypatch, caplog
) -> None:
    _rereading(monkeypatch)
    table = make()
    double = _DriverDouble(table)
    expected = len(table.rows)
    beats = {"n": 0}

    def _beat() -> None:  # fail, never hang: (id, BIT) at page 2 cycled forever at G138
        beats["n"] += 1
        assert beats["n"] <= 10 * expected, "the pages never ended"

    with caplog.at_level("WARNING", logger="dsql_migrator.core.validator"):
        total = v._source_checksum(double, table.tabledef(), page, _beat, dialect=_MY,
                                   expected_rows=expected)
    assert total == str(sum(_tok(r) for r in table.rows))  # the one exact statement
    assert _whole_table_sums(double) == 1
    pages = sum("SUM(ck_tok)" in sql for sql, _ in double.executed)
    assert pages <= expected // page + 1
    assert f"the source's keyset pages of {table.name} cannot be trusted" in caplog.text
    assert "read in one statement instead" in caplog.text


def test_a_cycle_of_pages_longer_than_one_stops_at_the_count() -> None:
    # Two boundaries in turn: the stall check (one page) cannot see it; the count does.
    table = _order_items().tabledef()
    cycle = [(5, 2, "1", "1"), (5, 2, "2", "2")] * 10
    conn = _Scripted(cycle)
    with pytest.raises(PagedReadError, match="had read 6 rows, more than the 5"):
        v._composite_pages_pg(conn, table, table.primary_key, 2, checksum=True,
                              side="source", expected_rows=5)
    assert len(conn.executed) == 3
    # Without a count to hold the pages to, only the script's end stops them.
    unbounded = _Scripted(cycle)
    assert v._composite_pages_pg(unbounded, table, table.primary_key, 2, checksum=True).rows == 40


def test_a_postgresql_source_counts_before_its_pages_and_errors_on_an_overrun() -> None:
    table = _order_items(9)
    source = PgDouble(table)
    v._source_checksum_for(_PG, _PgSqla(source), table.tabledef(), 4)
    statements = [sql for sql, _ in source.executed]
    first_page = next(i for i, sql in enumerate(statements) if "ck_k0" in sql)
    assert any(sql.startswith("SELECT COUNT(*)") for sql in statements[:first_page])

    class _Undercounts(PgDouble):
        def resolve(self, statement: Any, params: Any) -> Any:
            row = super().resolve(statement, params)
            return (5,) if self.executed[-1][0].startswith("SELECT COUNT(*)") else row

    undercounted = _Undercounts(table)
    with pytest.raises(PagedReadError, match="had read 8 rows, more than the 5"):
        v._source_checksum_for(_PG, _PgSqla(undercounted), table.tabledef(), 4)
    assert sum("ck_k0" in sql for sql, _ in undercounted.executed) == 2  # stopped at once


def test_a_mysql_overrun_reaches_the_report_as_a_match_not_an_error(monkeypatch) -> None:
    _rereading(monkeypatch)
    table = _times(negative_half_hours=True)
    target = _Table(table.name, table.key, table.rows, types=table.types)
    source = _DriverDouble(table)
    item = _validator(source, _TimedPg(target), reconcile_page_size=5).validate(
        _SOURCE, _TARGET, [table.tabledef()], ValidationMode.CHECKSUM,
        should_cancel=_poll_limit(table)).items[0]
    assert item.error is None and item.matched is True and item.checksum_match is True
    assert _whole_table_sums(source) == 1  # the pages were given up for the one statement


# ---------------------------------------------------------------------------
# A failed orphan check: kept apart from the comparison
# ---------------------------------------------------------------------------


_TYPE_CLASH = _SqlState("42883", "operator does not exist: integer = text")


def _failed_orphan_report(*, missing_row: bool) -> ValidationReport:
    child = _order_items(23)
    orders = _Table("app.orders", ("id",), [{"id": i} for i in range(1000, 1006)])
    target_child = _order_items(23)
    if missing_row:
        target_child.rows.pop(7)
    target = PgDouble(target_child, orders, fail={"WITH pg AS MATERIALIZED": _TYPE_CLASH})
    return _validator(MySqlDouble(child, orders), target, reconcile_page_size=5).validate(
        _SOURCE, _TARGET, [_with_fk(child), orders.tabledef()], ValidationMode.CHECKSUM,
        check_orphans=True)


def test_a_failed_orphan_check_does_not_hide_a_real_mismatch() -> None:
    from dsql_migrator.ui.validation import _failure_reasons, _table_row

    report = _failed_orphan_report(missing_row=True)
    item = {i.table: i for i in report.items}["app.order_items"]
    assert item.error is None and item.matched is False
    assert (item.source_row_count, item.target_row_count, item.row_count_match) == (23, 22, False)
    assert item.orphan_check_error == (
        "foreign key fk_order -> app.orders (_SqlState: operator does not exist: integer = text)")
    reasons = _failure_reasons(item)
    assert reasons[0] == "Row count differs — source 23, target 22."
    assert reasons[-1].startswith("The orphan check could not run: foreign key fk_order")
    row = _table_row(item)
    assert (row["source_rows"], row["target_rows"]) == ("23", "22")
    assert row["row_count"]["text"] == "mismatch" and row["result"]["text"] == "mismatch"
    assert report.is_match is False


def test_a_failed_orphan_check_alone_is_not_a_match_and_says_why() -> None:
    from dsql_migrator.ui.validation import _failure_reasons

    report = _failed_orphan_report(missing_row=False)
    item = {i.table: i for i in report.items}["app.order_items"]
    assert (item.row_count_match, item.checksum_match, item.matched) == (True, True, False)
    assert _failure_reasons(item) == [
        "The orphan check could not run: foreign key fk_order -> app.orders (_SqlState: "
        "operator does not exist: integer = text). Its orphan rows were not counted, so the "
        "table is not matched (see Orphan records)."]
    assert report.is_match is False


def test_every_failed_foreign_key_of_a_table_is_named() -> None:
    child = _order_items(10)
    orders = _Table("app.orders", ("id",), [{"id": i} for i in range(1000, 1006)])
    lines = _Table("app.lines", ("id",), [{"id": i} for i in range(4)])
    tabledef = child.tabledef().model_copy(update={"foreign_keys": [
        ForeignKeyDef(name="fk_order", columns=["order_id"], referenced_table="app.orders",
                      referenced_columns=["id"]),
        ForeignKeyDef(name="fk_line", columns=["line_no"], referenced_table="app.lines",
                      referenced_columns=["id"]),
    ]})
    target = PgDouble(child, orders, lines, fail={"WITH pg AS MATERIALIZED": _TYPE_CLASH})
    item = _validator(MySqlDouble(child), target).validate(
        _SOURCE, _TARGET, [tabledef], check_orphans=True).items[0]
    assert item.orphan_check_error == (
        "foreign key fk_order -> app.orders (_SqlState: operator does not exist: integer = "
        "text); foreign key fk_line -> app.lines (_SqlState: operator does not exist: "
        "integer = text)")


def test_the_orphan_section_lists_the_failed_check() -> None:
    from dsql_migrator.ui.validation import _render_orphans
    from tests.test_ui_validation import _ScreenUi

    ui = _ScreenUi()
    _render_orphans(ui, _failed_orphan_report(missing_row=False))
    body = " ".join(ui.texts)
    assert "The orphan check could not run for 1 table(s)" in body
    assert ("app.order_items: foreign key fk_order -> app.orders (_SqlState: operator does "
            "not exist: integer = text). Their orphan rows were not counted") in body
    assert "No orphan records found in the foreign keys that were checked." in ui.texts
    assert "No orphan records found." not in ui.texts


def test_the_text_report_names_the_failed_check() -> None:
    text = render_text_report(_failed_orphan_report(missing_row=True))
    assert ("- app.order_items: source=23 target=22 (row count MISMATCH), checksum MISMATCH, "
            "orphan check could not run, [MISMATCH]") in text
    assert "Orphan check: performed (could not run for 1 table(s), so they are not matched)" in text
    assert ("- could not run: app.order_items: foreign key fk_order -> app.orders "
            "(_SqlState: operator does not exist: integer = text)") in text


def test_an_unset_orphan_check_error_stays_out_of_the_payload() -> None:
    item = TableValidationResult(table="app.a", source_row_count=1, target_row_count=1,
                                 row_count_match=True, matched=True)
    assert "orphan_check_error" not in item.model_dump_json()  # an older build still loads it
    failed = item.model_copy(update={"matched": False, "orphan_check_error": "foreign key f"})
    assert TableValidationResult.model_validate_json(failed.model_dump_json()) == failed


def _dlq_report(**orders: object) -> ValidationReport:
    return ValidationReport.build(
        mode=ValidationMode.CHECKSUM, orphan_check_performed=True,
        items=[
            TableValidationResult(table="orders", source_row_count=1, target_row_count=1,
                                  row_count_match=True, checksum_match=True, matched=False,
                                  **orders),
            TableValidationResult(table="customers", source_row_count=1, target_row_count=1,
                                  row_count_match=True, checksum_match=True, matched=True),
        ],
    )


def test_a_failed_orphan_check_is_kept_out_of_the_dead_letter_exit() -> None:
    from tests.test_ui_validation import _g1_gate, _g1_render

    failed = _dlq_report(orphan_check_error="foreign key fk -> customers (boom)")
    body = _g1_render(failed, _g1_gate(failed), recheck_provider=lambda _t: None).body()
    assert "Validation could not finish comparing" not in body
    assert "The orphan check could not run for 1 table(s)" in body
    # ...while a comparison that could not finish still gets its exit.
    errored = _dlq_report(error="QueryCanceled: transaction age limit of 300s exceeded")
    body = _g1_render(errored, _g1_gate(errored), recheck_provider=lambda _t: None).body()
    assert "Validation could not finish comparing 1 table(s)" in body


def test_a_failed_orphan_check_is_never_an_explained_gap() -> None:
    from dsql_migrator.ui.validation import cutover_release_state, summarize_validation

    short = dict(table="orders", source_row_count=5, target_row_count=4, row_count_match=False,
                 matched=False, rows_quarantined=1)
    explained = ValidationReport.build(mode=ValidationMode.ROW_COUNT,
                                       items=[TableValidationResult(**short)])
    assert cutover_release_state(summarize_validation(explained)) == "acceptable"
    unchecked = ValidationReport.build(mode=ValidationMode.ROW_COUNT, items=[
        TableValidationResult(**short, orphan_check_error="foreign key fk -> p (boom)")])
    assert cutover_release_state(summarize_validation(unchecked)) == "blocked"


def test_a_recheck_replaces_a_tables_failed_orphan_check() -> None:
    from dsql_migrator.ui.validation import merge_revalidated

    report = _dlq_report(orphan_check_error="foreign key fk -> customers (boom)")
    fixed = TableValidationResult(table="orders", source_row_count=1, target_row_count=1,
                                  row_count_match=True, checksum_match=True, matched=True)
    merged = merge_revalidated(report, [fixed], orphan_findings=[], orphan_unchecked=[])
    assert merged.items[0].orphan_check_error is None and merged.is_match is True


# ---------------------------------------------------------------------------
# The manual
# ---------------------------------------------------------------------------


_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("lang, float_rule, stale", [
    ("en", "followed by another key column", "shortest decimal"),
    ("ko", "뒤에 다른 키 컬럼이 오는", "가장 짧은"),
    ("ja", "後ろに別のキー列が続く", "最短の"),
])
def test_the_manual_says_what_g140_changed(lang, float_rule, stale) -> None:
    full_load = (_ROOT / f"docs/manual/{lang}/03-full-load.md").read_text(encoding="utf-8")
    assert float_rule in full_load and stale not in full_load
    assert "`TIME`" in full_load and "`BIT`" in full_load
    validation = (_ROOT / f"docs/manual/{lang}/05-validation.md").read_text(encoding="utf-8")
    assert "`TIME`" in validation and "`BIT`" in validation
    assert "**Orphan records**" in validation and "**Check for orphan records**" in validation
