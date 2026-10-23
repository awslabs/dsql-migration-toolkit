# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G150: zero and invalid MySQL dates in the data.

MySQL can store dates Aurora DSQL has no value for -- the zero date '0000-00-00', a date with
a zero year, month or day ('2020-00-15'), the zero TIMESTAMP and, under ALLOW_INVALID_DATES,
a day the month does not have ('2020-02-31'). PyMySQL returns each as a str, which Aurora
DSQL rejects (22008), so every such row was quarantined and the table aborted after 1,000.

* The mapping (core.mysql_zero_dates): what the bundled Debezium 2.7.4 writes for one, read
  from its jars -- a zero date is NULL in a nullable DATE / DATETIME column and, in a NOT
  NULL one, the column's DEFAULT as Debezium parses it (SMART resolution) or else the epoch;
  the zero TIMESTAMP is the epoch whatever the nullability; a day the month does not have
  stops the source connector (fail mode), and Full Load writes Debezium's substitute, the
  epoch.
* Full Load: the reader maps the driver's str in O(1), counts per column, and the load
  writes ONE WARNING activity line per table (counts and replacements, never a value), on
  every load path: the thread path, in-process reader shards, the process worker and the
  process shards (summed in the parent).
* Validation: the MySQL side renders a DATE / DATETIME / TIMESTAMP through the same mapping
  in SQL (whole table, single-column keyset pages, composite pages, the row-diff sample's
  key); the reconcile listing never takes a temporal key.
* Evaluation: ZERO_DATES (MANUAL, database level) when the GLOBAL sql_mode lets such dates
  in or a column's DEFAULT is the zero date -- the columns, the mapping, a check query per
  table; SQL_MODE no longer carries the date part.
* The manual (en / ko / ja) says so in chapters 2, 3 and 6.

The SQL was run on MySQL 8.0.46 and 8.4.11 while this was written (the CASE renders each
mapped value as the target renders what Full Load writes, and every valid value exactly as
before); the tests here use doubles only: no MySQL, no PostgreSQL, no AWS.
"""

from __future__ import annotations

import dataclasses
import inspect
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.assessor import (
    SQL_MODE_RULE_ID,
    ZERO_DATES_RULE_ID,
    CompatibilityAssessor,
)
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.exporter import TableExporter, ValueConverter
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    ConversionNoteKind,
    SourceInventory,
    SourceType,
    TableDef,
)
from dsql_migrator.core.mysql_zero_dates import (
    EPOCH_DATE,
    EPOCH_DATETIME,
    INVALID,
    ZERO,
    _debezium_default,
    check_query,
    temporal_base,
    zero_date_mapping,
)
from dsql_migrator.core.validation_sql import (
    _mysql_checksum_expr,
    build_mysql_checksum_sql,
    build_mysql_composite_page_sql,
    build_mysql_page_checksum_first_sql,
    build_mysql_page_checksum_next_sql,
    build_mysql_pk_token_sql,
    integer_pk_column,
)
from dsql_migrator.ui.data_migration._full_load_engine import (
    BatchedTableMigrator,
    TableLoadResult,
    _record_zero_dates,
    _TableWorkerResult,
    _ZeroDateTally,
)
from tests.test_exporter import _FakeConnection, _FakeEngine, _source_config
from tests.test_ui_data_migration import (
    _FakeImporter,
    _FakeWatermarkCapturer,
    _inputs as _ui_inputs,
    _watermark,
)


def _col(name: str, mysql_type: str, *, nullable: bool = True, default=None,
         expression: bool = False) -> ColumnDef:
    return ColumnDef(name=name, mysql_type=mysql_type, nullable=nullable, default=default,
                     default_is_expression=expression)


def _dated_table(name: str = "app.orders") -> TableDef:
    """A table with every kind of temporal column the mapping distinguishes."""
    return TableDef(
        name=name,
        columns=[
            _col("id", "int", nullable=False),
            _col("placed", "date"),                                         # nullable
            _col("due", "date", nullable=False),                            # NOT NULL, none
            _col("born", "date", nullable=False, default="2000-01-01"),     # literal DEFAULT
            _col("seen", "datetime(6)"),
            _col("made", "datetime", nullable=False, default="CURRENT_TIMESTAMP",
                 expression=True),
            _col("at", "timestamp"),
            _col("stamped", "timestamp", nullable=False),
            _col("note", "varchar(20)"),
        ],
        primary_key=["id"],
    )


# ---------------------------------------------------------------------------
# 1. What the bundled CDC writes (core.mysql_zero_dates, pure)
# ---------------------------------------------------------------------------


def test_only_date_datetime_and_timestamp_columns_have_a_mapping() -> None:
    for spelling in ("date", "DATE", "datetime", "datetime(6)", "timestamp", "TIMESTAMP(3)"):
        assert temporal_base(_col("c", spelling)) in ("date", "datetime", "timestamp")
        assert zero_date_mapping(_col("c", spelling)) is not None
    for spelling in ("time", "time(6)", "year", "varchar(10)", "int", "json"):
        assert zero_date_mapping(_col("c", spelling)) is None, spelling
    assert zero_date_mapping(None) is None


@pytest.mark.parametrize(
    ("text", "rule"),
    [
        ("0000-00-00", ZERO), ("2020-00-15", ZERO), ("2020-05-00", ZERO), ("0000-05-15", ZERO),
        ("2020-02-31", INVALID), ("2021-02-29", INVALID), ("2020-04-31", INVALID),
    ],
)
def test_a_date_is_zero_with_any_zero_part_and_invalid_when_the_month_lacks_the_day(
    text: str, rule: str
) -> None:
    # RowDeserializers.deserializeDate: year == 0 || month == 0 || day == 0 -> null (a zero
    # date); otherwise LocalDate.of(...) throws for a day the month does not have.
    nullable = zero_date_mapping(_col("d", "date"))
    assert nullable.value(text) == (rule, None if rule == ZERO else EPOCH_DATE)
    not_null = zero_date_mapping(_col("d", "date", nullable=False))
    assert not_null.value(text) == (rule, EPOCH_DATE)


def test_a_datetime_maps_by_its_date_part_and_loses_the_time_of_day() -> None:
    mapping = zero_date_mapping(_col("dt", "datetime(6)", nullable=False))
    assert mapping.value("0000-00-00 00:00:00.000000") == (ZERO, EPOCH_DATETIME)
    assert mapping.value("2020-00-15 10:11:12.500000") == (ZERO, EPOCH_DATETIME)
    # handleException's substitute: LocalDateTime.of(LocalDate.EPOCH, LocalTime.MIDNIGHT).
    assert mapping.value("2020-02-31 10:11:12.123456") == (INVALID, EPOCH_DATETIME)
    assert zero_date_mapping(_col("dt", "datetime")).value("2020-05-00 10:00:00") == (ZERO, None)


def test_the_zero_timestamp_is_the_epoch_whatever_the_nullability() -> None:
    # deserializeTimestamp(V2) reads the stored 0 as the instant 0, never null.
    for nullable in (True, False):
        mapping = zero_date_mapping(_col("ts", "timestamp(6)", nullable=nullable))
        assert mapping.value("0000-00-00 00:00:00") == (ZERO, EPOCH_DATETIME)
        assert mapping.replacement_text(ZERO) == "1970-01-01 00:00:00 UTC"
        # A TIMESTAMP holds no other invalid value: anything else is not one.
        assert mapping.value("2020-02-31 10:00:00") is None


@pytest.mark.parametrize("text", ["2020-01-15", "2020-01-15 01:02:03", "garbage", ""])
def test_a_date_after_all_or_no_date_is_passed_on(text: str) -> None:
    assert zero_date_mapping(_col("d", "datetime")).value(text) is None


# What BinlogDefaultValueConverter.convertToLocalDate / convertToLocalDateTime make of a
# column DEFAULT: produced by re-creating its two formatters (as read from the 2.7.4
# bytecode) with the JDK alone -- ISO_LOCAL_DATE + optional ' ' ISO_LOCAL_TIME, and
# 'yyyy-MM-dd' + optional ISO_LOCAL_TIME, both SMART -- plus its zero patterns.
_JDK_DATE_DEFAULTS = [
    ("2000-01-01", date(2000, 1, 1)),
    ("0000-00-00", EPOCH_DATE),
    ("2020-00-15", EPOCH_DATE),
    ("2020-05-00", EPOCH_DATE),
    ("0000-05-15", EPOCH_DATE),
    ("2020-02-31", date(2020, 2, 29)),       # SMART: a day past the month's end is its last
    ("2021-02-29", date(2021, 2, 28)),
    ("2020-04-31", date(2020, 4, 30)),
    ("0999-12-31", date(999, 12, 31)),
    ("0", EPOCH_DATE),
    (" 2001-02-03 ", date(2001, 2, 3)),
    ("2000-13-01", EPOCH_DATE),             # unparsable: the fallback
    ("2000-01-32", EPOCH_DATE),
    ("garbage", EPOCH_DATE),
]
_JDK_DATETIME_DEFAULTS = [
    ("2000-01-01 10:11:12", datetime(2000, 1, 1, 10, 11, 12)),
    ("0000-00-00 00:00:00", EPOCH_DATETIME),
    ("2020-02-31 10:00:00", datetime(2020, 2, 29, 10, 0, 0)),
    ("2020-00-15 10:00:00", EPOCH_DATETIME),
    ("0000-05-15 10:00:00", EPOCH_DATETIME),  # year-of-era 0
    ("2000-01-01 10:11:12.123456", datetime(2000, 1, 1, 10, 11, 12, 123456)),
    ("2000-01-01 10:11:12.123", datetime(2000, 1, 1, 10, 11, 12, 123000)),
    ("2000-01-01", datetime(2000, 1, 1)),
    ("1970-01-01 00:00:00", EPOCH_DATETIME),
    ("0999-01-01 00:00:01", datetime(999, 1, 1, 0, 0, 1)),
]


@pytest.mark.parametrize(("default", "expected"), _JDK_DATE_DEFAULTS)
def test_a_not_null_date_takes_its_default_as_debezium_parses_it(default, expected) -> None:
    column = _col("d", "date", nullable=False, default=default)
    assert _debezium_default(column, "date") == expected
    assert zero_date_mapping(column).value("0000-00-00") == (ZERO, expected)


@pytest.mark.parametrize(("default", "expected"), _JDK_DATETIME_DEFAULTS)
def test_a_not_null_datetime_takes_its_default_as_debezium_parses_it(default, expected) -> None:
    column = _col("dt", "datetime(6)", nullable=False, default=default)
    assert _debezium_default(column, "datetime") == expected


def test_an_expression_or_no_default_gives_the_epoch() -> None:
    # The DDL parser turns CURRENT_TIMESTAMP / NOW() into '1970-01-01 00:00:00' and an
    # expression default into none, so the fallback (epoch day 0 / epoch 0) applies.
    for column in (
        _col("dt", "datetime", nullable=False, default="CURRENT_TIMESTAMP", expression=True),
        _col("d", "date", nullable=False, default="curdate()", expression=True),
        _col("d", "date", nullable=False),
    ):
        mapping = zero_date_mapping(column)
        assert mapping.value("0000-00-00") == (ZERO, mapping.invalid)
        assert mapping.zero == (EPOCH_DATE if column.mysql_type == "date" else EPOCH_DATETIME)
    # A nullable column's zero date is NULL whatever its DEFAULT.
    assert zero_date_mapping(_col("d", "date", default="2000-01-01")).zero is None


def test_the_replacement_texts_name_what_was_written_never_a_row_value() -> None:
    born = zero_date_mapping(_col("b", "date", nullable=False, default="2000-01-01"))
    assert (born.replacement_text(ZERO), born.replacement_text(INVALID)) == (
        "2000-01-01", "1970-01-01")
    seen = zero_date_mapping(_col("s", "datetime(6)"))
    assert (seen.replacement_text(ZERO), seen.replacement_text(INVALID)) == (
        "NULL", "1970-01-01 00:00:00")
    early = zero_date_mapping(_col("e", "datetime", nullable=False,
                                   default="0999-01-01 00:00:01.5"))
    assert early.replacement_text(ZERO) == "0999-01-01 00:00:01.500000"  # four year digits


# ---------------------------------------------------------------------------
# 2. Full Load writes what CDC writes, and counts it
# ---------------------------------------------------------------------------


def _row(**values) -> dict:
    base = {"id": 1, "placed": None, "due": date(2024, 5, 1), "born": date(1990, 1, 1),
            "seen": None, "made": datetime(2024, 5, 1, 9, 0), "at": None,
            "stamped": datetime(2024, 5, 1, 9, 0), "note": "x"}
    base.update(values)
    return base


def test_the_converter_writes_each_zero_or_invalid_date_as_cdc_does() -> None:
    converter = ValueConverter(_dated_table())
    out = converter.convert_row(_row(
        placed="0000-00-00", due="2020-02-31", born="2020-00-15",
        seen="2020-05-00 10:11:12.500000", made="0000-00-00 00:00:00",
        at="0000-00-00 00:00:00", stamped="0000-00-00 00:00:00",
    ))
    assert out["placed"] is None                                   # nullable -> NULL
    assert out["due"] == EPOCH_DATE                                # invalid -> the epoch
    assert out["born"] == date(2000, 1, 1)                         # NOT NULL -> its DEFAULT
    assert out["seen"] is None
    assert out["made"] == datetime(1970, 1, 1)                     # CURRENT_TIMESTAMP -> epoch
    assert out["made"].tzinfo is None                              # a timestamp, as ever
    epoch_utc = datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert out["at"] == epoch_utc and out["stamped"] == epoch_utc  # timestamptz instants
    assert out["note"] == "x"
    assert converter.zero_date_entries() == (
        ("at", ZERO, "1970-01-01 00:00:00 UTC", 1),
        ("born", ZERO, "2000-01-01", 1),
        ("due", INVALID, "1970-01-01", 1),
        ("made", ZERO, "1970-01-01 00:00:00", 1),
        ("placed", ZERO, "NULL", 1),
        ("seen", ZERO, "NULL", 1),
        ("stamped", ZERO, "1970-01-01 00:00:00 UTC", 1),
    )


def test_real_dates_are_converted_as_before_and_counted_never() -> None:
    converter = ValueConverter(_dated_table())
    row = _row(placed=date(2024, 2, 29), seen=datetime(2024, 2, 29, 23, 59, 59, 123456),
               at=datetime(2024, 2, 29, 12, 0))
    out = converter.convert_row(row)
    assert out["placed"] == date(2024, 2, 29) and out["due"] == date(2024, 5, 1)
    assert out["seen"] == datetime(2024, 2, 29, 23, 59, 59, 123456)
    assert out["at"] == datetime(2024, 2, 29, 12, 0, tzinfo=timezone.utc)
    assert converter.zero_date_entries() == () and converter.zero_dates == {}


def test_the_mapping_follows_the_source_type_whatever_the_target_type() -> None:
    # CDC's mapping happens in the source connector, before any target type is known.
    converter = ValueConverter(_dated_table(), target_types={"placed": "text"})
    assert converter.convert_value("placed", "0000-00-00") is None
    assert converter.convert_value("due", "0000-00-00") == EPOCH_DATE


def test_counts_add_up_per_column_and_kind() -> None:
    converter = ValueConverter(_dated_table())
    for text in ("0000-00-00", "2020-00-01", "2020-02-30"):
        converter.convert_value("placed", text)
    assert converter.zero_dates == {("placed", ZERO): 2, ("placed", INVALID): 1}


def _stream(rows, *, table=None, consume=None):
    connection = _FakeConnection(rows)
    exporter = TableExporter(engine_factory=lambda _cfg: _FakeEngine(connection), batch_size=2)
    reported: list = []
    stream = exporter.stream_converted_rows(
        _source_config(), table or _dated_table(), on_zero_dates=reported.append)
    out = list(stream) if consume is None else consume(stream)
    return out, reported, connection


def test_the_stream_reports_once_at_its_end_and_reads_nothing_more() -> None:
    rows = [_row(id=i, placed="0000-00-00" if i % 2 else date(2024, 1, i)) for i in range(1, 6)]
    out, reported, connection = _stream(rows)
    assert [r["placed"] for r in out] == [None, date(2024, 1, 2), None, date(2024, 1, 4), None]
    assert reported == [(("placed", ZERO, "NULL", 3),)]
    # The same statements as a table without one: no extra source query.
    _out, quiet, plain = _stream([_row(id=i) for i in range(1, 6)])
    assert quiet == []
    assert [sql for sql, _p in connection.executed] == [sql for sql, _p in plain.executed]


def test_a_stream_that_stops_early_reports_nothing() -> None:
    rows = [_row(id=i, placed="0000-00-00") for i in range(1, 6)]

    def _two(stream):
        taken = [next(stream), next(stream)]
        stream.close()
        return taken

    out, reported, _conn = _stream(rows, consume=_two)
    assert len(out) == 2 and reported == []


# ---------------------------------------------------------------------------
# 3. One WARNING line per table, on every load path
# ---------------------------------------------------------------------------


def _capture(monkeypatch) -> list:
    lines: list = []
    monkeypatch.setattr(engine, "log_activity", lambda _cat, action, **kw: lines.append(
        (action, kw.get("status"), kw.get("target"), kw.get("detail"))))
    return lines


def _warnings(lines) -> list:
    return [line for line in lines if line[1] is engine.ActivityStatus.WARNING]


def test_the_tally_sums_entries_from_several_streams() -> None:
    tally = _ZeroDateTally()
    tally.add((("placed", ZERO, "NULL", 2),))
    tally.add((("placed", ZERO, "NULL", 3), ("due", INVALID, "1970-01-01", 1)))
    tally.add(())
    assert tally.entries() == (("due", INVALID, "1970-01-01", 1), ("placed", ZERO, "NULL", 5))


def test_one_warning_line_names_the_counts_and_what_they_became(monkeypatch) -> None:
    lines = _capture(monkeypatch)
    _record_zero_dates("app.orders", [
        ("placed", ZERO, "NULL", 1200), ("due", INVALID, "1970-01-01", 3),
        ("placed", ZERO, "NULL", 34),            # another shard's share of the same column
    ])
    ((action, status, target, detail),) = lines
    assert (action, status, target) == ("load table", engine.ActivityStatus.WARNING, "app.orders")
    assert detail.startswith("1,237 zero or invalid date value(s)")
    assert "due: 3 invalid date(s) as 1970-01-01; placed: 1,234 zero date(s) as NULL" in detail
    assert "Validation compares the source the same way" in detail
    # CDC does not write an invalid day as Full Load does -- its source connector stops at
    # one -- so the line says what to do before Start CDC; only zero dates need no such word.
    assert ("CDC itself stops at the first change to a row that holds one: in a Full load + "
            "CDC migration, correct those rows on the source before Start CDC.") in detail
    _record_zero_dates("app.orders", ())
    _record_zero_dates("app.orders", None)
    assert len(lines) == 1
    _record_zero_dates("app.orders", [("placed", ZERO, "NULL", 2)])
    assert "Start CDC" not in lines[-1][3] and "stops" not in lines[-1][3]


def test_the_load_results_carry_the_entries_and_default_to_none() -> None:
    entries = (("placed", ZERO, "NULL", 1),)
    assert TableLoadResult(rows_loaded=1, zero_dates=entries).zero_dates == entries
    assert TableLoadResult(rows_loaded=1).zero_dates == ()
    assert _TableWorkerResult(table_name="t", status="DONE", zero_dates=entries).zero_dates \
        == entries
    assert _TableWorkerResult(table_name="t", status="DONE").zero_dates == ()


class _ShardedExporter(TableExporter):
    """The real reader over a fake connection, planned into two in-process shards."""

    def __init__(self, connection, ranges):
        super().__init__(engine_factory=lambda _cfg: _FakeEngine(connection), batch_size=2)
        self._ranges = ranges

    def plan_pk_shard_ranges(self, conn, table, shards, *, min_rows=0):
        return list(self._ranges)


def _migrator(rows, *, ranges=((None, None),), cdc_coexisting=False):
    table = _dated_table("orders")
    inputs = dataclasses.replace(
        _ui_inputs(), inventory=SourceInventory(tables=[table]), cdc_coexisting=cdc_coexisting)
    migrator = BatchedTableMigrator(
        inputs,
        exporter=_ShardedExporter(_FakeConnection(rows), ranges),  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _i: _FakeImporter(),  # type: ignore[arg-type,return-value]
    )
    return migrator, table


def test_migrate_table_sums_its_streams_and_its_shards() -> None:
    rows = [_row(id=i, placed="0000-00-00", due="2020-02-30" if i == 4 else date(2024, 1, 1))
            for i in range(1, 7)]
    migrator, table = _migrator(rows)
    result = migrator.migrate_table(table)
    assert result.rows_loaded == 6
    assert result.zero_dates == (("due", INVALID, "1970-01-01", 1), ("placed", ZERO, "NULL", 6))
    # Two in-process reader shards (a CDC-coexisting load): the same table total.
    migrator, table = _migrator(rows, ranges=[(None, 4), (4, None)], cdc_coexisting=True)
    assert migrator.migrate_table(table).zero_dates == result.zero_dates


class _Handle:
    cancelled = False

    def update(self, _fn):
        pass


class _ResultMigrator:
    source_type = SourceType.MYSQL

    def __init__(self, result):
        self.result = result

    def migrate_table(self, table, **_kwargs):
        return self.result


def test_the_thread_path_logs_one_warning_for_a_loaded_table(monkeypatch) -> None:
    lines = _capture(monkeypatch)
    entries = (("placed", ZERO, "NULL", 6),)
    outcome = engine._migrate_one_table(
        _Handle(), "job-1", _dated_table("orders"),
        _ResultMigrator(TableLoadResult(rows_loaded=6, zero_dates=entries)), ErrorLogStore())
    assert outcome is engine._TableLoadOutcome.LOADED
    ((_action, _status, target, detail),) = _warnings(lines)
    assert target == "orders" and "placed: 6 zero date(s) as NULL" in detail
    # A table without any logs no such line.
    lines.clear()
    engine._migrate_one_table(_Handle(), "job-1", _dated_table("orders"),
                              _ResultMigrator(TableLoadResult(rows_loaded=6)), ErrorLogStore())
    assert _warnings(lines) == []


def test_the_process_workers_hand_their_entries_to_the_parent(monkeypatch) -> None:
    entries = (("placed", ZERO, "NULL", 6),)

    class _Migrator:
        def __init__(self, inputs, **_k):
            pass

        def migrate_table(self, table, **_kwargs):
            return TableLoadResult(rows_loaded=6, zero_dates=entries)

    monkeypatch.setattr(engine, "BatchedTableMigrator", _Migrator)
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    whole = engine._migrate_one_table_in_process(engine._TableWorkerArgs(
        job_id="j", table=_dated_table("orders"), inputs=_ui_inputs()))
    assert whole.status == "DONE" and whole.zero_dates == entries
    # The shard worker reads through its own stream and reports that attempt's count.
    source = inspect.getsource(engine._migrate_shard_in_process)
    assert "on_zero_dates=_zero_dates[0].add" in source
    assert "zero_dates=_zero_dates[0].entries()" in source


def test_a_shard_worker_reports_the_dates_its_stream_mapped(monkeypatch) -> None:
    from tests.test_g137_mysql_reader_shards import _run_shard

    class _Exporter:
        def stream_converted_rows(self, conn, table, *, on_zero_dates=None, **_kw):
            def _rows():
                yield {"id": 1, "email": "a"}
                on_zero_dates((("placed", ZERO, "NULL", 2),))
            return _rows()

    result, _ = _run_shard(monkeypatch, shared=False, exporter=_Exporter())
    assert result.status == "DONE"
    assert result.zero_dates == (("placed", ZERO, "NULL", 2),)


def test_the_parent_sums_a_split_tables_parts_into_one_line(monkeypatch) -> None:
    from tests.test_full_load_overlay import _InlineExecutor
    from tests.test_g137_mysql_reader_shards import (
        _FOUR,
        _Handle as _JobHandle,
        _IndexImporter,
        _inputs,
        _Planner,
        _table,
        _wm,
    )

    lines = _capture(monkeypatch)

    def _shard(args):
        return _TableWorkerResult(
            table_name=args.table.name, status="DONE", rows_loaded=10,
            shard_index=args.shard_index,
            zero_dates=(("placed", ZERO, "NULL", args.shard_index + 1),))

    monkeypatch.setattr(engine, "ProcessPoolExecutor", _InlineExecutor)
    monkeypatch.setattr(engine, "_migrate_shard_in_process", _shard)
    monkeypatch.setattr(engine, "_PROCESS_LAUNCH_STAGGER_SECONDS", 0)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "2")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS", "1000")
    _InlineExecutor.submitted = []
    table = _table()
    migrator = BatchedTableMigrator(
        _inputs([table]), exporter=_Planner({table.name: _FOUR}), watermark_capturer=object(),
        importer_factory=lambda _i: _IndexImporter(), table_recreator=lambda t: [],
        target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
        target_generated_reader=lambda t: set(),
        target_unique_keys_reader=lambda t: [list(t.primary_key)],
    )
    handle = _JobHandle([table.name])
    engine._migrate_tables_in_parallel(handle, "j", [table], migrator, ErrorLogStore(),
                                       watermark=_wm(), fresh_watermark=True)
    assert {c.chunk_id: c.status for c in handle.job.chunks} == {table.name: "DONE"}
    ((_action, _status, target, detail),) = _warnings(lines)
    assert target == table.name
    assert detail.startswith("10 zero or invalid date value(s)")    # 1 + 2 + 3 + 4
    assert "placed: 10 zero date(s) as NULL" in detail


# ---------------------------------------------------------------------------
# 4. Validation renders the source the same way
# ---------------------------------------------------------------------------


def test_each_temporal_render_maps_zero_and_invalid_dates_first() -> None:
    table = _dated_table()
    by_name = {c.name: c for c in table.columns}
    placed = _mysql_checksum_expr(by_name["placed"])
    assert placed == (
        "CAST(CASE WHEN (YEAR(`placed`) = 0 OR MONTH(`placed`) = 0 OR DAYOFMONTH(`placed`) = 0) "
        "THEN NULL WHEN DAYOFMONTH(`placed`) > DAYOFMONTH(LAST_DAY(`placed`)) "
        "THEN DATE'1970-01-01' ELSE `placed` END AS CHAR)"
    )
    born = _mysql_checksum_expr(by_name["born"])
    assert "THEN DATE'2000-01-01' WHEN" in born and born.endswith("ELSE `born` END AS CHAR)")
    seen = _mysql_checksum_expr(by_name["seen"])
    assert seen.startswith("DATE_FORMAT(CASE WHEN (YEAR(`seen`) = 0")
    assert "THEN NULL WHEN" in seen and "THEN TIMESTAMP'1970-01-01 00:00:00' ELSE" in seen
    assert seen.endswith("ELSE `seen` END, '%Y-%m-%d %H:%i:%s.%f')")
    stamped = _mysql_checksum_expr(by_name["stamped"])
    assert stamped == (
        "DATE_FORMAT(CASE WHEN (YEAR(`stamped`) = 0 OR MONTH(`stamped`) = 0 OR "
        "DAYOFMONTH(`stamped`) = 0) THEN TIMESTAMP'1970-01-01 00:00:00' ELSE `stamped` END, "
        "'%Y-%m-%d %H:%i:%s.%f')"
    )
    # A nullable TIMESTAMP's zero is the epoch too, and no other column is touched.
    assert "THEN TIMESTAMP'1970-01-01 00:00:00' ELSE `at` END" in _mysql_checksum_expr(
        by_name["at"])
    assert _mysql_checksum_expr(by_name["note"]) == "CAST(`note` AS CHAR)"
    assert _mysql_checksum_expr(ColumnDef(name="t", mysql_type="time")) == \
        "TIME_FORMAT(`t`, '%H:%i:%s.%f')"


def test_the_sql_writes_the_same_replacements_the_reader_writes() -> None:
    # One mapping drives both: the literal each CASE branch writes is the value Full Load
    # binds for the same stored value, so the target's render of it is what the source renders.
    for column in _dated_table().columns:
        mapping = zero_date_mapping(column)
        if mapping is None:
            continue
        sql = _mysql_checksum_expr(column)
        for value in (mapping.zero, mapping.invalid):
            if value is None:
                assert " THEN NULL " in sql
            elif isinstance(value, datetime):
                assert f"TIMESTAMP'{value:%Y-%m-%d %H:%M:%S}'" in sql
            else:
                assert f"DATE'{value.isoformat()}'" in sql


def test_every_mysql_pager_renders_the_mapping() -> None:
    table = _dated_table()
    mapped = "THEN DATE'2000-01-01'"
    composite = table.model_copy(update={"primary_key": ["id", "due"]})
    for sql in (
        build_mysql_checksum_sql(table),                       # the whole-table fallback
        build_mysql_page_checksum_first_sql(table, "id"),      # single-column keyset
        build_mysql_page_checksum_next_sql(table, "id"),
        build_mysql_composite_page_sql(composite),             # composite key pages
        build_mysql_pk_token_sql(table, "id"),                 # the row-diff sample
    ):
        assert mapped in sql and "ELSE `stamped` END" in sql
    # A key column is rendered as stored (G154): Full Load does not map it.
    assert "YEAR(`due`)" not in build_mysql_composite_page_sql(composite)
    assert "YEAR(`due`) = 0" in build_mysql_checksum_sql(table)


def test_the_row_diff_sample_selects_a_temporal_key_as_the_target_holds_it() -> None:
    # G154: Full Load leaves a key column's zero or invalid date as stored (its row is
    # quarantined), so the sample selects the key as stored, the key the target holds.
    table = TableDef(name="app.days", columns=[_col("pk", "date", nullable=False),
                                                _col("v", "int")], primary_key=["pk"])
    sql = build_mysql_pk_token_sql(table, "pk")
    assert sql.startswith("SELECT `pk` AS pk, ") and "YEAR(`pk`)" not in sql
    assert sql.endswith("ORDER BY `pk` LIMIT :sample_size")
    # A key of another type is selected and ordered exactly as before.
    plain = build_mysql_pk_token_sql(_dated_table(), "id")
    assert plain.startswith("SELECT `id` AS pk, ") and plain.endswith(
        "ORDER BY `id` LIMIT :sample_size")
    # The reconcile listing pages integer keys only: a temporal key never reaches it.
    assert integer_pk_column(table) is None


# ---------------------------------------------------------------------------
# 5. Evaluation: ZERO_DATES
# ---------------------------------------------------------------------------


_STRICT = (
    "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
    "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)


def _items(tables, settings) -> dict:
    report = CompatibilityAssessor().assess(SourceInventory(tables=tables, server_settings=settings))
    return {i.rule_id: i for i in report.items if i.kind == "DATABASE"}


def _plain(name="app.t", *columns) -> TableDef:
    return TableDef(name=name, columns=[_col("id", "int", nullable=False), *columns],
                    primary_key=["id"])


def test_a_lenient_mode_lists_the_columns_the_mapping_and_a_query_per_table() -> None:
    tables = [_dated_table(), _plain("app.events", _col("at", "datetime(6)"))]
    item = _items(tables, {"sql_mode": "NO_ENGINE_SUBSTITUTION"})[ZERO_DATES_RULE_ID]
    assert item.classification is Classification.MANUAL and item.kind == "DATABASE"
    assert item.object_name == "zero and invalid dates" and item.effort.value == "MEDIUM"
    (concern,) = item.concerns
    assert concern.note_kind is ConversionNoteKind.LOSS and concern.risk == item.risk
    risk = item.risk
    assert risk.startswith("The source's global sql_mode is 'NO_ENGINE_SUBSTITUTION'")
    assert "(8 columns: app.orders.placed, app.orders.due, app.orders.born" in risk
    assert "app.events.at" in risk and "app.orders.note" not in risk
    assert ("a zero date, or a zero month or day, becomes NULL where the column allows NULL, "
            "and otherwise the column's DEFAULT when that is a date, or else 1970-01-01") in risk
    assert "the zero TIMESTAMP becomes 1970-01-01 00:00:00 UTC" in risk
    assert "Validation compares the source the same way" in risk
    # No ALLOW_INVALID_DATES: neither the invalid day nor the CDC stop is claimed.
    assert "'2024-02-31'" not in risk and "CDC cannot carry" not in risk
    rec = item.recommendation
    assert "Evaluation reads no rows" in rec
    assert check_query(tables[0]) in rec and check_query(tables[1]) in rec
    assert "SQLSTATE 22008" in rec and "or the DB parameter group of an instance" in rec


def test_allow_invalid_dates_names_the_cdc_stop_and_the_way_out() -> None:
    item = _items([_dated_table()], {"sql_mode": _STRICT + ",ALLOW_INVALID_DATES"})[
        ZERO_DATES_RULE_ID]
    assert "(ALLOW_INVALID_DATES): a day the month does not have ('2024-02-31')." in item.risk
    assert "a day the month does not have becomes 1970-01-01" in item.risk
    assert ("CDC cannot carry a day the month does not have: the source connector stops at "
            "the first change to a row that holds one, in any table the binary log records")\
        in item.risk
    # Strict mode with both zero flags refuses zero dates: only the invalid day is mapped.
    assert "a zero date" not in item.risk
    assert "before CDC starts: a CDC that stopped at one starts again only from a newer start " \
           "point" in item.recommendation


def test_a_zero_default_is_reported_without_a_lenient_mode_or_a_read_mode() -> None:
    zero = _plain("app.t", _col("d", "date", nullable=False, default="0000-00-00"),
                  _col("e", "date"))
    for settings in ({"sql_mode": _STRICT}, {}):
        item = _items([zero], settings)[ZERO_DATES_RULE_ID]
        assert item.risk.startswith("A column whose DEFAULT is the zero date holds one in every "
                                    "row written without a value for it (1 column: app.t.d)")
        assert "(2 columns: app.t.d, app.t.e)" in item.risk
        assert "a zero date becomes NULL" in item.risk
        assert "sql_mode in" not in item.recommendation        # nothing to change there
    # An expression DEFAULT, or a real date, is no zero date.
    for column in (_col("d", "datetime", default="CURRENT_TIMESTAMP", expression=True),
                   _col("d", "date", default="2000-01-01")):
        assert ZERO_DATES_RULE_ID not in _items([_plain("app.t", column)], {"sql_mode": _STRICT})


def test_nothing_without_a_date_column_or_with_a_strict_mode_and_no_zero_default() -> None:
    assert ZERO_DATES_RULE_ID not in _items([_plain()], {"sql_mode": ""})
    assert ZERO_DATES_RULE_ID not in _items([_dated_table()], {"sql_mode": _STRICT})
    assert ZERO_DATES_RULE_ID not in _items([_dated_table()], {})              # not read
    assert ZERO_DATES_RULE_ID not in _items([_dated_table()], {"sql_mode": "1073741824"})


def test_sql_mode_no_longer_carries_the_date_part() -> None:
    items = _items([_dated_table()], {"sql_mode": ""})
    assert "date" not in items[SQL_MODE_RULE_ID].risk.lower()
    assert ZERO_DATES_RULE_ID in items
    assert SQL_MODE_RULE_ID not in _items([_dated_table()], {"sql_mode": "ONLY_FULL_GROUP_BY"})


def test_the_check_query_tests_every_date_column_and_quotes_names() -> None:
    table = TableDef(name="my db.or`ders", columns=[
        _col("id", "int", nullable=False), _col("d`t", "date"), _col("ts", "timestamp"),
        _col("n", "varchar(5)")], primary_key=["id"])
    assert check_query(table) == (
        "SELECT COUNT(*) FROM `my db`.`or``ders` WHERE "
        "(YEAR(`d``t`) = 0 OR MONTH(`d``t`) = 0 OR DAYOFMONTH(`d``t`) = 0) OR "
        "DAYOFMONTH(`d``t`) > DAYOFMONTH(LAST_DAY(`d``t`)) OR "
        "(YEAR(`ts`) = 0 OR MONTH(`ts`) = 0 OR DAYOFMONTH(`ts`) = 0);"
    )
    assert check_query(_plain()) is None


def test_only_five_tables_queries_are_spelled_out() -> None:
    tables = [_plain(f"app.t{i}", _col("d", "date")) for i in range(7)]
    rec = _items(tables, {"sql_mode": ""})[ZERO_DATES_RULE_ID].recommendation
    assert check_query(tables[4]) in rec and check_query(tables[5]) not in rec
    assert "-- and 2 more tables, the same test over their DATE, DATETIME and TIMESTAMP " \
           "columns." in rec


# ---------------------------------------------------------------------------
# 6. The manual says the same in every language
# ---------------------------------------------------------------------------


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"


def _chapter(language: str, name: str) -> str:
    return " ".join((_MANUAL / language / name).read_text(encoding="utf-8").split())


_LIMITATIONS_ANCHOR = {
    "en": "06-limitations.md#62-migration-process-limits",
    "ko": "06-limitations.md#62-마이그레이션-프로세스-한계",
    "ja": "06-limitations.md#62-移行プロセスの制限",
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_limitations_chapter_states_the_mapping_and_the_cdc_stop(language: str) -> None:
    text = _chapter(language, "06-limitations.md")
    section = text.split("## 6.2", 1)[1].split("## 6.3", 1)[0]
    for spelled in ("`0000-00-00`", "`2024-00-15`", "`2024-02-31`", "`ALLOW_INVALID_DATES`",
                    "`ZERO_DATES`", "`1970-01-01`", "`1970-01-01 00:00:00`", "`DEFAULT`",
                    "`TIMESTAMP`", "Validation", "WARNING", "**Start CDC**",
                    "`22008`", "Debezium"):
        assert spelled in section, (language, spelled)


_CDC_STOPS_AT_AN_INVALID_DAY = {
    "en": "at which CDC stops instead",
    "ko": "CDC는 대신 그런 값에서 멈춥니다",
    "ja": "CDC はその代わりにそうした値で停止します",
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_full_load_and_evaluation_chapters_point_at_it(language: str) -> None:
    full_load = _chapter(language, "03-full-load.md").split("## 3.3", 1)[1].split("## 3.4", 1)[0]
    assert "`0000-00-00`" in full_load and "`1970-01-01`" in full_load
    assert _LIMITATIONS_ANCHOR[language] in full_load and "WARNING" in full_load
    # CDC writes nothing for a day the month does not have (its connector stops), so the
    # chapter does not say Full Load writes "what CDC writes" for one.
    assert _CDC_STOPS_AT_AN_INVALID_DAY[language] in full_load
    evaluation = _chapter(language, "02-evaluation-and-schema-conversion.md")
    section = evaluation.split("## 2.1", 1)[1].split("### ", 1)[0]
    assert f"`{ZERO_DATES_RULE_ID}`" in section and _LIMITATIONS_ANCHOR[language] in section
    # The claim G148 made, that such a row cannot be loaded, is gone.
    for claim in ("cannot be loaded as it is", "그대로 로드할 수 없습니다",
                  "そのままではロードできません"):
        assert claim not in section
