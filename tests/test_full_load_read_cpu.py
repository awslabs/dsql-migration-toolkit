# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The MySQL Full Load reader's per-row CPU: the same rows for less work.

The reader is client-CPU-bound (one table read by one process at about one core). Of the
read side's per-row work the tool's own share went down without changing what it yields:

1. ``ValueConverter.row_converter(names)``: each column's conversion is resolved once per
   table (``_value_function``: the type PyMySQL returns takes a fast path, any other value
   goes to ``convert_value`` itself) and a page's rows are converted by position -- zipped
   into a dict, only the columns ``convert_value`` changes visited, a None left as it is.
2. ``keyset_pages(converter=...)``: an SQLAlchemy result is read as its ``Row`` tuples and
   converted in the page (``_page_reader``) -- no ``RowMapping`` and no ``RowMapping.items()``
   per row, the key taken by position; the cursor's told-apart form is built once per page
   and compared only for a key equal to it.
3. ``_estimate_row_bytes``: a naive datetime, a date, a Decimal and a bool take exact-type
   paths (a datetime's, a date's and a bool's str() length without building it) -- tested in
   test_estimate_row_bytes_fast_path.

Every converted row is IDENTICAL (keys, order, types, values, reprs), as are the zero-date
tally and every error, the statements sent and the pages' cursor checks: ``convert_row`` and
``convert_value`` are unchanged and are the reference here, over a corpus of every MySQL type
the converter handles. A value the converter refuses still lets its page's result go before
the snapshot's COMMIT, as when the row stream converted the rows itself. No database: the
SQLAlchemy results are real ``IteratorResult`` / ``Row`` objects over Python values of the
types PyMySQL returns.
"""

from __future__ import annotations

import itertools
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Optional, Sequence

import pytest
from sqlalchemy.engine import IteratorResult
from sqlalchemy.engine.result import SimpleResultMetaData
from sqlalchemy.engine.row import RowMapping

from dsql_migrator.core.exporter import (
    CsvRowWriter,
    ExportError,
    TableExporter,
    ValueConversionError,
    ValueConverter,
    _page_reader,
    _told_apart,
    export_rows,
    keyset_pages,
    keyset_stream,
)
from dsql_migrator.core.exporter_postgres import PostgresValueConverter
from dsql_migrator.core.models import ColumnDef, IndexDef, SourceConnectionConfig, TableDef
from dsql_migrator.core.source_dialect import MySQLSourceDialect, PostgresSourceDialect

_UTC = timezone.utc
_PLUS9 = timezone(timedelta(hours=9))
_MINUS0130 = timezone(-timedelta(hours=1, minutes=30))


class _Str(str):
    pass


class _Bytes(bytes):
    pass


class _DateTime(datetime):
    pass


class _TimeDelta(timedelta):
    pass


_WKB = b"\x01\x01\x00\x00\x00" + b"\x00" * 16
_BAD_TIMES = [timedelta(hours=24, microseconds=1), timedelta(seconds=-1),
              timedelta(hours=838, minutes=59, seconds=59), timedelta(hours=-838)]
_DURATIONS = [timedelta(0), timedelta(hours=-838, minutes=-59, seconds=-59),
              timedelta(hours=838, minutes=59, seconds=59), timedelta(hours=25, microseconds=500000),
              timedelta(microseconds=-1), timedelta(days=1, seconds=1), _TimeDelta(hours=2),
              time(1, 2, 3), "12:00:00"]
_DATETIMES = [datetime(2024, 1, 2, 3, 4, 5, 123456), datetime(2024, 1, 2), datetime(1, 1, 1),
              datetime(9999, 12, 31, 23, 59, 59, 999999), datetime(2024, 11, 3, 1, 30, fold=1),
              datetime(2024, 1, 2, 3, 4, 5, tzinfo=_UTC), datetime(2024, 1, 2, 3, 4, 5, tzinfo=_PLUS9),
              datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=_MINUS0130), _DateTime(2024, 1, 1, 12),
              _DateTime(2024, 1, 1, 12, tzinfo=_PLUS9), "0000-00-00 00:00:00",
              "0000-00-00 00:00:00.000000", "2020-02-31 00:00:00", "2020-00-15 10:00:00.5",
              "2020-01-00 00:00:00", "2024-01-01 00:00:00", "not a date", _Str("0000-00-00 00:00:00"),
              date(2024, 1, 1), 20240101]
_DATES = [date(2024, 2, 29), date.min, date.max, "0000-00-00", "2021-02-30", "2020-00-15",
          "2024-01-01", _Str("0000-00-00"), datetime(2024, 1, 1, 5), "", 7]
_BYTES = [b"", b"\x00\xff", _WKB, _Bytes(b"x"), bytearray(b"y"), memoryview(b"z"), "text", 5]

# (column, target type override or None, the values a read can hand over -- PyMySQL's types,
# their subclasses and the odd ones a test double or another driver could)
_CORPUS: "list[tuple[ColumnDef, Optional[str], list[object]]]" = [
    (ColumnDef(name="flag", mysql_type="TINYINT(1)"), None,
     [0, 1, True, False, Decimal("1"), Decimal("0.0"), 1.0, 0.0, "1", "0", 2, -1, 0.5,
      Decimal("NaN"), Decimal("Infinity"), float("nan"), "x"]),
    (ColumnDef(name="flag_small", mysql_type="TINYINT(1)"), "smallint", [5, -1, 0, 1]),
    (ColumnDef(name="int_bool", mysql_type="INT"), "boolean", [0, 1, 2, True, "1"]),
    (ColumnDef(name="bit1", mysql_type="BIT(1)"), None,
     [b"\x01", b"\x00", bytearray(b"\x01"), memoryview(b"\x00"), _Bytes(b"\x01"), 1, b""]),
    (ColumnDef(name="bit8", mysql_type="BIT(8)"), None, [b"\xff", b"\x05", b"\x80", 7]),
    (ColumnDef(name="bit64", mysql_type="bit(64)"), None,
     [b"\xff" * 8, b"\x00" * 8, b"\x80" + b"\x00" * 7, b"\x12" * 8]),
    (ColumnDef(name="bit_bool", mysql_type="BIT(1)"), "boolean", [b"\x01", b"\x00", 1]),
    (ColumnDef(name="yr", mysql_type="YEAR"), None, [2024, 0, 1901, 2155]),
    (ColumnDef(name="status", mysql_type="ENUM('new','active','')"), None, ["new", "", "active"]),
    (ColumnDef(name="tags", mysql_type="SET('a','b','c')"), None, ["a,c", "", "a,b,c"]),
    (ColumnDef(name="doc", mysql_type="JSON"), None, ['{"k": [1, 2]}', "null", '"s"', '{"한": "😀"}']),
    (ColumnDef(name="geo", mysql_type="GEOMETRY"), None, _BYTES),
    (ColumnDef(name="pt", mysql_type="POINT"), None, [_WKB, memoryview(_WKB)]),
    (ColumnDef(name="blob", mysql_type="BLOB"), None, _BYTES),
    (ColumnDef(name="bin", mysql_type="BINARY(4)"), None, _BYTES),
    (ColumnDef(name="vbin", mysql_type="VARBINARY(16)"), None, _BYTES),
    (ColumnDef(name="txt", mysql_type="TEXT"), None, ["", "한국어", "😀 emoji", "naïve", _Str("s"), "x" * 5000]),
    (ColumnDef(name="vc", mysql_type="VARCHAR(50)"), None, ["", "Müller", "日本語"]),
    (ColumnDef(name="ch", mysql_type="CHAR(4)"), None, ["abcd", ""]),
    (ColumnDef(name="d", mysql_type="DATE"), None, _DATES),
    (ColumnDef(name="d_nn", mysql_type="DATE", nullable=False, default="2000-01-01"), None, _DATES),
    (ColumnDef(name="d_expr", mysql_type="DATE", nullable=False, default="(curdate())",
               default_is_expression=True), None, ["0000-00-00", "2021-02-30", date(2020, 1, 1)]),
    (ColumnDef(name="dt", mysql_type="DATETIME(6)"), None, _DATETIMES),
    (ColumnDef(name="dt_nn", mysql_type="DATETIME", nullable=False), None, _DATETIMES),
    (ColumnDef(name="dt_def", mysql_type="DATETIME", nullable=False, default="2001-02-31 10:20:30"), None,
     ["0000-00-00 00:00:00", "2021-04-31 00:00:00", datetime(2020, 1, 1)]),
    (ColumnDef(name="ts", mysql_type="TIMESTAMP(3)"), None, _DATETIMES),
    (ColumnDef(name="ts_nn", mysql_type="TIMESTAMP", nullable=False), None,
     ["0000-00-00 00:00:00", datetime(1970, 1, 1, 0, 0, 1)]),
    (ColumnDef(name="t", mysql_type="TIME"), None,
     [timedelta(0), timedelta(hours=23, minutes=59, seconds=59, microseconds=999999),
      timedelta(hours=24), timedelta(hours=12, minutes=34, seconds=56), _TimeDelta(hours=1),
      time(1, 2, 3), "12:00:00", *_BAD_TIMES]),
    (ColumnDef(name="t6", mysql_type="TIME(6)"), "time(6)", [timedelta(microseconds=1), *_BAD_TIMES[:1]]),
    (ColumnDef(name="t_dur", mysql_type="TIME(6)"), "interval", _DURATIONS),
    (ColumnDef(name="t_sec", mysql_type="TIME"), "interval second", _DURATIONS),
    (ColumnDef(name="t_txt", mysql_type="TIME"), "text", _DURATIONS),
    (ColumnDef(name="t_vc", mysql_type="TIME"), "varchar(20)", _DURATIONS),
    (ColumnDef(name="t_ch", mysql_type="TIME"), "char(10)", _DURATIONS),
    (ColumnDef(name="dec", mysql_type="DECIMAL(20,6)"), None,
     [Decimal("123.456000"), Decimal("-0.000001"), Decimal("NaN"), Decimal("-Infinity")]),
    (ColumnDef(name="big_u", mysql_type="BIGINT UNSIGNED"), None, [18446744073709551615, 0]),
    (ColumnDef(name="fl", mysql_type="FLOAT"), None, [1.5, -0.0, float("inf"), float("nan")]),
    (ColumnDef(name="db", mysql_type="DOUBLE"), None, [2.25e-300, 1.7976931348623157e308]),
    (ColumnDef(name="si", mysql_type="SMALLINT"), None, [-32768, 32767]),
    (ColumnDef(name="mi", mysql_type="MEDIUMINT UNSIGNED"), None, [16777215]),
    # Remapped in Schema Conversion: the applied type decides, the zero-date mapping stays.
    (ColumnDef(name="d_bytea", mysql_type="DATE"), "bytea", [b"x", "0000-00-00", date(2024, 1, 1)]),
    (ColumnDef(name="dt_text", mysql_type="DATETIME"), "text", _DATETIMES),
    (ColumnDef(name="dt_tz", mysql_type="DATETIME"), "timestamptz", _DATETIMES),
    (ColumnDef(name="ts_naive", mysql_type="TIMESTAMP"), "timestamp(6)", _DATETIMES),
    (ColumnDef(name="d_bool", mysql_type="DATE"), "boolean", ["0000-00-00", 1, date(2024, 1, 1)]),
    (ColumnDef(name="dt_time", mysql_type="DATETIME"), "time", ["0000-00-00 00:00:00", timedelta(hours=1)]),
]


def _table(columns: Sequence[ColumnDef], *, unique: Sequence[str] = (), key: str = "id",
           name: str = "app.corpus") -> TableDef:
    head = [] if any(c.name == key for c in columns) else [ColumnDef(name=key, mysql_type="BIGINT")]
    return TableDef(
        name=name,
        columns=head + list(columns),
        primary_key=[key],
        indexes=[IndexDef(name=f"u_{c}", columns=[c], unique=True) for c in unique],
    )


def _outcome(convert) -> tuple:
    """What a conversion gave -- every key in order with its value's type and repr -- or the
    error it raised."""
    try:
        row = convert()
    except Exception as exc:  # noqa: BLE001 - compared, not handled
        return ("raised", type(exc), str(exc))
    return ("row", tuple((k, type(v), repr(v)) for k, v in row.items()))


def _sa_rows(names: Sequence[str], rows: Sequence[Sequence[object]]) -> list:
    """Real SQLAlchemy ``Row`` objects of ``rows`` under ``names``."""
    return list(IteratorResult(SimpleResultMetaData(list(names)), iter([tuple(r) for r in rows])))


def _converters(table: TableDef, overrides: "dict[str, str]") -> tuple:
    return (ValueConverter(table, target_types=overrides), ValueConverter(table, target_types=overrides))


_CASES = [
    pytest.param(column, override, value, id=f"{column.name}-{index}")
    for column, override, values in _CORPUS
    for index, value in enumerate(values)
]


@pytest.mark.parametrize("column, override, value", _CASES)
@pytest.mark.parametrize("unique", [False, True], ids=["mapped", "in-a-unique-key"])
def test_each_value_converts_by_position_as_convert_row_converts_it(column, override, value, unique) -> None:
    table = _table([column], unique=[column.name] if unique else [])
    overrides = {column.name: override} if override else {}
    names = ("id", column.name)
    for values in ((7, value), _sa_rows(names, [(7, value)])[0]):
        reference, positional = _converters(table, overrides)
        expected = _outcome(lambda: reference.convert_row(dict(zip(names, (7, value)))))
        got = _outcome(lambda: positional.row_converter(names)(values))
        assert got == expected
        assert positional.zero_dates == reference.zero_dates
        assert positional.zero_date_entries() == reference.zero_date_entries()
    # A None is a None for every column (convert_value's first rule).
    reference, positional = _converters(table, overrides)
    assert positional.row_converter(names)((None, None)) == {"id": None, column.name: None}


def _wide() -> "tuple[TableDef, dict[str, str]]":
    columns = [column for column, _override, _values in _CORPUS]
    overrides = {column.name: override for column, override, _values in _CORPUS if override}
    return _table(columns, unique=["dt_nn"]), overrides


def _wide_rows(count: int) -> "list[tuple]":
    """``count`` rows over every corpus column, its values in turn, None in between; the
    values a column refuses are left out (each is refused alone above)."""
    table, overrides = _wide()
    probe = ValueConverter(table, target_types=overrides)
    usable = []
    for column, _override, values in _CORPUS:
        kept = []
        for value in values:
            try:
                probe.convert_value(column.name, value)
            except Exception:  # noqa: BLE001 - a refused value
                continue
            kept.append(value)
        usable.append(kept + [None])
    return [
        (i, *(values[(i * (n + 1)) % len(values)] for n, values in enumerate(usable)))
        for i in range(count)
    ]


def test_whole_rows_convert_by_position_as_convert_row_converts_them() -> None:
    table, overrides = _wide()
    names = tuple(c.name for c in table.columns)
    rows = _wide_rows(400)
    reference, positional = _converters(table, overrides)
    convert = positional.row_converter(names)
    for plain, row in zip(rows, _sa_rows(names, rows)):
        expected = _outcome(lambda: reference.convert_row(dict(zip(names, plain))))
        assert expected[0] == "row"
        assert _outcome(lambda: convert(row)) == expected
        assert _outcome(lambda: convert(plain)) == expected
    # The tally counted every zero / invalid date once per path (twice the reference here).
    assert reference.zero_dates and set(positional.zero_dates) == set(reference.zero_dates)
    assert {k: v // 2 for k, v in positional.zero_dates.items()} == reference.zero_dates


def test_names_in_another_order_with_one_the_table_lacks_convert_as_convert_row() -> None:
    table, overrides = _wide()
    names = tuple(reversed([c.name for c in table.columns])) + ("not_a_column",)
    for plain in _wide_rows(60):
        values = tuple(reversed(plain)) + (datetime(2024, 1, 1, tzinfo=_PLUS9),)
        reference, positional = _converters(table, overrides)
        expected = _outcome(lambda: reference.convert_row(dict(zip(names, values))))
        assert _outcome(lambda: positional.row_converter(names)(values)) == expected
        assert positional.zero_dates == reference.zero_dates


def test_the_first_refused_column_in_row_order_is_the_error_and_the_tally_up_to_it() -> None:
    columns = [ColumnDef(name="dt", mysql_type="DATETIME"),
               ColumnDef(name="flag", mysql_type="TINYINT(1)"),
               ColumnDef(name="d", mysql_type="DATE"),
               ColumnDef(name="t", mysql_type="TIME")]
    table = _table(columns)
    for names in (("id", "dt", "flag", "d", "t"), ("t", "d", "flag", "dt", "id")):
        values = {"id": 1, "dt": "0000-00-00 00:00:00", "flag": 2, "d": "2021-02-30",
                  "t": timedelta(hours=30)}
        reference, positional = _converters(table, {})
        expected = _outcome(lambda: reference.convert_row({n: values[n] for n in names}))
        assert expected[0] == "raised" and expected[1] is ValueConversionError
        assert _outcome(lambda: positional.row_converter(names)([values[n] for n in names])) == expected
        assert positional.zero_dates == reference.zero_dates


def test_a_column_that_converts_nothing_is_never_visited() -> None:
    table = _table([ColumnDef(name="n", mysql_type="INT"), ColumnDef(name="s", mysql_type="TEXT"),
                    ColumnDef(name="j", mysql_type="JSON"), ColumnDef(name="m", mysql_type="DECIMAL(9,2)")])
    converter = ValueConverter(table)
    assert converter._value_functions == {}
    marker = object()  # any value at all passes through, as convert_row passes it
    assert converter.row_converter(("id", "n", "s", "j", "m"))((1, marker, marker, marker, marker)) == {
        "id": 1, "n": marker, "s": marker, "j": marker, "m": marker}


def test_the_postgres_converter_converts_by_position_as_it_converts_a_mapping() -> None:
    table = _table([ColumnDef(name="j", mysql_type="jsonb"), ColumnDef(name="t", mysql_type="timetz")])
    converter = PostgresValueConverter(table)
    names = ("id", "j", "t")
    values = (1, '{"a": 1}', time(10, 0, tzinfo=_PLUS9))
    for row in (values, _sa_rows(names, [values])[0]):
        assert _outcome(lambda: converter.row_converter(names)(row)) == _outcome(
            lambda: converter.convert_row(dict(zip(names, values))))


def test_a_postgres_read_converts_by_position_as_by_name() -> None:
    import ipaddress
    import uuid

    pg_values = [uuid.UUID(int=7), [1, None, 3], Decimal("1e-30"), ipaddress.ip_address("10.0.0.1"),
                 b"\x00\xff", memoryview(b"ab"), True, datetime(2024, 1, 1, tzinfo=_PLUS9), timedelta(days=-1),
                 date(2024, 2, 29), time(23, 59, 59, 999999), "P1Y2M", '{"a": [1]}', float("-inf"), None]
    columns = [ColumnDef(name=f"c{i}", mysql_type=t) for i, t in enumerate(
        ["uuid", "integer[]", "numeric", "inet", "bytea", "bytea", "boolean", "timestamptz", "interval",
         "date", "time", "text", "jsonb", "double precision", "text"])]
    table = TableDef(name="public.t", columns=[ColumnDef(name="id", mysql_type="bigint"), *columns],
                     primary_key=["id"])
    rows = [{"id": i, **{c.name: pg_values[(i + n) % len(pg_values)] for n, c in enumerate(columns)}}
            for i in range(1, 41)]
    out = []
    for sqlalchemy in (False, True):
        session = _Session(rows, sqlalchemy=sqlalchemy)
        dialect = PostgresSourceDialect()
        out.append((_strict(keyset_stream(session, table, batch_size=7, dialect=dialect,
                                          converter=dialect.value_converter(table))), session.executed))
    assert len(out[0][0]) == 40 and out[1] == out[0]


# ---------------------------------------------------------------------------
# Which pages are read by position
# ---------------------------------------------------------------------------


class _SpyConverter(ValueConverter):
    def __init__(self, table: TableDef) -> None:
        super().__init__(table)
        self.by_name = 0
        self.by_position = 0

    def convert_row(self, row):  # noqa: ANN001, ANN201
        self.by_name += 1
        return super().convert_row(row)

    def row_converter(self, names):  # noqa: ANN001, ANN201
        convert = super().row_converter(names)

        def counted(values):  # noqa: ANN001, ANN202
            self.by_position += 1
            return convert(values)

        return counted


class _MappingsOnly:
    """A test double's page: mappings only, as every double in the suite."""

    def __init__(self, rows: "list[dict]") -> None:
        self._rows = rows

    def mappings(self) -> "list[dict]":
        return self._rows


def _key_of_mapping(row) -> tuple:  # noqa: ANN001
    return (row["id"],)


def _read(result, converter) -> list:  # noqa: ANN001
    rows, key_of, emit = _page_reader(result, ["id"], _key_of_mapping, converter)
    return [(key_of(row), row if emit is None else emit(row)) for row in rows]


def test_only_an_sqlalchemy_result_read_for_a_row_converter_is_read_by_position() -> None:
    table = _table([ColumnDef(name="dt", mysql_type="DATETIME")])
    page = [{"id": 1, "dt": datetime(2024, 1, 1)}, {"id": 2, "dt": "0000-00-00 00:00:00"}]

    def result(names=("id", "dt")):  # noqa: ANN001, ANN202
        return IteratorResult(SimpleResultMetaData(list(names)), iter([tuple(r[n] for n in names) for r in page]))

    spy = _SpyConverter(table)
    positional = _read(result(), spy)
    assert (spy.by_position, spy.by_name) == (2, 0)
    by_name = _read(_MappingsOnly(page), spy)
    assert (spy.by_position, spy.by_name) == (2, 2)
    assert positional == by_name == [((1,), {"id": 1, "dt": datetime(2024, 1, 1)}), ((2,), {"id": 2, "dt": None})]

    # Read by name as before: names that repeat or lack the key, a converter without
    # row_converter, and no converter at all (the result's own mappings, unconverted).
    class _ByNameOnly:
        def convert_row(self, row):  # noqa: ANN001, ANN202
            return dict(row)

    for names in (("id", "dt", "dt"), ("dt",)):
        spy = _SpyConverter(table)
        rows, _key_of, emit = _page_reader(result(names), ["id"], _key_of_mapping, spy)
        assert emit == spy.convert_row and not isinstance(rows, IteratorResult)
    rows, _key, emit = _page_reader(result(), ["id"], _key_of_mapping, _ByNameOnly())
    assert all(isinstance(row, RowMapping) for row in rows)
    rows, _key, emit = _page_reader(result(), ["id"], _key_of_mapping, None)
    assert emit is None and all(isinstance(row, RowMapping) for row in rows)


def test_a_raw_read_still_yields_the_results_mappings() -> None:
    table = _table([ColumnDef(name="v", mysql_type="TEXT")])
    session = _Session([{"id": i, "v": f"v{i}"} for i in range(1, 6)], sqlalchemy=True)
    rows = list(keyset_stream(session, table, batch_size=2))
    assert all(isinstance(row, RowMapping) for row in rows)
    assert [dict(row) for row in rows] == [{"id": i, "v": f"v{i}"} for i in range(1, 6)]


# ---------------------------------------------------------------------------
# Keyset reads: by position and by name, the same rows, statements and errors
# ---------------------------------------------------------------------------


class _Session:
    """A source session serving keyset pages over ``rows`` (a one-column key ``id``: the
    cursor, the range bound, the page size), either as SQLAlchemy results or as a test
    double's mappings, recording every statement and when each page result is let go."""

    def __init__(self, rows: "list[dict]", *, sqlalchemy: bool) -> None:
        self.rows = sorted(rows, key=lambda r: r["id"])
        self.sqlalchemy = sqlalchemy
        self.executed: "list[tuple[str, dict]]" = []
        self.events: "list[str]" = []

    def execution_options(self, **_options):  # noqa: ANN201
        return self

    def execute(self, statement, parameters=None, execution_options=None):  # noqa: ANN001, ANN201
        sql = " ".join(str(statement).split())
        params = dict(parameters or {})
        self.executed.append((sql, params))
        self.events.append(sql.split(" ", 1)[0])
        if not sql.startswith("SELECT "):
            return _MappingsOnly([])
        lo, hi, last = params.get("pk_lower"), params.get("pk_upper"), params.get("last")
        page = [r for r in self.rows
                if (lo is None or r["id"] >= lo) and (hi is None or r["id"] < hi)
                and (last is None or r["id"] > last)][: params["batch_size"]]
        if not self.sqlalchemy:
            return _MappingsOnly(page)
        names = list(page[0]) if page else ["id"]
        events = self.events

        class _Result(IteratorResult):
            def __del__(self) -> None:
                events.append("released")

        return _Result(SimpleResultMetaData(names), iter([tuple(r[n] for n in names) for r in page]))


class _Engine:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def connect(self):  # noqa: ANN201
        import contextlib

        return contextlib.nullcontext(self.session)

    def dispose(self) -> None:
        pass


def _strict(rows) -> list:  # noqa: ANN001
    return [tuple((k, type(v), repr(v)) for k, v in row.items()) for row in rows]


def _corpus_rows(count: int) -> "tuple[TableDef, dict[str, str], list[dict]]":
    table, overrides = _wide()
    names = [c.name for c in table.columns]
    return table, overrides, [dict(zip(names, (i + 1, *row[1:]))) for i, row in enumerate(_wide_rows(count))]


def _converted_by_reference(count: int = 120) -> "tuple[list, list]":
    """The corpus rows in key order as ONE fresh converter's ``convert_row`` converts them, and
    the zero-date report that converter gives -- what every read below yields, whatever its
    path: a read that converted nothing on both paths would still agree with itself."""
    table, overrides, rows = _corpus_rows(count)
    reference = ValueConverter(table, target_types=overrides)
    converted = _strict(reference.convert_row(row) for row in sorted(rows, key=lambda r: r["id"]))
    return converted, [reference.zero_date_entries()]


def _both(read) -> tuple:  # noqa: ANN001
    """``read(session)`` by name and by position: (outcome, statements, session) each."""
    out = []
    for sqlalchemy in (False, True):
        table, overrides, rows = _corpus_rows(120)
        session = _Session(rows, sqlalchemy=sqlalchemy)
        try:
            value = ("ok", read(session, table, overrides))
        except Exception as exc:  # noqa: BLE001 - compared
            value = ("raised", type(exc), str(exc))
        out.append((value, session.executed, session))
    return tuple(out)


@pytest.mark.parametrize("batch_size", [1, 7, 50, 500])
def test_stream_converted_rows_by_position_yields_what_it_yields_by_name(batch_size: int) -> None:
    def read(session, table, overrides):  # noqa: ANN001, ANN202
        zero: list = []
        rows = TableExporter(engine_factory=lambda _c: _Engine(session), batch_size=batch_size)
        got = _strict(rows.stream_converted_rows(
            SourceConnectionConfig(host="h"), table, target_types=overrides, on_zero_dates=zero.append))
        return got, zero

    (by_name, sql_a, _), (by_position, sql_b, _) = _both(read)
    assert by_name[0] == "ok" and len(by_name[1][0]) == 120 and by_name[1][1]
    assert by_position == by_name and sql_b == sql_a
    assert by_name[1] == _converted_by_reference()


@pytest.mark.parametrize("page_rows", [1, 9, 40])
def test_stream_converted_ranges_by_position_yields_what_it_yields_by_name(page_rows: int) -> None:
    def read(session, table, overrides):  # noqa: ANN001, ANN202
        zero: list = []
        pages = TableExporter(engine_factory=lambda _c: _Engine(session)).stream_converted_ranges(
            SourceConnectionConfig(host="h"), table, [(None, 30), (30, 31), (31, 90), (90, None)],
            page_rows=page_rows, target_types=overrides, on_zero_dates=zero.append)
        return [(index, _strict(page)) for index, page in pages], zero

    (by_name, sql_a, _), (by_position, sql_b, _) = _both(read)
    assert by_name[0] == "ok" and sum(len(rows) for _i, rows in by_name[1][0]) == 120
    assert by_position == by_name and sql_b == sql_a
    # The ranges come a page of each in turn; together, the rows the reference converts.
    pages, zero = by_name[1]
    flat = sorted((row for _index, rows in pages for row in rows), key=lambda row: int(row[0][2]))
    assert (flat, zero) == _converted_by_reference()


def test_export_rows_by_position_writes_what_it_writes_by_name() -> None:
    import io

    def read(session, table, overrides):  # noqa: ANN001, ANN202
        buffer = io.StringIO()
        count = export_rows(session, table, CsvRowWriter(buffer), batch_size=16, target_types=overrides)
        return count, buffer.getvalue()

    (by_name, sql_a, _), (by_position, sql_b, _) = _both(read)
    assert by_name[0] == "ok" and by_name[1][0] == 120
    assert by_position == by_name and sql_b == sql_a
    table, overrides, rows = _corpus_rows(120)
    reference = ValueConverter(table, target_types=overrides)
    buffer = io.StringIO()
    writer = CsvRowWriter(buffer)
    writer.write_header([c.name for c in table.columns])
    for row in rows:
        writer.write_row(reference.convert_row(row))
    assert by_name[1] == (120, buffer.getvalue())


def _refusing(rows: "list[dict]") -> "list[dict]":
    """``rows`` with a TINYINT(1) boolean holding 2 in the 50th row."""
    return [dict(r, flag=2 if r["id"] == 50 else r["flag"]) for r in rows]


def test_a_refused_value_stops_both_reads_alike_and_ends_the_snapshot() -> None:
    for page_rows in (7, 64):
        outcomes = []
        for sqlalchemy in (False, True):
            table, overrides, rows = _corpus_rows(120)
            session = _Session(_refusing(rows), sqlalchemy=sqlalchemy)
            got: list = []
            with pytest.raises(ValueConversionError) as caught:
                for row in TableExporter(engine_factory=lambda _c: _Engine(session), batch_size=page_rows
                                         ).stream_converted_rows(SourceConnectionConfig(host="h"), table,
                                                                 target_types=overrides):
                    got.append(row)
            outcomes.append((_strict(got), str(caught.value), session.executed))
            assert session.executed[-1][0] == "COMMIT"  # the snapshot ended after the failure
        assert outcomes[1] == outcomes[0] and len(outcomes[0][0]) == 49


def test_a_refused_value_lets_the_page_go_before_the_snapshot_ends() -> None:
    # The row stream converted the rows itself before: the page's result was let go when the
    # failed stream closed its pages, so its cursor ended before the snapshot's COMMIT (on
    # PyMySQL, the unread rows drained by the cursor, not by the COMMIT). Converted in the
    # page, the same order.
    table, overrides, rows = _corpus_rows(120)
    session = _Session(_refusing(rows), sqlalchemy=True)
    with pytest.raises(ValueConversionError):
        for _row in TableExporter(engine_factory=lambda _c: _Engine(session), batch_size=64
                                  ).stream_converted_rows(SourceConnectionConfig(host="h"), table,
                                                          target_types=overrides):
            pass
    events = session.events
    assert events.index("released", events.index("SELECT") + 1) < len(events) - 1
    assert events[-1] == "COMMIT"
    assert events[events.index("COMMIT") - 1] == "released"


# ---------------------------------------------------------------------------
# The cursor checks by position: the same keys, re-read skips and stops
# ---------------------------------------------------------------------------


class _AsSqlAlchemy:
    """``inner``'s keyset pages as SQLAlchemy results over the table's columns."""

    def __init__(self, inner: object, names: Sequence[str]) -> None:
        self._inner = inner
        self._names = list(names)

    def execute(self, statement, parameters=None, **_kw):  # noqa: ANN001, ANN201
        result = self._inner.execute(statement, parameters)  # type: ignore[attr-defined]
        if " LIMIT " not in str(statement):
            return result
        rows = list(result.mappings())
        return IteratorResult(SimpleResultMetaData(self._names),
                              iter([tuple(r[n] for n in self._names) for r in rows]))


def _keyset_both(make_source, table: TableDef, *, dialect, batch_size: int) -> list:
    """The read of ``make_source()`` by name and by position, converted: rows or the error."""
    out = []
    for positional in (False, True):
        source = make_source()
        if positional:
            source = _AsSqlAlchemy(source, [c.name for c in table.columns])
        converter = dialect.value_converter(table)
        try:
            got = ("ok", _strict(keyset_stream(source, table, batch_size=batch_size, dialect=dialect,
                                               converter=converter)))
        except ExportError as exc:
            got = ("raised", type(exc), str(exc))
        out.append(got)
    return out


@pytest.mark.parametrize("key", [["id"], ["a", "b"]], ids=["one-column", "composite"])
def test_a_key_after_other_columns_and_out_of_their_order_reads_by_its_names(key: list) -> None:
    # A row's key is taken by position: each key column's own position, in KEY order -- the
    # cursor the next page is read after. Here the key columns follow other columns and the
    # composite key's columns come in the other order in the row.
    from tests.test_g125_keyset_review import _MySqlModel, _table as _g125_table

    table = _g125_table("orders", [("note", "varchar(20)"), ("b", "int"), ("flag", "tinyint(1)"),
                                   ("a", "int"), ("id", "int")], key)
    rows = [{"note": f"n{a}{b}", "b": b, "flag": (a + b) % 2, "a": a, "id": 10 * a + b}
            for a in (3, 1, 2) for b in (2, 1, 3)]
    reference = ValueConverter(table)
    expected = ("ok", _strict(reference.convert_row(row)
                              for row in sorted(rows, key=lambda r: tuple(r[c] for c in key))))
    for batch in (1, 2, 4, 9):
        by_name, by_position = _keyset_both(lambda: _MySqlModel(table, rows), table,
                                            dialect=MySQLSourceDialect(), batch_size=batch)
        assert by_name == expected
        assert by_position == by_name


@pytest.mark.parametrize("batch", [2, 3, 5])
def test_an_enum_walk_and_its_stops_read_alike_by_position(batch: int) -> None:
    from tests.test_g125_keyset_review import _MySqlModel, _table as _g125_table

    table = _g125_table("tickets", [("status", "enum('new','active','closed')"), ("id", "int")],
                        ["status", "id"])
    rows = [{"status": s, "id": i} for s in ("new", "active", "closed") for i in range(1, 6)]
    by_name, by_position = _keyset_both(lambda: _MySqlModel(table, rows), table,
                                        dialect=MySQLSourceDialect(), batch_size=batch)
    assert by_name[0] == "ok" and len(by_name[1]) == 15 and by_position == by_name
    # Members the collation calls equal: the same stop (a page of 2 ends on one of them).
    equal = _g125_table("trail", [("u", "int"), ("k", "enum('a','A','b')"), ("f", "float")], ["u", "k", "f"])
    rows = [{"u": u, "k": k, "f": 1.0} for u in (1, 2, 3) for k in ("a", "A", "b")]
    by_name, by_position = _keyset_both(lambda: _MySqlModel(equal, rows, ci=True), equal,
                                        dialect=MySQLSourceDialect(), batch_size=batch)
    assert by_position == by_name
    if batch == 2:
        assert by_name[0] == "raised" and "returned the row the page was read after" in by_name[2]


@pytest.mark.parametrize("engine", ["mysql", "postgresql"])
@pytest.mark.parametrize("key", [("f",), ("n", "f")])
def test_a_float_key_reads_alike_by_position(engine: str, key: tuple) -> None:
    from tests.test_g144_float_key_full_load import _FloatSource, _f32, _prices, _tabledef

    dialect = MySQLSourceDialect() if engine == "mysql" else PostgresSourceDialect()
    table = _tabledef(engine, key)
    for batch in (3, 7):
        by_name, by_position = _keyset_both(lambda: _FloatSource(engine, key, _prices(key, 90)), table,
                                            dialect=dialect, batch_size=batch)
        assert by_name[0] == "ok" and len(by_name[1]) == 90 and by_position == by_name
    if engine == "mysql" and key == ("f",):
        # Values that read back alike: skipped where a page starts; a full page of them stops.
        stored = [0.5, 1.0, _f32(1.0000001), _f32(1.0000002), 2.0]
        alike = [{"f": f, "v": i} for i, f in enumerate(stored)]
        by_name, by_position = _keyset_both(lambda: _FloatSource(engine, key, alike), table,
                                            dialect=dialect, batch_size=3)
        assert by_name[0] == "ok" and len(by_name[1]) == 4 and by_position == by_name
        crowded = [{"f": _f32(1.0 + k * 2**-23), "v": k} for k in range(8)] + [{"f": 2.0, "v": 9}]
        by_name, by_position = _keyset_both(lambda: _FloatSource(engine, key, crowded), table,
                                            dialect=dialect, batch_size=3)
        assert by_name[0] == "raised" and by_position == by_name


def test_a_timetz_key_told_apart_by_its_offset_reads_alike_by_position() -> None:
    from tests.test_g125_keyset_review import _Result, _table as _g125_table

    rows = [
        {"n": 1, "t": time(11, 0, tzinfo=timezone(timedelta(hours=2)))},
        {"n": 1, "t": time(10, 0, tzinfo=timezone(timedelta(hours=1)))},
        {"n": 1, "t": time(9, 0, tzinfo=_UTC)},
        {"n": 2, "t": time(12, 0, tzinfo=_UTC)},
    ]

    class _Pages:
        def __init__(self, served: list) -> None:
            self.served, self.offset = served, 0

        def execute(self, statement: object, parameters: Optional[dict] = None) -> _Result:
            page = self.served[self.offset:self.offset + parameters["batch_size"]]
            self.offset += len(page)
            return _Result(page)

    table = _g125_table("public.t", [("n", "integer"), ("t", "time with time zone")], ["n", "t"])
    by_name, by_position = _keyset_both(lambda: _Pages(rows), table, dialect=PostgresSourceDialect(),
                                        batch_size=1)
    assert by_name == ("ok", _strict(rows))
    assert by_position == by_name
    # The SAME time again (its own offset) is the cursor's row: both stop.
    again = [rows[0], dict(rows[0])]
    by_name, by_position = _keyset_both(lambda: _Pages(again), table, dialect=PostgresSourceDialect(),
                                        batch_size=1)
    assert by_name[0] == "raised" and by_position == by_name


_KEYS = [0, 1, 1.0, True, Decimal("1"), Decimal("NaN"), float("nan"), "a", "A", b"a", None,
         date(2024, 1, 1), datetime(2024, 1, 1), datetime(2024, 1, 1, tzinfo=_UTC),
         time(10, 0), time(10, 0, tzinfo=_UTC), time(11, 0, tzinfo=timezone(timedelta(hours=1))),
         time(10, 0, tzinfo=timezone(timedelta(hours=1))), timedelta(0)]


def test_a_key_equal_to_its_cursor_told_apart_is_equal_to_it_as_it_is() -> None:
    # The page compares a key with its cursor as it is first and told apart only when equal:
    # the same verdict as told apart alone, for every pair (including one object twice).
    for left, right in itertools.product(_KEYS, repeat=2):
        for key, cursor in (((left,), (right,)), ((left, right), (right, left)), ((left, 1), (left, 1))):
            told = tuple(map(_told_apart, key)) == tuple(map(_told_apart, cursor))
            assert (key == cursor and told) == told, (key, cursor)


def test_keyset_pages_reads_a_page_by_position_only_for_a_converter() -> None:
    table = _table([ColumnDef(name="dt", mysql_type="DATETIME")])
    session = _Session([{"id": i, "dt": datetime(2024, 1, i)} for i in range(1, 6)], sqlalchemy=True)
    spy = _SpyConverter(table)
    pages = keyset_pages(session, table, batch_size=2, converter=spy)
    assert [[row["id"] for row in page] for page in pages] == [[1, 2], [3, 4], [5]]
    assert (spy.by_position, spy.by_name) == (5, 0)
