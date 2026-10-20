# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G144: the Full Load reader loads a single-precision float key again.

G130's page guard stops a read whose page returns the row it was read after. A float key
the cursor cannot name exactly met it at every boundary below the stored float, so a table
larger than one page stopped where 0.1.570 loaded it:

- a PostgreSQL ``real`` key: psycopg reads it as the float of its shortest decimal and bound
  that float as a float8, below the stored float4 for about half the values. Its cursor now
  goes back as text, which the server parses as the float4 it is. (Above the stored value, a
  ``real`` followed by another key column passed the rest of that value's rows by, silently,
  in 0.1.570 too.)
- a MySQL key whose last (or only) column is a ``FLOAT``: MySQL returns the value rounded to
  6 significant digits, and no binding names the stored one. The reader now skips the rows
  a page starts with that read back as its cursor (ones it has read, or ones the target holds
  as the same key) and stops only on a full page of them -- not for a key with an ENUM or SET
  column, whose collation-equal members G130 stops for in the same way.

The doubles below compare as the servers do; no database is reached.
"""

from __future__ import annotations

import re
import struct
from typing import Optional

import pytest

from dsql_migrator.core.exporter import ExportError, keyset_stream
from dsql_migrator.core.models import ColumnDef, TableDef
from dsql_migrator.core.source_dialect import MySQLSourceDialect, PostgresSourceDialect

_MY = MySQLSourceDialect()
_PG = PostgresSourceDialect()


def _f32(value: float) -> float:
    """``value`` rounded to the nearest float4 (a Python float holding it exactly)."""
    return struct.unpack("<f", struct.pack("<f", value))[0]


def _shortest(value: float) -> str:
    """A float4 as PostgreSQL prints it (``extra_float_digits`` > 0): its shortest decimal."""
    for digits in range(1, 10):
        text = f"{value:.{digits}g}"
        if _f32(float(text)) == value:
            return text
    raise AssertionError(value)


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self) -> list[dict]:
        return self._rows


class _FloatSource:
    """A source whose float key column ``f`` holds float4 values, read and compared the way
    ``engine`` and its driver do:

    - ``mysql``: returned rounded to 6 significant digits; a bound float is a DOUBLE, to
      which the stored FLOAT widens exactly.
    - ``postgresql``: returned as the float psycopg parses from its shortest decimal; a bound
      float is a float8 (as above), a bound text is parsed into a float4.
    """

    def __init__(self, engine: str, key: tuple[str, ...], rows: list[dict]) -> None:
        self.engine, self.key = engine, key
        self.rows = sorted(rows, key=lambda r: tuple(r[c] for c in key))
        self.executed: list[tuple[str, dict]] = []

    def _out(self, row: dict) -> dict:
        f = row["f"]
        read = float(f"{f:.6g}") if self.engine == "mysql" else float(_shortest(f))
        return {**row, "f": read}

    def _bound(self, column: str, value: object) -> object:
        if column != "f" or not isinstance(value, str):
            return value
        # MySQL compares a string with a FLOAT column as a DOUBLE; PostgreSQL parses an
        # untyped parameter in the column's own type, a float4.
        return float(value) if self.engine == "mysql" else _f32(float(value))

    def execute(self, statement: object, parameters: Optional[dict] = None) -> _Result:
        sql, params = str(statement), dict(parameters or {})
        self.executed.append((sql, params))
        assert len(self.executed) <= 4 * len(self.rows) + 10, "the read never ended"
        rows = self.rows
        where = re.search(r" WHERE (.*) ORDER BY ", sql)
        if where:
            q = "`" if self.engine == "mysql" else '"'
            expr = where[1]
            row_value = re.fullmatch(r"\((.*)\) > \((.*)\)", expr)
            if row_value:  # PostgreSQL's ("n", "f") > (:last_0, :last_1)
                columns = [c.strip().strip('"') for c in row_value[1].split(",")]
                names = [n.strip()[1:] for n in row_value[2].split(",")]
                rows = [r for r in rows if tuple(r[c] for c in columns)
                        > tuple(self._bound(c, params[n]) for c, n in zip(columns, names))]
            else:
                expr = re.sub(rf"{q}(\w+){q} > :(\w+)", r"_gt(row, '\1', '\2')", expr)
                expr = re.sub(rf"{q}(\w+){q} = :(\w+)", r"_eq(row, '\1', '\2')", expr)
                expr = expr.replace(" AND ", " and ").replace(" OR ", " or ")
                env = {
                    "_gt": lambda row, c, n: row[c] > self._bound(c, params[n]),
                    "_eq": lambda row, c, n: row[c] == self._bound(c, params[n]),
                }
                rows = [r for r in rows if eval(expr, env, {"row": r})]  # noqa: S307
        return _Result([self._out(r) for r in rows[: params["batch_size"]]])


def _tabledef(engine: str, key: tuple[str, ...]) -> TableDef:
    float_type = "float" if engine == "mysql" else "real"
    types = {"f": float_type, "n": "int" if engine == "mysql" else "integer", "v": "int"}
    return TableDef(
        name="app.prices",
        columns=[ColumnDef(name=c, mysql_type=types[c]) for c in ("n", "f", "v") if c in key or c == "v"],
        primary_key=list(key),
    )


def _prices(key: tuple[str, ...], n: int = 300) -> list[dict]:
    """``n`` price-like float4 keys (0.01 + 0.03 k), or for a composite key ten of them under
    each ``n``: many hold a value whose decimal is below the stored float4."""
    if key == ("f",):
        return [{"f": _f32(0.01 + 0.03 * k), "v": k} for k in range(n)]
    return [{"n": k // 10, "f": _f32(0.01 + 0.03 * (k % 10)), "v": k} for k in range(n)]


def _read(source: _FloatSource, key: tuple[str, ...], batch: int, dialect) -> list[int]:
    out = []
    for row in keyset_stream(source, _tabledef(source.engine, key), batch_size=batch, dialect=dialect):
        out.append(row["v"])
        assert len(out) <= 2 * len(source.rows), "the read never ended"
    return out


_CASES = [("mysql", _MY), ("postgresql", _PG)]


@pytest.mark.parametrize("engine, dialect", _CASES)
@pytest.mark.parametrize("key", [("f",), ("n", "f")])
@pytest.mark.parametrize("batch", [3, 7, 100])
def test_a_float_key_reads_every_row_once(engine, dialect, key, batch) -> None:
    rows = _prices(key)
    source = _FloatSource(engine, key, rows)
    assert _read(source, key, batch, dialect) == [r["v"] for r in source.rows]


def test_the_doubles_meet_the_boundaries_the_fix_is_for(monkeypatch) -> None:
    # Before G144 each engine's read stopped at the first boundary below the stored float.
    monkeypatch.setattr(MySQLSourceDialect, "keyset_rereads_cursor", lambda self, table: False)
    for engine, dialect in _CASES:
        if engine == "postgresql":
            monkeypatch.setattr(PostgresSourceDialect, "keyset_after_sql", _native_pg_cursor)
        with pytest.raises(ExportError, match="returned the row the page was read after"):
            _read(_FloatSource(engine, ("f",), _prices(("f",))), ("f",), 100, dialect)


def _native_pg_cursor(self, table, last_key, members):
    """The PostgreSQL cursor before G144: every value bound as psycopg returned it."""
    if len(last_key) == 1:
        return f'"{table.primary_key[0]}" > :last', {"last": last_key[0]}
    names = [f"last_{i}" for i in range(len(last_key))]
    columns = ", ".join(f'"{c}"' for c in table.primary_key)
    return f"({columns}) > ({', '.join(':' + n for n in names)})", dict(zip(names, last_key))


def _above() -> list[float]:
    """30 float4 values whose shortest decimal -- psycopg's float of it -- is ABOVE them."""
    values = (_f32(0.01 + 0.03 * k) for k in range(1000))
    return [v for v in values if float(_shortest(v)) > v][:30]


@pytest.mark.parametrize("batch", [3, 7, 97])  # each splits a value's ten rows
def test_a_real_followed_by_another_key_column_reads_every_row(batch, monkeypatch) -> None:
    # A real-led key with ten rows per value: bound as psycopg's float, a value above the
    # stored float4 passed the rest of that value's rows by, with no error -- the read
    # ended normally with rows missing.
    rows = [{"f": f, "n": n, "v": 10 * i + n} for i, f in enumerate(_above()) for n in range(10)]
    source = _FloatSource("postgresql", ("f", "n"), rows)
    assert _read(source, ("f", "n"), batch, _PG) == [r["v"] for r in source.rows]
    monkeypatch.setattr(PostgresSourceDialect, "keyset_after_sql", _native_pg_cursor)
    before = _read(_FloatSource("postgresql", ("f", "n"), rows), ("f", "n"), batch, _PG)
    assert len(before) < len(rows) and len(set(before)) == len(before)


def test_a_postgresql_real_key_cursor_goes_back_as_text() -> None:
    def table(*columns: tuple[str, str], base: Optional[str] = None) -> TableDef:
        return TableDef(
            name="public.t",
            columns=[ColumnDef(name=c, mysql_type=t, base_type=base if t == "price" else None)
                     for c, t in columns],
            primary_key=[c for c, _ in columns],
        )

    assert _PG.keyset_after_sql(table(("f", "real")), (0.3,), {}) == ('"f" > :last', {"last": "0.3"})
    assert _PG.keyset_after_sql(table(("n", "integer"), ("f", "real")), (4, 1e-07), {}) == (
        '("n", "f") > (:last_0, :last_1)', {"last_0": 4, "last_1": "1e-07"})
    # A domain over real binds as its storage type does.
    assert _PG.keyset_after_sql(table(("f", "price"), base="real"), (0.3,), {})[1] == {"last": "0.3"}
    # A double precision key binds exactly as the float it is; a text-read value is text.
    assert _PG.keyset_after_sql(table(("f", "double precision")), (0.3,), {})[1] == {"last": 0.3}
    assert _PG.keyset_after_sql(table(("t", "timestamp")), ("2026-01-01",), {})[1] == {
        "last": "2026-01-01"}


@pytest.mark.parametrize("types, expected", [
    (("float",), True),
    (("int", "float unsigned"), True),
    (("int", "float(7,3)"), True),
    (("double",), False),
    (("float", "int"), False),  # refused by keyset_after_sql instead (G140)
    (("int",), False),
])
def test_only_a_mysql_key_ending_in_a_float_skips_its_cursors_rows(types, expected) -> None:
    columns = [ColumnDef(name=f"k{i}", mysql_type=t) for i, t in enumerate(types)]
    table = TableDef(name="app.t", columns=columns, primary_key=[c.name for c in columns])
    assert _MY.keyset_rereads_cursor(table) is expected
    assert _PG.keyset_rereads_cursor(table) is False


def test_a_key_with_an_enum_keeps_the_stop_and_only_a_pages_first_rows_are_skipped() -> None:
    # G130's case -- members the collation calls equal re-read the cursor's row -- with a
    # FLOAT after them: no skip could tell the two apart, so the read still stops.
    from tests.test_g125_keyset_review import _MySqlModel, _table

    table = _table("trail", [("u", "int"), ("k", "enum('a','A','b')"), ("f", "float")],
                   ["u", "k", "f"])
    assert _MY.keyset_rereads_cursor(table) is False
    rows = [{"u": u, "k": k, "f": 1.0} for u in (1, 2, 3) for k in ("a", "A", "b")]
    with pytest.raises(ExportError, match="returned the row the page was read after"):
        list(keyset_stream(_MySqlModel(table, rows, ci=True), table, batch_size=2))

    # A row that reads back as the cursor AFTER a row past it is no rounding: a stop.
    class _OutOfOrder:
        pages = iter([[{"f": 0.5, "v": 0}, {"f": 1.0, "v": 1}],
                      [{"f": 2.0, "v": 3}, {"f": 1.0, "v": 2}]])

        def execute(self, statement: object, parameters: Optional[dict] = None) -> _Result:
            return _Result(next(self.pages))

    with pytest.raises(ExportError, match="returned the row the page was read after"):
        list(keyset_stream(_OutOfOrder(), _tabledef("mysql", ("f",)), batch_size=2, dialect=_MY))


def test_values_that_read_back_alike_load_once_and_a_full_page_of_them_stops() -> None:
    # Three FLOATs MySQL returns as 1 (1, 1.0000001, 1.0000002) between 0.5 and 2: the second
    # page starts at the rows that read back as its cursor, 1 -- one the read has loaded and
    # one the target holds as the same key -- and loads neither.
    stored = [0.5, 1.0, _f32(1.0000001), _f32(1.0000002), 2.0]
    source = _FloatSource("mysql", ("f",), [{"f": f, "v": i} for i, f in enumerate(stored)])
    assert _read(source, ("f",), 3, _MY) == [0, 1, 2, 4]
    # A page that holds nothing but such rows would be the next page again: a stop.
    crowded = [{"f": _f32(1.0 + k * 2**-23), "v": k} for k in range(8)] + [{"f": 2.0, "v": 9}]
    with pytest.raises(ExportError, match="held only rows whose key reads back as the page's cursor"):
        _read(_FloatSource("mysql", ("f",), crowded), ("f",), 3, _MY)
