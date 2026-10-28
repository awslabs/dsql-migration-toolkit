# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G154: a MySQL primary key that holds a zero date or a day the month does not have.

* The session pin: G148 pinned every MySQL source session to sql_mode '', and MySQL 8.0+
  then refuses a keyset cursor or a page boundary that names a stored day the month does not
  have ('2020-02-31', stored under ALLOW_INVALID_DATES) with error 1525 -- every read of a
  table whose page ended on one failed (live on MySQL 8.0.46 and 8.4.11; 5.7.44 never
  refused it). The pin is now sql_mode 'ALLOW_INVALID_DATES': still no flag that changes how
  SQL parses, and no NO_ZERO_DATE, so a zero-date cursor binds too.
* The key: G150 wrote a zero or invalid date as the bundled CDC writes it (NULL, the
  column's DEFAULT or 1970-01-01) in every DATE / DATETIME / TIMESTAMP column, the primary
  key's too, so keys fell onto one: the loader kept one row and counted the others as
  already on the target -- the valid row among them -- or failed the whole batch with
  "exhausted N attempts due to concurrent unique violations". A key column's value is now
  left as stored, Aurora DSQL rejects it (22008) and that row alone is quarantined, as
  before G150; Validation renders a key column as stored too.
* Evaluation (ZERO_DATES) names such key columns, and a unique key over such a column; the
  manual (en / ko / ja) says so, and no longer says such a key can fail the read with 1525.

Doubles only: no MySQL, no PostgreSQL, no AWS.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import pytest

from dsql_migrator.core.assessor import ZERO_DATES_RULE_ID, CompatibilityAssessor
from dsql_migrator.core.batched_import import (
    BatchedImporter,
    BatchedImportOptions,
    OnConflictMode,
)
from dsql_migrator.core.exporter import ValueConverter
from dsql_migrator.core.introspector import source_engine_kwargs
from dsql_migrator.core.models import ColumnDef, IndexDef, SourceInventory, TableDef
from dsql_migrator.core.mysql_zero_dates import zero_date_mapping, zero_date_mappings
from dsql_migrator.core.validation_sql import (
    _mysql_checksum_expr,
    build_mysql_checksum_sql,
    build_mysql_composite_page_sql,
    build_mysql_page_checksum_first_sql,
    build_mysql_pk_token_sql,
)


def _col(name: str, mysql_type: str, *, nullable: bool = True, default=None) -> ColumnDef:
    return ColumnDef(name=name, mysql_type=mysql_type, nullable=nullable, default=default)


def _calendar(name: str = "app.cal") -> TableDef:
    """A DATE primary key whose DEFAULT is a real date, beside a mapped DATE column."""
    return TableDef(
        name=name,
        columns=[
            _col("d", "date", nullable=False, default="2000-01-01"),
            _col("v", "varchar(10)"),
            _col("seen", "date"),
        ],
        primary_key=["d"],
    )


# ---------------------------------------------------------------------------
# 1. The session pin
# ---------------------------------------------------------------------------


def test_the_session_pin_lets_a_stored_invalid_day_bind_and_changes_no_parsing() -> None:
    for kwargs in (source_engine_kwargs(), source_engine_kwargs(read_timeout_seconds=30)):
        command = kwargs["connect_args"]["init_command"]
        assert command == "SET time_zone = '+00:00', sql_mode = 'ALLOW_INVALID_DATES'"
        mode = re.search(r"sql_mode = '([^']*)'", command).group(1)
        flags = set(mode.split(","))
        assert flags == {"ALLOW_INVALID_DATES"}
        # Nothing that changes how the tool's SQL parses or what a read returns ...
        for flag in ("ANSI", "ANSI_QUOTES", "NO_BACKSLASH_ESCAPES", "PIPES_AS_CONCAT",
                     "PAD_CHAR_TO_FULL_LENGTH", "ONLY_FULL_GROUP_BY"):
            assert flag not in flags
        # ... and nothing that refuses a stored zero date bound back as a cursor.
        for flag in ("NO_ZERO_DATE", "NO_ZERO_IN_DATE", "STRICT_TRANS_TABLES",
                     "STRICT_ALL_TABLES", "TRADITIONAL"):
            assert flag not in flags


# ---------------------------------------------------------------------------
# 2. Full Load leaves a key column's zero or invalid date as stored
# ---------------------------------------------------------------------------


def test_a_primary_key_column_is_not_mapped_and_the_other_columns_are() -> None:
    table = _calendar()
    assert set(zero_date_mappings(table)) == {"seen"}
    # The column on its own still has a mapping: only its place in the key keeps it out.
    assert zero_date_mapping(table.columns[0]) is not None
    composite = TableDef(
        name="app.ev",
        columns=[_col("dev", "int", nullable=False), _col("ts", "datetime", nullable=False),
                 _col("at", "datetime", nullable=False)],
        primary_key=["dev", "ts"],
    )
    assert set(zero_date_mappings(composite)) == {"at"}


def test_the_reader_passes_a_key_columns_zero_or_invalid_date_on_as_stored() -> None:
    converter = ValueConverter(_calendar())
    for stored in ("0000-00-00", "2020-00-15", "2020-02-31"):
        out = converter.convert_row({"d": stored, "v": "x", "seen": "0000-00-00"})
        assert out["d"] == stored             # as the driver returned it
        assert out["seen"] is None            # a nullable non-key column: NULL, as CDC
    assert converter.zero_date_entries() == (("seen", "zero", "NULL", 3),)


class _Dsql:
    """Aurora DSQL as far as this load meets it: a bare multi-row INSERT is atomic, refuses
    a key it holds or a key twice (23505), and refuses a date it has no value for -- the
    driver's str of one -- (22008); the loader's SELECT probe returns the keys it holds."""

    def __init__(self, columns: "list[str]", key: "list[str]") -> None:
        self.columns, self.key = columns, key
        self.rows: "dict[tuple, dict]" = {}

    def connect(self) -> "_Dsql._Connection":
        return _Dsql._Connection(self)

    class _Error(Exception):
        def __init__(self, sqlstate: str) -> None:
            super().__init__(f"sqlstate {sqlstate}")
            self.sqlstate = sqlstate

    class _Cursor:
        def __init__(self, db: "_Dsql") -> None:
            self._db, self._result, self.rowcount = db, [], -1

        def execute(self, query: Any, params: "Optional[list]" = None) -> None:
            db, values = self._db, list(params or [])
            text = query if isinstance(query, str) else query.as_string(None)
            if text.lstrip().upper().startswith("SELECT"):
                width = len(db.key)
                keys = [tuple(values[i:i + width]) for i in range(0, len(values), width)]
                self._result = [k for k in keys if k in db.rows]
                return
            width = len(db.columns)
            rows = [dict(zip(db.columns, values[i:i + width])) for i in range(0, len(values), width)]
            if any(isinstance(value, str) and re.match(r"\d{4}-\d{2}-\d{2}", value)
                   for row in rows for value in row.values()):
                raise _Dsql._Error("22008")
            keys = [tuple(row[name] for name in db.key) for row in rows]
            if len(set(keys)) < len(keys) or any(k in db.rows for k in keys):
                raise _Dsql._Error("23505")
            db.rows.update(zip(keys, rows))
            self.rowcount = len(rows)

        def fetchall(self) -> list:
            return self._result

        def close(self) -> None:
            pass

    class _Connection:
        def __init__(self, db: "_Dsql") -> None:
            self._db, self.autocommit, self.closed = db, True, False

        def cursor(self) -> "_Dsql._Cursor":
            return _Dsql._Cursor(self._db)

        def close(self) -> None:
            self.closed = True


def _load(table: TableDef, stored_rows: "list[dict]", batch_size: int, mode: OnConflictMode):
    converter = ValueConverter(table)
    rows = [converter.convert_row(row) for row in stored_rows]
    db = _Dsql([c.name for c in table.columns], list(table.primary_key))
    importer = BatchedImporter(
        BatchedImportOptions(batch_size=batch_size, parallelism=1, on_conflict=mode),
        connection_factory=db.connect, sleep=lambda _s: None, jitter=lambda: 0.0,
    )
    return importer.import_rows(rows, table, on_conflict=mode), db


@pytest.mark.parametrize("mode", [OnConflictMode.NONE, OnConflictMode.SKIP_EXISTING])
@pytest.mark.parametrize("batch_size", [2, 3000])
def test_a_zero_date_key_no_longer_takes_the_key_of_the_row_that_holds_that_date(
    mode: OnConflictMode, batch_size: int
) -> None:
    """The zero key's DEFAULT is the valid row's key. Mapped, the valid row was counted as
    already on the target (another batch) or the batch failed (the same one); as stored,
    the zero-key row alone is quarantined."""
    stored = [
        {"d": "0000-00-00", "v": "zero", "seen": None},
        {"d": date(1999, 12, 31), "v": "a", "seen": None},
        {"d": date(2000, 1, 1), "v": "REAL", "seen": None},
        {"d": date(2001, 1, 1), "v": "b", "seen": None},
    ]
    result, db = _load(_calendar(), stored, batch_size, mode)
    assert result.failures == 0 and result.first_error is None
    assert result.quarantined == 1 and result.conflicts == 0 and result.rows_loaded == 3
    assert db.rows[(date(2000, 1, 1),)]["v"] == "REAL"
    assert sorted(db.rows) == [(date(1999, 12, 31),), (date(2000, 1, 1),), (date(2001, 1, 1),)]


@pytest.mark.parametrize("batch_size", [1, 3000])
def test_composite_keys_that_held_different_zero_dates_stay_apart(batch_size: int) -> None:
    table = TableDef(
        name="app.ev",
        columns=[_col("dev", "int", nullable=False), _col("ts", "datetime", nullable=False),
                 _col("v", "varchar(10)")],
        primary_key=["dev", "ts"],
    )
    stored = [
        {"dev": 1, "ts": "0000-00-00 00:00:00", "v": "z1"},
        {"dev": 1, "ts": "2020-00-15 10:00:00", "v": "z2"},
        {"dev": 1, "ts": datetime(1970, 1, 1), "v": "epoch"},
        {"dev": 2, "ts": datetime(2020, 1, 1), "v": "b"},
    ]
    result, db = _load(table, stored, batch_size, OnConflictMode.NONE)
    assert result.failures == 0 and result.conflicts == 0
    # Two quarantined rows (the dedup of a key whose values the log withholds may count
    # them as one); the two valid rows, the real 1970-01-01 one among them, are loaded.
    assert result.quarantined >= 1 and result.rows_loaded == 2
    assert sorted(v["v"] for v in db.rows.values()) == ["b", "epoch"]


# ---------------------------------------------------------------------------
# 3. Validation renders a key column as stored
# ---------------------------------------------------------------------------


def test_validation_renders_a_key_column_as_stored_and_maps_the_others() -> None:
    table = _calendar()
    key, _value, seen = table.columns
    assert _mysql_checksum_expr(key, key=True) == "CAST(`d` AS CHAR)"
    assert _mysql_checksum_expr(key).startswith("CAST(CASE WHEN (YEAR(`d`) = 0")
    assert _mysql_checksum_expr(seen).startswith("CAST(CASE WHEN (YEAR(`seen`) = 0")
    for sql in (
        build_mysql_checksum_sql(table),
        build_mysql_page_checksum_first_sql(table, "d"),
        build_mysql_pk_token_sql(table, "d"),
    ):
        assert "CAST(`d` AS CHAR)" in sql and "YEAR(`d`)" not in sql
        assert "YEAR(`seen`) = 0" in sql
    # The row-diff sample selects and orders by the key itself.
    assert build_mysql_pk_token_sql(table, "d").startswith("SELECT `d` AS pk, ")
    assert build_mysql_pk_token_sql(table, "d").endswith("ORDER BY `d` LIMIT :sample_size")
    composite = TableDef(
        name="app.ev",
        columns=[_col("dev", "int", nullable=False), _col("ts", "datetime", nullable=False),
                 _col("at", "datetime")],
        primary_key=["dev", "ts"],
    )
    page = build_mysql_composite_page_sql(composite)
    assert "YEAR(`ts`)" not in page and "YEAR(`at`) = 0" in page


# ---------------------------------------------------------------------------
# 4. Evaluation names such keys
# ---------------------------------------------------------------------------


def _zero_dates_item(tables: "list[TableDef]"):
    report = CompatibilityAssessor().assess(
        SourceInventory(tables=tables, server_settings={"sql_mode": "NO_ENGINE_SUBSTITUTION"})
    )
    (item,) = [i for i in report.items if i.rule_id == ZERO_DATES_RULE_ID]
    return item


def test_zero_dates_names_a_temporal_primary_key_and_a_unique_key() -> None:
    keyed = _calendar()
    unique = TableDef(
        name="app.users",
        columns=[_col("id", "int", nullable=False), _col("born", "date", nullable=False),
                 _col("seen", "datetime")],
        primary_key=["id"],
        indexes=[IndexDef(name="uq_born", columns=["born"], unique=True),
                 IndexDef(name="ix_seen", columns=["seen"], unique=False)],
    )
    item = _zero_dates_item([keyed, unique])
    assert ("A primary key column is the exception (1 column: app.cal.d): mapped, rows whose "
            "keys hold such dates would fall onto one key") in item.risk
    assert "Aurora DSQL rejects (SQLSTATE 22008) -- the row is quarantined" in item.risk
    assert "CDC writes a change to such a row at the mapped key" in item.risk
    assert "In a unique key (1 column: app.users.born)" in item.risk
    assert "SQLSTATE 23505" in item.risk
    assert "and in every primary or unique key that holds such a date, correct the row" in \
        item.recommendation


def test_zero_dates_says_nothing_of_keys_where_no_key_holds_a_date() -> None:
    plain = TableDef(name="app.t", columns=[_col("id", "int", nullable=False), _col("d", "date")],
                     primary_key=["id"])
    item = _zero_dates_item([plain])
    assert "primary key column" not in item.risk and "unique key" not in item.risk
    assert "primary or unique key" not in item.recommendation


# ---------------------------------------------------------------------------
# 5. The manual
# ---------------------------------------------------------------------------


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"


def _chapter(language: str, name: str) -> str:
    return " ".join((_MANUAL / language / name).read_text(encoding="utf-8").split())


_KEY_QUARANTINED = {
    "en": "so such a row is quarantined",
    "ko": "그런 행은 격리되고",
    "ja": "そうした行は隔離され",
}
_UNIQUE_KEY = {
    "en": "A unique key over such a column.",
    "ko": "그런 컬럼에 걸린 유니크 키.",
    "ja": "そうした列にかかる一意キー。",
}
_FULL_LOAD_KEY = {
    "en": "A primary-key column's are left as stored",
    "ko": "기본 키 컬럼의 그런 값은 저장된 그대로 두며",
    "ja": "主キー列のそうした値は保存されたままになり",
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_a_key_is_left_as_stored_and_quotes_the_new_pin(language: str) -> None:
    limits = _chapter(language, "06-limitations.md").split("## 6.2", 1)[1].split("## 6.3", 1)[0]
    zero_dates = limits.split("`0000-00-00`", 1)[1].split("PostgreSQL", 1)[0]
    assert _KEY_QUARANTINED[language] in zero_dates and "`22008`" in zero_dates
    assert _UNIQUE_KEY[language] in zero_dates and "`23505`" in zero_dates
    # The read no longer fails on such a key, so the manual no longer says it can.
    assert "`1525`" not in zero_dates
    full_load = _chapter(language, "03-full-load.md").split("## 3.3", 1)[1].split("## 3.4", 1)[0]
    assert _FULL_LOAD_KEY[language] in full_load
    # Chapter 2 no longer quotes the tool's own session settings (a user guide since
    # 2026-10-25); it must still never state the old empty pin.
    evaluation = _chapter(language, "02-evaluation-and-schema-conversion.md")
    assert "sql_mode = ''" not in evaluation
