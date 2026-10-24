# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G187 (FX-2): a zero or invalid MySQL date in a column of a UNIQUE index.

G154 left a primary-key column's zero and invalid dates as stored, but G150 still wrote the
other columns' as the bundled CDC writes them (NULL, the column's DEFAULT or 1970-01-01) --
a UNIQUE index's too. Mapped, different such dates fall onto one value, or onto the value a
real row holds: in the default flow (Schema Conversion Apply creates the unique index, the
first Full Load appends) the batch's 23505 is not a key overlap, the select-filter found no
key on the target and retried until "exhausted 10 attempts due to concurrent unique
violations", and the table FAILED with its valid rows unloaded (live: MySQL 8.0.46 into
PostgreSQL 17 standing in for Aurora DSQL); a Drop & reload loaded the duplicates and failed
the index. ZERO_DATES and the manual said only that the index fails to build.

Now such a column is left as stored too (``unmapped_columns``): Aurora DSQL rejects the value
(22008) and that row alone is quarantined, as v0.1.574 did, and Validation renders the
column as stored. ZERO_DATES and the manual say so. Doubles only.
"""

from __future__ import annotations

import re
from datetime import date
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
from dsql_migrator.core.models import ColumnDef, IndexDef, SourceInventory, TableDef
from dsql_migrator.core.mysql_zero_dates import unmapped_columns, zero_date_mappings
from dsql_migrator.core.validation_sql import build_mysql_checksum_sql, build_mysql_pk_token_sql


def _col(name: str, mysql_type: str, *, nullable: bool = True, default=None) -> ColumnDef:
    return ColumnDef(name=name, mysql_type=mysql_type, nullable=nullable, default=default)


def _uq() -> TableDef:
    """uq(id PK, d DATE NOT NULL DEFAULT '2000-01-01' UNIQUE, seen DATE (indexed, not
    unique), note) -- the reviewed table, with a mapped column beside it."""
    return TableDef(
        name="fz.uq",
        columns=[
            _col("id", "int", nullable=False),
            _col("d", "date", nullable=False, default="2000-01-01"),
            _col("seen", "date"),
            _col("note", "varchar(10)"),
        ],
        primary_key=["id"],
        indexes=[IndexDef(name="uq_d", columns=["d"], unique=True),
                 IndexDef(name="ix_seen", columns=["seen"], unique=False)],
    )


def test_a_unique_keys_columns_are_not_mapped_and_the_others_are() -> None:
    table = _uq()
    assert unmapped_columns(table) == {"id", "d"}
    assert set(zero_date_mappings(table)) == {"seen"}
    composite = TableDef(
        name="fz.sd",
        columns=[_col("id", "int", nullable=False), _col("email", "varchar(20)", nullable=False),
                 _col("deleted_at", "datetime", nullable=False,
                      default="0000-00-00 00:00:00")],
        primary_key=["id"],
        indexes=[IndexDef(name="uq_sd", columns=["email", "deleted_at"], unique=True)],
    )
    assert zero_date_mappings(composite) == {}


def test_the_reader_passes_a_unique_columns_zero_or_invalid_date_on_as_stored() -> None:
    converter = ValueConverter(_uq())
    for stored in ("0000-00-00", "2020-00-15", "2020-02-31"):
        out = converter.convert_row({"id": 1, "d": stored, "seen": "0000-00-00", "note": "x"})
        assert out["d"] == stored
        assert out["seen"] is None
    assert converter.zero_date_entries() == (("seen", "zero", "NULL", 3),)


class _Dsql:
    """Aurora DSQL as far as this load meets it: a multi-row INSERT is atomic and refuses a
    date it has no value for (the driver's str of one, 22008) before a key or a UNIQUE value
    it holds or meets twice (23505; NULLs never collide); the loader's SELECT probe returns
    the primary keys it holds."""

    def __init__(self, columns: "list[str]", key: "list[str]", unique: "list[str]") -> None:
        self.columns, self.key, self.unique = columns, key, unique
        self.rows: "dict[tuple, dict]" = {}

    class _Error(Exception):
        def __init__(self, sqlstate: str) -> None:
            super().__init__(f"sqlstate {sqlstate}")
            self.sqlstate = sqlstate

    def connect(self) -> "_Dsql._Connection":
        return _Dsql._Connection(self)

    def insert(self, rows: "list[dict]") -> None:
        if any(isinstance(value, str) and re.match(r"\d{4}-\d{2}-\d{2}", value)
               for row in rows for value in row.values()):
            raise _Dsql._Error("22008")
        keys = [tuple(row[name] for name in self.key) for row in rows]
        held = {tuple(row[name] for name in self.unique) for row in self.rows.values()}
        values = [tuple(row[name] for name in self.unique) for row in rows]
        values = [value for value in values if None not in value]
        if (len(set(keys)) < len(keys) or any(k in self.rows for k in keys)
                or len(set(values)) < len(values) or any(v in held for v in values)):
            raise _Dsql._Error("23505")
        self.rows.update(zip(keys, rows))

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
            rows = [dict(zip(db.columns, values[i:i + width]))
                    for i in range(0, len(values), width)]
            db.insert(rows)
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


@pytest.mark.parametrize("mode", [OnConflictMode.SKIP_EXISTING, OnConflictMode.NONE])
@pytest.mark.parametrize("batch_size", [2, 3000])
def test_the_zero_and_invalid_rows_alone_are_quarantined_and_the_real_row_loads(
    mode: OnConflictMode, batch_size: int
) -> None:
    """The reviewed rows: the zero date's DEFAULT is a real row's value, and a zero month
    maps onto it too. Mapped, the append exhausted its retries ("concurrent unique
    violations") and loaded nothing of the batch, the real row included."""
    table = _uq()
    converter = ValueConverter(table)
    stored = [
        {"id": 1, "d": "0000-00-00", "seen": None, "note": "zero"},
        {"id": 2, "d": date(2000, 1, 1), "seen": None, "note": "REAL"},
        {"id": 3, "d": "2020-00-15", "seen": None, "note": "zpart"},
        {"id": 4, "d": date(2021, 5, 5), "seen": "0000-00-00", "note": "ok"},
    ]
    db = _Dsql([c.name for c in table.columns], ["id"], ["d"])
    importer = BatchedImporter(
        BatchedImportOptions(batch_size=batch_size, parallelism=1, on_conflict=mode),
        connection_factory=db.connect, sleep=lambda _s: None, jitter=lambda: 0.0,
    )
    result = importer.import_rows(
        [converter.convert_row(row) for row in stored], table, on_conflict=mode
    )
    assert result.failures == 0 and result.first_error is None
    assert result.quarantined == 2 and result.rows_loaded == 2
    assert {key: row["note"] for key, row in db.rows.items()} == {(2,): "REAL", (4,): "ok"}
    # The column beside it is still written as CDC writes it.
    assert db.rows[(4,)]["seen"] is None


def test_validation_renders_a_unique_column_as_stored_and_maps_the_others() -> None:
    table = _uq()
    for sql in (build_mysql_checksum_sql(table), build_mysql_pk_token_sql(table, "id")):
        assert "CAST(`d` AS CHAR)" in sql and "YEAR(`d`)" not in sql
        assert "YEAR(`seen`) = 0" in sql


def test_zero_dates_says_the_unique_keys_row_is_quarantined() -> None:
    report = CompatibilityAssessor().assess(
        SourceInventory(tables=[_uq()], server_settings={"sql_mode": "NO_ENGINE_SUBSTITUTION"})
    )
    (item,) = [i for i in report.items if i.rule_id == ZERO_DATES_RULE_ID]
    assert ("In a unique key (1 column: fz.uq.d), Full Load leaves such a date as stored too: "
            "mapped, different such dates could become one value, or the value another row "
            "holds, and the load would fail on the duplicate.") in item.risk
    assert ("Aurora DSQL rejects the stored value (SQLSTATE 22008), so the row is quarantined "
            "and Validation reports the difference") in item.risk
    assert "dead-letters it when another row holds that value (SQLSTATE 23505)" in item.risk
    assert "fails to build" not in item.risk


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"


def _chapter(language: str, name: str) -> str:
    return " ".join((_MANUAL / language / name).read_text(encoding="utf-8").split())


_UNIQUE_BULLET = {
    "en": ("**A unique key over such a column.**", "**The application's SQL.**",
           ("Full Load leaves the value of a unique key's column as stored too",
            "the load would fail on the duplicate", "so such a row is quarantined",
            "dead-letters it when another row holds that value (`23505`)"),
           "fails to build"),
    "ko": ("**그런 컬럼에 걸린 유니크 키.**", "**애플리케이션의 SQL.**",
           ("Full Load는 유니크 키 컬럼의 값도 저장된 그대로 둡니다", "로드가 그 중복에서 실패하기",
            "그런 행은 격리되고", "다른 행이 그 값을 가지고 있으면 dead-letter로 보냅니다(`23505`)"),
           "인덱스 생성이 실패"),
    "ja": ("**そうした列にかかる一意キー。**", "**アプリケーションの SQL。**",
           ("Full Load は一意キーの列の値も保存されたままにします", "ロードがその重複で失敗する",
            "そうした行は隔離され", "別の行がその値を持つ場合はデッドレターにします"),
           "インデックスの作成が失敗"),
}
_FULL_LOAD = {
    "en": "A primary-key column's are left as stored, and so are a unique key's",
    "ko": "기본 키 컬럼의 그런 값은 저장된 그대로 두며(유니크 키 컬럼의 값도 마찬가지)",
    "ja": "主キー列のそうした値は保存されたままになり(一意キーの列の値も同様です)",
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_a_unique_keys_row_is_quarantined(language: str) -> None:
    head, tail, claims, gone = _UNIQUE_BULLET[language]
    limits = _chapter(language, "06-limitations.md")
    bullet = limits.split(head, 1)[1].split(tail, 1)[0]
    for claim in claims:
        assert claim in bullet, (language, claim)
    assert "`22008`" in bullet and gone not in bullet, language
    assert _FULL_LOAD[language] in _chapter(language, "03-full-load.md"), language
