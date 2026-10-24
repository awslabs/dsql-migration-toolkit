# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G159: Full Load counts every quarantined row of a table keyed by a string or a date.

The loader dedups the rows it quarantines -- a retry re-reads rows an earlier attempt
already dropped -- by an identity of the row. That identity was the record's logged primary
key, which withholds a natural key's values (``code=<withheld>``, Property 7), so every
quarantined row of a table keyed by a string, a date or a decimal was ONE row: three dropped
rows were reported as one (the activity log, the error log, the table's count), and the
quarantine cap -- which stops a systematic error from splitting every batch down to single
rows -- never fired for such a table. A zero / invalid MySQL date in a primary key is
quarantined since G154, so such a table showed it at once.

The identity is now a digest of the row's key values (of the whole row without a key), held
in memory only and never logged; the records still withhold the values.

Doubles only (the fake Aurora DSQL store of test_batched_import): no database, no AWS.
"""

from __future__ import annotations

import threading

import pytest

from dsql_migrator.core.batched_import import (
    BatchedImporter,
    BatchedImportOptions,
    OnConflictMode,
    _BatchWork,
)
from dsql_migrator.core.models import ColumnDef, TableDef
from tests.test_batched_import import _FakeStore, _importer


def _keyed_table(name: str, key: "tuple[str, ...]", columns: "tuple[str, ...]") -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name=column, mysql_type="varchar(40)") for column in columns],
        primary_key=list(key),
    )


def _load(rows: list, poison: set, key: "list[str]", columns: "list[str]", **options):  # noqa: ANN003
    store = _FakeStore()
    store.poison_keys = poison
    importer = _importer(
        store, columns, key,
        options=BatchedImportOptions(on_conflict=OnConflictMode.NONE, **options),
    )
    result = importer.import_rows(iter(rows), _keyed_table("app.t", tuple(key), tuple(columns)), key_columns=key)
    return store, result


@pytest.mark.parametrize("batch_size", [2, 8])
def test_every_quarantined_row_of_a_string_keyed_table_is_counted(batch_size: int) -> None:
    rows = [{"code": f"c-{letter}", "name": letter} for letter in "abcdefgh"]
    store, result = _load(rows, {("c-b",), ("c-d",), ("c-f",)}, ["code"], ["code", "name"], batch_size=batch_size)
    assert result.quarantined == 3
    assert [record.primary_key for record in result.quarantine_records] == ["code=<withheld>"] * 3
    assert sorted(key for (key,) in store.rows) == ["c-a", "c-c", "c-e", "c-g", "c-h"]
    assert result.failures == 0


def test_every_quarantined_row_of_a_composite_natural_key_is_counted() -> None:
    """(dev, ts) as G154's zero-date keys reach Aurora DSQL: as stored, each rejected."""
    rows = [
        {"dev": "d1", "ts": "0000-00-00 00:00:00", "v": "x"},
        {"dev": "d1", "ts": "2020-00-15 10:00:00", "v": "y"},
        {"dev": "d1", "ts": "2020-01-15 10:00:00", "v": "z"},
    ]
    poison = {("d1", "0000-00-00 00:00:00"), ("d1", "2020-00-15 10:00:00")}
    _store, result = _load(rows, poison, ["dev", "ts"], ["dev", "ts", "v"], batch_size=3)
    assert result.quarantined == 2
    assert {record.primary_key for record in result.quarantine_records} == {"dev=<withheld>, ts=<withheld>"}


def test_the_quarantine_cap_fires_for_a_string_keyed_table(monkeypatch) -> None:  # noqa: ANN001
    """A systematic error must stop at the cap, not split every batch to single rows."""
    import dsql_migrator.core.batched_import as bi

    monkeypatch.setattr(bi, "_MAX_QUARANTINE_RECORDS", 3)
    rows = [{"code": f"c-{index}", "name": "n"} for index in range(8)]
    _store, result = _load(rows, {(row["code"],) for row in rows}, ["code"], ["code", "name"], batch_size=8)
    assert result.failures >= 1  # aborted loudly
    assert result.quarantined <= 3 and len(result.quarantine_records) <= 3


def _bare_importer() -> BatchedImporter:
    importer = BatchedImporter.__new__(BatchedImporter)
    importer._quarantine_lock = threading.Lock()
    importer._quarantine = []
    importer._quarantine_total = 0
    importer._quarantine_seen = set()
    importer._max_quarantine = 1000
    return importer


class _Poison(Exception):
    sqlstate = "22008"


def _work(row: dict, key: "tuple[str, ...]" = ("email",)) -> _BatchWork:
    return _BatchWork(
        chunk_id="app.users#0", table_name="app.users", columns=tuple(row), key_columns=key,
        on_conflict=OnConflictMode.NONE, rows=(row,),
    )


def test_a_re_read_row_still_counts_once_and_its_key_is_never_held_in_clear() -> None:
    importer = _bare_importer()
    importer._quarantine_one(_work({"email": "alice@example.com", "n": 1}), _Poison("bad date"))
    importer._quarantine_one(_work({"email": "bob@example.com", "n": 2}), _Poison("bad date"))
    importer._quarantine_one(_work({"email": "alice@example.com", "n": 1}), _Poison("bad date, again"))
    assert importer._quarantine_total == 2 and len(importer._quarantine) == 2
    held = repr(importer._quarantine_seen) + repr(importer._quarantine)
    assert "alice" not in held and "bob" not in held


def test_rows_of_a_table_without_a_key_are_told_apart_by_their_values() -> None:
    importer = _bare_importer()
    importer._quarantine_one(_work({"a": 1, "b": "x"}, key=()), _Poison("bad"))
    importer._quarantine_one(_work({"a": 2, "b": "x"}, key=()), _Poison("bad"))
    importer._quarantine_one(_work({"a": 1, "b": "x"}, key=()), _Poison("bad"))
    assert importer._quarantine_total == 2
    assert [record.primary_key for record in importer._quarantine] == ["(no primary key)"] * 2


def test_a_resumed_import_carries_the_identities_of_the_rows_it_dropped() -> None:
    from types import SimpleNamespace

    store = _FakeStore()
    store.poison_keys = {("c-b",)}
    columns, key = ["code", "name"], ["code"]
    importer = _importer(store, columns, key, options=BatchedImportOptions(on_conflict=OnConflictMode.NONE, batch_size=2))
    table = _keyed_table("app.t", ("code",), ("code", "name"))
    job = SimpleNamespace(resume_batch_watermark={})
    rows = [{"code": f"c-{letter}", "name": letter} for letter in "abcd"]
    assert importer.import_rows(iter(rows), table, key_columns=key, job=job).quarantined == 1
    assert job.resume_batch_watermark  # the committed prefix the next attempt skips
    resumed = importer.import_rows(iter(rows), table, key_columns=key, job=job)
    assert resumed.quarantined == 1 and len(resumed.quarantine_records) == 1
    # The same row re-read and dropped again is the same row.
    work = _BatchWork(
        chunk_id="app.t#0", table_name="app.t", columns=tuple(columns), key_columns=tuple(key),
        on_conflict=OnConflictMode.NONE, rows=({"code": "c-b", "name": "b"},),
    )
    importer._quarantine_one(work, _Poison("bad"))
    assert importer._quarantine_total == 1
