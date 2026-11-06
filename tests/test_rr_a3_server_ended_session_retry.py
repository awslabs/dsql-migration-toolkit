# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A3 (X3): the Full Load loader treats a session the server ended as a lost connection.

Aurora DSQL (or a ``pg_terminate_backend``, a shutdown, a restart) can end a loader's session
with SQLSTATE 57P01 admin_shutdown, 57P02 crash_shutdown or 57P03 cannot_connect_now. That is
a lost connection, as after a class-08 / no-SQLSTATE drop -- Validation's target reads and
Full Load's target catalog read already counted it as one (G141), and the CDC sink treats
class 57 as transient. The batch loader did not: ``_is_retryable_load_error`` saw a
SQLSTATE that was neither 40001, XX000 nor class 08, so ``_load_batch`` took it for a
poison row and binary-split the batch, and a second hit on a one-row half QUARANTINED A
HEALTHY ROW (it was reported in the error log and never loaded).

Pinned here:

* the classifier: 57P01 / 57P02 / 57P03 are retryable, 57014 (a cancelled statement) is not;
* a server-ended session retries the WHOLE batch on a fresh connection instead of splitting
  it, and a one-row half that meets it is loaded, never quarantined;
* the retry stays inside the existing bounded budget: one that outlasts it fails the batch
  (retryable, Retry unfinished tables), nothing split or quarantined;
* the post-load index pass retries it too, and a CREATE whose session the server ended
  while it ran is reported "not confirmed" (it may have landed), as after any drop (G25).
"""

from __future__ import annotations

import psycopg
import pytest

from dsql_migrator.core.batched_import import (
    UNCONFIRMED_INDEX_DROP_NOTE,
    OnConflictMode,
    _is_retryable_load_error,
)

from tests.test_batched_import import _UNIQUE_DDL, _FakeStore, _rows, _table
from tests.test_full_load_counters import _failing_factory, _importer
from tests.test_g25_fl import _Catalog
from tests.test_g25_fl import _importer as _index_importer

_SERVER_ENDED = ["57P01", "57P02", "57P03"]


def _ended(sqlstate: str) -> psycopg.Error:
    """The real psycopg error for a session the server ended (AdminShutdown, ...)."""
    return psycopg.errors.lookup(sqlstate)("terminating connection due to administrator command")


@pytest.mark.parametrize("sqlstate", _SERVER_ENDED)
def test_a_server_ended_session_is_a_retryable_load_error(sqlstate) -> None:
    assert _is_retryable_load_error(_ended(sqlstate)) is True


def test_a_cancelled_statement_is_still_not_retryable() -> None:
    # 57014 (a statement timeout or a cancel) is class 57 but no ended session.
    assert _is_retryable_load_error(psycopg.errors.lookup("57014")("canceling statement")) is False


@pytest.mark.parametrize("sqlstate", _SERVER_ENDED)
def test_a_server_ended_session_retries_the_batch_instead_of_splitting_it(sqlstate) -> None:
    store = _FakeStore()
    errors = [_ended(sqlstate)]
    importer = _importer(_failing_factory(store, ["id", "name"], ["id"], errors), batch_size=8)

    result = importer.import_rows(_rows(8), _table(), on_conflict=OnConflictMode.NONE)

    assert (result.rows_loaded, result.quarantined, result.failures) == (8, 0, 0)
    assert sorted(store.rows) == [(i,) for i in range(8)]
    s = importer.load_stats.snapshot()
    # ONE statement, run twice (the retry), not three statements (the batch and its halves).
    assert s["retries"] == {sqlstate: 1}
    assert s["attempts_per_statement"] == {2: 1}
    assert (s["batches"], s["batches_failed"]) == (1, 0)


@pytest.mark.parametrize("sqlstate", _SERVER_ENDED)
def test_a_one_row_half_that_meets_a_server_ended_session_is_loaded_not_quarantined(
    sqlstate,
) -> None:
    # The reported shape: the batch meets the ended session, then (a restart still under way)
    # so does its retry. Split, the second hit landed on a one-row half and quarantined a
    # healthy row; retried, every row loads.
    store = _FakeStore()
    errors = [_ended(sqlstate), _ended(sqlstate)]
    importer = _importer(_failing_factory(store, ["id", "name"], ["id"], errors), batch_size=2)

    result = importer.import_rows(_rows(2), _table(), on_conflict=OnConflictMode.NONE)

    assert (result.rows_loaded, result.quarantined, result.failures) == (2, 0, 0)
    assert sorted(store.rows) == [(0,), (1,)]
    assert importer.load_stats.snapshot()["retries"] == {sqlstate: 2}


def test_server_ended_sessions_that_outlast_the_budget_fail_the_batch_not_a_row() -> None:
    store = _FakeStore()
    errors = [_ended("57P01") for _ in range(10)]
    importer = _importer(
        _failing_factory(store, ["id", "name"], ["id"], errors), batch_size=4, attempts=3
    )

    result = importer.import_rows(_rows(4), _table(), on_conflict=OnConflictMode.NONE)

    # The existing budget bounds it: 3 attempts, then the batch fails -- retryable, nothing
    # quarantined, nothing split.
    assert (result.rows_loaded, result.quarantined, result.failures) == (0, 0, 1)
    assert store.rows == {}
    assert len(errors) == 7  # exactly the 3 attempts were made
    s = importer.load_stats.snapshot()
    assert s["retries"] == {"57P01": 3} and s["retries_exhausted"] == 1
    assert (s["batches"], s["batches_failed"], s["attempts"]) == (1, 1, 3)


def test_the_index_pass_retries_a_server_ended_session() -> None:
    # The CREATE never ran: the retry creates it.
    catalog = _Catalog(script=[(lambda: _ended("57P01"), False)])
    assert _index_importer(catalog).create_indexes([_UNIQUE_DDL]) == (1, [])
    assert (catalog.names, catalog.executes) == ({"ux_a"}, 2)


def test_a_create_whose_session_the_server_ended_after_it_landed_is_not_confirmed() -> None:
    # The CREATE committed before the session ended: the retry's 42P07 is not "created", and
    # not a missing index either -- it is reported as possibly created (G25).
    catalog = _Catalog(script=[(lambda: _ended("57P01"), True)])
    created, (failure,) = _index_importer(catalog).create_indexes([_UNIQUE_DDL])
    assert (created, catalog.executes, "ux_a" in catalog.names) == (0, 2, True)
    assert failure == (
        'UNIQUE ux_a: DuplicateTable: relation "ux_a" already exists' + UNCONFIRMED_INDEX_DROP_NOTE
    )
