# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The CDC step's two ad-hoc source reads dispose the engine they build (review item A6a).

"Refresh source/target counts" (``_cdc_status._fetch_migration_row_counts``, every click) and
the CDC partition sizing of a deploy before any Full Load (``_cdc_ui._estimate_cdc_table_rows``)
each build a source engine from ``make_source_engine_factory`` -- a plain SQLAlchemy
``QueuePool`` -- and never disposed it, so its pooled connection to the (production) source
stayed open until garbage collection. Every other one-shot source read on this step disposes
in a ``finally``; these two now do too, on success and when the read fails. Offline: the
engine, the dialect and the estimate reads are doubles.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest


class _Engine:
    """A source engine double that counts ``dispose()`` and can fail its read."""

    def __init__(self, *, connect_error: Exception | None = None) -> None:
        self.disposed = 0
        self.connect_error = connect_error

    @contextmanager
    def connect(self):
        if self.connect_error is not None:
            raise self.connect_error
        yield SimpleNamespace(execution_options=lambda **_k: None)

    def dispose(self) -> None:
        self.disposed += 1


class _Dialect:
    def prepare_estimate_session(self, _connection):
        return None

    def estimate_stats_at(self, _connection, _tables):
        return {}


def _session():
    return SimpleNamespace(
        source_config=SimpleNamespace(source_type=None),
        source_password=object(),
        has_source=lambda: True,
        has_target=lambda: False,
        target_config=None,
    )


def _patch_reads(monkeypatch, *, estimate_error: Exception | None = None):
    import dsql_migrator.core.source_dialect as sd
    import dsql_migrator.core.watermark as wm

    def _estimate(_connection, tables, _dialect):
        if estimate_error is not None:
            raise estimate_error
        return {name: 7 for name in tables}

    monkeypatch.setattr(sd, "dialect_for", lambda _t: _Dialect())
    monkeypatch.setattr(wm, "estimate_source_rows", _estimate)
    monkeypatch.setattr(wm, "max_pk_source", lambda _c, _p, _d: {})


@pytest.mark.parametrize(
    "failure", [None, "read", "connect"], ids=["success", "read-fails", "connect-fails"]
)
def test_refresh_counts_disposes_the_source_engine(monkeypatch, failure) -> None:
    from dsql_migrator.ui.data_migration import DataMigrationState, _cdc_status

    engine = _Engine(connect_error=OSError("source unreachable") if failure == "connect" else None)
    monkeypatch.setattr(
        _cdc_status, "make_source_engine_factory", lambda _pw: lambda _c: engine
    )
    _patch_reads(
        monkeypatch, estimate_error=RuntimeError("refused") if failure == "read" else None
    )
    source_counts, _t, _smax, _tmax, _at, available = _cdc_status._fetch_migration_row_counts(
        DataMigrationState(), _session(), ["app.t"]
    )
    assert engine.disposed == 1
    # The read's own outcome is unchanged: counts on success, a blank source side otherwise.
    assert available is (failure is None)
    assert source_counts == ({"app.t": 7} if failure is None else {"app.t": None})


@pytest.mark.parametrize(
    "failure", [None, "read", "connect"], ids=["success", "read-fails", "connect-fails"]
)
def test_cdc_partition_sizing_disposes_the_source_engine(monkeypatch, failure) -> None:
    import dsql_migrator.ui.connect as connect
    from dsql_migrator.ui.data_migration import _cdc_ui

    engine = _Engine(connect_error=OSError("source unreachable") if failure == "connect" else None)
    monkeypatch.setattr(connect, "make_source_engine_factory", lambda _pw: lambda _c: engine)
    _patch_reads(
        monkeypatch, estimate_error=RuntimeError("refused") if failure == "read" else None
    )
    estimates = _cdc_ui._estimate_cdc_table_rows(_session(), ["app.t"])
    assert engine.disposed == 1
    # Best-effort as before: the estimates on success, None (uniform sizing) otherwise.
    assert estimates == ({"app.t": 7} if failure is None else None)


def test_the_source_catalog_read_timeout_keeps_its_old_name() -> None:
    # The start-point, GTID and catalog reads share one bound, so it is named for what it is;
    # the old catalog-drift name stays an alias for anything that imports it.
    from dsql_migrator.ui.data_migration import _cdc_status

    assert _cdc_status._SOURCE_CATALOG_READ_TIMEOUT_SECONDS == 10
    assert (
        _cdc_status._CATALOG_DRIFT_READ_TIMEOUT_SECONDS
        == _cdc_status._SOURCE_CATALOG_READ_TIMEOUT_SECONDS
    )
