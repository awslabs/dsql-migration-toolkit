# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Offline guards for the live release gate (scripts/release_gate.py).

The gate's VALUE is that it fails when a live DSQL connection can't be made or its
options are rejected (the v0.1.438 lc_numeric regression class). These tests prove the
gate FAILS CLOSED — it never reports PASS when the target is unset or the real connector
raises — without touching AWS. (The gate's happy path is exercised live before a release,
not in CI; only the ``--roundtrip`` load through the Full Load write path also runs here,
on doubles, so its code path is covered and its read-back check fails closed.)
"""
from __future__ import annotations

import importlib
import os
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "scripts"))


@pytest.fixture()
def gate(monkeypatch):
    mod = importlib.import_module("release_gate")
    # Neutralize any real .env so cfg() sees only what the test sets.
    monkeypatch.setattr(mod, "_ENV", {}, raising=False)
    for k in ("TARGET_ENDPOINT", "TARGET_REGION", "TARGET_DATABASE", "TARGET_USERNAME"):
        monkeypatch.delenv(k, raising=False)
    return mod


def test_gate_fails_closed_when_target_unset(gate):
    ok, detail = gate.check_dsql(roundtrip=False)
    assert ok is False
    assert "TARGET_ENDPOINT" in detail


def test_gate_fails_closed_when_dsql_connect_raises(gate, monkeypatch):
    # A configured target, but the REAL connector raises (as it did on the lc_numeric
    # FATAL). The gate must surface that as FAIL, never swallow it into PASS.
    monkeypatch.setenv("TARGET_ENDPOINT", "c.dsql.ap-northeast-2.on.aws")

    import dsql_migrator.core.target_connection as tc

    class _Boom:
        def __init__(self, *a, **k):
            pass

        def connect(self):
            raise RuntimeError('FATAL: setting configuration parameter "lc_numeric" not supported')

    monkeypatch.setattr(tc, "DsqlConnector", _Boom)
    ok, detail = gate.check_dsql(roundtrip=False)
    assert ok is False
    assert "FAILED" in detail and "lc_numeric" in detail


class _GateServer:
    """The scratch statements of ``--roundtrip`` against an in-memory target, reached by
    every connection the (doubled) connector opens -- the gate's own and the importer's."""

    def __init__(self, *, lose_a_row: bool = False) -> None:
        self.gate_rows: list = []
        self.importer_inserts: list = []  # the rendered statements as sent, in order
        self.lose_a_row = lose_a_row


class _GateCursor:
    def __init__(self, server: _GateServer) -> None:
        self._server = server
        self._result: list = []
        self.rowcount = -1

    def execute(self, query, params=None):
        from decimal import Decimal

        text = query if isinstance(query, str) else query.as_string(None)
        server = self._server
        if text == "SELECT 1":
            self._result = [(1,)]
        elif text.startswith("SHOW "):
            self._result = [("ISO",)]
        elif text.startswith(("DROP SCHEMA", "CREATE SCHEMA", "CREATE TABLE")):
            self._result = []
        elif text.startswith('INSERT INTO "release_gate_smoke"."gate" '):
            self._result = []  # the literal one-row INSERT
        elif text.startswith("SELECT amount, note"):
            self._result = [(Decimal("3.14"), "gate")]
        elif text.startswith('INSERT INTO "release_gate_smoke"."gate_rows" '):
            server.importer_inserts.append(text)
            width = 5
            rows = [tuple(params[i : i + width]) for i in range(0, len(params), width)]
            server.gate_rows.extend(rows)
            self.rowcount = len(rows)
        elif text.startswith("SELECT id, amount, note, ts"):
            rows = sorted(server.gate_rows, key=lambda row: row[0])
            self._result = rows[:-1] if server.lose_a_row else rows
        else:
            raise AssertionError(f"unexpected statement: {text[:80]}")

    def fetchone(self):
        return self._result[0]

    def fetchall(self):
        return list(self._result)

    def close(self):
        pass


class _GateConnection:
    def __init__(self, server: _GateServer) -> None:
        self._server = server
        self.autocommit = True

    def cursor(self):
        return _GateCursor(self._server)

    def close(self):
        pass


def _doubled_connector(monkeypatch, server: _GateServer) -> None:
    import dsql_migrator.core.target_connection as tc

    class _Connector:
        def __init__(self, *a, **k):
            pass

        def connect(self):
            return _GateConnection(server)

    monkeypatch.setattr(tc, "DsqlConnector", _Connector)


def test_roundtrip_loads_rows_through_the_full_load_write_path(gate, monkeypatch):
    # The --roundtrip tier runs the real BatchedImporter on the connector's connections:
    # rendered multi-row INSERTs with $n placeholders, two rows per batch.
    monkeypatch.setenv("TARGET_ENDPOINT", "c.dsql.ap-northeast-2.on.aws")
    server = _GateServer()
    _doubled_connector(monkeypatch, server)

    ok, detail = gate.check_dsql(roundtrip=True)

    assert ok is True, detail
    assert "importer round-trip ok (5 rows in 3 multi-row INSERTs)" in detail
    inserts = server.importer_inserts
    assert len(inserts) == 3
    assert inserts[0] == inserts[1] == (
        'INSERT INTO "release_gate_smoke"."gate_rows" ("id", "amount", "note", "ts", '
        '"pct%off") VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10) '
    )
    assert inserts[2].endswith("VALUES ($1, $2, $3, $4, $5) ")
    # The values were bound, never interpolated -- the placeholder-looking one included.
    assert all("100%" not in text for text in inserts)
    assert [row[0] for row in server.gate_rows] == [1, 2, 3, 4, 5]


def test_roundtrip_fails_closed_when_the_rows_do_not_come_back(gate, monkeypatch):
    monkeypatch.setenv("TARGET_ENDPOINT", "c.dsql.ap-northeast-2.on.aws")
    _doubled_connector(monkeypatch, _GateServer(lose_a_row=True))

    ok, detail = gate.check_dsql(roundtrip=True)

    assert ok is False
    assert "DSQL smoke FAILED" in detail and "mismatch on id(s) [5]" in detail


def test_source_check_skips_when_unconfigured(gate, monkeypatch):
    for k in ("DB_HOST", "DB_PASSWORD", "SOURCE_TYPE"):
        monkeypatch.delenv(k, raising=False)
    ok, detail = gate.check_source()
    assert ok is True
    assert "skipped" in detail.lower()
