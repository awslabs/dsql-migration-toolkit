# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Offline guards for the live release gate (scripts/release_gate.py).

The gate's VALUE is that it fails when a live DSQL connection can't be made or its
options are rejected (the v0.1.438 lc_numeric regression class). These tests prove the
gate FAILS CLOSED — it never reports PASS when the target is unset or the real connector
raises — without touching AWS. (The gate's happy path is exercised live before a release,
not in CI.)
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


def test_source_check_skips_when_unconfigured(gate, monkeypatch):
    for k in ("DB_HOST", "DB_PASSWORD", "SOURCE_TYPE"):
        monkeypatch.delenv(k, raising=False)
    ok, detail = gate.check_source()
    assert ok is True
    assert "skipped" in detail.lower()
