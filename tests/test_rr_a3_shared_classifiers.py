# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A3, part 1 (no behavior change): one definition each of "the target connection is gone"
and of the source-error candidate walk.

* ``target_connection.is_dropped_connection`` -- class 08 / no SQLSTATE, or a session the
  server ended (57P01 / 57P02 / 57P03, G141) -- is the shared, public classifier. Validation
  keeps it under its historical private name, and Full Load's target catalog read no longer
  imports a Validation private to get it.
* ``source_dialect.base.error_candidates`` -- ``exc``, its ``.orig``, its ``.__cause__`` -- is
  the walk the MySQL and PostgreSQL source classifiers share. It never follows
  ``.__context__``.
"""

from __future__ import annotations

import inspect

import psycopg
import pytest

from dsql_migrator.core import target_connection, validator
from dsql_migrator.core.source_dialect.base import error_candidates
from dsql_migrator.core.target_connection import is_dropped_connection


@pytest.mark.parametrize("error, dropped", [
    (psycopg.errors.lookup("08006")("server closed the connection unexpectedly"), True),
    (psycopg.errors.lookup("08P01")("protocol violation"), True),
    (psycopg.OperationalError("consuming input failed: SSL SYSCALL error: EOF detected"), True),
    (psycopg.errors.lookup("57P01")("terminating connection due to administrator command"), True),
    (psycopg.errors.lookup("57P02")("terminating connection due to crash"), True),
    (psycopg.errors.lookup("57P03")("the database system is starting up"), True),
    # A cancelled statement leaves the session; server unavailable keeps it (Aurora DSQL).
    (psycopg.errors.lookup("57014")("canceling statement due to statement timeout"), False),
    (psycopg.errors.lookup("XX000")("server unavailable"), False),
    (psycopg.errors.lookup("23505")("duplicate key value"), False),
    (psycopg.DataError("PostgreSQL text fields cannot contain NUL (0x00) bytes"), False),
    (ValueError("not a driver error"), False),
], ids=lambda v: getattr(v, "sqlstate", None) or type(v).__name__ if isinstance(v, BaseException)
   else str(v))
def test_is_dropped_connection(error, dropped) -> None:
    assert is_dropped_connection(error) is dropped


def test_validation_keeps_its_historical_name_for_the_shared_classifier() -> None:
    assert validator._is_dropped_connection is target_connection.is_dropped_connection


def test_full_load_reads_the_target_catalog_with_the_public_classifier() -> None:
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    source = inspect.getsource(engine._target_columns_the_conversion_would_drop)
    assert "is_dropped_connection(exc)" in source
    assert "core.validator import" not in source


class _Wrapper(Exception):
    def __init__(self, orig: BaseException) -> None:
        super().__init__("wrapped")
        self.orig = orig


def test_error_candidates_walks_orig_then_cause() -> None:
    plain = ValueError("x")
    assert error_candidates(plain) == [plain]

    orig = psycopg.errors.lookup("57P01")("terminating connection")
    wrapped = _Wrapper(orig)
    assert error_candidates(wrapped) == [wrapped, orig]

    cause = psycopg.errors.lookup("53300")("too many clients")
    try:
        try:
            raise cause
        except Exception as exc:
            raise RuntimeError("re-raised") from exc
    except RuntimeError as raised:
        assert error_candidates(raised) == [raised, cause]


def test_error_candidates_never_follows_the_context_or_itself() -> None:
    context = psycopg.errors.lookup("57P01")("terminating connection")
    try:
        try:
            raise context
        except Exception:
            raise ValueError("raised while handling it")
    except ValueError as raised:
        assert raised.__context__ is context
        assert error_candidates(raised) == [raised]

    selfish = _Wrapper(None)  # type: ignore[arg-type]
    selfish.orig = selfish
    assert error_candidates(selfish) == [selfish]
