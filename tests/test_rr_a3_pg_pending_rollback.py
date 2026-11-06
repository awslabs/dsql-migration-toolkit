# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A3 (CI-05): a PostgreSQL source read on a connection an earlier drop invalidated is re-read.

SQLAlchemy raises ``PendingRollbackError`` ("Can't reconnect until invalid transaction is
rolled back") on a connection an EARLIER error invalidated. It is recoverable exactly when
that earlier error -- its ``__context__`` -- was. The MySQL classifier looks behind it
(b02ea88: a source connection lost mid-read is re-read, not failed); the PostgreSQL twin did
not, so the same shape after a 57P01 / class-08 drop failed the table on a PostgreSQL source
while a MySQL source re-read it. The rule belongs to ``is_transient_error`` only: the shared
candidate walk (``source_dialect.base.error_candidates``) keeps it out, so lock-timeout,
too-many-connections and row-security classification do not move.
"""

from __future__ import annotations

import psycopg
import pytest
from sqlalchemy.exc import OperationalError, PendingRollbackError

from dsql_migrator.core.introspector import is_source_transient_error, source_error_hint
from dsql_migrator.core.models import SourceType
from dsql_migrator.core.source_dialect import dialect_for


def _pending_rollback_after(error: BaseException) -> PendingRollbackError:
    """The PendingRollbackError a statement on the invalidated connection raises, as Python
    chains it: raised while ``error`` was being handled, so ``error`` is its context."""
    try:
        try:
            raise error
        except Exception:
            raise PendingRollbackError(
                "Can't reconnect until invalid transaction is rolled back.", None, None
            )
    except PendingRollbackError as raised:
        return raised


def _wrapped(orig: BaseException) -> OperationalError:
    """SQLAlchemy's wrapper over the psycopg error (the real SQLSTATE is on ``.orig``)."""
    return OperationalError("SELECT 1", {}, orig)


_DROPS = [
    psycopg.errors.lookup("57P01")("terminating connection due to administrator command"),
    psycopg.errors.lookup("08006")("server closed the connection unexpectedly"),
    psycopg.OperationalError("consuming input failed: server closed the connection"),
]


@pytest.mark.parametrize("drop", _DROPS, ids=["57P01", "08006", "no-sqlstate"])
@pytest.mark.parametrize("wrap", [False, True], ids=["driver", "sqlalchemy"])
def test_a_pending_rollback_error_after_a_drop_is_transient_on_postgres(drop, wrap) -> None:
    error = _pending_rollback_after(_wrapped(drop) if wrap else drop)
    assert dialect_for(SourceType.POSTGRES).is_transient_error(error) is True
    assert is_source_transient_error(error, SourceType.POSTGRES) is True
    # The operator reads the connection-lost hint, as on MySQL.
    assert "connection dropped mid-read" in (source_error_hint(error, SourceType.POSTGRES) or "")


def test_a_pending_rollback_error_after_a_real_error_stays_non_transient() -> None:
    d = dialect_for(SourceType.POSTGRES)
    for real in (
        psycopg.errors.lookup("42703")('column "x" does not exist'),
        psycopg.errors.lookup("23505")("duplicate key value"),
    ):
        assert d.is_transient_error(_pending_rollback_after(real)) is False
    # Nothing behind it: nothing to recover from.
    bare = PendingRollbackError("Can't reconnect until invalid transaction is rolled back.")
    assert bare.__context__ is None
    assert d.is_transient_error(bare) is False


def test_only_a_pending_rollback_error_looks_behind_itself_on_postgres() -> None:
    # Any other error raised while handling a drop is judged as itself (a data error raised
    # in an except block is still a data error).
    d = dialect_for(SourceType.POSTGRES)
    other = ValueError("bad value")
    other.__context__ = psycopg.errors.lookup("57P01")("terminating connection")
    assert d.is_transient_error(other) is False


def test_the_two_engines_now_agree_on_the_shape() -> None:
    pg = _pending_rollback_after(psycopg.errors.lookup("57P01")("terminating connection"))
    my = _pending_rollback_after(Exception(2013, "Lost connection to MySQL server during query"))
    assert dialect_for(SourceType.POSTGRES).is_transient_error(pg) is True
    assert dialect_for(SourceType.MYSQL).is_transient_error(my) is True


def test_the_other_postgres_classifiers_do_not_look_behind_it() -> None:
    # The rule is is_transient_error's alone: too-many-connections and the row-security
    # refusal judge a PendingRollbackError as itself, as before.
    d = dialect_for(SourceType.POSTGRES)
    slots = psycopg.errors.lookup("53300")("sorry, too many clients already")
    assert d.is_too_many_connections(slots) is True
    assert d.is_too_many_connections(_pending_rollback_after(slots)) is False
    policy = psycopg.errors.lookup("42501")(
        'query would be affected by row-level security policy for table "t"'
    )
    assert d.is_row_security_refusal(policy) is True
    assert d.is_row_security_refusal(_pending_rollback_after(policy)) is False
