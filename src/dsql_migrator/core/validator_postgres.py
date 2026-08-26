# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL-source validation adapter.

Kept in its own module -- not tangled into ``validator.py``'s MySQL-source reads -- per
the per-engine separation principle. Validation compares a source against the Aurora DSQL
target; when the SOURCE is PostgreSQL, BOTH ends are the same PostgreSQL-16 wire, so a PG
source can reuse the EXACT SAME checksum / count / keyset-PK readers the DSQL TARGET uses
(``validator._target_*`` + the ``build_pg_*`` SQL builders) -- one renderer for both sides,
no MySQL-vs-PG cross-engine text normalization to reconcile.

Those PG readers were written for a raw psycopg connection (``connection.cursor()`` +
``cursor.execute(composed, params)``). The validator's SOURCE connection, however, is a
guarded SQLAlchemy connection (read-only ``before_cursor_execute`` guard + the consistent
snapshot). :class:`PgSourceConnection` adapts the SQLAlchemy connection to that raw-cursor
interface by rendering each ``psycopg.sql.Composed`` to text (``as_string``) and running it
through ``exec_driver_sql`` -- so every read still passes through the read-only guard and
stays inside the snapshot transaction (no raw-cursor / transaction mixing).
"""

from __future__ import annotations

from typing import Any


class _PgSourceCursor:
    """Raw-cursor-like adapter backed by SQLAlchemy ``exec_driver_sql``.

    ``execute`` renders the ``psycopg.sql.Composed`` to a SQL string against the underlying
    psycopg connection (so baked literals + ``%(name)s`` placeholders come out correctly)
    and runs it via the guarded SQLAlchemy connection. Placeholders bind through psycopg's
    pyformat paramstyle. ``fetchone``/``fetchall`` mirror a DB-API cursor; ``close`` is a
    no-op (SQLAlchemy owns the real cursor and the transaction).
    """

    def __init__(self, sqla_connection: Any, raw_connection: Any) -> None:
        self._conn = sqla_connection
        self._raw = raw_connection
        self._result: Any = None

    def execute(self, statement: Any, params: Any = None) -> None:
        sql_text = statement.as_string(self._raw)
        if params:
            self._result = self._conn.exec_driver_sql(sql_text, params)
        else:
            self._result = self._conn.exec_driver_sql(sql_text)

    def fetchone(self) -> Any:
        return None if self._result is None else self._result.fetchone()

    def fetchall(self) -> Any:
        return [] if self._result is None else self._result.fetchall()

    def close(self) -> None:  # SQLAlchemy manages the cursor/transaction lifecycle.
        return None


class PgSourceConnection:
    """Adapts a guarded SQLAlchemy PostgreSQL SOURCE connection to the raw-cursor
    interface the PG read helpers expect, so a PostgreSQL source reuses the same PG-16
    readers as the DSQL target while keeping the source's read-only guard + snapshot."""

    def __init__(self, sqla_connection: Any) -> None:
        self._conn = sqla_connection
        # The underlying psycopg Connection -- used only to render Composed SQL to text
        # (Identifier quoting / Literal escaping); execution still goes via SQLAlchemy.
        self._raw = sqla_connection.connection.driver_connection

    def cursor(self) -> _PgSourceCursor:
        return _PgSourceCursor(self._conn, self._raw)


__all__ = ["PgSourceConnection"]
