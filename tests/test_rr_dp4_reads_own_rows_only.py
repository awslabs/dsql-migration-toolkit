# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""DP-4: whether a source read says ``FROM ONLY`` is ONE dialect hook.

A PostgreSQL inheritance parent is read ``FROM ONLY`` -- its own rows, the ones CDC captures
(G25). Four readers decided that each on their own, by testing the source type and then
importing ``pg_reads_own_rows_only`` (the Full Load's ``exporter._reads_own_rows_only``, the
CDC monitor's ``watermark.max_pk_source``, Validation's ``Validator._compare_table`` and the
size probe's ``SessionSourceProbe.oversized_values``). Now each asks
``SourceDialect.reads_own_rows_only(connection, table_name)``: ``False`` without a read by
default (MySQL), the live catalog read on PostgreSQL -- called through
``source_dialect.postgres``'s module global, so a patch of ``pg_reads_own_rows_only`` there
still applies (and now to Validation too). ``exporter._reads_own_rows_only`` stays the Full
Load's one entry (a test patches it), delegating to the hook.

Pinned here:

* parity: for each reader, on MySQL and PostgreSQL, a parent / a plain table / a name the
  catalog does not know / an unqualified name / a catalog read that fails -- the decision,
  the statements sent, and what the reader does with a failure;
* a patch of ``source_dialect.postgres.pg_reads_own_rows_only`` reaches every reader;
* structure: no reader tests the source type or imports ``pg_reads_own_rows_only`` again.

Doubles only: no source, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import ast
import pathlib
from types import SimpleNamespace

import pytest

from dsql_migrator.core import exporter as exporter_module
from dsql_migrator.core import validator as v
from dsql_migrator.core.models import ColumnDef, SourceType, TableDef, ValidationMode
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.watermark import max_pk_source

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
_MYSQL = dialect_for(SourceType.MYSQL)
_PG = dialect_for(SourceType.POSTGRES)

_RAISE = object()  # the catalog read fails


class _Conn:
    """Records statements; answers the inheritance read with ``parent`` (``_RAISE`` raises),
    every other statement with a scalar 7 / an empty result."""

    def __init__(self, parent):
        self.parent, self.sql = parent, []

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        self.sql.append(sql)
        if "AS inheritance_parent" in sql:
            if self.parent is _RAISE:
                raise RuntimeError("catalog unreadable")
            return SimpleNamespace(scalar=lambda: self.parent)
        return SimpleNamespace(scalar=lambda: 7)

    def catalog_reads(self):
        return [s for s in self.sql if "AS inheritance_parent" in s]


def _table(name="app.par"):
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="integer"),
                 ColumnDef(name="v", mysql_type="text")],
        primary_key=["id"],
    )


# (parent answer, table name) -> FROM ONLY on PostgreSQL? (None = the read raises)
_CASES = [
    pytest.param(True, "app.par", True, id="parent"),
    pytest.param(False, "app.par", False, id="plain"),
    pytest.param(None, "app.par", False, id="not-found"),
    pytest.param(1, "app.par", True, id="truthy-scalar"),
    pytest.param(True, "par", False, id="unqualified-no-read"),
    pytest.param(True, ".par", False, id="empty-schema-no-read"),
    pytest.param(_RAISE, "app.par", None, id="catalog-fails"),
]


def _reads_expected(name):
    schema, sep, rest = name.partition(".")
    return 1 if (sep and schema and rest) else 0


# ---------------------------------------------------------------------------
# The Full Load (exporter._reads_own_rows_only)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("parent, name, only", _CASES)
def test_the_full_load_decision(parent, name, only) -> None:
    conn = _Conn(parent)
    assert exporter_module._reads_own_rows_only(conn, _table(name), _MYSQL) is False
    assert conn.sql == []  # MySQL: no read at all
    if only is None:
        with pytest.raises(RuntimeError, match="catalog unreadable"):
            exporter_module._reads_own_rows_only(conn, _table(name), _PG)
    else:
        assert exporter_module._reads_own_rows_only(conn, _table(name), _PG) is only
    assert len(conn.catalog_reads()) == _reads_expected(name)


# ---------------------------------------------------------------------------
# The CDC monitor's source high-water (watermark.max_pk_source)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("parent, name, only", _CASES)
def test_the_high_water_decision(parent, name, only) -> None:
    conn = _Conn(parent)
    assert max_pk_source(conn, {name: "id"}, _MYSQL) == {name: 7}
    assert conn.catalog_reads() == []
    conn = _Conn(parent)
    out = max_pk_source(conn, {name: "id"}, _PG)
    if only is None:
        assert out == {name: None}  # unknown, never a high-water over the children
        assert [s for s in conn.sql if "MAX(" in s] == []
    else:
        assert out == {name: 7}
        (statement,) = [s for s in conn.sql if "MAX(" in s]
        assert statement == f'SELECT MAX("id") FROM {"ONLY " if only else ""}{_PG.quote_table(name)}'
    assert len(conn.catalog_reads()) == _reads_expected(name)


def test_the_high_water_without_a_dialect_is_mysql() -> None:
    conn = _Conn(True)
    assert max_pk_source(conn, {"app.par": "id"}) == {"app.par": 7}
    assert conn.sql == ["SELECT MAX(`id`) FROM `app`.`par`"]


# ---------------------------------------------------------------------------
# Validation (Validator._compare_table)
# ---------------------------------------------------------------------------


def _compare(monkeypatch, conn, table, dialect):
    seen: dict = {}

    def _capture(name, value):
        def _reader(*_a, **kw):
            seen[name] = kw.get("from_only")
            return value
        return _reader

    monkeypatch.setattr(v, "_source_row_count_live", _capture("count", 2))
    monkeypatch.setattr(v, "_source_checksum_for", _capture("checksum", "42"))
    monkeypatch.setattr(v, "_bounded_target_count", lambda *_a, **_k: 2)
    monkeypatch.setattr(v, "_target_checksum", lambda *_a, **_k: "42")
    monkeypatch.setattr(v, "read_target_primary_key", lambda *_a: ("id",))
    monkeypatch.setattr(v, "_source_declared_column_types", lambda *_a, **_k: {})
    result = v.Validator()._compare_table(
        conn, object(), table, ValidationMode.CHECKSUM, None, False, lambda: False, dialect)
    return result, seen


@pytest.mark.parametrize("parent, name, only", _CASES)
def test_the_validation_decision(monkeypatch, parent, name, only) -> None:
    conn = _Conn(parent)
    result, seen = _compare(monkeypatch, conn, _table(name), _MYSQL)
    assert result.matched is True and seen == {"count": False, "checksum": False}
    assert conn.catalog_reads() == []
    conn = _Conn(parent)
    if only is None:
        with pytest.raises(RuntimeError, match="catalog unreadable"):
            _compare(monkeypatch, conn, _table(name), _PG)
    else:
        result, seen = _compare(monkeypatch, conn, _table(name), _PG)
        assert result.matched is True and seen == {"count": only, "checksum": only}
    assert len(conn.catalog_reads()) == _reads_expected(name)


# ---------------------------------------------------------------------------
# The size probe (SessionSourceProbe.oversized_values)
# ---------------------------------------------------------------------------


class _ProbeConn(_Conn):
    def rollback(self):
        pass

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        if "AS inheritance_parent" in sql or sql.startswith("SET "):
            return super().execute(statement, params)
        self.sql.append(sql)
        return SimpleNamespace(first=lambda: (False,))

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False


def _probe(source_type, conn):
    from tests.test_prerequisite_probes import _size_probe

    return _size_probe(source_type, conn)


@pytest.mark.parametrize("parent, name, only", _CASES)
def test_the_size_probe_decision(parent, name, only) -> None:
    conn = _ProbeConn(parent)
    assert _probe(SourceType.MYSQL, conn).oversized_values(
        name, ["v"], limit_bytes=1024, timeout_seconds=5.0) == {"v": False}
    assert conn.catalog_reads() == []
    assert [s for s in conn.sql if "EXISTS" in s][0].startswith(
        f"SELECT EXISTS (SELECT 1 FROM {_MYSQL.quote_table(name)} WHERE")
    conn = _ProbeConn(parent)
    answer = _probe(SourceType.POSTGRES, conn).oversized_values(
        name, ["v"], limit_bytes=1024, timeout_seconds=5.0)
    if only is None:
        assert answer is None  # nothing measured, never a guess
        assert [s for s in conn.sql if "EXISTS" in s] == []
    else:
        assert answer == {"v": False}
        (statement,) = [s for s in conn.sql if "EXISTS" in s]
        assert statement.startswith(
            f"SELECT EXISTS (SELECT 1 FROM {'ONLY ' if only else ''}{_PG.quote_table(name)} WHERE")
    assert len(conn.catalog_reads()) == _reads_expected(name)


# ---------------------------------------------------------------------------
# A patch of the PostgreSQL rule reaches every reader
# ---------------------------------------------------------------------------


def test_a_patch_of_the_postgresql_rule_reaches_the_full_load_monitor_and_probe(
    monkeypatch,
) -> None:
    from dsql_migrator.core.source_dialect import postgres as postgres_dialect

    asked: list = []

    def _rule(connection, table_name):
        asked.append(table_name)
        return True

    monkeypatch.setattr(postgres_dialect, "pg_reads_own_rows_only", _rule)
    conn = _Conn(False)  # the real read would say "not a parent"
    assert exporter_module._reads_own_rows_only(conn, _table(), _PG) is True
    assert max_pk_source(conn, {"app.par": "id"}, _PG) == {"app.par": 7}
    assert conn.sql == ['SELECT MAX("id") FROM ONLY "app"."par"']
    probe_conn = _ProbeConn(False)
    _probe(SourceType.POSTGRES, probe_conn).oversized_values(
        "app.par", ["v"], limit_bytes=1, timeout_seconds=1.0)
    assert any(' FROM ONLY "app"."par" ' in s for s in probe_conn.sql)
    assert asked == ["app.par"] * 3


def test_a_patch_of_the_postgresql_rule_reaches_validation_too(monkeypatch) -> None:
    # New with the hook: Validation imported its own binding of the rule, which a patch of
    # source_dialect.postgres never reached. It now asks the dialect like the others.
    from dsql_migrator.core.source_dialect import postgres as postgres_dialect

    monkeypatch.setattr(postgres_dialect, "pg_reads_own_rows_only", lambda _c, _n: True)
    _result, seen = _compare(monkeypatch, _Conn(False), _table(), _PG)
    assert seen == {"count": True, "checksum": True}


def test_the_hook_is_the_dialects() -> None:
    from dsql_migrator.core.source_dialect.base import SourceDialect

    assert type(_MYSQL).reads_own_rows_only is SourceDialect.reads_own_rows_only
    assert _MYSQL.reads_own_rows_only(_Conn(_RAISE), "app.par") is False  # no read at all
    conn = _Conn(True)
    assert _PG.reads_own_rows_only(conn, "app.par") is True
    assert len(conn.catalog_reads()) == 1


# ---------------------------------------------------------------------------
# Structure: the decision is not re-made by a reader
# ---------------------------------------------------------------------------

_READERS = {
    "core/exporter.py": "_reads_own_rows_only",
    "core/watermark.py": "max_pk_source",
    "core/validator.py": "_compare_table",
    "ui/prerequisite_probes.py": "oversized_values",
}


def _function_node(path: pathlib.Path, name: str) -> ast.AST:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


@pytest.mark.parametrize("module, function", sorted(_READERS.items()))
def test_each_reader_asks_the_hook(module, function) -> None:
    node = _function_node(_SRC / module, function)
    calls = [
        call for call in ast.walk(node)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == "reads_own_rows_only"
    ]
    assert len(calls) == 1, (module, function)
    names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
    assert "pg_reads_own_rows_only" not in names, (module, function)


def test_no_module_but_the_dialect_calls_the_postgresql_rule() -> None:
    callers = []
    for path in _SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and getattr(node.func, "id", None) == "pg_reads_own_rows_only"
            ):
                callers.append(path.relative_to(_SRC).as_posix())
    assert callers == ["core/source_dialect/postgres.py"]
