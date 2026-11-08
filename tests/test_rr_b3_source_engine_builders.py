# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""One builder for every read-only source engine.

Property 1 (the source is read-only) and the per-session settings (G136's REPEATABLE READ
pin, PostgreSQL's ``row_security = off``) used to depend on two copies of the same five
steps agreeing: ``ui.connect.make_source_engine_factory`` and
``core.introspector._default_engine_factory``. Both now build through
``core.introspector.build_source_engine``. The source engines that are NOT built there are
named exceptions: the two unguarded write engines (the PostgreSQL CDC slot / publication
and the G147 MySQL start-point fence) and PostgreSQL's TimeZone probe.

Doubles only: no source, no AWS.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from dsql_migrator.config import SecretValue
from dsql_migrator.core import introspector
from dsql_migrator.core.introspector import _default_engine_factory, build_source_engine
from dsql_migrator.core.models import SourceConnectionConfig, SourceType
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.ui.connect import make_source_engine_factory

_SRC = Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"


def _config(source_type: SourceType) -> SourceConnectionConfig:
    return SourceConnectionConfig(
        host="db.example", database="app", username="reader", source_type=source_type
    )


def _listeners(engine) -> tuple[int, list]:  # noqa: ANN001
    """(read-only guards, session-setup statements) on a built, never-connected engine."""
    guards = len(engine.dispatch.before_cursor_execute)
    setup = [
        fn.session_setup_sql for fn in engine.pool.dispatch.connect
        if hasattr(fn, "session_setup_sql")
    ]
    return guards, setup


@pytest.mark.parametrize("source_type", [SourceType.MYSQL, SourceType.POSTGRES])
def test_every_read_only_source_engine_comes_from_the_one_builder(
    monkeypatch: pytest.MonkeyPatch, source_type: SourceType
) -> None:
    assert _default_engine_factory is build_source_engine
    built: list = []
    real = introspector.create_engine
    monkeypatch.setattr(
        introspector, "create_engine",
        lambda url, **kwargs: built.append((url, kwargs)) or real(url, **kwargs),
    )
    dialect = dialect_for(source_type)
    engines = {
        "ui.connect": make_source_engine_factory(SecretValue("pw"))(_config(source_type)),
        "ui.connect (stream)": make_source_engine_factory(
            SecretValue("pw"), read_timeout_seconds=30
        )(_config(source_type)),
        "core default": _default_engine_factory(_config(source_type)),
    }
    try:
        for name, engine in engines.items():
            assert _listeners(engine) == (1, [dialect.session_setup_sql]), name
    finally:
        for engine in engines.values():
            engine.dispose()
    # All three came through core's create_engine, with the dialect's kwargs; the password
    # only where one was given, and the read timeout only where it was asked for.
    assert [url.password for url, _kw in built] == ["pw", "pw", None]
    assert [kw for _url, kw in built] == [
        dialect.engine_kwargs(),
        dialect.engine_kwargs(read_timeout_seconds=30),
        dialect.engine_kwargs(),
    ]


def _calls_in(name: str) -> set:
    """``{"module:function", ...}``: every function in src that calls ``name``."""
    sites: set = set()
    for path in sorted(_SRC.rglob("*.py")):
        module = path.relative_to(_SRC).with_suffix("").as_posix().replace("/", ".")
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for call in ast.walk(node):
                    if (
                        isinstance(call, ast.Call)
                        and isinstance(call.func, ast.Name)
                        and call.func.id == name
                    ):
                        sites.add(f"{module}:{node.name}")
    return sites


def test_the_source_engines_not_built_by_the_one_builder_are_the_named_exceptions() -> None:
    # Nested functions are walked by their enclosing function too, so a site is named by
    # every function it sits in; each of these is a top-level builder.
    assert _calls_in("create_engine") == {
        "core.introspector:build_source_engine",
        "core.cdc_pg_slot:build_pg_source_write_engine",
        "core.mysql_start_point_fence:build_fence_engine",
        "core.source_dialect.postgres:_settings_probe_engine",
    }
    assert _calls_in("install_read_only_guard") == {
        "core.introspector:build_source_engine",
        "core.source_dialect.postgres:_settings_probe_engine",
    }
    assert _calls_in("install_session_setup") == {"core.introspector:build_source_engine"}
