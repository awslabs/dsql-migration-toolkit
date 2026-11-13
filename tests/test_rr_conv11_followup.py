# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CONV-11 follow-up: the PostgreSQL preview quoting and the Connect screen's engine name ask
the source dialect.

CONV-11 (tests/test_rr_conv11_quoting.py) made Validation's, the Schema Conversion preview's
MySQL and the CDC drift fix's identifier quoting delegate to the source dialect. Left beside
them:

* the Schema Conversion screen's PostgreSQL source-DDL preview quoting
  (``ui.schema_conversion._quote_pg_ident`` / ``_quote_pg_table``) -- the twins of the
  delegated ``_quote_mysql`` / ``_quote_mysql_table``, line for line the PostgreSQL
  dialect's ``quote_identifier`` / ``quote_table``;
* the Connect screen's two engine-name ternaries (``"PostgreSQL" if ... is
  SourceType.POSTGRES else "MySQL"``): the AI activity event's "Source (<engine>)" label and
  the coordinates an "Ask AI to help fix this" chat is grounded with. Each reads a value that
  is always a SourceType -- the page's engine choice, which the same page already passes to
  ``dialect_for`` when it renders the port, and the validated SourceConnectionConfig's
  ``source_type`` -- so the dialect's ``engine_display_name`` names the same engine.

Each now delegates, its name kept. Pinned here: hand-spelled goldens and parity over the
CONV-11 corpus, the PostgreSQL source-DDL preview's text, the Connect screen's event label
and AI coordinates for both engines (all run green on the code before the change), and the
structure. Pure: no source, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import ast
import asyncio
import pathlib
from typing import Any

import pytest

from dsql_migrator.core.models import (
    ColumnDef,
    ConnectionResult,
    ForeignKeyDef,
    IndexDef,
    SourceConnectionConfig,
    SourceType,
    TableDef,
)
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.ui import schema_conversion as sc
from tests.test_rr_conv11_quoting import _CORPUS
from tests.test_ui_connect_ai_status import _ai_on, _render

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
_PG = dialect_for(SourceType.POSTGRES)

# ---------------------------------------------------------------------------
# The PostgreSQL source-DDL preview's quoting
# ---------------------------------------------------------------------------

# name -> (_quote_pg_ident, _quote_pg_table), spelled by hand.
_PG_GOLDEN = {
    "": ('""', '""'),
    "a": ('"a"', '"a"'),
    'a"b': ('"a""b"', '"a""b"'),
    "a`b": ('"a`b"', '"a`b"'),
    "a.b": ('"a.b"', '"a"."b"'),
    "s.t.u": ('"s.t.u"', '"s"."t.u"'),
    'sh"op.Or"ders': ('"sh""op.Or""ders"', '"sh""op"."Or""ders"'),
    "a.": ('"a."', '"a."'),
    ".a": ('".a"', '".a"'),
    ".": ('"."', '"."'),
    "..": ('".."', '".."'),
    "a..b": ('"a..b"', '"a".".b"'),
    "a:b": ('"a:b"', '"a:b"'),
}


@pytest.mark.parametrize("name, quoted", sorted(_PG_GOLDEN.items()))
def test_the_previews_postgresql_quoting_golden(name, quoted) -> None:
    assert (sc._quote_pg_ident(name), sc._quote_pg_table(name)) == quoted


def test_the_previews_postgresql_quoting_is_the_dialects() -> None:
    for name in _CORPUS:
        assert sc._quote_pg_ident(name) == _PG.quote_identifier(name), ascii(name)
        assert sc._quote_pg_table(name) == _PG.quote_table(name), ascii(name)


def test_the_postgresql_source_ddl_preview_is_unchanged() -> None:
    table = TableDef(
        name='shop.Or"ders',
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False),
                 ColumnDef(name='cu"st', mysql_type="integer"),
                 ColumnDef(name="na.me", mysql_type="text")],
        primary_key=["id"],
        indexes=[IndexDef(name='ix"1', columns=["na.me"], unique=True)],
        foreign_keys=[ForeignKeyDef(name="fk.c", columns=['cu"st'],
                                    referenced_table="crm.cust", referenced_columns=["id"])],
    )
    assert sc.render_source_table_ddl(table, source_type=SourceType.POSTGRES) == (
        'CREATE TABLE "shop"."Or""ders" (\n'
        '  "id" integer NOT NULL,\n'
        '  "cu""st" integer,\n'
        '  "na.me" text,\n'
        '  PRIMARY KEY ("id"),\n'
        '  CONSTRAINT "fk.c" FOREIGN KEY ("cu""st") REFERENCES "crm"."cust" ("id")\n'
        ");\n"
        'CREATE UNIQUE INDEX "ix""1" ON "shop"."Or""ders" ("na.me")'
    )


# ---------------------------------------------------------------------------
# The Connect screen's engine name
# ---------------------------------------------------------------------------

_HOSTS = {SourceType.MYSQL: ("my.example", 3306), SourceType.POSTGRES: ("pg.example", 5432)}


@pytest.mark.parametrize(
    "engine, word", [(SourceType.MYSQL, "MySQL"), (SourceType.POSTGRES, "PostgreSQL")]
)
def test_the_connect_screens_engine_name_is_unchanged(engine, word, monkeypatch) -> None:
    from dsql_migrator.core import assessment_strategist

    host, port = _HOSTS[engine]
    grounded: list[str] = []
    real = assessment_strategist.build_connection_error_chat_system

    def recording(**kwargs: Any) -> str:
        grounded.append(kwargs["coordinates"])
        return real(**kwargs)

    # Read where the page calls it (a lazy import of the module's attribute).
    monkeypatch.setattr(assessment_strategist, "build_connection_error_chat_system", recording)

    def setup(state: Any) -> None:
        _ai_on(state)
        state.set_source(
            SourceConnectionConfig(
                source_type=engine, host=host, port=port, database="app", username="reader"
            ),
            None,
        )

    events: list[dict] = []
    scopes: list[dict] = []
    ui, _state = _render(
        setup,
        source_tester=lambda _config, _password: ConnectionResult(
            success=False, detail="REFUSED"
        ),
        ai_post_event=lambda **kwargs: events.append(kwargs),
        open_ai_scope=lambda **kwargs: scopes.append(kwargs),
    )
    asyncio.run(ui.find_button("Test source connection").click())
    assert events == [{"text": f"Source ({word}) connection test failed", "status": "error"}]

    ui.find_button("Ask AI to help fix this").click()
    assert len(scopes) == 1
    assert grounded == [f"{word} host={host} port={port} database=app"]


# ---------------------------------------------------------------------------
# Structure: the copies delegate; their names stay
# ---------------------------------------------------------------------------


def _function(rel: str, name: str) -> ast.FunctionDef:
    tree = ast.parse((_SRC / rel).read_text(encoding="utf-8"))
    return next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)


@pytest.mark.parametrize("name, method", [
    ("_quote_pg_ident", "quote_identifier"),
    ("_quote_pg_table", "quote_table"),
])
def test_the_postgresql_preview_copies_delegate(name, method) -> None:
    body = _function("ui/schema_conversion.py", name).body
    assert ast.unparse(body[-1]) == f"return dialect_for(SourceType.POSTGRES).{method}(name)"
    assert len(body) == 2  # its docstring and the delegation, nothing quoted by hand


def test_the_connect_screen_asks_the_dialect_for_the_engine_name() -> None:
    source = (_SRC / "ui" / "connect.py").read_text(encoding="utf-8")
    flat = " ".join(source.split())
    assert "dialect_for(_engine['type']).engine_display_name" in ast.unparse(ast.parse(source))
    assert "dialect_for(config.source_type).engine_display_name" in flat
    assert "'PostgreSQL' if config.source_type is SourceType.POSTGRES" not in flat
    assert '"PostgreSQL" if _engine["type"] is SourceType.POSTGRES' not in flat


def test_the_names_stay() -> None:
    for name in ("_quote_pg_ident", "_quote_pg_table"):
        assert callable(getattr(sc, name)), name
