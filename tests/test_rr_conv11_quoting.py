# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CONV-11 (~ X9, AS-10, VAL-9, CI-10): the identifier quoting copies ask the source dialect.

Three modules quoted identifiers with their own copy of a source dialect's rule:

* Validation's MySQL builders (``validation_sql._quote_mysql_identifier`` /
  ``_quote_mysql_table``, also read by ``mysql_zero_dates.check_query`` and bound by the E2E
  harness) -- the MySQL dialect's ``quote_identifier`` / ``quote_table``, line for line;
* the Schema Conversion screen's MySQL source-DDL preview (``ui.schema_conversion.
  _quote_mysql`` / ``_quote_mysql_table``) -- the same;
* the CDC schema-drift fix (``cdc_schema_evolve._quote_ident``) -- a sqlglot rendering that
  is the PostgreSQL dialect's ``quote_identifier`` for every ``str``.

Each now delegates, its name kept. One copy is NOT the dialect's rule and keeps its own:
``cdc_schema_evolve._quote_qualified`` splits any name holding a '.', so a name whose schema
or table part is empty ('a.', '.a', '.', '..') renders as two quoted parts (``"a".""``),
where ``quote_table`` quotes it whole (``"a."``). Only its per-part quoting is delegated.

Pinned here:

* parity over a corpus (backticks, double quotes, ':', '.', degenerate dotted names,
  non-ASCII, control characters, empty) and a seeded random corpus: each copy gives exactly
  what it gave, and what the dialect gives where it is the dialect's rule; the one split
  difference is spelled out;
* the callers' rendered text: the MySQL source-DDL preview, the zero-date check query,
  Validation's text() quoting, the harness's own dot.name expectation, the drift fix's
  ALTER statements;
* structure: no copy quotes by itself again.

Pure: no source, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import ast
import pathlib
import random

import pytest

from dsql_migrator.core import cdc_schema_evolve as evolve
from dsql_migrator.core import validation_sql as vsql
from dsql_migrator.core.models import ColumnDef, IndexDef, SourceType, TableDef
from dsql_migrator.core.mysql_zero_dates import check_query
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.ui import schema_conversion as sc

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
_MYSQL = dialect_for(SourceType.MYSQL)
_PG = dialect_for(SourceType.POSTGRES)

_NAMES = [
    "", "a", "Order", "a`b", "``", 'a"b', '""', "a:b", ":last", "a.b", "s.t.u", "a.", ".a",
    ".", "..", "a..b", "é", "😀", "a b", "\n", "\x00", "\\", "'", "$1", "a​b",
]


def _random_names(seed: int = 11, count: int = 2000) -> "list[str]":
    rng = random.Random(seed)
    alphabet = ["`", '"', ".", ":", "a", "Z", " ", "\\", "\n", "é", "😀", "\x00", "'", "_", "$"]
    return ["".join(rng.choice(alphabet) for _ in range(rng.randint(0, 8)))
            for _ in range(count)]


_CORPUS = _NAMES + _random_names()

# Spelled out by hand: name -> (MySQL identifier, MySQL table).
_MYSQL_GOLDEN = {
    "a": ("`a`", "`a`"),
    "a`b": ("`a``b`", "`a``b`"),
    "a.b": ("`a.b`", "`a`.`b`"),
    "s.t.u": ("`s.t.u`", "`s`.`t.u`"),           # split at the FIRST '.'
    "a.": ("`a.`", "`a.`"),                      # an empty part: quoted whole
    ".a": ("`.a`", "`.a`"),
    "a:b": ("`a:b`", "`a:b`"),
    "": ("``", "``"),
}


# ---------------------------------------------------------------------------
# Validation's MySQL quoting
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name, quoted", sorted(_MYSQL_GOLDEN.items()))
def test_validations_mysql_quoting_golden(name, quoted) -> None:
    assert (vsql._quote_mysql_identifier(name), vsql._quote_mysql_table(name)) == quoted
    assert vsql._text_mysql_identifier(name) == quoted[0].replace(":", "\\:")
    assert vsql._text_mysql_table(name) == quoted[1].replace(":", "\\:")


def test_validations_mysql_quoting_is_the_dialects() -> None:
    for name in _CORPUS:
        assert vsql._quote_mysql_identifier(name) == _MYSQL.quote_identifier(name), ascii(name)
        assert vsql._quote_mysql_table(name) == _MYSQL.quote_table(name), ascii(name)
        assert vsql._text_mysql_identifier(name) == _MYSQL.text_identifier(name), ascii(name)
        assert vsql._text_mysql_table(name) == _MYSQL.text_table(name), ascii(name)


def test_the_harness_dot_name_and_the_zero_date_query_are_unchanged() -> None:
    # tests the E2E harness's static check runs against the bound tool source
    assert vsql._quote_mysql_table("e2e125.dot.name") == "`e2e125`.`dot.name`"
    table = TableDef(
        name="shop.ord`ers",
        columns=[ColumnDef(name="id", mysql_type="int"),
                 ColumnDef(name="made `at", mysql_type="datetime", nullable=True)],
        primary_key=["id"],
    )
    query = check_query(table)
    assert query.startswith("SELECT COUNT(*) FROM `shop`.`ord``ers` WHERE ")
    assert "`made ``at`" in query


# ---------------------------------------------------------------------------
# The Schema Conversion screen's MySQL source-DDL preview
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name, quoted", sorted(_MYSQL_GOLDEN.items()))
def test_the_previews_mysql_quoting_golden(name, quoted) -> None:
    assert (sc._quote_mysql(name), sc._quote_mysql_table(name)) == quoted


def test_the_previews_mysql_quoting_is_the_dialects() -> None:
    for name in _CORPUS:
        assert sc._quote_mysql(name) == _MYSQL.quote_identifier(name), ascii(name)
        assert sc._quote_mysql_table(name) == _MYSQL.quote_table(name), ascii(name)


def test_the_mysql_source_ddl_preview_is_unchanged() -> None:
    from dsql_migrator.core.models import ForeignKeyDef

    table = TableDef(
        name="shop.Order`s",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="cust:id", mysql_type="int"),
                 ColumnDef(name="na`me", mysql_type="varchar(20)")],
        primary_key=["id"],
        indexes=[IndexDef(name="ix`1", columns=["na`me"], unique=True)],
        foreign_keys=[ForeignKeyDef(name="fk.c", columns=["cust:id"],
                                    referenced_table="crm.cust", referenced_columns=["id"])],
    )
    ddl = sc.render_source_table_ddl(table)
    assert ddl.startswith("CREATE TABLE `shop`.`Order``s` (\n")
    assert "  `cust:id` int" in ddl and "  `na``me` varchar(20)" in ddl
    assert "  PRIMARY KEY (`id`)" in ddl
    assert "  UNIQUE KEY `ix``1` (`na``me`)" in ddl
    assert ("  CONSTRAINT `fk.c` FOREIGN KEY (`cust:id`) REFERENCES `crm`.`cust` (`id`)"
            in ddl)


# ---------------------------------------------------------------------------
# The CDC schema-drift fix (PostgreSQL / Aurora DSQL quoting)
# ---------------------------------------------------------------------------


def test_the_drift_fixs_identifier_quoting_is_the_postgresql_dialects() -> None:
    assert evolve._quote_ident('a"b') == '"a""b"'
    assert evolve._quote_ident("") == '""'
    for name in _CORPUS:
        assert evolve._quote_ident(name) == _PG.quote_identifier(name), ascii(name)


# name -> what _quote_qualified renders (spelled out by hand)
_SPLIT_GOLDEN = {
    "s.t": '"s"."t"',
    "s.t.u": '"s"."t.u"',
    "t": '"t"',
    'S."T': '"S"."""T"',
    # The one difference from quote_table: a '.' with an empty part is still split.
    "a.": '"a".""',
    ".a": '"".\"a"',
    ".": '"".""',
    "..": '""."."',
}


@pytest.mark.parametrize("name, quoted", sorted(_SPLIT_GOLDEN.items()))
def test_the_drift_fixs_split_rule_is_kept(name, quoted) -> None:
    assert evolve._quote_qualified(name) == quoted


def test_the_split_rule_differs_from_quote_table_only_for_an_empty_part() -> None:
    differ = []
    for name in _CORPUS:
        if evolve._quote_qualified(name) != _PG.quote_table(name):
            differ.append(name)
    assert differ, "the corpus must hold the degenerate names"
    for name in differ:
        schema, dot, obj = name.partition(".")
        assert dot and not (schema and obj), ascii(name)
    assert _PG.quote_table("a.") == '"a."' != evolve._quote_qualified("a.")


def test_the_drift_fixs_statements_are_unchanged() -> None:
    plan = evolve.plan_add_columns(
        'app.Or"ders',
        source_columns=[("id", "int"), ('new "col', "varchar(10)")],
        target_columns=["id"],
        source_type=SourceType.MYSQL,
    )
    (step,) = plan.steps
    assert step.ddl == 'ALTER TABLE "app"."Or""ders" ADD COLUMN "new ""col" VARCHAR(10)'
    # A degenerate table name keeps the split rule's rendering.
    (odd,) = evolve.plan_add_columns(
        "app.", source_columns=[("c", "int")], target_columns=[]).steps
    assert odd.ddl == 'ALTER TABLE "app"."" ADD COLUMN "c" INT'


# ---------------------------------------------------------------------------
# Structure: the copies delegate; the names callers and the harness bind stay
# ---------------------------------------------------------------------------


def _function(rel: str, name: str) -> ast.FunctionDef:
    tree = ast.parse((_SRC / rel).read_text(encoding="utf-8"))
    return next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)


@pytest.mark.parametrize("rel, name, method, engine", [
    ("core/validation_sql.py", "_quote_mysql_identifier", "quote_identifier", "MYSQL"),
    ("core/validation_sql.py", "_quote_mysql_table", "quote_table", "MYSQL"),
    ("ui/schema_conversion.py", "_quote_mysql", "quote_identifier", "MYSQL"),
    ("ui/schema_conversion.py", "_quote_mysql_table", "quote_table", "MYSQL"),
    ("core/cdc_schema_evolve.py", "_quote_ident", "quote_identifier", "POSTGRES"),
])
def test_each_copy_delegates_to_its_dialect(rel, name, method, engine) -> None:
    body = _function(rel, name).body
    assert ast.unparse(body[-1]) == f"return dialect_for(SourceType.{engine}).{method}(name)"
    assert len(body) == 2  # its docstring and the delegation, nothing quoted by hand


def test_the_split_rule_stays_its_own() -> None:
    source = ast.unparse(_function("core/cdc_schema_evolve.py", "_quote_qualified"))
    assert "if '.' in name:" in source and "quote_table" not in source.split('"""')[-1]
    text = (_SRC / "core" / "cdc_schema_evolve.py").read_text(encoding="utf-8")
    assert "to_identifier" not in text and "from sqlglot" not in text


def test_the_bound_names_stay() -> None:
    # The E2E harness binds validation_sql._quote_mysql_table; mysql_zero_dates and the
    # CDC monitor import the rest.
    for module, names in (
        (vsql, ("_quote_mysql_identifier", "_quote_mysql_table", "_text_mysql_identifier",
                "_text_mysql_table")),
        (sc, ("_quote_mysql", "_quote_mysql_table")),
        (evolve, ("_quote_ident", "_quote_qualified")),
    ):
        for name in names:
            assert callable(getattr(module, name)), (module.__name__, name)
