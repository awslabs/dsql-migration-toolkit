# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G193: a PostgreSQL 15+ ``ON DELETE SET NULL (col, ...)`` / ``SET DEFAULT (col, ...)``
keeps its column list on Aurora DSQL.

Live-verified on Aurora DSQL (PostgreSQL 16 compatible, 2026-10-25): ``ALTER TABLE ... ADD
CONSTRAINT ... FOREIGN KEY (a, b) REFERENCES p (a, b) ON DELETE SET NULL (b) NOT VALID`` is
accepted, and once the parent row is deleted the child keeps ``a`` and nulls only ``b``. Since
v0.1.432 the converter stripped the list -- its comment said Aurora DSQL / PostgreSQL 16 cannot
express it -- so the re-created key nulled EVERY key column where the source nulls only the
listed ones.

* Both reflection paths return the list as ``pg_get_constraintdef`` prints it: SQLAlchemy's
  PostgreSQL dialect (``options["ondelete"]``) and the catalog read reflection falls back to
  (G163), which now reads a list whose quoted name holds a parenthesis whole, as SQLAlchemy
  does.
* Schema Conversion writes ``ON DELETE SET NULL ("b", "Mixed Col")`` for a PostgreSQL source,
  each name quoted like the key's own columns. ON UPDATE never has one (PostgreSQL refuses
  it), and a MySQL key is written as before. Every apply path runs these statements, and what
  reads a key's DDL (its name, columns, parent, actions; the AI merge) reads it as before.
* FK_PRESERVED and the Schema Conversion note name each key's list; the source DDL panel and
  the AI source detail show it as the source declares it.
* An inventory or saved session loads unchanged (no field was added).
* The manual says the list is kept, in every language.

The definitions and catalog rows below are what PostgreSQL 17.11 returned for these keys
(throwaway local container). The statements Schema Conversion writes for them ran there and on
16.15: the delete nulled only the listed columns, where the stripped key nulled them all. The
tests use doubles only.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects.postgresql.base import PGDialect

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter, SchemaConvertOptions
from dsql_migrator.core.introspector import _reflect_tables
from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    SourceInventory,
    SourceType,
    TableDef,
)
from dsql_migrator.core.source_dialect.postgres import pg_foreign_keys_from_catalog

_PG = SourceType.POSTGRES

# pg_get_constraintdef(oid, true) on PostgreSQL 17.11.
_DEF_LIST = (
    'FOREIGN KEY (a, b, "Mixed Col") REFERENCES rev.parent(a, b, "Mixed Col") ON UPDATE '
    'CASCADE ON DELETE SET NULL (b, "Mixed Col") DEFERRABLE INITIALLY DEFERRED'
)
_DEF_DEFAULT = (
    'FOREIGN KEY (a, b, c) REFERENCES rev.parent(a, b, "Mixed Col") ON DELETE SET DEFAULT (b)'
)
_DEF_PAREN = 'FOREIGN KEY (a, "x)y") REFERENCES rev.qp(a, "x)y") ON DELETE SET NULL ("x)y")'


def _sqlalchemy_options(definition: str) -> dict:
    """``options`` as SQLAlchemy 2.0.50's PostgreSQL dialect builds them from a definition
    (``get_multi_foreign_keys``: each parsed value, the NO ACTION and absent ones left out)."""
    parsed = PGDialect()._parse_fk(definition)  # noqa: SLF001
    _columns, _schema, _table, _refs, match, onupdate, ondelete, deferrable, initially = parsed
    values = {
        "onupdate": onupdate,
        "ondelete": ondelete,
        "initially": initially,
        "deferrable": None if deferrable is None else deferrable == "DEFERRABLE",
        "match": match,
    }
    return {k: v for k, v in values.items() if v is not None and v != "NO ACTION"}


# ---------------------------------------------------------------------------
# 1. What each reflection path returns
# ---------------------------------------------------------------------------


def test_sqlalchemy_reflects_the_list_as_postgresql_prints_it() -> None:
    assert _sqlalchemy_options(_DEF_LIST) == {
        "onupdate": "CASCADE", "ondelete": 'SET NULL (b, "Mixed Col")',
        "initially": "DEFERRED", "deferrable": True,
    }
    assert _sqlalchemy_options(_DEF_DEFAULT) == {"ondelete": "SET DEFAULT (b)"}
    assert _sqlalchemy_options(_DEF_PAREN) == {"ondelete": 'SET NULL ("x)y")'}


# pg_constraint for these keys as PostgreSQL 17.11 returned it for pg_foreign_keys_sql((17, 11)).
_CATALOG = {
    "child": {"conname": "fk_list", "columns": ["a", "b", "Mixed Col"], "ref_schema": "rev",
              "ref_table": "parent", "ref_columns": ["a", "b", "Mixed Col"], "on_update": "c",
              "on_delete": "n", "match_type": "s", "deferrable": True, "deferred": True,
              "definition": _DEF_LIST, "comment": None},
    "child2": {"conname": "fk_def", "columns": ["a", "b", "c"], "ref_schema": "rev",
               "ref_table": "parent", "ref_columns": ["a", "b", "Mixed Col"], "on_update": "a",
               "on_delete": "d", "match_type": "s", "deferrable": False, "deferred": False,
               "definition": _DEF_DEFAULT, "comment": None},
    "qc": {"conname": "qc_fk", "columns": ["a", "x)y"], "ref_schema": "rev", "ref_table": "qp",
           "ref_columns": ["a", "x)y"], "on_update": "a", "on_delete": "n", "match_type": "s",
           "deferrable": False, "deferred": False, "definition": _DEF_PAREN, "comment": None},
}


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self) -> list[dict]:
        return self._rows


class _Catalog:
    """A PostgreSQL 17.11 connection answering the foreign-key read of one table."""

    dialect = SimpleNamespace(name="postgresql", server_version_info=(17, 11))

    def execute(self, statement, params=None):  # noqa: ANN001, ANN201
        row = _CATALOG.get((params or {}).get("rel"))
        return _Result([row] if row else [])


@pytest.mark.parametrize(
    ("table", "definition"),
    [("child", _DEF_LIST), ("child2", _DEF_DEFAULT), ("qc", _DEF_PAREN)],
)
def test_the_catalog_read_returns_what_sqlalchemy_reflects(table: str, definition: str) -> None:
    (key,) = pg_foreign_keys_from_catalog(_Catalog(), "rev", table)
    # A quoted name holding a parenthesis too: the read cut it to SET NULL ("x).
    assert key["options"] == _sqlalchemy_options(definition)


class _Inspector:
    """SQLAlchemy's Inspector over the PostgreSQL 17.11 tables, each key's ``options`` as its
    PostgreSQL dialect builds them."""

    _FKS = {
        "child": [{"name": "fk_list", "constrained_columns": ["a", "b", "Mixed Col"],
                   "referred_schema": None, "referred_table": "parent",
                   "referred_columns": ["a", "b", "Mixed Col"],
                   "options": _sqlalchemy_options(_DEF_LIST), "comment": None}],
        "child2": [{"name": "fk_def", "constrained_columns": ["a", "b", "c"],
                    "referred_schema": None, "referred_table": "parent",
                    "referred_columns": ["a", "b", "Mixed Col"],
                    "options": _sqlalchemy_options(_DEF_DEFAULT), "comment": None}],
    }
    _COLUMNS = {
        "parent": ["a", "b", "Mixed Col"],
        "child": ["id", "a", "b", "Mixed Col"],
        "child2": ["id", "a", "b", "c"],
    }

    def __init__(self) -> None:
        self.dialect = SimpleNamespace(name="postgresql")

    def get_table_names(self, schema=None):  # noqa: ANN001, ANN201
        return list(self._COLUMNS) if schema == "rev" else []

    def get_view_names(self, schema=None):  # noqa: ANN001, ANN201
        return []

    def get_columns(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return [{"name": c, "type": "INTEGER", "nullable": c != "id"}
                for c in self._COLUMNS[table_name]]

    def get_pk_constraint(self, table_name, schema=None):  # noqa: ANN001, ANN201
        if table_name == "parent":
            return {"constrained_columns": ["a", "b", "Mixed Col"]}
        return {"constrained_columns": ["id"]}

    def get_indexes(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return []

    def get_foreign_keys(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return self._FKS.get(table_name, [])

    def get_check_constraints(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return []

    def get_table_comment(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return {"text": None}


def test_reflection_keeps_the_list_on_the_key() -> None:
    tables = {t.name.rsplit(".", 1)[-1]: t for t in _reflect_tables(_Inspector(), schema="rev")}
    (listed,) = tables["child"].foreign_keys
    assert (listed.on_delete, listed.on_update) == ('SET NULL (b, "Mixed Col")', "CASCADE")
    assert listed.on_delete_set_columns == ["b", "Mixed Col"]
    assert (listed.deferrable, listed.initially_deferred) == (True, True)
    assert listed.referenced_table == "rev.parent"
    (default,) = tables["child2"].foreign_keys
    assert (default.on_delete, default.on_delete_set_columns) == ("SET DEFAULT (b)", ["b"])


@pytest.mark.parametrize(
    ("on_delete", "columns"),
    [
        ("SET NULL (b)", ["b"]),
        ('SET NULL (b, "Mixed Col")', ["b", "Mixed Col"]),
        ("SET DEFAULT (b)", ["b"]),
        ('SET NULL ("x)y")', ["x)y"]),
        ('SET NULL ("q""z", a)', ['q"z', "a"]),
        # A bare name is folded, as PostgreSQL folds it (pg_get_constraintdef quotes any
        # other); an underscored action is read like the rest.
        ("SET_NULL (A)", ["a"]),
        ("set null ( b ,c ) ", ["b", "c"]),
        # No list, or one that cannot be read: none.
        ("SET NULL", []),
        ("CASCADE", []),
        ("RESTRICT (a)", []),
        ("SET NULL ()", []),
        ("SET NULL (b", []),
        ("SET NULL (b) x", []),
        (None, []),
    ],
)
def test_on_delete_set_columns(on_delete, columns) -> None:  # noqa: ANN001
    key = ForeignKeyDef(name="k", columns=["a"], referenced_table="rev.p",
                        referenced_columns=["a"], on_delete=on_delete)
    assert key.on_delete_set_columns == columns


# ---------------------------------------------------------------------------
# 2. Schema Conversion: the DDL every apply path runs
# ---------------------------------------------------------------------------

_INT = "integer"


def _parent() -> TableDef:
    return TableDef(
        name="rev.parent",
        columns=[ColumnDef(name=n, mysql_type=_INT, nullable=False)
                 for n in ("a", "b", "Mixed Col")],
        primary_key=["a", "b", "Mixed Col"],
    )


def _child(*foreign_keys: ForeignKeyDef) -> TableDef:
    return TableDef(
        name="rev.child",
        columns=[ColumnDef(name="id", mysql_type=_INT, nullable=False),
                 *(ColumnDef(name=n, mysql_type=_INT) for n in ("a", "b", "c", "Mixed Col"))],
        primary_key=["id"],
        foreign_keys=list(foreign_keys),
    )


_FK_LIST = ForeignKeyDef(name="fk_list", columns=["a", "b", "Mixed Col"],
                         referenced_table="rev.parent",
                         referenced_columns=["a", "b", "Mixed Col"], on_update="CASCADE",
                         on_delete='SET NULL (b, "Mixed Col")', deferrable=True,
                         initially_deferred=True)
_FK_DEFAULT = ForeignKeyDef(name="fk_def", columns=["a", "b", "c"],
                            referenced_table="rev.parent",
                            referenced_columns=["a", "b", "Mixed Col"],
                            on_delete="SET DEFAULT (b)")
_FK_PLAIN = ForeignKeyDef(name="fk_plain", columns=["a", "b", "c"],
                          referenced_table="rev.parent",
                          referenced_columns=["a", "b", "Mixed Col"], on_delete="SET NULL")

_HEAD = 'ALTER TABLE "rev"."child" ADD CONSTRAINT '
_TO_ABM = 'FOREIGN KEY ("a", "b", "Mixed Col") REFERENCES "rev"."parent" ("a", "b", "Mixed Col")'
_TO_ABC = 'FOREIGN KEY ("a", "b", "c") REFERENCES "rev"."parent" ("a", "b", "Mixed Col")'


def _convert(*tables: TableDef, source_type: SourceType = _PG, **options):  # noqa: ANN003, ANN202
    result = SchemaConverter(source_type=source_type).convert(
        SourceInventory(tables=list(tables)), SchemaConvertOptions(**options)
    )
    return {conversion.table: conversion for conversion in result.tables}


def _note(conversion, needle: str) -> str:  # noqa: ANN001
    (note,) = [w.message for w in conversion.warnings if needle in w.message]
    return note


def test_the_key_keeps_its_column_list() -> None:
    child = _convert(_parent(), _child(_FK_LIST, _FK_DEFAULT, _FK_PLAIN))["rev.child"]
    assert child.foreign_key_ddls == [
        # In PostgreSQL's grammar order, each listed name quoted like the key's columns.
        f'{_HEAD}"fk_list" {_TO_ABM} ON DELETE SET NULL ("b", "Mixed Col") ON UPDATE CASCADE '
        "DEFERRABLE INITIALLY DEFERRED NOT VALID",
        f'{_HEAD}"fk_def" {_TO_ABC} ON DELETE SET DEFAULT ("b") NOT VALID',
        # A plain SET NULL sets every key column, on the source too.
        f'{_HEAD}"fk_plain" {_TO_ABC} ON DELETE SET NULL NOT VALID',
    ]
    assert child.preserved_foreign_keys == [_FK_LIST, _FK_DEFAULT, _FK_PLAIN]


def test_a_quoted_name_in_the_list_is_quoted_again() -> None:
    key = ForeignKeyDef(name="fk_q", columns=["a", 'q"z', "x)y"], referenced_table="rev.parent",
                        referenced_columns=["a", "b", "Mixed Col"],
                        on_delete='SET NULL ("q""z", "x)y")')
    table = _child(key)
    table.columns += [ColumnDef(name='q"z', mysql_type=_INT), ColumnDef(name="x)y", mysql_type=_INT)]
    child = _convert(_parent(), table)["rev.child"]
    assert child.foreign_key_ddls == [
        f'{_HEAD}"fk_q" FOREIGN KEY ("a", "q""z", "x)y") REFERENCES "rev"."parent" ("a", "b", '
        '"Mixed Col") ON DELETE SET NULL ("q""z", "x)y") NOT VALID'
    ]


def test_on_update_and_an_unreadable_list_keep_the_action_alone() -> None:
    # PostgreSQL takes a list on ON DELETE only ("a column list with SET NULL is only
    # supported for ON DELETE actions"), so a hand-written inventory's ON UPDATE one goes;
    # a list that cannot be read leaves the action as it was written before G193.
    on_update = ForeignKeyDef(name="fk_u", columns=["a", "b", "c"],
                              referenced_table="rev.parent",
                              referenced_columns=["a", "b", "Mixed Col"],
                              on_update="SET NULL (b)", on_delete="SET NULL (b")
    child = _convert(_parent(), _child(on_update))["rev.child"]
    assert child.foreign_key_ddls == [
        f'{_HEAD}"fk_u" {_TO_ABC} ON DELETE SET NULL ON UPDATE SET NULL NOT VALID'
    ]


def test_a_mysql_key_converts_as_before() -> None:
    # A MySQL key has no column list (MySQL has no such clause, and SQLAlchemy's MySQL dialect
    # reads none); one in a hand-built MySQL inventory is still left out.
    orders = TableDef(
        name="shop.orders",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="cid", mysql_type="int"), ColumnDef(name="sid", mysql_type="int")],
        primary_key=["id"],
        foreign_keys=[
            ForeignKeyDef(name="fk_o_c", columns=["cid"], referenced_table="shop.customers",
                          referenced_columns=["id"], on_delete="SET NULL"),
            ForeignKeyDef(name="fk_o_s", columns=["cid", "sid"],
                          referenced_table="shop.stores", referenced_columns=["cid", "id"],
                          on_delete="SET NULL (sid)"),
        ],
    )
    conversion = _convert(orders, source_type=SourceType.MYSQL)["shop.orders"]
    assert conversion.foreign_key_ddls == [
        'ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_o_c" FOREIGN KEY ("cid") '
        'REFERENCES "shop"."customers" ("id") ON DELETE SET NULL NOT VALID',
        'ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_o_s" FOREIGN KEY ("cid", "sid") '
        'REFERENCES "shop"."stores" ("cid", "id") ON DELETE SET NULL NOT VALID',
    ]
    assert "column list" not in _note(conversion, "are preserved and re-created")
    item = next(
        i for i in CompatibilityAssessor(source_type=SourceType.MYSQL).assess(
            SourceInventory(tables=[orders])
        ).items
        if i.object_name == "shop.orders"
    )
    (preserved,) = [c for c in item.concerns if c.rule_id == "FK_PRESERVED"]
    assert "column list" not in preserved.risk


def test_what_reads_a_keys_ddl_reads_the_listed_key_as_before() -> None:
    from dsql_migrator.core.column_exclusion import _applied_foreign_keys
    from dsql_migrator.ui.data_migration._fk_validation import _fk_ddl_shape
    from dsql_migrator.ui.data_migration._full_load_engine import _named_foreign_key_ddls
    from dsql_migrator.ui.schema_conversion_apply import _key_definition

    child = _convert(_parent(), _child(_FK_LIST))["rev.child"]
    (ddl,) = child.foreign_key_ddls
    # The name the Full Load pass, Cut over and a reload's pre-drop pair it by.
    assert _named_foreign_key_ddls([ddl]) == [("fk_list", ddl)]
    assert _fk_ddl_shape(ddl) == (["a", "b", "Mixed Col"], "rev.parent", ["a", "b", "Mixed Col"])
    assert _applied_foreign_keys(_child(_FK_LIST), child) == [
        ("fk_list", "rev.parent", ["a", "b", "Mixed Col"], ["a", "b", "Mixed Col"], False)
    ]
    # The AI merge: an unnamed, unquoted restatement is the same key; one without the list
    # is another definition, added beside it as any changed action is.
    restated = ('ALTER TABLE rev.child ADD FOREIGN KEY (a, b, "Mixed Col") REFERENCES '
                'rev.parent (a, b, "Mixed Col") ON DELETE SET NULL (b, "Mixed Col") ON UPDATE '
                "CASCADE DEFERRABLE INITIALLY DEFERRED NOT VALID")
    assert _key_definition(restated) == _key_definition(ddl)
    assert _key_definition(restated.replace(' (b, "Mixed Col") ON', " ON")) != _key_definition(ddl)


# ---------------------------------------------------------------------------
# 3. The notes, the source DDL panel and the AI source detail
# ---------------------------------------------------------------------------

_KEPT = (
    "Their ON DELETE column lists are kept, so a deleted parent row sets only the listed key "
    'columns, as on the source: fk_list (ON DELETE SET NULL ("b", "Mixed Col")), fk_def (ON '
    'DELETE SET DEFAULT ("b")).'
)


def test_the_preserved_note_and_fk_preserved_name_each_list() -> None:
    child = _convert(_parent(), _child(_FK_LIST, _FK_DEFAULT, _FK_PLAIN))["rev.child"]
    note = _note(child, "are preserved and re-created")
    assert _KEPT in note
    # The deferral sentence is unchanged, and comes first.
    assert ("Their deferral and matching are kept as the source declares them: fk_list "
            "(DEFERRABLE INITIALLY DEFERRED). " + _KEPT) in note
    item = next(
        i for i in CompatibilityAssessor(source_type=_PG).assess(
            SourceInventory(tables=[_parent(), _child(_FK_LIST, _FK_DEFAULT, _FK_PLAIN)])
        ).items
        if i.object_name == "rev.child"
    )
    (preserved,) = [c for c in item.concerns if c.rule_id == "FK_PRESERVED"]
    assert _KEPT in preserved.risk
    # No list: no sentence.
    plain = _convert(_parent(), _child(_FK_PLAIN))["rev.child"]
    assert "column list" not in _note(plain, "are preserved and re-created")


def test_the_source_panel_and_the_ai_detail_show_the_list_as_the_source_declares_it() -> None:
    from dsql_migrator.ui.ai_tools import build_ai_tool_executor
    from dsql_migrator.ui.schema_conversion import render_source_table_ddl

    source = render_source_table_ddl(_child(_FK_LIST), source_type=_PG)
    # pg_get_constraintdef's own text after REFERENCES (_DEF_LIST); the target writes each
    # listed name quoted, so the side-by-side diff shows no lost clause.
    assert ('REFERENCES "rev"."parent" ("a", "b", "Mixed Col") ON UPDATE CASCADE ON DELETE SET '
            'NULL (b, "Mixed Col") DEFERRABLE INITIALLY DEFERRED') in source

    class _Obj:
        def __init__(self, **kw) -> None:  # noqa: ANN003
            self.__dict__.update(kw)

    class _Store:
        def __init__(self, obj=None, *, get_returns=None) -> None:  # noqa: ANN001
            self._obj, self._get = obj, get_returns

        def get_or_create(self, _sid):  # noqa: ANN001, ANN202
            return self._obj

        def get(self, _sid):  # noqa: ANN001, ANN202
            return self._get

    inventory = SourceInventory(tables=[_parent(), _child(_FK_LIST)])
    execute = build_ai_tool_executor(
        session_id="s1",
        session_store=_Store(_Obj(target_config=None, target_verified=False,
                                  source_config=None, source_password=None)),
        evaluation_store=_Store(get_returns=_Obj(result=_Obj(inventory=inventory))),
        schema_conversion_store=_Store(_Obj(generated_node_ids=[], apply_results=[])),
        validation_store=_Store(_Obj(result=None)),
        data_migration_store=_Store(_Obj(job_id=None, get_prereq_report=lambda _m: None)),
        job_manager=_Obj(),
        full_load_rate_eta=lambda *_a, **_k: (None, None),
    )
    detail = json.loads(execute("get_source_object_detail", {"object_name": "rev.child"}))
    (key,) = detail["foreign_keys"]
    assert (key["on_delete"], key["on_update"]) == ('SET NULL (b, "Mixed Col")', "CASCADE")


# ---------------------------------------------------------------------------
# 4. Persistence
# ---------------------------------------------------------------------------


def test_an_older_inventory_loads_unchanged_and_converts_with_its_list() -> None:
    # As an older build saved them: a plain SET NULL, and a list kept as it was reflected
    # (only the DDL that build wrote dropped it).
    saved = SourceInventory(tables=[_parent(), _child(_FK_PLAIN, _FK_DEFAULT)]).model_dump_json()
    loaded = SourceInventory.model_validate_json(saved)
    assert loaded.model_dump_json() == saved
    # Nothing new is written for a key: an inventory saved here loads on an older build.
    for key in json.loads(saved)["tables"][1]["foreign_keys"]:
        assert set(key) == {
            "name", "columns", "referenced_table", "referenced_columns", "on_delete",
            "on_update",
        }
    # Converted now, the key keeps the list it was saved with.
    ddls = _convert(*loaded.tables)["rev.child"].foreign_key_ddls
    assert ddls == [
        f'{_HEAD}"fk_plain" {_TO_ABC} ON DELETE SET NULL NOT VALID',
        f'{_HEAD}"fk_def" {_TO_ABC} ON DELETE SET DEFAULT ("b") NOT VALID',
    ]


# ---------------------------------------------------------------------------
# 5. The manual says the same in every language
# ---------------------------------------------------------------------------

_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"

_SAYS = {
    "en": ("sets only the listed columns to NULL (or to their default)",
           "which sets only those columns"),
    "ko": ("나열된 컬럼만 NULL(또는 기본값)로 설정됩니다", "그 컬럼만 설정하는 컬럼 목록"),
    "ja": ("列だけが NULL(またはデフォルト値)に設定されます", "その列だけを設定する列リスト"),
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_the_list_is_kept(language: str) -> None:
    text = " ".join(
        (_MANUAL / language / "02-evaluation-and-schema-conversion.md")
        .read_text(encoding="utf-8").split()
    )
    evaluation = text.split("## 2.1", 1)[1].split("## 2.2", 1)[0]
    schema = text.split("## 2.2", 1)[1].split("## 2.3", 1)[0]
    evaluation_says, schema_says = _SAYS[language]
    for section, says in ((evaluation, evaluation_says), (schema, schema_says)):
        assert "`ON DELETE SET NULL (…)`" in section and "`SET DEFAULT (…)`" in section
        assert "PostgreSQL 15+" in section and says in section, (language, says)
