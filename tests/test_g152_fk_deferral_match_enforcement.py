# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G152: PostgreSQL DEFERRABLE foreign keys, MATCH FULL, PG18 NOT ENFORCED, DEFERRABLE keys.

Live-verified on Aurora DSQL (2026-10-23): ``ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY
... REFERENCES ... DEFERRABLE INITIALLY DEFERRED NOT VALID`` is accepted and defers the check
to commit; ``DEFERRABLE INITIALLY IMMEDIATE NOT VALID`` and ``MATCH FULL NOT VALID`` are
accepted; ``ADD CONSTRAINT ... UNIQUE ... DEFERRABLE`` is rejected (unique keys are indexes).

* Introspection keeps SQLAlchemy's FK options ``deferrable`` / ``initially`` / ``match`` for
  a PostgreSQL source (never for MySQL), and one catalog read per schema records the
  DEFERRABLE primary / unique keys and, on PostgreSQL 18 only (``conenforced``), the NOT
  ENFORCED foreign keys.
* Schema Conversion writes ``[MATCH FULL] [ON DELETE] [ON UPDATE] [DEFERRABLE INITIALLY
  DEFERRED | IMMEDIATE] NOT VALID``; a deferrable key keeps RESTRICT (PostgreSQL never
  defers it); a NOT ENFORCED key is not created. Every apply path runs those statements.
* Evaluation: FK_PRESERVED names what is kept; PG_FK_NOT_ENFORCED (advice) and
  PG_DEFERRABLE_KEY (MANUAL) carry the conversion's notes.
* Orphan counts treat a partly NULL MATCH FULL key as an orphan; Validation skips a NOT
  ENFORCED key.

The SQLAlchemy option dicts and catalog rows below are what SQLAlchemy 2.0.50 and
PostgreSQL 18.6 / 17.11 / 13.23 returned for these definitions while this was written
(throwaway local containers); the tests use doubles only.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter, _normalize_fk_action
from dsql_migrator.core.introspector import _assemble_inventory, _reflect_tables
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    ConversionNoteKind,
    DeferrableKeyDef,
    EffortLevel,
    ForeignKeyDef,
    IndexDef,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    TableDef,
    TargetConnectionConfig,
    TemporalKeyDef,
)
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.source_dialect.postgres import _pg_read_key_timing, pg_key_timing_sql
from dsql_migrator.core.validation_sql import (
    build_orphan_count_sql,
    build_orphan_listing_sql,
    build_pg_composite_orphan_page_sql,
    build_pg_orphan_page_first_sql,
)
from dsql_migrator.ui.schema_conversion import render_source_table_ddl

_PG = SourceType.POSTGRES


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------


class _UndefinedColumn(Exception):
    """What PostgreSQL 13-17 raise for a statement naming ``conenforced`` (42703)."""

    sqlstate = "42703"


class _Rows:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self) -> list[dict]:
        return self._rows

    def first(self):  # noqa: ANN201
        return tuple(self._rows[0].values()) if self._rows else None

    def scalar(self):  # noqa: ANN201
        return None

    def __iter__(self):  # noqa: ANN204
        return iter(tuple(r.values()) for r in self._rows)


# pg_constraint as PostgreSQL 18.6 returned it for the G152 read of the tables below
# (``pg_get_constraintdef`` text as printed). A 13-17 catalog has the same key rows and no
# NOT ENFORCED foreign key (there is no such thing before 18).
_KEY_TIMING = [
    {"nsp": "app", "rel": "dkey", "conname": "dkey_code_key", "contype": "u",
     "deferred": False, "definition": "UNIQUE (code) DEFERRABLE", "index_name": "dkey_code_key"},
    {"nsp": "app", "rel": "dkey", "conname": "dkey_pkey", "contype": "p", "deferred": True,
     "definition": "PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED", "index_name": "dkey_pkey"},
    {"nsp": "app", "rel": "ne_child", "conname": "fk_ne", "contype": "f", "deferred": False,
     "definition": "FOREIGN KEY (pid) REFERENCES app.parent(id) NOT ENFORCED",
     "index_name": "parent_pkey"},
    {"nsp": "app", "rel": "ne_child", "conname": "fk_ne_def", "contype": "f", "deferred": True,
     "definition": "FOREIGN KEY (pid) REFERENCES app.parent(id) DEFERRABLE INITIALLY DEFERRED "
                   "NOT ENFORCED", "index_name": "parent_pkey"},
    {"nsp": "other", "rel": "dkey", "conname": "elsewhere_pkey", "contype": "p",
     "deferred": False, "definition": "PRIMARY KEY (id) DEFERRABLE", "index_name": "x"},
]


class _PgCatalog:
    """A PostgreSQL connection of one server version over the G152 catalog rows.

    Faithful where it matters: a statement naming ``conenforced`` fails on a server before 18,
    a NOT ENFORCED foreign key ('f') comes back only when the statement asks for it, and the
    ``:nsp`` / ``:names`` binds filter the rows. Every other catalog read answers nothing
    (``schemas`` for the schema list)."""

    class _Dialect:
        name = "postgresql"

        def __init__(self, version) -> None:  # noqa: ANN001
            self.server_version_info = version

    def __init__(self, rows: list[dict], version, schemas=("app",)) -> None:  # noqa: ANN001
        self.dialect = self._Dialect(version)
        self._rows = rows
        self._schemas = list(schemas)
        self.statements: list[tuple[str, dict]] = []

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        params = dict(parameters or {})
        self.statements.append((sql, params))
        if "conenforced" in sql and tuple(self.dialect.server_version_info or (0,)) < (18,):
            raise _UndefinedColumn("column con.conenforced does not exist")
        if "pg_get_constraintdef" in sql and "con.condeferrable" in sql:
            wanted = set(params.get("names") or ())
            return _Rows([
                {k: v for k, v in row.items() if k != "nsp"}
                for row in self._rows
                if row["nsp"] == params.get("nsp")
                and row["rel"] in wanted
                and (row["contype"] != "f" or "con.conenforced" in sql)
            ])
        if "NOT LIKE" in sql:
            return _Rows([{"nspname": name} for name in self._schemas])
        return _Rows([])


def _fk(  # noqa: ANN001
    name: str, columns=("pid",), parent="app.parent", ref=("id",), **flags
) -> ForeignKeyDef:
    return ForeignKeyDef(name=name, columns=list(columns), referenced_table=parent,
                         referenced_columns=list(ref), **flags)


_PK = ColumnDef(name="id", mysql_type="bigint", nullable=False)


def _parent() -> TableDef:
    return TableDef(
        name="app.parent",
        columns=[_PK, ColumnDef(name="a", mysql_type="integer"),
                 ColumnDef(name="b", mysql_type="integer")],
        primary_key=["id"],
        indexes=[IndexDef(name="parent_a_b_key", columns=["a", "b"], unique=True)],
    )


def _child(*foreign_keys: ForeignKeyDef, name: str = "app.child") -> TableDef:
    return TableDef(
        name=name,
        columns=[_PK, ColumnDef(name="pid", mysql_type="bigint"),
                 ColumnDef(name="a", mysql_type="integer"),
                 ColumnDef(name="b", mysql_type="integer")],
        primary_key=["id"],
        foreign_keys=list(foreign_keys),
    )


# The child's keys as PostgreSQL 18.6 defined them, read back through SQLAlchemy.
_FK_ALL = _fk("fk_all", ("a", "b"), ref=("a", "b"), on_delete="RESTRICT", deferrable=True,
              initially_deferred=True, match_full=True)
_FK_DEFERRABLE = _fk("fk_deferrable", on_delete="CASCADE", deferrable=True)
_FK_DEFERRED = _fk("fk_deferred", deferrable=True, initially_deferred=True)
_FK_FULL = _fk("fk_full", ("a", "b"), ref=("a", "b"), on_update="SET NULL",
               on_delete="RESTRICT", match_full=True)
_FK_PLAIN = _fk("fk_plain")
_FK_NE = _fk("fk_ne", enforced=False)

_HEAD = 'ALTER TABLE "app"."child" ADD CONSTRAINT '
_TO_ID = 'FOREIGN KEY ("pid") REFERENCES "app"."parent" ("id")'
_TO_AB = 'FOREIGN KEY ("a", "b") REFERENCES "app"."parent" ("a", "b")'


def _dkey(**kwargs) -> TableDef:  # noqa: ANN003
    kwargs.setdefault("deferrable_keys", [
        DeferrableKeyDef(name="dkey_pkey", definition="PRIMARY KEY (id) DEFERRABLE INITIALLY "
                         "DEFERRED", primary=True, initially_deferred=True, index="dkey_pkey"),
        DeferrableKeyDef(name="dkey_code_key", definition="UNIQUE (code) DEFERRABLE",
                         index="dkey_code_key"),
    ])
    return TableDef(
        name="app.dkey",
        columns=[_PK, ColumnDef(name="code", mysql_type="character varying(40)"),
                 ColumnDef(name="slot", mysql_type="integer")],
        primary_key=["id"],
        indexes=[IndexDef(name="dkey_code_key", columns=["code"], unique=True),
                 IndexDef(name="dkey_slot_key", columns=["slot"], unique=True)],
        **kwargs,
    )


def _convert(*tables: TableDef, source_type: SourceType = _PG, **options):  # noqa: ANN003, ANN202
    from dsql_migrator.core.converter import SchemaConvertOptions

    result = SchemaConverter(source_type=source_type).convert(
        SourceInventory(tables=list(tables)), SchemaConvertOptions(**options)
    )
    return {conversion.table: conversion for conversion in result.tables}


def _notes(conversion, needle: str):  # noqa: ANN001, ANN202
    return [w for w in conversion.warnings if needle in w.message]


# ---------------------------------------------------------------------------
# 1. Introspection
# ---------------------------------------------------------------------------


class _Inspector:
    """``get_foreign_keys`` etc. as SQLAlchemy 2.0.50 reflected the PostgreSQL 18.6 tables
    (``options`` exactly as its PostgreSQL dialect parsed each ``pg_get_constraintdef``)."""

    _FKS = {
        "child": [
            {"name": "fk_all", "constrained_columns": ["a", "b"], "referred_schema": None,
             "referred_table": "parent", "referred_columns": ["a", "b"],
             "options": {"ondelete": "RESTRICT", "initially": "DEFERRED", "deferrable": True,
                         "match": "FULL"}, "comment": None},
            {"name": "fk_deferrable", "constrained_columns": ["pid"], "referred_schema": None,
             "referred_table": "parent", "referred_columns": ["id"],
             "options": {"ondelete": "CASCADE", "deferrable": True}, "comment": None},
            {"name": "fk_deferred", "constrained_columns": ["pid"], "referred_schema": None,
             "referred_table": "parent", "referred_columns": ["id"],
             "options": {"initially": "DEFERRED", "deferrable": True}, "comment": None},
            {"name": "fk_full", "constrained_columns": ["a", "b"], "referred_schema": None,
             "referred_table": "parent", "referred_columns": ["a", "b"],
             "options": {"onupdate": "SET NULL", "ondelete": "RESTRICT", "match": "FULL"},
             "comment": None},
            {"name": "fk_plain", "constrained_columns": ["pid"], "referred_schema": None,
             "referred_table": "parent", "referred_columns": ["id"], "options": {},
             "comment": None},
        ],
        # NOT ENFORCED is not in SQLAlchemy's options: pg_get_constraintdef's " NOT ENFORCED"
        # follows what its pattern reads.
        "ne_child": [
            {"name": "fk_ne", "constrained_columns": ["pid"], "referred_schema": None,
             "referred_table": "parent", "referred_columns": ["id"], "options": {},
             "comment": None},
            {"name": "fk_ne_def", "constrained_columns": ["pid"], "referred_schema": None,
             "referred_table": "parent", "referred_columns": ["id"],
             "options": {"initially": "DEFERRED", "deferrable": True}, "comment": None},
        ],
    }
    _COLUMNS = {
        "parent": ["id", "a", "b"],
        "child": ["id", "pid", "a", "b"],
        "ne_child": ["id", "pid"],
        "dkey": ["id", "code", "slot"],
    }
    _INDEXES = {
        "parent": [{"name": "parent_a_b_key", "unique": True, "column_names": ["a", "b"],
                    "duplicates_constraint": "parent_a_b_key",
                    "dialect_options": {"postgresql_include": []}}],
        # A DEFERRABLE UNIQUE constraint's index reflects as an ordinary unique one.
        "dkey": [{"name": "dkey_code_key", "unique": True, "column_names": ["code"],
                  "duplicates_constraint": "dkey_code_key",
                  "dialect_options": {"postgresql_include": []}},
                 {"name": "dkey_slot_key", "unique": True, "column_names": ["slot"],
                  "duplicates_constraint": "dkey_slot_key",
                  "dialect_options": {"postgresql_include": []}}],
    }

    def __init__(self, dialect_name: str = "postgresql") -> None:
        self.dialect = SimpleNamespace(name=dialect_name)

    def get_table_names(self, schema=None):  # noqa: ANN001, ANN201
        return list(self._COLUMNS) if schema == "app" else []

    def get_view_names(self, schema=None):  # noqa: ANN001, ANN201
        return []

    def get_columns(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return [{"name": c, "type": "INTEGER", "nullable": c != "id"}
                for c in self._COLUMNS[table_name]]

    def get_pk_constraint(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return {"constrained_columns": ["id"]}

    def get_indexes(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return self._INDEXES.get(table_name, [])

    def get_foreign_keys(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return self._FKS.get(table_name, [])

    def get_check_constraints(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return []

    def get_table_comment(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return {"text": None}


def _by_name(tables: list[TableDef]) -> dict[str, TableDef]:
    return {table.name.rsplit(".", 1)[-1]: table for table in tables}


def test_reflection_keeps_a_postgresql_keys_deferral_and_match() -> None:
    child = _by_name(_reflect_tables(_Inspector(), schema="app"))["child"]
    flags = {
        fk.name: (fk.deferrable, fk.initially_deferred, fk.match_full, fk.enforced,
                  fk.on_delete, fk.on_update)
        for fk in child.foreign_keys
    }
    assert flags == {
        "fk_all": (True, True, True, True, "RESTRICT", None),
        "fk_deferrable": (True, False, False, True, "CASCADE", None),
        "fk_deferred": (True, True, False, True, None, None),
        "fk_full": (False, False, True, True, "RESTRICT", "SET NULL"),
        "fk_plain": (False, False, False, True, None, None),
    }


def test_a_mysql_key_never_takes_them() -> None:
    """MySQL foreign keys are never deferrable and InnoDB ignores MATCH: even options that
    said otherwise leave a MySQL key at the defaults (its actions are still read)."""
    child = _by_name(_reflect_tables(_Inspector("mysql"), schema="app"))["child"]
    assert all(
        (fk.deferrable, fk.initially_deferred, fk.match_full, fk.enforced)
        == (False, False, False, True)
        for fk in child.foreign_keys
    )
    assert {fk.name: fk.on_delete for fk in child.foreign_keys}["fk_deferrable"] == "CASCADE"


@pytest.mark.parametrize("version", [None, (13, 23), (14, 0), (16, 4), (17, 11)])
def test_the_catalog_read_names_conenforced_only_from_18(version) -> None:  # noqa: ANN001
    sql = pg_key_timing_sql(version)
    assert "conenforced" not in sql
    assert "(con.contype IN ('p', 'u') AND con.condeferrable)" in sql
    assert "pg_catalog.pg_get_constraintdef(con.oid)" in sql and "con.condeferred" in sql
    assert "n.nspname = :nsp" in sql and "c.relname = ANY(:names)" in sql


@pytest.mark.parametrize("version", [(18, 0), (18, 6), (19, 1)])
def test_the_catalog_read_asks_for_not_enforced_keys_on_18(version) -> None:  # noqa: ANN001
    sql = pg_key_timing_sql(version)
    assert "OR (con.contype = 'f' AND NOT con.conenforced)" in sql


def _bare() -> list[TableDef]:
    """The tables as reflection leaves them, BEFORE enrichment (bare names)."""
    tables = []
    for table in (_child(_FK_PLAIN), _child(_fk("fk_ne"), _fk("fk_ne_def", deferrable=True,
                                                               initially_deferred=True),
                                            name="ne_child"), _dkey(deferrable_keys=[])):
        table.name = table.name.rsplit(".", 1)[-1]
        tables.append(table)
    return tables


@pytest.mark.parametrize("version", [(13, 23), (15, 0), (17, 11)])
def test_enrich_on_a_catalog_without_conenforced(version) -> None:  # noqa: ANN001
    """PostgreSQL 13-17: the read never names conenforced (the double raises as they do);
    the DEFERRABLE keys are recorded and every foreign key stays enforced."""
    connection = _PgCatalog(_KEY_TIMING, version)
    tables = _bare()
    dialect_for(_PG).enrich(connection, "app", tables)
    by_name = _by_name(tables)
    assert [key.name for key in by_name["dkey"].deferrable_keys] == ["dkey_code_key", "dkey_pkey"]
    assert all(fk.enforced for table in tables for fk in table.foreign_keys)
    reads = [(sql, p) for sql, p in connection.statements if "con.condeferrable" in sql]
    assert len(reads) == 1  # ONE statement for the schema, scoped to the tables read
    assert reads[0][1]["nsp"] == "app"
    assert sorted(reads[0][1]["names"]) == sorted(t.name for t in tables)


def test_enrich_records_deferrable_keys_and_not_enforced_keys_on_18() -> None:
    tables = _bare()
    _pg_read_key_timing(_PgCatalog(_KEY_TIMING, (18, 6)), "app", tables)
    by_name = _by_name(tables)
    assert by_name["dkey"].deferrable_keys == [
        DeferrableKeyDef(name="dkey_code_key", definition="UNIQUE (code) DEFERRABLE",
                         primary=False, initially_deferred=False, index="dkey_code_key"),
        DeferrableKeyDef(name="dkey_pkey", definition="PRIMARY KEY (id) DEFERRABLE INITIALLY "
                         "DEFERRED", primary=True, initially_deferred=True, index="dkey_pkey"),
    ]
    assert {fk.name: fk.enforced for fk in by_name["ne_child"].foreign_keys} == {
        "fk_ne": False, "fk_ne_def": False,
    }
    # The key stays where it is (its index too): only its flag changed.
    assert [i.name for i in by_name["dkey"].indexes] == ["dkey_code_key", "dkey_slot_key"]
    assert by_name["child"].foreign_keys == [_FK_PLAIN]
    # Another schema's same-named table is not this one.
    assert all(key.name != "elsewhere_pkey" for t in tables for key in t.deferrable_keys)


def test_enrich_skips_rows_of_another_shape_and_reads_nothing_without_tables() -> None:
    class _Other(_PgCatalog):
        def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
            if "con.condeferrable" in str(statement):
                return _Rows([{"child": "dkey", "conname": "fk", "ref_schema": "app"},
                              {"rel": "dkey", "conname": "c1", "contype": "c"},
                              {"rel": "ne_child", "contype": "f"}])
            return super().execute(statement, parameters)

    tables = _bare()
    _pg_read_key_timing(_Other([], (18, 0)), "app", tables)
    assert all(not t.deferrable_keys for t in tables)
    assert all(fk.enforced for t in tables for fk in t.foreign_keys)
    connection = _PgCatalog(_KEY_TIMING, (18, 0))
    _pg_read_key_timing(connection, "app", [])
    assert connection.statements == []


@pytest.mark.parametrize("version", [(17, 11), (18, 6)])
def test_the_whole_catalog_read_end_to_end(version) -> None:  # noqa: ANN001
    connection = _PgCatalog(_KEY_TIMING, version)
    inventory = _assemble_inventory(_Inspector(), connection, "postgres", dialect=dialect_for(_PG))
    tables = {table.name: table for table in inventory.tables}
    fks = {fk.name: fk for fk in tables["app.child"].foreign_keys}
    assert fks["fk_all"].match_full and fks["fk_all"].initially_deferred
    assert fks["fk_all"].referenced_table == "app.parent"
    assert [k.name for k in tables["app.dkey"].deferrable_keys] == ["dkey_code_key", "dkey_pkey"]
    enforced = {fk.name: fk.enforced for fk in tables["app.ne_child"].foreign_keys}
    assert enforced == {"fk_ne": version < (18,), "fk_ne_def": version < (18,)}


# ---------------------------------------------------------------------------
# 2. Schema Conversion: the DDL every apply path runs
# ---------------------------------------------------------------------------


def test_the_key_keeps_match_full_and_its_deferral_in_grammar_order() -> None:
    child = _convert(_parent(), _child(_FK_ALL, _FK_DEFERRABLE, _FK_DEFERRED, _FK_FULL,
                                       _FK_PLAIN))["app.child"]
    assert child.foreign_key_ddls == [
        # A deferrable key keeps RESTRICT: PostgreSQL never defers it.
        f'{_HEAD}"fk_all" {_TO_AB} MATCH FULL ON DELETE RESTRICT DEFERRABLE INITIALLY '
        "DEFERRED NOT VALID",
        # INITIALLY IMMEDIATE is spelled out (the form live-verified on Aurora DSQL).
        f'{_HEAD}"fk_deferrable" {_TO_ID} ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE '
        "NOT VALID",
        f'{_HEAD}"fk_deferred" {_TO_ID} DEFERRABLE INITIALLY DEFERRED NOT VALID',
        # RESTRICT of a key that is not deferrable stays NO ACTION, as before.
        f'{_HEAD}"fk_full" {_TO_AB} MATCH FULL ON UPDATE SET NULL NOT VALID',
        f'{_HEAD}"fk_plain" {_TO_ID} NOT VALID',
    ]
    assert child.preserved_foreign_keys == [_FK_ALL, _FK_DEFERRABLE, _FK_DEFERRED, _FK_FULL,
                                            _FK_PLAIN]


def test_initially_deferred_alone_is_deferrable() -> None:
    """INITIALLY DEFERRED implies DEFERRABLE in PostgreSQL (a hand-written inventory may
    carry only the one flag)."""
    ddl = _convert(_parent(), _child(_fk("fk_x", initially_deferred=True,
                                         on_update="SET NULL (pid)")))["app.child"]
    assert ddl.foreign_key_ddls == [
        f'{_HEAD}"fk_x" {_TO_ID} ON UPDATE SET NULL DEFERRABLE INITIALLY DEFERRED NOT VALID'
    ]


def test_restrict_is_kept_only_for_a_deferrable_key() -> None:
    assert _normalize_fk_action("RESTRICT") == "NO ACTION"
    assert _normalize_fk_action("RESTRICT", deferrable=True) == "RESTRICT"
    assert _normalize_fk_action("CASCADE", deferrable=True) == "CASCADE"
    assert _normalize_fk_action("NO ACTION", deferrable=True) == "NO ACTION"


def test_a_not_enforced_key_is_not_created_and_the_note_says_so() -> None:
    child = _convert(_parent(), _child(_FK_PLAIN, _FK_NE))["app.child"]
    assert child.foreign_key_ddls == [f'{_HEAD}"fk_plain" {_TO_ID} NOT VALID']
    # Kept as metadata: a DDL the operator adds for it by hand is paired by its name.
    assert _FK_NE in child.preserved_foreign_keys
    (note,) = _notes(child, "NOT ENFORCED")
    assert note.classification is Classification.MANUAL
    assert note.kind is ConversionNoteKind.RECOMMENDATION
    assert "Foreign key constraints (fk_ne) are NOT ENFORCED on the source" in note.message
    assert "would check every write against them" in note.message
    (preserved,) = _notes(child, "are preserved and re-created")
    assert "(fk_plain)" in preserved.message and "fk_ne" not in preserved.message
    # Only NOT ENFORCED keys: no "preserved" note at all.
    alone = _convert(_parent(), _child(_FK_NE))["app.child"]
    assert alone.foreign_key_ddls == [] and not _notes(alone, "are preserved")
    # Stripped: the loss note names only the keys that would have been created.
    stripped = _convert(_parent(), _child(_FK_PLAIN, _FK_NE), preserve_foreign_keys=False)
    (loss,) = _notes(stripped["app.child"], "removed at your request")
    assert "(fk_plain)" in loss.message and "fk_ne" not in loss.message


def test_the_preserved_note_names_what_each_key_keeps() -> None:
    child = _convert(_parent(), _child(_FK_ALL, _FK_DEFERRABLE, _FK_FULL, _FK_PLAIN))
    (note,) = _notes(child["app.child"], "are preserved and re-created")
    assert note.kind is ConversionNoteKind.RECOMMENDATION
    assert (
        "Their deferral and matching are kept as the source declares them: fk_all (MATCH "
        "FULL, DEFERRABLE INITIALLY DEFERRED), fk_deferrable (DEFERRABLE INITIALLY "
        "IMMEDIATE), fk_full (MATCH FULL)."
    ) in note.message


def test_a_mysql_key_converts_as_before() -> None:
    orders = TableDef(
        name="shop.orders",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="cid", mysql_type="int")],
        primary_key=["id"],
        foreign_keys=[ForeignKeyDef(name="fk_o_c", columns=["cid"],
                                    referenced_table="shop.customers",
                                    referenced_columns=["id"], on_delete="RESTRICT")],
    )
    customers = TableDef(name="shop.customers",
                         columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                         primary_key=["id"])
    conversion = _convert(customers, orders, source_type=SourceType.MYSQL)["shop.orders"]
    assert conversion.foreign_key_ddls == [
        'ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_o_c" FOREIGN KEY ("cid") '
        'REFERENCES "shop"."customers" ("id") NOT VALID'
    ]
    (note,) = _notes(conversion, "are preserved and re-created")
    assert "deferral" not in note.message
    assert not _notes(conversion, "DEFERRABLE")


def test_a_deferrable_key_is_an_ordinary_key_with_a_note() -> None:
    dkey = _convert(_dkey())["app.dkey"]
    assert 'PRIMARY KEY ("id")' in dkey.target_ddl and "DEFERRABLE" not in dkey.target_ddl
    assert dkey.index_ddls == [
        'CREATE UNIQUE INDEX ASYNC "dkey_code_key" ON "app"."dkey" ("code")',
        'CREATE UNIQUE INDEX ASYNC "dkey_slot_key" ON "app"."dkey" ("slot")',
    ]
    (note,) = _notes(dkey, "DEFERRABLE key(s)")
    assert note.classification is Classification.MANUAL and note.kind is ConversionNoteKind.LOSS
    message = note.message
    assert ("dkey_pkey: PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED; dkey_code_key: "
            "UNIQUE (code) DEFERRABLE") in message
    assert "Aurora DSQL does not support deferrable unique keys" in message
    assert "the target checks such a key immediately" in message
    assert "or, INITIALLY DEFERRED, at commit" in message
    assert "not created at all" not in message
    # Only INITIALLY IMMEDIATE ones: no commit-time clause.
    immediate = _convert(_dkey(deferrable_keys=[
        DeferrableKeyDef(name="dkey_code_key", definition="UNIQUE (code) DEFERRABLE",
                         index="dkey_code_key")]))["app.dkey"]
    (note,) = _notes(immediate, "DEFERRABLE key(s)")
    assert "at commit" not in note.message


def test_a_deferrable_unique_key_whose_index_is_not_created_says_so() -> None:
    table = TableDef(
        name="app.docs",
        columns=[_PK, ColumnDef(name="doc", mysql_type="jsonb")],
        primary_key=["id"],
        indexes=[IndexDef(name="docs_doc_key", columns=["doc"], unique=True)],
        deferrable_keys=[DeferrableKeyDef(name="docs_doc_key", definition="UNIQUE (doc) "
                                          "DEFERRABLE", index="docs_doc_key")],
    )
    conversion = _convert(table)["app.docs"]
    assert conversion.index_ddls == []
    (note,) = _notes(conversion, "DEFERRABLE key(s)")
    assert "docs_doc_key is not created at all: see the note on its columns" in note.message


def test_a_mysql_source_never_gets_these_notes() -> None:
    table = _dkey()
    table.name = "shop.dkey"
    result = _convert(table, source_type=SourceType.MYSQL)["shop.dkey"]
    assert not _notes(result, "DEFERRABLE") and not _notes(result, "NOT ENFORCED")


# ---------------------------------------------------------------------------
# 3. Evaluation
# ---------------------------------------------------------------------------


def _item(*tables: TableDef, name: str):  # noqa: ANN202
    report = CompatibilityAssessor(source_type=_PG).assess(SourceInventory(tables=list(tables)))
    return next(i for i in report.items if i.object_name == name)


def test_fk_preserved_names_what_is_kept() -> None:
    item = _item(_parent(), _child(_FK_ALL, _FK_PLAIN, _FK_NE), name="app.child")
    concerns = {c.rule_id: c for c in item.concerns}
    preserved = concerns["FK_PRESERVED"]
    assert preserved.note_kind is ConversionNoteKind.RECOMMENDATION
    assert "Foreign key constraints (fk_all, fk_plain) convert cleanly" in preserved.risk
    assert ("Their deferral and matching are kept as the source declares them: fk_all (MATCH "
            "FULL, DEFERRABLE INITIALLY DEFERRED).") in preserved.risk
    assert "No action needed" in preserved.recommendation


def test_a_not_enforced_key_is_advice_and_says_what_the_conversion_says() -> None:
    item = _item(_parent(), _child(_FK_NE), name="app.child")
    # Advice only: the table converts automatically (the target, like the source, does
    # not check the key) and no effort is counted.
    assert item.classification is Classification.AUTO and item.effort is None
    (concern,) = [c for c in item.concerns if c.rule_id == "PG_FK_NOT_ENFORCED"]
    assert concern.note_kind is ConversionNoteKind.RECOMMENDATION
    assert concern.risk == _notes(_convert(_parent(), _child(_FK_NE))["app.child"],
                                  "NOT ENFORCED")[0].message
    assert "No action needed" in concern.recommendation
    assert "ADD CONSTRAINT ... FOREIGN KEY ... NOT VALID" in concern.recommendation
    assert not [c for c in item.concerns if c.rule_id == "FK_PRESERVED"]


def test_a_deferrable_key_is_a_manual_gap_in_the_conversions_words() -> None:
    item = _item(_dkey(), name="app.dkey")
    assert item.classification is Classification.MANUAL
    (concern,) = [c for c in item.concerns if c.rule_id == "PG_DEFERRABLE_KEY"]
    assert concern.classification is Classification.MANUAL
    assert concern.note_kind is ConversionNoteKind.LOSS and concern.effort is EffortLevel.SIMPLE
    assert concern.risk == _notes(_convert(_dkey())["app.dkey"], "DEFERRABLE key(s)")[0].message
    assert "swaps or renumbers key values" in concern.recommendation


def test_many_deferrable_keys_are_listed_with_a_bound() -> None:
    keys = [
        DeferrableKeyDef(name=f"u{n}", definition=f"UNIQUE (c{n}) DEFERRABLE") for n in range(8)
    ]
    (note,) = _notes(_convert(_dkey(deferrable_keys=keys))["app.dkey"], "DEFERRABLE key(s)")
    assert "u5: UNIQUE (c5) DEFERRABLE" in note.message and "u6: UNIQUE" not in note.message
    assert "and 2 more (u6, u7)" in note.message


# ---------------------------------------------------------------------------
# 4. Orphans: MATCH FULL counts a partly NULL key; a NOT ENFORCED key is not checked
# ---------------------------------------------------------------------------


def test_a_match_full_key_counts_a_partly_null_key_as_an_orphan() -> None:
    """On PostgreSQL 17.11 these statements counted 1 (SIMPLE) and 3 (FULL) orphans over
    rows (1,1) with a parent, (1,NULL), (NULL,NULL), (2,2) and (NULL,7) -- the same 3 rows
    its VALIDATE CONSTRAINT of the MATCH FULL key refused ("MATCH FULL does not allow mixing
    of null and nonnull key values")."""
    simple = _fk("fk_ab", ("a", "b"), ref=("a", "b"))
    full = simple.model_copy(update={"match_full": True})
    assert 'WHERE c."a" IS NOT NULL AND c."b" IS NOT NULL AND NOT EXISTS' in (
        build_orphan_count_sql("app.child", simple).as_string(None)
    )
    count = build_orphan_count_sql("app.child", full).as_string(None)
    assert 'WHERE (c."a" IS NOT NULL OR c."b" IS NOT NULL) AND NOT EXISTS' in count
    page = build_pg_orphan_page_first_sql("app.child", full, "id", 100).as_string(None)
    assert 'WHERE (pg."a" IS NOT NULL OR pg."b" IS NOT NULL) AND p."a" IS NULL' in page
    composite = build_pg_composite_orphan_page_sql(
        "app.child", full, ["id", "pid"], 100, first=True
    ).as_string(None)
    assert '(pg."a" IS NOT NULL OR pg."b" IS NOT NULL)' in composite
    listing = build_orphan_listing_sql("app.child", full, ["id"]).as_string(None)
    assert 'WHERE (c."a" IS NOT NULL OR c."b" IS NOT NULL) AND p."a" IS NULL' in listing
    # One column: the two rules are one.
    single = _fk("fk_one", match_full=True)
    assert 'WHERE c."pid" IS NOT NULL AND NOT EXISTS' in (
        build_orphan_count_sql("app.child", single).as_string(None)
    )


def test_validation_does_not_check_a_not_enforced_key() -> None:
    from tests.test_validator import (
        _SOURCE_CONFIG,
        _TARGET_CONFIG,
        _FakeSourceConnection,
        _FakeTargetConnection,
        _table,
        _validator,
    )

    def orders(*keys: ForeignKeyDef) -> TableDef:
        return _table("orders", columns=("id", "customer_id"), primary_key=("id",),
                      foreign_keys=list(keys))

    enforced = ForeignKeyDef(name="fk_orders_customer", columns=["customer_id"],
                             referenced_table="customers", referenced_columns=["id"])
    loose = enforced.model_copy(update={"name": "fk_orders_ne", "enforced": False})
    source = _FakeSourceConnection(counts={"orders": 4})
    target = _FakeTargetConnection(counts={"orders": 4}, orphans={"orders": 2})
    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [orders(enforced, loose)], check_orphans=True
    )
    assert [f.foreign_key for f in report.orphan_findings] == ["fk_orders_customer"]
    # Only a NOT ENFORCED key: no orphan query at all, and a clean report.
    source = _FakeSourceConnection(counts={"orders": 4})
    target = _FakeTargetConnection(counts={"orders": 4}, orphans={"orders": 2})
    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [orders(loose)], check_orphans=True
    )
    assert report.orphan_findings == [] and report.is_match is True
    assert all("NOT EXISTS" not in text and "LEFT JOIN" not in text for text in target.executed)


def test_a_column_a_not_enforced_key_references_can_be_excluded() -> None:
    """Without an applied conversion the exclusion rule reads the source's keys: one the
    conversion never renders (NOT ENFORCED) holds no referenced column."""
    from dsql_migrator.core.column_exclusion import build_exclusion_context

    inventory = SourceInventory(tables=[_parent(), _child(_fk("fk_ab_ne", ("a", "b"),
                                                              ref=("a", "b"), enforced=False),
                                                          _FK_PLAIN)])
    context = build_exclusion_context(inventory)
    assert context.fk_referenced == {"app.parent": {"id": ("app.child.fk_plain",)}}


# ---------------------------------------------------------------------------
# 5. Every apply path runs the same statements
# ---------------------------------------------------------------------------


def _fake_pass(monkeypatch) -> tuple[list, list]:  # noqa: ANN001
    import dsql_migrator.ui.data_migration._full_load_engine as engine

    added: list = []
    validated: list = []
    monkeypatch.setattr(engine, "_wait_for_referenced_unique_index",
                        lambda *a, **k: ("valid", 0.0))
    monkeypatch.setattr(engine, "apply_foreign_key",
                        lambda ddl, *, connection_factory: added.append(ddl))
    monkeypatch.setattr(engine, "validate_foreign_key",
                        lambda table, name, *, connection_factory: validated.append(name) or "j")
    monkeypatch.setattr(engine, "log_activity", lambda *a, **k: None)
    monkeypatch.setattr("dsql_migrator.core.target_introspector.foreign_key_validation_states",
                        lambda *a, **k: {})
    return added, validated


_EXPECTED_CHILD_DDLS = [
    f'{_HEAD}"fk_all" {_TO_AB} MATCH FULL ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED '
    "NOT VALID",
    f'{_HEAD}"fk_deferrable" {_TO_ID} ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE NOT VALID',
    f'{_HEAD}"fk_plain" {_TO_ID} NOT VALID',
]


def test_the_post_load_pass_adds_the_keys_with_their_clauses(monkeypatch) -> None:  # noqa: ANN001
    import dsql_migrator.ui.data_migration._full_load_engine as engine

    added, validated = _fake_pass(monkeypatch)
    conversions = _convert(_parent(), _child(_FK_ALL, _FK_DEFERRABLE, _FK_PLAIN, _FK_NE))
    result = engine.apply_preserved_foreign_keys(conversions, lambda: object())
    assert result == (3, 0, 0)
    assert added == _EXPECTED_CHILD_DDLS
    assert validated == ["fk_all", "fk_deferrable", "fk_plain"]


def test_the_cut_over_pass_adds_the_keys_with_their_clauses(monkeypatch) -> None:  # noqa: ANN001
    from dsql_migrator.ui.validation import ValidationState, _run_cutover_foreign_keys

    added, _validated = _fake_pass(monkeypatch)
    monkeypatch.setattr("dsql_migrator.core.target_connection.DsqlConnector",
                        lambda *a, **k: SimpleNamespace(connect=lambda: object()))
    inventory = SourceInventory(tables=[_parent(), _child(_FK_ALL, _FK_DEFERRABLE, _FK_PLAIN,
                                                          _FK_NE)])
    session = SimpleNamespace(
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws",
                                             region="us-east-1"),
        aws_profile=None,
        source_config=SourceConnectionConfig(host="h", database="d", source_type=_PG),
    )
    eval_store = SimpleNamespace(get_or_create=lambda _sid: SimpleNamespace(
        result=SimpleNamespace(inventory=inventory)))
    conv_store = SimpleNamespace(get=lambda _sid: SimpleNamespace(
        edited_target_ddls={}, preserve_foreign_keys=True))
    state = ValidationState()
    _run_cutover_foreign_keys(session, state, eval_store=eval_store, conversion_store=conv_store,
                              session_id="s", job_manager=None, refresh=lambda: None)
    assert state.cutover_fk_apply == (3, 0, 0)
    assert added == _EXPECTED_CHILD_DDLS


def test_the_reload_pre_drop_and_the_name_pairing_read_the_new_statements() -> None:
    """A Drop & reload pre-drops a key whose parent is replaced and the pass re-adds it from
    the same statements; the pass, Cut over and the orphan count pair each by its name."""
    from dsql_migrator.ui.data_migration._fk_validation import (
        foreign_key_defs,
        pending_foreign_keys,
    )
    from dsql_migrator.ui.data_migration._full_load_engine import (
        foreign_keys_blocking_replace,
        preserved_foreign_key_names,
    )

    conversions = _convert(_parent(), _child(_FK_ALL, _FK_DEFERRABLE, _FK_NE))
    assert foreign_keys_blocking_replace(conversions, ["app.parent"]) == [
        ("app.child", "fk_all"), ("app.child", "fk_deferrable")
    ]
    assert pending_foreign_keys(conversions) == [
        ("app.child", "fk_all"), ("app.child", "fk_deferrable")
    ]
    assert preserved_foreign_key_names(conversions) == {"app.child": ["fk_all", "fk_deferrable"]}
    defs = foreign_key_defs(conversions)
    assert defs[("app.child", "fk_all")] == _FK_ALL  # the orphan count's MATCH FULL
    assert ("app.child", "fk_ne") not in defs


def test_an_edited_script_keeps_the_clauses_it_was_generated_with() -> None:
    from dsql_migrator.core.converter import SchemaConvertOptions
    from dsql_migrator.ui.schema_conversion import applied_table_conversions, render_target_ddl

    result = SchemaConverter(source_type=_PG).convert(
        SourceInventory(tables=[_parent(), _child(_FK_ALL, _FK_PLAIN)]), SchemaConvertOptions()
    )
    child = next(t for t in result.tables if t.table == "app.child")
    script = render_target_ddl(child).replace('"pid" bigint', '"pid" bigint /* edited */')
    applied = applied_table_conversions(result, {"app.child": script})
    assert applied["app.child"].foreign_key_ddls == child.foreign_key_ddls


# ---------------------------------------------------------------------------
# 6. A persisted inventory / session carries the new fields, and old ones load
# ---------------------------------------------------------------------------


def test_the_new_fields_stay_out_of_the_payload_while_default() -> None:
    dumped = _child(_FK_PLAIN).model_dump()
    assert set(dumped["foreign_keys"][0]) == {
        "name", "columns", "referenced_table", "referenced_columns", "on_delete", "on_update"
    }
    assert "deferrable_keys" not in dumped
    # A payload of an earlier build loads; a populated one round-trips.
    assert TableDef.model_validate(dumped) == _child(_FK_PLAIN)
    populated = _child(_FK_ALL, _FK_NE).model_copy(
        update={"deferrable_keys": _dkey().deferrable_keys}
    )
    payload = json.loads(populated.model_dump_json())
    assert payload["foreign_keys"][0]["match_full"] is True
    assert payload["foreign_keys"][1]["enforced"] is False
    assert TableDef.model_validate(payload) == populated


def test_a_saved_session_round_trips_them(tmp_path) -> None:  # noqa: ANN001
    from dsql_migrator.core.session_state_store import SessionSnapshot, SqliteSessionStateStore

    inventory = SourceInventory(tables=[_parent(), _child(_FK_ALL, _FK_NE), _dkey()])
    store = SqliteSessionStateStore(str(tmp_path / "sessions.sqlite"))
    store.save(SessionSnapshot(session_id="s1", inventory=inventory))
    loaded = SqliteSessionStateStore(str(tmp_path / "sessions.sqlite")).load("s1")
    assert loaded is not None and loaded.inventory == inventory
    # ...and the restored inventory converts to the same statements.
    assert _convert(*loaded.inventory.tables)["app.child"].foreign_key_ddls == [
        _EXPECTED_CHILD_DDLS[0]
    ]


def test_a_session_saved_by_an_earlier_build_loads_with_defaults(tmp_path) -> None:  # noqa: ANN001
    from dsql_migrator.core.session_state_store import SqliteSessionStateStore

    store = SqliteSessionStateStore(str(tmp_path / "sessions.sqlite"))
    payload = json.dumps({
        "session_id": "old",
        "inventory": {"tables": [{
            "name": "app.child", "columns": [{"name": "pid", "mysql_type": "bigint"}],
            "primary_key": ["pid"],
            "foreign_keys": [{"name": "fk_old", "columns": ["pid"],
                              "referenced_table": "app.parent", "referenced_columns": ["id"],
                              "on_delete": None, "on_update": None}],
        }]},
    })
    store._conn.execute(  # noqa: SLF001 - a row an earlier build wrote
        "INSERT INTO sessions (session_id, payload, updated_at) VALUES (?, ?, ?)",
        ("old", payload, "2026-10-01T00:00:00Z"),
    )
    store._conn.commit()  # noqa: SLF001
    loaded = store.load("old")
    assert loaded is not None
    (fk,) = loaded.inventory.tables[0].foreign_keys
    assert (fk.deferrable, fk.initially_deferred, fk.match_full, fk.enforced) == (
        False, False, False, True
    )
    assert loaded.inventory.tables[0].deferrable_keys == []


# ---------------------------------------------------------------------------
# 7. The source DDL panel and the AI source detail
# ---------------------------------------------------------------------------


def test_the_source_ddl_shows_the_clauses_as_the_source_defines_them() -> None:
    child = render_source_table_ddl(_child(_FK_ALL, _FK_DEFERRABLE, _FK_FULL, _FK_NE),
                                    source_type=_PG)
    assert ('CONSTRAINT "fk_all" FOREIGN KEY ("a", "b") REFERENCES "app"."parent" ("a", "b") '
            "MATCH FULL ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED") in child
    assert ('CONSTRAINT "fk_deferrable" FOREIGN KEY ("pid") REFERENCES "app"."parent" ("id") '
            "ON DELETE CASCADE DEFERRABLE,") in child
    # pg_get_constraintdef's order: ON UPDATE before ON DELETE.
    assert "MATCH FULL ON UPDATE SET NULL ON DELETE RESTRICT," in child
    assert ('CONSTRAINT "fk_ne" FOREIGN KEY ("pid") REFERENCES "app"."parent" ("id") '
            "NOT ENFORCED") in child
    # A PostgreSQL 15+ column list keeps its case (only the action's words are keywords).
    columns = render_source_table_ddl(
        _child(_fk("fk_cols", ("a", "b"), ref=("a", "b"), on_delete="SET NULL (a)")),
        source_type=_PG,
    )
    assert 'REFERENCES "app"."parent" ("a", "b") ON DELETE SET NULL (a)' in columns
    dkey = render_source_table_ddl(_dkey(), source_type=_PG)
    assert 'CONSTRAINT "dkey_pkey" PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED' in dkey
    assert '  PRIMARY KEY ("id")' not in dkey
    assert 'CONSTRAINT "dkey_code_key" UNIQUE (code) DEFERRABLE' in dkey
    assert 'INDEX "dkey_code_key"' not in dkey
    assert 'CREATE UNIQUE INDEX "dkey_slot_key"' in dkey


def test_a_key_both_temporal_and_deferrable_is_shown_once() -> None:
    definition = "PRIMARY KEY (id, valid_at WITHOUT OVERLAPS) DEFERRABLE"
    table = TableDef(
        name="app.tp",
        columns=[_PK, ColumnDef(name="valid_at", mysql_type="daterange", nullable=False)],
        primary_key=["id", "valid_at"],
        temporal_keys=[TemporalKeyDef(name="tp_pk", definition=definition, primary=True,
                                      index="tp_pk")],
        deferrable_keys=[DeferrableKeyDef(name="tp_pk", definition=definition, primary=True,
                                          index="tp_pk")],
    )
    rendered = render_source_table_ddl(table, source_type=_PG)
    assert rendered.count(definition) == 1
    conversion = _convert(table)["app.tp"]
    assert _notes(conversion, "WITHOUT OVERLAPS key(s)") and _notes(conversion, "DEFERRABLE key(s)")


def test_the_mysql_source_ddl_is_unchanged() -> None:
    orders = TableDef(
        name="shop.orders",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="cid", mysql_type="int")],
        primary_key=["id"],
        foreign_keys=[ForeignKeyDef(name="fk_o_c", columns=["cid"],
                                    referenced_table="shop.customers",
                                    referenced_columns=["id"], on_delete="CASCADE")],
    )
    assert render_source_table_ddl(orders).endswith(
        "CONSTRAINT `fk_o_c` FOREIGN KEY (`cid`) REFERENCES `shop`.`customers` (`id`)\n)"
    )


def test_the_ai_source_detail_lists_them() -> None:
    from dsql_migrator.ui.ai_tools import build_ai_tool_executor

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

    inventory = SourceInventory(tables=[_child(_FK_ALL, _FK_PLAIN, _FK_NE), _dkey()])
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
    child = json.loads(execute("get_source_object_detail", {"object_name": "app.child"}))
    fks = {fk["name"]: fk for fk in child["foreign_keys"]}
    assert fks["fk_all"]["deferrable"] is fks["fk_all"]["initially_deferred"] is True
    assert fks["fk_all"]["match_full"] is True
    assert fks["fk_ne"]["enforced"] is False
    flags = {"deferrable", "initially_deferred", "match_full", "enforced"}
    assert flags.isdisjoint(fks["fk_plain"])
    dkey = json.loads(execute("get_source_object_detail", {"object_name": "app.dkey"}))
    assert [(k["name"], k["primary"]) for k in dkey["deferrable_keys"]] == [
        ("dkey_pkey", True), ("dkey_code_key", False)
    ]


# ---------------------------------------------------------------------------
# 8. The manual says the same in every language
# ---------------------------------------------------------------------------


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"


def _chapter(language: str, name: str) -> str:
    return " ".join((_MANUAL / language / name).read_text(encoding="utf-8").split())


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_states_what_happens_to_these_keys(language: str) -> None:
    conversion = _chapter(language, "02-evaluation-and-schema-conversion.md")
    evaluation = conversion.split("## 2.1", 1)[1].split("## 2.2", 1)[0]
    for spelled in ("`PG_DEFERRABLE_KEY`", "`PG_FK_NOT_ENFORCED`", "`MATCH FULL`",
                    "`NOT ENFORCED`", "`INITIALLY DEFERRED`"):
        assert spelled in evaluation, (language, spelled)
    schema = conversion.split("## 2.2", 1)[1].split("## 2.3", 1)[0]
    for spelled in ("`MATCH FULL`", "`DEFERRABLE INITIALLY DEFERRED`", "`RESTRICT`",
                    "`NOT ENFORCED`"):
        assert spelled in schema, (language, spelled)
    reference = conversion.split("## 2.3", 1)[1]
    assert "`PG_DEFERRABLE_KEY`" in reference and "`DEFERRABLE`" in reference
    limits = _chapter(language, "06-limitations.md").split("## 6.1", 1)[1].split("## 6.2", 1)[0]
    for spelled in ("`PG_DEFERRABLE_KEY`", "`PG_FK_NOT_ENFORCED`", "`MATCH FULL`",
                    "`NOT ENFORCED`"):
        assert spelled in limits, (language, spelled)
