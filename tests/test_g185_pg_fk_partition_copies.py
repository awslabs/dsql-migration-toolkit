# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G185 (PGFIX-4): a foreign key that references a PostgreSQL partitioned table.

PostgreSQL keeps one derived copy of such a key per partition of the referenced table, on the
referencing table itself (``pg_constraint.conparentid`` <> 0, ``confrelid`` the partition):
``plain_part_id_fkey1`` / ``2`` on 13-17, ``plain_part_id_fkey_1`` / ``_2`` on 18. SQLAlchemy's
``get_foreign_keys`` and G163's catalog read returned each copy as a key of its own, so the
inventory kept a key to every partition, FK_PRESERVED listed them, and Schema Conversion
emitted ``ADD CONSTRAINT ... REFERENCES <partition>`` for partitions that are folded into
their parent and never created on Aurora DSQL.

The catalog rows below are what PostgreSQL 17.11 and 18.6 returned for ``rev.plain`` (a key
into the partitioned ``rev.parts``), ``rev.orders_3`` (a partition of a referencing table,
detached: its own key is kept, conparentid 0, and so are its copies) and ``rev.direct`` (a key
the user declared on a partition itself, conparentid 0) -- throwaway local containers, 13.23
alike. The tests use doubles only.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.introspector import _qualify, _reflect_tables
from dsql_migrator.core.models import SourceInventory, SourceType
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.source_dialect.postgres import (
    pg_foreign_keys_from_catalog,
    pg_foreign_keys_sql,
)

_PG = SourceType.POSTGRES

# (referencing table, constraint, referenced table, conparentid) for schema rev, by naming.
_FK_ROWS = {
    "17": [
        ("plain", "plain_part_id_fkey", "parts", 0),
        ("plain", "plain_part_id_fkey1", "parts_1", 16501),
        ("plain", "plain_part_id_fkey2", "parts_2", 16501),
        ("orders_3", "orders_part_id_fkey", "parts", 0),
        ("orders_3", "orders_3_part_id_fkey", "parts_1", 16620),
        ("orders_3", "orders_3_part_id_fkey1", "parts_2", 16620),
        ("direct", "direct_p1_fkey", "parts_1", 0),
    ],
    "18": [
        ("plain", "plain_part_id_fkey", "parts", 0),
        ("plain", "plain_part_id_fkey_1", "parts_1", 16501),
        ("plain", "plain_part_id_fkey_2", "parts_2", 16501),
        ("orders_3", "orders_part_id_fkey", "parts", 0),
        ("orders_3", "orders_part_id_fkey_5", "parts_1", 16620),
        ("orders_3", "orders_part_id_fkey_6", "parts_2", 16620),
        ("direct", "direct_p1_fkey", "parts_1", 0),
    ],
}
_KEPT = {
    "rev.plain": [("plain_part_id_fkey", "rev.parts")],
    "rev.orders_3": [("orders_part_id_fkey", "rev.parts")],
    "rev.direct": [("direct_p1_fkey", "rev.parts_1")],
}


class _Rows:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self) -> list[dict]:
        return self._rows


class _Catalog:
    """Schema ``rev`` of the source: ``parts`` partitioned into ``parts_1`` / ``parts_2``.

    The schema-wide foreign-key read gets each row's ``derived`` only when it asks for
    ``conparentid``, as the real catalog would; every other read answers nothing."""

    dialect = SimpleNamespace(name="postgresql", server_version_info=(17, 11))

    def __init__(self, naming: str) -> None:
        self._fks = _FK_ROWS[naming]

    def execute(self, statement, params=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        if "c.relispartition AS is_partition" in sql:
            return _Rows([
                {"relname": "parts", "relkind": "p", "is_partition": False},
                {"relname": "parts_1", "relkind": "r", "is_partition": True},
                {"relname": "parts_2", "relkind": "r", "is_partition": True},
                {"relname": "plain", "relkind": "r", "is_partition": False},
                {"relname": "orders_3", "relkind": "r", "is_partition": False},
                {"relname": "direct", "relkind": "r", "is_partition": False},
            ])
        if "c.relname AS child, con.conname AS conname" in sql:
            rows = []
            for child, name, parent, parent_constraint in self._fks:
                row = {"child": child, "conname": name, "ref_schema": "rev", "ref_table": parent}
                if "con.conparentid <> 0 AS derived" in sql:
                    row["derived"] = parent_constraint != 0
                rows.append(row)
            return _Rows(rows)
        return _Rows([])


class _Inspector:
    """SQLAlchemy's Inspector over schema ``rev``: ``get_foreign_keys`` returns every
    constraint of the table, the per-partition copies included, as SQLAlchemy 2.0.50 does."""

    dialect = SimpleNamespace(name="postgresql")

    def __init__(self, naming: str) -> None:
        self._fks = _FK_ROWS[naming]

    def get_table_names(self, schema=None):  # noqa: ANN001, ANN201
        return ["parts", "parts_1", "parts_2", "plain", "orders_3", "direct"]

    def get_columns(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "part_id", "type": "INTEGER", "nullable": True},
        ]

    def get_pk_constraint(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return {"constrained_columns": ["id"]}

    def get_indexes(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return []

    def get_foreign_keys(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return [
            {"name": name, "constrained_columns": ["part_id"], "referred_schema": None,
             "referred_table": parent, "referred_columns": ["id"], "options": {},
             "comment": None}
            for child, name, parent, _parent_constraint in self._fks
            if child == table_name
        ]

    def get_check_constraints(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return []

    def get_table_comment(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return {"text": None}


def _inventory(naming: str) -> SourceInventory:
    """What introspection makes of schema ``rev``: reflect, enrich, qualify."""
    tables = _reflect_tables(_Inspector(naming), schema="rev")
    dialect_for(_PG).enrich(_Catalog(naming), "rev", tables)
    _qualify("rev", tables)
    return SourceInventory(tables=tables)


@pytest.mark.parametrize("naming", ["17", "18"])
def test_introspection_keeps_one_key_per_declared_foreign_key(naming: str) -> None:
    tables = {t.name: t for t in _inventory(naming).tables}
    assert set(tables) == {"rev.parts", "rev.plain", "rev.orders_3", "rev.direct"}
    assert tables["rev.parts"].partitioned is True
    keys = {
        name: [(fk.name, fk.referenced_table) for fk in table.foreign_keys]
        for name, table in tables.items()
        if table.foreign_keys
    }
    # The detached partition's own key and a key the user declared on a partition stay.
    assert keys == _KEPT


@pytest.mark.parametrize("naming", ["17", "18"])
def test_one_add_constraint_and_one_preserved_name_per_key(naming: str) -> None:
    inventory = _inventory(naming)
    converted = {
        t.table: t for t in SchemaConverter(source_type=_PG).convert(inventory).tables
    }
    assert converted["rev.plain"].foreign_key_ddls == [
        'ALTER TABLE "rev"."plain" ADD CONSTRAINT "plain_part_id_fkey" FOREIGN KEY '
        '("part_id") REFERENCES "rev"."parts" ("id") NOT VALID'
    ]
    assert [fk.name for fk in converted["rev.plain"].preserved_foreign_keys] == [
        "plain_part_id_fkey"
    ]
    assert len(converted["rev.orders_3"].foreign_key_ddls) == 1
    report = CompatibilityAssessor(source_type=_PG).assess(inventory)
    (plain,) = [item for item in report.items if item.object_name == "rev.plain"]
    (preserved,) = [c for c in plain.concerns if c.rule_id == "FK_PRESERVED"]
    assert "(plain_part_id_fkey)" in preserved.risk
    assert "parts_1" not in preserved.risk and "fkey1" not in preserved.risk


def test_without_any_foreign_key_the_catalog_is_not_read() -> None:
    class _Unread(_Catalog):
        def execute(self, statement, params=None):  # noqa: ANN001, ANN201
            assert "AS child" not in str(statement), "foreign keys read with none reflected"
            return super().execute(statement, params)

    tables = _reflect_tables(_Inspector("17"), schema="rev", names=["parts"])
    dialect_for(_PG).enrich(_Unread("17"), "rev", tables)
    assert tables[0].foreign_keys == []


# ---------------------------------------------------------------------------
# G163's catalog read (a table SQLAlchemy cannot parse) leaves the copies out too
# ---------------------------------------------------------------------------


class _BookingsCatalog:
    """pg_constraint for rev.bookings on 18.6: a PERIOD key, and a key into rev.parts with its
    copies. Rows with conparentid <> 0 are returned unless the statement filters them."""

    dialect = SimpleNamespace(name="postgresql", server_version_info=(18, 6))

    _ROWS = [
        ("bookings_part_id_fkey", "parts", 0, False),
        ("bookings_part_id_fkey_1", "parts_1", 16701, False),
        ("bookings_part_id_fkey_2", "parts_2", 16701, False),
        ("bookings_room_id_during_fkey", "rooms", 0, True),
    ]

    def execute(self, statement, params=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        rows = [
            {"conname": name, "columns": ["part_id"], "ref_schema": "rev", "ref_table": parent,
             "ref_columns": ["id"], "on_update": "a", "on_delete": "a", "match_type": "s",
             "deferrable": False, "deferred": False, "definition": "", "comment": None,
             "period": period}
            for name, parent, parent_constraint, period in self._ROWS
            if parent_constraint == 0 or "con.conparentid = 0" not in sql
        ]
        return _Rows(rows)


@pytest.mark.parametrize("version", [None, (13, 23), (17, 11), (18, 6)])
def test_the_catalog_read_asks_for_declared_keys_only(version) -> None:  # noqa: ANN001
    assert "con.contype = 'f' AND con.conparentid = 0 AND" in pg_foreign_keys_sql(version)


def test_the_catalog_read_returns_no_copy() -> None:
    keys = pg_foreign_keys_from_catalog(_BookingsCatalog(), "rev", "bookings")
    assert [(k["name"], k["referred_table"], k["period"]) for k in keys] == [
        ("bookings_part_id_fkey", "parts", False),
        ("bookings_room_id_during_fkey", "rooms", True),
    ]
