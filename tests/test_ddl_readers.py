# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The applied-DDL readers the loader and the exclusion rule share: generated columns
(structure, not a substring), a NAMED primary key, PostgreSQL identifier folding, and
the FK statements B3 reads."""

import dataclasses
from types import SimpleNamespace

from dsql_migrator.core.column_exclusion import (
    B2,
    B3,
    build_exclusion_context,
    column_exclusion_verdict,
)
from dsql_migrator.core.converter import (
    SchemaConverter,
    fold_identifier,
    parse_target_column_names,
    parse_target_generated_columns,
    parse_target_primary_key,
)
from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    SourceInventory,
    SourceType,
    TableDef,
)


def _pg_table(*extra):
    return TableDef(
        name="app.posts",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="body", mysql_type="text", default="'Auto-generated'::text"),
            ColumnDef(name="doubled", mysql_type="integer", generated=True,
                      generated_kind="STORED", generated_expression="(id * 2)"),
            *extra,
        ],
        primary_key=["id"],
    )


def test_a_default_or_check_mentioning_generated_is_not_a_generated_column():
    ddl = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(_pg_table()).target_ddl
    assert "DEFAULT CAST('Auto-generated' AS TEXT)" in ddl
    assert parse_target_generated_columns(ddl) == {"doubled"}
    enum = ('CREATE TABLE t (id INT PRIMARY KEY, '
            "s TEXT CHECK (s IN ('generated', 'released')))")
    assert parse_target_generated_columns(enum) == set()
    hand = "CREATE TABLE t (id INT PRIMARY KEY, g INT GENERATED ALWAYS AS (id * 2) STORED)"
    assert parse_target_generated_columns(hand) == {"g"}


def test_every_real_generated_shape_is_still_found():
    # The pg_get_expr shapes the converter's own comments list as breaking sqlglot.
    exprs = ["(tags #>> '{a,b}'::text[])", "((tags -> 'a'::text) ->> 'b'::text)",
             "date_part('year'::text, d)", "((name IS NOT NULL))::integer",
             "CASE WHEN (qty > 0) THEN 'generated'::text ELSE 'as'::text END"]
    cols = [ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="tags", mysql_type="jsonb"), ColumnDef(name="d", mysql_type="date"),
            ColumnDef(name="name", mysql_type="text"), ColumnDef(name="qty", mysql_type="integer")]
    cols += [ColumnDef(name=f"g{i}", mysql_type="text", generated=True, generated_kind="STORED",
                       generated_expression=e) for i, e in enumerate(exprs)]
    ddl = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(
        TableDef(name="app.x", columns=cols, primary_key=["id"])).target_ddl
    assert parse_target_generated_columns(ddl) == {f"g{i}" for i in range(len(exprs))}


def test_the_replace_path_now_loads_a_column_whose_default_says_generated():
    """The data-loss regression: the loader OMITTED 'body' and every recreated row got
    its DEFAULT instead of the source value."""
    from dsql_migrator.ui.data_migration._full_load_engine import BatchedTableMigrator

    from tests.test_ui_data_migration import _FakeExporter, _FakeImporter, _inputs

    table = _pg_table()
    conversion = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    migrator = BatchedTableMigrator(
        dataclasses.replace(_inputs(), table_conversions={table.name: conversion}),
        exporter=_FakeExporter(),  # type: ignore[arg-type]
        importer_factory=lambda _i: _FakeImporter(),  # type: ignore[arg-type,return-value]
    )
    assert migrator.resolve_generated_columns(table, conversion, target_is_final=True) == {"doubled"}


def test_a_named_primary_key_is_read_and_protects_its_columns():
    ddl = 'CREATE TABLE "shop"."orders" ("tenant" TEXT, "id" INT, "note" TEXT, CONSTRAINT "pk_o" PRIMARY KEY ("tenant", "id"))'
    assert parse_target_primary_key(ddl) == ["tenant", "id"]
    orders = TableDef(name="shop.orders", columns=[ColumnDef(name=c, mysql_type="text") for c in ("tenant", "id", "note")],
                      primary_key=["id"])
    ctx = build_exclusion_context(SourceInventory(tables=[orders]),
                                  {"shop.orders": SimpleNamespace(target_ddl=ddl, foreign_key_ddls=[], preserved_foreign_keys=[])})
    assert {f.code for f in column_exclusion_verdict("shop.orders", "tenant", ctx).findings} == {B2}


def test_unquoted_identifiers_fold_ascii_only_like_postgresql():
    assert fold_identifier("ÉTAT_Code") == "État_code"
    assert parse_target_column_names('CREATE TABLE t (ÉTAT TEXT, "Mixed" INT)') == frozenset({"État", "Mixed"})


def _fk_inventory(parent="shop.customers"):
    customers = TableDef(name=parent, columns=[ColumnDef(name=c, mysql_type="text") for c in ("id", "email")],
                         primary_key=["id"])
    orders = TableDef(
        name=parent.rsplit(".", 1)[0] + ".orders",
        columns=[ColumnDef(name=c, mysql_type="text") for c in ("id", "customer_id", "customer_email")],
        primary_key=["id"],
        foreign_keys=[ForeignKeyDef(name="orders_customer_fkey", columns=["customer_id"],
                                    referenced_table=parent, referenced_columns=["id"])])
    return SourceInventory(tables=[customers, orders]), orders


def test_b3_follows_an_edit_that_keeps_the_constraint_name():
    inv, orders = _fk_inventory()
    edited = ('ALTER TABLE "shop"."orders" ADD CONSTRAINT "orders_customer_fkey" FOREIGN KEY '
              '("customer_email") REFERENCES "shop"."customers" ("email") NOT VALID')
    ctx = build_exclusion_context(inv, {orders.name: SimpleNamespace(
        target_ddl="CREATE TABLE x (id INT)", foreign_key_ddls=[edited],
        preserved_foreign_keys=list(orders.foreign_keys))})
    assert {f.code for f in column_exclusion_verdict("shop.customers", "email", ctx).findings} == {B3}
    # The source key column is no longer referenced by what the FK pass runs.
    assert B3 not in {f.code for f in column_exclusion_verdict("shop.customers", "id", ctx).findings}


def test_b3_resolves_an_unqualified_parent_through_the_default_search_path():
    inv, orders = _fk_inventory("public.customers")
    ddl = "ALTER TABLE orders ADD CONSTRAINT fk_email FOREIGN KEY (customer_email) REFERENCES customers (email) NOT VALID"
    conv = {orders.name: SimpleNamespace(target_ddl="CREATE TABLE x (id INT)", foreign_key_ddls=[ddl],
                                         preserved_foreign_keys=[])}
    ctx = build_exclusion_context(inv, conv)
    assert {f.code for f in column_exclusion_verdict("public.customers", "email", ctx).findings} == {B3}
    # A one-part name that is not a migrated public table makes no claim.
    inv2, orders2 = _fk_inventory("shop.customers")
    ctx2 = build_exclusion_context(inv2, {orders2.name: conv[orders.name]})
    assert not column_exclusion_verdict("shop.customers", "email", ctx2).blocked


def test_the_size_probe_stops_when_a_transaction_cannot_be_ended():
    """A failed rollback means the next statement could run on a fresh, uncapped
    connection: stop and leave the rest not-determined."""
    from dsql_migrator.core.models import SourceType as ST

    from tests.test_prerequisite_probes import _SizeProbeConn, _size_probe

    conn = _SizeProbeConn(set())

    def _broken_rollback():
        raise RuntimeError("connection lost")

    conn.rollback = _broken_rollback  # type: ignore[method-assign]
    answer = _size_probe(ST.MYSQL, conn).oversized_values(
        "shop.docs", ["a", "b", "c"], limit_bytes=1024, timeout_seconds=5.0)
    assert answer == {"a": False}
    assert sum("SELECT EXISTS" in s for s in conn.statements) == 1
