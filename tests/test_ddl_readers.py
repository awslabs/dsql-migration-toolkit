# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The applied-DDL readers the loader, the exclusion rule and the overlay share: generated
columns (structure, not a substring), identity columns, a NAMED primary key, PostgreSQL
identifier folding, the FK statements B3 reads, the NOT NULL / DEFAULT reader, and the
frozen set_digest."""

import dataclasses
import hashlib
import os
import subprocess
import sys
from types import SimpleNamespace

from dsql_migrator.core.column_exclusion import (
    B2,
    B3,
    B4,
    build_exclusion_context,
    column_exclusion_verdict,
    set_digest,
)
from dsql_migrator.core.converter import (
    PrimaryKeyStrategy,
    SchemaConverter,
    SchemaConvertOptions,
    fold_identifier,
    parse_target_column_constraints,
    parse_target_column_names,
    parse_target_generated_columns,
    parse_target_identity_columns,
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
    # Carried verbatim (pg_get_expr text) -- the reader must still see a DEFAULT, not the
    # word "generated" inside it.
    assert "DEFAULT 'Auto-generated'::text" in ddl
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
    # The loader / CDC re-key keep the 0.1.561 reading (a named key is "unknown" there, so a
    # deployed pipeline's MessageKeyColumns cannot change); the exclusion rule reads it.
    assert parse_target_primary_key(ddl) == []
    assert parse_target_primary_key(ddl, for_exclusion=True) == ["tenant", "id"]
    unquoted = 'CREATE TABLE t (id BIGINT, CONSTRAINT PK_ORDERS PRIMARY KEY (ID))'
    assert parse_target_primary_key(unquoted, for_exclusion=True) == ["id"]  # folded
    assert parse_target_primary_key('CREATE TABLE t (ID INT PRIMARY KEY)') == ["ID"]  # as 0.1.561
    orders = TableDef(name="shop.orders", columns=[ColumnDef(name=c, mysql_type="text") for c in ("tenant", "id", "note")],
                      primary_key=["id"])
    ctx = build_exclusion_context(SourceInventory(tables=[orders]),
                                  {"shop.orders": SimpleNamespace(target_ddl=ddl, foreign_key_ddls=[], preserved_foreign_keys=[])})
    assert {f.code for f in column_exclusion_verdict("shop.orders", "tenant", ctx).findings} == {B2}


def test_identity_columns_are_read_through_the_cache_option():
    ddl = ('CREATE TABLE t (id BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 65536) PRIMARY KEY, '
           '"Seq" BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 5 INCREMENT BY 2) NOT NULL, '
           'SeqB BIGINT generated by default as identity, '
           'g INT GENERATED ALWAYS AS (id + 1) STORED, h INT GENERATED ALWAYS AS (id + 2), '
           "x TEXT DEFAULT 'GENERATED')")
    assert parse_target_identity_columns(ddl) == frozenset({"id", "Seq", "seqb"})
    assert parse_target_identity_columns("garbage(") == frozenset()
    assert parse_target_identity_columns("SELECT 1") == frozenset()
    # The converter's own emission (a cached identity key) is read too.
    table = TableDef(name="shop.t", columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                     primary_key=["id"], auto_increment_column="id")
    emitted = SchemaConverter().convert_table(
        table, SchemaConvertOptions(primary_key_strategy=PrimaryKeyStrategy.IDENTITY_WITH_CACHE)
    ).target_ddl
    assert "AS IDENTITY (CACHE" in emitted
    assert parse_target_identity_columns(emitted) == frozenset({"id"})


def test_a_non_key_identity_column_is_b4_and_a_key_identity_is_b2_only():
    ddl = ('CREATE TABLE "shop"."t" ("id" BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 65536) '
           'PRIMARY KEY, "seq" BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 65536), "note" TEXT)')
    table = TableDef(name="shop.t", columns=[ColumnDef(name=c, mysql_type="text") for c in ("id", "seq", "note")],
                     primary_key=["id"])
    ctx = build_exclusion_context(
        SourceInventory(tables=[table]),
        {"shop.t": SimpleNamespace(target_ddl=ddl, foreign_key_ddls=[], preserved_foreign_keys=[])})
    assert {f.code for f in column_exclusion_verdict("shop.t", "seq", ctx).findings} == {B4}
    assert B4 not in {f.code for f in column_exclusion_verdict("shop.t", "id", ctx).findings}
    assert not column_exclusion_verdict("shop.t", "note", ctx).blocked


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


def test_set_digest_is_frozen_order_free_and_process_stable():
    assert set_digest([]) == hashlib.sha256(b"v1|").hexdigest()
    assert set_digest(["b", "a", "a"]) == set_digest(["a", "b"]) != set_digest(["a"])
    code = "from dsql_migrator.core.column_exclusion import set_digest; print(set_digest(['b','a']))"
    for seed in ("0", "123"):
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                             env={**os.environ, "PYTHONHASHSEED": seed}).stdout.strip()
        assert out == set_digest(["a", "b"]), seed


def test_constraint_reader_on_converter_output_and_edits():
    t = TableDef(name="app.items", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="integer", nullable=False, identity=True),
        ColumnDef(name="Status", mysql_type="varchar(16)", nullable=False, default="'active'::character varying"),
        ColumnDef(name="body", mysql_type="text", default="'Auto-generated'::text"),
        ColumnDef(name="doubled", mysql_type="integer", generated=True, generated_kind="STORED",
                  generated_expression="(id * 2)")])
    ddl = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(t).target_ddl
    c = parse_target_column_constraints(ddl)
    assert c["id"][0] is True and c["id"][1] is None           # identity PK (CACHE stripped)
    assert c["Status"][0] is True and c["Status"][1] is not None
    assert c["body"] == (False, "CAST('Auto-generated' AS TEXT)")  # a DEFAULT, not generated
    assert c["doubled"] == (False, None)                        # generated: no DEFAULT
    edited = "CREATE TABLE t (UserName TEXT DEFAULT 'x', a INT NULL)"
    assert parse_target_column_constraints(edited) == {"username": (False, "'x'"), "a": (False, None)}
    assert parse_target_column_constraints("garbage(") == {} == parse_target_column_constraints("SELECT 1")
    first = parse_target_column_constraints(edited)
    first["username"] = (True, "tampered")
    assert parse_target_column_constraints(edited)["username"] == (False, "'x'")  # cache safe


def test_set_digest_non_empty_encoding_is_pinned_to_literals():
    # Literals, not a recomputation: every stored ledger record carries this digest.
    assert set_digest(["b", "a", "a"]) == "300474fd65f391c1b1ea757d8ba66a7ccaebb95125eff1d0a4d196af846a7840"
    assert set_digest(["État", "a"]) == "c86d4162b05c32d8f317711d6514ba9435eedd6f366c9accba41b9663559cc26"
    assert set_digest(["A", "a"]) == "3caa6cc493bb105d90ea775a90402ac55a03730d7262c22d09815d27a194a926"
    assert set_digest(["A"]) != set_digest(["a"]) and set_digest(["a,b"]) != set_digest(["a", "b"])


def test_identity_read_survives_a_clause_followed_directly_by_a_token():
    for tail in ("(CACHE 1)NOT NULL", "(CACHE 65536)PRIMARY KEY", "NOT NULL", "PRIMARY KEY"):
        ddl = (f'CREATE TABLE "shop"."t" ("x" BIGINT GENERATED BY DEFAULT AS IDENTITY {tail}, '
               '"seq2" BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 1), "note" TEXT)')
        assert parse_target_identity_columns(ddl) == frozenset({"x", "seq2"}), tail
    ddl = ('CREATE TABLE "shop"."t" ("id" BIGINT PRIMARY KEY, '
           '"seq" BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)NOT NULL, '
           '"seq2" BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 1), "note" TEXT)')
    table = TableDef(name="shop.t", columns=[ColumnDef(name=c, mysql_type="text") for c in ("id", "seq", "seq2", "note")],
                     primary_key=["id"])
    ctx = build_exclusion_context(
        SourceInventory(tables=[table]),
        {"shop.t": SimpleNamespace(target_ddl=ddl, foreign_key_ddls=[], preserved_foreign_keys=[])})
    for column in ("seq", "seq2"):
        assert {f.code for f in column_exclusion_verdict("shop.t", column, ctx).findings} == {B4}, column


def test_an_unquoted_generated_column_is_b4_under_its_stored_name():
    ddl = 'CREATE TABLE "app"."t" ("id" INT NOT NULL, "a" INT, "b" INT, Total INT GENERATED ALWAYS AS (a + b) STORED, PRIMARY KEY ("id"))'
    assert parse_target_generated_columns(ddl) == {"Total"}  # the loader's reading is unchanged
    assert parse_target_generated_columns(ddl, folded=True) == {"total"}
    table = TableDef(name="app.t", columns=[ColumnDef(name=c, mysql_type="integer") for c in ("id", "a", "b", "total")],
                     primary_key=["id"])
    ctx = build_exclusion_context(
        SourceInventory(tables=[table]),
        {"app.t": SimpleNamespace(target_ddl=ddl, foreign_key_ddls=[], preserved_foreign_keys=[])})
    assert {f.code for f in column_exclusion_verdict("app.t", "total", ctx).findings} == {B4}


def test_the_identity_strip_never_rewrites_a_string_literal():
    def default(text):
        return parse_target_column_constraints(f"CREATE TABLE t (id INT PRIMARY KEY, b8 VARCHAR(40) DEFAULT {text})")["b8"][1]

    assert default("'GENERATED ALWAYS AS IDENTITY (x)'") != default("''")
    assert default("'a GENERATED ALWAYS AS IDENTITY b'") != default("'a  b'")
    converted = SchemaConverter().convert_table(TableDef(
        name="shop.t", primary_key=["id"],
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="b8", mysql_type="varchar(40)", default="GENERATED ALWAYS AS IDENTITY (x)")])).target_ddl
    assert "IDENTITY (x)" in parse_target_column_constraints(converted)["b8"][1]
    # The clause itself is still removed outside quotes, so the rest keeps parsing.
    assert parse_target_primary_key(
        'CREATE TABLE t ("id" BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 65536) PRIMARY KEY, '
        "\"n\" TEXT DEFAULT 'GENERATED ALWAYS AS IDENTITY')") == ["id"]
