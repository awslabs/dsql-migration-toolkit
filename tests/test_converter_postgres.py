# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL-source DDL reconstruction (``converter_postgres.build_pg_source_ddl``)."""

import pytest
import sqlglot

from dsql_migrator.core.converter_postgres import build_pg_source_ddl
from dsql_migrator.core.models import ColumnDef, TableDef


def _col(name: str, typ: str, nullable: bool = True) -> ColumnDef:
    return ColumnDef(name=name, mysql_type=typ, nullable=nullable)


def test_renders_exact_pg_types_and_parses_as_postgres() -> None:
    table = TableDef(
        name="public.orders",
        columns=[
            _col("id", "bigint", nullable=False),
            _col("total", "numeric(12,2)"),
            _col("tags", "text[]"),
            _col("created_at", "timestamp with time zone"),
            _col("meta", "jsonb"),
            _col("blob", "bytea"),
            _col("uid", "uuid"),
        ],
        primary_key=["id"],
    )
    ddl = build_pg_source_ddl(table)
    assert '"public"."orders"' in ddl
    assert '"tags" text[]' in ddl
    assert '"created_at" timestamp with time zone' in ddl
    assert 'PRIMARY KEY ("id")' in ddl
    # Must parse cleanly as PostgreSQL (the converter reads it with read="postgres").
    assert sqlglot.parse_one(ddl, read="postgres") is not None


def test_composite_pk_and_not_null() -> None:
    table = TableDef(
        name="order_items",
        columns=[
            _col("order_id", "bigint", nullable=False),
            _col("line_no", "integer", nullable=False),
        ],
        primary_key=["order_id", "line_no"],
    )
    ddl = build_pg_source_ddl(table)
    assert 'PRIMARY KEY ("order_id", "line_no")' in ddl
    assert '"order_id" bigint NOT NULL' in ddl
    assert sqlglot.parse_one(ddl, read="postgres") is not None


def test_rejects_table_with_no_columns() -> None:
    with pytest.raises(ValueError):
        build_pg_source_ddl(TableDef(name="t", columns=[], primary_key=[]))


def test_double_quotes_are_escaped_and_reparse() -> None:
    table = TableDef(
        name='we"ird', columns=[_col('c"ol', "integer")], primary_key=[]
    )
    ddl = build_pg_source_ddl(table)
    assert '"we""ird"' in ddl and '"c""ol"' in ddl
    assert sqlglot.parse_one(ddl, read="postgres") is not None
