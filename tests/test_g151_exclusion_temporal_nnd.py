# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G151: PostgreSQL exclusion constraints, WITHOUT OVERLAPS keys and UNIQUE NULLS NOT DISTINCT.

Aurora DSQL rejects an ``EXCLUDE`` constraint (in CREATE TABLE and in ALTER TABLE ... ADD
CONSTRAINT, live-verified) and has no PostgreSQL 18 ``WITHOUT OVERLAPS``; it accepts
``CREATE UNIQUE INDEX ASYNC ... NULLS NOT DISTINCT`` (live-verified).

* Introspection reads, catalog only, the EXCLUDE constraints of the tables read and -- on
  PostgreSQL 18, where ``pg_constraint.conperiod`` exists -- their WITHOUT OVERLAPS keys,
  one round trip per schema. An exclusion constraint's index, which SQLAlchemy reflects as an
  ordinary non-unique index, leaves ``indexes`` / ``expression_indexes``: it existed only to
  enforce the rule. NULLS NOT DISTINCT comes from SQLAlchemy's reflection, which reads
  ``pg_index.indnullsnotdistinct`` only on 15+.
* Schema Conversion no longer creates that index as a plain btree index (one over a btree
  EXCLUDE graded the table AUTO); the table gets a note. A WITHOUT OVERLAPS key is kept as an
  ordinary key on the same columns, with a note. A NULLS NOT DISTINCT unique index keeps it.
* Evaluation: PG_EXCLUSION_CONSTRAINT and PG_TEMPORAL_KEY, MANUAL, the conversion's notes.
* The manual (en / ko / ja) says so in chapters 2 and 6.

The SQLAlchemy-shaped rows below are what SQLAlchemy 2.0.50 reflected from PostgreSQL 18.6,
17.11 and 13.23 while this was written (the tool's own introspection was run against them
from a container in the database's network namespace); the tests use doubles only.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import (
    SchemaConverter,
    _without_nulls_distinct,
    index_renames,
    parse_target_unique_keys,
)
from dsql_migrator.core.introspector import _assemble_inventory, _reflect_tables
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    ConversionNoteKind,
    EffortLevel,
    ExclusionConstraintDef,
    IndexDef,
    SourceInventory,
    SourceType,
    TableDef,
    TemporalKeyDef,
)
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.source_dialect.postgres import (
    _pg_read_exclusion_rules,
    pg_exclusion_rules_sql,
)
from dsql_migrator.ui.schema_conversion import render_source_table_ddl

_PG = SourceType.POSTGRES


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------


class _UndefinedColumn(Exception):
    """What PostgreSQL 13-17 raise for a statement naming ``conperiod`` (42703)."""

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


class _PgCatalog:
    """A PostgreSQL connection of one server version over a pg_constraint catalog.

    Faithful where it matters: a statement naming ``conperiod`` fails on a server before 18
    (the column does not exist there), a WITHOUT OVERLAPS key ('p' / 'u') comes back only
    when the statement asks for ``conperiod``, and the ``:nsp`` / ``:names`` binds filter
    the rows. Every other catalog read answers nothing (``schemas`` for the schema list).
    """

    class _Dialect:
        name = "postgresql"

        def __init__(self, version) -> None:  # noqa: ANN001
            self.server_version_info = version

    def __init__(self, constraints: list[dict], version, schemas=("app",)) -> None:  # noqa: ANN001
        self.dialect = self._Dialect(version)
        self._constraints = constraints
        self._schemas = list(schemas)
        self.statements: list[tuple[str, dict]] = []

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        params = dict(parameters or {})
        self.statements.append((sql, params))
        if "conperiod" in sql and tuple(self.dialect.server_version_info or (0,)) < (18,):
            raise _UndefinedColumn("column con.conperiod does not exist")
        if "pg_get_constraintdef" in sql and "contype = 'x'" in sql:
            wanted = set(params.get("names") or ())
            return _Rows(
                [
                    {k: v for k, v in row.items() if k != "nsp"}
                    for row in self._constraints
                    if row["nsp"] == params.get("nsp")
                    and row["rel"] in wanted
                    and (row["contype"] == "x" or "con.conperiod" in sql)
                ]
            )
        if "NOT LIKE" in sql:
            return _Rows([{"nspname": name} for name in self._schemas])
        return _Rows([])


# What pg_constraint holds for the tables of the PostgreSQL 18.6 check (``pg_get_constraintdef``
# text as it printed it). A 13-17 catalog holds the same EXCLUDE rows and no temporal key.
_CONSTRAINTS = [
    {"nsp": "app", "rel": "booking", "conname": "booking_no_overlap", "contype": "x",
     "definition": "EXCLUDE USING gist (room WITH =, during WITH &&)",
     "index_name": "booking_no_overlap"},
    {"nsp": "app", "rel": "booking_expr", "conname": "booking_expr_excl", "contype": "x",
     "definition": "EXCLUDE USING gist (room WITH =, tstzrange(starts, ends) WITH &&) "
                   "WHERE ((room > 0))",
     "index_name": "booking_expr_excl"},
    {"nsp": "app", "rel": "btree_excl", "conname": "btree_excl_code", "contype": "x",
     "definition": "EXCLUDE USING btree (code WITH =)", "index_name": "btree_excl_code"},
    {"nsp": "app", "rel": "price", "conname": "price_pk", "contype": "p",
     "definition": "PRIMARY KEY (product_id, valid_at WITHOUT OVERLAPS)",
     "index_name": "price_pk"},
    {"nsp": "app", "rel": "assignment", "conname": "assignment_uq", "contype": "u",
     "definition": "UNIQUE (emp_id, valid_at WITHOUT OVERLAPS)", "index_name": "assignment_uq"},
    {"nsp": "other", "rel": "booking", "conname": "elsewhere", "contype": "x",
     "definition": "EXCLUDE USING gist (r WITH &&)", "index_name": "elsewhere"},
]


def _bare_tables() -> list[TableDef]:
    """The tables as reflection leaves them, BEFORE enrichment (bare names, the exclusion
    constraints' indexes still among ``indexes``, the mixed one in ``expression_indexes``)."""
    pk = ColumnDef(name="id", mysql_type="bigint", nullable=False)
    return [
        TableDef(
            name="booking",
            columns=[pk, ColumnDef(name="room", mysql_type="integer", nullable=False),
                     ColumnDef(name="during", mysql_type="tstzrange", nullable=False)],
            primary_key=["id"],
            indexes=[
                IndexDef(name="booking_no_overlap", columns=["room", "during"], method="gist"),
                IndexDef(name="booking_room", columns=["room"]),
            ],
        ),
        TableDef(
            name="booking_expr",
            columns=[pk, ColumnDef(name="room", mysql_type="integer")],
            primary_key=["id"],
            indexes=[IndexDef(name="booking_expr_excl", columns=["room"], method="gist",
                              where="(room > 0)")],
            expression_indexes=["booking_expr_excl"],
        ),
        TableDef(
            name="btree_excl",
            columns=[pk, ColumnDef(name="code", mysql_type="text")],
            primary_key=["id"],
            indexes=[IndexDef(name="btree_excl_code", columns=["code"])],
        ),
        TableDef(
            name="price",
            columns=[ColumnDef(name="product_id", mysql_type="bigint", nullable=False),
                     ColumnDef(name="valid_at", mysql_type="daterange", nullable=False)],
            primary_key=["product_id", "valid_at"],
        ),
        TableDef(
            name="assignment",
            columns=[pk, ColumnDef(name="emp_id", mysql_type="bigint", nullable=False),
                     ColumnDef(name="valid_at", mysql_type="tstzrange", nullable=False)],
            primary_key=["id"],
            indexes=[IndexDef(name="assignment_uq", columns=["emp_id", "valid_at"],
                              unique=True, method="gist")],
        ),
    ]


def _by_name(tables: list[TableDef]) -> dict[str, TableDef]:
    return {table.name.rsplit(".", 1)[-1]: table for table in tables}


# ---------------------------------------------------------------------------
# 1. Introspection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("version", [None, (13, 23), (14, 0), (15, 0), (16, 4), (17, 11)])
def test_the_catalog_read_names_conperiod_only_from_postgresql_18(version) -> None:  # noqa: ANN001
    sql = pg_exclusion_rules_sql(version)
    assert "conperiod" not in sql
    assert "con.contype = 'x'" in sql and "pg_catalog.pg_get_constraintdef(con.oid)" in sql
    assert "n.nspname = :nsp" in sql and "c.relname = ANY(:names)" in sql


@pytest.mark.parametrize("version", [(18, 0), (18, 6), (19, 1)])
def test_the_catalog_read_asks_for_without_overlaps_keys_on_18(version) -> None:  # noqa: ANN001
    sql = pg_exclusion_rules_sql(version)
    assert "con.contype = 'x' OR (con.contype IN ('p', 'u') AND con.conperiod)" in sql


@pytest.mark.parametrize("version", [(13, 23), (14, 0), (15, 0), (17, 11)])
def test_enrich_on_a_catalog_without_conperiod(version) -> None:  # noqa: ANN001
    """PostgreSQL 13-17 have no ``conperiod``: the read never names it (the double raises
    as they do), the exclusion constraints are recorded and no temporal key is."""
    connection = _PgCatalog(_CONSTRAINTS, version)
    tables = _bare_tables()
    dialect_for(_PG).enrich(connection, "app", tables)
    by_name = _by_name(tables)
    assert [x.name for x in by_name["booking"].exclusion_constraints] == ["booking_no_overlap"]
    assert all(not table.temporal_keys for table in tables)
    # The read is ONE statement for the schema, scoped to the tables read.
    reads = [(sql, p) for sql, p in connection.statements if "pg_get_constraintdef" in sql]
    assert len(reads) == 1
    assert reads[0][1]["nsp"] == "app"
    assert sorted(reads[0][1]["names"]) == sorted(t.name for t in tables)


def test_enrich_moves_an_exclusion_constraints_index_out_of_the_indexes() -> None:
    tables = _bare_tables()
    _pg_read_exclusion_rules(_PgCatalog(_CONSTRAINTS, (17, 11)), "app", tables)
    by_name = _by_name(tables)
    booking = by_name["booking"]
    assert booking.exclusion_constraints == [
        ExclusionConstraintDef(
            name="booking_no_overlap",
            definition="EXCLUDE USING gist (room WITH =, during WITH &&)",
            index="booking_no_overlap",
        )
    ]
    # The rule's own index is gone; an ordinary index of the table stays.
    assert [index.name for index in booking.indexes] == ["booking_room"]
    # A mixed column + expression one leaves expression_indexes too (no expression-index
    # note for it: the exclusion note shows its whole definition).
    expr = by_name["booking_expr"]
    assert expr.indexes == [] and expr.expression_indexes == []
    assert "tstzrange(starts, ends) WITH &&" in expr.exclusion_constraints[0].definition
    assert by_name["btree_excl"].indexes == []
    # Another schema's same-named table is not this one.
    assert all(x.name != "elsewhere" for t in tables for x in t.exclusion_constraints)


def test_enrich_records_without_overlaps_keys_on_postgresql_18() -> None:
    tables = _bare_tables()
    _pg_read_exclusion_rules(_PgCatalog(_CONSTRAINTS, (18, 6)), "app", tables)
    by_name = _by_name(tables)
    assert by_name["price"].temporal_keys == [
        TemporalKeyDef(
            name="price_pk",
            definition="PRIMARY KEY (product_id, valid_at WITHOUT OVERLAPS)",
            primary=True,
            index="price_pk",
        )
    ]
    (unique,) = by_name["assignment"].temporal_keys
    assert unique.primary is False and unique.index == "assignment_uq"
    # The UNIQUE key's index stays: it is created as an ordinary UNIQUE index.
    assert [index.name for index in by_name["assignment"].indexes] == ["assignment_uq"]
    assert by_name["price"].primary_key == ["product_id", "valid_at"]


def test_enrich_skips_rows_of_another_shape_and_tables_without_rules() -> None:
    """A row without a constraint kind (another catalog read's answer) records nothing."""

    class _Other(_PgCatalog):
        def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
            if "pg_get_constraintdef" in str(statement):
                return _Rows([{"child": "booking", "conname": "fk", "ref_schema": "app"},
                              {"rel": "booking", "conname": "x1", "contype": "c"}])
            return super().execute(statement, parameters)

    tables = _bare_tables()
    _pg_read_exclusion_rules(_Other([], (18, 0)), "app", tables)
    assert all(not t.exclusion_constraints and not t.temporal_keys for t in tables)
    assert [i.name for i in _by_name(tables)["booking"].indexes] == [
        "booking_no_overlap", "booking_room"
    ]
    # No table, no read.
    connection = _PgCatalog(_CONSTRAINTS, (18, 0))
    _pg_read_exclusion_rules(connection, "app", [])
    assert connection.statements == []


class _Inspector:
    """``get_indexes`` etc. as SQLAlchemy 2.0.50 reflected the PostgreSQL 18.6 tables."""

    _INDEXES = {
        "booking": [
            {"name": "booking_no_overlap", "unique": False, "column_names": ["room", "during"],
             "duplicates_constraint": "booking_no_overlap",
             "dialect_options": {"postgresql_using": "gist", "postgresql_include": []}},
        ],
        "contact": [
            {"name": "contact_email_uq", "unique": True, "column_names": ["email"],
             "duplicates_constraint": "contact_email_uq",
             "dialect_options": {"postgresql_include": [],
                                 "postgresql_nulls_not_distinct": True}},
            {"name": "contact_phone_nnd_partial", "unique": True, "column_names": ["phone"],
             "dialect_options": {"postgresql_include": [], "postgresql_where": "(tenant > 0)",
                                 "postgresql_nulls_not_distinct": True}},
            {"name": "contact_tenant_phone_uq", "unique": True,
             "column_names": ["tenant", "phone"], "duplicates_constraint": "contact_tenant_phone_uq",
             "dialect_options": {"postgresql_include": []}},
        ],
        "assignment": [
            {"name": "assignment_uq", "unique": True, "column_names": ["emp_id", "valid_at"],
             "duplicates_constraint": "assignment_uq",
             "dialect_options": {"postgresql_using": "gist", "postgresql_include": []}},
        ],
        "price": [],
    }
    _COLUMNS = {
        "booking": ["id", "room", "during"],
        "contact": ["id", "email", "tenant", "phone"],
        "assignment": ["id", "emp_id", "valid_at"],
        "price": ["product_id", "valid_at"],
    }

    def get_table_names(self, schema=None):  # noqa: ANN001, ANN201
        return list(self._INDEXES) if schema == "app" else []

    def get_view_names(self, schema=None):  # noqa: ANN001, ANN201
        return []

    def get_columns(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return [{"name": c, "type": "INTEGER", "nullable": True} for c in self._COLUMNS[table_name]]

    def get_pk_constraint(self, table_name, schema=None):  # noqa: ANN001, ANN201
        key = ["product_id", "valid_at"] if table_name == "price" else ["id"]
        return {"constrained_columns": key}

    def get_indexes(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return self._INDEXES[table_name]

    def get_foreign_keys(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return []

    def get_check_constraints(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return []

    def get_table_comment(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return {"text": None}


def test_reflection_carries_nulls_not_distinct() -> None:
    contact = _by_name(_reflect_tables(_Inspector(), schema="app"))["contact"]
    flags = {index.name: index.nulls_not_distinct for index in contact.indexes}
    # A UNIQUE NULLS NOT DISTINCT constraint's index and a plain one alike; SQLAlchemy sets
    # the key only when pg_index.indnullsnotdistinct is true (and never before 15).
    assert flags == {
        "contact_email_uq": True,
        "contact_phone_nnd_partial": True,
        "contact_tenant_phone_uq": False,
    }


@pytest.mark.parametrize(("major", "reads_the_column"), [(13, False), (14, False), (15, True),
                                                          (17, True), (18, True)])
def test_sqlalchemy_reads_indnullsnotdistinct_only_from_15(major, reads_the_column) -> None:  # noqa: ANN001
    """The flag's source: SQLAlchemy's index query names pg_index.indnullsnotdistinct only
    for a 15+ server, and reports false on 13 and 14, which have no such column."""
    from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg

    dialect = PGDialect_psycopg()
    dialect.server_version_info = (major, 0)
    sql = str(dialect._index_query.compile(dialect=dialect))
    assert ("pg_catalog.pg_index.indnullsnotdistinct" in sql) is reads_the_column
    assert "indnullsnotdistinct" in sql  # the label is always there


@pytest.mark.parametrize("version", [(17, 11), (18, 6)])
def test_the_whole_catalog_read_end_to_end(version) -> None:  # noqa: ANN001
    connection = _PgCatalog(_CONSTRAINTS, version)
    inventory = _assemble_inventory(_Inspector(), connection, "postgres", dialect=dialect_for(_PG))
    tables = {table.name: table for table in inventory.tables}
    booking = tables["app.booking"]
    assert booking.indexes == [] and booking.exclusion_constraints[0].name == "booking_no_overlap"
    contact = tables["app.contact"]
    assert [i.nulls_not_distinct for i in contact.indexes] == [True, True, False]
    assert bool(tables["app.price"].temporal_keys) is (version >= (18,))
    assert bool(tables["app.assignment"].temporal_keys) is (version >= (18,))


# ---------------------------------------------------------------------------
# 2. Schema Conversion
# ---------------------------------------------------------------------------


def _enriched(version=(18, 6)) -> dict[str, TableDef]:  # noqa: ANN001
    tables = _bare_tables()
    _pg_read_exclusion_rules(_PgCatalog(_CONSTRAINTS, version), "app", tables)
    for table in tables:
        table.name = f"app.{table.name}"
    return _by_name(tables)


def _convert(*tables: TableDef):  # noqa: ANN202
    result = SchemaConverter(source_type=_PG).convert(SourceInventory(tables=list(tables)))
    return {conversion.table.rsplit(".", 1)[-1]: conversion for conversion in result.tables}


def _notes(conversion, needle: str):  # noqa: ANN001, ANN202
    return [w for w in conversion.warnings if needle in w.message]


def test_an_exclusion_constraints_index_is_not_created() -> None:
    tables = _enriched()
    converted = _convert(tables["booking"], tables["booking_expr"], tables["btree_excl"])
    booking = converted["booking"]
    assert booking.index_ddls == [
        'CREATE INDEX ASYNC "booking_room" ON "app"."booking" ("room")'
    ]
    (note,) = _notes(booking, "Exclusion constraint")
    assert note.classification is Classification.MANUAL and note.kind is ConversionNoteKind.LOSS
    assert "booking_no_overlap: EXCLUDE USING gist (room WITH =, during WITH &&)" in note.message
    assert "Aurora DSQL does not support EXCLUDE constraints" in note.message
    assert "is not created either" in note.message
    # Not described as a btree stand-in or an expression index any more.
    assert not _notes(booking, "btree-only") and not _notes(converted["booking_expr"], "Expression index")
    # The btree EXCLUDE (code WITH =), a uniqueness rule, emitted a plain index and no note.
    btree = converted["btree_excl"]
    assert btree.index_ddls == []
    assert _notes(btree, "btree_excl_code: EXCLUDE USING btree (code WITH =)")


def test_nothing_else_counts_an_exclusion_constraints_index() -> None:
    """The index budget and the index renames counted it while it was among ``indexes``."""
    columns = [ColumnDef(name="id", mysql_type="bigint", nullable=False)] + [
        ColumnDef(name=f"c{n}", mysql_type="integer") for n in range(23)
    ]
    wide = TableDef(
        name="wide",
        columns=[*columns, ColumnDef(name="span", mysql_type="int4range")],
        primary_key=["id"],
        # Named like another relation of the schema, which forced a rename; with 23
        # ordinary indexes beside it, one of those was left out over the 23-index budget.
        indexes=[IndexDef(name="slot", columns=["span"], method="gist")]
        + [IndexDef(name=f"ix_{n}", columns=[f"c{n}"]) for n in range(23)],
    )
    rules = [{"nsp": "app", "rel": "wide", "conname": "slot", "contype": "x",
              "definition": "EXCLUDE USING gist (span WITH &&)", "index_name": "slot"}]
    before = wide.model_copy(deep=True)
    _pg_read_exclusion_rules(_PgCatalog(rules, (17, 11)), "app", [wide])
    for table in (wide, before):
        table.name = "app.wide"
    other = TableDef(name="app.slot", columns=[columns[0]], primary_key=["id"])

    def _graded(inventory: SourceInventory) -> set[str]:
        report = CompatibilityAssessor(source_type=_PG).assess(inventory)
        item = next(i for i in report.items if i.object_name == "app.wide")
        return {c.rule_id for c in item.concerns}

    # As reflected (without G151's read): renamed, and an ordinary index left out.
    old_inventory = SourceInventory(tables=[before, other])
    assert index_renames(old_inventory, is_postgres=True) == {"app.wide": {"slot": "wide_slot"}}
    old = _convert(before, other)["wide"]
    assert len(old.index_ddls) == 23 and any('"wide_slot"' in ddl for ddl in old.index_ddls)
    assert {"TOO_MANY_INDEXES", "NAME_COLLISION"} <= _graded(old_inventory)
    # With it: no rename, every ordinary index created, nothing over the budget.
    inventory = SourceInventory(tables=[wide, other])
    assert index_renames(inventory, is_postgres=True) == {}
    conversion = _convert(wide, other)["wide"]
    assert conversion.index_ddls == [
        f'CREATE INDEX ASYNC "ix_{n}" ON "app"."wide" ("c{n}")' for n in range(23)
    ]
    rule_ids = _graded(inventory)
    assert "TOO_MANY_INDEXES" not in rule_ids and "NAME_COLLISION" not in rule_ids
    assert "PG_EXCLUSION_CONSTRAINT" in rule_ids


def test_a_without_overlaps_primary_key_is_kept_as_an_ordinary_key() -> None:
    price = _convert(_enriched()["price"])["price"]
    assert 'PRIMARY KEY ("product_id", "valid_at")' in price.target_ddl
    (note,) = _notes(price, "WITHOUT OVERLAPS")
    assert note.classification is Classification.MANUAL and note.kind is ConversionNoteKind.LOSS
    message = note.message
    assert "price_pk: PRIMARY KEY (product_id, valid_at WITHOUT OVERLAPS)" in message
    assert "accepts two rows with the same product_id whose valid_at periods overlap" in message
    assert "the key without it is not unique" in message
    # The read cost of a GiST primary key, and its fix.
    assert "add a btree index on (product_id, valid_at) on the source before the load" in message


def _temporal_price(*indexes: IndexDef, expression_indexes=()) -> TableDef:  # noqa: ANN001
    return TableDef(
        name="app.price",
        columns=[ColumnDef(name="product_id", mysql_type="bigint", nullable=False),
                 ColumnDef(name="valid_at", mysql_type="daterange", nullable=False),
                 ColumnDef(name="note", mysql_type="text")],
        primary_key=["product_id", "valid_at"],
        indexes=list(indexes),
        expression_indexes=list(expression_indexes),
        temporal_keys=[TemporalKeyDef(
            name="price_pk", definition="PRIMARY KEY (product_id, valid_at WITHOUT OVERLAPS)",
            primary=True, index="price_pk",
        )],
    )


@pytest.mark.parametrize(
    ("index", "advised"),
    [
        # The index the advice asks for, or one it leads: the keyset reads use it (an index
        # scan on PostgreSQL 18.6), so asking for it again after a re-run would mislead.
        (IndexDef(name="price_btree", columns=["product_id", "valid_at"]), False),
        (IndexDef(name="price_btree", columns=["product_id", "valid_at"], method="btree"), False),
        (IndexDef(name="price_btree", columns=["product_id", "valid_at", "note"]), False),
        # None of these returns every page in key order.
        (IndexDef(name="price_btree", columns=["valid_at", "product_id"]), True),
        (IndexDef(name="price_btree", columns=["product_id"]), True),
        (IndexDef(name="price_btree", columns=["product_id", "valid_at"], where="(note <> '')"),
         True),
        (IndexDef(name="price_gist", columns=["product_id", "valid_at"], method="gist"), True),
    ],
)
def test_the_read_cost_advice_is_left_out_once_the_source_has_the_btree_index(
    index: IndexDef, advised: bool
) -> None:
    (note,) = _notes(_convert(_temporal_price(index))["price"], "WITHOUT OVERLAPS")
    assert ("add a btree index on (product_id, valid_at)" in note.message) is advised
    # The no-overlap loss is reported either way.
    overlap = "accepts two rows with the same product_id whose valid_at periods overlap"
    assert overlap in note.message


def test_an_expression_index_does_not_count_as_the_btree_index() -> None:
    table = _temporal_price(
        IndexDef(name="price_expr", columns=["product_id", "valid_at"]),
        expression_indexes=["price_expr"],
    )
    (note,) = _notes(_convert(table)["price"], "WITHOUT OVERLAPS")
    assert "add a btree index on (product_id, valid_at)" in note.message


def test_a_without_overlaps_unique_key_is_an_ordinary_unique_index() -> None:
    assignment = _convert(_enriched()["assignment"])["assignment"]
    assert assignment.index_ddls == [
        'CREATE UNIQUE INDEX ASYNC "assignment_uq" ON "app"."assignment" ("emp_id", "valid_at")'
    ]
    (note,) = _notes(assignment, "WITHOUT OVERLAPS")
    assert "assignment_uq: UNIQUE (emp_id, valid_at WITHOUT OVERLAPS)" in note.message
    assert "same emp_id whose valid_at periods overlap" in note.message
    # Its GiST method is the key's need, not a search: no btree-only note for it, and no
    # primary-key read-cost sentence for a UNIQUE key.
    assert not _notes(assignment, "btree-only")
    assert "btree index on" not in note.message


def test_a_without_overlaps_unique_key_that_is_not_created_says_so() -> None:
    table = TableDef(
        name="app.slots",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False),
                 ColumnDef(name="k", mysql_type="bigint"),
                 # A user-defined range type has no stand-in: no index over it is created.
                 ColumnDef(name="span", mysql_type="app.floatrange", type_kind="range")],
        primary_key=["id"],
        indexes=[IndexDef(name="slots_uq", columns=["k", "span"], unique=True, method="gist")],
        temporal_keys=[TemporalKeyDef(name="slots_uq", definition="UNIQUE (k, span WITHOUT OVERLAPS)",
                                      index="slots_uq")],
    )
    conversion = _convert(table)["slots"]
    assert conversion.index_ddls == []
    (note,) = _notes(conversion, "WITHOUT OVERLAPS")
    assert "slots_uq is not created at all" in note.message


def test_nulls_not_distinct_is_kept() -> None:
    table = TableDef(
        name="app.contact",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False),
                 ColumnDef(name="email", mysql_type="text"),
                 ColumnDef(name="tenant", mysql_type="integer"),
                 ColumnDef(name="phone", mysql_type="text")],
        primary_key=["id"],
        indexes=[
            IndexDef(name="contact_email_uq", columns=["email"], unique=True,
                     nulls_not_distinct=True),
            IndexDef(name="contact_phone_nnd_partial", columns=["phone"], unique=True,
                     nulls_not_distinct=True, where="(tenant > 0)"),
            IndexDef(name="contact_tenant_phone_uq", columns=["tenant", "phone"], unique=True),
            # Never on a non-unique index (PostgreSQL has none; a stray flag is not emitted).
            IndexDef(name="contact_phone", columns=["phone"], nulls_not_distinct=True),
        ],
    )
    conversion = _convert(table)["contact"]
    assert conversion.index_ddls == [
        'CREATE UNIQUE INDEX ASYNC "contact_email_uq" ON "app"."contact" ("email") '
        "NULLS NOT DISTINCT",
        # Before WHERE, as PostgreSQL's grammar has it.
        'CREATE UNIQUE INDEX ASYNC "contact_phone_nnd_partial" ON "app"."contact" ("phone") '
        "NULLS NOT DISTINCT WHERE (tenant > 0)",
        'CREATE UNIQUE INDEX ASYNC "contact_tenant_phone_uq" ON "app"."contact" '
        '("tenant", "phone")',
        'CREATE INDEX ASYNC "contact_phone" ON "app"."contact" ("phone")',
    ]
    # Read back as unique keys: sqlglot reads such a statement only as a Command, so the
    # reader-shard guard refused to split the table ("its unique keys could not be read").
    assert parse_target_unique_keys(conversion.target_ddl, conversion.index_ddls) == [
        ["id"], ["email"], ["phone"], ["tenant", "phone"]
    ]
    report = CompatibilityAssessor(source_type=_PG).assess(SourceInventory(tables=[table]))
    assert report.items[0].classification is Classification.AUTO


def test_the_nulls_distinct_clause_is_matched_after_the_key_list_only() -> None:
    assert _without_nulls_distinct(
        'CREATE UNIQUE INDEX "i" ON "t" ("a") NULLS NOT DISTINCT WHERE note <> \'x) NULLS DISTINCT\''
    ) == 'CREATE UNIQUE INDEX "i" ON "t" ("a") WHERE note <> \'x) NULLS DISTINCT\''
    assert _without_nulls_distinct('CREATE UNIQUE INDEX "i" ON "t" ("a") nulls distinct') == (
        'CREATE UNIQUE INDEX "i" ON "t" ("a")'
    )
    assert parse_target_unique_keys(
        'CREATE TABLE "t" ("id" bigint, "a" text, PRIMARY KEY ("id"))',
        ['CREATE UNIQUE INDEX ASYNC "i" ON "t" ("a") NULLS NOT DISTINCT'],
    ) == [["id"], ["a"]]


def test_a_mysql_source_is_untouched() -> None:
    table = TableDef(
        name="shop.t",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False),
                 ColumnDef(name="e", mysql_type="varchar(20)")],
        primary_key=["id"],
        indexes=[IndexDef(name="ux_e", columns=["e"], unique=True)],
    )
    result = SchemaConverter(source_type=SourceType.MYSQL).convert(SourceInventory(tables=[table]))
    assert result.tables[0].index_ddls == ['CREATE UNIQUE INDEX ASYNC "ux_e" ON "shop"."t" ("e")']


# ---------------------------------------------------------------------------
# 3. Evaluation
# ---------------------------------------------------------------------------


def _concern(table: TableDef, rule_id: str):  # noqa: ANN202
    report = CompatibilityAssessor(source_type=_PG).assess(SourceInventory(tables=[table]))
    item = next(i for i in report.items if i.object_name == table.name)
    return item, next(c for c in item.concerns if c.rule_id == rule_id)


def test_evaluation_flags_each_exclusion_constraint() -> None:
    table = _enriched()["btree_excl"]
    item, concern = _concern(table, "PG_EXCLUSION_CONSTRAINT")
    # Was AUTO: the btree index looked like an ordinary one.
    assert item.classification is Classification.MANUAL
    assert concern.classification is Classification.MANUAL
    assert concern.note_kind is ConversionNoteKind.LOSS and concern.effort is EffortLevel.MEDIUM
    assert "btree_excl_code: EXCLUDE USING btree (code WITH =)" in concern.risk
    # The conversion's own words, so the two steps cannot describe it differently.
    conversion = _convert(table)["btree_excl"]
    assert concern.risk == _notes(conversion, "Exclusion constraint")[0].message
    assert "40001" in concern.recommendation and "application" in concern.recommendation


def test_evaluation_flags_each_without_overlaps_key() -> None:
    tables = _enriched()
    for name in ("price", "assignment"):
        item, concern = _concern(tables[name], "PG_TEMPORAL_KEY")
        assert item.classification is Classification.MANUAL
        assert concern.note_kind is ConversionNoteKind.LOSS and concern.effort is EffortLevel.MEDIUM
        assert "WITHOUT OVERLAPS)" in concern.risk
        assert "no-overlap rule in the application" in concern.recommendation


def test_many_rules_are_listed_with_a_bound() -> None:
    table = TableDef(
        name="app.many",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False)],
        primary_key=["id"],
        exclusion_constraints=[
            ExclusionConstraintDef(name=f"x{n}", definition=f"EXCLUDE (c{n} WITH =)")
            for n in range(9)
        ],
    )
    (note,) = _notes(_convert(table)["many"], "Exclusion constraint")
    assert "x5: EXCLUDE (c5 WITH =)" in note.message and "x6: EXCLUDE" not in note.message
    assert "and 3 more (x6, x7, x8)" in note.message


# ---------------------------------------------------------------------------
# 4. The source DDL panel shows the source's own clauses
# ---------------------------------------------------------------------------


def test_the_source_ddl_shows_the_rules_as_the_source_defines_them() -> None:
    tables = _enriched()
    booking = render_source_table_ddl(tables["booking"], source_type=_PG)
    assert 'CONSTRAINT "booking_no_overlap" EXCLUDE USING gist (room WITH =, during WITH &&)' in booking
    assert 'CREATE INDEX "booking_no_overlap"' not in booking
    price = render_source_table_ddl(tables["price"], source_type=_PG)
    assert 'CONSTRAINT "price_pk" PRIMARY KEY (product_id, valid_at WITHOUT OVERLAPS)' in price
    assert '  PRIMARY KEY ("product_id", "valid_at")' not in price
    assignment = render_source_table_ddl(tables["assignment"], source_type=_PG)
    assert 'CONSTRAINT "assignment_uq" UNIQUE (emp_id, valid_at WITHOUT OVERLAPS)' in assignment
    assert 'INDEX "assignment_uq"' not in assignment
    contact = TableDef(
        name="app.contact",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False),
                 ColumnDef(name="email", mysql_type="text")],
        primary_key=["id"],
        indexes=[IndexDef(name="ux", columns=["email"], unique=True, nulls_not_distinct=True,
                          where="(email <> '')")],
    )
    assert ('CREATE UNIQUE INDEX "ux" ON "app"."contact" ("email") NULLS NOT DISTINCT '
            "WHERE (email <> '')") in render_source_table_ddl(contact, source_type=_PG)


def test_the_ai_source_detail_lists_the_rules() -> None:
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

    tables = _enriched()
    inventory = SourceInventory(tables=[tables["booking"], tables["price"]])
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
    booking = json.loads(execute("get_source_object_detail", {"object_name": "app.booking"}))
    assert booking["exclusion_constraints"] == [
        {"name": "booking_no_overlap",
         "definition": "EXCLUDE USING gist (room WITH =, during WITH &&)"}
    ]
    price = json.loads(execute("get_source_object_detail", {"object_name": "app.price"}))
    assert price["temporal_keys"][0]["primary"] is True


# ---------------------------------------------------------------------------
# 5. The payload loads on either side of the change
# ---------------------------------------------------------------------------


def test_the_new_fields_stay_out_of_the_payload_while_empty() -> None:
    table = TableDef(
        name="app.t",
        columns=[ColumnDef(name="id", mysql_type="bigint")],
        primary_key=["id"],
        indexes=[IndexDef(name="ux", columns=["id"], unique=True)],
    )
    dumped = table.model_dump()
    assert "exclusion_constraints" not in dumped and "temporal_keys" not in dumped
    assert "nulls_not_distinct" not in dumped["indexes"][0]
    # A payload of an earlier build loads, and a populated one round-trips.
    assert TableDef.model_validate(dumped) == table
    populated = _enriched()["price"].model_copy(
        update={"indexes": [IndexDef(name="ux", columns=["product_id"], unique=True,
                                     nulls_not_distinct=True)]}
    )
    assert TableDef.model_validate_json(populated.model_dump_json()) == populated


# ---------------------------------------------------------------------------
# 6. The manual says the same in every language
# ---------------------------------------------------------------------------


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"


def _chapter(language: str, name: str) -> str:
    return " ".join((_MANUAL / language / name).read_text(encoding="utf-8").split())


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_states_what_happens_to_these_rules(language: str) -> None:
    conversion = _chapter(language, "02-evaluation-and-schema-conversion.md")
    classes = conversion.split("## 2.1", 1)[1].split("## 2.2", 1)[0]
    assert "`PG_EXCLUSION_CONSTRAINT`" in classes and "`PG_TEMPORAL_KEY`" in classes
    reference = conversion.split("## 2.3", 1)[1]
    for spelled in ("`EXCLUDE`", "`WITHOUT OVERLAPS`", "`NULLS NOT DISTINCT`",
                    "`PG_EXCLUSION_CONSTRAINT`", "`PG_TEMPORAL_KEY`"):
        assert spelled in reference, (language, spelled)
    limits = _chapter(language, "06-limitations.md").split("## 6.1", 1)[1].split("## 6.2", 1)[0]
    for spelled in ("`EXCLUDE`", "`WITHOUT OVERLAPS`", "`PG_EXCLUSION_CONSTRAINT`",
                    "`PG_TEMPORAL_KEY`", "btree"):
        assert spelled in limits, (language, spelled)
