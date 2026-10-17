# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Evaluation and Schema Conversion must grade the same table the same way -- both ways.

Evaluation is the go/no-go report; Schema Conversion is the next screen, and it grades
every table again from its own notes. When the two disagree the report is wrong in the
worst direction: a table Evaluation called AUTO ("No DSQL compatibility issues", score 100
'Ready') was graded UNSUPPORTED one step later -- a BINARY(16) primary key, which Aurora
DSQL cannot index -- and a UNIQUE index that would silently never exist read AUTO too.

``tests/test_converter.py`` already checks one direction (every Evaluation rule leaves a
conversion note). This drives BOTH engines over one corpus and checks the reverse, which is
the one that produced the false all-clear:

- a conversion note graded UNSUPPORTED  => the Evaluation item is UNSUPPORTED;
- a conversion note that is a LOSS      => the Evaluation item is not AUTO;
- an Evaluation gap                     => the conversion says something.

``_KNOWN_GAPS`` lists the reverse-direction gaps that are known and not yet closed. It is a
ratchet: a NEW gap fails the test, and so does closing one without removing it here.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    ConversionNoteKind,
    IndexDef,
    SourceInventory,
    SourceType,
    TableDef,
)

_PK = ColumnDef(name="id", mysql_type="bigint", nullable=False)
# Two column names that share their first 63 bytes collide once Aurora DSQL truncates them.
_LONG_A, _LONG_B = "c" * 63 + "a", "c" * 63 + "b"


def _t(name: str, columns=(), **kwargs) -> TableDef:
    kwargs.setdefault("primary_key", ["id"])
    return TableDef(name=f"app.{name}", columns=[_PK, *columns], **kwargs)


_MYSQL_CORPUS = [
    _t("clean"),
    TableDef(
        name="app.bytea_pk",
        columns=[ColumnDef(name="id", mysql_type="binary(16)", nullable=False)],
        primary_key=["id"],
    ),
    _t(
        "bytea_unique",
        [ColumnDef(name="sha", mysql_type="varbinary(32)")],
        indexes=[IndexDef(name="ux_bytea_unique_sha", columns=["sha"], unique=True)],
    ),
    _t(
        "bytea_index",
        [ColumnDef(name="b", mysql_type="blob")],
        indexes=[IndexDef(name="ix_bytea_index_b", columns=["b"])],
    ),
    _t(
        "ident_collision",
        [ColumnDef(name=_LONG_A, mysql_type="int"), ColumnDef(name=_LONG_B, mysql_type="int")],
    ),
    _t("ident_long", [ColumnDef(name=_LONG_A, mysql_type="int")]),
    # MySQL scopes index names per table, Aurora DSQL per schema.
    _t(
        "idx_reuse_a",
        [ColumnDef(name="user_id", mysql_type="int")],
        indexes=[IndexDef(name="user_id", columns=["user_id"])],
    ),
    _t(
        "idx_reuse_b",
        [ColumnDef(name="user_id", mysql_type="int")],
        indexes=[IndexDef(name="user_id", columns=["user_id"], unique=True)],
    ),
    _t("enum_col", [ColumnDef(name="s", mysql_type="enum('a','b')")]),
    _t("set_col", [ColumnDef(name="s", mysql_type="set('a','b')")]),
    _t("tinyint_col", [ColumnDef(name="f", mysql_type="tinyint(1)")]),
    _t("bit_col", [ColumnDef(name="f", mysql_type="bit(8)")]),
    _t("year_col", [ColumnDef(name="y", mysql_type="year")]),
    _t("decimal_wide", [ColumnDef(name="n", mysql_type="decimal(1001,30)")]),
    _t("geo_col", [ColumnDef(name="g", mysql_type="geometry")]),
    _t("lob_col", [ColumnDef(name="l", mysql_type="longblob")]),
    _t("json_col", [ColumnDef(name="j", mysql_type="json")]),
    _t(
        "ci_col",
        [ColumnDef(name="e", mysql_type="varchar(20)", collation="utf8mb4_0900_ai_ci")],
    ),
    _t("gen_col", [ColumnDef(name="m", mysql_type="int", generated=True)]),
    _t("onupd_col", [ColumnDef(name="u", mysql_type="datetime", auto_update_timestamp=True)]),
    _t("autoinc", auto_increment_column="id"),
    _t("parted", partitioned=True),
    _t(
        "fulltext",
        [ColumnDef(name="body", mysql_type="text")],
        indexes=[IndexDef(name="ft_fulltext_body", columns=["body"], index_type="FULLTEXT")],
    ),
    TableDef(name="app.nopk", columns=[_PK], primary_key=[]),
    _t("time_col", [ColumnDef(name="d", mysql_type="time")]),
    # A zero-date DEFAULT Aurora DSQL rejects: dropped, and NOT NULL makes it a break.
    _t(
        "zero_default",
        [ColumnDef(name="at", mysql_type="datetime", nullable=False, default="0000-00-00 00:00:00")],
    ),
    _t(
        "expr_idx",
        [ColumnDef(name="e", mysql_type="varchar(50)")],
        indexes=[IndexDef(name="fx_expr_idx", columns=["e"])],
        expression_indexes=["fx_expr_idx"],
    ),
]

_PG_CORPUS = [
    _t("clean"),
    TableDef(
        name="app.bytea_pk",
        columns=[ColumnDef(name="k", mysql_type="bytea", nullable=False)],
        primary_key=["k"],
    ),
    _t(
        "bytea_index",
        [ColumnDef(name="b", mysql_type="bytea")],
        indexes=[IndexDef(name="ix_bytea_index_b", columns=["b"])],
    ),
    _t("bare_numeric", [ColumnDef(name="amount", mysql_type="numeric")]),
    _t("wide_numeric", [ColumnDef(name="n", mysql_type="numeric(1001,2)")]),
    _t("text_array", [ColumnDef(name="tags", mysql_type="text[]", type_kind="array")]),
    _t("enum_col", [ColumnDef(name="s", mysql_type="mood", type_kind="enum")]),
    _t("money_col", [ColumnDef(name="m", mysql_type="money")]),
    _t("varchar_huge", [ColumnDef(name="v", mysql_type="character varying(70000)")]),
    # On an otherwise-clean column, so no other finding hides the index's own verdict.
    _t(
        "gin_idx",
        [ColumnDef(name="n", mysql_type="integer")],
        indexes=[IndexDef(name="ix_gin_idx_n", columns=["n"], method="gin")],
    ),
    _t(
        "partial_idx",
        [ColumnDef(name="e", mysql_type="text")],
        indexes=[
            IndexDef(
                name="ix_partial_idx_e", columns=["e"], unique=True, where="e IS NOT NULL"
            )
        ],
    ),
    _t("coll_col", [ColumnDef(name="n", mysql_type="text", collation="en_US")]),
    TableDef(name="app.nopk", columns=[_PK], primary_key=[]),
    _t("parted", partitioned=True),
    # A PostgreSQL expression index (reflection holds it in expression_indexes only).
    _t(
        "pg_expr_idx",
        [ColumnDef(name="email", mysql_type="text")],
        expression_indexes=["ux_pg_expr_idx_lower_email"],
    ),
    # DEFAULTs calling functions the target lacks (uuid_generate_v4 is translated instead).
    _t("fn_default", [ColumnDef(name="code", mysql_type="text", default="app.next_code()")]),
    _t("v1_default", [ColumnDef(name="u", mysql_type="uuid", default="uuid_generate_v1mc()")]),
    _t("v4_default", [ColumnDef(name="u", mysql_type="uuid", default="uuid_generate_v4()")]),
    # json/jsonb/interval/timetz have no index support on Aurora DSQL.
    _t(
        "jsonb_idx",
        [ColumnDef(name="doc", mysql_type="jsonb")],
        indexes=[IndexDef(name="ix_jsonb_idx_doc", columns=["doc"], method="gin")],
    ),
    TableDef(
        name="app.interval_pk",
        columns=[ColumnDef(name="span", mysql_type="interval", nullable=False)],
        primary_key=["span"],
    ),
    # A text key whose DEFAULT FORMATS a sequence value: no generated key, and the
    # conversion drops the DEFAULT with a LOSS note (it read AUTO at Evaluation).
    TableDef(
        name="app.formatted_key",
        columns=[
            ColumnDef(
                name="order_no", mysql_type="text", nullable=False,
                default="('ORD-'::text || lpad((nextval('app.order_seq'::regclass))::text, 8, '0'::text))",
            )
        ],
        primary_key=["order_no"],
    ),
    # A non-key column formatting a sequence value (NON_KEY_SEQUENCE advised an identity).
    _t(
        "formatted_nonkey",
        [ColumnDef(
            name="invoice_no", mysql_type="text", nullable=False,
            default="('INV-'::text || lpad((nextval('app.inv_seq'::regclass))::text, 8, '0'::text))",
        )],
    ),
    # A partial UNIQUE index whose predicate reads a column the conversion re-types.
    _t(
        "partial_retyped",
        [
            ColumnDef(name="e", mysql_type="text"),
            ColumnDef(name="tags", mysql_type="text[]", type_kind="array"),
        ],
        indexes=[
            IndexDef(
                name="ix_partial_retyped_e", columns=["e"], unique=True,
                where="(tags @> '{vip}'::text[])",
            )
        ],
    ),
]

# Reverse-direction gaps that are known and not yet closed: (engine, table, what the
# conversion reports that Evaluation does not). Close one => delete its line.
_KNOWN_GAPS = {
}


def _grades(engine: SourceType, corpus: list[TableDef]):
    inventory = SourceInventory(tables=corpus)
    report = CompatibilityAssessor(source_type=engine).assess(inventory)
    items = {item.object_name: item for item in report.items}
    conversion = SchemaConverter(source_type=engine).convert(inventory)
    return [(items[table.table], table.warnings) for table in conversion.tables]


@pytest.mark.parametrize(
    ("engine", "corpus"),
    [(SourceType.MYSQL, _MYSQL_CORPUS), (SourceType.POSTGRES, _PG_CORPUS)],
    ids=["mysql", "postgres"],
)
def test_conversion_verdicts_are_never_softer_at_evaluation(engine, corpus) -> None:
    gaps = set()
    for item, notes in _grades(engine, corpus):
        if (
            any(note.classification is Classification.UNSUPPORTED for note in notes)
            and item.classification is not Classification.UNSUPPORTED
        ):
            gaps.add((engine.value, item.object_name, "UNSUPPORTED"))
        elif (
            any(note.kind is ConversionNoteKind.LOSS for note in notes)
            and item.classification is Classification.AUTO
        ):
            gaps.add((engine.value, item.object_name, "LOSS"))
    known = {gap for gap in _KNOWN_GAPS if gap[0] == engine.value}
    assert gaps - known == set(), (
        "Schema Conversion grades these worse than Evaluation does -- the go/no-go "
        f"report would understate them: {sorted(gaps - known)}"
    )
    assert known - gaps == set(), (
        f"These known gaps are closed now; remove them from _KNOWN_GAPS: {sorted(known - gaps)}"
    )


@pytest.mark.parametrize(
    ("engine", "corpus"),
    [(SourceType.MYSQL, _MYSQL_CORPUS), (SourceType.POSTGRES, _PG_CORPUS)],
    ids=["mysql", "postgres"],
)
def test_every_evaluation_gap_is_also_reported_by_schema_conversion(engine, corpus) -> None:
    silent = [
        item.object_name
        for item, notes in _grades(engine, corpus)
        if item.classification is not Classification.AUTO and not notes
    ]
    assert silent == [], f"Evaluation flags these but Schema Conversion says nothing: {silent}"


def test_the_corpus_exercises_the_rules_this_parity_was_added_for() -> None:
    """Guard the fixtures: each case must still trigger the Evaluation rule it stands for."""
    mysql = {item.object_name: item.rule_id for item, _ in _grades(SourceType.MYSQL, _MYSQL_CORPUS)}
    pg = {item.object_name: item.rule_id for item, _ in _grades(SourceType.POSTGRES, _PG_CORPUS)}
    assert mysql["app.bytea_pk"] == "BYTEA_KEY"
    assert mysql["app.bytea_unique"] == "BYTEA_KEY"
    assert mysql["app.ident_collision"] == "IDENTIFIER_LENGTH"
    # Both indexes that share a name are renamed (advice).
    assert mysql["app.idx_reuse_a"] == mysql["app.idx_reuse_b"] == "NAME_COLLISION"
    assert pg["app.bytea_pk"] == "BYTEA_KEY"
    assert pg["app.bare_numeric"] == "PG_UNCONSTRAINED_NUMERIC"
    concerns = {
        item.object_name: {c.rule_id for c in item.concerns or ()}
        for item, _ in _grades(SourceType.POSTGRES, _PG_CORPUS)
    }
    assert "PG_UNINDEXABLE_COLUMN" in concerns["app.jsonb_idx"]
    assert pg["app.pg_expr_idx"] == "EXPRESSION_INDEX"
    assert mysql["app.zero_default"] == "DEFAULT_DROPPED"
    assert pg["app.fn_default"] == pg["app.v1_default"] == "DEFAULT_DROPPED"
    assert "DEFAULT_DROPPED" not in concerns["app.v4_default"]
    assert pg["app.interval_pk"] == "PG_UNINDEXABLE_COLUMN"  # UNSUPPORTED: leads the item
    assert "PG_INDEX_NOT_PRESERVED" in concerns["app.partial_retyped"]
    assert pg["app.formatted_key"] == "DEFAULT_DROPPED"
    assert "DEFAULT_DROPPED" in concerns["app.formatted_nonkey"]
    assert "NON_KEY_SEQUENCE" not in concerns["app.formatted_nonkey"]  # no identity advice
