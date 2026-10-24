# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G171 (K28, PG-9): the PostgreSQL types the bundled CDC connector has no converter for.

The shipped Debezium 2.7.4 PostgreSQL connector knows built-in types by OID and extension
types by six names; every other type is java.sql.Types.OTHER (a composite STRUCT, an array
ARRAY) to it, and such a column is left out of every change event -- inserts land NULL,
updates leave it stale, and only a CHECKSUM sees it. Evaluation and Step 3 graded only some
of them. This was established by building the connector's OWN change-event schemas
(PostgresSchema.refresh through its PostgresConnection and TypeRegistry, with cdc-stack.yaml's
value settings) from a live PostgreSQL 17.11 catalog (throwaway local container): 66 columns
were absent from the value schema -- pg_lsn, jsonpath, xid, xid8, cid, tid, aclitem, the
eleven reg* types, txid_snapshot, pg_snapshot, the six built-in multiranges, a composite, a
range and a multirange of the user's, a domain over a composite, and an array of each -- while
xml, interval, money, point, name, oid, the six built-in ranges, an enum and a domain over int
were present. Step 3's probe flagged 6 of the 66 before and all 66 now; Evaluation 5 before and
62 now (the 4 left are arrays of a user's composite / range / multirange and a domain over a
composite, whose element or base kind the inventory does not record; Step 3 sees them).
PG-9: a domain over an extension's type (earth over cube) is now judged by its base type at
Evaluation, as the probe already did.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef
from dsql_migrator.core.prerequisites_postgres import (
    CDC_UNCARRYABLE_CANDIDATE_ARRAY_ELEMENTS,
    CDC_UNCARRYABLE_CANDIDATE_SCALARS,
    UNCARRYABLE_COLUMN_ARRIVES_NULL,
    cdc_drops_extension_type,
    classify_uncarryable_cdc_column,
    uncarryable_columns_of,
)
from dsql_migrator.core.source_dialect.postgres import _PG_CDC_UNCARRYABLE_COLUMNS_SQL

# Left out of Debezium's value schema, scalar and array (the live run above).
_NO_CONVERTER = (
    "pg_lsn", "jsonpath", "xid", "xid8", "cid", "tid", "aclitem",
    "regclass", "regtype", "regproc", "regprocedure", "regoper", "regoperator", "regconfig",
    "regdictionary", "regnamespace", "regrole", "regcollation", "txid_snapshot", "pg_snapshot",
    "int4multirange", "int8multirange", "nummultirange", "datemultirange", "tsmultirange",
    "tstzmultirange",
)


@pytest.mark.parametrize("name", _NO_CONVERTER)
def test_a_type_with_no_converter_arrives_null_scalar_or_array(name: str) -> None:
    for is_array in (False, True):
        assert classify_uncarryable_cdc_column(
            is_array=is_array, base_type_name=name, element_typmod=-1
        ) == UNCARRYABLE_COLUMN_ARRIVES_NULL, (name, is_array)
    # ... and the probe asks the catalog for it (it filters on these lists server-side).
    assert name in CDC_UNCARRYABLE_CANDIDATE_SCALARS
    assert name in CDC_UNCARRYABLE_CANDIDATE_ARRAY_ELEMENTS


@pytest.mark.parametrize("name", ["xml", "interval", "money", "point", "name", "oid", "uuid",
                                  "jsonb", "int4range", "daterange", "inet", "macaddr8"])
def test_the_carried_scalars_stay_unflagged(name: str) -> None:
    assert classify_uncarryable_cdc_column(
        is_array=False, base_type_name=name, element_typmod=-1
    ) is None


def test_the_probe_asks_for_the_users_composite_range_and_multirange_types() -> None:
    sql = " ".join(_PG_CDC_UNCARRYABLE_COLUMNS_SQL.split())
    assert "OR (COALESCE(ebt.typtype, bt.typtype) IN ('b', 'c', 'r', 'm') " in sql
    # Such a row is graded like an extension's type the connector does not know.
    assert classify_uncarryable_cdc_column(
        is_array=False, base_type_name="pair", element_typmod=-1, extension=True
    ) == UNCARRYABLE_COLUMN_ARRIVES_NULL


def _table(*columns: ColumnDef) -> TableDef:
    return TableDef(name="app.t", columns=[ColumnDef(name="id", mysql_type="integer",
                                                     nullable=False), *columns],
                    primary_key=["id"])


def test_evaluation_names_them_by_name_and_by_kind() -> None:
    table = _table(
        ColumnDef(name="lsn", mysql_type="pg_lsn"),
        ColumnDef(name="rc_arr", mysql_type="regclass[]"),
        ColumnDef(name="x8", mysql_type="xid8"),
        ColumnDef(name="mr", mysql_type="int4multirange", type_kind="multirange"),
        ColumnDef(name="comp", mysql_type="app.pair", type_kind="composite"),
        ColumnDef(name="ur", mysql_type="floatrange", type_kind="range"),
        ColumnDef(name="fmr", mysql_type="floatmultirange", type_kind="multirange"),
        # Carried: a built-in range, an enum, a domain over a carried type, xml.
        ColumnDef(name="i4r", mysql_type="int4range", type_kind="range"),
        ColumnDef(name="dr", mysql_type="pg_catalog.daterange", type_kind="range"),
        ColumnDef(name="md", mysql_type="mood", type_kind="enum", enum_labels=("a",)),
        ColumnDef(name="idom", mysql_type="int_dom", type_kind="domain", base_type="integer"),
        ColumnDef(name="xm", mysql_type="xml"),
    )
    found = {c.column: c for c in uncarryable_columns_of(table)}
    assert sorted(found) == ["comp", "fmr", "lsn", "mr", "rc_arr", "ur", "x8"]
    assert all(c.reason == UNCARRYABLE_COLUMN_ARRIVES_NULL and c.in_event is False
               for c in found.values())
    assert found["comp"].declared_type == "app.pair"


def test_a_domain_is_judged_by_its_base_type_at_evaluation() -> None:
    """PG-9: earth (earthdistance's domain over cube), a domain over cube[] and one over
    tsvector are left out of the change event, as the probe has long said."""
    earth = ColumnDef(name="ea", mysql_type="earth", type_kind="domain", base_type="cube")
    cubes = ColumnDef(name="ca", mysql_type="app.cube_arr", type_kind="domain",
                      base_type="cube[]")
    tsv = ColumnDef(name="tv", mysql_type="app.doc", type_kind="domain", base_type="tsvector")
    text = ColumnDef(name="tx", mysql_type="app.label", type_kind="domain", base_type="text")
    assert cdc_drops_extension_type(earth) and cdc_drops_extension_type(cubes)
    assert not cdc_drops_extension_type(tsv) and not cdc_drops_extension_type(text)
    found = {c.column: c for c in uncarryable_columns_of(_table(earth, cubes, tsv, text))}
    assert sorted(found) == ["ca", "ea", "tv"]
    assert found["ea"].declared_type == "earth"  # the column's own spelling, as the probe's


def test_the_evaluation_finding_lists_them() -> None:
    from dsql_migrator.core.assessor import CompatibilityAssessor

    table = _table(ColumnDef(name="lsn", mysql_type="pg_lsn"),
                   ColumnDef(name="ea", mysql_type="earth", type_kind="domain", base_type="cube"))
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[table])
    )
    (item,) = report.items
    concern = {c.rule_id: c for c in item.concerns}["CDC_UNCARRYABLE_COLUMN"]
    assert "`lsn` (pg_lsn), `ea` (earth) are dropped from the change event" in concern.risk
