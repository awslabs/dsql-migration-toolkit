# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G174: following PG_TEMPORAL_KEY's index advice no longer leaves a silent duplicate index.

For a PostgreSQL 18 WITHOUT OVERLAPS primary key the note advises a btree index on the key's
columns on the source (Full Load and Validation read in key order, which the GiST index
cannot return). Once the source has it, Schema Conversion creates it on Aurora DSQL as well
-- ``CREATE INDEX ASYNC ... ("id", "valid")`` beside ``PRIMARY KEY ("id", "valid")`` -- where
the primary key already serves those reads: one index slot and its write cost spent for
nothing, and nothing said. Now the note names such an index and says to remove it from the
target script.
"""

from __future__ import annotations

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import _pg_temporal_key_warning
from dsql_migrator.core.models import (
    ColumnDef,
    IndexDef,
    SourceInventory,
    SourceType,
    TableDef,
    TemporalKeyDef,
)

_REPEATS = "repeats the primary key's columns: Aurora DSQL's primary key already serves"


def _rooms(*indexes: IndexDef) -> TableDef:
    return TableDef(
        name="rev.rooms",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False),
                 ColumnDef(name="valid", mysql_type="tstzrange", nullable=False),
                 ColumnDef(name="name", mysql_type="text")],
        primary_key=["id", "valid"],
        indexes=list(indexes),
        temporal_keys=[TemporalKeyDef(name="rooms_pkey", primary=True, index="rooms_pkey",
                                      definition="PRIMARY KEY (id, valid WITHOUT OVERLAPS)")],
    )


def test_the_advised_index_is_named_as_a_duplicate_on_the_target() -> None:
    message = _pg_temporal_key_warning(
        _rooms(IndexDef(name="rooms_id_valid_btree", columns=["id", "valid"]))
    ).message
    assert f"rooms_id_valid_btree {_REPEATS}" in message
    assert "remove it from the table's target script before applying" in message
    assert "add a btree index" not in message
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(SourceInventory(
        tables=[_rooms(IndexDef(name="rooms_id_valid_btree", columns=["id", "valid"]))]
    ))
    (item,) = report.items
    assert _REPEATS in {c.rule_id: c for c in item.concerns}["PG_TEMPORAL_KEY"].risk


def test_no_such_index_or_a_wider_one_is_no_duplicate() -> None:
    advised = _pg_temporal_key_warning(_rooms()).message
    assert "add a btree index on (id, valid) on the source" in advised and _REPEATS not in advised
    wider = _pg_temporal_key_warning(
        _rooms(IndexDef(name="rooms_id_valid_name", columns=["id", "valid", "name"]))
    ).message
    assert _REPEATS not in wider and "add a btree index" not in wider
    unique = _pg_temporal_key_warning(
        _rooms(IndexDef(name="rooms_id_valid_key", columns=["id", "valid"], unique=True))
    ).message
    assert _REPEATS not in unique
