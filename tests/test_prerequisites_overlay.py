# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P2-11: TARGET_COLUMNS_LOADABLE with the exclusion overlay (overlay_nullable)."""

from dsql_migrator.core.models import ColumnDef, PrerequisiteStatus, TableDef, apply_lob_exclusions
from dsql_migrator.core.prerequisites import check_target_columns_loadable

TABLE = TableDef(name="shop.docs", primary_key=["id"], columns=[
    ColumnDef(name="id", mysql_type="int", nullable=False), ColumnDef(name="body", mysql_type="text")])


def test_an_overlay_not_null_column_without_a_default_is_loadable():
    effective = apply_lob_exclusions(TABLE, {"body"})
    result = check_target_columns_loadable(effective, ["id", "body"], overlay_nullable={"body"})
    assert result.status is PrerequisiteStatus.PASS
    assert "`body` will be made nullable before the load" in result.detail


def test_the_same_column_as_a_legacy_entry_still_fails():
    effective = apply_lob_exclusions(TABLE, {"body"})
    result = check_target_columns_loadable(effective, ["id", "body"])  # legacy: not overlaid
    assert result.status is PrerequisiteStatus.FAIL and "`body`" in result.detail


def test_a_target_only_not_null_column_still_fails():
    result = check_target_columns_loadable(TABLE, ["id", "extra"], overlay_nullable={"body"})
    assert result.status is PrerequisiteStatus.FAIL and "`extra`" in result.detail
