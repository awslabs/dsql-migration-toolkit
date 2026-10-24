# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G160: a Full Load process worker derives a table's DDL in the source's time zone.

A table with no applied conversion (the library / harness path) is re-derived by Full Load
in the zone Evaluation read (G149). A process worker gets slimmed inputs, and the slimming
replaced the inventory with an empty one -- the server settings with it -- so a worker's
retry recreate wrote UTC clock defaults where the parent's pre-pass had written the source
zone's. The settings are kept now; the tables are still dropped.

Doubles only: no database, no AWS.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.models import (
    ColumnDef,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    TableDef,
    TargetConnectionConfig,
)
from dsql_migrator.ui.data_migration import DataMigrationInputs
from dsql_migrator.ui.data_migration._full_load_engine import (
    _derived_table_conversion,
    _slim_worker_inputs,
)

_ORDERS = TableDef(
    name="app.orders",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="created", mysql_type="datetime", default="CURRENT_TIMESTAMP", default_is_expression=True),
        ColumnDef(name="day", mysql_type="date", default="curdate()", default_is_expression=True),
    ],
    primary_key=["id"],
)
_OTHER = TableDef(name="app.other", columns=[ColumnDef(name="id", mysql_type="int", nullable=False)], primary_key=["id"])


def _inputs(settings: dict) -> DataMigrationInputs:
    return DataMigrationInputs(
        source_config=SourceConnectionConfig(host="db", database="app"),
        source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="cluster.dsql.example", region="us-east-1"),
        inventory=SourceInventory(tables=[_ORDERS, _OTHER], server_settings=settings),
    )


@pytest.mark.parametrize(("zone", "written"), [("Asia/Seoul", "AT TIME ZONE 'Asia/Seoul'"), ("+09:00", "INTERVAL '+09:00'")])
def test_a_workers_derived_ddl_names_the_source_zone_as_the_parents_does(zone: str, written: str) -> None:
    inputs = _inputs({"time_zone": zone, "sql_mode": "STRICT_TRANS_TABLES"})
    slim = _slim_worker_inputs(inputs, _ORDERS.name)
    parent = _derived_table_conversion(inputs, _ORDERS).target_ddl
    assert written in parent
    assert _derived_table_conversion(slim, _ORDERS).target_ddl == parent
    assert slim.inventory.server_settings == inputs.inventory.server_settings
    assert slim.inventory.tables == []  # still O(tables): no table rides along


def test_a_postgresql_workers_derived_ddl_names_the_source_timezone() -> None:
    table = TableDef(
        name="public.orders",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="created", mysql_type="timestamp without time zone", default="now()", default_is_expression=True),
        ],
        primary_key=["id"],
    )
    inputs = DataMigrationInputs(
        source_config=SourceConnectionConfig(host="db", database="app", source_type=SourceType.POSTGRES),
        source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="cluster.dsql.example", region="us-east-1"),
        inventory=SourceInventory(tables=[table], server_settings={"TimeZone": "Asia/Seoul"}),
    )
    worker = _derived_table_conversion(_slim_worker_inputs(inputs, table.name), table).target_ddl
    assert worker == _derived_table_conversion(inputs, table).target_ddl
    assert "AT TIME ZONE 'Asia/Seoul'" in worker
