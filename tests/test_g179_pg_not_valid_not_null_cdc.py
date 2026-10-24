# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G179: a PostgreSQL 18 NOT NULL added NOT VALID under CDC (follow-up to G167).

G167 records such a column nullable in the Step 1 inventory and creates it without NOT NULL
on Aurora DSQL, while PostgreSQL 18 sets ``pg_attribute.attnotnull`` as soon as the
constraint exists. The CDC catalog re-check reads ``attnotnull``, so it saw "v was NULL, now
NOT NULL" on every such column: an open finding that holds Cut over, cannot be waived and
never resolves (a Step 1 re-run records the column nullable again). Live on PostgreSQL 18.6
(throwaway local container): the read reported ``v`` NOT NULL and ``diff_table`` returned a
nullability change; at G167's parent the inventory had ``v`` NOT NULL and there was no drift.

The same split confused the column checks: Step 3 judged an insert on the target by the
source's NOT NULL (every insert "refused (23502)", "drop the NOT NULL" offered although the
target column has none), and Evaluation judged a bit(n)[] that stays in the event by the
target's (no longer "stops the connector", while Debezium marks the field required from
``attnotnull``).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.cdc_catalog_drift import (
    KIND_NULLABILITY_CHANGE,
    LiveColumn,
    LiveTable,
    baseline_digest,
    diff_table,
)
from dsql_migrator.core.models import (
    ColumnDef,
    PrerequisiteStatus,
    SourceInventory,
    SourceType,
    TableDef,
    TableValidationResult,
    ValidationMode,
    ValidationReport,
)
from dsql_migrator.core.prerequisites_postgres import (
    UNCARRYABLE_COLUMN_ARRIVES_NULL,
    PostgresCdcFacts,
    UncarryableCdcColumn,
    check_columns_replicable,
)
from dsql_migrator.ui.cdc_catalog_drift_gate import (
    CatalogDriftRequest,
    build_catalog_drift_probe,
    cdc_catalog_drift_cutover_gate,
    fold_catalog_drift_probe,
)

_PG = SourceType.POSTGRES
T0 = datetime(2026, 10, 24, 12, 0, tzinfo=timezone.utc)


def _nnv() -> TableDef:
    """rev.nnv as the G167 inventory holds it: v and ts NOT NULL NOT VALID, w NOT NULL."""
    return TableDef(
        name="rev.nnv",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="v", mysql_type="integer", nullable=True, not_null_not_valid=True),
            ColumnDef(name="ts", mysql_type="tsvector", nullable=True, not_null_not_valid=True),
            ColumnDef(name="w", mysql_type="integer", nullable=False),
            ColumnDef(name="x", mysql_type="integer", nullable=True),
        ],
        primary_key=["id"],
    )


def _live(**not_null: bool) -> LiveTable:
    """The PostgreSQL 18.6 catalog read of rev.nnv: attnotnull true for v and ts."""
    rules = {"id": True, "v": True, "ts": True, "w": True, "x": False, **not_null}
    types = {"ts": "tsvector"}
    return LiveTable(
        columns=tuple(
            LiveColumn(name, types.get(name, "integer"), not rules[name], False, rank)
            for rank, name in enumerate(("id", "v", "ts", "w", "x"))
        ),
        primary_key=frozenset({"id"}),
    )


def test_a_not_valid_not_null_column_is_no_drift_while_unvalidated_or_after_validate() -> None:
    assert diff_table(_nnv(), _live(), source_type=_PG) is None
    # The source VALIDATEs it (attnotnull stays true) or drops it (false): still nothing that
    # changes what CDC writes into the nullable target column.
    assert diff_table(_nnv(), _live(v=False, ts=False), source_type=_PG) is None


def test_a_real_null_rule_change_is_still_reported() -> None:
    diff = diff_table(_nnv(), _live(x=True), source_type=_PG)
    assert diff is not None and diff.kinds == (KIND_NULLABILITY_CHANGE,)
    assert [(c.column, c.before, c.after) for c in diff.changes] == [("x", "NULL", "NOT NULL")]
    diff = diff_table(_nnv(), _live(w=False), source_type=_PG)
    assert [(c.column, c.before, c.after) for c in diff.changes] == [("w", "NOT NULL", "NULL")]


def test_the_cut_over_gate_is_not_held_through_step_1_re_runs() -> None:
    request = CatalogDriftRequest(tables=(_nnv(),), source_type=_PG, token=("inv", ()))
    findings: dict = {}
    digests: dict = {}
    for n in range(3):  # each read after a Step 1 re-run that rebuilt the same baseline
        started = T0 + timedelta(minutes=n)
        probe = build_catalog_drift_probe(
            request, {"rev.nnv": _live()}, started=started, finished=started
        )
        assert probe.ok and probe.drift == {}
        findings, digests, transitions = fold_catalog_drift_probe(findings, digests, probe)
        assert transitions == [] and findings == {}
    assert digests == {"rev.nnv": baseline_digest(_nnv(), source_type=_PG)}
    item = TableValidationResult(
        table="rev.nnv", source_row_count=1, target_row_count=1, row_count_match=True,
        checksum_match=True, matched=True,
    )
    gate = cdc_catalog_drift_cutover_gate(
        findings=findings, scope=("rev.nnv",), cdc_in_use=True, cdc_evidence=True,
        registered=True, baseline_known=True, check_probe=False,
        report=ValidationReport.build(mode=ValidationMode.CHECKSUM, items=[item]),
        release="clean", inventory_names={"rev.nnv"}, explained_tables=(),
        compared_run_start=None, compared_overrides=None, last_probe=None, token=None,
        has_source_connection=True, waiver=None, evidence_at=None,
    )
    assert gate.open == () and not gate.blocks


def _facts(rows: dict) -> PostgresCdcFacts:
    names = list(rows)
    return PostgresCdcFacts(
        wal_level="logical", is_superuser=False, has_replication_role=True,
        max_replication_slots=10, used_replication_slots=1, max_wal_senders=10,
        used_wal_senders=1, is_in_recovery=False, max_slot_wal_keep_size_mb=-1,
        has_database_create=True, tables_not_owned=(),
        replica_identity={n: "d" for n in names}, unlogged={n: False for n in names},
        cdc_uncarryable_columns=rows,
    )


def test_step_3_says_a_not_valid_not_null_column_left_out_of_the_event_lands_null() -> None:
    # The probe reads attnotnull: NOT NULL on the source.
    probe = UncarryableCdcColumn(
        column="ts", declared_type="tsvector", reason=UNCARRYABLE_COLUMN_ARRIVES_NULL,
        nullable=False, in_event=False,
    )
    row = check_columns_replicable(_nnv(), _facts({"rev.nnv": (probe,)}))
    assert row is not None and row.status is PrerequisiteStatus.WARN and not row.required
    assert "a row CDC inserts gets NULL" in row.detail
    assert "23502" not in row.detail and "drop the NOT NULL" not in row.remediation
    # A plain NOT NULL column keeps the refused text and the route.
    plain = TableDef(
        name="rev.nnv",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False),
                 ColumnDef(name="ts", mysql_type="tsvector", nullable=False)],
        primary_key=["id"],
    )
    row = check_columns_replicable(plain, _facts({"rev.nnv": (probe,)}))
    assert "SQLSTATE 23502" in row.detail and "drop the NOT NULL" in row.remediation


def _concern(table: TableDef):
    report = CompatibilityAssessor(source_type=_PG).assess(SourceInventory(tables=[table]))
    (item,) = [i for i in report.items if i.object_name == table.name]
    return {c.rule_id: c for c in item.concerns}["CDC_UNCARRYABLE_COLUMN"]


def test_evaluation_says_a_not_valid_not_null_bit_array_stops_the_connector() -> None:
    table = TableDef(
        name="rev.b",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="b", mysql_type="bit(4)[]", nullable=True, not_null_not_valid=True),
        ],
        primary_key=["id"],
    )
    concern = _concern(table)
    assert "STOPS the PostgreSQL source connector" in concern.risk
    assert "NULL on every row CDC delivers" not in concern.risk
    # And as Step 3 grades it from the probe (attnotnull): a required FAIL.
    probe = UncarryableCdcColumn(
        column="b", declared_type="bit(4)[]", reason=UNCARRYABLE_COLUMN_ARRIVES_NULL,
        nullable=False, in_event=True,
    )
    row = check_columns_replicable(table, _facts({"rev.b": (probe,)}))
    assert row.status is PrerequisiteStatus.FAIL and row.required


def test_evaluation_says_a_not_valid_not_null_tsvector_lands_null() -> None:
    concern = _concern(_nnv())
    assert "a row CDC inserts gets NULL" in concern.risk and "23502" not in concern.risk
