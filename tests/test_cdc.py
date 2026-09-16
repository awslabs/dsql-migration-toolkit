# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the optional CDC catch-up stub (Requirement 5.5, Property 11).

Covers:

- CDC options are opt-in: ``enabled`` defaults to ``False``,
- a disabled coordinator's ``start`` is a no-op returning a DISABLED result,
- an enabled coordinator returns the documented NOT_IMPLEMENTED stub result that
  references resuming from the watermark coordinates,
- the result carries a resume point derived from the watermark (Property 11),
- watermarks without usable coordinates are reported as non-resumable,
- importing this module does not require the optional ``python-mysql-replication``
  package (it is a behind-a-flag stub).

No real MySQL source, binlog reader, or DSQL target is contacted by these tests.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from dsql_migrator.core.cdc import (
    CdcCatchUp,
    CdcOptions,
    CdcResult,
    CdcResumePoint,
    CdcStatus,
)
from dsql_migrator.core.models import (
    SourceConnectionConfig,
    TargetConnectionConfig,
    Watermark,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _source() -> SourceConnectionConfig:
    return SourceConnectionConfig(host="db.example.com", database="app")


def _target() -> TargetConnectionConfig:
    return TargetConnectionConfig(
        cluster_endpoint="my-cluster.dsql.us-east-1.on.aws",
        region="us-east-1",
    )


def _watermark(**overrides: object) -> Watermark:
    base: dict[str, object] = {
        "binlog_file": "mysql-bin.000123",
        "binlog_position": 45678,
        "gtid_executed": "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100",
        "server_uuid": "3E11FA47-71CA-11E1-9E33-C80AA9429562",
        "snapshot_timestamp": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        "table_row_counts": {"customers": 10},
    }
    base.update(overrides)
    return Watermark(**base)


def _catch_up(options: CdcOptions) -> CdcCatchUp:
    return CdcCatchUp(options, source=_source(), target=_target())


# ---------------------------------------------------------------------------
# Opt-in default (Requirement 5.5 optional goal)
# ---------------------------------------------------------------------------


def test_cdc_options_disabled_by_default() -> None:
    assert CdcOptions().enabled is False


# ---------------------------------------------------------------------------
# Disabled: start is a no-op returning DISABLED
# ---------------------------------------------------------------------------


def test_start_is_noop_and_returns_disabled_when_disabled() -> None:
    result = _catch_up(CdcOptions()).start(_watermark())
    assert isinstance(result, CdcResult)
    assert result.status is CdcStatus.DISABLED
    assert result.resume_point is None


# ---------------------------------------------------------------------------
# Enabled: documented NOT_IMPLEMENTED stub behavior
# ---------------------------------------------------------------------------


def test_start_returns_not_implemented_stub_when_enabled() -> None:
    result = _catch_up(CdcOptions(enabled=True)).start(_watermark())
    assert result.status is CdcStatus.NOT_IMPLEMENTED
    # The stub documents the intended python-mysql-replication integration.
    assert "python-mysql-replication" in result.detail


def test_enabled_result_carries_resume_point_from_watermark() -> None:
    watermark = _watermark()
    result = _catch_up(CdcOptions(enabled=True)).start(watermark)

    assert result.resume_point is not None
    assert result.resume_point.binlog_file == watermark.binlog_file
    assert result.resume_point.binlog_position == watermark.binlog_position
    assert result.resume_point.gtid_executed == watermark.gtid_executed
    assert result.resume_point.server_uuid == watermark.server_uuid
    # The "caught up to" point mirrors the watermark snapshot timestamp.
    assert result.caught_up_to == watermark.snapshot_timestamp


def test_enabled_reports_non_resumable_when_watermark_lacks_coordinates() -> None:
    # A watermark captured where binlog/GTID metadata was unavailable.
    watermark = _watermark(
        binlog_file=None,
        binlog_position=None,
        gtid_executed=None,
        server_uuid=None,
    )
    result = _catch_up(CdcOptions(enabled=True)).start(watermark)

    assert result.status is CdcStatus.NOT_IMPLEMENTED
    assert result.resume_point is not None
    assert result.resume_point.has_coordinates() is False
    assert "cannot resume" in result.detail.lower()


# ---------------------------------------------------------------------------
# Resume point contract (Property 11)
# ---------------------------------------------------------------------------


def test_resume_point_from_watermark_copies_coordinates() -> None:
    watermark = _watermark()
    point = CdcResumePoint.from_watermark(watermark)
    assert point.has_coordinates() is True
    assert point.binlog_file == "mysql-bin.000123"
    assert point.binlog_position == 45678


def test_resume_point_has_coordinates_with_gtid_only() -> None:
    point = CdcResumePoint(gtid_executed="uuid:1-5")
    assert point.has_coordinates() is True


def test_resume_point_has_coordinates_with_binlog_only() -> None:
    point = CdcResumePoint(binlog_file="mysql-bin.000001", binlog_position=4)
    assert point.has_coordinates() is True


def test_resume_point_without_coordinates_is_not_resumable() -> None:
    assert CdcResumePoint().has_coordinates() is False


# ---------------------------------------------------------------------------
# Stub does not pull in the optional dependency
# ---------------------------------------------------------------------------


def test_module_import_does_not_require_python_mysql_replication() -> None:
    # The behind-a-flag stub must never import the optional binlog package.
    assert "pymysqlreplication" not in sys.modules


# ---------------------------------------------------------------------------
# CDC precondition: enforced target foreign keys (Full-load-only -> CDC bug)
# ---------------------------------------------------------------------------


def test_cdc_blocking_foreign_keys_classifies_ours_foreign_and_unknown() -> None:
    """A Full-load-only run leaves FKs enforced; CDC must not stream until they are gone.

    The sink applies change records across several tasks with no parent-before-child
    ordering, so a child row can arrive first, be rejected with SQLSTATE 23503, and be
    dead-lettered PERMANENTLY (23503 is not in the sink's transient set) while the task
    keeps running and offsets advance -- silent data loss. Ownership decides what the
    tool may remove: only what its own conversion renders (cut over re-creates those).
    """
    from dsql_migrator.core.cdc import cdc_blocking_foreign_keys

    result = cdc_blocking_foreign_keys(
        {
            "orders": ["fk_orders_users"],          # ours -> safe to drop
            "audit": ["fk_audit_handmade"],         # the user's -> must NOT be dropped
            "products": [],                          # genuinely clean
            "huge": None,                            # unreadable -> unknown
        },
        {"orders": ["fk_orders_users"], "audit": [], "products": ["fk_never_applied"]},
    )
    assert result.ours == [("orders", "fk_orders_users")]
    assert result.foreign == [("audit", "fk_audit_handmade")]
    assert result.unknown_tables == ["huge"]
    assert result.blocking is True


def test_cdc_blocking_foreign_keys_is_silent_when_the_target_has_none() -> None:
    # The normal CDC-only case (no prior Full Load): nothing enforced -> no gate, no
    # warning. Severity calibration: do not alarm the user for the expected state.
    from dsql_migrator.core.cdc import cdc_blocking_foreign_keys

    result = cdc_blocking_foreign_keys(
        {"orders": [], "order_items": []},
        {"orders": ["fk_orders_users"], "order_items": ["fk_items_orders"]},
    )
    assert result == ([], [], [])
    assert result.blocking is False


def test_cdc_blocking_foreign_keys_fails_closed_on_an_unreadable_catalog() -> None:
    # None means "could not read", NOT "none exist". Reporting it as clean would open the
    # gate on exactly the case the gate exists for, so unknown must block.
    from dsql_migrator.core.cdc import cdc_blocking_foreign_keys

    result = cdc_blocking_foreign_keys({"orders": None}, {"orders": ["fk_orders_users"]})
    assert result.ours == [] and result.foreign == []
    assert result.unknown_tables == ["orders"]
    assert result.blocking is True


def test_cdc_blocking_foreign_keys_matches_case_sensitively_per_table() -> None:
    # The converter emits QUOTED identifiers, so target names keep the source's case;
    # case-folding would mis-classify. And a name is only "ours" on ITS OWN table -- a
    # user FK sharing a name on a different table must stay foreign.
    from dsql_migrator.core.cdc import cdc_blocking_foreign_keys

    result = cdc_blocking_foreign_keys(
        {"Orders": ["FK_Orders_Users"], "other": ["fk_orders_users"]},
        {"Orders": ["FK_Orders_Users"], "orders": ["fk_orders_users"]},
    )
    assert result.ours == [("Orders", "FK_Orders_Users")]
    assert result.foreign == [("other", "fk_orders_users")]


def test_cdc_blocking_foreign_keys_matches_a_name_truncated_to_63_bytes() -> None:
    """PostgreSQL/DSQL truncate identifiers to 63 bytes (NAMEDATALEN).

    A long source FK name is stored TRUNCATED in pg_constraint while the rendered DDL
    carries it in full, so a naive equality check disowns the tool's OWN constraint --
    and because the tool refuses to drop what it does not own, CDC would be blocked with
    no way out from the UI.
    """
    from dsql_migrator.core.cdc import cdc_blocking_foreign_keys

    long_name = "fk_" + ("x" * 70)          # 73 chars, as rendered in the DDL
    stored = long_name[:63]                  # what the catalog actually holds

    result = cdc_blocking_foreign_keys({"orders": [stored]}, {"orders": [long_name]})
    assert result.ours == [("orders", stored)]
    assert result.foreign == []

    # A DIFFERENT long name that merely shares the first 63 bytes is still ours only if
    # the truncation genuinely matches -- an unrelated constraint stays foreign.
    unrelated = cdc_blocking_foreign_keys(
        {"orders": ["fk_someone_elses_constraint"]}, {"orders": [long_name]}
    )
    assert unrelated.foreign == [("orders", "fk_someone_elses_constraint")]
    assert unrelated.ours == []
