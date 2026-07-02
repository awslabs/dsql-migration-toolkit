"""Unit tests for the CDC screen's connector-spike handling read-models.

These cover the NiceGUI-agnostic helpers added to surface the connector-spike
findings (cdc-handling-design.md / deploy/cdc-stack/SPIKE-RESULTS.md) in the CDC
screen:

- Oversized-LOB exclusion candidates + Debezium ``column.exclude.list`` value
  (H13), including PK exclusion and stable ordering.
- DLQ health classification against the circuit-breaker threshold (H6/H11).
- Connector health rows derived from managed state + lag (H1/H2/H9).
- The static CDC handling contract (H1-H13).
- ``DataMigrationState`` persistence of the opt-in LOB exclusion selection.
"""

from __future__ import annotations

from dsql_migrator.core.models import (
    ColumnDef,
    SourceInventory,
    TableDef,
)
from dsql_migrator.ui.data_migration import (
    DataMigrationState,
    assess_dlq_health,
    cdc_activity_summary,
    cdc_handling_facts,
    connector_health_rows,
    format_column_exclude_list,
    lob_exclusion_candidates,
)
from dsql_migrator.core.msk_connect_controller import ConnectorHealth


# ---------------------------------------------------------------------------
# Oversized-LOB exclusion (H13)
# ---------------------------------------------------------------------------


def _inventory_with_lobs() -> SourceInventory:
    return SourceInventory(
        tables=[
            TableDef(
                name="customers_sample.customers",
                columns=[
                    ColumnDef(name="id", mysql_type="BIGINT"),
                    ColumnDef(name="preferences", mysql_type="LONGTEXT"),
                    ColumnDef(name="avatar", mysql_type="MEDIUMBLOB"),
                    ColumnDef(name="name", mysql_type="VARCHAR(255)"),
                ],
                primary_key=["id"],
            ),
            TableDef(
                name="customers_sample.orders",
                columns=[
                    ColumnDef(name="order_id", mysql_type="BIGINT"),
                    ColumnDef(name="total", mysql_type="DECIMAL(10,2)"),
                ],
                primary_key=["order_id"],
            ),
        ]
    )


def test_lob_exclusion_candidates_flags_only_oversized_lob_columns() -> None:
    candidates = lob_exclusion_candidates(_inventory_with_lobs())
    assert len(candidates) == 1
    assert candidates[0].table == "customers_sample.customers"
    assert candidates[0].columns == ("preferences", "avatar")


def test_lob_exclusion_candidates_never_offers_pk_columns() -> None:
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="db.t",
                # A LOB column that is ALSO the primary key must not be offered.
                columns=[ColumnDef(name="doc", mysql_type="LONGTEXT")],
                primary_key=["doc"],
            )
        ]
    )
    assert lob_exclusion_candidates(inventory) == []


def test_lob_exclusion_candidates_empty_for_none_or_no_lobs() -> None:
    assert lob_exclusion_candidates(None) == []
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="db.t",
                columns=[ColumnDef(name="id", mysql_type="INT")],
                primary_key=["id"],
            )
        ]
    )
    assert lob_exclusion_candidates(inventory) == []


def test_lob_exclusion_candidates_sorted_by_table() -> None:
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="db.zeta",
                columns=[ColumnDef(name="blob_z", mysql_type="LONGBLOB")],
            ),
            TableDef(
                name="db.alpha",
                columns=[ColumnDef(name="blob_a", mysql_type="LONGBLOB")],
            ),
        ]
    )
    tables = [c.table for c in lob_exclusion_candidates(inventory)]
    assert tables == ["db.alpha", "db.zeta"]


def test_format_column_exclude_list_qualified_and_sorted() -> None:
    value = format_column_exclude_list(
        {
            "db.t2": ["b", "a"],
            "db.t1": ["z"],
        }
    )
    # db.table.column, sorted by table then column for a stable, reviewable value.
    assert value == "db.t1.z,db.t2.a,db.t2.b"


def test_format_column_exclude_list_empty() -> None:
    assert format_column_exclude_list({}) == ""


# ---------------------------------------------------------------------------
# DLQ health / circuit-breaker (H6/H11)
# ---------------------------------------------------------------------------


def test_assess_dlq_health_none_when_no_signal() -> None:
    assert assess_dlq_health(None) is None


def test_assess_dlq_health_ok_below_half_threshold() -> None:
    health = assess_dlq_health(10, threshold=100)
    assert health is not None
    assert health.level == "ok"
    assert "10" in health.message


def test_assess_dlq_health_zero_is_ok_no_records() -> None:
    health = assess_dlq_health(0, threshold=100)
    assert health is not None
    assert health.level == "ok"
    assert "No records" in health.message


def test_assess_dlq_health_warn_over_half_threshold() -> None:
    health = assess_dlq_health(60, threshold=100)
    assert health is not None
    assert health.level == "warn"


def test_assess_dlq_health_alarm_at_threshold() -> None:
    health = assess_dlq_health(100, threshold=100)
    assert health is not None
    assert health.level == "alarm"
    assert "systematic" in health.message.lower()


def test_assess_dlq_health_clamps_negative() -> None:
    health = assess_dlq_health(-5, threshold=100)
    assert health is not None
    assert health.depth == 0
    assert health.level == "ok"


# ---------------------------------------------------------------------------
# Connector health (H1/H2/H9)
# ---------------------------------------------------------------------------


def test_connector_health_running_is_ok_with_lag_detail() -> None:
    rows = connector_health_rows({"src": "RUNNING"}, lag_seconds=1.2)
    assert len(rows) == 1
    assert rows[0].tone == "ok"
    assert "1.2s" in rows[0].detail


def test_connector_health_failed_is_bad() -> None:
    rows = connector_health_rows({"sink": "FAILED"})
    assert rows[0].tone == "bad"


def test_connector_health_non_running_is_warn() -> None:
    rows = connector_health_rows({"sink": "PROVISIONING"})
    assert rows[0].tone == "warn"


def test_connector_health_elevated_lag_nudges_to_warn() -> None:
    rows = connector_health_rows(
        {"src": "RUNNING"}, lag_seconds=45.0, lag_warn_seconds=30.0
    )
    assert rows[0].tone == "warn"
    assert "elevated" in rows[0].detail


def test_connector_health_sorted_by_name_and_empty() -> None:
    rows = connector_health_rows({"z": "RUNNING", "a": "RUNNING"})
    assert [r.name for r in rows] == ["a", "z"]
    assert connector_health_rows({}) == []


def test_connector_role_label_friendly_names() -> None:
    from dsql_migrator.ui.data_migration import connector_role_label

    assert connector_role_label("mysql-dsql-cdc-spike-debezium-source").startswith("Source")
    assert connector_role_label("mysql-dsql-cdc-spike-dsql-sink-v6").startswith("Sink")
    # An unrecognized name falls back to the raw id (never hidden).
    assert connector_role_label("mystery-connector") == "mystery-connector"


def test_connector_health_rows_carry_friendly_label() -> None:
    rows = connector_health_rows({"mysql-dsql-cdc-spike-debezium-source": "RUNNING"})
    assert rows[0].label.startswith("Source")
    assert rows[0].name == "mysql-dsql-cdc-spike-debezium-source"  # raw id preserved


def test_connector_health_ordered_source_before_sink() -> None:
    # Real connector names: data-flow order (Source first) regardless of name sort.
    rows = connector_health_rows(
        {
            "mysql-dsql-cdc-spike-dsql-sink-v6": "RUNNING",
            "mysql-dsql-cdc-spike-debezium-source": "RUNNING",
        }
    )
    assert [r.label.split(" ")[0] for r in rows] == ["Source", "Sink"]


# ---------------------------------------------------------------------------
# CDC handling contract (H1-H13)
# ---------------------------------------------------------------------------


def test_cdc_handling_facts_cover_handled_and_caveats() -> None:
    facts = cdc_handling_facts()
    handled = [f for f in facts if f.handled]
    caveats = [f for f in facts if not f.handled]
    # Both the guarantees (no duplicates, inserts/updates/deletes, …) and the
    # caveats (DDL not replicated, large-value exclusion, DLQ accumulation) must be
    # present. Titles are customer-facing (no internal jargon / spike H-codes).
    assert handled and caveats
    blob = " ".join((f.title + " " + f.detail) for f in facts).lower()
    # The guarantee that retries don't duplicate, and the must-know limits.
    assert "duplicate" in blob
    assert "ddl" in blob  # the critical "DDL not replicated" caveat
    assert "dead-letter" in blob or "dlq" in blob
    # The internal spike hypothesis codes are retained on the model for
    # traceability but must NOT leak into the user-facing title/detail text.
    assert all(f.evidence for f in facts)
    assert "h4" not in blob and "h13" not in blob and "§4" not in blob


# ---------------------------------------------------------------------------
# DataMigrationState: opt-in LOB exclusion persistence (H13)
# ---------------------------------------------------------------------------


def test_state_lob_exclusion_toggle_on_and_off() -> None:
    state = DataMigrationState()
    assert state.cdc_lob_exclusions() == {}

    state.set_cdc_lob_exclusion("db.t", "doc", True)
    state.set_cdc_lob_exclusion("db.t", "blob", True)
    assert state.cdc_lob_exclusions() == {"db.t": {"doc", "blob"}}

    # Unticking the last column drops the table key entirely (clean empty state).
    state.set_cdc_lob_exclusion("db.t", "doc", False)
    state.set_cdc_lob_exclusion("db.t", "blob", False)
    assert state.cdc_lob_exclusions() == {}


def test_state_lob_exclusion_returns_a_copy() -> None:
    state = DataMigrationState()
    state.set_cdc_lob_exclusion("db.t", "doc", True)
    snapshot = state.cdc_lob_exclusions()
    snapshot["db.t"].add("mutated")
    # Mutating the returned copy must not affect the stored selection.
    assert state.cdc_lob_exclusions() == {"db.t": {"doc"}}


# ---------------------------------------------------------------------------
# cdc_activity_summary — the "no changes flowing" (cutover) signal
# ---------------------------------------------------------------------------


def test_activity_idle_when_both_rates_zero() -> None:
    health = {
        "src": ConnectorHealth(poll_rate=0.0),
        "sink": ConnectorHealth(send_rate=0.0),
    }
    summary = cdc_activity_summary(health)
    assert summary.idle is True
    assert summary.source_poll_rate == 0.0
    assert summary.sink_send_rate == 0.0


def test_activity_not_idle_when_streaming() -> None:
    health = {
        "src": ConnectorHealth(poll_rate=5.0),
        "sink": ConnectorHealth(send_rate=4.0),
    }
    summary = cdc_activity_summary(health)
    assert summary.idle is False


def test_activity_idle_none_when_poll_rate_unknown() -> None:
    # Source poll rate unknown -> cannot assert idle (must be None, never True).
    health = {
        "src": ConnectorHealth(poll_rate=None),
        "sink": ConnectorHealth(send_rate=0.0),
    }
    summary = cdc_activity_summary(health)
    assert summary.idle is None


def test_activity_idle_none_when_send_rate_unknown() -> None:
    health = {
        "src": ConnectorHealth(poll_rate=0.0),
        "sink": ConnectorHealth(send_rate=None),
    }
    assert cdc_activity_summary(health).idle is None


def test_activity_empty_health_is_unknown() -> None:
    summary = cdc_activity_summary({})
    assert summary.idle is None
    assert summary.source_poll_rate is None
    assert summary.sink_send_rate is None


def test_activity_takes_max_rate_across_connectors() -> None:
    # Worst-case "still flowing": the highest known rate wins.
    health = {
        "a": ConnectorHealth(poll_rate=0.0, send_rate=0.0),
        "b": ConnectorHealth(poll_rate=2.0, send_rate=1.0),
    }
    summary = cdc_activity_summary(health)
    assert summary.source_poll_rate == 2.0
    assert summary.sink_send_rate == 1.0
    assert summary.idle is False
