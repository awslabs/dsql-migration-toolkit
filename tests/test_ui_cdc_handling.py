# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the CDC screen's connector handling read-models.

These cover the NiceGUI-agnostic helpers that surface the CDC connector
handling behavior in the CDC screen:

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
    assert state.lob_exclusions() == {}

    state.set_lob_exclusion("db.t", "doc", True)
    state.set_lob_exclusion("db.t", "blob", True)
    assert state.lob_exclusions() == {"db.t": {"doc", "blob"}}

    # Unticking the last column drops the table key entirely (clean empty state).
    state.set_lob_exclusion("db.t", "doc", False)
    state.set_lob_exclusion("db.t", "blob", False)
    assert state.lob_exclusions() == {}


def test_state_lob_exclusion_returns_a_copy() -> None:
    state = DataMigrationState()
    state.set_lob_exclusion("db.t", "doc", True)
    snapshot = state.lob_exclusions()
    snapshot["db.t"].add("mutated")
    # Mutating the returned copy must not affect the stored selection.
    assert state.lob_exclusions() == {"db.t": {"doc"}}


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


def test_activity_idle_absorbs_source_heartbeat_floor() -> None:
    # The source (Debezium) connector never fully goes silent: heartbeats
    # (heartbeat.interval.ms=300000) keep SourceRecordPollRate at a small floor
    # (~0.03/s on the CloudWatch average) even when the captured tables are idle.
    # The threshold sits above that floor, so a drained pipeline (sink send 0) still
    # reads as idle instead of lingering as "streaming".
    health = {
        "src": ConnectorHealth(poll_rate=0.03),  # heartbeat residual, no real changes
        "sink": ConnectorHealth(send_rate=0.0),  # nothing being applied
    }
    assert cdc_activity_summary(health).idle is True


def test_activity_not_idle_when_sink_stalled() -> None:
    # Safety property of the AND-both-rates rule: if the SOURCE is still producing
    # real changes (poll rate well above the heartbeat floor) but the SINK is not
    # sending (stalled / lagging), the pipeline is NOT drained -- it must read as
    # "streaming", never idle, so cut-over is not wrongly signalled as safe.
    health = {
        "src": ConnectorHealth(poll_rate=5.0),  # real change traffic
        "sink": ConnectorHealth(send_rate=0.0),  # sink not keeping up
    }
    assert cdc_activity_summary(health).idle is False


def _diverged(previous=None):
    """One poll of the stall signature: source producing, sink applying nothing."""
    return cdc_activity_summary(
        {
            "src": ConnectorHealth(poll_rate=5.0),
            "sink": ConnectorHealth(send_rate=0.0),
        },
        previous=previous,
    )


def test_sink_stall_is_reported_separately_from_idle() -> None:
    # The reported failure: source producing, sink applying nothing. `idle` must stay
    # False (a stalled pipeline is NOT drained -- that guards cut-over), which is
    # exactly why the stall was invisible: not-idle rendered as "Streaming — changes
    # are flowing". `sink_stalled` is the signal that names the divergence.
    summary = _diverged()
    assert summary.idle is False
    assert summary.sink_stalled is True


def test_a_single_poll_divergence_is_not_reported_as_a_stall() -> None:
    # Regression (hit live on a healthy pipeline): both rates are CloudWatch averages
    # over a trailing window, so right after a burst of writes ends the source still
    # shows its residual while the sink has legitimately gone quiet. That one-poll
    # divergence is indistinguishable from a real stall, and it raised a red "Sink
    # stalled" alarm on a pipeline that was replicating fine. Only a SUSTAINED
    # divergence may be reported.
    first = _diverged()
    assert first.sink_stalled is True          # the observation stands
    assert first.sink_stall_confirmed is False  # ...but it is not yet a verdict
    second = _diverged(previous=first)
    assert second.sink_stall_confirmed is False


def test_a_sustained_divergence_is_confirmed_as_a_stall() -> None:
    # A real stall is permanent (an ejected consumer never rejoins by itself), so
    # persistence costs nothing in detection.
    s = None
    for _ in range(3):
        s = _diverged(previous=s)
    assert s.sink_stall_polls == 3
    assert s.sink_stall_confirmed is True


def test_the_stall_streak_resets_as_soon_as_the_sink_sends() -> None:
    s = _diverged(previous=_diverged())          # 2 polls of divergence
    assert s.sink_stall_polls == 2
    healthy = cdc_activity_summary(
        {"src": ConnectorHealth(poll_rate=5.0), "sink": ConnectorHealth(send_rate=4.8)},
        previous=s,
    )
    assert healthy.sink_stall_polls == 0
    assert healthy.sink_stall_confirmed is False


def test_an_unknown_rate_clears_the_stall_streak() -> None:
    # An unreadable CloudWatch metric is a monitoring failure, not a data one -- it
    # must never accumulate toward a stall verdict.
    s = _diverged(previous=_diverged())
    unknown = cdc_activity_summary({"src": ConnectorHealth(poll_rate=5.0)}, previous=s)
    assert unknown.sink_stalled is None
    assert unknown.sink_stall_polls == 0
    assert unknown.sink_stall_confirmed is False


def test_drained_pipeline_is_idle_but_not_stalled() -> None:
    # Both rates at ~0 = drained/caught up. That must NOT read as a stall, or every
    # quiet pipeline would raise a false alarm (the source's 5-min heartbeat leaves an
    # irreducible ~0.03/s floor, so "source quiet" is a small non-zero rate).
    summary = cdc_activity_summary(
        {
            "src": ConnectorHealth(poll_rate=0.03),
            "sink": ConnectorHealth(send_rate=0.0),
        }
    )
    assert summary.idle is True
    assert summary.sink_stalled is False


def test_healthy_streaming_is_neither_idle_nor_stalled() -> None:
    summary = cdc_activity_summary(
        {
            "src": ConnectorHealth(poll_rate=5.0),
            "sink": ConnectorHealth(send_rate=4.8),
        }
    )
    assert summary.idle is False
    assert summary.sink_stalled is False


def test_sink_stall_is_never_asserted_on_an_unknown_rate() -> None:
    # Same honesty rule as `idle`: an unreadable CloudWatch metric must not be
    # reported as a stall (that would alarm on a monitoring failure, not a data one).
    only_source = cdc_activity_summary({"src": ConnectorHealth(poll_rate=5.0)})
    assert only_source.sink_stalled is None
    only_sink = cdc_activity_summary({"sink": ConnectorHealth(send_rate=0.0)})
    assert only_sink.sink_stalled is None


def test_dlq_zero_depth_is_not_painted_green_while_the_sink_is_stalled() -> None:
    # A stalled sink never reaches a record to quarantine, so depth is 0 -- which the
    # panel used to paint as a green "success" all-clear during total data loss.
    from dsql_migrator.ui.data_migration._cdc_ui import _dlq_panel_tone
    from dsql_migrator.ui.data_migration._models import assess_dlq_health

    clean = assess_dlq_health(0)
    assert _dlq_panel_tone(clean) == "success"  # genuinely clean stream: unchanged
    assert _dlq_panel_tone(clean, sink_stalled=True) == "info"  # proves nothing now


def test_sink_stall_and_recovery_each_log_once_on_transition() -> None:
    # The CDC poll runs every few seconds, so the event must fire on the STATE CHANGE
    # only -- otherwise the durable activity log fills with identical lines.
    from dsql_migrator.ui.data_migration import _cdc_status as status_mod

    logged: list[tuple] = []

    class _FakeLog:
        @staticmethod
        def log_activity(category, action, **kwargs):
            logged.append((action, kwargs.get("status"), kwargs.get("detail") or ""))

    healthy = cdc_activity_summary(
        {"src": ConnectorHealth(poll_rate=5.0), "sink": ConnectorHealth(send_rate=5.0)}
    )
    # A CONFIRMED stall: the log event keys off sink_stall_confirmed, so a one-poll
    # blip must not write a FAILURE line (see the single-poll test above).
    stalled = None
    for _ in range(3):
        stalled = _diverged(previous=stalled)

    import dsql_migrator.core.activity_log as real_log

    original = real_log.log_activity
    real_log.log_activity = _FakeLog.log_activity  # type: ignore[assignment]
    try:
        # healthy -> stalled: one FAILURE event.
        status_mod._log_sink_stall_transition(healthy, stalled)
        # stalled -> stalled (repeated polls): silent.
        status_mod._log_sink_stall_transition(stalled, stalled)
        status_mod._log_sink_stall_transition(stalled, stalled)
        # stalled -> healthy: one recovery event.
        status_mod._log_sink_stall_transition(stalled, healthy)
        status_mod._log_sink_stall_transition(healthy, healthy)
    finally:
        real_log.log_activity = original  # type: ignore[assignment]

    assert [action for action, _, _ in logged] == ["sink stalled", "sink recovered"]
    stall_detail = logged[0][2]
    # Says what happened, with the rates, and what NOT to do next.
    assert "5.00 rec/s" in stall_detail and "0.00 rec/s" in stall_detail
    assert "cut over" in stall_detail.lower()


def test_an_unconfirmed_divergence_writes_no_activity_log_event() -> None:
    # The regression: a post-burst one-poll divergence put a FAILURE line in the
    # durable log for a healthy pipeline. Only a confirmed stall may be recorded.
    from dsql_migrator.ui.data_migration import _cdc_status as status_mod
    import dsql_migrator.core.activity_log as real_log

    logged: list = []
    healthy = cdc_activity_summary(
        {"src": ConnectorHealth(poll_rate=5.0), "sink": ConnectorHealth(send_rate=5.0)}
    )
    blip = _diverged(previous=healthy)  # 1 poll only -> not confirmed
    assert blip.sink_stalled is True and blip.sink_stall_confirmed is False

    original = real_log.log_activity
    real_log.log_activity = lambda *a, **k: logged.append(a)  # type: ignore[assignment]
    try:
        status_mod._log_sink_stall_transition(healthy, blip)
    finally:
        real_log.log_activity = original  # type: ignore[assignment]
    assert logged == []


def test_change_flow_renders_the_stall_instead_of_streaming_all_clear() -> None:
    # The regression this closes: with source 5 rec/s and sink 0, `idle is False` fell
    # through to "Streaming — changes are flowing", asserting health off the SOURCE rate
    # while nothing reached DSQL. The stall branch must win, and must say what to do.
    from tests.test_ui_data_migration import _RecordingUi
    from dsql_migrator.ui.data_migration._cdc_ui import _render_change_flow_status

    # A CONFIRMED stall (the divergence held for the required consecutive polls) --
    # a single-poll blip must not render this, which the test above covers.
    stalled = None
    for _ in range(3):
        stalled = _diverged(previous=stalled)
    assert stalled.sink_stall_confirmed is True
    ui = _RecordingUi()
    _render_change_flow_status(ui, stalled)
    text = " ".join(ui.texts)
    assert "changes are flowing" not in text  # the misleading all-clear is gone
    assert "Sink stalled" in text
    assert "NOT reaching DSQL" in text
    # Actionable, per the project's "what happened, what to do next" rule.
    assert "RUNNING is not evidence" in text
    assert "Commit of offsets timed out" in text
    assert "do NOT cut over" in text or "not cut over" in text.lower()
    # Both rate bars still render, so the operator sees the numbers behind the verdict.
    assert "5.00 rec/s" in text and "0.00 rec/s" in text


def test_change_flow_still_shows_streaming_when_the_sink_keeps_up() -> None:
    from tests.test_ui_data_migration import _RecordingUi
    from dsql_migrator.ui.data_migration._cdc_ui import _render_change_flow_status

    ui = _RecordingUi()
    _render_change_flow_status(
        ui,
        cdc_activity_summary(
            {
                "src": ConnectorHealth(poll_rate=5.0),
                "sink": ConnectorHealth(send_rate=4.8),
            }
        ),
    )
    text = " ".join(ui.texts)
    assert "Streaming — changes are flowing" in text
    assert "Sink stalled" not in text


def test_change_flow_shows_idle_for_a_drained_pipeline() -> None:
    from tests.test_ui_data_migration import _RecordingUi
    from dsql_migrator.ui.data_migration._cdc_ui import _render_change_flow_status

    ui = _RecordingUi()
    _render_change_flow_status(
        ui,
        cdc_activity_summary(
            {
                "src": ConnectorHealth(poll_rate=0.03),
                "sink": ConnectorHealth(send_rate=0.0),
            }
        ),
    )
    text = " ".join(ui.texts)
    assert "pipeline idle" in text
    assert "Sink stalled" not in text


# ---------------------------------------------------------------------------
# PostgreSQL CDC replication-slot WAL-health monitor (Phase C6)
# ---------------------------------------------------------------------------


class _StubSlotState:
    """Minimal migration-state stub for the slot-health fetch/render."""

    def __init__(self, stack_name="mysql-dsql-cdc-stack"):
        self.cdc_stack_name = stack_name
        self.cdc_slot_health = "unset"

    def set_cdc_slot_health(self, health):
        self.cdc_slot_health = health


def test_render_cdc_slot_health_panel() -> None:
    from types import SimpleNamespace

    from dsql_migrator.core.cdc_postgres import SlotHealth
    from dsql_migrator.ui.data_migration._cdc_monitoring import _render_cdc_slot_health
    from tests.test_ui_data_migration import _RecordingUi

    # None / no slot -> nothing rendered (inherently MySQL-safe: MySQL never populates it).
    ui = _RecordingUi()
    _render_cdc_slot_health(ui, SimpleNamespace(cdc_slot_health=None))
    assert ui.texts == []
    ui2 = _RecordingUi()
    _render_cdc_slot_health(ui2, SimpleNamespace(cdc_slot_health=SlotHealth("s", exists=False)))
    assert ui2.texts == []

    # Invalidated slot -> an error notice naming the problem.
    ui3 = _RecordingUi()
    _render_cdc_slot_health(
        ui3,
        SimpleNamespace(
            cdc_slot_health=SlotHealth("s", exists=True, active=True, wal_status="lost")
        ),
    )
    assert any("invalidated" in t.lower() for t in ui3.texts), ui3.texts

    # Healthy slot -> a success notice.
    ui4 = _RecordingUi()
    _render_cdc_slot_health(
        ui4,
        SimpleNamespace(
            cdc_slot_health=SlotHealth(
                "s", exists=True, active=True, wal_status="reserved", safe_wal_size=1000
            )
        ),
    )
    assert any("healthy" in t.lower() for t in ui4.texts), ui4.texts


def test_refresh_pg_slot_health_reads_for_pg_and_noops_for_mysql() -> None:
    from dsql_migrator.core.models import SourceConnectionConfig, SourceType
    from dsql_migrator.ui.data_migration._cdc_status import _refresh_pg_slot_health

    class _R:
        def __init__(self, row):
            self._row = row

        def first(self):
            return self._row

    class _Conn:
        def __init__(self, row):
            self._row = row

        def execute(self, statement, params=None):
            return _R(self._row)

    # MySQL source -> the dialect has no slot, so health is set to None (never crashes).
    mysql_state = _StubSlotState()
    _refresh_pg_slot_health(
        mysql_state,
        SourceConnectionConfig(source_type=SourceType.MYSQL, host="db", database="app"),
        _Conn(None),
    )
    assert mysql_state.cdc_slot_health is None

    # PostgreSQL source -> reads pg_replication_slots for the stack's deterministic slot.
    pg_state = _StubSlotState(stack_name="mysql-dsql-cdc-stack")
    _refresh_pg_slot_health(
        pg_state,
        SourceConnectionConfig(source_type=SourceType.POSTGRES, host="pg", database="app"),
        _Conn((True, "reserved", 999, "0/16B3748", "0/16B3800")),
    )
    from dsql_migrator.core.cdc_pg_slot import pg_slot_name

    assert pg_state.cdc_slot_health is not None
    assert pg_state.cdc_slot_health.slot_name == pg_slot_name("mysql-dsql-cdc-stack")
    assert pg_state.cdc_slot_health.wal_status == "reserved"


def test_cdc_resume_signal_is_engine_aware() -> None:
    # The shared CDC start signal differs by engine: MySQL Manual seeds a GTID/binlog
    # resume_override; PostgreSQL Manual has no coordinate and instead re-snapshots
    # (force_initial_snapshot), because Debezium PG resumes only from the slot.
    from types import SimpleNamespace
    from dsql_migrator.core.models import SourceConnectionConfig, SourceType
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_resume_signal

    def _session(source_type):
        return SimpleNamespace(
            source_config=SourceConnectionConfig(
                source_type=source_type, host="h", database="app"
            )
        )

    # PostgreSQL: no override value; Manual -> force initial (re-snapshot), Auto -> gapless.
    pg = _session(SourceType.POSTGRES)
    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    assert _cdc_resume_signal(state, pg) == (None, True)
    state.set_cdc_start_mode("auto")
    assert _cdc_resume_signal(state, pg) == (None, False)

    # MySQL: force_initial_snapshot is never set. A Manual GTID becomes the resume_override.
    my = _session(SourceType.MYSQL)
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(gtid="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100")
    override, force = _cdc_resume_signal(state, my)
    assert force is False
    assert override is not None and override.gtid_executed
    # MySQL Manual with nothing entered -> no override (nothing to seed).
    state.set_cdc_start_position(gtid=None, binlog_file=None, binlog_pos=None)
    assert _cdc_resume_signal(state, my) == (None, False)
    # MySQL Automatic -> no override.
    state.set_cdc_start_mode("auto")
    assert _cdc_resume_signal(state, my) == (None, False)


class _FakeEl:
    def __init__(self, sink):
        self._sink = sink

    def classes(self, *_a, **_k):
        return self

    def props(self, *_a, **_k):
        return self

    def tooltip(self, text="", *_a, **_k):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False


class _FakeUi:
    """Minimal NiceGUI double for rendering the CDC start-point card (collects text)."""

    def __init__(self):
        self.texts: list[str] = []
        self.radios: list[dict] = []

    def _el(self):
        return _FakeEl(self)

    def card(self, *_a, **_k):
        return self._el()

    def row(self, *_a, **_k):
        return self._el()

    def column(self, *_a, **_k):
        return self._el()

    def icon(self, *_a, **_k):
        return self._el()

    def space(self, *_a, **_k):
        return self._el()

    def spinner(self, *_a, **_k):
        return self._el()

    def label(self, text="", *_a, **_k):
        self.texts.append(str(text))
        return self._el()

    def badge(self, text="", *_a, **_k):
        self.texts.append(str(text))
        return self._el()

    def radio(self, options=None, *_a, **_k):
        if isinstance(options, dict):
            self.radios.append(options)
        return self._el()

    def input(self, label="", *_a, **_k):
        self.texts.append(str(label))
        return self._el()

    def button(self, text="", *_a, **_k):
        self.texts.append(str(text))
        return self._el()

    def notify(self, *_a, **_k):
        return None


def test_estimate_cdc_table_rows_threads_source_dialect(monkeypatch) -> None:
    # Regression: the CDC topic-sizing estimate must run with the SOURCE-engine dialect, so
    # a PostgreSQL source uses pg_class.reltuples -- not the MySQL information_schema default,
    # which raises on PG and (under the broad except) silently returns None -> uniform
    # partitions. Assert the dialect actually threaded through matches the source engine.
    from types import SimpleNamespace
    import dsql_migrator.core.watermark as wm
    import dsql_migrator.ui.connect as connect
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core.models import SourceConnectionConfig, SourceType
    from dsql_migrator.core.source_dialect import MySQLSourceDialect, PostgresSourceDialect
    from dsql_migrator.ui.data_migration._cdc_ui import _estimate_cdc_table_rows

    class _RoConn:
        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    class _RoEngine:
        def connect(self):
            return _RoConn()

        def dispose(self):
            pass

    seen = {}

    def _fake_estimate(conn, tables, dialect):
        seen["dialect"] = dialect
        return {t: 42 for t in tables}

    monkeypatch.setattr(connect, "make_source_engine_factory", lambda pw, **k: (lambda src: _RoEngine()))
    monkeypatch.setattr(wm, "estimate_source_rows", _fake_estimate)

    def _session(source_type):
        return SimpleNamespace(
            source_config=SourceConnectionConfig(
                source_type=source_type, host="h", database="app", username="u"
            ),
            source_password=SecretValue("pw"),
            has_source=lambda: True,
        )

    pg = _estimate_cdc_table_rows(_session(SourceType.POSTGRES), ["public.orders"])
    assert pg == {"public.orders": 42}
    assert isinstance(seen["dialect"], PostgresSourceDialect)  # NOT the MySQL default

    my = _estimate_cdc_table_rows(_session(SourceType.MYSQL), ["orders"])
    assert my == {"orders": 42}
    assert isinstance(seen["dialect"], MySQLSourceDialect)


def test_cdc_start_card_postgres_manual_is_resnapshot_without_coordinate_inputs() -> None:
    # PostgreSQL Manual renders a re-snapshot explanation (snapshot.mode=initial) with the
    # PG-worded radio labels -- NO GTID/binlog inputs, which Debezium PG cannot use.
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_start_point_card

    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    fake = _FakeUi()
    _render_cdc_start_point_card(
        fake, state, lambda: None,
        wm_resume=None, wm_usable=False, effective_resume=None,
        mode="manual", locked=False, session=None, source_type=SourceType.POSTGRES,
    )
    radio_values = [v for r in fake.radios for v in r.values()]
    assert "Manual — re-snapshot from scratch (initial)" in radio_values
    # No MySQL coordinate wording anywhere in the PG card.
    assert all("GTID" not in v and "binlog" not in v for v in radio_values)
    assert "GTID set" not in fake.texts  # the MySQL manual input field label
    joined = " ".join(fake.texts)
    assert "snapshot.mode=initial" in joined  # the re-snapshot confirmation/explanation


def test_cdc_start_card_postgres_auto_shows_slot_resume_and_wal_lsn() -> None:
    # PostgreSQL Automatic renders the gapless-slot label + the resolved WAL LSN.
    from dsql_migrator.core.cdc import CdcResumePoint
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_start_point_card

    wm = CdcResumePoint(wal_lsn="3/AF012B8")
    state = DataMigrationState()
    state.set_cdc_start_mode("auto")
    fake = _FakeUi()
    _render_cdc_start_point_card(
        fake, state, lambda: None,
        wm_resume=wm, wm_usable=True, effective_resume=wm,
        mode="auto", locked=False, session=None, source_type=SourceType.POSTGRES,
    )
    radio_values = [v for r in fake.radios for v in r.values()]
    assert any("gapless from the replication slot" in v for v in radio_values)
    assert "3/AF012B8" in " ".join(fake.texts)  # WAL LSN summary + confirmation
